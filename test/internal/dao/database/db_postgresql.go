package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"github.com/lib/pq"
	"golang.org/x/net/proxy"
	"gorm.io/gorm"
)

// pqDialer 实现 pq.Dialer 接口
type pqDialer struct {
	dialer proxy.Dialer
}

func (d *pqDialer) Dial(network, address string) (net.Conn, error) {
	// 直接使用代理拨号，不需要额外的 context 控制
	// 数据库操作的超时控制由各个 QueryContext/ExecContext 的 context 参数来管理
	return d.dialer.Dial(network, address)
}

func (d *pqDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	// 使用带超时的拨号
	// 注意：proxy.Dialer 可能不支持超时，所以这里通过 goroutine + channel 实现
	type dialResult struct {
		conn net.Conn
		err  error
	}
	resultCh := make(chan dialResult, 1)

	go func() {
		conn, err := d.dialer.Dial(network, address)
		resultCh <- dialResult{conn: conn, err: err}
	}()

	select {
	case <-time.After(timeout):
		return nil, fmt.Errorf("dial timeout after %v", timeout)
	case result := <-resultCh:
		return result.conn, result.err
	}
}

const (
	PostgresDefaultDB     = "postgres"
	PostgresDefaultSchema = "public"

	PostgresTableCountSql  = "SELECT n_live_tup AS TABLE_ROWS FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2"
	PostgresTableColumnSQL = `SELECT 
    a.attname AS COLUMN_NAME,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS DATA_TYPE,
    d.description AS COLUMN_COMMENT
FROM 
    pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_description d ON (d.objoid = a.attrelid AND d.objsubid = a.attnum)
    JOIN pg_catalog.pg_class c ON (a.attrelid = c.oid)
    JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
WHERE 
    n.nspname = $1
    AND c.relname = $2
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY 
    a.attnum`
	PostgresTableCommentSQL = `SELECT 
    COALESCE(d.description, '') AS TABLE_COMMENT
FROM 
    pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_description d 
        ON d.objoid = c.oid AND d.objsubid = 0
    JOIN pg_catalog.pg_namespace n 
        ON n.oid = c.relnamespace
WHERE 
    n.nspname = $1
    AND c.relname = $2
    AND c.relkind = 'r'`
	PostgresTableNameSql  = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = $1 AND TABLE_NAME = $2"
	PostgresPrimaryKeySql = `
SELECT 
    kcu.column_name
FROM 
    information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        AND tc.table_name = kcu.table_name
WHERE 
    tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = $1
    AND tc.table_name = $2
ORDER BY 
    kcu.ordinal_position
`
)

type DBPostgreSQL struct {
	dao Dao
}

func (d *DBPostgreSQL) GetDBConnection(ctx context.Context, connDbSource entity.DatabaseConn) (*sql.DB, error) {
	logx.I(ctx, "GetDBConnection: connDbSource(DbType: %s, Host: %s, SchemaName: %s, DbName: %s)",
		connDbSource.DbType, connDbSource.Host, connDbSource.SchemaName, connDbSource.DbName)

	connDbSource.Host = strings.TrimSpace(connDbSource.Host)
	connDbSource.Username = strings.TrimSpace(connDbSource.Username)

	// 进行二次校验并解析域名为IP（防止DNS重绑定攻击）
	resolvedHost, err := resolveAndCheckHost(ctx, connDbSource.Host, connDbSource.Uin)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.TestConnTimeout)
	defer cancel()

	dbName := connDbSource.DbName
	if dbName == "" {
		dbName = PostgresDefaultDB
		logx.W(ctx, "GetDBConnection, dbName empty, default: %s", dbName)
	}

	schema := connDbSource.SchemaName
	if schema == "" {
		schema = PostgresDefaultSchema
		logx.W(ctx, "GetDBConnection, schema empty, default: %s", schema)
	}

	var dsn string
	var dbConn *sql.DB

	// 判断是否使用代理（PostgreSQL 直接根据 Proxy 配置判断）
	useProxy := config.App().DbSource.Proxy != ""
	if useProxy {
		// 创建SOCKS5代理dialer
		var auth *proxy.Auth
		if config.App().DbSource.ProxyUser != "" {
			auth = &proxy.Auth{
				User:     config.App().DbSource.ProxyUser,
				Password: config.App().DbSource.ProxyPd,
			}
		}
		dialer, err := proxy.SOCKS5("tcp", config.App().DbSource.Proxy, auth, proxy.Direct)
		if err != nil {
			logx.E(ctx, "create SOCKS5 proxy failed: %v", err)
			return nil, errs.ErrOpenDbSourceFail
		}
		// 使用 pq.NewConnector 设置自定义 Dialer
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s search_path=%s %s",
			resolvedHost, connDbSource.Port, connDbSource.Username, connDbSource.Password, dbName,
			schema, config.App().DbSource.DsnConfigPostgreSQL)

		// 检查连接字符串是否包含不安全的参数
		if err := validateDSNSecurity(ctx, dsn); err != nil {
			logx.E(ctx, "PostgreSQL DSN security check failed: %v", err)
			return nil, err
		}

		connector, err := pq.NewConnector(dsn)
		if err != nil {
			logx.E(ctx, "create PostgreSQL connector failed: %v", err)
			return nil, errs.ErrOpenDbSourceFail
		}
		// 使用自定义 Dialer 包装器，不传入 context
		// 超时控制由各个数据库操作的 QueryContext/ExecContext 的 context 参数来管理
		connector.Dialer(&pqDialer{dialer: dialer})
		dbConn = sql.OpenDB(connector)
	} else {
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s search_path=%s %s",
			resolvedHost, connDbSource.Port, connDbSource.Username, connDbSource.Password, dbName,
			schema, config.App().DbSource.DsnConfigPostgreSQL)

		// 检查连接字符串是否包含不安全的参数
		if err := validateDSNSecurity(ctx, dsn); err != nil {
			logx.E(ctx, "PostgreSQL DSN security check failed: %v", err)
			return nil, err
		}

		dbConn, err = sql.Open(connDbSource.DbType, dsn)
		if err != nil {
			logx.E(ctx, "GetDBConnection, sql.Open failed, connDbSource: %+v, error: %v",
				connDbSource, err)
			return nil, errs.ErrOpenDbSourceFail
		}
	}
	connDbSource.Password = "****"

	// 使用异步的方式测试连接，因为pqDialer的超时时间是用dsn中的connect_timeout决定的，而不是ctx中的
	err = testConnection(ctx, resolvedHost, dbConn)
	if err != nil {
		logx.W(ctx, "test %v connection fail: %v", connDbSource.Host, err)
		return nil, errs.ErrOpenDbSourceFail
	}
	return dbConn, nil
}

// ShowDatabases 获取数据源中的数据库名称列表
// 原名 GetDBList
func (d *DBPostgreSQL) ShowDatabases(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "ShowDatabases, GetDBConnection failed, error: %+v", err)
		return nil, err
	}

	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			logx.E(ctx, "ShowDatabases, Close failed, error: %+v", err)
		}
	}(dbConn)

	var query string
	query = "SELECT datname FROM pg_database WHERE datistemplate = false"

	rows, err := dbConn.QueryContext(ctx, query)
	if err != nil {
		logx.E(ctx, "ShowDatabases, QueryContext failed, error: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx,
			i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logx.E(ctx, "ShowDatabases, rows.Close failed, error: %+v", err)
		}
	}()

	var databases []string
	for rows.Next() {
		var dbName string
		err = rows.Scan(&dbName)
		if err != nil {
			logx.E(ctx, "ShowDatabases, rows.Scan failed, error: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
				i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
		}

		databases = append(databases, dbName)
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "ShowDatabases, rows.Next failed, error: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	logx.I(ctx, "ShowDatabases, query: %s, databases: %+v", query, databases)
	return databases, nil
}

// GetDBTableList 获取数据源下的 table 列表
func (d *DBPostgreSQL) GetDBTableList(ctx context.Context, connDbSource entity.DatabaseConn,
	page, pageSize int) (tables []string, total int, err error) {
	logx.I(ctx, "GetDBTableList, host: %+v, port: %+v, schema: %+v, dbName: %+v",
		connDbSource.Host, connDbSource.Port, connDbSource.SchemaName, connDbSource.DbName)

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	conn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "GetDBTableList, GetDBConnection failed, error: %+v", err)
		return nil, 0, err
	}

	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			logx.E(ctx, "GetDBTableList, Close failed, error: %+v", err)
		}
	}(conn)

	schema := connDbSource.SchemaName
	if schema == "" {
		schema = PostgresDefaultSchema
	}

	var query string
	query = "SELECT tablename FROM pg_tables WHERE schemaname = $1"

	rows, err := conn.QueryContext(ctx, query, schema)
	if err != nil {
		logx.E(ctx, "GetDBTableList, QueryContext failed, error: %+v", err)
		return nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logx.E(ctx, "GetDBTableList, rows.Close failed: %v", err)
		}
	}()

	var allTables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			logx.E(ctx, "GetDBTableList, rows.Scan failed, error: %+v", err)
			return nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
				i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
		}
		allTables = append(allTables, table)
	}
	total = len(allTables)

	// 计算分页范围
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	logx.I(ctx, "GetDBTableList, query: %s, total: %d, start: %d, end: %d, pagedTable: %+v",
		query, total, start, end, allTables[start:end])
	// 返回分页结果
	return allTables[start:end], total, nil
}

// GetTableInfo 获取外部数据表的完整信息，包括列名、数据类型、行数和列数
func (d *DBPostgreSQL) GetTableInfo(ctx context.Context, connDbSource entity.DatabaseConn,
	tableName string) (*entity.TableInfo, error) {
	logx.I(ctx, "GetTableInfo, Host: %+v, Port: %+v, DbName: %+v, SchemaName: %+v, tableName: %+v",
		connDbSource.Host, connDbSource.Port, connDbSource.DbName, connDbSource.SchemaName, tableName)

	if tableName == "" {
		return nil, errs.ErrNameInvalid
	}

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "GetTableInfo, GetDBConnection failed, error: %+v", err)
		return nil, err
	}

	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			logx.E(ctx, "GetTableInfo, Close failed, error: %+v", err)
		}
	}(dbConn)

	var tableSafety string
	err = dbConn.QueryRowContext(ctx, PostgresTableNameSql, connDbSource.SchemaName, tableName).Scan(&tableSafety)
	if err != nil {
		logx.W(ctx, "GetTableInfo, QueryRowContext failed, error: %+v", tableName, err)
		return nil, errs.ErrWrapf(errs.ErrDbTableIsNotExist,
			i18n.Translate(ctx, i18nkey.KeyDatabaseTableInfo), connDbSource.DbName, tableName)
	}

	// 3.获取行数,  获取表注解
	var rowCount int64
	err = dbConn.QueryRowContext(ctx, PostgresTableCountSql, connDbSource.SchemaName, tableName).Scan(&rowCount)
	if err != nil {
		logx.W(ctx, "GetTableInfo, QueryRowContext failed, tableName: %+v, error: %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountInfoFailure), err.Error(), tableSafety)
	}

	// 4. 获取列信息
	var rows *sql.Rows
	rows, err = dbConn.QueryContext(ctx, PostgresTableColumnSQL, connDbSource.SchemaName, tableSafety)
	if err != nil {
		logx.W(ctx, "GetTableInfo, QueryRowContext failed, tableName: %+v, error: %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryDatabaseFailure), connDbSource.DbName)
	}

	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {
			logx.W(ctx, "GetTableInfo, rows.Close failed, error: %+v", err)
		}
	}(rows)

	// 5. 解析列信息
	columnInfo := make([]*entity.ColumnInfo, 0)
	for rows.Next() {
		var (
			field    string
			typeInfo string
			comment  sql.NullString
		)
		err = rows.Scan(&field, &typeInfo, &comment)
		if err != nil {
			logx.E(ctx, "GetTableInfo, rows.Scan failed, error: %+v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceParsingFailed,
				i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), connDbSource.DbName)
		}

		columnInfo = append(columnInfo, &entity.ColumnInfo{
			ColumnName: field,
			DataType:   typeInfo,
			ColComment: comment.String,
		})
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "GetTableInfo, rows.Next failed, error: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceParsingFailed,
			i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), connDbSource.DbName)
	}

	// 6. 获取表注解
	var tableComment sql.NullString

	// 查询表注释
	err = dbConn.QueryRowContext(ctx, PostgresTableCommentSQL, connDbSource.SchemaName, tableSafety).Scan(&tableComment)
	if err != nil {
		logx.W(ctx, "GetTableInfo, QueryRowContext failed, error: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableAnnotationFailure), tableSafety)
	}

	columnCount := len(columnInfo)

	// 7. 判断表格大小是否超出范围
	if rowCount > int64(config.App().DbSource.MaxTableRow) || columnCount > config.App().DbSource.MaxTableCol {
		logx.W(ctx, "表格大小超出了限定范围，表格名称: %+v, rowCount: %+v columnCount: %+v",
			tableName, rowCount, columnCount)
		return nil, errs.ErrWrapf(errs.ErrDbTableSizeInvalid, i18n.Translate(ctx, i18nkey.KeyTableSizeExceedLimit),
			tableName, rowCount, columnCount, config.App().DbSource.MaxTableRow, config.App().DbSource.MaxTableCol)
	}

	return &entity.TableInfo{
		ColumnInfo:   columnInfo,
		RowCount:     rowCount,
		ColumnCount:  columnCount,
		TableComment: tableComment.String,
	}, nil
}

// ListPreviewData 查询预览数据
func (d *DBPostgreSQL) ListPreviewData(ctx context.Context, connDbSource entity.DatabaseConn,
	tableName string, pageNumber, pageSize int, columnName string, columnValue string, timeout time.Duration) (
	columns []string, rows []*pb.RowData, total int64, err error) {
	logx.I(ctx, "ListPreviewData, DbType: %+v, Host: %v, DbName: %+v, SchemaName: %+v, "+
		"tableName: %+v, columnName: %+v, columnValue: %+v",
		connDbSource.DbType, connDbSource.Host, connDbSource.DbName, connDbSource.SchemaName,
		tableName, columnName, columnValue)

	if tableName == "" {
		return nil, nil, 0, errs.ErrNameInvalid
	}

	// TODO: 验证列名有效性

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "ListPreviewData, GetDBConnection failed, error: %+v", err)
		return nil, nil, 0, err
	}

	defer func() {
		err = dbConn.Close()
		if err != nil {
			logx.E(ctx, "ListPreviewData, dbConn.Close failed, error: %+v", err)
		}
	}()

	columnQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 0", d.adaptTableElement(ctx, tableName))
	rowsMeta, err := dbConn.QueryContext(ctx, columnQuery)
	if err != nil {
		logx.E(ctx, "ListPreviewData, QueryContext failed, error: %+v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableFailure), tableName)
	}

	defer func() {
		err = rowsMeta.Close()
		if err != nil {
			logx.W(ctx, "ListPreviewData, rowsMeta.Close failed, error: %+v", err)
		}
	}()

	columns, err = rowsMeta.Columns()
	logx.I(ctx, "ListPreviewData, columns: %+v", columns)
	if len(columns) == 0 {
		return nil, nil, 0, errs.ErrDbSourceTableEmpty
	}
	if err != nil {
		logx.E(ctx, "ListPreviewData, rowsMeta.Columns failed, error: %+v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableFailure), tableName)
	}

	// 检查是否需要筛选
	filterEnabled := columnName != "" && columnValue != ""
	if filterEnabled {
		columnExists := false
		for _, col := range columns {
			if col == columnName {
				columnExists = true
				break
			}
		}
		if !columnExists {
			return nil, nil, 0, errx.Newf(4720023, "column %s not exist", columnName)
		}
	}

	// 获取行数,  获取表注解
	err = dbConn.QueryRowContext(ctx, PostgresTableCountSql, connDbSource.SchemaName, tableName).Scan(&total)
	if err != nil {
		logx.W(ctx, "ListPreviewData, QueryRowContext failed, tableName: %s, error: %+v", tableName, err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountInfoFailure), err.Error(), err, tableName)
	}

	// 2. 查询总行数（带筛选条件）
	var countQuery string
	if filterEnabled {
		countQuery = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $1",
			d.adaptTableElement(ctx, tableName), d.adaptTableElement(ctx, columnName))
		err = dbConn.QueryRowContext(ctx, countQuery, columnValue).Scan(&total)
	} else if total < countMaxLimit {
		countQuery = fmt.Sprintf("SELECT COUNT(*) FROM %s", d.adaptTableElement(ctx, tableName))
		err = dbConn.QueryRowContext(ctx, countQuery).Scan(&total)
	}
	if err != nil {
		logx.E(ctx, "ListPreviewData, QueryRowContext failed, error: %+v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountFailure), tableName)
	}

	// 获取数据表的主键列名
	var primaryKeys []string
	primaryKeys, err = d.getPrimaryKeys(ctx, dbConn, connDbSource, tableName)
	if err != nil {
		logx.E(ctx, "ListPreviewData, getPrimaryKeys failed, error: %+v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), tableName)
	}

	orderByColumn := columns[0]
	if len(primaryKeys) > 0 {
		for _, key := range columns {
			if key == primaryKeys[0] {
				orderByColumn = key
				break
			}
		}
	}
	logx.D(ctx, "ListPreviewData, dbName: %+v, primaryKeys: %+v", connDbSource.DbName, primaryKeys)

	// 3. 分页查询数据（带筛选条件）
	offset := (pageNumber - 1) * pageSize
	var dataQuery string
	if filterEnabled {
		dataQuery = fmt.Sprintf("SELECT * FROM %s WHERE %s LIKE $1 ORDER BY %s LIMIT $2 OFFSET $3",
			d.adaptTableElement(ctx, tableName), d.adaptTableElement(ctx, columnName),
			d.adaptTableElement(ctx, orderByColumn))
	} else {
		dataQuery = fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT $1 OFFSET $2",
			d.adaptTableElement(ctx, tableName), d.adaptTableElement(ctx, orderByColumn))
	}
	var dataRows *sql.Rows
	if filterEnabled {
		dataRows, err = dbConn.QueryContext(ctx, dataQuery, "%"+columnValue+"%", pageSize, offset)
	} else {
		dataRows, err = dbConn.QueryContext(ctx, dataQuery, pageSize, offset)
	}
	if err != nil {
		logx.E(ctx, "ListPreviewData, QueryContext failed, sql: %+v, error: %+v", dataQuery, err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableDataFailure), tableName)
	}

	defer func() {
		err = dataRows.Close()
		if err != nil {
			logx.E(ctx, "ListPreviewData, dataRows.Close failed, error: %+v", err)
		}
	}()

	columnTypes, err := dataRows.ColumnTypes()
	if err != nil {
		logx.E(ctx, "ListPreviewData, dataRows.ColumnTypes failed, error: %+v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyGetColumnTypeFailure), tableName)
	}

	// 4. 解析数据行
	for dataRows.Next() {
		rowData := make([]any, len(columns))
		for i, _ := range columnTypes {
			rowData[i] = new(sql.NullString)
		}

		if err = dataRows.Scan(rowData...); err != nil {
			logx.E(ctx, "ListPreviewData, dataRows.Scan failed, error: %+v", err)
			return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
				i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), tableName)
		}

		row := &pb.RowData{
			Values: make([]string, len(columns)),
		}

		for i, val := range rowData {
			if v, ok := val.(*sql.NullString); ok && v.Valid {
				row.Values[i] = v.String
			} else {
				row.Values[i] = ""
			}
		}

		rows = append(rows, row)
	}

	if err = dataRows.Err(); err != nil {
		logx.E(ctx, "ListPreviewData, dataRows.Next failed, error: %+v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), tableName)
	}

	return columns, rows, total, nil
}

// getPrimaryKeys 获取表主键列名
func (d *DBPostgreSQL) getPrimaryKeys(ctx context.Context, db *sql.DB,
	connDbSource entity.DatabaseConn, tableName string) ([]string, error) {
	rows, err := db.Query(PostgresPrimaryKeySql, connDbSource.SchemaName, tableName)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logx.W(ctx, "getPrimaryKeys, rows.Close failed, error: %+v", err)
		}
	}()

	var keys []string
	for rows.Next() {
		var col string
		if err = rows.Scan(&col); err != nil {
			logx.W(ctx, "getPrimaryKeys, rows.Scan failed, error: %+v", err)
			return nil, err
		}

		keys = append(keys, col)
	}

	if err = rows.Err(); err != nil {
		logx.W(ctx, "getPrimaryKeys, rows.Next failed, error: %+v", err)
		return nil, err
	}

	return keys, nil
}

// parseTableElement 解析表信息
func (d *DBPostgreSQL) parseTableElement(ctx context.Context, tableInfo string) (string, string, error) {
	elements := strings.SplitN(tableInfo, ".", 2)

	if len(elements) == 0 {
		logx.E(ctx, "parseTableElement, tableInfo invalid, tableInfo: %s", tableInfo)
		return "", "", fmt.Errorf("tableInfo invalid")
	} else if len(elements) == 1 {
		return PostgresDefaultSchema, elements[0], nil
	}

	return elements[0], elements[1], nil
}

// ensureSchema 确保库表模式
func (d *DBPostgreSQL) ensureDbSchema(ctx context.Context, connDbSource entity.DatabaseConn) string {
	schema := connDbSource.SchemaName
	if schema == "" {
		schema = PostgresDefaultSchema
		logx.W(ctx, "ensureDbSchema, schema empty, default: %s", schema)
	}

	return schema
}

// CheckDbTableIsExisted 判断数据库是否可以连接并且数据表存在
func (d *DBPostgreSQL) CheckDbTableIsExisted(ctx context.Context, dbSource *entity.Database,
	tableName string) (bool, error) {
	var tableSafety string
	password, err := util.Decrypt(dbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		return false, err
	}
	uin, err := d.dao.GetDBUin(ctx, dbSource.CorpBizID)
	if err != nil {
		return false, err
	}

	connDbSource := entity.DatabaseConn{
		DbType:     dbSource.DBType,
		Host:       dbSource.Host,
		DbName:     dbSource.DBName,
		Username:   dbSource.Username,
		Password:   password,
		Port:       dbSource.Port,
		SchemaName: dbSource.SchemaName,
		CreateTime: &dbSource.CreateTime, // 使用数据库的创建时间
		Uin:        uin,
	}
	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "CheckDbTableIsExisted|获取数据库连接失败: %v", err)
		return false, err
	}
	defer func(dbConn *sql.DB) {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.W(ctx, "CheckDbTableIsExisted|关闭数据库连接失败: %v", err)
		}
	}(dbConn)

	err = dbConn.QueryRowContext(ctx, PostgresTableNameSql, connDbSource.SchemaName, tableName).Scan(&tableSafety)
	if err != nil {
		logx.W(ctx, "CheckDbTableIsExisted, QueryRowContext failed, tableName: %+v, error: %+v", tableName, err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, errs.ErrWrapf(errs.ErrDbTableIsNotExist,
				i18n.Translate(ctx, i18nkey.KeyDatabaseTableInfo), connDbSource.DbName, tableName)
		} else if err != nil {
			return false, err
		}
	}

	return true, nil
}

// GetTopNValueV2 获取数据库表列的TopN值
func (d *DBPostgreSQL) GetTopNValueV2(ctx context.Context, dbSource *entity.Database, robotId, dbTableBizID,
	embeddingVersion uint64, embeddingName string) error {
	t0 := time.Now()
	if dbSource == nil {
		return errs.ErrAddDbSourceFail
	}

	tableFilter := entity.TableFilter{
		CorpBizID:    dbSource.CorpBizID,
		AppBizID:     dbSource.AppBizID,
		DBTableBizID: dbTableBizID,
	}
	dbTable, err := d.dao.DescribeTable(ctx, &tableFilter)
	// dbTable, err := GetDBTableDao().GetByBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		return err
	}
	maxCowSize := min(int(dbTable.RowCount), config.App().DbSource.ValueLinkConfig.MaxTraverseRow)
	decrypt, err := util.Decrypt(dbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		return err
	}
	uin, err := d.dao.GetDBUin(ctx, dbSource.CorpBizID)
	if err != nil {
		return err
	}
	connDbSource := entity.DatabaseConn{
		DbType:     dbSource.DBType,
		Host:       dbSource.Host,
		DbName:     dbSource.DBName,
		Username:   dbSource.Username,
		Password:   decrypt,
		Port:       dbSource.Port,
		SchemaName: dbSource.SchemaName,
		CreateTime: &dbSource.CreateTime, // 使用数据库的创建时间
		Uin:        uin,
	}
	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "GetTopNValue|获取数据库连接失败: %v", err)
		return err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.W(ctx, "GetTopNValue|关闭数据库连接失败: %v", closeErr)
		}
	}()

	columnFilter := entity.ColumnFilter{
		CorpBizID:    dbSource.CorpBizID,
		AppBizID:     dbSource.AppBizID,
		DBTableBizID: dbTableBizID,
	}
	dbTableColumns, _, err := d.dao.DescribeColumnList(ctx, &columnFilter)
	// dbTableColumns, err := GetDBTableColumnDao().GetByTableBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		logx.E(ctx, "GetTopNValue|获取表列信息失败: %v", err)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ValueLinkTimeout)
	defer cancel()

	// 获取数据表的主键列名
	var primaryKeys []string
	primaryKeys, _ = d.getPrimaryKeys(ctx, dbConn, connDbSource, dbTable.Name)

	selectedColumns := make([]string, 0, len(dbTableColumns))
	selectedColumnForQuery := make([]string, 0, len(dbTableColumns))
	for _, col := range dbTableColumns {
		if _, ok := mysqlTopNDataType[col.DataType]; ok {
			selectedColumns = append(selectedColumns, col.ColumnName)
			selectedColumnForQuery = append(selectedColumnForQuery,
				fmt.Sprintf("%s", d.adaptTableElement(ctx, col.ColumnName)))
		}
	}
	if len(selectedColumns) == 0 {
		logx.W(ctx, "GetTopNValue|没有找到表列信息: dbTable: %v", dbTable)
		return nil
	}

	primaryColumn := dbTableColumns[0].ColumnName
	if len(primaryKeys) > 0 {
		primaryColumn = primaryKeys[0]
	}

	logx.I(ctx, "GetTopNValueV2, dbTable: %+v, selectedColumns: %+v, primaryColumn: %+v",
		dbTable, selectedColumns, primaryColumn)

	var sqlQuery string
	sqlQuery = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT $1 OFFSET $2",
		strings.Join(selectedColumnForQuery, ","),
		d.adaptTableElement(ctx, dbTable.Name), d.adaptTableElement(ctx, primaryColumn))

	col2ColInfo := make(map[string]*entity.Column)
	colStats := make(map[string]*TopN)
	for _, col := range dbTableColumns {
		col2ColInfo[col.ColumnName] = col
		colStats[col.ColumnName] = NewTopN(config.App().DbSource.ValueLinkConfig.MaxTopValueNum,
			config.App().DbSource.ValueLinkConfig.TrimKeepSize, config.App().DbSource.ValueLinkConfig.TrimThreshold)
	}

	offset := 0
	for offset < maxCowSize {
		limit := config.App().DbSource.ValueLinkConfig.MaxTraverseRow
		if offset+limit > maxCowSize {
			limit = maxCowSize - offset
		}
		var rows *sql.Rows
		rows, err = dbConn.QueryContext(ctx, sqlQuery, limit, offset)
		if err != nil {
			logx.E(ctx, "GetTopNValue|获取数据失败: %v sqlQuery:%v, limit:%v, offset:%v", err, sqlQuery, limit, offset)
			return err
		}

		columns, err := rows.Columns()
		if err != nil {
			logx.E(ctx, "GetTopNValue|获取列名失败: %v", err)
			return err
		}
		rowData := make([]any, len(columns))
		for i := range rowData {
			rowData[i] = new(sql.NullString)
		}

		for rows.Next() {
			if err = rows.Scan(rowData...); err != nil {
				logx.E(ctx, "GetTopNValue|扫描数据失败: %v", err)
				return err
			}
			for i, val := range rowData {
				if v, ok := val.(*sql.NullString); ok && v.Valid {
					if utf8.RuneCountInString(v.String) > config.App().DbSource.ValueLinkConfig.MaxValueLen {
						v.String = string([]rune(v.String)[:config.App().DbSource.ValueLinkConfig.MaxValueLen]) + "..."
					}
					colStats[columns[i]].Add(v.String)
				} else {
					continue
				}
			}
		}
		offset += limit
		if rows != nil {
			err = rows.Close()
			if err != nil {
				logx.W(ctx, "GetTopNValue|关闭数据失败: %v", err)
			}
		}
	}
	t1 := time.Now()
	logx.D(ctx, "GetTopNValue|get top value over cost:%v， dbTable: %v, offset:%v,  ",
		t1.Sub(t0).Milliseconds(), dbTable, offset)

	for _, colName := range selectedColumns {
		dbTableColumn := col2ColInfo[colName]
		topValue := make([]string, 0, config.App().DbSource.ValueLinkConfig.MaxTopValueNum)
		var dbTableTopValues []*entity.TableTopValue
		logx.D(ctx, "GetTopNValue|获取topN值: colName:%+v, value:%+v", colName, colStats[colName].Top())
		for _, values := range colStats[colName].Top() {
			value := values.Value
			topValue = append(topValue, value)
			dbTableTopValue := &entity.TableTopValue{
				CorpBizID:          dbTableColumn.CorpBizID,
				AppBizID:           dbTableColumn.AppBizID,
				DBSourceBizID:      dbTable.DBSourceBizID,
				DbTableBizID:       dbTableColumn.DBTableBizID,
				DbTableColumnBizID: dbTableColumn.DBTableColumnBizID,
				BusinessID:         idgen.GetId(),
				ColumnName:         dbTableColumn.ColumnName,
				ColumnValue:        value,
				ColumnComment:      dbTableColumn.ColumnComment,
				IsDeleted:          false,
				CreateTime:         time.Now(),
				UpdateTime:         time.Now(),
			}
			dbTableTopValues = append(dbTableTopValues, dbTableTopValue)
		}

		err = d.dao.CreateTopValue(ctx, dbTableTopValues)
		if err != nil {
			logx.E(ctx, "GetTopNValue| value:%v, 新增topN值失败: %v", dbTableTopValues, err)
			return err
		}
		for _, dbTableTopValue := range dbTableTopValues {
			content := fmt.Sprintf("%v:%v", dbTableColumn.ColumnName, dbTableTopValue.ColumnValue)
			err = d.dao.AddVector(ctx, robotId, dbTableColumn.AppBizID, dbTableBizID, dbTableTopValue.BusinessID,
				embeddingVersion, embeddingName, dbTable.EnableScope, content, retrieval.EnvType_Test)
			if err != nil {
				logx.E(ctx, "GetTopNValue|input vdb err: %v", err)
				return err
			}
		}
	}
	t2 := time.Now()
	logx.I(ctx, "GetTopNValue|add vdb and update: cost: %v dbTable: %v", t2.Sub(t1).Milliseconds(), dbTable)
	return nil
}

func (d *DBPostgreSQL) QuerySqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn,
	exeSql string, sqlParams []string) ([]string, []*pb.RowData, string, error) {
	logx.I(ctx, "QuerySqlForDbSource|dbName: %v, sql:%v, sqlParams:%+v, len:%v",
		connDbSource.DbName, exeSql, sqlParams, len(sqlParams))

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.E(ctx, "QuerySqlForDbSource|get db connection failed: %v", err)
		return nil, nil, err.Error(), err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.W(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()
	anySlice := make([]any, len(sqlParams))
	for i, v := range sqlParams {
		anySlice[i] = v
	}
	// 查询语句判断是否要加limit，没有需要使用默认limit
	defaultLimit := gox.IfElse(config.App().DbSource.DefaultSelectLimit > 0,
		config.App().DbSource.DefaultSelectLimit, 100)
	exeSql = strings.TrimRight(strings.TrimSpace(exeSql), ";") // 去除末尾分号
	exeSql = addMysqlDefaultLimit(exeSql, defaultLimit)
	logx.I(ctx, "QuerySqlForDbSource, exeSql: %s", exeSql)

	rows, err := dbConn.QueryContext(ctx, exeSql, anySlice...)
	if err != nil {
		logx.W(ctx, "QueryContext err: %v sqlQuery:%v, sqlParams:%v, dbSource:%v",
			err, exeSql, sqlParams, connDbSource.DbName)
		return nil, nil, err.Error(), err
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.W(ctx, "关闭查询结果集失败: %v", closeErr)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		logx.E(ctx, "获取列名失败: %v", err)
		return nil, nil, err.Error(), err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		logx.E(ctx, "获取列类型失败: %v", err)
		return nil, nil, err.Error(), err
	}
	var values []*pb.RowData

	for rows.Next() {
		rowData := make([]any, len(columns))
		// 针对 sql server 特殊处理 uniqueidentifier 进行特殊处理
		for i, colType := range columnTypes {
			if colType.DatabaseTypeName() == sqlserverUniqueidentifier {
				rowData[i] = new([]byte)
			} else {
				rowData[i] = new(sql.NullString)
			}
		}
		if err = rows.Scan(rowData...); err != nil {
			logx.E(ctx, "Scan error: %v", err)
			return nil, nil, "", err
		}
		row := &pb.RowData{
			Values: make([]string, len(columns)),
		}

		for i, val := range rowData {
			// 针对 sql server 特殊处理 uniqueidentifier 进行特殊处理
			if columnTypes[i].DatabaseTypeName() == sqlserverUniqueidentifier {
				if v, ok := val.(*[]byte); ok && v != nil {
					row.Values[i] = bytesToGUIDString(*v)
				} else {
					row.Values[i] = ""
				}
			} else {
				if v, ok := val.(*sql.NullString); ok && v.Valid {
					row.Values[i] = v.String
				} else {
					row.Values[i] = ""
				}
			}
		}
		values = append(values, row)
	}
	return columns, values, "", nil
}

func (d *DBPostgreSQL) QueryDBSchemas(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	logx.I(ctx, "QueryDBSchemas, Host: %+v, Port: %+v, SchemaName: %+v, DbName: %+v",
		connDbSource.Host, connDbSource.Port, connDbSource.SchemaName, connDbSource.DbName)

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	conn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "QueryDBSchemas, GetDBConnection failed, error: %+v", err)
		return nil, errs.ErrOpenDbSourceFail
	}

	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			logx.E(ctx, "QueryDBSchemas, Close failed, error: %+v", err)
		}
	}(conn)

	var query string
	query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema') " +
		"AND schema_name NOT LIKE 'pg_%'"

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		logx.E(ctx, "QueryDBSchemas, QueryContext failed, error: %+v", err)
		return nil, errs.ErrDBSchemaQueryFailed
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logx.E(ctx, "QueryDBSchemas, rows.Close failed: %v", err)
		}
	}()

	var schemas []string
	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			logx.E(ctx, "QueryDBSchemas, rows.Scan failed, error: %+v", err)
			return nil, errs.ErrDBSchemaQueryFailed
		}
		schemas = append(schemas, schema)
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "QueryDBSchemas, rows iteration error: %+v", err)
		return nil, errs.ErrDBSchemaQueryFailed
	}

	logx.I(ctx, "QueryDBSchemas, query: %s, schemas: %+v", query, schemas)

	return schemas, nil
}

func (d *DBPostgreSQL) adaptTableElement(ctx context.Context, tableElement string) string {
	if !util.HasUpperCase(tableElement) {
		return tableElement
	}

	adaptedElement := fmt.Sprintf("\"%s\"", tableElement)
	logx.I(ctx, "adaptTableElement, tableElement: %s, adaptedElement: %s", tableElement, adaptedElement)
	return adaptedElement
}
