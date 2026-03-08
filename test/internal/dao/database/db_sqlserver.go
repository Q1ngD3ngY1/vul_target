package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
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
	mssql "github.com/denisenkom/go-mssqldb"
	"golang.org/x/net/proxy"
	"gorm.io/gorm"
)

// mssqlDialer 实现 mssql.Dialer 接口
type mssqlDialer struct {
	dialer proxy.Dialer
}

func (d *mssqlDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// 使用 channel 实现带超时的拨号
	type dialResult struct {
		conn net.Conn
		err  error
	}
	resultCh := make(chan dialResult, 1)

	go func() {
		conn, err := d.dialer.Dial(network, addr)
		resultCh <- dialResult{conn: conn, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.conn, result.err
	}
}

type DBSQLServer struct {
	dao Dao
}

func (d *DBSQLServer) GetDBConnection(ctx context.Context, connDbSource entity.DatabaseConn) (*sql.DB, error) {
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

	var dsn string
	var dbConn *sql.DB

	useProxy := shouldUseProxy(ctx, connDbSource)
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
		// 使用 mssql.Connector 设置自定义 Dialer
		dsn = fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&%s",
			connDbSource.Username, url.QueryEscape(connDbSource.Password), resolvedHost, connDbSource.Port, connDbSource.DbName, config.App().DbSource.DsnConfigSqlServer)

		// 检查连接字符串是否包含不安全的参数
		if err := validateDSNSecurity(ctx, dsn); err != nil {
			logx.E(ctx, "SQL Server DSN security check failed: %v", err)
			return nil, err
		}

		connector, err := mssql.NewConnector(dsn)
		if err != nil {
			logx.E(ctx, "create SQL Server connector failed: %v", err)
			return nil, errs.ErrOpenDbSourceFail
		}
		connector.Dialer = &mssqlDialer{dialer: dialer}
		dbConn = sql.OpenDB(connector)
	} else {
		dsn = fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&%s",
			connDbSource.Username, url.QueryEscape(connDbSource.Password), resolvedHost, connDbSource.Port, connDbSource.DbName, config.App().DbSource.DsnConfigSqlServer)

		// 检查连接字符串是否包含不安全的参数
		if err := validateDSNSecurity(ctx, dsn); err != nil {
			logx.E(ctx, "SQL Server DSN security check failed: %v", err)
			return nil, err
		}

		dbConn, err = sql.Open(connDbSource.DbType, dsn)
	}
	connDbSource.Password = ""
	if err != nil {
		logx.E(ctx, "dao:GetDBConnection open db :%+v failed: %v", connDbSource, err)
		return nil, errs.ErrOpenDbSourceFail
	}

	// 异步测试连接，支持超时
	err = testConnection(ctx, resolvedHost, dbConn)
	if err != nil {
		logx.W(ctx, "test %v connection fail: %v", connDbSource.Host, err)
		return nil, errs.ErrOpenDbSourceFail
	}
	return dbConn, nil
}

// ShowDatabases 获取数据源中的数据库名称列表
// 原名 GetDBList
func (d *DBSQLServer) ShowDatabases(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "GetDBList|获取数据库连接失败: %v", err)
		return nil, err
	}

	defer func(db *sql.DB) {
		closeErr := db.Close()
		if closeErr != nil {
			logx.E(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}(dbConn)

	var query string
	switch connDbSource.DbType {
	case entity.DBTypeMySQL:
		query = "SHOW DATABASES"
	case entity.DBTypeSQLServer:
		query = "SELECT name FROM sys.databases"
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}

	rows, err := dbConn.QueryContext(ctx, query)
	if err != nil {
		logx.E(ctx, "查询数据库列表失败: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.E(ctx, "关闭查询结果失败: %v", closeErr)
		}
	}()

	var databases []string
	for rows.Next() {
		var dbName string
		err = rows.Scan(&dbName)
		if err != nil {
			logx.E(ctx, "解析数据库名称失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
		}
		databases = append(databases, dbName)
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "遍历数据库列表失败: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	return databases, nil
}

// GetDBTableList 获取数据源下的 table 列表
func (d *DBSQLServer) GetDBTableList(ctx context.Context, connDbSource entity.DatabaseConn, page, pageSize int) (tables []string, total int, err error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	conn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "GetDBTableList|获取数据库连接失败: %v", err)
		return nil, 0, err
	}

	defer func() {
		closeErr := conn.Close()
		if closeErr != nil {
			logx.E(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()

	var query string
	switch connDbSource.DbType {
	case entity.DBTypeMySQL:
		query = "SHOW TABLES"
	case entity.DBTypeSQLServer:
		query = "SELECT name FROM sys.tables"
	default:
		return nil, 0, errs.ErrDbSourceTypeNotSupport
	}

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		logx.E(ctx, "query db failed: %v", err)
		return nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.E(ctx, "close rows failed: %v", closeErr)
		}
	}()

	var allTables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			logx.E(ctx, "scan row failed: %v", err)
			return nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
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

	// 返回分页结果
	return allTables[start:end], total, nil
}

// GetTableInfo 获取外部数据表的完整信息，包括列名、数据类型、行数和列数
func (d *DBSQLServer) GetTableInfo(ctx context.Context, connDbSource entity.DatabaseConn, table string) (*entity.TableInfo, error) {

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()
	logx.I(ctx, "GetTabelInfo: host:%v port:%v, dbName :%v, table: %v", connDbSource.Host, connDbSource.Port, connDbSource.DbName, table)

	if table == "" {
		return nil, errs.ErrNameInvalid
	}

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "GetTableInfo|获取数据库连接失败: %v", err)
		return nil, err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.W(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()

	var VarifyTableNameSql string
	var tableSafety string
	VarifyTableNameSql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = @p1 AND TABLE_NAME = @p2"
	err = dbConn.QueryRowContext(ctx, VarifyTableNameSql, connDbSource.DbName, table).Scan(&tableSafety)
	if err != nil {
		logx.W(ctx, "查询表名失败: %v, 查询的表 %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbTableIsNotExist, i18n.Translate(ctx, i18nkey.KeyDatabaseTableInfo), connDbSource.DbName, table)
	}

	// 3.获取行数,  获取表注解
	var rowCount int64

	switch connDbSource.DbType {
	case entity.DBTypeMySQL:
		err = dbConn.QueryRowContext(ctx, mysqlCountSql, connDbSource.DbName, table).Scan(&rowCount)
	case entity.DBTypeSQLServer:
		err = dbConn.QueryRowContext(ctx, SqlServerCountSql, table).Scan(&rowCount)
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	if err != nil {
		logx.W(ctx, "查询表行数信息失败: %v, 查询的表 %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountInfoFailure), err.Error(), tableSafety)
	}

	// 1. 获取列信息
	var rows *sql.Rows
	switch connDbSource.DbType {
	case entity.DBTypeMySQL:
		rows, err = dbConn.QueryContext(ctx, "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", connDbSource.DbName, tableSafety)
	case entity.DBTypeSQLServer:
		rows, err = dbConn.QueryContext(ctx, "SELECT c.name AS COLUMN_NAME, t.name AS DATA_TYPE, ep.value AS COLUMN_COMMENT FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description' WHERE c.object_id = OBJECT_ID(@p1)", tableSafety)
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	if err != nil {
		logx.W(ctx, "查询表列信息失败: %v, 查询的表 %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseFailure), connDbSource.DbName)
	}
	defer func(rows *sql.Rows) {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.W(ctx, "关闭列名查询结果失败: %v", closeErr)
		}
	}(rows)

	columnInfo := make([]*entity.ColumnInfo, 0)
	// 解析列信息
	for rows.Next() {
		var (
			field    string
			typeInfo string
			comment  sql.NullString
		)
		err = rows.Scan(&field, &typeInfo, &comment)
		if err != nil {
			logx.E(ctx, "解析列信息失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceParsingFailed, i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), connDbSource.DbName)
		}
		columnInfo = append(columnInfo, &entity.ColumnInfo{
			ColumnName: field,
			DataType:   typeInfo,
			ColComment: comment.String,
		})
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "遍历列信息失败: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceParsingFailed, i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), connDbSource.DbName)
	}

	// 3.获取行数,  获取表注解
	var tableComment sql.NullString
	switch connDbSource.DbType {
	case entity.DBTypeMySQL:
		// 如果行数足够小，则直接查询行数
		if rowCount < countMaxLimit {
			// 查询行数
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableSafety)
			err = dbConn.QueryRowContext(ctx, countQuery).Scan(&rowCount)
			if err != nil {
				logx.W(ctx, "统计表行数失败: %v, 数据表过大", err)
				return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyCountTableRowTimeout),
					tableSafety, config.App().DbSource.MaxTableRow, config.App().DbSource.MaxTableCol)
			}
		}

		// 查询表注释
		commentQuery := "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =? and TABLE_NAME = ?"
		err = dbConn.QueryRowContext(ctx, commentQuery, connDbSource.DbName, tableSafety).Scan(&tableComment)
		if err != nil {
			logx.W(ctx, "查询表注解失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableAnnotationFailure), tableSafety)
		}
	case entity.DBTypeSQLServer:
		// 如果行数足够小，则直接查询行数
		if rowCount < countMaxLimit {
			// 查询行数
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableSafety)
			err = dbConn.QueryRowContext(ctx, countQuery).Scan(&rowCount)
			if err != nil {
				logx.W(ctx, "查询表行数失败: %v", err)
				return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountFailure), table)
			}
		}
		// 查询表注释
		commentQuery := `
        SELECT ep.value 
        FROM sys.tables t 
        LEFT JOIN sys.extended_properties ep 
            ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' 
        WHERE t.name = @p1`
		err = dbConn.QueryRowContext(ctx, commentQuery, table).Scan(&tableComment)
		if err != nil {
			logx.W(ctx, "查询表注解失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableAnnotationFailure), table)
		}
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}

	columnCount := len(columnInfo)

	// 4. 判断表格大小是否超出范围
	if rowCount > int64(config.App().DbSource.MaxTableRow) || columnCount > config.App().DbSource.MaxTableCol {
		logx.W(ctx, "表格大小超出了限定范围，表格名称: %v， row: %v col : %v", table, rowCount, columnCount)
		return nil, errs.ErrWrapf(errs.ErrDbTableSizeInvalid, i18n.Translate(ctx, i18nkey.KeyTableSizeExceedLimit),
			table, rowCount, columnCount, config.App().DbSource.MaxTableRow, config.App().DbSource.MaxTableCol)
	}

	return &entity.TableInfo{
		ColumnInfo:   columnInfo,
		RowCount:     rowCount,
		ColumnCount:  columnCount,
		TableComment: tableComment.String,
	}, nil
}

// ListPreviewData 查询预览数据
func (d *DBSQLServer) ListPreviewData(ctx context.Context, connDbSource entity.DatabaseConn,
	table string, page, pageSize int, columnName string, columnValue string, timeout time.Duration) (columns []string, rows []*pb.RowData, total int64, err error) {
	logx.I(ctx, "ListPreviewData: DbType:%v, Host:%v, DbName:%v, table:%v, columnName:%v,columnValue:%v",
		connDbSource.DbType, connDbSource.Host, connDbSource.DbName, table, columnName, columnValue)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if table == "" {
		return nil, nil, 0, errs.ErrNameInvalid
	}

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "ListPreviewData|获取数据库连接失败: %v", err)
		return nil, nil, 0, err
	}

	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.E(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()

	sqlColumn := fmt.Sprintf("SELECT * FROM `%s` LIMIT 0", table)
	if connDbSource.DbType == entity.DBTypeSQLServer {
		sqlColumn = fmt.Sprintf("SELECT TOP 0 * FROM [%s]", table)
	}
	rowsMeta, err := dbConn.QueryContext(ctx, sqlColumn)
	if err != nil {
		logx.E(ctx, "查询表列名失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableFailure), table)
	}

	defer func() {
		closeErr := rowsMeta.Close()
		if closeErr != nil {
			logx.W(ctx, "关闭列名查询结果失败: %v", closeErr)
		}
	}()

	columns, err = rowsMeta.Columns()
	logx.I(ctx, "查询列名成功: %v", columns)
	if len(columns) == 0 {
		return nil, nil, 0, errs.ErrDbSourceTableEmpty
	}
	if err != nil {
		logx.E(ctx, "解析列名失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableFailure), table)
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
			return nil, nil, 0, errx.Newf(4720023, "列名 '%s' 不存在", columnName)
		}
	}

	// 获取行数,  获取表注解
	switch connDbSource.DbType {
	case entity.DBTypeMySQL:
		err = dbConn.QueryRowContext(ctx, mysqlCountSql, connDbSource.DbName, table).Scan(&total)
	case entity.DBTypeSQLServer:
		err = dbConn.QueryRowContext(ctx, SqlServerCountSql, table).Scan(&total)
	default:
		return nil, nil, 0, errs.ErrDbSourceTypeNotSupport
	}
	if err != nil {
		logx.W(ctx, "查询表行数信息失败: %v, 查询的表 %v", err, table)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountInfoFailure), err.Error(), err, table)
	}

	// 2. 查询总行数（带筛选条件）
	var countQuery string
	if filterEnabled {
		countQuery = fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE `%s` = ?", table, columnName)
		if connDbSource.DbType == entity.DBTypeSQLServer {
			countQuery = fmt.Sprintf("SELECT COUNT(*) FROM [%s] WHERE [%s] = @p1", table, columnName)
		}
		err = dbConn.QueryRowContext(ctx, countQuery, columnValue).Scan(&total)
	} else if total < countMaxLimit {
		countQuery = fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		if connDbSource.DbType == entity.DBTypeSQLServer {
			countQuery = fmt.Sprintf("SELECT COUNT(*) FROM [%s]", table)
		}
		err = dbConn.QueryRowContext(ctx, countQuery).Scan(&total)
	}
	if err != nil {
		logx.E(ctx, "查询总行数失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountFailure), table)
	}

	// 获取数据表的主键列名
	var primaryKeys []string
	primaryKeys, _ = getSQLServerPrimaryKeys(ctx, dbConn, table)

	orderByColumn := columns[0]
	if len(primaryKeys) > 0 {
		for _, key := range columns {
			if key == primaryKeys[0] {
				orderByColumn = key
				break
			}
		}
	}
	logx.D(ctx, "ListPreviewData｜conn：%+v, primaryKeys: %+v", connDbSource.DbName, primaryKeys)

	// 3. 分页查询数据（带筛选条件）
	offset := (page - 1) * pageSize
	var dataQuery string
	if filterEnabled {
		dataQuery = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` LIKE ? ORDER BY `%s` LIMIT ? OFFSET ?", table, columnName, orderByColumn)
		if connDbSource.DbType == entity.DBTypeSQLServer {
			dataQuery = fmt.Sprintf("SELECT * FROM [%s] WHERE [%s] LIKE @p1 ORDER BY [%s] OFFSET @p2 ROWS FETCH NEXT @p3 ROWS ONLY", table, columnName, orderByColumn)
		}
	} else {
		dataQuery = fmt.Sprintf("SELECT * FROM `%s` ORDER BY `%s` LIMIT ? OFFSET ?", table, orderByColumn)
		if connDbSource.DbType == entity.DBTypeSQLServer {
			dataQuery = fmt.Sprintf("SELECT * FROM [%s] ORDER BY [%s] OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY", table, orderByColumn)
		}
	}
	var dataRows *sql.Rows
	if filterEnabled {
		if connDbSource.DbType == entity.DBTypeSQLServer {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, "%"+columnValue+"%", offset, pageSize)
		} else {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, "%"+columnValue+"%", pageSize, offset)
		}
	} else {
		if connDbSource.DbType == entity.DBTypeSQLServer {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, offset, pageSize)
		} else {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, pageSize, offset)
		}
	}
	if err != nil {
		logx.E(ctx, "分页查询数据失败: sql: %v %v", dataQuery, err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableDataFailure), table)
	}

	defer func() {
		closeErr := dataRows.Close()
		if closeErr != nil {
			logx.E(ctx, "关闭数据查询结果失败: %v", closeErr)
		}
	}()

	columnTypes, err := dataRows.ColumnTypes()
	if err != nil {
		logx.E(ctx, "获取列类型失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyGetColumnTypeFailure), table)
	}

	// 4. 解析数据行
	for dataRows.Next() {
		rowData := make([]any, len(columns))
		// 针对 sql server 特殊处理 uniqueidentifier 进行特殊处理
		for i, colType := range columnTypes {
			if colType.DatabaseTypeName() == sqlserverUniqueidentifier {
				rowData[i] = new([]byte)
			} else {
				rowData[i] = new(sql.NullString)
			}
		}

		if err = dataRows.Scan(rowData...); err != nil {
			logx.E(ctx, "解析数据行失败: %v", err)
			return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), table)
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

		rows = append(rows, row)
	}

	if err = dataRows.Err(); err != nil {
		logx.E(ctx, "遍历数据行失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), table)
	}

	return columns, rows, total, nil
}

// CheckDbTableIsExisted 判断数据库是否可以连接并且数据表存在
func (d *DBSQLServer) CheckDbTableIsExisted(ctx context.Context, dbSource *entity.Database, table string) (bool, error) {
	var varifyTableNameSql string
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
	switch connDbSource.DbType {
	case entity.DBTypeMySQL:
		varifyTableNameSql = mysqlCheckTableNameSql
	case entity.DBTypeSQLServer:
		varifyTableNameSql = sqlServerCheckTableNameSql
	default:
		return false, errs.ErrWrapf(errs.ErrDbSourceTypeNotSupport, i18n.Translate(ctx, i18nkey.KeyDatabaseType), connDbSource.DbType)
	}

	err = dbConn.QueryRowContext(ctx, varifyTableNameSql, connDbSource.DbName, table).Scan(&tableSafety)
	if err != nil {
		logx.W(ctx, "CheckDbTableIsExisted|查询表名失败: %v, 查询的表 %v", err, table)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, errs.ErrWrapf(errs.ErrDbTableIsNotExist, i18n.Translate(ctx, i18nkey.KeyDatabaseTableInfo), connDbSource.DbName, table)
		} else if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (d *DBSQLServer) GetTopNValueV2(ctx context.Context, dbSource *entity.Database,
	robotId, dbTableBizID, embeddingVersion uint64, embeddingName string) error {
	logx.D(ctx, "GetTopNValueV2|开始获取topN值: dbSource:%v, dbtableBizId:%v", dbSource, dbTableBizID)
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
	// dbTableColumns, err := GetDBTableColumnDao().GetByTableBizID(ctx, dbSource.CorpBizID,
	// dbSource.AppBizID, dbTableBizID)
	if err != nil {
		logx.E(ctx, "GetTopNValue|获取表列信息失败: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ValueLinkTimeout)
	defer cancel()

	// 获取数据表的主键列名
	var primaryKeys []string
	primaryKeys, _ = getSQLServerPrimaryKeys(ctx, dbConn, dbTable.Name)

	selectedColumns := make([]string, 0, len(dbTableColumns))
	selectedColumnForQuery := make([]string, 0, len(dbTableColumns))
	for _, col := range dbTableColumns {
		if _, ok := sqlServerTopNDataType[col.DataType]; ok {
			selectedColumns = append(selectedColumns, col.ColumnName)
			selectedColumnForQuery = append(selectedColumnForQuery, fmt.Sprintf("[%s]", col.ColumnName))
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

	logx.I(ctx, "GetTopNValueV2|获取topN值: dbTable: %v, selectedColumns: %v, primaryColumn:%v",
		dbTable, selectedColumns, primaryColumn)

	var sqlQuery string
	sqlQuery = fmt.Sprintf("SELECT %s FROM [%s] ORDER BY [%s] OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY",
		strings.Join(selectedColumnForQuery, ","), dbTable.Name, primaryColumn)

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
		rows, err = dbConn.QueryContext(ctx, sqlQuery, offset, limit)
		if err != nil {
			logx.E(ctx, "GetTopNValue|获取数据失败: %v sqlQuery:%v, limit:%v, offset:%v",
				err, sqlQuery, limit, offset)
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
	logx.D(ctx, "GetTopNValue|get top value over cost:%v， dbTable: %v, offset:%v,  ", t1.Sub(t0).Milliseconds(), dbTable, offset)

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

func (d *DBSQLServer) QuerySqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn,
	exeSql string, sqlParams []string) ([]string, []*pb.RowData, string, error) {
	logx.I(ctx, "QuerySqlForDbSource|dbName: %v, sql:%v, sqlParams:%+v, len:%v", connDbSource.DbName, exeSql, sqlParams, len(sqlParams))
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
	defaultLimit := gox.IfElse(config.App().DbSource.DefaultSelectLimit > 0, config.App().DbSource.DefaultSelectLimit, 100)
	exeSql = strings.TrimRight(strings.TrimSpace(exeSql), ";") // 去除末尾分号
	exeSql = addSqlServerDefaultTop(exeSql, defaultLimit)
	logx.I(ctx, "exeSql:%s", exeSql)

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

func (d *DBSQLServer) QueryDBSchemas(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	return nil, errs.ErrDbSchemaNotSupport
}

// getSQLServerPrimaryKeys 获取SQL Server表的主键列名
func getSQLServerPrimaryKeys(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(sqlServerGetPrimaryKeySql, tableName)
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.W(ctx, "closeErr: %v", closeErr)
		}
	}()

	var keys []string
	for rows.Next() {
		var col string
		if err = rows.Scan(&col); err != nil {
			return nil, err
		}
		keys = append(keys, col)
	}
	return keys, nil
}
