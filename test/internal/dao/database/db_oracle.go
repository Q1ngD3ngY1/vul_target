package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"regexp"
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
	go_ora "github.com/sijms/go-ora/v2"
	"golang.org/x/net/proxy"
	"gorm.io/gorm"
)

var (
	selectRegex = regexp.MustCompile(`(?i)^\s*select\b`)
	limitRegex  = regexp.MustCompile(`(?i)fetch next[\s\w:]+rows only$`)
)

// oracleDialer 实现 go-ora 的 ContextDialer 接口
type oracleDialer struct {
	dialer proxy.Dialer
}

func (d *oracleDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	// 使用 channel 实现带超时的拨号
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
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.conn, result.err
	}
}

type DBOracle struct {
	dao Dao
}

func (d *DBOracle) GetDBConnection(ctx context.Context, connDbSource entity.DatabaseConn) (*sql.DB, error) {
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

	// 构建 Oracle 连接字符串
	// 如果是SYS用户（不区分大小写），则使用SYSDBA权限
	var options map[string]string
	if strings.EqualFold(connDbSource.Username, "SYS") {
		options = map[string]string{
			"SYSDBA": "1",
		}
	}

	connStr := go_ora.BuildUrl(
		resolvedHost,
		int(connDbSource.Port),
		connDbSource.DbName, // Service Name
		connDbSource.Username,
		connDbSource.Password,
		options,
	)

	// 检查连接字符串是否包含不安全的参数
	if err := validateDSNSecurity(ctx, connStr); err != nil {
		logx.E(ctx, "Oracle DSN security check failed: %v", err)
		return nil, err
	}

	var dbConn *sql.DB

	// 判断是否使用代理
	if config.App().DbSource.Proxy != "" {
		// 创建 SOCKS5 代理 dialer
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

		// 创建带自定义 dialer 的 connector
		connector := go_ora.NewConnector(connStr).(*go_ora.OracleConnector)
		connector.Dialer(&oracleDialer{dialer: dialer})
		dbConn = sql.OpenDB(connector)

		logx.I(ctx, "Oracle using proxy: %s:%d (via %s)", resolvedHost, connDbSource.Port, config.App().DbSource.Proxy)
	} else {
		// 直接连接
		dbConn, err = sql.Open("oracle", connStr)
		if err != nil {
			logx.E(ctx, "dao:GetDBConnection open db failed: %v", err)
			return nil, errs.ErrOpenDbSourceFail
		}
	}

	// Test the connection
	err = dbConn.PingContext(ctx)
	if err != nil {
		logx.E(ctx, "test %v connection fail: %v", connDbSource.Host, err)
		dbConn.Close()
		return nil, errs.ErrOpenDbSourceFail
	}
	return dbConn, nil
}

// ShowDatabases 对于oracle而言，无法查询有哪些数据库，只能外部输入
func (d *DBOracle) ShowDatabases(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	if connDbSource.DbName == "" {
		logx.E(ctx, "oracle db name is empty, host: %s", connDbSource.Host)
		return nil, errs.ErrDbNameIsInvalid
	}
	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		return nil, err
	}
	defer dbConn.Close()

	return []string{connDbSource.DbName}, nil
}

func (d *DBOracle) GetDBTableList(ctx context.Context, connDbSource entity.DatabaseConn, page, pageSize int) (tables []string, total int, err error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "ShowDatabases: get db connection failed %v", err)
		return nil, 0, err
	}
	defer func(db *sql.DB) {
		closeErr := db.Close()
		if closeErr != nil {
			logx.E(ctx, "ShowDatabases: close db connection failed %v", closeErr)
		}
	}(dbConn)

	query := "SELECT table_name FROM user_tables ORDER BY table_name"
	rows, err := dbConn.QueryContext(ctx, query)
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

	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return allTables[start:end], total, nil
}

func (d *DBOracle) GetTableInfo(ctx context.Context, connDbSource entity.DatabaseConn, table string) (*entity.TableInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()
	logx.I(ctx, "GetTabelInfo: host:%v port:%v, dbName :%v, table: %v", connDbSource.Host, connDbSource.Port, connDbSource.DbName, table)
	if table == "" {
		return nil, errs.ErrNameInvalid
	}

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "GetTableInfo: get db connection failed %v", err)
		return nil, err
	}
	defer func(db *sql.DB) {
		closeErr := db.Close()
		if closeErr != nil {
			logx.E(ctx, "GetTableInfo: close db connection failed %v", closeErr)
		}
	}(dbConn)

	// Check table and get row count.
	var tableSafety string
	var rowCount sql.NullInt64
	query := "SELECT table_name, num_rows FROM user_tables WHERE table_name = :tb"
	err = dbConn.QueryRowContext(ctx, query, table).Scan(&tableSafety, &rowCount)
	if err != nil {
		logx.W(ctx, "GetTableInfo: check table and rows failed db=%v table=%v err=%v info=%v", connDbSource.DbName, table, err, table)
		return nil, errs.ErrWrapf(errs.ErrDbTableIsNotExist, i18n.Translate(ctx, i18nkey.KeyDatabaseTableInfo), connDbSource.DbName, table)
	}
	preciseCount := rowCount.Int64
	if rowCount.Int64 < countMaxLimit {
		err = dbConn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s",
			d.adaptTableElement(ctx, tableSafety))).Scan(&preciseCount)
		if err != nil {
			logx.W(ctx, "GetTableInfo: count rows failed: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyCountTableRowTimeout),
				tableSafety, config.App().DbSource.MaxTableRow, config.App().DbSource.MaxTableCol)
		}
	}

	// Get table columns information.
	var rows *sql.Rows
	query = `SELECT c.column_name, c.data_type, cc.comments FROM all_tab_columns c LEFT JOIN
       all_col_comments cc ON c.table_name = cc.table_name AND c.column_name = cc.column_name WHERE  c.table_name = :tb`
	rows, err = dbConn.QueryContext(ctx, query, tableSafety)
	if err != nil {
		logx.W(ctx, "GetTableInfo: query columns info failed: err=%v table=%v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseFailure), connDbSource.DbName)
	}
	defer func(rows *sql.Rows) {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.W(ctx, "GetTableInfo: close rows failed %v", closeErr)
		}
	}(rows)
	columnInfo := make([]*entity.ColumnInfo, 0)
	for rows.Next() {
		var (
			field    string
			typeInfo string
			comment  sql.NullString
		)
		err = rows.Scan(&field, &typeInfo, &comment)
		if err != nil {
			logx.E(ctx, "GetTableInfo: parse column info failed: %v", err)
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
		logx.E(ctx, "GetTableInfo: parse column info got error: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceParsingFailed,
			i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), connDbSource.DbName)
	}
	columnCount := len(columnInfo)
	if preciseCount > int64(config.App().DbSource.MaxTableRow) || columnCount > config.App().DbSource.MaxTableCol {
		logx.W(ctx, "GetTableInfo: table is too large: table=%v row=%v col=%v", table, preciseCount, columnCount)
		return nil, errs.ErrWrapf(errs.ErrDbTableSizeInvalid, i18n.Translate(ctx, i18nkey.KeyTableSizeExceedLimit),
			table, preciseCount, columnCount, config.App().DbSource.MaxTableRow, config.App().DbSource.MaxTableCol)
	}

	// Get table comment.
	var tableComment sql.NullString
	query = "SELECT comments FROM all_tab_comments WHERE table_name = :tb"
	err = dbConn.QueryRowContext(ctx, query, tableSafety).Scan(&tableComment)
	if err != nil {
		logx.W(ctx, "GetTableInfo: query table comment failed: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableAnnotationFailure), tableSafety)
	}

	return &entity.TableInfo{
		ColumnInfo:   columnInfo,
		RowCount:     preciseCount,
		ColumnCount:  columnCount,
		TableComment: tableComment.String,
	}, nil
}

func (d *DBOracle) ListPreviewData(ctx context.Context, connDbSource entity.DatabaseConn,
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
		logx.W(ctx, "ListPreviewData: get db connection failed %v", err)
		return nil, nil, 0, err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.E(ctx, "ListPreviewData: close db connection failed: %v", closeErr)
		}
	}()

	// Query column names of given table.
	sqlColumn := fmt.Sprintf("SELECT * FROM %s OFFSET 0 ROWS FETCH NEXT 0 ROWS ONLY",
		d.adaptTableElement(ctx, table))
	rowsMeta, err := dbConn.QueryContext(ctx, sqlColumn)
	if err != nil {
		logx.E(ctx, "ListPreviewData: get table column names failed: table=%v err=%v", table, err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableFailure), table)
	}
	defer func() {
		closeErr := rowsMeta.Close()
		if closeErr != nil {
			logx.W(ctx, "ListPreviewData: close parsing column names failed: %v", closeErr)
		}
	}()
	columns, err = rowsMeta.Columns()
	logx.I(ctx, "ListPreviewData: get table column names success: %v", columns)
	if len(columns) == 0 {
		return nil, nil, 0, errs.ErrDbSourceTableEmpty
	}
	if err != nil {
		logx.E(ctx, "ListPreviewData: parse table column names failed: %v", err)
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

	query := "SELECT NVL(num_rows, 0) FROM user_tables WHERE table_name = :tb"
	err = dbConn.QueryRowContext(ctx, query, table).Scan(&total)
	if err != nil {
		logx.W(ctx, "ListPreviewData: get table rows count failed: err=%v, table=%v", err, table)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountInfoFailure), err.Error(), err, table)
	}
	logx.I(ctx, "ListPreviewData: get table rows count success: total=%v, table=%v", total, table)
	if filterEnabled {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = :tb",
			d.adaptTableElement(ctx, table), d.adaptTableElement(ctx, columnName))
		err = dbConn.QueryRowContext(ctx, countQuery, columnValue).Scan(&total)

	} else if total < countMaxLimit {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", d.adaptTableElement(ctx, table))
		err = dbConn.QueryRowContext(ctx, countQuery).Scan(&total)
	}
	if err != nil {
		logx.E(ctx, "ListPreviewData: get total row count failed: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountFailure), table)
	}

	// 获取数据表的主键列名
	var primaryKeys []string
	primaryKeys, err = d.getPrimaryKeys(ctx, dbConn, table)
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

	var dataRows *sql.Rows
	var dataQuery string
	offset := (page - 1) * pageSize
	if filterEnabled {
		dataQuery = fmt.Sprintf("SELECT * FROM %s WHERE %s LIKE :tb ORDER BY %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY",
			d.adaptTableElement(ctx, table), d.adaptTableElement(ctx, columnName),
			d.adaptTableElement(ctx, orderByColumn), offset, pageSize)
		searchPattern := "%" + columnValue + "%"
		dataRows, err = dbConn.QueryContext(ctx, dataQuery, searchPattern)
	} else {
		dataQuery = fmt.Sprintf("SELECT * FROM %s ORDER BY %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY",
			d.adaptTableElement(ctx, table), d.adaptTableElement(ctx, orderByColumn), offset, pageSize)
		dataRows, err = dbConn.QueryContext(ctx, dataQuery)
	}
	if err != nil {
		logx.E(ctx, "ListPreviewData: query data failed: sql=%v err=%v", dataQuery, err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyQueryTableDataFailure), table)
	}
	defer func() {
		closeErr := dataRows.Close()
		if closeErr != nil {
			logx.E(ctx, "ListPreviewData: close data rows failed: %v", closeErr)
		}
	}()

	for dataRows.Next() {
		rowData := make([]any, len(columns))
		for i := range rowData {
			rowData[i] = &sql.NullString{}
		}
		if err = dataRows.Scan(rowData...); err != nil {
			logx.E(ctx, "ListPreviewData: parse data row failed: %v", err)
			return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
				i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), table)
		}

		row := &pb.RowData{Values: make([]string, len(columns))}
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
		logx.E(ctx, "ListPreviewData: parse data row failed: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut,
			i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), table)
	}
	return columns, rows, total, nil
}

func (d *DBOracle) getPrimaryKeys(ctx context.Context, conn *sql.DB, table string) ([]string, error) {
	query := fmt.Sprintf("SELECT COLUMN_NAME FROM all_ind_columns WHERE table_name='%s'", table)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		logx.E(ctx, "getPrimaryKeys: query failed: table=%v err=%v", table, err)
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var col string
		if err = rows.Scan(&col); err != nil {
			logx.W(ctx, "Scan error: %v", err)
			return nil, err
		}
		keys = append(keys, col)
	}
	if err = rows.Err(); err != nil {
		logx.W(ctx, "rows.Err(): %v", err)
		return nil, err
	}
	return keys, nil
}

func (d *DBOracle) CheckDbTableIsExisted(ctx context.Context, dbSource *entity.Database, table string) (bool, error) {
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

	query := "SELECT table_name FROM user_tables WHERE table_name = :tb"
	err = dbConn.QueryRowContext(ctx, query, table).Scan(&tableSafety)
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

func (d *DBOracle) GetTopNValueV2(ctx context.Context, dbSource *entity.Database, robotId, dbTableBizID,
	embeddingVersion uint64, embeddingName string) error {
	logx.D(ctx, "GetTopNValueV2: dbSource:%v, dbtableBizId:%v", dbSource, dbTableBizID)
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
		logx.W(ctx, "GetTopNValue: get db connection failed %v", err)
		return err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.W(ctx, "GetTopNValue: close db connection failed %v", closeErr)
		}
	}()

	columnFilter := entity.ColumnFilter{
		CorpBizID:    dbSource.CorpBizID,
		AppBizID:     dbSource.AppBizID,
		DBTableBizID: dbTableBizID,
	}
	dbTableColumns, _, err := d.dao.DescribeColumnList(ctx, &columnFilter)
	if err != nil {
		logx.E(ctx, "GetTopNValue: get table column list failed: %v", err)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ValueLinkTimeout)
	defer cancel()

	var primaryKeys []string
	primaryKeys, err = d.getPrimaryKeys(ctx, dbConn, dbTable.Name)
	selectedColumns := make([]string, 0, len(dbTableColumns))
	selectedColumnForQuery := make([]string, 0, len(dbTableColumns))
	for _, col := range dbTableColumns {
		dt := strings.ToUpper(col.DataType)
		if _, ok := oracleTopNDataType[dt]; ok {
			selectedColumns = append(selectedColumns, col.ColumnName)
			selectedColumnForQuery = append(selectedColumnForQuery, fmt.Sprintf("\"%s\"", col.ColumnName))
		}
	}
	if len(selectedColumns) == 0 {
		logx.W(ctx, "GetTopNValue: no columns got for dbTable: %v", dbTable)
		return nil
	}

	primaryColumn := dbTableColumns[0].ColumnName
	if len(primaryKeys) > 0 {
		primaryColumn = primaryKeys[0]
	}
	logx.I(ctx, "GetTopNValueV2: dbTable: %v, selectedColumns: %v, primaryColumn:%v", dbTable, selectedColumns, primaryColumn)

	col2ColInfo := make(map[string]*entity.Column)
	colStats := make(map[string]*TopN)
	for _, col := range dbTableColumns {
		col2ColInfo[col.ColumnName] = col
		colStats[col.ColumnName] = NewTopN(config.App().DbSource.ValueLinkConfig.MaxTopValueNum, config.App().DbSource.ValueLinkConfig.TrimKeepSize, config.App().DbSource.ValueLinkConfig.TrimThreshold)
	}

	offset := 0
	for offset < maxCowSize {
		limit := config.App().DbSource.ValueLinkConfig.MaxTraverseRow
		if offset+limit > maxCowSize {
			limit = maxCowSize - offset
		}
		query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY",
			strings.Join(selectedColumnForQuery, ","), d.adaptTableElement(ctx, dbTable.Name),
			d.adaptTableElement(ctx, primaryColumn), offset, limit)
		var rows *sql.Rows
		rows, err = dbConn.QueryContext(ctx, query)
		if err != nil {
			logx.E(ctx, "GetTopNValue: query page data failed: err=%v sqlQuery=%v (params: limit=%v offset=%v)", err, query, limit, offset)
			return err
		}

		columns, err := rows.Columns()
		if err != nil {
			logx.E(ctx, "GetTopNValue: get columns failed: %v", err)
			return err
		}
		rowData := make([]any, len(columns))
		for i := range rowData {
			rowData[i] = new(sql.NullString)
		}
		for rows.Next() {
			if err = rows.Scan(rowData...); err != nil {
				logx.E(ctx, "GetTopNValue: scan row data failed %v", err)
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
				logx.W(ctx, "GetTopNValue: close rows data failed: %v", err)
			}
		}
	}
	t1 := time.Now()
	logx.D(ctx, "GetTopNValue|get top value over cost:%v， dbTable: %v, offset:%v,  ", t1.Sub(t0).Milliseconds(), dbTable, offset)

	for _, colName := range selectedColumns {
		dbTableColumn := col2ColInfo[colName]
		topValue := make([]string, 0, config.App().DbSource.ValueLinkConfig.MaxTopValueNum)
		var dbTableTopValues []*entity.TableTopValue
		logx.D(ctx, "GetTopNValue: colName:%+v, value:%+v", colName, colStats[colName].Top())
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
			logx.E(ctx, "GetTopNValue: create top value failed: value=%v err=%v", dbTableTopValues, err)
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

func (d *DBOracle) QuerySqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn, exeSql string, sqlParams []string) ([]string, []*pb.RowData, string, error) {
	logx.I(ctx, "QuerySqlForDbSource|dbName: %v, sql:%v, sqlParams:%+v, len:%v", connDbSource.DbName, exeSql, sqlParams, len(sqlParams))
	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.E(ctx, "QuerySqlForDbSource|get db connection failed: %v", err)
		return nil, nil, err.Error(), err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.W(ctx, "QuerySqlForDbSource: close db connection failed %v", closeErr)
		}
	}()
	anySlice := make([]any, len(sqlParams))
	for i, v := range sqlParams {
		anySlice[i] = v
	}
	// 查询语句判断是否要加limit，没有需要使用默认limit
	defaultLimit := gox.IfElse(config.App().DbSource.DefaultSelectLimit > 0, config.App().DbSource.DefaultSelectLimit, 100)
	exeSql = strings.TrimRight(strings.TrimSpace(exeSql), ";") // 去除末尾分号
	exeSql = d.addSelectLimit(exeSql, defaultLimit)
	logx.I(ctx, "QuerySqlForDbSource, exeSql: %s", exeSql)
	rows, err := dbConn.QueryContext(ctx, exeSql, anySlice...)
	if err != nil {
		logx.W(ctx, "QuerySqlForDbSource: query err: %v sqlQuery:%v, sqlParams:%v, dbSource:%v",
			err, exeSql, sqlParams, connDbSource.DbName)
		return nil, nil, err.Error(), err
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.W(ctx, "QuerySqlForDbSource: close rows failed: %v", closeErr)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		logx.E(ctx, "QuerySqlForDbSource: get column info failed: %v", err)
		return nil, nil, err.Error(), err
	}

	var values []*pb.RowData
	for rows.Next() {
		rowData := make([]any, len(columns))
		for i := range columns {
			rowData[i] = new(sql.NullString)
		}
		if err = rows.Scan(rowData...); err != nil {
			logx.E(ctx, "Scan error: %v", err)
			return nil, nil, "", err
		}
		row := &pb.RowData{Values: make([]string, len(columns))}

		for i, val := range rowData {
			if v, ok := val.(*sql.NullString); ok && v.Valid {
				row.Values[i] = v.String
			} else {
				row.Values[i] = ""
			}
		}
		values = append(values, row)
	}
	return columns, values, "", nil
}

func (d *DBOracle) QueryDBSchemas(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "ShowDatabases: get db connection failed %v", err)
		return nil, err
	}
	defer func(db *sql.DB) {
		closeErr := db.Close()
		if closeErr != nil {
			logx.E(ctx, "ShowDatabases: close db connection failed %v", closeErr)
		}
	}(dbConn)

	rows, err := dbConn.QueryContext(ctx, "SELECT username FROM all_users ORDER BY username")
	if err != nil {
		logx.E(ctx, "ShowDatabases: query failed %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.E(ctx, "ShowDatabases: close rows failed %v", closeErr)
		}
	}()

	var databases []string
	for rows.Next() {
		var dbName string
		err = rows.Scan(&dbName)
		if err != nil {
			logx.E(ctx, "ShowDatabases: parse database failed %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
		}
		databases = append(databases, dbName)
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "ShowDatabases: scan database failed %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	return databases, nil
}

func (d *DBOracle) adaptTableElement(ctx context.Context, tableElement string) string {
	if !util.HasLowerCase(tableElement) {
		return tableElement
	}

	adaptedElement := fmt.Sprintf("\"%s\"", tableElement)
	logx.I(ctx, "adaptTableElement, tableElement: %s, adaptedElement: %s", tableElement, adaptedElement)
	return adaptedElement
}

func (d *DBOracle) addSelectLimit(sql string, defaultLimit int) string {
	if !selectRegex.MatchString(strings.TrimSpace(sql)) {
		return sql
	}

	if limitRegex.MatchString(strings.TrimSpace(sql)) {
		return sql
	}

	trimmed := strings.TrimRight(sql, ";")
	trimmed = strings.TrimSpace(trimmed)

	return trimmed + fmt.Sprintf(" FETCH NEXT %d ROWS ONLY", defaultLimit)
}
