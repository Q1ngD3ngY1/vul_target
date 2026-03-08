package dao

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec"
	_ "github.com/denisenkom/go-mssqldb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	DBCorpBizID        = "corp_biz_id"
	DBAppBizID         = "app_biz_id"
	DBSourceBizID      = "db_source_biz_id"
	DBTableBizID       = "db_table_biz_id"
	DBTableColumnBizID = "db_table_column_biz_id"
	DBIsDeleted        = "is_deleted"
	DBId               = " id "
)

const (
	CountMaxLimit             = 200000
	SqlserverUniqueidentifier = "UNIQUEIDENTIFIER"
)

const (
	mysqlCountSql     = "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	SqlServerCountSql = `
    SELECT 
        SUM(p.rows) AS row_count
    FROM 
        sys.tables AS t
        INNER JOIN sys.partitions AS p ON t.object_id = p.object_id
    WHERE 
        t.name = @p1
        AND p.index_id IN (0,1)
    GROUP BY 
        t.name;
`
	mysqlGetPrimaryKeySql     = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI' ORDER BY ORDINAL_POSITION"
	SqlServerGetPrimaryKeySql = `
        SELECT k.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
          ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        WHERE t.TABLE_NAME = @p1
          AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY k.ORDINAL_POSITION;
    `
	mysqlCheckTableNameSql     = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	sqlServerCheckTableNameSql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = @p1 AND TABLE_NAME = @p2"
)

var (
	dbSourceDao             *DBSourceDao
	BandDbNamesForSqlServer = []string{"master", "model", "tempdb", "msdb", "Resource", "Monitor"}
)

const (
	DbTypeMysql     = "mysql"
	DbTypeSqlserver = "sqlserver"
)

const (
	sqlDbSourceId              = " id "
	sqlDbSourceDbName          = "db_name"
	sqlDbSourceUpdateTime      = "update_time"
	sqlDbSourceNextAction      = "next_action"
	sqlDbSourceCorpBizID       = "corp_biz_id"
	sqlDbSourceDbAppBizID      = "app_biz_id"
	sqlDbSourceReleaseStatus   = "release_status"
	sqlAddAndDelWithoutPublish = "!(next_action = 1 AND is_deleted = 1)"
)

func GetDBSourceDao() *DBSourceDao {
	if dbSourceDao == nil {
		dbSourceDao = &DBSourceDao{db: globalBaseDao.tdsqlGormDB}
	}
	return dbSourceDao
}

type DBSourceDao struct {
	db *gorm.DB
}

// CheckDbNameIsBanned 判断数据库名称是否被禁止添加
func (r *DBSourceDao) CheckDbNameIsBanned(dbName, dbType string) error {
	if dbType == DbTypeSqlserver {
		for _, name := range BandDbNamesForSqlServer {
			if name == dbName {
				return errs.ErrDbNameBanned
			}
		}
	}
	return nil
}

// Create 新增
func (r *DBSourceDao) Create(ctx context.Context, source *model.DBSource) error {
	err := r.db.WithContext(ctx).Create(source).Error
	if err != nil {
		log.ErrorContextf(ctx, "create db error, %v", err)
		return err
	}
	return nil
}

// GetByBizID 通过业务ID获取
func (r *DBSourceDao) GetByBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64) (*model.DBSource, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var source model.DBSource
	err := r.db.WithContext(ctx).Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, dbSourceBizID).First(&source).Error
	if err != nil {
		log.ErrorContextf(ctx, "get db source by biz id failed: %v", err)
		return nil, err
	}
	return &source, nil
}

// GetDbSourceNumByAppBizID 获取应用下数据源数量
func (r *DBSourceDao) GetDbSourceNumByAppBizID(ctx context.Context, corpBizID, appBizID uint64) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&model.DBSource{}).Where("corp_biz_id = ? AND app_biz_id = ? AND is_deleted = 0", corpBizID, appBizID).Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "get db source by biz id failed: %v", err)
		return 0, err
	}
	return count, nil
}

// GetDbSourceBizIdByAppBizID 获取应用下的数据源BizId
func (r *DBSourceDao) GetDbSourceBizIdByAppBizID(ctx context.Context, corpBizID, appBizID uint64) ([]uint64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}

	db := r.db.
		WithContext(ctx).
		Model(&model.DBSource{}).
		Select("db_source_biz_id").
		Where("corp_biz_id = ? AND app_biz_id = ? AND is_deleted = 0", corpBizID, appBizID)
	var dbSourceBizIds []uint64
	if err := db.Find(&dbSourceBizIds).Error; err != nil {
		log.ErrorContextf(ctx, "GetDbSourceBizIdByAppBizID Find fail: %v", err)
		return nil, err
	}
	return dbSourceBizIds, nil
}

// GetDbSourceBizIdByAppBizIDWithReleaseStatus 获取应用下的数据源BizId,且包含过滤发布状态
func (r *DBSourceDao) GetDbSourceBizIdByAppBizIDWithReleaseStatus(ctx context.Context, corpBizID, appBizID uint64,
	releaseStatus []int) ([]uint64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}

	db := r.db.
		WithContext(ctx).
		Model(&model.DBSource{}).
		Select("db_source_biz_id").
		Where("corp_biz_id = ? AND app_biz_id = ? AND is_deleted = 0 AND is_indexed = 1 AND release_status IN ?", corpBizID, appBizID, releaseStatus)
	var dbSourceBizIds []uint64
	if err := db.Find(&dbSourceBizIds).Error; err != nil {
		log.ErrorContextf(ctx, "GetDbSourceBizIdByAppBizIDWithReleaseStatus Find fail: %v", err)
		return nil, err
	}
	return dbSourceBizIds, nil
}

type ListDBSourcesOption struct {
	CorpBizID uint64
	AppBizID  uint64
	IDOrName  string // 可选参数，为空时不按DB名过滤
	IsEnable  *bool  // 为nil的时候不生效
	Page      int    // 必填，从1开始
	PageSize  int    // 必填
}

// ListByOption 统一的数据源查询函数
func (r *DBSourceDao) ListByOption(ctx context.Context, opt *ListDBSourcesOption) ([]*model.DBSource, int64, error) {
	// 参数校验
	if opt.CorpBizID == 0 || opt.AppBizID == 0 {
		return nil, 0, errs.ErrParameterInvalid
	}

	var (
		sources []*model.DBSource
		total   int64
	)

	// 构建基础查询条件
	db := r.db.WithContext(ctx).Model(&model.DBSource{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND is_deleted = 0",
			opt.CorpBizID, opt.AppBizID)

	// 如果传入了DB名，添加DB名过滤条件
	if opt.IDOrName != "" {
		db = db.Where("(alias_name LIKE ? OR db_source_biz_id = ?)",
			"%"+opt.IDOrName+"%", opt.IDOrName)
	}

	// 如果传入了启用状态，添加过滤条件
	if opt.IsEnable != nil {
		indexed := 0
		if *opt.IsEnable {
			indexed = 1
		}
		db = db.Where("is_indexed = ?", indexed)
	}

	// 计算总数
	if err := db.Count(&total).Error; err != nil {
		log.ErrorContextf(ctx, "ListByOption count failed: %v", err)
		return nil, 0, err
	}

	// 分页查询
	offset := (opt.Page - 1) * opt.PageSize
	err := db.Order("id DESC").Offset(offset).Limit(opt.PageSize).Find(&sources).Error
	if err != nil {
		log.ErrorContextf(ctx, "ListByOption find failed: %v", err)
		return nil, 0, err
	}

	return sources, total, nil
}

// UpdateByBizID 通过业务ID更新（部分字段）
func (r *DBSourceDao) UpdateByBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64,
	updateColumns []string, dbSource *model.DBSource) error {
	res := r.db.WithContext(ctx).Model(&model.DBSource{}).
		Select(updateColumns).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0",
			corpBizID, appBizID, dbSourceBizID).
		Updates(dbSource)

	if res.Error != nil {
		log.ErrorContextf(ctx, "update db source failed: %v", res.Error)
		return res.Error
	}
	return nil
}

// SoftDeleteByBizID 软删除
func (r *DBSourceDao) SoftDeleteByBizID(ctx context.Context, corpBizId, appBizID, dbSourceBizID uint64) error {
	res := r.db.WithContext(ctx).Model(&model.DBSource{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0",
			corpBizId, appBizID, dbSourceBizID).
		Updates(map[string]interface{}{
			"is_deleted":  1,
			"next_action": model.ReleaseActionDelete,
			"release_status": gorm.Expr(`CASE WHEN next_action != ? THEN ? ELSE release_status END`,
				model.ReleaseActionAdd, model.ReleaseStatusUnreleased),
		})
	if res.Error != nil {
		log.ErrorContextf(ctx, "soft delete db source failed: %v", res.Error)
		return res.Error
	}
	return nil
}

func (r *DBSourceDao) BatchGetDbSources(ctx context.Context, appBizId uint64, dbSourceBizIDs []uint64) ([]*model.DBSource, error) {
	var dbSources []*model.DBSource
	err := r.db.WithContext(ctx).Model(&model.DBSource{}).
		Where("app_biz_id = ? AND db_source_biz_id IN (?) AND is_deleted = 0", appBizId, dbSourceBizIDs).
		Find(&dbSources).Error
	if err != nil {
		log.ErrorContextf(ctx, "get db source list failed: %v", err)
		return nil, err
	}
	return dbSources, nil
}

// ------------------- 外部数据库相关接口 -----------------------

func (r *DBSourceDao) GetDBConnection(ctx context.Context, connDbSource model.ConnDbSource) (*sql.DB, error) {
	var dsn string

	connDbSource.Host = strings.TrimSpace(connDbSource.Host)
	connDbSource.Username = strings.TrimSpace(connDbSource.Username)

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.TestConnTimeout)
	defer cancel()

	switch connDbSource.DbType {
	case DbTypeMysql:
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
			connDbSource.Username, connDbSource.Password, connDbSource.Host, connDbSource.Port, connDbSource.DbName, config.App().DbSource.DsnConfigMysql)
	case DbTypeSqlserver:
		dsn = fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&%s",
			connDbSource.Username, url.QueryEscape(connDbSource.Password), connDbSource.Host, connDbSource.Port, connDbSource.DbName, config.App().DbSource.DsnConfigSqlServer)
	default:
		log.WarnContextf(ctx, "unsupported db_type: %s", connDbSource.DbType)
		return nil, errs.ErrOpenDbSourceFail
	}
	connDbSource.Password = ""

	dbConn, err := sql.Open(connDbSource.DbType, dsn)
	if err != nil {
		log.WarnContextf(ctx, "GetDBConnection|connDbSource:%v open failed: %v", connDbSource, err)
		return nil, errs.ErrOpenDbSourceFail
	}

	// Test the connection
	var result int
	err = dbConn.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		log.WarnContextf(ctx, "GetDBConnection| test db connection failed: %v, conn: %v", err, connDbSource)
		return nil, errs.ErrOpenDbSourceFail
	}

	return dbConn, nil
}

// GetDBList 获取数据源中的数据库名称列表
func (r *DBSourceDao) GetDBList(ctx context.Context, connDbSource model.ConnDbSource) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()

	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "GetDBList|获取数据库连接失败: %v", err)
		return nil, err
	}

	defer func(db *sql.DB) {
		closeErr := db.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}(dbConn)

	var query string
	switch connDbSource.DbType {
	case DbTypeMysql:
		query = "SHOW DATABASES"
	case DbTypeSqlserver:
		query = "SELECT name FROM sys.databases"
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}

	rows, err := dbConn.QueryContext(ctx, query)
	if err != nil {
		log.ErrorContextf(ctx, "查询数据库列表失败: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "关闭查询结果失败: %v", closeErr)
		}
	}()

	var databases []string
	for rows.Next() {
		var dbName string
		err = rows.Scan(&dbName)
		if err != nil {
			log.ErrorContextf(ctx, "解析数据库名称失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
		}
		databases = append(databases, dbName)
	}

	if err = rows.Err(); err != nil {
		log.ErrorContextf(ctx, "遍历数据库列表失败: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	return databases, nil
}

// GetDBTableList 获取数据源下的 table 列表
func (r *DBSourceDao) GetDBTableList(
	ctx context.Context,
	connDbSource model.ConnDbSource,
	page int,
	pageSize int,
) (tables []string, total int, err error) {
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()
	db, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "GetDBTableList|获取数据库连接失败: %v", err)
		return nil, 0, err
	}

	defer func() {
		closeErr := db.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()

	var query string
	switch connDbSource.DbType {
	case DbTypeMysql:
		query = "SHOW TABLES"
	case DbTypeSqlserver:
		query = "SELECT name FROM sys.tables"
	default:
		return nil, 0, errs.ErrDbSourceTypeNotSupport
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.ErrorContextf(ctx, "query db failed: %v", err)
		return nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseListFailure), connDbSource.DbName)
	}

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "close rows failed: %v", closeErr)
		}
	}()

	var allTables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			log.ErrorContextf(ctx, "scan row failed: %v", err)
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
func (r *DBSourceDao) GetTableInfo(
	ctx context.Context,
	connDbSource model.ConnDbSource,
	table string,
) (*model.TableInfo, error) {

	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ReadConnTimeout)
	defer cancel()
	log.InfoContextf(ctx, "GetTabelInfo: host:%v port:%v, dbName :%v, table: %v", connDbSource.Host, connDbSource.Port, connDbSource.DbName, table)

	if table == "" {
		return nil, errs.ErrNameInvalid
	}

	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "GetTableInfo|获取数据库连接失败: %v", err)
		return nil, err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()

	var VarifyTableNameSql string
	var tableSafety string

	switch connDbSource.DbType {
	case DbTypeMysql:
		VarifyTableNameSql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	case DbTypeSqlserver:
		VarifyTableNameSql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = @p1 AND TABLE_NAME = @p2"
	}

	err = dbConn.QueryRowContext(ctx, VarifyTableNameSql, connDbSource.DbName, table).Scan(&tableSafety)
	if err != nil {
		log.WarnContextf(ctx, "查询表名失败: %v, 查询的表 %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbTableIsNotExist, i18n.Translate(ctx, i18nkey.KeyDatabaseTableInfo), connDbSource.DbName, tableSafety)
	}

	// 3.获取行数,  获取表注解
	var rowCount int64

	switch connDbSource.DbType {
	case DbTypeMysql:
		err = dbConn.QueryRowContext(ctx, mysqlCountSql, connDbSource.DbName, table).Scan(&rowCount)
	case DbTypeSqlserver:

		err = dbConn.QueryRowContext(ctx, SqlServerCountSql, table).Scan(&rowCount)
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	if err != nil {
		log.WarnContextf(ctx, "查询表行数信息失败: %v, 查询的表 %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountInfoFailure), err.Error(), tableSafety)
	}

	// 1. 获取列信息
	var rows *sql.Rows
	switch connDbSource.DbType {
	case DbTypeMysql:
		rows, err = dbConn.QueryContext(ctx, "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", connDbSource.DbName, tableSafety)
	case DbTypeSqlserver:
		rows, err = dbConn.QueryContext(ctx, "SELECT c.name AS COLUMN_NAME, t.name AS DATA_TYPE, ep.value AS COLUMN_COMMENT FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description' WHERE c.object_id = OBJECT_ID(@p1)", tableSafety)
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	if err != nil {
		log.WarnContextf(ctx, "查询表列信息失败: %v, 查询的表 %v", err, tableSafety)
		return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryDatabaseFailure), connDbSource.DbName)
	}
	defer func(rows *sql.Rows) {
		closeErr := rows.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "关闭列名查询结果失败: %v", closeErr)
		}
	}(rows)

	columnInfo := make([]*model.ColumnInfo, 0)
	// 解析列信息
	for rows.Next() {
		var (
			field    string
			typeInfo string
			comment  sql.NullString
		)
		err = rows.Scan(&field, &typeInfo, &comment)
		if err != nil {
			log.ErrorContextf(ctx, "解析列信息失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceParsingFailed, i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), connDbSource.DbName)
		}
		columnInfo = append(columnInfo, &model.ColumnInfo{
			ColumnName: field,
			DataType:   typeInfo,
			ColComment: comment.String,
		})
	}

	if err = rows.Err(); err != nil {
		log.ErrorContextf(ctx, "遍历列信息失败: %v", err)
		return nil, errs.ErrWrapf(errs.ErrDbSourceParsingFailed, i18n.Translate(ctx, i18nkey.KeyParseColumnInfoFailure), connDbSource.DbName)
	}

	// 3.获取行数,  获取表注解
	var (
		tableComment sql.NullString
	)

	switch connDbSource.DbType {
	case DbTypeMysql:
		// 如果行数足够小，则直接查询行数
		if rowCount < CountMaxLimit {
			// 查询行数
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableSafety)
			err = dbConn.QueryRowContext(ctx, countQuery).Scan(&rowCount)
			if err != nil {
				log.WarnContextf(ctx, "统计表行数失败: %v, 数据表过大", err)
				return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyCountTableRowTimeout),
					tableSafety, config.App().DbSource.MaxTableRow, config.App().DbSource.MaxTableCol)
			}
		}

		// 查询表注释
		commentQuery := "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =? and TABLE_NAME = ?"
		err = dbConn.QueryRowContext(ctx, commentQuery, connDbSource.DbName, tableSafety).Scan(&tableComment)
		if err != nil {
			log.WarnContextf(ctx, "查询表注解失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableAnnotationFailure), tableSafety)
		}
	case DbTypeSqlserver:
		// 如果行数足够小，则直接查询行数
		if rowCount < CountMaxLimit {
			// 查询行数
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableSafety)
			err = dbConn.QueryRowContext(ctx, countQuery).Scan(&rowCount)
			if err != nil {
				log.WarnContextf(ctx, "查询表行数失败: %v", err)
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
			log.WarnContextf(ctx, "查询表注解失败: %v", err)
			return nil, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableAnnotationFailure), table)
		}
	default:
		return nil, errs.ErrDbSourceTypeNotSupport
	}

	columnCount := len(columnInfo)

	// 4. 判断表格大小是否超出范围
	if rowCount > int64(config.App().DbSource.MaxTableRow) || columnCount > config.App().DbSource.MaxTableCol {
		log.WarnContextf(ctx, "表格大小超出了限定范围，表格名称: %v， row: %v col : %v", table, rowCount, columnCount)
		return nil, errs.ErrWrapf(errs.ErrDbTableSizeInvalid, i18n.Translate(ctx, i18nkey.KeyTableSizeExceedLimit),
			table, rowCount, columnCount, config.App().DbSource.MaxTableRow, config.App().DbSource.MaxTableCol)
	}

	return &model.TableInfo{
		ColumnInfo:   columnInfo,
		RowCount:     rowCount,
		ColumnCount:  columnCount,
		TableComment: tableComment.String,
	}, nil
}

// ListPreviewData 查询预览数据
func (r *DBSourceDao) ListPreviewData(ctx context.Context, connDbSource model.ConnDbSource,
	table string, page, pageSize int, columnName string, columnValue string, timeout time.Duration) (columns []string, rows []*pb.RowData, total int64, err error) {
	log.InfoContextf(ctx, "ListPreviewData: DbType:%v, Host:%v, DbName:%v, table:%v, columnName:%v,columnValue:%v",
		connDbSource.DbType, connDbSource.Host, connDbSource.DbName, table, columnName, columnValue)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if table == "" {
		return nil, nil, 0, errs.ErrNameInvalid
	}

	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "ListPreviewData|获取数据库连接失败: %v", err)
		return nil, nil, 0, err
	}

	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()

	sqlColumn := fmt.Sprintf("SELECT * FROM `%s` LIMIT 0", table)
	if connDbSource.DbType == DbTypeSqlserver {
		sqlColumn = fmt.Sprintf("SELECT TOP 0 * FROM [%s]", table)
	}
	rowsMeta, err := dbConn.QueryContext(ctx, sqlColumn)
	if err != nil {
		log.ErrorContextf(ctx, "查询表列名失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableFailure), table)
	}

	defer func() {
		closeErr := rowsMeta.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "关闭列名查询结果失败: %v", closeErr)
		}
	}()

	columns, err = rowsMeta.Columns()
	log.InfoContextf(ctx, "查询列名成功: %v", columns)
	if len(columns) == 0 {
		return nil, nil, 0, errs.ErrDbSourceTableEmpty
	}
	if err != nil {
		log.ErrorContextf(ctx, "解析列名失败: %v", err)
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
			return nil, nil, 0, i18n.Newf(4720023, "列名 '%s' 不存在", columnName)
		}
	}

	// 获取行数,  获取表注解
	switch connDbSource.DbType {
	case DbTypeMysql:
		err = dbConn.QueryRowContext(ctx, mysqlCountSql, connDbSource.DbName, table).Scan(&total)
	case DbTypeSqlserver:
		err = dbConn.QueryRowContext(ctx, SqlServerCountSql, table).Scan(&total)
	default:
		return nil, nil, 0, errs.ErrDbSourceTypeNotSupport
	}
	if err != nil {
		log.WarnContextf(ctx, "查询表行数信息失败: %v, 查询的表 %v", err, table)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountInfoFailure), err.Error(), table)
	}

	// 2. 查询总行数（带筛选条件）
	var countQuery string
	if filterEnabled {
		countQuery = fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE `%s` = ?", table, columnName)
		if connDbSource.DbType == DbTypeSqlserver {
			countQuery = fmt.Sprintf("SELECT COUNT(*) FROM [%s] WHERE [%s] = @p1", table, columnName)
		}
		err = dbConn.QueryRowContext(ctx, countQuery, columnValue).Scan(&total)
	} else if total < CountMaxLimit {
		countQuery = fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		if connDbSource.DbType == DbTypeSqlserver {
			countQuery = fmt.Sprintf("SELECT COUNT(*) FROM [%s]", table)
		}
		err = dbConn.QueryRowContext(ctx, countQuery).Scan(&total)
	}
	if err != nil {
		log.ErrorContextf(ctx, "查询总行数失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableRowCountFailure), table)
	}

	// 获取数据表的主键列名
	var primaryKeys []string
	switch expr := connDbSource.DbType; expr {
	case DbTypeMysql:
		primaryKeys, _ = getMySQLPrimaryKeys(ctx, dbConn, connDbSource.DbName, table)
	case DbTypeSqlserver:
		primaryKeys, _ = getSQLServerPrimaryKeys(ctx, dbConn, table)
	default:
		return nil, nil, 0, errs.ErrDbSourceTypeNotSupport
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
	log.DebugContextf(ctx, "ListPreviewData｜conn：%+v, primaryKeys: %+v", connDbSource.DbName, primaryKeys)

	// 3. 分页查询数据（带筛选条件）
	offset := (page - 1) * pageSize
	var dataQuery string
	if filterEnabled {
		dataQuery = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` LIKE ? ORDER BY `%s` LIMIT ? OFFSET ?", table, columnName, orderByColumn)
		if connDbSource.DbType == DbTypeSqlserver {
			dataQuery = fmt.Sprintf("SELECT * FROM [%s] WHERE [%s] LIKE @p1 ORDER BY [%s] OFFSET @p2 ROWS FETCH NEXT @p3 ROWS ONLY", table, columnName, orderByColumn)
		}
	} else {
		dataQuery = fmt.Sprintf("SELECT * FROM `%s` ORDER BY `%s` LIMIT ? OFFSET ?", table, orderByColumn)
		if connDbSource.DbType == DbTypeSqlserver {
			dataQuery = fmt.Sprintf("SELECT * FROM [%s] ORDER BY [%s] OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY", table, orderByColumn)
		}
	}
	var dataRows *sql.Rows
	if filterEnabled {
		if connDbSource.DbType == DbTypeSqlserver {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, "%"+columnValue+"%", offset, pageSize)
		} else {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, "%"+columnValue+"%", pageSize, offset)
		}
	} else {
		if connDbSource.DbType == DbTypeSqlserver {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, offset, pageSize)
		} else {
			dataRows, err = dbConn.QueryContext(ctx, dataQuery, pageSize, offset)
		}
	}
	if err != nil {
		log.ErrorContextf(ctx, "分页查询数据失败: sql: %v %v", dataQuery, err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyQueryTableDataFailure), table)
	}

	defer func() {
		closeErr := dataRows.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "关闭数据查询结果失败: %v", closeErr)
		}
	}()

	columnTypes, err := dataRows.ColumnTypes()
	if err != nil {
		log.ErrorContextf(ctx, "获取列类型失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyGetColumnTypeFailure), table)
	}

	// 4. 解析数据行
	for dataRows.Next() {
		rowData := make([]interface{}, len(columns))
		// 针对 sql server 特殊处理 uniqueidentifier 进行特殊处理
		for i, colType := range columnTypes {
			if colType.DatabaseTypeName() == SqlserverUniqueidentifier {
				rowData[i] = new([]byte)
			} else {
				rowData[i] = new(sql.NullString)
			}
		}

		if err = dataRows.Scan(rowData...); err != nil {
			log.ErrorContextf(ctx, "解析数据行失败: %v", err)
			return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), table)
		}

		row := &pb.RowData{
			Values: make([]string, len(columns)),
		}

		for i, val := range rowData {
			// 针对 sql server 特殊处理 uniqueidentifier 进行特殊处理
			if columnTypes[i].DatabaseTypeName() == SqlserverUniqueidentifier {
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
		log.ErrorContextf(ctx, "遍历数据行失败: %v", err)
		return nil, nil, 0, errs.ErrWrapf(errs.ErrDbSourceTimeOut, i18n.Translate(ctx, i18nkey.KeyParseDataRowFailure), table)
	}

	return columns, rows, total, nil
}

// bytesToGUIDString 把 16 字节转成标准 GUID 字符串
func bytesToGUIDString(b []byte) string {
	if len(b) != 16 {
		return ""
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.LittleEndian.Uint32(b[0:4]),
		binary.LittleEndian.Uint16(b[4:6]),
		binary.LittleEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16],
	)
}

func (r *DBSourceDao) CollectUnreleasedDBSource(ctx context.Context, appBizID, releaseBizID uint64) error {
	// 1. 找出所有 release_status 为1 待发布的 t_db_source
	dbSources, err := r.GetUnreleasedDBSource(ctx, appBizID)
	if err != nil {
		return err
	}
	if len(dbSources) == 0 {
		return nil
	}

	corpBizID := dbSources[0].CorpBizID

	// 2. 补充releaseBizID，并写入到t_release_db_source
	for _, chunk := range slicex.Chunk(dbSources, 100) {
		var releaseChunk []*model.ReleaseDBSource
		var ids []uint64
		for _, d := range chunk {
			ids = append(ids, d.DBSourceBizID)
			releaseDBSource := &model.ReleaseDBSource{
				CorpBizID:     d.CorpBizID,
				AppBizID:      d.AppBizID,
				DBSourceBizID: d.DBSourceBizID,
				DBName:        d.DBName,
				AliasName:     d.AliasName,
				Description:   d.AliasName,
				DBType:        d.DBType,
				Host:          d.Host,
				Port:          d.Port,
				Username:      d.Username,
				Password:      d.Password,
				Alive:         d.Alive,
				LastSyncTime:  d.LastSyncTime,
				ReleaseStatus: d.ReleaseStatus,
				ReleaseBizID:  releaseBizID,
				Action:        d.NextAction,
				IsDeleted:     d.IsDeleted,
			}
			releaseChunk = append(releaseChunk, releaseDBSource)
		}
		err = r.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "corp_biz_id"},
				{Name: "app_biz_id"},
				{Name: "db_source_biz_id"},
				{Name: "release_biz_id"},
			},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"db_name":        gorm.Expr("VALUES(db_name)"),
				"alias_name":     gorm.Expr("VALUES(alias_name)"),
				"description":    gorm.Expr("VALUES(description)"),
				"db_type":        gorm.Expr("VALUES(db_type)"),
				"host":           gorm.Expr("VALUES(host)"),
				"port":           gorm.Expr("VALUES(port)"),
				"username":       gorm.Expr("VALUES(username)"),
				"password":       gorm.Expr("VALUES(password)"),
				"alive":          gorm.Expr("VALUES(alive)"),
				"last_sync_time": gorm.Expr("VALUES(last_sync_time)"),
				"release_status": gorm.Expr("VALUES(release_status)"),
				"action":         gorm.Expr("VALUES(action)"),
				"is_deleted":     gorm.Expr("VALUES(is_deleted)"),
			}),
		}).Create(releaseChunk).Error
		if err != nil {
			log.ErrorContextf(ctx, "create release db source error, %v", err)
			return err
		}

		// 3. 更新db_source表的状态为待发布
		err = r.db.WithContext(ctx).Model(&model.DBSource{}).
			Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id IN (?)",
				corpBizID, appBizID, ids).
			Updates(map[string]interface{}{
				"release_status": model.ReleaseStatusReleasing,
			}).Error
		if err != nil {
			log.ErrorContextf(ctx, "update db source releasing error, id: %+v, %v", ids, err)
			return err
		}
	}

	return nil
}

// GetUnreleasedDBSource 获取所有的待发布的db source表记录
func (r *DBSourceDao) GetUnreleasedDBSource(ctx context.Context, appBizID uint64) ([]*model.DBSource, error) {
	limit := 200
	startID := uint64(0)
	var all []*model.DBSource
	for {
		var chunk []*model.DBSource
		// 排除是新增，但是未发布的情况下又删除的数据库
		err := r.db.WithContext(ctx).Where("app_biz_id = ? AND release_status = 1 AND "+
			"!(next_action = 1 AND is_deleted = 1) AND id > ?", appBizID, startID).
			Order("id ASC").Limit(limit).Find(&chunk).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetUnreleasedDBSource error, %v", err)
			return nil, err
		}
		all = append(all, chunk...)
		if len(chunk) < limit {
			break
		}
		startID = chunk[len(chunk)-1].ID
	}
	log.InfoContextf(ctx, "GetUnreleasedDBSource len: %v", len(all))
	return all, nil
}

// FindUnReleaseDBSourceByConditions 根据 DBName、UpdateTime 和 NextAction 进行多条件检索
func (r *DBSourceDao) FindUnReleaseDBSourceByConditions(ctx context.Context, corpBizID, appBizID uint64, dbName string, beginTime,
	endTime time.Time, nextAction []uint32, page, pageSize uint32) ([]*model.DBSource, error) {
	var sources []*model.DBSource
	query := r.db.Model(&model.DBSource{})
	query = query.Where(sqlDbSourceCorpBizID+sqlEqual, corpBizID)
	query = query.Where(sqlDbSourceDbAppBizID+sqlEqual, appBizID)

	if dbName != "" {
		query = query.Where(sqlDbSourceDbName+sqlLike, fmt.Sprintf("%%%s%%", dbName))
	}
	if !beginTime.IsZero() {
		query = query.Where(sqlDbSourceUpdateTime+sqlMoreEqual, beginTime)
	}
	if !endTime.IsZero() {
		query = query.Where(sqlDbSourceUpdateTime+sqlLessEqual, endTime)
	}

	if len(nextAction) > 0 {
		query = query.Where(sqlDbSourceNextAction+sqlIn, nextAction)
	}
	query = query.Where(sqlDbSourceReleaseStatus+sqlEqual, model.ReleaseStatusUnreleased)
	query = query.Where(sqlAddAndDelWithoutPublish)
	query = query.Limit(int(pageSize)).Offset(int(pageSize * (page - 1)))
	query = query.Order(sqlDbSourceId + SqlOrderByDesc)

	if err := query.Find(&sources).Error; err != nil {
		log.ErrorContextf(ctx, "FindDBSourceByConditions| appBizID:%v, dbName:%v, beginTime:%v, endTime:%v, nextAction:%v, page:%v, pageSize:%v",
			appBizID, dbName, beginTime, endTime, nextAction, page, pageSize)
		return nil, err
	}
	return sources, nil
}

// GetAllReleaseDBSources 获取db source的快照信息
func (r *DBSourceDao) GetAllReleaseDBSources(ctx context.Context, appBizID, releaseBizID uint64) ([]*model.ReleaseDBSource, error) {
	limit := 200
	startID := uint64(0)
	var all []*model.ReleaseDBSource
	for {
		var chunk []*model.ReleaseDBSource
		// 排除是新增，但是未发布的情况下又删除的数据库
		err := r.db.WithContext(ctx).Where("app_biz_id = ? AND release_biz_id = ? AND id > ?",
			appBizID, releaseBizID, startID).Order("id ASC").Limit(limit).Find(&chunk).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetAllReleaseDBSources error, %v", err)
			return nil, err
		}
		all = append(all, chunk...)
		if len(chunk) < limit {
			break
		}
		startID = chunk[len(chunk)-1].ID
	}
	log.InfoContextf(ctx, "GetAllReleaseDBSources len: %v", len(all))
	return all, nil
}

// ReleaseDBSource 发布db source
func (r *DBSourceDao) ReleaseDBSource(ctx context.Context, appBizID, releaseBizID uint64) error {
	releaseDBSources, err := r.GetAllReleaseDBSources(ctx, appBizID, releaseBizID)
	if err != nil {
		return err
	}
	// 2. 补充releaseBizID，并写入到t_release_db_source 的
	var dbSourceBizIDs []uint64
	for _, chunk := range slicex.Chunk(releaseDBSources, 100) {
		corpBizID := chunk[0].CorpBizID
		var dbSourceChunk []*model.ProdDBSource
		for _, d := range chunk {
			dbSource := &model.ProdDBSource{
				CorpBizID:     d.CorpBizID,
				AppBizID:      d.AppBizID,
				DBSourceBizID: d.DBSourceBizID,
				DBName:        d.DBName,
				AliasName:     d.AliasName,
				Description:   d.Description,
				DBType:        d.DBType,
				Host:          d.Host,
				Port:          d.Port,
				Username:      d.Username,
				Password:      d.Password,
				Alive:         d.Alive,
				LastSyncTime:  d.LastSyncTime,
				IsDeleted:     d.IsDeleted,
			}
			dbSourceChunk = append(dbSourceChunk, dbSource)
			dbSourceBizIDs = append(dbSourceBizIDs, d.DBSourceBizID)
		}
		// 1. 将release表复制到prod表
		err = r.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "corp_biz_id"},
				{Name: "app_biz_id"},
				{Name: "db_source_biz_id"},
			},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"db_name":        gorm.Expr("VALUES(db_name)"),
				"alias_name":     gorm.Expr("VALUES(alias_name)"),
				"description":    gorm.Expr("VALUES(description)"),
				"db_type":        gorm.Expr("VALUES(db_type)"),
				"host":           gorm.Expr("VALUES(host)"),
				"port":           gorm.Expr("VALUES(port)"),
				"username":       gorm.Expr("VALUES(username)"),
				"password":       gorm.Expr("VALUES(password)"),
				"alive":          gorm.Expr("VALUES(alive)"),
				"last_sync_time": gorm.Expr("VALUES(last_sync_time)"),
				"is_deleted":     gorm.Expr("VALUES(is_deleted)"),
			}),
		}).Create(dbSourceChunk).Error
		if err != nil {
			log.ErrorContextf(ctx, "ReleaseDBSource|Create appBizID: %v, releaseBizID: %v error, %v", appBizID, releaseBizID, err)
			return err
		}

		// 2. 修改评测端的表发布状态
		err = r.db.WithContext(ctx).Model(&model.DBSource{}).Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id in (?)",
			corpBizID, appBizID, dbSourceBizIDs).Updates(map[string]interface{}{
			"release_status": model.ReleaseStatusReleased,
			"next_action":    model.ReleaseActionPublish,
		}).Error
		if err != nil {
			log.ErrorContextf(ctx, "update release status error, %v", err)
			return err
		}
	}

	return nil
}

// CheckDbSourceField  审核用户自定义的数据库描述
func (r *DBSourceDao) CheckDbSourceField(ctx context.Context, appBizId, dbSourceBizId uint64, source, content string, d Dao) error {
	if len(content) == 0 {
		return nil
	}
	t0 := time.Now()
	auditReq := &infosec.CheckReq{
		User: &infosec.CheckReq_User{
			AccountType: model.AccountTypeOther,
			Uin:         fmt.Sprintf("%d", appBizId),
		},
		Id:       fmt.Sprintf("%d", dbSourceBizId),
		PostTime: time.Now().Unix(),
		Source:   source,
		Type:     model.AuditTypePlainText,
		Content:  content,
		BizType:  model.AuditDbSourceCheckBizType,
	}

	req, err := infosec.NewInfosecClientProxy().Check(ctx, auditReq)
	if err != nil {
		log.ErrorContextf(ctx, "请求送审失败 req:%+v err:%+v", auditReq, err)
		return err
	}
	if req.GetResultCode() == model.AuditResultFail {
		log.InfoContext(ctx, "CheckDbSourceName|appBizId:%d dbSourceBizId:%d content:%s", appBizId, dbSourceBizId, content)
		return errs.ErrInvalidFields
	}
	t1 := time.Now()
	log.InfoContextf(ctx, "CheckDbSourceField|cost:%v", t1.Sub(t0).Milliseconds())
	return nil
}

// getMySQLPrimaryKeys 获取MySQL表的主键列名
func getMySQLPrimaryKeys(ctx context.Context, db *sql.DB, dbName, tableName string) ([]string, error) {
	rows, err := db.Query(mysqlGetPrimaryKeySql, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "closeErr: %v", closeErr)
		}
	}()

	var keys []string
	for rows.Next() {
		var col string
		if err = rows.Scan(&col); err != nil {
			log.WarnContextf(ctx, "Scan error: %v", err)
			return nil, err
		}
		keys = append(keys, col)
	}
	if err = rows.Err(); err != nil {
		log.WarnContextf(ctx, "rows.Err(): %v", err)
		return nil, err
	}
	return keys, nil
}

// getSQLServerPrimaryKeys 获取SQL Server表的主键列名
func getSQLServerPrimaryKeys(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(SqlServerGetPrimaryKeySql, tableName)
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "closeErr: %v", closeErr)
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

func (r *DBSourceDao) CountByBizIDAndStatus(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0 AND learn_status = 2",
			corpBizID, appBizID, dbSourceBizID).
		Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "count db table by biz id and status failed: %v", err)
		return 0, err
	}
	return count, nil
}

// todo 性能优化
func (r *DBSourceDao) CountByBizIDsAndStatus(ctx context.Context, corpBizID, appBizID uint64, dbSourceBizIDs []uint64) (map[uint64]int64, error) {
	if len(dbSourceBizIDs) == 0 {
		return make(map[uint64]int64), nil
	}

	var results []struct {
		DBSourceBizID uint64
		Count         int64
	}

	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Select("db_source_biz_id, COUNT(*) as count").
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id IN (?) AND is_deleted = 0 AND learn_status = 2",
			corpBizID, appBizID, dbSourceBizIDs).
		Group("db_source_biz_id").
		Find(&results).Error

	if err != nil {
		log.ErrorContextf(ctx, "batch count db table by biz ids and status failed: %v", err)
		return nil, err
	}

	countMap := make(map[uint64]int64, len(results))
	for _, result := range results {
		countMap[result.DBSourceBizID] = result.Count
	}

	return countMap, nil
}

// BatchGetByBizIDs 批量获取数据库源信息
func (r *DBSourceDao) BatchGetByBizIDs(ctx context.Context, dbSourceBizIDs []uint64) ([]*model.DBSource, error) {
	var results []*model.DBSource
	dbSourceBizIDs = slicex.Unique(dbSourceBizIDs)
	batchSize := 200 // 每批获取的记录数
	for i := 0; i < len(dbSourceBizIDs); i += batchSize {
		end := i + batchSize
		if end > len(dbSourceBizIDs) {
			end = len(dbSourceBizIDs)
		}
		batch := dbSourceBizIDs[i:end]

		var batchResults []*model.DBSource
		err := r.db.WithContext(ctx).Model(&model.DBSource{}).
			Where(DBSourceBizID+sqlIn, batch).
			Where(DBIsDeleted+sqlEqual, IsNotDeleted).
			Find(&batchResults).Error
		if err != nil {
			log.ErrorContextf(ctx, "BatchGetByBizIDs|failed to query DBSource by bizIDs: batchID:%v, err:%v", batch, err)
			return nil, err
		}
		results = append(results, batchResults...)
	}
	return results, nil
}

// ListOnlyByAppBizID 仅根据appBizID进行分页查询
func (r *DBSourceDao) ListOnlyByAppBizID(ctx context.Context, appBizID uint64, page, pageSize int) ([]*model.DBSource, int64, error) {
	if appBizID == 0 {
		return nil, 0, errs.ErrParameterInvalid
	}
	var (
		sources []*model.DBSource
		total   int64
	)
	db := r.db.WithContext(ctx).Model(&model.DBSource{}).
		Where(DBAppBizID+sqlEqual, appBizID).
		Where(DBIsDeleted+sqlEqual, IsNotDeleted)
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	offset := (page - 1) * pageSize
	err := db.Order(DBId + SqlOrderByDesc).Offset(offset).Limit(pageSize).Find(&sources).Error
	if err != nil {
		log.ErrorContextf(ctx, "get db source list failed: %v", err)
		return nil, 0, err
	}
	return sources, total, nil
}

// GetOnlyByBizID 通过业务ID获取
func (r *DBSourceDao) GetOnlyByBizID(ctx context.Context, dbSourceBizID uint64) (*model.DBSource, error) {
	if dbSourceBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var source model.DBSource
	err := r.db.WithContext(ctx).
		Where(DBSourceBizID+sqlEqual, dbSourceBizID).
		Where(DBIsDeleted+sqlEqual, IsNotDeleted).
		First(&source).Error
	if err != nil {
		log.ErrorContextf(ctx, "get db source by biz id failed: %v", err)
		return nil, err
	}
	return &source, nil
}

// CheckDbTableIsExisted 判断数据库是否可以连接并且数据表存在
func (r *DBSourceDao) CheckDbTableIsExisted(ctx context.Context, dbSource *model.DBSource, table string) (bool, error) {
	var varifyTableNameSql string
	var tableSafety string
	password, err := util.Decrypt(dbSource.Password)
	if err != nil {
		return false, err
	}
	connDbSource := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: password,
		Port:     dbSource.Port,
	}
	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "CheckDbTableIsExisted|获取数据库连接失败: %v", err)
		return false, err
	}
	defer func(dbConn *sql.DB) {
		closeErr := dbConn.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "CheckDbTableIsExisted|关闭数据库连接失败: %v", err)
		}
	}(dbConn)
	switch connDbSource.DbType {
	case DbTypeMysql:
		varifyTableNameSql = mysqlCheckTableNameSql
	case DbTypeSqlserver:
		varifyTableNameSql = sqlServerCheckTableNameSql
	default:
		return false, errs.ErrWrapf(errs.ErrDbSourceTypeNotSupport, i18n.Translate(ctx, i18nkey.KeyDatabaseType), connDbSource.DbType)
	}

	err = dbConn.QueryRowContext(ctx, varifyTableNameSql, connDbSource.DbName, table).Scan(&tableSafety)
	if err != nil {
		log.WarnContextf(ctx, "CheckDbTableIsExisted|查询表名失败: %v, 查询的表 %v", err, table)
		if errors.Is(err, sql.ErrNoRows) {
			return false, errs.ErrWrapf(errs.ErrDbTableIsNotExist, i18n.Translate(ctx, i18nkey.KeyDatabaseTableInfo), connDbSource.DbName, table)
		} else if err != nil {
			return false, err
		}
	}
	return true, nil
}

// CountDBSourceWithTimeAndStatus 通过时间，获取指定状态的数据库总数
// 新增：✅
// 修改：✅
// 删除：✅
// 修改后删除：✅
// 新增后删除：❌
func (r *DBSourceDao) CountDBSourceWithTimeAndStatus(
	ctx context.Context,
	corpBizID, appBizID uint64,
	startTime time.Time) (uint64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return 0, errs.ErrParameterInvalid
	}
	var count int64
	err := r.db.WithContext(ctx).
		Model(&model.DBSource{}).
		Distinct("id").
		Where("corp_biz_id = ? AND app_biz_id = ? ", corpBizID, appBizID).
		Where("(create_time >= ? AND is_deleted = 0) OR (update_time >= ? AND create_time < ?)", startTime, startTime, startTime).
		Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "CountDBSourceWithTimeAndStatus count fail, err: %v", err)
		return 0, err
	}
	return uint64(count), nil
}
