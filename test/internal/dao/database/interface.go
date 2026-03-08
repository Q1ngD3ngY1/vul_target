package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/redis/go-redis/v9"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

//go:generate mockgen -source interface.go -destination interface_mock.go -package database

type Dao interface {
	RedisClient() redis.UniversalClient
	Query() *tdsqlquery.Query
	Rpc() *rpc.RPC

	// database
	CreateDatabase(context.Context, *entity.Database) error
	DescribeDatabase(context.Context, *entity.DatabaseFilter) (*entity.Database, error)
	DescribeDatabaseProd(context.Context, *entity.DatabaseFilter) (*entity.DatabaseProd, error)
	DeleteDatabase(context.Context, *entity.DatabaseFilter) error
	ShowDatabases(context.Context, entity.DatabaseConn) ([]string, error)
	CountDatabase(context.Context, *entity.DatabaseFilter) (int64, error)
	ModifyDatabase(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64, updateColumns []string, dbSource *entity.Database) error
	ModifyDatabaseSimple(ctx context.Context, filter *entity.DatabaseFilter, data map[string]any) error
	DescribeDatabaseList(context.Context, *entity.DatabaseFilter) ([]*entity.Database, int64, error)
	FindUnreleaseDatabaseByConds(ctx context.Context, corpBizID, appBizID uint64, dbName string, beginTime, endTime time.Time, nextAction []uint32, page, pageSize uint32) ([]*entity.Database, error)
	DescribeReleaseDatabaseList(ctx context.Context, appBizID, releaseBizID uint64) ([]*entity.DatabaseRelease, error)
	DescribeUnreleasedDatabase(ctx context.Context, appBizID uint64) ([]*entity.Database, error)
	GetDBConnection(ctx context.Context, connDbSource entity.DatabaseConn) (*sql.DB, error)
	CollectUnreleasedDatabase(ctx context.Context, appBizID, releaseBizID uint64) error
	CheckDbTableIsExisted(ctx context.Context, dbSource *entity.Database, table string) (bool, error)
	ReleaseDBSource(ctx context.Context, appBizID, releaseBizID uint64) error
	CountDBSourceWithTimeAndStatus(ctx context.Context, corpBizID, appBizID uint64, startTime time.Time) (uint64, error)
	QueryDBSchemas(ctx context.Context, connDbSource entity.DatabaseConn) (schemas []string, err error)
	GetDBUin(ctx context.Context, corpBizID uint64) (string, error)

	// table
	DescribeTable(context.Context, *entity.TableFilter) (*entity.Table, error)
	DescribeTableList(context.Context, *entity.TableFilter) ([]*entity.Table, int64, error)
	DescribeTableProdList(ctx context.Context, filter *entity.TableFilter) ([]*entity.TableProd, int64, error)
	ModifyTable(ctx context.Context, filter *entity.TableFilter, data map[string]any) error
	DeleteTable(context.Context, *entity.TableFilter) error
	CountTable(context.Context, *entity.TableFilter) (int64, error)
	DescribeUnreleasedTableList(ctx context.Context, appBizID uint64) ([]*entity.Table, error)
	GetDBTableList(ctx context.Context, connDbSource entity.DatabaseConn, page, pageSize int) (tables []string, total int, err error)
	DescribeUnreleaseTableListByConds(ctx context.Context, corpBizID, appBizID uint64, dbTableName string, beginTime, endTime time.Time, nextAction []uint32, page, pageSize uint32) ([]*entity.Table, error)
	GetAllReleaseDBTables(ctx context.Context, appBizID, releaseBizID uint64, onlyBizID bool) ([]*entity.TableRelease, error)
	BatchUpsertByBizID(ctx context.Context, cols []string, tables []*entity.Table) error
	CreateTableList(ctx context.Context, tables []*entity.Table) error
	BatchSoftDeleteByDBSourceBizID(ctx context.Context, corpBizID, appBizID uint64, dbSourceBizIDs []uint64) error
	CollectUnreleasedDBTable(ctx context.Context, appBizID, releaseBizID uint64) error
	GetReleaseDBTable(ctx context.Context, appBizID uint64, releaseBizID uint64, dbTableBizID uint64) (entity.TableRelease, error)
	ReleaseDBTableToProd(ctx context.Context, releaseDBTable entity.TableRelease) error
	GetCountByDbSourceBizID(ctx context.Context, corpBizID, dbSourceBizID uint64) (int64, error)
	GetCountByDbSourceBizIDs(ctx context.Context, corpBizID uint64, dbSourceBizIDs []uint64) (map[uint64]int32, error)
	BatchGetTableByBizIDs(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*entity.Table, error)
	BatchGetByBizIDsForProd(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*entity.TableProd, error)

	// column
	DescribeColumn(context.Context, *entity.ColumnFilter) (*entity.Column, error)
	DescribeColumnList(ctx context.Context, filter *entity.ColumnFilter) ([]*entity.Column, int64, error)
	SoftDeleteByTableBizID(ctx context.Context, filter *entity.ColumnFilter) error
	DeleteByTableBizID(ctx context.Context, tableBizIDs []uint64) (int64, error)
	ListPreviewData(ctx context.Context, connDbSource entity.DatabaseConn, table string, page, pageSize int, columnName string, columnValue string, timeout time.Duration) (columns []string, rows []*pb.RowData, total int64, err error)
	CreateColumnList(ctx context.Context, columns []*entity.Column) error
	BatchUpdateByBizID(ctx context.Context, corpBizID, appBizID uint64, updateColumns []string, dbTableColumns []*entity.Column) (int64, error)
	BatchGetByTableBizID(ctx context.Context, corpBizID, appBizID uint64, tableBizIDs []uint64) (map[uint64][]*entity.Column, error)

	// 外部数据库
	DescribeAvailableDBTypes(ctx context.Context) ([]string, error)
	CreateTopValue(ctx context.Context, cols []*entity.TableTopValue) error
	GetTableInfo(ctx context.Context, connDbSource entity.DatabaseConn, table string) (*entity.TableInfo, error)
	GetTopNValueV2(ctx context.Context, dbSource *entity.Database, robotId, dbTableBizID, embeddingVersion uint64, embeddingName string) error
	GetTopValuesPageByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID, ID uint64, pageSize int) ([]*entity.TableTopValue, error)
	GetDeletedTopValuesPageByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID, ID uint64, pageSize int) ([]*entity.TableTopValue, error)
	AddVector(ctx context.Context, robotId, appBizID, dbTableBizID, businessID, embeddingVersion uint64,
		embeddingName string, enableScope uint32, content string, envType retrieval.EnvType) error
	DeleteVector(ctx context.Context, robotId, appBizID, embeddingVersion uint64, embeddingName string, businessIDs []uint64, envType retrieval.EnvType) error
	BatchSoftDeleteTopValuesByBizID(ctx context.Context, businessIDs []uint64) error
	BatchDeleteTopValuesByBizID(ctx context.Context, businessIDs []uint64) error
	BatchSoftDeleteTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) error
	BatchCleanDeletedTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) error
	QuerySqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn, exeSql string, sqlParams []string) ([]string, []*pb.RowData, string, error)
	RunSqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn, exeSql string, sqlParams []string) (int64, string, error)
	GetTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) ([]*entity.TableTopValue, error)
}

type DBSource interface {
	GetDBConnection(ctx context.Context, connDbSource entity.DatabaseConn) (*sql.DB, error)
	ShowDatabases(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error)
	GetDBTableList(ctx context.Context, connDbSource entity.DatabaseConn, page, pageSize int) (tables []string, total int, err error)
	GetTableInfo(ctx context.Context, connDbSource entity.DatabaseConn, table string) (*entity.TableInfo, error)
	ListPreviewData(ctx context.Context, connDbSource entity.DatabaseConn,
		table string, page, pageSize int, columnName string, columnValue string, timeout time.Duration) (columns []string, rows []*pb.RowData, total int64, err error)
	CheckDbTableIsExisted(ctx context.Context, dbSource *entity.Database, table string) (bool, error)
	GetTopNValueV2(ctx context.Context, dbSource *entity.Database, robotId, dbTableBizID, embeddingVersion uint64, embeddingName string) error
	QuerySqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn, exeSql string, sqlParams []string) ([]string, []*pb.RowData, string, error)
	QueryDBSchemas(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error)
}

type dao struct {
	rpc       *rpc.RPC
	tdsql     *tdsqlquery.Query
	adminRdb  types.AdminRedis
	sourceMap map[string]DBSource
}

func NewDao(rpc *rpc.RPC, tdsql types.TDSQLDB, adminRdb types.AdminRedis) Dao {
	obj := &dao{
		rpc:      rpc,
		tdsql:    tdsqlquery.Use(tdsql),
		adminRdb: adminRdb,
	}
	obj.sourceMap = map[string]DBSource{
		entity.DBTypeMySQL:      &DBMySQL{obj},
		entity.DBTypeSQLServer:  &DBSQLServer{obj},
		entity.DBTypeOracle:     &DBOracle{obj},
		entity.DBTypePostgreSQL: &DBPostgreSQL{obj},
	}
	return obj
}

func (d *dao) RedisClient() redis.UniversalClient {
	return d.adminRdb
}

func (d *dao) Rpc() *rpc.RPC {
	return d.rpc
}

func (d *dao) Query() *tdsqlquery.Query {
	return d.tdsql
}

func (d *dao) getDBSource(sourceType string) DBSource {
	if source, ok := d.sourceMap[sourceType]; ok {
		return source
	}
	return nil
}

func (d *dao) GetDBConnection(ctx context.Context, connDbSource entity.DatabaseConn) (*sql.DB, error) {
	source := d.getDBSource(connDbSource.DbType)
	if source == nil {
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	return source.GetDBConnection(ctx, connDbSource)
}

func (d *dao) ShowDatabases(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	source := d.getDBSource(connDbSource.DbType)
	if source == nil {
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	return source.ShowDatabases(ctx, connDbSource)
}

func (d *dao) QueryDBSchemas(ctx context.Context, connDbSource entity.DatabaseConn) ([]string, error) {
	source := d.getDBSource(connDbSource.DbType)
	if source == nil {
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	return source.QueryDBSchemas(ctx, connDbSource)
}

// GetDBTableList 获取数据源下的 table 列表
func (d *dao) GetDBTableList(ctx context.Context, connDbSource entity.DatabaseConn, page, pageSize int) (tables []string, total int, err error) {
	source := d.getDBSource(connDbSource.DbType)
	if source == nil {
		return nil, 0, errs.ErrDbSourceTypeNotSupport
	}
	return source.GetDBTableList(ctx, connDbSource, page, pageSize)
}

// GetTableInfo 获取外部数据表的完整信息，包括列名、数据类型、行数和列数
func (d *dao) GetTableInfo(ctx context.Context, connDbSource entity.DatabaseConn, table string) (*entity.TableInfo, error) {
	source := d.getDBSource(connDbSource.DbType)
	if source == nil {
		return nil, errs.ErrDbSourceTypeNotSupport
	}
	return source.GetTableInfo(ctx, connDbSource, table)
}

// ListPreviewData 查询预览数据
func (d *dao) ListPreviewData(ctx context.Context, connDbSource entity.DatabaseConn,
	table string, page, pageSize int, columnName string, columnValue string, timeout time.Duration) (columns []string, rows []*pb.RowData, total int64, err error) {
	source := d.getDBSource(connDbSource.DbType)
	if source == nil {
		return nil, nil, 0, errs.ErrDbSourceTypeNotSupport
	}
	return source.ListPreviewData(ctx, connDbSource, table, page, pageSize, columnName, columnValue, timeout)
}

func (d *dao) CheckDbTableIsExisted(ctx context.Context, dbSource *entity.Database, table string) (bool, error) {
	source := d.getDBSource(dbSource.DBType)
	if source == nil {
		return false, errs.ErrDbSourceTypeNotSupport
	}
	return source.CheckDbTableIsExisted(ctx, dbSource, table)
}

// GetTopNValueV2 获取表的topN数据【同时会去同步向量】
// todo 后续这里考虑优化，dao层只做数据获取，logic层去做向量同步
func (d *dao) GetTopNValueV2(ctx context.Context, dbSource *entity.Database, robotId, dbTableBizID,
	embeddingVersion uint64, embeddingName string) error {
	source := d.getDBSource(dbSource.DBType)
	if source == nil {
		return errs.ErrDbSourceTypeNotSupport
	}
	return source.GetTopNValueV2(ctx, dbSource, robotId, dbTableBizID, embeddingVersion, embeddingName)
}

func (d *dao) QuerySqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn, exeSql string, sqlParams []string) ([]string, []*pb.RowData, string, error) {
	source := d.getDBSource(connDbSource.DbType)
	if source == nil {
		return nil, nil, "", errs.ErrDbSourceTypeNotSupport
	}
	return source.QuerySqlForDbSource(ctx, connDbSource, exeSql, sqlParams)
}

func (d *dao) GetDBUin(ctx context.Context, corpBizID uint64) (string, error) {
	uin := contextx.Metadata(ctx).Uin()
	if uin != "" {
		return uin, nil
	}

	corp, err := d.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, corpBizID)
	if err != nil {
		logx.E(ctx, "get uin by corp biz id failed, corpBizID: %d, err: %v", corpBizID, err)
		return "", err
	}
	if corp == nil {
		return "", errs.ErrCorpNotFound
	}
	return corp.Uin, nil
}
