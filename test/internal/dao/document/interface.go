package document

import (
	"context"
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"gorm.io/gorm"

	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

//go:generate mockgen -source interface.go -destination interface_mock.go -package document

type DocDao interface {
	CreateDoc(ctx context.Context, doc *docEntity.Doc, tx *gorm.DB) error
	GetAllDocs(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) ([]*docEntity.Doc, error)
	GetDocList(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) ([]*docEntity.Doc, error)

	GetDocCountByDistinctID(ctx context.Context, filter *docEntity.DocFilter) (int64, error)
	CountDocWithTimeAndStatus(ctx context.Context, corpID, robotID uint64, status []uint32,
		startTime time.Time, tx *gorm.DB) (uint64, error)
	GetRobotDocExceedUsage(ctx context.Context, corpID uint64, robotIDs []uint64) (map[uint64]entity.CapacityUsage, error)
	GetRobotDocUsage(ctx context.Context, robotID uint64, corpID uint64) (entity.CapacityUsage, error)

	GetDocListWithFilter(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter, db *gorm.DB) (
		[]*docEntity.Doc, error)
	GetDocCountWithFilter(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter, db *gorm.DB) (int64, error)
	GetDocCountAndList(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) ([]*docEntity.Doc, int64, error)

	GetDocByDocFilter(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter,
		tx *gorm.DB) (*docEntity.Doc, error)
	GetDocByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*docEntity.Doc, error)
	GetDocIdByDocBizId(ctx context.Context, docBizId, robotId uint64) (uint64, error)

	GetDocAutoRefreshList(ctx context.Context, nextUpdateTime time.Time) ([]*docEntity.Doc, error)
	GetDiffDocs(ctx context.Context, filter *docEntity.DocFilter) ([]*docEntity.Doc, error)
	BatchUpdateDocsByFilter(ctx context.Context, filter *docEntity.DocFilter, updateColumns map[string]any, tx *gorm.DB) error
	BatchUpdateDocs(ctx context.Context, docs []*docEntity.Doc, tx *gorm.DB) error
	UpdateDoc(ctx context.Context, updateColumns []string, filter *docEntity.DocFilter,
		doc *docEntity.Doc) (int64, error)
	UpdateDocByTx(ctx context.Context, updateColumns []string, filter *docEntity.DocFilter,
		doc *docEntity.Doc, tx *gorm.DB) (int64, error)

	DeleteDocByTx(ctx context.Context, filter *docEntity.DocFilter, doc *docEntity.Doc, tx *gorm.DB) error
	UpdateDocStatus(ctx context.Context, id, robotId uint64, status uint32) error

	// GetDocsByCursor 游标分页获取文档（用于导出）
	// 使用索引 idx_corp_robot_opt (corp_id, robot_id, is_deleted, opt)
	GetDocsByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int) ([]*docEntity.Doc, error)
}

type Text2SqlDocDao interface {
	GetDocMetaDataByDocId(ctx context.Context, docId, robotId uint64) ([]*docEntity.Text2sqlMetaMappingPreview, error)
	GetText2sqlMaxId(ctx context.Context, dbName, dbTableName string) (int, error)
	GetText2sqlDataPreview(ctx context.Context, dbName, dbTableName string, fetchFull bool, lastId, pageSize int) (
		[]string, []*pb.RowData, int64, error)
	GetText2sqlTableInfo(ctx context.Context, dbName, dbTableName string) (*database.TableInfo, error)
	GetDocMetaDataForSchema(ctx context.Context, docId, robotId uint64, scenes uint32) (
		bool, []docEntity.Text2sqlMetaMappingPreview, error)
}

type DocDiffDao interface {
	CreateDocDiffData(ctx context.Context, docDiffData *docEntity.DocDiffData) error
	UpdateDocDiffData(ctx context.Context, updateColumns []string, corpBizId uint64, robotBizId uint64, diffBizIds []uint64,
		docDiffData *docEntity.DocDiffData) error
	DeleteDocDiffData(ctx context.Context, corpBizId uint64, robotBizId uint64,
		diffBizIds []uint64) error
	GetDocDiffDataCountAndList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocDiffDataFilter) ([]*docEntity.DocDiffData, int64, error)
	GetDocDiffDataList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocDiffDataFilter) ([]*docEntity.DocDiffData, error)
	GetDocDiffDataCount(ctx context.Context, selectColumns []string,
		filter *docEntity.DocDiffDataFilter) (int64, error)

	CreateDocDiff(ctx context.Context, docDiff *docEntity.DocDiff) error
	UpdateDocDiffTasks(ctx context.Context, updateColumns []string,
		corpBizId uint64, robotBizId uint64, businessIds []uint64, docDiff *docEntity.DocDiff) error
	DeleteDocDiffTasks(ctx context.Context, corpBizId uint64, robotBizId uint64,
		businessIds []uint64) error
	GetDocDiffTask(ctx context.Context, selectColumns []string, corpBizId,
		robotBizId, diffId uint64) (*docEntity.DocDiff, error)
	GetDocDiffTaskCount(ctx context.Context, selectColumns []string,
		filter *docEntity.DocDiffTaskFilter) (int64, error)
	GetDocDiffTaskList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocDiffTaskFilter) ([]*docEntity.DocDiff, error)
	GetDocDiffTaskCountAndList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocDiffTaskFilter) ([]*docEntity.DocDiff, int64, error)
}

type DocSchemaDao interface {
	CreateDocSchema(ctx context.Context, docSchema *docEntity.DocSchema) error
	GetDocSchemaCount(ctx context.Context, selectColumns []string,
		filter *docEntity.DocSchemaFilter) (int64, error)
	GetDocSchemaCountAndList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocSchemaFilter) ([]*docEntity.DocSchema, int64, error)
	GetDocSchemaList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocSchemaFilter) ([]*docEntity.DocSchema, error)
	UpdateDocSchema(ctx context.Context, updateColumns []string,
		docSchema *docEntity.DocSchema) error
	DeleteDocSchema(ctx context.Context, corpBizId uint64, appBizId uint64,
		docBizIds []uint64) error

	CreateDocClusterSchema(ctx context.Context, docClusterSchema *docEntity.DocClusterSchema) error
	GetDocClusterSchemaList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocClusterSchemaFilter) ([]*docEntity.DocClusterSchema, error)
	GetDocClusterSchemaCount(ctx context.Context, selectColumns []string,
		filter *docEntity.DocClusterSchemaFilter) (int64, error)
	GetDocClusterSchemaCountAndList(ctx context.Context, selectColumns []string,
		filter *docEntity.DocClusterSchemaFilter) ([]*docEntity.DocClusterSchema, int64, error)
	UpdateDocClusterSchema(ctx context.Context, updateColumns []string,
		corpBizId uint64, appBizId uint64, clusterBizId uint64, docClusterSchema *docEntity.DocClusterSchema) error
	DeleteDocClusterSchemaAllOldVersion(ctx context.Context, corpBizId,
		appBizId, maxVersion uint64) error
	GetDocClusterSchemaDaoMaxVersion(ctx context.Context, appBizId uint64) (uint64, error)
}

type DocParseDao interface {
	CreateDocParseTask(ctx context.Context, docParse *docEntity.DocParse) error
	UpdateDocParseTaskByTx(ctx context.Context, updateColumns []string, docParse *docEntity.DocParse, tx *gorm.DB) error
	GetDocParseList(ctx context.Context, selectColumns []string, filter *docEntity.DocParseFilter) (
		[]*docEntity.DocParse, error)
	GetDocParseListWithTx(ctx context.Context, selectColumns []string, filter *docEntity.DocParseFilter, tx *gorm.DB) (
		[]*docEntity.DocParse, error)
	DeleteDocParseByDocID(ctx context.Context, corpID, robotID, docID uint64) error
}

type DocInterneveDao interface {
	GetOrgDataListByDocID(ctx context.Context, docID, offset, limit uint64) ([]string, error)
	GetOrgDataCountByDocID(ctx context.Context, docID uint64) (count int64, err error)
}

type Refer interface {
	CreateRefer(ctx context.Context, refers []entity.Refer) error
	GetReferListByFilter(ctx context.Context, selectColumns []string, filters *entity.ReferFilter) ([]*entity.Refer, error)
}

type CorpCosDocDap interface {
	CreateCorpCosDoc(ctx context.Context, doc *docEntity.CorpCOSDoc) error
	ModifyCorpCosDoc(ctx context.Context, updateColumns []string, filter *docEntity.CorpCOSDocFilter,
		doc *docEntity.CorpCOSDoc) error
	DescribeCorpCosDoc(ctx context.Context, selectColumns []string, filter *docEntity.CorpCOSDocFilter,
	) (*docEntity.CorpCOSDoc, error)
	DescribeCorpCosDocList(ctx context.Context, selectColumns []string, filter *docEntity.CorpCOSDocFilter,
	) ([]*docEntity.CorpCOSDoc, error)
}

type Dao interface {
	Query() *mysqlquery.Query
	TdsqlQuery() *tdsqlquery.Query
	RedisCli() types.AdminRedis

	DocDao
	DocSchemaDao
	DocDiffDao
	DocParseDao
	DocInterneveDao
	Refer
	Text2SqlDocDao
	CorpCosDocDap
}

func NewDao(mysqlDB types.MySQLDB, tdsqlDB types.TDSQLDB, text2sqlDB types.Tex2sqlDB, adminRdb types.AdminRedis) Dao {
	return &daoImpl{
		mysql:      mysqlquery.Use(mysqlDB),
		tdsql:      tdsqlquery.Use(tdsqlDB),
		text2sqlDB: text2sqlDB,
		adminRdb:   adminRdb,
	}
}

type daoImpl struct {
	mysql      *mysqlquery.Query
	tdsql      *tdsqlquery.Query
	text2sqlDB *gorm.DB
	adminRdb   types.AdminRedis
}

func (d *daoImpl) TdsqlQuery() *tdsqlquery.Query {
	return d.tdsql
}

func (d *daoImpl) Query() *mysqlquery.Query {
	return d.mysql
}

func (d *daoImpl) RedisCli() types.AdminRedis {
	return d.adminRdb
}
