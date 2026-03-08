package segment

import (
	"context"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
	"gorm.io/gorm"
)

type DocSegmentDao interface {
	CreateDocSegments(ctx context.Context, docSegments []*segEntity.DocSegment, tx *gorm.DB) error

	BatchUpdateDocSegmentByFilter(ctx context.Context, filter *segEntity.DocSegmentFilter, updateColumns map[string]any, tx *gorm.DB) error
	BatchUpdateDocSegmentLastActionByID(ctx context.Context, docSegmentIds []uint64, action uint32) error
	BatchUpdateDocSegmentsWithTx(ctx context.Context, updateColumns []string, docSegments []*segEntity.DocSegment, db *gorm.DB) error
	UpdateDocSegmentWithTx(ctx context.Context, updateColumns []string, filter *segEntity.DocSegmentFilter,
		docSegment *segEntity.DocSegment, db *gorm.DB) error

	GetDocSegmentByID(ctx context.Context, robotID, docSegmentID uint64) (
		*segEntity.DocSegmentExtend, error)
	GetSegmentByDocID(ctx context.Context, robotID, docID, startID, count uint64, selectColumns []string, tx *gorm.DB) (
		[]*segEntity.DocSegmentExtend, uint64, error)
	GetReleaseSegmentCount(ctx context.Context, docID uint64, robotID uint64, tx *gorm.DB) (uint64, error)
	GetReleaseSegmentList(ctx context.Context, docID uint64, page, pageSize uint32, robotID uint64, tx *gorm.DB) (
		[]*segEntity.DocSegmentExtend, error)
	GetDocSegmentCountWithTx(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentFilter, tx *gorm.DB) (int64, error)
	GetDocSegmentListWithTx(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentFilter, tx *gorm.DB) ([]*segEntity.DocSegment, error)
	GetDocSegmentCountAndList(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentFilter) ([]*segEntity.DocSegment, int64, error)
	BatchGetDocSegmentList(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentFilter, db *gorm.DB) ([]*segEntity.DocSegment, error)

	GetDocSegmentByFilter(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentFilter, tx *gorm.DB) (*segEntity.DocSegment, error)

	UpdateDocSegmentAuditStatus(ctx context.Context, corpBizID uint64,
		appBizID uint64, docBizID uint64, businessIDs []string, auditStatus uint32) error
	QuerySegment(ctx context.Context, robotID uint64, indexType knowClient.IndexType, source []string,
		conditions map[string]any, size int) ([]*segEntity.EsSegment, error)
	QueryQaSegmentInProd(ctx context.Context, robotID uint64, qaID uint64, source []string, size int) ([]*segEntity.EsSegment, error)
	QueryDocSegmentInProd(ctx context.Context, robotID uint64, docID uint64, source []string, size int) ([]*segEntity.EsSegment, error)
	QueryDocBigData(ctx context.Context, robotID uint64, docID uint64, source []string, size int) ([]*segEntity.EsDocBigData, error)
}
type DocSegmentInteveneDao interface {
	CreateDocSegmentOrgDataTemporary(ctx context.Context, orgData *segEntity.DocSegmentOrgDataTemporary) error
	UpdateDocSegmentTemporaryOrgData(ctx context.Context, updateColumns []string,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter, orgData *segEntity.DocSegmentOrgDataTemporary) (
		int64, error)
	UpdateDocSegmentTemporaryOrgDataContent(ctx context.Context, corpBizID uint64,
		appBizID uint64, docBizID uint64, businessIDs []string, orgData string) error
	// TODO: not-used
	BatchRecoverDocTemporaryOrgDataByDocBizID(ctx context.Context, corpBizID,
		appBizID, docBizID uint64, batchSize int) error
	BatchUpdateDocTemporaryOrgData(ctx context.Context,
		updateColumns []string, filter *segEntity.DocSegmentOrgDataTemporaryFilter, orgData *segEntity.DocSegmentOrgDataTemporary,
		batchSize int) error
	DeleteDocSegmentTemporaryOrgData(ctx context.Context, corpBizID uint64,
		appBizID uint64, docBizID uint64, businessIDs []string) error
	BatchDeleteTemporaryDocOrgDataByDocBizID(ctx context.Context, corpBizID, appBizID, docBizID uint64, batchSize int) error

	RealityDeleteDocSegmentTemporary(ctx context.Context, filter *segEntity.DocSegmentOrgDataTemporaryFilter) (int64, error)
	// TODO: not-used ?
	RealityDeleteDocSegmentTemporaryOrgDataByOrgDataBizID(ctx context.Context, corpBizID,
		appBizID, docBizID uint64, businessID string) error
	RealityBatchDeleteTemporaryDocOrgData(ctx context.Context,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter, batchSize int) error

	DisabledDocSegmentTemporaryOrgData(ctx context.Context, corpBizID uint64,
		appBizID uint64, docBizID uint64, businessIDs []string) error
	EnableDocSegmentTemporaryOrgData(ctx context.Context, corpBizID uint64,
		appBizID uint64, docBizID uint64, businessIDs []string) error
	GetDocTemporaryOrgDataList(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error)
	GetDocTemporaryOrgDataByDocBizID(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error)
	GetDocTemporaryOrgDataListByKeyWords(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error)
	GetEditTemporaryOrgData(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error)
	GetInsertTemporaryOrgData(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error)
	GetDocTemporaryOrgData(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter) (*segEntity.DocSegmentOrgDataTemporary, error)
	GetDocTemporaryOrgDataByBizID(ctx context.Context, selectColumns []string, corpBizID,
		appBizID, docBizID uint64, businessID string) (*segEntity.DocSegmentOrgDataTemporary, error)
	GetDocTemporaryOrgDataByLastOrgDataID(ctx context.Context, selectColumns []string, corpBizID,
		appBizID, docBizID uint64, lastOrgDataID string) (*segEntity.DocSegmentOrgDataTemporary, error)
	GetDocTemporaryOrgDataCount(ctx context.Context,
		filter *segEntity.DocSegmentOrgDataTemporaryFilter) (int64, error)
	GetDocTemporaryOrgDataByOriginOrgDataID(ctx context.Context, selectColumns []string, corpBizID,
		appBizID, docBizID uint64, originOrgDataID string) (*segEntity.DocSegmentOrgDataTemporary, error)

	CreateDocSegmentOrgData(ctx context.Context, orgData *segEntity.DocSegmentOrgData, db *gorm.DB) error
	GetDocOrgDataList(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) ([]*segEntity.DocSegmentOrgData, error)
	GetDocOrgDataListByKeyWords(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) ([]*segEntity.DocSegmentOrgData, error)
	GetDocOrgData(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) (*segEntity.DocSegmentOrgData, error)
	GetDocOrgDataCount(ctx context.Context, filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) (int64, error)
	UpdateDocSegmentOrgData(ctx context.Context, updateColumns []string,
		filter *segEntity.DocSegmentOrgDataFilter, orgData *segEntity.DocSegmentOrgData, db *gorm.DB) (int64, error)
	RealityDeleteDocSegment(ctx context.Context, filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) (int64, error)

	// GetDocSegmentOrgDataByCursor 游标分页获取文档切片原始数据
	GetDocSegmentOrgDataByCursor(ctx context.Context, corpBizID, appBizID, docBizID uint64, lastBusinessID uint64, limit int, db *gorm.DB) ([]*segEntity.DocSegmentOrgData, error)
}

type DocSegmentPageInfoDao interface {
	CreateDocSegmentPageInfos(ctx context.Context, docSegPaegInfos []*segEntity.DocSegmentPageInfo, tx *gorm.DB) error
	BatchGetDocSegmentPageInfoList(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentPageInfoFilter, db *gorm.DB) ([]*segEntity.DocSegmentPageInfo, error)
	GetDocSegmentPageInfoListWithTx(ctx context.Context, selectColumns []string,
		filter *entity.DocSegmentPageInfoFilter, tx *gorm.DB) ([]*entity.DocSegmentPageInfo, error)
	BatchUpdateDocSegmentPageInfos(ctx context.Context, filter *segEntity.DocSegmentPageInfoFilter, updateColumns map[string]any, tx *gorm.DB) error

	// GetDocSegmentPageInfosByCursor 游标分页获取文档切片页码信息
	GetDocSegmentPageInfosByCursor(ctx context.Context, robotID, robotBizID, docID uint64, lastID uint64, limit int) ([]*segEntity.DocSegmentPageInfo, error)
}

type DocSegmentImageDao interface {
	CreateDocSegmentImages(ctx context.Context, docSegImages []*segEntity.DocSegmentImage, tx *gorm.DB) error
	BatchUpdateDocSegmentImages(ctx context.Context, filter *segEntity.DocSegmentImageFilter, updateColumns map[string]any, tx *gorm.DB) error
	GetDocSegmentImageCountWithTx(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentImageFilter, tx *gorm.DB) (int64, error)
	GetDocSegmentImageListWithTx(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentImageFilter, tx *gorm.DB) ([]*segEntity.DocSegmentImage, error)
	GetDocSegmentImageCountAndList(ctx context.Context, selectColumns []string,
		filter *segEntity.DocSegmentImageFilter) ([]*segEntity.DocSegmentImage, int64, error)

	// GetDocSegmentImagesByCursor 游标分页获取文档切片图片
	GetDocSegmentImagesByCursor(ctx context.Context, robotID, robotBizID, docID uint64, lastID uint64, limit int) ([]*segEntity.DocSegmentImage, error)
}

type DocSegmentSheetDao interface {
	CreateDocSegmentSheet(ctx context.Context, sheet *segEntity.DocSegmentSheetTemporary) error
	GetSheetList(ctx context.Context, selectColumns []string, filter *segEntity.DocSegmentSheetTemporaryFilter) ([]*segEntity.DocSegmentSheetTemporary, error)
	GetSheet(ctx context.Context, selectColumns []string, filter *segEntity.DocSegmentSheetTemporaryFilter) (*segEntity.DocSegmentSheetTemporary, error)
	GetDocSheetCount(ctx context.Context, filter *segEntity.DocSegmentSheetTemporaryFilter) (int64, error)
	UpdateDocSegmentSheet(ctx context.Context, updateColumns []string,
		filter *segEntity.DocSegmentSheetTemporaryFilter, sheet *segEntity.DocSegmentSheetTemporary) (int64, error)
	DeleteDocSegmentSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, businessIDs []uint64) error
	DisabledDocSegmentSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, businessIDs []uint64) error
	EnableDocSegmentSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, businessIDs []uint64) error
	DisabledRetrievalEnhanceSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, sheetNames []string) error
	EnableRetrievalEnhanceSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, sheetNames []string) error
	UpdateDocSegmentSheetAuditStatus(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, businessIDs []uint64, auditStatus uint32) error
	BatchUpdateDocSegmentSheet(ctx context.Context, updateColumns []string, filter *segEntity.DocSegmentSheetTemporaryFilter,
		sheet *segEntity.DocSegmentSheetTemporary, batchSize int) error
	BatchDeleteDocSegmentSheetByDocBizID(ctx context.Context, corpBizID, appBizID, docBizID uint64, batchSize int) error
	RealityDeleteDocSegmentSheet(ctx context.Context, filter *segEntity.DocSegmentSheetTemporaryFilter) (int64, error)
	RealityBatchDeleteDocSheet(ctx context.Context, filter *segEntity.DocSegmentSheetTemporaryFilter, batchSize int) error
}

type Dao interface {
	Query() *mysqlquery.Query
	TdsqlQuery() *tdsqlquery.Query

	DocSegmentDao
	DocSegmentInteveneDao
	DocSegmentPageInfoDao
	DocSegmentImageDao
	DocSegmentSheetDao
}

func NewDao(mysqlDB types.MySQLDB, tdsqlDB types.TDSQLDB, es types.ESClient) Dao {
	return &daoImpl{
		mysql:    mysqlquery.Use(mysqlDB),
		tdsql:    tdsqlquery.Use(tdsqlDB),
		esClient: es,
	}
}

type daoImpl struct {
	mysql    *mysqlquery.Query
	tdsql    *tdsqlquery.Query
	esClient *elasticv8.TypedClient
}

func (d *daoImpl) Query() *mysqlquery.Query {
	return d.mysql
}

func (d *daoImpl) TdsqlQuery() *tdsqlquery.Query {
	return d.tdsql
}
