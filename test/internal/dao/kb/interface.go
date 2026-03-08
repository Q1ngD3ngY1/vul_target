package kb

import (
	"context"

	"gorm.io/gorm"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

//go:generate mockgen -source interface.go -destination interface_mock.go -package kb

type Dao interface {
	TDSQLQuery() *tdsqlquery.Query
	DescribeKnowledgeBase(ctx context.Context, corpBizID uint64, ids []uint64) ([]*kbe.KnowledgeBase, error)
	GetAppShareKGList(ctx context.Context, appBizID uint64) ([]*kbe.AppShareKnowledge, error)
	GetShareKGAppBizIDList(ctx context.Context, knowledgeBizId []uint64) ([]*kbe.AppShareKnowledge, error)
	DeleteAppShareKG(ctx context.Context, appBizID uint64, knowledgeBizIDs []uint64) error
	ExistShareKG(ctx context.Context, appBizID uint64) (*kbe.AppShareKnowledge, error)
	CreateAppShareKG(ctx context.Context, appShareKGs []*kbe.AppShareKnowledge) error
	CreateSharedKnowledge(ctx context.Context, params *kbe.CreateSharedKnowledgeParams) (uint64, error)
	UpdateSharedKnowledge(ctx context.Context, corpBizID, knowledgeBizID uint64,
		userInfo *pb.UserBaseInfo, updateInfo *pb.KnowledgeUpdateInfo) (int64, error)
	RetrieveBaseSharedKnowledge(context.Context, *kbe.ShareKnowledgeFilter) ([]*kbe.SharedKnowledgeInfo, error)
	ListBaseSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64, pageNumber, pageSize uint32,
		keyword string, spaceID string) ([]*kbe.SharedKnowledgeInfo, error)
	RetrieveSharedKnowledgeCount(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64,
		keyword string, spaceID string) (int64, error)
	DeleteSharedKnowledge(ctx context.Context, corpBizID uint64,
		knowledgeBizIDList []uint64) (int64, error)
	RetrieveSharedKnowledgeByName(ctx context.Context, corpBizID uint64, knowledgeNameList []string, spaceId string) (
		[]*kbe.SharedKnowledgeInfo, error)
	ClearSpaceSharedKnowledge(ctx context.Context, corpBizID uint64, spaceID string) (int64, error)
	ListSpaceShareKnowledgeExSelf(ctx context.Context, corpBizID, exStaffID uint64, spaceID,
		keyword string, pageNumber, pageSize uint32) (int64, []*kbe.SharedKnowledgeInfo, error)
	GetKnowledgeConfigsByModelAssociated(
		ctx context.Context, corpBizId uint64, modelKeyword string) ([]*kbe.KnowledgeConfig, error)

	SetKnowledgeBase(ctx context.Context, corpBizId, knowledgeBizId, processingFlag uint64) error
	GetKnowledgeBases(ctx context.Context,
		corpBizId uint64, knowledgeBizIds []uint64) ([]*kbe.KnowledgeBase, error)
	DeleteKnowledgeBases(ctx context.Context, corpBizId uint64, knowledgeBizIds []uint64) error
	GetAppShareKGListProd(ctx context.Context, appBizID uint64) ([]*kbe.AppShareKnowledge, error)

	// ======refer======
	CreateRefer(ctx context.Context, refers []*kbe.Refer) error
	GetRefersByBusinessIDs(ctx context.Context, robotID uint64,
		businessIDs []uint64) ([]*kbe.Refer, error)
	GetRefersByBusinessID(ctx context.Context, businessID uint64) (*kbe.Refer, error)

	// ====KnowledgeConfig=====
	DeleteKnowledgeConfigs(ctx context.Context, corpBizId uint64, knowledgeBizIds []uint64) error
	GetShareKnowledgeConfigs(ctx context.Context,
		corpBizId uint64, knowledgeBizIds []uint64, configTypes []uint32) ([]*kbe.KnowledgeConfig, error)
	SetKnowledgeConfig(ctx context.Context, config *kbe.KnowledgeConfig, tx *tdsqlquery.Query, updateReleaseConfig bool) error
	ModifyKnowledgeConfigList(ctx context.Context, configList []*kbe.KnowledgeConfig) error
	DescribeAppKnowledgeConfig(ctx context.Context, corpBizID, appBizID, knowledgeBizID uint64) ([]*kbe.KnowledgeConfig, error)
	DescribeAppKnowledgeConfigList(ctx context.Context, corpBizID uint64, appBizIDs []uint64) ([]*kbe.KnowledgeConfig, error)
	DeleteAppSharedKnowledgeConfigs(ctx context.Context, corpBizId, appBizId uint64, knowledgeBizIds []uint64) error
	DeleteShareKnowledgeConfigFromCache(ctx context.Context, corpBizId, knowledgeBizId uint64)
	DeleteAppKnowledgeConfigFromCache(ctx context.Context, corpBizId, appBizId uint64)
	// --------KnowledgeSchema ---redis-------
	GetKnowledgeSchema(ctx context.Context, appBizId uint64, envType string) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error)
	GetKnowledgeSchemaAppBizIdByDocClusterId(ctx context.Context,
		docClusterBizId uint64, envType string) (uint64, error)
	SetKnowledgeSchemaDocIdByDocClusterId(ctx context.Context, appBizId uint64, envType string,
		docClusterSchema *docEntity.DocClusterSchema) error
	GetKnowledgeSchemaDocIdByDocClusterId(ctx context.Context, appBizId uint64, envType string,
		docClusterBizId uint64) ([]uint64, error)
	SetKnowledgeSchema(ctx context.Context, appBizId uint64, envType string, schemaItems []*pb.GetKnowledgeSchemaRsp_SchemaItem) error
	SetKnowledgeSchemaAppBizIdByDocClusterId(ctx context.Context,
		docClusterBizId, appBizId uint64, envType string) error

	// --------KnowledgeSchemaTask ----------
	GetKnowledgeSchemaTask(ctx context.Context, selectColumns []string,
		filter *kbe.KnowledgeSchemaTaskFilter) (*kbe.KnowledgeSchemaTask, error)
	UpdateKnowledgeSchemaTask(ctx context.Context, updateColumns []string, task *kbe.KnowledgeSchemaTask) error
	CreateKnowledgeSchemaTask(ctx context.Context, task *kbe.KnowledgeSchemaTask) error
	DeleteKnowledgeSchema(ctx context.Context, corpBizId, appBizId uint64) error
	GetKnowledgeSchemaMaxVersion(ctx context.Context, appBizId uint64) (uint64, error)
	FindKnowledgeSchema(ctx context.Context, selectColumns []string,
		filter *kbe.KnowledgeSchemaFilter) ([]*kbe.KnowledgeSchema, error)
	CreateKnowledgeSchema(ctx context.Context, knowledgeSchema *kbe.KnowledgeSchema) error
	CreateKnowledgeDeleteTask(ctx context.Context, params entity.KnowledgeDeleteParams) error
	DeleteByCustomFieldID(ctx context.Context, tableName string, limit int64,
		customFields []string, customConditions []string, customFieldValues []any) (int64, error)
	CountTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
		tableName string) (int64, error)
	CountTableNeedDeletedDataBizID(ctx context.Context, corpBizID, robotBizID uint64,
		tableName string) (int64, error)
	CountTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpBizID, robotBizID uint64,
		tableName string) (int64, error)
	DeleteTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
		tableName string, totalCount int64) error
	DeleteTableNeedDeletedDataBizID(ctx context.Context, corpBizID, robotBizID uint64,
		tableName string, totalCount int64) error
	DeleteTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpBizID, robotBizID uint64,
		tableName string, totalCount int64) error
	GetCustomFieldIDList(ctx context.Context, corpID, robotID uint64,
		tableName, customField string) ([]uint64, error)

	// 检索配置
	DescribeRetrievalConfig(ctx context.Context, appPrimaryId uint64) (*entity.RetrievalConfig, error)
	ModifyRetrievalConfig(ctx context.Context, retrievalConfig *entity.RetrievalConfig) error
	DescribeRetrievalConfigCache(ctx context.Context, appPrimaryId uint64) (*entity.RetrievalConfig, error)
	ModifyRetrievalConfigCache(ctx context.Context, retrievalConfig *entity.RetrievalConfig) error
	DescribeRetrievalConfigList(ctx context.Context, appPrimaryIds []uint64) ([]*entity.RetrievalConfig, error)

	// --------KnowledgeConfigHistory 知识库配置发布记录----------
	DescribeKnowledgeConfigHistoryList(ctx context.Context,
		filter *kbe.KnowledgeConfigHistoryFilter) ([]*kbe.KnowledgeConfigHistory, error)
	DescribeKnowledgeConfigHistory(ctx context.Context,
		filter *kbe.KnowledgeConfigHistoryFilter) (*kbe.KnowledgeConfigHistory, error)
	CreateKnowledgeConfigHistory(ctx context.Context,
		do *kbe.KnowledgeConfigHistory, tx *tdsqlquery.Query) error
	DeleteKnowledgeConfigHistory(ctx context.Context,
		filter *kbe.KnowledgeConfigHistoryFilter) error
	ModifyKnowledgeConfigHistory(ctx context.Context,
		filter *kbe.KnowledgeConfigHistoryFilter, do *kbe.KnowledgeConfigHistory, tx *tdsqlquery.Query) error
}

func NewDao(mysql types.MySQLDB, tdsql types.TDSQLDB, kbDelDb types.KbDeleteDB,
	rdb types.AdminRedis, retrievalRdb types.RetrievalRedis) Dao {
	return &daoImpl{
		mysql:        mysqlquery.Use(mysql),
		tdsql:        tdsqlquery.Use(tdsql),
		kbdel:        kbDelDb,
		rdb:          rdb,
		retrievalRdb: retrievalRdb,
	}
}

type daoImpl struct {
	mysql        *mysqlquery.Query
	tdsql        *tdsqlquery.Query
	kbdel        *gorm.DB
	rdb          types.AdminRedis
	retrievalRdb types.RetrievalRedis
}

func (d *daoImpl) TDSQLQuery() *tdsqlquery.Query {
	return d.tdsql
}
