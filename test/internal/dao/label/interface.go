package label

import (
	"context"
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
	"gorm.io/gorm"
)

//go:generate mockgen -source interface.go -destination interface_mock.go -package label

type Dao interface {
	GetAttributeTotal(ctx context.Context, robotID uint64, query string, ids []uint64) (uint64, error)
	GetAttributeList(ctx context.Context, robotID uint64, queryStr string, page, pageSize uint32,
		ids []uint64) ([]*entity.Attribute, error)
	GetAttributeListInfo(ctx context.Context, selectColumns []string, filter *entity.AttributeFilter) ([]*entity.Attribute, error)
	GetAttributeListByIDs(ctx context.Context, robotID uint64, ids []uint64) ([]*entity.Attribute, error)
	GetAttributeByBizIDs(ctx context.Context, robotID uint64, bizIDs []uint64) (map[uint64]*entity.Attribute, error)
	GetAttributeByKeys(ctx context.Context, robotID uint64, keys []string) (map[string]*entity.Attribute, error)
	GetAttributeByNames(ctx context.Context, robotID uint64, names []string) (map[string]*entity.Attribute, error)
	GetAttributeByRobotID(ctx context.Context, robotID uint64) (map[string]struct{}, map[string]struct{}, error)
	GetAttributeKeyAndIDsByRobotID(ctx context.Context, robotID uint64) ([]*entity.AttributeKeyAndID, error)
	GetAttributeKeyAndIDsByRobotIDProd(ctx context.Context, robotID uint64) ([]*entity.AttributeKeyAndID, error)
	GetAttributeLabelCountByFilter(ctx context.Context, selectColumns []string, filter *entity.AttributeLabelFilter) (int64, error)
	GetAttributeLabelByIDOrder(ctx context.Context, robotID uint64, ids []uint64) ([]*entity.AttributeLabel, error)
	GetAttributeLabelByBizIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*entity.AttributeLabel, error)
	GetAttributeLabelCount(ctx context.Context, attrID uint64, queryStr string, queryScope string, robotID uint64) (uint64, error)
	GetAttributeLabelByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*entity.AttributeLabel, error)
	GetAttributeLabelByAttrIDs(ctx context.Context, attrIDs []uint64, robotID uint64) (map[uint64][]*entity.AttributeLabel, error)
	GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx context.Context, attrIDs []uint64, robotID uint64) (map[uint64][]*entity.AttributeLabel, error)
	GetAttributeLabelByName(ctx context.Context, attrID uint64, name string, robotID uint64) ([]*entity.AttributeLabel, error)
	GetDocAttributeLabel(ctx context.Context, robotID uint64, docIDs []uint64) ([]*entity.DocAttributeLabel, error)
	GetDocAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs, labelIDs []uint64) (uint64, error)
	GetDocAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32,
		attrIDs []uint64, labelIDs []uint64, page uint32, pageSize uint32) ([]*entity.DocAttributeLabel, error)
	GetDocCountByAttributeLabel(ctx context.Context, robotID uint64, noStatusList []uint32, attrID uint64, labelIDs []uint64) (uint64, error)
	GetQAAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs []uint64, labelIDs []uint64) (uint64, error)
	GetQAAttributeLabel(ctx context.Context, robotID uint64, qaIDs []uint64) ([]*entity.QAAttributeLabel, error)
	GetQAAttributeLabelForExport(ctx context.Context, robotID uint64, lastID uint64, limit int) ([]*entity.QAAttributeLabel, error)
	GetQAAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32,
		attrIDs []uint64, labelIDs []uint64, page, pageSize uint32) ([]*entity.QAAttributeLabel, error)
	GetQACountByAttributeLabel(ctx context.Context, robotID uint64,
		noReleaseStatusList []uint32, attrID uint64, labelIDs []uint64) (uint64, error)
	GetAttributeLabelList(ctx context.Context, attrID uint64, queryStr string, queryScope string,
		lastLabelID uint64, limit uint32, robotID uint64) ([]*entity.AttributeLabel, error)
	GetAttributeByIDs(ctx context.Context, robotID uint64, ids []uint64) (map[uint64]*entity.Attribute, error)
	GetWaitReleaseAttributeCount(ctx context.Context, robotID uint64, name string,
		actions []uint32, startTime, endTime time.Time) (uint64, error)
	GetWaitReleaseAttributeList(ctx context.Context, robotID uint64, name string,
		actions []uint32, page, pageSize uint32, startTime, endTime time.Time) ([]*entity.Attribute, error)
	GetWaitReleaseAttributeLables(ctx context.Context, robotID uint64, attrIDs []uint64) (
		[]*entity.AttributeLabel, error)
	UpdateAttributeSuccess(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) error
	UpdateAttributeFail(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) error
	GetAttributeKeysDelStatusAndIDs(ctx context.Context, robotID uint64, attrKeys []string) (map[string]*entity.Attribute, error)
	GetAttributeLabelListInfo(ctx context.Context, selectColumns []string, filter *entity.AttributeLabelFilter) ([]*entity.AttributeLabel, error)
	GetAttributeLabelCountV2(ctx context.Context, selectColumns []string,
		filter *entity.AttributeLabelFilter) (int64, error)
	UpdateAttributeStatus(ctx context.Context, attribute *entity.Attribute, tx *gorm.DB) error
	UpdateAttributeLabelStatus(ctx context.Context, attributeLabel *entity.AttributeLabel, tx *gorm.DB) error
	ReleaseAttributeProd(ctx context.Context, releaseAttribute *releaseEntity.ReleaseAttribute, tx *gorm.DB) error
	ReleaseAttributeLabelProd(ctx context.Context, releaseAttributeLabel *releaseEntity.ReleaseAttributeLabel, tx *gorm.DB) error
	GetAttributeLabelChunkByAttrID(ctx context.Context, selectColumns []string, robotID, attrID, startID uint64, limit int) ([]*entity.AttributeLabel, error)
	GetAttributeChunkByRobotID(ctx context.Context, robotID, startID uint64, limit int) ([]*entity.Attribute, error)
	GetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey, envType string) ([]entity.AttributeLabelRedisValue, error)
	SetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey string,
		redisValue []entity.AttributeLabelRedisValue, envType string) error
	CreateAttributeLabelsRedis(ctx context.Context, attrItem *entity.AttributeLabelItem) error
	PipelineDelAttributeLabelRedis(ctx context.Context, robotID uint64, attrKeys []string, envType string) error
	PipelineSetAttributeLabelRedis(ctx context.Context, robotID uint64,
		attrKey2RedisValue map[string][]entity.AttributeLabelRedisValue, envType string) error
	GetDocAttributeLabelCount(ctx context.Context, selectColumns []string, filter *entity.DocAttributeLabelFilter) (int64, error)
	UpdateAttributeLabelTask(ctx context.Context, task *entity.AttributeLabelTask) error
	GetDocAttributeLabelListInfo(ctx context.Context, selectColumns []string, filter *entity.DocAttributeLabelFilter) ([]*entity.DocAttributeLabel, error)
	GetUpdateAttributeTask(ctx context.Context, taskID, corpID, robotID uint64) (*entity.AttributeLabelTask, error)
	DeleteAttributeLabel(ctx context.Context, robotID uint64, attrID uint64, deleteLabelIDs []uint64, tx *gorm.DB) error
	UpdateAttribute(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) error
	UpdateAttributeLabels(ctx context.Context, labels []*entity.AttributeLabel, tx *gorm.DB) error
	BatchUpdateAttributes(ctx context.Context, attrReq *entity.AttributeFilter, updateColumns map[string]any, tx *gorm.DB) error
	BatchUpdateAttributeLabels(ctx context.Context, attrLabelReq *entity.AttributeLabelFilter, updateColumns map[string]any, tx *gorm.DB) error
	CreateAttributeLabel(ctx context.Context, labels []*entity.AttributeLabel, tx *gorm.DB) error
	CreateUpdateAttributeTask(ctx context.Context, req *entity.UpdateAttributeLabelReq,
		corpID, staffID, robotID uint64) (uint64, error)
	CreateAttribute(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) (uint64, error)
	UpdateDocAttributeLabelByTx(ctx context.Context, robotID, docID uint64, attributeLabelReq *entity.UpdateDocAttributeLabelReq, tx *gorm.DB) error

	CreateDocAttributeLabel(ctx context.Context, labels []*entity.DocAttributeLabel, tx *gorm.DB) error
	DeleteDocAttributeLabel(ctx context.Context, robotID uint64, docID uint64, tx *gorm.DB) error
	CreateQAAttributeLabel(ctx context.Context, labels []*entity.QAAttributeLabel) error
	DeleteQAAttributeLabel(ctx context.Context, robotID uint64, qaID uint64) error
	// ----------------es---------------
	BatchAddAndUpdateAttributes(ctx context.Context, robotID uint64, attributes []*entity.Attribute) error
	AddAndUpdateAttribute(ctx context.Context, robotID uint64, attr *entity.Attribute) error
	QueryAttributeMatchPhrase(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error)
	QueryAttributeWildcard(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error)
	BatchDeleteAttributes(ctx context.Context, robotID uint64, attrIDs []uint64) error
	BatchAddAndUpdateAttributeLabels(ctx context.Context, robotID, attrID uint64, labels []*entity.AttributeLabel) error
	GetAttrIDByQueryLabelMatchPhrase(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error)
	GetAttrIDByQueryLabelWildcard(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error)
	BatchDeleteAttributeLabelByAttrIDs(ctx context.Context, robotID uint64, attrIDs []uint64) error
	DeleteAttributeLabelByAttrIDs(ctx context.Context, robotID uint64, attrIDs []uint64, tx *gorm.DB) error
	DeleteAttribute(ctx context.Context, robotID uint64, attrIDs []uint64, tx *gorm.DB) error
	BatchDeleteAttributeLabelsByIDs(ctx context.Context, robotID uint64, attrLabelIDs []uint64) error
	QueryAttributeLabelCursorMatchPhrase(ctx context.Context, attrID uint64, query string, queryScope string,
		lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error)
	QueryAttributeLabelCursorWildcard(ctx context.Context, attrID uint64, query string, queryScope string,
		lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error)

	GetTDSQL() *tdsqlquery.Query
}

func NewDao(mysql types.MySQLDB, tdsql types.TDSQLDB, rdb types.AdminRedis, es types.ESClient) Dao {
	return &daoImpl{
		mysql:    mysqlquery.Use(mysql),
		tdsql:    tdsqlquery.Use(tdsql),
		rdb:      rdb,
		esClient: es,
	}
}

type daoImpl struct {
	mysql    *mysqlquery.Query
	tdsql    *tdsqlquery.Query
	rdb      types.AdminRedis
	esClient *elasticv8.TypedClient
}

// GetTDSQL 返回TDSQL的Query实例（别名方法）
func (d *daoImpl) GetTDSQL() *tdsqlquery.Query {
	return d.tdsql
}
