package release

import (
	"context"
	"strings"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"gorm.io/gorm"
)

var special = strings.NewReplacer(`\`, `\\`, `_`, `\_`, `%`, `\%`, `'`, `\'`)

type AuditResultStat struct {
	Total       uint32 `gorm:"column:total"`
	AuditStatus uint32 `gorm:"column:audit_status"` // 审核状态(1未审核 2审核中 3审核通过 4审核失败)
}

type ReleaseDao interface {
	UpdateReleaseRecord(ctx context.Context, updateColumns []string, filter *releaseEntity.ReleaseFilter,
		record *releaseEntity.Release, tx *gorm.DB) (int64, error)
	GetLatestRelease(ctx context.Context, corpID, robotID uint64) (*releaseEntity.Release, error)
	GetLatestSuccessRelease(ctx context.Context, corpID, robotID uint64) (
		*releaseEntity.Release, error)
	GetReleaseByBizID(ctx context.Context, bizID uint64) (
		*releaseEntity.Release, error)
	GetReleaseByID(ctx context.Context, id uint64) (*releaseEntity.Release, error)
}

type ReleaseQADao interface {
	CreateReleaseQARecords(ctx context.Context, releaseQas []*releaseEntity.ReleaseQA, tx *gorm.DB) error
	BatchUpdateReleaseQARecords(ctx context.Context, updateColumns map[string]any,
		filter *releaseEntity.ReleaseQAFilter, tx *gorm.DB) (uint64, error)
	GetReleaseQaIdMap(ctx context.Context, corpId, robotId, versionId uint64,
		qaIds []uint64) (map[uint64]struct{}, error)
	GetReleaseQaDocIdMap(ctx context.Context, corpId, robotId, versionId uint64,
		docIds []uint64) (map[uint64]struct{}, error)
	GetDocIDInReleaseDocQAs(ctx context.Context, release *releaseEntity.Release) (
		[]uint64, error)
	GetModifyQACount(ctx context.Context, robotID, versionID uint64,
		question string, actions []uint32, releaseStatuses []uint32) (uint64, error)
	GetModifyQAList(ctx context.Context, req *releaseEntity.ListReleaseQAReq) (
		[]*releaseEntity.ReleaseQA, error)
	GetAuditQAByVersion(ctx context.Context, versionID uint64) (
		[]*releaseEntity.AuditReleaseQA, error)
	GetReleaseModifyQA(ctx context.Context, release *releaseEntity.Release, qas []*qaEntity.DocQA) (
		map[uint64]*releaseEntity.ReleaseQA, error)
	GetReleaseQAByID(ctx context.Context, id uint64) (*releaseEntity.ReleaseQA, error)
	GetReleaseQAAuditStat(ctx context.Context, versionID uint64) (
		map[uint32]*releaseEntity.AuditResultStat, error)
	GetForbidReleaseQA(ctx context.Context, versionID uint64) ([]*releaseEntity.ReleaseQA, error)
	GetAuditFailReleaseQA(ctx context.Context, versionID uint64, message string) (uint64, error)
	IsExistReleaseQA(ctx context.Context, filter *releaseEntity.ReleaseQAFilter) (bool, error)
	GetAuditQAFailByQaID(ctx context.Context, corpID, qaID uint64) ([]uint64, error)
	GetAuditQAFailByVersion(ctx context.Context, corpID, versionID uint64) ([]*releaseEntity.AuditReleaseQA, error)
}

type ReleaseDocDao interface {
	CreateReleaseDocRecords(ctx context.Context, releaseDocs []*releaseEntity.ReleaseDoc, tx *gorm.DB) error
	UpdateReleaseDocRecords(ctx context.Context, releaseDocs []*releaseEntity.ReleaseDoc, tx *gorm.DB) error
	UpdateReleaseDocRecord(ctx context.Context, updateColumns map[string]any, filter *releaseEntity.ReleaseDocFilter, tx *gorm.DB) error
	GetReleaseDocIdMap(ctx context.Context, corpId, robotId, versionId uint64,
		docIds []uint64) (map[uint64]struct{}, error)
	GetModifyDocCount(ctx context.Context, robotID, versionID uint64,
		fileName string, actions []uint32, statuses []uint32) (uint64, error)
	GetModifyDocList(ctx context.Context, req *releaseEntity.ListReleaseDocReq) (
		[]*releaseEntity.ReleaseDoc, error)
	IsExistReleaseDoc(ctx context.Context, filter *releaseEntity.ReleaseDocFilter) (bool, error)
}

type ReleaseConfigDao interface {
	CreateReleaseConfigRecords(ctx context.Context, releaseConfigs []*releaseEntity.ReleaseConfig, tx *gorm.DB) error
	UpdateReleaseConfigRecords(ctx context.Context, releaseConfigs []*releaseEntity.ReleaseConfig, tx *gorm.DB) error
	GetAuditConfigItemByVersion(ctx context.Context, versionID uint64) (
		[]*releaseEntity.AuditReleaseConfig, error)
	GetReleaseConfigItemByID(ctx context.Context, id uint64) (*releaseEntity.ReleaseConfig, error)
	UpdateAuditConfigItem(ctx context.Context, cfg *releaseEntity.ReleaseConfig) error
	GetReleaseConfigAuditStat(ctx context.Context, versionID uint64) (
		map[uint32]*releaseEntity.AuditResultStat, error)
	GetModifyReleaseConfigCount(ctx context.Context, versionID uint64,
		releaseStatuses []uint32, query string) (uint64, error)
	GetConfigItemByVersionID(ctx context.Context, versionID uint64) ([]*releaseEntity.ReleaseConfig, error)
	GetInAppealConfigItem(ctx context.Context, robotID uint64) ([]*releaseEntity.ReleaseConfig, error)
	ListConfigByVersionID(ctx context.Context, listReq *releaseEntity.ListReleaseConfigReq) (
		[]*releaseEntity.ReleaseConfig, error)
}

type ReleaseSegmentDao interface {
	CreateReleaseSegmentRecords(ctx context.Context, releaseSegments []*releaseEntity.ReleaseSegment, tx *gorm.DB) error
	UpdateReleaseSegmentRecords(ctx context.Context, updateColumns []string,
		filter *releaseEntity.ReleaseSegmentFilter, releaseSeg *releaseEntity.ReleaseSegment, tx *gorm.DB) (uint64, error)
	BatchUpdateReleaseSegmentRecords(ctx context.Context, updateColumns map[string]any,
		filter *releaseEntity.ReleaseSegmentFilter, tx *gorm.DB) (uint64, error)
	GetDocIDInReleaseDocSegements(ctx context.Context, release *releaseEntity.Release) (
		[]uint64, error)
	GetReleaseModifySegment(ctx context.Context, release *releaseEntity.Release,
		segments []*segEntity.DocSegmentExtend) (map[uint64]*releaseEntity.ReleaseSegment, error)
	GetModifySegmentCount(ctx context.Context, robotID, versionID uint64, action uint32, releaseStatuses []uint32) (uint64, error)
	GetModifySegmentList(ctx context.Context,
		req *releaseEntity.ListReleaseSegmentReq) ([]*releaseEntity.ReleaseSegment, error)
	ClearRealtimeAppResourceReleaseSegment(ctx context.Context, removeTime int64, tx *gorm.DB) error
	IsExistReleaseSegment(ctx context.Context, filter *releaseEntity.ReleaseSegmentFilter) (bool, error)
}

type ReleaseRejectedQuestionDao interface {
	CreateReleaseRejectedQuestionRecords(ctx context.Context,
		releaseRejectedQuestions []*releaseEntity.ReleaseRejectedQuestion, tx *gorm.DB) error
	UpdateReleaseRejectedQuestionRecords(ctx context.Context, updateColumns []string,
		filter *releaseEntity.ReleaseRejectedQuestionFilter, rejectedQuestion *releaseEntity.ReleaseRejectedQuestion, tx *gorm.DB) error
	GetModifyRejectedQuestionCount(ctx context.Context,
		corpID, robotID, versionID uint64, question string, releaseStatuses []uint32) (
		uint64, error)
	GetModifyRejectedQuestionList(ctx context.Context,
		req releaseEntity.ListReleaseRejectedQuestionReq) ([]*releaseEntity.ReleaseRejectedQuestion, error)
	GetReleaseModifyRejectedQuestion(ctx context.Context, release *releaseEntity.Release,
		rejectedQuestion []*qaEntity.RejectedQuestion) (map[uint64]*releaseEntity.ReleaseRejectedQuestion, error)
	GetReleaseRejectedQuestionByVersion(ctx context.Context, corpID uint64, robotID uint64,
		versionID uint64) ([]*releaseEntity.ReleaseRejectedQuestion, error)
	IsExistReleaseRejectedQuestion(ctx context.Context, filter *releaseEntity.ReleaseRejectedQuestionFilter) (bool, error)
}

type ReleaseQASimilarQuestionDao interface {
	CreateReleaseSimilarQuestionRecord(ctx context.Context, sqs []*releaseEntity.ReleaseQaSimilarQuestion, tx *gorm.DB) error
	UpdateReleaseSimilarQuestionRecords(ctx context.Context, updateColumns []string,
		filter *releaseEntity.ReleaseQaSimilarQuestionFilter, qa *releaseEntity.ReleaseQaSimilarQuestion, tx *gorm.DB) error
	BatchUpdateReleaseSimilarQuestionRecords(ctx context.Context, updateColumns map[string]any,
		filter *releaseEntity.ReleaseQaSimilarQuestionFilter, tx *gorm.DB) (uint64, error)
	GetReleaseSimilarQuestionList(ctx context.Context, selectColumns []string,
		filter *releaseEntity.ReleaseQaSimilarQuestionFilter) ([]*releaseEntity.ReleaseQaSimilarQuestion, error)
	GetReleaseSimilarQuestionListCount(ctx context.Context, selectColumns []string,
		filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (uint64, error)
	GetReleaseSimilarQACountByGroup(ctx context.Context, groupField string, filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (
		[]*releaseEntity.ReleaseQaSimilarQuestionState, error)
	GetReleaseSimilarQuestion(ctx context.Context, selectColumns []string,
		filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (*releaseEntity.ReleaseQaSimilarQuestion, error)
	IsExistReleaseQaSimilar(ctx context.Context, filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (bool, error)
	GetSimilarIDWithDeleteFlag(ctx context.Context, robotID uint64, versionID uint64, similarID,
		limit uint64, isDeleted bool) ([]uint64, error)
}

type ReleaseAuditDao interface {
	GetAuditByFilter(ctx context.Context, selectColumns []string, filter *releaseEntity.AuditFilter) (*releaseEntity.Audit, error)
	GetAuditList(ctx context.Context, selectColumns []string, filter *releaseEntity.AuditFilter) (
		[]*releaseEntity.Audit, error)
	GetParentAuditsByParentRelateID(ctx context.Context, filter *releaseEntity.AuditFilter) ([]*releaseEntity.Audit, error)
	BatchGetAuditList(ctx context.Context, selectColumns []string, filter *releaseEntity.AuditFilter) (
		[]*releaseEntity.Audit, error)
	GetBizAuditStatusStat(ctx context.Context, id, corpID, robotID uint64) ([]*releaseEntity.AuditStatusStat, error)
	GetBizAuditStatusByRelateIDs(ctx context.Context, robotID, corpID uint64,
		relateIDs []uint64) ([]*releaseEntity.AuditRelateID, error)
	CreateAudit(ctx context.Context, tx *gorm.DB, audit *releaseEntity.Audit) (uint64, error)
	CreateAuditByAuditSendParams(ctx context.Context, p entity.AuditSendParams, tx *gorm.DB) (*releaseEntity.Audit, error)
	BatchCreateAudit(ctx context.Context, audits []*releaseEntity.Audit, tx *gorm.DB) error
	UpdateAudit(ctx context.Context, updateColumns []string, filter *releaseEntity.AuditFilter,
		audit *releaseEntity.Audit, tx *gorm.DB) (int64, error)
}

type ReleaseAtttributeDao interface {
	GetReleaseAttributeCount(ctx context.Context, robotID, versionID uint64, name string,
		actions []uint32, tx *gorm.DB) (uint64, error)
	GetReleaseAttributeList(ctx context.Context, robotID, versionID uint64, name string, actions []uint32,
		page, pageSize uint32, tx *gorm.DB) ([]*releaseEntity.ReleaseAttribute, error)
	GetReleaseAttributeLabels(ctx context.Context, robotID, versionID uint64, attrIDs []uint64, tx *gorm.DB,
	) (map[uint64][]*releaseEntity.ReleaseAttributeLabel, error)

	CreateReleaseAttribute(ctx context.Context, releaseAttributes *releaseEntity.ReleaseAttribute, tx *gorm.DB) error
	BatchUpdateReleaseAttribute(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteFilter, updateColumns map[string]any, tx *gorm.DB) (uint64, error)
	UpdateReleaseAttributeStatus(ctx context.Context, releaseAttribute *releaseEntity.ReleaseAttribute, tx *gorm.DB) error
	CreateReleaseAttributeLabel(ctx context.Context, releaseAttributeLabels *releaseEntity.ReleaseAttributeLabel, tx *gorm.DB) error
	BatchUpdateReleaseAttributeLabel(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteLabelFilter, updateColumns map[string]any, tx *gorm.DB) (uint64, error)
	UpdateReleaseAttributeLabelStatus(ctx context.Context, label *releaseEntity.ReleaseAttributeLabel, tx *gorm.DB) error
	IsExistReleaseAttribute(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteFilter, tx *gorm.DB) (bool, error)
	IsExistReleaseAttributeLabel(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteLabelFilter, tx *gorm.DB) (bool, error)
}

type DevReleaseRelationInfoDao interface {
	// GetDevReleaseRelationInfoList 根据条件批量查询开发域和发布域的关联关系
	// 返回: map[dev_business_id]release_business_id
	GetDevReleaseRelationInfoList(ctx context.Context, corpID, robotID uint64, relationType uint32, devBusinessIDs []uint64) (map[uint64]uint64, error)
}

type Dao interface {
	ReleaseDao
	ReleaseQADao
	ReleaseDocDao
	ReleaseConfigDao
	ReleaseSegmentDao
	ReleaseRejectedQuestionDao
	ReleaseQASimilarQuestionDao
	ReleaseAuditDao
	ReleaseAtttributeDao
	DevReleaseRelationInfoDao

	MysqlQuery() *mysqlquery.Query
	Query() *tdsqlquery.Query
}

func NewDao(mysqlDB types.MySQLDB, tdsqlDB types.TDSQLDB) Dao {
	return &daoImpl{
		mysql: mysqlquery.Use(mysqlDB),
		tdsql: tdsqlquery.Use(tdsqlDB),
	}
}

func (d *daoImpl) MysqlQuery() *mysqlquery.Query {
	return d.mysql
}

func (d *daoImpl) Query() *tdsqlquery.Query {
	return d.tdsql
}

type daoImpl struct {
	mysql *mysqlquery.Query
	tdsql *tdsqlquery.Query
}
