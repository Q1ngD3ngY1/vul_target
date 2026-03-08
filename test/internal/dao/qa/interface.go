package qa

import (
	"context"
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"gorm.io/gorm"
)

type DocQADao interface {
	// GetDocQas(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error)
	GetAllDocQas(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error)
	GetDocQaList(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error)
	GetDocQasByPagenation(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter, pagenation bool) ([]*qaEntity.DocQA, error)
	GetDocQaCountWithTx(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter, tx *gorm.DB) (int64, error)
	GetQaByFilterWithTx(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter, tx *gorm.DB) (*qaEntity.DocQA, error)
	CreateDocQa(ctx context.Context, docQa *qaEntity.DocQA) error
	UpdateDocQas(ctx context.Context, updateColumns []string, filter *qaEntity.DocQaFilter, docQa *qaEntity.DocQA) error
	UpdateDocQasWithTx(ctx context.Context, updateColumns []string, filter *qaEntity.DocQaFilter, docQa *qaEntity.DocQA, tx *gorm.DB) (int64, error)
	BatchUpdateDocQALastActionByID(ctx context.Context, docQAID []uint64, action uint32) error
	BatchUpdateDocQAReleaseStatusByID(ctx context.Context, docQAID []uint64, releaseStatus uint32, tx *gorm.DB) error
	BatchUpdateDocQA(ctx context.Context, filter *qaEntity.DocQaFilter, updateFields map[string]any, tx *gorm.DB) (int64, error)

	GetDocQANum(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter, tx *gorm.DB) ([]*qaEntity.DocQANum, error)
	GetQAByID(ctx context.Context, docQAID uint64) (*qaEntity.DocQA, error)
	GetQAsByBizIDs(ctx context.Context, corpID, robotID uint64, qaBizIDs []uint64, offset, limit int,
	) ([]*qaEntity.DocQA, error)
	GetReleaseQACount(ctx context.Context, corpID, robotID uint64, question string, startTime,
		endTime time.Time, actions []uint32) (uint64, error)
	GetReleaseQAList(ctx context.Context, corpID, robotID uint64, question string, startTime,
		endTime time.Time, actions []uint32, page, pageSize uint32) ([]*qaEntity.DocQA, error)
	GetRobotQAUsage(ctx context.Context, robotID uint64, corpID uint64) (entity.CapacityUsage, error)
	GetRobotQAExceedUsage(ctx context.Context, corpID uint64, robotIDs []uint64) (map[uint64]entity.CapacityUsage, error)
	RecordUserAccessUnCheckQATime(ctx context.Context, robotID, staffID uint64) error
	GetQADetailsByReleaseStatus(ctx context.Context, corpID, robotID uint64, ids []uint64,
		releaseStatus uint32) (map[uint64]*qaEntity.DocQA, error)
	UpdateAppealQA(ctx context.Context, qaIDs, simIDs []uint64,
		releaseStatus, isAuditFree uint32) error
	// GetQAsByCursor 游标分页获取QA（用于导出）
	GetQAsByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int) ([]*qaEntity.DocQA, error)
}

type RejectedQuestionDao interface {
	CreateRejectedQuestion(ctx context.Context, rejectedQuestion *qaEntity.RejectedQuestion) error
	UpdateRejectedQuestion(ctx context.Context, filter *qaEntity.RejectedQuestionFilter,
		updateColumns []string, rqa *qaEntity.RejectedQuestion, tx *gorm.DB) error
	GetRejectedQuestionListCount(ctx context.Context, req *qaEntity.RejectedQuestionFilter) (int64, error)
	GetReleaseRejectedQuestionCount(ctx context.Context, corpID, robotID uint64, question string, startTime,
		endTime time.Time, status []uint32) (uint64, error)
	GetReleaseRejectedQuestionList(ctx context.Context, corpID, robotID uint64, page, pageSize uint32,
		query string, startTime, endTime time.Time, status []uint32) ([]*qaEntity.RejectedQuestion, error)
	GetRejectedQuestionList(ctx context.Context, selectColumns []string, req *qaEntity.RejectedQuestionFilter) (
		[]*qaEntity.RejectedQuestion, error)
	ListRejectedQuestion(ctx context.Context, selectColumns []string, req *qaEntity.RejectedQuestionFilter) (
		[]*qaEntity.RejectedQuestion, int64, error)
	GetRejectedQuestion(ctx context.Context, req *qaEntity.RejectedQuestionFilter) (*qaEntity.RejectedQuestion, error)
	GetRejectedQuestionByID(ctx context.Context, id uint64) (*qaEntity.RejectedQuestion, error)

	BatchUpdateRejectedQuestion(ctx context.Context, filter *qaEntity.RejectedQuestionFilter, updateColumns map[string]any, tx *gorm.DB) error
	BatchUpdateRejectedQuestions(ctx context.Context, rejectedQuestions []*qaEntity.RejectedQuestion) error
}

type SimilarQuestion interface {
	CreateSimilarQuestion(ctx context.Context, tx *gorm.DB, qa *qaEntity.SimilarQuestion) error
	BatchCreateSimilarQuestions(ctx context.Context, qas []*qaEntity.SimilarQuestion) error
	UpdateSimilarQuestion(ctx context.Context, tx *gorm.DB,
		updateColumns []string, req *qaEntity.SimilarityQuestionReq, sq *qaEntity.SimilarQuestion) error
	BatchUpdateSimilarQuestion(ctx context.Context, req *qaEntity.SimilarityQuestionReq, updatedFieleds map[string]any, tx *gorm.DB) error
	GetSimilarQuestionBySimilarID(ctx context.Context, relatedQAId uint64, similarQuestionID uint64) (*qaEntity.SimilarQuestion, error)
	GetSimilarQuestionByFilter(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) (*qaEntity.SimilarQuestion, error)
	ListSimilarQuestions(ctx context.Context, selectColumns []string, req *qaEntity.SimilarityQuestionReq) ([]*qaEntity.SimilarQuestion, error)
	BatchListSimilarQuestions(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) ([]*qaEntity.SimilarQuestion, error)
	GetQASimilarQuestionsCount(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) (int, error)
	GetSimilarQuestionsCountByQAIDs(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) ([]*qaEntity.SimilarQuestionCount, error)
	GetSimilarQuestionsCharSize(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) (uint64, uint64, error)
}

type QATaskDao interface {
	CreateDocQATask(ctx context.Context, docQaTask *qaEntity.DocQATask, tx *gorm.DB) error
	UpdateDocQATasks(ctx context.Context, updateColumns []string, filter *qaEntity.DocQaTaskFilter, docQaTask *qaEntity.DocQATask, tx *gorm.DB) error
	BatchUpdateDocQATasks(ctx context.Context, filter *qaEntity.DocQaTaskFilter, updatedFieleds map[string]any, tx *gorm.DB) error
	GetDocQaTaskList(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaTaskFilter) ([]*qaEntity.DocQATask, error)
	GetDocQaTaskListCount(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaTaskFilter) (int64, error)
	ListDocQaTasks(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaTaskFilter) (uint64, []*qaEntity.DocQATask, error)
	GetDocQaTaskByFilter(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaTaskFilter) (*qaEntity.DocQATask, error)
	GetDocQATaskGeneratingMaps(ctx context.Context, corpID, robotID uint64, docID []uint64) (map[uint64]*qaEntity.DocQATask, error)
}

type DocQASimilar interface {
	CreateDocQASimilar(ctx context.Context, docQaSimilar *qaEntity.DocQASimilar) error
	UpdateDocQASimilar(ctx context.Context, updateColumns []string, filter *qaEntity.DocQASimilarFilter, docQaSimilar *qaEntity.DocQASimilar) error
	CheckDedupDocQASimilarCount(ctx context.Context, docQaSimilar *qaEntity.DocQASimilar) (int64, error)
	BatchUpdateDocQASimilar(ctx context.Context, filter *qaEntity.DocQASimilarFilter, updatedFieleds map[string]any, tx *gorm.DB) error
	DeleteQASimilarByQA(ctx context.Context, qa *qaEntity.DocQA) error
	DeleteQASimilarByBizIDs(ctx context.Context, businessIDs []uint64) error
	ListDocQaSimilars(ctx context.Context, selectColumns []string, filter *qaEntity.DocQASimilarFilter) ([]*qaEntity.DocQASimilar, int64, error)
	GetDocQaSimilarList(ctx context.Context, selectColumns []string, filter *qaEntity.DocQASimilarFilter) ([]*qaEntity.DocQASimilar, error)
	GetDocQaSimilarListCount(ctx context.Context, filter *qaEntity.DocQASimilarFilter) (int64, error)
	GetDocQaSimilarByFilter(ctx context.Context, filter *qaEntity.DocQASimilarFilter) (*qaEntity.DocQASimilar, error)
}
type Dao interface {
	Query() *mysqlquery.Query
	RedisCli() types.AdminRedis

	DocQADao
	RejectedQuestionDao
	SimilarQuestion
	QATaskDao
	DocQASimilar
}

func NewDao(mysqlDB types.MySQLDB, tdsqlDB types.TDSQLDB, adminRdb types.AdminRedis) Dao {
	return &daoImpl{
		mysql:    mysqlquery.Use(mysqlDB),
		tdsql:    tdsqlquery.Use(tdsqlDB),
		adminRdb: adminRdb,
	}
}

type daoImpl struct {
	mysql    *mysqlquery.Query
	tdsql    *tdsqlquery.Query
	adminRdb types.AdminRedis
}

func (d *daoImpl) RedisCli() types.AdminRedis {
	return d.adminRdb
}

func (d *daoImpl) Query() *mysqlquery.Query {
	return d.mysql
}

func (d *daoImpl) getDocQaUpdateColumns(fields map[string]any) map[string]any {
	updatedColumns := map[string]any{}
	tbl := d.mysql.TDocQa
	for k, v := range fields {
		if c, ok := tbl.GetFieldByName(k); ok {
			updatedColumns[c.ColumnName().String()] = v
		}
	}
	return updatedColumns
}

func (d *daoImpl) getSimilarityQuestionUpdateColumns(fields map[string]any) map[string]any {
	updatedColumns := map[string]any{}
	tbl := d.mysql.TQaSimilarQuestion
	for k, v := range fields {
		if c, ok := tbl.GetFieldByName(k); ok {
			updatedColumns[c.ColumnName().String()] = v
		}
	}
	return updatedColumns
}
