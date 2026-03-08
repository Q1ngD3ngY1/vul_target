package dao

import (
	"context"
	"errors"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"gorm.io/gorm"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalKnowledgeSchemaTaskDao *KnowledgeSchemaTaskDao

const (
	knowledgeSchemaTaskTableName = "t_knowledge_schema_task"

	KnowledgeSchemaTaskTblColId         = "id"
	KnowledgeSchemaTaskTblColCorpBizId  = "corp_biz_id" // 企业业务ID
	KnowledgeSchemaTaskTblColAppBizId   = "app_biz_id"  // 应用业务ID
	KnowledgeSchemaTaskTblColBusinessId = "business_id" // 业务ID
	KnowledgeSchemaTaskTblColStatus     = "status"      // 状态(0待处理 1处理中 2处理成功 3处理失败)
	KnowledgeSchemaTaskTblColStatusCode = "status_code" // 状态(0待处理 1处理中 2处理成功 3处理失败)
	KnowledgeSchemaTaskTblColIsDeleted  = "is_deleted"  // 是否删除
	KnowledgeSchemaTaskTblColMessage    = "message"     // 是否删除
	KnowledgeSchemaTaskTblColCreateTime = "create_time" // 创建时间
	KnowledgeSchemaTaskTblColUpdateTime = "update_time" // 更新时间
)

var KnowledgeSchemaTaskTblColList = []string{
	KnowledgeSchemaTaskTblColId,
	KnowledgeSchemaTaskTblColCorpBizId,
	KnowledgeSchemaTaskTblColAppBizId,
	KnowledgeSchemaTaskTblColBusinessId,
	KnowledgeSchemaTaskTblColStatus,
	KnowledgeSchemaTaskTblColIsDeleted,
	KnowledgeSchemaTaskTblColMessage,
	KnowledgeSchemaTaskTblColCreateTime,
	KnowledgeSchemaTaskTblColUpdateTime,
}

type KnowledgeSchemaTaskDao struct {
	BaseDao
}

// GetKnowledgeSchemaTaskDao 获取全局的数据操作对象
func GetKnowledgeSchemaTaskDao() *KnowledgeSchemaTaskDao {
	if globalKnowledgeSchemaTaskDao == nil {
		globalKnowledgeSchemaTaskDao = &KnowledgeSchemaTaskDao{*globalBaseDao}
	}
	return globalKnowledgeSchemaTaskDao
}

type KnowledgeSchemaTaskFilter struct {
	AppBizId       uint64 // 应用业务ID
	Statuses       []int32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string

	IsDeleted *int
}

// 生成查询条件，必须按照索引的顺序排列
func generateKnowledgeSchemaTaskCondition(ctx context.Context, session *gorm.DB, filter *KnowledgeSchemaTaskFilter) {
	if filter.AppBizId != 0 {
		session = session.Where(KnowledgeSchemaTaskTblColAppBizId+sqlEqual, filter.AppBizId)
	}
	if len(filter.Statuses) > 0 {
		session = session.Where(DocDiffTaskTblColStatus+sqlIn, filter.Statuses)
	}
	if filter.Limit > 0 {
		session = session.Limit(int(filter.Limit))
	}
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "generateKnowledgeSchemaTaskCondition invalid order direction: %s",
				filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	// is_deleted必须加入查询条件, 否则默认查询未删除的
	if filter.IsDeleted != nil {
		session = session.Where(KnowledgeSchemaTaskTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	} else {
		session = session.Where(KnowledgeSchemaTaskTblColIsDeleted+sqlEqual, IsNotDeleted)
	}
}

// CreateKnowledgeSchemaTask 创建任务
func (k *KnowledgeSchemaTaskDao) CreateKnowledgeSchemaTask(ctx context.Context, task *model.KnowledgeSchemaTask) error {
	res := k.tdsqlGormDB.WithContext(ctx).Table(knowledgeSchemaTaskTableName).Create(&task)
	if res.Error != nil {
		log.ErrorContextf(ctx, "CreateKnowledgeSchemaTask execute sql failed, err=%+v", res.Error)
		return res.Error
	}
	return nil
}

// GetKnowledgeSchemaTask 获取知识库schema任务
func (k *KnowledgeSchemaTaskDao) GetKnowledgeSchemaTask(
	ctx context.Context,
	selectColumns []string,
	filter *KnowledgeSchemaTaskFilter,
) (*model.KnowledgeSchemaTask, error) {
	if len(filter.OrderColumn) != len(filter.OrderDirection) {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaTask invalid order length, "+
			"len(OrderColumn)=%d, len(OrderDirection)=%d", len(filter.OrderColumn), len(filter.OrderDirection))
		return nil, errors.New("invalid order length")
	}

	knowledgeSchemaTask := &model.KnowledgeSchemaTask{}
	session := k.tdsqlGormDB.WithContext(ctx).Table(knowledgeSchemaTaskTableName).Select(selectColumns)
	generateKnowledgeSchemaTaskCondition(ctx, session, filter)

	res := session.Take(knowledgeSchemaTask)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			log.WarnContextf(ctx, "GetKnowledgeSchemaTask task not exist err=%+v", res.Error)
			return knowledgeSchemaTask, errs.ErrKnowledgeSchemaTaskNotFound
		}
		log.ErrorContextf(ctx, "GetKnowledgeSchemaTask execute sql failed, err=%+v", res.Error)
		return knowledgeSchemaTask, res.Error
	}
	return knowledgeSchemaTask, nil
}

// UpdateKnowledgeSchemaTask 更新知识库schema任务
func (k *KnowledgeSchemaTaskDao) UpdateKnowledgeSchemaTask(ctx context.Context, tx *gorm.DB, updateColumns []string,
	task *model.KnowledgeSchemaTask) error {
	if tx == nil {
		tx = k.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(knowledgeSchemaTaskTableName).Select(updateColumns).
		Where(KnowledgeSchemaTaskTblColCorpBizId+sqlEqual, task.CorpBizId).
		Where(KnowledgeSchemaTaskTblColAppBizId+sqlEqual, task.AppBizId).
		Where(KnowledgeSchemaTaskTblColBusinessId+sqlEqual, task.BusinessID)
	res := session.Updates(task)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateKnowledgeSchemaTask failed for task businessID: %+v, err: %+v",
			task.BusinessID, res.Error)
		return res.Error
	}
	return nil
}
