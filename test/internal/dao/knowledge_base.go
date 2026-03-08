package dao

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 知识库表
const (
	knowledgeBaseTableName = "t_knowledge_base"

	KnowledgeBaseTblColId             = "id"
	KnowledgeBaseTblColCorpBizId      = "corp_biz_id"
	KnowledgeBaseTblColKnowBizId      = "knowledge_biz_id"
	KnowledgeBaseTblColProcessingFlag = "processing_flag"
	KnowledgeBaseTblColIsDeleted      = "is_deleted"  // 是否删除
	KnowledgeBaseTblColCreateTime     = "create_time" // 创建时间
	KnowledgeBaseTblColUpdateTime     = "update_time" // 更新时间
)

type KnowledgeBaseDao struct {
	DB *gorm.DB
}

// GetKnowledgeBaseDao 获取全局的数据操作对象
func GetKnowledgeBaseDao(db *gorm.DB) *KnowledgeBaseDao {
	if db == nil {
		db = globalBaseDao.tdsqlGormDB
	}
	return &KnowledgeBaseDao{DB: db}
}

// SetKnowledgeBase 设置知识库信息
func (d *KnowledgeBaseDao) SetKnowledgeBase(ctx context.Context, corpBizId, knowledgeBizId uint64, processingFlag uint64) error {
	knowledgeBase := model.KnowledgeBase{
		CorpBizID:      corpBizId,
		KnowledgeBizId: knowledgeBizId,
		ProcessingFlag: processingFlag,
		IsDeleted:      IsNotDeleted,
		CreateTime:     time.Now(),
		UpdateTime:     time.Now(),
	}
	err := d.DB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: KnowledgeBaseTblColCorpBizId},
			{Name: KnowledgeBaseTblColKnowBizId},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			KnowledgeBaseTblColProcessingFlag,
			KnowledgeBaseTblColUpdateTime,
			KnowledgeBaseTblColIsDeleted,
		}), // 冲突时更新 flag,update_time,is_deleted 字段
	}).Create(&knowledgeBase).Error
	if err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeBase set corpBizId:%d knowledgeBizId:%d processingFlag:%d err:%+v",
			corpBizId, knowledgeBizId, processingFlag, err)
		return err
	}
	return nil
}

// GetKnowledgeBases 获取知识库信息
func (d *KnowledgeBaseDao) GetKnowledgeBases(ctx context.Context,
	corpBizId uint64, knowledgeBizIds []uint64) ([]*model.KnowledgeBase, error) {
	var knowledgeBases []*model.KnowledgeBase
	err := d.DB.WithContext(ctx).Model(&model.KnowledgeBase{}).
		Where(KnowledgeBaseTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeBaseTblColKnowBizId+sqlIn, knowledgeBizIds).
		Where(KnowledgeBaseTblColIsDeleted+sqlEqual, IsNotDeleted).
		Find(&knowledgeBases).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeBase corpBizId:%d knowledgeBizIds:%d err:%+v",
			corpBizId, knowledgeBizIds, err)
		return nil, err
	}
	return knowledgeBases, err
}

// DeleteKnowledgeBases 删除知识库信息
func (d *KnowledgeBaseDao) DeleteKnowledgeBases(ctx context.Context, corpBizId uint64,
	knowledgeBizIds []uint64) error {
	knowledgeBase := &model.KnowledgeBase{
		IsDeleted: IsDeleted,
	}
	updateColumns := []string{KnowledgeBaseTblColIsDeleted}
	res := d.DB.WithContext(ctx).Table(knowledgeBaseTableName).Select(updateColumns).
		Where(KnowledgeBaseTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeBaseTblColKnowBizId+sqlIn, knowledgeBizIds).
		Updates(knowledgeBase)
	if res.Error != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeBases corpBizId:%+v knowledgeBizIds:%+v err:%+v",
			corpBizId, knowledgeBizIds, res.Error)
		return res.Error
	}

	return nil
}
