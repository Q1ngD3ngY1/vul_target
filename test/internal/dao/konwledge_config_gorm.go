package dao

import (
	"context"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 应用配置表
const (
	knowledgeConfigTableName = "t_knowledge_config"

	KnowledgeConfigTblColId         = "id"
	KnowledgeConfigTblColCorpBizId  = "corp_biz_id"
	KnowledgeConfigTblColKnowBizId  = "knowledge_biz_id"
	KnowledgeConfigTblColType       = "type"
	KnowledgeConfigTblColConfig     = "config"
	KnowledgeConfigTblColIsDeleted  = "is_deleted"  // 是否删除
	KnowledgeConfigTblColCreateTime = "create_time" // 创建时间
	KnowledgeConfigTblColUpdateTime = "update_time" // 更新时间
)

type KnowledgeConfigDao struct {
	DB *gorm.DB
}

// GetKnowledgeConfigDao 获取全局的数据操作对象
func GetKnowledgeConfigDao(db *gorm.DB) *KnowledgeConfigDao {
	if db == nil {
		db = globalBaseDao.tdsqlGormDB
	}
	return &KnowledgeConfigDao{DB: db}
}

// SetKnowledgeConfig 设置知识库配置
func (d *KnowledgeConfigDao) SetKnowledgeConfig(ctx context.Context, corpBizId, knowledgeBizId uint64,
	configType uint32, config string) error {
	//t_knowledge_config有唯一索引`uk_biz_type` (`corp_biz_id`, `knowledge_biz_id`, `type`)
	knowledgeConfig := model.KnowledgeConfig{
		CorpBizID:      corpBizId,
		KnowledgeBizId: knowledgeBizId,
		Type:           configType,
		Config:         config,
		IsDeleted:      0,
		CreateTime:     time.Now(),
		UpdateTime:     time.Now(),
	}
	err := d.DB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: KnowledgeConfigTblColCorpBizId},
			{Name: KnowledgeConfigTblColKnowBizId},
			{Name: KnowledgeConfigTblColType},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			KnowledgeConfigTblColConfig,
			KnowledgeConfigTblColUpdateTime,
			KnowledgeConfigTblColIsDeleted,
		}), // 冲突时更新 config,update_time,is_deleted 字段
	}).Create(&knowledgeConfig).Error
	if err != nil {
		log.ErrorContextf(ctx, "SetAppConfig set err:%v,corpBizId:%v,knowledgeBizId:%v,configType:%v,"+
			"config:%v", err, corpBizId, knowledgeBizId, configType, config)
		return err
	}
	return nil
}

// GetKnowledgeConfigs 获取知识库配置
func (d *KnowledgeConfigDao) GetKnowledgeConfigs(ctx context.Context,
	corpBizId uint64, knowledgeBizIds []uint64, configTypes []uint32) ([]*model.KnowledgeConfig, error) {
	var configs []*model.KnowledgeConfig
	err := d.DB.WithContext(ctx).Model(&model.KnowledgeConfig{}).
		Where(KnowledgeConfigTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeConfigTblColKnowBizId+sqlIn, knowledgeBizIds).
		Where(KnowledgeConfigTblColType+sqlIn, configTypes).
		Where(KnowledgeConfigTblColIsDeleted+sqlEqual, IsNotDeleted).
		Find(&configs).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeConfig err:%v,corpBizId:%v,knowledgeBizIds:%v,configTypes:%v",
			err, corpBizId, knowledgeBizIds, configTypes)
		return nil, err
	}
	return configs, err
}

// DeleteKnowledgeConfigs 删除知识库配置
func (d *KnowledgeConfigDao) DeleteKnowledgeConfigs(ctx context.Context, corpBizId uint64,
	knowledgeBizIds []uint64) error {
	config := &model.KnowledgeConfig{
		IsDeleted: IsDeleted,
	}
	updateColumns := []string{KnowledgeConfigTblColIsDeleted}
	res := d.DB.WithContext(ctx).Table(knowledgeConfigTableName).Select(updateColumns).
		Where(KnowledgeConfigTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeConfigTblColKnowBizId+sqlIn, knowledgeBizIds).
		Updates(config)
	if res.Error != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeConfigs corpBizId:%+v,knowledgeBizIds:%+v err:%+v",
			corpBizId, knowledgeBizIds, res.Error)
		return res.Error
	}

	return nil
}

// GetKnowledgeConfigsByModelAssociated 通过模型信息，获取知识库配置
func (d *KnowledgeConfigDao) GetKnowledgeConfigsByModelAssociated(ctx context.Context,
	corpBizId uint64, modelKeyword string) ([]*model.KnowledgeConfig, error) {
	var configs []*model.KnowledgeConfig
	err := d.DB.WithContext(ctx).Model(&model.KnowledgeConfig{}).
		Where(KnowledgeConfigTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeConfigTblColType+sqlIn, []uint32{
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
			uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL),
			uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
		}).
		Where(KnowledgeConfigTblColIsDeleted+sqlEqual, IsNotDeleted).
		Where(KnowledgeConfigTblColConfig+sqlLike, "%"+modelKeyword+"%").
		Find(&configs).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeConfigByModelAssociated err:%v, corpBizId:%v ,modelKeyword:%v",
			err, corpBizId, modelKeyword)
		return nil, err
	}
	return configs, err
}
