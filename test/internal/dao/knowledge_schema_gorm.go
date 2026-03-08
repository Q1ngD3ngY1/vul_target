package dao

import (
	"context"
	"errors"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"gorm.io/gorm"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalKnowledgeSchemaDao *KnowledgeSchemaDao

const (
	knowledgeSchemaTableName     = "t_knowledge_schema"
	knowledgeSchemaProdTableName = "t_knowledge_schema_prod"

	KnowledgeSchemaTblColId          = "id"
	KnowledgeSchemaTblColCorpBizId   = "corp_biz_id" // 企业业务ID
	KnowledgeSchemaTblColAppBizId    = "app_biz_id"  // 应用业务ID
	KnowledgeSchemaTblColVersion     = "version"     // 版本id，对应任务表的自增id
	KnowledgeSchemaTblColItemType    = "item_type"   // 物料类型,1:文档 2:文档聚类
	KnowledgeSchemaTblColItemBizType = "item_biz_id" // 物料ID：文档业务ID或文档聚类业务ID
	KnowledgeSchemaTblColItemName    = "name"        // 文档或者文档聚类名
	KnowledgeSchemaTblColItemSummary = "summary"     // 文档或者文档聚类摘要
	KnowledgeSchemaTblColIsDeleted   = "is_deleted"  // 是否删除
	KnowledgeSchemaTblColCreateTime  = "create_time" // 创建时间
	KnowledgeSchemaTblColUpdateTime  = "update_time" // 更新时间
)

var KnowledgeSchemaTblColList = []string{
	KnowledgeSchemaTblColId,
	KnowledgeSchemaTblColCorpBizId,
	KnowledgeSchemaTblColAppBizId,
	KnowledgeSchemaTblColVersion,
	KnowledgeSchemaTblColItemType,
	KnowledgeSchemaTblColItemBizType,
	KnowledgeSchemaTblColItemName,
	KnowledgeSchemaTblColItemSummary,
	KnowledgeSchemaTblColIsDeleted,
	KnowledgeSchemaTblColCreateTime,
	KnowledgeSchemaTblColUpdateTime,
}

type KnowledgeSchemaDao struct {
	BaseDao
}

// GetKnowledgeSchemaDao 获取全局的数据操作对象
func GetKnowledgeSchemaDao() *KnowledgeSchemaDao {
	if globalKnowledgeSchemaDao == nil {
		globalKnowledgeSchemaDao = &KnowledgeSchemaDao{*globalBaseDao}
	}
	return globalKnowledgeSchemaDao
}

type KnowledgeSchemaFilter struct {
	AppBizId       uint64 // 应用业务ID
	EnvType        string // 环境类型
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string

	IsDeleted *int
}

// CreateKnowledgeSchema 创建知识库schema
func (k *KnowledgeSchemaDao) CreateKnowledgeSchema(ctx context.Context, tx *gorm.DB, knowledgeSchema *model.KnowledgeSchema) error {
	if tx == nil {
		tx = k.tdsqlGormDB
	}
	res := tx.WithContext(ctx).Table(knowledgeSchemaTableName).Create(knowledgeSchema)
	if res.Error != nil {
		log.ErrorContextf(ctx, "CreateKnowledgeSchema execute sql failed, err=%+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func generateKnowledgeSchemaCondition(ctx context.Context, session *gorm.DB, filter *KnowledgeSchemaFilter) {
	if filter.AppBizId != 0 {
		session = session.Where(KnowledgeSchemaTblColAppBizId+sqlEqual, filter.AppBizId)
	}
	if filter.Limit > 0 {
		session = session.Limit(int(filter.Limit))
	}
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "generateKnowledgeSchemaCondition invalid order direction: %s",
				filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	// is_deleted必须加入查询条件, 否则默认查询未删除的
	if filter.IsDeleted != nil {
		session = session.Where(KnowledgeSchemaTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	} else {
		session = session.Where(KnowledgeSchemaTblColIsDeleted+sqlEqual, IsNotDeleted)
	}
}

// FindKnowledgeSchema 获取知识库schema
func (k *KnowledgeSchemaDao) FindKnowledgeSchema(
	ctx context.Context,
	selectColumns []string,
	filter *KnowledgeSchemaFilter,
) ([]*model.KnowledgeSchema, error) {
	if len(filter.OrderColumn) != len(filter.OrderDirection) {
		log.ErrorContextf(ctx, "GetKnowledgeSchema invalid order length, "+
			"len(OrderColumn)=%d, len(OrderDirection)=%d", len(filter.OrderColumn), len(filter.OrderDirection))
		return nil, errors.New("invalid order length")
	}

	tableName := knowledgeSchemaTableName
	if filter.EnvType == model.EnvTypeProduct {
		tableName = knowledgeSchemaProdTableName
	}
	var knowledgeSchemaList []*model.KnowledgeSchema
	session := k.tdsqlGormDB.WithContext(ctx).Table(tableName).Select(selectColumns)
	generateKnowledgeSchemaCondition(ctx, session, filter)

	res := session.Find(&knowledgeSchemaList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema execute sql failed, err=%+v", res.Error)
		return knowledgeSchemaList, res.Error
	}
	return knowledgeSchemaList, nil
}

// DeleteKnowledgeSchema 硬性删除知识库schema
func (k *KnowledgeSchemaDao) DeleteKnowledgeSchema(ctx context.Context, tx *gorm.DB, corpBizId, appBizId uint64) error {
	if tx == nil {
		tx = k.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(knowledgeSchemaTableName).
		Where(KnowledgeSchemaTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeSchemaTblColAppBizId+sqlEqual, appBizId)
	res := session.Delete(&model.KnowledgeSchema{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeSchema failed for appBizId: %+v, err: %+v",
			appBizId, res.Error)
		return res.Error
	}
	return nil
}

// GetKnowledgeSchemaMaxVersion 获取知识库schema最大版本
func (k *KnowledgeSchemaDao) GetKnowledgeSchemaMaxVersion(ctx context.Context, appBizId uint64) (uint64, error) {
	// 使用Select和Row获取最大值
	maxVersion := uint64(0)
	row := k.tdsqlGormDB.WithContext(ctx).Table(knowledgeSchemaTableName).
		Select("COALESCE(MAX(version), 0) as max_version").
		Where(KnowledgeSchemaTblColAppBizId+sqlEqual, appBizId).Row()
	if err := row.Scan(&maxVersion); err != nil {
		return 0, err
	}
	return maxVersion, nil
}
