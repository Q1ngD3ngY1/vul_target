package dao

import (
	"context"
	"database/sql"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// 库表字段定义
const (
	ShareKnowledgeTableName = "t_share_knowledge"

	ShareKnowledgeFieldCorpBizID      = "corp_biz_id"
	ShareKnowledgeFieldBusinessID     = "business_id"
	ShareKnowledgeFieldName           = "name"
	ShareKnowledgeFieldDescription    = "description"
	ShareKnowledgeFieldUserBizID      = "user_biz_id"
	ShareKnowledgeFieldUserName       = "user_name"
	ShareKnowledgeFieldEmbeddingModel = "embedding_model"
	ShareKnowledgeFieldQaExtractModel = "qa_extract_model"
	ShareKnowledgeFieldIsDeleted      = "is_deleted"
	ShareKnowledgeFieldCreateTime     = "create_time"
	ShareKnowledgeFieldUpdateTime     = "update_time"
)

const (
	DefaultSpaceID = "default_space"
)

const (
	sharedKnowledgeFields = `
		id,corp_biz_id,business_id,name,description,user_biz_id,user_name,embedding_model,qa_extract_model,
		is_deleted,create_time,update_time,space_id
	`

	getSharedKnowledge = `
		SELECT
			%s
		FROM
		    t_share_knowledge
		WHERE
		    corp_biz_id = ? AND business_id IN (%s) and is_deleted = 0
	`
)

// CreateSharedKnowledge 创建共享知识库
func (d *dao) CreateSharedKnowledge(ctx context.Context, corpBizID, knowledgeBizID uint64,
	userInfo *knowledge.UserBaseInfo,
	createInfo *knowledge.CreateSharedKnowledgeReq) (uint64, error) {
	log.InfoContextf(ctx, "CreateSharedKnowledge, corpBizID: %d, knowledgeBizID: %d, "+
		"userInfo: %+v, createInfo: %+v", corpBizID, knowledgeBizID, userInfo, createInfo)

	db := d.tdsqlGorm.WithContext(ctx)
	current := time.Now()

	info := &model.SharedKnowledgeInfo{
		CorpBizID:  corpBizID,
		BusinessID: knowledgeBizID,

		Name:        createInfo.GetKnowledgeName(),
		Description: createInfo.GetKnowledgeDescription(),
		// NOTICE: EmbeddingModel、QaExtractModel数据由模型配置管理
		//EmbeddingModel: createInfo.GetEmbeddingModel(),
		//QaExtractModel: config.App().ShareKnowledgeConfig.DefaultQaExtractModel,

		UserBizID: userInfo.GetUserBizId(),
		UserName:  userInfo.GetUserName(),

		IsDeleted:    IsNotDeleted,
		UpdateTime:   current,
		CreateTime:   current,
		SpaceID:      createInfo.GetSpaceId(),
		OwnerStaffID: pkg.StaffID(ctx),
	}
	if err := db.Table(ShareKnowledgeTableName).Create(info).Error; err != nil {
		log.ErrorContextf(ctx, "CreateSharedKnowledge failed, corpBizID: %d, knowledgeBizID: %d, error: %+v",
			corpBizID, knowledgeBizID, err)
		return 0, err
	}

	return info.ID, nil
}

// UpdateSharedKnowledge 更新共享知识库
func (d *dao) UpdateSharedKnowledge(ctx context.Context, corpBizID, knowledgeBizID uint64,
	userInfo *knowledge.UserBaseInfo,
	updateInfo *knowledge.KnowledgeUpdateInfo) (int64, error) {
	log.InfoContextf(ctx, "UpdateSharedKnowledge, corpBizID: %d, knowledgeBizID: %d, "+
		"userInfo: %+v, updateInfo: %+v", corpBizID, knowledgeBizID, userInfo, updateInfo)

	db := d.tdsqlGorm.WithContext(ctx)
	current := time.Now()

	result := db.Model(model.SharedKnowledgeInfo{}).
		Where("corp_biz_id = ?", corpBizID).
		Where("business_id = ?", knowledgeBizID).
		Where("is_deleted = ?", IsNotDeleted).
		Updates(map[string]interface{}{
			ShareKnowledgeFieldName:        updateInfo.GetKnowledgeName(),
			ShareKnowledgeFieldDescription: updateInfo.GetKnowledgeDescription(),
			// NOTICE: EmbeddingModel、QaExtractModel数据由模型配置管理
			//ShareKnowledgeFieldEmbeddingModel: updateInfo.GetEmbeddingModel(),
			//ShareKnowledgeFieldQaExtractModel: updateInfo.GetQaExtractModel(),

			ShareKnowledgeFieldUserBizID: userInfo.GetUserBizId(),
			ShareKnowledgeFieldUserName:  userInfo.GetUserName(),

			ShareKnowledgeFieldUpdateTime: current,
		})
	if result.Error != nil {
		log.ErrorContextf(ctx, "UpdateSharedKnowledge failed, corpBizID: %d, knowledgeBizID: %d, error: %+v",
			corpBizID, knowledgeBizID, result.Error)
		return 0, result.Error
	}

	return result.RowsAffected, nil
}

func (d *dao) RetrieveBaseSharedKnowledge(ctx context.Context, corpBizID uint64,
	knowledgeBizIDList []uint64) ([]*model.SharedKnowledgeInfo, error) {
	log.InfoContextf(ctx, "RetrieveBaseSharedKnowledge, corpBizID: %d, knowledgeBizIDList: %+v",
		corpBizID, knowledgeBizIDList)

	if len(knowledgeBizIDList) == 0 {
		return []*model.SharedKnowledgeInfo{}, nil
	}

	dbClient := knowClient.DBClient(ctx, ShareKnowledgeTableName, knowClient.NotVIP,
		client.WithCalleeMethod("RetrieveBaseSharedKnowledge"))

	querySQL := fmt.Sprintf(getSharedKnowledge, sharedKnowledgeFields, placeholder(len(knowledgeBizIDList)))

	var result []*model.SharedKnowledgeInfo
	args := []any{corpBizID}
	for _, businessID := range knowledgeBizIDList {
		args = append(args, businessID)
	}
	if err := dbClient.QueryToStructs(ctx, &result, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "RetrieveBaseSharedKnowledge failed, sql: %s, args: %+v, error: %+v",
			querySQL, args, err)
		return nil, err
	}

	if len(result) == 0 {
		//log.WarnContextf(ctx, "RetrieveBaseSharedKnowledge, sql: %s, args: %+v, result.size: %d",
		//	querySQL, args, len(result))
		return nil, sql.ErrNoRows
	}

	return result, nil
}

// ListBaseSharedKnowledge 列举共享知识库清单
func (d *dao) ListBaseSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64,
	pageNumber, pageSize uint32, keyword string, spaceID string) (
	[]*model.SharedKnowledgeInfo, error) {
	log.InfoContextf(ctx, "ListBaseSharedKnowledge, corpBizID: %d, pageNumber: %d, pageSize: %d, keyword: %s",
		corpBizID, pageNumber, pageSize, keyword)

	if pageNumber == 0 || pageSize == 0 {
		return nil, fmt.Errorf("pageNumber/pageSize invalid")
	}

	db := d.tdsqlGorm.WithContext(ctx)
	offset := pageSize * (pageNumber - 1)
	knowledgeList := make([]*model.SharedKnowledgeInfo, 0)

	db = db.Model(model.SharedKnowledgeInfo{}).
		Where("corp_biz_id = ?", corpBizID).
		Where("is_deleted = ?", IsNotDeleted)

	if knowledgeBizIDList != nil && len(knowledgeBizIDList) > 0 {
		db = db.Where("business_id IN (?)", knowledgeBizIDList)
	}

	if len(keyword) > 0 {
		filter := fmt.Sprintf("%%%s%%", strings.ReplaceAll(keyword, "%", "\\%"))
		db = db.Where("name LIKE ? OR user_name LIKE ?", filter, filter)
	}

	if spaceID != "" {
		db = db.Where("space_id = ?", spaceID)
	}

	db = db.Order(fmt.Sprintf("%s DESC", ShareKnowledgeFieldUpdateTime))

	result := db.Offset(int(offset)).Limit(int(pageSize)).Find(&knowledgeList)
	if result.Error != nil {
		log.ErrorContextf(ctx, "RetrieveSharedKnowledgeCount failed, corpBizID: %d, "+
			"pageNumber: %d, pageSize: %d, keyword: %s, error: %+v",
			corpBizID, pageNumber, pageSize, keyword, result.Error)
		return nil, result.Error
	}
	return knowledgeList, nil
}

// RetrieveSharedKnowledgeCount 统计共享知识库数量
func (d *dao) RetrieveSharedKnowledgeCount(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64,
	keyword string, spaceID string) (int64, error) {
	log.InfoContextf(ctx, "RetrieveSharedKnowledgeCount, corpBizID: %d, keyword: %s",
		corpBizID, keyword)

	db := d.tdsqlGorm.WithContext(ctx)
	var count int64

	db = db.Model(model.SharedKnowledgeInfo{}).
		Where("corp_biz_id = ?", corpBizID).
		Where("is_deleted = ?", IsNotDeleted)

	if knowledgeBizIDList != nil && len(knowledgeBizIDList) > 0 {
		db = db.Where("business_id IN (?)", knowledgeBizIDList)
	}

	if len(keyword) > 0 {
		filter := fmt.Sprintf("%%%s%%", strings.ReplaceAll(keyword, "%", "\\%"))
		db = db.Where("name LIKE ? OR user_name LIKE ?", filter, filter)
	}

	if spaceID != "" {
		db = db.Where("space_id = ?", spaceID)
	}

	result := db.Count(&count)
	if result.Error != nil {
		log.ErrorContextf(ctx, "RetrieveSharedKnowledgeCount failed, corpBizID: %d, "+
			"keyword: %s, error: %+v", corpBizID, keyword, result.Error)
		return 0, result.Error
	}
	return count, nil
}

// DeleteSharedKnowledge 删除共享知识库
func (d *dao) DeleteSharedKnowledge(ctx context.Context, corpBizID uint64,
	knowledgeBizIDList []uint64) (int64, error) {
	log.InfoContextf(ctx, "DeleteSharedKnowledge, corpBizID: %d, knowledgeBizIDList: %+v",
		corpBizID, knowledgeBizIDList)

	if len(knowledgeBizIDList) == 0 {
		log.ErrorContextf(ctx, "DeleteSharedKnowledge failed, corpBizID: %d, "+
			"knowledgeBizIDList empty", corpBizID)
		return 0, fmt.Errorf("knowledgeBizIDList empty")
	}

	db := d.tdsqlGorm.WithContext(ctx)

	result := db.Model(model.SharedKnowledgeInfo{}).
		Where("corp_biz_id = ?", corpBizID).
		Where("business_id IN (?)", knowledgeBizIDList).
		Where("is_deleted = ?", IsNotDeleted).
		Updates(&model.SharedKnowledgeInfo{
			IsDeleted: IsDeleted,
		})

	if result.Error != nil {
		log.ErrorContextf(ctx, "DeleteSharedKnowledge failed, corpBizID: %d, "+
			"knowledgeBizIDList: %+v, error: %+v", corpBizID, knowledgeBizIDList, result.Error)
		return 0, result.Error
	}

	return result.RowsAffected, nil
}

// RetrieveSharedKnowledgeByName 按名称检索知识库
func (d *dao) RetrieveSharedKnowledgeByName(ctx context.Context, corpBizID uint64, knowledgeNameList []string, spaceID string) (
	[]*model.SharedKnowledgeInfo, error) {
	log.InfoContextf(ctx, "RetrieveSharedKnowledgeByName, corpBizID: %d, knowledgeNameList: %+v",
		corpBizID, knowledgeNameList)

	if len(knowledgeNameList) == 0 {
		return nil, fmt.Errorf("knowledgeNameList empty")
	}

	db := d.tdsqlGorm.WithContext(ctx)
	knowledgeList := make([]*model.SharedKnowledgeInfo, 0)

	db = db.Model(model.SharedKnowledgeInfo{}).
		Where("corp_biz_id = ?", corpBizID).
		Where("is_deleted = ?", IsNotDeleted).
		Where(fmt.Sprintf("%s IN (?)", ShareKnowledgeFieldName), knowledgeNameList)
	if spaceID != "" {
		db = db.Where("space_id = ?", spaceID)
	}

	result := db.Find(&knowledgeList)
	if result.Error != nil {
		log.ErrorContextf(ctx, "RetrieveSharedKnowledgeByName failed, corpBizID: %d, "+
			"knowledgeNameList: %+v, error: %+v",
			corpBizID, knowledgeNameList, result.Error)
		return nil, result.Error
	}

	return knowledgeList, nil
}

func (d *dao) ClearSpaceSharedKnowledge(ctx context.Context, corpBizID uint64, spaceID string) (int64, error) {
	log.InfoContextf(ctx, "ClearSpaceSharedKnowledge, corpBizID:%d, spaceID: %s", corpBizID, spaceID)

	if corpBizID == 0 || spaceID == "" {
		log.ErrorContextf(ctx, "ClearSpaceSharedKnowledge fail, corpBizID or spaceID is empty")
		return 0, fmt.Errorf("corpBizID or spaceID empty")
	}

	db := d.tdsqlGorm.WithContext(ctx)

	result := db.Model(model.SharedKnowledgeInfo{}).
		Where("corp_biz_id = ?", corpBizID).
		Where("space_id = ?", spaceID).
		Where("is_deleted = ?", IsNotDeleted).
		Updates(&model.SharedKnowledgeInfo{
			IsDeleted: IsDeleted,
		})

	if result.Error != nil {
		log.ErrorContextf(ctx, "ClearSpaceSharedKnowledge fail, spaceID: %s, err: %+v", spaceID, result.Error)
		return 0, result.Error
	}

	return result.RowsAffected, nil
}

func (d *dao) ListSpaceShareKnowledgeExSelf(ctx context.Context,
	corpBizID, exStaffID uint64,
	spaceID, keyword string,
	pageNumber, pageSize uint32) (int64, []*model.SharedKnowledgeInfo, error) {

	log.InfoContextf(ctx, "ListSpaceShareKnowledgeExSelf, "+
		"corpBizID:%d, spaceID:%s, exStaffID:%d, pageNumber:%d, pageSize:%d, keyword:%s",
		corpBizID, spaceID, exStaffID, pageNumber, pageSize, keyword)

	if corpBizID == 0 {
		return 0, nil, fmt.Errorf("corpBizID empty")
	}
	if spaceID == "" {
		return 0, nil, fmt.Errorf("spaceID empty")
	}
	if pageNumber == 0 || pageSize == 0 {
		return 0, nil, fmt.Errorf("pageNumber/pageSize invalid")
	}

	db := d.tdsqlGorm.WithContext(ctx)
	offset := pageSize * (pageNumber - 1)
	knowledgeList := make([]*model.SharedKnowledgeInfo, 0)

	db = db.Model(model.SharedKnowledgeInfo{}).
		Where("corp_biz_id = ?", corpBizID).
		Where("space_id = ?", spaceID).
		Where("is_deleted = ?", IsNotDeleted)
	if exStaffID > 0 {
		db.Not("owner_staff_id = ?", exStaffID)
	}
	if len(keyword) > 0 {
		filter := fmt.Sprintf("%%%s%%", strings.ReplaceAll(keyword, "%", "\\%"))
		db = db.Where("name LIKE ? OR user_name LIKE ?", filter, filter)
	}

	var total int64
	res := db.Count(&total)
	if res.Error != nil {
		log.ErrorContextf(ctx, "ListSpaceShareKnowledgeExSelf Count faile, err: %+v", res.Error)
		return 0, nil, res.Error
	}

	db = db.Order(fmt.Sprintf("%s DESC", ShareKnowledgeFieldUpdateTime))

	result := db.Offset(int(offset)).Limit(int(pageSize)).Find(&knowledgeList)
	if result.Error != nil {
		log.ErrorContextf(ctx, "ListSpaceShareKnowledgeExSelf fail, "+
			"corpBizID:%d, spaceID:%s, exStaffID:%d, pageNumber:%d, pageSize:%d, keyword:%s, err:%+v",
			corpBizID, spaceID, exStaffID, pageNumber, pageSize, keyword, result.Error)
		return 0, nil, result.Error
	}
	return total, knowledgeList, nil
}

// GetShareKnowledgeBaseByIDRange 获取共享知识库ID范围内的知识库列表
func (d *dao) GetShareKnowledgeBaseByIDRange(ctx context.Context, startID, endID uint64, limit int) ([]*model.SharedKnowledgeInfo, error) {
	log.InfoContextf(ctx, "GetShareKnowledgeBaseByIDRange, startID:%d, endID:%d, limit:%d", startID, endID, limit)
	db := d.tdsqlGorm.WithContext(ctx)
	knowledgeList := make([]*model.SharedKnowledgeInfo, 0)
	db = db.Model(model.SharedKnowledgeInfo{}).
		Where("id BETWEEN ? AND ?", startID, endID).
		Order("id ASC").
		Limit(limit)
	result := db.Find(&knowledgeList)
	if result.Error != nil {
		log.ErrorContextf(ctx, "GetShareKnowledgeBaseByIDRange fail, startID:%d, endID:%d, limit:%d, err:%+v",
			startID, endID, limit, result.Error)
		return nil, result.Error
	}
	return knowledgeList, nil
}

// UpdateShareKnowledgeBaseOwnerStaffID 更新共享知识库的所有者
func (d *dao) UpdateShareKnowledgeBaseOwnerStaffID(ctx context.Context, kb *model.SharedKnowledgeInfo) error {
	log.InfoContextf(ctx, "UpdateShareKnowledgeBaseOwnerStaffID, kb:%+v", kb)
	db := d.tdsqlGorm.WithContext(ctx)
	result := db.Model(kb).Where("id = ?", kb.ID).Update("owner_staff_id", kb.OwnerStaffID)
	if result.Error != nil {
		log.ErrorContextf(ctx, "UpdateShareKnowledgeBaseOwnerStaffID fail, kb:%+v, err:%+v", kb, result.Error)
		return result.Error
	}
	return nil
}
