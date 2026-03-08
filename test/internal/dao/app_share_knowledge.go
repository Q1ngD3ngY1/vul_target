package dao

import (
	"context"
	"errors"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalAppShareKGDao *AppShareKnowledgeDao

const (
	appShareKnowledgeTableName     = "t_app_share_knowledge"
	appShareKnowledgeProdTableName = "t_app_share_knowledge_prod"

	AppShareKGTblColAppBizID       = "app_biz_id"
	AppShareKGTblColCorpBizID      = "corp_biz_id"
	AppShareKGTblColKnowledgeBizID = "knowledge_biz_id"
	AppShareKGTblColUpdateTime     = "update_time"
)

// AppShareKnowledgeDao 应用引用共享库句柄
type AppShareKnowledgeDao struct {
	BaseDao
}

// GetAppShareKGDao 获取全局的数据操作对象
func GetAppShareKGDao() *AppShareKnowledgeDao {
	if globalAppShareKGDao == nil {
		globalAppShareKGDao = &AppShareKnowledgeDao{*globalBaseDao}
	}
	return globalAppShareKGDao
}

// GetAppShareKGList 获取应用引用共享库的列表
func (d *AppShareKnowledgeDao) GetAppShareKGList(ctx context.Context,
	appBizID uint64) ([]*model.AppShareKnowledge, error) {
	list := make([]*model.AppShareKnowledge, 0)
	if appBizID == 0 {
		return list, errors.New("appBizID is zero")
	}
	selectColumns := []string{AppShareKGTblColAppBizID, AppShareKGTblColKnowledgeBizID, AppShareKGTblColUpdateTime}
	res := d.tdsqlGormDB.WithContext(ctx).Table(appShareKnowledgeTableName).Select(selectColumns).
		Where(AppShareKGTblColAppBizID+sqlEqual, appBizID).
		Find(&list)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return list, nil
}

// GetAppShareKGListProd 获取应用引用共享库的列表
func (d *AppShareKnowledgeDao) GetAppShareKGListProd(ctx context.Context,
	appBizID uint64) ([]*model.AppShareKnowledge, error) {
	list := make([]*model.AppShareKnowledge, 0)
	if appBizID == 0 {
		return list, errors.New("appBizID is zero")
	}
	selectColumns := []string{AppShareKGTblColAppBizID, AppShareKGTblColKnowledgeBizID}
	res := d.tdsqlGormDB.WithContext(ctx).Table(appShareKnowledgeProdTableName).Select(selectColumns).
		Where(AppShareKGTblColAppBizID+sqlEqual, appBizID).
		Find(&list)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return list, nil
}

// GetShareKGAppBizIDList 获取共享库的引用应用列表
func (d *AppShareKnowledgeDao) GetShareKGAppBizIDList(ctx context.Context,
	shareKGBizIDs []uint64) ([]*model.AppShareKnowledge, error) {
	list := make([]*model.AppShareKnowledge, 0)
	if len(shareKGBizIDs) == 0 {
		return list, errors.New("shareKGBizIDs is empty")
	}
	selectColumns := []string{AppShareKGTblColAppBizID, AppShareKGTblColKnowledgeBizID}
	res := d.tdsqlGormDB.WithContext(ctx).Table(appShareKnowledgeTableName).Select(selectColumns).
		Where(AppShareKGTblColKnowledgeBizID+sqlIn, shareKGBizIDs).
		Find(&list)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return list, res.Error
	}
	return list, nil
}

// CreateAppShareKG 创建应用引用共享库
func (d *AppShareKnowledgeDao) CreateAppShareKG(ctx context.Context, appShareKGs []model.AppShareKnowledge) error {
	if len(appShareKGs) == 0 {
		log.WarnContextf(ctx, "CreateAppShareKG appShareKGs is empty")
		return nil
	}
	res := d.tdsqlGormDB.WithContext(ctx).Table(appShareKnowledgeTableName).Create(appShareKGs)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// DeleteAppShareKG 删除应用引用共享库（硬删除）
func (d *AppShareKnowledgeDao) DeleteAppShareKG(ctx context.Context, appBizID uint64, knowledgeBizIDs []uint64) error {
	if len(knowledgeBizIDs) == 0 {
		log.WarnContextf(ctx, "DeleteAppShareKG knowledgeBizIDs is empty, appBizID: %d", appBizID)
		return nil
	}
	res := d.tdsqlGormDB.WithContext(ctx).Table(appShareKnowledgeTableName).
		Where(AppShareKGTblColAppBizID+sqlEqual, appBizID).
		Where(AppShareKGTblColKnowledgeBizID+sqlIn, knowledgeBizIDs).
		Delete(&model.AppShareKnowledge{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "DeleteAppShareKG failed for knowledgeBizIDs: %+v, err: %+v",
			knowledgeBizIDs, res.Error)
		return res.Error
	}
	return nil
}

// ExistShareKG 应用是否引用了共享库
func (d *AppShareKnowledgeDao) ExistShareKG(ctx context.Context,
	appBizID uint64) (*model.AppShareKnowledge, error) {
	list := make([]*model.AppShareKnowledge, 0)
	if appBizID == 0 {
		return nil, errors.New("ExistShareKG appBizID is zero")
	}
	selectColumns := []string{AppShareKGTblColAppBizID, AppShareKGTblColCorpBizID}
	res := d.tdsqlGormDB.WithContext(ctx).Table(appShareKnowledgeTableName).Select(selectColumns).
		Where(AppShareKGTblColAppBizID+sqlEqual, appBizID).Limit(1).
		Find(&list)
	if res.Error != nil {
		log.ErrorContextf(ctx, "ExistShareKG execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	if len(list) > 0 {
		return list[0], nil
	}
	return nil, nil
}
