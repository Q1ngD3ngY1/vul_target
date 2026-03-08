package handler

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
)

// CorpAppBizIDHandler 通用bizID删除
type CorpAppBizIDHandler struct {
	dao dao.Dao
}

// NewCorpAppBizIDHandler 初始化通用bizID处理
func NewCorpAppBizIDHandler() *CorpAppBizIDHandler {
	return &CorpAppBizIDHandler{
		dao: dao.New(),
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (c *CorpAppBizIDHandler) CountNeedDeletedData(ctx context.Context, corpID, AppID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "CorpAppBizIDHandler CountNeedDeletedData corpID:%d, RobotID:%d, tableName:%s",
		corpID, AppID, tableName)
	app, err := c.dao.GetAppByID(ctx, AppID)
	if err != nil {
		log.ErrorContextf(ctx, "CountNeedDeletedData GetAppByID err: %+v", err)
		return 0, err
	}
	if app == nil {
		// 可能已经被清理了
		log.WarnContextf(ctx, "CountNeedDeletedData GetAppByID app is nil")
		return 0, nil
	}
	corp, err := c.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "CountNeedDeletedData GetCorpByID err: %+v", err)
		return 0, err
	}
	if corp == nil {
		log.ErrorContextf(ctx, "CountNeedDeletedData GetCorpByID corp is nil")
		return 0, nil
	}
	log.InfoContextf(ctx, "CorpAppBizIDHandler CountNeedDeletedData corpBizID:%d, AppBizID:%d, tableName:%s",
		corp.BusinessID, app.BusinessID, tableName)
	return c.dao.CountTableNeedDeletedDataByCorpAndAppBizID(ctx, corp.BusinessID, app.BusinessID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (c *CorpAppBizIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, AppID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "CorpAppIDHandler DeleteNeedDeletedData corpID:%d, RobotID:%d, tableName:%s, "+
		"totalCount:%d", corpID, AppID, tableName, totalCount)
	app, err := c.dao.GetAppByID(ctx, AppID)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteNeedDeletedData GetAppByID err: %+v", err)
		return err
	}
	if app == nil {
		// 可能已经被清理了
		log.WarnContextf(ctx, "DeleteNeedDeletedData GetAppByID app is nil")
		return nil
	}
	corp, err := c.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteNeedDeletedData GetCorpByID err: %+v", err)
		return err
	}
	if corp == nil {
		log.ErrorContextf(ctx, "DeleteNeedDeletedData GetCorpByID corp is nil")
		return nil
	}
	log.InfoContextf(ctx, "CorpAppBizIDHandler DeleteNeedDeletedData corpBizID:%d, AppBizID:%d, tableName:%s",
		corp.BusinessID, app.BusinessID, tableName)

	return c.dao.DeleteTableNeedDeletedDataByCorpAndAppBizID(ctx, corp.BusinessID, app.BusinessID, tableName, totalCount)
}
