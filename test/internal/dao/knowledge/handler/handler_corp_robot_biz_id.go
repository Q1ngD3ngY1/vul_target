// bot-knowledge-config-server
//
// @(#)handler_corp_robot_biz_id.go  星期四, 四月 24, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package handler

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
)

// CorpRobotBizIDHandler 通用bizID删除
type CorpRobotBizIDHandler struct {
	dao dao.Dao
}

// NewCorpRobotBizIDHandler 初始化通用bizID处理
func NewCorpRobotBizIDHandler() *CorpRobotBizIDHandler {
	return &CorpRobotBizIDHandler{
		dao: dao.New(),
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (c *CorpRobotBizIDHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "CorpRobotBizIDHandler CountNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	app, err := c.dao.GetAppByID(ctx, robotID)
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
	log.InfoContextf(ctx, "CorpRobotBizIDHandler CountNeedDeletedData corpBizID:%d, robotBizID:%d, tableName:%s",
		corp.BusinessID, app.BusinessID, tableName)
	return c.dao.CountTableNeedDeletedDataBizID(ctx, corp.BusinessID, app.BusinessID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (c *CorpRobotBizIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "CorpRobotIDHandler DeleteNeedDeletedData corpID:%d, robotID:%d, tableName:%s, "+
		"totalCount:%d", corpID, robotID, tableName, totalCount)
	app, err := c.dao.GetAppByID(ctx, robotID)
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
	log.InfoContextf(ctx, "CorpRobotBizIDHandler DeleteNeedDeletedData corpBizID:%d, robotBizID:%d, tableName:%s",
		corp.BusinessID, app.BusinessID, tableName)

	return c.dao.DeleteTableNeedDeletedDataBizID(ctx, corp.BusinessID, app.BusinessID, tableName, totalCount)
}
