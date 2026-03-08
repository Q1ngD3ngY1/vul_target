package handler

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
)

// RobotIDHandler 应用ID删除
type RobotIDHandler struct {
	dao dao.Dao
}

// NewRobotIDHandler 初始化应用ID删除处理
func NewRobotIDHandler() *RobotIDHandler {
	return &RobotIDHandler{
		dao: dao.New(),
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (r *RobotIDHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "RobotIDHandler CountNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	// corpID置为0
	return r.dao.CountTableNeedDeletedData(ctx, 0, robotID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (r *RobotIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "RobotIDHandler DeleteNeedDeletedData corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpID, robotID, tableName, totalCount)
	// corpID置为0
	return r.dao.DeleteTableNeedDeletedData(ctx, 0, robotID, tableName, totalCount)
}
