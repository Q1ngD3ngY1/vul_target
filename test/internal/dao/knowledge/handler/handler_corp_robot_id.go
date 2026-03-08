package handler

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
)

// CorpRobotIDHandler 通用删除
type CorpRobotIDHandler struct {
	dao dao.Dao
}

// NewCorpRobotIDHandler 初始化通用处理
func NewCorpRobotIDHandler() *CorpRobotIDHandler {
	return &CorpRobotIDHandler{
		dao: dao.New(),
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (c *CorpRobotIDHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "CorpRobotIDHandler CountNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	return c.dao.CountTableNeedDeletedData(ctx, corpID, robotID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (c *CorpRobotIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "CorpRobotIDHandler DeleteNeedDeletedData corpID:%d, robotID:%d, tableName:%s, "+
		"totalCount:%d", corpID, robotID, tableName, totalCount)
	return c.dao.DeleteTableNeedDeletedData(ctx, corpID, robotID, tableName, totalCount)
}
