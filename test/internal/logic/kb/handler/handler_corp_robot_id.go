package handler

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
)

// CorpRobotIDHandler 通用删除
type CorpRobotIDHandler struct {
	kbDao kbdao.Dao
}

// NewCorpRobotIDHandler 初始化通用处理
func NewCorpRobotIDHandler(kbDao kbdao.Dao) *CorpRobotIDHandler {
	return &CorpRobotIDHandler{
		kbDao: kbDao,
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (c *CorpRobotIDHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "CorpRobotIDHandler CountNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	return c.kbDao.CountTableNeedDeletedData(ctx, corpID, robotID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (c *CorpRobotIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "CorpRobotIDHandler DeleteNeedDeletedData corpID:%d, robotID:%d, tableName:%s, "+
		"totalCount:%d", corpID, robotID, tableName, totalCount)
	return c.kbDao.DeleteTableNeedDeletedData(ctx, corpID, robotID, tableName, totalCount)
}
