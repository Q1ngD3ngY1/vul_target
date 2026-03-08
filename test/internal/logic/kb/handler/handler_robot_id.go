package handler

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
)

// RobotIDHandler 应用ID删除
type RobotIDHandler struct {
	kbDao kbdao.Dao
}

// NewRobotIDHandler 初始化应用ID删除处理
func NewRobotIDHandler(kbDao kbdao.Dao) *RobotIDHandler {
	return &RobotIDHandler{
		kbDao: kbDao,
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (r *RobotIDHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "RobotIDHandler CountNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	// corpID置为0
	return r.kbDao.CountTableNeedDeletedData(ctx, 0, robotID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (r *RobotIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "RobotIDHandler DeleteNeedDeletedData corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpID, robotID, tableName, totalCount)
	// corpID置为0
	return r.kbDao.DeleteTableNeedDeletedData(ctx, 0, robotID, tableName, totalCount)
}
