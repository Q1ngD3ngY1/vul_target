package handler

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

// CorpRobotBizIDHandler 通用bizID删除
type CorpRobotBizIDHandler struct {
	kbDao kbdao.Dao
	rpc   *rpc.RPC
}

// NewCorpRobotBizIDHandler 初始化通用bizID处理
func NewCorpRobotBizIDHandler(kbDao kbdao.Dao, rpc *rpc.RPC) *CorpRobotBizIDHandler {
	return &CorpRobotBizIDHandler{
		kbDao: kbDao,
		rpc:   rpc,
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (c *CorpRobotBizIDHandler) CountNeedDeletedData(ctx context.Context, corpID, appBizID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "CorpRobotBizIDHandler CountNeedDeletedData corpID:%d, appID(business):%d, tableName:%s",
		corpID, appBizID, tableName)
	//app, err := c.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, robotID)
	//if err != nil {
	//	logx.E(ctx, "CountNeedDeletedData DescribeAppByPrimaryIdWithoutNotFoundError err: %+v", err)
	//	return 0, err
	//}
	//if app == nil {
	//	// 可能已经被清理了
	//	logx.W(ctx, "CountNeedDeletedData DescribeAppByPrimaryIdWithoutNotFoundError app is nil")
	//	return 0, nil
	//}
	corp, err := c.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	// corp, err := c.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		logx.E(ctx, "CountNeedDeletedData GetCorpByID err: %+v", err)
		return 0, err
	}
	if corp == nil {
		logx.E(ctx, "CountNeedDeletedData GetCorpByID corp is nil")
		return 0, nil
	}
	logx.I(ctx, "CorpRobotBizIDHandler CountNeedDeletedData corpBizID:%d, robotBizID:%d, tableName:%s", corp.GetCorpId(), appBizID, tableName)
	return c.kbDao.CountTableNeedDeletedDataBizID(ctx, corp.GetCorpId(), appBizID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (c *CorpRobotBizIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, appBizID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "CorpRobotIDHandler DeleteNeedDeletedData corpID:%d, appID(business):%d, tableName:%s, "+
		"totalCount:%d", corpID, appBizID, tableName, totalCount)
	//app, err := c.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, robotID)
	//if err != nil {
	//	logx.E(ctx, "DeleteNeedDeletedData DescribeAppByPrimaryIdWithoutNotFoundError err: %+v", err)
	//	return err
	//}
	//if app == nil {
	//	// 可能已经被清理了
	//	logx.W(ctx, "DeleteNeedDeletedData DescribeAppByPrimaryIdWithoutNotFoundError app is nil")
	//	return nil
	//}
	corp, err := c.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	// corp, err := c.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		logx.E(ctx, "DeleteNeedDeletedData GetCorpByID err: %+v", err)
		return err
	}
	if corp == nil {
		logx.E(ctx, "DeleteNeedDeletedData GetCorpByID corp is nil")
		return nil
	}
	logx.I(ctx, "CorpRobotBizIDHandler DeleteNeedDeletedData corpBizID:%d, robotBizID:%d, tableName:%s", corp.GetCorpId(), appBizID, tableName)

	return c.kbDao.DeleteTableNeedDeletedDataBizID(ctx, corp.GetCorpId(), appBizID, tableName, totalCount)
}
