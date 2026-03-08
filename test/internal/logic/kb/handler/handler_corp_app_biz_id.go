package handler

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

// CorpAppBizIDHandler 通用bizID删除
type CorpAppBizIDHandler struct {
	kbDao kbdao.Dao
	rpc   *rpc.RPC
}

// NewCorpAppBizIDHandler 初始化通用bizID处理
func NewCorpAppBizIDHandler(kbDao kbdao.Dao, rpc *rpc.RPC) *CorpAppBizIDHandler {
	return &CorpAppBizIDHandler{
		rpc:   rpc,
		kbDao: kbDao,
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (c *CorpAppBizIDHandler) CountNeedDeletedData(ctx context.Context, corpID, AppBizID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "CorpAppBizIDHandler CountNeedDeletedData corpID:%d, AppID(business):%d, tableName:%s",
		corpID, AppBizID, tableName)
	//app, err := c.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, AppID)
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
	logx.I(ctx, "CorpAppBizIDHandler CountNeedDeletedData corpBizID:%d, AppBizID:%d, tableName:%s", corp.GetCorpId(), AppBizID, tableName)
	return c.kbDao.CountTableNeedDeletedDataByCorpAndAppBizID(ctx, corp.GetCorpId(), AppBizID, tableName)
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (c *CorpAppBizIDHandler) DeleteNeedDeletedData(ctx context.Context, corpID, AppBizID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "CorpAppIDHandler DeleteNeedDeletedData corpID:%d, AppID(business):%d, tableName:%s, "+
		"totalCount:%d", corpID, AppBizID, tableName, totalCount)
	//app, err := c.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, AppID)
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
	logx.I(ctx, "CorpAppBizIDHandler DeleteNeedDeletedData corpBizID:%d, AppBizID:%d, tableName:%s", corp.GetCorpId(), AppBizID, tableName)

	return c.kbDao.DeleteTableNeedDeletedDataByCorpAndAppBizID(ctx, corp.GetCorpId(), AppBizID, tableName, totalCount)
}
