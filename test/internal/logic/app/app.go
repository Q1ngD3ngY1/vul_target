package app

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// GetAppNormalModelName 获取应用的主模型名称
func GetAppNormalModelName(ctx context.Context, appInfo *entity.App) (string, error) {
	modelMap := appInfo.QaConfig.Model
	if modelMap != nil {
		if _, ok := modelMap[entity.AppModelNormal]; ok {
			return modelMap[entity.AppModelNormal].ModelName, nil
		}
	}
	logx.E(ctx, "GetAppNormalModelName", "modelMap is nil")
	return "", errs.ErrNotInvalidModel
}

func getLoginUinAndSubAccountUin(ctx context.Context) (string, string) {
	md := contextx.Metadata(ctx)
	if md.SID() == entity.CloudSID {
		return md.Uin(), md.SubAccountUin()
	}
	return md.LoginUin(), md.LoginSubAccountUin()
}

// GetAppByAppBizID 通过业务ID获取应用
func GetAppByAppBizID(ctx context.Context, r *rpc.RPC, appBizID uint64) (*entity.App, error) {
	appInfo, err := r.DescribeAppById(ctx, appBizID)
	if err != nil {
		logx.E(ctx, "get robot appBizID:%d err:%+v", appBizID, errs.ErrAppNotFound)
		return nil, errs.ErrAppNotFound
	}
	if appInfo == nil {
		logx.E(ctx, "get robot appBizID:%d err:%+v", appBizID, errs.ErrAppNotFound)
		return nil, errs.ErrAppNotFound
	}
	if appInfo.IsDeleted {
		logx.E(ctx, "get robot appBizID:%d err:%+v", appBizID, errs.ErrAppNotFound)
		return nil, errs.ErrAppNotFound
	}
	if appInfo.AppType != entity.KnowledgeQaAppType {
		return nil, errs.ErrGetAppFail
	}
	corpID := contextx.Metadata(ctx).CorpID()
	if corpID != 0 && corpID != appInfo.CorpPrimaryId {
		logx.W(ctx, "当前企业与应用归属企业不一致 businessID:%d corpID:%d robot:%+v",
			appBizID, corpID, appInfo)
		// 给C端分享出去的链接使用上传图片或者文档，此时当前登录的Corp和App可能不是归属关系
		// 这里将app信息返回，可以针对ErrCorpAppNotEqual处理
		return appInfo, errs.ErrCorpAppNotEqual
	}
	return appInfo, nil
}
