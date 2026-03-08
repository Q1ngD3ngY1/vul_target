package app

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	appImpl "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
)

// GetAppNormalModelName 获取应用的主模型名称
func GetAppNormalModelName(ctx context.Context, appInfo *admin.GetAppInfoRsp) (string, error) {
	modelMap := appInfo.GetKnowledgeQa().GetModel()
	if modelMap != nil {
		if _, ok := modelMap[model.AppModelNormal]; ok {
			return modelMap[model.AppModelNormal].ModelName, nil
		}
	}
	log.ErrorContext(ctx, "GetAppNormalModelName", "modelMap is nil")
	return "", errs.ErrNotInvalidModel
}

func getLoginUinAndSubAccountUin(ctx context.Context) (string, string) {
	uin := pkg.LoginUin(ctx)
	subAccountUin := pkg.LoginSubAccountUin(ctx)
	if pkg.SID(ctx) == model.CloudSID {
		uin = pkg.Uin(ctx)
		subAccountUin = pkg.SubAccountUin(ctx)
	}
	return uin, subAccountUin
}

// GetAppByAppBizID 通过业务ID获取应用
func GetAppByAppBizID(ctx context.Context, db dao.Dao, appBizID uint64) (*model.App, error) {
	appDB, err := db.GetAppByAppBizID(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "get robot appBizID:%d err:%+v", appBizID, errs.ErrAppNotFound)
		return nil, errs.ErrAppNotFound
	}
	if appDB == nil {
		return nil, errs.ErrAppNotFound
	}
	if appDB.HasDeleted() {
		return nil, errs.ErrAppNotFound
	}
	instance := appImpl.GetApp(appDB.AppType)
	if instance == nil {
		return nil, errs.ErrAppTypeInvalid
	}
	app, err := instance.AnalysisDescribeApp(ctx, appDB)
	if err != nil {
		return nil, errs.ErrSystem
	}
	corpID := pkg.CorpID(ctx)
	if corpID != 0 && corpID != app.CorpID {
		log.WarnContextf(ctx, "当前企业与应用归属企业不一致 businessID:%d corpID:%d robot:%+v",
			appBizID, corpID, app)
		// 给C端分享出去的链接使用上传图片或者文档，此时当前登录的Corp和App可能不是归属关系
		// 这里将app信息返回，可以针对ErrCorpAppNotEqual处理
		return app, errs.ErrCorpAppNotEqual
	}
	if err = db.CreateAppVectorIndex(ctx, appDB); err != nil {
		return nil, errs.ErrRobotInitFail
	}
	return app, nil
}

// GetPrompt 获取Prompt
func GetPrompt(ctx context.Context, dao dao.Dao, app *admin.GetAppInfoRsp, modelType string) (string, error) {
	if app == nil {
		return "", errs.ErrAppNotFound
	}
	modelInfo, ok := app.GetKnowledgeQa().GetModel()[modelType]
	if !ok {
		log.ErrorContextf(ctx, "GetPrompt, modelType:%s not found", modelType)
		return "", errs.ErrSystem
	}
	language := pkg.LanguageDef(ctx)
	prompt, err := dao.GetPromptCli().Get(ctx, modelType, modelInfo.ModelName, language, modelInfo.PromptVersion)
	if err != nil {
		log.ErrorContextf(ctx, "GetPrompt, modelType:%s, modelName:%s, language:%s, version:%s, err:%v",
			modelType, modelInfo.ModelName, language, modelInfo.PromptVersion, err)
		return "", errs.ErrSystem
	}
	return prompt, nil
}
