package app_share_knowledge

import (
	"context"
	"errors"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
)

// GetShareKGAppList 获取共享库引用的应用列表
func GetShareKGAppList(ctx context.Context, shareKGBizIDs []uint64) (map[uint64][]*model.ShareKGAppListRsp, error) {
	log.InfoContextf(ctx, "GetShareKGAppList shareKGBizIDs: %v", shareKGBizIDs)
	shareKGAppListMap := make(map[uint64][]*model.ShareKGAppListRsp)
	if len(shareKGBizIDs) == 0 {
		return nil, errors.New("shareKGBizIDs is empty")
	}
	// 获取引用的id列表
	shareKGAppList, err := dao.GetAppShareKGDao().GetShareKGAppBizIDList(ctx, shareKGBizIDs)
	if err != nil {
		log.ErrorContextf(ctx, "GetShareKGAppList failed, err: %+v", err)
		return nil, err
	}
	if len(shareKGAppList) == 0 {
		return shareKGAppListMap, nil
	}
	appBizIDs := make([]uint64, 0)
	for _, appInfo := range shareKGAppList {
		appBizIDs = append(appBizIDs, appInfo.AppBizID)
	}
	// appBizIDS 去重
	appBizIDs = slicex.Unique(appBizIDs)
	appList, err := client.GetAppsByBizIDs(ctx, appBizIDs, model.RunEnvSandbox)
	if err != nil {
		log.ErrorContextf(ctx, "GetShareKGAppList.GetAppsByBizIDs failed, err: %+v", err)
		return nil, err
	}
	// appList转成map
	appMap := make(map[uint64]*admin.GetAppsByBizIDsRsp_AppInfo)
	for _, app := range appList.GetApps() {
		appMap[app.GetAppBizId()] = app
	}
	for _, val := range shareKGAppList {
		shareKGAppListRsp := &model.ShareKGAppListRsp{
			AppBizID: val.AppBizID,
			AppName:  appMap[val.AppBizID].GetName(),
		}
		info, ok := shareKGAppListMap[val.KnowledgeBizID]
		if !ok {
			shareKGAppListMap[val.KnowledgeBizID] = make([]*model.ShareKGAppListRsp, 0)
			shareKGAppListMap[val.KnowledgeBizID] = append(info, shareKGAppListRsp)
		} else {
			shareKGAppListMap[val.KnowledgeBizID] = append(info, shareKGAppListRsp)
		}
	}
	return shareKGAppListMap, nil
}

// DeleteAppShareKG 删除应用引用的共享知识库
func DeleteAppShareKG(ctx context.Context, app *admin.GetAppInfoRsp, shareKGBizIDs []uint64) error {
	// 解绑角色权限
	err := dao.GetRoleDao(nil).RemoveKnowledgeAssociation(ctx, app.GetCorpBizId(), app.GetAppBizId(), shareKGBizIDs)
	if err != nil {
		log.ErrorContext(ctx, "DeleteAppShareKG.RemoveKnowledgeAssociation failed, err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}
	// 删除引用关系
	err = dao.GetAppShareKGDao().DeleteAppShareKG(ctx, app.GetAppBizId(), shareKGBizIDs)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteAppShareKG.DeleteAppShareKG failed, err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}
	return nil
}
