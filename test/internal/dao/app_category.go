package dao

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"golang.org/x/exp/slices"
)

// GetUserAppCategory 获取用户应用分类
func (d *dao) GetUserAppCategory(ctx context.Context, sid int, uin, subAccountUin string) (
	[]string, []model.AppCategory, error) {
	si, err := d.GetSystemIntegratorByID(ctx, sid)
	if err != nil {
		return nil, nil, err
	}
	if si.IsSelfManagePermission() { // 集成商全部放开
		appTypeList := make([]string, 0, 10)
		appCategory := make([]model.AppCategory, 0, 10)
		for _, cate := range config.App().PermissionAppCategory {
			appTypeList = append(appTypeList, cate.AppType)
			appCategory = append(appCategory, model.AppCategory{
				AppType: cate.AppType,
				Name:    cate.Name,
				Logo:    cate.Logo,
			})
		}
		return appTypeList, appCategory, nil
	}
	if uin == "" && subAccountUin == "" { // 老站点用户只有知识库问答
		return []string{model.KnowledgeQaAppType}, nil, nil
	}
	hasAllPermission := false
	if slices.Contains(config.App().WhiteListUin, subAccountUin) {
		log.DebugContextf(ctx, "subAccountUin:%s is in white list:%+v", subAccountUin, config.App().WhiteListUin)
		hasAllPermission = true
	}
	allowPermission := make([]string, 0, 10)
	if !hasAllPermission {
		permissions, err := d.GetUserPermission(ctx, uin, subAccountUin, []string{})
		if err != nil {
			return nil, nil, err
		}
		for _, perm := range permissions {
			if perm.ParentID != "" {
				continue
			}
			allowPermission = append(allowPermission, perm.PermissionID)
		}
	}
	appTypeList := make([]string, 0, 10)
	appCategory := make([]model.AppCategory, 0, 10)
	for _, cate := range config.App().PermissionAppCategory {
		if !hasAllPermission && !slices.Contains(allowPermission, cate.PermissionID) {
			continue
		}
		appTypeList = append(appTypeList, cate.AppType)
		appCategory = append(appCategory, model.AppCategory{
			AppType: cate.AppType,
			Name:    cate.Name,
			Logo:    cate.Logo,
		})
	}
	return appTypeList, appCategory, nil
}
