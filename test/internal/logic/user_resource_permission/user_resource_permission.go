package user_resource_permission

import (
	"context"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
	aiconfManager "git.woa.com/dialogue-platform/proto/pb-stub/aiconf-manager-server"
)

// AddUserResourcePermission 添加用户数据权限
func AddUserResourcePermission(ctx context.Context, d dao.Dao, uin, subAccountUin, spaceID string,
	resourceType bot_common.ResourceType, resourceID string) error {
	spaceID = utils.When(len(spaceID) == 0, model.DefaultSpaceID, spaceID)
	si, err := d.GetSystemIntegratorByID(ctx, pkg.SID(ctx))
	if err != nil {
		return errs.ErrSystem
	}
	// 集成商权限不处理权限
	if si.IsSelfManagePermission() {
		return nil
	}
	req := &aiconfManager.AddUserResourcePermissionReq{
		RequestId:     common.GetRequestID(ctx),
		Uin:           uin,
		SubAccountUin: subAccountUin,
		OperUin:       subAccountUin,
		SpaceId:       spaceID,
		ResourceType:  model.MapResourceType[resourceType],
		ResourceId:    resourceID,
	}
	return client.AddUserResourcePermission(ctx, req)
}
