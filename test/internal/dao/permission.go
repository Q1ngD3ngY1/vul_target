package dao

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	accessManager "git.woa.com/dialogue-platform/proto/pb-stub/access-manager-server"
	"git.woa.com/dialogue-platform/proto/pb-stub/permission"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slices"
)

func getRequestID(ctx context.Context) string {
	requestID := pkg.RequestID(ctx)
	if requestID != "" {
		return requestID
	}
	return trace.SpanContextFromContext(ctx).TraceID().String()
}

// GetUserPermission 获取用户权限
func (d *dao) GetUserPermission(ctx context.Context, uin, subAccountUin string, permissionIDs []string) (
	[]*model.PermissionInfo, error) {
	if uin == "" && subAccountUin == "" {
		return nil, nil
	}
	if slices.Contains(config.App().WhiteListUin, subAccountUin) {
		log.DebugContextf(ctx, "subAccountUin:%s is in white list:%+v", subAccountUin, config.App().WhiteListUin)
		return []*model.PermissionInfo{
			{
				PermissionID: model.AllPermissionID(), PermissionName: model.AllPermissionName(),
			},
		}, nil
	}
	permissionIDs = slicex.Unique(permissionIDs)
	condition := make([]*accessManager.ListPermissionCond, 0, len(permissionIDs))
	for _, id := range permissionIDs {
		if id == "" {
			continue
		}
		condition = append(condition, &accessManager.ListPermissionCond{
			PermissionId: id,
		})
	}
	req := &accessManager.ListPermissionsReq{
		RequestId:     getRequestID(ctx),
		Uin:           uin,
		SubAccountUin: subAccountUin,
		ProductType:   model.ProductType,
		Conditions:    condition,
	}
	rsp, err := d.accessCli.ListPermissions(ctx, req, WithTrpcSelector())
	log.DebugContextf(ctx, "获取用户权限 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "获取用户权限失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	if rsp.GetCode() != 0 {
		log.ErrorContextf(ctx, "获取用户权限失败 req:%+v rsp:%+v", req, rsp)
		return nil, fmt.Errorf(rsp.GetMessage())
	}
	list := make([]*model.PermissionInfo, 0, len(rsp.GetPermissions()))
	for _, perm := range rsp.GetPermissions() {
		log.DebugContextf(ctx, "permissionID:%s permissionName:%s permissionType:%d action:%+v",
			perm.GetPermissionId(), perm.GetPermissionName(), perm.GetPermissionType(), perm.GetActions())
		p := &model.PermissionInfo{
			PermissionID:   perm.GetPermissionId(),
			ParentID:       perm.GetParentId(),
			ProductType:    perm.GetProductType(),
			PermissionName: perm.GetPermissionName(),
			PermissionType: perm.GetPermissionType(),
			Actions:        perm.GetActions(),
			Resources:      make([]*model.PermissionResource, 0),
		}
		for _, i := range perm.GetResources() {
			resource := &model.PermissionResource{
				ResourceType:        i.GetResourceType(),
				ResourceIDs:         i.GetResourceIds(),
				EffectPermissionIDs: i.GetEffectPermissionIds(),
				ResourceProperties:  d.getResourceProperties(i.GetResourceProperties()),
			}
			p.Resources = append(p.Resources, resource)
		}
		list = append(list, p)
	}
	return list, nil
}

func (d *dao) getResourceProperties(
	resourceProperty map[string]*permission.PermissionResourceProperty,
) map[string]*model.PermissionResourceProperty {
	rsp := make(map[string]*model.PermissionResourceProperty, 0)
	for key, value := range resourceProperty {
		properties := make([]*model.Property, 0)
		for _, v := range value.Properties {
			properties = append(properties, &model.Property{
				Key:   v.GetKey(),
				Value: v.GetValue(),
			})
		}
		rsp[key] = &model.PermissionResourceProperty{
			Properties: properties,
		}
	}
	return rsp
}

// VerifyPermission 验证权限 TODO: 增加应用分类维度
func (d *dao) VerifyPermission(ctx context.Context, uin, subAccountUin, action string) (bool, error) {
	if uin == "" && subAccountUin == "" {
		return true, nil
	}
	if slices.Contains(config.App().WhiteListUin, subAccountUin) {
		log.DebugContextf(ctx, "subAccountUin:%s is in white list:%+v", subAccountUin, config.App().WhiteListUin)
		return true, nil
	}
	req := &accessManager.VerifyPermissionsReq{
		RequestId:     getRequestID(ctx),
		Uin:           uin,
		SubAccountUin: subAccountUin,
		ProductType:   model.ProductType,
		Conditions: []*accessManager.VerifyPermissionCond{
			{
				Action:    action,
				Resources: nil,
			},
		},
	}
	rsp, err := d.accessCli.VerifyPermission(ctx, req, WithTrpcSelector())
	log.DebugContextf(ctx, "验证权限 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "验证权限失败 req:%+v err:%+v", req, err)
		return false, err
	}
	if rsp.GetCode() != 0 {
		log.ErrorContextf(ctx, "验证权限失败 req:%+v rsp:%+v", req, rsp)
		return false, fmt.Errorf(rsp.GetMessage())
	}
	hasPermission := false
	for _, state := range rsp.GetStates() {
		if state.GetAction() == action && state.GetIsAllowed() == model.PermissionAllow {
			hasPermission = true
		}
	}
	return hasPermission, nil
}

// VerifyResource 验证资源
func (d *dao) VerifyResource(ctx context.Context, uin, subAccountUin string, botBizID uint64,
	isShared bool) (bool, error) {
	if uin == "" && subAccountUin == "" {
		return true, nil
	}
	if slices.Contains(config.App().WhiteListUin, subAccountUin) {
		log.DebugContextf(ctx, "subAccountUin:%s is in white list:%+v", subAccountUin, config.App().WhiteListUin)
		return true, nil
	}
	permissionID := model.ListAppPermissionID()
	if isShared {
		permissionID = model.ListShareKnowledgePermissionID()
	}
	resourceIDs, err := d.GetUserResource(ctx, uin, subAccountUin, permissionID)
	if err != nil {
		return false, err
	}
	verify := false
	for _, id := range resourceIDs {
		if model.HasAllResourcePermission(id) {
			log.DebugContextf(ctx, "uin:%s subAccountUin:%s has all resource permission", uin, subAccountUin)
			verify = true
		}
		if id == fmt.Sprintf("%d", botBizID) {
			log.DebugContextf(ctx, "uin:%s subAccountUin:%s has %d resource permission", uin, subAccountUin, botBizID)
			verify = true
		}
	}
	return verify, nil
}

// GetUserResource 获取用户资源
func (d *dao) GetUserResource(ctx context.Context, uin, subAccountUin, permissionID string) ([]string, error) {
	if uin == "" && subAccountUin == "" {
		return []string{model.AllResourcePermissionID()}, nil
	}
	if slices.Contains(config.App().WhiteListUin, subAccountUin) {
		log.DebugContextf(ctx, "subAccountUin:%s is in white list:%+v", subAccountUin, config.App().WhiteListUin)
		return []string{model.AllResourcePermissionID()}, nil
	}
	req := &accessManager.ListPermissionsReq{
		RequestId:     getRequestID(ctx),
		Uin:           uin,
		SubAccountUin: subAccountUin,
		ProductType:   model.ProductType,
		Conditions: []*accessManager.ListPermissionCond{
			{
				PermissionId: permissionID,
			},
		},
	}
	rsp, err := d.accessCli.ListPermissions(ctx, req, WithTrpcSelector())
	log.DebugContextf(ctx, "获取用户资源 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "获取用户资源失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	if rsp.GetCode() != 0 {
		log.ErrorContextf(ctx, "获取用户资源失败 req:%+v rsp:%+v", req, rsp)
		return nil, fmt.Errorf(rsp.GetMessage())
	}
	resourceIDs := make([]string, 0)
	for _, perm := range rsp.GetPermissions() {
		for _, i := range perm.GetResources() {
			resourceIDs = append(resourceIDs, i.GetResourceIds()...)
		}
	}
	resourceIDs = slicex.Unique(resourceIDs)
	log.DebugContextf(ctx, "获取用户资源 resourceIDs:%+v", resourceIDs)
	return resourceIDs, nil
}

// MustCreateBusinessAdministrator 确保创建主账号
func (d *dao) MustCreateBusinessAdministrator(ctx context.Context, uin, desc string, pp model.ProductPermission) error {
	cfg := config.App().AIConf
	if _, err := d.DescribeAccountInfo(ctx, uin); err != nil {
		return err
	}
	r, err := d.DescribeUserPermissions(ctx, uin)
	if err != nil {
		return err
	}
	if r.IsNewAccount {
		return d.CreateBusinessAdministrator(ctx, uin, desc, pp)
	}
	return d.ModifyBusinessAdministrator(
		ctx, uin, desc, append(
			slicex.Filter(
				r.Permissions, func(p model.ProductPermission) bool { return p.ProductType != cfg.ProductType },
			),
			pp,
		),
	)
}

// DescribeUserPermissions 获取主账号权限数据
func (d *dao) DescribeUserPermissions(ctx context.Context, uin string) (*model.DescribeUserPermissionsRsp, error) {
	cfg := config.App().AIConf
	req := &model.DescribeUserPermissionsReq{
		Uin:           cfg.SuperAdminUin,
		SubAccountUin: cfg.SuperAdminUin,
		AccountUin:    uin,
	}
	opts := []client.Option{WithTrpcSelector()}
	w := &model.Response{Response: &model.DescribeUserPermissionsRsp{}}
	if err := d.aiConfCli.Post(ctx, "/cgi/DescribeUserPermissions", req, &w, opts...); err != nil {
		log.ErrorContextf(ctx, "Describe user permissions error: %+v, req: %+v", err, req)
		return nil, err
	}
	rsp := w.Response.(*model.DescribeUserPermissionsRsp)
	if rsp.Error != nil {
		err := errs.ErrWrapf(errs.ErrCodeAIConf, rsp.Error.Message)
		log.ErrorContextf(ctx, "Describe user permissions biz error: %+v, req: %+v", err, req)
		return nil, err
	}
	return rsp, nil
}

// DescribeAccountInfo 获取账号基本信息
func (d *dao) DescribeAccountInfo(ctx context.Context, accountUin string) (*model.DescribeAccountInfoRsp, error) {
	cfg := config.App().AIConf
	req := &model.DescribeAccountInfoReq{
		Uin:           cfg.SuperAdminUin,
		SubAccountUin: cfg.SuperAdminUin,
		AccountUin:    accountUin,
	}
	opts := []client.Option{WithTrpcSelector()}
	w := &model.Response{Response: &model.DescribeAccountInfoRsp{}}
	if err := d.aiConfCli.Post(ctx, "/cgi/DescribeAccountInfo", req, &w, opts...); err != nil {
		log.ErrorContextf(ctx, "Describe account info error: %+v, req: %+v", err, req)
		return nil, err
	}
	rsp := w.Response.(*model.DescribeAccountInfoRsp)
	if rsp.Error != nil {
		err := errs.ErrWrapf(errs.ErrCodeAIConf, rsp.Error.Message)
		log.ErrorContextf(ctx, "Describe account info biz error: %+v, req: %+v", err, req)
		return nil, err
	}
	return rsp, nil
}

// DescribeProductPermissions 获取产品对应的权限数据
func (d *dao) DescribeProductPermissions(ctx context.Context) (*model.DescribeProductPermissionsRsp, error) {
	cfg := config.App().AIConf
	req := model.DescribeProductPermissionsReq{
		Uin:           cfg.SuperAdminUin,
		SubAccountUin: cfg.SuperAdminUin,
	}
	opts := []client.Option{WithTrpcSelector()}
	w := &model.Response{Response: &model.DescribeProductPermissionsRsp{}}
	if err := d.aiConfCli.Post(ctx, "/cgi/DescribeProductPermissions", req, &w, opts...); err != nil {
		log.ErrorContextf(ctx, "Describe product permission error: %+v, req: %+v", err, req)
		return nil, err
	}
	rsp := w.Response.(*model.DescribeProductPermissionsRsp)
	if rsp.Error != nil {
		err := errs.ErrWrapf(errs.ErrCodeAIConf, rsp.Error.Message)
		log.ErrorContextf(ctx, "Describe product permission biz error: %+v, req: %+v", err, req)
		return nil, err
	}
	return rsp, nil
}

// CreateBusinessAdministrator 创建主账号
func (d *dao) CreateBusinessAdministrator(ctx context.Context, uin, desc string, perms model.ProductPermission) error {
	cfg := config.App().AIConf
	req := &model.CreateBusinessAdministratorReq{
		Uin:           cfg.SuperAdminUin,
		SubAccountUin: cfg.SuperAdminUin,
		Src:           "online",
		AccountUin:    uin,
		Description:   desc,
		Permissions:   []model.ProductPermission{perms},
	}
	opts := []client.Option{WithTrpcSelector()}
	w := &model.Response{Response: &model.CreateBusinessAdministratorRsp{}}
	if err := d.aiConfCli.Post(ctx, "/cgi/CreateBusinessAdministrator", req, &w, opts...); err != nil {
		log.ErrorContextf(ctx, "Create business administrator error: %+v, req: %+v", err, req)
		return err
	}
	rsp := w.Response.(*model.CreateBusinessAdministratorRsp)
	if rsp.Error != nil {
		err := errs.ErrWrapf(errs.ErrCodeAIConf, rsp.Error.Message)
		log.ErrorContextf(ctx, "Create business administrator biz error: %+v, req: %+v", err, req)
		return err
	}
	return nil
}

// ModifyBusinessAdministrator 编辑主账号
func (d *dao) ModifyBusinessAdministrator(
	ctx context.Context, uin, desc string, perms []model.ProductPermission,
) error {
	cfg := config.App().AIConf
	req := &model.ModifyBusinessAdministratorReq{
		Uin:           cfg.SuperAdminUin,
		SubAccountUin: cfg.SuperAdminUin,
		Src:           "online",
		AccountUin:    uin,
		Description:   desc,
		Permissions:   perms,
	}
	opts := []client.Option{WithTrpcSelector()}
	w := &model.Response{Response: &model.ModifyBusinessAdministratorRsp{}}
	if err := d.aiConfCli.Post(ctx, "/cgi/ModifyBusinessAdministrator", req, &w, opts...); err != nil {
		log.ErrorContextf(ctx, "Modify business administrator error: %+v, req: %+v", err, req)
		return err
	}
	rsp := w.Response.(*model.ModifyBusinessAdministratorRsp)
	if rsp.Error != nil {
		err := errs.ErrWrapf(errs.ErrCodeAIConf, rsp.Error.Message)
		log.ErrorContextf(ctx, "Modify business administrator biz error: %+v, req: %+v", err, req)
		return err
	}
	return nil
}
