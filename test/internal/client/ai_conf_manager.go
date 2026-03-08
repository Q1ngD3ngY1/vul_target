package client

import (
	"context"
	"errors"
	"git.code.oa.com/trpc-go/trpc-go/log"
	aiconfManager "git.woa.com/dialogue-platform/proto/pb-stub/aiconf-manager-server"
)

// AddUserResourcePermission 添加用户数据权限
func AddUserResourcePermission(ctx context.Context, req *aiconfManager.AddUserResourcePermissionReq) error {
	rsp, err := aiConfMCli.AddUserResourcePermission(ctx, req, WithTrpcSelector())
	if err != nil {
		log.ErrorContextf(ctx, "aiconf.AddUserResourcePermission err:%+v,req:%+v", err, req)
		return err
	}
	if rsp.GetCode() != 0 {
		log.ErrorContextf(ctx, "aiconf.AddUserResourcePermission err req:%+v rsp:%+v", req, rsp)
		return errors.New(rsp.GetMessage())
	}
	return nil
}
