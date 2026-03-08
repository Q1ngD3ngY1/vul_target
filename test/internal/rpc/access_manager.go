package rpc

import (
	"context"

	pb "git.woa.com/dialogue-platform/proto/pb-stub/access-manager-server"
)

type AccessManagerRPC interface {
	ListPermissions(context.Context, *pb.ListPermissionsReq) (*pb.ListPermissionsRsp, error)
}

// ListPermissions 列出权限
func (r *RPC) ListPermissions(ctx context.Context, req *pb.ListPermissionsReq) (*pb.ListPermissionsRsp, error) {
	return r.accessManager.ListPermissions(ctx, req)
}
