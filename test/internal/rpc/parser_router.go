package rpc

import (
	"context"

	file_parse_common "git.woa.com/adp/pb-go/kb/parse_engine/file_parse_common"
)

type ParseRouterRPC interface {
	DescribeTaskStatusList(ctx context.Context, req *file_parse_common.DescribeTaskStatusListReq) (*file_parse_common.DescribeTaskStatusListRsp, error)
}

func (r *RPC) DescribeTaskStatusList(ctx context.Context, req *file_parse_common.DescribeTaskStatusListReq) (*file_parse_common.DescribeTaskStatusListRsp, error) {
	rsp, err := r.parseRouter.DescribeTaskStatusList(ctx, req)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}
