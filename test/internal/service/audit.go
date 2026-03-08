package service

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"

	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetAuditSwitch 获取审核开关
func (s *Service) GetAuditSwitch(ctx context.Context, req *pb.GetAuditSwitchReq) (*pb.GetAuditSwitchRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	return &pb.GetAuditSwitchRsp{}, nil
}

// DescribeAuditSwitch 获取审核开关
func (s *Service) DescribeAuditSwitch(ctx context.Context, req *pb.DescribeAuditSwitchReq) (*pb.DescribeAuditSwitchRsp,
	error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	return &pb.DescribeAuditSwitchRsp{}, nil
}
