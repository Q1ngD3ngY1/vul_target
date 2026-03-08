package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ListIntentPolicy 策略列表
func (s *Service) ListIntentPolicy(ctx context.Context, req *pb.ListIntentPolicyReq) (*pb.ListIntentPolicyRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ListIntentPolicyRsp)
	return rsp, nil
}

// CreateIntentPolicy 新建策略
func (s *Service) CreateIntentPolicy(ctx context.Context, req *pb.CreateIntentPolicyReq) (
	*pb.CreateIntentPolicyRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CreateIntentPolicyRsp)
	return rsp, nil
}

// UpdateIntentPolicy 更新策略
func (s *Service) UpdateIntentPolicy(ctx context.Context, req *pb.UpdateIntentPolicyReq) (
	*pb.UpdateIntentPolicyRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.UpdateIntentPolicyRsp)
	return rsp, nil
}

// DeleteIntentPolicy 删除策略
func (s *Service) DeleteIntentPolicy(ctx context.Context, req *pb.DeleteIntentPolicyReq) (
	*pb.DeleteIntentPolicyRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.DeleteIntentPolicyRsp)
	return rsp, nil
}

// ListIntentPolicyKeyMap 获取意图映射列表
func (s *Service) ListIntentPolicyKeyMap(ctx context.Context, req *pb.ListIntentPolicyKeyMapReq) (
	*pb.ListIntentPolicyKeyMapRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ListIntentPolicyKeyMapRsp)
	return rsp, nil
}
