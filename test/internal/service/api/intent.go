package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetIntent 创建消息通知
func (s *Service) GetIntent(ctx context.Context, req *pb.GetIntentReq) (*pb.GetIntentRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	return &pb.GetIntentRsp{}, nil
}

// ListIntent 意图列表
func (s *Service) ListIntent(ctx context.Context, req *pb.ListIntentReq) (*pb.ListIntentRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ListIntentRsp)
	return rsp, nil
}

// CreateIntent 新增意图
func (s *Service) CreateIntent(ctx context.Context, req *pb.CreateIntentReq) (*pb.CreateIntentRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CreateIntentRsp)
	return rsp, nil
}

// UpdateIntent 更新意图
func (s *Service) UpdateIntent(ctx context.Context, req *pb.UpdateIntentReq) (*pb.UpdateIntentRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.UpdateIntentRsp)
	return rsp, nil
}

// DeleteIntent 删除意图
func (s *Service) DeleteIntent(ctx context.Context, req *pb.DeleteIntentReq) (*pb.DeleteIntentRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.DeleteIntentRsp)
	return rsp, nil
}

// ListIntentByPolicyID 获取策略绑定的意图列表
func (s *Service) ListIntentByPolicyID(ctx context.Context, req *pb.ListIntentByPolicyIDReq) (
	*pb.ListIntentByPolicyIDRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ListIntentByPolicyIDRsp)
	return rsp, nil
}

// ListUnusedIntentKeyMap 获取未使用的意图列表
func (s *Service) ListUnusedIntentKeyMap(ctx context.Context, req *pb.ListUnusedIntentKeyMapReq) (
	*pb.ListUnusedIntentKeyMapRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ListUnusedIntentKeyMapRsp)
	return rsp, nil
}
