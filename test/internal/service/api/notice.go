package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// CreateNotice 创建消息通知
func (s *Service) CreateNotice(ctx context.Context, req *pb.CreateNoticeReq) (*pb.CreateNoticeRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CreateNoticeRsp)
	return rsp, nil
}
