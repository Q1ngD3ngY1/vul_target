package api

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// CreateCorpCustomModel 新增自定义模型
func (s *Service) CreateCorpCustomModel(ctx context.Context, req *pb.CreateCorpCustomModelReq) (
	*pb.CreateCorpCustomModelRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CreateCorpCustomModelRsp)
	return rsp, nil
}
