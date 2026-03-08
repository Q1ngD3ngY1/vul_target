package api

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_base"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// DescribeKnowledgeBase 查询知识库信息
func (s *Service) DescribeKnowledgeBase(ctx context.Context, req *pb.DescribeKnowledgeBaseReq) (
	*pb.DescribeKnowledgeBaseRsp, error) {
	return knowledge_base.DescribeKnowledgeBase(ctx, req)
}
