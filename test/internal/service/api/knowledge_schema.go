package api

import (
	"context"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

func (s *Service) GetKnowledgeSchema(ctx context.Context, req *pb.GetKnowledgeSchemaReq) (*pb.GetKnowledgeSchemaRsp, error) {
	return service.GetKnowledgeSchema(ctx, req, s.dao)
}
