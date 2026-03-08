package api

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"golang.org/x/net/context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/share_knowledge"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetModelAssociatedApps 获取模型关联的应用信息
func (s *Service) GetModelAssociatedApps(ctx context.Context, req *pb.GetModelAssociatedAppsReq) (*pb.GetModelAssociatedAppsRsp, error) {
	resp := &pb.GetModelAssociatedAppsRsp{}
	log.InfoContextf(ctx, "GetModelAssociatedApps, request: %+v", req)
	spaceID := pkg.SpaceID(ctx)
	knowledgeBaseInfoList, err := share_knowledge.GetModelAssociatedApps(ctx, s.dao, req.GetCorpBizId(), spaceID, req.GetModelKeyword())
	if err != nil {
		return nil, err
	}
	resp.KnowledgeBases = knowledgeBaseInfoList

	return resp, nil
}
