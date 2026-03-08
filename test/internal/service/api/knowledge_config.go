package api

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// InnerSetKnowledgeBaseConfig 设置知识配置
func (s *Service) InnerSetKnowledgeBaseConfig(ctx context.Context, req *pb.InnerSetKnowledgeBaseConfigReq) (
	*pb.InnerSetKnowledgeBaseConfigRsp, error) {
	corpBizId := pkg.CorpBizID(ctx)
	rsp := new(pb.InnerSetKnowledgeBaseConfigRsp)
	err := knowledge_config.SetKnowledgeBaseConfig(ctx, corpBizId, req.GetKnowledgeBaseConfigs())
	return rsp, err
}

// InnerGetKnowledgeBaseConfig 获取知识配置
func (s *Service) InnerGetKnowledgeBaseConfig(ctx context.Context, req *pb.InnerGetKnowledgeBaseConfigReq) (
	*pb.InnerGetKnowledgeBaseConfigRsp, error) {
	corpBizId := pkg.CorpBizID(ctx)
	var knowledgeBizIds []uint64
	for _, knowledgeBizId := range req.GetKnowledgeBizIds() {
		knowledgeBizIdUint64, err := util.CheckReqParamsIsUint64(ctx, knowledgeBizId)
		if err != nil {
			return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "knowledgeBizId: %s", knowledgeBizId)
		}
		knowledgeBizIds = append(knowledgeBizIds, knowledgeBizIdUint64)
	}
	var configTypes []uint32
	for _, configType := range req.GetConfigTypes() {
		configTypes = append(configTypes, uint32(configType))
	}
	knowledgeBaseConfigs, err := knowledge_config.GetKnowledgeBaseConfigs(ctx, corpBizId, knowledgeBizIds, configTypes)
	if err != nil {
		return nil, err
	}
	pbKnowledgeBaseConfigs, err := service.KnowledgeConfigsDbToPb(ctx, knowledgeBaseConfigs)
	if err != nil {
		return nil, err
	}
	rsp := &pb.InnerGetKnowledgeBaseConfigRsp{
		KnowledgeBaseConfigs: pbKnowledgeBaseConfigs,
	}
	return rsp, nil
}
