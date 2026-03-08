package service

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
	"strconv"
)

// SetKnowledgeBaseConfig 设置知识库配置
func (s *Service) SetKnowledgeBaseConfig(ctx context.Context, req *pb.SetKnowledgeBaseConfigReq) (*pb.SetKnowledgeBaseConfigRsp, error) {
	log.InfoContextf(ctx, "SetKnowledgeBaseConfig Req:%+v", req)
	rsp := new(pb.SetKnowledgeBaseConfigRsp)
	corpBizId := pkg.CorpBizID(ctx)
	knowledgeBaseConfigs := make([]*pb.KnowledgeBaseConfig, 0)
	knowledgeBaseConfigs = append(knowledgeBaseConfigs, &pb.KnowledgeBaseConfig{
		KnowledgeBizId:       req.GetKnowledgeBizId(),
		ConfigTypes:          req.GetConfigTypes(),
		ThirdAclConfig:       req.GetThirdAclConfig(),
		EmbeddingModel:       req.GetEmbeddingModel(),
		QaExtractModel:       req.GetQaExtractModel(),
		KnowledgeSchemaModel: req.GetKnowledgeSchemaModel(),
	})
	err := knowledge_config.SetKnowledgeBaseConfig(ctx, corpBizId, knowledgeBaseConfigs)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

// GetKnowledgeBaseConfig 获取知识库配置
func (s *Service) GetKnowledgeBaseConfig(ctx context.Context, req *pb.GetKnowledgeBaseConfigReq) (*pb.GetKnowledgeBaseConfigRsp, error) {
	log.InfoContextf(ctx, "GetKnowledgeBaseConfig Req:%+v", req)
	rsp := new(pb.GetKnowledgeBaseConfigRsp)
	corpBizId := pkg.CorpBizID(ctx)
	knowledgeBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetKnowledgeBizId())
	if err != nil {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "knowledgeBizId: %s", req.GetKnowledgeBizId())
	}
	var configTypes []uint32
	for _, configType := range req.GetConfigTypes() {
		configTypes = append(configTypes, uint32(configType))
	}
	knowledgeBaseConfigs, err := knowledge_config.GetKnowledgeBaseConfigs(ctx, corpBizId, []uint64{knowledgeBizId}, configTypes)
	if err != nil {
		return nil, err
	}
	pbKnowledgeBaseConfigs, err := KnowledgeConfigsDbToPb(ctx, knowledgeBaseConfigs)
	if err != nil {
		return nil, err
	}
	if len(pbKnowledgeBaseConfigs) > 0 {
		rsp.KnowledgeBaseConfig = pbKnowledgeBaseConfigs[0]
	}
	return rsp, nil
}

func KnowledgeConfigsDbToPb(ctx context.Context, configs []*model.KnowledgeConfig) ([]*pb.KnowledgeBaseConfig, error) {
	pbKnowledgeConfigMap := make(map[uint64]*pb.KnowledgeBaseConfig)
	var err error
	for _, config := range configs {
		if config.KnowledgeBizId == 0 || config.Type == 0 || config.Config == "" {
			log.ErrorContextf(ctx, "KnowledgeConfigsDbToPb config is empty, config:%+v", config)
			continue
		}
		pbConfig, ok := pbKnowledgeConfigMap[config.KnowledgeBizId]
		if !ok {
			pbConfig = &pb.KnowledgeBaseConfig{
				KnowledgeBizId: strconv.FormatUint(config.KnowledgeBizId, 10),
			}
		}
		pbConfig.ConfigTypes = append(pbConfig.ConfigTypes, pb.KnowledgeBaseConfigType(config.Type))
		switch pb.KnowledgeBaseConfigType(config.Type) {
		case pb.KnowledgeBaseConfigType_THIRD_ACL:
			thirdAclConfig := &pb.ThirdAclConfig{}
			err = jsoniter.Unmarshal([]byte(config.Config), thirdAclConfig)
			if err != nil {
				log.ErrorContextf(ctx, "KnowledgeConfigsDbToPb jsoniter.Unmarshal err:%v, config:%+v", err, config)
				return nil, err
			}
			pbConfig.ThirdAclConfig = thirdAclConfig
		case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
			pbConfig.EmbeddingModel = config.Config
		case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
			pbConfig.QaExtractModel = config.Config
		case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
			pbConfig.KnowledgeSchemaModel = config.Config
		default:
			log.WarnContextf(ctx, "KnowledgeConfigsDbToPb configType:%d illegal", config.Type)
			return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "configType: %d", config.Type)
		}
		pbKnowledgeConfigMap[config.KnowledgeBizId] = pbConfig
	}
	knowledgeConfigs := make([]*pb.KnowledgeBaseConfig, 0)
	for _, knowledgeConfig := range pbKnowledgeConfigMap {
		knowledgeConfigs = append(knowledgeConfigs, knowledgeConfig)
	}
	return knowledgeConfigs, nil
}
