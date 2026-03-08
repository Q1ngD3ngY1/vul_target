package share_knowledge

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// BatchGetAppInfo 批量获取应用信息
func BatchGetAppInfo(ctx context.Context,
	sharedKGAppList []*model.AppShareKnowledge) (map[uint64]*admin.GetAppsByBizIDsRsp_AppInfo, error) {
	appBizIDList := slicex.Map(sharedKGAppList, func(item *model.AppShareKnowledge) uint64 {
		return item.AppBizID
	})

	getAppsResponse, err := client.GetAppsByBizIDs(ctx, appBizIDList, model.RunEnvSandbox)
	if err != nil {
		log.ErrorContextf(ctx, "BatchGetAppInfo failed, error: %+v", err)
		return nil, err
	}

	appInfoMap := slicex.MapKV(getAppsResponse.Apps, func(item *admin.GetAppsByBizIDsRsp_AppInfo) (
		uint64, *admin.GetAppsByBizIDsRsp_AppInfo) {
		return item.GetAppBizId(), item
	})

	log.InfoContextf(ctx, "BatchGetAppInfo, appInfoMap(%d): %+v", len(appInfoMap), appInfoMap)
	return appInfoMap, nil
}

// RetrieveModelConfig 检索模型配置
func RetrieveModelConfig(ctx context.Context, corpBizID uint64, knowledgeList []*model.SharedKnowledgeInfo) (
	[]*model.SharedKnowledgeInfo, error) {
	knowledgeBizIDList := slicex.Map(knowledgeList, func(item *model.SharedKnowledgeInfo) uint64 {
		return item.BusinessID
	})
	log.InfoContextf(ctx, "RetrieveModelConfig, corpBizID: %d, knowledgeBizIDList(%d): %+v",
		corpBizID, len(knowledgeBizIDList), knowledgeBizIDList)

	// NOTICE: 检索模型信息(QaExtract/KnowledgeSchema/Embedding)
	configList, err := knowledge_config.GetKnowledgeBaseConfigs(ctx, corpBizID,
		knowledgeBizIDList, []uint32{
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
			uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL),
			uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
		})
	if err != nil {
		log.ErrorContextf(ctx, "RetrieveModelConfig, GetKnowledgeBaseConfig failed, error: %v", err)
		return nil, errs.ErrQueryKnowledgeModelConfigFailed
	}

	log.InfoContextf(ctx, "RetrieveModelConfig, configList(%d): %+v",
		len(configList), configList)

	knowledgeMap := slicex.MapKV(knowledgeList, func(item *model.SharedKnowledgeInfo) (
		uint64, *model.SharedKnowledgeInfo) {
		return item.BusinessID, item
	})

	// NOTICE: 填充模型信息
	for _, config := range configList {
		item, ok := knowledgeMap[config.KnowledgeBizId]
		if !ok {
			log.WarnContextf(ctx, "RetrieveModelConfig, knowledge not found, "+
				"knowledgeBizID: %d", config.KnowledgeBizId)
			continue
		}

		switch pb.KnowledgeBaseConfigType(config.Type) {
		case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
			item.EmbeddingModel = config.Config
		case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
			item.QaExtractModel = config.Config
		case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
			item.KnowledgeSchemaModel = config.Config
		default:
			log.WarnContextf(ctx, "RetrieveModelConfig, unexpected config type, config: %+v",
				config)
		}

	}

	return knowledgeList, nil
}

// DeleteModelConfig 删除模型配置
func DeleteModelConfig(ctx context.Context, corpBizID uint64, knowledgeBizID uint64) error {
	log.InfoContextf(ctx, "DeleteModelConfig, corpBizID: %d, knowledgeBizID: %d",
		corpBizID, knowledgeBizID)

	// NOTICE: 删除模型信息(QaExtract/KnowledgeSchema/Embedding)
	err := dao.GetKnowledgeConfigDao(nil).DeleteKnowledgeConfigs(ctx, corpBizID, []uint64{
		knowledgeBizID,
	})

	return err
}
