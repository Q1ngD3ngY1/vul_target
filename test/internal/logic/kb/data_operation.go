package kb

import (
	"context"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	entity2 "git.woa.com/adp/kb/kb-config/internal/entity"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// BatchGetAppName 批量获取应用信息
func BatchGetAppName(ctx context.Context, rpc *rpc.RPC, sharedKGAppList []*entity.AppShareKnowledge) (map[uint64]string, error) {
	appBizIDList := slicex.Map(sharedKGAppList, func(item *entity.AppShareKnowledge) uint64 {
		return item.AppBizID
	})

	appDBs, _, err := rpc.AppAdmin.ListAppBaseInfo(ctx, &appconfig.ListAppBaseInfoReq{
		AppBizIds:  appBizIDList,
		PageNumber: 1,
		PageSize:   uint32(len(appBizIDList)),
	})
	if err != nil {
		logx.E(ctx, "BatchGetAppName failed, error: %+v", err)
		return nil, err
	}

	appInfoMap := slicex.MapKV(appDBs, func(item *entity2.AppBaseInfo) (uint64, string) {
		return item.BizId, item.Name
	})

	logx.I(ctx, "BatchGetAppName, appInfoMap(%d): %+v", len(appInfoMap), appInfoMap)
	return appInfoMap, nil
}

// RetrieveModelConfig 检索模型配置
func (l *Logic) RetrieveModelConfig(ctx context.Context, corpBizID uint64,
	knowledgeList []*entity.SharedKnowledgeInfo) (
	[]*entity.SharedKnowledgeInfo, error) {
	knowledgeBizIDList := slicex.Map(knowledgeList, func(item *entity.SharedKnowledgeInfo) uint64 {
		return item.BusinessID
	})
	logx.I(ctx, "RetrieveModelConfig, corpBizID: %d, knowledgeBizIDList(%d): %+v",
		corpBizID, len(knowledgeBizIDList), knowledgeBizIDList)
	// NOTICE: 检索模型信息(QaExtract/KnowledgeSchema/Embedding)
	configList, err := l.GetShareKBModelNames(ctx, corpBizID,
		knowledgeBizIDList, []uint32{
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
			uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL),
			uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
		})
	if err != nil {
		logx.E(ctx, "RetrieveModelConfig, GetKnowledgeBaseConfig failed, error: %v", err)
		return nil, errs.ErrQueryKnowledgeModelConfigFailed
	}
	logx.I(ctx, "RetrieveModelConfig, configList(%d): %+v",
		len(configList), configList)

	knowledgeMap := slicex.MapKV(knowledgeList, func(item *entity.SharedKnowledgeInfo) (
		uint64, *entity.SharedKnowledgeInfo) {
		return item.BusinessID, item
	})

	// NOTICE: 填充模型信息
	for _, config := range configList {
		item, ok := knowledgeMap[config.KnowledgeBizID]
		if !ok {
			logx.W(ctx, "RetrieveModelConfig, knowledge not found, "+
				"knowledgeBizID: %d", config.KnowledgeBizID)
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
			logx.W(ctx, "RetrieveModelConfig, unexpected config type, config: %+v",
				config)
		}

	}
	return knowledgeList, nil
}
