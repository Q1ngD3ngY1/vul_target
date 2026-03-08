package knowledge_config

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	logicKnowledgeBase "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_base"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pbknowledge "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	ChangeEmbeddingModelUpdateBaseVersion = 10000
)

// SetKnowledgeBaseConfig 设置知识库配置
func SetKnowledgeBaseConfig(ctx context.Context, corpBizId uint64,
	configs []*pb.KnowledgeBaseConfig) error {
	for _, config := range configs {
		knowledgeBizId, err := util.CheckReqParamsIsUint64(ctx, config.GetKnowledgeBizId())
		if err != nil {
			return err
		}
		for _, configType := range config.ConfigTypes {
			// 先读取旧的配置，如果未变就不用更新
			oldConfigStr, err := GetKnowledgeBaseConfig(ctx, corpBizId, knowledgeBizId, uint32(configType))
			if err != nil {
				return err
			}
			var newConfigStr string
			oldEmbeddingModelUpdateInfo := &model.EmbeddingModelUpdateInfo{}
			switch configType {
			case pb.KnowledgeBaseConfigType_THIRD_ACL:
				newConfigStr, err = jsoniter.MarshalToString(config.ThirdAclConfig)
				if err != nil {
					log.ErrorContextf(ctx, "SetKnowledgeSchemaConfig marshal thirdAclConfig fail, err=%+v", err)
					return err
				}
			case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
				if strings.HasPrefix(oldConfigStr, "{") {
					// 初始状态旧配置是默认embedding模型名称，比如"sn-llm-embedding-2b-v0.2.2"
					// 变更过一次之后就是EmbeddingModelUpdateInfo接口体的json字符串，比如"{"xxx": "yyy"}"
					// 所以通过"{"前缀来判断是第1次变更，还是第n次变更
					err = jsoniter.UnmarshalFromString(oldConfigStr, oldEmbeddingModelUpdateInfo)
					if err != nil {
						return err
					}
					log.DebugContextf(ctx, "oldEmbeddingModelUpdateInfo: %+v", oldEmbeddingModelUpdateInfo)
					oldConfigStr = oldEmbeddingModelUpdateInfo.NewModelName
				}
				newConfigStr = config.EmbeddingModel
			case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
				newConfigStr = config.QaExtractModel
			case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
				newConfigStr = config.KnowledgeSchemaModel
			default:
				log.WarnContextf(ctx, "SetKnowledgeSchemaConfig configType:%d illegal", configType)
				return errs.ErrWrapf(errs.ErrParameterInvalid, "configType: %d", configType)
			}
			if oldConfigStr != "" && oldConfigStr == newConfigStr {
				// 如果未变就不用更新
				continue
			}

			if configType == pb.KnowledgeBaseConfigType_EMBEDDING_MODEL {
				// 检查白名单
				uin := pkg.Uin(ctx)
				if uin == "" {
					return errs.ErrWrapf(errs.ErrParameterInvalid, "uin is empty")
				}
				if !utilConfig.IsInWhiteList(uin, knowledgeBizId, utilConfig.GetWhitelistConfig().UpdateEmbeddingModelWhiteList) {
					log.WarnContextf(ctx, "SetKnowledgeSchemaConfig update embedding model IsInWhiteList fail, "+
						"uin:%s, knowledgeBizId:%d", uin, knowledgeBizId)
					return errs.ErrWrapf(errs.ErrEmbeddingModelUpdateFailed, i18n.Translate(ctx, i18nkey.KeyVectorModelChangeRequireWhitelist))
				}
				// 先检查是否在embedding model变更处理中
				isProcessing, err := logicKnowledgeBase.CheckProcessingFlag(ctx, corpBizId, knowledgeBizId,
					pbknowledge.KnowledgeBaseInfo_EMBEDDING_MODEL_CHANGING)
				if err != nil {
					log.ErrorContextf(ctx, "SetKnowledgeSchemaConfig CheckProcessingFlag fail, err=%+v", err)
					return err
				}
				if isProcessing {
					log.WarnContextf(ctx, "SetKnowledgeSchemaConfig isProcessing, corpBizId:%d, knowledgeBizId:%d", corpBizId, knowledgeBizId)
					return errs.ErrWrapf(errs.ErrEmbeddingModelUpdating, i18n.Translate(ctx, i18nkey.KeyVectorModelChangeProcessing))
				}

				// 设置embedding model切换处理中标记
				err = logicKnowledgeBase.AddProcessingFlags(ctx, corpBizId, []uint64{knowledgeBizId},
					[]pbknowledge.KnowledgeBaseInfo_ProcessingFlag{pbknowledge.KnowledgeBaseInfo_EMBEDDING_MODEL_CHANGING})
				if err != nil {
					log.ErrorContextf(ctx, "SetKnowledgeSchemaConfig AddProcessingFlags fail, err=%+v", err)
					return err
				}

				// 更新embedding model时，需要刷新向量库
				newEmbeddingModelUpdateInfo := &model.EmbeddingModelUpdateInfo{}
				if oldEmbeddingModelUpdateInfo.NewModelName == "" {
					// 说明是第1次变更
					app, err := knowClient.GetAppInfo(ctx, knowledgeBizId, model.AppTestScenes)
					if err != nil {
						log.ErrorContextf(ctx, "GetKnowledgeSchemaConfig GetAppInfo fail, err=%+v", err)
						return err
					}
					newEmbeddingModelUpdateInfo.OldModelName = ""
					newEmbeddingModelUpdateInfo.OldModelVersion = app.GetKnowledgeQa().GetEmbedding().GetVersion()
					newEmbeddingModelUpdateInfo.NewModelName = config.EmbeddingModel
					newEmbeddingModelUpdateInfo.NewModelVersion = ChangeEmbeddingModelUpdateBaseVersion
				} else {
					newEmbeddingModelUpdateInfo.OldModelName = oldEmbeddingModelUpdateInfo.NewModelName
					newEmbeddingModelUpdateInfo.OldModelVersion = oldEmbeddingModelUpdateInfo.NewModelVersion
					newEmbeddingModelUpdateInfo.NewModelName = config.EmbeddingModel
					newEmbeddingModelUpdateInfo.NewModelVersion = oldEmbeddingModelUpdateInfo.NewModelVersion + 1
				}

				newConfigStr, err = jsoniter.MarshalToString(newEmbeddingModelUpdateInfo)
				if err != nil {
					log.ErrorContextf(ctx, "SetKnowledgeSchemaConfig marshal embeddingModelUpdateInfo fail, err=%+v", err)
					return err
				}

				params := &model.UpdateEmbeddingModelParams{
					AppBizID:                 knowledgeBizId,
					EmbeddingModelUpdateInfo: newEmbeddingModelUpdateInfo,
					ChunkSize:                2000,
					RetryTimes:               3,
					RetryInterval:            1000,
					Batch:                    10,
				}
				appId, err := dao.GetAppIDByAppBizID(ctx, knowledgeBizId)
				if err != nil {
					log.ErrorContextf(ctx, "GetKnowledgeSchemaConfig GetAppIDByAppBizID fail, err=%+v", err)
					return err
				}
				_, err = dao.NewUpdateEmbeddingModelTask(ctx, appId, params)
				if err != nil {
					log.ErrorContextf(ctx, "GetKnowledgeSchemaConfig NewUpdateEmbeddingModelTask fail, err=%+v", err)
					return err
				}
			}

			err = dao.GetKnowledgeConfigDao(nil).
				SetKnowledgeConfig(ctx, corpBizId, knowledgeBizId, uint32(configType), newConfigStr)
			if err != nil {
				log.ErrorContextf(ctx, "SetKnowledgeSchemaConfig dao.SetKnowledgeConfig fail, err:%+v", err)
				return err
			}

		}
	}

	return nil
}

// GetKnowledgeBaseConfig 获取知识库配置
func GetKnowledgeBaseConfig(ctx context.Context, corpBizId uint64, knowledgeBizId uint64, configType uint32) (string, error) {
	configs, err := GetKnowledgeBaseConfigs(ctx, corpBizId, []uint64{knowledgeBizId}, []uint32{configType})
	if err != nil {
		return "", err
	}
	if len(configs) == 0 || configs[0].Config == "" {
		errMsg := fmt.Sprintf("GetKnowledgeBaseConfig knowledgeBizId:%d configTypes:%d is empty",
			knowledgeBizId, configType)
		err = errors.New(errMsg)
		log.ErrorContextf(ctx, "GetKnowledgeBaseConfig err:%+v", err)
		return "", err
	}
	return configs[0].Config, nil
}

// GetKnowledgeBaseConfigs 批量获取知识库配置
func GetKnowledgeBaseConfigs(ctx context.Context, corpBizId uint64, knowledgeBizIds []uint64, configTypes []uint32) ([]*model.KnowledgeConfig, error) {
	knowledgeBaseConfigs, err := dao.GetKnowledgeConfigDao(nil).
		GetKnowledgeConfigs(ctx, corpBizId, knowledgeBizIds, configTypes)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaConfig dao.GetKnowledgeConfigDao fail, err=%+v", err)
		return nil, err
	}
	// 如果某个配置没查出来，则返回该配置的默认值
	defaultKnowledgeBaseConfigMap, err := GetDefaultKnowledgeBaseConfig(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeBaseConfig GetDefaultKnowledgeBaseConfig fail, err=%+v", err)
		return nil, err
	}
	log.DebugContextf(ctx, "defaultKnowledgeBaseConfigMap: %+v", defaultKnowledgeBaseConfigMap)
	knowledgeBizIDMap := make(map[string]struct{})
	for _, config := range knowledgeBaseConfigs {
		key := fmt.Sprintf("%d-%d", config.KnowledgeBizId, config.Type)
		knowledgeBizIDMap[key] = struct{}{}
	}
	for _, knowledgeBizId := range knowledgeBizIds {
		for _, configType := range configTypes {
			key := fmt.Sprintf("%d-%d", knowledgeBizId, configType)
			if _, ok := knowledgeBizIDMap[key]; !ok {
				knowledgeBaseConfig := &model.KnowledgeConfig{
					CorpBizID:      corpBizId,
					KnowledgeBizId: knowledgeBizId,
					Type:           configType,
				}
				knowledgeBaseConfigs = append(knowledgeBaseConfigs, knowledgeBaseConfig)
			}
		}
	}
	for _, config := range knowledgeBaseConfigs {
		// 如果有七彩石配置干预，则使用七彩石配置
		configInYaml := utilConfig.GetBotKnowledgeBaseConfig(corpBizId, config.KnowledgeBizId, config.Type)
		if configInYaml != "" {
			config.Config = configInYaml
			continue
		}
		if config.Config == "" {
			// 获取默认配置
			defaultConfig, ok := defaultKnowledgeBaseConfigMap[pb.KnowledgeBaseConfigType(config.Type)]
			if !ok {
				log.ErrorContextf(ctx, "GetDefaultKnowledgeBaseConfig configType:%d not found", config.Type)
				continue
			}
			config.Config = defaultConfig
		}
		switch config.Type {
		case uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL):
			if strings.HasPrefix(config.Config, "{") {
				embeddingModelUpdateInfo := &model.EmbeddingModelUpdateInfo{}
				err = jsoniter.UnmarshalFromString(config.Config, embeddingModelUpdateInfo)
				if err != nil {
					return nil, err
				}
				config.Config = embeddingModelUpdateInfo.NewModelName
			}
		}
	}
	return knowledgeBaseConfigs, nil
}

// GetDefaultKnowledgeBaseConfig 获取知识库默认配置
func GetDefaultKnowledgeBaseConfig(ctx context.Context) (map[pb.KnowledgeBaseConfigType]string, error) {
	defaultModelMap := map[pb.KnowledgeBaseConfigType]string{
		pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:        knowClient.ModelCategoryEmbedding,
		pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:       knowClient.ModelCategoryGenerate,
		pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL: knowClient.ModelCategoryGenerate,
	}
	defaultKnowledgeBaseConfigMap := make(map[pb.KnowledgeBaseConfigType]string)
	for k, v := range defaultModelMap {
		rsp, err := knowClient.GetDefaultModelConfig(ctx, v)
		if err != nil {
			log.ErrorContextf(ctx, "GetDefaultKnowledgeBaseConfig GetDefaultModelConfig fail, err=%+v", err)
			return defaultKnowledgeBaseConfigMap, err
		}
		defaultKnowledgeBaseConfigMap[k] = rsp.ModelName
	}
	return defaultKnowledgeBaseConfigMap, nil
}
