package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appCommon "git.woa.com/adp/pb-go/app/common"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrievalPb "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
)

// SetKnowledgeBaseConfig 设置知识库配置
func (l *Logic) SetKnowledgeBaseConfig(ctx context.Context, corpBizID uint64, knowledgeConfigList []*kbEntity.KnowledgeConfig) error {
	if err := l.kbDao.TDSQLQuery().Transaction(func(tx *tdsqlquery.Query) error {
		for _, knowledgeConfig := range knowledgeConfigList {
			updateReleaseConfig := false
			if knowledgeConfig.AppBizID == 0 || knowledgeConfig.IsUpdateReleased {
				// 只有全局共享知识库才允许更新发布配置，应用下的知识库配置不允许通过外部调用直接更新发布配置，只能通过发布流程更新。
				updateReleaseConfig = true
			}
			err := l.kbDao.SetKnowledgeConfig(ctx, knowledgeConfig, tx, updateReleaseConfig)
			if err != nil {
				logx.E(ctx, "SetKnowledgeBaseConfig err: %+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "SetKnowledgeBaseConfig err: %+v", err)
		return err
	}
	// 这里清除缓存不能在事务中，因为缓存即使被清除了，事务还没提交，此时的并发查询还是会查到旧数据，回填缓存的就是旧数据，导致缓存不一致
	l.DeleteKBConfigCache(ctx, corpBizID, knowledgeConfigList)
	return nil
}

// DeleteKBConfigCache 删除知识库配置缓存
func (l *Logic) DeleteKBConfigCache(ctx context.Context, corpBizId uint64, knowledgeConfigList []*kbEntity.KnowledgeConfig) {
	// Cache-Aside模式，更新成功后，删除知识库配置缓存
	shareKBBizIDs := make([]uint64, 0)
	appBizIDs := make([]uint64, 0)
	for _, knowledgeConfig := range knowledgeConfigList {
		if knowledgeConfig.AppBizID != 0 {
			appBizIDs = append(appBizIDs, knowledgeConfig.AppBizID)
		} else {
			shareKBBizIDs = append(shareKBBizIDs, knowledgeConfig.KnowledgeBizID)
		}
	}
	shareKBBizIDs = slicex.Unique(shareKBBizIDs)
	appBizIDs = slicex.Unique(appBizIDs)
	for _, knowledgeBizID := range shareKBBizIDs {
		// Cache-Aside模式，更新成功后，删除共享知识库配置缓存
		l.kbDao.DeleteShareKnowledgeConfigFromCache(ctx, corpBizId, knowledgeBizID)
	}
	for _, appBizID := range appBizIDs {
		// Cache-Aside模式，更新成功后，删除应用下的知识库配置缓存
		l.kbDao.DeleteAppKnowledgeConfigFromCache(ctx, corpBizId, appBizID)
	}
}

func (l *Logic) GetKnowledgeEmbeddingModel(ctx context.Context, corpBizId, appBizId, knowledgeBizId uint64, isShare bool) (
	string, error) {
	var embeddingModelName string
	var err error
	if isShare {
		embeddingModelName, err = l.GetShareKnowledgeBaseConfig(ctx, corpBizId, knowledgeBizId,
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
		if err != nil {
			logx.E(ctx, "GetKnowledgeEmbeddingModel |  GetShareKnowledgeBaseConfig err:%+v", err)
			return "", err
		}

	} else {
		embeddingModelName, err = l.GetDefaultKnowledgeBaseConfig(ctx, corpBizId, appBizId, knowledgeBizId,
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL), bot_common.AdpDomain_ADP_DOMAIN_DEV)
		if err != nil {
			logx.E(ctx, "GetKnowledgeEmbeddingModel | GetDefaultKnowledgeBaseConfig err:%+v", err)
			return "", err
		}
	}
	return embeddingModelName, nil
}

func (l *Logic) GetKnowledgeText2sqlModelConfig(ctx context.Context, corpBizId, appBizId, knowledgeBizId uint64, isShare bool) (
	*retrievalPb.Text2SQLModelConfig, error) {
	retrievalConfig := &pb.RetrievalConfig{}
	var text2sqlConfig *retrievalPb.Text2SQLModelConfig
	var err error
	var config string
	if isShare {
		config, err = l.GetShareKnowledgeBaseConfig(ctx, corpBizId, knowledgeBizId, uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING))
		if err != nil {
			logx.W(ctx, "GetKnowledgeText2sqlModelConfig | GetShareKnowledgeBaseConfig err:%+v", err)
			return text2sqlConfig, err
		}

	} else {
		config, err = l.GetDefaultKnowledgeBaseConfig(ctx, corpBizId, appBizId, knowledgeBizId,
			uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING), bot_common.AdpDomain_ADP_DOMAIN_DEV)
		if err != nil {
			logx.W(ctx, "GetKnowledgeText2sqlModelConfig | GetDefaultKnowledgeBaseConfig err:%+v", err)
			return text2sqlConfig, err
		}
	}

	if config != "" {
		err = jsonx.UnmarshalFromString(config, retrievalConfig)
		if err != nil {
			logx.W(ctx, "GetKnowledgeText2sqlModelConfig | jsonx.UnmarshalFromString err:%+v", err)
			return text2sqlConfig, err
		}
		nl2sqlConf := retrievalConfig.GetSearchStrategy().GetNatureLanguageToSqlModelConfig()
		if nl2sqlConf != nil {
			text2sqlConfig = &retrievalPb.Text2SQLModelConfig{
				ModelConfig: map[string]*retrievalPb.RetrievalModelConfig{
					"text2sql": {
						ModelName:   nl2sqlConf.GetModel().GetModelName(),
						ModelParams: nl2sqlConf.GetModel().GetModelParams(),
					},
				},
			}
		}
	}

	return text2sqlConfig, err
}

// GetShareKnowledgeBaseConfig 获取共享知识库配置
// 如果是获取embedding模型，调用处会放过错误，来兼容历史数据.
func (l *Logic) GetShareKnowledgeBaseConfig(ctx context.Context, corpBizId uint64, knowledgeBizId uint64, configType uint32) (string, error) {
	configs, err := l.GetShareKBModelNames(ctx, corpBizId, []uint64{knowledgeBizId}, []uint32{configType})
	if err != nil {
		return "", err
	}
	if len(configs) == 0 || configs[0].Config == "" {
		errMsg := fmt.Sprintf("GetShareKnowledgeBaseConfig knowledgeBizId:%d configTypes:%d is empty",
			knowledgeBizId, configType)
		err = errors.New(errMsg)
		logx.W(ctx, "GetShareKnowledgeBaseConfig err:%+v", err)

		return "", err
	}
	return configs[0].Config, nil
}

func (l *Logic) CompactKnowledgeConfig(ctx context.Context, config string, configType uint32) string {
	switch configType {
	case uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL):
		configItem, err := l.ConvertStr2QAExtractModelConfigItem(ctx, config, true)
		if err != nil {
			logx.E(ctx, "GetKnowledgeBaseConfig ConvertStr2QAExtractModelConfigItem fail, err=%+v", err)
			return ""
		}
		return configItem.ModelName
	case uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL):
		configItem, err := l.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, config, true)
		if err != nil {
			logx.E(ctx, "GetKnowledgeBaseConfig ConvertStr2KnowledgeSchemaModelConfigItem fail, err=%+v", err)
			return ""
		}
		return configItem.ModelName
	case uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING):
		return config
	case uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL):
		configItem, err := l.ConvertStr2EmbeddingModelConfigItem(ctx, config, true)
		if err != nil {
			logx.E(ctx, "GetKnowledgeBaseConfig ConvertStr2EmbeddingModelConfigItem fail, err=%+v", err)
			return ""
		}
		return configItem.ModelName
	default:
		return config
	}
}

// GetRawShareKnowledgeBaseConfigs 批量共享获取知识库的原始配置（含超参）
func (l *Logic) GetRawShareKnowledgeBaseConfigs(ctx context.Context, corpBizId uint64, knowledgeBizIds []uint64, configTypes []uint32) ([]*kbEntity.KnowledgeConfig, error) {
	validConfigTypes := make([]uint32, 0)
	for _, configType := range configTypes {
		if configType == uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING) {
			// 共享知识库没有检索配置
			continue
		}
		validConfigTypes = append(validConfigTypes, configType)
	}
	knowledgeBaseConfigs, err := l.kbDao.GetShareKnowledgeConfigs(ctx, corpBizId, knowledgeBizIds, validConfigTypes)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchemaConfig dao.GetKnowledgeConfigDao fail, err=%+v", err)
		return nil, err
	}
	knowledgeBizIDMap := make(map[string]struct{})
	for _, configItem := range knowledgeBaseConfigs {
		configItem.Config = l.formatModelConfigStr(ctx, configItem.Type, configItem.Config)
		key := fmt.Sprintf("%d-%d", configItem.KnowledgeBizID, configItem.Type)
		knowledgeBizIDMap[key] = struct{}{}
	}
	for _, knowledgeBizId := range knowledgeBizIds {
		for _, configType := range validConfigTypes {
			key := fmt.Sprintf("%d-%d", knowledgeBizId, configType)
			if _, ok := knowledgeBizIDMap[key]; !ok {
				knowledgeBaseConfig := &kbEntity.KnowledgeConfig{
					CorpBizID:      corpBizId,
					KnowledgeBizID: knowledgeBizId,
					Type:           configType,
					Config:         l.formatModelConfigStr(ctx, configType, ""),
				}
				knowledgeBaseConfigs = append(knowledgeBaseConfigs, knowledgeBaseConfig)
			}
		}
	}
	return knowledgeBaseConfigs, nil
}

// GetDefaultKnowledgeBaseConfig 获取默认知识库配置
func (l *Logic) GetDefaultKnowledgeBaseConfig(ctx context.Context, corpBizId, appBizID, knowledgeBizId uint64, configType uint32, domain bot_common.AdpDomain) (string, error) {
	// 首先尝试从应用配置中获取
	config, found, err := l.getConfigFromAppKnowledge(ctx, corpBizId, appBizID, knowledgeBizId, configType, domain)
	if err != nil {
		logx.W(ctx, "GetDefaultKnowledgeBaseConfig getConfigFromAppKnowledge failed, will use default config, err: %+v", err)
		// 即使查询失败，也继续尝试使用默认配置
	}
	if found {
		return config, nil
	}
	logx.D(ctx, "GetDefaultKnowledgeBaseConfig knowledgeBizId:%d configType:%d not found in app config, use default conf", knowledgeBizId, configType)
	// 使用默认配置兜底，不区分开发域和发布域
	return l.getDefaultConfig(ctx, configType, knowledgeBizId)
}

// getConfigFromAppKnowledge 从应用知识库配置中获取指定类型的配置
func (l *Logic) getConfigFromAppKnowledge(ctx context.Context, corpBizId, appBizID, knowledgeBizId uint64, configType uint32, domain bot_common.AdpDomain) (string, bool, error) {
	configs, err := l.kbDao.DescribeAppKnowledgeConfig(ctx, corpBizId, appBizID, knowledgeBizId)
	if err != nil {
		return "", false, err
	}
	configMap := make(map[uint32]*kbEntity.KnowledgeConfig)
	for _, config := range configs {
		configMap[config.Type] = config
	}
	if config, exists := configMap[configType]; exists {
		if domain == bot_common.AdpDomain_ADP_DOMAIN_DEV {
			configStr := l.CompactKnowledgeConfig(ctx, config.PreviewConfig, config.Type)
			config.PreviewConfig = configStr
			return config.PreviewConfig, true, nil
		}
		configStr := l.CompactKnowledgeConfig(ctx, config.Config, config.Type)
		config.Config = configStr
		return config.Config, true, nil
	}

	return "", false, nil
}

// getDefaultConfig 获取默认配置
func (l *Logic) getDefaultConfig(ctx context.Context, configType uint32, knowledgeBizId uint64) (string, error) {
	defaultKnowledgeBaseConfigMap, err := l.GetKnowledgeBaseDefaultConfig(ctx)
	if err != nil {
		logx.E(ctx, "GetDefaultKnowledgeBaseConfig GetKnowledgeBaseDefaultConfig fail, err=%+v", err)
		return "", err
	}

	defaultConfig, ok := defaultKnowledgeBaseConfigMap[pb.KnowledgeBaseConfigType(configType)]
	if !ok {
		errMsg := fmt.Sprintf("GetDefaultKnowledgeBaseConfig knowledgeBizId:%d configType:%d not found in default config",
			knowledgeBizId, configType)
		err = errors.New(errMsg)
		logx.E(ctx, "GetDefaultKnowledgeBaseConfig err:%+v", err)
		return "", err
	}

	return defaultConfig, nil
}

// GetShareKBModelNames 批量获取共享知识库的模型名称(KnowledgeConfig.Config只包含模型名)
func (l *Logic) GetShareKBModelNames(ctx context.Context, corpBizId uint64, knowledgeBizIds []uint64, configTypes []uint32) ([]*kbEntity.KnowledgeConfig, error) {
	knowledgeBaseConfigs, err := l.GetRawShareKnowledgeBaseConfigs(ctx, corpBizId, knowledgeBizIds, configTypes)
	if err != nil {
		return nil, err
	}
	for _, configItem := range knowledgeBaseConfigs {
		configStr := l.CompactKnowledgeConfig(ctx, configItem.Config, configItem.Type)
		configItem.Config = configStr
	}
	return knowledgeBaseConfigs, nil
}

// GetKnowledgeBaseDefaultConfig 获取知识库默认配置
func (l *Logic) GetKnowledgeBaseDefaultConfig(ctx context.Context) (map[pb.KnowledgeBaseConfigType]string, error) {
	defaultModelMap := map[pb.KnowledgeBaseConfigType]string{
		pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:        entity.ModelCategoryEmbedding,
		pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:       entity.ModelCategoryGenerate,
		pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL: entity.ModelCategoryGenerate,
	}
	defaultKnowledgeBaseConfigMap := make(map[pb.KnowledgeBaseConfigType]string)
	for k, v := range defaultModelMap {
		modelName, err := l.GetDefaultKnowledgeConfigModel(ctx, v)
		if err != nil {
			logx.E(ctx, "GetKnowledgeBaseDefaultConfig GetDefaultModelConfig fail, err=%+v", err)
			return defaultKnowledgeBaseConfigMap, err
		}
		defaultKnowledgeBaseConfigMap[k] = modelName.ModelName

	}

	return defaultKnowledgeBaseConfigMap, nil

}

// GetDefaultKnowledgeConfigModel 获取知识库默认模型配置
func (l *Logic) GetDefaultKnowledgeConfigModel(ctx context.Context, modelType string) (kbEntity.KnowledgeModel, error) {
	rsp, err := l.rpc.Resource.GetDefaultModelConfig(ctx, modelType)
	if err != nil {
		logx.E(ctx, "GetDefaultKnowledgeConfigModel GetDefaultModelConfig fail (modelType: %s), err=%+v",
			modelType, err)
		return kbEntity.KnowledgeModel{}, err
	}
	return kbEntity.KnowledgeModel{
		ModelName:      rsp.ModelName,
		ModelAliasName: rsp.AliasName,
	}, nil
}

// DescribeAppKnowledgeBaseConfigList 获取应用的知识库配置
func (l *Logic) DescribeAppKnowledgeBaseConfigList(ctx context.Context,
	corpBizID uint64, appBizIDs []uint64, withShareKbInfo bool, releasePrimaryId uint64) ([]*kbEntity.KnowledgeConfig, error) {
	var knowledgeConfigList []*kbEntity.KnowledgeConfig
	var err error

	// 当 releasePrimaryId > 0 时，从历史表读取配置
	if releasePrimaryId > 0 {
		logx.I(ctx, "DescribeAppKnowledgeBaseConfigList read from history table, releasePrimaryId=%d", releasePrimaryId)
		knowledgeConfigList, err = l.getKnowledgeConfigFromHistory(ctx, corpBizID, appBizIDs, releasePrimaryId)
		if err != nil {
			logx.E(ctx, "DescribeAppKnowledgeBaseConfig getKnowledgeConfigFromHistory fail, err=%+v", err)
			return nil, err
		}
	} else {
		// 否则走原有逻辑，从当前配置表读取
		knowledgeConfigList, err = l.kbDao.DescribeAppKnowledgeConfigList(ctx, corpBizID, appBizIDs)
		if err != nil {
			logx.E(ctx, "DescribeAppKnowledgeBaseConfig dao.DescribeAppKnowledgeConfigList fail, err=%+v", err)
			return nil, err
		}
	}
	for _, knowledgeConfig := range knowledgeConfigList {
		if knowledgeConfig.Config != "" {
			knowledgeConfig.Config = l.formatModelConfigStr(ctx, knowledgeConfig.Type, knowledgeConfig.Config)
		}
		if knowledgeConfig.PreviewConfig != "" {
			knowledgeConfig.PreviewConfig = l.formatModelConfigStr(ctx, knowledgeConfig.Type, knowledgeConfig.PreviewConfig)
		}
	}
	if !withShareKbInfo {
		return knowledgeConfigList, nil
	}
	shareKbBizIDList := make([]uint64, 0)
	for _, knowledgeConfig := range knowledgeConfigList {
		if knowledgeConfig == nil {
			logx.E(ctx, "DescribeAppKnowledgeBaseConfig knowledgeConfig is nil")
			continue
		}
		if knowledgeConfig.KnowledgeBizID == knowledgeConfig.AppBizID {
			// 应用下默认知识库
			continue
		} else {
			// 应用下共享知识库
			logx.D(ctx, "Get shared knowledge biz id(%d) for app biz id(%d)", knowledgeConfig.KnowledgeBizID, knowledgeConfig.AppBizID)
			shareKbBizIDList = append(shareKbBizIDList, knowledgeConfig.KnowledgeBizID)
		}
	}
	if len(shareKbBizIDList) == 0 {
		return knowledgeConfigList, nil
	}
	shareKbBizIDList = slicex.Unique(shareKbBizIDList)
	// 获取共享知识库的名称
	shareKnowledgeFilter := kbEntity.ShareKnowledgeFilter{
		CorpBizID: corpBizID,
		BizIds:    shareKbBizIDList,
	}
	sharedKnowledgeInfoList, err := l.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil && !errors.Is(err, errx.ErrNotFound) {
		logx.E(ctx, "DescribeAppKnowledgeBaseConfig dao.RetrieveBaseSharedKnowledge fail, err=%+v", err)
		return nil, err
	}
	knowledgeID2ShareKb := make(map[uint64]*kbEntity.SharedKnowledgeInfo)
	for _, sharedKnowledgeInfo := range sharedKnowledgeInfoList {
		knowledgeID2ShareKb[sharedKnowledgeInfo.BusinessID] = sharedKnowledgeInfo
	}
	var resolvedKbConfigList []*kbEntity.KnowledgeConfig // 含共享知识库配置的最终结果
	for _, knowledgeConfig := range knowledgeConfigList {
		if knowledgeConfig.KnowledgeBizID != knowledgeConfig.AppBizID {
			// 应用下共享知识库补齐知识库名称
			if sharedKnowledgeInfo, ok := knowledgeID2ShareKb[knowledgeConfig.KnowledgeBizID]; ok {
				knowledgeConfig.ShareKbName = sharedKnowledgeInfo.Name
				resolvedKbConfigList = append(resolvedKbConfigList, knowledgeConfig)
			} else {
				logx.W(ctx, "KnowledgeBizID(%d) not found in knowledgeID2ShareKb", knowledgeConfig.KnowledgeBizID)
			}
		} else {
			// 应用下默认知识库配置
			resolvedKbConfigList = append(resolvedKbConfigList, knowledgeConfig)
		}
	}
	// 获取应用下引用共享知识库除检索配置外的其他配置
	sharedKbConfigTypes := []uint32{
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
		uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL),
		uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
		uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL),
	}
	sharedKbConfigs, err := l.GetRawShareKnowledgeBaseConfigs(ctx, corpBizID, shareKbBizIDList, sharedKbConfigTypes)
	if err != nil {
		logx.E(ctx, "DescribeAppKnowledgeBaseConfig GetShareKBModelNames fail, err=%+v", err)
		return nil, err
	}
	resolvedKbConfigList = append(resolvedKbConfigList, sharedKbConfigs...)
	return resolvedKbConfigList, nil
}

// getKnowledgeConfigFromHistory 从历史表读取知识库配置
func (l *Logic) getKnowledgeConfigFromHistory(ctx context.Context, corpBizID uint64, appBizIDs []uint64, versionID uint64) ([]*kbEntity.KnowledgeConfig, error) {
	knowledgeConfigList := make([]*kbEntity.KnowledgeConfig, 0)

	// 遍历每个应用ID，查询对应的历史配置
	for _, appBizID := range appBizIDs {
		filter := &kbEntity.KnowledgeConfigHistoryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			VersionID: versionID,
		}

		historyList, err := l.kbDao.DescribeKnowledgeConfigHistoryList(ctx, filter)
		if err != nil {
			logx.E(ctx, "getKnowledgeConfigFromHistory DescribeKnowledgeConfigHistoryList fail, appBizID=%d, versionID=%d, err=%+v",
				appBizID, versionID, err)
			return nil, err
		}

		// 将历史配置转换为知识库配置格式
		for _, history := range historyList {
			config := &kbEntity.KnowledgeConfig{
				ID:             history.ID,
				CorpBizID:      history.CorpBizID,
				KnowledgeBizID: history.KnowledgeBizID,
				AppBizID:       history.AppBizID,
				Type:           history.Type,
				Config:         history.ReleaseJSON, // 历史表的 ReleaseJSON 对应当前表的 Config
				PreviewConfig:  "",                  // 历史配置没有预览配置
				IsDeleted:      history.IsDeleted,
				CreateTime:     history.CreateTime,
				UpdateTime:     history.UpdateTime,
			}
			knowledgeConfigList = append(knowledgeConfigList, config)
		}
	}

	logx.I(ctx, "getKnowledgeConfigFromHistory success, corpBizID=%d, appBizIDs=%+v, versionID=%d, configCount=%d",
		corpBizID, appBizIDs, versionID, len(knowledgeConfigList))

	return knowledgeConfigList, nil
}

// CheckShareKG 检查应用还原后共享知识库是否存在
func (l *Logic) CheckShareKG(ctx context.Context, appBizId uint64) ([]*appCommon.AppConfigException, error) {
	res := make([]*appCommon.AppConfigException, 0)
	// 1. 获取应用关联的共享知识库列表
	shareKbList, err := l.kbDao.GetAppShareKGList(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "CheckAppConfig GetAppShareKGList failed, appBizId: %d, err: %v", appBizId, err)
		return res, err
	}
	if len(shareKbList) == 0 {
		logx.I(ctx, "CheckAppConfig no shared knowledge found for appBizId: %d", appBizId)
		return res, nil
	}

	// 2. 获取共享知识库的BizID列表
	shareKbBizIds := slicex.Pluck(shareKbList, func(item *kbEntity.AppShareKnowledge) uint64 { return item.KnowledgeBizID })

	// 3. 查询这些共享知识库是否存在
	shareKnowledgeFilter := kbEntity.ShareKnowledgeFilter{
		CorpBizID:   contextx.Metadata(ctx).CorpBizID(),
		BizIds:      shareKbBizIds,
		WithDeleted: ptrx.Bool(true),
	}
	existingKbList, err := l.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil && !errx.IsNotFound(err) {
		logx.E(ctx, "CheckAppConfig RetrieveBaseSharedKnowledge failed, appBizId: %d, err: %v", appBizId, err)
		return res, err
	}

	// 4. 构建存在的知识库ID集合
	existingKbMap := slicex.MapKV(existingKbList, func(v *kbEntity.SharedKnowledgeInfo) (uint64, *kbEntity.SharedKnowledgeInfo) { return v.BusinessID, v })

	// 5. 检查哪些共享知识库不存在，加入异常信息
	for _, kg := range shareKbList {
		v, ok := existingKbMap[kg.KnowledgeBizID]
		if !ok {
			res = append(res, &appCommon.AppConfigException{
				ResourceId:   fmt.Sprintf("%d", kg.KnowledgeBizID),
				ResourceName: "", // 知识库不存在，无法获取名称
				ResourceType: appCommon.ComponentType_KB,
				Exception:    i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseNotFound),
			})
			continue
		}
		if v.IsDeleted {
			res = append(res, &appCommon.AppConfigException{
				ResourceId:   fmt.Sprintf("%d", kg.KnowledgeBizID),
				ResourceName: v.Name,
				ResourceType: appCommon.ComponentType_KB,
				Exception:    i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseDeleted),
			})
		}
	}
	return res, nil
}

// CheckKGModel 检查应用还原后模型是否存在
func (l *Logic) CheckKGModel(ctx context.Context, appBizId uint64) ([]*appCommon.AppConfigException, error) {
	res := make([]*appCommon.AppConfigException, 0)
	// 1.获取知识库配置，用于检查模型是否存在
	knowledgeConfigList, err := l.DescribeAppKnowledgeBaseConfigList(ctx, contextx.Metadata(ctx).CorpBizID(), []uint64{appBizId}, false, 0)
	if err != nil {
		logx.E(ctx, "CheckAppConfig DescribeAppKnowledgeBaseConfigList failed, appBizId: %d, err: %v", appBizId, err)
		return res, err
	}

	// 2.获取app和corp信息
	app, err := l.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "CheckAppConfig DescribeAppById failed, appBizId: %d, err: %v", appBizId, err)
		return res, err
	}
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, contextx.Metadata(ctx).CorpBizID())
	if err != nil {
		logx.E(ctx, "CheckAppConfig DescribeCorpByBizId failed, appBizId: %d, err: %v", appBizId, err)
		return res, err
	}
	// 3.获取模型信息
	hasModels, err := l.rpc.Resource.ListCorpModel(ctx, contextx.Metadata(ctx).CorpBizID(), app.AppType, app.SpaceId)
	if err != nil {
		logx.E(ctx, "CheckAppConfig ListCorpModel failed, appBizId: %d, err: %v", appBizId, err)
		return res, err
	}
	logx.D(ctx, "CheckAppConfig hasModels: %+v", hasModels)

	modelMappings, err := l.rpc.Resource.GetAllModelMapping(ctx, corp.Uin)
	if err != nil {
		logx.E(ctx, "App.CheckAppConfig GetModelMapping failed, appBizID:%d, err:%v", appBizId, err)
		return res, err
	}
	logx.D(ctx, "CheckAppConfig modelMappings: %+v", modelMappings)

	// 4.获取知识库配置中所有模型相关的名称
	var modelNames []string
	modelName2AliasName := make(map[string]string)
	for _, knowledgeConfig := range knowledgeConfigList {
		if knowledgeConfig.Type == uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL) {
			embeddingModel, err := l.ConvertStr2EmbeddingModelConfigItem(ctx, knowledgeConfig.PreviewConfig, true)
			if err != nil {
				logx.E(ctx, "CheckAppConfig ConvertStr2EmbeddingModelConfigItem failed, appBizId: %d, err: %v", appBizId, err)
				continue
			}
			if embeddingModel.GetModelName() != "" {
				logx.D(ctx, "CheckAppConfig embeddingModel.GetModelName(): %s", embeddingModel.GetModelName())
				modelNames = append(modelNames, embeddingModel.GetModelName())
				modelName2AliasName[embeddingModel.GetModelName()] = embeddingModel.GetAliasName()
			}
		} else if knowledgeConfig.Type == uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL) {
			qaExtractModel, err := l.ConvertStr2QAExtractModelConfigItem(ctx, knowledgeConfig.PreviewConfig, true)
			if err != nil {
				logx.E(ctx, "CheckAppConfig ConvertStr2QAExtractModelConfigItem failed, appBizId: %d, err: %v", appBizId, err)
				continue
			}
			if qaExtractModel.GetModelName() != "" {
				logx.D(ctx, "CheckAppConfig qaExtractModel.GetModelName(): %s", qaExtractModel.GetModelName())
				modelNames = append(modelNames, qaExtractModel.GetModelName())
				modelName2AliasName[qaExtractModel.GetModelName()] = qaExtractModel.GetAliasName()
			}
		} else if knowledgeConfig.Type == uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL) {
			knowledgeSchemaModel, err := l.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, knowledgeConfig.PreviewConfig, true)
			if err != nil {
				logx.E(ctx, "CheckAppConfig ConvertStr2KnowledgeSchemaModelConfigItem failed, appBizId: %d, err: %v", appBizId, err)
				continue
			}
			if knowledgeSchemaModel.GetModelName() != "" {
				logx.D(ctx, "CheckAppConfig knowledgeSchemaModel.GetModelName(): %s", knowledgeSchemaModel.GetModelName())
				modelNames = append(modelNames, knowledgeSchemaModel.GetModelName())
				modelName2AliasName[knowledgeSchemaModel.GetModelName()] = knowledgeSchemaModel.GetAliasName()
			}
		} else if knowledgeConfig.Type == uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING) {
			retrievalConfig := &pb.RetrievalConfig{}
			if knowledgeConfig.PreviewConfig != "" {
				err = json.Unmarshal([]byte(knowledgeConfig.PreviewConfig), retrievalConfig)
				if err != nil {
					logx.E(ctx, "CheckAppConfig json.Unmarshal fail, err=%+v", err)
					continue
				}
				nl2SqlModelName := retrievalConfig.GetSearchStrategy().GetNatureLanguageToSqlModelConfig().GetModel().GetModelName()
				if nl2SqlModelName != "" {
					logx.D(ctx, "CheckAppConfig nl2SqlModelName: %s", nl2SqlModelName)
					modelNames = append(modelNames, nl2SqlModelName)
					modelName2AliasName[nl2SqlModelName] = retrievalConfig.GetSearchStrategy().GetNatureLanguageToSqlModelConfig().GetModel().GetAliasName()
				}
				rerankModelName := retrievalConfig.GetSearchStrategy().GetRerankModel()
				if rerankModelName != "" {
					logx.D(ctx, "CheckAppConfig rerankModelName: %s", rerankModelName)
					modelNames = append(modelNames, rerankModelName)
					modelName2AliasName[rerankModelName] = rerankModelName
				}
			}
		} else if knowledgeConfig.Type == uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL) {
			fileParseModel, err := l.ConvertStr2FileParseModelConfigItem(ctx, knowledgeConfig.PreviewConfig, true)
			if err != nil {
				logx.E(ctx, "CheckAppConfig ConvertStr2FileParseModelConfigItem failed, appBizId: %d, err: %v", appBizId, err)
				continue
			}
			if fileParseModel.GetModelName() != "" {
				logx.D(ctx, "CheckAppConfig fileParseModel.GetModelName(): %s", fileParseModel.GetModelName())
				modelNames = append(modelNames, fileParseModel.GetModelName())
				modelName2AliasName[fileParseModel.GetModelName()] = fileParseModel.GetAliasName()
			}
		}

	}

	// 5.对比模型名称和模型信息，检查模型是否存在
	dump := make(map[string]struct{})
	for _, v := range modelNames {
		modelName := v
		if _, ok := dump[modelName]; ok {
			continue
		}
		dump[modelName] = struct{}{}
		if mappedModelName, ok := modelMappings[modelName]; ok {
			modelName = mappedModelName
		}
		if _, ok := hasModels[modelName]; !ok {
			res = append(res, &appCommon.AppConfigException{
				ResourceId:   modelName,
				ResourceName: modelName2AliasName[modelName],
				ResourceType: appCommon.ComponentType_KB,
				Exception:    i18n.Translate(ctx, i18nkey.KeyModelNotFound),
			})
		}
	}
	return res, nil
}

// DescribeAppKnowledgeBaseConfigList 获取应用的知识库配置
func (l *Logic) DescribeFileParseModelByAppBaseInfo(ctx context.Context, corpId uint64, appBaseInfo *entity.AppBaseInfo) (*common.FileParseModel, error) {
	if appBaseInfo == nil {
		return nil, errs.ErrRobotNotFound
	}
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpId)
	if err != nil {
		return nil, err
	}
	corpBizId := corp.GetCorpId()
	configStr := ""
	if appBaseInfo.IsShared {
		knowledgeBaseConfigs, err := l.GetRawShareKnowledgeBaseConfigs(ctx, corpBizId, []uint64{appBaseInfo.BizId}, []uint32{uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL)})
		if err != nil {
			return nil, err
		}
		if len(knowledgeBaseConfigs) > 0 {
			configStr = knowledgeBaseConfigs[0].Config
		}
	} else {
		knowledgeBaseConfigs, err := l.DescribeAppKnowledgeBaseConfigList(ctx, corpBizId, []uint64{appBaseInfo.BizId}, false, 0)
		if err != nil {
			return nil, err
		}
		for _, knowledgeBaseConfig := range knowledgeBaseConfigs {
			if knowledgeBaseConfig.Type == uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL) {
				configStr = knowledgeBaseConfig.PreviewConfig
				break
			}
		}
	}
	return l.ConvertStr2FileParseModelConfigItem(ctx, configStr, true)
}
