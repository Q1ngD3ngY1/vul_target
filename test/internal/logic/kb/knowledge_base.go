package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"git.code.oa.com/trpc-go/trpc-database/localcache"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"git.woa.com/adp/pb-go/resource_gallery/resource_gallery"

	apppb "git.woa.com/adp/pb-go/app/app_config"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

const (
	// NextActionAdd 新增
	NextActionAdd = uint32(1)
	// NextActionUpdate 更新
	NextActionUpdate = uint32(2)
	// NextActionDelete 删除
	NextActionDelete = uint32(3)
	// NextActionPublish 发布
	NextActionPublish = uint32(4)

	expiration = 10 // 10 sec
	capacity   = 10000
)

var (
	modelType2DefaultConfigCache localcache.Cache
	modelName2ModelInfoCache     localcache.Cache
	modelNameMappingCache        localcache.Cache // 模型名映射缓存
)

func init() {
	modelType2DefaultConfigCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
	modelName2ModelInfoCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
	modelNameMappingCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
}

func (l *Logic) GetModelAssociatedApps(ctx context.Context, corpBizID uint64,
	modelKeyword, spaceID string) ([]*pb.KnowledgeBaseInfo, error) {
	knowledgeBaseConfigs, err := l.kbDao.GetKnowledgeConfigsByModelAssociated(ctx, corpBizID, modelKeyword)
	if err != nil {
		logx.E(ctx, "GetModelAssociatedApps dao.GetKnowledgeConfigsByModelAssociated fail, err=%+v", err)
		return nil, err
	}
	if len(knowledgeBaseConfigs) == 0 {
		return nil, nil
	}
	knowledgeBizIDList := make([]uint64, 0, len(knowledgeBaseConfigs))
	for _, knowledgeBaseConfig := range knowledgeBaseConfigs {
		knowledgeBizIDList = append(knowledgeBizIDList, knowledgeBaseConfig.KnowledgeBizID)
	}
	shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
		CorpBizID: corpBizID,
		BizIds:    knowledgeBizIDList,
	}
	knowledgeBaseInfoList, err := l.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil {
		if errors.Is(err, errx.ErrNotFound) {
			return nil, nil
		}
		logx.E(ctx, "GetModelAssociatedApps dao.RetrieveBaseSharedKnowledge fail, err=%+v", err)
		return nil, err
	}

	result := make([]*pb.KnowledgeBaseInfo, 0, len(knowledgeBaseInfoList))
	for idx := range knowledgeBaseInfoList {
		if spaceID != "" && knowledgeBaseInfoList[idx].SpaceId != spaceID {
			continue
		}
		baseInfo, _ := ConvertSharedKnowledgeBaseInfo(ctx, knowledgeBaseInfoList[idx])
		result = append(result, baseInfo)
	}

	return result, nil
}

// KnowledgeDeleteResultCallback 知识删除任务结果回调
func (l *Logic) KnowledgeDeleteResultCallback(ctx context.Context, taskID uint64, isSuccess bool, message string) error {
	logx.I(ctx, "KnowledgeDeleteResultCallback taskID:%d, isSuccess:%v, message:%s", taskID, isSuccess, message)
	req := &apppb.ClearAppResourceCallbackReq{
		TaskId:       taskID,
		IsSuccess:    isSuccess,
		Message:      message,
		ResourceType: apppb.ClearAppResourceType_ClearAppResourceTypeKnowledge,
	}
	logx.D(ctx, "KnowledgeDeleteResultCallback ClearAppKnowledgeCallback req:%+v", req)
	rsp, err := l.rpc.AppAdmin.ClearAppResourceCallback(ctx, req)
	if err != nil {
		logx.E(ctx, "KnowledgeDeleteResultCallback ClearAppKnowledgeCallback Failed, err:%+v", err)
		return err
	}
	logx.D(ctx, "KnowledgeDeleteResultCallback ClearAppKnowledgeCallback rsp:%+v", rsp)
	return nil
}

func (l *Logic) GetSpaceShareKnowledgeListExSelf(ctx context.Context, corpBizID, exStaffID uint64, spaceID,
	keyword string, pageNumber, pageSize uint32) (int64, []*pb.KnowledgeBaseInfo, error) {
	total, list, err := l.kbDao.ListSpaceShareKnowledgeExSelf(ctx, corpBizID, exStaffID, spaceID, keyword, pageNumber, pageSize)
	if err != nil {
		logx.E(ctx, "GetSpaceShareKnowledgeListExSelf ListSpaceShareKnowledgeExSelf fail, err=%+v", err)
		return 0, nil, err
	}
	result := make([]*pb.KnowledgeBaseInfo, 0, len(list))
	for idx := range list {
		baseInfo, _ := ConvertSharedKnowledgeBaseInfo(ctx, list[idx])
		result = append(result, baseInfo)
	}
	return total, result, nil
}

func (l *Logic) AppKnowledgeConfigRetrievalDetailDiff(ctx context.Context,
	corpBizID, appBizID uint64, spaceID string) ([]*kbe.KnowledgeConfigDiff, bool, error) {
	knowledgeConfigList, err := l.DescribeAppKnowledgeBaseConfigList(ctx, corpBizID, []uint64{appBizID}, true, 0)
	if err != nil {
		logx.E(ctx, "AppKnowledgeConfigDetailDiff GetAppKnowledgeConfig fail, err=%+v", err)
		return nil, false, err
	}
	// 柔性放过
	modelMapping, _ := l.rpc.Resource.GetModelMapping(ctx, nil)
	modelAliasNameMap, _ := l.rpc.Resource.GetModelAliasName(ctx, corpBizID, spaceID)
	referShareKBChanged := false // 应用下引用共享知识库关系是否发生了变更
	var diff []*kbe.KnowledgeConfigDiff
	for _, knowledgeConfig := range knowledgeConfigList {
		if knowledgeConfig.Type == uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL) {
			// 修改embedding 模型，因为影响到运行时，需要发布;
			if knowledgeConfig.AppBizID != knowledgeConfig.KnowledgeBizID {
				// 共享知识库，不比较向量化模型
				continue
			}
			diff = append(diff, l.equalEmbeddingModelConfigItem(ctx,
				knowledgeConfig.Config, knowledgeConfig.PreviewConfig, modelAliasNameMap, modelMapping)...)
		}
		if knowledgeConfig.Type == uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING) {
			previewJSON := knowledgeConfig.PreviewConfig
			releaseJSON := knowledgeConfig.Config
			if knowledgeConfig.AppBizID != knowledgeConfig.KnowledgeBizID &&
				knowledgeConfig.PreviewConfig == "" && knowledgeConfig.Config != "" {
				// 删除共享知识库(PreviewConfig为空)，不比较检索设置的比较，但需要标识应用下引用共享知识库关系发生了变更
				referShareKBChanged = true
				continue
			}
			previewConfig := &pb.RetrievalConfig{}
			if previewJSON != "" {
				err = json.Unmarshal([]byte(previewJSON), previewConfig)
				if err != nil {
					logx.E(ctx, "AppKnowledgeConfigDetailDiff json.Unmarshal fail, err=%+v", err)
					return nil, referShareKBChanged, err
				}
			}
			if previewConfig.GetSearchStrategy() != nil {
				l.MapSearchStrategyModels(ctx, previewConfig.GetSearchStrategy())
			}
			releaseConfig := &pb.RetrievalConfig{}
			if releaseJSON != "" {
				err = json.Unmarshal([]byte(releaseJSON), releaseConfig)
				if err != nil {
					logx.E(ctx, "AppKnowledgeConfigDetailDiff json.Unmarshal fail, err=%+v", err)
					return nil, referShareKBChanged, err
				}
			}
			if releaseConfig.GetSearchStrategy() != nil {
				l.MapSearchStrategyModels(ctx, releaseConfig.GetSearchStrategy())
			}
			logx.D(ctx, "AppKnowledgeConfigDetailDiff ShareKbName=%s, releaseConfig=%+v, previewConfig=%+v",
				knowledgeConfig.ShareKbName, releaseConfig, previewConfig)
			diff = append(diff, l.equalRetrievalRange(ctx, knowledgeConfig.ShareKbName, releaseConfig, previewConfig)...)
			diff = append(diff, l.equalRetrievalConfig(ctx, knowledgeConfig.ShareKbName, releaseConfig, previewConfig)...)
			diff = append(diff, l.equalSearchStrategy(ctx, knowledgeConfig.ShareKbName, releaseConfig.GetSearchStrategy(),
				previewConfig.GetSearchStrategy(), modelAliasNameMap, modelMapping)...)
			diff = append(diff, l.equalSearchStrategy(ctx, knowledgeConfig.ShareKbName, releaseConfig.GetSearchStrategy(), previewConfig.GetSearchStrategy(), modelAliasNameMap, modelMapping)...)
		}
	}
	return diff, referShareKBChanged, nil
}

// MapSearchStrategyModels 映射搜索策略模型
func (l *Logic) MapSearchStrategyModels(ctx context.Context, strategy *pb.SearchStrategy) {
	if strategy == nil {
		return
	}
	strategy.RerankModel = l.GetMappingModelName(ctx, strategy.GetRerankModel())
	strategy.EmbeddingModel = l.GetMappingModelName(ctx, strategy.GetEmbeddingModel())
	if model := strategy.GetNatureLanguageToSqlModelConfig().GetModel(); model != nil && model.GetModelName() != "" {
		model.ModelName = l.GetMappingModelName(ctx, model.GetModelName())
	}
}

func newDiffConfig(ctx context.Context, configItem string, lastValue string, value string,
	content string, actions ...uint32) *kbe.KnowledgeConfigDiff {
	action := NextActionUpdate
	if len(actions) > 0 {
		action = actions[0]
	}
	return &kbe.KnowledgeConfigDiff{
		ConfigItem: i18n.Translate(ctx, configItem),
		Action:     action,
		NewValue:   i18n.Translate(ctx, value),
		LastValue:  i18n.Translate(ctx, lastValue),
		Content:    i18n.Translate(ctx, content),
	}
}

// searchRangeConditionMap 知识检索范围条件描述映射表
var searchRangeConditionMap = map[string]string{
	"":    i18nkey.KeyNotSet,
	"OR":  i18nkey.KeyOr,
	"or":  i18nkey.KeyOr,
	"and": i18nkey.KeyAnd,
	"AND": i18nkey.KeyAnd,
}

// GetSearchRangeConditionDesc 获取知识检索范围条件描述
func GetSearchRangeConditionDesc(ctx context.Context, condition string) string {
	desc := searchRangeConditionMap[condition]
	return i18n.Translate(ctx, desc)
}

func (l *Logic) equalRetrievalRange(ctx context.Context, knowledgeName string, release, preview *pb.RetrievalConfig) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	content := diffRetrievalRangeConfigContent(ctx, release.GetRetrievalRange(), preview.GetRetrievalRange())
	if len(content) == 0 {
		return diff
	}
	if knowledgeName != "" {
		diff = append(diff, newDiffConfig(ctx, i18nkey.KeySharedKnowledgeBaseRetrievalRangeSetting,
			"", "", fmt.Sprintf("%s %s", knowledgeName, content)))
	} else {
		diff = append(diff, newDiffConfig(ctx, i18nkey.KeyDefaultKnowledgeBaseRetrievalRangeSetting,
			"", "", content))
	}
	return diff
}

func diffRetrievalRangeConfigContent(ctx context.Context, release, preview *pb.RetrievalRange) string {
	items := make([]string, 0)
	if release.GetCondition() != preview.GetCondition() {
		items = append(items, i18n.Translate(ctx, i18nkey.KeyAPISelectParamChanged,
			GetSearchRangeConditionDesc(ctx, release.GetCondition()), GetSearchRangeConditionDesc(ctx, preview.GetCondition())))
	}
	if !isEqualSearchRange(release, preview) {
		items = append(items, i18n.Translate(ctx, i18nkey.KeyAPIParamOrTagChanged))
	}
	return strings.Join(items, "\n")
}

func isEqualSearchRange(release, preview *pb.RetrievalRange) bool {
	if len(preview.GetApiVarAttrInfos()) != len(release.GetApiVarAttrInfos()) {
		return false
	}
	srcMap := make(map[string]uint64)
	for _, v := range preview.GetApiVarAttrInfos() {
		srcMap[v.GetApiVarId()] = v.GetAttrBizId()
	}
	descMap := make(map[string]uint64)
	for _, v := range release.GetApiVarAttrInfos() {
		descMap[v.GetApiVarId()] = v.GetAttrBizId()
	}
	for _, v := range preview.GetApiVarAttrInfos() {
		if _, ok := descMap[v.GetApiVarId()]; !ok {
			return false
		}
		if srcMap[v.GetApiVarId()] != descMap[v.GetApiVarId()] {
			return false
		}
	}
	for _, v := range release.GetApiVarAttrInfos() {
		if _, ok := srcMap[v.GetApiVarId()]; !ok {
			return false
		}
		if srcMap[v.GetApiVarId()] != descMap[v.GetApiVarId()] {
			return false
		}
	}
	return true
}

// GetFiltersTypeNameDesc 获取检索类型描述信息
func GetFiltersTypeNameDesc(ctx context.Context, i uint32) string {
	desc := kbe.GetFiltersTypeName(i)
	return i18n.Translate(ctx, desc)
}

func createKnowledgeType2RetrievalMap(searchList []*pb.RetrievalInfo) map[uint32]*pb.RetrievalInfo {
	res := make(map[uint32]*pb.RetrievalInfo)
	for _, v := range searchList {
		res[uint32(v.GetRetrievalType())] = v
	}
	return res
}

func (l *Logic) equalRetrievalConfig(ctx context.Context, knowledgeName string, release, preview *pb.RetrievalConfig) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	if knowledgeName != "" {
		knowledgeName += "/"
	}
	knowledgeTypeList := []uint32{entity.QaFilterType, entity.DocFilterType, entity.DBFilterType}
	if len(release.GetRetrievals()) == 0 {
		for _, v := range preview.GetRetrievals() {
			if !slices.Contains(knowledgeTypeList, uint32(v.GetRetrievalType())) {
				continue
			}
			if v.GetIsEnable() {
				knowledgeType := uint32(v.GetRetrievalType())
				diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemIsEnabled, GetFiltersTypeNameDesc(ctx, knowledgeType)), kbe.ShutDown, kbe.Open, ""))
				if knowledgeType != kbe.TaskFlowFilterType && knowledgeType != kbe.SearchFilterType && knowledgeType != kbe.DBFilterType {
					diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemTopN, GetFiltersTypeNameDesc(ctx, knowledgeType)), strconv.Itoa(int(v.GetTopN())),
						strconv.Itoa(int(v.GetTopN())), ""))
					diff = append(diff,
						newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemConfidence, GetFiltersTypeNameDesc(ctx, knowledgeType)), fmt.Sprintf("%.2f", v.GetConfidence()),
							fmt.Sprintf("%.2f", v.GetConfidence()), ""))
				}
			}
		}
	} else {
		releaseMap := createKnowledgeType2RetrievalMap(release.GetRetrievals())
		previewMap := createKnowledgeType2RetrievalMap(preview.GetRetrievals())
		for _, knowledgeType := range knowledgeTypeList {
			oldRetrievalInfo, ok := releaseMap[knowledgeType]
			if !ok {
				continue
			}
			newRetrievalInfo, ok := previewMap[knowledgeType]
			if !ok {
				continue
			}
			logx.D(ctx, "knowledgeType[%d], oldRetrievalInfo=%+v, newRetrievalInfo=%+v", knowledgeType, oldRetrievalInfo, newRetrievalInfo)
			if oldRetrievalInfo.GetIsEnable() != newRetrievalInfo.GetIsEnable() {
				diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx,
					kbe.ConfigItemIsEnabled, GetFiltersTypeNameDesc(ctx, knowledgeType)),
					gox.IfElse(oldRetrievalInfo.GetIsEnable(), kbe.Open, kbe.ShutDown),
					gox.IfElse(newRetrievalInfo.GetIsEnable(), kbe.Open, kbe.ShutDown), ""))
			}
			if newRetrievalInfo.GetIsEnable() {
				if oldRetrievalInfo.GetTopN() != newRetrievalInfo.GetTopN() {
					diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemTopN,
						GetFiltersTypeNameDesc(ctx, knowledgeType)),
						strconv.Itoa(int(oldRetrievalInfo.GetTopN())), strconv.Itoa(int(newRetrievalInfo.GetTopN())), ""))
				}
				if oldRetrievalInfo.GetConfidence() != newRetrievalInfo.GetConfidence() {
					diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemConfidence,
						GetFiltersTypeNameDesc(ctx, knowledgeType)),
						fmt.Sprintf("%.2f", oldRetrievalInfo.GetConfidence()), fmt.Sprintf("%.2f", newRetrievalInfo.GetConfidence()), ""))
				}
			}
		}
	}
	return diff
}

// StrategyTypeDescMap 检索策略说明
var StrategyTypeDescMap = map[uint32]string{
	0: i18nkey.KeyHybridRetrieval,
	1: i18nkey.KeySemanticRetrieval,
}

// compareStrategyType 比较策略类型差异
func (l *Logic) compareStrategyType(ctx context.Context, knowledgeName string, release, preview *pb.SearchStrategy) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	if preview.GetStrategyType() != release.GetStrategyType() {
		lastConfig := StrategyTypeDescMap[uint32(release.GetStrategyType())]
		newConfig := StrategyTypeDescMap[uint32(preview.GetStrategyType())]
		diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemStrategyType), lastConfig, newConfig, ""))
	}
	return diff
}

// compareTableEnhancement 比较表格增强配置差异
func (l *Logic) compareTableEnhancement(ctx context.Context, knowledgeName string, release, preview *pb.SearchStrategy) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	if preview.GetTableEnhancement() != release.GetTableEnhancement() {
		lastConfig := gox.IfElse(release.GetTableEnhancement(), kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.GetTableEnhancement(), kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemTableEnhancement), lastConfig, newConfig, ""))
	}
	return diff
}

// compareRerankModelSwitch 比较重排模型开关差异
func (l *Logic) compareRerankModelSwitch(ctx context.Context, knowledgeName string, release, preview *pb.SearchStrategy) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	if preview.GetRerankModelSwitch() != release.GetRerankModelSwitch() {
		lastConfig := gox.IfElse(release.GetRerankModelSwitch() == "on", kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.GetRerankModelSwitch() == "on", kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemSearchStrategyReRankModelSwitch), lastConfig, newConfig, ""))
	}
	return diff
}

// compareRerankModelName 比较重排模型名称差异
func (l *Logic) compareRerankModelName(ctx context.Context, knowledgeName string, release, preview *pb.SearchStrategy,
	modelAliasNameMap, modelMapping map[string]string) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	releaseModelName := getModelMappingName(release.GetRerankModel(), modelMapping)
	previewModelName := getModelMappingName(preview.GetRerankModel(), modelMapping)
	if previewModelName != releaseModelName {
		if v, ok := modelAliasNameMap[releaseModelName]; ok {
			releaseModelName = v
		}
		value, ok := modelAliasNameMap[previewModelName]
		if !ok {
			value = previewModelName
		}
		diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigItemSearchStrategyReRankModelName),
			releaseModelName, value, ""))
	}
	return diff
}

// compareNL2SQLModel 比较NL2SQL模型配置差异
func (l *Logic) compareNL2SQLModel(ctx context.Context, knowledgeName string, release, preview *pb.SearchStrategy,
	modelAliasNameMap, modelMapping map[string]string) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff

	previewConfig := preview.GetNatureLanguageToSqlModelConfig()
	if previewConfig.GetModel().GetModelName() == "" {
		previewConfig = &pb.NL2SQLModelConfig{}
		previewConfig.Model, _ = l.GetDefaultNL2SQLModelConfigItem(ctx)
	}
	releaseConfig := release.GetNatureLanguageToSqlModelConfig()
	if releaseConfig.GetModel().GetModelName() == "" {
		releaseConfig = &pb.NL2SQLModelConfig{}
		releaseConfig.Model, _ = l.GetDefaultNL2SQLModelConfigItem(ctx)
	}
	logx.D(ctx, "releaseNL2SQLModelConfig=%+v, previewNL2SQLModelConfig=%+v", releaseConfig, previewConfig)

	releaseModelName := getModelMappingName(releaseConfig.GetModel().GetModelName(), modelMapping)
	previewModelName := getModelMappingName(previewConfig.GetModel().GetModelName(), modelMapping)
	if previewModelName != releaseModelName {
		if v, ok := modelAliasNameMap[releaseModelName]; ok {
			releaseModelName = v
		}
		value, ok := modelAliasNameMap[previewModelName]
		if !ok {
			value = previewModelName
		}
		diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigKnowledgeModelNL2SQLModel),
			releaseModelName, value, ""))
	}

	if previewConfig.GetModel().GetHistoryLimit() != releaseConfig.GetModel().GetHistoryLimit() {
		diff = append(diff, newDiffConfig(ctx, knowledgeName+i18n.Translate(ctx, kbe.ConfigKnowledgeModelNL2SQLModel)+" "+i18n.Translate(ctx, kbe.HistoryLimit),
			fmt.Sprintf("%d", releaseConfig.GetModel().GetHistoryLimit()),
			fmt.Sprintf("%d", previewConfig.GetModel().GetHistoryLimit()), ""))
	}

	diff = append(diff, l.equalModelParams(ctx, kbe.ConfigKnowledgeModelNL2SQLModel, releaseConfig.GetModel().GetModelParams(), previewConfig.GetModel().GetModelParams())...)
	return diff
}

func (l *Logic) equalSearchStrategy(ctx context.Context, knowledgeName string, release, preview *pb.SearchStrategy,
	modelAliasNameMap, modelMapping map[string]string) []*kbe.KnowledgeConfigDiff {
	if knowledgeName != "" {
		knowledgeName += "/"
	}

	var diff []*kbe.KnowledgeConfigDiff

	// 比较策略类型
	diff = append(diff, l.compareStrategyType(ctx, knowledgeName, release, preview)...)

	// 比较表格增强配置
	diff = append(diff, l.compareTableEnhancement(ctx, knowledgeName, release, preview)...)

	// 比较重排模型开关
	diff = append(diff, l.compareRerankModelSwitch(ctx, knowledgeName, release, preview)...)

	// 比较重排模型名称
	diff = append(diff, l.compareRerankModelName(ctx, knowledgeName, release, preview, modelAliasNameMap, modelMapping)...)

	// 比较NL2SQL模型配置
	diff = append(diff, l.compareNL2SQLModel(ctx, knowledgeName, release, preview, modelAliasNameMap, modelMapping)...)

	return diff
}

// formatModelConfigStr 格式化模型配置为含超参的标准行驶
func (l *Logic) formatModelConfigStr(ctx context.Context, configType uint32, str string) string {
	switch configType {
	case uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL):
		embeddingModel, err := l.ConvertStr2EmbeddingModelConfigItem(ctx, str, false)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr ConvertStr2EmbeddingModelConfigItem err: %+v", err)
			return ""
		}
		str, err = jsonx.MarshalToString(embeddingModel)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr jsonx.MarshalToString err: %+v", err)
			return ""
		}
	case uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL):
		qaExtractModel, err := l.ConvertStr2QAExtractModelConfigItem(ctx, str, false)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr ConvertStr2QAExtractModelConfigItem err: %+v", err)
			return ""
		}
		str, err = jsonx.MarshalToString(qaExtractModel)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr jsonx.MarshalToString err: %+v", err)
			return ""
		}
	case uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL):
		knowledgeSchemaModel, err := l.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, str, false)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr ConvertStr2KnowledgeSchemaModelConfigItem err: %+v", err)
			return ""
		}
		str, err = jsonx.MarshalToString(knowledgeSchemaModel)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr jsonx.MarshalToString err: %+v", err)
			return ""
		}
	case uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL):
		fileParseModel, err := l.ConvertStr2FileParseModelConfigItem(ctx, str, false)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr ConvertStr2FileParseModelConfigItem err: %+v", err)
			return ""
		}
		str, err = jsonx.MarshalToString(fileParseModel)
		if err != nil {
			logx.I(ctx, "formatModelConfigStr jsonx.MarshalToString err: %+v", err)
			return ""
		}
	}
	return str
}

func (l *Logic) ConvertStr2EmbeddingModelConfigItem(ctx context.Context, str string, needMappedModelName bool) (*pb.EmbeddingModel, error) {
	res := &pb.EmbeddingModel{}
	if str != "" && jsonx.Valid([]byte(str)) {
		err := jsonx.Unmarshal([]byte(str), res)
		if err != nil {
			logx.E(ctx, "ConvertStr2EmbeddingModelConfigItem jsonx.Unmarshal err: %+v", err)
			return nil, errs.ErrSystem
		}
	}
	if res.GetAliasName() == "" {
		// 配置中没有aliasName: case1.空字符串 case2.老的配置格式--只有一个模型名称
		if str == "" {
			// 如果是空字符串，直接获取默认的embedding模型配置
			value, exist := modelType2DefaultConfigCache.Get(entity.ModelCategoryEmbedding)
			if exist {
				rsp, ok := value.(*resource_gallery.GetDefaultModelConfigRsp)
				if !ok {
					logx.E(ctx, "get model(%s) info from local cache failed", entity.ModelCategoryEmbedding)
					exist = false
				} else {
					logx.D(ctx, "get model(%s) info from local cache: %+v", entity.ModelCategoryEmbedding, rsp)
					res.ModelName = rsp.GetModelName()
					res.AliasName = rsp.GetAliasName()
				}
			}
			if !exist {
				rsp, err := l.rpc.Resource.GetDefaultModelConfig(ctx, entity.ModelCategoryEmbedding)
				logx.D(ctx, "ConvertStr2EmbeddingModelConfigItem GetDefaultModelConfig rsp:%+v, err:%+v", rsp, err)
				if err != nil {
					logx.E(ctx, "ConvertStr2EmbeddingModelConfigItem GetDefaultModelConfig err: %+v", err)
					return nil, errs.ErrSystem
				} else {
					modelType2DefaultConfigCache.Set(entity.ModelCategoryEmbedding, rsp)
					res.ModelName = rsp.GetModelName()
					res.AliasName = rsp.GetAliasName()
				}
			}
		} else {
			if res.ModelName != "" {
				str = res.ModelName
			}
			// 如果不是空字符串，说明是老的配置格式--只有一个模型名称
			value, exist := modelName2ModelInfoCache.Get(str)
			if exist {
				providerModelInfo, ok := value.(*resource_gallery.ProviderModelInfo)
				if !ok {
					logx.E(ctx, "get model(%s) info from local cache failed", str)
					exist = false
				} else {
					logx.D(ctx, "get model(%s) info from local cache: %+v", str, providerModelInfo)
					res.ModelName = providerModelInfo.GetModelName()
					res.AliasName = providerModelInfo.GetModelAliasName()
				}
			}
			if !exist {
				mappedModelName := l.GetMappingModelName(ctx, str)
				rsp, err := l.rpc.Resource.GetModelInfoByModelName(ctx, []string{mappedModelName}) // 这里需要用映射出的模型名请求
				logx.D(ctx, "ConvertStr2EmbeddingModelConfigItem GetModelInfoByModelName rsp:%+v, err:%+v", rsp, err)
				if err != nil {
					logx.E(ctx, "ConvertStr2EmbeddingModelConfigItem GetModelInfoByModelName err: %+v", err)
					return nil, errs.ErrSystem
				} else if providerModelInfo, ok := rsp.GetModelInfo()[mappedModelName]; ok {
					modelName2ModelInfoCache.Set(str, providerModelInfo)
					res.ModelName = providerModelInfo.GetModelName()
					res.AliasName = providerModelInfo.GetModelAliasName()
				}
			}
		}
	}
	if needMappedModelName {
		res.ModelName = l.GetMappingModelName(ctx, res.ModelName)
	}
	return res, nil
}

func (l *Logic) ConvertStr2QAExtractModelConfigItem(ctx context.Context, str string, needMappedModelName bool) (*pb.QaExtractModel, error) {
	res := &pb.QaExtractModel{}
	if str != "" && jsonx.Valid([]byte(str)) {
		err := jsonx.Unmarshal([]byte(str), res)
		if err != nil {
			logx.E(ctx, "ConvertStr2QAExtractModelConfigItem jsonx.Unmarshal err: %+v", err)
			return nil, errs.ErrSystem
		}
	}
	if res.GetAliasName() == "" {
		// 配置中没有aliasName: case1.空字符串 case2.老的配置格式--只有一个模型名称
		if str == "" {
			// 如果是空字符串，直接获取默认的生成模型配置
			value, exist := modelType2DefaultConfigCache.Get(entity.ModelCategoryGenerate)
			if exist {
				rsp, ok := value.(*resource_gallery.GetDefaultModelConfigRsp)
				if !ok {
					logx.E(ctx, "get model(%s) info from local cache failed", entity.ModelCategoryGenerate)
					exist = false
				} else {
					logx.D(ctx, "get model(%s) info from local cache: %+v", entity.ModelCategoryGenerate, rsp)
					res.ModelName = rsp.GetModelName()
					res.AliasName = rsp.GetAliasName()
				}
			}
			if !exist {
				rsp, err := l.rpc.Resource.GetDefaultModelConfig(ctx, entity.ModelCategoryGenerate)
				logx.D(ctx, "ConvertStr2QAExtractModelConfigItem GetDefaultModelConfig rsp:%+v, err:%+v", rsp, err)
				if err != nil {
					logx.E(ctx, "ConvertStr2KnowledgeSchemaModelConfigItem GetModelInfoByModelName err: %+v", err)
					return nil, errs.ErrSystem
				} else {
					modelType2DefaultConfigCache.Set(entity.ModelCategoryGenerate, rsp)
					res.ModelName = rsp.GetModelName()
					res.AliasName = rsp.GetAliasName()
					res.ModelParams = entity.GetModelParamsFromRules(ctx, rsp.ModelParams)
				}
			}
		} else {
			if res.ModelName != "" {
				str = res.ModelName
			}
			// 如果不是空字符串，说明是老的配置格式--只有一个模型名称
			value, exist := modelName2ModelInfoCache.Get(str)
			if exist {
				providerModelInfo, ok := value.(*resource_gallery.ProviderModelInfo)
				if !ok {
					logx.E(ctx, "get model(%s) info from local cache failed", str)
					exist = false
				} else {
					logx.D(ctx, "get model(%s) info from local cache: %+v", str, providerModelInfo)
					res.ModelName = providerModelInfo.GetModelName()
					res.ModelParams = entity.GetModelParamsFromStr(ctx, providerModelInfo.GetModelParams())
					res.AliasName = providerModelInfo.GetModelAliasName()
				}
			}
			if !exist {
				mappedModelName := l.GetMappingModelName(ctx, str)
				rsp, err := l.rpc.Resource.GetModelInfoByModelName(ctx, []string{mappedModelName}) // 这里需要用映射出的模型名请求
				logx.D(ctx, "ConvertStr2QAExtractModelConfigItem GetModelInfoByModelName rsp:%+v, err:%+v", rsp, err)
				if err != nil {
					logx.E(ctx, "ConvertStr2QAExtractModelConfigItem GetModelInfoByModelName err: %+v", err)
					return nil, errs.ErrSystem
				} else if providerModelInfo, ok := rsp.GetModelInfo()[mappedModelName]; ok {
					modelName2ModelInfoCache.Set(str, providerModelInfo)
					res.ModelName = providerModelInfo.GetModelName()
					res.ModelParams = entity.GetModelParamsFromStr(ctx, providerModelInfo.GetModelParams())
					res.AliasName = providerModelInfo.GetModelAliasName()
				}
			}
		}
	}
	if needMappedModelName {
		res.ModelName = l.GetMappingModelName(ctx, res.ModelName)
	}
	return res, nil
}

func (l *Logic) ConvertStr2KnowledgeSchemaModelConfigItem(ctx context.Context, str string, needMappedModelName bool) (*pb.KnowledgeSchemaModel, error) {
	res := &pb.KnowledgeSchemaModel{}
	if str != "" && jsonx.Valid([]byte(str)) {
		err := jsonx.Unmarshal([]byte(str), res)
		if err != nil {
			logx.E(ctx, "ConvertStr2QAExtractModelConfigItem jsonx.Unmarshal err: %+v", err)
			return nil, errs.ErrSystem
		}
	}
	if res.GetAliasName() == "" {
		// 配置中没有aliasName: case1.空字符串 case2.老的配置格式--只有一个模型名称
		if str == "" {
			// 如果是空字符串，直接获取默认的生成模型配置
			value, exist := modelType2DefaultConfigCache.Get(entity.ModelCategoryGenerate)
			if exist {
				rsp, ok := value.(*resource_gallery.GetDefaultModelConfigRsp)
				if !ok {
					logx.E(ctx, "get model(%s) info from local cache failed", entity.ModelCategoryGenerate)
					exist = false
				} else {
					logx.D(ctx, "get model(%s) info from local cache: %+v", entity.ModelCategoryGenerate, rsp)
					res.ModelName = rsp.GetModelName()
					res.AliasName = rsp.GetAliasName()
				}
			}
			if !exist {
				rsp, err := l.rpc.Resource.GetDefaultModelConfig(ctx, entity.ModelCategoryGenerate)
				logx.D(ctx, "ConvertStr2QAExtractModelConfigItem GetDefaultModelConfig rsp:%+v, err:%+v", rsp, err)
				if err != nil {
					logx.E(ctx, "ConvertStr2KnowledgeSchemaModelConfigItem GetModelInfoByModelName err: %+v", err)
					return nil, errs.ErrSystem
				} else {
					modelType2DefaultConfigCache.Set(entity.ModelCategoryGenerate, rsp)
					res.ModelName = rsp.GetModelName()
					res.AliasName = rsp.GetAliasName()
					res.ModelParams = entity.GetModelParamsFromRules(ctx, rsp.ModelParams)
				}
			}
		} else {
			if res.ModelName != "" {
				str = res.ModelName
			}
			// 如果不是空字符串，说明是老的配置格式--只有一个模型名称，需要获取模型的超参
			value, exist := modelName2ModelInfoCache.Get(str)
			if exist {
				providerModelInfo, ok := value.(*resource_gallery.ProviderModelInfo)
				if !ok {
					logx.E(ctx, "get model(%s) info from local cache failed", str)
					exist = false
				} else {
					logx.D(ctx, "get model(%s) info from local cache: %+v", str, providerModelInfo)
					res.ModelName = providerModelInfo.GetModelName()
					res.ModelParams = entity.GetModelParamsFromStr(ctx, providerModelInfo.GetModelParams())
					res.AliasName = providerModelInfo.GetModelAliasName()
				}
			}
			if !exist {
				mappedModelName := l.GetMappingModelName(ctx, str)
				rsp, err := l.rpc.Resource.GetModelInfoByModelName(ctx, []string{mappedModelName}) // 这里需要用映射出的模型名请求
				logx.D(ctx, "ConvertStr2KnowledgeSchemaModelConfigItem GetModelInfoByModelName rsp:%+v, err:%+v", rsp, err)
				if err != nil {
					logx.E(ctx, "ConvertStr2KnowledgeSchemaModelConfigItem GetModelInfoByModelName err: %+v", err)
					return nil, errs.ErrSystem
				} else if providerModelInfo, ok := rsp.GetModelInfo()[mappedModelName]; ok {
					modelName2ModelInfoCache.Set(str, providerModelInfo)
					res.ModelName = providerModelInfo.GetModelName()
					res.ModelParams = entity.GetModelParamsFromStr(ctx, providerModelInfo.GetModelParams())
					res.AliasName = providerModelInfo.GetModelAliasName()
				}
			}
		}
	}
	if needMappedModelName {
		res.ModelName = l.GetMappingModelName(ctx, res.ModelName)
	}
	return res, nil
}

func (l *Logic) ConvertStr2FileParseModelConfigItem(ctx context.Context, str string, needMappedModelName bool) (*common.FileParseModel, error) {
	logx.D(ctx, "---------------ConvertStr2FileParseModelConfigItem str:%s", str)
	res := &common.FileParseModel{}
	if str != "" && jsonx.Valid([]byte(str)) {
		err := jsonx.Unmarshal([]byte(str), res)
		if err != nil {
			logx.E(ctx, "ConvertStr2FileParseModelConfigItem jsonx.Unmarshal err: %+v", err)
			return nil, errs.ErrSystem
		}
	}
	if res.GetModelName() == "" {
		// 如果为空，直接获取默认的生成模型配置
		value, exist := modelType2DefaultConfigCache.Get(entity.ModelCategoryFileParse)
		if exist {
			rsp, ok := value.(*resource_gallery.GetDefaultModelConfigRsp)
			if !ok {
				logx.E(ctx, "get model(%s) info from local cache failed", entity.ModelCategoryFileParse)
				exist = false
			} else {
				logx.D(ctx, "get model(%s) info from local cache: %+v", entity.ModelCategoryFileParse, rsp)
				res.ModelName = rsp.GetModelName()
				res.AliasName = rsp.GetAliasName()
				res.ModelId = rsp.GetModelName()
			}
		}
		if !exist {
			rsp, err := l.rpc.Resource.GetDefaultModelConfig(ctx, entity.ModelCategoryFileParse)
			logx.D(ctx, "ConvertStr2FileParseModelConfigItem GetDefaultModelConfig rsp:%+v, err:%+v", rsp, err)
			if err != nil {
				logx.E(ctx, "ConvertStr2FileParseModelConfigItem GetModelInfoByModelName err: %+v", err)
				return nil, errs.ErrSystem
			} else {
				modelType2DefaultConfigCache.Set(entity.ModelCategoryFileParse, rsp)
				res.ModelName = rsp.GetModelName()
				res.AliasName = rsp.GetAliasName()
				res.ModelId = rsp.GetModelName()
			}
		}
	}
	res.ModelName = l.GetMappingModelName(ctx, res.ModelName)
	res.Desc, res.ModelProviderType = l.GetModelDesc(ctx, pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL, res.ModelName)
	return res, nil
}

func (l *Logic) GetDefaultNL2SQLModelConfigItem(ctx context.Context) (*common.AppModelDetailInfo, error) {
	res := &common.AppModelDetailInfo{
		ModelName: entity.DefaultNL2SQLModel,
	}
	value, exist := modelName2ModelInfoCache.Get(entity.DefaultNL2SQLModel)
	if exist {
		providerModelInfo, ok := value.(*resource_gallery.ProviderModelInfo)
		if !ok {
			logx.E(ctx, "get model(%s) info from local cache failed", entity.DefaultNL2SQLModel)
			exist = false
		} else {
			logx.D(ctx, "get model(%s) info from local cache: %+v", entity.DefaultNL2SQLModel, providerModelInfo)
			res.ModelParams = entity.GetModelParamsFromStr(ctx, providerModelInfo.GetModelParams())
			res.AliasName = providerModelInfo.GetModelAliasName()
		}
	}
	if !exist {
		rsp, err := l.rpc.Resource.GetModelInfoByModelName(ctx, []string{entity.DefaultNL2SQLModel})
		logx.D(ctx, "GetDefaultNL2SQLModelConfigItem GetModelInfoByModelName:%s, rsp:%+v, err:%+v", entity.DefaultNL2SQLModel, rsp, err)
		if err != nil {
			logx.E(ctx, "GetDefaultNL2SQLModelConfigItem GetModelInfoByModelName:%s, err:%+v", entity.DefaultNL2SQLModel, err)
			return nil, err
		} else if providerModelInfo, ok := rsp.GetModelInfo()[entity.DefaultNL2SQLModel]; ok {
			modelName2ModelInfoCache.Set(entity.DefaultNL2SQLModel, providerModelInfo)
			res.ModelParams = entity.GetModelParamsFromStr(ctx, providerModelInfo.GetModelParams())
			res.AliasName = providerModelInfo.GetModelAliasName()
		}
	}
	// deepseek-v3-0324模型在模型解耦text2sql默认参数调整成top_p=0，temperature=1，presence_penalty=1
	res.ModelParams.TopP = ptrx.Float32(0)
	res.ModelParams.Temperature = ptrx.Float32(1)
	res.ModelParams.PresencePenalty = ptrx.Float32(1)
	res.ModelName = l.GetMappingModelName(ctx, res.ModelName)
	return res, nil
}

func (l *Logic) equalEmbeddingModelConfigItem(ctx context.Context, release, preview string,
	modelAliasNameMap, modelMapping map[string]string) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	releaseItem, err := l.ConvertStr2EmbeddingModelConfigItem(ctx, release, true)
	if err != nil {
		logx.E(ctx, "releaseItem ConvertStr2KBModelConfigItem err: %+v", err)
		return nil
	}
	previewItem, err := l.ConvertStr2EmbeddingModelConfigItem(ctx, preview, true)
	if err != nil {
		logx.E(ctx, "previewItem ConvertStr2KBModelConfigItem err: %+v", err)
		return nil
	}
	logx.D(ctx, "equalKBModelConfigItem releaseItem = %+v, previewItem = %+v", releaseItem, previewItem)
	lastValue := getModelMappingName(releaseItem.GetModelName(), modelMapping)
	previewModelName := getModelMappingName(previewItem.GetModelName(), modelMapping)
	if previewModelName != lastValue {
		if v, ok := modelAliasNameMap[lastValue]; ok {
			lastValue = v
		}
		value, ok := modelAliasNameMap[previewModelName]
		if !ok {
			value = previewModelName
		}
		diff = append(diff, newDiffConfig(ctx, kbe.ConfigKnowledgeModelEmbeddingModel, lastValue, value, ""))
	}
	return diff
}

func getModelMappingName(modelName string, modelMappings map[string]string) string {
	if v, ok := modelMappings[modelName]; ok {
		return v
	}
	return modelName
}

func (l *Logic) equalModelParams(ctx context.Context, modelItemKey string, release, preview *common.ModelParams) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	logx.D(ctx, "equalQAExtractModelConfigItem releaseItem = %+v, previewItem = %+v", release, preview)
	if release == nil || preview == nil {
		return diff
	}
	if (preview.Temperature != nil) != (release.Temperature != nil) {
		lastConfig := gox.IfElse(release.Temperature != nil, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.Temperature != nil, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.TemperatureEnable), lastConfig, newConfig, ""))
	}
	if preview.Temperature != nil {
		if preview.GetTemperature() != release.GetTemperature() {
			diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.Temperature),
				fmt.Sprintf("%.2f", release.GetTemperature()), fmt.Sprintf("%.2f", preview.GetTemperature()), ""))
		}
	}

	if (preview.TopP != nil) != (release.TopP != nil) {
		lastConfig := gox.IfElse(release.TopP != nil, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.TopP != nil, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.TopPEnable), lastConfig, newConfig, ""))
	}
	if preview.TopP != nil {
		if preview.GetTopP() != release.GetTopP() {
			diff = append(diff, newDiffConfig(ctx, fmt.Sprintf(i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.TopP)),
				fmt.Sprintf("%.2f", release.GetTopP()), fmt.Sprintf("%.2f", preview.GetTopP()), ""))
		}
	}

	if (preview.Seed != nil) != (release.Seed != nil) {
		lastConfig := gox.IfElse(release.Seed != nil, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.Seed != nil, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.RandomSeedEnable), lastConfig, newConfig, ""))
	}
	if preview.Seed != nil {
		if preview.GetSeed() != release.GetSeed() {
			diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.RandomSeed),
				fmt.Sprintf("%d", release.GetSeed()), fmt.Sprintf("%d", preview.GetSeed()), ""))
		}
	}

	if (preview.PresencePenalty != nil) != (release.PresencePenalty != nil) {
		lastConfig := gox.IfElse(release.PresencePenalty != nil, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.PresencePenalty != nil, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.PresencePenaltyEnable), lastConfig, newConfig, ""))
	}
	if preview.PresencePenalty != nil {
		if preview.GetPresencePenalty() != release.GetPresencePenalty() {
			diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.PresencePenalty),
				fmt.Sprintf("%.2f", release.GetPresencePenalty()), fmt.Sprintf("%.2f", preview.GetPresencePenalty()), ""))
		}
	}

	if (preview.FrequencyPenalty != nil) != (release.FrequencyPenalty != nil) {
		lastConfig := gox.IfElse(release.FrequencyPenalty != nil, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.FrequencyPenalty != nil, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.FrequencyPenaltyEnable), lastConfig, newConfig, ""))
	}
	if preview.FrequencyPenalty != nil {
		if preview.GetFrequencyPenalty() != release.GetFrequencyPenalty() {
			diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.FrequencyPenalty),
				fmt.Sprintf("%.2f", release.GetFrequencyPenalty()), fmt.Sprintf("%.2f", preview.GetFrequencyPenalty()), ""))
		}
	}

	if (preview.RepetitionPenalty != nil) != (release.RepetitionPenalty != nil) {
		lastConfig := gox.IfElse(release.RepetitionPenalty != nil, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.RepetitionPenalty != nil, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.RepetitionPenaltyEnable), lastConfig, newConfig, ""))
	}
	if preview.RepetitionPenalty != nil {
		if preview.GetRepetitionPenalty() != release.GetRepetitionPenalty() {
			diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.RepetitionPenalty),
				fmt.Sprintf("%.2f", release.GetRepetitionPenalty()), fmt.Sprintf("%.2f", preview.GetRepetitionPenalty()), ""))
		}
	}

	if (preview.MaxTokens != nil) != (release.MaxTokens != nil) {
		lastConfig := gox.IfElse(release.MaxTokens != nil, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(preview.MaxTokens != nil, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.MaxTokensEnable), lastConfig, newConfig, ""))
	}
	if preview.MaxTokens != nil {
		if preview.GetMaxTokens() != release.GetMaxTokens() {
			diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.MaxTokens),
				fmt.Sprintf("%d", release.GetMaxTokens()), fmt.Sprintf("%d", preview.GetMaxTokens()), ""))
		}
	}

	if (len(preview.GetStopSequences()) > 0) != (len(release.GetStopSequences()) > 0) {
		lastConfig := gox.IfElse(len(release.GetStopSequences()) > 0, kbe.Open, kbe.ShutDown)
		newConfig := gox.IfElse(len(preview.GetStopSequences()) > 0, kbe.Open, kbe.ShutDown)
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.StopSequenceEnable), lastConfig, newConfig, ""))
	}
	if len(preview.GetStopSequences()) > 0 {
		if reflect.DeepEqual(preview.GetStopSequences(), release.GetStopSequences()) {
			diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.StopSequence),
				strings.Join(release.GetStopSequences(), ","), strings.Join(preview.GetStopSequences(), ","), ""))
		}
	}

	if preview.GetReplyFormat() != release.GetReplyFormat() {
		diff = append(diff, newDiffConfig(ctx, i18n.Translate(ctx, modelItemKey)+" "+i18n.Translate(ctx, kbe.ReplyFormat),
			fmt.Sprintf("%d", release.GetMaxTokens()), fmt.Sprintf("%d", preview.GetMaxTokens()), ""))
	}

	return diff
}

func (l *Logic) equalQAExtractModelConfigItem(ctx context.Context, release, preview string,
	modelAliasNameMap, modelMapping map[string]string) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	releaseItem, err := l.ConvertStr2QAExtractModelConfigItem(ctx, release, true)
	if err != nil {
		logx.E(ctx, "releaseItem ConvertStr2QAExtractModelConfigItem err: %+v", err)
		return nil
	}
	previewItem, err := l.ConvertStr2QAExtractModelConfigItem(ctx, preview, true)
	if err != nil {
		logx.E(ctx, "previewItem ConvertStr2QAExtractModelConfigItem err: %+v", err)
		return nil
	}
	logx.D(ctx, "equalQAExtractModelConfigItem releaseItem = %+v, previewItem = %+v", releaseItem, previewItem)
	lastValue := getModelMappingName(releaseItem.GetModelName(), modelMapping)
	previewModelName := getModelMappingName(previewItem.GetModelName(), modelMapping)
	if previewModelName != lastValue {
		if v, ok := modelAliasNameMap[lastValue]; ok {
			lastValue = v
		}
		value, ok := modelAliasNameMap[previewModelName]
		if !ok {
			value = previewModelName
		}
		diff = append(diff, newDiffConfig(ctx, kbe.ConfigKnowledgeModelQAExtractModel, lastValue, value, ""))
	}

	if previewItem.GetHistoryLimit() != releaseItem.GetHistoryLimit() {
		diff = append(diff, newDiffConfig(ctx, kbe.ConfigKnowledgeModelQAExtractModel+" "+kbe.HistoryLimit,
			fmt.Sprintf("%d", releaseItem.GetHistoryLimit()), fmt.Sprintf("%d", previewItem.GetHistoryLimit()), ""))
	}

	diff = append(diff, l.equalModelParams(ctx, kbe.ConfigKnowledgeModelQAExtractModel, releaseItem.GetModelParams(), previewItem.GetModelParams())...)
	return diff
}

func (l *Logic) equalKnowledgeSchemaModelConfigItem(ctx context.Context, release, preview string,
	modelAliasNameMap, modelMapping map[string]string) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	releaseItem, err := l.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, release, true)
	if err != nil {
		logx.E(ctx, "releaseItem ConvertStr2KnowledgeSchemaModelConfigItem err: %+v", err)
		return nil
	}
	previewItem, err := l.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, preview, true)
	if err != nil {
		logx.E(ctx, "previewItem ConvertStr2KnowledgeSchemaModelConfigItem err: %+v", err)
		return nil
	}
	logx.D(ctx, "equalKnowledgeSchemaModelConfigItem releaseItem = %+v, previewItem = %+v", releaseItem, previewItem)
	lastValue := getModelMappingName(releaseItem.GetModelName(), modelMapping)
	previewModelName := getModelMappingName(previewItem.GetModelName(), modelMapping)
	if previewModelName != lastValue {
		if v, ok := modelAliasNameMap[lastValue]; ok {
			lastValue = v
		}
		value, ok := modelAliasNameMap[previewModelName]
		if !ok {
			value = previewModelName
		}
		diff = append(diff, newDiffConfig(ctx, kbe.ConfigKnowledgeModelQAExtractModel, lastValue, value, ""))
	}

	if previewItem.GetHistoryLimit() != releaseItem.GetHistoryLimit() {
		diff = append(diff, newDiffConfig(ctx, kbe.ConfigKnowledgeModelQAExtractModel+" "+kbe.HistoryLimit,
			fmt.Sprintf("%d", releaseItem.GetHistoryLimit()), fmt.Sprintf("%d", previewItem.GetHistoryLimit()), ""))
	}

	diff = append(diff, l.equalModelParams(ctx, kbe.ConfigKnowledgeModelQAExtractModel, releaseItem.GetModelParams(), previewItem.GetModelParams())...)
	return diff
}

func (l *Logic) equalFileParseModelConfigItem(ctx context.Context, release, preview string,
	modelAliasNameMap map[string]string) []*kbe.KnowledgeConfigDiff {
	var diff []*kbe.KnowledgeConfigDiff
	releaseItem, err := l.ConvertStr2FileParseModelConfigItem(ctx, release, true)
	if err != nil {
		logx.E(ctx, "releaseItem ConvertStr2KBModelConfigItem err: %+v", err)
		return nil
	}
	previewItem, err := l.ConvertStr2FileParseModelConfigItem(ctx, preview, true)
	if err != nil {
		logx.E(ctx, "previewItem ConvertStr2KBModelConfigItem err: %+v", err)
		return nil
	}
	logx.D(ctx, "equalKBModelConfigItem releaseItem = %+v, previewItem = %+v", releaseItem, previewItem)
	if previewItem.GetModelName() != releaseItem.GetModelName() {
		lastValue := releaseItem.GetModelName()
		if v, ok := modelAliasNameMap[releaseItem.GetModelName()]; ok {
			lastValue = v
		}
		value, ok := modelAliasNameMap[previewItem.GetModelName()]
		if !ok {
			value = previewItem.GetModelName()
		}
		diff = append(diff, newDiffConfig(ctx, kbe.ConfigKnowledgeModelFileParseModel, lastValue, value, ""))
	}
	return diff
}

// OperateAllVectorIndex 创建相似库、评测库
func (l *Logic) OperateAllVectorIndex(ctx context.Context, appID, appBizID, appEmbeddingVersion uint64,
	embeddingModel string, operator uint32) error {
	embeddingVersion := entity.GetEmbeddingVersion(embeddingModel)
	logx.I(ctx, "operatorAllVectorIndex for shareKnowledge botBizID:%d, embeddingVersion:%d, appEmbeddingVersion:%d",
		appBizID, embeddingVersion, appEmbeddingVersion)

	if embeddingVersion == appEmbeddingVersion {
		logx.W(ctx, "operatorAllVectorIndex for shareKnowledge botBizID:%d, "+
			"embeddingVersion:%d = appEmbeddingVersion = %d, skip to create",
			appBizID, embeddingVersion, appEmbeddingVersion)
		return nil

	}

	// 定义任务配置切片
	tasks := []struct {
		indexID uint64
		docType uint32
	}{
		{entity.SimilarVersionID, entity.DocTypeQA},
		{entity.ReviewVersionID, entity.DocTypeQA},
		{entity.SegmentReviewVersionID, entity.DocTypeSegment},
		{entity.RejectedQuestionReviewVersionID, entity.DocTypeRejectedQuestion},
		{entity.RealtimeSegmentVersionID, entity.DocTypeSegment},
		{entity.SegmentImageReviewVersionID, entity.DocTypeImage},
		{entity.RealtimeSegmentImageVersionID, entity.DocTypeImage},
		{entity.DbSourceVersionID, entity.DocTypeSegment},
	}

	g := errgroupx.New()
	g.SetLimit(10)

	// 循环创建并发任务
	for _, task := range tasks {
		task := task // 创建局部变量避免闭包捕获问题
		g.Go(func() error {
			switch operator {
			case kbe.OperatorCreate:
				rsp, err := l.rpc.RetrievalDirectIndex.CreateIndex(ctx, &retrieval.CreateIndexReq{
					RobotId:            appID,
					IndexId:            task.indexID,
					EmbeddingVersion:   embeddingVersion,
					DocType:            task.docType,
					BotBizId:           appBizID,
					EmbeddingModelName: embeddingModel,
				})

				if err != nil {
					logx.W(ctx, "operatorAllVectorIndex (operator:%d) for shareKnowledge botBizID:%d, err:%v",
						operator, appBizID, err)
					return err
				}
				logx.I(ctx, "operatorAllVectorIndex (operator:%d) for shareKnowledge botBizID:%d, rsp:%v",
					operator, appBizID, rsp)
			case kbe.OperatorDelete:
				rsp, err := l.rpc.RetrievalDirectIndex.DeleteIndex(ctx, &retrieval.DeleteIndexReq{
					IndexId:            task.indexID,
					RobotId:            appID,
					EmbeddingVersion:   embeddingVersion,
					BotBizId:           appBizID,
					EmbeddingModelName: embeddingModel,
				})

				if err != nil {
					logx.W(ctx, "operatorAllVectorIndex (operator:%d) for shareKnowledge botBizID:%d, err:%v",
						operator, appBizID, err)
					return nil
				}
				logx.I(ctx, "operatorAllVectorIndex (operator:%d) for shareKnowledge botBizID:%d, rsp:%v",
					operator, appBizID, rsp)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		logx.W(ctx, "operatorAllVectorIndex for shareKnowledge botBizID:%d, err:%v", appBizID, err)
		return err
	}
	return nil
}

func (l *Logic) GetModelDesc(ctx context.Context, configType pb.KnowledgeBaseConfigType, modelName string) (string, string) {
	var descKey string
	modelType := entity.ProviderTypeCustom
	switch configType {
	case pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL:
		descKey = i18nkey.KeyCustomFileParseModelDesc
		if strings.HasPrefix(modelName, "Youtu/") {
			descKey = i18nkey.KeyDefaultFileParseModelDesc
			modelType = entity.ProviderTypeSelf
		} else if strings.HasPrefix(modelName, "AliYun/") {
			if strings.Contains(modelName, "DocParser") {
				descKey = i18nkey.KeyAliYunAsyncFileParseModelDesc
			} else {
				descKey = i18nkey.KeyAliYunSyncFileParseModelDesc
			}
			modelType = entity.ProviderTypeThird
		} else {
			descKey = i18nkey.KeyCustomFileParseModelDesc
		}

	}
	logx.D(ctx, "GetModelDesc, configType:%d, modelName:%s, descKey:%s", configType, modelName, descKey)
	return gox.IfElse(descKey != "", i18n.Translate(ctx, descKey), ""), modelType
}

// GetMappingModelName 获取默认模型配置并应用模型名映射
// 模型名映射会使用本地缓存，只有缓存未命中时才会调用 RPC 接口。
// 然后调用 GetModelMapping 获取模型名映射关系，将原始模型名替换为新的模型名。
func (l *Logic) GetMappingModelName(ctx context.Context, originalModelName string) (mappedModelName string) {
	// 1. 尝试从缓存获取模型映射
	mappedModelName = originalModelName
	value, exist := modelNameMappingCache.Get(originalModelName)
	if exist {
		if cached, ok := value.(string); ok {
			mappedModelName = cached
			logx.D(ctx, "GetMappingModelName get model mapping from cache, original:%s, mapped:%s", originalModelName, mappedModelName)
		} else {
			logx.W(ctx, "GetMappingModelName cache value type error, original:%s", originalModelName)
			exist = false
		}
	}

	// 2. 缓存未命中，调用 RPC 获取模型映射
	if !exist {
		mappingRsp, err := l.rpc.Resource.GetModelMapping(ctx, []string{originalModelName})
		if err != nil {
			logx.W(ctx, "GetMappingModelName GetModelMapping failed, modelName:%s, err:%+v", originalModelName, err)
			// 映射失败不影响主流程，使用原始模型名
		} else if len(mappingRsp) > 0 {
			if mappedName, ok := mappingRsp[originalModelName]; ok && mappedName != "" {
				mappedModelName = mappedName
				modelNameMappingCache.Set(originalModelName, mappedModelName) // 将映射关系存入缓存
				logx.D(ctx, "GetMappingModelName get model mapping from RPC, original:%s, mapped:%s", originalModelName, mappedName)
			}
		}
	}

	// 4. 如果模型名发生了变化，更新配置中的模型名
	if mappedModelName != originalModelName {
		return mappedModelName
	}

	return originalModelName
}

// AppKnowledgeConfigAuditDiff 应用知识库配置差异审计
func (l *Logic) AppKnowledgeConfigAuditDiff(ctx context.Context, corpBizId uint64,
	originKbConfigs, updatedKBConfigs []*kbe.KnowledgeConfig, spaceID string) ([]*kbe.KnowledgeConfigDiff, error) {
	// 柔性放过
	modelMapping, _ := l.rpc.Resource.GetModelMapping(ctx, nil)
	modelAliasNameMap, err := l.rpc.Resource.GetModelAliasName(ctx, corpBizId, spaceID)
	if err != nil {
		logx.W(ctx, "AppKnowledgeConfigAuditDiff GetModelAliasName failed, err:%+v", err)
	}
	var diff []*kbe.KnowledgeConfigDiff

	for _, updatedKBConfig := range updatedKBConfigs {
		originKBConfigs := slicex.Filter(originKbConfigs, func(item *kbe.KnowledgeConfig) bool {
			return item.AppBizID == updatedKBConfig.KnowledgeBizID &&
				item.KnowledgeBizID == updatedKBConfig.KnowledgeBizID && item.Type == updatedKBConfig.Type
		})
		originPreviewConfig := ""
		shareKbName := ""
		if len(originKBConfigs) > 0 {
			originPreviewConfig = originKBConfigs[0].PreviewConfig
			shareKbName = originKBConfigs[0].ShareKbName
		}
		updatedPreviewConfig := updatedKBConfig.PreviewConfig
		logx.I(ctx, "audit kb config, origin: %s, updated: %s", originPreviewConfig, updatedPreviewConfig)

		if updatedKBConfig.Type == uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL) {
			if updatedKBConfig.AppBizID != updatedKBConfig.KnowledgeBizID {
				// 共享知识库，不比较向量化模型
				continue
			}
			diff = append(diff, l.equalEmbeddingModelConfigItem(ctx,
				originPreviewConfig, updatedPreviewConfig, modelAliasNameMap, modelMapping)...)
		}
		if updatedKBConfig.Type == uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL) {
			if updatedKBConfig.AppBizID != updatedKBConfig.KnowledgeBizID {
				// 共享知识库，不比较文档生成问答模型
				continue
			}
			diff = append(diff, l.equalQAExtractModelConfigItem(ctx,
				originPreviewConfig, updatedPreviewConfig, modelAliasNameMap, modelMapping)...)
		} else if updatedKBConfig.Type == uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL) {
			if updatedKBConfig.AppBizID != updatedKBConfig.KnowledgeBizID {
				// 共享知识库，不比较知识库schema生成模型
				continue
			}
			diff = append(diff, l.equalKnowledgeSchemaModelConfigItem(ctx,
				originPreviewConfig, updatedPreviewConfig, modelAliasNameMap, modelMapping)...)
		} else if updatedKBConfig.Type == uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING) {
			if updatedKBConfig.AppBizID != updatedKBConfig.KnowledgeBizID &&
				updatedPreviewConfig == "" && originPreviewConfig != "" {
				// 删除共享知识库，不比较检索设置的比较
				continue
			}
			updatedRetrievalConfig := &pb.RetrievalConfig{}
			if updatedPreviewConfig != "" {
				err = json.Unmarshal([]byte(updatedPreviewConfig), updatedRetrievalConfig)
				if err != nil {
					logx.E(ctx, "AppKnowledgeConfigAuditDiff json.Unmarshal fail, err=%+v", err)
					return nil, err
				}
			}
			if updatedRetrievalConfig.GetSearchStrategy() != nil {
				l.MapSearchStrategyModels(ctx, updatedRetrievalConfig.GetSearchStrategy())
			}
			originRetrievalConfig := &pb.RetrievalConfig{}
			if originPreviewConfig != "" {
				err = json.Unmarshal([]byte(originPreviewConfig), originRetrievalConfig)
				if err != nil {
					logx.E(ctx, "AppKnowledgeConfigAuditDiff json.Unmarshal fail, err=%+v", err)
					return nil, err
				}
			}
			if originRetrievalConfig.GetSearchStrategy() != nil {
				l.MapSearchStrategyModels(ctx, originRetrievalConfig.GetSearchStrategy())
			}
			logx.D(ctx, "AppKnowledgeConfigDetailDiff ShareKbName=%s, originRetrievalConfig=%+v, updatedRetrievalConfig=%+v",
				shareKbName, originRetrievalConfig, updatedRetrievalConfig)
			diff = append(diff, l.equalRetrievalRange(ctx, shareKbName, originRetrievalConfig, updatedRetrievalConfig)...)
			diff = append(diff, l.equalRetrievalConfig(ctx, shareKbName, originRetrievalConfig, updatedRetrievalConfig)...)
			diff = append(diff, l.equalSearchStrategy(ctx, shareKbName, originRetrievalConfig.GetSearchStrategy(),
				updatedRetrievalConfig.GetSearchStrategy(), modelAliasNameMap, modelMapping)...)
		} else if updatedKBConfig.Type == uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL) {
			if updatedKBConfig.AppBizID != updatedKBConfig.KnowledgeBizID {
				// 共享知识库，不比较文档解析模型
				continue
			}
			diff = append(diff, l.equalFileParseModelConfigItem(ctx,
				updatedKBConfig.Config, updatedKBConfig.PreviewConfig, modelAliasNameMap)...)
		}
	}

	return diff, nil
}

// MultiBindShareKb 为应用批量绑定共享知识库
func (l *Logic) MultiBindShareKb(ctx context.Context, kbList []*kbe.AppShareKnowledge, configList []*kbe.KnowledgeConfig) error {
	if len(kbList) == 0 {
		return nil
	}
	logx.I(ctx, "MultiAddShareKb kbList:%+v, configList:%+v", kbList, configList)

	err := l.GetKbDao().CreateAppShareKG(ctx, kbList)
	if err != nil {
		logx.E(ctx, "MultiAddShareKb CreateAppShareKG failed, err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}

	if len(configList) == 0 {
		return nil
	}

	err = l.GetKbDao().ModifyKnowledgeConfigList(ctx, configList)
	if err != nil {
		logx.E(ctx, "MultiAddShareKb ModifyKnowledgeConfigList err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}
	return nil
}

// MultiUnbindShareKb 为应用批量解绑共享知识库
func (l *Logic) MultiUnbindShareKb(ctx context.Context, corpPrimaryId, corpBizId, appBizID uint64, kbBizIds []uint64) error {
	if len(kbBizIds) == 0 {
		return nil
	}
	logx.I(ctx, "MultiDeleteShareKb kbBizIds : %+v", kbBizIds)

	// 解绑角色权限
	err := l.userLogic.DeleteKnowledgeAssociation(ctx, corpPrimaryId, appBizID, kbBizIds)
	if err != nil {
		logx.E(ctx, "MultiDeleteShareKb DeleteKnowledgeAssociation failed, err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}

	// 删除共享知识
	err = l.GetKbDao().DeleteAppShareKG(ctx, appBizID, kbBizIds)
	if err != nil {
		logx.E(ctx, "MultiDeleteShareKb DeleteAppShareKG failed, err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}

	// 删除共享知识配置
	err = l.GetKbDao().DeleteAppSharedKnowledgeConfigs(ctx, corpBizId, appBizID, kbBizIds)
	if err != nil {
		logx.E(ctx, "MultiDeleteShareKb DeleteAppSharedKnowledgeConfigs failed, err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}
	return nil
}

// ClearSpaceKnowledges 清理空间下的所有知识库
func (l *Logic) ClearSpaceKnowledge(ctx context.Context, corpBizId uint64, spaceId string) error {
	logx.I(ctx, "ClearSpaceKnowledge start, corpBizId=%d, spaceId=%s", corpBizId, spaceId)

	// 1. 循环查询出该空间下所有的知识库
	allKnowledgeList := make([]*kbe.SharedKnowledgeInfo, 0)
	pageSize := uint32(100)
	pageNum := uint32(1)
	for {
		knowledgeList, err := l.kbDao.ListBaseSharedKnowledge(ctx, corpBizId, nil, pageNum, pageSize, "", spaceId)
		if err != nil {
			logx.E(ctx, "ClearSpaceKnowledge ListBaseSharedKnowledge fail, page=%d, err=%+v", pageNum, err)
			return err
		}
		logx.D(ctx, "ClearSpaceKnowledge, page=%d, found %d knowledge bases", pageNum, len(knowledgeList))

		if len(knowledgeList) == 0 {
			break
		}

		allKnowledgeList = append(allKnowledgeList, knowledgeList...)

		// 如果返回的数量小于pageSize，说明已经是最后一页
		if len(knowledgeList) < int(pageSize) {
			break
		}

		pageNum++
	}
	logx.I(ctx, "ClearSpaceKnowledge, total found %d knowledge bases to delete", len(allKnowledgeList))

	// 2. 删除知识库记录
	rowsAffected, err := l.kbDao.ClearSpaceSharedKnowledge(ctx, corpBizId, spaceId)
	if err != nil {
		logx.E(ctx, "ClearSpaceKnowledge ClearSpaceSharedKnowledge fail, err=%+v", err)
		return err
	}
	logx.D(ctx, "ClearSpaceKnowledge, rowsAffected: %+v", rowsAffected)

	// 3. 异步逐一删除共享应用
	gox.GoWithContext(ctx, func(ctx context.Context) {
		defer gox.Recover()
		corp, err := l.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, corpBizId)
		if err != nil {
			logx.E(ctx, "ClearSpaceKnowledge DescribeCorpByBizId fail, err=%+v", err)
			return
		}
		ctx = contextx.SetServerMetaData(ctx, contextx.MDCorpID, fmt.Sprintf("%d", corp.CorpPrimaryId))

		wg, wgCtx := errgroupx.WithContext(ctx)
		wg.SetLimit(10) // 限制并发数
		for _, knowledge := range allKnowledgeList {
			knowledge := knowledge // 避免闭包问题
			wg.Go(func() error {
				if _, err := l.rpc.AppAdmin.DeleteShareKnowledgeBaseApp(wgCtx, corp.Uin, knowledge.BusinessID); err != nil {
					logx.W(wgCtx, "DeleteShareKnowledgeBaseApp failed for knowledgeBizId=%d, err: %+v", knowledge.BusinessID, err)
					// 继续删除其他知识库，不因为单个失败而中断
					return err
				}
				logx.D(wgCtx, "ClearSpaceKnowledge, successfully deleted app for knowledgeBizId=%d", knowledge.BusinessID)
				return nil
			})
		}

		// 等待所有删除操作完成
		if err := wg.Wait(); err != nil {
			logx.W(ctx, "ClearSpaceKnowledge, some delete operations failed, err=%+v", err)
		}
		logx.I(ctx, "ClearSpaceKnowledge, async delete completed")
	})

	return nil
}

func (l *Logic) ListBaseSharedKnowledge(ctx context.Context, corpBizId uint64, knowledgeBizIdList []uint64, pageNumber, pageSize uint32, keyword string, spaceId string) ([]*kbe.SharedKnowledgeInfo, error) {
	return l.kbDao.ListBaseSharedKnowledge(ctx, corpBizId, knowledgeBizIdList, pageNumber, pageSize, keyword, spaceId)
}
