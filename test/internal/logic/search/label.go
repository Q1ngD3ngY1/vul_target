package search

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/local_cache"
	"slices"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	utils2 "git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/search"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/errors"
	"git.woa.com/dialogue-platform/common/v3/utils"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	knowledgeConfig "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// FilterCloseKnowledge 每个知识库基于closeKnowledge二次过滤filter-工作流场景使用
func (imp *BotContext) FilterCloseKnowledge(ctx context.Context, closeKnowledge []pb.DocType,
	filters []*retrieval.SearchFilter) []*retrieval.SearchFilter {
	var resultFilter []*retrieval.SearchFilter
	if len(closeKnowledge) > 0 { // 目前支持过滤问答，文档，数据库
		closeMap := slicex.MapKV(closeKnowledge, func(kgType pb.DocType) (pb.DocType, bool) {
			return kgType, true
		})
		for _, f := range filters {
			if ok := closeMap[pb.DocType(f.GetDocType())]; ok {
				continue // 不支持检索，需要过滤
			} else {
				resultFilter = append(resultFilter, &retrieval.SearchFilter{
					IndexId:    f.GetIndexId(),
					Confidence: f.GetConfidence(),
					TopN:       f.GetTopN(),
					DocType:    f.GetDocType(),
				})
			}
		}
	} else {

		for _, f := range filters {
			resultFilter = append(resultFilter, &retrieval.SearchFilter{
				IndexId:    f.GetIndexId(),
				Confidence: f.GetConfidence(),
				TopN:       f.GetTopN(),
				DocType:    f.GetDocType(),
			})
		}
	}
	return resultFilter
}

// HandleSearchLabels 处理检索的标签[针对知识库filters 0代表适用于全部filter]
func (imp *BotContext) HandleSearchLabels(ctx context.Context, kgCfg search.RetrievalKGConfig, docType uint32) *retrieval.LabelExpression {
	defer errors.PanicHandler()
	labelExpression := &retrieval.LabelExpression{}
	var vLabels []*pb.VectorLabel
	var kgLabels []*knowledgeConfig.VectorLabel
	var err error
	if imp.KnowledgeType == pb.KnowledgeType_WORKFLOW { // 步骤一 处理api传入的参数[chat调用场景]
		needDefaultLabel := false
		for _, v := range imp.VisitorLabels {
			if v.Name == "DocID" && docType == model.DocTypeQA { //工作流问答检索不需要带上这些标签
				continue
			}
			vLabels = append(vLabels, v)
		}
		// 步骤二 处理工作流场景的标签
		if len(kgCfg.WorkflowKGCfg.GetLabels()) > 0 { // 1.1 指定知识库标签
			var attrBizIds []uint64
			for _, label := range kgCfg.WorkflowKGCfg.GetLabels() {
				attrBizIds = append(attrBizIds, label.GetAttrBizId())
			}
			attrMap := make(map[uint64]*model.Attribute)
			if kgCfg.IsShareKG {
				// 共享知识库要用自己的知识库ID去查询，否则会查询不到
				attrMap, err = imp.dao.GetAttributeByBizIDs(ctx, kgCfg.KnowledgeID, attrBizIds)
			} else {
				// 应用知识库在isearch场景，需要用自己的应用ID去查询，否则会查询不到
				attrMap, err = imp.dao.GetAttributeByBizIDs(ctx, imp.App.GetId(), attrBizIds)
			}
			if err != nil {
				log.WarnContextf(ctx, "HandleSearchLabels GetAttributeByBizIDs failed, %v", err)
			} else if len(attrMap) > 0 {
				for _, label := range kgCfg.WorkflowKGCfg.GetLabels() {
					kgLabels = append(kgLabels, &knowledgeConfig.VectorLabel{
						Name:   attrMap[label.GetAttrBizId()].AttrKey,
						Values: label.GetAttrValues(),
					})
				}
			}
			vLabels = convertToPbLabels(kgLabels)
			log.DebugContextf(ctx, "HandleSearchLabels Workflow vLabels: %s", utils.Any2String(vLabels))
			needDefaultLabel = true // 携带全局default标签
		} else if len(kgCfg.WorkflowKGCfg.GetKnowledgeScope()) > 0 && docType != model.DocTypeQA { // 1.2 指定知识范围 工作流问答检索不需要带上这些标签
			for _, scope := range kgCfg.WorkflowKGCfg.GetKnowledgeScope() {
				var valuesStr []string
				for _, valuesInt := range scope.GetValues() {
					valuesStr = append(valuesStr, strconv.FormatUint(valuesInt, 10))
				}
				var name string
				if scope.GetScopeType() == pb.KnowledgeScopeTypeEnum_DOC_ID {
					name = "DocID"
				} else if scope.GetScopeType() == pb.KnowledgeScopeTypeEnum_DOC_BIZ_ID {
					// 标签过滤只支持DocID
					name = "DocID"
					valuesStr = handleDocBizIDsLabels(ctx, imp.ReplaceApp.GetAppBizId(), scope.GetValues())
				} else if scope.GetScopeType() == pb.KnowledgeScopeTypeEnum_DOC_CATE_BIZ_ID {
					name = "lke_category_key"
					cateCache, err := dao.GetCateDao(model.DocCate).GetCateCache(ctx, imp.App.GetCorpId(), kgCfg.KnowledgeID)
					if err == nil {
						for _, valuesInt := range scope.GetValues() {
							for _, cateBizID := range cateCache[int(valuesInt)] {
								valuesStr = append(valuesStr, cast.ToString(cateBizID))
							}
						}
						valuesStr = pkg.UniqueStrArr(valuesStr)
					} else {
						log.WarnContextf(ctx, "HandleSearchLabels GetCateCache err:%v", err)
					}
				} else {
					log.WarnContextf(ctx, "HandleSearchLabels invalid scope type, %v", scope.GetScopeType())
					continue
				}
				kgLabels = append(kgLabels, &knowledgeConfig.VectorLabel{
					Name:   name,
					Values: valuesStr,
				})
			}
			vLabels = convertToPbLabels(kgLabels)

			log.DebugContextf(ctx, "HandleSearchLabels Workflow scope labels: %s", utils.Any2String(vLabels))
		}
		// 处理kbAgent标签
		knowledgeSchemaLabel := imp.transKnowledgeSchemaLabels(ctx, imp.App, imp.SceneType, imp.CustomVariables)
		if knowledgeSchemaLabel != nil && docType != model.DocTypeQA { //工作流问答检索不需要带上这些标签
			vLabels = append(vLabels, knowledgeSchemaLabel)
		}
		if needDefaultLabel { // 携带全局default标签
			labelExpression = fillLabelExpression(vLabels,
				getAppLabelCondition(imp.App.GetAppBizId(), getLabelLogicOpr(kgCfg.WorkflowKGCfg.GetLabelLogicOpr())))
		} else { // 不带全局default标签
			labelExpression = fillLabelWithoutGeneralVectorExpression(vLabels,
				getAppLabelCondition(imp.App.GetAppBizId(), getLabelLogicOpr(kgCfg.WorkflowKGCfg.GetLabelLogicOpr())))
		}
	} else { // 非工作流场景，即chat调用场景
		// 1.1处理api自定义参数，custom_variables <api参数名，参数对应的值>
		// 1.2处理api指示标签 visitor_labels【已下线，只兼容存量】
		needDefaultLabel := true
		kgLabels = handleCustomVariablesLabels(imp.VisitorLabels, imp.CustomVariables, imp.APISearchRange[kgCfg.KnowledgeBizID])
		if len(imp.SearchDocBizIDs) > 0 { // 指定文档id检索
			kgLabels = append(kgLabels, &knowledgeConfig.VectorLabel{
				Name:   "DocID",
				Values: handleDocBizIDsLabels(ctx, imp.ReplaceApp.GetAppBizId(), imp.SearchDocBizIDs),
			})
			needDefaultLabel = false
		}
		vLabels = convertToPbLabels(kgLabels)
		env := utils2.When(imp.SceneType == pb.SceneType_TEST, model.AttributeLabelsPreview, model.AttributeLabelsProd)
		vLabels = imp.similarLabels2StandardLabels(ctx, imp.App.GetId(), vLabels, env)
		if needDefaultLabel { // 携带全局default标签
			labelExpression = fillLabelExpression(vLabels,
				getAppLabelCondition(imp.App.GetAppBizId(), imp.APISearchRange[kgCfg.KnowledgeBizID].GetCondition()))
		} else { // 不带全局default标签
			labelExpression = fillLabelWithoutGeneralVectorExpression(vLabels,
				getAppLabelCondition(imp.App.GetAppBizId(), imp.APISearchRange[kgCfg.KnowledgeBizID].GetCondition()))
		}

	}

	finalLabelExp := &retrieval.LabelExpression{}
	// 处理角色权限标签【和知识库标签需要 AND 角色标签】
	roleLabelExpression, ok := imp.RoleLabels[kgCfg.KnowledgeBizID]

	if ok && roleLabelExpression != nil && len(roleLabelExpression.GetExpressions()) > 0 {
		if labelExpression != nil {
			finalLabelExp.Operator = retrieval.LabelExpression_AND
			finalLabelExp.Expressions = append(finalLabelExp.Expressions, labelExpression)
			finalLabelExp.Expressions = append(finalLabelExp.Expressions, roleLabelExpression)
		} else {
			finalLabelExp = roleLabelExpression
		}
	} else if labelExpression != nil {
		finalLabelExp = labelExpression
	} else {
		finalLabelExp = nil
	}
	log.InfoContextf(ctx, "kgBizId:%d,isShareKG:%v,docType:%v,kgLabelExp:%s, roleLabelExp:%s, finalLabelExp:%s",
		kgCfg.KnowledgeBizID, kgCfg.IsShareKG, docType,
		utils.Any2String(labelExpression),
		utils.Any2String(roleLabelExpression),
		utils.Any2String(finalLabelExp))
	return finalLabelExp
}

func handleDocBizIDsLabels(ctx context.Context, routerAppBizID uint64, docBizIDs []uint64) []string {
	var docIDStr []string
	docBizID2DocIDMap, err := local_cache.GetDocIDByDocBizIDs(ctx, routerAppBizID, docBizIDs)
	if err == nil {
		if docBizID2DocIDMap != nil {
			for _, docID := range docBizID2DocIDMap {
				docIDStr = append(docIDStr, strconv.FormatUint(docID, 10))
			}
		}
	} else {
		log.WarnContextf(ctx, "handleDocBizIDsLabels GetDocIDByDocBizIDs err:%v", err)
	}
	return docIDStr
}

// handleCustomVariablesLabels 处理请求CustomVariables中的labels
func handleCustomVariablesLabels(labels []*pb.VectorLabel,
	customVariables map[string]string, searchRange *admin.AppSearchRange) []*knowledgeConfig.VectorLabel {
	var retLabels []*knowledgeConfig.VectorLabel
	// 知识检索范围不为空 取customVariables中的值查找映射关系中的标签
	if len(searchRange.GetCondition()) != 0 && len(searchRange.GetApiVarAttrInfos()) > 0 {
		labels = make([]*pb.VectorLabel, 0)
		for k, v := range customVariables {
			label := &pb.VectorLabel{
				Name:   k,
				Values: strings.Split(v, model.CustomVariableSplitSep),
			}
			labels = append(labels, label)
		}
		if len(labels) == 0 {
			return retLabels
		}
		return handleReqLabels(searchRange, labels)
	} else { // 没有配置过API参数，说明标签已经处理好，直接使用
		for _, label := range labels {
			retLabels = append(retLabels, &knowledgeConfig.VectorLabel{
				Name:   label.GetName(),
				Values: label.GetValues(),
			})
		}
		return retLabels
	}
}

// handleReqLabels 处理请求labels
func handleReqLabels(searchRange *admin.AppSearchRange, labels []*pb.VectorLabel) []*knowledgeConfig.VectorLabel {
	var newLabels []*knowledgeConfig.VectorLabel
	// 通过检索范围的自定义参数《=》label 映射关系转换
	if len(searchRange.GetCondition()) != 0 && len(searchRange.GetApiVarAttrInfos()) > 0 {
		var ApiVarAttrInfosMap = make(map[string]uint64)
		for _, attrInfo := range searchRange.GetApiVarAttrInfos() {
			ApiVarAttrInfosMap[attrInfo.GetApiVarId()] = attrInfo.GetAttrBizId()
		}
		ApiVarNameIDMap := make(map[string]string)
		for apiVarID, ApiVarName := range searchRange.GetApiVarMap() {
			ApiVarNameIDMap[ApiVarName] = apiVarID
		}
		for _, label := range labels {
			// apiVarName 转换 apiVarID
			if apiVarID, ok := ApiVarNameIDMap[label.GetName()]; ok {
				// 通过 apiVarID 获取 attrBizID
				if AttrBizID, ok := ApiVarAttrInfosMap[apiVarID]; ok {
					// 通过 attrBizID 获取 attrKey
					if attrKey, ok := searchRange.GetLabelAttrMap()[AttrBizID]; ok {
						newLabels = append(newLabels, &knowledgeConfig.VectorLabel{
							Name:   attrKey,
							Values: label.GetValues(), // todo 这里后续应该优化成用ID
						})
					}
				}
			}
		}
	}
	return newLabels
}

// transKnowledgeSchemaLabels 处理kbagent标签
func (imp *BotContext) transKnowledgeSchemaLabels(ctx context.Context, app *admin.GetAppInfoRsp, SceneType pb.SceneType,
	customVariables map[string]string) *pb.VectorLabel {
	labelValueDocIds := make([]string, 0)
	for customVariableKey, customVariableValue := range customVariables {
		if customVariableKey != "knowledge_schema_biz_id" || customVariableValue == "" {
			continue
		}
		schemaIds := strings.Split(customVariableValue, "|")
		if len(schemaIds) == 0 {
			continue
		}
		for _, schemaId := range schemaIds {
			// 如果是目录id，读取缓存获取文档自增id
			if docClusterBizIdStr, found := strings.CutPrefix(schemaId, "doc_cluster_"); found {
				envType := model.Scene2EnvType[uint32(SceneType)]
				docClusterBizId, err := strconv.ParseUint(docClusterBizIdStr, 10, 64)
				if err != nil {
					log.WarnContextf(ctx, "transKnowledgeSchemaLabels docClusterBizIdStr convert to uint64 fail,"+
						"docClusterBizIdStr=%+v", docClusterBizIdStr)
				}
				appBizId, err := redis.GetKnowledgeSchemaAppBizIdByDocClusterId(ctx, docClusterBizId, envType)
				if err != nil {
					log.WarnContextf(ctx, "transKnowledgeSchemaLabels GetKnowledgeSchemaAppBizIdByDocClusterId fail,"+
						" docClusterId=%+v, err=%+v", docClusterBizId, err)
				} else if appBizId > 0 {
					docIds, err := redis.GetKnowledgeSchemaDocIdByDocClusterId(ctx, appBizId, envType, docClusterBizId)
					if err != nil {
						log.WarnContextf(ctx, "transKnowledgeSchemaLabels GetKnowledgeSchemaDocIdByDocClusterId fail,"+
							" docClusterId=%+v, err=%+v", docClusterBizId, err)
					} else if len(docIds) > 0 {
						for _, docId := range docIds {
							labelValueDocIds = append(labelValueDocIds, strconv.FormatUint(docId, 10))
						}
					}
				}
			} else if docId, found := strings.CutPrefix(schemaId, "doc_"); found {
				// 如果是文档id，直接用作标签值
				labelValueDocIds = append(labelValueDocIds, docId)
			}
		}
	}
	if len(labelValueDocIds) == 0 {
		return nil
	}
	label := &pb.VectorLabel{
		Name:   "DocID",
		Values: labelValueDocIds,
	}
	return label
}

// similarLabels2StandardLabels 把标签列表中的相似标签转换成主标签（标准标签），未命中相似标签就保持不变
func (imp *BotContext) similarLabels2StandardLabels(ctx context.Context, robotID uint64,
	labels []*pb.VectorLabel, envType string) []*pb.VectorLabel {
	log.InfoContextf(ctx, "similarLabels2StandardLabels, req, robotID:%d, labels:%+v", robotID, labels)
	if len(labels) == 0 {
		return labels
	}
	var mapAttrKey2Labels = make(map[string][]model.AttrLabelAndSimilarLabels, 0)
	// 1. 把所有attrKey对应的labels都取出来
	for _, label := range labels {
		if label == nil {
			continue
		}
		if _, ok := mapAttrKey2Labels[label.GetName()]; ok {
			continue
		}
		labelRedisValue, err := imp.dao.GetAttributeLabelsRedis(ctx, robotID, label.GetName(), envType)
		if err != nil { // 忽略错误
			log.InfoContextf(ctx, "similarLabels2StandardLabels, GetAttributeLabelsRedis failed, robotID:%d, "+
				"attrKey:%s, err:%v", robotID, label.GetName(), err)
			continue
		}
		var labelAndSimilarList []model.AttrLabelAndSimilarLabels
		for _, v := range labelRedisValue {
			if len(v.SimilarLabels) == 0 {
				continue // 相似标签为空，直接跳过
			}
			labelAndSimilar := model.AttrLabelAndSimilarLabels{
				BusinessID: v.BusinessID,
				Name:       v.Name,
			}
			err = jsoniter.Unmarshal([]byte(v.SimilarLabels), &labelAndSimilar.SimilarLabels)
			if err != nil { // 忽略错误
				log.InfoContextf(ctx, "similarLabels2StandardLabels, Unmarshal SimilarLabels failed, "+
					"robotID:%d, SimilarLabels:%s, err:%v", robotID, v.SimilarLabels, err)
				continue
			}
			labelAndSimilarList = append(labelAndSimilarList, labelAndSimilar)
		}
		if len(labelAndSimilarList) > 0 {
			mapAttrKey2Labels[label.GetName()] = labelAndSimilarList
		}
	}

	// 2. 相似标签转主标签
	for i, label := range labels {
		if label == nil {
			continue
		}
		var labelAndSimilarList []model.AttrLabelAndSimilarLabels
		var ok bool
		if labelAndSimilarList, ok = mapAttrKey2Labels[label.GetName()]; !ok {
			// 如果没找到，就保持原数据不变
			continue
		}
		labelValues := label.GetValues()
		for j, l := range labelValues {
			for _, labelAndSimilar := range labelAndSimilarList {
				if slices.Contains(labelAndSimilar.SimilarLabels, l) {
					labelValues[j] = labelAndSimilar.Name // 相似标签转主标签
					break
				}
			}
		}
		labels[i].Values = slicex.Unique(labelValues)
	}
	log.InfoContextf(ctx, "similarLabels2StandardLabels, rsp, robotID:%d, labels:%+v", robotID, labels)
	return labels
}

// filterSearchScope 基于检索范围二次过滤
func filterSearchScope(ctx context.Context, searchScope uint32,
	filters []*retrieval.SearchFilter) []*retrieval.SearchFilter {
	log.InfoContextf(ctx, "filterSearchScope|searchScope:%d", searchScope)
	var scopeFilter []*retrieval.SearchFilter
	if searchScope > 0 {
		scopeFilter = make([]*retrieval.SearchFilter, 0, 1)
		for _, f := range filters {
			if f.GetDocType() == searchScope {
				scopeFilter = append(scopeFilter, f)
				break
			}
		}
	} else {
		scopeFilter = filters
	}
	log.InfoContextf(ctx, "filterSearchScope|scopeFilter:%+v", scopeFilter)
	return scopeFilter
}

// fillLabelExpression 填充标签表达式
func fillLabelExpression(labels []*pb.VectorLabel, condition string) *retrieval.LabelExpression {
	if len(labels) == 0 { // 不传标签的情况 不做任何过滤 直接返回nil
		return nil
	}
	var rsp = &retrieval.LabelExpression{
		Operator: retrieval.LabelExpression_OR,
		Expressions: []*retrieval.LabelExpression{
			{
				Operator: retrieval.LabelExpression_NOOP,
				Condition: &retrieval.LabelExpression_Condition{
					Type:   retrieval.LabelExpression_Condition_STRING,
					Name:   config.App().AttributeLabel.GeneralVectorAttrKey,
					Values: []string{config.App().AttributeLabel.FullLabelValue},
				},
			},
		},
	}
	// 标签检索条件
	var operator retrieval.LabelExpression_LogicOpr
	switch condition {
	case model.AppSearchConditionOr:
		operator = retrieval.LabelExpression_OR
	default:
		operator = retrieval.LabelExpression_AND
	}
	labelExpression := &retrieval.LabelExpression{
		Operator: operator,
	}
	for _, label := range labels {
		labelExpression.Expressions = append(labelExpression.Expressions, &retrieval.LabelExpression{
			Operator: retrieval.LabelExpression_NOOP,
			Condition: &retrieval.LabelExpression_Condition{
				Type:   retrieval.LabelExpression_Condition_ARRAY,
				Name:   label.GetName(),
				Values: append(label.GetValues(), config.App().AttributeLabel.FullLabelValue),
			},
		})
	}
	rsp.Expressions = append(rsp.Expressions, labelExpression)
	return rsp
}

// fillLabelWithoutGeneralVectorExpression 填充标签表达式不带默认的全局标签
func fillLabelWithoutGeneralVectorExpression(labels []*pb.VectorLabel, condition string) *retrieval.LabelExpression {
	if len(labels) == 0 { // 不传标签的情况 不做任何过滤 直接返回nil
		return nil
	}
	// 标签检索条件
	var operator retrieval.LabelExpression_LogicOpr
	switch condition {
	case model.AppSearchConditionOr:
		operator = retrieval.LabelExpression_OR
	default:
		operator = retrieval.LabelExpression_AND
	}
	labelExpression := &retrieval.LabelExpression{
		Operator: operator,
	}
	for _, label := range labels {
		labelExpression.Expressions = append(labelExpression.Expressions, &retrieval.LabelExpression{
			Operator: retrieval.LabelExpression_NOOP,
			Condition: &retrieval.LabelExpression_Condition{
				Type:   retrieval.LabelExpression_Condition_ARRAY,
				Name:   label.GetName(),
				Values: append(label.GetValues(), config.App().AttributeLabel.FullLabelValue),
			},
		})
	}
	return labelExpression
}

// getAppLabelCondition 获取应用标签检索条件
func getAppLabelCondition(appID uint64, condition string) string {
	if len(condition) == 0 {
		condition = model.AppSearchConditionAnd
	}
	isOR, ok := utilConfig.GetWhitelistConfig().LabelOrWhitelist[appID]
	if !ok {
		return condition
	}
	if isOR {
		return model.AppSearchConditionOr
	} else {
		return model.AppSearchConditionAnd
	}
}

// convertToPbLabels 转换Label类型
func convertToPbLabels(labels []*knowledgeConfig.VectorLabel) []*pb.VectorLabel {
	pbLabels := make([]*pb.VectorLabel, 0)
	for _, label := range labels {
		pbLabels = append(pbLabels, &pb.VectorLabel{
			Name:   label.GetName(),
			Values: label.GetValues(),
		})
	}
	return pbLabels
}

// getLabelLogicOpr 获取标签检索条件
func getLabelLogicOpr(opr pb.LogicOpr) string {
	switch opr {
	case pb.LogicOpr_AND:
		return model.AppSearchConditionAnd
	case pb.LogicOpr_OR:
		return model.AppSearchConditionOr
	default:
		return model.AppSearchConditionAnd
	}
}
