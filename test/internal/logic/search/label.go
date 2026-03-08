package search

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"git.woa.com/adp/common/x/logx"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"github.com/spf13/cast"
	"golang.org/x/exp/maps"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/category"
	databaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/entity/search"
	knowledgeConfig "git.woa.com/adp/pb-go/kb/kb_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// FilterCloseKnowledge 每个知识库基于closeKnowledge二次过滤filter-工作流场景使用
func (imp *BotContext) FilterCloseKnowledge(ctx context.Context, closeKnowledge []pb.DocType, filters []*retrieval.SearchFilter) []*retrieval.SearchFilter {
	var resultFilter []*retrieval.SearchFilter
	if len(closeKnowledge) > 0 { // 目前支持过滤问答，文档，数据库
		closeMap := slicex.MapKV(closeKnowledge, func(kgType pb.DocType) (pb.DocType, bool) {
			return kgType, true
		})
		for _, f := range filters {
			if closeMap[pb.DocType(f.GetDocType())] {
				continue // 不支持检索，需要过滤
			}
			resultFilter = append(resultFilter, &retrieval.SearchFilter{
				IndexId:    f.GetIndexId(),
				Confidence: f.GetConfidence(),
				TopN:       f.GetTopN(),
				DocType:    f.GetDocType(),
			})
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
	defer gox.RecoverWithContext(ctx)
	var labelExpression *retrieval.LabelExpression

	if imp.KnowledgeType == pb.SearchKnowledgeType_WORKFLOW {
		labelExpression = imp.handleWorkflowSearchLabels(ctx, kgCfg, docType)
	} else {
		labelExpression = imp.handleRagSearchLabels(ctx, kgCfg, docType)
	}
	finalLabelExp := &retrieval.LabelExpression{
		Operator: retrieval.LabelExpression_AND,
		Expressions: []*retrieval.LabelExpression{
			{
				Operator: retrieval.LabelExpression_NOOP,
				Condition: &retrieval.LabelExpression_Condition{
					Name:   entity.EnableScopeAttr,
					Values: imp.EnableScopeValues(),
				},
			},
		},
	}
	// 处理角色权限标签【和知识库标签需要 AND 角色标签】
	roleLabelExpression, ok := imp.RoleLabels[kgCfg.KnowledgeBizID]

	if ok && roleLabelExpression != nil && len(roleLabelExpression.GetExpressions()) > 0 {
		if labelExpression != nil {
			//finalLabelExp.Operator = retrieval.LabelExpression_AND
			finalLabelExp.Expressions = append(finalLabelExp.Expressions, labelExpression)
			finalLabelExp.Expressions = append(finalLabelExp.Expressions, roleLabelExpression)
		} else {
			finalLabelExp.Expressions = append(finalLabelExp.Expressions, roleLabelExpression)
		}
	} else if labelExpression != nil {
		finalLabelExp.Expressions = append(finalLabelExp.Expressions, labelExpression)
	}
	logx.I(ctx, "kgBizId:%d,isShareKG:%v,docType:%v,knowledgeType:%v,kgLabelExp:%s,roleLabelExp:%s,finalLabelExp:%s",
		kgCfg.KnowledgeBizID, kgCfg.IsShareKG, docType, imp.KnowledgeType,
		jsonx.MustMarshalToString(labelExpression),
		jsonx.MustMarshalToString(roleLabelExpression),
		jsonx.MustMarshalToString(finalLabelExp))
	return finalLabelExp
}

// handleWorkflowSearchLabels 处理工作流场景的检索标签
func (imp *BotContext) handleWorkflowSearchLabels(ctx context.Context, kgCfg search.RetrievalKGConfig, docType uint32) *retrieval.LabelExpression {
	var vLabels []*pb.VectorLabel
	var kgLabels []*knowledgeConfig.VectorLabel
	var err error
	needDefaultLabel := false

	// 步骤一 处理api传入的参数
	for _, v := range imp.VisitorLabels {
		if v.Name == "DocID" && (docType == entity.DocTypeQA || docType == entity.DocTypeDB) { // 工作流问答检索不需要带上这些标签
			continue
		}
		vLabels = append(vLabels, v)
	}

	// 步骤二 处理工作流场景的标签
	if len(kgCfg.WorkflowKGCfg.GetLabels()) > 0 { // 1.1 指定知识库标签-不区分文档，问答，数据库
		var attrBizIds []uint64
		for _, label := range kgCfg.WorkflowKGCfg.GetLabels() {
			attrBizIds = append(attrBizIds, label.GetAttrBizId())
		}
		attrMap := make(map[uint64]*labelEntity.Attribute)
		if kgCfg.IsShareKG {
			// 共享知识库要用自己的知识库ID去查询，否则会查询不到
			attrMap, err = imp.labelDao.GetAttributeByBizIDs(ctx, kgCfg.KnowledgeID, attrBizIds)
		} else {
			// 应用知识库在isearch场景，需要用自己的应用ID去查询，否则会查询不到
			attrMap, err = imp.labelDao.GetAttributeByBizIDs(ctx, imp.App.PrimaryId, attrBizIds)
		}
		if err != nil {
			logx.W(ctx, "handleWorkflowSearchLabels GetAttributeByBizIDs failed, %v", err)
		} else if len(attrMap) > 0 {
			for _, label := range kgCfg.WorkflowKGCfg.GetLabels() {
				kgLabels = append(kgLabels, &knowledgeConfig.VectorLabel{
					Name:   attrMap[label.GetAttrBizId()].AttrKey,
					Values: label.GetAttrValues(),
				})
			}
		}
		vLabels = convertToPbLabels(kgLabels)
		logx.D(ctx, "handleWorkflowSearchLabels Workflow vLabels: %s", jsonx.MustMarshalToString(vLabels))
		needDefaultLabel = true // 携带全局default标签
	} else if len(kgCfg.WorkflowKGCfg.GetKnowledgeScope()) > 0 { // 1.2 指定知识范围，需要按类型处理
		if docType != entity.DocTypeQA && docType != entity.DocTypeDB {
			for _, scope := range kgCfg.WorkflowKGCfg.GetKnowledgeScope() {
				var valuesStr []string
				for _, valuesInt := range scope.GetValues() {
					valuesStr = append(valuesStr, strconv.FormatUint(valuesInt, 10))
				}
				var name string
				if scope.GetScopeType() == pb.KnowledgeScopeTypeEnum_DOC_ID {
					name = "DocID"
					if imp.SceneType == pb.SceneType_PROD {
						// 知识库概念统一，检索发布域需要替换成从发布域拷贝到开发域的文档ID
						docID2DocBizIDMap, err := imp.cacheLogic.GetDocBizIDByDocIDs(ctx, imp.ReplaceApp.BizId, scope.GetValues())
						if err != nil {
							logx.W(ctx, "handleWorkflowSearchLabels GetDocIDByDocBizIDs err:%v", err)
						}
						valuesStr = imp.handleDocBizIDsLabels(ctx, imp.ReplaceApp.BizId, maps.Values(docID2DocBizIDMap))
					}
				} else if scope.GetScopeType() == pb.KnowledgeScopeTypeEnum_DOC_BIZ_ID {
					// 标签过滤只支持DocID
					name = "DocID"
					valuesStr = imp.handleDocBizIDsLabels(ctx, imp.ReplaceApp.BizId, scope.GetValues())
				} else if scope.GetScopeType() == pb.KnowledgeScopeTypeEnum_DOC_CATE_BIZ_ID {
					name = "lke_category_key"
					cateCache, err := imp.cateLogic.DescribeCateCache(ctx, category.DocCate, imp.App.CorpPrimaryId, kgCfg.KnowledgeID)
					if err == nil {
						for _, valuesInt := range scope.GetValues() {
							for _, cateBizID := range cateCache[int(valuesInt)] {
								valuesStr = append(valuesStr, cast.ToString(cateBizID))
							}
						}
						valuesStr = slicex.Unique(valuesStr)
					} else {
						logx.W(ctx, "handleWorkflowSearchLabels GetCateCache err:%v", err)
					}
				} else {
					logx.W(ctx, "handleWorkflowSearchLabels invalid scope type, %v", scope.GetScopeType())
					continue
				}
				kgLabels = append(kgLabels, &knowledgeConfig.VectorLabel{
					Name:   name,
					Values: valuesStr,
				})
			}
			vLabels = convertToPbLabels(kgLabels)
		} else if docType == entity.DocTypeDB { // 处理数据库标签
			// 处理DB_TABLE_BIZ_ID类型的知识范围
			for _, scope := range kgCfg.WorkflowKGCfg.GetKnowledgeScope() {
				if scope.GetScopeType() == pb.KnowledgeScopeTypeEnum_DB_TABLE_BIZ_ID {
					// 知识库概念统一刷数据需要兼容工作流和agent绑定文档的场景
					// 如果是检索发布域，需要先从临时表中查询是否该文档从发布域拷贝数据到了开发域
					// 先查询docBizId拷贝后的新docBizId（从发布域拷贝到开发域的映射关系）
					tableBizIDs := scope.GetValues()
					if imp.SceneType == pb.SceneType_PROD {
						devReleaseRelationMap, err := imp.releaseDao.GetDevReleaseRelationInfoList(ctx, imp.App.CorpPrimaryId,
							imp.App.PrimaryId, releaseEntity.DevReleaseRelationTypeTable, scope.GetValues())
						if err != nil {
							logx.W(ctx, "handleWorkflowSearchLabels GetDevReleaseRelationInfoList err:%v", err)
						} else if len(devReleaseRelationMap) > 0 {
							// 如果查询到了映射关系，将原tableBizIDs中可以替换的替换掉，不能替换的继续保留
							newTableBizIDs := make([]uint64, 0, len(tableBizIDs))
							for _, tableBizID := range tableBizIDs {
								if newTableBizID, ok := devReleaseRelationMap[tableBizID]; ok {
									newTableBizIDs = append(newTableBizIDs, newTableBizID)
								} else {
									newTableBizIDs = append(newTableBizIDs, tableBizID)
								}
							}
							tableBizIDs = newTableBizIDs
							logx.I(ctx, "handleWorkflowSearchLabels found dev-release relation, use new tableBizIDs:%v", tableBizIDs)
						}
					}
					var valuesStr []string
					for _, valuesInt := range tableBizIDs {
						valuesStr = append(valuesStr, cast.ToString(valuesInt))
					}
					valuesStr = slicex.Unique(valuesStr)
					kgLabels = append(kgLabels, &knowledgeConfig.VectorLabel{
						Name:   databaseEntity.LabelDBTableBizID,
						Values: valuesStr,
					})
					vLabels = convertToPbLabels(kgLabels)
				}
			}
		}
		logx.D(ctx, "handleWorkflowSearchLabels docType:%d, labels: %s", docType, jsonx.MustMarshalToString(vLabels))
	}
	// 处理kbAgent标签
	knowledgeSchemaLabel := imp.transKnowledgeSchemaLabels(ctx, imp.SceneType, imp.CustomVariables)
	if knowledgeSchemaLabel != nil && docType != entity.DocTypeQA { // 工作流问答检索不需要带上这些标签
		vLabels = append(vLabels, knowledgeSchemaLabel)
	}

	// 支持标签值按竖线|分隔并去重
	vLabels = deduplicateAndSplitLabels(ctx, vLabels)

	if needDefaultLabel { // 携带全局default标签
		return fillLabelExpression(vLabels,
			getAppLabelCondition(imp.App.BizId, getLabelLogicOpr(kgCfg.WorkflowKGCfg.GetLabelLogicOpr())))
	} else { // 不带全局default标签
		return fillLabelWithoutGeneralVectorExpression(vLabels,
			getAppLabelCondition(imp.App.BizId, getLabelLogicOpr(kgCfg.WorkflowKGCfg.GetLabelLogicOpr())))
	}
}

// handleRagSearchLabels 处理非工作流场景（rag调用场景）的检索标签
func (imp *BotContext) handleRagSearchLabels(ctx context.Context, kgCfg search.RetrievalKGConfig, docType uint32) *retrieval.LabelExpression {
	// 1.1处理api自定义参数，custom_variables <api参数名，参数对应的值>
	// 1.2处理api指示标签 visitor_labels【已下线，只兼容存量】
	needDefaultLabel := true
	kgLabels := imp.handleCustomVariablesLabels(ctx, kgCfg.KnowledgeID, imp.VisitorLabels, imp.CustomVariables, imp.APISearchRange[kgCfg.KnowledgeBizID])
	if len(imp.SearchDocBizIDs) > 0 { // 指定文档id检索
		kgLabels = append(kgLabels, &knowledgeConfig.VectorLabel{
			Name:   "DocID",
			Values: imp.handleDocBizIDsLabels(ctx, imp.ReplaceApp.BizId, imp.SearchDocBizIDs),
		})
		needDefaultLabel = false
	}
	vLabels := convertToPbLabels(kgLabels)
	env := gox.IfElse[string](imp.SceneType == pb.SceneType_TEST, labelEntity.AttributeLabelsPreview, labelEntity.AttributeLabelsProd)
	vLabels = imp.similarLabels2StandardLabels(ctx, kgCfg.KnowledgeID, vLabels, env)

	if needDefaultLabel { // 携带全局default标签
		return fillLabelExpression(vLabels,
			getAppLabelCondition(imp.App.BizId, imp.APISearchRange[kgCfg.KnowledgeBizID].Condition))
	} else { // 不带全局default标签
		return fillLabelWithoutGeneralVectorExpression(vLabels,
			getAppLabelCondition(imp.App.BizId, imp.APISearchRange[kgCfg.KnowledgeBizID].Condition))
	}
}

func (imp *BotContext) handleDocBizIDsLabels(ctx context.Context, routerAppBizID uint64, docBizIDs []uint64) []string {
	var docIDStr []string
	// 知识库概念统一刷数据需要兼容工作流和agent绑定文档的场景
	// 如果是检索发布域，需要先从临时表中查询是否该文档从发布域拷贝数据到了开发域
	// 先查询docBizId拷贝后的新docBizId（从发布域拷贝到开发域的映射关系）
	if imp.SceneType == pb.SceneType_PROD {
		devReleaseRelationMap, err := imp.releaseDao.GetDevReleaseRelationInfoList(ctx, imp.App.CorpPrimaryId, imp.App.PrimaryId, releaseEntity.DevReleaseRelationTypeDocument, docBizIDs)
		if err != nil {
			logx.W(ctx, "handleDocBizIDsLabels GetDevReleaseRelationInfoList err:%v", err)
		} else if len(devReleaseRelationMap) > 0 {
			// 如果查询到了映射关系，将原docBizIDs中可以替换的替换掉，不能替换的继续保留
			newDocBizIDs := make([]uint64, 0, len(docBizIDs))
			for _, docBizID := range docBizIDs {
				if newDocBizID, ok := devReleaseRelationMap[docBizID]; ok {
					newDocBizIDs = append(newDocBizIDs, newDocBizID)
				} else {
					newDocBizIDs = append(newDocBizIDs, docBizID)
				}
			}
			docBizIDs = newDocBizIDs
			logx.I(ctx, "handleDocBizIDsLabels found dev-release relation, use new docBizIDs:%v", docBizIDs)
		}
	}

	docBizID2DocIDMap, err := imp.cacheLogic.GetDocIDByDocBizIDs(ctx, routerAppBizID, docBizIDs)
	if err == nil {
		if docBizID2DocIDMap != nil {
			for _, docID := range docBizID2DocIDMap {
				docIDStr = append(docIDStr, strconv.FormatUint(docID, 10))
			}
		}
	} else {
		logx.W(ctx, "handleDocBizIDsLabels GetDocIDByDocBizIDs err:%v", err)
	}
	return docIDStr
}

// handleCustomVariablesLabels 处理请求CustomVariables中的labels
func (imp *BotContext) handleCustomVariablesLabels(ctx context.Context, kbPrimaryId uint64, labels []*pb.VectorLabel,
	customVariables map[string]string, searchRange *entity.SearchRange) []*knowledgeConfig.VectorLabel {
	logx.D(ctx, "handleCustomVariablesLabels kbPrimaryId:%d, labels:%s, customVariables:%s, searchRange:%s",
		kbPrimaryId, jsonx.MustMarshalToString(labels), jsonx.MustMarshalToString(customVariables), jsonx.MustMarshalToString(searchRange))
	var newLabels []*knowledgeConfig.VectorLabel
	if searchRange == nil {
		logx.E(ctx, "handleCustomVariablesLabels searchRange is nil")
		return newLabels
	}
	if searchRange.Condition == "" || len(searchRange.ApiVarAttrInfos) == 0 || len(customVariables) == 0 {
		// 兼容直接传入attrKey的逻辑，该入参已废弃
		// 没有配置过API参数，说明标签已经处理好，直接使用
		for _, label := range labels {
			newLabels = append(newLabels, &knowledgeConfig.VectorLabel{
				Name:   label.GetName(),
				Values: label.GetValues(),
			})
		}
		return newLabels
	}

	// 知识检索范围不为空，忽略已废弃的入参labels，取customVariables中的值查找映射关系中的标签
	labels = make([]*pb.VectorLabel, 0)
	for k, v := range customVariables {
		label := &pb.VectorLabel{
			Name:   k,
			Values: strings.Split(v, entity.CustomVariableSplitSep),
		}
		labels = append(labels, label)
	}
	if len(labels) == 0 {
		return newLabels
	}
	// 通过检索范围的自定义参数《=》labelEntity 映射关系转换
	var ApiVarAttrInfosMap = make(map[string]uint64)
	attrBizIds := make([]uint64, 0)
	for _, attrInfo := range searchRange.ApiVarAttrInfos {
		if attrInfo.ApiVarID == "" || attrInfo.AttrBizID == 0 {
			logx.E(ctx, "handleCustomVariablesLabels ApiVarAttrInfosMap ApiVarID or AttrBizID is empty, ApiVarAttrInfos:%+v", attrInfo)
			continue
		}
		ApiVarAttrInfosMap[attrInfo.ApiVarID] = attrInfo.AttrBizID
		attrBizIds = append(attrBizIds, attrInfo.AttrBizID)
	}
	attrBizIds = slicex.Unique(attrBizIds)
	ApiVarNameIDMap := make(map[string]string)
	for apiVarID, ApiVarName := range searchRange.APIVarMap {
		if ApiVarName == "" || apiVarID == "" {
			logx.E(ctx, "handleCustomVariablesLabels ApiVarName or apiVarID is empty, ApiVarName:%s, apiVarID:%s", ApiVarName, apiVarID)
			continue
		}
		ApiVarNameIDMap[ApiVarName] = apiVarID
	}
	// 通过AttrBizID查询attrKey
	attrMap, err := imp.labelDao.GetAttributeByBizIDs(ctx, kbPrimaryId, attrBizIds)
	if err != nil {
		logx.E(ctx, "handleCustomVariablesLabels GetAttributeByBizIDs err:%v", err)
		return newLabels
	}

	for _, label := range labels {
		// apiVarName 转换 apiVarID
		if apiVarID, ok := ApiVarNameIDMap[label.GetName()]; ok {
			// 通过 apiVarID 获取 attrBizID
			if AttrBizID, ok := ApiVarAttrInfosMap[apiVarID]; ok {
				// 通过 attrBizID 获取 attrKey
				if attr, ok := attrMap[AttrBizID]; ok {
					newLabels = append(newLabels, &knowledgeConfig.VectorLabel{
						Name:   attr.AttrKey,
						Values: label.GetValues(), // todo 这里后续应该优化成用ID
					})
				}
			}
		}
	}
	logx.D(ctx, "handleCustomVariablesLabels|newLabels:%+v", newLabels)
	return newLabels
}

// transKnowledgeSchemaLabels 处理kbagent标签
func (imp *BotContext) transKnowledgeSchemaLabels(ctx context.Context, SceneType pb.SceneType,
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
				envType := entity.Scene2EnvType[uint32(SceneType)]
				docClusterBizId, err := strconv.ParseUint(docClusterBizIdStr, 10, 64)
				if err != nil {
					logx.W(ctx, "transKnowledgeSchemaLabels docClusterBizIdStr convert to uint64 fail,"+
						"docClusterBizIdStr=%+v", docClusterBizIdStr)
				}
				appBizId, err := imp.kbDao.GetKnowledgeSchemaAppBizIdByDocClusterId(ctx, docClusterBizId, envType)
				if err != nil {
					logx.W(ctx, "transKnowledgeSchemaLabels GetKnowledgeSchemaAppBizIdByDocClusterId fail,"+
						" docClusterId=%+v, err=%+v", docClusterBizId, err)
				} else if appBizId > 0 {
					docIds, err := imp.kbDao.GetKnowledgeSchemaDocIdByDocClusterId(ctx, appBizId, envType, docClusterBizId)
					if err != nil {
						logx.W(ctx, "transKnowledgeSchemaLabels GetKnowledgeSchemaDocIdByDocClusterId fail,"+
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
	logx.I(ctx, "similarLabels2StandardLabels, req, robotID:%d, labels:%+v", robotID, labels)
	if len(labels) == 0 {
		return labels
	}
	var mapAttrKey2Labels = make(map[string][]labelEntity.AttrLabelAndSimilarLabels, 0)
	// 1. 把所有attrKey对应的labels都取出来
	for _, lb := range labels {
		if lb == nil {
			continue
		}
		if _, ok := mapAttrKey2Labels[lb.GetName()]; ok {
			continue
		}
		labelRedisValue, err := imp.labelDao.GetAttributeLabelsRedis(ctx, robotID, lb.GetName(), envType)
		if err != nil { // 忽略错误
			logx.I(ctx, "similarLabels2StandardLabels, GetAttributeLabelsRedis failed, robotID:%d, attrKey:%s, err:%v", robotID, lb.GetName(), err)
			continue
		}
		var labelAndSimilarList []labelEntity.AttrLabelAndSimilarLabels
		for _, v := range labelRedisValue {
			if len(v.SimilarLabels) == 0 {
				continue // 相似标签为空，直接跳过
			}
			labelAndSimilar := labelEntity.AttrLabelAndSimilarLabels{
				BusinessID: v.BusinessID,
				Name:       v.Name,
			}
			err = jsonx.Unmarshal([]byte(v.SimilarLabels), &labelAndSimilar.SimilarLabels)
			if err != nil { // 忽略错误
				logx.I(ctx, "similarLabels2StandardLabels, Unmarshal SimilarLabels failed, "+
					"robotID:%d, SimilarLabels:%s, err:%v", robotID, v.SimilarLabels, err)
				continue
			}
			labelAndSimilarList = append(labelAndSimilarList, labelAndSimilar)
		}
		if len(labelAndSimilarList) > 0 {
			mapAttrKey2Labels[lb.GetName()] = labelAndSimilarList
		}
	}

	// 2. 相似标签转主标签
	for i, lb := range labels {
		if lb == nil {
			continue
		}
		var labelAndSimilarList []labelEntity.AttrLabelAndSimilarLabels
		var ok bool
		if labelAndSimilarList, ok = mapAttrKey2Labels[lb.GetName()]; !ok {
			// 如果没找到，就保持原数据不变
			continue
		}
		labelValues := lb.GetValues()
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
	logx.I(ctx, "similarLabels2StandardLabels, rsp, robotID:%d, labels:%+v", robotID, labels)
	return labels
}

// filterSearchScope 基于检索范围二次过滤
func filterSearchScope(ctx context.Context, searchScope uint32,
	filters []*retrieval.SearchFilter) []*retrieval.SearchFilter {
	logx.I(ctx, "filterSearchScope|searchScope:%d", searchScope)
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
	logx.I(ctx, "filterSearchScope|scopeFilter:%+v", scopeFilter)
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
	case entity.AppSearchConditionOr:
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
	case entity.AppSearchConditionOr:
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
		condition = entity.AppSearchConditionAnd
	}
	isOR, ok := config.GetWhitelistConfig().LabelOrWhitelist[appID]
	if !ok {
		return condition
	}
	if isOR {
		return entity.AppSearchConditionOr
	} else {
		return entity.AppSearchConditionAnd
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
		return entity.AppSearchConditionAnd
	case pb.LogicOpr_OR:
		return entity.AppSearchConditionOr
	default:
		return entity.AppSearchConditionAnd
	}
}

// deduplicateAndSplitLabels 按标签名去重，并将标签值按竖线分隔再去重
// 例如：标签A：x,y 和 标签A：y|z,x|m 合并后变成 标签A：x,y,z,m
func deduplicateAndSplitLabels(ctx context.Context, labels []*pb.VectorLabel) []*pb.VectorLabel {
	if len(labels) == 0 {
		return labels
	}

	// 使用map按标签名分组
	labelMap := make(map[string][]string)

	for _, label := range labels {
		if label == nil {
			continue
		}
		labelName := label.GetName()
		if labelName == "" {
			continue
		}

		if _, exists := labelMap[labelName]; !exists {
			labelMap[labelName] = make([]string, 0)
		}

		// 处理每个标签值，按竖线分隔
		for _, value := range label.GetValues() {
			if value == "" {
				continue
			}
			// 按竖线分隔标签值
			splitValues := strings.Split(value, entity.CustomVariableSplitSep)
			for _, splitValue := range splitValues {
				if splitValue != "" {
					labelMap[labelName] = append(labelMap[labelName], splitValue)
				}
			}
		}
	}

	// 构建去重后的标签列表
	result := make([]*pb.VectorLabel, 0, len(labelMap))
	for labelName, values := range labelMap {
		if len(values) == 0 {
			continue
		}
		// 对标签值去重
		uniqueValues := slicex.Unique(values)
		result = append(result, &pb.VectorLabel{
			Name:   labelName,
			Values: uniqueValues,
		})
	}

	logx.I(ctx, "deduplicateAndSplitLabels|input:%s|output:%s",
		jsonx.MustMarshalToString(labels), jsonx.MustMarshalToString(result))

	return result
}
