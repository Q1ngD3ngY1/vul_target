package api

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"

	logicSearch "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/search"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// SearchKnowledge 知识库检索
func (s *Service) SearchKnowledge(ctx context.Context, req *knowledge.SearchKnowledgeReq) (
	rsp *knowledge.SearchKnowledgeRsp, err error) {
	log.InfoContextf(ctx, "SearchKnowledge called, req: %+v", req)
	rsp = &knowledge.SearchKnowledgeRsp{}
	if err = s.checkSearchKnowledgeReq(ctx, req); err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, req.GetReq().GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	ctx = pkg.WithSpaceID(ctx, app.SpaceID)
	switch req.KnowledgeType {
	case knowledge.KnowledgeType_GLOBAL_KNOWLEDGE:
		rsp, err = s.searchGlobalKnowledge(ctx, req, app)
	case knowledge.KnowledgeType_DOC_QA, knowledge.KnowledgeType_WORKFLOW:
		var bc logicSearch.BotContext
		err = bc.Init(ctx, nil, req, s.dao)
		if err != nil {
			log.ErrorContextf(ctx, "SearchKnowledge Init failed, err: %+v", err)
			return nil, err
		}
		if bc.RoleNotAllowedSearch {
			log.WarnContextf(ctx, "Role not allow Search")
			return rsp, nil
		}
		rsp, err = s.searchAnswer(ctx, &bc)
	case knowledge.KnowledgeType_REJECTED_QUESTION:
		rsp, err = s.searchRejectQuestion(ctx, req, app)
	case knowledge.KnowledgeType_REALTIME:
		rsp, err = s.searchRealtime(ctx, req, app)
	default:
		err = fmt.Errorf("SearchKnowledge KnowledgeType:%+v illegal", req.KnowledgeType)
	}
	if err != nil {
		log.ErrorContextf(ctx, "SearchKnowledge failed, err: %+v", err)
		return nil, err
	}

	log.InfoContextf(ctx, "SearchKnowledge called, rsp: %+v", rsp)
	return rsp, nil
}

// SearchKnowledgeBatch 知识库检索（支持检索多个知识库）
func (s *Service) SearchKnowledgeBatch(ctx context.Context, req *knowledge.SearchKnowledgeBatchReq) (
	rsp *knowledge.SearchKnowledgeRsp, err error) {
	log.InfoContextf(ctx, "SearchKnowledge called, req: %+v", req)
	app, err := s.getAppByAppBizID(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	ctx = pkg.WithSpaceID(ctx, app.SpaceID)
	var bc logicSearch.BotContext
	err = bc.Init(ctx, req, nil, s.dao)
	if err != nil {
		log.ErrorContextf(ctx, "SearchKnowledge Init failed, err: %+v", err)
		return nil, err
	}
	if bc.RoleNotAllowedSearch {
		log.WarnContextf(ctx, "Role not allow Search")
		return rsp, nil
	}
	switch req.KnowledgeType { // 拒答，实时文档，全局知识库，不存在同时检索多个知识库，这里不做处理
	case knowledge.KnowledgeType_DOC_QA, knowledge.KnowledgeType_WORKFLOW:
		if dbTableID := bc.GetDBTableID(); dbTableID > 0 {
			rsp, err = s.searchDBAnswer(ctx, &bc, dbTableID)
		} else {
			rsp, err = s.searchAnswer(ctx, &bc)
		}
	default:
		err = fmt.Errorf("SearchKnowledge KnowledgeType:%+v illegal", req.KnowledgeType)
	}
	if err != nil {
		log.ErrorContextf(ctx, "SearchKnowledge failed, err: %+v", err)
		return nil, err
	}

	log.InfoContextf(ctx, "SearchKnowledge called, rsp: %+v", rsp)
	return rsp, nil
}

// checkSearchKnowledgeReq 校验知识库检索Req
func (s *Service) checkSearchKnowledgeReq(ctx context.Context, req *knowledge.SearchKnowledgeReq) error {
	// 检查标签
	for _, label := range req.GetReq().GetLabels() {
		if label == nil {
			req.Req.Labels = nil
			log.WarnContextf(ctx, "SearchKnowledge label is nil, req: %+v", req)
			break
		}
		if len(label.GetName()) == 0 || len(label.GetValues()) == 0 {
			// 降级处理，删除该标签，不报错
			req.Req.Labels = nil
			log.WarnContextf(ctx, "SearchKnowledge label is nil, req: %+v", req)
			break
		}
		for _, v := range label.GetValues() {
			if len(v) == 0 {
				req.Req.Labels = nil
				log.WarnContextf(ctx, "SearchKnowledge label is nil, req: %+v", req)
				break
			}
		}
	}
	return nil
}

// searchGlobalKnowledge 全局知识库
func (s *Service) searchGlobalKnowledge(ctx context.Context, req *knowledge.SearchKnowledgeReq, app *model.App) (
	rsp *knowledge.SearchKnowledgeRsp, err error) {
	log.InfoContextf(ctx, "searchGlobalKnowledge, req: %+v", req)
	if req.GetSceneType() != knowledge.SceneType_UNKNOWN_SCENE {
		err = fmt.Errorf("searchGlobalKnowledge SceneType:%+v illegal", req.GetSceneType())
		return nil, err
	}
	pbReq := &pb.GlobalKnowledgeReq{
		Question:  req.GetReq().GetQuestion(),
		FilterKey: model.SearchGlobalFilterKey,
		Labels:    convertToPbLabels(req.GetReq().GetLabels()),
	}
	pbRsp, err := s.GlobalKnowledge(ctx, pbReq)
	if err != nil {
		log.ErrorContextf(ctx, "searchGlobalKnowledge failed, err: %+v", err)
		return nil, err
	}
	docs := make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range pbRsp.Docs {
		docs = append(docs, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			DocId:                doc.GetDocId(),
			DocType:              doc.GetDocType(),
			RelatedId:            doc.GetRelatedId(),
			Question:             doc.GetQuestion(),
			Answer:               doc.GetAnswer(),
			Confidence:           doc.GetConfidence(),
			OrgData:              doc.GetOrgData(),
			RelatedBizId:         doc.GetRelatedBizId(),
			Extra:                convertToKnowledgeExtra(doc.GetExtra()),
			ImageUrls:            doc.GetImageUrls(),
			IsBigData:            doc.GetIsBigData(),
			ResultType:           convertToKnowledgeResultType(doc.GetResultType()),
			SheetInfo:            doc.GetSheetInfo(),
			SimilarQuestionExtra: convertToKnowledgeSimilarQuestionExtra(doc.GetSimilarQuestionExtra()),
		})
	}
	return &knowledge.SearchKnowledgeRsp{
		KnowledgeType: knowledge.KnowledgeType_GLOBAL_KNOWLEDGE,
		SceneType:     knowledge.SceneType_UNKNOWN_SCENE,
		Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
			Docs: docs,
		},
	}, nil
}

// convertPreviewDocToKnowledgeDoc 转换PreviewDoc类型
func convertPreviewDocToKnowledgeDoc(docs []*pb.SearchPreviewRsp_Doc) []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			DocId:                doc.GetDocId(),
			DocType:              doc.GetDocType(),
			RelatedId:            doc.GetRelatedId(),
			Question:             doc.GetQuestion(),
			Answer:               doc.GetAnswer(),
			Confidence:           doc.GetConfidence(),
			OrgData:              doc.GetOrgData(),
			RelatedBizId:         doc.GetRelatedBizId(),
			QuestionPlaceholders: convertToKnowledgePlaceholders(doc.GetQuestionPlaceholders()),
			AnswerPlaceholders:   convertToKnowledgePlaceholders(doc.GetAnswerPlaceholders()),
			OrgDataPlaceholders:  convertToKnowledgePlaceholders(doc.GetOrgDataPlaceholders()),
			CustomParam:          doc.GetCustomParam(),
			QuestionDesc:         doc.GetQuestionDesc(),
			Extra:                convertToKnowledgeExtra(doc.GetExtra()),
			ImageUrls:            doc.GetImageUrls(),
			IsBigData:            doc.GetIsBigData(),
			ResultType:           convertToKnowledgeResultType(doc.GetResultType()),
			SheetInfo:            doc.GetSheetInfo(),
			SimilarQuestionExtra: convertToKnowledgeSimilarQuestionExtra(doc.GetSimilarQuestionExtra()),
		})
	}
	return knowledgeDocs
}

// convertReleaseDocToKnowledgeDoc 转换ReleaseDoc类型
func convertReleaseDocToKnowledgeDoc(docs []*pb.SearchRsp_Doc) []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			DocId:                doc.GetDocId(),
			DocType:              doc.GetDocType(),
			RelatedId:            doc.GetRelatedId(),
			Question:             doc.GetQuestion(),
			Answer:               doc.GetAnswer(),
			Confidence:           doc.GetConfidence(),
			OrgData:              doc.GetOrgData(),
			RelatedBizId:         doc.GetRelatedBizId(),
			QuestionPlaceholders: convertToKnowledgePlaceholders(doc.GetQuestionPlaceholders()),
			AnswerPlaceholders:   convertToKnowledgePlaceholders(doc.GetAnswerPlaceholders()),
			OrgDataPlaceholders:  convertToKnowledgePlaceholders(doc.GetOrgDataPlaceholders()),
			CustomParam:          doc.GetCustomParam(),
			QuestionDesc:         doc.GetQuestionDesc(),
			Extra:                convertToKnowledgeExtra(doc.GetExtra()),
			ImageUrls:            doc.GetImageUrls(),
			IsBigData:            doc.GetIsBigData(),
			ResultType:           convertToKnowledgeResultType(doc.GetResultType()),
			SheetInfo:            doc.GetSheetInfo(),
			SimilarQuestionExtra: convertToKnowledgeSimilarQuestionExtra(doc.GetSimilarQuestionExtra()),
		})
	}
	return knowledgeDocs
}

// searchRejectQuestion 拒答
func (s *Service) searchRejectQuestion(ctx context.Context, req *knowledge.SearchKnowledgeReq, app *model.App) (
	rsp *knowledge.SearchKnowledgeRsp, err error) {
	log.InfoContextf(ctx, "searchRejectQuestion, req: %+v", req)
	switch req.GetSceneType() {
	case knowledge.SceneType_TEST:
		pbReq := &pb.SearchPreviewRejectedQuestionReq{
			BotBizId:     req.GetReq().GetBotBizId(),
			Question:     req.GetReq().GetQuestion(),
			SubQuestions: req.GetReq().GetSubQuestions(),
			ModelName:    req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchPreviewRejectedQuestion(ctx, pbReq)
		if err != nil {
			log.ErrorContextf(ctx, "searchRejectQuestion failed, err: %+v", err)
			return nil, err
		}
		return &knowledge.SearchKnowledgeRsp{
			KnowledgeType: knowledge.KnowledgeType_REJECTED_QUESTION,
			SceneType:     knowledge.SceneType_TEST,
			Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
				Docs: convertPreviewRejectedQueToKnowledgeRejectedQue(pbRsp.GetList()),
			},
		}, nil
	case knowledge.SceneType_PROD:
		pbReq := &pb.SearchReleaseRejectedQuestionReq{
			BotBizId:     req.GetReq().GetBotBizId(),
			Question:     req.GetReq().GetQuestion(),
			SubQuestions: req.GetReq().GetSubQuestions(),
			ModelName:    req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchReleaseRejectedQuestion(ctx, pbReq)
		if err != nil {
			log.ErrorContextf(ctx, "searchRejectQuestion failed, err: %+v", err)
			return nil, err
		}
		return &knowledge.SearchKnowledgeRsp{
			KnowledgeType: knowledge.KnowledgeType_REJECTED_QUESTION,
			SceneType:     knowledge.SceneType_PROD,
			Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
				Docs: convertReleaseRejectedQueToKnowledgeRejectedQue(pbRsp.GetList()),
			},
		}, nil
	default:
		err = fmt.Errorf("searchRejectQuestion SceneType:%+v illegal", req.GetSceneType())
	}
	return nil, err
}

// convertPreviewRejectedQueToKnowledgeRejectedQue 转换PreviewRejectedQuestion类型
func convertPreviewRejectedQueToKnowledgeRejectedQue(
	rejectedQues []*pb.SearchPreviewRejectedQuestionRsp_RejectedQuestions) (
	knowledgeRejectedQues []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc) {
	knowledgeRejectedQues = make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, rejectedQue := range rejectedQues {
		knowledgeRejectedQues = append(knowledgeRejectedQues, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			RelatedId:  rejectedQue.GetId(),
			Question:   rejectedQue.GetQuestion(),
			Confidence: rejectedQue.GetConfidence(),
		})
	}
	return knowledgeRejectedQues
}

// convertReleaseRejectedQueToKnowledgeRejectedQue 转换PreviewRejectedQuestion类型
func convertReleaseRejectedQueToKnowledgeRejectedQue(
	rejectedQues []*pb.SearchReleaseRejectedQuestionRsp_RejectedQuestions) (
	knowledgeRejectedQues []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc) {
	knowledgeRejectedQues = make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, rejectedQue := range rejectedQues {
		knowledgeRejectedQues = append(knowledgeRejectedQues, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			RelatedId:  rejectedQue.GetId(),
			Question:   rejectedQue.GetQuestion(),
			Confidence: rejectedQue.GetConfidence(),
		})
	}
	return knowledgeRejectedQues
}

// searchRealtime 实时文档
func (s *Service) searchRealtime(ctx context.Context, req *knowledge.SearchKnowledgeReq, app *model.App) (
	rsp *knowledge.SearchKnowledgeRsp, err error) {
	log.InfoContextf(ctx, "searchRealtime, req: %+v", req)
	switch req.GetSceneType() {
	case knowledge.SceneType_TEST:
		pbReq := &knowledge.SearchRealtimeReq{
			BotBizId:       req.GetReq().GetBotBizId(),
			Question:       req.GetReq().GetQuestion(),
			FilterKey:      model.AppSearchRealtimePreviewFilterKey,
			Labels:         req.GetReq().GetLabels(),
			UsePlaceholder: req.GetReq().GetUsePlaceholder(),
			ImageUrls:      req.GetReq().GetImageUrls(),
			SubQuestions:   req.GetReq().GetSubQuestions(),
			ModelName:      req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchRealtime(ctx, pbReq)
		if err != nil {
			log.ErrorContextf(ctx, "searchRealtime failed, err: %+v", err)
			return nil, err
		}
		return &knowledge.SearchKnowledgeRsp{
			KnowledgeType: knowledge.KnowledgeType_REALTIME,
			SceneType:     knowledge.SceneType_TEST,
			Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
				Docs: convertRealtimeDocToKnowledgeDoc(pbRsp.GetDocs()),
			},
		}, nil
	case knowledge.SceneType_PROD:
		pbReq := &knowledge.SearchRealtimeReq{
			BotBizId:       req.GetReq().GetBotBizId(),
			Question:       req.GetReq().GetQuestion(),
			FilterKey:      model.AppSearchRealtimeReleaseFilterKey,
			Labels:         req.GetReq().GetLabels(),
			UsePlaceholder: req.GetReq().GetUsePlaceholder(),
			ImageUrls:      req.GetReq().GetImageUrls(),
			SubQuestions:   req.GetReq().GetSubQuestions(),
			ModelName:      req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchRealtime(ctx, pbReq)
		if err != nil {
			log.ErrorContextf(ctx, "searchRealtime failed, err: %+v", err)
			return nil, err
		}
		return &knowledge.SearchKnowledgeRsp{
			KnowledgeType: knowledge.KnowledgeType_REALTIME,
			SceneType:     knowledge.SceneType_PROD,
			Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
				Docs: convertRealtimeDocToKnowledgeDoc(pbRsp.GetDocs()),
			},
		}, nil
	default:
		err = fmt.Errorf("searchRealtime SceneType:%+v illegal", req.GetSceneType())
	}
	return nil, err
}

// convertRealtimeDocToKnowledgeDoc 转换PreviewDoc类型
func convertRealtimeDocToKnowledgeDoc(
	docs []*knowledge.SearchRealtimeRsp_Doc) []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			DocId:                doc.GetDocId(),
			DocType:              doc.GetDocType(),
			RelatedId:            doc.GetRelatedId(),
			Question:             doc.GetQuestion(),
			Answer:               doc.GetAnswer(),
			Confidence:           doc.GetConfidence(),
			OrgData:              doc.GetOrgData(),
			RelatedBizId:         doc.GetRelatedBizId(),
			QuestionPlaceholders: doc.GetQuestionPlaceholders(),
			AnswerPlaceholders:   doc.GetAnswerPlaceholders(),
			OrgDataPlaceholders:  doc.GetOrgDataPlaceholders(),
			CustomParam:          doc.GetCustomParam(),
			Extra:                doc.GetExtra(),
			ImageUrls:            doc.GetImageUrls(),
			IsBigData:            doc.GetIsBigData(),
			ResultType:           doc.GetResultType(),
		})
	}
	return knowledgeDocs
}

// searchWorkflow 工作流程
func (s *Service) searchWorkflow(ctx context.Context, req *knowledge.SearchKnowledgeReq, app *model.App) (
	rsp *knowledge.SearchKnowledgeRsp, err error) {
	log.InfoContextf(ctx, "searchWorkflow, req: %+v", req)
	if req.GetReq() == nil {
		return nil, errs.ErrSystem
	}
	// isearch第三方权限校验特殊逻辑，需要将label中的指定attrkey转为custom_variables中的lke_userid
	// custom_variables中的lke_userid会用来做最后结果的第三方权限校验
	thirdPermissionConfig, ok := utilConfig.GetMainConfig().ThirdPermissionCheck[req.GetReq().GetBotBizId()]
	if ok && thirdPermissionConfig.Enable && thirdPermissionConfig.WorkFlowLkeUserIdAttrKey != "" {
		customVariables := make(map[string]string)
		newLabel := make([]*knowledge.VectorLabel, 0, len(req.GetReq().GetLabels()))
		for _, label := range req.GetReq().GetLabels() {
			if label.Name == thirdPermissionConfig.WorkFlowLkeUserIdAttrKey {
				if len(label.Values) > 0 {
					customVariables[logicSearch.CustomVariableKeyLkeUserId] = label.Values[0]
				}
			} else {
				newLabel = append(newLabel, label)
			}
		}
		req.GetReq().CustomVariables = customVariables
		req.GetReq().Labels = newLabel
		log.InfoContextf(ctx, "searchWorkflow, req: %+v", req)
	}
	pbLabelConfig := &model.CustomLabelConfig{
		LabelLogicOpr:    getLabelLogicOpr(req.GetReq().GetWorkflowSearchExtraParam().GetLabelLogicOpr()),
		IsLabelOrGeneral: req.GetReq().GetWorkflowSearchExtraParam().GetIsLabelOrGeneral(),
		SearchStrategy:   req.GetReq().GetWorkflowSearchExtraParam().GetSearchStrategy(),
	}
	switch req.GetSceneType() {
	case knowledge.SceneType_TEST:
		pbReq := &pb.CustomSearchPreviewReq{
			BotBizId:       req.GetReq().GetBotBizId(),
			Question:       req.GetReq().GetQuestion(),
			Filters:        convertToPbPreviewFilters(req.GetReq().GetWorkflowSearchExtraParam().GetFilters()),
			TopN:           req.GetReq().GetWorkflowSearchExtraParam().GetTopN(),
			Labels:         convertToPbLabels(req.GetReq().GetLabels()),
			UsePlaceholder: req.GetReq().GetUsePlaceholder(),
			SubQuestions:   req.GetReq().GetSubQuestions(),
			ModelName:      req.GetReq().GetModelName(),
		}
		pbRsp, err := s.CustomSearchPreviewWithLabelConfig(ctx, pbReq, pbLabelConfig)
		if err != nil {
			log.ErrorContextf(ctx, "searchWorkflow failed, err: %+v", err)
			return nil, err
		}
		return &knowledge.SearchKnowledgeRsp{
			KnowledgeType: knowledge.KnowledgeType_WORKFLOW,
			SceneType:     knowledge.SceneType_TEST,
			Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
				Docs: convertCustomPreviewDocToKnowledgeDoc(pbRsp.GetDocs()),
			},
		}, nil
	case knowledge.SceneType_PROD:
		pbReq := &pb.CustomSearchReq{
			BotBizId:       req.GetReq().GetBotBizId(),
			Question:       req.GetReq().GetQuestion(),
			Filters:        convertToPbReleaseFilters(req.GetReq().GetWorkflowSearchExtraParam().GetFilters()),
			TopN:           req.GetReq().GetWorkflowSearchExtraParam().GetTopN(),
			Labels:         convertToPbLabels(req.GetReq().GetLabels()),
			UsePlaceholder: req.GetReq().GetUsePlaceholder(),
			SubQuestions:   req.GetReq().GetSubQuestions(),
			ModelName:      req.GetReq().GetModelName(),
		}
		pbRsp, err := s.CustomSearchWithLabelConfig(ctx, pbReq, pbLabelConfig)
		if err != nil {
			log.ErrorContextf(ctx, "searchWorkflow failed, err: %+v", err)
			return nil, err
		}
		return &knowledge.SearchKnowledgeRsp{
			KnowledgeType: knowledge.KnowledgeType_WORKFLOW,
			SceneType:     knowledge.SceneType_PROD,
			Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
				Docs: convertCustomReleaseDocToKnowledgeDoc(pbRsp.GetDocs()),
			},
		}, nil
	default:
		err = fmt.Errorf("searchWorkflow SceneType:%+v illegal", req.GetSceneType())
	}
	return nil, err
}

// convertToPbPreviewFilters 转换Filter类型
func convertToPbPreviewFilters(
	filters []*knowledge.WorkflowSearchExtraParam_Filter) []*pb.CustomSearchPreviewReq_Filter {
	pbFilters := make([]*pb.CustomSearchPreviewReq_Filter, 0)
	for _, filter := range filters {
		pbFilters = append(pbFilters, &pb.CustomSearchPreviewReq_Filter{
			DocType:    filter.GetDocType(),
			Confidence: filter.GetConfidence(),
			TopN:       filter.GetTopN(),
		})
	}
	return pbFilters
}

// convertToPbReleaseFilters 转换Filter类型
func convertToPbReleaseFilters(
	filters []*knowledge.WorkflowSearchExtraParam_Filter) []*pb.CustomSearchReq_Filter {
	pbFilters := make([]*pb.CustomSearchReq_Filter, 0)
	for _, filter := range filters {
		pbFilters = append(pbFilters, &pb.CustomSearchReq_Filter{
			DocType:    filter.GetDocType(),
			Confidence: filter.GetConfidence(),
			TopN:       filter.GetTopN(),
		})
	}
	return pbFilters
}

// getLabelLogicOpr 获取标签检索条件
func getLabelLogicOpr(opr knowledge.LogicOpr) string {
	switch opr {
	case knowledge.LogicOpr_AND:
		return model.AppSearchConditionAnd
	case knowledge.LogicOpr_OR:
		return model.AppSearchConditionOr
	default:
		return model.AppSearchConditionAnd
	}
}

// convertCustomPreviewDocToKnowledgeDoc 转换CustomPreviewDoc类型
func convertCustomPreviewDocToKnowledgeDoc(
	docs []*pb.CustomSearchPreviewRsp_Doc) []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			DocId:                doc.GetDocId(),
			DocType:              doc.GetDocType(),
			RelatedId:            doc.GetRelatedId(),
			Question:             doc.GetQuestion(),
			Answer:               doc.GetAnswer(),
			Confidence:           doc.GetConfidence(),
			OrgData:              doc.GetOrgData(),
			RelatedBizId:         doc.GetRelatedBizId(),
			QuestionPlaceholders: convertToKnowledgePlaceholders(doc.GetQuestionPlaceholders()),
			AnswerPlaceholders:   convertToKnowledgePlaceholders(doc.GetAnswerPlaceholders()),
			OrgDataPlaceholders:  convertToKnowledgePlaceholders(doc.GetOrgDataPlaceholders()),
			CustomParam:          doc.GetCustomParam(),
			QuestionDesc:         doc.GetQuestionDesc(),
			Extra:                convertToKnowledgeExtra(doc.GetExtra()),
			ImageUrls:            doc.GetImageUrls(),
			IsBigData:            doc.GetIsBigData(),
			ResultType:           convertToKnowledgeResultType(doc.GetResultType()),
			SheetInfo:            doc.GetSheetInfo(),
			SimilarQuestionExtra: convertToKnowledgeSimilarQuestionExtra(doc.GetSimilarQuestionExtra()),
		})
	}
	return knowledgeDocs
}

// convertCustomReleaseDocToKnowledgeDoc 转换CustomReleaseDoc类型
func convertCustomReleaseDocToKnowledgeDoc(
	docs []*pb.CustomSearchRsp_Doc) []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*knowledge.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
			DocId:                doc.GetDocId(),
			DocType:              doc.GetDocType(),
			RelatedId:            doc.GetRelatedId(),
			Question:             doc.GetQuestion(),
			Answer:               doc.GetAnswer(),
			Confidence:           doc.GetConfidence(),
			OrgData:              doc.GetOrgData(),
			RelatedBizId:         doc.GetRelatedBizId(),
			QuestionPlaceholders: convertToKnowledgePlaceholders(doc.GetQuestionPlaceholders()),
			AnswerPlaceholders:   convertToKnowledgePlaceholders(doc.GetAnswerPlaceholders()),
			OrgDataPlaceholders:  convertToKnowledgePlaceholders(doc.GetOrgDataPlaceholders()),
			CustomParam:          doc.GetCustomParam(),
			QuestionDesc:         doc.GetQuestionDesc(),
			Extra:                convertToKnowledgeExtra(doc.GetExtra()),
			ImageUrls:            doc.GetImageUrls(),
			IsBigData:            doc.GetIsBigData(),
			ResultType:           convertToKnowledgeResultType(doc.GetResultType()),
			SheetInfo:            doc.GetSheetInfo(),
			SimilarQuestionExtra: convertToKnowledgeSimilarQuestionExtra(doc.GetSimilarQuestionExtra()),
		})
	}
	return knowledgeDocs
}

// convertToPbLabels 转换Label类型
func convertToPbLabels(labels []*knowledge.VectorLabel) []*pb.VectorLabel {
	pbLabels := make([]*pb.VectorLabel, 0)
	for _, label := range labels {
		pbLabels = append(pbLabels, &pb.VectorLabel{
			Name:   label.GetName(),
			Values: label.GetValues(),
		})
	}
	return pbLabels
}

// convertToKnowledgePlaceholders 转换Placeholder类型
func convertToKnowledgePlaceholders(placeholders []*pb.Placeholder) []*knowledge.Placeholder {
	knowledgePlaceholders := make([]*knowledge.Placeholder, 0)
	for _, placeholder := range placeholders {
		knowledgePlaceholders = append(knowledgePlaceholders, &knowledge.Placeholder{
			Key:   placeholder.GetKey(),
			Value: placeholder.GetValue(),
		})
	}
	return knowledgePlaceholders
}

// convertToKnowledgeExtra 转换Extra类型
func convertToKnowledgeExtra(extra *pb.RetrievalExtra) *knowledge.RetrievalExtra {
	return &knowledge.RetrievalExtra{
		EmbRank:     extra.GetEmbRank(),
		EsScore:     extra.GetEsScore(),
		EsRank:      extra.GetEsRank(),
		RerankScore: extra.GetRerankScore(),
		RerankRank:  extra.GetRerankRank(),
		RrfScore:    extra.GetRrfScore(),
		RrfRank:     extra.GetRrfRank(),
	}
}

// convertToKnowledgeResultType 转换ResultType类型
func convertToKnowledgeResultType(resultType pb.RetrievalResultType) knowledge.RetrievalResultType {
	return knowledge.RetrievalResultType(resultType.Number())
}

// convertToKnowledgeSimilarQuestionExtra 转换SimilarQuestionExtra类型
func convertToKnowledgeSimilarQuestionExtra(
	similarQuestionExtra *pb.SimilarQuestionExtra) *knowledge.SimilarQuestionExtra {
	return &knowledge.SimilarQuestionExtra{
		SimilarId:       similarQuestionExtra.GetSimilarId(),
		SimilarQuestion: similarQuestionExtra.GetSimilarQuestion(),
	}
}
