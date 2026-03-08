package api

import (
	"context"
	"fmt"
	"git.woa.com/adp/kb/kb-config/internal/util"

	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/label"
	logicSearch "git.woa.com/adp/kb/kb-config/internal/logic/search"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// NOTE:(ericjwang): 检索方法汇总（梳理）
func (s *Service) searchMethods() {
	// ====== search.go
	_ = s.SearchKnowledge      // 知识库检索，主要接口
	_ = s.SearchKnowledgeBatch // 知识库检索（支持检索多个知识库）

	// ====== realtime.go
	_ = s.SearchRealtime // 实时文档检索

	// ====== vector.go
	_ = s.CustomSearch // 自定义对话评测，prod环境使用【op检索用】，调用 CustomSearchWithLabelConfig
	_ = s.CustomSearchWithLabelConfig
	_ = s.CustomSearchPreview // 自定义对话评测查询【op检索用】, 调用 CustomSearchPreviewWithLabelConfig
	_ = s.CustomSearchPreviewWithLabelConfig

	_ = s.SearchPreview                    // 已经删除，但原子能力在调用
	_ = s.SearchPreviewWithCustomVariables // 没有调用

	_ = s.SearchKnowledgeRelease // 本质是调用 Search
	_ = s.Search                 // 包装请求 SearchWithCustomVariables
	_ = s.SearchWithCustomVariables

	_ = s.SearchPreviewRejectedQuestion // RPC 不用了，但是还在被 SearchKnowledge 内部调用
	_ = s.SearchReleaseRejectedQuestion // RPC 不用了，但是还在被 SearchKnowledge 内部调用
}

// SearchKnowledge 知识库检索
func (s *Service) SearchKnowledge(ctx context.Context, req *pb.SearchKnowledgeReq) (rsp *pb.SearchKnowledgeRsp, err error) {
	logx.I(ctx, "SearchKnowledge called, req: %+v", req)
	if err = s.checkSearchKnowledgeReq(ctx, req); err != nil {
		return nil, err
	}
	appid := convx.Uint64ToString(req.GetReq().GetBotBizId())
	app, err := s.svc.DescribeAppAndCheckCorp(ctx, appid)
	if err != nil || app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)

	switch req.KnowledgeType {
	case pb.SearchKnowledgeType_GLOBAL_KNOWLEDGE:
		// 全局知识库入口已经废弃，不检索也不返回 doc 数据
		rsp = &pb.SearchKnowledgeRsp{KnowledgeType: pb.SearchKnowledgeType_GLOBAL_KNOWLEDGE, SceneType: req.SceneType, Rsp: &pb.SearchKnowledgeRsp_SearchRsp{}}
	case pb.SearchKnowledgeType_DOC_QA, pb.SearchKnowledgeType_WORKFLOW:
		var bc logicSearch.BotContext
		err = bc.Init(newCtx, s.rpc, nil, req, s.kbDao, s.labelDao, s.releaseDao, s.cateLogic, s.cacheLogic, s.userLogic, s.kbLogic)
		if err != nil {
			logx.E(ctx, "SearchKnowledge Init failed, err: %+v", err)
			return nil, err
		}
		if bc.RoleNotAllowedSearch {
			logx.W(ctx, "Role not allow Search")
			return rsp, nil
		}
		rsp, err = s.searchAnswer(newCtx, &bc)
	case pb.SearchKnowledgeType_REJECTED_QUESTION:
		rsp, err = s.searchRejectQuestion(newCtx, req)
	case pb.SearchKnowledgeType_REALTIME:
		rsp, err = s.searchRealtime(newCtx, req)
	default:
		err = fmt.Errorf("SearchKnowledge KnowledgeType:%+v illegal", req.KnowledgeType)
	}
	if err != nil {
		logx.E(ctx, "SearchKnowledge failed, err: %+v", err)
		return nil, err
	}
	logx.I(ctx, "SearchKnowledge called, rsp: %+v", rsp)
	return rsp, nil
}

// searchKnowledgeBatch 知识库检索（支持检索多个知识库）
func (s *Service) searchKnowledgeBatch(ctx context.Context, req *pb.SearchKnowledgeBatchReq) (*entity.App, *pb.SearchKnowledgeRsp, error) {
	rsp := &pb.SearchKnowledgeRsp{}
	logx.I(ctx, "searchKnowledgeBatch called, req: %+v", req)

	scene := uint32(req.GetSceneType())
	appBizId := convx.Uint64ToString(req.GetAppBizId())
	app, err := s.svc.DescribeAppBySceneAndCheckCorp(ctx, appBizId, scene)
	if err != nil {
		return nil, nil, err
	}
	ctx = entity.ContextWithApp(ctx, scene, app)
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)

	var bc logicSearch.BotContext
	err = bc.Init(newCtx, s.rpc, req, nil, s.kbDao, s.labelDao, s.releaseDao, s.cateLogic, s.cacheLogic, s.userLogic, s.kbLogic)
	if err != nil {
		logx.E(ctx, "searchKnowledgeBatch Init failed, err: %+v", err)
		return nil, nil, err
	}
	if bc.RoleNotAllowedSearch {
		logx.W(ctx, "Role not allow Search")
		return app, rsp, nil
	}
	switch req.KnowledgeType { // 拒答，实时文档，全局知识库，不存在同时检索多个知识库，这里不做处理
	case pb.SearchKnowledgeType_DOC_QA, pb.SearchKnowledgeType_WORKFLOW:
		if dbTableID := bc.GetDBTableID(); dbTableID > 0 {
			rsp, err = s.searchDBAnswer(newCtx, &bc, dbTableID)
		} else {
			rsp, err = s.searchAnswer(newCtx, &bc)
		}
	default:
		err = fmt.Errorf("searchKnowledgeBatch KnowledgeType:%+v illegal", req.KnowledgeType)
	}
	if err != nil {
		logx.E(ctx, "searchKnowledgeBatch failed, err: %+v", err)
		return nil, nil, err
	}

	logx.I(ctx, "searchKnowledgeBatch called, rsp: %+v", rsp)
	return app, rsp, nil
}

func (s *Service) SearchKnowledgeBatch(ctx context.Context, req *pb.SearchKnowledgeBatchReq) (*pb.SearchKnowledgeRsp, error) {
	_, rsp, err := s.searchKnowledgeBatch(ctx, req)
	return rsp, err
}

// checkSearchKnowledgeReq 校验知识库检索Req
func (s *Service) checkSearchKnowledgeReq(ctx context.Context, req *pb.SearchKnowledgeReq) error {
	// 检查标签
	for _, label := range req.GetReq().GetLabels() {
		if label == nil {
			req.Req.Labels = nil
			logx.W(ctx, "SearchKnowledge label is nil, req: %+v", req)
			break
		}
		if len(label.GetName()) == 0 || len(label.GetValues()) == 0 {
			// 降级处理，删除该标签，不报错
			req.Req.Labels = nil
			logx.W(ctx, "SearchKnowledge label is nil, req: %+v", req)
			break
		}
		for _, v := range label.GetValues() {
			if len(v) == 0 {
				req.Req.Labels = nil
				logx.W(ctx, "SearchKnowledge label is nil, req: %+v", req)
				break
			}
		}
	}
	return nil
}

// convertPreviewDocToKnowledgeDoc 转换PreviewDoc类型
func convertPreviewDocToKnowledgeDoc(docs []*pb.SearchPreviewRsp_Doc) []*pb.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &pb.SearchKnowledgeRsp_SearchRsp_Doc{
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
func convertReleaseDocToKnowledgeDoc(docs []*pb.SearchRsp_Doc) []*pb.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &pb.SearchKnowledgeRsp_SearchRsp_Doc{
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
func (s *Service) searchRejectQuestion(ctx context.Context, req *pb.SearchKnowledgeReq) (rsp *pb.SearchKnowledgeRsp, err error) {
	logx.I(ctx, "searchRejectQuestion, req: %+v", req)
	switch req.GetSceneType() {
	case pb.SceneType_TEST:
		pbReq := &pb.SearchPreviewRejectedQuestionReq{
			BotBizId:     req.GetReq().GetBotBizId(),
			Question:     req.GetReq().GetQuestion(),
			SubQuestions: req.GetReq().GetSubQuestions(),
			ModelName:    req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchPreviewRejectedQuestion(ctx, pbReq)
		if err != nil {
			logx.E(ctx, "searchRejectQuestion failed, err: %+v", err)
			return nil, err
		}
		return &pb.SearchKnowledgeRsp{
			KnowledgeType: pb.SearchKnowledgeType_REJECTED_QUESTION,
			SceneType:     pb.SceneType_TEST,
			Rsp: &pb.SearchKnowledgeRsp_SearchRsp{
				Docs: convertPreviewRejectedQueToKnowledgeRejectedQue(pbRsp.GetList()),
			},
		}, nil
	case pb.SceneType_PROD:
		pbReq := &pb.SearchReleaseRejectedQuestionReq{
			BotBizId:     req.GetReq().GetBotBizId(),
			Question:     req.GetReq().GetQuestion(),
			SubQuestions: req.GetReq().GetSubQuestions(),
			ModelName:    req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchReleaseRejectedQuestion(ctx, pbReq)
		if err != nil {
			logx.E(ctx, "searchRejectQuestion failed, err: %+v", err)
			return nil, err
		}
		return &pb.SearchKnowledgeRsp{
			KnowledgeType: pb.SearchKnowledgeType_REJECTED_QUESTION,
			SceneType:     pb.SceneType_PROD,
			Rsp: &pb.SearchKnowledgeRsp_SearchRsp{
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
	knowledgeRejectedQues []*pb.SearchKnowledgeRsp_SearchRsp_Doc) {
	knowledgeRejectedQues = make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, rejectedQue := range rejectedQues {
		knowledgeRejectedQues = append(knowledgeRejectedQues, &pb.SearchKnowledgeRsp_SearchRsp_Doc{
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
	knowledgeRejectedQues []*pb.SearchKnowledgeRsp_SearchRsp_Doc) {
	knowledgeRejectedQues = make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, rejectedQue := range rejectedQues {
		knowledgeRejectedQues = append(knowledgeRejectedQues, &pb.SearchKnowledgeRsp_SearchRsp_Doc{
			RelatedId:  rejectedQue.GetId(),
			Question:   rejectedQue.GetQuestion(),
			Confidence: rejectedQue.GetConfidence(),
		})
	}
	return knowledgeRejectedQues
}

// searchRealtime 实时文档
func (s *Service) searchRealtime(ctx context.Context, req *pb.SearchKnowledgeReq) (rsp *pb.SearchKnowledgeRsp, err error) {
	logx.I(ctx, "searchRealtime, req: %+v", req)
	switch req.GetSceneType() {
	case pb.SceneType_TEST:
		pbReq := &pb.SearchRealtimeReq{
			BotBizId:       req.GetReq().GetBotBizId(),
			Question:       req.GetReq().GetQuestion(),
			FilterKey:      entity.AppSearchRealtimePreviewFilterKey,
			Labels:         req.GetReq().GetLabels(),
			UsePlaceholder: req.GetReq().GetUsePlaceholder(),
			ImageUrls:      req.GetReq().GetImageUrls(),
			SubQuestions:   req.GetReq().GetSubQuestions(),
			ModelName:      req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchRealtime(ctx, pbReq)
		if err != nil {
			logx.E(ctx, "searchRealtime failed, err: %+v", err)
			return nil, err
		}
		return &pb.SearchKnowledgeRsp{
			KnowledgeType: pb.SearchKnowledgeType_REALTIME,
			SceneType:     pb.SceneType_TEST,
			Rsp: &pb.SearchKnowledgeRsp_SearchRsp{
				Docs: convertRealtimeDocToKnowledgeDoc(pbRsp.GetDocs()),
			},
		}, nil
	case pb.SceneType_PROD:
		pbReq := &pb.SearchRealtimeReq{
			BotBizId:       req.GetReq().GetBotBizId(),
			Question:       req.GetReq().GetQuestion(),
			FilterKey:      entity.AppSearchRealtimeReleaseFilterKey,
			Labels:         req.GetReq().GetLabels(),
			UsePlaceholder: req.GetReq().GetUsePlaceholder(),
			ImageUrls:      req.GetReq().GetImageUrls(),
			SubQuestions:   req.GetReq().GetSubQuestions(),
			ModelName:      req.GetReq().GetModelName(),
		}
		pbRsp, err := s.SearchRealtime(ctx, pbReq)
		if err != nil {
			logx.E(ctx, "searchRealtime failed, err: %+v", err)
			return nil, err
		}
		return &pb.SearchKnowledgeRsp{
			KnowledgeType: pb.SearchKnowledgeType_REALTIME,
			SceneType:     pb.SceneType_PROD,
			Rsp: &pb.SearchKnowledgeRsp_SearchRsp{
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
	docs []*pb.SearchRealtimeRsp_Doc) []*pb.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &pb.SearchKnowledgeRsp_SearchRsp_Doc{
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
func (s *Service) searchWorkflow(ctx context.Context, req *pb.SearchKnowledgeReq) (
	rsp *pb.SearchKnowledgeRsp, err error) {
	logx.I(ctx, "searchWorkflow, req: %+v", req)
	if req.GetReq() == nil {
		return nil, errs.ErrSystem
	}
	// isearch第三方权限校验特殊逻辑，需要将label中的指定attrkey转为custom_variables中的lke_userid
	// custom_variables中的lke_userid会用来做最后结果的第三方权限校验
	thirdPermissionConfig, ok := config.GetMainConfig().ThirdPermissionCheck[req.GetReq().GetBotBizId()]
	if ok && thirdPermissionConfig.Enable && thirdPermissionConfig.WorkFlowLkeUserIdAttrKey != "" {
		customVariables := make(map[string]string)
		newLabel := make([]*pb.VectorLabel, 0, len(req.GetReq().GetLabels()))
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
		logx.I(ctx, "searchWorkflow, req: %+v", req)
	}
	pbLabelConfig := &label.CustomLabelConfig{
		LabelLogicOpr:    getLabelLogicOpr(req.GetReq().GetWorkflowSearchExtraParam().GetLabelLogicOpr()),
		IsLabelOrGeneral: req.GetReq().GetWorkflowSearchExtraParam().GetIsLabelOrGeneral(),
		SearchStrategy:   req.GetReq().GetWorkflowSearchExtraParam().GetSearchStrategy(),
	}
	switch req.GetSceneType() {
	case pb.SceneType_TEST:
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
			logx.E(ctx, "searchWorkflow failed, err: %+v", err)
			return nil, err
		}
		return &pb.SearchKnowledgeRsp{
			KnowledgeType: pb.SearchKnowledgeType_WORKFLOW,
			SceneType:     pb.SceneType_TEST,
			Rsp: &pb.SearchKnowledgeRsp_SearchRsp{
				Docs: convertCustomPreviewDocToKnowledgeDoc(pbRsp.GetDocs()),
			},
		}, nil
	case pb.SceneType_PROD:
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
			logx.E(ctx, "searchWorkflow failed, err: %+v", err)
			return nil, err
		}
		return &pb.SearchKnowledgeRsp{
			KnowledgeType: pb.SearchKnowledgeType_WORKFLOW,
			SceneType:     pb.SceneType_PROD,
			Rsp: &pb.SearchKnowledgeRsp_SearchRsp{
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
	filters []*pb.WorkflowSearchExtraParam_Filter) []*pb.CustomSearchPreviewReq_Filter {
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
	filters []*pb.WorkflowSearchExtraParam_Filter) []*pb.CustomSearchReq_Filter {
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

// convertCustomPreviewDocToKnowledgeDoc 转换CustomPreviewDoc类型
func convertCustomPreviewDocToKnowledgeDoc(
	docs []*pb.CustomSearchPreviewRsp_Doc) []*pb.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &pb.SearchKnowledgeRsp_SearchRsp_Doc{
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
	docs []*pb.CustomSearchRsp_Doc) []*pb.SearchKnowledgeRsp_SearchRsp_Doc {
	knowledgeDocs := make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range docs {
		knowledgeDocs = append(knowledgeDocs, &pb.SearchKnowledgeRsp_SearchRsp_Doc{
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
func convertToPbLabels(labels []*pb.VectorLabel) []*pb.VectorLabel {
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
func convertToKnowledgePlaceholders(placeholders []*pb.Placeholder) []*pb.Placeholder {
	knowledgePlaceholders := make([]*pb.Placeholder, 0)
	for _, placeholder := range placeholders {
		knowledgePlaceholders = append(knowledgePlaceholders, &pb.Placeholder{
			Key:   placeholder.GetKey(),
			Value: placeholder.GetValue(),
		})
	}
	return knowledgePlaceholders
}

// convertToKnowledgeExtra 转换Extra类型
func convertToKnowledgeExtra(extra *pb.RetrievalExtra) *pb.RetrievalExtra {
	return &pb.RetrievalExtra{
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
func convertToKnowledgeResultType(resultType pb.RetrievalResultType) pb.RetrievalResultType {
	return pb.RetrievalResultType(resultType.Number())
}

// convertToKnowledgeSimilarQuestionExtra 转换SimilarQuestionExtra类型
func convertToKnowledgeSimilarQuestionExtra(
	similarQuestionExtra *pb.SimilarQuestionExtra) *pb.SimilarQuestionExtra {
	return &pb.SimilarQuestionExtra{
		SimilarId:       similarQuestionExtra.GetSimilarId(),
		SimilarQuestion: similarQuestionExtra.GetSimilarQuestion(),
	}
}
