package api

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/data_statistics"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/search"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"

	"golang.org/x/exp/maps"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	logicSearch "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/search"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utils2 "git.woa.com/dialogue-platform/common/v3/utils"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/linker"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/go-comm/clues"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	knowPB "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// CustomSearchPreview 自定义对话评测查询【op检索用】
func (s *Service) CustomSearchPreview(
	ctx context.Context, req *pb.CustomSearchPreviewReq,
) (*pb.CustomSearchPreviewRsp, error) {
	return s.CustomSearchPreviewWithLabelConfig(ctx, req, nil)
}

// CustomSearchPreviewWithLabelConfig 自定义对话评测查询
func (s *Service) CustomSearchPreviewWithLabelConfig(
	ctx context.Context, req *pb.CustomSearchPreviewReq,
	labelConfig *model.CustomLabelConfig,
) (*pb.CustomSearchPreviewRsp, error) {
	rsp := new(pb.CustomSearchPreviewRsp)
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppTestScenes)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	replaceApp := app
	if newAppID, ok := utilConfig.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.GetAppBizId()]; ok {
		replaceApp, err = client.GetAppInfo(ctx, newAppID, model.AppTestScenes)
		clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		log.DebugContextf(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v",
			app, replaceApp)
	}
	req.Labels = s.transCustomReqLabels(ctx, model.AppTestScenes, replaceApp, labelConfig, req.GetLabels()) // 标签转换
	searchVectorReq, err := s.getCustomSearchVectorReq(ctx, app, req, labelConfig, replaceApp)
	if err != nil {
		return rsp, err
	}
	searchVectorRsp, err := client.SearchVector(ctx, searchVectorReq)
	if err != nil {
		return rsp, err
	}
	linkContents, err := s.dao.GetLinkContentsFromSearchVectorResponse(
		ctx, replaceApp.GetId(), searchVectorRsp.GetDocs(),
		func(doc *retrieval.SearchVectorRsp_Doc, qa *model.DocQA) any {
			return &pb.CustomSearchPreviewRsp_Doc{
				DocId:                qa.DocID,
				DocType:              doc.GetDocType(),
				RelatedId:            doc.GetId(), // QAID
				Question:             qa.Question,
				Answer:               qa.Answer,
				CustomParam:          qa.CustomParam,
				QuestionDesc:         qa.QuestionDesc,
				Confidence:           doc.GetConfidence(),
				RelatedBizId:         qa.BusinessID,
				Extra:                convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:            doc.GetImageUrls(),
				ResultType:           convertRetrievalResultType(doc.GetResultType()),
				SimilarQuestionExtra: convertSimilarQuestionExtraForVector(doc.GetSimilarQuestionExtra()),
			}
		},
		func(doc *retrieval.SearchVectorRsp_Doc, segment *model.DocSegmentExtend) any {
			return &pb.CustomSearchPreviewRsp_Doc{
				DocId:        segment.DocID,
				DocType:      doc.GetDocType(),
				RelatedId:    doc.GetId(),
				OrgData:      doc.GetOrgData(),
				Confidence:   doc.GetConfidence(),
				RelatedBizId: segment.BusinessID,
				IsBigData:    doc.GetIsBigData(),
				Extra:        convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:    doc.GetImageUrls(),
				ResultType:   convertRetrievalResultType(doc.GetResultType()),
				SheetInfo:    convertSearchVectorRetrievalSheetInfo(doc.GetText2SqlExtra()),
			}
		},
		func(doc *retrieval.SearchVectorRsp_Doc) any {
			return &pb.CustomSearchPreviewRsp_Doc{
				DocType:    doc.GetDocType(),
				Question:   doc.GetQuestion(),
				Answer:     doc.GetAnswer(),
				Confidence: doc.GetConfidence(),
				Extra:      convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:  doc.GetImageUrls(),
				ResultType: convertRetrievalResultType(doc.GetResultType()),
			}
		},
	)
	if err != nil {
		return rsp, err
	}
	rsp.Docs = dao.Link(ctx, linkContents,
		func(t *pb.CustomSearchPreviewRsp_Doc, v linker.Content) *pb.CustomSearchPreviewRsp_Doc {
			t.OrgData = v.Value
			return t
		},
	)
	log.DebugContextf(ctx, "req: %+v rsp:%+v", req, rsp)
	return searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp), nil
}

func getRerankModel(app *admin.GetAppInfoRsp) (model.RerankModelConfig, error) {
	f, ok := app.GetKnowledgeQa().GetFilters()[model.AppRerankFilterKey]
	if !ok {
		return model.RerankModelConfig{}, fmt.Errorf("AppRerankFilterKey filter not found")
	}
	modelInfo, ok := app.GetKnowledgeQa().GetModel()[model.RerankModel]
	if !ok {
		return model.RerankModelConfig{}, fmt.Errorf("RerankModel filter not found")
	}
	return model.RerankModelConfig{
		ModelName: modelInfo.GetModelName(),
		TopN:      f.GetTopN(),
		Enable:    app.GetKnowledgeQa().GetEnableRerank(),
	}, nil
}

func (s *Service) getCustomSearchVectorReq(ctx context.Context, app *admin.GetAppInfoRsp, req *pb.CustomSearchPreviewReq,
	labelConfig *model.CustomLabelConfig, replaceApp *admin.GetAppInfoRsp) (*retrieval.SearchVectorReq, error) {
	filters := make([]*retrieval.SearchVectorReq_Filter, 0, 1)
	for _, filter := range req.GetFilters() {
		filters = append(filters, &retrieval.SearchVectorReq_Filter{
			IndexId:         model.GetType(filter.GetDocType()),
			Confidence:      filter.GetConfidence(),
			TopN:            filter.GetTopN(),
			DocType:         filter.GetDocType(),
			LabelExprString: fillLabelExprString(req.GetLabels()), // 2.6之前旧的表达式，保持逻辑不变
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return nil, err
	}
	// 工作流使用入参检索配置
	searchStrategy := getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy())
	if labelConfig != nil && labelConfig.SearchStrategy != nil {
		searchStrategy = getWorkflowSearchStrategy(labelConfig.SearchStrategy)
		log.DebugContextf(ctx, "getCustomSearchVectorReq|WorkflowSearchStrategy|StrategyType:%+v|Enhancement:%+v",
			searchStrategy.GetStrategyType(), searchStrategy.GetTableEnhancement())
	}
	return &retrieval.SearchVectorReq{
		RobotId:          replaceApp.GetId(),
		BotBizId:         replaceApp.GetAppBizId(),
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             req.GetTopN(),
		EmbeddingVersion: replaceApp.GetKnowledgeQa().GetEmbedding().GetVersion(),
		Rerank: &retrieval.SearchVectorReq_Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		// 加上FilterKey标识知识检索
		FilterKey:       model.AppSearchPreviewFilterKey,
		Labels:          convertSearchVectorLabel(req.GetLabels()),
		LabelExpression: s.transCustomLabelExpression(model.AppTestScenes, replaceApp, labelConfig, req.GetLabels()),
		SubQuestions:    req.GetSubQuestions(),
		SearchStrategy:  searchStrategy,
	}, nil
}

func (s *Service) getType(docType uint32) uint64 {
	typ := model.ReviewVersionID
	if docType == model.DocTypeSegment {
		typ = model.SegmentReviewVersionID
	} else if docType == model.DocTypeRejectedQuestion {
		typ = model.RejectedQuestionReviewVersionID
	}
	return typ
}

// CustomSearch 自定义对话评测，prod环境使用【op检索用】
func (s *Service) CustomSearch(ctx context.Context, req *pb.CustomSearchReq) (*pb.CustomSearchRsp, error) {
	return s.CustomSearchWithLabelConfig(ctx, req, nil)
}

// CustomSearchWithLabelConfig 自定义对话评测
func (s *Service) CustomSearchWithLabelConfig(ctx context.Context, req *pb.CustomSearchReq,
	labelConfig *model.CustomLabelConfig) (*pb.CustomSearchRsp, error) {
	rsp := new(pb.CustomSearchRsp)
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppReleaseScenes)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	replaceApp := app
	if newAppID, ok := utilConfig.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.GetAppBizId()]; ok {
		replaceApp, err = client.GetAppInfo(ctx, newAppID, model.AppReleaseScenes)
		clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		log.DebugContextf(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v",
			app, replaceApp)
	}
	if replaceApp.GetKnowledgeQa().GetQaVersion() == 0 {
		log.WarnContextf(ctx, "bot_biz_id:%d qaVersion is 0", replaceApp.GetAppBizId())
		return rsp, nil
	}
	req.Labels = s.transCustomReqLabels(ctx, model.AppReleaseScenes, replaceApp, labelConfig, req.GetLabels()) // 标签转换
	filters := make([]*retrieval.SearchReq_Filter, 0, len(req.GetFilters()))
	for _, filter := range req.GetFilters() {
		filters = append(filters, &retrieval.SearchReq_Filter{
			DocType:    filter.GetDocType(),
			Confidence: filter.GetConfidence(),
			TopN:       filter.GetTopN(),
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return rsp, err
	}
	// 工作流使用入参检索配置
	searchStrategy := getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy())
	if labelConfig != nil && labelConfig.SearchStrategy != nil {
		searchStrategy = getWorkflowSearchStrategy(labelConfig.SearchStrategy)
		log.DebugContextf(ctx, "getCustomSearchVectorReq|WorkflowSearchStrategy|StrategyType:%+v|Enhancement:%+v",
			searchStrategy.GetStrategyType(), searchStrategy.GetTableEnhancement())
	}
	searchReq := &retrieval.SearchReq{
		RobotId:   replaceApp.GetId(),
		VersionId: replaceApp.GetKnowledgeQa().GetQaVersion(),
		Question:  req.GetQuestion(),
		Filters:   filters,
		TopN:      req.GetTopN(),
		Rerank:    &retrieval.SearchReq_Rerank{Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable},
		// 加上FilterKey标识知识检索
		FilterKey:       model.AppSearchReleaseFilterKey,
		Labels:          convertSearchVectorLabel(req.GetLabels()),
		LabelExpression: s.transCustomLabelExpression(model.AppReleaseScenes, replaceApp, labelConfig, req.GetLabels()),
		SubQuestions:    req.GetSubQuestions(),
		SearchStrategy:  searchStrategy,
		ModelName:       req.GetModelName(),
	}
	searchRsp, err := s.dao.Search(ctx, searchReq)
	if err != nil {
		return rsp, err
	}
	linkContents, err := s.dao.GetLinkContentsFromSearchResponse(
		ctx, replaceApp.GetId(), searchRsp.GetDocs(),
		func(doc *retrieval.SearchRsp_Doc, qa *model.DocQA) any {
			return &pb.CustomSearchRsp_Doc{
				DocId:                doc.GetDocId(),
				DocType:              doc.GetDocType(),
				RelatedId:            doc.GetRelatedId(),
				Question:             doc.GetQuestion(),
				Answer:               doc.GetAnswer(),
				CustomParam:          doc.GetCustomParam(),
				QuestionDesc:         doc.GetQuestionDesc(),
				Confidence:           doc.GetConfidence(),
				RelatedBizId:         qa.BusinessID,
				Extra:                convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:            doc.GetImageUrls(),
				ResultType:           convertRetrievalResultType(doc.GetResultType()),
				SimilarQuestionExtra: convertSimilarQuestionExtra(doc.GetSimilarQuestionExtra()),
			}
		},
		func(doc *retrieval.SearchRsp_Doc, segment *model.DocSegmentExtend) any {
			return &pb.CustomSearchRsp_Doc{
				DocId:        doc.GetDocId(),
				DocType:      doc.GetDocType(),
				RelatedId:    doc.GetRelatedId(),
				OrgData:      doc.GetOrgData(),
				Confidence:   doc.GetConfidence(),
				RelatedBizId: segment.BusinessID,
				IsBigData:    doc.GetIsBigData(),
				Extra:        convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:    doc.GetImageUrls(),
				ResultType:   convertRetrievalResultType(doc.GetResultType()),
				SheetInfo:    convertSearchRetrievalSheetInfo(doc.GetText2SqlExtra()),
			}
		},
		func(doc *retrieval.SearchRsp_Doc) any {
			return &pb.CustomSearchRsp_Doc{
				DocId:      doc.GetDocId(),
				DocType:    doc.GetDocType(),
				RelatedId:  doc.GetRelatedId(),
				Question:   doc.GetQuestion(),
				Answer:     doc.GetAnswer(),
				Confidence: doc.GetConfidence(),
				Extra:      convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:  doc.GetImageUrls(),
				ResultType: convertRetrievalResultType(doc.GetResultType()),
			}
		},
	)
	if err != nil {
		return rsp, err
	}
	rsp.Docs = dao.Link(ctx, linkContents, func(t *pb.CustomSearchRsp_Doc, v linker.Content) *pb.CustomSearchRsp_Doc {
		t.OrgData = v.Value
		return t
	})
	log.DebugContextf(ctx, "req: %+v, rsp: %+v", req, rsp)
	return searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp), nil
}

// transCustomReqLabels 转换自定义检索的标签
func (s *Service) transCustomReqLabels(ctx context.Context, scenes uint32, app *admin.GetAppInfoRsp,
	labelConfig *model.CustomLabelConfig, labels []*pb.VectorLabel) []*pb.VectorLabel {
	labels = make([]*pb.VectorLabel, 0)
	envType, ok := model.Scenes2AttrLabelEnvType[scenes]
	if !ok {
		return labels
	}
	if labelConfig == nil {
		// handleReqLabels 之后 VectorLabel中的Name就是AttrKey
		return s.similarLabels2StandardLabels(ctx, app.GetId(), labels, envType)
	}
	return labels
}

// transCustomLabelExpression 转换自定义标签检索表达式
func (s *Service) transCustomLabelExpression(scenes uint32, app *admin.GetAppInfoRsp,
	labelConfig *model.CustomLabelConfig, labels []*pb.VectorLabel) *retrieval.LabelExpression {
	// 未定义配置 取应用的配置值
	if labelConfig == nil {
		switch scenes {
		case model.AppTestScenes:
			return fillLabelExpression(labels,
				getAppLabelCondition(app.GetAppBizId(), app.GetKnowledgeQa().GetSearchRange().GetCondition()))
		case model.AppReleaseScenes:
			return fillLabelExpression(labels,
				getAppLabelCondition(app.GetAppBizId(), app.GetKnowledgeQa().GetSearchRange().GetCondition()))
		default:
			return fillLabelExpression(labels, getAppLabelCondition(app.GetAppBizId(), model.AppSearchConditionAnd))
		}
	}
	// 有定义配置 取定义的配置值
	if labelConfig.IsLabelOrGeneral {
		return fillLabelExpression(labels, // 携带全局default标签
			getAppLabelCondition(app.GetAppBizId(), labelConfig.LabelLogicOpr))
	} else {
		return fillLabelWithoutGeneralVectorExpression(labels, // 不带全局default标签
			getAppLabelCondition(app.GetAppBizId(), labelConfig.LabelLogicOpr))
	}
}

// convertSearchVectorLabel 转换标签的结构体
func convertSearchVectorLabel(labels []*pb.VectorLabel) []*retrieval.SearchVectorLabel {
	searchLabels := make([]*retrieval.SearchVectorLabel, 0, len(labels))
	for _, label := range labels {
		searchLabels = append(searchLabels, &retrieval.SearchVectorLabel{
			Name:   label.Name,
			Values: label.Values,
		})
	}
	return searchLabels
}

func convertRetrievalExtra(extra *retrieval.RetrievalExtra) *pb.RetrievalExtra {
	if extra == nil {
		return nil
	}
	return &pb.RetrievalExtra{
		EmbRank:     extra.EmbRank,
		EsScore:     extra.EsScore,
		EsRank:      extra.EsRank,
		RerankScore: extra.RerankScore,
		RerankRank:  extra.RerankRank,
		RrfScore:    extra.RrfScore,
		RrfRank:     extra.RrfRank,
	}
}

func convertSimilarQuestionExtraForVector(extra *retrieval.SearchVectorRsp_Doc_SimilarQuestionExtra) *pb.SimilarQuestionExtra {
	if extra == nil {
		return nil
	}
	return &pb.SimilarQuestionExtra{
		SimilarId:       extra.GetSimilarId(),
		SimilarQuestion: extra.GetSimilarQuestion(),
	}
}

func convertSimilarQuestionExtra(extra *retrieval.SearchRsp_Doc_SimilarQuestionExtra) *pb.SimilarQuestionExtra {
	if extra == nil {
		return nil
	}
	return &pb.SimilarQuestionExtra{
		SimilarId:       extra.GetSimilarId(),
		SimilarQuestion: extra.GetSimilarQuestion(),
	}
}

// convertRetrievalResultType 转换结果类型
func convertRetrievalResultType(resultType retrieval.RetrievalResultType) pb.RetrievalResultType {
	return pb.RetrievalResultType(resultType.Number())
}

// convertSearchVectorRetrievalSheetInfo 转换结果类型
func convertSearchVectorRetrievalSheetInfo(extra *retrieval.SearchVectorRsp_Doc_Text2SQLExtra) string {
	if extra == nil || len(extra.GetTableInfos()) == 0 {
		return ""
	}

	sheetData := make([]*knowledge.PageContent_SheetData, 0)
	for _, info := range extra.GetTableInfos() {
		sheetData = append(sheetData, &knowledge.PageContent_SheetData{
			SheetName: info.GetTableName(),
		})
	}

	if len(sheetData) == 0 {
		return ""
	}

	sheetInfoStr, _ := jsoniter.MarshalToString(sheetData)
	return sheetInfoStr
}

// convertSearchRetrievalSheetInfo 转换结果类型
func convertSearchRetrievalSheetInfo(extra *retrieval.SearchRsp_Doc_Text2SQLExtra) string {
	if extra == nil || len(extra.GetTableInfos()) == 0 {
		return ""
	}

	sheetData := make([]*knowledge.PageContent_SheetData, 0)
	for _, info := range extra.GetTableInfos() {
		sheetData = append(sheetData, &knowledge.PageContent_SheetData{
			SheetName: info.GetTableName(),
		})
	}

	if len(sheetData) == 0 {
		return ""
	}

	sheetInfoStr, _ := jsoniter.MarshalToString(sheetData)
	return sheetInfoStr
}

// SearchPreview 对话评测
func (s *Service) SearchPreview(ctx context.Context, req *pb.SearchPreviewReq) (*pb.SearchPreviewRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口SearchPreview收到了请求 deprecated interface req:%+v", req)
	//return s.SearchPreviewWithCustomVariables(ctx, req)
	return &pb.SearchPreviewRsp{}, nil
}

// SearchPreviewWithCustomVariables 对话评测带自定义参数
func (s *Service) SearchPreviewWithCustomVariables(ctx context.Context,
	req *pb.SearchPreviewReq, customVariables map[string]string) (*pb.SearchPreviewRsp, error) {
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	ctx = clues.NewTrackContext(ctx)
	clues.AddTrackData(ctx, "SearchPreview.req", req)
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppTestScenes)
	clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	replaceApp := app
	if newAppID, ok := utilConfig.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.GetAppBizId()]; ok {
		replaceApp, err = client.GetAppInfo(ctx, newAppID, model.AppTestScenes)
		clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		log.DebugContextf(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v",
			app, replaceApp)
	}
	if app.GetKnowledgeQa() == nil || replaceApp.GetKnowledgeQa() == nil {
		return nil, errs.ErrAppTypeSupportFilters
	}

	key := utils.When(req.GetFilterKey() == "", model.AppSearchPreviewFilterKey, req.GetFilterKey())
	filter, ok := app.GetKnowledgeQa().GetFilters()[key]
	if !ok {
		return nil, fmt.Errorf("robot %s filter not found scenes %d", key, model.AppTestScenes)
	}
	clues.AddTrackData(ctx, "app.GetFilter", map[string]any{
		"scenes": model.AppTestScenes, "key": key, "filter": filter,
	})
	if replaceApp.GetKnowledgeQa() != nil {
		req.Labels = handleCustomVariablesLabels(req.Labels,
			replaceApp.GetKnowledgeQa().GetSearchRange(), customVariables)
	}
	// handleReqLabels 之后 VectorLabel中的Name就是AttrKey
	newLabels := s.similarLabels2StandardLabels(ctx, replaceApp.GetId(), req.GetLabels(), model.AttributeLabelsPreview)
	filters := make([]*retrieval.SearchVectorReq_Filter, 0, len(filter.Filter))
	for _, f := range filter.GetFilter() {
		if f.GetDocType() == model.DocTypeSearchEngine {
			continue
		}
		if f.GetDocType() == model.DocTypeTaskFlow {
			continue
		}
		if key == model.AppSearchPreviewFilterKey && !f.GetIsEnable() {
			continue
		}
		filters = append(filters, &retrieval.SearchVectorReq_Filter{
			IndexId:         uint64(f.GetIndexId()),
			Confidence:      f.GetConfidence(),
			TopN:            f.GetTopN(),
			DocType:         f.GetDocType(),
			LabelExprString: fillLabelExprString(newLabels), // 2.6之前旧的表达式，保持逻辑不变
		})
	}
	// 敏捷发布：新增检索范围，支持指定范围检索
	filters = filterSearchVectorScope(ctx, req.GetSearchScope(), filters)
	rerank, err := getRerankModel(app)
	clues.AddTrackData(ctx, "app.GetRerankModel", map[string]any{
		"scenes": model.AppTestScenes, "rerank": rerank, "err": err,
	})
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return nil, err
	}

	searchVectorReq := &retrieval.SearchVectorReq{
		RobotId:          replaceApp.GetId(),
		BotBizId:         replaceApp.GetAppBizId(),
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             filter.GetTopN(),
		EmbeddingVersion: replaceApp.GetKnowledgeQa().GetEmbedding().GetVersion(),
		Rerank: &retrieval.SearchVectorReq_Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		// 传FilterKey区分知识检索和已采纳问题直接回复
		FilterKey: key,
		Labels:    convertSearchVectorLabel(newLabels),
		ImageUrls: req.GetImageUrls(),
		LabelExpression: fillLabelExpression(newLabels,
			getAppLabelCondition(replaceApp.GetAppBizId(), replaceApp.GetKnowledgeQa().GetSearchRange().GetCondition())),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy()),
		ModelName:      req.GetModelName(),
	}

	searchVectorRsp, err := client.SearchVector(ctx, searchVectorReq)
	if err != nil {
		log.ErrorContextf(ctx, "DirectSearchVector robotID: %d, err: %v", app.GetId(), err)
		return nil, err
	}

	t1 := time.Now()
	var docs []*pb.SearchPreviewRsp_Doc
	docs, err = s.getPreviewRspDocByApp(ctx, replaceApp, searchVectorRsp.GetDocs(), replaceApp.GetId())
	clues.AddTrackData(ctx, "getPreviewRspDocByApp", map[string]any{
		"app": app, "req-docs": searchVectorRsp.GetDocs(), "resp-docs": docs, "err": err != nil, "ELAPSED": time.Since(t1).String(),
	})
	if err != nil {
		return nil, err
	}

	rsp := &pb.SearchPreviewRsp{Docs: docs}
	post := searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp)
	clues.AddTrackData(ctx, "searchRspPostProcess", map[string]any{
		"usePlaceholder": req.GetUsePlaceholder(), "SearchPreviewRsp": rsp, "post": post,
	})
	return post, nil
}

// searchAnswer 检索内容[统一接口]
func (s *Service) searchAnswer(ctx context.Context, bc *logicSearch.BotContext) (*knowledge.SearchKnowledgeRsp, error) {
	log.InfoContextf(ctx, "searchAnswer req:%s", utils2.Any2String(bc))
	// 处理策略
	searchStrategy := bc.HandleSearchStrategy(ctx)
	// 处理rerank配置
	rerank, err := bc.HandleRerank(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return nil, err
	}
	// 处理默认知识库的召回数量
	recallNum, err := bc.HandleRetrievalRecallNum(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "HandleRetrievalRecallNum failed, err: %+v", err)
		return nil, err
	}
	searchMultiReq := &retrieval.SearchMultiKnowledgeReq{
		RobotId:        bc.ReplaceApp.GetId(),
		BotBizId:       bc.ReplaceApp.GetAppBizId(),
		Question:       bc.Question,
		TopN:           recallNum,
		Rerank:         rerank,
		ImageUrls:      bc.ImageURLs,
		SubQuestions:   bc.SubQuestions,
		SearchStrategy: searchStrategy, // retrieval已废弃该字段，knowledge后续也删除
		ModelName:      bc.ModelName,
	}
	kgs := bc.GetRetrievalKGList(ctx)
	if len(kgs) == 0 {
		log.WarnContextf(ctx, "retrieval knowledge list empty")
		return &knowledge.SearchKnowledgeRsp{}, nil
	}
	for _, kgInfo := range kgs {
		var data retrieval.SearchData
		data.KnowledgeId = kgInfo.KnowledgeID
		data.KnowledgeBizId = kgInfo.KnowledgeBizID
		data.EmbeddingVersion = kgInfo.EmbeddingVersion
		data.EmbeddingModelName = kgInfo.EmbeddingModelName
		data.Rerank = kgInfo.Rerank
		data.QaVersion = kgInfo.QAVersion
		data.FilterKey = kgInfo.FilterKey // 传FilterKey区分知识检索和已采纳问题直接回复
		data.SearchStrategy = kgInfo.SearchStrategy
		// 基于知识库配置二次过滤
		data.Filters = bc.FilterCloseKnowledge(ctx, kgInfo.WorkflowKGCfg.GetCloseKnowledge(), kgInfo.Filters)
		if len(data.Filters) == 0 {
			log.WarnContextf(ctx, "retrieval knowledge filter list empty")
			continue
		}
		// 处理知识库标签
		kcgLabel := bc.HandleSearchLabels(ctx, kgInfo, 0)      //获取全局适用的filter_label
		if bc.KnowledgeType == knowPB.KnowledgeType_WORKFLOW { //如果是工作流问答的标签需要特殊处理(不能带DocID)
			for _, v := range data.Filters {
				if v.DocType == model.DocTypeQA {
					v.LabelExpression = bc.HandleSearchLabels(ctx, kgInfo, v.DocType)
				} else {
					v.LabelExpression = kcgLabel
				}
			}
		} else {
			data.LabelExpression = kcgLabel
		}
		searchMultiReq.SearchData = append(searchMultiReq.SearchData, &data)

		if search.EnumSearchScope(bc.SearchBatchReq.GetSearchScope()) != search.EnumSearchScopeQAPriority {
			// 问答0.97直出不上报，避免多次重复上报
			// 上报统计数据
			go func(newCtx context.Context) { //异步上报
				counterInfo := &data_statistics.CounterInfo{
					CorpBizId:       bc.ReplaceApp.GetCorpBizId(),
					SpaceId:         bc.ReplaceApp.GetSpaceId(),
					AppBizId:        bc.ReplaceApp.GetAppBizId(),
					StatisticObject: statistics.StatObject_STAT_OBJECT_KB,
					StatisticType:   statistics.StatType_STAT_TYPE_CALL,
					ObjectId:        strconv.FormatUint(kgInfo.KnowledgeBizID, 10),
					ObjectName:      kgInfo.KnowledgeName,
					Count:           1,
				}
				data_statistics.Counter(newCtx, counterInfo)
			}(trpc.CloneContext(ctx))
		}
	}
	if len(searchMultiReq.SearchData) == 0 {
		log.WarnContextf(ctx, "retrieval knowledge search data list empty")
		return &knowledge.SearchKnowledgeRsp{}, nil
	}
	searchMultiRsp, err := s.SearchMultiKnowledge(ctx, bc, searchMultiReq)
	if err != nil {
		log.ErrorContextf(ctx, "SearchMultiKnowledge robotID: %d, err: %v", bc.App.GetId(), err)
		return nil, err
	}

	if searchMultiRsp != nil && searchMultiRsp.GetRsp() != nil {
		rspDocs := searchMultiRsp.GetRsp().GetDocs()
		// 获取文档的业务ID
		// 获取所有检索到的文档信息，需要先按文档id去重
		docIDMap := make(map[uint64]struct{})
		for _, doc := range rspDocs {
			if doc.GetDocType() == model.DocTypeSegment {
				// 只对检索出的文档鉴权
				docIDMap[doc.DocId] = struct{}{}
			}
		}
		if len(docIDMap) == 0 {
			return searchMultiRsp, nil
		}

		docFilter := &dao.DocFilter{
			RouterAppBizID: bc.ReplaceApp.GetAppBizId(),
			IDs:            maps.Keys(docIDMap),
		}
		selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId, dao.DocTblColRobotId, dao.DocTblColFileName,
			dao.DocTblColCustomerKnowledgeId, dao.DocTblColAttributeFlag}
		docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, docFilter)
		if err != nil {
			return nil, err
		}
		if len(docs) != len(docFilter.IDs) {
			// 降级处理
			log.WarnContextf(ctx, "some docs not found, len(docs):%d docIDs:%+v", len(docs), docFilter.IDs)
		}
		docInfos := make(map[uint64]*model.Doc)
		for _, doc := range docs {
			docInfos[doc.ID] = doc
		}
		for _, rspDoc := range rspDocs {
			if doc, ok := docInfos[rspDoc.DocId]; ok {
				rspDoc.DocBizId = doc.BusinessID
				rspDoc.DocName = doc.FileName
				rspDoc.Title = strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName))
				rspDoc.KnowledgeBizId, err = dao.GetAppBizIDByAppID(ctx, doc.RobotID)
				if err != nil {
					// 降级处理
					log.WarnContextf(ctx, "get app biz id by app id failed, err:%v", err)
				}
			}
		}

		// 如果该应用配置了第三方权限系统，需要调用第三方权限系统校验该用户是否有检索结果中文档权限
		searchMultiRsp.GetRsp().Docs, err = logicSearch.CheckThirdPermission(ctx, bc.ReplaceApp.GetAppBizId(),
			bc.GetLKEUserID(), searchMultiRsp.GetRsp().GetDocs())
		if err != nil {
			log.ErrorContextf(ctx, "SearchKnowledge CheckThirdPermission failed, err: %+v", err)
			return nil, err
		}

	}

	log.InfoContextf(ctx, "searchMultiRsp:%s", utils2.Any2String(searchMultiRsp))
	return searchMultiRsp, nil
}

// SearchMultiKnowledge 批量检索多知识库接口
func (s *Service) SearchMultiKnowledge(ctx context.Context, bc *logicSearch.BotContext,
	searchMultiReq *retrieval.SearchMultiKnowledgeReq) (*knowledge.SearchKnowledgeRsp, error) {
	t1 := time.Now()
	log.InfoContextf(ctx, "searchMultiKnowledgeReq:%s", utils2.Any2String(searchMultiReq))
	rsp := new(knowledge.SearchKnowledgeRsp)
	rsp.KnowledgeType = knowledge.KnowledgeType_DOC_QA
	rsp.SceneType = bc.SceneType
	if bc.SceneType == knowledge.SceneType_TEST {
		multiRsp, err := client.SearchMultiKnowledgePreview(ctx, searchMultiReq)
		if err != nil {
			log.ErrorContextf(ctx, "DirectSearchVector robotID: %d, err: %v", bc.App.GetId(), err)
			return nil, err
		}
		var docs []*pb.SearchPreviewRsp_Doc
		docs, err = s.getPreviewRspDocByApp(ctx, bc.ReplaceApp, multiRsp.GetDocs(), bc.ReplaceApp.GetId())
		if err != nil {
			log.ErrorContextf(ctx, "getPreviewRspDocByApp robotID: %d, err: %v", bc.ReplaceApp.GetId(), err)
			return nil, err
		}
		previewRsp := &pb.SearchPreviewRsp{Docs: docs}
		post := searchRspPostProcess(ctx, bc.UsePlaceholder, previewRsp)
		rsp.Rsp = &knowledge.SearchKnowledgeRsp_SearchRsp{
			Docs: convertPreviewDocToKnowledgeDoc(post.GetDocs()),
		}
	} else {
		// 走发布库
		releaseRsp := new(pb.SearchRsp)
		multiRsp, err := client.SearchMultiKnowledgeRelease(ctx, searchMultiReq)
		if err != nil {
			return nil, err
		}
		docs, err := s.searchDocs(ctx, multiRsp, bc.ReplaceApp.GetId())
		if err != nil {
			return nil, err
		}
		releaseRsp.Docs = docs
		post := searchRspPostProcess(ctx, bc.UsePlaceholder, releaseRsp)
		rsp.Rsp = &knowledge.SearchKnowledgeRsp_SearchRsp{
			Docs: convertReleaseDocToKnowledgeDoc(post.GetDocs()),
		}
	}
	log.InfoContextf(ctx, "SearchMultiKnowledge rsp:%s, cost:%d", utils2.Any2String(rsp),
		time.Since(t1).Milliseconds())
	return rsp, nil
}

func getSearchStrategy(appSearchStrategy *admin.AppSearchStrategy) *retrieval.SearchStrategy {
	var searchStrategy *retrieval.SearchStrategy
	if appSearchStrategy != nil {
		searchStrategy = &retrieval.SearchStrategy{
			StrategyType:     retrieval.SearchStrategyTypeEnum(appSearchStrategy.GetStrategyType()),
			TableEnhancement: appSearchStrategy.GetTableEnhancement(),
		}
	}
	return searchStrategy
}

// getWorkflowSearchStrategy 获取工作流应用检索策略
func getWorkflowSearchStrategy(knowledgeSearchStrategy *knowledge.SearchStrategy) *retrieval.SearchStrategy {
	var searchStrategy *retrieval.SearchStrategy
	if knowledgeSearchStrategy != nil {
		searchStrategy = &retrieval.SearchStrategy{
			StrategyType:     retrieval.SearchStrategyTypeEnum(knowledgeSearchStrategy.GetStrategyType()),
			TableEnhancement: knowledgeSearchStrategy.GetTableEnhancement(),
		}
	}
	return searchStrategy
}

// filterSearchVectorScope 过滤指定范围检索
func filterSearchVectorScope(ctx context.Context, searchScope uint32,
	filters []*retrieval.SearchVectorReq_Filter) []*retrieval.SearchVectorReq_Filter {
	log.InfoContextf(ctx, "filterSearchVectorScope|searchScope:%d", searchScope)
	var scopeFilter []*retrieval.SearchVectorReq_Filter
	if searchScope > 0 {
		scopeFilter = make([]*retrieval.SearchVectorReq_Filter, 0, 1)
		for _, f := range filters {
			if f.DocType == searchScope {
				scopeFilter = append(scopeFilter, f)
				break
			}
		}
	} else {
		scopeFilter = filters
	}
	log.InfoContextf(ctx, "filterSearchVectorScope|scopeFilter:%+v", scopeFilter)
	return scopeFilter
}

// SearchRelease 向量特征检索(待用户接口迁移到SearchKnowledgeRelease后弃用)
func (s *Service) SearchRelease(ctx context.Context, req *pb.SearchReleaseReq) (*pb.SearchReleaseRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)

	loginUin := pkg.LoginUin(ctx)
	loginSubAccountUin := pkg.LoginSubAccountUin(ctx)
	if loginUin != "" && loginSubAccountUin != "" { // 集成商走admin进行鉴权
		proxy := admin.NewLoginClientProxy()
		sessionReq := &admin.CheckSessionReq{}
		sessionRsp, err := proxy.CheckSession(ctx, sessionReq)
		if err != nil {
			log.ErrorContextf(ctx, "CheckSession err: %v", err)
			return nil, err
		}
		log.InfoContextf(ctx, "staffId:%d corpId:%d sid:%d", sessionRsp.GetStaffId(), sessionRsp.GetCorpId(),
			sessionRsp.GetSId())
		ctx = pkg.WithStaffID(ctx, sessionRsp.GetStaffId())
		ctx = pkg.WithCorpID(ctx, sessionRsp.GetCorpId())
		ctx = pkg.WithSID(ctx, sessionRsp.GetSId())
		searchRsp, err := s.Search(ctx, convertSearchReleaseReqToSearchReq(req))
		return convertSearchRspToSearchReleaseRsp(searchRsp), err
	}
	// 非集成商模式，权限校验，填充CorpID，验证uin是否有app的权限
	uin := pkg.Uin(ctx)
	if uin == "" {
		return nil, errs.ErrUserNotFound
	}
	corp, err := s.dao.GetCorpBySidAndUin(ctx, uin)
	if err != nil || corp == nil {
		log.ErrorContextf(ctx, "GetCorpBySidAndUin err: %v, uin: %s", err, uin)
		return nil, errs.ErrUserNotFound
	}
	if corp.ID == 0 {
		return nil, errs.ErrUserNotFound
	}
	ctx = pkg.WithCorpID(ctx, corp.ID)
	ctx = pkg.WithSID(ctx, uint64(corp.SID))
	searchRsp, err := s.Search(ctx, convertSearchReleaseReqToSearchReq(req))
	return convertSearchRspToSearchReleaseRsp(searchRsp), err
}

func convertSearchReleaseReqToSearchReq(req *pb.SearchReleaseReq) *pb.SearchReq {
	return &pb.SearchReq{
		BotBizId:       req.GetBotBizId(),
		Question:       req.GetQuestion(),
		FilterKey:      req.GetFilterKey(),
		Labels:         req.GetLabels(),
		UsePlaceholder: req.GetUsePlaceholder(),
		ImageUrls:      req.GetImageUrls(),
		SearchScope:    req.GetSearchScope(),
	}
}

func convertSearchRspToSearchReleaseRsp(rsp *pb.SearchRsp) *pb.SearchReleaseRsp {
	searchReleaseRsp := &pb.SearchReleaseRsp{}
	for _, v := range rsp.GetDocs() {
		searchReleaseRsp.Docs = append(searchReleaseRsp.Docs, &pb.SearchReleaseRsp_Doc{
			DocId:                v.GetDocId(),
			DocType:              v.GetDocType(),
			RelatedId:            v.GetRelatedId(),
			Question:             v.GetQuestion(),
			Answer:               v.GetAnswer(),
			Confidence:           v.GetConfidence(),
			OrgData:              v.GetOrgData(),
			RelatedBizId:         v.GetRelatedBizId(),
			QuestionPlaceholders: v.GetQuestionPlaceholders(),
			AnswerPlaceholders:   v.GetAnswerPlaceholders(),
			OrgDataPlaceholders:  v.GetOrgDataPlaceholders(),
			CustomParam:          v.GetCustomParam(),
			IsBigData:            v.GetIsBigData(),
			Extra:                v.GetExtra(),
			ImageUrls:            v.GetImageUrls(),
			ResultType:           v.GetResultType(),
			SimilarQuestionExtra: v.GetSimilarQuestionExtra(),
			SheetInfo:            v.GetSheetInfo(),
			DocTitle:             v.GetDocTitle(),
		})
	}
	return searchReleaseRsp
}

// SearchKnowledgeRelease 向量特征检索
func (s *Service) SearchKnowledgeRelease(ctx context.Context, req *pb.SearchKnowledgeReleaseReq) (
	*pb.SearchKnowledgeReleaseRsp, error) {
	log.InfoContextf(ctx, "SearchKnowledgeRelease req:%+v", req)
	rsp := &pb.SearchKnowledgeReleaseRsp{}
	defer func() {
		log.InfoContextf(ctx, "SearchKnowledgeRelease rsp:%+v", rsp)
	}()
	loginUin := pkg.LoginUin(ctx)
	loginSubAccountUin := pkg.LoginSubAccountUin(ctx)
	if loginUin != "" && loginSubAccountUin != "" { // 集成商走admin进行鉴权
		proxy := admin.NewLoginClientProxy()
		sessionReq := &admin.CheckSessionReq{}
		sessionRsp, err := proxy.CheckSession(ctx, sessionReq)
		if err != nil {
			log.ErrorContextf(ctx, "CheckSession err: %v", err)
			return nil, err
		}
		log.InfoContextf(ctx, "staffId:%d corpId:%d sid:%d", sessionRsp.GetStaffId(), sessionRsp.GetCorpId(),
			sessionRsp.GetSId())
		ctx = pkg.WithStaffID(ctx, sessionRsp.GetStaffId())
		ctx = pkg.WithCorpID(ctx, sessionRsp.GetCorpId())
		ctx = pkg.WithSID(ctx, sessionRsp.GetSId())
	} else { // 非集成商模式，权限校验，填充CorpID，验证uin是否有app的权限
		uin := pkg.Uin(ctx)
		if uin == "" {
			return nil, errs.ErrUserNotFound
		}
		corp, err := s.dao.GetCorpBySidAndUin(ctx, uin)
		if err != nil || corp == nil {
			log.ErrorContextf(ctx, "GetCorpBySidAndUin err: %v, uin: %s", err, uin)
			return nil, errs.ErrUserNotFound
		}
		if corp.ID == 0 {
			return nil, errs.ErrUserNotFound
		}
		ctx = pkg.WithCorpID(ctx, corp.ID)
		ctx = pkg.WithSID(ctx, uint64(corp.SID))
	}
	app, err := client.GetAppInfo(ctx, req.GetAppBizId(), model.AppReleaseScenes)
	if err != nil || app == nil || app.GetIsDelete() {
		return nil, errs.ErrRobotNotFound
	}

	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())

	customVariables := make(map[string]string)
	for _, item := range req.CustomVariables {
		customVariables[item.Name] = item.Value
	}
	searchKnowledgeBatchReq := &knowledge.SearchKnowledgeBatchReq{
		SceneType:       model.AppReleaseScenes,
		AppBizId:        req.AppBizId,
		Question:        req.Question,
		KnowledgeType:   knowledge.KnowledgeType_DOC_QA,
		CustomVariables: customVariables,
		VisitorBizId:    req.VisitorBizId,
	}

	searchKnowledgeBatchRsp, err := s.SearchKnowledgeBatch(ctx, searchKnowledgeBatchReq)
	if err != nil {
		log.ErrorContextf(ctx, "SearchKnowledgeBatch err: %v", err)
		return nil, err
	}
	searchKnowledgeRsp := searchKnowledgeBatchRsp.GetRsp()
	if searchKnowledgeRsp == nil || searchKnowledgeRsp.GetDocs() == nil || len(searchKnowledgeRsp.GetDocs()) == 0 {
		return rsp, nil
	}

	// Search接口返回的是文档自增id，对外需要转成业务id
	docIDs := make([]uint64, 0)
	for _, res := range searchKnowledgeRsp.GetDocs() {
		if res.GetDocType() == model.DocTypeSegment {
			docIDs = append(docIDs, res.GetDocId())
		}
	}
	docIDs = slicex.Unique(docIDs)
	docMap := make(map[uint64]*model.Doc)
	if len(docIDs) != 0 {
		filter := &dao.DocFilter{
			RouterAppBizID: app.GetAppBizId(),
			RobotId:        app.GetId(),
			IDs:            docIDs,
		}
		selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId}
		docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, filter)
		if err == nil && len(docs) != 0 {
			for _, doc := range docs {
				docMap[doc.ID] = doc
			}
		} else {
			// 降级，不报错，不返回docID
		}
	}

	rsp = convertSearKnowledgeRspToSearchKnowledgeReleaseRsp(ctx, searchKnowledgeBatchRsp, docMap)
	return rsp, nil
}

func convertSearKnowledgeRspToSearchKnowledgeReleaseRsp(ctx context.Context, searchKnowledgeRsp *knowledge.SearchKnowledgeRsp, docMap map[uint64]*model.Doc) *pb.SearchKnowledgeReleaseRsp {
	searchKnowledgeReleaseRsp := &pb.SearchKnowledgeReleaseRsp{}
	knowledgeList := make([]*pb.SearchKnowledgeReleaseRsp_KnowledgeItem, 0)
	rsp := searchKnowledgeRsp.GetRsp()
	if rsp == nil || rsp.GetDocs() == nil || len(rsp.GetDocs()) == 0 {
		return searchKnowledgeReleaseRsp
	}

	for _, res := range rsp.GetDocs() {
		knowledgeType := ""
		switch res.GetDocType() {
		case model.DocTypeSegment:
			knowledgeType = model.DataTypeDoc
		case model.DocTypeQA:
			knowledgeType = model.DataTypeQA
		default:
			log.ErrorContextf(ctx, "docType:%d is invalid", res.GetDocType())
			continue
		}
		knowledgeItem := &pb.SearchKnowledgeReleaseRsp_KnowledgeItem{
			KnowledgeType: knowledgeType,
			KnowledgeId:   fmt.Sprintf("%d", res.GetRelatedBizId()),
			Question:      res.GetQuestion(),
			Content: func() string {
				if res.GetDocType() == model.DocTypeQA {
					return res.GetAnswer()
				}
				return res.GetOrgData()
			}(),
			Title:           res.GetTitle(),
			KnowledgeBaseId: fmt.Sprintf("%d", res.GetKnowledgeBizId()),
			DocName:         res.GetDocName(),
		}
		if doc, ok := docMap[res.GetDocId()]; ok && doc != nil {
			// 文档自增id转成业务id
			knowledgeItem.RelatedDocId = fmt.Sprintf("%d", doc.BusinessID)
		}
		knowledgeList = append(knowledgeList, knowledgeItem)
	}
	searchKnowledgeReleaseRsp.KnowledgeList = knowledgeList
	return searchKnowledgeReleaseRsp
}

// Search 向量特征检索
func (s *Service) Search(ctx context.Context, req *pb.SearchReq) (*pb.SearchRsp, error) {
	return s.SearchWithCustomVariables(ctx, req, nil)
}

// SearchWithCustomVariables 向量特征检索带自定义参数
func (s *Service) SearchWithCustomVariables(ctx context.Context,
	req *pb.SearchReq, customVariables map[string]string) (*pb.SearchRsp, error) {
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	ctx = clues.NewTrackContext(ctx)
	clues.AddTrackData(ctx, "Search.req", req)
	rsp := new(pb.SearchRsp)
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppReleaseScenes)
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	replaceApp := app
	if newAppID, ok := utilConfig.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.GetAppBizId()]; ok {
		replaceApp, err = client.GetAppInfo(ctx, newAppID, model.AppReleaseScenes)
		clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		log.DebugContextf(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v",
			app, replaceApp)
	}
	if app.GetKnowledgeQa() == nil || replaceApp.GetKnowledgeQa() == nil {
		return nil, errs.ErrAppTypeSupportFilters
	}

	key := utils.When(req.GetFilterKey() == "", model.AppSearchReleaseFilterKey, req.GetFilterKey())
	filter, ok := app.GetKnowledgeQa().GetFilters()[key]
	if !ok {
		return nil, fmt.Errorf("robot %s filter not found scenes %d", key, model.AppReleaseScenes)
	}
	clues.AddTrackData(ctx, "app.GetFilter", map[string]any{
		"scenes": model.AppReleaseScenes, "key": key, "filter": filter,
	})
	if replaceApp.GetKnowledgeQa() != nil {
		req.Labels = handleCustomVariablesLabels(req.Labels,
			replaceApp.GetKnowledgeQa().GetSearchRange(), customVariables)
	}
	// handleReqLabels 之后 VectorLabel中的Name就是AttrKey
	newLabels := s.similarLabels2StandardLabels(ctx, replaceApp.GetId(), req.GetLabels(), model.AttributeLabelsProd)
	filters := make([]*retrieval.SearchReq_Filter, 0, len(filter.Filter))
	for _, f := range filter.Filter {
		if f.GetDocType() == model.DocTypeSearchEngine {
			continue
		}
		if f.GetDocType() == model.DocTypeTaskFlow {
			continue
		}
		if key == model.AppSearchReleaseFilterKey && !f.GetIsEnable() {
			continue
		}
		filters = append(filters, &retrieval.SearchReq_Filter{
			DocType:    f.GetDocType(),
			Confidence: f.GetConfidence(),
			TopN:       f.GetTopN(),
		})
	}
	// 敏捷发布：新增检索范围，支持指定范围检索
	filters = filterSearchScope(ctx, req.GetSearchScope(), filters)
	rerank, err := getRerankModel(app)
	clues.AddTrackData(ctx, "app.GetRerankModel", map[string]any{
		"scenes": model.AppReleaseScenes, "rerank": rerank, "err": err != nil,
	})
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return rsp, err
	}
	r, err := s.dao.Search(ctx, &retrieval.SearchReq{
		RobotId:   replaceApp.GetId(),
		VersionId: replaceApp.GetKnowledgeQa().GetQaVersion(),
		Question:  req.GetQuestion(),
		Filters:   filters,
		TopN:      filter.GetTopN(),
		Rerank:    &retrieval.SearchReq_Rerank{Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable},
		// 传FilterKey区分知识检索和已采纳问题直接回复
		FilterKey: key,
		Labels:    convertSearchVectorLabel(newLabels),
		ImageUrls: req.GetImageUrls(),
		LabelExpression: fillLabelExpression(newLabels,
			getAppLabelCondition(replaceApp.GetAppBizId(), replaceApp.GetKnowledgeQa().GetSearchRange().GetCondition())),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy()),
		ModelName:      req.GetModelName(),
	})
	if err != nil {
		return nil, err
	}
	t1 := time.Now()
	docs, err := s.searchDocs(ctx, r, replaceApp.GetId())
	clues.AddTrackData(ctx, "searchDocs", map[string]any{
		"req": r, "docs": docs, "err": err != nil, "ELAPSED": time.Since(t1).String(),
	})
	if err != nil {
		return nil, err
	}
	rsp.Docs = docs
	post := searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp)
	clues.AddTrackData(ctx, "searchRspPostProcess", map[string]any{
		"usePlaceholder": req.GetUsePlaceholder(), "SearchPreviewRsp": rsp, "post": post,
	})
	return post, nil
}

// filterSearchScope 过滤指定范围检索
func filterSearchScope(ctx context.Context, searchScope uint32,
	filters []*retrieval.SearchReq_Filter) []*retrieval.SearchReq_Filter {
	log.InfoContextf(ctx, "filterSearchScope|searchScope:%d", searchScope)
	var scopeFilter []*retrieval.SearchReq_Filter
	if searchScope > 0 {
		scopeFilter = make([]*retrieval.SearchReq_Filter, 0, 1)
		for _, f := range filters {
			if f.DocType == searchScope {
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

func (s *Service) searchDocs(ctx context.Context, r *retrieval.SearchRsp, robotID uint64) ([]*pb.SearchRsp_Doc, error) {
	linkContents, err := s.dao.GetLinkContentsFromSearchResponse(
		ctx, robotID, r.GetDocs(),
		func(doc *retrieval.SearchRsp_Doc, qa *model.DocQA) any {
			return &pb.SearchRsp_Doc{
				DocType:              doc.GetDocType(),
				DocId:                doc.GetDocId(),
				RelatedId:            doc.GetRelatedId(),
				Question:             doc.GetQuestion(),
				Answer:               doc.GetAnswer(),
				CustomParam:          doc.GetCustomParam(),
				QuestionDesc:         doc.GetQuestionDesc(),
				Confidence:           doc.GetConfidence(),
				RelatedBizId:         qa.BusinessID,
				Extra:                convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:            doc.GetImageUrls(),
				ResultType:           convertRetrievalResultType(doc.GetResultType()),
				SimilarQuestionExtra: convertSimilarQuestionExtra(doc.GetSimilarQuestionExtra()),
			}
		},
		func(doc *retrieval.SearchRsp_Doc, segment *model.DocSegmentExtend) any {
			return &pb.SearchRsp_Doc{
				DocType:      doc.GetDocType(),
				DocId:        doc.GetDocId(),
				RelatedId:    doc.GetRelatedId(),
				OrgData:      doc.GetOrgData(),
				Confidence:   doc.GetConfidence(),
				RelatedBizId: segment.BusinessID,
				IsBigData:    doc.GetIsBigData(),
				Extra:        convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:    doc.GetImageUrls(),
				ResultType:   convertRetrievalResultType(doc.GetResultType()),
				SheetInfo:    convertSearchRetrievalSheetInfo(doc.GetText2SqlExtra()),
				DocTitle: func() string {
					suffix := ": \n"
					if strings.HasSuffix(segment.Title, suffix) {
						return segment.Title[0 : len(segment.Title)-len(suffix)]
					}
					return segment.Title
				}(),
			}
		},
		func(doc *retrieval.SearchRsp_Doc) any {
			return &pb.SearchRsp_Doc{
				DocType:    doc.GetDocType(),
				DocId:      doc.GetDocId(),
				RelatedId:  doc.GetRelatedId(),
				Question:   doc.GetQuestion(),
				Answer:     doc.GetAnswer(),
				Confidence: doc.GetConfidence(),
				Extra:      convertRetrievalExtra(doc.GetExtra()),
				ImageUrls:  doc.GetImageUrls(),
				ResultType: convertRetrievalResultType(doc.GetResultType()),
			}
		},
	)
	if err != nil {
		return nil, err
	}
	docs := dao.Link(ctx, linkContents, func(t *pb.SearchRsp_Doc, v linker.Content) *pb.SearchRsp_Doc {
		t.OrgData = v.Value
		return t
	})
	return docs, nil
}

func patchConfidence(refers []model.Refer, answer string) ([]model.Refer, []Score) {
	scores := make([]Score, len(refers))
	answerWords := strings.Join(seg.Cut(answer, true), " ")
	for i, refer := range refers {
		reference := refer.OrgData
		if refer.DocType == model.DocTypeQA {
			reference = refer.Answer
		}
		referenceWords := strings.Join(seg.Cut(reference, true), " ")
		score := GetRougeScore(answerWords, referenceWords)

		refers[i].Confidence = float32(math.Max(score.P, score.R))
		refers[i].RougeScore, _ = jsoniter.MarshalToString(score)
		scores[i] = score
	}
	return refers, scores
}

// MatchRefer 匹配来源
func (s *Service) MatchRefer(ctx context.Context, req *pb.MatchReferReq) (*pb.MatchReferRsp, error) {
	log.DebugContextf(ctx, "MatchRefer|req:%+v", req)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return &pb.MatchReferRsp{}, errs.ErrRobotNotFound
	}
	filter, refers, docIDs, qaIDs, err := s.getRefersFromReq(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "MatchRefer|getRefersFromReq failed, err:%+v", err)
		return nil, err
	}
	if len(refers) == 0 {
		log.WarnContextf(ctx, "MatchRefer|refers is empty|ignore")
		return &pb.MatchReferRsp{}, nil
	}
	log.DebugContextf(ctx, "MatchRefer|IgnoreConfidenceScore:%+v", req.GetIgnoreConfidenceScore())
	if !req.GetIgnoreConfidenceScore() {
		// 2.3迭代特殊逻辑：
		// 	- 目前使用工程计算相似度的方式，准确率不高，增加一个开关决定是否使用工程相似度计算
		// 	- 默认false走相似度计算过滤，为true则默认全部匹配
		scores := make([]Score, len(refers))
		refers, scores = patchConfidence(refers, req.GetAnswer())
		refers = filterRefer(refers, scores, filter.GetFilter(), filter.GetTopN())
	}
	log.DebugContextf(ctx, "MatchRefer|after filterRefer|refers:%+v", refers)
	segmentDocs, err := s.dao.GetDocByIDs(ctx, slicex.Unique(docIDs), app.ID)
	if err != nil {
		return nil, err
	}
	qas, qaDocs, err := s.dao.GetQAAndRelateDocs(ctx, slicex.Unique(qaIDs), app.ID)
	if err != nil {
		return nil, err
	}
	knowIDs := make([]uint64, 0, len(segmentDocs))
	for _, v := range segmentDocs {
		knowIDs = append(knowIDs, v.RobotID)
	}
	for _, v := range qas {
		knowIDs = append(knowIDs, v.RobotID)
	}
	for _, v := range qaDocs {
		knowIDs = append(knowIDs, v.RobotID)
	}
	// 本应用引入了共享知识库才需要区分知识库名字
	ctx = pkg.WithCorpID(ctx, app.CorpID)
	knowInfoByID, hasShare, err := logicSearch.GetReferKnowledgeName(ctx, s.dao, app.BusinessID, knowIDs)
	if err != nil { //柔性放过,降级为不返回名称
		log.ErrorContextf(ctx, "MatchRefer GetReferKnowledgeName err:%v,appBizID:%v,knowIDs:%v", err, app.BusinessID, knowIDs)
	}
	newRefers := make([]model.Refer, 0, len(refers))
	data := make([]*pb.MatchReferRsp_Refer, 0, len(refers))
	for _, refer := range refers {
		appID, docID, docBizID, qaBizID, name, docName, url := s.getReferNameURL(ctx, refer, segmentDocs, qaDocs, qas)
		if req.GetIsRelease() && refer.DocType == model.ReferTypeSegment && url == "" {
			log.DebugContextf(ctx, "MatchRefer Release doc url ignore")
			continue
		}
		if refer.DocType == model.ReferTypeQA {
			refer.DocID = docID
		}
		knowledgeBizID, knowledgeName := app.BusinessID, ""
		if hasShare {
			if appID == app.ID {
				knowledgeName = i18n.Translate(ctx, i18nkey.KeyDefaultKnowledgeBase)
			} else if knowInfo, ok := knowInfoByID[appID]; ok {
				knowledgeBizID, knowledgeName = knowInfo.BusinessID, knowInfo.Name
			} else {
				log.WarnContextf(ctx, "MatchRefer app:%v not found,knowInfoByID:%v", appID, knowInfoByID)
			}
		}
		urlType := model.MatchReferUrlTypeDefault
		if req.GetIsRelease() && refer.DocType == model.ReferTypeSegment {
			doc, ok := segmentDocs[refer.DocID]
			if !ok || doc.HasDeleted() || doc.IsExpire() {
				log.DebugContextf(ctx, "MatchRefer Release doc HasDeleted:%v,IsExpire:%v",
					doc.HasDeleted(), doc.IsExpire())
			} else {
				if doc.IsReferOpen() && doc.Source == model.SourceFromWeb &&
					doc.ReferURLType == model.ReferURLTypeWebDocURL {
					log.DebugContextf(ctx, "MatchRefer Release web_doc url:%v,urlType:%v",
						doc.OriginalURL, model.MatchReferUrlTypeOriginalURL)
					url = doc.OriginalURL
					urlType = model.MatchReferUrlTypeOriginalURL
				}
			}
		}

		newRefers = append(newRefers, refer)
		data = append(data, &pb.MatchReferRsp_Refer{
			Url:            url,
			UrlType:        uint32(urlType),
			Name:           name,
			ReferId:        refer.BusinessID,
			ReferType:      refer.DocType,
			DocType:        refer.DocType,
			DocId:          docID,
			DocBizId:       docBizID,
			QaBizId:        qaBizID,
			DocName:        docName,
			KnowledgeBizId: knowledgeBizID,
			KnowledgeName:  knowledgeName,
			RelatedId:      refer.RelateID,
		})
	}

	if err := s.dao.CreateRefer(ctx, newRefers); err != nil {
		return nil, errs.ErrSystem
	}
	log.DebugContextf(ctx, "MatchRefer|rsp|Refers:%+v", data)
	return &pb.MatchReferRsp{Refers: data}, nil
}

// getRefersFromReq 构造参考来源
func (s *Service) getRefersFromReq(ctx context.Context, req *pb.MatchReferReq) (
	*admin.AppFilters, []model.Refer, []uint64, []uint64, error) {
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppReleaseScenes)
	if err != nil {
		return nil, nil, nil, nil, errs.ErrRobotNotFound
	}
	filter, ok := app.GetKnowledgeQa().GetFilters()[model.AppMatchReferFilterKey]
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("filter not found")
	}
	log.DebugContextf(ctx, "getRefersFromReq|filter:%+v", filter)
	docTypeMap := make(map[uint32]struct{})
	for _, f := range filter.GetFilter() {
		docTypeMap[f.GetDocType()] = struct{}{}
	}
	refers := make([]model.Refer, 0, len(req.GetDocs()))
	docIds, segmentIds, qaIds := make([]uint64, 0), make([]uint64, 0), make([]uint64, 0)
	docSegmentsKeyMap := make(map[string]struct{})
	qaIdMap := make(map[uint64]struct{})
	for _, doc := range req.GetDocs() {
		if doc.GetAnswer() == "" && doc.GetOrgData() == "" {
			continue
		}
		if _, ok := docTypeMap[doc.GetDocType()]; !ok {
			continue
		}
		// 2.3新增text2Sql数据：返回的文档段没有DocId
		if doc.GetDocId() == 0 && doc.GetRelatedId() == 0 &&
			(doc.DocType == model.DocTypeSegment || doc.DocType == model.DocTypeDB) {
			continue
		}

		if doc.DocType == model.DocTypeSegment {
			docIds = append(docIds, doc.GetDocId())
			if doc.GetRelatedId() > 0 {
				key := fmt.Sprintf("%d-$-%d", doc.GetDocId(), doc.GetRelatedId())
				if _, ok := docSegmentsKeyMap[key]; ok {
					// 需要对引用的文档片段去重
					continue
				}
				segmentIds = append(segmentIds, doc.GetRelatedId())
				docSegmentsKeyMap[key] = struct{}{}
			}
		}
		if doc.DocType == model.DocTypeQA {
			if _, exists := qaIdMap[doc.GetRelatedId()]; exists {
				continue // 问答去重检查
			}
			qaIds = append(qaIds, doc.GetRelatedId())
			qaIdMap[doc.GetRelatedId()] = struct{}{}
		}

		refers = append(refers, model.NewRefer(doc, req, app.GetId(), s.dao.GenerateSeqID()))
	}
	// 文档切片页码信息
	segmentPageInfoMap := make(map[uint64]*model.DocSegmentPageInfo)
	if len(segmentIds) > 0 {
		segmentPageInfoMap, err = s.dao.GetSegmentPageInfosBySegIDs(ctx, app.GetId(), segmentIds)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	for i := range refers {
		if refers[i].DocType != model.DocTypeSegment { // 只有文档切片才有页码信息
			continue
		}
		segmentPageInfo, ok := segmentPageInfoMap[refers[i].RelateID]
		if !ok {
			continue
		}
		if refers[i].IsBigData {
			refers[i].PageInfos = segmentPageInfo.BigPageNumbers
		} else {
			refers[i].PageInfos = segmentPageInfo.OrgPageNumbers
		}
		if len(refers[i].SheetInfos) == 0 { // 为空才填充，不为空说明是text2Sql的结果，是由检索带回来的
			refers[i].SheetInfos = segmentPageInfo.SheetData
		}
	}
	log.DebugContextf(ctx, "getRefersFromReq|filter:%+v|refers:%+v|docIDs:%+v|qaIDs:%+v",
		filter, refers, docIds, qaIds)
	return filter, refers, docIds, qaIds, nil
}

// getReferNameURL 获取引用名称和链接
func (s *Service) getReferNameURL(ctx context.Context, refer model.Refer, segmentDocs, qaDocs map[uint64]*model.Doc,
	qas map[uint64]*model.DocQA) (
	appID, docID, docBizID, qaBizID uint64, name, docName, url string) {
	if refer.DocType == model.ReferTypeQA {
		qa, ok := qas[refer.RelateID]
		if !ok {
			return 0, 0, 0, 0, "", "", ""
		}
		appID = qa.RobotID
		qaBizID = qa.BusinessID
		doc, ok := qaDocs[refer.RelateID]
		if !ok || doc.HasDeleted() || doc.IsExpire() {
			return appID, 0, 0, qaBizID, refer.Question, "", ""
		}
		docID = doc.ID
		docBizID = doc.BusinessID
		docName = doc.FileName
		if !doc.IsReferOpen() {
			return appID, docID, docBizID, qaBizID, refer.Question, docName, ""
		}
		return appID, docID, docBizID, qaBizID, refer.Question, docName,
			utils.When(doc.UseWebURL(), doc.WebURL, s.getReferURLByPreviewType(ctx, doc))
	}

	if refer.DocType == model.ReferTypeSegment {
		doc, ok := segmentDocs[refer.DocID]
		if !ok || doc.HasDeleted() || doc.IsExpire() {
			return 0, 0, 0, 0, "", "", ""
		}
		docName = doc.FileName
		if doc.FileNameInAudit != "" && refer.SessionType == model.SessionTypeExperience {
			// 文档重命名成功之后, 尚未发布的情况下, 内部引用文档名使用审核中的文档名
			docName = doc.GetFileNameByStatus()
		}
		if !doc.IsReferOpen() {
			return doc.RobotID, doc.ID, doc.BusinessID, 0, docName, docName, ""
		}
		return doc.RobotID, doc.ID, doc.BusinessID, 0, docName, docName,
			utils.When(doc.UseWebURL(), doc.WebURL, s.getReferURLByPreviewType(ctx, doc))
	}

	log.WarnContextf(ctx, "Unsupported refer type, refer: %+v", refer)
	return 0, 0, 0, 0, "", "", ""
}

// getReferURLByPreviewType 生成参考来源文件预览链接
func (s *Service) getReferURLByPreviewType(ctx context.Context, doc *model.Doc) string {
	if config.App().DocPreviewType == model.AppUseDownloadURL {
		// 目前只有私有化的minio使用下载链接预览，不需要判断存储类型
		log.DebugContextf(ctx, "文档ID:%d 文档类型:%s 采用下载地址", doc.ID, doc.FileType)
		return fmt.Sprintf("%s/%s%s", s.dao.GetDomain(ctx), doc.Bucket, doc.CosURL)
	}
	docPreviewConf := config.App().DocPreview
	docPreview, ok := docPreviewConf[doc.FileType]
	if !ok || len(docPreview.URL) == 0 {
		log.DebugContextf(ctx, "文档ID:%d 文档类型:%s 未配置文档预览地址", doc.ID, doc.FileType)
		return ""
	}
	return fmt.Sprintf("%s?id=%d", docPreview.URL, doc.BusinessID)
}

// filterRefer 过滤参考来源
func filterRefer(refers []model.Refer, scores []Score, filters []*admin.AppFiltersInfo, topN uint32) []model.Refer {
	filterMap := make(map[uint32]*admin.AppFiltersInfo)
	for _, filter := range filters {
		filterMap[filter.GetDocType()] = filter
	}

	group := make(map[uint32][]model.Refer)
	for i, r := range refers {
		filter, ok := filterMap[r.DocType]
		if !ok || filter.GetTopN() == 0 {
			continue
		}
		thres := filter.GetRougeScore()
		if scores[i].F <= thres.GetF() && scores[i].P <= thres.GetP() && scores[i].R <= thres.GetR() {
			continue
		}
		group[r.DocType] = append(group[r.DocType], r)
	}

	for typ := range group {
		sort.SliceStable(group[typ], func(i, j int) bool {
			return group[typ][i].Confidence > group[typ][j].Confidence
		})
	}

	var refs []model.Refer
filter:
	for _, filter := range filters {
		for i, refer := range group[filter.GetDocType()] {
			if len(refs) >= int(topN) {
				break filter
			}
			if i >= int(filter.GetTopN()) {
				break
			}
			refs = append(refs, refer)
		}
	}
	return refs
}

// SearchPreviewRejectedQuestion 拒答问题测评库查询
func (s *Service) SearchPreviewRejectedQuestion(ctx context.Context, req *pb.SearchPreviewRejectedQuestionReq) (
	*pb.SearchPreviewRejectedQuestionRsp, error) {
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	ctx = clues.NewTrackContext(ctx)
	rsp := new(pb.SearchPreviewRejectedQuestionRsp)
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppTestScenes)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	robotFilter, ok := app.GetKnowledgeQa().GetFilters()[model.AppSearchRejectedQuestionPreview]
	if !ok {
		return rsp, fmt.Errorf("robot not filer key:%s", model.AppSearchRejectedQuestionPreview)
	}
	filters := make([]*retrieval.SearchVectorReq_Filter, 0, 1)
	for _, filter := range robotFilter.GetFilter() {
		filters = append(filters, &retrieval.SearchVectorReq_Filter{
			IndexId:    s.getType(filter.GetDocType()),
			Confidence: filter.GetConfidence(),
			TopN:       filter.GetTopN(),
			DocType:    filter.GetDocType(),
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return rsp, err
	}
	//
	embeddingModelName, err := logicKnowConfig.GetKnowledgeBaseConfig(ctx, app.CorpBizId, app.GetAppBizId(),
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeBaseConfig err: %v", err)
		return rsp, err
	}
	searchVectorReq := &retrieval.SearchVectorReq{
		RobotId:          app.GetId(),
		BotBizId:         app.GetAppBizId(),
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             robotFilter.GetTopN(),
		EmbeddingVersion: app.GetKnowledgeQa().GetEmbedding().GetVersion(),
		Rerank: &retrieval.SearchVectorReq_Rerank{
			Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable,
		},
		// 传FilterKey标识拒答问题的请求
		FilterKey:          model.AppSearchRejectedQuestionPreview,
		SubQuestions:       req.GetSubQuestions(),
		SearchStrategy:     getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy()),
		ModelName:          req.GetModelName(),
		EmbeddingModelName: embeddingModelName, // 需要传embedding模型名
	}
	searchVectorRsp, err := client.SearchVector(ctx, searchVectorReq)
	if err != nil {
		log.ErrorContextf(ctx, "SearchPreviewRejectedQuestion req: %+v rsp:%+v searchVectorRsp:%+v, err:%+v",
			req, rsp, searchVectorRsp, err)
		return rsp, err
	}
	rejectedQuestions := make([]*pb.SearchPreviewRejectedQuestionRsp_RejectedQuestions, 0)
	for _, doc := range searchVectorRsp.GetDocs() {
		if doc.GetIndexId() == model.RejectedQuestionReviewVersionID ||
			doc.GetIndexId() == model.RejectedQuestionSimilarVersionID {
			rejectedQuestion, err := s.dao.GetRejectedQuestionByID(ctx, app.GetCorpId(), app.GetId(), doc.GetId())
			if err != nil {
				return rsp, errs.ErrRejectedQuestionNotFound
			}
			if rejectedQuestion == nil {
				return rsp, errs.ErrRejectedQuestionNotFound
			}
			rejectedQuestions = append(rejectedQuestions, &pb.SearchPreviewRejectedQuestionRsp_RejectedQuestions{
				Id:         rejectedQuestion.ID,
				Question:   rejectedQuestion.Question,
				Confidence: doc.GetConfidence(),
			})
		}
	}
	rsp.List = rejectedQuestions
	log.DebugContextf(ctx, "req: %+v rsp:%+v", req, rsp)
	return rsp, nil
}

// SearchReleaseRejectedQuestion 拒答问题线上库查询
func (s *Service) SearchReleaseRejectedQuestion(ctx context.Context, req *pb.SearchReleaseRejectedQuestionReq) (
	*pb.SearchReleaseRejectedQuestionRsp, error) {
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	ctx = clues.NewTrackContext(ctx)
	rsp := new(pb.SearchReleaseRejectedQuestionRsp)
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppReleaseScenes)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	robotFilter, ok := app.GetKnowledgeQa().GetFilters()[model.AppSearchRejectedQuestionRelease]
	if !ok {
		return rsp, fmt.Errorf("filters not found")
	}
	filters := make([]*retrieval.SearchReq_Filter, 0, len(robotFilter.GetFilter()))
	for _, f := range robotFilter.GetFilter() {
		filters = append(filters, &retrieval.SearchReq_Filter{
			DocType:    f.GetDocType(),
			Confidence: f.GetConfidence(),
			TopN:       f.GetTopN(),
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return rsp, err
	}
	searchReq := &retrieval.SearchReq{
		RobotId:   app.GetId(),
		VersionId: app.GetKnowledgeQa().GetQaVersion(),
		Question:  req.GetQuestion(),
		Filters:   filters,
		TopN:      robotFilter.GetTopN(),
		Rerank:    &retrieval.SearchReq_Rerank{Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable},
		// 传FilterKey标识拒答问题的请求
		FilterKey:      model.AppSearchRejectedQuestionRelease,
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy()),
		ModelName:      req.GetModelName(),
	}
	searchRsp, err := s.dao.Search(ctx, searchReq)
	if err != nil {
		log.ErrorContextf(ctx, "SearchReleaseRejectedQuestion req: %+v rsp:%+v searchRsp:%+v, err:%+v",
			req, rsp, searchRsp, err)
		return rsp, err
	}
	rejectedQuestions := make([]*pb.SearchReleaseRejectedQuestionRsp_RejectedQuestions, 0)
	for _, doc := range searchRsp.GetDocs() {
		if doc.GetDocType() == uint32(model.DocTypeRejectedQuestion) {
			rejectedQuestions = append(rejectedQuestions, &pb.SearchReleaseRejectedQuestionRsp_RejectedQuestions{
				Id:         doc.GetRelatedId(),
				Question:   doc.GetQuestion(),
				Confidence: doc.GetConfidence(),
			})
		}
	}
	rsp.List = rejectedQuestions
	log.DebugContextf(ctx, "req: %+v rsp:%+v", req, rsp)
	return rsp, nil
}

// [向量带标签检索]
// 逻辑运算表达式 and 与、or 或、not 非
// 字符串类型表达式 in 匹配任意一个字符串值、not in 排除所有字符串值、all in 匹配所有字符串值、= 匹配单个字符串值、!= 排除单个字符串值
// [表达式]
// general_vector：业务定义的特殊属性，表示通用知识，default：业务定义的特殊标签值，表示默认/任意数据
// 样例 general_vector="default" or (car in ("c1","c2","default") and source in ("s1","s2","default"))
// 样例逻辑为：检索所有符合 "通用文档 或者（车型为c1、c2、任意车型 且 来源为s1、s2、任意来源)" 标签的知识

// fillLabelExprString TODO
func fillLabelExprString(labels []*pb.VectorLabel) string {
	if len(labels) == 0 {
		return ""
	}
	labelExprs := make([]string, 0)
	for _, label := range labels {
		values := make([]string, 0)
		for _, value := range label.GetValues() {
			values = append(values, fmt.Sprintf("\"%s\"", value))
		}
		values = append(values, fmt.Sprintf("\"%s\"", config.App().AttributeLabel.FullLabelValue))
		labelExprs = append(labelExprs, fmt.Sprintf("%s in (%s)", label.GetName(), strings.Join(values, ",")))
	}
	return fmt.Sprintf("%s=\"%s\" or (%s)", config.App().AttributeLabel.GeneralVectorAttrKey,
		config.App().AttributeLabel.FullLabelValue, strings.Join(labelExprs, " and "))
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

// CustomSimilarity 计算相似度
func (s *Service) CustomSimilarity(ctx context.Context, req *pb.CustomSimilarityReq) (*pb.CustomSimilarityRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	return &pb.CustomSimilarityRsp{}, nil
}

// handleCustomVariablesLabels 处理请求CustomVariables中的labels
func handleCustomVariablesLabels(labels []*pb.VectorLabel, searchRange *admin.AppSearchRange,
	customVariables map[string]string) []*pb.VectorLabel {
	if len(searchRange.GetCondition()) != 0 && len(searchRange.GetApiVarAttrInfos()) > 0 { // 知识检索范围不为空 取customVariables中的值查找映射关系中的标签
		labels = make([]*pb.VectorLabel, 0)
		for k, v := range customVariables {
			label := &pb.VectorLabel{
				Name:   k,
				Values: strings.Split(v, model.CustomVariableSplitSep),
			}
			labels = append(labels, label)
		}
		if len(labels) == 0 {
			return labels
		}
		return handleReqLabels(searchRange, labels)
	} else { // 知识检索范围不为空 取请求中携带的标签
		return labels
	}
}

// handleReqLabels 处理请求labels
func handleReqLabels(searchRange *admin.AppSearchRange, labels []*pb.VectorLabel) []*pb.VectorLabel {
	var newLabels []*pb.VectorLabel
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
						newLabels = append(newLabels, &pb.VectorLabel{
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

// (s *Service) similarLabels2StandardLabels 把标签列表中的相似标签转换成主标签（标准标签），未命中相似标签就保持不变
func (s *Service) similarLabels2StandardLabels(ctx context.Context, robotID uint64,
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
		labelRedisValue, err := s.dao.GetAttributeLabelsRedis(ctx, robotID, label.GetName(), envType)
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

func (s *Service) searchDBAnswer(ctx context.Context,
	bc *logicSearch.BotContext, dbTableID uint64) (*knowledge.SearchKnowledgeRsp, error) {
	log.InfoContextf(ctx, "searchDBAnswer req:%s", utils2.Any2String(bc))
	// dbTable反查询DBSourceID
	table, err := dao.GetDBTableDao().GetByBizID(ctx, bc.App.CorpBizId, bc.AppBizID, dbTableID)
	if err != nil {
		log.ErrorContextf(ctx, "searchDBAnswer GetDBTableDao.GetByBizID fail, err:%v", err)
		return nil, err
	}
	resultDoc := &knowledge.SearchKnowledgeRsp_SearchRsp_Doc{
		DocType:   model.DocTypeQA,
		RelatedId: table.DBTableBizID,
		Question:  bc.Question,
	}

	// 运行SQL获取结果
	_, result, _, errMsg, err := db_source.RunSql(ctx, bc.AppBizID, table.DBSourceBizID, bc.Question, nil, s.dao)
	if err != nil {
		if errMsg == "" {
			errMsg = err.Error()
		}
		log.ErrorContextf(ctx, "searchDBAnswer RunSql fail, errMsg=%+v, err:%v", errMsg, err)
		resultDoc.Answer = errMsg
	} else {
		answer, err := jsoniter.MarshalToString(result)
		if err != nil {
			log.ErrorContextf(ctx, "searchDBAnswer Marshal RunSql result fail, err:%v", err)
			resultDoc.Answer = err.Error()
		} else {
			resultDoc.Answer = answer
		}
	}

	// 拼接rsp结果返回
	resp := &knowledge.SearchKnowledgeRsp{
		KnowledgeType: knowledge.KnowledgeType_DOC_QA,
		SceneType:     bc.SceneType,
		Rsp: &knowledge.SearchKnowledgeRsp_SearchRsp{
			Docs: []*knowledge.SearchKnowledgeRsp_SearchRsp_Doc{resultDoc},
		},
	}
	return resp, nil
}
