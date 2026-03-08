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
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/contextx/clues"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/entity/search"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	logicSearch "git.woa.com/adp/kb/kb-config/internal/logic/search"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/algorithm/rouge"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/internal/util/linker"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	adpCommon "git.woa.com/adp/pb-go/common"
	knowPB "git.woa.com/adp/pb-go/kb/kb_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/exp/maps"
)

// CustomSearchPreview 自定义对话评测查询【op检索用】
func (s *Service) CustomSearchPreview(ctx context.Context, req *pb.CustomSearchPreviewReq) (*pb.CustomSearchPreviewRsp, error) {
	return s.CustomSearchPreviewWithLabelConfig(ctx, req, nil)
}

// CustomSearchPreviewWithLabelConfig 自定义对话评测查询
func (s *Service) CustomSearchPreviewWithLabelConfig(ctx context.Context, req *pb.CustomSearchPreviewReq, labelConfig *labelEntity.CustomLabelConfig) (*pb.CustomSearchPreviewRsp, error) {
	rsp := new(pb.CustomSearchPreviewRsp)
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppTestScenes)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	replaceApp := app
	if newAppID, ok := config.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.BizId]; ok {
		replaceApp, err = s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(newCtx, newAppID, entity.AppTestScenes)
		clues.AddTrackDataWithError(newCtx, "getAppByAppBizID", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		logx.D(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v", app, replaceApp)
	}
	req.Labels = s.transCustomReqLabels(ctx, entity.AppTestScenes, replaceApp, labelConfig, req.GetLabels()) // 标签转换
	searchVectorReq, err := s.getCustomSearchVectorReq(newCtx, app, req, labelConfig, replaceApp)
	if err != nil {
		return rsp, err
	}
	searchVectorRsp, err := s.rpc.RetrievalDirectIndex.SearchMultiKnowledgePreview(newCtx, searchVectorReq)
	if err != nil {
		return rsp, err
	}
	linkContents, err := s.docLogic.GetLinkContentsFromSearchVectorResponse(
		newCtx, replaceApp.PrimaryId, searchVectorRsp.GetDocs(),
		func(doc *retrieval.SearchVectorRsp_Doc, qa *qa.DocQA) any {
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
		func(doc *retrieval.SearchVectorRsp_Doc, segment *segment.DocSegmentExtend) any {
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
	rsp.Docs = linker.Link(newCtx, linkContents,
		func(t *pb.CustomSearchPreviewRsp_Doc, v linker.Content) *pb.CustomSearchPreviewRsp_Doc {
			t.OrgData = v.Value
			return t
		},
	)
	logx.D(ctx, "req: %+v rsp:%+v", req, rsp)
	return searchRspPostProcess(newCtx, req.GetUsePlaceholder(), rsp), nil
}

func getRerankModel(app *entity.App) (entity.RerankModelConfig, error) {
	f, ok := app.QaConfig.Filters[entity.AppRerankFilterKey]
	if !ok {
		return entity.RerankModelConfig{}, fmt.Errorf("AppRerankFilterKey filter not found")
	}
	modelInfo, ok := app.QaConfig.Model[entity.RerankModel]
	if !ok {
		return entity.RerankModelConfig{}, fmt.Errorf("RerankModel filter not found")
	}
	return entity.RerankModelConfig{
		ModelName: modelInfo.ModelName,
		TopN:      f.TopN,
		Enable:    app.QaConfig.EnableRerank,
	}, nil
}

func (s *Service) getCustomSearchVectorReq(ctx context.Context, app *entity.App, req *pb.CustomSearchPreviewReq,
	labelConfig *labelEntity.CustomLabelConfig, replaceApp *entity.App) (*retrieval.SearchMultiKnowledgeReq, error) {
	filters := make([]*retrieval.SearchFilter, 0, 1)
	for _, filter := range req.GetFilters() {
		filters = append(filters, &retrieval.SearchFilter{
			IndexId:         entity.GetType(filter.GetDocType()),
			Confidence:      filter.GetConfidence(),
			TopN:            filter.GetTopN(),
			DocType:         filter.GetDocType(),
			LabelExprString: fillLabelExprString(req.GetLabels()), // 2.6之前旧的表达式，保持逻辑不变
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		logx.E(ctx, "get rerank model err:%v", err)
		return nil, err
	}
	// 工作流使用入参检索配置
	searchStrategy := getSearchStrategy(app.QaConfig.SearchStrategy)
	if labelConfig != nil && labelConfig.SearchStrategy != nil {
		searchStrategy = getWorkflowSearchStrategy(labelConfig.SearchStrategy)
		logx.D(ctx, "getCustomSearchVectorReq|WorkflowSearchStrategy|StrategyType:%+v|Enhancement:%+v",
			searchStrategy.GetStrategyType(), searchStrategy.GetTableEnhancement())
	}
	return &retrieval.SearchMultiKnowledgeReq{
		RobotId:  replaceApp.PrimaryId,
		BotBizId: replaceApp.BizId,
		Question: req.GetQuestion(),
		TopN:     req.GetTopN(),
		Rerank: &retrieval.Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: searchStrategy,
		SearchData: []*retrieval.SearchData{{
			KnowledgeId:      replaceApp.PrimaryId,
			KnowledgeBizId:   replaceApp.BizId,
			EmbeddingVersion: replaceApp.Embedding.Version,
			Filters:          filters,
			FilterKey:        entity.AppSearchPreviewFilterKey,
			Labels:           convertSearchVectorLabel(req.GetLabels()),
			LabelExpression:  s.transCustomLabelExpression(entity.AppTestScenes, replaceApp, labelConfig, req.GetLabels()),
		}},
	}, nil
}

func (s *Service) getType(docType uint32) uint64 {
	typ := entity.ReviewVersionID
	if docType == entity.DocTypeSegment {
		typ = entity.SegmentReviewVersionID
	} else if docType == entity.DocTypeRejectedQuestion {
		typ = entity.RejectedQuestionReviewVersionID
	}
	return typ
}

// CustomSearch 自定义对话评测，prod环境使用【op检索用】
func (s *Service) CustomSearch(ctx context.Context, req *pb.CustomSearchReq) (*pb.CustomSearchRsp, error) {
	return s.CustomSearchWithLabelConfig(ctx, req, nil)
}

// CustomSearchWithLabelConfig 自定义对话评测
func (s *Service) CustomSearchWithLabelConfig(ctx context.Context, req *pb.CustomSearchReq,
	labelConfig *labelEntity.CustomLabelConfig) (*pb.CustomSearchRsp, error) {
	rsp := new(pb.CustomSearchRsp)
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppReleaseScenes)
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	replaceApp := app
	if newAppID, ok := config.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.BizId]; ok {
		replaceApp, err = s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, newAppID, entity.AppReleaseScenes)
		clues.AddTrackDataWithError(ctx, "DescribeAppInfoUsingScenesById", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		logx.D(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v",
			app, replaceApp)
	}
	if replaceApp.QaVersion == 0 {
		logx.W(ctx, "bot_biz_id:%d qaVersion is 0", replaceApp.BizId)
		return rsp, nil
	}
	req.Labels = s.transCustomReqLabels(newCtx, entity.AppReleaseScenes, replaceApp, labelConfig, req.GetLabels()) // 标签转换
	filters := make([]*retrieval.SearchFilter, 0, len(req.GetFilters()))
	for _, filter := range req.GetFilters() {
		filters = append(filters, &retrieval.SearchFilter{
			DocType:    filter.GetDocType(),
			Confidence: filter.GetConfidence(),
			TopN:       filter.GetTopN(),
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		logx.E(ctx, "get rerank model err:%v", err)
		return rsp, err
	}
	// 工作流使用入参检索配置
	searchStrategy := getSearchStrategy(app.QaConfig.SearchStrategy)
	if labelConfig != nil && labelConfig.SearchStrategy != nil {
		searchStrategy = getWorkflowSearchStrategy(labelConfig.SearchStrategy)
		logx.D(ctx, "getCustomSearchVectorReq|WorkflowSearchStrategy|StrategyType:%+v|Enhancement:%+v",
			searchStrategy.GetStrategyType(), searchStrategy.GetTableEnhancement())
	}
	searchReq := &retrieval.SearchMultiKnowledgeReq{
		RobotId:        replaceApp.PrimaryId,
		Question:       req.GetQuestion(),
		TopN:           req.GetTopN(),
		Rerank:         &retrieval.Rerank{Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable},
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: searchStrategy,
		ModelName:      req.GetModelName(),
		SearchData: []*retrieval.SearchData{{
			KnowledgeId:      replaceApp.PrimaryId,
			KnowledgeBizId:   replaceApp.BizId,
			FilterKey:        entity.AppSearchReleaseFilterKey,
			Filters:          filters,
			Labels:           convertSearchVectorLabel(req.GetLabels()),
			LabelExpression:  s.transCustomLabelExpression(entity.AppReleaseScenes, replaceApp, labelConfig, req.GetLabels()),
			QaVersion:        replaceApp.QaVersion,
			EmbeddingVersion: replaceApp.Embedding.Version,
		}},
	}
	searchRsp, err := s.search(newCtx, searchReq)
	if err != nil {
		return rsp, err
	}
	linkContents, err := s.docLogic.GetLinkContentsFromSearchResponse(
		newCtx, replaceApp.PrimaryId, searchRsp.GetDocs(),
		func(doc *retrieval.SearchRsp_Doc, qa *qa.DocQA) any {
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
		func(doc *retrieval.SearchRsp_Doc, segment *segment.DocSegmentExtend) any {
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
	rsp.Docs = linker.Link(newCtx, linkContents, func(t *pb.CustomSearchRsp_Doc, v linker.Content) *pb.CustomSearchRsp_Doc {
		t.OrgData = v.Value
		return t
	})
	logx.D(ctx, "req: %+v, rsp: %+v", req, rsp)
	return searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp), nil
}

// Search 向量搜索
func (s *Service) search(ctx context.Context, req *retrieval.SearchMultiKnowledgeReq) (*retrieval.SearchRsp, error) {
	botBizID, err := s.dao.GetBotBizIDByID(ctx, req.RobotId)
	if err != nil {
		return nil, err
	}
	req.BotBizId = botBizID
	return s.rpc.Retrieval.SearchMultiKnowledgeRelease(ctx, req)
}

// transCustomReqLabels 转换自定义检索的标签
func (s *Service) transCustomReqLabels(ctx context.Context, scenes uint32, app *entity.App,
	labelConfig *labelEntity.CustomLabelConfig, labels []*pb.VectorLabel) []*pb.VectorLabel {
	labels = make([]*pb.VectorLabel, 0)
	envType, ok := entity.Scenes2AttrLabelEnvType[scenes]
	if !ok {
		return labels
	}
	if labelConfig == nil {
		// handleReqLabels 之后 VectorLabel中的Name就是AttrKey
		return s.similarLabels2StandardLabels(ctx, app.PrimaryId, labels, envType)
	}
	return labels
}

// transCustomLabelExpression 转换自定义标签检索表达式
func (s *Service) transCustomLabelExpression(scenes uint32, app *entity.App,
	labelConfig *labelEntity.CustomLabelConfig, labels []*pb.VectorLabel) *retrieval.LabelExpression {
	// 未定义配置 取应用的配置值
	if labelConfig == nil {
		switch scenes {
		case entity.AppTestScenes:
			return fillLabelExpression(labels,
				getAppLabelCondition(app.BizId, app.QaConfig.SearchRange.Condition))
		case entity.AppReleaseScenes:
			return fillLabelExpression(labels,
				getAppLabelCondition(app.BizId, app.QaConfig.SearchRange.Condition))
		default:
			return fillLabelExpression(labels, getAppLabelCondition(app.BizId, entity.AppSearchConditionAnd))
		}
	}
	// 有定义配置 取定义的配置值
	if labelConfig.IsLabelOrGeneral {
		return fillLabelExpression(labels, // 携带全局default标签
			getAppLabelCondition(app.BizId, labelConfig.LabelLogicOpr))
	} else {
		return fillLabelWithoutGeneralVectorExpression(labels, // 不带全局default标签
			getAppLabelCondition(app.BizId, labelConfig.LabelLogicOpr))
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

	sheetData := make([]*pb.PageContent_SheetData, 0)
	for _, info := range extra.GetTableInfos() {
		sheetData = append(sheetData, &pb.PageContent_SheetData{
			SheetName: info.GetTableName(),
		})
	}

	if len(sheetData) == 0 {
		return ""
	}

	sheetInfoStr, _ := jsonx.MarshalToString(sheetData)
	return sheetInfoStr
}

// convertSearchRetrievalSheetInfo 转换结果类型
func convertSearchRetrievalSheetInfo(extra *retrieval.SearchRsp_Doc_Text2SQLExtra) string {
	if extra == nil || len(extra.GetTableInfos()) == 0 {
		return ""
	}

	sheetData := make([]*pb.PageContent_SheetData, 0)
	for _, info := range extra.GetTableInfos() {
		sheetData = append(sheetData, &pb.PageContent_SheetData{
			SheetName: info.GetTableName(),
		})
	}

	if len(sheetData) == 0 {
		return ""
	}

	sheetInfoStr, _ := jsonx.MarshalToString(sheetData)
	return sheetInfoStr
}

// SearchPreview 对话评测
// Deprecated: 该接口即将废弃
// TODO(ericjwang): 原子能力在调用，应该怎么处理？
func (s *Service) SearchPreview(ctx context.Context, req *pb.SearchPreviewReq) (*pb.SearchPreviewRsp, error) {
	logx.E(ctx, "准备删除的接口SearchPreview收到了请求 deprecated interface req:%+v", req)
	// return s.SearchPreviewWithCustomVariables(ctx, req)
	return &pb.SearchPreviewRsp{}, nil
}

// SearchPreviewWithCustomVariables 对话评测带自定义参数
// Deprecated: 该接口即将废弃
func (s *Service) SearchPreviewWithCustomVariables(ctx context.Context,
	req *pb.SearchPreviewReq, customVariables map[string]string) (*pb.SearchPreviewRsp, error) {
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	ctx = clues.NewTrackContext(ctx)
	clues.AddTrackData(ctx, "SearchPreview.req", req)
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppTestScenes)
	clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	replaceApp := app
	if newAppID, ok := config.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.BizId]; ok {
		replaceApp, err = s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, newAppID, entity.AppTestScenes)
		clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		logx.D(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v",
			app, replaceApp)
	}
	if app.QaConfig == nil || replaceApp.QaConfig == nil {
		return nil, errs.ErrAppTypeSupportFilters
	}

	key := gox.IfElse(req.GetFilterKey() == "", entity.AppSearchPreviewFilterKey, req.GetFilterKey())
	filter, ok := app.QaConfig.Filters[key]
	if !ok {
		return nil, fmt.Errorf("robot %s filter not found scenes %d", key, entity.AppTestScenes)
	}
	clues.AddTrackData(ctx, "app.GetFilter", map[string]any{
		"scenes": entity.AppTestScenes, "key": key, "filter": filter,
	})
	if replaceApp.QaConfig != nil {
		req.Labels = handleCustomVariablesLabels(req.Labels,
			replaceApp.QaConfig.SearchRange, customVariables)
	}
	// handleReqLabels 之后 VectorLabel中的Name就是AttrKey

	newLabels := s.similarLabels2StandardLabels(ctx, replaceApp.PrimaryId, req.GetLabels(), labelEntity.AttributeLabelsPreview)
	filters := make([]*retrieval.SearchFilter, 0, len(filter.Filter))
	for _, f := range filter.Filter {
		if f.DocType == entity.DocTypeSearchEngine {
			continue
		}
		if f.DocType == entity.DocTypeTaskFlow {
			continue
		}
		if key == entity.AppSearchPreviewFilterKey && !f.IsEnabled {
			continue
		}
		filters = append(filters, &retrieval.SearchFilter{
			IndexId:         uint64(f.IndexID),
			Confidence:      f.Confidence,
			TopN:            f.TopN,
			DocType:         f.DocType,
			LabelExprString: fillLabelExprString(newLabels), // 2.6之前旧的表达式，保持逻辑不变
		})
	}
	// 敏捷发布：新增检索范围，支持指定范围检索
	filters = filterSearchVectorScope(ctx, req.GetSearchScope(), filters)
	rerank, err := getRerankModel(app)
	clues.AddTrackData(ctx, "app.GetRerankModel", map[string]any{
		"scenes": entity.AppTestScenes, "rerank": rerank, "err": err,
	})
	if err != nil {
		logx.E(ctx, "get rerank model err:%v", err)
		return nil, err
	}

	searchReq := &retrieval.SearchMultiKnowledgeReq{
		RobotId:  replaceApp.PrimaryId,
		BotBizId: replaceApp.BizId,
		Question: req.GetQuestion(),
		TopN:     filter.TopN,
		Rerank: &retrieval.Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		ImageUrls:      req.GetImageUrls(),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.QaConfig.SearchStrategy),
		ModelName:      req.GetModelName(),
		SearchData: []*retrieval.SearchData{{
			KnowledgeId:      replaceApp.PrimaryId,
			KnowledgeBizId:   replaceApp.BizId,
			FilterKey:        key,
			Filters:          filters,
			Labels:           convertSearchVectorLabel(newLabels),
			LabelExpression:  fillLabelExpression(newLabels, getAppLabelCondition(replaceApp.BizId, replaceApp.QaConfig.SearchRange.Condition)),
			EmbeddingVersion: replaceApp.Embedding.Version,
		}},
	}

	searchVectorRsp, err := s.rpc.RetrievalDirectIndex.SearchMultiKnowledgePreview(newCtx, searchReq)
	if err != nil {
		logx.E(newCtx, "DirectSearchVector robotID: %d, err: %v", app.PrimaryId, err)
		return nil, err
	}

	t1 := time.Now()
	var docs []*pb.SearchPreviewRsp_Doc
	docs, err = s.getPreviewRspDocByApp(newCtx, searchVectorRsp.GetDocs(), replaceApp.PrimaryId)
	clues.AddTrackData(ctx, "getPreviewRspDocByApp", map[string]any{
		"app": app, "req-docs": searchVectorRsp.GetDocs(), "resp-docs": docs, "err": err != nil, "ELAPSED": time.Since(t1).String(),
	})
	if err != nil {
		return nil, err
	}

	rsp := &pb.SearchPreviewRsp{Docs: docs}
	post := searchRspPostProcess(newCtx, req.GetUsePlaceholder(), rsp)
	clues.AddTrackData(newCtx, "searchRspPostProcess", map[string]any{
		"usePlaceholder": req.GetUsePlaceholder(), "SearchPreviewRsp": rsp, "post": post,
	})
	return post, nil
}

// searchAnswer 检索内容[统一接口]
func (s *Service) searchAnswer(ctx context.Context, bc *logicSearch.BotContext) (*pb.SearchKnowledgeRsp, error) {
	logx.I(ctx, "searchAnswer req:%s", jsonx.MustMarshalToString(bc))
	corpBizId := contextx.Metadata(ctx).CorpBizID()
	// 处理策略
	searchStrategy := bc.HandleSearchStrategy(ctx)
	// 处理rerank配置
	rerank, err := bc.HandleRerank(ctx)
	if err != nil {
		logx.E(ctx, "get rerank model err:%v", err)
		return nil, err
	}
	// 处理默认知识库的召回数量
	recallNum, err := bc.HandleRetrievalRecallNum(ctx)
	if err != nil {
		logx.E(ctx, "HandleRetrievalRecallNum failed, err: %+v", err)
		return nil, err
	}
	searchMultiReq := &retrieval.SearchMultiKnowledgeReq{
		RobotId:        bc.ReplaceApp.PrimaryId,
		BotBizId:       bc.ReplaceApp.BizId,
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
		logx.W(ctx, "retrieval knowledge list empty")
		return &pb.SearchKnowledgeRsp{}, nil
	}
	logx.I(ctx, "searchAnswer knowledge list:%+v", kgs)
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

		t2sConf, err := s.kbLogic.GetKnowledgeText2sqlModelConfig(ctx, corpBizId, bc.AppBizID,
			kgInfo.KnowledgeBizID, kgInfo.IsShareKG)
		if err != nil {
			logx.W(ctx, "searchAnswer knowledge kgInfo.SearchStrategy.TableEnhancement:%+t,"+
				" fetch text2sql config of kg failed, err: %+v", kgInfo.SearchStrategy.TableEnhancement, err)
		} else if t2sConf != nil {
			data.SearchStrategy.Text2SqlModelConfig = t2sConf
		}
		logx.I(ctx, "searchAnswer knowledge kgInfo.SearchStrategy.TableEnhancement:%+t,"+
			" fetch text2sql config of kg (id:%d)：%s", kgInfo.SearchStrategy.TableEnhancement, kgInfo.KnowledgeID,
			jsonx.MustMarshalToString(data.SearchStrategy.Text2SqlModelConfig))
		// 基于知识库配置二次过滤
		data.Filters = bc.FilterCloseKnowledge(ctx, kgInfo.WorkflowKGCfg.GetCloseKnowledge(), kgInfo.Filters)
		if len(data.Filters) == 0 {
			logx.W(ctx, "retrieval knowledge filter list empty, %d, filterKey:%s, filters:%+v, closeKnowledge:%v", kgInfo.KnowledgeID, kgInfo.FilterKey, kgInfo.Filters, kgInfo.WorkflowKGCfg.GetCloseKnowledge())
			continue
		}
		// 处理知识库标签
		kcgLabel := bc.HandleSearchLabels(ctx, kgInfo, 0)            // 获取全局适用的filter_label
		if bc.KnowledgeType == knowPB.SearchKnowledgeType_WORKFLOW { // 如果是工作流问答的标签需要特殊处理(不能带DocID)
			for _, v := range data.Filters {
				if v.DocType == entity.DocTypeQA || v.DocType == entity.DocTypeDB {
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
			go func(newCtx context.Context) { // 异步上报
				defer gox.Recover()
				counterInfo := &common.CounterInfo{
					CorpBizId:       bc.ReplaceApp.CorpBizId,
					SpaceId:         bc.ReplaceApp.SpaceId,
					AppBizId:        bc.ReplaceApp.BizId,
					StatisticObject: adpCommon.StatObject_STAT_OBJECT_KB,
					StatisticType:   adpCommon.StatType_STAT_TYPE_CALL,
					ObjectId:        strconv.FormatUint(kgInfo.KnowledgeBizID, 10),
					ObjectName:      kgInfo.KnowledgeName,
					Count:           1,
				}
				common.Counter(newCtx, counterInfo, s.rpc)
			}(trpc.CloneContext(ctx))
		}
	}
	if len(searchMultiReq.SearchData) == 0 {
		logx.W(ctx, "retrieval knowledge search data list empty")
		return &pb.SearchKnowledgeRsp{}, nil
	}
	searchMultiRsp, err := s.SearchMultiKnowledge(ctx, bc, searchMultiReq)
	if err != nil {
		logx.E(ctx, "SearchMultiKnowledge robotID: %d, err: %v", bc.App.PrimaryId, err)
		return nil, err
	}
	if searchMultiRsp != nil && searchMultiRsp.GetRsp() != nil {
		// 上报token用量【embedding和rerank】
		if bc.BillingInfo.NeedReportEmbeddingAndRerank && len(searchMultiRsp.GetRsp().GetDocs()) > 0 { // 如果没有检索到文档，则不上报token用量
			gox.GoWithContext(ctx, func(ctx context.Context) {
				if bc.BillingInfo.CallSource != adpCommon.CallSource_CALL_SOURCE_RAG { // rag场景不上报token用量
					s.reportTokenUsage(ctx, searchMultiRsp.TokenUsages, bc)
				}
			})
			s.financeLogic.ReportBusinessUsage(ctx, bc.AppBizID)
		}
		rspDocs := searchMultiRsp.GetRsp().GetDocs()
		// 获取文档的业务ID
		// 获取所有检索到的文档信息，需要先按文档id去重
		docIDMap := make(map[uint64]struct{})
		for _, doc := range rspDocs {
			if doc.GetDocType() == entity.DocTypeSegment {
				// 只对检索出的文档鉴权
				docIDMap[doc.DocId] = struct{}{}
			}
		}
		if len(docIDMap) == 0 {
			return searchMultiRsp, nil
		}

		docFilter := &docEntity.DocFilter{
			RouterAppBizID: bc.ReplaceApp.BizId,
			IDs:            maps.Keys(docIDMap),
		}
		selectColumns := []string{docEntity.DocTblColId, docEntity.DocTblColBusinessId, docEntity.DocTblColRobotId, docEntity.DocTblColFileName,
			docEntity.DocTblColCustomerKnowledgeId, docEntity.DocTblColAttributeFlag}
		docs, err := s.docLogic.GetDocList(ctx, selectColumns, docFilter)
		if err != nil {
			return nil, err
		}
		if len(docs) != len(docFilter.IDs) {
			// 降级处理
			logx.W(ctx, "some docs not found, len(docs):%d docIDs:%+v", len(docs), docFilter.IDs)
		}
		docInfos := make(map[uint64]*docEntity.Doc)
		for _, doc := range docs {
			docInfos[doc.ID] = doc
		}
		for _, rspDoc := range rspDocs {
			if doc, ok := docInfos[rspDoc.DocId]; ok {
				rspDoc.DocBizId = doc.BusinessID
				rspDoc.DocName = doc.FileName
				rspDoc.Title = strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName))
				corpPrimaryId := contextx.Metadata(ctx).CorpID()
				rspDoc.KnowledgeBizId, err = s.cacheLogic.GetAppBizIdByPrimaryId(ctx, corpPrimaryId, doc.RobotID)
				if err != nil {
					// 降级处理
					logx.W(ctx, "get app biz id by app id failed, err:%v", err)
				}
			}
		}

		// 如果该应用配置了第三方权限系统，需要调用第三方权限系统校验该用户是否有检索结果中文档权限
		searchMultiRsp.GetRsp().Docs, err = s.searchLogic.CheckThirdPermission(ctx, bc.ReplaceApp,
			bc.GetLKEUserID(), searchMultiRsp.GetRsp().GetDocs())
		if err != nil {
			logx.E(ctx, "SearchKnowledge CheckThirdPermission failed, err: %+v", err)
			return nil, err
		}

	}
	logx.I(ctx, "searchMultiRsp:%s", searchMultiRsp)
	return searchMultiRsp, nil
}

// SearchMultiKnowledge 批量检索多知识库接口
// 这里只有 searchAnswer会用到
func (s *Service) SearchMultiKnowledge(ctx context.Context, bc *logicSearch.BotContext,
	searchMultiReq *retrieval.SearchMultiKnowledgeReq) (*pb.SearchKnowledgeRsp, error) {
	t1 := time.Now()
	logx.I(ctx, "searchMultiKnowledgeReq:%s", searchMultiReq)
	rsp := new(pb.SearchKnowledgeRsp)
	rsp.KnowledgeType = pb.SearchKnowledgeType_DOC_QA
	rsp.SceneType = bc.SceneType
	// 对于问答、文档和数据库的知识，统一走Preview;
	// 按照EnableScope标签进行处理
	multiRsp, err := s.rpc.RetrievalDirectIndex.SearchMultiKnowledgePreview(ctx, searchMultiReq)
	if err != nil {
		logx.E(ctx, "DirectSearchVector robotID: %d, err: %v", bc.App.PrimaryId, err)
		return nil, err
	}
	var docs []*pb.SearchPreviewRsp_Doc
	docs, err = s.getPreviewRspDocByApp(ctx, multiRsp.GetDocs(), bc.ReplaceApp.PrimaryId)
	if err != nil {
		logx.E(ctx, "getPreviewRspDocByApp robotID: %d, err: %v", bc.ReplaceApp.PrimaryId, err)
		return nil, err
	}
	previewRsp := &pb.SearchPreviewRsp{Docs: docs}
	post := searchRspPostProcess(ctx, bc.UsePlaceholder, previewRsp)
	rsp.Rsp = &pb.SearchKnowledgeRsp_SearchRsp{
		Docs: convertPreviewDocToKnowledgeDoc(post.GetDocs()),
	}
	rsp.TokenUsages = append(rsp.TokenUsages, multiRsp.GetEmbeddingUsage()...)
	rsp.TokenUsages = append(rsp.TokenUsages, multiRsp.GetRerankUsage()...)
	// 按模型名合并汇总 TokenUsages
	rsp.TokenUsages = mergeTokenUsagesByModel(rsp.TokenUsages)
	logx.I(ctx, "SearchMultiKnowledge rsp:%s, cost:%d", rsp, time.Since(t1).Milliseconds())
	return rsp, nil
}

// mergeTokenUsagesByModel 按模型名合并汇总 TokenUsages
func mergeTokenUsagesByModel(usages []*adpCommon.TokenUsage) []*adpCommon.TokenUsage {
	if len(usages) == 0 {
		return usages
	}

	// 按 model_name 归类，累加所有 token
	modelName2Usage := make(map[string]*adpCommon.TokenUsage)
	for _, usage := range usages {
		if usage == nil {
			continue
		}
		modelName := usage.GetModelName()
		if modelName == "" {
			continue
		}

		if existingUsage, exists := modelName2Usage[modelName]; exists {
			// 累加 token 数量
			existingUsage.TotalTokens += usage.GetTotalTokens()
			existingUsage.InputTokens += usage.GetInputTokens()
			existingUsage.OutputTokens += usage.GetOutputTokens()
		} else {
			// 创建新的 usage 记录
			modelName2Usage[modelName] = &adpCommon.TokenUsage{
				ModelName:    modelName,
				TotalTokens:  usage.GetTotalTokens(),
				InputTokens:  usage.GetInputTokens(),
				OutputTokens: usage.GetOutputTokens(),
			}
		}
	}

	// 将 map 转换为 slice
	result := make([]*adpCommon.TokenUsage, 0, len(modelName2Usage))
	for _, usage := range modelName2Usage {
		result = append(result, usage)
	}

	return result
}

func getSearchStrategy(appSearchStrategy *entity.SearchStrategy) *retrieval.SearchStrategy {
	var searchStrategy *retrieval.SearchStrategy
	if appSearchStrategy != nil {
		searchStrategy = &retrieval.SearchStrategy{
			StrategyType:     retrieval.SearchStrategyTypeEnum(appSearchStrategy.StrategyType),
			TableEnhancement: appSearchStrategy.TableEnhancement,
		}
	}
	return searchStrategy
}

// getWorkflowSearchStrategy 获取工作流应用检索策略
func getWorkflowSearchStrategy(knowledgeSearchStrategy *pb.SearchStrategy) *retrieval.SearchStrategy {
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
	filters []*retrieval.SearchFilter) []*retrieval.SearchFilter {
	logx.I(ctx, "filterSearchVectorScope|searchScope:%d", searchScope)
	var scopeFilter []*retrieval.SearchFilter
	if searchScope > 0 {
		scopeFilter = make([]*retrieval.SearchFilter, 0, 1)
		for _, f := range filters {
			if f.DocType == searchScope {
				scopeFilter = append(scopeFilter, f)
				break
			}
		}
	} else {
		scopeFilter = filters
	}
	logx.I(ctx, "filterSearchVectorScope|scopeFilter:%+v", scopeFilter)
	return scopeFilter
}

// SearchKnowledgeRelease 向量特征检索
func (s *Service) SearchKnowledgeRelease(ctx context.Context, req *pb.SearchKnowledgeReleaseReq) (*pb.SearchKnowledgeReleaseRsp, error) {
	logx.I(ctx, "SearchKnowledgeRelease req:%+v", req)
	rsp := &pb.SearchKnowledgeReleaseRsp{}
	defer func() {
		logx.I(ctx, "SearchKnowledgeRelease rsp:%+v", rsp)
	}()
	loginUin := contextx.Metadata(ctx).LoginUin()
	loginSubAccountUin := contextx.Metadata(ctx).LoginSubAccountUin()
	if loginUin != "" && loginSubAccountUin != "" { // 集成商走admin进行鉴权
		sessionRsp, err := s.rpc.PlatformLogin.CheckSession(ctx)
		if err != nil {
			logx.E(ctx, "CheckSession err: %v", err)
			return nil, err
		}
		logx.I(ctx, "staffId:%d corpId:%d sid:%d", sessionRsp.GetStaffId(), sessionRsp.GetCorpId(), sessionRsp.GetSid())
		md := contextx.Metadata(ctx)
		md.WithStaffID(sessionRsp.GetStaffId())
		md.WithCorpID(sessionRsp.GetCorpId())
		md.WithSID(sessionRsp.GetSid())
	} else { // 非集成商模式，权限校验，填充CorpID，验证uin是否有app的权限
		uin := contextx.Metadata(ctx).Uin()
		if uin == "" {
			return nil, errs.ErrUserNotFound
		}
		corpReq := pm.DescribeCorpReq{Uin: uin}
		corp, err := s.rpc.PlatformAdmin.DescribeCorp(ctx, &corpReq)
		// corp, err := s.dao.GetCorpBySidAndUin(ctx, uin)
		if err != nil || corp == nil {
			logx.E(ctx, "GetCorpBySidAndUin err: %v, uin: %s", err, uin)
			return nil, errs.ErrUserNotFound
		}
		if corp.GetCorpPrimaryId() == 0 {
			return nil, errs.ErrUserNotFound
		}
		md := contextx.Metadata(ctx)
		md.WithCorpID(corp.GetCorpPrimaryId())
		md.WithSID(corp.GetSid())
	}

	customVariables := make(map[string]string)
	for _, item := range req.CustomVariables {
		customVariables[item.Name] = item.Value
	}
	searchKnowledgeBatchReq := &pb.SearchKnowledgeBatchReq{
		SceneType:       entity.AppReleaseScenes,
		AppBizId:        req.AppBizId,
		Question:        req.Question,
		KnowledgeType:   pb.SearchKnowledgeType_DOC_QA,
		CustomVariables: customVariables,
		VisitorBizId:    req.VisitorBizId,
		BillingTags:     req.GetBillingTags(),
	}

	app, searchKnowledgeBatchRsp, err := s.searchKnowledgeBatch(ctx, searchKnowledgeBatchReq)
	if err != nil {
		logx.E(ctx, "SearchKnowledgeBatch err: %v", err)
		return nil, err
	}
	searchKnowledgeRsp := searchKnowledgeBatchRsp.GetRsp()
	if searchKnowledgeRsp == nil || searchKnowledgeRsp.GetDocs() == nil || len(searchKnowledgeRsp.GetDocs()) == 0 {
		return rsp, nil
	}

	// Search接口返回的是文档自增id，对外需要转成业务id
	docIDs := make([]uint64, 0)
	for _, res := range searchKnowledgeRsp.GetDocs() {
		if res.GetDocType() == entity.DocTypeSegment {
			docIDs = append(docIDs, res.GetDocId())
		}
	}
	docIDs = slicex.Unique(docIDs)
	docMap := make(map[uint64]*docEntity.Doc)
	if len(docIDs) != 0 {
		docs, err := s.docLogic.GetDocByIDs(ctx, docIDs, app.PrimaryId)
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

func convertSearKnowledgeRspToSearchKnowledgeReleaseRsp(ctx context.Context, searchKnowledgeRsp *pb.SearchKnowledgeRsp,
	docMap map[uint64]*docEntity.Doc) *pb.SearchKnowledgeReleaseRsp {
	searchKnowledgeReleaseRsp := &pb.SearchKnowledgeReleaseRsp{}
	knowledgeList := make([]*pb.SearchKnowledgeReleaseRsp_KnowledgeItem, 0)
	rsp := searchKnowledgeRsp.GetRsp()
	if rsp == nil || rsp.GetDocs() == nil || len(rsp.GetDocs()) == 0 {
		return searchKnowledgeReleaseRsp
	}

	for _, res := range rsp.GetDocs() {
		knowledgeType := ""
		switch res.GetDocType() {
		case entity.DocTypeSegment:
			knowledgeType = entity.DataTypeDoc
		case entity.DocTypeQA:
			knowledgeType = entity.DataTypeQA
		default:
			logx.E(ctx, "docType:%d is invalid", res.GetDocType())
			continue
		}
		knowledgeItem := &pb.SearchKnowledgeReleaseRsp_KnowledgeItem{
			KnowledgeType: knowledgeType,
			KnowledgeId:   fmt.Sprintf("%d", res.GetRelatedBizId()),
			Question:      res.GetQuestion(),
			Content: func() string {
				if res.GetDocType() == entity.DocTypeQA {
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
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppReleaseScenes)
	clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	replaceApp := app
	if newAppID, ok := config.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.BizId]; ok {
		replaceApp, err = s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, newAppID, entity.AppReleaseScenes)
		clues.AddTrackDataWithError(ctx, "getAppByAppBizID", app, err)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		logx.D(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 app:%+v replaceApp:%+v",
			app, replaceApp)
	}
	if app.QaConfig == nil || replaceApp.QaConfig == nil {
		return nil, errs.ErrAppTypeSupportFilters
	}

	key := gox.IfElse(req.GetFilterKey() == "", entity.AppSearchReleaseFilterKey, req.GetFilterKey())
	filter, ok := app.QaConfig.Filters[key]
	if !ok {
		return nil, fmt.Errorf("robot %s filter not found scenes %d", key, entity.AppReleaseScenes)
	}
	clues.AddTrackData(ctx, "app.GetFilter", map[string]any{
		"scenes": entity.AppReleaseScenes, "key": key, "filter": filter,
	})
	if replaceApp.QaConfig != nil {
		req.Labels = handleCustomVariablesLabels(req.Labels,
			replaceApp.QaConfig.SearchRange, customVariables)
	}
	// handleReqLabels 之后 VectorLabel中的Name就是AttrKey
	newLabels := s.similarLabels2StandardLabels(ctx, replaceApp.PrimaryId, req.GetLabels(), labelEntity.AttributeLabelsProd)
	filters := make([]*retrieval.SearchFilter, 0, len(filter.Filter))
	for _, f := range filter.Filter {
		if f.DocType == entity.DocTypeSearchEngine {
			continue
		}
		if f.DocType == entity.DocTypeTaskFlow {
			continue
		}
		if key == entity.AppSearchReleaseFilterKey && !f.IsEnabled {
			continue
		}
		filters = append(filters, &retrieval.SearchFilter{
			DocType:    f.DocType,
			Confidence: f.Confidence,
			TopN:       f.TopN,
		})
	}
	// 敏捷发布：新增检索范围，支持指定范围检索
	filters = filterSearchScope(newCtx, req.GetSearchScope(), filters)
	rerank, err := getRerankModel(app)
	clues.AddTrackData(newCtx, "app.GetRerankModel", map[string]any{
		"scenes": entity.AppReleaseScenes, "rerank": rerank, "err": err != nil,
	})
	if err != nil {
		logx.E(newCtx, "get rerank model err:%v", err)
		return rsp, err
	}
	r, err := s.search(newCtx, &retrieval.SearchMultiKnowledgeReq{
		RobotId:        replaceApp.PrimaryId,
		Question:       req.GetQuestion(),
		TopN:           filter.TopN,
		Rerank:         &retrieval.Rerank{Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable},
		ImageUrls:      req.GetImageUrls(),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.QaConfig.SearchStrategy),
		ModelName:      req.GetModelName(),
		SearchData: []*retrieval.SearchData{{
			KnowledgeId:      replaceApp.PrimaryId,
			KnowledgeBizId:   replaceApp.BizId,
			QaVersion:        replaceApp.QaVersion,
			Filters:          filters,
			FilterKey:        key,
			Labels:           convertSearchVectorLabel(newLabels),
			LabelExpression:  fillLabelExpression(newLabels, getAppLabelCondition(replaceApp.BizId, replaceApp.QaConfig.SearchRange.Condition)),
			EmbeddingVersion: replaceApp.Embedding.Version,
		}},
	})
	if err != nil {
		return nil, err
	}
	t1 := time.Now()
	docs, err := s.searchDocs(newCtx, r, replaceApp.PrimaryId)
	clues.AddTrackData(ctx, "searchDocs", map[string]any{
		"req": r, "docs": docs, "err": err != nil, "ELAPSED": time.Since(t1).String(),
	})
	if err != nil {
		return nil, err
	}
	rsp.Docs = docs
	post := searchRspPostProcess(newCtx, req.GetUsePlaceholder(), rsp)
	clues.AddTrackData(ctx, "searchRspPostProcess", map[string]any{
		"usePlaceholder": req.GetUsePlaceholder(), "SearchPreviewRsp": rsp, "post": post,
	})
	return post, nil
}

// filterSearchScope 过滤指定范围检索
func filterSearchScope(ctx context.Context, searchScope uint32,
	filters []*retrieval.SearchFilter) []*retrieval.SearchFilter {
	logx.I(ctx, "filterSearchScope|searchScope:%d", searchScope)
	var scopeFilter []*retrieval.SearchFilter
	if searchScope > 0 {
		scopeFilter = make([]*retrieval.SearchFilter, 0, 1)
		for _, f := range filters {
			if f.DocType == searchScope {
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

func (s *Service) searchDocs(ctx context.Context, r *retrieval.SearchRsp, robotID uint64) ([]*pb.SearchRsp_Doc, error) {
	linkContents, err := s.docLogic.GetLinkContentsFromSearchResponse(
		ctx, robotID, r.GetDocs(),
		func(doc *retrieval.SearchRsp_Doc, qa *qa.DocQA) any {
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
		func(doc *retrieval.SearchRsp_Doc, segment *segment.DocSegmentExtend) any {
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
	docs := linker.Link(ctx, linkContents, func(t *pb.SearchRsp_Doc, v linker.Content) *pb.SearchRsp_Doc {
		t.OrgData = v.Value
		return t
	})
	return docs, nil
}

func patchConfidence(refers []entity.Refer, answer string) ([]entity.Refer, []rouge.Score) {
	scores := make([]rouge.Score, len(refers))
	answerWords := strings.Join(rouge.Seg.Cut(answer, true), " ")
	for i, refer := range refers {
		reference := refer.OrgData
		if refer.DocType == entity.DocTypeQA {
			reference = refer.Answer
		}
		referenceWords := strings.Join(rouge.Seg.Cut(reference, true), " ")
		score := rouge.GetRougeScore(answerWords, referenceWords)

		refers[i].Confidence = float32(math.Max(score.P, score.R))
		refers[i].RougeScore, _ = jsonx.MarshalToString(score)
		scores[i] = score
	}
	return refers, scores
}

// MatchRefer 匹配来源
func (s *Service) MatchRefer(ctx context.Context, req *pb.MatchReferReq) (*pb.MatchReferRsp, error) {
	logx.I(ctx, "MatchRefer|req:%+v", req)
	appid := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.svc.DescribeAppAndCheckCorp(ctx, appid)
	if err != nil {
		return &pb.MatchReferRsp{}, errs.ErrRobotNotFound
	}
	_, refers, docIDs, qaIDs, err := s.getRefersFromReq(ctx, req)
	if err != nil {
		logx.E(ctx, "MatchRefer|getRefersFromReq failed, err:%+v", err)
		return nil, err
	}
	if len(refers) == 0 {
		logx.W(ctx, "MatchRefer|refers is empty|ignore")
		return &pb.MatchReferRsp{}, nil
	}
	logx.I(ctx, "MatchRefer|IgnoreConfidenceScore:%+v", req.GetIgnoreConfidenceScore())
	if !req.GetIgnoreConfidenceScore() {
		// 2.3迭代特殊逻辑：
		// 	- 目前使用工程计算相似度的方式，准确率不高，增加一个开关决定是否使用工程相似度计算
		// 	- 默认false走相似度计算过滤，为true则默认全部匹配
		scores := make([]rouge.Score, len(refers))
		refers, scores = patchConfidence(refers, req.GetAnswer())
		refers = filterRefer(refers, scores, config.App().RobotDefault.Filters[entity.AppMatchReferFilterKey])
	}
	logx.I(ctx, "MatchRefer|after filterRefer|refers:%+v", refers)
	segmentDocs, err := s.docLogic.GetDocByIDs(ctx, slicex.Unique(docIDs), app.PrimaryId)
	if err != nil {
		return nil, err
	}
	qas, qaDocs, err := s.qaLogic.GetQAAndRelateDocs(ctx, slicex.Unique(qaIDs), app.PrimaryId)
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
	contextx.Metadata(ctx).WithCorpID(app.CorpPrimaryId)
	knowInfoByID, hasShare, err := s.searchLogic.GetReferKnowledgeName(ctx, app.BizId, knowIDs)
	if err != nil { // 柔性放过,降级为不返回名称
		logx.W(ctx, "MatchRefer GetReferKnowledgeName err:%v,appBizID:%v,knowIDs:%v", err, app.BizId, knowIDs)
	}
	newRefers := make([]entity.Refer, 0, len(refers))
	data := make([]*pb.MatchReferRsp_Refer, 0, len(refers))
	for _, refer := range refers {
		appID, docID, docBizID, qaBizID, name, docName, url := s.getReferNameURL(ctx, refer, segmentDocs, qaDocs, qas)
		if req.GetIsRelease() && refer.DocType == entity.ReferTypeSegment && url == "" {
			logx.W(ctx, "MatchRefer Release doc url ignore")
			continue
		}
		if refer.DocType == entity.ReferTypeQA {
			refer.DocID = docID
		}
		knowledgeBizID, knowledgeName := app.BizId, ""
		if hasShare {
			if appID == app.PrimaryId {
				knowledgeName = i18n.Translate(ctx, i18nkey.KeyDefaultKnowledgeBase)
			} else if knowInfo, ok := knowInfoByID[appID]; ok {
				knowledgeBizID, knowledgeName = knowInfo.BusinessID, knowInfo.Name
			} else {
				logx.W(ctx, "MatchRefer app:%v not found,knowInfoByID:%v", appID, knowInfoByID)
			}
		}
		urlType := docEntity.MatchReferUrlTypeDefault
		if req.GetIsRelease() && refer.DocType == entity.ReferTypeSegment {
			doc, ok := segmentDocs[refer.DocID]
			if !ok || doc.HasDeleted() || doc.IsExpire() {
				logx.W(ctx, "MatchRefer Release doc HasDeleted:%v,IsExpire:%v",
					doc.HasDeleted(), doc.IsExpire())
			} else {
				if doc.IsReferOpen() && doc.Source == docEntity.SourceFromWeb &&
					doc.ReferURLType == docEntity.ReferURLTypeWebDocURL {
					url = doc.OriginalURL
					urlType = docEntity.MatchReferUrlTypeOriginalURL
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

	if err := s.docLogic.GetDao().CreateRefer(ctx, newRefers); err != nil {
		return nil, errs.ErrSystem
	}
	logx.D(ctx, "MatchRefer|rsp|Refers:%+v", data)
	return &pb.MatchReferRsp{Refers: data}, nil
}

// getRefersFromReq 构造参考来源
func (s *Service) getRefersFromReq(ctx context.Context, req *pb.MatchReferReq) (
	*config.RobotFilter, []entity.Refer, []uint64, []uint64, error) {
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppReleaseScenes)
	if err != nil {
		return nil, nil, nil, nil, errs.ErrRobotNotFound
	}
	filter, ok := app.QaConfig.Filters[entity.AppMatchReferFilterKey]
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("filter not found")
	}
	logx.D(ctx, "getRefersFromReq|filter:%+v", filter)
	docTypeMap := make(map[uint32]struct{})
	for _, f := range filter.Filter {
		docTypeMap[f.DocType] = struct{}{}
	}
	refers := make([]entity.Refer, 0, len(req.GetDocs()))
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
			(doc.DocType == entity.DocTypeSegment || doc.DocType == entity.DocTypeDB) {
			continue
		}

		if doc.DocType == entity.DocTypeSegment {
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
		if doc.DocType == entity.DocTypeQA {
			if _, exists := qaIdMap[doc.GetRelatedId()]; exists {
				continue // 问答去重检查
			}
			qaIds = append(qaIds, doc.GetRelatedId())
			qaIdMap[doc.GetRelatedId()] = struct{}{}
		}

		refers = append(refers, entity.NewRefer(doc, req, app.PrimaryId, idgen.GetId()))
	}
	// 文档切片页码信息
	segmentPageInfoMap := make(map[uint64]*segment.DocSegmentPageInfo)
	if len(segmentIds) > 0 {
		segmentPageInfoMap, err = s.segLogic.GetSegmentPageInfosBySegIDs(ctx, app.PrimaryId, segmentIds)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	for i := range refers {
		if refers[i].DocType != entity.DocTypeSegment { // 只有文档切片才有页码信息
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
	logx.D(ctx, "getRefersFromReq|filter:%+v|refers:%+v|docIDs:%+v|qaIDs:%+v",
		filter, refers, docIds, qaIds)
	return &filter, refers, docIds, qaIds, nil
}

// getReferNameURL 获取引用名称和链接
func (s *Service) getReferNameURL(ctx context.Context, refer entity.Refer, segmentDocs, qaDocs map[uint64]*docEntity.Doc,
	qas map[uint64]*qa.DocQA) (
	appID, docID, docBizID, qaBizID uint64, name, docName, url string) {
	if refer.DocType == entity.ReferTypeQA {
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
		if qa.EnableScope == entity.EnableScopeInvalid || doc.EnableScope == entity.EnableScopeInvalid ||
			(qa.EnableScope != entity.EnableScopeAll && doc.EnableScope != entity.EnableScopeAll && qa.EnableScope != doc.EnableScope) {
			logx.D(ctx, "MatchRefer.getReferNameURL | qa.EnableScope: %d, doc.EnableScope: %d",
				qa.EnableScope, doc.EnableScope)
			return appID, 0, 0, qaBizID, refer.Question, "", ""
		}
		docID = doc.ID
		docBizID = doc.BusinessID
		docName = doc.GetFileNameByStatus()
		if !doc.IsReferOpen() {
			return appID, docID, docBizID, qaBizID, refer.Question, docName, ""
		}
		return appID, docID, docBizID, qaBizID, refer.Question, docName,
			gox.IfElse(doc.UseWebURL(), doc.WebURL, s.getReferURLByPreviewType(ctx, doc))
	}

	if refer.DocType == entity.ReferTypeSegment {
		doc, ok := segmentDocs[refer.DocID]
		if !ok || doc.HasDeleted() || doc.IsExpire() {
			return 0, 0, 0, 0, "", "", ""
		}
		docName = doc.GetFileNameByStatus()
		if !doc.IsReferOpen() {
			return doc.RobotID, doc.ID, doc.BusinessID, 0, docName, docName, ""
		}
		return doc.RobotID, doc.ID, doc.BusinessID, 0, docName, docName,
			gox.IfElse(doc.UseWebURL(), doc.WebURL, s.getReferURLByPreviewType(ctx, doc))
	}

	logx.W(ctx, "Unsupported refer type, refer: %+v", refer)
	return 0, 0, 0, 0, "", "", ""
}

// getReferURLByPreviewType 生成参考来源文件预览链接
func (s *Service) getReferURLByPreviewType(ctx context.Context, doc *docEntity.Doc) string {
	if config.App().DocPreviewType == entity.AppUseDownloadURL {
		// 目前只有私有化的minio使用下载链接预览，不需要判断存储类型
		logx.D(ctx, "文档ID:%d 文档类型:%s 采用下载地址", doc.ID, doc.FileType)
		return fmt.Sprintf("%s/%s%s", s.s3.GetDomain(ctx), doc.Bucket, doc.CosURL)
	}
	docPreviewConf := config.App().DocPreview
	docPreview, ok := docPreviewConf[doc.FileType]
	if !ok || len(docPreview.URL) == 0 {
		logx.D(ctx, "文档ID:%d 文档类型:%s 未配置文档预览地址", doc.ID, doc.FileType)
		return ""
	}
	return fmt.Sprintf("%s?cat=phone&id=%d", docPreview.URL, doc.BusinessID)
}

// filterRefer 过滤参考来源
func filterRefer(refers []entity.Refer, scores []rouge.Score, matchReferFilter config.RobotFilter) []entity.Refer {
	filterMap := make(map[uint32]config.RobotFilterDetail)
	for _, filter := range matchReferFilter.Filter {
		filterMap[filter.DocType] = filter
	}

	group := make(map[uint32][]entity.Refer)
	for i, r := range refers {
		filter, ok := filterMap[r.DocType]
		if !ok || filter.TopN == 0 {
			continue
		}
		thres := filter.RougeScore
		if scores[i].F <= thres.F && scores[i].P <= thres.P && scores[i].R <= thres.R {
			continue
		}
		group[r.DocType] = append(group[r.DocType], r)
	}

	for typ := range group {
		sort.SliceStable(group[typ], func(i, j int) bool {
			return group[typ][i].Confidence > group[typ][j].Confidence
		})
	}

	var refs []entity.Refer
filter:
	for _, filter := range matchReferFilter.Filter {
		for i, refer := range group[filter.DocType] {
			if len(refs) >= int(matchReferFilter.TopN) {
				break filter
			}
			if i >= int(filter.TopN) {
				break
			}
			refs = append(refs, refer)
		}
	}
	return refs
}

// SearchPreviewRejectedQuestion 拒答问题测评库查询
// NOTE(ericjwang): RPC 不用了，但是还在被内部调用
func (s *Service) SearchPreviewRejectedQuestion(ctx context.Context, req *pb.SearchPreviewRejectedQuestionReq) (*pb.SearchPreviewRejectedQuestionRsp, error) {
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	ctx = clues.NewTrackContext(ctx)
	rsp := new(pb.SearchPreviewRejectedQuestionRsp)
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppTestScenes)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	robotFilter, ok := app.QaConfig.Filters[entity.AppSearchRejectedQuestionPreview]
	if !ok {
		return rsp, fmt.Errorf("robot not filer key:%s", entity.AppSearchRejectedQuestionPreview)
	}
	filters := make([]*retrieval.SearchFilter, 0, 1)
	for _, filter := range robotFilter.Filter {
		filters = append(filters, &retrieval.SearchFilter{
			IndexId:    s.getType(filter.DocType),
			Confidence: filter.Confidence,
			TopN:       filter.TopN,
			DocType:    filter.DocType,
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		logx.E(ctx, "get rerank model err:%v", err)
		return rsp, err
	}
	searchReq := &retrieval.SearchMultiKnowledgeReq{
		RobotId:  app.PrimaryId,
		BotBizId: app.BizId,
		Question: req.GetQuestion(),
		TopN:     robotFilter.TopN,
		Rerank: &retrieval.Rerank{
			Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable,
		},
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.QaConfig.SearchStrategy),
		ModelName:      req.GetModelName(),
		SearchData: []*retrieval.SearchData{{
			KnowledgeId:      app.PrimaryId,
			KnowledgeBizId:   app.BizId,
			FilterKey:        entity.AppSearchRejectedQuestionPreview,
			Filters:          filters,
			EmbeddingVersion: app.Embedding.Version,
		}},
	}
	searchVectorRsp, err := s.rpc.RetrievalDirectIndex.SearchMultiKnowledgePreview(newCtx, searchReq)
	if err != nil {
		logx.E(newCtx, "SearchPreviewRejectedQuestion req: %+v rsp:%+v searchVectorRsp:%+v, err:%+v",
			req, rsp, searchVectorRsp, err)
		return rsp, err
	}
	rejectedQuestions := make([]*pb.SearchPreviewRejectedQuestionRsp_RejectedQuestions, 0)
	for _, doc := range searchVectorRsp.GetDocs() {
		if doc.GetIndexId() == entity.RejectedQuestionReviewVersionID ||
			doc.GetIndexId() == entity.RejectedQuestionSimilarVersionID {
			rejectedQuestion, err := s.qaLogic.GetRejectedQuestionByID(ctx, app.CorpPrimaryId, app.PrimaryId, doc.GetId())
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
	logx.D(ctx, "req: %+v rsp:%+v", req, rsp)
	return rsp, nil
}

// SearchReleaseRejectedQuestion 拒答问题线上库查询
// NOTE(ericjwang): RPC 不用了，但是还在被内部调用
func (s *Service) SearchReleaseRejectedQuestion(ctx context.Context, req *pb.SearchReleaseRejectedQuestionReq) (*pb.SearchReleaseRejectedQuestionRsp, error) {
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	ctx = clues.NewTrackContext(ctx)
	rsp := new(pb.SearchReleaseRejectedQuestionRsp)
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppReleaseScenes)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	robotFilter, ok := app.QaConfig.Filters[entity.AppSearchRejectedQuestionRelease]
	if !ok {
		return rsp, fmt.Errorf("filters not found")
	}
	filters := make([]*retrieval.SearchFilter, 0, len(robotFilter.Filter))
	for _, f := range robotFilter.Filter {
		filters = append(filters, &retrieval.SearchFilter{
			DocType:    f.DocType,
			Confidence: f.Confidence,
			TopN:       f.TopN,
		})
	}
	rerank, err := getRerankModel(app)
	if err != nil {
		logx.E(ctx, "get rerank model err:%v", err)
		return rsp, err
	}
	searchReq := &retrieval.SearchMultiKnowledgeReq{
		RobotId:        app.PrimaryId,
		Question:       req.GetQuestion(),
		TopN:           robotFilter.TopN,
		Rerank:         &retrieval.Rerank{Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable},
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.QaConfig.SearchStrategy),
		ModelName:      req.GetModelName(),
		SearchData: []*retrieval.SearchData{{
			KnowledgeId:      app.PrimaryId,
			KnowledgeBizId:   app.BizId,
			QaVersion:        app.QaVersion,
			Filters:          filters,
			FilterKey:        entity.AppSearchRejectedQuestionRelease,
			EmbeddingVersion: app.Embedding.Version,
		}},
	}
	searchRsp, err := s.search(newCtx, searchReq)
	if err != nil {
		logx.E(newCtx, "SearchReleaseRejectedQuestion req: %+v rsp:%+v searchRsp:%+v, err:%+v",
			req, rsp, searchRsp, err)
		return rsp, err
	}
	rejectedQuestions := make([]*pb.SearchReleaseRejectedQuestionRsp_RejectedQuestions, 0)
	for _, doc := range searchRsp.GetDocs() {
		if doc.GetDocType() == uint32(entity.DocTypeRejectedQuestion) {
			rejectedQuestions = append(rejectedQuestions, &pb.SearchReleaseRejectedQuestionRsp_RejectedQuestions{
				Id:         doc.GetRelatedId(),
				Question:   doc.GetQuestion(),
				Confidence: doc.GetConfidence(),
			})
		}
	}
	rsp.List = rejectedQuestions
	logx.D(ctx, "req: %+v rsp:%+v", req, rsp)
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

// handleCustomVariablesLabels 处理请求CustomVariables中的labels
func handleCustomVariablesLabels(labels []*pb.VectorLabel, searchRange *entity.SearchRange,
	customVariables map[string]string) []*pb.VectorLabel {
	if len(searchRange.Condition) != 0 && len(searchRange.ApiVarAttrInfos) > 0 { // 知识检索范围不为空 取customVariables中的值查找映射关系中的标签
		labels = make([]*pb.VectorLabel, 0)
		for k, v := range customVariables {
			label := &pb.VectorLabel{
				Name:   k,
				Values: strings.Split(v, entity.CustomVariableSplitSep),
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
func handleReqLabels(searchRange *entity.SearchRange, labels []*pb.VectorLabel) []*pb.VectorLabel {
	var newLabels []*pb.VectorLabel
	// 通过检索范围的自定义参数《=》label 映射关系转换
	if len(searchRange.Condition) != 0 && len(searchRange.ApiVarAttrInfos) > 0 {
		var ApiVarAttrInfosMap = make(map[string]uint64)
		for _, attrInfo := range searchRange.ApiVarAttrInfos {
			ApiVarAttrInfosMap[attrInfo.ApiVarID] = attrInfo.AttrBizID
		}
		ApiVarNameIDMap := make(map[string]string)
		for apiVarID, ApiVarName := range searchRange.APIVarMap {
			ApiVarNameIDMap[ApiVarName] = apiVarID
		}
		for _, label := range labels {
			// apiVarName 转换 apiVarID
			if apiVarID, ok := ApiVarNameIDMap[label.GetName()]; ok {
				// 通过 apiVarID 获取 attrBizID
				if AttrBizID, ok := ApiVarAttrInfosMap[apiVarID]; ok {
					// 通过 attrBizID 获取 attrKey
					if attrKey, ok := searchRange.LabelAttrMap[AttrBizID]; ok {
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
	logx.I(ctx, "similarLabels2StandardLabels, req, robotID:%d, labels:%+v", robotID, labels)
	if len(labels) == 0 {
		return labels
	}
	var mapAttrKey2Labels = make(map[string][]labelEntity.AttrLabelAndSimilarLabels, 0)
	// 1. 把所有attrKey对应的labels都取出来
	for _, label := range labels {
		if label == nil {
			continue
		}
		if _, ok := mapAttrKey2Labels[label.GetName()]; ok {
			continue
		}
		labelRedisValue, err := s.labelDao.GetAttributeLabelsRedis(ctx, robotID, label.GetName(), envType)
		if err != nil { // 忽略错误
			logx.I(ctx, "similarLabels2StandardLabels, GetAttributeLabelsRedis failed, robotID:%d, "+
				"attrKey:%s, err:%v", robotID, label.GetName(), err)
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
			mapAttrKey2Labels[label.GetName()] = labelAndSimilarList
		}
	}

	// 2. 相似标签转主标签
	for i, label := range labels {
		if label == nil {
			continue
		}
		var labelAndSimilarList []labelEntity.AttrLabelAndSimilarLabels
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
	logx.I(ctx, "similarLabels2StandardLabels, rsp, robotID:%d, labels:%+v", robotID, labels)
	return labels
}

func (s *Service) searchDBAnswer(ctx context.Context,
	bc *logicSearch.BotContext, dbTableID uint64) (*pb.SearchKnowledgeRsp, error) {
	logx.I(ctx, "searchDBAnswer req:%s", jsonx.MustMarshalToString(bc))
	// dbTable反查询DBSourceID，不能用AppBizID过滤，会导致共享知识库的table查不到
	tableFilter := dbEntity.TableFilter{
		CorpBizID:    bc.App.CorpBizId,
		DBTableBizID: dbTableID,
	}
	table, err := s.dbLogic.DescribeTable(ctx, &tableFilter)
	if err != nil {
		logx.E(ctx, "get db table by biz id failed, %v", err)
		return nil, err
	}

	// 根据SceneType判断表在当前域是否有效
	var envType pb.EnvType
	if bc.SceneType == pb.SceneType_TEST {
		envType = pb.EnvType_Test
	} else if bc.SceneType == pb.SceneType_PROD {
		envType = pb.EnvType_Prod
	} else {
		// 默认使用测试环境
		envType = pb.EnvType_Test
	}

	// 验证表在当前环境是否有效
	if !s.dbLogic.IsTableValidInEnv(envType, table) {
		logx.W(ctx, "table %d is not valid in current environment (SceneType: %v, EnableScope: %v)",
			table.DBTableBizID, bc.SceneType, table.EnableScope)
		return &pb.SearchKnowledgeRsp{}, nil
	}

	resultDoc := &pb.SearchKnowledgeRsp_SearchRsp_Doc{
		DocType:   entity.DocTypeQA,
		RelatedId: table.DBTableBizID,
		Question:  bc.Question,
	}

	// 运行SQL获取结果
	_, result, _, errMsg, err := s.dbLogic.RunSql(ctx, table.DBSourceBizID, bc.Question, nil)
	if err != nil {
		if errMsg == "" {
			errMsg = err.Error()
		}
		logx.E(ctx, "searchDBAnswer RunSql fail, errMsg=%+v, err:%v", errMsg, err)
		resultDoc.Answer = errMsg
	} else {
		answer, err := jsoniter.MarshalToString(result)
		if err != nil {
			logx.E(ctx, "searchDBAnswer Marshal RunSql result fail, err:%v", err)
			resultDoc.Answer = err.Error()
		} else {
			resultDoc.Answer = answer
		}
	}

	// 拼接rsp结果返回
	resp := &pb.SearchKnowledgeRsp{
		KnowledgeType: pb.SearchKnowledgeType_DOC_QA,
		SceneType:     bc.SceneType,
		Rsp: &pb.SearchKnowledgeRsp_SearchRsp{
			Docs: []*pb.SearchKnowledgeRsp_SearchRsp_Doc{resultDoc},
		},
	}
	return resp, nil
}

func (s *Service) reportTokenUsage(ctx context.Context, usages []*adpCommon.TokenUsage, bc *logicSearch.BotContext) error {
	var dosage *finance.TokenDosage
	// 处理usages,先按model_name归类，累加所有token
	modelName2Usage := make(map[string]*adpCommon.TokenUsage)
	for _, usage := range usages {
		if usage == nil {
			continue
		}
		// 按model_name归类
		modelName := usage.GetModelName()
		if modelName == "" {
			continue
		}
		if existingUsage, exists := modelName2Usage[modelName]; exists {
			// 累加token数量
			existingUsage.TotalTokens += usage.GetTotalTokens()
			existingUsage.InputTokens += usage.GetInputTokens()
			existingUsage.OutputTokens += usage.GetOutputTokens()
		} else {
			// 创建新的usage记录
			modelName2Usage[modelName] = &adpCommon.TokenUsage{
				ModelName:    modelName,
				TotalTokens:  usage.GetTotalTokens(),
				InputTokens:  usage.GetInputTokens(),
				OutputTokens: usage.GetOutputTokens(),
			}
		}
	}
	// 将归类后的usage转换为dosage格式
	if len(modelName2Usage) > 0 {
		// 为每个model_name创建TokenDosage
		for modelName, usage := range modelName2Usage {
			dosage = &finance.TokenDosage{
				AppID:           bc.ReplaceApp.BizId,
				AppType:         bc.ReplaceApp.AppType,
				ModelName:       modelName,
				AliasName:       "", // 需要从模型信息获取
				RecordID:        fmt.Sprintf("%d", idgen.GetId()),
				StartTime:       bc.StartTime,
				EndTime:         time.Now(),
				InputDosages:    []int{int(usage.GetInputTokens())},
				OutputDosages:   []int{int(usage.GetOutputTokens())},
				BillingTags:     bc.BillingInfo.BillingTags,
				KnowledgeBaseID: bc.ReplaceApp.BizId,
				SourceType:      "chat",
				SpaceID:         bc.ReplaceApp.SpaceId,
			}
			// 为每个model_name单独上报token用量
			stat := &rpc.StatisticInfo{}
			stat.InputTokens = usage.GetInputTokens()
			stat.OutputTokens = usage.GetOutputTokens()
			stat.TotalTokens = usage.GetTotalTokens()
			err := s.financeLogic.ReportTokenDosage(ctx, stat, dosage, bc.ReplaceApp.CorpPrimaryId, bc.BillingInfo.SubBizType, bc.ReplaceApp)
			if err != nil {
				logx.W(ctx, "ReportTokenDosage failed, dosage:%v, err:%+v", dosage, err)
			}
		}
	}
	return nil
}
