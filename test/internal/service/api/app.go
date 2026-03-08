package api

import (
	"context"
	"errors"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/linker"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"github.com/spf13/cast"
)

func (s *Service) getPreviewRspDocByApp(ctx context.Context, docs []*retrieval.SearchVectorRsp_Doc, robotID uint64) ([]*pb.SearchPreviewRsp_Doc, error) {
	linkContents, err := s.docLogic.GetLinkContentsFromSearchVectorResponse(
		ctx, robotID, docs,
		func(doc *retrieval.SearchVectorRsp_Doc, qa *qaEntity.DocQA) any {
			return &pb.SearchPreviewRsp_Doc{
				DocId:                qa.DocID,
				DocType:              doc.GetDocType(),
				RelatedId:            doc.GetId(),
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
		func(doc *retrieval.SearchVectorRsp_Doc, segment *segEntity.DocSegmentExtend) any {
			return &pb.SearchPreviewRsp_Doc{
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
			return &pb.SearchPreviewRsp_Doc{
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
		return nil, err
	}
	return linker.Link(ctx, linkContents, func(t *pb.SearchPreviewRsp_Doc, v linker.Content) *pb.SearchPreviewRsp_Doc {
		t.OrgData = v.Value
		return t
	}), nil
}

// GetRobotRetrievalConfig 获取机器人检索配置
func (s *Service) GetRobotRetrievalConfig(ctx context.Context, req *pb.GetRobotRetrievalConfigReq) (*pb.GetRobotRetrievalConfigRsp, error) {
	rsp := new(pb.GetRobotRetrievalConfigRsp)
	retrievalConfig, err := s.kbLogic.DescribeRetrievalConfigCache(ctx, req.GetRobotId())
	if err != nil {
		return rsp, err
	}
	rsp.Settings = &pb.RobotRetrievalConfig{
		EnableEsRecall:     retrievalConfig.EnableEsRecall,
		EnableVectorRecall: retrievalConfig.EnableVectorRecall,
		EnableRrf:          retrievalConfig.EnableRrf,
		EnableText2Sql:     retrievalConfig.EnableText2Sql,
		RerankThreshold:    retrievalConfig.ReRankThreshold,
		RrfVecWeight:       retrievalConfig.RRFVecWeight,
		RrfEsWeight:        retrievalConfig.RRFEsWeight,
		RrfRerankWeight:    retrievalConfig.RRFReRankWeight,
		DocVecRecallNum:    retrievalConfig.DocVecRecallNum,
		QaVecRecallNum:     retrievalConfig.QaVecRecallNum,
		EsRecallNum:        retrievalConfig.EsRecallNum,
		EsRerankMinNum:     retrievalConfig.EsReRankMinNum,
		RrfReciprocalConst: int32(retrievalConfig.RRFReciprocalConst),
		EsTopN:             retrievalConfig.EsTopN,
		Text2SqlModel:      retrievalConfig.Text2sqlModel,
		Text2SqlPrompt:     retrievalConfig.Text2sqlPrompt,
	}
	return rsp, nil
}

// SaveRobotRetrievalConfig 保存机器人检索配置
func (s *Service) SaveRobotRetrievalConfig(ctx context.Context, req *pb.SaveRobotRetrievalConfigReq) (*pb.SaveRobotRetrievalConfigRsp, error) {
	rsp := new(pb.SaveRobotRetrievalConfigRsp)
	retrievalConfig := convertToRetrievalConfig(req.GetRobotId(), req.GetSettings())
	if !retrievalConfig.IsCheckRetrievalConfig() {
		return rsp, errs.ErrRetrievalConfig
	}
	logx.I(ctx, "SaveRobotRetrievalConfig:%v", retrievalConfig)
	err := s.kbLogic.ModifyRetrievalConfig(ctx, &retrievalConfig)
	// err := s.dao.SaveRetrievalConfig(ctx, req.GetRobotId(), retrievalConfig, req.GetSettings().Operator)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

func convertToRetrievalConfig(robotID uint64, settings *pb.RobotRetrievalConfig) entity.RetrievalConfig {
	if settings == nil {
		return entity.RetrievalConfig{}
	}
	now := time.Now()
	return entity.RetrievalConfig{
		RobotID:            robotID,
		EnableEsRecall:     settings.GetEnableEsRecall(),
		EnableVectorRecall: settings.GetEnableVectorRecall(),
		EnableRrf:          settings.GetEnableRrf(),
		EnableText2Sql:     settings.GetEnableText2Sql(),
		ReRankThreshold:    settings.GetRerankThreshold(),
		RRFVecWeight:       settings.GetRrfVecWeight(),
		RRFEsWeight:        settings.GetRrfEsWeight(),
		RRFReRankWeight:    settings.GetRrfRerankWeight(),
		DocVecRecallNum:    settings.GetDocVecRecallNum(),
		QaVecRecallNum:     settings.GetQaVecRecallNum(),
		EsRecallNum:        settings.GetEsRecallNum(),
		EsReRankMinNum:     settings.GetEsRerankMinNum(),
		RRFReciprocalConst: uint32(settings.GetRrfReciprocalConst()),
		Operator:           settings.GetOperator(),
		CreateTime:         now,
		UpdateTime:         now,
		EsTopN:             settings.GetEsTopN(),
		Text2sqlModel:      settings.GetText2SqlModel(),
		Text2sqlPrompt:     settings.GetText2SqlPrompt(),
	}
}

// ClearAppKnowledgeResource 应用被删除回调，用来延迟（n小时/天后）清理应用知识库资源（文档、问答等）
func (s *Service) ClearAppKnowledgeResource(ctx context.Context, req *pb.ClearAppKnowledgeResourceReq) (*pb.ClearAppKnowledgeResourceRsp, error) {
	logx.I(ctx, "ClearAppKnowledgeResource Req:%+v", req)
	rsp := new(pb.ClearAppKnowledgeResourceRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, botBizID)
	if (err != nil && !errs.Is(err, errs.ErrRobotNotFound)) || (app != nil && !app.IsDeleted) {
		// 如果未返回robot不存在的错误码，或者app不是已删除的状态，但则不能清理资源，返回错误
		logx.E(ctx, "ClearAppKnowledgeResource appBizId:%s is not deleted", req.BotBizId)
		return rsp, errs.ErrSystem
	}
	if app == nil && (req.CorpPrimaryId == 0 || req.AppPrimaryId == 0) {
		logx.E(ctx, "ClearAppKnowledgeResource appBizId:%s is deleted, but corpPrimaryId or appPrimaryId is empty", req.BotBizId)
		return rsp, errs.ErrParameterInvalid
	}
	if app != nil {
		// 如果app能正常查到，则使用app的信息，否则使用传入的信息
		req.CorpPrimaryId = app.CorpPrimaryId
		req.AppPrimaryId = app.PrimaryId
	}

	if err = s.kbDao.CreateKnowledgeDeleteTask(ctx, entity.KnowledgeDeleteParams{
		Name:     entity.TaskTypeNameMap[entity.KnowledgeDeleteTask],
		RobotID:  req.AppPrimaryId,
		CorpID:   req.CorpPrimaryId,
		AppBizID: botBizID,
		TaskID:   req.GetTaskId(),
	}); err != nil {
		logx.E(ctx, "ClearAppKnowledgeResource CreateKnowledgeDeleteTask err:%+v", err)
		return nil, err
	}
	return &pb.ClearAppKnowledgeResourceRsp{}, nil
}

// AppDeletedCallback 应用被删除回调，用来停止异步任务（解析、审核、学习等），释放资源
// NOTE(ericjwang): 调用路径：/trpc.KEP.bot_admin_config_server.Admin/DeleteApp -> here，峰值 QPS 约为 1
func (s *Service) AppDeletedCallback(ctx context.Context, req *pb.AppDeletedCallbackReq) (*pb.AppDeletedCallbackRsp, error) {
	logx.I(ctx, "AppDeletedCallback req:%+v", req)
	rsp := &pb.AppDeletedCallbackRsp{}
	corpBizID, err := cast.ToUint64E(req.GetCorpBizId())
	if err != nil {
		logx.E(ctx, "AppDeletedCallback corpBizID:%+v err:%+v", req.GetCorpBizId(), err)
		return nil, err
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, corpBizID)
	// corpID, err := dao.GetCorpIDByCorpBizID(ctx, corpBizID)
	if err != nil {
		logx.E(ctx, "AppDeletedCallback corpBizID:%+v err:%+v", corpBizID, err)
		return nil, err
	}
	appBizID, err := cast.ToUint64E(req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "AppDeletedCallback appBizID:%+v err:%+v", req.GetAppBizId(), err)
		return nil, err
	}
	appID, err := s.cacheLogic.GetAppPrimaryIdByBizId(ctx, appBizID)
	if err != nil {
		if errors.Is(err, errs.ErrAppNotFound) {
			logx.W(ctx, "AppDeletedCallback appBizId:%d app not found, skip", appBizID)
			return rsp, nil
		}
		return nil, err
	}
	// 查询该应用下所有的解析任务
	filter := &docEntity.DocParseFilter{
		CorpPrimaryId: corp.GetCorpPrimaryId(),
		AppPrimaryId:  appID,
		Status:        []int32{docEntity.DocParseIng},
	}
	selectColumns := []string{docEntity.DocParseTblColTaskID}
	docParseList, err := s.docLogic.GetDocParseList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	if len(docParseList) == 0 {
		return rsp, nil
	}
	requestID := contextx.Metadata(ctx).RequestID()
	// 停止解析任务
	stopDocParseTaskWg := errgroupx.New()
	stopDocParseTaskWg.SetLimit(5)
	for _, docParse := range docParseList {
		logx.D(ctx, "AppDeletedCallback appBizId:%d StopDocParseTask taskID:%s", appBizID, docParse.TaskID)
		oneTask := docParse
		stopDocParseTaskWg.Go(func() error {
			err := s.rpc.FileManager.StopDocParseTask(ctx, oneTask.TaskID, requestID, appBizID)
			if err != nil {
				logx.W(ctx, "AppDeletedCallback appBizId:%d StopDocParseTask taskID:%s err:%v", appBizID, oneTask.TaskID, err)
			}
			return nil
		})
	}
	if err = stopDocParseTaskWg.Wait(); err != nil {
		logx.W(ctx, "AppDeletedCallback appBizId:%d StopDocParseTask len(docParseList):%d err:%v", appBizID, len(docParseList), err)
	}

	// 停止所有所有执行中的任务（审核中，学习中）
	tasks, err := scheduler.GetTasksByAppID(ctx, appID)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return rsp, nil
	}
	stopTaskWg := errgroupx.New()
	stopTaskWg.SetLimit(5)
	for _, task := range tasks {
		logx.D(ctx, "AppDeletedCallback appBizId:%d taskType:%d taskID:%d", appBizID, task.Type, task.ID)
		if task.Type == entity.DocDeleteTask {
			// 文档删除异步任务不能停止，因为文档删除异步任务需要调用retrieval服务接口清理数据
			continue
		}
		logx.D(ctx, "AppDeletedCallback appBizId:%d StopTask taskType:%d taskID:%d", appBizID, task.Type, task.ID)
		oneTask := task
		stopTaskWg.Go(func() error {
			err := scheduler.StopTask(ctx, oneTask.ID)
			if err != nil {
				logx.W(ctx, "AppDeletedCallback appBizId:%d StopTask taskID:%d err:%v", appBizID, oneTask.ID, err)
			}
			return nil
		})
	}
	if err = stopTaskWg.Wait(); err != nil {
		logx.W(ctx, "AppDeletedCallback appBizId:%d StopTask len(tasks):%d err:%v", appBizID, len(tasks), err)
	}

	return rsp, nil
}

// DescribrAppCharSize 获取应用的字符数
func (s *Service) DescribeAppCharSize(ctx context.Context, req *pb.DescribeAppCharSizeReq) (*pb.DescribeAppCharSizeRsp, error) {
	return s.svc.DescribeAppCharSize(ctx, req)
}

func (s *Service) ClearRealtimeAppResourceReleaseSegment(ctx context.Context, req *pb.ClearRealtimeAppResourceReleaseSegmentReq) (*pb.ClearRealtimeAppResourceReleaseSegmentRsp, error) {
	return s.svc.ClearRealtimeAppResourceReleaseSegment(ctx, req)
}
