package api

import (
	"context"
	"fmt"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/linker"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"

	"git.code.oa.com/trpc-go/trpc-go/log"
	appImpl "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// getAppByAppBizID 通过业务ID获取应用详情
func (s *Service) getAppByAppBizID(ctx context.Context, businessID uint64) (*model.App, error) {
	appDB, err := s.dao.GetAppByAppBizID(ctx, businessID)
	if err != nil {
		return nil, err
	}
	if appDB == nil {
		return nil, errs.ErrAppNotFound
	}
	if appDB.HasDeleted() {
		return nil, errs.ErrAppNotFound
	}
	instance := appImpl.GetApp(appDB.AppType)
	if instance == nil {
		return nil, errs.ErrAppTypeInvalid
	}
	app, err := instance.AnalysisDescribeApp(ctx, appDB)
	if err != nil {
		return nil, errs.ErrSystem
	}
	corpID := pkg.CorpID(ctx)
	if corpID != 0 && corpID != appDB.CorpID {
		log.WarnContextf(ctx, "当前企业与应用归属企业不一致 corpID:%d robot:%+v", corpID, appDB)
		return nil, errs.ErrAppNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, appDB.CorpID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if !corp.IsValid() {
		return nil, errs.ErrCorpInValid
	}
	if err = s.dao.CreateAppVectorIndex(ctx, appDB); err != nil {
		return nil, errs.ErrAppInitFail
	}
	return app, nil
}

func (s *Service) getPreviewRspDocByApp(
	ctx context.Context, _ *admin.GetAppInfoRsp, docs []*retrieval.SearchVectorRsp_Doc, robotID uint64,
) ([]*pb.SearchPreviewRsp_Doc, error) {
	linkContents, err := s.dao.GetLinkContentsFromSearchVectorResponse(
		ctx, robotID, docs,
		func(doc *retrieval.SearchVectorRsp_Doc, qa *model.DocQA) any {
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
		func(doc *retrieval.SearchVectorRsp_Doc, segment *model.DocSegmentExtend) any {
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
	return dao.Link(ctx, linkContents, func(t *pb.SearchPreviewRsp_Doc, v linker.Content) *pb.SearchPreviewRsp_Doc {
		t.OrgData = v.Value
		return t
	}), nil
}

// GetRobotRetrievalConfig 获取机器人检索配置
func (s *Service) GetRobotRetrievalConfig(ctx context.Context, req *pb.GetRobotRetrievalConfigReq) (
	*pb.GetRobotRetrievalConfigRsp, error) {
	rsp := new(pb.GetRobotRetrievalConfigRsp)
	retrievalConfig, err := s.dao.GetRetrievalConfig(ctx, req.GetRobotId())
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
		RrfReciprocalConst: retrievalConfig.RRFReciprocalConst,
		EsTopN:             retrievalConfig.EsTopN,
		Text2SqlModel:      retrievalConfig.Text2sqlModel,
		Text2SqlPrompt:     retrievalConfig.Text2sqlPrompt,
	}
	return rsp, nil
}

// SaveRobotRetrievalConfig 保存机器人检索配置
func (s *Service) SaveRobotRetrievalConfig(ctx context.Context, req *pb.SaveRobotRetrievalConfigReq) (
	*pb.SaveRobotRetrievalConfigRsp, error) {
	rsp := new(pb.SaveRobotRetrievalConfigRsp)
	retrievalConfig := convertToRetrievalConfig(req.GetSettings())
	if !retrievalConfig.IsCheckRetrievalConfig() {
		return rsp, errs.ErrRetrievalConfig
	}
	log.InfoContextf(ctx, "SaveRobotRetrievalConfig:%v", retrievalConfig)
	err := s.dao.SaveRetrievalConfig(ctx, req.GetRobotId(), retrievalConfig, req.GetSettings().Operator)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

func convertToRetrievalConfig(settings *pb.RobotRetrievalConfig) model.RetrievalConfig {
	if settings == nil {
		return model.RetrievalConfig{}
	}
	return model.RetrievalConfig{
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
		RRFReciprocalConst: settings.GetRrfReciprocalConst(),
		EsTopN:             settings.GetEsTopN(),
		Text2sqlModel:      settings.GetText2SqlModel(),
		Text2sqlPrompt:     settings.GetText2SqlPrompt(),
	}
}

// CheckVarIsUsed 检查自定义参数是否被使用
func (s *Service) CheckVarIsUsed(ctx context.Context, req *pb.CheckVarIsUsedReq) (
	*pb.CheckVarIsUsedRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CheckVarIsUsedRsp)
	return rsp, nil
}

// ModifyAppVar 修改应用检索范围自定义参数
func (s *Service) ModifyAppVar(ctx context.Context, req *pb.ModifyAppVarReq) (
	*pb.ModifyAppVarRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ModifyAppVarRsp)
	return rsp, nil
}

// modifyAppSearchRange 修改应用检索范围数据
func (s *Service) modifyAppSearchRange(ctx context.Context, app *model.App, varInfo *pb.ModifyAppVarReqVarInfo, attribute *model.Attribute) error {
	changed, err := fillAppPreview(app, varInfo, attribute)
	if err != nil {
		return err
	}
	if changed {
		err = s.dao.ModifyAppPreviewJSON(ctx, app.ToDB())
		if err != nil {
			log.WarnContextf(ctx, "ModifyAppVar ModifyAppPreviewJSON err:%+v", err)
			return err
		}
	}
	return nil
}

// fillAppPreview 填充 app preview 数据
func fillAppPreview(app *model.App, varInfo *pb.ModifyAppVarReqVarInfo, attribute *model.Attribute) (bool, error) {
	var changed bool
	if app.AppType != model.KnowledgeQaAppType {
		return changed, nil
	}
	if varInfo != nil {
		if _, ok := app.PreviewDetails.AppConfig.KnowledgeQaConfig.SearchRange.APIVarMap[varInfo.VarId]; ok {
			app.PreviewDetails.AppConfig.KnowledgeQaConfig.SearchRange.APIVarMap[varInfo.VarId] = varInfo.VarName
			changed = true
		}
	}
	if attribute != nil {
		if _, ok := app.PreviewDetails.AppConfig.KnowledgeQaConfig.SearchRange.LabelAttrMap[attribute.BusinessID]; ok {
			app.PreviewDetails.AppConfig.KnowledgeQaConfig.SearchRange.LabelAttrMap[attribute.BusinessID] = attribute.Name
			changed = true
		}
	}
	return changed, nil
}

// ClearAppKnowledgeResource 应用被删除回调，用来延迟（n小时/天后）清理应用知识库资源（文档、问答等）
func (s *Service) ClearAppKnowledgeResource(ctx context.Context, req *pb.ClearAppKnowledgeResourceReq) (
	*pb.ClearAppKnowledgeResourceRsp, error) {
	log.InfoContextf(ctx, "ClearAppKnowledgeResource Req:%+v", req)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	appInfo, err := client.GetAppInfo(ctx, botBizID, model.AppTestScenes)
	if err != nil && !errs.Is(err, errs.ErrRobotNotFound) {
		return nil, err
	}
	if appInfo != nil && !appInfo.GetIsDelete() {
		return nil, fmt.Errorf("robot:%d not exits", botBizID)
	}
	if err = s.dao.CreateKnowledgeDeleteTask(ctx, model.KnowledgeDeleteParams{
		Name:    model.TaskTypeNameMap[model.KnowledgeDeleteTask],
		RobotID: req.GetAppPrimaryId(),
		CorpID:  req.GetCorpPrimaryId(),
		TaskID:  req.GetTaskId(),
	}); err != nil {
		log.ErrorContextf(ctx, "ClearAppKnowledgeResource CreateKnowledgeDeleteTask err:%+v", err)
		return nil, err
	}
	return &pb.ClearAppKnowledgeResourceRsp{}, nil
}

// AppDeletedCallback 应用被删除回调，用来停止异步任务（解析、审核、学习等），释放资源
func (s *Service) AppDeletedCallback(ctx context.Context, req *pb.AppDeletedCallbackReq) (*pb.AppDeletedCallbackRsp, error) {
	log.InfoContextf(ctx, "AppDeletedCallback req:%+v", req)
	rsp := &pb.AppDeletedCallbackRsp{}
	corpBizID, err := cast.ToUint64E(req.GetCorpBizId())
	if err != nil {
		return nil, err
	}
	corpID, err := dao.GetCorpIDByCorpBizID(ctx, corpBizID)
	if err != nil {
		return nil, err
	}
	appBizID, err := cast.ToUint64E(req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	appID, err := dao.GetAppIDByAppBizID(ctx, appBizID)
	if err != nil {
		return nil, err
	}
	// 查询该应用下所有的解析任务
	filter := &dao.DocParseFilter{
		CorpID:  corpID,
		RobotID: appID,
		Status:  []int32{model.DocParseIng},
	}
	selectColumns := []string{dao.DocParseTblColTaskID}
	docParseList, err := dao.GetDocParseDao().GetDocParseList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	if len(docParseList) == 0 {
		return rsp, nil
	}
	requestID := pkg.RequestID(ctx)
	// 停止解析任务
	stopDocParseTaskWg := errgroupx.Group{}
	stopDocParseTaskWg.SetLimit(5)
	for _, docParse := range docParseList {
		log.DebugContextf(ctx, "AppDeletedCallback appBizId:%d StopDocParseTask taskID:%s",
			appBizID, docParse.TaskID)
		oneTask := docParse
		stopDocParseTaskWg.Go(func() error {
			err := client.StopDocParseTask(ctx, oneTask.TaskID, requestID, appBizID)
			if err != nil {
				log.WarnContextf(ctx, "AppDeletedCallback appBizId:%d StopDocParseTask taskID:%s err:%v",
					appBizID, oneTask.TaskID, err)
			}
			return nil
		})
	}
	if err = stopDocParseTaskWg.Wait(); err != nil {
		log.WarnContextf(ctx, "AppDeletedCallback appBizId:%d StopDocParseTask len(docParseList):%d err:%v",
			appBizID, len(docParseList), err)
	}

	// 停止所有所有执行中的任务（审核中，学习中）
	tasks, err := dao.GetTasksByAppID(ctx, appID)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return rsp, nil
	}
	stopTaskWg := errgroupx.Group{}
	stopTaskWg.SetLimit(5)
	for _, task := range tasks {
		if task.Type == model.DocDeleteTask {
			// 文档删除异步任务不能停止，因为文档删除异步任务需要调用retrieval服务接口清理数据
			continue
		}
		log.DebugContextf(ctx, "AppDeletedCallback appBizId:%d StopTask taskType:%d taskID:%d",
			appBizID, task.Type, task.ID)
		oneTask := task
		stopTaskWg.Go(func() error {
			err := dao.StopTask(ctx, oneTask.ID)
			if err != nil {
				log.WarnContextf(ctx, "AppDeletedCallback appBizId:%d StopTask taskID:%d err:%v",
					appBizID, oneTask.ID, err)
			}
			return nil
		})
	}
	if err = stopTaskWg.Wait(); err != nil {
		log.WarnContextf(ctx, "AppDeletedCallback appBizId:%d StopTask len(tasks):%d err:%v",
			appBizID, len(tasks), err)
	}

	return rsp, nil
}
