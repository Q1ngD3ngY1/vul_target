package service

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// CheckUnconfirmedQa 是否存在未确认问答
func (s *Service) CheckUnconfirmedQa(ctx context.Context, req *pb.CheckUnconfirmedQaReq) (
	*pb.CheckUnconfirmedQaRsp, error) {
	rsp := new(pb.CheckUnconfirmedQaRsp)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	rsp.Exist, err = s.dao.CheckUnconfirmedQa(ctx, app.ID)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

// ListReleaseDocPreview 获取发布文档预览
func (s *Service) ListReleaseDocPreview(ctx context.Context, req *pb.ListReleaseDocPreviewReq) (
	*pb.ListReleaseDocPreviewRsp, error) {
	log.InfoContextf(ctx, "ListReleaseDocPreview Req:%+v", req)
	if req.GetReleaseBizId() == "" {
		// 版本ID=0，查询t_doc待发布文档
		return s.getReleaseDocWithZeroVersion(ctx, req)
	}
	releaseBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	release, err := s.dao.GetReleaseByBizID(ctx, releaseBizID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if release == nil {
		return nil, errs.ErrReleaseNotFound
	}
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	total, err := s.dao.GetModifyDocCount(ctx, app.ID, release.ID, req.GetQuery(), req.GetActions(), nil)
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.dao.GetModifyDocList(ctx, app.ID, release.ID, req.GetQuery(), req.GetActions(),
		req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		return nil, errs.ErrSystem
	}
	list := make([]*pb.ListReleaseDocPreviewRsp_Doc, 0)
	for _, doc := range docs {
		list = append(list, &pb.ListReleaseDocPreviewRsp_Doc{
			FileName:   doc.FileName,
			FileType:   doc.FileType,
			UpdateTime: doc.UpdateTime.Unix(),
			Action:     doc.Action,
			ActionDesc: i18n.Translate(ctx, doc.ActionDesc()),
			Message:    "-",
			DocBizId:   doc.BusinessID,
		})
	}
	return &pb.ListReleaseDocPreviewRsp{
		Total: total,
		List:  list,
	}, nil
}

func (s *Service) getReleaseDocWithZeroVersion(ctx context.Context, req *pb.ListReleaseDocPreviewReq) (
	*pb.ListReleaseDocPreviewRsp, error) {
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	isInit, err := s.checkReleaseIsInit(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if isInit {
		return &pb.ListReleaseDocPreviewRsp{}, nil
	}
	var startTime, endTime time.Time
	if req.GetStartTime() != 0 {
		startTime = time.Unix(req.GetStartTime(), 0)
	}
	if req.GetEndTime() != 0 {
		endTime = time.Unix(req.GetEndTime(), 0)
	}
	total, err := s.dao.GetWaitReleaseDocCount(ctx, corpID, app.ID, req.GetQuery(), startTime, endTime,
		req.GetActions())
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.dao.GetWaitReleaseDoc(ctx, corpID, app.ID, req.GetQuery(), startTime, endTime, req.GetActions(),
		req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		return nil, errs.ErrSystem
	}
	list := make([]*pb.ListReleaseDocPreviewRsp_Doc, 0)
	for _, doc := range docs {
		list = append(list, &pb.ListReleaseDocPreviewRsp_Doc{
			FileName:   doc.GetRealFileName(),
			FileType:   doc.FileType,
			UpdateTime: doc.UpdateTime.Unix(),
			Action:     doc.NextAction,
			ActionDesc: i18n.Translate(ctx, doc.NextActionDesc()),
			DocBizId:   doc.BusinessID,
		})
	}
	return &pb.ListReleaseDocPreviewRsp{
		Total: total,
		List:  list,
	}, nil
}

// ListReleaseQAPreview 获取发布QA预览
func (s *Service) ListReleaseQAPreview(ctx context.Context, req *pb.ListReleaseQAPreviewReq) (
	*pb.ListReleaseQAPreviewRsp, error) {
	log.InfoContextf(ctx, "ListReleaseQAPreview Req:%+v", req)
	if req.GetReleaseBizId() == "" {
		return s.getReleaseQAWithZeroVersion(ctx, req)
	}
	releaseID, err := util.CheckReqParamsIsUint64(ctx, req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	release, err := s.dao.GetReleaseByBizID(ctx, releaseID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if release == nil {
		return nil, errs.ErrReleaseNotFound
	}
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	emptyAction := make([]uint32, 0)
	total, err := s.dao.GetModifyQACount(ctx, app.ID, release.ID, req.GetQuery(), emptyAction, req.GetReleaseStatus())
	if err != nil {
		return nil, errs.ErrSystem
	}
	orderBy := ""
	if release.IsPublishFailed() {
		orderBy = " ORDER BY release_status DESC,id ASC"
	}
	list, err := s.dao.GetModifyQAList(ctx, app.ID, release.ID, req.GetQuery(), emptyAction, req.GetPageNumber(),
		req.GetPageSize(), orderBy, req.GetReleaseStatus())
	if err != nil {
		return nil, errs.ErrSystem
	}
	docIDs := make([]uint64, 0, len(list))
	qaIDs := make([]uint64, 0, len(list))
	for _, qa := range list {
		qaIDs = append(qaIDs, qa.QAID)
		if qa.DocID == 0 {
			continue
		}
		docIDs = append(docIDs, qa.DocID)
	}
	docs, err := s.dao.GetDocByIDs(ctx, slicex.Unique(docIDs), app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaList, err := s.dao.GetQADetails(ctx, pkg.CorpID(ctx), app.ID, qaIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	rspList := make([]*pb.ListReleaseQAPreviewRsp_QA, 0)
	for _, item := range list {
		fileName, fileType := "", ""
		docBizID := uint64(0)
		if doc, ok := docs[item.DocID]; ok {
			fileName = doc.FileName
			fileType = doc.FileType
			docBizID = doc.BusinessID
		}
		var qaBizID uint64
		if qa, ok := qaList[item.QAID]; ok {
			qaBizID = qa.BusinessID
		}

		rspList = append(rspList, &pb.ListReleaseQAPreviewRsp_QA{
			Question:      item.Question,
			UpdateTime:    item.UpdateTime.Unix(),
			Action:        item.Action,
			ActionDesc:    i18n.Translate(ctx, item.ActionDesc()),
			Source:        item.Source,
			SourceDesc:    i18n.Translate(ctx, item.SourceDesc(docs)),
			FileName:      fileName,
			FileType:      fileType,
			ReleaseStatus: item.ReleaseStatus,
			Message:       item.Message,
			QaBizId:       qaBizID,
			DocBizId:      docBizID,
		})
	}
	return &pb.ListReleaseQAPreviewRsp{
		Total: total,
		List:  rspList,
	}, nil
}

func (s *Service) getReleaseQAWithZeroVersion(ctx context.Context, req *pb.ListReleaseQAPreviewReq) (
	*pb.ListReleaseQAPreviewRsp, error) {
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	isInit, err := s.checkReleaseIsInit(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if isInit {
		return &pb.ListReleaseQAPreviewRsp{}, nil
	}
	var startTime, endTime time.Time
	if req.GetStartTime() != 0 {
		startTime = time.Unix(req.GetStartTime(), 0)
	}
	if req.GetEndTime() != 0 {
		endTime = time.Unix(req.GetEndTime(), 0)
	}
	total, err := s.dao.GetReleaseQACount(ctx, corpID, app.ID, req.GetQuery(), startTime, endTime, req.GetActions())
	if err != nil {
		return nil, errs.ErrSystem
	}
	list, err := s.dao.GetReleaseQAList(ctx, corpID, app.ID, req.GetQuery(), startTime, endTime, req.GetActions(),
		req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.getSourceDoc(ctx, list, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	rspList := make([]*pb.ListReleaseQAPreviewRsp_QA, 0)
	for _, item := range list {
		fileName, fileType := "", ""
		docBizID := uint64(0)
		if doc, ok := docs[item.DocID]; ok {
			fileName = doc.FileName
			fileType = doc.FileType
			docBizID = doc.BusinessID
		}
		rspList = append(rspList, &pb.ListReleaseQAPreviewRsp_QA{
			Question:   item.Question,
			UpdateTime: item.UpdateTime.Unix(),
			Action:     item.NextAction,
			ActionDesc: i18n.Translate(ctx, item.NextActionDesc()),
			Source:     item.Source,
			SourceDesc: i18n.Translate(ctx, item.SourceDesc(docs)),
			FileName:   fileName,
			FileType:   fileType,
			QaBizId:    item.BusinessID,
			DocBizId:   docBizID,
		})
	}
	return &pb.ListReleaseQAPreviewRsp{
		Total: total,
		List:  rspList,
	}, nil
}

// ListRejectedQuestionPreview 发布拒答问题预览
func (s *Service) ListRejectedQuestionPreview(ctx context.Context, req *pb.ListRejectedQuestionPreviewReq) (
	*pb.ListRejectedQuestionPreviewRsp, error) {
	log.InfoContextf(ctx, "ListRejectedQuestionPreview Req:%+v", req)
	if req.GetReleaseBizId() == "" {
		return s.getReleaseRejectedQuestionWithZeroVersion(ctx, req)
	}
	releaseID, err := util.CheckReqParamsIsUint64(ctx, req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	release, err := s.dao.GetReleaseByBizID(ctx, releaseID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if release == nil {
		return nil, errs.ErrReleaseNotFound
	}
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil || app.CorpID != corpID {
		return nil, errs.ErrRobotNotFound
	}
	query := req.GetQuery()
	page := req.GetPageNumber()
	pageSize := req.GetPageSize()
	total, err := s.dao.GetModifyRejectedQuestionCount(ctx, corpID, app.ID, release.ID, query, nil)
	if err != nil {
		return nil, err
	}
	list, err := s.dao.GetModifyRejectedQuestionList(ctx, corpID, app.ID, release.ID, query, page, pageSize)
	if err != nil {
		return nil, err
	}
	rspList := make([]*pb.ListRejectedQuestionPreviewRsp_RejectedQuestions, 0, len(list))
	for _, v := range list {
		rspList = append(rspList, &pb.ListRejectedQuestionPreviewRsp_RejectedQuestions{
			Question:   v.Question,
			UpdateTime: v.UpdateTime.Unix(),
			Action:     v.Action,
			ActionDesc: i18n.Translate(ctx, v.RejectedQuestionActionDesc()),
			Message:    v.Message,
		})
	}
	return &pb.ListRejectedQuestionPreviewRsp{
		Total: total,
		List:  rspList,
	}, nil
}

func (s *Service) getReleaseRejectedQuestionWithZeroVersion(ctx context.Context,
	req *pb.ListRejectedQuestionPreviewReq) (
	*pb.ListRejectedQuestionPreviewRsp, error) {
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil || app.CorpID != corpID {
		return nil, errs.ErrRobotNotFound
	}
	isInit, err := s.checkReleaseIsInit(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if isInit {
		return &pb.ListRejectedQuestionPreviewRsp{}, nil
	}
	query := req.GetQuery()
	page := req.GetPageNumber()
	pageSize := req.GetPageSize()

	var startTime, endTime time.Time
	if req.GetStartTime() != 0 {
		startTime = time.Unix(req.GetStartTime(), 0)
	}

	if req.GetEndTime() != 0 {
		endTime = time.Unix(req.GetEndTime(), 0)
	}

	total, err := s.dao.GetReleaseRejectedQuestionCount(ctx, corpID, app.ID, query, startTime, endTime,
		req.GetActions())
	if err != nil {
		return nil, err
	}

	list, err := s.dao.GetReleaseRejectedQuestionList(ctx, corpID, app.ID, page, pageSize, query, startTime, endTime,
		req.GetActions())
	if err != nil {
		return nil, err
	}
	rspList := make([]*pb.ListRejectedQuestionPreviewRsp_RejectedQuestions, 0, len(list))
	for _, v := range list {
		rspList = append(rspList, &pb.ListRejectedQuestionPreviewRsp_RejectedQuestions{
			Question:   v.Question,
			UpdateTime: v.UpdateTime.Unix(),
			Action:     v.Action,
			ActionDesc: i18n.Translate(ctx, v.ActionDesc()),
		})
	}
	return &pb.ListRejectedQuestionPreviewRsp{
		Total: total,
		List:  rspList,
	}, nil
}

// checkReleaseIsInit 校验发布任务是否在采集中
func (s *Service) checkReleaseIsInit(ctx context.Context, corpID, robotID uint64) (bool, error) {
	release, err := s.dao.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return false, err
	}
	if release != nil && release.IsInit() {
		return true, nil
	}
	return false, nil
}
