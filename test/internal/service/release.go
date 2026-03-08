package service

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// CheckUnconfirmedQa 是否存在未确认问答
func (s *Service) CheckUnconfirmedQa(ctx context.Context, req *pb.CheckUnconfirmedQaReq) (
	*pb.CheckUnconfirmedQaRsp, error) {
	rsp := new(pb.CheckUnconfirmedQaRsp)
	botBizID := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	rsp.Exist, err = s.qaLogic.CheckUnconfirmedQa(ctx, app.PrimaryId)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

// ListReleaseDocPreview 获取发布文档预览
func (s *Service) ListReleaseDocPreview(ctx context.Context, req *pb.ListReleaseDocPreviewReq) (
	*pb.ListReleaseDocPreviewRsp, error) {
	logx.I(ctx, "ListReleaseDocPreview Req:%+v", req)
	if req.GetReleaseBizId() == "" {
		// 版本ID=0，查询t_doc待发布文档
		return s.getReleaseDocWithZeroVersion(ctx, req)
	}
	releaseBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	release, err := s.releaseLogic.GetReleaseByBizID(ctx, releaseBizID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if release == nil {
		return nil, errs.ErrReleaseNotFound
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	total, err := s.releaseLogic.GetModifyDocCount(ctx, app.PrimaryId, release.ID, req.GetQuery(), req.GetActions(), nil)
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.releaseLogic.GetModifyDocList(ctx, app.PrimaryId, release.ID, req.GetQuery(), req.GetActions(), req.GetPageNumber(), req.GetPageSize())
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
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	isInit, err := s.checkReleaseIsInit(ctx, app.CorpPrimaryId, app.PrimaryId)
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
	total, err := s.docLogic.GetWaitReleaseDocCount(ctx, app.CorpPrimaryId, app.PrimaryId, req.GetQuery(), startTime, endTime, req.GetActions())
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.docLogic.GetWaitReleaseDoc(ctx, app.CorpPrimaryId, app.PrimaryId, req.GetQuery(), startTime, endTime, req.GetActions(),
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
	logx.I(ctx, "ListReleaseQAPreview Req:%+v", req)
	if req.GetReleaseBizId() == "" {
		logx.I(ctx, "ListReleaseQAPreview Req:%+v", req)
		return s.getReleaseQAWithZeroVersion(ctx, req)
	}
	releaseID, err := util.CheckReqParamsIsUint64(ctx, req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	release, err := s.releaseLogic.GetReleaseByBizID(ctx, releaseID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if release == nil {
		return nil, errs.ErrReleaseNotFound
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	emptyAction := make([]uint32, 0)
	total, err := s.releaseLogic.GetModifyQACount(ctx, app.PrimaryId, release.ID, req.GetQuery(), emptyAction, req.GetReleaseStatus())
	if err != nil {
		return nil, errs.ErrSystem
	}
	orderBy := ""
	if release.IsPublishFailed() {
		orderBy = " ORDER BY release_status DESC,id ASC"
	}

	list, err := s.releaseLogic.GetModifyQAList(ctx, app.PrimaryId, release.ID, req.GetQuery(), emptyAction, req.GetPageNumber(),
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
	docs, err := s.docLogic.GetDocByIDs(ctx, slicex.Unique(docIDs), app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaList, err := s.qaLogic.GetQADetails(ctx, contextx.Metadata(ctx).CorpID(), app.PrimaryId, qaIDs)
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
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	isInit, err := s.checkReleaseIsInit(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "[getReleaseQAWithZeroVersion]checkReleaseIsInit failed, error:%v", err)
		return nil, errs.ErrSystem
	}
	if isInit {
		return &pb.ListReleaseQAPreviewRsp{
			Total: 0,
			List:  make([]*pb.ListReleaseQAPreviewRsp_QA, 0),
		}, nil
	}
	var startTime, endTime time.Time
	if req.GetStartTime() != 0 {
		startTime = time.Unix(req.GetStartTime(), 0)
	}
	if req.GetEndTime() != 0 {
		endTime = time.Unix(req.GetEndTime(), 0)
	}
	total, err := s.qaLogic.GetReleaseQACount(ctx, app.CorpPrimaryId, app.PrimaryId, req.GetQuery(), startTime, endTime, req.GetActions())
	if err != nil {
		logx.E(ctx, "[getReleaseQAWithZeroVersion]GetReleaseQACount failed, error:%v", err)
		return nil, errs.ErrSystem
	}
	list, err := s.qaLogic.GetReleaseQAList(ctx, app.CorpPrimaryId, app.PrimaryId, req.GetQuery(), startTime, endTime, req.GetActions(),
		req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		logx.E(ctx, "[getReleaseQAWithZeroVersion]GetReleaseQAList failed, error:%v", err)
		return nil, errs.ErrSystem
	}
	docs, err := s.getSourceDoc(ctx, list, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "[getReleaseQAWithZeroVersion]getSourceDoc failed, error:%v", err)
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

func (s *Service) getReleaseLabelWithZeroVersion(ctx context.Context, req *pb.ListReleaseLabelPreviewReq) (
	*pb.ListReleaseLabelPreviewRsp, error) {
	rsp := new(pb.ListReleaseLabelPreviewRsp)
	corpID := contextx.Metadata(ctx).CorpID()
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	isInit, err := s.checkReleaseIsInit(ctx, corpID, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if isInit {
		return &pb.ListReleaseLabelPreviewRsp{
			Total: 0,
			List:  make([]*pb.ReleaseLabel, 0),
		}, nil
	}
	var startTime, endTime time.Time
	if req.GetStartTime() != 0 {
		startTime = time.Unix(req.GetStartTime(), 0)
	}
	if req.GetEndTime() != 0 {
		endTime = time.Unix(req.GetEndTime(), 0)
	}
	total, err := s.labelLogic.GetWaitReleaseAttributeCount(ctx, app.PrimaryId, req.GetQuery(), req.GetActions(),
		startTime, endTime)
	if err != nil {
		return nil, errs.ErrSystem
	}
	rsp.Total = total
	rsp.List = make([]*pb.ReleaseLabel, 0)
	if total == 0 {
		return rsp, nil
	}
	labels, err := s.labelLogic.GetWaitReleaseAttributeList(ctx, app.PrimaryId, req.GetQuery(), req.GetActions(),
		req.GetPageNumber(), req.GetPageSize(), startTime, endTime)
	if err != nil {
		return nil, errs.ErrSystem
	}
	for _, v := range labels {
		rsp.List = append(rsp.List, &pb.ReleaseLabel{
			LabelName:  v.Name,
			UpdateTime: v.UpdateTime.Unix(),
			Action:     v.NextAction,
			ActionDesc: i18n.Translate(ctx, v.ActionDesc()),
			Message:    "",
		})
	}
	return rsp, nil
}

func (s *Service) ListReleaseLabelPreview(ctx context.Context, req *pb.ListReleaseLabelPreviewReq) (
	*pb.ListReleaseLabelPreviewRsp, error) {
	rsp := new(pb.ListReleaseLabelPreviewRsp)
	return rsp, nil
}

// ListRejectedQuestionPreview 获取发布拒答问题预览
func (s *Service) ListRejectedQuestionPreview(ctx context.Context, req *pb.ListRejectedQuestionPreviewReq) (
	*pb.ListRejectedQuestionPreviewRsp, error) {
	logx.I(ctx, "ListRejectedQuestionPreview Req:%+v", req)
	if req.GetReleaseBizId() == "" || req.GetReleaseBizId() == "0" {
		logx.I(ctx, "ListRejectedQuestionPreview Req with zeroVersion")
		return s.getReleaseRejectedQuestionWithZeroVersion(ctx, req)
	}
	releaseID, err := util.CheckReqParamsIsUint64(ctx, req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	release, err := s.releaseLogic.GetReleaseByBizID(ctx, releaseID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if release == nil {
		return nil, errs.ErrReleaseNotFound
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	query := req.GetQuery()
	page := req.GetPageNumber()
	pageSize := req.GetPageSize()
	total, err := s.releaseLogic.GetModifyRejectedQuestionCount(ctx, app.CorpPrimaryId, app.PrimaryId, release.ID, query, nil)
	if err != nil {
		return nil, err
	}
	list, err := s.releaseLogic.GetModifyRejectedQuestionList(ctx, app.CorpPrimaryId, app.PrimaryId, release.ID, query, page, pageSize)
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
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	isInit, err := s.checkReleaseIsInit(ctx, app.CorpPrimaryId, app.PrimaryId)
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

	total, err := s.releaseLogic.GetReleaseRejectedQuestionCount(ctx, app.CorpPrimaryId, app.PrimaryId, query, startTime, endTime, req.GetActions())
	if err != nil {
		return nil, err
	}

	list, err := s.releaseLogic.GetReleaseRejectedQuestionList(ctx, app.CorpPrimaryId, app.PrimaryId, page, pageSize, query, startTime, endTime, req.GetActions())
	if err != nil {
		return nil, err
	}
	rspList := make([]*pb.ListRejectedQuestionPreviewRsp_RejectedQuestions, 0, len(list))
	for _, v := range list {
		rspList = append(rspList, &pb.ListRejectedQuestionPreviewRsp_RejectedQuestions{
			Question:   v.Question,
			UpdateTime: v.UpdateTime.Unix(),
			Action:     uint32(v.Action),
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
	release, err := s.releaseLogic.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return false, err
	}
	if release != nil && release.IsInit() {
		return true, nil
	}
	return false, nil
}
