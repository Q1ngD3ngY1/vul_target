package api

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"golang.org/x/exp/slices"
	"math"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ListQA 问答对列表
func (s *Service) ListQA(ctx context.Context, req *pb.ListQAReq) (*pb.ListQARsp, error) {
	log.InfoContextf(ctx, "ListQA Req:%+v", req)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if req.GetQueryType() == "" {
		req.QueryType = model.DocQueryTypeFileName
	}
	req.AcceptStatus = slicex.Unique(req.GetAcceptStatus())
	req.ReleaseStatus = slicex.Unique(req.GetReleaseStatus())
	if app.IsShared {
		if slices.Contains(req.ReleaseStatus, model.QAReleaseStatusInit) {
			req.ReleaseStatus = append(req.ReleaseStatus, model.QAReleaseStatusSuccess)
		}
	}
	qaListReq, err := s.getQaListReq(ctx, req, app.ID, app.CorpID)
	if err != nil {
		return nil, err
	}
	waitVerifyTotal, notAcceptedTotal, acceptedTotal, total, err := s.dao.GetQAListCount(ctx, qaListReq)
	if err != nil {
		return nil, errs.ErrSystem
	}
	pageNumber, err := s.getQAPageNumber(ctx, app, req)
	if err != nil {
		return nil, errs.ErrSystem
	}
	list, err := s.dao.GetQAList(ctx, qaListReq)
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.getSourceDoc(ctx, list, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	pendingReleaseQA, err := s.getPendingReleaseQA(ctx, app.ID, list)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaList := make([]*pb.ListQARsp_QA, 0, len(list))
	latestRelease, err := s.dao.GetLatestRelease(ctx, app.CorpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaIDs := make([]uint64, 0, len(list))
	for _, qa := range list {
		qaIDs = append(qaIDs, qa.ID)
	}
	qaCountMap, err := s.dao.GetSimilarQuestionsCountByQAIDs(ctx, app.CorpID, app.ID, qaIDs)
	if err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsCountByQAIDs failed, qaIDs(%+v) err:%+v", qaIDs, err)
		return nil, errs.ErrSystem
	}
	for _, item := range list {
		_, pendingOk := pendingReleaseQA[item.ID]
		var fileName, fileType string
		var docBizID uint64
		var simCount uint32
		if doc, ok := docs[item.DocID]; ok {
			fileName = doc.FileName
			fileType = doc.FileType
			docBizID = doc.BusinessID
		}
		if qaCountMap != nil {
			if val, ok := qaCountMap[item.ID]; ok {
				simCount = val
			} else {
				simCount = 0
			}
		}
		qaNode := &pb.ListQARsp_QA{
			QaBizId:            item.BusinessID,
			Question:           item.Question,
			Source:             item.Source,
			SourceDesc:         i18n.Translate(ctx, item.SourceDesc(docs)),
			UpdateTime:         item.UpdateTime.Unix(),
			CreateTime:         item.CreateTime.Unix(),
			Status:             item.Status(),
			StatusDesc:         i18n.Translate(ctx, item.StatusDesc(latestRelease.IsPublishPause())),
			DocBizId:           docBizID,
			IsAllowEdit:        !pendingOk && item.IsAllowEdit(),
			IsAllowAccept:      !pendingOk && item.IsAllowAccept(),
			IsAllowDelete:      !pendingOk && item.IsAllowDelete(),
			FileName:           fileName,
			FileType:           fileType,
			Answer:             item.Answer,
			QaCharSize:         item.CharSize,
			ExpireStart:        uint64(item.ExpireStart.Unix()),
			ExpireEnd:          uint64(item.ExpireEnd.Unix()),
			SimilarQuestionNum: simCount,
		}
		if app.IsShared {
			if qaNode.Status == model.QAReleaseStatusInit {
				qaNode.StatusDesc = i18n.Translate(ctx, i18nkey.KeyImportComplete)
			}
		}
		qaList = append(qaList, qaNode)
	}
	return &pb.ListQARsp{
		Total:            uint64(total),
		WaitVerifyTotal:  uint64(waitVerifyTotal),
		NotAcceptedTotal: uint64(notAcceptedTotal),
		AcceptedTotal:    uint64(acceptedTotal),
		PageNumber:       pageNumber,
		List:             qaList,
	}, nil
}

// getQaListReq 获取QaListReq 请求参数
func (s *Service) getQaListReq(ctx context.Context, req *pb.ListQAReq, robotID, corpID uint64) (
	*model.QAListReq, error) {
	deletingDocID, err := s.getDeletingDocID(ctx, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	var cateIDs []uint64
	if req.GetCateBizId() != model.AllCateID {
		cateID, err := s.dao.CheckCateBiz(ctx, model.QACate, corpID, uint64(req.GetCateBizId()), robotID)
		if err != nil {
			return nil, err
		}
		cateIDs = append(cateIDs, cateID)
	}
	docID, err := s.validateDocAndRetrieveID(ctx, req.GetDocBizId(), robotID)
	if err != nil {
		return nil, err
	}
	validityStatus, releaseStatus, err := s.getQaExpireStatus(req)
	if err != nil {
		return nil, err
	}
	return &model.QAListReq{
		RobotID:        robotID,
		CorpID:         corpID,
		IsDeleted:      model.QAIsNotDeleted,
		Query:          req.GetQuery(),
		QueryType:      req.GetQueryType(),
		QueryAnswer:    req.GetQueryAnswer(),
		Source:         req.GetSource(),
		ExcludeDocID:   deletingDocID,
		AcceptStatus:   req.GetAcceptStatus(),
		ReleaseStatus:  releaseStatus,
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		DocID:          utils.When(docID > 0, []uint64{docID}, nil),
		CateIDs:        cateIDs,
		QABizIDs:       req.GetQaBizIds(),
		ValidityStatus: validityStatus,
	}, nil
}

func (s *Service) getQAPageNumber(ctx context.Context, app *model.App, req *pb.ListQAReq) (uint32, error) {
	corpID := pkg.CorpID(ctx)
	if req.GetQaBizId() == 0 {
		return req.GetPageNumber(), nil
	}
	qa, err := s.dao.GetQAByBizID(ctx, req.GetQaBizId())
	if err != nil {
		return 0, err
	}
	if qa == nil {
		return req.GetPageNumber(), nil
	}
	if qa.CorpID != corpID || qa.RobotID != app.ID {
		return 0, errs.ErrQANotFound
	}
	pageReq := &model.QAListReq{
		CorpID:        corpID,
		RobotID:       app.ID,
		IsDeleted:     model.QAIsNotDeleted,
		Query:         req.GetQuery(),
		QueryType:     req.GetQueryType(),
		AcceptStatus:  req.GetAcceptStatus(),
		ReleaseStatus: req.GetReleaseStatus(),
		UpdateTime:    qa.UpdateTime,
	}
	if req.GetCateBizId() != model.AllCateID {
		cates, err := s.dao.GetCateList(ctx, model.QACate, corpID, app.ID)
		if err != nil {
			return 0, errs.ErrSystem
		}

		node := model.BuildCateTree(cates).FindNode(uint64(req.GetCateBizId()))
		if node == nil {
			return 0, errs.ErrCateNotFound
		}
		pageReq.CateIDs = append(node.ChildrenIDs(), node.ID)
	}
	if req.GetDocBizId() != 0 {
		doc, err := s.dao.GetDocByBizID(ctx, req.GetDocBizId(), app.ID)
		if err != nil {
			return 0, errs.ErrSystem
		}
		if doc != nil {
			pageReq.DocID = []uint64{doc.ID}
		}
	}

	pageReq.UpdateTimeEqual = false
	_, _, _, afterQAUpdateTimeCount, err := s.dao.GetQAListCount(ctx, pageReq)
	if err != nil {
		return 0, err
	}
	pageReq.UpdateTimeEqual = true
	pageReq.QAID = qa.ID
	_, _, _, afterQAIDCount, err := s.dao.GetQAListCount(ctx, pageReq)
	if err != nil {
		return 0, err
	}
	pages := uint32(math.Ceil(float64(afterQAUpdateTimeCount+afterQAIDCount) / float64(req.GetPageSize())))
	return pages, nil
}

func (s *Service) getPendingReleaseQA(ctx context.Context, robotID uint64, qas []*model.DocQA) (map[uint64]*model.
ReleaseQA, error) {
	corpID := pkg.CorpID(ctx)
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return nil, nil
	}
	if latestRelease.IsPublishDone() {
		return nil, nil
	}
	modifyQAs, err := s.dao.GetReleaseModifyQA(ctx, latestRelease, qas)
	if err != nil {
		return nil, err
	}
	return modifyQAs, nil
}

// getSourceDoc 来源文档
func (s *Service) getSourceDoc(ctx context.Context, qas []*model.DocQA, robotID uint64) (map[uint64]*model.Doc, error) {
	docIDs := make([]uint64, 0, len(qas))
	for _, qa := range qas {
		if qa.DocID == 0 {
			continue
		}
		docIDs = append(docIDs, qa.DocID)
	}
	return s.dao.GetDocByIDs(ctx, slicex.Unique(docIDs), robotID)
}

func (s *Service) getDeletingDocID(ctx context.Context, corpID, robotID uint64) ([]uint64, error) {
	docs, err := s.dao.GetDeletingDoc(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids, nil
}

func (s *Service) validateDocAndRetrieveID(ctx context.Context, docBizID uint64, robotID uint64) (uint64, error) {
	if docBizID == 0 {
		return 0, nil
	}
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, robotID)
	if err != nil {
		return 0, errs.ErrSystem
	}
	if doc == nil {
		return 0, errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return 0, errs.ErrDocHasDeleted
	}
	return doc.ID, nil
}

func (s *Service) getQaExpireStatus(req *pb.ListQAReq) (uint32, []uint32, error) {
	var validityStatus uint32
	if len(req.GetReleaseStatus()) == 0 {
		return validityStatus, req.GetReleaseStatus(), nil
	}
	var releaseStatus []uint32
	for i := range req.GetReleaseStatus() {
		switch req.GetReleaseStatus()[i] { // 预留后续会有未生效、生效中状态
		case model.QAReleaseStatusExpired:
			validityStatus = model.QaExpiredStatus
		default:
			releaseStatus = append(releaseStatus, req.GetReleaseStatus()[i])
		}
	}
	// 如果选择了状态，但是没有选择已过期，那就是未过期
	if validityStatus != model.QaExpiredStatus && len(releaseStatus) > 0 {
		validityStatus = model.QaUnExpiredStatus
	}
	return validityStatus, releaseStatus, nil
}
