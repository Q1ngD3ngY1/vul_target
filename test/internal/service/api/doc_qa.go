package api

import (
	"context"
	"math"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ListQA 问答对列表
func (s *Service) ListQA(ctx context.Context, req *pb.ListQAReq) (*pb.ListQARsp, error) {
	logx.I(ctx, "ListQA Req:%+v", req)
	appid := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.svc.DescribeAppAndCheckCorp(ctx, appid)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if req.GetQueryType() == "" {
		req.QueryType = docEntity.DocQueryTypeFileName
	}
	qaListReq, err := s.getQaListReq(ctx, req, app.PrimaryId, app.CorpPrimaryId)
	if err != nil {
		return nil, err
	}
	waitVerifyTotal, notAcceptedTotal, acceptedTotal, total, err := s.qaLogic.GetQAListCount(ctx, qaListReq)
	if err != nil {
		return nil, errs.ErrSystem
	}
	pageNumber, err := s.getQAPageNumber(ctx, app, req)
	if err != nil {
		return nil, errs.ErrSystem
	}
	list, err := s.qaLogic.GetQAList(ctx, qaListReq)
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.getSourceDoc(ctx, list, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	pendingReleaseQA, err := s.getPendingReleaseQA(ctx, app.PrimaryId, list)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaList := make([]*pb.ListQARsp_QA, 0, len(list))
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaIDs := make([]uint64, 0, len(list))
	for _, qa := range list {
		qaIDs = append(qaIDs, qa.ID)
	}
	qaCountMap, err := s.qaLogic.GetSimilarQuestionsCountByQAIDs(ctx, app.CorpPrimaryId, app.PrimaryId, qaIDs)
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionsCountByQAIDs failed, qaIDs(%+v) err:%+v", qaIDs, err)
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
			QaSize:             item.QaSize,
		}
		if app.IsShared {
			if qaNode.Status == qaEntity.QAReleaseStatusInit {
				qaNode.StatusDesc = "导入完成"
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
	*qaEntity.QAListReq, error) {
	deletingDocID, err := s.getDeletingDocID(ctx, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	var cateIDs []uint64
	if req.GetCateBizId() != cateEntity.AllCateID {
		cateID, err := s.cateLogic.VerifyCateBiz(ctx, cateEntity.QACate, corpID, uint64(req.GetCateBizId()), robotID)
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
	return &qaEntity.QAListReq{
		RobotID:        robotID,
		CorpID:         corpID,
		IsDeleted:      qaEntity.QAIsNotDeleted,
		Query:          req.GetQuery(),
		QueryType:      req.GetQueryType(),
		QueryAnswer:    req.GetQueryAnswer(),
		Source:         req.GetSource(),
		ExcludeDocID:   deletingDocID,
		AcceptStatus:   req.GetAcceptStatus(),
		ReleaseStatus:  releaseStatus,
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		DocID:          gox.IfElse(docID > 0, []uint64{docID}, nil),
		CateIDs:        cateIDs,
		QABizIDs:       req.GetQaBizIds(),
		ValidityStatus: validityStatus,
	}, nil
}

func (s *Service) getQAPageNumber(ctx context.Context, app *entity.App, req *pb.ListQAReq) (uint32, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	if req.GetQaBizId() == 0 {
		return req.GetPageNumber(), nil
	}
	qa, err := s.qaLogic.GetQAByBizID(ctx, req.GetQaBizId())
	if err != nil {
		return 0, err
	}
	if qa == nil {
		return req.GetPageNumber(), nil
	}
	if qa.CorpID != corpID || qa.RobotID != app.PrimaryId {
		return 0, errs.ErrQANotFound
	}
	pageReq := &qaEntity.QAListReq{
		CorpID:        corpID,
		RobotID:       app.PrimaryId,
		IsDeleted:     qaEntity.QAIsNotDeleted,
		Query:         req.GetQuery(),
		QueryType:     req.GetQueryType(),
		AcceptStatus:  req.GetAcceptStatus(),
		ReleaseStatus: req.GetReleaseStatus(),
		UpdateTime:    qa.UpdateTime,
	}
	if req.GetCateBizId() != cateEntity.AllCateID {
		cates, err := s.cateLogic.DescribeCateList(ctx, cateEntity.QACate, corpID, app.PrimaryId)
		if err != nil {
			return 0, errs.ErrSystem
		}

		node := cateEntity.BuildCateTree(cates).FindNode(uint64(req.GetCateBizId()))
		if node == nil {
			return 0, errs.ErrCateNotFound
		}
		pageReq.CateIDs = append(node.ChildrenIDs(), node.ID)
	}
	if req.GetDocBizId() != 0 {
		doc, err := s.docLogic.GetDocByBizID(ctx, req.GetDocBizId(), app.PrimaryId)
		if err != nil {
			return 0, errs.ErrSystem
		}
		if doc != nil {
			pageReq.DocID = []uint64{doc.ID}
		}
	}

	pageReq.UpdateTimeEqual = false
	_, _, _, afterQAUpdateTimeCount, err := s.qaLogic.GetQAListCount(ctx, pageReq)
	if err != nil {
		return 0, err
	}
	pageReq.UpdateTimeEqual = true
	pageReq.QAID = qa.ID
	_, _, _, afterQAIDCount, err := s.qaLogic.GetQAListCount(ctx, pageReq)
	if err != nil {
		return 0, err
	}
	pages := uint32(math.Ceil(float64(afterQAUpdateTimeCount+afterQAIDCount) / float64(req.GetPageSize())))
	return pages, nil
}

func (s *Service) getPendingReleaseQA(ctx context.Context, robotID uint64, qas []*qaEntity.DocQA) (map[uint64]*releaseEntity.
	ReleaseQA, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return nil, nil
	}
	if latestRelease.IsPublishDone() {
		return nil, nil
	}
	modifyQAs, err := s.releaseLogic.GetReleaseModifyQA(ctx, latestRelease, qas)
	if err != nil {
		return nil, err
	}
	return modifyQAs, nil
}

// getSourceDoc 来源文档
func (s *Service) getSourceDoc(ctx context.Context, qas []*qaEntity.DocQA, robotID uint64) (
	map[uint64]*docEntity.Doc, error) {
	docIDs := make([]uint64, 0, len(qas))
	for _, qa := range qas {
		if qa.DocID == 0 {
			continue
		}
		docIDs = append(docIDs, qa.DocID)
	}
	return s.docLogic.GetDocByIDs(ctx, slicex.Unique(docIDs), robotID)
}

func (s *Service) getDeletingDocID(ctx context.Context, corpID, robotID uint64) ([]uint64, error) {
	docs, err := s.docLogic.GetDeletingDoc(ctx, corpID, robotID)
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
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, robotID)
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
		case qaEntity.QAReleaseStatusExpired:
			validityStatus = qaEntity.QaExpiredStatus
		default:
			releaseStatus = append(releaseStatus, req.GetReleaseStatus()[i])
		}
	}
	// 如果选择了状态，但是没有选择已过期，那就是未过期
	if validityStatus != qaEntity.QaExpiredStatus && len(releaseStatus) > 0 {
		validityStatus = qaEntity.QaUnExpiredStatus
	}
	return validityStatus, releaseStatus, nil
}
