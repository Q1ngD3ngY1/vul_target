package api

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

var appealType2AuditType = map[uint32]uint32{
	entity.AppealBizTypeDoc:           releaseEntity.AuditBizTypeDoc,
	entity.AppealBizTypeQa:            releaseEntity.AuditBizTypeQa,
	entity.AppealBizTypeDocName:       releaseEntity.AuditBizTypeDocName,
	entity.AppealBizTypeDocSegment:    releaseEntity.AuditBizTypeDocSegment,
	entity.AppealBizTypeDocTableSheet: releaseEntity.AuditBizTypeDocTableSheet,
}

func (s *Service) UpdateAppealQA(ctx context.Context, req *pb.UpdateAppealQAReq) (*pb.UpdateAppealQARsp, error) {
	rsp := new(pb.UpdateAppealQARsp)
	logx.I(ctx, "UpdateAppealQA Req:%+v", req)
	err := s.qaDao.UpdateAppealQA(ctx, req.GetQaIds(), req.GetSimIds(), req.GetReleaseStatus(), req.GetIsAuditFree())
	if err != nil {
		logx.E(ctx, "UpdateAppealQA fail, req:%+v, err:%v", req, err)
		return nil, err
	}
	return rsp, nil
}

func (s *Service) UpdateAppealDoc(ctx context.Context, req *pb.UpdateAppealDocReq) (*pb.UpdateAppealDocRsp, error) {
	rsp := new(pb.UpdateAppealDocRsp)
	logx.I(ctx, "UpdateAppealDoc Req:%+v", req)
	err := s.docLogic.UpdateDocStatusByRobotId(ctx, req.GetDocId(), req.GetRobotId(), req.GetStatus())
	if err != nil {
		logx.E(ctx, "UpdateAppealDoc fail, req:%+v, err:%v", req, err)
		return nil, err
	}
	return rsp, nil

}
func (s *Service) GetAppealInfo(ctx context.Context, req *pb.GetAppealInfoReq) (*pb.GetAppealInfoRsp, error) {
	logx.I(ctx, "GetAppealInfo Req:%+v", req)
	rsp := new(pb.GetAppealInfoRsp)
	var err error
	var docQA *qaEntity.DocQA
	var similarQuestion map[uint64][]*qaEntity.SimilarQuestion
	var doc *docEntity.Doc
	switch req.GetQueryType() {
	case pb.QueryType_QUERY_TYPE_QA_BY_BIZ_ID:
		docQA, err = s.qaLogic.GetQAByBizID(ctx, req.GetQaParams().GetBizId())
		rsp.QaInfo = docQAToPbQAInfo(docQA)
	case pb.QueryType_QUERY_TYPE_QA_BY_ID:
		docQA, err = s.qaLogic.GetQAByID(ctx, req.GetQaParams().GetQaId())
		rsp.QaInfo = docQAToPbQAInfo(docQA)
	case pb.QueryType_QUERY_TYPE_DOC:
		doc, err = s.docLogic.GetDocByBizID(ctx, req.GetDocParams().GetDocBizId(), req.GetDocParams().GetRobotId())
		rsp.Doc = DocToPb(doc)
	case pb.QueryType_QUERY_TYPE_SIMILAR_QA:
		params := req.GetSimilarQaParams()
		similarQuestion, err = s.qaLogic.GetSimilarQAMap(ctx, params.GetCorpId(), params.RobotId, []uint64{params.GetQaId()})
		rsp.SimilarQaInfo = &pb.SimilarQAInfo{
			SimilarQaMap: map[uint64]*pb.SimilarQAList{},
		}
		rsp.SimilarQaInfo.SimilarQaMap = similarQuestionMapToDocSimilarQAMap(similarQuestion)
	}
	if err != nil {
		logx.W(ctx, "GetAppealInfo fail, req:%+v, err:%v", req, err)
		return nil, err
	}
	return rsp, nil
}

// AppealCallback 申诉回调接口
// 同一个qa的问答文本、答案中的图片、答案中的视频、相似问，前端是整体申诉的，但是后端却是分开申诉的。
// 每个子申诉任务完成后，OP服务会回调本接口。任意一个子申诉的结果为不通过，则整个申诉任务就会失败，前端就会展示问答申诉失败。
// 所以问答申诉失败后，OP那边可能还有未处理的申诉子任务。
// 申诉失败后，用户可以编辑问答，然后走机器审核。如果机器审核不通过，用户可以再次申诉。此时未处理完的旧申诉和新申诉并存，
// 所以在处理申诉前，需要判断当前回调是否是最新的申诉任务。
// NOTE(ericjwang): 有零星调用， bot-op-server:/opapi/appeal/update --> here
func (s *Service) AppealCallback(ctx context.Context, req *pb.AppealCallbackReq) (*pb.AppealCallbackRsp, error) {
	rsp := new(pb.AppealCallbackRsp)
	appealType := req.GetAppealType()
	appealID := req.GetAppealId()

	if appealType != entity.AppealBizTypeQa &&
		appealType != entity.AppealBizTypeDoc &&
		appealType != entity.AppealBizTypeDocName &&
		appealType != entity.AppealBizTypeDocSegment &&
		appealType != entity.AppealBizTypeDocTableSheet { // 这里是判断申诉类型
		logx.E(ctx, "申诉类型不匹配，appealType:%d, req:%+v", appealType, req)
		return rsp, errs.ErrParams
	}
	var err error
	key := fmt.Sprintf(dao.LockForAuditAppeal, appealID)
	if err = s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		return rsp, errs.ErrAppealCallbackDoing
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()

	auditType, _ := appealType2AuditType[appealType]
	audit := s.fillAudit(req, auditType)
	logx.I(ctx, "AppealCallback audit:%+v, req:%+v", audit, req)
	// 1. 判断当前回调是否是最新的申诉任务。这里相当于也进行了参数校验
	lastParentAudit, err := s.auditLogic.GetLatestParentAuditFailByRelateID(ctx, req.GetCorpId(), req.GetRobotId(),
		req.GetRelateId(), auditType)
	if err != nil {
		logx.E(ctx, "GetLatestParentAuditFailByRelateID fail，appealType:%d, audit:%+v, err:%v", appealType, audit, err)
		return rsp, err
	}
	if lastParentAudit.ID != req.GetAuditParentId() && lastParentAudit.Status != releaseEntity.AuditStatusAppealIng {
		logx.I(ctx, "当前回调的父审核id不是最新的，或者没有处于人工申诉中，忽略本次申诉回调请求。 lastParentAuditID:%d, req:%+v", lastParentAudit.ID, req)
		return rsp, nil
	}

	// 2. 更新t_audit表对应的子审核状态
	if err = s.auditLogic.UpdateAuditStatus(ctx, audit); err != nil {
		logx.E(ctx, "UpdateAuditStatus fail，appealType:%d, audit:%+v, err:%v",
			appealType, audit, err)
		return rsp, errs.ErrSystem
	}
	// 3. 获取该parentID下的所有子审核状态
	auditStatus, err := s.auditLogic.GetBizAuditStatusStat(ctx, req.GetAuditParentId(), req.GetCorpId(), req.GetRobotId())
	if err != nil {
		logx.E(ctx, "GetBizAuditStatusStat fail，appealType:%d, audit:%+v, err:%v", appealType, audit, err)
		return rsp, errs.ErrSystem
	}

	allPass := true
	for k := range auditStatus {
		if k != releaseEntity.AuditStatusPass && k != releaseEntity.AuditStatusAppealSuccess {
			allPass = false
		}
	}
	// 4. 如果是都是通过，就把对应的qa&相似问或者文档状态改成审核通过
	parentAudit := s.fillParentAudit(req, auditType)
	if allPass {
		if auditType == releaseEntity.AuditBizTypeQa { // 这里是判断审核类型
			err = s.auditLogic.AuditQa(ctx, parentAudit, true, true, "")
		} else if auditType == releaseEntity.AuditBizTypeDocName {
			err = s.auditLogic.AuditDocName(ctx, parentAudit, true, true, "")
		} else { // 干预也在AuditDoc中处理
			err = s.auditLogic.AuditDoc(ctx, parentAudit, true, true, "")
		}
		if err != nil {
			logx.E(ctx, "allPass is true, Audit qa or doc fail，appealType:%d, parentAudit:%+v, err:%v", appealType, parentAudit, err)
			return rsp, err
		}
	}
	// 5. 如果有子审核失败，就把对应的qa&相似问或者文档状态改成审核失败
	if !req.GetIsPass() {
		if auditType == releaseEntity.AuditBizTypeQa { // 这里是判断审核类型
			err = s.auditLogic.AuditQa(ctx, parentAudit, false, true, req.GetAppealRejectReason())
		} else if auditType == releaseEntity.AuditBizTypeDocName {
			err = s.auditLogic.AuditDocName(ctx, parentAudit, false, true, req.GetAppealRejectReason())
		} else {
			err = s.auditLogic.AuditDoc(ctx, parentAudit, false, true, req.GetAppealRejectReason())
		}
		if err != nil {
			logx.E(ctx, "hasAppealFail is true, Audit qa or doc fail，appealType:%d, parentAudit:%+v, err:%v", appealType, parentAudit, err)
			return rsp, err
		}
	}
	logx.I(ctx, "Audit qa or doc ok，appealType:%d, audit:%+v", appealType, audit)
	return rsp, nil
}

// fillAudit .
func (s *Service) fillAudit(req *pb.AppealCallbackReq, auditType uint32) *releaseEntity.Audit {
	audit := &releaseEntity.Audit{}
	audit.ID = req.GetAuditId()
	audit.UpdateTime = time.Now()
	audit.CorpID = req.GetCorpId()
	audit.RobotID = req.GetRobotId()
	audit.RelateID = req.GetRelateId()
	audit.CreateStaffID = req.GetCreateStaffId()
	audit.ParentID = req.GetAuditParentId()
	audit.Type = auditType
	if req.GetIsPass() {
		audit.Status = releaseEntity.AuditStatusAppealSuccess
	} else {
		audit.Status = releaseEntity.AuditStatusAppealFail
	}
	return audit
}

// fillParentAudit .
func (s *Service) fillParentAudit(req *pb.AppealCallbackReq, auditType uint32) *releaseEntity.Audit {
	audit := &releaseEntity.Audit{}
	audit.ID = req.GetAuditParentId()
	audit.UpdateTime = time.Now()
	audit.CorpID = req.GetCorpId()
	audit.RobotID = req.GetRobotId()
	audit.RelateID = req.GetRelateId()
	audit.CreateStaffID = req.GetCreateStaffId()
	audit.ParentID = 0
	audit.Type = auditType
	return audit
}

func docQAToPbQAInfo(docQA *qaEntity.DocQA) *pb.QAInfo {
	if docQA == nil {
		return nil
	}
	return &pb.QAInfo{
		Id:            docQA.ID,
		BusinessId:    docQA.BusinessID,
		RobotId:       docQA.RobotID,
		CorpId:        docQA.CorpID,
		StaffId:       docQA.StaffID,
		DocId:         docQA.DocID,
		OriginDocId:   docQA.OriginDocID,
		SegmentId:     docQA.SegmentID,
		CategoryId:    docQA.CategoryID,
		Source:        docQA.Source,
		Question:      docQA.Question,
		Answer:        docQA.Answer,
		CustomParam:   docQA.CustomParam,
		QuestionDesc:  docQA.QuestionDesc,
		ReleaseStatus: docQA.ReleaseStatus,
		IsAuditFree:   docQA.IsAuditFree,
		IsDeleted:     int32(docQA.IsDeleted),
		Message:       docQA.Message,
		AcceptStatus:  docQA.AcceptStatus,
		SimilarStatus: docQA.SimilarStatus,
		NextAction:    docQA.NextAction,
		CharSize:      docQA.CharSize,
		AttrRange:     docQA.AttrRange,
		CreateTime:    docQA.CreateTime.Unix(),
		UpdateTime:    docQA.UpdateTime.Unix(),
		ExpireStart:   docQA.ExpireStart.Unix(),
		ExpireEnd:     docQA.ExpireEnd.Unix(),
	}
}

func similarQuestionMapToDocSimilarQAMap(sqMap map[uint64][]*qaEntity.SimilarQuestion) map[uint64]*pb.SimilarQAList {
	if len(sqMap) == 0 {
		return nil
	}
	result := make(map[uint64]*pb.SimilarQAList, len(sqMap))
	for qaID, similarQuestions := range sqMap {
		if len(similarQuestions) == 0 {
			continue
		}
		docSimilarQAs := make([]*pb.DocSimilarQA, 0, len(similarQuestions))
		for _, sq := range similarQuestions {
			if docSQ := SimilarQuestionToPbDocSimilarQA(sq); docSQ != nil {
				docSimilarQAs = append(docSimilarQAs, docSQ)
			}
		}
		if len(docSimilarQAs) > 0 {
			result[qaID] = &pb.SimilarQAList{
				SimilarQas: docSimilarQAs,
			}
		}
	}
	return result
}

// SimilarQuestionToPbDocSimilarQA 将SimilarQuestion结构体转换为DocSimilarQA proto消息
func SimilarQuestionToPbDocSimilarQA(sq *qaEntity.SimilarQuestion) *pb.DocSimilarQA {
	if sq == nil {
		return nil
	}
	return &pb.DocSimilarQA{
		Id:            sq.ID,
		SimilarId:     sq.SimilarID,
		RobotId:       sq.RobotID,
		CorpId:        sq.CorpID,
		StaffId:       sq.StaffID,
		RelatedQaId:   sq.RelatedQAID,
		Source:        sq.Source,
		Question:      sq.Question,
		Message:       sq.Message,
		IsDeleted:     int32(sq.IsDeleted),
		ReleaseStatus: sq.ReleaseStatus,
		IsAuditFree:   sq.IsAuditFree,
		NextAction:    sq.NextAction,
		CreateTime:    sq.CreateTime.Unix(),
		UpdateTime:    sq.UpdateTime.Unix(),
		CharSize:      sq.CharSize,
	}
}

// DocToPb 将Doc结构体转换为Doc proto消息
func DocToPb(doc *docEntity.Doc) *pb.Doc {
	if doc == nil {
		return nil
	}
	var isDeleted uint32
	if doc.IsDeleted {
		isDeleted = 1
	}
	return &pb.Doc{
		Id:                  doc.ID,
		BusinessId:          doc.BusinessID,
		RobotId:             doc.RobotID,
		CorpId:              doc.CorpID,
		StaffId:             doc.StaffID,
		FileName:            doc.FileName,
		FileNameInAudit:     doc.FileNameInAudit,
		FileType:            doc.FileType,
		FileSize:            doc.FileSize,
		Bucket:              doc.Bucket,
		CosUrl:              doc.CosURL,
		CosHash:             doc.CosHash,
		Message:             doc.Message,
		Status:              doc.Status,
		IsDeleted:           isDeleted,
		IsRefer:             doc.IsRefer,
		Source:              doc.Source,
		WebUrl:              doc.WebURL,
		BatchId:             int32(doc.BatchID),
		AuditFlag:           doc.AuditFlag,
		CharSize:            doc.CharSize,
		IsCreatingQa:        doc.IsCreatingQA,
		IsCreatedQa:         doc.IsCreatedQA,
		IsCreatingIndex:     doc.IsCreatingIndex,
		NextAction:          doc.NextAction,
		AttrRange:           doc.AttrRange,
		ReferUrlType:        doc.ReferURLType,
		CreateTime:          doc.CreateTime.Unix(),
		UpdateTime:          doc.UpdateTime.Unix(),
		ExpireStart:         doc.ExpireStart.Unix(),
		ExpireEnd:           doc.ExpireEnd.Unix(),
		Opt:                 doc.Opt,
		CategoryId:          uint64(doc.CategoryID),
		OriginalUrl:         doc.OriginalURL,
		ProcessingFlag:      doc.ProcessingFlag,
		CustomerKnowledgeId: doc.CustomerKnowledgeId,
		AttributeFlag:       doc.AttributeFlag,
		IsDownloadable:      doc.IsDownloadable,
	}
}
