package service

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/mapx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ListQaSimilar 获取相似问答对
func (s *Service) ListQaSimilar(ctx context.Context, req *pb.ListQaSimilarReq) (*pb.ListQaSimilarRsp, error) {
	rsp := new(pb.ListQaSimilarRsp)
	botBizID := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	total, qaSimilarList, err := s.qaLogic.ListQASimilar(ctx, app.CorpPrimaryId, app.PrimaryId, req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	var similarBizIDs []uint64
	for _, similar := range qaSimilarList {
		similarBizIDs = append(similarBizIDs, similar.BusinessID)
	}
	rsp.Total = total
	rsp.SimilarBizIds = similarBizIDs
	return rsp, nil
}

// DescribeQaSimilar 相似问答对详情
func (s *Service) DescribeQaSimilar(ctx context.Context, req *pb.DescribeQaSimilarReq) (*pb.DescribeQaSimilarRsp,
	error) {
	rsp := new(pb.DescribeQaSimilarRsp)
	botBizID := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	qaSimilar, err := s.qaLogic.GetQASimilarBizID(ctx, app.CorpPrimaryId, req.GetSimilarBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	details, err := s.qaLogic.GetQADetails(ctx, app.CorpPrimaryId, app.PrimaryId, []uint64{qaSimilar.QaID, qaSimilar.SimilarID})
	if err != nil {
		return rsp, errs.ErrQANotFound
	}
	var lastDocID uint64
	var fileName, fileType string
	docs := map[uint64]*docEntity.Doc{}
	for _, qa := range details {
		// 文档ID不为0，并且第二个的docID和第一个docID不一样
		if qa.DocID != 0 && qa.DocID != lastDocID {
			docs, err = s.docLogic.GetDocByIDs(ctx, []uint64{qa.DocID}, app.PrimaryId)
			if err != nil {
				return rsp, errs.ErrDocNotFound
			}
			doc, ok := docs[qa.DocID]
			if ok {
				fileName = doc.FileName
				fileType = doc.FileType
			}
		}
		lastDocID = qa.DocID
		rsp.List = append(rsp.List, &pb.DescribeQaSimilarRsp_QA{
			QaBizId:     qa.BusinessID,
			Question:    qa.Question,
			Source:      qa.Source,
			SourceDesc:  i18n.Translate(ctx, qa.SourceDesc(docs)),
			Answer:      qa.Answer,
			UpdateTime:  qa.UpdateTime.Unix(),
			DocBizId:    qa.DocBizID(docs),
			FileName:    fileName,
			FileType:    fileType,
			EnableScope: pb.RetrievalEnableScope(qa.EnableScope),
		})
	}
	return rsp, nil
}

// SubmitQaSimilar 相似文档提交处理结果
func (s *Service) SubmitQaSimilar(ctx context.Context, req *pb.SubmitQaSimilarReq) (*pb.SubmitQaSimilarRsp, error) {
	rsp := new(pb.SubmitQaSimilarRsp)
	staffID := contextx.Metadata(ctx).StaffID()
	botBizID := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if req.GetIsIgnoreAll() {
		if err = s.qaLogic.IgnoreAllQASimilar(ctx, app.CorpPrimaryId, app.PrimaryId); err != nil {
			return rsp, errs.ErrSystem
		}
		return rsp, nil
	}
	// Deprecated
	var qaSimilarIDs, delQaIDs []uint64
	// 保留
	var qaSimilarBizIDs, delQaBizIDs []uint64
	for _, info := range req.GetQaSimilarInfo() {
		if info.GetSimilarId() != 0 {
			qaSimilarIDs = append(qaSimilarIDs, info.GetSimilarId())
		}
		if info.GetSimilarBizId() != 0 {
			qaSimilarBizIDs = append(qaSimilarBizIDs, info.GetSimilarBizId())
		}
		if info.GetIsIgnore() {
			continue
		}
		if info.GetDelQaId() != 0 {
			delQaIDs = append(delQaIDs, info.GetDelQaId())
		}
		if info.GetDelQaBizId() != 0 {
			delQaBizIDs = append(delQaBizIDs, info.GetDelQaBizId())
		}
	}
	delQaIDs = slicex.Unique(delQaIDs)
	delQas := make([]*qa.DocQA, 0)

	if len(delQaIDs) != 0 {
		// Deprecated
		qas, err := s.qaLogic.GetQADetails(ctx, app.CorpPrimaryId, app.PrimaryId, delQaIDs)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
		delQas = mapx.Values(qas)
	} else if len(delQaBizIDs) != 0 {
		// 保留
		qas, err := s.qaLogic.GetQADetailsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, delQaBizIDs)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
		delQas = mapx.Values(qas)
	}
	if len(qaSimilarIDs) != 0 {
		if err = s.qaLogic.DoQASimilar(ctx, app.CorpPrimaryId, app.PrimaryId, staffID, qaSimilarIDs, delQas); err != nil {
			return rsp, errs.ErrSystem
		}
	} else if len(qaSimilarBizIDs) != 0 {
		// 保留
		if err = s.qaLogic.DoQABizSimilar(ctx, app.CorpPrimaryId, app.PrimaryId, staffID, qaSimilarBizIDs, delQas); err != nil {
			return rsp, errs.ErrSystem
		}
	}
	return rsp, nil
}
