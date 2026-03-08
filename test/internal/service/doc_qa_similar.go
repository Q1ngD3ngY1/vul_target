package service

import (
	"context"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/mapx"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ListQaSimilar 获取相似问答对
func (s *Service) ListQaSimilar(ctx context.Context, req *pb.ListQaSimilarReq) (*pb.ListQaSimilarRsp, error) {
	rsp := new(pb.ListQaSimilarRsp)
	corpID := pkg.CorpID(ctx)
	var err error
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	total, qaSimilarList, err := s.dao.ListQASimilar(ctx, corpID, app.ID, req.GetPageNumber(), req.GetPageSize())
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
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	qaSimilar, err := s.dao.GetQASimilarBizID(ctx, corpID, req.GetSimilarBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	details, err := s.dao.GetQADetails(ctx, corpID, app.ID, []uint64{qaSimilar.QaID, qaSimilar.SimilarID})
	if err != nil {
		return rsp, errs.ErrQANotFound
	}
	var lastDocID uint64
	var fileName, fileType string
	docs := map[uint64]*model.Doc{}
	for _, qa := range details {
		// 文档ID不为0，并且第二个的docID和第一个docID不一样
		if qa.DocID != 0 && qa.DocID != lastDocID {
			docs, err = s.dao.GetDocByIDs(ctx, []uint64{qa.DocID}, app.ID)
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
			QaBizId:    qa.BusinessID,
			Question:   qa.Question,
			Source:     qa.Source,
			SourceDesc: i18n.Translate(ctx, qa.SourceDesc(docs)),
			Answer:     qa.Answer,
			UpdateTime: qa.UpdateTime.Unix(),
			DocBizId:   qa.DocBizID(docs),
			FileName:   fileName,
			FileType:   fileType,
		})
	}
	return rsp, nil
}

// SubmitQaSimilar 相似文档提交处理结果
func (s *Service) SubmitQaSimilar(ctx context.Context, req *pb.SubmitQaSimilarReq) (*pb.SubmitQaSimilarRsp, error) {
	rsp := new(pb.SubmitQaSimilarRsp)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if req.GetIsIgnoreAll() {
		if err = s.dao.IgnoreAllQASimilar(ctx, corpID, app.ID); err != nil {
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
	delQas := make([]*model.DocQA, 0)

	if len(delQaIDs) != 0 {
		// Deprecated
		qas, err := s.dao.GetQADetails(ctx, corpID, app.ID, delQaIDs)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
		delQas = mapx.Values(qas)
	} else if len(delQaBizIDs) != 0 {
		// 保留
		qas, err := s.dao.GetQADetailsByBizIDs(ctx, corpID, app.ID, delQaBizIDs)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
		delQas = mapx.Values(qas)
	}
	if len(qaSimilarIDs) != 0 {
		// Deprecated
		if err = s.dao.DoQASimilar(ctx, corpID, app.ID, staffID, qaSimilarIDs, delQas); err != nil {
			return rsp, errs.ErrSystem
		}
	} else if len(qaSimilarBizIDs) != 0 {
		// 保留
		if err = s.dao.DoQABizSimilar(ctx, corpID, app.ID, staffID, qaSimilarBizIDs, delQas); err != nil {
			return rsp, errs.ErrSystem
		}
	}
	return rsp, nil
}
