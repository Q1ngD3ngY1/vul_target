package api

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetDocs 获取文档内容
func (s *Service) GetDocs(ctx context.Context, req *pb.GetDocsReq) (*pb.GetDocsRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetDocsRsp)
	return rsp, nil
}

// DeleteDocSegmentImages 删除文档切片图片 -- 仅vector服务调用
func (s *Service) DeleteDocSegmentImages(ctx context.Context, req *pb.DeleteDocSegmentImagesReq) (
	rsp *pb.DeleteDocSegmentImagesRsp, err error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)

	log.InfoContextf(ctx, "DeleteDocSegmentImages|callesd|req:%+v", req)
	err = s.dao.DeleteSegmentImages(ctx, req.GetRobotId(), req.GetDocIds())
	if err != nil {
		log.ErrorContextf(ctx, "DeleteDocSegmentImages|failed|err:%+v", err)
		return nil, err
	}
	return rsp, nil
}

// InnerDescribeDocs 批量获取文档详情（内部接口）
func (s *Service) InnerDescribeDocs(ctx context.Context, req *pb.InnerDescribeDocsReq) (
	*pb.InnerDescribeDocsRsp, error) {
	log.InfoContextf(ctx, "InnerDescribeDocs Req:%+v", req)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	if len(docBizIDs) > utilConfig.GetMainConfig().BatchInterfaceLimit.GeneralMaxLimit {
		return nil, errs.ErrDescribeDocLimit
	}
	docs, err := s.dao.GetDocByBizIDs(ctx, docBizIDs, app.ID)
	if err != nil || len(docs) == 0 {
		return nil, errs.ErrDocNotFound
	}
	docIDs, cateIDs := make([]uint64, 0, len(docs)), make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
		cateIDs = append(cateIDs, uint64(doc.CategoryID))
	}
	qaNums, err := s.dao.GetDocQANum(ctx, corpID, app.ID, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	pendingDoc, err := s.getPendingDoc(ctx, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.dao.GetDocAttributeLabelDetail(ctx, app.ID, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	cateMap, err := s.dao.GetCateByIDs(ctx, model.DocCate, cateIDs)
	if err != nil {
		return nil, errs.ErrCateNotFound
	}

	docDetails := getDocDetails(docs, qaNums, pendingDoc, latestRelease, mapDocID2AttrLabels, cateMap)
	return &pb.InnerDescribeDocsRsp{Docs: docDetails}, nil
}

// getPendingDoc 获取发布中的文档
func (s *Service) getPendingDoc(ctx context.Context, robotID uint64) (map[uint64]struct{}, error) {
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
	releaseDocs, err := s.dao.GetReleaseDoc(ctx, latestRelease)
	if err != nil {
		return nil, err
	}
	return releaseDocs, nil
}

// getDocDetails 获取文档详情
func getDocDetails(docs map[uint64]*model.Doc, qaNums map[uint64]map[uint32]uint32,
	pendingDoc map[uint64]struct{}, latestRelease *model.Release, mapDocID2AttrLabels map[uint64][]*model.AttrLabel,
	cateMap map[uint64]*model.CateInfo) []*pb.InnerDescribeDocsRsp_DocDetail {
	docDetails := make([]*pb.InnerDescribeDocsRsp_DocDetail, 0)
	for _, doc := range docs {
		_, ok := pendingDoc[doc.ID]
		docDetail := &pb.InnerDescribeDocsRsp_DocDetail{
			DocBizId:       doc.BusinessID,
			FileName:       doc.FileName,
			CosUrl:         doc.CosURL,
			Reason:         doc.Message,
			UpdateTime:     doc.UpdateTime.Unix(),
			Status:         doc.StatusCorrect(),
			StatusDesc:     doc.StatusDesc(latestRelease.IsPublishPause()),
			FileType:       doc.FileType,
			IsRefer:        doc.IsRefer,
			QaNum:          qaNums[doc.ID][model.QAIsNotDeleted],
			IsDeleted:      doc.HasDeleted(),
			Source:         doc.Source,
			SourceDesc:     doc.DocSourceDesc(),
			IsAllowRestart: !ok && doc.IsAllowCreateQA(),
			IsDeletedQa:    qaNums[doc.ID][model.QAIsNotDeleted] == 0 && qaNums[doc.ID][model.QAIsDeleted] != 0,
			IsCreatingQa:   doc.IsCreatingQaV1(),
			IsAllowDelete:  !ok && doc.IsAllowDelete(),
			IsAllowRefer:   doc.IsAllowRefer(),
			IsCreatedQa:    doc.IsCreatedQA,
			DocCharSize:    doc.CharSize,
			IsAllowEdit:    !ok && doc.IsAllowEdit(),
			AttrRange:      doc.AttrRange,
			AttrLabels:     fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
			DocId:          doc.ID,
		}
		if cate, ok := cateMap[uint64(doc.CategoryID)]; ok {
			if cate != nil {
				docDetail.CateBizId = cate.BusinessID
			}
		}
		docDetails = append(docDetails, docDetail)
	}
	return docDetails
}

// fillPBAttrLabels 转成成PB的属性标签
func fillPBAttrLabels(attrLabels []*model.AttrLabel) []*pb.AttrLabel {
	list := make([]*pb.AttrLabel, 0)
	for _, v := range attrLabels {
		attrLabel := &pb.AttrLabel{
			Source:    v.Source,
			AttrBizId: v.BusinessID,
			AttrKey:   v.AttrKey,
			AttrName:  v.AttrName,
		}
		for _, label := range v.Labels {
			labelName := label.LabelName
			if label.LabelID == 0 {
				labelName = config.App().AttributeLabel.FullLabelDesc
			}
			attrLabel.Labels = append(attrLabel.Labels, &pb.AttrLabel_Label{
				LabelBizId: label.BusinessID,
				LabelName:  labelName,
			})
		}
		list = append(list, attrLabel)
	}
	return list
}
