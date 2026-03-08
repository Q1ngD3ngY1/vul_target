package api

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	attrEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// InnerDescribeDocs 批量获取文档详情（内部接口）
func (s *Service) InnerDescribeDocs(ctx context.Context, req *pb.InnerDescribeDocsReq) (*pb.InnerDescribeDocsRsp, error) {
	logx.I(ctx, "InnerDescribeDocs Req:%+v", req)
	app, err := s.svc.DescribeAppByScene(ctx, req.GetBotBizId(), entity.AppTestScenes)
	if err != nil {
		return nil, err
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	if len(docBizIDs) > config.GetMainConfig().BatchInterfaceLimit.GeneralMaxLimit {
		return nil, errs.ErrDescribeDocLimit
	}
	docs, err := s.docLogic.GetDocByBizIDs(ctx, docBizIDs, app.PrimaryId)
	if err != nil || len(docs) == 0 {
		return nil, errs.ErrDocNotFound
	}
	docIDs, cateIDs := make([]uint64, 0, len(docs)), make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
		cateIDs = append(cateIDs, uint64(doc.CategoryID))
	}
	qaNums, err := s.qaLogic.GetDocQANum(ctx, app.CorpPrimaryId, app.PrimaryId, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	pendingDoc, err := s.getPendingDoc(ctx, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.labelLogic.GetDocAttributeLabelDetail(ctx, app.PrimaryId, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	cateMap, err := s.cateLogic.DescribeCateByIDs(ctx, cateEntity.DocCate, cateIDs)
	if err != nil {
		return nil, errs.ErrCateNotFound
	}

	docDetails := getDocDetails(docs, qaNums, pendingDoc, latestRelease, mapDocID2AttrLabels, cateMap)
	return &pb.InnerDescribeDocsRsp{Docs: docDetails}, nil
}

// getPendingDoc 获取发布中的文档
func (s *Service) getPendingDoc(ctx context.Context, robotID uint64) (map[uint64]struct{}, error) {
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
	releaseDocs, err := s.releaseLogic.GetReleaseDoc(ctx, latestRelease)
	if err != nil {
		return nil, err
	}
	return releaseDocs, nil
}

// getDocDetails 获取文档详情
func getDocDetails(docs map[uint64]*docEntity.Doc, qaNums map[uint64]map[uint32]uint32,
	pendingDoc map[uint64]struct{}, latestRelease *releaseEntity.Release,
	mapDocID2AttrLabels map[uint64][]*attrEntity.AttrLabel,
	cateMap map[uint64]*cateEntity.CateInfo) []*pb.InnerDescribeDocsRsp_DocDetail {
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
			QaNum:          qaNums[doc.ID][qaEntity.QAIsNotDeleted],
			IsDeleted:      doc.HasDeleted(),
			Source:         doc.Source,
			SourceDesc:     doc.DocSourceDesc(),
			IsAllowRestart: !ok && doc.IsAllowCreateQA(),
			IsDeletedQa:    qaNums[doc.ID][qaEntity.QAIsNotDeleted] == 0 && qaNums[doc.ID][qaEntity.QAIsDeleted] != 0,
			IsCreatingQa:   doc.IsCreatingQaV1(),
			IsAllowDelete:  !ok && doc.IsAllowDelete(),
			IsAllowRefer:   doc.IsAllowRefer(),
			IsCreatedQa:    doc.IsCreatedQA,
			DocCharSize:    doc.CharSize,
			IsAllowEdit:    !ok && doc.IsAllowEdit(),
			AttrRange:      doc.AttrRange,
			AttrLabels:     fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
			DocId:          doc.ID,
			FileSize:       doc.FileSize,
			CosHash:        doc.CosHash,
			WebUrl:         doc.WebURL,
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
func fillPBAttrLabels(attrLabels []*attrEntity.AttrLabel) []*pb.AttrLabel {
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
