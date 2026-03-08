package doc_segment

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/go-comm/clues"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

// DescribeSegments 获取文档分段信息
func DescribeSegments(ctx context.Context, appBizID uint64, segBizIDs []uint64) (*pb.DescribeSegmentsRsp, error) {
	rsp := &pb.DescribeSegmentsRsp{}
	segments, err := GetDocSegmentList(ctx, appBizID, segBizIDs)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return rsp, nil
	}
	// 获取切片的文档信息
	var docIDs []uint64
	for i := range segments {
		docIDs = append(docIDs, segments[i].DocID)
	}
	docIDMap := make(map[uint64]*model.Doc)
	if len(docIDs) != 0 {
		docFilter := &dao.DocFilter{
			RouterAppBizID: appBizID,
			CorpId:         pkg.CorpID(ctx),
			IDs:            docIDs,
		}
		selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId, dao.DocTblColCosURL, dao.DocTblColWebURL}
		docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, docFilter)
		if err != nil {
			return nil, err
		}
		for _, doc := range docs {
			docIDMap[doc.ID] = doc
		}
	}

	// 获取切片的文档页码信息
	docSegmentIDs := make([]uint64, 0, len(segments))
	for _, segment := range segments {
		docSegmentIDs = append(docSegmentIDs, segment.ID)
	}
	segmentPageInfoMap := make(map[uint64]*model.DocSegmentPageInfo)
	log.DebugContextf(ctx, "DescribeSegments docSegmentIDs:%+v", docSegmentIDs)
	if len(docSegmentIDs) > 0 {
		filter := &dao.DocSegmentPageInfoFilter{
			RouterAppBizId: appBizID,
			SegmentIDs:     docSegmentIDs,
			Limit:          uint32(len(docSegmentIDs)),
		}
		selectColumns := []string{dao.DocSegmentPageInfoTblSegmentID, dao.DocSegmentPageInfoTblOrgPageNumbers}
		segmentPageInfos, err := dao.GetDocSegmentPageInfoDao().GetDocSegmentPageInfoList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocSegmentPageInfoList error: %v", err)
			return rsp, err
		}
		log.DebugContextf(ctx, "DescribeSegments GetDocSegmentPageInfoList len(segmentPageInfos): %+v",
			len(segmentPageInfos))
		for _, segmentPageInfo := range segmentPageInfos {
			segmentPageInfoMap[segmentPageInfo.SegmentID] = segmentPageInfo
		}
	}
	pbSegments := DocSegmentDb2Pb(ctx, segments, docIDMap, segmentPageInfoMap)
	rsp.List = pbSegments

	clues.AddT(ctx, "DescribeSegmentsRsp", rsp)
	return rsp, nil
}

// GetDocSegmentList 获取文档分段信息，兼容2.9.0之前旧文档的org data同时存在于t_doc_segment和t_doc_segment_org_data表中
func GetDocSegmentList(ctx context.Context, appBizID uint64, segBizIDs []uint64) ([]*model.DocSegment, error) {
	if len(segBizIDs) == 0 {
		return []*model.DocSegment{}, nil
	}
	filter := &dao.DocSegmentFilter{
		RouterAppBizID: appBizID,
	}
	if segBizIDs[0] < dao.MinBizID {
		// 对外接口，需要兼容之前接收自增id的场景
		filter.IDs = segBizIDs
	} else {
		filter.BusinessIDs = segBizIDs
	}
	selectColumns := []string{dao.DocSegmentTblColID, dao.DocSegmentTblColCorpID, dao.DocSegmentTblColBusinessID,
		dao.DocSegmentTblColDocId, dao.DocSegmentTblColFileType, dao.DocSegmentTblColSegmentType,
		dao.DocSegmentTblColTitle, dao.DocSegmentTblColPageContent, dao.DocSegmentTblColOrgData,
		dao.DocSegmentTblColOrgDataBizID}
	segments, err := dao.GetDocSegmentDao().GetDocSegmentList(ctx, selectColumns, filter)
	clues.AddTrackE(ctx, "dao.GetSegmentByIDs", segments, err)
	if err != nil {
		return segments, err
	}

	for _, seg := range segments {
		// 防越权校验，为了兼容入参中的segmentID为共享知识库的情况
		if seg.CorpID != pkg.CorpID(ctx) {
			return segments, errs.ErrSegmentNotFound
		}
	}

	// 获取分段信息
	orgDataBizIDs := make([]uint64, 0)
	for _, seg := range segments {
		if seg == nil {
			log.WarnContextf(ctx, "GetDocSegmentList seg is nil")
			continue
		}
		if seg.OrgData == "" {
			// 2.9.0之前旧文档的org data同时存在于t_doc_segment和t_doc_segment_org_data表中
			// 2.9.0之后的新文档org data只存在t_doc_segment_org_data表中，需要再查一次t_doc_segment_org_data
			orgDataBizIDs = append(orgDataBizIDs, seg.OrgDataBizID)
		}
	}
	if len(orgDataBizIDs) != 0 {
		orgDataBizIDs = slicex.Unique(orgDataBizIDs)
		log.DebugContextf(ctx, "DescribeSegments orgDataBizIDs:%+v", orgDataBizIDs)
		selectColumns := []string{dao.DocSegmentOrgDataTblColBusinessID, dao.DocSegmentOrgDataTblColOrgData}
		filter := &dao.DocSegmentOrgDataFilter{
			CorpBizID: pkg.CorpBizID(ctx),
			//AppBizID:    appBizID, // 不能打开，需要兼容入参中的segmentID为共享知识库的情况
			BusinessIDs:    orgDataBizIDs,
			Offset:         0,
			Limit:          uint32(len(orgDataBizIDs)),
			RouterAppBizID: appBizID,
		}
		docSegmentOrgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataList(ctx, selectColumns, filter)
		if err != nil {
			return segments, err
		}
		//log.DebugContextf(ctx, "DescribeSegments docSegmentOrgData:%+v", docSegmentOrgData)
		orgDataBizIDMap := make(map[uint64]string)
		for _, orgData := range docSegmentOrgData {
			orgDataBizIDMap[orgData.BusinessID] = orgData.OrgData
		}
		for i := range segments {
			if segments[i].OrgData == "" {
				if orgData, ok := orgDataBizIDMap[segments[i].OrgDataBizID]; ok {
					segments[i].OrgData = orgData
				}
			}
		}
	}
	return segments, nil
}

// DocSegmentDb2Pb 将数据库中的DocSegment转换为pb格式
func DocSegmentDb2Pb(ctx context.Context, segments []*model.DocSegment, docIDMap map[uint64]*model.Doc,
	segmentPageInfoMap map[uint64]*model.DocSegmentPageInfo) []*pb.DescribeSegmentsRsp_Segment {
	pbSegments := make([]*pb.DescribeSegmentsRsp_Segment, 0, len(segments))
	for _, segment := range segments {
		pbSegment := &pb.DescribeSegmentsRsp_Segment{
			Id:          segment.ID,
			BusinessId:  segment.BusinessID,
			DocId:       segment.DocID,
			FileType:    segment.FileType,
			SegmentType: segment.SegmentType,
			Title:       segment.Title,
			PageContent: segment.PageContent,
			OrgData:     segment.OrgData,
		}
		if doc, ok := docIDMap[pbSegment.DocId]; ok {
			pbSegment.DocBizId = doc.BusinessID
			pbSegment.DocUrl = doc.CosURL
			pbSegment.WebUrl = doc.WebURL
		}
		if segmentPageInfo, ok := segmentPageInfoMap[segment.ID]; ok {
			if len(segmentPageInfo.OrgPageNumbers) != 0 {
				pageInfos, pageData := make([]uint32, 0), make([]int32, 0)
				if err := jsoniter.UnmarshalFromString(segmentPageInfo.OrgPageNumbers, &pageData); err != nil {
					log.WarnContextf(ctx, "DocSegmentDb2Pb UnmarshalFromString err:%+v", err)
				}
				for _, page := range pageData {
					pageInfos = append(pageInfos, uint32(page))
				}
				pbSegment.PageInfos = pageInfos
			}
		}

		pbSegments = append(pbSegments, pbSegment)
	}
	return pbSegments
}
