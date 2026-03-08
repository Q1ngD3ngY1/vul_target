package service

import (
	"context"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// DescribeRefer 获取来源详情
func (s *Service) DescribeRefer(ctx context.Context, req *pb.DescribeReferReq) (*pb.DescribeReferRsp, error) {
	rsp := new(pb.DescribeReferRsp)
	logx.I(ctx, "DescribeRefer Req:%+v", req)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, botBizID)
	// app, err := s.dao.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	// threshold := config.App().HighLightThreshold

	referBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetReferBizIds())
	if err != nil {
		return nil, err
	}
	refers, err := s.docLogic.GetRefersByBusinessIDs(ctx, app.PrimaryId, referBizIDs)
	if err != nil {
		return rsp, errs.ErrGetReferFail
	}
	if len(refers) == 0 {
		return rsp, nil
	}
	docIDs := make([]uint64, 0, len(refers))
	for _, refer := range refers {
		docIDs = append(docIDs, refer.DocID)
	}
	docIDs = slicex.Unique(docIDs)
	docs, err := s.docLogic.GetDocByIDs(ctx, docIDs, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrGetReferFail
	}
	var corpID uint64
	appIDs := make([]uint64, 0, len(refers))
	for _, doc := range docs {
		appIDs = append(appIDs, doc.RobotID)
		if corpID == 0 {
			corpID = doc.CorpID // 这里需要使用doc的企业id
		}
	}
	appID2AppBizIDMap, err := s.cacheLogic.GetAppBizIdsByPrimaryIds(ctx, corpID, appIDs)
	if err != nil {
		logx.E(ctx, "DescribeRefer|GetAppBizIdsByPrimaryIds|err:%+v", err)
		return rsp, err
	}
	rsp.List = make([]*pb.DescribeReferRsp_ReferDetail, 0, len(refers))
	// var highlightRes []*pb.Highlight
	for _, refer := range refers {
		var docBizID uint64
		var docName string
		var docAppID uint64
		if doc, ok := docs[refer.DocID]; ok {
			// QA类型且文档未开启引用时，仅记录日志不获取业务ID和文件名
			if refer.DocType == entity.ReferTypeQA && !doc.IsReferOpen() {
				logx.I(ctx, "DescribeRefer|!IsReferOpen|docID|%d|referID|%d",
					doc.BusinessID, refer.BusinessID)
			} else {
				// 非QA类型或文档已开启引用时，正常获取业务ID和文件名
				docBizID = doc.BusinessID
				docName = doc.FileName
				docAppID = doc.RobotID
			}
		}
		pageInfos, pageData := make([]uint32, 0), make([]int32, 0)
		if len(refer.PageInfos) != 0 {
			if err = jsonx.UnmarshalFromString(refer.PageInfos, &pageData); err != nil {
				logx.W(ctx, "DescribeRefer|PageInfos|UnmarshalFromString err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint32(page))
			}
		}
		sheetInfos, sheetData := make([]string, 0), make([]*pb.PageContent_SheetData, 0)
		if len(refer.SheetInfos) != 0 {
			if err = jsonx.UnmarshalFromString(refer.SheetInfos, &sheetData); err != nil {
				logx.W(ctx, "DescribeRefer|SheetInfos|UnmarshalFromString err:%+v", err)
			}
			for _, sheet := range sheetData {
				sheetInfos = append(sheetInfos, sheet.SheetName)
			}
		}
		// highlightRes = model.HighlightRefer(ctx, refer.Answer, refer.OrgData, threshold)
		knowledgeBizID, ok := appID2AppBizIDMap[docAppID]
		if !ok {
			logx.W(ctx, "DescribeRefer|GetAppBizIdsByPrimaryIds|appID|%d|not found", docAppID)
			// 降级：用当前应用业务ID兜底
			knowledgeBizID = app.BizId
		}

		rsp.List = append(rsp.List, &pb.DescribeReferRsp_ReferDetail{
			ReferBizId: refer.BusinessID,
			DocType:    refer.DocType,
			DocName:    docName,
			// 待下个版本前端修改后可删除 @sinutelu
			// /qbot/admin/getReferDetail
			//   page_content -> org_data
			PageContent:    refer.OrgData,
			OrgData:        refer.OrgData,
			Question:       refer.Question,
			Answer:         refer.Answer,
			Confidence:     refer.Confidence,
			Mark:           refer.Mark,
			Highlights:     []*pb.Highlight{},
			PageInfos:      pageInfos,
			SheetInfos:     sheetInfos,
			DocBizId:       docBizID,
			KnowledgeBizId: knowledgeBizID,
		})
	}
	return rsp, nil
}
