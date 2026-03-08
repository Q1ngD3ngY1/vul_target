package service

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"
)

// GetReferDetail 获取来源详情
func (s *Service) GetReferDetail(ctx context.Context, req *pb.GetReferDetailReq) (*pb.GetReferDetailRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.GetReferDetailRsp{}
	return rsp, nil
}

// DescribeRefer 获取来源详情
func (s *Service) DescribeRefer(ctx context.Context, req *pb.DescribeReferReq) (*pb.DescribeReferRsp, error) {
	rsp := new(pb.DescribeReferRsp)
	log.InfoContextf(ctx, "DescribeRefer Req:%+v", req)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	//threshold := config.App().HighLightThreshold

	referBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetReferBizIds())
	if err != nil {
		return nil, err
	}
	refers, err := s.dao.GetRefersByBusinessIDs(ctx, app.ID, referBizIDs)
	if err != nil {
		return rsp, errs.ErrGetReferFail
	}
	docIDs := make([]uint64, 0, len(refers))
	for _, refer := range refers {
		docIDs = append(docIDs, refer.DocID)
	}
	docIDs = slicex.Unique(docIDs)
	docs, err := s.dao.GetDocByIDs(ctx, docIDs, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReferFail
	}
	appIDs := make([]uint64, 0, len(refers))
	for _, doc := range docs {
		appIDs = append(appIDs, doc.RobotID)
	}
	appID2AppBizIDMap, err := dao.GetAppBizIDsByAppIDs(ctx, appIDs)
	if err != nil {
		log.ErrorContextf(ctx, "DescribeRefer|GetAppBizIDsByAppIDs|err:%+v", err)
		return rsp, err
	}
	rsp.List = make([]*pb.DescribeReferRsp_ReferDetail, 0, len(refers))
	//var highlightRes []*pb.Highlight
	for _, refer := range refers {
		var docBizID uint64
		var docName string
		var docAppID uint64
		if doc, ok := docs[refer.DocID]; ok {
			// QA类型且文档未开启引用时，仅记录日志不获取业务ID和文件名
			if refer.DocType == model.ReferTypeQA && !doc.IsReferOpen() {
				log.InfoContextf(ctx, "DescribeRefer|!IsReferOpen|docID|%d|referID|%d",
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
			if err = jsoniter.UnmarshalFromString(refer.PageInfos, &pageData); err != nil {
				log.WarnContextf(ctx, "DescribeRefer|PageInfos|UnmarshalFromString err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint32(page))
			}
		}
		sheetInfos, sheetData := make([]string, 0), make([]*knowledge.PageContent_SheetData, 0)
		if len(refer.SheetInfos) != 0 {
			if err = jsoniter.UnmarshalFromString(refer.SheetInfos, &sheetData); err != nil {
				log.WarnContextf(ctx, "DescribeRefer|SheetInfos|UnmarshalFromString err:%+v", err)
			}
			for _, sheet := range sheetData {
				sheetInfos = append(sheetInfos, sheet.SheetName)
			}
		}
		//highlightRes = model.HighlightRefer(ctx, refer.Answer, refer.OrgData, threshold)
		knowledgeBizID, ok := appID2AppBizIDMap[docAppID]
		if !ok {
			log.WarnContextf(ctx, "DescribeRefer|GetAppBizIDsByAppIDs|appID|%d|not found", docAppID)
			// 降级：用当前应用业务ID兜底
			knowledgeBizID = app.BusinessID
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

// MarkRefer .
func (s *Service) MarkRefer(ctx context.Context, req *pb.MarkReferReq) (*pb.MarkReferRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.MarkReferRsp)
	return rsp, nil
}
