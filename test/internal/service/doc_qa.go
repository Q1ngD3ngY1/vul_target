package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/checker"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicCommon "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_diff_task"
	logicDocQa "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_qa"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
)

const (
	standSpace   = '\u0020'
	noBreakSpace = '\u00a0'
)

// ListQA 问答对列表
func (s *Service) ListQA(ctx context.Context, req *pb.ListQAReq) (*pb.ListQARsp, error) {
	log.InfoContextf(ctx, "ListQA Req:%+v", req)
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if req.GetQueryType() == "" {
		req.QueryType = model.DocQueryTypeFileName
	}
	// 问题创建时，会将连续的空格，\u0020 间隔替换替换为\u00a0，搜索输入框不会，为了能和正确检索，这里需要替换
	// 如 \u0020\u0020 ---> \u0020\u00a0
	if len(req.GetQueryAnswer()) > 0 {
		runeAns := []rune(req.GetQueryAnswer())
		for i := 1; i < len(runeAns); i++ {
			if runeAns[i] == standSpace && runeAns[i-1] == standSpace {
				runeAns[i] = noBreakSpace
			}
		}
		req.QueryAnswer = string(runeAns)
	}
	req.AcceptStatus = slicex.Unique(req.GetAcceptStatus())
	req.ReleaseStatus = slicex.Unique(req.GetReleaseStatus())
	if app.IsShared {
		if slices.Contains(req.ReleaseStatus, model.QAReleaseStatusInit) {
			req.ReleaseStatus = append(req.ReleaseStatus, model.QAReleaseStatusSuccess)
		}
	}
	qaListReq, err := s.getQaListReq(ctx, req, app.ID, corpID)
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
	mapQaID2AttrLabels, err := s.dao.GetQAAttributeLabelDetail(ctx, app.ID, slicex.Pluck(list, (*model.DocQA).GetID))
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.getSourceDoc(ctx, list, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaIds := make([]uint64, 0, len(list))
	for _, item := range list {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, app.ID, qaIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	qaList := make([]*pb.ListQARsp_QA, 0, len(list))
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	staffIDs, qaIDs := make([]uint64, 0, len(list)), make([]uint64, 0, len(list))
	for _, qa := range list {
		qaIDs = append(qaIDs, qa.ID)
		staffIDs = append(staffIDs, qa.StaffID)
	}
	qaCountMap, err := s.dao.GetSimilarQuestionsCountByQAIDs(ctx, app.CorpID, app.ID, qaIDs)
	if err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsCountByQAIDs failed, qaIDs(%+v) err:%+v", qaIDs, err)
		return nil, errs.ErrSystem
	}
	// 获取员工名称
	staffByID, err := client.ListCorpStaffByIds(ctx, pkg.CorpBizID(ctx), staffIDs)
	if err != nil { // 失败降级为返回员工ID
		log.ErrorContextf(ctx, "ListDbSource get staff name err:%v,staffIDs:%v", err, staffIDs)
	}
	for _, item := range list {
		_, isReleasing := releasingQaIdMap[item.ID]
		var fileName, fileType string
		var docBizID uint64
		var simCount uint32
		if doc, ok := docs[item.DocID]; ok {
			fileName = doc.GetFileNameByStatus()
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
		log.InfoContextf(ctx, "similarQuestionTips:%s", item.SimilarQuestionTips)

		refineAnswer := []rune(item.Answer)
		for i := 0; i < len(refineAnswer); i++ {
			if refineAnswer[i] == noBreakSpace {
				refineAnswer[i] = standSpace
			}
		}
		qaNode := &pb.ListQARsp_QA{
			QaBizId:             item.BusinessID,
			Question:            item.Question,
			Source:              item.Source,
			SourceDesc:          i18n.Translate(ctx, item.SourceDesc(docs)),
			UpdateTime:          item.UpdateTime.Unix(),
			CreateTime:          item.CreateTime.Unix(),
			Status:              item.Status(),
			StatusDesc:          i18n.Translate(ctx, item.StatusDesc(latestRelease.IsPublishPause())),
			DocBizId:            docBizID,
			IsAllowEdit:         !isReleasing && item.IsAllowEdit(),
			IsAllowAccept:       !isReleasing && item.IsAllowAccept(),
			IsAllowDelete:       !isReleasing && item.IsAllowDelete(),
			FileName:            fileName,
			FileType:            fileType,
			Answer:              string(refineAnswer),
			QaCharSize:          item.CharSize,
			ExpireStart:         uint64(item.ExpireStart.Unix()),
			ExpireEnd:           uint64(item.ExpireEnd.Unix()),
			AttrRange:           item.AttrRange,
			AttrLabels:          fillPBAttrLabels(mapQaID2AttrLabels[item.ID]),
			SimilarQuestionNum:  simCount,
			SimilarQuestionTips: item.SimilarQuestionTips,
			IsDisabled:          item.IsDisable(),
		}
		if staffName, ok := staffByID[item.StaffID]; ok { // 赋值员工名称
			qaNode.StaffName = staffName
		} else { // 没取到返回员工ID
			qaNode.StaffName = cast.ToString(item.StaffID)
		}
		if app.IsShared {
			if qaNode.Status == model.QAReleaseStatusSuccess {
				// 共享知识库，需要兼容从应用知识库人工转换成共享知识库的情况
				qaNode.Status = model.QAReleaseStatusInit
			}
			if qaNode.Status == model.QAReleaseStatusInit {
				// 共享知识库不需要发布，所以将待发布、已发布状态的问答显示为导入完成
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
		if req.GetShowCurrCate() == model.ShowCurrCate { // 只展示当前分类的数据
			cateIDs = append(cateIDs, cateID)
		} else {
			cateIDs, err = s.getCateChildrenIDs(ctx, model.QACate, corpID, robotID, cateID)
			if err != nil {
				return nil, err
			}
		}
	}
	docID, err := s.validateDocAndRetrieveID(ctx, req.GetDocBizId(), robotID)
	if err != nil {
		return nil, err
	}
	validityStatus, releaseStatus, err := s.getQaExpireStatus(req)
	if err != nil {
		return nil, err
	}
	err = s.checkQueryType(req.GetQueryType())
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
		ReleaseStatus:  slicex.Unique(releaseStatus),
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		DocID:          utils.When(docID > 0, []uint64{docID}, nil),
		CateIDs:        cateIDs,
		QABizIDs:       req.GetQaBizIds(),
		ValidityStatus: validityStatus,
	}, nil
}

func (s *Service) getValidityQaCount(ctx context.Context, robotID, corpID uint64) (uint64, error) {
	deletingDocID, err := s.getDeletingDocID(ctx, corpID, robotID)
	if err != nil {
		return 0, errs.ErrSystem
	}
	req := model.QAListReq{
		RobotID:        robotID,
		CorpID:         corpID,
		IsDeleted:      model.QAIsNotDeleted,
		ExcludeDocID:   deletingDocID,
		Page:           1,
		PageSize:       1,
		AcceptStatus:   []uint32{model.AcceptYes},
		ReleaseStatus:  s.getValidityReleaseStatus(),
		ValidityStatus: model.QaUnExpiredStatus,
	}
	_, _, _, total, err := s.dao.GetQAListCount(ctx, &req)
	if err != nil {
		return 0, err
	}
	return uint64(total), nil
}

func (s *Service) getValidityReleaseStatus() []uint32 {
	return []uint32{
		model.QAReleaseStatusInit,
		model.QAReleaseStatusIng,
		model.QAReleaseStatusSuccess,
		model.QAReleaseStatusFail,
	}
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

func (s *Service) getPendingReleaseQA(ctx context.Context, robotID uint64, qas []*model.DocQA) (
	map[uint64]*model.ReleaseQA, error) {
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

// getSourceCate 来源分类
func (s *Service) getSourceCate(ctx context.Context, qas []*model.DocQA) (map[uint64]*model.CateInfo, error) {
	cateIDs := make([]uint64, 0, len(qas))
	for _, qa := range qas {
		cateIDs = append(cateIDs, qa.CategoryID)
	}
	return s.dao.GetCateByIDs(ctx, model.QACate, slicex.Unique(cateIDs))
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

// DescribeQA 获取QA详情
func (s *Service) DescribeQA(ctx context.Context, req *pb.DescribeQAReq) (*pb.DescribeQARsp, error) {
	log.InfoContextf(ctx, "DescribeQA Req:%+v", req)
	rsp := new(pb.DescribeQARsp)
	corpID := pkg.CorpID(ctx)
	threshold := config.App().HighLightThreshold
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	var qaBizID uint64
	if req.GetQaBizId() != "" {
		vv, err := util.CheckReqParamsIsUint64(ctx, req.GetQaBizId())
		if err != nil {
			return nil, err
		}
		qaBizID = vv
	}
	qa, err := s.dao.GetQADetailsByBizID(ctx, corpID, app.ID, qaBizID)
	if err != nil {
		return rsp, err
	}
	if qa.IsDelete() {
		return rsp, errs.ErrQAIsNotExist
	}
	docs, err := s.getSourceDoc(ctx, []*model.DocQA{qa}, app.ID)
	if err != nil {
		return rsp, errs.ErrDocNotFound
	}
	cates, err := s.getSourceCate(ctx, []*model.DocQA{qa})
	if err != nil {
		return rsp, errs.ErrCateNotFound
	}

	sqs, err := s.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		// 伽利略error日志告警
		log.ErrorContextf(ctx, "DescribeQA qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
		// 柔性放过
	}
	var segmentByID *model.DocSegmentExtend
	if qa.Source != model.SourceFromManual {
		segmentByID, err = s.dao.GetSegmentByID(ctx, qa.SegmentID, app.ID)
		if err != nil {
			log.ErrorContextf(ctx, "[DescribeQA] get segment of doc_qa(%d) failed: %v", qa.ID, err)
			return rsp, errs.ErrSegmentNotFound
		}
		if segmentByID == nil { // 如果问答绑定的segment不存在，说明相应doc和segment已被删除，则返回空
			log.InfoContextf(ctx, "[DescribeQA] qa_id(%d): segment_id(%d) not existed in t_doc_segment", qa.ID, qa.SegmentID)
			segmentByID = &model.DocSegmentExtend{}
		} else if segmentByID.OrgData == "" { // 如果旧表格没有orgData，则从t_doc_segment_org_data新表中获取orgData
			corpReq := &admin.GetCorpReq{
				Id: corpID,
			}
			corpRsp, err := s.dao.GetAdminApiCli().GetCorp(ctx, corpReq)
			if err != nil || corpRsp == nil {
				log.ErrorContextf(ctx, "[DescribeQA] get corp(%d) failed: %+v", corpID, err)
				return rsp, errs.ErrCorpNotFound
			}
			doc, err := s.dao.GetDocByID(ctx, segmentByID.DocID, segmentByID.RobotID)
			if err != nil {
				log.ErrorContextf(ctx, "[DescribeQA] get doc(%d) failed: %+v", segmentByID.DocID, err)
				return rsp, err
			}
			if doc == nil {
				log.ErrorContextf(ctx, "[DescribeQA] doc(%d) not existed", segmentByID.DocID)
				return rsp, errs.ErrDocNotFound
			}
			filter := &dao.DocSegmentOrgDataFilter{
				CorpBizID:      corpRsp.GetCorpBizId(),
				AppBizID:       app.BusinessID,
				DocBizID:       doc.BusinessID,
				BusinessIDs:    []uint64{segmentByID.OrgDataBizID},
				RouterAppBizID: app.BusinessID,
			}
			orgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgData(ctx,
				[]string{dao.DocSegmentOrgDataTblColOrgData}, filter)
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				log.ErrorContextf(ctx, "[DescribeQA] get doc org data with %d failed: %+v", segmentByID.OrgDataBizID, err)
				return rsp, err
			}
			if orgData != nil && orgData.OrgData != "" {
				segmentByID.OrgData = orgData.OrgData
			}
			log.DebugContextf(ctx, "task(DocToQA) Process GetDocOrgDataByBizID|segmentByID.OrgData:%s",
				segmentByID.OrgData)
		}
		// 待下个版本前端修改后可删除 @sinutelu
		// /qbot/admin/getQADetail
		//   page_content -> org_data
		rsp.PageContent = segmentByID.OrgData
		rsp.OrgData = segmentByID.OrgData
		rsp.SegmentBizId = segmentByID.BusinessID
		rsp.Highlights = model.HighlightRefer(ctx, qa.Answer, segmentByID.OrgData, threshold)
	}
	mapQaID2AttrLabels, err := s.dao.GetQAAttributeLabelDetail(ctx, app.ID, []uint64{qa.ID})
	if err != nil {
		log.ErrorContextf(ctx, "[DescribeQA] get qa_attribute_label_detail err: %+v", err)
		return rsp, errs.ErrSystem
	}
	auditFailList, err := s.dao.GetLatestAuditFailListByRelateID(ctx, corpID, qa.RobotID, qa.ID, model.AuditBizTypeQa,
		false)
	if err != nil && err != errs.ErrAppealNotFound {
		log.ErrorContextf(ctx, "DescribeQA qa:%+v, GetLatestAuditFailListByRelateID err: %+v", qa, err)
	}
	if len(auditFailList) > 0 {
		err = s.dao.DescribeQaAuditFailStatus(ctx, qa, sqs, auditFailList, pkg.Uin(ctx), app.BusinessID)
		if err != nil {
			log.ErrorContextf(ctx, "DescribeQA qa:%+v, DescribeQaAuditFailStatus err: %+v", qa, err)
		}
		log.InfoContextf(ctx, "DescribeQA, len(auditFailList):%+v, qa:%+v", len(auditFailList), qa)
	}
	rsp = s.fillDescribeQaRsp(ctx, qa, sqs, docs, cates, mapQaID2AttrLabels, rsp)
	return rsp, nil
}

func (s *Service) fillDescribeQaRsp(ctx context.Context, qa *model.DocQA,
	sqs []*model.SimilarQuestion, docs map[uint64]*model.Doc, cates map[uint64]*model.CateInfo,
	mapQaID2AttrLabels map[uint64][]*model.AttrLabel,
	rsp *pb.DescribeQARsp) *pb.DescribeQARsp {
	rsp.QaBizId = qa.BusinessID
	rsp.Question = qa.Question
	rsp.Answer = qa.Answer
	rsp.CustomParam = qa.CustomParam
	rsp.QuestionDesc = qa.QuestionDesc
	rsp.Source = qa.Source
	rsp.SourceDesc = i18n.Translate(ctx, qa.SourceDesc(docs))
	rsp.UpdateTime = qa.UpdateTime.Unix()
	rsp.Status = qa.Status()
	rsp.StatusDesc = i18n.Translate(ctx, qa.StatusDesc(false))
	rsp.CateBizId = qa.CateBizID(cates)
	rsp.DocBizId = qa.DocBizID(docs)
	rsp.IsAllowAccept = qa.IsAllowAccept()
	rsp.IsAllowEdit = qa.IsAllowEdit()
	rsp.IsAllowDelete = qa.IsAllowDelete()
	rsp.AttrRange = qa.AttrRange
	rsp.AttrLabels = fillPBAttrLabels(mapQaID2AttrLabels[qa.ID])
	rsp.ExpireStart = uint64(qa.ExpireStart.Unix())
	rsp.ExpireEnd = uint64(qa.ExpireEnd.Unix())
	if doc, ok := docs[qa.DocID]; ok {
		rsp.FileName = doc.FileName
		if doc.Status == model.DocStatusWaitRelease {
			doc.GetRealFileName()
		}
		rsp.FileType = doc.FileType
	}
	rsp.QaAuditStatus = model.FrontEndAuditPass
	rsp.PicAuditStatus = model.FrontEndAuditPass
	rsp.VideoAuditStatus = model.FrontEndAuditPass
	if qa.QaAuditFail {
		rsp.QaAuditStatus = model.FrontEndQaAuditFailed
	}
	if qa.PicAuditFail {
		rsp.PicAuditStatus = model.FrontEndPicAuditFailed
	}
	if qa.VideoAuditFail {
		rsp.VideoAuditStatus = model.FrontEndVideoAuditFailed
	}
	pbSqs := make([]*pb.SimilarQuestion, 0, len(sqs))
	for _, sq := range sqs {
		pbSqs = append(pbSqs, &pb.SimilarQuestion{
			SimBizId: sq.SimilarID,
			Question: sq.Question,
			AuditStatus: func() uint32 {
				if sq.ReleaseStatus == model.QAReleaseStatusAuditNotPass ||
					sq.ReleaseStatus == model.QAReleaseStatusAppealFail {
					return model.FrontEndSimilarQuestionAuditFailed
				}
				return model.FrontEndAuditPass
			}(),
		})
	}
	rsp.SimilarQuestions = pbSqs
	rsp.IsDisabled = qa.IsDisable()
	return rsp
}

// GetQADetail QA详情
func (s *Service) GetQADetail(ctx context.Context, req *pb.GetQADetailReq) (*pb.GetQADetailRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.GetQADetailRsp{}
	return rsp, nil
}

// CreateQA 创建QA
func (s *Service) CreateQA(ctx context.Context, req *pb.CreateQAReq) (*pb.CreateQARsp, error) {
	log.InfoContextf(ctx, "CreateQA Req:%+v", req)
	rsp := new(pb.CreateQARsp)
	staffID := pkg.StaffID(ctx)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}
	// 前端按base64传代码内容，需要做decode
	decodeQuestion, isBase64 := util.StrictBase64DecodeToValidString(req.GetQuestion())
	log.DebugContextf(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, req.GetQuestion(),
		decodeQuestion)

	decodeAnswer, isBase64 := util.StrictBase64DecodeToValidString(req.GetAnswer())
	log.DebugContextf(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, req.GetAnswer(), decodeAnswer)

	req.Answer = strings.TrimSpace(decodeAnswer)
	req.Question = strings.TrimSpace(decodeQuestion)

	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	if !app.IsShared && releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
		return rsp, errs.ErrReleaseQaMaxCount
	}
	if err = checkQAAndDescAndParam(ctx, req.GetQuestion(), req.GetAnswer(),
		req.GetQuestionDesc(), req.GetCustomParam()); err != nil {
		return nil, err
	}
	if _, err = checkSimilarQuestionNumLimit(ctx, len(req.GetSimilarQuestions()), 0, 0); err != nil {
		return nil, err
	}
	var simTotalCharSize = 0 // 相似问总字符数
	if simTotalCharSize, err = checkSimilarQuestionContent(ctx, req.GetQuestion(), req.GetSimilarQuestions()); err != nil {
		return nil, err
	}
	var cateID uint64
	if req.GetCateBizId() != "" {
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cateID, err = s.dao.CheckCateBiz(ctx, model.QACate, corpID, cateBizID, app.ID)
	} else {
		cateID, err = s.dao.GetRobotUncategorizedCateID(ctx, model.QACate, corpID, app.ID)
	}
	if err != nil {
		return nil, errs.ErrCateNotFound
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	docID, err := s.validateDocAndRetrieveID(ctx, docBizID, app.ID)
	if err != nil {
		return nil, errs.ErrDocNotFound
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = model.AttrRangeCondition
	} else {
		req.AttrRange = model.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.ID, config.App().AttributeLabel.QAAttrLimit,
		config.App().AttributeLabel.QAAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, err
	}
	err = util.CheckMarkdownImageURL(ctx, req.GetAnswer(), pkg.Uin(ctx), app.BusinessID, nil)
	if err != nil {
		log.WarnContextf(ctx, "ModifyQA Answer CheckQaImgURLSafeToMD err:%d", err)
		return nil, err
	}
	videoCharSize, err := s.dao.GetVideoURLsCharSize(ctx, req.GetAnswer())
	if err != nil {
		return nil, err
	}
	log.InfoContextf(ctx, "CreateQA Answer videoCharSize|%d", videoCharSize)
	// 检查字符限制(含相似问、答案中视频转换的字符数)
	diff := utf8.RuneCountInString(req.GetQuestion()+req.GetAnswer()) + simTotalCharSize + videoCharSize
	if err := CheckIsCharSizeExceeded(ctx, s.dao, botBizID, corpID, int64(diff)); err != nil {
		return nil, err
	}
	var replyID uint64
	if req.GetBusinessId() > 0 {
		reply, err := s.dao.GetUnsatisfiedReplyByBizIDs(ctx, corpID, app.ID, []uint64{req.GetBusinessId()})
		if err != nil {
			return rsp, errs.ErrSystem
		}
		if len(reply) == 0 {
			return rsp, errs.ErrUnsatisfiedReplyNotFound
		}
		replyID = reply[0].ID
	}
	businessID := s.dao.GenerateSeqID()

	expireStart, expireEnd, err := util.CheckReqStartEndTime(ctx, req.GetExpireStart(), req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	var releaseStatus, isAuditFree = model.QAReleaseStatusAuditing, model.QAIsAuditNotFree
	if !config.AuditSwitch() {
		releaseStatus = model.QAReleaseStatusLearning
		isAuditFree = model.QAIsAuditFree
	}
	qa := &model.DocQA{
		BusinessID:    businessID,
		RobotID:       app.ID,
		CorpID:        corpID,
		StaffID:       staffID,
		DocID:         docID,
		Source:        model.SourceFromManual,
		Question:      strings.TrimSpace(req.GetQuestion()),
		Answer:        strings.TrimSpace(req.GetAnswer()),
		CustomParam:   strings.TrimSpace(req.GetCustomParam()),
		QuestionDesc:  strings.TrimSpace(req.GetQuestionDesc()),
		ReleaseStatus: releaseStatus,
		IsAuditFree:   isAuditFree,
		IsDeleted:     model.QAIsNotDeleted,
		AcceptStatus:  model.AcceptYes,
		CategoryID:    cateID,
		NextAction:    model.NextActionAdd,
		CharSize:      uint64(diff), // 总字符数(含相似问)
		AttrRange:     req.GetAttrRange(),
		ExpireStart:   time.Unix(int64(expireStart), 0),
		ExpireEnd:     time.Unix(int64(expireEnd), 0),
	}

	attributeLabelPB, err := fillQAAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}
	if err = s.dao.CreateQA(ctx, qa, req.GetBusinessSource(), replyID, attributeLabelPB,
		req.GetSimilarQuestions()); err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.QaBizId = businessID
	_ = s.dao.AddOperationLog(ctx, model.QaEventAdd, corpID, app.ID, req, rsp, nil, qa)
	return rsp, nil
}

// getCosFileSize 获取cos文件大小
func (s *Service) getCosFileSize(ctx context.Context, cosUrl string) (*model.ObjectInfo, error) {
	u, err := url.Parse(cosUrl)
	if err != nil {
		return nil, err
	}
	// 去掉前面的斜线
	path := strings.TrimPrefix(u.Path, "/")
	log.InfoContextf(ctx, "getCosFileSize|Path:%s", path)
	objectInfo, err := s.dao.StatObject(ctx, path)
	if err != nil || objectInfo == nil {
		log.ErrorContextf(ctx, "getCosFileSize|StatObject:%+v err:%v", err, objectInfo)
		return nil, err
	}
	log.InfoContextf(ctx, "getCosFileSize|StatObject:%+v", objectInfo)
	return objectInfo, nil
}

// checkSimilarQuestions 相似问修改请求参数校验
func (s *Service) checkSimilarQuestions(ctx context.Context, qa *model.DocQA,
	similarModify *pb.SimilarQuestionModify) error {
	if qa == nil || similarModify == nil {
		return nil
	}
	// 判断当前主问的相似问总数是否超出
	count, err := s.dao.GetSimilarQuestionsCount(ctx, qa)
	if err != nil {
		return err
	}
	_, err = checkSimilarQuestionNumLimit(ctx, len(similarModify.GetAddQuestions()),
		len(similarModify.GetDeleteQuestions()), count)
	if err != nil {
		return err
	}
	sqs, err := s.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		return err
	}
	sqStrs := make([]string, 0)
	deletedMap := make(map[uint64]struct{})
	for _, sq := range similarModify.GetDeleteQuestions() {
		deletedMap[sq.GetSimBizId()] = struct{}{}
	}
	updatedMap := make(map[uint64]string)
	for _, sq := range similarModify.GetUpdateQuestions() {
		updatedMap[sq.GetSimBizId()] = sq.GetQuestion()
	}
	for _, sq := range sqs {
		if _, ok := deletedMap[sq.ID]; ok {
			// 针对删除的相似问，不做检测
			continue
		}
		if q, ok := updatedMap[sq.ID]; ok {
			// 针对更新的相似问，用更新后的 question 做检测
			sqStrs = append(sqStrs, q)
			continue
		}
		sqStrs = append(sqStrs, sq.Question)
	}
	// 新增的相似问
	sqStrs = append(sqStrs, similarModify.GetAddQuestions()...)
	_, err = checkSimilarQuestionContent(ctx, qa.Question, sqStrs)
	return err
}

// checkQAAndParam 检查问题答案｜自定义参数限制

// checkQAAndDescAndParam 检查问题答案｜问题描述|自定义参数限制
func checkQAAndDescAndParam(ctx context.Context, question, answer, questionDesc, param string) error {
	err := checkQuestionAndAnswer(ctx, question, answer)
	if err != nil {
		return err
	}
	err = checkQuestionDesc(ctx, questionDesc)
	if err != nil {
		return err
	}
	paramCfg := config.App().DocQA.CustomParam
	param = strings.TrimSpace(param)
	if len([]rune(param)) < paramCfg.MinLength {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeyCustomParamTooShort),
			paramCfg.MinLength)
	}
	if len([]rune(param)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, paramCfg.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooLong, i18n.Translate(ctx, i18nkey.KeyCustomParamTooLong),
			paramCfg.MaxLength)
	}
	return nil
}

// checkQuestionDesc 检查问题描述限制
func checkQuestionDesc(ctx context.Context, questionDesc string) error {
	questionDescCfg := config.App().DocQA.QuestionDesc
	questionDesc = strings.TrimSpace(questionDesc)
	if len([]rune(questionDesc)) < questionDescCfg.MinLength {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeyProblemDescriptionTooShort),
			questionDescCfg.MinLength)
	}
	if len([]rune(questionDesc)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, questionDescCfg.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooLong, i18n.Translate(ctx, i18nkey.KeyProblemDescriptionTooLong),
			questionDescCfg.MaxLength)
	}
	return nil
}

// checkSimilarQuestionNumLimit 检查相似问总的数量限制
func checkSimilarQuestionNumLimit(ctx context.Context, newNum, deleteNum, existedNum int) (totalLength int, err error) {
	cfg := config.App().DocQA
	totalNum := existedNum + newNum - deleteNum
	if totalNum > config.App().DocQA.SimilarQuestionNumLimit {
		return 0, errs.ErrWrapf(errs.ErrCodeSimilarQuestionExceedLimit,
			i18n.Translate(ctx, i18nkey.KeySimilarQuestionLimitExceeded),
			cfg.SimilarQuestionNumLimit)
	}
	return totalNum, nil
}

// checkSimilarQuestionContent 检查相似问内容: 是否存在重复, 以及每一个相似问的字符数(满足限制), 返回相似问总字符数
func checkSimilarQuestionContent(ctx context.Context, qa string, sqs []string) (simTotalCharSize int, err error) {
	if len(qa) == 0 || len(sqs) == 0 {
		return 0, nil
	}
	cfg := config.App().DocQA
	simTotalCharSize = 0
	allQuestions := make(map[string]struct{})
	allQuestions[strings.TrimSpace(qa)] = struct{}{}
	for _, q := range sqs {
		s := strings.TrimSpace(q)
		if _, ok := allQuestions[s]; ok {
			return 0, errs.ErrWrapf(errs.ErrCodeSimilarQuestionRepeated, i18n.Translate(ctx,
				i18nkey.KeyDuplicateSimilarQuestionFound),
				s)
		}
		if len([]rune(s)) < cfg.SimilarQuestion.MinLength {
			return 0, errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooShort),
				cfg.SimilarQuestion.MinLength)
		}
		if len([]rune(s)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.SimilarQuestion.MaxLength) {
			return 0, errs.ErrWrapf(errs.ErrCodeQuestionTooLong, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooLong),
				cfg.SimilarQuestion.MaxLength)
		}
		simTotalCharSize += utf8.RuneCountInString(s)
		allQuestions[s] = struct{}{}
	}
	return simTotalCharSize, nil
}

func checkQuestionAndAnswer(ctx context.Context, question, answer string) error {
	cfg := config.App().DocQA
	question = strings.TrimSpace(question)
	answer = strings.TrimSpace(answer)
	if len([]rune(question)) < cfg.Question.MinLength {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooShort),
			cfg.Question.MinLength)
	}
	if len([]rune(question)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Question.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooLong, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooLong),
			i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Question.MaxLength))
	}
	if len([]rune(answer)) < cfg.Answer.MinLength {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooShort, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooShort),
			cfg.Answer.MinLength)
	}
	if len([]rune(answer)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Answer.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooLong, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooLong),
			i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Answer.MaxLength))
	}
	return nil
}

// checkInDataValidity 检查有效期格式
func checkInDataValidity(ctx context.Context, row []string) error {
	if model.ExcelTplExpireTimeIndex+1 > len(row) {
		return nil
	}
	s := string(pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplExpireTimeIndex]))))
	if len(s) == 0 || s == model.ExcelNoExpireTime || s == i18n.Translate(ctx, model.ExcelNoExpireTime) {
		return nil
	}
	t, err := time.ParseInLocation(model.ExcelTplTimeLayout, s, time.Local)
	if err != nil {
		log.WarnContextf(ctx, "解析文件时间字段失败 err:%+v", err)
		return errs.ErrExcelParseFailInDateValidity
	}
	if t.Unix()%model.HalfHourTime != 0 {
		return errs.ErrExcelParseFailNotHalfHour
	}
	return nil
}

func (s *Service) checkDocs(ctx context.Context, docIds []uint64, robotID uint64) error {
	if len(docIds) == 0 {
		return nil
	}
	docs, err := s.dao.GetDocByIDs(ctx, docIds, robotID)
	if err != nil {
		return err
	}
	for _, docId := range docIds {
		if doc, ok := docs[docId]; ok {
			if doc.HasDeleted() {
				return errs.ErrDocHasDeleted
			}
		} else {
			return errs.ErrDocNotFound
		}
	}
	return nil
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
	if doc.RobotID != robotID { // 文档不属于该应用
		return 0, errs.ErrDocNotFound
	}
	return doc.ID, nil
}

// ModifyQA 更新QA
func (s *Service) ModifyQA(ctx context.Context, req *pb.ModifyQAReq) (*pb.ModifyQARsp, error) {
	log.InfoContextf(ctx, "ModifyQA Req:%+v", req)
	rsp := new(pb.ModifyQARsp)
	// 先加锁，防止重复更新
	qaBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetQaBizId())
	if err != nil {
		return nil, err
	}
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteQa, 2*time.Second, []uint64{qaBizID})
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteQa, []uint64{qaBizID})
	if err != nil {
		return nil, errs.ErrQaIsModifyingOrDeleting
	}

	corpID := pkg.CorpID(ctx)
	expireStart, expireEnd, err := util.CheckReqStartEndTime(ctx, req.GetExpireStart(), req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}

	// 前端按base64传代码内容，需要做decode
	decodeQuestion, isBase64 := util.StrictBase64DecodeToValidString(req.GetQuestion())
	log.DebugContextf(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, req.GetQuestion(),
		decodeQuestion)

	decodeAnswer, isBase64 := util.StrictBase64DecodeToValidString(req.GetAnswer())
	log.DebugContextf(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, req.GetAnswer(), decodeAnswer)

	req.Answer = strings.TrimSpace(decodeAnswer)
	req.Question = strings.TrimSpace(decodeQuestion)

	if err := checkQAAndDescAndParam(ctx, req.GetQuestion(), req.GetAnswer(),
		req.GetQuestionDesc(), req.GetCustomParam()); err != nil {
		return nil, err
	}
	qa, err := s.dao.GetQADetailsByBizID(ctx, corpID, app.ID, qaBizID)
	if err != nil {
		return rsp, errs.ErrQANotFound
	}
	if qa.IsDelete() {
		return rsp, errs.ErrQAIsDeleted
	}
	if !qa.IsAllowEdit() {
		return rsp, errs.ErrQANotAllowEdit
	}
	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	if !app.IsShared && qa.ReleaseStatus == model.QAReleaseStatusSuccess &&
		releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
		return rsp, errs.ErrReleaseQaMaxCount
	}
	if err := s.checkSimilarQuestions(ctx, qa, req.GetSimilarQuestionModify()); err != nil {
		return nil, err
	}
	var docID uint64
	if req.DocBizId == "" {
		docID = qa.DocID
	} else {
		docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
		if err != nil {
			return nil, err
		}
		docID, err = s.validateDocAndRetrieveID(ctx, docBizID, app.ID)
		if err != nil {
			return rsp, errs.ErrDocNotFound
		}
	}
	if qa.Source == model.SourceFromDoc && qa.DocID != docID {
		return rsp, errs.ErrReferDocFail
	}
	var cateID uint64
	if req.GetCateBizId() != "" {
		catBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cateID, err = s.dao.CheckCateBiz(ctx, model.QACate, corpID, catBizID, app.ID)
	} else {
		cateID, err = s.dao.GetRobotUncategorizedCateID(ctx, model.QACate, corpID, app.ID)
	}
	if err != nil {
		return rsp, err
	}
	attrs, labels, isAttributeLabelReferChange, err := s.checkQAAttributeLabelRefer(ctx, req, app.ID, qa)
	if err != nil {
		return rsp, err
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, app.ID, []uint64{qa.ID})
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	if _, ok := releasingQaIdMap[qa.ID]; ok {
		return rsp, errs.ErrQAIsPendingRelease
	}
	oldQa := *qa
	isNeedAudit := s.isNeedAudit(qa, req)
	isNeedPublish, isAllSimQuestionsNeedPublish := s.isNeedPublish(qa, isAttributeLabelReferChange, req, docID, cateID,
		expireStart, expireEnd)
	s.fillQaReleaseStatus(isNeedAudit, isNeedPublish, qa)
	err = util.CheckMarkdownImageURL(ctx, req.GetAnswer(), pkg.Uin(ctx), app.BusinessID, nil)
	if err != nil {
		log.WarnContextf(ctx, "ModifyQA Answer ConvertDocQaHtmlToMD err:%d", err)
		return nil, err
	}
	qa.Answer = strings.TrimSpace(req.GetAnswer())
	// req.Answer = strings.TrimSpace(mdAnswer)
	var diff int64
	if qa.IsAccepted() {
		qa.CharSize, err = s.dao.GetNewCharSize(ctx, &oldQa, req)
		if err != nil {
			log.WarnContextf(ctx, "ModifyQA|GetNewCharSize err:%+v", err)
			if errors.Is(err, errs.ErrVideoURLFail) {
				return rsp, err
			}
			return nil, errs.ErrSystem
		}
		diff = int64(qa.CharSize) - int64(oldQa.CharSize)
		if err := CheckIsCharSizeExceeded(ctx, s.dao, botBizID, corpID, diff); err != nil {
			return rsp, err
		}
	}

	attributeLabelPB, err := fillQAAttributeLabelsFromPB(ctx, req.GetAttrLabels(), isAttributeLabelReferChange, attrs,
		labels)
	if err != nil {
		return nil, err
	}
	var sqm *model.SimilarQuestionModifyInfo
	// 如果主问的过期时间、属性标签等全局信息发生变更，则需要发布所有相似问
	if isAllSimQuestionsNeedPublish || attributeLabelPB.IsNeedChange {
		sqm, err = s.dao.NewSimilarQuestionsFromDBAndReq(ctx, qa, req.GetSimilarQuestionModify(), true)
	} else {
		sqm, err = s.dao.NewSimilarQuestionsFromDBAndReq(ctx, qa, req.GetSimilarQuestionModify(), false)
	}
	if err != nil {
		return nil, errs.ErrSystem
	}
	qa.StaffID = pkg.StaffID(ctx)
	if err = s.dao.UpdateQA(ctx, qa, sqm, isNeedPublish, isNeedAudit, diff, attributeLabelPB); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.QaEventEdit, corpID, app.GetAppID(), req, rsp, qa, qa)
	return rsp, nil
}

// isNeedAudit 是否需要审核
func (s *Service) isNeedAudit(qa *model.DocQA, req *pb.ModifyQAReq) bool {
	isNeedAudit := false
	if !config.AuditSwitch() { // 审核开关关闭就不需要审核
		return isNeedAudit
	}
	if qa.Question != strings.TrimSpace(req.GetQuestion()) ||
		qa.Answer != strings.TrimSpace(req.GetAnswer()) ||
		qa.QuestionDesc != strings.TrimSpace(req.GetQuestionDesc()) {
		isNeedAudit = true
	}
	// 相似问新增或修改，也需要审核
	sqm := req.GetSimilarQuestionModify()
	if sqm != nil {
		if len(sqm.GetAddQuestions()) != 0 || len(sqm.GetUpdateQuestions()) != 0 {
			isNeedAudit = true
		}
	}
	return isNeedAudit
}

// isNeedPublish 会更新qa的信息
func (s *Service) isNeedPublish(qa *model.DocQA, isAttributeLabelReferChange bool, req *pb.ModifyQAReq, docID,
	cateID, expireStart, expireEnd uint64) (isNeedPublish bool, isAllSimQuestionsNeedPublish bool) {
	isNeedPublish = false
	isAllSimQuestionsNeedPublish = false
	if qa.Question != strings.TrimSpace(req.GetQuestion()) ||
		qa.Answer != strings.TrimSpace(req.GetAnswer()) ||
		qa.CustomParam != strings.TrimSpace(req.GetCustomParam()) ||
		qa.QuestionDesc != strings.TrimSpace(req.GetQuestionDesc()) ||
		qa.DocID != docID || isAttributeLabelReferChange ||
		qa.CategoryID != cateID {
		qa.Question = strings.TrimSpace(req.GetQuestion())
		qa.Answer = strings.TrimSpace(req.GetAnswer())
		qa.CustomParam = strings.TrimSpace(req.GetCustomParam())
		qa.QuestionDesc = strings.TrimSpace(req.GetQuestionDesc())
		qa.DocID = docID
		qa.CategoryID = cateID
		isNeedPublish = true
	}
	if qa.ReleaseStatus == model.QAReleaseStatusAuditNotPass {
		// 审核不通过的即使没有做任何更改也算一次调整
		isNeedPublish = true
	}
	// 知识库有效期发生变更，也需要发布
	if qa.ExpireStart.Unix() != int64(expireStart) || qa.ExpireEnd.Unix() != int64(expireEnd) {
		qa.ExpireStart = time.Unix(int64(expireStart), 0)
		qa.ExpireEnd = time.Unix(int64(expireEnd), 0)
		isNeedPublish = true
		isAllSimQuestionsNeedPublish = true
	}
	// 相似问有变化，也需要发布
	sqm := req.GetSimilarQuestionModify()
	if sqm != nil {
		if len(sqm.GetAddQuestions()) != 0 || len(sqm.GetDeleteQuestions()) != 0 || len(sqm.GetUpdateQuestions()) != 0 {
			isNeedPublish = true
		}
	}

	qa.Answer = strings.TrimSpace(req.GetAnswer())
	qa.CustomParam = strings.TrimSpace(req.GetCustomParam())
	qa.QuestionDesc = strings.TrimSpace(req.GetQuestionDesc())
	qa.CategoryID = cateID
	qa.AttrRange = req.GetAttrRange()
	if isNeedPublish {
		qa.SimilarStatus = model.SimilarStatusInit
	}
	if isNeedPublish && !qa.IsNextActionAdd() {
		qa.NextAction = model.NextActionUpdate
	}
	return isNeedPublish, isAllSimQuestionsNeedPublish
}
func (s *Service) fillQaReleaseStatus(isNeedAudit, isNeedPublish bool, qa *model.DocQA) {
	if isNeedAudit { // 需要审核，就改成审核状态
		qa.ReleaseStatus = model.QAReleaseStatusAuditing
		qa.IsAuditFree = model.QAIsAuditNotFree
		return
	}
	if !isNeedPublish { // 不需要审核，也不需要发布，就不改qa状态
		return
	}
	// 不需要审核，需要发布，但是原先处于审核失败或者人工申诉失败，也不改qa状态
	if qa.ReleaseStatus == model.QAReleaseStatusAuditNotPass ||
		qa.ReleaseStatus == model.QAReleaseStatusAppealFail {
		return
	}
	qa.ReleaseStatus = model.QAReleaseStatusLearning
	qa.IsAuditFree = model.QAIsAuditFree
}
func (s *Service) checkQAAttributeLabelRefer(ctx context.Context, req *pb.ModifyQAReq, robotID uint64,
	qa *model.DocQA) (map[uint64]*model.Attribute, map[uint64]*model.AttributeLabel, bool, error) {
	if qa.Source == model.SourceFromDoc && req.GetAttrRange() != model.AttrRangeDefault {
		return nil, nil, false, errs.ErrAttributeLabelRefer
	}
	if qa.Source == model.SourceFromDoc {
		return nil, nil, false, nil
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = model.AttrRangeCondition
	} else {
		req.AttrRange = model.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, robotID, config.App().AttributeLabel.QAAttrLimit,
		config.App().AttributeLabel.QAAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, nil, false, err
	}
	isAttributeLabelReferChange, err := s.isQAAttributeLabelChange(ctx, robotID, qa.ID, qa.AttrRange,
		req.GetAttrRange(), req.GetAttrLabels(), attrs, labels)
	if err != nil {
		return nil, nil, false, err
	}
	return attrs, labels, isAttributeLabelReferChange, nil
}

// DeleteQA 删除QA
func (s *Service) DeleteQA(ctx context.Context, req *pb.DeleteQAReq) (*pb.DeleteQARsp, error) {
	log.InfoContextf(ctx, "DeleteQA Req:%+v", req)
	// 先加锁，防止重复删除
	qaBizIds, err := util.CheckReqSliceUint64(ctx, req.GetQaBizIds())
	if err != nil {
		return nil, err
	}
	bizIds := slicex.Unique(qaBizIds)
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteQa, 2*time.Second, bizIds)
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteQa, bizIds)
	if err != nil {
		return nil, errs.ErrQaIsModifyingOrDeleting
	}

	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return nil, err
	}
	details, err := s.dao.GetQADetailsByBizIDs(ctx, corpID, app.ID, bizIds)
	if err != nil || len(details) == 0 {
		return nil, errs.ErrQANotFound
	}
	// ps 这里不判断qa关联的文档和分组是否在删除流程中，重复删除不影响最终数据
	notDeletedQA := make([]*model.DocQA, 0, len(details))
	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrGetReleaseFail
	}
	for _, detail := range details {
		if detail.IsDelete() {
			continue
		}
		if !detail.IsAllowDelete() {
			continue
		}
		if !app.IsShared && detail.ReleaseStatus == model.QAReleaseStatusSuccess &&
			releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
			return nil, errs.ErrReleaseQaMaxCount
		}
		notDeletedQA = append(notDeletedQA, detail)
	}
	if len(notDeletedQA) == 0 {
		return nil, errs.ErrQaForbidDelete
	}
	qaIds := make([]uint64, 0, len(notDeletedQA))
	for _, item := range notDeletedQA {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, app.ID, qaIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	for _, qa := range notDeletedQA {
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			return nil, errs.ErrQAIsPendingRelease
		}
	}
	if err = s.dao.DeleteQAs(ctx, corpID, app.ID, staffID, notDeletedQA); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.QaEventDel, corpID, app.GetAppID(), req, nil, nil, nil)
	return &pb.DeleteQARsp{}, nil
}

// VerifyQA 验证QA
func (s *Service) VerifyQA(ctx context.Context, req *pb.VerifyQAReq) (*pb.VerifyQARsp, error) {
	log.InfoContextf(ctx, "VerifyQA Req:%+v", req)
	var err error
	var botBizId uint64
	if botBizId, err = checker.VerifyQaChecker(ctx, req); err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizId)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	corpID := pkg.CorpID(ctx)
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return nil, err
	}

	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrGetReleaseFail
	}

	var ids []uint64
	var cateBizIDs []uint64
	for _, v := range req.GetList() {
		if v.GetCateBizId() != "" {
			cateBizID, err := util.CheckReqParamsIsUint64(ctx, v.GetCateBizId())
			if err != nil {
				return nil, err
			}
			cateBizIDs = append(cateBizIDs, cateBizID)
		}
		qaBizID, err := util.CheckReqParamsIsUint64(ctx, v.GetQaBizId())
		if err != nil {
			return nil, err
		}
		if !app.IsShared && v.IsAccepted && releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
			return nil, errs.ErrReleaseQaMaxCount
		}
		if err = util.CheckMarkdownImageURL(ctx, v.GetAnswer(), pkg.Uin(ctx), app.BusinessID, nil); err != nil {
			return nil, err
		}
		ids = append(ids, qaBizID)
	}
	ids = slicex.Unique(ids)
	cateBizIDs = slicex.Unique(cateBizIDs)
	cateList, err := s.dao.GetCateListByBusinessIDs(ctx, model.QACate, corpID, app.ID, cateBizIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qas, addCharSize, err := s.checkCloudAPIVerifyQA(ctx, ids, req.GetList(), app, cateList)
	if err != nil {
		return nil, err
	}
	staffID := pkg.StaffID(ctx)
	for _, qa := range qas {
		qa.StaffID = staffID
	}
	if err := s.dao.VerifyQA(ctx, qas, app.ID, addCharSize); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.QaEventVerify, corpID, app.GetAppID(), req, nil, nil, nil)

	return &pb.VerifyQARsp{}, nil
}

// ModifyQAStatus 修改QA状态
func (s *Service) ModifyQAStatus(ctx context.Context, req *pb.ModifyQAStatusReq) (*pb.ModifyQAStatusRsp, error) {
	log.InfoContextf(ctx, "ModifyQAStatus Req:%+v", util.Object2String(req))
	rsp := new(pb.ModifyQAStatusRsp)
	// 先加锁，防止重复更新
	qaBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetQaBizId())
	if err != nil {
		return nil, err
	}
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteQa, 2*time.Second, []uint64{qaBizID})
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteQa, []uint64{qaBizID})
	if err != nil {
		return nil, errs.ErrQaIsModifyingOrDeleting
	}

	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil || app == nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}
	qa, err := s.dao.GetQADetailsByBizID(ctx, corpID, app.ID, qaBizID)
	if err != nil {
		return rsp, errs.ErrQANotFound
	}
	if qa.IsDelete() {
		return rsp, errs.ErrQAIsDeleted
	}
	if !qa.IsAllowEdit() {
		return rsp, errs.ErrQANotAllowEdit
	}
	if qa.IsDisable() && req.GetIsDisabled() {
		return rsp, errs.ErrQAIsDisabled
	}
	if !qa.IsDisable() && !req.GetIsDisabled() {
		return rsp, errs.ErrQAIsEnabled
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, app.ID, []uint64{qa.ID})
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	if _, ok := releasingQaIdMap[qa.ID]; ok {
		return rsp, errs.ErrQAIsPendingRelease
	}
	isAllSimQuestionsNeedPublish := true
	qa.ReleaseStatus = model.QAReleaseStatusLearning
	qa.NextAction = model.NextActionAdd
	if req.GetIsDisabled() {
		qa.NextAction = model.NextActionDelete
	}
	var sqm *model.SimilarQuestionModifyInfo
	// 如果主问的过期时间、属性标签等全局信息发生变更，则需要发布所有相似问
	if isAllSimQuestionsNeedPublish {
		sqm, err = s.dao.NewSimilarQuestionsFromDBAndReq(ctx, qa, nil, true)
	}
	if err != nil {
		return nil, errs.ErrSystem
	}
	qa.StaffID = pkg.StaffID(ctx)
	if err = s.dao.UpdateQADisableState(ctx, qa, sqm, req.GetIsDisabled()); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.QaEventEdit, corpID, app.GetAppID(), req, rsp, qa, qa)
	return rsp, nil
}

// GroupQA QA分组
func (s *Service) GroupQA(ctx context.Context, req *pb.GroupQAReq) (*pb.GroupQARsp, error) {
	log.InfoContextf(ctx, "GroupQA Req:%+v", req)
	rsp := new(pb.GroupQARsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrGetReleaseFail
	}
	if !app.IsShared && releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
		return nil, errs.ErrReleaseQaMaxCount
	}
	var cateID uint64
	if req.GetCateId() != "" {
		// Deprecated
		cateIDUint64, err := util.CheckReqParamsIsUint64(ctx, req.GetCateId())
		if err != nil {
			return nil, err
		}
		if err := s.dao.CheckCate(ctx, model.QACate, corpID, cateIDUint64, app.ID); err != nil {
			return rsp, errs.ErrCateNotFound
		}
	} else {
		// 保留
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		if cateID, err = s.dao.CheckCateBiz(ctx, model.QACate, corpID, cateBizID, app.ID); err != nil {
			return rsp, errs.ErrCateNotFound
		}
	}
	var details map[uint64]*model.DocQA
	var qaIDs []uint64
	if len(req.GetIds()) > 0 {
		// Deprecated
		reqIDs, err := util.CheckReqSliceUint64(ctx, req.GetIds())
		if err != nil {
			return nil, err
		}
		ids := slicex.Unique(reqIDs)
		details, err = s.dao.GetQADetails(ctx, corpID, app.ID, ids)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
		qaIDs = maps.Keys(details)
	} else {
		// 保留
		ids := slicex.Unique(req.GetQaBizIds())
		details, err = s.dao.GetQADetailsByBizIDs(ctx, corpID, app.ID, ids)
		qaIDs = listQAIDs(details)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
	}
	if err = dao.GetCateDao(model.QACate).GroupCateObject(ctx, s.dao, model.QACate, qaIDs, cateID, app); err != nil {
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// listQAIDs 获取QA ID
func listQAIDs(details map[uint64]*model.DocQA) []uint64 {
	values := maps.Values(details)
	var qaIDs []uint64
	for _, value := range values {
		qaIDs = append(qaIDs, value.ID)
	}
	return qaIDs
}

// saveQaSimilar 保存相似问答对
func (s *Service) saveQaSimilar(ctx context.Context, qa *model.DocQA, searchVector *config.SearchVector,
	embeddingVersion uint64, appBizID uint64) (
	err error) {
	latestQA, err := s.dao.GetQAByID(ctx, qa.ID)
	if err != nil {
		log.ErrorContextf(ctx, "查询相似问答对失败,ID:%d err:%+v", qa.ID, err)
		return
	}
	if latestQA.Question != qa.Question {
		log.WarnContextf(ctx, "问答对问题已更新 跳过不处理 id:%d", qa.ID)
		return
	}
	// search获取问答对相似问答对
	filters := make([]*retrieval.SearchVectorReq_Filter, 0, 1)
	filters = append(filters, &retrieval.SearchVectorReq_Filter{
		IndexId:    model.SimilarVersionID,
		Confidence: searchVector.Confidence,
		TopN:       searchVector.TopN,
		DocType:    model.DocTypeQA,
	})
	req := &retrieval.SearchVectorReq{
		RobotId:          qa.RobotID,
		BotBizId:         appBizID,
		Question:         qa.Question,
		Filters:          filters,
		TopN:             searchVector.TopN,
		EmbeddingVersion: embeddingVersion,
		Rerank:           &retrieval.SearchVectorReq_Rerank{Enable: false},
	}
	similarID := qa.ID
	rsp := &retrieval.SearchVectorRsp{}
	for i := 0; i < 4; i++ {
		rsp, err = client.SearchVector(ctx, req)
		log.DebugContextf(ctx, "saveQaSimilar retry:%d req:%+v rsp:%+v err:%+v", i, req, rsp, err)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.ErrorContextf(ctx, "saveQaSimilar req:%+v rsp:%+v err:%+v", req, rsp, err)
		return err
	}
	now := time.Now()
	for _, doc := range rsp.Docs {
		if doc.Id == similarID {
			continue
		}
		// 判断当前qaID是否存在
		if _, err := s.dao.GetQADetail(ctx, qa.CorpID, qa.RobotID, doc.Id); err != nil {
			continue
		}
		// 存储相似问答对
		_ = s.dao.SaveQaSimilar(ctx, &model.DocQASimilar{
			RobotID:    qa.RobotID,
			CorpID:     qa.CorpID,
			StaffID:    qa.StaffID,
			DocID:      qa.DocID,
			QaID:       doc.Id,
			SimilarID:  qa.ID,
			IsValid:    model.QaSimilarIsValid,
			CreateTime: now,
			UpdateTime: now,
			Status:     model.QaSimilarStatusInit,
		})
	}
	return
}

// ExportQAList 导出QA
func (s *Service) ExportQAList(ctx context.Context, req *pb.ExportQAListReq) (*pb.ExportQAListRsp, error) {
	log.InfoContextf(ctx, "ExportQAList Req:%+v", req)
	rsp := new(pb.ExportQAListRsp)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrExportQA
	}

	_, err = util.CheckReqSliceUint64(ctx, req.GetQaBizIds())
	if err != nil {
		return rsp, err
	}

	// 检查导出数量
	if len(req.GetQaBizIds()) > 0 { // 按ID导出
		if len(req.GetQaBizIds()) > int(config.App().CronTask.ExportQATask.MaxQACount) {
			return rsp, errs.ErrExportQATooMany
		}
	} else { // 按筛选器导出
		l, err := s.ListQA(ctx, req.GetFilters())
		if err != nil {
			log.ErrorContextf(ctx, "导出QA失败, 获取总数失败, req: %+v, err: %+v", req, err)
			return rsp, errs.ErrExportQA
		}
		if l.GetTotal() > uint64(config.App().CronTask.ExportQATask.MaxQACount) {
			return rsp, errs.ErrExportQATooMany
		}
	}

	paramStr, err := jsoniter.MarshalToString(req)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	export := model.Export{
		CorpID:        corpID,
		RobotID:       app.ID,
		CreateStaffID: staffID,
		TaskType:      model.ExportQaTaskType,
		Name:          model.ExportQaTaskName,
		Params:        paramStr,
		Status:        model.TaskExportStatusInit,
		UpdateTime:    now,
		CreateTime:    now,
	}

	params := model.ExportParams{
		CorpID:           corpID,
		RobotID:          app.ID,
		CreateStaffID:    staffID,
		TaskType:         model.ExportQaTaskType,
		TaskName:         model.ExportQaTaskName,
		Params:           paramStr,
		NoticeContent:    i18n.Translate(ctx, model.ExportQANoticeContent),
		NoticePageID:     model.NoticeQAPageID,
		NoticeTypeExport: model.NoticeTypeQAExport,
		NoticeContentIng: i18n.Translate(ctx, model.ExportQANoticeContentIng),
	}

	if _, err = s.dao.CreateExportTask(ctx, corpID, staffID, app.ID, export, params); err != nil {
		return rsp, err
	}

	return rsp, nil
}

// RecordUserFirstGenQA 记录操作首次生成问答标记
func (s *Service) RecordUserFirstGenQA(ctx context.Context, req *pb.RecordUserFirstGenQAReq) (
	*pb.RecordUserFirstGenQARsp, error) {
	rsp := new(pb.RecordUserFirstGenQARsp)
	staffID := pkg.StaffID(ctx)
	cropStaff, err := s.dao.GetStaffByID(ctx, staffID)
	if err != nil || cropStaff == nil {
		return nil, errs.ErrUserNotFound
	}
	err = s.dao.UpdateCorpStaffGenQA(ctx, cropStaff)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

// RecordUserAccessUnCheckQATime 记录访问未检验问答时间
func (s *Service) RecordUserAccessUnCheckQATime(ctx context.Context, req *pb.RecordUserAccessUnCheckQATimeReq) (
	*pb.RecordUserAccessUnCheckQATimeRsp, error) {
	rsp := new(pb.RecordUserAccessUnCheckQATimeRsp)
	staffID := pkg.StaffID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	cropStaff, err := s.dao.GetStaffByID(ctx, staffID)
	if err != nil || cropStaff == nil {
		return nil, errs.ErrUserNotFound
	}
	err = s.dao.RecordUserAccessUnCheckQATime(ctx, app.ID, staffID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	return rsp, nil
}

// checkCloudAPIVerifyQA 校验问答前检查qa
func (s *Service) checkCloudAPIVerifyQA(ctx context.Context, ids []uint64, list []*pb.VerifyQAReq_QAList,
	app *model.App, cateList map[uint64]*model.CateInfo) ([]*model.DocQA, uint64, error) {
	var qaSlice []*model.DocQA
	qas, err := s.dao.GetQADetailsByBizIDs(ctx, app.CorpID, app.ID, ids)
	log.DebugContextf(ctx, "get qa details success,qas:%+v", qas)
	if err != nil {
		return nil, 0, errs.ErrQANotFound
	}
	qaIds := make([]uint64, 0, len(qas))
	for _, item := range qas {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, app.ID, qaIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, 0, errs.ErrSystem
	}
	var addCharSize uint64
	for _, item := range list {
		qaBizID, err := util.CheckReqParamsIsUint64(ctx, item.GetQaBizId())
		if err != nil {
			return nil, 0, err
		}
		qa, ok := qas[qaBizID]
		if !ok {
			continue
		}
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			return nil, 0, errs.ErrQAIsPendingRelease
		}
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, item.GetCateBizId())
		if err != nil {
			return nil, 0, err
		}
		if cate, ok := cateList[cateBizID]; ok {
			qa.CategoryID = cate.ID
		}
		qa.AcceptStatus = utils.When(item.GetIsAccepted(), model.AcceptYes, model.AcceptNo)
		qa.Question = utils.When(len(strings.TrimSpace(item.GetQuestion())) > 0,
			strings.TrimSpace(item.GetQuestion()), qa.Question)
		err = util.CheckMarkdownImageURL(ctx, item.GetAnswer(), pkg.Uin(ctx), app.BusinessID, nil)
		if err != nil {
			log.WarnContextf(ctx, "checkCloudAPIVerifyQA Answer CheckQaImgURLSafeToMD err:%d", err)
			return nil, 0, err
		}
		qa.Answer = utils.When(len(strings.TrimSpace(item.GetAnswer())) > 0,
			strings.TrimSpace(item.GetAnswer()), qa.Answer)
		qa.CharSize = utils.When(len(strings.TrimSpace(item.GetQuestion())) > 0 ||
			len(strings.TrimSpace(item.GetAnswer())) > 0, uint64(utf8.RuneCountInString(item.GetQuestion()+
			item.GetAnswer())), qa.CharSize)
		if item.GetIsAccepted() {
			addCharSize += qa.CharSize
		}
		qaSlice = append(qaSlice, qa)
	}

	if err = CheckIsCharSizeExceeded(ctx, s.dao, app.BusinessID, app.CorpID, int64(addCharSize)); err != nil {
		return nil, 0, err
	}
	return qaSlice, addCharSize, nil
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
		case model.QAReleaseStatusCharExceeded:
			releaseStatus = append(releaseStatus, model.QAReleaseStatusCharExceeded,
				model.QAReleaseStatusAppealFailCharExceeded, model.QAReleaseStatusAuditNotPassCharExceeded,
				model.QAReleaseStatusLearnFailCharExceeded)
		case model.QAReleaseStatusResuming:
			releaseStatus = append(releaseStatus, model.QAReleaseStatusResuming,
				model.QAReleaseStatusAppealFailResuming, model.QAReleaseStatusAuditNotPassResuming,
				model.QAReleaseStatusLearnFailResuming)
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

// ModifyQAAttrRange 编辑QA适用范围
func (s *Service) ModifyQAAttrRange(ctx context.Context, req *pb.ModifyQAAttrRangeReq) (*pb.ModifyQAAttrRangeRsp,
	error) {
	log.InfoContextf(ctx, "ModifyQAAttrRange Req:%+v", req)
	rsp := new(pb.ModifyQAAttrRangeRsp)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	robot, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = robot.IsWriteable(); err != nil {
		return rsp, err
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = model.AttrRangeCondition
	} else {
		req.AttrRange = model.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, robot.ID, config.App().AttributeLabel.QAAttrLimit,
		config.App().AttributeLabel.QAAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return rsp, err
	}
	qaBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetQaBizIds())
	if err != nil {
		return nil, err
	}
	ids := slicex.Unique(qaBizIDs)
	qas, err := s.dao.GetQAsByBizIDs(ctx, corpID, robot.ID, ids, 0, uint64(len(ids)))
	if err != nil || len(qas) == 0 {
		return rsp, errs.ErrQANotFound
	}
	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, robot.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	qaIds := make([]uint64, 0, len(qas))
	for _, qa := range qas {
		if qa.IsDelete() {
			return rsp, errs.ErrQAIsDeleted
		}
		if !qa.IsAllowEdit() {
			return rsp, errs.ErrQANotAllowEdit
		}
		if !robot.IsShared && qa.ReleaseStatus == model.QAReleaseStatusSuccess &&
			releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
			return rsp, errs.ErrReleaseQaMaxCount
		}
		qaIds = append(qaIds, qa.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, robot.ID, qaIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	modifyQAs, err := s.filterAttributeLabelChangedQA(ctx, robot.ID, qaBizIDs, qas,
		req.GetAttrRange(), req.GetAttrLabels(), attrs, labels)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if len(modifyQAs) == 0 {
		return rsp, nil
	}
	for _, v := range modifyQAs {
		if v.Source == model.SourceFromDoc {
			return rsp, errs.ErrAttributeLabelDocQa
		}
		if _, ok := releasingQaIdMap[v.ID]; ok {
			return rsp, errs.ErrAttributeLabelDocQaSync
		}
		v.AttrRange = req.GetAttrRange()
		// 原先处于审核失败或者人工申诉失败，不修改qa状态
		if v.ReleaseStatus != model.QAReleaseStatusAuditNotPass &&
			v.ReleaseStatus != model.QAReleaseStatusAppealFail {
			v.ReleaseStatus = model.QAReleaseStatusLearning
		}
		v.SimilarStatus = model.SimilarStatusInit
		v.NextAction = model.NextActionUpdate
		v.StaffID = staffID
	}
	attributeLabelPB, err := fillQAAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}
	if err = s.dao.UpdateQAAttrRange(ctx, modifyQAs, attributeLabelPB); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.QaEventEdit, corpID, robot.GetAppID(), req, rsp, nil, nil)
	return rsp, nil
}

// BatchModifyQaExpire 批量修改问答过期时间
func (s *Service) BatchModifyQaExpire(ctx context.Context, req *pb.BatchModifyQaExpireReq) (*pb.BatchModifyQaExpireRsp,
	error) {
	log.InfoContextf(ctx, "BatchModifyQaExpire Req:%+v", req)
	rsp := new(pb.BatchModifyQaExpireRsp)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	if len(req.GetQaBizIds()) == 0 || req.GetExpireEnd() == "" || req.GetBotBizId() == "" {
		return rsp, errs.ErrParams
	}
	qaBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetQaBizIds())
	if err != nil {
		return nil, err
	}
	_, expireEnd, err := util.CheckReqStartEndTime(ctx, "", req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}
	var updateDocQA []*model.DocQA
	var errorDocQA []*model.DocQA
	isFailQa := false
	qas, err := s.dao.GetQADetailsByBizIDs(ctx, app.CorpID, app.ID, qaBizIDs)
	qaIds := make([]uint64, 0, len(qas))
	for _, item := range qas {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, app.ID, qaIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	for _, qaBizId := range qaBizIDs {
		qa, err := s.dao.GetQADetailsByBizID(ctx, corpID, app.ID, qaBizId)
		if err != nil {
			log.ErrorContextf(ctx, "BatchModifyQaExpire|GetQADetailsByBizID|qa|%v|err:%+v", qa, err)
			isFailQa = true
			continue
		}
		if !app.IsShared && qa.ReleaseStatus == model.QAReleaseStatusSuccess &&
			releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
			return rsp, errs.ErrReleaseQaMaxCount
		}
		if qa.Source == model.SourceFromDoc {
			log.InfoContextf(ctx, "BatchModifyQaExpire|SourceFromDoc|qa|%v|err:%+v", qa,
				errs.ErrModifyQaExpireFail)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if qa.IsDelete() {
			log.InfoContextf(ctx, "BatchModifyQaExpire|IsDelete|%v|qa:%+v", qa.IsDelete(), qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if !qa.IsAllowEdit() {
			log.InfoContextf(ctx, "BatchModifyQaExpire|is not allow edit|qa:%+v", qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			log.InfoContextf(ctx, "BatchModifyQaExpire|pendingReleaseQA|qa:%v|err:%v",
				qa, errs.ErrQAIsPendingRelease)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		qa.ExpireEnd = time.Unix(int64(expireEnd), 0)
		qa.StaffID = staffID
		updateDocQA = append(updateDocQA, qa)
	}

	if len(errorDocQA) > 0 {
		log.InfoContextf(ctx, "BatchModifyQaExpire|errorDocQA|%v", errorDocQA)
	}
	if len(updateDocQA) == 0 {
		return rsp, errs.ErrModifyQaExpireFail
	}

	if err = s.dao.UpdateQAsExpire(ctx, updateDocQA); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.QaEventEdit, corpID, app.ID, req, rsp, nil, nil)
	if isFailQa {
		return rsp, errs.ErrModifyQaExpireFail
	}
	return rsp, nil
}

// BatchModifyQaDoc 批量修改问答关联文档
func (s *Service) BatchModifyQaDoc(ctx context.Context, req *pb.BatchModifyQaDocReq) (*pb.BatchModifyQaDocRsp,
	error) {
	rsp := new(pb.BatchModifyQaDocRsp)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	if len(req.GetQaBizIds()) == 0 || req.GetDocBizId() == "" {
		return rsp, errs.ErrParams
	}
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	qaBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetQaBizIds())
	if err != nil {
		return nil, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	docID, err := s.validateDocAndRetrieveID(ctx, docBizID, app.ID)
	if err != nil {
		return rsp, errs.ErrDocNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}
	var updateDocQA []*model.DocQA
	var errorDocQA []*model.DocQA
	isFailQa := false
	qas, err := s.dao.GetQADetailsByBizIDs(ctx, app.CorpID, app.ID, qaBizIDs)
	qaIds := make([]uint64, 0, len(qas))
	for _, item := range qas {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := logicDocQa.GetReleasingQaId(ctx, app.ID, qaIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	log.InfoContextf(ctx, "BatchModifyQaDoc|releaseCount|%d", releaseCount)
	for _, qaBizId := range qaBizIDs {
		qa, err := s.dao.GetQADetailsByBizID(ctx, corpID, app.ID, qaBizId)
		if err != nil {
			log.ErrorContextf(ctx, "BatchModifyQaDoc|GetQADetailsByBizID|qa|%v|err:%+v", qa, err)
			isFailQa = true
			continue
		}
		if !app.IsShared && qa.ReleaseStatus == model.QAReleaseStatusSuccess &&
			releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
			return rsp, errs.ErrReleaseQaMaxCount
		}
		if qa.Source == model.SourceFromDoc && qa.DocID != docID {
			log.InfoContextf(ctx, "BatchModifyQaDoc|SourceFromDoc|qa|%v|err:%+v", qa, errs.ErrReferDocFail)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if qa.IsDelete() {
			log.InfoContextf(ctx, "BatchModifyQaDoc|IsDelete|%v|qa:%+v", qa.IsDelete(), qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if !qa.IsAllowEdit() {
			log.InfoContextf(ctx, "BatchModifyQaDoc|is not allow edit|qa:%+v", qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			log.ErrorContextf(ctx, "BatchModifyQaDoc|pendingReleaseQA|qa:%v|err:%v",
				qa, errs.ErrQAIsPendingRelease)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		qa.DocID = docID
		qa.StaffID = staffID
		updateDocQA = append(updateDocQA, qa)
	}
	if len(errorDocQA) > 0 {
		log.InfoContextf(ctx, "BatchModifyQaDoc|errorDocQA|%v", errorDocQA)
	}
	if len(updateDocQA) == 0 {
		return rsp, errs.ErrModifyQaDocFail
	}

	if err = s.dao.UpdateQAsDoc(ctx, updateDocQA); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.QaEventEdit, corpID, app.ID, req, rsp, nil, nil)
	if isFailQa {
		return rsp, errs.ErrModifyQaDocFail
	}
	return rsp, nil
}

// ResumeQA QA 超量恢复
func (s *Service) ResumeQA(ctx context.Context, req *pb.ResumeQAReq) (*pb.ResumeQARsp, error) {
	log.DebugContextf(ctx, "恢复问答 REQ: %+v", req)
	if config.App().DocQA.ResumeMaxCountLimit != 0 && len(req.GetQaBizIds()) > config.App().DocQA.ResumeMaxCountLimit {
		return nil, errs.ErrResumeQAMaxCountLimit
	}
	rsp := new(pb.ResumeQARsp)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		log.ErrorContextf(ctx, "恢复问答失败 CheckReqBotBizIDUint64 err: %+v", err)
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		log.ErrorContextf(ctx, "恢复问答失败 GetAppByAppBizIDerr: %+v", err)
		return nil, err
	}
	// 字符数超限不可执行
	if err = CheckIsUsedCharSizeExceeded(ctx, s.dao, botBizID, corpID); err != nil {
		return rsp, s.dao.ConvertErrMsg(ctx, 0, app.CorpID, err)
	}
	// TODO: 实现超量恢复的逻辑
	log.DebugContext(ctx, "ResumeDoc REQ: ", req)
	qaBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetQaBizIds())
	if err != nil {
		log.ErrorContextf(ctx, "恢复问答失败 BatchCheckReqParamsIsUint64 err: %+v", err)
		return rsp, err
	}
	filter := &dao.DocQaFilter{
		CorpId:      corpID,
		RobotId:     app.ID,
		BusinessIds: qaBizIDs,
	}
	qas, err := dao.GetDocQaDao().GetAllDocQas(ctx, dao.DocQaTblColList, filter)
	// qas, err := s.dao.GetQAsByBizIDs(ctx, corpID, robotID, qaBizIDs, 0, uint64(len(qaBizIDs)))
	if err != nil {
		log.ErrorContextf(ctx, "恢复问答失败 GetDocByBizIDs err: %+v", err)
		return rsp, err
	}
	qaExceededTimes := make([]model.QAExceededTime, 0)
	for _, qa := range qas {
		if qa == nil {
			log.ErrorContextf(ctx, "恢复问答失败 qa is nil")
			continue
		}
		if !qa.IsCharExceeded() && !qa.IsResuming() {
			log.InfoContextf(ctx, "没有超量失败的问答，不做处理，qa: %+v", qa)
			continue
		}
		qaExceededTimes = append(qaExceededTimes, model.QAExceededTime{
			BizID:      qa.BusinessID,
			UpdateTime: qa.UpdateTime,
		})
		qa.StaffID = staffID
		if err := s.resumeQA(ctx, qa); err != nil {
			log.ErrorContextf(ctx, "恢复部分问答失败 resumeQA err: %+v", err)
			continue
		}
	}
	if len(qaExceededTimes) != 0 {
		if err := s.dao.CreateQAResumeTask(ctx, corpID, app.ID, staffID, qaExceededTimes); err != nil {
			log.ErrorContextf(ctx, "恢复问答失败 CreateDocResumeTask err: %+v", err)
			return rsp, err
		}
	}
	return rsp, nil
}

func (s *Service) resumeQA(ctx context.Context, qa *model.DocQA) error {
	switch qa.ReleaseStatus {
	case model.QAReleaseStatusCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusResuming
	case model.QAReleaseStatusAppealFailCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusAppealFailResuming
	case model.QAReleaseStatusAuditNotPassCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusAuditNotPassResuming
	case model.QAReleaseStatusLearnFailCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusLearnFailResuming
	default:
		// 不可恢复
		return nil
	}
	// 增加相似问的超量恢复的逻辑
	sqs, err := s.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx,
			"bot: %d, qaID: %d, GetSimilarQuestionsByQA err: %+v", qa.RobotID, qa.ID, err)
		// 柔性放过
	}
	log.DebugContextf(ctx, "update QA(%d) and SimilarQuestions", qa.ID)

	sqm := &model.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	if err := s.dao.UpdateQA(ctx, qa, sqm, true, false, 0, &model.UpdateQAAttributeLabelReq{}); err != nil {
		return err
	}
	log.DebugContextf(ctx, "resume QA and SimilarQuestions, qa:%+v, sqm:%+v", qa, sqm)
	return nil
}

// RetryQaTask 重试或继续生成qa任务
func (s *Service) RetryQaTask(ctx context.Context, req *pb.RetryQaTaskReq) (*pb.RetryQaTaskRsp, error) {
	log.InfoContextf(ctx, "RetryQaTask: %+v", req)
	rsp := new(pb.RetryQaTaskRsp)
	if req.GetBotBizId() == "" || req.GetTaskId() == "" {
		return nil, errs.ErrParams
	}
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	taskID, err := util.CheckReqParamsIsUint64(ctx, req.GetTaskId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	docQATask, err := s.dao.GetDocQATaskByBusinessID(ctx, taskID, corpID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "RetryQaTask|GetDocQATaskByBusinessID|获取生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "RetryQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, app.ID, taskID)
		return rsp, errs.ErrDocQaTaskNotFound
	}
	if docQATask.SourceID != 0 && !req.GetIsContinue() {
		return nil, errs.ErrDocDiffQaNotRetry
	}

	if req.GetIsContinue() { // 暂停后继续的操作
		if docQATask.SourceID != 0 {
			// 如果是doc diff创建的任务
			err = doc_diff_task.ContinueDocDiffTask(ctx, docQATask.SourceID, corpID, app.ID, app.BusinessID, taskID, s.dao)
			if err != nil {
				return nil, err
			}
			return rsp, nil
		}

		if !docQATask.DocQATaskIsContinue() {
			log.ErrorContextf(ctx, "RetryQaTask|任务当前状态不可继续|corpID|%d|robotID%d|id|%d|docQATask|%+v ",
				corpID, app.ID, taskID, docQATask)
			return rsp, errs.ErrContinueQaTaskStatusFail
		}
		// 继续逻辑
		err = s.dao.ContinueQaTask(ctx, corpID, app.ID, docQATask, app.BusinessID)
		if err != nil {
			log.ErrorContextf(ctx, "RetryQaTask|ContinueQaTask|取消任务失败 err:%+v", err)
			return rsp, err
		}
		return rsp, nil
	}
	if docQATask.Status != model.DocQATaskStatusFail {
		log.ErrorContextf(ctx, "RetryQaTask|任务当前状态不可重试 docQATask:%+v", docQATask)
		return rsp, errs.ErrRetryQaTaskStatusFail
	}
	// 目前失败后重试的逻辑也从失败的切片开始,复用继续逻辑
	err = s.dao.ContinueQaTask(ctx, corpID, app.ID, docQATask, app.BusinessID)
	if err != nil {
		log.ErrorContextf(ctx, "RetryQaTask|重试任务失败 err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

// ListQaTask 生成问答任务列表
func (s *Service) ListQaTask(ctx context.Context, req *pb.ListQaTaskReq) (*pb.ListQaTaskRsp, error) {
	log.InfoContextf(ctx, "ListQaTaskReq: %+v", req)
	rsp := new(pb.ListQaTaskRsp)
	corpID := pkg.CorpID(ctx)
	if req.GetBotBizId() == "" {
		return nil, errs.ErrParams
	}
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	listQaTasReq, err := s.getListQaTaskReq(req, corpID, app.ID)
	if err != nil {
		return rsp, err
	}
	total, tasks, err := s.dao.GetListQaTask(ctx, listQaTasReq)
	if err != nil {
		return rsp, errs.ErrGetListQaTaskFail
	}
	docIDs := make([]uint64, 0)
	for _, task := range tasks {
		docIDs = append(docIDs, task.DocID)
	}
	docs, err := s.dao.GetDocByIDs(ctx, docIDs, app.ID)
	if err != nil {
		return rsp, errs.ErrGetDocListFail
	}
	docMap := make(map[uint64]*model.Doc, len(docs))
	for _, doc := range docs {
		docMap[doc.ID] = doc
	}
	rsp.Total = total
	rsp.List = make([]*pb.ListQaTaskRsp_Task, 0, len(tasks))
	for _, task := range tasks {
		var sourceType uint32 = model.DocQaTaskSourceTypeOrigin
		if task.SourceID != 0 {
			// 当前来源只有doc to qa task和 doc diff任务
			sourceType = model.DocQaTaskSourceTypeDocDiff
		}

		item := &pb.ListQaTaskRsp_Task{
			FileName:         task.DocName,
			FileType:         task.DocType,
			Status:           uint32(task.Status),
			QaCount:          uint32(task.QACount),
			SegmentCount:     uint32(task.SegmentCount),
			SegmentCountDone: uint32(task.SegmentCountDone),
			InputToken:       task.InputToken,
			OutputToken:      task.OutputToken,
			TaskId:           task.BusinessID,
			TaskMessage:      task.Message,
			SourceType:       sourceType,
			SourceId:         task.SourceID,
		}
		if doc, ok := docMap[task.DocID]; ok {
			item.DocBizId = doc.BusinessID
		}
		rsp.List = append(rsp.List, item)
	}
	return rsp, nil
}

// getListQaTaskReq 获取生成问答任务列表请求参数
func (s *Service) getListQaTaskReq(req *pb.ListQaTaskReq, corpID, robotID uint64) (*model.ListQaTaskReq, error) {
	return &model.ListQaTaskReq{
		CorpID:   corpID,
		RobotID:  robotID,
		Page:     req.GetPageNumber(),
		PageSize: req.GetPageSize(),
	}, nil
}

// DeleteQaTask 删除生成问答任务
func (s *Service) DeleteQaTask(ctx context.Context, req *pb.DeleteQaTaskReq) (*pb.DeleteQaTaskRsp, error) {
	log.InfoContextf(ctx, "DeleteQaTaskReq: %+v", req)
	rsp := new(pb.DeleteQaTaskRsp)
	corpID := pkg.CorpID(ctx)
	if req.GetBotBizId() == "" || req.GetTaskId() == "" {
		return nil, errs.ErrParams
	}
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	taskID, err := util.CheckReqParamsIsUint64(ctx, req.GetTaskId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	docQATask, err := s.dao.GetDocQATaskByBusinessID(ctx, taskID, corpID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteQaTask|GetDocQATaskByBusinessID|获取生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "DeleteQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, app.ID, taskID)
		return rsp, errs.ErrDocQaTaskNotFound
	}
	// 判断status状态是否合法,仅 成功、失败、手动取消状态可删除
	if docQATask.Status != model.DocQATaskStatusFail && docQATask.Status != model.DocQATaskStatusCancel &&
		docQATask.Status != model.DocQATaskStatusSuccess {
		return rsp, errs.ErrDeleteQaTaskStatusFail
	}
	err = s.dao.DeleteQaTask(ctx, corpID, app.ID, docQATask.ID)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteQaTask删除生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}

	log.InfoContextf(ctx, "DeleteQaTask success: botBizID:%d|taskID:%d", botBizID, taskID)
	return rsp, nil
}

// StopQaTask 暂停或取消任务
func (s *Service) StopQaTask(ctx context.Context, req *pb.StopQaTaskReq) (*pb.StopQaTaskRsp, error) {
	log.InfoContextf(ctx, "StopQaTask: %+v", req)
	rsp := new(pb.StopQaTaskRsp)
	corpID := pkg.CorpID(ctx)
	if req.GetBotBizId() == "" || req.GetTaskId() == "" {
		return nil, errs.ErrParams
	}
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	taskID, err := util.CheckReqParamsIsUint64(ctx, req.GetTaskId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	docQATask, err := s.dao.GetDocQATaskByBusinessID(ctx, taskID, corpID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "StopQaTask|GetDocQATaskByBusinessID|获取生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "StopQaTask|获取生成问答任务不存在|corpID|%d|robotID%d|id|%d ",
			corpID, app.ID, taskID)
		return rsp, errs.ErrDocQaTaskNotFound
	}
	if req.GetIsCancel() {
		if !docQATask.DocQATaskIsCancel() {
			log.ErrorContextf(ctx, "StopQaTask|任务当前状态不可取消|corpID|%d|robotID%d|id|%d|docQATask|%+v ",
				corpID, app.ID, taskID, docQATask)
			return rsp, errs.ErrCancelQaTaskStatusFail
		}
		// 调用 取消逻辑
		err = s.dao.CancelQaTask(ctx, corpID, app.ID, docQATask.ID)
		if err != nil {
			log.ErrorContextf(ctx, "StopQaTask|CancelQaTask|取消任务失败 err:%+v", err)
			return rsp, err
		}
		return rsp, nil
	}
	if !docQATask.DocQATaskIsStop() {
		log.ErrorContextf(ctx, "StopQaTask|任务当前状态不可暂停|corpID|%d|robotID%d|id|%d ",
			corpID, app.ID, taskID)
		return rsp, errs.ErrStopQaTaskStatusFail
	}

	err = s.dao.StopQaTask(ctx, corpID, app.ID, docQATask.ID, false, "")
	if err != nil {
		log.ErrorContextf(ctx, "StopQaTask暂停任务失败|err:%+v", err)
		return rsp, err
	}

	return rsp, nil
}

// SyncDeletedSimilarQuestion 相似问刷数据，db已删除，但是向量未删除，刷向量库
// 这是一次性的代码，后续应该删掉
func (s *Service) SyncDeletedSimilarQuestion(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("\nonly POST is allowed\n"))
		return
	}

	ctx := r.Context()
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("\nBody Read Failed, err:%+v\n", err)))
		return
	}
	type SyncDeletedSimilarQuestionReq struct {
		Days     int      `json:"days"`
		MaxID    uint64   `json:"max_id"`
		Limit    uint64   `json:"limit"`
		RobotID  []string `json:"robot_id"`
		Duration uint64   `json:"duration"` // 单位毫秒
	}
	syncReq := &SyncDeletedSimilarQuestionReq{}
	if err = jsoniter.Unmarshal(reqBody, syncReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(fmt.Sprintf("\nBody Unmarshal Failed, err:%+v\n", err)))
		return
	}

	if syncReq.Days <= 0 || syncReq.MaxID == 0 || syncReq.Limit == 0 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(fmt.Sprintf("\nBad Req:%+v\n", syncReq)))
		return
	}
	var botIDs []uint64
	for i := range syncReq.RobotID {
		var id uint64
		id, err = util.CheckReqBotBizIDUint64(ctx, syncReq.RobotID[i])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(fmt.Sprintf("\nCheckReqBotBizIDUint64 Failed, err:%v\n", err)))
			return
		}
		botIDs = append(botIDs, id)
	}

	requestID := uuid.NewString()
	ctx = log.WithContextFields(ctx, "RequestID", requestID)
	log.InfoContextf(ctx, "SyncDeletedSimilarQuestion syncReq:%+v", syncReq)
	now := time.Now()
	// 减去N天
	daysAgo := now.AddDate(0, 0, -1*syncReq.Days)
	var id uint64 = 0
	for id <= syncReq.MaxID {
		log.InfoContextf(ctx, "SyncDeletedSimilarQuestion id:%d, maxID:%d", id, syncReq.MaxID)
		var sims []*model.SimilarQuestion
		sims, err = s.dao.GetSimilarQuestionsByUpdateTime(ctx, daysAgo, now, syncReq.Limit, id, botIDs)
		if err != nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(fmt.Sprintf("\nGetSimilarQuestionsByUpdateTime error:%v\n", err)))
			return
		}
		// 处理相似问
		if len(sims) > 0 {
			// 添加同步任务
			if err = s.dao.AddSimilarQuestionSyncBatch(ctx, sims); err != nil {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(fmt.Sprintf("\n添加相似问同步任务失败 error:%v\n", err)))
				return
			}
		}
		id += syncReq.Limit
		d := time.Duration(syncReq.Duration)
		time.Sleep(time.Millisecond * d)
	}

	log.InfoContextf(ctx, "SyncDeletedSimilarQuestion success")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("\nOK, RequestID: %s\n", requestID)))
	return
}

// GenerateSimilarQuestions 生成相似问
func (s *Service) GenerateSimilarQuestions(ctx context.Context, req *pb.GenerateSimilarQuestionsReq) (
	*pb.GenerateSimilarQuestionsRsp, error) {
	log.InfoContextf(ctx, "GenerateSimilarQuestionsReq: %+v", req)
	rsp := new(pb.GenerateSimilarQuestionsRsp)
	if req.GetBotBizId() == "" {
		return nil, errs.ErrParams
	}
	if req.GetQuestion() == "" {
		return nil, errs.ErrGenerateSimilarParams
	}
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := client.GetAppInfo(ctx, botBizID, model.AppTestScenes)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	list, err := logicDocQa.GenerateSimilarQuestions(ctx, s.dao, app, req.Question, req.Answer)
	if err != nil {
		if errors.Is(err, errs.ErrNoTokenBalance) {
			return rsp, errs.ErrNoTokenBalance
		}
		log.ErrorContextf(ctx, "GenerateSimilarQuestions err:%+v", err)
		return rsp, errs.ErrSystem
	}
	rsp.Question = list
	return rsp, nil
}

func (s *Service) getShareKnowledgeValidityQACount(ctx context.Context, appBizID uint64) (uint64, error) {
	shareKnowledges, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, appBizID)
	if err != nil {
		return 0, err
	}
	knowledgesBizIDs := make([]uint64, 0, len(shareKnowledges))
	for _, v := range shareKnowledges {
		if v.KnowledgeBizID == appBizID {
			continue
		}
		knowledgesBizIDs = append(knowledgesBizIDs, v.KnowledgeBizID)
	}
	robots, err := s.dao.GetRobotList(ctx, 0, "", knowledgesBizIDs, 0, 1, uint32(len(knowledgesBizIDs)))
	if err != nil {
		return 0, err
	}
	knowledgeIDs := make([]uint64, 0, len(robots))
	for _, v := range robots {
		knowledgeIDs = append(knowledgeIDs, v.ID)
	}

	if len(knowledgeIDs) == 0 {
		return 0, nil
	}
	total, err := dao.GetDocQaDao().GetDocQACount(ctx, &dao.DocQaFilter{
		RobotIDs: knowledgeIDs,
	})
	log.DebugContextf(ctx, "getShareKnowledgeValidityQACount appBizID:%d robots:%v total:%d", appBizID, knowledgeIDs,
		total)
	return uint64(total), err
}

func (s *Service) getValidityDBTableCount(ctx context.Context, appBizID uint64) (uint64, error) {
	shareKnowledges, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, appBizID)
	if err != nil {
		return 0, err
	}
	knowledgesBizIDs := make([]uint64, 0, len(shareKnowledges))
	for _, v := range shareKnowledges {
		knowledgesBizIDs = append(knowledgesBizIDs, v.KnowledgeBizID)
	}

	if !slicex.Contains(knowledgesBizIDs, appBizID) {
		knowledgesBizIDs = append(knowledgesBizIDs, appBizID)
	}
	total, err := dao.GetDBTableDao().GetCountByAppBizIDs(ctx, knowledgesBizIDs)
	log.DebugContextf(ctx, "getValidityDBTableCount appBizID:%d knows:%v total:%d", appBizID, knowledgesBizIDs, total)
	return uint64(total), err
}
