package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/gox/stringx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	logicCommon "git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
	"github.com/spf13/cast"
	"golang.org/x/exp/maps"
	"gorm.io/gorm"
)

const (
	standSpace   = '\u0020'
	noBreakSpace = '\u00a0'
)

// ListQA 问答对列表
func (s *Service) ListQA(ctx context.Context, req *pb.ListQAReq) (*pb.ListQARsp, error) {
	logx.I(ctx, "ListQA Req:%+v", req)
	botBizID := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if req.GetQueryType() == "" {
		req.QueryType = docEntity.DocQueryTypeFileName
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
	qaListReq, err := s.getQaListReq(ctx, req, app.PrimaryId, app.CorpPrimaryId)
	if err != nil {
		return nil, err
	}
	if req.GetEnableScope() != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN {
		qaListReq.EnableScope = ptrx.Uint32(uint32(req.GetEnableScope()))
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
	mapQaID2AttrLabels, err := s.labelLogic.GetQAAttributeLabelDetail(ctx, app.PrimaryId, slicex.Pluck(list, (*qaEntity.DocQA).GetID))
	if err != nil {
		return nil, errs.ErrSystem
	}
	docs, err := s.getSourceDoc(ctx, list, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qaIds := make([]uint64, 0, len(list))
	for _, item := range list {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, qaIds)
	if err != nil {
		logx.E(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	qaList := make([]*pb.ListQARsp_QA, 0, len(list))
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	staffIDs, qaIDs := make([]uint64, 0, len(list)), make([]uint64, 0, len(list))
	for _, qa := range list {
		qaIDs = append(qaIDs, qa.ID)
		staffIDs = append(staffIDs, qa.StaffID)
	}
	qaCountMap, err := s.qaLogic.GetSimilarQuestionsCountByQAIDs(ctx, app.CorpPrimaryId, app.PrimaryId, qaIDs)
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionsCountByQAIDs failed, qaIDs(%+v) err:%+v", qaIDs, err)
		return nil, errs.ErrSystem
	}
	// 获取员工名称
	req2 := pm.DescribeCorpStaffListReq{
		Status:          []uint32{entity.CorpStatusValid},
		Page:            1,
		StaffPrimaryIds: staffIDs,
		PageSize:        uint32(len(staffIDs)),
		CorpId:          contextx.Metadata(ctx).CorpBizID(),
	}
	staffs, _, err := s.rpc.PlatformAdmin.DescribeCorpStaffList(ctx, &req2)
	if err != nil { // 失败降级为返回员工ID
		logx.E(ctx, "ListQA get staff name staffIDs:%v, error:%v", staffIDs, err)
	}
	staffIdMap := slicex.MapKV(staffs, func(i *entity.CorpStaff) (uint64, string) { return i.ID, i.NickName })
	for _, item := range list {
		_, isReleasing := releasingQaIdMap[item.ID]
		var fileName, fileType string
		var docBizID uint64
		var simCount uint32
		var docEnableScope uint32
		if doc, ok := docs[item.DocID]; ok {
			fileName = doc.GetFileNameByStatus()
			fileType = doc.FileType
			docBizID = doc.BusinessID
			docEnableScope = doc.EnableScope
		}
		if qaCountMap != nil {
			if val, ok := qaCountMap[item.ID]; ok {
				simCount = val
			} else {
				simCount = 0
			}
		}
		logx.I(ctx, "similarQuestionTips:%s", item.SimilarQuestionTips)

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
			IsDisabled:          false, // 知识库概念统一后该字段已废弃
			EnableScope:         pb.RetrievalEnableScope(item.EnableScope),
			DocEnableScope:      pb.RetrievalEnableScope(gox.IfElse(docEnableScope == 0, item.EnableScope, docEnableScope)),
			QaSize:              item.QaSize,
		}
		if staffName, ok := staffIdMap[item.StaffID]; ok { // 赋值员工名称
			qaNode.StaffName = staffName
		} else { // 没取到返回员工ID
			qaNode.StaffName = cast.ToString(item.StaffID)
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
		if req.GetShowCurrCate() == docEntity.ShowCurrCate { // 只展示当前分类的数据
			cateIDs = append(cateIDs, cateID)
		} else {
			cateIDs, err = s.getCateChildrenIDs(ctx, cateEntity.QACate, corpID, robotID, cateID)
			if err != nil {
				return nil, err
			}
		}
	}
	docID, err := s.qaLogic.ValidateDocAndRetrieveID(ctx, req.GetDocBizId(), robotID)
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
		ReleaseStatus:  slicex.Unique(releaseStatus),
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		DocID:          gox.IfElse(docID > 0, []uint64{docID}, nil),
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
	req := qaEntity.QAListReq{
		RobotID:        robotID,
		CorpID:         corpID,
		IsDeleted:      qaEntity.QAIsNotDeleted,
		ExcludeDocID:   deletingDocID,
		Page:           1,
		PageSize:       1,
		AcceptStatus:   []uint32{qaEntity.AcceptYes},
		ReleaseStatus:  s.getValidityReleaseStatus(),
		ValidityStatus: qaEntity.QaUnExpiredStatus,
	}
	_, _, _, total, err := s.qaLogic.GetQAListCount(ctx, &req)
	if err != nil {
		return 0, err
	}
	return uint64(total), nil
}

func (s *Service) getValidityReleaseStatus() []uint32 {
	return []uint32{
		qaEntity.QAReleaseStatusInit,
		qaEntity.QAReleaseStatusIng,
		qaEntity.QAReleaseStatusSuccess,
		qaEntity.QAReleaseStatusFail,
	}
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

func (s *Service) getPendingReleaseQA(ctx context.Context, robotID uint64, qas []*qaEntity.DocQA) (map[uint64]*releaseEntity.ReleaseQA, error) {
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

// getSourceCate 来源分类
func (s *Service) getSourceCate(ctx context.Context, qas []*qaEntity.DocQA) (map[uint64]*cateEntity.CateInfo, error) {
	cateIDs := make([]uint64, 0, len(qas))
	for _, qa := range qas {
		cateIDs = append(cateIDs, qa.CategoryID)
	}
	return s.cateLogic.DescribeCateByIDs(ctx, cateEntity.QACate, slicex.Unique(cateIDs))
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

// DescribeQA 获取QA详情
func (s *Service) DescribeQA(ctx context.Context, req *pb.DescribeQAReq) (*pb.DescribeQARsp, error) {
	logx.I(ctx, "DescribeQA Req:%+v", req)
	rsp := new(pb.DescribeQARsp)
	threshold := config.App().HighLightThreshold
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
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
	qa, err := s.qaLogic.GetQADetailsByBizID(ctx, app.CorpPrimaryId, app.PrimaryId, qaBizID)
	if err != nil {
		return rsp, err
	}
	if qa.IsDelete() {
		return rsp, errs.ErrQAIsNotExist
	}
	docs, err := s.getSourceDoc(ctx, []*qaEntity.DocQA{qa}, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrDocNotFound
	}
	cateList, err := s.cateLogic.DescribeCateList(ctx, cateEntity.QACate, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	tree := cateEntity.BuildCateTree(cateList)
	cateNamePath, cateBizIdPath := tree.Path(ctx, qa.CategoryID)
	logx.D(ctx, "get cate path--qaBizId:%d, cateNamePath:%+v, cateBizIdPath:%+v", qaBizID, cateNamePath, cateBizIdPath)
	if len(cateBizIdPath) == 0 {
		return nil, errs.ErrCateNotFound
	}

	sqs, err := s.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		// 伽利略error日志告警
		logx.E(ctx, "DescribeQA qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
		// 柔性放过
	}
	var segmentByID *segEntity.DocSegmentExtend
	if qa.Source != docEntity.SourceFromManual && qa.SegmentID > 0 {
		segmentByID, err = s.segLogic.GetSegmentByID(ctx, qa.SegmentID, app.PrimaryId)
		if err != nil {
			return rsp, errs.ErrSegmentNotFound
		}
		if segmentByID == nil { // 如果问答绑定的segment不存在，说明相应doc和segment已被删除，则返回空
			log.InfoContextf(ctx, "[DescribeQA] qa_id(%d): segment_id(%d) not existed in t_doc_segment", qa.ID, qa.SegmentID)
			segmentByID = &segEntity.DocSegmentExtend{}
		} else if segmentByID.OrgData == "" { // 如果旧表格没有orgData，则从t_doc_segment_org_data新表中获取orgData
			corpRsp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
			if err != nil || corpRsp == nil {
				return rsp, errs.ErrCorpNotFound
			}
			doc, err := s.docLogic.GetDocByID(ctx, segmentByID.DocID, segmentByID.RobotID)
			if err != nil {
				return rsp, err
			}
			if doc == nil {
				return rsp, errs.ErrDocNotFound
			}
			filter := &segEntity.DocSegmentOrgDataFilter{
				CorpBizID:      corpRsp.GetCorpId(),
				AppBizID:       app.BizId,
				DocBizID:       doc.BusinessID,
				BusinessIDs:    []uint64{segmentByID.OrgDataBizID},
				RouterAppBizID: app.BizId,
			}
			orgData, err := s.segLogic.GetDocOrgData(ctx,
				[]string{segEntity.DocSegmentOrgDataTblColOrgData}, filter)
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return rsp, err
			}
			if orgData != nil && orgData.OrgData != "" {
				segmentByID.OrgData = orgData.OrgData
			}
			logx.D(ctx, "task(DocToQA) Process GetDocOrgDataByBizID|segmentByID.OrgData:%s",
				segmentByID.OrgData)
		}
		// 待下个版本前端修改后可删除 @sinutelu
		// /qbot/admin/getQADetail
		//   page_content -> org_data
		rsp.PageContent = segmentByID.OrgData
		rsp.OrgData = segmentByID.OrgData
		rsp.SegmentBizId = segmentByID.BusinessID
		rsp.Highlights = entity.HighlightRefer(ctx, qa.Answer, segmentByID.OrgData, threshold)
	}
	mapQaID2AttrLabels, err := s.labelLogic.GetQAAttributeLabelDetail(ctx, app.PrimaryId, []uint64{qa.ID})
	if err != nil {
		return rsp, errs.ErrSystem
	}
	auditFailList, err := s.auditLogic.GetLatestAuditFailListByRelateID(ctx, app.CorpPrimaryId, qa.RobotID, qa.ID, releaseEntity.AuditBizTypeQa,
		false)
	if err != nil && err != errs.ErrAppealNotFound {
		logx.E(ctx, "DescribeQA qa:%+v, GetLatestAuditFailListByRelateID err: %+v", qa, err)
	}
	if len(auditFailList) > 0 {
		err = s.auditLogic.DescribeQaAuditFailStatus(ctx, qa, sqs, auditFailList, contextx.Metadata(ctx).Uin(), app.BizId)
		if err != nil {
			logx.E(ctx, "DescribeQA qa:%+v, DescribeQaAuditFailStatus err: %+v", qa, err)
		}
		logx.I(ctx, "DescribeQA, len(auditFailList):%+v, qa:%+v", len(auditFailList), qa)
	}
	rsp = s.fillDescribeQaRsp(ctx, qa, sqs, docs, cateNamePath, cateBizIdPath, mapQaID2AttrLabels, rsp)
	return rsp, nil
}

func (s *Service) fillDescribeQaRsp(ctx context.Context, qa *qaEntity.DocQA,
	sqs []*qaEntity.SimilarQuestion, docs map[uint64]*docEntity.Doc,
	cateNamePath []string, cateBizIdPath []uint64,
	mapQaID2AttrLabels map[uint64][]*labelEntity.AttrLabel,
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
	rsp.CateBizId = cateBizIdPath[len(cateBizIdPath)-1]
	rsp.DocBizId = qa.DocBizID(docs)
	rsp.IsAllowAccept = qa.IsAllowAccept()
	rsp.IsAllowEdit = qa.IsAllowEdit()
	rsp.IsAllowDelete = qa.IsAllowDelete()
	rsp.AttrRange = qa.AttrRange
	rsp.AttrLabels = fillPBAttrLabels(mapQaID2AttrLabels[qa.ID])
	rsp.ExpireStart = uint64(qa.ExpireStart.Unix())
	rsp.ExpireEnd = uint64(qa.ExpireEnd.Unix())
	rsp.DocEnableScope = pb.RetrievalEnableScope(qa.EnableScope)
	if doc, ok := docs[qa.DocID]; ok {
		rsp.FileName = doc.FileName
		if doc.Status == docEntity.DocStatusWaitRelease {
			doc.GetRealFileName()
		}
		rsp.FileType = doc.FileType
		rsp.DocEnableScope = pb.RetrievalEnableScope(doc.EnableScope)
	}
	rsp.QaAuditStatus = docEntity.FrontEndAuditPass
	rsp.PicAuditStatus = docEntity.FrontEndAuditPass
	rsp.VideoAuditStatus = docEntity.FrontEndAuditPass
	if qa.QaAuditFail {
		rsp.QaAuditStatus = docEntity.FrontEndQaAuditFailed
	}
	if qa.PicAuditFail {
		rsp.PicAuditStatus = docEntity.FrontEndPicAuditFailed
	}
	if qa.VideoAuditFail {
		rsp.VideoAuditStatus = docEntity.FrontEndVideoAuditFailed
	}
	pbSqs := make([]*pb.SimilarQuestion, 0, len(sqs))
	for _, sq := range sqs {
		pbSqs = append(pbSqs, &pb.SimilarQuestion{
			SimBizId: sq.SimilarID,
			Question: sq.Question,
			AuditStatus: func() uint32 {
				if sq.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass ||
					sq.ReleaseStatus == qaEntity.QAReleaseStatusAppealFail {
					return docEntity.FrontEndSimilarQuestionAuditFailed
				}
				return docEntity.FrontEndAuditPass
			}(),
		})
	}
	rsp.SimilarQuestions = pbSqs
	rsp.IsDisabled = false // 知识库概念统一后该字段已废弃
	rsp.CateBizIdPath = append([]uint64{cateEntity.AllCateID}, cateBizIdPath...)
	rsp.CateNamePath = append([]string{i18n.Translate(ctx, cateEntity.AllCateName)}, cateNamePath...)
	rsp.EnableScope = pb.RetrievalEnableScope(qa.EnableScope)
	return rsp
}

// CreateQA 创建QA
func (s *Service) CreateQA(ctx context.Context, req *pb.CreateQAReq) (*pb.CreateQARsp, error) {
	logx.I(ctx, "CreateQA Req:%+v", req)
	rsp := new(pb.CreateQARsp)
	staffID := contextx.Metadata(ctx).StaffID()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}

	enableScope := checkAndGetEnableScope(ctx, app, req.GetEnableScope())

	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = docEntity.AttrRangeCondition
	} else {
		req.AttrRange = docEntity.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.PrimaryId, config.App().AttributeLabel.QAAttrLimit,
		config.App().AttributeLabel.QAAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	logx.I(ctx, "CreateQA attrs:%+v labels:%+v", attrs, labels)
	if err != nil {
		return nil, err
	}

	var replyID uint64
	if req.GetBusinessId() > 0 {
		// reply, err := s.dao.GetUnsatisfiedReplyByBizIDs(ctx, app.CorpPrimaryId, app.ID, []uint64{req.GetBusinessId()})
		replyReq := &appconfig.DescribeUnsatisfiedReplyListReq{
			BotBizId:    convx.Uint64ToString(app.BizId),
			BusinessIds: []uint64{req.GetBusinessId()},
		}

		replyRsp, err := s.rpc.AppAdmin.DescribeUnsatisfiedReplyList(ctx, replyReq)
		if err != nil {
			return nil, errs.ErrSystem
		}
		reply := replyRsp.List
		if len(reply) == 0 {
			return rsp, errs.ErrUnsatisfiedReplyNotFound
		}
		replyID = reply[0].ReplyBizId
	}
	businessID := idgen.GetId()

	expireStart, expireEnd, err := util.CheckReqStartEndTime(ctx, req.GetExpireStart(), req.GetExpireEnd())
	if err != nil {
		return nil, err
	}

	qa := &qaEntity.DocQA{
		BusinessID:   businessID,
		StaffID:      staffID,
		Source:       docEntity.SourceFromManual,
		Question:     strings.TrimSpace(req.GetQuestion()),
		Answer:       strings.TrimSpace(req.GetAnswer()),
		CustomParam:  strings.TrimSpace(req.GetCustomParam()),
		QuestionDesc: strings.TrimSpace(req.GetQuestionDesc()),
		AcceptStatus: qaEntity.AcceptYes,
		AttrRange:    req.GetAttrRange(),
		ExpireStart:  time.Unix(int64(expireStart), 0),
		ExpireEnd:    time.Unix(int64(expireEnd), 0),
		EnableScope:  uint32(enableScope),
	}

	attributeLabelPB, err := fillQAAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}
	if err = s.qaLogic.CreateQA(ctx, app, qa, req.GetCateBizId(), req.GetDocBizId(), req.GetBusinessSource(), replyID,
		config.AuditSwitch(), attributeLabelPB, req.GetSimilarQuestions()); err != nil {
		return rsp, err
	}
	rsp.QaBizId = businessID

	//  上报操作日志
	if req.GetBusinessSource() == qaEntity.BusinessSourceUnsatisfiedReply {
		auditx.Modify(auditx.BizUnsatisfactoryQuestion).Space(app.SpaceId).App(app.BizId).
			Log(ctx, i18n.Translate(ctx, i18nkey.KeyUnsatisfiedCorrect, req.GetQuestion()))
	} else {
		auditx.Create(auditx.BizQA).Space(app.SpaceId).App(app.BizId).Log(ctx, businessID, req.GetQuestion())
	}

	return rsp, nil
}

// getCosFileSize 获取cos文件大小
func (s *Service) getCosFileSize(ctx context.Context, cosUrl string) (*s3x.ObjectInfo, error) {
	u, err := url.Parse(cosUrl)
	if err != nil {
		return nil, err
	}
	// 去掉前面的斜线
	path := strings.TrimPrefix(u.Path, "/")
	logx.I(ctx, "getCosFileSize|Path:%s", path)
	objectInfo, err := s.s3.StatObject(ctx, path)
	if err != nil || objectInfo == nil {
		logx.E(ctx, "getCosFileSize|StatObject:%+v err:%v", err, objectInfo)
		return nil, err
	}
	logx.I(ctx, "getCosFileSize|StatObject:%+v", objectInfo)
	return objectInfo, nil
}

// checkSimilarQuestions 相似问修改请求参数校验
func (s *Service) checkSimilarQuestions(ctx context.Context, qa *qaEntity.DocQA,
	similarModify *pb.SimilarQuestionModify) error {
	if qa == nil || similarModify == nil {
		return nil
	}
	// 判断当前主问的相似问总数是否超出
	count, err := s.qaLogic.GetSimilarQuestionsCount(ctx, qa)
	if err != nil {
		return err
	}
	_, err = s.qaLogic.CheckSimilarQuestionNumLimit(ctx, len(similarModify.GetAddQuestions()),
		len(similarModify.GetDeleteQuestions()), count)
	if err != nil {
		return err
	}
	sqs, err := s.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
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
	_, _, err = s.qaLogic.CheckSimilarQuestionContent(ctx, qa.Question, sqStrs)
	return err
}

// checkSimilarQuestionContent 检查相似问内容: 是否存在重复, 以及每一个相似问的字符数(满足限制), 返回相似问总字符数
func checkSimilarQuestionContent(ctx context.Context, qa string, sqs []string) (simTotalCharSize int, simTotalBytes int, err error) {
	if len(qa) == 0 || len(sqs) == 0 {
		return 0, 0, nil
	}
	cfg := config.App().DocQA
	simTotalCharSize = 0
	simTotalBytes = 0
	allQuestions := make(map[string]struct{})
	allQuestions[strings.TrimSpace(qa)] = struct{}{}
	for _, q := range sqs {
		s := strings.TrimSpace(q)
		if _, ok := allQuestions[s]; ok {
			return 0, 0, errs.ErrWrapf(errs.ErrCodeSimilarQuestionRepeated, i18n.Translate(ctx, i18nkey.KeyDuplicateSimilarQuestionFound),
				s)
		}
		if len([]rune(s)) < cfg.SimilarQuestion.MinLength {
			return 0, 0, errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooShort),
				cfg.SimilarQuestion.MinLength)
		}
		if len([]rune(s)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.SimilarQuestion.MaxLength) {
			return 0, 0, errs.ErrWrapf(errs.ErrCodeQuestionTooLong, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooLong),
				cfg.SimilarQuestion.MaxLength)
		}
		simTotalCharSize += utf8.RuneCountInString(s)
		simTotalBytes += len(s)
		allQuestions[s] = struct{}{}
	}
	return simTotalCharSize, simTotalBytes, nil
}

// checkInDataValidity 检查有效期格式
func checkInDataValidity(ctx context.Context, row []string) error {
	if docEntity.ExcelTplExpireTimeIndex+1 > len(row) {
		return nil
	}
	s := stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplExpireTimeIndex]))
	if len(s) == 0 || s == docEntity.ExcelNoExpireTime || s == i18n.Translate(
		ctx, docEntity.ExcelNoExpireTime) {
		return nil
	}
	t, err := time.ParseInLocation(docEntity.ExcelTplTimeLayout, s, time.Local)
	if err != nil {
		logx.W(ctx, "解析文件时间字段失败 err:%+v", err)
		return errs.ErrExcelParseFailInDateValidity
	}
	if t.Unix()%docEntity.HalfHourTime != 0 {
		return errs.ErrExcelParseFailNotHalfHour
	}
	return nil
}

func (s *Service) checkDocs(ctx context.Context, docIds []uint64, robotID uint64) error {
	if len(docIds) == 0 {
		return nil
	}
	docs, err := s.docLogic.GetDocByIDs(ctx, docIds, robotID)
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

// ModifyQA 更新QA
func (s *Service) ModifyQA(ctx context.Context, req *pb.ModifyQAReq) (*pb.ModifyQARsp, error) {
	logx.I(ctx, "ModifyQA Req:%+v", req)
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

	expireStart, expireEnd, err := util.CheckReqStartEndTime(ctx, req.GetExpireStart(), req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}

	// 前端按base64传代码内容，需要做decode
	decodeQuestion, isBase64 := util.StrictBase64DecodeToValidString(req.GetQuestion())
	logx.D(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, req.GetQuestion(),
		decodeQuestion)

	decodeAnswer, isBase64 := util.StrictBase64DecodeToValidString(req.GetAnswer())
	logx.D(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, req.GetAnswer(), decodeAnswer)

	req.Answer = strings.TrimSpace(decodeAnswer)
	req.Question = strings.TrimSpace(decodeQuestion)

	if err := s.qaLogic.CheckQAAndDescAndParam(ctx, req.GetQuestion(), req.GetAnswer(),
		req.GetQuestionDesc(), req.GetCustomParam()); err != nil {
		return nil, err
	}
	qa, err := s.qaLogic.GetQADetailsByBizID(ctx, app.CorpPrimaryId, app.PrimaryId, qaBizID)
	if err != nil {
		return rsp, errs.ErrQANotFound
	}
	if qa.IsDelete() {
		return rsp, errs.ErrQAIsDeleted
	}
	if !qa.IsAllowEdit() {
		return rsp, errs.ErrQANotAllowEdit
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
		docID, err = s.qaLogic.ValidateDocAndRetrieveID(ctx, docBizID, app.PrimaryId)
		if err != nil {
			return rsp, errs.ErrDocNotFound
		}
	}
	if qa.Source == docEntity.SourceFromDoc && qa.DocID != docID {
		return rsp, errs.ErrReferDocFail
	}
	var cateID uint64
	if req.GetCateBizId() != "" && req.GetCateBizId() != "0" {
		catBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cateID, err = s.cateLogic.VerifyCateBiz(ctx, cateEntity.QACate, app.CorpPrimaryId, catBizID, app.PrimaryId)
	} else {
		cateID, err = s.cateLogic.DescribeRobotUncategorizedCateID(ctx, cateEntity.QACate, app.CorpPrimaryId, app.PrimaryId)
	}
	if err != nil {
		return rsp, err
	}
	attrs, labels, isAttributeLabelReferChange, err := s.checkQAAttributeLabelRefer(ctx, req, app.PrimaryId, qa)
	if err != nil {
		return rsp, err
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, []uint64{qa.ID})
	if err != nil {
		logx.E(ctx, "获取发布中的问答失败 err:%+v", err)
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
	err = util.CheckMarkdownImageURL(ctx, req.GetAnswer(), contextx.Metadata(ctx).Uin(), app.BizId, nil)
	if err != nil {
		logx.W(ctx, "ModifyQA Answer ConvertDocQaHtmlToMD err:%d", err)
		return nil, err
	}
	qa.Answer = strings.TrimSpace(req.GetAnswer())
	// req.Answer = strings.TrimSpace(mdAnswer)
	var diff, diffBytes int64
	if qa.IsAccepted() {
		qa.CharSize, qa.QaSize, err = s.qaLogic.GetNewCharSize(ctx, &oldQa, req)
		if err != nil {
			logx.W(ctx, "ModifyQA|GetNewCharSize err:%+v", err)
			if errors.Is(err, errs.ErrVideoURLFail) {
				return rsp, err
			}
			return nil, errs.ErrSystem
		}
		diff = int64(qa.CharSize) - int64(oldQa.CharSize)
		diffBytes = int64(qa.QaSize) - int64(oldQa.QaSize)
		err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
			App:                  app,
			NewCharSize:          uint64(diff),
			NewKnowledgeCapacity: uint64(diffBytes),
		})
		if err != nil {
			return rsp, err
		}
	}

	attributeLabelPB, err := fillQAAttributeLabelsFromPB(ctx, req.GetAttrLabels(), isAttributeLabelReferChange, attrs, labels)
	if err != nil {
		return nil, err
	}
	var sqm *qaEntity.SimilarQuestionModifyInfo
	// 如果主问的过期时间、属性标签等全局信息发生变更，则需要发布所有相似问
	if isAllSimQuestionsNeedPublish || attributeLabelPB.IsNeedChange {
		sqm, err = s.qaLogic.NewSimilarQuestionsFromDBAndReq(ctx, qa, req.GetSimilarQuestionModify(), true)
	} else {
		sqm, err = s.qaLogic.NewSimilarQuestionsFromDBAndReq(ctx, qa, req.GetSimilarQuestionModify(), false)
	}
	if err != nil {
		return nil, errs.ErrSystem
	}
	qa.StaffID = contextx.Metadata(ctx).StaffID()
	if err = s.qaLogic.UpdateQA(ctx, qa, sqm, isNeedPublish, isNeedAudit, diff, diffBytes, attributeLabelPB); err != nil {
		return nil, errs.ErrSystem
	}

	auditArgs := []any{qa.BusinessID, oldQa.Question}
	if oldQa.Question != qa.Question {
		auditArgs = append(auditArgs, qa.Question)
	}

	auditx.Modify(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, auditArgs...)
	return rsp, nil
}

// isNeedAudit 是否需要审核
func (s *Service) isNeedAudit(qa *qaEntity.DocQA, req *pb.ModifyQAReq) bool {
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
func (s *Service) isNeedPublish(qa *qaEntity.DocQA, isAttributeLabelReferChange bool, req *pb.ModifyQAReq, docID,
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
	if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass {
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

	if req.EnableScope != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN && qa.EnableScope != uint32(req.EnableScope) {
		qa.EnableScope = uint32(req.EnableScope)
		isNeedPublish = true
		isAllSimQuestionsNeedPublish = true
	}

	qa.Answer = strings.TrimSpace(req.GetAnswer())
	qa.CustomParam = strings.TrimSpace(req.GetCustomParam())
	qa.QuestionDesc = strings.TrimSpace(req.GetQuestionDesc())
	qa.CategoryID = cateID
	qa.AttrRange = req.GetAttrRange()
	if isNeedPublish {
		qa.SimilarStatus = docEntity.SimilarStatusInit
	}
	if isNeedPublish && !qa.IsNextActionAdd() {
		qa.NextAction = qaEntity.NextActionUpdate
	}
	return isNeedPublish, isAllSimQuestionsNeedPublish
}
func (s *Service) fillQaReleaseStatus(isNeedAudit, isNeedPublish bool, qa *qaEntity.DocQA) {
	if isNeedAudit { // 需要审核，就改成审核状态
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditing
		qa.IsAuditFree = qaEntity.QAIsAuditNotFree
		return
	}
	if !isNeedPublish { // 不需要审核，也不需要发布，就不改qa状态
		return
	}
	// 不需要审核，需要发布，但是原先处于审核失败或者人工申诉失败，也不改qa状态
	if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass ||
		qa.ReleaseStatus == qaEntity.QAReleaseStatusAppealFail {
		return
	}
	qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
	qa.IsAuditFree = qaEntity.QAIsAuditFree
}
func (s *Service) checkQAAttributeLabelRefer(ctx context.Context, req *pb.ModifyQAReq, robotID uint64,
	qa *qaEntity.DocQA) (map[uint64]*labelEntity.Attribute, map[uint64]*labelEntity.AttributeLabel, bool, error) {
	if qa.Source == docEntity.SourceFromDoc && req.GetAttrRange() != docEntity.AttrRangeDefault {
		return nil, nil, false, errs.ErrAttributeLabelRefer
	}
	if qa.Source == docEntity.SourceFromDoc {
		return nil, nil, false, nil
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = docEntity.AttrRangeCondition
	} else {
		req.AttrRange = docEntity.AttrRangeAll
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
	logx.I(ctx, "DeleteQA Req:%+v", req)
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

	staffID := contextx.Metadata(ctx).StaffID()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	details, err := s.qaLogic.GetQADetailsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, bizIds)
	if err != nil || len(details) == 0 {
		return nil, errs.ErrQANotFound
	}
	// ps 这里不判断qa关联的文档和分组是否在删除流程中，重复删除不影响最终数据
	notDeletedQA := make([]*qaEntity.DocQA, 0, len(details))
	logx.I(ctx, "DeleteQA details:%+v", jsonx.MustMarshalToString(details))
	for _, detail := range details {
		if detail.IsDelete() {
			continue
		}
		if !detail.IsAllowDelete() {
			continue
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
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, qaIds)
	if err != nil {
		logx.E(ctx, "Failed to get releasingQaIdMap err:%+v", err)
		return nil, errs.ErrSystem
	}
	for _, qa := range notDeletedQA {
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			logx.I(ctx, "[DeleteQA] qa ia pending release: %d", qa.ID)
			return nil, errs.ErrQAIsPendingRelease
		}

	}

	logx.I(ctx, "[DeleteQA] deleting QAs: %d", len(notDeletedQA))
	if err = s.qaLogic.DeleteQAs(ctx, app.CorpPrimaryId, app.PrimaryId, staffID, notDeletedQA); err != nil {
		return nil, errs.ErrSystem
	}

	for _, qa := range notDeletedQA {
		auditx.Delete(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, qa.BusinessID, qa.Question)
	}

	return &pb.DeleteQARsp{}, nil
}

// VerifyQA 验证QA
func (s *Service) VerifyQA(ctx context.Context, req *pb.VerifyQAReq) (*pb.VerifyQARsp, error) {
	logx.I(ctx, "VerifyQA Req:%+v", req)
	if len(req.GetList()) > 1000 { // TODO: 1000 改成配置
		return nil, errs.ErrReqQaListExceedLimit
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
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
		if err = util.CheckMarkdownImageURL(ctx, v.GetAnswer(), contextx.Metadata(ctx).Uin(), app.BizId, nil); err != nil {
			return nil, err
		}
		ids = append(ids, qaBizID)
	}
	ids = slicex.Unique(ids)
	cateBizIDs = slicex.Unique(cateBizIDs)
	cateList, err := s.cateLogic.DescribeCateListByBusinessIDs(ctx, cateEntity.QACate, app.CorpPrimaryId, app.PrimaryId, cateBizIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	qas, err := s.checkCloudAPIVerifyQA(ctx, ids, req.GetList(), app, cateList)
	if err != nil {
		return nil, err
	}
	staffID := contextx.Metadata(ctx).StaffID()
	for _, qa := range qas {
		qa.StaffID = staffID
	}
	if err := s.qaLogic.VerifyQA(ctx, qas, app.PrimaryId); err != nil {
		return nil, errs.ErrSystem
	}
	return &pb.VerifyQARsp{}, nil
}

// ModifyQAStatus 修改QA状态
func (s *Service) ModifyQAStatus(ctx context.Context, req *pb.ModifyQAStatusReq) (*pb.ModifyQAStatusRsp, error) {
	logx.I(ctx, "ModifyQAStatus Req:%s", req)
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

	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil || app == nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	qa, err := s.qaLogic.GetQADetailsByBizID(ctx, app.CorpPrimaryId, app.PrimaryId, qaBizID)
	if err != nil {
		return rsp, errs.ErrQANotFound
	}
	if qa.IsDelete() {
		return rsp, errs.ErrQAIsDeleted
	}
	if !qa.IsAllowEdit() {
		return rsp, errs.ErrQANotAllowEdit
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, []uint64{qa.ID})
	if err != nil {
		logx.E(ctx, "获取发布中的问答失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	if _, ok := releasingQaIdMap[qa.ID]; ok {
		return rsp, errs.ErrQAIsPendingRelease
	}
	isAllSimQuestionsNeedPublish := true
	qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
	qa.NextAction = qaEntity.NextActionAdd
	if req.GetIsDisabled() {
		qa.NextAction = qaEntity.NextActionDelete
	}
	var sqm *qaEntity.SimilarQuestionModifyInfo
	// 如果主问的过期时间、属性标签等全局信息发生变更，则需要发布所有相似问
	if isAllSimQuestionsNeedPublish {
		sqm, err = s.qaLogic.NewSimilarQuestionsFromDBAndReq(ctx, qa, nil, true)
	}
	if err != nil {
		return nil, errs.ErrSystem
	}
	qa.StaffID = contextx.Metadata(ctx).StaffID()
	if err = s.qaLogic.UpdateQADisableState(ctx, qa, sqm, req.GetIsDisabled()); err != nil {
		return nil, errs.ErrSystem
	}
	if req.GetIsDisabled() {
		auditx.Disable(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, qa.BusinessID, qa.Question)
	} else {
		auditx.Enable(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, qa.BusinessID, qa.Question)
	}
	return rsp, nil
}

// GroupQA QA分组
func (s *Service) GroupQA(ctx context.Context, req *pb.GroupQAReq) (*pb.GroupQARsp, error) {
	logx.I(ctx, "GroupQA Req:%+v", req)
	rsp := new(pb.GroupQARsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var cateID uint64
	if req.GetCateId() != "" {
		// Deprecated
		cateIDUint64, err := util.CheckReqParamsIsUint64(ctx, req.GetCateId())
		if err != nil {
			return nil, err
		}
		if err := s.cateLogic.VerifyCate(ctx, cateEntity.QACate, app.CorpPrimaryId, cateIDUint64, app.PrimaryId); err != nil {
			return rsp, errs.ErrCateNotFound
		}
	} else {
		// 保留
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		if cateID, err = s.cateLogic.VerifyCateBiz(ctx, cateEntity.QACate, app.CorpPrimaryId, cateBizID, app.PrimaryId); err != nil {
			return rsp, errs.ErrCateNotFound
		}
	}
	var details map[uint64]*qaEntity.DocQA
	var qaIDs []uint64
	if len(req.GetIds()) > 0 {
		// Deprecated
		reqIDs, err := util.CheckReqSliceUint64(ctx, req.GetIds())
		if err != nil {
			return nil, err
		}
		ids := slicex.Unique(reqIDs)
		details, err = s.qaLogic.GetQADetails(ctx, app.CorpPrimaryId, app.PrimaryId, ids)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
		qaIDs = maps.Keys(details)
	} else {
		// 保留
		ids := slicex.Unique(req.GetQaBizIds())
		details, err = s.qaLogic.GetQADetailsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, ids)
		qaIDs = listQAIDs(details)
		if err != nil {
			return rsp, errs.ErrQANotFound
		}
	}
	if err = s.cateLogic.GroupCateObject(ctx, cateEntity.QACate, qaIDs, cateID, app); err != nil {
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// listQAIDs 获取QA ID
func listQAIDs(details map[uint64]*qaEntity.DocQA) []uint64 {
	values := maps.Values(details)
	var qaIDs []uint64
	for _, value := range values {
		qaIDs = append(qaIDs, value.ID)
	}
	return qaIDs
}

// saveQaSimilar 保存相似问答对
func (s *Service) saveQaSimilar(ctx context.Context, qa *qaEntity.DocQA, embeddingModel string, embeddingVersion uint64, appBizID uint64) (
	err error) {
	logx.I(ctx, "saveQaSimilar qa:%+v embeddingModel:%s embeddingVersion:%d appBizID:%d",
		qa, embeddingModel, embeddingVersion, appBizID)
	latestQA, err := s.qaLogic.GetQAByID(ctx, qa.ID)
	if err != nil {
		logx.E(ctx, "查询相似问答对失败,ID:%d err:%+v", qa.ID, err)
		return
	}
	if latestQA.Question != qa.Question {
		logx.W(ctx, "问答对问题已更新 跳过不处理 id:%d", qa.ID)
		return
	}
	// search获取问答对相似问答对
	filters := make([]*retrieval.SearchFilter, 0, 1)
	filters = append(filters, &retrieval.SearchFilter{
		IndexId:    entity.SimilarVersionID,
		Confidence: config.App().RobotDefault.SearchVector.Confidence,
		TopN:       config.App().RobotDefault.SearchVector.TopN,
		DocType:    entity.DocTypeQA,
	})
	req := &retrieval.SearchMultiKnowledgeReq{
		RobotId:  qa.RobotID,
		BotBizId: appBizID,
		Question: qa.Question,
		TopN:     config.App().RobotDefault.SearchVector.TopN,
		Rerank:   &retrieval.Rerank{Enable: false},
		SearchData: []*retrieval.SearchData{{
			KnowledgeId:        qa.RobotID,
			KnowledgeBizId:     appBizID,
			Filters:            filters,
			EmbeddingModelName: embeddingModel,
			EmbeddingVersion:   embeddingVersion,
		}},
	}
	rsp := &retrieval.SearchVectorRsp{}
	err = utilx.Retry(ctx, "saveQaSimilar|SearchMultiKnowledgePreview", 3, func() error {
		rsp, err = s.rpc.RetrievalDirectIndex.SearchMultiKnowledgePreview(ctx, req)
		return err
	})
	if err != nil {
		logx.E(ctx, "saveQaSimilar req:%+v rsp:%+v err:%+v", req, rsp, err)
		return err
	}

	now := time.Now()
	for _, doc := range rsp.Docs {
		if doc.Id == qa.ID {
			continue
		}
		// 判断当前qaID是否存在
		if _, err := s.qaLogic.GetQADetail(ctx, qa.CorpID, qa.RobotID, doc.Id); err != nil {
			continue
		}
		// 存储相似问答对
		_ = s.qaLogic.SaveQaSimilar(ctx, &qaEntity.DocQASimilar{
			RobotID:    qa.RobotID,
			CorpID:     qa.CorpID,
			StaffID:    qa.StaffID,
			DocID:      qa.DocID,
			QaID:       doc.Id,
			SimilarID:  qa.ID,
			IsValid:    true,
			CreateTime: now,
			UpdateTime: now,
			Status:     qaEntity.QaSimilarStatusInit,
		})
	}
	return
}

// ExportQAList 导出QA
func (s *Service) ExportQAList(ctx context.Context, req *pb.ExportQAListReq) (*pb.ExportQAListRsp, error) {
	logx.I(ctx, "ExportQAList Req:%+v", req)
	rsp := new(pb.ExportQAListRsp)
	staffID := contextx.Metadata(ctx).StaffID()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
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
			logx.E(ctx, "导出QA失败, 获取总数失败, req: %+v, err: %+v", req, err)
			return rsp, errs.ErrExportQA
		}
		if l.GetTotal() > uint64(config.App().CronTask.ExportQATask.MaxQACount) {
			return rsp, errs.ErrExportQATooMany
		}
	}

	paramStr, err := jsonx.MarshalToString(req)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	export := &entity.Export{
		CorpID:        app.CorpPrimaryId,
		RobotID:       app.PrimaryId,
		CreateStaffID: staffID,
		TaskType:      entity.ExportQaTaskType,
		Name:          entity.ExportQaTaskName,
		Params:        paramStr,
		Status:        qaEntity.TaskExportStatusInit,
		UpdateTime:    now,
		CreateTime:    now,
	}

	params := &entity.ExportParams{
		CorpID:           app.CorpPrimaryId,
		RobotID:          app.PrimaryId,
		AppBizID:         app.BizId,
		FileName:         fmt.Sprintf("export-%d-%d.xlsx", entity.ExportQaTaskType, time.Now().Unix()),
		CreateStaffID:    staffID,
		TaskType:         entity.ExportQaTaskType,
		TaskName:         entity.ExportQaTaskName,
		Params:           paramStr,
		NoticeContent:    i18n.Translate(ctx, qaEntity.ExportQANoticeContent),
		NoticePageID:     releaseEntity.NoticeQAPageID,
		NoticeTypeExport: releaseEntity.NoticeTypeQAExport,
		NoticeContentIng: i18n.Translate(ctx, qaEntity.ExportQANoticeContentIng),
	}

	if _, err = s.exportLogic.CreateExportTask(ctx, app.CorpPrimaryId, staffID, app.PrimaryId, export, params); err != nil {
		return rsp, err
	}

	logx.I(ctx, "Send Export Audit")
	auditx.Export(auditx.BizQA).Corp(app.CorpBizId).
		App(app.BizId).
		Space(app.SpaceId).
		Log(ctx, params.FileName)

	return rsp, nil
}

func (s *Service) BatchModifyQA(ctx context.Context, req *pb.BatchModifyQAReq) (*pb.BatchModifyQARsp, error) {
	rsp := new(pb.BatchModifyQARsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, err
	}
	if app.IsDeleted {
		logx.W(ctx, "app is deleted, app:%+v", app)
		return rsp, errs.ErrRobotNotFound
	}

	qaBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetQaBizIds())
	if err != nil {
		return nil, err
	}
	ids := slicex.Unique(qaBizIDs)
	qas, err := s.qaLogic.GetQAsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, ids, 0, len(ids))
	if err != nil || len(qas) == 0 {
		return rsp, errs.ErrQANotFound
	}

	switch req.GetActionType() {
	case pb.ModifyQAActionType_MODIFY_QA_ENABLE_SCOPE_ACTION:
		if req.GetEnableScope() == pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN {
			logx.W(ctx, "enable scope is unknown, req:%+v", req)
			return rsp, errs.ErrParameterInvalid
		}
		if err := s.handleUpdateQAEnableScope(ctx, qas, uint32(req.GetEnableScope())); err != nil {
			return rsp, err
		}
	default:
		logx.W(ctx, "action type is unknown, req:%+v", req)
		return rsp, errs.ErrParameterInvalid

	}

	return rsp, nil
}

func (s *Service) handleUpdateQAEnableScope(ctx context.Context, qas []*qaEntity.DocQA, enableScope uint32) error {
	logx.I(ctx, "handleUpdateQAEnableScope qas:%+v, enableScope:%d", qas, enableScope)

	modifiedQas := make([]*qaEntity.DocQA, 0, len(qas))

	for _, qa := range qas {
		if qa.IsDelete() {
			logx.W(ctx, "qa is deleted, qa:%+v", qa)
			continue
		}
		if !qa.IsAllowEdit() {
			logx.W(ctx, "qa is not allow edit, qa:%+v", qa)
			return errs.ErrQANotAllowEdit
		}

		if qa.EnableScope != enableScope {
			qa.EnableScope = enableScope
			modifiedQas = append(modifiedQas, qa)
			// 原先处于审核失败或者人工申诉失败，不修改qa状态
			if qa.ReleaseStatus != qaEntity.QAReleaseStatusAuditNotPass &&
				qa.ReleaseStatus != qaEntity.QAReleaseStatusAppealFail {
				qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
			}
			qa.SimilarStatus = docEntity.SimilarStatusInit
			qa.NextAction = qaEntity.NextActionUpdate
			qa.StaffID = contextx.Metadata(ctx).StaffID()
		}
	}

	if len(modifiedQas) == 0 {
		logx.I(ctx, "no modified qa for enable_scoped")
		return nil
	}

	if err := s.qaLogic.UpdateQAEnableScope(ctx, modifiedQas); err != nil {
		logx.W(ctx, "BatchModifyQA rpc req:%+v err:%+v", modifiedQas, err)
		return err
	}
	return nil
}

// RecordUserFirstGenQA 记录操作首次生成问答标记
func (s *Service) RecordUserFirstGenQA(ctx context.Context, req *pb.RecordUserFirstGenQAReq) (
	*pb.RecordUserFirstGenQARsp, error) {
	rsp := new(pb.RecordUserFirstGenQARsp)
	staffID := contextx.Metadata(ctx).StaffID()
	corpStaff, err := s.rpc.PlatformAdmin.GetStaffByID(ctx, staffID)
	if err != nil || corpStaff == nil {
		return nil, errs.ErrUserNotFound
	}
	// err = s.dao.UpdateCorpStaffGenQA(ctx, corpStaff)

	if corpStaff.IsGenQA {
		logx.I(ctx, "RecordUserFirstGenQA corpStaff already IsGenQA:%+v", corpStaff.IsGenQA)
		return rsp, nil
	}
	req2 := pm.ModifyCorpStaffReq{
		StaffId:        corpStaff.BusinessID,
		StaffPrimaryId: corpStaff.ID,
		IsGenQa:        ptrx.Bool(true),
	}
	if err := s.rpc.PlatformAdmin.ModifyCorpStaff(ctx, &req2); err != nil {
		logx.E(ctx, "ModifyCorpStaff rpc req:%+v err:%+v", &req2, err)
		return nil, errs.ErrSystem
	}
	return rsp, nil
}

// RecordUserAccessUnCheckQATime 记录访问未检验问答时间
func (s *Service) RecordUserAccessUnCheckQATime(ctx context.Context, req *pb.RecordUserAccessUnCheckQATimeReq) (
	*pb.RecordUserAccessUnCheckQATimeRsp, error) {
	rsp := new(pb.RecordUserAccessUnCheckQATimeRsp)
	staffID := contextx.Metadata(ctx).StaffID()
	botBizID := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, botBizID)
	if err != nil {
		return rsp, err
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	cropStaff, err := s.rpc.PlatformAdmin.GetStaffByID(ctx, staffID)
	if err != nil || cropStaff == nil {
		return nil, errs.ErrUserNotFound
	}
	err = s.qaLogic.RecordUserAccessUnCheckQATime(ctx, app.PrimaryId, staffID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	return rsp, nil
}

// checkCloudAPIVerifyQA 校验问答前检查qa
func (s *Service) checkCloudAPIVerifyQA(ctx context.Context, ids []uint64, list []*pb.VerifyQAReq_QAList,
	app *entity.App, cateList map[uint64]*cateEntity.CateInfo) ([]*qaEntity.DocQA, error) {
	var qaSlice []*qaEntity.DocQA
	qas, err := s.qaLogic.GetQADetailsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, ids)
	logx.D(ctx, "get qa details success,qas:%+v", qas)
	if err != nil {
		return nil, errs.ErrQANotFound
	}
	qaIds := make([]uint64, 0, len(qas))
	for _, item := range qas {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, qaIds)
	if err != nil {
		logx.E(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	var addCharSize, addBytes uint64
	for _, item := range list {
		qaBizID, err := util.CheckReqParamsIsUint64(ctx, item.GetQaBizId())
		if err != nil {
			return nil, err
		}
		qa, ok := qas[qaBizID]
		if !ok {
			continue
		}
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			return nil, errs.ErrQAIsPendingRelease
		}
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, item.GetCateBizId())
		if err != nil {
			return nil, err
		}
		if cate, ok := cateList[cateBizID]; ok {
			qa.CategoryID = cate.ID
		}
		qa.AcceptStatus = gox.IfElse(item.GetIsAccepted(), qaEntity.AcceptYes, qaEntity.AcceptNo)
		qa.Question = gox.IfElse(len(strings.TrimSpace(item.GetQuestion())) > 0,
			strings.TrimSpace(item.GetQuestion()), qa.Question)
		err = util.CheckMarkdownImageURL(ctx, item.GetAnswer(), contextx.Metadata(ctx).Uin(), app.BizId, nil)
		if err != nil {
			logx.W(ctx, "checkCloudAPIVerifyQA Answer CheckQaImgURLSafeToMD err:%d", err)
			return nil, err
		}
		qa.Answer = gox.IfElse(len(strings.TrimSpace(item.GetAnswer())) > 0,
			strings.TrimSpace(item.GetAnswer()), qa.Answer)
		qa.CharSize = gox.IfElse(len(strings.TrimSpace(item.GetQuestion())) > 0 ||
			len(strings.TrimSpace(item.GetAnswer())) > 0, uint64(utf8.RuneCountInString(item.GetQuestion()+
			item.GetAnswer())), qa.CharSize)
		qa.QaSize = gox.IfElse(len(strings.TrimSpace(item.GetQuestion())) > 0 ||
			len(strings.TrimSpace(item.GetAnswer())) > 0, uint64(len(item.GetQuestion()+item.GetAnswer())), qa.QaSize)
		if item.GetIsAccepted() {
			addCharSize += qa.CharSize
			addBytes += qa.QaSize
		}
		qaSlice = append(qaSlice, qa)
	}
	err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
		App:                  app,
		NewCharSize:          addCharSize,
		NewKnowledgeCapacity: addBytes,
	})
	if err != nil {
		return nil, err
	}
	return qaSlice, nil
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
		case qaEntity.QAReleaseStatusCharExceeded:
			releaseStatus = append(releaseStatus, qaEntity.QAReleaseStatusCharExceeded,
				qaEntity.QAReleaseStatusAppealFailCharExceeded, qaEntity.QAReleaseStatusAuditNotPassCharExceeded,
				qaEntity.QAReleaseStatusLearnFailCharExceeded)
		case qaEntity.QAReleaseStatusResuming:
			releaseStatus = append(releaseStatus, qaEntity.QAReleaseStatusResuming,
				qaEntity.QAReleaseStatusAppealFailResuming, qaEntity.QAReleaseStatusAuditNotPassResuming,
				qaEntity.QAReleaseStatusLearnFailResuming)
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

// ModifyQAAttrRange 编辑QA适用范围
func (s *Service) ModifyQAAttrRange(ctx context.Context, req *pb.ModifyQAAttrRangeReq) (*pb.ModifyQAAttrRangeRsp,
	error) {
	logx.I(ctx, "ModifyQAAttrRange Req:%+v", req)
	rsp := new(pb.ModifyQAAttrRangeRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = docEntity.AttrRangeCondition
	} else {
		req.AttrRange = docEntity.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.PrimaryId, config.App().AttributeLabel.QAAttrLimit,
		config.App().AttributeLabel.QAAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return rsp, err
	}
	qaBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetQaBizIds())
	if err != nil {
		return nil, err
	}
	ids := slicex.Unique(qaBizIDs)
	qas, err := s.qaLogic.GetQAsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, ids, 0, len(ids))
	if err != nil || len(qas) == 0 {
		return rsp, errs.ErrQANotFound
	}
	qaIds := make([]uint64, 0, len(qas))
	for _, qa := range qas {
		if qa.IsDelete() {
			return rsp, errs.ErrQAIsDeleted
		}
		if !qa.IsAllowEdit() {
			return rsp, errs.ErrQANotAllowEdit
		}
		qaIds = append(qaIds, qa.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, qaIds)
	if err != nil {
		logx.E(ctx, "获取发布中的问答失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	modifyQAs, err := s.filterAttributeLabelChangedQA(ctx, app.PrimaryId, qaBizIDs, qas,
		req.GetAttrRange(), req.GetAttrLabels(), attrs, labels)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if len(modifyQAs) == 0 {
		return rsp, nil
	}
	for _, v := range modifyQAs {
		if v.Source == docEntity.SourceFromDoc {
			return rsp, errs.ErrAttributeLabelDocQa
		}
		if _, ok := releasingQaIdMap[v.ID]; ok {
			return rsp, errs.ErrAttributeLabelDocQaSync
		}
		v.AttrRange = req.GetAttrRange()
		// 原先处于审核失败或者人工申诉失败，不修改qa状态
		if v.ReleaseStatus != qaEntity.QAReleaseStatusAuditNotPass &&
			v.ReleaseStatus != qaEntity.QAReleaseStatusAppealFail {
			v.ReleaseStatus = qaEntity.QAReleaseStatusLearning
		}
		v.SimilarStatus = docEntity.SimilarStatusInit
		v.NextAction = qaEntity.NextActionUpdate
		v.StaffID = contextx.Metadata(ctx).StaffID()
	}
	attributeLabelPB, err := fillQAAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}
	if err = s.qaLogic.UpdateQAAttrRange(ctx, modifyQAs, attributeLabelPB); err != nil {
		return nil, errs.ErrSystem
	}
	return rsp, nil
}

// BatchModifyQaExpire 批量修改问答过期时间
func (s *Service) BatchModifyQaExpire(ctx context.Context, req *pb.BatchModifyQaExpireReq) (*pb.BatchModifyQaExpireRsp,
	error) {
	logx.I(ctx, "BatchModifyQaExpire Req:%+v", req)
	rsp := new(pb.BatchModifyQaExpireRsp)
	staffID := contextx.Metadata(ctx).StaffID()
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
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var updateDocQA []*qaEntity.DocQA
	var errorDocQA []*qaEntity.DocQA
	isFailQa := false
	qas, err := s.qaLogic.GetQADetailsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, qaBizIDs)
	if err != nil {
		return nil, err
	}
	qaIds := make([]uint64, 0, len(qas))
	for _, item := range qas {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, qaIds)
	if err != nil {
		logx.E(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	for _, qaBizId := range qaBizIDs {
		qa, err := s.qaLogic.GetQADetailsByBizID(ctx, app.CorpPrimaryId, app.PrimaryId, qaBizId)
		if err != nil {
			logx.E(ctx, "BatchModifyQaExpire|GetQADetailsByBizID|qa|%v|err:%+v", qa, err)
			isFailQa = true
			continue
		}
		if qa.Source == docEntity.SourceFromDoc {
			logx.I(ctx, "BatchModifyQaExpire|SourceFromDoc|qa|%v|err:%+v", qa,
				errs.ErrModifyQaExpireFail)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if qa.IsDelete() {
			logx.I(ctx, "BatchModifyQaExpire|IsDelete|%v|qa:%+v", qa.IsDelete(), qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if !qa.IsAllowEdit() {
			logx.I(ctx, "BatchModifyQaExpire|is not allow edit|qa:%+v", qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			logx.I(ctx, "BatchModifyQaExpire|pendingReleaseQA|qa:%v|err:%v",
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
		logx.I(ctx, "BatchModifyQaExpire|errorDocQA|%v", errorDocQA)
	}
	if len(updateDocQA) == 0 {
		return rsp, errs.ErrModifyQaExpireFail
	}

	if err = s.qaLogic.UpdateQAsExpire(ctx, updateDocQA); err != nil {
		return nil, errs.ErrSystem
	}
	if isFailQa {
		return rsp, errs.ErrModifyQaExpireFail
	}
	for _, qa := range updateDocQA {
		auditx.Modify(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, qa.BusinessID, qa.Question, qa.GetExpireTime())
	}
	return rsp, nil
}

// BatchModifyQaDoc 批量修改问答关联文档
func (s *Service) BatchModifyQaDoc(ctx context.Context, req *pb.BatchModifyQaDocReq) (*pb.BatchModifyQaDocRsp,
	error) {
	rsp := new(pb.BatchModifyQaDocRsp)
	if len(req.GetQaBizIds()) == 0 || req.GetDocBizId() == "" {
		return rsp, errs.ErrParams
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
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
	docID, err := s.qaLogic.ValidateDocAndRetrieveID(ctx, docBizID, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrDocNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var updateDocQA []*qaEntity.DocQA
	var errorDocQA []*qaEntity.DocQA
	isFailQa := false
	qas, err := s.qaLogic.GetQADetailsByBizIDs(ctx, app.CorpPrimaryId, app.PrimaryId, qaBizIDs)
	qaIds := make([]uint64, 0, len(qas))
	for _, item := range qas {
		qaIds = append(qaIds, item.ID)
	}
	// 检查问答是否在发布中
	releasingQaIdMap, err := s.releaseLogic.GetReleasingQaId(ctx, app.PrimaryId, qaIds)
	if err != nil {
		logx.E(ctx, "获取发布中的问答失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	for _, qaBizId := range qaBizIDs {
		qa, err := s.qaLogic.GetQADetailsByBizID(ctx, app.CorpPrimaryId, app.PrimaryId, qaBizId)
		if err != nil {
			logx.E(ctx, "BatchModifyQaDoc|GetQADetailsByBizID|qa|%v|err:%+v", qa, err)
			isFailQa = true
			continue
		}
		if qa.Source == docEntity.SourceFromDoc && qa.DocID != docID {
			logx.I(ctx, "BatchModifyQaDoc|SourceFromDoc|qa|%v|err:%+v", qa, errs.ErrReferDocFail)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if qa.IsDelete() {
			logx.I(ctx, "BatchModifyQaDoc|IsDelete|%v|qa:%+v", qa.IsDelete(), qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if !qa.IsAllowEdit() {
			logx.I(ctx, "BatchModifyQaDoc|is not allow edit|qa:%+v", qa)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		if _, ok := releasingQaIdMap[qa.ID]; ok {
			logx.E(ctx, "BatchModifyQaDoc|pendingReleaseQA|qa:%v|err:%v",
				qa, errs.ErrQAIsPendingRelease)
			isFailQa = true
			errorDocQA = append(errorDocQA, qa)
			continue
		}
		qa.DocID = docID
		qa.StaffID = contextx.Metadata(ctx).StaffID()
		updateDocQA = append(updateDocQA, qa)
	}
	if len(errorDocQA) > 0 {
		logx.I(ctx, "BatchModifyQaDoc|errorDocQA|%v", errorDocQA)
	}
	if len(updateDocQA) == 0 {
		return rsp, errs.ErrModifyQaDocFail
	}

	if err = s.qaLogic.UpdateQAsDoc(ctx, updateDocQA); err != nil {
		return nil, errs.ErrSystem
	}
	if isFailQa {
		return rsp, errs.ErrModifyQaDocFail
	}
	return rsp, nil
}

// ResumeQA QA 超量恢复
func (s *Service) ResumeQA(ctx context.Context, req *pb.ResumeQAReq) (*pb.ResumeQARsp, error) {
	logx.D(ctx, "恢复问答 REQ: %+v", req)
	if config.App().DocQA.ResumeMaxCountLimit != 0 && len(req.GetQaBizIds()) > config.App().DocQA.ResumeMaxCountLimit {
		return nil, errs.ErrResumeQAMaxCountLimit
	}
	rsp := new(pb.ResumeQARsp)
	staffID, corpID := contextx.Metadata(ctx).StaffID(), contextx.Metadata(ctx).CorpID()
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		logx.E(ctx, "恢复问答失败 CheckReqBotBizIDUint64 err: %+v", err)
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, botBizID)
	// app, err := s.dao.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		logx.E(ctx, "恢复问答失败 GetAppByAppBizIDerr: %+v", err)
		return nil, err
	}
	// 字符数超限不可执行
	if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
		return rsp, logicCommon.ConvertErrMsg(ctx, s.rpc, 0, app.CorpPrimaryId, err)
	}
	// TODO: 实现超量恢复的逻辑
	log.DebugContext(ctx, "ResumeDoc REQ: ", req)
	qaBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetQaBizIds())
	if err != nil {
		logx.E(ctx, "恢复问答失败 BatchCheckReqParamsIsUint64 err: %+v", err)
		return rsp, err
	}
	filter := &qaEntity.DocQaFilter{
		CorpId:      corpID,
		RobotId:     app.PrimaryId,
		BusinessIds: qaBizIDs,
	}
	qas, err := s.qaLogic.GetAllDocQas(ctx, qaEntity.DocQaTblColList, filter)
	// qas, err := s.dao.GetQAsByBizIDs(ctx, corpID, robotID, qaBizIDs, 0, uint64(len(qaBizIDs)))
	if err != nil {
		logx.E(ctx, "恢复问答失败 GetDocByBizIDs err: %+v", err)
		return rsp, err
	}
	qaExceededTimes := make([]entity.QAExceededTime, 0)
	for _, qa := range qas {
		if qa == nil {
			logx.E(ctx, "恢复问答失败 qa is nil")
			continue
		}
		if !qa.IsCharExceeded() && !qa.IsResuming() {
			logx.I(ctx, "没有超量失败的问答，不做处理，qa: %+v", qa)
			continue
		}
		qaExceededTimes = append(qaExceededTimes, entity.QAExceededTime{
			BizID:      qa.BusinessID,
			UpdateTime: qa.UpdateTime,
		})
		qa.StaffID = staffID
		if err := s.resumeQA(ctx, qa); err != nil {
			logx.E(ctx, "恢复部分问答失败 resumeQA err: %+v", err)
			continue
		}
		auditx.Recover(auditx.BizQA).Corp(app.CorpBizId).App(app.BizId).Space(app.SpaceId).
			Log(ctx, qa.BusinessID, qa.Question)
	}
	if len(qaExceededTimes) != 0 {
		if err := scheduler.NewQAResumeTask(ctx, corpID, app.PrimaryId, staffID, qaExceededTimes); err != nil {
			logx.E(ctx, "恢复问答失败 CreateDocResumeTask err: %+v", err)
			return rsp, err
		}
	}
	return rsp, nil
}

func (s *Service) resumeQA(ctx context.Context, qa *qaEntity.DocQA) error {
	switch qa.ReleaseStatus {
	case qaEntity.QAReleaseStatusCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusResuming
	case qaEntity.QAReleaseStatusAppealFailCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAppealFailResuming
	case qaEntity.QAReleaseStatusAuditNotPassCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPassResuming
	case qaEntity.QAReleaseStatusLearnFailCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusLearnFailResuming
	default:
		// 不可恢复
		return nil
	}
	// 增加相似问的超量恢复的逻辑
	sqs, err := s.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx,
			"bot: %d, qaID: %d, GetSimilarQuestionsByQA err: %+v", qa.RobotID, qa.ID, err)
		// 柔性放过
	}
	logx.D(ctx, "update QA(%d) and SimilarQuestions", qa.ID)

	sqm := &qaEntity.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	if err := s.qaLogic.UpdateQA(ctx, qa, sqm, true, false, 0, 0, &labelEntity.UpdateQAAttributeLabelReq{}); err != nil {
		return err
	}
	logx.D(ctx, "resume QA and SimilarQuestions, qa:%+v, sqm:%+v", qa, sqm)
	return nil
}

// RetryQaTask 重试或继续生成qa任务
func (s *Service) RetryQaTask(ctx context.Context, req *pb.RetryQaTaskReq) (*pb.RetryQaTaskRsp, error) {
	logx.I(ctx, "RetryQaTask: %+v", req)
	rsp := new(pb.RetryQaTaskRsp)
	if req.GetBotBizId() == "" || req.GetTaskId() == "" {
		return nil, errs.ErrParams
	}
	taskID, err := util.CheckReqParamsIsUint64(ctx, req.GetTaskId())
	if err != nil {
		return nil, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	docQATask, err := s.taskLogic.GetDocQATaskByBusinessID(ctx, taskID, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "RetryQaTask|GetDocQATaskByBusinessID|获取生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "RetryQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ", app.CorpPrimaryId, app.PrimaryId, taskID)
		return rsp, errs.ErrDocQaTaskNotFound
	}
	if docQATask.SourceID != 0 && !req.GetIsContinue() {
		return nil, errs.ErrDocDiffQaNotRetry
	}

	if req.GetIsContinue() { // 暂停后继续的操作
		if docQATask.SourceID != 0 {
			// 如果是doc diff创建的任务
			err = s.taskLogic.ContinueDocDiffTask(ctx, docQATask.SourceID, app.CorpPrimaryId, app.PrimaryId, app.BizId, taskID)
			if err != nil {
				return nil, err
			}
			return rsp, nil
		}

		if !docQATask.DocQATaskIsContinue() {
			logx.E(ctx, "RetryQaTask|任务当前状态不可继续|corpID|%d|robotID%d|id|%d|docQATask|%+v ", app.CorpPrimaryId, app.PrimaryId, taskID, docQATask)
			return rsp, errs.ErrContinueQaTaskStatusFail
		}
		// 继续逻辑
		err = s.taskLogic.ContinueQaTask(ctx, app.CorpPrimaryId, app.PrimaryId, docQATask)
		if err != nil {
			logx.E(ctx, "RetryQaTask|ContinueQaTask|取消任务失败 err:%+v", err)
			return rsp, err
		}
		return rsp, nil
	}
	if docQATask.Status != qaEntity.DocQATaskStatusFail {
		logx.E(ctx, "RetryQaTask|任务当前状态不可重试 docQATask:%+v", docQATask)
		return rsp, errs.ErrRetryQaTaskStatusFail
	}
	// 目前失败后重试的逻辑也从失败的切片开始,复用继续逻辑
	err = s.taskLogic.ContinueQaTask(ctx, app.CorpPrimaryId, app.PrimaryId, docQATask)
	if err != nil {
		logx.E(ctx, "RetryQaTask|重试任务失败 err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

// ListQaTask 生成问答任务列表
func (s *Service) ListQaTask(ctx context.Context, req *pb.ListQaTaskReq) (*pb.ListQaTaskRsp, error) {
	logx.I(ctx, "ListQaTaskReq: %+v", req)
	rsp := new(pb.ListQaTaskRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	listQaTasReq, err := s.getListQaTaskReq(req, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return rsp, err
	}
	total, tasks, err := s.taskLogic.GetListQaTask(ctx, listQaTasReq)
	if err != nil {
		return rsp, errs.ErrGetListQaTaskFail
	}
	docIDs := make([]uint64, 0)
	for _, task := range tasks {
		docIDs = append(docIDs, task.DocID)
	}
	docs, err := s.docLogic.GetDocByIDs(ctx, docIDs, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrGetDocListFail
	}
	docMap := make(map[uint64]*docEntity.Doc, len(docs))
	for _, doc := range docs {
		docMap[doc.ID] = doc
	}
	rsp.Total = total
	rsp.List = make([]*pb.ListQaTaskRsp_Task, 0, len(tasks))
	for _, task := range tasks {
		var sourceType uint32 = qaEntity.DocQaTaskSourceTypeOrigin
		if task.SourceID != 0 {
			// 当前来源只有doc to qa task和 doc diff任务
			sourceType = qaEntity.DocQaTaskSourceTypeDocDiff
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
			TaskMessage:      i18n.Translate(ctx, task.Message),
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
func (s *Service) getListQaTaskReq(req *pb.ListQaTaskReq, corpID, robotID uint64) (*qaEntity.DocQaTaskFilter, error) {
	return &qaEntity.DocQaTaskFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		IsDeleted: ptrx.Int(qaEntity.DocQATaskIsNotDeleted),
		PageNo:    req.GetPageNumber(),
		PageSize:  req.GetPageSize(),
	}, nil
}

// DeleteQaTask 删除生成问答任务
func (s *Service) DeleteQaTask(ctx context.Context, req *pb.DeleteQaTaskReq) (*pb.DeleteQaTaskRsp, error) {
	logx.I(ctx, "DeleteQaTaskReq: %+v", req)
	rsp := new(pb.DeleteQaTaskRsp)
	if req.GetBotBizId() == "" || req.GetTaskId() == "" {
		return nil, errs.ErrParams
	}
	taskID, err := util.CheckReqParamsIsUint64(ctx, req.GetTaskId())
	if err != nil {
		return nil, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	docQATask, err := s.taskLogic.GetDocQATaskByBusinessID(ctx, taskID, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "DeleteQaTask|GetDocQATaskByBusinessID|获取生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "DeleteQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ", app.CorpPrimaryId, app.PrimaryId, taskID)
		return rsp, errs.ErrDocQaTaskNotFound
	}
	// 判断status状态是否合法,仅 成功、失败、手动取消状态可删除
	if docQATask.Status != qaEntity.DocQATaskStatusFail && docQATask.Status != qaEntity.DocQATaskStatusCancel &&
		docQATask.Status != qaEntity.DocQATaskStatusSuccess {
		return rsp, errs.ErrDeleteQaTaskStatusFail
	}
	err = s.taskLogic.DeleteQaTask(ctx, app.CorpPrimaryId, app.PrimaryId, docQATask.ID)
	if err != nil {
		logx.E(ctx, "DeleteQaTask删除生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}

	logx.I(ctx, "DeleteQaTask success: botBizID:%d|taskID:%d", app.BizId, taskID)
	return rsp, nil
}

// StopQaTask 暂停或取消任务
func (s *Service) StopQaTask(ctx context.Context, req *pb.StopQaTaskReq) (*pb.StopQaTaskRsp, error) {
	logx.I(ctx, "StopQaTask: %+v", req)
	rsp := new(pb.StopQaTaskRsp)
	if req.GetBotBizId() == "" || req.GetTaskId() == "" {
		return nil, errs.ErrParams
	}
	taskID, err := util.CheckReqParamsIsUint64(ctx, req.GetTaskId())
	if err != nil {
		return nil, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	docQATask, err := s.taskLogic.GetDocQATaskByBusinessID(ctx, taskID, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "StopQaTask|GetDocQATaskByBusinessID|获取生成问答任务详情失败 err:%+v", err)
		return rsp, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "StopQaTask|获取生成问答任务不存在|corpID|%d|robotID%d|id|%d ", app.CorpPrimaryId, app.PrimaryId, taskID)
		return rsp, errs.ErrDocQaTaskNotFound
	}
	if req.GetIsCancel() {
		if !docQATask.DocQATaskIsCancel() {
			logx.E(ctx, "StopQaTask|任务当前状态不可取消|corpID|%d|robotID%d|id|%d|docQATask|%+v ", app.CorpPrimaryId, app.PrimaryId, taskID, docQATask)
			return rsp, errs.ErrCancelQaTaskStatusFail
		}
		// 调用 取消逻辑
		err = s.taskLogic.CancelQaTask(ctx, app.CorpPrimaryId, app.PrimaryId, docQATask.ID)
		if err != nil {
			logx.E(ctx, "StopQaTask|CancelQaTask|取消任务失败 err:%+v", err)
			return rsp, err
		}
		return rsp, nil
	}
	if !docQATask.DocQATaskIsStop() {
		logx.E(ctx, "StopQaTask|任务当前状态不可暂停|corpID|%d|robotID%d|id|%d ", app.CorpPrimaryId, app.PrimaryId, taskID)
		return rsp, errs.ErrStopQaTaskStatusFail
	}

	err = s.taskLogic.StopQaTask(ctx, app.CorpPrimaryId, app.PrimaryId, docQATask.ID, false, "")
	if err != nil {
		logx.E(ctx, "StopQaTask暂停任务失败|err:%+v", err)
		return rsp, err
	}

	return rsp, nil
}

// GenerateSimilarQuestions 生成相似问
func (s *Service) GenerateSimilarQuestions(ctx context.Context, req *pb.GenerateSimilarQuestionsReq) (
	*pb.GenerateSimilarQuestionsRsp, error) {
	logx.I(ctx, "GenerateSimilarQuestionsReq: %+v", req)
	rsp := new(pb.GenerateSimilarQuestionsRsp)
	if req.GetBotBizId() == "" {
		return nil, errs.ErrParams
	}
	if req.GetQuestion() == "" {
		return nil, errs.ErrGenerateSimilarParams
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	modelName := ""
	if app.IsShared {
		modelName, err = s.kbLogic.GetShareKnowledgeBaseConfig(ctx, corpBizID, app.BizId, uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL))
	} else {
		modelName, err = s.kbLogic.GetDefaultKnowledgeBaseConfig(ctx, corpBizID, app.BizId, app.BizId, uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL), bot_common.AdpDomain_ADP_DOMAIN_DEV)
	}
	if err != nil {
		logx.E(ctx, "GetTokenDosage GetKnowledgeBaseConfig err: %+v", err)
		return nil, err
	}
	list, err := s.qaLogic.GenerateSimilarQuestions(ctx, app, req.Question, req.Answer, modelName)
	if err != nil {
		if errors.Is(err, errs.ErrNoTokenBalance) {
			return rsp, errs.ErrNoTokenBalance
		}
		logx.E(ctx, "GenerateSimilarQuestions err:%+v", err)
		return rsp, errs.ErrSystem
	}
	rsp.Question = list
	return rsp, nil
}

func (s *Service) getShareKnowledgeValidityQACount(ctx context.Context, appBizID uint64) (uint64, error) {
	shareKnowledges, err := s.kbDao.GetAppShareKGList(ctx, appBizID)
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
	appListReq := appconfig.ListAppBaseInfoReq{
		AppBizIds:  knowledgesBizIDs,
		PageNumber: 1,
		PageSize:   uint32(len(knowledgesBizIDs)),
	}
	apps, _, err := s.rpc.AppAdmin.ListAppBaseInfo(ctx, &appListReq)
	// robots, err := s.dao.GetRobotList(ctx, 0, "", knowledgesBizIDs, 0, 1, uint32(len(knowledgesBizIDs)))
	if err != nil {
		return 0, err
	}
	knowledgeIDs := slicex.Pluck(apps, func(v *entity.AppBaseInfo) uint64 { return v.PrimaryId }) // 主键 id
	if len(knowledgeIDs) == 0 {
		return 0, nil
	}
	total, err := s.qaLogic.GetDocQaCount(ctx, []string{}, &qaEntity.DocQaFilter{RobotIDs: knowledgeIDs})
	logx.D(ctx, "getShareKnowledgeValidityQACount appBizID:%d robots:%v total:%d", appBizID, knowledgeIDs, total)
	return uint64(total), err
}

func (s *Service) getValidityDBTableCount(ctx context.Context, appBizID uint64) (uint64, error) {
	shareKnowledges, err := s.kbDao.GetAppShareKGList(ctx, appBizID)
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
	tableFilter := dbEntity.TableFilter{
		AppBizIDs: knowledgesBizIDs,
	}
	total, err := s.dbLogic.CountTable(ctx, &tableFilter)
	// total, err := dao.GetDBTableDao().GetCountByAppBizIDs(ctx, knowledgesBizIDs)
	logx.D(ctx, "getValidityDBTableCount appBizID:%d knows:%v total:%d", appBizID, knowledgesBizIDs, total)
	return uint64(total), err
}
