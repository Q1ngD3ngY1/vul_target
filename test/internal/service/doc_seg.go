// bot-knowledge-config-server
//
// @(#)doc_seg.go  Monday, June 17, 2024
// Copyright(c) 2024, leyton@Tencent. All rights reserved.

package service

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_segment"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_intervene"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/go-comm/clues"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	KeywordsMaxLimit = 100
)

// DescribeSegments 获取片段详情
func (s *Service) DescribeSegments(ctx context.Context, req *pb.DescribeSegmentsReq) (*pb.DescribeSegmentsRsp, error) {
	ctx = clues.NewTrackContext(ctx)
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	clues.AddTrackData(ctx, "DescribeSegmentsReq", req)
	log.InfoContextf(ctx, "DescribeSegments req:%v", req)
	if len(req.GetSegBizId()) == 0 {
		return &pb.DescribeSegmentsRsp{}, nil
	}
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return &pb.DescribeSegmentsRsp{}, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return &pb.DescribeSegmentsRsp{}, errs.ErrRobotNotFound
	}
	segBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetSegBizId())
	if err != nil {
		return &pb.DescribeSegmentsRsp{}, err
	}
	if len(segBizIDs) == 0 {
		return &pb.DescribeSegmentsRsp{}, errs.ErrSegmentNotFound
	}

	rsp, err := doc_segment.DescribeSegments(ctx, app.BusinessID, segBizIDs)
	return rsp, nil
}

// ListDocSegment 获取文档切片列表
func (s *Service) ListDocSegment(ctx context.Context, req *pb.ListDocSegmentReq) (*pb.ListDocSegmentRsp, error) {
	log.InfoContextf(ctx, "ListDocSegment|Req:%+v", req)
	rsp := new(pb.ListDocSegmentRsp)
	// 1.校验基础参数
	if utf8.RuneCountInString(req.Keywords) > KeywordsMaxLimit {
		log.ErrorContextf(ctx, "ListDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrDocSegmentKeywordsMaxLimit
	}
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 || req.PageNumber < 1 || req.PageSize < 1 {
		log.ErrorContextf(ctx, "ListDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegment|AppBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegment|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// sheetName去除title
	req.SheetName = strings.Split(req.GetSheetName(), ":")[0]
	// 2.页大小不能超过200，会导致索引失效
	pageSize := req.GetPageSize()
	if pageSize > dao.DocSegmentInterveneMaxPageSize {
		log.ErrorContextf(ctx, "ListDocSegment|PageSize is larger than 200|PageSize:%d", req.PageSize)
		return rsp, errs.ErrGetDocSegmentTooLarge
	}
	// 3.获取企业信息
	staffBizID, corpBizID, corpID := pkg.StaffBizID(ctx), pkg.CorpBizID(ctx), pkg.CorpID(ctx)
	// 4.获取应用信息
	app, err := s.getAppByAppBizID(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegment|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 5.获取文档信息
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegment|GetDocByBizID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		log.ErrorContextf(ctx, "ListDocSegment|GetDocByBizID|err:%+v", err)
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 6.获取切片数据
	common := &model.DocSegmentCommon{
		AppID:      app.ID,
		AppBizID:   appBizID,
		CorpID:     corpID,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
		SheetName:  req.GetSheetName(),
	}
	rsp, err = logicDoc.ListDocSegment(ctx, req, s.dao, common, doc)
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegment|ListDocSegment|err:%+v", err)
		return rsp, err
	}
	if logicDoc.IsExcel(req.GetFileType()) {
		// 如果是excel，需去除表头
		rsp.SegmentList = logicDoc.RemoveTableHeader(ctx, rsp.SegmentList)
	}
	return rsp, nil
}

// ListTableSheet 获取表格sheet列表
func (s *Service) ListTableSheet(ctx context.Context, req *pb.ListTableSheetReq) (*pb.ListTableSheetRsp, error) {
	log.InfoContextf(ctx, "ListTableSheet|Req:%+v", req)
	rsp := new(pb.ListTableSheetRsp)
	// 1.校验基础参数
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 {
		log.ErrorContextf(ctx, "ListTableSheet|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|AppBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 2.获取企业信息
	staffBizID, corpBizID, corpID := pkg.StaffBizID(ctx), pkg.CorpBizID(ctx), pkg.CorpID(ctx)
	// 3.获取应用信息
	app, err := s.getAppByAppBizID(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 4.获取文档信息
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|GetDocByBizID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 5.获取切片数据
	common := &model.DocSegmentCommon{
		AppID:      app.ID,
		AppBizID:   appBizID,
		CorpID:     corpID,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
	}
	// 6.结构化数据初始化
	text2SqlReq := &pb.Text2SqlPreviewTableReq{
		AppBizId:   appBizID,
		DocBizId:   docBizID,
		PageNumber: 1,
		PageSize:   1,
	}
	_, err = s.Text2SqlPreviewTable(ctx, text2SqlReq)
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|Text2SqlPreviewTable|err:%+v", err)
		return nil, err
	}
	rsp, err = logicDoc.ListTableSheet(ctx, req, s.dao, common, doc)
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|ListTableSheet|err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

// ModifyDocSegment 修改文档切片
func (s *Service) ModifyDocSegment(ctx context.Context, req *pb.ModifyDocSegmentReq) (*pb.ModifyDocSegmentRsp, error) {
	log.InfoContextf(ctx, "ModifyDocSegment|Req:%+v", req)
	rsp := new(pb.ModifyDocSegmentRsp)
	// 1.校验基础参数
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 ||
		(len(req.GetDeleteSegBizIds()) == 0 && len(req.GetModifySegments()) == 0 &&
			len(req.GetEnableSegBizIds()) == 0 && len(req.GetDisabledSegBizIds()) == 0) {
		log.ErrorContextf(ctx, "ModifyDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	req.SheetName = strings.Split(req.GetSheetName(), ":")[0]
	appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ModifyDocSegment|AppBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ModifyDocSegment|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 2.获取企业信息
	staffBizID, corpBizID, corpID := pkg.StaffBizID(ctx), pkg.CorpBizID(ctx), pkg.CorpID(ctx)
	// 3.获取应用信息
	app, err := s.getAppByAppBizID(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyDocSegment|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 4.获取文档信息
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyDocSegment|GetDocByID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 5.获取切片数据
	common := &model.DocSegmentCommon{
		AppID:      app.ID,
		AppBizID:   appBizID,
		CorpID:     corpID,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
	}
	rsp, err = logicDoc.ModifyDocSegment(ctx, req, s.dao, common, doc)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyDocSegment|ModifyDocSegment|err:%+v", err)
		return rsp, err
	}
	// 更新文档的修改人
	updateDocFilter := &dao.DocFilter{
		IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
	}
	update := &model.Doc{
		StaffID:    pkg.StaffID(ctx),
		UpdateTime: time.Now(),
	}
	updateDocColumns := []string{dao.DocTblColStaffId, dao.DocTblColUpdateTime}
	_, err = dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil { //柔性放过
		log.ErrorContextf(ctx, "ModifyDocSegment|UpdateDoc|err:%+v", err)
	}
	return rsp, nil
}

// ModifyTableSheet 修改表格sheet
func (s *Service) ModifyTableSheet(ctx context.Context, req *pb.ModifyTableSheetReq) (*pb.ModifyTableSheetRsp, error) {
	ctx = clues.NewTrackContext(ctx)
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	log.InfoContextf(ctx, "ModifyTableSheet|Req:%+v", req)
	rsp := new(pb.ModifyTableSheetRsp)
	// 1.校验基础参数
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 ||
		(len(req.GetModifyTableSheets()) == 0 && len(req.GetDeleteSheetBizIds()) == 0 &&
			len(req.GetDisabledSheetBizIds()) == 0 && len(req.GetEnableSheetBizIds()) == 0 &&
			len(req.GetDisabledRetrievalEnhanceSheetNames()) == 0 && len(req.GetEnableRetrievalEnhanceSheetNames()) == 0) {
		log.ErrorContextf(ctx, "ModifyTableSheet|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ModifyTableSheet|AppBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ModifyTableSheet|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 2.获取企业信息
	staffBizID, corpBizID, corpID := pkg.StaffBizID(ctx), pkg.CorpBizID(ctx), pkg.CorpID(ctx)
	// 3.获取应用信息
	app, err := s.getAppByAppBizID(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyTableSheet|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 4.获取文档信息
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyTableSheet|GetDocByID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 5.获取切片数据
	common := &model.DocSegmentCommon{
		AppID:      app.ID,
		AppBizID:   appBizID,
		CorpID:     corpID,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
	}
	rsp, err = logicDoc.ModifyTableSheet(ctx, req, s.dao, common)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyTableSheet|ModifyTableSheet|err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

// CreateDocParsingIntervention 提交切片干预任务
func (s *Service) CreateDocParsingIntervention(ctx context.Context, req *pb.CreateDocParsingInterventionReq) (*pb.CreateDocParsingInterventionRsp, error) {
	ctx = clues.NewTrackContext(ctx)
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	log.InfoContextf(ctx, "CreateDocParsingIntervention|Req:%+v", req)
	rsp := new(pb.CreateDocParsingInterventionRsp)
	key := fmt.Sprintf(dao.LockForCreateDocParsingIntervention, req.OriginDocBizId)
	if err := s.dao.Lock(ctx, key, 120*time.Second); err != nil {
		return nil, errs.ErrDocNotSupportInterveneFailed
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	// 校验基础参数
	if len(req.AppBizId) == 0 || len(req.OriginDocBizId) == 0 {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|AppBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	originDocBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetOriginDocBizId())
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 获取企业信息
	staffBizID, staffID, corpBizID, corpID := pkg.StaffBizID(ctx), pkg.StaffID(ctx), pkg.CorpBizID(ctx), pkg.CorpID(ctx)
	// 获取应用信息
	app, err := s.getAppByAppBizID(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 获取旧文档信息
	originDoc, err := s.dao.GetDocByBizID(ctx, originDocBizId, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|GetDocByID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	if originDoc.RobotID != app.ID {
		return rsp, errs.ErrWrapf(errs.ErrDocNotFound, i18n.Translate(ctx, i18nkey.KeyDocumentNotInCurrentApp))
	}
	// 文档非可干预状态拦截
	if !originDoc.IsValidIntervene(originDoc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	if !app.IsShared && originDoc.Status == model.DocStatusReleaseSuccess && releaseCount >= int64(config.App().
		RobotDefault.
		DocReleaseMaxLimit) {
		return rsp, errs.ErrReleaseMaxCount
	}
	// 获取切片数据
	common := &model.DocSegmentCommon{
		AppID:      app.ID,
		AppBizID:   appBizID,
		CorpID:     corpID,
		CorpBizID:  corpBizID,
		StaffID:    staffID,
		StaffBizID: staffBizID,
		DocBizID:   originDocBizId,
		DocID:      originDoc.ID,
		DataSource: uint32(logicDoc.GetDataSource(ctx, originDoc.SplitRule)),
	}
	// 校验字符数
	if err = CheckIsUsedCharSizeExceeded(ctx, s.dao, appBizID, corpID); err != nil {
		return rsp, s.dao.ConvertErrMsg(ctx, 0, app.CorpID, err)
	}
	// 审核
	auditFlag, err := s.getAuditFlag(req.GetFileType())
	if err != nil {
		return rsp, err
	}
	// (特殊处理逻辑)若为表格文档且仅修改结构化数据则触发同步任务
	if logicDoc.IsExcel(req.GetFileType()) {
		intervene, err := logicDoc.CheckTableContentIntervene(ctx, common)
		if err != nil {
			return rsp, err
		}
		if intervene == false {
			return s.TableStructuredDataIntervention(ctx, common, originDoc)
		}
	}
	rsp, err = logicDoc.CreateDocParsingIntervention(ctx, s.dao, common, auditFlag, originDoc)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|CreateDocParsingIntervention|err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

func (s *Service) TableStructuredDataIntervention(ctx context.Context, docCommon *model.DocSegmentCommon,
	doc *model.Doc) (*pb.CreateDocParsingInterventionRsp, error) {
	rsp := new(pb.CreateDocParsingInterventionRsp)
	err := doc_intervene.UpdateSheet2Knowledge(ctx, docCommon.AppBizID, docCommon.DocBizID)
	// 直接更新旧文档的状态
	updateDocFilter := &dao.DocFilter{
		IDs:     []uint64{doc.ID},
		CorpId:  doc.CorpID,
		RobotId: doc.RobotID,
	}
	update := &model.Doc{
		StaffID:    pkg.StaffID(ctx),
		UpdateTime: time.Now(),
	}
	updateDocColumns := []string{dao.DocTblColStaffId, dao.DocTblColUpdateTime}
	if err != nil {
		log.ErrorContextf(ctx, "TableStructuredDataIntervention|UpdateSheet2Knowledge|err:%v", err)
		update = &model.Doc{
			Message:    i18nkey.KeyTableStructuredInfoLearningFailed,
			Status:     model.DocStatusCreateIndexFail,
			UpdateTime: time.Now(),
		}
		updateDocColumns = []string{dao.DocTblColMessage, dao.DocTblColStatus, dao.DocTblColUpdateTime}
	}
	_, err = dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		log.ErrorContextf(ctx, "TableStructuredDataIntervention|UpdateDocStatus|doc_id:%d|err:%+v", doc.ID, err)
		return rsp, err
	}
	return rsp, err
}

func (s *Service) getOldDocAttributeLabel(ctx context.Context,
	doc *model.Doc, appID uint64) (*model.UpdateDocAttributeLabelReq, error) {
	log.InfoContextf(ctx, "getOldDocAttributeLabel start")
	rsp := new(model.UpdateDocAttributeLabelReq)
	mapAttrLabel, err := s.dao.GetDocAttributeLabelDetail(ctx, doc.RobotID, []uint64{doc.ID})
	if err != nil {
		log.ErrorContextf(ctx, "getOldDocAttributeLabel|GetDocAttributeLabelDetail|err:%+v", err)
		return rsp, err
	}
	for _, v := range mapAttrLabel[doc.ID] {
		if v == nil {
			log.ErrorContextf(ctx, "getOldDocAttributeLabel|AttrLabels has nil member")
			return rsp, errs.ErrAttributeLabelNotFound
		}
	}
	attlLabelList := mapAttrLabel[doc.ID]
	var attrLabelReferList []*pb.AttrLabelRefer
	for _, v := range attlLabelList {
		var labelBizIDs []string
		for _, l := range v.Labels {
			labelBizIDs = append(labelBizIDs, strconv.FormatUint(l.BusinessID, 10))
		}
		attrLabel := &pb.AttrLabelRefer{
			Source:         v.Source,
			AttributeBizId: strconv.FormatUint(v.BusinessID, 10),
			LabelBizIds:    labelBizIDs,
		}
		attrLabelReferList = append(attrLabelReferList, attrLabel)
	}
	var attrRange uint32
	if len(attrLabelReferList) > 0 {
		attrRange = model.AttrRangeCondition
	} else {
		attrRange = model.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, appID, config.App().AttributeLabel.DocAttrLimit,
		config.App().AttributeLabel.DocAttrLabelLimit, attrRange, attrLabelReferList)
	if err != nil {
		return rsp, err
	}
	docAttributeLabelsFromPB, err := fillDocAttributeLabelsFromPB(ctx, attrLabelReferList, true, attrs, labels)
	if err != nil {
		return nil, err
	}
	log.InfoContextf(ctx, "getOldDocAttributeLabel|docAttributeLabelsFromPB:%v", &docAttributeLabelsFromPB)
	return docAttributeLabelsFromPB, nil
}
