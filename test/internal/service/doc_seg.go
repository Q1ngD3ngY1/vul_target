package service

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/contextx/clues"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	logicCommon "git.woa.com/adp/kb/kb-config/internal/logic/common"
	logicDoc "git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	KeywordsMaxLimit = 100
)

// DescribeSegments 获取片段详情
func (s *Service) DescribeSegments(ctx context.Context, req *pb.DescribeSegmentsReq) (*pb.DescribeSegmentsRsp, error) {
	ctx = clues.NewTrackContext(ctx)
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	clues.AddTrackData(ctx, "DescribeSegmentsReq", req)
	logx.I(ctx, "DescribeSegments req:%v", req)
	rsp := new(pb.DescribeSegmentsRsp)
	if len(req.GetSegBizId()) == 0 {
		return rsp, nil
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	segBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetSegBizId())
	if err != nil {
		return rsp, err
	}
	if len(segBizIDs) == 0 {
		return rsp, errs.ErrSegmentNotFound
	}

	rsp, err = s.segLogic.DescribeSegments(ctx, app.BizId, segBizIDs)
	if err != nil {
		logx.E(ctx, "DescribeSegments|DescribeSegments|err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

// ListDocSegment 获取文档切片列表
func (s *Service) ListDocSegment(ctx context.Context, req *pb.ListDocSegmentReq) (*pb.ListDocSegmentRsp, error) {
	logx.I(ctx, "ListDocSegment|Req:%+v", req)
	rsp := new(pb.ListDocSegmentRsp)
	// 1.校验基础参数
	if utf8.RuneCountInString(req.Keywords) > KeywordsMaxLimit {
		logx.E(ctx, "ListDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrDocSegmentKeywordsMaxLimit
	}
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 || req.PageNumber < 1 || req.PageSize < 1 {
		logx.E(ctx, "ListDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		logx.E(ctx, "ListDocSegment|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 2.页大小不能超过200，会导致索引失效
	pageSize := req.GetPageSize()
	if pageSize > segEntity.DocSegmentInterveneMaxPageSize {
		logx.E(ctx, "ListDocSegment|PageSize is larger than 200|PageSize:%d", req.PageSize)
		return rsp, errs.ErrGetDocSegmentTooLarge
	}
	// 3.获取企业信息
	staffBizID, corpBizID := contextx.Metadata(ctx).StaffBizID(), contextx.Metadata(ctx).CorpBizID()
	// 4.获取应用信息
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "ListDocSegment|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 5.获取文档信息
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "ListDocSegment|GetDocByBizID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		logx.E(ctx, "ListDocSegment|GetDocByBizID|err:%+v", err)
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 6.获取切片数据
	common := &segEntity.DocSegmentCommon{
		AppID:      app.PrimaryId,
		AppBizID:   app.BizId,
		CorpID:     app.CorpPrimaryId,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
		SheetName:  req.GetSheetName(),
	}
	rsp, err = s.segLogic.ListDocSegment(ctx, req, common, doc)
	if err != nil {
		logx.E(ctx, "ListDocSegment|ListDocSegment|err:%+v", err)
		return rsp, err
	}

	if doc.IsExcelx() {
		// 如果是excel，需去除表头
		rsp.SegmentList = s.RemoveTableHeader(ctx, rsp.SegmentList)
	}
	return rsp, nil
}

// RemoveTableHeader 从切片中移除表格的表头
func (s *Service) RemoveTableHeader(ctx context.Context,
	docSegments []*pb.ListDocSegmentRsp_DocSegmentItem) []*pb.ListDocSegmentRsp_DocSegmentItem {
	if docSegments == nil || len(docSegments) == 0 {
		return docSegments
	}
	for i := range docSegments {
		docSegments[i].OrgData = s.segLogic.GetSliceTable(docSegments[i].OrgData, 1)
		logx.D(ctx, "RemoveTableHeader|OrgData:%s", docSegments[i].OrgData)
	}
	return docSegments
}

// ListTableSheet 获取表格sheet列表
func (s *Service) ListTableSheet(ctx context.Context, req *pb.ListTableSheetReq) (*pb.ListTableSheetRsp, error) {
	logx.I(ctx, "ListTableSheet|Req:%+v", req)
	rsp := new(pb.ListTableSheetRsp)
	// 1.校验基础参数
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 {
		logx.E(ctx, "ListTableSheet|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		logx.E(ctx, "ListTableSheet|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 2.获取企业信息
	staffBizID, corpBizID := contextx.Metadata(ctx).StaffBizID(), contextx.Metadata(ctx).CorpBizID()
	// 3.获取应用信息
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "ListTableSheet|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 4.获取文档信息
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "ListTableSheet|GetDocByBizID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 5.获取切片数据
	common := &segment.DocSegmentCommon{
		AppID:      app.PrimaryId,
		AppBizID:   app.BizId,
		CorpID:     app.CorpPrimaryId,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
	}
	// 6.结构化数据初始化
	text2SqlReq := &pb.Text2SqlPreviewTableReq{
		AppBizId:   app.BizId,
		DocBizId:   docBizID,
		PageNumber: 1,
		PageSize:   1,
	}
	_, err = s.Text2SqlPreviewTable(ctx, text2SqlReq)
	if err != nil {
		logx.E(ctx, "ListTableSheet|Text2SqlPreviewTable|err:%+v", err)
		return nil, err
	}
	rsp, err = s.docLogic.ListTableSheet(ctx, req, common, doc)
	if err != nil {
		logx.E(ctx, "ListTableSheet|ListTableSheet|err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

// ModifyDocSegment 修改文档切片
func (s *Service) ModifyDocSegment(ctx context.Context, req *pb.ModifyDocSegmentReq) (*pb.ModifyDocSegmentRsp, error) {
	logx.I(ctx, "ModifyDocSegment|Req:%+v", req)
	rsp := new(pb.ModifyDocSegmentRsp)
	// 1.校验基础参数
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 ||
		(len(req.GetDeleteSegBizIds()) == 0 && len(req.GetModifySegments()) == 0 &&
			len(req.GetEnableSegBizIds()) == 0 && len(req.GetDisabledSegBizIds()) == 0) {
		logx.E(ctx, "ModifyDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		logx.E(ctx, "ModifyDocSegment|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 2.获取企业信息
	staffBizID, corpBizID := contextx.Metadata(ctx).StaffBizID(), contextx.Metadata(ctx).CorpBizID()
	// 3.获取应用信息
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "ModifyDocSegment|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 4.获取文档信息
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "ModifyDocSegment|GetDocByID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 5.获取切片数据
	common := &segment.DocSegmentCommon{
		AppID:      app.PrimaryId,
		AppBizID:   app.BizId,
		CorpID:     app.CorpPrimaryId,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
	}
	rsp, err = s.segLogic.ModifyDocSegment(ctx, req, common, doc)
	if err != nil {
		logx.E(ctx, "ModifyDocSegment|ModifyDocSegment|err:%+v", err)
		return rsp, err
	}
	// 更新文档的修改人
	updateDocFilter := &docEntity.DocFilter{
		IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
	}
	update := &docEntity.Doc{
		StaffID:    contextx.Metadata(ctx).StaffID(),
		UpdateTime: time.Now(),
	}
	updateDocColumns := []string{docEntity.DocTblColStaffId, docEntity.DocTblColUpdateTime}
	_, err = s.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil { // 柔性放过
		logx.E(ctx, "ModifyDocSegment|UpdateDoc|err:%+v", err)
	}
	return rsp, nil
}

// ModifyTableSheet 修改表格sheet
func (s *Service) ModifyTableSheet(ctx context.Context, req *pb.ModifyTableSheetReq) (*pb.ModifyTableSheetRsp, error) {
	ctx = clues.NewTrackContext(ctx)
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	logx.I(ctx, "ModifyTableSheet|Req:%+v", req)
	rsp := new(pb.ModifyTableSheetRsp)
	// 1.校验基础参数
	if len(req.AppBizId) == 0 || len(req.DocBizId) == 0 ||
		(len(req.GetModifyTableSheets()) == 0 && len(req.GetDeleteSheetBizIds()) == 0 &&
			len(req.GetDisabledSheetBizIds()) == 0 && len(req.GetEnableSheetBizIds()) == 0 &&
			len(req.GetDisabledRetrievalEnhanceSheetNames()) == 0 && len(req.GetEnableRetrievalEnhanceSheetNames()) == 0) {
		logx.E(ctx, "ModifyTableSheet|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		logx.E(ctx, "ModifyTableSheet|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 2.获取企业信息
	staffBizID, corpBizID := contextx.Metadata(ctx).StaffBizID(), contextx.Metadata(ctx).CorpBizID()
	// 3.获取应用信息
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "ModifyTableSheet|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 4.获取文档信息
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "ModifyTableSheet|GetDocByID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	// 文档非可干预状态拦截
	if !doc.IsValidIntervene(doc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 5.获取切片数据
	common := &segment.DocSegmentCommon{
		AppID:      app.PrimaryId,
		AppBizID:   app.BizId,
		CorpID:     app.CorpPrimaryId,
		CorpBizID:  corpBizID,
		StaffBizID: staffBizID,
		DocBizID:   docBizID,
		DocID:      doc.ID,
	}
	rsp, err = s.docLogic.ModifyTableSheet(ctx, req, common)
	if err != nil {
		logx.E(ctx, "ModifyTableSheet|ModifyTableSheet|err:%+v", err)
		return rsp, err
	}
	return rsp, nil
}

// CreateDocParsingIntervention 提交切片干预任务
func (s *Service) CreateDocParsingIntervention(ctx context.Context, req *pb.CreateDocParsingInterventionReq) (*pb.CreateDocParsingInterventionRsp, error) {
	ctx = clues.NewTrackContext(ctx)
	defer func(ctx *context.Context) { clues.Flush(*ctx) }(&ctx)
	logx.I(ctx, "CreateDocParsingIntervention|Req:%+v", req)
	rsp := new(pb.CreateDocParsingInterventionRsp)
	key := fmt.Sprintf(dao.LockForCreateDocParsingIntervention, req.OriginDocBizId)
	if err := s.dao.Lock(ctx, key, 120*time.Second); err != nil {
		return nil, errs.ErrDocNotSupportInterveneFailed
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	// 校验基础参数
	if len(req.AppBizId) == 0 || len(req.OriginDocBizId) == 0 {
		logx.E(ctx, "CreateDocParsingIntervention|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	originDocBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetOriginDocBizId())
	if err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|DocBizIDToUint64|err:%+v", err)
		return rsp, err
	}
	// 获取企业信息
	staffBizID, staffID, corpBizID := contextx.Metadata(ctx).StaffBizID(), contextx.Metadata(ctx).StaffID(), contextx.Metadata(ctx).CorpBizID()
	// 获取应用信息
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|getAppByAppBizID|err:%+v", err)
		return rsp, errs.ErrRobotNotFound
	}
	// 获取旧文档信息
	originDoc, err := s.docLogic.GetDocByBizID(ctx, originDocBizId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|GetDocByID|err:%+v", err)
		return rsp, errs.ErrDocNotFound
	}
	if originDoc.RobotID != app.PrimaryId {
		return rsp, errs.ErrWrapf(errs.ErrDocNotFound, "当前应用中不存在该文档")
	}
	// 文档非可干预状态拦截
	if !originDoc.IsValidIntervene(originDoc.Status) {
		return rsp, errs.ErrDocNotSupportInterveneFailed
	}
	// 获取切片数据
	common := &segment.DocSegmentCommon{
		AppID:      app.PrimaryId,
		AppBizID:   app.BizId,
		CorpID:     app.CorpPrimaryId,
		CorpBizID:  corpBizID,
		StaffID:    staffID,
		StaffBizID: staffBizID,
		DocBizID:   originDocBizId,
		DocID:      originDoc.ID,
		DataSource: uint32(logicDoc.GetDataSource(ctx, originDoc.SplitRule)),
	}
	// 校验字符数
	if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
		return rsp, logicCommon.ConvertErrMsg(ctx, s.rpc, 0, app.CorpPrimaryId, err)
	}
	// 审核
	auditFlag, err := util.GetFileAuditFlag(req.GetFileType())
	if err != nil {
		return rsp, err
	}
	// (特殊处理逻辑)若为表格文档且仅修改结构化数据则触发同步任务
	if req.FileType == docEntity.FileTypeXlsx || req.FileType == docEntity.FileTypeXls ||
		req.FileType == docEntity.FileTypeCsv || req.FileType == docEntity.FileTypeNumbers {
		intervene, err := s.docLogic.CheckTableContentIntervene(ctx, common)
		if err != nil {
			return rsp, err
		}
		if intervene == false {
			return s.TableStructuredDataIntervention(ctx, common, originDoc)
		}
	}
	rsp, err = s.docLogic.CreateDocParsingIntervention(ctx, common, auditFlag, originDoc)
	if err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|CreateDocParsingIntervention|err:%+v", err)
		return rsp, err
	}
	auditx.Modify(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, originDoc.BusinessID, originDoc.FileName,
		i18n.Translate(ctx, "CreateDocParsingIntervention"))
	return rsp, nil
}

func (s *Service) TableStructuredDataIntervention(ctx context.Context, docCommon *segment.DocSegmentCommon,
	doc *docEntity.Doc) (*pb.CreateDocParsingInterventionRsp, error) {
	rsp := new(pb.CreateDocParsingInterventionRsp)
	err := s.docLogic.UpdateSheet2Knowledge(ctx, docCommon.AppBizID, docCommon.DocBizID)
	// 直接更新旧文档的状态
	updateDocFilter := &docEntity.DocFilter{
		IDs:     []uint64{doc.ID},
		CorpId:  doc.CorpID,
		RobotId: doc.RobotID,
	}
	update := &docEntity.Doc{
		StaffID:    contextx.Metadata(ctx).StaffID(),
		UpdateTime: time.Now(),
	}
	updateDocColumns := []string{docEntity.DocTblColStaffId, docEntity.DocTblColUpdateTime}
	if err != nil {
		logx.E(ctx, "TableStructuredDataIntervention|UpdateSheet2Knowledge|err:%v", err)
		update = &docEntity.Doc{
			Message:    i18nkey.KeyTableStructuredInfoLearningFailed,
			Status:     docEntity.DocStatusCreateIndexFail,
			UpdateTime: time.Now(),
		}
		updateDocColumns = []string{docEntity.DocTblColMessage, docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime}
	}
	_, err = s.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		logx.E(ctx, "TableStructuredDataIntervention|UpdateDocStatus|doc_id:%d|err:%+v", doc.ID, err)
		return rsp, err
	}
	return rsp, err
}

func (s *Service) getOldDocAttributeLabel(ctx context.Context,
	doc *docEntity.Doc, appID uint64) (*labelEntity.UpdateDocAttributeLabelReq, error) {
	logx.I(ctx, "getOldDocAttributeLabel start")
	rsp := new(labelEntity.UpdateDocAttributeLabelReq)
	mapAttrLabel, err := s.labelLogic.GetDocAttributeLabelDetail(ctx, doc.RobotID, []uint64{doc.ID})
	if err != nil {
		logx.E(ctx, "getOldDocAttributeLabel|GetDocAttributeLabelDetail|err:%+v", err)
		return rsp, err
	}
	for _, v := range mapAttrLabel[doc.ID] {
		if v == nil {
			logx.E(ctx, "getOldDocAttributeLabel|AttrLabels has nil member")
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
		attrRange = docEntity.AttrRangeCondition
	} else {
		attrRange = docEntity.AttrRangeAll
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
	logx.I(ctx, "getOldDocAttributeLabel|docAttributeLabelsFromPB:%v", &docAttributeLabelsFromPB)
	return docAttributeLabelsFromPB, nil
}
