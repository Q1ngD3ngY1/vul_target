package service

import (
	"context"
	"fmt"
	"slices"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity0 "git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// parseSimilarLabels TODO
func parseSimilarLabels(values []string) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	similarLabels := make([]string, 0)
	for _, value := range values {
		if len(value) == 0 {
			continue
		}
		similarLabels = append(similarLabels, value)
	}
	return jsonx.MarshalToString(similarLabels)
}

// checkAttributeUsed 检查属性标签是否已被使用
func (s *Service) checkAttributeUsed(ctx context.Context, app *entity0.App, attributesBizIDs []uint64) error {
	if app.QaConfig == nil ||
		len(app.QaConfig.SearchRange.ApiVarAttrInfos) == 0 {
		return nil
	}
	apiVarAttrInfoMap := make(map[uint64]struct{})
	for _, apiVarAttrInfo := range app.QaConfig.SearchRange.ApiVarAttrInfos {
		apiVarAttrInfoMap[apiVarAttrInfo.AttrBizID] = struct{}{}
	}

	for _, attributesBizID := range attributesBizIDs {
		if _, ok := apiVarAttrInfoMap[attributesBizID]; ok {
			return errs.ErrAttributeLabelAttrHasUsed
		}
	}
	return nil
}

// checkAttributeLabelUsed 检查属性标签是否已被使用
func (s *Service) checkAttributeLabelUsed(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64) error {
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return nil
	}
	isUsed, err := s.isAttributeLabelUsed(ctx, robotID, source, attrIDs, labelIDs)
	if err != nil {
		return err
	}
	if isUsed {
		return errs.ErrAttributeLabelAttrHasUsed
	}
	return nil
}

// ExportAttributeLabel 导出属性标签
func (s *Service) ExportAttributeLabel(ctx context.Context, req *pb.ExportAttributeLabelReq) (
	*pb.ExportAttributeLabelRsp, error) {
	rsp := new(pb.ExportAttributeLabelRsp)
	staffID := contextx.Metadata(ctx).StaffID()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	_, err = util.CheckReqSliceUint64(ctx, req.GetAttributeBizIds())
	if err != nil {
		return rsp, err
	}
	paramStr, err := jsonx.MarshalToString(req)
	if err != nil {
		logx.D(ctx, "json marshl to string req:%+v, err:%+v", req, err)
		return rsp, err
	}
	now := time.Now()
	export := &entity0.Export{
		CorpID:        app.CorpPrimaryId,
		RobotID:       app.PrimaryId,
		CreateStaffID: staffID,
		TaskType:      entity0.ExportAttributeLabelTaskType,
		Name:          entity0.ExportAttributeLabelTaskName,
		Params:        paramStr,
		Status:        qaEntity.TaskExportStatusInit,
		UpdateTime:    now,
		CreateTime:    now,
	}
	params := &entity0.ExportParams{
		CorpID:           app.CorpPrimaryId,
		RobotID:          app.PrimaryId,
		AppBizID:         app.BizId,
		CreateStaffID:    staffID,
		FileName:         fmt.Sprintf("export-%d-%d.xlsx", entity0.ExportAttributeLabelTaskType, time.Now().Unix()),
		TaskType:         entity0.ExportAttributeLabelTaskType,
		TaskName:         entity0.ExportAttributeLabelTaskName,
		Params:           paramStr,
		NoticeContent:    i18n.Translate(ctx, labelEntity.AttributeLabelNoticeContent),
		NoticeContentIng: i18n.Translate(ctx, labelEntity.AttributeLabelNoticeContentIng),
		NoticePageID:     releaseEntity.NoticeAttributeLabelPageID,
		NoticeTypeExport: releaseEntity.NoticeTypeAttributeLabelExport,
	}
	taskID, err := s.exportLogic.CreateExportTask(ctx, app.CorpPrimaryId, staffID, app.PrimaryId, export, params)
	if err != nil {
		logx.E(ctx, "create export task req:%+v, err:%+v", req, err)
		return rsp, err
	}
	rsp.TaskId = taskID
	return rsp, nil
}

// CheckAttributeLabelExist 检查属性下的标签名是否存在
func (s *Service) CheckAttributeLabelExist(ctx context.Context, req *pb.CheckAttributeLabelExistReq) (
	*pb.CheckAttributeLabelExistRsp, error) {
	logx.I(ctx, "CheckAttributeLabelExist|req:%+v", req)
	rsp := new(pb.CheckAttributeLabelExistRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	attributeBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetAttributeBizId())
	if err != nil {
		return nil, err
	}
	attributeMap, err := s.labelDao.GetAttributeByBizIDs(ctx, app.PrimaryId, []uint64{attributeBizId})
	if err != nil || len(attributeMap) != 1 {
		logx.E(ctx, "CheckAttributeLabelExist|AttributeBizId:%d, len(attributeMap):%d, err:%+v",
			req.GetAttributeBizId(), len(attributeMap), err)
		return rsp, errs.ErrAttributeLabelNotFound
	}
	attribute, ok := attributeMap[attributeBizId]
	if !ok || attribute == nil {
		logx.E(ctx, "CheckAttributeLabelExist|attribute:%+v", attribute)
		return rsp, errs.ErrAttributeLabelNotFound
	}
	labels, err := s.labelDao.GetAttributeLabelByName(ctx, attribute.ID, req.GetLabelName(), app.PrimaryId)
	if err != nil {
		return rsp, err
	}
	lastLabelID := uint64(0)
	if len(req.GetLastLabelBizId()) > 0 {
		lastLabelBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetLastLabelBizId())
		if err != nil {
			return nil, err
		}
		labelMap, err := s.labelDao.GetAttributeLabelByBizIDs(ctx, []uint64{lastLabelBizId}, app.PrimaryId)
		if err != nil || len(labelMap) != 1 {
			logx.E(ctx, "CheckAttributeLabelExist|LastLabelBizID:%d, len(labelMap):%d, err:%+v",
				req.GetLastLabelBizId(), len(labelMap), err)
			return rsp, errs.ErrAttributeLabelNotFound
		}
		lastLabel, ok := labelMap[lastLabelBizId]
		if !ok || lastLabel == nil {
			logx.E(ctx, "CheckAttributeLabelExist|lastLabel:%+v", lastLabel)
			return rsp, errs.ErrAttributeLabelNotFound
		}
		lastLabelID = lastLabel.ID
	}
	for _, v := range labels {
		if v.Name == req.GetLabelName() && (lastLabelID == 0 || v.ID < lastLabelID) {
			rsp.IsExist = true
			return rsp, nil
		}
		similarLabels, _ := getSimilarLabels(v.SimilarLabel)
		if slices.Contains(similarLabels, req.GetLabelName()) && (lastLabelID == 0 || v.ID < lastLabelID) {
			rsp.IsExist = true
			return rsp, nil
		}
	}
	return rsp, nil
}

// getAttributeLabelRow TODO
func (s *Service) getAttributeLabelRow(ctx context.Context, req *pb.UploadAttributeLabelReq, fileName string,
	body []byte, check util.CheckFunc) ([][]string, *pb.UploadAttributeLabelRsp, error) {
	// 将配置中文件头翻译成ctx中语言
	var checkHead []string
	for _, v := range config.App().AttributeLabel.ExeclHead {
		checkHead = append(checkHead, i18n.Translate(ctx, v))
	}
	logx.I(ctx, "getAttributeLabelRow checkHead:%v", checkHead)
	rows, bs, err := util.CheckContent(ctx, fileName, config.App().AttributeLabel.MinRow,
		config.App().AttributeLabel.MaxRow, checkHead, body, check)
	if err != nil {
		if err != errs.ErrExcelContent {
			logx.W(ctx, "checkAttributeLabelXlsx file check excel err :%v", err)
			return nil, nil, err
		}
		key := req.GetCosUrl() + ".check.xlsx"
		if err := s.s3.PutObject(ctx, bs, key); err != nil {
			return nil, nil, errs.ErrSystem
		}
		url, err := s.s3.GetPreSignedURL(ctx, key)
		if err != nil {
			logx.D(ctx, "UploadSampleFile file write excl err :%v", err)
			return nil, nil, errs.ErrSystem
		}
		return nil, &pb.UploadAttributeLabelRsp{
			ErrorMsg:      i18n.Translate(ctx, i18nkey.KeyFileDataErrorPleaseDownloadErrorFile),
			ErrorLink:     url,
			ErrorLinkText: i18n.Translate(ctx, i18nkey.KeyDownload),
		}, nil
	}
	return rows, nil, nil
}

func (s *Service) checkAttributeLabelRefer(ctx context.Context, robotID uint64, attrLimit int, attrLabelLimit int,
	attrRange uint32, refers []*pb.AttrLabelRefer) (map[uint64]*labelEntity.Attribute, map[uint64]*labelEntity.AttributeLabel, error) {
	if err := checkAttributeLabelReferBasicData(ctx, attrLimit, attrRange, refers); err != nil {
		return nil, nil, err
	}
	return s.checkAttributeLabelReferOfKg(ctx, robotID, refers, attrLabelLimit)
}

// checkAttributeLabelReferBasicData TODO
func checkAttributeLabelReferBasicData(ctx context.Context, attrLimit int, attrRange uint32, refers []*pb.AttrLabelRefer) error {
	switch attrRange {
	case docEntity.AttrRangeAll:
		if len(refers) > 0 {
			return errs.ErrAttributeLabelRefer
		}
		return nil
	case docEntity.AttrRangeCondition:
		if len(refers) == 0 {
			return errs.ErrAttributeLabelRefer
		}
		if len(refers) > attrLimit {
			return errs.ErrWrapf(errs.ErrAttributeReferLimit, i18n.Translate(ctx, i18nkey.KeyKnowledgeAssociatedTagCountExceedLimit),
				len(refers), attrLimit)
		}
	default:
		return errs.ErrAttributeLabelRefer
	}
	mapSourceAttrID := make(map[string]struct{})
	mapSourceLabelID := make(map[string]struct{})
	for _, v := range refers {
		if !labelEntity.IsAttributeLabelReferSource(v.GetSource()) {
			return errs.ErrAttributeLabelSource
		}
		sourceAttrID := fmt.Sprintf("%d_%s", v.GetSource(), v.GetAttributeBizId())
		if _, ok := mapSourceAttrID[sourceAttrID]; ok {
			return errs.ErrAttributeLabelRepeated
		}
		mapSourceAttrID[sourceAttrID] = struct{}{}
		for _, labelBizID := range v.GetLabelBizIds() {
			if labelBizID == "0" && len(v.GetLabelBizIds()) > 1 {
				return errs.ErrAttributeLabelRefer
			}
			if labelBizID == "0" {
				continue
			}
			sourceLabelID := fmt.Sprintf("%d_%s", v.GetSource(), labelBizID)
			if _, ok := mapSourceLabelID[sourceLabelID]; ok {
				return errs.ErrAttributeLabelRepeated
			}
			mapSourceLabelID[sourceLabelID] = struct{}{}
		}
	}
	return nil
}

func (s *Service) checkAttributeLabelReferOfKg(ctx context.Context, robotID uint64,
	refers []*pb.AttrLabelRefer, attrLabelLimit int) (map[uint64]*labelEntity.Attribute, map[uint64]*labelEntity.AttributeLabel, error) {
	logx.I(ctx, "checkAttributeLabelReferOfKg robotID : %v ", robotID)
	attrBizIDs, labelBizIDs, mapLabelBizID2AttrBizID, err := getAttrLabelReferOfSource(ctx, refers, labelEntity.AttributeLabelSourceKg)
	if err != nil {
		return nil, nil, err
	}
	logx.I(ctx, "checkAttributeLabelReferOfKg attrBizIDs:%+v, labelBizIDs: %v", attrBizIDs, labelBizIDs)
	if len(attrBizIDs) == 0 && len(labelBizIDs) == 0 {
		return nil, nil, nil
	}
	mapAttrID2Info, err := s.labelDao.GetAttributeByBizIDs(ctx, robotID, attrBizIDs)
	if err != nil {
		logx.E(ctx, "checkAttributeLabelReferOfKg err:%+v", err)
		return nil, nil, err
	}
	mapLabelID2Info, err := s.labelDao.GetAttributeLabelByBizIDs(ctx, labelBizIDs, robotID)
	if err != nil {
		logx.E(ctx, "checkAttributeLabelReferOfKg err:%+v", err)
		return nil, nil, err
	}
	if len(mapAttrID2Info) != len(attrBizIDs) || len(mapLabelID2Info) != len(labelBizIDs) {
		logx.E(ctx, "checkAttributeLabelReferOfKg len(mapAttrID2Info) != len(attrBizIDs) || len(mapLabelID2Info) != len(labelBizIDs) ", err)
		return nil, nil, errs.ErrAttributeLabelNotFound
	}
	for labelBizID, label := range mapLabelID2Info {
		logx.I(ctx, "checkAttributeLabelReferOfKg mapLabelBizID2AttrBizID : %v", mapLabelBizID2AttrBizID)
		logx.I(ctx, "checkAttributeLabelReferOfKg labelBizID : %v", labelBizID)
		attrBizID, ok := mapLabelBizID2AttrBizID[labelBizID]
		if !ok {
			logx.E(ctx, "checkAttributeLabelReferOfKg labelBizID:%+v, label:%+v", labelBizID, label)
			return nil, nil, errs.ErrAttributeLabelNotFound
		}
		attr, ok := mapAttrID2Info[attrBizID]
		if !ok {
			return nil, nil, errs.ErrAttributeLabelNotFound
		}
		if attr.ID != label.AttrID {
			return nil, nil, errs.ErrAttributeLabelNotFound
		}
	}
	if len(mapLabelID2Info) > attrLabelLimit {
		return nil, nil, errs.ErrWrapf(errs.ErrAttributeLabelReferLimit, i18n.Translate(ctx, i18nkey.KeyKnowledgeAssociatedTagValueCountExceedLimit),
			len(mapLabelID2Info), attrLabelLimit)
	}
	return mapAttrID2Info, mapLabelID2Info, nil
}

// isDocAllowedToModify 检查文档是否允许修改
func (s *Service) isDocAllowedToModify(ctx context.Context, doc docEntity.Doc, app entity0.App, corpID uint64) error {
	if doc.CorpID != corpID || doc.RobotID != app.PrimaryId {
		return errs.ErrPermissionDenied
	}
	if doc.HasDeleted() {
		return errs.ErrDocHasDeleted
	}
	if !doc.IsAllowEdit() {
		return errs.ErrDocNotAllowEdit
	}
	if doc.IsProcessing([]uint64{docEntity.DocProcessingFlagHandlingDocDiffTask}) {
		return errs.ErrDocDiffTaskRunIng
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, []uint64{doc.ID})
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return errs.ErrSystem
	}
	if _, ok := releasingDocIdMap[doc.ID]; ok {
		return errs.ErrDocIsRelease
	}
	return nil
}

// getAttrLabelReferOfSource TODO
func getAttrLabelReferOfSource(ctx context.Context, refers []*pb.AttrLabelRefer, source uint32) ([]uint64, []uint64, map[uint64]uint64, error) {
	var attrBizIDs, labelBizIDs []uint64
	mapLabelBizID2AttrBizID := make(map[uint64]uint64)
	for _, v := range refers {
		if v.Source != source {
			continue
		}
		attributeBizId, err := util.CheckReqParamsIsUint64(ctx, v.GetAttributeBizId())
		if err != nil {
			return nil, nil, nil, err
		}
		attrBizIDs = append(attrBizIDs, attributeBizId)
		for _, labelBizID := range v.GetLabelBizIds() {
			if labelBizID == "0" {
				continue
			}
			labelBizIDUint64, err := util.CheckReqParamsIsUint64(ctx, labelBizID)
			if err != nil {
				return nil, nil, nil, err
			}
			labelBizIDs = append(labelBizIDs, labelBizIDUint64)
			mapLabelBizID2AttrBizID[labelBizIDUint64] = attributeBizId
		}
	}
	return attrBizIDs, labelBizIDs, mapLabelBizID2AttrBizID, nil
}

// fillDocAttributeLabelsFromPB TODO
func fillDocAttributeLabelsFromPB(ctx context.Context,
	attrLabelRefers []*pb.AttrLabelRefer,
	isNeedChange bool,
	attrs map[uint64]*labelEntity.Attribute,
	labels map[uint64]*labelEntity.AttributeLabel,
) (*labelEntity.UpdateDocAttributeLabelReq, error) {
	req := &labelEntity.UpdateDocAttributeLabelReq{
		IsNeedChange:    isNeedChange,
		AttributeLabels: make([]*labelEntity.DocAttributeLabel, 0),
	}
	if !isNeedChange {
		return req, nil
	}
	for _, v := range attrLabelRefers {
		var attrID uint64
		attributeBizID, err := util.CheckReqParamsIsUint64(ctx, v.GetAttributeBizId())
		if err != nil {
			return nil, err
		}
		if attr, ok := attrs[attributeBizID]; ok {
			attrID = attr.ID
		}
		for _, labelBizID := range v.GetLabelBizIds() {
			var labelID uint64
			labelBizIDUint64, err := util.CheckReqParamsIsUint64(ctx, labelBizID)
			if err != nil {
				return nil, err
			}
			if label, ok := labels[labelBizIDUint64]; ok {
				labelID = label.ID
			}
			req.AttributeLabels = append(req.AttributeLabels, &labelEntity.DocAttributeLabel{
				Source:  v.GetSource(),
				AttrID:  attrID,
				LabelID: labelID,
			})
		}
	}
	return req, nil
}

// fillQAAttributeLabelsFromPB TODO
func fillQAAttributeLabelsFromPB(ctx context.Context,
	attrLabelRefers []*pb.AttrLabelRefer,
	isNeedChange bool,
	attrs map[uint64]*labelEntity.Attribute,
	labels map[uint64]*labelEntity.AttributeLabel,
) (*labelEntity.UpdateQAAttributeLabelReq, error) {
	req := &labelEntity.UpdateQAAttributeLabelReq{
		IsNeedChange:    isNeedChange,
		AttributeLabels: make([]*labelEntity.QAAttributeLabel, 0),
	}
	if !isNeedChange {
		return req, nil
	}
	for _, v := range attrLabelRefers {
		var attrID uint64
		attributeBizID, err := util.CheckReqParamsIsUint64(ctx, v.GetAttributeBizId())
		if err != nil {
			return nil, err
		}
		if attr, ok := attrs[attributeBizID]; ok {
			attrID = attr.ID
		}
		for _, labelBizID := range v.GetLabelBizIds() {
			labelBizIDUint64, err := util.CheckReqParamsIsUint64(ctx, labelBizID)
			if err != nil {
				return nil, err
			}
			var labelID uint64
			if label, ok := labels[labelBizIDUint64]; ok {
				labelID = label.ID
			}
			req.AttributeLabels = append(req.AttributeLabels, &labelEntity.QAAttributeLabel{
				Source:  v.GetSource(),
				AttrID:  attrID,
				LabelID: labelID,
			})
		}
	}
	return req, nil
}

func (s *Service) isQAAttributeLabelChange(
	ctx context.Context,
	robotID, qaID uint64,
	oldAttrRange, attrRange uint32,
	refers []*pb.AttrLabelRefer,
	attrs map[uint64]*labelEntity.Attribute,
	labels map[uint64]*labelEntity.AttributeLabel,
) (bool, error) {
	if oldAttrRange != attrRange {
		return true, nil
	}
	oldRefers, err := s.labelDao.GetQAAttributeLabel(ctx, robotID, []uint64{qaID})
	if err != nil {
		return false, errs.ErrSystem
	}
	mapOldRefer := make(map[string]struct{})
	mapRefer := make(map[string]struct{})
	for _, v := range refers {
		var attrID uint64
		attributeBizId, err := util.CheckReqParamsIsUint64(ctx, v.GetAttributeBizId())
		if err != nil {
			return false, err
		}
		if attr, ok := attrs[attributeBizId]; ok {
			attrID = attr.ID
		}
		for _, labelBizID := range v.GetLabelBizIds() {
			var labelID uint64
			labelBizIDUint64, err := util.CheckReqParamsIsUint64(ctx, labelBizID)
			if err != nil {
				return false, err
			}
			if label, ok := labels[labelBizIDUint64]; ok {
				labelID = label.ID
			}
			mapRefer[fmt.Sprintf("%d_%d_%d", v.GetSource(), attrID, labelID)] = struct{}{}
		}
	}
	for _, v := range oldRefers {
		mapOldRefer[fmt.Sprintf("%d_%d_%d", v.Source, v.AttrID, v.LabelID)] = struct{}{}
	}
	if len(mapOldRefer) != len(mapRefer) {
		return true, nil
	}
	for key := range mapOldRefer {
		if _, ok := mapRefer[key]; !ok {
			return true, nil
		}
	}
	return false, nil
}

// fillPBAttrLabels 转成成PB的属性标签
func fillPBAttrLabels(attrLabels []*labelEntity.AttrLabel) []*pb.AttrLabel {
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

func (s *Service) filterAttributeLabelChangedQA(
	ctx context.Context,
	robotID uint64, qaIDs []uint64, qas []*qaEntity.DocQA,
	attrRange uint32,
	refers []*pb.AttrLabelRefer,
	attrs map[uint64]*labelEntity.Attribute,
	labels map[uint64]*labelEntity.AttributeLabel,
) ([]*qaEntity.DocQA, error) {
	result := make([]*qaEntity.DocQA, 0, len(qas))
	mapRefer := make(map[string]struct{})
	for _, v := range refers {
		var attrID uint64
		attributeBizId, err := util.CheckReqParamsIsUint64(ctx, v.GetAttributeBizId())
		if err != nil {
			return nil, err
		}
		if attr, ok := attrs[attributeBizId]; ok {
			attrID = attr.ID
		}
		for _, labelBizID := range v.GetLabelBizIds() {
			var labelID uint64
			labelBizIDUint64, err := util.CheckReqParamsIsUint64(ctx, labelBizID)
			if err != nil {
				return nil, err
			}
			if label, ok := labels[labelBizIDUint64]; ok {
				labelID = label.ID
			}
			mapRefer[fmt.Sprintf("%d_%d_%d", v.GetSource(), attrID, labelID)] = struct{}{}
		}
	}
	oldRefers, err := s.labelDao.GetQAAttributeLabel(ctx, robotID, qaIDs)
	if err != nil {
		return result, err
	}
	mapOldRefer := make(map[uint64]map[string]struct{})
	for _, old := range oldRefers {
		v := mapOldRefer[old.QAID]
		if v == nil {
			v = make(map[string]struct{})
		}
		v[fmt.Sprintf("%d_%d_%d", old.Source, old.AttrID, old.LabelID)] = struct{}{}
		mapOldRefer[old.QAID] = v
	}
	for _, qa := range qas {
		if qa.AttrRange != attrRange {
			result = append(result, qa)
			continue
		}
		qaOldRefer := mapOldRefer[qa.ID]
		if isDifferent(qaOldRefer, mapRefer) {
			result = append(result, qa)
		}
	}
	return result, nil
}
func isDifferent(s, d map[string]struct{}) bool {
	if len(s) != len(d) {
		return true
	}
	for key := range s {
		if _, ok := d[key]; !ok {
			return true
		}
	}
	return false
}
