package label

import (
	"context"
	"strings"
	"unicode/utf8"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"golang.org/x/exp/maps"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// parseSimilarLabels 解析相似标签值
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

// checkNeedPublishLabelDocAndQaStatus 检查待修改的属性标签关联的文档和QA的状态
func (l *Logic) checkNeedPublishLabelDocAndQaStatus(ctx context.Context, robotID uint64,
	publishParams *labelEntity.AttributeLabelUpdateParams) error {
	if len(publishParams.LabelIDs) == 0 {
		return nil
	}
	g := errgroupx.New()
	g.SetLimit(10)
	// 检查文档
	g.Go(func() error {
		total, err := l.dao.GetDocCountByAttributeLabel(ctx, robotID, docEntity.DocStableStatus, publishParams.AttrID,
			publishParams.LabelIDs)
		if err != nil {
			return err
		}
		if total > 0 {
			return errs.ErrAttributeLabelDocQaSync
		}
		return nil
	})
	// 检查QA
	g.Go(func() error {
		total, err := l.dao.GetQACountByAttributeLabel(ctx, robotID, qaEntity.QAStableStatus, publishParams.AttrID,
			publishParams.LabelIDs)
		if err != nil {
			return err
		}
		if total > 0 {
			return errs.ErrAttributeLabelDocQaSync
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		logx.W(ctx, "checkAttributeLabelDocAndQaStatus robotID:%d,publishParams:%+v err :%v",
			robotID, publishParams, err)
		return err
	}
	return nil
}

// checkModifyAttribute 检查修改属性的参数是否合法
func (l *Logic) checkModifyAttribute(ctx context.Context, robotID uint64, req *pb.ModifyAttributeLabelReq) (
	*labelEntity.Attribute, error) {
	var err error
	// 检查属性是否存在和状态
	attributeBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetAttributeBizId())
	if err != nil {
		return nil, err
	}
	mapAttrID2Info, err := l.dao.GetAttributeByBizIDs(ctx, robotID, []uint64{attributeBizID})
	if err != nil {
		return nil, err
	}
	attr, ok := mapAttrID2Info[attributeBizID]
	if !ok {
		return nil, errs.ErrAttributeLabelNotFound
	}
	if attr.IsUpdating || attr.ReleaseStatus == labelEntity.AttributeStatusReleasing ||
		attr.ReleaseStatus == labelEntity.AttributeStatusReleaseUpdating {
		return nil, errs.ErrAttributeLabelUpdating
	}

	if req.GetAttrName() != attr.Name {
		// 如果修改属性名称
		// 检查属性名称是否合法
		if err = l.checkAttributeNameInvalid(ctx, robotID, req.GetAttrName()); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

// checkModifyAttributeUpdateLabels 检查修改属性更新标签的参数是否合法
func (l *Logic) checkModifyAttributeUpdateLabels(ctx context.Context, reqLabels []*pb.AttributeLabel, robotID uint64,
	attrID uint64, labelBizID2Info map[uint64]*labelEntity.AttributeLabel) ([]uint64, error) {
	updateNameLabelIDs := make([]uint64, 0)
	updateLabelBizIDs := make([]uint64, 0)
	updateLabels := make([]*pb.AttributeLabel, 0)
	for _, label := range reqLabels {
		if label == nil {
			continue
		}
		if label.GetLabelBizId() == "" {
			// id为空表示新增标签
			continue
		}
		labelBizID, err := util.CheckReqParamsIsUint64(ctx, label.GetLabelBizId())
		if err != nil {
			return nil, err
		}
		updateLabelBizIDs = append(updateLabelBizIDs, labelBizID)
		updateLabels = append(updateLabels, label)
	}
	updateLabelBizIDs = slicex.Unique(updateLabelBizIDs)
	if len(updateLabelBizIDs) == 0 {
		return []uint64{}, nil
	}
	filter := &labelEntity.AttributeLabelFilter{
		RobotId:     robotID,
		BusinessIds: updateLabelBizIDs,
	}
	selectColumns := []string{labelEntity.AttributeLabelTblColId, labelEntity.AttributeLabelTblColBusinessId,
		labelEntity.AttributeLabelTblColAttrId, labelEntity.AttributeLabelTblColName, labelEntity.AttributeLabelTblColSimilarLabel}
	labelInfos, err := l.GetAttributeLabelList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	if len(labelInfos) != len(updateLabelBizIDs) {
		logx.W(ctx, "GetAttributeLabelList Failed! FaileInfo:%+v", err)
		return nil, errs.ErrAttributeLabelNotFound
	}
	for _, labelInfo := range labelInfos {
		labelBizID2Info[labelInfo.BusinessID] = labelInfo
	}

	for _, label := range updateLabels {
		labelBizID, err := util.CheckReqParamsIsUint64(ctx, label.GetLabelBizId())
		if err != nil {
			return nil, err
		}
		labelInfo, ok := labelBizID2Info[labelBizID]
		if !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		if labelInfo.Name == label.GetLabelName() {
			// 标签标准词未修改，则不需要重新学习
			continue
		}
		updateNameLabelIDs = append(updateNameLabelIDs, labelInfo.ID)
	}
	updateNameLabelIDs = slicex.Unique(updateNameLabelIDs)
	// 检查修改的标签值是否合法
	err = l.checkAttributeLabelInvalid(ctx, updateLabels, labelBizID2Info)
	if err != nil {
		return nil, err
	}
	// 检查修改的标签值是否在使用中，只有使用中的标签值才需要重新学习
	usedAttrLabelIDs, err := l.getAttributeLabelUsed(ctx, robotID, labelEntity.AttributeLabelSourceKg, []uint64{attrID},
		updateNameLabelIDs)
	if err != nil {
		return nil, err
	}
	needPublishLabelIDs := make([]uint64, 0)
	for _, usedLabelIDs := range usedAttrLabelIDs {
		needPublishLabelIDs = append(needPublishLabelIDs, maps.Keys(usedLabelIDs)...)
	}
	return needPublishLabelIDs, nil
}

// checkModifyAttributeAddLabels 检查修改属性新增标签的参数是否合法
func (l *Logic) checkModifyAttributeAddLabels(ctx context.Context, reqLabels []*pb.AttributeLabel,
	labelBizID2Info map[uint64]*labelEntity.AttributeLabel) (uint64, error) {
	addLabels := make([]*pb.AttributeLabel, 0)
	addLabelCounts := uint64(0)
	for _, label := range reqLabels {
		if label == nil {
			continue
		}
		if label.GetLabelBizId() != "" {
			// id非空表示修改标签
			continue
		}
		addLabelCounts++
		addLabels = append(addLabels, label)
	}
	// 检查新增的标签值是否合法
	err := l.checkAttributeLabelInvalid(ctx, addLabels, labelBizID2Info)
	if err != nil {
		return 0, err
	}
	return addLabelCounts, nil
}

// checkAttributeLabelInvalid 检查属性标签是否合法
func (l *Logic) checkAttributeLabelInvalid(ctx context.Context, labels []*pb.AttributeLabel,
	labelBizID2Info map[uint64]*labelEntity.AttributeLabel) error {
	if len(labels) == 0 {
		return nil
	}
	existLabelNames := make(map[string]struct{})
	for _, label := range labelBizID2Info {
		existLabelNames[label.Name] = struct{}{}
	}
	// 检查标签标准词
	for _, label := range labels {
		labelName := label.GetLabelName()
		if label.GetLabelBizId() != "" {
			// 如果是修改标签，先检查是否有修改过标签标准词
			labelBizID, err := util.CheckReqParamsIsUint64(ctx, label.GetLabelBizId())
			if err != nil {
				return err
			}
			if labelBizID2Info[labelBizID].Name == labelName {
				// 如果标签标准词没变，不用校验
				continue
			}
		}
		if strings.TrimSpace(labelName) == "" {
			// 可以包含空格，但是不能只包含空格
			return errs.ErrAttributeLabelEmpty
		}
		if utf8.RuneCountInString(labelName) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
			config.App().AttributeLabel.LabelNameMaxLen) {
			// 检查标签名称字符长度限制，不能先去掉空格再比较
			return errs.ErrAttributeLabelNameMaxLen
		}
		if labelName == config.App().AttributeLabel.FullLabelValue {
			// 不能与系统标签同名
			return errs.ErrAttributeLabelSystem
		}
		if _, ok := existLabelNames[labelName]; ok {
			return errs.ErrAttributeLabelNameRepeated
		}
		existLabelNames[labelName] = struct{}{}
	}

	// 检查标签相似词
	for _, label := range labels {
		if label.GetLabelBizId() != "" {
			// 如果是修改标签，先检查是否有修改过标签相似词
			similarLabel, err := parseSimilarLabels(label.GetSimilarLabels())
			if err != nil {
				logx.D(ctx, "parse similar labels err:%v", err)
				return err
			}
			labelBizID, err := util.CheckReqParamsIsUint64(ctx, label.GetLabelBizId())
			if err != nil {
				return err
			}
			if labelBizID2Info[labelBizID].SimilarLabel == similarLabel {
				// 如果标签相似词没变，不用校验
				continue
			}
		}
		if len(label.GetSimilarLabels()) == 0 {
			continue
		}
		if len(label.GetSimilarLabels()) > config.App().AttributeLabel.SimilarLabelLimit {
			return errs.ErrAttributeLabelSimilarLimit
		}
		for _, similarLabel := range label.GetSimilarLabels() {
			if utf8.RuneCountInString(strings.TrimSpace(similarLabel)) == 0 {
				return errs.ErrAttributeLabelEmpty
			}
			if utf8.RuneCountInString(similarLabel) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
				config.App().AttributeLabel.SimilarLabelMaxLen) {
				return errs.ErrAttributeLabelSimilarMaxLen
			}
			if _, ok := existLabelNames[similarLabel]; ok {
				return errs.ErrAttributeLabelNameRepeated
			}
			existLabelNames[similarLabel] = struct{}{}
		}
	}

	return nil
}

// checkAttributeNameInvalid 检查属性名称是否合法
func (l *Logic) checkAttributeNameInvalid(ctx context.Context, robotID uint64, attrName string) error {
	// 检查属性名称字符长度限制
	if utf8.RuneCountInString(strings.TrimSpace(attrName)) == 0 {
		return errs.ErrAttributeLabelEmpty
	}
	if utf8.RuneCountInString(attrName) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		config.App().AttributeLabel.AttrNameMaxLen) {
		return errs.ErrAttributeLabelAttrNameMaxLen
	}
	// 检查属性名称是否已经存在
	mapAttrName2Info, err := l.dao.GetAttributeByNames(ctx, robotID, []string{attrName})
	if err != nil {
		return err
	}
	if _, ok := mapAttrName2Info[attrName]; ok {
		return errs.ErrAttributeLabelAttrNameExist
	}
	return nil
}

// getAttributeLabelUsed 查询使用中的属性标签
func (l *Logic) getAttributeLabelUsed(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64) (map[uint64]map[uint64]struct{}, error) {
	usedAttrLabelIDs := make(map[uint64]map[uint64]struct{})
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return usedAttrLabelIDs, nil
	}
	var docAttributeLabelErr error
	var qaAttributeLabelErr error
	docAttributeLabels := make([]*labelEntity.DocAttributeLabel, 0)
	qaAttributeLabels := make([]*labelEntity.QAAttributeLabel, 0)
	g := errgroupx.New()
	g.SetLimit(10)
	g.Go(func() error {
		docAttributeLabels, docAttributeLabelErr = l.dao.GetDocAttributeLabelByAttrLabelIDs(ctx,
			robotID, source, attrIDs, labelIDs, 1, 1000)
		if docAttributeLabelErr != nil {
			return docAttributeLabelErr
		}
		return nil
	})
	g.Go(func() error {
		qaAttributeLabels, qaAttributeLabelErr = l.dao.GetQAAttributeLabelByAttrLabelIDs(ctx,
			robotID, source, attrIDs, labelIDs, 1, 1000)
		if qaAttributeLabelErr != nil {
			return qaAttributeLabelErr
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		logx.W(ctx, "checkAttributeLabelUsed robotID:%d,source:%d,attrIDs:%+v,labelIDs:%+v err :%v",
			robotID, source, attrIDs, labelIDs, err)
		return usedAttrLabelIDs, err
	}
	for _, label := range docAttributeLabels {
		usedLabelIDs, ok := usedAttrLabelIDs[label.AttrID]
		if !ok {
			usedLabelIDs = make(map[uint64]struct{})
		}
		usedLabelIDs[label.LabelID] = struct{}{}
		usedAttrLabelIDs[label.AttrID] = usedLabelIDs
	}
	for _, label := range qaAttributeLabels {
		usedLabelIDs, ok := usedAttrLabelIDs[label.AttrID]
		if !ok {
			usedLabelIDs = make(map[uint64]struct{})
		}
		usedLabelIDs[label.LabelID] = struct{}{}
		usedAttrLabelIDs[label.AttrID] = usedLabelIDs
	}
	return usedAttrLabelIDs, nil
}

// checkModifyAttributeDeleteLabels 检查修改属性删除标签的参数是否合法
func (l *Logic) checkModifyAttributeDeleteLabels(ctx context.Context, deleteLabelBizIDStrList []string, robotID uint64,
	attrID uint64, labelBizID2Info map[uint64]*labelEntity.AttributeLabel) ([]uint64, []uint64, error) {
	deleteLabelIDs := make([]uint64, 0)
	deleteLabelBizIDs := make([]uint64, 0, len(deleteLabelBizIDStrList))
	if len(deleteLabelBizIDStrList) == 0 {
		return deleteLabelBizIDs, deleteLabelIDs, nil
	}
	for _, deleteLabelID := range deleteLabelBizIDStrList {
		bizID, err := util.CheckReqParamsIsUint64(ctx, deleteLabelID)
		if err != nil {
			return deleteLabelBizIDs, deleteLabelIDs, err
		}
		deleteLabelBizIDs = append(deleteLabelBizIDs, bizID)
	}
	deleteLabelBizIDs = slicex.Unique(deleteLabelBizIDs)
	filter := &labelEntity.AttributeLabelFilter{
		RobotId:     robotID,
		BusinessIds: deleteLabelBizIDs,
	}
	selectColumns := []string{labelEntity.AttributeLabelTblColId, labelEntity.AttributeLabelTblColBusinessId,
		labelEntity.AttributeLabelTblColAttrId, labelEntity.AttributeLabelTblColName, labelEntity.AttributeLabelTblColSimilarLabel}
	labelInfos, err := l.GetAttributeLabelList(ctx, selectColumns, filter)
	if err != nil {
		return deleteLabelBizIDs, deleteLabelIDs, err
	}
	if len(labelInfos) != len(deleteLabelBizIDStrList) {
		return deleteLabelBizIDs, deleteLabelIDs, errs.ErrAttributeLabelNotFound
	}
	for _, labelInfo := range labelInfos {
		labelBizID2Info[labelInfo.BusinessID] = labelInfo
		deleteLabelIDs = append(deleteLabelIDs, labelInfo.ID)
	}
	// 检查删除的标签是否在使用中
	usedAttrLabelIDs, err := l.getAttributeLabelUsed(ctx, robotID, labelEntity.AttributeLabelSourceKg, []uint64{attrID}, deleteLabelIDs)
	if err != nil {
		return nil, nil, err
	}
	if len(usedAttrLabelIDs) > 0 {
		// 删除的标签在使用中，不允许删除
		return nil, nil, errs.ErrAttributeLabelAttrHasUsed
	}
	return deleteLabelBizIDs, deleteLabelIDs, nil
}

// checkAttrNameValid 检查属性名称是否合法（不检查是否已存在）
func (l *Logic) checkAttrNameValid(ctx context.Context, attrName string) error {
	if utf8.RuneCountInString(strings.TrimSpace(attrName)) == 0 {
		return errs.ErrAttributeLabelEmpty
	}
	if utf8.RuneCountInString(attrName) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		config.App().AttributeLabel.AttrNameMaxLen) {
		return errs.ErrAttributeLabelAttrNameMaxLen
	}
	return nil
}

// checkLabelsValid 检查标签列表是否合法
// isWhiteList: 是否为白名单用户，白名单用户跳过标签数量限制
func (l *Logic) checkLabelsValid(ctx context.Context, labels []*CreateAttributeLabelItem, isWhiteList bool) error {
	if len(labels) == 0 {
		return nil
	}

	// 检查标签数量限制（白名单用户跳过）
	if !isWhiteList && len(labels) > config.App().AttributeLabel.LabelLimit {
		return errs.ErrAttributeLabelLimit
	}

	existLabelNames := make(map[string]struct{})
	for _, label := range labels {
		labelName := label.LabelName
		// 检查标签名称
		if strings.TrimSpace(labelName) == "" {
			return errs.ErrAttributeLabelEmpty
		}
		if utf8.RuneCountInString(labelName) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
			config.App().AttributeLabel.LabelNameMaxLen) {
			return errs.ErrAttributeLabelNameMaxLen
		}
		if labelName == config.App().AttributeLabel.FullLabelValue {
			return errs.ErrAttributeLabelSystem
		}
		if _, ok := existLabelNames[labelName]; ok {
			return errs.ErrAttributeLabelNameRepeated
		}
		existLabelNames[labelName] = struct{}{}

		// 检查相似标签
		if len(label.SimilarLabels) > config.App().AttributeLabel.SimilarLabelLimit {
			return errs.ErrAttributeLabelSimilarLimit
		}
		for _, similarLabel := range label.SimilarLabels {
			if strings.TrimSpace(similarLabel) == "" {
				return errs.ErrAttributeLabelEmpty
			}
			if utf8.RuneCountInString(similarLabel) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
				config.App().AttributeLabel.SimilarLabelMaxLen) {
				return errs.ErrAttributeLabelSimilarMaxLen
			}
			if _, ok := existLabelNames[similarLabel]; ok {
				return errs.ErrAttributeLabelNameRepeated
			}
			existLabelNames[similarLabel] = struct{}{}
		}
	}

	return nil
}

// CheckAttributeKeyNameExist 检查属性标识和名称是否已存在
func (l *Logic) CheckAttributeKeyNameExist(ctx context.Context, robotID uint64, attrKey, attrName string,
	excludeAttrID uint64) error {
	// 检查属性标识是否存在
	if attrKey != "" {
		mapAttrKeyInfo, err := l.dao.GetAttributeByKeys(ctx, robotID, []string{attrKey})
		if err != nil {
			return err
		}
		if attr, ok := mapAttrKeyInfo[attrKey]; ok && attr.ID != excludeAttrID {
			return errs.ErrAttributeLabelAttrKeyExist
		}
	}
	// 检查属性名称是否存在
	mapAttrName2Info, err := l.dao.GetAttributeByNames(ctx, robotID, []string{attrName})
	if err != nil {
		return err
	}
	if attr, ok := mapAttrName2Info[attrName]; ok && attr.ID != excludeAttrID {
		return errs.ErrAttributeLabelAttrNameExist
	}
	return nil
}
