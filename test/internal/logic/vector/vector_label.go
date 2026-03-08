package vector

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	"github.com/spf13/cast"

	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// GetQAVectorLabels 获取QA的向量标签 问答的向量标签统一走这个函数 做全量覆盖
func (v *VectorSyncLogic) GetQAVectorLabels(ctx context.Context, appBizId uint64, qa *qaEntity.DocQA) (
	[]*retrieval.VectorLabel, error) {
	logx.I(ctx, "GetQAVectorLabels appBizId:%v,qa:%v", appBizId, qa)
	var vectorLabels []*retrieval.VectorLabel
	// feature_permission
	// 1.处理问答的标签属性
	// 来源文档只继承父文档的标签,分类和角色还按问答自己的处理
	vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
		Name:  entity.EnableScopeAttr,
		Value: entity.EnableScopeDb2Label[qa.EnableScope],
	})
	if qa.Source == docEntity.SourceFromDoc {
		doc, err := v.getDocByID(ctx, qa.OriginDocID, qa.RobotID)
		if err != nil {
			logx.W(ctx, "GetQAVectorLabels getDocByID err:%v,appBizId:%v,qaBizId:%v,qa.OriginDocID:%v",
				err, appBizId, qa.BusinessID, qa.OriginDocID)
		} else {
			tmp, err := v.getDocVectorLabels(ctx, doc)
			if err != nil {
				return nil, err
			}
			vectorLabels = append(vectorLabels, tmp...)
		}
	} else if qa.AttrRange == docEntity.AttrRangeAll {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
			Value: config.App().AttributeLabel.FullLabelValue,
		})
	} else {
		mapAttrLabels, err := v.getQAAttributeLabelDetail(ctx, qa.RobotID, []uint64{qa.ID})
		if err != nil {
			logx.E(ctx, "GetQAVectorLabels getQAAttributeLabelDetail err:%v,appBizId:%v,qaBizId:%v",
				err, appBizId, qa.BusinessID)
			return nil, err
		}
		vectorLabels = append(vectorLabels, labelEntity.FillVectorLabels(mapAttrLabels[qa.ID])...)
	}
	// 2.处理问答的分类,根据主键id
	tmp, err := v.GetQACateAndRoleLabels(ctx, appBizId, qa)
	if err != nil {
		logx.E(ctx, "GetQAVectorLabels GetQACateAndRoleLabels err:%v,appBizId:%v,qaBizId:%v",
			err, appBizId, qa.BusinessID)
		return nil, err
	}
	if len(tmp) > 0 {
		vectorLabels = append(vectorLabels, tmp...)
	}
	logx.D(ctx, "feature_permission getQAVectorLabels len(vectorLabels):%d (app_biz_id:%d, qa_id:%d)",
		len(vectorLabels), appBizId, qa.ID)
	return vectorLabels, nil
}

// GetQACateAndRoleLabels 获取问答的分类和角色标签
func (v *VectorSyncLogic) GetQACateAndRoleLabels(ctx context.Context, appBizId uint64, qa *qaEntity.DocQA) (
	vectorLabels []*retrieval.VectorLabel, err error) {
	logx.I(ctx, "GetQACateAndRoleLabels appBizId:%v,qaBizId:%v", appBizId, qa.BusinessID)
	// 1.处理问答的分类,根据主键id
	if qa.CategoryID != 0 {
		cateBusinessID, err := v.cateDao.DescribeQACateBusinessIDByID(ctx, qa.CategoryID, qa.CorpID, qa.RobotID)
		if err != nil {
			logx.E(ctx, "GetQACateAndRoleLabels getCateInfo err:%v,cate_id:%v", err, qa.CategoryID)
			return nil, err
		}
		if cateBusinessID != 0 {
			vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
				Name:  config.GetMainConfig().Permissions.CateRetrievalKey, // 分类向量统一key
				Value: cast.ToString(cateBusinessID),
			})
		}
	}

	// 2.处理问答的角色
	roleList, err := v.userDao.DescribeKnowledgeRoleQAList(ctx, 0, 0,
		&entity.KnowledgeRoleQAFilter{
			KnowledgeBizIDs: []uint64{appBizId},
			QABizIDs:        []uint64{qa.BusinessID},
			BatchSize:       10000, // 一次取1万行，因为一个问答可能被无限个角色引用，这边会有耗时问题
		})
	if err != nil {
		logx.E(ctx, "GetQACateAndRoleLabels getRoleList err:%v,app_biz_id:%v,qa_biz_id:%v",
			err, appBizId, qa.BusinessID)
		return nil, err
	}
	for _, role := range roleList {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.GetMainConfig().Permissions.RoleRetrievalKey, // 角色向量统一key
			Value: cast.ToString(role.RoleBizID),
		})
	}
	return vectorLabels, nil
}

// getDocVectorLabels 获取文档的向量标签
func (v *VectorSyncLogic) getDocVectorLabels(ctx context.Context, doc *docEntity.Doc) ([]*retrieval.VectorLabel, error) {
	logx.I(ctx, "getDocVectorLabels robotID:%d,docID:%d", doc.RobotID, doc.ID)
	var vectorLabels []*retrieval.VectorLabel
	if doc.AttrRange == docEntity.AttrRangeAll {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
			Value: config.App().AttributeLabel.FullLabelValue,
		})
		return vectorLabels, nil
	}
	mapAttrLabels, err := v.getDocAttributeLabelDetail(ctx, doc.RobotID, []uint64{doc.ID})
	if err != nil {
		return nil, err
	}
	return labelEntity.FillVectorLabels(mapAttrLabels[doc.ID]), nil
}

func (v *VectorSyncLogic) getDocAttributeLabelDetail(ctx context.Context, robotID uint64, docIDs []uint64) (
	map[uint64][]*labelEntity.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*labelEntity.AttrLabel)
	if len(docIDs) == 0 {
		return mapDocID2AttrLabels, nil
	}
	attributeLabels, err := v.getDocAttributeLabel(ctx, robotID, docIDs)
	if err != nil {
		return nil, err
	}
	if len(attributeLabels) == 0 {
		return mapDocID2AttrLabels, nil
	}
	var mapKgAttrLabels map[uint64][]*labelEntity.AttrLabel
	var kgErr error
	// 查询不同来源的属性标签信息
	g := errgroupx.New()
	g.SetLimit(10)
	// 来源，属性标签
	g.Go(func() error {
		mapKgAttrLabels, kgErr = v.getDocAttributeLabelOfKg(ctx, robotID, attributeLabels)
		return kgErr
	})
	if err := g.Wait(); err != nil {
		logx.W(ctx, "getDocAttributeLabelDetail robotID:%d,docIDs:%+v err :%v", robotID, docIDs, err)
		return nil, err
	}
	for docID, attrLabels := range mapKgAttrLabels {
		mapDocID2AttrLabels[docID] = append(mapDocID2AttrLabels[docID], attrLabels...)
	}
	return mapDocID2AttrLabels, nil
}

func (v *VectorSyncLogic) getQAAttributeLabelDetail(ctx context.Context, robotID uint64, qaIDs []uint64) (
	map[uint64][]*labelEntity.AttrLabel, error) {
	logx.I(ctx, "getQAAttributeLabelDetail robotID:%d,qaIDs:%+v", robotID, qaIDs)
	mapQAID2AttrLabels := make(map[uint64][]*labelEntity.AttrLabel)
	if len(qaIDs) == 0 {
		return mapQAID2AttrLabels, nil
	}
	attributeLabels, err := v.getQAAttributeLabel(ctx, robotID, qaIDs)
	if err != nil {
		return nil, err
	}
	if len(attributeLabels) == 0 {
		return mapQAID2AttrLabels, nil
	}
	var mapKgAttrLabels map[uint64][]*labelEntity.AttrLabel
	var kgErr error
	// 查询不同来源的属性标签信息
	g := errgroupx.New()
	g.SetLimit(10)
	// 来源，属性标签
	g.Go(func() error {
		mapKgAttrLabels, kgErr = v.getQAAttributeLabelOfKg(ctx, robotID, attributeLabels)
		return kgErr
	})
	if err := g.Wait(); err != nil {
		logx.W(ctx, "GetQAAttributeLabelDetail robotID:%d,qaIDs:%+v err :%v", robotID, qaIDs, err)
		return nil, err
	}
	for qaID, attrLabels := range mapKgAttrLabels {
		mapQAID2AttrLabels[qaID] = append(mapQAID2AttrLabels[qaID], attrLabels...)
	}
	return mapQAID2AttrLabels, nil
}

// getQAAttributeLabel 获取QA的属性标签信息
func (v *VectorSyncLogic) getQAAttributeLabel(ctx context.Context, robotID uint64, qaIDs []uint64) (
	[]*labelEntity.QAAttributeLabel, error) {
	if len(qaIDs) == 0 {
		return nil, nil
	}

	qaAttrLabels, err := v.labelDao.GetQAAttributeLabel(ctx, robotID, qaIDs)
	if err != nil {
		logx.E(ctx, "Failed to getQAAttributeLabel robotID:%d,qaIDs:%+v err :%v", robotID, qaIDs, err)
		return nil, err
	}
	return qaAttrLabels, nil
}

// getQAAttributeLabelOfKg 获取获取来源为知识标签的QA属性标签信息
func (v *VectorSyncLogic) getQAAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*labelEntity.QAAttributeLabel) (map[uint64][]*labelEntity.AttrLabel, error) {
	mapQAID2AttrLabels := make(map[uint64][]*labelEntity.AttrLabel)
	mapQAAttrID2Attr := make(map[string]*labelEntity.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getQAAttributeLabelOfSource(attributeLabels,
		labelEntity.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := v.getAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := v.getAttributeLabelByIDs(ctx, robotID, labelIDs)
	if err != nil {
		return nil, err
	}
	for _, v := range sourceAttributeLabels {
		attr, ok := mapAttrID2Info[v.AttrID]
		if !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		label, ok := mapLabelID2Info[v.LabelID]
		if v.LabelID > 0 && !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		labelName := label.GetName()
		if v.LabelID == 0 {
			labelName = config.App().AttributeLabel.FullLabelValue
		}
		labelInfo := &labelEntity.Label{
			LabelID:   v.LabelID,
			LabelName: labelName,
		}
		qaAttrID := fmt.Sprintf("%d_%d", v.QAID, v.AttrID)
		attrInfo, ok := mapQAAttrID2Attr[qaAttrID]
		if !ok {
			attrInfo = &labelEntity.AttrLabel{
				Source:   v.Source,
				AttrID:   v.AttrID,
				AttrKey:  attr.AttrKey,
				AttrName: attr.Name,
			}
			mapQAAttrID2Attr[qaAttrID] = attrInfo
			mapQAID2AttrLabels[v.QAID] = append(mapQAID2AttrLabels[v.QAID], attrInfo)
		}
		attrInfo.Labels = append(attrInfo.Labels, labelInfo)
	}
	return mapQAID2AttrLabels, nil
}

// getDocAttributeLabelOfKg 获取获取来源为知识标签的文档属性标签信息
func (v *VectorSyncLogic) getDocAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*labelEntity.DocAttributeLabel) (map[uint64][]*labelEntity.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*labelEntity.AttrLabel)
	mapDocAttrID2Attr := make(map[string]*labelEntity.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getDocAttributeLabelOfSource(attributeLabels,
		labelEntity.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := v.getAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := v.getAttributeLabelByIDs(ctx, robotID, labelIDs)
	if err != nil {
		return nil, err
	}
	for _, v := range sourceAttributeLabels {
		attr, ok := mapAttrID2Info[v.AttrID]
		if !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		label, ok := mapLabelID2Info[v.LabelID]
		if v.LabelID > 0 && !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		labelName := label.GetName()
		if v.LabelID == 0 {
			labelName = config.App().AttributeLabel.FullLabelValue
		}
		labelInfo := &labelEntity.Label{
			LabelID:   v.LabelID,
			LabelName: labelName,
		}
		docAttrID := fmt.Sprintf("%d_%d", v.DocID, v.AttrID)
		attrInfo, ok := mapDocAttrID2Attr[docAttrID]
		if !ok {
			attrInfo = &labelEntity.AttrLabel{
				Source:   v.Source,
				AttrID:   v.AttrID,
				AttrKey:  attr.AttrKey,
				AttrName: attr.Name,
			}
			mapDocAttrID2Attr[docAttrID] = attrInfo
			mapDocID2AttrLabels[v.DocID] = append(mapDocID2AttrLabels[v.DocID], attrInfo)
		}
		attrInfo.Labels = append(attrInfo.Labels, labelInfo)
	}
	return mapDocID2AttrLabels, nil
}

func (v *VectorSyncLogic) getAttributeByIDs(ctx context.Context, robotID uint64, ids []uint64) (
	map[uint64]*labelEntity.Attribute, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	attrs, err := v.labelDao.GetAttributeByIDs(ctx, robotID, ids)
	if err != nil {
		logx.E(ctx, "Failed to get attribute by ids err:%+v", err)
		return nil, err
	}
	return attrs, nil
}

func (v *VectorSyncLogic) getAttributeLabelByIDs(ctx context.Context, robotID uint64, ids []uint64) (
	map[uint64]*labelEntity.AttributeLabel, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	labels, err := v.labelDao.GetAttributeLabelByIDs(ctx, ids, robotID)
	if err != nil {
		logx.E(ctx, "Failed to get attribute labelEntity by ids err:%+v", err)
		return nil, err
	}
	return labels, nil
}

// getDocAttributeLabelOfSource
func getDocAttributeLabelOfSource(attributeLabels []*labelEntity.DocAttributeLabel, source uint32) (
	[]*labelEntity.DocAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*labelEntity.DocAttributeLabel
	var attrIDs, labelIDs []uint64
	mapAttrID := make(map[uint64]struct{}, 0)
	mapLabelID := make(map[uint64]struct{}, 0)
	for _, v := range attributeLabels {
		if v.Source != source {
			continue
		}
		sourceAttributeLabels = append(sourceAttributeLabels, v)
		if _, ok := mapAttrID[v.AttrID]; !ok {
			mapAttrID[v.AttrID] = struct{}{}
			attrIDs = append(attrIDs, v.AttrID)
		}
		if v.LabelID == 0 {
			continue
		}
		if _, ok := mapLabelID[v.LabelID]; !ok {
			mapLabelID[v.LabelID] = struct{}{}
			labelIDs = append(labelIDs, v.LabelID)
		}
	}
	return sourceAttributeLabels, attrIDs, labelIDs
}

// getQAAttributeLabelOfSource TODO
func getQAAttributeLabelOfSource(attributeLabels []*labelEntity.QAAttributeLabel, source uint32) (
	[]*labelEntity.QAAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*labelEntity.QAAttributeLabel
	var attrIDs, labelIDs []uint64
	mapAttrID := make(map[uint64]struct{}, 0)
	mapLabelID := make(map[uint64]struct{}, 0)
	for _, v := range attributeLabels {
		if v.Source != source {
			continue
		}
		sourceAttributeLabels = append(sourceAttributeLabels, v)
		if _, ok := mapAttrID[v.AttrID]; !ok {
			mapAttrID[v.AttrID] = struct{}{}
			attrIDs = append(attrIDs, v.AttrID)
		}
		if v.LabelID == 0 {
			continue
		}
		if _, ok := mapLabelID[v.LabelID]; !ok {
			mapLabelID[v.LabelID] = struct{}{}
			labelIDs = append(labelIDs, v.LabelID)
		}
	}
	return sourceAttributeLabels, attrIDs, labelIDs
}

func (v *VectorSyncLogic) getDocAttributeLabel(ctx context.Context, robotID uint64, docIDs []uint64) (
	[]*labelEntity.DocAttributeLabel, error) {
	if len(docIDs) == 0 {
		return nil, nil
	}

	docLables, err := v.labelDao.GetDocAttributeLabel(ctx, robotID, docIDs)
	if err != nil {
		logx.E(ctx, "Failed to GetDocAttributeLabel err:%+v", err)
		return nil, err
	}
	return docLables, nil
}

func (v *VectorSyncLogic) getDocByID(ctx context.Context, id uint64, robotID uint64) (*docEntity.Doc, error) {
	docs, err := v.getDocByIDs(ctx, []uint64{id}, robotID)
	if err != nil {
		return nil, err
	}
	doc, ok := docs[id]
	if !ok {
		return nil, errs.ErrDocNotFound
	}
	return doc, nil
}

func (v *VectorSyncLogic) getDocByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*docEntity.Doc, error) {
	docs := make(map[uint64]*docEntity.Doc, 0)
	if len(ids) == 0 {
		return docs, nil
	}

	docs, err := v.docDao.GetDocByIDs(ctx, ids, robotID)
	if err != nil {
		logx.E(ctx, " Failed to getDocByIDs err:%+v", err)
		return nil, err
	}

	return docs, nil
}
