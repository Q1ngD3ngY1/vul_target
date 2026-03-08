package vector

import (
	"context"
	"fmt"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/client"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

var (
	// 系统标签(for相似问场景)
	sysLabelQAFlagName         = "_sys_str_qa_flag"
	sysLabelQAFlagValueSimilar = "similar"        // 标识相似问
	sysLabelQAIdName           = "_sys_str_qa_id" // 标识相似问的qaId

	docFields = `
        id,robot_id,attr_range,is_deleted,status
    `
	attributeFields = `
        id,robot_id,attr_key,name,is_deleted,deleted_time,create_time,update_time
    `
	attributeLabelFields = `
        id,attr_id,name,similar_label,is_deleted,create_time,update_time
    `
	docAttributeLabelFields = `
        id,robot_id,doc_id,source,attr_id,label_id,is_deleted,create_time,update_time
    `
	qaAttributeLabelFields = `
        id,robot_id,qa_id,source,attr_id,label_id,is_deleted,create_time,update_time
    `
	getDocByIDs = `
        SELECT 
            %s  
        FROM 
            t_doc 
        WHERE 
            id IN (%s)
    `
	getAttributeByIDs = `
        SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ? AND id IN (%s)
    `
	getAttributeLabelByIDs = `
        SELECT 
            %s
        FROM
            t_attribute_label
        WHERE
            is_deleted = ? AND id IN (%s)
    `
	getDocAttributeLabel = `
	    SELECT
            %s
        FROM
            t_doc_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND doc_id IN (%s)
	`
	getQAAttributeLabel = `
        SELECT
            %s
        FROM
            t_qa_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND qa_id IN (%s)
    `
	getQaBizId = `
		select 
			business_id 
		from 
			t_doc_qa_category 
		where 
			id = ? and robot_id = ? limit 1
	`
)

const (
	docTableName               = "t_doc"
	attributeTableName         = "t_attribute"
	attributeLabelTableName    = "t_attribute_label"
	docAttributeLabelTableName = "t_doc_attribute_label"
	qaAttributeLabelTableName  = "t_qa_attribute_label"
)

// getDocVectorLabels 获取文档的向量标签
func (v *SyncVector) getDocVectorLabels(ctx context.Context, doc *model.Doc) ([]*retrieval.VectorLabel, error) {
	var vectorLabels []*retrieval.VectorLabel
	if doc.AttrRange == model.AttrRangeAll {
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
	return model.FillVectorLabels(mapAttrLabels[doc.ID]), nil
}

// GetQAVectorLabels 获取QA的向量标签 问答的向量标签统一走这个函数 做全量覆盖
func (v *SyncVector) GetQAVectorLabels(ctx context.Context, appBizId uint64, qa *model.DocQA) (
	[]*retrieval.VectorLabel, error) {
	var vectorLabels []*retrieval.VectorLabel
	// feature_permission
	//1.处理问答的标签属性
	//来源文档只继承父文档的标签,分类和角色还按问答自己的处理
	if qa.Source == model.SourceFromDoc {
		doc, err := v.getDocByID(ctx, qa.OriginDocID, qa.RobotID)
		if err != nil {
			return nil, err
		}
		tmp, err := v.getDocVectorLabels(ctx, doc)
		if err != nil {
			return nil, err
		}
		vectorLabels = append(vectorLabels, tmp...)
	} else if qa.AttrRange == model.AttrRangeAll {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
			Value: config.App().AttributeLabel.FullLabelValue,
		})
	} else {
		mapAttrLabels, err := v.getQAAttributeLabelDetail(ctx, qa.RobotID, []uint64{qa.ID})
		if err != nil {
			return nil, err
		}
		vectorLabels = append(vectorLabels, model.FillVectorLabels(mapAttrLabels[qa.ID])...)
	}
	//2.处理问答的分类,根据主键id
	tmp, err := v.GetQACateAndRoleLabels(ctx, appBizId, qa)
	if err != nil {
		log.ErrorContextf(ctx, "GetQAVectorLabels GetQACateAndRoleLabels err:%v,appBizId:%v,qaBizId:%v",
			err, appBizId, qa.BusinessID)
		return nil, err
	}
	if len(tmp) > 0 {
		vectorLabels = append(vectorLabels, tmp...)
	}
	log.DebugContextf(ctx, "feature_permission getQAVectorLabels len(vectorLabels):%v", len(vectorLabels))
	return vectorLabels, nil
}

// GetQACateAndRoleLabels 获取问答的分类和角色标签
func (v *SyncVector) GetQACateAndRoleLabels(ctx context.Context, appBizId uint64, qa *model.DocQA) (
	vectorLabels []*retrieval.VectorLabel, err error) {
	//1.处理问答的分类,根据主键id
	cate := &model.CateInfo{}
	if qa.CategoryID != 0 {
		err := v.db.QueryToStruct(ctx, cate, getQaBizId, qa.CategoryID, qa.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "GetQACateAndRoleLabels getCateInfo err:%v,cate_id:%v", err, qa.CategoryID)
			return nil, err
		}
	}
	vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
		Name:  utilConfig.GetMainConfig().Permissions.CateRetrievalKey, //分类向量统一key
		Value: cast.ToString(cate.BusinessID),
	})
	//2.处理问答的角色
	maxId, selectRow := 0, 10000 //一次取1万行，因为一个问答可能被无限个角色引用，这边会有耗时问题
	for selectRow == 10000 {
		var roleList []*model.KnowledgeRoleQA
		err := v.tdsql.WithContext(ctx).Model(&model.KnowledgeRoleQA{}).Where("is_deleted = 0").
			Where("knowledge_biz_id = ?", appBizId). //兼容共享知识库处理
			Where("qa_biz_id = ?", qa.BusinessID).
			Where("id > ?", maxId). //避免深分页问题
			Select([]string{"id,role_biz_id"}).Limit(10000).Order("id asc").Find(&roleList).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetQACateAndRoleLabels getRoleList err:%v,app_biz_id:%v,qa_biz_id:%v",
				err, appBizId, qa.BusinessID)
			return nil, err
		}
		for _, role := range roleList {
			vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
				Name:  utilConfig.GetMainConfig().Permissions.RoleRetrievalKey, //角色向量统一key
				Value: cast.ToString(role.RoleBizID),
			})
		}
		selectRow = len(roleList)
		if selectRow != 0 {
			maxId = int(roleList[selectRow-1].ID)
		}
	}
	return vectorLabels, nil
}

func (v *SyncVector) getDocByID(ctx context.Context, id uint64, robotID uint64) (*model.Doc, error) {
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

func (v *SyncVector) getDocByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.Doc, error) {
	docs := make(map[uint64]*model.Doc, 0)
	if len(ids) == 0 {
		return docs, nil
	}
	querySQL := fmt.Sprintf(getDocByIDs, docFields, placeholder(len(ids)))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	list := make([]*model.Doc, 0)
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, doc := range list {
		docs[doc.ID] = doc
	}
	return docs, nil
}

func (v *SyncVector) getAttributeByIDs(ctx context.Context, robotID uint64, ids []uint64) (
	map[uint64]*model.Attribute, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var args []any
	var attrs []*model.Attribute
	args = append(args, robotID, model.AttributeIsNotDeleted)
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getAttributeByIDs, attributeFields, placeholder(len(ids)))
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute by ids sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapAttrID2Info := make(map[uint64]*model.Attribute)
	for _, v := range attrs {
		mapAttrID2Info[v.ID] = v
	}
	return mapAttrID2Info, nil
}

func (v *SyncVector) getAttributeLabelByIDs(ctx context.Context, ids []uint64, robotID uint64) (
	map[uint64]*model.AttributeLabel, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var args []any
	args = append(args, model.AttributeLabelIsNotDeleted)
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getAttributeLabelByIDs, attributeLabelFields, placeholder(len(ids)))
	var labels []*model.AttributeLabel
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &labels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过标签ID查询标签失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapLabelID2Info := make(map[uint64]*model.AttributeLabel)
	for _, v := range labels {
		mapLabelID2Info[v.ID] = v
	}
	return mapLabelID2Info, nil
}

func (v *SyncVector) getDocAttributeLabelDetail(ctx context.Context, robotID uint64, docIDs []uint64) (
	map[uint64][]*model.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*model.AttrLabel)
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
	var mapKgAttrLabels map[uint64][]*model.AttrLabel
	var kgErr error
	// 查询不同来源的属性标签信息
	g := errgroupx.Group{}
	g.SetLimit(10)
	// 来源，属性标签
	g.Go(func() error {
		mapKgAttrLabels, kgErr = v.getDocAttributeLabelOfKg(ctx, robotID, attributeLabels)
		return kgErr
	})
	if err := g.Wait(); err != nil {
		log.WarnContextf(ctx, "getDocAttributeLabelDetail robotID:%d,docIDs:%+v err :%v", robotID, docIDs, err)
		return nil, err
	}
	for docID, attrLabels := range mapKgAttrLabels {
		mapDocID2AttrLabels[docID] = append(mapDocID2AttrLabels[docID], attrLabels...)
	}
	return mapDocID2AttrLabels, nil
}

func (v *SyncVector) getDocAttributeLabel(ctx context.Context, robotID uint64, docIDs []uint64) (
	[]*model.DocAttributeLabel, error) {
	if len(docIDs) == 0 {
		return nil, nil
	}
	var args []any
	var attributeLabels []*model.DocAttributeLabel
	args = append(args, robotID, model.DocAttributeLabelIsNotDeleted)
	for _, docID := range docIDs {
		args = append(args, docID)
	}
	querySQL := fmt.Sprintf(getDocAttributeLabel, docAttributeLabelFields, placeholder(len(docIDs)))
	db := knowClient.DBClient(ctx, docAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attributeLabels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询文档属性标签失败 sql:%s,args:%+v, err:%+v", querySQL, args, err)
		return nil, err
	}
	return attributeLabels, nil
}

// getDocAttributeLabelOfKg 获取获取来源为知识标签的文档属性标签信息
func (v *SyncVector) getDocAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*model.DocAttributeLabel) (map[uint64][]*model.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*model.AttrLabel)
	mapDocAttrID2Attr := make(map[string]*model.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getDocAttributeLabelOfSource(attributeLabels,
		model.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := v.getAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := v.getAttributeLabelByIDs(ctx, labelIDs, robotID)
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
		labelInfo := &model.Label{
			LabelID:   v.LabelID,
			LabelName: labelName,
		}
		docAttrID := fmt.Sprintf("%d_%d", v.DocID, v.AttrID)
		attrInfo, ok := mapDocAttrID2Attr[docAttrID]
		if !ok {
			attrInfo = &model.AttrLabel{
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

// getDocAttributeLabelOfSource TODO
func getDocAttributeLabelOfSource(attributeLabels []*model.DocAttributeLabel, source uint32) (
	[]*model.DocAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*model.DocAttributeLabel
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

func (v *SyncVector) getQAAttributeLabelDetail(ctx context.Context, robotID uint64, qaIDs []uint64) (
	map[uint64][]*model.AttrLabel, error) {
	mapQAID2AttrLabels := make(map[uint64][]*model.AttrLabel)
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
	var mapKgAttrLabels map[uint64][]*model.AttrLabel
	var kgErr error
	// 查询不同来源的属性标签信息
	g := errgroupx.Group{}
	g.SetLimit(10)
	// 来源，属性标签
	g.Go(func() error {
		mapKgAttrLabels, kgErr = v.getQAAttributeLabelOfKg(ctx, robotID, attributeLabels)
		return kgErr
	})
	if err := g.Wait(); err != nil {
		log.WarnContextf(ctx, "GetQAAttributeLabelDetail robotID:%d,qaIDs:%+v err :%v", robotID, qaIDs, err)
		return nil, err
	}
	for qaID, attrLabels := range mapKgAttrLabels {
		mapQAID2AttrLabels[qaID] = append(mapQAID2AttrLabels[qaID], attrLabels...)
	}
	return mapQAID2AttrLabels, nil
}

// getQAAttributeLabel 获取QA的属性标签信息
func (v *SyncVector) getQAAttributeLabel(ctx context.Context, robotID uint64, qaIDs []uint64) (
	[]*model.QAAttributeLabel, error) {
	if len(qaIDs) == 0 {
		return nil, nil
	}
	var args []any
	var attributeLabels []*model.QAAttributeLabel
	args = append(args, robotID, model.QAAttributeLabelIsNotDeleted)
	for _, qaID := range qaIDs {
		args = append(args, qaID)
	}
	querySQL := fmt.Sprintf(getQAAttributeLabel, qaAttributeLabelFields, placeholder(len(qaIDs)))
	db := knowClient.DBClient(ctx, qaAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attributeLabels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询QA属性标签失败 sql:%s,args:%+v, err:%+v", querySQL, args, err)
		return nil, err
	}
	return attributeLabels, nil
}

// getQAAttributeLabelOfKg 获取获取来源为知识标签的QA属性标签信息
func (v *SyncVector) getQAAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*model.QAAttributeLabel) (map[uint64][]*model.AttrLabel, error) {
	mapQAID2AttrLabels := make(map[uint64][]*model.AttrLabel)
	mapQAAttrID2Attr := make(map[string]*model.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getQAAttributeLabelOfSource(attributeLabels,
		model.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := v.getAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := v.getAttributeLabelByIDs(ctx, labelIDs, robotID)
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
		labelInfo := &model.Label{
			LabelID:   v.LabelID,
			LabelName: labelName,
		}
		qaAttrID := fmt.Sprintf("%d_%d", v.QAID, v.AttrID)
		attrInfo, ok := mapQAAttrID2Attr[qaAttrID]
		if !ok {
			attrInfo = &model.AttrLabel{
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

// addSimilarQuestionVectorLabel 添加相似问标签
func (v *SyncVector) addSimilarQuestionVectorLabel(ctx context.Context, labels []*retrieval.VectorLabel,
	qaId uint64) []*retrieval.VectorLabel {
	var newLabels []*retrieval.VectorLabel
	newLabels = append(newLabels, &retrieval.VectorLabel{Name: sysLabelQAFlagName, Value: sysLabelQAFlagValueSimilar})
	newLabels = append(newLabels, &retrieval.VectorLabel{Name: sysLabelQAIdName, Value: fmt.Sprintf("%d", qaId)})

	if len(labels) == 0 {
		return newLabels
	}
	labels = append(labels, newLabels...)
	return labels
}

// getQAAttributeLabelOfSource TODO
func getQAAttributeLabelOfSource(attributeLabels []*model.QAAttributeLabel, source uint32) (
	[]*model.QAAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*model.QAAttributeLabel
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

func placeholder(c int) string {
	if c <= 0 {
		log.Errorf("invalid placeholder count: %d", c)
		return ""
	}
	return "?" + strings.Repeat(", ?", c-1)
}
