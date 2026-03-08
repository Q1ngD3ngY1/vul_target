package attribute

import (
	"context"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"golang.org/x/exp/maps"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicApp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP_WF"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

// GetDocIdsByAttrSubStr 查询属性名称或者属性标签包含指定子串的文档id列表
func GetDocIdsByAttrSubStr(ctx context.Context, robotId uint64, nameSubStr string) ([]uint64, error) {
	log.InfoContextf(ctx, "GetDocIdsByAttrSubStr robotId:%d nameSubStr:%s", robotId, nameSubStr)
	isDeleted := dao.IsNotDeleted
	// 因为t_doc_attribute_label表缺少robot_id字段，需要先查询t_doc_attribute表，获取该应用，所有的属性id
	attributeFilter := &dao.AttributeFilter{
		RobotId:   robotId,
		IsDeleted: &isDeleted,
	}
	selectColumns := []string{dao.AttributeTblColId}
	attributes, err := dao.GetAttributeDao().GetAttributeList(ctx, selectColumns, attributeFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	attributeIds := make([]uint64, 0)
	for _, attribute := range attributes {
		attributeIds = append(attributeIds, attribute.ID)
	}
	// 查询属性标签表t_doc_attribute_label，获取标签名称或者相似标签包含子串的属性标签id
	attributeLabelFilter := &dao.AttributeLabelFilter{
		RobotId:                  robotId,
		AttrIds:                  attributeIds,
		NameOrSimilarLabelSubStr: nameSubStr,
		IsDeleted:                &isDeleted,
	}
	selectColumns = []string{dao.AttributeLabelTblColAttrId, dao.AttributeLabelTblColId}
	attributeLabels, err := dao.GetAttributeLabelDao().GetAttributeLabelList(ctx, selectColumns, attributeLabelFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	// 再查属性表t_doc_attribute，获取属性名称包含子串的属性id
	attributeFilter = &dao.AttributeFilter{
		RobotId:    robotId,
		NameSubStr: nameSubStr,
		IsDeleted:  &isDeleted,
	}
	selectColumns = []string{dao.AttributeTblColId}
	attributes, err = dao.GetAttributeDao().GetAttributeList(ctx, selectColumns, attributeFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	// 将属性表和属性标签表的属性id合并，去重
	attributeIDMap := make(map[uint64]struct{})
	labelIDs := make([]uint64, 0)
	for _, attribute := range attributes {
		attributeIDMap[attribute.ID] = struct{}{}
	}
	for _, attributeLabel := range attributeLabels {
		if _, ok := attributeIDMap[attributeLabel.AttrID]; ok {
			continue
		}
		labelIDs = append(labelIDs, attributeLabel.ID)
	}
	docIDMap := make(map[uint64]struct{})
	if len(attributeIDMap) != 0 {
		attributeIDs := maps.Keys(attributeIDMap)
		for _, attributeIDChunks := range slicex.Chunk(attributeIDs, dao.MaxSqlInCount) {
			// 查询文档属性标签表，获取所有满足条件的文档id
			docAttributeLabelFilter := &dao.DocAttributeLabelFilter{
				RobotId:   robotId,
				Source:    model.AttributeLabelSourceKg,
				AttrIDs:   attributeIDChunks,
				IsDeleted: &isDeleted,
			}
			selectColumns = []string{dao.DocAttributeLabelTblColDocId}
			docAttributeLabels, err := dao.GetDocAttributeLabelDao().GetDocAttributeLabelList(ctx, selectColumns, docAttributeLabelFilter)
			if err != nil {
				log.ErrorContextf(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
				return nil, err
			}
			for _, docAttributeLabel := range docAttributeLabels {
				docIDMap[docAttributeLabel.DocID] = struct{}{}
			}
		}
	}
	if len(labelIDs) != 0 {
		// 查询文档属性标签表，获取所有满足条件的文档id
		for _, labelIDChunks := range slicex.Chunk(labelIDs, dao.MaxSqlInCount) {
			docAttributeLabelFilter := &dao.DocAttributeLabelFilter{
				RobotId:   robotId,
				Source:    model.AttributeLabelSourceKg,
				LabelIDs:  labelIDChunks,
				IsDeleted: &isDeleted,
			}
			selectColumns = []string{dao.DocAttributeLabelTblColDocId}
			docAttributeLabels, err := dao.GetDocAttributeLabelDao().GetDocAttributeLabelList(ctx, selectColumns, docAttributeLabelFilter)
			if err != nil {
				log.ErrorContextf(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
				return nil, err
			}
			for _, docAttributeLabel := range docAttributeLabels {
				docIDMap[docAttributeLabel.DocID] = struct{}{}
			}
		}
	}

	return maps.Keys(docIDMap), nil
}

// GetDocIdsByAttr 查询已有标签的文档id
func GetDocIdsByAttr(ctx context.Context, robotId uint64) ([]uint64, error) {
	log.InfoContextf(ctx, "GetDocIdsByAttrSubStr robotId:%d", robotId)
	isDeleted := dao.IsNotDeleted
	// 查询文档属性标签表，获取所有满足条件的文档id
	docAttributeLabelFilter := &dao.DocAttributeLabelFilter{
		RobotId:   robotId,
		IsDeleted: &isDeleted,
	}
	selectColumns := []string{dao.DocAttributeLabelTblColDocId}
	docAttributeLabels, err := dao.GetDocAttributeLabelDao().GetDocAttributeLabelList(ctx, selectColumns, docAttributeLabelFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	docIds := make([]uint64, 0)
	docIdMap := make(map[uint64]struct{}, 0)
	for _, docAttributeLabel := range docAttributeLabels {
		if _, ok := docIdMap[docAttributeLabel.DocID]; ok {
			continue
		}
		docIds = append(docIds, docAttributeLabel.DocID)
		docIdMap[docAttributeLabel.DocID] = struct{}{}
	}
	return docIds, nil
}

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
	return jsoniter.MarshalToString(similarLabels)
}

// fillModifyAttributeLabelReq 构造修改属性标签请求结构体
func fillModifyAttributeLabelReq(ctx context.Context, db dao.Dao, robotID uint64, req *pb.ModifyAttributeLabelReq,
	publishParams *model.AttributeLabelUpdateParams, oldAttr *model.Attribute, deleteLabelIDs []uint64, deleteLabelBizIDs []uint64,
	labelBizID2Info map[uint64]*model.AttributeLabel) (*model.UpdateAttributeLabelReq, bool, []model.AttributeLabelRedisValue, error) {
	log.DebugContextf(ctx, "fillModifyAttributeLabelReq robotID:%d, req:%+v publishParams:%+v oldAttr:%+v "+
		"deleteLabelIDs:%+v deleteLabelBizIDs:%+v labelBizID2Info:%+v", robotID, req, publishParams, oldAttr,
		deleteLabelIDs, deleteLabelBizIDs, labelBizID2Info)
	for key, value := range labelBizID2Info {
		log.DebugContextf(ctx, "fillModifyAttributeLabelReq labelBizID2Info key:%d value:%+v", key, value)
	}
	attrNextAction := oldAttr.NextAction
	if oldAttr.NextAction != model.AttributeNextActionAdd {
		attrNextAction = model.AttributeNextActionUpdate
	}
	isNeedPublish := false
	if len(publishParams.LabelIDs) > 0 {
		isNeedPublish = true
	}
	// 构造属性标签
	attr := &model.Attribute{
		ID:            oldAttr.ID,
		BusinessID:    oldAttr.BusinessID,
		RobotID:       robotID,
		AttrKey:       req.GetAttrKey(),
		Name:          req.GetAttrName(),
		ReleaseStatus: model.AttributeStatusWaitRelease,
		NextAction:    attrNextAction,
		IsUpdating:    isNeedPublish,
		UpdateTime:    time.Now(),
	}
	// 构造标签值
	addLabels := make([]*model.AttributeLabel, 0)
	updateLabels := make([]*model.AttributeLabel, 0)
	needUpdateCacheFlag := false
	for _, labelBizId := range deleteLabelBizIDs {
		if labelBizID2Info[labelBizId].SimilarLabel != "" {
			// 如果删除的标签包含相似标签值，需要更新缓存
			needUpdateCacheFlag = true
		}
		delete(labelBizID2Info, labelBizId)
	}
	for _, label := range req.GetLabels() {
		log.DebugContextf(ctx, "fillModifyAttributeLabelReq label:%+v", label)
		similarLabel, err := parseSimilarLabels(label.GetSimilarLabels())
		if err != nil {
			log.DebugContextf(ctx, "parse similar labels err:%v", err)
			return nil, false, nil, err
		}
		if label.GetLabelBizId() == "" {
			newLabel := &model.AttributeLabel{
				RobotID:       robotID,
				BusinessID:    db.GenerateSeqID(),
				AttrID:        oldAttr.ID,
				Name:          label.GetLabelName(),
				SimilarLabel:  similarLabel,
				ReleaseStatus: model.AttributeStatusWaitRelease,
				NextAction:    model.AttributeNextActionAdd,
			}
			addLabels = append(addLabels, newLabel)
			labelBizID2Info[newLabel.BusinessID] = newLabel
			if len(label.GetSimilarLabels()) > 0 {
				// 如果新增的标签包含相似标签值，需要更新缓存
				needUpdateCacheFlag = true
			}
			continue
		}
		labelBizID, err := util.CheckReqParamsIsUint64(ctx, label.GetLabelBizId())
		if err != nil {
			return nil, false, nil, err
		}
		labelInfo, ok := labelBizID2Info[labelBizID]
		if !ok || labelInfo == nil {
			log.WarnContextf(ctx, "labelBizID:%d not found", labelBizID)
			continue
		}
		if labelBizID2Info[labelBizID].SimilarLabel != "" || len(label.GetSimilarLabels()) > 0 {
			// 如果更新的标签变更前后包含相似标签值，需要更新缓存
			needUpdateCacheFlag = true
		}
		labelNextAction := labelBizID2Info[labelBizID].NextAction
		if labelBizID2Info[labelBizID].NextAction != model.AttributeNextActionAdd {
			labelNextAction = model.AttributeNextActionUpdate
		}
		updateLabel := &model.AttributeLabel{
			ID:            labelBizID2Info[labelBizID].ID,
			RobotID:       robotID,
			BusinessID:    labelBizID,
			AttrID:        oldAttr.ID,
			Name:          label.GetLabelName(),
			SimilarLabel:  similarLabel,
			ReleaseStatus: model.AttributeStatusWaitRelease,
			NextAction:    labelNextAction,
		}
		labelBizID2Info[labelBizID] = updateLabel
		updateLabels = append(updateLabels, updateLabel)
	}
	updateAttributeLabelReq := &model.UpdateAttributeLabelReq{
		IsNeedPublish:     isNeedPublish,
		PublishParams:     *publishParams,
		Attr:              attr,
		DeleteLabelIDs:    deleteLabelIDs,
		DeleteLabelBizIDs: deleteLabelBizIDs,
		AddLabels:         addLabels,
		UpdateLabels:      updateLabels,
	}
	newLabelRedisValue := make([]model.AttributeLabelRedisValue, 0)
	for _, label := range labelBizID2Info {
		if label.SimilarLabel == "" {
			// 只缓存包含相似标签值的标签
			continue
		}
		newLabelRedisValue = append(newLabelRedisValue, model.AttributeLabelRedisValue{
			BusinessID:    label.BusinessID,
			Name:          label.Name,
			SimilarLabels: label.SimilarLabel,
		})
	}
	return updateAttributeLabelReq, needUpdateCacheFlag, newLabelRedisValue, nil
}

// ModifyAttributeLabel 编辑属性标签
func ModifyAttributeLabel(ctx context.Context, db dao.Dao, req *pb.ModifyAttributeLabelReq) (
	*pb.ModifyAttributeLabelRsp, error) {
	log.InfoContextf(ctx, "ModifyAttributeLabel Req:%+v", req)
	var err error
	rsp := new(pb.ModifyAttributeLabelRsp)
	// 检查应用
	app, err := logicApp.CheckApp(ctx, db, req.GetBotBizId())
	if err != nil {
		return rsp, err
	}
	// 检查属性
	oldAttr, err := checkModifyAttribute(ctx, db, app.ID, req)
	if err != nil {
		return nil, err
	}
	// 收集所有变化的标签信息
	labelBizID2Info := make(map[uint64]*model.AttributeLabel)
	// 检查待删除的标签值
	deleteLabelBizIDs, deleteLabelIDs, err :=
		checkModifyAttributeDeleteLabels(ctx, db, req.GetDeleteLabelBizIds(), app.ID, oldAttr.ID, labelBizID2Info)
	if err != nil {
		return nil, err
	}
	// 检查待更新的标签值
	needPublishLabelIDs, err :=
		checkModifyAttributeUpdateLabels(ctx, db, req.GetLabels(), app.ID, oldAttr.ID, labelBizID2Info)
	if err != nil {
		return nil, err
	}
	// 构造发布参数
	publishParams := &model.AttributeLabelUpdateParams{
		CorpID:   app.CorpID,
		StaffID:  app.StaffID,
		RobotID:  app.ID,
		AttrID:   oldAttr.ID,
		LabelIDs: needPublishLabelIDs,
	}

	if err := checkNeedPublishLabelDocAndQaStatus(ctx, db, app.ID, publishParams); err != nil {
		return rsp, err
	}
	// 检查待新增的标签值
	addLabelCounts, err := checkModifyAttributeAddLabels(ctx, req.GetLabels(), labelBizID2Info)
	if err != nil {
		return nil, err
	}
	// 检查标签值总数是否超限
	uin := pkg.Uin(ctx)
	if !utilConfig.IsInWhiteList(uin, app.BusinessID, utilConfig.GetWhitelistConfig().InfinityAttributeLabel) {
		// 非白名单应用需要校验标签值总数是否超限
		filter := &dao.AttributeLabelFilter{
			RobotId: app.ID,
			AttrIds: []uint64{oldAttr.ID},
		}
		selectColumns := []string{dao.AttributeLabelTblColId}
		count, err := dao.GetAttributeLabelDao().GetAttributeLabelCount(ctx, selectColumns, filter)
		if err != nil {
			return nil, err
		}
		if uint64(count)+addLabelCounts-uint64(len(deleteLabelBizIDs)) >
			uint64(config.App().AttributeLabel.LabelLimit) {
			return nil, errs.ErrAttributeLabelLimit
		}
	}
	updateAttributeLabelReq, needUpdateCacheFlag, newLabelRedisValue, err := fillModifyAttributeLabelReq(ctx, db,
		app.ID, req, publishParams, oldAttr, deleteLabelIDs, deleteLabelBizIDs, labelBizID2Info)
	log.DebugContextf(ctx, "ModifyAttributeLabel updateAttributeLabelReq:%+v needUpdateCacheFlag:%+v "+
		"newLabelRedisValue:%+v ", updateAttributeLabelReq, needUpdateCacheFlag, newLabelRedisValue)
	taskID, err := db.UpdateAttribute(ctx, updateAttributeLabelReq, oldAttr, app.CorpID, app.StaffID,
		needUpdateCacheFlag, newLabelRedisValue)
	if err != nil {
		return rsp, err
	}
	rsp.TaskId = taskID
	rsp.Labels = make([]*pb.AttributeLabel, len(updateAttributeLabelReq.AddLabels))
	for i, label := range updateAttributeLabelReq.AddLabels {
		rsp.Labels[i] = &pb.AttributeLabel{
			LabelBizId: strconv.FormatUint(label.BusinessID, 10),
			LabelName:  label.Name,
		}
	}
	//feature_permission 有删除的属性值需要删除绑定关系
	go dao.GetRoleDao(nil).BatchDeleteRoleLabel(trpc.CloneContext(ctx), app.BusinessID, deleteLabelBizIDs)
	_ = db.AddOperationLog(ctx, model.AttributeLabelUpdate, app.CorpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// GetWorkflowListByAttributeLabel 获取标签值被引用的工作流列表
func GetWorkflowListByAttributeLabel(ctx context.Context, req *pb.CheckAttributeLabelReferReq) (
	[]*KEP_WF.AttributeLabelRefByWorkflow, error) {
	rsp, err := client.GetWorkflowListByAttributeLabel(ctx, req.BotBizId, []string{req.GetLabelBizId()})
	if err != nil {
		log.ErrorContextf(ctx, "GetWorkflowListByAttributeLabel failed, err: %+v", err)
		return nil, err
	}
	if rsp == nil || len(rsp.GetList()) == 0 {
		return []*KEP_WF.AttributeLabelRefByWorkflow{}, nil
	}
	return rsp.GetList(), nil
}

// GetWorkflowListByAttribute 获取标签被引用的工作流列表
func GetWorkflowListByAttribute(ctx context.Context, req *pb.CheckAttributeReferWorkFlowReq) (
	[]*KEP_WF.AttributeRefByWorkflow, error) {
	rsp, err := client.GetWorkflowListByAttribute(ctx, req.BotBizId, req.GetAttributeBizIds())
	if err != nil {
		log.ErrorContextf(ctx, "GetWorkflowListByAttribute failed, err: %+v", err)
		return nil, err
	}
	if rsp == nil || len(rsp.GetList()) == 0 {
		return []*KEP_WF.AttributeRefByWorkflow{}, nil
	}
	return rsp.GetList(), nil
}
