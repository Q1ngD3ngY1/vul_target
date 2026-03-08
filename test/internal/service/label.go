package service

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	entity0 "git.woa.com/adp/kb/kb-config/internal/entity"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

// ModifyAttributeLabel 编辑属性标签
func (s *Service) ModifyAttributeLabel(ctx context.Context, req *pb.ModifyAttributeLabelReq) (
	*pb.ModifyAttributeLabelRsp, error) {
	rsp := new(pb.ModifyAttributeLabelRsp)
	logx.I(ctx, "ModifyAttributeLabel Req:%+v", req)
	modifyRsp, err := s.labelLogic.ModifyAttributeLabel(ctx, req)
	if err != nil {
		logx.E(ctx, "ModifyAttributeLabel ModifyAttributeLabel failed, err:%+v", err)
		return nil, err
	}
	// feature_permission 有删除的属性值需要删除绑定关系
	go s.userLogic.DeleteKnowledgeRoleAttributeLabelByAttrAndLabelBizIDs(trpc.CloneContext(ctx), modifyRsp.BusinessID, modifyRsp.DeleteLabelBizIDs, nil, 200, 10000)
	rsp.TaskId = modifyRsp.TaskId
	rsp.Labels = make([]*pb.AttributeLabel, len(modifyRsp.AddLabels))
	for i, label := range modifyRsp.AddLabels {
		rsp.Labels[i] = &pb.AttributeLabel{
			LabelBizId: strconv.FormatUint(label.BusinessID, 10),
			LabelName:  label.Name,
		}
	}
	logx.I(ctx, "ModifyAttributeLabel Rsp:%+v", rsp)
	return rsp, nil
}

// UploadAttributeLabel 导入属性标签
func (s *Service) UploadAttributeLabel(ctx context.Context, req *pb.UploadAttributeLabelReq) (
	*pb.UploadAttributeLabelRsp, error) {
	logx.I(ctx, "UploadAttributeLabel Req:%+v", req)
	rsp := new(pb.UploadAttributeLabelRsp)
	key := fmt.Sprintf(dao.LockForUplodAttributeLabel, req.GetCosHash())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		logx.E(ctx, "UploadAttributeLabel file lock req:%+v,err :%v", req, err)
		return rsp, errs.ErrAttributeLabelUploading
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}
	if err = s.s3.CheckURLPrefix(ctx, app.CorpPrimaryId, corp.GetCorpId(), app.BizId, req.CosUrl); err != nil {
		logx.E(ctx, "UploadAttributeLabel|CheckURLPrefix failed, err:%+v", err)
		return rsp, errs.ErrInvalidURL
	}
	rows, errRsp, err := s.checkAttributeLabelXlsx(ctx, app, req)
	if err != nil || errRsp != nil {
		return errRsp, err
	}
	items := s.getAttributeLabelsFromRows(app.PrimaryId, rows)
	if _, err := s.labelLogic.BatchCreateAttribute(ctx, items, nil); err != nil {
		return rsp, err
	}
	return rsp, nil
}

// CreateAttributeLabel 创建属性标签
func (s *Service) CreateAttributeLabel(ctx context.Context, req *pb.CreateAttributeLabelReq) (*pb.CreateAttributeLabelRsp, error) {
	logx.I(ctx, "CreateAttributeLabel Req:%+v", req)
	rsp := new(pb.CreateAttributeLabelRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		logx.W(ctx, "CreateAttributeLabel rpc  DescribeAppAndCheckCorp failed ,err =%v")
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		logx.W(ctx, "CreateAttributeLabel app.IsWriteable() failed ,err =%v", err)
		return rsp, err
	}
	req.AttrKey, _ = generateAttrKey(req.GetAttrName())

	// 构建 通用的logic 层的请求结构
	createReq := s.labelLogic.BuildCreateAttributeLabelReq(ctx, app.PrimaryId, app.BizId, req)
	if err := s.labelLogic.CheckCreateAttributeLabel(ctx, createReq); err != nil {
		return rsp, err
	}

	attrLabel, err := s.labelLogic.BuildAttributeLabelItem(ctx, createReq)
	if err != nil {
		return rsp, err
	}

	if _, err := s.labelLogic.BatchCreateAttribute(ctx, []*labelEntity.AttributeLabelItem{attrLabel}, nil); err != nil {
		logx.W(ctx, "CreateAttributeLabel BatchCreateAttribute failed ,err =%v", err)
		return rsp, err
	}
	rsp.AttrBizId = strconv.FormatUint(attrLabel.Attr.BusinessID, 10)
	rsp.Labels = make([]*pb.AttributeLabel, len(attrLabel.Labels))
	for i, label := range attrLabel.Labels {
		rsp.Labels[i] = &pb.AttributeLabel{
			LabelBizId: strconv.FormatUint(label.BusinessID, 10),
			LabelName:  label.Name,
		}
	}
	logx.I(ctx, "CreateAttributeLabel Rsp:%+v", rsp)
	return rsp, nil
}

// DeleteAttributeLabel 删除属性标签
func (s *Service) DeleteAttributeLabel(ctx context.Context, req *pb.DeleteAttributeLabelReq) (
	*pb.DeleteAttributeLabelRsp, error) {
	logx.I(ctx, "DeleteAttributeLabel Req:%+v", req)
	rsp := new(pb.DeleteAttributeLabelRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	// TODO(ericjwang): 这段 if 逻辑如何理解？
	// 云API 3.0逻辑 后续需要移除if和if之下以外的逻辑
	if len(req.GetAttributeBizIds()) > 0 {
		var ids []uint64
		var attrKeys []string
		attributesBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetAttributeBizIds())
		if err != nil {
			return rsp, err
		}
		mapAttrID2Info, err := s.labelDao.GetAttributeByBizIDs(ctx, app.PrimaryId, attributesBizIDs)
		if err != nil {
			return rsp, err
		}
		if len(mapAttrID2Info) != len(req.GetAttributeBizIds()) {
			return rsp, errs.ErrAttributeLabelNotFound
		}
		for _, info := range mapAttrID2Info {
			if info.ReleaseStatus == labelEntity.AttributeStatusReleasing ||
				info.ReleaseStatus == labelEntity.AttributeStatusReleaseUpdating {
				return rsp, errs.ErrAttributeLabelUpdating
			}
			ids = append(ids, info.ID)
			attrKeys = append(attrKeys, info.AttrKey)
		}
		if err := s.checkAttributeUsed(ctx, app, attributesBizIDs); err != nil {
			return rsp, err
		}
		if err := s.checkAttributeLabelUsed(ctx, app.PrimaryId, labelEntity.AttributeLabelSourceKg, ids, nil); err != nil {
			return rsp, err
		}
		if err := s.labelLogic.DeleteAttribute(ctx, app.PrimaryId, ids, attrKeys); err != nil {
			return rsp, err
		}
		// feature_permission
		// 删除属性标签需要同步删除角色标签绑定关系 异步删除，不要影响原功能
		go s.userLogic.DeleteKnowledgeRoleAttributeLabelByAttrAndLabelBizIDs(trpc.CloneContext(ctx), app.BizId, attributesBizIDs, nil, 200, 10000)
		return rsp, nil
	}
	reqIDs, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return nil, err
	}
	mapAttrID2Info, err := s.labelDao.GetAttributeByIDs(ctx, app.PrimaryId, reqIDs)
	if err != nil {
		return rsp, err
	}
	if len(mapAttrID2Info) != len(req.GetIds()) {
		return rsp, errs.ErrAttributeLabelNotFound
	}
	var attrKeys []string
	var attributesBizIDs []uint64
	for _, info := range mapAttrID2Info {
		if info.ReleaseStatus == labelEntity.AttributeStatusReleasing ||
			info.ReleaseStatus == labelEntity.AttributeStatusReleaseUpdating {
			return rsp, errs.ErrAttributeLabelUpdating
		}
		attrKeys = append(attrKeys, info.AttrKey)
		attributesBizIDs = append(attributesBizIDs, info.BusinessID)
	}
	if err := s.checkAttributeUsed(ctx, app, attributesBizIDs); err != nil {
		return rsp, err
	}
	if err := s.checkAttributeLabelUsed(ctx, app.PrimaryId, labelEntity.AttributeLabelSourceKg, reqIDs, nil); err != nil {
		return rsp, err
	}

	if err := s.labelLogic.DeleteAttribute(ctx, app.PrimaryId, reqIDs, attrKeys); err != nil {
		return rsp, err
	}
	// feature_permission
	// 删除属性标签需要同步删除角色标签绑定关系
	go s.userLogic.DeleteKnowledgeRoleAttributeLabelByAttrAndLabelBizIDs(trpc.CloneContext(ctx), app.BizId, attributesBizIDs, nil, 200, 10000)
	return rsp, nil
}

// ListAttributeLabel 查询属性标签列表
func (s *Service) ListAttributeLabel(ctx context.Context, req *pb.ListAttributeLabelReq) (
	*pb.ListAttributeLabelRsp, error) {
	logx.I(ctx, "ListAttributeLabel req: %+v", req)
	rsp := new(pb.ListAttributeLabelRsp)
	rsp.List = make([]*pb.ListAttributeLabelRsp_AttrLabelDetail, 0)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if req.GetPageSize() > 2000 {
		return rsp, errs.ErrParamsNotExpected
	}
	var list []*labelEntity.Attribute
	if config.App().EsSearch.AttributeEnableEs && req.GetQuery() != "" {
		ids, total, err := s.labelLogic.GetAttrIDBySearchLabelsWithEs(ctx, app.PrimaryId, req)
		if err != nil {
			return rsp, err
		}

		rsp.Total = uint64(total)
		list, err = s.labelDao.GetAttributeListByIDs(ctx, app.PrimaryId, ids)
		if err != nil {
			return rsp, err
		}
	} else {
		total, err := s.labelDao.GetAttributeTotal(ctx, app.PrimaryId, req.GetQuery(), nil)
		logx.D(ctx, "total:%+v", total)
		if err != nil {
			return rsp, err
		}
		rsp.Total = total
		if rsp.GetTotal() == 0 {
			return rsp, nil
		}
		list, err = s.labelDao.GetAttributeList(ctx, app.PrimaryId, req.GetQuery(), req.GetPageNumber(), req.GetPageSize(), nil)
		if err != nil {
			return rsp, err
		}
	}

	mapAttrID2Labels := make(map[uint64][]*labelEntity.AttributeLabel)
	mapAttrID2LabelTotal := make(map[uint64]uint64)
	for _, v := range list {
		labelTotal, err := s.labelDao.GetAttributeLabelCount(ctx, v.ID, req.GetQuery(), "", app.PrimaryId)
		if err != nil {
			return rsp, err
		}
		mapAttrID2LabelTotal[v.ID] = labelTotal
		attrLabels, err := s.labelDao.GetAttributeLabelList(ctx, v.ID, "", "", 0, req.GetLabelSize(), app.PrimaryId)
		if err != nil {
			return rsp, err
		}
		mapAttrID2Labels[v.ID] = attrLabels
	}
	for _, v := range list {
		attrLabel := &pb.ListAttributeLabelRsp_AttrLabelDetail{
			AttrBizId:  v.BusinessID,
			AttrKey:    v.AttrKey,
			AttrName:   v.Name,
			IsUpdating: v.IsUpdating,
			Status:     v.ReleaseStatus,
			StatusDesc: i18n.Translate(ctx, v.StatusDesc()),
		}
		if labelTotal, ok := mapAttrID2LabelTotal[v.ID]; ok {
			attrLabel.LabelTotalCount = labelTotal
		}
		for _, label := range mapAttrID2Labels[v.ID] {
			attrLabel.LabelNames = append(attrLabel.LabelNames, label.Name)
		}
		rsp.List = append(rsp.List, attrLabel)
	}
	return rsp, nil
}

// DescribeAttributeLabel 查询属性标签详情
func (s *Service) DescribeAttributeLabel(ctx context.Context, req *pb.DescribeAttributeLabelReq) (
	*pb.DescribeAttributeLabelRsp, error) {
	logx.I(ctx, "DescribeAttributeLabel Req:%+v", req)
	rsp := new(pb.DescribeAttributeLabelRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	attributeBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetAttributeBizId())
	if err != nil {
		return nil, err
	}
	mapAttrID2Info, err := s.labelDao.GetAttributeByBizIDs(ctx, app.PrimaryId, []uint64{attributeBizId})
	if err != nil {
		return rsp, err
	}
	attr, ok := mapAttrID2Info[attributeBizId]
	if !ok {
		return rsp, errs.ErrAttributeLabelNotFound
	}
	total, err := s.labelDao.GetAttributeLabelCount(ctx, attr.ID, req.GetQuery(), req.GetQueryScope(), app.PrimaryId)
	if err != nil {
		return rsp, err
	}
	rsp.AttributeBizId, rsp.AttrKey, rsp.AttrName, rsp.LabelNumber = attr.BusinessID, attr.AttrKey, attr.Name, total
	if rsp.GetLabelNumber() == 0 {
		return rsp, nil
	}
	var labelID uint64
	astLabelBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetLastLabelBizId())
	if err != nil {
		return rsp, nil
	}
	if astLabelBizId != 0 {
		lastLabelBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetLastLabelBizId())
		if err != nil {
			return nil, err
		}
		labels, err := s.labelDao.GetAttributeLabelByBizIDs(ctx, []uint64{lastLabelBizID}, app.PrimaryId)
		if err != nil {
			return rsp, errs.ErrSystem
		}
		label, ok := labels[lastLabelBizID]
		if !ok {
			return rsp, errs.ErrAttributeLabelNotFound
		}
		labelID = label.ID
	}
	var list []*labelEntity.AttributeLabel
	if config.App().EsSearch.AttributeEnableEs && req.GetQuery() != "" {
		ids, err := s.labelLogic.QueryAttributeLabelCursor(ctx, attr.ID, req.GetQuery(),
			req.GetQueryScope(), labelID, req.GetLimit(), app.PrimaryId)
		if err != nil {
			return rsp, err
		}
		list, err = s.labelDao.GetAttributeLabelByIDOrder(ctx, app.PrimaryId, ids)
		if err != nil {
			return rsp, err
		}
	} else {
		list, err = s.labelDao.GetAttributeLabelList(ctx, attr.ID, req.GetQuery(), req.GetQueryScope(), labelID,
			req.GetLimit(), app.PrimaryId)
		if err != nil {
			return rsp, err
		}
	}
	for _, v := range list {
		similarLabels, err := getSimilarLabels(v.SimilarLabel)
		if err != nil {
			logx.D(ctx, "parse similar labels req:%+v, err:%+v", req, err)
			return rsp, err
		}
		rsp.Labels = append(rsp.Labels, &pb.AttributeLabel{
			LabelBizId:    cast.ToString(v.BusinessID),
			LabelName:     v.Name,
			SimilarLabels: similarLabels,
		})
	}
	return rsp, nil
}

func (s *Service) CheckAttributeReferWorkFlow(ctx context.Context, req *pb.CheckAttributeReferWorkFlowReq) (
	*pb.CheckAttributeReferWorkFlowRsp, error) {
	logx.I(ctx, "CheckAttributeReferWorkFlow Req:%+v", req)
	rsp := new(pb.CheckAttributeReferWorkFlowRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	attributeBizIds, err := util.CheckReqSliceUint64(ctx, req.GetAttributeBizIds())
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "CheckAttributeReferWorkFlow|attributeBizIds:%+v", attributeBizIds)
	workFlowList, err := s.labelLogic.GetWorkflowListByAttribute(ctx, req)
	if err != nil {
		return rsp, err
	}
	logx.D(ctx, "GetWorkflowListByAttribute|workFlowList:%+v", workFlowList)
	rsp.List = workFlowList
	return rsp, nil
}

func (s *Service) CheckAttribute(ctx context.Context, req *pb.CheckAttributeReq) (*pb.CheckAttributeRsp, error) {
	logx.I(ctx, "CheckAttribute Req:%+v", req)
	rsp := new(pb.CheckAttributeRsp)
	var err error
	var referRsp *pb.CheckAttributeLabelReferRsp
	var workFlowRsp *pb.CheckAttributeReferWorkFlowRsp
	var existRsp *pb.CheckAttributeLabelExistRsp
	switch req.GetCheckType() {
	case pb.CheckAttributeLabelType_CHECK_ATTRIBUTE_LABEL_TYPE_REFER:
		referRsp, err = s.CheckAttributeLabelRefer(ctx, req.GetCheckAttributeLabelReferReq())
		rsp.CheckAttributeLabelReferRsp = referRsp

	case pb.CheckAttributeLabelType_CHECK_ATTRIBUTE_LABEL_TYPE_REFER_WORKFLOW:
		workFlowRsp, err = s.CheckAttributeReferWorkFlow(ctx, req.GetCheckAttributeReferWorkflowReq())
		rsp.CheckAttributeReferWorkflowRsp = workFlowRsp

	case pb.CheckAttributeLabelType_CHECK_ATTRIBUTE_LABEL_TYPE_EXIST:
		existRsp, err = s.CheckAttributeLabelExist(ctx, req.GetCheckAttributeLabelExistReq())
		rsp.CheckAttributeLabelExistRsp = existRsp
	default:
		logx.W(ctx, "CheckAttribute unknown check type value = %v", req.GetCheckType())
		return nil, errs.ErrCheckAttributeTypeUnknown
	}
	if err != nil {
		logx.W(ctx, "CheckAttribute|req:%+v, err:%+v", req, err)
		return nil, err
	}
	logx.I(ctx, "CheckAttribute|rsp:%+v", rsp)
	return rsp, nil
}

func (s *Service) CheckAttributeLabelRefer(ctx context.Context, req *pb.CheckAttributeLabelReferReq) (
	*pb.CheckAttributeLabelReferRsp, error) {
	logx.I(ctx, "CheckAttributeLabelRefer Req:%+v", req)
	rsp := new(pb.CheckAttributeLabelReferRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	// API兼容逻辑，上云后需要删除此处if判断及if以下代码逻辑
	if len(req.GetAttributeBizId()) > 0 || len(req.GetLabelBizId()) > 0 {
		var attrIds []uint64
		attributeBizIds, err := util.CheckReqSliceUint64(ctx, req.GetAttributeBizId())
		if err != nil {
			return nil, err
		}
		attrs, err := s.labelDao.GetAttributeByBizIDs(ctx, app.PrimaryId, attributeBizIds)
		if err != nil {
			return rsp, errs.ErrSystem
		}
		labelBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetLabelBizId())
		if err != nil {
			return nil, err
		}
		labels, err := s.labelDao.GetAttributeLabelByBizIDs(ctx, []uint64{labelBizID}, app.PrimaryId)
		if err != nil {
			return rsp, errs.ErrSystem
		}
		for _, v := range attrs {
			attrIds = append(attrIds, v.ID)
		}
		var labelIDs []uint64
		if _, ok := labels[labelBizID]; ok {
			labelIDs = append(labelIDs, labels[labelBizID].ID)
		}
		isUsed, err := s.isAttributeLabelUsed(ctx, app.PrimaryId, labelEntity.AttributeLabelSourceKg, attrIds, labelIDs)
		if err != nil {
			return rsp, err
		}
		rsp.IsRefer = isUsed
		workFlowList, err := s.labelLogic.GetWorkflowListByAttributeLabel(ctx, req)
		if err != nil {
			return rsp, err
		}
		logx.D(ctx, "GetWorkflowListByAttributeLabel|workFlowList:%+v", workFlowList)
		rsp.List = workFlowList
		return rsp, nil
	}
	if len(req.GetIds()) == 0 && req.GetLabelId() == "" {
		logx.D(ctx, "CheckAttributeLabelRefer params err req:%+v", req)
		return rsp, errs.ErrParams
	}
	labelIDs := make([]uint64, 0)
	if len(req.GetLabelId()) > 0 {
		labelID, err := util.CheckReqParamsIsUint64(ctx, req.GetLabelId())
		if err != nil {
			return nil, err
		}
		labelIDs = append(labelIDs, labelID)
	}
	reqIDs, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return nil, err
	}
	isUsed, err := s.isAttributeLabelUsed(ctx, app.PrimaryId, labelEntity.AttributeLabelSourceKg, reqIDs, labelIDs)
	if err != nil {
		return rsp, err
	}
	rsp.IsRefer = isUsed
	workFlowList, err := s.labelLogic.GetWorkflowListByAttributeLabel(ctx, req)
	if err != nil {
		return rsp, err
	}
	logx.D(ctx, "GetWorkflowListByAttributeLabel|workFlowList:%+v", workFlowList)
	rsp.List = workFlowList
	return rsp, nil
}

// ================================================================================================================================
func (s *Service) isAttributeLabelUsed(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64) (bool, error) {
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return false, nil
	}
	var docAttributeLabelCount, qaAttributeLabelCount uint64
	var docAttributeLabelErr, qaAttributeLabelErr error
	g := errgroupx.New()
	g.SetLimit(10)
	g.Go(func() error {
		docAttributeLabelCount, docAttributeLabelErr = s.labelDao.GetDocAttributeLabelCountByAttrLabelIDs(ctx,
			robotID, source, attrIDs, labelIDs)
		if docAttributeLabelErr != nil {
			return docAttributeLabelErr
		}
		return nil
	})
	g.Go(func() error {
		qaAttributeLabelCount, qaAttributeLabelErr = s.labelDao.GetQAAttributeLabelCountByAttrLabelIDs(ctx,
			robotID, source, attrIDs, labelIDs)
		if qaAttributeLabelErr != nil {
			return qaAttributeLabelErr
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		logx.W(ctx, "checkAttributeLabelUsed robotID:%d,source:%d,attrIDs:%+v,labelIDs:%+v err :%v",
			robotID, source, attrIDs, labelIDs, err)
		return false, err
	}
	if docAttributeLabelCount > 0 || qaAttributeLabelCount > 0 {
		return true, nil
	}
	return false, nil
}

// getSimilarLabels TODO
func getSimilarLabels(similarLabelStr string) ([]string, error) {
	if len(similarLabelStr) == 0 {
		return nil, nil
	}
	var similarLabels []string
	err := jsonx.UnmarshalFromString(similarLabelStr, &similarLabels)
	return similarLabels, err
}

// getAttributeLabelsFromRows TODO
func (s *Service) getAttributeLabelsFromRows(robotID uint64, rows [][]string) []*labelEntity.AttributeLabelItem {
	items := make([]*labelEntity.AttributeLabelItem, 0)
	mapAttrInfo := make(map[string]*labelEntity.AttributeLabelItem)
	for _, row := range rows {
		attrKey, attrName, labelName, similarLabels := getAttributeLabelInfoFromRow(row)
		similarLabel, _ := parseSimilarLabels(similarLabels)
		label := &labelEntity.AttributeLabel{
			BusinessID:    idgen.GetId(),
			RobotID:       robotID,
			Name:          labelName,
			SimilarLabel:  similarLabel,
			ReleaseStatus: labelEntity.AttributeStatusWaitRelease,
			NextAction:    labelEntity.AttributeNextActionAdd,
		}
		if item, ok := mapAttrInfo[attrKey]; ok {
			item.Labels = append(item.Labels, label)
			continue
		}
		attr := &labelEntity.Attribute{
			BusinessID:    idgen.GetId(),
			RobotID:       robotID,
			AttrKey:       attrKey,
			Name:          attrName,
			ReleaseStatus: labelEntity.AttributeStatusWaitRelease,
			NextAction:    labelEntity.AttributeNextActionAdd,
		}
		item := &labelEntity.AttributeLabelItem{Attr: attr}
		item.Labels = append(item.Labels, label)
		items = append(items, item)
		mapAttrInfo[attrKey] = item
	}
	return items
}

// checkAttributeLabelXlsx 检查属性标签文件是否符合要求
func (s *Service) checkAttributeLabelXlsx(ctx context.Context, app *entity0.App, req *pb.UploadAttributeLabelReq) (
	[][]string, *pb.UploadAttributeLabelRsp, error) {
	body, err := s.s3.GetObject(ctx, req.GetCosUrl())
	if err != nil {
		logx.E(ctx, "checkSampleXlsx file get file by url err :%v", err)
		return nil, nil, errs.ErrSystem
	}
	fileName := strings.TrimSuffix(req.GetFileName(), ".xlsx")
	if len(fileName) == 0 {
		return nil, nil, errs.ErrInvalidFileName
	}
	mapAttrKey, mapAttrName, err := s.labelDao.GetAttributeByRobotID(ctx, app.PrimaryId)
	if err != nil {
		return nil, nil, err
	}
	attrKeyNamePair := make(map[string]*labelEntity.AttrKeyNamePair)
	attrNameKeyPair := make(map[string]*labelEntity.AttrKeyNamePair)
	attrLabelCount := make(map[string]int)
	total := len(mapAttrKey)
	check := func(ctx context.Context, i int, row []string, uniqueAttrLabel map[string]int) string {
		errMsg := make([]string, 0)
		attrKey, attrName, labelName, similarLabels := getAttributeLabelInfoFromRow(row)
		// 检查属性格式信息
		attrMsgs := checkAttributeLabelRowAttr(ctx, i, attrKey, attrName, attrKeyNamePair, attrNameKeyPair, total,
			mapAttrKey, mapAttrName)
		errMsg = append(errMsg, attrMsgs...)
		// 检查标签格式信息
		labelMsgs := checkAttributeLabelRowAttrLabel(ctx, app, i, attrKey, labelName, similarLabels, attrLabelCount,
			uniqueAttrLabel)
		errMsg = append(errMsg, labelMsgs...)
		if len(errMsg) > 0 {
			return strings.Join(errMsg, ";")
		}
		return ""
	}
	return s.getAttributeLabelRow(ctx, req, fileName, body, check)
}

// getAttributeLabelInfoFromRow TODO
func getAttributeLabelInfoFromRow(row []string) (string, string, string, []string) {
	var attrKey, attrName, labelName, similarLabelStr string
	for cellIndex, cell := range row {
		switch cellIndex {
		case labelEntity.ExcelTplAttrNameIndex:
			attrName = strings.TrimSpace(cell)
		case labelEntity.ExcelTplLabelIndex:
			labelName = strings.TrimSpace(cell)
		case labelEntity.ExcelTplSimilarLabelIndex:
			similarLabelStr = strings.TrimSpace(cell)
		}
	}
	attrKey, err := generateAttrKey(attrName)
	if err != nil {
		return attrKey, attrName, labelName, nil
	}

	if len(similarLabelStr) == 0 {
		return attrKey, attrName, labelName, nil
	}
	similarLabels := make([]string, 0)
	values := strings.Split(strings.ReplaceAll(similarLabelStr, "，", ","), ",")
	for _, v := range values {
		value := strings.TrimSpace(v)
		if len(value) == 0 {
			continue
		}
		similarLabels = append(similarLabels, value)
	}
	return attrKey, attrName, labelName, similarLabels
}

// fillAttrLabel TODO
func fillAttrLabel(attrKey, labelName string) string {
	return fmt.Sprintf("%s_%s", attrKey, labelName)
}

// checkAttributeLabelRowAttrLabel TODO
func checkAttributeLabelRowAttrLabel(ctx context.Context, app *entity0.App, i int, attrKey, labelName string, similarLabels []string,
	attrLabelCount, uniqueAttrLabel map[string]int) []string {
	errMsgs := make([]string, 0)
	if utf8.RuneCountInString(attrKey) == 0 {
		return errMsgs
	}
	labelNameLen := utf8.RuneCountInString(labelName)
	if labelNameLen == 0 {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagEmpty))
	}
	if labelNameLen > config.App().AttributeLabel.LabelNameMaxLen {
		errMsgs = append(errMsgs,
			i18n.Translate(ctx, i18nkey.KeyTagCharLengthExceed, i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
				config.App().AttributeLabel.LabelNameMaxLen)))
	}
	if labelNameLen > 0 && labelName == config.App().AttributeLabel.FullLabelValue {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagCannotUseSystemKeyword))
	}
	attrLabelKey := fillAttrLabel(attrKey, labelName)
	if lastRowID, ok := uniqueAttrLabel[attrLabelKey]; labelNameLen > 0 && ok {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagDuplicateWithLine, lastRowID))
	}
	if labelNameLen > 0 {
		uniqueAttrLabel[attrLabelKey] = i + 1
	}
	if len(similarLabels) > config.App().AttributeLabel.SimilarLabelLimit {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeySynonymCountUnderTagExceedLimit,
			config.App().AttributeLabel.SimilarLabelLimit))
	}
	for _, similarLabel := range similarLabels {
		similarLabelLen := utf8.RuneCountInString(similarLabel)
		if similarLabelLen > config.App().AttributeLabel.SimilarLabelMaxLen {
			errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeySynonymCharLengthExceed, similarLabel,
				i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
					config.App().AttributeLabel.SimilarLabelMaxLen)))
		}
		if similarLabelLen == 0 {
			continue
		}
		attrSimilarLabelKey := fillAttrLabel(attrKey, similarLabel)
		if lastRowID, ok := uniqueAttrLabel[attrSimilarLabelKey]; ok {
			errMsgs = append(errMsgs,
				i18n.Translate(ctx, i18nkey.KeySynonymDuplicateWithLine, similarLabel, lastRowID))
		}
		uniqueAttrLabel[attrSimilarLabelKey] = i + 1
	}
	if labelNameLen > 0 {
		attrLabelCount[attrKey] += 1
	}
	// 检查属性数量限制
	uin := contextx.Metadata(ctx).Uin()
	if !config.IsInWhiteList(uin, app.BizId, config.GetWhitelistConfig().InfinityAttributeLabel) {
		if attrLabelCount[attrKey] > config.App().AttributeLabel.LabelLimit {
			errMsgs = append(errMsgs,
				i18n.Translate(ctx, i18nkey.KeyTagCountUnderAttributeExceedLimit, config.App().AttributeLabel.LabelLimit))
		}
	}
	return errMsgs
}

// checkAttributeLabelRowAttr TODO
func checkAttributeLabelRowAttr(ctx context.Context, i int, attrKey, attrName string, attrKeyNamePair,
	attrNameKeyPair map[string]*labelEntity.AttrKeyNamePair, total int,
	mapAttrKey, mapAttrName map[string]struct{}) []string {
	errMsgs := make([]string, 0)
	// 检查属性名称长度限制
	attrNameLen := utf8.RuneCountInString(attrName)
	if attrNameLen == 0 {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagNameEmpty))
	}
	if attrNameLen > config.App().AttributeLabel.AttrNameMaxLen {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagNameCharLengthExceed,
			i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, config.App().AttributeLabel.AttrNameMaxLen)))
	}
	if _, ok := mapAttrKey[attrKey]; ok {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagIdDuplicateWithAdmin))
	}
	if _, ok := mapAttrName[attrName]; attrNameLen > 0 && ok {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagNameDuplicateWithAdmin))
	}
	keyPair, attrKeyExisted := attrKeyNamePair[attrKey]
	if attrKeyExisted && keyPair.AttrName != attrName {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagIdSameButNameDifferentWithLine, keyPair.Row))
	}
	namePair, attrNameExisted := attrNameKeyPair[attrName]
	if attrNameExisted && namePair.AttrKey != attrKey {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagNameSameButIdDifferentWithLine, namePair.Row))
	}
	if attrNameLen == 0 {
		return errMsgs
	}
	pair := &labelEntity.AttrKeyNamePair{
		AttrKey:  attrKey,
		AttrName: attrName,
		Row:      i + 1,
	}
	if !attrKeyExisted {
		attrKeyNamePair[attrKey] = pair
	}
	if !attrNameExisted {
		attrNameKeyPair[attrName] = pair
	}
	if total+len(attrKeyNamePair) > config.App().AttributeLabel.AttrLimit {
		errMsgs = append(errMsgs,
			i18n.Translate(ctx, i18nkey.KeyTagLimitExceedPerBot, config.App().AttributeLabel.AttrLimit))
	}
	return errMsgs
}

// generateAttrKey 生成 AttrKey
func generateAttrKey(content string) (string, error) {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	codeStr := str + "_"
	h := sha1.New()
	h.Write([]byte(content))
	b := h.Sum(nil)
	base64Code := base64.StdEncoding.EncodeToString(b)
	var shortCode string
	for _, v := range base64Code {
		if len(shortCode) == 0 {
			if strings.Contains(str, string(v)) {
				shortCode += string(v)
			}
			continue
		} else if strings.Contains(codeStr, string(v)) {
			shortCode += string(v)
		}
		if len(shortCode) == 8 {
			break
		}
	}
	if len(shortCode) == 0 {
		return generateAttrKey(content + base64Code)
	}
	if len(shortCode) < 8 {
		shortCode = shortCode + strings.Repeat("_", 8-len(shortCode))
	}
	// 校验属性标识
	if shortCode == config.App().AttributeLabel.GeneralVectorAttrKey {
		return generateAttrKey(content + base64Code)
	}
	if _, ok := config.App().AttributeLabel.GeneralVectorAttrKeyMap[shortCode]; ok {
		return generateAttrKey(content + base64Code)
	}
	attrKeyRegx := regexp.MustCompile(config.App().AttributeLabel.AttrKeyRegx)
	if !attrKeyRegx.MatchString(shortCode) {
		return generateAttrKey(content + base64Code)
	}
	return shortCode, nil
}
