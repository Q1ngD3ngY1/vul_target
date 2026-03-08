package service

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicAttribute "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/attribute"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// CreateAttributeLabel 创建属性标签
func (s *Service) CreateAttributeLabel(ctx context.Context, req *pb.CreateAttributeLabelReq) (
	*pb.CreateAttributeLabelRsp, error) {
	log.InfoContextf(ctx, "CreateAttributeLabel Req:%+v", req)
	rsp := new(pb.CreateAttributeLabelRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	req.AttrKey, _ = generateAttrKey(req.GetAttrName())
	if err := s.checkCreateAttributeLabel(ctx, req, app); err != nil {
		return rsp, err
	}
	attrLabel, err := s.fillAttributeLabel(ctx, req, app.ToDB())
	if err != nil {
		return rsp, err
	}
	if err := s.dao.BatchCreateAttribute(ctx, []*model.AttributeLabelItem{attrLabel}); err != nil {
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
	_ = s.dao.AddOperationLog(ctx, model.AttributeLabelAdd, app.CorpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// checkCreateAttributeLabel TODO
func (s *Service) checkCreateAttributeLabel(ctx context.Context, req *pb.CreateAttributeLabelReq,
	app *model.App) error {
	// 校验属性标识和名称的有效性
	if err := checkAttributeLabelAttrInvalid(ctx, req.GetAttrKey(), req.GetAttrName()); err != nil {
		return err
	}
	// 检查标签有效性
	if err := checkAttributeLabelInvalid(ctx, app, req.GetLabels()); err != nil {
		return err
	}
	// 检查属性标识是否存在
	if err := s.checkAttributeKeyNamesExist(ctx, app.ID, req.GetAttrKey(), req.GetAttrName(), 0); err != nil {
		return err
	}
	// 检查属性数量限制
	uin := pkg.Uin(ctx)
	if !utilConfig.IsInWhiteList(uin, app.BusinessID, utilConfig.GetWhitelistConfig().InfinityAttributeLabel) {
		count, err := s.dao.GetAttributeTotal(ctx, app.ID, "", nil)
		if err != nil {
			return err
		}
		if count >= uint64(config.App().AttributeLabel.AttrLimit) {
			return errs.ErrAttributeLabelAttrLimit
		}
	}
	return nil
}

// checkAttributeKeyNamesExist TODO
func (s *Service) checkAttributeKeyNamesExist(ctx context.Context, robotID uint64, attrKey, attrName string,
	attrID uint64) error {
	// 检查属性标识是否存在
	if attrKey != "" {
		mapAttrKeyInfo, err := s.dao.GetAttributeByKeys(ctx, robotID, []string{attrKey})
		if err != nil {
			return err
		}
		if attr, ok := mapAttrKeyInfo[attrKey]; ok && attr.ID != attrID {
			return errs.ErrAttributeLabelAttrKeyExist
		}
	}
	// 检查属性名称是否存在
	mapAttrName2Info, err := s.dao.GetAttributeByNames(ctx, robotID, []string{attrName})
	if err != nil {
		return err
	}
	if attr, ok := mapAttrName2Info[attrName]; ok && attr.ID != attrID {
		return errs.ErrAttributeLabelAttrNameExist
	}
	return nil
}

// checkAttributeLabelAttrInvalid TODO
func checkAttributeLabelAttrInvalid(ctx context.Context, attrKey, attrName string) error {
	// 检查属性名称字符长度限制
	if utf8.RuneCountInString(strings.TrimSpace(attrName)) == 0 {
		return errs.ErrAttributeLabelEmpty
	}
	if utf8.RuneCountInString(attrName) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		config.App().AttributeLabel.AttrNameMaxLen) {
		return errs.ErrAttributeLabelAttrNameMaxLen
	}
	return nil
}

// checkAttributeLabelInvalid TODO
func checkAttributeLabelInvalid(ctx context.Context, app *model.App, labels []*pb.AttributeLabel) error {
	if len(labels) == 0 {
		return nil
	}
	// 检查属性数量限制
	uin := pkg.Uin(ctx)
	if !utilConfig.GetInfinityAttributeLabel(uin, app.BusinessID) {
		if len(labels) > config.App().AttributeLabel.LabelLimit {
			return errs.ErrAttributeLabelLimit
		}
	}
	mapLabelName := make(map[string]int)
	for _, label := range labels {
		if utf8.RuneCountInString(strings.TrimSpace(label.GetLabelName())) == 0 {
			return errs.ErrAttributeLabelEmpty
		}
		if utf8.RuneCountInString(label.GetLabelName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
			config.App().AttributeLabel.LabelNameMaxLen) {
			return errs.ErrAttributeLabelNameMaxLen
		}
		if len(label.GetSimilarLabels()) > config.App().AttributeLabel.SimilarLabelLimit {
			return errs.ErrAttributeLabelSimilarLimit
		}
		mapLabelName[label.GetLabelName()] += 1
		for _, similarLabel := range label.GetSimilarLabels() {
			if utf8.RuneCountInString(strings.TrimSpace(similarLabel)) == 0 {
				return errs.ErrAttributeLabelEmpty
			}
			if utf8.RuneCountInString(similarLabel) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
				config.App().AttributeLabel.SimilarLabelMaxLen) {
				return errs.ErrAttributeLabelSimilarMaxLen
			}
			mapLabelName[similarLabel] += 1
		}
	}
	for labelName, count := range mapLabelName {
		if labelName == config.App().AttributeLabel.FullLabelValue {
			return errs.ErrAttributeLabelSystem
		}
		if count > 1 {
			return errs.ErrAttributeLabelNameRepeated
		}
	}
	return nil
}

// fillAttributeLabel TODO
func (s *Service) fillAttributeLabel(ctx context.Context, req *pb.CreateAttributeLabelReq, appDB *model.AppDB) (
	*model.AttributeLabelItem, error) {
	attr := &model.Attribute{
		RobotID:       appDB.ID,
		BusinessID:    s.dao.GenerateSeqID(),
		AttrKey:       req.GetAttrKey(),
		Name:          req.GetAttrName(),
		ReleaseStatus: model.AttributeStatusWaitRelease,
		NextAction:    model.AttributeNextActionAdd,
		IsDeleted:     model.AttributeLabelIsNotDeleted,
		DeletedTime:   0,
	}
	labels := make([]*model.AttributeLabel, 0)
	for _, v := range req.GetLabels() {
		similarLabel, err := parseSimilarLabels(v.GetSimilarLabels())
		if err != nil {
			log.DebugContextf(ctx, "parse similar labels err:%v", err)
			return nil, err
		}
		labels = append(labels, &model.AttributeLabel{
			RobotID:       appDB.ID,
			Name:          v.GetLabelName(),
			BusinessID:    s.dao.GenerateSeqID(),
			SimilarLabel:  similarLabel,
			ReleaseStatus: model.AttributeStatusWaitRelease,
			NextAction:    model.AttributeNextActionAdd,
			IsDeleted:     model.AttributeLabelIsNotDeleted,
		})
	}
	return &model.AttributeLabelItem{Attr: attr, Labels: labels}, nil
}

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
	return jsoniter.MarshalToString(similarLabels)
}

// getSimilarLabels TODO
func getSimilarLabels(similarLabelStr string) ([]string, error) {
	if len(similarLabelStr) == 0 {
		return nil, nil
	}
	var similarLabels []string
	err := jsoniter.UnmarshalFromString(similarLabelStr, &similarLabels)
	return similarLabels, err
}

// DeleteAttributeLabel 删除属性标签
func (s *Service) DeleteAttributeLabel(ctx context.Context, req *pb.DeleteAttributeLabelReq) (
	*pb.DeleteAttributeLabelRsp, error) {
	log.InfoContextf(ctx, "DeleteAttributeLabel Req:%+v", req)
	rsp := new(pb.DeleteAttributeLabelRsp)
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
	// 云API 3.0逻辑 后续需要移除if和if之下以外的逻辑
	if len(req.GetAttributeBizIds()) > 0 {
		var ids []uint64
		var attrKeys []string
		attributesBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetAttributeBizIds())
		if err != nil {
			return rsp, err
		}
		mapAttrID2Info, err := s.dao.GetAttributeByBizIDs(ctx, app.ID, attributesBizIDs)
		if err != nil {
			return rsp, err
		}
		if len(mapAttrID2Info) != len(req.GetAttributeBizIds()) {
			return rsp, errs.ErrAttributeLabelNotFound
		}
		for _, info := range mapAttrID2Info {
			if info.ReleaseStatus == model.AttributeStatusReleasing ||
				info.ReleaseStatus == model.AttributeStatusReleaseUpdating {
				return rsp, errs.ErrAttributeLabelUpdating
			}
			ids = append(ids, info.ID)
			attrKeys = append(attrKeys, info.AttrKey)
		}
		if err := s.checkAttributeUsed(ctx, app, attributesBizIDs); err != nil {
			return rsp, err
		}
		if err := s.checkAttributeLabelUsed(ctx, app.ID, model.AttributeLabelSourceKg, ids, nil); err != nil {
			return rsp, err
		}
		if err := s.dao.DeleteAttribute(ctx, app.ID, ids, attrKeys); err != nil {
			return rsp, err
		}
		// feature_permission
		//删除属性标签需要同步删除角色标签绑定关系 异步删除，不要影响原功能
		go dao.GetRoleDao(nil).BatchDeleteRoleAttribute(trpc.CloneContext(ctx), botBizID, attributesBizIDs)
		_ = s.dao.AddOperationLog(ctx, model.AttributeLabelDelete, app.CorpID, app.ID, req, rsp, nil, nil)
		return rsp, nil
	}
	reqIDs, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return nil, err
	}
	mapAttrID2Info, err := s.dao.GetAttributeByIDs(ctx, app.ID, reqIDs)
	if err != nil {
		return rsp, err
	}
	if len(mapAttrID2Info) != len(req.GetIds()) {
		return rsp, errs.ErrAttributeLabelNotFound
	}
	var attrKeys []string
	var attributesBizIDs []uint64
	for _, info := range mapAttrID2Info {
		if info.ReleaseStatus == model.AttributeStatusReleasing ||
			info.ReleaseStatus == model.AttributeStatusReleaseUpdating {
			return rsp, errs.ErrAttributeLabelUpdating
		}
		attrKeys = append(attrKeys, info.AttrKey)
		attributesBizIDs = append(attributesBizIDs, info.BusinessID)
	}
	if err := s.checkAttributeUsed(ctx, app, attributesBizIDs); err != nil {
		return rsp, err
	}
	if err := s.checkAttributeLabelUsed(ctx, app.ID, model.AttributeLabelSourceKg, reqIDs, nil); err != nil {
		return rsp, err
	}

	if err := s.dao.DeleteAttribute(ctx, app.ID, reqIDs, attrKeys); err != nil {
		return rsp, err
	}
	// feature_permission
	//删除属性标签需要同步删除角色标签绑定关系
	go dao.GetRoleDao(nil).BatchDeleteRoleAttribute(trpc.CloneContext(ctx), botBizID, attributesBizIDs)

	_ = s.dao.AddOperationLog(ctx, model.AttributeLabelDelete, app.CorpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

func (s *Service) isAttributeLabelUsed(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64) (bool, error) {
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return false, nil
	}
	var docAttributeLabelCount, qaAttributeLabelCount uint64
	var docAttributeLabelErr, qaAttributeLabelErr error
	g := errgroupx.Group{}
	g.SetLimit(10)
	g.Go(func() error {
		docAttributeLabelCount, docAttributeLabelErr = s.dao.GetDocAttributeLabelCountByAttrLabelIDs(ctx,
			robotID, source, attrIDs, labelIDs)
		if docAttributeLabelErr != nil {
			return docAttributeLabelErr
		}
		return nil
	})
	g.Go(func() error {
		qaAttributeLabelCount, qaAttributeLabelErr = s.dao.GetQAAttributeLabelCountByAttrLabelIDs(ctx,
			robotID, source, attrIDs, labelIDs)
		if qaAttributeLabelErr != nil {
			return qaAttributeLabelErr
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		log.WarnContextf(ctx, "checkAttributeLabelUsed robotID:%d,source:%d,attrIDs:%+v,labelIDs:%+v err :%v",
			robotID, source, attrIDs, labelIDs, err)
		return false, err
	}
	if docAttributeLabelCount > 0 || qaAttributeLabelCount > 0 {
		return true, nil
	}
	return false, nil
}

// checkAttributeUsed 检查属性标签是否已被使用
func (s *Service) checkAttributeUsed(ctx context.Context, app *model.App, attributesBizIDs []uint64) error {
	if app.PreviewDetails.AppConfig.KnowledgeQaConfig == nil ||
		len(app.PreviewDetails.AppConfig.KnowledgeQaConfig.SearchRange.ApiVarAttrInfos) == 0 {
		return nil
	}
	apiVarAttrInfoMap := make(map[uint64]struct{})
	for _, apiVarAttrInfo := range app.PreviewDetails.AppConfig.KnowledgeQaConfig.SearchRange.ApiVarAttrInfos {
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

// UpdateAttributeLabel 编辑属性标签
func (s *Service) UpdateAttributeLabel(ctx context.Context, req *pb.UpdateAttributeLabelReq) (
	*pb.UpdateAttributeLabelRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.UpdateAttributeLabelRsp{}
	return rsp, nil
}

// ModifyAttributeLabel 编辑属性标签
func (s *Service) ModifyAttributeLabel(ctx context.Context, req *pb.ModifyAttributeLabelReq) (
	*pb.ModifyAttributeLabelRsp, error) {
	log.InfoContextf(ctx, "ModifyAttributeLabel Req:%+v", req)
	rsp, err := logicAttribute.ModifyAttributeLabel(ctx, s.dao, req)
	return rsp, err
}

// GetAttributeLabelList 查询属性标签列表
func (s *Service) GetAttributeLabelList(ctx context.Context, req *pb.GetAttributeLabelListReq) (
	*pb.GetAttributeLabelListRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetAttributeLabelListRsp)
	return rsp, nil
}

// ListAttributeLabel 查询属性标签列表
func (s *Service) ListAttributeLabel(ctx context.Context, req *pb.ListAttributeLabelReq) (
	*pb.ListAttributeLabelRsp, error) {
	log.InfoContextf(ctx, "ListAttributeLabel req: %+v", req)
	rsp := new(pb.ListAttributeLabelRsp)
	rsp.List = make([]*pb.ListAttributeLabelRsp_AttrLabelDetail, 0)
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if req.GetPageSize() > 2000 {
		return rsp, errs.ErrParamsNotExpected
	}

	var list []*model.Attribute
	if config.App().EsSearch.AttributeEnableEs && req.GetQuery() != "" {
		ids, total, err := logicAttribute.GetAttrIDBySearchLabelsWithEs(ctx, app.ID, req)
		if err != nil {
			return rsp, err
		}

		rsp.Total = uint64(total)
		list, err = s.dao.GetAttributeListByIDs(ctx, app.ID, ids)
		if err != nil {
			return rsp, err
		}
	} else {
		total, err := s.dao.GetAttributeTotal(ctx, app.ID, req.GetQuery(), nil)
		log.DebugContextf(ctx, "total:%+v", total)
		if err != nil {
			return rsp, err
		}
		rsp.Total = total
		if rsp.GetTotal() == 0 {
			return rsp, nil
		}
		list, err = s.dao.GetAttributeList(ctx, app.ID, req.GetQuery(), req.GetPageNumber(), req.GetPageSize(), nil)
		if err != nil {
			return rsp, err
		}
	}

	mapAttrID2Labels := make(map[uint64][]*model.AttributeLabel)
	mapAttrID2LabelTotal := make(map[uint64]uint64)
	for _, v := range list {
		labelTotal, err := s.dao.GetAttributeLabelCount(ctx, v.ID, req.GetQuery(), "", app.ID)
		if err != nil {
			return rsp, err
		}
		mapAttrID2LabelTotal[v.ID] = labelTotal
		attrLabels, err := s.dao.GetAttributeLabelList(ctx, v.ID, "", "", 0, req.GetLabelSize(), app.ID)
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

// GetAttributeLabelDetail 查询属性标签详情
func (s *Service) GetAttributeLabelDetail(ctx context.Context, req *pb.GetAttributeLabelDetailReq) (
	*pb.GetAttributeLabelDetailRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetAttributeLabelDetailRsp)
	return rsp, nil
}

// DescribeAttributeLabel 查询属性标签详情
func (s *Service) DescribeAttributeLabel(ctx context.Context, req *pb.DescribeAttributeLabelReq) (
	*pb.DescribeAttributeLabelRsp, error) {
	log.InfoContextf(ctx, "DescribeAttributeLabel Req:%+v", req)
	rsp := new(pb.DescribeAttributeLabelRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	attributeBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetAttributeBizId())
	if err != nil {
		return nil, err
	}
	mapAttrID2Info, err := s.dao.GetAttributeByBizIDs(ctx, app.ID, []uint64{attributeBizId})
	if err != nil {
		return rsp, err
	}
	attr, ok := mapAttrID2Info[attributeBizId]
	if !ok {
		return rsp, errs.ErrAttributeLabelNotFound
	}
	total, err := s.dao.GetAttributeLabelCount(ctx, attr.ID, req.GetQuery(), req.GetQueryScope(), app.ID)
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
		labels, err := s.dao.GetAttributeLabelByBizIDs(ctx, []uint64{lastLabelBizID}, app.ID)
		if err != nil {
			return rsp, errs.ErrSystem
		}
		label, ok := labels[lastLabelBizID]
		if !ok {
			return rsp, errs.ErrAttributeLabelNotFound
		}
		labelID = label.ID
	}
	var list []*model.AttributeLabel
	if config.App().EsSearch.AttributeEnableEs && req.GetQuery() != "" {
		ids, err := logicAttribute.QueryAttributeLabelCursor(ctx, attr.ID, req.GetQuery(),
			req.GetQueryScope(), labelID, req.GetLimit(), app.ID)
		if err != nil {
			return rsp, err
		}
		list, err = s.dao.GetAttributeLabelByIDOrder(ctx, app.ID, ids)
		if err != nil {
			return rsp, err
		}
	} else {
		list, err = s.dao.GetAttributeLabelList(ctx, attr.ID, req.GetQuery(), req.GetQueryScope(), labelID,
			req.GetLimit(), app.ID)
		if err != nil {
			return rsp, err
		}
	}
	for _, v := range list {
		similarLabels, err := getSimilarLabels(v.SimilarLabel)
		if err != nil {
			log.DebugContextf(ctx, "parse similar labels req:%+v, err:%+v", req, err)
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

// ExportAttributeLabel 导出属性标签
func (s *Service) ExportAttributeLabel(ctx context.Context, req *pb.ExportAttributeLabelReq) (
	*pb.ExportAttributeLabelRsp, error) {
	rsp := new(pb.ExportAttributeLabelRsp)
	staffID := pkg.StaffID(ctx)
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	_, err = util.CheckReqSliceUint64(ctx, req.GetAttributeBizIds())
	if err != nil {
		return rsp, err
	}
	paramStr, err := jsoniter.MarshalToString(req)
	if err != nil {
		log.DebugContextf(ctx, "json marshl to string req:%+v, err:%+v", req, err)
		return rsp, err
	}
	now := time.Now()
	export := model.Export{
		CorpID:        app.CorpID,
		RobotID:       app.ID,
		CreateStaffID: staffID,
		TaskType:      model.ExportAttributeLabelTaskType,
		Name:          model.ExportAttributeLabelTaskName,
		Params:        paramStr,
		Status:        model.TaskExportStatusInit,
		UpdateTime:    now,
		CreateTime:    now,
	}
	params := model.ExportParams{
		CorpID:           app.CorpID,
		RobotID:          app.ID,
		CreateStaffID:    staffID,
		TaskType:         model.ExportAttributeLabelTaskType,
		TaskName:         model.ExportAttributeLabelTaskName,
		Params:           paramStr,
		NoticeContent:    i18n.Translate(ctx, model.AttributeLabelNoticeContent),
		NoticeContentIng: i18n.Translate(ctx, model.AttributeLabelNoticeContentIng),
		NoticePageID:     model.NoticeAttributeLabelPageID,
		NoticeTypeExport: model.NoticeTypeAttributeLabelExport,
	}
	taskID, err := s.dao.CreateExportTask(ctx, app.CorpID, staffID, app.ID, export, params)
	if err != nil {
		log.ErrorContextf(ctx, "create export task req:%+v, err:%+v", req, err)
		return rsp, err
	}
	rsp.TaskId = taskID
	return rsp, nil
}

// CheckAttributeLabelRefer 检查属性下标签是否引用
func (s *Service) CheckAttributeLabelRefer(ctx context.Context, req *pb.CheckAttributeLabelReferReq) (
	*pb.CheckAttributeLabelReferRsp, error) {
	log.InfoContextf(ctx, "CheckAttributeLabelRefer Req:%+v", req)
	rsp := new(pb.CheckAttributeLabelReferRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
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
		attrs, err := s.dao.GetAttributeByBizIDs(ctx, app.ID, attributeBizIds)
		if err != nil {
			return rsp, errs.ErrSystem
		}
		labelBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetLabelBizId())
		if err != nil {
			return nil, err
		}
		labels, err := s.dao.GetAttributeLabelByBizIDs(ctx, []uint64{labelBizID}, app.ID)
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
		isUsed, err := s.isAttributeLabelUsed(ctx, app.ID, model.AttributeLabelSourceKg, attrIds, labelIDs)
		if err != nil {
			return rsp, err
		}
		rsp.IsRefer = isUsed
		workFlowList, err := logicAttribute.GetWorkflowListByAttributeLabel(ctx, req)
		if err != nil {
			return rsp, err
		}
		log.DebugContextf(ctx, "GetWorkflowListByAttributeLabel|workFlowList:%+v", workFlowList)
		rsp.List = workFlowList
		return rsp, nil
	}
	if len(req.GetIds()) == 0 && req.GetLabelId() == "" {
		log.DebugContextf(ctx, "CheckAttributeLabelRefer params err req:%+v", req)
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
	isUsed, err := s.isAttributeLabelUsed(ctx, app.ID, model.AttributeLabelSourceKg, reqIDs, labelIDs)
	if err != nil {
		return rsp, err
	}
	rsp.IsRefer = isUsed
	workFlowList, err := logicAttribute.GetWorkflowListByAttributeLabel(ctx, req)
	if err != nil {
		return rsp, err
	}
	log.DebugContextf(ctx, "GetWorkflowListByAttributeLabel|workFlowList:%+v", workFlowList)
	rsp.List = workFlowList
	return rsp, nil
}

// CheckAttributeReferWorkFlow 检查标签引用的工作流
func (s *Service) CheckAttributeReferWorkFlow(ctx context.Context, req *pb.CheckAttributeReferWorkFlowReq) (
	*pb.CheckAttributeReferWorkFlowRsp, error) {
	log.InfoContextf(ctx, "CheckAttributeReferWorkFlow Req:%+v", req)
	rsp := new(pb.CheckAttributeReferWorkFlowRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
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
	log.InfoContextf(ctx, "CheckAttributeReferWorkFlow|attributeBizIds:%+v", attributeBizIds)
	workFlowList, err := logicAttribute.GetWorkflowListByAttribute(ctx, req)
	if err != nil {
		return rsp, err
	}
	log.DebugContextf(ctx, "GetWorkflowListByAttribute|workFlowList:%+v", workFlowList)
	rsp.List = workFlowList
	return rsp, nil
}

// CheckAttributeLabelExist 检查属性下的标签名是否存在
func (s *Service) CheckAttributeLabelExist(ctx context.Context, req *pb.CheckAttributeLabelExistReq) (
	*pb.CheckAttributeLabelExistRsp, error) {
	log.InfoContextf(ctx, "CheckAttributeLabelExist|req:%+v", req)
	rsp := new(pb.CheckAttributeLabelExistRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	attributeBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetAttributeBizId())
	if err != nil {
		return nil, err
	}
	attributeMap, err := s.dao.GetAttributeByBizIDs(ctx, app.ID, []uint64{attributeBizId})
	if err != nil || len(attributeMap) != 1 {
		log.ErrorContextf(ctx, "CheckAttributeLabelExist|AttributeBizId:%d, len(attributeMap):%d, err:%+v",
			req.GetAttributeBizId(), len(attributeMap), err)
		return rsp, errs.ErrAttributeLabelNotFound
	}
	attribute, ok := attributeMap[attributeBizId]
	if !ok || attribute == nil {
		log.ErrorContextf(ctx, "CheckAttributeLabelExist|attribute:%+v", attribute)
		return rsp, errs.ErrAttributeLabelNotFound
	}
	labels, err := s.dao.GetAttributeLabelByName(ctx, attribute.ID, req.GetLabelName(), app.ID)
	if err != nil {
		return rsp, err
	}
	lastLabelID := uint64(0)
	if len(req.GetLastLabelBizId()) > 0 {
		lastLabelBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetLastLabelBizId())
		if err != nil {
			return nil, err
		}
		labelMap, err := s.dao.GetAttributeLabelByBizIDs(ctx, []uint64{lastLabelBizId}, app.ID)
		if err != nil || len(labelMap) != 1 {
			log.ErrorContextf(ctx, "CheckAttributeLabelExist|LastLabelBizID:%d, len(labelMap):%d, err:%+v",
				req.GetLastLabelBizId(), len(labelMap), err)
			return rsp, errs.ErrAttributeLabelNotFound
		}
		lastLabel, ok := labelMap[lastLabelBizId]
		if !ok || lastLabel == nil {
			log.ErrorContextf(ctx, "CheckAttributeLabelExist|lastLabel:%+v", lastLabel)
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
		if pkg.StringsIn(similarLabels, req.GetLabelName()) && (lastLabelID == 0 || v.ID < lastLabelID) {
			rsp.IsExist = true
			return rsp, nil
		}
	}
	return rsp, nil
}

// UploadAttributeLabel 导入属性标签
func (s *Service) UploadAttributeLabel(ctx context.Context, req *pb.UploadAttributeLabelReq) (
	*pb.UploadAttributeLabelRsp, error) {
	log.InfoContextf(ctx, "UploadAttributeLabel Req:%+v", req)
	rsp := new(pb.UploadAttributeLabelRsp)
	key := fmt.Sprintf(dao.LockForUplodAttributeLabel, req.GetCosHash())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		log.ErrorContextf(ctx, "UploadAttributeLabel file lock req:%+v,err :%v", req, err)
		return rsp, errs.ErrAttributeLabelUploading
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
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
	corp, err := s.dao.GetCorpByID(ctx, app.CorpID)
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}
	if err = s.dao.CheckURLPrefix(ctx, app.CorpID, corp.BusinessID, app.BusinessID, req.CosUrl); err != nil {
		log.ErrorContextf(ctx, "UploadAttributeLabel|CheckURLPrefix failed, err:%+v", err)
		return rsp, errs.ErrInvalidURL
	}
	rows, errRsp, err := s.checkAttributeLabelXlsx(ctx, app, req)
	if err != nil || errRsp != nil {
		return errRsp, err
	}
	items := s.getAttributeLabelsFromRows(app.ID, rows)
	if err := s.dao.BatchCreateAttribute(ctx, items); err != nil {
		return rsp, err
	}
	_ = s.dao.AddOperationLog(ctx, model.AttributeLabelUpload, app.CorpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// getAttributeLabelsFromRows TODO
func (s *Service) getAttributeLabelsFromRows(robotID uint64, rows [][]string) []*model.AttributeLabelItem {
	items := make([]*model.AttributeLabelItem, 0)
	mapAttrInfo := make(map[string]*model.AttributeLabelItem)
	for _, row := range rows {
		attrKey, attrName, labelName, similarLabels := getAttributeLabelInfoFromRow(row)
		similarLabel, _ := parseSimilarLabels(similarLabels)
		label := &model.AttributeLabel{
			BusinessID:    s.dao.GenerateSeqID(),
			RobotID:       robotID,
			Name:          labelName,
			SimilarLabel:  similarLabel,
			ReleaseStatus: model.AttributeStatusWaitRelease,
			NextAction:    model.AttributeNextActionAdd,
		}
		if item, ok := mapAttrInfo[attrKey]; ok {
			item.Labels = append(item.Labels, label)
			continue
		}
		attr := &model.Attribute{
			BusinessID:    s.dao.GenerateSeqID(),
			RobotID:       robotID,
			AttrKey:       attrKey,
			Name:          attrName,
			ReleaseStatus: model.AttributeStatusWaitRelease,
			NextAction:    model.AttributeNextActionAdd,
		}
		item := &model.AttributeLabelItem{Attr: attr}
		item.Labels = append(item.Labels, label)
		items = append(items, item)
		mapAttrInfo[attrKey] = item
	}
	return items
}

// getAttributeLabelInfoFromRow TODO
func getAttributeLabelInfoFromRow(row []string) (string, string, string, []string) {
	var attrKey, attrName, labelName, similarLabelStr string
	for cellIndex, cell := range row {
		switch cellIndex {
		case model.ExcelTplAttrNameIndex:
			attrName = strings.TrimSpace(cell)
		case model.ExcelTplLabelIndex:
			labelName = strings.TrimSpace(cell)
		case model.ExcelTplSimilarLabelIndex:
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

// checkAttributeLabelXlsx 检查属性标签文件是否符合要求
func (s *Service) checkAttributeLabelXlsx(ctx context.Context, app *model.App, req *pb.UploadAttributeLabelReq) (
	[][]string, *pb.UploadAttributeLabelRsp, error) {
	body, err := s.dao.GetObject(ctx, req.GetCosUrl())
	if err != nil {
		log.ErrorContextf(ctx, "checkSampleXlsx file get file by url err :%v", err)
		return nil, nil, errs.ErrSystem
	}
	fileName := strings.TrimSuffix(req.GetFileName(), ".xlsx")
	if len(fileName) == 0 {
		return nil, nil, errs.ErrInvalidFileName
	}
	mapAttrKey, mapAttrName, err := s.dao.GetAttributeByRobotID(ctx, app.ID)
	if err != nil {
		return nil, nil, err
	}
	attrKeyNamePair := make(map[string]*model.AttrKeyNamePair)
	attrNameKeyPair := make(map[string]*model.AttrKeyNamePair)
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

// checkAttributeLabelRowAttrLabel TODO
func checkAttributeLabelRowAttrLabel(ctx context.Context, app *model.App, i int, attrKey, labelName string, similarLabels []string,
	attrLabelCount, uniqueAttrLabel map[string]int) []string {
	errMsgs := make([]string, 0)
	if utf8.RuneCountInString(attrKey) == 0 {
		return errMsgs
	}
	labelNameLen := utf8.RuneCountInString(labelName)
	if labelNameLen == 0 {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagEmpty))
	}
	if labelNameLen > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		config.App().AttributeLabel.LabelNameMaxLen) {
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
		if similarLabelLen > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
			config.App().AttributeLabel.SimilarLabelMaxLen) {
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
	uin := pkg.Uin(ctx)
	if !utilConfig.GetInfinityAttributeLabel(uin, app.BusinessID) {
		if attrLabelCount[attrKey] > config.App().AttributeLabel.LabelLimit {
			errMsgs = append(errMsgs,
				i18n.Translate(ctx, i18nkey.KeyTagCountUnderAttributeExceedLimit, config.App().AttributeLabel.LabelLimit))
		}
	}
	return errMsgs
}

// fillAttrLabel TODO
func fillAttrLabel(attrKey, labelName string) string {
	return fmt.Sprintf("%s_%s", attrKey, labelName)
}

// checkAttributeLabelRowAttr TODO
func checkAttributeLabelRowAttr(ctx context.Context, i int, attrKey, attrName string, attrKeyNamePair,
	attrNameKeyPair map[string]*model.AttrKeyNamePair, total int,
	mapAttrKey, mapAttrName map[string]struct{}) []string {
	errMsgs := make([]string, 0)
	// 检查属性名称长度限制
	attrNameLen := utf8.RuneCountInString(attrName)
	if attrNameLen == 0 {
		errMsgs = append(errMsgs, i18n.Translate(ctx, i18nkey.KeyTagNameEmpty))
	}
	if attrNameLen > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		config.App().AttributeLabel.AttrNameMaxLen) {
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
	pair := &model.AttrKeyNamePair{
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

// getAttributeLabelRow TODO
func (s *Service) getAttributeLabelRow(ctx context.Context, req *pb.UploadAttributeLabelReq, fileName string,
	body []byte, check util.CheckFunc) ([][]string, *pb.UploadAttributeLabelRsp, error) {
	// 将配置中文件头翻译成ctx中语言
	var checkHead []string
	for _, v := range config.App().AttributeLabel.ExeclHead {
		checkHead = append(checkHead, i18n.Translate(ctx, v))
	}
	log.InfoContextf(ctx, "getAttributeLabelRow checkHead:%v", checkHead)
	rows, bs, err := util.CheckContent(ctx, fileName, config.App().AttributeLabel.MinRow,
		config.App().AttributeLabel.MaxRow, checkHead, body, check)
	if err != nil {
		if err != errs.ErrExcelContent {
			log.WarnContextf(ctx, "checkAttributeLabelXlsx file check excel err :%v", err)
			return nil, nil, err
		}
		key := req.GetCosUrl() + ".check.xlsx"
		if err := s.dao.PutObject(ctx, bs, key); err != nil {
			return nil, nil, errs.ErrSystem
		}
		url, err := s.dao.GetPresignedURL(ctx, key)
		if err != nil {
			log.DebugContextf(ctx, "UploadSampleFile file write excl err :%v", err)
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
	attrRange uint32, refers []*pb.AttrLabelRefer) (map[uint64]*model.Attribute, map[uint64]*model.AttributeLabel, error) {
	if err := checkAttributeLabelReferBasicData(ctx, attrLimit, attrRange, refers); err != nil {
		return nil, nil, err
	}
	return s.checkAttributeLabelReferOfKg(ctx, robotID, refers, attrLabelLimit)
}

// checkAttributeLabelReferBasicData TODO
func checkAttributeLabelReferBasicData(ctx context.Context, attrLimit int, attrRange uint32, refers []*pb.AttrLabelRefer) error {
	switch attrRange {
	case model.AttrRangeAll:
		if len(refers) > 0 {
			return errs.ErrAttributeLabelRefer
		}
		return nil
	case model.AttrRangeCondition:
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
		if !model.IsAttributeLabelReferSource(v.GetSource()) {
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
	refers []*pb.AttrLabelRefer, attrLabelLimit int) (map[uint64]*model.Attribute, map[uint64]*model.AttributeLabel, error) {
	attrBizIDs, labelBizIDs, mapLabelBizID2AttrBizID, err := getAttrLabelReferOfSource(ctx, refers, model.AttributeLabelSourceKg)
	if err != nil {
		return nil, nil, err
	}
	if len(attrBizIDs) == 0 && len(labelBizIDs) == 0 {
		return nil, nil, nil
	}
	mapAttrID2Info, err := s.dao.GetAttributeByBizIDs(ctx, robotID, attrBizIDs)
	if err != nil {
		return nil, nil, err
	}
	mapLabelID2Info, err := s.dao.GetAttributeLabelByBizIDs(ctx, labelBizIDs, robotID)
	if err != nil {
		return nil, nil, err
	}
	if len(mapAttrID2Info) != len(attrBizIDs) || len(mapLabelID2Info) != len(labelBizIDs) {
		return nil, nil, errs.ErrAttributeLabelNotFound
	}
	for labelBizID, label := range mapLabelID2Info {
		attrBizID, ok := mapLabelBizID2AttrBizID[labelBizID]
		if !ok {
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
func (s *Service) isDocAllowedToModify(ctx context.Context, doc model.Doc, app model.App, corpID uint64) error {
	if doc.CorpID != corpID || doc.RobotID != app.ID {
		return errs.ErrPermissionDenied
	}
	if doc.HasDeleted() {
		return errs.ErrDocHasDeleted
	}
	if !doc.IsAllowEdit() {
		return errs.ErrDocNotAllowEdit
	}
	if doc.IsProcessing([]uint64{model.DocProcessingFlagHandlingDocDiffTask}) {
		return errs.ErrDocDiffTaskRunIng
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, []uint64{doc.ID})
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return errs.ErrSystem
	}
	if _, ok := releasingDocIdMap[doc.ID]; ok {
		return errs.ErrDocIsRelease
	}
	return nil
}

func (s *Service) isDocAttributeLabelChange(ctx context.Context, robotID, docID uint64, oldAttrRange,
	attrRange uint32, refers []*pb.AttrLabelRefer) (bool, error) {
	if oldAttrRange != attrRange {
		return true, nil
	}
	oldRefers, err := s.dao.GetDocAttributeLabel(ctx, robotID, []uint64{docID})
	if err != nil {
		return false, err
	}
	mapOldRefer := make(map[string]struct{})
	mapRefer := make(map[string]struct{})
	for _, v := range refers {
		for _, labelID := range v.GetLabelBizIds() {
			mapRefer[fmt.Sprintf("%d_%s_%s", v.GetSource(), v.GetAttributeBizId(), labelID)] = struct{}{}
		}
	}
	var attrIds []uint64
	var labelIds []uint64
	for _, v := range oldRefers {
		attrIds = append(attrIds, v.AttrID)
		labelIds = append(labelIds, v.LabelID)
	}
	attrs, err := s.dao.GetAttributeByIDs(ctx, robotID, attrIds)
	if err != nil {
		return false, err
	}
	labels, err := s.dao.GetAttributeLabelByIDs(ctx, labelIds, robotID)
	if err != nil {
		return false, err
	}
	if len(attrs) <= 0 {
		return false, nil
	}
	for _, v := range oldRefers {
		var labelBusinessID uint64
		if v.LabelID != 0 {
			labelBusinessID = labels[v.LabelID].BusinessID
		}
		mapOldRefer[fmt.Sprintf("%d_%d_%d", v.Source, attrs[v.AttrID].BusinessID,
			labelBusinessID)] = struct{}{}
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
	attrs map[uint64]*model.Attribute,
	labels map[uint64]*model.AttributeLabel,
) (*model.UpdateDocAttributeLabelReq, error) {
	req := &model.UpdateDocAttributeLabelReq{
		IsNeedChange:    isNeedChange,
		AttributeLabels: make([]*model.DocAttributeLabel, 0),
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
			req.AttributeLabels = append(req.AttributeLabels, &model.DocAttributeLabel{
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
	attrs map[uint64]*model.Attribute,
	labels map[uint64]*model.AttributeLabel,
) (*model.UpdateQAAttributeLabelReq, error) {
	req := &model.UpdateQAAttributeLabelReq{
		IsNeedChange:    isNeedChange,
		AttributeLabels: make([]*model.QAAttributeLabel, 0),
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
			req.AttributeLabels = append(req.AttributeLabels, &model.QAAttributeLabel{
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
	attrs map[uint64]*model.Attribute,
	labels map[uint64]*model.AttributeLabel,
) (bool, error) {
	if oldAttrRange != attrRange {
		return true, nil
	}
	oldRefers, err := s.dao.GetQAAttributeLabel(ctx, robotID, []uint64{qaID})
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
func fillPBAttrLabels(attrLabels []*model.AttrLabel) []*pb.AttrLabel {
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
	robotID uint64, qaIDs []uint64, qas []*model.DocQA,
	attrRange uint32,
	refers []*pb.AttrLabelRefer,
	attrs map[uint64]*model.Attribute,
	labels map[uint64]*model.AttributeLabel,
) ([]*model.DocQA, error) {
	result := make([]*model.DocQA, 0, len(qas))
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
	oldRefers, err := s.dao.GetQAAttributeLabel(ctx, robotID, qaIDs)
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
