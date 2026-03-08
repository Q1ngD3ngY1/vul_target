package service

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

var appealType2AuditType = map[uint32]uint32{
	model.AppealBizTypeRobotName:    model.AuditBizTypeRobotName,
	model.AppealBizTypeBareAnswer:   model.AuditBizTypeBareAnswer,
	model.AppealBizTypeDoc:          model.AuditBizTypeDoc,
	model.AppealBizTypeRelease:      model.AuditBizTypeRelease,
	model.AppealBizTypeRobotProfile: model.AuditBizTypeRobotProfile,
	model.AppealBizTypeQa:           model.AuditBizTypeQa,
}

// CreateAppeal 提交申诉申请
func (s *Service) CreateAppeal(ctx context.Context, req *pb.CreateAppealReq) (*pb.CreateAppealRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CreateAppealRsp)
	return rsp, nil
}

func (s *Service) getAppealParams(ctx context.Context, req *pb.CreateAppealReq, robot *model.App) (uint64,
	uint64, *model.Doc, *model.DocQA, error) {
	appealType := req.GetAppealType()
	version := req.GetVersionId()
	// 云Api 3.0逻辑兼容老接口，上面的version和qaID需要删除if后新赋值
	if req.GetReleaseBizId() > 0 {
		release, err := s.dao.GetReleaseByBizID(ctx, req.GetReleaseBizId())
		if err != nil {
			return 0, 0, nil, nil, errs.ErrSystem
		}
		if release != nil {
			version = release.ID
		}
	}
	if appealType == model.AppealBizTypeQa && req.GetQaBizId() == 0 {
		return 0, 0, nil, nil, errs.ErrAppealTypeNotMatch
	}
	if appealType == model.AppealBizTypeDoc && len(req.GetDocBizId()) == 0 {
		return 0, 0, nil, nil, errs.ErrAppealTypeNotMatch
	}
	qaID := req.GetQaId()
	var qa *model.DocQA
	var err error
	if req.GetQaBizId() > 0 {
		qa, err = s.dao.GetQAByBizID(ctx, req.GetQaBizId())
		if err != nil {
			return 0, 0, nil, nil, errs.ErrSystem
		}
		if qa == nil || qa.RobotID != robot.ID { // QA不存在报错
			return 0, 0, nil, nil, errs.ErrParams
		}
		qaID = qa.ID
	}

	var docBizID uint64
	var doc *model.Doc
	if appealType == model.AppealBizTypeDoc {
		if docBizID, err = util.CheckReqParamsIsUint64(ctx, req.GetDocBizId()); err != nil {
			return 0, 0, nil, nil, errs.ErrAppealTypeNotMatch
		}
		if docBizID == 0 {
			return 0, 0, nil, nil, errs.ErrAppealTypeNotMatch
		}
		doc, err = s.dao.GetDocByBizID(ctx, docBizID, robot.ID)
		if err != nil {
			return 0, 0, nil, nil, errs.ErrSystem
		}
		if doc == nil || doc.RobotID != robot.ID { // doc不存在报错
			return 0, 0, nil, nil, errs.ErrParams
		}
	}
	if req.GetQaBizId() > 0 {
		return version, qaID, nil, qa, nil
	}
	if len(req.GetDocBizId()) > 0 {
		return version, qaID, doc, nil, nil
	}
	return version, qaID, nil, nil, nil
}

func (s *Service) generateAppealLockKey(appealType uint32, qaID, docID, version uint64, robot *model.App) string {
	var lockStr string
	if appealType == model.AppealBizTypeRelease {
		lockStr = fmt.Sprintf("%d:%d", robot.ID, version)
	}
	if appealType == model.AppealBizTypeRobotProfile || appealType == model.AppealBizTypeBareAnswer {
		lockStr = fmt.Sprintf("%d:%d", robot.ID, appealType)
	}
	if appealType == model.AppealBizTypeQa {
		lockStr = fmt.Sprintf("%d:%d:%d", robot.ID, appealType, qaID)
	}
	if appealType == model.AppealBizTypeDoc {
		lockStr = fmt.Sprintf("%d:%d:%d", robot.ID, appealType, docID)
	}
	return fmt.Sprintf(dao.LockForCreateAppeal, lockStr)
}

func (s *Service) getAuditFailLists(ctx context.Context, auditType uint32, qaID, corpID, robotID,
	version uint64) ([]*model.AuditFailList, map[uint64]*model.DocQA, []*model.ReleaseConfig, error) {
	if auditType == model.AuditBizTypeRelease && qaID != 0 {
		qa, err := s.dao.GetQAByID(ctx, qaID)
		if err != nil {
			return nil, nil, nil, errs.ErrSystem
		}
		if qa.ReleaseStatus != model.QAReleaseStatusAuditNotPass {
			return nil, nil, nil, errs.ErrAppealNotFound
		}
		docQAs := make(map[uint64]*model.DocQA, 0)
		docQAs[qa.ID] = qa
		auditFailList, err := s.getAuditFailQaIDs(ctx, corpID, robotID, qaID, auditType)
		if err != nil {
			return nil, nil, nil, errs.ErrAppealNotFound
		}
		return auditFailList, docQAs, nil, nil
	}
	if auditType == model.AuditBizTypeRelease && version != 0 {
		auditFailList, docQAs, err := s.getAuditQAFailByVersion(ctx, corpID, robotID, version)
		if err != nil {
			return nil, nil, nil, err
		}
		failLists, configs, err := s.getAuditConfigFailByVersion(ctx, corpID, robotID, version)
		if err != nil {
			return nil, nil, configs, err
		}
		auditFailList = append(auditFailList, failLists...)
		return auditFailList, docQAs, configs, nil
	}
	if auditType == model.AuditBizTypeRobotProfile || auditType == model.AuditBizTypeBareAnswer {
		auditFailList, err := s.dao.GetBizAuditFailList(ctx, corpID, robotID, auditType)
		if err != nil {
			return nil, nil, nil, errs.ErrAppealNotFound
		}
		return auditFailList, nil, nil, nil
	}
	log.ErrorContextf(ctx, "提交申诉申请参数错误 corpID:%d, robotID:%d, auditType:%d, qaID:%d, "+
		"version:%d", corpID, robotID, auditType, qaID, version)
	return nil, nil, nil, errs.ErrParams
}

func (s *Service) getDocOrQaAuditFailLists(ctx context.Context, auditType uint32, corpID, robotID uint64,
	doc *model.Doc, qa *model.DocQA) ([]*model.AuditFailList, error) {
	log.InfoContextf(ctx, "getDocOrQaAuditFailLists, corpID:%d, robotID:%d, auditType:%d, doc:%+v, qa:%+v",
		corpID, robotID, auditType, doc, qa)
	if auditType == model.AuditBizTypeQa {
		if qa == nil {
			log.ErrorContextf(ctx, "qa == nil, corpID:%d, robotID:%d, auditType:%d, qa:%+v",
				corpID, robotID, auditType, qa)
			return nil, errs.ErrParams
		}
		if qa.ReleaseStatus != model.QAReleaseStatusAuditNotPass {
			log.ErrorContextf(ctx, "非审核不通过状态, corpID:%d, robotID:%d, auditType:%d, qa:%+v",
				corpID, robotID, auditType, qa)
			return nil, errs.ErrParams
		}
		auditFailList, err := s.dao.GetLatestAuditFailListByRelateID(ctx, corpID, robotID, qa.ID,
			model.AuditBizTypeQa, true)
		if err == errs.ErrAppealNotFound {
			log.ErrorContextf(ctx, "未找到失败的qa列表, corpID:%d, robotID:%d, auditType:%d, qa:%+v",
				corpID, robotID, auditType, qa)
			return nil, err
		}
		if err != nil {
			log.ErrorContextf(ctx, "GetBizAuditFailListByRelateIDs fail err:%v, corpID:%d, "+
				"robotID:%d, auditType:%d, qa:%+v", err, corpID, robotID, auditType, qa)
			return nil, errs.ErrSystem
		}
		return auditFailList, nil
	}
	if auditType == model.AuditBizTypeDoc {
		if doc.Status != model.DocStatusAuditFail {
			log.ErrorContextf(ctx, "非审核不通过状态, corpID:%d, robotID:%d, auditType:%d, doc:%+v",
				corpID, robotID, auditType, doc)
			return nil, errs.ErrParams
		}
		auditFailList, err := s.dao.GetLatestAuditFailListByRelateID(ctx, corpID, robotID, doc.ID,
			model.AuditBizTypeDoc, true)
		if err != nil {
			log.ErrorContextf(ctx, "GetBizAuditFailListByRelateIDs fail err:%v, corpID:%d, "+
				"robotID:%d, auditType:%d, doc:%+v", err, corpID, robotID, auditType, doc)
			return nil, errs.ErrAppealNotFound
		}
		return auditFailList, nil
	}
	log.ErrorContextf(ctx, "auditType error, corpID:%d, robotID:%d, auditType:%d, doc:%+v, qa:%+v",
		corpID, robotID, auditType, doc, qa)
	return nil, errs.ErrParams
}

func (s *Service) updateAppealStatus(ctx context.Context, appealType uint32, auditList []*model.Audit,
	docQAs map[uint64]*model.DocQA, robot *model.App, configs []*model.ReleaseConfig) error {
	var err error
	if appealType == model.AppealBizTypeRelease {
		if err = s.appealRelease(ctx, docQAs, robot.ID, configs); err != nil {
			return errs.ErrSystem
		}
		return nil
	}
	err = s.dao.UpdateAuditListStatus(ctx, auditList)
	if err != nil {
		return errs.ErrSystem
	}
	return nil
}

func (s *Service) getAuditAppealList(auditFailList []*model.AuditFailList, corp *model.Corp, robotID uint64,
	appealType uint32, reason string, staff *model.CorpStaff) ([]*model.Appeal, []*model.Audit, *model.Audit) {
	appealList := make([]*model.Appeal, 0)
	auditList := make([]*model.Audit, 0)
	parentAudit := &model.Audit{
		CorpID:        corp.ID,
		RobotID:       robotID,
		CreateStaffID: staff.ID,
	}
	for _, v := range auditFailList {
		appealList = append(appealList, &model.Appeal{
			CorpID:         corp.ID,
			CorpFullName:   corp.FullName,
			RobotID:        robotID,
			CreateStaffID:  staff.ID,
			AuditParentID:  v.ParentID,
			AuditID:        v.ID,
			AppealParentID: 0,
			Type:           appealType,
			Params:         v.Params,
			RelateID:       v.RelateID,
			Status:         model.AppealIng,
			InKeywordList:  0,
			Result:         "",
			Reason:         reason,
			Operator:       staff.NickName,
			CreateTime:     time.Now(),
			UpdateTime:     time.Now(),
		})
		if parentAudit.ID == 0 {
			parentAudit.ID = v.ParentID
		}
		if len(auditList) == 0 {
			auditList = append(auditList, &model.Audit{
				ID:         v.ParentID,
				Status:     model.AuditStatusAppealIng,
				UpdateTime: time.Now(),
			})
		}
		auditList = append(auditList, &model.Audit{
			ID:         v.ID,
			Status:     model.AuditStatusAppealIng,
			UpdateTime: time.Now(),
		})
	}
	return appealList, auditList, parentAudit
}

func (s *Service) getAuditQAFailByVersion(ctx context.Context, corpID, robotID, version uint64) (
	[]*model.AuditFailList, map[uint64]*model.DocQA, error) {
	modifyQas, err := s.dao.GetAuditQAFailByVersion(ctx, corpID, robotID, version)
	if err != nil {
		return nil, nil, errs.ErrAppealNotFound
	}
	qaIDs := make([]uint64, 0, len(modifyQas))
	for _, v := range modifyQas {
		qaIDs = append(qaIDs, v.QaID)
	}
	docQAs, err := s.dao.GetQADetailsByReleaseStatus(ctx, corpID, robotID, qaIDs,
		model.QAReleaseStatusAuditNotPass)
	if err != nil {
		return nil, docQAs, errs.ErrAppealNotFound
	}
	auditIDs := make([]uint64, 0, len(modifyQas))
	for _, v := range modifyQas {
		if _, ok := docQAs[v.QaID]; !ok {
			continue
		}
		auditIDs = append(auditIDs, v.ID)
	}
	auditFailList, err := s.dao.GetBizAuditFailListByRelateIDs(ctx, corpID, robotID, model.AuditBizTypeRelease,
		auditIDs)
	if err != nil {
		log.DebugContextf(ctx, "提交申诉申请 corpID:%d, robotID:%d, auditIDs:%+v, auditFailLists：%+v, err:%+v,"+
			" version:%d", corpID, robotID, auditIDs, auditFailList, err, version)
		return nil, docQAs, errs.ErrAppealNotFound
	}
	return auditFailList, docQAs, nil
}

func (s *Service) getAuditConfigFailByVersion(ctx context.Context, corpID, robotID, version uint64) (
	[]*model.AuditFailList, []*model.ReleaseConfig, error) {
	configs, err := s.dao.GetConfigItemByVersionID(ctx, version)
	if err != nil {
		return nil, nil, errs.ErrAppealNotFound
	}
	auditFailID := make([]uint64, 0, len(configs))
	appealTypeMap := make(map[uint64]uint32, 0)
	for _, v := range configs {
		if v.AuditStatus == model.ConfigReleaseStatusAppealIng {
			return nil, nil, errs.ErrAppealNotFound
		}
		if v.ConfigItem == model.ConfigItemBareAnswer {
			appealTypeMap[v.ID] = model.AuditBizTypeBareAnswer
		}
		auditFailID = append(auditFailID, v.ID)
		appealTypeMap[v.ID] = model.AuditBizTypeRobotProfile
	}
	auditFailList, err := s.dao.GetBizAuditFailListByRelateIDs(ctx, corpID, robotID, model.AuditBizTypeRelease,
		auditFailID)
	if err != nil {
		log.DebugContextf(ctx, "提交申诉申请 corpID:%d, robotID:%d, auditIDs:%+v, auditFailLists：%+v, err:%+v,"+
			" version:%d", corpID, robotID, auditFailID, auditFailList, err, version)
		return nil, nil, errs.ErrAppealNotFound
	}
	if len(appealTypeMap) > 0 {
		for _, v := range auditFailList {
			v.Type = appealTypeMap[v.RelateID]
		}
	}
	return auditFailList, configs, nil
}

func (s *Service) getAuditFailQaIDs(ctx context.Context, corpID, robotID, qaID uint64,
	appealType uint32) ([]*model.AuditFailList, error) {
	releaseQA, err := s.dao.GetAuditQAFailByQaID(ctx, corpID, robotID, qaID)
	if err != nil {
		return nil, err
	}
	relateIDs := make([]uint64, 0, 1)
	qaIDs := make([]uint64, 0, 1)
	for _, v := range releaseQA {
		if v.AuditStatus == model.ReleaseQAAuditStatusFail {
			relateIDs = append(relateIDs, v.ID)
			qaIDs = append(qaIDs, v.QaID)
		}
	}
	if len(relateIDs) == 0 {
		log.DebugContextf(ctx, "提交申诉申请 relateIDs 为空 appealType：%d, releaseQA:%+v, relateIDs:%+v, qaIDs:%+v",
			appealType, releaseQA, relateIDs, qaIDs)
		return nil, errs.ErrAppealNotFound
	}
	auditFailList, err := s.dao.GetBizAuditFailListByRelateIDs(ctx, corpID, robotID, model.AuditBizTypeRelease,
		relateIDs)
	if err != nil {
		log.DebugContextf(ctx, "提交申诉申请 corpID:%d, robotID:%d, auditIDs:%+v, qaIDs:%+v, lists：%+v,"+
			" err:%+v", corpID, robotID, relateIDs, qaIDs, auditFailList, err)
		return auditFailList, errs.ErrAppealNotFound
	}

	return auditFailList, nil
}

func (s *Service) appealRelease(ctx context.Context, docQAs map[uint64]*model.DocQA,
	robotID uint64, configs []*model.ReleaseConfig) error {
	var release model.AppDetailsConfig
	var preview model.AppDetailsConfig
	robot, err := s.dao.GetAppByID(ctx, robotID)
	if err != nil {
		return err
	}
	for _, qaDetail := range docQAs {
		qaDetail.ReleaseStatus = model.QAReleaseStatusAppealIng
		qaDetail.IsAuditFree = model.QAIsAuditNotFree
	}
	if len(docQAs) > 0 {
		err = s.dao.UpdateAppealQA(ctx, docQAs)
		if err != nil {
			return errs.ErrSystem
		}
	}
	if err = jsoniter.Unmarshal([]byte(robot.ReleaseJSON), &release); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
	}
	if err = jsoniter.Unmarshal([]byte(robot.PreviewJSON), &preview); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
	}
	for _, v := range configs {
		if v.AuditStatus == model.ConfigReleaseStatusAuditNotPass {
			v.AuditStatus = model.ConfigReleaseStatusAppealIng
			if err := s.dao.UpdateAuditConfigItem(ctx, v); err != nil {
				return errs.ErrSystem
			}
			updateAppBeforeAppeal(&preview, &release, v, robot)
		}
	}
	previewJSON, err := jsoniter.Marshal(preview)
	if err != nil {
		return err
	}
	robot.PreviewJSON = string(previewJSON)
	if err = s.dao.ModifyAppJSON(ctx, robot); err != nil {
		return err
	}
	return nil
}

func updateAppBeforeAppeal(preview, release *model.AppDetailsConfig, configItem *model.ReleaseConfig,
	robot *model.AppDB) {
	switch configItem.ConfigItem {
	case model.ConfigItemBareAnswer:
		if release.AppConfig.KnowledgeQaConfig != nil {
			preview.AppConfig.KnowledgeQaConfig.BareAnswer = release.AppConfig.KnowledgeQaConfig.BareAnswer
		} else {
			preview.AppConfig.KnowledgeQaConfig.BareAnswer = ""
		}
	case model.ConfigItemName:
		if len(release.BaseConfig.Name) == 0 {
			return
		}
		preview.BaseConfig.Name = release.BaseConfig.Name
		robot.Name = release.BaseConfig.Name
	case model.ConfigItemAvatar:
		if len(release.BaseConfig.Avatar) == 0 {
			return
		}
		preview.BaseConfig.Avatar = release.BaseConfig.Avatar
		robot.Avatar = release.BaseConfig.Avatar
	case model.ConfigItemGreeting:
		if release.AppConfig.KnowledgeQaConfig != nil {
			preview.AppConfig.KnowledgeQaConfig.Greeting = release.AppConfig.KnowledgeQaConfig.Greeting
		} else {
			preview.AppConfig.KnowledgeQaConfig.Greeting = ""
		}
	case model.ConfigItemRoleDescription:
		if release.AppConfig.KnowledgeQaConfig != nil {
			preview.AppConfig.KnowledgeQaConfig.RoleDescription = release.AppConfig.KnowledgeQaConfig.RoleDescription
		} else {
			preview.AppConfig.KnowledgeQaConfig.RoleDescription = ""
		}
	}
}
