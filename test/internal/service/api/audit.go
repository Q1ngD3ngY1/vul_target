package api

import (
	"context"
	"fmt"
	logicAudit "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/audit"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/mapx"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	infosec "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"golang.org/x/exp/slices"
)

// AuditResultCallback 审核回调
func (s *Service) AuditResultCallback(ctx context.Context, req *infosec.CheckResultReq) (
	*infosec.CheckResultRsp, error) {
	rsp := new(infosec.CheckResultRsp)
	bizID := cast.ToUint64(req.GetId())
	audit, err := s.dao.GetAuditByBizID(ctx, bizID)
	if err != nil {
		return rsp, err
	}
	if audit == nil {
		return rsp, errs.ErrAuditNotFound
	}
	if err = logicAudit.ResultCallback(ctx, s.dao, audit, req.GetResultCode(), req.GetResultType()); err != nil {
		log.ErrorContextf(ctx, "审核结果回调失败 audit:%+v err:%+v", audit, err)
		return rsp, err
	}

	return rsp, nil
}

// UpdateAuditStatus 更审核表状态
func (s *Service) UpdateAuditStatus(ctx context.Context, req *pb.UpdateAuditStatusReq) (*pb.UpdateAuditStatusRsp,
	error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)

	rsp := new(pb.UpdateAuditStatusRsp)
	isPass := req.GetIsPass()
	auditType := req.GetAuditType()
	appealID := req.AppealId
	appealTaskDone := req.AppealTaskDone
	audit := s.getAudit(req)

	key := fmt.Sprintf(dao.LockForAuditAppeal, appealID)
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		return nil, errs.ErrAuditAppealIng
	}

	log.DebugContextf(ctx, "更审核表状态 audit:%+v, req:%+v", audit, req)
	if auditType == model.AuditBizTypeRelease {
		releaseQa, err := s.dao.GetReleaseQAByID(ctx, audit.RelateID)
		if err != nil {
			return nil, err
		}
		if releaseQa != nil {
			qa, err := s.dao.GetQADetail(ctx, audit.CorpID, audit.RobotID, releaseQa.QAID)
			if err != nil {
				return nil, err
			}
			qa.ReleaseStatus = model.QAReleaseStatusAppealFail
			qa.IsAuditFree = model.QAIsAuditNotFree
			if isPass {
				qa.ReleaseStatus = model.QAReleaseStatusInit
				qa.IsAuditFree = model.QAIsAuditFree
			}
			qas := make(map[uint64]*model.DocQA, 0)
			qas[qa.ID] = qa
			err = s.dao.UpdateAppealQA(ctx, qas)
			if err != nil {
				return nil, errs.ErrSystem
			}
		}
	} else {
		if err := s.dao.UpdateAuditStatus(ctx, audit); err != nil {
			return nil, err
		}
		stat, err := s.dao.GetBizAuditStatusStat(ctx, audit.ParentID, audit.CorpID, audit.RobotID)
		if err != nil {
			return nil, err
		}
		statusList := mapx.Keys(stat)
		isCallBackDoing := slices.ContainsFunc(statusList, func(u uint32) bool {
			return u != model.AuditStatusPass &&
				u != model.AuditStatusFail &&
				u != model.AuditStatusTimeoutFail &&
				u != model.AuditStatusAppealSuccess &&
				u != model.AuditStatusAppealFail
		})
		if !isCallBackDoing {
			if err = s.bizCallBackReleaseConfig(ctx, audit, isPass); err != nil {
				return rsp, nil
			}
		}
	}
	if appealTaskDone {
		_ = s.dao.CreateReleaseAppealNotice(ctx, req.NumSuccess, req.NumFail, req.NumTotal, audit)
	}
	return rsp, nil
}

func (s *Service) getAudit(req *pb.UpdateAuditStatusReq) *model.Audit {
	audit := &model.Audit{}
	audit.ID = req.AuditId
	audit.UpdateTime = time.Now()
	audit.Status = s.getAppealStatus(req.IsPass)
	audit.CorpID = req.CorpId
	audit.RobotID = req.RobotId
	audit.RelateID = req.RelateId
	audit.CreateStaffID = req.CreateStaffId
	audit.ParentID = req.AuditParentId
	return audit
}

func (s *Service) getAppealStatus(isPass bool) uint32 {
	if isPass {
		return model.AuditStatusAppealSuccess
	}
	return model.AuditStatusAppealFail
}

func (s *Service) bizCallBackReleaseConfig(ctx context.Context, audit *model.Audit, isPass bool) error {
	auditParent, err := s.dao.GetAuditByID(ctx, audit.ParentID)
	if err != nil {
		log.ErrorContextf(ctx, "审核发送回调 获取父审核数据失败 ParentID:%d err:%+v", audit.ParentID, err)
		return err
	}
	configs, err := s.dao.GetConfigItemByVersionID(ctx, auditParent.RelateID)
	if err != nil {
		return err
	}
	robot, err := s.dao.GetAppByID(ctx, auditParent.RobotID)
	if err != nil {
		return err
	}
	for _, v := range configs {
		auditStatus := utils.When(isPass, model.ConfigReleaseStatusAppealSuccess, model.ConfigReleaseStatusAppealFail)
		message := utils.When(isPass, model.ConfigReleaseStatusAppealSuccessMsg, model.ConfigReleaseStatusAppealFailMsg)
		v.AuditStatus = auditStatus
		v.Message = message
		if err = s.dao.UpdateAuditConfigItem(ctx, v); err != nil {
			return err
		}
	}
	var release model.AppDetailsConfig
	var preview model.AppDetailsConfig
	if err = jsoniter.Unmarshal([]byte(robot.ReleaseJSON), &release); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
	}
	if err = jsoniter.Unmarshal([]byte(robot.PreviewJSON), &preview); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
	}
	for _, v := range configs {
		updateAppAppeal(&preview, &release, v, robot)
	}
	previewJSON, err := jsoniter.Marshal(preview)
	if err != nil {
		return err
	}
	robot.PreviewJSON = string(previewJSON)
	releaseJSON, err := jsoniter.Marshal(release)
	if err != nil {
		return err
	}
	robot.ReleaseJSON = string(releaseJSON)
	if err = s.dao.ModifyAppJSON(ctx, robot); err != nil {
		return err
	}
	return nil
}

func updateAppAppeal(preview, release *model.AppDetailsConfig, configItem *model.ReleaseConfig, robot *model.AppDB) {
	if configItem == nil {
		return
	}
	switch configItem.ConfigItem {
	case model.ConfigItemBareAnswer:
		bareAnswer := utils.When(configItem.AuditStatus == model.ConfigReleaseStatusAppealSuccess,
			configItem.Value, release.AppConfig.KnowledgeQaConfig.BareAnswer)
		preview.AppConfig.KnowledgeQaConfig.BareAnswer = bareAnswer
		if release.AppConfig.KnowledgeQaConfig != nil {
			release.AppConfig.KnowledgeQaConfig.BareAnswer = bareAnswer
		}
	case model.ConfigItemName:
		name := utils.When(configItem.AuditStatus == model.ConfigReleaseStatusAppealSuccess,
			configItem.Value, release.BaseConfig.Name)
		if len(name) > 0 {
			preview.BaseConfig.Name = name
			release.BaseConfig.Name = name
			robot.Name = name
		}
	case model.ConfigItemAvatar:
		avatar := utils.When(configItem.AuditStatus == model.ConfigReleaseStatusAppealSuccess,
			configItem.Value, release.BaseConfig.Avatar)
		if len(avatar) > 0 {
			preview.BaseConfig.Avatar = avatar
			release.BaseConfig.Avatar = avatar
			robot.Avatar = avatar
		}
	case model.ConfigItemGreeting:
		greeting := utils.When(configItem.AuditStatus == model.ConfigReleaseStatusAppealSuccess,
			configItem.Value, release.AppConfig.KnowledgeQaConfig.Greeting)
		preview.AppConfig.KnowledgeQaConfig.Greeting = greeting
		if release.AppConfig.KnowledgeQaConfig != nil {
			release.AppConfig.KnowledgeQaConfig.Greeting = greeting
		}
	case model.ConfigItemRoleDescription:
		roleDescription := utils.When(configItem.AuditStatus == model.ConfigReleaseStatusAppealSuccess,
			configItem.Value, release.AppConfig.KnowledgeQaConfig.RoleDescription)
		preview.AppConfig.KnowledgeQaConfig.RoleDescription = roleDescription
		if release.AppConfig.KnowledgeQaConfig != nil {
			release.AppConfig.KnowledgeQaConfig.RoleDescription = roleDescription
		}
	}
}
