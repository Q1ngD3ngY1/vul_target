// bot-knowledge-config-server
//
// @(#)appeal.go  星期四, 十月 17, 2024
// Copyright(c) 2024, randalchen@Tencent. All rights reserved.

package api

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

var appealType2AuditType = map[uint32]uint32{
	model.AppealBizTypeDoc:           model.AuditBizTypeDoc,
	model.AppealBizTypeQa:            model.AuditBizTypeQa,
	model.AppealBizTypeDocName:       model.AuditBizTypeDocName,
	model.AppealBizTypeDocSegment:    model.AuditBizTypeDocSegment,
	model.AppealBizTypeDocTableSheet: model.AuditBizTypeDocTableSheet,
}

// AppealCallback 申诉回调接口
// 同一个qa的问答文本、答案中的图片、答案中的视频、相似问，前端是整体申诉的，但是后端却是分开申诉的。
// 每个子申诉任务完成后，OP服务会回调本接口。任意一个子申诉的结果为不通过，则整个申诉任务就会失败，前端就会展示问答申诉失败。
// 所以问答申诉失败后，OP那边可能还有未处理的申诉子任务。
// 申诉失败后，用户可以编辑问答，然后走机器审核。如果机器审核不通过，用户可以再次申诉。此时未处理完的旧申诉和新申诉并存，
// 所以在处理申诉前，需要判断当前回调是否是最新的申诉任务。
func (s *Service) AppealCallback(ctx context.Context, req *pb.AppealCallbackReq) (*pb.AppealCallbackRsp,
	error) {
	rsp := new(pb.AppealCallbackRsp)
	appealType := req.GetAppealType()
	appealID := req.GetAppealId()

	if appealType != model.AppealBizTypeQa &&
		appealType != model.AppealBizTypeDoc &&
		appealType != model.AppealBizTypeDocName &&
		appealType != model.AppealBizTypeDocSegment &&
		appealType != model.AppealBizTypeDocTableSheet { // 这里是判断申诉类型
		log.ErrorContextf(ctx, "申诉类型不匹配，appealType:%d, req:%+v", appealType, req)
		return rsp, errs.ErrParams
	}
	var err error
	key := fmt.Sprintf(dao.LockForAuditAppeal, appealID)
	if err = s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		return rsp, errs.ErrAppealCallbackDoing
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()

	auditType, _ := appealType2AuditType[appealType]
	audit := s.fillAudit(req, auditType)
	log.InfoContextf(ctx, "AppealCallback audit:%+v, req:%+v", audit, req)
	// 1. 判断当前回调是否是最新的申诉任务。这里相当于也进行了参数校验
	lastParentAudit, err := s.dao.GetLatestParentAuditFailByRelateID(ctx, req.GetCorpId(), req.GetRobotId(),
		req.GetRelateId(), auditType)
	if err != nil {
		log.ErrorContextf(ctx, "GetLatestParentAuditFailByRelateID fail，appealType:%d, audit:%+v, err:%v",
			appealType, audit, err)
		return rsp, err
	}
	if lastParentAudit.ID != req.GetAuditParentId() && lastParentAudit.Status != model.AuditStatusAppealIng {
		log.InfoContextf(ctx, "当前回调的父审核id不是最新的，或者没有处于人工申诉中，忽略本次申诉回调请求。 "+
			"lastParentAuditID:%d, req:%+v", lastParentAudit.ID, req)
		return rsp, nil
	}

	// 2. 更新t_audit表对应的子审核状态
	if err = s.dao.UpdateAuditStatus(ctx, audit); err != nil {
		log.ErrorContextf(ctx, "UpdateAuditStatus fail，appealType:%d, audit:%+v, err:%v",
			appealType, audit, err)
		return rsp, errs.ErrSystem
	}
	// 3. 获取该parentID下的所有子审核状态
	auditStatus, err := s.dao.GetBizAuditStatusStat(ctx, req.GetAuditParentId(), req.GetCorpId(), req.GetRobotId())
	if err != nil {
		log.ErrorContextf(ctx, "GetBizAuditStatusStat fail，appealType:%d, audit:%+v, err:%v",
			appealType, audit, err)
		return rsp, errs.ErrSystem
	}

	allPass := true
	for k := range auditStatus {
		if k != model.AuditStatusPass && k != model.AuditStatusAppealSuccess {
			allPass = false
		}
	}
	// 4. 如果是都是通过，就把对应的qa&相似问或者文档状态改成审核通过
	parentAudit := s.fillParentAudit(req, auditType)
	if allPass {
		if auditType == model.AuditBizTypeQa { // 这里是判断审核类型
			err = s.dao.AuditQa(ctx, parentAudit, true, true, "")
		} else if auditType == model.AuditBizTypeDocName {
			err = s.dao.AuditDocName(ctx, parentAudit, true, true, "")
		} else { // 干预也在AuditDoc中处理
			err = s.dao.AuditDoc(ctx, parentAudit, true, true, "")
		}
		if err != nil {
			log.ErrorContextf(ctx, "allPass is true, Audit qa or doc fail，appealType:%d, parentAudit:%+v, "+
				"err:%v", appealType, parentAudit, err)
			return rsp, err
		}
	}
	// 5. 如果有子审核失败，就把对应的qa&相似问或者文档状态改成审核失败
	if !req.GetIsPass() {
		if auditType == model.AuditBizTypeQa { // 这里是判断审核类型
			err = s.dao.AuditQa(ctx, parentAudit, false, true, req.GetAppealRejectReason())
		} else if auditType == model.AuditBizTypeDocName {
			err = s.dao.AuditDocName(ctx, parentAudit, false, true, req.GetAppealRejectReason())
		} else {
			err = s.dao.AuditDoc(ctx, parentAudit, false, true, req.GetAppealRejectReason())
		}
		if err != nil {
			log.ErrorContextf(ctx, "hasAppealFail is true, Audit qa or doc fail，appealType:%d, "+
				"parentAudit:%+v, err:%v", appealType, parentAudit, err)
			return rsp, err
		}
	}
	log.InfoContextf(ctx, "Audit qa or doc ok，appealType:%d, audit:%+v", appealType, audit)
	return rsp, nil
}

// fillAudit .
func (s *Service) fillAudit(req *pb.AppealCallbackReq, auditType uint32) *model.Audit {
	audit := &model.Audit{}
	audit.ID = req.GetAuditId()
	audit.UpdateTime = time.Now()
	audit.CorpID = req.GetCorpId()
	audit.RobotID = req.GetRobotId()
	audit.RelateID = req.GetRelateId()
	audit.CreateStaffID = req.GetCreateStaffId()
	audit.ParentID = req.GetAuditParentId()
	audit.Type = auditType
	if req.GetIsPass() {
		audit.Status = model.AuditStatusAppealSuccess
	} else {
		audit.Status = model.AuditStatusAppealFail
	}
	return audit
}

// fillParentAudit .
func (s *Service) fillParentAudit(req *pb.AppealCallbackReq, auditType uint32) *model.Audit {
	audit := &model.Audit{}
	audit.ID = req.GetAuditParentId()
	audit.UpdateTime = time.Now()
	audit.CorpID = req.GetCorpId()
	audit.RobotID = req.GetRobotId()
	audit.RelateID = req.GetRelateId()
	audit.CreateStaffID = req.GetCreateStaffId()
	audit.ParentID = 0
	audit.Type = auditType
	return audit
}
