// Package task 送审任务
package async

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// SendAuditTaskHandler 送审任务
type SendAuditTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.AuditSendParams
}

func registerSendAuditTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.SendAuditTask,
		func(t task_scheduler.Task, params entity.AuditSendParams) task_scheduler.TaskHandler {
			return &SendAuditTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *SendAuditTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(SendAudit) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	var children []*releaseEntity.Audit
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if appDB == nil {
		return kv, errs.ErrRobotNotFound
	}
	if d.p.Type == releaseEntity.AuditBizTypeQa && d.p.ParentRelateID > 0 { // 如果是批量导入问答的审核，就走单独的流程
		return d.batchDealWithParentAudits(ctx, appDB, kv)
	}

	parent, err := d.auditLogic.GetAuditByBizID(ctx, d.p.ParentAuditBizID)
	if err != nil {
		logx.E(ctx, "task(SendAudit) GetAuditByBizID: %+v, params: %+v, err:%+v", d.task, err)
		return kv, err
	}
	if parent == nil {
		return kv, errs.ErrAuditNotFound
	}

	logx.I(ctx, "task(SendAudit) GetAuditByParentID: %+v, params: %+v", d.task, d.p)
	audits, err := d.auditLogic.GetAuditByParentID(ctx, parent.ID, d.p)
	if err != nil {
		return kv, err
	}
	for _, audit := range audits {
		if audit.Status == releaseEntity.AuditStatusDoing {
			kv[fmt.Sprintf("%d", audit.BusinessID)] = fmt.Sprintf("%d", audit.BusinessID)
		}
	}
	if audits != nil {
		return kv, nil
	}
	if d.p.Type == releaseEntity.AuditBizTypeRelease {
		release, err := d.releaseLogic.GetReleaseByID(ctx, d.p.RelateID)
		if err != nil {
			return kv, errs.ErrReleaseNotFound
		}
		if release == nil {
			return kv, errs.ErrReleaseNotFound
		}
		if appDB.AppType != entity.KnowledgeQaAppType {
			return kv, errs.ErrGetAppFail
		}
		audits, err := d.auditLogic.AuditCollect(ctx, parent, release, d.p)
		if err != nil {
			return kv, err
		}
		children = audits
	} else {
		children, err = d.auditLogic.BatchCreateAudit(ctx, parent, appDB, d.p)
		if err != nil {
			return kv, err
		}
	}
	for _, child := range children {
		kv[fmt.Sprintf("%d", child.BusinessID)] = fmt.Sprintf("%d", child.BusinessID)
	}
	return kv, nil
}

// Init 初始化
func (d *SendAuditTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *SendAuditTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(SendAudit) Process, task: %+v, params: %+v", d.task, d.p)
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "task(SendAudit) DescribeAppByPrimaryIdWithoutNotFoundError: %+v, params: %+v, err:%+v", d.task, err)
		return err
	}
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(SendAudit) Start k:%s, v:%s", k, v)
		key := k
		if appDB.HasDeleted() {
			logx.D(ctx, "task(SendAudit) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(SendAudit) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		audit, err := d.auditLogic.GetAuditByBizID(ctx, id)
		if err != nil {
			return err
		}
		if audit == nil {
			return errs.ErrAuditNotFound
		}
		if audit.Type == releaseEntity.AuditBizTypeRelease {
			if appDB.AppType != entity.KnowledgeQaAppType {
				return errs.ErrGetAppFail
			}
			if err = d.auditLogic.BeforeAudit(ctx, audit); err != nil {
				return err
			}
		}
		if !audit.IsMaxSendAuditRetryTimes() {
			logx.I(ctx, "task(SendAudit) start to SendAudit:%+v to infosec", audit)
			if err = d.auditLogic.SendAudit(ctx, audit, appDB.InfosecBizType); err != nil {
				return err
			}
		}
		if err = progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(SendAudit) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(SendAudit) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *SendAuditTaskHandler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *SendAuditTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *SendAuditTaskHandler) Done(ctx context.Context) error {
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		return err
	}
	if appDB.HasDeleted() {
		logx.D(ctx, "task(SendAudit) appDB.HasDeleted()|appID:%d", d.p.RobotID)
		return nil
	}
	if d.p.Type == releaseEntity.AuditBizTypeQa && d.p.ParentRelateID > 0 { // 如果是批量导入问答的审核，就走单独的流程
		if err = d.auditLogic.CreateParentAuditCheckForExcel2Qa(ctx, d.p); err != nil {
			return err
		}
		logx.D(ctx, "task(SendAudit) Done, AuditSendParams: %+v", d.p)
		return nil
	}
	parent, err := d.auditLogic.GetAuditByBizID(ctx, d.p.ParentAuditBizID)
	if err != nil {
		logx.E(ctx, "审核发送回调 获取父审核数据失败 bizID:%d err:%+v", d.p.ParentAuditBizID, err)
		return err
	}
	if parent == nil { // 找不到父审核任务，可能是db数据被回滚掉了，不报错
		logx.I(ctx, "审核发送回调 父审核数据不存在 bizID:%d", d.p.ParentAuditBizID)
		return nil
	}
	// 如果为干预任务则创建AuditCheck任务时，需携带
	if d.p.Type == releaseEntity.AuditBizTypeDocSegment || d.p.Type == releaseEntity.AuditBizTypeDocTableSheet {
		if err = d.auditLogic.CreateParentAuditCheckWithOriginDocBizID(ctx, parent, d.p.OriginDocBizID); err != nil {
			logx.E(ctx, "CreateParentAuditCheckWithOriginDocBizID failed for OriginDocBizID:%d and parent:%+v, error:%v",
				d.p.OriginDocBizID, parent, err)
			return err
		}
	} else {
		if err = d.auditLogic.CreateParentAuditCheck(ctx, parent); err != nil {
			return err
		}
	}
	logx.D(ctx, "task(SendAudit) Done, parent audit: %+v", parent)
	return nil
}

// batchDealWithParentAudits 批量处理父审核任务
func (d *SendAuditTaskHandler) batchDealWithParentAudits(ctx context.Context, appDB *entity.App,
	kv task_scheduler.TaskKV) (task_scheduler.TaskKV, error) {
	logx.I(ctx, "batchDealWithParentAudits, params:%+v", d.p)
	idStart := uint64(0)
	length := 1
	limit := 500
	for length > 0 {
		logx.I(ctx, "batchDealWithParentAudits, idStart:%d, limit:%d", idStart, limit)
		parentAudits, err := d.auditLogic.GetParentAuditsByParentRelateID(ctx, d.p, idStart, limit)
		if err != nil {
			logx.E(ctx, "batchDealWithParentAudits, 获取父审核数据失败 params:%+v, err:%+v", d.p, err)
			return kv, err
		}
		length = len(parentAudits)
		if length == 0 {
			break
		}
		idStart = parentAudits[length-1].ID
		logx.I(ctx, "batchDealWithParentAudits, parentAudits::%d", len(parentAudits))
		for _, parentAudit := range parentAudits {
			audits, err := d.auditLogic.GetAuditByParentID(ctx, parentAudit.ID, d.p)
			if err != nil {
				logx.E(ctx, "batchDealWithParentAudits, 获取子审核数据失败 params:%+v, err:%+v",
					d.p, err)
				return kv, err
			}
			for _, audit := range audits {
				if audit.Status == releaseEntity.AuditStatusDoing {
					kv[fmt.Sprintf("%d", audit.BusinessID)] = fmt.Sprintf("%d", audit.BusinessID)
				}
			}
			if audits != nil {
				continue
			}
			// 每个父审核，分别创建子审核，子审核的parent_id 一定不为0；
			children, err := d.auditLogic.BatchCreateAudit(ctx, parentAudit, appDB, d.p)
			if err != nil {
				logx.E(ctx, "batchDealWithParentAudits, 批量创建子审核失败 params:%+v, err:%+v",
					d.p, err)
				return kv, err
			}
			for _, child := range children {
				kv[fmt.Sprintf("%d", child.BusinessID)] = fmt.Sprintf("%d", child.BusinessID)
			}
		}
	}
	if len(kv) == 0 {
		logx.W(ctx, "batchDealWithParentAudits, 送审kv为空 params:%+v", d.p)
	}
	return kv, nil
}
