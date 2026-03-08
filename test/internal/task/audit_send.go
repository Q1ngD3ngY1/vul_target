// Package task 送审任务
package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"

	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

// SendAuditScheduler 送审任务
type SendAuditScheduler struct {
	dao      dao.Dao
	task     task_scheduler.Task
	instance app.Base
	p        model.AuditSendParams
}

func initSendAuditScheduler() {
	task_scheduler.Register(
		model.SendAuditTask,
		func(t task_scheduler.Task, params model.AuditSendParams) task_scheduler.TaskHandler {
			return &SendAuditScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *SendAuditScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(SendAudit) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	var children []*model.Audit
	appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if appDB == nil {
		return kv, errs.ErrRobotNotFound
	}
	if d.p.Type == model.AuditBizTypeQa && d.p.ParentRelateID > 0 { // 如果是批量导入问答的审核，就走单独的流程
		return d.batchDealWithParentAudits(ctx, appDB, kv)
	}

	parent, err := d.dao.GetAuditByBizID(ctx, d.p.ParentAuditBizID)
	if err != nil {
		return kv, err
	}
	if parent == nil {
		return kv, errs.ErrAuditNotFound
	}
	audits, err := d.dao.GetAuditByParentID(ctx, parent.ID, d.p)
	if err != nil {
		return kv, err
	}
	for _, audit := range audits {
		if audit.Status == model.AuditStatusDoing {
			kv[fmt.Sprintf("%d", audit.BusinessID)] = fmt.Sprintf("%d", audit.BusinessID)
		}
	}
	if audits != nil {
		return kv, nil
	}
	if d.p.Type == model.AuditBizTypeRelease {
		release, err := d.dao.GetReleaseByID(ctx, d.p.RelateID)
		if err != nil {
			return kv, errs.ErrReleaseNotFound
		}
		if release == nil {
			return kv, errs.ErrReleaseNotFound
		}
		instance := app.GetApp(appDB.AppType)
		if instance == nil {
			return kv, errs.ErrGetAppFail
		}
		d.instance = instance
		audits, err := d.instance.AuditCollect(ctx, parent, release, d.p)
		if err != nil {
			return kv, err
		}
		children = audits
	} else {
		children, err = d.dao.BatchCreateAudit(ctx, parent, appDB, d.p)
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
func (d *SendAuditScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *SendAuditScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(SendAudit) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(SendAudit) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(SendAudit) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(SendAudit) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		audit, err := d.dao.GetAuditByBizID(ctx, id)
		if err != nil {
			return err
		}
		if audit == nil {
			return errs.ErrAuditNotFound
		}
		if audit.Type == model.AuditBizTypeRelease {
			instance := app.GetApp(appDB.AppType)
			if instance == nil {
				return errs.ErrGetAppFail
			}
			if err = instance.BeforeAudit(ctx, audit); err != nil {
				return err
			}
		}
		if !audit.IsMaxSendAuditRetryTimes() {
			if err = d.dao.SendAudit(ctx, audit, appDB.InfosecBizType); err != nil {
				return err
			}
		}
		if err = progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(SendAudit) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(SendAudit) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *SendAuditScheduler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *SendAuditScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *SendAuditScheduler) Done(ctx context.Context) error {
	appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
	if err != nil {
		return err
	}
	if appDB.HasDeleted() {
		log.DebugContextf(ctx, "task(SendAudit) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
		return nil
	}
	if d.p.Type == model.AuditBizTypeQa && d.p.ParentRelateID > 0 { // 如果是批量导入问答的审核，就走单独的流程
		if err = d.dao.CreateParentAuditCheckForExcel2Qa(ctx, d.p); err != nil {
			return err
		}
		log.DebugContextf(ctx, "task(SendAudit) Done, AuditSendParams: %+v", d.p)
		return nil
	}
	parent, err := d.dao.GetAuditByBizID(ctx, d.p.ParentAuditBizID)
	if err != nil {
		log.ErrorContextf(ctx, "审核发送回调 获取父审核数据失败 bizID:%d err:%+v", d.p.ParentAuditBizID, err)
		return err
	}
	if parent == nil { // 找不到父审核任务，可能是db数据被回滚掉了，不报错
		log.InfoContextf(ctx, "审核发送回调 父审核数据不存在 bizID:%d", d.p.ParentAuditBizID)
		return nil
	}
	// 如果为干预任务则创建AuditCheck任务时，需携带
	if d.p.Type == model.AuditBizTypeDocSegment || d.p.Type == model.AuditBizTypeDocTableSheet {
		if err = d.dao.CreateParentAuditCheckWithOriginDocBizID(ctx, parent, d.p.OriginDocBizID); err != nil {
			log.ErrorContextf(ctx, "CreateParentAuditCheckWithOriginDocBizID failed for OriginDocBizID:%d and parent:%+v, error:%v",
				d.p.OriginDocBizID, parent, err)
			return err
		}
	} else {
		if err = d.dao.CreateParentAuditCheck(ctx, parent); err != nil {
			return err
		}
	}
	log.DebugContextf(ctx, "task(SendAudit) Done, parent audit: %+v", parent)
	return nil
}

// batchDealWithParentAudits 批量处理父审核任务
func (d *SendAuditScheduler) batchDealWithParentAudits(ctx context.Context, appDB *model.AppDB,
	kv task_scheduler.TaskKV) (task_scheduler.TaskKV, error) {
	idStart := uint64(0)
	length := 1
	limit := 500
	for length > 0 {
		parentAudits, err := d.dao.GetParentAuditsByParentRelateID(ctx, d.p, idStart, limit)
		if err != nil {
			log.ErrorContextf(ctx, "batchDealWithParentAudits, 获取父审核数据失败 params:%+v, err:%+v", d.p, err)
			return kv, err
		}
		length = len(parentAudits)
		if length == 0 {
			break
		}
		idStart = parentAudits[length-1].ID
		for _, parentAudit := range parentAudits {
			audits, err := d.dao.GetAuditByParentID(ctx, parentAudit.ID, d.p)
			if err != nil {
				log.ErrorContextf(ctx, "batchDealWithParentAudits, 获取子审核数据失败 params:%+v, err:%+v",
					d.p, err)
				return kv, err
			}
			for _, audit := range audits {
				if audit.Status == model.AuditStatusDoing {
					kv[fmt.Sprintf("%d", audit.BusinessID)] = fmt.Sprintf("%d", audit.BusinessID)
				}
			}
			if audits != nil {
				continue
			}
			// 每个父审核，分别创建子审核
			children, err := d.dao.BatchCreateAudit(ctx, parentAudit, appDB, d.p)
			if err != nil {
				log.ErrorContextf(ctx, "batchDealWithParentAudits, 批量创建子审核失败 params:%+v, err:%+v",
					d.p, err)
				return kv, err
			}
			for _, child := range children {
				kv[fmt.Sprintf("%d", child.BusinessID)] = fmt.Sprintf("%d", child.BusinessID)
			}
		}
	}
	if len(kv) == 0 {
		log.WarnContextf(ctx, "batchDealWithParentAudits, 送审kv为空 params:%+v", d.p)
	}
	return kv, nil
}
