package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"strconv"
	"time"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/mapx"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicAudit "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/audit"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"golang.org/x/exp/slices"
)

// CheckAuditScheduler 审核回调check
type CheckAuditScheduler struct {
	dao      dao.Dao
	task     task_scheduler.Task
	instance app.Base
	p        model.AuditCheckParams
}

func initCheckAuditScheduler() {
	task_scheduler.Register(
		model.CheckAuditTask,
		func(t task_scheduler.Task, params model.AuditCheckParams) task_scheduler.TaskHandler {
			return &CheckAuditScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *CheckAuditScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(CheckAudit) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)

	if d.p.Type == model.AuditBizTypeQa && d.p.ParentRelateID > 0 { // 如果是批量导入问答的审核，就走单独的流程
		return d.batchPrepare(ctx, kv)
	}
	audit, err := d.dao.GetAuditByID(ctx, d.p.AuditID) // AuditID是parent ID，返回的audit是父审核任务
	if err != nil {
		return kv, err
	}
	if audit == nil {
		return kv, errs.ErrAuditNotFound
	}
	kv[fmt.Sprintf("%d", d.p.AuditID)] = fmt.Sprintf("%d", d.p.AuditID)
	return kv, nil
}

// Init 初始化
func (d *CheckAuditScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *CheckAuditScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(CheckAudit) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(CheckAudit) Start kv:%s", k)
		key := k
		id := cast.ToUint64(v)
		audit, err := d.dao.GetAuditByID(ctx, id) // 这里的id是parent ID，返回的audit是父审核任务
		log.DebugContextf(ctx, "task(CheckAudit) Start kv:%s audit: %+v", k, audit)
		if err != nil {
			log.ErrorContextf(ctx, "task(CheckAudit) kv:%s err: %+v", k, err)
			return err
		}
		if audit == nil {
			return errs.ErrAuditNotFound
		}
		if audit.IsCallbackDone() {
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(CheckAudit) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		stat, err := d.dao.GetBizAuditStatusStat(ctx, audit.ID, audit.CorpID, audit.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "task(CheckAudit) kv:%s err: %+v", k, err)
			return err
		}
		statusList := mapx.Keys(stat)
		isCallBackDoing := slices.ContainsFunc(statusList, func(u uint32) bool {
			return u != model.AuditStatusPass &&
				u != model.AuditStatusFail &&
				u != model.AuditStatusTimeoutFail &&
				u != model.AuditStatusAppealSuccess &&
				u != model.AuditStatusAppealFail
		})
		if isCallBackDoing {
			if d.reachRetryLimit(ctx, audit) {
				log.InfoContextf(ctx, "task(CheckAudit) 审核超过最大重试次数, task:%+v, audit:%+v",
					d.task, audit)
				// 文档审核和问答审核，超过最大重试次数，就把子审核设置为审核超时
				if err = d.updateChildAuidtStatusWhenReachRetryLimit(ctx, audit); err != nil {
					return err
				}
				// 重新获取子审核状态
				stat, err = d.dao.GetBizAuditStatusStat(ctx, audit.ID, audit.CorpID, audit.RobotID)
				if err != nil {
					return err
				}
			} else {
				continue
			}
		}
		if err = d.bizCallBack(ctx, audit, stat); err != nil {
			return err
		}
		if err = progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(CheckAudit) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(CheckAudit) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *CheckAuditScheduler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *CheckAuditScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *CheckAuditScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(CheckAudit) Done")
	return nil
}

func (d *CheckAuditScheduler) bizCallBack(
	ctx context.Context, audit *model.Audit, stat map[uint32]*model.AuditStatusStat) (err error) {
	lockKey := fmt.Sprintf(dao.LockForAuditCheck, audit.ID)
	if err = d.dao.Lock(ctx, lockKey, 10*time.Second); err != nil {
		return errs.ErrOperateDoing
	}
	defer func() { _ = d.dao.UnLock(ctx, lockKey) }()
	isAuditPass := true
	if _, ok := stat[model.AuditStatusFail]; ok {
		isAuditPass = false
	} else if _, ok = stat[model.AuditStatusTimeoutFail]; ok {
		isAuditPass = false
	} else if _, ok = stat[model.AuditStatusAppealFail]; ok {
		isAuditPass = false
	}

	switch audit.Type {
	case model.AuditBizTypeRobotProfile:
		err = d.dao.AuditRobotProfile(ctx, audit)
	case model.AuditBizTypeBareAnswer:
		err = d.dao.AuditBareAnswer(ctx, audit)
	case model.AuditBizTypeDoc, model.AuditBizTypeDocSegment, model.AuditBizTypeDocTableSheet:
		if len(config.App().DebugConfig.DocAuditTimeoutBotList) > 0 {
			bizID, err := d.dao.GetBotBizIDByID(ctx, audit.RobotID)
			if err == nil {
				bizIDStr := strconv.FormatUint(bizID, 10)
				if slices.Contains(config.App().DebugConfig.DocAuditTimeoutBotList, bizIDStr) {
					d.testSetDocAuditTimeout(ctx, audit)
					isAuditPass = false
				}
			}
		}
		err = logicAudit.ProcessDocAuditParentTask(ctx, d.dao, audit, isAuditPass, false, "", d.p)
	case model.AuditBizTypeRelease:
		err = d.auditReleaseExec(ctx, audit, isAuditPass)
	case model.AuditBizTypeQa:
		err = d.dao.AuditQa(ctx, audit, isAuditPass, false, "")
	case model.AuditBizTypeDocName:
		err = d.dao.AuditDocName(ctx, audit, isAuditPass, false, "")
	default:
		return errs.ErrInvalidAuditSource
	}
	if err != nil {
		return err
	}
	return nil
}

// testSetDocAuditTimeout 设置文档审核超时，用于测试
func (d *CheckAuditScheduler) testSetDocAuditTimeout(ctx context.Context, audit *model.Audit) {
	parentAudit := *audit
	parentAudit.Status = model.AuditStatusTimeoutFail
	parentAudit.UpdateTime = time.Now()
	log.InfoContextf(ctx, "设置文档审核超时，用于测试，audit:%+v", parentAudit)
	_ = d.dao.TestUpdateAuditStatusByParentID(ctx, &parentAudit)
	return
}

func (d *CheckAuditScheduler) auditReleaseExec(ctx context.Context, audit *model.Audit, isAuditPass bool) error {
	release, err := d.dao.GetReleaseByID(ctx, audit.RelateID)
	if err != nil {
		return errs.ErrReleaseNotFound
	}
	if release == nil {
		return errs.ErrReleaseNotFound
	}
	appDB, err := d.dao.GetAppByID(ctx, release.RobotID)
	if err != nil {
		return err
	}
	if appDB == nil {
		return errs.ErrRobotNotFound
	}
	instance := app.GetApp(appDB.AppType)
	if instance == nil {
		return errs.ErrGetAppFail
	}
	d.instance = instance
	if err = d.instance.AfterAudit(ctx, audit, isAuditPass); err != nil {
		return err
	}
	return nil
}

// batchPrepare 批量数据准备
func (d *CheckAuditScheduler) batchPrepare(ctx context.Context, kv task_scheduler.TaskKV) (task_scheduler.TaskKV,
	error) {
	idStart := uint64(0)
	length := 1
	limit := 1000
	for length > 0 {
		parentAuditIDs, err := d.dao.GetParentAuditIDsByParentRelateID(ctx, d.p, idStart, limit)
		if err != nil {
			log.ErrorContextf(ctx, "batchPrepare, 获取父审核数据失败 params:%+v, err:%+v", d.p, err)
			return kv, err
		}
		length = len(parentAuditIDs)
		if length == 0 {
			break
		}
		idStart = parentAuditIDs[length-1]
		for _, id := range parentAuditIDs {
			kv[fmt.Sprintf("%d", id)] = fmt.Sprintf("%d", id)
			continue
		}
	}
	return kv, nil
}

func (d *CheckAuditScheduler) reachRetryLimit(ctx context.Context, audit *model.Audit) bool {
	if audit.Type != model.AuditBizTypeDoc &&
		audit.Type != model.AuditBizTypeQa &&
		audit.Type != model.AuditBizTypeDocName {
		return false
	}
	if len(config.App().DebugConfig.AuditCheckReachRetryLimitBotList) > 0 {
		bizID, err := d.dao.GetBotBizIDByID(ctx, audit.RobotID)
		if err == nil {
			bizIDStr := strconv.FormatUint(bizID, 10)
			if slices.Contains(config.App().DebugConfig.AuditCheckReachRetryLimitBotList, bizIDStr) {
				d.task.RetryTimes = d.task.MaxRetryTimes - 1
			}
		}
	}
	if d.task.RetryTimes >= d.task.MaxRetryTimes-1 && d.task.MaxRetryTimes > 0 {
		return true
	}
	return false
}

// updateChildAuidtStatusWhenReachRetryLimit 当审核回调check任务达到最大重试次数，仍然未完成审核回调，就更新子审核状态为超时
func (d *CheckAuditScheduler) updateChildAuidtStatusWhenReachRetryLimit(ctx context.Context,
	audit *model.Audit) error {
	audit1 := *audit
	audit1.Status = model.AuditStatusTimeoutFail
	audit1.UpdateTime = time.Now()
	audit1.Message = model.MessageAuditCheckReachRetryLimit
	if err := d.dao.UpdateAuditStatusByParentID(ctx, &audit1, 100); err != nil {
		log.ErrorContextf(ctx, "更新子审核状态失败, parentAudit:%+v, err:%+v", audit1, err)
		return err
	}
	return nil
}
