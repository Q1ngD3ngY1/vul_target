package async

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/gox/mapx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"golang.org/x/exp/slices"
)

// CheckAuditTaskHandler 审核回调check
type CheckAuditTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.AuditCheckParams
}

func registerCheckAuditTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.CheckAuditTask,
		func(t task_scheduler.Task, params entity.AuditCheckParams) task_scheduler.TaskHandler {
			return &CheckAuditTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *CheckAuditTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(CheckAudit) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)

	if d.p.Type == releaseEntity.AuditBizTypeQa && d.p.ParentRelateID > 0 { // 如果是批量导入问答的审核，就走单独的流程
		return d.batchPrepare(ctx, kv)
	}
	audit, err := d.auditLogic.GetAuditByID(ctx, d.p.AuditID) // AuditID是parent ID，返回的audit是父审核任务
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
func (d *CheckAuditTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *CheckAuditTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(CheckAudit) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(CheckAudit) Start kv:%s", k)
		key := k
		id := cast.ToUint64(v)
		audit, err := d.auditLogic.GetAuditByID(ctx, id) // 这里的id是parent ID，返回的audit是父审核任务
		logx.D(ctx, "task(CheckAudit) Start kv:%s audit: %+v", k, audit)
		if err != nil {
			logx.E(ctx, "task(CheckAudit) kv:%s err: %+v", k, err)
			return err
		}
		if audit == nil {
			return errs.ErrAuditNotFound
		}
		if audit.IsCallbackDone() {
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(CheckAudit) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		stat, err := d.auditLogic.GetBizAuditStatusStat(ctx, audit.ID, audit.CorpID, audit.RobotID)
		if err != nil {
			logx.E(ctx, "task(CheckAudit) kv:%s err: %+v", k, err)
			return err
		}
		statusList := mapx.Keys(stat)
		isCallBackDoing := slices.ContainsFunc(statusList, func(u uint32) bool {
			return u != releaseEntity.AuditStatusPass &&
				u != releaseEntity.AuditStatusFail &&
				u != releaseEntity.AuditStatusTimeoutFail &&
				u != releaseEntity.AuditStatusAppealSuccess &&
				u != releaseEntity.AuditStatusAppealFail
		})
		if isCallBackDoing {
			if d.reachRetryLimit(ctx, audit) {
				logx.I(ctx, "task(CheckAudit) 审核超过最大重试次数, task:%+v, audit:%+v",
					d.task, audit)
				// 文档审核和问答审核，超过最大重试次数，就把子审核设置为审核超时
				if err = d.updateChildAuidtStatusWhenReachRetryLimit(ctx, audit); err != nil {
					return err
				}
				// 重新获取子审核状态
				stat, err = d.auditLogic.GetBizAuditStatusStat(ctx, audit.ID, audit.CorpID, audit.RobotID)
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
			logx.E(ctx, "task(CheckAudit) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(CheckAudit) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *CheckAuditTaskHandler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *CheckAuditTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *CheckAuditTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(CheckAudit) Done")
	return nil
}

func (d *CheckAuditTaskHandler) bizCallBack(
	ctx context.Context, audit *releaseEntity.Audit, stat map[uint32]*releaseEntity.AuditStatusStat) (err error) {
	lockKey := fmt.Sprintf(dao.LockForAuditCheck, audit.ID)
	if err = d.dao.Lock(ctx, lockKey, 10*time.Second); err != nil {
		return errs.ErrOperateDoing
	}
	defer func() { _ = d.dao.UnLock(ctx, lockKey) }()
	isAuditPass := true
	if _, ok := stat[releaseEntity.AuditStatusFail]; ok {
		isAuditPass = false
	} else if _, ok = stat[releaseEntity.AuditStatusTimeoutFail]; ok {
		isAuditPass = false
	} else if _, ok = stat[releaseEntity.AuditStatusAppealFail]; ok {
		isAuditPass = false
	}

	switch audit.Type {
	// 这两类的审核 应该归属于admin
	// case releaseEntity.AuditBizTypeRobotProfile:
	// 	err = d.auditLogic.AuditRobotProfile(ctx, audit)
	// case releaseEntity.AuditBizTypeBareAnswer:
	// 	err = d.auditLogic.AuditBareAnswer(ctx, audit)
	case releaseEntity.AuditBizTypeDoc, releaseEntity.AuditBizTypeDocSegment, releaseEntity.AuditBizTypeDocTableSheet:
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
		err = d.auditLogic.ProcessDocAuditParentTask(ctx, audit, isAuditPass, false, "", d.p)
	case releaseEntity.AuditBizTypeRelease:
		// do nothing  release not need audit
	case releaseEntity.AuditBizTypeQa:
		err = d.auditLogic.AuditQa(ctx, audit, isAuditPass, false, "")
	case releaseEntity.AuditBizTypeDocName:
		err = d.auditLogic.AuditDocName(ctx, audit, isAuditPass, false, "")
	default:
		return errs.ErrInvalidAuditSource
	}
	if err != nil {
		logx.E(ctx, "bizCallBack, 审核回调失败, audit:%+v, err:%+v", audit, err)
		return err
	}
	return nil
}

// testSetDocAuditTimeout 设置文档审核超时，用于测试
func (d *CheckAuditTaskHandler) testSetDocAuditTimeout(ctx context.Context, audit *releaseEntity.Audit) {
	parentAudit := *audit
	parentAudit.Status = releaseEntity.AuditStatusTimeoutFail
	parentAudit.UpdateTime = time.Now()
	logx.I(ctx, "设置文档审核超时，用于测试，audit:%+v", parentAudit)
	_ = d.auditLogic.TestUpdateAuditStatusByParentID(ctx, &parentAudit)
	return
}

// batchPrepare 批量数据准备
func (d *CheckAuditTaskHandler) batchPrepare(ctx context.Context, kv task_scheduler.TaskKV) (task_scheduler.TaskKV,
	error) {
	idStart := uint64(0)
	length := 1
	limit := 1000
	for length > 0 {
		parentAuditIDs, err := d.auditLogic.GetParentAuditIDsByParentRelateID(ctx, d.p, idStart, limit)
		if err != nil {
			logx.E(ctx, "batchPrepare, 获取父审核数据失败 params:%+v, err:%+v", d.p, err)
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

func (d *CheckAuditTaskHandler) reachRetryLimit(ctx context.Context, audit *releaseEntity.Audit) bool {
	if audit.Type != releaseEntity.AuditBizTypeDoc &&
		audit.Type != releaseEntity.AuditBizTypeQa &&
		audit.Type != releaseEntity.AuditBizTypeDocName {
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
func (d *CheckAuditTaskHandler) updateChildAuidtStatusWhenReachRetryLimit(ctx context.Context,
	audit *releaseEntity.Audit) error {
	audit1 := *audit
	audit1.Status = releaseEntity.AuditStatusTimeoutFail
	audit1.UpdateTime = time.Now()
	audit1.Message = releaseEntity.MessageAuditCheckReachRetryLimit
	if err := d.auditLogic.UpdateAuditStatusByParentID(ctx, &audit1, 100); err != nil {
		logx.E(ctx, "更新子审核状态失败, parentAudit:%+v, err:%+v", audit1, err)
		return err
	}
	return nil
}
