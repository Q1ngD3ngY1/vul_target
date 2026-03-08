package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"
)

// ReleaseCollectScheduler 发布采集任务
type ReleaseCollectScheduler struct {
	dao      dao.Dao
	task     task_scheduler.Task
	instance app.Base
	p        model.ReleaseCollectParams
}

func initReleaseCollectScheduler() {
	task_scheduler.Register(
		model.ReleaseCollectTask,
		func(t task_scheduler.Task, params model.ReleaseCollectParams) task_scheduler.TaskHandler {
			return &ReleaseCollectScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *ReleaseCollectScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(ReleaseCollect) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	pkg.WithRequestID(ctx, traceID)
	release, err := d.dao.GetReleaseByID(ctx, d.p.VersionID)
	if err != nil {
		return kv, err
	}
	if release == nil {
		return kv, errs.ErrReleaseNotFound
	}
	appDB, err := d.dao.GetAppByID(ctx, release.RobotID)
	if err != nil {
		return kv, err
	}
	if appDB == nil {
		return kv, errs.ErrRobotNotFound
	}
	instance := app.GetApp(appDB.AppType)
	if instance == nil {
		return kv, errs.ErrGetAppFail
	}
	d.instance = instance
	if err = d.instance.Collect(ctx, release); err != nil {
		return kv, err
	}
	kv[fmt.Sprintf("%d", release.ID)] = fmt.Sprintf("%d", release.ID)
	return kv, nil
}

// Init 初始化
func (d *ReleaseCollectScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *ReleaseCollectScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(ReleaseCollect) Process, task: %+v, params: %+v", d.task, d.p)
	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	pkg.WithRequestID(ctx, traceID)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(ReleaseCollect) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(ReleaseCollect) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(ReleaseCollect) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		release, err := d.dao.GetReleaseByID(ctx, id)
		if err != nil {
			return err
		}
		if release == nil {
			return errs.ErrReleaseNotFound
		}
		if release.IsAudit() {
			if err = d.dao.CreateReleaseAudit(ctx, release, d.p.EnvSet); err != nil {
				return err
			}
		}
		if release.IsPending() {
			if err = d.instance.ExecRelease(ctx, release); err != nil {
				return err
			}
		}
		if err = progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(ReleaseCollect) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(ReleaseCollect) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *ReleaseCollectScheduler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *ReleaseCollectScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *ReleaseCollectScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(ReleaseCollect) Done")
	return nil
}
