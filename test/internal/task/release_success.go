package task

import (
	"context"
	"fmt"

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

// ReleaseSuccessScheduler 发布成功任务
type ReleaseSuccessScheduler struct {
	dao      dao.Dao
	task     task_scheduler.Task
	instance app.Base
	p        model.ReleaseSuccessParams
}

func initReleaseSuccessScheduler() {
	task_scheduler.Register(
		model.ReleaseSuccessTask,
		func(t task_scheduler.Task, params model.ReleaseSuccessParams) task_scheduler.TaskHandler {
			return &ReleaseSuccessScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *ReleaseSuccessScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	log.DebugContextf(ctx, "task(ReleaseSuccess) Prepare, task: %+v, params: %+v", d.task, d.p)
	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	pkg.WithRequestID(ctx, traceID)
	kv := make(task_scheduler.TaskKV)
	release, err := d.dao.GetReleaseByID(ctx, d.p.VersionID)
	if err != nil {
		return kv, err
	}
	if release == nil {
		return kv, errs.ErrReleaseNotFound
	}
	kv[fmt.Sprintf("%d", release.ID)] = fmt.Sprintf("%d", release.ID)
	return kv, nil
}

// Init 初始化
func (d *ReleaseSuccessScheduler) Init(_ context.Context, _ task_scheduler.TaskKV) error {
	return nil
}

// Process 任务处理
func (d *ReleaseSuccessScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(ReleaseSuccess) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(ReleaseSuccess) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(ReleaseSuccess) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(ReleaseSuccess) Finish kv:%s err:%+v", key, err)
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
		if release.IsPublishDone() {
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(ReleaseSuccess) Finish kv:%s err:%+v", key, err)
				return err
			}
			return nil
		}
		if appDB == nil {
			return errs.ErrRobotNotFound
		}
		instance := app.GetApp(appDB.AppType)
		if instance == nil {
			return errs.ErrGetAppFail
		}
		d.instance = instance
		if err = d.instance.Success(ctx, release); err != nil {
			return err
		}
		if err = progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(ReleaseSuccess) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(ReleaseSuccess) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *ReleaseSuccessScheduler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *ReleaseSuccessScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *ReleaseSuccessScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(ReleaseSuccess) Done")
	return nil
}
