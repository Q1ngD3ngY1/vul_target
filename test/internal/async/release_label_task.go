package async

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	app "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/encoding/protojson"
)

// ReleaseLabelTaskHandler  标签发布任务
type ReleaseLabelTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.ReleaseLabelParams
}

func registerReleaseLabelTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.ReleaseLabelTask,
		func(t task_scheduler.Task, params entity.ReleaseLabelParams) task_scheduler.TaskHandler {
			return &ReleaseLabelTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

func (d *ReleaseLabelTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(ReleaseLabelTaskHandler) Prepare, task: %+v, params: %+v", d.task, d.p)
	contextx.Metadata(ctx).WithRequestID(contextx.TraceID(ctx))
	kv := make(task_scheduler.TaskKV)
	release, err := d.releaseLogic.GetReleaseByID(ctx, d.p.ReleaseID)
	if err != nil {
		return kv, err
	}
	if release == nil {
		return kv, errs.ErrReleaseNotFound
	}
	kv[fmt.Sprintf("%d", release.ID)] = fmt.Sprintf("%d", release.ID)
	return kv, nil
}

func (d *ReleaseLabelTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

func (d *ReleaseLabelTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(ReleaseLabelTaskHandler) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(ReleaseLabelTaskHandler) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppById(ctx, d.p.AppBizID)
		if err != nil {
			return err
		}
		if appDB == nil {
			return errs.ErrRobotNotFound
		}
		if appDB.AppType != entity.KnowledgeQaAppType {
			return errs.ErrGetAppFail
		}
		if appDB.IsDeleted {
			logx.D(ctx, "task(ReleaseLabelTaskHandler) appDB.HasDeleted()|appID:%d", appDB.PrimaryId)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(ReleaseLabelTaskHandler) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		release, err := d.releaseLogic.GetReleaseByID(ctx, id)
		if err != nil {
			return err
		}
		if release == nil {
			return errs.ErrReleaseNotFound
		}
		if release.IsPublishDone() {
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(ReleaseLabelTaskHandler) Finish (release:%d is publish done) kv:%s err:%+v", key, err)
				return err
			}
			return nil
		}

		releaseLabelDetails, err := d.releaseLogic.GetReleaseLabelByVersionID(ctx, release.RobotID, release.ID)
		if err != nil {
			return err
		}

		logx.I(ctx, "task(ReleaseLabelTaskHandler) Process releaseLabelDetails: %+v", releaseLabelDetails)

		if err := d.releaseLogic.ReleaseLabelDetailSuccess(ctx, release, releaseLabelDetails); err != nil {
			return err
		}

		if err = progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(ReleaseLabelTaskHandler) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(ReleaseLabelTaskHandler) Finish kv:%s", k)
	}
	return nil
}

func (d *ReleaseLabelTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(ReleaseLabelTaskHandler) Done")

	if err := d.releaseLogic.DoSuccessNotifyRelease(ctx, d.p.RobotID, d.p.AppBizID, d.p.ReleaseID, releaseEntity.ReleaseLabelCallback); err != nil {
		return err
	}
	return nil
}

func (d *ReleaseLabelTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "task(ReleaseLabelTaskHandler) fail, task ID: %v, param: %+v", d.task.ID, d.p)
	retryTimes := task_scheduler.DefaultTaskConfig.MaxRetry
	c, ok := config.App().Tasks[entity.TaskTypeNameMap[entity.ReleaseLabelTask]]
	if ok {
		retryTimes = c.MaxRetry
	}
	r := &pb.ContinueTerminatedTaskReq{
		TaskId:      uint64(d.task.ID),
		RetryTimes:  uint64(retryTimes),
		WaitToStart: 0,
	}
	transparent, err := protojson.Marshal(r)
	if err != nil {
		err = fmt.Errorf("task (ReleaseLabelTaskHandler) fail, protojson.Marshal fail, err: %w", err)
		logx.W(ctx, err.Error())
		return err
	}

	logx.I(ctx, "task(ReleaseLabelTaskHandler) fail, SEND FAIL CALLBACK. transparent: %v",
		string(transparent))
	_, err = d.rpc.AppAdmin.ReleaseNotify(ctx, &app.ReleaseNotifyReq{
		RobotId:        d.p.RobotID,
		VersionId:      d.p.ReleaseID,
		IsSuccess:      false,
		Message:        "",
		Transparent:    string(transparent),
		CallbackSource: releaseEntity.ReleaseLabelCallback,
	})
	if err != nil {
		err = fmt.Errorf("task (ReleaseLabelTaskHandler) fail, Fail, err: %w", err)
		logx.W(ctx, err.Error())
		return err
	}
	return nil
}

func (d *ReleaseLabelTaskHandler) Stop(ctx context.Context) error {
	return nil
}
