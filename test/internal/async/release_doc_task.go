package async

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx/auditx"

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

// ReleaseDocTaskHandler Doc 发布任务
// ? t_release_db_source 和 t_release_db_table本身快照的状态不更改？
type ReleaseDocTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.ReleaseDocParams
}

func registerReleaseDocTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.ReleaseDocTask,
		func(t task_scheduler.Task, params entity.ReleaseDocParams) task_scheduler.TaskHandler {
			return &ReleaseDocTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

func (d *ReleaseDocTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(ReleaseDocTaskHandler) Prepare, task: %+v, params: %+v", d.task, d.p)
	contextx.Metadata(ctx).WithRequestID(contextx.TraceID(ctx))
	kv := make(task_scheduler.TaskKV)
	release, err := d.releaseLogic.GetReleaseByID(ctx, d.p.ReleaseID)
	if err != nil {
		return kv, err
	}
	if release == nil {
		return kv, errs.ErrReleaseNotFound
	}

	logx.I(ctx, "task(ReleaseDocTaskHandler) Prepare, (Do CollectReleaseDoc) for release: %+v ", release)
	if err := d.releaseLogic.CollectReleaseDoc(ctx, release.CorpID, release.RobotID, d.p.AppBizID, release.ID); err != nil {
		logx.E(ctx, "task(ReleaseDocTaskHandler) Prepare, (Do CollectReleaseDoc) for release: %+v, err: %+v", release, err)
		return kv, err
	}
	kv[fmt.Sprintf("%d", release.ID)] = fmt.Sprintf("%d", release.ID)
	return kv, nil
}

func (d *ReleaseDocTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	logx.D(ctx, "task(ReleaseDocTaskHandler) Init, task: %+v, params: %+v", d.task, d.p)
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

func (d *ReleaseDocTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(ReleaseDocTaskHandler) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(ReleaseDocTaskHandler) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppById(ctx, d.p.AppBizID)
		if err != nil {
			return err
		}
		if appDB.IsDeleted {
			logx.D(ctx, "task(ReleaseDocTaskHandler) appDB.HasDeleted()|appID:%d", appDB.PrimaryId)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(ReleaseDocTaskHandler) Finish kv:%s err:%+v", key, err)
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
				logx.E(ctx, "task(ReleaseDocTaskHandler) Finish (release:%d is publish done) kv:%s err:%+v", key, err)
				return err
			}
			return nil
		}
		if appDB == nil {
			return errs.ErrRobotNotFound
		}
		if appDB.AppType != entity.KnowledgeQaAppType {
			return errs.ErrGetAppFail
		}

		if err := d.releaseLogic.ReleaseVector(ctx, entity.DocTypeSegment, 0, d.p.TaskReleaseVectorParams); err != nil {
			logx.E(ctx, "task(ReleaseDocTaskHandler) ReleaseVector err:%+v", err)
			return err
		}

		releaseDocs, err := d.releaseLogic.GetReleaseDocByVersion(ctx, release)
		if err != nil {
			return err
		}

		docs, err := d.releaseLogic.GetDocReleasedList(ctx, release.RobotID, releaseDocs)
		if err != nil {
			return err
		}

		if err = d.releaseLogic.ReleaseDocSuccess(ctx, releaseDocs, docs); err != nil {
			logx.E(ctx, "release doc success error:%v", err)
			return err
		}

		for _, doc := range docs {
			auditx.Release(auditx.BizDocument).App(appDB.BizId).Space(appDB.SpaceId).Log(ctx, doc.BusinessID, doc.FileName)
		}

		if err = progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(ReleaseDocTaskHandler) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(ReleaseDocTaskHandler) Finish kv:%s", k)
	}
	return nil
}

func (d *ReleaseDocTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(ReleaseDocTaskHandler) Done")

	if err := d.releaseLogic.DoSuccessNotifyRelease(ctx, d.p.RobotID, d.p.AppBizID, d.p.ReleaseID, releaseEntity.ReleaseDocCallback); err != nil {
		return err
	}
	return nil
}

func (d *ReleaseDocTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "task(ReleaseDocTaskHandler) fail, task ID: %v, param: %+v", d.task.ID, d.p)
	retryTimes := task_scheduler.DefaultTaskConfig.MaxRetry
	c, ok := config.App().Tasks[entity.TaskTypeNameMap[entity.ReleaseDocTask]]
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
		err = fmt.Errorf("task (ReleaseDocTaskHandler) fail, protojson.Marshal fail, err: %w", err)
		logx.W(ctx, err.Error())
		return err
	}

	logx.I(ctx, "task(ReleaseDocTaskHandler) fail, SEND FAIL CALLBACK. transparent: %v",
		string(transparent))
	_, err = d.rpc.AppAdmin.ReleaseNotify(ctx, &app.ReleaseNotifyReq{
		RobotId:        d.p.RobotID,
		VersionId:      d.p.ReleaseID,
		IsSuccess:      false,
		Message:        "",
		Transparent:    string(transparent),
		CallbackSource: releaseEntity.ReleaseDocCallback,
	})
	if err != nil {
		err = fmt.Errorf("task (ReleaseDocTaskHandler) fail, Fail, err: %w", err)
		logx.W(ctx, err.Error())
		return err
	}
	return nil
}

func (d *ReleaseDocTaskHandler) Stop(ctx context.Context) error {
	return nil
}
