package async

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// QADeleteTaskHandler 问答删除任务
type QADeleteTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.QADeleteParams
}

func registerQADeleteTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.QADeleteTask,
		func(t task_scheduler.Task, params entity.QADeleteParams) task_scheduler.TaskHandler {
			return &QADeleteTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (q *QADeleteTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, q.p.Language)
	logx.D(ctx, "task(QADelete) Prepare, task: %+v, params: %+v", q.task, q.p)
	kv := make(task_scheduler.TaskKV)
	qas, err := q.qaLogic.GetQADetails(ctx, q.p.CorpID, q.p.RobotID, q.p.QAIDs)
	if err != nil {
		return kv, err
	}
	for _, qa := range qas {
		kv[fmt.Sprintf("%d", qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}
	return kv, nil
}

// Init 初始化
func (q *QADeleteTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, q.p.Language)
	logx.I(ctx, "task(QADelete) Init start")
	return nil
}

// Process 任务处理
func (q *QADeleteTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(QADelete) Process, task: %+v, params: %+v", q.task, q.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(QADelete) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := q.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, q.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(QADelete) appDB.HasDeleted()|appID:%d", q.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(QADelete) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		qa, err := q.qaLogic.GetQAByID(ctx, id)
		if err != nil {
			return err
		}
		if err = q.qaLogic.DeleteQA(ctx, qa); err != nil {
			return err
		}
		if err = q.qaLogic.DeleteQASimilarByQA(ctx, qa); err != nil {
			return err
		}
		if err = progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(QADelete) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(QADelete) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (q *QADeleteTaskHandler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (q *QADeleteTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (q *QADeleteTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(QADelete) Done")
	return nil
}
