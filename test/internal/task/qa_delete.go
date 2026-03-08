package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// QADeleteScheduler 问答删除任务
type QADeleteScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.QADeleteParams
}

func initQADeleteScheduler() {
	task_scheduler.Register(
		model.QADeleteTask,
		func(t task_scheduler.Task, params model.QADeleteParams) task_scheduler.TaskHandler {
			return &QADeleteScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (q *QADeleteScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, q.p.Language)
	log.DebugContextf(ctx, "task(QADelete) Prepare, task: %+v, params: %+v", q.task, q.p)
	kv := make(task_scheduler.TaskKV)
	qas, err := q.dao.GetQADetails(ctx, q.p.CorpID, q.p.RobotID, q.p.QAIDs)
	if err != nil {
		return kv, err
	}
	for _, qa := range qas {
		kv[fmt.Sprintf("%d", qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}
	return kv, nil
}

// Init 初始化
func (q *QADeleteScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, q.p.Language)
	log.InfoContextf(ctx, "task(QADelete) Init start")
	return nil
}

// Process 任务处理
func (q *QADeleteScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(QADelete) Process, task: %+v, params: %+v", q.task, q.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(QADelete) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := q.dao.GetAppByID(ctx, q.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(QADelete) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(QADelete) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		qa, err := q.dao.GetQAByID(ctx, id)
		if err != nil {
			return err
		}
		if err = q.dao.DeleteQA(ctx, qa); err != nil {
			return err
		}
		if err = q.dao.DeleteQASimilar(ctx, qa); err != nil {
			return err
		}
		if err = progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(QADelete) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(QADelete) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (q *QADeleteScheduler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (q *QADeleteScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (q *QADeleteScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(QADelete) Done")
	return nil
}
