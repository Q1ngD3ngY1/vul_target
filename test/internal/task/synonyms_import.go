package task

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"strings"

	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

// TODO(rich): 需要确认key是否唯一
const (
	synonymsImportPrefix = "synonyms:import:"
)

// SynonymsImportScheduler 任务
type SynonymsImportScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.SynonymsImportParams
}

func initSynonymsImportScheduler() {
	task_scheduler.Register(
		model.SynonymsImportTask,
		func(t task_scheduler.Task, params model.SynonymsImportParams) task_scheduler.TaskHandler {
			return &SynonymsImportScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *SynonymsImportScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(SynonymsImport) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	kv[fmt.Sprintf("%s%d", synonymsImportPrefix, d.p.TaskID)] = fmt.Sprintf("%d", d.p.TaskID)
	return kv, nil
}

// Init 初始化
func (d *SynonymsImportScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *SynonymsImportScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(SynonymsImport) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(SynonymsImport) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(SynonymsImport) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(SynonymsImport) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, synonymsImportPrefix) {
			taskInfo, err := d.dao.GetSynonymsTaskInfo(ctx, id, d.p.CorpID, d.p.RobotID)
			if err != nil || taskInfo == nil {
				return errs.ErrSynonymsTaskNotFound
			}
			if err = d.dao.UpdateSynonymsImportTaskStatus(ctx, taskInfo, model.SynonymsTaskStatusRunning); err != nil {
				return err
			}
			log.DebugContextf(ctx, "task(SynonymsImport) ParseExcelAndImportSynonyms taskInfo: %+v", taskInfo)
			url, err := d.dao.ParseExcelAndImportSynonyms(ctx, taskInfo.CosURL, taskInfo.FileName, taskInfo.RobotID,
				taskInfo.CorpID)
			if err != nil && errors.Is(err, errs.ErrSynonymsTaskImportFailWithConflict) {
				taskInfo.ErrorCosURL = url
				if err := d.dao.UpdateSynonymsTaskErrorCosUrl(ctx, taskInfo); err != nil {
					log.ErrorContextf(ctx, "task(SynonymsImport) UpdateSynonymsTaskErrorCosUrl fail, err:%+v", err)
					return err
				}
				log.InfoContextf(ctx, "task(SynonymsImport) ParseExcelAndImportSynonyms fail: %+v, taskInfo %+v",
					err, taskInfo)
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(SynonymsImport) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(SynonymsImport) Finish kv:%s", key)
	}
	return nil
}

// Fail 任务失败
func (d *SynonymsImportScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(SynonymsImport) Fail, task: %+v, params: %+v", d.task, d.p)
	taskInfo, err := d.dao.GetSynonymsTaskInfo(ctx, d.p.TaskID, d.p.CorpID, d.p.RobotID)
	if err != nil || taskInfo == nil {
		return errs.ErrSynonymsTaskNotFound
	}
	if err := d.dao.UpdateSynonymsImportTaskStatus(ctx, taskInfo, model.SynonymsTaskStatusFailed); err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *SynonymsImportScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *SynonymsImportScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(SynonymsImport) Done, task: %+v, params: %+v", d.task, d.p)
	taskInfo, err := d.dao.GetSynonymsTaskInfo(ctx, d.p.TaskID, d.p.CorpID, d.p.RobotID)
	if err != nil || taskInfo == nil {
		return errs.ErrSynonymsTaskNotFound
	}
	// 任务完成了,但导入不一定成功
	if len(taskInfo.ErrorCosURL) > 0 {
		err = d.dao.UpdateSynonymsImportTaskStatus(ctx, taskInfo, model.SynonymsTaskStatusFailed)
	} else {
		err = d.dao.UpdateSynonymsImportTaskStatus(ctx, taskInfo, model.SynonymsTaskStatusSuccess)
	}
	return err
}
