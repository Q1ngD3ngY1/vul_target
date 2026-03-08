package task

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/knowledge"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/knowledge/handler"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/dialogue-platform/common/v3/errors"
	"github.com/spf13/cast"
	"sync"
)

// KnowledgeDeleteScheduler 知识删除任务
type KnowledgeDeleteScheduler struct {
	dao    dao.Dao
	task   task_scheduler.Task
	params model.KnowledgeDeleteParams

	sync.RWMutex
	tableHandlerMap map[string]knowledge.DeleteHandler
}

// init 初始化
func initKnowledgeDeleteScheduler() {
	task_scheduler.Register(
		model.KnowledgeDeleteTask,
		func(task task_scheduler.Task, params model.KnowledgeDeleteParams) task_scheduler.TaskHandler {
			return &KnowledgeDeleteScheduler{
				dao:             dao.New(),
				task:            task,
				params:          params,
				tableHandlerMap: map[string]knowledge.DeleteHandler{},
			}
		},
	)
}

// tableDataCount 表数据数量
type tableDataCount struct {
	TableName  string `json:"table_name"`  // 表名称
	TableCount int64  `json:"table_count"` // 表数量
}

// Prepare 任务准备
func (k *KnowledgeDeleteScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Prepare, task:%+v, params:%+v", k.task, k.params)
	app, err := k.dao.GetAppByID(ctx, k.params.RobotID)
	if err != nil {
		return nil, err
	}
	if !app.HasDeleted() { // 应用未删除不允许删除知识
		return nil, fmt.Errorf("robot:%d has not been deleted", k.params.RobotID)
	}
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Prepare, taskID:%d, traceID:%s", k.task.ID, k.task.TraceID)
	return task_scheduler.TaskKV{}, nil
}

// Init 数据初始化
func (k *KnowledgeDeleteScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Init, taskID:%d, traceID:%s", k.task.ID, k.task.TraceID)
	// 统计应用下还未删除的各个资源数量
	tableCountChan := make(chan tableDataCount,
		len(utilConfig.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables))
	errChan := make(chan error, len(utilConfig.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables))
	wg := sync.WaitGroup{}
	for tableName, handlerName := range utilConfig.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables {
		deleteHandler, err := handler.GetDeleteHandler(handlerName)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(ctx context.Context, tableName string, deleteHandler knowledge.DeleteHandler) {
			defer errors.PanicHandler()
			defer wg.Done()
			tableCount, err := deleteHandler.CountNeedDeletedData(ctx, k.params.CorpID, k.params.RobotID, tableName)
			if err != nil {
				errChan <- err
				return
			}
			if tableCount > 0 {
				k.Lock()
				k.tableHandlerMap[tableName] = deleteHandler
				k.Unlock()

				tableCountChan <- tableDataCount{
					TableName:  tableName,
					TableCount: tableCount,
				}
			}
		}(trpc.CloneContext(ctx), tableName, deleteHandler)
	}
	wg.Wait()
	close(tableCountChan)

	select {
	case err := <-errChan:
		log.ErrorContextf(ctx, "KnowledgeDeleteScheduler Init, taskID:%d, traceID:%s, Failed err:%+v",
			k.task.ID, k.task.TraceID, err)
		return err
	default:
		for tableCount := range tableCountChan {
			_, ok := utilConfig.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables[tableCount.TableName]
			if ok && tableCount.TableCount > 0 {
				log.DebugContextf(ctx, "KnowledgeDeleteScheduler Init, taskID:%d, traceID:%s, "+
					"table:%s, count:%d", k.task.ID, k.task.TraceID, tableCount.TableName, tableCount.TableCount)
				kv[fmt.Sprintf("%s", tableCount.TableName)] = fmt.Sprintf("%d", tableCount.TableCount)
			}
		}
		log.InfoContextf(ctx, "KnowledgeDeleteScheduler Init, taskID:%d, traceID:%s, kv:%+v",
			k.task.ID, k.task.TraceID, kv)
		return nil
	}
}

// Process 任务处理
func (k *KnowledgeDeleteScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Process, taskID:%d, traceID:%s", k.task.ID, k.task.TraceID)
	for taskK, taskV := range progress.TaskKV(ctx) {
		log.InfoContextf(ctx, "KnowledgeDeleteScheduler Process, taskID:%d, traceID:%s, Start: k:%s, v:%s",
			k.task.ID, k.task.TraceID, taskK, taskV)
		tableName := taskK
		tableCount := cast.ToInt64(taskV)
		deleteHandler := k.tableHandlerMap[tableName]
		if err := deleteHandler.DeleteNeedDeletedData(ctx, k.params.CorpID, k.params.RobotID,
			tableName, tableCount); err != nil {
			log.ErrorContextf(ctx, "KnowledgeDeleteScheduler Process, taskID:%d, traceID:%s, Failed err:%+v",
				k.task.ID, k.task.TraceID, err)
			return err
		}
	}
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Process, taskID:%d, traceID:%s done",
		k.task.ID, k.task.TraceID)
	return nil
}

// Done 任务完成
func (k *KnowledgeDeleteScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Done, taskID:%d, traceID:%s", k.task.ID, k.task.TraceID)
	// 回调admin任务状态
	err := k.dao.KnowledgeDeleteResultCallback(ctx, k.params.TaskID, true, "")
	if err != nil {
		return err
	}
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Done, taskID:%d, traceID:%s done", k.task.ID, k.task.TraceID)
	return nil
}

// Fail 任务失败
func (k *KnowledgeDeleteScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Fail, taskID:%d, traceID:%s", k.task.ID, k.task.TraceID)
	// 回调admin任务状态
	err := k.dao.KnowledgeDeleteResultCallback(ctx, k.params.TaskID, false, k.task.Result)
	if err != nil {
		return err
	}
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Fail, taskID:%d, traceID:%s done", k.task.ID, k.task.TraceID)
	return nil
}

// Stop 任务停止
func (k *KnowledgeDeleteScheduler) Stop(ctx context.Context) error {
	log.InfoContextf(ctx, "KnowledgeDeleteScheduler Stop, taskID:%d, traceID:%s", k.task.ID, k.task.TraceID)
	return nil
}
