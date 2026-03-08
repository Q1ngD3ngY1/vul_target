package async

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb/handler"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// KnowledgeDeleteTaskHandler 知识删除任务
type KnowledgeDeleteTaskHandler struct {
	*taskCommon

	task   task_scheduler.Task
	params entity.KnowledgeDeleteParams
	kbDao  kbdao.Dao

	sync.RWMutex
	tableHandlerMap map[string]handler.DeleteHandler
}

// init 初始化
func registerKnowledgeDeleteTaskHandler(tc *taskCommon, kbDao kbdao.Dao) {
	task_scheduler.Register(
		entity.KnowledgeDeleteTask,
		func(task task_scheduler.Task, params entity.KnowledgeDeleteParams) task_scheduler.TaskHandler {
			return &KnowledgeDeleteTaskHandler{
				taskCommon:      tc,
				kbDao:           kbDao,
				task:            task,
				params:          params,
				tableHandlerMap: map[string]handler.DeleteHandler{},
			}
		},
	)
}

// tableDataCount 表数据数量
type tableDataCount struct {
	HandlerType string `json:"handler_type"` // 应用id
	TableName   string `json:"table_name"`   // 表名称
	TableCount  int64  `json:"table_count"`  // 表数量
}

// Prepare 任务准备
func (d *KnowledgeDeleteTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	logx.I(ctx, "KnowledgeDeleteTaskHandler Prepare, task:%+v, params:%+v", d.task, d.params)
	app, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.params.RobotID)
	if err != nil {
		logx.E(ctx, "KnowledgeDeleteTaskHandler Prepare, taskID:%d, traceID:%s, Failed err:%+v",
			d.task.ID, d.task.TraceID, err)
		return nil, err
	}
	if app != nil && !app.HasDeleted() { // 应用未删除不允许删除知识
		logx.E(ctx, "KnowledgeDeleteTaskHandler Prepare, taskID:%d, traceID:%s, robot:%d has not been deleted",
			d.task.ID, d.task.TraceID, d.params.RobotID)
		return nil, fmt.Errorf("robot:%d has not been deleted", d.params.RobotID)
	}
	logx.I(ctx, "KnowledgeDeleteTaskHandler Prepare, taskID:%d, traceID:%s", d.task.ID, d.task.TraceID)
	return task_scheduler.TaskKV{}, nil
}

// Init 数据初始化
func (d *KnowledgeDeleteTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	logx.I(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s", d.task.ID, d.task.TraceID)
	// 统计应用下还未删除的各个资源数量
	// var needDeleteTablesCount int64
	// for tableName, _ := range config.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables {
	//	if strings.HasPrefix(tableName, "t_synonyms") || strings.HasPrefix(tableName, "t_unsatisfied_reply") {
	//		// 同义词和不满意回复表不属于kb-config
	//		logx.I(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s, "+
	//			"table:%s, skip count", d.task.ID, d.task.TraceID, tableName)
	//		continue
	//	}
	//	needDeleteTablesCount = needDeleteTablesCount + 1
	// }
	// logx.I(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s, "+
	//	"needDeleteTablesCount:%d", d.task.ID, d.task.TraceID, needDeleteTablesCount)
	tableCountChan := make(chan tableDataCount, len(config.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables))
	errChan := make(chan error, len(config.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables))
	wg := sync.WaitGroup{}
	for tableName, handlerName := range config.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables {
		logx.I(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s, table:%s, handler:%s",
			d.task.ID, d.task.TraceID, tableName, handlerName)
		deleteHandler, err := handler.GetDeleteHandler(handlerName, d.rpc, d.kbDao, d.docLogic.GetDao())
		if err != nil {
			logx.E(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s, Failed err:%+v",
				d.task.ID, d.task.TraceID, err)
			return err
		}
		wg.Add(1)
		go func(ctx context.Context, tableName string, deleteHandler handler.DeleteHandler) {
			defer gox.Recover()
			defer wg.Done()
			appID := d.params.RobotID
			if handlerName == handler.CorpRobotBizIDDeleteHandler || handlerName == handler.CorpAppBizIDDeleteHandler {
				appID = d.params.AppBizID
			}
			tableCount, err := deleteHandler.CountNeedDeletedData(ctx, d.params.CorpID, appID, tableName)
			if err != nil {
				errChan <- err
				return
			}
			if tableCount > 0 {
				d.Lock()
				d.tableHandlerMap[tableName] = deleteHandler
				d.Unlock()

				tableCountChan <- tableDataCount{
					HandlerType: handlerName,
					TableName:   tableName,
					TableCount:  tableCount,
				}
			}
		}(trpc.CloneContext(ctx), tableName, deleteHandler)
	}
	wg.Wait()
	close(tableCountChan)

	select {
	case err := <-errChan:
		logx.E(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s, Failed err:%+v",
			d.task.ID, d.task.TraceID, err)
		return err
	default:
		for tableCount := range tableCountChan {
			_, ok := config.GetMainConfig().KnowledgeDeleteConfig.NeedDeleteTables[tableCount.TableName]
			if ok && tableCount.TableCount > 0 {
				logx.D(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s, "+
					"table:%s, count:%d", d.task.ID, d.task.TraceID, tableCount.TableName, tableCount.TableCount)
				kv[fmt.Sprintf("%s|%s", tableCount.TableName, tableCount.HandlerType)] = fmt.Sprintf("%d", tableCount.TableCount)
			}
		}
		logx.I(ctx, "KnowledgeDeleteTaskHandler Init, taskID:%d, traceID:%s, kv:%+v",
			d.task.ID, d.task.TraceID, kv)
		return nil
	}
}

// Process 任务处理
func (d *KnowledgeDeleteTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.I(ctx, "KnowledgeDeleteTaskHandler Process, taskID:%d, traceID:%s", d.task.ID, d.task.TraceID)
	for taskK, taskV := range progress.TaskKV(ctx) {
		logx.I(ctx, "KnowledgeDeleteTaskHandler Process, taskID:%d, traceID:%s, Start: k:%s, v:%s",
			d.task.ID, d.task.TraceID, taskK, taskV)
		tableElem := strings.Split(taskK, "|")
		tableName := tableElem[0]
		appID := d.params.RobotID
		if len(tableElem) > 1 {
			handlerName := tableElem[1]
			if handlerName == handler.CorpRobotBizIDDeleteHandler || handlerName == handler.CorpAppBizIDDeleteHandler {
				appID = d.params.AppBizID
			}
		}

		tableCount := cast.ToInt64(taskV)
		deleteHandler := d.tableHandlerMap[tableName]
		if err := deleteHandler.DeleteNeedDeletedData(ctx, d.params.CorpID, appID, tableName, tableCount); err != nil {
			logx.E(ctx, "KnowledgeDeleteTaskHandler Process, taskID:%d, traceID:%s, Failed err:%+v",
				d.task.ID, d.task.TraceID, err)
			return err
		}
	}
	logx.I(ctx, "KnowledgeDeleteTaskHandler Process, taskID:%d, traceID:%s done",
		d.task.ID, d.task.TraceID)
	return nil
}

// Done 任务完成
func (d *KnowledgeDeleteTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "KnowledgeDeleteTaskHandler Done, taskID:%d, traceID:%s", d.task.ID, d.task.TraceID)
	// 回调admin任务状态
	err := d.kbLogic.KnowledgeDeleteResultCallback(ctx, d.params.TaskID, true, "")
	if err != nil {
		return err
	}
	logx.I(ctx, "KnowledgeDeleteTaskHandler Done, taskID:%d, traceID:%s done", d.task.ID, d.task.TraceID)
	return nil
}

// Fail 任务失败
func (d *KnowledgeDeleteTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "KnowledgeDeleteTaskHandler Fail, taskID:%d, traceID:%s", d.task.ID, d.task.TraceID)
	// 回调admin任务状态
	err := d.kbLogic.KnowledgeDeleteResultCallback(ctx, d.params.TaskID, false, d.task.Result)
	if err != nil {
		return err
	}
	logx.I(ctx, "KnowledgeDeleteTaskHandler Fail, taskID:%d, traceID:%s done", d.task.ID, d.task.TraceID)
	return nil
}

// Stop 任务停止
func (d *KnowledgeDeleteTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "KnowledgeDeleteTaskHandler Stop, taskID:%d, traceID:%s", d.task.ID, d.task.TraceID)
	return nil
}
