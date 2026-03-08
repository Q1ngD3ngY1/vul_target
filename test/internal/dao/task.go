package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	// AdminTaskTable knowledge任务表
	AdminTaskTable = `t_knowledge_task`
	// VectorDocTaskTable vectordoc任务表
	VectorDocTaskTable = `t_vector_doc_task`
	taskFields         = `id,user_id,task_type,task_mutex,params,retry_times,max_retry_times,timeout,runner,runner_instance, 
        result,trace_id,start_time,end_time,next_start_time,create_time,update_time
    `
	taskHistoryFields = `
        id,user_id,task_type,task_mutex,params,retry_times,max_retry_times,timeout,runner,runner_instance,
        result,is_success,trace_id,start_time,end_time,next_start_time,create_time
    `
	getTaskTotal = `
        SELECT
            COUNT(1)
        FROM
            %s
        WHERE
            user_id = ? %s
    `
	getTaskList = `
        SELECT 
            %s
        FROM
            %s
        WHERE 
            user_id = ? %s
         ORDER BY 
            id DESC 
        LIMIT ?,?
    `
	getTaskHistoryTotal = `
         SELECT
            COUNT(1)
        FROM
            %s_history
        WHERE
            user_id = ? %s
    `
	getTaskHistoryList = `
         SELECT 
            %s
        FROM
            %s_history
        WHERE 
            user_id = ? %s
         ORDER BY 
            id DESC 
        LIMIT ?,?
    `
)

// GetTaskTotal 获取任务数量
func (d *dao) GetTaskTotal(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32) (
	uint64, error) {
	var total uint64
	args, condition := fillTaskParams(robotID, taskTypes)
	querySQL := fmt.Sprintf(getTaskTotal, tableName, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get task total sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetTaskList 获取任务列表
func (d *dao) GetTaskList(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32, page,
	pageSize uint32) ([]*model.Task, error) {
	var tasks []*model.Task
	args, condition := fillTaskParams(robotID, taskTypes)
	querySQL := fmt.Sprintf(getTaskList, taskFields, tableName, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	if err := d.db.QueryToStructs(ctx, &tasks, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get task list sql:%s args:%+v err:%+v", querySQL, args, err)
		return tasks, err
	}
	return tasks, nil
}

// fillTaskParams TODO
func fillTaskParams(robotID uint64, taskTypes []uint32) ([]any, string) {
	var args []any
	var condition string
	args = append(args, robotID)
	if len(taskTypes) > 0 {
		condition += fmt.Sprintf(" AND task_type IN (%s)", placeholder(len(taskTypes)))
		for _, taskType := range taskTypes {
			args = append(args, taskType)
		}
	}
	return args, condition
}

// GetTaskHistoryTotal 获取历史任务数量
func (d *dao) GetTaskHistoryTotal(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32) (
	uint64, error) {
	var total uint64
	args, condition := fillTaskHistoryParams(robotID, taskTypes)
	querySQL := fmt.Sprintf(getTaskHistoryTotal, tableName, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get task history total sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetTaskHistoryList 获取历史任务列表
func (d *dao) GetTaskHistoryList(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32, page,
	pageSize uint32) ([]*model.TaskHistory, error) {
	var taskHistorys []*model.TaskHistory
	args, condition := fillTaskHistoryParams(robotID, taskTypes)
	querySQL := fmt.Sprintf(getTaskHistoryList, taskHistoryFields, tableName, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	if err := d.db.QueryToStructs(ctx, &taskHistorys, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get task history list sql:%s args:%+v err:%+v", querySQL, args, err)
		return taskHistorys, err
	}
	return taskHistorys, nil
}

// fillTaskHistoryParams TODO
func fillTaskHistoryParams(robotID uint64, taskTypes []uint32) ([]any, string) {
	var args []any
	var condition string
	args = append(args, robotID)
	if len(taskTypes) > 0 {
		condition += fmt.Sprintf(" AND task_type IN (%s)", placeholder(len(taskTypes)))
		for _, taskType := range taskTypes {
			args = append(args, taskType)
		}
	}
	return args, condition
}
