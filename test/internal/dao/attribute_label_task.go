package dao

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/client"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	attributeLabelTaskFields = `
        id,corp_id,robot_id,create_staff_id,params,status,message,cos_url,update_time,create_time
    `
	createAttributeTaskLabel = `
        INSERT INTO
            t_attribute_label_task (%s)
        VALUES 
            (null,:corp_id,:robot_id,:create_staff_id,:params,:status,:message,:cos_url,NOW(),NOW())   
    `
	getAttributeTaskInfo = `
	    SELECT
      		%s
    	FROM
      		t_attribute_label_task 
		WHERE
			id = ? AND corp_id = ? and robot_id = ? 
	`
	updateAttributeLabelTask = `
        UPDATE 
            t_attribute_label_task
        SET 
            status = :status,
            message = :message,
            params = :params
        WHERE
            id = :id AND corp_id = :corp_id AND robot_id = :robot_id
    `
)

const (
	attributeLabelTaskTableName = "t_attribute_label_task"
)

// UpdateAttributeTask 更新标签异步任务
func (d *dao) UpdateAttributeTask(ctx context.Context, attributeLabelTask *model.AttributeLabelTask) error {
	log.DebugContextf(ctx, "UpdateAttributeTask,  params: %+v", attributeLabelTask)
	if err := d.updateAttributeLabelTask(ctx, attributeLabelTask); err != nil {
		log.ErrorContextf(ctx, "编辑更新标签异步任务失败 err:%+v", err)
		return err
	}
	return nil
}

// GetUpdateAttributeTask 获取更新标签异步任务信息
func (d *dao) GetUpdateAttributeTask(ctx context.Context, taskID, corpID, robotID uint64) (
	*model.AttributeLabelTask, error) {
	log.DebugContextf(ctx, "GetUpdateAttributeTask,params: %d, %d, %d", taskID, corpID, robotID)
	sql := fmt.Sprintf(getAttributeTaskInfo, attributeLabelTaskFields)
	args := []any{taskID, corpID, robotID}
	labelTask := make([]*model.AttributeLabelTask, 0)
	if err := d.db.QueryToStructs(ctx, &labelTask, sql, args...); err != nil {
		log.ErrorContextf(ctx, "获取更新标签异步任务信息失败 labelTask:%+v, err:%+v", labelTask, err)
		return nil, err
	}
	if len(labelTask) == 0 {
		return nil, nil
	}
	return labelTask[0], nil
}

// CreateAttributeLabelTask 创建属性标签任务
func (d *dao) createAttributeLabelTask(ctx context.Context, labelsTask *model.AttributeLabelTask) (uint64,
	error) {
	if labelsTask.CorpID <= 0 || labelsTask.CreateStaffID <= 0 || labelsTask.RobotID <= 0 {
		log.ErrorContextf(ctx, "createAttributeLabelTask args err:%+v", labelsTask)
		return 0, errs.ErrParams
	}
	execSQL := fmt.Sprintf(createAttributeTaskLabel, attributeLabelTaskFields)
	db := knowClient.DBClient(ctx, attributeLabelTaskTableName, labelsTask.RobotID, []client.Option{}...)
	res, err := db.NamedExec(ctx, execSQL, labelsTask)
	if err != nil {
		log.ErrorContextf(ctx, "创建标签标准词任务失败 sql:%s args:%+v err:%+v", execSQL, labelsTask, err)
		return 0, err
	}
	taskID, _ := res.LastInsertId()
	return uint64(taskID), nil
}

// updateAttributeLabelTask 更新属性标签任务
func (d *dao) updateAttributeLabelTask(ctx context.Context, labelsTask *model.AttributeLabelTask) error {
	db := knowClient.DBClient(ctx, attributeLabelTaskTableName, labelsTask.RobotID, []client.Option{}...)
	_, err := db.NamedExec(ctx, updateAttributeLabelTask, labelsTask)
	if err != nil {
		log.ErrorContextf(ctx, "更新标签标准词任务 sql:%s args:%+v err:%+v", updateAttributeLabelTask,
			labelsTask, err)
		return err
	}
	return nil
}

// createUpdateAttributeTask 添加标签异步任务
func (d *dao) createUpdateAttributeTask(ctx context.Context, req *model.UpdateAttributeLabelReq,
	corpID, staffID,
	robotID uint64) (uint64, error) {
	log.DebugContextf(ctx, "createUpdateAttributeTask req:%v", req)
	attributeLabelTask := model.AttributeLabelTask{
		CorpID:        corpID,
		RobotID:       robotID,
		CreateStaffID: staffID,
		Status:        model.AttributeLabelTaskStatusPending,
	}
	taskID, err := d.createAttributeLabelTask(ctx, &attributeLabelTask)
	if err != nil {
		log.ErrorContextf(ctx, "添加标签异步任务失败 err:%+v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "createUpdateAttributeTask taskID:%d", taskID)
	return taskID, nil
}
