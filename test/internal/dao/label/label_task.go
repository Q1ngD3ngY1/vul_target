package label

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// GetUpdateAttributeTask 获取属性标签任务（Gen风格）
func (d *daoImpl) GetUpdateAttributeTask(ctx context.Context, taskID, corpID, robotID uint64) (*entity.AttributeLabelTask, error) {
	logx.D(ctx, "GetUpdateAttributeTask params: taskID=%d, corpID=%d, robotID=%d",
		taskID, corpID, robotID)
	tbl := d.mysql.TAttributeLabelTask
	db := tbl.WithContext(ctx).Where(
		tbl.ID.Eq(taskID),
		tbl.CorpID.Eq(corpID),
		tbl.RobotID.Eq(robotID),
	)
	ret := &entity.AttributeLabelTask{}
	err := db.Scan(ret)
	if err != nil {
		return nil, fmt.Errorf("GetAttributeKeysDelStatusAndIDs Scan failed: %v", err)
	}
	return ret, nil
}

// CreateAttributeLabelTask 创建属性标签任务（Gen风格）
func (d *daoImpl) createAttributeLabelTask(ctx context.Context, task *entity.AttributeLabelTask) (uint64, error) {
	logx.D(ctx, "createUpdateAttributeTask task.corp id :%v", task.CorpID)
	logx.D(ctx, "createUpdateAttributeTask task.AppPrimaryId id :%v", task.RobotID)
	logx.D(ctx, "createUpdateAttributeTask task.CreateStaffID id :%v", task.CreateStaffID)
	// 1. 参数校验
	if task.CorpID == 0 || task.CreateStaffID == 0 || task.RobotID == 0 {
		logx.E(ctx, "参数错误: corpID=%d, staffID=%d, robotID=%d",
			task.CorpID, task.CreateStaffID, task.RobotID)
		return 0, errs.ErrParams
	}
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTaskTableName, task.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "createAttributeLabelTask  get GormClient err:%v,robotID:%v", err, task.RobotID)
		return 0, err
	}
	// 2. 使用Gen生成的Query
	q := mysqlquery.Use(gormClient).TAttributeLabelTask.WithContext(ctx)
	// 3. 设置创建时间（若模型未自动处理）
	now := time.Now()
	task.CreateTime = now
	task.UpdateTime = now
	taskPo := getAttributeLabelDO2PO(task)
	// 4. 执行插入操作
	err = q.Create(taskPo)
	if err != nil {
		return 0, fmt.Errorf("CreateAttributeLabelTask failed err: %v", err)
	}
	// 5. 返回自增ID
	return taskPo.ID, nil
}

func getAttributeLabelDO2PO(do *entity.AttributeLabelTask) *model.TAttributeLabelTask {
	if do == nil {
		return nil
	}
	return &model.TAttributeLabelTask{
		CorpID:        do.CorpID,
		RobotID:       do.RobotID,
		CreateStaffID: do.CreateStaffID,
		Params:        do.Params,
		Status:        do.Status,
		Message:       do.Message,
		CosURL:        do.CosURL,
		UpdateTime:    time.Now(),
		CreateTime:    time.Now(),
	}
}

// CreateUpdateAttributeTask 添加标签异步任务
func (d *daoImpl) CreateUpdateAttributeTask(ctx context.Context, req *entity.UpdateAttributeLabelReq,
	corpID, staffID, robotID uint64) (uint64, error) {
	logx.D(ctx, "createUpdateAttributeTask req:%#+v", req)
	attributeLabelTask := entity.AttributeLabelTask{
		CorpID:        corpID,
		RobotID:       robotID,
		CreateStaffID: staffID,
		Status:        entity.AttributeLabelTaskStatusPending,
	}
	taskID, err := d.createAttributeLabelTask(ctx, &attributeLabelTask)
	if err != nil {
		logx.E(ctx, "添加标签异步任务失败 err:%+v", err)
		return 0, err
	}
	logx.I(ctx, "createUpdateAttributeTask taskID:%d", taskID)
	return taskID, nil
}

// UpdateAttributeLabelTask 更新属性标签任务（Gen风格）
func (d *daoImpl) UpdateAttributeLabelTask(ctx context.Context, task *entity.AttributeLabelTask) error {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTaskTableName, task.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateAttributeLabelTask  get GormClient err:%v,robotID:%v", err, task.RobotID)
		return err
	}
	// 1. 使用Gen生成的Query
	tbl := mysqlquery.Use(gormClient).TAttributeLabelTask
	q := tbl.WithContext(ctx)
	updateMap := map[string]any{
		tbl.Status.ColumnName().String():  task.Status,
		tbl.Message.ColumnName().String(): task.Message,
		tbl.Params.ColumnName().String():  task.Params,
	}
	// 执行更新（带条件校验）
	_, err = q.Where(
		tbl.ID.Eq(task.ID),
		tbl.CorpID.Eq(task.CorpID),
		tbl.RobotID.Eq(task.RobotID),
	).Updates(updateMap)
	if err != nil {
		return fmt.Errorf("UpdateAttributeLabelTask failed : taskID=%d, robotID=%d, err=%v",
			task.ID, task.RobotID, err)
	}
	logx.I(ctx, "UpdateAttributeLabelTask ok taskID=%d, status=%d", task.ID, task.Status)
	return nil
}
