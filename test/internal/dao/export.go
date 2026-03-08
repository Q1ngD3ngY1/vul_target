package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	exportField = `
		id,corp_id,robot_id,create_staff_id,task_type,name,params,status,result,bucket,cos_url,update_time,create_time
	`
	createExport = `
		INSERT INTO
			t_export (%s)
		VALUES
			(null,:corp_id,:robot_id,:create_staff_id,:task_type,:name,:params,:status,:result,:bucket,:cos_url,
			:update_time,:create_time)
	`
	exportTaskEnd = `
		UPDATE
			t_export 
		SET
			status = :status,
			result = :result,
			bucket = :bucket,
			cos_url = :cos_url,
			update_time = :update_time
		WHERE
			id = :id
	`
	getExportInfoByID = `
	    SELECT
      		%s
    	FROM
      		t_export 
		WHERE
			id = ?
	`

	getExportTaskInfo = `
	    SELECT
      		%s
    	FROM
      		t_export 
		WHERE
			id = ? AND corp_id = ? and robot_id = ? 
	`
)

// CreateExportTask 新建导出任务
func (d *dao) CreateExportTask(ctx context.Context, corpID, staffID, robotID uint64, exportTask model.Export,
	params model.ExportParams) (uint64, error) {
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		SQL := fmt.Sprintf(createExport, exportField)
		res, err := tx.NamedExecContext(ctx, SQL, exportTask)
		if err != nil {
			log.ErrorContextf(ctx, "新建导出任务失败 exportTask:%+v, err:%+v", exportTask, err)
			return err
		}
		taskID, _ := res.LastInsertId()
		params.TaskID = uint64(taskID)
		if err = newExportTask(ctx, robotID, params); err != nil {
			return err
		}
		// 不满意度导出目前没有横条，这里目前仅针对QA导出。
		operations := make([]model.Operation, 0)
		noticeOptions := []model.NoticeOption{
			model.WithPageID(params.NoticePageID),
			model.WithLevel(model.LevelInfo),
			model.WithContent(i18n.Translate(ctx, params.NoticeContentIng)),
			model.WithForbidCloseFlag(),
		}
		notice := model.NewNotice(params.NoticeTypeExport, uint64(taskID), corpID, robotID, staffID,
			noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := d.createNotice(ctx, tx, notice); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return params.TaskID, nil
}

// UpdateExport 更新导出任务
func (d *dao) UpdateExport(ctx context.Context, export model.Export) error {
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		SQL := exportTaskEnd
		if _, err := tx.NamedExecContext(ctx, SQL, export); err != nil {
			log.ErrorContextf(ctx, "更新导出任务失败 export:%+v, err:%+v", export, err)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// GetExportInfo 获取导出任务信息
func (d *dao) GetExportInfo(ctx context.Context, taskID uint64) (*model.Export, error) {
	export := &model.Export{}
	SQL := fmt.Sprintf(getExportInfoByID, exportField)
	args := []any{taskID}
	if err := d.db.QueryToStruct(ctx, export, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取导出任务信息失败 export:%+v, err:%+v", export, err)
		if mysql.IsNoRowsError(err) {
			return nil, nil
		}
		return nil, err
	}
	return export, nil
}

// GetExportTaskInfo 获取导出任务信息
func (d *dao) GetExportTaskInfo(ctx context.Context, taskID, corpID, robotID uint64) (*model.Export, error) {
	log.DebugContextf(ctx, "GetExportTaskInfo,params: %d, %d, %d", taskID, corpID, robotID)
	sql := fmt.Sprintf(getExportTaskInfo, exportField)
	args := []any{taskID, corpID, robotID}
	export := make([]*model.Export, 0)
	if err := d.db.QueryToStructs(ctx, &export, sql, args...); err != nil {
		log.ErrorContextf(ctx, "获取导出任务信息失败 export:%+v, err:%+v", export, err)
		return nil, err
	}
	if len(export) == 0 {
		return nil, nil
	}
	return export[0], nil
}
