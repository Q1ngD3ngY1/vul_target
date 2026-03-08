// bot-knowledge-config-server
//
// @(#)doc_qa_task.go  星期三, 八月 14, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

package dao

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"github.com/jmoiron/sqlx"
)

const (
	// qaTaskFields 创建文档生成QA任务表字段
	qaTaskFields = `
		id,business_id,robot_id,doc_id,corp_id,task_id,source_id,doc_name,doc_type,qa_count,segment_count_done,segment_count,
        stop_next_segment_id,input_token,output_token,status,message,is_deleted,update_time,create_time
	`

	// createQaTask 创建文档生成QA任务sql
	createQaTask = `
		INSERT INTO
			t_doc_qa_task (%s)
		VALUES
			(null,:business_id,:robot_id,:doc_id,:corp_id,:task_id,:source_id,:doc_name,:doc_type,:qa_count,:segment_count_done,
			 :segment_count, :stop_next_segment_id, :input_token, :output_token, :status, :message, :is_deleted,
			 :update_time, :create_time)
	`

	// updateQaTaskID sql更新问答任务关联定时任务ID
	updateQaTaskID = `
		UPDATE
			t_doc_qa_task
		SET
		    task_id = ?, update_time = NOW()
		WHERE
		    id = ? 
	`

	// updateDocQAContinueTask sql更新问答任务继续状态
	updateDocQAContinueTask = `
		UPDATE
			t_doc_qa_task
		SET
		    task_id = ?,status = ?, message = ? , update_time = NOW()
		WHERE
		    id = ? 
	`
	// updateQaTaskStatus sql更新问答任务状态
	updateQaTaskStatus = `
		UPDATE
			t_doc_qa_task
		SET
		    status = ?, update_time = NOW()
		WHERE
		    id = ? 
	`

	// updateQaTaskStatusFail sql更新问答任务状态计费失败信息
	updateQaTaskStatusFail = `
		UPDATE
			t_doc_qa_task
		SET
		    status = ?,message = ? , update_time = NOW()
		WHERE
		    id = ? 
	`

	// deleteQaTask sql删除问答任务状态
	deleteQaTask = `
		UPDATE
			t_doc_qa_task
		SET
		    is_deleted = ?, update_time = NOW()
		WHERE
		    id = ? 
	`

	// getQaTaskByID 根据ID查询生成问答任务
	getQaTaskByID = `
		SELECT 
		    %s 
		FROM 
		    t_doc_qa_task 
		WHERE 
		    corp_id = ? AND robot_id = ? AND id = ? and is_deleted = ?  
	`
	// getQaTaskByBusinessID 根据对外ID查询生成问答任务
	getQaTaskByBusinessID = `
		SELECT 
		    %s 
		FROM 
		    t_doc_qa_task 
		WHERE 
		    corp_id = ? AND robot_id = ? AND business_id = ? and is_deleted = ? 
	`
	// getDocQATaskGenerating 查询文档是否有进行中任务
	getDocQATaskGenerating = `
		SELECT 
		    %s 
		FROM 
		    t_doc_qa_task 
		WHERE 
		    corp_id = ? AND robot_id = ? AND doc_id = ? and status IN (%s) and is_deleted = ? 
	`

	// updateQaTaskToken sql更新问答任务token
	updateQaTaskToken = `
		UPDATE
			t_doc_qa_task
		SET
		    input_token = ?,output_token = ?, update_time = NOW()
		WHERE
		    id = ? 
	`

	// updateQaTaskSegmentCountDone sql更新问答任务SegmentCount
	updateQaTaskSegmentCountDone = `
		UPDATE
			t_doc_qa_task
		SET
		    segment_count_done = ?,qa_count = ? ,stop_next_segment_id = ? , update_time = NOW()
		WHERE
		    id = ? 
	`

	getListQaTaskCount = `
		SELECT
			count(*)
		FROM
		    t_doc_qa_task
		WHERE
		    corp_id = ? AND robot_id = ? AND is_deleted = ? 
	`

	getListQaTaskList = `
		SELECT
    		%s
		FROM
		    t_doc_qa_task
		WHERE
		    corp_id = ? AND robot_id = ? AND is_deleted = ? 
		ORDER BY
		    create_time DESC,id DESC
		LIMIT ?,?
		`
)

// GetListQaTask 查询文档生成问答任务列表
func (d *dao) GetListQaTask(ctx context.Context, req *model.ListQaTaskReq) (uint64, []*model.DocQATaskList, error) {

	args := make([]any, 0, 3)
	args = append(args, req.CorpID, req.RobotID, model.DocQATaskIsNotDeleted)

	var total uint64
	if err := d.db.Get(ctx, &total, getListQaTaskCount, args...); err != nil {
		log.ErrorContextf(ctx, "获取文档生成问答任务列表失败 sql:%s args:%+v err:%+v", getListQaTaskCount, args, err)
		return 0, nil, err
	}
	querySQL := fmt.Sprintf(getListQaTaskList, qaTaskFields)
	offset := (req.Page - 1) * req.PageSize
	args = append(args, offset, req.PageSize)
	tasks := make([]*model.DocQATaskList, 0)
	log.DebugContextf(ctx, "获取文档生成问答任务列表 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &tasks, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取文档生成问答任务列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	return total, tasks, nil
}

// CreateDocQATaskRecord 创建一条文档生成QA任务
func (d *dao) CreateDocQATaskRecord(ctx context.Context, qaTask *model.DocQATask, doc *model.Doc) (uint64, error) {
	var lastInsertId uint64
	var err error
	if err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		lastInsertId, err = d.CreateDocQATask(ctx, tx, qaTask, doc)
		if err != nil {
			log.ErrorContextf(ctx, "create doc qa task failed, qaTask: %v, doc: %+v, %v", qaTask, doc, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "开始生成问答失败 err:%+v", err)
		return 0, err
	}
	return lastInsertId, nil
}

// CreateDocQATask 创建文档生成QA任务
func (d *dao) CreateDocQATask(ctx context.Context, tx *sqlx.Tx, qaTask *model.DocQATask, doc *model.Doc) (
	uint64, error) {
	if doc == nil || doc.ID == 0 {
		return 0, errs.ErrDocNotFound
	}
	ids, err := d.GetSegmentIDByDocIDAndBatchID(ctx, doc.ID, doc.BatchID, doc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocQATask|GetSegmentIDByDocIDAndBatchID err:%v", err)
		return 0, errs.ErrGetSegmentFail
	}
	if len(ids) == 0 {
		return 0, errs.ErrSegmentIsEmpty
	}
	now := time.Now()
	qaTask.UpdateTime = now
	qaTask.CreateTime = now
	qaTask.BusinessID = d.GenerateSeqID()
	qaTask.DocID = doc.ID
	qaTask.DocName = doc.GetRealFileName()
	qaTask.DocType = doc.FileType
	qaTask.Status = model.DocQATaskStatusGenerating
	qaTask.SegmentCount = uint64(len(ids))

	querySQL := fmt.Sprintf(createQaTask, qaTaskFields)
	res, err := tx.NamedExecContext(ctx, querySQL, qaTask)
	if err != nil {
		log.ErrorContextf(ctx, "创建文档生成QA任务失败 sql:%s args:%+v err:%+v", querySQL, qaTask, err)
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocQATask get last inserted id error, %v", err)
	}
	qaTask.ID = uint64(id)
	return uint64(id), nil
}

// UpdateDocQATaskID 更新问答任务关联定时任务ID
func (d *dao) UpdateDocQATaskID(ctx context.Context, tx *sqlx.Tx, qaTask *model.DocQATask) error {
	if qaTask == nil {
		return errs.ErrDocQaTaskNotFound
	}
	if _, err := tx.ExecContext(ctx, updateQaTaskID, qaTask.TaskID, qaTask.ID); err != nil {
		log.ErrorContextf(ctx, "更新问答任务关联定时任务ID失败, sql: %s, args: %+v, err: %+v",
			updateQaTaskID, qaTask, err)
		return err
	}
	return nil
}

// UpdateDocQAContinueTask 更新问答任务继续状态
func (d *dao) UpdateDocQAContinueTask(ctx context.Context, tx *sqlx.Tx, qaTask *model.DocQATask) error {
	if qaTask == nil {
		return errs.ErrDocQaTaskNotFound
	}
	if _, err := tx.ExecContext(ctx, updateDocQAContinueTask, qaTask.TaskID, model.DocQATaskStatusGenerating,
		qaTask.Message, qaTask.ID); err != nil {
		log.ErrorContextf(ctx, "更新问答任务关联定时任务ID失败, sql: %s, args: %+v, err: %+v",
			updateQaTaskID, qaTask, err)
		return err
	}
	return nil
}

// UpdateDocQATaskStatusTx 更新问答任务状态
func (d *dao) UpdateDocQATaskStatusTx(ctx context.Context, tx *sqlx.Tx, status int, id uint64, msg string) error {
	if id == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 判断status状态是否合法
	switch status {
	case model.DocQATaskStatusGenerating,
		model.DocQATaskStatusSuccess,
		model.DocQATaskStatusPause,
		model.DocQATaskStatusResource,
		model.DocQATaskStatusFail,
		model.DocQATaskStatusCancel:
	default:
		return errs.ErrDocQaTaskStatusFail
	}
	args := make([]any, 0, 3)
	args = append(args, status, msg, id)
	if _, err := tx.ExecContext(ctx, updateQaTaskStatusFail, args...); err != nil {
		log.ErrorContextf(ctx, "更新问答任务状态失败, sql: %s, args: %+v, err: %+v",
			updateQaTaskID, args, err)
		return err
	}
	return nil
}

// UpdateDocQATaskStatus 更新问答任务状态
func (d *dao) UpdateDocQATaskStatus(ctx context.Context, status int, id uint64) error {
	if id == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 判断status状态是否合法
	switch status {
	case model.DocQATaskStatusGenerating,
		model.DocQATaskStatusSuccess,
		model.DocQATaskStatusPause,
		model.DocQATaskStatusResource,
		model.DocQATaskStatusFail,
		model.DocQATaskStatusCancel:
	default:
		return errs.ErrDocQaTaskStatusFail
	}
	args := make([]any, 0, 2)
	args = append(args, status, id)
	if _, err := d.db.Exec(ctx, updateQaTaskStatus, args...); err != nil {
		log.ErrorContextf(ctx, "更新问答任务状态失败, sql: %s, args: %+v, err: %+v",
			updateQaTaskID, args, err)
		return err
	}
	return nil
}

// GetDocQATaskByID 根据ID查询生成问答任务
func (d *dao) GetDocQATaskByID(ctx context.Context, id, corpID, robotID uint64) (*model.DocQATask, error) {
	if id == 0 || corpID == 0 || robotID == 0 {
		return nil, errs.ErrDocQaTaskNotFound
	}
	querySQL := fmt.Sprintf(getQaTaskByID, qaTaskFields)
	args := []any{corpID, robotID, id, model.DocQATaskIsNotDeleted}
	var docQATask []*model.DocQATask
	if err := d.db.QueryToStructs(ctx, &docQATask, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据ID查询生成问答任务详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(docQATask) == 0 {
		return nil, nil
	}
	return docQATask[0], nil
}

// GetDocQATaskByBusinessID 根据对外查询生成问答任务
func (d *dao) GetDocQATaskByBusinessID(ctx context.Context, taskID, corpID, robotID uint64) (*model.DocQATask, error) {
	if taskID == 0 || corpID == 0 || robotID == 0 {
		return nil, errs.ErrDocQaTaskNotFound
	}
	querySQL := fmt.Sprintf(getQaTaskByBusinessID, qaTaskFields)
	args := []any{corpID, robotID, taskID, model.DocQATaskIsNotDeleted}
	var docQATask []*model.DocQATask
	if err := d.db.QueryToStructs(ctx, &docQATask, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据对外查询生成问答任务详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(docQATask) == 0 {
		return nil, nil
	}
	return docQATask[0], nil
}

// GetDocQATaskGenerating 查询文档是否有进行中任务
func (d *dao) GetDocQATaskGenerating(ctx context.Context, corpID, robotID, docID uint64) (bool,
	error) {
	if docID == 0 || corpID == 0 || robotID == 0 {
		return false, errs.ErrDocQaTaskNotFound
	}
	generatingStatus := []int{model.DocQATaskStatusGenerating, model.DocQATaskStatusPause,
		model.DocQATaskStatusResource, model.DocQATaskStatusFail}
	querySQL := fmt.Sprintf(getDocQATaskGenerating, qaTaskFields, placeholder(len(generatingStatus)))
	args := []any{corpID, robotID, docID}
	for _, status := range generatingStatus {
		args = append(args, status)
	}
	args = append(args, model.DocQATaskIsNotDeleted)
	var docQATask []*model.DocQATask
	if err := d.db.QueryToStructs(ctx, &docQATask, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询文档是否有进行中任务失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return false, err
	}
	if len(docQATask) > 0 {
		return true, nil
	}
	return false, nil
}

// UpdateDocQATaskToken 更新问答任务使用token
func (d *dao) UpdateDocQATaskToken(ctx context.Context, inputToken, outputToken, corpID, robotID, id uint64) error {
	if id == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	docQATask, err := d.GetDocQATaskByID(ctx, id, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateDocQATaskToken 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "UpdateDocQATaskToken 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, id)
		return errs.ErrDocQaTaskNotFound
	}
	inputToken = inputToken + docQATask.InputToken
	outputToken = outputToken + docQATask.OutputToken
	args := make([]any, 0, 3)
	args = append(args, inputToken, outputToken, id)
	if _, err := d.db.Exec(ctx, updateQaTaskToken, args...); err != nil {
		log.ErrorContextf(ctx, "更新问答任务使用token失败, sql: %s, args: %+v, err: %+v",
			updateQaTaskID, args, err)
		return err
	}
	return nil
}

// UpdateDocQATaskSegmentDoneAndQaCount 更新问答任务已完成的切片数量和问答数
func (d *dao) UpdateDocQATaskSegmentDoneAndQaCount(ctx context.Context, qaCount, segmentCountDone, corpID,
	robotID,
	id uint64) error {
	if id == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	docQATask, err := d.GetDocQATaskByID(ctx, id, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateDocQATaskSegmentCountDone 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "UpdateDocQATaskSegmentCountDone 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, id)
		return errs.ErrDocQaTaskNotFound
	}

	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		segmentCountDone = segmentCountDone + docQATask.SegmentCountDone
		qaCount = qaCount + docQATask.QACount
		stopNextSegmentIndex := docQATask.StopNextSegmentID + 1
		args := make([]any, 0, 4)
		args = append(args, segmentCountDone, qaCount, stopNextSegmentIndex, id)
		if _, err := tx.Exec(updateQaTaskSegmentCountDone, args...); err != nil {
			log.ErrorContextf(ctx, "更新问答任务已完成的切片数量失败, sql: %s, args: %+v, err: %+v",
				updateQaTaskID, args, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "取消生成问答任务失败 err:%+v", err)
		return err
	}

	return nil
}

// DeleteQaTask 删除生成问答任务
func (d *dao) DeleteQaTask(ctx context.Context, corpID, robotID, taskID uint64) error {
	if taskID == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 通过db自增ID操作
	docQATask, err := d.GetDocQATaskByID(ctx, taskID, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteQaTask 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "DeleteQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, taskID)
		return errs.ErrDocQaTaskNotFound
	}
	args := make([]any, 0, 2)
	args = append(args, model.DocQATaskIsDeleted, docQATask.ID)
	if _, err := d.db.Exec(ctx, deleteQaTask, args...); err != nil {
		log.ErrorContextf(ctx, "删除生成问答任务失败, sql: %s, args: %+v, err: %+v",
			deleteQaTask, args, err)
		return err
	}
	return nil
}

// StopQaTask 暂停任务
func (d *dao) StopQaTask(ctx context.Context, corpID, robotID, taskID uint64, finance bool, modelName string) error {
	if taskID == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 通过db自增ID操作
	docQATask, err := d.GetDocQATaskByID(ctx, taskID, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "StopQaTask 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "StopQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, taskID)
		return errs.ErrDocQaTaskNotFound
	}
	if docQATask.Status != model.DocQATaskStatusGenerating {
		return errs.ErrStopQaTaskStatusFail
	}
	doc, err := d.GetDocByID(ctx, docQATask.DocID, robotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	staffID := doc.StaffID //pkg.StaffID(ctx)
	if doc.HasDeleted() {
		return errs.ErrDocHasDeleted
	}

	status := model.DocQATaskStatusPause
	message := ""
	if finance {
		status = model.DocQATaskStatusResource
		message = i18n.Translate(ctx, i18nkey.KeyGeneratingNoTokenBalance)

		log.InfoContextf(ctx, "StopQaTask 获取生成问答语言 %s 18:%s ", message, string(trpc.GetMetaData(ctx,
			i18n.I18nLang)))
	}

	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := d.UpdateDocQATaskStatusTx(ctx, tx, status, docQATask.ID, message); err != nil {
			return err
		}
		err = stopStopQaTask(ctx, docQATask.TaskID)
		if err != nil {
			log.ErrorContextf(ctx, "任务ID %d 终止任务,失败 %+v", docQATask.TaskID, err)
			return err
		}
		doc.IsCreatingQA = false
		doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
		doc.Message = i18nkey.KeyDocumentQAGenerationPaused
		doc.UpdateTime = time.Now()
		if err := d.UpdateDocToQACreatingQa(ctx, tx, doc); err != nil {
			return err
		}
		if finance {
			operations := make([]model.Operation, 0)
			operations = append(operations, model.Operation{Typ: model.OpTypeDocToQaModelCapacity,
				Params: model.OpParams{}})
			operations = append(operations, model.Operation{Typ: model.OpTypeDocToQaModelPostPaid,
				Params: model.OpParams{}})
			operations = append(operations, model.Operation{Typ: model.OpTypeDocToQaModelTopUp,
				Params: model.OpParams{}})
			operations = append(operations, model.Operation{Typ: model.OpTypeDocToQaModelSwitchModel,
				Params: model.OpParams{}})
			noticeOptions := []model.NoticeOption{
				model.WithPageID(model.NoticeQAPageID),
				model.WithLevel(model.LevelWarning),
				model.WithContent(i18n.Translate(ctx, i18nkey.KeyGenerateQAPausedWithName, modelName)),
				model.WithForbidCloseFlag(),
			}
			notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
				noticeOptions...)
			if err := notice.SetOperation(operations); err != nil {
				log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
				return err
			}
			if err := d.createNotice(ctx, tx, notice); err != nil {
				return err
			}
		} else {
			operations := make([]model.Operation, 0)
			var noticeOptions []model.NoticeOption
			notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
				noticeOptions...)
			if err := notice.SetOperation(operations); err != nil {
				log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
				return err
			}
			if err := d.createNotice(ctx, tx, notice); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "暂停生成问答任务失败 err:%+v", err)
		return err
	}
	return nil
}

// CancelQaTask 取消任务
func (d *dao) CancelQaTask(ctx context.Context, corpID, robotID, taskID uint64) error {
	if taskID == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 通过db自增ID操作
	docQATask, err := d.GetDocQATaskByID(ctx, taskID, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "CancelQaTask 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.ErrorContextf(ctx, "CancelQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, taskID)
		return errs.ErrDocQaTaskNotFound
	}
	if docQATask.Status != model.DocQATaskStatusPause && docQATask.Status != model.DocQATaskStatusGenerating &&
		docQATask.Status != model.DocQATaskStatusResource {
		return errs.ErrCancelQaTaskStatusFail
	}
	doc, err := d.GetDocByID(ctx, docQATask.DocID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "CancelQaTask GetDocByID|docID|%d", docQATask.DocID)
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	staffID := pkg.StaffID(ctx)
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		// 发送定时任务停止信号
		err = stopStopQaTask(ctx, docQATask.TaskID)
		if err != nil {
			log.ErrorContextf(ctx, "取消任务ID %d 终止任务,失败 %+v", docQATask.TaskID, err)
			return err
		}
		// 更新任务状态
		if err = d.UpdateDocQATaskStatusTx(ctx, tx, model.DocQATaskStatusCancel, docQATask.ID, ""); err != nil {
			log.ErrorContextf(ctx, "取消生成问答任务失败 err:%+v", err)
			return err
		}
		// 还原已完成切片状态
		if err = d.UpdateQaSegmentToDocStatusTx(ctx, tx, doc.ID, doc.BatchID); err != nil {
			log.ErrorContextf(ctx, "取消任务, UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
				err, docQATask.TaskID)
			return err
		}
		doc.IsCreatingQA = false
		doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
		doc.UpdateTime = time.Now()
		querySQL := updateCreatingQAFlag
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "取消生成问答任务失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
			return err
		}
		operations := make([]model.Operation, 0)
		noticeOptions := []model.NoticeOption{
			model.WithPageID(model.NoticeQAPageID),
			model.WithLevel(model.LevelSuccess),
			model.WithContent(i18n.Translate(ctx, i18nkey.KeyGenerateQATaskCancelledWithName, doc.FileName)),
		}
		notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
			noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := d.createNotice(ctx, tx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "取消生成问答任务失败 err:%+v", err)
		return err
	}
	if err := d.RemoveQaTaskExistsOrgData(ctx, doc.ID); err != nil {
		log.ErrorContextf(ctx, "取消生成问答任务 清空文档问答任务orgData数据 err:%+v", err)
		return err
	}
	return nil
}

// ContinueQaTask 继续任务
func (d *dao) ContinueQaTask(ctx context.Context, corpID, robotID uint64, qaTask *model.DocQATask, appBizID uint64) error {
	if qaTask == nil || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	doc, err := d.GetDocByID(ctx, qaTask.DocID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "ContinueQaTask|GetDocByID fail|err:%+v", err)
		return errs.ErrCreateDocToQATaskFail
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return errs.ErrDocHasDeleted
	}
	staffID := pkg.StaffID(ctx)
	now := time.Now()
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		qaTask.Status = model.DocQATaskStatusContinue
		taskID, err := d.CreateDocToQATask(ctx, doc, qaTask, appBizID)
		if err != nil {
			log.ErrorContextf(ctx, "ContinueQaTask|CreateDocToQATask|err:%+v", err)
			return errs.ErrCreateDocToQATaskFail
		}
		qaTask.TaskID = taskID
		log.InfoContextf(ctx, "ContinueQaTask|CreateDocToQATask|taskID:%d", taskID)
		qaTask.Message = ""
		if err := d.UpdateDocQAContinueTask(ctx, tx, qaTask); err != nil {
			log.ErrorContextf(ctx, "ContinueQaTask|UpdateDocQAContinueTask|err:%+v", err)
			return err
		}

		doc.IsCreatingQA = true
		doc.AddProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
		doc.UpdateTime = now
		querySQL := updateCreatingQAFlag
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "继续生成问答任务失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
			return err
		}
		operations := make([]model.Operation, 0)
		noticeOptions := []model.NoticeOption{
			model.WithPageID(model.NoticeQAPageID),
			model.WithLevel(model.LevelInfo),
			model.WithContent(i18n.Translate(ctx, i18nkey.KeyContinueGeneratingQAWithName, doc.FileName)),
			model.WithForbidCloseFlag(),
		}
		notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
			noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := d.createNotice(ctx, tx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "继续生成问答任务失败 err:%+v", err)
		return err
	}
	return nil
}

// RemoveQaTaskExistsOrgData 清空文档问答任务orgData数据
func (d *dao) RemoveQaTaskExistsOrgData(ctx context.Context, docID uint64) error {
	if docID == 0 {
		return errs.ErrDocNotFound
	}
	key := fmt.Sprintf("%s%d", model.DocQaExistsOrgDataPreFix, docID)
	// 重置orgData去重缓存
	if _, err := d.RedisCli().Do(ctx, "DEL", key); err != nil {
		log.ErrorContextf(ctx, "RemoveQaTaskExistsOrgData Redis del failed, err:%+v", err)
		return errs.ErrQaTaskExistsOrgDataFail
	}
	return nil
}
