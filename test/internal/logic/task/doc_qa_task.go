package task

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"

	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
)

func (l *Logic) UpdateDocQATasks(ctx context.Context, updateColumns []string, filter *qaEntity.DocQaTaskFilter, docQaTask *qaEntity.DocQATask) error {
	return l.qaLogic.GetDao().UpdateDocQATasks(ctx, updateColumns, filter, docQaTask, nil)

}

// StopQaTask 暂停任务
func (l *Logic) StopQaTask(ctx context.Context, corpID, robotID, taskID uint64, finance bool, modelName string) error {
	if taskID == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 通过db自增ID操作
	docQATask, err := l.GetDocQATaskByID(ctx, taskID, corpID, robotID)
	if err != nil {
		logx.E(ctx, "StopQaTask 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "StopQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, taskID)
		return errs.ErrDocQaTaskNotFound
	}
	if docQATask.Status != qaEntity.DocQATaskStatusGenerating {
		return errs.ErrStopQaTaskStatusFail
	}
	doc, err := l.docLogic.GetDocByID(ctx, docQATask.DocID, robotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	staffID := doc.StaffID // pkg.StaffID(ctx)
	if doc.HasDeleted() {
		return errs.ErrDocHasDeleted
	}

	status := qaEntity.DocQATaskStatusPause
	message := ""
	if finance {
		status = qaEntity.DocQATaskStatusResource
		message = i18nkey.KeyGeneratingNoTokenBalance
	}

	if err := l.qaLogic.GetDao().Query().TDocQaTask.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		if err := l.UpdateDocQATaskStatusTx(ctx, tx, status, docQATask.ID, message); err != nil {
			return err
		}
		err = scheduler.StopStopQaTask(ctx, docQATask.TaskID)
		if err != nil {
			logx.E(ctx, "Failed to StopQaTask. ID=%d err:%+v", docQATask.TaskID, err)
			return err
		}
		doc.IsCreatingQA = false
		doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
		doc.Message = i18nkey.KeyDocumentQAGenerationPaused
		doc.UpdateTime = time.Now()
		if err := l.docLogic.UpdateDocToQACreatingQa(ctx, tx, doc); err != nil {
			return err
		}
		if finance {
			operations := make([]releaseEntity.Operation, 0)
			operations = append(operations, releaseEntity.Operation{Type: releaseEntity.OpTypeDocToQaModelCapacity,
				Params: releaseEntity.OpParams{}})
			operations = append(operations, releaseEntity.Operation{Type: releaseEntity.OpTypeDocToQaModelTopUp,
				Params: releaseEntity.OpParams{}})
			operations = append(operations, releaseEntity.Operation{Type: releaseEntity.OpTypeDocToQaModelSwitchModel,
				Params: releaseEntity.OpParams{}})
			noticeOptions := []releaseEntity.NoticeOption{
				releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
				releaseEntity.WithLevel(releaseEntity.LevelWarning),
				releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyGenerateQAPausedWithName, modelName)),
				releaseEntity.WithForbidCloseFlag(),
			}
			notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
				noticeOptions...)
			if err := notice.SetOperation(operations); err != nil {
				logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
				return err
			}
			if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
				return err
			}
		} else {
			operations := make([]releaseEntity.Operation, 0)
			var noticeOptions []releaseEntity.NoticeOption
			notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
				noticeOptions...)
			if err := notice.SetOperation(operations); err != nil {
				logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
				return err
			}
			if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to StopQaTask. err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocQATaskStatus 更新问答任务状态
func (l *Logic) UpdateDocQATaskStatus(ctx context.Context, status int, id uint64) error {
	/*
		`
			UPDATE
				t_doc_qa_task
			SET
			    status = ?, update_time = NOW()
			WHERE
			    id = ?
		`
	*/
	if id == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 判断status状态是否合法
	switch status {
	case qaEntity.DocQATaskStatusGenerating,
		qaEntity.DocQATaskStatusSuccess,
		qaEntity.DocQATaskStatusPause,
		qaEntity.DocQATaskStatusResource,
		qaEntity.DocQATaskStatusFail,
		qaEntity.DocQATaskStatusCancel:
	default:
		return errs.ErrDocQaTaskStatusFail
	}
	filter := &qaEntity.DocQaTaskFilter{
		ID: id,
	}
	updateColumns := map[string]any{
		qaEntity.DocQaTaskTblColStatus:     status,
		qaEntity.DocQaTaskTblColUpdateTime: time.Now(),
	}

	if err := l.qaLogic.GetDao().BatchUpdateDocQATasks(ctx, filter, updateColumns, nil); err != nil {
		logx.E(ctx, "UpdateDocQATaskStatus error. err: %+v", err)
		return err
	}
	return nil
}

// UpdateDocQATaskToken 更新问答任务使用token
func (l *Logic) UpdateDocQATaskToken(ctx context.Context, inputToken, outputToken, corpID, robotID, id uint64) error {
	/*
		`
			UPDATE
				t_doc_qa_task
			SET
			    input_token = ?,output_token = ?, update_time = NOW()
			WHERE
			    id = ?
		`
	*/
	if id == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	docQATask, err := l.GetDocQATaskByID(ctx, id, corpID, robotID)
	if err != nil {
		logx.E(ctx, "UpdateDocQATaskToken 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "UpdateDocQATaskToken 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, id)
		return errs.ErrDocQaTaskNotFound
	}
	inputToken = inputToken + docQATask.InputToken
	outputToken = outputToken + docQATask.OutputToken

	filter := &qaEntity.DocQaTaskFilter{
		ID: id,
	}
	updateColumns := map[string]any{
		qaEntity.DocQaTaskTblColInputToken:  inputToken,
		qaEntity.DocQaTaskTblColOutputToken: outputToken,
		qaEntity.DocQaTaskTblColUpdateTime:  time.Now(),
	}

	if err := l.qaLogic.GetDao().BatchUpdateDocQATasks(ctx, filter, updateColumns, nil); err != nil {
		logx.E(ctx, "UpdateDocQATaskToken error. err: %+v", err)
		return err
	}
	return nil
}

// UpdateDocQATaskStatusTx 更新问答任务状态
func (l *Logic) UpdateDocQATaskStatusTx(ctx context.Context, tx *gorm.DB, status int, id uint64, msg string) error {
	/*
		// updateQaTaskStatusFail sql更新问答任务状态计费失败信息
		updateQaTaskStatusFail = `
			UPDATE
				t_doc_qa_task
			SET
			    status = ?,message = ? , update_time = NOW()
			WHERE
			    id = ?
	*/
	if id <= 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 判断status状态是否合法
	switch status {
	case qaEntity.DocQATaskStatusGenerating,
		qaEntity.DocQATaskStatusSuccess,
		qaEntity.DocQATaskStatusPause,
		qaEntity.DocQATaskStatusResource,
		qaEntity.DocQATaskStatusFail,
		qaEntity.DocQATaskStatusCancel:
	default:
		return errs.ErrDocQaTaskStatusFail
	}
	filter := &qaEntity.DocQaTaskFilter{
		ID: id,
	}
	updateColumns := map[string]any{
		qaEntity.DocQaTaskTblColStatus:     status,
		qaEntity.DocQaTaskTblColMessage:    msg,
		qaEntity.DocQaTaskTblColUpdateTime: time.Now(),
	}

	if err := l.qaLogic.GetDao().BatchUpdateDocQATasks(ctx, filter, updateColumns, tx); err != nil {
		logx.E(ctx, "UpdateDocQATaskStatusTx for updating fail status error. err: %+v", err)
		return err
	}
	return nil
}

// UpdateDocQATaskSegmentDoneAndQaCount 更新问答任务已完成的切片数量和问答数
func (l *Logic) UpdateDocQATaskSegmentDoneAndQaCount(ctx context.Context, qaCount, segmentCountDone, corpID,
	robotID,
	id uint64) error {
	if id == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	docQATask, err := l.GetDocQATaskByID(ctx, id, corpID, robotID)
	if err != nil {
		logx.E(ctx, "UpdateDocQATaskSegmentCountDone 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "UpdateDocQATaskSegmentCountDone 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, id)
		return errs.ErrDocQaTaskNotFound
	}

	/*
		`
			UPDATE
				t_doc_qa_task
			SET
			    segment_count_done = ?,qa_count = ? ,stop_next_segment_id = ? , update_time = NOW()
			WHERE
			    id = ?
		`
	*/
	segmentCountDone = segmentCountDone + docQATask.SegmentCountDone
	qaCount = qaCount + docQATask.QACount
	stopNextSegmentIndex := docQATask.StopNextSegmentID + 1

	filter := &qaEntity.DocQaTaskFilter{
		ID: id,
	}
	updateColumns := map[string]any{
		qaEntity.DocQaTaskTblColSegmentCountDone:  segmentCountDone,
		qaEntity.DocQaTaskTblColQaCount:           qaCount,
		qaEntity.DocQaTaskTblColStopNextSegmentId: stopNextSegmentIndex,
		qaEntity.DocQaTaskTblColUpdateTime:        time.Now(),
	}

	if err := l.qaLogic.GetDao().BatchUpdateDocQATasks(ctx, filter, updateColumns, nil); err != nil {
		logx.E(ctx, "UpdateDocQATaskSegmentDoneAndQaCount error. err: %+v", err)
		return err
	}
	return nil
}

// ContinueQaTask 继续任务
func (l *Logic) ContinueQaTask(ctx context.Context, corpID, robotID uint64, qaTask *qaEntity.DocQATask) error {
	if qaTask == nil || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	doc, err := l.docLogic.GetDocByID(ctx, qaTask.DocID, robotID)
	if err != nil {
		logx.E(ctx, "ContinueQaTask|GetDocByID fail|err:%+v", err)
		return errs.ErrCreateDocToQATaskFail
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return errs.ErrDocHasDeleted
	}
	staffID := contextx.Metadata(ctx).StaffID()
	now := time.Now()
	if err := l.qaLogic.GetDao().Query().TDocQaTask.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		qaTask.Status = qaEntity.DocQATaskStatusContinue
		taskID, err := l.docLogic.CreateDocToQATask(ctx, doc, qaTask)
		if err != nil {
			logx.E(ctx, "ContinueQaTask|CreateDocToQATask|err:%+v", err)
			return errs.ErrCreateDocToQATaskFail
		}
		qaTask.TaskID = taskID
		logx.I(ctx, "ContinueQaTask|CreateDocToQATask|taskID:%d", taskID)
		qaTask.Message = ""
		if err := l.UpdateDocQAContinueTask(ctx, tx, qaTask); err != nil {
			logx.E(ctx, "ContinueQaTask|UpdateDocQAContinueTask|err:%+v", err)
			return err
		}

		doc.IsCreatingQA = true
		doc.AddProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
		doc.UpdateTime = now
		/*
				`
				UPDATE
					t_doc
				SET
				    batch_id = :batch_id,
				    is_creating_qa = :is_creating_qa,
				    update_time = :update_time
				WHERE
				    id = :id
			`
		*/

		filter := &docEntity.DocFilter{
			ID: doc.ID,
		}

		updateColumns := []string{docEntity.DocTblColIsCreatingQa,
			docEntity.DocTblColBatchId,
			docEntity.DocTblColProcessingFlag,
			docEntity.DocTblColUpdateTime}

		if _, err := l.docLogic.GetDao().UpdateDoc(ctx, updateColumns, filter, doc); err != nil {
			logx.E(ctx, "Failed to updateCreatingQAFlag When ContinueQaTask. err:%+v", err)
			return err
		}

		operations := make([]releaseEntity.Operation, 0)
		noticeOptions := []releaseEntity.NoticeOption{
			releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
			releaseEntity.WithLevel(releaseEntity.LevelInfo),
			releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyContinueGeneratingQAWithName, doc.FileName)),
			releaseEntity.WithForbidCloseFlag(),
		}
		notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
			noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "继续生成问答任务失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocQAContinueTask 更新问答任务继续状态
func (l *Logic) UpdateDocQAContinueTask(ctx context.Context, tx *gorm.DB, qaTask *qaEntity.DocQATask) error {
	/*
		`
			UPDATE
				t_doc_qa_task
			SET
			    task_id = ?,status = ?, message = ? , update_time = NOW()
			WHERE
			    id = ?
		`
	*/
	if qaTask == nil {
		return errs.ErrDocQaTaskNotFound
	}
	filter := &qaEntity.DocQaTaskFilter{
		ID: qaTask.ID,
	}

	updateColumns := map[string]any{
		qaEntity.DocQaTaskTblColTaskId:     qaTask.TaskID,
		qaEntity.DocQaTaskTblColStatus:     qaEntity.DocQATaskStatusGenerating,
		qaEntity.DocQaTaskTblColMessage:    qaTask.Message,
		qaEntity.DocQaTaskTblColUpdateTime: time.Now(),
	}

	if err := l.qaLogic.GetDao().BatchUpdateDocQATasks(ctx, filter, updateColumns, tx); err != nil {
		logx.E(ctx, "UpdateDocQAContinueTask err:%v", err)
		return err
	}
	return nil
}

// UpdateQaSegmentToDocStatusTx 还原切片状态
func (l *Logic) updateQaSegmentToDocStatusTx(ctx context.Context, tx *gorm.DB, docID uint64, batchID int) error {
	/*
		`
			UPDATE
			    t_doc_segment
			SET
			    status = ?,
			    update_time = NOW()
			WHERE
			    doc_id= ? AND batch_id = ? AND is_deleted = ?
		`
	*/

	deleteFlag := segEntity.SegmentIsNotDeleted
	filter := &segEntity.DocSegmentFilter{
		DocID:     docID,
		BatchID:   batchID,
		IsDeleted: &deleteFlag,
	}
	updateColumns := map[string]any{
		segEntity.DocSegmentTblColStatus:     segEntity.SegmentStatusDone,
		segEntity.DocSegmentTblColUpdateTime: time.Now(),
	}

	if err := l.segLogic.GetDao().BatchUpdateDocSegmentByFilter(ctx, filter, updateColumns, tx); err != nil {
		logx.E(ctx, "updateQaSegmentToDocStatus error. err:%+v", err)
		return err
	}
	return nil
}

// CancelQaTask 取消任务
func (l *Logic) CancelQaTask(ctx context.Context, corpID, robotID, taskID uint64) error {
	if taskID == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 通过db自增ID操作
	docQATask, err := l.GetDocQATaskByID(ctx, taskID, corpID, robotID)
	if err != nil {
		logx.E(ctx, "CancelQaTask 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.E(ctx, "CancelQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, taskID)
		return errs.ErrDocQaTaskNotFound
	}
	if docQATask.Status != qaEntity.DocQATaskStatusPause && docQATask.Status != qaEntity.DocQATaskStatusGenerating &&
		docQATask.Status != qaEntity.DocQATaskStatusResource {
		return errs.ErrCancelQaTaskStatusFail
	}
	doc, err := l.docLogic.GetDocByID(ctx, docQATask.DocID, robotID)
	if err != nil {
		logx.E(ctx, "CancelQaTask GetDocByID|docID|%d", docQATask.DocID)
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	staffID := contextx.Metadata(ctx).StaffID()
	if err := l.qaLogic.GetDao().Query().TDocQaTask.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		// 发送定时任务停止信号
		err = scheduler.StopStopQaTask(ctx, docQATask.TaskID)
		if err != nil {
			logx.E(ctx, "取消任务ID %d 终止任务,失败 %+v", docQATask.TaskID, err)
			return err
		}
		// 更新任务状态
		if err = l.UpdateDocQATaskStatusTx(ctx, tx, qaEntity.DocQATaskStatusCancel, docQATask.ID, ""); err != nil {
			logx.E(ctx, "取消生成问答任务失败 err:%+v", err)
			return err
		}
		// 还原已完成切片状态
		if err = l.updateQaSegmentToDocStatusTx(ctx, tx, doc.ID, doc.BatchID); err != nil {
			logx.E(ctx, "取消任务, UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
				err, docQATask.TaskID)
			return err
		}

		/*
			`
			UPDATE
				t_doc
			SET
			    batch_id = :batch_id,
			    is_creating_qa = :is_creating_qa,
			    update_time = :update_time
			WHERE
			    id = :id
		*/
		doc.IsCreatingQA = false
		doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
		doc.UpdateTime = time.Now()

		filter := &docEntity.DocFilter{
			ID: doc.ID,
		}

		updateColumns := []string{docEntity.DocTblColIsCreatingQa,
			docEntity.DocTblColBatchId,
			docEntity.DocTblColProcessingFlag,
			docEntity.DocTblColUpdateTime}

		if _, err := l.docLogic.GetDao().UpdateDoc(ctx, updateColumns, filter, doc); err != nil {
			logx.E(ctx, "Failed to updateCreatingQAFlag When ContinueQaTask. err:%+v", err)
			return err
		}

		// querySQL := updateCreatingQAFlag
		// if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
		// 	logx.E(ctx, "取消生成问答任务失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
		// 	return err
		// }
		operations := make([]releaseEntity.Operation, 0)
		noticeOptions := []releaseEntity.NoticeOption{
			releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
			releaseEntity.WithLevel(releaseEntity.LevelSuccess),
			releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyGenerateQATaskCancelledWithName, doc.FileName)),
		}
		notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
			noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "取消生成问答任务失败 err:%+v", err)
		return err
	}
	if err := l.RemoveQaTaskExistsOrgData(ctx, doc.ID); err != nil {
		logx.E(ctx, "取消生成问答任务 清空文档问答任务orgData数据 err:%+v", err)
		return err
	}
	return nil
}

// RemoveQaTaskExistsOrgData 清空文档问答任务orgData数据
func (l *Logic) RemoveQaTaskExistsOrgData(ctx context.Context, docID uint64) error {
	if docID == 0 {
		return errs.ErrDocNotFound
	}
	key := fmt.Sprintf("%s%d", qaEntity.DocQaExistsOrgDataPreFix, docID)
	// 重置orgData去重缓存
	// if _, err := l.rawSqlDao.RedisCli().Do(ctx, "DEL", key); err != nil {
	if _, err := l.docLogic.GetDao().RedisCli().Del(ctx, key).Result(); err != nil {
		logx.E(ctx, "RemoveQaTaskExistsOrgData Redis del failed, err:%+v", err)
		return errs.ErrQaTaskExistsOrgDataFail
	}
	return nil
}

// DeleteQaTask 删除生成问答任务
func (l *Logic) DeleteQaTask(ctx context.Context, corpID, robotID, taskID uint64) error {
	if taskID == 0 || corpID == 0 || robotID == 0 {
		return errs.ErrDocQaTaskNotFound
	}
	// 通过db自增ID操作
	docQATask, err := l.GetDocQATaskByID(ctx, taskID, corpID, robotID)
	if err != nil {
		logx.E(ctx, "DeleteQaTask 获取生成问答任务详情失败 err:%+v", err)
		return err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "DeleteQaTask 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			corpID, robotID, taskID)
		return errs.ErrDocQaTaskNotFound
	}
	/*
		`
			UPDATE
				t_doc_qa_task
			SET
			    is_deleted = ?, update_time = NOW()
			WHERE
			    id = ?
		`
	*/

	filter := &qaEntity.DocQaTaskFilter{
		ID: docQATask.ID,
	}

	updateColumns := map[string]any{
		qaEntity.DocQaTaskTblColIsDeleted:  qaEntity.DocQATaskIsDeleted,
		qaEntity.DocQaTaskTblColUpdateTime: time.Now(),
	}

	if err := l.qaLogic.GetDao().BatchUpdateDocQATasks(ctx, filter, updateColumns, nil); err != nil {
		logx.E(ctx, "DeleteQaTask for updating docQaTask deleted flag error. err: %+v", err)
		return err
	}
	return nil
}

// GetDocQATaskByID 根据ID查询生成问答任务
func (l *Logic) GetDocQATaskByID(ctx context.Context, id, corpID, robotID uint64) (*qaEntity.DocQATask, error) {
	/*
		`
			SELECT
			    %s
			FROM
			    t_doc_qa_task
			WHERE
			    corp_id = ? AND robot_id = ? AND id = ? and is_deleted = ?
		`
	*/
	if id == 0 || corpID == 0 || robotID == 0 {
		return nil, errs.ErrDocQaTaskNotFound
	}

	deleteFlag := qaEntity.DocQATaskIsNotDeleted

	filter := &qaEntity.DocQaTaskFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		ID:        id,
		IsDeleted: &deleteFlag,
	}

	selectColumns := qaEntity.DocQaTaskTblColList

	task, err := l.qaLogic.GetDao().GetDocQaTaskByFilter(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "GetDocQATaskByID err:%+v", err)
		return nil, err
	}
	return task, nil
}

// GetDocQATaskByBusinessID 根据对外查询生成问答任务
func (l *Logic) GetDocQATaskByBusinessID(ctx context.Context, businessID, corpID, robotID uint64) (*qaEntity.DocQATask, error) {
	/*
		`
			SELECT
			    %s
			FROM
			    t_doc_qa_task
			WHERE
			    corp_id = ? AND robot_id = ? AND business_id = ? and is_deleted = ?
		`
	*/
	deleteFlag := qaEntity.DocQATaskIsNotDeleted

	filter := &qaEntity.DocQaTaskFilter{
		CorpId:     corpID,
		RobotId:    robotID,
		BusinessId: businessID,
		IsDeleted:  &deleteFlag,
	}

	selectColumns := qaEntity.DocQaTaskTblColList

	task, err := l.qaLogic.GetDao().GetDocQaTaskByFilter(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "GetDocQATaskByBusinessID err:%+v", err)
		return nil, err
	}
	return task, nil
}

// GetListQaTask 查询文档生成问答任务列表
func (l *Logic) GetListQaTask(ctx context.Context, filter *qaEntity.DocQaTaskFilter) (uint64, []*qaEntity.DocQATask, error) {
	selectColumns := qaEntity.DocQaTaskTblColList

	total, tasks, err := l.qaLogic.GetDao().ListDocQaTasks(ctx, selectColumns, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, tasks, nil
}

// CreateDocQATaskRecord 创建一条文档生成QA任务
func (l *Logic) CreateDocQATaskRecord(ctx context.Context, qaTask *qaEntity.DocQATask, doc *docEntity.Doc) (uint64, error) {
	var lastInsertId uint64
	var err error
	if err = l.qaLogic.GetDao().Query().TDocQaTask.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		lastInsertId, err = l.CreateDocQATask(ctx, tx, qaTask, doc)
		if err != nil {
			logx.E(ctx, "create doc qa task failed, qaTask: %v, doc: %+v, %v", qaTask, doc, err)
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to CreateDocQATaskRecord err:%+v", err)
		return 0, err
	}
	return lastInsertId, nil
}

// CreateDocQATask 创建文档生成QA任务
func (l *Logic) CreateDocQATask(ctx context.Context, tx *gorm.DB, qaTask *qaEntity.DocQATask, doc *docEntity.Doc) (
	uint64, error) {
	if doc == nil || doc.ID == 0 {
		return 0, errs.ErrDocNotFound
	}
	ids, err := l.segLogic.GetSegmentIDByDocIDAndBatchID(ctx, doc.ID, doc.BatchID, doc.RobotID)
	if err != nil {
		logx.E(ctx, "CreateDocQATask|GetSegmentIDByDocIDAndBatchID err:%v", err)
		return 0, errs.ErrGetSegmentFail
	}
	if len(ids) == 0 {
		return 0, errs.ErrSegmentIsEmpty
	}
	now := time.Now()
	qaTask.UpdateTime = now
	qaTask.CreateTime = now
	qaTask.BusinessID = idgen.GetId()
	qaTask.DocID = doc.ID
	qaTask.DocName = doc.GetRealFileName()
	qaTask.DocType = doc.FileType
	qaTask.Status = qaEntity.DocQATaskStatusGenerating
	qaTask.SegmentCount = uint64(len(ids))
	/*
		`
			INSERT INTO
				t_doc_qa_task (%s)
			VALUES
				(null,:business_id,:robot_id,:doc_id,:corp_id,:task_id,:source_id,:doc_name,:doc_type,:qa_count,:segment_count_done,
				 :segment_count, :stop_next_segment_id, :input_token, :output_token, :status, :message, :is_deleted,
				 :update_time, :create_time)
		`
	*/

	err = l.qaLogic.GetDao().CreateDocQATask(ctx, qaTask, tx)
	if err != nil {
		logx.E(ctx, "CreateDocQATask err:%v", err)
		return 0, err
	}
	return qaTask.ID, nil
}

// UpdateDocQATaskID 更新问答任务关联定时任务ID
func (l *Logic) UpdateDocQATaskID(ctx context.Context, tx *gorm.DB, qaTask *qaEntity.DocQATask) error {
	/*
		`
			UPDATE
				t_doc_qa_task
			SET
			    task_id = ?, update_time = NOW()
			WHERE
			    id = ?
		`

	*/
	filter := &qaEntity.DocQaTaskFilter{
		ID: qaTask.ID,
	}

	updateColumns := []string{qaEntity.DocQaTaskTblColTaskId, qaEntity.DocQaTaskTblColUpdateTime}
	if err := l.qaLogic.GetDao().UpdateDocQATasks(ctx, updateColumns, filter, qaTask, tx); err != nil {
		logx.E(ctx, "UpdateDocQATaskID err:%v", err)
		return err
	}
	return nil
}

// GenerateQA 开始生成问答
func (l *Logic) GenerateQA(ctx context.Context, staffID uint64, docs []*docEntity.Doc,
	docQaTask *qaEntity.DocQATask) error {
	now := time.Now()
	if len(docs) == 0 {
		return nil
	}

	db, err := knowClient.GormClient(ctx, qaEntity.DocQaTaskTableName, docs[0].RobotID, docs[0].BusinessID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "Failed to get db client. err:%+v", err)
		return err
	}
	// docDb := knowClient.DBClient(ctx, docTableName, docs[0].AppPrimaryId, []client.Option{}...)
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, doc := range docs {
			// 每次循环创建新的任务对象,避免自增id污染
			qaTask := &qaEntity.DocQATask{
				CorpID:  docQaTask.CorpID,
				RobotID: docQaTask.RobotID,
			}
			lastInsertId, err := l.CreateDocQATask(ctx, tx, qaTask, doc)
			if err != nil {
				logx.E(ctx, "GenerateQA|CreateDocQATask|qaTask|%v|doc|%v|err:%+v", qaTask, doc, err)
				return err
			}
			if lastInsertId == 0 {
				logx.E(ctx, "GenerateQA|CreateDocQATask|fail|qaTask|%v|doc|%v", qaTask, docs)
				return errs.ErrCreateDocToQATaskFail
			}
			qaTask.ID = lastInsertId
			taskID, err := l.docLogic.CreateDocToQATask(ctx, doc, qaTask)
			if err != nil {
				logx.E(ctx, "GenerateQA|CreateDocToQATask|err:%+v", err)
				return errs.ErrCreateDocToQATaskFail
			}
			qaTask.TaskID = taskID
			logx.I(ctx, "GenerateQA|CreateDocToQATask|taskID:%d", taskID)

			if err := l.UpdateDocQATaskID(ctx, tx, qaTask); err != nil {
				logx.E(ctx, "GenerateQA|UpdateDocQATaskID|err:%+v", err)
				return err
			}

			// splitStrategy := ""
			// if _, ok := splitStrategys[doc.ID]; ok {
			//	splitStrategy = splitStrategys[doc.ID]
			// }
			// requestID := contextx.TraceID(ctx)
			// taskID, err := d.SendDocParseCreateQA(ctx, doc, splitStrategy, requestID, robotBizID)
			// if err != nil {
			//	return err
			// }
			// newDocParse := model.DocParse{
			//	DocID:     doc.ID,
			//	CorpPrimaryId:    doc.CorpPrimaryId,
			//	AppPrimaryId:   doc.AppPrimaryId,
			//	StaffID:   doc.StaffID,
			//	RequestID: requestID,
			//	Type:      model.DocParseTaskTypeSplitQA,
			//	OpType:    model.DocParseOpTypeSplit,
			//	Status:    model.DocParseIng,
			//	TaskID:    taskID,
			// }
			// err = d.CreateDocParse(ctx, tx, newDocParse)
			// if err != nil {
			//	return err
			// }
			/*
						 `
					UPDATE
						t_doc
					SET
					    batch_id = :batch_id,
					    is_creating_qa = :is_creating_qa,
					    update_time = :update_time
					WHERE
					    id = :id
				`
			*/

			doc.IsCreatingQA = true
			doc.AddProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
			doc.UpdateTime = now

			filter := &docEntity.DocFilter{
				ID: doc.ID,
			}

			updateColumns := []string{docEntity.DocTblColIsCreatingQa,
				docEntity.DocTblColBatchId,
				docEntity.DocTblColProcessingFlag,
				docEntity.DocTblColUpdateTime}

			if _, err := l.docLogic.GetDao().UpdateDocByTx(ctx, updateColumns, filter, doc, tx); err != nil {
				logx.E(ctx, "Failed to updateCreatingQAFlag When GenerateQA. err:%+v", err)
				return err
			}

			operations := make([]releaseEntity.Operation, 0)
			noticeOptions := []releaseEntity.NoticeOption{
				releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
				releaseEntity.WithLevel(releaseEntity.LevelInfo),
				releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyGeneratingQAWithName, doc.GetRealFileName())),
				releaseEntity.WithForbidCloseFlag(),
			}
			notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
				noticeOptions...)
			if err := notice.SetOperation(operations); err != nil {
				logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
				return err
			}
			if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "开始生成问答失败 err:%+v", err)
		return err
	}
	return nil
}

// GetDocQATaskGenerating 查询文档是否有进行中任务
func (l *Logic) GetDocQATaskGenerating(ctx context.Context, corpID, robotID, docID uint64) (bool,
	error) {
	/*
			`
			SELECT
			    %s
			FROM
			    t_doc_qa_task
			WHERE
			    corp_id = ? AND robot_id = ? AND doc_id = ? and status IN (%s) and is_deleted = ?
		`
	*/
	if docID == 0 || corpID == 0 || robotID == 0 {
		return false, errs.ErrDocQaTaskNotFound
	}

	generatingStatus := []int{qaEntity.DocQATaskStatusGenerating, qaEntity.DocQATaskStatusPause,
		qaEntity.DocQATaskStatusResource, qaEntity.DocQATaskStatusFail}
	deleteFlag := qaEntity.DocQATaskIsNotDeleted

	filter := &qaEntity.DocQaTaskFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		DocId:     []uint64{docID},
		Status:    generatingStatus,
		IsDeleted: &deleteFlag,
	}

	selectColumns := qaEntity.DocQaTaskTblColList

	docQATasks, err := l.qaLogic.GetDao().GetDocQaTaskList(ctx, selectColumns, filter)

	if err != nil {
		logx.E(ctx, "Failed to GetDocQATaskGenerating. err:%+v", err)
		return false, err

	}

	if len(docQATasks) > 0 {
		return true, nil
	}
	return false, nil
}

// GetNoticeQATaskNum 获取生成问答任务中数量
func (l *Logic) GetNoticeQATaskNum(ctx context.Context, corpID, robotID uint64) (uint64, error) {
	/*
		`
				SELECT
					count(*)
				FROM
				    t_doc_qa_task
				WHERE
				   corp_id = ? AND  robot_id = ? AND is_deleted = ? AND status = ?
			`
	*/

	filter := &qaEntity.DocQaTaskFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		IsDeleted: ptrx.Int(qaEntity.DocQATaskIsNotDeleted),
		Status:    []int{qaEntity.DocQATaskStatusGenerating},
	}

	total, err := l.qaLogic.GetDao().GetDocQaTaskListCount(ctx, nil, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetNoticeQATaskNum. err:%+v", err)
		return 0, err
	}
	return uint64(total), nil
}
