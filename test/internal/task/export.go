package task

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"math"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/xuri/excelize/v2"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

// ExportScheduler 导出任务
type ExportScheduler struct {
	dao    dao.Dao
	task   task_scheduler.Task
	params model.ExportParams
}

// ExportTaskHandler 通用导出任务定义
type ExportTaskHandler interface {
	// GetExportTotal 获取导出数据总数
	GetExportTotal(ctx context.Context, corpID, robotID uint64, params string) (uint64, error)
	// GetExportData 分页获取导出数据
	GetExportData(ctx context.Context, corpID, robotID uint64, params string, page, pageSize uint32) (
		[][]string, error)
	// GetExportHeader 获取导出数据表头
	GetExportHeader(ctx context.Context) []string
}

func initExportScheduler() {
	task_scheduler.Register(
		model.TaskExport,
		func(t task_scheduler.Task, params model.ExportParams) task_scheduler.TaskHandler {
			return &ExportScheduler{
				dao:    dao.New(),
				task:   t,
				params: params,
			}
		},
	)
}

// Prepare 数据准备
func (r *ExportScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, r.params.Language)
	var kv task_scheduler.TaskKV
	return kv, nil
}

// Init 初始化
func (r *ExportScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, r.params.Language)
	log.InfoContextf(ctx, "task(export) Init start")
	return nil
}

// Process 任务处理
func (r *ExportScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	exportParams := &model.ExportParams{}
	err := jsoniter.UnmarshalFromString(r.task.Params, exportParams)
	if err != nil {
		log.ErrorContextf(ctx, "导出任务参数解析失败, r.task.Params:%+v err:%+v", r.task.Params, err)
		return err
	}
	exportTaskID := exportParams.TaskID
	corpID := exportParams.CorpID
	robotID := exportParams.RobotID
	taskType := exportParams.TaskType
	// 通用导出
	export, err := r.getExportInfo(taskType, r.dao)
	if err != nil {
		return err
	}
	cosPath, err := r.getExportData(ctx, export, corpID, robotID, exportParams.Params, taskType)
	if err != nil {
		return err
	}
	var result = "success"
	var status = model.TaskExportStatusEnd
	now := time.Now()
	exportTask := model.Export{
		ID:         exportTaskID,
		Status:     status,
		Result:     result,
		Bucket:     r.dao.GetBucket(ctx),
		CosURL:     cosPath,
		UpdateTime: now,
	}
	err = r.dao.UpdateExport(ctx, exportTask)
	if err != nil {
		log.ErrorContextf(ctx, "更新导出任务状态失败, task:%+v err:%+v", exportTaskID, err)
		return err
	}
	return nil
}

func (r *ExportScheduler) createNotice(ctx context.Context, exportTaskInfo *model.Export,
	exportParams *model.ExportParams, result bool) (*model.Notice, error) {
	var noticeContent, noticeSubject, noticeLevel string
	if exportParams.NoticeContent == "" {
		exportParams.NoticeContent = i18nkey.KeyExportStatus
	}
	if result {
		noticeContent = i18n.Translate(ctx, exportParams.NoticeContent, i18n.Translate(ctx, i18nkey.KeySuccess))
		noticeSubject = i18n.Translate(ctx, i18nkey.KeyExportSuccess)
		noticeLevel = model.LevelSuccess
	} else {
		noticeContent = i18n.Translate(ctx, exportParams.NoticeContent, i18n.Translate(ctx, i18nkey.KeyFailure))
		noticeSubject = i18n.Translate(ctx, i18nkey.KeyExportFailure)
		noticeLevel = model.LevelError
	}

	operations := []model.Operation{
		{Typ: model.OpTypeExportQADownload, Params: model.OpParams{CosPath: exportTaskInfo.CosURL}},
		{Typ: model.OpTypeViewDetail, Params: model.OpParams{}},
	}
	noticeOptions := []model.NoticeOption{
		model.WithPageID(exportParams.NoticePageID),
		model.WithLevel(noticeLevel),
		model.WithSubject(noticeSubject),
		model.WithContent(noticeContent),
		model.WithGlobalFlag(),
	}
	notice := model.NewNotice(
		exportParams.NoticeTypeExport, exportTaskInfo.ID, exportParams.CorpID,
		exportParams.RobotID, exportParams.CreateStaffID, noticeOptions...,
	)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "导出任务通知操作序列化失败 exportTaskInfo.ID:%+v err:%+v", exportTaskInfo.ID, err)
		return nil, err
	}
	return notice, nil
}

func (r *ExportScheduler) getExportInfo(taskType uint32, d dao.Dao) (ExportTaskHandler, error) {
	var export ExportTaskHandler
	if taskType == model.ExportUnsatisfiedReplyTaskType {
		export = &dao.ExportUnsatisfiedReplyTask{Dao: d}
	} else if taskType == model.ExportRejectedQuestionTaskType {
		export = &dao.ExportRejectedQuestionTask{Dao: d}
	} else if taskType == model.ExportQaTaskType {
		export = &dao.ExportQaTask{Dao: d}
	} else if taskType == model.ExportQaTaskTypeV1 {
		// 老版本的任务岛主
		export = &dao.ExportQaTaskV1{Dao: d}
	} else if taskType == model.ExportAttributeLabelTaskType {
		export = &dao.ExportAttributeLabelTask{Dao: d}
	} else if taskType == model.ExportSynonymsTaskType {
		export = &dao.ExportSynonymsTask{Dao: d}
	} else {
		return nil, errs.ErrExportTaskTypeNotFound
	}
	return export, nil
}

func (r *ExportScheduler) getExportData(ctx context.Context, export ExportTaskHandler, corpID uint64, robotID uint64,
	params string, taskType uint32) (string, error) {
	var rows [][]string
	headers := export.GetExportHeader(ctx)
	pages := uint32(math.Ceil(float64(config.App().MaxExportCount) / float64(config.App().MaxExportBatchSize)))
	rows = append([][]string{headers}, rows...)
	for page := uint32(1); page <= pages; page++ {
		row, err := export.GetExportData(ctx, corpID, robotID, params, page, config.App().MaxExportBatchSize)
		if err != nil {
			return "", err
		}
		if len(row) == 0 {
			break
		}
		rows = append(rows, row...)
	}
	// 多取了一轮可能会有超过 MaxExportCount 的数据, 限制回 MaxExportCount
	if len(rows) > int(config.App().MaxExportCount) {
		rows = rows[:config.App().MaxExportCount]
	}
	f := excelize.NewFile()
	sheet := "Sheet1"
	for x, row := range rows {
		for y, cell := range row {
			cellName, err := excelize.CoordinatesToCellName(y+1, x+1)
			if err != nil {
				return "", err
			}
			if err = f.SetCellStr(sheet, cellName, cell); err != nil {
				return "", err
			}
		}
	}
	b, err := f.WriteToBuffer()
	if err != nil {
		return "", err
	}
	filename := fmt.Sprintf("export-%d-%d.xlsx", taskType, time.Now().Unix())
	cosPath := r.dao.GetCorpCOSFilePath(ctx, corpID, filename)
	if err = r.dao.PutObject(ctx, b.Bytes(), cosPath); err != nil {
		log.ErrorContextf(ctx, "导出任务上传cos失败, corpID:%+v, robotID:%+v, cosPath:%+v err:%+v", corpID,
			robotID, cosPath, err)
		return "", err
	}
	return cosPath, nil
}

// Fail 任务失败
func (r *ExportScheduler) Fail(ctx context.Context) error {
	exportParams := &model.ExportParams{}
	err := jsoniter.UnmarshalFromString(r.task.Params, exportParams)
	if err != nil {
		log.ErrorContextf(ctx, "导出任务参数解析失败, r.task.Params:%+v err:%+v", r.task.Params, err)
		return err
	}
	taskID := exportParams.TaskID
	exportTaskInfo, err := r.dao.GetExportInfo(ctx, taskID)
	if err != nil {
		return err
	}

	notice, err := r.createNotice(ctx, exportTaskInfo, exportParams, false)
	if err != nil {
		return err
	}
	if err := r.dao.CreateNotice(ctx, notice); err != nil {
		return err
	}

	return nil
}

// Stop 任务停止
func (r *ExportScheduler) Stop(ctx context.Context) error {
	return nil
}

// Done 任务完成回调
func (r *ExportScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(export) recall")
	exportParams := &model.ExportParams{}
	err := jsoniter.UnmarshalFromString(r.task.Params, exportParams)
	if err != nil {
		log.ErrorContextf(ctx, "导出任务参数解析失败, r.task.Params:%+v err:%+v", r.task.Params, err)
		return err
	}
	taskID := exportParams.TaskID
	exportTaskInfo, err := r.dao.GetExportInfo(ctx, taskID)
	if err != nil {
		return err
	}

	notice, err := r.createNotice(ctx, exportTaskInfo, exportParams, true)
	if err != nil {
		return err
	}
	if err := r.dao.CreateNotice(ctx, notice); err != nil {
		return err
	}

	return nil
}
