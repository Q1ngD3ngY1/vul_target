package async

import (
	"context"
	"fmt"
	"math"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/dao/qa"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/logic/export"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/xuri/excelize/v2"
)

// ExportTaskHandler 导出任务
type ExportTaskHandler struct {
	*taskCommon

	task     task_scheduler.Task
	params   entity.ExportParams
	qaDao    qa.Dao
	labelDao label.Dao
}

// ExportTaskHandlerIface 通用导出任务定义
type ExportTaskHandlerIface interface {
	// GetExportTotal 获取导出数据总数
	// GetExportTotal(ctx context.Context, corpID, robotID uint64, params string) (uint64, error)
	// GetExportData 分页获取导出数据
	GetExportData(ctx context.Context, corpID, robotID uint64, params string, page, pageSize uint32) (
		[][]string, error)
	// GetExportHeader 获取导出数据表头
	GetExportHeader(ctx context.Context) []string
}

func registerExportTaskHandler(tc *taskCommon, qaDao qa.Dao, labelDao label.Dao) {
	task_scheduler.Register(
		entity.TaskExport,
		func(t task_scheduler.Task, params entity.ExportParams) task_scheduler.TaskHandler {
			return &ExportTaskHandler{
				taskCommon: tc,
				qaDao:      qaDao,
				labelDao:   labelDao,
				task:       t,
				params:     params,
			}
		},
	)
}

// Prepare 数据准备
func (r *ExportTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, r.params.Language)
	var kv task_scheduler.TaskKV
	return kv, nil
}

// Init 初始化
func (r *ExportTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, r.params.Language)
	logx.I(ctx, "task(export) Init start")
	return nil
}

// Process 任务处理
func (r *ExportTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	exportParams := &entity.ExportParams{}
	err := jsonx.UnmarshalFromString(r.task.Params, exportParams)
	if err != nil {
		logx.E(ctx, "导出任务参数解析失败, r.task.Params:%+v err:%+v", r.task.Params, err)
		return err
	}
	exportTaskID := exportParams.TaskID
	corpID := exportParams.CorpID
	robotID := exportParams.RobotID
	taskType := exportParams.TaskType
	// 通用导出
	export, err := r.getExportInfo(taskType)
	if err != nil {
		return err
	}
	cosPath, err := r.getExportData(ctx, export, corpID, robotID, exportParams.Params, taskType)
	if err != nil {
		return err
	}
	var result = "success"
	var status = qaEntity.TaskExportStatusEnd
	now := time.Now()
	exportTask := &entity.Export{
		ID:         exportTaskID,
		Status:     status,
		Result:     result,
		Bucket:     r.s3.GetBucket(ctx),
		CosURL:     cosPath,
		UpdateTime: now,
	}
	err = r.exportLogic.ModifyExportTask(ctx, exportTask)
	if err != nil {
		logx.E(ctx, "更新导出任务状态失败, task:%+v err:%+v", exportTaskID, err)
		return err
	}
	return nil
}

func (r *ExportTaskHandler) createNotice(ctx context.Context, exportTaskInfo *entity.Export,
	exportParams *entity.ExportParams, result bool) (*releaseEntity.Notice, error) {
	var noticeContent, noticeSubject, noticeLevel string
	if exportParams.NoticeContent == "" {
		exportParams.NoticeContent = i18nkey.KeyExportStatus
	}
	if result {
		noticeContent = i18n.Translate(ctx, exportParams.NoticeContent, i18n.Translate(ctx, i18nkey.KeySuccess))
		noticeSubject = i18n.Translate(ctx, i18nkey.KeyExportSuccess)
		noticeLevel = releaseEntity.LevelSuccess
	} else {
		noticeContent = i18n.Translate(ctx, exportParams.NoticeContent, i18n.Translate(ctx, i18nkey.KeyFailure))
		noticeSubject = i18n.Translate(ctx, i18nkey.KeyExportFailure)
		noticeLevel = releaseEntity.LevelError
	}

	operations := []releaseEntity.Operation{
		{Type: releaseEntity.OpTypeExportQADownload, Params: releaseEntity.OpParams{CosPath: exportTaskInfo.CosURL}},
		{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
	}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(exportParams.NoticePageID),
		releaseEntity.WithLevel(noticeLevel),
		releaseEntity.WithSubject(noticeSubject),
		releaseEntity.WithContent(noticeContent),
		releaseEntity.WithGlobalFlag(),
	}
	notice := releaseEntity.NewNotice(
		exportParams.NoticeTypeExport, exportTaskInfo.ID, exportParams.CorpID,
		exportParams.RobotID, exportParams.CreateStaffID, noticeOptions...,
	)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "导出任务通知操作序列化失败 exportTaskInfo.ID:%+v err:%+v", exportTaskInfo.ID, err)
		return nil, err
	}
	return notice, nil
}

func (r *ExportTaskHandler) getExportInfo(taskType uint32) (ExportTaskHandlerIface, error) {
	var handler ExportTaskHandlerIface
	if taskType == entity.ExportRejectedQuestionTaskType {
		handler = export.NewRejectedQuestionExportLogic()
	} else if taskType == entity.ExportQaTaskType {
		handler = export.NewQaExportLogic(r.qaDao, r.docLogic, r.qaLogic, r.cateLogic, r.rpc)
	} else if taskType == entity.ExportAttributeLabelTaskType {
		handler = export.NewAttributeLabelExportLogic(r.labelDao)
	} else {
		return nil, errs.ErrExportTaskTypeNotFound
	}
	return handler, nil
}

func (r *ExportTaskHandler) getExportData(ctx context.Context, export ExportTaskHandlerIface, corpID uint64, robotID uint64,
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
	filename := r.params.FileName
	if filename == "" {
		filename = fmt.Sprintf("export-%d-%d.xlsx", taskType, time.Now().Unix())
	}
	cosPath := r.s3.GetCorpCOSFilePath(ctx, corpID, filename)
	if err = r.s3.PutObject(ctx, b.Bytes(), cosPath); err != nil {
		logx.E(ctx, "导出任务上传cos失败, corpID:%+v, robotID:%+v, cosPath:%+v err:%+v", corpID,
			robotID, cosPath, err)
		return "", err
	}
	logx.I(ctx, "导出任务上传cos成功, corpID:%+v, robotID:%+v, cosPath:%+v (%d rows, sheet:%s)",
		corpID, robotID, cosPath, len(rows), sheet)

	return cosPath, nil
}

// Fail 任务失败
func (r *ExportTaskHandler) Fail(ctx context.Context) error {
	exportParams := &entity.ExportParams{}
	err := jsonx.UnmarshalFromString(r.task.Params, exportParams)
	if err != nil {
		logx.E(ctx, "导出任务参数解析失败, r.task.Params:%+v err:%+v", r.task.Params, err)
		return err
	}
	taskID := exportParams.TaskID
	exportTaskInfo, err := r.exportLogic.DescribeExportTask(ctx, taskID, 0, 0)
	if err != nil {
		return err
	}

	notice, err := r.createNotice(ctx, exportTaskInfo, exportParams, false)
	if err != nil {
		return err
	}
	if err := r.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}

	return nil
}

// Stop 任务停止
func (r *ExportTaskHandler) Stop(ctx context.Context) error {
	return nil
}

// Done 任务完成回调
func (r *ExportTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(export) recall")
	exportParams := &entity.ExportParams{}
	err := jsonx.UnmarshalFromString(r.task.Params, exportParams)
	if err != nil {
		logx.E(ctx, "导出任务参数解析失败, r.task.Params:%+v err:%+v", r.task.Params, err)
		return err
	}
	taskID := exportParams.TaskID
	exportTaskInfo, err := r.exportLogic.DescribeExportTask(ctx, taskID, 0, 0)
	if err != nil {
		return err
	}

	notice, err := r.createNotice(ctx, exportTaskInfo, exportParams, true)
	if err != nil {
		return err
	}
	if err := r.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}

	return nil
}
