package scheduler

import (
	"context"
	"os"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/mapx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/pb-go/common"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"

	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

const taskTableName = "t_knowledge_task"

var taskScheduler *task_scheduler.TaskScheduler

// RunTask 定时任务执行
func RunTask(rdb types.AdminRedis, db types.MySQLClient) error {
	runner, err := os.Hostname()
	if err != nil {
		return err
	}
	taskScheduler = task_scheduler.New(
		runner,
		task_scheduler.ScheduleConfig{
			FetchNum:     config.App().TaskFetchNum,
			FetchTimeout: config.App().TaskFetchTimeout,
			FetchPeriod:  config.App().TaskFetchPeriod,
		},
		task_scheduler.NewDataAccessor(rdb, config.App().TaskPrefix, db, taskTableName),
		task_scheduler.WithTaskConfigFn(func(typ task_scheduler.TaskType) task_scheduler.TaskConfig {
			conf, ok := config.App().Tasks[entity.TaskTypeNameMap[typ]]
			if !ok {
				return task_scheduler.DefaultTaskConfig
			}
			return task_scheduler.TaskConfig{
				Runners:           conf.Runners,
				BindRunners:       conf.BindRunners,
				RetryWaitTime:     conf.RetryWaitTime,
				MaxRetry:          conf.MaxRetry,
				Timeout:           conf.Timeout,
				FailTimeout:       conf.FailTimeout,
				Delay:             conf.Delay,
				Batch:             conf.Batch,
				BatchSize:         conf.BatchSize,
				StoppedResumeTime: conf.StoppedResumeTime,
			}
		}),
	)
	log.Infof("start task scheduler (runner:%s), task config: %+v", runner, entity.TaskTypeNameMap)
	return taskScheduler.Run()
}

// NewDocDeleteTask 创建文档删除任务[入库]
func NewDocDeleteTask(ctx context.Context, robotID uint64, params entity.DocDeleteParams) error {
	params.Name = entity.TaskTypeNameMap[entity.DocDeleteTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewDocDeleteTask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocDeleteTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create DocDeleteTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewExportTask 创建Export任务[入库]
func NewExportTask(ctx context.Context, robotID uint64, params entity.ExportParams) error {
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.TaskExport, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		return err
	}
	return nil
}

// NewKbPackageExportTask 创建知识库导出任务[入库]
func NewKbPackageExportTask(ctx context.Context, corpBizID uint64, params entity.ExportKbPackageParams) (uint64, error) {
	params.Language = i18n.GetUserLang(ctx)
	params.Name = "knowledge_base_export"

	logx.I(ctx, "Creating kb package export task, corpBizID: %d, kbIDs: %v", corpBizID, params.KbIDs)

	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(corpBizID), entity.ExportKbPackageTask, entity.TaskMutexNone, params,
	)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "Failed to create kb package export task, corpBizID: %d, error: %v", corpBizID, err)
		return uint64(taskID), err
	}
	logx.I(ctx, "Kb package export task created successfully, corpBizID: %d, taskID: %d, task: %+v", corpBizID, taskID, jsonx.MustMarshalToString(t))
	return uint64(taskID), nil
}

// NewKbPackageImportTask 创建知识库导入任务[入库]
func NewKbPackageImportTask(ctx context.Context, corpBizID uint64, params entity.ImportKbPackageParams) (uint64, error) {
	params.Language = i18n.GetUserLang(ctx)
	params.Name = "knowledge_base_import"

	logx.I(ctx, "Creating kb package import task, corpBizID: %d, importPath: %s", corpBizID, params.ImportAppPackageURL)

	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(corpBizID), entity.ImportKbPackageTask, entity.TaskMutexNone, params,
	)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "Failed to create kb package import task, corpBizID: %d, error: %v", corpBizID, err)
		return uint64(taskID), err
	}
	logx.I(ctx, "Kb package import task created successfully, corpBizID: %d, taskID: %d, task: %+v", corpBizID, taskID, jsonx.MustMarshalToString(t))
	return uint64(taskID), nil
}

// NewQADeleteTask 创建问答删除任务[入库]
func NewQADeleteTask(ctx context.Context, robotID uint64, params entity.QADeleteParams) error {
	params.Name = entity.TaskTypeNameMap[entity.QADeleteTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewQADeleteTask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.QADeleteTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create QADeleteTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocToIndexTask 创建文档生成索引任务[入库]
func NewDocToIndexTask(ctx context.Context, robotID uint64, params entity.DocToIndexParams) error {
	params.Name = entity.TaskTypeNameMap[entity.DocToIndexTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewDocToIndexTask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocToIndexTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create DocToIndexTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocRenameToIndexTask 创建文档重命名后重新生成索引任务[入库]
func NewDocRenameToIndexTask(ctx context.Context, robotID uint64, params entity.DocRenameToIndexParams) error {
	params.Name = entity.TaskTypeNameMap[entity.DocRenameToIndexTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewDocRenameToIndexTask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocRenameToIndexTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create DocRenameToIndexTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocToQATask 创建文档生成问答任务[入库]
func NewDocToQATask(ctx context.Context, robotID uint64, params entity.DocToQAParams) (uint64, error) {
	params.Name = entity.TaskTypeNameMap[entity.DocToQATask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewDocToQATask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocToQATask, entity.TaskMutexNone, params,
	)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "create DocToQATask fail, task: %+v, err: %v", t, err)
		return uint64(taskID), err
	}
	return uint64(taskID), nil
}

// NewExcelToQATask 创建Excel生成问答任务[入库]
func NewExcelToQATask(ctx context.Context, robotID uint64, params entity.ExcelToQAParams) error {
	params.Name = entity.TaskTypeNameMap[entity.ExcelToQATask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewExcelToQATask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.ExcelToQATask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create ExcelToQATask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewAuditSendTask 创建送审任务[入库]
func NewAuditSendTask(ctx context.Context, robotID uint64, params entity.AuditSendParams) error {
	if !config.AuditSwitch() {
		return nil
	}
	params.Name = entity.TaskTypeNameMap[entity.SendAuditTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewAuditSendTask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.SendAuditTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create SendAuditTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewAuditCheckTask 创建送审回调check任务[入库]
func NewAuditCheckTask(ctx context.Context, robotID uint64, params entity.AuditCheckParams) error {
	if !config.AuditSwitch() {
		return nil
	}
	params.Name = entity.TaskTypeNameMap[entity.CheckAuditTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewAuditCheckTask, robotID: %d, params: %+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.CheckAuditTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create CheckAuditTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewReleaseSuccessTask 创建发布成功任务[入库]
func NewReleaseSuccessTask(ctx context.Context, robotID uint64, params entity.ReleaseSuccessParams) error {
	params.Name = entity.TaskTypeNameMap[entity.ReleaseSuccessTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.ReleaseSuccessTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create ReleaseSuccessTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewResourceExpireTask 创建资源包过期后处理任务[入库]
func NewResourceExpireTask(ctx context.Context, params entity.ResExpireParams) error {
	params.Name = entity.TaskTypeNameMap[entity.ResourceExpireTask]
	params.Language = i18n.GetUserLang(ctx)
	// 后台任务, userID取corpID, 以便并行处理
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.CorpID), entity.ResourceExpireTask, entity.TaskMutexResourceExpire, params,
	)
	logx.I(ctx, "NewResourceExpireTask newResourceExpireTask: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create ResourceExpireTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocResumeTask 创建文档恢复任务[入库]
func NewDocResumeTask(ctx context.Context, corpID uint64, robotID uint64, stuffID uint64,
	docExceededTimes []entity.DocExceededTime) error {
	params := entity.DocResumeParams{}
	params.Name = entity.TaskTypeNameMap[entity.DocResumeTask]
	params.Language = i18n.GetUserLang(ctx)
	params.CorpID = corpID
	params.RobotID = robotID
	params.StaffID = stuffID
	params.DocExceededTimes = docExceededTimes
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocResumeTask, entity.TaskMutexNone, params,
	)
	logx.D(ctx, "ResumeDoc newDocResumeTask: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create DocResumeTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewQAResumeTask 创建文档恢复任务[入库]
func NewQAResumeTask(ctx context.Context, corpID uint64, robotID uint64, stuffID uint64,
	qaExceededTimes []entity.QAExceededTime) error {
	params := entity.QAResumeParams{}
	params.Name = entity.TaskTypeNameMap[entity.QAResumeTask]
	params.Language = i18n.GetUserLang(ctx)
	params.CorpID = corpID
	params.RobotID = robotID
	params.StaffID = stuffID
	params.QAExceededTimes = qaExceededTimes
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.QAResumeTask, entity.TaskMutexNone, params,
	)
	logx.D(ctx, "ResumeQA newQAResumeTask: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create QAResumeTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocModifyTask 创建文档编辑任务
func NewDocModifyTask(ctx context.Context, robotID uint64, params entity.DocModifyParams) error {
	params.Name = entity.TaskTypeNameMap[entity.DocModifyTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocModifyTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create DocModifyTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewAttributeLabelUpdateTask 创建属性标签更新任务
func NewAttributeLabelUpdateTask(ctx context.Context, robotID uint64, params label.AttributeLabelUpdateParams) error {
	params.Name = entity.TaskTypeNameMap[entity.AttributeLabelUpdateTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.AttributeLabelUpdateTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create AttributeLabelUpdateTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewSynonymsImportTask 创建同义词导入任务
func NewSynonymsImportTask(ctx context.Context, robotID uint64, params entity.SynonymsImportParams) error {
	params.Name = entity.TaskTypeNameMap[entity.SynonymsImportTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.SynonymsImportTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create SynonymsImportTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newEmbeddingUpgradeTask embedding 升级任务

// GetTasksByAppID 获取应用下所有执行中的任务
func GetTasksByAppID(ctx context.Context, appID uint64) ([]*task_scheduler.Task, error) {
	tasks, err := taskScheduler.GetTasksByUserID(ctx, task_scheduler.UserID(appID))
	if err != nil {
		logx.E(ctx, "get tasks fail, appID: %d, err: %v", appID, err)
		return nil, err
	}
	return tasks, err
}

// StopStopQaTask 停止qa生成任务
func StopStopQaTask(ctx context.Context, taskID uint64) error {
	logx.I(ctx, "stopStopQaTask taskID: %d", taskID)
	if err := taskScheduler.StopTask(ctx, task_scheduler.TaskID(taskID)); err != nil {
		logx.E(ctx, "stopStopQaTask stop fail, task: %+v, err: %v", taskID, err)
		return err
	}
	return nil
}

// NewKnowledgeDeleteTask 创建知识删除任务
func NewKnowledgeDeleteTask(ctx context.Context, params entity.KnowledgeDeleteParams) error {
	params.Name = entity.TaskTypeNameMap[entity.KnowledgeDeleteTask]
	// 后台任务, userID取corpID, 以便并行处理
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.CorpID), entity.KnowledgeDeleteTask, entity.TaskMutexKnowledgeDelete, params,
	)
	logx.I(ctx, "newKnowledgeDeleteTask task: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "newKnowledgeDeleteTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocDiffDataTask 创建文档比对详情任务[入库]
func NewDocDiffDataTask(ctx context.Context, robotID uint64, params entity.DocDiffParams) error {
	params.Name = entity.TaskTypeNameMap[entity.DocDiffDataTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocDiffDataTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create DocDiffDataTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocDiffOperationTask 创建后台处理任务
func NewDocDiffOperationTask(ctx context.Context, params *entity.DocDiffOperationParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.DocDiffOperationTask]
	params.Language = i18n.GetUserLang(ctx)
	params.EnvSet = contextx.Metadata(ctx).EnvSet()

	// 后台任务, userID取corpID, 以便并行处理
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.CorpID), entity.DocDiffOperationTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "newDocDiffOperationTask new task error, %v", err)
		return 0, err
	}
	logx.I(ctx, "newDocDiffOperationTask task: %+v", t)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "newDocDiffOperationTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return taskID, nil
}

// NewBatchUpdateVectorTask 创建批量更新向量任务
func NewBatchUpdateVectorTask(ctx context.Context, appBizID uint64, params entity.BatchUpdateVector) error {
	params.Name = entity.TaskTypeNameMap[entity.BatchUpdateVectorTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.D(ctx, "NewBatchUpdateVectorTask appBizID:%v,params:%+v", appBizID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(appBizID), entity.BatchUpdateVectorTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create newBatchUpdateVectorTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewKnowledgeGenerateSchemaTask 创建知识库生成schema任务
func NewKnowledgeGenerateSchemaTask(ctx context.Context, robotID uint64,
	params *kbEntity.KnowledgeGenerateSchemaParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.KnowledgeGenerateSchemaTask]
	params.Language = i18n.GetUserLang(ctx)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID),
		entity.KnowledgeGenerateSchemaTask, entity.TaskMutexKnowledgeGenerateSchema, params,
	)
	if err != nil {
		logx.E(ctx, "NewKnowledgeGenerateSchemaTask new task error, %v", err)
		return 0, err
	}
	logx.I(ctx, "NewKnowledgeGenerateSchemaTask task: %+v", t)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewKnowledgeGenerateSchemaTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return taskID, nil
}

// NewDocSegInterveneTask 创建解析切分干预任务
func NewDocSegInterveneTask(ctx context.Context, robotID uint64, params entity.DocSegInterveneParams) error {
	params.Name = entity.TaskTypeNameMap[entity.DocSegInterveneTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewDocSegInterveneTask robotID:%v,params:%+v", robotID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), entity.DocSegInterveneTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create NewDocSegInterveneTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// New ReleaseDocTask 创建文档发布任务
func NewReleaseDocTask(ctx context.Context, params *entity.ReleaseDocParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.ReleaseDocTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewReleaseDocTask params: %+v", params)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), entity.ReleaseDocTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewReleaseDocTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewReleaseDocTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewReleaseDocTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// New ReleaseDocQATask 创建问答发布任务
func NewReleaseDocQATask(ctx context.Context, params *entity.ReleaseQAParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.ReleaseDocQATask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewReleaseDocQATask params: %+v", params)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), entity.ReleaseDocQATask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewReleaseDocQATask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewReleaseDocQATask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewReleaseDocQATask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// New ReleaseRejectedQuestionTask 创建拒绝问答发布任务
func NewReleaseRejectedQuestionTask(ctx context.Context, params *entity.ReleaseRejectedQuestionParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.ReleaseRejectedQuestionTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewReleaseRejectedQuestionTask params: %+v", params)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), entity.ReleaseRejectedQuestionTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewReleaseRejectedQuestionTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewReleaseRejectedQuestionTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewReleaseRejectedQuestionTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// New ReleaseLabelTask 创建标签发布任务
func NewReleaseLabelTask(ctx context.Context, params *entity.ReleaseLabelParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.ReleaseLabelTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewReleaseLabelTask params: %+v", params)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), entity.ReleaseLabelTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewReleaseLabelTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewReleaseLabelTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewReleaseLabelTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewReleaseKnowledgeConfigTask 创建知识库配置发布任务
func NewReleaseKnowledgeConfigTask(ctx context.Context, params *entity.ReleaseKnowledgeConfigParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.ReleaseKnowledgeConfigTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewReleaseKnowledgeConfigTask params: %+v", params)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), entity.ReleaseKnowledgeConfigTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewReleaseKnowledgeConfigTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewReleaseKnowledgeConfigTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewReleaseKnowledgeConfigTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewReleaseDBTask 创建数据库发布后台处理任务
func NewReleaseDBTask(ctx context.Context, params *entity.ReleaseDBParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.ReleaseDBTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewReleaseDBTask params: %+v", params)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), entity.ReleaseDBTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewReleaseDBTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewReleaseDBTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewReleaseDBTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewEnableDbSourceTask 创建数据源启用任务
func NewEnableDbSourceTask(ctx context.Context, params *entity.EnableDBSourceParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.EnableDBSourceTask]
	params.Language = i18n.GetUserLang(ctx)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), entity.EnableDBSourceTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewDbSourceTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewDbSourceTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewDbSourceTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewAddTableTask 创建添加数据表任务
func NewAddTableTask(ctx context.Context, params *entity.LearnDBTableParams) (task_scheduler.TaskID, error) {
	params.Name = entity.TaskTypeNameMap[entity.AddDbTableTask]
	params.Language = i18n.GetUserLang(ctx)
	logx.I(ctx, "NewAddTableTask params: %+v", params)
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.DBTableBizID), entity.AddDbTableTask, entity.TaskMutexNone, params,
	)
	if err != nil {
		logx.E(ctx, "NewAddTableTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "NewAddTableTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	logx.I(ctx, "NewAddTableTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewTxDocRefreshTask 创建腾讯文档刷新任务
func NewTxDocRefreshTask(ctx context.Context, tFileInfo []entity.TxDocRefreshTFileInfo) (uint64, error) {
	if len(tFileInfo) == 0 {
		return 0, errs.ErrParams
	}
	params := entity.TxDocRefreshParams{}
	params.Name = entity.TaskTypeNameMap[entity.TxDocRefreshTask]
	params.Language = i18n.GetUserLang(ctx)
	params.EnvSet = contextx.Metadata(ctx).EnvSet()
	tFileInfoParams := make(map[uint64]entity.TxDocRefreshTFileInfo)
	for _, info := range tFileInfo {
		tFileInfoParams[info.DocID] = info
	}
	params.TFileInfo = tFileInfoParams
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(tFileInfo[0].RobotID), entity.TxDocRefreshTask, entity.TaskMutexNone,
		params,
	) // tFileInfo[0].AppPrimaryId
	if err != nil {
		logx.E(ctx, "new test fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "task run test fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return uint64(taskID), nil
}

// NewDocDocSplitRuleModifyTask 创建文档切分规则修改任务
func NewDocDocSplitRuleModifyTask(ctx context.Context, params entity.DocSplitRuleModifyParams) error {
	params.Name = entity.TaskTypeNameMap[entity.DocSplitRuleModifyTask]
	params.Language = i18n.GetUserLang(ctx)
	params.SourceEnvSet = contextx.Metadata(ctx).EnvSet()
	logx.I(ctx, "NewDocDocSplitRuleModifyTask task: %+v", params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppID), entity.DocSplitRuleModifyTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		logx.E(ctx, "create NewDocDocSplitRuleModifyTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewCorpCOSDocRefreshTask 创建客户COS文档刷新任务
func NewCorpCOSDocRefreshTask(ctx context.Context, docs []*docEntity.Doc) (uint64, error) {
	if len(docs) == 0 {
		return 0, errs.ErrParams
	}
	params := entity.CorpCOSDocRefreshParams{}
	params.Name = entity.TaskTypeNameMap[entity.CorpCOSDocRefreshTask]
	params.Language = i18n.GetUserLang(ctx)
	params.EnvSet = contextx.Metadata(ctx).EnvSet()
	params.Docs = docs
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(docs[0].RobotID), entity.CorpCOSDocRefreshTask, entity.TaskMutexNone,
		params,
	)
	if err != nil {
		logx.E(ctx, "new test fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		logx.E(ctx, "task run test fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return uint64(taskID), nil
}

// StopTask 停止任务
func StopTask(ctx context.Context, taskID task_scheduler.TaskID) error {
	err := taskScheduler.StopTask(ctx, taskID)
	if err != nil {
		logx.E(ctx, "StopTask task:%d err:%+v", taskID, err)
		return err
	}
	return nil
}

func NewImportThirdDocTask(ctx context.Context, appBizID, corpBizID uint64, operationAndFileIDMap map[string]uint64) error {

	md := contextx.Metadata(ctx)
	staffID, corpID := md.StaffID(), md.CorpID()
	uin, subUin := md.Uin(), md.SubAccountUin()

	params := entity.MigrateThirdPartyDocParams{
		Name:         entity.TaskTypeNameMap[entity.MigrateThirdDocTask],
		AppBizID:     appBizID,
		CorpID:       corpID,
		CorpBizID:    corpBizID,
		StaffID:      staffID,
		SourceFrom:   uint32(common.SourceFromType_SOURCE_FROM_TYPE_ONEDRIVE),
		Uin:          uin,
		SUin:         subUin,
		OperationIDs: mapx.Values(operationAndFileIDMap),
		Language:     i18n.GetUserLang(ctx),
	}

	task, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(staffID), entity.MigrateThirdDocTask, entity.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, task); err != nil {
		logx.ErrorContext(ctx, "create task failed: %v", err)
		return err
	}
	return nil
}

func NewThirdDocRefreshTask(ctx context.Context, sourceFrom uint32, docFileInfo []*entity.DocRefreshFileInfo) (uint64, error) {
	if len(docFileInfo) == 0 {
		return 0, errs.ErrParams
	}

	params := entity.DocRefreshParams{}
	params.Name = entity.TaskTypeNameMap[entity.RefreshThirdDocTask]
	params.Language = i18n.GetUserLang(ctx)
	params.EnvSet = contextx.Metadata(ctx).EnvSet()
	docFileInfoParams := make(map[uint64]*entity.DocRefreshFileInfo)
	for _, info := range docFileInfo {
		docFileInfoParams[info.DocID] = info
	}

	params.SourceFrom = sourceFrom
	params.FileInfo = docFileInfoParams
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(docFileInfo[0].RobotID), entity.RefreshThirdDocTask, entity.TaskMutexNone,
		params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "new test fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "task run test fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return uint64(taskID), nil
}
