package dao

import (
	"context"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"os"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/trpc-go/trpc-database/goredis/v3"
)

var (
	// taskScheduler 调度器
	taskScheduler *task_scheduler.TaskScheduler
)

// RunTask 定时任务执行
func RunTask() error {
	runner, err := os.Hostname()
	if err != nil {
		return err
	}
	redis, err := goredis.New("redis.qbot.admin")
	if err != nil {
		log.Errorf("go redis new err:%+v", err)
		return err
	}
	db := mysql.NewClientProxy("mysql.qbot.admin")
	taskScheduler = task_scheduler.New(
		runner,
		task_scheduler.ScheduleConfig{
			FetchNum:     config.App().TaskFetchNum,
			FetchTimeout: config.App().TaskFetchTimeout,
			FetchPeriod:  config.App().TaskFetchPeriod,
		},
		task_scheduler.NewDataAccessor(redis, config.App().TaskPrefix, db, "t_knowledge_task"),
		task_scheduler.WithTaskConfigFn(func(typ task_scheduler.TaskType) task_scheduler.TaskConfig {
			conf, ok := config.App().Tasks[model.TaskTypeNameMap[typ]]
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
	log.Info("start task scheduler")
	return taskScheduler.Run()
}

// newDocDeleteTask 创建文档删除任务[入库]
func newDocDeleteTask(ctx context.Context, robotID uint64, params model.DocDeleteParams) error {
	params.Name = model.TaskTypeNameMap[model.DocDeleteTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocDeleteTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create DocDeleteTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newExportTask 创建Export任务[入库]
func newExportTask(ctx context.Context, robotID uint64, params model.ExportParams) error {
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.TaskExport, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		return err
	}
	return nil
}

// newQADeleteTask 创建问答删除任务[入库]
func newQADeleteTask(ctx context.Context, robotID uint64, params model.QADeleteParams) error {
	params.Name = model.TaskTypeNameMap[model.QADeleteTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.QADeleteTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create QADeleteTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newDocToIndexTask 创建文档生成索引任务[入库]
func newDocToIndexTask(ctx context.Context, robotID uint64, params model.DocToIndexParams) error {
	params.Name = model.TaskTypeNameMap[model.DocToIndexTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocToIndexTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create DocToIndexTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newDocRenameToIndexTask 创建文档重命名后重新生成索引任务[入库]
func newDocRenameToIndexTask(ctx context.Context, robotID uint64, params model.DocRenameToIndexParams) error {
	params.Name = model.TaskTypeNameMap[model.DocRenameToIndexTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocRenameToIndexTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create DocRenameToIndexTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newDocToQATask 创建文档生成问答任务[入库]
func newDocToQATask(ctx context.Context, robotID uint64, params model.DocToQAParams) (uint64, error) {
	params.Name = model.TaskTypeNameMap[model.DocToQATask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocToQATask, model.TaskMutexNone, params,
	)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "create DocToQATask fail, task: %+v, err: %v", t, err)
		return uint64(taskID), err
	}
	return uint64(taskID), nil
}

// newExcelToQATask 创建Excel生成问答任务[入库]
func newExcelToQATask(ctx context.Context, robotID uint64, params model.ExcelToQAParams) error {
	params.Name = model.TaskTypeNameMap[model.ExcelToQATask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.ExcelToQATask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create ExcelToQATask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newAuditSendTask 创建送审任务[入库]
func newAuditSendTask(ctx context.Context, robotID uint64, params model.AuditSendParams) error {
	if !config.AuditSwitch() {
		return nil
	}
	params.Name = model.TaskTypeNameMap[model.SendAuditTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.SendAuditTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create SendAuditTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newAuditCheckTask 创建送审回调check任务[入库]
func newAuditCheckTask(ctx context.Context, robotID uint64, params model.AuditCheckParams) error {
	if !config.AuditSwitch() {
		return nil
	}
	params.Name = model.TaskTypeNameMap[model.CheckAuditTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.CheckAuditTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create CheckAuditTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newReleaseCollectTask 创建发布采集任务[入库]
func newReleaseCollectTask(ctx context.Context, robotID uint64, params model.ReleaseCollectParams) error {
	params.Name = model.TaskTypeNameMap[model.ReleaseCollectTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.ReleaseCollectTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create ReleaseCollectTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newReleaseSuccessTask 创建发布成功任务[入库]
func newReleaseSuccessTask(ctx context.Context, robotID uint64, params model.ReleaseSuccessParams) error {
	params.Name = model.TaskTypeNameMap[model.ReleaseSuccessTask]
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.ReleaseSuccessTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create ReleaseSuccessTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newResourceExpireTask 创建资源包过期后处理任务[入库]
func newResourceExpireTask(ctx context.Context, params model.ResExpireParams) error {
	params.Name = model.TaskTypeNameMap[model.ResourceExpireTask]
	params.Language = i18n.GetUserLang(ctx)
	// 后台任务, userID取corpID, 以便并行处理
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.CorpID), model.ResourceExpireTask, model.TaskMutexResourceExpire, params,
	)
	log.InfoContextf(ctx, "NotifyKnowledgeCapacityExpired newResourceExpireTask: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create ResourceExpireTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newDocResumeTask 创建文档恢复任务[入库]
func newDocResumeTask(ctx context.Context, corpID uint64, robotID uint64, stuffID uint64,
	docExceededTimes []model.DocExceededTime) error {
	params := model.DocResumeParams{}
	params.Name = model.TaskTypeNameMap[model.DocResumeTask]
	params.Language = i18n.GetUserLang(ctx)
	params.CorpID = corpID
	params.RobotID = robotID
	params.StaffID = stuffID
	params.DocExceededTimes = docExceededTimes
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocResumeTask, model.TaskMutexNone, params,
	)
	log.DebugContextf(ctx, "ResumeDoc newDocResumeTask: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create DocResumeTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newQAResumeTask 创建文档恢复任务[入库]
func newQAResumeTask(ctx context.Context, corpID uint64, robotID uint64, stuffID uint64,
	qaExceededTimes []model.QAExceededTime) error {
	params := model.QAResumeParams{}
	params.Name = model.TaskTypeNameMap[model.QAResumeTask]
	params.Language = i18n.GetUserLang(ctx)
	params.CorpID = corpID
	params.RobotID = robotID
	params.StaffID = stuffID
	params.QAExceededTimes = qaExceededTimes
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.QAResumeTask, model.TaskMutexNone, params,
	)
	log.DebugContextf(ctx, "ResumeQA newQAResumeTask: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create QAResumeTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewTestTask 创建评测任务
func NewTestTask(ctx context.Context, corpID, robotID, testID, createStaffID uint64) (uint64, error) {
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.TaskTest, model.TestTaskMutex,
		model.TestParams{
			CorpID:  corpID,
			RobotID: robotID,
			TestID:  testID,
		},
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

// stopTestTask 停止评测任务
func stopTestTask(ctx context.Context, taskID uint64) error {
	if err := taskScheduler.StopTask(ctx, task_scheduler.TaskID(taskID)); err != nil {
		log.ErrorContextf(ctx, "task stop test fail, task: %+v, err: %v", taskID, err)
		return err
	}
	return nil
}

// newDocModifyTask 创建文档编辑任务
func newDocModifyTask(ctx context.Context, robotID uint64, params model.DocModifyParams) error {
	params.Name = model.TaskTypeNameMap[model.DocModifyTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocModifyTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create DocModifyTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newAttributeLabelUpdateTask 创建属性标签更新任务
func newAttributeLabelUpdateTask(ctx context.Context, robotID uint64, params model.AttributeLabelUpdateParams) error {
	params.Name = model.TaskTypeNameMap[model.AttributeLabelUpdateTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.AttributeLabelUpdateTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create AttributeLabelUpdateTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newSynonymsImportTask 创建同义词导入任务
func newSynonymsImportTask(ctx context.Context, robotID uint64, params model.SynonymsImportParams) error {
	params.Name = model.TaskTypeNameMap[model.SynonymsImportTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.SynonymsImportTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create SynonymsImportTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// newEmbeddingUpgradeTask embedding 升级任务

// GetTasksByAppID 获取应用下所有执行中的任务
func GetTasksByAppID(ctx context.Context, appID uint64) ([]*task_scheduler.Task, error) {
	tasks, err := taskScheduler.GetTasksByUserID(ctx, task_scheduler.UserID(appID))
	if err != nil {
		log.ErrorContextf(ctx, "get tasks fail, appID: %d, err: %v", appID, err)
		return nil, err
	}
	return tasks, err
}

// stopStopQaTask 停止qa生成任务
func stopStopQaTask(ctx context.Context, taskID uint64) error {
	log.InfoContextf(ctx, "stopStopQaTask taskID: %d", taskID)
	if err := taskScheduler.StopTask(ctx, task_scheduler.TaskID(taskID)); err != nil {
		log.ErrorContextf(ctx, "stopStopQaTask stop fail, task: %+v, err: %v", taskID, err)
		return err
	}
	return nil
}

// newKnowledgeDeleteTask 创建知识删除任务
func newKnowledgeDeleteTask(ctx context.Context, params model.KnowledgeDeleteParams) error {
	params.Name = model.TaskTypeNameMap[model.KnowledgeDeleteTask]
	// 后台任务, userID取corpID, 以便并行处理
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.CorpID), model.KnowledgeDeleteTask, model.TaskMutexKnowledgeDelete, params,
	)
	log.InfoContextf(ctx, "newKnowledgeDeleteTask task: %+v", t)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "newKnowledgeDeleteTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocDiffDataTask 创建文档比对详情任务[入库]
func NewDocDiffDataTask(ctx context.Context, robotID uint64, params model.DocDiffParams) error {
	params.Name = model.TaskTypeNameMap[model.DocDiffDataTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocDiffDataTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create DocDiffDataTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewDocDiffOperationTask 创建后台处理任务
func NewDocDiffOperationTask(ctx context.Context, params *model.DocDiffOperationParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.DocDiffOperationTask]
	params.Language = i18n.GetUserLang(ctx)
	params.EnvSet = getEnvSet(ctx)

	// 后台任务, userID取corpID, 以便并行处理
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.CorpID), model.DocDiffOperationTask, model.TaskMutexNone, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "newDocDiffOperationTask new task error, %v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "newDocDiffOperationTask task: %+v", t)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "newDocDiffOperationTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return taskID, nil
}

// NewSyncAttributeTask 创建后台处理任务
func NewSyncAttributeTask(ctx context.Context, params *model.SyncAttributeParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.SyncAttributeTask]

	// 后台任务, userID取corpID, 以便并行处理
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(0), model.SyncAttributeTask, model.TaskMutexNone, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "NewSyncAttributeTask new task error, %v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "NewSyncAttributeTask task: %+v", t)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "NewSyncAttributeTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return taskID, nil
}

// NewBatchUpdateVectorTask 创建批量更新向量任务
func NewBatchUpdateVectorTask(ctx context.Context, appBizID uint64, params model.BatchUpdateVector) error {
	params.Name = model.TaskTypeNameMap[model.BatchUpdateVectorTask]
	params.Language = i18n.GetUserLang(ctx)
	log.DebugContextf(ctx, "NewBatchUpdateVectorTask appBizID:%v,params:%+v", appBizID, params)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(appBizID), model.BatchUpdateVectorTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create newBatchUpdateVectorTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewKnowledgeGenerateSchemaTask 创建知识库生成schema任务
func NewKnowledgeGenerateSchemaTask(ctx context.Context, robotID uint64,
	params *model.KnowledgeGenerateSchemaParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.KnowledgeGenerateSchemaTask]
	params.Language = i18n.GetUserLang(ctx)

	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID),
		model.KnowledgeGenerateSchemaTask, model.TaskMutexKnowledgeGenerateSchema, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "NewKnowledgeGenerateSchemaTask new task error, %v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "NewKnowledgeGenerateSchemaTask task: %+v", t)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "NewKnowledgeGenerateSchemaTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return taskID, nil
}

// NewDocSegInterveneTask 创建解析切分干预任务
func NewDocSegInterveneTask(ctx context.Context, robotID uint64, params model.DocSegInterveneParams) error {
	params.Name = model.TaskTypeNameMap[model.DocSegInterveneTask]
	params.Language = i18n.GetUserLang(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID), model.DocSegInterveneTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create NewDocSegInterveneTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// NewReleaseDBTask 创建数据库发布后台处理任务
func NewReleaseDBTask(ctx context.Context, params *model.ReleaseDBParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.ReleaseDBTask]
	params.Language = i18n.GetUserLang(ctx)

	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), model.ReleaseDBTask, model.TaskMutexNone, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "NewReleaseDBTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "NewReleaseDBTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	log.InfoContextf(ctx, "NewReleaseDBTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewSyncOrgDataTask 创建org_data同步任务
func NewSyncOrgDataTask(ctx context.Context, params *model.SyncOrgDataParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.SyncOrgDataTask]

	// 后台任务, userID取corpID, 以便并行处理
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(0), model.SyncOrgDataTask, model.TaskMutexNone, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "NewSyncOrgDataTask new task error, %v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "NewSyncOrgDataTask task: %+v", t)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "NewSyncOrgDataTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return taskID, nil
}

// NewEnableDbSourceTask 创建数据源启用任务
func NewEnableDbSourceTask(ctx context.Context, params *model.EnableDBSourceParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.EnableDBSourceTask]
	params.Language = i18n.GetUserLang(ctx)

	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppBizID), model.EnableDBSourceTask, model.TaskMutexNone, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "NewDbSourceTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "NewDbSourceTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	log.InfoContextf(ctx, "NewDbSourceTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewAddTableTask 创建添加数据表任务
func NewAddTableTask(ctx context.Context, params *model.LearnDBTableParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.AddDbTableTask]
	params.Language = i18n.GetUserLang(ctx)

	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.DBTableBizID), model.AddDbTableTask, model.TaskMutexNone, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "NewAddTableTask new task error, %v", err)
		return 0, err
	}
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "NewAddTableTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	log.InfoContextf(ctx, "NewAddTableTask task: %+v, task id: %v", t, taskID)
	return taskID, nil
}

// NewTxDocRefreshTask 创建腾讯文档刷新任务
func NewTxDocRefreshTask(ctx context.Context, tFileInfo []model.TxDocRefreshTFileInfo) (uint64, error) {
	if len(tFileInfo) == 0 {
		return 0, errs.ErrParams
	}
	params := model.TxDocRefreshParams{}
	params.Name = model.TaskTypeNameMap[model.TxDocRefreshTask]
	params.Language = i18n.GetUserLang(ctx)
	params.EnvSet = getEnvSet(ctx)
	tFileInfoParams := make(map[uint64]model.TxDocRefreshTFileInfo)
	for _, info := range tFileInfo {
		tFileInfoParams[info.DocID] = info
	}
	params.TFileInfo = tFileInfoParams
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(tFileInfo[0].RobotID), model.TxDocRefreshTask, model.TaskMutexNone,
		params,
	) //tFileInfo[0].RobotID
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

// NewCorpCOSDocRefreshTask 创建客户COS文档刷新任务
func NewCorpCOSDocRefreshTask(ctx context.Context, docs []*model.Doc) (uint64, error) {
	if len(docs) == 0 {
		return 0, errs.ErrParams
	}
	params := model.CorpCOSDocRefreshParams{}
	params.Name = model.TaskTypeNameMap[model.CorpCOSDocRefreshTask]
	params.Language = i18n.GetUserLang(ctx)
	params.EnvSet = getEnvSet(ctx)
	params.Docs = docs
	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(docs[0].RobotID), model.CorpCOSDocRefreshTask, model.TaskMutexNone,
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

// NewDocDocSplitRuleModifyTask 创建文档切分规则修改任务
func NewDocDocSplitRuleModifyTask(ctx context.Context, params model.DocSplitRuleModifyParams) error {
	params.Name = model.TaskTypeNameMap[model.TxDocRefreshTask]
	params.Language = i18n.GetUserLang(ctx)
	params.SourceEnvSet = getEnvSet(ctx)
	t, _ := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(params.AppID), model.DocSplitRuleModifyTask, model.TaskMutexNone, params,
	)
	if _, err := taskScheduler.CreateTask(ctx, t); err != nil {
		log.ErrorContextf(ctx, "create NewDocDocSplitRuleModifyTask fail, task: %+v, err: %v", t, err)
		return err
	}
	return nil
}

// StopTask 停止任务
func StopTask(ctx context.Context, taskID task_scheduler.TaskID) error {
	err := taskScheduler.StopTask(ctx, taskID)
	if err != nil {
		log.ErrorContextf(ctx, "StopTask task:%d err:%+v", taskID, err)
		return err
	}
	return nil
}

// NewUpdateEmbeddingModelTask 创建切换embedding模型任务
func NewUpdateEmbeddingModelTask(ctx context.Context, robotID uint64,
	params *model.UpdateEmbeddingModelParams) (task_scheduler.TaskID, error) {
	params.Name = model.TaskTypeNameMap[model.UpdateEmbeddingModelTask]
	params.Language = i18n.GetUserLang(ctx)

	t, err := task_scheduler.NewTask(
		ctx, task_scheduler.UserID(robotID),
		model.UpdateEmbeddingModelTask, model.TaskMutexUpdateEmbeddingModel, params,
	)
	if err != nil {
		log.ErrorContextf(ctx, "NewUpdateEmbeddingModelTask new task error, %v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "NewUpdateEmbeddingModelTask task: %+v", t)
	taskID, err := taskScheduler.CreateTask(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "NewUpdateEmbeddingModelTask fail, task: %+v, err: %v", t, err)
		return 0, err
	}
	return taskID, nil
}
