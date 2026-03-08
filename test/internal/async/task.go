package async

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb_package"

	"git.woa.com/adp/kb/kb-config/internal/logic/llm"
	"git.woa.com/adp/kb/kb-config/internal/logic/third_document"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/gox/ptrx"
	pb "git.woa.com/adp/pb-go/app/app_config"

	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	labeldao "git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/logic/audit"
	"git.woa.com/adp/kb/kb-config/internal/logic/category"
	"git.woa.com/adp/kb/kb-config/internal/logic/database"
	"git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/export"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb"
	"git.woa.com/adp/kb/kb-config/internal/logic/label"
	"git.woa.com/adp/kb/kb-config/internal/logic/qa"
	"git.woa.com/adp/kb/kb-config/internal/logic/release"
	"git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/task"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

// Init 异步任务初始化
func Init(r *rpc.RPC, adminRdb types.AdminRedis, db types.MySQLClient, tdSql types.TDSQLDB, d dao.Dao, kbDao kbdao.Dao, auditLogic *audit.Logic, labelDao labeldao.Dao,
	userLogic *user.Logic, docLogic *document.Logic, qaLogic *qa.Logic, releaseLogic *release.Logic,
	s3 dao.S3, segLogic *segment.Logic, cateLogic *category.Logic, dbLogic *database.Logic, exportLogic *export.Logic, taskLogic *task.Logic, labelLogic *label.Logic,
	kbLogic *kb.Logic, dbDao dbdao.Dao, financeLogic *finance.Logic, llmLogic *llm.Logic, thirdLogic *third_document.Logic, kbPKGLogic *kb_package.Logic,
) error {

	ctx := trpc.BackgroundContext()
	tc := newTaskCommon(r, d, s3, adminRdb, kbDao, docLogic, qaLogic, userLogic, labelLogic, auditLogic,
		releaseLogic, cateLogic, dbLogic, segLogic, taskLogic, exportLogic, kbLogic, financeLogic, llmLogic, thirdLogic, kbPKGLogic)

	// 注册任务处理器
	logx.I(ctx, "TaskTypeNameMap:%+v", entity.TaskTypeNameMap)
	for _, handle := range config.GetMainConfig().HandleTasks {
		logx.I(ctx, "load task handler:%s", handle)
		switch handle {
		case entity.TaskTypeNameMap[entity.TaskExport]:
			registerExportTaskHandler(tc, qaLogic.GetDao(), labelDao)
		case entity.TaskTypeNameMap[entity.ImportKbPackageTask]:
			registerImportKBPackageTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.ExportKbPackageTask]:
			registerExportKBPackageTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocDeleteTask]:
			registerDocDeleteTaskHandler(tc, dbDao)
		case entity.TaskTypeNameMap[entity.QADeleteTask]:
			registerQADeleteTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocToIndexTask]:
			registerDocToIndexTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocToQATask]:
			registerDocToQATaskHandler(tc)
		case entity.TaskTypeNameMap[entity.SendAuditTask]:
			registerSendAuditTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.CheckAuditTask]:
			registerCheckAuditTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.ReleaseDocTask]:
			registerReleaseDocTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.ReleaseDocQATask]:
			registerReleaseDocQATaskHandler(tc)
		case entity.TaskTypeNameMap[entity.ReleaseRejectedQuestionTask]:
			registerReleaseRejectedQuestionTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.ReleaseLabelTask]:
			registerReleaseLabelTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.ReleaseKnowledgeConfigTask]:
			registerReleaseKnowledgeConfigTaskHandler(tc)
		// case entity.TaskTypeNameMap[entity.ReleaseVectorTask]:
		//	registerReleaseVectorTaskHandler(tc, r)
		case entity.TaskTypeNameMap[entity.ExcelToQATask]:
			registerExcelToQATaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocModifyTask]:
			registerDocModifyTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.AttributeLabelUpdateTask]:
			registerAttributeLabelUpdateTaskHandler(tc, labelDao)
		// case entity.TaskTypeNameMap[entity.EmbeddingUpgradeTask]:
		// 	initEmbeddingUpgradeScheduler(tc,d, docLogic, qaLogic)
		case entity.TaskTypeNameMap[entity.ResourceExpireTask]:
			registerResExpireTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocResumeTask]:
			registerDocResumeScheduler(tc)
		case entity.TaskTypeNameMap[entity.QAResumeTask]:
			registerQAResumeTaskHandler(tc)
		// case entity.TaskTypeNameMap[entity.SynonymsDeleteTask]:
		// 	// 未找到对应处理函数
		// 	break
		// case entity.TaskTypeNameMap[entity.EvaluateTestDeleteTask]:
		// 	// 处理函数已经移除
		// 	break
		case entity.TaskTypeNameMap[entity.KnowledgeDeleteTask]:
			registerKnowledgeDeleteTaskHandler(tc, kbDao)
		case entity.TaskTypeNameMap[entity.DocRenameToIndexTask]:
			registerDocRenameToIndexTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocDiffDataTask]:
			registerDocDiffTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocDiffOperationTask]:
			registerDocDiffOperationTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.SyncAttributeTask]:
			registerSyncAttributeTaskHandler(tc, labelDao)
		case entity.TaskTypeNameMap[entity.BatchUpdateVectorTask]:
			registerBatchUpdateVectorTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.KnowledgeGenerateSchemaTask]:
			registerKnowledgeGenerateSchemaTaskHandler(tc, tdSql, kbDao)
		case entity.TaskTypeNameMap[entity.DocSegInterveneTask]:
			registerDocSegInterveneTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.ReleaseDBTask]:
			registerReleaseDBTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.FullUpdateLabelTask]:
			registerFullUpdateLabelTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.SyncOrgDataTask]:
			registerSyncOrgDataTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.EnableDBSourceTask]:
			registerDbSourceTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.AddDbTableTask]:
			registerAddDbTableTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.TxDocRefreshTask]:
			registerTxDocRefreshTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.DocSplitRuleModifyTask]:
			registerDocSplitRuleModifyTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.CorpCOSDocRefreshTask]:
			registerCorpCOSDocRefreshScheduler(tc, r, s3, docLogic, financeLogic)
		case entity.TaskTypeNameMap[entity.FullUpdateDatabaseLabelTask]:
			registerFullUpdateDatabaseLabelTaskHandler(tc)
		case entity.TaskTypeNameMap[entity.MigrateThirdDocTask]:
			registerMigrateOnedriveDocScheduler(tc)
		case entity.TaskTypeNameMap[entity.RefreshThirdDocTask]:
			registerDocRefreshScheduler(tc)
		}
	}

	if err := scheduler.RunTask(adminRdb, db); err != nil {
		panic(fmt.Errorf("run task error: %v", err))
	}
	return nil
}

type taskCommon struct {
	rpc           *rpc.RPC
	dao           dao.Dao
	s3            dao.S3
	adminRdb      types.AdminRedis
	kbDao         kbdao.Dao
	docLogic      *document.Logic
	qaLogic       *qa.Logic
	userLogic     *user.Logic
	labelLogic    *label.Logic
	auditLogic    *audit.Logic
	releaseLogic  *release.Logic
	cateLogic     *category.Logic
	dbLogic       *database.Logic
	segLogic      *segment.Logic
	taskLogic     *task.Logic
	exportLogic   *export.Logic
	kbLogic       *kb.Logic
	financeLogic  *finance.Logic
	llmLogic      *llm.Logic
	thirdDocLogic *third_document.Logic
	kbPKGLogic    *kb_package.Logic
}

func newTaskCommon(r *rpc.RPC, d dao.Dao, s3 dao.S3, adminRdb types.AdminRedis, kbDao kbdao.Dao, docLogic *document.Logic,
	qaLogic *qa.Logic, userLogic *user.Logic, labelLogic *label.Logic, auditLogic *audit.Logic,
	releaseLogic *release.Logic, cateLogic *category.Logic, dbLogic *database.Logic, segLogic *segment.Logic,
	taskLogic *task.Logic, exportLogic *export.Logic, kbLogic *kb.Logic, financeLogic *finance.Logic, llmLogic *llm.Logic,
	thirdDocLogic *third_document.Logic, kbPkgLogic *kb_package.Logic,
) *taskCommon {
	return &taskCommon{
		rpc:           r,
		dao:           d,
		s3:            s3,
		adminRdb:      adminRdb,
		kbDao:         kbDao,
		docLogic:      docLogic,
		qaLogic:       qaLogic,
		userLogic:     userLogic,
		labelLogic:    labelLogic,
		auditLogic:    auditLogic,
		releaseLogic:  releaseLogic,
		cateLogic:     cateLogic,
		dbLogic:       dbLogic,
		segLogic:      segLogic,
		taskLogic:     taskLogic,
		exportLogic:   exportLogic,
		kbLogic:       kbLogic,
		financeLogic:  financeLogic,
		llmLogic:      llmLogic,
		thirdDocLogic: thirdDocLogic,
		kbPKGLogic:    kbPkgLogic,
	}
}

func (c *taskCommon) updateAppCharSize(ctx context.Context, robotID uint64, corpID uint64) error {
	docUsage, err := c.docLogic.GetRobotDocUsage(ctx, robotID, corpID)
	if err != nil {
		return err
	}
	qaUsage, err := c.qaLogic.GetRobotQAUsage(ctx, robotID, corpID)
	if err != nil {
		return err
	}
	req := pb.ModifyAppReq{
		Inner: &pb.ModifyAppInner{
			AppPrimaryId:          robotID,
			UsedCharSize:          ptrx.Uint64(uint64(docUsage.CharSize) + uint64(qaUsage.CharSize)),
			UsedKnowledgeCapacity: ptrx.Uint64(uint64(docUsage.KnowledgeCapacity) + uint64(qaUsage.KnowledgeCapacity)),
		},
	}
	if _, err := c.rpc.AppAdmin.ModifyApp(ctx, &req); err != nil {
		return fmt.Errorf("updateAppCharSize|ModifyApp error:%w", err)
	}
	return nil
}
