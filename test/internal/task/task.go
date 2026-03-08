package task

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
)

// Init 异步任务初始化
func Init() {
	ctx := context.Background()
	log.InfoContextf(ctx, "TaskTypeNameMap:%+v", model.TaskTypeNameMap)
	for _, handle := range utilConfig.GetMainConfig().HandleTasks {
		log.InfoContextf(ctx, "load task handler:%s", handle)
		switch handle {
		case model.TaskTypeNameMap[model.TaskExport]:
			initExportScheduler()
		case model.TaskTypeNameMap[model.TaskTest]:
			initRunScheduler()
		case model.TaskTypeNameMap[model.DocDeleteTask]:
			initDocDeleteScheduler()
		case model.TaskTypeNameMap[model.QADeleteTask]:
			initQADeleteScheduler()
		case model.TaskTypeNameMap[model.DocToIndexTask]:
			initDocToIndexScheduler()
		case model.TaskTypeNameMap[model.DocToQATask]:
			initDocToQAScheduler()
		case model.TaskTypeNameMap[model.SendAuditTask]:
			initSendAuditScheduler()
		case model.TaskTypeNameMap[model.CheckAuditTask]:
			initCheckAuditScheduler()
		case model.TaskTypeNameMap[model.ReleaseCollectTask]:
			initReleaseCollectScheduler()
		case model.TaskTypeNameMap[model.ReleaseSuccessTask]:
			initReleaseSuccessScheduler()
		case model.TaskTypeNameMap[model.ExcelToQATask]:
			initExcelToQAScheduler()
		case model.TaskTypeNameMap[model.DocModifyTask]:
			initDocModifyScheduler()
		case model.TaskTypeNameMap[model.AttributeLabelUpdateTask]:
			initAttributeLabelUpdateScheduler()
		case model.TaskTypeNameMap[model.EmbeddingUpgradeTask]:
			initEmbeddingUpgradeScheduler()
		case model.TaskTypeNameMap[model.ResourceExpireTask]:
			initResExpireScheduler()
		case model.TaskTypeNameMap[model.DocResumeTask]:
			initDocResumeScheduler()
		case model.TaskTypeNameMap[model.QAResumeTask]:
			initQAResumeScheduler()
		case model.TaskTypeNameMap[model.SynonymsDeleteTask]:
			// 未找到对应处理函数
			break
		case model.TaskTypeNameMap[model.SynonymsImportTask]:
			initSynonymsImportScheduler()
		case model.TaskTypeNameMap[model.EvaluateTestDeleteTask]:
			// 未找到对应处理函数
			break
		case model.TaskTypeNameMap[model.KnowledgeDeleteTask]:
			initKnowledgeDeleteScheduler()
		case model.TaskTypeNameMap[model.DocRenameToIndexTask]:
			initDocRenameToIndexScheduler()
		case model.TaskTypeNameMap[model.DocDiffDataTask]:
			initDocDiffScheduler()
		case model.TaskTypeNameMap[model.DocDiffOperationTask]:
			initDocDiffOperationTaskScheduler()
		case model.TaskTypeNameMap[model.SyncAttributeTask]:
			initSyncAttributeScheduler()
		case model.TaskTypeNameMap[model.BatchUpdateVectorTask]:
			initBatchUpdateVectorScheduler()
		case model.TaskTypeNameMap[model.KnowledgeGenerateSchemaTask]:
			initKnowledgeGenerateSchemaScheduler()
		case model.TaskTypeNameMap[model.DocSegInterveneTask]:
			initDocSegInterveneScheduler()
		case model.TaskTypeNameMap[model.ReleaseDBTask]:
			initReleaseDBScheduler()
		case model.TaskTypeNameMap[model.FullUpdateLabelTask]:
			initFullUpdateLabelScheduler()
		case model.TaskTypeNameMap[model.SyncOrgDataTask]:
			initSyncOrgDataScheduler()
		case model.TaskTypeNameMap[model.EnableDBSourceTask]:
			initDbSourceScheduler()
		case model.TaskTypeNameMap[model.AddDbTableTask]:
			initAddDbTableScheduler()
		case model.TaskTypeNameMap[model.SyncDbSourceVdbIndexTask]:
			initSyncDbSourceVdbIndexScheduler()
		case model.TaskTypeNameMap[model.TxDocRefreshTask]:
			initTxDocRefreshScheduler()
		case model.TaskTypeNameMap[model.DocSplitRuleModifyTask]:
			initDocSplitRuleModifyScheduler()
		case model.TaskTypeNameMap[model.UpdateEmbeddingModelTask]:
			initChangeEmbeddingModelScheduler()
		case model.TaskTypeNameMap[model.CorpCOSDocRefreshTask]:
			initCorpCOSDocRefreshScheduler()
		}
	}
}
