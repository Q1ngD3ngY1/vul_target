package async

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"go.opentelemetry.io/otel/trace"
)

// DocSplitRuleModifyTaskHandler 切分规则修改任务
type DocSplitRuleModifyTaskHandler struct {
	*taskCommon

	task   task_scheduler.Task
	params entity.DocSplitRuleModifyParams
}

func registerDocSplitRuleModifyTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocSplitRuleModifyTask,
		func(t task_scheduler.Task, params entity.DocSplitRuleModifyParams) task_scheduler.TaskHandler {
			return &DocSplitRuleModifyTaskHandler{
				taskCommon: tc,
				task:       t,
				params:     params,
			}
		},
	)
}

// Prepare 数据准备 仅在该任务第一次执行时触发一次, 在整个任务的生命周期内, 执行且只执行一次
func (d *DocSplitRuleModifyTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	logx.D(ctx, "task(DocSplitRuleModify) Prepare, task: %+v, params: %+v", d.task, d.params)
	kv := make(task_scheduler.TaskKV)
	// 默认解析中，无需更新状态
	// 做一些校验
	doc, err := d.docLogic.GetDocByBizID(ctx, d.params.DocBizID, d.params.AppID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return kv, nil
	}
	kv[fmt.Sprintf("%d", doc.BusinessID)] = fmt.Sprintf("%d", doc.BusinessID)
	return kv, nil
}

// Init 初始化
func (d *DocSplitRuleModifyTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	logx.I(ctx, "task(DocSplitRuleModify) Init start")
	return nil
}

// Process 任务处理
func (d *DocSplitRuleModifyTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocSplitRuleModify) Process, task: %+v, params: %+v", d.task, d.params)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(DocSplitRuleModify) Start k:%s, v:%s", k, v)
		err := d.CleanSegmentsData(ctx, d.params.CorpBizID, d.params.AppBizID, d.params.DocBizID)
		if err != nil {
			logx.E(ctx, "task(DocSplitRuleModify) CleanSegmentsData|err:%v", err)
			return err
		}
		doc, err := d.docLogic.GetDocByBizID(ctx, d.params.DocBizID, d.params.AppID)
		if err != nil {
			logx.E(ctx, "task(DocSplitRuleModify) GetDocByBizID|err:%v", err)
			return err
		}
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		taskID, err := d.docLogic.SendDocParseWordCount(ctx, doc, requestID, doc.FileType)
		if err != nil {
			logx.E(ctx, "task(DocSplitRuleModify) SendDocParseWordCount|err:%v", err)
			return err
		}
		// 将之前的解析任务删除，仅保留当前任务
		err = d.docLogic.DeleteDocParseByDocID(ctx, doc.CorpID, doc.RobotID, doc.ID)
		if err != nil {
			logx.E(ctx, "task(DocSplitRuleModify) DeleteDocParseByDocID|err:%v", err)
			return err
		}
		docParse := &docEntity.DocParse{
			DocID:        doc.ID,
			CorpID:       doc.CorpID,
			RobotID:      doc.RobotID,
			StaffID:      doc.StaffID,
			RequestID:    requestID,
			Type:         docEntity.DocParseTaskTypeWordCount,
			OpType:       docEntity.DocParseOpTypeWordCount,
			Status:       docEntity.DocParseIng,
			TaskID:       taskID,
			SourceEnvSet: d.params.SourceEnvSet,
		}
		err = d.docLogic.CreateDocParseTask(ctx, docParse)
		if err != nil {
			logx.E(ctx, "task(DocSplitRuleModify) getCacheInterveneDocPath|err:%v", err)
			return err
		}
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "task(DocSplitRuleModify) Finish kv:%s err:%+v", k, err)
			return err
		}
		logx.D(ctx, "task(DocSplitRuleModify) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *DocSplitRuleModifyTaskHandler) Fail(ctx context.Context) error {
	logx.E(ctx, "task(DocSplitRuleModify) fail, doc id: %v", d.params.DocBizID)
	return nil
}

// Stop 任务停止
func (d *DocSplitRuleModifyTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocSplitRuleModifyTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(DocSplitRuleModify) done, doc id: %v", d.params.DocBizID)
	return nil
}

// CleanSegmentsData 清除已切分的数据
func (d *DocSplitRuleModifyTaskHandler) CleanSegmentsData(ctx context.Context, corpBizID, appBizID, docBizID uint64) error {
	// 删除OrgData
	deleteFilter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		RouterAppBizID: appBizID,
	}
	err := d.segLogic.RealityBatchDeleteDocOrgData(ctx, deleteFilter, 10000)
	if err != nil {
		logx.E(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	// 删除临时OrgData
	deleteTempFilter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	err = d.segLogic.RealityBatchDeleteTemporaryDocOrgData(ctx, deleteTempFilter, 10000)
	if err != nil {
		logx.E(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	// 删除旧sheet
	deleteSheetTempFilter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	err = d.segLogic.RealityBatchDeleteDocSheet(ctx, deleteSheetTempFilter, 10000)
	if err != nil {
		logx.E(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	return nil
}
