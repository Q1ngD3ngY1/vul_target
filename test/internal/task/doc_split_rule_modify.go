// Package task 解析切分干预任务处理
package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"go.opentelemetry.io/otel/trace"
)

// DocSplitRuleModifyScheduler 切分规则修改任务
type DocSplitRuleModifyScheduler struct {
	dao    dao.Dao
	task   task_scheduler.Task
	params model.DocSplitRuleModifyParams
}

func initDocSplitRuleModifyScheduler() {
	task_scheduler.Register(
		model.DocSplitRuleModifyTask,
		func(t task_scheduler.Task, params model.DocSplitRuleModifyParams) task_scheduler.TaskHandler {
			return &DocSplitRuleModifyScheduler{
				dao:    dao.New(),
				task:   t,
				params: params,
			}
		},
	)
}

// Prepare 数据准备 仅在该任务第一次执行时触发一次, 在整个任务的生命周期内, 执行且只执行一次
func (d *DocSplitRuleModifyScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	log.DebugContextf(ctx, "task(DocSplitRuleModify) Prepare, task: %+v, params: %+v", d.task, d.params)
	kv := make(task_scheduler.TaskKV)
	// 默认解析中，无需更新状态
	// 做一些校验
	doc, err := d.dao.GetDocByBizID(ctx, d.params.DocBizID, d.params.AppID)
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
func (d *DocSplitRuleModifyScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	log.InfoContextf(ctx, "task(DocSplitRuleModify) Init start")
	return nil
}

// Process 任务处理
func (d *DocSplitRuleModifyScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocSplitRuleModify) Process, task: %+v, params: %+v", d.task, d.params)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(DocSplitRuleModify) Start k:%s, v:%s", k, v)
		err := d.CleanSegmentsData(ctx, d.params.CorpBizID, d.params.AppBizID, d.params.DocBizID)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSplitRuleModify) CleanSegmentsData|err:%v", err)
			return err
		}
		doc, err := d.dao.GetDocByBizID(ctx, d.params.DocBizID, d.params.AppID)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSplitRuleModify) GetDocByBizID|err:%v", err)
			return err
		}
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		taskID, err := d.dao.SendDocParseWordCount(ctx, doc, requestID, doc.FileType)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSplitRuleModify) SendDocParseWordCount|err:%v", err)
			return err
		}
		// 将之前的解析任务删除，仅保留当前任务
		err = dao.GetDocParseDao().DeleteDocParseByDocID(ctx, doc.CorpID, doc.RobotID, doc.ID)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSplitRuleModify) DeleteDocParseByDocID|err:%v", err)
			return err
		}
		docParse := model.DocParse{
			DocID:     doc.ID,
			CorpID:    doc.CorpID,
			RobotID:   doc.RobotID,
			StaffID:   doc.StaffID,
			RequestID: requestID,
			Type:      model.DocParseTaskTypeWordCount,
			OpType:    model.DocParseOpTypeWordCount,
			Status:    model.DocParseIng,
			TaskID:    taskID,
		}
		err = d.dao.CreateDocParseWithSourceEnvSet(ctx, nil, docParse, d.params.SourceEnvSet)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSplitRuleModify) getCacheInterveneDocPath|err:%v", err)
			return err
		}
		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "task(DocSplitRuleModify) Finish kv:%s err:%+v", k, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocSplitRuleModify) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *DocSplitRuleModifyScheduler) Fail(ctx context.Context) error {
	log.ErrorContextf(ctx, "task(DocSplitRuleModify) fail, doc id: %v", d.params.DocBizID)
	return nil
}

// Stop 任务停止
func (d *DocSplitRuleModifyScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocSplitRuleModifyScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocSplitRuleModify) done, doc id: %v", d.params.DocBizID)
	return nil
}

// CleanSegmentsData 清除已切分的数据
func (d *DocSplitRuleModifyScheduler) CleanSegmentsData(ctx context.Context, corpBizID, appBizID, docBizID uint64) error {
	// 删除OrgData
	deleteFilter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		RouterAppBizID: appBizID,
	}
	err := dao.GetDocSegmentOrgDataDao().RealityBatchDeleteDocOrgData(ctx, nil, deleteFilter, 10000)
	if err != nil {
		log.ErrorContextf(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	// 删除临时OrgData
	deleteTempFilter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	err = dao.GetDocSegmentOrgDataTemporaryDao().RealityBatchDeleteDocOrgData(ctx,
		nil, deleteTempFilter, appBizID, 10000)
	if err != nil {
		log.ErrorContextf(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	// 删除旧sheet
	deleteSheetTempFilter := &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	err = dao.GetDocSegmentSheetTemporaryDao().RealityBatchDeleteDocSheet(ctx,
		nil, deleteSheetTempFilter, 10000)
	if err != nil {
		log.ErrorContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	return nil
}
