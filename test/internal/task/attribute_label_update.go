package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	docModifyPrefix = "doc:modify:"
)

// AttributeLabelUpdateScheduler 属性标签更新任务
type AttributeLabelUpdateScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.AttributeLabelUpdateParams
}

func initAttributeLabelUpdateScheduler() {
	task_scheduler.Register(
		model.AttributeLabelUpdateTask,
		func(t task_scheduler.Task, params model.AttributeLabelUpdateParams) task_scheduler.TaskHandler {
			return &AttributeLabelUpdateScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *AttributeLabelUpdateScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(AttributeLabelUpdate) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	mapAtrr, err := d.dao.GetAttributeByIDs(ctx, d.p.RobotID, []uint64{d.p.AttrID})
	if err != nil {
		return kv, err
	}
	if _, ok := mapAtrr[d.p.AttrID]; !ok {
		return kv, errs.ErrAttributeLabelNotFound
	}
	docIDs, err := d.getDocAttributeLabel(ctx)
	if err != nil {
		return kv, err
	}
	qaIDs, err := d.getQAAttributeLabel(ctx)
	if err != nil {
		return kv, err
	}
	for _, docID := range docIDs {
		kv[fmt.Sprintf("%s%d", docModifyPrefix, docID)] = fmt.Sprintf("%d", docID)
	}
	for _, qaID := range qaIDs {
		kv[fmt.Sprintf("%s%d", qaModifyPrefix, qaID)] = fmt.Sprintf("%d", qaID)
	}
	return kv, nil
}

// Init 初始化
func (d *AttributeLabelUpdateScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *AttributeLabelUpdateScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(AttributeLabelUpdate) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(AttributeLabelUpdate) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(AttributeLabelUpdate) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(AttributeLabelUpdate) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, docModifyPrefix) {
			if err := updateDocAttributeLabel(ctx, d.dao, d.p.StaffID, d.p.RobotID, id); err != nil {
				return err
			}
		}
		if strings.HasPrefix(key, qaModifyPrefix) {
			if err := updateQAAttributeLabel(ctx, d.dao, d.p.RobotID, id); err != nil {
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(AttributeLabelUpdate) Finish kv:%s err:%+v", key, err)
			return err
		}
		if err := d.updateAttributeTask(ctx, model.AttributeLabelTaskStatusRunning, ""); err != nil {
			return err
		}
		log.DebugContextf(ctx, "task(AttributeLabelUpdate) Finish kv:%s", key)
	}
	return nil
}

// updateAttributeTask 更新任务流程状态
func (d *AttributeLabelUpdateScheduler) updateAttributeTask(ctx context.Context, status int, message string) error {
	log.DebugContextf(ctx, "task(updateAttributeTask), task: %+v, params: %+v", d.task, d.p)
	taskInfo := model.AttributeLabelTask{}
	taskInfo.ID = d.p.TaskID
	taskInfo.CorpID = d.p.CorpID
	taskInfo.RobotID = d.p.RobotID
	taskInfo.CreateStaffID = d.p.StaffID
	taskInfo.Status = uint32(status)
	taskInfo.Message = message
	err := d.dao.UpdateAttributeTask(ctx, &taskInfo)
	if err != nil {
		return err
	}
	return nil
}

// Fail 任务失败
func (d *AttributeLabelUpdateScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(AttributeLabelUpdate) Done")
	if err := d.updateAttributeTask(ctx, model.AttributeLabelTaskStatusFailed, ""); err != nil {
		return err
	}
	mapAttr, err := d.dao.GetAttributeByIDs(ctx, d.p.RobotID, []uint64{d.p.AttrID})
	if err != nil {
		return err
	}
	attr, ok := mapAttr[d.p.AttrID]
	if !ok {
		return errs.ErrAttributeLabelNotFound
	}
	if err := d.dao.UpdateAttributeFail(ctx, attr, d.p.CorpID, d.p.StaffID); err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *AttributeLabelUpdateScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *AttributeLabelUpdateScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(AttributeLabelUpdate) Done, task: %+v, params: %+v", d.task, d.p)
	if err := d.updateAttributeTask(ctx, model.AttributeLabelTaskStatusSuccess, ""); err != nil {
		return err
	}
	mapAttr, err := d.dao.GetAttributeByIDs(ctx, d.p.RobotID, []uint64{d.p.AttrID})
	if err != nil {
		return err
	}
	attr, ok := mapAttr[d.p.AttrID]
	if !ok {
		return errs.ErrAttributeLabelNotFound
	}
	if err := d.dao.UpdateAttributeSuccess(ctx, attr, d.p.CorpID, d.p.StaffID); err != nil {
		return err
	}

	robotBizID := uint64(0)
	if r, err := d.dao.GetRobotInfo(ctx, d.p.CorpID, d.p.RobotID); err == nil {
		robotBizID = r.BusinessID
	} else {
		return errs.ErrRobotNotFound
	}
	labels := make([]uint64, 0, 10)
	if labelInfos, err := d.dao.GetAttributeLabelByIDs(ctx, d.p.LabelIDs, d.p.RobotID); err == nil {
		for _, labelInfo := range labelInfos {
			labels = append(labels, labelInfo.BusinessID)
		}
	} else {
		return errs.ErrAttributeLabelNotFound
	}
	if err := dao.GetRoleDao(nil).UpdateRoleKnowledgeByAttrChange(ctx, robotBizID, []uint64{d.p.AttrID}, labels)(); err != nil {
		log.ErrorContextf(ctx, "UpdateRoleKnowledgeByAttrChange failed, robotBizID: %d, attrID: %d, labels: %+v, err: %+v",
			robotBizID, d.p.AttrID, labels, err)
	}

	return nil
}

// getDocAttributeLabel 获取文档的属性标签
func (d *AttributeLabelUpdateScheduler) getDocAttributeLabel(ctx context.Context) ([]uint64, error) {
	mapDocID := make(map[uint64]struct{})
	docIDs := make([]uint64, 0)
	page, pageSize := uint32(1), uint32(500)
	for {
		docAttriLabels, err := d.dao.GetDocAttributeLabelByAttrLabelIDs(ctx, d.p.RobotID, model.AttributeLabelSourceKg,
			[]uint64{d.p.AttrID}, d.p.LabelIDs, page, pageSize)
		if err != nil {
			return nil, err
		}
		if len(docAttriLabels) == 0 {
			break
		}
		page++
		for _, v := range docAttriLabels {
			if _, ok := mapDocID[v.DocID]; !ok {
				mapDocID[v.DocID] = struct{}{}
				docIDs = append(docIDs, v.DocID)
			}
		}
	}
	return docIDs, nil
}

// getQAAttributeLabel 获取QA的属性标签
func (d *AttributeLabelUpdateScheduler) getQAAttributeLabel(ctx context.Context) ([]uint64, error) {
	mapQAID := make(map[uint64]struct{})
	qaIDs := make([]uint64, 0)
	page, pageSize := uint32(1), uint32(500)
	for {
		qaAttriLabels, err := d.dao.GetQAAttributeLabelByAttrLabelIDs(ctx, d.p.RobotID, model.AttributeLabelSourceKg,
			[]uint64{d.p.AttrID}, d.p.LabelIDs, page, pageSize)
		if err != nil {
			return nil, err
		}
		if len(qaAttriLabels) == 0 {
			break
		}
		page++
		for _, v := range qaAttriLabels {
			if _, ok := mapQAID[v.QAID]; !ok {
				mapQAID[v.QAID] = struct{}{}
				qaIDs = append(qaIDs, v.QAID)
			}
		}
	}
	return qaIDs, nil
}

// updateDocAttributeLabel TODO
func updateDocAttributeLabel(ctx context.Context, dao dao.Dao, staffID, robotID, docID uint64) error {
	doc, err := dao.GetDocByID(ctx, docID, robotID)
	if err != nil || doc == nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}
	if doc.Status != model.DocStatusWaitRelease && doc.Status != model.DocStatusReleaseSuccess {
		// 只有待发布和发布成功的文档才需要更新
		// 其他稳定状态的文档都必须重新学习才会达到待发布的状态，所以不需要在这里强制更新
		return nil
	}
	doc.Status = model.DocStatusUpdating
	if !doc.IsNextActionAdd() {
		doc.NextAction = model.DocNextActionUpdate
	}
	attributeLabelReq := &model.UpdateDocAttributeLabelReq{IsNeedChange: false}
	if err := dao.UpdateDoc(ctx, staffID, doc, true, attributeLabelReq); err != nil {
		return err
	}
	return nil
}

// updateQAAttributeLabel TODO
func updateQAAttributeLabel(ctx context.Context, dao dao.Dao, robotID, qaID uint64) error {
	qa, err := dao.GetQAByID(ctx, qaID)
	if err != nil || qa == nil {
		return err
	}
	if qa.IsDelete() {
		return nil
	}
	if qa.ReleaseStatus != model.QAReleaseStatusInit && qa.ReleaseStatus != model.QAReleaseStatusSuccess {
		// 只有待发布和发布成功的问答才需要更新
		// 其他稳定状态的问答都必须重新学习才会达到待发布的状态，所以不需要在这里强制更新
		return nil
	}
	qa.SimilarStatus = model.SimilarStatusInit
	qa.ReleaseStatus = model.QAReleaseStatusLearning
	qa.IsAuditFree = model.QAIsAuditFree
	if !qa.IsNextActionAdd() {
		qa.NextAction = model.NextActionUpdate
	}
	// 获取相似问的处理
	sqs, err := dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByQAID failed, qaID: %d, err: %+v", qaID, err)
		// 柔性放过
	}
	sqm := &model.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	attributeLabelReq := &model.UpdateQAAttributeLabelReq{IsNeedChange: false}
	if err = dao.UpdateQA(ctx, qa, sqm, true, false, 0, attributeLabelReq); err != nil {
		return err
	}
	return nil
}
