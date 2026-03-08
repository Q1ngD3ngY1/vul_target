package async

import (
	"context"
	"fmt"
	"strings"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	docModifyPrefix = "doc:modify:"
)

// AttributeLabelUpdateTaskHandler 属性标签更新任务
type AttributeLabelUpdateTaskHandler struct {
	*taskCommon

	task     task_scheduler.Task
	p        labelEntity.AttributeLabelUpdateParams
	labelDao label.Dao
}

func registerAttributeLabelUpdateTaskHandler(tc *taskCommon, labelDao label.Dao) {
	task_scheduler.Register(
		entity.AttributeLabelUpdateTask,
		func(t task_scheduler.Task, params labelEntity.AttributeLabelUpdateParams) task_scheduler.TaskHandler {
			return &AttributeLabelUpdateTaskHandler{
				taskCommon: tc,
				task:       t,
				labelDao:   labelDao,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *AttributeLabelUpdateTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(AttributeLabelUpdate) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	mapAtrr, err := d.labelDao.GetAttributeByIDs(ctx, d.p.RobotID, []uint64{d.p.AttrID})
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
func (d *AttributeLabelUpdateTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *AttributeLabelUpdateTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(AttributeLabelUpdate) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(AttributeLabelUpdate) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(AttributeLabelUpdate) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(AttributeLabelUpdate) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, docModifyPrefix) {
			if err := d.updateDocAttributeLabel(ctx, d.p.StaffID, d.p.RobotID, id); err != nil {
				return err
			}
		}
		if strings.HasPrefix(key, qaModifyPrefix) {
			if err := d.updateQAAttributeLabel(ctx, d.p.RobotID, id); err != nil {
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(AttributeLabelUpdate) Finish kv:%s err:%+v", key, err)
			return err
		}
		if err := d.updateAttributeTask(ctx, labelEntity.AttributeLabelTaskStatusRunning, ""); err != nil {
			return err
		}
		logx.D(ctx, "task(AttributeLabelUpdate) Finish kv:%s", key)
	}
	return nil
}

// updateAttributeTask 更新任务流程状态
func (d *AttributeLabelUpdateTaskHandler) updateAttributeTask(ctx context.Context, status int, message string) error {
	logx.D(ctx, "task(updateAttributeTask), task: %+v, params: %+v", d.task, d.p)
	taskInfo := labelEntity.AttributeLabelTask{}
	taskInfo.ID = d.p.TaskID
	taskInfo.CorpID = d.p.CorpID
	taskInfo.RobotID = d.p.RobotID
	taskInfo.CreateStaffID = d.p.StaffID
	taskInfo.Status = uint32(status)
	taskInfo.Message = message
	err := d.labelLogic.UpdateAttributeTask(ctx, &taskInfo)
	if err != nil {
		return err
	}
	return nil
}

// Fail 任务失败
func (d *AttributeLabelUpdateTaskHandler) Fail(ctx context.Context) error {
	logx.D(ctx, "task(AttributeLabelUpdate) Done")
	if err := d.updateAttributeTask(ctx, labelEntity.AttributeLabelTaskStatusFailed, ""); err != nil {
		return err
	}
	mapAttr, err := d.labelDao.GetAttributeByIDs(ctx, d.p.RobotID, []uint64{d.p.AttrID})
	if err != nil {
		return err
	}
	attr, ok := mapAttr[d.p.AttrID]
	if !ok {
		return errs.ErrAttributeLabelNotFound
	}
	if err := d.labelLogic.UpdateAttributeFail(ctx, attr, d.p.CorpID, d.p.StaffID); err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *AttributeLabelUpdateTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *AttributeLabelUpdateTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(AttributeLabelUpdate) Done, task: %+v, params: %+v", d.task, d.p)
	if err := d.updateAttributeTask(ctx, labelEntity.AttributeLabelTaskStatusSuccess, ""); err != nil {
		return err
	}
	mapAttr, err := d.labelDao.GetAttributeByIDs(ctx, d.p.RobotID, []uint64{d.p.AttrID})
	if err != nil {
		return err
	}
	attr, ok := mapAttr[d.p.AttrID]
	if !ok {
		return errs.ErrAttributeLabelNotFound
	}
	if err := d.labelLogic.UpdateAttributeSuccess(ctx, attr, d.p.CorpID, d.p.StaffID); err != nil {
		return err
	}

	robotBizID := uint64(0)
	if r, err := d.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, d.p.RobotID); err == nil {
		robotBizID = r.BizId
	} else {
		return errs.ErrRobotNotFound
	}
	labels := make([]uint64, 0, 10)
	if labelInfos, err := d.labelDao.GetAttributeLabelByIDs(ctx, d.p.LabelIDs, d.p.RobotID); err == nil {
		for _, labelInfo := range labelInfos {
			labels = append(labels, labelInfo.BusinessID)
		}
	} else {
		return errs.ErrAttributeLabelNotFound
	}
	if err := d.userLogic.ModifyRoleKnowledgeByAttrChange(ctx, robotBizID, []uint64{d.p.AttrID}, labels)(); err != nil {
		logx.E(ctx, "UpdateRoleKnowledgeByAttrChange failed, robotBizID: %d, attrID: %d, labels: %+v, err: %+v",
			robotBizID, d.p.AttrID, labels, err)
	}

	return nil
}

// getDocAttributeLabel 获取文档的属性标签
func (d *AttributeLabelUpdateTaskHandler) getDocAttributeLabel(ctx context.Context) ([]uint64, error) {
	mapDocID := make(map[uint64]struct{})
	docIDs := make([]uint64, 0)
	page, pageSize := uint32(1), uint32(500)
	for {
		docAttriLabels, err := d.labelDao.GetDocAttributeLabelByAttrLabelIDs(ctx, d.p.RobotID, labelEntity.AttributeLabelSourceKg,
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
func (d *AttributeLabelUpdateTaskHandler) getQAAttributeLabel(ctx context.Context) ([]uint64, error) {
	mapQAID := make(map[uint64]struct{})
	qaIDs := make([]uint64, 0)
	page, pageSize := uint32(1), uint32(500)
	for {
		qaAttriLabels, err := d.labelDao.GetQAAttributeLabelByAttrLabelIDs(ctx, d.p.RobotID, labelEntity.AttributeLabelSourceKg,
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
func (d *AttributeLabelUpdateTaskHandler) updateDocAttributeLabel(ctx context.Context, staffID, robotID, docID uint64) error {
	doc, err := d.docLogic.GetDocByID(ctx, docID, robotID)
	if err != nil || doc == nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}
	if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleaseSuccess {
		// 只有待发布和发布成功的文档才需要更新
		// 其他稳定状态的文档都必须重新学习才会达到待发布的状态，所以不需要在这里强制更新
		return nil
	}
	doc.Status = docEntity.DocStatusUpdating
	if !doc.IsNextActionAdd() {
		doc.NextAction = docEntity.DocNextActionUpdate
	}
	attributeLabelReq := &labelEntity.UpdateDocAttributeLabelReq{IsNeedChange: false}
	if err := d.docLogic.UpdateDoc(ctx, staffID, doc, true, attributeLabelReq); err != nil {
		return err
	}
	return nil
}

// updateQAAttributeLabel TODO
func (d *AttributeLabelUpdateTaskHandler) updateQAAttributeLabel(ctx context.Context, robotID, qaID uint64) error {
	qa, err := d.qaLogic.GetQAByID(ctx, qaID)
	if err != nil || qa == nil {
		return err
	}
	if qa.IsDelete() {
		return nil
	}
	if qa.ReleaseStatus != qaEntity.QAReleaseStatusInit && qa.ReleaseStatus != qaEntity.QAReleaseStatusSuccess {
		// 只有待发布和发布成功的问答才需要更新
		// 其他稳定状态的问答都必须重新学习才会达到待发布的状态，所以不需要在这里强制更新
		return nil
	}
	qa.SimilarStatus = docEntity.SimilarStatusInit
	qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
	qa.IsAuditFree = qaEntity.QAIsAuditFree
	if !qa.IsNextActionAdd() {
		qa.NextAction = qaEntity.NextActionUpdate
	}
	// 获取相似问的处理
	sqs, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionsByQAID failed, qaID: %d, err: %+v", qaID, err)
		// 柔性放过
	}
	sqm := &qaEntity.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	attributeLabelReq := &labelEntity.UpdateQAAttributeLabelReq{IsNeedChange: false}
	if err = d.qaLogic.UpdateQA(ctx, qa, sqm, true, false, 0, 0, attributeLabelReq); err != nil {
		return err
	}
	return nil
}
