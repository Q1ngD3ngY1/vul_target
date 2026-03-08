package async

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	qaResumeDosagePrefix = "qaResume:dosage:"
)

// QAResumeTaskHandler 资源包到期后处理离线任务
type QAResumeTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.QAResumeParams
}

func registerQAResumeTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.QAResumeTask,
		func(t task_scheduler.Task, params entity.QAResumeParams) task_scheduler.TaskHandler {
			return &QAResumeTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *QAResumeTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(QAResume) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	filter := &qaEntity.DocQaFilter{
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
		BusinessIds: d.p.QABizIDs(),
	}
	qas, err := d.qaLogic.GetAllDocQas(ctx, qaEntity.DocQaTblColList, filter)
	if err != nil {
		return kv, err
	}
	logx.D(ctx, "task(QAResume) Prepare, task: %+v, qas: %+v", d.task.ID, qas)
	for _, qa := range qas {
		kv[fmt.Sprintf("%s%d", qaResumeDosagePrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}
	return kv, nil
}

// Init 初始化
func (d *QAResumeTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// resumeQA 恢复问答
func (d *QAResumeTaskHandler) resumeQA(ctx context.Context, qa *qaEntity.DocQA) error {
	logx.D(ctx, "task(QAResume) Process, task: %+v, resumeQA: %+v", d.task.ID, qa.ID)
	app, err := d.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, qa.RobotID)
	if err != nil {
		return err
	}
	err = d.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
		App:                  app,
		NewCharSize:          qa.CharSize,
		NewKnowledgeCapacity: qa.QaSize,
		NewComputeCapacity:   qa.QaSize,
	})
	if err != nil {
		if errors.Is(err, errs.ErrOverCharacterSizeLimit) {
			_ = d.qaResumeNoticeError(ctx, qa)
		}
		return common.ConvertErrMsg(ctx, d.rpc, 0, d.p.CorpID, err)
	}
	switch qa.ReleaseStatus {
	case qaEntity.QAReleaseStatusResuming:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusInit
	case qaEntity.QAReleaseStatusAppealFailResuming:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAppealFail
	case qaEntity.QAReleaseStatusAuditNotPassResuming:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPass
	case qaEntity.QAReleaseStatusLearnFailResuming:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusLearnFail
	default:
		return nil
	}
	// 增加相似问的超量恢复的逻辑
	sqs, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx,
			"task(QAResume) Process, task: %+v, bot: %+v, qaID: %+v, GetSimilarQuestionsByQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		// 柔性放过
	}
	sqm := &qaEntity.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	logx.D(ctx, "update QA(%d) and SimilarQuestions", qa.ID)
	err = d.qaLogic.UpdateQA(ctx, qa, sqm, true, false, 0, 0, &labelEntity.UpdateQAAttributeLabelReq{})
	if err != nil {
		logx.E(ctx, "task(QAResume) Process, task: %+v, bot: %+v, qaID: %+v, UpdateQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		return err
	}
	return nil
}

// Process 任务处理
func (d *QAResumeTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(QAResume) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(QAResume) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(QAResume) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(QAResume) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, qaResumeDosagePrefix) {
			qa, err := d.qaLogic.GetQAByID(ctx, id)
			if err != nil {
				logx.E(ctx, "task(QAResume) GetDocResumingByBizIDs kv:%s err:%+v", key, err)
				return err
			}
			if err := d.resumeQA(ctx, qa); err != nil {
				logx.E(ctx, "task(QAResume) resumeQA kv:%s err:%+v", key, err)
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(QAResume) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(QAResume) Finish kv:%s", key)
	}
	return nil
}

// Fail 任务失败
func (d *QAResumeTaskHandler) Fail(ctx context.Context) error {
	defer d.taskCommon.updateAppCharSize(ctx, d.p.RobotID, d.p.CorpID)
	logx.D(ctx, "task(QAResume) Fail")
	filter := &qaEntity.DocQaFilter{
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
		BusinessIds: d.p.QABizIDs(),
	}
	qas, err := d.qaLogic.GetAllDocQas(ctx, qaEntity.DocQaTblColList, filter)
	if err != nil {
		return err
	}
	updateM := map[uint64]time.Time{}
	for _, v := range d.p.QAExceededTimes {
		updateM[v.BizID] = v.UpdateTime
	}
	for _, qa := range qas {
		switch qa.ReleaseStatus {
		case qaEntity.QAReleaseStatusResuming:
			qa.ReleaseStatus = qaEntity.QAReleaseStatusCharExceeded
		case qaEntity.QAReleaseStatusAppealFailResuming:
			qa.ReleaseStatus = qaEntity.QAReleaseStatusAppealFail
		case qaEntity.QAReleaseStatusAuditNotPassResuming:
			qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPass
		case qaEntity.QAReleaseStatusLearnFailResuming:
			qa.ReleaseStatus = qaEntity.QAReleaseStatusLearnFail
		default:
			continue
		}
		v, ok := updateM[qa.BusinessID]
		if !ok {
			continue
		}
		// 还原更新时间
		qa.UpdateTime = v
		logx.W(ctx, "task(QAResume) Fail reset qa %+v status: %+v, update_time: %+v", qa.ID, qa.ReleaseStatus,
			v)
		if err := d.qaLogic.UpdateQAReleaseStatus(ctx, qa); err != nil {
			logx.W(ctx, "task(QAResume) Fail reset qa %+v release status err: %+v", qa.ID, err)
		}
	}

	return nil
}

// Stop 任务停止
func (d *QAResumeTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *QAResumeTaskHandler) Done(ctx context.Context) error {
	defer d.taskCommon.updateAppCharSize(ctx, d.p.RobotID, d.p.CorpID)
	logx.D(ctx, "task(QAResume) Done")
	filter := &qaEntity.DocQaFilter{
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
		BusinessIds: d.p.QABizIDs(),
	}
	qas, err := d.qaLogic.GetAllDocQas(ctx, qaEntity.DocQaTblColList, filter)
	if err != nil {
		return err
	}
	return d.qaResumeNoticeSuccess(ctx, qas)
}

func (d *QAResumeTaskHandler) qaResumeNoticeSuccess(ctx context.Context, qas []*qaEntity.DocQA) error {
	if len(qas) == 0 {
		return nil
	}
	qa := qas[0]
	logx.D(ctx, "task(QAResume) Done qaResumeNoticeSuccess, botID: %+v, qa count: %+v",
		qa.RobotID, len(qas))

	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "task(QAResume) qaResumeNoticeSuccess, DescribeAppByPrimaryIdWithoutNotFoundError err: %+v", err)
		return err
	}

	operations := []releaseEntity.Operation{}
	var content string
	if appDB.IsShared {
		content = i18n.Translate(ctx, i18nkey.KeyQARestoreSuccessWithNameAndCountNotRelease, qa.Question, len(qas))
	} else {
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyQARestoreSuccessWithNameAndCount, qa.Question, len(qas)))
	}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
		releaseEntity.WithLevel(releaseEntity.LevelSuccess),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyQARestoreSuccess)),
		releaseEntity.WithContent(content),
	}

	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeQAResume, qa.ID, d.p.CorpID, qa.RobotID, d.p.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "task(QAResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	logx.D(ctx, "task(QAResume) Done, CreateNotice notice: %+v", notice)
	if err := d.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "task(QAResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}

func (d *QAResumeTaskHandler) qaResumeNoticeError(ctx context.Context, qa *qaEntity.DocQA) error {
	logx.D(ctx, "task(QAResume) Process qaResumeNoticeError, botID: %+v, qa: %+v", qa.RobotID, qa.Question)
	operations := make([]releaseEntity.Operation, 0)
	corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, qa.CorpID)
	// corp, err := d.dao.GetCorpByID(ctx, qa.CorpPrimaryId)
	if err != nil {
		return err
	}
	// isSystemIntegrator := d.dao.IsSystemIntegrator(ctx, corp.GetSid())
	isSystemIntegrator := d.rpc.PlatformAdmin.IsSystemIntegrator(ctx, corp.GetSid())
	if !isSystemIntegrator {
		// 非系统集成商才需要额外增加跳转
		operations = append(operations, releaseEntity.Operation{Type: releaseEntity.OpTypeExpandCapacity, Params: releaseEntity.OpParams{}})
	}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
		releaseEntity.WithLevel(releaseEntity.LevelWarning),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyQARestoreFailure)),
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficientQARestoreFailureWithName, qa.Question)),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeQAResume, qa.ID, d.p.CorpID, qa.RobotID, d.p.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "task(QAResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	logx.D(ctx, "task(QAResume) Done, CreateNotice notice: %+v", notice)
	if err := d.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "task(QAResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}
