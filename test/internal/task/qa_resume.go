package task

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	qaResumeDosagePrefix = "qaResume:dosage:"
)

// QAResumeScheduler 资源包到期后处理离线任务
type QAResumeScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.QAResumeParams
}

func initQAResumeScheduler() {
	task_scheduler.Register(
		model.QAResumeTask,
		func(t task_scheduler.Task, params model.QAResumeParams) task_scheduler.TaskHandler {
			return &QAResumeScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *QAResumeScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(QAResume) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	filter := &dao.DocQaFilter{
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
		BusinessIds: d.p.QABizIDs(),
	}
	qas, err := dao.GetDocQaDao().GetAllDocQas(ctx, dao.DocQaTblColList, filter)
	if err != nil {
		return kv, err
	}
	log.DebugContextf(ctx, "task(QAResume) Prepare, task: %+v, qas: %+v", d.task.ID, qas)
	for _, qa := range qas {
		kv[fmt.Sprintf("%s%d", qaResumeDosagePrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}
	return kv, nil
}

// Init 初始化
func (d *QAResumeScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}
func (d *QAResumeScheduler) checkUsedCharSizeExceeded(ctx context.Context, corpID uint64, tmpCharSize uint64) (uint64,
	error) {
	corp, err := d.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		return 0, errs.ErrCorpNotFound
	}
	log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, corp: %+v", d.task.ID, corp)
	corp, err = d.dao.GetCorpBillingInfo(ctx, corp)
	if err != nil {
		return 0, errs.ErrCorpNotFound
	}
	usedCharSize, err := d.dao.GetCorpUsedCharSizeUsage(ctx, corpID)
	if err != nil {
		return 0, errs.ErrSystem
	}
	usedCharSize += tmpCharSize // 加上待恢复的字符数
	// TODO: 临时逻辑, 限制 MaxCharSize
	// corp.MaxCharSize = 0
	log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, usedCharSize: %+v, MaxCharSize: %+v", d.task.ID,
		usedCharSize, corp.MaxCharSize)
	if corp.IsUsedCharSizeExceeded(int64(usedCharSize)) {
		exceededCount := usedCharSize
		if corp != nil {
			exceededCount = usedCharSize - corp.MaxCharSize
		}
		return exceededCount, errs.ErrOverCharacterSizeLimit
	}
	return 0, nil
}

// resumeQA 恢复问答
func (d *QAResumeScheduler) resumeQA(ctx context.Context, qa *model.DocQA) error {
	log.DebugContextf(ctx, "task(QAResume) Process, task: %+v, resumeQA: %+v", d.task.ID, qa.ID)
	_, err := d.checkUsedCharSizeExceeded(ctx, d.p.CorpID, qa.CharSize)
	if err != nil {
		if errors.Is(err, errs.ErrOverCharacterSizeLimit) {
			_ = d.qaResumeNoticeError(ctx, qa)
		}
		return d.dao.ConvertErrMsg(ctx, 0, d.p.CorpID, err)
	}
	switch qa.ReleaseStatus {
	case model.QAReleaseStatusResuming:
		qa.ReleaseStatus = model.QAReleaseStatusInit
	case model.QAReleaseStatusAppealFailResuming:
		qa.ReleaseStatus = model.QAReleaseStatusAppealFail
	case model.QAReleaseStatusAuditNotPassResuming:
		qa.ReleaseStatus = model.QAReleaseStatusAuditNotPass
	case model.QAReleaseStatusLearnFailResuming:
		qa.ReleaseStatus = model.QAReleaseStatusLearnFail
	default:
		return nil
	}
	// 增加相似问的超量恢复的逻辑
	sqs, err := d.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx,
			"task(QAResume) Process, task: %+v, bot: %+v, qaID: %+v, GetSimilarQuestionsByQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		// 柔性放过
	}
	sqm := &model.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	log.DebugContextf(ctx, "update QA(%d) and SimilarQuestions", qa.ID)
	err = d.dao.UpdateQA(ctx, qa, sqm, true, false, 0, &model.UpdateQAAttributeLabelReq{})
	if err != nil {
		log.ErrorContextf(ctx, "task(QAResume) Process, task: %+v, bot: %+v, qaID: %+v, UpdateQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		return err
	}
	return nil
}

// Process 任务处理
func (d *QAResumeScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(QAResume) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(QAResume) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(QAResume) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(QAResume) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, qaResumeDosagePrefix) {
			qa, err := d.dao.GetQAByID(ctx, id)
			if err != nil {
				log.ErrorContextf(ctx, "task(QAResume) GetDocResumingByBizIDs kv:%s err:%+v", key, err)
				return err
			}
			if err := d.resumeQA(ctx, qa); err != nil {
				log.ErrorContextf(ctx, "task(QAResume) resumeQA kv:%s err:%+v", key, err)
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(QAResume) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(QAResume) Finish kv:%s", key)
	}
	return nil
}

// Fail 任务失败
func (d *QAResumeScheduler) Fail(ctx context.Context) error {
	defer updateAppCharSize(ctx, d.dao, d.p.RobotID, d.p.CorpID)
	log.DebugContextf(ctx, "task(QAResume) Fail")
	filter := &dao.DocQaFilter{
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
		BusinessIds: d.p.QABizIDs(),
	}
	qas, err := dao.GetDocQaDao().GetAllDocQas(ctx, dao.DocQaTblColList, filter)
	if err != nil {
		return err
	}
	updateM := map[uint64]time.Time{}
	for _, v := range d.p.QAExceededTimes {
		updateM[v.BizID] = v.UpdateTime
	}
	for _, qa := range qas {
		switch qa.ReleaseStatus {
		case model.QAReleaseStatusResuming:
			qa.ReleaseStatus = model.QAReleaseStatusCharExceeded
		case model.QAReleaseStatusAppealFailResuming:
			qa.ReleaseStatus = model.QAReleaseStatusAppealFail
		case model.QAReleaseStatusAuditNotPassResuming:
			qa.ReleaseStatus = model.QAReleaseStatusAuditNotPass
		case model.QAReleaseStatusLearnFailResuming:
			qa.ReleaseStatus = model.QAReleaseStatusLearnFail
		default:
			continue
		}
		v, ok := updateM[qa.BusinessID]
		if !ok {
			continue
		}
		// 还原更新时间
		qa.UpdateTime = v
		log.WarnContextf(ctx, "task(QAResume) Fail reset qa %+v status: %+v, update_time: %+v", qa.ID, qa.ReleaseStatus,
			v)
		if err := d.dao.UpdateQAStatusAndUpdateTime(ctx, qa); err != nil {
			log.WarnContextf(ctx, "task(QAResume) Fail reset qa %+v status err: %+v", qa.ID, err)
		}
	}

	return nil
}

// Stop 任务停止
func (d *QAResumeScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *QAResumeScheduler) Done(ctx context.Context) error {
	defer updateAppCharSize(ctx, d.dao, d.p.RobotID, d.p.CorpID)
	log.DebugContextf(ctx, "task(QAResume) Done")
	filter := &dao.DocQaFilter{
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
		BusinessIds: d.p.QABizIDs(),
	}
	qas, err := dao.GetDocQaDao().GetAllDocQas(ctx, dao.DocQaTblColList, filter)
	if err != nil {
		return err
	}
	return d.qaResumeNoticeSuccess(ctx, qas)
}

func (d *QAResumeScheduler) qaResumeNoticeSuccess(ctx context.Context, qas []*model.DocQA) error {
	if len(qas) == 0 {
		return nil
	}
	qa := qas[0]
	log.DebugContextf(ctx, "task(QAResume) Done qaResumeNoticeSuccess, botID: %+v, qa count: %+v",
		qa.RobotID, len(qas))

	appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(QAResume) qaResumeNoticeSuccess, GetAppByID err: %+v", err)
		return err
	}

	operations := []model.Operation{}
	var content string
	if appDB.IsShared {
		content = i18n.Translate(ctx, i18nkey.KeyQARestoreSuccessWithNameAndCountNotRelease, qa.Question, len(qas))
	} else {
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyQARestoreSuccessWithNameAndCount, qa.Question, len(qas)))
	}
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeQAPageID),
		model.WithLevel(model.LevelSuccess),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyQARestoreSuccess)),
		model.WithContent(content),
	}

	notice := model.NewNotice(model.NoticeTypeQAResume, qa.ID, d.p.CorpID, qa.RobotID, d.p.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "task(QAResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	log.DebugContextf(ctx, "task(QAResume) Done, CreateNotice notice: %+v", notice)
	if err := d.dao.CreateNotice(ctx, notice); err != nil {
		log.ErrorContextf(ctx, "task(QAResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}

func (d *QAResumeScheduler) qaResumeNoticeError(ctx context.Context, qa *model.DocQA) error {
	log.DebugContextf(ctx, "task(QAResume) Process qaResumeNoticeError, botID: %+v, qa: %+v", qa.RobotID, qa.Question)
	operations := make([]model.Operation, 0)
	corp, err := d.dao.GetCorpByID(ctx, qa.CorpID)
	if err != nil {
		return err
	}
	isSystemIntegrator := d.dao.IsSystemIntegrator(ctx, corp)
	if !isSystemIntegrator {
		// 非系统集成商才需要额外增加跳转
		operations = append(operations, model.Operation{Typ: model.OpTypeExpandCapacity, Params: model.OpParams{}})
	}
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeQAPageID),
		model.WithLevel(model.LevelWarning),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyQARestoreFailure)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficientQARestoreFailureWithName, qa.Question)),
	}
	notice := model.NewNotice(model.NoticeTypeQAResume, qa.ID, d.p.CorpID, qa.RobotID, d.p.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "task(QAResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	log.DebugContextf(ctx, "task(QAResume) Done, CreateNotice notice: %+v", notice)
	if err := d.dao.CreateNotice(ctx, notice); err != nil {
		log.ErrorContextf(ctx, "task(QAResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}
