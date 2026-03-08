package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"
)

const (
	docResumeDosagePrefix = "docResume:dosage:"
)

// DocResumeScheduler 资源包到期后处理离线任务
type DocResumeScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.DocResumeParams
}

func initDocResumeScheduler() {
	task_scheduler.Register(
		model.DocResumeTask,
		func(t task_scheduler.Task, params model.DocResumeParams) task_scheduler.TaskHandler {
			return &DocResumeScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocResumeScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocResume) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	docs, err := d.dao.GetDocByBizIDs(ctx, d.p.DocBizIDs(), d.p.RobotID)
	if err != nil {
		return kv, err
	}
	for _, doc := range docs {
		kv[fmt.Sprintf("%s%d", docResumeDosagePrefix, doc.ID)] = fmt.Sprintf("%d", doc.ID)
	}
	return kv, nil
}

// Init 初始化
func (d *DocResumeScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// resumeDoc 恢复文档
func (d *DocResumeScheduler) resumeDoc(ctx context.Context, doc *model.Doc) error {
	log.DebugContextf(ctx, "task(DocResume) Process, task: %+v, resumeDoc: %+v", d.task.ID, doc.ID)
	_, err := d.checkUsedCharSizeExceeded(ctx, d.p.CorpID, doc.CharSize)
	if err != nil {
		if err == errs.ErrOverCharacterSizeLimit {
			_ = d.docResumeNoticeError(ctx, doc)
		}
		return d.dao.ConvertErrMsg(ctx, 0, d.p.CorpID, err)
	}
	switch doc.Status {
	case model.DocStatusResuming:
		doc.Status = model.DocStatusWaitRelease
	case model.DocStatusParseImportFailResuming:
		doc.Status = model.DocStatusParseImportFail
	case model.DocStatusAuditFailResuming:
		doc.Status = model.DocStatusAuditFail
	case model.DocStatusUpdateFailResuming:
		doc.Status = model.DocStatusUpdateFail
	case model.DocStatusCreateIndexFailResuming:
		doc.Status = model.DocStatusCreateIndexFail
	case model.DocStatusExpiredResuming:
		doc.Status = model.DocStatusExpired
	case model.DocStatusAppealFailedResuming:
		doc.Status = model.DocStatusAppealFailed
	}
	if _, err := d.resumeDocQAs(ctx, doc); err != nil {
		log.ErrorContextf(ctx, "task(DocResume) resumeDocQAs err:%+v", err)
		return err
	}
	if err := d.resumeSegments(ctx, doc); err != nil {
		return err
	}
	if err := d.dao.UpdateDocStatus(ctx, doc); err != nil {
		return err
	}
	return nil
}
func (d *DocResumeScheduler) checkUsedCharSizeExceeded(ctx context.Context, corpID uint64, tmpCharSize uint64) (uint64,
	error) {
	corp, err := d.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		return 0, errs.ErrCorpNotFound
	}
	log.DebugContextf(ctx, "task(DocResume) Process, task: %+v, corp: %+v", d.task.ID, corp)
	corp, err = d.dao.GetCorpBillingInfo(ctx, corp)
	if err != nil {
		return 0, errs.ErrCorpNotFound
	}
	usedCharSize, err := d.dao.GetCorpUsedCharSizeUsage(ctx, corpID)
	if err != nil {
		return 0, errs.ErrSystem
	}
	usedCharSize += tmpCharSize // 加上当前需要恢复的charsize
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

// resumeSegments 恢复文档切片
func (d *DocResumeScheduler) resumeSegments(ctx context.Context, doc *model.Doc) error {
	pageSize := uint32(100)
	segLen := pageSize
	// page := uint32(0)
	vectorLabels, err := getDocVectorLabels(ctx, doc, d.dao)
	if err != nil {
		return err
	}
	allSegs := make([]*model.DocSegmentExtend, 0)
	for segLen > 0 {
		segs, err := d.dao.GetSegmentDeletedList(ctx, d.p.CorpID, doc.ID, 1, pageSize, doc.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocResume) Process, task: %+v, ResumeSegment err:%+v", d.task.ID, err)
			return err
		}
		allSegs = append(allSegs, segs...)
		segLen = uint32(len(segs))
		if err := d.dao.ResumeSegments(ctx, segs, d.p.RobotID); err != nil {
			log.ErrorContextf(ctx, "task(DocResume) Process, task: %+v, ResumeSegments err: %+v", d.task.ID, err)
			return err
		}
		// 实际在按页删除的过程中已经将page数减少了
		// page++
	}
	text2SQLSegmentMeta, err := getText2sqlSegmentMeta(ctx, doc, d.dao)
	if err != nil {
		return err
	}
	corpBizId, err := dao.GetCorpBizIDByCorpID(ctx, d.p.CorpID)
	if err != nil {
		return err
	}
	appBizId, err := dao.GetAppBizIDByAppID(ctx, d.p.RobotID)
	if err != nil {
		return err
	}
	embeddingModelName, err := logicKnowConfig.GetKnowledgeBaseConfig(ctx, corpBizId, appBizId,
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
	if err != nil {
		return err
	}
	for _, seg := range allSegs {
		if seg.IsSegmentForQA() {
			continue
		}
		appDB, err := d.dao.GetAppByID(ctx, seg.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(DocResume) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			return nil
		}
		embeddingConf, _, err := appDB.GetEmbeddingConf()
		if err != nil {
			log.ErrorContextf(ctx,
				"task(DocResume) Process, 向量同步,查询机器人数据失败 robots[0].GetEmbeddingConf() err:%+v", err)
			return err
		}
		embeddingVersion := embeddingConf.Version
		switch seg.SegmentType {
		case model.SegmentTypeText2SQLMeta:
			// ignore, do nothing
		case model.SegmentTypeText2SQLContent:
			if err = addText2sqlSegment(ctx, d.dao, seg, text2SQLSegmentMeta, vectorLabels, d.p.RobotID); err != nil {
				return err
			}
		default:
			if err = d.dao.DirectAddSegmentKnowledge(ctx, seg, embeddingVersion, vectorLabels, embeddingModelName); err != nil {
				return err
			}
		}
	}
	return d.resumeBigDataElastic(ctx, doc)
}

func (d *DocResumeScheduler) resumeBigDataElastic(ctx context.Context, doc *model.Doc) error {
	req := &bot_retrieval_server.RecoverBigDataElasticReq{
		RobotId: doc.RobotID,
		DocId:   doc.ID,
	}
	if _, err := d.dao.RecoverBigDataElastic(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *DocResumeScheduler) resumeDocQA(ctx context.Context, qa *model.DocQA) error {
	switch qa.ReleaseStatus {
	case model.QAReleaseStatusCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusInit
	case model.QAReleaseStatusAppealFailCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusAppealFail
	case model.QAReleaseStatusAuditNotPassCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusAuditNotPass
	case model.QAReleaseStatusLearnFailCharExceeded:
		qa.ReleaseStatus = model.QAReleaseStatusLearnFail
	default:
		return nil
	}
	// 这里除了需要更新 QA 外，还需要更新相似问
	sqs, err := d.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocResume) Process, task: %+v, GetSimilarQuestionsByQA err:%+v", d.task.ID, err)
		// 柔性放过
	}
	sqm := &model.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	if err := d.dao.UpdateQA(ctx, qa, sqm, true, false, 0, &model.UpdateQAAttributeLabelReq{}); err != nil {
		log.DebugContextf(ctx, "task(DocResume) Process, task: %+v, bot: %+v, qaID: %+v, UpdateQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		return err
	}
	return nil
}

func (d *DocResumeScheduler) resumeDocQAs(ctx context.Context, doc *model.Doc) (uint64, error) {
	docQAList, err := getBotQAList(ctx, d.p.CorpID, doc.RobotID, doc.ID, d.dao)
	if err != nil {
		return 0, err
	}
	docQACharSize := uint64(0)
	for _, qa := range docQAList {
		docQACharSize += uint64(qa.CharSize)
		if err := d.resumeDocQA(ctx, qa); err != nil {
			return 0, err
		}
	}
	return docQACharSize, nil
}

// Process 任务处理
func (d *DocResumeScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocResume) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(DocResume) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(DocResume) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocResume) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, docResumeDosagePrefix) {
			doc, err := d.dao.GetDocByID(ctx, id, d.p.RobotID)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocResume) GetDocByBizID kv:%s err:%+v", key, err)
				return err
			}
			if err := d.resumeDoc(ctx, doc); err != nil {
				log.ErrorContextf(ctx, "task(DocResume) resumeDoc kv:%s err:%+v", key, err)
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(DocResume) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocResume) Finish kv:%s", key)
	}
	return nil
}

// Fail 任务失败
func (d *DocResumeScheduler) Fail(ctx context.Context) error {
	defer updateAppCharSize(ctx, d.dao, d.p.RobotID, d.p.CorpID)
	log.DebugContextf(ctx, "task(DocResume) Fail")
	docM, err := d.dao.GetDocByBizIDs(ctx, d.p.DocBizIDs(), d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocResume) Fail, GetDocByBizIDs err: %+v", err)
		return err
	}
	updateM := map[uint64]time.Time{}
	for _, v := range d.p.DocExceededTimes {
		updateM[v.BizID] = v.UpdateTime
	}
	for _, doc := range docM {
		switch doc.Status {
		case model.DocStatusParseImportFailResuming:
			doc.Status = model.DocStatusParseImportFailCharExceeded
		case model.DocStatusAuditFailResuming:
			doc.Status = model.DocStatusAuditFailCharExceeded
		case model.DocStatusUpdateFailResuming:
			doc.Status = model.DocStatusUpdateFailCharExceeded
		case model.DocStatusCreateIndexFailResuming:
			doc.Status = model.DocStatusCreateIndexFailCharExceeded
		case model.DocStatusExpiredResuming:
			doc.Status = model.DocStatusExpiredCharExceeded
		case model.DocStatusResuming:
			doc.Status = model.DocStatusCharExceeded
		case model.DocStatusAppealFailedResuming:
			doc.Status = model.DocStatusAppealFailedCharExceeded
		default:
			continue
		}
		v, ok := updateM[doc.BusinessID]
		if !ok {
			continue
		}
		// 还原更新时间
		doc.UpdateTime = v
		log.WarnContextf(ctx, "task(DocResume) Fail reset doc %+v status: %+v, update_time: %+v", doc.ID, doc.Status, v)
		if err := d.dao.UpdateDocStatusAndUpdateTime(ctx, doc); err != nil {
			log.WarnContextf(ctx, "task(DocResume) Fail reset doc %+v status err: %+v", doc.ID, err)
		}
	}
	return nil
}

// Stop 任务停止
func (d *DocResumeScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocResumeScheduler) Done(ctx context.Context) error {
	defer updateAppCharSize(ctx, d.dao, d.p.RobotID, d.p.CorpID)
	log.DebugContextf(ctx, "task(DocResume) Done")
	docM, err := d.dao.GetDocByBizIDs(ctx, d.p.DocBizIDs(), d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocResume) Done, GetDocByBizIDs err: %+v", err)
		return err
	}
	docBizM := map[uint64]*model.Doc{}
	for _, doc := range docM {
		docBizM[doc.BusinessID] = doc
	}
	if len(docBizM) == 0 {
		log.WarnContextf(ctx, "task(DocResume) Done, GetDocByBizIDs count: 0")
		return nil
	}

	return d.docResumeNoticeSuccess(ctx, docBizM)
}

func (d *DocResumeScheduler) docResumeNoticeSuccess(ctx context.Context, docM map[uint64]*model.Doc) error {
	firstDoc, ok := docM[d.p.DocBizIDs()[0]]
	if !ok {
		return nil
	}
	appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocResume) docResumeNoticeSuccess, GetAppByID err: %+v", err)
		return err
	}
	log.DebugContextf(ctx, "task(DocResume) Done docResumeNoticeSuccess, botID: %+v, doc count: %+v", firstDoc.RobotID,
		len(docM))
	operations := []model.Operation{}
	var content string
	if appDB.IsShared {
		content = i18n.Translate(ctx, i18nkey.KeyDocumentRestoreSuccessWithNameAndCountNotRelease, firstDoc.FileName, len(docM))
	} else {
		content = i18n.Translate(ctx, i18nkey.KeyDocumentRestoreSuccessWithNameAndCount, firstDoc.FileName, len(docM))
	}
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelSuccess),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentRestoreSuccess)),
		model.WithContent(content),
	}

	notice := model.NewNotice(model.NoticeTypeDocResume, firstDoc.ID, d.p.CorpID, firstDoc.RobotID, d.p.StaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "task(DocResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	log.DebugContextf(ctx, "task(DocResume) Done, CreateNotice notice: %+v", notice)
	if err := d.dao.CreateNotice(ctx, notice); err != nil {
		log.ErrorContextf(ctx, "task(DocResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}

func (d *DocResumeScheduler) docResumeNoticeError(ctx context.Context, doc *model.Doc) error {
	log.DebugContextf(ctx, "task(DocResume) Process docResumeNoticeError, botID: %+v, doc: %+v", doc.RobotID,
		doc.FileName)
	operations := make([]model.Operation, 0)
	corp, err := d.dao.GetCorpByID(ctx, doc.CorpID)
	if err != nil {
		return err
	}
	isSystemIntegrator := d.dao.IsSystemIntegrator(ctx, corp)
	if !isSystemIntegrator {
		// 非系统集成商才需要额外增加跳转
		operations = append(operations, model.Operation{Typ: model.OpTypeExpandCapacity, Params: model.OpParams{}})
	}
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelWarning),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentRestoreFailure)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficientDocumentRestoreFailureWithName, doc.FileName)),
	}
	notice := model.NewNotice(model.NoticeTypeDocResume, doc.ID, d.p.CorpID, doc.RobotID, d.p.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "task(DocResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	log.DebugContextf(ctx, "task(DocResume) Done, CreateNotice notice: %+v", notice)
	if err := d.dao.CreateNotice(ctx, notice); err != nil {
		log.ErrorContextf(ctx, "task(DocResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}
