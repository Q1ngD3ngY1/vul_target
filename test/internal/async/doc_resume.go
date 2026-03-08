package async

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/adp/kb/kb-config/internal/util"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	docResumeDosagePrefix = "docResume:dosage:"
)

// DocResumeTaskHandler 资源包到期后处理离线任务
type DocResumeTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.DocResumeParams
}

func registerDocResumeScheduler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocResumeTask,
		func(t task_scheduler.Task, params entity.DocResumeParams) task_scheduler.TaskHandler {
			return &DocResumeTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocResumeTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocResume) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	docs, err := d.docLogic.GetDocByBizIDs(ctx, d.p.DocBizIDs(), d.p.RobotID)
	if err != nil {
		return kv, err
	}
	for _, doc := range docs {
		kv[fmt.Sprintf("%s%d", docResumeDosagePrefix, doc.ID)] = fmt.Sprintf("%d", doc.ID)
	}
	return kv, nil
}

// Init 初始化
func (d *DocResumeTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// resumeDoc 恢复文档
func (d *DocResumeTaskHandler) resumeDoc(ctx context.Context, doc *docEntity.Doc) error {
	logx.D(ctx, "task(DocResume) Process, task: %+v, resumeDoc: %+v", d.task.ID, doc.ID)
	app, err := d.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	err = d.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
		App:                  app,
		NewCharSize:          doc.CharSize,
		NewKnowledgeCapacity: doc.FileSize,
		NewStorageCapacity:   gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, 0, doc.FileSize),
		NewComputeCapacity:   doc.FileSize,
	})
	if err != nil {
		if errors.Is(err, errs.ErrOverCharacterSizeLimit) {
			_ = d.docResumeNoticeError(ctx, doc)
		}
		return common.ConvertErrMsg(ctx, d.rpc, 0, d.p.CorpID, err)
	}
	switch doc.Status {
	case docEntity.DocStatusResuming:
		doc.Status = docEntity.DocStatusWaitRelease
	case docEntity.DocStatusParseImportFailResuming:
		doc.Status = docEntity.DocStatusParseImportFail
	case docEntity.DocStatusAuditFailResuming:
		doc.Status = docEntity.DocStatusAuditFail
	case docEntity.DocStatusUpdateFailResuming:
		doc.Status = docEntity.DocStatusUpdateFail
	case docEntity.DocStatusCreateIndexFailResuming:
		doc.Status = docEntity.DocStatusCreateIndexFail
	case docEntity.DocStatusExpiredResuming:
		doc.Status = docEntity.DocStatusExpired
	case docEntity.DocStatusAppealFailedResuming:
		doc.Status = docEntity.DocStatusAppealFailed
	}
	if _, err := d.resumeDocQAs(ctx, doc); err != nil {
		logx.E(ctx, "task(DocResume) resumeDocQAs err:%+v", err)
		return err
	}
	if err := d.resumeSegments(ctx, doc); err != nil {
		return err
	}
	if err := d.docLogic.UpdateDocStatus(ctx, doc); err != nil {
		return err
	}
	return nil
}

// resumeSegments 恢复文档切片
func (d *DocResumeTaskHandler) resumeSegments(ctx context.Context, doc *docEntity.Doc) error {
	pageSize := uint32(100)
	segLen := pageSize
	// page := uint32(0)
	vectorLabels, err := getDocVectorLabels(ctx, doc, d.rpc, d.userLogic, d.cateLogic, d.labelLogic)
	if err != nil {
		return err
	}
	allSegs := make([]*segEntity.DocSegmentExtend, 0)
	for segLen > 0 {
		segs, err := d.segLogic.GetSegmentDeletedList(ctx, d.p.CorpID, doc.ID, 1, pageSize, doc.RobotID)
		if err != nil {
			logx.E(ctx, "task(DocResume) Process, task: %+v, ResumeSegment err:%+v", d.task.ID, err)
			return err
		}
		allSegs = append(allSegs, segs...)
		segLen = uint32(len(segs))
		if err := d.segLogic.ResumeSegments(ctx, segs, d.p.RobotID); err != nil {
			logx.E(ctx, "task(DocResume) Process, task: %+v, ResumeSegments err: %+v", d.task.ID, err)
			return err
		}
		// 实际在按页删除的过程中已经将page数减少了
		// page++
	}
	text2SQLSegmentMeta, err := getText2sqlSegmentMeta(ctx, doc, d.segLogic)
	if err != nil {
		return err
	}
	for _, seg := range allSegs {
		if seg.IsSegmentForQA() {
			continue
		}
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, seg.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(DocResume) appDB.HasDeleted()|appID:%d", seg.RobotID)
			return nil
		}
		embeddingVersion := appDB.Embedding.Version
		embeddingModel := ""
		logx.I(ctx, "task(DocResume) get embeddingModel. appBizID:%d, appDBID:%+d",
			appDB.BizId, appDB.PrimaryId)
		embeddingModel, err = d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)
		if err != nil {
			logx.E(ctx, "task(DocResume) GetShareKnowledgeBaseConfig err:%+v", err)
			return err
		}
		if embeddingModel != "" {
			embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
		}
		logx.I(ctx, "task(DocResume) get embeddingModel. appBizID:%d, embeddingModel:%s, embeddingVersion:%s",
			appDB.BizId, embeddingModel, embeddingVersion)
		switch seg.SegmentType {
		case segEntity.SegmentTypeText2SQLMeta:
			// ignore, do nothing
		case segEntity.SegmentTypeText2SQLContent:
			if err = d.addText2sqlSegment(ctx, seg, text2SQLSegmentMeta, vectorLabels, d.p.RobotID); err != nil {
				return err
			}
		default:
			botBizID, err := d.dao.GetBotBizIDByID(ctx, seg.RobotID)
			if err != nil {
				logx.E(ctx, "新增分片知识 GetBotBizIDByID:%+v err:%+v", seg.RobotID, err)
				return err
			}
			req := retrieval.BatchAddKnowledgeReq{
				RobotId:            seg.RobotID,
				IndexId:            entity.SegmentReviewVersionID,
				DocType:            entity.DocTypeSegment,
				EmbeddingVersion:   embeddingVersion,
				EmbeddingModelName: embeddingModel,
				BotBizId:           botBizID,
				Knowledge: []*retrieval.KnowledgeData{{
					Id:          seg.ID,
					SegmentType: seg.SegmentType,
					DocId:       seg.DocID,
					PageContent: seg.PageContent,
					Labels:      vectorLabels,
					ExpireTime:  seg.GetExpireTime(),
				}},
			}
			logx.I(ctx, "resumeSegments|AddKnowledge|req:%+v", &req)
			rsp, err := d.rpc.RetrievalDirectIndex.BatchAddKnowledge(ctx, &req)
			if err != nil {
				logx.E(ctx, "resumeSegments|AddKnowledge|err:%v", err)
				return err
			}
			logx.I(ctx, "resumeSegments|AddKnowledge|rsp:%+v", rsp)
		}
	}
	return d.resumeBigDataElastic(ctx, doc)
}

func (d *DocResumeTaskHandler) addText2sqlSegment(ctx context.Context, seg *segEntity.DocSegmentExtend, meta segEntity.Text2SQLSegmentMeta,
	vectorLabels []*retrieval.VectorLabel, robotID uint64) error {
	content := segEntity.Text2SQLSegmentContent{}
	err := jsonx.Unmarshal([]byte(seg.PageContent), &content)
	if err != nil {
		logx.E(ctx, "addText2sqlSegment|Unmarshal|DocID:%d|PageContent:%s|err:%+v", seg.DocID,
			seg.PageContent, err)
		return err
	}
	for _, tableMeta := range meta.TableMetas {
		// 先判断meta的tableID和content的是否一样，不是同个sheet的就跳过
		if content.TableID != tableMeta.TableID {
			logx.I(ctx, "addText2sqlSegment|DocID:%d|content.TableID:%s|tableMeta.TableID:%s NOT equal",
				seg.DocID, content.TableID, tableMeta.TableID)
			continue
		}
		text2SQLMeta, rows, err := buildMetaAndRow(ctx, tableMeta, content, seg)
		if err != nil {
			logx.W(ctx, "addText2sqlSegment|buildMetaAndRow|DocID:%d|SegID:%d|err:%+v",
				seg.DocID, seg.ID, err)
			seg.ReleaseStatus = segEntity.SegmentReleaseStatusNotRequired
			err := d.segLogic.UpdateSegmentReleaseStatus(ctx, seg, robotID)
			if err != nil {
				logx.E(ctx, "addText2sqlSegment|UpdateSegmentReleaseStatus|DocID:%d|SegID:%d|err:%+v",
					seg.DocID, seg.ID, err)
			}
			continue
		}
		req := retrieval.AddText2SQLReq{
			RobotId:    robotID,
			DocId:      seg.DocID,
			Meta:       text2SQLMeta,
			Rows:       rows,
			Labels:     vectorLabels,
			ExpireTime: seg.GetExpireTime(),
			FileName:   meta.FileName,
			CorpId:     seg.CorpID,
			DisableEs:  false,
		}
		_, err = d.rpc.RetrievalDirectIndex.AddText2SQL(ctx, &req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DocResumeTaskHandler) resumeBigDataElastic(ctx context.Context, doc *docEntity.Doc) error {
	req := &retrieval.RecoverBigDataElasticReq{
		RobotId: doc.RobotID,
		DocId:   doc.ID,
	}
	if _, err := d.rpc.RetrievalDirectIndex.RecoverBigDataElastic(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *DocResumeTaskHandler) resumeDocQA(ctx context.Context, qa *qaEntity.DocQA) error {
	switch qa.ReleaseStatus {
	case qaEntity.QAReleaseStatusCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusInit
	case qaEntity.QAReleaseStatusAppealFailCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAppealFail
	case qaEntity.QAReleaseStatusAuditNotPassCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPass
	case qaEntity.QAReleaseStatusLearnFailCharExceeded:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusLearnFail
	default:
		return nil
	}
	// 这里除了需要更新 QA 外，还需要更新相似问
	sqs, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx, "task(DocResume) Process, task: %+v, GetSimilarQuestionsByQA err:%+v", d.task.ID, err)
		// 柔性放过
	}
	sqm := &qaEntity.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	if err := d.qaLogic.UpdateQA(ctx, qa, sqm, true, false, 0, 0, &labelEntity.UpdateQAAttributeLabelReq{}); err != nil {
		logx.D(ctx, "task(DocResume) Process, task: %+v, bot: %+v, qaID: %+v, UpdateQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		return err
	}
	return nil
}

func (d *DocResumeTaskHandler) resumeDocQAs(ctx context.Context, doc *docEntity.Doc) (uint64, error) {
	docQAList, err := getBotQAList(ctx, d.p.CorpID, doc.RobotID, doc.ID, d.qaLogic)
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
func (d *DocResumeTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocResume) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(DocResume) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(DocResume) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(DocResume) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, docResumeDosagePrefix) {
			doc, err := d.docLogic.GetDocByID(ctx, id, d.p.RobotID)
			if err != nil {
				logx.E(ctx, "task(DocResume) GetDocByBizID kv:%s err:%+v", key, err)
				return err
			}
			if err := d.resumeDoc(newCtx, doc); err != nil {
				logx.E(ctx, "task(DocResume) resumeDoc kv:%s err:%+v", key, err)
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(DocResume) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(DocResume) Finish kv:%s", key)
	}
	return nil
}

// Fail 任务失败
func (d *DocResumeTaskHandler) Fail(ctx context.Context) error {
	defer d.taskCommon.updateAppCharSize(ctx, d.p.RobotID, d.p.CorpID)
	logx.D(ctx, "task(DocResume) Fail")
	docM, err := d.docLogic.GetDocByBizIDs(ctx, d.p.DocBizIDs(), d.p.RobotID)
	if err != nil {
		logx.E(ctx, "task(DocResume) Fail, GetDocByBizIDs err: %+v", err)
		return err
	}
	updateM := map[uint64]time.Time{}
	for _, v := range d.p.DocExceededTimes {
		updateM[v.BizID] = v.UpdateTime
	}
	for _, doc := range docM {
		switch doc.Status {
		case docEntity.DocStatusParseImportFailResuming:
			doc.Status = docEntity.DocStatusParseImportFailCharExceeded
		case docEntity.DocStatusAuditFailResuming:
			doc.Status = docEntity.DocStatusAuditFailCharExceeded
		case docEntity.DocStatusUpdateFailResuming:
			doc.Status = docEntity.DocStatusUpdateFailCharExceeded
		case docEntity.DocStatusCreateIndexFailResuming:
			doc.Status = docEntity.DocStatusCreateIndexFailCharExceeded
		case docEntity.DocStatusExpiredResuming:
			doc.Status = docEntity.DocStatusExpiredCharExceeded
		case docEntity.DocStatusResuming:
			doc.Status = docEntity.DocStatusCharExceeded
		case docEntity.DocStatusAppealFailedResuming:
			doc.Status = docEntity.DocStatusAppealFailedCharExceeded
		default:
			continue
		}
		v, ok := updateM[doc.BusinessID]
		if !ok {
			continue
		}
		// 还原更新时间
		doc.UpdateTime = v
		logx.W(ctx, "task(DocResume) Fail reset doc %+v status: %+v, update_time: %+v", doc.ID, doc.Status, v)
		if err := d.docLogic.UpdateDocStatusAndUpdateTime(ctx, doc); err != nil {
			logx.W(ctx, "task(DocResume) Fail reset doc %+v status err: %+v", doc.ID, err)
		}
	}
	return nil
}

// Stop 任务停止
func (d *DocResumeTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocResumeTaskHandler) Done(ctx context.Context) error {
	defer d.taskCommon.updateAppCharSize(ctx, d.p.RobotID, d.p.CorpID)
	logx.D(ctx, "task(DocResume) Done")
	docM, err := d.docLogic.GetDocByBizIDs(ctx, d.p.DocBizIDs(), d.p.RobotID)
	if err != nil {
		logx.E(ctx, "task(DocResume) Done, GetDocByBizIDs err: %+v", err)
		return err
	}
	docBizM := map[uint64]*docEntity.Doc{}
	for _, doc := range docM {
		docBizM[doc.BusinessID] = doc
	}
	if len(docBizM) == 0 {
		logx.W(ctx, "task(DocResume) Done, GetDocByBizIDs count: 0")
		return nil
	}

	return d.docResumeNoticeSuccess(ctx, docBizM)
}

func (d *DocResumeTaskHandler) docResumeNoticeSuccess(ctx context.Context, docM map[uint64]*docEntity.Doc) error {
	firstDoc, ok := docM[d.p.DocBizIDs()[0]]
	if !ok {
		return nil
	}
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "task(DocResume) docResumeNoticeSuccess, DescribeAppByPrimaryIdWithoutNotFoundError err: %+v", err)
		return err
	}
	logx.D(ctx, "task(DocResume) Done docResumeNoticeSuccess, botID: %+v, doc count: %+v", firstDoc.RobotID, len(docM))
	operations := []releaseEntity.Operation{}
	var content string
	if appDB.IsShared {
		content = i18n.Translate(ctx, i18nkey.KeyDocumentRestoreSuccessWithNameAndCountNotRelease, firstDoc.FileName, len(docM))
	} else {
		content = i18n.Translate(ctx, i18nkey.KeyDocumentRestoreSuccessWithNameAndCount, firstDoc.FileName, len(docM))
	}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelSuccess),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentRestoreSuccess)),
		releaseEntity.WithContent(content),
	}

	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocResume, firstDoc.ID, d.p.CorpID, firstDoc.RobotID, d.p.StaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "task(DocResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	logx.D(ctx, "task(DocResume) Done, CreateNotice notice: %+v", notice)
	if err := d.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "task(DocResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}

func (d *DocResumeTaskHandler) docResumeNoticeError(ctx context.Context, doc *docEntity.Doc) error {
	logx.D(ctx, "task(DocResume) Process docResumeNoticeError, botID: %+v, doc: %+v", doc.RobotID,
		doc.FileName)
	operations := make([]releaseEntity.Operation, 0)
	corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
	// corp, err := d.dao.GetCorpByID(ctx, doc.CorpPrimaryId)
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
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelWarning),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentRestoreFailure)),
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficientDocumentRestoreFailureWithName, doc.FileName)),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocResume, doc.ID, d.p.CorpID, doc.RobotID, d.p.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "task(DocResume) Done, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	logx.D(ctx, "task(DocResume) Done, CreateNotice notice: %+v", notice)
	if err := d.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "task(DocResume) Done, CreateNotice notice: %+v err: %+v", notice, err)
		return err
	}
	return nil
}
