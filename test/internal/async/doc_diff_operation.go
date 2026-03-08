package async

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

const (
	docOperationPrefix = "doc:operation:"
	segToQAPrefix      = "seg:to:qa:"

	waitDocRenameSleepTime = 1
	waitDocRenameTimeout   = 120
	docHasBeenDeleted      = "docHasBeenDeleted"
)

// qaOperationState 封装操作状态
type qaOperationState struct {
	ctx      context.Context
	d        *DocDiffOperationTaskHandler
	progress *task_scheduler.Progress
	newDoc   *docEntity.Doc
	oldDoc   *docEntity.Doc
	appDB    *entity.App
	taskKV   map[string]string

	message             string
	qaTask              *qaEntity.DocQATask
	qaOperationFinished bool
	err                 error
}

// 更新状态
func (s *qaOperationState) updateStatus() {
	if !s.qaOperationFinished && s.err == nil {
		logx.I(s.ctx, "task not finish yet, not update qa status, remain %v", s.progress.Remain())
		return
	}

	status := docEntity.DocDiffQAAndDocOpStatusSuccess
	if s.err != nil {
		status = docEntity.DocDiffQAAndDocOpStatusFailed
		if s.message == "" {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyQAOperationFailure)
		}
	}

	logx.I(s.ctx, "update qa operation status: %v, message: %v", status, s.message)
	update := &docEntity.DocDiff{
		QaOperationStatus: uint32(status),
		QaOperationResult: s.message,
	}
	updateColumns := []string{docEntity.DocDiffTaskTblColQaOperationStatus, docEntity.DocDiffTaskTblColQaOperationResult}
	_ = s.d.taskLogic.UpdateDocDiffTasks(s.ctx, updateColumns, s.d.p.CorpBizID, s.appDB.BizId,
		[]uint64{s.d.p.DocDiffID}, update)
}

// 处理文档片段生成问答
func (s *qaOperationState) handleSegmentToQA() error {
	logx.I(s.ctx, "begin to process segment to qa")
	segmentToQAType := docEntity.GetDocDiffSegmentToQAType(s.d.p.QAOperation)
	if segmentToQAType == docEntity.SegmentToQATypeNone {
		return nil
	}

	docForGenQa := s.newDoc
	if segmentToQAType == docEntity.SegmentToQATypeOldAll || segmentToQAType == docEntity.SegmentToQATypeOldUni {
		docForGenQa = s.oldDoc
	}

	uniqueQaMap := make(map[string]bool)
	var uniqueQas []*qaEntity.QA

	for key, value := range s.taskKV {
		//if !checkModelStatus(s.ctx, s.d.rpc, s.d.p.Uin, s.d.p.Sid, s.d.tokenDosage.ModelName) {
		if !s.d.financeLogic.CheckModelStatus(s.ctx, s.d.p.CorpID, s.d.tokenDosage.ModelName, rpc.DocExtractQABizType) {
			logx.W(s.ctx, "task(DocDiffOperation) checkModelStatus 余量不足 TaskKV, taskID %v", s.d.p.DocDiffID)
			s.message = i18n.Translate(s.ctx, i18nkey.KeyGenerateQAFailureNoResources)
			s.progress.Stop(s.ctx)
			s.err = s.d.taskLogic.StopQaTask(s.ctx, s.d.p.CorpID, s.d.p.RobotID, s.d.p.DocQATaskID, true, s.d.tokenDosage.ModelName)
			if s.err != nil {
				logx.E(s.ctx, "stop qa task failed")
				return s.err
			}
			s.err = fmt.Errorf(s.message)
			return s.err
		}

		s.err = s.d.segmentToQA(s.ctx, cast.ToUint64(value), s.appDB, docForGenQa, uniqueQaMap, uniqueQas)
		if s.err != nil {
			if errors.Is(s.err, errs.ErrOperateDoing) {
				logx.E(s.ctx, "task(DocDiffOperation) checkSegmentOrgDataExists err:%v", s.err)
				continue
			}
			_ = s.d.docToQAFailed(s.ctx, s.oldDoc, s.newDoc)
			return s.err
		}

		if s.err = s.progress.Finish(s.ctx, key); s.err != nil {
			logx.E(s.ctx, "task(DocDiffOperation) Finish kv:%s err:%+v", key, s.err)
			return s.err
		}
		logx.I(s.ctx, "finish segment to qa, key: %v", key)
	}

	if s.progress.Remain() != 0 {
		return nil
	}

	return s.d.docToQADone(s.ctx, s.oldDoc, s.newDoc)
}

// 2. 处理其他问答操作
func (s *qaOperationState) handleQAOperation(op docEntity.QAOperation) error {

	logx.I(s.ctx, "begin to process qa db operation")
	switch op {
	case docEntity.QAOperation0, docEntity.QAOperation6, docEntity.QAOperation15:
		// 不需要对问答做处理
		return nil
	case docEntity.QAOperation2, docEntity.QAOperation4, docEntity.QAOperation5,
		docEntity.QAOperation11, docEntity.QAOperation13, docEntity.QAOperation14,
		docEntity.QAOperation17, docEntity.QAOperation20:
		// 这些case没有其他的问答数据库操作
		return s.handleSimpleQACases(op)
	case docEntity.QAOperation1, docEntity.QAOperation19:
		// 将新文档的问答关联到旧文档
		return s.handleRebindQAToDoc(op, s.newDoc, s.oldDoc)
	case docEntity.QAOperation3, docEntity.QAOperation21:
		// 删除新文档生成的问答
		return s.handleDeleteQAs(op, s.newDoc)
	case docEntity.QAOperation7:
		// 2. 删除旧文档差异片段产生的QA
		return s.handleComplexCase7()
	case docEntity.QAOperation8, docEntity.QAOperation10, docEntity.QAOperation16:
		// 将旧文档全部问答关联到新文档
		return s.handleRebindQAToDoc(op, s.oldDoc, s.newDoc)
	case docEntity.QAOperation9, docEntity.QAOperation12, docEntity.QAOperation18:
		// 删除旧文档问答
		return s.handleDeleteQAs(op, s.oldDoc)
	default:
		return fmt.Errorf("qa operation type error, %v", op)
	}
}

func (s *qaOperationState) handleSimpleQACases(op docEntity.QAOperation) error {
	var qas []*qaEntity.DocQA
	var err error

	switch op {
	case docEntity.QAOperation2, docEntity.QAOperation20:
		qas, err = getDocNotDeleteQA(s.ctx, s.newDoc, s.d.qaLogic)
		if err != nil {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQARetained)
			logx.E(s.ctx, "get doc %v not delete qa failed, %v", s.newDoc.ID, err)
		} else {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQARetainedCount, len(qas))
		}
	case docEntity.QAOperation4, docEntity.QAOperation5:
		genQACount := s.getGenQACount()
		if genQACount != -1 {
			if op == docEntity.QAOperation4 {
				s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocDiffFragmentGenerateQACountPendingReview, genQACount)
			} else {
				s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocGenerateQACountPendingReview, genQACount)
			}
		} else {
			if op == docEntity.QAOperation4 {
				s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocDiffFragmentQAGenerateSuccessPendingReview)
			} else {
				s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocQAGenerateSuccessPendingReview)
			}
		}
	case docEntity.QAOperation11, docEntity.QAOperation17:
		qas, err = getDocNotDeleteQA(s.ctx, s.oldDoc, s.d.qaLogic)
		s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocQARetained)
		if err == nil {
			if len(qas) == 0 {
				s.message = i18n.Translate(s.ctx, i18nkey.KeyRetainOldDocQAFailureNoOldQA)
			} else {
				s.message += i18n.Translate(s.ctx, i18nkey.KeyTotalQACount, len(qas))
			}
		}
	case docEntity.QAOperation13:
		genQACount := s.getGenQACount()
		if genQACount != -1 {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocDiffFragmentGenerateQACountPendingReview, genQACount)
		} else {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocDiffFragmentQAGenerateSuccess)
		}
	case docEntity.QAOperation14:
		genQACount := s.getGenQACount()
		if genQACount != -1 {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocGenerateQACountPendingReview, genQACount)
		} else {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQAGenerateSuccessPendingReview)
		}
	}
	return nil
}

func (s *qaOperationState) getGenQACount() int {
	if docEntity.GetDocDiffSegmentToQAType(s.d.p.QAOperation) == docEntity.SegmentToQATypeNone {
		return -1
	}

	docQATask, err := s.d.taskLogic.GetDocQATaskByID(s.ctx, s.d.p.DocQATaskID, s.d.p.CorpID, s.d.p.RobotID)
	if err != nil || docQATask == nil {
		return -1
	}
	return int(docQATask.QACount)
}

func (s *qaOperationState) handleRebindQAToDoc(op docEntity.QAOperation, fromDoc, toDoc *docEntity.Doc) error {
	qaCount, err := s.d.rebindQAToDoc(s.ctx, fromDoc, toDoc)
	if err != nil {
		if op == docEntity.QAOperation1 || op == docEntity.QAOperation19 {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQAAssociateOldDocFailure)
		} else {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocQAAssociateNewDocFailure)
		}
		return err
	}

	if qaCount == 0 {
		if op == docEntity.QAOperation1 || op == docEntity.QAOperation19 {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQAAssociateOldDocFailureNoNewQA)
			return fmt.Errorf("no qa is new doc")
		} else {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocQAAssociateNewDocFailureNoNewQA)
			return fmt.Errorf("no qa is old doc")
		}
	}

	switch op {
	case docEntity.QAOperation1, docEntity.QAOperation19:
		s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQAAssociatedToOldDocWithDetails, qaCount)
	case docEntity.QAOperation8:
		s.qaTask, err = s.d.taskLogic.GetDocQATaskByID(s.ctx, s.d.p.DocQATaskID, s.d.p.CorpID, s.d.p.RobotID)
		if err == nil {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocDiffFragmentGenerateQACountPendingReviewWithDot, s.qaTask.QACount)
		} else {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocDiffFragmentQAGeneratedPendingReview)
		}
		s.message += i18n.Translate(s.ctx, i18nkey.KeyRetainQAAssociateNewDoc, qaCount)
	case docEntity.QAOperation10, docEntity.QAOperation16:
		s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocQAAssociatedToNewDocWithDetails, qaCount)
	}
	return nil
}

func (s *qaOperationState) handleDeleteQAs(op docEntity.QAOperation, doc *docEntity.Doc) error {
	qas, err := getDocNotDeleteQA(s.ctx, doc, s.d.qaLogic)
	if err != nil {
		logx.E(s.ctx, "get doc %v not delete qa failed, %v", doc.ID, err)
		return err
	}

	if op == docEntity.QAOperation3 || op == docEntity.QAOperation21 {
		s.message = i18n.Translate(s.ctx, i18nkey.KeyDeleteNewDocQAFailure)
	} else {
		s.message = i18n.Translate(s.ctx, i18nkey.KeyDeleteOldDocQAFailure)
	}

	if err = s.d.qaLogic.DeleteQAs(s.ctx, s.d.p.CorpID, s.d.p.RobotID, s.d.p.StaffID, qas); err != nil {
		return err
	}

	logx.I(s.ctx, "delete doc %v %v %v qa success", doc.ID, doc.FileName, len(qas))

	if op == docEntity.QAOperation3 || op == docEntity.QAOperation21 {
		s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQADeletedCount, len(qas))
	} else if op == docEntity.QAOperation9 {
		s.qaTask, err = s.d.taskLogic.GetDocQATaskByID(s.ctx, s.d.p.DocQATaskID, s.d.p.CorpID, s.d.p.RobotID)
		if err == nil {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocGenerateQACountPendingReviewWithDot, s.qaTask.QACount)
		} else {
			s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocQAGeneratedPendingReview)
		}
		if s.message != "" {
			s.message = "\n" + s.message
		}
		s.message += i18n.Translate(s.ctx, i18nkey.KeyOldDocDeleteQACountPublishEffective, len(qas))
	} else {
		s.message = i18n.Translate(s.ctx, i18nkey.KeyOldDocDeleteQACountPublishEffective, len(qas))
	}
	return nil
}

func (s *qaOperationState) handleComplexCase7() error {
	// 1. 删除旧文档差异片段产生的QA
	segmentDiff, err := s.d.diffDocSegment(s.ctx, s.oldDoc, s.newDoc)
	if err != nil {
		return err
	}

	var segmentIDs []uint64
	for _, seg := range segmentDiff.oldUni {
		segmentIDs = append(segmentIDs, seg.ID)
	}

	qas, err := s.d.qaLogic.GetQasBySegmentIDs(s.ctx, s.d.p.CorpID, s.oldDoc.ID, segmentIDs)
	if err != nil {
		logx.E(s.ctx, "GetQasBySegmentIDs error, %v", err)
		return err
	}

	if err = s.d.qaLogic.DeleteQAs(s.ctx, s.d.p.CorpID, s.d.p.RobotID, s.d.p.StaffID, qas); err != nil {
		logx.E(s.ctx, "DeleteQAs error, %v", err)
		return err
	}

	logx.I(s.ctx, "delete doc %v %v diff %v segment qa success", s.oldDoc.ID, s.oldDoc.FileName, len(qas))

	// 2. 将旧文档公共部分的QA关联到新文档
	qaCount, err := s.d.rebindQAToDoc(s.ctx, s.oldDoc, s.newDoc)
	if err != nil {
		return err
	}

	logx.I(s.ctx, "rebind qa from old doc common segment to new doc success, qa count %v", qaCount)

	s.qaTask, err = s.d.taskLogic.GetDocQATaskByID(s.ctx, s.d.p.DocQATaskID, s.d.p.CorpID, s.d.p.RobotID)
	if err == nil {
		s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocDiffFragmentGenerateQACountPendingReviewWithDot, s.qaTask.QACount)
	} else {
		s.message = i18n.Translate(s.ctx, i18nkey.KeyNewDocDiffFragmentQAGeneratedPendingReview)
	}
	s.message += i18n.Translate(s.ctx, i18nkey.KeyOldDocDeleteQARetainAssociateNewDoc, len(qas), qaCount)
	return nil
}

// DocDiffOperationTaskHandler 文档处理任务
type DocDiffOperationTaskHandler struct {
	*taskCommon

	task        task_scheduler.Task
	p           entity.DocDiffOperationParams
	tokenDosage finance.TokenDosage
}

func registerDocDiffOperationTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocDiffOperationTask,
		func(t task_scheduler.Task, params entity.DocDiffOperationParams) task_scheduler.TaskHandler {
			return &DocDiffOperationTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocDiffOperationTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.I(ctx, "task(DocDiffOperation) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)

	if docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) == docEntity.SegmentToQATypeNone {
		// 没有片段生成qa的操作，一次操作即可
		kv[docOperationPrefix+cast.ToString(d.p.DocDiffID)] = cast.ToString(d.p.DocDiffID)
		return kv, nil
	}

	// 有生成qa的场景
	oldDoc, err := d.docLogic.GetDocByBizID(ctx, d.p.OldDocBizID, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "GetDocByBizID failed, %v", err)
		return nil, err
	}
	if oldDoc.HasDeleted() {
		logx.E(ctx, "task(DocDiffOperation) old doc %v has been deleted", d.p.OldDocBizID)
		return nil, errs.ErrDocNotFound
	}
	newDoc, err := d.docLogic.GetDocByBizID(ctx, d.p.NewDocBizID, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "GetDocByBizID failed, %v", err)
		return nil, err
	}
	if newDoc.HasDeleted() {
		logx.E(ctx, "task(DocDiffOperation) new doc %v has been deleted", d.p.NewDocBizID)
		return nil, errs.ErrDocNotFound
	}

	segmentDiff, err := d.diffDocSegment(ctx, oldDoc, newDoc)
	if err != nil {
		return kv, err
	}

	// 准备用于生成qa的segment
	var segmentsForGenQA []*segEntity.DocSegmentExtend
	tarDoc := newDoc
	segmentToQAType := docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation)
	if segmentToQAType == docEntity.SegmentToQATypeOldUni {
		segmentsForGenQA = segmentDiff.oldUni
		tarDoc = oldDoc
	} else if segmentToQAType == docEntity.SegmentToQATypeOldAll {
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.oldUni...)
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.oldCommon...)
		tarDoc = oldDoc
	} else if segmentToQAType == docEntity.SegmentToQATypeNewUni {
		segmentsForGenQA = segmentDiff.newUni
	} else if segmentToQAType == docEntity.SegmentToQATypeNewAll {
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.newUni...)
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.newCommon...)
	} else {
		return nil, fmt.Errorf("unexpect segmentToQAType %v", segmentToQAType)
	}

	docQATask, err := d.taskLogic.GetDocQATaskByID(ctx, d.p.DocQATaskID, d.p.CorpID, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "task(DocToQA) retry 获取生成问答任务详情失败 err:%+v", err)
		return kv, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		logx.I(ctx, "task(DocToQA) retry 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			d.p.CorpID, d.p.RobotID, d.p.DocQATaskID)
		return kv, errs.ErrDocQaTaskNotFound
	}

	filter := &qaEntity.DocQaTaskFilter{
		BusinessId: docQATask.BusinessID,
		CorpId:     d.p.CorpID,
		RobotId:    d.p.RobotID,
	}
	update := &qaEntity.DocQATask{SegmentCount: uint64(len(segmentsForGenQA))}
	err = d.taskLogic.UpdateDocQATasks(ctx, []string{qaEntity.DocQaTaskTblColSegmentCount}, filter, update)
	if err != nil {
		return kv, err
	}

	if d.p.QaTaskType != qaEntity.DocQATaskStatusContinue {
		// 还原已完成切片状态
		if err = d.segLogic.UpdateQaSegmentToDocStatus(ctx, tarDoc.ID, tarDoc.BatchID, tarDoc.RobotID); err != nil {
			logx.E(ctx, "task(DocToQA) Prepare, UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
				err, d.p.DocQATaskID)
			return kv, err
		}

		key := fmt.Sprintf("%s%d", qaEntity.DocQaExistsOrgDataPreFix, tarDoc.ID)
		// 重置orgData去重缓存
		// if _, err = d.dao.RedisCli().Do(ctx, "DEL", key); err != nil {
		if _, err = d.adminRdb.Del(ctx, key).Result(); err != nil {
			logx.E(ctx, "task(DocToQA) Prepare, Redis del failed, err:%+v", err)
			return kv, errs.ErrGetQaExistsFail
		}

		for _, segment := range segmentsForGenQA {
			kv[segToQAPrefix+cast.ToString(segment.ID)] = cast.ToString(segment.ID)
		}
		return kv, nil
	}

	// 需要续跑的场景
	logx.I(ctx, "task(DocToQA) retry task: %+v, DocToQAParams: %+v", d.task, d.p)

	for _, segment := range segmentsForGenQA {
		if segment.Status != segEntity.SegmentStatusCreatedQa {
			kv[segToQAPrefix+cast.ToString(segment.ID)] = cast.ToString(segment.ID)
		}
	}

	return kv, nil
}

// SegmentDiff 记录对比切片的结果
type SegmentDiff struct {
	oldUni    []*segEntity.DocSegmentExtend
	oldCommon []*segEntity.DocSegmentExtend
	newUni    []*segEntity.DocSegmentExtend
	newCommon []*segEntity.DocSegmentExtend
}

func (d *DocDiffOperationTaskHandler) diffDocSegment(ctx context.Context, oldDoc, newDoc *docEntity.Doc) (
	*SegmentDiff, error) {
	// 1. 读取文档的t_doc_segment，并生成MD5
	oldSegments, err := getDocNotDeleteSegment(ctx, oldDoc, d.segLogic)
	if err != nil {
		return nil, err
	}
	for _, seg := range oldSegments {
		seg.MD5 = hashString(removeFirstLine(seg.OrgData))
	}

	newSegments, err := getDocNotDeleteSegment(ctx, newDoc, d.segLogic)
	if err != nil {
		return nil, err
	}
	for _, seg := range newSegments {
		seg.MD5 = hashString(removeFirstLine(seg.OrgData))
	}

	// 2. 两两比较，找出旧文档的差异片段
	diff := &SegmentDiff{
		oldUni:    make([]*segEntity.DocSegmentExtend, 0),
		oldCommon: make([]*segEntity.DocSegmentExtend, 0),
		newUni:    make([]*segEntity.DocSegmentExtend, 0),
		newCommon: make([]*segEntity.DocSegmentExtend, 0),
	}
	newSegIDHashMap := make(map[uint64]bool)
	for _, oldSeg := range oldSegments {
		common := false
		var newCommonSeg *segEntity.DocSegmentExtend
		for _, newSeg := range newSegments {
			if oldSeg.MD5 == newSeg.MD5 {
				common = true
				newCommonSeg = newSeg
				break
			}
		}
		if common {
			diff.oldCommon = append(diff.oldCommon, oldSeg)
			diff.newCommon = append(diff.newCommon, newCommonSeg)
			newSegIDHashMap[newCommonSeg.ID] = true
		} else {
			diff.oldUni = append(diff.oldUni, oldSeg)
		}
	}

	for _, newSeg := range newSegments {
		if !newSegIDHashMap[newSeg.ID] {
			diff.newUni = append(diff.newUni, newSeg)
		}
	}

	logx.I(ctx, "old doc: %v %v, unique:%v, common:%v | new doc: %v %v, unique:%v, common:%v",
		oldDoc.ID, oldDoc.FileName, len(diff.oldUni), len(diff.oldCommon),
		newDoc.ID, newDoc.FileName, len(diff.newUni), len(diff.newCommon))
	return diff, nil
}

// Init 初始化
func (d *DocDiffOperationTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocDiffOperation) prepareTokenDosage")
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		return nil
	}
	modelName, err := d.kbLogic.GetShareKnowledgeBaseConfig(ctx, d.p.CorpBizID, d.p.RobotBizID, uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL))
	if err != nil {
		logx.E(ctx, "GetTokenDosage GetShareKnowledgeBaseConfig err: %+v", err)
		return err
	}
	dosage, err := d.financeLogic.GetTokenDosage(ctx, appDB, modelName)
	if err != nil {
		logx.E(ctx, "task(DocDiffOperation) Init err: %v", err)
		return err
	}
	if dosage == nil {
		logx.E(ctx, "task(DocDiffOperation) Init dosage is nil")
		return errs.ErrSystem
	}
	d.tokenDosage = *dosage
	return nil
}

// Process 任务处理
func (d *DocDiffOperationTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.I(ctx, "task(DocDiffOperation) Process, task: %+v, params: %+v", d.task, d.p)
	if len(d.p.EnvSet) > 0 { // 文件重命名创建审核任务，审核回调需要env-set
		contextx.Metadata(ctx).WithEnvSet(d.p.EnvSet)
	}
	// 1. 检查APP和文档状态，并获取对应的信息
	appDB, oldDoc, newDoc, err := d.checkAppAndDocStatus(ctx, progress)
	if err != nil {
		if strings.Contains(err.Error(), docHasBeenDeleted) &&
			docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) != docEntity.SegmentToQATypeNone {
			message := i18n.Translate(ctx, i18nkey.KeyDocumentGenerateQAFailureStateNotSupported)
			update := &docEntity.DocDiff{
				QaOperationResult: message,
			}
			_ = d.taskLogic.UpdateDocDiffTasks(ctx, []string{docEntity.DocDiffTaskTblColQaOperationResult},
				d.p.CorpBizID, d.p.RobotBizID, []uint64{d.p.DocDiffID}, update)
		}
		return err
	}

	// 2. 处理qa操作
	err = d.operateQA(ctx, progress, newDoc, oldDoc, appDB)
	if err != nil {
		return err
	}

	// 3. 处理文档操作
	err = d.operateDoc(ctx, progress, appDB, oldDoc, newDoc)
	if err != nil {
		return err
	}

	if docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) == docEntity.SegmentToQATypeNone {
		// 对于没有片段生成问答的操作，没有拆分KV，只有一个KV，标记为完成
		for key := range progress.TaskKV(ctx) {
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(DocDiffOperation) Finish kv:%s err:%+v", key, err)
				return err
			}
		}
	}

	return nil
}

// 返回：app, old doc, new doc, 报错
func (d *DocDiffOperationTaskHandler) checkAppAndDocStatus(ctx context.Context, progress *task_scheduler.Progress) (
	*entity.App, *docEntity.Doc, *docEntity.Doc, error) {
	// 前置1：判断app是否已经被删除了
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		return nil, nil, nil, err
	}
	if appDB.HasDeleted() {
		logx.I(ctx, "task(DocDiffOperation) appDB.HasDeleted()|appID:%d", d.p.RobotID)
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf("app has bee deleted")
	}
	// 前置2：判断原始任务任务是否已被删除或者是否已失效
	docDiff, err := d.taskLogic.GetDocDiffTask(ctx, []string{docEntity.DocDiffTaskTblColStatus}, d.p.CorpBizID,
		appDB.BizId, d.p.DocDiffID)
	if errors.Is(err, errs.ErrHandleDocDiffNotFound) {
		logx.E(ctx, "task %v is not exist, stop task", d.p.DocDiffID)
		progress.Stop(ctx)
		return nil, nil, nil, err
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if docDiff.Status == uint32(docEntity.DocDiffStatusFinish) || docDiff.Status == uint32(docEntity.DocDiffStatusInvalid) {
		logx.I(ctx, "doc diff task status: %v, stop", docDiff.Status)
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf(docHasBeenDeleted)
	}

	// 前置3：获取新旧文档数据，检测文档是否已被删除
	oldDoc, err := d.docLogic.GetDocByBizID(ctx, d.p.OldDocBizID, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "get old doc failed, %v", err)
		return nil, nil, nil, err
	}
	if oldDoc.HasDeleted() {
		logx.E(ctx, "task(DocDiffOperation) old doc %v has been deleted", d.p.OldDocBizID)
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf(docHasBeenDeleted)
	}
	newDoc, err := d.docLogic.GetDocByBizID(ctx, d.p.NewDocBizID, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "GetDocByBizID failed, %v", err)
		return nil, nil, nil, err
	}
	if newDoc.HasDeleted() {
		logx.E(ctx, "task(DocDiffOperation) new doc %v has been deleted", d.p.NewDocBizID)
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf(docHasBeenDeleted)
	}
	return appDB, oldDoc, newDoc, nil
}

func (d *DocDiffOperationTaskHandler) operateDoc(ctx context.Context, progress *task_scheduler.Progress,
	appDB *entity.App, oldDoc *docEntity.Doc, newDoc *docEntity.Doc) error {
	if docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) != docEntity.SegmentToQATypeNone && progress.Remain() != 0 {
		logx.I(ctx, "qa operation finished yet, skip doc operation")
		return nil
	}
	var renamedDoc *docEntity.Doc
	var err error
	defer func() {
		docOperationStatus := docEntity.DocDiffQAAndDocOpStatusSuccess
		status := docEntity.DocDiffStatusFinish
		if err != nil {
			docOperationStatus = docEntity.DocDiffQAAndDocOpStatusFailed
		}

		update := &docEntity.DocDiff{
			DocOperationStatus: uint32(docOperationStatus),
			Status:             uint32(status),
		}

		updateColumns := []string{docEntity.DocDiffTaskTblColDocOperationStatus, docEntity.DocDiffTaskTblColStatus}
		logx.I(ctx, "finish task %v, doc op status: %v, task status: %v", d.p.DocDiffID, docOperationStatus, status)
		_ = d.taskLogic.UpdateDocDiffTasks(ctx, updateColumns, d.p.CorpBizID, appDB.BizId,
			[]uint64{d.p.DocDiffID}, update)
	}()

	switch d.p.DocOperation {
	case docEntity.DocOperationDeleteOldDoc:
		err = d.docLogic.DeleteDocs(ctx, d.p.StaffID, appDB.PrimaryId, appDB.BizId, []*docEntity.Doc{oldDoc})
		if err != nil {
			logx.E(ctx, "delete old doc %v failed, %v", oldDoc.BusinessID, err)
			return err
		}
		logx.I(ctx, "old doc %v %v delete success", oldDoc.BusinessID, oldDoc.FileName)
	case docEntity.DocOperationDeleteNewDoc:
		err = d.docLogic.DeleteDocs(ctx, d.p.StaffID, appDB.PrimaryId, appDB.BizId, []*docEntity.Doc{newDoc})
		if err != nil {
			logx.E(ctx, "delete new doc %v failed, %v", newDoc.BusinessID, err)
			return err
		}
		logx.I(ctx, "new doc %v %v delete success", newDoc.BusinessID, newDoc.FileName)
	case docEntity.DocOperationOldReName:
		renamedDoc = oldDoc
	case docEntity.DocOperationNewReName:
		renamedDoc = newDoc
	case docEntity.DocOperationDefault:
		// 不处理
	default:
		return fmt.Errorf("doc operation type error, %v", d.p.QAOperation)
	}

	// 2.1 阻塞等待重命名文档异步任务的处理结果
	// 重命名文档可能出现的终态  DocStatusWaitRelease   DocStatusReleaseSuccess  DocStatusDocNameAuditFail
	if (d.p.DocOperation == docEntity.DocOperationOldReName || d.p.DocOperation == docEntity.DocOperationNewReName) &&
		renamedDoc != nil {
		renamedDoc.FileNameInAudit = d.p.NewName
		if err = d.docLogic.RenameDoc(ctx, d.p.StaffID, appDB, renamedDoc); err != nil {
			logx.E(ctx, "文档重命名失败 RenameDoc err: %+v", err)
			return err
		}
		logx.I(ctx, "rename doc %v %v to %v", renamedDoc.ID, renamedDoc.FileName, d.p.NewName)
		start := time.Now()
		for {
			renamedDoc, err = d.docLogic.GetDocByBizID(ctx, renamedDoc.BusinessID, d.p.RobotID)
			if err != nil {
				logx.E(ctx, "GetDocByBizID failed, %v", err)
				return err
			}
			if renamedDoc.HasDeleted() {
				logx.E(ctx, "task(DocDiffOperation) renamed doc %v has been deleted", renamedDoc.ID)
				progress.Stop(ctx)
				return fmt.Errorf("doc has beed deleted")
			}
			if time.Now().Sub(start) > waitDocRenameTimeout*time.Second {
				logx.E(ctx, "wait doc rename timeout")
				return fmt.Errorf("wait doc rename timeout")
			}
			if !renamedDoc.IsStableStatus() {
				logx.I(ctx, "renamed doc status is not stable, id: %v, status: %v", renamedDoc.ID, renamedDoc.Status)
				time.Sleep(waitDocRenameSleepTime * time.Second)
				continue
			}
			break
		}
		// 文档重命名处理完成，成功的场景下直接在Done中更新提示信息，失败立刻更新提示信息
		if renamedDoc.Status == docEntity.DocStatusWaitRelease || renamedDoc.Status == docEntity.DocStatusReleaseSuccess {
			logx.I(ctx, "rename operation success, doc %v, status: %v", renamedDoc.ID, renamedDoc.Status)
			return nil
		}
		if renamedDoc.Status == docEntity.DocStatusDocNameAuditFail {
			logx.E(ctx, "rename operation failed, doc %v, status: %v", renamedDoc.ID, renamedDoc.Status)
			return err
		} else {
			// 原理上重命名不应该出现其他状态，报错
			logx.E(ctx, "doc %v rename failed, unexpect status %v", renamedDoc.ID, renamedDoc.Status)
			return err
		}
	}
	return nil
}
func (d *DocDiffOperationTaskHandler) operateQA(ctx context.Context, progress *task_scheduler.Progress,
	newDoc *docEntity.Doc, oldDoc *docEntity.Doc, appDB *entity.App) error {
	op := d.p.QAOperation
	d.tokenDosage.StartTime = time.Now()
	taskKV := progress.TaskKV(ctx)

	// 初始化状态跟踪
	qaState := &qaOperationState{
		ctx:                 ctx,
		d:                   d,
		progress:            progress,
		newDoc:              newDoc,
		oldDoc:              oldDoc,
		appDB:               appDB,
		taskKV:              taskKV,
		message:             "",
		qaTask:              nil,
		qaOperationFinished: false,
	}

	defer qaState.updateStatus()
	// 1. 处理文档生成问答
	if err := qaState.handleSegmentToQA(); err != nil {
		return err
	}

	// 2. 处理其他问答操作
	if err := qaState.handleQAOperation(op); err != nil {
		return err
	}

	qaState.qaOperationFinished = true
	return nil
}

// rebindQAToDoc 将qa关联到文档，如果qa为0，返回0, nil
func (d *DocDiffOperationTaskHandler) rebindQAToDoc(ctx context.Context, qaSourceDoc, destDoc *docEntity.Doc) (
	int, error) {
	qas, err := getDocNotDeleteQA(ctx, qaSourceDoc, d.qaLogic)
	if err != nil {
		return 0, err
	}
	qaCount := len(qas)
	if qaCount == 0 {
		return 0, nil
	}
	var qaIDs []uint64
	var qaBizIDs []uint64
	for _, qa := range qas {
		qaIDs = append(qaIDs, qa.ID)
		qaBizIDs = append(qaBizIDs, qa.BusinessID)
	}

	filter := &qaEntity.DocQaFilter{
		BusinessIds: qaBizIDs,
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
	}
	qaUpdate := &qaEntity.DocQA{
		DocID: destDoc.ID,
	}
	err = d.qaLogic.UpdateDocQas(ctx, []string{qaEntity.DocQaTblColDocId}, filter, qaUpdate)
	if err != nil {
		return qaCount, err
	}

	err = d.qaLogic.UpdateQASimilarsDocID(ctx, qaIDs, destDoc.ID)
	if err != nil {
		return qaCount, err
	}
	logx.I(ctx, "rebind %v qa from %v %v to %v %v success", qaCount, qaSourceDoc.ID,
		qaSourceDoc.FileName, destDoc.ID, destDoc.FileName)
	return qaCount, nil
}

// Fail 任务失败
func (d *DocDiffOperationTaskHandler) Fail(ctx context.Context) error {
	logx.E(ctx, "task(DocDiffOperation) Fail")
	// 1. 更新文档本身的状态标志位，发送通知
	d.cleanFlagAndSendNotice(ctx)

	// 任务出现异常，收尾，结束doc diff task 和 doc qa task的的状态
	selectColumns := []string{docEntity.DocDiffTaskTblColDocOperationStatus, docEntity.DocDiffTaskTblColQaOperationStatus,
		docEntity.DocDiffTaskTblColStatus, docEntity.DocDiffTaskTblColIsDeleted}
	docDiff, err := d.taskLogic.GetDocDiffTask(ctx, selectColumns, d.p.CorpBizID, d.p.RobotBizID, d.p.DocDiffID)
	if err == nil && !docDiff.IsDeleted {
		updateColumns := []string{docEntity.DocDiffTaskTblColStatus}
		update := &docEntity.DocDiff{
			Status: uint32(docEntity.DocDiffStatusFinish),
		}
		if docDiff.QaOperationStatus == uint32(docEntity.DocDiffQAAndDocOpStatusFailed) {
			update.DocOperationStatus = uint32(docEntity.DocDiffQAAndDocOpStatusFailed)
			updateColumns = append(updateColumns, docEntity.DocDiffTaskTblColDocOperationStatus)
		}
		logx.I(ctx, "update task %v failed, doc op status: %v, task status: %v", d.p.DocDiffID,
			update.DocOperationStatus, update.Status)
		_ = d.taskLogic.UpdateDocDiffTasks(ctx, updateColumns, d.p.CorpBizID, d.p.RobotBizID,
			[]uint64{d.p.DocDiffID}, update)
	}

	return nil
}

func (d *DocDiffOperationTaskHandler) docToQAFailed(ctx context.Context, oldDoc, newDoc *docEntity.Doc) error {
	defer func() {
		// 更新生成问答任务状态到终态
		if err := d.taskLogic.UpdateDocQATaskStatus(ctx, qaEntity.DocQATaskStatusFail, d.p.DocQATaskID); err != nil {
			logx.E(ctx, "Fail UpdateDocQATaskStatus failed, err:%+v|QaTaskID|%d",
				err, d.p.DocQATaskID)
		}
	}()

	targetDoc := newDoc
	if docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) == docEntity.SegmentToQATypeOldUni ||
		docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) == docEntity.SegmentToQATypeOldUni {
		targetDoc = oldDoc
	}
	targetDoc.Message = i18nkey.KeyGenerateQAFailed
	targetDoc.IsCreatingQA = false
	targetDoc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
	targetDoc.UpdateTime = time.Now()
	if err := d.docLogic.CreateDocQADone(ctx, d.p.StaffID, targetDoc, -1, false); err != nil {
		logx.E(ctx, "task(DocToQA) Fail CreateDocQADone failed, err:%+v|doc|%v",
			err, targetDoc)
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocDiffOperationTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "task(DocDiffOperation) stop, doc diff id: %v", d.p.DocDiffID)
	_ = d.Fail(ctx)
	return nil
}

// Done 任务完成回调
func (d *DocDiffOperationTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(DocDiffOperation) done, doc diff id: %v", d.p.DocDiffID)
	// 1. 更新文档本身的状态标志位
	d.cleanFlagAndSendNotice(ctx)

	return nil
}

func (d *DocDiffOperationTaskHandler) cleanFlagAndSendNotice(ctx context.Context) {
	oldFileName := ""
	newFileName := ""
	oldDoc, err := d.docLogic.GetDocByBizID(ctx, d.p.OldDocBizID, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "get old doc failed, %v", err)
		return
	}
	if oldDoc != nil {
		_ = d.clearDocProcessingFlag(ctx, oldDoc)
		oldFileName = oldDoc.GetRealFileName()
	}

	newDoc, err := d.docLogic.GetDocByBizID(ctx, d.p.NewDocBizID, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "GetDocByBizID failed, %v", err)
		return
	}
	if newDoc != nil {
		_ = d.clearDocProcessingFlag(ctx, newDoc)
		newFileName = newDoc.GetRealFileName()
	}

	_ = d.taskLogic.CreateDocDiffTaskRunningNotice(ctx, d.p.StaffID, d.p.CorpID, d.p.RobotID, d.p.DocDiffID,
		newFileName, oldFileName, false)

	return
}

// docToQADone 文档生成问答成功的后处理
func (d *DocDiffOperationTaskHandler) docToQADone(ctx context.Context, oldDoc, newDoc *docEntity.Doc) error {
	targetDoc := newDoc
	if docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) == docEntity.SegmentToQATypeOldUni ||
		docEntity.GetDocDiffSegmentToQAType(d.p.QAOperation) == docEntity.SegmentToQATypeOldUni {
		targetDoc = oldDoc
	}

	qaListReq := &qaEntity.QAListReq{
		CorpID:  d.p.CorpID,
		DocID:   []uint64{targetDoc.ID},
		RobotID: d.p.RobotID,
	}
	count, err := d.qaLogic.GetQaCountWithDocID(ctx, qaListReq)
	if err != nil {
		logx.E(ctx, "task(DocToQA) Done GetQaCountWithDocID failed, err:%+v|qaListReq|%v",
			err, qaListReq)
		return err
	}
	targetDoc.Message = i18nkey.KeyGenerateQASuccess
	targetDoc.IsCreatingQA = false
	targetDoc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
	targetDoc.UpdateTime = time.Now()

	success := true
	if count == 0 {
		success = false
	}
	// 更新doc表IsCreatingQA标志位，发送notice
	if err = d.docLogic.CreateDocQADone(ctx, d.p.StaffID, targetDoc, int(count), success); err != nil {
		logx.E(ctx, "task(DocToQA) Done CreateDocQADone failed, err:%+v|DocID %d",
			err, targetDoc.ID)
		return err
	}
	// 更新生成问答任务状态
	if err = d.taskLogic.UpdateDocQATaskStatus(ctx, qaEntity.DocQATaskStatusSuccess, d.p.DocQATaskID); err != nil {
		logx.E(ctx, "task(DocToQA) Done UpdateDocQATaskStatus failed, err:%+v|QaTaskID|%d",
			err, d.p.DocQATaskID)
		return err
	}

	// 还原已完成切片状态
	if err = d.segLogic.UpdateQaSegmentToDocStatus(ctx, targetDoc.ID, targetDoc.BatchID, targetDoc.RobotID); err != nil {
		logx.E(ctx, "task(DocToQA) Done UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
			err, d.p.DocQATaskID)
		return err
	}

	key := fmt.Sprintf("%s%d", qaEntity.DocQaExistsOrgDataPreFix, targetDoc.ID)
	// 重置orgData去重缓存
	// if _, err = d.dao.RedisCli().Do(ctx, "DEL", key); err != nil {
	if _, err = d.adminRdb.Del(ctx, key).Result(); err != nil {
		logx.E(ctx, "task(DocToQA) Done, Redis del failed, err:%+v", err)
		return err
	}
	return nil
}

func (d *DocDiffOperationTaskHandler) clearDocProcessingFlag(ctx context.Context, doc *docEntity.Doc) error {
	updateDocFilter := &docEntity.DocFilter{
		IDs:     []uint64{doc.ID},
		CorpId:  doc.CorpID,
		RobotId: doc.RobotID,
	}
	update := &docEntity.Doc{
		ProcessingFlag: doc.ProcessingFlag,
	}
	update.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagHandlingDocDiffTask})
	updateDocColumns := []string{
		docEntity.DocTblColProcessingFlag}
	_, err := d.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		logx.E(ctx, "clear doc %v %v diff task flag err: %+v", doc.ID, doc.FileName, err)
		return err
	}
	return nil
}

// removeFirstLine 去除字符串第一行，如果字符串本身不存在换行符，返回原字符串
func removeFirstLine(s string) string {
	index := strings.Index(s, "\n")
	if index == -1 {
		return s
	}
	return s[index+1:]
}

// 返回： 是否需要在循环中跳过当前的key  报错的message  错误
func (d *DocDiffOperationTaskHandler) segmentToQA(ctx context.Context, segmentID uint64, appDB *entity.App,
	doc *docEntity.Doc, uniqueQaMap map[string]bool, uniqueQas []*qaEntity.QA) error {
	segment, err := d.segLogic.GetSegmentByID(ctx, segmentID, appDB.PrimaryId)
	if err != nil {
		return err
	}
	if segment == nil {
		return errs.ErrSegmentNotFound
	}
	if segment.SegmentType == segEntity.SegmentTypeTable {
		// 表格类型不生成问答
		return nil
	}
	if segment.OrgData == "" { // 如果旧表格没有orgData，则从t_doc_segment_org_data新表中获取orgData
		corpRsp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, appDB.CorpPrimaryId)
		if err != nil || corpRsp == nil {
			return errs.ErrCorpNotFound
		}
		orgData, err := d.segLogic.GetDocOrgDataByBizID(ctx,
			[]string{segEntity.DocSegmentOrgDataTblColOrgData}, corpRsp.GetCorpId(), appDB.BizId,
			doc.BusinessID, segment.OrgDataBizID)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if orgData != nil && orgData.OrgData != "" {
			segment.OrgData = orgData.OrgData
		}
		logx.D(ctx, "task(DocToQA) Process GetDocOrgDataByBizID|segment.OrgData:%s",
			segment.OrgData)
	}
	// 判断是否有重复的orgData,重复的orgData不进行问答生成
	exists, err := checkSegmentOrgDataExists(ctx, segment, d.dao, d.adminRdb, segment.DocID)
	if err != nil {
		return err
	}
	if exists {
		logx.D(ctx, "task(DocDiffOperation) checkSegmentOrgDataExists 重复ordData:%v segment:%+v",
			exists, segment)
		// 更新生成问答任务已完成切片数量和问答数量
		if err = d.taskLogic.UpdateDocQATaskSegmentDoneAndQaCount(ctx, 0, 1,
			d.p.CorpID, d.p.RobotID, d.p.DocQATaskID); err != nil {
			logx.E(ctx, "task(DocDiffOperation) Done UpdateDocQATaskSegmentCountDone failed,"+
				" err:%+v|QaTaskID|%d", err, d.p.DocQATaskID)
			return err
		}
		segment.Status = segEntity.SegmentStatusCreatedQa
		// 更新切片状态
		if err = d.segLogic.UpdateQaSegmentStatus(ctx, segment, d.p.RobotID); err != nil {
			logx.E(ctx, "task(DocDiffOperation) Done UpdateDocQATaskSegmentCountDone failed,"+
				" err:%+v|QaTaskID|%d", err, d.p.DocDiffID)
			return err
		}
		return nil
	}
	segment.ExpireStart = doc.ExpireStart
	segment.ExpireEnd = doc.ExpireEnd
	tree, qas, tokenStatisticInfo, err := getQAAndCateNode(ctx, doc, segment, appDB, d.cateLogic, d.qaLogic, d.tokenDosage.ModelName, d.llmLogic)
	if err != nil {
		logx.E(ctx, "task(DocDiffOperation) getQAAndCateNode err|%v|doc:%+v|segment:%+v",
			err, doc, segment.ID)
		return err
	}
	qas = slicex.Filter(qas, func(qa *qaEntity.QA) bool {
		return checkQuestionAndAnswer(ctx, qa.Question, qa.Answer, qa.SimilarQuestions) == nil
	})
	for _, qa := range qas {
		uniqueKey := qa.Question + qa.Answer
		if !uniqueQaMap[uniqueKey] {
			uniqueQaMap[uniqueKey] = true
			uniqueQas = append(uniqueQas, qa)
		}
	}
	// 文档生成问答时不需要审核，在后续采纳问答时走审核
	if err = d.qaLogic.BatchCreateQA(ctx, segment, doc, uniqueQas, tree, false); err != nil {
		logx.E(ctx, "task(DocDiffOperation) Process BatchCreateQA failed,"+
			" err:%+v|DocDiffID|%d|docID|%d", err, d.p.DocDiffID, doc.ID)
		return err
	}
	// 通过文档生成的问答默认未采纳，这里在采纳问答的时候会更新机器人字符使用量，调用大模型，消耗token，上报token
	// 调用大模型，消耗token，上报token
	err = reportTokenDosage(ctx, tokenStatisticInfo, d.taskLogic, d.financeLogic, &d.tokenDosage, appDB, d.p.CorpID, d.p.DocQATaskID)
	if err != nil {
		logx.E(ctx, "task(DocToQA) Process reportTokenDosage failed,"+
			" err:%+v|tokenStatisticInfo|%v", err, tokenStatisticInfo)
		return err
	}

	segment.Status = segEntity.SegmentStatusCreatedQa
	// 更新切片状态
	if err = d.segLogic.UpdateQaSegmentStatus(ctx, segment, d.p.RobotID); err != nil {
		logx.E(ctx, "task(DocDiffOperation) Done UpdateDocQATaskSegmentCountDone failed,"+
			" err:%+v|QaTaskID|%d", err, d.p.DocDiffID)
		return err
	}

	// 更新生成问答任务已完成切片数量和问答数量
	if err = d.taskLogic.UpdateDocQATaskSegmentDoneAndQaCount(ctx, uint64(len(uniqueQas)), 1,
		d.p.CorpID, d.p.RobotID, d.p.DocQATaskID); err != nil {
		logx.E(ctx, "task(DocToQA) Done UpdateDocQATaskSegmentCountDone failed,"+
			" err:%+v|QaTaskID|%d", err, d.p.DocQATaskID)
		return err
	}
	return nil
}
