package task

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	logicCorp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/corp"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strings"
	"time"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_diff_task"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/billing"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	docOperationPrefix = "doc:operation:"
	segToQAPrefix      = "seg:to:qa:"

	waitDocRenameSleepTime = 1
	waitDocRenameTimeout   = 120
	docHasBeenDeleted      = "docHasBeenDeleted"
)

// DocDiffOperationScheduler 文档处理任务
type DocDiffOperationScheduler struct {
	dao         dao.Dao
	task        task_scheduler.Task
	p           model.DocDiffOperationParams
	tokenDosage billing.TokenDosage
}

func initDocDiffOperationTaskScheduler() {
	task_scheduler.Register(
		model.DocDiffOperationTask,
		func(t task_scheduler.Task, params model.DocDiffOperationParams) task_scheduler.TaskHandler {
			return &DocDiffOperationScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocDiffOperationScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.InfoContextf(ctx, "task(DocDiffOperation) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)

	if model.GetDocDiffSegmentToQAType(d.p.QAOperation) == model.SegmentToQATypeNone {
		// 没有片段生成qa的操作，一次操作即可
		kv[docOperationPrefix+cast.ToString(d.p.DocDiffID)] = cast.ToString(d.p.DocDiffID)
		return kv, nil
	}

	// 有生成qa的场景
	oldDoc, err := d.dao.GetDocByBizID(ctx, d.p.OldDocBizID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocByBizID failed, %v", err)
		return nil, err
	}
	if oldDoc.HasDeleted() {
		log.ErrorContextf(ctx, "task(DocDiffOperation) old doc %v has been deleted", d.p.OldDocBizID)
		return nil, errs.ErrDocNotFound
	}
	newDoc, err := d.dao.GetDocByBizID(ctx, d.p.NewDocBizID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocByBizID failed, %v", err)
		return nil, err
	}
	if newDoc.HasDeleted() {
		log.ErrorContextf(ctx, "task(DocDiffOperation) new doc %v has been deleted", d.p.NewDocBizID)
		return nil, errs.ErrDocNotFound
	}

	segmentDiff, err := d.diffDocSegment(ctx, oldDoc, newDoc)
	if err != nil {
		return kv, err
	}

	// 准备用于生成qa的segment
	var segmentsForGenQA []*model.DocSegmentExtend
	tarDoc := newDoc
	segmentToQAType := model.GetDocDiffSegmentToQAType(d.p.QAOperation)
	if segmentToQAType == model.SegmentToQATypeOldUni {
		segmentsForGenQA = segmentDiff.oldUni
		tarDoc = oldDoc
	} else if segmentToQAType == model.SegmentToQATypeOldAll {
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.oldUni...)
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.oldCommon...)
		tarDoc = oldDoc
	} else if segmentToQAType == model.SegmentToQATypeNewUni {
		segmentsForGenQA = segmentDiff.newUni
	} else if segmentToQAType == model.SegmentToQATypeNewAll {
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.newUni...)
		segmentsForGenQA = append(segmentsForGenQA, segmentDiff.newCommon...)
	} else {
		return nil, fmt.Errorf("unexpect segmentToQAType %v", segmentToQAType)
	}

	docQATask, err := d.dao.GetDocQATaskByID(ctx, d.p.DocQATaskID, d.p.CorpID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) retry 获取生成问答任务详情失败 err:%+v", err)
		return kv, err
	}
	if docQATask == nil || docQATask.ID <= 0 {
		log.InfoContextf(ctx, "task(DocToQA) retry 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
			d.p.CorpID, d.p.RobotID, d.p.DocQATaskID)
		return kv, errs.ErrDocQaTaskNotFound
	}

	filter := &dao.DocQaTaskFilter{
		BusinessId: docQATask.BusinessID,
		CorpId:     d.p.CorpID,
		RobotId:    d.p.RobotID,
	}
	update := &model.DocQATask{SegmentCount: uint64(len(segmentsForGenQA))}
	err = dao.GetDocQaTaskDao().UpdateDocQATasks(ctx, []string{dao.DocQaTaskTblColSegmentCount}, filter, update)
	if err != nil {
		return kv, err
	}

	if d.p.QaTaskType != model.DocQATaskStatusContinue {
		// 还原已完成切片状态
		if err = d.dao.UpdateQaSegmentToDocStatus(ctx, tarDoc.ID, tarDoc.BatchID, tarDoc.RobotID); err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) Prepare, UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
				err, d.p.DocQATaskID)
			return kv, err
		}

		key := fmt.Sprintf("%s%d", model.DocQaExistsOrgDataPreFix, tarDoc.ID)
		// 重置orgData去重缓存
		if _, err = d.dao.RedisCli().Do(ctx, "DEL", key); err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) Prepare, Redis del failed, err:%+v", err)
			return kv, errs.ErrGetQaExistsFail
		}

		for _, segment := range segmentsForGenQA {
			kv[segToQAPrefix+cast.ToString(segment.ID)] = cast.ToString(segment.ID)
		}
		return kv, nil
	}

	// 需要续跑的场景
	log.InfoContextf(ctx, "task(DocToQA) retry task: %+v, DocToQAParams: %+v", d.task, d.p)

	for _, segment := range segmentsForGenQA {
		if segment.Status != model.SegmentStatusCreatedQa {
			kv[segToQAPrefix+cast.ToString(segment.ID)] = cast.ToString(segment.ID)
		}
	}

	return kv, nil
}

// SegmentDiff 记录对比切片的结果
type SegmentDiff struct {
	oldUni    []*model.DocSegmentExtend
	oldCommon []*model.DocSegmentExtend
	newUni    []*model.DocSegmentExtend
	newCommon []*model.DocSegmentExtend
}

func (d *DocDiffOperationScheduler) diffDocSegment(ctx context.Context, oldDoc, newDoc *model.Doc) (*SegmentDiff, error) {
	// 1. 读取文档的t_doc_segment，并生成MD5
	oldSegments, err := getDocNotDeleteSegment(ctx, oldDoc, d.dao)
	if err != nil {
		return nil, err
	}
	for _, seg := range oldSegments {
		seg.MD5 = hashString(removeFirstLine(seg.OrgData))
	}

	newSegments, err := getDocNotDeleteSegment(ctx, newDoc, d.dao)
	if err != nil {
		return nil, err
	}
	for _, seg := range newSegments {
		seg.MD5 = hashString(removeFirstLine(seg.OrgData))
	}

	// 2. 两两比较，找出旧文档的差异片段
	diff := &SegmentDiff{
		oldUni:    make([]*model.DocSegmentExtend, 0),
		oldCommon: make([]*model.DocSegmentExtend, 0),
		newUni:    make([]*model.DocSegmentExtend, 0),
		newCommon: make([]*model.DocSegmentExtend, 0),
	}
	newSegIDHashMap := make(map[uint64]bool)
	for _, oldSeg := range oldSegments {
		common := false
		var newCommonSeg *model.DocSegmentExtend
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

	log.InfoContextf(ctx, "old doc: %v %v, unique:%v, common:%v | new doc: %v %v, unique:%v, common:%v",
		oldDoc.ID, oldDoc.FileName, len(diff.oldUni), len(diff.oldCommon),
		newDoc.ID, newDoc.FileName, len(diff.newUni), len(diff.newCommon))
	return diff, nil
}

// Init 初始化
func (d *DocDiffOperationScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocDiffOperation) prepareTokenDosage")
	dosage, err := logicCorp.GetTokenDosage(ctx, d.p.RobotBizID, "",
		uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL))
	if err != nil {
		log.ErrorContextf(ctx, "task(DocDiffOperation) Init err: %v", err)
		return err
	}
	if dosage == nil {
		log.ErrorContextf(ctx, "task(DocDiffOperation) Init dosage is nil")
		return errs.ErrSystem
	}
	d.tokenDosage = *dosage
	return nil
}

// Process 任务处理
func (d *DocDiffOperationScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.InfoContextf(ctx, "task(DocDiffOperation) Process, task: %+v, params: %+v", d.task, d.p)
	if len(d.p.EnvSet) > 0 { // 文件重命名创建审核任务，审核回调需要env-set
		ctx = pkg.WithEnvSet(ctx, d.p.EnvSet)
	}
	// 1. 检查APP和文档状态，并获取对应的信息
	app, oldDoc, newDoc, err := d.checkAppAndDocStatus(ctx, progress)
	if err != nil {
		if strings.Contains(err.Error(), docHasBeenDeleted) &&
			model.GetDocDiffSegmentToQAType(d.p.QAOperation) != model.SegmentToQATypeNone {
			message := i18n.Translate(ctx, i18nkey.KeyDocumentGenerateQAFailureStateNotSupported)
			update := &model.DocDiff{
				QaOperationResult: message,
			}
			_ = dao.GetDocDiffTaskDao().UpdateDocDiffTasks(ctx, nil, []string{dao.DocDiffTaskTblColQaOperationResult},
				d.p.CorpBizID, d.p.RobotBizID, []uint64{d.p.DocDiffID}, update)
		}
		return err
	}

	// 2. 处理qa操作
	err = d.operateQA(ctx, progress, newDoc, oldDoc, app)
	if err != nil {
		return err
	}

	// 3. 处理文档操作
	err = d.operateDoc(ctx, progress, app, oldDoc, newDoc)
	if err != nil {
		return err
	}

	if model.GetDocDiffSegmentToQAType(d.p.QAOperation) == model.SegmentToQATypeNone {
		// 对于没有片段生成问答的操作，没有拆分KV，只有一个KV，标记为完成
		for key := range progress.TaskKV(ctx) {
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocDiffOperation) Finish kv:%s err:%+v", key, err)
				return err
			}
		}
	}

	return nil
}

// 返回：app, old doc, new doc, 报错
func (d *DocDiffOperationScheduler) checkAppAndDocStatus(ctx context.Context, progress *task_scheduler.Progress) (
	*admin.GetAppInfoRsp, *model.Doc, *model.Doc, error) {
	// 前置1：判断app是否已经被删除了
	app, err := client.GetAppInfo(ctx, d.p.RobotBizID, model.AppTestScenes)
	if err != nil {
		return nil, nil, nil, err
	}
	if app.GetIsDelete() {
		log.InfoContextf(ctx, "task(DocDiffOperation) app.GetIsDelete()|appID:%d", app.GetId())
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf("app has bee deleted")
	}
	// 前置2：判断原始任务任务是否已被删除或者是否已失效
	docDiff, err := dao.GetDocDiffTaskDao().GetDocDiffTask(ctx, []string{dao.DocDiffTaskTblColStatus}, d.p.CorpBizID,
		app.GetAppBizId(), d.p.DocDiffID)
	if errors.Is(err, errs.ErrHandleDocDiffNotFound) {
		log.ErrorContextf(ctx, "task %v is not exist, stop task", d.p.DocDiffID)
		progress.Stop(ctx)
		return nil, nil, nil, err
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if docDiff.Status == uint32(model.DocDiffStatusFinish) || docDiff.Status == uint32(model.DocDiffStatusInvalid) {
		log.InfoContextf(ctx, "doc diff task status: %v, stop", docDiff.Status)
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf(docHasBeenDeleted)
	}

	// 前置3：获取新旧文档数据，检测文档是否已被删除
	oldDoc, err := d.dao.GetDocByBizID(ctx, d.p.OldDocBizID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "get old doc failed, %v", err)
		return nil, nil, nil, err
	}
	if oldDoc.HasDeleted() {
		log.ErrorContextf(ctx, "task(DocDiffOperation) old doc %v has been deleted", d.p.OldDocBizID)
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf(docHasBeenDeleted)
	}
	newDoc, err := d.dao.GetDocByBizID(ctx, d.p.NewDocBizID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocByBizID failed, %v", err)
		return nil, nil, nil, err
	}
	if newDoc.HasDeleted() {
		log.ErrorContextf(ctx, "task(DocDiffOperation) new doc %v has been deleted", d.p.NewDocBizID)
		progress.Stop(ctx)
		return nil, nil, nil, fmt.Errorf(docHasBeenDeleted)
	}
	return app, oldDoc, newDoc, nil
}

func (d *DocDiffOperationScheduler) operateDoc(ctx context.Context, progress *task_scheduler.Progress,
	app *admin.GetAppInfoRsp, oldDoc *model.Doc, newDoc *model.Doc) error {
	if model.GetDocDiffSegmentToQAType(d.p.QAOperation) != model.SegmentToQATypeNone && progress.Remain() != 0 {
		log.InfoContextf(ctx, "qa operation finished yet, skip doc operation")
		return nil
	}
	var renamedDoc *model.Doc
	var err error
	defer func() {
		docOperationStatus := model.DocDiffQAAndDocOpStatusSuccess
		status := model.DocDiffStatusFinish
		if err != nil {
			docOperationStatus = model.DocDiffQAAndDocOpStatusFailed
		}

		update := &model.DocDiff{
			DocOperationStatus: uint32(docOperationStatus),
			Status:             uint32(status),
		}

		updateColumns := []string{dao.DocDiffTaskTblColDocOperationStatus, dao.DocDiffTaskTblColStatus}
		log.InfoContextf(ctx, "finish task %v, doc op status: %v, task status: %v", d.p.DocDiffID, docOperationStatus, status)
		_ = dao.GetDocDiffTaskDao().UpdateDocDiffTasks(ctx, nil, updateColumns, d.p.CorpBizID, app.GetAppBizId(),
			[]uint64{d.p.DocDiffID}, update)
	}()

	switch d.p.DocOperation {
	case model.DocOperationDeleteOldDoc:
		err = d.dao.DeleteDocs(ctx, d.p.StaffID, app.GetAppBizId(), []*model.Doc{oldDoc})
		if err != nil {
			log.ErrorContextf(ctx, "delete old doc %v failed, %v", oldDoc.BusinessID, err)
			return err
		}
		log.InfoContextf(ctx, "old doc %v %v delete success", oldDoc.BusinessID, oldDoc.FileName)
	case model.DocOperationDeleteNewDoc:
		err = d.dao.DeleteDocs(ctx, d.p.StaffID, app.GetAppBizId(), []*model.Doc{newDoc})
		if err != nil {
			log.ErrorContextf(ctx, "delete new doc %v failed, %v", newDoc.BusinessID, err)
			return err
		}
		log.InfoContextf(ctx, "new doc %v %v delete success", newDoc.BusinessID, newDoc.FileName)
	case model.DocOperationOldReName:
		renamedDoc = oldDoc
	case model.DocOperationNewReName:
		renamedDoc = newDoc
	case model.DocOperationDefault:
		// 不处理
	default:
		return fmt.Errorf("doc operation type error, %v", d.p.QAOperation)
	}

	// 2.1 阻塞等待重命名文档异步任务的处理结果
	// 重命名文档可能出现的终态  DocStatusWaitRelease   DocStatusReleaseSuccess  DocStatusDocNameAuditFail
	if (d.p.DocOperation == model.DocOperationOldReName || d.p.DocOperation == model.DocOperationNewReName) &&
		renamedDoc != nil {
		renamedDoc.FileNameInAudit = d.p.NewName
		if err = d.dao.RenameDoc(ctx, d.p.StaffID, app, renamedDoc); err != nil {
			log.ErrorContextf(ctx, "文档重命名失败 RenameDoc err: %+v", err)
			return err
		}
		log.InfoContextf(ctx, "rename doc %v %v to %v", renamedDoc.ID, renamedDoc.FileName, d.p.NewName)
		start := time.Now()
		for {
			renamedDoc, err = d.dao.GetDocByBizID(ctx, renamedDoc.BusinessID, d.p.RobotID)
			if err != nil {
				log.ErrorContextf(ctx, "GetDocByBizID failed, %v", err)
				return err
			}
			if renamedDoc.HasDeleted() {
				log.ErrorContextf(ctx, "task(DocDiffOperation) renamed doc %v has been deleted", renamedDoc.ID)
				progress.Stop(ctx)
				return fmt.Errorf("doc has beed deleted")
			}
			if time.Now().Sub(start) > waitDocRenameTimeout*time.Second {
				log.ErrorContextf(ctx, "wait doc rename timeout")
				return fmt.Errorf("wait doc rename timeout")
			}
			if !renamedDoc.IsStableStatus() {
				log.InfoContextf(ctx, "renamed doc status is not stable, id: %v, status: %v", renamedDoc.ID, renamedDoc.Status)
				time.Sleep(waitDocRenameSleepTime * time.Second)
				continue
			}
			break
		}
		// 文档重命名处理完成，成功的场景下直接在Done中更新提示信息，失败立刻更新提示信息
		if renamedDoc.Status == model.DocStatusWaitRelease || renamedDoc.Status == model.DocStatusReleaseSuccess {
			log.InfoContextf(ctx, "rename operation success, doc %v, status: %v", renamedDoc.ID, renamedDoc.Status)
			return nil
		}
		if renamedDoc.Status == model.DocStatusDocNameAuditFail {
			log.ErrorContextf(ctx, "rename operation failed, doc %v, status: %v", renamedDoc.ID, renamedDoc.Status)
			return err
		} else {
			// 原理上重命名不应该出现其他状态，报错
			log.ErrorContextf(ctx, "doc %v rename failed, unexpect status %v", renamedDoc.ID, renamedDoc.Status)
			return err
		}
	}
	return nil
}

func (d *DocDiffOperationScheduler) operateQA(ctx context.Context, progress *task_scheduler.Progress,
	newDoc *model.Doc, oldDoc *model.Doc, app *admin.GetAppInfoRsp) error {
	op := d.p.QAOperation

	var uniqueQas []*model.QA
	uniqueQaMap := make(map[string]bool)
	// 本函数的代码中，在defer中判断了err，代码中不要覆盖err变量
	var err error
	message := ""
	d.tokenDosage.StartTime = time.Now()
	taskKV := progress.TaskKV(ctx)
	qaOperationFinish := false
	var qaTask *model.DocQATask
	defer func() {
		// 如果是多批次，且批次没完成，不更新
		if !qaOperationFinish && err == nil {
			log.InfoContextf(ctx, "task not finish yet, not update qa status, remain %v", progress.Remain())
			return
		}

		qaOperationStatus := model.DocDiffQAAndDocOpStatusSuccess
		if err != nil {
			// 失败立刻更新doc diff task表，所有批次成功后，在Done中更新
			qaOperationStatus = model.DocDiffQAAndDocOpStatusFailed
			if message == "" {
				message = i18n.Translate(ctx, i18nkey.KeyQAOperationFailure)
			}
		}

		log.InfoContextf(ctx, "update qa operation status: %v, message: %v", qaOperationStatus, message)
		update := &model.DocDiff{
			QaOperationStatus: uint32(qaOperationStatus),
			QaOperationResult: message,
		}
		updateColumns := []string{dao.DocDiffTaskTblColQaOperationStatus, dao.DocDiffTaskTblColQaOperationResult}
		_ = dao.GetDocDiffTaskDao().UpdateDocDiffTasks(ctx, nil, updateColumns, d.p.CorpBizID, app.GetAppBizId(),
			[]uint64{d.p.DocDiffID}, update)
	}()

	// 1. 先处理文档生成问答
	segmentToQAType := model.GetDocDiffSegmentToQAType(d.p.QAOperation)
	if segmentToQAType != model.SegmentToQATypeNone {
		docForGenQa := newDoc
		if segmentToQAType == model.SegmentToQATypeOldAll || segmentToQAType == model.SegmentToQATypeOldUni {
			docForGenQa = oldDoc
		}
		for key, value := range taskKV {
			// 数据库取出旧文档差异片段，生成问答；重命名文档
			if !logicCorp.CheckModelStatus(ctx, d.dao, d.p.CorpID, d.tokenDosage.ModelName, client.DocExtractQABizType) {
				log.WarnContextf(ctx, "task(DocDiffOperation) checkModelStatus 余量不足 TaskKV, taskID %v", d.p.DocDiffID)
				message = i18n.Translate(ctx, i18nkey.KeyGenerateQAFailureNoResources)
				progress.Stop(ctx)
				err = d.dao.StopQaTask(ctx, d.p.CorpID, d.p.RobotID, d.p.DocQATaskID, true,
					d.tokenDosage.ModelName)
				if err != nil {
					log.ErrorContextf(ctx, "stop qa task failed")
					return err
				}
				err = fmt.Errorf(message)
				return err
			}

			err = d.segmentToQA(ctx, cast.ToUint64(value), app, docForGenQa, uniqueQaMap, uniqueQas)
			if err != nil {
				if errors.Is(err, errs.ErrOperateDoing) {
					log.ErrorContextf(ctx, "task(DocDiffOperation) checkSegmentOrgDataExists err:%v", err)
					// 获取锁冲突的时候跳过，等待下一次重试
					continue
				}
				_ = d.docToQAFailed(ctx, oldDoc, newDoc)
				return err
			}
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocDiffOperation) Finish kv:%s err:%+v", key, err)
				return err
			}
			log.InfoContextf(ctx, "finish segment to qa, key: %v", key)
		}
		if progress.Remain() != 0 {
			// 如果有问答需要生成，并且问答生成批次没有结束
			return nil
		}
		// 后处理：更新doc segment doc_qa_task等状态
		err = d.docToQADone(ctx, oldDoc, newDoc)
		if err != nil {
			return err
		}
	}

	// 2. 处理其他问答操作
	var qas []*model.DocQA
	qaCount := 0
	log.InfoContextf(ctx, "begin to process qa db operation")

	genQACount := -1
	if model.GetDocDiffSegmentToQAType(op) != model.SegmentToQATypeNone {
		docQATask, docQATaskErr := d.dao.GetDocQATaskByID(ctx, d.p.DocQATaskID, d.p.CorpID, d.p.RobotID)
		if docQATaskErr != nil && docQATask != nil {
			genQACount = int(docQATask.QACount)
		}
	}

	switch op {
	case model.QAOperation0, model.QAOperation6, model.QAOperation15:
		// 不需要对问答做处理
	case model.QAOperation2, model.QAOperation4, model.QAOperation5,
		model.QAOperation11, model.QAOperation13, model.QAOperation14,
		model.QAOperation17, model.QAOperation20:
		// 这些case没有其他的问答数据库操作
		if op == model.QAOperation2 || op == model.QAOperation20 {
			// 获取新文档问答数量
			qas, err = getDocNotDeleteQA(ctx, newDoc, d.dao)
			if err != nil {
				// 这里只是为了获取问答数据写提示消息，报错不影响结果
				message = i18n.Translate(ctx, i18nkey.KeyNewDocQARetained)
				log.ErrorContextf(ctx, "get doc %v not delete qa failed, %v", newDoc.ID, err)
			} else {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocQARetainedCount, len(qas))
			}
		} else if op == model.QAOperation4 || op == model.QAOperation5 {
			if genQACount != -1 {
				if op == model.QAOperation4 {
					message = i18n.Translate(ctx, i18nkey.KeyOldDocDiffFragmentGenerateQACountPendingReview, genQACount)
				} else {
					message = i18n.Translate(ctx, i18nkey.KeyOldDocGenerateQACountPendingReview, genQACount)
				}
			} else {
				if op == model.QAOperation4 {
					message = i18n.Translate(ctx, i18nkey.KeyOldDocDiffFragmentQAGenerateSuccessPendingReview)
				} else {
					message = i18n.Translate(ctx, i18nkey.KeyOldDocQAGenerateSuccessPendingReview)
				}
			}
		} else if op == model.QAOperation11 || op == model.QAOperation17 {
			qas, err = getDocNotDeleteQA(ctx, oldDoc, d.dao)
			message = i18n.Translate(ctx, i18nkey.KeyOldDocQARetained)
			if err == nil {
				if len(qas) == 0 {
					message = i18n.Translate(ctx, i18nkey.KeyRetainOldDocQAFailureNoOldQA)
				} else {
					message += i18n.Translate(ctx, i18nkey.KeyTotalQACount, len(qas))
				}
			}
		} else if op == model.QAOperation13 {
			if genQACount != -1 {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocDiffFragmentGenerateQACountPendingReview, genQACount)
			} else {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocDiffFragmentQAGenerateSuccess)
			}
		} else if op == model.QAOperation14 {
			if genQACount != -1 {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocGenerateQACountPendingReview, genQACount)
			} else {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocQAGenerateSuccessPendingReview)
			}
		}

	case model.QAOperation1, model.QAOperation19:
		// 将新文档的问答关联到旧文档
		qaCount, err = d.rebindQAToDoc(ctx, newDoc, oldDoc)
		if err != nil {
			message = i18n.Translate(ctx, i18nkey.KeyNewDocQAAssociateOldDocFailure)
			return err
		}
		if qaCount == 0 {
			message = i18n.Translate(ctx, i18nkey.KeyNewDocQAAssociateOldDocFailureNoNewQA)
			return fmt.Errorf("no qa is new doc")
		}
		message = i18n.Translate(ctx, i18nkey.KeyNewDocQAAssociatedToOldDocWithDetails, qaCount)
	case model.QAOperation3, model.QAOperation21:
		// 删除新文档生成的问答
		message = i18n.Translate(ctx, i18nkey.KeyDeleteNewDocQAFailure)
		qas, err = getDocNotDeleteQA(ctx, newDoc, d.dao)
		if err != nil {
			log.ErrorContextf(ctx, "get doc %v not delete qa failed, %v", newDoc.ID, err)
			return err
		}
		if len(qas) == 0 {
			message = i18n.Translate(ctx, i18nkey.KeyNewDocQADeleted)
		} else if err = d.dao.DeleteQAs(ctx, d.p.CorpID, d.p.RobotID, d.p.StaffID, qas); err != nil {
			log.ErrorContextf(ctx, "delete qas failed, %v", err)
			return err
		}
		log.InfoContextf(ctx, "delete doc %v %v qa success", newDoc.ID, newDoc.FileName)
		message = i18n.Translate(ctx, i18nkey.KeyNewDocQADeletedCount, len(qas))
	case model.QAOperation7:
		// 2. 删除旧文档差异片段产生的QA
		var segmentDiff *SegmentDiff
		segmentDiff, err = d.diffDocSegment(ctx, oldDoc, newDoc)
		if err != nil {
			return err
		}
		var segmentIDs []uint64
		for _, seg := range segmentDiff.oldUni {
			segmentIDs = append(segmentIDs, seg.ID)
		}
		qas, err = dao.GetDocQaDao().GetQasBySegmentIDs(ctx, d.p.CorpID, oldDoc.ID, segmentIDs)
		if err != nil {
			log.ErrorContextf(ctx, "GetQasBySegmentIDs error, %v", err)
			return err
		}
		err = d.dao.DeleteQAs(ctx, d.p.CorpID, d.p.RobotID, d.p.StaffID, qas)
		if err != nil {
			log.ErrorContextf(ctx, "DeleteQAs error, %v", err)
			return err
		}
		log.InfoContextf(ctx, "delete doc %v %v diff %v segment qa success", oldDoc.ID, oldDoc.FileName, len(qas))

		// 3. 删除旧文档的差异片段产生的QA后，将旧文档公共部分(即剩余部分)的产生的QA关联到新文档，允许公共部分问答为0
		qaCount, err = d.rebindQAToDoc(ctx, oldDoc, newDoc)
		if err != nil {
			return err
		}
		log.InfoContextf(ctx, "rebind qa from old doc common segment to new doc success, qa count %v", qaCount)

		qaTask, err = d.dao.GetDocQATaskByID(ctx, d.p.DocQATaskID, d.p.CorpID, d.p.RobotID)
		if err == nil {
			message = i18n.Translate(ctx, i18nkey.KeyNewDocDiffFragmentGenerateQACountPendingReviewWithDot, qaTask.QACount)
		} else {
			message = i18n.Translate(ctx, i18nkey.KeyNewDocDiffFragmentQAGeneratedPendingReview)
		}
		message += i18n.Translate(ctx, i18nkey.KeyOldDocDeleteQARetainAssociateNewDoc, len(qas), qaCount)
	case model.QAOperation8, model.QAOperation10, model.QAOperation16:
		// 将旧文档全部问答关联到新文档
		qaCount, err = d.rebindQAToDoc(ctx, oldDoc, newDoc)
		if err != nil {
			message = i18n.Translate(ctx, i18nkey.KeyOldDocQAAssociateNewDocFailure)
			return err
		}
		if qaCount == 0 {
			message = i18n.Translate(ctx, i18nkey.KeyOldDocQAAssociateNewDocFailureNoNewQA)
			log.ErrorContextf(ctx, "no qa in old doc %v %v", oldDoc.ID, oldDoc.FileName)
			return fmt.Errorf("no qa is old doc")
		}
		// 操作成功，生成提示信息
		if op == model.QAOperation8 {
			qaTask, err = d.dao.GetDocQATaskByID(ctx, d.p.DocQATaskID, d.p.CorpID, d.p.RobotID)
			if err == nil {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocDiffFragmentGenerateQACountPendingReviewWithDot, qaTask.QACount)
			} else {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocDiffFragmentQAGeneratedPendingReview)
			}
			message += i18n.Translate(ctx, i18nkey.KeyRetainQAAssociateNewDoc, qaCount)
		} else if op == model.QAOperation10 || op == model.QAOperation16 {
			message = i18n.Translate(ctx, i18nkey.KeyOldDocQAAssociatedToNewDocWithDetails, qaCount)
		}

	case model.QAOperation9, model.QAOperation12, model.QAOperation18:
		// 删除旧文档问答
		qas, err = getDocNotDeleteQA(ctx, oldDoc, d.dao)
		if err != nil {
			log.ErrorContextf(ctx, "get doc %v not delete qa failed, %v", oldDoc.ID, err)
			return err
		}
		if err = d.dao.DeleteQAs(ctx, d.p.CorpID, d.p.RobotID, d.p.StaffID, qas); err != nil {
			message = i18n.Translate(ctx, i18nkey.KeyDeleteOldDocQAFailure)
			return err
		}
		log.InfoContextf(ctx, "delete doc %v %v %v qa success", oldDoc.ID, oldDoc.FileName, len(qas))

		// 操作完成，生成提示信息
		if op == model.QAOperation9 {
			qaTask, err = d.dao.GetDocQATaskByID(ctx, d.p.DocQATaskID, d.p.CorpID, d.p.RobotID)
			if err == nil {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocGenerateQACountPendingReviewWithDot, qaTask.QACount)
			} else {
				message = i18n.Translate(ctx, i18nkey.KeyNewDocQAGeneratedPendingReview)
			}
		}
		if message != "" {
			message = "\n" + message
		}
		message += i18n.Translate(ctx, i18nkey.KeyOldDocDeleteQACountPublishEffective, len(qas))
	default:
		return fmt.Errorf("qa operation type error, %v", d.p.QAOperation)
	}
	qaOperationFinish = true
	return nil
}

// rebindQAToDoc 将qa关联到文档，如果qa为0，返回0, nil
func (d *DocDiffOperationScheduler) rebindQAToDoc(ctx context.Context, qaSourceDoc, destDoc *model.Doc) (int, error) {
	qas, err := getDocNotDeleteQA(ctx, qaSourceDoc, d.dao)
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

	filter := &dao.DocQaFilter{
		BusinessIds: qaBizIDs,
		CorpId:      d.p.CorpID,
		RobotId:     d.p.RobotID,
	}
	qaUpdate := &model.DocQA{
		DocID: destDoc.ID,
	}
	err = dao.GetDocQaDao().UpdateDocQas(ctx, []string{dao.DocQaTblColDocId}, filter, qaUpdate)
	if err != nil {
		return qaCount, err
	}

	err = d.dao.UpdateQASimilarsDocID(ctx, qaIDs, destDoc.ID)
	if err != nil {
		return qaCount, err
	}
	log.InfoContextf(ctx, "rebind %v qa from %v %v to %v %v success", qaCount, qaSourceDoc.ID,
		qaSourceDoc.FileName, destDoc.ID, destDoc.FileName)
	return qaCount, nil
}

// Fail 任务失败
func (d *DocDiffOperationScheduler) Fail(ctx context.Context) error {
	log.ErrorContextf(ctx, "task(DocDiffOperation) Fail")
	// 1. 更新文档本身的状态标志位，发送通知
	d.cleanFlagAndSendNotice(ctx)

	// 任务出现异常，收尾，结束doc diff task 和 doc qa task的的状态
	selectColumns := []string{dao.DocDiffTaskTblColDocOperationStatus, dao.DocDiffTaskTblColQaOperationStatus,
		dao.DocDiffTaskTblColStatus, dao.DocDiffTaskTblColIsDeleted}
	docDiff, err := dao.GetDocDiffTaskDao().GetDocDiffTask(ctx, selectColumns, d.p.CorpBizID, d.p.RobotBizID, d.p.DocDiffID)
	if err == nil && !docDiff.IsDocDiffDeleted() {
		updateColumns := []string{dao.DocDiffTaskTblColStatus}
		update := &model.DocDiff{
			Status: uint32(model.DocDiffStatusFinish),
		}
		if docDiff.QaOperationStatus == uint32(model.DocDiffQAAndDocOpStatusFailed) {
			update.DocOperationStatus = uint32(model.DocDiffQAAndDocOpStatusFailed)
			updateColumns = append(updateColumns, dao.DocDiffTaskTblColDocOperationStatus)
		}
		log.InfoContextf(ctx, "update task %v failed, doc op status: %v, task status: %v", d.p.DocDiffID,
			update.DocOperationStatus, update.Status)
		_ = dao.GetDocDiffTaskDao().UpdateDocDiffTasks(ctx, nil, updateColumns, d.p.CorpBizID, d.p.RobotBizID,
			[]uint64{d.p.DocDiffID}, update)
	}

	return nil
}

func (d *DocDiffOperationScheduler) docToQAFailed(ctx context.Context, oldDoc, newDoc *model.Doc) error {
	defer func() {
		// 更新生成问答任务状态到终态
		if err := d.dao.UpdateDocQATaskStatus(ctx, model.DocQATaskStatusFail, d.p.DocQATaskID); err != nil {
			log.ErrorContextf(ctx, "Fail UpdateDocQATaskStatus failed, err:%+v|QaTaskID|%d",
				err, d.p.DocQATaskID)
		}
	}()

	targetDoc := newDoc
	if model.GetDocDiffSegmentToQAType(d.p.QAOperation) == model.SegmentToQATypeOldUni ||
		model.GetDocDiffSegmentToQAType(d.p.QAOperation) == model.SegmentToQATypeOldUni {
		targetDoc = oldDoc
	}
	targetDoc.Message = i18nkey.KeyGenerateQAFailed
	targetDoc.IsCreatingQA = false
	targetDoc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
	targetDoc.UpdateTime = time.Now()
	if err := d.dao.CreateDocQADone(ctx, d.p.StaffID, targetDoc, -1, false); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Fail CreateDocQADone failed, err:%+v|doc|%v",
			err, targetDoc)
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocDiffOperationScheduler) Stop(ctx context.Context) error {
	log.InfoContextf(ctx, "task(DocDiffOperation) stop, doc diff id: %v", d.p.DocDiffID)
	_ = d.Fail(ctx)
	return nil
}

// Done 任务完成回调
func (d *DocDiffOperationScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(DocDiffOperation) done, doc diff id: %v", d.p.DocDiffID)
	// 1. 更新文档本身的状态标志位
	d.cleanFlagAndSendNotice(ctx)

	return nil
}

func (d *DocDiffOperationScheduler) cleanFlagAndSendNotice(ctx context.Context) {
	oldFileName := ""
	newFileName := ""
	oldDoc, err := d.dao.GetDocByBizID(ctx, d.p.OldDocBizID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "get old doc failed, %v", err)
		return
	}
	if oldDoc != nil {
		_ = clearDocProcessingFlag(ctx, oldDoc)
		oldFileName = oldDoc.GetRealFileName()
	}

	newDoc, err := d.dao.GetDocByBizID(ctx, d.p.NewDocBizID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocByBizID failed, %v", err)
		return
	}
	if newDoc != nil {
		_ = clearDocProcessingFlag(ctx, newDoc)
		newFileName = newDoc.GetRealFileName()
	}

	_ = doc_diff_task.CreateDocDiffTaskRunningNotice(ctx, d.dao, d.p.StaffID, d.p.CorpID, d.p.RobotID, d.p.DocDiffID,
		newFileName, oldFileName, false)

	return
}

// docToQADone 文档生成问答成功的后处理
func (d *DocDiffOperationScheduler) docToQADone(ctx context.Context, oldDoc, newDoc *model.Doc) error {
	targetDoc := newDoc
	if model.GetDocDiffSegmentToQAType(d.p.QAOperation) == model.SegmentToQATypeOldUni ||
		model.GetDocDiffSegmentToQAType(d.p.QAOperation) == model.SegmentToQATypeOldUni {
		targetDoc = oldDoc
	}

	qaListReq := &model.QAListReq{
		CorpID:  d.p.CorpID,
		DocID:   []uint64{targetDoc.ID},
		RobotID: d.p.RobotID,
	}
	count, err := d.dao.GetQaCountWithDocID(ctx, qaListReq)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done GetQaCountWithDocID failed, err:%+v|qaListReq|%v",
			err, qaListReq)
		return err
	}
	targetDoc.Message = i18nkey.KeyGenerateQASuccess
	targetDoc.IsCreatingQA = false
	targetDoc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
	targetDoc.UpdateTime = time.Now()

	success := true
	if count == 0 {
		success = false
	}
	// 更新doc表IsCreatingQA标志位，发送notice
	if err = d.dao.CreateDocQADone(ctx, d.p.StaffID, targetDoc, int(count), success); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done CreateDocQADone failed, err:%+v|DocID %d",
			err, targetDoc.ID)
		return err
	}
	// 更新生成问答任务状态
	if err = d.dao.UpdateDocQATaskStatus(ctx, model.DocQATaskStatusSuccess, d.p.DocQATaskID); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done UpdateDocQATaskStatus failed, err:%+v|QaTaskID|%d",
			err, d.p.DocQATaskID)
		return err
	}

	// 还原已完成切片状态
	if err = d.dao.UpdateQaSegmentToDocStatus(ctx, targetDoc.ID, targetDoc.BatchID, targetDoc.RobotID); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
			err, d.p.DocQATaskID)
		return err
	}

	key := fmt.Sprintf("%s%d", model.DocQaExistsOrgDataPreFix, targetDoc.ID)
	// 重置orgData去重缓存
	if _, err = d.dao.RedisCli().Do(ctx, "DEL", key); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done, Redis del failed, err:%+v", err)
		return err
	}
	return nil
}

func clearDocProcessingFlag(ctx context.Context, doc *model.Doc) error {
	updateDocFilter := &dao.DocFilter{
		IDs:     []uint64{doc.ID},
		CorpId:  doc.CorpID,
		RobotId: doc.RobotID,
	}
	update := &model.Doc{
		ProcessingFlag: doc.ProcessingFlag,
	}
	update.RemoveProcessingFlag([]uint64{model.DocProcessingFlagHandlingDocDiffTask})
	updateDocColumns := []string{
		dao.DocTblColProcessingFlag}
	_, err := dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		log.ErrorContextf(ctx, "clear doc %v %v diff task flag err: %+v", doc.ID, doc.FileName, err)
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
func (d *DocDiffOperationScheduler) segmentToQA(ctx context.Context, segmentID uint64, app *admin.GetAppInfoRsp,
	doc *model.Doc, uniqueQaMap map[string]bool, uniqueQas []*model.QA) error {
	segment, err := d.dao.GetSegmentByID(ctx, segmentID, app.GetId())
	if err != nil {
		return err
	}
	if segment == nil {
		return errs.ErrSegmentNotFound
	}
	if segment.SegmentType == model.SegmentTypeTable {
		// 表格类型不生成问答
		return nil
	}
	if segment.OrgData == "" { // 如果旧表格没有orgData，则从t_doc_segment_org_data新表中获取orgData
		corpReq := &admin.GetCorpReq{
			Id: app.GetCorpId(),
		}
		corpRsp, err := d.dao.GetAdminApiCli().GetCorp(ctx, corpReq)
		if err != nil || corpRsp == nil {
			return errs.ErrCorpNotFound
		}
		orgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataByBizID(ctx,
			[]string{dao.DocSegmentOrgDataTblColOrgData}, corpRsp.GetCorpBizId(), app.GetAppBizId(),
			doc.BusinessID, segment.OrgDataBizID)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if orgData != nil && orgData.OrgData != "" {
			segment.OrgData = orgData.OrgData
		}
		log.DebugContextf(ctx, "task(DocToQA) Process GetDocOrgDataByBizID|segment.OrgData:%s",
			segment.OrgData)
	}
	// 判断是否有重复的orgData,重复的orgData不进行问答生成
	exists, err := checkSegmentOrgDataExists(ctx, segment, d.dao, segment.DocID)
	if err != nil {
		return err
	}
	if exists {
		log.DebugContextf(ctx, "task(DocDiffOperation) checkSegmentOrgDataExists 重复ordData:%v segment:%+v",
			exists, segment)
		// 更新生成问答任务已完成切片数量和问答数量
		if err = d.dao.UpdateDocQATaskSegmentDoneAndQaCount(ctx, 0, 1,
			d.p.CorpID, d.p.RobotID, d.p.DocQATaskID); err != nil {
			log.ErrorContextf(ctx, "task(DocDiffOperation) Done UpdateDocQATaskSegmentCountDone failed,"+
				" err:%+v|QaTaskID|%d", err, d.p.DocQATaskID)
			return err
		}
		segment.Status = model.SegmentStatusCreatedQa
		// 更新切片状态
		if err = d.dao.UpdateQaSegmentStatus(ctx, segment, d.p.RobotID); err != nil {
			log.ErrorContextf(ctx, "task(DocDiffOperation) Done UpdateDocQATaskSegmentCountDone failed,"+
				" err:%+v|QaTaskID|%d", err, d.p.DocDiffID)
			return err
		}
		return nil
	}
	segment.ExpireStart = doc.ExpireStart
	segment.ExpireEnd = doc.ExpireEnd
	tree, qas, tokenStatisticInfo, err := getQAAndCateNode(ctx, doc, segment, app, d.dao, d.tokenDosage.ModelName)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocDiffOperation) getQAAndCateNode err|%v|doc:%+v|segment:%+v",
			err, doc, segment.ID)
		return err
	}
	qas = slicex.Filter(qas, func(qa *model.QA) bool {
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
	if err = d.dao.BatchCreateQA(ctx, segment, doc, uniqueQas, tree, false); err != nil {
		log.ErrorContextf(ctx, "task(DocDiffOperation) Process BatchCreateQA failed,"+
			" err:%+v|DocDiffID|%d|docID|%d", err, d.p.DocDiffID, doc.ID)
		return err
	}
	// 通过文档生成的问答默认未采纳，这里在采纳问答的时候会更新机器人字符使用量，调用大模型，消耗token，上报token
	// 调用大模型，消耗token，上报token
	err = reportTokenDosage(ctx, tokenStatisticInfo, d.dao, &d.tokenDosage, d.p.CorpID, d.p.RobotID, app.GetAppBizId(), d.p.DocQATaskID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Process reportTokenDosage failed,"+
			" err:%+v|tokenStatisticInfo|%v", err, tokenStatisticInfo)
		return err
	}

	segment.Status = model.SegmentStatusCreatedQa
	// 更新切片状态
	if err = d.dao.UpdateQaSegmentStatus(ctx, segment, d.p.RobotID); err != nil {
		log.ErrorContextf(ctx, "task(DocDiffOperation) Done UpdateDocQATaskSegmentCountDone failed,"+
			" err:%+v|QaTaskID|%d", err, d.p.DocDiffID)
		return err
	}

	// 更新生成问答任务已完成切片数量和问答数量
	if err = d.dao.UpdateDocQATaskSegmentDoneAndQaCount(ctx, uint64(len(uniqueQas)), 1,
		d.p.CorpID, d.p.RobotID, d.p.DocQATaskID); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done UpdateDocQATaskSegmentCountDone failed,"+
			" err:%+v|QaTaskID|%d", err, d.p.DocQATaskID)
		return err
	}
	return nil
}
