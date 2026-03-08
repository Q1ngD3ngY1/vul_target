package task

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// CreateDocDiffTask 创建对比任务
func (l *Logic) CreateDocDiffTask(ctx context.Context, corpID, robotID, corpBizId,
	robotBizId uint64, newDocID, oldDocID,
	staffID uint64, comparisonReason uint32, isAutoRunDiff bool) (uint64, error) {

	err := l.checkCreateDocDiffTask(ctx, corpID, robotID, corpBizId,
		robotBizId, newDocID, oldDocID, isAutoRunDiff)
	if err != nil {
		logx.W(ctx, "checkCreateDocDiffTask fail: %+v", err)
		return 0, err
	}

	createDiffID := idgen.GetId()
	docDiff := docEntity.DocDiff{}
	docDiff.BusinessID = createDiffID
	docDiff.CorpBizID = corpBizId
	docDiff.RobotBizID = robotBizId
	docDiff.NewDocBizID = newDocID
	docDiff.OldDocBizID = oldDocID
	docDiff.ComparisonReason = comparisonReason
	docDiff.StaffBizID = staffID
	docDiff.DiffDataProcessStatus = docEntity.DiffDataProcessStatusInit
	err = l.docLogic.GetDao().CreateDocDiff(ctx, &docDiff)
	if err != nil {
		logx.E(ctx, "创建文档对比任务失败 CreateDocDiffTask err: %+v", err)
		return 0, err
	}
	// 创建文档比对任务的时候要同步创建文件文档比对详情异步任务
	err = scheduler.NewDocDiffDataTask(ctx, robotID,
		entity.DocDiffParams{CorpBizID: corpBizId, RobotBizID: robotBizId, DiffBizID: createDiffID})
	if err != nil {
		logx.E(ctx, "创建文档对比任务失败 CreateDocDiffTask err: %+v", err)
		return 0, err
	}
	return createDiffID, nil
}

// checkCreateDocDiffTask 检查是否可以创建对比任务
func (l *Logic) checkCreateDocDiffTask(ctx context.Context, corpID, robotID, corpBizId,
	robotBizId, newDocID, oldDocID uint64, isAutoRunDiff bool) error {
	filter := &docEntity.DocFilter{
		CorpId:      corpID,
		RobotId:     robotID,
		IsDeleted:   ptrx.Bool(false),
		BusinessIds: []uint64{newDocID, oldDocID},
		Limit:       2,
	}
	docs, err := l.docLogic.GetDao().GetDiffDocs(ctx, filter)
	if err != nil {
		logx.E(ctx, "获取需要对比的文档失败 checkCreateDocDiffTask err: %+v", err)
		return err
	}
	if len(docs) != 2 {
		logx.W(ctx, "需要对比的文档不存在 newDocID:%+d oldDocID:%+d", newDocID, oldDocID)
		return errs.ErrCreateDocDiffTaskFail
	}
	if docs[0].FileType != docs[1].FileType {
		logx.W(ctx, "需要对比的文档类型不一致 docID1:%+d docType1:%s,docID2:%+d docType2:%s",
			docs[0].BusinessID, docs[0].FileType, docs[1].BusinessID, docs[1].FileType)
		return errs.ErrCreateDocDiffTaskDocTypeFail
	}

	docBizIds := make([]uint64, 0)
	for _, doc := range docs {
		if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleaseSuccess {
			logx.W(ctx, "需要对比的文档状态不可用发起对比 DocID:%d,Status:%d", doc.BusinessID, doc.Status)
			return errs.ErrCreateDocDiffTaskFail
		}
		docBizIds = append(docBizIds, doc.BusinessID)
	}
	if isAutoRunDiff {
		// 自动对比任务,可以创建重复文档的对比任务,但是同时只能有一个在执行
		return nil
	}
	docDiffFilter := &docEntity.DocDiffTaskFilter{
		CorpBizId:     corpBizId,
		RobotBizId:    robotBizId,
		IsDeleted:     ptrx.Bool(false),
		Statuses:      []int32{docEntity.DocDiffTaskStatusInit, docEntity.DocDiffTaskStatusProcessing},
		InNewOldDocId: docBizIds,
		Limit:         len(docBizIds),
	}
	docDiffList, err := l.docLogic.GetDao().GetDocDiffTaskList(ctx, docEntity.DocDiffTaskTblColList, docDiffFilter)
	if err != nil {
		logx.E(ctx, "获取是否已有文档在对比任务中 GetDocDiffTaskList err: %+v", err)
		return err
	}
	if len(docDiffList) > 0 {
		logx.I(ctx, "checkCreateDocDiffTask 文档:%d或%d, 有未完成的对比任务 不启动对比任务",
			newDocID, oldDocID)
		return errs.ErrCreateDocDiffTaskInTaskFail
	}
	return nil
}

// addAutoRunDocDiffTask 添加自动对比任务
func (l *Logic) addAutoRunDocDiffTask(ctx context.Context, newDoc *docEntity.Doc, oldDocs []*docEntity.Doc, staffID, corpBizId, robotBizId uint64) error {
	if newDoc == nil {
		logx.W(ctx, "addAutoRunDocDiffTask newDoc:%+v is nil", newDoc)
		return nil
	}

	comparisonReason := docEntity.DocDiffTaskComparisonReasonNameDiff
	if newDoc.Source == docEntity.SourceFromWeb {
		comparisonReason = docEntity.DocDiffTaskComparisonReasonUrlDiff
	}
	// 组装对比任务参数,可能发起多个自动对比任务
	for _, oldDoc := range oldDocs {
		logx.I(ctx, "addAutoRunDocDiffTask newDocFile:%+v,oldDocsbizID:%+v",
			newDoc.FileName, oldDoc.BusinessID)
		diffTaskID, err := l.CreateDocDiffTask(ctx, newDoc.CorpID, newDoc.RobotID, corpBizId,
			robotBizId, newDoc.BusinessID,
			oldDoc.BusinessID, staffID, comparisonReason, true)
		if err != nil {
			logx.E(ctx, "addAutoRunDocDiffTask 创建对比任务失败  newDocbizID:%+v,oldDocbizID:%+v err: %+v",
				newDoc.BusinessID, oldDoc.BusinessID, err)
			return err
		}
		logx.I(ctx, "addAutoRunDocDiffTask Done newDocbizID:%+v,oldDocbizID:%+v,diffTaskID:%d",
			newDoc.BusinessID, oldDoc.BusinessID, diffTaskID)
	}
	return nil
}

// AutoRunDocDiffTask 检查是否需要自动启用diff任务
func (l *Logic) AutoRunDocDiffTask(ctx context.Context, doc *docEntity.Doc, staffID uint64) error {
	if doc == nil {
		logx.W(ctx, "AutoRunDocDiffTask doc:%+v is nil", doc)
		return nil
	}
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
	// corp, err := getDao.GetCorpByID(ctx, doc.CorpPrimaryId)
	if err != nil {
		logx.E(ctx, "AutoRunDocDiffTask GetCorpByID err: %+v", err)
		return err
	}
	corpBizId := corp.GetCorpId()
	app, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil {
		logx.E(ctx, "AutoRunDocDiffTask DescribeAppByPrimaryIdWithoutNotFoundError err: %+v", err)
		return err
	}
	robotBizId := app.BizId
	// 如果在黑名单中，则不启动自动对比任务，避免超大知识库的文档数量太多，导致数据库慢查询
	if config.IsInWhiteList(corp.Uin, robotBizId, config.GetWhitelistConfig().AutoDocDiffBlackList) {
		logx.W(ctx, "AutoRunDocDiffTask corp:%s,robot:%d,doc:%d in black list, not run auto diff task",
			corp.Uin, robotBizId, doc.BusinessID)
		return nil
	}

	diffDoc, err := l.docLogic.GetDocDiff(ctx, doc)
	if err != nil {
		logx.E(ctx, "Failed to AutoRunDocDiffTask. GetDocDiffURL err: %+v", err)
		return err
	}
	logx.I(ctx, "checkAutoRunDiff len:%d", len(diffDoc))
	if len(diffDoc) == 0 {
		logx.I(ctx, "doc:%d NotAutoRunDiff task", doc.BusinessID)
		return nil
	}
	docIds := make([]uint64, 0)
	docIds = append(docIds, doc.ID) // 加入主文档id
	docBizIds := make([]uint64, 0)
	docBizIds = append(docBizIds, doc.BusinessID) // 加入主文档id
	for _, doc := range diffDoc {
		docIds = append(docIds, doc.ID)
		docBizIds = append(docBizIds, doc.BusinessID)
	}

	staff, err := l.rpc.PlatformAdmin.GetStaffByID(ctx, staffID)
	if err != nil || staff == nil {
		return errs.ErrStaffNotFound
	}
	staffBizId := staff.BusinessID

	filter := &docEntity.DocDiffTaskFilter{
		CorpBizId:     corpBizId,
		RobotBizId:    robotBizId,
		IsDeleted:     ptrx.Bool(false),
		Statuses:      []int32{docEntity.DocDiffTaskStatusInit, docEntity.DocDiffTaskStatusProcessing},
		InNewOldDocId: docBizIds,
		Limit:         len(docBizIds),
	}
	docDiffList, err := l.docLogic.GetDao().GetDocDiffTaskList(ctx, docEntity.DocDiffTaskTblColList, filter)
	if err != nil {
		logx.E(ctx, "获取是否已有文档在对比任务中 GetDocDiffTaskList err: %+v", err)
		return err
	}
	docDiffIngDocBizID := make(map[uint64]uint64) // 已在未完成对比任务中的文档bizList
	if len(docDiffList) > 0 {
		for _, diff := range docDiffList {
			docDiffIngDocBizID[diff.OldDocBizID] = diff.BusinessID
			docDiffIngDocBizID[diff.NewDocBizID] = diff.BusinessID
		}
	}

	// 判断主文档，就是新文档，已经发起任务则直接返回
	if diffID, ok := docDiffIngDocBizID[doc.BusinessID]; ok {
		logx.I(ctx, "AutoRunDocDiffTask 主文档:%s,%d, 有未完成的对比任务 diffID:%d 不启动对比任务", doc.FileName, doc.ID, diffID)
		return nil
	}

	runDiffDoc := make([]*docEntity.Doc, 0) // 需要对比的旧文档列表
	// 判断查询出来重名或者重复url的文档，是否有进行中diff任务，如果有，则不启动自动diff任务
	for _, doc := range diffDoc {
		// 判断文档是否已在对比任务中，如果在，则不加入runDiffDoc
		if diffID, ok := docDiffIngDocBizID[doc.BusinessID]; ok {
			logx.I(ctx, "AutoRunDocDiffTask 文档:%s,%d, 有未完成的对比任务 diffID:%d 不启动对比任务", doc.FileName, doc.ID, diffID)
			continue
		}
		runDiffDoc = append(runDiffDoc, doc)
	}

	if len(runDiffDoc) == 0 {
		logx.I(ctx, "doc:%d AutoRunDiff 可执行diff的旧文档为空:%+v", doc.BusinessID, runDiffDoc)
		return nil
	}

	// 需要自动创建diff任务,根据 doc source 判断 网页diff 还是 文件diff
	logx.I(ctx, "doc:%d AutoRunDiff diffDoc:%+v", doc.BusinessID, runDiffDoc)
	err = l.addAutoRunDocDiffTask(ctx, doc, runDiffDoc, staffBizId, corpBizId, robotBizId)
	if err != nil {
		logx.E(ctx, "添加自动对比任务失败 addAutoRunDocDiffTask err: %+v", err)
		return err
	}

	noticeOptionContent := i18n.Translate(ctx, i18nkey.KeyDiscoverDuplicateNameDocuments)
	if doc.Source == docEntity.SourceFromWeb {
		noticeOptionContent = i18n.Translate(ctx, i18nkey.KeyDiscoverDuplicateURLDocuments)
	}
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeDocAutoDiffTask, Params: releaseEntity.OpParams{}}}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelWarning),
		releaseEntity.WithContent(noticeOptionContent),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocAutoDiffTask, doc.ID, doc.CorpID, doc.RobotID, staffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "CreateNotice err err:%+v", err)
		return err
	}
	return nil
}

// DeleteDocDiffTask 删除对比任务
func (l *Logic) DeleteDocDiffTask(ctx context.Context, corpBizID, robotBizID uint64, docDiffIds []string) error {
	if len(docDiffIds) > docEntity.HandleDocDiffSize {
		return errs.ErrHandleDocDiffSizeFail
	}
	diffIds := make([]uint64, 0)
	for _, taskID := range docDiffIds {
		diffTaskID, err := util.CheckReqBotBizIDUint64(ctx, taskID)
		if err != nil {
			return err
		}
		diffIds = append(diffIds, diffTaskID)
	}
	listFilter := &docEntity.DocDiffTaskFilter{
		BusinessIds: diffIds,
		CorpBizId:   corpBizID,
		RobotBizId:  robotBizID,
		IsDeleted:   ptrx.Bool(false),
		Limit:       len(diffIds),
	}
	list, err := l.docLogic.GetDao().GetDocDiffTaskList(ctx, docEntity.DocDiffTaskTblColList, listFilter)
	if err != nil {
		logx.E(ctx, "获取文档对比任务列表 GetDocDiffTaskList err: %+v", err)
		return err
	}
	if len(list) == 0 || len(list) != len(diffIds) {
		return errs.ErrHandleDocDiffNotFound
	}

	for _, docDiffTask := range list {
		if docDiffTask.Status == uint32(docEntity.DocDiffTaskStatusProcessing) {
			logx.W(ctx, "删除对比任务 DeleteDocDiffTask DiffTaskID: %+v status:+%d",
				docDiffTask.BusinessID, docDiffTask.Status)
			return errs.ErrHandleDocDiffNotFail
		}
	}
	err = l.deleteDocDiffTasksAndData(ctx, corpBizID, robotBizID, diffIds)
	if err != nil {
		logx.E(ctx, "删除对比任务 deleteDocDiffTask err: %+v", err)
		return err
	}

	return nil
}

// GetDocDiffGeneratingMaps 查询文档对比任务进行中的任务集合
func (l *Logic) GetDocDiffGeneratingMaps(ctx context.Context, corpID, robotID uint64, docID []uint64) (
	map[uint64]*qaEntity.DocQATask, error) {
	if len(docID) == 0 || corpID == 0 || robotID == 0 {
		return nil, errs.ErrDocQaTaskNotFound
	}
	generatingStatus := []int{qaEntity.DocQATaskStatusGenerating, qaEntity.DocQATaskStatusPause,
		qaEntity.DocQATaskStatusResource, qaEntity.DocQATaskStatusFail}

	notDeleted := docEntity.DocIsNotDeleted
	filter := &qaEntity.DocQaTaskFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		IsDeleted: &notDeleted,
		Status:    generatingStatus,
		DocId:     docID,
	}
	docQATaskMap := make(map[uint64]*qaEntity.DocQATask)
	docQATasks, err := l.qaLogic.GetDao().GetDocQaTaskList(ctx, qaEntity.DocQaTaskTblColList, filter)
	if err != nil {
		logx.E(ctx, "获取文档对比任务进行中的任务集合失败 GetDocQaTasks err: %+v", err)
		return docQATaskMap, err
	}
	if len(docQATasks) == 0 {
		return docQATaskMap, nil
	}
	for _, dqt := range docQATasks {
		docQATaskMap[dqt.DocID] = dqt
	}
	return docQATaskMap, nil
}

// getDocDiffGeneratingDocMaps 获取 文档中有对比任务进行中的,对比任务中文档详情,文档有生成问答进行中的
func (l *Logic) getDocDiffGeneratingDocMaps(ctx context.Context, corpID, robotID uint64, corpBizId,
	robotBizId uint64, docBizID []uint64) (map[uint64]*docEntity.DocDiff, map[uint64]*docEntity.Doc,
	map[uint64]*qaEntity.DocQATask, error) {
	docBizMap := make(map[uint64]*docEntity.Doc)
	docDiffTaskMap := make(map[uint64]*docEntity.DocDiff)
	docQATaskMap := make(map[uint64]*qaEntity.DocQATask)

	notDeleted := ptrx.Bool(false)
	filter := &docEntity.DocDiffTaskFilter{
		CorpBizId:     corpBizId,
		RobotBizId:    robotBizId,
		IsDeleted:     notDeleted,
		Statuses:      []int32{docEntity.DocDiffTaskStatusProcessing},
		InNewOldDocId: docBizID,
		Limit:         len(docBizID),
	}
	docDiffList, err := l.docLogic.GetDao().GetDocDiffTaskList(ctx, docEntity.DocDiffTaskTblColList, filter)
	if err != nil {
		logx.E(ctx, "checkHandleDocDiffDoc 获取是否已有文档在对比任务中 GetDocDiffTaskList err: %+v", err)
		return docDiffTaskMap, docBizMap, docQATaskMap, err
	}
	for _, docDiffTask := range docDiffList { // 新旧文档有在执行中的任务
		docDiffTaskMap[docDiffTask.NewDocBizID] = docDiffTask
		docDiffTaskMap[docDiffTask.OldDocBizID] = docDiffTask
	}
	// if len(docDiffList) > 0 {
	//	logx.I(ctx, "checkCreateDocDiffTask 文档:%v 中有未完成的对比任务 不能启动对比任务",
	//		docBizID)
	//	return docBizMap, pkg.ErrCreateDocDiffTaskInTaskFail
	// }

	docFilter := &docEntity.DocFilter{
		CorpId:      corpID,
		RobotId:     robotID,
		IsDeleted:   notDeleted,
		BusinessIds: docBizID,
	}
	docs, err := l.docLogic.GetDao().GetDiffDocs(ctx, docFilter)
	if err != nil {
		logx.E(ctx, "Failed to get docDiffTask. checkCreateDocDiffTask err: %+v", err)
		return docDiffTaskMap, docBizMap, docQATaskMap, err
	}
	if len(docs) == 0 {
		logx.W(ctx, "DocDiffTasks were not found. docBizID:%+v，len(docBizID):%+d,len(docs) %+d",
			docBizID, len(docBizID), len(docs))
		return docDiffTaskMap, docBizMap, docQATaskMap, errs.ErrHandleDocDiffNotFail
	}
	docIds := make([]uint64, 0)
	for _, doc := range docs {
		// if doc.Status != model.DocStatusWaitRelease && doc.Status != model.DocStatusReleaseSuccess {
		//	logx.W(ctx, "需要对比的文档状态不可用发起对比 DocID:%d,Status:%d", doc.BusinessID, doc.Status)
		//	return docBizMap, pkg.ErrHandleDocDiffNotFail
		// }
		docIds = append(docIds, doc.ID)
		docBizMap[doc.BusinessID] = doc
	}

	docQaTask, err := l.qaLogic.GetDao().GetDocQATaskGeneratingMaps(ctx, corpID, robotID, docIds)
	if err != nil {
		logx.E(ctx, "获取文档执行中问答任务失败 GetDocQATaskGeneratingMaps err: %+v", err)
		return docDiffTaskMap, docBizMap, docQATaskMap, err
	}
	// for _, doc := range docs {
	//	if qaTask, ok := docQaTask[doc.ID]; ok {
	//		logx.I(ctx, "checkCreateDocDiffTask 文档:%s,%d, 有未完成的问答任务 taskID:%d 不启动对比任务",
	//			doc.FileName, doc.ID, qaTask.TaskID)
	//		return docBizMap, pkg.ErrCreateDocDiffTaskInQaFail
	//	}
	// }
	return docDiffTaskMap, docBizMap, docQaTask, nil
}

// checkHandleCreateDocDiff 检查对比任务文档是否可以处理
func (l *Logic) checkHandleDocDiffDoc(ctx context.Context, corpID, robotID uint64, corpBizId,
	robotBizId uint64, docBizID []uint64, qaContinue bool) (map[uint64]*docEntity.Doc, error) {
	docBizMap := make(map[uint64]*docEntity.Doc)
	notDeleted := ptrx.Bool(false)
	filter := &docEntity.DocDiffTaskFilter{
		CorpBizId:     corpBizId,
		RobotBizId:    robotBizId,
		IsDeleted:     notDeleted,
		Statuses:      []int32{docEntity.DocDiffTaskStatusProcessing},
		InNewOldDocId: docBizID,
		Limit:         len(docBizID),
	}
	docDiffList, err := l.docLogic.GetDao().GetDocDiffTaskList(ctx, docEntity.DocDiffTaskTblColList, filter)
	if err != nil {
		logx.E(ctx, "checkHandleDocDiffDoc 获取是否已有文档在对比任务中 GetDocDiffTaskList err: %+v", err)
		return docBizMap, err
	}
	if len(docDiffList) > 0 {
		logx.I(ctx, "checkCreateDocDiffTask 文档:%v 中有未完成的对比任务 不能启动对比任务",
			docBizID)
		return docBizMap, errs.ErrCreateDocDiffTaskInTaskFail
	}

	docFilter := &docEntity.DocFilter{
		CorpId:      corpID,
		RobotId:     robotID,
		IsDeleted:   notDeleted,
		BusinessIds: docBizID,
	}
	docs, err := l.docLogic.GetDao().GetDiffDocs(ctx, docFilter)
	if err != nil {
		logx.E(ctx, "获取需要对比的文档失败 checkCreateDocDiffTask err: %+v", err)
		return docBizMap, err
	}
	if len(docs) == 0 || len(docs) != len(docBizID) {
		logx.W(ctx, "没有找到处理任务的文档 docBizID:%+v，len(docBizID):%+d,len(docs) %+d",
			docBizID, len(docBizID), len(docs))
		return docBizMap, errs.ErrHandleDocDiffNotFail
	}
	docIds := make([]uint64, 0)
	for _, doc := range docs {
		if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleaseSuccess {
			logx.W(ctx, "需要对比的文档状态不可用发起对比 DocID:%d,Status:%d", doc.BusinessID, doc.Status)
			return docBizMap, errs.ErrHandleDocDiffNotFail
		}
		docIds = append(docIds, doc.ID)
		docBizMap[doc.BusinessID] = doc
	}
	if qaContinue {
		return docBizMap, nil
	}
	docQaTask, err := l.qaLogic.GetDao().GetDocQATaskGeneratingMaps(ctx, corpID, robotID, docIds)
	if err != nil {
		logx.E(ctx, "获取文档执行中问答任务失败 GetDocQATaskGeneratingMaps err: %+v", err)
		return docBizMap, err
	}
	for _, doc := range docs {
		if qaTask, ok := docQaTask[doc.ID]; ok {
			logx.I(ctx, "checkCreateDocDiffTask 文档:%s,%d, 有未完成的问答任务 taskID:%d 不启动对比任务",
				doc.FileName, doc.ID, qaTask.TaskID)
			return docBizMap, errs.ErrCreateDocDiffTaskInQaFail
		}
	}
	return docBizMap, nil
}

// HandleDocDiffTaskAction 处理文档对比任务
func (l *Logic) HandleDocDiffTaskAction(ctx context.Context, staffID, corpID, robotID, corpBizId,
	robotBizId uint64,
	req []*pb.HandleDocDiffTaskReq_DocDiffTask, rsp *pb.HandleDocDiffTaskRsp) error {
	if len(req) > docEntity.HandleDocDiffSize {
		return errs.ErrHandleDocDiffSizeFail
	}
	diffIds := make([]uint64, 0)
	reqMap := make(map[uint64]*pb.HandleDocDiffTaskReq_DocDiffTask)
	for _, diffTask := range req {
		var op docEntity.DiffTaskOperation
		op.LevelDiffTypeOperation = docEntity.DiffTypeOperation(diffTask.GetDiffType())
		op.LevelDocOperation = docEntity.DocOperation(diffTask.GetDocOperation())
		op.LevelQaOperation = docEntity.QAOperation(diffTask.GetQaOperation())
		if err := op.HandleDocDiffTaskValidate(ctx); err != nil {
			logx.W(ctx, "HandleDocDiffTaskValidate Operation:%v err: %+v", op, err)
			return err
		}
		if (docEntity.DocOperation(diffTask.GetDocOperation()) == docEntity.DocOperationOldReName ||
			docEntity.DocOperation(diffTask.GetDocOperation()) == docEntity.DocOperationNewReName) &&
			diffTask.GetDocRename() == "" {
			logx.W(ctx, "HandleDocDiffTaskValidate ReName fail op:%v diffTask: %+v", op, diffTask)
			return errs.ErrHandleQaOperationReNameFail
		}

		diffTaskID, err := util.CheckReqBotBizIDUint64(ctx, diffTask.GetDocDiffTaskBizId())
		if err != nil {
			return err
		}
		diffIds = append(diffIds, diffTaskID)
		reqMap[diffTaskID] = diffTask
	}
	if len(diffIds) == 0 {
		return errs.ErrParams
	}
	runList, docBizMap, err := l.checkHandleDocDiffFailList(ctx, reqMap, corpID, robotID, corpBizId, robotBizId, diffIds,
		rsp)
	if err != nil {
		logx.W(ctx, "检查对比任务文档是否可以处理 err: %+v", err)
		return err
	}
	logx.I(ctx, "checkHandleDocDiffDoc runList:%+v rsp:%v", runList, rsp)
	for _, diff := range runList {
		reqDiff, ok := reqMap[diff.BusinessID]
		if !ok {
			logx.W(ctx, "checkHandleDocDiffDoc reqMap taskID:%d, 不存在", diff.BusinessID)
			addToFailList(ctx, rsp, diff.BusinessID, errs.ErrHandleDocDiffNotFound)
			continue
		}
		// 更新任务状态
		updateTaskStatus(diff, reqDiff)
		// 处理重命名操作
		if err := handleRenameOperation(ctx, reqDiff, diff, docBizMap); err != nil {
			addToFailList(ctx, rsp, diff.BusinessID, err)
			continue
		}
		segmentToQAType := docEntity.GetDocDiffSegmentToQAType(docEntity.QAOperation(diff.QaOperation))
		// 准备操作参数
		params := prepareOperationParams(ctx, staffID, corpID, robotID, corpBizId, robotBizId, diff, reqDiff)
		if segmentToQAType != docEntity.SegmentToQATypeNone {
			targetDocIBizID := diff.NewDocBizID
			if segmentToQAType == docEntity.SegmentToQATypeOldUni || segmentToQAType == docEntity.SegmentToQATypeOldAll {
				targetDocIBizID = diff.OldDocBizID
			}

			logx.I(ctx, "targetDocIBizID %+v", targetDocIBizID)
			targetDoc, err := l.docLogic.GetDocByBizID(ctx, targetDocIBizID, robotID)
			if err != nil {
				return err
			}

			logx.I(ctx, "task doc is %+v", targetDoc)
			qaTask := &qaEntity.DocQATask{
				CorpID:   corpID,
				RobotID:  robotID,
				SourceID: diff.BusinessID,
			}
			logx.I(ctx, "qaTask %+v", qaTask)

			docQATaskID, err := l.CreateDocQATaskRecord(ctx, qaTask, targetDoc)
			if err != nil {
				return err
			}
			logx.I(ctx, "docQATaskID %v", docQATaskID)
			diff.DocQATaskID = docQATaskID
			params.DocQATaskID = docQATaskID
			params.QaTaskType = qaEntity.DocQATaskStatusGenerating
		}

		taskID, err := scheduler.NewDocDiffOperationTask(ctx, params)
		if err != nil {
			return err
		}
		diff.TaskID = uint64(taskID)

		updateColumns := []string{
			docEntity.DocDiffTaskTblColTaskId,
			docEntity.DocDiffTaskTblDocQATaskId,
			docEntity.DocDiffTaskTblColDiffType,
			docEntity.DocDiffTaskTblColDocOperation,
			docEntity.DocDiffTaskTblColQaOperation,
			docEntity.DocDiffTaskTblColStatus,
			docEntity.DocDiffTaskTblColNewDocRename,
			docEntity.DocDiffTaskTblColOldDocRename,
		}
		err = l.docLogic.GetDao().UpdateDocDiffTasks(ctx, updateColumns, corpBizId, robotBizId,
			[]uint64{diff.BusinessID}, diff)
		if err != nil {
			logx.E(ctx, "处理对比任务失败 UpdateDocDiffTasks err: %+v", err)
			return err
		}
		oldDoc, ok := docBizMap[diff.OldDocBizID]
		if !ok {
			logx.W(ctx, "checkHandleDocDiffFailList 文档不存在 newDocBizID:%d, oldDocBizID:%d",
				diff.NewDocBizID, diff.OldDocBizID)
			addToFailList(ctx, rsp, diff.BusinessID, errs.ErrHandleDocDiffTaskDocNotFoundFail)
			continue
		}
		newDoc, ok := docBizMap[diff.NewDocBizID]
		if !ok {
			logx.W(ctx, "checkHandleDocDiffFailList 文档不存在 newDocBizID:%d, oldDocBizID:%d",
				diff.NewDocBizID, diff.OldDocBizID)
			addToFailList(ctx, rsp, diff.BusinessID, errs.ErrHandleDocDiffTaskDocNotFoundFail)
			continue
		}

		updateDocFilter := &docEntity.DocFilter{
			IDs:     []uint64{oldDoc.ID, newDoc.ID},
			CorpId:  corpID,
			RobotId: robotID,
		}
		doc := docEntity.Doc{}
		doc.AddProcessingFlag([]uint64{docEntity.DocProcessingFlagHandlingDocDiffTask})
		updateDocColumns := []string{
			docEntity.DocTblColProcessingFlag}
		_, err = l.docLogic.GetDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, &doc)
		if err != nil {
			logx.E(ctx, "更新对比任务文档状态失败 UpdateDocDiffTasks err: %+v", err)
			return err
		}

		rsp.SuccessTotal++
		err = l.CreateDocDiffTaskRunningNotice(ctx, staffID, corpID, robotID, diff.BusinessID, newDoc.GetRealFileName(),
			oldDoc.GetRealFileName(), true)
		if err != nil {
			return err
		}
	}
	return nil
}

// updateTaskStatus 更新任务状态
func updateTaskStatus(diff *docEntity.DocDiff, reqDiff *pb.HandleDocDiffTaskReq_DocDiffTask) {
	diff.DiffType = reqDiff.GetDiffType()
	diff.DocOperation = reqDiff.GetDocOperation()
	diff.QaOperation = reqDiff.GetQaOperation()
	diff.Status = uint32(docEntity.DocDiffTaskStatusProcessing)
}

// prepareOperationParams 准备操作参数
func prepareOperationParams(ctx context.Context, staffID, corpID, robotID, corpBizId, robotBizId uint64,
	diff *docEntity.DocDiff, reqDiff *pb.HandleDocDiffTaskReq_DocDiffTask) *entity.DocDiffOperationParams {
	return &entity.DocDiffOperationParams{
		StaffID:      staffID,
		CorpID:       corpID,
		CorpBizID:    corpBizId,
		RobotID:      robotID,
		RobotBizID:   robotBizId,
		NewDocBizID:  diff.NewDocBizID,
		OldDocBizID:  diff.OldDocBizID,
		DocOperation: docEntity.DocOperation(diff.DocOperation),
		QAOperation:  docEntity.QAOperation(diff.QaOperation),
		Uin:          contextx.Metadata(ctx).Uin(),
		Sid:          contextx.Metadata(ctx).SID(),
		DocDiffID:    diff.BusinessID,
		NewName:      reqDiff.GetDocRename(),
	}
}

// handleRenameOperation 处理重命名操作
func handleRenameOperation(ctx context.Context, reqDiff *pb.HandleDocDiffTaskReq_DocDiffTask,
	diff *docEntity.DocDiff, docBizMap map[uint64]*docEntity.Doc) error {
	var reNameDocBizID uint64
	switch reqDiff.DocOperation {
	case uint32(docEntity.DocOperationOldReName):
		reNameDocBizID = diff.OldDocBizID
	case uint32(docEntity.DocOperationNewReName):
		reNameDocBizID = diff.NewDocBizID

	}
	if reNameDocBizID > 0 {
		reNameDoc, ok := docBizMap[reNameDocBizID]
		if !ok {
			logx.W(ctx, "handleRenameOperation 文档:%d, 不存在", reNameDocBizID)
			return errs.ErrHandleDocDiffTaskDocNotFoundFail
		}
		diff.OldDocRename = reNameDoc.FileName
		diff.NewDocRename = reqDiff.GetDocRename()
	}
	return nil
}

// checkHandleDocDiffFailList 检验批量对比任务是否可以处理,返回处理失败的对比任务和可以执行的任务
func (l *Logic) checkHandleDocDiffFailList(ctx context.Context, req map[uint64]*pb.HandleDocDiffTaskReq_DocDiffTask, corpID,
	robotID, corpBizId, robotBizId uint64, diffIds []uint64,
	rsp *pb.HandleDocDiffTaskRsp) ([]*docEntity.DocDiff, map[uint64]*docEntity.Doc, error) {
	// 初始化返回列表和过滤器
	var runList []*docEntity.DocDiff
	filter := &docEntity.DocDiffTaskFilter{
		BusinessIds: diffIds,
		CorpBizId:   corpBizId,
		RobotBizId:  robotBizId,
		IsDeleted:   ptrx.Bool(false),
		Limit:       len(diffIds),
	}
	// 获取文档对比任务列表
	list, err := l.docLogic.GetDao().GetDocDiffTaskList(ctx, docEntity.DocDiffTaskTblColList, filter)
	if err != nil {
		logx.E(ctx, "获取文档对比任务列表 GetDocDiffTaskList err: %+v", err)
		return nil, nil, err
	}
	if len(list) == 0 || len(list) != len(diffIds) {
		logx.I(ctx, "checkHandleDocDiffFailList total fail len(list):%d|  len(diffIds):%d",
			len(list), len(diffIds))
		return nil, nil, errs.ErrHandleDocDiffNotFound
	}

	// 收集所有文档业务ID
	docBizIDs := make([]uint64, 0)
	for _, diffTask := range list {
		docBizIDs = append(docBizIDs, diffTask.NewDocBizID, diffTask.OldDocBizID)
	}

	// 获取相关文档映射信息
	docDiffTaskMap, docBizMap, docQATaskMap, err := l.getDocDiffGeneratingDocMaps(ctx, corpID, robotID, corpBizId,
		robotBizId, docBizIDs)
	if err != nil {
		logx.E(ctx, "getDocDiffGeneratingDocMaps err: %+v", err)
		return nil, nil, err
	}

	uniqueDocMap := make(map[uint64]bool, len(docBizIDs))
	for _, diffTask := range list {
		// 检查文档是否存在
		diffTaskNewDoc := docBizMap[diffTask.NewDocBizID]
		diffTaskOldDoc := docBizMap[diffTask.OldDocBizID]
		if diffTaskNewDoc == nil || diffTaskOldDoc == nil {
			logx.W(ctx, "checkHandleDocDiffFailList 文档不存在 newDocBizID:%d, oldDocBizID:%d",
				diffTask.NewDocBizID, diffTask.OldDocBizID)
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrHandleDocDiffTaskDocNotFoundFail)
			continue
		}
		// 检查同批次是否有重复文档任务
		if uniqueDocMap[diffTask.NewDocBizID] || uniqueDocMap[diffTask.OldDocBizID] {
			logx.W(ctx, "发起对比任务中有相同文档同时发起不同任务 newDocBizID:%d, oldDocBizID:%d",
				diffTask.NewDocBizID, diffTask.OldDocBizID)
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrHandleDocDiffTaskInDiffFail)
			continue
		}
		// 检查是否有未完成的对比任务
		if isDiffTasking := checkDiffTaskRunning(ctx, docDiffTaskMap, diffTaskNewDoc.ID, diffTaskOldDoc.ID); isDiffTasking {
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrCreateDocDiffTaskInTaskFail)
			continue
		}
		// 检查任务状态
		if diffTask.Status != uint32(docEntity.DocDiffTaskStatusInit) {
			logx.W(ctx, "文档对比任务状态不正确 taskID:%d, status:%d",
				diffTask.TaskID, diffTask.Status)
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrHandleDocDiffTaskTypeFail)
			continue
		}
		// 检查文档状态
		if !isDocStatusValid(diffTaskNewDoc.Status) || !isDocStatusValid(diffTaskOldDoc.Status) {
			logx.W(ctx, "文档状态不可用发起对比 diffTaskID:%d, NewDocStatus:%d, OldDocStatus:%d",
				diffTask.TaskID, diffTaskNewDoc.Status, diffTaskOldDoc.Status)
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrHandleDocDiffTaskDocTypeFail)
			continue
		}
		// 检查是否有未完成的问答任务
		if isQaGenerating := checkQATaskRunning(ctx, docQATaskMap, diffTaskNewDoc.ID, diffTaskOldDoc.ID); isQaGenerating {
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrCreateDocDiffTaskInQaFail)
			continue
		}
		// 处理文档重命名逻辑
		if err := handleDocRenameCheck(ctx, req, diffTask, docBizMap, rsp); err != nil {
			continue
		}
		// 标记文档已使用并添加到执行列表
		uniqueDocMap[diffTask.NewDocBizID] = true
		uniqueDocMap[diffTask.OldDocBizID] = true
		runList = append(runList, diffTask)
	}
	return runList, docBizMap, nil
}

// addToFailList 将失败任务添加到失败列表
func addToFailList(ctx context.Context, rsp *pb.HandleDocDiffTaskRsp, businessID uint64, err error) {
	rsp.FailList = append(rsp.FailList, &pb.HandleDocDiffTaskRsp_FailDocDiffTask{
		DocDiffTaskBizId: strconv.FormatUint(businessID, 10),
		ErrMsg:           errx.Msg(i18n.TranslateErr(ctx, err)),
	})
}

// handleDocRenameCheck 处理文档重命名检查逻辑
func handleDocRenameCheck(ctx context.Context, req map[uint64]*pb.HandleDocDiffTaskReq_DocDiffTask,
	diffTask *docEntity.DocDiff, docBizMap map[uint64]*docEntity.Doc, rsp *pb.HandleDocDiffTaskRsp) error {
	var reNameDocBizID uint64
	switch req[diffTask.BusinessID].DocOperation {
	case uint32(docEntity.DocOperationOldReName):
		reNameDocBizID = diffTask.OldDocBizID
	case uint32(docEntity.DocOperationNewReName):
		reNameDocBizID = diffTask.NewDocBizID
	}
	if reNameDocBizID > 0 {
		reNameDoc, ok := docBizMap[reNameDocBizID]
		if !ok {
			logx.W(ctx, "checkHandleDocDiffDoc 文档:%d, 不存在", reNameDocBizID)
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrDocNotFound)
			return errs.ErrDocNotFound
		}
		if req[diffTask.BusinessID].GetDocRename() == reNameDoc.GetRealFileName() {
			logx.W(ctx, "HandleDocDiffTaskValidate DocRename 与之前名称相同 docBizID:%d",
				reNameDoc.BusinessID)
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrDocNameNotChanged)
			return errs.ErrDocNameNotChanged
		}
		if filepath.Ext(reNameDoc.GetRealFileName()) != filepath.Ext(req[diffTask.BusinessID].GetDocRename()) {
			logx.W(ctx, "文档重命名失败, 文档名称后缀不一致, 原文档名: %+v, 新文档名: %+v",
				reNameDoc.GetRealFileName(), req[diffTask.BusinessID].GetDocRename())
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrDocNameExtNotMatch)
			return errs.ErrDocNameExtNotMatch
		}
		if util.FileNameNoSuffix(req[diffTask.BusinessID].GetDocRename()) == "" {
			logx.W(ctx, "文档重命名失败, 文档名称是空的, 原文档名: %+v, 新文档名: %+v",
				reNameDoc.GetRealFileName(), reNameDoc.GetRealFileName())
			addToFailList(ctx, rsp, diffTask.BusinessID, errs.ErrDocNameVerifyFailed)
			return errs.ErrDocNameVerifyFailed
		}
	}
	return nil
}

// isDocStatusValid 检查文档状态是否有效
func isDocStatusValid(status uint32) bool {
	return status == docEntity.DocStatusWaitRelease || status == docEntity.DocStatusReleaseSuccess
}

// checkDiffTaskRunning 检查是否有未完成的对比任务
func checkDiffTaskRunning(ctx context.Context, docDiffTaskMap map[uint64]*docEntity.DocDiff, newDocID, oldDocID uint64) bool {
	if task, ok := docDiffTaskMap[newDocID]; ok {
		logx.I(ctx, "文档:%d 有未完成的对比任务 taskID:%d", newDocID, task.TaskID)
		return true
	}
	if task, ok := docDiffTaskMap[oldDocID]; ok {
		logx.I(ctx, "文档:%d 有未完成的对比任务 taskID:%d", oldDocID, task.TaskID)
		return true
	}
	return false
}

// checkQATaskRunning 检查是否有未完成的问答任务
func checkQATaskRunning(ctx context.Context, docQATaskMap map[uint64]*qaEntity.DocQATask, newDocID, oldDocID uint64) bool {
	if task, ok := docQATaskMap[newDocID]; ok {
		logx.I(ctx, "文档:%d 有未完成的问答任务 taskID:%d", newDocID, task.TaskID)
		return true
	}
	if task, ok := docQATaskMap[oldDocID]; ok {
		logx.I(ctx, "文档:%d 有未完成的问答任务 taskID:%d", oldDocID, task.TaskID)
		return true
	}
	return false
}

// CreateDocDiffTaskRunningNotice 处理diff任务,通知中心处理
func (l *Logic) CreateDocDiffTaskRunningNotice(ctx context.Context, staffID, corpID, robotID, diffID uint64,
	newDocName, OldFileName string, create bool) error {
	var operations []releaseEntity.Operation
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
	}
	noticeFileName := fmt.Sprintf("【%s】", newDocName)
	if newDocName != OldFileName {
		noticeFileName = i18n.Translate(ctx, `【"%s"与"%s"】`, newDocName, OldFileName)
	}
	if create {
		// 创建执行中通知
		noticeOptions = append(noticeOptions,
			releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentComparisonTaskInProgressWithParam, noticeFileName)),
			releaseEntity.WithForbidCloseFlag(),
			releaseEntity.WithLevel(releaseEntity.LevelInfo))
		operations = append(operations, releaseEntity.Operation{Type: releaseEntity.OpTypeDocDiffRunning})
	} else {
		// 完成通知
		noticeOptions = append(noticeOptions,
			releaseEntity.WithGlobalFlag(),
			releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentComparisonTaskComplete)),
			releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentComparisonTaskCompleteWithParam, noticeFileName)),
			releaseEntity.WithLevel(releaseEntity.LevelSuccess))
		operations = append(operations, releaseEntity.Operation{Type: releaseEntity.OpTypeDocDiffFinish})
	}

	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocDiffTask, diffID, corpID, robotID, staffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 err:%+v", err)
		return err
	}

	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "CreateNotice err err:%+v", err)
		return err
	}
	return nil
}

// InvalidDocDiffTask 设置对比任务为无效
func (l *Logic) InvalidDocDiffTask(ctx context.Context, corpBizId, robotBizId uint64, docBusinessID []uint64) error {
	filter := &docEntity.DocDiffTaskFilter{
		CorpBizId:     corpBizId,
		RobotBizId:    robotBizId,
		IsDeleted:     ptrx.Bool(false),
		Statuses:      []int32{docEntity.DocDiffTaskStatusInit},
		InNewOldDocId: docBusinessID,
		Limit:         docEntity.DocDiffTaskTableMaxPageSize,
	}
	docDiffList, err := l.docLogic.GetDao().GetDocDiffTaskList(ctx, docEntity.DocDiffTaskTblColList, filter)
	if err != nil {
		logx.E(ctx, "获取是否已有文档在对比任务待处理中 GetDocDiffTaskList err: %+v", err)
		return err
	}
	if len(docDiffList) == 0 {
		logx.I(ctx, "InvalidDocDiffTask is null")
		return nil
	}
	for _, diff := range docDiffList {
		diff.Status = uint32(docEntity.DocDiffTaskStatusExceeded)
		updateColumns := []string{
			docEntity.DocDiffTaskTblColStatus}
		err = l.docLogic.GetDao().UpdateDocDiffTasks(ctx, updateColumns, corpBizId, robotBizId,
			[]uint64{diff.BusinessID}, diff)
		if err != nil {
			logx.E(ctx, "设置对比任务为无效 UpdateDocDiffTasks err: %+v", err)
			return err
		}
	}
	return nil
}

// ContinueDocDiffTask 继续文档对比任务
func (l *Logic) ContinueDocDiffTask(ctx context.Context, docDiffID, corpID, robotID, robotBizId, docQaTaskBizID uint64) error {
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	// corp, err := l.rawSqlDao.GetCorpByID(ctx, corpID)
	if err != nil {
		return err
	}

	docDiff, err := l.docLogic.GetDao().GetDocDiffTask(ctx, docEntity.DocDiffTaskTblColList, corp.GetCorpId(), robotBizId, docDiffID)
	if err != nil {
		return err
	}
	// 校验是否能够继续，文档是否处于其他任务中
	_, err = l.checkHandleDocDiffDoc(ctx, corpID, robotID, corp.GetCorpId(), robotBizId,
		[]uint64{docDiff.OldDocBizID, docDiff.NewDocBizID}, true)
	if err != nil {
		return err
	}

	// 1. 修改 doc diff的 TaskID status为processing
	update := &docEntity.DocDiff{
		DocOperationStatus: uint32(docEntity.DocDiffQAAndDocOpStatusProcessing),
		QaOperationStatus:  uint32(docEntity.DocDiffQAAndDocOpStatusProcessing),
		QaOperationResult:  "",
		Status:             uint32(docEntity.DocDiffTaskStatusProcessing),
	}

	updateColumns := []string{docEntity.DocDiffTaskTblColDocOperationStatus, docEntity.DocDiffTaskTblColQaOperationStatus,
		docEntity.DocDiffTaskTblColQaOperationResult, docEntity.DocDiffTaskTblColStatus}
	logx.I(ctx, "update task %v processing", docDiffID)
	err = l.docLogic.GetDao().UpdateDocDiffTasks(ctx, updateColumns, corp.GetCorpId(), robotBizId, []uint64{docDiff.BusinessID}, update)
	if err != nil {
		return err
	}

	// 2. 新建 doc diff operation的异步任务
	params := &entity.DocDiffOperationParams{
		StaffID:      docDiff.StaffBizID,
		CorpID:       corpID,
		CorpBizID:    corp.GetCorpId(),
		RobotID:      robotID,
		RobotBizID:   robotBizId,
		NewDocBizID:  docDiff.NewDocBizID,
		OldDocBizID:  docDiff.OldDocBizID,
		DocOperation: docEntity.DocOperation(docDiff.DocOperation),
		QAOperation:  docEntity.QAOperation(docDiff.QaOperation),
		Uin:          contextx.Metadata(ctx).Uin(),
		Sid:          contextx.Metadata(ctx).SID(),
		DocDiffID:    docDiff.BusinessID,
		NewName:      docDiff.NewDocRename,
		DocQATaskID:  docDiff.DocQATaskID,
		QaTaskType:   qaEntity.DocQATaskStatusContinue,
	}
	taskID, err := scheduler.NewDocDiffOperationTask(ctx, params)
	if err != nil {
		return err
	}
	update = &docEntity.DocDiff{
		TaskID: uint64(taskID),
	}
	updateColumns = []string{docEntity.DocDiffTaskTblColTaskId}
	err = l.docLogic.GetDao().UpdateDocDiffTasks(ctx, updateColumns, corp.GetCorpId(), robotBizId, []uint64{docDiff.BusinessID}, update)
	if err != nil {
		return err
	}

	// 3. 修改 doc qa status的状态为 生成中
	filter := &qaEntity.DocQaTaskFilter{
		BusinessId: docQaTaskBizID,
		CorpId:     corpID,
		RobotId:    robotID,
	}
	docQaTaskUpdate := &qaEntity.DocQATask{
		Status:  qaEntity.DocQATaskStatusGenerating,
		Message: "",
	}
	qaTaskColumns := []string{qaEntity.DocQaTaskTblColStatus, qaEntity.DocQaTaskTblColMessage}
	err = l.qaLogic.GetDao().UpdateDocQATasks(ctx, qaTaskColumns, filter, docQaTaskUpdate, nil)
	if err != nil {
		return err
	}
	logx.I(ctx, "continue qa task success, update qa task status Generating")

	return nil
}

// deleteDocDiffTasksAndData 【事务】删除对比任务和对比结果详情
func (l *Logic) deleteDocDiffTasksAndData(ctx context.Context, corpBizId uint64, robotBizId uint64,
	businessIds []uint64) error {
	tbl := l.docLogic.GetDao().TdsqlQuery()
	if err := tbl.Transaction(func(tx *tdsqlquery.Query) error {
		if err := l.docLogic.GetDao().DeleteDocDiffTasks(ctx, corpBizId, robotBizId, businessIds); err != nil {
			return err
		}
		if err := l.docLogic.GetDao().DeleteDocDiffData(ctx, corpBizId, robotBizId, businessIds); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "DeleteDocDiffTasksAndData failed for diffBizIds: %+v, err: %+v",
			businessIds, err)
		return err
	}
	return nil
}

func (l *Logic) UpdateDocDiffTasks(ctx context.Context, updateColumns []string, corpBizID, appBizID uint64,
	diffID []uint64, updateDocDiff *docEntity.DocDiff) error {
	return l.docLogic.GetDao().UpdateDocDiffTasks(ctx, updateColumns, corpBizID, appBizID, diffID, updateDocDiff)
}

func (l *Logic) GetDocDiffTaskCountAndList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffTaskFilter) ([]*docEntity.DocDiff, int64, error) {
	return l.docLogic.GetDao().GetDocDiffTaskCountAndList(ctx, selectColumns, filter)
}

func (l *Logic) GetDocDiffTask(ctx context.Context, selectColumns []string,
	corpBizId uint64, robotBizId uint64, businessId uint64) (*docEntity.DocDiff, error) {
	return l.docLogic.GetDao().GetDocDiffTask(ctx, selectColumns, corpBizId, robotBizId, businessId)
}
