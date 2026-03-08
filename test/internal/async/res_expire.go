package async

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	financeEntity "git.woa.com/adp/kb/kb-config/internal/entity/finance"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	pc "git.woa.com/adp/pb-go/platform/platform_charger"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

// ResExpireTaskHandler 资源包到期后处理离线任务
type ResExpireTaskHandler struct {
	*taskCommon

	task            task_scheduler.Task
	p               entity.ResExpireParams
	hasDocNotStable bool
	hasQANotStable  bool
}

func registerResExpireTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.ResourceExpireTask,
		func(t task_scheduler.Task, params entity.ResExpireParams) task_scheduler.TaskHandler {
			return &ResExpireTaskHandler{
				taskCommon:      tc,
				task:            t,
				p:               params,
				hasDocNotStable: false,
				hasQANotStable:  false,
			}
		},
	)
}

// Prepare 数据准备
func (d *ResExpireTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(ResourceExpire) Prepare, task: %+v, params: %+v", d.task, d.p)
	// TODO: ...
	return task_scheduler.TaskKV{}, nil
}

// Init 初始化
func (d *ResExpireTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *ResExpireTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(ResourceExpire) Process, task: %+v, params: %+v", d.task, d.p)
	corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, d.p.CorpID)
	if err != nil {
		logx.E(ctx, "task(ResourceExpire) Process, task: %+v, DescribeCorpByPrimaryId err: %+v", d.task.ID, err)
		return err
	}

	// 调用DescribeAccountQuota获取配额信息，判断新老用户
	quotaRsp, err := d.rpc.Finance.DescribeAccountQuota(ctx, corp.GetUin(), corp.GetSid())
	if err != nil {
		logx.E(ctx, "task(ResourceExpire) Process, task: %+v, DescribeAccountQuota failed, uin:%s, sid:%d, err:%+v",
			d.task.ID, corp.GetUin(), corp.GetSid(), err)
		return err
	}
	if quotaRsp == nil {
		logx.E(ctx, "GetKnowledgeBaseUsage quotaRsp is nil")
		return errs.ErrSystem
	}

	var exceededCount uint64
	var isPurchasePackage bool
	exceededCount, isPurchasePackage, err = d.checkExceeded(ctx, corp, quotaRsp)
	if err == nil {
		logx.I(ctx, "task(ResourceExpire) Process, task: %+v, not exceeded", d.task.ID)
		return nil
	}
	if !errors.Is(err, errs.ErrOverCharacterSizeLimit) {
		// 非超量的情况不处理
		logx.E(ctx, "task(ResourceExpire) Process, task: %+v, not Exceeded, err: %+v", d.task.ID, err)
		return err
	}

	logx.D(ctx, "task(ResourceExpire) Process, task: %+v, Exceeded, exceededCount: %+v", d.task.ID, exceededCount)

	listAppBaseInfoReq := appconfig.ListAppBaseInfoReq{CorpPrimaryId: d.p.CorpID}
	botList, _, err := d.rpc.AppAdmin.ListAllAppBaseInfo(ctx, &listAppBaseInfoReq)
	if err != nil {
		logx.E(ctx, "task(ResourceExpire) Process, task: %+v, ListAllAppBaseInfo err: %+v", d.task.ID, err)
		return err
	}

	logx.D(ctx, "task(ResourceExpire) Process, task: %+v, app count: %+v", d.task.ID, len(botList))
	for _, bot := range botList {
		if d.p.IsDebug {
			// TODO: 调试逻辑, 用于针对指定应用进行超量隔离
			appID := fmt.Sprintf("%d", bot.BizId)
			if d.p.ResourceID != appID {
				continue
			}
		}
		newCtx := util.SetMultipleMetaData(ctx, bot.SpaceId, bot.Uin)
		logx.D(ctx, "task(ResourceExpire) Process, task: %+v, appID: %+v", d.task.ID, bot.BizId)

		// 重新检查是否超量
		exceededCount, isPurchasePackage, err = d.checkExceeded(newCtx, corp, quotaRsp)

		if !errors.Is(err, errs.ErrOverCharacterSizeLimit) {
			// 删除到可用总量以下
			return err
		}
		logx.D(ctx, "task(ResourceExpire) Process, task: %+v, botID: %+v, exceededCount: %+v, isPurchasePackage: %v", d.task.ID, bot.BizId, exceededCount, isPurchasePackage)
		if _, err := d.markBotExceeded(newCtx, bot, exceededCount, isPurchasePackage); err != nil {
			return err
		}
	}
	if d.hasDocNotStable {
		return errs.ErrDocNotStable
	}
	if d.hasQANotStable {
		return errs.ErrQANotStable
	}
	return nil
}

func (d *ResExpireTaskHandler) checkExceeded(ctx context.Context, corp *pm.DescribeCorpRsp, quotaRsp *pc.DescribeAccountQuotaRsp) (uint64, bool, error) {
	if quotaRsp != nil && quotaRsp.GetIsPackageScene() {
		checkRsp, err := d.financeLogic.CheckNewUserQuota(ctx, financeEntity.CheckQuotaReq{}, corp, quotaRsp)
		if err == nil && checkRsp.Status == financeEntity.QuotaStatusExceeded {
			err = errs.ErrOverCharacterSizeLimit
		} else if checkRsp.Status == financeEntity.QuotaStatusTolerated { // 超量但有资源允许继续使用，需要做超量上报
			err = d.rpc.PlatformApi.ModifyCorpKnowledgeOverCapacity(ctx, corp.CorpId, entity.CapacityUsage{
				KnowledgeCapacity: int64(checkRsp.KnowledgeCapacityExceeded),
				StorageCapacity:   int64(checkRsp.StorageCapacityExceeded),
				ComputeCapacity:   int64(checkRsp.ComputeCapacityExceeded),
			})
			if err != nil {
				logx.E(ctx, "checkExceeded ModifyCorpKnowledgeOverCapacity failed, corpBizID:%d, err:%+v", corp.CorpId, err)
			}
		}
		return checkRsp.KnowledgeCapacityExceeded, true, err
	}
	count, err := d.checkOldUserCharSizeExceeded(ctx, corp.GetCorpId(), corp.GetSid(), corp.GetUin())
	return count, false, err
}

// getBotDocList 机器人下的文档
func (d *ResExpireTaskHandler) getBotDocList(ctx context.Context, bot *entity.AppBaseInfo) ([]*docEntity.Doc, error) {
	pageSize := 1000
	page := 1
	docs := make([]*docEntity.Doc, 0)
	for {
		offset, limit := utilx.Page(page, pageSize)
		docFilter := &docEntity.DocFilter{
			CorpId:  bot.CorpPrimaryId,
			RobotId: bot.PrimaryId,
			Offset:  offset,
			Limit:   limit,
		}

		list, err := d.docLogic.GetDocList(ctx, docEntity.DocTblColList, docFilter)
		if err != nil {
			return nil, err
		}
		if len(list) == 0 {
			break
		}
		docs = append(docs, list...)
		page++
	}
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].CreateTime.After(docs[j].CreateTime)
	})
	return docs, nil
}

// getBotQAList 机器人下的问答(不含文档生成的问答)
func getBotQAList(ctx context.Context, corpID uint64, botID uint64, docID uint64, qaLogic *qa.Logic) ([]*qaEntity.DocQA, error) {
	pageSize := uint32(1000)
	page := uint32(1)
	qas := make([]*qaEntity.DocQA, 0)
	for {
		req := &qaEntity.QAListReq{
			CorpID:    corpID,
			RobotID:   botID,
			DocID:     []uint64{docID},
			IsDeleted: qaEntity.QAIsNotDeleted,
			Page:      page,
			PageSize:  pageSize,
		}
		list, err := qaLogic.GetQAList(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(list) == 0 {
			break
		}
		qas = append(qas, list...)
		page++
	}
	sort.Slice(qas, func(i, j int) bool {
		return qas[i].CreateTime.After(qas[j].CreateTime)
	})
	return qas, nil
}

func genRelatedID(botID uint64, pageID uint32) uint64 {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%d_%d", botID, pageID)))
	v := int64(h.Sum64())
	if v < 0 {
		return uint64(-v)
	}
	return uint64(v)
}

func (d *ResExpireTaskHandler) charExceededNotice(ctx context.Context, bot *entity.AppBaseInfo, page uint32, opType uint32,
	global bool) error {
	logx.D(ctx, "task(ResourceExpire) charExceededNotice, botID: %+v", bot.PrimaryId)
	operations := []releaseEntity.Operation{{Type: opType, Params: releaseEntity.OpParams{}}}
	corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, bot.CorpPrimaryId)
	// corp, err := d.dao.GetCorpByID(ctx, bot.GetCorpPrimaryId())
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
		releaseEntity.WithPageID(page),
		releaseEntity.WithLevel(releaseEntity.LevelWarning),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficient)),
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficientKnowledgeExpired)),
		// model.WithForbidCloseFlag(),
	}
	if global {
		noticeOptions = append(noticeOptions, releaseEntity.WithGlobalFlag())
	}
	// 给所有用户对这个robot都发通知
	// staffs, err := d.dao.GetStaffByCorpID(ctx, d.p.CorpPrimaryId, 1, 1000)
	req := pm.DescribeCorpStaffListReq{
		Status:        []uint32{entity.CorpStatusValid},
		Page:          1,
		PageSize:      1000,
		CorpPrimaryId: d.p.CorpID,
	}
	staffs, _, err := d.rpc.PlatformAdmin.DescribeCorpStaffList(ctx, &req)
	if err != nil {
		logx.E(ctx, "task(ResourceExpire) Process, GetStaffByCorpID:%+v err:%+v", d.p.CorpID, err)
		return err
	}
	noticeType := releaseEntity.NoticeTypeDocCharExceeded
	if page == releaseEntity.OpTypeViewQACharExceeded {
		noticeType = releaseEntity.NoticeTypeQACharExceeded
	}
	for _, staff := range staffs {
		notice := releaseEntity.NewNotice(noticeType, genRelatedID(bot.PrimaryId, page), d.p.CorpID, bot.PrimaryId, staff.ID, noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			logx.E(ctx, "task(ResourceExpire) Process, 序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		logx.D(ctx, "task(ResourceExpire) Process, CreateNotice notice: %+v", notice)
		if err := d.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
			logx.E(ctx, "task(ResourceExpire) Process, CreateNotice notice: %+v err: %+v", notice, err)
			return err
		}
	}
	return nil
}

func (d *ResExpireTaskHandler) markBotExceeded(ctx context.Context, bot *entity.AppBaseInfo,
	exceededCount uint64, useCapacity bool) (uint64, error) {
	defer d.taskCommon.updateAppCharSize(ctx, bot.PrimaryId, bot.CorpPrimaryId)
	hasExceeded := false
	opType := releaseEntity.OpTypeViewDocCharExceeded
	docHasExceeded, exceededCount, err := d.markDocExceeded(ctx, bot, exceededCount, useCapacity)
	if err != nil {
		return 0, err
	}
	qaHasExceeded, exceededCount, err := d.markQAExceeded(ctx, bot, exceededCount, useCapacity)
	if err != nil {
		return 0, err
	}
	if docHasExceeded || qaHasExceeded {
		hasExceeded = true
	}
	if (!docHasExceeded) && qaHasExceeded {
		opType = releaseEntity.OpTypeViewQACharExceeded
	}
	if !hasExceeded {
		return exceededCount, nil
	}
	if err := d.charExceededNotice(ctx, bot, releaseEntity.NoticeRobotInfoPageID, opType, true); err != nil {
		logx.W(ctx, "task(ResourceExpire) Process task: %+v, bot: %+v, charExceededNotice err: %+v",
			d.task.ID, bot.PrimaryId, err)
		return exceededCount, err
	}
	return exceededCount, nil
}

func (d *ResExpireTaskHandler) updateDocStatusExceeded(ctx context.Context, doc *docEntity.Doc) error {
	switch doc.Status {
	case docEntity.DocStatusParseImportFail:
		doc.Status = docEntity.DocStatusParseImportFailCharExceeded
	case docEntity.DocStatusAuditFail:
		doc.Status = docEntity.DocStatusAuditFailCharExceeded
	case docEntity.DocStatusUpdateFail:
		doc.Status = docEntity.DocStatusUpdateFailCharExceeded
	case docEntity.DocStatusCreateIndexFail:
		doc.Status = docEntity.DocStatusCreateIndexFailCharExceeded
	case docEntity.DocStatusExpired:
		doc.Status = docEntity.DocStatusExpiredCharExceeded
	case docEntity.DocStatusAppealFailed:
		doc.Status = docEntity.DocStatusAppealFailedCharExceeded
	default:
		doc.Status = docEntity.DocStatusCharExceeded
	}
	if err := d.docLogic.UpdateDocStatus(ctx, doc); err != nil {
		logx.D(ctx,
			"task(ResourceExpire) Process, task: %+v, bot: %+v, docID: %+v, docName: %+v, UpdateDocStatus err: %+v",
			d.task.ID, doc.RobotID, doc.ID, doc.FileName, err)
		return err
	}
	return nil
}

func (d *ResExpireTaskHandler) deleteDocSegments(ctx context.Context, bot *entity.AppBaseInfo, doc *docEntity.Doc) error {
	pageSize := uint32(50)
	segLen := pageSize
	for segLen > 0 {
		segs, err := d.segLogic.GetSegmentList(ctx, d.p.CorpID, doc.ID, 1, pageSize, bot.PrimaryId)
		if err != nil {
			logx.E(ctx, "task(ResourceExpire) Process, task: %+v, GetSegmentList err: %+v", d.task.ID, err)
			return err
		}

		segLen = uint32(len(segs))
		if segLen == 0 {
			break
		}
		data := make([]*retrieval.KnowledgeIDType, 0, len(segs))
		dataAll := make([]*retrieval.KnowledgeIDType, 0, len(segs))
		for _, seg := range segs {
			if !seg.IsSegmentForQA() && !seg.IsText2sqlSegmentType() {
				// 评测端的, Text2SQL在DeleteText2SQL中删除
				data = append(data, &retrieval.KnowledgeIDType{
					Id:          seg.ID,
					SegmentType: seg.SegmentType,
				})
			}
			//
			dataAll = append(dataAll, &retrieval.KnowledgeIDType{
				Id:          seg.ID,
				SegmentType: seg.SegmentType,
			})
		}
		if len(data) != 0 {
			if err := d.deleteSandboxKnowledge(ctx, bot, doc, data); err != nil {
				logx.E(ctx, "task(ResourceExpire) Process task: %+v deleteSandboxKnowledge err:%+v",
					d.task.ID, err)
				return err
			}
		}
		if len(dataAll) != 0 {
			if err := d.deleteProdKnowledge(ctx, bot, doc, dataAll); err != nil {
				logx.E(ctx, "task(ResourceExpire) Process task: %+v deleteProdKnowledge err:%+v", d.task.ID,
					err)
				return err
			}
		}
		if err := d.segLogic.BatchDeleteSegments(ctx, segs, bot.PrimaryId); err != nil {
			logx.E(ctx, "task(ResourceExpire) Process, task: %+v, DeleteSegment err: %+v", d.task.ID, err)
			return err
		}
		// 实际在按页删除的过程中已经将page数减少了
		// page++
	}
	req := retrieval.DeleteBigDataElasticReq{
		RobotId:    doc.RobotID,
		DocId:      doc.ID,
		Type:       retrieval.KnowledgeType_KNOWLEDGE,
		HardDelete: false,
	}
	return d.rpc.RetrievalDirectIndex.DeleteBigDataElastic(ctx, &req)
}

func (d *ResExpireTaskHandler) markDocExceeded(ctx context.Context, bot *entity.AppBaseInfo, exceededCount uint64, useCapacity bool) (bool,
	uint64, error) {
	hasExceeded := false
	docs, err := d.getBotDocList(ctx, bot)
	if err != nil {
		return false, 0, err
	}
	logx.D(ctx, "task(ResourceExpire) Process, task: %+v, bot: %+v, doc count: %+v", d.task.ID, bot.PrimaryId, len(docs))
	usedSize := uint64(0)
	for _, doc := range docs {
		var docSize uint64
		if useCapacity {
			docSize = doc.FileSize
		} else {
			docSize = doc.CharSize
		}
		logx.D(ctx,
			"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, Status: %+v, Size: %+v, Exceeded: %+v",
			d.task.ID, bot.PrimaryId, doc.FileName, doc.StatusDesc(false), docSize, doc.IsCharSizeExceeded())
		if doc.Status == docEntity.DocStatusUnderAppeal || doc.Status == docEntity.DocStatusAuditIng {
			logx.D(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, Status: %+v "+
					"is not stable, skip it", d.task.ID, bot.PrimaryId, doc.FileName, doc.StatusDesc(false))
			continue
		}
		if !doc.IsStableStatus() {
			logx.D(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, Status: %+v "+
					"is not stable, skip it", d.task.ID, bot.PrimaryId, doc.FileName, doc.StatusDesc(false))
			d.hasDocNotStable = true
			continue
		}
		if doc.IsCharSizeExceeded() {
			continue
		}
		if doc.Status == docEntity.DocStatusParseFail {
			continue
		}
		usedSize += docSize
		docQASize, err := d.updateDocQAStatusExceeded(ctx, bot, doc, useCapacity)
		if err != nil {
			return false, 0, err
		}
		usedSize += docQASize
		if err := d.deleteDocSegments(ctx, bot, doc); err != nil {
			logx.E(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, deleteDocSegments err: %+v",
				d.task.ID, bot.PrimaryId, doc.FileName, err)
			return false, 0, err
		}
		req := retrieval.DeleteText2SQLReq{
			RobotId:     doc.RobotID,
			DocId:       doc.ID,
			SegmentType: segEntity.SegmentTypeText2SQLContent,
		}
		if err = d.rpc.RetrievalDirectIndex.DeleteText2SQL(ctx, &req); err != nil {
			return false, 0, err
		}
		if err := d.updateDocStatusExceeded(ctx, doc); err != nil {
			logx.E(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docID: %+v, docName: %+v, UpdateDocStatus err: %+v",
				d.task.ID, bot.PrimaryId, doc.ID, doc.FileName, err)
			return false, 0, err
		}
		hasExceeded = true
		if usedSize > exceededCount {
			break
		}
	}
	if usedSize <= exceededCount {
		exceededCount = exceededCount - usedSize
	}
	if hasExceeded {
		if err := d.charExceededNotice(ctx, bot, releaseEntity.NoticeDocPageID, releaseEntity.OpTypeViewDocCharExceeded,
			false); err != nil {
			logx.W(ctx, "task(ResourceExpire) Process task: %+v, bot: %+v, charExceededNotice err: %+v",
				d.task.ID, bot.PrimaryId, err)
		}
	}
	return hasExceeded, exceededCount, nil
}

func (d *ResExpireTaskHandler) deleteQASandboxKnowledge(ctx context.Context, bot *entity.AppBaseInfo, qa *qaEntity.DocQA) error {
	logx.D(ctx, "task(ResourceExpire) deleteQASandboxKnowledge, qa: %+v", qa)
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, qa.RobotID)
	if err != nil {
		return err
	}
	embeddingVersion := appDB.Embedding.Version
	embeddingName, err :=
		d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

	if err != nil {
		logx.W(ctx, "task(ResourceExpire) GetKnowledgeEmbeddingModel err:%+v", err)
		return err
	}
	data := []*retrieval.KnowledgeIDType{
		{
			Id: qa.ID,
		},
	}
	// 支持相似问删除
	sims, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.W(ctx, "Failed to get similar questions: %+v, err: %+v", qa, err)
		return err
	}
	if len(sims) > 0 {
		for _, v := range sims {
			data = append(data, &retrieval.KnowledgeIDType{
				Id: v.SimilarID,
			})
		}
	}
	req := &retrieval.BatchDeleteKnowledgeReq{
		RobotId:            qa.RobotID,
		IndexId:            entity.ReviewVersionID,
		Data:               data,
		DocType:            entity.DocTypeQA,
		BotBizId:           bot.BizId,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
	}
	if err = d.rpc.RetrievalDirectIndex.BatchDeleteKnowledge(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireTaskHandler) deleteQAProdKnowledge(ctx context.Context, bot *entity.AppBaseInfo, qa *qaEntity.DocQA) error {
	if bot.QaVersion == 0 {
		// 应用未发布,可以跳过删除发布端数据的逻辑
		return nil
	}
	data := []*retrieval.KnowledgeIDType{
		{
			Id: qa.ID,
		},
	}
	// 支持相似问删除
	sims, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx, "获取qa的相似问失败: %+v, err: %+v", qa, err)
		return err
	}
	if len(sims) > 0 {
		for _, v := range sims {
			data = append(data, &retrieval.KnowledgeIDType{
				Id: v.SimilarID,
			})
		}
	}
	req := &retrieval.BatchDeleteAllKnowledgeProdReq{
		RobotId:   qa.RobotID,
		VersionId: bot.QaVersion,
		Data:      data,
		DocType:   entity.DocTypeQA,
		BotBizId:  bot.BizId,
		DocId:     qa.DocID,
	}
	if _, err := d.rpc.Retrieval.BatchDeleteAllKnowledgeProd(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireTaskHandler) deleteSandboxKnowledge(ctx context.Context, bot *entity.AppBaseInfo, doc *docEntity.Doc,
	data []*retrieval.KnowledgeIDType) error {
	logx.D(ctx, "task(ResourceExpire) deleteSandboxKnowledge, doc: %+v", doc)
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	embeddingVersion := appDB.Embedding.Version
	embeddingName, err :=
		d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

	if err != nil {
		logx.W(ctx, "task(ResourceExpire) GetKnowledgeEmbeddingModel err:%+v", err)
		return err
	}
	req := &retrieval.BatchDeleteKnowledgeReq{
		RobotId:            doc.RobotID,
		IndexId:            entity.SegmentReviewVersionID,
		Data:               data,
		DocType:            entity.DocTypeSegment,
		BotBizId:           bot.BizId,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
	}
	if err = d.rpc.RetrievalDirectIndex.BatchDeleteKnowledge(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireTaskHandler) deleteProdKnowledge(ctx context.Context, bot *entity.AppBaseInfo, doc *docEntity.Doc,
	data []*retrieval.KnowledgeIDType) error {
	if bot.QaVersion == 0 {
		return nil
	}
	req := &retrieval.BatchDeleteAllKnowledgeProdReq{
		RobotId:   doc.RobotID,
		VersionId: bot.QaVersion,
		Data:      data,
		DocType:   entity.DocTypeSegment,
		BotBizId:  bot.BizId,
		DocId:     doc.ID,
	}
	if _, err := d.rpc.Retrieval.BatchDeleteAllKnowledgeProd(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireTaskHandler) updateDocQAStatusExceeded(ctx context.Context,
	bot *entity.AppBaseInfo, doc *docEntity.Doc, useQASize bool) (uint64, error) {
	docQAList, err := getBotQAList(ctx, d.p.CorpID, bot.PrimaryId, doc.ID, d.qaLogic)
	if err != nil {
		return 0, err
	}
	docQACharSize := uint64(0)
	for _, qa := range docQAList {
		// 判断是否处于稳态；如果有非稳态，就跳过这个qa，处理其他的qa
		if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditing || qa.ReleaseStatus == qaEntity.QAReleaseStatusAppealIng {
			// 如果是审核中或者人工申诉中，不认为有非稳态，且直接跳过这个qa，不做超量逻辑；审核/人工申诉回调接口会更新状态
			logx.D(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v, skip it",
				d.task.ID, bot.PrimaryId, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			continue
		}
		if qa.ReleaseStatus == qaEntity.QAReleaseStatusLearning {
			// 如果是学习中，就认为有非稳态，则标记并跳过该qa,继续处理下一个；所有qa处理完之后再报错，等待任务下一次执行
			logx.D(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v is not stable, skip it",
				d.task.ID, bot.PrimaryId, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			d.hasQANotStable = true
			continue
		}
		if useQASize {
			docQACharSize += qa.QaSize
		} else {
			docQACharSize += qa.CharSize
		}
		if err := d.deleteQASandboxKnowledge(ctx, bot, qa); err != nil {
			return 0, err
		}
		if err := d.deleteQAProdKnowledge(ctx, bot, qa); err != nil {
			return 0, err
		}

		if err := d.updateQAStatusExceeded(ctx, qa); err != nil {
			return 0, err
		}
	}
	return docQACharSize, nil
}

func (d *ResExpireTaskHandler) updateQAStatusExceeded(ctx context.Context, qa *qaEntity.DocQA) error {
	switch qa.ReleaseStatus {
	case qaEntity.QAReleaseStatusInit:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusCharExceeded
	case qaEntity.QAReleaseStatusSuccess:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusCharExceeded
	case qaEntity.QAReleaseStatusFail:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusCharExceeded
	case qaEntity.QAReleaseStatusExpired:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusCharExceeded
	case qaEntity.QAReleaseStatusAppealFail:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAppealFailCharExceeded
	case qaEntity.QAReleaseStatusAuditNotPass:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPassCharExceeded
	case qaEntity.QAReleaseStatusLearnFail:
		qa.ReleaseStatus = qaEntity.QAReleaseStatusLearnFailCharExceeded
	}
	// 更新 QA，不更新向量库
	// 相似问同 QA，也只更新相似问 ，不更新向量库
	sqs, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx,
			"task(ResourceExpire) Process, task: %+v, bot: %+v, qaID: %+v, GetSimilarQuestionsByQA err: %+v")
		// 柔性放过
	}
	sqm := &qaEntity.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	if err := d.qaLogic.UpdateQA(ctx, qa, sqm, false, false, 0, 0, &labelEntity.UpdateQAAttributeLabelReq{}); err != nil {
		logx.D(ctx, "task(ResourceExpire) Process, task: %+v, bot: %+v, qaID: %+v, UpdateQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		return err
	}
	return nil
}

func (d *ResExpireTaskHandler) markQAExceeded(ctx context.Context, bot *entity.AppBaseInfo, exceededCount uint64, useCapacity bool) (bool,
	uint64, error) {
	hasExceeded := false
	qas, err := getBotQAList(ctx, d.p.CorpID, bot.PrimaryId, 0, d.qaLogic)
	if err != nil {
		return false, 0, err
	}
	usedSize := uint64(0)
	for _, qa := range qas {
		if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditing || qa.ReleaseStatus == qaEntity.QAReleaseStatusAppealIng {
			logx.D(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v, skip it",
				d.task.ID, bot.PrimaryId, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			continue
		}
		if qa.ReleaseStatus == qaEntity.QAReleaseStatusLearning {
			logx.D(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v is not stable, skip it",
				d.task.ID, bot.PrimaryId, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			d.hasQANotStable = true
			continue
		}
		if useCapacity {
			usedSize += qa.QaSize
		} else {
			usedSize += qa.CharSize
		}
		if err := d.deleteQASandboxKnowledge(ctx, bot, qa); err != nil {
			return false, 0, err
		}
		if err := d.deleteQAProdKnowledge(ctx, bot, qa); err != nil {
			return false, 0, err
		}
		if err := d.updateQAStatusExceeded(ctx, qa); err != nil {
			return false, 0, err
		}
		hasExceeded = true
		if usedSize > exceededCount {
			break
		}
	}
	if usedSize <= exceededCount {
		exceededCount = exceededCount - usedSize
	}
	if hasExceeded && !useCapacity {
		if err := d.charExceededNotice(ctx, bot, releaseEntity.NoticeQAPageID, releaseEntity.OpTypeViewQACharExceeded,
			false); err != nil {
			logx.W(ctx, "task(ResourceExpire) Process task: %+v, bot: %+v, charExceededNotice err: %+v",
				d.task.ID, bot, err)
			return hasExceeded, exceededCount, err
		}
	}
	return hasExceeded, exceededCount, nil
}

// checkOldUserCharSizeExceeded 检查老用户字符数是否超量（基于字符数判断）

func (d *ResExpireTaskHandler) checkOldUserCharSizeExceeded(ctx context.Context, corpBizId, sid uint64, uin string) (uint64, error) {
	logx.D(ctx, "task(ResourceExpire) checkOldUserCharSizeExceeded, task: %+v, corp: %d", d.task.ID, corpBizId)
	maxCharSize, err := d.rpc.Finance.GetCorpMaxCharSize(ctx, sid, uin)
	if err != nil {
		return 0, errs.ErrCorpNotFound
	}
	usedCharSize, err := d.rpc.AppAdmin.CountCorpAppCharSize(ctx, d.p.CorpID)
	// usedCharSize, err := d.dao.GetCorpUsedCharSizeUsage(ctx, corpID)
	if err != nil {
		return 0, errs.ErrSystem
	}
	if d.p.IsDebug {
		// TODO: 调试逻辑, 限制 MaxCharSize, 使用超量参数作为剩余容量
		maxCharSize = uint64(d.p.Capacity)
	}
	logx.D(ctx, "task(ResourceExpire) Process, task: %+v, usedCharSize: %+v, MaxCharSize: %+v", d.task.ID, usedCharSize, maxCharSize)
	if usedCharSize > maxCharSize {
		exceededCount := usedCharSize - maxCharSize
		return exceededCount, errs.ErrOverCharacterSizeLimit
	}
	return 0, nil
}

// Fail 任务失败
func (d *ResExpireTaskHandler) Fail(ctx context.Context) error {
	logx.D(ctx, "task(ResourceExpire) Fail")
	return nil
}

// Stop 任务停止
func (d *ResExpireTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *ResExpireTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(ResourceExpire) Done")
	// TODO: ...
	return nil
}
