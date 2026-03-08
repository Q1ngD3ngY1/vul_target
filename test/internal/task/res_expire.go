package task

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"hash/fnv"
	"sort"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

const (
	resExpireSegDeletePrefix   = "resExpire:seg:delete:"
	resExpireSegGenQAPrefix    = "resExpire:seg:gen:qa:"
	resExpireSegGenIndexPrefix = "resExpire:seg:gen:index:"
	resExpireQADeletePrefix    = "resExpire:qa:delete:"
)

// ResExpireScheduler 资源包到期后处理离线任务
type ResExpireScheduler struct {
	dao             dao.Dao
	task            task_scheduler.Task
	p               model.ResExpireParams
	hasDocNotStable bool
	hasQANotStable  bool
}

func initResExpireScheduler() {
	task_scheduler.Register(
		model.ResourceExpireTask,
		func(t task_scheduler.Task, params model.ResExpireParams) task_scheduler.TaskHandler {
			return &ResExpireScheduler{
				dao:             dao.New(),
				task:            t,
				p:               params,
				hasDocNotStable: false,
				hasQANotStable:  false,
			}
		},
	)
}

// Prepare 数据准备
func (d *ResExpireScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(ResourceExpire) Prepare, task: %+v, params: %+v", d.task, d.p)
	// TODO: ...
	return task_scheduler.TaskKV{}, nil
}

// Init 初始化
func (d *ResExpireScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *ResExpireScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, params: %+v", d.task, d.p)
	exceededCount, err := d.checkUsedCharSizeExceeded(ctx, d.p.CorpID)
	if err == nil {
		log.InfoContextf(ctx, "task(ResourceExpire) Process, task: %+v, not exceeded", d.task.ID)
		return nil
	}
	if err != errs.ErrOverCharacterSizeLimit {
		// 非超量的情况不处理
		log.ErrorContextf(ctx, "task(ResourceExpire) Process, task: %+v, CharSize not Exceeded, err: %+v", d.task.ID,
			err)
		return err
	}

	log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, CharSize Exceeded", d.task.ID)
	botList, err := getBotList(ctx, d.p.CorpID, d.dao)
	if err != nil {
		log.ErrorContextf(ctx, "task(ResourceExpire) Process, task: %+v, getBotList err: %+v", d.task.ID, err)
		return err
	}

	log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, app count: %+v", d.task.ID, len(botList))
	for _, bot := range botList {
		if d.p.IsDebug {
			// TODO: 调试逻辑, 用于针对指定应用进行超量隔离
			appID := fmt.Sprintf("%d", bot.BusinessID)
			if d.p.ResourceID != appID {
				continue
			}
		}
		bot, err := d.dao.GetAppByAppBizID(ctx, bot.BusinessID)
		if err != nil {
			log.WarnContextf(ctx, "GetAppByAppBizID error: %s", err.Error())
			continue
		}
		if bot.HasDeleted() {
			// 删除的应用不再处理
			continue
		}
		ctx = pkg.WithSpaceID(ctx, bot.SpaceID)
		log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, botID: %+v, appID: %+v, appName: %+v",
			d.task.ID, bot.ID, bot.BusinessID, bot.Name)
		exceededCount, err = d.checkUsedCharSizeExceeded(ctx, d.p.CorpID)
		if err != errs.ErrOverCharacterSizeLimit {
			// 删除到可用总量以下
			return err
		}
		log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, botID: %+v, exceededCount: %+v",
			d.task.ID, bot.ID, exceededCount)
		if _, err := d.markBotCharSizeExceeded(ctx, bot, exceededCount); err != nil {
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

// getBotList 获取企业下的机器人
func getBotList(ctx context.Context, corpID uint64, dao dao.Dao) ([]*model.AppDB, error) {
	pageSize := uint32(1000)
	page := uint32(1)
	botList := make([]*model.AppDB, 0)
	for {
		list, err := dao.GetRobotList(ctx, corpID, "", []uint64{}, 0, page, pageSize)
		if err != nil {
			return nil, err
		}
		if len(list) == 0 {
			break
		}
		botList = append(botList, list...)
		page++
	}
	sort.Slice(botList, func(i, j int) bool {
		// 按创建时间顺序排列
		return botList[i].CreateTime.After(botList[j].CreateTime)
	})
	return botList, nil
}

// getBotDocList 机器人下的文档
func getBotDocList(ctx context.Context, bot *model.AppDB, dao dao.Dao) ([]*model.Doc, error) {
	pageSize := uint32(1000)
	page := uint32(1)
	docs := make([]*model.Doc, 0)
	for {
		req := &model.DocListReq{
			CorpID:   bot.CorpID,
			RobotID:  bot.ID,
			Page:     page,
			PageSize: pageSize,
		}
		_, list, err := dao.GetDocList(ctx, req)
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
func getBotQAList(ctx context.Context, corpID uint64, botID uint64, docID uint64, dao dao.Dao) ([]*model.DocQA, error) {
	pageSize := uint32(1000)
	page := uint32(1)
	qas := make([]*model.DocQA, 0)
	for {
		req := &model.QAListReq{
			CorpID:    corpID,
			RobotID:   botID,
			DocID:     []uint64{docID},
			IsDeleted: model.QAIsNotDeleted,
			Page:      page,
			PageSize:  pageSize,
		}
		list, err := dao.GetQAList(ctx, req)
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

func (d *ResExpireScheduler) charExceededNotice(ctx context.Context, bot *model.AppDB, page uint32, opType uint32,
	global bool) error {
	log.DebugContextf(ctx, "task(ResourceExpire) charExceededNotice, botID: %+v", bot.ID)
	operations := []model.Operation{{Typ: opType, Params: model.OpParams{}}}
	corp, err := d.dao.GetCorpByID(ctx, bot.CorpID)
	if err != nil {
		return err
	}
	isSystemIntegrator := d.dao.IsSystemIntegrator(ctx, corp)
	if !isSystemIntegrator {
		// 非系统集成商才需要额外增加跳转
		operations = append(operations, model.Operation{Typ: model.OpTypeExpandCapacity, Params: model.OpParams{}})
	}
	noticeOptions := []model.NoticeOption{
		model.WithPageID(page),
		model.WithLevel(model.LevelWarning),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficient)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyKnowledgeBaseCapacityInsufficientKnowledgeExpired)),
		// model.WithForbidCloseFlag(),
	}
	if global {
		noticeOptions = append(noticeOptions, model.WithGlobalFlag())
	}
	// 给所有用户对这个robot都发通知
	staffs, err := d.dao.GetStaffByCorpID(ctx, d.p.CorpID, "", []uint64{}, 1, 1000)
	if err != nil {
		log.ErrorContextf(ctx, "task(ResourceExpire) Process, GetStaffByCorpID:%+v err:%+v", d.p.CorpID, err)
		return err
	}
	noticeType := model.NoticeTypeDocCharExceeded
	if page == model.OpTypeViewQACharExceeded {
		noticeType = model.NoticeTypeQACharExceeded
	}
	for _, staff := range staffs {
		notice := model.NewNotice(noticeType, genRelatedID(bot.ID, page), d.p.CorpID, bot.ID, staff.ID,
			noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "task(ResourceExpire) Process, 序列化通知操作参数失败 operations:%+v err:%+v",
				operations, err)
			return err
		}
		log.DebugContextf(ctx, "task(ResourceExpire) Process, CreateNotice notice: %+v", notice)
		if err := d.dao.CreateNotice(ctx, notice); err != nil {
			log.ErrorContextf(ctx, "task(ResourceExpire) Process, CreateNotice notice: %+v err: %+v", notice, err)
			return err
		}
	}
	return nil
}

func (d *ResExpireScheduler) markBotCharSizeExceeded(ctx context.Context, bot *model.AppDB,
	exceededCount uint64) (uint64, error) {
	defer updateAppCharSize(ctx, d.dao, bot.ID, bot.CorpID)
	hasExceeded := false
	opType := model.OpTypeViewDocCharExceeded
	docHasExceeded, exceededCount, err := d.markDocCharSizeExceeded(ctx, bot, exceededCount)
	if err != nil {
		return 0, err
	}
	qaHasExceeded, exceededCount, err := d.markQACharSizeExceeded(ctx, bot, exceededCount)
	if err != nil {
		return 0, err
	}
	if docHasExceeded || qaHasExceeded {
		hasExceeded = true
	}
	if (!docHasExceeded) && qaHasExceeded {
		opType = model.OpTypeViewQACharExceeded
	}
	if !hasExceeded {
		return exceededCount, nil
	}
	if hasExceeded {
		if err := d.charExceededNotice(ctx, bot, model.NoticeRobotInfoPageID, opType, true); err != nil {
			log.WarnContextf(ctx, "task(ResourceExpire) Process task: %+v, bot: %+v, charExceededNotice err: %+v",
				d.task.ID, bot.ID, err)
			return exceededCount, err
		}
	}
	return exceededCount, nil
}

func (d *ResExpireScheduler) updateDocStatusExceeded(ctx context.Context, doc *model.Doc) error {
	switch doc.Status {
	case model.DocStatusParseImportFail:
		doc.Status = model.DocStatusParseImportFailCharExceeded
	case model.DocStatusAuditFail:
		doc.Status = model.DocStatusAuditFailCharExceeded
	case model.DocStatusUpdateFail:
		doc.Status = model.DocStatusUpdateFailCharExceeded
	case model.DocStatusCreateIndexFail:
		doc.Status = model.DocStatusCreateIndexFailCharExceeded
	case model.DocStatusExpired:
		doc.Status = model.DocStatusExpiredCharExceeded
	case model.DocStatusAppealFailed:
		doc.Status = model.DocStatusAppealFailedCharExceeded
	default:
		doc.Status = model.DocStatusCharExceeded
	}
	if err := d.dao.UpdateDocStatus(ctx, doc); err != nil {
		log.DebugContextf(ctx,
			"task(ResourceExpire) Process, task: %+v, bot: %+v, docID: %+v, docName: %+v, UpdateDocStatus err: %+v",
			d.task.ID, doc.RobotID, doc.ID, doc.FileName, err)
		return err
	}
	return nil
}

func (d *ResExpireScheduler) deleteDocSegments(ctx context.Context, bot *model.AppDB, doc *model.Doc) error {
	pageSize := uint32(50)
	segLen := pageSize
	for segLen > 0 {
		segs, err := d.dao.GetSegmentList(ctx, d.p.CorpID, doc.ID, 1, pageSize, bot.ID)
		if err != nil {
			log.ErrorContextf(ctx, "task(ResourceExpire) Process, task: %+v, GetSegmentList err: %+v", d.task.ID, err)
			return err
		}

		segLen = uint32(len(segs))
		if segLen == 0 {
			break
		}
		data := make([]*bot_retrieval_server.KnowledgeIDType, 0, len(segs))
		dataAll := make([]*bot_retrieval_server.KnowledgeIDType, 0, len(segs))
		for _, seg := range segs {
			if !seg.IsSegmentForQA() && !seg.IsText2sqlSegmentType() {
				// 评测端的, Text2SQL在DeleteText2SQL中删除
				data = append(data, &bot_retrieval_server.KnowledgeIDType{
					Id:          seg.ID,
					SegmentType: seg.SegmentType,
				})
			}
			//
			dataAll = append(dataAll, &bot_retrieval_server.KnowledgeIDType{
				Id:          seg.ID,
				SegmentType: seg.SegmentType,
			})
		}
		if len(data) != 0 {
			if err := d.deleteSandboxKnowledge(ctx, bot, doc, data); err != nil {
				log.ErrorContextf(ctx, "task(ResourceExpire) Process task: %+v deleteSandboxKnowledge err:%+v",
					d.task.ID, err)
				return err
			}
		}
		if len(dataAll) != 0 {
			if err := d.deleteProdKnowledge(ctx, bot, doc, dataAll); err != nil {
				log.ErrorContextf(ctx, "task(ResourceExpire) Process task: %+v deleteProdKnowledge err:%+v", d.task.ID,
					err)
				return err
			}
		}
		if err := d.dao.BatchDeleteSegments(ctx, segs, bot.ID); err != nil {
			log.ErrorContextf(ctx, "task(ResourceExpire) Process, task: %+v, DeleteSegment err: %+v", d.task.ID, err)
			return err
		}
		// 实际在按页删除的过程中已经将page数减少了
		// page++
	}
	return d.dao.DeleteBigDataElastic(ctx, bot.ID, doc.ID, bot_retrieval_server.KnowledgeType_KNOWLEDGE, false)
}

func (d *ResExpireScheduler) markDocCharSizeExceeded(ctx context.Context, bot *model.AppDB, exceededCount uint64) (bool,
	uint64, error) {
	hasExceeded := false
	docs, err := getBotDocList(ctx, bot, d.dao)
	if err != nil {
		return false, 0, err
	}
	log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, bot: %+v, doc count: %+v", d.task.ID, bot.ID,
		len(docs))
	// exceededDocs := make([]*model.Doc, 0, len(docs))
	charSize := uint64(0)
	for _, doc := range docs {
		log.DebugContextf(ctx,
			"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, Status: %+v, CharSize: %+v, Exceeded: %+v",
			d.task.ID, bot.ID, doc.FileName, doc.StatusDesc(false), doc.CharSize, doc.IsCharSizeExceeded())
		if doc.Status == model.DocStatusUnderAppeal || doc.Status == model.DocStatusAuditIng {
			// 1. 处于审核中或者人工申诉中的文档，不存在生成的问答
			// 2. 审核/申诉回调的时候，会判断文档是否超量，所以这里不需要处理
			log.DebugContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, Status: %+v "+
					"is not stable, skip it", d.task.ID, bot.ID, doc.FileName, doc.StatusDesc(false))
			continue
		}
		if !doc.IsStableStatus() {
			// 若存在非稳态的文档,则标记并跳过该文档,继续处理下一个；所有文档处理完之后再报错，等待任务下一次执行
			log.DebugContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, Status: %+v "+
					"is not stable, skip it", d.task.ID, bot.ID, doc.FileName, doc.StatusDesc(false))
			d.hasDocNotStable = true
			continue
		}
		if doc.IsCharSizeExceeded() {
			continue
		}
		if doc.Status == model.DocStatusParseFail {
			continue
		}
		charSize += doc.CharSize
		// 文档生成的问答也对应清理
		docQACharSize, err := d.updateDocQAStatusExceeded(ctx, bot, doc)
		if err != nil {
			return false, 0, err
		}
		charSize += docQACharSize
		if err := d.deleteDocSegments(ctx, bot, doc); err != nil {
			log.ErrorContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, deleteDocSegments err: %+v",
				d.task.ID, bot.ID, doc.FileName, err)
			return false, 0, err
		}

		if err = d.dao.DeleteText2SQL(ctx, doc.RobotID, doc.ID); err != nil {
			return false, 0, err
		}
		if err := d.updateDocStatusExceeded(ctx, doc); err != nil {
			log.ErrorContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, docName: %+v, updateDocStatusExceeded err: %+v",
				d.task.ID, bot.ID, doc.FileName, err)
			return false, 0, err
		}
		hasExceeded = true
		if charSize > exceededCount {
			// 标记到此已经到目标容量了
			break
		}
	}
	if charSize <= exceededCount {
		exceededCount = exceededCount - charSize
	}
	if hasExceeded {
		if err := d.charExceededNotice(ctx, bot, model.NoticeDocPageID, model.OpTypeViewDocCharExceeded,
			false); err != nil {
			log.WarnContextf(ctx, "task(ResourceExpire) Process task: %+v, bot: %+v, charExceededNotice err: %+v",
				d.task.ID, bot.ID, err)
		}
	}
	return hasExceeded, exceededCount, nil
}
func (d *ResExpireScheduler) deleteQASandboxKnowledge(ctx context.Context, bot *model.AppDB, qa *model.DocQA) error {
	appDB, err := d.dao.GetAppByID(ctx, qa.RobotID)
	if err != nil {
		return err
	}
	embeddingConf, _, err := appDB.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "删除文档切片,查询机器人数据失败 GetEmbeddingConf() err:%+v", err)
		return err
	}
	data := []*bot_retrieval_server.KnowledgeIDType{
		{
			Id: qa.ID,
		},
	}
	// 支持相似问删除
	sims, err := d.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "获取qa的相似问失败: %+v, err: %+v", qa, err)
		return err
	}
	if len(sims) > 0 {
		for _, v := range sims {
			data = append(data, &bot_retrieval_server.KnowledgeIDType{
				Id: v.SimilarID,
			})
		}
	}
	req := &bot_retrieval_server.BatchDeleteKnowledgeReq{
		RobotId:            qa.RobotID,
		IndexId:            model.ReviewVersionID,
		Data:               data,
		DocType:            model.DocTypeQA,
		BotBizId:           bot.BusinessID,
		EmbeddingVersion:   embeddingConf.Version,
		EmbeddingModelName: model.EmptyEmbeddingModelName,
	}
	if _, err := d.dao.BatchDeleteKnowledge(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireScheduler) deleteQAProdKnowledge(ctx context.Context, bot *model.AppDB, qa *model.DocQA) error {
	if bot.QAVersion == 0 {
		// 应用未发布,可以跳过删除发布端数据的逻辑
		return nil
	}
	data := []*bot_retrieval_server.KnowledgeIDType{
		{
			Id: qa.ID,
		},
	}
	// 支持相似问删除
	sims, err := d.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "获取qa的相似问失败: %+v, err: %+v", qa, err)
		return err
	}
	if len(sims) > 0 {
		for _, v := range sims {
			data = append(data, &bot_retrieval_server.KnowledgeIDType{
				Id: v.SimilarID,
			})
		}
	}
	req := &bot_retrieval_server.BatchDeleteAllKnowledgeProdReq{
		RobotId:   qa.RobotID,
		VersionId: bot.QAVersion,
		Data:      data,
		DocType:   model.DocTypeQA,
		BotBizId:  bot.BusinessID,
		DocId:     qa.DocID,
	}
	if _, err := d.dao.BatchDeleteAllKnowledgeProd(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireScheduler) deleteSandboxKnowledge(ctx context.Context, bot *model.AppDB, doc *model.Doc,
	data []*bot_retrieval_server.KnowledgeIDType) error {
	appDB, err := d.dao.GetAppByID(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	embeddingConf, _, err := appDB.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "删除文档切片,查询机器人数据失败 GetEmbeddingConf() err:%+v", err)
		return err
	}
	req := &bot_retrieval_server.BatchDeleteKnowledgeReq{
		RobotId:            doc.RobotID,
		IndexId:            model.SegmentReviewVersionID,
		Data:               data,
		DocType:            model.DocTypeSegment,
		BotBizId:           bot.BusinessID,
		EmbeddingVersion:   embeddingConf.Version,
		EmbeddingModelName: model.EmptyEmbeddingModelName,
	}
	if _, err := d.dao.BatchDeleteKnowledge(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireScheduler) deleteProdKnowledge(ctx context.Context, bot *model.AppDB, doc *model.Doc,
	data []*bot_retrieval_server.KnowledgeIDType) error {
	if bot.QAVersion == 0 {
		return nil
	}
	req := &bot_retrieval_server.BatchDeleteAllKnowledgeProdReq{
		RobotId:   doc.RobotID,
		VersionId: bot.QAVersion,
		Data:      data,
		DocType:   model.DocTypeSegment,
		BotBizId:  bot.BusinessID,
		DocId:     doc.ID,
	}
	if _, err := d.dao.BatchDeleteAllKnowledgeProd(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *ResExpireScheduler) updateDocQAStatusExceeded(ctx context.Context, bot *model.AppDB, doc *model.Doc) (uint64,
	error) {
	docQAList, err := getBotQAList(ctx, d.p.CorpID, bot.ID, doc.ID, d.dao)
	if err != nil {
		return 0, err
	}
	docQACharSize := uint64(0)
	for _, qa := range docQAList {
		// 判断是否处于稳态；如果有非稳态，就跳过这个qa，处理其他的qa
		if qa.ReleaseStatus == model.QAReleaseStatusAuditing || qa.ReleaseStatus == model.QAReleaseStatusAppealIng {
			// 如果是审核中或者人工申诉中，不认为有非稳态，且直接跳过这个qa，不做超量逻辑；审核/人工申诉回调接口会更新状态
			log.DebugContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v, skip it",
				d.task.ID, bot.ID, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			continue
		}
		if qa.ReleaseStatus == model.QAReleaseStatusLearning {
			// 如果是学习中，就认为有非稳态，则标记并跳过该qa,继续处理下一个；所有qa处理完之后再报错，等待任务下一次执行
			log.DebugContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v is not stable, skip it",
				d.task.ID, bot.ID, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			d.hasQANotStable = true
			continue
		}
		docQACharSize += uint64(qa.CharSize)
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

func (d *ResExpireScheduler) updateQAStatusExceeded(ctx context.Context, qa *model.DocQA) error {
	switch qa.ReleaseStatus {
	case model.QAReleaseStatusInit:
		qa.ReleaseStatus = model.QAReleaseStatusCharExceeded
	case model.QAReleaseStatusSuccess:
		qa.ReleaseStatus = model.QAReleaseStatusCharExceeded
	case model.QAReleaseStatusFail:
		qa.ReleaseStatus = model.QAReleaseStatusCharExceeded
	case model.QAReleaseStatusExpired:
		qa.ReleaseStatus = model.QAReleaseStatusCharExceeded
	case model.QAReleaseStatusAppealFail:
		qa.ReleaseStatus = model.QAReleaseStatusAppealFailCharExceeded
	case model.QAReleaseStatusAuditNotPass:
		qa.ReleaseStatus = model.QAReleaseStatusAuditNotPassCharExceeded
	case model.QAReleaseStatusLearnFail:
		qa.ReleaseStatus = model.QAReleaseStatusLearnFailCharExceeded
	}
	// 更新 QA，不更新向量库
	// 相似问同 QA，也只更新相似问 ，不更新向量库
	sqs, err := d.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx,
			"task(ResourceExpire) Process, task: %+v, bot: %+v, qaID: %+v, GetSimilarQuestionsByQA err: %+v")
		// 柔性放过
	}
	sqm := &model.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	if err := d.dao.UpdateQA(ctx, qa, sqm, false, false, 0, &model.UpdateQAAttributeLabelReq{}); err != nil {
		log.DebugContextf(ctx, "task(ResourceExpire) Process, task: %+v, bot: %+v, qaID: %+v, UpdateQA err: %+v",
			d.task.ID, qa.RobotID, qa.ID, err)
		return err
	}
	return nil
}

// markQACharSizeExceeded 只处理非文档生成的问答
func (d *ResExpireScheduler) markQACharSizeExceeded(ctx context.Context, bot *model.AppDB, exceededCount uint64) (bool,
	uint64, error) {
	hasExceeded := false
	qas, err := getBotQAList(ctx, d.p.CorpID, bot.ID, 0, d.dao) // 非文档生成的问答
	if err != nil {
		return false, 0, err
	}
	// exceededQAs := make([]*model.DocQA, 0, len(qas))
	charSize := uint64(0)
	for _, qa := range qas {
		// 判断是否处于稳态；如果有非稳态，就跳过这个qa，处理其他的qa
		if qa.ReleaseStatus == model.QAReleaseStatusAuditing || qa.ReleaseStatus == model.QAReleaseStatusAppealIng {
			// 如果是审核中或者人工申诉中，不认为有非稳态，且直接跳过这个qa，不做超量逻辑；审核/人工申诉回调接口会更新状态
			log.DebugContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v, skip it",
				d.task.ID, bot.ID, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			continue
		}
		if qa.ReleaseStatus == model.QAReleaseStatusLearning {
			// 如果是学习中，就认为有非稳态，则标记并跳过该qa,继续处理下一个；所有qa处理完之后再报错，等待任务下一次执行
			log.DebugContextf(ctx,
				"task(ResourceExpire) Process, task: %+v, bot: %+v, qa: %+v, Status: %+v is not stable, skip it",
				d.task.ID, bot.ID, qa.Question, i18n.Translate(ctx, qa.StatusDesc(false)))
			d.hasQANotStable = true
			continue
		}
		charSize += qa.CharSize
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
		if charSize > exceededCount {
			// 标记到此已经到目标容量了
			break
		}
	}
	if charSize <= exceededCount {
		exceededCount = exceededCount - charSize
	}
	if hasExceeded {
		if err := d.charExceededNotice(ctx, bot, model.NoticeQAPageID, model.OpTypeViewQACharExceeded,
			false); err != nil {
			log.WarnContextf(ctx, "task(ResourceExpire) Process task: %+v, bot: %+v, charExceededNotice err: %+v",
				d.task.ID, bot, err)
			return hasExceeded, exceededCount, err
		}
	}
	return hasExceeded, exceededCount, nil
}

func (d *ResExpireScheduler) checkUsedCharSizeExceeded(ctx context.Context, corpID uint64) (uint64, error) {
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
	if d.p.IsDebug {
		// TODO: 调试逻辑, 限制 MaxCharSize, 使用超量参数作为剩余容量
		corp.MaxCharSize = uint64(d.p.Capacity)
	}
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

// Fail 任务失败
func (d *ResExpireScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(ResourceExpire) Fail")
	return nil
}

// Stop 任务停止
func (d *ResExpireScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *ResExpireScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(ResourceExpire) Done")
	// TODO: ...
	return nil
}

func updateAppCharSize(ctx context.Context, dao dao.Dao, robotID uint64, corpID uint64) error {
	docSize, err := dao.GetRobotDocCharSize(ctx, robotID, corpID)
	if err != nil {
		return err
	}
	qaSize, err := dao.GetRobotQACharSize(ctx, robotID, corpID)
	if err != nil {
		return err
	}
	if err = dao.UpdateAppCharSize(ctx, robotID, docSize+qaSize); err != nil {
		return err
	}
	return nil
}
