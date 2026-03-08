package task

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/realtime"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"time"
)

// EmbeddingUpgradeScheduler embedding 升级
// 【【知识引擎】embedding模型升级sn-large-zh-v0.2.1版本-新用户用新embedding】
// https://tapd.woa.com/project_qrobot/prong/stories/view/1070080800116042823
type EmbeddingUpgradeScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.EmbeddingUpgradeParams
}

const (
	// SysLabelQAFlagName TODO
	SysLabelQAFlagName = "_sys_str_qa_flag" // 系统标签(for相似问场景)
	// SysLabelQAFlagValueSimilar TODO
	SysLabelQAFlagValueSimilar = "similar" // 标识相似问
	// SysLabelQAIdName TODO
	SysLabelQAIdName = "_sys_str_qa_id" // 标识相似问的qaId
)

func initEmbeddingUpgradeScheduler() {
	task_scheduler.Register(
		model.EmbeddingUpgradeTask,
		func(t task_scheduler.Task, params model.EmbeddingUpgradeParams) task_scheduler.TaskHandler {
			return &EmbeddingUpgradeScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare .
func (e *EmbeddingUpgradeScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	// 查找所有待升级应用
	log.InfoContextf(ctx, "查找所有待升级应用, 待升级版本: %d, 目标版本: %d", e.p.FromVer, e.p.ToVer)
	apps, err := e.dao.GetWaitEmbeddingUpgradeApp(ctx, e.p.AppIDs, e.p.FromVer, e.p.ToVer)
	if err != nil {
		log.WarnContextf(ctx, "GetWaitEmbeddingUpgradeApp error: %v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "待升级应用数: %d", len(apps))
	if len(apps) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(apps))
	kv := make(task_scheduler.TaskKV)
	for _, app := range apps {
		id := fmt.Sprintf("%d", app.ID)
		ids = append(ids, id)
		kv[id] = ""
	}
	log.InfoContextf(ctx, "待升级应用ID: %+v", ids)

	// 创建 应用 检索库
	// log.InfoContextf(ctx, "创建 应用 检索库")
	// if err = e.initAppIndex(ctx, apps); err != nil {
	// 	log.WarnContextf(ctx, "创建 应用 检索库 失败, err: %v", err)
	// 	return nil, err
	// }
	// log.InfoContextf(ctx, "创建 应用 检索库 完成")

	// 升级 全局知识库 embedding
	log.InfoContextf(ctx, "升级 全局知识库")
	if err = e.upgradeGlobalKnowledge(ctx); err != nil {
		log.WarnContextf(ctx, "升级 全局知识库 失败, err: %v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "升级 全局知识库 完成")

	log.InfoContextf(ctx, "初始化 offset 记录")
	if _, err = e.dao.RedisCli().Do(ctx, "DEL", e.cacheKey()); err != nil {
		log.WarnContextf(ctx, "初始化 offset 记录失败(DEL), error: %v", err)
		return nil, err
	}
	if _, err = e.dao.RedisCli().Do(ctx, "HSET", e.cacheKey(), "app_ids", strings.Join(ids, ",")); err != nil {
		log.WarnContextf(ctx, "初始化 offset 记录失败(HSET), error: %v", err)
		return nil, err
	}
	if _, err = e.dao.RedisCli().Do(ctx, "EXPIRE", e.cacheKey(), 3600*24*30*3); err != nil {
		log.WarnContextf(ctx, "初始化 offset 记录失败(EXPIRE), error: %v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "初始化 offset 记录完成")
	return kv, nil
}

// Init .
func (e *EmbeddingUpgradeScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	return nil
}

// Process .
func (e *EmbeddingUpgradeScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k := range progress.TaskKV(ctx) {
		t0 := time.Now()
		log.InfoContextf(
			ctx, "待升级应用数: %d, 已完成百分比: %d/%d(%s)",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(),
		)
		log.InfoContextf(ctx, "升级应用(%s)", k)
		appID, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			log.ErrorContextf(ctx, "应用ID(%s)非 uint64, ParseUint error: %v", k, err)
			return err
		}
		if err = e.upgrade(ctx, appID, func(ctx context.Context) error {
			if err = progress.Finish(ctx, k); err != nil {
				return err
			}
			log.InfoContextf(ctx, "升级应用成功(%s), cost:%d", k, int(time.Since(t0).Seconds()))
			return nil
		}); err != nil {
			log.WarnContextf(ctx, "升级应用失败(%s), err: %v", k, err)
			return err
		}
	}
	if progress.Remain() != 0 {
		var ids []string
		for k := range progress.TaskKV(ctx) {
			ids = append(ids, k)
		}
		log.WarnContextf(
			ctx, "待升级应用数: %d, 已完成百分比: %d/%d(%s), 剩余未完成应用ID: %+v",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(), ids,
		)
		return fmt.Errorf(
			"待升级应用数: %d, 已完成百分比: %d/%d(%s)",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(),
		)
	} else {
		log.InfoContextf(ctx, "升级应用完成(all)")
		return nil
	}
}

// upgrade 单应用升级
func (e *EmbeddingUpgradeScheduler) upgrade(ctx context.Context, appID uint64, done func(ctx context.Context) error) error {
	// 获取应用
	app, err := e.getApp(ctx, appID)
	if err != nil {
		log.WarnContextf(ctx, "getApp fail, appID: %d, err: %v", appID, err)
		return err
	}
	if app.HasDeleted() { // 应用已被删除, 不做处理
		return done(ctx)
	}
	ctx = pkg.WithSpaceID(ctx, app.SpaceID)
	// 检查库大小, 大库晚上升级
	upgradeNow, err := e.upgradeNow(ctx, app)
	if err != nil {
		log.WarnContextf(ctx, "upgradeNow fail, appID: %d, err: %v", appID, err)
		return err
	}
	if !upgradeNow {
		return nil
	}
	// 创建缓存
	if err = e.createCache(ctx, app); err != nil {
		log.WarnContextf(ctx, "createCache fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 写入锁定
	if err = e.lockApp(ctx, app); err != nil {
		log.WarnContextf(ctx, "lockApp fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 创建新库
	if err = e.buildIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildIndex fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 解除锁定
	if err = e.unlockApp(ctx, app); err != nil {
		log.WarnContextf(ctx, "unlockApp fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 清理老库
	if err = e.cleanIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "cleanIndex fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 该应用有发布过则触发线上库升级
	if app.QAVersion > 0 {
		log.InfoContextf(ctx, "应用(%d)通知线上库升级", appID)
		if err = e.dao.ProdEmbeddingUpgrade(ctx, appID, e.p.ToVer); err != nil {
			log.ErrorContextf(ctx, "应用(%d)通知线上库升级失败, err: %v", appID, err)
			return err
		}
	}
	// 标记完成
	return done(ctx)
}

// upgradeGlobalKnowledge 升级全局知识库
func (e *EmbeddingUpgradeScheduler) upgradeGlobalKnowledge(ctx context.Context) error {
	if !e.p.BuildGlobalKnowledge {
		log.InfoContextf(ctx, "upgradeGlobalKnowledge, skip (BuildGlobalKnowledge == false)")
		return nil
	}
	knowledge, err := e.dao.GetAllGlobalKnowledge(ctx)
	if err != nil {
		log.WarnContextf(ctx, "GetAllGlobalKnowledge fail, err: %v", err)
		return err
	}
	// 重建目标版本知识库
	deleteIndexReq := &retrieval.DirectDeleteIndexReq{
		Name:             model.GlobalKnowledgeGroupName,
		IndexId:          model.SearchGlobalVersionID,
		EmbeddingVersion: e.p.ToVer,
	}
	if _, err = e.dao.DirectDeleteIndex(ctx, deleteIndexReq); err != nil {
		log.WarnContextf(ctx, "DirectDeleteIndex fail, req: %+v, err: %v", deleteIndexReq, err)
		return err
	}
	time.Sleep(10 * time.Second)
	createIndexReq := &retrieval.DirectCreateIndexReq{
		Name:             model.GlobalKnowledgeGroupName,
		IndexId:          model.SearchGlobalVersionID,
		EmbeddingVersion: e.p.ToVer,
		DocType:          model.DocTypeQA,
	}
	if _, err = e.dao.DirectCreateIndex(ctx, createIndexReq); err != nil {
		log.WarnContextf(ctx, "DirectCreateIndex fail, req: %+v, err: %v", createIndexReq, err)
		return err
	}
	// 升级全局知识
	for _, v := range knowledge {
		if v.IsDeleted || !v.IsSync {
			continue
		}
		if _, err = e.dao.DirectAddVector(ctx, &retrieval.DirectAddVectorReq{
			Name:             model.GlobalKnowledgeGroupName,
			IndexId:          model.SearchGlobalVersionID,
			Id:               uint64(v.ID),
			PageContent:      v.Question,
			DocType:          model.DocTypeQA,
			EmbeddingVersion: e.p.ToVer,
			Labels:           nil,
		}); err != nil {
			log.ErrorContextf(ctx, "DirectAddVector fail, knowledge: %+v, err: %v", v, err)
			return errs.ErrAddGlobalKnowledge
		}
	}
	return nil
}

func (e *EmbeddingUpgradeScheduler) getApp(ctx context.Context, appID uint64) (model.AppDB, error) {
	app, err := e.dao.GetAppByID(ctx, appID)
	if err != nil {
		log.WarnContextf(ctx, "GetRobotByID fail, appID: %d, err: %v", appID, err)
		return model.AppDB{}, err
	}
	if app == nil {
		log.WarnContextf(ctx, "GetRobotByID fail, app not found, appID: %d", appID)
		return model.AppDB{}, errs.ErrRobotNotFound
	}
	return *app, nil
}

// upgradeNow 检查当前是否允许升级, 小库直接升级, 大库等到晚上再执行
func (e *EmbeddingUpgradeScheduler) upgradeNow(ctx context.Context, app model.AppDB) (bool, error) {
	log.InfoContextf(ctx, "check if upgradeNow, appID: %d", app.ID)
	// 检查是否有执行中的任务, 如果有执行中的任务不允许升级
	tasks, err := dao.GetTasksByAppID(ctx, app.ID)
	if err != nil {
		log.WarnContextf(ctx, "GetTasksByAppID fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	if len(tasks) > 0 {
		for _, task := range tasks {
			// 排除已经终止的任务
			if task.Runner == "terminated" {
				continue
			} else {
				log.InfoContextf(
					ctx,
					"upgradeNow(no), appID: %d, taskID:%d, %d tasks still running",
					app.ID, task.ID, len(tasks),
				)
				return false, nil
			}
		}

	}
	// 检查是否有恢复中的文档，有恢复中的文档则先不升级，等恢复完成后再升级，避免恢复过程中文档只升级部分切片
	resumeDocCount, err := e.dao.GetResumeDocCount(ctx, app.CorpID, app.ID)
	if err != nil {
		log.WarnContextf(ctx, "GetResumeDocCount fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	if resumeDocCount > 0 {
		log.InfoContextf(ctx, "upgradeNow(no), doc is resuming, appID: %d, resumeDocCount:%d", app.ID, resumeDocCount)
		return false, nil
	}
	// 检查库大小
	similarChunkCnt, err := e.dao.GetSimilarChunkCount(ctx, app.CorpID, app.ID)
	if err != nil {
		log.WarnContextf(ctx, "GetSimilarChunkCount fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	qaCnt, err := e.dao.GetQAChunkCount(ctx, app.CorpID, app.ID)
	if err != nil {
		log.WarnContextf(ctx, "GetQAChunkCount fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	// 问答的相似问总数
	qaSimilarCnt, err := e.dao.GetQASimilarQuestionsCount(ctx, app.CorpID, app.ID)
	if err != nil {
		log.WarnContextf(ctx, "GetQASimilarQuestionsCount fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	segmentCnt, err := e.dao.GetSegmentChunkCount(ctx, app.CorpID, app.ID)
	if err != nil {
		log.WarnContextf(ctx, "GetSegmentChunkCount fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	rejectCnt, err := e.dao.GetRejectChunkCount(ctx, app.CorpID, app.ID)
	if err != nil {
		log.WarnContextf(ctx, "GetRejectChunkCount fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	total := similarChunkCnt + qaCnt + qaSimilarCnt + segmentCnt + rejectCnt
	if time.Now().Hour() >= e.p.RunThresholdHourStart || time.Now().Hour() < e.p.RunThresholdHourEnd {
		log.InfoContextf(
			ctx,
			"upgradeNow(yes), appID: %d, similarChunkCnt: %d, qaCnt: %d, qaSimilarCnt:%d, segmentCnt: %d, rejectCnt: %d, "+
				"total: %d > threshold: %d, "+
				"but current hour(%d) in threshold hour(%d - %d)",
			app.ID, similarChunkCnt, qaCnt, qaSimilarCnt, segmentCnt, rejectCnt, total, e.p.RunNowThreshold,
			time.Now().Hour(), e.p.RunThresholdHourStart, e.p.RunThresholdHourEnd,
		)
		return true, nil
	} else if total <= e.p.RunNowThreshold {
		log.InfoContextf(
			ctx,
			"upgradeNow(yes), appID: %d, similarChunkCnt: %d, qaCnt: %d, qaSimilarCnt: %d,"+
				"segmentCnt: %d, rejectCnt: %d, total: %d <= threshold: %d",
			app.ID, similarChunkCnt, qaCnt, qaSimilarCnt, segmentCnt, rejectCnt, total, e.p.RunNowThreshold,
		)
		return true, nil
	} else if app.UpdateTime.Before(time.Now().Add(-7*time.Hour*24)) && total <= 5*e.p.RunNowThreshold {
		log.InfoContextf(
			ctx,
			"upgradeNow(yes), appID: %d, similarChunkCnt: %d, qaCnt: %d, qaSimilarCnt: %d,"+
				"segmentCnt: %d, rejectCnt: %d, silence more than 7 days, total: %d <= 5*threshold: %d",
			app.ID, similarChunkCnt, qaCnt, qaSimilarCnt, segmentCnt, rejectCnt, total, 5*e.p.RunNowThreshold,
		)
		return true, nil
	}
	log.InfoContextf(
		ctx,
		"upgradeNow(no), appID: %d, similarChunkCnt: %d, qaCnt: %d, qaSimilarCnt: %d,segmentCnt: %d, rejectCnt: %d, "+
			"total: %d > threshold: %d, "+
			"and current hour(%d) not in threshold hour(%d - %d)",
		app.ID, similarChunkCnt, qaCnt, qaSimilarCnt, segmentCnt, rejectCnt, total, e.p.RunNowThreshold,
		time.Now().Hour(), e.p.RunThresholdHourStart, e.p.RunThresholdHourEnd,
	)
	return false, nil
}

// createCache 无锁创建 embedding 缓存, 避免升级时间长导致应用长时间无法写入
func (e *EmbeddingUpgradeScheduler) createCache(ctx context.Context, app model.AppDB) error {
	// 相似
	// 拒答
	// 问答
	// 文档
	return nil
}

// buildIndex 创建检索库
func (e *EmbeddingUpgradeScheduler) buildIndex(ctx context.Context, app model.AppDB) error {
	log.InfoContextf(ctx, "buildIndex, appID: %d", app.ID)
	if err := e.buildRejectIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildRejectIndex fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	log.InfoContextf(ctx, "buildIndex(buildRejectIndex) success, appID: %d", app.ID)
	if err := e.buildQAIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildQAIndex fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	log.InfoContextf(ctx, "buildIndex(buildQAIndex) success, appID: %d", app.ID)
	if err := e.buildDocIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildDocIndex fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	log.InfoContextf(ctx, "buildIndex(buildDocIndex) success, appID: %d", app.ID)
	if err := e.buildRealTimeDocIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildRealTimeDocIndex fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	log.InfoContextf(ctx, "buildIndex(buildRealTimeDocIndex) success, appID: %d", app.ID)
	log.InfoContextf(ctx, "buildIndex all success, appID: %d", app.ID)
	return nil
}

func (e *EmbeddingUpgradeScheduler) cleanIndex(ctx context.Context, app model.AppDB) error {
	log.InfoContextf(ctx, "cleanIndex, appID: %d", app.ID)
	allIndexIds := []uint64{model.SimilarVersionID, model.ReviewVersionID, model.SegmentReviewVersionID,
		model.RejectedQuestionReviewVersionID, model.RealtimeSegmentVersionID, model.SegmentImageReviewVersionID,
		model.RealtimeSegmentImageVersionID}
	if err := e.retry(ctx, "DeleteVectorIndex", 10*time.Second, func(ctx context.Context) error {
		return e.dao.DeleteVectorIndex(ctx, app.ID, app.BusinessID, e.p.FromVer, model.EmptyEmbeddingModelName, allIndexIds)
	}); err != nil {
		log.WarnContextf(ctx, "cleanIndex fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	log.InfoContextf(ctx, "cleanIndex success, appID: %d", app.ID)
	return nil
}

func (e *EmbeddingUpgradeScheduler) retry(
	ctx context.Context, name string, timeout time.Duration, fn func(context.Context) error,
) (err error) {
	if e.p.RetryTimes > 0 {
		timeout = time.Duration(e.p.RetryInterval) * timeout
	}
	rCtx, cancel := context.WithTimeout(trpc.CloneContext(ctx), timeout)
	defer cancel()
	if err = fn(rCtx); err == nil {
		return nil
	}
	log.InfoContextf(rCtx, "run %s fail, err: %v", name, err)
	for i := 0; i < e.p.RetryTimes; i++ {
		if rCtx.Err() != nil {
			return rCtx.Err()
		}
		time.Sleep(time.Duration(e.p.RetryInterval) * time.Millisecond)
		log.InfoContextf(rCtx, "retry %s, %d time(s)", name, i)
		if err = fn(rCtx); err == nil {
			log.InfoContextf(rCtx, "retry %s, %d time(s), success", name, i)
			return nil
		}
		log.InfoContextf(rCtx, "retry %s, %d time(s), err: %v", name, i, err)
	}
	return err
}

// buildQAIndex TODO
// createSimilarIndex 创建 相似库 & 问答库
func (e *EmbeddingUpgradeScheduler) buildQAIndex(ctx context.Context, app model.AppDB) error {
	field := fmt.Sprintf("app_%d_qa", app.ID)
	offset, isExist, err := e.getOffset(ctx, field)
	if err != nil {
		log.WarnContextf(ctx, "get qa offset fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	if !isExist {
		offset = 0
		if err = e.dao.ReCreateVectorIndex(
			ctx, app.ID, model.SimilarVersionID, e.p.ToVer, app.BusinessID, 10*time.Second, model.EmptyEmbeddingModelName,
		); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(similar) fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if err = e.dao.ReCreateVectorIndex(
			ctx, app.ID, model.ReviewVersionID, e.p.ToVer, app.BusinessID, 10*time.Second, model.EmptyEmbeddingModelName,
		); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(qa) fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if err = e.setOffset(ctx, field, offset); err != nil {
			log.WarnContextf(ctx, "set qa offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
			return err
		}
		log.InfoContextf(ctx, "init qa offset success, appID: %d, offset: %d", app.ID, offset)
	}
	for {
		log.InfoContextf(ctx, "process qa, appID: %d, offset: %d", app.ID, offset)
		var qas []*model.DocQA
		if qas, err = e.dao.GetQAChunk(ctx, app.CorpID, app.ID, offset, e.p.ChunkSize); err != nil {
			log.WarnContextf(ctx, "GetQAChunk fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if len(qas) == 0 {
			log.InfoContextf(ctx, "process qa success, appID: %d, offset: %d", app.ID, offset)
			break
		}
		var labelAbles []model.LabelAble
		var qaIDs []uint64
		for _, qa := range qas {
			labelAbles = append(labelAbles, qa)
			qaIDs = append(qaIDs, qa.ID)
		}
		var qaAttributes []model.Attributes
		if qaAttributes, err = e.dao.GetAttributes(ctx, app.ID, labelAbles); err != nil {
			log.WarnContextf(ctx, "GetAttributes fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		// 查询相似问
		var sqsMap map[uint64][]*model.SimilarQuestionSimple
		if sqsMap, err = e.dao.GetSimilarQuestionsSimpleByQAIDs(ctx, app.CorpID, app.ID, qaIDs); err != nil {
			log.WarnContextf(ctx, "GetSimilarQuestionsCountByQAIDs fail, appID: %d, qaIDs:%+v, err: %v",
				app.ID, qaIDs, err)
			return err
		}
		batch := e.batch(ctx)
		qasGroup := slicex.Chunk(qas, batch)
		qaAttributesGroup := slicex.Chunk(qaAttributes, batch)
		x := 0
		for i, qaGroup := range qasGroup {
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(batch)
			for j, qa := range qaGroup {
				x++
				qa := qa
				j := j
				offset = qa.ID
				similarQuestions := sqsMap[qa.ID] // 标准问答对应的相似问
				g.Go(func() error {
					log.InfoContextf(gCtx, "process qa, appID: %d, qaID: %d", app.ID, qa.ID)
					return e.processQA(gCtx, qa, qaAttributesGroup[i][j], similarQuestions, app.BusinessID)
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(
				ctx, "save qa offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.ID, offset, x, len(qas), float64(x)*100/float64(len(qas)),
			)
			if err = e.setOffset(ctx, field, offset); err != nil {
				log.WarnContextf(ctx, "set qa offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
				return err
			}
		}
	}
	return nil
}

func (e *EmbeddingUpgradeScheduler) processQA(ctx context.Context, qa *model.DocQA, qaAttribute model.Attributes,
	similarQuestions []*model.SimilarQuestionSimple, botBizID uint64) error {
	if qa.IsExpire() || qa.IsCharExceeded() {
		log.InfoContextf(ctx, "qa is expire or exceeded, qaID: %d, ReleaseStatus:%d", qa.ID, qa.ReleaseStatus)
		return nil
	}
	qaLabels := qaAttribute.ToVectorLabels()
	if !qa.IsNotAccepted() { // 非不采纳(未校验/已采纳)进相似库
		req := &retrieval.AddVectorReq{
			RobotId:          qa.RobotID,
			IndexId:          model.SimilarVersionID,
			Id:               qa.ID,
			PageContent:      qa.Question,
			DocType:          model.DocTypeQA,
			EmbeddingVersion: e.p.ToVer,
			Labels:           qaLabels,
			ExpireTime:       qa.ExpireEnd.Unix(),
			BotBizId:         botBizID,
		}
		if err := e.retry(ctx, "add similar vector", 10*time.Second, func(ctx context.Context) error {
			_, err := e.dao.AddVector(ctx, req)
			return err
		}); err != nil {
			log.WarnContextf(ctx, "AddVector fail, req: %+v, err: %v", req, err)
			return err
		}
	}
	if qa.IsAccepted() { // 已采纳进 QA 库
		req := &retrieval.AddVectorReq{
			RobotId:          qa.RobotID,
			IndexId:          model.ReviewVersionID,
			Id:               qa.ID,
			PageContent:      qa.Question,
			DocType:          model.DocTypeQA,
			EmbeddingVersion: e.p.ToVer,
			Labels:           qaLabels,
			ExpireTime:       qa.ExpireEnd.Unix(),
			BotBizId:         botBizID,
		}
		if err := e.retry(ctx, "add qa vector", 10*time.Second, func(ctx context.Context) error {
			_, err := e.dao.AddVector(ctx, req)
			return err
		}); err != nil {
			log.WarnContextf(ctx, "AddVector fail, req: %+v, err: %v", req, err)
			return err
		}
	}
	// 添加相似问
	if len(similarQuestions) > 0 {
		for _, similarQuestion := range similarQuestions {
			req := &retrieval.AddVectorReq{
				RobotId:          qa.RobotID,
				IndexId:          model.ReviewVersionID,
				Id:               similarQuestion.SimilarID,
				PageContent:      similarQuestion.Question,
				DocType:          model.DocTypeQA,
				EmbeddingVersion: e.p.ToVer,
				Labels:           addSimilarQuestionVectorLabel(ctx, qaLabels, qa.ID),
				ExpireTime:       qa.ExpireEnd.Unix(),
				BotBizId:         botBizID,
			}
			log.DebugContextf(ctx, "add qa similar question vector req:%+v", req)
			if err := e.retry(ctx, "add qa similar question vector", 10*time.Second, func(ctx context.Context) error {
				_, err := e.dao.AddVector(ctx, req)
				return err
			}); err != nil {
				log.WarnContextf(ctx, "AddVector fail, req: %+v, err: %v", req, err)
				return err
			}
		}
	}
	return nil
}

// addSimilarQuestionVectorLabel 添加相似问标签
func addSimilarQuestionVectorLabel(ctx context.Context, labels []*retrieval.VectorLabel,
	qaId uint64) []*retrieval.VectorLabel {
	var newLabels []*retrieval.VectorLabel
	newLabels = append(newLabels, &retrieval.VectorLabel{Name: SysLabelQAFlagName, Value: SysLabelQAFlagValueSimilar})
	newLabels = append(newLabels, &retrieval.VectorLabel{Name: SysLabelQAIdName, Value: fmt.Sprintf("%d", qaId)})

	if len(labels) == 0 {
		return newLabels
	} else {
		labels = append(labels, newLabels...)
		return labels
	}
}

func (e *EmbeddingUpgradeScheduler) batch(ctx context.Context) int {
	h := time.Now().Hour()
	if h >= e.p.RunThresholdHourStart && h < e.p.RunThresholdHourEnd {
		return e.p.BatchTurbo
	}
	return e.p.Batch
}

// buildRejectIndex 创建拒答库
func (e *EmbeddingUpgradeScheduler) buildRejectIndex(ctx context.Context, app model.AppDB) error {
	field := fmt.Sprintf("app_%d_reject_qa", app.ID)
	offset, isExist, err := e.getOffset(ctx, field)
	if err != nil {
		log.WarnContextf(ctx, "get reject qa offset fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	if !isExist {
		offset = 0
		if err = e.dao.ReCreateVectorIndex(
			ctx, app.ID, model.RejectedQuestionReviewVersionID, e.p.ToVer, app.BusinessID, 10*time.Second, model.EmptyEmbeddingModelName,
		); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(reject qa) fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if err = e.setOffset(ctx, field, offset); err != nil {
			log.WarnContextf(ctx, "set reject qa offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
			return err
		}
		log.InfoContextf(ctx, "init reject qa offset success, appID: %d, offset: %d", app.ID, offset)
	}
	for {
		log.InfoContextf(ctx, "process reject qa, appID: %d, offset: %d", app.ID, offset)
		var rejects []*model.RejectedQuestion
		if rejects, err = e.dao.GetRejectChunk(ctx, app.CorpID, app.ID, offset, e.p.ChunkSize); err != nil {
			log.WarnContextf(ctx, "GetRejectChunk fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if len(rejects) == 0 {
			log.InfoContextf(ctx, "process reject qa success, appID: %d, offset: %d", app.ID, offset)
			break
		}
		batch := e.batch(ctx)
		rejectsGroup := slicex.Chunk(rejects, batch)
		x := 0
		for _, rejectGroup := range rejectsGroup {
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(batch)
			for _, reject := range rejectGroup {
				x++
				reject := reject
				offset = reject.ID
				g.Go(func() error {
					log.InfoContextf(ctx, "process reject qa, appID: %d, rejectID: %d", app.ID, reject.ID)
					return e.processReject(gCtx, *reject, app.BusinessID)
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(
				ctx, "save reject offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.ID, offset, x, len(rejects), float64(x)*100/float64(len(rejects)),
			)
			if err = e.setOffset(ctx, field, offset); err != nil {
				log.WarnContextf(ctx, "set reject offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
				return err
			}
		}
	}
	return nil
}

func (e *EmbeddingUpgradeScheduler) processReject(ctx context.Context, reject model.RejectedQuestion, botBizID uint64) error {
	req := &retrieval.AddVectorReq{
		RobotId:          reject.RobotID,
		IndexId:          model.RejectedQuestionReviewVersionID,
		Id:               reject.ID,
		PageContent:      reject.Question,
		DocType:          model.DocTypeRejectedQuestion,
		EmbeddingVersion: e.p.ToVer,
		BotBizId:         botBizID,
	}
	if err := e.retry(ctx, "add reject vector", 10*time.Second, func(ctx context.Context) error {
		_, err := e.dao.AddVector(ctx, req)
		return err
	}); err != nil {
		log.WarnContextf(ctx, "add reject vector fail, req: %+v, err: %v", req, err)
		return err
	}
	return nil
}

func (e *EmbeddingUpgradeScheduler) cacheKey() string {
	return fmt.Sprintf("task_embedding_upgrade_%d", e.task.ID)
}

func (e *EmbeddingUpgradeScheduler) getOffset(ctx context.Context, field string) (uint64, bool, error) {
	offset := uint64(0)
	isExist := false
	rr, err := redis.String(e.dao.RedisCli().Do(ctx, "HGET", e.cacheKey(), field))
	if err == nil {
		isExist = true
		if offset, err = strconv.ParseUint(rr, 10, 64); err != nil {
			log.WarnContextf(
				ctx, "get qa offset fail, key: %s, field: %s, redis reply: %s, err: %v",
				e.cacheKey(), field, rr, err,
			)
			return 0, false, err
		}
	} else if errors.Is(err, redis.ErrNil) {
		isExist = false
		offset = 0
	} else {
		log.WarnContextf(ctx, "get qa offset fail, key: %s, field: %s, err: %v", e.cacheKey(), field, err)
		return 0, false, err
	}
	return offset, isExist, nil
}

func (e *EmbeddingUpgradeScheduler) setOffset(ctx context.Context, field string, offset uint64) error {
	if _, err := e.dao.RedisCli().Do(ctx, "HSET", e.cacheKey(), field, fmt.Sprintf("%d", offset)); err != nil {
		log.WarnContextf(
			ctx, "set qa offset fail, key: %s, field: %s, offset: %d, err: %v", e.cacheKey(), field, offset, err,
		)
		return err
	}
	return nil
}

// buildDocIndex 创建文档库
func (e *EmbeddingUpgradeScheduler) buildDocIndex(ctx context.Context, app model.AppDB) error {
	field := fmt.Sprintf("app_%d_doc", app.ID)
	offset, isExist, err := e.getOffset(ctx, field)
	if err != nil {
		log.WarnContextf(ctx, "get doc offset fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	if !isExist {
		offset = 0
		if err = e.dao.ReCreateVectorIndex(ctx,
			app.ID, model.SegmentReviewVersionID, e.p.ToVer, app.BusinessID, 10*time.Second, model.EmptyEmbeddingModelName); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(doc) fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		// 创建图片向量库
		if err = e.dao.ReCreateVectorIndex(ctx,
			app.ID, model.SegmentImageReviewVersionID, e.p.ToVer, app.BusinessID, 10*time.Second, model.EmptyEmbeddingModelName); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(image) fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if err = e.setOffset(ctx, field, offset); err != nil {
			log.WarnContextf(ctx, "set doc offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
			return err
		}
		log.InfoContextf(ctx, "init doc offset success, appID: %d, offset: %d", app.ID, offset)
	}
	for {
		log.InfoContextf(ctx, "process doc, appID: %d, offset: %d", app.ID, offset)
		var segments []*model.DocSegment
		if segments, err = e.dao.GetSegmentChunk(ctx, app.CorpID, app.ID, offset, e.p.ChunkSize); err != nil {
			log.WarnContextf(ctx, "GetSegmentChunk fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if len(segments) == 0 {
			log.InfoContextf(ctx, "process doc success, appID: %d, offset: %d", app.ID, offset)
			break
		}
		var docIDs []uint64
		for _, v := range segments {
			docIDs = append(docIDs, v.DocID)
		}
		docs := make(map[uint64]*model.Doc)
		if docs, err = e.dao.GetDocByIDs(ctx, slicex.Unique(docIDs), app.ID); err != nil {
			log.WarnContextf(ctx, "GetDocByIDs fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		var labelAbles []model.LabelAble
		for _, v := range segments {
			labelAble, ok := docs[v.DocID]
			if !ok {
				log.ErrorContextf(ctx, "doc not found, segment: %+v, docID: %d", v, v.DocID)
				return errs.ErrDocNotFound
			}
			labelAbles = append(labelAbles, labelAble)
		}
		var docAttributes []model.Attributes
		if docAttributes, err = e.dao.GetAttributes(ctx, app.ID, labelAbles); err != nil {
			log.WarnContextf(ctx, "GetAttributes fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		batch, x := e.batch(ctx), 0
		segmentsGroup, docAttributesGroup := slicex.Chunk(segments, batch), slicex.Chunk(docAttributes, batch)
		for i, segmentGroup := range segmentsGroup {
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(batch)
			for j, segment := range segmentGroup {
				x++
				j, segment := j, segment
				offset = segment.ID
				doc := docs[segment.DocID]
				g.Go(func() error {
					log.InfoContextf(ctx, "process doc, appID: %d, segmentID: %d", app.ID, segment.ID)
					return e.processSegment(gCtx, *segment, *doc, docAttributesGroup[i][j], app.BusinessID)
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(ctx, "save segment offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.ID, offset, x, len(segments), float64(x)*100/float64(len(segments)))
			if err = e.setOffset(ctx, field, offset); err != nil {
				log.WarnContextf(ctx, "set segment offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
				return err
			}
		}
	}
	return nil
}

// buildRealTimeDocIndex 创建实时文档库
func (e *EmbeddingUpgradeScheduler) buildRealTimeDocIndex(ctx context.Context, app model.AppDB) error {
	field := fmt.Sprintf("app_%d_realtime", app.ID)
	offset, isExist, err := e.getOffset(ctx, field)
	if err != nil {
		log.WarnContextf(ctx, "get realtime doc offset fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	if !isExist {
		offset = 0
		if err = e.dao.ReCreateVectorIndex(ctx,
			app.ID, model.RealtimeSegmentVersionID, e.p.ToVer, app.BusinessID, 10*time.Second, model.EmptyEmbeddingModelName); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(realtime) fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		// 创建实时文档图片向量库
		if err = e.dao.ReCreateVectorIndex(ctx,
			app.ID, model.RealtimeSegmentImageVersionID, e.p.ToVer, app.BusinessID, 10*time.Second, model.EmptyEmbeddingModelName); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(image) fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if err = e.setOffset(ctx, field, offset); err != nil {
			log.WarnContextf(ctx, "set realtime doc offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
			return err
		}
		log.InfoContextf(ctx, "init realtime doc offset success, appID: %d, offset: %d", app.ID, offset)
	}
	for {
		log.InfoContextf(ctx, "process realtime doc, appID: %d, offset: %d", app.ID, offset)
		var realtimeSegments []*realtime.TRealtimeDocSegment
		// 分批查询实时文档
		if realtimeSegments, err = e.dao.GetRealTimeSegmentChunk(ctx, app.CorpID, app.ID, offset, e.p.ChunkSize); err != nil {
			log.WarnContextf(ctx, "GetRealTimeSegmentChunk fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if len(realtimeSegments) == 0 {
			log.InfoContextf(ctx, "process realtime doc success, appID: %d, offset: %d", app.ID, offset)
			break
		}

		batch, x := e.batch(ctx), 0
		segmentsGroup := slicex.Chunk(realtimeSegments, batch)
		for _, segmentGroup := range segmentsGroup {
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(batch)
			for _, realtimeSegment := range segmentGroup {
				x++
				realtimeSegmentTmp := realtimeSegment
				offset = realtimeSegmentTmp.ID
				g.Go(func() error {
					log.InfoContextf(ctx, "process doc, appID: %d, segmentID: %d", app.ID, realtimeSegmentTmp.SegmentID)
					return e.processRealTimeSegment(gCtx, realtimeSegmentTmp, app.BusinessID)
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(ctx, "save segment offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.ID, offset, x, len(realtimeSegments), float64(x)*100/float64(len(realtimeSegments)))
			if err = e.setOffset(ctx, field, offset); err != nil {
				log.WarnContextf(ctx, "set segment offset fail, appID: %d, offset: %d, err: %v", app.ID, offset, err)
				return err
			}
		}
	}
	return nil
}

func (e *EmbeddingUpgradeScheduler) processSegment(
	ctx context.Context, segment model.DocSegment, doc model.Doc, docAttribute model.Attributes, botBizID uint64,
) error {
	if doc.IsExpire() || doc.IsCharSizeExceeded() {
		log.InfoContextf(ctx, "segment is expire or exceeded, skip, segmentID: %d, docID:%d, status",
			segment.ID, doc.ID, doc.Status)
		return nil
	}
	if doc.Status == model.DocStatusCreateIndexFail {
		log.InfoContextf(ctx, "doc status(create index fail), skip, segmentID: %d", segment.ID)
		return nil
	}
	// 文档片段标签，添加文档ID标签
	labels := addDocIDLabel(docAttribute.ToVectorLabels(), doc.ID)
	log.DebugContextf(ctx, "processSegment labels:%+v|segmentID:%d|docID:%d", labels, segment.ID, doc.ID)
	req := &retrieval.AddVectorReq{
		RobotId:          segment.RobotID,
		IndexId:          model.SegmentReviewVersionID,
		Id:               segment.ID,
		PageContent:      segment.PageContent, // 检索使用 PageContent
		DocType:          model.DocTypeSegment,
		EmbeddingVersion: e.p.ToVer,
		Labels:           labels,
		ExpireTime:       doc.ExpireEnd.Unix(),
		Type:             retrieval.KnowledgeType_KNOWLEDGE,
		BotBizId:         botBizID,
	}
	if err := e.retry(ctx, "add segment vector", 10*time.Second, func(ctx context.Context) error {
		_, err := e.dao.AddVector(ctx, req)
		return err
	}); err != nil {
		log.WarnContextf(ctx, "add segment vector fail, req: %+v, err: %v", req, err)
		return err
	}
	return nil
}

func addDocIDLabel(labels []*retrieval.VectorLabel, docID uint64) []*retrieval.VectorLabel {
	labels = append(labels, &retrieval.VectorLabel{
		Name:  model.SysLabelDocID,
		Value: strconv.FormatUint(docID, 10),
	})
	return labels
}

// genRealTimeSegmentLabels 生成实时文档切片的标签
func (e *EmbeddingUpgradeScheduler) genRealTimeSegmentLabels(
	realtimeSegment *realtime.TRealtimeDocSegment) []*retrieval.VectorLabel {
	labels := make([]*retrieval.VectorLabel, 0)
	labels = append(labels, &retrieval.VectorLabel{
		Name:  dao.RealtimeSessionIDLabel,
		Value: realtimeSegment.SessionID,
	})
	labels = append(labels, &retrieval.VectorLabel{
		Name:  dao.RealtimeDocIDLabel,
		Value: strconv.FormatUint(realtimeSegment.DocID, 10),
	})
	return labels
}

// processRealTimeSegment 处理实时文档切片
func (e *EmbeddingUpgradeScheduler) processRealTimeSegment(ctx context.Context,
	realtimeSegment *realtime.TRealtimeDocSegment, botBizID uint64) error {
	// 实时文档检索标签
	labels := e.genRealTimeSegmentLabels(realtimeSegment)
	req := &retrieval.AddVectorReq{
		RobotId:          realtimeSegment.RobotID,
		IndexId:          model.RealtimeSegmentVersionID, // 实时文档向量库
		Id:               realtimeSegment.SegmentID,
		PageContent:      realtimeSegment.PageContent, // 检索使用 PageContent
		DocType:          model.DocTypeSegment,
		EmbeddingVersion: e.p.ToVer,
		Labels:           labels,
		ExpireTime:       0,
		Type:             retrieval.KnowledgeType_REALTIME,
		BotBizId:         botBizID,
	}
	if err := e.retry(ctx, "add realtime segment vector", 10*time.Second, func(ctx context.Context) error {
		_, err := e.dao.AddVector(ctx, req)
		return err
	}); err != nil {
		log.WarnContextf(ctx, "add realtime segment vector fail, req: %+v, err: %v", req, err)
		return err
	}
	return nil
}

// lockApp 锁定应用写入
func (e *EmbeddingUpgradeScheduler) lockApp(ctx context.Context, app model.AppDB) error {
	log.InfoContextf(ctx, "lockApp, appID: %d", app.ID)
	if err := client.StartEmbeddingUpgradeApp(ctx, app.BusinessID, e.p.FromVer, e.p.ToVer); err != nil {
		log.WarnContextf(ctx, "lockApp fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	log.InfoContextf(ctx, "lockApp success, appID: %d", app.ID)
	return nil
}

// unlockApp 解除应用锁定
func (e *EmbeddingUpgradeScheduler) unlockApp(ctx context.Context, app model.AppDB) error {
	log.InfoContextf(ctx, "unlockApp, appID: %d", app.ID)
	if err := client.FinishEmbeddingUpgradeApp(ctx, app.BusinessID, e.p.FromVer, e.p.ToVer); err != nil {
		log.WarnContextf(ctx, "unlockApp fail, appID: %d, err: %v", app.ID, err)
		return err
	}
	log.InfoContextf(ctx, "unlockApp success, appID: %d", app.ID)
	return nil
}

// func (e *EmbeddingUpgradeScheduler) initAppIndex(ctx context.Context, apps []model.Robot) error {
// 	// 重建目标版本知识库
// 	for _, app := range apps {
// 		if err := e.retry(
// 			ctx, "DeleteAllVectorIndex", 30*time.Second,
// 			func(ctx context.Context) error {
// 				return e.dao.DeleteAllVectorIndex(ctx, app.ID, e.p.ToVer)
// 			},
// 		); err != nil {
// 			return err
// 		}
// 		if err := e.retry(
// 			ctx, "CreateAllVectorIndex", 30*time.Second,
// 			func(ctx context.Context) error {
// 				return e.dao.CreateAllVectorIndex(ctx, app.ID, e.p.ToVer)
// 			},
// 		); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// Done .
func (e *EmbeddingUpgradeScheduler) Done(ctx context.Context) error {
	return nil
}

// Fail .
func (e *EmbeddingUpgradeScheduler) Fail(ctx context.Context) error {
	return nil
}

// Stop .
func (e *EmbeddingUpgradeScheduler) Stop(ctx context.Context) error {
	return nil
}
