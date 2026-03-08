package task

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicKnowledgeBase "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_base"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	pbknowledge "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"golang.org/x/sync/errgroup"
	"strconv"
	"time"
)

// UpdateEmbeddingModelScheduler 切换embedding模型
type UpdateEmbeddingModelScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    *model.UpdateEmbeddingModelParams
}

func initChangeEmbeddingModelScheduler() {
	task_scheduler.Register(
		model.UpdateEmbeddingModelTask,
		func(t task_scheduler.Task, params model.UpdateEmbeddingModelParams) task_scheduler.TaskHandler {
			return &UpdateEmbeddingModelScheduler{
				dao:  dao.New(),
				task: t,
				p:    &params,
			}
		},
	)
}

// Prepare .
func (c *UpdateEmbeddingModelScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, c.p.Language)
	log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler Prepare")
	app, err := knowClient.GetAppInfo(ctx, c.p.AppBizID, model.AppTestScenes)
	if err != nil {
		log.WarnContextf(ctx, "UpdateEmbeddingModelScheduler GetAppInfo err:%+v", err)
		return nil, err
	}
	if app == nil || app.GetIsDelete() {
		err = errors.New("UpdateEmbeddingModelScheduler GetAppInfo app is nil or is deleted")
		log.WarnContextf(ctx, "app:%+v err:%+v", app, err)
		return nil, err
	}

	ctx = pkg.WithSpaceID(ctx, app.SpaceId)

	// 将该应用下所有指定状态（待发布、已发布）的知识（文档、问答、数据库）变更为学习中状态
	filter := &dao.DocFilter{
		RouterAppBizID: c.p.AppBizID,
		RobotId:        app.GetId(),
		Status:         []uint32{model.DocStatusWaitRelease, model.DocStatusReleaseSuccess},
		IsDeleted:      pkg.GetIntPtr(dao.IsNotDeleted),
	}
	updateColumns := []string{dao.DocTblColStatus}
	doc := &model.Doc{
		Status: model.DocStatusCreatingIndex,
	}
	rowsAffected, err := dao.GetDocDao().UpdateDoc(ctx, updateColumns, filter, doc)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler Prepare UpdateDoc fail, err:%+v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler Prepare UpdateDoc rowsAffected:%d", rowsAffected)

	c.p.AppInfo = app
	kv := make(task_scheduler.TaskKV)
	key := strconv.FormatUint(app.GetId(), 10)
	kv[key] = ""

	return kv, nil
}

// Init .
func (c *UpdateEmbeddingModelScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, c.p.Language)
	log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler Init")
	return nil
}

// Process .
func (c *UpdateEmbeddingModelScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler Process")
	for k := range progress.TaskKV(ctx) {
		log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler change embedding model appId:%s", k)
		t0 := time.Now()
		appID, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler appId:%s ParseUint err:%+v", k, err)
			return err
		}
		if err = c.upgrade(ctx, appID, func(ctx context.Context) error {
			if err = progress.Finish(ctx, k); err != nil {
				return err
			}
			log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler appId:%s success, cost:%d",
				k, int(time.Since(t0).Seconds()))
			return nil
		}); err != nil {
			log.WarnContextf(ctx, "UpdateEmbeddingModelScheduler appId:%s err: %v", k, err)
			return err
		}
	}
	return nil
}

// upgrade 单应用升级
func (c *UpdateEmbeddingModelScheduler) upgrade(ctx context.Context, appID uint64, done func(ctx context.Context) error) error {
	// 检查是否能升级
	upgradeNow, err := c.upgradeNow(ctx, c.p.AppInfo)
	if err != nil {
		log.WarnContextf(ctx, "upgrade upgradeNow fail, appID: %d, err: %v", appID, err)
		return err
	}
	if !upgradeNow {
		log.WarnContext(ctx, "upgrade ")
		return nil
	}
	// 写入锁定
	if err = c.lockApp(ctx, c.p.AppInfo); err != nil {
		log.WarnContextf(ctx, "lockApp fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 创建新库
	if err = c.buildIndex(ctx, c.p.AppInfo); err != nil {
		log.WarnContextf(ctx, "buildIndex fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 解除锁定
	if err = c.unlockApp(ctx, c.p.AppInfo); err != nil {
		log.WarnContextf(ctx, "unlockApp fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 清理老库
	if err = c.cleanIndex(ctx, c.p.AppInfo); err != nil {
		log.WarnContextf(ctx, "cleanIndex fail, appID: %d, err: %v", appID, err)
		return err
	}
	// 标记完成
	return done(ctx)
}

// upgradeNow 检查当前是否允许升级
func (c *UpdateEmbeddingModelScheduler) upgradeNow(ctx context.Context, app *admin.GetAppInfoRsp) (bool, error) {
	log.InfoContextf(ctx, "check if upgradeNow, appID: %d", app.GetId())
	// 检查是否有执行中的任务, 如果有执行中的任务不允许升级
	tasks, err := dao.GetTasksByAppID(ctx, app.GetId())
	if err != nil {
		log.WarnContextf(ctx, "GetTasksByAppID fail, appID: %d, err: %v", app.GetId(), err)
		return false, err
	}
	if len(tasks) > 0 {
		for _, task := range tasks {
			// 排除已经终止的任务
			if task.Runner == "terminated" || task.Type == model.UpdateEmbeddingModelTask {
				continue
			} else {
				log.InfoContextf(
					ctx,
					"upgradeNow(no), appID: %d, taskID:%d, %d tasks still running",
					app.GetId(), task.ID, len(tasks),
				)
				return false, nil
			}
		}

	}
	// 检查是否有恢复中的文档，有恢复中的文档则先不升级，等恢复完成后再升级，避免恢复过程中文档只升级部分切片
	resumeDocCount, err := c.dao.GetResumeDocCount(ctx, app.GetCorpId(), app.GetId())
	if err != nil {
		log.WarnContextf(ctx, "GetResumeDocCount fail, appID: %d, err: %v", app.GetId(), err)
		return false, err
	}
	if resumeDocCount > 0 {
		log.InfoContextf(ctx, "upgradeNow(no), doc is resuming, appID: %d, resumeDocCount:%d", app.GetId(), resumeDocCount)
		return false, nil
	}
	return true, nil
}

// buildIndex 创建检索库
func (c *UpdateEmbeddingModelScheduler) buildIndex(ctx context.Context, app *admin.GetAppInfoRsp) error {
	log.InfoContextf(ctx, "buildIndex, appID: %d", app.GetId())
	if err := c.buildRejectIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildRejectIndex fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	log.InfoContextf(ctx, "buildIndex(buildRejectIndex) success, appID: %d", app.GetId())
	if err := c.buildQAIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildQAIndex fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	log.InfoContextf(ctx, "buildIndex(buildQAIndex) success, appID: %d", app.GetId())
	if err := c.buildDocIndex(ctx, app); err != nil {
		log.WarnContextf(ctx, "buildDocIndex fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	log.InfoContextf(ctx, "buildIndex(buildDocIndex) success, appID: %d", app.GetId())
	//if err := c.buildDBIndex(ctx, app); err != nil {
	//	log.WarnContextf(ctx, "buildDBIndex fail, appID: %d, err: %v", app.GetId(), err)
	//	return err
	//}
	log.InfoContextf(ctx, "buildIndex(buildDBIndex) success, appID: %d", app.GetId())
	log.InfoContextf(ctx, "buildIndex all success, appID: %d", app.GetId())
	return nil
}

func (c *UpdateEmbeddingModelScheduler) cleanIndex(ctx context.Context, app *admin.GetAppInfoRsp) error {
	log.InfoContextf(ctx, "cleanIndex, appID: %d", app.GetId())
	indexIds := []uint64{model.SimilarVersionID, model.ReviewVersionID, model.SegmentReviewVersionID,
		model.RejectedQuestionReviewVersionID, model.SegmentImageReviewVersionID}
	if err := c.retry(ctx, "DeleteVectorIndex", 10*time.Second, func(ctx context.Context) error {
		return c.dao.DeleteVectorIndex(ctx, app.GetId(), app.GetAppBizId(),
			c.p.EmbeddingModelUpdateInfo.OldModelVersion, c.p.EmbeddingModelUpdateInfo.OldModelName, indexIds)
	}); err != nil {
		log.WarnContextf(ctx, "cleanIndex fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	log.InfoContextf(ctx, "cleanIndex success, appID: %d", app.GetId())
	return nil
}

func (c *UpdateEmbeddingModelScheduler) retry(
	ctx context.Context, name string, timeout time.Duration, fn func(context.Context) error,
) (err error) {
	if c.p.RetryTimes > 0 {
		timeout = time.Duration(c.p.RetryInterval) * timeout
	}
	rCtx, cancel := context.WithTimeout(trpc.CloneContext(ctx), timeout)
	defer cancel()
	if err = fn(rCtx); err == nil {
		return nil
	}
	log.InfoContextf(rCtx, "run %s fail, err: %v", name, err)
	for i := 0; i < c.p.RetryTimes; i++ {
		if rCtx.Err() != nil {
			return rCtx.Err()
		}
		time.Sleep(time.Duration(c.p.RetryInterval) * time.Millisecond)
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
func (c *UpdateEmbeddingModelScheduler) buildQAIndex(ctx context.Context, app *admin.GetAppInfoRsp) error {
	field := fmt.Sprintf("app_%d_qa", app.GetId())
	offset, isExist, err := c.getOffset(ctx, field)
	if err != nil {
		log.WarnContextf(ctx, "get qa offset fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	if !isExist {
		offset = 0
		if err = c.dao.ReCreateVectorIndex(
			ctx, app.GetId(), model.SimilarVersionID, c.p.EmbeddingModelUpdateInfo.NewModelVersion, app.GetAppBizId(),
			10*time.Second, c.p.EmbeddingModelUpdateInfo.NewModelName,
		); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(similar) fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		if err = c.dao.ReCreateVectorIndex(
			ctx, app.GetId(), model.ReviewVersionID, c.p.EmbeddingModelUpdateInfo.NewModelVersion, app.GetAppBizId(),
			10*time.Second, c.p.EmbeddingModelUpdateInfo.NewModelName,
		); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(qa) fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		if err = c.setOffset(ctx, field, offset); err != nil {
			log.WarnContextf(ctx, "set qa offset fail, appID: %d, offset: %d, err: %v", app.GetId(), offset, err)
			return err
		}
		log.InfoContextf(ctx, "init qa offset success, appID: %d, offset: %d", app.GetId(), offset)
	}
	for {
		log.InfoContextf(ctx, "process qa, appID: %d, offset: %d", app.GetId(), offset)
		var qas []*model.DocQA
		if qas, err = c.dao.GetQAChunk(ctx, app.GetCorpId(), app.GetId(), offset, c.p.ChunkSize); err != nil {
			log.WarnContextf(ctx, "GetQAChunk fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		if len(qas) == 0 {
			log.InfoContextf(ctx, "process qa success, appID: %d, offset: %d", app.GetId(), offset)
			break
		}
		var labelAbles []model.LabelAble
		var qaIDs []uint64
		for _, qa := range qas {
			labelAbles = append(labelAbles, qa)
			qaIDs = append(qaIDs, qa.ID)
		}
		var qaAttributes []model.Attributes
		if qaAttributes, err = c.dao.GetAttributes(ctx, app.GetId(), labelAbles); err != nil {
			log.WarnContextf(ctx, "GetAttributes fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		// 查询相似问
		var sqsMap map[uint64][]*model.SimilarQuestionSimple
		if sqsMap, err = c.dao.GetSimilarQuestionsSimpleByQAIDs(ctx, app.GetCorpId(), app.GetId(), qaIDs); err != nil {
			log.WarnContextf(ctx, "GetSimilarQuestionsCountByQAIDs fail, appID: %d, qaIDs:%+v, err: %v",
				app.GetId(), qaIDs, err)
			return err
		}
		batch := c.p.Batch
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
					log.InfoContextf(gCtx, "process qa, appID: %d, qaID: %d", app.GetId(), qa.ID)
					return c.processQA(gCtx, qa, qaAttributesGroup[i][j], similarQuestions, app.GetAppBizId())
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(
				ctx, "save qa offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.GetId(), offset, x, len(qas), float64(x)*100/float64(len(qas)),
			)
			if err = c.setOffset(ctx, field, offset); err != nil {
				log.WarnContextf(ctx, "set qa offset fail, appID: %d, offset: %d, err: %v", app.GetId(), offset, err)
				return err
			}
		}
	}
	return nil
}

func (c *UpdateEmbeddingModelScheduler) processQA(ctx context.Context, qa *model.DocQA, qaAttribute model.Attributes,
	similarQuestions []*model.SimilarQuestionSimple, botBizID uint64) error {
	if qa.IsExpire() || qa.IsCharExceeded() {
		log.InfoContextf(ctx, "qa is expire or exceeded, qaID: %d, ReleaseStatus:%d", qa.ID, qa.ReleaseStatus)
		return nil
	}
	qaLabels := qaAttribute.ToVectorLabels()
	if !qa.IsNotAccepted() { // 非不采纳(未校验/已采纳)进相似库
		req := &retrieval.AddVectorReq{
			RobotId:            qa.RobotID,
			IndexId:            model.SimilarVersionID,
			Id:                 qa.ID,
			PageContent:        qa.Question,
			DocType:            model.DocTypeQA,
			EmbeddingVersion:   c.p.EmbeddingModelUpdateInfo.NewModelVersion,
			Labels:             qaLabels,
			ExpireTime:         qa.ExpireEnd.Unix(),
			BotBizId:           botBizID,
			EmbeddingModelName: c.p.EmbeddingModelUpdateInfo.NewModelName,
		}
		if err := c.retry(ctx, "add similar vector", 10*time.Second, func(ctx context.Context) error {
			_, err := c.dao.AddVector(ctx, req)
			return err
		}); err != nil {
			log.WarnContextf(ctx, "AddVector fail, req: %+v, err: %v", req, err)
			return err
		}
	}
	if qa.IsAccepted() { // 已采纳进 QA 库
		req := &retrieval.AddVectorReq{
			RobotId:            qa.RobotID,
			IndexId:            model.ReviewVersionID,
			Id:                 qa.ID,
			PageContent:        qa.Question,
			DocType:            model.DocTypeQA,
			EmbeddingVersion:   c.p.EmbeddingModelUpdateInfo.NewModelVersion,
			Labels:             qaLabels,
			ExpireTime:         qa.ExpireEnd.Unix(),
			BotBizId:           botBizID,
			EmbeddingModelName: c.p.EmbeddingModelUpdateInfo.NewModelName,
		}
		if err := c.retry(ctx, "add qa vector", 10*time.Second, func(ctx context.Context) error {
			_, err := c.dao.AddVector(ctx, req)
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
				RobotId:            qa.RobotID,
				IndexId:            model.ReviewVersionID,
				Id:                 similarQuestion.SimilarID,
				PageContent:        similarQuestion.Question,
				DocType:            model.DocTypeQA,
				EmbeddingVersion:   c.p.EmbeddingModelUpdateInfo.NewModelVersion,
				Labels:             c.addSimilarQuestionVectorLabel(ctx, qaLabels, qa.ID),
				ExpireTime:         qa.ExpireEnd.Unix(),
				BotBizId:           botBizID,
				EmbeddingModelName: c.p.EmbeddingModelUpdateInfo.NewModelName,
			}
			log.DebugContextf(ctx, "add qa similar question vector req:%+v", req)
			if err := c.retry(ctx, "add qa similar question vector", 10*time.Second, func(ctx context.Context) error {
				_, err := c.dao.AddVector(ctx, req)
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
func (c *UpdateEmbeddingModelScheduler) addSimilarQuestionVectorLabel(ctx context.Context, labels []*retrieval.VectorLabel,
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

// buildRejectIndex 创建拒答库
func (c *UpdateEmbeddingModelScheduler) buildRejectIndex(ctx context.Context, app *admin.GetAppInfoRsp) error {
	field := fmt.Sprintf("app_%d_reject_qa", app.GetId())
	offset, isExist, err := c.getOffset(ctx, field)
	if err != nil {
		log.WarnContextf(ctx, "get reject qa offset fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	if !isExist {
		offset = 0
		if err = c.dao.ReCreateVectorIndex(
			ctx, app.GetId(), model.RejectedQuestionReviewVersionID, c.p.EmbeddingModelUpdateInfo.NewModelVersion,
			app.GetAppBizId(), 10*time.Second, c.p.EmbeddingModelUpdateInfo.NewModelName,
		); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(reject qa) fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		if err = c.setOffset(ctx, field, offset); err != nil {
			log.WarnContextf(ctx, "set reject qa offset fail, appID: %d, offset: %d, err: %v", app.GetId(), offset, err)
			return err
		}
		log.InfoContextf(ctx, "init reject qa offset success, appID: %d, offset: %d", app.GetId(), offset)
	}
	for {
		log.InfoContextf(ctx, "process reject qa, appID: %d, offset: %d", app.GetId(), offset)
		var rejects []*model.RejectedQuestion
		if rejects, err = c.dao.GetRejectChunk(ctx, app.GetCorpId(), app.GetId(), offset, c.p.ChunkSize); err != nil {
			log.WarnContextf(ctx, "GetRejectChunk fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		if len(rejects) == 0 {
			log.InfoContextf(ctx, "process reject qa success, appID: %d, offset: %d", app.GetId(), offset)
			break
		}
		batch := c.p.Batch
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
					log.InfoContextf(ctx, "process reject qa, appID: %d, rejectID: %d", app.GetId(), reject.ID)
					return c.processReject(gCtx, *reject, app.GetAppBizId())
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(
				ctx, "save reject offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.GetId(), offset, x, len(rejects), float64(x)*100/float64(len(rejects)),
			)
			if err = c.setOffset(ctx, field, offset); err != nil {
				log.WarnContextf(ctx, "set reject offset fail, appID: %d, offset: %d, err: %v", app.GetId(), offset, err)
				return err
			}
		}
	}
	return nil
}

func (c *UpdateEmbeddingModelScheduler) processReject(ctx context.Context, reject model.RejectedQuestion, botBizID uint64) error {
	req := &retrieval.AddVectorReq{
		RobotId:          reject.RobotID,
		IndexId:          model.RejectedQuestionReviewVersionID,
		Id:               reject.ID,
		PageContent:      reject.Question,
		DocType:          model.DocTypeRejectedQuestion,
		EmbeddingVersion: c.p.EmbeddingModelUpdateInfo.NewModelVersion,
		BotBizId:         botBizID,
	}
	if err := c.retry(ctx, "add reject vector", 10*time.Second, func(ctx context.Context) error {
		_, err := c.dao.AddVector(ctx, req)
		return err
	}); err != nil {
		log.WarnContextf(ctx, "add reject vector fail, req: %+v, err: %v", req, err)
		return err
	}
	return nil
}

func (c *UpdateEmbeddingModelScheduler) cacheKey() string {
	return fmt.Sprintf("change_embedding_model_%d", c.task.ID)
}

func (c *UpdateEmbeddingModelScheduler) getOffset(ctx context.Context, field string) (uint64, bool, error) {
	offset := uint64(0)
	isExist := false
	rr, err := redis.String(c.dao.RedisCli().Do(ctx, "HGET", c.cacheKey(), field))
	if err == nil {
		isExist = true
		if offset, err = strconv.ParseUint(rr, 10, 64); err != nil {
			log.WarnContextf(
				ctx, "get qa offset fail, key: %s, field: %s, redis reply: %s, err: %v",
				c.cacheKey(), field, rr, err,
			)
			return 0, false, err
		}
	} else if errors.Is(err, redis.ErrNil) {
		isExist = false
		offset = 0
	} else {
		log.WarnContextf(ctx, "get qa offset fail, key: %s, field: %s, err: %v", c.cacheKey(), field, err)
		return 0, false, err
	}
	return offset, isExist, nil
}

func (c *UpdateEmbeddingModelScheduler) setOffset(ctx context.Context, field string, offset uint64) error {
	if _, err := c.dao.RedisCli().Do(ctx, "HSET", c.cacheKey(), field, fmt.Sprintf("%d", offset)); err != nil {
		log.WarnContextf(
			ctx, "set qa offset fail, key: %s, field: %s, offset: %d, err: %v", c.cacheKey(), field, offset, err,
		)
		return err
	}
	return nil
}

// buildDocIndex 创建文档库
func (c *UpdateEmbeddingModelScheduler) buildDocIndex(ctx context.Context, app *admin.GetAppInfoRsp) error {
	field := fmt.Sprintf("app_%d_doc", app.GetId())
	offset, isExist, err := c.getOffset(ctx, field)
	if err != nil {
		log.WarnContextf(ctx, "get doc offset fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	if !isExist {
		offset = 0
		if err = c.dao.ReCreateVectorIndex(ctx,
			app.GetId(), model.SegmentReviewVersionID, c.p.EmbeddingModelUpdateInfo.NewModelVersion, app.GetAppBizId(),
			10*time.Second, c.p.EmbeddingModelUpdateInfo.NewModelName); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(doc) fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		// 创建图片向量库
		if err = c.dao.ReCreateVectorIndex(ctx,
			app.GetId(), model.SegmentImageReviewVersionID, c.p.EmbeddingModelUpdateInfo.NewModelVersion,
			app.GetAppBizId(), 10*time.Second, c.p.EmbeddingModelUpdateInfo.NewModelName); err != nil {
			log.WarnContextf(ctx, "ReCreateVectorIndex(image) fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		if err = c.setOffset(ctx, field, offset); err != nil {
			log.WarnContextf(ctx, "set doc offset fail, appID: %d, offset: %d, err: %v", app.GetId(), offset, err)
			return err
		}
		log.InfoContextf(ctx, "init doc offset success, appID: %d, offset: %d", app.GetId(), offset)
	}
	for {
		log.InfoContextf(ctx, "process doc, appID: %d, offset: %d", app.GetId(), offset)
		var segments []*model.DocSegment
		if segments, err = c.dao.GetSegmentChunk(ctx, app.GetCorpId(), app.GetId(), offset, c.p.ChunkSize); err != nil {
			log.WarnContextf(ctx, "GetSegmentChunk fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		if len(segments) == 0 {
			log.InfoContextf(ctx, "process doc success, appID: %d, offset: %d", app.GetId(), offset)
			break
		}
		var docIDs []uint64
		for _, v := range segments {
			docIDs = append(docIDs, v.DocID)
		}
		docs := make(map[uint64]*model.Doc)
		if docs, err = c.dao.GetDocByIDs(ctx, slicex.Unique(docIDs), app.GetId()); err != nil {
			log.WarnContextf(ctx, "GetDocByIDs fail, appID: %d, err: %v", app.GetId(), err)
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
		if docAttributes, err = c.dao.GetAttributes(ctx, app.GetId(), labelAbles); err != nil {
			log.WarnContextf(ctx, "GetAttributes fail, appID: %d, err: %v", app.GetId(), err)
			return err
		}
		batch := c.p.Batch
		x := 0
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
					log.InfoContextf(ctx, "process doc, appID: %d, segmentID: %d", app.GetId(), segment.ID)
					return c.processSegment(gCtx, *segment, *doc, docAttributesGroup[i][j], app.GetAppBizId())
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(ctx, "save segment offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.GetId(), offset, x, len(segments), float64(x)*100/float64(len(segments)))
			if err = c.setOffset(ctx, field, offset); err != nil {
				log.WarnContextf(ctx, "set segment offset fail, appID: %d, offset: %d, err: %v", app.GetId(), offset, err)
				return err
			}
		}
	}
	return nil
}

func (c *UpdateEmbeddingModelScheduler) processSegment(
	ctx context.Context, segment model.DocSegment, doc model.Doc, docAttribute model.Attributes, botBizID uint64,
) error {
	if doc.IsExpire() || doc.IsCharSizeExceeded() {
		log.InfoContextf(ctx, "segment is expire or exceeded, skip, segmentID:%d, docID:%d, status:%d",
			segment.ID, doc.ID, doc.Status)
		return nil
	}
	if doc.Status == model.DocStatusCreateIndexFail {
		log.InfoContextf(ctx, "doc status(create index fail), skip, segmentID: %d", segment.ID)
		return nil
	}
	// 文档片段标签，添加文档ID标签
	labels := c.addDocIDLabel(docAttribute.ToVectorLabels(), doc.ID)
	log.DebugContextf(ctx, "processSegment labels:%+v|segmentID:%d|docID:%d", labels, segment.ID, doc.ID)
	req := &retrieval.AddVectorReq{
		RobotId:            segment.RobotID,
		IndexId:            model.SegmentReviewVersionID,
		Id:                 segment.ID,
		PageContent:        segment.PageContent, // 检索使用 PageContent
		DocType:            model.DocTypeSegment,
		EmbeddingVersion:   c.p.EmbeddingModelUpdateInfo.NewModelVersion,
		Labels:             labels,
		ExpireTime:         doc.ExpireEnd.Unix(),
		Type:               retrieval.KnowledgeType_KNOWLEDGE,
		BotBizId:           botBizID,
		EmbeddingModelName: c.p.EmbeddingModelUpdateInfo.NewModelName,
	}
	if err := c.retry(ctx, "add segment vector", 10*time.Second, func(ctx context.Context) error {
		_, err := c.dao.AddVector(ctx, req)
		return err
	}); err != nil {
		log.WarnContextf(ctx, "add segment vector fail, req: %+v, err: %v", req, err)
		return err
	}
	return nil
}

func (c *UpdateEmbeddingModelScheduler) buildDBIndex(ctx context.Context, app *admin.GetAppInfoRsp) error {
	field := fmt.Sprintf("app_%d_db", app.GetId())
	offset, isExist, err := c.getOffset(ctx, field)
	if err != nil {
		return fmt.Errorf("get db offset fail, appID: %d, err: %w", app.GetId(), err)
	}
	if !isExist {
		offset = 0
		if err = c.dao.ReCreateVectorIndex(
			ctx, app.GetId(), model.DbSourceVersionID, c.p.EmbeddingModelUpdateInfo.NewModelVersion,
			app.GetAppBizId(), 10*time.Second, c.p.EmbeddingModelUpdateInfo.NewModelName,
		); err != nil {
			return fmt.Errorf("ReCreateVectorIndex(db) fail, appID: %d, err: %w", app.GetId(), err)
		}
		if err = c.setOffset(ctx, field, offset); err != nil {
			return fmt.Errorf("set db offset fail, appID: %d, offset: %d, err: %w", app.GetId(), offset, err)
		}
		log.InfoContextf(ctx, "init db offset success, appID: %d, offset: %d", app.GetId(), offset)
	}

	dbSourceIDs, listSourceErr := dao.GetDBSourceDao().GetDbSourceBizIdByAppBizID(ctx, app.GetCorpBizId(), app.GetAppBizId())
	if listSourceErr != nil {
		return fmt.Errorf("GetDbSourceBizIdByAppBizID fail, appID: %d, err: %w", app.GetId(), listSourceErr)
	}
	var dbTableBizIDs []uint64
	for _, dbSourceID := range dbSourceIDs {
		tables, listTableErr := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, app.GetCorpBizId(), app.GetAppBizId(), dbSourceID)
		if listTableErr != nil {
			return fmt.Errorf("ListAllByDBSourceBizID failed: appID %d, err %w", app.GetId(), listTableErr)
		}
		for _, table := range tables {
			dbTableBizIDs = append(dbTableBizIDs, table.DBTableBizID)
		}
	}
	robotID := c.p.AppInfo.GetId()
	appDB, appErr := c.dao.GetAppByID(ctx, robotID)
	if appErr != nil {
		return fmt.Errorf("get app info from db failed: %w", appErr)
	}
	embedConf, _, embedErr := appDB.GetEmbeddingConf()
	if embedErr != nil {
		return fmt.Errorf("get app embed info from db failed: %w", embedErr)
	}
	log.InfoContextf(ctx, "process db for app %d: get %d db sources and %d tables in total, embedConf %v", app.GetId(), len(dbSourceIDs), len(dbTableBizIDs), embedConf)

	for {
		log.InfoContextf(ctx, "process db, appID: %d, offset: %d", app.GetId(), offset)

		var currentTableIDs []uint64
		if int(offset) < len(dbTableBizIDs) {
			dbTableBizIDs = dbTableBizIDs[offset:]
		}
		pageSize := int(c.p.ChunkSize)
		if pageSize < len(dbTableBizIDs) {
			currentTableIDs = dbTableBizIDs[:pageSize]
			dbTableBizIDs = dbTableBizIDs[pageSize:]
			offset += uint64(pageSize)
		}
		if len(currentTableIDs) == 0 {
			break
		}
		var topValues []*model.DbTableTopValue
		for _, tableID := range currentTableIDs {
			values, valuesErr := dao.GetDBSourceDao().GetTopValuesByDbTableBizID(ctx, app.GetCorpBizId(), app.GetAppBizId(), tableID)
			if valuesErr != nil {
				log.WarnContextf(ctx, "GetTopValuesByDbTableBizID fail, appID: %d, err: %v", app.GetId(), valuesErr)
				return valuesErr
			}
			topValues = append(topValues, values...)
		}
		if len(topValues) == 0 {
			break
		}

		batchSize, processed := c.p.Batch, 0
		topValuesGroup := slicex.Chunk(topValues, batchSize)
		for _, tvs := range topValuesGroup {
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(batchSize)
			for _, tv := range tvs {
				processed++
				g.Go(func() error {
					log.InfoContextf(gCtx, "process db: appID=%d, dbSourceID=%d, dbTableID=%d, dbTableColumnID=%d, topValueID=%d",
						app.GetId(), tv.DBSourceBizID, tv.DbTableBizID, tv.DbTableColumnBizID, tv.ID)
					content := fmt.Sprintf("%v;%v;%v", tv.ColumnName, tv.ColumnComment, tv.ColumnValue)
					return c.retry(gCtx, "add db vector", 10*time.Second, func(c context.Context) error {
						return dao.GetDBSourceDao().AddVdb(c, robotID, app.GetAppBizId(), tv.BusinessID, embedConf.Version, content, retrieval.EnvType_Prod)
					})
				})
			}
			if err = g.Wait(); err != nil {
				return fmt.Errorf("execute process db of appID %d failed: %w", app.GetId(), err)
			}

			log.InfoContextf(ctx, "save db offset, appID: %d, offset: %d, %d/%d(%.2f%%)",
				app.GetId(), offset, processed, len(dbTableBizIDs), float64(processed)*100/float64(len(dbTableBizIDs)))
			if err = c.setOffset(ctx, field, offset); err != nil {
				return fmt.Errorf("set db offset fail, appID: %d, offset: %d, err: %v", app.GetId(), offset, err)
			}
		}
	}
	return nil
}

func (c *UpdateEmbeddingModelScheduler) addDocIDLabel(labels []*retrieval.VectorLabel, docID uint64) []*retrieval.VectorLabel {
	labels = append(labels, &retrieval.VectorLabel{
		Name:  model.SysLabelDocID,
		Value: strconv.FormatUint(docID, 10),
	})
	return labels
}

// lockApp 锁定应用写入
func (c *UpdateEmbeddingModelScheduler) lockApp(ctx context.Context, app *admin.GetAppInfoRsp) error {
	log.InfoContextf(ctx, "lockApp, appID: %d", app.GetId())
	// embedding模型切换场景，填固定值
	if err := knowClient.StartEmbeddingUpgradeApp(ctx, app.GetAppBizId(), c.p.EmbeddingModelUpdateInfo.OldModelVersion,
		c.p.EmbeddingModelUpdateInfo.NewModelVersion); err != nil {
		log.WarnContextf(ctx, "lockApp fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	log.InfoContextf(ctx, "lockApp success, appID: %d", app.GetId())
	return nil
}

// unlockApp 解除应用锁定
func (c *UpdateEmbeddingModelScheduler) unlockApp(ctx context.Context, app *admin.GetAppInfoRsp) error {
	log.InfoContextf(ctx, "unlockApp, appID: %d", app.GetId())
	// embedding模型切换场景，填固定值
	if err := knowClient.FinishEmbeddingUpgradeApp(ctx, app.GetAppBizId(), c.p.EmbeddingModelUpdateInfo.OldModelVersion,
		c.p.EmbeddingModelUpdateInfo.NewModelVersion); err != nil {
		log.WarnContextf(ctx, "unlockApp fail, appID: %d, err: %v", app.GetId(), err)
		return err
	}
	log.InfoContextf(ctx, "unlockApp success, appID: %d", app.GetId())
	return nil
}

// Done .
func (c *UpdateEmbeddingModelScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler Done")
	// 将该应用下所有学习中的知识（文档、问答、数据库）变更为待发布状态，next_action置为修改
	// 1.文档
	err := c.UpdateDoc(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDoc fail, err=%+v", err)
		return err
	}

	// 文档片段
	err = c.UpdateDocSegment(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDocSegment fail, err=%+v", err)
		return err
	}

	// 2.TODO:问答
	err = c.UpdateDocQa(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDocQa fail, err=%+v", err)
		return err
	}

	// 3.TODO:数据库

	// 设置embedding model切换处理中标记
	err = logicKnowledgeBase.RemoveProcessingFlags(ctx, c.p.AppInfo.GetCorpBizId(), []uint64{c.p.AppBizID},
		[]pbknowledge.KnowledgeBaseInfo_ProcessingFlag{pbknowledge.KnowledgeBaseInfo_EMBEDDING_MODEL_CHANGING})
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler RemoveProcessingFlags fail, err=%+v", err)
		return err
	}

	return nil
}

// Fail .
func (c *UpdateEmbeddingModelScheduler) Fail(ctx context.Context) error {
	return nil
}

// Stop .
func (c *UpdateEmbeddingModelScheduler) Stop(ctx context.Context) error {
	return nil
}

// UpdateDoc 更新文档状态
func (c *UpdateEmbeddingModelScheduler) UpdateDoc(ctx context.Context) error {
	// 将所有next_action为已发布状的更新成修改，其他next_action的保持不变即可
	filter := &dao.DocFilter{
		RouterAppBizID: c.p.AppBizID,
		RobotId:        c.p.AppInfo.GetId(),
		NextActions:    []uint32{model.DocNextActionPublish},
		IsDeleted:      pkg.GetIntPtr(dao.IsNotDeleted),
	}
	updateColumns := []string{dao.DocTblColNextAction}
	doc := &model.Doc{
		NextAction: model.DocNextActionUpdate,
	}
	rowsAffected, err := dao.GetDocDao().UpdateDoc(ctx, updateColumns, filter, doc)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDoc fail, err:%+v", err)
		return err
	}

	// 将所有学习中状态的文档更新为待发布状态
	filter = &dao.DocFilter{
		RouterAppBizID: c.p.AppBizID,
		RobotId:        c.p.AppInfo.GetId(),
		Status:         []uint32{model.DocStatusCreatingIndex},
		IsDeleted:      pkg.GetIntPtr(dao.IsNotDeleted),
	}
	updateColumns = []string{dao.DocTblColStatus}
	doc = &model.Doc{
		Status: model.DocStatusWaitRelease,
	}
	rowsAffected, err = dao.GetDocDao().UpdateDoc(ctx, updateColumns, filter, doc)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDoc fail, err:%+v", err)
		return err
	}

	log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDoc success, rowsAffected:%d", rowsAffected)
	return nil
}

// UpdateDocSegment 更新文档片段信息
func (c *UpdateEmbeddingModelScheduler) UpdateDocSegment(ctx context.Context) error {
	filter := &dao.DocSegmentFilter{
		RouterAppBizID:  c.p.AppBizID,
		RobotID:         c.p.AppInfo.GetId(),
		ReleaseStatuses: []uint32{model.SegmentReleaseStatusSuccess},
	}
	updateColumns := []string{dao.DocSegmentTblColReleaseStatus}
	docSegment := &model.DocSegment{
		ReleaseStatus: model.DocStatusWaitRelease,
	}
	rowsAffected, err := dao.GetDocSegmentDao().UpdateDocSegment(ctx, updateColumns, filter, docSegment)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDocSegment fail, err:%+v", err)
		return err
	}
	log.InfoContextf(ctx, "UpdateEmbeddingModelScheduler UpdateDocSegment success, rowsAffected:%d", rowsAffected)
	return nil
}

// UpdateDocQa 更新问答状态
func (c *UpdateEmbeddingModelScheduler) UpdateDocQa(ctx context.Context) error {
	filter := &dao.DocQaFilter{
		RobotId:       c.p.AppInfo.GetId(),
		IsDeleted:     pkg.GetIntPtr(dao.IsNotDeleted),
		ReleaseStatus: []uint32{model.DocStatusReleaseSuccess},
	}
	updateColumns := []string{dao.DocQaTblColReleaseStatus}
	docQA := &model.DocQA{
		ReleaseStatus: model.DocStatusWaitRelease,
	}
	if err := dao.GetDocQaDao().UpdateDocQas(ctx, updateColumns, filter, docQA); err != nil {
		return fmt.Errorf("UpdateEmbeddingModelScheduler UpdateDocQa fail, err:%+v", err)
	}
	return nil
}
