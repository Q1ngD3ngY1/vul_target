package vector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/util"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/dao/category"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	kbDao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	qaDao "git.woa.com/adp/kb/kb-config/internal/dao/qa"
	segDao "git.woa.com/adp/kb/kb-config/internal/dao/segment"
	userDao "git.woa.com/adp/kb/kb-config/internal/dao/user"
	vecDao "git.woa.com/adp/kb/kb-config/internal/dao/vector"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	vecEntity "git.woa.com/adp/kb/kb-config/internal/entity/vector"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	kbPb "git.woa.com/adp/pb-go/kb/kb_config"
	retrievalPb "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"gorm.io/gorm"
)

const syncChanSize = 100000

type vectorSyncItem struct {
	ctx context.Context
	id  uint64
}

var (
	// 系统标签(for相似问场景)
	sysLabelQAFlagName         = "_sys_str_qa_flag"
	sysLabelQAFlagValueSimilar = "similar"        // 标识相似问
	sysLabelQAIdName           = "_sys_str_qa_id" // 标识相似问的qaId

	vectorSyncChan = make(chan *vectorSyncItem, syncChanSize)
)

type GetBotBizIDByIDFunc func(ctx context.Context, robotID uint64) (uint64, error)

type VectorSyncLogic struct {
	rawSqlDao dao.Dao
	kbDao     kbDao.Dao
	docDao    docDao.Dao
	labelDao  label.Dao
	qaDao     qaDao.Dao
	userDao   userDao.Dao
	segDao    segDao.Dao
	vecDao    vecDao.Dao
	cateDao   category.Dao
	rpc       *rpc.RPC
}

func (v *VectorSyncLogic) GetDao() vecDao.Dao {
	return v.vecDao
}

func NewLogic(rawSqlDao dao.Dao,
	docDao docDao.Dao,
	labelDao label.Dao,
	kbDao kbDao.Dao,
	qaDao qaDao.Dao,
	segDao segDao.Dao,
	userDao userDao.Dao,
	vecDao vecDao.Dao,
	cateDao category.Dao,
	retrievalRPC *rpc.RPC) *VectorSyncLogic {
	return &VectorSyncLogic{
		rawSqlDao: rawSqlDao,
		docDao:    docDao,
		labelDao:  labelDao,
		kbDao:     kbDao,
		qaDao:     qaDao,
		segDao:    segDao,
		userDao:   userDao,
		vecDao:    vecDao,
		cateDao:   cateDao,
		rpc:       retrievalRPC,
	}
}

// Push 新增向量同步队列
func (v *VectorSyncLogic) Push(ctx context.Context, id uint64) {
	select {
	case <-ctx.Done():
	case vectorSyncChan <- &vectorSyncItem{ctx: contextx.CloneWithoutTimeout(ctx), id: id}:
		logx.D(ctx, "chan add wating sync id:%d", id)
	default:
		logx.I(ctx, "failed to add wating sync id:%d", id)
	}
}

// BatchPush 批量新增向量同步队列
func (v *VectorSyncLogic) BatchPush(ctx context.Context, ids []uint64) {
	for _, id := range ids {
		v.Push(ctx, id)
	}
}

// DoSync 执行同步
func (v *VectorSyncLogic) DoSync() {
	ctx := context.Background()
	go func() {
		defer gox.Recover()
		for {
			v.fetchNotSuccessSync(ctx)
			time.Sleep(10 * time.Second)
		}
	}()
	g := errgroupx.New()
	g.SetLimit(config.App().VectorSync.Concurrent)
	for item := range vectorSyncChan {
		logx.D(ctx, "[syncVector]len(vectorSyncChan):%d", len(vectorSyncChan))
		if item.id == 0 {
			continue
		}
		syncItem := item
		g.Go(func() error {
			v.dealOneSync(syncItem.ctx, syncItem.id)
			return nil
		})
	}
	_ = g.Wait()
}

func (v *VectorSyncLogic) fetchNotSuccessSync(ctx context.Context) {
	logx.I(ctx, "[syncVectorQA]fetchNotSuccessSync start (at %s)", time.Now().String())
	notSuccessSyns, err := v.vecDao.FetchNotSuccessSync(ctx)
	if err != nil {
		logx.E(ctx, "Failed to FetchNotSuccessSync err:%+v", err)
		return
	}

	if len(notSuccessSyns) == 0 {
		logx.I(ctx, "[syncVectorQA]not success vector sync not found!")
		return
	}
	ids := make([]uint64, 0)
	for _, s := range notSuccessSyns {
		logx.I(ctx, "[syncVectorQA]fetchNotSuccessSync id:%d,type:%d,status:%d,try_times:%d,related_id:%d",
			s.ID, s.Type, s.Status, s.TryTimes, s.RelateID)
		ids = append(ids, s.ID)
	}

	updateFields := map[string]any{
		"status":      vecEntity.StatusSyncInit,
		"update_time": time.Now(),
	}

	if err := v.vecDao.BatchUpdateVectorSync(ctx, notSuccessSyns, updateFields); err != nil {
		logx.E(ctx, "[syncVector]Failed to BatchUpdateVectorSyncSync err:%+v", err)
		return
	}

	for _, id := range ids {
		vectorSyncChan <- &vectorSyncItem{ctx: ctx, id: id}
	}
}

func (v *VectorSyncLogic) dealOneSync(ctx context.Context, id uint64) {
	logx.D(ctx, "[syncVector]dealOneSync id:%d", id)
	isLock := v.lockOneSync(ctx, id)
	logx.D(ctx, "[syncVector]dealOneSync id:%d,isLock:%t", id, isLock)
	if !isLock {
		logx.D(ctx, "[syncVector]dealOneSync id:%d,is not lock， skip to deal", id)
		return
	}
	row, err := v.getOneSync(ctx, id)
	if err != nil {
		logx.E(ctx, "[syncVector]dealOneSync id:%d,getOneSync err:%+v", id, err)
		return
	}

	if row.IsVectorTypeQA() {
		err = v.syncVectorQA(ctx, row)
	}
	if row.IsVectorTypeSegment() {
		err = v.syncVectorSegment(ctx, row)
	}
	if row.IsVectorTypeRejectedQuestion() {
		err = v.syncVectorRejectedQuestion(ctx, row)
	}
	row.Status = vecEntity.StatusSyncSuccess
	row.Result = ""
	if err != nil {
		logx.E(ctx, "[syncVector]dealOneSync err:%+v (sync_id:%d, write_sync_id:%d)", err, row.ID, row.WriteSyncId)
		row.Status = vecEntity.StatusSyncFailed
		row.Result = err.Error()
	}
	v.updateOneSync(ctx, row)
	logx.I(ctx, "[syncVector]dealOneSync res: status:%d id:%d,type:%d,,try_times:%d,related_id:%d,write_sync_id:%d",
		row.Status, id, row.Type, row.TryTimes, row.RelateID, row.WriteSyncId)
}

func (v *VectorSyncLogic) lockOneSync(ctx context.Context, id uint64) bool {
	isLock := false
	updateCols := map[string]any{
		"status": vecEntity.StatusSyncing,
	}
	rows, err := v.vecDao.UpdateAndLockVectorSync(ctx, id, vecEntity.StatusSyncInit, updateCols)
	if err != nil {
		logx.E(ctx, "[syncVector]Failed to UpdateAndLockVectorSync err:%+v", err)
		return isLock
	}
	if rows == 1 {
		isLock = true
	}
	return isLock
}

func (v *VectorSyncLogic) getOneSync(ctx context.Context, id uint64) (*vecEntity.VectorSync, error) {
	logx.I(ctx, "[syncVector]getOneSync id:%d", id)
	row, err := v.vecDao.GetVectorSyncBySyncId(ctx, id)
	if err != nil {
		logx.E(ctx, "[syncVector]Failed to GetVectorSync err:%+v", err)
		return nil, err
	}
	return row, nil
}

func (v *VectorSyncLogic) updateOneSync(ctx context.Context, row *vecEntity.VectorSync) {
	logx.I(ctx, "[syncVector]updateOneSync row:%+v", row)
	row.UpdateTime = time.Now()
	row.TryTimes += 1
	logx.D(ctx, "[syncVector]updateOneSync id:%d type:%d status:%d", row.ID, row.Type, row.Status)
	if err := v.qaDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		if row.Status == vecEntity.StatusSyncFailed {
			_, err := v.vecDao.UpdateVectorSync(ctx, row.ID, map[string]any{
				"status":      row.Status,
				"result":      row.Result,
				"update_time": row.UpdateTime,
				"try_times":   row.TryTimes,
				"request":     row.Request,
			})
			if err != nil {
				logx.E(ctx, "[syncVector]Failed to UpdateVectorSync err:%+v", err)
				return err
			}

			err = v.updateQaAndSimilarStatusIfLearnFail(ctx, row)
			if err != nil {
				logx.E(ctx, "[syncVector]Failed to update QA and similar status err:%+v", err)
				return err
			}
		}
		if row.Status == vecEntity.StatusSyncSuccess {
			err := v.vecDao.BatchCreateVectorSyncHistory(ctx, []*vecEntity.VectorSync{row})
			if err != nil {
				logx.E(ctx, "[syncVector]Failed to BatchCreateVectorSyncHistory err:%+v", err)
				return err
			}

			err = v.vecDao.DeleteVectorSync(ctx, row.ID)
			if err != nil {
				logx.E(ctx, "[syncVector]Failed to DeleteVectorSync err:%+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "[syncVector]Failed to updateOneSync row:%+v err:%+v", row, err)
	}
}

func (v *VectorSyncLogic) ExtractEmbeddingModelOfKB(ctx context.Context, corpBizID uint64, robot *entity.App) (string, error) {
	logx.D(ctx, "ExtractEmbeddingModelOfKB (knowledgeBizId:%d)", robot.BizId)

	var kbConfig *kbe.KnowledgeConfig

	if robot.IsShared {
		kbConfigs, err := v.kbDao.GetShareKnowledgeConfigs(ctx, corpBizID, []uint64{robot.BizId},
			[]uint32{uint32(kbPb.KnowledgeBaseConfigType_EMBEDDING_MODEL)})
		if err != nil {
			logx.W(ctx, "ExtractEmbeddingModelOfKB (knowledgeBizId:%d) err:%+v",
				robot.BizId, err)
			return "", err
		}
		if len(kbConfigs) > 0 {
			kbConfig = kbConfigs[0]
		}
	} else {
		kbConfigs, err := v.kbDao.DescribeAppKnowledgeConfig(ctx, corpBizID, robot.BizId, robot.BizId)
		if err != nil {
			logx.W(ctx, "ExtractEmbeddingModelOfKB (knowledgeBizId:%d) err:%+v",
				robot.BizId, err)
			return "", err
		}
		if len(kbConfigs) > 0 {
			embeddingConfig := slicex.Filter(kbConfigs, func(item *kbe.KnowledgeConfig) bool {
				return item.Type == uint32(kbPb.KnowledgeBaseConfigType_EMBEDDING_MODEL)
			})
			if len(embeddingConfig) > 0 {
				kbConfig = embeddingConfig[0]
			}
		}
	}

	if kbConfig != nil {
		// qa学习的过程在默认知识库一定是评测端的配置
		embeddingConfig := kbConfig.PreviewConfig
		if robot.IsShared {
			embeddingConfig = kbConfig.Config
		}

		return v.CompactEmbeddingModel(ctx, embeddingConfig), nil
	}

	return "", nil
}

func (v *VectorSyncLogic) CompactEmbeddingModel(ctx context.Context, config string) string {
	if config == "" {
		return ""
	}

	res := &kbPb.EmbeddingModel{}
	if config != "" && jsonx.Valid([]byte(config)) {
		err := jsonx.Unmarshal([]byte(config), res)
		if err != nil {
			logx.W(ctx, "ConvertStr2EmbeddingModelConfigItem jsonx.Unmarshal err: %+v", err)
			return ""
		}
		return res.GetModelName()
	}
	return config
}

func (v *VectorSyncLogic) syncVectorQA(ctx context.Context, row *vecEntity.VectorSync) error {
	logx.D(ctx, "[syncVector][syncVectorQA] row:%+v (sync_id:%d, write_sync_id:%d)", row, row.ID, row.WriteSyncId)
	if !row.IsAllowSync() {
		logx.D(ctx, "[syncVector][syncVectorQA] row:%+v (sync_id:%d, write_sync_id:%d) IsNotAllowSync",
			row, row.ID, row.WriteSyncId)
		return nil
	}

	qa, err := v.qaDao.GetQAByID(ctx, row.RelateID)
	if err != nil {
		logx.E(ctx, "[syncVector][syncVectorQA] Failed to GetQAByID err:%+v (sync_id:%d, write_sync_id:%d)",
			err, row.ID, row.WriteSyncId)
		return err
	}
	logx.D(ctx, "[syncVector][syncVectorQA] qa:%d (sync_id:%d, write_sync_id:%d)", qa.ID, row.ID, row.WriteSyncId)

	robot, err := v.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, qa.RobotID)
	if err != nil {
		logx.E(ctx, "[syncVector][syncVectorQA] Failed to DescribeAppByPrimaryIdWithoutNotFoundError err:%+v", err)
		return err
	}

	if robot.HasDeleted() {
		logx.D(ctx, "[syncVector][syncVectorQA] app has deleted, appID:%d(sync_id:%d, write_sync_id:%d)",
			qa.RobotID, row.ID, row.WriteSyncId)
		return nil
	}

	logx.D(ctx, "[syncVector][syncVectorQA] app_biz_id:%d (sync_id:%d, write_sync_id:%d)", robot.BizId, row.ID, row.WriteSyncId)

	embeddingVersion := robot.Embedding.Version
	embeddingModel := ""
	embeddingModel, err = v.ExtractEmbeddingModelOfKB(ctx, robot.CorpBizId, robot)
	if err != nil {
		logx.E(ctx, "[syncVector][syncVectorQA] Failed to ExtractEmbeddingModelOfKB err:%+v", err)
		return err
	}
	if embeddingModel != "" {
		embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
	}
	logx.I(ctx, "[syncVector][syncVectorQA] syncVectorQA for"+
		" embeddingModelName:%s, using embeddingVersion:%d (sync_id:%d, write_sync_id:%d)",
		embeddingModel, embeddingVersion, row.ID, row.WriteSyncId)
	vectorLabels, err := v.GetQAVectorLabels(ctx, robot.BizId, qa)
	if err != nil {
		logx.E(ctx, "[syncVector][syncVectorQA] Failed to getQAVectorLabels err:%+v", err)
		return err
	}
	if qa.IsResuming() { // qa的状态处于超量恢复中，不需要写向量，等到状态恢复成待发布之后，才需要写向量。
		logx.D(ctx, "[syncVector][syncVectorQA] qa is resuming, qaID:%d", qa.ID)
		return nil
	}

	logx.D(ctx, "[syncVector][syncVectorQA] start to sync vector for qa %d (sync_id:%d, write_sync_id:%d)",
		qa.ID, row.ID, row.WriteSyncId)

	oldQa := *qa
	if row.ExtendedId == 0 {

		// 更新QA的场景
		if qa.IsDelete() || qa.IsNotAccepted() || qa.IsCharExceeded() {
			logx.D(ctx, "[syncVector][syncVectorQA] deleteQASimilar, qaID:%d, status:%d", qa.ID, qa.Status)
			newCtx := util.SetMultipleMetaData(ctx, robot.SpaceId, robot.Uin)
			if err = v.deleteQASimilar(newCtx, row, qa, embeddingVersion, embeddingModel); err != nil {
				return err
			}

			logx.D(ctx, "[syncVector][syncVectorQA] deleteQAKnowledge, qaID:%d", qa.ID)
			if err = v.deleteQAKnowledge(newCtx, row, qa, embeddingVersion, embeddingModel); err != nil {
				return err
			}
			// feature_permission
			// 删除角色问答绑定关系,每次删除一万条
			logx.D(ctx, "[syncVector][syncVectorQA] feature_permission deleteRoleQa appBizId:%v,qaBizId:%v", robot.BizId, qa.BusinessID)
			filter := &entity.KnowledgeRoleQAFilter{
				KnowledgeBizIDs: []uint64{robot.BizId},
				QABizIDs:        []uint64{qa.BusinessID},
				BatchSize:       10000,
			}
			err := v.userDao.DeleteKnowledgeRoleQAList(ctx, 0, 0, filter, nil)
			if err != nil { // 柔性放过
				logx.E(ctx, "[syncVector][syncVectorQA] feature_permission deleteRoleQA err:%v,appBizId:%v,qaBizId:%v",
					err, robot.BizId, qa.BusinessID)
			}
		} else {
			logx.D(ctx, "[syncVector][syncVectorQA] addQASimilar, qaID:%d, status:%d", qa.ID, qa.Status)
			newCtx := util.SetMultipleMetaData(ctx, robot.SpaceId, robot.Uin)
			if err = v.addQASimilar(newCtx, row, qa, embeddingVersion, embeddingModel, vectorLabels); err != nil {
				logx.E(ctx, "[syncVector][syncVectorQA] Failed to addQASimilar err:%+v", err)
				return err
			}

			logx.D(ctx, "[syncVector][syncVectorQA] addQAKnowledge, qaID:%d, status:%d", qa.ID, qa.Status)
			if err = v.addQAKnowledge(newCtx, row, qa, embeddingVersion, embeddingModel, vectorLabels); err != nil {
				logx.E(ctx, "[syncVector][syncVectorQA] Failed to addQAKnowledge err:%+v", err)
				return err
			}
		}
	} else { // 更新QA相似问的场景(QAType下, extendedId为相似问ID)
		logx.I(ctx, "[syncVector][syncVectorQA],QA with similarQA, qa_id: %d, similar_id: %d", row.RelateID, row.ExtendedId)

		sim, err := v.qaDao.GetSimilarQuestionBySimilarID(ctx, row.RelateID, row.ExtendedId)

		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			logx.E(ctx, "[syncVector][syncVectorQA],Failed to GetSimilarQuestionBySimilarID  err:%+v (syncId:%d, relatedId:%d)",
				err, row.ID, row.RelateID)
			return err
		}
		if sim != nil {
			logx.D(ctx, "[syncVector][syncVectorQA],GetSimilarQuestionBySimilarID  sim:%+v (syncId:%d, relatedId:%d)",
				sim, row.ID, row.RelateID)
			// 同步到vector时，是按照docTypeQA的流程来，需要更新为相似问的Id和Question
			qa.ID = sim.SimilarID
			qa.Question = sim.Question
			// 问答是否停用，如果停用过期时间需要设为当前
			if qa.IsDelete() || qa.IsNotAccepted() || qa.IsCharExceeded() || sim.IsDelete() {
				newCtx := util.SetMultipleMetaData(ctx, robot.SpaceId, robot.Uin)
				if err = v.deleteQAKnowledge(newCtx, row, qa, embeddingVersion, embeddingModel); err != nil {
					return err
				}
			} else {
				vectorLabels = v.addSimilarQuestionVectorLabel(ctx, vectorLabels, sim.RelatedQAID)
				logx.D(ctx, "[syncVector]vectorLabels: %+v", vectorLabels)
				newCtx := util.SetMultipleMetaData(ctx, robot.SpaceId, robot.Uin)
				if err = v.addQAKnowledge(newCtx, row, qa, embeddingVersion, embeddingModel, vectorLabels); err != nil {
					return err
				}
			}
		}
	}
	err = v.updateQaAndSimilarStatus(ctx, &oldQa, row)

	if err != nil {
		logx.E(ctx, "[syncVector][syncVectorQA] Failed to updateQaAndSimilarStatus err:%+v (syncId:%d, relatedId:%d)",
			err, row.ID, row.RelateID)
		return err
	}
	logx.D(ctx, "[syncVector][syncVectorQA] done. (syncId:%d, relatedId:%d, write_sync_id:%d)",
		row.ID, row.RelateID, row.WriteSyncId)
	return nil
}

func (v *VectorSyncLogic) syncVectorSegment(ctx context.Context, row *vecEntity.VectorSync) error {
	logx.D(ctx, "[syncVector]syncVectorSegment row:%+v", row)
	if !row.IsAllowSync() {
		return nil
	}
	seg, err := v.segDao.GetDocSegmentByID(ctx, 0, row.RelateID)
	if err != nil {
		logx.E(ctx, "[syncVector][syncVectorSegment],Failed to GetDocSegemtnByID err:%+v", err)
		return err
	}

	robot, err := v.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, seg.RobotID)
	if err != nil {
		logx.E(ctx, "[syncVector][syncVectorSegment],Failed to DescribeAppByPrimaryIdWithoutNotFoundError err:%+v", err)
		return err
	}

	if robot.HasDeleted() {
		logx.D(ctx, "[syncVector][syncVectorSegment],App has deleted, appID:%d", seg.RobotID)
		return nil
	}
	embeddingVersion := robot.Embedding.Version
	if seg.IsDelete() {
		return v.deleteSegmentVector(ctx, row, seg, embeddingVersion)
	}
	// 从doc表里面同步切片的有效期
	err = v.syncSegmentExpireTime(ctx, seg)
	if err != nil {
		return err
	}
	return v.addSegmentVector(ctx, row, seg, embeddingVersion)
}

func (v *VectorSyncLogic) syncVectorRejectedQuestion(ctx context.Context, row *vecEntity.VectorSync) error {
	logx.D(ctx, "[syncVectorQA.syncVectorRejectedQuestion] row:%+v (sync_id:%d, write_sync_id:%d)",
		row, row.ID, row.WriteSyncId)
	if !row.IsAllowSync() {
		return nil
	}

	rejectQuestion, err := v.qaDao.GetRejectedQuestionByID(ctx, row.RelateID)

	if err != nil {
		logx.E(ctx, "[syncVector.syncVectorRejectedQuestion],Failedc to GetRejectedQuestionByID err:%+v "+
			"(sync_id:%d, write_sync_id:%d)", err, row.ID, row.WriteSyncId)
		return err
	}
	robot, err := v.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, rejectQuestion.RobotID)
	if err != nil {
		logx.E(ctx, "[syncVector.syncVectorRejectedQuestion],Failed to DescribeAppByPrimaryIdWithoutNotFoundError err:%+v", err)
		return err
	}
	if robot.HasDeleted() {
		logx.D(ctx, "[syncVector.syncVectorRejectedQuestion],App has been deleted. appID:%d, "+
			"(sync_id:%d, write_sync_id:%d)", rejectQuestion.RobotID, row.ID, row.WriteSyncId)
		return nil
	}

	// 拒答不属于知识问答的一种，所以还是用默认的embedding模型.
	embeddingVersion := robot.Embedding.Version
	embeddingModel := ""

	logx.I(ctx, "[syncVector][syncVectorRejectedQuestion] syncVectorRejectedQuestion for shared kg "+
		" embeddingModelName:%s, using embeddingVersion:%d (sync_id:%d, write_sync_id:%d)",
		embeddingModel, embeddingVersion, row.ID, row.WriteSyncId)
	if rejectQuestion.IsDelete() {
		err = v.deleteRejectedQuestionVector(ctx, row, rejectQuestion, embeddingVersion, embeddingModel)
	} else {
		err = v.addRejectedQuestionVector(ctx, row, rejectQuestion, embeddingVersion, embeddingModel)
	}
	if err != nil {
		logx.E(ctx, "[syncVector.syncVectorRejectedQuestion]Failed. err:%+v "+
			"(sync_id:%d, write_sync_id:%d)", err, row.ID, row.WriteSyncId)
		return err
	}
	logx.I(ctx, "[syncVector.syncVectorRejectedQuestion] done. (sync_id:%d, write_sync_id:%d)",
		row.ID, row.WriteSyncId)
	return nil
}

// addQASimilar
func (v *VectorSyncLogic) addQASimilar(
	ctx context.Context, row *vecEntity.VectorSync, qa *qaEntity.DocQA, embeddingVersion uint64, embeddingModel string,
	vectorLabels []*retrievalPb.VectorLabel) error {
	logx.I(ctx, "[syncVector]addQASimilar vector, qa:%+v embeddingVersion:%d embeddingName:%s",
		qa, embeddingVersion, embeddingModel)
	req := retrievalPb.BatchAddKnowledgeReq{
		RobotId:            qa.RobotID,
		IndexId:            entity.SimilarVersionID,
		DocType:            entity.DocTypeQA,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		IsVector:           true,
		Knowledge: []*retrievalPb.KnowledgeData{{
			Id:          qa.ID,
			PageContent: qa.Question,
			Labels:      vectorLabels,
			ExpireTime:  qa.GetExpireTime(),
		}},
	}
	row.SetRequest(&req)
	return v.addVector(ctx, &req)
}

// addQAKnowledge 新增问答知识
func (v *VectorSyncLogic) addQAKnowledge(
	ctx context.Context, row *vecEntity.VectorSync, qa *qaEntity.DocQA, embeddingVersion uint64, embeddingModel string,
	vectorLabels []*retrievalPb.VectorLabel) error {
	if !qa.IsAccepted() {
		logx.I(ctx, "[syncVector]addQAKnowledge qa is not accepted ignore, qa:%+v", qa)
		return nil
	}
	botBizID, err := v.rawSqlDao.GetBotBizIDByID(ctx, qa.RobotID)
	if err != nil {
		logx.E(ctx, "[syncVector][addKnowledge] Failed.|getBotBizIDByID|err:%v", err)
		return err
	}
	req := retrievalPb.BatchAddKnowledgeReq{
		RobotId:            qa.RobotID,
		IndexId:            entity.ReviewVersionID,
		DocType:            entity.DocTypeQA,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		Knowledge: []*retrievalPb.KnowledgeData{{
			Id:          qa.ID,
			DocId:       qa.DocID,
			PageContent: qa.Question,
			Labels:      vectorLabels,
			ExpireTime:  qa.GetExpireTime(),
		}},
		BotBizId: botBizID,
		IsVector: false,
		Type:     retrievalPb.KnowledgeType_KNOWLEDGE,
		// EnvType:            0,
	}
	row.SetRequest(&req)
	rsp, err := v.rpc.RetrievalDirectIndex.BatchAddKnowledge(ctx, &req)
	if err != nil {
		logx.E(ctx, "[syncVectorQA][addKnowledge] Failed.|err:%v", err)
		return err
	}
	logx.I(ctx, "[syncVectorQA][addKnowledge] |rsp:%+v", rsp)
	return nil
}

// TODO: @deprecated
func (v *VectorSyncLogic) addSegmentVector(
	ctx context.Context, row *vecEntity.VectorSync, seg *segEntity.DocSegmentExtend, embeddingVersion uint64,
) error {
	logx.I(ctx, "[syncVector]addSegmentVector vector, seg:%+vm embeddingVersion:%d", seg, embeddingVersion)
	if !seg.IsSegmentForQAAndIndex() && !seg.IsSegmentForIndex() {
		row.SetRequest(nil)
		return nil
	}
	req := retrievalPb.BatchAddKnowledgeReq{
		RobotId:          seg.RobotID,
		IndexId:          entity.SegmentReviewVersionID,
		DocType:          entity.DocTypeSegment,
		EmbeddingVersion: embeddingVersion,
		IsVector:         true,
		Knowledge: []*retrievalPb.KnowledgeData{{
			Id:          seg.ID,
			PageContent: seg.PageContent, // 检索使用 PageContent
			ExpireTime:  seg.GetExpireTime(),
		}},
	}
	row.SetRequest(&req)
	if err := v.addVector(ctx, &req); err != nil {
		return err
	}
	return nil
}

func (v *VectorSyncLogic) addRejectedQuestionVector(ctx context.Context, row *vecEntity.VectorSync,
	rejectedQuestion *qaEntity.RejectedQuestion, embeddingVersion uint64, embeddingModel string) error {
	logx.I(ctx, "[syncVector]addRejectedQuestionVector vector, rejectedQuestion:%+v embeddingVersion:%d",
		rejectedQuestion, embeddingVersion)
	req := retrievalPb.BatchAddKnowledgeReq{
		RobotId:            rejectedQuestion.RobotID,
		IndexId:            entity.RejectedQuestionReviewVersionID,
		DocType:            entity.DocTypeRejectedQuestion,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		IsVector:           true,
		Knowledge: []*retrievalPb.KnowledgeData{{
			Id:          rejectedQuestion.ID,
			PageContent: rejectedQuestion.Question,
		}},
	}
	row.SetRequest(&req)
	if err := v.addVector(ctx, &req); err != nil {
		return err
	}
	return nil
}

// addSimilarQuestionVectorLabel 添加相似问标签
func (v *VectorSyncLogic) addSimilarQuestionVectorLabel(ctx context.Context, labels []*retrievalPb.VectorLabel,
	qaId uint64) []*retrievalPb.VectorLabel {
	var newLabels []*retrievalPb.VectorLabel
	newLabels = append(newLabels, &retrievalPb.VectorLabel{Name: sysLabelQAFlagName, Value: sysLabelQAFlagValueSimilar})
	newLabels = append(newLabels, &retrievalPb.VectorLabel{Name: sysLabelQAIdName, Value: fmt.Sprintf("%d", qaId)})

	if len(labels) == 0 {
		return newLabels
	}
	labels = append(labels, newLabels...)
	return labels
}

// DeleteQAVector 删除问答相似向量
func (v *VectorSyncLogic) deleteQASimilar(ctx context.Context, row *vecEntity.VectorSync, qa *qaEntity.DocQA,
	embeddingVersion uint64, embeddingModel string) error {
	logx.I(ctx, "deleteQASimilar vector, qa:%+v, embeddingVersion:%d, embeddingModel:%s", qa, embeddingVersion, embeddingModel)
	req := retrievalPb.BatchDeleteKnowledgeReq{
		RobotId:            qa.RobotID,
		IndexId:            entity.SimilarVersionID,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		IsVector:           true,
		Data:               []*retrievalPb.KnowledgeIDType{{Id: qa.ID}},
	}
	row.SetRequest(&req)
	return v.deleteVector(ctx, &req)
}

// deleteQAKnowledge 删除问答知识
func (v *VectorSyncLogic) deleteQAKnowledge(ctx context.Context, row *vecEntity.VectorSync, qa *qaEntity.DocQA,
	embeddingVersion uint64, embeddingModel string) error {
	logx.I(ctx, "[syncVector]deleteQAKnowledge vector, qa:%+vm embeddingVersion:%d, embeddingModel:%s",
		qa, embeddingVersion, embeddingModel)
	botBizID, err := v.rawSqlDao.GetBotBizIDByID(ctx, qa.RobotID)
	if err != nil {
		logx.E(ctx, "[syncVector][deleteKnowledge] Failed.|getBotBizIDByID|err:%v", err)
		return err
	}
	req := retrievalPb.BatchDeleteKnowledgeReq{
		RobotId:            qa.RobotID,
		IndexId:            entity.ReviewVersionID,
		Data:               []*retrievalPb.KnowledgeIDType{{Id: qa.ID}},
		DocType:            entity.DocTypeQA,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		BotBizId:           botBizID,
		IsVector:           false,
		// EnvType:          0,
	}
	row.SetRequest(&req)
	if err = v.rpc.RetrievalDirectIndex.BatchDeleteKnowledge(ctx, &req); err != nil {
		logx.E(ctx, "[syncVector][deleteKnowledge] Failed.|err:%v", err)
		return err
	}
	logx.I(ctx, "[syncVector]deleteKnowledge done")
	return nil
}

// deleteSegmentVector 删除分片向量
func (v *VectorSyncLogic) deleteSegmentVector(ctx context.Context, row *vecEntity.VectorSync,
	seg *segEntity.DocSegmentExtend, embeddingVersion uint64) error {
	logx.I(ctx, "[syncVector]deleteSegmentVector vector, seg:%+vm embeddingVersion:%d", seg, embeddingVersion)
	req := retrievalPb.BatchDeleteKnowledgeReq{
		RobotId:          seg.RobotID,
		EmbeddingVersion: embeddingVersion,
		Data:             []*retrievalPb.KnowledgeIDType{{Id: seg.ID}},
		DocType:          entity.DocTypeSegment,
		IndexId:          entity.SegmentReviewVersionID,
		IsVector:         true,
	}
	row.SetRequest(&req)
	return v.deleteVector(ctx, &req)
}

func (v *VectorSyncLogic) deleteRejectedQuestionVector(ctx context.Context, row *vecEntity.VectorSync,
	rejectedQuestion *qaEntity.RejectedQuestion, embeddingVersion uint64, embeddingModel string) error {
	logx.I(ctx, "[syncVector]deleteRejectedQuestionVector vector, rejectedQuestion:%+v embeddingVersion:%d embeddingModel:%s",
		rejectedQuestion, embeddingVersion, embeddingModel)
	req := retrievalPb.BatchDeleteKnowledgeReq{
		RobotId:            rejectedQuestion.RobotID,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		Data:               []*retrievalPb.KnowledgeIDType{{Id: rejectedQuestion.ID}},
		DocType:            entity.DocTypeRejectedQuestion,
		IndexId:            entity.RejectedQuestionReviewVersionID,
		IsVector:           true,
	}
	row.SetRequest(&req)
	return v.deleteVector(ctx, &req)
}

// addVector 新增向量
func (v *VectorSyncLogic) addVector(ctx context.Context, req *retrievalPb.BatchAddKnowledgeReq) error {
	logx.I(ctx, "[syncVector]addVector vector, req:%+v", req)
	botBizID, err := v.rawSqlDao.GetBotBizIDByID(ctx, req.RobotId)
	if err != nil {
		logx.E(ctx, "[syncVector][addVector] Failed|getBotBizIDByID|err:%v", err)
		return err
	}
	req.BotBizId = botBizID
	if _, err := v.rpc.RetrievalDirectIndex.BatchAddKnowledge(ctx, req); err != nil {
		logx.W(ctx, "[syncVector][addVector] | BatchAddKnowledge Failed. req:%+v err:%+v", req, err)
		return err
	}
	return nil
}

// deleteVector 删除向量
func (v *VectorSyncLogic) deleteVector(ctx context.Context, req *retrievalPb.BatchDeleteKnowledgeReq) error {
	botBizID, err := v.rawSqlDao.GetBotBizIDByID(ctx, req.RobotId)
	if err != nil {
		logx.E(ctx, "[syncVector][deleteKnowledge] Failed |getBotBizIDByID|err:%v", err)
		return err
	}
	req.BotBizId = botBizID
	if err := v.rpc.RetrievalDirectIndex.BatchDeleteKnowledge(ctx, req); err != nil {
		logx.W(ctx, "[syncVector][deleteVector] Failed req:%+v err:%+v", req, err)
		return err
	}
	return nil
}

func (v *VectorSyncLogic) syncSegmentExpireTime(ctx context.Context, seg *segEntity.DocSegmentExtend) error {
	doc, err := v.getDocByID(ctx, seg.DocID, seg.RobotID)
	if err != nil {
		logx.E(ctx, "[syncVector][syncSegmentExpireTime],Failed to getDocByID err:%+v", err)
		return err
	}
	seg.ExpireStart = doc.ExpireStart
	seg.ExpireEnd = doc.ExpireEnd
	return nil
}

// DeleteVectorSyncHistory 删除t_vector_history表中冗余数据
func (v *VectorSyncLogic) DeleteVectorSyncHistory(ctx context.Context, cutoffTime time.Time, limit int64) (int64, error) {

	rowsAffected, err := v.vecDao.BatchDeleteVectorSyncHistoryWithCutoffTime(ctx, cutoffTime, int(limit))

	if err != nil {
		logx.E(ctx, "[syncVector]deleteVectorSyncHistory| GetRowsAffected Failed! err:%+v", err)
		return -1, err
	}
	return rowsAffected, nil
}

// getCountByRelatedID 获取不在同步中的数据的数量
func (v *VectorSyncLogic) getCountByRelatedID(ctx context.Context, relateID uint64, typ int) (uint32, error) {
	req := &vecEntity.ListVectorSyncReq{
		RelatedID:   relateID,
		Type:        uint32(typ),
		StatusNotIn: []uint32{vecEntity.StatusSyncing, vecEntity.StatusSyncFailed}, // 之前有失败的记录的不会在sync表里面删除
	}
	count, err := v.vecDao.GetVectorSyncCount(ctx, req)
	if err != nil {
		logx.E(ctx, "[syncVector]getCountByRelatedID Failed! err:%+v", err)
		return 0, err
	}
	return uint32(count), nil
}

// updateQaAndSimilarStatus 更新QA和相似问的状态
func (v *VectorSyncLogic) updateQaAndSimilarStatus(ctx context.Context, qa *qaEntity.DocQA, row *vecEntity.VectorSync) error {
	logx.D(ctx, "[syncVector]updateQaAndSimilarStatus, qa:%+v, row:%+v", qa, row)
	// 已删除/未采纳/非学习中的问答，都不需要把状态改成待发布
	if qa.IsDelete() || qa.IsNotAccepted() || qa.ReleaseStatus != qaEntity.QAReleaseStatusLearning {
		return nil
	}
	count, err := v.getCountByRelatedID(ctx, row.RelateID, vecEntity.VectorTypeQA)
	if err != nil {
		return err
	}
	// 如果count==0，表示没有待入库的数据，但是可能存在多条同步中的数据
	if count <= 0 {
		err = v.qaDao.Query().Transaction(func(tx *mysqlquery.Query) error {
			logx.I(ctx, "[syncVector]updateQaAndSimilarStatus, qa:%+v, row:%+v", qa, row)
			qa.ReleaseStatus = qaEntity.QAReleaseStatusInit
			qa.UpdateTime = time.Now()
			if err = v.updateQAStatus(ctx, qa); err != nil {
				return err
			}
			if row.ExtendedId > 0 {
				// 这里为啥不按照similarID 单条更新呢? (wemysschen 提出的疑问)
				sq := &qaEntity.SimilarQuestion{
					RobotID:       qa.RobotID,
					RelatedQAID:   qa.ID,
					ReleaseStatus: qaEntity.QAReleaseStatusInit,
					UpdateTime:    qa.UpdateTime,
				}
				if err = v.updateSimilarQuestionsStatus(ctx, sq); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err != nil {
		logx.E(ctx, "[syncVector]updateQaAndSimilarStatus fail, qa:%+v, row:%+v", qa, row)
		return err
	}
	return nil
}

// updateQaAndSimilarStatusIfLearnFail 如果重试次数达到了最大值，依然没有入库成功，就更新QA和相似问的状态为学习失败
func (v *VectorSyncLogic) updateQaAndSimilarStatusIfLearnFail(ctx context.Context,
	row *vecEntity.VectorSync) error {
	if !row.IsStatusFail() || !row.IsVectorTypeQA() || !row.ReachedMaxTryTimes() {
		return nil
	}

	qa, err := v.qaDao.GetQAByID(ctx, row.RelateID)
	if err != nil {
		logx.E(ctx, "[syncVector][updateQaAndSimilarStatusIfLearnFail] Failed to GetQAByID err:%+v", err)
		return err
	}
	if qa == nil {
		logx.W(ctx, "[syncVector][updateQaAndSimilarStatusIfLearnFail] Failed to GetQAByID, qa is nil")
		return nil
	}
	logx.I(ctx, "[syncVector][updateQaAndSimilarStatusIfLearnFail], qa:%+v, row:%+v", qa, row)

	qa.ReleaseStatus = qaEntity.QAReleaseStatusLearnFail
	qa.UpdateTime = time.Now()
	if err = v.updateQAStatus(ctx, qa); err != nil {
		logx.E(ctx, "[syncVector][updateQaAndSimilarStatusIfLearnFail] Failed to updateQAStatus err:%+v, qa:%+v", err, qa)
		return err
	}
	if row.ExtendedId > 0 {
		sq := &qaEntity.SimilarQuestion{
			RobotID:       qa.RobotID,
			RelatedQAID:   qa.ID,
			ReleaseStatus: qaEntity.QAReleaseStatusLearnFail,
			UpdateTime:    qa.UpdateTime,
		}
		if err = v.updateSimilarQuestionsStatus(ctx, sq); err != nil {
			logx.E(ctx, "[syncVector]Failed to updateSimilarQuestionsStatus err:%+v, qa:%+v", err, sq)
			return err
		}
	}
	return nil
}

// updateSimilarQuestionsStatus 更新相似问的状态(Update时相似问状态严格和qa保持一致)
func (v *VectorSyncLogic) updateSimilarQuestionsStatus(ctx context.Context, sq *qaEntity.SimilarQuestion) error {
	req := &qaEntity.SimilarityQuestionReq{
		RelatedQAID: sq.RelatedQAID,
		RobotId:     sq.RobotID,
		IsDeleted:   1,
	}

	err := v.qaDao.BatchUpdateSimilarQuestion(ctx, req, map[string]any{
		"release_status": sq.ReleaseStatus,
		"update_time":    sq.UpdateTime,
	}, nil)

	if err != nil {
		logx.E(ctx, "[syncVector][updateSimilarQuestionsStatus] Failed to BatchUpdateSimilarQuestion, err: %+v", err)
		return err
	}
	return nil
}

// updateQAStatus 更新问答对状态
func (v *VectorSyncLogic) updateQAStatus(ctx context.Context, qa *qaEntity.DocQA) error {
	logx.I(ctx, "[syncVector][updateQAStatus] qa:%+v", qa)
	if qa == nil {
		return nil
	}
	filter := &qaEntity.DocQaFilter{
		QAId: qa.ID,
	}

	qa.UpdateTime = time.Now()

	updateColumns := []string{
		v.qaDao.Query().TDocQa.ReleaseStatus.ColumnName().String(),
		v.qaDao.Query().TDocQa.UpdateTime.ColumnName().String(),
	}

	err := v.qaDao.UpdateDocQas(ctx, updateColumns, filter, qa)
	if err != nil {
		logx.E(ctx, "[syncVector]Failed to updateQAStatus, err: %+v", err)
		return err
	}
	return nil
}
