// Package vector 向量库同步接口
package vector

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"strings"
	"time"

	"git.woa.com/dialogue-platform/common/v3/errors"
	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/jmoiron/sqlx"
)

const (
	syncChanSize     = 100000
	vectorSyncFields = `
		id,type,relate_id,status,request,try_times,max_try_times,result,create_time,update_time,extended_id,write_sync_id
	`
	vectorSyncFieldsLength = 12
	vectorHistoryFields    = `
		id,type,relate_id,status,request,try_times,max_try_times,create_time,update_time,extended_id,write_sync_id
	`
	robotFields = `
		id,business_id,split_doc,embedding,is_deleted
	`
	getVectorSync = `
		SELECT
			%s
		FROM
		    t_vector_sync
		WHERE
		    id = ?
	`
	addSync = `
		INSERT INTO
			t_vector_sync(%s)
		VALUES
			(:id,:type,:relate_id,:status,:request,:try_times,:max_try_times,:result,:create_time,:update_time,
			 :extended_id,:write_sync_id)
    `
	addSyncBatch = `
		INSERT INTO
			t_vector_sync(%s)
		VALUES
		    %s
    `
	getCountByRelatedID = `
		SELECT
			count(*)
		FROM
		    t_vector_sync
		WHERE
		    relate_id = ? AND type= ? AND status != ?
	`
	fetchIdByWriteSyncId = `
		SELECT
			id
		FROM
		    t_vector_sync
		WHERE
		    write_sync_id IN (%s)
		ORDER BY
		    id ASC
	`
	fetchNotSuccessSync = `
		SELECT
			id
		FROM
		    t_vector_sync
		WHERE
		    try_times < max_try_times AND (status = ? OR (status IN (?,?) AND update_time < ?))
		ORDER BY
		    id ASC
		LIMIT
			?
    `
	lockOneSync = `
		UPDATE
			t_vector_sync
		SET
		    status = ?
		WHERE
		    id = ? AND status = ?
	`
	batchUpdateSyncStatus = `
		UPDATE
			t_vector_sync
		SET
		    status = ?,
		    update_time = ?
		WHERE
		    id IN (%s)
	`
	updateOneSync = `
		UPDATE
			t_vector_sync
		SET
		    status = :status,
		    request = :request,
		    try_times = :try_times,
		    result = :result,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	deleteVectorSyncHistory = `	DELETE FROM t_vector_sync_history WHERE update_time < ? LIMIT ? `
	deleteOneSync           = `
        DELETE FROM
			t_vector_sync
        WHERE
            id = ?
	`
	moveToHistory = `
		INSERT INTO
			t_vector_sync_history(%s)
		VALUES
		    (null,:type,:relate_id,:status,:request,:try_times,:max_try_times,:create_time,:update_time,:extended_id,:write_sync_id)
	`
	getQAByID = `
		SELECT
			id,business_id,robot_id,corp_id,staff_id,doc_id,origin_doc_id,segment_id,category_id,source,question,answer,
			custom_param,question_desc,release_status,is_audit_free,is_deleted,message,accept_status,next_action,
			similar_status,char_size,attr_range,create_time,update_time,expire_start,expire_end,attribute_flag
		FROM
		    t_doc_qa
		WHERE
		    id = ?
	`
	getDocByID = `SELECT
			id,expire_start,expire_end
		FROM
		    t_doc
		WHERE
		    id = ?`
	getSegmentByID = `
		SELECT
			id,robot_id,doc_id,type,page_content,is_deleted
		FROM
		    t_doc_segment
		WHERE
		    id = ?
	`
	getRobotByID = `
		SELECT
			%s
		FROM
		    t_robot
		WHERE
		    id = ?
    `
	getRejectedQuestionByID = `
		SELECT
			id,robot_id,question,is_deleted,release_status
		FROM
			t_rejected_question
		WHERE
			id = ?
	`

	// TODO(sim): sync表里没有 robot_id, 这里查性能不好
	getSimilarQuestionByID = `
		SELECT
			similar_id,robot_id,related_qa_id,source,question,is_deleted
		FROM
		    t_qa_similar_question
		WHERE
		    related_qa_id = ? AND similar_id = ?
	`
	updateQAStatus = `
		UPDATE
			t_doc_qa
		SET
		    update_time = :update_time,
		    release_status = :release_status
		WHERE
		    id = :id
	`
	updateSimilarQuestionStatus = `
        UPDATE
            t_qa_similar_question
        SET
            update_time = :update_time,
			release_status = :release_status
        WHERE
            robot_id = :robot_id AND related_qa_id = :related_qa_id AND is_deleted = 1
	`
	getCorpBizId = `
		select 
			business_id 
		from 
			t_corp 
		where 
			id = ?
	`
)

var vectorSyncChan = make(chan uint64, syncChanSize)

type GetBotBizIDByIDFunc func(ctx context.Context, robotID uint64) (uint64, error)

// SyncVector 同步向量
type SyncVector struct {
	db              mysql.Client
	tdsql           *gorm.DB
	directIndexCli  pb.DirectIndexClientProxy
	getBotBizIDByID GetBotBizIDByIDFunc
}

// NewVectorSync .
func NewVectorSync(db mysql.Client, tdsql *gorm.DB) *SyncVector {
	return &SyncVector{
		db:             db,
		tdsql:          tdsql,
		directIndexCli: pb.NewDirectIndexClientProxy(),
	}
}

// SetGetBotBizIDByIDFunc 设置使用 robotID 换取 botBizID的方法
func (v *SyncVector) SetGetBotBizIDByIDFunc(f GetBotBizIDByIDFunc) {
	v.getBotBizIDByID = f
}

// AddSyncBatch add sync 批量接口
func (v *SyncVector) AddSyncBatch(ctx context.Context, tx *sqlx.Tx, row []*model.VectorSync) ([]uint64, error) {
	if len(row) == 0 {
		return nil, nil
	}
	sql := addSyncBatch
	placeholders := "(" + placeholder(vectorSyncFieldsLength) + ")"
	values := make([]string, len(row))
	for i := range row {
		values[i] = placeholders
	}
	querySQL := fmt.Sprintf(sql, vectorSyncFields, strings.Join(values, ","))
	// 插入任务
	args := make([]any, 0, len(row)*vectorSyncFieldsLength)
	writeSyncIds := make([]uint64, 0, len(row))
	for _, q := range row {
		args = append(args,
			q.ID,
			q.Type,
			q.RelateID,
			q.Status,
			q.Request,
			q.TryTimes,
			q.MaxTryTimes,
			q.Result,
			q.CreateTime,
			q.UpdateTime,
			q.ExtendedId,
			q.WriteSyncId,
		)
		writeSyncIds = append(writeSyncIds, q.WriteSyncId)
	}
	// log.DebugContextf(ctx, "addSyncBatch, querySQL: %s, args: %+v", querySQL, args)
	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "批量新增向量同步记录失败 sql:%s args:%+v err:%+v", querySQL, row, err)
		return nil, err
	}
	// 反查任务IDs
	ids := make([]uint64, 0)
	querySQL = fmt.Sprintf(fetchIdByWriteSyncId, placeholder(len(writeSyncIds)))
	args = []any{}
	for _, id := range writeSyncIds {
		args = append(args, id)
	}
	// log.DebugContextf(ctx, "fetchIdByWriteSyncId, querySQL: %s, args: %+v", querySQL, args)
	if err := tx.SelectContext(ctx, &ids, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "批量反查同步记录ID失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "fetchIdByWriteSyncId, ids: %+v", ids)
	return ids, nil
}

// AddSync 新增sync
func (v *SyncVector) AddSync(ctx context.Context, tx *sqlx.Tx, row *model.VectorSync) (uint64, error) {
	querySQL := fmt.Sprintf(addSync, vectorSyncFields)
	res, err := tx.NamedExecContext(ctx, querySQL, row)
	if err != nil {
		log.ErrorContextf(ctx, "新增向量同步记录失败 sql:%s args:%+v err:%+v", querySQL, row, err)
		return 0, err
	}
	id, _ := res.LastInsertId()
	return uint64(id), nil
}

// Push 新增向量同步队列
func (v *SyncVector) Push(ctx context.Context, id uint64) {
	select {
	case <-ctx.Done():
	case vectorSyncChan <- id:
		log.Debugf("chan 新增向量待同步ID:%d", id)
	default:
		log.Debugf("db 新增向量待同步ID:%d", id)
	}
}

// BatchPush 批量新增向量同步队列
func (v *SyncVector) BatchPush(ctx context.Context, ids []uint64) {
	for _, id := range ids {
		v.Push(ctx, id)
	}
}

// DoSync 执行同步
func (v *SyncVector) DoSync() {
	ctx := context.Background()
	go func() {
		defer errors.PanicHandler()
		for {
			v.fetchNotSuccessSync(ctx)
			time.Sleep(10 * time.Second)
		}
	}()
	g := errgroupx.Group{}
	g.SetLimit(config.App().VectorSync.Concurrent)
	for id := range vectorSyncChan {
		log.DebugContextf(ctx, "len(vectorSyncChan):%d", len(vectorSyncChan))
		if id == 0 {
			continue
		}
		syncID := id
		g.Go(func() error {
			v.dealOneSync(ctx, syncID)
			return nil
		})
	}
	_ = g.Wait()
}

func (v *SyncVector) fetchNotSuccessSync(ctx context.Context) {
	querySQL := fetchNotSuccessSync
	args := make([]any, 0, 5)
	args = append(args, model.StatusSyncFailed, model.StatusSyncInit, model.StatusSyncing,
		time.Now().Add(-config.App().VectorSync.TimeBefore), config.App().VectorSync.Limit)
	ids := make([]uint64, 0)
	if err := v.db.Select(ctx, &ids, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询流程中超时/失败的同步记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return
	}
	log.DebugContextf(ctx, "查询流程中超时/失败的同步记录 sql:%s args:%+v len(ids):%d", querySQL, args, len(ids))
	if len(ids) == 0 {
		return
	}
	args = make([]any, 0, 2+len(ids))
	args = append(args, model.StatusSyncInit, time.Now())
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL = fmt.Sprintf(batchUpdateSyncStatus, "?"+strings.Repeat(", ?", len(ids)-1))
	if _, err := v.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新流程中超时/失败的同步记录失败 sql:%s args:%+v err:%+v",
			querySQL, args, err)
		return
	}
	for _, id := range ids {
		vectorSyncChan <- id
	}
}

func (v *SyncVector) dealOneSync(ctx context.Context, id uint64) {
	isLock := v.lockOneSync(ctx, id)
	if !isLock {
		return
	}
	row, err := v.getOneSync(ctx, id)
	if err != nil {
		return
	}
	defer v.updateOneSync(ctx, row)
	if row.IsVectorTypeQA() {
		err = v.syncVectorQA(ctx, row)
	}
	if row.IsVectorTypeSegment() {
		err = v.syncVectorSegment(ctx, row)
	}
	if row.IsVectorTypeRejectedQuestion() {
		err = v.syncVectorRejectedQuestion(ctx, row)
	}
	row.Status = model.StatusSyncSuccess
	row.Result = ""
	if err != nil {
		row.Status = model.StatusSyncFailed
		row.Result = err.Error()
	}
}

func (v *SyncVector) lockOneSync(ctx context.Context, id uint64) bool {
	isLock := false
	querySQL := lockOneSync
	args := make([]any, 0, 3)
	args = append(args, model.StatusSyncing, id, model.StatusSyncInit)
	res, err := v.db.Exec(ctx, querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "锁定向量同步记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return isLock
	}
	rows, _ := res.RowsAffected()
	if rows == 1 {
		isLock = true
	}
	return isLock
}

func (v *SyncVector) getOneSync(ctx context.Context, id uint64) (*model.VectorSync, error) {
	row := &model.VectorSync{}
	querySQL := fmt.Sprintf(getVectorSync, vectorSyncFields)
	args := make([]any, 0, 1)
	args = append(args, id)
	if err := v.db.QueryToStruct(ctx, row, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取向量同步记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return row, nil
}

func (v *SyncVector) updateOneSync(ctx context.Context, row *model.VectorSync) {
	row.UpdateTime = time.Now()
	row.TryTimes += 1
	log.DebugContextf(ctx, "updateOneSync id:%d type:%d status:%d", row.ID, row.Type, row.Status)
	if err := v.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if row.Status == model.StatusSyncFailed {
			querySQL := updateOneSync
			if _, err := v.db.NamedExec(ctx, querySQL, row); err != nil {
				log.ErrorContextf(ctx, "更新同步记录失败 sql:%s args:%+v err:%+v", querySQL, row, err)
				return err
			}
			err := v.updateQaAndSimilarStatusIfLearnFail(ctx, tx, row)
			if err != nil {
				log.ErrorContextf(ctx, "更新问答和相似问状态失败 err:%+v", err)
				return err
			}
		}
		if row.Status == model.StatusSyncSuccess {
			querySQL := fmt.Sprintf(moveToHistory, vectorHistoryFields)
			if _, err := v.db.NamedExec(ctx, querySQL, row); err != nil {
				log.ErrorContextf(ctx, "移动同步记录失败 sql:%s args:%+v err:%+v", querySQL, row, err)
				return err
			}
			querySQL = deleteOneSync
			if _, err := v.db.Exec(ctx, querySQL, row.ID); err != nil {
				log.ErrorContextf(ctx, "删除同步记录失败 sql:%s args:%+v err:%+v", querySQL, row, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "updateOneSync失败 row:%+v err:%+v", row, err)
	}
}

// syncVectorQA 同步QA向量, 且支持相似问场景
func (v *SyncVector) syncVectorQA(ctx context.Context, row *model.VectorSync) error {
	if !row.IsAllowSync() {
		return nil
	}
	var err error
	qa := &model.DocQA{}
	querySQL := getQAByID
	args := make([]any, 0, 1)
	args = append(args, row.RelateID)
	if err = v.db.QueryToStruct(ctx, qa, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "向量同步,查询问答数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	querySQL = fmt.Sprintf(getRobotByID, robotFields)
	args = []any{qa.RobotID}
	robot := &model.AppDB{}
	if err = v.db.QueryToStruct(ctx, robot, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "向量同步,查询机器人数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	if robot.HasDeleted() {
		log.DebugContextf(ctx, "向量同步,机器人已经删除 机器人ID:%d", robot.GetAppID())
		return nil
	}
	ctx = pkg.WithSpaceID(ctx, robot.SpaceID)
	embeddingConf, _, err := robot.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "向量同步,查询机器人数据失败 robots[0].GetEmbeddingConf() err:%+v", err)
		return err
	}
	embeddingVersion := embeddingConf.Version
	vectorLabels, err := v.GetQAVectorLabels(ctx, robot.BusinessID, qa)
	if err != nil {
		log.ErrorContextf(ctx, "向量同步,查询QA属性标签数据失败 getQAVectorLabels err:%+v", err)
		return err
	}
	if qa.IsResuming() { // qa的状态处于超量恢复中，不需要写向量，等到状态恢复成待发布之后，才需要写向量。
		return nil
	}
	if qa.IsDisable() { // 如果问答停用，（标准问和相识问）过期时间填当前
		qa.ExpireEnd = time.Now()
	}
	oldQa := *qa
	if row.ExtendedId == 0 {
		// 更新QA的场景
		if qa.IsDelete() || qa.IsNotAccepted() || qa.IsCharExceeded() {
			if err = v.deleteQASimilar(ctx, row, qa, embeddingVersion); err != nil {
				return err
			}
			if err = v.deleteQAKnowledge(ctx, row, qa, embeddingVersion); err != nil {
				return err
			}
			// feature_permission
			//删除角色问答绑定关系,每次删除一万条
			log.DebugContextf(ctx, "feature_permission deleteRoleQa appBizId:%v,qaBizId:%v", robot.BusinessID, qa.BusinessID)
			for deleteRows := 10000; deleteRows == 10000; {
				res := v.tdsql.WithContext(ctx).Model(&model.KnowledgeRoleQA{}).Where("is_deleted = 0").
					Where("knowledge_biz_id = ?", robot.BusinessID). //兼容共享知识库处理
					Where("qa_biz_id = ?", qa.BusinessID).Limit(10000).
					Updates(map[string]interface{}{"is_deleted": 1, "update_time": time.Now()})
				if res.Error != nil { //柔性放过
					log.ErrorContextf(ctx, "feature_permission deleteRoleQA err:%v,appBizId:%v,qaBizId:%v",
						res.Error, robot.BusinessID, qa.BusinessID)
					break
				}
				deleteRows = int(res.RowsAffected)
			}
		} else {
			if err = v.addQASimilar(ctx, row, qa, embeddingVersion, vectorLabels); err != nil {
				return err
			}
			if err = v.addQAKnowledge(ctx, row, qa, embeddingVersion, vectorLabels); err != nil {
				return err
			}
		}
	} else { // 更新QA相似问的场景(QAType下, extendedId为相似问ID)
		log.InfoContextf(ctx, "向量同步,QA-相似问场景, qa_id: %d, similar_id: %d", row.RelateID, row.ExtendedId)
		sim := &model.SimilarQuestion{}
		args = []any{row.RelateID, row.ExtendedId}
		if err = v.db.QueryToStruct(ctx, sim, getSimilarQuestionByID, args...); err != nil {
			log.ErrorContextf(ctx, "向量同步,查询问答-相似问数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		// 同步到vector时，是按照docTypeQA的流程来，需要更新为相似问的Id和Question
		qa.ID = sim.SimilarID
		qa.Question = sim.Question
		// 问答是否停用，如果停用过期时间需要设为当前
		if qa.IsDelete() || qa.IsNotAccepted() || qa.IsCharExceeded() || sim.IsDelete() {
			if err = v.deleteQAKnowledge(ctx, row, qa, embeddingVersion); err != nil {
				return err
			}
		} else {
			vectorLabels = v.addSimilarQuestionVectorLabel(ctx, vectorLabels, sim.RelatedQAID)
			log.DebugContextf(ctx, "vectorLabels: %+v", vectorLabels)
			if err = v.addQAKnowledge(ctx, row, qa, embeddingVersion, vectorLabels); err != nil {
				return err
			}
		}
	}
	err = v.updateQaAndSimilarStatus(ctx, &oldQa, row)
	return err
}

// updateQaAndSimilarStatus 更新QA和相似问的状态
func (v *SyncVector) updateQaAndSimilarStatus(ctx context.Context, qa *model.DocQA, row *model.VectorSync) error {
	log.DebugContextf(ctx, "updateQaAndSimilarStatus, qa:%+v, row:%+v", qa, row)
	// 已删除/未采纳/非学习中的问答，都不需要把状态改成待发布
	if qa.IsDelete() || qa.IsNotAccepted() || qa.ReleaseStatus != model.QAReleaseStatusLearning {
		return nil
	}
	count, err := v.getCountByRelatedID(ctx, row.RelateID, model.VectorTypeQA)
	if err != nil {
		return err
	}
	// 如果count==0，表示没有待入库的数据，但是可能存在多条同步中的数据
	if count <= 0 {
		err = v.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
			qa.ReleaseStatus = model.QAReleaseStatusInit
			qa.UpdateTime = time.Now()
			if err = v.updateQAStatus(ctx, tx, qa); err != nil {
				return err
			}
			if row.ExtendedId > 0 {
				sq := &model.SimilarQuestion{
					RobotID:       qa.RobotID,
					RelatedQAID:   qa.ID,
					ReleaseStatus: model.QAReleaseStatusInit,
					UpdateTime:    qa.UpdateTime,
				}
				if err = v.updateSimilarQuestionsStatus(ctx, tx, sq); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err != nil {
		log.ErrorContextf(ctx, "updateQaAndSimilarStatus fail, qa:%+v, row:%+v", qa, row)
		return err
	}
	return nil
}

// updateQaAndSimilarStatusIfLearnFail 如果重试次数达到了最大值，依然没有入库成功，就更新QA和相似问的状态为学习失败
func (v *SyncVector) updateQaAndSimilarStatusIfLearnFail(ctx context.Context, tx *sqlx.Tx,
	row *model.VectorSync) error {
	if !row.IsStatusFail() || !row.IsVectorTypeQA() || !row.ReachedMaxTryTimes() {
		return nil
	}
	qa := &model.DocQA{}
	querySQL := getQAByID
	args := make([]any, 0, 1)
	var err error
	args = append(args, row.RelateID)
	if err = v.db.QueryToStruct(ctx, qa, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询问答数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	log.InfoContextf(ctx, "updateQaAndSimilarStatusIfLearnFail, qa:%+v, row:%+v", qa, row)

	qa.ReleaseStatus = model.QAReleaseStatusLearnFail
	qa.UpdateTime = time.Now()
	if err = v.updateQAStatus(ctx, tx, qa); err != nil {
		log.ErrorContextf(ctx, "更新qa状态失败 err:%+v, qa:%+v", err, qa)
		return err
	}
	if row.ExtendedId > 0 {
		sq := &model.SimilarQuestion{
			RobotID:       qa.RobotID,
			RelatedQAID:   qa.ID,
			ReleaseStatus: model.QAReleaseStatusLearnFail,
			UpdateTime:    qa.UpdateTime,
		}
		if err = v.updateSimilarQuestionsStatus(ctx, tx, sq); err != nil {
			log.ErrorContextf(ctx, "更新相似问状态失败 err:%+v, qa:%+v", err, sq)
			return err
		}
	}
	return nil
}

// updateSimilarQuestionsStatus 更新相似问的状态(Update时相似问状态严格和qa保持一致)
func (v *SyncVector) updateSimilarQuestionsStatus(ctx context.Context, tx *sqlx.Tx,
	sq *model.SimilarQuestion) error {
	if tx == nil || sq == nil {
		return nil
	}
	sql := updateSimilarQuestionStatus
	if _, err := tx.NamedExecContext(ctx, sql, sq); err != nil {
		log.ErrorContextf(ctx, "批量更新相似问状态失败, sql: %s, err: %+v", sql, err)
		return err
	}
	return nil
}

// updateQAStatus 更新问答对状态
func (v *SyncVector) updateQAStatus(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA) error {
	if tx == nil || qa == nil {
		return nil
	}
	_, err := tx.NamedExecContext(ctx, updateQAStatus, qa)
	if err != nil {
		log.ErrorContextf(ctx, "更新qa状态失败, sql: %s, err: %+v", updateQAStatus, err)
		return err
	}
	return nil
}

// getCountByRelatedID 获取不在同步中的数据的数量
func (v *SyncVector) getCountByRelatedID(ctx context.Context, relateID uint64, typ int) (uint32, error) {
	querySQL := getCountByRelatedID
	args := []any{relateID, typ, model.StatusSyncing}
	count := make([]uint32, 0, 1)
	if err := v.db.Select(ctx, &count, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "getCountByRelatedID sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	if len(count) == 0 {
		return 0, fmt.Errorf("getCountByRelatedID err")
	}
	return count[0], nil
}
func (v *SyncVector) syncVectorSegment(ctx context.Context, row *model.VectorSync) error {
	if !row.IsAllowSync() {
		return nil
	}
	var err error
	seg := &model.DocSegmentExtend{}
	querySQL := getSegmentByID
	args := make([]any, 0, 1)
	args = append(args, row.RelateID)
	if err = v.db.QueryToStruct(ctx, seg, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "向量同步,查询分片数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	querySQL = fmt.Sprintf(getRobotByID, robotFields)
	args = []any{seg.RobotID}
	robot := &model.AppDB{}
	if err = v.db.QueryToStruct(ctx, robot, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "向量同步,查询机器人数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	if robot.HasDeleted() {
		log.DebugContextf(ctx, "向量同步,机器人已经删除 机器人ID:%d", robot.GetAppID())
		return nil
	}
	embeddingConf, _, err := robot.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "向量同步,查询机器人数据失败 robots[0].GetEmbeddingConf() err:%+v", err)
		return err
	}
	embeddingVersion := embeddingConf.Version
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

func (v *SyncVector) syncVectorRejectedQuestion(ctx context.Context, row *model.VectorSync) error {
	if !row.IsAllowSync() {
		return nil
	}
	var err error
	rejectedQuestion := &model.RejectedQuestion{}
	SQL := getRejectedQuestionByID
	args := make([]any, 0, 1)
	args = append(args, row.RelateID)
	if err = v.db.QueryToStruct(ctx, rejectedQuestion, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "向量同步,查询分片数据失败 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		if mysql.IsNoRowsError(err) {
			return nil
		}
		return err
	}
	SQL = fmt.Sprintf(getRobotByID, robotFields)
	args = []any{rejectedQuestion.RobotID}
	robot := &model.AppDB{}
	if err = v.db.QueryToStruct(ctx, robot, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "向量同步,查询机器人数据失败 sql:%s args:%+v err:%+v", SQL, args, err)
		return err
	}
	if robot.HasDeleted() {
		log.DebugContextf(ctx, "向量同步,机器人已经删除 机器人ID:%d", robot.GetAppID())
		return nil
	}
	embeddingConf, _, err := robot.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "向量同步,查询机器人数据失败 robots[0].GetEmbeddingConf() err:%+v", err)
		return err
	}
	embeddingVersion := embeddingConf.Version
	if rejectedQuestion.IsDelete() {
		err = v.deleteRejectedQuestionVector(ctx, row, rejectedQuestion, embeddingVersion)
	} else {
		err = v.addRejectedQuestionVector(ctx, row, rejectedQuestion, embeddingVersion)
	}
	return err
}

// addQAVector 新增问答向量
func (v *SyncVector) addQAVector(
	ctx context.Context, row *model.VectorSync, qa *model.DocQA, embeddingVersion uint64,
	vectorLabels []*pb.VectorLabel) error {
	reqs := make([]*pb.AddVectorReq, 0, 2)
	reqs = append(reqs, &pb.AddVectorReq{
		RobotId:          qa.RobotID,
		IndexId:          model.SimilarVersionID,
		Id:               qa.ID,
		PageContent:      qa.Question,
		DocType:          model.DocTypeQA,
		EmbeddingVersion: embeddingVersion,
		Labels:           vectorLabels,
		ExpireTime:       qa.GetExpireTime(),
	})
	if qa.IsAccepted() {
		reqs = append(reqs, &pb.AddVectorReq{
			RobotId:          qa.RobotID,
			IndexId:          model.ReviewVersionID,
			Id:               qa.ID,
			PageContent:      qa.Question,
			DocType:          model.DocTypeQA,
			EmbeddingVersion: embeddingVersion,
			Labels:           vectorLabels,
			ExpireTime:       qa.GetExpireTime(),
		})
	}
	row.SetRequest(reqs)
	return v.addVector(ctx, reqs)
}

// addQASimilar TODO
// AddQAVector 新增问答相似向量
func (v *SyncVector) addQASimilar(
	ctx context.Context, row *model.VectorSync, qa *model.DocQA, embeddingVersion uint64,
	vectorLabels []*pb.VectorLabel) error {
	reqs := make([]*pb.AddVectorReq, 0, 1)
	reqs = append(reqs, &pb.AddVectorReq{
		RobotId:          qa.RobotID,
		IndexId:          model.SimilarVersionID,
		Id:               qa.ID,
		PageContent:      qa.Question,
		DocType:          model.DocTypeQA,
		EmbeddingVersion: embeddingVersion,
		Labels:           vectorLabels,
		ExpireTime:       qa.GetExpireTime(),
	})
	row.SetRequest(reqs)
	return v.addVector(ctx, reqs)
}

// addQAKnowledge 新增问答知识
func (v *SyncVector) addQAKnowledge(
	ctx context.Context, row *model.VectorSync, qa *model.DocQA, embeddingVersion uint64,
	vectorLabels []*pb.VectorLabel) error {
	if !qa.IsAccepted() {
		log.InfoContextf(ctx, "addQAKnowledge qa is not accepted ignore, qa:%+v", qa)
		return nil
	}
	reqs := make([]*pb.AddKnowledgeReq, 0, 1)
	reqs = append(reqs, &pb.AddKnowledgeReq{
		RobotId:          qa.RobotID,
		IndexId:          model.ReviewVersionID,
		Id:               qa.ID,
		PageContent:      qa.Question,
		PageContentExtra: fmt.Sprintf("%s\n%s\n%s", qa.Question, qa.QuestionDesc, qa.Answer),
		DocType:          model.DocTypeQA,
		EmbeddingVersion: embeddingVersion,
		Labels:           vectorLabels,
		ExpireTime:       qa.GetExpireTime(),
	})
	row.SetRequest(reqs)
	return v.addKnowledge(ctx, reqs)
}

func (v *SyncVector) addSegmentVector(
	ctx context.Context, row *model.VectorSync, seg *model.DocSegmentExtend, embeddingVersion uint64,
) error {
	reqs := make([]*pb.AddVectorReq, 0, 1)
	if !seg.IsSegmentForQAAndIndex() && !seg.IsSegmentForIndex() {
		row.SetRequest(reqs)
		return nil
	}
	reqs = append(reqs, &pb.AddVectorReq{
		RobotId:          seg.RobotID,
		IndexId:          model.SegmentReviewVersionID,
		Id:               seg.ID,
		PageContent:      seg.PageContent, // 检索使用 PageContent
		DocType:          model.DocTypeSegment,
		EmbeddingVersion: embeddingVersion,
		ExpireTime:       seg.GetExpireTime(),
	})
	row.SetRequest(reqs)
	if err := v.addVector(ctx, reqs); err != nil {
		return err
	}
	return nil
}

func (v *SyncVector) addRejectedQuestionVector(ctx context.Context, row *model.VectorSync,
	rejectedQuestion *model.RejectedQuestion, embeddingVersion uint64) error {
	reqs := make([]*pb.AddVectorReq, 0, 1)
	reqs = append(reqs, &pb.AddVectorReq{
		RobotId:          rejectedQuestion.RobotID,
		IndexId:          model.RejectedQuestionReviewVersionID,
		Id:               rejectedQuestion.ID,
		PageContent:      rejectedQuestion.Question,
		DocType:          model.DocTypeRejectedQuestion,
		EmbeddingVersion: embeddingVersion,
	})
	row.SetRequest(reqs)
	if err := v.addVector(ctx, reqs); err != nil {
		return err
	}
	return nil
}

// deleteQAVector 删除问答向量
func (v *SyncVector) deleteQAVector(
	ctx context.Context, row *model.VectorSync, qa *model.DocQA, embeddingVersion uint64,
) error {
	reqs := make([]*pb.DeleteVectorReq, 0, 2)
	reqs = append(reqs, &pb.DeleteVectorReq{
		RobotId:          qa.RobotID,
		IndexId:          model.SimilarVersionID,
		Id:               qa.ID,
		EmbeddingVersion: embeddingVersion,
	})
	reqs = append(reqs, &pb.DeleteVectorReq{
		RobotId:          qa.RobotID,
		IndexId:          model.ReviewVersionID,
		Id:               qa.ID,
		EmbeddingVersion: embeddingVersion,
	})
	row.SetRequest(reqs)
	return v.deleteVector(ctx, reqs)
}

// deleteQASimilar TODO
// DeleteQAVector 删除问答相似向量
func (v *SyncVector) deleteQASimilar(
	ctx context.Context, row *model.VectorSync, qa *model.DocQA, embeddingVersion uint64,
) error {
	reqs := make([]*pb.DeleteVectorReq, 0, 1)
	reqs = append(reqs, &pb.DeleteVectorReq{
		RobotId:          qa.RobotID,
		IndexId:          model.SimilarVersionID,
		Id:               qa.ID,
		EmbeddingVersion: embeddingVersion,
	})
	row.SetRequest(reqs)
	return v.deleteVector(ctx, reqs)
}

// deleteQAKnowledge 删除问答知识
func (v *SyncVector) deleteQAKnowledge(
	ctx context.Context, row *model.VectorSync, qa *model.DocQA, embeddingVersion uint64,
) error {
	reqs := make([]*pb.DeleteKnowledgeReq, 0, 1)
	reqs = append(reqs, &pb.DeleteKnowledgeReq{
		RobotId:          qa.RobotID,
		IndexId:          model.ReviewVersionID,
		Id:               qa.ID,
		DocType:          model.DocTypeQA,
		EmbeddingVersion: embeddingVersion,
	})
	row.SetRequest(reqs)
	return v.deleteKnowledge(ctx, reqs)
}

// deleteSegmentVector 删除分片向量
func (v *SyncVector) deleteSegmentVector(
	ctx context.Context, row *model.VectorSync, seg *model.DocSegmentExtend, embeddingVersion uint64,
) error {
	reqs := make([]*pb.DeleteVectorReq, 0, 2)
	reqs = append(reqs, &pb.DeleteVectorReq{
		RobotId:          seg.RobotID,
		IndexId:          model.SegmentReviewVersionID,
		Id:               seg.ID,
		EmbeddingVersion: embeddingVersion,
	})
	row.SetRequest(reqs)
	return v.deleteVector(ctx, reqs)
}

func (v *SyncVector) deleteRejectedQuestionVector(ctx context.Context, row *model.VectorSync,
	rejectedQuestion *model.RejectedQuestion, embeddingVersion uint64) error {
	reqs := make([]*pb.DeleteVectorReq, 0, 2)
	reqs = append(reqs, &pb.DeleteVectorReq{
		RobotId:          rejectedQuestion.RobotID,
		IndexId:          model.RejectedQuestionReviewVersionID,
		Id:               rejectedQuestion.ID,
		EmbeddingVersion: embeddingVersion,
	})
	row.SetRequest(reqs)
	return v.deleteVector(ctx, reqs)
}

// addVector 新增向量
func (v *SyncVector) addVector(ctx context.Context, reqs []*pb.AddVectorReq) error {
	for _, req := range reqs {
		botBizID, err := v.getBotBizIDByID(ctx, req.RobotId)
		if err != nil {
			log.ErrorContextf(ctx, "新增向量失败|getBotBizIDByID|err:%v", err)
			return err
		}
		req.BotBizId = botBizID
		if _, err := v.directIndexCli.AddVector(ctx, req); err != nil {
			log.WarnContextf(ctx, "新增向量失败 req:%+v err:%+v", req, err)
			return err
		}
	}
	return nil
}

// addKnowledge TODO
// addVector 新增向量
func (v *SyncVector) addKnowledge(ctx context.Context, reqs []*pb.AddKnowledgeReq) error {
	for _, req := range reqs {
		log.InfoContextf(ctx, "addKnowledge|req:%+v", req)
		botBizID, err := v.getBotBizIDByID(ctx, req.RobotId)
		if err != nil {
			log.ErrorContextf(ctx, "addKnowledge|getBotBizIDByID|err:%v", err)
			return err
		}
		req.BotBizId = botBizID
		rsp, err := v.directIndexCli.AddKnowledge(ctx, req)
		if err != nil {
			log.ErrorContextf(ctx, "addKnowledge|err:%v", err)
			return err
		}
		log.InfoContextf(ctx, "addKnowledge|rsp:%+v", rsp)
	}
	return nil
}

// deleteVector 删除向量
func (v *SyncVector) deleteVector(ctx context.Context, reqs []*pb.DeleteVectorReq) error {
	for _, req := range reqs {
		botBizID, err := v.getBotBizIDByID(ctx, req.RobotId)
		if err != nil {
			log.ErrorContextf(ctx, "deleteKnowledge|getBotBizIDByID|err:%v", err)
			return err
		}
		req.BotBizId = botBizID
		if _, err := v.directIndexCli.DeleteVector(ctx, req); err != nil {
			log.WarnContextf(ctx, "删除向量失败 req:%+v err:%+v", req, err)
			return err
		}
	}
	return nil
}

// deleteKnowledge 删除知识
func (v *SyncVector) deleteKnowledge(ctx context.Context, reqs []*pb.DeleteKnowledgeReq) error {
	for _, req := range reqs {
		log.InfoContextf(ctx, "deleteKnowledge|req:%+v", req)
		botBizID, err := v.getBotBizIDByID(ctx, req.RobotId)
		if err != nil {
			log.ErrorContextf(ctx, "deleteKnowledge|getBotBizIDByID|err:%v", err)
			return err
		}
		req.BotBizId = botBizID
		rsp, err := v.directIndexCli.DeleteKnowledge(ctx, req)
		if err != nil {
			log.ErrorContextf(ctx, "deleteKnowledge|err:%v", err)
			return err
		}
		log.InfoContextf(ctx, "deleteKnowledge|rsp:%+v", rsp)
	}
	return nil
}

func (v *SyncVector) syncSegmentExpireTime(ctx context.Context, seg *model.DocSegmentExtend) error {
	doc := &model.Doc{}
	querySQL := getDocByID
	args := []any{seg.DocID}
	if err := v.db.QueryToStruct(ctx, doc, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "向量同步,查询文档数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	seg.ExpireStart = doc.ExpireStart
	seg.ExpireEnd = doc.ExpireEnd
	return nil
}

// DeleteVectorSyncHistory 删除t_vector_history表中冗余数据
func (v *SyncVector) DeleteVectorSyncHistory(ctx context.Context, cutoffTime time.Time, limit int64) (int64, error) {
	deleteSQL := deleteVectorSyncHistory
	if v.db == nil {
		log.ErrorContextf(ctx, "deleteVectorSyncHistory| v.db nil ! cutoffTime:%+v, limit:%+v,", cutoffTime, limit)
		return -1, nil
	}
	result, err := v.db.Exec(ctx, deleteSQL, cutoffTime, limit)
	if err != nil {
		log.ErrorContextf(ctx, "deleteVectorSyncHistory| db.Delete Failed! cutoffTime:%+v, limit:%+v,err:%+v", cutoffTime, limit, err)
		return -1, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.ErrorContextf(ctx, "deleteVectorSyncHistory| GetRowsAffected Failed! result:%+v,err:%+v", result, err)
		return -1, err
	}
	return rowsAffected, nil
}
