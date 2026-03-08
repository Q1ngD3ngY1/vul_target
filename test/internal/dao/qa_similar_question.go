package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"strings"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-database/mysql"

	"github.com/jmoiron/sqlx"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	similarQuestionFields = `
		similar_id, robot_id, corp_id, staff_id, create_user_id, related_qa_id, source, question,
		is_deleted, release_status, is_audit_free, next_action, create_time, update_time, char_size
	`
	similarQuestionFieldsLen = 15

	// addSimilarQuestions 添加相似问
	addSimilarQuestions = `
        INSERT INTO
			t_qa_similar_question (%s)
        VALUES
            %s
    `

	updateSimilarQuestion = `
        UPDATE
            t_qa_similar_question
        SET
            question = :question,
            update_time = :update_time,
			release_status = :release_status,
			is_audit_free = :is_audit_free,
			next_action = :next_action,
            char_size = :char_size
        WHERE
            corp_id = :corp_id AND robot_id = :robot_id AND similar_id = :similar_id
	`
	updateSimilarQuestionStatus = `
        UPDATE
            t_qa_similar_question
        SET
            update_time = :update_time,
			release_status = :release_status
        WHERE
            robot_id = :robot_id AND similar_id = :similar_id
	`
	getSimilarQuestionsByQA = `
		SELECT
			%s
        FROM
            t_qa_similar_question
        WHERE
			corp_id = ?
		AND robot_id = ?
		AND is_deleted = ?
		AND related_qa_id = ?
		LIMIT ?
	`

	getSimilarQuestionsByQAWithoutRelease = `
		SELECT
			%s
        FROM
            t_qa_similar_question
        WHERE
			corp_id = ?
		AND robot_id = ?
		AND is_deleted = ?
		AND related_qa_id = ?
        AND release_status NOT IN (%s)
		LIMIT ?
	`

	getSimilarQuestionsSimpleByQAIDs = `
		SELECT
		    related_qa_id, similar_id, question
        FROM
            t_qa_similar_question
        WHERE
			corp_id = ? AND robot_id = ? AND is_deleted = ? AND related_qa_id IN (%s)
		LIMIT ?, ?
	`

	getSimilarQuestionsCountByQAIDs = `
		SELECT
			related_qa_id, count(*) as total
		FROM
		    t_qa_similar_question
		WHERE
		     corp_id = ? AND robot_id = ? AND is_deleted = ? AND related_qa_id IN (%s)
		GROUP BY related_qa_id
	`

	deleteSimilarQuestions = `
        UPDATE
            t_qa_similar_question
        SET
            is_deleted = ?,
            update_time = ?,
            release_status = ?,
            is_audit_free = ?,
			next_action = ?
        WHERE
            corp_id = ? AND robot_id = ? AND similar_id IN (%s)
	`

	deleteSimilarQuestionsByQA = `
        UPDATE
            t_qa_similar_question
        SET
            is_deleted = ?,
            update_time = ?,
	        release_status = ?,
            is_audit_free = ?,
			next_action = ?
		WHERE
             corp_id = ? AND robot_id = ? AND related_qa_id = ?
	`

	getSimilarQuestionsSize = `
        SELECT
		    IFNULL(SUM(char_size), 0)
        FROM
			t_qa_similar_question
        WHERE
			robot_id = ?
        AND corp_id = ?
        AND is_deleted = ?
        AND release_status NOT IN (%s)
        AND similar_id IN (%s)
	`

	getSimilarQuestionCount = `
		SELECT
			count(*)
		FROM
		    t_qa_similar_question
		WHERE
		    robot_id = ?
		AND corp_id = ?
		AND is_deleted = ?
		AND related_qa_id = ?
        AND release_status NOT IN (%s)
	`

	getQASimilarQuestionsCount = `
		SELECT COUNT(*) FROM t_qa_similar_question
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ?
	`

	getSimilarQuestionsByUpdateTime = `
		SELECT %s FROM t_qa_similar_question
		WHERE id > ? AND id <= ? AND update_time > ? AND update_time <= ? AND is_deleted = ?
	`
)

// GetNewCharSize 修改 qa 时，根据已有的 size 和 modify 请求里的相似问的信息，重新计算最新的 size, oldQA, 返回新的QASize
func (d *dao) GetNewCharSize(ctx context.Context, oldQA *model.DocQA, modifyReq *pb.ModifyQAReq) (uint64, error) {
	newVideoSize, err := d.GetVideoURLsCharSize(ctx, modifyReq.GetAnswer())
	if err != nil {
		log.ErrorContextf(ctx, "GetNewCharSize|newVideoSize|failed err:%d", err)
		return 0, err
	}
	oldVideoSize, err := d.GetVideoURLsCharSize(ctx, oldQA.Answer)
	if err != nil {
		log.ErrorContextf(ctx, "GetNewCharSize|oldVideoSize|failed err:%d", err)
		return 0, err
	}
	log.InfoContextf(ctx, "modifyQa|newVideoSize:%d, oldVideoSize:%d",
		newVideoSize, oldVideoSize)
	// 新的主问 size
	newQASize := utf8.RuneCountInString(modifyReq.GetQuestion()+modifyReq.GetAnswer()) + newVideoSize
	// 旧的主问 size
	oldQASize := utf8.RuneCountInString(oldQA.Question+oldQA.Answer) + oldVideoSize
	similarModify := modifyReq.GetSimilarQuestionModify()
	if similarModify == nil {
		// 当前修改不涉及相似问，需要考虑主问的大小变化
		newSize := int64(oldQA.CharSize) + int64(newQASize) - int64(oldQASize)
		if newSize <= 0 {
			log.ErrorContextf(ctx, "GetNewCharSize failed get invalid size:%d", newSize)
			return 0, nil
		}
		return uint64(newSize), nil
	}
	addSimsSize := 0
	for _, addQ := range similarModify.GetAddQuestions() {
		addSimsSize += utf8.RuneCountInString(strings.TrimSpace(addQ))
	}
	delSimsSize := 0
	for _, deleteQ := range similarModify.GetDeleteQuestions() {
		delSimsSize += utf8.RuneCountInString(strings.TrimSpace(deleteQ.GetQuestion()))
	}

	updateBizIds := make([]uint64, 0, len(similarModify.GetUpdateQuestions()))
	updateSimsSizeNew := 0
	for _, updateQ := range similarModify.GetUpdateQuestions() {
		updateBizIds = append(updateBizIds, updateQ.GetSimBizId())
		updateSimsSizeNew += utf8.RuneCountInString(strings.TrimSpace(updateQ.GetQuestion()))
	}
	updateSimsSizeOld, _ := d.GetSimilarQuestionsCharSize(ctx, oldQA, updateBizIds)

	// 新 char size
	newSize := int64(oldQA.CharSize) + int64(newQASize) - int64(oldQASize) + int64(updateSimsSizeNew) -
		int64(updateSimsSizeOld) + int64(addSimsSize) - int64(delSimsSize)
	if newSize <= 0 {
		log.ErrorContextf(ctx, "GetNewCharSize failed get invalid size:%d", newSize)
		return 0, nil
	}

	return uint64(newSize), nil
}

// NewSimilarQuestionsFromModifyReq 从修改请求中生成相似问
func (d *dao) NewSimilarQuestionsFromModifyReq(ctx context.Context, qa *model.DocQA,
	similarModify *pb.SimilarQuestionModify) (*model.SimilarQuestionModifyInfo, error) {
	if qa == nil {
		return nil, errors.New("qa is nil")
	}
	if similarModify == nil { // 当前修改没有涉及相似问
		return nil, nil
	}
	sqm := &model.SimilarQuestionModifyInfo{
		AddQuestions: d.NewSimilarQuestions(ctx, qa, similarModify.GetAddQuestions()),
		DeleteQuestions: d.NewSimilarQuestionsFromPB(ctx, qa, similarModify.GetDeleteQuestions(),
			model.NextActionDelete),
		UpdateQuestions: d.NewSimilarQuestionsFromPB(ctx, qa, similarModify.GetUpdateQuestions(), qa.NextAction),
	}

	return sqm, nil
}

// NewSimilarQuestionsFromDBAndReq 从 db 中和修改请求中生成去重后的相似问
func (d *dao) NewSimilarQuestionsFromDBAndReq(ctx context.Context, qa *model.DocQA,
	similarModify *pb.SimilarQuestionModify, isAll bool) (*model.SimilarQuestionModifyInfo, error) {
	if qa == nil {
		return nil, errors.New("qa is nil")
	}
	var err error
	currentSqs := make([]*model.SimilarQuestion, 0)
	if isAll {
		currentSqs, err = d.GetSimilarQuestionsByQA(ctx, qa)
	} else {
		currentSqs, err = d.GetSimilarQuestionsByQAWithoutRelease(ctx, qa)
	}
	if err != nil {
		return nil, err
	}
	sqm, err := d.NewSimilarQuestionsFromModifyReq(ctx, qa, similarModify)
	if err != nil {
		return nil, err
	}
	if sqm == nil { // modify req 里没有相似问
		sqm = &model.SimilarQuestionModifyInfo{}
	}
	duplicatedIds := make(map[uint64]struct{})
	for _, q := range similarModify.GetDeleteQuestions() {
		duplicatedIds[q.GetSimBizId()] = struct{}{}
	}
	for _, q := range similarModify.GetUpdateQuestions() {
		duplicatedIds[q.GetSimBizId()] = struct{}{}
	}
	for i, sq := range currentSqs {
		if _, ok := duplicatedIds[sq.SimilarID]; ok {
			continue
		}
		// 针对已存储的相似问，如果没有被删除或者更新，这里都放到 update 列表里重新发布
		sqm.UpdateQuestions = append(sqm.UpdateQuestions, currentSqs[i])
	}

	return sqm, nil
}

// ModifySimilarQuestions 整体更新相似问：包括新增、删除、update(复用QA的状态)
func (d *dao) ModifySimilarQuestions(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA,
	sqm *model.SimilarQuestionModifyInfo) error {

	if qa == nil || sqm == nil {
		return nil
	}
	if len(sqm.AddQuestions) > 0 {
		if err := d.AddSimilarQuestions(ctx, tx, sqm.AddQuestions); err != nil {
			return err
		}
	}
	if len(sqm.DeleteQuestions) > 0 {
		if err := d.DeleteSimilarQuestions(ctx, tx, sqm.DeleteQuestions); err != nil {
			return err
		}
	}
	if len(sqm.UpdateQuestions) > 0 {
		if err := d.UpdateSimilarQuestions(ctx, tx, qa, sqm.UpdateQuestions); err != nil {
			return err
		}
	}

	return nil
}

// AddSimilarQuestions 添加相似问
func (d *dao) AddSimilarQuestions(ctx context.Context, tx *sqlx.Tx, sqs []*model.SimilarQuestion) error {
	if len(sqs) == 0 {
		return nil
	}
	sql := addSimilarQuestions
	placeholders := "(" + placeholder(similarQuestionFieldsLen) + ")"
	values := make([]string, len(sqs))
	for i := range sqs {
		values[i] = placeholders
	}
	querySql := fmt.Sprintf(sql, similarQuestionFields, strings.Join(values, ","))
	// prepare the arguments
	args := make([]any, 0, len(sqs)*similarQuestionFieldsLen)
	for _, q := range sqs {
		args = append(args,
			q.SimilarID,
			q.RobotID,
			q.CorpID,
			q.StaffID,
			q.CreateUserID,
			q.RelatedQAID,
			q.Source,
			q.Question,
			q.IsDeleted,
			q.ReleaseStatus,
			q.IsAuditFree,
			q.NextAction,
			q.CreateTime,
			q.UpdateTime,
			q.CharSize,
		)
	}
	if _, err := tx.ExecContext(ctx, querySql, args...); err != nil {
		log.ErrorContextf(ctx, "添加相似问失败, sql: %s, args: %+v, err: %+v", sql, args, err)
		return err
	}

	return nil
}

// NewSimilarQuestions 相似问将复用QA的大部分属性(除IsDeleted,NextAction,CharSize,Time)
func (d *dao) NewSimilarQuestions(ctx context.Context, qa *model.DocQA, questions []string) []*model.SimilarQuestion {
	sqs := make([]*model.SimilarQuestion, 0)
	if qa == nil || len(questions) == 0 {
		return sqs
	}
	for i := range questions {
		similarID := d.GenerateSeqID()
		now := time.Now()
		question := strings.TrimSpace(questions[i])
		sq := &model.SimilarQuestion{
			SimilarID:     similarID,
			RobotID:       qa.RobotID,
			CorpID:        qa.CorpID,
			StaffID:       qa.StaffID,
			CreateUserID:  qa.StaffID,
			RelatedQAID:   qa.ID,
			Source:        qa.Source,
			Question:      question,
			ReleaseStatus: qa.ReleaseStatus,
			IsAuditFree:   qa.IsAuditFree,
			IsDeleted:     model.QAIsNotDeleted,
			NextAction:    model.NextActionAdd,
			CharSize:      uint64(utf8.RuneCountInString(question)),
			CreateTime:    now,
			UpdateTime:    now,
		}
		sqs = append(sqs, sq)
	}

	return sqs
}

// NewSimilarQuestionsFromPB 从pb转换为model
func (d *dao) NewSimilarQuestionsFromPB(ctx context.Context, qa *model.DocQA, sqsPB []*pb.SimilarQuestion,
	nextAction uint32) []*model.SimilarQuestion {
	sqs := make([]*model.SimilarQuestion, 0)
	if qa == nil || len(sqsPB) == 0 {
		return sqs
	}
	releaseStatus := model.QAReleaseStatusInit
	isAuditFree := model.QAIsAuditNotFree
	isDeleted := model.QAIsNotDeleted
	if nextAction == model.NextActionDelete {
		isDeleted = model.QAIsDeleted
	}
	for _, q := range sqsPB {
		now := time.Now()
		question := strings.TrimSpace(q.Question)
		sq := &model.SimilarQuestion{
			SimilarID:     q.SimBizId,
			RobotID:       qa.RobotID,
			CorpID:        qa.CorpID,
			StaffID:       qa.StaffID,
			CreateUserID:  qa.StaffID,
			RelatedQAID:   qa.ID,
			Source:        qa.Source,
			Question:      question,
			ReleaseStatus: releaseStatus,
			IsAuditFree:   isAuditFree,
			IsDeleted:     isDeleted,
			NextAction:    nextAction,
			CharSize:      uint64(utf8.RuneCountInString(question)),
			UpdateTime:    now,
		}
		sqs = append(sqs, sq)
	}

	return sqs

}

// DeleteSimilarQuestions 删除相似问
func (d *dao) DeleteSimilarQuestions(ctx context.Context, tx *sqlx.Tx, sqs []*model.SimilarQuestion) error {
	if tx == nil || len(sqs) == 0 {
		return nil
	}
	sql := deleteSimilarQuestions
	args := make([]any, 0, 7+len(sqs))
	args = append(args, model.QAIsDeleted, time.Now(), model.QAReleaseStatusInit,
		model.QAIsAuditNotFree, model.NextActionDelete, sqs[0].CorpID, sqs[0].RobotID)
	for _, qa := range sqs {
		args = append(args, qa.SimilarID)
	}
	querySql := fmt.Sprintf(sql, placeholder(len(sqs)))
	if _, err := tx.ExecContext(ctx, querySql, args...); err != nil {
		log.ErrorContextf(ctx, "删除相似问失败, sql: %s, args: %+v, err: %+v", sql, args, err)
		return err
	}

	return nil
}

// DeleteSimilarQuestionsByQA 根据标准问删除相似问
func (d *dao) DeleteSimilarQuestionsByQA(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA) error {
	if tx == nil || qa == nil || qa.ID == 0 {
		return nil
	}
	sql := deleteSimilarQuestionsByQA
	args := make([]any, 0, 8)
	args = append(args, model.QAIsNotDeleted, time.Now(), model.QAReleaseStatusInit,
		model.QAIsAuditNotFree, model.NextActionDelete, qa.CorpID, qa.RobotID, qa.ID)
	if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "删除相似问失败, sql: %s, args: %+v, err: %+v", sql, args, err)
		return err
	}

	return nil
}

// UpdateSimilarQuestions 更新相似问, 复用QA的状态(Update时相似问状态严格和qa保持一致)
func (d *dao) UpdateSimilarQuestions(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA,
	sqs []*model.SimilarQuestion) error {
	if tx == nil || qa == nil || len(sqs) == 0 {
		return nil
	}
	for _, sq := range sqs {
		now := time.Now()
		// 完全和QA状态保持一致(不做额外兜底)
		similarQ := &model.SimilarQuestion{
			CorpID:        sq.CorpID,
			RobotID:       sq.RobotID,
			SimilarID:     sq.SimilarID,
			Question:      sq.Question,
			ReleaseStatus: qa.ReleaseStatus,
			IsAuditFree:   qa.IsAuditFree,
			NextAction:    qa.NextAction,
			CharSize:      uint64(utf8.RuneCountInString(sq.Question)),
			UpdateTime:    now,
		}
		sql := updateSimilarQuestion
		if _, err := tx.NamedExecContext(ctx, sql, similarQ); err != nil {
			log.ErrorContextf(ctx, "更新相似问失败, sql: %s, err: %+v", sql, err)
			return err
		}
	}

	return nil
}

// UpdateSimilarQuestionsStatus 更新相似问的状态(Update时相似问状态严格和qa保持一致)
func (d *dao) UpdateSimilarQuestionsStatus(ctx context.Context, tx *sqlx.Tx,
	sqs []*model.SimilarQuestion) error {
	if tx == nil || len(sqs) == 0 {
		return nil
	}
	for _, sq := range sqs {
		sql := updateSimilarQuestionStatus
		if _, err := tx.NamedExecContext(ctx, sql, sq); err != nil {
			log.ErrorContextf(ctx, "更新相似问状态失败, sql: %s, err: %+v", sql, err)
			return err
		}
	}
	return nil
}

// GetSimilarQuestionsCharSize 获取相似问的总长度(from db)
func (d *dao) GetSimilarQuestionsCharSize(ctx context.Context, qa *model.DocQA, similarQIDS []uint64) (uint64,
	error) {
	if qa == nil || len(similarQIDS) == 0 {
		return 0, nil
	}
	var count uint64
	args := []any{qa.RobotID, qa.CorpID, model.QAIsNotDeleted}
	exceededStatus := []any{
		model.QAReleaseStatusCharExceeded,
		model.QAReleaseStatusResuming,
		model.QAReleaseStatusAppealFailCharExceeded,
		model.QAReleaseStatusAppealFailResuming,
		model.QAReleaseStatusAuditNotPassCharExceeded,
		model.QAReleaseStatusAuditNotPassResuming,
		model.QAReleaseStatusLearnFailCharExceeded,
		model.QAReleaseStatusLearnFailResuming,
	}
	args = append(args, exceededStatus...)
	for _, id := range similarQIDS {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getSimilarQuestionsSize, placeholder(len(exceededStatus)), placeholder(len(similarQIDS)))
	log.DebugContextf(ctx, "GetSimilarQuestionsCharSize querySQL: %s, args: %+v", querySQL, args)
	if err := d.db.Get(ctx, &count, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsCharSize failed sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}

	return count, nil
}

// GetSimilarQuestionsCount 获取相似问的总数
func (d *dao) GetSimilarQuestionsCount(ctx context.Context, qa *model.DocQA) (int, error) {
	if qa == nil {
		log.WarnContextf(ctx, "GetSimilarQuestionsCount get invalid qa param %v", qa)
		return 0, nil
	}
	args := []any{qa.RobotID, qa.CorpID, model.QAIsNotDeleted, qa.ID}
	exceededStatus := []any{
		model.QAReleaseStatusCharExceeded,
		model.QAReleaseStatusResuming,
		model.QAReleaseStatusAppealFailCharExceeded,
		model.QAReleaseStatusAppealFailResuming,
		model.QAReleaseStatusAuditNotPassCharExceeded,
		model.QAReleaseStatusAuditNotPassResuming,
		model.QAReleaseStatusLearnFailCharExceeded,
		model.QAReleaseStatusLearnFailResuming,
	}
	args = append(args, exceededStatus...)
	querySQL := fmt.Sprintf(getSimilarQuestionCount, placeholder(len(exceededStatus)))
	var count int
	if err := d.db.Get(ctx, &count, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsCount failed sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}

	return count, nil
}

// GetSimilarQuestionsByQAWithoutRelease 根据标准问获取非已发布状态的相似问
func (d *dao) GetSimilarQuestionsByQAWithoutRelease(ctx context.Context, qa *model.DocQA) ([]*model.SimilarQuestion,
	error) {
	sqs := make([]*model.SimilarQuestion, 0)
	if qa == nil || qa.ID == 0 {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByQAWithoutRelease get invalid qa param %v", qa)
		return sqs, nil
	}
	// 忽略掉发布成功 和 待发布状态 的相似问
	notInStatus := []any{
		model.QAReleaseStatusInit,
		model.QAReleaseStatusSuccess,
	}
	querySql := fmt.Sprintf(getSimilarQuestionsByQAWithoutRelease, similarQuestionFields, placeholder(len(notInStatus)))
	args := []any{qa.CorpID, qa.RobotID, model.QAIsNotDeleted, qa.ID}
	args = append(args, notInStatus...)
	args = append(args, config.App().DocQA.SimilarQuestionNumLimit)
	log.DebugContextf(ctx, "GetSimilarQuestionsByQAWithoutRelease sql: %s, args: %+v", querySql, args)
	if err := d.db.QueryToStructs(ctx, &sqs, querySql, args...); err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByQAWithoutRelease failed, sql: %s, args: %+v, err: %+v", querySql,
			args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "GetSimilarQuestionsByQAWithoutRelease sqs len: %d", len(sqs))

	return sqs, nil
}

// GetSimilarQuestionsByQA 根据标准问获取所有相似问
func (d *dao) GetSimilarQuestionsByQA(ctx context.Context, qa *model.DocQA) ([]*model.SimilarQuestion, error) {
	sqs := make([]*model.SimilarQuestion, 0)
	if qa == nil || qa.ID == 0 {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByQAID get invalid qa param %v", qa)
		return sqs, nil
	}

	querySql := fmt.Sprintf(getSimilarQuestionsByQA, similarQuestionFields)
	args := []any{qa.CorpID, qa.RobotID, model.QAIsNotDeleted, qa.ID, config.App().DocQA.SimilarQuestionNumLimit}
	log.DebugContextf(ctx, "GetSimilarQuestionsByQAID sql: %s, args: %+v", querySql, args)
	if err := d.db.QueryToStructs(ctx, &sqs, querySql, args...); err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByQAID failed, sql: %s, args: %+v, err: %+v", querySql, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "GetSimilarQuestionsByQAID sqs len: %d", len(sqs))

	return sqs, nil
}

// GetSimilarQuestionsByUpdateTime 根据更新时间获取相似问
func (d *dao) GetSimilarQuestionsByUpdateTime(ctx context.Context, start, end time.Time, limit, offset uint64,
	appidList []uint64) ([]*model.SimilarQuestion, error) {
	sqs := make([]*model.SimilarQuestion, 0)
	querySql := fmt.Sprintf(getSimilarQuestionsByUpdateTime, similarQuestionFields)
	idStart := offset
	idEnd := offset + limit
	args := []any{idStart, idEnd, start, end, model.QAIsDeleted}
	if len(appidList) > 0 {
		querySql += fmt.Sprintf(" AND robot_id IN (%s)", placeholder(len(appidList)))
		for _, appid := range appidList {
			args = append(args, appid)
		}
	}
	log.InfoContextf(ctx, "GetSimilarQuestionsByUpdateTime args: %+v, sql:%s", args, querySql)
	if start.After(end) {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByUpdateTime failed, starttime is after endtime")
		return nil, fmt.Errorf("starttime is after endtime")
	}
	if err := d.db.QueryToStructs(ctx, &sqs, querySql, args...); err != nil && !mysql.IsNoRowsError(err) {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByUpdateTime failed, sql: %s, args: %+v, err: %+v",
			querySql, args, err)
		return nil, err
	}
	log.InfoContextf(ctx, "GetSimilarQuestionsByUpdateTime sqs len: %d", len(sqs))
	return sqs, nil
}

// GetSimilarQuestionsCountByQAIDs 获取标准问对应的相似问个数
func (d *dao) GetSimilarQuestionsCountByQAIDs(ctx context.Context, corpID, robotID uint64,
	qaIDs []uint64) (map[uint64]uint32, error) {
	var err error
	sqsMap := make(map[uint64]uint32)
	if len(qaIDs) == 0 {
		return sqsMap, nil
	}
	querySQL := fmt.Sprintf(getSimilarQuestionsCountByQAIDs, placeholder(len(qaIDs)))
	args := make([]any, 0, 3+len(qaIDs))
	args = append(args, corpID, robotID, model.QAIsNotDeleted)
	for _, docID := range qaIDs {
		args = append(args, docID)
	}
	totalList := make([]*model.SimilarQuestionCount, 0)
	if err = d.db.QueryToStructs(ctx, &totalList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据QAIDs获取相似问个数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, sq := range totalList {
		sqsMap[sq.RelatedQAID] = sq.Total
	}

	return sqsMap, nil
}

// GetSimilarQuestionsSimpleByQAIDs 根据qaIDs批量获取相似问id和内容
func (d *dao) GetSimilarQuestionsSimpleByQAIDs(ctx context.Context, corpID, robotID uint64,
	qaIDs []uint64) (map[uint64][]*model.SimilarQuestionSimple, error) {
	if len(qaIDs) == 0 {
		return nil, nil
	}
	var err error

	sqsMap := make(map[uint64][]*model.SimilarQuestionSimple)
	for _, qaIDChunks := range slicex.Chunk(qaIDs, MaxSqlInCount) {
		querySQL := fmt.Sprintf(getSimilarQuestionsSimpleByQAIDs, placeholder(len(qaIDChunks)))
		args := make([]any, 0, 5+len(qaIDChunks))
		args = append(args, corpID, robotID, model.QAIsNotDeleted)
		for _, docID := range qaIDChunks {
			args = append(args, docID)
		}
		limitFrom := 0
		limitOffset := 800
		args = append(args, limitFrom, limitOffset)
		// 分页获取相似问
		for {
			args[len(args)-2] = limitFrom
			log.DebugContextf(ctx, "分批获取相似问, from_limit:%+v", args[len(args)-2:])
			simList := make([]*model.SimilarQuestionSimple, 0)
			if err = d.db.QueryToStructs(ctx, &simList, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "根据QAIDs获取相似问对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
				return nil, err
			}
			for i, sq := range simList {
				if _, ok := sqsMap[sq.RelatedQAID]; !ok {
					sqsMap[sq.RelatedQAID] = make([]*model.SimilarQuestionSimple, 0)
				}
				sqsMap[sq.RelatedQAID] = append(sqsMap[sq.RelatedQAID], simList[i])
			}
			if len(simList) < limitOffset {
				break
			}
			limitFrom += limitOffset
		}
	}

	return sqsMap, nil
}

// GetQASimilarQuestionsCount 获取qa的相似问总数
func (d *dao) GetQASimilarQuestionsCount(ctx context.Context, corpID, appID uint64) (int, error) {
	query := getQASimilarQuestionsCount
	args := []any{corpID, appID, model.QAIsNotDeleted}
	var count int
	if err := d.db.Get(ctx, &count, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetQASimilarQuestionsCount fail, query: %s args: %+v err: %v", query, args, err)
		return 0, err
	}
	return count, nil
}

//// GetSimilarQuestionsTipsByQaIDs 根据qaIDs批量获取问答相似问的tips
//func (d *dao) GetSimilarQuestionsTipsByQaIDs(ctx context.Context, corpID, robotID uint64,
//	qaIDs []uint64, query string) (map[uint64][]*model.SimilarQuestionSimple, error) {
//
//}
