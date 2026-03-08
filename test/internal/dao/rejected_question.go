package dao

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	rejectedQuestionField = `
		id,business_id,corp_id,robot_id,create_staff_id,business_source_id,business_source,question,release_status
			,is_deleted,action,update_time,create_time
	`
	getRejectedQuestionCount = `
		SELECT
			COUNT(*) total
		FROM
			t_rejected_question
		WHERE
			corp_id = ? 
			AND is_deleted = ? 
			AND robot_id = ? %s
	`
	getRejectedQuestionList = `
		SELECT
			%s
		FROM
			t_rejected_question
		WHERE
			corp_id = ? 
			AND is_deleted = ? 
			AND robot_id = ? %s
		ORDER BY 
			update_time DESC,id DESC 
		LIMIT
			?,?
	`
	createRejectedQuestion = `
		INSERT INTO
			t_rejected_question (%s)
		VALUES
			(null,:business_id,:corp_id,:robot_id,:create_staff_id,:business_source_id,:business_source,:question,
			 :release_status,:is_deleted,:action,:update_time,:create_time)
	`
	updateRejectedQuestionReleaseStatus = `
		UPDATE
			t_rejected_question
		SET
			update_time = ?,
			release_status = ?
		WHERE
			id IN (%s)
	`
	updateRejectedQuestionRelease = `
		UPDATE
			t_rejected_question
		SET
			release_status = ?,
			update_time = ?
		WHERE
			id IN (%s)
	`
	updateRejectedQuestion = `
		UPDATE
			t_rejected_question
		SET
			release_status = :release_status,
			question = :question,
			action = :action,
			update_time = :update_time
		WHERE
			id = :id
	`
	publishRejectedQuestion = `
		UPDATE
			t_rejected_question
		SET
			action = :action,
			release_status = :release_status,
			update_time = update_time
		WHERE
			id = :id
	`
	publishReleaseRejectedQuestion = `
		UPDATE
			t_release_rejected_question
		SET
			release_status = :release_status,
			message = :message,
			update_time = update_time
		WHERE
			id = :id
	`
	batchDeleteRejectedQuestion = `
		UPDATE
			t_rejected_question
		SET
			action = ?,
			is_deleted = ?,
			release_status = ?,
			update_time = ?
		WHERE
			corp_id = ?
			AND robot_id = ? %s
	`
	batchDeleteRejectedQuestionAdd = `
		UPDATE
			t_rejected_question
		SET
			is_deleted = ?,
			update_time = ?
		WHERE
			corp_id = ?
			AND robot_id = ? %s
	`
	getReleaseRejectedQuestionCount = `
		SELECT
			COUNT(*) total
		FROM
			t_rejected_question
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND release_status = ?  
			AND !(action = ? AND is_deleted = ?) %s
	`
	getReleaseRejectedQuestionList = `
		SELECT
			%s
		FROM
			t_rejected_question
		WHERE
			corp_id = ?
			AND robot_id = ? 
			AND release_status = ?
			AND !(action = ? AND is_deleted = ?) %s
		ORDER BY 
			update_time DESC,id DESC  
		LIMIT
			?,?
	`
	getReleaseRejectedQuestionByID = `
	    SELECT
      		%s
    	FROM
      		t_rejected_question
		WHERE
			id = ? AND corp_id = ? AND robot_id = ?
	`
	getReleaseRejectedQuestionByBizID = `
	    SELECT
      		%s
    	FROM
      		t_rejected_question
		WHERE
			business_id = ? AND corp_id = ? AND robot_id = ?
	`
	getReleaseRejectedQuestionByIDs = `
	    SELECT
      		%s
    	FROM
      		t_rejected_question
		WHERE
			corp_id = ?
			AND id IN (%s)
	`
	getReleaseRejectedQuestionByBizIDs = `
	    SELECT
      		%s
    	FROM
      		t_rejected_question
		WHERE
			corp_id = ?
			AND business_id IN (%s)
	`
	getRejectChunk = `
		SELECT ` + rejectedQuestionField + ` FROM t_rejected_question
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND id > ?
		ORDER BY id ASC LIMIT ?
	`
	getRejectChunkCount = `
		SELECT COUNT(*) FROM t_rejected_question
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ?
	`
)

// GetReleaseRejectedQuestionList 获取待发布拒答问题列表
func (d *dao) GetReleaseRejectedQuestionList(ctx context.Context, corpID, robotID uint64, page, pageSize uint32,
	query string, startTime, endTime time.Time, status []uint32) ([]*model.RejectedQuestion, error) {
	condition := ""
	args := make([]any, 0, 5)
	args = append(args, corpID, robotID, model.RejectedQuestionReleaseStatusInit, model.RejectedQuestionAdd,
		model.RejectedQuestionIsDeleted)
	if len(query) > 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ?")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(query)))
	}

	if !startTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time >= ? ")
		args = append(args, startTime)
	}
	if !endTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time <= ? ")
		args = append(args, endTime)
	}

	if len(status) > 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND action IN (?"+strings.Repeat(", ?", len(status)-1)+")")
		for _, action := range status {
			args = append(args, action)
		}
	}

	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getReleaseRejectedQuestionList, rejectedQuestionField, condition)
	rejectedQuestions := make([]*model.RejectedQuestion, 0)
	if err := d.db.QueryToStructs(ctx, &rejectedQuestions, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取拒答问题列表失败 sql:%s, args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return rejectedQuestions, nil
}

// GetRejectedQuestionList 获取拒答问题列表
func (d *dao) GetRejectedQuestionList(ctx context.Context,
	req model.GetRejectedQuestionListReq) (uint64, []*model.RejectedQuestion, error) {
	condition := ""
	args := make([]any, 0, 4)
	args = append(args, req.CorpID, model.RejectedQuestionIsNotDeleted, req.RobotID)
	if len(req.Query) > 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ?")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(req.Query)))
	}
	if len(req.Actions) > 0 {
		condition = fmt.Sprintf("%s AND action IN (%s)", condition, placeholder(len(req.Actions)))
		for i := range req.Actions {
			args = append(args, req.Actions[i])
		}
	}
	countSQL := fmt.Sprintf(getRejectedQuestionCount, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, countSQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取拒答问题列表数量失败 countSQL:%s args:%+v err:%+v", countSQL, args, err)
		return 0, nil, err
	}

	offset := (req.Page - 1) * req.PageSize
	args = append(args, offset, req.PageSize)
	querySQL := fmt.Sprintf(getRejectedQuestionList, rejectedQuestionField, condition)
	rejectedQuestions := make([]*model.RejectedQuestion, 0)
	if err := d.db.QueryToStructs(ctx, &rejectedQuestions, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取拒答问题列表失败 sql:%s, args:%+v err:%+v", querySQL, args, err)
		return total, nil, err
	}

	return total, rejectedQuestions, nil
}

// CreateRejectedQuestion 创建拒答问题
func (d *dao) CreateRejectedQuestion(ctx context.Context, rejectedQuestion *model.RejectedQuestion) error {
	var syncID uint64
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		rejectedQuestion.UpdateTime = now
		rejectedQuestion.CreateTime = now
		rejectedQuestion.ReleaseStatus = model.RejectedQuestionReleaseStatusInit
		rejectedQuestion.IsDeleted = model.RejectedQuestionIsNotDeleted
		rejectedQuestion.Action = model.RejectedQuestionAdd
		sql := fmt.Sprintf(createRejectedQuestion, rejectedQuestionField)
		res, err := tx.NamedExecContext(ctx, sql, rejectedQuestion)
		if err != nil {
			log.ErrorContextf(ctx, "创建拒答问题失败 sql:%s args:%+v err:%+v", sql, rejectedQuestion, err)
			return err
		}
		id, _ := res.LastInsertId()
		rejectedQuestion.ID = uint64(id)
		syncID, err = d.addRejectedQuestionSync(ctx, tx, rejectedQuestion)
		if err != nil {
			return err
		}
		if rejectedQuestion.BusinessSource == model.BusinessSourceUnsatisfiedReply &&
			rejectedQuestion.BusinessSourceID != 0 {
			err := d.updateUnsatisfiedReplyStatus(ctx, tx, rejectedQuestion.CorpID, rejectedQuestion.RobotID,
				[]uint64{rejectedQuestion.BusinessSourceID}, model.UnsatisfiedReplyStatusWait,
				model.UnsatisfiedReplyStatusReject)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	d.vector.Push(ctx, syncID)
	return nil
}

// UpdateRejectedQuestion 修改拒答问题
func (d *dao) UpdateRejectedQuestion(ctx context.Context, rejectedQuestion *model.RejectedQuestion,
	isNeedPublish bool) error {
	var syncID uint64
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		rejectedQuestion.UpdateTime = now
		sql := updateRejectedQuestion
		_, err := tx.NamedExecContext(ctx, sql, rejectedQuestion)
		if err != nil {
			log.ErrorContextf(ctx, "更新拒答问题失败 sql:%s args:%+v err:%+v", sql, rejectedQuestion, err)
			return err
		}
		if !isNeedPublish {
			return nil
		}
		syncID, err = d.addRejectedQuestionSync(ctx, tx, rejectedQuestion)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !isNeedPublish {
		return nil
	}
	d.vector.Push(ctx, syncID)
	return nil
}

// DeleteRejectedQuestion 删除拒答问题
func (d *dao) DeleteRejectedQuestion(ctx context.Context, corpID, robotID uint64,
	rejectedQuestions []*model.RejectedQuestion) error {
	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.deleteRejectedQuestion(ctx, tx, corpID, robotID, rejectedQuestions)
	})
}

func (d *dao) deleteRejectedQuestion(ctx context.Context, tx *sqlx.Tx, corpID, robotID uint64,
	rejectedQuestions []*model.RejectedQuestion) error {
	rejectedQAAdd := make([]*model.RejectedQuestion, 0, len(rejectedQuestions))
	rejectedQANotAdd := make([]*model.RejectedQuestion, 0, len(rejectedQuestions))
	for _, v := range rejectedQuestions {
		if v.Action == model.RejectedQuestionAdd {
			rejectedQAAdd = append(rejectedQAAdd, v)
		} else {
			rejectedQANotAdd = append(rejectedQANotAdd, v)
		}
	}
	pageSize := 100
	pagesNotAdd := int(math.Ceil(float64(len(rejectedQANotAdd)) / float64(pageSize)))
	syncIDsNotAdd, err := d.deleteRejectedQuestionNotAdd(ctx, tx, corpID, robotID, pagesNotAdd, pageSize,
		rejectedQANotAdd)
	if err != nil {
		return err
	}
	pagesAdd := int(math.Ceil(float64(len(rejectedQAAdd)) / float64(pageSize)))
	syncIDsAdd, err := d.deleteRejectedQuestionAdd(ctx, tx, corpID, robotID, pagesAdd, pageSize, rejectedQAAdd)
	if err != nil {
		return err
	}
	var syncIDs []uint64
	syncIDs = append(syncIDs, syncIDsNotAdd...)
	syncIDs = append(syncIDs, syncIDsAdd...)
	for _, syncID := range syncIDs {
		d.vector.Push(ctx, syncID)
	}
	return nil
}

func (d *dao) deleteRejectedQuestionAdd(ctx context.Context, tx *sqlx.Tx, corpID uint64, robotID uint64, pagesAdd int,
	pageSize int, rejectedQAAdd []*model.RejectedQuestion) ([]uint64, error) {
	length := len(rejectedQAAdd)
	now := time.Now()
	var syncIDs []uint64
	for i := 0; i < pagesAdd; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpRejectedQuestion := rejectedQAAdd[start:end]
		args := make([]any, 0, 7+len(tmpRejectedQuestion))
		args = append(args,
			model.RejectedQuestionIsDeleted,
			now,
			corpID,
			robotID,
		)
		condition := fmt.Sprintf("AND id IN (%s) ", placeholder(len(tmpRejectedQuestion)))

		for _, id := range tmpRejectedQuestion {
			args = append(args, id.ID)
		}
		sql := fmt.Sprintf(batchDeleteRejectedQuestionAdd, condition)

		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			log.ErrorContextf(ctx, "删除拒答问题失败 sql:%s, args:%+v, err:%+v", sql, args, err)
			return nil, err
		}
		for _, id := range tmpRejectedQuestion {
			syncID, err := d.addRejectedQuestionSync(ctx, tx, &model.RejectedQuestion{
				ID: id.ID,
			})
			if err != nil {
				return nil, err
			}
			syncIDs = append(syncIDs, syncID)
		}
	}
	return syncIDs, nil
}

func (d *dao) deleteRejectedQuestionNotAdd(ctx context.Context, tx *sqlx.Tx, corpID uint64, robotID uint64,
	pagesNotAdd int, pageSize int, rejectedQANotAdd []*model.RejectedQuestion) ([]uint64, error) {
	length := len(rejectedQANotAdd)
	now := time.Now()
	var syncIDs []uint64
	for i := 0; i < pagesNotAdd; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpRejectedQuestion := rejectedQANotAdd[start:end]
		args := make([]any, 0, 7+len(tmpRejectedQuestion))
		args = append(args,
			model.RejectedQuestionDelete,
			model.RejectedQuestionIsDeleted,
			model.RejectedQuestionReleaseStatusInit,
			now,
			corpID,
			robotID,
		)
		condition := fmt.Sprintf("AND id IN (%s) ", placeholder(len(tmpRejectedQuestion)))

		for _, id := range tmpRejectedQuestion {
			args = append(args, id.ID)
		}
		sql := fmt.Sprintf(batchDeleteRejectedQuestion, condition)

		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			log.ErrorContextf(ctx, "删除拒答问题失败 sql:%s, args:%+v, err:%+v", sql, args, err)
			return nil, err
		}
		for _, id := range tmpRejectedQuestion {
			syncID, err := d.addRejectedQuestionSync(ctx, tx, &model.RejectedQuestion{
				ID: id.ID,
			})
			if err != nil {
				return nil, err
			}
			syncIDs = append(syncIDs, syncID)
		}
	}
	return syncIDs, nil
}

// GetReleaseRejectedQuestionCount 发布拒答问题预览数量
func (d *dao) GetReleaseRejectedQuestionCount(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, status []uint32) (uint64, error) {
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, model.RejectedQuestionReleaseStatusInit, model.RejectedQuestionAdd,
		model.RejectedQuestionIsDeleted)
	condition := ""
	if question != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(question)))
	}

	if !startTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time >= ? ")
		args = append(args, startTime)
	}
	if !endTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time <= ? ")
		args = append(args, endTime)
	}

	if len(status) > 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND action IN (?"+strings.Repeat(", ?", len(status)-1)+")")
		for _, action := range status {
			args = append(args, action)
		}
	}

	var total uint64
	SQL := fmt.Sprintf(getReleaseRejectedQuestionCount, condition)

	if err := d.db.Get(ctx, &total, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询拒答问题数量失败 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return 0, err
	}

	return total, nil
}

// GetRejectedQuestionByID 按 ID 查询拒答问题
func (d *dao) GetRejectedQuestionByID(ctx context.Context, corpId, robotId, id uint64) (*model.RejectedQuestion, error) {
	rejectedQuestion := &model.RejectedQuestion{}
	SQL := fmt.Sprintf(getReleaseRejectedQuestionByID, rejectedQuestionField)
	args := []any{id, corpId, robotId}
	if err := d.db.QueryToStruct(ctx, rejectedQuestion, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "按ID查询拒答问题 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return nil, err
	}
	return rejectedQuestion, nil
}

// GetRejectedQuestionByBizID 按 bizID 查询拒答问题
func (d *dao) GetRejectedQuestionByBizID(ctx context.Context, corpId, robotId, bizID uint64) (*model.RejectedQuestion, error) {
	rejectedQuestion := &model.RejectedQuestion{}
	SQL := fmt.Sprintf(getReleaseRejectedQuestionByBizID, rejectedQuestionField)
	args := []any{bizID, corpId, robotId}
	if err := d.db.QueryToStruct(ctx, rejectedQuestion, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "按bizID查询拒答问题 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return nil, err
	}
	return rejectedQuestion, nil
}

// GetRejectedQuestionByIDs 按多个ID查询拒答问题
func (d *dao) GetRejectedQuestionByIDs(ctx context.Context, corpID uint64, ids []uint64) ([]*model.RejectedQuestion,
	error) {
	rejectedQuestions := make([]*model.RejectedQuestion, 0, len(ids)+1)
	if len(ids) == 0 {
		return rejectedQuestions, nil
	}
	SQL := fmt.Sprintf(getReleaseRejectedQuestionByIDs, rejectedQuestionField, placeholder(len(ids)))
	args := []any{corpID}
	for _, id := range ids {
		args = append(args, id)
	}
	if err := d.db.QueryToStructs(ctx, &rejectedQuestions, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "按多个ID查询拒答问题 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return nil, err
	}
	return rejectedQuestions, nil
}

// GetRejectedQuestionByBizIDs 按多个业务ID查询拒答问题
func (d *dao) GetRejectedQuestionByBizIDs(ctx context.Context, corpID uint64,
	bizIDs []uint64) ([]*model.RejectedQuestion,
	error) {
	rejectedQuestions := make([]*model.RejectedQuestion, 0, len(bizIDs)+1)
	if len(bizIDs) == 0 {
		return rejectedQuestions, nil
	}
	SQL := fmt.Sprintf(getReleaseRejectedQuestionByBizIDs, rejectedQuestionField, placeholder(len(bizIDs)))
	args := []any{corpID}
	for _, id := range bizIDs {
		args = append(args, id)
	}
	if err := d.db.QueryToStructs(ctx, &rejectedQuestions, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "按多个业务ID查询拒答问题 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return nil, err
	}
	return rejectedQuestions, nil
}

func (d *dao) PublishRejectedQuestion(ctx context.Context, rejectedQuestion *model.RejectedQuestion,
	modifyRejectedQuestion *model.ReleaseRejectedQuestion) error {
	now := time.Now()
	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		SQL := publishRejectedQuestion
		rejectedQuestion.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, SQL, rejectedQuestion); err != nil {
			log.ErrorContextf(ctx, "发布拒答问题失败 SQL:%s rejectedQuestion:%+v err:%+v", SQL, rejectedQuestion,
				err)
			return err
		}
		SQL = publishReleaseRejectedQuestion
		modifyRejectedQuestion.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, SQL, modifyRejectedQuestion); err != nil {
			log.ErrorContextf(ctx, "发布拒答问题失败 SQL:%s modifyRejectedQuestion:%+v err:%+v", SQL,
				modifyRejectedQuestion, err)
			return err
		}
		return nil
	})
}

// GetRejectChunk 分段获取拒答
func (d *dao) GetRejectChunk(
	ctx context.Context, corpID, appID, offset, limit uint64,
) ([]*model.RejectedQuestion, error) {
	query := getRejectChunk
	args := []any{corpID, appID, model.RejectedQuestionIsNotDeleted, offset, limit}
	var rejects []*model.RejectedQuestion
	if err := d.db.Select(ctx, &rejects, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetRejectChunk fail, query: %s args: %+v err: %v", query, args, err)
		return nil, err
	}
	return rejects, nil
}

// GetRejectChunkCount 获取拒答总数
func (d *dao) GetRejectChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	query := getRejectChunkCount
	args := []any{corpID, appID, model.RejectedQuestionIsNotDeleted}
	var count int
	if err := d.db.Get(ctx, &count, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetRejectChunkCount fail, query: %s args: %+v err: %v", query, args, err)
		return 0, err
	}
	return count, nil
}
