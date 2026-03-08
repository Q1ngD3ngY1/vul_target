package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"github.com/jmoiron/sqlx"
)

const (
	unsatisfiedReplyFields = `
        id,business_id,corp_id,robot_id,record_id,question,answer,context,status,staff_id,user_type,is_deleted,create_time,
		update_time
    `
	getUnsatisfiedReplyTotal = `
        SELECT
            COUNT(1)
        FROM
            t_unsatisfied_reply
        WHERE
            is_deleted = ? AND corp_id = ? AND robot_id = ? %s 
    `
	getUnsatisfiedReplyList = `
        SELECT
            %s 
        FROM
            t_unsatisfied_reply
        WHERE
            is_deleted = ?  AND corp_id = ? AND robot_id = ? %s 
        ORDER BY 
            id DESC 
        LIMIT ?,?
    `
	getUnsatisfiedReplyByIDs = `
        SELECT
            %s 
        FROM
            t_unsatisfied_reply
        WHERE
            is_deleted = ? AND status = ? AND corp_id = ? AND robot_id = ? AND id IN (?)
    `
	getUnsatisfiedReplyByBizIDs = `
        SELECT
            %s 
        FROM
            t_unsatisfied_reply
        WHERE
            is_deleted = ? AND status = ? AND corp_id = ? AND robot_id = ? AND business_id IN (?)
    `
	getUnsatisfiedReplyByRecordID = `
        SELECT
            %s 
        FROM
            t_unsatisfied_reply
        WHERE
            is_deleted = ? AND status = ? AND corp_id = ? AND robot_id = ? AND record_id = ?
    `
	addUnsatisfiedReply = `
        INSERT INTO
            t_unsatisfied_reply (%s)
        VALUES
            (:id,:business_id,:corp_id,:robot_id,:record_id,:question,:answer,:context,:status,
             :staff_id,:user_type,:is_deleted,NOW(),NOW())
    `
	updateUnsatisfiedReplyStatus = `
        UPDATE 
            t_unsatisfied_reply
        SET 
            status = ?, staff_id = ?
        WHERE 
            is_deleted = ? AND corp_id = ? AND robot_id = ? AND status = ? AND id IN (?) 
    `

	deleteUnsatisfiedReply = `
        UPDATE 
            t_unsatisfied_reply
        SET 
            is_deleted = :is_deleted
        WHERE 
            is_deleted = 0 AND id = :id
    `

	updateUnsatisfiedReply = `
		 UPDATE 
            t_unsatisfied_reply
        SET 
        	context = :context
        WHERE
        	is_deleted = :is_deleted AND status = :status AND id = :id
	`
	unsatisfiedReasonFields = `
        id,unsatisfied_id,reason,is_deleted
    `
	delUnsatisfiedReason = `
    	UPDATE
    		t_unsatisfied_reason
    	SET 
    		is_deleted = ?
    	WHERE 
    		is_deleted = ? AND unsatisfied_id = ?
    `
	addUnsatisfiedReason = `
        INSERT INTO
            t_unsatisfied_reason (%s)
        VALUES
            (:id,:unsatisfied_id,:reason,:is_deleted)
    `
	getUnsatisfiedIDsByReason = `
        SELECT 
            unsatisfied_id
        FROM
            t_unsatisfied_reason
        WHERE
            is_deleted = ? AND reason IN (?)
    `
	getUnsatisfiedReasonByUnsatisfiedIDs = `
        SELECT
           %s
        FROM
            t_unsatisfied_reason
        WHERE 
            is_deleted = ? AND unsatisfied_id IN (?)
    `
)

// AddUnsatisfiedReply 创建不满意回复
func (d *dao) AddUnsatisfiedReply(ctx context.Context, unsatisfiedReply *model.UnsatisfiedReplyInfo) error {
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		// 添加不满意回复
		execSQL := fmt.Sprintf(addUnsatisfiedReply, unsatisfiedReplyFields)
		res, err := tx.NamedExecContext(ctx, execSQL, unsatisfiedReply)
		if err != nil {
			log.ErrorContextf(ctx, "add unsatisfied reply execSQL:%s args:%+v err:%+v", execSQL,
				unsatisfiedReply, err)
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			log.ErrorContextf(ctx, "add unsatisfied reply execSQL:%s args:%+v err:%+v", execSQL,
				unsatisfiedReply, err)
			return err
		}
		// 添加不满意回复原因
		if len(unsatisfiedReply.Reasons) == 0 {
			return nil
		}
		unsatisfiedReasons := fillUnsatisfiedReason(ctx, uint64(id), unsatisfiedReply.Reasons)
		execSQL = fmt.Sprintf(addUnsatisfiedReason, unsatisfiedReasonFields)
		if _, err := tx.NamedExecContext(ctx, execSQL, unsatisfiedReasons); err != nil {
			log.ErrorContextf(ctx, "add unsatisfied reason execSQL:%s args:%+v err:%+v", execSQL,
				unsatisfiedReasons, err)
			return err
		}
		return nil
	})
	return err
}

// fillUnsatisfiedReason TODO
func fillUnsatisfiedReason(ctx context.Context, unsatisfiedID uint64, reasons []string) []*model.UnsatisfiedReason {
	unsatisfiedReasons := make([]*model.UnsatisfiedReason, 0)
	for _, reason := range reasons {
		unsatisfiedReasons = append(unsatisfiedReasons, &model.UnsatisfiedReason{
			UnsatisfiedID: unsatisfiedID,
			Reason:        reason,
			IsDeleted:     model.UnsatisfiedReasonIsNotDeleted,
		})
	}
	return unsatisfiedReasons
}

// UpdateUnsatisfiedReplyStatus 更新不满意回复状态
func (d *dao) UpdateUnsatisfiedReplyStatus(ctx context.Context, corpID, robotID uint64, ids []uint64, oldStatus,
	status uint32) error {
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.updateUnsatisfiedReplyStatus(ctx, tx, corpID, robotID, ids, oldStatus, status)
	})
	return err
}

// updateUnsatisfiedReplyStatus TODO
func (d *dao) updateUnsatisfiedReplyStatus(ctx context.Context, tx *sqlx.Tx, corpID, robotID uint64,
	ids []uint64, oldStatus, status uint32) error {
	if corpID == 0 || robotID == 0 || len(ids) == 0 || status == oldStatus {
		return errs.ErrUnsatisfiedReplyParams
	}
	execSQL := updateUnsatisfiedReplyStatus
	args := []any{status, pkg.StaffID(ctx), model.UnsatisfiedReplyIsNotDeleted, corpID, robotID, oldStatus, ids}
	execSQL, args, err := sqlx.In(execSQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "update unsatisfied status execSQL:%s args:%+v err:%+v", execSQL,
			args, err)
		return err
	}
	if _, err := tx.ExecContext(ctx, execSQL, args...); err != nil {
		log.ErrorContextf(ctx, "update unsatisfied status execSQL:%s args:%+v err:%+v", execSQL,
			args, err)
		return err
	}
	return nil
}

// GetUnsatisfiedReplyTotal 获取不满意回复数量
func (d *dao) GetUnsatisfiedReplyTotal(ctx context.Context, req *model.UnsatisfiedReplyListReq) (uint64, error) {
	var total uint64
	args, condition := fillUnsatisfiedReplyParams(req)
	querySQL := fmt.Sprintf(getUnsatisfiedReplyTotal, condition)
	querySQL, args, err := sqlx.In(querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply count sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return 0, err
	}
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply count sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return 0, err
	}
	return total, nil
}

// fillUnsatisfiedReplyParams TODO
func fillUnsatisfiedReplyParams(req *model.UnsatisfiedReplyListReq) ([]any, string) {
	var args []any
	var condition string
	// CorpID and RobotID
	args = append(args, model.UnsatisfiedReplyIsNotDeleted, req.CorpID,
		req.RobotID)
	// Query
	if len(req.Query) > 0 {
		condition += " AND (question LIKE ? OR answer LIKE ?)"
		args = append(args, "%"+req.Query+"%", "%"+req.Query+"%")
	}
	// Reasons
	if len(req.Reasons) > 0 {
		condition += fmt.Sprintf(" AND id IN (%s)", getUnsatisfiedIDsByReason)
		args = append(args, model.UnsatisfiedReasonIsNotDeleted, req.Reasons)
	}
	// IDs
	if len(req.IDs) > 0 {
		condition += " AND id IN (?)"
		args = append(args, req.IDs)
	}
	// BizIDs
	if len(req.BizIDs) > 0 {
		condition += " AND business_id IN (?)"
		args = append(args, req.BizIDs)
	}
	// 操作状态
	if req.Status == model.UnsatisfiedStatusNotHandle {
		condition += " AND status = 0  "

	} else if req.Status == model.UnsatisfiedStatusHandled {
		condition += " AND status in (1,2,3) AND update_time >= ?  "
		oneMonthAgo := time.Now().AddDate(0, -1, 0)
		args = append(args, oneMonthAgo)
	}
	return args, condition
}

// GetUnsatisfiedReplyList 获取不满意回复列表
func (d *dao) GetUnsatisfiedReplyList(ctx context.Context, req *model.UnsatisfiedReplyListReq) (
	[]*model.UnsatisfiedReplyInfo, error) {
	var replys []*model.UnsatisfiedReplyInfo
	args, condition := fillUnsatisfiedReplyParams(req)
	querySQL := fmt.Sprintf(getUnsatisfiedReplyList, unsatisfiedReplyFields, condition)
	// page and pageSize
	offset := (req.Page - 1) * req.PageSize
	args = append(args, offset, req.PageSize)
	// query
	querySQL, args, err := sqlx.In(querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply list sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	if err := d.db.QueryToStructs(ctx, &replys, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply list sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	// 获取不满意原因
	if err := d.getUnsatisfiedReasons(ctx, replys); err != nil {
		return nil, err
	}
	return replys, nil
}

// getUnsatisfiedReasons TODO
func (d *dao) getUnsatisfiedReasons(ctx context.Context, replys []*model.UnsatisfiedReplyInfo) error {
	if len(replys) == 0 {
		return nil
	}
	var reasons []*model.UnsatisfiedReason
	var unsatisfiedIDs []uint64
	for _, v := range replys {
		unsatisfiedIDs = append(unsatisfiedIDs, v.ID)
	}
	querySQL := fmt.Sprintf(getUnsatisfiedReasonByUnsatisfiedIDs, unsatisfiedReasonFields)
	args := []any{model.UnsatisfiedReasonIsNotDeleted, unsatisfiedIDs}
	querySQL, args, err := sqlx.In(querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reason sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return err
	}
	if err := d.db.QueryToStructs(ctx, &reasons, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reason sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return err
	}
	mapUnsatisfiedID2Reasons := make(map[uint64][]string)
	for _, v := range reasons {
		mapUnsatisfiedID2Reasons[v.UnsatisfiedID] = append(mapUnsatisfiedID2Reasons[v.UnsatisfiedID], v.Reason)
	}
	for _, v := range replys {
		v.Reasons = mapUnsatisfiedID2Reasons[v.ID]
	}
	return nil
}

// GetUnsatisfiedReplyByIDs 通过不满意回复ID获取不满意记录
func (d *dao) GetUnsatisfiedReplyByIDs(ctx context.Context, corpID, robotID uint64, ids []uint64) (
	[]*model.UnsatisfiedReplyInfo, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var replys []*model.UnsatisfiedReplyInfo
	querySQL := fmt.Sprintf(getUnsatisfiedReplyByIDs, unsatisfiedReplyFields)
	args := []any{model.UnsatisfiedReplyIsNotDeleted, model.UnsatisfiedReplyStatusWait, corpID, robotID, ids}
	querySQL, args, err := sqlx.In(querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied by ids sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	if err := d.db.QueryToStructs(ctx, &replys, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply by ids sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	// 获取不满意原因
	if err := d.getUnsatisfiedReasons(ctx, replys); err != nil {
		return nil, err
	}
	return replys, nil
}

// GetUnsatisfiedReplyByBizIDs 通过不满意回复BizID获取不满意记录
func (d *dao) GetUnsatisfiedReplyByBizIDs(ctx context.Context, corpID, robotID uint64, ids []uint64) (
	[]*model.UnsatisfiedReplyInfo, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var replys []*model.UnsatisfiedReplyInfo
	querySQL := fmt.Sprintf(getUnsatisfiedReplyByBizIDs, unsatisfiedReplyFields)
	args := []any{model.UnsatisfiedReplyIsNotDeleted, model.UnsatisfiedReplyStatusWait, corpID, robotID, ids}
	querySQL, args, err := sqlx.In(querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied by ids sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	if err := d.db.QueryToStructs(ctx, &replys, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply by ids sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	// 获取不满意原因
	if err := d.getUnsatisfiedReasons(ctx, replys); err != nil {
		return nil, err
	}
	return replys, nil
}

// GetUnsatisfiedReplyByRecordID 通过记录ID获取不满意回复
func (d *dao) GetUnsatisfiedReplyByRecordID(ctx context.Context, corpID, robotID uint64, recordID string) (
	*model.UnsatisfiedReplyInfo, error) {
	var replys []*model.UnsatisfiedReplyInfo
	querySQL := fmt.Sprintf(getUnsatisfiedReplyByRecordID, unsatisfiedReplyFields)
	args := []any{model.UnsatisfiedReplyIsNotDeleted, model.UnsatisfiedReplyStatusWait, corpID, robotID,
		recordID}
	if err := d.db.QueryToStructs(ctx, &replys, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply list sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	if len(replys) == 0 {
		return nil, nil
	}
	// 获取不满意原因
	if err := d.getUnsatisfiedReasons(ctx, replys); err != nil {
		return nil, err
	}
	return replys[0], nil
}

// UpdateUnsatisfiedReply 更新不满意回复
func (d *dao) UpdateUnsatisfiedReply(ctx context.Context, unsatisfiedReply *model.UnsatisfiedReplyInfo) error {
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if unsatisfiedReply.IsDeleted == model.UnsatisfiedReplyDeleted { // 如果是取消反馈场景，则直接删除不满意问题
			delSQL := deleteUnsatisfiedReply
			if _, err := tx.NamedExecContext(ctx, delSQL, unsatisfiedReply); err != nil {
				log.ErrorContextf(ctx, "delete unsatisfied reply execSQL:%s args:%+v err:%+v", delSQL,
					unsatisfiedReply, err)
				return err
			}
		} else {
			// 更新不满意回复
			execSQL := updateUnsatisfiedReply
			if _, err := tx.NamedExecContext(ctx, execSQL, unsatisfiedReply); err != nil {
				log.ErrorContextf(ctx, "update unsatisfied reply execSQL:%s args:%+v err:%+v", execSQL,
					unsatisfiedReply, err)
				return err
			}
		}
		// 更新不满意问题原因
		execSQL := delUnsatisfiedReason
		if _, err := tx.ExecContext(ctx, execSQL, model.UnsatisfiedReasonDeleted,
			model.UnsatisfiedReasonIsNotDeleted, unsatisfiedReply.ID); err != nil {
			log.ErrorContextf(ctx, "delete unsatisfied reason execSQL:%s args:%+v err:%+v", execSQL,
				unsatisfiedReply, err)
			return err
		}
		if len(unsatisfiedReply.Reasons) == 0 {
			return nil
		}
		// 添加不满意回复原因
		unsatisfiedReasons := fillUnsatisfiedReason(ctx, unsatisfiedReply.ID, unsatisfiedReply.Reasons)
		execSQL = fmt.Sprintf(addUnsatisfiedReason, unsatisfiedReasonFields)
		if _, err := tx.NamedExecContext(ctx, execSQL, unsatisfiedReasons); err != nil {
			log.ErrorContextf(ctx, "add unsatisfied reason execSQL:%s args:%+v err:%+v", execSQL,
				unsatisfiedReasons, err)
			return err
		}
		return nil
	})
	return err
}
