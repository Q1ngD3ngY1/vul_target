package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"github.com/jmoiron/sqlx"
)

const (
	qaSimilarFields = `
		id,business_id,robot_id,corp_id,staff_id,doc_id,qa_id,similar_id,is_valid,status,create_time,update_time
	`
	createQaSimilar = `
		INSERT INTO
			t_doc_qa_similar (%s)
		VALUES
			(null,:business_id,:robot_id,:corp_id,:staff_id,:doc_id,:qa_id,:similar_id,:is_valid,:status,:create_time,
			 :update_time)`

	getSimilarCount = `
		SELECT
			count(*)
		FROM
			t_doc_qa_similar
		WHERE 
			((qa_id = ? AND similar_id = ?) OR (qa_id = ? AND similar_id = ?))
		AND corp_id = ?	AND is_valid = ?	AND status = ?
		`
	getQASimilarCount = `
		SELECT 
			count(*) 
		FROM 
		    t_doc_qa_similar 
		WHERE 
		    corp_id = ? AND robot_id = ? AND is_valid = ? AND status = ?  	
	`
	getQASimilarList = `
		SELECT 
			%s 
		FROM 
		    t_doc_qa_similar 
		WHERE 
		     corp_id = ? AND robot_id = ? AND is_valid = ? AND status = ?  
		ORDER BY 
		    create_time DESC 
		LIMIT 
			?, ?
	`
	getQASimilarID = `
		SELECT 
			%s 
		FROM 
		    t_doc_qa_similar 
		WHERE 
		     corp_id = ? AND id = ?
		LIMIT 1
	`
	getQASimilarBizID = `
		SELECT 
			%s 
		FROM 
		    t_doc_qa_similar 
		WHERE 
		     corp_id = ? AND business_id = ?
		LIMIT 1
	`

	delQqSimilar = `
		UPDATE 
			t_doc_qa_similar 
		SET 
		    is_valid = ?, status = ?, update_time = ? 
		WHERE 
		    id IN (%s)
	`
	delQASimilarByBizID = `
		UPDATE 
			t_doc_qa_similar 
		SET 
		    is_valid = ?, status = ?, update_time = ? 
		WHERE 
		    business_id IN (%s)
	`
	ignoreAllQASimilar = `
		UPDATE 
			t_doc_qa_similar 
		SET 
		    is_valid = ?, status = ?, update_time = ? 
		WHERE 
		    corp_id = ? AND robot_id = ? AND is_valid = ? AND status = ?
	`
	// SyncDelQaSimilar 同步删除相似问答对
	SyncDelQaSimilar = `
		UPDATE 
			t_doc_qa_similar 
		SET 
		    is_valid = ?, update_time = ? 
		WHERE 
		    (qa_id IN (%s) OR similar_id IN (%s)) AND status = ?
	`
)

const (
	docQaSimTableName = "t_qa_similar_question"

	DocQaSimTblColId            = "id"
	DocQaSimTblColSimilarId     = "similar_id"
	DocQaSimTblColRobotId       = "robot_id"
	DocQaSimTblColRelatedQaId   = "related_qa_id"
	DocQaSimTblColReleaseStatus = "release_status"
	DocQaSimTblColIsDelted      = "is_deleted"
	DocQaSimTblColCreateTime    = "create_time"
	DocQaSimTblColUpdateTime    = "update_time"
)

// SaveQaSimilar 保存相似问答对
func (d *dao) SaveQaSimilar(ctx context.Context, qaSimilar *model.DocQASimilar) error {
	args := []any{qaSimilar.QaID, qaSimilar.SimilarID, qaSimilar.SimilarID, qaSimilar.QaID, qaSimilar.CorpID,
		model.QaSimilarIsValid, model.QaSimilarStatusInit}
	// 避免插入重复数据
	querySQL := getSimilarCount
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取互为相似问答总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	if total > 0 {
		log.DebugContextf(ctx, "保存相似问答对 已存在未处理的相似问答对 total:%+v", total)
		return nil
	}
	qaSimilar.BusinessID = d.GenerateSeqID()
	querySQL = fmt.Sprintf(createQaSimilar, qaSimilarFields)
	_, err := d.db.NamedExec(ctx, querySQL, qaSimilar)
	if err != nil {
		log.ErrorContextf(ctx, "保存相似问答对失败 sql:%s args:%+v err:%+v", querySQL, qaSimilar, err)
		return err
	}
	return nil
}

// GetQASimilarList 获取相似问答对列表
func (d *dao) GetQASimilarList(ctx context.Context, corpID, robotID uint64, page, pageSize uint32) (
	uint64, []uint64, error) {
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, model.QaSimilarIsValid, model.QaSimilarStatusInit)
	var total uint64
	querySQL := getQASimilarCount
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取相似问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL = fmt.Sprintf(getQASimilarList, qaSimilarFields)
	qaSimilarList := make([]*model.DocQASimilar, 0)
	if err := d.db.QueryToStructs(ctx, &qaSimilarList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取相似问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	var similarIDs []uint64
	for _, similar := range qaSimilarList {
		similarIDs = append(similarIDs, similar.ID)
	}
	return total, similarIDs, nil
}

// ListQASimilar 获取相似问答对列表
func (d *dao) ListQASimilar(ctx context.Context, corpID, robotID uint64, pageNumber, pageSize uint32) (
	uint64, []*model.DocQASimilar, error) {
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, model.QaSimilarIsValid, model.QaSimilarStatusInit)
	var total uint64
	querySQL := getQASimilarCount
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取相似问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	offset := (pageNumber - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL = fmt.Sprintf(getQASimilarList, qaSimilarFields)
	qaSimilarList := make([]*model.DocQASimilar, 0)
	if err := d.db.QueryToStructs(ctx, &qaSimilarList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取相似问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	return total, qaSimilarList, nil
}

// GetQASimilarID 获取相似问答对 ID
func (d *dao) GetQASimilarID(ctx context.Context, corpID, id uint64) (
	*model.DocQASimilar, error) {
	args := []any{corpID, id}
	querySQL := fmt.Sprintf(getQASimilarID, qaSimilarFields)
	qaSimilarList := make([]*model.DocQASimilar, 0)
	if err := d.db.QueryToStructs(ctx, &qaSimilarList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取相似QA失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(qaSimilarList) == 0 {
		return nil, errs.ErrQASimilarNotFound
	}
	return qaSimilarList[0], nil
}

// GetQASimilarBizID 获取相似问答对 ID
func (d *dao) GetQASimilarBizID(ctx context.Context, corpID, bizID uint64) (
	*model.DocQASimilar, error) {
	args := []any{corpID, bizID}
	querySQL := fmt.Sprintf(getQASimilarBizID, qaSimilarFields)
	qaSimilarList := make([]*model.DocQASimilar, 0)
	if err := d.db.QueryToStructs(ctx, &qaSimilarList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取相似QA失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(qaSimilarList) == 0 {
		return nil, errs.ErrQASimilarNotFound
	}
	return qaSimilarList[0], nil
}

// DoQASimilar 处理相似QA
func (d *dao) DoQASimilar(ctx context.Context, corpID, robotID, staffID uint64, qaSimilarID []uint64,
	delQas []*model.DocQA) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if len(qaSimilarID) == 0 {
			return nil
		}
		args := make([]any, 0, len(qaSimilarID))
		args = append(args, model.QaSimilarInValid, model.QaSimilarStatusDone, time.Now())
		for _, id := range qaSimilarID {
			args = append(args, id)
		}
		querySQL := fmt.Sprintf(delQqSimilar, placeholder(len(qaSimilarID)))
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "处理相似问答失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		if err := d.deleteQAs(ctx, tx, corpID, robotID, staffID, delQas); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "处理相似问答失败 err:%+v", err)
		return err
	}
	return nil
}

// DoQABizSimilar 处理相似QA
func (d *dao) DoQABizSimilar(ctx context.Context, corpID, robotID, staffID uint64, qaSimilarBizID []uint64,
	delQas []*model.DocQA) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if len(qaSimilarBizID) == 0 {
			return nil
		}
		args := make([]any, 0, len(qaSimilarBizID))
		args = append(args, model.QaSimilarInValid, model.QaSimilarStatusDone, time.Now())
		for _, id := range qaSimilarBizID {
			args = append(args, id)
		}
		querySQL := fmt.Sprintf(delQASimilarByBizID, placeholder(len(qaSimilarBizID)))
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "处理相似问答失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		if err := d.deleteQAs(ctx, tx, corpID, robotID, staffID, delQas); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "处理相似问答失败 err:%+v", err)
		return err
	}
	return nil
}

// IgnoreAllQASimilar 忽略当前所有相似问答对
func (d *dao) IgnoreAllQASimilar(ctx context.Context, corpID, robotID uint64) error {
	args := make([]any, 0, 8)
	args = append(args, model.QaSimilarInValid, model.QaSimilarStatusDone, time.Now(),
		corpID, robotID, model.QaSimilarIsValid, model.QaSimilarStatusInit)
	querySQL := ignoreAllQASimilar
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "忽略当前所有相似问答对失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}
