package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	globalKnowledgeFields = `id, question, answer, is_sync, is_deleted, create_time, update_time`
	getGlobalKnowledge    = `SELECT %s FROM t_global_knowledge WHERE is_deleted = 0 AND id = ?`
	getGlobalKnowledges   = `SELECT %s FROM t_global_knowledge WHERE is_deleted = 0 AND id IN(?)`
	insertGlobalKnowledge = `INSERT INTO t_global_knowledge (question, answer, is_sync, is_deleted)
		VALUES (:question, :answer, :is_sync, :is_deleted)`
	updateGlobalKnowledge = `UPDATE t_global_knowledge SET question = :question, answer = :answer, is_sync = :is_sync
		WHERE id = :id`
	deleteGlobalKnowledge   = `UPDATE t_global_knowledge SET is_deleted = 1 WHERE id = ?`
	getGlobalKnowledgeCount = `
		SELECT 
    		count(*) 
		FROM 
		    t_global_knowledge 
		WHERE is_deleted = ? %s 
	`
	getGlobalKnowledgeList = `
		SELECT 
    		%s 
		FROM 
		    t_global_knowledge 
		WHERE
		    is_deleted = ?  %s
		ORDER BY 
		    update_time DESC
		LIMIT ?,?
		`
	getAllGlobalKnowledge = `SELECT ` + globalKnowledgeFields + ` FROM t_global_knowledge WHERE is_deleted = 0`
)

// GetGlobalKnowledge 通过id获取知识库
func (d *dao) GetGlobalKnowledge(ctx context.Context, id model.GlobalKnowledgeID) (model.GlobalKnowledge, error) {
	query := fmt.Sprintf(getGlobalKnowledge, globalKnowledgeFields)
	args := []any{id}
	globalKnowledges := make([]model.GlobalKnowledge, 0)
	if err := d.db.Select(ctx, &globalKnowledges, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetGlobalKnowledge fail, query: %s, args: %+v, err: %v", query, args, err)
		return model.GlobalKnowledge{}, err
	}
	if len(globalKnowledges) == 0 {
		log.ErrorContextf(ctx, "GetGlobalKnowledge fail, len(globalKnowledges) == 0, id: %d", id)
		return model.GlobalKnowledge{}, errs.ErrGlobalKnowledgeNotFound
	}
	return globalKnowledges[0], nil
}

// GetGlobalKnowledges 通过id获取知识库
func (d *dao) GetGlobalKnowledges(ctx context.Context, ids []model.GlobalKnowledgeID) ([]model.GlobalKnowledge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var args []any
	for _, v := range ids {
		args = append(args, v)
	}
	query, args, err := sqlx.In(fmt.Sprintf(getGlobalKnowledges, globalKnowledgeFields), args)
	if err != nil {
		log.ErrorContextf(
			ctx, "GetGlobalKnowledge fail, sqlx.In fail,  query: %s, args: %+v, err: %v",
			fmt.Sprintf(getGlobalKnowledges, globalKnowledgeFields), ids, err,
		)
		return nil, err
	}
	globalKnowledges := make([]model.GlobalKnowledge, 0)
	if err = d.db.Select(ctx, &globalKnowledges, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetGlobalKnowledge fail, query: %s, args: %+v, err: %v", query, args, err)
		return nil, err
	}
	return globalKnowledges, nil
}

// InsertGlobalKnowledge 插入知识库
func (d *dao) InsertGlobalKnowledge(
	ctx context.Context, knowledge model.GlobalKnowledge,
) (model.GlobalKnowledgeID, error) {
	query := insertGlobalKnowledge
	r, err := d.db.NamedExec(ctx, query, knowledge)
	if err != nil {
		log.ErrorContextf(
			ctx, "InsertGlobalKnowledge fail, query: %s, args: %+v, err: %v", query, knowledge, err,
		)
		return 0, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return model.GlobalKnowledgeID(id), nil
}

// UpdateGlobalKnowledge 更新知识库
func (d *dao) UpdateGlobalKnowledge(ctx context.Context, knowledge model.GlobalKnowledge) error {
	query := updateGlobalKnowledge
	if _, err := d.db.NamedExec(ctx, query, knowledge); err != nil {
		log.ErrorContextf(ctx, "UpdateGlobalKnowledge fail, query: %s, args: %+v, err: %v", query, knowledge, err)
		return err
	}
	return nil
}

// DeleteGlobalKnowledge 删除知识库
func (d *dao) DeleteGlobalKnowledge(ctx context.Context, id model.GlobalKnowledgeID) error {
	query := deleteGlobalKnowledge
	args := []any{id}
	if _, err := d.db.Exec(ctx, query, args...); err != nil {
		log.ErrorContextf(ctx, "DeleteGlobalKnowledge fail, query: %s, args: %+v, err: %v", query, args, err)
		return err
	}
	return nil
}

// ListGlobalKnowledge 知识库列表
func (d *dao) ListGlobalKnowledge(ctx context.Context, query string, pageNumber, pageSize uint32) (uint64,
	[]*model.GlobalKnowledge, error) {
	var args []any
	args = append(args, model.GlobalKnowledgeIsNotDeleted)
	condition := ""
	if len(query) > 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? OR answer LIKE ?")
		args = append(args,
			fmt.Sprintf("%%%s%%", special.Replace(query)),
			fmt.Sprintf("%%%s%%", special.Replace(query)),
		)
	}
	querySQL := fmt.Sprintf(getGlobalKnowledgeCount, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取全局干预知识库总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	querySQL = fmt.Sprintf(getGlobalKnowledgeList, globalKnowledgeFields, condition)
	offset := (pageNumber - 1) * pageSize
	args = append(args, offset, pageSize)
	docs := make([]*model.GlobalKnowledge, 0)
	log.DebugContextf(ctx, "获取全局干预知识库列表 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &docs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取全局干预知识库列表 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	return total, docs, nil
}

// GetAllGlobalKnowledge 获取所有全局知识(不包括已删除的记录)
func (d *dao) GetAllGlobalKnowledge(ctx context.Context) ([]model.GlobalKnowledge, error) {
	query := getAllGlobalKnowledge
	var rows []model.GlobalKnowledge
	if err := d.db.Select(ctx, &rows, query); err != nil {
		log.ErrorContextf(ctx, "GetAllGlobalKnowledge fail, query: %s, err: %v", query, err)
		return nil, err
	}
	return rows, nil
}
