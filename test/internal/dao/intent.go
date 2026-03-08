package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	intentFields = `id,policy_id,category,name,is_deleted,is_used,operator,update_time,create_time`

	createIntent = `
		INSERT INTO 
			t_intent (%s) 
		VALUES 
			(null,:policy_id,:category,:name,:is_deleted,:is_used,:operator,:update_time,:create_time)
	`
	getIntent = `
		SELECT 
			%s 
		FROM 
		    t_intent 
		WHERE 
		    is_deleted = ? AND policy_id = ? AND name = ?
    `
	getIntentByID = `
		SELECT 
			%s 
		FROM 
		    t_intent 
		WHERE 
		    id = ? AND is_deleted = ?
    `
	getIntentByPolicyID = `
		SELECT 
			%s 
		FROM 
		    t_intent 
		WHERE 
		    is_deleted = ? AND policy_id IN (%s)
		ORDER BY 
		    policy_id ASC,category ASC
    `
	getIntentCount = `
		SELECT 
    		count(*) 
		FROM 
		    t_intent %s
	`
	listIntent = `
		SELECT 
			%s 
		FROM 
		    t_intent %s
		ORDER BY 
		    is_used DESC,is_deleted ASC,update_time DESC  
		LIMIT ?,?
    `
	updateIntent = `
		UPDATE 
			t_intent 
		SET 
		    policy_id = :policy_id, 
		    category = :category, 
		    operator = :operator,
		    name = :name,
		    is_used = :is_used,
		    update_time = :update_time
		WHERE 
		    id = :id
	`
	deleteIntent = `
		UPDATE 
			t_intent 
		SET 
		    operator = :operator,
		    is_deleted = :is_deleted,
		    update_time = :update_time
		WHERE 
		    id = :id
	`
	batchUpdateIntentPolicyID = `
		UPDATE 
			t_intent 
		SET 
		    operator = ?,
		    policy_id = ?,
		    is_used = ?
		WHERE 
		    id IN (%s)
	`
	batchUpdateIntent = `
		UPDATE 
			t_intent 
		SET 
		    operator = ?,
		    policy_id = ?,
		    is_used = ?,
		    category = ?
		WHERE 
		    id IN (%s)
	`
	getUnusedIntentList = `
		SELECT 
			%s 
		FROM 
		    t_intent 
		WHERE 
		    is_deleted = ? AND is_used = ?
    `
)

// CreateIntent 创建意图
func (d *dao) CreateIntent(ctx context.Context, intent *model.Intent) error {
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		intent.UpdateTime = now
		intent.CreateTime = now
		querySQL := fmt.Sprintf(createIntent, intentFields)
		_, err := tx.NamedExecContext(ctx, querySQL, intent)
		if err != nil {
			log.ErrorContextf(ctx, "创建意图失败 sql:%s args:%+v err:%+v", querySQL, intent, err)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// ListIntent 获取意图列表
func (d *dao) ListIntent(ctx context.Context, req *model.ListIntentReq) ([]*model.Intent, uint32, error) {
	condition := new(model.Condition)
	pageSize := uint32(15)
	page := uint32(1)
	var total uint32
	if len(req.Name) > 0 {
		condition.WithCondition("name = ?", req.Name)
	}
	if req.IsDeleted != 0 {
		condition.WithCondition("is_deleted = ?", req.IsDeleted)
	}
	if req.UsedFilter {
		condition.WithCondition("is_used = ?", req.IsUsed)
	}
	if req.PageSize != 0 {
		pageSize = req.PageSize
	}
	if req.Page != 0 {
		page = req.Page
	}
	querySQL := fmt.Sprintf(getIntentCount, condition.Condition())
	if err := d.db.Get(ctx, &total, querySQL, condition.Args()...); err != nil {
		log.ErrorContextf(ctx, "获取意图列表总数失败 sql:%s args:%+v err:%+v", querySQL, condition.Args(), err)
		return nil, 0, err
	}
	offset := (page - 1) * pageSize
	args := append(condition.Args(), offset, pageSize)
	querySQL = fmt.Sprintf(listIntent, intentFields, condition.Condition())
	intents := make([]*model.Intent, 0)
	log.DebugContextf(ctx, "获取意图列表 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &intents, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取意图列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, 0, err
	}
	if len(intents) == 0 {
		return nil, 0, nil
	}
	return intents, total, nil
}

// GetIntent 获取意图
func (d *dao) GetIntent(ctx context.Context, policyID uint32, name string) (*model.Intent, error) {
	args := []any{model.IntentIsValid, policyID, name}
	querySQL := fmt.Sprintf(getIntent, intentFields)
	intents := make([]*model.Intent, 0)
	if err := d.db.QueryToStructs(ctx, &intents, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取意图失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(intents) == 0 {
		return nil, nil
	}
	return intents[0], nil
}

// GetIntentByID 通过ID获取意图
func (d *dao) GetIntentByID(ctx context.Context, intentID uint64) (*model.Intent, error) {
	args := []any{intentID, model.IntentIsValid}
	querySQL := fmt.Sprintf(getIntentByID, intentFields)
	intents := make([]*model.Intent, 0)
	if err := d.db.QueryToStructs(ctx, &intents, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取意图失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(intents) == 0 {
		return nil, nil
	}
	return intents[0], nil
}

// UpdateIntent 更新意图
func (d *dao) UpdateIntent(ctx context.Context, intent *model.Intent) error {
	now := time.Now()
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		sql := updateIntent
		intent.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, sql, intent); err != nil {
			log.ErrorContextf(ctx, "更新意图失败 sql:%s args:%+v err:%+v", sql, intent, err)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteIntent 删除意图
func (d *dao) DeleteIntent(ctx context.Context, intent *model.Intent) error {
	now := time.Now()
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := deleteIntent
		intent.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, intent); err != nil {
			log.ErrorContextf(ctx, "删除意图失败 sql:%s args:%+v err:%+v", querySQL, intent, err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GetIntentByPolicyID 获取策略下的意图列表
func (d *dao) GetIntentByPolicyID(ctx context.Context, policyID []uint32) ([]*model.Intent, error) {
	args := []any{model.IntentIsValid}
	for _, id := range policyID {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getIntentByPolicyID, intentFields, placeholder(len(policyID)))
	intents := make([]*model.Intent, 0)
	if err := d.db.QueryToStructs(ctx, &intents, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取意图失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(intents) == 0 {
		return nil, nil
	}
	return intents, nil
}

// BatchUpdateIntentPolicyID 更新意图
func (d *dao) BatchUpdateIntentPolicyID(ctx context.Context, policyID uint32, ids []uint32, operator string) error {
	status := utils.When(policyID != 0, model.IntentIsUsed, model.IntentNotUsed)
	querySQL := batchUpdateIntentPolicyID
	querySQL = fmt.Sprintf(batchUpdateIntentPolicyID, placeholder(len(ids)))
	args := make([]any, 0, 3+len(ids))
	args = append(args, operator, policyID, status)
	for _, id := range ids {
		args = append(args, id)
	}
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "批量更新意图策略id失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// BatchUpdateIntent 更新意图
func (d *dao) BatchUpdateIntent(ctx context.Context, policyID uint32,
	ids []uint32, operator string, category string) error {
	status := utils.When(policyID != 0, model.IntentIsUsed, model.IntentNotUsed)
	querySQL := batchUpdateIntent
	querySQL = fmt.Sprintf(batchUpdateIntent, placeholder(len(ids)))
	args := make([]any, 0, 4+len(ids))
	args = append(args, operator, policyID, status, category)
	for _, id := range ids {
		args = append(args, id)
	}
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "批量更新意图策略id失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// GetUnusedIntentList 获取未使用的意图列表
func (d *dao) GetUnusedIntentList(ctx context.Context) ([]*model.Intent, error) {
	args := []any{model.IntentIsValid, model.IntentNotUsed}
	querySQL := fmt.Sprintf(getUnusedIntentList, intentFields)
	intents := make([]*model.Intent, 0)
	if err := d.db.QueryToStructs(ctx, &intents, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取意图失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(intents) == 0 {
		return nil, nil
	}
	return intents, nil
}
