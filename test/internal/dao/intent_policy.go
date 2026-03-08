package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	intentPolicyFields = `id,name,is_deleted,is_used,operator,update_time,create_time`

	createIntentPolicy = `
		INSERT INTO 
			t_intent_policy (%s) 
		VALUES 
			(null,:name,:is_deleted,:is_used,:operator,:update_time,:create_time)
	`
	listIntentPolicy = `
		SELECT 
			%s 
		FROM 
		    t_intent_policy %s
		ORDER BY 
		    update_time DESC  
		LIMIT ?,?
    `
	getIntentPolicyCount = `
		SELECT 
    		count(*) 
		FROM 
		    t_intent_policy %s
	`
	updateIntentPolicy = `
		UPDATE 
			t_intent_policy 
		SET  
		    operator = :operator,
		    name = :name,
		    is_used = :is_used,
		    update_time = :update_time
		WHERE 
		    id = :id
	`
	deleteIntentPolicy = `
		UPDATE 
			t_intent_policy 
		SET 
		    operator = :operator,
		    is_deleted = :is_deleted,
		    update_time = :update_time
		WHERE 
		    id = :id
	`
	getIntentPolicy = `
		SELECT 
			%s 
		FROM 
		    t_intent_policy 
		WHERE 
		    id = ? AND is_deleted = ? 
    `
	getIntentPolicyIDMap = `
		SELECT 
			%s 
		FROM 
		    t_intent_policy 
		WHERE 
		    is_deleted = ? 
    `
)

// ListIntentPolicy 获取策略列表
func (d *dao) ListIntentPolicy(ctx context.Context, req *model.ListIntentPolicyReq) (
	[]*model.IntentPolicy, uint32, error) {
	condition := new(model.Condition)
	pageSize := uint32(15)
	page := uint32(1)
	var total uint32
	condition.WithCondition("is_deleted = ?", model.IntentPolicyIsValid)
	if len(req.Name) > 0 {
		condition.WithCondition("name = ?", req.Name)
	}
	if req.PageSize != 0 {
		pageSize = req.PageSize
	}
	if req.Page != 0 {
		page = req.Page
	}
	querySQL := fmt.Sprintf(getIntentPolicyCount, condition.Condition())
	if err := d.db.Get(ctx, &total, querySQL, condition.Args()...); err != nil {
		log.ErrorContextf(ctx, "获取策略总数失败 sql:%s args:%+v err:%+v", querySQL, condition.Args(), err)
		return nil, 0, err
	}
	offset := (page - 1) * pageSize
	args := append(condition.Args(), offset, pageSize)
	querySQL = fmt.Sprintf(listIntentPolicy, intentPolicyFields, condition.Condition())
	intentPolicyList := make([]*model.IntentPolicy, 0)
	log.DebugContextf(ctx, "获取策略列表 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &intentPolicyList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取策略列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, 0, err
	}
	if len(intentPolicyList) == 0 {
		return nil, 0, nil
	}
	return intentPolicyList, total, nil
}

// CreateIntentPolicy 创建策略
func (d *dao) CreateIntentPolicy(ctx context.Context, intentPolicy *model.IntentPolicy) (uint64, error) {
	now := time.Now()
	intentPolicy.UpdateTime = now
	intentPolicy.CreateTime = now
	query := createIntentPolicy
	query = fmt.Sprintf(createIntentPolicy, intentPolicyFields)
	r, err := d.db.NamedExec(ctx, query, intentPolicy)
	if err != nil {
		log.ErrorContextf(
			ctx, "InsertIntentPolicy fail, query: %s, args: %+v, err: %v", query, intentPolicy, err,
		)
		return 0, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(id), nil
}

// UpdateIntentPolicy 更新策略
func (d *dao) UpdateIntentPolicy(ctx context.Context, intentPolicy *model.IntentPolicy) error {
	now := time.Now()
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := updateIntentPolicy
		intentPolicy.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, intentPolicy); err != nil {
			log.ErrorContextf(ctx, "更新策略失败 sql:%s args:%+v err:%+v", querySQL, intentPolicy, err)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteIntentPolicy 删除策略
func (d *dao) DeleteIntentPolicy(ctx context.Context, intentPolicy *model.IntentPolicy) error {
	now := time.Now()
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := deleteIntentPolicy
		intentPolicy.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, intentPolicy); err != nil {
			log.ErrorContextf(ctx, "删除策略失败 sql:%s args:%+v err:%+v", querySQL, intentPolicy, err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GetIntentPolicyByID 通过ID获取策略
func (d *dao) GetIntentPolicyByID(ctx context.Context, policyID uint32) (*model.IntentPolicy, error) {
	args := []any{policyID, model.IntentPolicyIsValid}
	querySQL := fmt.Sprintf(getIntentPolicy, intentPolicyFields)
	policy := make([]*model.IntentPolicy, 0)
	if err := d.db.QueryToStructs(ctx, &policy, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取策略失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(policy) == 0 {
		return nil, nil
	}
	return policy[0], nil
}

// GetIntentPolicyIDMap 获取意图策略映射列表
func (d *dao) GetIntentPolicyIDMap(ctx context.Context) ([]*model.IntentPolicy, error) {
	args := []any{model.IntentPolicyIsValid}
	querySQL := fmt.Sprintf(getIntentPolicyIDMap, intentPolicyFields)
	intents := make([]*model.IntentPolicy, 0)
	if err := d.db.QueryToStructs(ctx, &intents, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取意图失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(intents) == 0 {
		return nil, nil
	}
	return intents, nil
}
