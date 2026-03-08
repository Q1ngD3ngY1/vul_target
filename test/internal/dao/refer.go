package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	referFields = `
		id,business_id,robot_id,msg_id,raw_question,doc_id,doc_type,relate_id,confidence,question,
		answer,org_data,page_infos,sheet_infos,mark,session_type,update_time,create_time,rouge_score
	`
	createRefer = `
		INSERT INTO 
			t_refer (%s) 
		VALUES 
		    (null,:business_id,:robot_id,:msg_id,:raw_question,:doc_id,:doc_type,:relate_id,:confidence,:question,
		:answer,:org_data,:page_infos,:sheet_infos,:mark,:session_type,:update_time,:create_time,:rouge_score)
	`
	getRefersByBusinessIDs = `
		SELECT 
			%s 
		FROM 
		    t_refer 
		WHERE 
		    robot_id = ? AND business_id IN (%s)
	`
	getRefersByBusinessID = `
		SELECT 
			%s 
		FROM 
		    t_refer 
		WHERE 
		    business_id = ?
	`
	markRefer = `
		UPDATE 
			t_refer 
		SET 
		    mark = ?,
		    update_time = ? 
		WHERE 
		    business_id = ? AND mark = ? AND robot_id = ?
	`
)

// CreateRefer 创建refer
func (d *dao) CreateRefer(ctx context.Context, refers []model.Refer) error {
	if len(refers) == 0 {
		return nil
	}
	querySQL := fmt.Sprintf(createRefer, referFields)
	if _, err := d.db.NamedExec(ctx, querySQL, refers); err != nil {
		log.ErrorContextf(ctx, "创建refer失败 sql:%s args:%+v err:%+v", querySQL, refers, err)
		return err
	}
	return nil
}

// GetRefersByBusinessIDs 通过business_id获取refer
func (d *dao) GetRefersByBusinessIDs(
	ctx context.Context, robotID uint64, businessIDs []uint64,
) ([]*model.Refer, error) {
	querySQL := fmt.Sprintf(getRefersByBusinessIDs, referFields, placeholder(len(businessIDs)))
	args := make([]any, 0, len(businessIDs)+1)
	args = append(args, robotID)
	for _, bID := range businessIDs {
		args = append(args, bID)
	}
	refers := make([]*model.Refer, 0)
	if err := d.db.QueryToStructs(ctx, &refers, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过business_id获取refer失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return refers, nil
}

// GetRefersByBusinessID 通过business_id获取refer
func (d *dao) GetRefersByBusinessID(ctx context.Context, businessID uint64) (*model.Refer, error) {
	querySQL := fmt.Sprintf(getRefersByBusinessID, referFields)
	args := make([]any, 0, 1)
	args = append(args, businessID)
	refers := make([]*model.Refer, 0)
	if err := d.db.QueryToStructs(ctx, &refers, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过business_id获取refer失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(refers) == 0 {
		log.ErrorContextf(ctx, "通过business_id获取refer失败 len(refers):%d", len(refers))
		return nil, errs.ErrGetReferFail
	}
	return refers[0], nil
}

// MarkRefer .
func (d *dao) MarkRefer(ctx context.Context, robotID, businessID uint64, mark uint32) error {
	querySQL := markRefer
	now := time.Now()
	args := make([]any, 0, 5)
	args = append(args, mark, now, businessID, model.MarkInit, robotID)
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "MarkRefer失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}
