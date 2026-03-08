package handler

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
)

// EvaluateHandler 评测删除
type EvaluateHandler struct {
	dao dao.Dao
}

// NewEvaluateHandler 初始化评测删除处理
func NewEvaluateHandler() *EvaluateHandler {
	return &EvaluateHandler{
		dao: dao.New(),
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (e *EvaluateHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "EvaluateHandler CountNeedDeletedData, corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	if tableName != "t_evaluate_sample_set" {
		return 0, fmt.Errorf("tableName must be `t_evaluate_sample_set`")
	}
	return e.dao.CountTableNeedDeletedData(ctx, corpID, robotID, "t_evaluate_sample_set")
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (e *EvaluateHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "EvaluateHandler DeleteNeedDeletedData, corpID:%d, robotID:%d, tableName:%s, "+
		"totalCount:%d", corpID, robotID, tableName, totalCount)
	if tableName != "t_evaluate_sample_set" {
		return fmt.Errorf("tableName must be `t_evaluate_sample_set`")
	}
	deletedCount := int64(0)
	ids, err := e.dao.GetCustomFieldIDList(ctx, corpID, robotID, "t_evaluate_sample_set", "id")
	if err != nil {
		return err
	}
	for len(ids) > 0 {
		// t_evaluate_test_record
		_, err = e.dao.DeleteByCustomFieldID(ctx, "t_evaluate_test_record", 0,
			[]string{"set_id"}, []string{"IN"}, []interface{}{ids})
		if err != nil {
			return err
		}

		// t_evaluate_test
		_, err = e.dao.DeleteByCustomFieldID(ctx, "t_evaluate_test", 0,
			[]string{"corp_id", "robot_id", "set_id"}, []string{"=", "=", "IN"}, []interface{}{corpID, robotID, ids})
		if err != nil {
			return err
		}

		// t_evaluate_sample_set_record
		_, err = e.dao.DeleteByCustomFieldID(ctx, "t_evaluate_sample_set_record", 0,
			[]string{"set_id"}, []string{"IN"}, []interface{}{ids})
		if err != nil {
			return err
		}

		// t_evaluate_sample_set
		count, err := e.dao.DeleteByCustomFieldID(ctx, "t_evaluate_sample_set", int64(len(ids)),
			[]string{"id", "corp_id", "robot_id"}, []string{"IN", "=", "="}, []interface{}{ids, corpID, robotID})
		if err != nil {
			return err
		}
		deletedCount += count

		ids, err = e.dao.GetCustomFieldIDList(ctx, corpID, robotID, "t_evaluate_sample_set", "id")
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		log.ErrorContextf(ctx, "EvaluateHandler DeleteNeedDeletedData Failed, err:%+v", err)
		return err
	}
	return nil
}
