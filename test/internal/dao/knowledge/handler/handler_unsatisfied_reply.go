package handler

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
)

// UnsatisfiedReplyHandler 不满意回复删除
type UnsatisfiedReplyHandler struct {
	dao dao.Dao
}

// NewUnsatisfiedReplyHandler 初始化不满意回复删除处理
func NewUnsatisfiedReplyHandler() *UnsatisfiedReplyHandler {
	return &UnsatisfiedReplyHandler{
		dao: dao.New(),
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (l *UnsatisfiedReplyHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "UnsatisfiedReplyHandler CountNeedDeletedData, corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	if tableName != "t_unsatisfied_reply" {
		return 0, fmt.Errorf("tableName must be `t_unsatisfied_reply`")
	}
	return l.dao.CountTableNeedDeletedData(ctx, corpID, robotID, "t_unsatisfied_reply")
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (l *UnsatisfiedReplyHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "UnsatisfiedReplyHandler DeleteNeedDeletedData, corpID:%d, robotID:%d, tableName:%s, "+
		"totalCount:%d", corpID, robotID, tableName, totalCount)
	if tableName != "t_unsatisfied_reply" {
		return fmt.Errorf("tableName must be `t_unsatisfied_reply`")
	}
	deletedCount := int64(0)
	ids, err := l.dao.GetCustomFieldIDList(ctx, corpID, robotID, "t_unsatisfied_reply", "id")
	if err != nil {
		return err
	}
	for len(ids) > 0 {
		// t_unsatisfied_reason
		_, err = l.dao.DeleteByCustomFieldID(ctx, "t_unsatisfied_reason", 0,
			[]string{"unsatisfied_id"}, []string{"IN"}, []interface{}{ids})
		if err != nil {
			return err
		}
		// t_unsatisfied_reply
		count, err := l.dao.DeleteByCustomFieldID(ctx, "t_unsatisfied_reply", int64(len(ids)),
			[]string{"id", "corp_id", "robot_id"}, []string{"IN", "=", "="}, []interface{}{ids, corpID, robotID})
		if err != nil {
			return err
		}
		deletedCount += count

		ids, err = l.dao.GetCustomFieldIDList(ctx, corpID, robotID, "t_unsatisfied_reply", "id")
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		log.ErrorContextf(ctx, "UnsatisfiedReplyHandler DeleteNeedDeletedData Failed, err:%+v", err)
		return err
	}
	return nil
}
