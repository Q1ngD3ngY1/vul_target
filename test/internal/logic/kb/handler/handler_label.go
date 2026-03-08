package handler

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
)

// LabelHandler 标签删除
type LabelHandler struct {
	kbDao kbdao.Dao
}

// NewLabelHandler 初始化标签删除处理
func NewLabelHandler(kbDao kbdao.Dao) *LabelHandler {
	return &LabelHandler{
		kbDao: kbDao,
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (l *LabelHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "LabelHandler CountNeedDeletedData, corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	if tableName != "t_attribute" {
		return 0, fmt.Errorf("tableName must be `t_attribute`")
	}
	// t_attribute 只有robot_id字段
	return l.kbDao.CountTableNeedDeletedData(ctx, 0, robotID, "t_attribute")
}

// DeleteNeedDeletedData 删除表需要删除的数据
func (l *LabelHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "LabelHandler DeleteNeedDeletedData, corpID:%d, robotID:%d, tableName:%s, "+
		"totalCount:%d", corpID, robotID, tableName, totalCount)
	if tableName != "t_attribute" {
		return fmt.Errorf("tableName must be `t_attribute`")
	}
	deletedCount := int64(0)
	// t_attribute 只有robot_id字段
	ids, err := l.kbDao.GetCustomFieldIDList(ctx, 0, robotID, "t_attribute", "id")
	if err != nil {
		return err
	}
	for len(ids) > 0 {
		// t_attribute_prod
		_, err = l.kbDao.DeleteByCustomFieldID(ctx, "t_attribute_prod", 0,
			[]string{"attr_id", "robot_id"}, []string{"IN", "="}, []any{ids, robotID})
		if err != nil {
			return err
		}
		// t_attribute_label
		_, err = l.kbDao.DeleteByCustomFieldID(ctx, "t_attribute_label", 0,
			[]string{"attr_id"}, []string{"IN"}, []any{ids})
		if err != nil {
			return err
		}
		// t_attribute_label_prod
		_, err = l.kbDao.DeleteByCustomFieldID(ctx, "t_attribute_label_prod", 0,
			[]string{"attr_id"}, []string{"IN"}, []any{ids})
		if err != nil {
			return err
		}
		// t_attribute
		count, err := l.kbDao.DeleteByCustomFieldID(ctx, "t_attribute", int64(len(ids)),
			[]string{"id", "robot_id"}, []string{"IN", "="}, []any{ids, robotID})
		if err != nil {
			return err
		}
		deletedCount += count

		ids, err = l.kbDao.GetCustomFieldIDList(ctx, 0, robotID, "t_attribute", "id")
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		logx.E(ctx, "LabelHandler DeleteNeedDeletedData Failed, err:%+v", err)
		return err
	}
	return nil
}
