package handler

import "context"

// DeleteHandler 删除应用知识
type DeleteHandler interface {
	// CountNeedDeletedData 统计表需要删除数据的数量
	CountNeedDeletedData(ctx context.Context, corpID, robotID uint64, tableName string) (int64, error)
	// DeleteNeedDeletedData 删除表需要删除的数据
	DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64, tableName string, totalCount int64) error
}
