package dao

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
)

// ExportRejectedQuestionTask 导出拒答问题任务定义
type ExportRejectedQuestionTask struct {
	Dao Dao
}

// GetExportTotal 获取拒答问题总数
func (e ExportRejectedQuestionTask) GetExportTotal(ctx context.Context, corpID, robotID uint64, params string) (uint64,
	error) {
	total := uint64(1)
	return total, nil
}

// GetExportData 获取拒答问题导出数据
func (e ExportRejectedQuestionTask) GetExportData(ctx context.Context, corpID, robotID uint64, params string, page,
	pageSize uint32) (
	[][]string, error) {
	var rows [][]string
	rows = append(rows, []string{
		i18n.Translate(ctx, i18nkey.KeyHuaHua),
		"20",
		i18n.Translate(ctx, i18nkey.KeyFemale),
	})
	return rows, nil
}

// GetExportHeader 获取拒答问题表头信息
func (e ExportRejectedQuestionTask) GetExportHeader(ctx context.Context) []string {
	header := make([]string, 0)
	header = append(header,
		i18n.Translate(ctx, i18nkey.KeyName), i18n.Translate(ctx, i18nkey.KeyAge), i18n.Translate(ctx, i18nkey.KeyGender))
	return header
}
