package export

import (
	"context"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

type RejectedQuestionExportLogic struct {
}

func NewRejectedQuestionExportLogic() *QaExportLogic {
	return &QaExportLogic{}
}

func (e RejectedQuestionExportLogic) GetExportData(ctx context.Context, corpID, robotID uint64, params string, page,
	pageSize uint32) ([][]string, error) {
	var rows [][]string
	rows = append(rows, []string{
		i18n.Translate(ctx, i18nkey.KeyHuaHua),
		"20",
		i18n.Translate(ctx, i18nkey.KeyFemale),
	})
	return rows, nil
}

// GetExportHeader 获取 QA 导出表头信息
func (e RejectedQuestionExportLogic) GetExportHeader(ctx context.Context) []string {
	header := make([]string, 0)
	header = append(header,
		i18n.Translate(ctx, i18nkey.KeyName), i18n.Translate(ctx, i18nkey.KeyAge), i18n.Translate(ctx, i18nkey.KeyGender))
	return header
}
