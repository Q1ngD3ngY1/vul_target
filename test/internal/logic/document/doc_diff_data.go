package document

import (
	"context"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

func (l *Logic) CreateDocDiffData(ctx context.Context, docDiffRes *docEntity.DocDiffData) error {
	return l.docDao.CreateDocDiffData(ctx, docDiffRes)
}

func (l *Logic) DeleteDocDiffData(ctx context.Context, corpBizId, robotBizId uint64, businessIds []uint64) error {
	return l.docDao.DeleteDocDiffData(ctx, corpBizId, robotBizId, businessIds)
}

func (l *Logic) GetDocDiffDataCountAndList(ctx context.Context, selectColumns []string, filter *docEntity.DocDiffDataFilter) (
	[]*docEntity.DocDiffData, int64, error) {
	return l.docDao.GetDocDiffDataCountAndList(ctx, selectColumns, filter)
}
