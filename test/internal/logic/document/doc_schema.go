package document

import (
	"context"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

func (l *Logic) CreateDocSchema(ctx context.Context, docSchema *docEntity.DocSchema) error {

	return l.docDao.CreateDocSchema(ctx, docSchema)
}

func (l *Logic) DeleteDocSchema(ctx context.Context, corpBizId uint64, appBizId uint64,
	docBizIds []uint64) error {
	return l.docDao.DeleteDocSchema(ctx, corpBizId, appBizId, docBizIds)
}

func (l *Logic) UpdateDocSchema(ctx context.Context, updateColumns []string,
	docSchema *docEntity.DocSchema) error {
	return l.docDao.UpdateDocSchema(ctx, updateColumns, docSchema)
}

func (l *Logic) GetDocSchemaCountAndList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocSchemaFilter) ([]*docEntity.DocSchema, int64, error) {
	return l.docDao.GetDocSchemaCountAndList(ctx, selectColumns, filter)
}
