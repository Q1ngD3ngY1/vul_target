package document

import (
	"context"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

func (l *Logic) CreateDocClusterSchema(ctx context.Context, docClusterSchema *docEntity.DocClusterSchema) error {
	return l.docDao.CreateDocClusterSchema(ctx, docClusterSchema)

}
func (l *Logic) GetDocClusterSchemaDaoMaxVersion(ctx context.Context, appBizId uint64) (uint64, error) {
	return l.docDao.GetDocClusterSchemaDaoMaxVersion(ctx, appBizId)
}
