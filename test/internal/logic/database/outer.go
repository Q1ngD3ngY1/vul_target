package database

import (
	"context"

	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
)

func (l *Logic) GetTopNValueV2(ctx context.Context, dbSource *entity.Database, robotId, dbTableBizID,
	embeddingVersion uint64, embeddingName string) error {
	return l.dao.GetTopNValueV2(ctx, dbSource, robotId, dbTableBizID, embeddingVersion, embeddingName)
}
