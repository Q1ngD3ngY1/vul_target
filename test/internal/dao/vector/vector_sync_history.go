package vector

import (
	"context"
	"time"

	vecEntity "git.woa.com/adp/kb/kb-config/internal/entity/vector"
)

func (v *daoImpl) BatchCreateVectorSyncHistory(ctx context.Context, row []*vecEntity.VectorSync) error {
	if len(row) == 0 {
		return nil
	}
	tRowPo := BatchConvertVectorSyncHistoryDOToPO(row)
	db := v.mysql.TVectorSyncHistory.WithContext(ctx).Debug()

	if err := db.Create(tRowPo...); err != nil {
		return err
	}
	return nil
}

func (v *daoImpl) BatchDeleteVectorSyncHistoryWithCutoffTime(ctx context.Context, updateTimeUpper time.Time, limit int) (
	int64, error) {
	/*
		`	DELETE FROM t_vector_sync_history WHERE update_time < ? LIMIT ? `
	*/
	if updateTimeUpper.IsZero() {
		return 0, nil
	}
	db := v.mysql.TVectorSyncHistory.WithContext(ctx).Debug()

	if info, err := db.
		Where(v.mysql.TVectorSyncHistory.UpdateTime.Lte(updateTimeUpper)).
		Limit(limit).
		Delete(); err != nil {
		return 0, err
	} else {
		return info.RowsAffected, nil
	}
}
