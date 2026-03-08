package vector

import (
	"context"
	"gorm.io/gen/field"
	"time"

	"gorm.io/gen"
	"gorm.io/gorm"

	"git.woa.com/adp/kb/kb-config/internal/config"
	vecEntity "git.woa.com/adp/kb/kb-config/internal/entity/vector"
)

func (v *daoImpl) convertListReqToCondition(req *vecEntity.ListVectorSyncReq) []gen.Condition {
	conds := []gen.Condition{}
	if req.RelatedID > 0 {
		conds = append(conds, v.mysql.TVectorSync.RelateID.Eq(req.RelatedID))
	}
	if req.Type > 0 {
		conds = append(conds, v.mysql.TVectorSync.Type.Eq(req.Type))
	}
	if len(req.Status) > 0 {
		conds = append(conds, v.mysql.TVectorSync.Status.In(req.Status...))
	}
	if len(req.StatusNotIn) > 0 {
		conds = append(conds, v.mysql.TVectorSync.Status.NotIn(req.StatusNotIn...))
	}
	return conds
}

func (v *daoImpl) getUpdateColumns(fields map[string]any) map[string]any {
	updatedColumns := map[string]any{}
	tbl := v.mysql.TVectorSync
	for k, v := range fields {
		if c, ok := tbl.GetFieldByName(k); ok {
			updatedColumns[c.ColumnName().String()] = v
		}
	}
	return updatedColumns
}

func (v *daoImpl) BatchCreateVectorSync(ctx context.Context, row []*vecEntity.VectorSync) error {
	if len(row) == 0 {
		return nil
	}
	tRowPo := BatchConvertVectorSyncDOToPO(row)
	db := v.mysql.TVectorSync.WithContext(ctx).Debug()

	if err := db.Create(tRowPo...); err != nil {
		return err
	}
	return nil
}

func (v *daoImpl) CreateVectorSync(ctx context.Context, row *vecEntity.VectorSync) (uint64, error) {
	if row == nil {
		return 0, nil
	}
	tRowPo := ConvertVectorSyncDOToPO(row)
	db := v.mysql.TVectorSync.WithContext(ctx).
		Omit(v.mysql.TVectorSync.ID).Debug()

	if err := db.Create(tRowPo); err != nil {
		return 0, err
	}

	return tRowPo.ID, nil
}

func (v *daoImpl) DeleteVectorSync(ctx context.Context, syncId uint64) error {
	/*
		`
		        DELETE FROM
					t_vector_sync
		        WHERE
		            id = ?
			`
	*/
	if syncId == 0 {
		return nil
	}
	db := v.mysql.TVectorSync.WithContext(ctx).Debug()

	if _, err := db.Where(v.mysql.TVectorSync.ID.Eq(syncId)).Delete(); err != nil {
		return err
	}
	return nil
}

func (v *daoImpl) ListVectorSynsByWriteSynId(ctx context.Context, writeSyncIds []uint64) (
	/*
		`
			SELECT
				id
			FROM
			    t_vector_sync
			WHERE
			    write_sync_id IN (%s)
			ORDER BY
			    id ASC
		`
	*/
	[]*vecEntity.VectorSync, error) {
	if len(writeSyncIds) == 0 {
		return nil, nil
	}
	db := v.mysql.TVectorSync.WithContext(ctx).Debug()

	if rows, err := db.Where(v.mysql.TVectorSync.WriteSyncID.In(writeSyncIds...)).Find(); err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	} else {
		return BatchConvertVectorSyncPOToDO(rows), nil
	}

}

func (v *daoImpl) GetVectorSyncBySyncId(ctx context.Context, syncId uint64) (*vecEntity.VectorSync, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_vector_sync
			WHERE
			    id = ?
		`
	*/
	if syncId == 0 {
		return nil, nil
	}
	db := v.mysql.TVectorSync.WithContext(ctx).Debug()

	if row, err := db.Where(v.mysql.TVectorSync.ID.Eq(syncId)).Take(); err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	} else {
		return ConvertVectorSyncPOToDO(row), nil
	}

}

func (v *daoImpl) FetchNotSuccessSync(ctx context.Context) ([]*vecEntity.VectorSync, error) {
	/*

		`
				SELECT
					id
				FROM
				    t_vector_sync
				WHERE
				    try_times < max_try_times AND (status = ? OR (status IN (?,?) AND update_time < ?))
				ORDER BY
				    id ASC
				LIMIT
					?
		    `
	*/
	tbl := v.mysql.TVectorSync
	db := tbl.WithContext(ctx).Debug()
	updateTimeUpper := time.Now().Add(-config.App().VectorSync.TimeBefore)
	limit := config.App().VectorSync.Limit
	if limit == 0 {
		limit = 1000
	}

	cond := field.Or(tbl.Status.Eq(vecEntity.StatusSyncFailed),
		field.And(tbl.Status.In(vecEntity.StatusSyncInit, vecEntity.StatusSyncing), tbl.UpdateTime.Lt(updateTimeUpper)))
	if rows, err := db.
		Where(cond).Where(tbl.TryTimes.LtCol(tbl.MaxTryTimes)).
		Order(tbl.ID.Asc()).
		Limit(limit).
		Find(); err != nil {
		return nil, err
	} else {
		return BatchConvertVectorSyncPOToDO(rows), nil
	}
}

func (v *daoImpl) GetVectorSyncCount(ctx context.Context, req *vecEntity.ListVectorSyncReq) (uint64, error) {
	/*
		`
			SELECT
				count(*)
			FROM
			    t_vector_sync
			WHERE
			    relate_id = ? AND type= ? AND status != ?
		`
	*/
	tbl := v.mysql.TVectorSync
	db := tbl.WithContext(ctx).Debug()
	cond := v.convertListReqToCondition(req)

	count, err := db.Where(cond...).Count()
	if err != nil {
		return 0, err

	}
	return uint64(count), nil
}

func (v *daoImpl) BatchUpdateVectorSync(ctx context.Context, syncs []*vecEntity.VectorSync,
	updateFields map[string]any) error {
	/*
		`
			UPDATE
				t_vector_sync
			SET
			    status = ?,
			    update_time = ?
			WHERE
			    id IN (%s)
		`
	*/
	if len(syncs) == 0 {
		return nil
	}
	tbl := v.mysql.TVectorSync
	db := tbl.WithContext(ctx).Debug()
	ids := []uint64{}
	for _, sync := range syncs {
		ids = append(ids, sync.ID)
	}
	queryCond := []gen.Condition{
		tbl.ID.In(ids...),
	}

	updatedColumns := v.getUpdateColumns(updateFields)

	if _, err := db.Where(queryCond...).Updates(updatedColumns); err != nil {
		return err
	}
	return nil

}

func (v *daoImpl) UpdateAndLockVectorSync(ctx context.Context, syncId uint64, status uint32,
	updateFields map[string]any) (int64, error) {
	/*
		`
			UPDATE
				t_vector_sync
			SET
			    status = ?
			WHERE
			    id = ? AND status = ?
		`
	*/
	if syncId == 0 {
		return 0, nil
	}
	tbl := v.mysql.TVectorSync
	db := tbl.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		tbl.ID.Eq(syncId),
		tbl.Status.Eq(status),
	}
	updatedColumns := v.getUpdateColumns(updateFields)

	if res, err := db.Where(queryCond...).Updates(updatedColumns); err != nil {
		return 0, err
	} else {
		return res.RowsAffected, nil
	}

}
func (v *daoImpl) UpdateVectorSync(ctx context.Context, syncId uint64, updateFields map[string]any) (int64, error) {
	/*
		`
			UPDATE
				t_vector_sync
			SET
			    status = :status,
			    request = :request,
			    try_times = :try_times,
			    result = :result,
			    update_time = :update_time
			WHERE
			    id = :id
		`
	*/
	if syncId == 0 {
		return 0, nil
	}
	tbl := v.mysql.TVectorSync
	db := tbl.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		tbl.ID.Eq(syncId),
	}
	updatedColumns := v.getUpdateColumns(updateFields)

	if res, err := db.Where(queryCond...).Updates(updatedColumns); err != nil {
		return 0, err
	} else {
		return res.RowsAffected, nil
	}

}
