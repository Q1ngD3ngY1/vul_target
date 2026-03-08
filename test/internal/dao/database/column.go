package database

import (
	"context"
	"errors"
	"fmt"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/boolx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gen"
	"gorm.io/gorm"
)

func (d *dao) DescribeColumn(ctx context.Context, filter *entity.ColumnFilter) (*entity.Column, error) {
	conds := columnFilterToConds(d.tdsql, filter)
	column, err := d.tdsql.TDbTableColumn.WithContext(ctx).Where(conds...).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, fmt.Errorf("dao:DescribeColumn|filter: %v, db error: %w", filter, err)
	}
	return columnPO2DO(column), nil
}

func (d *dao) DescribeColumnList(ctx context.Context, filter *entity.ColumnFilter) ([]*entity.Column, int64, error) {
	conds := columnFilterToConds(d.tdsql, filter)
	db := d.tdsql.TDbTableColumn.WithContext(ctx).Where(conds...)
	total, err := db.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeColumnList|filter: %v, db error: %w", filter, err)
	}
	columns, err := db.FindInBatch(100, func(tx gen.Dao, batch int) error { return nil })
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeColumnList|filter: %v, db error: %w", filter, err)
	}
	return columnsPO2DO(columns), total, nil
}

func columnFilterToConds(q *tdsqlquery.Query, filter *entity.ColumnFilter) (conds []gen.Condition) {
	if filter == nil {
		return
	}

	if filter.CorpBizID != 0 {
		conds = append(conds, q.TDbTableColumn.CorpBizID.Eq(filter.CorpBizID))
	}
	if filter.AppBizID != 0 {
		conds = append(conds, q.TDbTableColumn.AppBizID.Eq(filter.AppBizID))
	}
	if filter.DBTableBizID != 0 {
		conds = append(conds, q.TDbTableColumn.DbTableBizID.Eq(filter.DBTableBizID))
	}
	if len(filter.DBTableBizIDs) > 0 {
		conds = append(conds, q.TDbTableColumn.DbTableBizID.In(filter.DBTableBizIDs...))
	}
	if filter.DBColumnBizID != 0 {
		conds = append(conds, q.TDbTableColumn.DbTableColumnBizID.Eq(filter.DBColumnBizID))
	}
	if len(filter.DBColumnBizIDs) > 0 {
		conds = append(conds, q.TDbTableColumn.DbTableColumnBizID.In(filter.DBColumnBizIDs...))
	}
	if filter.Name != nil {
		conds = append(conds, q.TDbTableColumn.ColumnName.Eq(*filter.Name))
	}

	if filter.IsDeleted == nil {
		conds = append(conds, q.TDbTableColumn.IsDeleted.Is(false))
	} else {
		conds = append(conds, q.TDbTableColumn.IsDeleted.Is(*filter.IsDeleted))
	}
	return
}

func columnPO2DO(po *model.TDbTableColumn) *entity.Column {
	if po == nil {
		return nil
	}
	return &entity.Column{
		CorpBizID:          po.CorpBizID,
		AppBizID:           po.AppBizID,
		DBTableBizID:       po.DbTableBizID,
		DBTableColumnBizID: po.DbTableColumnBizID,
		ColumnName:         po.ColumnName,
		DataType:           po.DataType,
		ColumnComment:      po.ColumnComment,
		AliasName:          po.AliasName,
		Description:        po.Description,
		Unit:               po.Unit,
		IsIndexed:          po.IsIndexed,
		IsDeleted:          po.IsDeleted,
		CreateTime:         po.CreateTime,
		UpdateTime:         po.UpdateTime,
	}
}

func columnsPO2DO(pos []*model.TDbTableColumn) []*entity.Column {
	if len(pos) == 0 {
		return nil
	}
	dos := make([]*entity.Column, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, columnPO2DO(po))
	}
	return dos
}

func columnDO2PO(do *entity.Column) *model.TDbTableColumn {
	if do == nil {
		return nil
	}
	return &model.TDbTableColumn{
		CorpBizID:          do.CorpBizID,
		AppBizID:           do.AppBizID,
		DbTableBizID:       do.DBTableBizID,
		DbTableColumnBizID: do.DBTableColumnBizID,
		ColumnName:         do.ColumnName,
		DataType:           do.DataType,
		ColumnComment:      do.ColumnComment,
		AliasName:          do.AliasName,
		Description:        do.Description,
		Unit:               do.Unit,
		IsIndexed:          do.IsIndexed,
		IsDeleted:          do.IsDeleted,
		CreateTime:         do.CreateTime,
		UpdateTime:         do.UpdateTime,
	}
}

func columnsDO2PO(dos []*entity.Column) []*model.TDbTableColumn {
	if len(dos) == 0 {
		return nil
	}
	pos := make([]*model.TDbTableColumn, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, columnDO2PO(do))
	}
	return pos
}

// CreateColumnList 批量添加列
func (d *dao) CreateColumnList(ctx context.Context, columns []*entity.Column) error {
	if len(columns) == 0 {
		return errs.ErrDbSourceTableBlankFail
	}
	data := columnsDO2PO(columns)
	if err := d.tdsql.TDbTableColumn.WithContext(ctx).Create(data...); err != nil {
		return fmt.Errorf("dao:CreateColumnList|columns: %v, db error: %w", columns, err)
	}
	return nil
}

func (d *dao) BatchUpdateByBizID(ctx context.Context, corpBizID, appBizID uint64, updateColumns []string, dbTableColumns []*entity.Column) (int64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return 0, errs.ErrParameterInvalid
	}
	if len(dbTableColumns) == 0 {
		logx.I(ctx, "BatchUpdateByBizID|dbTableColumns is empty")
		return 0, nil
	}
	var totalRows int64 = 0
	err := d.tdsql.TDbTableColumn.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		for _, col := range dbTableColumns {
			res := tx.Model(&entity.Column{}).
				Select(updateColumns).
				Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_column_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, col.DBTableColumnBizID).
				Updates(col)
			if res.Error != nil {
				return res.Error
			}
			totalRows += res.RowsAffected
		}
		return nil
	})

	if err != nil {
		logx.E(ctx, "BatchUpdateByBizID|db error: %v", err)
		return 0, err
	}
	return totalRows, nil
}

// SoftDeleteByTableBizID 软删除某个表的所有列
func (d *dao) SoftDeleteByTableBizID(ctx context.Context, filter *entity.ColumnFilter) error {
	if filter.CorpBizID == 0 || filter.AppBizID == 0 || filter.DBTableBizID == 0 {
		return errs.ErrParameterInvalid
	}
	conds := columnFilterToConds(d.tdsql, filter)
	_, err := d.tdsql.TDbTableColumn.WithContext(ctx).Where(conds...).UpdateSimple(d.tdsql.TDbTableColumn.IsDeleted.Value(true))
	if err != nil {
		return fmt.Errorf("dao:SoftDeleteByTableBizID|filter: %v, error: %w", filter, err)
	}
	return nil
}

// DeleteByTableBizID 删除 tableBizIDs 下的所有列
func (d *dao) DeleteByTableBizID(ctx context.Context, tableBizIDs []uint64) (int64, error) {
	result := d.tdsql.TDbTableColumn.WithContext(ctx).UnderlyingDB().WithContext(ctx).
		Where("db_table_biz_id IN (?)", tableBizIDs).
		Delete(&entity.Column{})
	if result.Error != nil {
		logx.E(ctx, "DeleteByTableBizID|db error: %v", result.Error)
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// BatchGetByTableBizID 批量根据表查询，返回map[tableBizID][]*model.DBTableColumn
func (d *dao) BatchGetByTableBizID(ctx context.Context, corpBizID, appBizID uint64, tableBizIDs []uint64) (
	map[uint64][]*entity.Column, error) {
	tableBizIDs = slicex.Unique(tableBizIDs)
	result := make(map[uint64][]*entity.Column)
	if len(tableBizIDs) == 0 {
		return result, nil
	}
	var cursor = 0
	const batchSize = 100
	for {
		var batchIDs []uint64
		if len(tableBizIDs) > cursor {
			end := cursor + batchSize
			if end > len(tableBizIDs) {
				end = len(tableBizIDs)
			}
			batchIDs = tableBizIDs[cursor:end]
			cursor = end
		}

		if len(batchIDs) == 0 {
			break
		}

		var cols []*entity.Column
		err := d.tdsql.TDbTableColumn.WithContext(ctx).UnderlyingDB().WithContext(ctx).
			Where(DBCorpBizID+sqlEqual, corpBizID).
			Where(DBAppBizID+sqlEqual, appBizID).
			Where(DBTableBizID+sqlIn, batchIDs).
			Where(DBIsDeleted+sqlEqual, boolx.No).
			Find(&cols).Error
		if err != nil {
			logx.E(ctx, "BatchGetByTableBizID|db error: corpBizID=%d, appBizID=%d, batchIDs=%v, error=%v",
				corpBizID, appBizID, batchIDs, err)
			return nil, err
		}

		for _, col := range cols {
			result[col.DBTableBizID] = append(result[col.DBTableBizID], col)
		}
	}
	return result, nil
}
