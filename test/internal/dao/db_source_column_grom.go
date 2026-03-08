package dao

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"gorm.io/gorm"
)

const (
	blankStr = ""
)

var dbTableColumnDao *DBTableColumnDao

// GetDBTableColumnDao TODO
func GetDBTableColumnDao() *DBTableColumnDao {
	if dbTableColumnDao == nil {
		dbTableColumnDao = &DBTableColumnDao{db: globalBaseDao.tdsqlGormDB}
	}
	return dbTableColumnDao
}

// DBTableColumnDao TODO
type DBTableColumnDao struct {
	db *gorm.DB
}

// Create 新增
func (r *DBTableColumnDao) Create(ctx context.Context, col *model.DBTableColumn) error {
	err := r.db.WithContext(ctx).Create(col).Error
	if err != nil {
		log.ErrorContextf(ctx, "Create|db error: %v", err)
		return err
	}
	return nil
}

// AddColumns 添加列,根据表信息，便利列
func (r *DBTableColumnDao) AddColumns(ctx context.Context, tableInfo *model.TableInfo, table *model.DBTable,
	d Dao) error {
	if tableInfo == nil || table == nil {
		log.ErrorContextf(ctx, "AddColumns|tableInfo or table is nil, tableInfo: %v, table: %v", tableInfo, table)
		return errs.ErrParameterInvalid
	}

	if tableInfo.ColumnInfo == nil || len(tableInfo.ColumnInfo) == 0 {
		log.ErrorContextf(ctx, "AddColumns|tableInfo.ColumnInfo is nil, tableInfo: %v", tableInfo)
		return errs.ErrDbSourceTableBlankFail
	}
	columns := make([]*model.DBTableColumn, 0)
	for _, columnInfo := range tableInfo.ColumnInfo {
		dbTableColumn := &model.DBTableColumn{
			CorpBizID:          table.CorpBizID,
			AppBizID:           table.AppBizID,
			DBTableBizID:       table.DBTableBizID,
			DBTableColumnBizID: d.GenerateSeqID(),
			ColumnName:         columnInfo.ColumnName,
			DataType:           columnInfo.DataType,
			ColumnComment:      columnInfo.ColComment,
			AliasName:          blankStr,
			Description:        blankStr,
			Unit:               blankStr,
			IsIndexed:          true,
			IsDeleted:          false,
			CreateTime:         time.Now(),
			UpdateTime:         time.Now(),
		}
		columns = append(columns, dbTableColumn)
	}
	err := r.BatchCreate(ctx, columns)
	if err != nil {
		log.ErrorContextf(ctx, "AddColumns|table %v, db error: %v", table, err)
		return err
	}
	return nil
}

// BatchCreate 批量新增
func (r *DBTableColumnDao) BatchCreate(ctx context.Context, cols []*model.DBTableColumn) error {
	if len(cols) == 0 {
		return nil
	}
	err := r.db.WithContext(ctx).Create(&cols).Error
	if err != nil {
		log.ErrorContextf(ctx, "BatchCreate|db error: %v", err)
		return errs.ErrAddDbSourceFail
	}
	return nil
}

// GetByTableBizID 根据表查询
func (r *DBTableColumnDao) GetByTableBizID(ctx context.Context, corpBizID, appBizID,
	tableBizID uint64) ([]*model.DBTableColumn, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	cols := make([]*model.DBTableColumn, 0)
	err := r.db.WithContext(ctx).Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 0",
		corpBizID, appBizID, tableBizID).Find(&cols).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetByTableBizID|db error: %v", err)
		return nil, err
	}
	return cols, nil
}

// GetByColumnByTableBizIDAndColumnName 根据表和列名查询
func (r *DBTableColumnDao) GetByColumnByTableBizIDAndColumnName(ctx context.Context, corpBizID, appBizID,
	tableBizID uint64, columnName string) (*model.DBTableColumn, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	col := &model.DBTableColumn{}
	// 使用 find 方法，避免使用 first 方法，first 方法如果检索为空会报错，逻辑允许为空，所以使用 find 方法
	err := r.db.WithContext(ctx).Where(
		"corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND column_name = ? AND is_deleted = 0",
		corpBizID, appBizID, tableBizID, columnName).Find(&col).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetByTableBizID|tableBizID %v, columnName %v, db error: %v", tableBizID, columnName, err)
		return nil, err
	}
	return col, nil
}

// BatchUpdateByBizID TODO
func (r *DBTableColumnDao) BatchUpdateByBizID(
	ctx context.Context,
	corpBizID, appBizID uint64,
	updateColumns []string,
	dbTableColumns []*model.DBTableColumn,
) (int64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return 0, errs.ErrParameterInvalid
	}
	if len(dbTableColumns) == 0 {
		log.InfoContextf(ctx, "BatchUpdateByBizID|dbTableColumns is empty")
		return 0, nil
	}
	var totalRows int64 = 0
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, col := range dbTableColumns {
			res := tx.Model(&model.DBTableColumn{}).
				Select(updateColumns).
				Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_column_biz_id = ? AND is_deleted = 0", corpBizID, appBizID,
					col.DBTableColumnBizID).
				Updates(col)
			if res.Error != nil {
				return res.Error
			}
			totalRows += res.RowsAffected
		}
		return nil
	})

	if err != nil {
		log.ErrorContextf(ctx, "BatchUpdateByBizID|db error: %v", err)
		return 0, err
	}
	return totalRows, nil
}

// SoftDeleteByTableBizID 软删除某个表的所有列
func (r *DBTableColumnDao) SoftDeleteByTableBizID(ctx context.Context, corpBizID, appBizID, tableBizID uint64) error {
	if corpBizID == 0 || appBizID == 0 {
		return errs.ErrParameterInvalid
	}
	err := r.db.WithContext(ctx).
		Model(&model.DBTableColumn{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ?", corpBizID, appBizID, tableBizID).
		Update("is_deleted", true).Error
	if err != nil {
		log.ErrorContextf(ctx, "SoftDeleteByTableBizID|db error: %v", err)
		return err
	}
	return nil
}

// DeleteByTableBizID 删除 tableBizIDs 下的所有列
func (r *DBTableColumnDao) DeleteByTableBizID(ctx context.Context, tableBizIDs []uint64) (int64, error) {
	result := r.db.WithContext(ctx).
		Where("db_table_biz_id IN (?)", tableBizIDs).
		Delete(&model.DBTableColumn{})
	if result.Error != nil {
		log.ErrorContextf(ctx, "DeleteByTableBizID|db error: %v", result.Error)
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// BatchGetByTableBizID 批量根据表查询，返回map[tableBizID][]*model.DBTableColumn
func (r *DBTableColumnDao) BatchGetByTableBizID(ctx context.Context, corpBizID, appBizID uint64, tableBizIDs []uint64) (
	map[uint64][]*model.DBTableColumn, error) {
	tableBizIDs = slicex.Unique(tableBizIDs)
	result := make(map[uint64][]*model.DBTableColumn)
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

		var cols []*model.DBTableColumn
		err := r.db.WithContext(ctx).
			Where(DBCorpBizID+sqlEqual, corpBizID).
			Where(DBAppBizID+sqlEqual, appBizID).
			Where(DBTableBizID+sqlIn, batchIDs).
			Where(DBIsDeleted+sqlEqual, IsNotDeleted).
			Find(&cols).Error
		if err != nil {
			log.ErrorContextf(ctx, "BatchGetByTableBizID|db error: corpBizID=%d, appBizID=%d, batchIDs=%v, error=%v",
				corpBizID, appBizID, batchIDs, err)
			return nil, err
		}

		for _, col := range cols {
			result[col.DBTableBizID] = append(result[col.DBTableBizID], col)
		}
	}
	return result, nil
}
