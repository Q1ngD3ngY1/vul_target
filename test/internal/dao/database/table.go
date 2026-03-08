package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/boolx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/gox/stringx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	DBCorpBizID        = "corp_biz_id"
	DBAppBizID         = "app_biz_id"
	DBSourceBizID      = "db_source_biz_id"
	DBTableBizID       = "db_table_biz_id"
	DBTableColumnBizID = "db_table_column_biz_id"
	DBIsDeleted        = "is_deleted"
	DBId               = " id "
)

const (
	sqlDbTableName        = "table_name"
	sqlDbTableLearnStatus = "learn_status"
)

const (
	sqlEqual     = " = ?"
	sqlNotEqual  = " != ?"
	sqlLess      = " < ?"
	sqlLessEqual = " <= ?"
	sqlMore      = " > ?"
	sqlMoreEqual = " >= ?"
	sqlLike      = " LIKE ?"
	sqlIn        = " IN ?"
	sqlSubIn     = " IN (?)"
	sqlSubNotIn  = " NOT IN (?)"
	sqlOrderAND  = " AND "
	sqlOr        = " OR "

	SqlOrderByAsc  = "ASC"
	SqlOrderByDesc = "DESC"
)

// CreateTableList 批量新增表
func (d *dao) CreateTableList(ctx context.Context, tables []*entity.Table) error {
	if len(tables) == 0 {
		logx.W(ctx, "BatchCreate failed: tables is empty")
		return nil
	}
	data := tablesDO2PO(tables)
	if err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Create(data).Error; err != nil {
		logx.E(ctx, "BatchCreate failed %v", err)
		return errs.ErrAddDbSourceFail
	}
	return nil
}

// GetByBizID 通过业务ID获取
func (d *dao) GetByBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) (*entity.Table, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var table entity.Table
	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 0",
		corpBizID, appBizID, dbTableBizID).First(&table).Error
	if err != nil {
		logx.E(ctx, "GetByBizID failed %v", err)
		return nil, err
	}
	return &table, nil
}

// GetByBizIDAndTableName 通过业务ID和表名获取
func (d *dao) GetByBizIDAndTableName(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64, tableName string) (*entity.Table, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var table entity.Table
	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND table_name = ? AND is_deleted = 0",
		corpBizID, appBizID, dbSourceBizID, tableName).First(&table).Error
	if err != nil {
		logx.E(ctx, "GetByBizIDAndTableName|appBizID: %v, dbSourceBizID: %v, tableName %v failed %v", appBizID, dbSourceBizID, tableName, err)
		return nil, err
	}
	return &table, nil
}

// GetByBizIDs 通过业务IDs获取
func (d *dao) GetByBizIDs(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*entity.Table, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var table []*entity.Table
	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id in ? AND is_deleted = 0",
		corpBizID, appBizID, dbTableBizIDs).Find(&table).Error
	if err != nil {
		logx.E(ctx, "GetByBizIDs failed %v", err)
		return nil, err
	}
	return table, nil
}

// ListByDBSourceBizID 分页查询某数据源下的表
func (d *dao) ListByDBSourceBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64, page, pageSize int) ([]*entity.Table, int64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, 0, errs.ErrParameterInvalid
	}
	var (
		tables []*entity.Table
		total  int64
	)
	db := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, dbSourceBizID)
	if err := db.Count(&total).Error; err != nil {
		logx.E(ctx, "ListByDBSourceBizID failed", err)
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	err := db.Order("id DESC").Offset(offset).Limit(pageSize).Find(&tables).Error
	if err != nil {
		logx.E(ctx, "ListByDBSourceBizID failed", err)
		return nil, 0, err
	}
	return tables, total, nil
}

// ListByDBSourceBizIDAndTableName 分页查询，支持根据 table_name 过滤
func (d *dao) ListByDBSourceBizIDAndTableName(ctx context.Context,
	corpBizID, appBizID, dbSourceBizID uint64, tableName string, page, pageSize int) ([]*entity.Table, int64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, 0, errs.ErrParameterInvalid
	}
	var (
		tables []*entity.Table
		total  int64
	)
	db := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND table_name LIKE ? AND is_deleted = 0",
			corpBizID, appBizID, dbSourceBizID, "%"+tableName+"%")
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	offset := (page - 1) * pageSize
	err := db.Order("id DESC").Offset(offset).Limit(pageSize).Find(&tables).Error
	if err != nil {
		logx.E(ctx, "ListByDBSourceBizIDAndTableName failed", err)
		return nil, 0, err
	}
	return tables, total, nil
}

func (d *dao) ListAllTableNameByDBSourceBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64) ([]string, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var tables []string
	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, dbSourceBizID).
		Order("id DESC").
		Pluck("table_name", &tables).Error
	if err != nil {
		logx.E(ctx, "ListAllTableNameByDBSourceBizID failed", err)
		return nil, err
	}
	return tables, nil
}

func (d *dao) ModifyTable(ctx context.Context, filter *entity.TableFilter, data map[string]any) error {
	if filter.CorpBizID == 0 || filter.AppBizID == 0 {
		return errs.ErrParameterInvalid
	}
	conds := tableFilterToConds(d.tdsql, filter)
	_, err := d.tdsql.TDbTable.WithContext(ctx).Where(conds...).Updates(data)
	if err != nil {
		return fmt.Errorf("dao:ModifyTable filter:%+v, data:%+v, error: %w", filter, data, err)
	}
	return nil
}

// BatchUpsertByBizID 批量插入或更新（通过业务ID），使用 OnConflict + Create 方式
func (d *dao) BatchUpsertByBizID(ctx context.Context, cols []string, tables []*entity.Table) error {
	if len(tables) == 0 {
		logx.W(ctx, "BatchUpsertByBizID failed: tables is empty")
		return nil
	}

	batchSize := 200
	for i := 0; i < len(tables); i += batchSize {
		end := i + batchSize
		if end > len(tables) {
			end = len(tables)
		}
		batch := tables[i:end]
		err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: DBCorpBizID},
				{Name: DBAppBizID},
				{Name: DBTableBizID},
			},
			DoUpdates: clause.AssignmentColumns(cols),
		}).Create(batch).Error

		if err != nil {
			logx.E(ctx, "BatchUpsertByBizID failed %v", err)
			return err
		}
	}
	return nil
}

// DeleteTable 删除表
func (d *dao) DeleteTable(ctx context.Context, filter *entity.TableFilter) error {
	if filter.CorpBizID == 0 || filter.AppBizID == 0 {
		return errs.ErrParameterInvalid
	}
	conds := tableFilterToConds(d.tdsql, filter)
	_, err := d.tdsql.TDbTable.WithContext(ctx).Where(conds...).Updates(
		map[string]any{
			"is_deleted":  boolx.Yes,
			"next_action": releaseEntity.ReleaseActionDelete,
			"release_status": gorm.Expr(`CASE WHEN next_action != ? THEN ? ELSE release_status END`,
				releaseEntity.ReleaseActionAdd, releaseEntity.ReleaseStatusInit),
		})
	if err != nil {
		return fmt.Errorf("dao:DeleteTable filter:%+v, error: %w", filter, err)
	}
	return nil
}

// BatchSoftDeleteByDBSourceBizID 批量软删除
func (d *dao) BatchSoftDeleteByDBSourceBizID(ctx context.Context, corpBizID, appBizID uint64, dbSourceBizIDs []uint64) error {
	if corpBizID == 0 || appBizID == 0 || len(dbSourceBizIDs) == 0 {
		return errs.ErrParameterInvalid
	}
	res := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id IN ? AND is_deleted = 0", corpBizID, appBizID, dbSourceBizIDs).
		Updates(map[string]any{
			"is_deleted": 1,
		})
	if res.Error != nil {
		logx.E(ctx, "BatchSoftDeleteByDBSourceBizID failed", res.Error)
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errs.ErrDataNotExistOrIsDeleted
	}
	return nil
}

func (d *dao) CollectUnreleasedDBTable(ctx context.Context, appBizID, releaseBizID uint64) error {
	// 1. 找出所有 release_status 为1 待发布的 t_db_table
	dbTables, err := d.DescribeUnreleasedTableList(ctx, appBizID)
	if err != nil {
		return err
	}
	if len(dbTables) == 0 {
		return nil
	}

	releaseTbl := d.tdsql.TReleaseDbTable

	corpBizID := dbTables[0].CorpBizID
	// 2. 补充releaseBizID，并写入到t_release_db_table
	for _, chunk := range slicex.Chunk(dbTables, 200) {
		var releaseChunk []map[string]any
		var ids []uint64
		for _, d := range chunk {
			ids = append(ids, d.DBTableBizID)
			releaseDBTable := map[string]any{
				releaseTbl.CorpBizID.ColumnName().String():         d.CorpBizID,
				releaseTbl.AppBizID.ColumnName().String():          d.AppBizID,
				releaseTbl.DbSourceBizID.ColumnName().String():     d.DBSourceBizID,
				releaseTbl.DbTableBizID.ColumnName().String():      d.DBTableBizID,
				releaseTbl.ReleaseBizID.ColumnName().String():      releaseBizID,
				releaseTbl.Source.ColumnName().String():            d.Source,
				releaseTbl.TableName_.ColumnName().String():        d.Name,
				releaseTbl.TableSchema.ColumnName().String():       d.TableSchema,
				releaseTbl.TableComment.ColumnName().String():      d.TableComment,
				releaseTbl.AliasName.ColumnName().String():         d.AliasName,
				releaseTbl.Description.ColumnName().String():       d.Description,
				releaseTbl.RowCount.ColumnName().String():          d.RowCount,
				releaseTbl.ColumnCount.ColumnName().String():       d.ColumnCount,
				releaseTbl.TableAddedTime.ColumnName().String():    d.TableAddedTime,
				releaseTbl.TableModifiedTime.ColumnName().String(): d.TableModifiedTime,
				releaseTbl.LastSyncTime.ColumnName().String():      d.LastSyncTime,
				releaseTbl.ReleaseStatus.ColumnName().String():     d.ReleaseStatus,
				releaseTbl.Action.ColumnName().String():            d.NextAction,
				releaseTbl.IsIndexed.ColumnName().String():         d.IsIndexed,
				releaseTbl.IsDeleted.ColumnName().String():         d.IsDeleted,
				releaseTbl.Alive.ColumnName().String():             d.Alive,
			}
			releaseChunk = append(releaseChunk, releaseDBTable)
		}
		err = d.tdsql.TReleaseDbTable.WithContext(ctx).UnderlyingDB().Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "corp_biz_id"},
				{Name: "app_biz_id"},
				{Name: "db_table_biz_id"},
				{Name: "release_biz_id"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"table_name":          gorm.Expr("VALUES(table_name)"),
				"table_schema":        gorm.Expr("VALUES(table_schema)"),
				"table_comment":       gorm.Expr("VALUES(table_comment)"),
				"alias_name":          gorm.Expr("VALUES(alias_name)"),
				"description":         gorm.Expr("VALUES(description)"),
				"row_count":           gorm.Expr("VALUES(row_count)"),
				"column_count":        gorm.Expr("VALUES(column_count)"),
				"table_added_time":    gorm.Expr("VALUES(table_added_time)"),
				"table_modified_time": gorm.Expr("VALUES(table_modified_time)"),
				"last_sync_time":      gorm.Expr("VALUES(last_sync_time)"),
				"release_status":      gorm.Expr("VALUES(release_status)"),
				"action":              gorm.Expr("VALUES(action)"),
				"is_indexed":          gorm.Expr("VALUES(is_indexed)"),
				"is_deleted":          gorm.Expr("VALUES(is_deleted)"),
			}),
		}).Create(releaseChunk).Error
		if err != nil {
			logx.E(ctx, "create release table error, %v", err)
			return err
		}

		// 3. 更新db_table表的状态为待发布
		err = d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Model(&model.TDbTable{}).
			Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id IN (?)",
				corpBizID, appBizID, ids).
			Updates(map[string]any{
				"release_status": releaseEntity.ReleaseStatusPending,
			}).Error
		if err != nil {
			logx.E(ctx, "update db table releasing error, id: %+v, %v", ids, err)
			return err
		}
	}

	return nil
}

// GetUnreleasedDBTable 获取待发布的数据表信息
func (d *dao) DescribeUnreleasedTableList(ctx context.Context, appBizID uint64) ([]*entity.Table, error) {
	limit := 200
	startID := uint64(0)
	var all []*entity.Table
	for {
		// var chunk []*entity.Table
		var sources []*model.TDbTable
		// 排除是新增，但是未发布的情况下又删除的数据库
		err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Where("app_biz_id = ? AND release_status = 1 AND "+
			"!(next_action = 1 AND is_deleted = 1) AND id > ? AND (is_deleted = 1 OR learn_status = ?)", appBizID, startID, entity.LearnStatusLearned).
			Order("id ASC").Limit(limit).Find(&sources).Error
		if err != nil {
			logx.E(ctx, "DescribeUnreleasedTableList error, %v", err)
			return nil, err
		}
		chunk := tablesPO2DO(sources)
		all = append(all, chunk...)
		if len(chunk) < limit {
			break
		}
		startID = chunk[len(chunk)-1].ID
	}
	logx.I(ctx, "DescribeUnreleasedTableList len: %v", len(all))
	return all, nil
}

// FindUnReleaseDBTableByConditions 根据 DBName、UpdateTime 和 NextAction 进行多条件检索
func (d *dao) DescribeUnreleaseTableListByConds(ctx context.Context, corpBizID, appBizID uint64, dbTableName string, beginTime,
	endTime time.Time, nextAction []uint32, page, pageSize uint32) ([]*entity.Table, error) {
	var sources []*model.TDbTable
	query := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Model(&model.TDbTable{})
	query = query.Where(sqlDbSourceCorpBizID+sqlEqual, corpBizID)
	query = query.Where(sqlDbSourceDbAppBizID+sqlEqual, appBizID)

	if dbTableName != "" {
		query = query.Where(sqlDbTableName+sqlLike, fmt.Sprintf("%%%s%%", dbTableName))
	}
	if !beginTime.IsZero() {
		query = query.Where(sqlDbSourceUpdateTime+sqlMoreEqual, beginTime)
	}
	if !endTime.IsZero() {
		query = query.Where(sqlDbSourceUpdateTime+sqlLessEqual, endTime)
	}

	if len(nextAction) > 0 {
		query = query.Where(sqlDbSourceNextAction+sqlIn, nextAction)
	}
	query = query.Where(sqlDbSourceReleaseStatus+sqlEqual, releaseEntity.ReleaseStatusInit)
	query = query.Where(sqlDbTableLearnStatus+sqlEqual, entity.LearnStatusLearned)
	query = query.Where(sqlAddAndDelWithoutPublish)
	query = query.Limit(int(pageSize)).Offset(int(pageSize * (page - 1)))
	query = query.Order(sqlDbSourceId + SqlOrderByDesc)

	if err := query.Find(&sources).Error; err != nil {
		logx.E(ctx, "DescribeUnreleaseTableListByConds|appBizID:%v, dbName:%v, beginTime:%v, endTime:%v, nextAction:%v, page:%v, pageSize:%v",
			appBizID, dbTableName, beginTime, endTime, nextAction, page, pageSize)
		return nil, err
	}
	return tablesPO2DO(sources), nil
}

// GetAllReleaseDBTables 获取单次发布的快照表 t_release_db_table 集合
func (d *dao) GetAllReleaseDBTables(ctx context.Context, appBizID, releaseBizID uint64, onlyBizID bool) ([]*entity.TableRelease, error) {
	limit := 200
	startID := uint64(0)
	var all []*entity.TableRelease
	for {
		var chunk []*model.TReleaseDbTable
		session := d.tdsql.TReleaseDbTable.WithContext(ctx).UnderlyingDB()
		if onlyBizID {
			session = session.Select([]string{"id", "db_table_biz_id"})
		}
		// 排除是新增，但是未发布的情况下又删除的数据库
		err := session.Where("app_biz_id = ? AND release_biz_id = ? AND id > ?", appBizID, releaseBizID, startID).
			Order("id ASC").Limit(limit).Find(&chunk).Error
		if err != nil {
			logx.E(ctx, "DescribeUnreleasedTableList error, %v", err)
			return nil, err
		}
		all = append(all, releaseTablePO2DOs(chunk)...)
		if len(chunk) < limit {
			break
		}
		startID = chunk[len(chunk)-1].ID
	}
	logx.I(ctx, "GetAllReleaseDBTables len: %v", len(all))
	return all, nil
}

func (d *dao) ReleaseDBTableToProd(ctx context.Context, releaseDBTable entity.TableRelease) error {
	tableProd := &model.TDbTableProd{
		CorpBizID:         releaseDBTable.CorpBizID,
		AppBizID:          releaseDBTable.AppBizID,
		DbSourceBizID:     releaseDBTable.DBSourceBizID,
		DbTableBizID:      releaseDBTable.DBTableBizID,
		Source:            releaseDBTable.Source,
		TableName_:        releaseDBTable.Name,
		TableSchema:       releaseDBTable.TableSchema,
		TableComment:      releaseDBTable.TableComment,
		AliasName:         releaseDBTable.AliasName,
		Description:       releaseDBTable.Description,
		RowCount:          uint64(releaseDBTable.RowCount),
		ColumnCount:       uint32(releaseDBTable.ColumnCount),
		TableAddedTime:    releaseDBTable.TableAddedTime,
		TableModifiedTime: releaseDBTable.TableModifiedTime,
		Alive:             releaseDBTable.Alive,
		IsIndexed:         releaseDBTable.IsIndexed,
		IsDeleted:         releaseDBTable.IsDeleted,
		ReleaseStatus:     releaseDBTable.ReleaseStatus,
		LastSyncTime:      releaseDBTable.LastSyncTime,
	}
	err := d.tdsql.TDbTableProd.WithContext(ctx).UnderlyingDB().Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "corp_biz_id"},
			{Name: "app_biz_id"},
			{Name: "db_table_biz_id"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"table_name":          gorm.Expr("VALUES(table_name)"),
			"table_schema":        gorm.Expr("VALUES(table_schema)"),
			"table_comment":       gorm.Expr("VALUES(table_comment)"),
			"alias_name":          gorm.Expr("VALUES(alias_name)"),
			"description":         gorm.Expr("VALUES(description)"),
			"row_count":           gorm.Expr("VALUES(row_count)"),
			"column_count":        gorm.Expr("VALUES(column_count)"),
			"table_added_time":    gorm.Expr("VALUES(table_added_time)"),
			"table_modified_time": gorm.Expr("VALUES(table_modified_time)"),
			"last_sync_time":      gorm.Expr("VALUES(last_sync_time)"),
			"is_deleted":          gorm.Expr("VALUES(is_deleted)"),
			"is_indexed":          gorm.Expr("VALUES(is_indexed)"),
			"alive":               gorm.Expr("VALUES(alive)"),
		}),
	}).Create(tableProd).Error
	if err != nil {
		logx.E(ctx, "insert release db table prod %v error, %v", releaseDBTable.DBSourceBizID, err)
		return err
	}

	// 修改评测端的表发布状态
	err = d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Model(&model.TDbTable{}).Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ?",
		releaseDBTable.CorpBizID, releaseDBTable.AppBizID, releaseDBTable.DBTableBizID).Updates(map[string]any{
		"release_status": releaseEntity.ReleaseStatusSuccess,
		"next_action":    releaseEntity.ReleaseActionPublish,
	}).Error
	if err != nil {
		logx.E(ctx, "update release status error, %v", err)
		return err
	}

	return nil
}

func (d *dao) GetReleaseDBTable(ctx context.Context, appBizID uint64, releaseBizID uint64, dbTableBizID uint64) (entity.TableRelease, error) {
	var releaseDBTable entity.TableRelease
	err := d.tdsql.TReleaseDbTable.WithContext(ctx).UnderlyingDB().Where("app_biz_id = ? AND release_biz_id = ? AND db_table_biz_id = ?",
		appBizID, releaseBizID, dbTableBizID).First(&releaseDBTable).Error
	if err != nil {
		logx.E(ctx, "get release db table error, table: %v, release: %v, error %v",
			dbTableBizID, releaseBizID, err)
		return entity.TableRelease{}, err
	}
	return releaseDBTable, nil
}

// GetCountByAppBizIDs 获取应用下所有表的数量
func (d *dao) GetCountByAppBizIDs(ctx context.Context, appBizIDs []uint64) (int64, error) {
	if len(appBizIDs) == 0 {
		return 0, errs.ErrParameterInvalid
	}
	count := int64(0)
	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).Where("app_biz_id in ?  AND is_deleted = 0",
		appBizIDs).Count(&count).Error
	if err != nil {
		logx.E(ctx, "GetByBizIDs failed %v", err)
		return 0, err
	}
	return count, nil
}

// GetCountByDbSourceBizID 获取数据库下所有表的数量
func (d *dao) GetCountByDbSourceBizID(ctx context.Context, corpBizID, dbSourceBizID uint64) (int64, error) {
	if dbSourceBizID == 0 {
		return 0, errs.ErrParameterInvalid
	}
	count := int64(0)
	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
		Where(DBCorpBizID+sqlEqual, corpBizID).
		Where(DBSourceBizID+sqlEqual, dbSourceBizID).
		Where(DBIsDeleted+sqlEqual, boolx.No).
		Count(&count).Error
	if err != nil {
		logx.E(ctx, "GetCountByDbSourceBizID failed: corpBizID:%v, dbSourceBizID:%v, err:%v", corpBizID, dbSourceBizID, err)
		return 0, err
	}
	return count, nil
}

// GetCountByDbSourceBizIDs 批量获取数据库下所有表的数量
func (d *dao) GetCountByDbSourceBizIDs(ctx context.Context, corpBizID uint64, dbSourceBizIDs []uint64) (map[uint64]int32, error) {
	if len(dbSourceBizIDs) == 0 {
		return nil, errs.ErrParameterInvalid
	}

	var results []struct {
		DBSourceBizID uint64
		Count         int64
	}

	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
		Select("db_source_biz_id, COUNT(*) as count").
		Where(DBCorpBizID+sqlEqual, corpBizID).
		Where(DBSourceBizID+sqlIn, dbSourceBizIDs).
		Where(DBIsDeleted+sqlEqual, boolx.No).
		Group(DBSourceBizID).
		Find(&results).Error

	if err != nil {
		logx.E(ctx, "GetCountByDbSourceBizIDs failed: corpBizID:%v, dbSourceBizIDs:%v, err:%v", corpBizID, dbSourceBizIDs, err)
		return nil, err
	}

	countMap := make(map[uint64]int32)
	for _, result := range results {
		countMap[result.DBSourceBizID] = int32(result.Count)
	}
	return countMap, nil
}

// BatchGetTableByBizIDs 批量获取表
func (d *dao) BatchGetTableByBizIDs(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*entity.Table, error) {
	var results []*entity.Table
	batchSize := 200 // 每批获取的记录数
	for i := 0; i < len(dbTableBizIDs); i += batchSize {
		end := i + batchSize
		if end > len(dbTableBizIDs) {
			end = len(dbTableBizIDs)
		}
		batch := dbTableBizIDs[i:end]
		var batchResults []*model.TDbTable
		err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
			Where(DBCorpBizID+sqlEqual, corpBizID).
			Where(DBAppBizID+sqlEqual, appBizID).
			Where(DBTableBizID+sqlIn, batch).
			Where(DBIsDeleted+sqlEqual, boolx.No).
			Find(&batchResults).Error
		if err != nil {
			logx.E(ctx, "BatchGetByBizIDs|failed:batchID:%v, err:%v", batch, err)
			return nil, err
		}
		results = append(results, tablesPO2DO(batchResults)...)
	}
	return results, nil
}

// BatchGetByBizIDsForProd 批量获取表
func (d *dao) BatchGetByBizIDsForProd(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*entity.TableProd, error) {
	var results []*entity.TableProd
	batchSize := 200 // 每批获取的记录数
	for i := 0; i < len(dbTableBizIDs); i += batchSize {
		end := i + batchSize
		if end > len(dbTableBizIDs) {
			end = len(dbTableBizIDs)
		}
		batch := dbTableBizIDs[i:end]
		var batchResults []*model.TDbTableProd
		err := d.tdsql.TDbTableProd.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTableProd).
			Where(DBCorpBizID+sqlEqual, corpBizID).
			Where(DBAppBizID+sqlEqual, appBizID).
			Where(DBTableBizID+sqlIn, batch).
			Where(DBIsDeleted+sqlEqual, boolx.No).
			Find(&batchResults).Error
		if err != nil {
			logx.E(ctx, "BatchGetByBizIDs|failed:batchID:%v, err:%v", batch, err)
			return nil, err
		}
		results = append(results, prodTablesPO2DO(batchResults)...)
	}
	return results, nil
}

func (d *dao) BatchGetTableName(ctx context.Context, corpBizID, appBizID uint64) ([]string, error) {
	res := make([]string, 0, 10)

	temp := make([]*entity.Table, 0, 10)
	dup := make(map[string]bool, 10)
	err := d.tdsql.TDbTable.WithContext(ctx).UnderlyingDB().Table(model.TableNameTDbTable).
		Where(DBCorpBizID+sqlEqual, corpBizID).
		Where(DBAppBizID+sqlEqual, appBizID).
		Where(DBIsDeleted+sqlEqual, boolx.No).
		FindInBatches(&temp, 200, func(tx *gorm.DB, batch int) error {
			for _, v := range temp {
				if dup[v.Name] {
					continue
				}
				dup[v.Name] = true
				res = append(res, v.Name)
			}
			return nil
		}).Error
	if err != nil {
		logx.E(ctx, "BatchGetTableName|failed:err:%v", err)
		return nil, err
	}
	return res, nil
}

func (d *dao) DescribeTable(ctx context.Context, filter *entity.TableFilter) (*entity.Table, error) {
	conds := tableFilterToConds(d.tdsql, filter)
	table, err := d.tdsql.TDbTable.WithContext(ctx).Where(conds...).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, fmt.Errorf("dao:DescribeTable failed: %w", err)
	}
	return tablePO2DO(table), nil
}

// DescribeTableList 查询表列表
func (d *dao) DescribeTableList(ctx context.Context, filter *entity.TableFilter) ([]*entity.Table, int64, error) {
	conds := tableFilterToConds(d.tdsql, filter)
	tbl := d.tdsql.TDbTable
	db := tbl.WithContext(ctx).Where(conds...)
	total, err := db.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeTableList count failed: %w", err)
	}

	if filter.PageNumber != nil {
		offset, limit := utilx.Page(*filter.PageNumber, filter.PageSize)
		db = db.Offset(offset).Limit(limit)
	}

	if len(filter.Select) > 0 {
		seletect := make([]field.Expr, 0, len(filter.Select))
		for _, field := range filter.Select {
			fn, ok := tbl.GetFieldByName(field)
			if !ok {
				return nil, 0, fmt.Errorf("dao:DescribeTableList invalid select field: %s", field)
			}
			seletect = append(seletect, fn)
		}
		db = db.Select(seletect...)
	}
	tables, err := db.Order(tbl.ID.Desc()).Find()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeTableList failed: %w", err)
	}
	return tablesPO2DO(tables), total, nil
}

// DescribeTableProdList 查询发布域表列表
func (d *dao) DescribeTableProdList(ctx context.Context, filter *entity.TableFilter) ([]*entity.TableProd, int64, error) {
	conds := tableProdFilterToConds(d.tdsql, filter)
	tbl := d.tdsql.TDbTableProd
	db := tbl.WithContext(ctx).Where(conds...)
	total, err := db.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeTableProdList count failed: %w", err)
	}

	if len(filter.Select) > 0 {
		seletect := make([]field.Expr, 0, len(filter.Select))
		for _, f := range filter.Select {
			fn, ok := tbl.GetFieldByName(f)
			if !ok {
				return nil, 0, fmt.Errorf("dao:DescribeTableProdList invalid select field: %s", f)
			}
			seletect = append(seletect, fn)
		}
		db = db.Select(seletect...)
	}
	tables, err := db.Order(tbl.ID.Desc()).Find()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeTableProdList failed: %w", err)
	}
	return prodTablesPO2DO(tables), total, nil
}

func (d *dao) CountTable(ctx context.Context, filter *entity.TableFilter) (int64, error) {
	conds := tableFilterToConds(d.tdsql, filter)
	count, err := d.tdsql.TDbTable.WithContext(ctx).Where(conds...).Count()
	if err != nil {
		return 0, fmt.Errorf("dao:CountTable failed: %w", err)
	}
	return count, nil
}

func tableFilterToConds(q *tdsqlquery.Query, filter *entity.TableFilter) (conds []gen.Condition) {
	if filter == nil {
		return
	}

	if filter.CorpBizID > 0 {
		conds = append(conds, q.TDbTable.CorpBizID.Eq(filter.CorpBizID))
	}
	if filter.AppBizID > 0 {
		conds = append(conds, q.TDbTable.AppBizID.Eq(filter.AppBizID))
	}
	if len(filter.AppBizIDs) > 0 {
		conds = append(conds, q.TDbTable.AppBizID.In(filter.AppBizIDs...))
	}
	if filter.DBSourceBizID > 0 {
		conds = append(conds, q.TDbTable.DbSourceBizID.Eq(filter.DBSourceBizID))
	}
	if len(filter.DBSourceBizIDs) > 0 {
		conds = append(conds, q.TDbTable.DbSourceBizID.In(filter.DBSourceBizIDs...))
	}
	if filter.DBTableBizID > 0 {
		conds = append(conds, q.TDbTable.DbTableBizID.Eq(filter.DBTableBizID))
	}
	if len(filter.DBTableBizIDs) > 0 {
		conds = append(conds, q.TDbTable.DbTableBizID.In(filter.DBTableBizIDs...))
	}
	if filter.LearnStatus != nil {
		conds = append(conds, q.TDbTable.LearnStatus.Eq(*filter.LearnStatus))
	}
	if filter.Name != nil && !stringx.IsEmpty(*filter.Name) {
		conds = append(conds, q.TDbTable.TableName_.Eq(*filter.Name))
	}
	if filter.NameLike != nil && !stringx.IsEmpty(*filter.NameLike) {
		conds = append(conds, q.TDbTable.TableName_.Like("%"+*filter.NameLike+"%"))
	}

	if filter.EnableScope != nil {
		conds = append(conds, q.TDbTable.EnableScope.Eq(*filter.EnableScope))
	}

	if filter.IsDeleted == nil { // 默认查未删除的
		conds = append(conds, q.TDbTable.IsDeleted.Is(false))
	} else {
		conds = append(conds, q.TDbTable.IsDeleted.Is(*filter.IsDeleted))
	}

	return
}

func tableProdFilterToConds(q *tdsqlquery.Query, filter *entity.TableFilter) (conds []gen.Condition) {
	if filter == nil {
		return
	}

	if filter.CorpBizID > 0 {
		conds = append(conds, q.TDbTableProd.CorpBizID.Eq(filter.CorpBizID))
	}
	if filter.AppBizID > 0 {
		conds = append(conds, q.TDbTableProd.AppBizID.Eq(filter.AppBizID))
	}
	if len(filter.AppBizIDs) > 0 {
		conds = append(conds, q.TDbTableProd.AppBizID.In(filter.AppBizIDs...))
	}
	if filter.DBSourceBizID > 0 {
		conds = append(conds, q.TDbTableProd.DbSourceBizID.Eq(filter.DBSourceBizID))
	}
	if len(filter.DBSourceBizIDs) > 0 {
		conds = append(conds, q.TDbTableProd.DbSourceBizID.In(filter.DBSourceBizIDs...))
	}
	if filter.DBTableBizID > 0 {
		conds = append(conds, q.TDbTableProd.DbTableBizID.Eq(filter.DBTableBizID))
	}
	if len(filter.DBTableBizIDs) > 0 {
		conds = append(conds, q.TDbTableProd.DbTableBizID.In(filter.DBTableBizIDs...))
	}
	if filter.Name != nil && !stringx.IsEmpty(*filter.Name) {
		conds = append(conds, q.TDbTableProd.TableName_.Eq(*filter.Name))
	}
	if filter.NameLike != nil && !stringx.IsEmpty(*filter.NameLike) {
		conds = append(conds, q.TDbTableProd.TableName_.Like("%"+*filter.NameLike+"%"))
	}

	if filter.IsDeleted == nil { // 默认查未删除的
		conds = append(conds, q.TDbTableProd.IsDeleted.Is(false))
	} else {
		conds = append(conds, q.TDbTableProd.IsDeleted.Is(*filter.IsDeleted))
	}

	return
}

func tablesPO2DO(tables []*model.TDbTable) []*entity.Table {
	res := make([]*entity.Table, 0, len(tables))
	for _, t := range tables {
		res = append(res, tablePO2DO(t))
	}
	return res
}

func tablePO2DO(table *model.TDbTable) *entity.Table {
	if table == nil {
		return nil
	}
	return &entity.Table{
		ID:                table.ID,
		CorpBizID:         table.CorpBizID,
		AppBizID:          table.AppBizID,
		DBSourceBizID:     table.DbSourceBizID,
		DBTableBizID:      table.DbTableBizID,
		Source:            table.Source,
		Name:              table.TableName_,
		TableSchema:       table.TableSchema,
		TableComment:      table.TableComment,
		AliasName:         table.AliasName,
		Description:       table.Description,
		RowCount:          int64(table.RowCount),
		ColumnCount:       int(table.ColumnCount),
		TableAddedTime:    table.TableAddedTime,
		TableModifiedTime: table.TableModifiedTime,
		LastSyncTime:      table.LastSyncTime,
		LearnStatus:       table.LearnStatus,
		ReleaseStatus:     table.ReleaseStatus,
		NextAction:        table.NextAction,
		Alive:             table.Alive,
		IsIndexed:         table.IsIndexed,
		PrevIsIndexed:     table.PrevIsIndexed,
		IsDeleted:         table.IsDeleted,
		StaffID:           table.StaffID,
		CreateTime:        table.CreateTime,
		UpdateTime:        table.UpdateTime,
		EnableScope:       table.EnableScope,
	}
}

func tablesDO2PO(tables []*entity.Table) []*model.TDbTable {
	res := make([]*model.TDbTable, 0, len(tables))
	for _, t := range tables {
		res = append(res, tableDO2PO(t))
	}
	return res
}

func tableDO2PO(table *entity.Table) *model.TDbTable {
	if table == nil {
		return nil
	}
	return &model.TDbTable{
		ID:                table.ID,
		CorpBizID:         table.CorpBizID,
		AppBizID:          table.AppBizID,
		DbSourceBizID:     table.DBSourceBizID,
		DbTableBizID:      table.DBTableBizID,
		Source:            table.Source,
		TableName_:        table.Name,
		TableSchema:       table.TableSchema,
		TableComment:      table.TableComment,
		AliasName:         table.AliasName,
		Description:       table.Description,
		RowCount:          uint64(table.RowCount),
		ColumnCount:       uint32(table.ColumnCount),
		TableAddedTime:    table.TableAddedTime,
		TableModifiedTime: table.TableModifiedTime,
		LastSyncTime:      table.LastSyncTime,
		LearnStatus:       table.LearnStatus,
		ReleaseStatus:     table.ReleaseStatus,
		NextAction:        table.NextAction,
		Alive:             table.Alive,
		IsIndexed:         table.IsIndexed,
		PrevIsIndexed:     table.PrevIsIndexed,
		IsDeleted:         table.IsDeleted,
		StaffID:           table.StaffID,
		CreateTime:        table.CreateTime,
		UpdateTime:        table.UpdateTime,
		EnableScope:       table.EnableScope,
	}
}

func prodTablesPO2DO(tables []*model.TDbTableProd) []*entity.TableProd {
	res := make([]*entity.TableProd, 0, len(tables))
	for _, t := range tables {
		res = append(res, prodTablePO2DO(t))
	}
	return res
}

func prodTablePO2DO(table *model.TDbTableProd) *entity.TableProd {
	if table == nil {
		return nil
	}
	return &entity.TableProd{
		ID:                table.ID,
		CorpBizID:         table.CorpBizID,
		AppBizID:          table.AppBizID,
		DBSourceBizID:     table.DbSourceBizID,
		DBTableBizID:      table.DbTableBizID,
		Source:            table.Source,
		Name:              table.TableName_,
		TableSchema:       table.TableSchema,
		TableComment:      table.TableComment,
		AliasName:         table.AliasName,
		Description:       table.Description,
		RowCount:          int64(table.RowCount),
		ColumnCount:       int(table.ColumnCount),
		TableAddedTime:    table.TableAddedTime,
		TableModifiedTime: table.TableModifiedTime,
		LastSyncTime:      table.LastSyncTime,
		ReleaseStatus:     table.ReleaseStatus,
		Alive:             table.Alive,
		IsDeleted:         table.IsDeleted,
		CreateTime:        table.CreateTime,
		UpdateTime:        table.UpdateTime,
	}
}
