package database

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/gox/stringx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	countMaxLimit = 200000

	mysqlCountSql          = "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	mysqlGetPrimaryKeySql  = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI' ORDER BY ORDINAL_POSITION"
	mysqlCheckTableNameSql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"

	sqlserverUniqueidentifier = "UNIQUEIDENTIFIER"
	sqlServerGetPrimaryKeySql = `
        SELECT k.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
          ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        WHERE t.TABLE_NAME = @p1
          AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY k.ORDINAL_POSITION;
    `
	sqlServerCheckTableNameSql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = @p1 AND TABLE_NAME = @p2"
	SqlServerCountSql          = `
    SELECT 
        SUM(p.rows) AS row_count
    FROM 
        sys.tables AS t
        INNER JOIN sys.partitions AS p ON t.object_id = p.object_id
    WHERE 
        t.name = @p1
        AND p.index_id IN (0,1)
    GROUP BY 
        t.name;
`
)

const (
	sqlDbSourceId              = " id "
	sqlDbSourceDbName          = "db_name"
	sqlDbSourceUpdateTime      = "update_time"
	sqlDbSourceNextAction      = "next_action"
	sqlDbSourceCorpBizID       = "corp_biz_id"
	sqlDbSourceDbAppBizID      = "app_biz_id"
	sqlDbSourceReleaseStatus   = "release_status"
	sqlAddAndDelWithoutPublish = "!(next_action = 1 AND is_deleted = 1)"
)

// DescribeAvailableDBTypes 查询可用的数据库类型
func (d *dao) DescribeAvailableDBTypes(ctx context.Context) ([]string, error) {
	var types []string
	for key, _ := range d.sourceMap {
		types = append(types, key)
	}

	sort.Strings(types)
	logx.I(ctx, "DescribeAvailableDBTypes, types[%d]: %v", len(types), types)
	return types, nil
}

// CreateDatabase 新增 DB
func (d *dao) CreateDatabase(ctx context.Context, item *entity.Database) error {
	data := databaseDO2PO(item)
	err := d.tdsql.TDbSource.WithContext(ctx).Create(data)
	if err != nil {
		return fmt.Errorf("dao:Create error: %w", err)
	}
	return nil
}

// DescribeDatabase 通过业务ID获取
func (d *dao) DescribeDatabase(ctx context.Context, filter *entity.DatabaseFilter) (*entity.Database, error) {
	conds := dbFilterToConds(d.tdsql, filter)
	data, err := d.tdsql.TDbSource.WithContext(ctx).Where(conds...).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, fmt.Errorf("dao:GetByBizID Find error: %w", err)
	}
	return databasePO2DO(data), nil
}

// DescribeDatabaseProd 通过业务ID获取
func (d *dao) DescribeDatabaseProd(ctx context.Context, filter *entity.DatabaseFilter) (*entity.DatabaseProd, error) {
	conds := dbProdFilterToConds(d.tdsql, filter)
	data, err := d.tdsql.TDbSourceProd.WithContext(ctx).Where(conds...).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, fmt.Errorf("dao:GetByBizID Find error: %w", err)
	}
	return prodDatabasePO2DO(data), nil
}

// DescribeDatabaseProd ...
func (d *dao) DescribeDatabaseProdList(ctx context.Context, filter *entity.DatabaseFilter) ([]*entity.DatabaseProd, error) {
	conds := dbProdFilterToConds(d.tdsql, filter)
	list, err := d.tdsql.TDbSourceProd.WithContext(ctx).Where(conds...).Find()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, fmt.Errorf("dao:GetByBizID Find error: %w", err)
	}
	return prodDatabasesDO2PO(list), nil
}

func dbFilterToConds(q *tdsqlquery.Query, filter *entity.DatabaseFilter) []gen.Condition {
	var conds []gen.Condition
	if filter == nil {
		return conds
	}

	if filter.CorpBizID > 0 {
		conds = append(conds, q.TDbSource.CorpBizID.Eq(filter.CorpBizID))
	}
	// AppBizIDs和AppBizID互斥使用，优先使用AppBizIDs
	if len(filter.AppBizIDs) > 0 {
		conds = append(conds, q.TDbSource.AppBizID.In(filter.AppBizIDs...))
	} else if filter.AppBizID > 0 {
		conds = append(conds, q.TDbSource.AppBizID.Eq(filter.AppBizID))
	}
	if filter.DBSourceBizID > 0 {
		conds = append(conds, q.TDbSource.DbSourceBizID.Eq(filter.DBSourceBizID))
	}
	if len(filter.DBSourceBizIDs) > 0 {
		conds = append(conds, q.TDbSource.DbSourceBizID.In(filter.DBSourceBizIDs...))
	}
	if filter.IsDeleted == nil { // 默认查未删除的
		conds = append(conds, q.TDbSource.IsDeleted.Is(false))
	} else {
		conds = append(conds, q.TDbSource.IsDeleted.Is(*filter.IsDeleted))
	}

	if filter.DBNameEq != nil {
		conds = append(conds, q.TDbSource.AliasName.Eq(*filter.DBNameEq))
	}
	if filter.DBNameLike != nil && !stringx.IsEmpty(*filter.DBNameLike) {
		conds = append(conds, q.TDbSource.AliasName.Like("%"+*filter.DBNameLike+"%"))
	}
	if len(filter.ReleaseStatus) > 0 {
		conds = append(conds, q.TDbSource.ReleaseStatus.In(filter.ReleaseStatus...))
	}
	// 如果传入了启用状态，添加过滤条件
	if filter.IsEnable != nil {
		conds = append(conds, q.TDbSource.IsIndexed.Is(*filter.IsEnable))
	}

	if filter.EnableScope != nil {
		conds = append(conds, q.TDbSource.EnableScope.Eq(*filter.EnableScope))
	}
	return conds
}

func dbProdFilterToConds(q *tdsqlquery.Query, filter *entity.DatabaseFilter) []gen.Condition {
	var conds []gen.Condition
	if filter == nil {
		return conds
	}

	if filter.CorpBizID > 0 {
		conds = append(conds, q.TDbSourceProd.CorpBizID.Eq(filter.CorpBizID))
	}
	if filter.AppBizID > 0 {
		conds = append(conds, q.TDbSourceProd.AppBizID.Eq(filter.AppBizID))
	}
	if filter.DBSourceBizID > 0 {
		conds = append(conds, q.TDbSourceProd.DbSourceBizID.Eq(filter.DBSourceBizID))
	}
	if len(filter.DBSourceBizIDs) > 0 {
		conds = append(conds, q.TDbSourceProd.DbSourceBizID.In(filter.DBSourceBizIDs...))
	}
	if filter.IsDeleted == nil { // 默认查未删除的
		conds = append(conds, q.TDbSourceProd.IsDeleted.Is(false))
	} else {
		conds = append(conds, q.TDbSourceProd.IsDeleted.Is(*filter.IsDeleted))
	}

	if filter.DBNameEq != nil {
		conds = append(conds, q.TDbSourceProd.AliasName.Eq(*filter.DBNameEq))
	}
	if filter.DBNameLike != nil && !stringx.IsEmpty(*filter.DBNameLike) {
		conds = append(conds, q.TDbSourceProd.AliasName.Like("%"+*filter.DBNameLike+"%"))
	}
	if len(filter.ReleaseStatus) > 0 {
		conds = append(conds, q.TDbSourceProd.ReleaseStatus.In(filter.ReleaseStatus...))
	}
	return conds
}

// CountDatabase 获取应用下数据源数量
func (d *dao) CountDatabase(ctx context.Context, dbFilter *entity.DatabaseFilter) (int64, error) {
	conds := dbFilterToConds(d.tdsql, dbFilter)
	count, err := d.tdsql.TDbSource.WithContext(ctx).Where(conds...).Count()
	if err != nil {
		return 0, fmt.Errorf("dao:GetDbSourceNumByAppBizID Count error: %w", err)
	}
	return count, nil
}

func (d *dao) DeleteDatabase(ctx context.Context, filter *entity.DatabaseFilter) error {
	err := d.ModifyDatabaseSimple(ctx, filter, map[string]any{"is_deleted": 1})
	if err != nil {
		return fmt.Errorf("dao:DeleteDatabase Delete error: %w", err)
	}
	return nil
}

// GetDbSourceBizIdByAppBizID 获取应用下的数据源BizId
func (d *dao) GetDbSourceBizIdByAppBizID(ctx context.Context, corpBizID, appBizID uint64) ([]uint64, error) {
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.CorpBizID.Eq(corpBizID),
		tbl.AppBizID.Eq(appBizID),
		tbl.IsDeleted.Is(false),
	}
	var dbSourceBizIds []uint64
	err := tbl.WithContext(ctx).Where(conds...).Select(tbl.DbSourceBizID).Scan(&dbSourceBizIds)
	if err != nil {
		return nil, fmt.Errorf("dao:GetDbSourceBizIdByAppBizID Find error: %w", err)
	}
	return dbSourceBizIds, nil
}

// ListByAppBizID 分页查询
func (d *dao) ListByAppBizID(ctx context.Context, corpBizID, appBizID uint64, page, pageSize int) ([]*entity.Database, int64, error) {
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.CorpBizID.Eq(corpBizID),
		tbl.AppBizID.Eq(appBizID),
		tbl.IsDeleted.Is(false),
	}
	db := tbl.WithContext(ctx).Where(conds...)
	total, err := db.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:ListByAppBizID Count error: %w", err)
	}
	if total == 0 {
		return []*entity.Database{}, 0, nil
	}
	offset := (page - 1) * pageSize
	pos, err := db.Order(tbl.ID.Desc()).Offset(offset).Limit(pageSize).Find()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:ListByAppBizID Find error: %w", err)
	}
	return databasesPO2DO(pos), total, nil
}

// ListByAppBizIDAndDBName 分页查询，支持根据 db_name 过滤
func (d *dao) ListByAppBizIDAndDBName(ctx context.Context, corpBizID, appBizID uint64, dbName string, page, pageSize int) ([]*entity.Database, int64, error) {
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.CorpBizID.Eq(corpBizID),
		tbl.AppBizID.Eq(appBizID),
		tbl.IsDeleted.Is(false),
	}
	db := tbl.WithContext(ctx)
	db = db.Where(conds...).Where(
		db.Where(tbl.DbSourceBizID.Eq(convx.MustStringToUint64(dbName))).
			Or(tbl.AliasName.Like("%" + dbName + "%")))
	total, err := db.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:ListByAppBizIDAndDBName Count error: %w", err)
	}
	if total == 0 {
		return []*entity.Database{}, 0, nil
	}
	offset := (page - 1) * pageSize
	pos, err := db.Order(tbl.ID.Desc()).Offset(offset).Limit(pageSize).Find()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:ListByAppBizIDAndDBName Find error: %w", err)
	}
	return databasesPO2DO(pos), total, nil
}

func (d *dao) UpdateByBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64, data map[string]any) error {
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.CorpBizID.Eq(corpBizID),
		tbl.AppBizID.Eq(appBizID),
		tbl.DbSourceBizID.Eq(dbSourceBizID),
		tbl.IsDeleted.Is(false),
	}
	_, err := tbl.WithContext(ctx).Where(conds...).Updates(data)
	if err != nil {
		return fmt.Errorf("dao:UpdateByBizID error: %w", err)
	}
	return nil
}

func (d *dao) ModifyDatabaseSimple(ctx context.Context, filter *entity.DatabaseFilter, data map[string]any) error {
	conds := dbFilterToConds(d.tdsql, filter)
	_, err := d.tdsql.TDbSource.WithContext(ctx).Where(conds...).Updates(data)
	if err != nil {
		return fmt.Errorf("dao:ModifyDatabaseSimple error: %w", err)
	}
	return nil
}

// ModifyDatabase 通过业务ID更新（部分字段）
// UpdateDBByBizID
func (d *dao) ModifyDatabase(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64,
	updateColumns []string, dbSource *entity.Database) error {
	db := databaseDO2PO(dbSource)
	res := d.tdsql.TDbSource.WithContext(ctx).UnderlyingDB().Model(&model.TDbSource{}).
		Select(updateColumns).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0",
			corpBizID, appBizID, dbSourceBizID).
		Updates(db)

	if res.Error != nil {
		logx.E(ctx, "update db source failed: %v", res.Error)
		return res.Error
	}
	return nil
}

// SoftDeleteByBizID 软删除
func (d *dao) SoftDeleteByBizID(ctx context.Context, corpBizId, appBizID, dbSourceBizID uint64) error {
	res := d.tdsql.TDbSource.WithContext(ctx).UnderlyingDB().Model(&entity.Database{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0",
			corpBizId, appBizID, dbSourceBizID).
		Updates(map[string]any{
			"is_deleted":  1,
			"next_action": releaseEntity.ReleaseActionDelete,
			"release_status": gorm.Expr(`CASE WHEN next_action != ? THEN ? ELSE release_status END`,
				releaseEntity.ReleaseActionAdd, releaseEntity.ReleaseStatusInit),
		})
	if res.Error != nil {
		logx.E(ctx, "soft delete db source failed: %v", res.Error)
		return res.Error
	}
	return nil
}

func (d *dao) BatchGetDbSources(ctx context.Context, appBizId uint64, dbSourceBizIDs []uint64) ([]*entity.Database, error) {
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.AppBizID.Eq(appBizId),
		tbl.DbSourceBizID.In(dbSourceBizIDs...),
		tbl.IsDeleted.Is(false),
	}
	pos, err := tbl.WithContext(ctx).Where(conds...).Find()
	if err != nil {
		return nil, fmt.Errorf("dao:BatchGetDbSources Find error: %w", err)
	}
	return databasesPO2DO(pos), nil
}

// bytesToGUIDString 把 16 字节转成标准 GUID 字符串
func bytesToGUIDString(b []byte) string {
	if len(b) != 16 {
		return ""
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.LittleEndian.Uint32(b[0:4]),
		binary.LittleEndian.Uint16(b[4:6]),
		binary.LittleEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16],
	)
}

func (d *dao) CollectUnreleasedDatabase(ctx context.Context, appBizID, releaseBizID uint64) error {
	// 1. 找出所有 release_status 为1 待发布的 t_db_source
	dbSources, err := d.DescribeUnreleasedDatabase(ctx, appBizID)
	if err != nil {
		return err
	}
	if len(dbSources) == 0 {
		return nil
	}

	corpBizID := dbSources[0].CorpBizID

	// 2. 补充releaseBizID，并写入到t_release_db_source
	for _, chunk := range slicex.Chunk(dbSources, 100) {
		var releaseChunk []*model.TReleaseDbSource
		var ids []uint64
		for _, d := range chunk {
			ids = append(ids, d.DBSourceBizID)
			releaseDBSource := &model.TReleaseDbSource{
				CorpBizID:     d.CorpBizID,
				AppBizID:      d.AppBizID,
				DbSourceBizID: d.DBSourceBizID,
				DbName:        d.DBName,
				AliasName:     d.AliasName,
				Description:   d.AliasName,
				DbType:        d.DBType,
				Host:          d.Host,
				Port:          d.Port,
				Username:      d.Username,
				Password:      d.Password,
				Alive:         d.Alive,
				LastSyncTime:  d.LastSyncTime,
				ReleaseStatus: d.ReleaseStatus,
				ReleaseBizID:  releaseBizID,
				Action:        d.NextAction,
				IsDeleted:     d.IsDeleted,
				SchemaName:    d.SchemaName,
			}
			releaseChunk = append(releaseChunk, releaseDBSource)
		}

		err = d.tdsql.TReleaseDbSource.WithContext(ctx).Debug().UnderlyingDB().Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "corp_biz_id"},
				{Name: "app_biz_id"},
				{Name: "db_source_biz_id"},
				{Name: "release_biz_id"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"db_name":        gorm.Expr("VALUES(db_name)"),
				"alias_name":     gorm.Expr("VALUES(alias_name)"),
				"description":    gorm.Expr("VALUES(description)"),
				"db_type":        gorm.Expr("VALUES(db_type)"),
				"host":           gorm.Expr("VALUES(host)"),
				"port":           gorm.Expr("VALUES(port)"),
				"username":       gorm.Expr("VALUES(username)"),
				"password":       gorm.Expr("VALUES(password)"),
				"alive":          gorm.Expr("VALUES(alive)"),
				"last_sync_time": gorm.Expr("VALUES(last_sync_time)"),
				"release_status": gorm.Expr("VALUES(release_status)"),
				"action":         gorm.Expr("VALUES(action)"),
				"is_deleted":     gorm.Expr("VALUES(is_deleted)"),
			}),
		}).Create(releaseChunk).Error
		if err != nil {
			logx.E(ctx, "create release db source error, %v", err)
			return err
		}

		// 3. 更新db_source表的状态为待发布
		tbl := d.tdsql.TDbSource
		conds := []gen.Condition{
			tbl.CorpBizID.Eq(corpBizID),
			tbl.AppBizID.Eq(appBizID),
			tbl.DbSourceBizID.In(ids...),
		}
		_, err = tbl.WithContext(ctx).Where(conds...).Updates(tbl.ReleaseStatus.Eq(releaseEntity.ReleaseStatusPending))
		if err != nil {
			logx.E(ctx, "update db source releasing error, id: %+v, %v", ids, err)
			return err
		}
	}

	return nil
}

// DescribeUnreleasedDatabase 获取所有的待发布的db source表记录
func (d *dao) DescribeUnreleasedDatabase(ctx context.Context, appBizID uint64) ([]*entity.Database, error) {
	// 排除是新增，但是未发布的情况下又删除的数据库
	// 原来的 raw sql 是 "app_biz_id = ? AND release_status = 1 AND !(next_action = 1 AND is_deleted = 1) AND id > ?"
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.AppBizID.Eq(appBizID),
		tbl.ReleaseStatus.Eq(releaseEntity.ReleaseStatusInit),
	}
	db := tbl.WithContext(ctx)
	data, err := db.Where(conds...).Where(
		db.Where(tbl.NextAction.Neq(releaseEntity.ReleaseActionAdd)).
			Or(tbl.IsDeleted.Is(false))).
		Order(tbl.ID.Asc()).FindInBatch(200, func(tx gen.Dao, batch int) error { return nil })
	if err != nil {
		logx.E(ctx, "DescribeUnreleasedDatabase error, %v", err)
		return nil, err
	}
	logx.I(ctx, "DescribeUnreleasedDatabase len: %v", len(data))
	return databasesPO2DO(data), nil
}

// FindUnReleaseDBSourceByConditions 根据 DBName、UpdateTime 和 NextAction 进行多条件检索
func (d *dao) FindUnreleaseDatabaseByConds(ctx context.Context, corpBizID, appBizID uint64, dbName string, beginTime,
	endTime time.Time, nextAction []uint32, page, pageSize uint32) ([]*entity.Database, error) {
	var all []*model.TDbSource
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.CorpBizID.Eq(corpBizID),
		tbl.AppBizID.Eq(appBizID),
		tbl.ReleaseStatus.Eq(releaseEntity.ReleaseStatusInit),
	}
	if dbName != "" {
		conds = append(conds, tbl.DbName.Like(fmt.Sprintf("%%%s%%", dbName)))
	}
	if !beginTime.IsZero() {
		conds = append(conds, tbl.UpdateTime.Gte(beginTime))
	}
	if !endTime.IsZero() {
		conds = append(conds, tbl.UpdateTime.Lte(endTime))
	}
	if len(nextAction) > 0 {
		conds = append(conds, tbl.NextAction.In(nextAction...))
	}

	db := tbl.WithContext(ctx)
	db = db.Where(conds...).Where(
		db.Where(tbl.NextAction.Neq(releaseEntity.ReleaseActionAdd)).
			Or(tbl.IsDeleted.Is(false)))
	offset := (page - 1) * pageSize
	chunk, err := db.Order(tbl.ID.Desc()).Limit(int(pageSize)).Offset(int(offset)).Find()
	if err != nil {
		logx.E(ctx, "FindUnreleaseDatabaseByConds| appBizID:%v, dbName:%v, beginTime:%v, endTime:%v, nextAction:%v, page:%v, pageSize:%v",
			appBizID, dbName, beginTime, endTime, nextAction, page, pageSize)
		return nil, err
	}
	all = append(all, chunk...)
	return databasesPO2DO(all), nil
}

// GetAllReleaseDBSources 获取db source的快照信息
func (d *dao) DescribeReleaseDatabaseList(ctx context.Context, appBizID, releaseBizID uint64) ([]*entity.DatabaseRelease, error) {
	tbl := d.tdsql.TReleaseDbSource
	conds := []gen.Condition{
		tbl.AppBizID.Eq(appBizID),
		tbl.ReleaseBizID.Eq(releaseBizID),
	}
	data, err := tbl.WithContext(ctx).Where(conds...).Order(tbl.ID.Asc()).
		FindInBatch(200, func(tx gen.Dao, batch int) error { return nil })
	if err != nil {
		logx.E(ctx, "DescribeReleaseDatabaseList error, %v", err)
		return nil, err
	}
	logx.I(ctx, "DescribeReleaseDatabaseList len: %v", len(data))
	return releaseDatabasesPO2DO(data), nil
}

// ReleaseDBSource 发布db source
func (d *dao) ReleaseDBSource(ctx context.Context, appBizID, releaseBizID uint64) error {
	releaseDBSources, err := d.DescribeReleaseDatabaseList(ctx, appBizID, releaseBizID)
	if err != nil {
		return err
	}
	// 2. 补充releaseBizID，并写入到t_release_db_source 的
	var dbSourceBizIDs []uint64
	for _, chunk := range slicex.Chunk(releaseDBSources, 100) {
		corpBizID := chunk[0].CorpBizID
		var dbSourceChunk []*model.TDbSourceProd
		for _, d := range chunk {
			dbSource := &model.TDbSourceProd{
				CorpBizID:     d.CorpBizID,
				AppBizID:      d.AppBizID,
				DbSourceBizID: d.DBSourceBizID,
				DbName:        d.DBName,
				AliasName:     d.AliasName,
				Description:   d.Description,
				DbType:        d.DBType,
				Host:          d.Host,
				Port:          d.Port,
				Username:      d.Username,
				Password:      d.Password,
				Alive:         d.Alive,
				LastSyncTime:  d.LastSyncTime,
				IsDeleted:     d.IsDeleted,
				SchemaName:    d.SchemaName,
			}
			dbSourceChunk = append(dbSourceChunk, dbSource)
			dbSourceBizIDs = append(dbSourceBizIDs, d.DBSourceBizID)
		}
		// 1. 将release表复制到prod表
		err = d.tdsql.TDbSourceProd.WithContext(ctx).UnderlyingDB().Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "corp_biz_id"},
				{Name: "app_biz_id"},
				{Name: "db_source_biz_id"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"db_name":        gorm.Expr("VALUES(db_name)"),
				"alias_name":     gorm.Expr("VALUES(alias_name)"),
				"description":    gorm.Expr("VALUES(description)"),
				"db_type":        gorm.Expr("VALUES(db_type)"),
				"host":           gorm.Expr("VALUES(host)"),
				"port":           gorm.Expr("VALUES(port)"),
				"username":       gorm.Expr("VALUES(username)"),
				"password":       gorm.Expr("VALUES(password)"),
				"alive":          gorm.Expr("VALUES(alive)"),
				"last_sync_time": gorm.Expr("VALUES(last_sync_time)"),
				"is_deleted":     gorm.Expr("VALUES(is_deleted)"),
			}),
		}).Create(dbSourceChunk).Error
		if err != nil {
			logx.E(ctx, "DatabaseRelease|Create appBizID: %v, releaseBizID: %v error, %v", appBizID, releaseBizID, err)
			return err
		}

		// 2. 修改评测端的表发布状态
		tbl := d.tdsql.TDbSource
		conds := []gen.Condition{
			tbl.CorpBizID.Eq(corpBizID),
			tbl.AppBizID.Eq(appBizID),
			tbl.DbSourceBizID.In(dbSourceBizIDs...),
		}
		_, err = tbl.WithContext(ctx).Where(conds...).Updates(map[string]any{
			"release_status": releaseEntity.ReleaseStatusSuccess,
			"next_action":    releaseEntity.ReleaseActionPublish,
		})
		if err != nil {
			logx.E(ctx, "update release status error, %v", err)
			return err
		}
	}

	return nil
}

func (d *dao) CountByBizIDAndStatus(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64) (int64, error) {
	results, err := d.CountByBizIDsAndStatus(ctx, corpBizID, appBizID, []uint64{dbSourceBizID})
	if err != nil {
		return 0, err
	}
	return results[dbSourceBizID], nil
}

// todo 性能优化
func (d *dao) CountByBizIDsAndStatus(ctx context.Context, corpBizID, appBizID uint64, dbSourceBizIDs []uint64) (map[uint64]int64, error) {
	if len(dbSourceBizIDs) == 0 {
		return make(map[uint64]int64), nil
	}
	countMap := make(map[uint64]int64, len(dbSourceBizIDs))
	tbl := d.tdsql.TDbTable
	conds := []gen.Condition{
		tbl.CorpBizID.Eq(corpBizID),
		tbl.AppBizID.Eq(appBizID),
		tbl.DbSourceBizID.In(dbSourceBizIDs...),
		tbl.IsDeleted.Is(false),
		tbl.LearnStatus.Eq(entity.LearnStatusLearning),
	}
	err := tbl.WithContext(ctx).Select(tbl.DbSourceBizID, tbl.ID.Count().As("count")).
		Where(conds...).Group(tbl.DbSourceBizID).Scan(&countMap)
	if err != nil {
		return nil, fmt.Errorf("dao:CountByBizIDsAndStatus error: %w", err)
	}
	return countMap, nil
}

// BatchGetByBizIDs 批量获取数据库源信息
func (d *dao) BatchGetByBizIDs(ctx context.Context, dbSourceBizIDs []uint64) ([]*entity.Database, error) {
	if len(dbSourceBizIDs) == 0 {
		return nil, nil
	}
	dbSourceBizIDs = slicex.Unique(dbSourceBizIDs)
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.DbSourceBizID.In(dbSourceBizIDs...),
		tbl.IsDeleted.Is(false),
	}
	data, err := tbl.WithContext(ctx).Where(conds...).FindInBatch(200, func(tx gen.Dao, batch int) error { return nil })
	if err != nil {
		logx.E(ctx, "BatchGetByBizIDs|failed to query Database by bizIDs: bizIDs:%v, err:%v", dbSourceBizIDs, err)
		return nil, err
	}
	return databasesPO2DO(data), nil
}

// ListOnlyByAppBizID 仅根据appBizID进行分页查询
func (d *dao) ListOnlyByAppBizID(ctx context.Context, appBizID uint64, page, pageSize int) ([]*entity.Database, int64, error) {
	if appBizID == 0 {
		return nil, 0, errs.ErrParameterInvalid
	}
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.AppBizID.Eq(appBizID),
		tbl.IsDeleted.Is(false),
	}
	db := tbl.WithContext(ctx).Where(conds...)
	total, err := db.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:ListOnlyByAppBizID Count error: %w", err)
	}
	offset, limit := utilx.Page(page, pageSize)
	data, err := db.Order(tbl.ID.Desc()).Limit(limit).Offset(offset).Find()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:ListOnlyByAppBizID Find error: %w", err)
	}
	return databasesPO2DO(data), total, nil
}

// GetOnlyByBizID 通过业务ID获取
func (d *dao) GetOnlyByBizID(ctx context.Context, dbSourceBizID uint64) (*entity.Database, error) {
	if dbSourceBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	tbl := d.tdsql.TDbSource
	conds := []gen.Condition{
		tbl.DbSourceBizID.Eq(dbSourceBizID),
		tbl.IsDeleted.Is(false),
	}
	data, err := tbl.WithContext(ctx).Where(conds...).First()
	if err != nil {
		logx.E(ctx, "GetOnlyByBizID|failed to query Database by bizID: bizID:%v, err:%v", dbSourceBizID, err)
		return nil, err
	}
	return databasePO2DO(data), nil
}

func (d *dao) DescribeDatabaseList(ctx context.Context, filter *entity.DatabaseFilter) ([]*entity.Database, int64, error) {
	if filter == nil {
		return nil, 0, errs.ErrParameterInvalid
	}
	conds := dbFilterToConds(d.tdsql, filter)
	tbl := d.tdsql.TDbSource
	db := tbl.WithContext(ctx).Where(conds...)
	total, err := db.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeDatabaseList Count error: %w", err)
	}

	if filter.PageNumber != nil {
		offset, limit := utilx.Page(*filter.PageNumber, filter.PageSize)
		db = db.Offset(offset).Limit(limit)
	}
	data, err := db.Order(tbl.ID.Desc()).Find()
	if err != nil {
		return nil, 0, fmt.Errorf("dao:DescribeDatabaseList Find error: %w", err)
	}
	return databasesPO2DO(data), total, nil
}

func databaseDO2PO(do *entity.Database) *model.TDbSource {
	if do == nil {
		return nil
	}
	po := &model.TDbSource{
		ID:            do.ID,
		CorpBizID:     do.CorpBizID,
		AppBizID:      do.AppBizID,
		DbSourceBizID: do.DBSourceBizID,
		AliasName:     do.AliasName,
		DbType:        do.DBType,
		Host:          do.Host,
		Port:          do.Port,
		DbName:        do.DBName,
		Username:      do.Username,
		Password:      do.Password,
		CreateTime:    do.CreateTime,
		UpdateTime:    do.UpdateTime,
		IsDeleted:     do.IsDeleted,
		StaffID:       do.StaffID,
		Description:   do.Description,
		NextAction:    do.NextAction,
		ReleaseStatus: do.ReleaseStatus,
		Alive:         do.Alive,
		IsIndexed:     do.IsIndexed,
		LastSyncTime:  do.LastSyncTime,
		SchemaName:    do.SchemaName,
		EnableScope:   do.EnableScope,
	}
	return po
}

func databasesPO2DO(pos []*model.TDbSource) []*entity.Database {
	dos := make([]*entity.Database, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, databasePO2DO(po))
	}
	return dos
}

func databasePO2DO(po *model.TDbSource) *entity.Database {
	if po == nil {
		return nil
	}
	do := &entity.Database{
		ID:            po.ID,
		CorpBizID:     po.CorpBizID,
		AppBizID:      po.AppBizID,
		DBSourceBizID: po.DbSourceBizID,
		AliasName:     po.AliasName,
		DBType:        po.DbType,
		Host:          po.Host,
		Port:          po.Port,
		DBName:        po.DbName,
		Username:      po.Username,
		Password:      po.Password,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
		IsDeleted:     po.IsDeleted,
		StaffID:       po.StaffID,
		Description:   po.Description,
		LastSyncTime:  po.LastSyncTime,
		Alive:         po.Alive,
		IsIndexed:     po.IsIndexed,
		NextAction:    po.NextAction,
		ReleaseStatus: po.ReleaseStatus,
		SchemaName:    po.SchemaName,
		EnableScope:   po.EnableScope,
	}
	return do
}

func prodDatabasePO2DO(po *model.TDbSourceProd) *entity.DatabaseProd {
	if po == nil {
		return nil
	}
	do := &entity.DatabaseProd{
		ID:            po.ID,
		CorpBizID:     po.CorpBizID,
		AppBizID:      po.AppBizID,
		DBSourceBizID: po.DbSourceBizID,
		AliasName:     po.AliasName,
		DBType:        po.DbType,
		Host:          po.Host,
		Port:          po.Port,
		DBName:        po.DbName,
		Username:      po.Username,
		Password:      po.Password,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
		Description:   po.Description,
		LastSyncTime:  po.LastSyncTime,
		SchemaName:    po.SchemaName,
	}
	return do
}

func prodDatabasesDO2PO(pos []*model.TDbSourceProd) []*entity.DatabaseProd {
	dos := make([]*entity.DatabaseProd, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, prodDatabasePO2DO(po))
	}
	return dos
}

func databasesDO2PO(dos []*entity.Database) []*model.TDbSource {
	pos := make([]*model.TDbSource, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, databaseDO2PO(do))
	}
	return pos
}

func releaseDatabasesPO2DO(pos []*model.TReleaseDbSource) []*entity.DatabaseRelease {
	dos := make([]*entity.DatabaseRelease, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, releaseDatabasePO2DO(po))
	}
	return dos
}

func releaseDatabasePO2DO(po *model.TReleaseDbSource) *entity.DatabaseRelease {
	if po == nil {
		return nil
	}
	do := &entity.DatabaseRelease{
		ID:            po.ID,
		CorpBizID:     po.CorpBizID,
		AppBizID:      po.AppBizID,
		DBSourceBizID: po.DbSourceBizID,
		AliasName:     po.AliasName,
		DBType:        po.DbType,
		Host:          po.Host,
		Port:          po.Port,
		DBName:        po.DbName,
		Username:      po.Username,
		Password:      po.Password,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
		IsDeleted:     po.IsDeleted,
		Description:   po.Description,
		LastSyncTime:  po.LastSyncTime,
		Alive:         po.Alive,
		ReleaseStatus: po.ReleaseStatus,
		ReleaseBizID:  po.ReleaseBizID,
		SchemaName:    po.SchemaName,
	}
	return do
}

func releaseDatabasesDO2PO(dos []*entity.DatabaseRelease) []*model.TReleaseDbSource {
	pos := make([]*model.TReleaseDbSource, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, releaseDatabaseDO2PO(do))
	}
	return pos
}

func releaseDatabaseDO2PO(do *entity.DatabaseRelease) *model.TReleaseDbSource {
	if do == nil {
		return nil
	}
	po := &model.TReleaseDbSource{
		ID:            do.ID,
		CorpBizID:     do.CorpBizID,
		AppBizID:      do.AppBizID,
		DbSourceBizID: do.DBSourceBizID,
		AliasName:     do.AliasName,
		DbType:        do.DBType,
		Host:          do.Host,
		Port:          do.Port,
		DbName:        do.DBName,
		Username:      do.Username,
		Password:      do.Password,
		CreateTime:    do.CreateTime,
		UpdateTime:    do.UpdateTime,
		IsDeleted:     do.IsDeleted,
		Description:   do.Description,
		ReleaseStatus: do.ReleaseStatus,
		Alive:         do.Alive,
		LastSyncTime:  do.LastSyncTime,
		ReleaseBizID:  do.ReleaseBizID,
		SchemaName:    do.SchemaName,
	}
	return po
}

func releaseTablePO2DO(po *model.TReleaseDbTable) *entity.TableRelease {
	if po == nil {
		return nil
	}
	do := &entity.TableRelease{
		ID:                po.ID,
		CorpBizID:         po.CorpBizID,
		AppBizID:          po.AppBizID,
		DBSourceBizID:     po.DbSourceBizID,
		DBTableBizID:      po.DbTableBizID,
		Name:              po.TableName_,
		TableSchema:       po.TableSchema,
		TableComment:      po.TableComment,
		AliasName:         po.AliasName,
		Description:       po.Description,
		RowCount:          int64(po.RowCount),
		ColumnCount:       int(po.ColumnCount),
		TableModifiedTime: po.TableModifiedTime,

		LastSyncTime:  po.LastSyncTime,
		ReleaseStatus: po.ReleaseStatus,
		ReleaseBizID:  po.ReleaseBizID,
		Action:        po.Action,
		Alive:         po.Alive,
		IsIndexed:     po.IsIndexed,
		IsDeleted:     po.IsDeleted,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
	}
	return do
}

func releaseTablePO2DOs(pos []*model.TReleaseDbTable) []*entity.TableRelease {
	dos := make([]*entity.TableRelease, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, releaseTablePO2DO(po))
	}
	return dos
}

func releaseTableDO2PO(do *entity.TableRelease) *model.TReleaseDbTable {
	if do == nil {
		return nil
	}
	po := &model.TReleaseDbTable{
		ID:                do.ID,
		CorpBizID:         do.CorpBizID,
		AppBizID:          do.AppBizID,
		DbSourceBizID:     do.DBSourceBizID,
		DbTableBizID:      do.DBTableBizID,
		TableName_:        do.Name,
		TableSchema:       do.TableSchema,
		TableComment:      do.TableComment,
		AliasName:         do.AliasName,
		Description:       do.Description,
		RowCount:          uint64(do.RowCount),
		ColumnCount:       uint32(do.ColumnCount),
		TableModifiedTime: do.TableModifiedTime,

		LastSyncTime:  do.LastSyncTime,
		ReleaseStatus: do.ReleaseStatus,
		ReleaseBizID:  do.ReleaseBizID,
		Action:        do.Action,
		Alive:         do.Alive,
		IsIndexed:     do.IsIndexed,
		IsDeleted:     do.IsDeleted,
		CreateTime:    do.CreateTime,
		UpdateTime:    do.UpdateTime,
	}
	return po
}

func (d *dao) CountDBSourceWithTimeAndStatus(
	ctx context.Context,
	corpBizID, appBizID uint64,
	startTime time.Time) (uint64, error) {
	if corpBizID == 0 || appBizID == 0 {
		return 0, errs.ErrParameterInvalid
	}
	var count int64

	count, err := d.tdsql.TDbSource.WithContext(ctx).
		Distinct(d.tdsql.TDbSource.ID).
		Where(d.tdsql.TDbSource.CorpBizID.Eq(corpBizID)).
		Where(d.tdsql.TDbSource.AppBizID.Eq(appBizID)).
		Where(field.Or(
			field.And(d.tdsql.TDbSource.CreateTime.Gte(startTime), d.tdsql.TDbSource.IsDeleted.Is(false)),
			field.And(d.tdsql.TDbSource.UpdateTime.Gte(startTime), d.tdsql.TDbSource.CreateTime.Lt(startTime)))).Count()
	return uint64(count), err
}

func testConnection(ctx context.Context, host string, dbConn *sql.DB) error {
	type testResult struct {
		err error
	}
	resultCh := make(chan testResult, 1)

	go func() {
		var result int
		err := dbConn.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		resultCh <- testResult{err: err}
	}()

	// 等待测试完成或超时
	select {
	case <-ctx.Done():
		// 超时或取消，关闭连接
		dbConn.Close()

		return fmt.Errorf("test db %v timeout, error:%w", host, ctx.Err())
	case result := <-resultCh:
		if result.err != nil {
			dbConn.Close()
			return fmt.Errorf("test db %v, error:%w", host, result.err)
		}
		return nil
	}
}

func shouldUseProxy(ctx context.Context, connDbSource entity.DatabaseConn) bool {
	useProxy := false
	if config.App().DbSource.Proxy != "" {
		// 只要配置了代理，就默认走代理，除非是之前创建的mysql和sql server
		useProxy = true
		if connDbSource.CreateTime != nil && config.App().DbSource.ProxyEffectTime != "" {
			proxyTime, err := time.ParseInLocation("2006-01-02 15:04:05", config.App().DbSource.ProxyEffectTime, time.Local)
			if err != nil {
				logx.W(ctx, "parse ProxyEffectTime failed: %v, use direct connection", err)
			} else if connDbSource.CreateTime.Before(proxyTime) {
				useProxy = false
			}
		}
		logx.D(ctx, "db %v create time %v, use proxy %v", connDbSource.Host, connDbSource.CreateTime, useProxy)
	}
	return useProxy
}

// validateDSNSecurity 检查DSN中是否包含不安全的参数
func validateDSNSecurity(ctx context.Context, dsn string) error {
	// 从配置中获取不安全的DSN参数列表
	unsafeParams := config.App().DbSource.UnsafeDSNParams
	unsafeParams = append(unsafeParams, "allowAllFiles", "allowLoadLocalInfile")

	// 将DSN转换为小写进行检查（不区分大小写）
	dsnLower := strings.ToLower(dsn)

	// 检查每个不安全的参数
	for _, param := range unsafeParams {
		if strings.Contains(dsnLower, strings.ToLower(param)) {
			logx.E(ctx, "检测到不安全的DSN参数: %s", param)
			return errs.ErrWrapf(errs.ErrOpenDbSourceFail, "DSN包含不安全的参数: %s，已被系统拒绝", param)
		}
	}

	logx.D(ctx, "DSN安全检查通过")
	return nil
}
