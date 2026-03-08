package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var dbTableDao *DBTableDao

func GetDBTableDao() *DBTableDao {
	if dbTableDao == nil {
		dbTableDao = &DBTableDao{db: globalBaseDao.tdsqlGormDB}
	}
	return dbTableDao
}

type DBTableDao struct {
	db *gorm.DB
}

const (
	sqlDbTableName        = "table_name"
	sqlDbTableLearnStatus = "learn_status"
)

// BatchCreate 批量新增表
func (r *DBTableDao) BatchCreate(ctx context.Context, tables []*model.DBTable) error {
	if len(tables) == 0 {
		log.WarnContextf(ctx, "BatchCreate failed: tables is empty")
		return nil
	}
	if err := r.db.WithContext(ctx).Create(tables).Error; err != nil {
		log.ErrorContextf(ctx, "BatchCreate failed %v", err)
		return errs.ErrAddDbSourceFail
	}
	return nil
}

// GetByBizID 通过业务ID获取
func (r *DBTableDao) GetByBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) (*model.DBTable, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var table model.DBTable
	err := r.db.WithContext(ctx).Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 0",
		corpBizID, appBizID, dbTableBizID).First(&table).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetByBizID failed %v", err)
		return nil, err
	}
	return &table, nil
}

// GetByBizIDAndTableName 通过业务ID和表名获取
func (r *DBTableDao) GetByBizIDAndTableName(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64, tableName string) (*model.DBTable, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var table model.DBTable
	err := r.db.WithContext(ctx).Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND table_name = ? AND is_deleted = 0",
		corpBizID, appBizID, dbSourceBizID, tableName).First(&table).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetByBizIDAndTableName|appBizID: %v, dbSourceBizID: %v, tableName %v failed %v", appBizID, dbSourceBizID, tableName, err)
		return nil, err
	}
	return &table, nil
}

// GetByBizIDs 通过业务IDs获取
func (r *DBTableDao) GetByBizIDs(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*model.DBTable, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var table []*model.DBTable
	err := r.db.WithContext(ctx).Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id in ? AND is_deleted = 0",
		corpBizID, appBizID, dbTableBizIDs).Find(&table).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetByBizIDs failed %v", err)
		return nil, err
	}
	return table, nil
}

// Text2sqlGetByDbSourceBizID  通过 sheet Biz ID 获取对应的数据表
func (r *DBTableDao) Text2sqlGetByDbSourceBizID(ctx context.Context, corpBizID, appBizID, docSheetBizId uint64) (*model.DBTable, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var table model.DBTable
	err := r.db.WithContext(ctx).Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, docSheetBizId).First(&table).Error
	if err != nil {
		log.ErrorContextf(ctx, "Text2sqlGetByDbSourceBizID failed %v", err)
		return nil, err
	}
	return &table, nil
}

// Text2sqlExistsByDbSourceBizID  通过 sheet Biz ID 判断是否存在
func (r *DBTableDao) Text2sqlExistsByDbSourceBizID(ctx context.Context, corpBizID, appBizID, docSheetBizId uint64) (bool, error) {
	if corpBizID == 0 || appBizID == 0 {
		return false, errs.ErrParameterInvalid
	}
	var count int64
	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, docSheetBizId).
		Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "ExistsByDbSourceBizID failed %v", err)
		return false, err
	}
	return count > 0, nil
}

type ListDBTablesOption struct {
	CorpBizID     uint64
	AppBizID      uint64
	DBSourceBizID uint64
	TableName     string // 可选参数，为空时不按表名过滤
	IsEnable      *bool  // 为nil的时候不生效
	Page          int    // 必填，从1开始
	PageSize      int    // 必填
}

// ListByOption 统一的表查询函数
func (r *DBTableDao) ListByOption(ctx context.Context, opt *ListDBTablesOption) ([]*model.DBTable, int64, error) {
	// 参数校验
	if opt.CorpBizID == 0 || opt.AppBizID == 0 {
		return nil, 0, errs.ErrParameterInvalid
	}

	var (
		tables []*model.DBTable
		total  int64
	)

	// 构建基础查询条件
	db := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0",
			opt.CorpBizID, opt.AppBizID, opt.DBSourceBizID)

	if opt.TableName != "" {
		db = db.Where("table_name LIKE ?", "%"+opt.TableName+"%")
	}
	if opt.IsEnable != nil {
		indexed := 0
		if *opt.IsEnable {
			indexed = 1
		}
		db = db.Where("is_indexed = ?", indexed)
	}

	// 计算总数
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 分页查询
	offset := (opt.Page - 1) * opt.PageSize
	err := db.Order("id DESC").Offset(offset).Limit(opt.PageSize).Find(&tables).Error
	if err != nil {
		log.ErrorContext(ctx, "ListByOption failed", err)
		return nil, 0, err
	}

	return tables, total, nil
}

// ListAllByDBSourceBizID 获取数据源下的所有表单
func (r *DBTableDao) ListAllByDBSourceBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64) ([]*model.DBTable, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var tables []*model.DBTable
	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, dbSourceBizID).
		Order("id DESC").
		Find(&tables).Error
	if err != nil {
		log.ErrorContext(ctx, "ListAllByDBSourceBizID failed", err)
		return nil, errs.ErrDataBase
	}
	return tables, nil
}

func (r *DBTableDao) ListAllTableNameByDBSourceBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64) ([]string, error) {
	if corpBizID == 0 || appBizID == 0 {
		return nil, errs.ErrParameterInvalid
	}
	var tables []string
	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, dbSourceBizID).
		Order("id DESC").
		Pluck("table_name", &tables).Error
	if err != nil {
		log.ErrorContext(ctx, "ListAllTableNameByDBSourceBizID failed", err)
		return nil, err
	}
	return tables, nil
}

// UpdateByBizID 通过业务ID更新（部分字段）
func (r *DBTableDao) UpdateByBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64,
	updateColumns []string, table *model.DBTable) error {
	if corpBizID == 0 || appBizID == 0 {
		return errs.ErrParameterInvalid
	}
	res := r.db.WithContext(ctx).Model(&model.DBTable{}).Select(updateColumns).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 0",
			corpBizID, appBizID, dbTableBizID).
		Updates(table)
	if res.Error != nil {
		log.ErrorContext(ctx, "UpdateByBizID failed", res.Error)
		return res.Error
	}
	return nil
}

// BatchUpsertByBizID 批量插入或更新（通过业务ID），使用 OnConflict + Create 方式
func (r *DBTableDao) BatchUpsertByBizID(ctx context.Context, cols []string, tables []*model.DBTable) error {
	if len(tables) == 0 {
		log.WarnContextf(ctx, "BatchUpsertByBizID failed: tables is empty")
		return nil
	}

	batchSize := 200
	for i := 0; i < len(tables); i += batchSize {
		end := i + batchSize
		if end > len(tables) {
			end = len(tables)
		}
		batch := tables[i:end]
		err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: DBCorpBizID},
				{Name: DBAppBizID},
				{Name: DBTableBizID},
			},
			DoUpdates: clause.AssignmentColumns(cols),
		}).Create(batch).Error

		if err != nil {
			log.ErrorContextf(ctx, "BatchUpsertByBizID failed %v", err)
			return err
		}
	}
	return nil
}

// SoftDeleteByBizID 软删除
func (r *DBTableDao) SoftDeleteByBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) error {
	if corpBizID == 0 || appBizID == 0 {
		return errs.ErrParameterInvalid
	}
	res := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 0",
			corpBizID, appBizID, dbTableBizID).
		Updates(map[string]interface{}{
			"is_deleted":  1,
			"next_action": model.ReleaseActionDelete,
			"release_status": gorm.Expr(`CASE WHEN next_action != ? THEN ? ELSE release_status END`,
				model.ReleaseActionAdd, model.ReleaseStatusUnreleased),
		})
	if res.Error != nil {
		log.ErrorContext(ctx, "SoftDeleteByBizID failed", res.Error)
		return res.Error
	}
	return nil
}

// BatchSoftDeleteByDBSourceBizID 批量软删除
func (r *DBTableDao) BatchSoftDeleteByDBSourceBizID(ctx context.Context, corpBizID, appBizID uint64, dbSourceBizIDs []uint64) error {
	if corpBizID == 0 || appBizID == 0 || len(dbSourceBizIDs) == 0 {
		return errs.ErrParameterInvalid
	}
	res := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_source_biz_id IN ? AND is_deleted = 0", corpBizID, appBizID, dbSourceBizIDs).
		Updates(map[string]interface{}{
			"is_deleted": 1,
		})
	if res.Error != nil {
		log.ErrorContext(ctx, "BatchSoftDeleteByDBSourceBizID failed", res.Error)
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errs.ErrDataNotExistOrIsDeleted
	}
	return nil
}

func (r *DBTableDao) CollectUnreleasedDBTable(ctx context.Context, appBizID, releaseBizID uint64) error {
	// 1. 找出所有 release_status 为1 待发布的 t_db_table
	dbTables, err := r.GetUnreleasedDBTable(ctx, appBizID)
	if err != nil {
		return err
	}
	if len(dbTables) == 0 {
		return nil
	}

	corpBizID := dbTables[0].CorpBizID
	// 2. 补充releaseBizID，并写入到t_release_db_source
	for _, chunk := range slicex.Chunk(dbTables, 200) {
		var releaseChunk []*model.ReleaseDBTable
		var ids []uint64
		for _, d := range chunk {
			ids = append(ids, d.DBTableBizID)
			releaseDBSource := &model.ReleaseDBTable{
				CorpBizID:         d.CorpBizID,
				AppBizID:          d.AppBizID,
				DBSourceBizID:     d.DBSourceBizID,
				DBTableBizID:      d.DBTableBizID,
				Source:            d.Source,
				Name:              d.Name,
				TableSchema:       d.TableSchema,
				TableComment:      d.TableComment,
				AliasName:         d.AliasName,
				Description:       d.Description,
				RowCount:          d.RowCount,
				ColumnCount:       d.ColumnCount,
				TableAddedTime:    d.TableAddedTime,
				TableModifiedTime: d.TableModifiedTime,
				LastSyncTime:      d.LastSyncTime,
				ReleaseStatus:     d.ReleaseStatus,
				ReleaseBizID:      releaseBizID,
				Action:            d.NextAction,
				Alive:             d.Alive,
				IsDeleted:         d.IsDeleted,
				IsIndexed:         d.IsIndexed,
			}
			releaseChunk = append(releaseChunk, releaseDBSource)
		}
		err = r.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "corp_biz_id"},
				{Name: "app_biz_id"},
				{Name: "db_table_biz_id"},
				{Name: "release_biz_id"},
			},
			DoUpdates: clause.Assignments(map[string]interface{}{
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
				"is_deleted":          gorm.Expr("VALUES(is_deleted)"),
			}),
		}).Create(releaseChunk).Error
		if err != nil {
			log.ErrorContextf(ctx, "create release table error, %v", err)
			return err
		}

		// 3. 更新db_table表的状态为待发布
		err = r.db.WithContext(ctx).Model(&model.DBTable{}).
			Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id IN (?)",
				corpBizID, appBizID, ids).
			Updates(map[string]interface{}{
				"release_status": model.ReleaseStatusReleasing,
			}).Error
		if err != nil {
			log.ErrorContextf(ctx, "update db table releasing error, id: %+v, %v", ids, err)
			return err
		}
	}

	return nil
}

// GetUnreleasedDBTable 获取待发布的数据表信息
func (r *DBTableDao) GetUnreleasedDBTable(ctx context.Context, appBizID uint64) ([]*model.DBTable, error) {
	limit := 200
	startID := uint64(0)
	var all []*model.DBTable
	for {
		var chunk []*model.DBTable
		// 排除是新增，但是未发布的情况下又删除的数据库
		err := r.db.WithContext(ctx).Where("app_biz_id = ? AND release_status = 1 AND "+
			"!(next_action = 1 AND is_deleted = 1) AND id > ? AND (is_deleted = 1 OR learn_status = ?)", appBizID, startID, model.LearnStatusLearned).
			Order("id ASC").Limit(limit).Find(&chunk).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetUnreleasedDBTable error, %v", err)
			return nil, err
		}
		all = append(all, chunk...)
		if len(chunk) < limit {
			break
		}
		startID = chunk[len(chunk)-1].ID
	}
	log.InfoContextf(ctx, "GetUnreleasedDBTable len: %v", len(all))
	return all, nil
}

// FindUnReleaseDBTableByConditions 根据 DBName、UpdateTime 和 NextAction 进行多条件检索
func (r *DBTableDao) FindUnReleaseDBTableByConditions(ctx context.Context, corpBizID, appBizID uint64, dbTableName string, beginTime,
	endTime time.Time, nextAction []uint32, page, pageSize uint32) ([]*model.DBTable, error) {
	var sources []*model.DBTable
	query := r.db.Model(&model.DBTable{})
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
	query = query.Where(sqlDbSourceReleaseStatus+sqlEqual, model.ReleaseStatusUnreleased)
	query = query.Where(sqlDbTableLearnStatus+sqlEqual, model.LearnStatusLearned)
	query = query.Where(sqlAddAndDelWithoutPublish)
	query = query.Limit(int(pageSize)).Offset(int(pageSize * (page - 1)))
	query = query.Order(sqlDbSourceId + SqlOrderByDesc)

	if err := query.Find(&sources).Error; err != nil {
		log.ErrorContextf(ctx, "FindUnReleaseDBTableByConditions|appBizID:%v, dbName:%v, beginTime:%v, endTime:%v, nextAction:%v, page:%v, pageSize:%v",
			appBizID, dbTableName, beginTime, endTime, nextAction, page, pageSize)
		return nil, err
	}
	return sources, nil
}

// GetAllReleaseDBTables 获取单次发布的快照表 t_release_db_table 集合
func (r *DBTableDao) GetAllReleaseDBTables(ctx context.Context, appBizID, releaseBizID uint64,
	onlyBizID bool) ([]*model.ReleaseDBTable, error) {
	limit := 200
	startID := uint64(0)
	var all []*model.ReleaseDBTable
	for {
		var chunk []*model.ReleaseDBTable
		session := r.db.WithContext(ctx)
		if onlyBizID {
			session = session.Select([]string{"id", "db_table_biz_id"})
		}
		// 排除是新增，但是未发布的情况下又删除的数据库
		err := session.Where("app_biz_id = ? AND release_biz_id = ? AND id > ?", appBizID, releaseBizID, startID).
			Order("id ASC").Limit(limit).Find(&chunk).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetUnreleasedDBTable error, %v", err)
			return nil, err
		}
		all = append(all, chunk...)
		if len(chunk) < limit {
			break
		}
		startID = chunk[len(chunk)-1].ID
	}
	log.InfoContextf(ctx, "GetAllReleaseDBTables len: %v", len(all))
	return all, nil
}

func (r *DBTableDao) ReleaseDBTableToProd(ctx context.Context, releaseDBTable model.ReleaseDBTable) error {
	tableProd := &model.ProdDBTable{
		CorpBizID:         releaseDBTable.CorpBizID,
		AppBizID:          releaseDBTable.AppBizID,
		DBSourceBizID:     releaseDBTable.DBSourceBizID,
		DBTableBizID:      releaseDBTable.DBTableBizID,
		Source:            releaseDBTable.Source,
		Name:              releaseDBTable.Name,
		TableSchema:       releaseDBTable.TableSchema,
		TableComment:      releaseDBTable.TableComment,
		AliasName:         releaseDBTable.AliasName,
		Description:       releaseDBTable.Description,
		RowCount:          releaseDBTable.RowCount,
		ColumnCount:       releaseDBTable.ColumnCount,
		TableAddedTime:    releaseDBTable.TableAddedTime,
		TableModifiedTime: releaseDBTable.TableModifiedTime,
		Alive:             releaseDBTable.Alive,
		IsDeleted:         releaseDBTable.IsDeleted,
		ReleaseStatus:     releaseDBTable.ReleaseStatus,
		LastSyncTime:      releaseDBTable.LastSyncTime,
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "corp_biz_id"},
			{Name: "app_biz_id"},
			{Name: "db_table_biz_id"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
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
		}),
	}).Create(tableProd).Error
	if err != nil {
		log.ErrorContextf(ctx, "insert release db table prod %v error, %v", releaseDBTable.DBSourceBizID, err)
		return err
	}

	// 修改评测端的表发布状态
	err = r.db.WithContext(ctx).Model(&model.DBTable{}).Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ?",
		releaseDBTable.CorpBizID, releaseDBTable.AppBizID, releaseDBTable.DBTableBizID).Updates(map[string]interface{}{
		"release_status": model.ReleaseStatusReleased,
		"next_action":    model.ReleaseActionPublish,
	}).Error
	if err != nil {
		log.ErrorContextf(ctx, "update release status error, %v", err)
		return err
	}

	return nil
}

func (r *DBTableDao) GetReleaseDBTable(ctx context.Context, appBizID uint64, releaseBizID uint64,
	dbTableBizID uint64) (model.ReleaseDBTable, error) {
	var releaseDBTable model.ReleaseDBTable
	err := r.db.WithContext(ctx).Where("app_biz_id = ? AND release_biz_id = ? AND db_table_biz_id = ?",
		appBizID, releaseBizID, dbTableBizID).First(&releaseDBTable).Error
	if err != nil {
		log.ErrorContextf(ctx, "get release db table error, table: %v, release: %v, error %v",
			dbTableBizID, releaseBizID, err)
		return model.ReleaseDBTable{}, err
	}
	return releaseDBTable, nil
}

// GetRoleLabelsOfDBTable 获取db table的所有标签
func GetRoleLabelsOfDBTable(ctx context.Context, appBizID, dbTableBizID uint64) ([]*retrieval.VectorLabel, error) {
	//1.根据数据库业务id获取被引用的角色业务id
	roleBizIDS, err := GetRoleDao(nil).GetRoleByDbBiz(ctx, appBizID, dbTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "getAllLabelsOfDBTable err:%v,dbTableBizID:%+v", err, dbTableBizID)
		return nil, err
	}
	vectorLabels := make([]*retrieval.VectorLabel, 0, len(roleBizIDS))
	for _, v := range roleBizIDS {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  utilConfig.GetMainConfig().Permissions.RoleRetrievalKey, //角色向量统一key
			Value: cast.ToString(v),
		})
	}
	return vectorLabels, nil
}

// GetCountByAppBizIDs 获取应用下所有表的数量
func (r *DBTableDao) GetCountByAppBizIDs(ctx context.Context, appBizIDs []uint64) (int64, error) {
	if len(appBizIDs) == 0 {
		return 0, errs.ErrParameterInvalid
	}
	count := int64(0)
	err := r.db.WithContext(ctx).Model(&model.DBTable{}).Where("app_biz_id in ?  AND is_deleted = 0",
		appBizIDs).Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetByBizIDs failed %v", err)
		return 0, err
	}
	return count, nil
}

// GetCountByDbSourceBizID 获取数据库下所有表的数量
func (r *DBTableDao) GetCountByDbSourceBizID(ctx context.Context, corpBizID, dbSourceBizID uint64) (int64, error) {
	if dbSourceBizID == 0 {
		return 0, errs.ErrParameterInvalid
	}
	count := int64(0)
	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where(DBCorpBizID+sqlEqual, corpBizID).
		Where(DBSourceBizID+sqlEqual, dbSourceBizID).
		Where(DBIsDeleted+sqlEqual, IsNotDeleted).
		Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetCountByDbSourceBizID failed: corpBizID:%v, dbSourceBizID:%v, err:%v", corpBizID, dbSourceBizID, err)
		return 0, err
	}
	return count, nil
}

// GetCountByDbSourceBizIDs 批量获取数据库下所有表的数量
func (r *DBTableDao) GetCountByDbSourceBizIDs(ctx context.Context, corpBizID uint64, dbSourceBizIDs []uint64) (map[uint64]int32, error) {
	if len(dbSourceBizIDs) == 0 {
		return nil, errs.ErrParameterInvalid
	}

	var results []struct {
		DBSourceBizID uint64
		Count         int64
	}

	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Select("db_source_biz_id, COUNT(*) as count").
		Where(DBCorpBizID+sqlEqual, corpBizID).
		Where(DBSourceBizID+sqlIn, dbSourceBizIDs).
		Where(DBIsDeleted+sqlEqual, IsNotDeleted).
		Group(DBSourceBizID).
		Find(&results).Error

	if err != nil {
		log.ErrorContextf(ctx, "GetCountByDbSourceBizIDs failed: corpBizID:%v, dbSourceBizIDs:%v, err:%v", corpBizID, dbSourceBizIDs, err)
		return nil, err
	}

	countMap := make(map[uint64]int32)
	for _, result := range results {
		countMap[result.DBSourceBizID] = int32(result.Count)
	}
	return countMap, nil
}

// BatchGetByBizIDs 批量获取表
func (r *DBTableDao) BatchGetByBizIDs(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*model.DBTable, error) {
	var results []*model.DBTable
	batchSize := 200 // 每批获取的记录数
	for i := 0; i < len(dbTableBizIDs); i += batchSize {
		end := i + batchSize
		if end > len(dbTableBizIDs) {
			end = len(dbTableBizIDs)
		}
		batch := dbTableBizIDs[i:end]
		var batchResults []*model.DBTable
		err := r.db.WithContext(ctx).Model(&model.DBTable{}).
			Where(DBCorpBizID+sqlEqual, corpBizID).
			Where(DBAppBizID+sqlEqual, appBizID).
			Where(DBTableBizID+sqlIn, batch).
			Where(DBIsDeleted+sqlEqual, IsNotDeleted).
			Find(&batchResults).Error
		if err != nil {
			log.ErrorContextf(ctx, "BatchGetByBizIDs|failed:batchID:%v, err:%v", batch, err)
			return nil, err
		}
		results = append(results, batchResults...)
	}
	return results, nil
}

// BatchGetByBizIDsForProd 批量获取表
func (r *DBTableDao) BatchGetByBizIDsForProd(ctx context.Context, corpBizID, appBizID uint64, dbTableBizIDs []uint64) ([]*model.ProdDBTable, error) {
	var results []*model.ProdDBTable
	batchSize := 200 // 每批获取的记录数
	for i := 0; i < len(dbTableBizIDs); i += batchSize {
		end := i + batchSize
		if end > len(dbTableBizIDs) {
			end = len(dbTableBizIDs)
		}
		batch := dbTableBizIDs[i:end]
		var batchResults []*model.ProdDBTable
		err := r.db.WithContext(ctx).Model(&model.ProdDBTable{}).
			Where(DBCorpBizID+sqlEqual, corpBizID).
			Where(DBAppBizID+sqlEqual, appBizID).
			Where(DBTableBizID+sqlIn, batch).
			Where(DBIsDeleted+sqlEqual, IsNotDeleted).
			Find(&batchResults).Error
		if err != nil {
			log.ErrorContextf(ctx, "BatchGetByBizIDs|failed:batchID:%v, err:%v", batch, err)
			return nil, err
		}
		results = append(results, batchResults...)
	}
	return results, nil
}

func (r *DBTableDao) BatchGetTableName(ctx context.Context, corpBizID, appBizID uint64) ([]string, error) {
	res := make([]string, 0, 10)

	temp := make([]*model.DBTable, 0, 10)
	dup := make(map[string]bool, 10)
	err := r.db.WithContext(ctx).Model(&model.DBTable{}).
		Where(DBCorpBizID+sqlEqual, corpBizID).
		Where(DBAppBizID+sqlEqual, appBizID).
		Where(DBIsDeleted+sqlEqual, IsNotDeleted).
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
		log.ErrorContextf(ctx, "BatchGetTableName|failed:err:%v", err)
		return nil, err
	}
	return res, nil
}
