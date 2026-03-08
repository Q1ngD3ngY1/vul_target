package kb

import (
	"context"
	"fmt"
	"strings"

	"git.woa.com/adp/common/x/logx"
	async "git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
)

const (
	dbFieldCorpID     = "corp_id"      // 企业ID
	dbFieldRobotID    = "robot_id"     // 应用ID
	dbFieldCorpBizID  = "corp_biz_id"  // 企业BizID
	dbFieldRobotBizID = "robot_biz_id" // 应用BizID
	dbFieldAppBizID   = "app_biz_id"   // 应用BizID
)

// CreateKnowledgeDeleteTask 创建知识删除任务
func (d *daoImpl) CreateKnowledgeDeleteTask(ctx context.Context, params entity.KnowledgeDeleteParams) error {
	logx.I(ctx, "CreateKnowledgeDeleteTask params:%+v", params)
	return async.NewKnowledgeDeleteTask(ctx, params)
}

// DeleteByCustomFieldID 删除指定表自定义字段列表
func (d *daoImpl) DeleteByCustomFieldID(ctx context.Context, tableName string, limit int64,
	customFields []string, customConditions []string, customFieldValues []any) (int64, error) {
	logx.I(ctx, "DeleteByCustomFieldID tableName:%s, limit:%d, "+
		"customFields:%+v, customConditions:%+v, len(customFieldValues):%d", tableName, limit,
		customFields, customConditions, len(customFieldValues))
	q := d.kbdel.Table(tableName)
	deletedCount := int64(0)
	if len(customFields) > 0 && len(customFields) == len(customConditions) &&
		len(customFields) == len(customFieldValues) {
		conditions := make([]string, 0)
		for i, filed := range customFields {
			conditions = append(conditions, fmt.Sprintf("%s %s ?", filed, customConditions[i]))
		}
		deleteCon := strings.Join(conditions, " AND ")
		deleteSql := fmt.Sprintf("DELETE from %s WHERE %s LIMIT %d", tableName, deleteCon, limit)
		if limit <= 0 { // 不做限制
			deleteSql = fmt.Sprintf("DELETE from %s WHERE %s", tableName, deleteCon)
		}
		db := q.Exec(deleteSql, customFieldValues...)
		if db.Error != nil {
			logx.E(ctx, "DeleteByCustomFieldID gormDB.Deleted Failed, err:%+v", db.Error)
			return 0, db.Error
		}
		deletedCount = db.RowsAffected
	}
	logx.I(ctx, "DeleteByCustomFieldID tableName:%s, deletedCount:%d", tableName, deletedCount)
	return deletedCount, nil
}

// CountTableNeedDeletedData 统计表需要删除数据的数量
func (d *daoImpl) CountTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	db := d.kbdel.Table(tableName) // 这里先随便指定一个model,后续以q.UnderlyingDB().Table(tableName) 为准
	var count int64
	if corpID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpID, dbFieldRobotID)
		db = db.Where(query, corpID, robotID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldRobotID)
		db = db.Where(query, robotID)
	}
	err := db.Count(&count).Error
	if err != nil {
		logx.E(ctx, "CountTableNeedDeletedData gormDB.Count Failed, err:%+v", err)
		return 0, err
	}
	logx.I(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s, count:%d",
		corpID, robotID, tableName, count)
	return count, nil
}

// CountTableNeedDeletedDataBizID 统计表需要删除数据的数量
func (d *daoImpl) CountTableNeedDeletedDataBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpBizID, robotBizID, tableName)
	var count int64
	db := d.tdsql.TKnowledgeSchema.WithContext(ctx).UnderlyingDB().Table(tableName) // 这里先随便指定一个model,后续以q.UnderlyingDB().Table(tableName) 为准
	if corpBizID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpBizID, dbFieldRobotBizID)
		db = db.Where(query, corpBizID, robotBizID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldRobotBizID)
		db = db.Where(query, robotBizID)
	}
	err := db.Count(&count).Error
	if err != nil {
		logx.E(ctx, "CountTableNeedDeletedData gormDB.Count Failed, err:%+v", err)
		return 0, err
	}
	logx.I(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s, count:%d",
		corpBizID, robotBizID, tableName, count)
	return count, nil
}

// CountTableNeedDeletedDataByCorpAndAppBizID 统计表需要删除数据的数量
func (d *daoImpl) CountTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string) (int64, error) {
	logx.I(ctx, "CountTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s",
		corpBizID, robotBizID, tableName)
	var count int64
	db := d.tdsql.TKnowledgeSchema.WithContext(ctx).UnderlyingDB().Table(tableName) // 这里先随便指定一个model,后续以q.UnderlyingDB().Table(tableName) 为准
	if corpBizID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpBizID, dbFieldAppBizID)
		db = db.Where(query, corpBizID, robotBizID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldAppBizID)
		db = db.Where(query, robotBizID)
	}
	err := db.Count(&count).Error
	if err != nil {
		logx.E(ctx, "CountTableNeedDeletedDataByCorpAndAppBizID gormDB.Count Failed, err:%+v", err)
		return 0, err
	}
	logx.I(ctx, "CountTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s, count:%d",
		corpBizID, robotBizID, tableName, count)
	return count, nil
}

// DeleteTableNeedDeletedData 删除表需要删除的数据
func (d *daoImpl) DeleteTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "DeleteTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpID, robotID, tableName, totalCount)
	count, err := d.CountTableNeedDeletedData(ctx, corpID, robotID, tableName)
	if err != nil {
		return err
	}
	if count != totalCount {
		err = fmt.Errorf("count not equal totalCount:%d != %d", count, totalCount)
		logx.E(ctx, "DeleteTableNeedDeletedData Failed, err:%+v", err)
		return err
	}
	deletedCount := int64(0)
	for count > 0 {
		db := d.kbdel.Table(tableName) // 这里先随便指定一个model,后续以q.UnderlyingDB().Table(tableName) 为准
		if corpID > 0 {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? AND %s = ? LIMIT %d", tableName,
				dbFieldCorpID, dbFieldRobotID, config.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, corpID, robotID)
		} else {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? LIMIT %d", tableName,
				dbFieldRobotID, config.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, robotID)
		}
		if db.Error != nil {
			logx.E(ctx, "DeleteTableNeedDeletedData gormDB.Deleted Failed, err:%+v", db.Error)
			return db.Error
		}
		deletedCount += db.RowsAffected
		logx.I(ctx, "DeleteTableNeedDeletedData deletedCount:%d", deletedCount)
		count, err = d.CountTableNeedDeletedData(ctx, corpID, robotID, tableName)
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		logx.E(ctx, "DeleteTableNeedDeletedData Failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "DeleteTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s success",
		corpID, robotID, tableName)
	return nil
}

// DeleteTableNeedDeletedDataBizID 删除表需要删除的数据
func (d *daoImpl) DeleteTableNeedDeletedDataBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "DeleteTableNeedDeletedDataBizID corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpBizID, robotBizID, tableName, totalCount)
	count, err := d.CountTableNeedDeletedDataBizID(ctx, corpBizID, robotBizID, tableName)
	if err != nil {
		return err
	}
	if count != totalCount {
		err = fmt.Errorf("count not equal totalCount:%d != %d", count, totalCount)
		logx.E(ctx, "DeleteTableNeedDeletedDataBizID Failed, err:%+v", err)
		return err
	}
	deletedCount := int64(0)
	for count > 0 {
		db := d.tdsql.TKnowledgeSchema.WithContext(ctx).UnderlyingDB().Table(tableName) // 这里先随便指定一个model,后续以q.UnderlyingDB().Table(tableName) 为准
		if corpBizID > 0 {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? AND %s = ? LIMIT %d", tableName,
				dbFieldCorpBizID, dbFieldRobotBizID, config.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, corpBizID, robotBizID)
		} else {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? LIMIT %d", tableName,
				dbFieldRobotBizID, config.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, robotBizID)
		}
		if db.Error != nil {
			logx.E(ctx, "DeleteTableNeedDeletedDataBizID gormDB.Deleted Failed, err:%+v", db.Error)
			return db.Error
		}
		deletedCount += db.RowsAffected
		logx.I(ctx, "DeleteTableNeedDeletedDataBizID deletedCount:%d", deletedCount)
		count, err = d.CountTableNeedDeletedDataBizID(ctx, corpBizID, robotBizID, tableName)
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		logx.E(ctx, "DeleteTableNeedDeletedDataBizID Failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "DeleteTableNeedDeletedDataBizID corpID:%d, robotID:%d, tableName:%s success",
		corpBizID, robotBizID, tableName)
	return nil
}

// DeleteTableNeedDeletedDataByCorpAndAppBizID 删除表需要删除的数据
func (d *daoImpl) DeleteTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string, totalCount int64) error {
	logx.I(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpBizID, robotBizID, tableName, totalCount)
	count, err := d.CountTableNeedDeletedDataByCorpAndAppBizID(ctx, corpBizID, robotBizID, tableName)
	if err != nil {
		return err
	}
	if count != totalCount {
		err = fmt.Errorf("count not equal totalCount:%d != %d", count, totalCount)
		logx.E(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID Failed, err:%+v", err)
		return err
	}
	deletedCount := int64(0)
	for count > 0 {
		db := d.tdsql.TKnowledgeSchema.WithContext(ctx).UnderlyingDB().Table(tableName) // 这里先随便指定一个model,后续以q.UnderlyingDB().Table(tableName) 为准
		if corpBizID > 0 {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? AND %s = ? LIMIT %d", tableName,
				dbFieldCorpBizID, dbFieldAppBizID, config.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, corpBizID, robotBizID)
		} else {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? LIMIT %d", tableName,
				dbFieldAppBizID, config.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, robotBizID)
		}
		if db.Error != nil {
			logx.E(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID gormDB.Deleted Failed, err:%+v", db.Error)
			return db.Error
		}
		deletedCount += db.RowsAffected
		logx.I(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID deletedCount:%d", deletedCount)
		count, err = d.CountTableNeedDeletedDataByCorpAndAppBizID(ctx, corpBizID, robotBizID, tableName)
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		logx.E(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID Failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s success",
		corpBizID, robotBizID, tableName)
	return nil
}

// GetCustomFieldIDList 获取指定表自定义主键ID列表
func (d *daoImpl) GetCustomFieldIDList(ctx context.Context, corpID, robotID uint64,
	tableName, customField string) ([]uint64, error) {
	logx.I(ctx, "GetCustomFieldIDList corpID:%d, robotID:%d, tableName:%s, customField:%s",
		corpID, robotID, tableName, customField)
	ids := make([]uint64, 0)
	db := d.kbdel.Table(tableName) // 这里先随便指定一个model,后续以q.UnderlyingDB().Table(tableName) 为准
	db = db.Select(customField)
	if corpID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpID, dbFieldRobotID)
		db = db.Where(query, corpID, robotID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldRobotID)
		db = db.Where(query, robotID)
	}
	err := db.Limit(config.GetMainConfig().KnowledgeDeleteConfig.QueryBatchSize).
		Find(&ids).Error
	if err != nil {
		logx.E(ctx, "GetCustomFieldIDList gormDB.Find Failed, err:%+v", err)
		return nil, err
	}
	logx.I(ctx, "GetCustomFieldIDList corpID:%d, robotID:%d, tableName:%s, customField:%s, "+
		"len(ids):%d", corpID, robotID, tableName, customField, len(ids))
	return ids, nil
}
