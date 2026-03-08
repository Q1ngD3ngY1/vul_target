package dao

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"strings"
)

const (
	dbFieldCorpID     = "corp_id"      // 企业ID
	dbFieldRobotID    = "robot_id"     // 应用ID
	dbFieldCorpBizID  = "corp_biz_id"  // 企业BizID
	dbFieldRobotBizID = "robot_biz_id" // 应用BizID
	dbFieldAppBizID   = "app_biz_id"   // 应用BizID
)

// GetAppListByBizIDs 获取应用列表
func (d *dao) GetAppListByBizIDs(ctx context.Context, scenes uint32, robotIDs []uint64) (
	map[uint64]*admin.GetAppListRsp_AppInfo, error) {
	log.InfoContextf(ctx, "GetAppListByBizIDs scenes:%d, robotIDs:%v", scenes, robotIDs)
	req := &admin.GetAppListReq{
		Page:      1,
		PageSize:  uint32(len(robotIDs)),
		BotBizIds: robotIDs,
		Scenes:    scenes,
	}
	log.DebugContextf(ctx, "GetAppListByBizIDs GetAppList req:%+v", req)
	rsp, err := d.adminApiCli.GetAppList(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppListByBizIDs GetAppList Failed, err:%+v", err)
		return nil, err
	}
	log.DebugContextf(ctx, "GetAppListByBizIDs GetAppList rsp:%+v", rsp)
	appInfoMap := make(map[uint64]*admin.GetAppListRsp_AppInfo)
	for _, appInfo := range rsp.GetList() {
		appInfoMap[appInfo.AppBizId] = appInfo
	}
	if len(robotIDs) != len(appInfoMap) {
		return nil, fmt.Errorf("len(robotIDs):%d != len(appInfoMap):%d", len(robotIDs), len(appInfoMap))
	}
	log.InfoContextf(ctx, "GetAppListByBizIDs appInfoMap:%v", appInfoMap)
	return appInfoMap, nil
}

// CreateKnowledgeDeleteTask 创建知识删除任务
func (d *dao) CreateKnowledgeDeleteTask(ctx context.Context, params model.KnowledgeDeleteParams) error {
	log.InfoContextf(ctx, "CreateKnowledgeDeleteTask params:%+v", params)
	return newKnowledgeDeleteTask(ctx, params)
}

// CountTableNeedDeletedData 统计表需要删除数据的数量
func (d *dao) CountTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpID, robotID, tableName)
	var count int64
	db := d.gormDBDelete.WithContext(ctx).Table(tableName)
	if corpID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpID, dbFieldRobotID)
		db = db.Where(query, corpID, robotID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldRobotID)
		db = db.Where(query, robotID)
	}
	err := db.Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "CountTableNeedDeletedData gormDB.Count Failed, err:%+v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s, count:%d",
		corpID, robotID, tableName, count)
	return count, nil
}

// CountTableNeedDeletedDataBizID 统计表需要删除数据的数量
func (d *dao) CountTableNeedDeletedDataBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s",
		corpBizID, robotBizID, tableName)
	var count int64
	db := d.tdsqlGormDelete.WithContext(ctx).Table(tableName)
	if corpBizID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpBizID, dbFieldRobotBizID)
		db = db.Where(query, corpBizID, robotBizID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldRobotBizID)
		db = db.Where(query, robotBizID)
	}
	err := db.Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "CountTableNeedDeletedData gormDB.Count Failed, err:%+v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "CountTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s, count:%d",
		corpBizID, robotBizID, tableName, count)
	return count, nil
}

// CountTableNeedDeletedDataByCorpAndAppBizID 统计表需要删除数据的数量
func (d *dao) CountTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string) (int64, error) {
	log.InfoContextf(ctx, "CountTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s",
		corpBizID, robotBizID, tableName)
	var count int64
	db := d.tdsqlGormDelete.WithContext(ctx).Table(tableName)
	if corpBizID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpBizID, dbFieldAppBizID)
		db = db.Where(query, corpBizID, robotBizID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldAppBizID)
		db = db.Where(query, robotBizID)
	}
	err := db.Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "CountTableNeedDeletedDataByCorpAndAppBizID gormDB.Count Failed, err:%+v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "CountTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s, count:%d",
		corpBizID, robotBizID, tableName, count)
	return count, nil
}

// DeleteTableNeedDeletedData 删除表需要删除的数据
func (d *dao) DeleteTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "DeleteTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpID, robotID, tableName, totalCount)
	count, err := d.CountTableNeedDeletedData(ctx, corpID, robotID, tableName)
	if err != nil {
		return err
	}
	if count != totalCount {
		err = fmt.Errorf("count not equal totalCount:%d != %d", count, totalCount)
		log.ErrorContextf(ctx, "DeleteTableNeedDeletedData Failed, err:%+v", err)
		return err
	}
	deletedCount := int64(0)
	for count > 0 {
		db := d.gormDBDelete.WithContext(ctx)
		if corpID > 0 {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? AND %s = ? LIMIT %d", tableName,
				dbFieldCorpID, dbFieldRobotID, utilConfig.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, corpID, robotID)
		} else {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? LIMIT %d", tableName,
				dbFieldRobotID, utilConfig.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, robotID)
		}
		if db.Error != nil {
			log.ErrorContextf(ctx, "DeleteTableNeedDeletedData gormDB.Deleted Failed, err:%+v", db.Error)
			return db.Error
		}
		deletedCount += db.RowsAffected
		log.InfoContextf(ctx, "DeleteTableNeedDeletedData deletedCount:%d", deletedCount)
		count, err = d.CountTableNeedDeletedData(ctx, corpID, robotID, tableName)
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		log.ErrorContextf(ctx, "DeleteTableNeedDeletedData Failed, err:%+v", err)
		return err
	}
	log.InfoContextf(ctx, "DeleteTableNeedDeletedData corpID:%d, robotID:%d, tableName:%s success",
		corpID, robotID, tableName)
	return nil
}

// DeleteTableNeedDeletedDataBizID 删除表需要删除的数据
func (d *dao) DeleteTableNeedDeletedDataBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "DeleteTableNeedDeletedDataBizID corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpBizID, robotBizID, tableName, totalCount)
	count, err := d.CountTableNeedDeletedDataBizID(ctx, corpBizID, robotBizID, tableName)
	if err != nil {
		return err
	}
	if count != totalCount {
		err = fmt.Errorf("count not equal totalCount:%d != %d", count, totalCount)
		log.ErrorContextf(ctx, "DeleteTableNeedDeletedDataBizID Failed, err:%+v", err)
		return err
	}
	deletedCount := int64(0)
	for count > 0 {
		db := d.tdsqlGormDelete.WithContext(ctx)
		if corpBizID > 0 {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? AND %s = ? LIMIT %d", tableName,
				dbFieldCorpBizID, dbFieldRobotBizID, utilConfig.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, corpBizID, robotBizID)
		} else {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? LIMIT %d", tableName,
				dbFieldRobotBizID, utilConfig.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, robotBizID)
		}
		if db.Error != nil {
			log.ErrorContextf(ctx, "DeleteTableNeedDeletedDataBizID gormDB.Deleted Failed, err:%+v", db.Error)
			return db.Error
		}
		deletedCount += db.RowsAffected
		log.InfoContextf(ctx, "DeleteTableNeedDeletedDataBizID deletedCount:%d", deletedCount)
		count, err = d.CountTableNeedDeletedDataBizID(ctx, corpBizID, robotBizID, tableName)
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		log.ErrorContextf(ctx, "DeleteTableNeedDeletedDataBizID Failed, err:%+v", err)
		return err
	}
	log.InfoContextf(ctx, "DeleteTableNeedDeletedDataBizID corpID:%d, robotID:%d, tableName:%s success",
		corpBizID, robotBizID, tableName)
	return nil
}

// DeleteTableNeedDeletedDataByCorpAndAppBizID 删除表需要删除的数据
func (d *dao) DeleteTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpBizID, robotBizID uint64,
	tableName string, totalCount int64) error {
	log.InfoContextf(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s, totalCount:%d",
		corpBizID, robotBizID, tableName, totalCount)
	count, err := d.CountTableNeedDeletedDataByCorpAndAppBizID(ctx, corpBizID, robotBizID, tableName)
	if err != nil {
		return err
	}
	if count != totalCount {
		err = fmt.Errorf("count not equal totalCount:%d != %d", count, totalCount)
		log.ErrorContextf(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID Failed, err:%+v", err)
		return err
	}
	deletedCount := int64(0)
	for count > 0 {
		db := d.tdsqlGormDelete.WithContext(ctx)
		if corpBizID > 0 {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? AND %s = ? LIMIT %d", tableName,
				dbFieldCorpBizID, dbFieldAppBizID, utilConfig.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, corpBizID, robotBizID)
		} else {
			deleteSql := fmt.Sprintf("DELETE from %s WHERE %s = ? LIMIT %d", tableName,
				dbFieldAppBizID, utilConfig.GetMainConfig().KnowledgeDeleteConfig.DeleteBatchSize)
			db = db.Exec(deleteSql, robotBizID)
		}
		if db.Error != nil {
			log.ErrorContextf(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID gormDB.Deleted Failed, err:%+v", db.Error)
			return db.Error
		}
		deletedCount += db.RowsAffected
		log.InfoContextf(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID deletedCount:%d", deletedCount)
		count, err = d.CountTableNeedDeletedDataByCorpAndAppBizID(ctx, corpBizID, robotBizID, tableName)
		if err != nil {
			return err
		}
	}
	if deletedCount != totalCount {
		err = fmt.Errorf("deletedCount not equal totalCount:%d != %d", deletedCount, totalCount)
		log.ErrorContextf(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID Failed, err:%+v", err)
		return err
	}
	log.InfoContextf(ctx, "DeleteTableNeedDeletedDataByCorpAndAppBizID corpID:%d, robotID:%d, tableName:%s success",
		corpBizID, robotBizID, tableName)
	return nil
}

// GetCustomFieldIDList 获取指定表自定义主键ID列表
func (d *dao) GetCustomFieldIDList(ctx context.Context, corpID, robotID uint64,
	tableName, customField string) ([]uint64, error) {
	log.InfoContextf(ctx, "GetCustomFieldIDList corpID:%d, robotID:%d, tableName:%s, customField:%s",
		corpID, robotID, tableName, customField)
	ids := make([]uint64, 0)
	db := d.gormDBDelete.WithContext(ctx).Table(tableName).Select(customField)
	if corpID > 0 {
		query := fmt.Sprintf("%s = ? AND %s = ?", dbFieldCorpID, dbFieldRobotID)
		db = db.Where(query, corpID, robotID)
	} else {
		query := fmt.Sprintf("%s = ?", dbFieldRobotID)
		db = db.Where(query, robotID)
	}
	err := db.Limit(utilConfig.GetMainConfig().KnowledgeDeleteConfig.QueryBatchSize).
		Find(&ids).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetCustomFieldIDList gormDB.Find Failed, err:%+v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "GetCustomFieldIDList corpID:%d, robotID:%d, tableName:%s, customField:%s, "+
		"len(ids):%d", corpID, robotID, tableName, customField, len(ids))
	return ids, nil
}

// DeleteByCustomFieldID 删除指定表自定义字段列表
func (d *dao) DeleteByCustomFieldID(ctx context.Context, tableName string, limit int64,
	customFields []string, customConditions []string, customFieldValues []interface{}) (int64, error) {
	log.InfoContextf(ctx, "DeleteByCustomFieldID tableName:%s, limit:%d, "+
		"customFields:%+v, customConditions:%+v, len(customFieldValues):%d", tableName, limit,
		customFields, customConditions, len(customFieldValues))
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
		db := d.gormDBDelete.WithContext(ctx).Exec(deleteSql, customFieldValues...)
		if db.Error != nil {
			log.ErrorContextf(ctx, "DeleteByCustomFieldID gormDB.Deleted Failed, err:%+v", db.Error)
			return 0, db.Error
		}
		deletedCount = db.RowsAffected
	}
	log.InfoContextf(ctx, "DeleteByCustomFieldID tableName:%s, deletedCount:%d", tableName, deletedCount)
	return deletedCount, nil
}

// KnowledgeDeleteResultCallback 知识删除任务结果回调
func (d *dao) KnowledgeDeleteResultCallback(ctx context.Context, taskID uint64, isSuccess bool, message string) error {
	log.InfoContextf(ctx, "KnowledgeDeleteResultCallback taskID:%d, isSuccess:%v, message:%s",
		taskID, isSuccess, message)
	req := &admin.ClearAppKnowledgeCallbackReq{
		TaskId:    taskID,
		IsSuccess: isSuccess,
		Message:   message,
	}
	log.DebugContextf(ctx, "KnowledgeDeleteResultCallback ClearAppKnowledgeCallback req:%+v", req)
	rsp, err := d.adminApiCli.ClearAppKnowledgeCallback(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "KnowledgeDeleteResultCallback ClearAppKnowledgeCallback Failed, err:%+v", err)
		return err
	}
	log.DebugContextf(ctx, "KnowledgeDeleteResultCallback ClearAppKnowledgeCallback rsp:%+v", rsp)
	return nil
}
