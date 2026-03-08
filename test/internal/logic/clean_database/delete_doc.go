package clean_database

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"time"
)

const (
	docTableName         = "t_doc"
	tableColumnCorpBizId = "corp_biz_id"
	tableColumnRobotId   = "robot_id"
	tableColumnAppBizId  = "app_biz_id"
	tableColumnDocId     = "doc_id"
	tableColumnDocBizId  = "doc_biz_id"
)

// docRelatedTables 文档相关需要清理的表
var docRelatedTables = map[string][]string{
	"t_doc_parse":                      {tableColumnDocId},
	"t_doc_parse_intervene":            {tableColumnDocId},
	"t_doc_segment":                    {tableColumnDocId},
	"t_doc_segment_image":              {tableColumnRobotId, tableColumnDocId},
	"t_doc_segment_page_info":          {tableColumnRobotId, tableColumnDocId},
	"t_doc_attribute_label":            {tableColumnRobotId, tableColumnDocId},
	"t_doc_segment_org_data":           {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
	"t_doc_segment_org_data_temporary": {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
	"t_doc_segment_sheet_temporary":    {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
	"t_doc_schema":                     {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
}

// docRelatedTablesDeleteSql 文档相关需要清理的表的删除语句
var docRelatedTablesDeleteSql = map[string]string{
	"t_doc_parse":                      "delete from t_doc_parse where doc_id=? limit ?",
	"t_doc_parse_intervene":            "delete from t_doc_parse_intervene where doc_id=? limit ?",
	"t_doc_segment":                    "delete from t_doc_segment where doc_id=? limit ?",
	"t_doc_segment_image":              "delete from t_doc_segment_image where robot_id=? and doc_id=? limit ?",
	"t_doc_segment_page_info":          "delete from t_doc_segment_page_info where robot_id=? and doc_id=? limit ?",
	"t_doc_attribute_label":            "delete from t_doc_attribute_label where robot_id=? and doc_id=? limit ?",
	"t_doc_segment_org_data":           "delete from t_doc_segment_org_data where corp_biz_id=? and app_biz_id=? and doc_biz_id=? limit ?",
	"t_doc_segment_org_data_temporary": "delete from t_doc_segment_org_data_temporary where corp_biz_id=? and app_biz_id=? and doc_biz_id=? limit ?",
	"t_doc_segment_sheet_temporary":    "delete from t_doc_segment_sheet_temporary where corp_biz_id=? and app_biz_id=? and doc_biz_id=? limit ?",
	"t_doc_schema":                     "delete from t_doc_schema where corp_biz_id=? and app_biz_id=? and doc_biz_id=? limit ?",
}

// CleanDeletedDocs 清理已删除的文档
func CleanDeletedDocs(ctx context.Context, maxUpdateTime time.Time, batchSize uint32) {
	dbs := client.GetAllDbClients(ctx, docTableName)
	for _, db := range dbs {
		log.DebugContextf(ctx, "CleanDeletedDocs begin db:%+v", db)
		// t_doc文档表部署在多个实例，主实例和独立部署的实例（比如isearch）都需要清理
		docs := make([]*model.Doc, 0)
		selectSql := fmt.Sprintf("SELECT `id`,`business_id`,`corp_id`,`robot_id`,`status`,`is_deleted`,`update_time` FROM `t_doc` " +
			"WHERE is_deleted = 1 AND status = 12 AND update_time < ? LIMIT ?")
		args := []any{maxUpdateTime, batchSize}
		err := db.QueryToStructs(ctx, &docs, selectSql, args...)
		if err != nil {
			log.ErrorContextf(ctx, "CleanDeletedDocs selectSql:%s error:%+v", selectSql, err)
			return
		}
		log.DebugContextf(ctx, "CleanDeletedDocs GetDocList count:%d", len(docs))
		if len(docs) == 0 {
			continue
		}
		for _, doc := range docs {
			if !config.GetMainConfig().DefaultDatabaseCleanConfig.Enable {
				// 清理功能被关闭
				log.DebugContextf(ctx, "CleanDeletedDocs CleanDatabaseCommonData is not enable")
				return
			}
			if doc.UpdateTime.After(maxUpdateTime) {
				log.ErrorContextf(ctx, "CleanDeletedDocs doc.UpdateTime:%+v > maxUpdateTime:%+v",
					doc.UpdateTime, maxUpdateTime)
				continue
			}
			err = CleanDeletedDoc(ctx, doc)
			if err != nil {
				log.ErrorContextf(ctx, "CleanDeletedDocs CleanDeletedDoc doc.ID:%d failed, err: %+v", doc.ID, err)
				continue
			}
			_ = deleteDoc(ctx, db, doc.ID)
		}
	}
}

// CleanDeletedDoc 清理单个已删除的文档
func CleanDeletedDoc(ctx context.Context, doc *model.Doc) error {
	log.DebugContextf(ctx, "CleanDeletedDoc doc.ID:%d doc.BusinessID:%d doc.Status:%d doc.IsDeleted:%d doc.UpdateTime:%+v",
		doc.ID, doc.BusinessID, doc.Status, doc.IsDeleted, doc.UpdateTime)
	if doc.IsDeleted != model.IsDeleted || doc.Status != model.DocStatusReleaseSuccess {
		// 高危操作，再次检查文档信息
		log.ErrorContextf(ctx, "CleanDeletedDoc doc.IsDeleted:%+v != model.IsDeleted:%+v || doc.Status:%+v != model.DocStatusReleaseSuccess:%+v",
			doc.IsDeleted, model.IsDeleted, doc.Status, model.DocStatusReleaseSuccess)
		return errs.ErrSystem
	}
	corpBizId, err := dao.GetCorpBizIDByCorpID(ctx, doc.CorpID)
	if err != nil {
		log.ErrorContextf(ctx, "CleanDeletedDoc GetCorpBizIDByCorpID doc.ID:%d doc.CorpID:%d error:%+v",
			doc.ID, doc.CorpID, err)
		return err
	}
	appBizId, err := dao.GetAppBizIDByAppID(ctx, doc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "CleanDeletedDoc GetAppBizIDByAppID doc.ID:%d doc.RobotID:%d error:%+v",
			doc.ID, doc.RobotID, err)
		return err
	}
	// 先删除文档相关的其他表信息，最后删除文档信息，避免中途失败导致的数据不一致
	// 删除文档相关的其他表信息
	for table, conditionColumns := range docRelatedTables {
		deleteSql, ok := docRelatedTablesDeleteSql[table]
		if !ok {
			log.ErrorContextf(ctx, "CleanDeletedDoc table:%s deleteSql not found", table)
			return errs.ErrSystem
		}
		args := make([]any, 0)
		for _, column := range conditionColumns {
			switch column {
			case tableColumnCorpBizId:
				args = append(args, corpBizId)
			case tableColumnRobotId:
				args = append(args, doc.RobotID)
			case tableColumnAppBizId:
				args = append(args, appBizId)
			case tableColumnDocId:
				args = append(args, doc.ID)
			case tableColumnDocBizId:
				args = append(args, doc.BusinessID)
			}
		}

		dbs := client.GetAllDbClients(ctx, table)
		for _, db := range dbs {
			deletedCount, err := DeleteDataInTableBySql(ctx, db, deleteSql, args)
			if err != nil {
				log.ErrorContextf(ctx, "DeleteDataInTable table:%s doc.ID:%d error:%+v", table, doc.ID, err)
				return err
			}
			log.DebugContextf(ctx, "DeleteDataInTable table:%s doc.ID:%d deletedCount:%d",
				table, doc.ID, deletedCount)
		}
	}
	return nil
}

// deleteDoc 删除单个文档信息
func deleteDoc(ctx context.Context, db mysql.Client, docID uint64) error {
	deleteSql := fmt.Sprintf("delete from t_doc where id = ? limit 1")
	args := []any{docID}
	_, err := db.Exec(ctx, deleteSql, args...)
	if err != nil {
		log.ErrorContextf(ctx, "CleanDeletedDoc delete doc:%d error:%+v", docID, err)
		return err
	}
	log.DebugContextf(ctx, "CleanDeletedDoc delete doc:%d", docID)
	return nil
}
