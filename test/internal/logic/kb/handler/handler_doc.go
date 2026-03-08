package handler

import (
	"context"
	"time"

	"gorm.io/gorm"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	R "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// LabelHandler 标签删除
type DocHandler struct {
	r      *rpc.RPC
	docDao docDao.Dao
}

// NewLabelHandler 初始化标签删除处理
func NewDocHandler(r *rpc.RPC, docDao docDao.Dao) *DocHandler {
	return &DocHandler{
		r:      r,
		docDao: docDao,
	}
}

// CountNeedDeletedData 统计表需要删除数据的数量
func (d *DocHandler) CountNeedDeletedData(ctx context.Context, corpID, robotID uint64, tableName string) (int64, error) {
	return 0, nil

}

// DeleteNeedDeletedData 删除表需要删除的数据
func (d *DocHandler) DeleteNeedDeletedData(ctx context.Context, corpID, robotID uint64, tableName string, totalCount int64) error {
	return nil

}

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
	"t_doc_parse": {tableColumnDocId},
	// "t_doc_parse_intervene":            {tableColumnDocId},
	"t_doc_segment":                    {tableColumnDocId},
	"t_doc_segment_image":              {tableColumnRobotId, tableColumnDocId},
	"t_doc_segment_page_info":          {tableColumnRobotId, tableColumnDocId},
	"t_doc_attribute_label":            {tableColumnRobotId, tableColumnDocId},
	"t_doc_segment_org_data":           {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
	"t_doc_segment_org_data_temporary": {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
	"t_doc_segment_sheet_temporary":    {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
	"t_doc_schema":                     {tableColumnCorpBizId, tableColumnAppBizId, tableColumnDocBizId},
}

var docRelatedTableDelCondition = map[string]*DelteDataCondition{
	"t_doc_parse": {ConditionStr: "doc_id=?", Model: &model.TDocParse{}},
	// "t_doc_parse_intervene":             &DelteDataCondition{ConditionStr: "doc_id=?", Model: &model.TDoc{}},
	"t_doc_segment":                    {ConditionStr: "doc_id=?", Model: &model.TDocSegment{}},
	"t_doc_segment_image":              {ConditionStr: "robot_id=? and doc_id=?", Model: &model.TDocSegmentImage{}},
	"t_doc_segment_page_info":          {ConditionStr: "robot_id=? and doc_id=?", Model: &model.TDocSegmentPageInfo{}},
	"t_doc_attribute_label":            {ConditionStr: "robot_id=? and doc_id=?", Model: &model.TDocAttributeLabel{}},
	"t_doc_segment_org_data":           {ConditionStr: "corp_biz_id=? and app_biz_id=? and doc_biz_id=?", Model: &model.TDocSegmentOrgDatum{}},
	"t_doc_segment_org_data_temporary": {ConditionStr: "corp_biz_id=? and app_biz_id=? and doc_biz_id=?", Model: &model.TDocSegmentOrgDataTemporary{}},
	"t_doc_segment_sheet_temporary":    {ConditionStr: "corp_biz_id=? and app_biz_id=? and doc_biz_id=?", Model: &model.TDocSegmentSheetTemporary{}},
	"t_doc_schema":                     {ConditionStr: "corp_biz_id=? and app_biz_id=? and doc_biz_id=?", Model: &model.TDocSchema{}},
}

type DelteDataCondition struct {
	TableName    string
	ConditionStr string
	Args         []any
	Limit        int
	Model        any
}

// DeleteDataInTable 删除表中所有满足条件的数据
func DeleteDataInTable(ctx context.Context, db *gorm.DB, delCond *DelteDataCondition) (int64, error) {
	if delCond == nil {
		return 0, errs.ErrSystem
	}
	if delCond.ConditionStr == "" {
		logx.E(ctx, "DeleteDataInTableByLimit conditionStr is empty")
		return 0, errs.ErrSystem
	}
	if len(delCond.Args) == 0 {
		logx.E(ctx, "DeleteDataInTableByLimit args is empty")
		return 0, errs.ErrSystem
	}
	if delCond.Model == nil {
		logx.E(ctx, "DeleteDataInTableByLimit deleted model is empty")
		return 0, errs.ErrSystem
	}
	deletedCount := int64(0)
	limit := config.GetMainConfig().DefaultDatabaseCleanConfig.DeleteBatchSize
	delCond.Limit = int(limit)
	for {
		affectedCount, err := DeleteDataInTableByCondition(ctx, db, delCond)
		if err != nil {
			logx.E(ctx, "DeleteDataInTable delCond:%+v error:%+v",
				delCond, err)
			return 0, err
		}
		deletedCount += affectedCount
		if affectedCount < int64(limit) {
			// 已分页清理完所有数据
			break
		}
	}
	logx.D(ctx, "DeleteDataInTable delCond:%+v deletedCount:%d",
		delCond, deletedCount)
	return deletedCount, nil
}

// DeleteDataInTableByCondition 删除表中指定数量满足条件的数据
func DeleteDataInTableByCondition(ctx context.Context, db *gorm.DB, delCond *DelteDataCondition) (int64, error) {

	// 2、根据id删除数据
	db = db.WithContext(ctx).Table(delCond.TableName).
		Where(delCond.ConditionStr, delCond.Args...)

	if delCond.Limit > 0 {
		db = db.Limit(delCond.Limit)
	}
	if res := db.Delete(delCond.Model); res.Error != nil {
		logx.E(ctx, "DeleteDataInTableByCondition delCond:%+v error:%+v", delCond, res.Error)
		return 0, res.Error
	} else {
		affectedCount := res.RowsAffected

		logx.D(ctx, "DeleteDataInTableByCondition delCond%+v affectedCount:%d",
			delCond, affectedCount)
		return affectedCount, nil

	}
}

// CleanDeletedDocs 清理已删除的文档
func CleanDeletedDocs(ctx context.Context, maxUpdateTime time.Time, batchSize int, docDao docDao.DocDao, rpc *rpc.RPC, cache *localcache.Logic) {
	dbs := R.GetAllGormClients(ctx, docTableName)
	for _, db := range dbs {
		logx.D(ctx, "CleanDeletedDocs begin db:%+v", db)
		// t_doc文档表部署在多个实例，主实例和独立部署的实例（比如isearch）都需要清理
		docs := make([]*docEntity.Doc, 0)

		// fmt.Sprintf("SELECT `id`,`business_id`,`corp_id`,`robot_id`,`status`,`is_deleted`,`update_time` FROM `t_doc` " +
		// 	"WHERE is_deleted = 1 AND status = 12 AND update_time < ? LIMIT ?")
		delFlag := true

		docFilter := &docEntity.DocFilter{
			IsDeleted:     &delFlag,
			Status:        []uint32{docEntity.DocStatusReleaseSuccess},
			MaxUpdateTime: maxUpdateTime,

			Limit: batchSize,
		}

		selectColumns := []string{
			docEntity.DocTblColId, docEntity.DocTblColBusinessId, docEntity.DocTblColCorpId, docEntity.DocParseTblColRobotID,
			docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime, docEntity.DocTblColIsDeleted,
		}

		docs, err := docDao.GetAllDocs(ctx, selectColumns, docFilter)

		if err != nil {
			logx.E(ctx, "Faild to CleanDeletedDocs. error:%+v", err)
			return
		}

		logx.D(ctx, "CleanDeletedDocs GetDocList count:%d", len(docs))
		if len(docs) == 0 {
			continue
		}
		for _, doc := range docs {
			if !config.GetMainConfig().DefaultDatabaseCleanConfig.Enable {
				// 清理功能被关闭
				logx.D(ctx, "CleanDeletedDocs CleanDatabaseCommonData is not enable")
				return
			}
			if doc.UpdateTime.After(maxUpdateTime) {
				logx.E(ctx, "CleanDeletedDocs doc.UpdateTime:%+v > maxUpdateTime:%+v",
					doc.UpdateTime, maxUpdateTime)
				continue
			}
			err = CleanDeletedDoc(ctx, doc, rpc, cache)
			if err != nil {
				logx.E(ctx, "CleanDeletedDocs CleanDeletedDoc doc.ID:%d failed, err: %+v", doc.ID, err)
				continue
			}
			_ = deleteDoc(ctx, db, doc, docDao)
		}
	}
}

// CleanDeletedDoc 清理单个已删除的文档
func CleanDeletedDoc(ctx context.Context, doc *docEntity.Doc, rpc *rpc.RPC, cache *localcache.Logic) error {
	logx.D(ctx, "CleanDeletedDoc doc.ID:%d doc.BusinessID:%d doc.Status:%d doc.IsDeleted:%d doc.UpdateTime:%+v",
		doc.ID, doc.BusinessID, doc.Status, doc.IsDeleted, doc.UpdateTime)
	if !doc.IsDeleted || doc.Status != docEntity.DocStatusReleaseSuccess {
		// 高危操作，再次检查文档信息
		logx.E(ctx, "CleanDeletedDoc doc.IsDeleted:%+v || doc.Status:%+v not match", doc.IsDeleted, doc.Status)
		return errs.ErrSystem
	}
	corp, err := rpc.DescribeCorpByPrimaryId(ctx, doc.CorpID)
	// corpBizId, err := dao.GetCorpBizIDByCorpID(ctx, doc.CorpPrimaryId)
	if err != nil {
		logx.E(ctx, "CleanDeletedDoc GetCorpBizIDByCorpID doc.ID:%d doc.CorpPrimaryId:%d error:%+v",
			doc.ID, doc.CorpID, err)
		return err
	}
	corpPrimaryId := contextx.Metadata(ctx).CorpID()
	appBizId, err := cache.GetAppBizIdByPrimaryId(ctx, corpPrimaryId, doc.RobotID)
	if err != nil {
		logx.E(ctx, "CleanDeletedDoc GetAppBizIdByPrimaryId doc.ID:%d doc.AppPrimaryId:%d error:%+v",
			doc.ID, doc.RobotID, err)
		return err
	}
	// 先删除文档相关的其他表信息，最后删除文档信息，避免中途失败导致的数据不一致
	// 删除文档相关的其他表信息
	for table, conditionColumns := range docRelatedTables {
		// deleteSql, ok := docRelatedTablesDeleteSql[table]
		deleteCondtion, ok := docRelatedTableDelCondition[table]
		if !ok || deleteCondtion == nil {
			logx.E(ctx, "CleanDeletedDoc table:%s deleteCondtion not found", table)
			return errs.ErrSystem
		}
		args := make([]any, 0)
		for _, column := range conditionColumns {
			switch column {
			case tableColumnCorpBizId:
				args = append(args, corp.GetCorpId())
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

		deleteCondtion.Args = args
		deleteCondtion.TableName = table

		dbs := R.GetAllGormClients(ctx, table)
		for _, db := range dbs {
			deletedCount, err := DeleteDataInTable(ctx, db, deleteCondtion)
			if err != nil {
				logx.E(ctx, "DeleteDataInTable table:%s doc.ID:%d error:%+v", table, doc.ID, err)
				return err
			}
			logx.D(ctx, "DeleteDataInTable table:%s doc.ID:%d deletedCount:%d",
				table, doc.ID, deletedCount)
		}
	}
	return nil
}

// deleteDoc 删除单个文档信息
func deleteDoc(ctx context.Context, db *gorm.DB, doc *docEntity.Doc, docDao docDao.DocDao) error {
	// fmt.Sprintf("delete from t_doc where id = ? limit 1")
	if err := docDao.DeleteDocByTx(ctx, nil, doc, db); err != nil {
		logx.E(ctx, "CleanDeletedDoc delete doc:%d error:%+v", doc.ID, err)
		return err
	}
	logx.D(ctx, "CleanDeletedDoc delete doc:%d", doc.ID)
	return nil
}
