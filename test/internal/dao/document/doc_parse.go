package document

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
)

func (d *daoImpl) CreateDocParseTask(ctx context.Context, docParse *docEntity.DocParse) error {
	/*
		  `
			INSERT INTO
			    t_doc_parse (%s)
			VALUES
			    (null,:robot_id,:corp_id,:request_id,:doc_id,:source_env_set,:task_id,:type,:op_type,:result,:status,
				:create_time,:update_time)
		`
	*/
	tbl := d.Query().TDocParse
	docParseTableName := tbl.TableName()

	now := time.Now()
	docParse.UpdateTime = now
	docParse.CreateTime = now
	tDocParse := ConvertDocParseDO2PO(docParse)

	db, err := knowClient.GormClient(ctx, docParseTableName, docParse.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.TDocParse{}).Create(tDocParse).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to Create DocParse Dao. err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocParseTask 更新文档解析任务信息
func (d *daoImpl) UpdateDocParseTaskByTx(ctx context.Context, updateColumns []string, docParse *docEntity.DocParse, tx *gorm.DB) error {
	/*
		`
			UPDATE
				t_doc_parse
			SET
			    result = :result,
			    status = :status,
				request_id = :request_id,
			    update_time = :update_time
			WHERE
			    id = :
	*/
	logx.I(ctx, "UpdateDocParseTask|updateColumns:%+v", updateColumns)

	tbl := d.Query().TDocParse
	if tx != nil {
		tbl = mysqlquery.Use(tx).TDocParse
	}
	db := tbl.WithContext(ctx).UnderlyingDB()

	tDocParse := ConvertDocParseDO2PO(docParse)
	now := time.Now()
	tDocParse.UpdateTime = now

	if len(updateColumns) == 0 {
		updateColumns = docEntity.DocParseUpdateColList
	}

	if err := db.Model(tDocParse).Select(updateColumns).Updates(tDocParse).Error; err != nil {
		logx.E(ctx, "Update DocParse Result error. docParse:%+v err:%+v", docParse, err)
		return err
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateDocParseCondition(ctx context.Context, session *gorm.DB, filter *docEntity.DocParseFilter) *gorm.DB {
	tbl := d.Query().TDocParse
	if filter.CorpPrimaryId != 0 {
		session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, filter.CorpPrimaryId)
	}
	if filter.AppPrimaryId != 0 {
		session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.AppPrimaryId)
	}
	if filter.DocID != 0 {
		session.Where(tbl.DocID.ColumnName().String()+util.SqlEqual, filter.DocID)
	}
	if len(filter.DocIDs) > 0 {
		session.Where(tbl.DocID.ColumnName().String()+util.SqlIn, filter.DocIDs)
	}
	if len(filter.Status) > 0 {
		session.Where(tbl.Status.ColumnName().String()+util.SqlIn, filter.Status)
	}
	if filter.OpType != 0 {
		session.Where(tbl.OpType.ColumnName().String()+util.SqlEqual, filter.OpType)
	}
	if filter.Type != 0 {
		session.Where(tbl.Type.ColumnName().String()+util.SqlEqual, filter.Type)
	}
	if filter.TaskID != "" {
		session.Where(tbl.TaskID.ColumnName().String()+util.SqlEqual, filter.TaskID)
	}
	if !filter.DeadlineForCreation.IsZero() {
		session.Where(tbl.CreateTime.Gte(filter.DeadlineForCreation))
	}
	return session
}

// DeleteDocParseByDocID 物理删除某一文档的解析任务数据
func (d *daoImpl) DeleteDocParseByDocID(ctx context.Context, corpID,
	robotID, docID uint64) error {
	tbl := d.Query().TDocParse
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}
	session := db.WithContext(ctx).Table(tableName).
		Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, corpID).
		Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
		Where(tbl.DocID.ColumnName().String()+util.SqlEqual, docID)
	res := session.Delete(&model.TDoc{})
	if res.Error != nil {
		logx.E(ctx, "DeleteDocParseByDocID|docID:%d|err:%+v",
			docID, res.Error)
		return res.Error
	}
	return nil
}

func (d *daoImpl) GetDocParseListWithTx(ctx context.Context, selectColumns []string,
	filter *docEntity.DocParseFilter, tx *gorm.DB) ([]*docEntity.DocParse, error) {
	tbl := d.Query().TDocParse
	tableName := tbl.TableName()
	docParses := make([]*model.TDocParse, 0)

	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	session := db.Debug().Table(tableName).Select(selectColumns)
	session = d.generateDocParseCondition(ctx, session, filter)

	if filter.Limit == 0 {
		logx.I(ctx, "GetDocParseListWithTx|limit is 0, set  DefaultMaxPageSize")
		// 为0正常返回空结果即可
		// return BatchConvertDocParsePO2DO(docParses), nil
		filter.Limit = util.DefaultMaxPageSize
	}
	if filter.Limit > 0 {
		if filter.Limit > util.DefaultMaxPageSize {
			// 限制单次查询最大条数
			logx.W(ctx, "GetDocParseListWithTx limit is too large, limit:%d, convert limit to  %d",
				filter.Limit, util.DefaultMaxPageSize)
			// err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
			// logx.E(ctx, "GetDocParseList err: %+v", err)
			// return BatchConvertDocParsePO2DO(docParses), err
			filter.Limit = util.DefaultMaxPageSize
		}
		session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	}

	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docParses)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return BatchConvertDocParsePO2DO(docParses), res.Error
	}
	return BatchConvertDocParsePO2DO(docParses), nil
}

// GetDocParseList 获取文档解析任务列表
func (d *daoImpl) GetDocParseList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocParseFilter) ([]*docEntity.DocParse, error) {
	tbl := d.Query().TDocParse
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, filter.AppPrimaryId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}
	logx.D(ctx, "GetDocParseList filter:%+v", filter)
	allDocParseList := make([]*docEntity.DocParse, 0)
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docParseList, err := d.GetDocParseListWithTx(ctx, selectColumns, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocParseList failed, err: %+v", err)
			return nil, err
		}
		allDocParseList = append(allDocParseList, docParseList...)
		if len(docParseList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocParseList count:%d cost:%dms",
		len(allDocParseList), time.Since(beginTime).Milliseconds())
	return allDocParseList, nil
}
