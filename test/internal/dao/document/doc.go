package document

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"gorm.io/gen/field"
	"gorm.io/gorm"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
)

func (d *daoImpl) getDocGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TDoc.WithContext(ctx).UnderlyingDB()
	}
	return d.mysql.TDoc.WithContext(ctx).UnderlyingDB()
}

func (d *daoImpl) CreateDoc(ctx context.Context, doc *docEntity.Doc, tx *gorm.DB) error {
	/*
		`
				INSERT INTO
				    t_doc (%s)
				VALUES
					(null,:business_id,:robot_id,:corp_id,:staff_id,:file_name,:file_name_in_audit,:file_type,:file_size,:bucket,:cos_url,:cos_hash,
				    :message,:status,:is_refer,:is_deleted,:source,:web_url,:batch_id,:audit_flag,:char_size,:is_creating_qa,
					:is_creating_index,:next_action,:attr_range,:is_created_qa,:refer_url_type,:create_time,:update_time,
				    :expire_start,:expire_end,:opt,:category_id,:original_url,:processing_flag,:customer_knowledge_id,
					:attribute_flag,:is_downloadable,:update_period_h,:next_update_time,:split_rule)
			`
	*/
	tbl := d.mysql.TDoc
	if tx != nil {
		tbl = mysqlquery.Use(tx).TDoc
	}
	db := tbl.WithContext(ctx)

	now := time.Now()
	doc.UpdateTime = now
	doc.CreateTime = now

	tDoc := ConvertDocDoToPO(doc)

	err := db.Create(tDoc)
	if err != nil {
		logx.E(ctx, "CreateDoc failed doc:%v, err: %+v", doc, err)
		return err
	}
	doc.ID = tDoc.ID // 这里需要验证一下 好不好用
	return nil
}

func (d *daoImpl) BatchUpdateDocsByFilter(ctx context.Context, filter *docEntity.DocFilter,
	updateColumns map[string]any, tx *gorm.DB) error {
	/*
		UPDATE t_doc SET status = ?,  next_action = ?, update_time = ?
							WHERE
							    id IN (%s)
	*/
	tbl := d.mysql.TDoc

	db := tx
	if tx == nil {
		db = d.mysql.TDoc.WithContext(ctx).Debug().UnderlyingDB()
	}

	var err error

	if filter.RobotId > 0 {
		db, err = knowClient.GormClient(ctx, tbl.TableName(), filter.RobotId, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "get GormClient failed, err: %+v", err)
			return err
		}

	}

	session := mysqlquery.Use(db.Table(tbl.TableName())).TDoc.WithContext(ctx).Debug()

	session = d.generateGenCondition(ctx, session, filter)
	if _, err := session.Updates(updateColumns); err != nil {
		logx.E(ctx, "BatchUpdateDocsByFilter update filter:%+v, error:%v",
			filter, err)
		return err
	}
	return nil
}

// UpdateDoc 更新文档
func (d *daoImpl) UpdateDoc(ctx context.Context, updateColumns []string, filter *docEntity.DocFilter,
	doc *docEntity.Doc) (int64, error) {
	tbl := d.Query().TDoc
	db, err := knowClient.GormClient(ctx, tbl.TableName(), filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := mysqlquery.Use(db).TDoc.WithContext(ctx).Debug().Select(d.getTDocGenFields(updateColumns)...)
	session = d.generateGenCondition(ctx, session, filter)
	tDoc := ConvertDocDoToPO(doc)
	res, err := session.Updates(tDoc)
	if err != nil {
		logx.E(ctx, "UpdateDoc failed doc:%v, err: %+v", doc, err)
		return 0, res.Error
	}
	logx.D(ctx, "update doc record: %v", res.RowsAffected)
	return res.RowsAffected, nil
}

// UpdateDoc 更新文档
func (d *daoImpl) UpdateDocByTx(ctx context.Context, updateColumns []string, filter *docEntity.DocFilter,
	doc *docEntity.Doc, tx *gorm.DB) (int64, error) {
	tbl := d.Query().TDoc
	// db, err := knowClient.GormClient(ctx, tbl.TableName(), doc.AppPrimaryId, doc.BusinessID, []client.Option{}...)
	// if err != nil {
	// 	logx.E(ctx, "get GormClient failed, err: %+v", err)
	// 	return 0, err
	// }
	session := tx
	if tx == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	qSession := mysqlquery.Use(session).TDoc.WithContext(ctx).Debug()

	if len(updateColumns) != 0 {
		qSession = qSession.Select(d.getTDocGenFields(updateColumns)...)
	}

	qSession = d.generateGenCondition(ctx, qSession, filter)
	tDoc := ConvertDocDoToPO(doc)

	res, err := qSession.Updates(tDoc)
	if err != nil {
		logx.E(ctx, "UpdateDoc failed doc:%v, err: %+v", doc, err)
		return 0, err
	}
	logx.D(ctx, "update doc record: %v", res.RowsAffected)
	return res.RowsAffected, nil
}

func (d *daoImpl) BatchUpdateDocs(ctx context.Context, docs []*docEntity.Doc, tx *gorm.DB) error {
	/*
		UPDATE t_doc SET status = ?,  next_action = ?, update_time = ?
							WHERE
							    id IN (%s)
	*/
	if len(docs) == 0 {
		logx.I(ctx, "not doc records to update")
		return nil
	}

	db := d.getDocGormDB(ctx, tx)

	tDocs := BatchConvertDocDoToPO(docs)
	err := mysqlquery.Use(db).Transaction(func(tx *mysqlquery.Query) error {
		session := tx.TDoc.WithContext(ctx).Debug()

		for _, v := range tDocs {
			if _, err := session.Updates(v); err != nil {
				logx.E(ctx, "BatchUpdateDocs update t_doc doc:%+v, error:%v",
					v.ID, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *daoImpl) generateStatusCondition(ctx context.Context,
	q mysqlquery.ITDocDo,
	status []uint32, validityStatus uint32) mysqlquery.ITDocDo {
	tbl := d.mysql.TDoc
	subQ := tbl.WithContext(ctx).Debug()
	zeroTime := time.Unix(0, 0)
	nowTime := time.Now()
	// zeroTimeStr := time.Unix(0, 0).Format("2006-01-02 15:04:05.000")
	// nowTimeStr := time.Now().Format("2006-01-02 15:04:05.000")

	expiredStatusCondition := subQ.Where(tbl.ExpireEnd.Gt(zeroTime)).Where(tbl.ExpireEnd.Lt(nowTime))
	validStatusCondition := subQ.Where(tbl.ExpireEnd.Eq(zeroTime)).Or(tbl.ExpireEnd.Gte(nowTime))

	// 勾选其他状态，未勾选已过期
	if len(status) != 0 && validityStatus != docEntity.DocExpiredStatus {
		q = q.Where(validStatusCondition).Where(tbl.Status.In(status...))
		return q
	}

	// 只勾选已过期
	if len(status) == 0 && validityStatus == docEntity.DocExpiredStatus {
		q = q.Where(expiredStatusCondition)
		return q
	}
	// 勾选其他状态+已过期
	if len(status) != 0 && validityStatus == docEntity.DocExpiredStatus {
		q = q.Where(
			subQ.Where(expiredStatusCondition).
				Or(subQ.Where(validStatusCondition).Where(tbl.Status.In(status...))))
		return q
	}
	return q
}

func (d *daoImpl) getTDocGenFields(selectColumns []string) []field.Expr {
	fields := make([]field.Expr, 0)
	for _, v := range selectColumns {
		if f, ok := d.mysql.TDoc.GetFieldByName(v); ok {
			fields = append(fields, f)
		}
	}
	return fields
}

func (d *daoImpl) generateGenCondition(ctx context.Context, q mysqlquery.ITDocDo, filter *docEntity.DocFilter) mysqlquery.ITDocDo {
	if filter == nil {
		return q
	}

	tbl := d.Query().TDoc
	subQ := tbl.WithContext(ctx).Debug()

	if filter.ID != 0 {
		q = q.Where(tbl.ID.Eq(filter.ID))
		// conds = append(conds, tbl.ID.Eq(filter.ID))
	}

	if len(filter.IDs) != 0 {
		q = q.Where(tbl.ID.In(filter.IDs...))
	}

	if filter.CorpId != 0 {
		q = q.Where(tbl.CorpID.Eq(filter.CorpId))
		// conds = append(conds, tbl.CorpPrimaryId.Eq(filter.CorpId))
	}

	if filter.RobotId != 0 {
		q = q.Where(tbl.RobotID.Eq(filter.RobotId))
		// conds = append(conds, tbl.AppPrimaryId.Eq(filter.RobotId))
	}

	if len(filter.RobotIDs) != 0 {
		q = q.Where(tbl.RobotID.In(filter.RobotIDs...))
		// conds = append(conds, tbl.AppPrimaryId.In(filter.RobotIDs...))
	}

	if len(filter.OrIDs) != 0 {
		q = q.Or(tbl.ID.In(filter.OrIDs...))
	}

	if filter.CosHash != "" {
		q = q.Where(tbl.CosHash.Eq(filter.CosHash))
	}

	if filter.Source != nil {
		q = q.Where(tbl.Source.In(filter.Source...))
	}

	if !filter.NextUpdateTime.IsZero() {
		q = q.Where(tbl.NextUpdateTime.Eq(filter.NextUpdateTime))
	}

	// is_deleted不能设置默认查询条件，有些场景需要查询已删除的文档，比如文档比对任务
	if filter.IsDeleted != nil {
		q = q.Where(tbl.IsDeleted.Is(*filter.IsDeleted))
	}
	for flag, val := range filter.FilterFlag {
		// 兜底保护, 查询指定标识字段
		switch flag {
		case docEntity.DocFilterFlagIsCreatedQa:
			q = q.Where(tbl.IsCreatedQa.Is(val))
		}
	}

	if filter.OriginalURL != "" {
		q = q.Where(tbl.OriginalURL.Eq(filter.OriginalURL))
	}

	if len(filter.FileTypes) != 0 {
		q = q.Where(tbl.FileType.In(filter.FileTypes...))
	}

	if len(filter.Opts) != 0 {
		q = q.Where(tbl.Opt.In(filter.Opts...))
	}

	if len(filter.CategoryIds) != 0 {
		q = q.Where(tbl.CategoryID.In(filter.CategoryIds...))
	}

	if len(filter.BusinessIds) != 0 {
		q = q.Where(tbl.BusinessID.In(filter.BusinessIds...))
	}
	if len(filter.NotInBusinessIds) != 0 {
		// 业务Id为该表唯一索引
		q = q.Where(tbl.BusinessID.NotIn(filter.NotInBusinessIds...))
	}
	if len(filter.NotInIDs) != 0 {
		// 业务Id为该表唯一索引
		q = q.Where(tbl.ID.NotIn(filter.NotInIDs...))
	}
	if !filter.MinUpdateTime.IsZero() {
		q = q.Where(tbl.UpdateTime.Gte(filter.MinUpdateTime))
	}
	if !filter.MaxUpdateTime.IsZero() {
		q = q.Where(tbl.UpdateTime.Lte(filter.MaxUpdateTime))
	}

	if len(filter.Status) != 0 || filter.ValidityStatus != 0 {
		// 状态相关的过滤条件
		q = d.generateStatusCondition(ctx, q, filter.Status, filter.ValidityStatus)
	}
	if filter.FileNameOrAuditName != "" {

		// 文件名或者审核中的文件名相同
		newStr := util.Special.Replace(filter.FileNameOrAuditName)

		q = q.Where(subQ.Where(tbl.FileNameInAudit.Eq(newStr)).
			Or(subQ.Where(tbl.FileNameInAudit.Eq("")).
				Where(tbl.FileName.Eq(newStr))))
	}
	if filter.FileNameSubStrOrAuditNameSubStr != "" {
		// 文件名或者审核中的文件名包含该字符串子串
		newStr := fmt.Sprintf("%%%s%%", util.Special.Replace(filter.FileNameSubStrOrAuditNameSubStr))

		q = q.Where(subQ.Where(tbl.FileNameInAudit.Like(newStr)).
			Or(subQ.Where(tbl.FileNameInAudit.Eq("")).
				Where(tbl.FileName.Like(newStr))))
	}

	if filter.FileNameSubStr != "" {
		newStr := fmt.Sprintf("%%%s%%", util.Special.Replace(filter.FileNameSubStr))
		q = q.Where(tbl.FileName.Like(newStr))
	}

	if len(filter.NextActions) > 0 {
		q = q.Where(tbl.NextAction.In(filter.NextActions...))
	}

	if filter.EnableScope != nil {
		q = q.Where(tbl.EnableScope.Eq(*filter.EnableScope))
	}

	return q

}

// GetDocList 获取文档列表
func (d *daoImpl) GetDocList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocFilter) ([]*docEntity.Doc, error) {
	allDocList := make([]*docEntity.Doc, 0)
	tbl := d.mysql.TDoc
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return allDocList, err
	}

	logx.D(ctx, "GetDocList filter:%+v", filter)

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
		docList, err := d.GetDocListWithFilter(ctx, selectColumns, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocList failed, err: %+v", err)
			return nil, err
		}
		allDocList = append(allDocList, docList...)
		if len(docList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocList count:%d cost:%dms",
		len(allDocList), time.Since(beginTime).Milliseconds())
	return allDocList, nil
}

func (d *daoImpl) CountDocWithTimeAndStatus(ctx context.Context,
	corpID, robotID uint64,
	status []uint32,
	startTime time.Time,
	tx *gorm.DB,
) (uint64, error) {
	/*
			querySQL := `
			SELECT
					count(DISTINCT t_doc.id)
				FROM
				    t_doc
				WHERE
				    t_doc.corp_id = ?
				AND t_doc.robot_id = ?
				AND (
		                (t_doc.create_time >= ? AND t_doc.is_deleted = 0) OR   -- 新增且未被删除
		                (t_doc.update_time >= ? AND t_doc.create_time < ?)     -- 修改或删除
		            )
		            AND t_doc.status IN (%s)
			`
			querySQL = fmt.Sprintf(querySQL, util.Placeholder(len(status))) */

	timeStr := startTime.Format("2006-01-02 15:04:05.000")
	tbl := d.mysql.TDoc
	tableName := tbl.TableName()

	session := tx

	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB().Table(tableName)
	}
	session = session.Table(tbl.TableName())

	session = session.Where(tbl.CorpID.Eq(corpID), tbl.RobotID.Eq(robotID))

	session = session.Where("(t_doc.create_time >= ? AND t_doc.is_deleted = 0) OR (t_doc.update_time >= ? AND t_doc.create_time < ?)",
		timeStr, timeStr, timeStr)

	if len(status) > 0 {
		session = session.Where(tbl.Status.In(status...))
	}

	var count int64
	res := session.Distinct(tbl.ID.ColumnName().String()).Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return uint64(count), nil
	//
	// queryCondition := `t_doc.corp_id = ?
	//	AND t_doc.robot_id = ?
	//	AND (
	//            (t_doc.create_time >= ? AND t_doc.is_deleted = 0) OR   -- 新增且未被删除
	//            (t_doc.update_time >= ? AND t_doc.create_time < ?)     -- 修改或删除
	//        )
	//        `
	// queryArgs := []any{corpID, robotID, timeStr, timeStr, timeStr}
	//
	// if len(status) > 0 {
	//	queryCondition += `AND t_doc.status IN (?)`
	//	queryArgs = append(queryArgs, status)
	// }
	//
	// var count int64
	// res := session.Where(queryCondition, queryArgs...).Distinct(tbl.ID.ColumnName().String()).Count(&count)
	// if res.Error != nil {
	//	logx.E(ctx, "execute sql failed, err: %+v", res.Error)
	//	return 0, res.Error
	// }
	// return uint64(count), nil
}

// GetDocCount 获取文档总数
func (d *daoImpl) GetDocCountByDistinctID(ctx context.Context, filter *docEntity.DocFilter) (int64, error) {
	/*
		`
				SELECT
					count(DISTINCT t_doc.id)
				FROM
				    t_doc %s
				WHERE
				    %s
			`
	*/
	tableName := d.Query().TDoc.TableName()
	db, err := knowClient.GormClient(ctx, tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}

	session := mysqlquery.Use(db).TDoc.WithContext(ctx).Debug()

	session = d.generateGenCondition(ctx, session, filter)
	count, err := session.Distinct(d.Query().TDoc.ID).Count()
	if err != nil {
		logx.E(ctx, "execute sql failed, err: %+v", err)
		return 0, err
	}
	return count, nil
}

func (d *daoImpl) GetRobotDocUsage(ctx context.Context, robotID uint64, corpID uint64) (entity.CapacityUsage, error) {
	/* `
			SELECT
	    		IFNULL(SUM(char_size), 0) as char_size,
				IFNULL(SUM(file_size), 0) as file_size
			FROM
			    t_doc
			WHERE
			     robot_id = ?
			AND corp_id = ?
			AND is_deleted = ?
			AND status NOT IN (%s)
		`*/
	type DocUsage struct {
		CharSize uint64 `gorm:"char_size"`
		FileSize uint64 `gorm:"file_size"`
	}
	var result DocUsage
	tbl := d.mysql.TDoc
	tableName := tbl.TableName()
	exceededStatus := []uint32{
		docEntity.DocStatusCharExceeded,
		docEntity.DocStatusResuming,
		docEntity.DocStatusParseImportFailCharExceeded,
		docEntity.DocStatusAuditFailCharExceeded,
		docEntity.DocStatusUpdateFailCharExceeded,
		docEntity.DocStatusCreateIndexFailCharExceeded,
		docEntity.DocStatusParseImportFailResuming,
		docEntity.DocStatusAuditFailResuming,
		docEntity.DocStatusUpdateFailResuming,
		docEntity.DocStatusCreateIndexFailResuming,
		docEntity.DocStatusExpiredCharExceeded,
		docEntity.DocStatusExpiredResuming,
		docEntity.DocStatusAppealFailedCharExceeded,
		docEntity.DocStatusAppealFailedResuming,
	}

	db, err := knowClient.GormClient(ctx, tableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return entity.CapacityUsage{}, err
	}
	session := mysqlquery.Use(db).TDoc.WithContext(ctx).Debug()

	if err := session.Where(tbl.RobotID.Eq(robotID), tbl.CorpID.Eq(corpID), tbl.IsDeleted.Is(false), tbl.Status.NotIn(exceededStatus...)).
		Select(tbl.CharSize.Sum().IfNull(0).As("char_size"), tbl.FileSize.Sum().IfNull(0).As("file_size")).Scan(&result); err != nil {
		logx.E(ctx, "Failed to GetRobotDocUsage, err:%+v", err)
		return entity.CapacityUsage{}, err
	}

	logx.D(ctx, "GetRobotDocUsage char_size: %d, file_size: %d", result.CharSize, result.FileSize)
	return entity.CapacityUsage{
		CharSize:          int64(result.CharSize),
		KnowledgeCapacity: int64(result.FileSize),
	}, nil
}

// GetRobotDocExceedUsage 获取机器人超量字符大小和文件大小
// todo cooper 关注下性能，看是否要优化下索引
func (d *daoImpl) GetRobotDocExceedUsage(ctx context.Context, corpID uint64, robotIDs []uint64) (
	map[uint64]entity.CapacityUsage, error) {
	/*
		getAppDocExceedCharSize := `
			SELECT
			    IFNULL(SUM(char_size), 0) as exceed_char_size,
			    IFNULL(SUM(file_size), 0) as exceed_file_size,
			    robot_id
			FROM
			    t_doc
			WHERE
				corp_id = ? AND is_deleted = ? AND robot_id IN (%s) AND status IN (%s)
			Group By
			    robot_id
		` */
	robotDocExceedSizeMap := make(map[uint64]entity.CapacityUsage)
	// robotsIDs为空时，该企业没有机器人返回空
	if len(robotIDs) == 0 {
		return robotDocExceedSizeMap, nil
	}
	exceededStatus := []uint32{
		docEntity.DocStatusCharExceeded,
		docEntity.DocStatusResuming,
		docEntity.DocStatusParseImportFailCharExceeded,
		docEntity.DocStatusAuditFailCharExceeded,
		docEntity.DocStatusUpdateFailCharExceeded,
		docEntity.DocStatusCreateIndexFailCharExceeded,
		docEntity.DocStatusParseImportFailResuming,
		docEntity.DocStatusAuditFailResuming,
		docEntity.DocStatusUpdateFailResuming,
		docEntity.DocStatusCreateIndexFailResuming,
		docEntity.DocStatusExpiredCharExceeded,
		docEntity.DocStatusExpiredResuming,
		docEntity.DocStatusAppealFailedCharExceeded,
		docEntity.DocStatusAppealFailedResuming,
	}

	tbl := d.mysql.TDoc
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, robotIDs[0], 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return robotDocExceedSizeMap, err
	}
	// 分批处理
	for _, batchRobotIDs := range slicex.Chunk(robotIDs, util.MaxSqlInCount) {
		if err := d.queryRobotDocExceedUsageBatch(ctx, db, corpID, batchRobotIDs, exceededStatus, robotDocExceedSizeMap); err != nil {
			return nil, err
		}
	}
	return robotDocExceedSizeMap, nil
}

// queryRobotDocExceedUsageBatch 分批查询机器人超量使用情况
func (d *daoImpl) queryRobotDocExceedUsageBatch(ctx context.Context, db *gorm.DB, corpID uint64,
	batchRobotIDs []uint64, exceededStatus []uint32, result map[uint64]entity.CapacityUsage) error {
	type AppDocExceedSize struct {
		RobotID        uint64 `gorm:"robot_id"`         // 应用ID
		ExceedCharSize uint64 `gorm:"exceed_char_size"` // 超量字符
		ExceedFileSize uint64 `gorm:"exceed_file_size"` // 超量文件大小
	}

	tbl := d.mysql.TDoc
	session := mysqlquery.Use(db).TDoc.WithContext(ctx).Debug()
	session = session.Where(
		tbl.CorpID.Eq(corpID),
		tbl.IsDeleted.Is(false),
		tbl.Status.In(exceededStatus...),
		tbl.RobotID.In(batchRobotIDs...),
	)

	rows, err := session.
		Select(tbl.CharSize.Sum().IfNull(0).As("exceed_char_size"),
			tbl.FileSize.Sum().IfNull(0).As("exceed_file_size"),
			tbl.RobotID).
		Group(tbl.RobotID).
		Rows()
	if err != nil {
		logx.E(ctx, "GetRobotDocExceedUsage: execute sql failed, err: %+v", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var appDocExceedSize AppDocExceedSize
		if err := rows.Scan(&appDocExceedSize.ExceedCharSize, &appDocExceedSize.ExceedFileSize, &appDocExceedSize.RobotID); err != nil {
			logx.E(ctx, "GetRobotDocExceedUsage: scan row failed, err: %+v", err)
			return err
		}
		// CharSize存储的是超额字符数，KnowledgeCapacity存储的是文件大小
		result[appDocExceedSize.RobotID] = entity.CapacityUsage{
			CharSize:          int64(appDocExceedSize.ExceedCharSize),
			KnowledgeCapacity: int64(appDocExceedSize.ExceedFileSize),
		}
	}
	return nil
}

func (d *daoImpl) GetDocListWithFilter(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter, db *gorm.DB) (
	[]*docEntity.Doc, error) {
	logx.D(ctx, "GetDocListWithFilter|selectColumns:%+v, filter:%+v", selectColumns, filter)
	docs := make([]*model.TDoc, 0)
	tbl := d.mysql.TDoc
	session := tbl.WithContext(ctx).Debug()
	if db != nil {
		session = mysqlquery.Use(db).TDoc.WithContext(ctx).Debug()
	}
	// if filter.Limit == 0 {
	//	// 为0正常返回空结果即可
	//	return docs, nil
	// }
	if filter.Limit > docEntity.DocTableMaxPageSize {
		// 限制单次查询最大条数
		filter.Limit = docEntity.DocTableMaxPageSize
		// err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		// logx.E(ctx, "GetDocList err: %+v", err)
		// return docs, err
	}

	if len(selectColumns) > 0 {
		session = session.Select(d.getTDocGenFields(selectColumns)...)
	} else {
		session = session.Select(d.getTDocGenFields(docEntity.DocTblColList)...)
	}

	session = d.generateGenCondition(ctx, session, filter)

	if filter.Limit > 0 {
		session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	}

	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		if orderField, ok := tbl.GetFieldByName(orderColumn); ok {
			if filter.OrderDirection[i] == util.SqlOrderByAsc {
				session = session.Order(orderField.Asc())
			} else {
				session = session.Order(orderField.Desc())
			}
		}
	}

	docs, err := session.Find()
	if err != nil {
		logx.E(ctx, "execute sql failed, err: %+v", err)
		return nil, err
	}
	return BatchConvertDocPOToDO(docs), nil
}

// GetDocCount 获取文档总数
func (d *daoImpl) GetDocCountWithFilter(ctx context.Context, selectColumns []string,
	filter *docEntity.DocFilter, db *gorm.DB) (int64, error) {
	tb := d.mysql.TDoc

	session := tb.WithContext(ctx)
	if db != nil {
		session = mysqlquery.Use(db).TDoc.WithContext(ctx)
	}

	session = session.Select(tb.ID.Count())

	session = d.generateGenCondition(ctx, session, filter)
	var count int64
	err := session.Scan(&count)
	if err != nil {
		logx.E(ctx, "[GetDocCountWithFilter] execute sql failed, err: %+v", err)
		return 0, err
	}
	return count, nil
}

// GetDocCountAndList 获取文档总数和分页列表
func (d *daoImpl) GetDocCountAndList(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) (
	[]*docEntity.Doc, int64, error) {
	tableName := d.mysql.TDoc.TableName()
	db, err := knowClient.GormClient(ctx, tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, 0, err
	}
	count, err := d.GetDocCountWithFilter(ctx, selectColumns, filter, db)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocListWithFilter(ctx, selectColumns, filter, db)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetTxDocAutoRefreshList 获取需要自动刷新腾讯文档
func (d *daoImpl) GetDocAutoRefreshList(ctx context.Context, nextUpdateTime time.Time) ([]*docEntity.Doc, error) {
	tbl := d.mysql.TDoc
	source := docEntity.RefreshSourceList
	docFilter := &docEntity.DocFilter{
		Source:         source,
		NextUpdateTime: nextUpdateTime,
		IsDeleted:      ptrx.Bool(false),
		Opts:           []uint32{docEntity.DocOptDocImport},
	}
	selectColumns := slicex.Map(tbl.Columns(), func(col field.Expr) string {
		return col.ColumnName().String()
	})

	// DocTblColList
	docs, err := d.GetDocList(ctx, selectColumns, docFilter)
	if err != nil {
		return nil, err
	}
	return docs, nil
}

// GetDiffDocs 获取需要diff的Doc
func (d *daoImpl) GetDiffDocs(ctx context.Context, filter *docEntity.DocFilter) ([]*docEntity.Doc,
	error) {
	tbl := d.mysql.TDoc
	beginTime := time.Now()
	offset := 0
	limit := docEntity.DocTableMaxPageSize
	allDocs := make([]*docEntity.Doc, 0)
	for {
		filter.Offset = offset
		filter.Limit = limit

		docs, err := d.GetDocList(ctx, []string{tbl.ID.ColumnName().String(),
			tbl.BusinessID.ColumnName().String(), tbl.RobotID.ColumnName().String(), tbl.Status.ColumnName().String(),
			tbl.FileName.ColumnName().String(), tbl.FileType.ColumnName().String(), tbl.FileNameInAudit.ColumnName().String()},
			filter)
		if err != nil {
			logx.E(ctx, "GetAllDocQas failed, err: %+v", err)
			return nil, err
		}
		allDocs = append(allDocs, docs...)
		if len(docs) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}
	logx.D(ctx, "GetDocDiffURL count:%d cost:%dms", len(allDocs), time.Since(beginTime).Milliseconds())
	return allDocs, nil
}

func (d *daoImpl) GetDocByDocFilter(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter,
	tx *gorm.DB) (*docEntity.Doc, error) {
	tbl := d.mysql.TDoc
	session := tbl.WithContext(ctx)
	if tx != nil {
		session = mysqlquery.Use(tx).TDoc.WithContext(ctx)
	}

	if len(selectColumns) > 0 {
		session = session.Select(d.getTDocGenFields(selectColumns)...)
	}

	session = d.generateGenCondition(ctx, session, filter)
	if doc, err := session.First(); err != nil {
		return nil, err
	} else {
		return ConvertDocPOToDO(doc), nil
	}
}

func (d *daoImpl) GetDocByIDs(ctx context.Context, ids []uint64, robotID uint64) (
	map[uint64]*docEntity.Doc, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc
			WHERE
			    id IN (%s)
		`
	*/

	docs := make(map[uint64]*docEntity.Doc, 0)
	if len(ids) == 0 {
		return docs, nil
	}

	tbl := d.mysql.TDoc
	db, err := knowClient.GormClient(ctx, model.TableNameTDoc, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}

	tDocs := make([]*model.TDoc, 0)

	if err := db.Table(model.TableNameTDoc).
		Where(tbl.ID.In(ids...)).Scan(&tDocs).Error; err != nil {
		return nil, err
	}
	for _, tDoc := range tDocs {
		docs[tDoc.ID] = ConvertDocPOToDO(tDoc)
	}
	return docs, nil
}

// GetAllDocs 获取所有文档
func (d *daoImpl) GetAllDocs(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) ([]*docEntity.Doc, error) {
	beginTime := time.Now()
	offset := 0
	limit := docEntity.DocTableMaxPageSize
	allDocs := make([]*docEntity.Doc, 0)
	tbl := d.mysql.TDoc
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return allDocs, err
	}
	for {
		filter.Offset = offset
		filter.Limit = limit

		docs, err := d.GetDocListWithFilter(ctx, selectColumns, filter, db)
		if err != nil {
			logx.E(ctx, "GetAllDocs failed, err: %+v", err)
			return nil, err
		}
		allDocs = append(allDocs, docs...)
		if len(docs) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}
	logx.D(ctx, "GetAllDocs count:%d cost:%dms",
		len(allDocs), time.Since(beginTime).Milliseconds())
	return allDocs, nil
}

func (d *daoImpl) DeleteDocByTx(ctx context.Context, filter *docEntity.DocFilter, doc *docEntity.Doc, tx *gorm.DB) error {
	tbl := d.mysql.TDoc
	tableName := tbl.TableName()
	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(tableName).Where("id = ?", doc.ID).Limit(1).Delete(&model.TDoc{})
	if err := session.Error; err != nil {
		return err
	}
	return nil
}

// UpdateDocStatus 更新文档状态（Gen风格）
func (d *daoImpl) UpdateDocStatus(ctx context.Context, id, robotId uint64, status uint32) error {
	// 1. 设置更新时间
	gormClient, err := knowClient.GormClient(ctx, docEntity.DocTableName, robotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateDocStatus get GormClient failed, err: %+v", err)
		return err
	}
	tbl := mysqlquery.Use(gormClient).TDoc
	// 3. 执行类型安全更新
	res, err := tbl.
		WithContext(ctx).
		Where(
			tbl.ID.Eq(id),
			tbl.RobotID.Eq(robotId),
		).Updates(map[string]interface{}{
		tbl.Status.ColumnName().String():     status,
		tbl.UpdateTime.ColumnName().String(): time.Now(),
	})

	// 4. 错误处理
	if err != nil {
		return fmt.Errorf("UpdateDocStatus: docID=%d, status=%d, err=%v",
			id, status, err)
	}
	// 4. 检查实际更新行数
	if res.RowsAffected == 0 {
		logx.W(ctx, "UpdateDocStatus RowsAffected = 0 robotID=%d", robotId)
		return errx.ErrNotFound
	}
	return nil
}

// GetDocsByCursor 游标分页获取文档（用于导出）
// 使用索引 idx_corp_robot_opt (corp_id, robot_id, is_deleted, opt)
// 排序规则: ORDER BY id ASC
// 游标条件: id > lastID
func (d *daoImpl) GetDocsByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int) ([]*docEntity.Doc, error) {
	tbl := d.mysql.TDoc
	tableName := tbl.TableName()

	session, err := knowClient.GormClient(ctx, tableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocsByCursor get GormClient failed, err: %v", err)
		return nil, err
	}

	var tDocs []*model.TDoc
	// 使用索引 idx_corp_robot_opt (corp_id, robot_id, is_deleted, opt)
	// 游标条件: id > lastID
	err = session.WithContext(ctx).Table(tableName).
		Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, corpID).
		Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, false).
		Where(tbl.ID.ColumnName().String()+util.SqlMore, lastID).
		Order(tbl.ID.ColumnName().String() + " " + util.SqlOrderByAsc).
		Limit(limit).
		Find(&tDocs).Error
	if err != nil {
		logx.E(ctx, "GetDocsByCursor query failed, err: %v", err)
		return nil, err
	}

	return BatchConvertDocPOToDO(tDocs), nil
}
