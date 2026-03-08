package qa

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"github.com/spf13/cast"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) getDocQaGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TDocQa.WithContext(ctx).UnderlyingDB()
	}
	return d.Query().TDocQa.WithContext(ctx).UnderlyingDB()
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateCondition(ctx context.Context, session *gorm.DB, filter *qaEntity.DocQaFilter) *gorm.DB {
	tbl := d.Query().TDocQa
	if filter.QAId != 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlEqual, filter.QAId)
	}
	if len(filter.QAIds) > 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlIn, filter.QAIds)
	}
	if filter.CorpId != 0 {
		session = session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, filter.CorpId)
	}
	if filter.RobotId != 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.RobotId)
	}
	if filter.BusinessId != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlEqual, filter.BusinessId)
	}
	if len(filter.BusinessIds) != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, filter.BusinessIds)
	}

	if len(filter.CategoryIds) != 0 {
		// 分类
		session = session.Where(tbl.CategoryID.ColumnName().String()+util.SqlIn, filter.CategoryIds)
	}

	if filter.DocID != 0 {
		session = session.Where(tbl.DocID.ColumnName().String()+util.SqlEqual, filter.DocID)
	}
	if len(filter.DocIDs) != 0 {
		session = session.Where(tbl.DocID.ColumnName().String()+util.SqlIn, filter.DocIDs)
	}
	if filter.OriginDocID != 0 {
		session = session.Where(tbl.OriginDocID.ColumnName().String()+util.SqlEqual, filter.OriginDocID)
	}
	if len(filter.SegmentIDs) != 0 {
		session = session.Where(tbl.SegmentID.ColumnName().String()+util.SqlIn, filter.SegmentIDs)
	}
	if len(filter.RobotIDs) != 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlIn, filter.RobotIDs)
	}

	if len(filter.ReleaseStatusList) != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(tbl.ReleaseStatus.ColumnName().String()+util.SqlIn, filter.ReleaseStatusList)
	}

	if filter.ReleaseStatusNot != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(tbl.ReleaseStatus.ColumnName().String()+util.SqlNotEqual, filter.ReleaseStatusNot)
	}
	if filter.AcceptStatus != 0 {
		session = session.Where(tbl.AcceptStatus.ColumnName().String()+util.SqlEqual, filter.AcceptStatus)
	}

	if filter.SimilarStatus != nil {
		session = session.Where(tbl.SimilarStatus.ColumnName().String()+util.SqlEqual, *filter.SimilarStatus)
	}

	// 查询当前未发布(不包括未发布删除的)和已发布修改过(包括已发布后进行删除、修改)的数量
	if filter.ReleaseCount {
		// session = session.Where(DocQaTblColNextAction+sqlNotEqual, filter.NotNextAction)
		// 添加复杂条件: (next_action != 1 AND is_deleted = 2) or (is_deleted = 1)
		session = session.Where(
			"("+tbl.NextAction.ColumnName().String()+util.SqlNotEqual+" AND "+tbl.IsDeleted.ColumnName().String()+util.SqlEqual+") OR ("+
				tbl.IsDeleted.ColumnName().String()+util.SqlEqual+")",
			releaseEntity.ReleaseActionAdd, qaEntity.QAIsDeleted, qaEntity.QAIsNotDeleted,
		)
	}

	if filter.EnableScope != nil {
		session = session.Where(tbl.EnableScope.ColumnName().String()+util.SqlEqual, *filter.EnableScope)
	}

	if filter.Question != "" {
		session = session.Where(tbl.Question.ColumnName().String()+util.SqlLike, "%"+filter.Question+"%")
	}

	if len(filter.ActionList) > 0 {
		session = session.Where(tbl.NextAction.ColumnName().String()+util.SqlIn, filter.ActionList)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}

	if !filter.MaxUpdateTime.IsZero() {
		session = session.Where(tbl.UpdateTime.ColumnName().String()+util.SqlLess, filter.MaxUpdateTime)
	}

	if !filter.MinUpdateTime.IsZero() {
		session = session.Where(tbl.UpdateTime.ColumnName().String()+util.SqlMore, filter.MinUpdateTime)
	}

	if !filter.MinEqCreateTime.IsZero() {
		session = session.Where(tbl.CreateTime.ColumnName().String()+util.SqlMoreEqual, filter.MinEqCreateTime)
	}

	if filter.ExtraCondition != "" {
		if len(filter.ExtraParams) > 0 {
			session = session.Where(filter.ExtraCondition, filter.ExtraParams...)
		} else {
			session = session.Where(filter.ExtraCondition)
		}
	}
	return session
}

// func (d *daoImpl) GetDocQas(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error) {
// 	return
// }

func (d *daoImpl) GetDocQasByPagenation(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter, pagenation bool) (
	[]*qaEntity.DocQA, error) {
	if filter.RawQuery != "" {
		return d.GetDocQasByRawQueryWithTx(ctx, filter.RawQuery, filter.RawQueryArgs, nil)
	}
	return d.GetDocQasByPagenationWithTx(ctx, selectColumns, filter, pagenation, nil)
}

func (d *daoImpl) GetDocQasByRawQueryWithTx(ctx context.Context, rawQuery string, rawParams []any, tx *gorm.DB) ([]*qaEntity.DocQA, error) {
	docQas := make([]*model.TDocQa, 0)
	db := d.Query().TDocQa
	docQaTableName := db.TableName()
	session := tx

	if session == nil {
		session = db.WithContext(ctx).UnderlyingDB()
	}

	dbRes := session.Table(docQaTableName).Raw(rawQuery, rawParams...).Scan(&docQas)

	if dbRes.Error != nil {
		logx.E(ctx, "[GetDocQasByRawQueryWithTx] execute sql failed, sql:%s, err: %+v", rawQuery, dbRes.Error)
		return nil, dbRes.Error
	}
	return BatchConvertDocQAPOToDO(docQas), nil
}

// GetDocQasByPagenationWithTx 获取问答对
func (d *daoImpl) GetDocQasByPagenationWithTx(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter, pagenation bool, tx *gorm.DB) ([]*qaEntity.DocQA, error) {
	docQas := make([]*model.TDocQa, 0)
	db := d.Query().TDocQa
	docQaTableName := db.TableName()

	session := tx

	if session == nil {
		session = db.WithContext(ctx).UnderlyingDB()

	}
	session = session.Table(docQaTableName)

	session = d.generateCondition(ctx, session, filter)

	if len(selectColumns) > 0 {
		session = session.Select(selectColumns)
	}

	if pagenation {
		if filter.Limit == 0 || filter.Limit > qaEntity.DocQaTableMaxPageSize {
			filter.Limit = qaEntity.DocQaTableMaxPageSize
		}
		session = session.Offset(filter.Offset).Limit(filter.Limit)
	}

	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session = session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docQas)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocQAPOToDO(docQas), nil
}

// GetDocQaCount 获取问答总数
func (d *daoImpl) GetDocQaCountWithTx(ctx context.Context, selectColumns []string,
	filter *qaEntity.DocQaFilter, tx *gorm.DB) (int64, error) {
	rawDB := d.Query().TDocQa
	docQaTableName := rawDB.TableName()
	session := tx
	if session == nil {
		session = rawDB.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(docQaTableName)
	session = d.generateCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

func (d *daoImpl) GetDocQANum(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter, tx *gorm.DB) ([]*qaEntity.DocQANum, error) {
	/*
			`
			SELECT
				doc_id,is_deleted,count(*) as total
			FROM
			    t_doc_qa
			WHERE
			    corp_id = ? AND robot_id = ? AND doc_id IN (%s)
			GROUP BY doc_id,is_deleted
		`
	*/
	rawDB := d.Query().TDocQa
	docQaTableName := rawDB.TableName()
	session := tx
	if session == nil {
		session = rawDB.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(docQaTableName)

	session = d.generateCondition(ctx, session, filter)
	docQaNums := []*qaEntity.DocQANum{}

	if len(selectColumns) > 0 {
		elems := make([]string, 0)
		for _, col := range selectColumns {
			elems = append(elems, col+" as "+col)
		}
		elems = append(elems, "count(*) as total")
		session = session.Select(strings.Join(elems, ","))
		for _, col := range selectColumns {
			session = session.Group(col)
		}
	}

	if err := session.Scan(&docQaNums).Error; err != nil {
		logx.E(ctx, "GetDocQANum execute sql failed, err: %+v", err)
		return nil, err
	}

	return docQaNums, nil
}

func (d *daoImpl) GetDocQaList(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error) {
	rawDB := d.Query().TDocQa.WithContext(ctx).UnderlyingDB()

	logx.D(ctx, "GetDocQaList filter:%+v", filter)
	allDocQaList := make([]*qaEntity.DocQA, 0)
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
		docQaList, err := d.GetDocQasByPagenationWithTx(ctx, selectColumns, filter, true, rawDB)
		if err != nil {
			logx.E(ctx, "GetDocQaList failed, err: %+v", err)
			return nil, err
		}
		allDocQaList = append(allDocQaList, docQaList...)
		if len(docQaList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocQaList count:%d cost:%dms",
		len(allDocQaList), time.Since(beginTime).Milliseconds())
	return allDocQaList, nil
}

// GetAllDocQas 获取所有问答对
func (d *daoImpl) GetAllDocQas(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error) {
	beginTime := time.Now()
	offset := 0
	limit := qaEntity.DocQaTableMaxPageSize
	allDocQas := make([]*qaEntity.DocQA, 0)
	for {
		filter.Offset = offset
		filter.Limit = limit

		docQas, err := d.GetDocQasByPagenation(ctx, selectColumns, filter, true)
		if err != nil {
			logx.E(ctx, "GetAllDocQas failed, err: %+v", err)
			return nil, err
		}
		allDocQas = append(allDocQas, docQas...)
		if len(docQas) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}
	logx.D(ctx, "GetAllDocQas count:%d cost:%dms",
		len(allDocQas), time.Since(beginTime).Milliseconds())
	return allDocQas, nil
}

func (d *daoImpl) BatchUpdateDocQAReleaseStatusByID(ctx context.Context, docQAID []uint64, releaseStatus uint32, tx *gorm.DB) error {
	tbl := d.Query().TDocQa

	db := d.getDocQaGormDB(ctx, tx)

	if len(docQAID) == 0 {
		logx.I(ctx, "no doc qa record to update")
		return nil
	}

	res := db.
		Table(tbl.TableName()).
		Where(tbl.ID.ColumnName().String()+util.SqlIn, docQAID).
		Updates(map[string]any{
			tbl.ReleaseStatus.ColumnName().String(): releaseStatus,
			tbl.UpdateTime.ColumnName().String():    time.Now(),
		})

	if res.Error != nil {
		logx.E(ctx, "BatchUpdateDocQAReleaseStatusByID failed,  error:%v", res.Error)
		return res.Error
	}

	logx.I(ctx, "BatchUpdateDocQAReleaseStatusByID success")
	return nil
}

func (d *daoImpl) BatchUpdateDocQALastActionByID(ctx context.Context,
	docQAID []uint64, action uint32) error {
	tbl := d.Query().TDocQa
	db := tbl.WithContext(ctx).Debug()
	if len(docQAID) == 0 {
		logx.I(ctx, "no doc qa record to update")
		return nil
	}

	queryCond := []gen.Condition{
		tbl.ID.In(docQAID...),
	}

	info, err := db.Where(queryCond...).
		Updates(map[string]any{
			tbl.NextAction.ColumnName().String(): action,
			tbl.UpdateTime.ColumnName().String(): time.Now(),
		})

	if err != nil {
		logx.E(ctx, "BatchUpdateDocQALastActionByID failed,  error:%v", err)
		return err
	}

	logx.I(ctx, "BatchUpdateDocQALastActionByID success, info:%+v", info)
	return nil
}

// 创建Qa记录
func (d *daoImpl) CreateDocQa(ctx context.Context, docQa *qaEntity.DocQA) error {
	/*
		`
			INSERT INTO
				t_doc_qa (%s)
			VALUES
				(null,:business_id,:robot_id,:corp_id,:staff_id,:doc_id,:origin_doc_id,:segment_id,:category_id,:source,
				:question,:answer,:custom_param,:question_desc,:release_status,:is_audit_free,:is_deleted,:message,:accept_status,
				:next_action,:similar_status,:char_size,:attr_range,:create_time,:update_time,:expire_start,:expire_end,:attribute_flag)
		`
	*/

	tbl := d.Query().TDocQa
	db := tbl.WithContext(ctx).UnderlyingDB()

	tDocQa := ConvertDocQADOToPO(docQa)
	res := db.Create(tDocQa)
	if res.Error != nil {
		logx.E(ctx, "CreateDocQa failed, qa: %+v, err: %+v", docQa, res.Error)
		return res.Error
	}
	docQa.ID = tDocQa.ID
	return nil
}

func (d *daoImpl) UpdateDocQasWithTx(ctx context.Context, updateColumns []string, filter *qaEntity.DocQaFilter, docQa *qaEntity.DocQA, tx *gorm.DB) (int64, error) {
	rawDB := d.Query().TDocQa
	docQaTableName := rawDB.TableName()

	session := d.getDocQaGormDB(ctx, tx)
	session = session.Table(docQaTableName)

	if len(updateColumns) > 0 {
		session = session.Select(updateColumns)
	}

	session = d.generateCondition(ctx, session, filter)

	tDocQa := ConvertDocQADOToPO(docQa)

	res := session.Updates(tDocQa)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocQas failed, col: %v, param: %+v, qa: %+v, err: %+v",
			updateColumns, filter, docQa, res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil

}

// UpdateDocQas 更新doc qa
func (d *daoImpl) UpdateDocQas(ctx context.Context, updateColumns []string, filter *qaEntity.DocQaFilter, docQa *qaEntity.DocQA) error {
	_, err := d.UpdateDocQasWithTx(ctx, updateColumns, filter, docQa, nil)
	return err

}

func (d *daoImpl) BatchUpdateDocQA(ctx context.Context, filter *qaEntity.DocQaFilter, updateFields map[string]any, tx *gorm.DB) (
	int64, error) {
	if updateFields == nil || len(updateFields) == 0 {
		logx.E(ctx, "BatchUpdateDocQA skipped, updateFields is empty")
		return 0, nil
	}
	db := d.getDocQaGormDB(ctx, tx)

	db = db.Table(d.Query().TDocQa.TableName())
	db = d.generateCondition(ctx, db, filter)

	info := db.Updates(updateFields)
	err := info.Error

	if err != nil {
		logx.E(ctx, "BatchUpdateDocQA failed,  error:%v", err)
		return 0, err
	}
	return info.RowsAffected, nil
}

func (d *daoImpl) GetQaByFilterWithTx(ctx context.Context, selectColumns []string,
	filter *qaEntity.DocQaFilter, tx *gorm.DB) (*qaEntity.DocQA, error) {
	tbl := d.Query().TDocQa
	db := tbl.WithContext(ctx).UnderlyingDB()
	if tx != nil {
		db = mysqlquery.Use(tx).TDocQa.WithContext(ctx).UnderlyingDB()
	}
	db = db.Table(tbl.TableName()).Select(selectColumns)
	db = d.generateCondition(ctx, db, filter)

	tDocQA := &model.TDocQa{}

	if err := db.Take(tDocQA).Error; err != nil {
		logx.E(ctx, "GetQaByFilterWithTx failed, err: %+v", err)
		return nil, err
	}
	return ConvertDocQAPOToDO(tDocQA), nil
}

func (d *daoImpl) GetQAByID(ctx context.Context, docQAID uint64) (*qaEntity.DocQA, error) {
	/*
		`
			SELECT
				id,business_id,robot_id,corp_id,staff_id,doc_id,origin_doc_id,segment_id,category_id,source,question,answer,
				custom_param,question_desc,release_status,is_audit_free,is_deleted,message,accept_status,next_action,
				similar_status,char_size,attr_range,create_time,update_time,expire_start,expire_end,attribute_flag
			FROM
			    t_doc_qa
			WHERE
			    id = ?
	*/
	if docQAID == 0 {
		logx.I(ctx, "no doc qa record to get")
		return nil, nil
	}

	tbl := d.mysql.TDocQa
	queryCond := []gen.Condition{
		tbl.ID.Eq(docQAID),
	}

	db := tbl.WithContext(ctx).Debug()
	if row, err := db.Where(queryCond...).Take(); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logx.I(ctx, "GetQAByID, no record found, docQAID: %d", docQAID)
			return nil, nil
		}
		logx.E(ctx, "GetQAByID failed,  error:%+v", err)
		return nil, err
	} else {
		return ConvertDocQAPOToDO(row), nil
	}
}

// GetQAsByBizIDs 根据业务ID获取问答
func (d *daoImpl) GetQAsByBizIDs(
	ctx context.Context, corpID, robotID uint64, qaBizIDs []uint64, offset, limit int,
) ([]*qaEntity.DocQA, error) {
	/*
		 `
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			     corp_id = ? AND robot_id = ? AND accept_status = ? AND is_deleted = ? AND business_id IN(?)
			ORDER BY
			    id ASC
			LIMIT ?,?
		`
	*/
	if len(qaBizIDs) == 0 {
		return nil, nil
	}

	delFlag := qaEntity.QAIsNotDeleted

	filter := &qaEntity.DocQaFilter{
		CorpId:       corpID,
		RobotId:      robotID,
		AcceptStatus: qaEntity.AcceptYes,
		IsDeleted:    &delFlag,
		BusinessIds:  qaBizIDs,

		OrderColumn:    []string{qaEntity.DocQaTblColId},
		OrderDirection: []string{"ASC"},
		Offset:         offset,
		Limit:          limit,
	}

	qas, err := d.GetDocQasByPagenation(ctx, qaEntity.DocQaTblColList, filter, true)

	if err != nil {
		logx.E(ctx, "Failed to GetQAsByBizIDs, err: %v", err)
		return nil, err
	}
	return qas, nil
}

// GetReleaseQACount 获取发布QA总数
func (d *daoImpl) GetReleaseQACount(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, actions []uint32) (uint64, error) {
	/*
			`
			SELECT
				count(*)
			FROM
			    t_doc_qa
			WHERE
			    corp_id = ?
			  	AND robot_id = ?
			    AND accept_status != ?
			    AND release_status = ?
				AND !(next_action = ? AND is_deleted = ?)
			    AND !(next_action = ? AND accept_status = ?) %s
		`
	*/

	tbl := d.Query().TDocQa

	q := tbl.WithContext(ctx)
	if corpID != 0 {
		q = q.Where(tbl.CorpID.Eq(corpID))
	}
	if robotID != 0 {
		q = q.Where(tbl.RobotID.Eq(robotID))
	}
	if question != "" {
		q = q.Where(tbl.Question.Like(fmt.Sprintf("%%%s%%", question)))
	}
	if !startTime.IsZero() {
		q = q.Where(tbl.UpdateTime.Gte(startTime))
	}
	if !endTime.IsZero() {
		q = q.Where(tbl.UpdateTime.Lte(endTime))
	}
	if len(actions) > 0 {
		q = q.Where(tbl.NextAction.In(actions...))
	}
	q = q.Where(tbl.AcceptStatus.Neq(qaEntity.AcceptInit)).
		Where(tbl.ReleaseStatus.Eq(qaEntity.QAReleaseStatusInit)).
		Where(field.Or(tbl.NextAction.Neq(qaEntity.NextActionAdd), tbl.IsDeleted.Neq(qaEntity.QAIsDeleted))).
		Where(field.Or(tbl.NextAction.Neq(qaEntity.NextActionAdd), tbl.AcceptStatus.Neq(qaEntity.AcceptNo)))

	count, err := q.Count()

	if err != nil {
		logx.E(ctx, "GetReleaseQACount failed, err: %+v", err)
		return 0, err
	}
	return uint64(count), nil
}

// GetReleaseQAList 获取发布问答对列表
func (d *daoImpl) GetReleaseQAList(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, actions []uint32, page, pageSize uint32) (
	[]*qaEntity.DocQA, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			    corp_id = ?
				AND robot_id = ?
				AND accept_status != ?
				AND release_status = ?
				AND !(next_action = ? AND is_deleted = ?)
			    AND !(next_action = ? AND accept_status = ?) %s
			LIMIT ?,?
		`

	*/

	tbl := d.Query().TDocQa

	q := tbl.WithContext(ctx)
	if corpID != 0 {
		q = q.Where(tbl.CorpID.Eq(corpID))
	}
	if robotID != 0 {
		q = q.Where(tbl.RobotID.Eq(robotID))
	}
	if question != "" {
		q = q.Where(tbl.Question.Like(fmt.Sprintf("%%%s%%", question)))
	}
	if !startTime.IsZero() {
		q = q.Where(tbl.UpdateTime.Gte(startTime))
	}
	if !endTime.IsZero() {
		q = q.Where(tbl.UpdateTime.Lte(endTime))
	}
	if len(actions) > 0 {
		q = q.Where(tbl.NextAction.In(actions...))
	}

	q = q.Where(tbl.AcceptStatus.Neq(qaEntity.AcceptInit)).
		Where(tbl.ReleaseStatus.Eq(qaEntity.QAReleaseStatusInit)).
		Where(field.Or(tbl.NextAction.Neq(qaEntity.NextActionAdd), tbl.IsDeleted.Neq(qaEntity.QAIsDeleted))).
		Where(field.Or(tbl.NextAction.Neq(qaEntity.NextActionAdd), tbl.AcceptStatus.Neq(qaEntity.AcceptNo)))

	docQas := make([]*model.TDocQa, 0)

	offset, limit := utilx.Page(page, pageSize)

	docQas, err := q.Offset(int(offset)).Limit(int(limit)).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseQAList failed, err: %+v", err)
		return nil, err
	}
	return BatchConvertDocQAPOToDO(docQas), nil
}

func (d *daoImpl) GetRobotQAUsage(ctx context.Context, robotID uint64, corpID uint64) (entity.CapacityUsage, error) {

	/*
		`
			SELECT
				IFNULL(SUM(char_size), 0) as char_size, IFNULL(SUM(qa_size), 0) as qa_size
			FROM
			    t_doc_qa
			WHERE
			    robot_id = ?
			AND corp_id = ?
			AND is_deleted = ?
			AND accept_status = ?
			AND release_status NOT IN (%s)
		`
	*/

	exceededStatus := []uint32{
		qaEntity.QAReleaseStatusCharExceeded,
		qaEntity.QAReleaseStatusResuming,
		qaEntity.QAReleaseStatusAppealFailCharExceeded,
		qaEntity.QAReleaseStatusAppealFailResuming,
		qaEntity.QAReleaseStatusAuditNotPassCharExceeded,
		qaEntity.QAReleaseStatusAuditNotPassResuming,
		qaEntity.QAReleaseStatusAuditNotPassCharExceeded,
		qaEntity.QAReleaseStatusLearnFailResuming,
	}

	tbl := d.mysql.TDocQa
	session := tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())

	session = session.Where(tbl.RobotID.Eq(robotID)).Where(tbl.CorpID.Eq(corpID)).
		Where(tbl.IsDeleted.Eq(qaEntity.QAIsNotDeleted)).
		Where(tbl.AcceptStatus.Eq(qaEntity.AcceptYes)).
		Where(tbl.ReleaseStatus.NotIn(exceededStatus...))

	type Result struct {
		CharSize uint64
		QaSize   uint64
	}
	var result Result
	err := session.Select("IFNULL(SUM(char_size), 0) as char_size, IFNULL(SUM(qa_size), 0) as qa_size").
		Scan(&result).Error
	if err != nil {
		logx.E(ctx, "GetRobotQAUsage failed, err: %+v", err)
		return entity.CapacityUsage{}, err
	}
	logx.D(ctx, "GetRobotQAUsage char_size: %d, qa_size: %d", result.CharSize, result.QaSize)
	return entity.CapacityUsage{
		CharSize:          int64(result.CharSize),
		KnowledgeCapacity: int64(result.QaSize),
	}, nil
}

func (d *daoImpl) GetRobotQAExceedUsage(ctx context.Context, corpID uint64, appPrimaryIds []uint64) (map[uint64]entity.CapacityUsage, error) {
	/*
		`
			SELECT
				IFNULL(SUM(char_size), 0) as exceed_char_size, IFNULL(SUM(qa_size), 0) as qa_size, robot_id
			FROM
			    t_doc_qa
			WHERE
			    corp_id = ? AND is_deleted = ? AND accept_status = ? AND robot_id IN (%s) AND release_status IN (%s)
			Group By
			    robot_id
		`
	*/

	exceededStatus := []uint32{
		qaEntity.QAReleaseStatusCharExceeded,
		qaEntity.QAReleaseStatusResuming,
		qaEntity.QAReleaseStatusAppealFailCharExceeded,
		qaEntity.QAReleaseStatusAppealFailResuming,
		qaEntity.QAReleaseStatusAuditNotPassCharExceeded,
		qaEntity.QAReleaseStatusAuditNotPassResuming,
		qaEntity.QAReleaseStatusLearnFailCharExceeded,
		qaEntity.QAReleaseStatusLearnFailResuming,
	}
	tbl := d.mysql.TDocQa
	session := tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())

	session = session.Where(tbl.CorpID.Eq(corpID)).
		Where(tbl.IsDeleted.Eq(qaEntity.QAIsNotDeleted)).
		Where(tbl.AcceptStatus.Eq(qaEntity.AcceptYes)).
		Where(tbl.ReleaseStatus.In(exceededStatus...))

	if len(appPrimaryIds) > 0 {
		session = session.Where(tbl.RobotID.In(appPrimaryIds...))
	}

	res := make(map[uint64]entity.CapacityUsage)
	rows, err := session.
		Select("IFNULL(SUM(char_size), 0) as exceed_char_size, IFNULL(SUM(qa_size), 0) as qa_size, robot_id").
		Group(tbl.RobotID.ColumnName().String()).Rows()

	if err != nil {
		logx.E(ctx, "GetRobotQAExceedUsage failed, err: %+v", err)
		return nil, err
	}

	for rows.Next() {
		var exceedCharSize uint64
		var qaSize uint64
		var robotID uint64
		err := rows.Scan(&exceedCharSize, &qaSize, &robotID)
		if err != nil {
			logx.E(ctx, "GetRobotQAExceedUsage failed, err: %+v", err)
			return nil, err
		}
		res[robotID] = entity.CapacityUsage{
			CharSize:          int64(exceedCharSize),
			KnowledgeCapacity: int64(qaSize),
		}
	}
	return res, nil
}

// RecordUserAccessUnCheckQATime 记录访问未检验问答时间
func (d *daoImpl) RecordUserAccessUnCheckQATime(ctx context.Context, robotID, staffID uint64) error {
	key := fmt.Sprintf("qbot:admin:qa:%s:%s", cast.ToString(robotID), cast.ToString(staffID))
	val := time.Now().UnixMilli()
	// TODO(ericjwang): 这里不需要设置过期时间吗？重构前就没有设置
	err := d.adminRdb.Set(ctx, key, val, 0).Err()
	if err != nil {
		logx.E(ctx, "设置用户访问问答时间错误: %+v, key: %s", err, key)
		return fmt.Errorf("RecordUserAccessUnCheckQATime error: %w", err)
	}
	return nil
}

// GetQADetailsByReleaseStatus 根据发布状态获取QA详情（Gen风格）
func (d *daoImpl) GetQADetailsByReleaseStatus(ctx context.Context, corpID, robotID uint64, ids []uint64,
	releaseStatus uint32) (map[uint64]*qaEntity.DocQA, error) {
	/*
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND robot_id = ? AND release_status = ? AND id IN (%s)
	*/
	// 1. 参数校验
	if len(ids) == 0 {
		return nil, nil
	}
	// 2. 初始化Gen生成的Query
	q := d.mysql.TDocQa.WithContext(ctx)

	// 3. 构建类型安全查询（直接指定字段）
	docQAs, err := q.Where(
		d.mysql.TDocQa.CorpID.Eq(corpID),
		d.mysql.TDocQa.RobotID.Eq(robotID),
		d.mysql.TDocQa.ReleaseStatus.Eq(releaseStatus),
		d.mysql.TDocQa.ID.In(ids...), // 动态ID列表
	).
		Find()
	// 4. 错误处理
	if err != nil {
		return nil, fmt.Errorf("GetQADetailsByReleaseStatus failed : corpID=%d, robotID=%d, err=%v", corpID, robotID, err)
	}
	// 5. 结果转换
	if len(docQAs) == 0 {
		return nil, errx.ErrNotFound
	}
	qaMap := make(map[uint64]*qaEntity.DocQA, len(docQAs))
	for _, item := range docQAs {
		qaMap[item.ID] = ConvertDocQAPOToDO(item)
	}
	return qaMap, nil
}

// UpdateAppealQA 批量更新申诉问答状态（Gen风格）
func (d *daoImpl) UpdateAppealQA(ctx context.Context, qaIDs, simIDs []uint64,
	releaseStatus, isAuditFree uint32) error {
	// 1. 参数校验
	if len(qaIDs) == 0 && len(simIDs) == 0 {
		return nil
	}
	db := d.mysql.TDocQa.WithContext(ctx)
	// 2. 执行事务
	if err := db.UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		// 3. 更新主问答表
		if err := tx.Table(qaEntity.DocQaTableName).Where(d.mysql.TDocQa.ID.In(qaIDs...)).
			Updates(map[string]interface{}{
				d.mysql.TDocQa.ReleaseStatus.ColumnName().String(): releaseStatus,
				d.mysql.TDocQa.IsAuditFree.ColumnName().String():   isAuditFree,
				d.mysql.TDocQa.UpdateTime.ColumnName().String():    now,
			}).Error; err != nil {
			return fmt.Errorf("UpdateAppealQA t_doc_qa failed err : %v", err)
		}
		// 4. 更新相似问答表
		if err := tx.Table(qaEntity.QaSimilarQuestionTableName).Where(d.mysql.TQaSimilarQuestion.ID.In(simIDs...)).
			Updates(map[string]interface{}{
				d.mysql.TQaSimilarQuestion.ReleaseStatus.ColumnName().String(): releaseStatus,
				d.mysql.TQaSimilarQuestion.IsAuditFree.ColumnName().String():   isAuditFree,
				d.mysql.TQaSimilarQuestion.UpdateTime.ColumnName().String():    now,
			}).Error; err != nil {
			return fmt.Errorf("UpdateAppealQA t_qa_similar_question failed err : %v", err)
		}
		return nil
	}); err != nil {
		logx.E(ctx, "UpdateAppealQA failed err:%+v", err)
		return err
	}
	return nil
}

// GetQAsByCursor 游标分页获取QA（用于导出）
// 使用索引 idx_corp_robot (corp_id, robot_id, is_deleted)
func (d *daoImpl) GetQAsByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int) ([]*qaEntity.DocQA, error) {
	tbl := d.mysql.TDocQa
	tableName := tbl.TableName()

	var tDocQas []*model.TDocQa
	// 使用索引 idx_corp_robot (corp_id, robot_id, is_deleted)
	// 游标条件: id > lastID
	err := tbl.WithContext(ctx).UnderlyingDB().Table(tableName).
		Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, corpID).
		Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, qaEntity.QAIsNotDeleted).
		Where(tbl.ID.ColumnName().String()+util.SqlMore, lastID).
		Order(tbl.ID.ColumnName().String() + " " + util.SqlOrderByAsc).
		Limit(limit).
		Find(&tDocQas).Error
	if err != nil {
		logx.E(ctx, "GetQAsByCursor query failed, err: %v", err)
		return nil, err
	}

	return BatchConvertDocQAPOToDO(tDocQas), nil
}
