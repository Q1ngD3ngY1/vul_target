package release

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) getAuditGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TAudit.WithContext(ctx).UnderlyingDB()
	}
	return d.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB()
}

const (
	AuditTableMaxPageSize = 1000
)

func ConvertAuditPOToDO(do *model.TAudit) *releaseEntity.Audit {
	if do == nil {
		return nil
	}
	return &releaseEntity.Audit{
		ID:             do.ID,
		BusinessID:     do.BusinessID,
		CorpID:         do.CorpID,
		RobotID:        do.RobotID,
		CreateStaffID:  do.CreateStaffID,
		ParentID:       do.ParentID,
		Type:           uint32(do.Type),
		Params:         do.Params,
		RelateID:       uint64(do.RelateID),
		Status:         uint32(do.Status),
		RetryTimes:     uint32(do.RetryTimes),
		ETag:           do.ETag,
		Message:        do.Message,
		CreateTime:     do.CreateTime,
		UpdateTime:     do.UpdateTime,
		ParentRelateID: uint64(do.ParentRelateID),
	}
}

func BatchConvertAuditPOToDO(dos []*model.TAudit) []*releaseEntity.Audit {
	if len(dos) == 0 {
		return nil
	}
	auditList := make([]*releaseEntity.Audit, 0, len(dos))
	for _, do := range dos {
		auditList = append(auditList, ConvertAuditPOToDO(do))
	}
	return auditList
}

func ConvertAuditDOToPO(po *releaseEntity.Audit) *model.TAudit {
	if po == nil {
		return nil
	}
	return &model.TAudit{
		ID:             po.ID,
		BusinessID:     po.BusinessID,
		CorpID:         po.CorpID,
		RobotID:        po.RobotID,
		CreateStaffID:  po.CreateStaffID,
		ParentID:       po.ParentID,
		Type:           po.Type,
		Params:         po.Params,
		RelateID:       int64(po.RelateID),
		Status:         po.Status,
		RetryTimes:     int32(po.RetryTimes),
		ETag:           po.ETag,
		Message:        po.Message,
		CreateTime:     po.CreateTime,
		UpdateTime:     po.UpdateTime,
		ParentRelateID: int64(po.ParentRelateID),
	}
}

func BatchConvertAuditDOToPO(pos []*releaseEntity.Audit) []*model.TAudit {
	if len(pos) == 0 {
		return nil
	}
	auditList := make([]*model.TAudit, 0, len(pos))
	for _, do := range pos {
		auditList = append(auditList, ConvertAuditDOToPO(do))
	}
	return auditList
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateCondition(ctx context.Context, q mysqlquery.ITAuditDo, filter *releaseEntity.AuditFilter) mysqlquery.ITAuditDo {
	if filter == nil {
		return q
	}

	if filter.ID != 0 {
		q = q.Where(d.mysql.TAudit.ID.Eq(filter.ID))
	}

	if filter.IDMore != 0 {
		q = q.Where(d.mysql.TAudit.ID.Gt(filter.IDMore))
	}

	if filter.ParentRelatedID != 0 {
		q = q.Where(d.mysql.TAudit.ParentRelateID.Eq(filter.ParentRelatedID))
	}

	if filter.BusinessID != 0 {
		q = q.Where(d.mysql.TAudit.BusinessID.Eq(filter.BusinessID))
	}

	if len(filter.IDs) != 0 {
		q = q.Where(d.mysql.TAudit.ID.In(filter.IDs...))
	}

	if filter.CorpID != 0 {
		q = q.Where(d.mysql.TAudit.CorpID.Eq(filter.CorpID))
	}

	if filter.RobotID != 0 {
		q = q.Where(d.mysql.TAudit.RobotID.Eq(filter.RobotID))
	}

	if filter.ParentID != nil {
		q = q.Where(d.mysql.TAudit.ParentID.Eq(*filter.ParentID))
	}

	if filter.Status != 0 {
		q = q.Where(d.mysql.TAudit.Status.Eq(filter.Status))
	}

	if len(filter.StatusList) > 0 {
		q = q.Where(d.mysql.TAudit.Status.In(filter.StatusList...))
	}

	if filter.RelatedID != 0 {
		q = q.Where(d.mysql.TAudit.RelateID.Eq(filter.RelatedID))
	}

	if filter.Type != 0 {
		q = q.Where(d.mysql.TAudit.Type.Eq(filter.Type))
	}

	if filter.Etag != "" {
		q = q.Where(d.mysql.TAudit.ETag.Eq(filter.Etag))
	}

	return q

}

// GetParentAuditsByParentRelateID 根据父级关联ID获取审核列表
func (d *daoImpl) GetParentAuditsByParentRelateID(ctx context.Context, filter *releaseEntity.AuditFilter) ([]*releaseEntity.Audit, error) {
	/*
		`
		SELECT
			%s
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_relate_id = ?
		    AND type = ?
			AND parent_id = 0
			AND id > ?
			ORDER BY id ASC
			LIMIT ?
	*/
	db := d.getAuditGormDB(ctx, nil).Table(d.mysql.TAudit.TableName())
	q := mysqlquery.Use(db).TAudit.WithContext(ctx)

	q = d.generateCondition(ctx, q, filter)

	q = q.Where(d.mysql.TAudit.ParentID.Eq(0))

	if filter.Limit > 0 {
		q = q.Offset(filter.Offset).Limit(filter.Limit)
	}

	q = q.Order(d.mysql.TAudit.ID.Asc())

	if auditListDo, err := q.Find(); err != nil {
		return nil, err
	} else {
		return BatchConvertAuditPOToDO(auditListDo), nil
	}
}

// getAuditList 获取审核列表
func (d *daoImpl) GetAuditList(ctx context.Context, selectColumns []string,
	filter *releaseEntity.AuditFilter) ([]*releaseEntity.Audit, error) {
	// auditList := make([]*releaseEntity.Audit, 0)
	// if filter.Limit == 0 {
	// 	// 为0正常返回空结果即可
	// 	return auditList, nil
	// }
	// if filter.Limit > AuditTableMaxPageSize {
	// 	// 限制单次查询最大条数
	// 	err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
	// 	return auditList, err
	// }

	db := d.getAuditGormDB(ctx, nil).Table(d.mysql.TAudit.TableName())
	q := mysqlquery.Use(db).TAudit.WithContext(ctx)
	if len(selectColumns) > 0 {
		field := []field.Expr{}
		for _, col := range selectColumns {
			if colField, ok := d.mysql.TAudit.GetFieldByName(col); ok {
				field = append(field, colField)
			}

		}
		q = q.Select(field...)
	}

	q = d.generateCondition(ctx, q, filter)

	if filter.Limit > 0 {
		q = q.Offset(filter.Offset).Limit(filter.Limit)
	}
	if len(filter.OrderByField) != 0 {
		if orderCol, ok := d.mysql.TAudit.GetFieldByName(filter.OrderByField); ok {
			if filter.OrderByType == util.SqlOrderByDesc {
				q = q.Order(orderCol.Desc())
			} else {
				q = q.Order(orderCol.Asc())
			}
		}
	}

	if auditListDo, err := q.Find(); err != nil {
		return nil, err
	} else {
		return BatchConvertAuditPOToDO(auditListDo), nil
	}
}

func (d *daoImpl) GetAuditByFilter(ctx context.Context, selectColumns []string, filter *releaseEntity.AuditFilter) (*releaseEntity.Audit, error) {
	db := d.getAuditGormDB(ctx, nil).Table(d.mysql.TAudit.TableName())
	q := mysqlquery.Use(db).TAudit.WithContext(ctx)
	if len(selectColumns) > 0 {
		field := []field.Expr{}
		for _, col := range selectColumns {
			if colField, ok := d.mysql.TAudit.GetFieldByName(col); ok {
				field = append(field, colField)
			}

		}
		q = q.Select(field...)
	}

	q = d.generateCondition(ctx, q, filter)
	if filter.OrderByField != "" {
		if orderCol, ok := d.mysql.TAudit.GetFieldByName(filter.OrderByField); ok {
			if filter.OrderByType == util.SqlOrderByDesc {
				q = q.Order(orderCol.Desc())
			} else {
				q = q.Order(orderCol.Asc())
			}
		}
	}
	if auditDo, err := q.First(); err != nil {
		return nil, err
	} else {
		return ConvertAuditPOToDO(auditDo), nil
	}
}

func (d *daoImpl) BatchGetAuditList(ctx context.Context, selectColumns []string, filter *releaseEntity.AuditFilter) (
	[]*releaseEntity.Audit, error) {
	allAuditList := make([]*releaseEntity.Audit, 0)
	if filter.Limit == 0 {
		logx.W(ctx, "GetAuditList limit is 0")
		filter.Limit = AuditTableMaxPageSize
	}
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
		auditList, err := d.GetAuditList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetAuditList failed, err: %+v", err)
			return nil, err
		}
		allAuditList = append(allAuditList, auditList...)
		if len(auditList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetAuditList count:%d cost:%dms",
		len(allAuditList), time.Since(beginTime).Milliseconds())
	return allAuditList, nil

}

func (d *daoImpl) GetBizAuditStatusStat(ctx context.Context, id, corpID, robotID uint64) ([]*releaseEntity.AuditStatusStat, error) {
	//  "SELECT status, COUNT(*) as count FROM t_audit WHERE parent_id = ? AND corp_id = ? AND robot_id = ? GROUP BY status"

	tbl := d.mysql.TAudit

	db := tbl.WithContext(ctx).UnderlyingDB()

	res := []*releaseEntity.AuditStatusStat{}

	if err := db.Select("status", "count(*) as count").
		Where("parent_id = ?", id).Where("corp_id = ?", corpID).Where("robot_id = ?", robotID).
		Group("status").Scan(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

func (d *daoImpl) GetBizAuditStatusByRelateIDs(ctx context.Context, robotID, corpID uint64,
	relateIDs []uint64) ([]*releaseEntity.AuditRelateID, error) {
	/*
		`
		SELECT
			max(id) as id,type,relate_id,status
		FROM
			t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_id != 0
			AND type = ?
			%s
			GROUP BY
			    relate_id,status
	*/
	db := d.mysql.TAudit.WithContext(ctx)
	res := []*releaseEntity.AuditRelateID{}

	queryCond := []gen.Condition{
		d.mysql.TAudit.CorpID.Eq(corpID),
		d.mysql.TAudit.RobotID.Eq(robotID),
		d.mysql.TAudit.ParentID.Neq(0),
		d.mysql.TAudit.Type.Eq(releaseEntity.AuditBizTypeDoc),
	}

	if len(relateIDs) > 0 {
		relatedQueryIds := []int64{}
		for _, id := range relateIDs {
			relatedQueryIds = append(relatedQueryIds, int64(id))
		}
		queryCond = append(queryCond, d.mysql.TAudit.RelateID.In(relatedQueryIds...))
	}

	err := db.Select(
		d.mysql.TAudit.ID.Max().As("id"),
		d.mysql.TAudit.Type,
		d.mysql.TAudit.RelateID,
		d.mysql.TAudit.Status,
	).Where(queryCond...).Group(d.mysql.TAudit.RelateID, d.mysql.TAudit.Status).Scan(&res)

	if err != nil {
		return nil, err
	}
	return res, nil
}

// createAudit 创建单条送审
func (d *daoImpl) CreateAuditByAuditSendParams(ctx context.Context, p entity.AuditSendParams, tx *gorm.DB) (*releaseEntity.Audit, error) {
	now := time.Now()
	audit := releaseEntity.NewParentAudit(p.CorpID, p.RobotID, p.StaffID, p.RelateID, 0, p.Type)
	audit.BusinessID = idgen.GetId()
	audit.UpdateTime = now
	audit.CreateTime = now

	id, err := d.CreateAudit(ctx, tx, audit)

	if err != nil {
		logx.E(ctx, "Failed to create audit data. err:%+v", err)
		return nil, err
	}

	audit.ID = id
	p.ParentAuditBizID = audit.BusinessID

	return audit, nil
}

func (d *daoImpl) CreateAudit(ctx context.Context, tx *gorm.DB, audit *releaseEntity.Audit) (uint64, error) {
	tAudit := ConvertAuditDOToPO(audit)

	tbl := d.MysqlQuery().TAudit
	auditTableName := tbl.TableName()

	var db *gorm.DB
	var err error

	if tx == nil {
		db, err = knowClient.GormClient(ctx, auditTableName, audit.RobotID, 0, []client.Option{}...)
		if err != nil {
			return 0, err
		}
	} else {
		db = tx
	}

	err = db.Table(tbl.TableName()).Create(tAudit).Error
	if err != nil {
		return 0, err
	}
	return tAudit.ID, nil
}

func (d *daoImpl) BatchCreateAudit(ctx context.Context, audits []*releaseEntity.Audit, tx *gorm.DB) error {
	if len(audits) == 0 {
		return nil
	}

	db := d.getAuditGormDB(ctx, tx)

	err := db.Transaction(func(tx *gorm.DB) error {
		for _, audit := range audits {
			if id, err := d.CreateAudit(ctx, tx, audit); err != nil {
				return err
			} else {
				audit.ID = id
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *daoImpl) UpdateAudit(ctx context.Context, updateColumns []string, filter *releaseEntity.AuditFilter,
	audit *releaseEntity.Audit, tx *gorm.DB) (int64, error) {
	if audit == nil {
		logx.E(ctx, "[UpdateAudit] Audit is nil")
		return 0, nil
	}

	db := d.getAuditGormDB(ctx, tx)

	q := mysqlquery.Use(db).TAudit.WithContext(ctx).Debug()
	if len(updateColumns) > 0 {
		updateFileds := []field.Expr{}
		for _, column := range updateColumns {
			if field, ok := d.mysql.TAudit.GetFieldByName(column); ok {
				updateFileds = append(updateFileds, field)
			}
		}
		q = q.Select(updateFileds...)
	}
	q = d.generateCondition(ctx, q, filter)

	if res, err := q.Updates(ConvertAuditDOToPO(audit)); err != nil {
		logx.E(ctx, "[UpdateAudit] UpdateAudit error: %v", err)
		return 0, err
	} else {
		return res.RowsAffected, nil
	}

}
