package release

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"gorm.io/gorm/clause"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

const (
	removeReleaseLimit = 200
	// ReleaseSegmentReleaseStatusSuccess 发布成功
	ReleaseSegmentReleaseStatusSuccess = uint32(4)
)

func (d *daoImpl) getReleaseSegmentGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseSegment.WithContext(ctx).UnderlyingDB()
	}
	return d.mysql.TReleaseSegment.WithContext(ctx).Debug().UnderlyingDB()
}

func (d *daoImpl) IsExistReleaseSegment(ctx context.Context, filter *releaseEntity.ReleaseSegmentFilter) (bool, error) {
	/*
		`
			SELECT
				COUNT(1)
			FROM
				t_release_segment
			WHERE
				robot_id = ? AND version_id = ? AND doc_id = ? AND segment_id = ?
		`
	*/
	db := d.mysql.TReleaseSegment.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		d.mysql.TReleaseSegment.RobotID.Eq(filter.RobotID),
		d.mysql.TReleaseSegment.VersionID.Eq(filter.VersionID),
		d.mysql.TReleaseSegment.DocID.Eq(filter.DocID),
		d.mysql.TReleaseSegment.SegmentID.Eq(filter.SegmentID),
	}

	total, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "IsExistReleaseSegment data req:(robotId:%v, versionId:%v, docId:%v, segmentId:%v), error:%v",
			filter.RobotID, filter.VersionID, filter.DocID, filter.SegmentID, err)
		return false, err
	}
	return total > 0, nil
}

func (d *daoImpl) CreateReleaseSegmentRecords(ctx context.Context, releaseSegments []*releaseEntity.ReleaseSegment, tx *gorm.DB) error {
	if len(releaseSegments) == 0 {
		logx.I(ctx, "no release segments to create")
		return nil
	}
	db := d.getReleaseSegmentGormDB(ctx, tx)
	total := len(releaseSegments)
	logx.I(ctx, "CreateReleaseSegmentRecords data %d releaseSegments", total)
	toCreateSegments := BatchConvertReleaseSegmentPOToDO(releaseSegments)
	q := mysqlquery.Use(db.Table(model.TableNameTReleaseSegment))

	err := q.TReleaseSegment.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: q.TReleaseSegment.VersionID.ColumnName().String()},
			{Name: q.TReleaseSegment.SegmentID.ColumnName().String()},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			q.TReleaseSegment.SegmentType.ColumnName().String(),
			q.TReleaseSegment.Status.ColumnName().String(),
			q.TReleaseSegment.ReleaseStatus.ColumnName().String(),
			q.TReleaseSegment.UpdateTime.ColumnName().String(),
			q.TReleaseSegment.IsDeleted.ColumnName().String(),
			q.TReleaseSegment.PageContent.ColumnName().String(),
		}), // 冲突时更新 config,update_time,is_deleted,preview_config 字段
	}).CreateInBatches(toCreateSegments, 100)
	if err != nil {
		return err
	}
	return nil
}

func (d *daoImpl) UpdateReleaseSegmentRecords(ctx context.Context, updateColumns []string,
	filter *releaseEntity.ReleaseSegmentFilter, releaseSeg *releaseEntity.ReleaseSegment, tx *gorm.DB) (uint64, error) {
	db := d.getReleaseSegmentGormDB(ctx, tx)
	session := mysqlquery.Use(db).TReleaseSegment.WithContext(ctx)

	if filter.RobotID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.RobotID.Eq(filter.RobotID))
	}

	if filter.ID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.ID.Eq(int64(filter.ID)))
	}

	if filter.VersionID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.VersionID.Eq(filter.VersionID))
	}

	if filter.DocID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.DocID.Eq(filter.DocID))
	}

	updateFileds := []field.Expr{}

	for _, v := range updateColumns {
		if f, ok := d.mysql.TReleaseSegment.GetFieldByName(v); ok {
			updateFileds = append(updateFileds, f)
		}
	}

	info, err := session.Select(updateFileds...).Updates(releaseSeg)
	if err != nil {
		logx.E(ctx, "UpdateReleaseSegmentRecords data error:%v", err)
		return 0, err
	}
	return uint64(info.RowsAffected), nil
}

func (d *daoImpl) BatchUpdateReleaseSegmentRecords(ctx context.Context, updateColumns map[string]any,
	filter *releaseEntity.ReleaseSegmentFilter, tx *gorm.DB) (uint64, error) {
	db := d.getReleaseSegmentGormDB(ctx, tx)
	session := mysqlquery.Use(db).TReleaseSegment.WithContext(ctx)

	if filter.RobotID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.RobotID.Eq(filter.RobotID))
	}

	if filter.ID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.ID.Eq(int64(filter.ID)))
	}

	if len(filter.IDs) > 0 {
		session = session.Where(d.mysql.TReleaseSegment.ID.In(filter.IDs...))
	}

	if filter.VersionID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.VersionID.Eq(filter.VersionID))
	}

	if filter.DocID != 0 {
		session = session.Where(d.mysql.TReleaseSegment.DocID.Eq(filter.DocID))
	}

	info, err := session.Updates(updateColumns)
	if err != nil {
		logx.E(ctx, "BatchUpdateReleaseSegmentRecords data error:%v", err)
		return 0, err
	}
	return uint64(info.RowsAffected), nil
}

// GetReleaseDoc 获取版本改动的文档ID
func (d *daoImpl) GetDocIDInReleaseDocSegements(ctx context.Context, release *releaseEntity.Release) (
	[]uint64, error) {
	/***
		`
			SELECT
				DISTINCT(doc_id)
			FROM
			    t_release_segment
			WHERE
			    robot_id = ? AND version_id = ? AND corp_id = ? AND doc_id != 0
	    `
	***/
	db := d.mysql.TReleaseSegment.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseSegment.RobotID.Eq(release.RobotID),
		d.mysql.TReleaseSegment.VersionID.Eq(release.ID),
		d.mysql.TReleaseSegment.CorpID.Eq(release.CorpID),
		d.mysql.TReleaseSegment.DocID.Neq(0),
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseDocSegements data req:(robotId:%v, versionId:%v, corpId:%v), error:%v",
			release.RobotID, release.ID, release.CorpID, err)
		return nil, err
	}

	docIDs := slicex.Map(res, func(item *model.TReleaseSegment) uint64 {
		return item.DocID
	})
	return docIDs, nil

	// docID := make(map[uint64]struct{}, 0)
	// for _, item := range res {
	// 	docID[item.DocID] = struct{}{}
	// }

	// return docID, nil
}

// GetReleaseModifySegment 获取版本改动的segment
func (d *daoImpl) GetReleaseModifySegment(ctx context.Context, release *releaseEntity.Release,
	segments []*segment.DocSegmentExtend) (
	map[uint64]*releaseEntity.ReleaseSegment, error) {
	/***
	`
		SELECT
			%s
		FROM
		    t_release_segment
		WHERE
		    corp_id = ? AND robot_id = ? AND version_id = ? %s
	`
	***/
	db := d.mysql.TReleaseSegment.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		d.mysql.TReleaseSegment.CorpID.Eq(release.CorpID),
		d.mysql.TReleaseSegment.RobotID.Eq(release.RobotID),
		d.mysql.TReleaseSegment.VersionID.Eq(release.ID),
	}

	if len(segments) > 0 {
		segmentIds := make([]uint64, 0, len(segments))
		for _, segment := range segments {
			segmentIds = append(segmentIds, segment.ID)
		}

		queryCond = append(queryCond, d.mysql.TReleaseSegment.SegmentID.In(segmentIds...))
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseModifySegment data req:(robotId:%v, versionId:%v, corpId:%v), error:%v",
			release.RobotID, release.ID, release.CorpID, err)
		return nil, err
	}
	modifySegment := make(map[uint64]*releaseEntity.ReleaseSegment, 0)
	for _, item := range res {
		modifySegment[item.SegmentID] = ConvertReleaseSegmentDoToPO(item)
	}

	return modifySegment, nil

}

// GetModifySegmentCount 获取版本改动segment数量
func (d *daoImpl) GetModifySegmentCount(ctx context.Context, robotID, versionID uint64, action uint32, releaseStatuses []uint32) (uint64, error) {

	/***
	`
		SELECT
			count(*)
		FROM
		    t_release_segment
		WHERE
			 robot_id = ? AND version_id = ? %s
	`
	***/
	db := d.mysql.TReleaseSegment.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		d.mysql.TReleaseSegment.RobotID.Eq(robotID),
		d.mysql.TReleaseSegment.VersionID.Eq(versionID),
	}

	if action != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.Action.Eq(action))
	}

	if len(releaseStatuses) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.ReleaseStatus.In(releaseStatuses...))
	}

	total, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "GetModifySegmentCount data req:(robotId:%v, versionId:%v, action:%v), error:%v",
			robotID, versionID, action, err)
		return 0, err
	}

	return uint64(total), nil

}

// GetModifySegmentList 获取版本改动segment范围
func (d *daoImpl) GetModifySegmentList(ctx context.Context,
	req *releaseEntity.ListReleaseSegmentReq) ([]*releaseEntity.ReleaseSegment, error) {

	/***
	`
		SELECT
			%s
		FROM
		    t_release_segment
		WHERE
		    robot_id = ? AND version_id = ? %s
		LIMIT ?,?
	`
	***/

	db := d.mysql.TReleaseSegment.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		d.mysql.TReleaseSegment.RobotID.Eq(req.RobotID),
		d.mysql.TReleaseSegment.VersionID.Eq(req.VersionID),
	}
	if req.MinSegmentID != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.SegmentID.Gte(req.MinSegmentID))
	}
	if req.MaxSegmentID != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.SegmentID.Lte(req.MaxSegmentID))
	}
	if req.IsAllowRelease != nil {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.IsAllowRelease.Eq(*req.IsAllowRelease))
	}
	if req.IsDeleted != nil {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.IsDeleted.Eq(*req.IsDeleted))
	}
	if req.IsDeletedNot != nil {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.IsDeleted.Neq(*req.IsDeletedNot))
	}
	if len(req.Actions) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseSegment.Action.In(req.Actions...))

	}

	db = db.Where(queryCond...)

	if len(req.OrderBy) != 0 {
		for _, v := range req.OrderBy {
			if v == d.mysql.TReleaseSegment.ID.ColumnName().String() {
				db = db.Order(d.mysql.TReleaseSegment.ID.Asc())
			} else if v == d.mysql.TReleaseSegment.SegmentID.ColumnName().String() {
				db = db.Order(d.mysql.TReleaseSegment.SegmentID.Asc())
			}
		}
	}

	offset, limit := utilx.Page(req.Page, req.PageSize)
	res, err := db.Offset(offset).Limit(limit).Find()
	if err != nil {
		logx.E(ctx, "GetModifySegmentList data req:(robotId:%v, versionId:%v, action:%v), error:%v",
			req.RobotID, req.VersionID, req.Actions, err)
		return nil, err
	}

	return BatchConvertReleaseSegmentDoToPO(res), nil
}

func (d *daoImpl) ClearRealtimeAppResourceReleaseSegment(ctx context.Context, removeTime int64, tx *gorm.DB) error {
	var groupName string
	tbl := d.mysql.TReleaseSegment
	tableName := tbl.TableName()
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	// 2. 分批查询并删除
	for {
		session := db.Table(tableName).Select(d.mysql.TReleaseSegment.ID.ColumnName(), d.mysql.TReleaseSegment.DocID.ColumnName(),
			d.mysql.TReleaseSegment.RobotID.ColumnName())
		segments := make([]*model.TReleaseSegment, 0)
		// 2.1 查询待删除数据
		err := session.Where(
			d.mysql.TReleaseSegment.CreateTime.Lte(time.Unix(removeTime, 0)),
			d.mysql.TReleaseSegment.ReleaseStatus.Eq(ReleaseSegmentReleaseStatusSuccess),
		).
			Order(d.mysql.TReleaseSegment.CreateTime.Asc()).
			Limit(removeReleaseLimit).
			Find(&segments).Error
		if err != nil {
			logx.E(ctx, "查询失败: group=%s, err=%v", groupName, err)
			return fmt.Errorf("查询待删除分段失败: %w", err)
		}
		// 2.2 批量删除
		for _, v := range segments {
			err := session.Table(tableName).Where(
				d.mysql.TReleaseSegment.RobotID.Eq(v.RobotID), // robot_id = ?
				d.mysql.TReleaseSegment.ID.Eq(int64(v.ID)),    // id = ?
				d.mysql.TReleaseSegment.DocID.Eq(v.DocID),     // doc_id = ?
			).Delete(nil).Error
			if err != nil {
				logx.E(ctx, "clearRealtimeAppResourceReleaseSegment remove data,groupName:%s,err:%+v",
					groupName, err)
				return err
			}
		}
		// 2.3 终止条件
		if len(segments) < removeReleaseLimit {
			break
		}
	}
	return nil
}
