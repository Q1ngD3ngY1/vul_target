package release

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) generateReleaseCondition(ctx context.Context, session *gorm.DB, filter *releaseEntity.ReleaseFilter) *gorm.DB {
	if filter == nil {
		return session
	}
	tbl := d.mysql.TRelease
	if filter.ID != 0 {
		session = session.Where(tbl.ID.Eq(filter.ID))
	}
	if filter.Status != 0 {
		session = session.Where(tbl.Status.Eq(filter.Status))
	}
	return session
}

func (d *daoImpl) getReleaseGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TRelease.WithContext(ctx).UnderlyingDB()
	}
	return d.mysql.TRelease.WithContext(ctx).Debug().UnderlyingDB()
}

func (d *daoImpl) UpdateReleaseRecord(ctx context.Context, updateColumns []string, filter *releaseEntity.ReleaseFilter,
	record *releaseEntity.Release, tx *gorm.DB) (int64, error) {
	tbl := d.mysql.TRelease
	tableName := tbl.TableName()

	session := d.getReleaseGormDB(ctx, tx)
	session = session.Table(tableName).Select(updateColumns)

	session = d.generateReleaseCondition(ctx, session, filter)

	record.UpdateTime = time.Now()

	res := session.Updates(&record)
	err := res.Error
	if res.Error != nil {
		logx.E(ctx, "UpdateReleaseRecord data record:%+v, error:%v",
			record, err)
		return 0, err
	}
	logx.D(ctx, "UpdateReleaseRecord data record:%+v, res:%v",
		record, res)
	return res.RowsAffected, nil
}

// GetLatestRelease 获取最近一次正式发布id和状态
func (d *daoImpl) GetLatestRelease(ctx context.Context, corpID, robotID uint64) (*releaseEntity.Release, error) {
	tbl := d.mysql.TRelease
	db := tbl.WithContext(ctx).Debug()

	conds := []gen.Condition{
		tbl.CorpID.Eq(corpID),
		tbl.RobotID.Eq(robotID),
	}

	orderCond := []field.Expr{
		tbl.ID.Desc(),
	}

	res, err := db.Where(conds...).Order(orderCond...).Limit(1).Find()
	if err != nil {
		logx.E(ctx, "GetLatestRelease data req:(corpId:%v, robotId:%v), error:%v", corpID, robotID, err)
		return nil, err
	}

	if len(res) == 0 {
		logx.I(ctx, "GetLatestRelease data req:(corpId:%v, robotId:%v) not found", corpID, robotID)
		return nil, nil
	}
	return ConvertReleaseDoToPO(res[0]), nil
}

func (d *daoImpl) GetLatestSuccessRelease(ctx context.Context, corpID, robotID uint64) (
	*releaseEntity.Release, error) {
	/*
		`
			SELECT
				id,update_time
			FROM
			    t_release
			WHERE
			    corp_id = ? AND robot_id = ? AND status = ?
			ORDER BY
			    id DESC
			LIMIT 1
		`

	*/
	tbl := d.mysql.TRelease
	db := d.mysql.TRelease.WithContext(ctx).Debug()
	conds := []gen.Condition{
		tbl.CorpID.Eq(corpID),
		tbl.RobotID.Eq(robotID),
		tbl.Status.Eq(releaseEntity.ReleaseStatusSuccess),
	}

	orderCond := []field.Expr{
		tbl.ID.Desc(),
	}
	res, err := db.Where(conds...).
		Order(orderCond...).
		Limit(1).
		Find()
	if err != nil {
		logx.E(ctx, "GetLatestSuccessRelease data req:(corpId:%v, robotId:%v), error:%v",
			corpID, robotID, err)
		return nil, err
	}

	if len(res) == 0 {
		logx.E(ctx, "GetLatestSuccessRelease data req:(corpId:%v, robotId:%v) not found",
			corpID, robotID)
		return nil, nil
	}
	return ConvertReleaseDoToPO(res[0]), nil
}

func (d *daoImpl) GetReleaseByID(ctx context.Context, id uint64) (*releaseEntity.Release, error) {
	/*
			`
				SELECT
					%s
				FROM
				    t_release
				WHERE
					id = ?
		    `

	*/
	tbl := d.mysql.TRelease
	db := d.mysql.TRelease.WithContext(ctx).Debug()
	conds := []gen.Condition{
		tbl.ID.Eq(id),
	}
	res, err := db.Where(conds...).
		Find()
	if err != nil {
		logx.E(ctx, "GetReleaseByID data req:(id:%v), error:%v",
			id, err)
		return nil, err
	}

	if len(res) == 0 {
		logx.E(ctx, "GetReleaseByID data req:(id:%v) not found",
			id)
		return nil, nil
	}
	return ConvertReleaseDoToPO(res[0]), nil
}

func (d *daoImpl) GetReleaseByBizID(ctx context.Context, bizID uint64) (
	*releaseEntity.Release, error) {

	/*
			`
				SELECT
					%s
				FROM
				    t_release
				WHERE
					business_id = ?
		    `

	*/
	tbl := d.mysql.TRelease
	db := d.mysql.TRelease.WithContext(ctx).Debug()
	conds := []gen.Condition{
		tbl.BusinessID.Eq(bizID),
	}
	res, err := db.Where(conds...).
		Find()
	if err != nil {
		logx.E(ctx, "GetReleaseByBizID data req:(bizId:%v), error:%v",
			bizID, err)
		return nil, err
	}

	if len(res) == 0 {
		logx.W(ctx, "GetReleaseByBizID data req:(bizId:%v) not found",
			bizID)
		return nil, nil
	}
	return ConvertReleaseDoToPO(res[0]), nil
}
