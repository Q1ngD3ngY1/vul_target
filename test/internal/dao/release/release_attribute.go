package release

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"gorm.io/gorm"
)

func (d *daoImpl) generateReleaseAttributeCondition(ctx context.Context, session *gorm.DB, filter *releaseEntity.ReleaseArrtibuteFilter) *gorm.DB {
	if filter == nil {
		return session
	}

	tbl := d.mysql.TReleaseAttribute

	if filter.RobotID != 0 {
		session = session.Where(tbl.RobotID.Eq(filter.RobotID))
	}

	if filter.VersionID != 0 {
		session = session.Where(tbl.VersionID.Eq(filter.VersionID))
	}

	if filter.AttrID != 0 {
		session = session.Where(tbl.AttrID.Eq(filter.AttrID))
	}
	return session
}

func (d *daoImpl) generateReleaseAttributeLabelCondition(ctx context.Context, session *gorm.DB, filter *releaseEntity.ReleaseArrtibuteLabelFilter) *gorm.DB {
	if filter == nil {
		return session
	}
	tbl := d.mysql.TReleaseAttributeLabel

	if filter.RobotID != 0 {
		session = session.Where(tbl.RobotID.Eq(filter.RobotID))
	}

	if filter.VersionID != 0 {
		session = session.Where(tbl.VersionID.Eq(filter.VersionID))
	}

	if filter.AttrID != 0 {
		session = session.Where(tbl.AttrID.Eq(filter.AttrID))
	}
	return session
}

func (d *daoImpl) GetReleaseAttributeDao(ctx context.Context, robotID uint64, tx *gorm.DB) (*gorm.DB, error) {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseAttribute.WithContext(ctx).UnderlyingDB(), nil
	}
	db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttribute, robotID, 0)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (d *daoImpl) GetReleaseAttributeLabelDao(ctx context.Context, robotID uint64, tx *gorm.DB) (*gorm.DB, error) {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseAttributeLabel.WithContext(ctx).UnderlyingDB(), nil
	}
	db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttributeLabel, robotID, 0)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (d *daoImpl) IsExistReleaseAttribute(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteFilter, tx *gorm.DB) (bool, error) {
	/*
		`
		        SELECT
		            COUNT(1)
		        FROM
		            t_release_attribute
		        WHERE
		            robot_id = ? AND version_id = ? AND attr_id = ?
		    `
	*/
	db, err := d.GetReleaseAttributeDao(ctx, filter.RobotID, tx)
	if err != nil {
		logx.E(ctx, "Failed to GetReleaseAttributeDao. data filter:%+v, error:%v",
			filter, err)
		return false, err
	}
	db = db.Table(d.mysql.TReleaseAttribute.TableName()).Select("COUNT(1) as cnt")

	if filter != nil {
		db = d.generateReleaseAttributeCondition(ctx, db, filter)
	}

	var count int64

	res := db.Scan(&count)

	if res.Error != nil {
		logx.E(ctx, "Failed to IsExistReleaseAttribute. data filter:%+v, error:%v",
			filter, res.Error)
		return false, res.Error
	}

	return count > 0, nil
}

func (d *daoImpl) IsExistReleaseAttributeLabel(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteLabelFilter, tx *gorm.DB) (bool, error) {
	/*
			`
		        SELECT
		            COUNT(1)
		        FROM
		            t_release_attribute_label
		        WHERE
		            robot_id = ? AND version_id = ? AND attr_id = ? AND label_id = ?
		    `
	*/
	db, err := d.GetReleaseAttributeLabelDao(ctx, filter.RobotID, tx)
	if err != nil {
		logx.E(ctx, "IsExistReleaseAttributeLabel|GetReleaseAttributeLabelDao|err: %v", err)
		return false, err
	}
	db = db.Table(d.mysql.TReleaseAttributeLabel.TableName()).Select("COUNT(1) as cnt")

	if filter != nil {
		db = d.generateReleaseAttributeLabelCondition(ctx, db, filter)
	}

	var count int64

	res := db.Scan(&count)

	if res.Error != nil {
		logx.E(ctx, "Failed to IsExistReleaseAttributeLabel. data filter:%+v, error:%v",
			filter, res.Error)
		return false, res.Error
	}

	return count > 0, nil

}

// GetReleaseAttributeCount  调的时候tx 参数得用GormAdminClient 获取 TODO @wemysschen
func (d *daoImpl) GetReleaseAttributeCount(ctx context.Context, robotID, versionID uint64, name string,
	actions []uint32, tx *gorm.DB) (uint64, error) {
	var total int64
	session, err := d.GetReleaseAttributeDao(ctx, robotID, tx)
	if err != nil {
		logx.E(ctx, "GetReleaseAttributeCount|GetReleaseAttributeDao|err: %v", err)
		return 0, err
	}
	session = session.Table(model.TableNameTReleaseAttribute)
	session = session.Where("robot_id = ? AND version_id = ?", robotID, versionID)
	if name != "" {
		session = session.Where("name LIKE ? ", fmt.Sprintf("%%%s%%", special.Replace(name)))
	}
	if len(actions) > 0 {
		session = session.Where("action IN ?", actions)
	}
	if err := session.Count(&total).Error; err != nil {
		return 0, fmt.Errorf("GetReleaseAttributeCount failed: robotID:%d versionID:%d name:%s actions:%v err:%v", robotID, versionID, name, actions, err)
	}
	return uint64(total), nil
}

func (d *daoImpl) GetReleaseAttributeList(ctx context.Context, robotID, versionID uint64, name string, actions []uint32,
	page, pageSize uint32, tx *gorm.DB) ([]*releaseEntity.ReleaseAttribute, error) {
	session, err := d.GetReleaseAttributeDao(ctx, robotID, tx)
	if err != nil {
		logx.E(ctx, "GetReleaseAttributeList|GetReleaseAttributeDao|err: %v", err)
		return nil, err
	}
	labels := make([]*releaseEntity.ReleaseAttribute, 0)
	// GORM 分页 (page从1开始)
	offset := int((page - 1) * pageSize)
	limit := int(pageSize)
	session = session.Table(model.TableNameTReleaseAttribute)
	session = session.Where("robot_id = ? AND version_id = ?", robotID, versionID)
	if name != "" {
		session = session.Where("name LIKE ? ", fmt.Sprintf("%%%s%%", special.Replace(name)))
	}
	if len(actions) > 0 {
		session = session.Where("action IN ?", actions)
	}
	err = session.
		Order("id ASC").
		Offset(offset).
		Limit(limit).
		Find(&labels).Error
	if err != nil {
		return nil, fmt.Errorf("GetReleaseAttributeList Find failed : robotID:%d versionID:%d name:%s actions:%v page:%d size:%d err:%v",
			robotID, versionID, name, actions, page, pageSize, err)
	}
	return labels, nil
}

func (d *daoImpl) GetReleaseAttributeLabels(ctx context.Context, robotID, versionID uint64, attrIDs []uint64, tx *gorm.DB,
) (map[uint64][]*releaseEntity.ReleaseAttributeLabel, error) {
	if versionID == 0 || len(attrIDs) == 0 {
		return nil, nil
	}
	session, err := d.GetReleaseAttributeLabelDao(ctx, robotID, tx)
	if err != nil {
		logx.E(ctx, "GetReleaseAttributeLabels|GetReleaseAttributeLabelDao|err: %v", err)
		return nil, err
	}
	session = session.Table(model.TableNameTReleaseAttributeLabel).
		Where("robot_id = ? AND version_id = ?", robotID, versionID).
		Where("attr_id IN ?", attrIDs)
	labelValues := make([]*releaseEntity.ReleaseAttributeLabel, 0)
	if err := session.Find(&labelValues).Error; err != nil {
		return nil, fmt.Errorf("GetReleaseAttributeLabels failed: robotID:%d versionID:%d attrIDs:%v err:%v",
			robotID, versionID, attrIDs, err)
	}
	mapAttrID2LabelValues := make(map[uint64][]*releaseEntity.ReleaseAttributeLabel, len(attrIDs))
	for _, v := range labelValues {
		mapAttrID2LabelValues[v.AttrID] = append(mapAttrID2LabelValues[v.AttrID], v)
	}
	return mapAttrID2LabelValues, nil
}

// CreateReleaseAttribute - 批量创建发布标签
func (d *daoImpl) CreateReleaseAttribute(ctx context.Context, releaseAttribute *releaseEntity.ReleaseAttribute, tx *gorm.DB) error {
	/*
		 `
			INSERT INTO
				t_release_attribute(%s)
			VALUES
				(:id,:business_id,:robot_id,:version_id,:attr_id,:attr_key,:name,:message,:release_status,:action,
				 :is_deleted,:deleted_time,:create_time,:update_time)
		`
	*/
	if releaseAttribute == nil {
		return nil
	}
	db, err := d.GetReleaseAttributeDao(ctx, releaseAttribute.RobotID, tx)
	if err != nil {
		logx.E(ctx, "CreateReleaseAttribute|GetReleaseAttributeDao|err: %v", err)
		return err
	}

	tReleaseAtribute := ConvertReleaseAttributeDO2PO(releaseAttribute)

	if err := db.Table(model.TableNameTReleaseAttribute).Create(tReleaseAtribute).Error; err != nil {
		return err
	}
	return nil
}

// CreateReleaseAttributeLabel - 批量创建发布标签值
func (d *daoImpl) CreateReleaseAttributeLabel(ctx context.Context, releaseAttributeLabel *releaseEntity.ReleaseAttributeLabel, tx *gorm.DB) error {
	if releaseAttributeLabel == nil {
		return nil
	}
	db, err := d.GetReleaseAttributeLabelDao(ctx, releaseAttributeLabel.RobotID, tx)
	if err != nil {
		logx.E(ctx, "CreateReleaseAttributeLabel|GetReleaseAttributeLabelDao|err: %v", err)
		return err
	}

	tReleaseAttributeLabel := ConvertReleaseAttributeLabelDO2PO(releaseAttributeLabel)

	if err := db.Table(model.TableNameTReleaseAttributeLabel).Create(tReleaseAttributeLabel).Error; err != nil {
		return err
	}
	return nil

}

// BatchUpdateReleaseAttribute - 批量更新发布标签
func (d *daoImpl) BatchUpdateReleaseAttribute(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteFilter, updateColumns map[string]any, tx *gorm.DB) (uint64, error) {
	db, err := d.GetReleaseAttributeDao(ctx, filter.RobotID, tx)
	if err != nil {
		logx.E(ctx, "BatchUpdateReleaseAttribute|GetReleaseAttributeDao|err: %v", err)
		return 0, err
	}

	db = db.Table(model.TableNameTReleaseAttribute)

	if filter != nil {
		db = d.generateReleaseAttributeCondition(ctx, db, filter)
	}

	if res := db.Updates(updateColumns); res.Error != nil {
		return 0, res.Error
	} else {
		return uint64(res.RowsAffected), nil
	}
}

// BatchUpdateReleaseAttributeLabel - 批量更新发布标签值
func (d *daoImpl) BatchUpdateReleaseAttributeLabel(ctx context.Context, filter *releaseEntity.ReleaseArrtibuteLabelFilter, updateColumns map[string]any, tx *gorm.DB) (uint64, error) {
	db, err := d.GetReleaseAttributeLabelDao(ctx, filter.RobotID, tx)
	if err != nil {
		logx.E(ctx, "BatchUpdateReleaseAttributeLabel|GetReleaseAttributeLabelDao|err: %v", err)
		return 0, err
	}
	db = db.Table(model.TableNameTReleaseAttributeLabel)

	if filter != nil {
		db = d.generateReleaseAttributeLabelCondition(ctx, db, filter)
	}

	if res := db.Updates(updateColumns); res.Error != nil {
		return 0, res.Error
	} else {
		return uint64(res.RowsAffected), nil
	}

}

// UpdateReleaseAttributeLabelStatus -
func (d *daoImpl) UpdateReleaseAttributeLabelStatus(ctx context.Context, label *releaseEntity.ReleaseAttributeLabel, tx *gorm.DB) error {
	session, err := d.GetReleaseAttributeLabelDao(ctx, label.RobotID, tx)
	if err != nil {
		logx.E(ctx, "UpdateReleaseAttributeLabelStatus|GetReleaseAttributeLabelDao|err: %v", err)
		return err
	}

	tbl := mysqlquery.Use(session).TAttributeLabel
	releaseAttributeUpdateValues := map[string]interface{}{
		tbl.ReleaseStatus.ColumnName().String(): label.ReleaseStatus,
		tbl.UpdateTime.ColumnName().String():    time.Now(),
	}
	err = session.Table(model.TableNameTReleaseAttributeLabel).
		Where("id = ? AND robot_id = ? AND attr_id = ?", label.ID, label.RobotID, label.AttrID).
		Updates(releaseAttributeUpdateValues).Error
	if err != nil {
		return fmt.Errorf("UpdateReleaseAttributeLabelStatus failed , robotID:%d, status:%s, err:%+v",
			label.RobotID, label.ReleaseStatus, err)
	}
	return nil
}

// UpdateReleaseAttributeStatus -
func (d *daoImpl) UpdateReleaseAttributeStatus(ctx context.Context, releaseAttribute *releaseEntity.ReleaseAttribute, tx *gorm.DB) error {
	session, err := d.GetReleaseAttributeDao(ctx, releaseAttribute.RobotID, tx)
	if err != nil {
		logx.E(ctx, "UpdateReleaseAttributeStatus|GetReleaseAttributeDao|err: %v", err)
		return err
	}
	releaseAttributeUpdateValues := map[string]interface{}{
		d.mysql.TReleaseAttribute.ReleaseStatus.ColumnName().String(): releaseAttribute.ReleaseStatus,
		d.mysql.TReleaseAttribute.UpdateTime.ColumnName().String():    time.Now(),
	}
	err = session.Table(model.TableNameTReleaseAttribute).
		Where("id = ? AND robot_id = ? AND attr_id = ?", releaseAttribute.ID, releaseAttribute.RobotID, releaseAttribute.AttrID).
		Updates(releaseAttributeUpdateValues).Error
	if err != nil {
		return fmt.Errorf("UpdateReleaseAttributeStatus failed, attrID:%d, robotID:%d, status:%s, err:%+v",
			releaseAttribute.AttrID, releaseAttribute.ReleaseStatus, releaseAttribute.ReleaseStatus, err)
	}
	return nil
}
