package release

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"gorm.io/gen"
	"gorm.io/gorm"
)

func (d *daoImpl) getReleaseConfigGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseConfig.WithContext(ctx).UnderlyingDB()
	}
	return d.mysql.TReleaseConfig.WithContext(ctx).Debug().UnderlyingDB()
}

func (d *daoImpl) CreateReleaseConfigRecords(ctx context.Context, releaseConfigs []*releaseEntity.ReleaseConfig, tx *gorm.DB) error {
	if len(releaseConfigs) == 0 {
		logx.I(ctx, "no release config to create")
		return nil
	}
	db := d.getReleaseConfigGormDB(ctx, tx)
	total := len(releaseConfigs)

	toCreateConfigs := BatchConvertRelaseConfigPoToDo(releaseConfigs)
	if err := db.CreateInBatches(toCreateConfigs, total+1).Error; err != nil {
		logx.E(ctx, "CreateRelaseConfigRecords data %d releaseConfigs, error:%v",
			len(toCreateConfigs), err)
		return err
	}

	return nil
}

func (d *daoImpl) UpdateReleaseConfigRecords(ctx context.Context, releaseConfigs []*releaseEntity.ReleaseConfig, tx *gorm.DB) error {
	if len(releaseConfigs) == 0 {
		logx.I(ctx, "no release config record to update")
		return nil
	}
	db := d.getReleaseConfigGormDB(ctx, tx)
	releaseConfigIds := make([]int64, 0)
	for _, config := range releaseConfigs {
		releaseConfigIds = append(releaseConfigIds, int64(config.ID))
	}

	info := db.Table(d.mysql.TReleaseConfig.TableName()).
		Where(d.mysql.TReleaseConfig.ID.In(releaseConfigIds...)).
		Updates(map[string]any{
			d.mysql.TReleaseConfig.ReleaseStatus.ColumnName().String(): releaseConfigs[0].ReleaseStatus,
			d.mysql.TReleaseConfig.AuditStatus.ColumnName().String():   releaseConfigs[0].AuditStatus,
			d.mysql.TReleaseConfig.Message.ColumnName().String():       releaseConfigs[0].Message,
			d.mysql.TReleaseConfig.AuditResult.ColumnName().String():   releaseConfigs[0].AuditResult,
			d.mysql.TReleaseConfig.UpdateTime.ColumnName().String():    time.Now(),
		})

	err := info.Error
	if err != nil {
		logx.E(ctx, "UpdateReleaseConfigRecords data %d releaseConfigs, error:%v",
			len(releaseConfigs), err)
		return err
	}
	logx.I(ctx, "UpdateReleaseConfigRecords success, info:%+v", info)
	return nil
}

// GetAuditConfigItemByVersion 获取要审核的配置内容
func (d *daoImpl) GetAuditConfigItemByVersion(ctx context.Context, versionID uint64) (
	[]*releaseEntity.AuditReleaseConfig, error) {
	/***
			getAuditConfigByVersion = `
			SELECT
				id,version_id,config_item,value
			FROM
			    t_release_config
			WHERE
			    version_id = ? AND audit_status = ?
		`
	    ***/

	db := d.mysql.TReleaseConfig.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseConfig.VersionID.Eq(versionID),
		d.mysql.TReleaseConfig.AuditStatus.Eq(entity.ConfigReleaseStatusAuditing),
	}

	modifyConfigs := make([]*model.TReleaseConfig, 0)
	modifyConfigs, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetAuditConfigItemByVersion data req:(versionID:%v), error:%v",
			versionID, err)
		return nil, err
	}

	res := make([]*releaseEntity.AuditReleaseConfig, 0)
	for _, item := range modifyConfigs {
		res = append(res, &releaseEntity.AuditReleaseConfig{
			ID:          uint64(item.ID),
			VersionID:   item.VersionID,
			ConfigItem:  item.ConfigItem,
			Value:       item.Value,
			AuditStatus: item.AuditStatus,
		})
	}

	return res, nil
}

// GetReleaseConfigItemByID 通过ID获取发布的配置内容
func (d *daoImpl) GetReleaseConfigItemByID(ctx context.Context, id uint64) (*releaseEntity.ReleaseConfig, error) {
	/***
	`
		SELECT
			%s
		FROM
		    t_release_config
		WHERE
			 id = ?
	`
	***/
	db := d.mysql.TReleaseConfig.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseConfig.ID.Eq(int64(id)),
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseConfigItemByID data req:(id:%v), error:%v",
			id, err)
		return nil, err
	}
	if len(res) == 0 {
		logx.I(ctx, "[warning] GetReleaseConfigItemByID data req:(id:%v) not found", id)
		return nil, nil
	}
	return ConvertReleaseConfigDOToPO(res[0]), nil
}

// UpdateAuditConfigItem 更新配置审核状态
func (d *daoImpl) UpdateAuditConfigItem(ctx context.Context, cfg *releaseEntity.ReleaseConfig) error {
	/***
	 `
		UPDATE
			t_release_config
		SET
		    release_status = :release_status,
		    audit_status = :audit_status,
		    message = :message,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	***/

	db := d.mysql.TReleaseConfig.WithContext(ctx).Debug()

	if err := db.UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		_, err := db.Where(d.mysql.TReleaseConfig.ID.Eq(int64(cfg.ID))).Updates(cfg)
		if err != nil {

			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "UpdateAuditConfigItem data req:(id:%v), error:%v",
			cfg.ID, err)
	}
	return nil
}

// GetReleaseConfigAuditStat 统计QA审核
func (d *daoImpl) GetReleaseConfigAuditStat(ctx context.Context, versionID uint64) (
	map[uint32]*releaseEntity.AuditResultStat, error) {

	/***
		`
		SELECT
			audit_status,count(*) total
		FROM
		    t_release_config
		WHERE
		    version_id = ?
		GROUP BY
		    audit_status
	`
	***/

	tbl := d.mysql.TReleaseConfig
	db := d.mysql.TReleaseConfig.WithContext(ctx)

	res := make([]*releaseEntity.AuditResultStat, 0)
	queryArgs := make([]any, 0, 1)
	queryArgs = append(queryArgs, versionID)

	if err := db.Select(tbl.AuditStatus, tbl.ID.Count().As("total")).
		Where(tbl.VersionID.Eq(versionID)).
		Group(tbl.AuditStatus).
		Scan(&res); err != nil {
		logx.E(ctx, "GetReleaseConfigAuditStat data req:(versionID:%v), error:%v",
			versionID, err)
		return nil, err
	}
	stat := make(map[uint32]*releaseEntity.AuditResultStat, 0)
	for _, item := range res {
		auditStat := &releaseEntity.AuditResultStat{
			AuditStatus: item.AuditStatus,
			Total:       item.Total,
		}
		stat[item.AuditStatus] = auditStat
	}
	return stat, nil
}

// GetModifyReleaseConfigCount 发布配置项预览数量
func (d *daoImpl) GetModifyReleaseConfigCount(ctx context.Context, versionID uint64,
	releaseStatuses []uint32, query string) (uint64, error) {
	/***
		`
		SELECT
			count(*) total
		FROM
			t_release_config
		WHERE
			version_id = ? %s
	`
	***/

	db := d.mysql.TReleaseConfig.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		d.mysql.TReleaseConfig.VersionID.Eq(versionID),
	}

	if len(releaseStatuses) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseConfig.ReleaseStatus.In(releaseStatuses...))
	}

	if len(query) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseConfig.ConfigItem.Like(query))
	}

	total, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "GetModifyReleaseConfigCount data req:(versionID:%v), error:%v",
			versionID, err)
		return 0, err
	}
	return uint64(total), nil
}

// GetConfigItemByVersionID 获取发布配置内容列表
func (d *daoImpl) GetConfigItemByVersionID(ctx context.Context, versionID uint64) (
	[]*releaseEntity.ReleaseConfig, error) {
	/***
		`
			SELECT
				%s
			FROM
			    t_release_config
			WHERE
			    version_id = ?
		`
	***/
	db := d.mysql.TReleaseConfig.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseConfig.VersionID.Eq(versionID),
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetConfigItemByVersionID data req:(versionID:%v), error:%v",
			versionID, err)
		return nil, err
	}
	return BatchConvertRelaseConfigDoToPo(res), nil
}

// GetInAppealConfigItem 获取正在申诉的发布配置内容列表
func (d *daoImpl) GetInAppealConfigItem(ctx context.Context, robotID uint64) (
	[]*releaseEntity.ReleaseConfig, error) {
	/***
	  `
			SELECT
				%s
			FROM
			    t_release_config
			WHERE
			    robot_id = ? AND audit_status = ?
	`
	***/

	db := d.mysql.TReleaseConfig.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseConfig.RobotID.Eq(robotID),
		d.mysql.TReleaseConfig.AuditStatus.Eq(entity.ConfigReleaseStatusAppealIng),
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetInAppealConfigItem data req:(robotID:%v), error:%v",
			robotID, err)
		return nil, err
	}
	return BatchConvertRelaseConfigDoToPo(res), nil
}

// ListConfigByVersionID 获取发布配置内容列表
func (d *daoImpl) ListConfigByVersionID(ctx context.Context, listReq *releaseEntity.ListReleaseConfigReq) (
	[]*releaseEntity.ReleaseConfig, error) {
	db := d.mysql.TReleaseConfig.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		d.mysql.TReleaseConfig.VersionID.Eq(listReq.VersionID),
	}

	if len(listReq.Status) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseConfig.ReleaseStatus.In(listReq.Status...))
	}

	if len(listReq.Query) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseConfig.ConfigItem.Like(listReq.Query))
	}

	offset, limit := utilx.Page(listReq.Page, listReq.PageSize)

	// 获取列表
	configs, err := db.Where(queryCond...).Limit(limit).Offset(offset).Find()
	if err != nil {
		logx.E(ctx, "ListConfigByVersionID data req:(versionID:%v), error:%v",
			listReq.VersionID, err)
		return nil, err
	}
	return BatchConvertRelaseConfigDoToPo(configs), nil
}

func ConvertReleaseConfigDOToPO(configDo *model.TReleaseConfig) *releaseEntity.ReleaseConfig {
	if configDo == nil {
		return nil
	}
	return &releaseEntity.ReleaseConfig{
		ID:            uint64(configDo.ID),
		CorpID:        configDo.CorpID,
		StaffID:       configDo.StaffID,
		RobotID:       configDo.RobotID,
		VersionID:     configDo.VersionID,
		ConfigItem:    configDo.ConfigItem,
		OldValue:      configDo.OldValue,
		Value:         configDo.Value,
		Content:       configDo.Content,
		Action:        uint32(configDo.Action),
		ReleaseStatus: configDo.ReleaseStatus,
		Message:       configDo.Message,
		AuditStatus:   uint32(configDo.AuditStatus),
		AuditResult:   configDo.AuditResult,
		UpdateTime:    configDo.UpdateTime,
		CreateTime:    configDo.CreateTime,
		ExpireTime:    configDo.ExpireTime,
	}
}

func BatchConvertRelaseConfigDoToPo(do []*model.TReleaseConfig) []*releaseEntity.ReleaseConfig {
	ret := make([]*releaseEntity.ReleaseConfig, 0, len(do))
	for _, v := range do {
		ret = append(ret, ConvertReleaseConfigDOToPO(v))
	}
	return ret
}

func ConvertReleaseConfigPoToDO(configPo *releaseEntity.ReleaseConfig) *model.TReleaseConfig {
	if configPo == nil {
		return nil
	}
	return &model.TReleaseConfig{
		ID:            int64(configPo.ID),
		CorpID:        configPo.CorpID,
		StaffID:       configPo.StaffID,
		RobotID:       configPo.RobotID,
		VersionID:     configPo.VersionID,
		ConfigItem:    configPo.ConfigItem,
		OldValue:      configPo.OldValue,
		Value:         configPo.Value,
		Content:       configPo.Content,
		Action:        configPo.Action,
		ReleaseStatus: configPo.ReleaseStatus,
		Message:       configPo.Message,
		AuditStatus:   configPo.AuditStatus,
		AuditResult:   configPo.AuditResult,
		UpdateTime:    configPo.UpdateTime,
		CreateTime:    configPo.CreateTime,
		ExpireTime:    configPo.ExpireTime,
	}
}

func BatchConvertRelaseConfigPoToDo(po []*releaseEntity.ReleaseConfig) []*model.TReleaseConfig {
	ret := make([]*model.TReleaseConfig, 0, len(po))
	for _, v := range po {
		ret = append(ret, ConvertReleaseConfigPoToDO(v))
	}
	return ret
}
