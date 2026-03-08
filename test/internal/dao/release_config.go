package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
)

const (
	releaseConfigField = `
		id,corp_id,staff_id,robot_id,version_id,config_item,old_value,value,content,action,
		release_status,message,audit_status,audit_result,create_time,update_time,expire_time
	`
	createReleaseConfig = `
		INSERT INTO 
		    t_release_config(%s) 
		VALUES 
		    (null,:corp_id,:staff_id,:robot_id,:version_id,:config_item,:old_value,:value,:content,
		     :action,:release_status,:message,:audit_status,:audit_result,:create_time,:update_time,:expire_time)
	`
	getAuditConfigByVersion = `
		SELECT 
			id,version_id,config_item,value 
		FROM 
		    t_release_config 
		WHERE 
		    version_id = ? AND audit_status = ?
	`
	getConfigListByVersionID = `
		SELECT 
			%s 
		FROM 
		    t_release_config 
		WHERE 
		    version_id = ? 
	`
	getAppealConfigItem = `
		SELECT 
			%s 
		FROM 
		    t_release_config 
		WHERE 
		    robot_id = ? AND audit_status = ?
	`
	listConfigByVersionID = `
		SELECT
			%s
		FROM
			t_release_config
		WHERE
			version_id = ? %s
		LIMIT 
			?,?
	`
	getReleaseConfigByID = `
		SELECT 
			%s 
		FROM 
		    t_release_config 
		WHERE 
			 id = ?
	`
	getReleaseConfigListByID = `
		SELECT 
			%s 
		FROM 
		    t_release_config 
		WHERE 
			 id = IN (%s)
	`
	updateAuditConfig = `
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
	releaseConfigAuditPass = `
		UPDATE 
		    t_release_config 
		SET 
		    release_status = ?,
		    audit_status = ?,
		    audit_result = ?,
		    message = ?,
		    update_time = ? 
		WHERE 
		    id = ? AND audit_status IN (?, ?)
	`
	releaseConfigAuditNotPass = `
		UPDATE 
		    t_release_config 
		SET 
		    release_status = ?,
		    audit_status = ?,
		    audit_result = ?,
		    message = ?,
		    update_time = ? 
		WHERE 
		    id = ? 
	`
	getReleaseConfigAuditStat = `
		SELECT 
			audit_status,count(*) total 
		FROM 
		    t_release_config 
		WHERE 
		    version_id = ? 
		GROUP BY 
		    audit_status
	`
	getReleaseConfigCount = `
		SELECT
			count(*) total
		FROM
			t_release_config
		WHERE
			version_id = ? %s
	`
	updateConfigItemRelease = `
		UPDATE
			t_release_config
		SET
			release_status = ?,
			update_time = ?
		WHERE
			id IN (%s)
	`
)

// GetAuditConfigItemByVersion 获取要审核的配置内容
func (d *dao) GetAuditConfigItemByVersion(ctx context.Context, versionID uint64) ([]*model.AuditReleaseConfig, error) {
	args := make([]any, 0, 2)
	args = append(args, versionID, model.ConfigReleaseStatusAuditing)
	querySQL := getAuditConfigByVersion
	modifyConfigs := make([]*model.AuditReleaseConfig, 0)
	if err := d.db.QueryToStructs(ctx, &modifyConfigs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取要审核的配置内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyConfigs, nil
}

// GetReleaseConfigItemByID 通过ID获取发布的配置内容
func (d *dao) GetReleaseConfigItemByID(ctx context.Context, id uint64) (*model.ReleaseConfig, error) {
	args := make([]any, 0, 1)
	args = append(args, id)
	querySQL := fmt.Sprintf(getReleaseConfigByID, releaseConfigField)
	list := make([]*model.ReleaseConfig, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布的配置项失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// getReleaseConfigListByID 通过ID获取发布的配置内容
func (d *dao) getReleaseConfigListByID(ctx context.Context, id []uint64) ([]*model.ReleaseConfig, error) {
	args := make([]any, 0, len(id)+1)
	args = append(args, id)
	querySQL := fmt.Sprintf(getReleaseConfigListByID, releaseConfigField, placeholder(len(id)))
	list := make([]*model.ReleaseConfig, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布的配置项失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list, nil
}

// UpdateAuditConfigItem 更新配置审核状态
func (d *dao) UpdateAuditConfigItem(ctx context.Context, cfg *model.ReleaseConfig) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		cfg.UpdateTime = time.Now()
		querySQL := updateAuditConfig
		if _, err := tx.NamedExecContext(ctx, querySQL, cfg); err != nil {
			log.ErrorContextf(ctx, "更新配置项审核状态 sql:%s args:%+v err:%+v", querySQL, cfg, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新配置项审核状态 err:%+v", err)
		return err
	}
	return nil
}

// getReleaseConfigAuditStat 统计QA审核
func (d *dao) getReleaseConfigAuditStat(ctx context.Context, versionID uint64) (map[uint32]*model.AuditResultStat,
	error) {
	args := make([]any, 0, 1)
	args = append(args, versionID)
	querySQL := getReleaseConfigAuditStat
	list := make([]*model.AuditResultStat, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "统计配置项审核失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	stat := make(map[uint32]*model.AuditResultStat, 0)
	for _, item := range list {
		stat[item.AuditStatus] = item
	}
	return stat, nil
}

// GetModifyReleaseConfigCount 发布配置项预览数量
func (d *dao) GetModifyReleaseConfigCount(ctx context.Context, versionID uint64,
	releaseStatuses []uint32, query string) (uint64, error) {
	args := make([]any, 0, 4+len(releaseStatuses))
	args = append(args, versionID)
	condition := ""
	if len(releaseStatuses) > 0 {
		condition = fmt.Sprintf("%s AND release_status IN (%s)", condition, placeholder(len(releaseStatuses)))
		for _, releaseStatus := range releaseStatuses {
			args = append(args, releaseStatus)
		}
	}
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND config_item LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(query)))
	}
	var total uint64
	SQL := fmt.Sprintf(getReleaseConfigCount, condition)
	if err := d.db.Get(ctx, &total, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询发布配置数量失败 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return 0, err
	}

	return total, nil
}

// GetConfigItemByVersionID 获取发布配置内容列表
func (d *dao) GetConfigItemByVersionID(ctx context.Context, versionID uint64) ([]*model.ReleaseConfig, error) {
	args := make([]any, 0, 1)
	args = append(args, versionID)
	querySQL := fmt.Sprintf(getConfigListByVersionID, releaseConfigField)
	modifyConfigs := make([]*model.ReleaseConfig, 0)
	if err := d.db.QueryToStructs(ctx, &modifyConfigs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取配置内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyConfigs, nil
}

// GetInAppealConfigItem 获取正在申诉的发布配置内容列表
func (d *dao) GetInAppealConfigItem(ctx context.Context, robotID uint64) ([]*model.ReleaseConfig, error) {
	args := make([]any, 0, 2)
	args = append(args, robotID, model.ConfigReleaseStatusAppealIng)
	querySQL := fmt.Sprintf(getAppealConfigItem, releaseConfigField)
	modifyConfigs := make([]*model.ReleaseConfig, 0)
	if err := d.db.QueryToStructs(ctx, &modifyConfigs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取配置内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyConfigs, nil
}

// ListConfigByVersionID 获取发布配置内容列表
func (d *dao) ListConfigByVersionID(ctx context.Context, versionID uint64, query string,
	page, pageSize uint32, status []uint32) ([]*model.ReleaseConfig, error) {
	condition := ""
	args := make([]any, 0, 5)
	args = append(args, versionID)
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND config_item LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(query)))
	}
	if len(status) > 0 {
		condition = fmt.Sprintf("%s AND release_status IN (%s)", condition, placeholder(len(status)))
		for _, releaseStatus := range status {
			args = append(args, releaseStatus)
		}
	}
	start := (page - 1) * pageSize
	args = append(args, start, pageSize)
	querySQL := fmt.Sprintf(listConfigByVersionID, releaseConfigField, condition)
	modifyConfigs := make([]*model.ReleaseConfig, 0)
	if err := d.db.QueryToStructs(ctx, &modifyConfigs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取配置内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyConfigs, nil
}

func (d *dao) releaseConfig(ctx context.Context, tx *sqlx.Tx, configAuditPass,
	configAuditFail []*model.ReleaseConfig, now time.Time, release *model.Release, appDB *model.AppDB) error {
	cfg, err := d.GetConfigHistoryByVersionID(ctx, release.RobotID, release.ID)
	if err != nil {
		return err
	}
	// 有审核失败的配置项
	if len(configAuditFail) > 0 {
		json, err := d.releaseAuditFailConfig(ctx, cfg, configAuditFail, appDB)
		if err != nil {
			return err
		}
		cfg.ReleaseJSON = json
	} else if appDB.AppType == model.KnowledgeQaAppType {
		releaseJSON, err := transformFilterToRelease(ctx, cfg)
		if err != nil {
			return err
		}
		cfg.ReleaseJSON = releaseJSON
	}
	if len(configAuditPass) > 0 {
		querySQL := fmt.Sprintf(updateConfigItemRelease, placeholder(len(configAuditPass)))
		args := make([]any, 0, len(configAuditPass)+2)
		args = append(args, model.ConfigReleaseStatusSuccess, now)
		for _, v := range configAuditPass {
			args = append(args, v.ID)
		}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "更新发布配置项状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
	}
	if err = d.updateConfigHistoryByVersionID(ctx, tx, release.RobotID, release.ID, cfg.ReleaseJSON); err != nil {
		return err
	}
	if err = d.updateAppRelease(ctx, tx, cfg.ReleaseJSON, model.AppStatusRunning, release.RobotID); err != nil {
		return err
	}
	return nil
}

func (d *dao) releaseAuditFailConfig(ctx context.Context, preview *model.RobotConfigHistory,
	configAuditFail []*model.ReleaseConfig, appDB *model.AppDB) (string, error) {
	var previewConfig model.AppDetailsConfig
	var releaseConfig model.AppDetailsConfig
	if err := jsoniter.Unmarshal([]byte(preview.ReleaseJSON), &previewConfig); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
		return "", err
	}
	if len(appDB.ReleaseJSON) > 0 {
		if err := jsoniter.Unmarshal([]byte(appDB.ReleaseJSON), &releaseConfig); err != nil {
			log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
			return "", err
		}
	}
	for _, v := range configAuditFail {
		modifyConfigStruct(v, &previewConfig, &releaseConfig)
	}
	log.DebugContextf(ctx, "审核失败回滚配置,preview:%+v release:%+v", previewConfig, releaseConfig)
	if appDB.AppType == model.KnowledgeQaAppType {
		transformReleaseQAFilterIndex(&previewConfig)
	}
	json, err := jsoniter.Marshal(previewConfig)
	if err != nil {
		return string(json), err
	}
	return string(json), nil
}

// modifyConfigStruct 修改审核不通过的config对象字段值
func modifyConfigStruct(configAuditFail *model.ReleaseConfig, preview, release *model.AppDetailsConfig) {
	if configAuditFail == nil || preview == nil {
		return
	}
	switch configAuditFail.ConfigItem {
	case model.ConfigItemName:
		if release == nil {
			preview.BaseConfig.Name = ""
			return
		}
		preview.BaseConfig.Name = release.BaseConfig.Name
	case model.ConfigItemAvatar:
		if release == nil {
			preview.BaseConfig.Avatar = ""
			return
		}
		preview.BaseConfig.Avatar = release.BaseConfig.Avatar
	case model.ConfigItemRoleDescription:
		if release.AppConfig.KnowledgeQaConfig == nil {
			preview.AppConfig.KnowledgeQaConfig.RoleDescription = ""
			return
		}
		preview.AppConfig.KnowledgeQaConfig.RoleDescription = release.AppConfig.KnowledgeQaConfig.RoleDescription
	case model.ConfigItemBareAnswer:
		if release.AppConfig.KnowledgeQaConfig == nil {
			preview.AppConfig.KnowledgeQaConfig.BareAnswer = ""
			return
		}
		preview.AppConfig.KnowledgeQaConfig.BareAnswer = release.AppConfig.KnowledgeQaConfig.BareAnswer
	case model.ConfigItemGreeting:
		if release.AppConfig.KnowledgeQaConfig == nil {
			preview.AppConfig.KnowledgeQaConfig.Greeting = ""
			return
		}
		preview.AppConfig.KnowledgeQaConfig.Greeting = release.AppConfig.KnowledgeQaConfig.Greeting
	}
}

func transformFilterToRelease(ctx context.Context, preview *model.RobotConfigHistory) (string, error) {
	var previewConfig model.AppDetailsConfig
	if err := jsoniter.Unmarshal([]byte(preview.ReleaseJSON), &previewConfig); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
		return "", err
	}
	if previewConfig.AppConfig.KnowledgeQaConfig == nil ||
		previewConfig.AppConfig.KnowledgeQaConfig.Filters == nil {
		return preview.ReleaseJSON, nil
	}
	transformReleaseQAFilterIndex(&previewConfig)
	json, err := jsoniter.Marshal(previewConfig)
	if err != nil {
		return string(json), err
	}
	return string(json), nil
}

func transformReleaseQAFilterIndex(previewConfig *model.AppDetailsConfig) {
	for k := range model.PreviewFilterKeys {
		if _, ok := previewConfig.AppConfig.KnowledgeQaConfig.Filters[k]; !ok {
			continue
		}
		filter := previewConfig.AppConfig.KnowledgeQaConfig.Filters[k]
		for i := range filter.Filter {
			filter.Filter[i].IndexID = model.GetDefaultFiltersIndexID(config.App().RobotDefault.Filters,
				model.FilterKeyPairs[k], filter.Filter[i].DocType)
		}
		previewConfig.AppConfig.KnowledgeQaConfig.Filters[model.FilterKeyPairs[k]] = filter
		delete(previewConfig.AppConfig.KnowledgeQaConfig.Filters, k)
	}
}
