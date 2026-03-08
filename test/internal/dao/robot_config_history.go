package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	configHistoryFields = `
		id,robot_id,version_id,release_json,is_release,create_time,update_time
	`
	createConfigHistory = `
		INSERT INTO 
		    t_robot_config_history (%s) 
		VALUES 
		    (null,:robot_id,:version_id,:release_json,:is_release,:create_time,:update_time)
	`
	getConfigHistoryByVersionID = `
		SELECT 
		    %s	
		FROM	
		    t_robot_config_history 
		WHERE 
		    robot_id = ? AND version_id = ? 
	`
	updateConfigHistoryReleaseStatus = `
		UPDATE
			t_robot_config_history
		SET
		    release_json = ?,
			is_release = ?,
			update_time = ?
		WHERE
			robot_id = ? AND version_id = ? 
	`
	getLastVersionID = `SELECT robot_id,max(version_id) as version_id FROM t_robot_config_history
	WHERE is_release=1 AND robot_id IN(?) group by robot_id`
)

// createReleaseConfigHistory 创建配置发布记录
func (d *dao) createReleaseConfigHistory(ctx context.Context, record *model.RobotConfigHistory) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := fmt.Sprintf(createConfigHistory, configHistoryFields)
		_, err := tx.NamedExecContext(ctx, querySQL, record)
		if err != nil {
			log.ErrorContextf(ctx, "创建配置发布记录失败 sql:%s args:%+v err:%+v", querySQL, record, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建配置发布记录失败 err:%+v", err)
		return err
	}
	return nil
}

// GetConfigHistoryByVersionID 通过发布单ID获取历史配置信息
func (d *dao) GetConfigHistoryByVersionID(ctx context.Context, robotID, versionID uint64) (
	*model.RobotConfigHistory, error) {
	args := make([]any, 0, 3)
	args = append(args, robotID, versionID)
	querySQL := fmt.Sprintf(getConfigHistoryByVersionID, configHistoryFields)
	config := make([]*model.RobotConfigHistory, 0)
	if err := d.db.QueryToStructs(ctx, &config, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取历史发布配置信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(config) == 0 {
		return nil, nil
	}
	return config[0], nil
}

// updateConfigHistoryByVersionID 通过发布单ID修改历史配置状态
func (d *dao) updateConfigHistoryByVersionID(ctx context.Context, tx *sqlx.Tx, robotID, versionID uint64,
	json string) error {
	now := time.Now()
	args := make([]any, 0, 5)
	args = append(args, json, model.ConfigIsReleased, now, robotID, versionID)
	querySQL := updateConfigHistoryReleaseStatus
	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新发布历史配置状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// GetLastConfigVersionID 获取最近一次的配置版本id
func (d *dao) GetLastConfigVersionID(ctx context.Context, appIds []uint64) (map[uint64]uint64, error) {
	result := make(map[uint64]uint64)
	if len(appIds) == 0 {
		return result, nil
	}
	config := make([]*model.RobotConfigHistory, 0)
	querySQL, args, err := sqlx.In(getLastVersionID, appIds)
	if err != nil {
		log.ErrorContextf(ctx, "更新发布历史配置状态失败 sql:%s args: %+v err: %v", querySQL, args, err)
		return nil, err
	}
	err = d.db.QueryToStructs(ctx, &config, querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "更新发布历史配置状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}

	for _, v := range config {
		result[v.RobotID] = v.VersionID
	}
	return result, nil
}
