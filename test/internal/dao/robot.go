package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
)

const (
	updateRobot = `
		UPDATE
		    t_robot
		SET
		    update_time=NOW(),
			%s
		WHERE
		    id = :id
	`
	updateRobotOfOp = `
		UPDATE
		    t_robot
		SET
			model = :model,
			filters = :filters,
			split_doc = :split_doc,
			search_vector = :search_vector,
			intent_policy_id = :intent_policy_id
		WHERE
		    id = :id
	`
	getUsePolicyRobotCount = `
		SELECT
    		count(1)
		FROM
		    t_robot
		WHERE
		    intent_policy_id = ? AND is_deleted = ?
	`
	getWaitEmbeddingUpgradeApp = `
		SELECT ` + appFields + ` FROM t_robot
		WHERE embedding != ''
		AND embedding->'$.version' = embedding->'$.upgrade_version'
		AND embedding->'$.version' = ?
		AND embedding->'$.upgrade_version' != ?
		AND is_deleted = 0
    `
	getWaitEmbeddingUpgradeAppByIDs = `
		SELECT ` + appFields + ` FROM t_robot
		WHERE embedding != ''
		AND embedding->'$.version' = embedding->'$.upgrade_version'
		AND embedding->'$.version' = ?
		AND embedding->'$.upgrade_version' != ?
		AND is_deleted = 0
		AND id IN (?)
    `

	getWaitOrgDataSyncApp = `
		SELECT ` + appFields + ` FROM t_robot
		WHERE is_deleted = 0
    `
	getWaitOrgDataSyncAppByIDs = `
		SELECT ` + appFields + ` FROM t_robot
		WHERE is_deleted = 0
		AND id IN (?)
    `

	updateEmbeddingConf = `UPDATE t_robot SET embedding = ?, update_time = ? WHERE id = ?`

	getRobotByIDForUpdate = `
		SELECT
			%s
		FROM
		    t_robot
		WHERE
		    id = ?
		FOR UPDATE
	`
)

// UpdateRobot 更新机器人属性 TODO
func (d *dao) UpdateRobot(ctx context.Context, typ uint32, app *model.AppDB, isNeedAudit bool) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		var fields []string
		var auditBizType uint32
		if typ == model.RobotBasicConfig {
			auditBizType = model.AuditBizTypeRobotProfile
			if isNeedAudit {
				if app.AvatarInAudit != "" {
					fields = append(fields, "avatar_in_audit=:avatar_in_audit")
				}
				if app.NameInAudit != "" {
					fields = append(fields, "name_in_audit=:name_in_audit")
				}
				if app.GreetingInAudit != "" {
					fields = append(fields, "greeting_in_audit=:greeting_in_audit")
				} else if app.GreetingInAudit == "" && app.Greeting == "" {
					fields = append(fields, "greeting=:greeting")
				}
				if app.RoleDescriptionInAudit != "" {
					fields = append(fields, "role_description_in_audit=:role_description_in_audit")
				} else if app.RoleDescription != "" {
					fields = append(fields, "role_description=:role_description")
				}
			} else {
				fields = []string{"name=:name", "avatar=:avatar", "greeting=:greeting"}
			}
		} else if typ == model.RobotTransferConfig {
			fields = []string{"transfer_method=:transfer_method", "transfer_keywords=:transfer_keywords",
				"transfer_unsatisfied_count=:transfer_unsatisfied_count"}
		} else if typ == model.RobotDialogueStrategyConfig {
			auditBizType = model.AuditBizTypeBareAnswer
			fields = []string{"bare_answer_in_audit=:bare_answer_in_audit",
				"use_general_knowledge=:use_general_knowledge", "use_search_engine=:use_search_engine",
				"reply_flexibility=:reply_flexibility", "show_search_engine=:show_search_engine"}
			if app.UseGeneralKnowledge || !isNeedAudit {
				isNeedAudit = false
				fields = []string{"bare_answer=:bare_answer",
					"use_general_knowledge=:use_general_knowledge", "use_search_engine=:use_search_engine",
					"reply_flexibility=:reply_flexibility", "show_search_engine=:show_search_engine"}
			}
		}
		// 兼容应用配置内容
		fields = append(fields, "preview_json=:preview_json")
		fields = append(fields, "release_json=:release_json")
		if len(fields) == 0 {
			return nil
		}
		sql := fmt.Sprintf(updateRobot, strings.Join(fields, ","))
		if _, err := tx.NamedExecContext(ctx, sql, app); err != nil {
			log.ErrorContextf(ctx, "更新机器人属性失败, sql: %s, args: %+v, err: %+v", sql, app, err)
			return err
		}
		log.DebugContextf(ctx, "更新机器人属性成功 sql: %s, args: %+v", sql, app)
		if !isNeedAudit {
			return nil
		}
		if err := d.createAudit(ctx, model.AuditSendParams{
			CorpID:   app.CorpID,
			RobotID:  app.ID,
			StaffID:  app.StaffID,
			Type:     auditBizType,
			RelateID: app.ID,
			EnvSet:   getEnvSet(ctx),
		}); err != nil {
			log.ErrorContextf(ctx, "创建机器人送审任务失败 err:%+v", err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新机器人属性失败 err:%+v", err)
		return err
	}
	return nil
}

// GetRobotTotal 获取机器人数量
func (d *dao) GetRobotTotal(ctx context.Context, corpID uint64, name string, botBizIDs []uint64, deleteFlag uint32) (
	uint32, error) {
	var total uint32
	args, condition := fillRobotListParams(corpID, name, botBizIDs, deleteFlag)
	querySQL := fmt.Sprintf(getAppCount, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取所有机器人数量失败 sql:%s  err:%+v", querySQL, err)
		return 0, err
	}
	return total, nil
}

// fillRobotListParams TODO
func fillRobotListParams(corpID uint64, name string, botBizIDs []uint64, deleteFlag uint32) ([]any, string) {
	var args []any
	var condition string
	if corpID > 0 {
		condition += " AND corp_id = ?"
		args = append(args, corpID)
	}
	if len(name) > 0 {
		condition += " AND name like ?"
		args = append(args, "%"+name+"%")
	}
	if len(botBizIDs) != 0 {
		for _, v := range botBizIDs {
			args = append(args, v)
		}
		condition += fmt.Sprintf(" AND business_id IN(%s)", placeholder(len(botBizIDs)))
	}
	if deleteFlag == 1 {
		condition += " AND is_deleted = ?"
		args = append(args, model.AppIsNotDeleted)
	}
	if deleteFlag == 2 {
		condition += " AND is_deleted = ?"
		args = append(args, model.AppIsDeleted)
	}

	return args, condition
}

// GetRobotList 获取机器人列表
func (d *dao) GetRobotList(
	ctx context.Context, corpID uint64, name string, botBizIDs []uint64, deleteFlag uint32, page, pageSize uint32,
) ([]*model.AppDB, error) {
	var models []*model.AppDB
	args, condition := fillRobotListParams(corpID, name, botBizIDs, deleteFlag)
	querySQL := fmt.Sprintf(getAppList, appFields, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	if err := d.db.QueryToStructs(ctx, &models, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取所有机器人列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return models, nil
}

// GetRobotInfo 获取机器人信息
func (d *dao) GetRobotInfo(ctx context.Context, corpID, appID uint64) (*model.AppDB, error) {
	models := &model.AppDB{}
	querySQL := fmt.Sprintf("select %s from t_robot where id = ? and corp_id = ? and is_deleted = 0 limit 1", appFields)
	if err := d.db.QueryToStruct(ctx, models, querySQL, appID, corpID); err != nil {
		log.ErrorContextf(ctx, "获取机器人信息失败 sql:%s err:%+v corpID:%v, appID:%v",
			querySQL, err, corpID, appID)
		return nil, err
	}
	return models, nil
}

// UpdateRobotOfOp 运营工具更新robot
func (d *dao) UpdateRobotOfOp(ctx context.Context, robot *model.AppDB) error {
	querySQL := updateRobotOfOp
	if _, err := d.db.NamedExec(ctx, querySQL, robot); err != nil {
		log.ErrorContextf(ctx, "更新robot失败 sql:%s args:%+v err:%+v", querySQL, robot, err)
		return err
	}
	return nil
}

// GetUsePolicyRobotCount 获取使用了对应id策略的机器人数量
func (d *dao) GetUsePolicyRobotCount(ctx context.Context, policyID uint32) (uint32, error) {
	var total uint32
	querySQL := getUsePolicyRobotCount
	args := []any{policyID, model.AppIsNotDeleted}
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取机器人数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetWaitEmbeddingUpgradeApp 获取待升级 embedding 的应用
func (d *dao) GetWaitEmbeddingUpgradeApp(
	ctx context.Context, ids []uint64, fromEmbVer uint64, toEmbVer uint64,
) ([]model.AppDB, error) {
	var err error
	var apps []model.AppDB
	query := getWaitEmbeddingUpgradeApp
	args := []any{fromEmbVer, toEmbVer}
	if len(ids) > 0 {
		if query, args, err = sqlx.In(getWaitEmbeddingUpgradeAppByIDs, fromEmbVer, toEmbVer, ids); err != nil {
			log.ErrorContextf(
				ctx, "获取待升级 embedding 应用列表失败, query: %s args: %+v, err: %+v",
				getWaitEmbeddingUpgradeAppByIDs, []any{fromEmbVer, toEmbVer, ids}, err,
			)
			return nil, err
		}
	}
	if err = d.db.Select(ctx, &apps, query, args...); err != nil {
		log.ErrorContextf(
			ctx, "获取待升级 embedding 应用列表失败, query: %s args: %+v, err: %+v", query, args, err,
		)
		return nil, err
	}
	return apps, nil
}

// GetWaitOrgDataSyncApp 获取待同步 org_data 的应用
func (d *dao) GetWaitOrgDataSyncApp(ctx context.Context, ids []uint64) ([]model.AppDB, error) {
	var err error
	var apps []model.AppDB
	query := getWaitOrgDataSyncApp
	var args []any
	if len(ids) > 0 {
		if query, args, err = sqlx.In(getWaitOrgDataSyncAppByIDs, ids); err != nil {
			log.ErrorContextf(
				ctx, "获取待同步 org_data 的应用列表失败, query: %s args: %+v, err: %+v",
				getWaitOrgDataSyncAppByIDs, []any{ids}, err,
			)
			return nil, err
		}
	}
	if err = d.db.Select(ctx, &apps, query, args...); err != nil {
		log.ErrorContextf(
			ctx, "获取待同步 org_data 的应用列表失败, query: %s args: %+v, err: %+v", query, args, err,
		)
		return nil, err
	}
	return apps, nil
}

// StartEmbeddingUpgradeApp 应用升级 embedding 开始
func (d *dao) StartEmbeddingUpgradeApp(ctx context.Context, id uint64, fromEmbVer uint64, toEmbVer uint64) error {
	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		// 校验升级状态
		var app model.AppDB
		query := fmt.Sprintf(getRobotByIDForUpdate, appFields)
		args := []any{id}
		if err := tx.Get(&app, query, args...); err != nil {
			log.ErrorContextf(ctx, "获取待升级 embedding 应用失败, query: %s args: %+v, err: %+v", query, args, err)
			return err
		}
		if app.HasDeleted() { // 已删除不做处理
			return nil
		}
		conf, _, err := app.GetEmbeddingConf()
		if err != nil {
			log.ErrorContextf(ctx, "获取应用 embedding 配置失败, appID: %d, err: %+v", id, err)
			return err
		}
		if conf.Version != fromEmbVer {
			log.ErrorContextf(
				ctx, "应用 embedding 版本不匹配, appID: %d, fromEmbVer: %d, toEmbVer: %d", id, fromEmbVer, toEmbVer,
			)
			return errs.ErrEmbeddingVersionNotMatch
		}
		if conf.UpgradeVersion == toEmbVer { // 当前版本已经和目标版本一致, 不做处理
			return nil
		}
		conf.UpgradeVersion = toEmbVer

		// 标记升级
		upgradeConf, err := jsoniter.MarshalToString(conf)
		if err != nil {
			log.ErrorContextf(ctx, "更新 embedding 配置失败, MarshalToString fail, appID: %d, err: %+v", id, err)
			return err
		}
		query = updateEmbeddingConf
		args = []any{upgradeConf, app.UpdateTime, id}
		if _, err = tx.Exec(query, args...); err != nil {
			log.ErrorContextf(ctx, "更新 embedding 配置失败, query: %s args: %+v, err: %+v", query, args, err)
			return err
		}
		return nil
	})
}

// FinishEmbeddingUpgradeApp 应用升级 embedding 结束
func (d *dao) FinishEmbeddingUpgradeApp(ctx context.Context, id uint64, fromEmbVer uint64, toEmbVer uint64) error {
	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		// 校验升级状态
		var app model.AppDB
		query := fmt.Sprintf(getRobotByIDForUpdate, appFields)
		args := []any{id}
		if err := tx.Get(&app, query, args...); err != nil {
			log.ErrorContextf(ctx, "获取待升级 embedding 应用失败, query: %s args: %+v, err: %+v", query, args, err)
			return err
		}
		if app.HasDeleted() { // 已删除不做处理
			return nil
		}
		conf, _, err := app.GetEmbeddingConf()
		if err != nil {
			log.ErrorContextf(ctx, "获取应用 embedding 配置失败, appID: %d, err: %+v", id, err)
			return err
		}
		if conf.UpgradeVersion != toEmbVer {
			log.ErrorContextf(
				ctx, "应用 embedding 版本不匹配, appID: %d, fromEmbVer: %d, toEmbVer: %d", id, fromEmbVer, toEmbVer,
			)
			return errs.ErrEmbeddingVersionNotMatch
		}
		if conf.Version != fromEmbVer {
			log.ErrorContextf(
				ctx, "应用 embedding 版本不匹配, appID: %d, fromEmbVer: %d, toEmbVer: %d", id, fromEmbVer, toEmbVer,
			)
			return errs.ErrEmbeddingVersionNotMatch
		}
		conf.Version = toEmbVer

		// 标记升级
		upgradeConf, err := jsoniter.MarshalToString(conf)
		if err != nil {
			log.ErrorContextf(ctx, "更新 embedding 配置失败, MarshalToString fail, appID: %d, err: %+v", id, err)
			return err
		}
		query = updateEmbeddingConf
		args = []any{upgradeConf, app.UpdateTime, id}
		if _, err = tx.Exec(query, args...); err != nil {
			log.ErrorContextf(ctx, "更新 embedding 配置失败, query: %s args: %+v, err: %+v", query, args, err)
			return err
		}
		return nil
	})
}
