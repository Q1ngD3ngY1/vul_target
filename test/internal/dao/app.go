package dao

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"

	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/chat"
)

const (
	appFields = `
		id,app_key,business_id,corp_id,app_type,app_status,app_status_reason,name,name_in_audit,
        description,role_description,role_description_in_audit,greeting,greeting_in_audit,model_name,preview_json,
        release_json,reply_flexibility,model,filters,split_doc,embedding,search_vector,qa_version,transfer_keywords,
        enabled,used_char_size,is_create_vector_index,is_deleted,enable_rerank,transfer_method,
        transfer_unsatisfied_count,intent_policy_id,use_search_engine,show_search_engine,token_usage,bare_answer,space_id,
		bare_answer_in_audit,use_general_knowledge,staff_id,expire_time,create_time,update_time,infosec_biz_type,is_shared
	`
	createApp = `
		INSERT INTO
		    t_robot (%s)
		VALUES
		    (null,:app_key,:business_id,:corp_id,:app_type,:app_status,:app_status_reason,:name,:name_in_audit,
		     :description,:role_description,:role_description_in_audit,:greeting,:greeting_in_audit,
		     :model_name,:preview_json,:release_json,:reply_flexibility,:model,:filters,:split_doc,:embedding,
		     :search_vector,:qa_version,:transfer_keywords,:enabled,:used_char_size,:is_create_vector_index,
		     :is_deleted,:enable_rerank,:transfer_method,:transfer_unsatisfied_count,:intent_policy_id,
		     :use_search_engine,:show_search_engine,:token_usage,:bare_answer,:bare_answer_in_audit,
			 :use_general_knowledge,:staff_id,:expire_time,:create_time,:update_time)
	`
	updateAppCreateVectorIndexDone = `
		UPDATE
			t_robot
		SET
		    is_create_vector_index = :is_create_vector_index,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	deleteApp = `
		UPDATE
			t_robot
		SET
		    is_deleted = :is_deleted,
		    update_time = :update_time,
			staff_id = :staff_id
		WHERE
		    id = :id
	`
	updateApp = `
		UPDATE
		    t_robot
		SET
			%s
		WHERE
		    id = :id
	`
	updateAppJSON = `
		UPDATE
		    t_robot
		SET
		    update_time = NOW(),
		    name = :name,
			release_json = :release_json,
			preview_json = :preview_json
		WHERE
		    id = :id
	`
	updateAppPreviewJSON = `
		UPDATE
		    t_robot
		SET
			preview_json = :preview_json
		WHERE
		    id = :id
	`
	getAppByBusinessID = `
		SELECT
		    %s
		FROM
		    t_robot
		WHERE
		    business_id = ?
	`
	getAppByCorpID = `
		SELECT
			%s
		FROM
			t_robot
		WHERE
            corp_id = ? AND is_deleted = ?
		ORDER BY
		    id DESC
	`
	getAppByCorpIDAndAppBizIDs = `
		SELECT
		    %s
		FROM
		    t_robot
		WHERE
		    corp_id = ? AND is_deleted = ? AND business_id IN (%s)
		ORDER BY
		    id DESC
	`
	getAppCount = `
        SELECT
            COUNT(*)
        FROM
            t_robot
        WHERE
            1=1 %s
    `
	getAppList = `
        SELECT
            %s
        FROM
            t_robot
        WHERE
            1=1 %s
        ORDER BY
            update_time DESC
        LIMIT ?,?
    `
	updateAppReleaseJSON = `
		UPDATE
		    t_robot
		SET
			release_json = ?,
		    update_time = NOW()
		WHERE
		    id = ?
	`
	updateAppStatus = `
		UPDATE
		    t_robot
		SET
			app_status = ?,
			app_status_reason = ?,
		    update_time = NOW()
		WHERE
		    id = ? AND app_status = ?
	`
	getAppByName = `
		SELECT
			%s
		FROM
		    t_robot
		WHERE
		    corp_id = ? AND (name = ? OR name_in_audit = ?) AND is_deleted = ?
	`
	getAppByAppKey = `
		SELECT
			%s
		FROM
		    t_robot
		WHERE
		    app_key = ? AND is_deleted = ?
	`
	getCorpUsedCharSizeUsage = `
		SELECT
			IFNULL(SUM(used_char_size), 0) AS used_char_size
		FROM
		    t_robot
		WHERE
		    corp_id = ? AND is_deleted = ?
	`
	getBotUsedCharSizeUsage = `
		SELECT
			IFNULL(SUM(used_char_size), 0) AS used_char_size
		FROM
		    t_robot
		WHERE
		    business_id = ? AND is_deleted = ?
	`
	getCorpTokenUsage = `
		SELECT
			IFNULL(SUM(token_usage), 0) AS token_usage
		FROM
		    t_robot
		WHERE
		    corp_id = ?
	`
	updateTrialCorpAppStatus = `
		UPDATE
		    t_robot
		SET
			app_status = ?,
			app_status_reason = ?,
		    update_time = update_time
		WHERE
		    corp_id = ? AND is_deleted = ?
	`
	getAllValidApp = `
		SELECT
			%s
		FROM
		    t_robot
		WHERE
		    is_deleted = ?
	`

	getValidAppIDs = `
		SELECT
			%s
		FROM
		    t_robot
		WHERE
		    id > ? AND id <= ? AND is_deleted = ?
		ORDER BY id ASC
	`
	getAppByID = `
		SELECT
			%s
		FROM
		    t_robot
		WHERE
		    id = ?
	`
	updateAppDiffUsedCharSize = `
		UPDATE
		    t_robot
		SET
			used_char_size = used_char_size + ?,
			update_time = NOW()
		WHERE
		    id = ?
	`
	updateAppUsedCharSize = `
		UPDATE
		    t_robot
		SET
			used_char_size = ?,
		    update_time = update_time
		WHERE
		    id = ?
	`
	getAppByIDRange = `
		SELECT
			%s
		FROM
			t_robot
		WHERE
			id BETWEEN ? AND ?
		ORDER BY
			id ASC
		LIMIT ?
	`
	syncAppData = `
		UPDATE
			t_robot
		SET
			app_type = :app_type,
			app_status = :app_status,
			model_name = :model_name,
			preview_json = :preview_json,
			release_json = :release_json
		WHERE
		    id = :id
	`
	updateAppQAVersion = `
		UPDATE
			t_robot
		SET
			qa_version = ?, update_time = ?
		WHERE
		    id = ?
	`
	updateAuditName = `
		UPDATE
		    t_robot
		SET
		    name=:name,
			greeting=:greeting,
		    name_in_audit=:name_in_audit,
		    greeting_in_audit=:greeting_in_audit,
		    update_time=:update_time
		WHERE
		    id = :id
	`
	updateBareAnswer = `
		UPDATE
		    t_robot
		SET
		    bare_answer=:bare_answer,
		    bare_answer_in_audit=:bare_answer_in_audit,
		    update_time=:update_time
		WHERE
		    id = :id
	`
	updateAppUsage = `
		UPDATE
		    t_robot
		SET
			token_usage = :token_usage,
			update_time = update_time
		WHERE
		    id = :id
    `
	updateAppOfOp = `
        UPDATE
            t_robot
        SET
            model_name = :model_name,
            preview_json = :preview_json,
            release_json = :release_json,
            intent_policy_id = :intent_policy_id
        WHERE
            id = :id
    `

	// 刷preview_json和release_json应用配置数据
	flushAppData = `
        UPDATE
            t_robot
        SET
            preview_json = :preview_json,
            release_json = :release_json
        WHERE
            id = :id
    `

	// insertSearchSettingFields 新增更新检索配置字段
	insertSearchSettingFields = `id,robot_id,enable_vector_recall,enable_es_recall,enable_rrf,enable_text2sql,
	rerank_threshold,rrf_vec_weight,rrf_es_weight,rrf_rerank_weight,doc_vec_recall_num,qa_vec_recall_num,
	es_recall_num,es_rerank_min_num,rrf_reciprocal_const,operator,es_top_n,text2sql_model,text2sql_prompt`
	// insertOrUpdateSearchSetting 新增更新检索配置
	insertOrUpdateSearchSetting = `INSERT INTO t_retrieval_config (%s)
		VALUES(null,:robot_id,:enable_vector_recall,:enable_es_recall,:enable_rrf,:enable_text2sql,
		:rerank_threshold,:rrf_vec_weight,:rrf_es_weight,:rrf_rerank_weight,:doc_vec_recall_num,:qa_vec_recall_num,
	:es_recall_num,:es_rerank_min_num,:rrf_reciprocal_const,:operator,:es_top_n,:text2sql_model,:text2sql_prompt)
		ON DUPLICATE KEY UPDATE
			enable_vector_recall = VALUES(enable_vector_recall),
			enable_es_recall = VALUES(enable_es_recall),
			enable_rrf = VALUES(enable_rrf),
			enable_text2sql = VALUES(enable_text2sql),
			rerank_threshold = VALUES(rerank_threshold),
			rrf_vec_weight = VALUES(rrf_vec_weight),
			rrf_es_weight = VALUES(rrf_es_weight),
			rrf_rerank_weight = VALUES(rrf_rerank_weight),
			doc_vec_recall_num = VALUES(doc_vec_recall_num),
			qa_vec_recall_num = VALUES(qa_vec_recall_num),
			es_recall_num = VALUES(es_recall_num),
			es_rerank_min_num = VALUES(es_rerank_min_num),
			rrf_reciprocal_const = VALUES(rrf_reciprocal_const),
			operator = VALUES(operator),
			es_top_n = VALUES(es_top_n),
			text2sql_model = VALUES(text2sql_model),
			text2sql_prompt = VALUES(text2sql_prompt)`

	// getRetrievalConfigByRobotIDFields 查询检索配置字段
	getRetrievalConfigByRobotIDFields = `id,robot_id,enable_vector_recall,enable_es_recall,enable_rrf,enable_text2sql,
	rerank_threshold,rrf_vec_weight,rrf_es_weight,rrf_rerank_weight,doc_vec_recall_num,qa_vec_recall_num,
	es_recall_num,es_rerank_min_num,rrf_reciprocal_const,operator,create_time,update_time,es_top_n,text2sql_model,text2sql_prompt`
	// getRetrievalConfigByRobotID 通过RobotID查询检索配置
	getRetrievalConfigByRobotID = `select %s from t_retrieval_config where robot_id in (?) `
	// getRetrievalConfigs 查询所有检索配置
	getRetrievalConfigs = `select %s from t_retrieval_config`

	getAppListOrderByUsedCharSize = `
        SELECT
            %s
        FROM
            t_robot
        WHERE
            1=1 %s
        ORDER BY
            used_char_size DESC
        LIMIT ?,?
    `

	getMaxAppID = `
        SELECT
            MAX(id)
        FROM
            t_robot
    `
)

// GetMaxAppID 获取最大应用ID
func (d *dao) GetMaxAppID(ctx context.Context) (uint64, error) {
	var maxID []sql.NullInt64
	querySQL := getMaxAppID
	if err := d.db.Select(ctx, &maxID, querySQL); err != nil {
		log.ErrorContextf(ctx, "GetMaxRobotID err:%+v sql:%v", err, querySQL)
		return 0, err
	}
	if len(maxID) == 0 || !maxID[0].Valid {
		return 0, errors.New("maxID is null")
	}
	// log.InfoContextf(ctx, "GetMaxAppID maxAppID:%d", maxID[0].Int64)
	return uint64(maxID[0].Int64), nil
}

// GetAppsByCorpID 通过企业ID获取应用数据
func (d *dao) GetAppsByCorpID(ctx context.Context, corpID uint64) ([]*model.AppDB, error) {
	args := make([]any, 0, 2)
	args = append(args, corpID, model.AppIsNotDeleted)
	querySQL := fmt.Sprintf(getAppByCorpID, appFields)
	apps := make([]*model.AppDB, 0)
	if err := d.db.QueryToStructs(ctx, &apps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取应用信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return apps, nil
}

// GetAppsByCorpIDAndBizIDList 通过企业ID和应用ID获取应用信息
func (d *dao) GetAppsByCorpIDAndBizIDList(ctx context.Context, corpID uint64, appBizIDList []uint64) (
	[]*model.AppDB, error) {
	if len(appBizIDList) == 0 {
		return nil, nil
	}
	args := make([]any, 0, 2+len(appBizIDList))
	args = append(args, corpID, model.AppIsNotDeleted)
	for _, id := range appBizIDList {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getAppByCorpIDAndAppBizIDs, appFields, placeholder(len(appBizIDList)))
	robots := make([]*model.AppDB, 0)
	if err := d.db.QueryToStructs(ctx, &robots, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过企业ID和应用ID获取应用信息 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return robots, nil
}

// GetAppByName 通过应用名称获取应用数据
func (d *dao) GetAppByName(ctx context.Context, corpID uint64, name string) (*model.AppDB, error) {
	args := make([]any, 0, 4)
	args = append(args, corpID, name, name, model.AppIsNotDeleted)
	querySQL := fmt.Sprintf(getAppByName, appFields)
	apps := make([]*model.AppDB, 0)
	if err := d.db.QueryToStructs(ctx, &apps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取应用信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(apps) == 0 {
		return nil, nil
	}
	return apps[0], nil
}

// ModifyApp 更新应用配置信息
func (d *dao) ModifyApp(ctx context.Context, appDB *model.AppDB) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		var fields []string
		fields = append(fields, "name=:name", "description=:description",
			"model_name=:model_name", "preview_json=:preview_json", "staff_id=:staff_id", "update_time=:update_time")
		if len(appDB.AppStatusReason) > 0 {
			fields = append(fields, "app_status_reason=:app_status_reason")
		}
		sql := fmt.Sprintf(updateApp, strings.Join(fields, ","))
		if _, err := tx.NamedExecContext(ctx, sql, appDB); err != nil {
			log.ErrorContextf(ctx, "更新应用配置失败, sql: %s, args: %+v, err: %+v", sql, appDB, err)
			return err
		}
		log.DebugContextf(ctx, "更新应用配置成功 sql:%s args: %+v", sql, appDB)
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新应用配置失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateAppVectorIndex 按应用维度创建相似库、评测库
func (d *dao) CreateAppVectorIndex(ctx context.Context, appDB *model.AppDB) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.createAppVectorIndex(ctx, tx, appDB)
	}); err != nil {
		log.ErrorContextf(ctx, "按机器人维度创建相似库、评测库失败 err:%+v", err)
		return err
	}
	return nil
}

// DeleteApp 删除应用
func (d *dao) DeleteApp(ctx context.Context, appDB *model.AppDB) error {
	querySQL := deleteApp
	appDB.IsDeleted = model.AppIsDeleted
	appDB.UpdateTime = time.Now()
	if _, err := d.db.NamedExec(ctx, querySQL, appDB); err != nil {
		log.ErrorContextf(ctx, "删除应用失败 sql:%s args:%+v err:%+v", querySQL, appDB, err)
		return err
	}
	return nil
}

// GetAppByAppBizID 通过对外ID获取应用信息
func (d *dao) GetAppByAppBizID(ctx context.Context, bID uint64) (*model.AppDB, error) {
	args := make([]any, 0, 1)
	args = append(args, bID)
	querySQL := fmt.Sprintf(getAppByBusinessID, appFields)
	apps := make([]*model.AppDB, 0)
	if err := d.db.QueryToStructs(ctx, &apps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取应用信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(apps) == 0 {
		return nil, nil
	}
	return apps[0], nil
}

func (d *dao) createAppVectorIndex(ctx context.Context, tx *sqlx.Tx, appDB *model.AppDB) error {
	if appDB.HasCreateVectorIndexDone() {
		return nil
	}
	embeddingConf, _, err := appDB.GetEmbeddingConf()
	if err != nil {
		return err
	}
	if err = d.CreateAllVectorIndex(ctx, appDB.ID, embeddingConf.Version, model.EmptyEmbeddingModelName); err != nil {
		return err
	}
	appDB.IsCreateVectorIndex = model.IsCreateVectorIndexDone
	appDB.UpdateTime = time.Now()
	querySQL := updateAppCreateVectorIndexDone
	if _, err = tx.NamedExecContext(ctx, querySQL, appDB); err != nil {
		log.ErrorContextf(ctx, "更新应用维度创建相似库、评测库失败 sql:%s args:%+v err:%+v",
			querySQL, appDB, err)
		return err
	}
	return nil
}

// CreateAllVectorIndex 创建相似库、评测库
func (d *dao) CreateAllVectorIndex(ctx context.Context, robotID uint64, embeddingVersion uint64, embeddingModelName string) error {
	if _, err := d.CreateIndex(
		ctx,
		&pb.CreateIndexReq{
			RobotId:            robotID,
			IndexId:            model.SimilarVersionID,
			EmbeddingVersion:   embeddingVersion,
			DocType:            model.DocTypeQA,
			EmbeddingModelName: embeddingModelName,
		},
	); err != nil {
		return err
	}
	if _, err := d.CreateIndex(
		ctx,
		&pb.CreateIndexReq{
			RobotId:            robotID,
			IndexId:            model.ReviewVersionID,
			EmbeddingVersion:   embeddingVersion,
			DocType:            model.DocTypeQA,
			EmbeddingModelName: embeddingModelName,
		},
	); err != nil {
		return err
	}
	if _, err := d.CreateIndex(
		ctx,
		&pb.CreateIndexReq{
			RobotId:            robotID,
			IndexId:            model.SegmentReviewVersionID,
			EmbeddingVersion:   embeddingVersion,
			DocType:            model.DocTypeSegment,
			EmbeddingModelName: embeddingModelName,
		},
	); err != nil {
		return err
	}
	if _, err := d.CreateIndex(
		ctx,
		&pb.CreateIndexReq{
			RobotId:            robotID,
			IndexId:            model.RejectedQuestionReviewVersionID,
			EmbeddingVersion:   embeddingVersion,
			DocType:            model.DocTypeRejectedQuestion,
			EmbeddingModelName: embeddingModelName,
		},
	); err != nil {
		return err
	}
	return nil
}

// DeleteVectorIndex 删除指定的向量库
func (d *dao) DeleteVectorIndex(ctx context.Context, robotID uint64, botBizID uint64,
	embeddingVersion uint64, embeddingModelName string, indexIds []uint64) error {
	if len(indexIds) == 0 {
		return nil
	}
	for _, indexId := range indexIds {
		if _, err := d.DeleteIndex(
			ctx,
			&pb.DeleteIndexReq{
				RobotId:            robotID,
				IndexId:            indexId,
				EmbeddingVersion:   embeddingVersion,
				BotBizId:           botBizID,
				EmbeddingModelName: embeddingModelName,
			},
		); err != nil {
			return err
		}
	}

	return nil
}

// GetDocType 获取文档类型
func (d *dao) GetDocType(ctx context.Context, indexType uint64) (uint32, error) {
	var docType uint32
	switch indexType {
	case model.SimilarVersionID, model.ReviewVersionID:
		docType = model.DocTypeQA
	case model.SegmentReviewVersionID:
		docType = model.DocTypeSegment
	case model.RejectedQuestionReviewVersionID:
		docType = model.DocTypeRejectedQuestion
	case model.RealtimeSegmentVersionID:
		docType = model.DocTypeSegment
	case model.SegmentImageReviewVersionID, model.RealtimeSegmentImageVersionID:
		docType = model.DocTypeImage
	default:
		log.ErrorContextf(ctx, "unknown indexType(%d), err: %v", indexType, errs.ErrUnknownDocTypeForIndexType)
		return 0, errs.ErrUnknownDocTypeForIndexType
	}
	return docType, nil
}

// ReCreateVectorIndex 重建相似库、评测库
func (d *dao) ReCreateVectorIndex(
	ctx context.Context, robotID uint64, indexType uint64, embeddingVersion uint64, botBizID uint64, wait time.Duration,
	embeddingModelName string,
) error {
	docType, err := d.GetDocType(ctx, indexType)
	if err != nil {
		log.ErrorContextf(ctx, "获取文档类型失败, err: %+v", err)
		return err
	}
	delReq := &pb.DeleteIndexReq{
		RobotId:            robotID,
		IndexId:            indexType,
		EmbeddingVersion:   embeddingVersion,
		BotBizId:           botBizID,
		EmbeddingModelName: embeddingModelName,
	}
	if _, err = d.DeleteIndex(ctx, delReq); err != nil {
		log.ErrorContextf(ctx, "删除索引失败, req: %+v, err: %+v", delReq, err)
		return err
	}
	time.Sleep(wait)
	createReq := &pb.CreateIndexReq{
		RobotId:            robotID,
		IndexId:            indexType,
		EmbeddingVersion:   embeddingVersion,
		DocType:            docType,
		BotBizId:           botBizID,
		EmbeddingModelName: embeddingModelName,
	}
	if _, err = d.CreateIndex(ctx, createReq); err != nil {
		log.ErrorContextf(ctx, "创建索引失败, req: %+v, err: %+v", createReq, err)
		return err
	}
	return nil
}

// GetAppByAppKey 通过应用key获取应用信息
func (d *dao) GetAppByAppKey(ctx context.Context, appKey string) (*model.AppDB, error) {
	args := make([]any, 0)
	args = append(args, appKey, model.AppIsNotDeleted)
	querySQL := fmt.Sprintf(getAppByAppKey, appFields)
	apps := make([]*model.AppDB, 0)
	if err := d.db.QueryToStructs(ctx, &apps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过应用key获取应用信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(apps) == 0 {
		return nil, nil
	}
	return apps[0], nil
}

// fillAppListParams TODO
func fillAppListParams(corpID uint64, staffIDs, appBizIDList []uint64, appTypeList []string,
	deleteFlags []uint32, keywords, appKey string) ([]any, string) {
	var args []any
	var condition string
	if corpID > 0 {
		condition += " AND corp_id = ?"
		args = append(args, corpID)
	}
	if len(deleteFlags) > 0 {
		condition += fmt.Sprintf(" AND is_deleted IN (%s)", placeholder(len(deleteFlags)))
		for _, deletedFlag := range deleteFlags {
			args = append(args, deletedFlag)
		}
	}
	if len(appBizIDList) > 0 {
		condition += fmt.Sprintf(" AND business_id IN (%s)", placeholder(len(appBizIDList)))
		for _, appBizID := range appBizIDList {
			args = append(args, appBizID)
		}
	}
	if len(appTypeList) > 0 {
		condition += fmt.Sprintf(" AND app_type IN (%s)", placeholder(len(appTypeList)))
		for _, appType := range appTypeList {
			args = append(args, appType)
		}
	}
	keywordArgs, keywordCondition := fillGetAppListKeywordParam(staffIDs, keywords)
	if len(keywordCondition) > 0 {
		condition += keywordCondition
		args = append(args, keywordArgs...)
	}
	if len(appKey) > 0 {
		condition += " AND app_key = ?"
		args = append(args, appKey)
	}
	return args, condition
}

func fillGetAppListKeywordParam(staffIDs []uint64, keywords string) ([]any, string) {
	var args []any
	var condition string
	if len(staffIDs) > 0 && len(keywords) == 0 {
		condition += fmt.Sprintf(" AND staff_id IN (%s)", placeholder(len(staffIDs)))
		for _, staffID := range staffIDs {
			args = append(args, staffID)
		}
		return args, condition
	}
	if len(staffIDs) == 0 && len(keywords) > 0 {
		condition += " AND name LIKE ?"
		args = append(args, fmt.Sprintf("%%%s%%", keywords))
		return args, condition
	}
	if len(staffIDs) > 0 && len(keywords) >= 0 {
		condition += fmt.Sprintf(" AND (staff_id IN (%s) OR name LIKE ?)", placeholder(len(staffIDs)))
		for _, staffID := range staffIDs {
			args = append(args, staffID)
		}
		args = append(args, fmt.Sprintf("%%%s%%", keywords))
	}
	return args, condition
}

// GetAppCount 获取应用数量
func (d *dao) GetAppCount(ctx context.Context, corpID uint64, staffIDs, appBizIDList []uint64,
	appTypeList []string, deleteFlags []uint32, keywords, appKey string) (uint64, error) {
	var total uint64
	args, condition := fillAppListParams(corpID, staffIDs, appBizIDList, appTypeList, deleteFlags,
		keywords, appKey)
	querySQL := fmt.Sprintf(getAppCount, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get app count sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetAppList 获取应用列表
func (d *dao) GetAppList(ctx context.Context, corpID uint64, staffIDs, appBizIDList []uint64,
	appTypeList []string, deleteFlags []uint32, keywords, appKey string, page, pageSize uint32) (
	[]*model.AppDB, error) {
	var appDBList []*model.AppDB
	args, condition := fillAppListParams(corpID, staffIDs, appBizIDList, appTypeList, deleteFlags,
		keywords, appKey)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getAppList, appFields, condition)
	if err := d.db.QueryToStructs(ctx, &appDBList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get app list sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return appDBList, nil
}

// GetAppListOrderByUsedCharSize 获取应用列表按照字符数使用情况排序
func (d *dao) GetAppListOrderByUsedCharSize(ctx context.Context, corpID uint64, appBizIDList []uint64,
	appTypeList []string, deleteFlags []uint32, page, pageSize uint32) ([]*model.AppDB, error) {
	var appDBList []*model.AppDB
	args, condition := fillAppListOrderByUsedCharSizeParams(corpID, appBizIDList, appTypeList, deleteFlags)
	if page > 0 && pageSize > 0 {
		offset := (page - 1) * pageSize
		args = append(args, offset, pageSize)
	} else {
		args = append(args, 0, 10)
	}
	column := strings.Join([]string{RobotTblColId, RobotTblColBusinessId, RobotTblColUsedCharSize, RobotTblColName, RobotTblColIsShared}, ",")
	querySQL := fmt.Sprintf(getAppListOrderByUsedCharSize, column, condition)
	if err := d.db.QueryToStructs(ctx, &appDBList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetAppListOrderByUsedCharSize failed|sql:%s args:%+v err:%+v",
			querySQL, args, err)
		return nil, err
	}
	return appDBList, nil
}

// fillAppListOrderByUsedCharSizeParams 填充查询参数
func fillAppListOrderByUsedCharSizeParams(corpID uint64, appBizIDList []uint64, appTypeList []string,
	deleteFlags []uint32) ([]any, string) {
	var args []any
	var condition string
	if corpID > 0 {
		condition += " AND corp_id = ?"
		args = append(args, corpID)
	}
	if len(deleteFlags) > 0 {
		condition += fmt.Sprintf(" AND is_deleted IN (%s)", placeholder(len(deleteFlags)))
		for _, deletedFlag := range deleteFlags {
			args = append(args, deletedFlag)
		}
	}
	if len(appBizIDList) > 0 {
		condition += fmt.Sprintf(" AND business_id IN (%s)", placeholder(len(appBizIDList)))
		for _, appBizID := range appBizIDList {
			args = append(args, appBizID)
		}
	}
	if len(appTypeList) > 0 {
		condition += fmt.Sprintf(" AND app_type IN (%s)", placeholder(len(appTypeList)))
		for _, appType := range appTypeList {
			args = append(args, appType)
		}
	}
	return args, condition
}

// updateAppRelease 更新应用发布
func (d *dao) updateAppRelease(ctx context.Context, tx *sqlx.Tx, json string,
	status uint32, robotID uint64) error {
	sql := updateAppReleaseJSON
	args := []any{json, robotID}
	if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "更新应用属性失败, sql:%s args:%+v err:%+v", sql, args, err)
		return err
	}
	// 第一次发布需要变更机器人状态为运行中
	sql = updateAppStatus
	args = []any{status, "", robotID, model.AppStatusInit}
	if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "更新应用属性失败, sql:%s args:%+v err:%+v", sql, args, err)
		return err
	}
	return nil
}

// ModifyAppTokenUsage 更新应用token用量
func (d *dao) ModifyAppTokenUsage(ctx context.Context, app *model.AppDB) error {
	usageReq := &chat.TokenUsageReq{
		BotBizId: []uint64{app.BusinessID},
	}
	usageRsp, err := d.chatCli.GetTokenUsage(ctx, usageReq)
	if err != nil {
		log.ErrorContextf(ctx, "获取应用token用量失败 req:%+v err:%+v", usageReq, err)
		return err
	}
	log.DebugContextf(ctx, "获取应用token用量成功 req:%+v rsp:%+v", usageReq, usageRsp)
	if len(usageRsp.GetUsage()) == 0 {
		return nil
	}
	app.TokenUsage = usageRsp.GetUsage()[0].Usage
	app.UpdateTime = time.Now()
	querySQL := updateAppUsage
	if _, err = d.db.NamedExec(ctx, querySQL, app); err != nil {
		log.ErrorContextf(ctx, "更新应用token用量失败 sql:%s args:%+v err:%+v", querySQL, app, err)
		return err
	}
	return nil
}

// UpdateTrialCorpAppStatus 更新试用企业机器人状态
func (d *dao) UpdateTrialCorpAppStatus(
	ctx context.Context, corpID uint64, appStatus uint32, appStatusReason string) error {
	args := []any{appStatus, appStatusReason, corpID, model.AppIsNotDeleted}
	querySQL := updateTrialCorpAppStatus
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新试用企业应用状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// GetAllValidAppIDs 获取所有应用ID
func (d *dao) GetAllValidAppIDs(ctx context.Context) ([]uint64, error) {
	maxID, err := d.GetMaxAppID(ctx)
	if err != nil {
		return nil, err
	}
	args := make([]any, 3)
	querySQL := fmt.Sprintf(getValidAppIDs, "id")
	ids := make([]uint64, 0)
	pageSize := uint64(2000)
	for i := uint64(0); i <= maxID; i += pageSize {
		args[0] = i
		args[1] = i + pageSize
		args[2] = model.AppIsNotDeleted
		apps := make([]*model.AppDB, 0)
		if err := d.db.QueryToStructs(ctx, &apps, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "获取所有应用信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return nil, err
		}
		for _, v := range apps {
			ids = append(ids, v.ID)
		}
	}
	log.InfoContextf(ctx, "GetAllValidAppIDs len:%d", len(ids))
	return ids, nil
}

// GetAppByID 获取应用信息
func (d *dao) GetAppByID(ctx context.Context, id uint64) (*model.AppDB, error) {
	args := make([]any, 0)
	args = append(args, id)
	querySQL := fmt.Sprintf(getAppByID, appFields)
	apps := make([]*model.AppDB, 0)
	if err := d.db.QueryToStructs(ctx, &apps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取应用信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(apps) == 0 {
		return nil, nil
	}
	return apps[0], nil
}

// GetBotBizIDByID 获取应用ID
func (d *dao) GetBotBizIDByID(ctx context.Context, id uint64) (uint64, error) {
	key := fmt.Sprintf("bot_id:bot_biz_id:%d", id)
	botBizID, err := redis.Uint64(d.RedisCli().Do(ctx, "GET", key))
	if err == nil {
		return botBizID, nil
	}
	app, err := d.GetAppByID(ctx, id)
	if err != nil {
		return 0, err
	}
	if _, err := d.RedisCli().Do(ctx, "SET", key, app.BusinessID); err != nil {
		log.WarnContextf(ctx, "缓存失败 SET %s %+v err:%+v", key, app.BusinessID, err)
	}
	return app.BusinessID, nil
}

// UpdateAppUsedCharSize 更新应用使用字符数
func (d *dao) UpdateAppUsedCharSize(ctx context.Context, tx *sqlx.Tx, charSize int64, appID uint64) error {
	sql := updateAppDiffUsedCharSize
	args := []any{charSize, appID}
	db := knowClient.DBClient(ctx, robotTableName, appID, []client.Option{}...)
	err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			log.ErrorContextf(ctx, "增量更新应用已使用字符数失败, sql:%s args:%+v err:%+v", sql, args, err)
			return err
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "增量更新应用已使用字符数失败, charSize:%d robotID:%d err:%+v", charSize, appID, err)
		return err
	}
	return nil
}

// UpdateAppUsedCharSizeTx 更新应用使用字符数(支持事物)
func (d *dao) UpdateAppUsedCharSizeTx(ctx context.Context, charSize int64, appID uint64) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.UpdateAppUsedCharSize(ctx, tx, charSize, appID)
	}); err != nil {
		log.ErrorContextf(ctx, "增量更新应用已使用字符数失败, charSize:%d robotID:%d err:%+v", charSize, appID, err)
		return err
	}
	return nil
}

// UpdateAppUsedCharSizeNotTx 更新应用使用字符数
func (d *dao) UpdateAppUsedCharSizeNotTx(ctx context.Context, charSize int64, appID uint64) error {
	sql := updateAppDiffUsedCharSize
	args := []any{charSize, appID}
	if _, err := d.db.Exec(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "增量更新应用已使用字符数失败, sql:%s args:%+v err:%+v", sql, args, err)
		return err
	}
	return nil
}

// UpdateAppCharSize 更新应用使用字符数
func (d *dao) UpdateAppCharSize(ctx context.Context, id, size uint64) error {
	sql := updateAppUsedCharSize
	args := []any{size, id}
	if _, err := d.db.Exec(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "更新应用属性失败, sql:%s args:%+v err:%+v", sql, args, err)
		return err
	}
	return nil
}

// GetAppByIDRange 通过ID范围获取机器人信息
func (d *dao) GetAppByIDRange(ctx context.Context, startID, endID uint64, limit uint32) (
	[]*model.AppDB, error) {
	args := make([]any, 0, 3)
	args = append(args, startID, endID, limit)
	querySQL := fmt.Sprintf(getAppByIDRange, appFields)
	apps := make([]*model.AppDB, 0)
	if err := d.db.QueryToStructs(ctx, &apps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取机器人信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return apps, nil
}

// SyncAppData 同步机器人数据（机器人转化为应用数据）
func (d *dao) SyncAppData(ctx context.Context, apps []*model.AppDB) error {
	if len(apps) == 0 {
		return nil
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, robot := range apps {
			querySQL := syncAppData
			if _, err := tx.NamedExecContext(ctx, querySQL, robot); err != nil {
				log.ErrorContextf(ctx, "同步机器人数据失败,id:%d, err:%+v", robot.ID, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "同步机器人数据失败 err:%+v", err)
		return err
	}
	return nil
}

// FlushAppData 刷新机器人preview_json和release_json数据
func (d *dao) FlushAppData(ctx context.Context, apps []*model.AppDB) error {
	if len(apps) == 0 {
		return nil
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, robot := range apps {
			querySQL := flushAppData
			if _, err := tx.NamedExecContext(ctx, querySQL, robot); err != nil {
				log.ErrorContextf(ctx, "FlushAppJsonData error,id:%d, err:%+v", robot.ID, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "FlushAppJsonData error err:%+v", err)
		return err
	}
	return nil
}

// ModifyAppJSON 更新应用配置信息
func (d *dao) ModifyAppJSON(ctx context.Context, appDB *model.AppDB) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, updateAppJSON, appDB); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAppJSON, appDB, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新应用配置失败 err:%+v", err)
		return err
	}
	return nil
}

// ModifyAppPreviewJSON 更新应用待发布配置信息
func (d *dao) ModifyAppPreviewJSON(ctx context.Context, appDB *model.AppDB) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, updateAppPreviewJSON, appDB); err != nil {
			log.ErrorContextf(ctx, "更新应用待发布配置信息 sql:%s args:%+v err:%+v", updateAppPreviewJSON, appDB, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新应用待发布配置信息 err:%+v", err)
		return err
	}
	return nil
}

// ModifyAppOfOp OP更新应用配置信息
func (d *dao) ModifyAppOfOp(ctx context.Context, appDB *model.AppDB) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, updateAppOfOp, appDB); err != nil {
			log.ErrorContextf(ctx, "OP更新应用配置失败 sql:%s args:%+v err:%+v", updateAppOfOp, appDB, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "OP更新应用配置失败 err:%+v", err)
		return err
	}
	return nil
}

// GetRetrievalConfig 获取检索配置
func (d *dao) GetRetrievalConfig(ctx context.Context, robotID uint64) (model.RetrievalConfig, error) {
	retrievalConfig := model.RetrievalConfig{}
	keys := model.GetRetrievalConfigKey(robotID)
	defaultConfigKey := model.GetRetrievalConfigKey(model.DefaultConfigRobotID)
	log.DebugContextf(ctx, "GetRetrievalConfig redis key:%s", keys)
	robotRetrievalConfig, err := redis.Bytes(d.retrievalConfigRedis.Do(ctx, "GET", keys))
	if errors.Is(err, redis.ErrNil) { // 配置不存在取默认配置
		defaultRetrievalConfig, err := redis.Bytes(d.retrievalConfigRedis.Do(ctx, "GET", defaultConfigKey))
		if err != nil { // 默认配置手动写入,如果不存在返回异常
			log.ErrorContextf(ctx, "defaultRetrievalConfig GET failed err:%v", err)
			return retrievalConfig, err
		}
		err = jsoniter.Unmarshal(defaultRetrievalConfig, &retrievalConfig)
		if err != nil {
			log.ErrorContextf(ctx, "defaultRetrievalConfig Unmarshal failed err:%v", err)
			return retrievalConfig, err
		}
		log.DebugContextf(ctx, "GetRetrievalConfig defaultRetrievalConfig:%v", retrievalConfig)
		return retrievalConfig, nil
	} else if err != nil {
		log.ErrorContextf(ctx, "Redis SET failed err:%v", err)
		return retrievalConfig, err
	}
	err = jsoniter.Unmarshal(robotRetrievalConfig, &retrievalConfig)
	if err != nil {
		log.ErrorContextf(ctx, "retrievalConfig Unmarshal failed err:%v", err)
		return retrievalConfig, err
	}
	return retrievalConfig, nil
}

// SaveRetrievalConfig 保存检索配置
func (d *dao) SaveRetrievalConfig(ctx context.Context, robotID uint64, retrievalConfig model.RetrievalConfig,
	operator string) error {
	needUpdate, err := d.checkRetrievalConfigNeedUpdate(ctx, robotID, retrievalConfig)
	if err != nil {
		log.ErrorContextf(ctx, "SaveRetrievalConfig checkRetrievalConfigNeedUpdate failed err:%v", err)
		return err
	}
	if !needUpdate {
		log.InfoContextf(ctx, "SaveRetrievalConfig nothing need update,robotID:%d retrievalConfig:%+v",
			robotID, retrievalConfig)
		return nil
	}
	execSQL := fmt.Sprintf(insertOrUpdateSearchSetting, insertSearchSettingFields)
	retrievalConfigDB := model.RetrievalConfigDB{
		EnableVectorRecall: retrievalConfig.EnableVectorRecall,
		EnableRRF:          retrievalConfig.EnableRrf,
		EnableESRecall:     retrievalConfig.EnableEsRecall,
		EnableText2Sql:     retrievalConfig.EnableText2Sql,
		ReRankThreshold:    retrievalConfig.ReRankThreshold,
		RRFVecWeight:       retrievalConfig.RRFVecWeight,
		RRFEsWeight:        retrievalConfig.RRFEsWeight,
		RRFReRankWeight:    retrievalConfig.RRFReRankWeight,
		DocVecRecallNum:    retrievalConfig.DocVecRecallNum,
		QaVecRecallNum:     retrievalConfig.QaVecRecallNum,
		EsRecallNum:        retrievalConfig.EsRecallNum,
		EsReRankMinNum:     retrievalConfig.EsReRankMinNum,
		RRFReciprocalConst: retrievalConfig.RRFReciprocalConst,
		RobotID:            int64(robotID),
		Operator:           operator,
		EsTopN:             retrievalConfig.EsTopN,
		Text2sqlModel:      retrievalConfig.Text2sqlModel,
		Text2sqlPrompt:     retrievalConfig.Text2sqlPrompt,
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, execSQL, retrievalConfigDB); err != nil {
			log.ErrorContextf(ctx, "SaveRetrievalConfig sql:%s args:%+v err:%+v", execSQL, retrievalConfigDB, err)
			return err
		}
		keys := model.GetRetrievalConfigKey(robotID)
		reqJSON, _ := jsoniter.MarshalToString(retrievalConfig)
		_, err := d.retrievalConfigRedis.Do(ctx, "SET", keys, reqJSON)
		if err != nil {
			log.ErrorContextf(ctx, "Redis SET failed err:%v", err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "SaveRetrievalConfig err:%+v", err)
		return err
	}
	return nil
}

// SyncRetrievalConfigFromDB 应用的检索配置从DB同步到redis robotID为空则表示同步所有已配置的应用
func (d *dao) SyncRetrievalConfigFromDB(ctx context.Context, robotIDs []uint64) error {
	dbRetrievalConfigs, err := d.GetRetrievalConfigsFromDB(ctx, robotIDs)
	if err != nil {
		return err
	}
	for _, dbRetrievalConfig := range dbRetrievalConfigs {
		retrievalConfig := dbRetrievalConfig.ConvertToRetrievalConfig()
		robotID := uint64(dbRetrievalConfig.RobotID)
		redisRetrievalConfig, err := d.GetRetrievalConfig(ctx, robotID)
		if err != nil {
			log.ErrorContextf(ctx, "GetRetrievalConfig err:%+v, robotID:%d", err, robotID)
			return err
		}
		if !retrievalConfig.CheckRetrievalConfigDiff(redisRetrievalConfig) {
			log.InfoContextf(ctx,
				"CheckRetrievalConfigDiff NOT equal and update, robotID:%d, retrievalConfig:%+v, redis:%+v",
				robotID, retrievalConfig, redisRetrievalConfig)
			keys := model.GetRetrievalConfigKey(robotID)
			reqJSON, _ := jsoniter.MarshalToString(retrievalConfig)
			_, err := d.retrievalConfigRedis.Do(ctx, "SET", keys, reqJSON)
			if err != nil {
				log.ErrorContextf(ctx, "Redis SET failed err:%v", err)
				return err
			}
		} else {
			log.InfoContextf(ctx,
				"CheckRetrievalConfigDiff equal and NOT update, robotID:%d, retrievalConfig:%+v, redis:%+v",
				robotID, retrievalConfig, redisRetrievalConfig)
		}
	}
	return nil
}

// GetRetrievalConfigByRobotID 通过RobotID查询检索配置
func (d *dao) GetRetrievalConfigByRobotID(ctx context.Context, robotID uint64) (*model.RetrievalConfigDB, error) {
	args := make([]any, 0, 1)
	args = append(args, robotID)
	querySQL := fmt.Sprintf(getRetrievalConfigByRobotID, getRetrievalConfigByRobotIDFields)
	retrievalConfigs := make([]*model.RetrievalConfigDB, 0)
	if err := d.db.QueryToStructs(ctx, &retrievalConfigs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取应用检索配置失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(retrievalConfigs) == 0 {
		return nil, nil
	}
	return retrievalConfigs[0], nil
}

// GetRetrievalConfigsFromDB 通过RobotID批量查询检索配置，为空则查询所有
func (d *dao) GetRetrievalConfigsFromDB(ctx context.Context, robotIDs []uint64) ([]*model.RetrievalConfigDB, error) {
	args := make([]any, 0, 1)
	var querySQL string
	var err error
	if len(robotIDs) == 0 {
		querySQL = fmt.Sprintf(getRetrievalConfigs, getRetrievalConfigByRobotIDFields)
	} else {
		querySQL = fmt.Sprintf(getRetrievalConfigByRobotID, getRetrievalConfigByRobotIDFields)
		querySQL, args, err = sqlx.In(querySQL, robotIDs)
		if err != nil {
			log.ErrorContextf(ctx, "GetRetrievalConfigsFromDB sqlx.In err:%+v, robotIDs:%+v", err, robotIDs)
			return nil, err
		}
	}
	retrievalConfigs := make([]*model.RetrievalConfigDB, 0)
	if err = d.db.QueryToStructs(ctx, &retrievalConfigs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetRetrievalConfigsFromDB select error, sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	log.InfoContextf(ctx, "GetRetrievalConfigsFromDB success, count:%d, retrievalConfigs:%+v", len(retrievalConfigs),
		retrievalConfigs)
	return retrievalConfigs, nil
}

// checkRetrievalConfigNeedUpdate 检查是否需要更新
func (d *dao) checkRetrievalConfigNeedUpdate(ctx context.Context, robotID uint64,
	retrievalConfig model.RetrievalConfig) (bool, error) {
	log.InfoContextf(ctx, "checkRetrievalConfigNeedUpdate robotID:%d,retrievalConfig:%+v",
		robotID, retrievalConfig)
	redisRetrievalConfig, err := d.GetRetrievalConfig(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "检查是否需要更新  GetRetrievalConfig robotID:%d,err:%+v", robotID, err)
		return false, err
	}
	log.InfoContextf(ctx, "checkRetrievalConfigNeedUpdate redisRetrievalConfig%+v", redisRetrievalConfig)
	// 如果输入值与缓存中或者默认配置中相同，则不更新
	if redisRetrievalConfig.CheckRetrievalConfigDiff(retrievalConfig) {
		return false, nil
	}
	dbRetrievalConfig, err := d.GetRetrievalConfigByRobotID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "检查是否需要更新  GetRetrievalConfigByRobotID robotID:%d,err:%+v", robotID, err)
		return false, err
	}
	// db中没有数据直接更新
	if dbRetrievalConfig == nil {
		return true, nil
	}
	log.InfoContextf(ctx, "checkRetrievalConfigNeedUpdate dbRetrievalConfig:%+v", dbRetrievalConfig)
	// 输入值跟db中相同,不更新
	if dbRetrievalConfig.ConvertToRetrievalConfig().CheckRetrievalConfigDiff(retrievalConfig) {
		log.InfoContextf(ctx, "checkRetrievalConfigNeedUpdate redis is db diff, dbRetrievalConfig:%+v,"+
			"redisRetrievalConfig:%+v", dbRetrievalConfig, redisRetrievalConfig)
		return false, nil
	}
	return true, nil
}
