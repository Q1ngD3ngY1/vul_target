package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"slices"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
)

const (
	attributeFields = `
        id,business_id,robot_id,attr_key,name,is_updating,release_status,next_action,is_deleted,deleted_time,create_time,update_time
    `
	getAttributeTotal = `
        SELECT
            COUNT(1)
        FROM
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ? %s
    `
	getAttributeList = `
        SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ? %s
        ORDER BY 
            update_time DESC,id DESC
        LIMIT ?,? 
    `
	getAttributeByIDs = `
        SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ? AND id IN (%s)
    `
	getAttributeByBizIDs = `
        SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ? AND business_id IN (%s)
    `
	getAttributeByKeys = `
        SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ? AND attr_key IN (%s)
    `
	getAttributeByNames = `
        SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ? AND name IN (%s)
    `
	getAttributeByRobotID = `
		 SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ?
	`
	getAttributeKeyAndIDsByRobotID = `
		 SELECT
            %s
        FROM 
            t_attribute
        WHERE
            robot_id = ? AND is_deleted = ?
	`
	getAttributeKeyAndIDsByRobotIDProd = `
		 SELECT
            %s
        FROM 
            t_attribute_prod
        WHERE
            robot_id = ? AND is_deleted = ?
	`
	getAttributeKeysDelStatus = `
        SELECT 
            id,attr_id,is_deleted,attr_key
        FROM
            t_attribute_prod
        WHERE
            robot_id = ? AND attr_key IN (%s)
    `
	createAttribute = `
        INSERT INTO
            t_attribute (%s)
        VALUES
            (:id,:business_id,:robot_id,:attr_key,:name,:is_updating,:release_status,:next_action,:is_deleted,deleted_time,NOW(),NOW())
    `
	updateAttribute = `
        UPDATE 
            t_attribute
        SET 
            name = :name,
            is_updating = :is_updating,
            next_action = :next_action,
            release_status = :release_status,
            is_updating = :is_updating,
            update_time = :update_time
        WHERE
            robot_id = :robot_id AND is_deleted = :is_deleted AND id = :id
    `
	deleteAttribute = `
        UPDATE
            t_attribute
        SET 
            is_deleted = ?,
            deleted_time = ?,
            next_action = IF(next_action = ?, ?, ?),
            release_status = ?,
            update_time = now()
        WHERE
            robot_id = ? AND is_deleted = ? AND id IN (%s)
    `
	attributeModifyUpdating = `
		 UPDATE 
            t_attribute
        SET 
 
            is_updating = :is_updating,
            update_time = :update_time,
            release_status = :release_status
        WHERE
            robot_id = :robot_id AND is_deleted = :is_deleted AND id = :id
	`
	attributeLabelFields = `
        id,robot_id,business_id,attr_id,name,similar_label,release_status,next_action,is_deleted,create_time,update_time
    `
	attributeLabelFieldsProd = `
        id,robot_id,business_id,attr_id,label_id,name,similar_label,is_deleted,create_time,update_time
    `
	getAttributeLabelCount = `
		SELECT 
			COUNT(1)
		FROM
			t_attribute_label
		WHERE
			robot_id = ? AND attr_id = ? AND is_deleted = ? %s
	`
	getAttributeLabelIdList = `
		SELECT 
			id 
		FROM
			t_attribute_label
		WHERE
			robot_id = ? AND attr_id = ? AND is_deleted = ? %s
		ORDER BY 
			id DESC
		LIMIT ?
	`
	getAttributeLabelList = `
		SELECT 
			%s 
		FROM
			t_attribute_label
		WHERE
			robot_id = ? AND id IN (%s)
		ORDER BY FIELD(id,%s)
	`
	getAttributeLabelByAttrIDs = `
        SELECT 
            %s
        FROM
            t_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND attr_id IN (%s)
    `
	getAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd = `
        SELECT 
            %s
        FROM
            t_attribute_label_prod
        WHERE
            robot_id = ? AND attr_id IN (%s) AND similar_label != "" AND is_deleted = ? 
    `
	getAttributeLabelByIDs = `
        SELECT 
            %s
        FROM
            t_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND id IN (%s)
    `
	getAttributeLabelByBizIDs = `
        SELECT 
            %s
        FROM
            t_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND business_id IN (%s)
    `
	getAttributeLabelByName = `
		 SELECT 
            %s
        FROM
            t_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND attr_id = ? AND (name LIKE ? OR similar_label LIKE ?)
	`
	getAttributeIDByLabelName = `
        SELECT
            DISTINCT attr_id
        FROM
            t_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND name LIKE ?
    `
	createAttributeLabel = `
        INSERT INTO
            t_attribute_label (%s)
        VALUES 
            (:id,:robot_id,:business_id,:attr_id,:name,:similar_label,:release_status,:next_action,:is_deleted,NOW(),NOW())   
    `
	updateAttributeLabel = `
        UPDATE 
            t_attribute_label
        SET 
            name = :name,
            similar_label = :similar_label,
            next_action = :next_action,
            release_status = :release_status,
            update_time = now()
        WHERE
            robot_id = :robot_id AND attr_id = :attr_id AND is_deleted = :is_deleted AND id = :id
    `
	deleteAttributeLabel = `
        UPDATE 
            t_attribute_label
        SET 
            is_deleted = ?, 
            next_action = IF(next_action = ?, ?, ?),
            release_status = ?, 
            update_time = now()
        WHERE
            robot_id = ? AND attr_id = ? AND is_deleted = ? AND id IN (%s)
    `
	deleteAttributeLabelByAttrIDs = `
        UPDATE 
            t_attribute_label
        SET 
            is_deleted = ?, 
            next_action = IF(next_action = ?, ?, ?),
            release_status = ?, 
            update_time = now()
        WHERE
            robot_id = ? AND is_deleted = ? AND attr_id IN (%s)
    `
	docAttributeLabelFields = `
        id,robot_id,doc_id,source,attr_id,label_id,is_deleted,create_time,update_time
    `
	getDocAttributeLabel = `
	    SELECT
            %s
        FROM
            t_doc_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND doc_id IN (%s)
       	ORDER BY
       		id ASC
	`
	getDocAttributeLabelCountByAttrLabelIDs = `
      	SELECT
           	COUNT(1)
      	FROM
           	t_doc_attribute_label
      	WHERE
           	robot_id = ? AND is_deleted = ? AND source = ? %s
    `
	getDocAttributeLabelByAttrLabelIDs = `
    	SELECT
            %s
        FROM
            t_doc_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND source = ? %s
        ORDER BY
         	id DESC
     	LIMIT ?,?
    `
	createDocAttributeLabel = `
        INSERT INTO
            t_doc_attribute_label (%s)
        VALUES
            (:id,:robot_id,:doc_id,:source,:attr_id,:label_id,:is_deleted,NOW(),NOW())
    `
	deleteDocAttributeLabel = `
        UPDATE
            t_doc_attribute_label
        SET
            is_deleted = ?
        WHERE
            robot_id = ? AND doc_id = ? AND is_deleted = ?
    `
	getDocCountByAttributeLabel = `
        SELECT
            COUNT(*)
        FROM
            t_doc
        WHERE
           robot_id = ? AND is_deleted = ? %s
    `
	getAttributeLabelDocIDs = `
     	SELECT
        	DISTINCT doc_id
     	FROM
         	t_doc_attribute_label
     	WHERE
         	robot_id = ? AND is_deleted = ? AND source = ? %s
    `
	qaAttributeLabelFields = `
        id,robot_id,qa_id,source,attr_id,label_id,is_deleted,create_time,update_time
    `
	getQAAttributeLabel = `
        SELECT
            %s
        FROM
            t_qa_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND qa_id IN (%s)
        ORDER BY
       		id ASC
    `
	getQAAttributeLabelCountByAttrLabelIDs = `
      	SELECT
           	COUNT(1)
      	FROM
           	t_qa_attribute_label
      	WHERE
           	robot_id = ? AND is_deleted = ? AND source = ? %s
    `
	getQAAttributeLabelByAttrLabelIDs = `
        SELECT
            %s
        FROM
            t_qa_attribute_label
        WHERE
            robot_id = ? AND is_deleted = ? AND source = ? %s
        ORDER BY
            id DESC
        LIMIT ?,?
    `
	createQAAttributeLabel = `
        INSERT INTO
            t_qa_attribute_label (%s)
        VALUES
            (:id,:robot_id,:qa_id,:source,:attr_id,:label_id,:is_deleted,NOW(),NOW())
    `
	deleteQAAttributeLabel = `
        UPDATE
            t_qa_attribute_label
        SET
            is_deleted = ?
        WHERE
            robot_id = ? AND qa_id = ? AND is_deleted = ?
    `
	getQACountByAttributeLabel = `
        SELECT
            COUNT(*)
        FROM
            t_doc_qa
        WHERE
           robot_id = ? AND is_deleted = ? %s
    `
	getAttributeLabelQAIDs = `
     	SELECT
         	DISTINCT qa_id
     	FROM
         	t_qa_attribute_label
     	WHERE
         	robot_id = ? AND is_deleted = ? AND source = ? %s
    `
)

const (
	attributeProdTableName      = "t_attribute_prod"
	attributeLabelProdTableName = "t_attribute_label_prod"
	qaAttributeLabelTableName   = "t_qa_attribute_label"

	// 查询范围 all(或者传空):标准词和相似词 standard:标准词 similar:相似词
	attributeLabelQueryScopeAll      = "all"
	attributeLabelQueryScopeStandard = "standard"
	attributeLabelQueryScopeSimilar  = "similar"
)

// fillAttributeListParams TODO
func fillAttributeListParams(robotID uint64, query string, ids []uint64) ([]any, string) {
	var args []any
	var condition string
	args = append(args, robotID, model.AttributeIsNotDeleted)
	if len(query) > 0 {
		condition += fmt.Sprintf(" AND (name LIKE ? OR id IN (%s))", getAttributeIDByLabelName)
		queryStr := fmt.Sprintf("%%%s%%", query)
		args = append(args, queryStr, model.AttributeLabelIsNotDeleted, queryStr)
	}
	if len(ids) > 0 {
		condition += fmt.Sprintf(" AND id IN (%s)", placeholder(len(ids)))
		for _, id := range ids {
			args = append(args, id)
		}
	}
	return args, condition
}

func (d *dao) createAttribute(ctx context.Context, tx *sqlx.Tx, attr *model.Attribute) (uint64, error) {
	execSQL := fmt.Sprintf(createAttribute, attributeFields)
	attr.BusinessID = d.GenerateSeqID()
	res, err := tx.NamedExecContext(ctx, execSQL, attr)
	if err != nil {
		log.ErrorContextf(ctx, "创建标签失败 sql:%s args:%+v err:%+v", execSQL, attr, err)
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	attr.ID = uint64(id)
	return uint64(id), nil
}

func (d *dao) createAttributeLabel(ctx context.Context, tx *sqlx.Tx, labels []*model.AttributeLabel) error {
	if len(labels) == 0 {
		return nil
	}
	execSQL := fmt.Sprintf(createAttributeLabel, attributeLabelFields)
	if _, err := tx.NamedExecContext(ctx, execSQL, labels); err != nil {
		log.ErrorContextf(ctx, "创建标签标准词失败 sql:%s args:%+v err:%+v", execSQL, labels, err)
		return err
	}
	return nil
}
func (d *dao) updateAttribute(ctx context.Context, tx *sqlx.Tx, attr *model.Attribute) error {
	execSQL := updateAttribute
	if _, err := tx.NamedExecContext(ctx, execSQL, attr); err != nil {
		log.ErrorContextf(ctx, "更新标签失败 sql:%s args:%+v err:%+v", execSQL, attr, err)
		return err
	}
	return nil
}

func (d *dao) deleteAttributeLabel(ctx context.Context, tx *sqlx.Tx, robotID uint64, attrID uint64, deleteLabelIDs []uint64) error {
	if len(deleteLabelIDs) == 0 {
		return nil
	}
	var args []any
	args = append(args, model.AttributeLabelDeleted, model.AttributeNextActionAdd,
		model.AttributeNextActionAdd, model.AttributeNextActionDelete, model.AttributeStatusWaitRelease,
		robotID, attrID, model.AttributeLabelIsNotDeleted)
	for _, deleteLabelID := range deleteLabelIDs {
		args = append(args, deleteLabelID)
	}
	execSQL := fmt.Sprintf(deleteAttributeLabel, placeholder(len(deleteLabelIDs)))
	if _, err := tx.ExecContext(ctx, execSQL, args...); err != nil {
		log.ErrorContextf(ctx, "删除标签标准词失败 sql:%s args:%+v err:%+v", execSQL, args, err)
		return err
	}
	return nil
}

func (d *dao) updateAttributeLabel(ctx context.Context, tx *sqlx.Tx, labels []*model.AttributeLabel) error {
	if len(labels) == 0 {
		return nil
	}
	execSQL := updateAttributeLabel
	for _, label := range labels {
		if _, err := tx.NamedExecContext(ctx, execSQL, label); err != nil {
			log.ErrorContextf(ctx, "更新标签标准词失败 sql:%s args:%+v err:%+v", execSQL, labels, err)
			return err
		}
	}
	return nil
}

func (d *dao) deleteAttribute(ctx context.Context, tx *sqlx.Tx, robotID uint64, attrIDs []uint64) error {
	if len(attrIDs) == 0 {
		return nil
	}
	var args []any
	args = append(args, model.AttributeDeleted, time.Now().UnixNano(), model.AttributeNextActionAdd,
		model.AttributeNextActionAdd, model.AttributeNextActionDelete,
		model.AttributeStatusWaitRelease, robotID, model.AttributeIsNotDeleted)
	for _, attrID := range attrIDs {
		args = append(args, attrID)
	}
	execSQL := fmt.Sprintf(deleteAttribute, placeholder(len(attrIDs)))
	if _, err := tx.ExecContext(ctx, execSQL, args...); err != nil {
		log.ErrorContextf(ctx, "删除标签标签失败 sql:%s args:%+v err:%+v", execSQL, args, err)
		return err
	}
	return nil
}

func (d *dao) deleteAttributeLabelByAttrIDs(ctx context.Context, tx *sqlx.Tx, robotID uint64, attrIDs []uint64) error {
	if len(attrIDs) == 0 {
		return nil
	}
	var args []any
	args = append(args, model.AttributeLabelDeleted, model.AttributeNextActionAdd,
		model.AttributeNextActionAdd, model.AttributeNextActionDelete, model.AttributeStatusWaitRelease,
		robotID, model.AttributeLabelIsNotDeleted)
	for _, attrID := range attrIDs {
		args = append(args, attrID)
	}
	execSQL := fmt.Sprintf(deleteAttributeLabelByAttrIDs, placeholder(len(attrIDs)))
	if _, err := tx.ExecContext(ctx, execSQL, args...); err != nil {
		log.ErrorContextf(ctx, "删除标签标准词失败 sql:%s args:%+v err:%+v", execSQL, args, err)
		return err
	}
	return nil
}

func (d *dao) createDocAttributeLabel(ctx context.Context, tx *sqlx.Tx, labels []*model.DocAttributeLabel) error {
	if len(labels) == 0 {
		return nil
	}
	execSQL := fmt.Sprintf(createDocAttributeLabel, docAttributeLabelFields)
	if _, err := tx.NamedExecContext(ctx, execSQL, labels); err != nil {
		log.ErrorContextf(ctx, "创建文档关联标签失败 sql:%s args:%+v err:%+v", execSQL, labels, err)
		return err
	}
	return nil
}

func (d *dao) deleteDocAttributeLabel(ctx context.Context, tx *sqlx.Tx, robotID uint64, docID uint64) error {
	var args []any
	args = append(args, model.DocAttributeLabelDeleted, robotID, docID, model.DocAttributeLabelIsNotDeleted)
	execSQL := deleteDocAttributeLabel
	if _, err := tx.ExecContext(ctx, execSQL, args...); err != nil {
		log.ErrorContextf(ctx, "删除文档关联标签标准词失败 sql:%s,args:%+v, err:%+v", execSQL, args, err)
		return err
	}
	return nil
}

func (d *dao) createQAAttributeLabel(ctx context.Context, tx *sqlx.Tx, labels []*model.QAAttributeLabel) error {
	if len(labels) == 0 {
		return nil
	}
	execSQL := fmt.Sprintf(createQAAttributeLabel, qaAttributeLabelFields)
	if _, err := tx.NamedExecContext(ctx, execSQL, labels); err != nil {
		log.ErrorContextf(ctx, "创建QA关联标签失败 sql:%s args:%+v err:%+v", execSQL, labels, err)
		return err
	}
	return nil
}

func (d *dao) deleteQAAttributeLabel(ctx context.Context, tx *sqlx.Tx, robotID uint64, qaID uint64) error {
	var args []any
	args = append(args, model.QAAttributeLabelDeleted, robotID, qaID, model.QAAttributeLabelIsNotDeleted)
	execSQL := deleteQAAttributeLabel
	if _, err := tx.ExecContext(ctx, execSQL, args...); err != nil {
		log.ErrorContextf(ctx, "删除QA关联标签标准词失败 sql:%s,args:%+v, err:%+v", execSQL, args, err)
		return err
	}
	return nil
}

// fillAttributeLabelListParams TODO
func fillAttributeLabelListParams(robotID uint64, attrID uint64, query string, queryScope string) ([]any, string) {
	var args []any
	var condition string
	var queryStr string
	args = append(args, robotID, attrID, model.AttributeLabelIsNotDeleted)
	if len(query) > 0 {
		queryStr = fmt.Sprintf("%%%s%%", query)
		switch queryScope {
		case attributeLabelQueryScopeStandard:
			condition += " AND (name LIKE ?)"
			args = append(args, queryStr)
		case attributeLabelQueryScopeSimilar:
			condition += " AND (similar_label LIKE ?)"
			args = append(args, queryStr)
		case attributeLabelQueryScopeAll:
			fallthrough
		default:
			condition += " AND (name LIKE ? OR similar_label LIKE ?)"
			args = append(args, queryStr, queryStr)
		}
	}
	return args, condition
}

// GetAttributeTotal 查询属性数量
func (d *dao) GetAttributeTotal(ctx context.Context, robotID uint64, query string, ids []uint64) (
	uint64, error) {
	var total uint64
	args, condition := fillAttributeListParams(robotID, query, ids)
	querySQL := fmt.Sprintf(getAttributeTotal, condition)
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute total sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetAttributeList 查询属性列表
func (d *dao) GetAttributeList(ctx context.Context, robotID uint64, query string, page, pageSize uint32,
	ids []uint64) ([]*model.Attribute, error) {
	log.DebugContextf(ctx, "GetAttributeList robotID:%d query:%s page:%d pageSize:%d ids:%+v",
		robotID, query, page, pageSize, ids)
	var attrs []*model.Attribute
	args, condition := fillAttributeListParams(robotID, query, ids)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getAttributeList, attributeFields, condition)
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute list sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "GetAttributeList attrs:%+v", attrs)
	return attrs, nil
}

// GetAttributeByIDs 通过属性IDs获取属性信息
func (d *dao) GetAttributeByIDs(ctx context.Context, robotID uint64, ids []uint64) (
	map[uint64]*model.Attribute, error) {
	attrs, err := d.GetAttributeListByIDs(ctx, robotID, ids)
	if err != nil {
		return nil, err
	}
	mapAttrID2Info := make(map[uint64]*model.Attribute)
	for _, v := range attrs {
		mapAttrID2Info[v.ID] = v
	}
	return mapAttrID2Info, nil
}

// GetAttributeListByIDs 通过属性IDs获取属性列表信息
func (d *dao) GetAttributeListByIDs(ctx context.Context, robotID uint64, ids []uint64) (
	[]*model.Attribute, error) {
	var attrs []*model.Attribute
	if len(ids) == 0 {
		return attrs, nil
	}
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	for _, idChunk := range slicex.Chunk(ids, 200) {
		var attrChunk []*model.Attribute
		var args []any
		args = append(args, robotID, model.AttributeIsNotDeleted)
		for _, id := range idChunk {
			args = append(args, id)
		}
		querySQL := fmt.Sprintf(getAttributeByIDs, attributeFields, placeholder(len(idChunk)))
		if err := db.QueryToStructs(ctx, &attrChunk, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "get attribute by ids sql:%s args:%+v err:%+v", querySQL, args, err)
			return nil, err
		}
		attrs = append(attrs, attrChunk...)
	}
	return attrs, nil
}

// GetAttributeByBizIDs 通过属性BizIDs获取属性信息
func (d *dao) GetAttributeByBizIDs(ctx context.Context, robotID uint64, ids []uint64) (
	map[uint64]*model.Attribute, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var args []any
	var attrs []*model.Attribute
	args = append(args, robotID, model.AttributeIsNotDeleted)
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getAttributeByBizIDs, attributeFields, placeholder(len(ids)))
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute by ids sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapAttrID2Info := make(map[uint64]*model.Attribute)
	for _, v := range attrs {
		mapAttrID2Info[v.BusinessID] = v
	}
	return mapAttrID2Info, nil
}

// GetAttributeByKeys 通过属性标识获取属性信息
func (d *dao) GetAttributeByKeys(ctx context.Context, robotID uint64, keys []string) (
	map[string]*model.Attribute, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	var args []any
	var attrs []*model.Attribute
	args = append(args, robotID, model.AttributeIsNotDeleted)
	for _, key := range keys {
		args = append(args, key)
	}
	querySQL := fmt.Sprintf(getAttributeByKeys, attributeFields, placeholder(len(keys)))
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute by keys sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapAttrKey2Info := make(map[string]*model.Attribute)
	for _, v := range attrs {
		mapAttrKey2Info[v.AttrKey] = v
	}
	return mapAttrKey2Info, nil
}

// GetAttributeByNames 通过属性名称获取属性信息
func (d *dao) GetAttributeByNames(ctx context.Context, robotID uint64, names []string) (
	map[string]*model.Attribute, error) {
	if len(names) == 0 {
		return nil, nil
	}
	var args []any
	var attrs []*model.Attribute
	args = append(args, robotID, model.AttributeIsNotDeleted)
	for _, name := range names {
		args = append(args, name)
	}
	querySQL := fmt.Sprintf(getAttributeByNames, attributeFields, placeholder(len(names)))
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute by names sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapAttrName2Info := make(map[string]*model.Attribute)
	for _, v := range attrs {
		mapAttrName2Info[v.Name] = v
	}
	return mapAttrName2Info, nil
}

// GetAttributeByRobotID 查询机器人下的属性信息
func (d *dao) GetAttributeByRobotID(ctx context.Context, robotID uint64) (map[string]struct{}, map[string]struct{},
	error) {
	var args []any
	var attrs []*model.Attribute
	args = append(args, robotID, model.AttributeIsNotDeleted)
	querySQL := fmt.Sprintf(getAttributeByRobotID, attributeFields)
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute by robotID sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, nil, err
	}
	mapAttrKey := make(map[string]struct{})
	mapAttrName := make(map[string]struct{})
	for _, v := range attrs {
		mapAttrKey[v.AttrKey] = struct{}{}
		mapAttrName[v.Name] = struct{}{}
	}
	return mapAttrKey, mapAttrName, nil
}

// GetAttributeKeyAndIDsByRobotID 查询机器人下的属性key和id
func (d *dao) GetAttributeKeyAndIDsByRobotID(ctx context.Context, robotID uint64) ([]*model.AttributeKeyAndID, error) {
	var args []any
	var attrs []*model.AttributeKeyAndID
	args = append(args, robotID, model.AttributeIsNotDeleted)
	querySQL := fmt.Sprintf(getAttributeKeyAndIDsByRobotID, "id,attr_key")
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetAttributeKeyAndIDsByRobotID sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return attrs, nil
}

// GetAttributeKeyAndIDsByRobotIDProd 查询发布环境机器人下的属性key和id
func (d *dao) GetAttributeKeyAndIDsByRobotIDProd(ctx context.Context, robotID uint64) ([]*model.AttributeKeyAndID,
	error) {
	var args []any
	var attrs []*model.AttributeKeyAndID
	args = append(args, robotID, model.AttributeIsNotDeleted)
	querySQL := fmt.Sprintf(getAttributeKeyAndIDsByRobotIDProd, "id,attr_id,attr_key")
	db := knowClient.DBClient(ctx, attributeProdTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetAttributeKeyAndIDsByRobotIDProd sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return attrs, nil
}

// BatchCreateAttribute 批量创建属性
func (d *dao) BatchCreateAttribute(ctx context.Context, attrLabels []*model.AttributeLabelItem) error {
	if len(attrLabels) == 0 {
		return nil
	}
	robotID := attrLabels[0].Attr.RobotID
	var attributeList []*model.Attribute
	var attributeLabelList []*model.AttributeLabel
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, v := range attrLabels {
			attributeList = append(attributeList, v.Attr)
			attributeLabelList = append(attributeLabelList, v.Labels...)
			attrID, err := d.createAttribute(ctx, tx, v.Attr)
			if err != nil {
				return err
			}
			redisAttrItem := &model.AttributeLabelItem{
				Attr: &model.Attribute{
					RobotID: v.Attr.RobotID,
					AttrKey: v.Attr.AttrKey,
				},
				Labels: make([]*model.AttributeLabel, 0),
			}
			for _, label := range v.Labels {
				label.BusinessID = d.GenerateSeqID()
				label.AttrID = attrID
				if label.SimilarLabel != "" {
					redisAttrItem.Labels = append(redisAttrItem.Labels, label)
				}
			}
			if err := d.createAttributeLabel(ctx, tx, v.Labels); err != nil {
				return err
			}
			if len(redisAttrItem.Labels) > 0 {
				// 包含相似标签的标签才需要同步到redis，以便在对话时做相似标签替换
				if err := d.createAttributeLabelsRedis(ctx, redisAttrItem); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "批量创建标签失败 err:%+v", err)
		return err
	}

	// 往es写入所有attr bulk超过1000之后会有性能问题，导入属性目前上限为100
	err = BatchAddAndUpdateAttributes(ctx, robotID, attributeList)
	if err != nil {
		return err
	}

	log.InfoContextf(ctx, "批量创建标签数量,count:%+v", len(attributeLabelList))
	// 填充插入后的标签ID和attr id
	labelsWithID, err := d.fillAttributeLabelIDsAfterInsert(ctx, robotID, attributeLabelList)
	if err != nil {
		return err
	}
	log.InfoContextf(ctx, "批量创建标签标准值,count:%+v", len(labelsWithID))
	for _, labelChunk := range slicex.Chunk(labelsWithID, 100) {
		err = BatchAddAndUpdateAttributeLabels(ctx, robotID, 0, labelChunk)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateAttribute 更新属性
func (d *dao) UpdateAttribute(ctx context.Context, req *model.UpdateAttributeLabelReq, oldAttr *model.Attribute,
	corpID, staffID uint64, needUpdateCacheFlag bool, newLabelRedisValue []model.AttributeLabelRedisValue) (uint64, error) {
	taskID, err := d.createUpdateAttributeTask(ctx, req, corpID, staffID, oldAttr.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "更新标签,创建任务失败 err:%+v", err)
		return 0, err
	}
	robotID := oldAttr.RobotID
	db := knowClient.DBClient(ctx, attributeTableName, oldAttr.RobotID, []client.Option{}...)
	err = db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err = d.updateAttribute(ctx, tx, req.Attr); err != nil {
			return err
		}
		if err = d.deleteAttributeLabel(ctx, tx, oldAttr.RobotID, req.Attr.ID, req.DeleteLabelIDs); err != nil {
			return err
		}
		if err = d.createAttributeLabel(ctx, tx, req.AddLabels); err != nil {
			return err
		}
		if err = d.updateAttributeLabel(ctx, tx, req.UpdateLabels); err != nil {
			return err
		}
		if needUpdateCacheFlag {
			if len(newLabelRedisValue) == 0 {
				if err = d.PiplineDelAttributeLabelRedis(ctx, oldAttr.RobotID, []string{oldAttr.AttrKey},
					model.AttributeLabelsPreview); err != nil {
					return err
				}
			} else {
				if err = d.SetAttributeLabelsRedis(ctx, oldAttr.RobotID, oldAttr.AttrKey, newLabelRedisValue,
					model.AttributeLabelsPreview); err != nil {
					// 可能出现删除redis旧数据成功，添加新数据失败，没法回滚，要通过定时任务刷新缓存
					return err
				}
			}
		}
		if !req.IsNeedPublish { // 不需要同步的情况下,PublishParams为空使用传入的参数
			taskInfo := model.AttributeLabelTask{
				ID:            taskID,
				CorpID:        corpID,
				CreateStaffID: staffID,
				RobotID:       oldAttr.RobotID,
				Status:        model.AttributeLabelTaskStatusSuccess,
			}
			err = d.UpdateAttributeTask(ctx, &taskInfo)
			return err
		}
		req.PublishParams.TaskID = taskID
		if err = d.sendAttributeLabelUpdateNotice(ctx, tx, req.PublishParams.CorpID, req.PublishParams.StaffID,
			req.Attr, model.AttributeLabelUpdatingNoticeContent, model.LevelInfo); err != nil {
			return err
		}
		if err = newAttributeLabelUpdateTask(ctx, req.Attr.RobotID, req.PublishParams); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "更新标签失败 err:%+v", err)
		taskInfo := model.AttributeLabelTask{
			ID:            taskID,
			CorpID:        corpID,
			CreateStaffID: staffID,
			RobotID:       oldAttr.RobotID,
			Status:        model.AttributeLabelTaskStatusFailed,
		}
		err = d.UpdateAttributeTask(ctx, &taskInfo)
		return 0, err
	}

	err = AddAndUpdateAttribute(ctx, robotID, req.Attr)
	if err != nil {
		return 0, err
	}

	addAndUpdateLabels, err := d.fillAttributeLabelIDsAfterInsert(ctx, req.Attr.RobotID, req.AddLabels)
	if err != nil {
		return 0, err
	}
	err = BatchAddAndUpdateAttributeLabels(ctx, req.Attr.RobotID, req.Attr.ID, addAndUpdateLabels)
	if err != nil {
		return 0, err
	}
	err = BatchDeleteAttributeLabelsByIDs(ctx, req.Attr.RobotID, req.DeleteLabelIDs)
	if err != nil {
		return 0, err
	}

	return taskID, nil
}

func (d *dao) fillAttributeLabelIDsAfterInsert(ctx context.Context, robotID uint64,
	addLabels []*model.AttributeLabel) ([]*model.AttributeLabel, error) {
	if len(addLabels) == 0 {
		return addLabels, nil
	}
	// 插入的label没有id，需要从数据库中反查出来
	addLabelBizIDs := make([]uint64, 0)
	for _, label := range addLabels {
		addLabelBizIDs = append(addLabelBizIDs, label.BusinessID)
	}
	addLabelMap, err := d.GetAttributeLabelByBizIDs(ctx, addLabelBizIDs, robotID)
	if err != nil {
		return addLabels, err
	}

	var addLabelsWithID []*model.AttributeLabel
	for _, label := range addLabelMap {
		addLabelsWithID = append(addLabelsWithID, label)
	}
	return addLabelsWithID, nil
}

// DeleteAttribute 删除属性
func (d *dao) DeleteAttribute(ctx context.Context, robotID uint64, ids []uint64, attrKeys []string) error {
	err := BatchDeleteAttributes(ctx, robotID, ids)
	if err != nil {
		return err
	}
	err = BatchDeleteAttributeLabelByAttrIDs(ctx, robotID, ids)
	if err != nil {
		return err
	}
	db := knowClient.DBClient(ctx, attributeTableName, robotID, []client.Option{}...)
	err = db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := d.deleteAttribute(ctx, tx, robotID, ids); err != nil {
			return err
		}
		if err := d.deleteAttributeLabelByAttrIDs(ctx, tx, robotID, ids); err != nil {
			return err
		}
		if err := d.PiplineDelAttributeLabelRedis(ctx, robotID, attrKeys, model.AttributeLabelsPreview); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "删除标签失败 err:%+v", err)
		return err
	}
	return nil
}

// GetAttributeLabelCount 获取属性标签数量
func (d *dao) GetAttributeLabelCount(ctx context.Context, attrID uint64, query string, queryScope string,
	robotID uint64) (uint64, error) {
	var total uint64
	args, condition := fillAttributeLabelListParams(robotID, attrID, query, queryScope)
	querySQL := fmt.Sprintf(getAttributeLabelCount, condition)
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute label count sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetAttributeLabelList 获取属性标签列表
func (d *dao) GetAttributeLabelList(ctx context.Context, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]*model.AttributeLabel, error) {
	args, condition := fillAttributeLabelListParams(robotID, attrID, query, queryScope)
	if lastLabelID > 0 {
		condition += " AND id < ?"
		args = append(args, lastLabelID)
	}
	args = append(args, limit)
	querySQL := fmt.Sprintf(getAttributeLabelIdList, condition)
	ids := make([]uint64, 0)
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	if err := db.Select(ctx, &ids, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "get attribute label list sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}

	labels, err := d.GetAttributeLabelByIDOrder(ctx, robotID, ids)
	if err != nil {
		return nil, err
	}
	return labels, nil
}

// GetAttributeLabelByIDOrder 根据输入的id的顺序查询AttributeLabel
func (d *dao) GetAttributeLabelByIDOrder(ctx context.Context, robotID uint64, ids []uint64) ([]*model.AttributeLabel, error) {
	var labels []*model.AttributeLabel
	if len(ids) == 0 {
		return labels, nil
	}
	var newArgs []any
	newArgs = append(newArgs, robotID)
	// 需要拼接两次，一次作为IN查询参数，一次作为排序参数
	for _, id := range ids {
		newArgs = append(newArgs, id)
	}
	for _, id := range ids {
		newArgs = append(newArgs, id)
	}
	newQuerySQL := fmt.Sprintf(getAttributeLabelList, attributeLabelFields, placeholder(len(ids)), placeholder(len(ids)))
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &labels, newQuerySQL, newArgs...); err != nil {
		log.ErrorContextf(ctx, "get attribute label list sql:%s args:%+v err:%+v", newQuerySQL, newArgs, err)
		return nil, err
	}

	return labels, nil
}

// GetAttributeLabelByAttrIDs 获取指定属性下的标签信息
func (d *dao) GetAttributeLabelByAttrIDs(ctx context.Context, attrIDs []uint64, robotID uint64) (
	map[uint64][]*model.AttributeLabel, error) {
	log.DebugContextf(ctx, "GetAttributeLabelByAttrIDs attrIDs:%+v", attrIDs)
	if len(attrIDs) == 0 {
		return nil, nil
	}
	var args []any
	var labels []*model.AttributeLabel
	args = append(args, robotID, model.AttributeLabelIsNotDeleted)
	for _, attrID := range attrIDs {
		args = append(args, attrID)
	}
	querySQL := fmt.Sprintf(getAttributeLabelByAttrIDs, attributeLabelFields, placeholder(len(attrIDs)))
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &labels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过标签IDs查询标签标准词失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapAttrID2Labels := make(map[uint64][]*model.AttributeLabel)
	for _, v := range labels {
		mapAttrID2Labels[v.AttrID] = append(mapAttrID2Labels[v.AttrID], v)
	}
	log.DebugContextf(ctx, "GetAttributeLabelByAttrIDs mapAttrID2Labels:%+v", mapAttrID2Labels)
	return mapAttrID2Labels, nil
}

// GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd 获取发布环境指定属性下相似标签不为空的标签信息
func (d *dao) GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx context.Context, attrIDs []uint64, robotID uint64) (
	map[uint64][]*model.AttributeLabel, error) {
	if len(attrIDs) == 0 {
		return nil, nil
	}
	var args []any
	args = append(args, robotID)
	var labels []*model.AttributeLabel
	for _, attrID := range attrIDs {
		args = append(args, attrID)
	}
	args = append(args, model.AttributeLabelIsNotDeleted)
	querySQL := fmt.Sprintf(getAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd, attributeLabelFieldsProd,
		placeholder(len(attrIDs)))
	db := knowClient.DBClient(ctx, attributeLabelProdTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &labels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过标签IDs查询标签标准词失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapAttrID2Labels := make(map[uint64][]*model.AttributeLabel)
	for _, v := range labels {
		mapAttrID2Labels[v.AttrID] = append(mapAttrID2Labels[v.AttrID], v)
	}
	return mapAttrID2Labels, nil
}

// GetAttributeLabelByIDs 获取指定标签ID的信息
func (d *dao) GetAttributeLabelByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.AttributeLabel,
	error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var args []any
	args = append(args, robotID, model.AttributeLabelIsNotDeleted)
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getAttributeLabelByIDs, attributeLabelFields, placeholder(len(ids)))
	var labels []*model.AttributeLabel
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &labels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过标准词ID查询标准词失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	mapLabelID2Info := make(map[uint64]*model.AttributeLabel)
	for _, v := range labels {
		mapLabelID2Info[v.ID] = v
	}
	return mapLabelID2Info, nil
}

// GetAttributeLabelByBizIDs 获取指定标签ID的信息
func (d *dao) GetAttributeLabelByBizIDs(ctx context.Context, ids []uint64, robotID uint64) (
	map[uint64]*model.AttributeLabel, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	mapLabelID2Info := make(map[uint64]*model.AttributeLabel)
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	for _, chunk := range slicex.Chunk(ids, MaxSqlInCount) { //size后续改成配置文件
		var args []any
		args = append(args, robotID, model.AttributeLabelIsNotDeleted)
		for _, id := range chunk {
			args = append(args, id)
		}
		querySQL := fmt.Sprintf(getAttributeLabelByBizIDs, attributeLabelFields, placeholder(len(chunk)))
		var labels []*model.AttributeLabel
		if err := db.QueryToStructs(ctx, &labels, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "通过标准词ID查询标准词失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return nil, err
		}
		for _, v := range labels {
			mapLabelID2Info[v.BusinessID] = v
		}
	}
	return mapLabelID2Info, nil
}

// GetAttributeLabelByName 检索标签名或相似标签名
func (d *dao) GetAttributeLabelByName(ctx context.Context, attrID uint64, name string, robotID uint64) (
	[]*model.AttributeLabel, error) {
	var args []any
	labelNameStr := fmt.Sprintf("%%%s%%", name)
	args = append(args, robotID, model.AttributeLabelIsNotDeleted, attrID, labelNameStr, labelNameStr)
	querySQL := fmt.Sprintf(getAttributeLabelByName, attributeLabelFields)
	var labels []*model.AttributeLabel
	db := knowClient.DBClient(ctx, attributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &labels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过标准词名称查询标准词失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return labels, nil
}

// GetDocAttributeLabel 获取文档的属性标签信息
func (d *dao) GetDocAttributeLabel(ctx context.Context, robotID uint64, docIDs []uint64) ([]*model.DocAttributeLabel,
	error) {
	if len(docIDs) == 0 {
		return nil, nil
	}
	var args []any
	var attributeLabels []*model.DocAttributeLabel
	args = append(args, robotID, model.DocAttributeLabelIsNotDeleted)
	for _, docID := range docIDs {
		args = append(args, docID)
	}
	querySQL := fmt.Sprintf(getDocAttributeLabel, docAttributeLabelFields, placeholder(len(docIDs)))
	db := knowClient.DBClient(ctx, docAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attributeLabels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询文档标签标准词失败 sql:%s,args:%+v, err:%+v", querySQL, args, err)
		return nil, err
	}
	return attributeLabels, nil
}

// GetDocAttributeLabelCountByAttrLabelIDs 通过属性和标签ID获取文档属性标签数量
func (d *dao) GetDocAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64) (uint64, error) {
	var total uint64
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return 0, nil
	}
	args, condition := fillDocAttributeLabelParams(robotID, source, attrIDs, labelIDs)
	querySQL := fmt.Sprintf(getDocAttributeLabelCountByAttrLabelIDs, condition)
	db := knowClient.DBClient(ctx, docAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过标签和标签ID查询文档标签标准词数量失败 sql:%s,args:%+v, err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetDocAttributeLabelByAttrLabelIDs 通过属性和标签ID获取文档属性标签
func (d *dao) GetDocAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64, page, pageSize uint32) ([]*model.DocAttributeLabel, error) {
	var attributeLabels []*model.DocAttributeLabel
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return attributeLabels, nil
	}
	args, condition := fillDocAttributeLabelParams(robotID, source, attrIDs, labelIDs)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getDocAttributeLabelByAttrLabelIDs, docAttributeLabelFields, condition)
	db := knowClient.DBClient(ctx, docAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attributeLabels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过属性和标签ID查询文档标签标准词失败 sql:%s args:%+v, err:%+v", querySQL, args, err)
		return nil, err
	}
	return attributeLabels, nil
}

// fillDocAttributeLabelParams TODO
func fillDocAttributeLabelParams(robotID uint64, source uint32, attrIDs, labelIDs []uint64) ([]any, string) {
	var args []any
	var condition string
	args = append(args, robotID, model.DocAttributeLabelIsNotDeleted, source)
	if len(attrIDs) > 0 {
		condition += fmt.Sprintf(" AND attr_id IN (%s)", placeholder(len(attrIDs)))
		for _, attrID := range attrIDs {
			args = append(args, attrID)
		}
	}
	if len(labelIDs) > 0 {
		condition += fmt.Sprintf(" AND label_id IN (%s)", placeholder(len(labelIDs)))
		for _, labelID := range labelIDs {
			args = append(args, labelID)
		}
	}
	return args, condition
}

// updateDocAttributeLabel 更新文档的属性标签
func (d *dao) updateDocAttributeLabel(ctx context.Context, tx *sqlx.Tx, robotID, docID uint64,
	attributeLabelReq *model.UpdateDocAttributeLabelReq) error {
	if !attributeLabelReq.IsNeedChange {
		return nil
	}
	for _, v := range attributeLabelReq.AttributeLabels {
		v.RobotID, v.DocID = robotID, docID
	}
	if err := d.deleteDocAttributeLabel(ctx, tx, robotID, docID); err != nil {
		return err
	}
	if err := d.createDocAttributeLabel(ctx, tx, attributeLabelReq.AttributeLabels); err != nil {
		return err
	}
	return nil
}

// GetDocCountByAttributeLabel 通过关联的属性标签获取文档数量
func (d *dao) GetDocCountByAttributeLabel(ctx context.Context, robotID uint64, noStatusList []uint32, attrID uint64,
	labelIDs []uint64) (uint64, error) {
	var args []any
	var condition string
	args = append(args, robotID, model.DocIsNotDeleted)
	if len(noStatusList) > 0 {
		condition += fmt.Sprintf(" AND (status NOT IN (%s) OR is_creating_qa = ?)", placeholder(len(noStatusList)))
		for _, noStatus := range noStatusList {
			args = append(args, noStatus)
		}
		args = append(args, model.DocCreatingQA)
	}
	subArgs, subCondition := fillAttributeLabelDocIDsSQL(robotID, model.AttributeLabelSourceKg, attrID, labelIDs)
	args = append(args, subArgs...)
	condition += fmt.Sprintf(" AND id IN (%s)", subCondition)
	querySQL := fmt.Sprintf(getDocCountByAttributeLabel, condition)
	var total uint64
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取关联标签标准词的文档总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	if total > 0 {
		log.InfoContextf(ctx, "获取关联标签标准词的文档总数成功 sql:%s args:%+v total:%d", querySQL, args, total)
	}
	return total, nil
}

// fillAttributeLabelDocIDsSQL TODO
func fillAttributeLabelDocIDsSQL(robotID uint64, source uint32, attrID uint64, labelIDs []uint64) ([]any, string) {
	var args []any
	var condition string
	args = append(args, robotID, model.DocAttributeLabelIsNotDeleted, source)
	if attrID > 0 {
		condition += " AND attr_id = ?"
		args = append(args, attrID)
	}
	if len(labelIDs) > 0 {
		condition += fmt.Sprintf(" AND label_id IN (%s)", placeholder(len(labelIDs)))
		for _, labelID := range labelIDs {
			args = append(args, labelID)
		}
	}
	querySQL := fmt.Sprintf(getAttributeLabelDocIDs, condition)
	return args, querySQL
}

// GetDocAttributeLabelDetail 获取文档的属性标签详情
func (d *dao) GetDocAttributeLabelDetail(ctx context.Context, robotID uint64, docIDs []uint64) (
	map[uint64][]*model.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*model.AttrLabel)
	if len(docIDs) == 0 {
		return mapDocID2AttrLabels, nil
	}
	attributeLabels, err := d.GetDocAttributeLabel(ctx, robotID, docIDs)
	if err != nil {
		return nil, err
	}
	if len(attributeLabels) == 0 {
		return mapDocID2AttrLabels, nil
	}
	var mapKgAttrLabels map[uint64][]*model.AttrLabel
	var kgErr error
	// 查询不同来源的属性标签信息
	g := errgroupx.Group{}
	g.SetLimit(10)
	// 来源，属性标签
	g.Go(func() error {
		mapKgAttrLabels, kgErr = d.getDocAttributeLabelOfKg(ctx, robotID, attributeLabels)
		return kgErr
	})
	if err := g.Wait(); err != nil {
		log.WarnContextf(ctx, "GetDocAttributeLabelDetail robotID:%d,docIDs:%+v err :%v", robotID, docIDs, err)
		return nil, err
	}
	for docID, attrLabels := range mapKgAttrLabels {
		mapDocID2AttrLabels[docID] = append(mapDocID2AttrLabels[docID], attrLabels...)
	}
	return mapDocID2AttrLabels, nil
}

// getDocAttributeLabelOfKg 获取获取来源为知识标签的文档属性标签信息
func (d *dao) getDocAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*model.DocAttributeLabel) (map[uint64][]*model.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*model.AttrLabel)
	mapDocAttrID2Attr := make(map[string]*model.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getDocAttributeLabelOfSource(attributeLabels,
		model.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := d.GetAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := d.GetAttributeLabelByIDs(ctx, labelIDs, robotID)
	if err != nil {
		return nil, err
	}
	for _, v := range sourceAttributeLabels {
		attr, ok := mapAttrID2Info[v.AttrID]
		if !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		label, ok := mapLabelID2Info[v.LabelID]
		if v.LabelID > 0 && !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		labelName := label.GetName()
		if v.LabelID == 0 {
			labelName = config.App().AttributeLabel.FullLabelValue
		}
		labelInfo := &model.Label{
			LabelID:    v.LabelID,
			BusinessID: label.GetBusinessID(),
			LabelName:  labelName,
		}
		docAttrID := fmt.Sprintf("%d_%d", v.DocID, v.AttrID)
		attrInfo, ok := mapDocAttrID2Attr[docAttrID]
		if !ok {
			attrInfo = &model.AttrLabel{
				Source:     v.Source,
				AttrID:     v.AttrID,
				BusinessID: attr.BusinessID,
				AttrKey:    attr.AttrKey,
				AttrName:   attr.Name,
			}
			mapDocAttrID2Attr[docAttrID] = attrInfo
			mapDocID2AttrLabels[v.DocID] = append(mapDocID2AttrLabels[v.DocID], attrInfo)
		}
		attrInfo.Labels = append(attrInfo.Labels, labelInfo)
	}
	return mapDocID2AttrLabels, nil
}

// getDocAttributeLabelOfSource TODO
func getDocAttributeLabelOfSource(attributeLabels []*model.DocAttributeLabel, source uint32) (
	[]*model.DocAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*model.DocAttributeLabel
	var attrIDs, labelIDs []uint64
	mapAttrID := make(map[uint64]struct{}, 0)
	mapLabelID := make(map[uint64]struct{}, 0)
	for _, v := range attributeLabels {
		if v.Source != source {
			continue
		}
		sourceAttributeLabels = append(sourceAttributeLabels, v)
		if _, ok := mapAttrID[v.AttrID]; !ok {
			mapAttrID[v.AttrID] = struct{}{}
			attrIDs = append(attrIDs, v.AttrID)
		}
		if v.LabelID == 0 {
			continue
		}
		if _, ok := mapLabelID[v.LabelID]; !ok {
			mapLabelID[v.LabelID] = struct{}{}
			labelIDs = append(labelIDs, v.LabelID)
		}
	}
	return sourceAttributeLabels, attrIDs, labelIDs
}

// GetQAAttributeLabel 获取QA的属性标签信息
func (d *dao) GetQAAttributeLabel(ctx context.Context, robotID uint64, qaIDs []uint64) ([]*model.QAAttributeLabel,
	error) {
	if len(qaIDs) == 0 {
		return nil, nil
	}
	var args []any
	var attributeLabels []*model.QAAttributeLabel
	args = append(args, robotID, model.QAAttributeLabelIsNotDeleted)
	for _, qaID := range qaIDs {
		args = append(args, qaID)
	}
	querySQL := fmt.Sprintf(getQAAttributeLabel, qaAttributeLabelFields, placeholder(len(qaIDs)))
	db := knowClient.DBClient(ctx, qaAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attributeLabels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询QA标签标准词失败 sql:%s,args:%+v, err:%+v", querySQL, args, err)
		return nil, err
	}
	return attributeLabels, nil
}

// GetQAAttributeLabelCountByAttrLabelIDs 通过属性和标签ID获取QA属性标签数量
func (d *dao) GetQAAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64) (uint64, error) {
	var total uint64
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return 0, nil
	}
	args, condition := fillQAAttributeLabelParams(robotID, source, attrIDs, labelIDs)
	querySQL := fmt.Sprintf(getQAAttributeLabelCountByAttrLabelIDs, condition)
	db := knowClient.DBClient(ctx, qaAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过属性和标签ID查询QA属性标签数量失败 sql:%s,args:%+v, err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetQAAttributeLabelByAttrLabelIDs 通过属性和标签ID获取QA属性标签
func (d *dao) GetQAAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
	labelIDs []uint64, page, pageSize uint32) ([]*model.QAAttributeLabel, error) {
	var attributeLabels []*model.QAAttributeLabel
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return attributeLabels, nil
	}
	args, condition := fillQAAttributeLabelParams(robotID, source, attrIDs, labelIDs)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getQAAttributeLabelByAttrLabelIDs, qaAttributeLabelFields, condition)
	db := knowClient.DBClient(ctx, qaAttributeLabelTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attributeLabels, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过属性和标签ID查询文档属性标签失败 sql:%s args:%+v, err:%+v", querySQL, args, err)
		return nil, err
	}
	return attributeLabels, nil
}

// fillQAAttributeLabelParams TODO
func fillQAAttributeLabelParams(robotID uint64, source uint32, attrIDs, labelIDs []uint64) ([]any, string) {
	var args []any
	var condition string
	args = append(args, robotID, model.QAAttributeLabelIsNotDeleted, source)
	if len(attrIDs) > 0 {
		condition += fmt.Sprintf(" AND attr_id IN (%s)", placeholder(len(attrIDs)))
		for _, attrID := range attrIDs {
			args = append(args, attrID)
		}
	}
	if len(labelIDs) > 0 {
		condition += fmt.Sprintf(" AND label_id IN (%s)", placeholder(len(labelIDs)))
		for _, labelID := range labelIDs {
			args = append(args, labelID)
		}
	}
	return args, condition
}

// updateQAAttributeLabel 更新QA的属性标签
func (d *dao) updateQAAttributeLabel(ctx context.Context, tx *sqlx.Tx, robotID, qaID uint64,
	attributeLabelReq *model.UpdateQAAttributeLabelReq) error {
	if !attributeLabelReq.IsNeedChange {
		return nil
	}
	for _, v := range attributeLabelReq.AttributeLabels {
		v.RobotID, v.QAID = robotID, qaID
	}
	if err := d.deleteQAAttributeLabel(ctx, tx, robotID, qaID); err != nil {
		return err
	}
	if err := d.createQAAttributeLabel(ctx, tx, attributeLabelReq.AttributeLabels); err != nil {
		return err
	}
	return nil
}

// GetQACountByAttributeLabel 通过关联的属性标签获取QA数量
func (d *dao) GetQACountByAttributeLabel(ctx context.Context, robotID uint64, noReleaseStatusList []uint32,
	attrID uint64, labelIDs []uint64) (uint64, error) {
	var args []any
	var condition string
	args = append(args, robotID, model.QAIsNotDeleted)
	if len(noReleaseStatusList) > 0 {
		condition += fmt.Sprintf(" AND release_status NOT IN (%s)", placeholder(len(noReleaseStatusList)))
		for _, noReleaseStatus := range noReleaseStatusList {
			args = append(args, noReleaseStatus)
		}
	}
	subArgs, subCondition := fillAttributeLabelQAIDsSQL(robotID, model.AttributeLabelSourceKg, attrID, labelIDs)
	args = append(args, subArgs...)
	condition += fmt.Sprintf(" AND id IN (%s)", subCondition)
	querySQL := fmt.Sprintf(getQACountByAttributeLabel, condition)
	var total uint64
	db := knowClient.DBClient(ctx, docQaTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取关联属性标签的QA总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	if total > 0 {
		log.InfoContextf(ctx, "获取关联属性标签的QA总数成功 sql:%s args:%+v total:%d", querySQL, args, total)
	}
	return total, nil
}

// fillAttributeLabelQAIDsSQL TODO
func fillAttributeLabelQAIDsSQL(robotID uint64, source uint32, attrID uint64, labelIDs []uint64) ([]any, string) {
	var args []any
	var condition string
	args = append(args, robotID, model.QAAttributeLabelIsNotDeleted, source)
	if attrID > 0 {
		condition += " AND attr_id = ?"
		args = append(args, attrID)
	}
	if len(labelIDs) > 0 {
		condition += fmt.Sprintf(" AND label_id IN (%s)", placeholder(len(labelIDs)))
		for _, labelID := range labelIDs {
			args = append(args, labelID)
		}
	}
	querySQL := fmt.Sprintf(getAttributeLabelQAIDs, condition)
	return args, querySQL
}

// GetQAAttributeLabelDetail 获取QA的属性标签详情
func (d *dao) GetQAAttributeLabelDetail(ctx context.Context, robotID uint64, qaIDs []uint64) (
	map[uint64][]*model.AttrLabel, error) {
	mapQAID2AttrLabels := make(map[uint64][]*model.AttrLabel)
	if len(qaIDs) == 0 {
		return mapQAID2AttrLabels, nil
	}
	attributeLabels, err := d.GetQAAttributeLabel(ctx, robotID, qaIDs)
	if err != nil {
		return nil, err
	}
	if len(attributeLabels) == 0 {
		return mapQAID2AttrLabels, nil
	}
	var mapKgAttrLabels map[uint64][]*model.AttrLabel
	// 查询不同来源的属性标签信息
	mapKgAttrLabels, err = d.getQAAttributeLabelOfKg(ctx, robotID, attributeLabels)
	if err != nil {
		log.WarnContextf(ctx, "GetQAAttributeLabelDetail robotID:%d,qaIDs:%+v err :%v", robotID, qaIDs, err)
		return nil, err
	}
	for qaID, attrLabels := range mapKgAttrLabels {
		mapQAID2AttrLabels[qaID] = append(mapQAID2AttrLabels[qaID], attrLabels...)
	}
	return mapQAID2AttrLabels, nil
}

// getQAAttributeLabelOfKg 获取获取来源为知识标签的QA属性标签信息
func (d *dao) getQAAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*model.QAAttributeLabel) (map[uint64][]*model.AttrLabel, error) {
	mapQAID2AttrLabels := make(map[uint64][]*model.AttrLabel)
	mapQAAttrID2Attr := make(map[string]*model.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getQAAttributeLabelOfSource(attributeLabels,
		model.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := d.GetAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := d.GetAttributeLabelByIDs(ctx, labelIDs, robotID)
	if err != nil {
		return nil, err
	}
	for _, v := range sourceAttributeLabels {
		attr, ok := mapAttrID2Info[v.AttrID]
		if !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		label, ok := mapLabelID2Info[v.LabelID]
		if v.LabelID > 0 && !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		labelName := label.GetName()
		if v.LabelID == 0 {
			labelName = config.App().AttributeLabel.FullLabelValue
		}
		labelInfo := &model.Label{
			LabelID:    v.LabelID,
			BusinessID: label.GetBusinessID(),
			LabelName:  labelName,
		}
		qaAttrID := fmt.Sprintf("%d_%d", v.QAID, v.AttrID)
		attrInfo, ok := mapQAAttrID2Attr[qaAttrID]
		if !ok {
			attrInfo = &model.AttrLabel{
				Source:     v.Source,
				AttrID:     v.AttrID,
				BusinessID: attr.BusinessID,
				AttrKey:    attr.AttrKey,
				AttrName:   attr.Name,
			}
			mapQAAttrID2Attr[qaAttrID] = attrInfo
			mapQAID2AttrLabels[v.QAID] = append(mapQAID2AttrLabels[v.QAID], attrInfo)
		}
		attrInfo.Labels = append(attrInfo.Labels, labelInfo)
	}
	return mapQAID2AttrLabels, nil
}

// getQAAttributeLabelOfSource TODO
func getQAAttributeLabelOfSource(attributeLabels []*model.QAAttributeLabel, source uint32) (
	[]*model.QAAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*model.QAAttributeLabel
	var attrIDs, labelIDs []uint64
	mapAttrID := make(map[uint64]struct{}, 0)
	mapLabelID := make(map[uint64]struct{}, 0)
	for _, v := range attributeLabels {
		if v.Source != source {
			continue
		}
		sourceAttributeLabels = append(sourceAttributeLabels, v)
		if _, ok := mapAttrID[v.AttrID]; !ok {
			mapAttrID[v.AttrID] = struct{}{}
			attrIDs = append(attrIDs, v.AttrID)
		}
		if v.LabelID == 0 {
			continue
		}
		if _, ok := mapLabelID[v.LabelID]; !ok {
			mapLabelID[v.LabelID] = struct{}{}
			labelIDs = append(labelIDs, v.LabelID)
		}
	}
	return sourceAttributeLabels, attrIDs, labelIDs
}

func (d *dao) sendAttributeLabelUpdateNotice(ctx context.Context, tx *sqlx.Tx, corpID, staffID uint64,
	attr *model.Attribute, content, level string) error {
	operations := make([]model.Operation, 0)
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeAttributeLabelPageID),
		model.WithLevel(level),
		model.WithContent(i18n.Translate(ctx, content, attr.Name)),
	}
	switch level {
	case model.LevelSuccess:
		noticeOptions = append(noticeOptions, model.WithSubject(i18n.Translate(ctx, i18nkey.KeyKnowledgeTagUpdateSuccess)))
		noticeOptions = append(noticeOptions, model.WithGlobalFlag())
	case model.LevelError:
		noticeOptions = append(noticeOptions, model.WithSubject(i18n.Translate(ctx, i18nkey.KeyKnowledgeTagUpdateFailure)))
		noticeOptions = append(noticeOptions, model.WithGlobalFlag())
	case model.LevelInfo:
		noticeOptions = append(noticeOptions, model.WithForbidCloseFlag())
	}
	notice := model.NewNotice(model.NoticeTypeAttributeLabelUpdate, attr.ID, corpID, attr.RobotID,
		staffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// UpdateAttributeSuccess 属性标签更新成功
func (d *dao) UpdateAttributeSuccess(ctx context.Context, attr *model.Attribute, corpID, staffID uint64) error {
	now := time.Now()
	db := knowClient.DBClient(ctx, attributeTableName, attr.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := attributeModifyUpdating
		attr.IsUpdating = false
		attr.ReleaseStatus = model.AttributeStatusWaitRelease
		attr.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, attr); err != nil {
			log.ErrorContextf(ctx, "更新属性标签success失败 sql:%s args:%+v err:%+v", querySQL, attr, err)
			return err
		}
		if err := d.sendAttributeLabelUpdateNotice(ctx, tx, corpID, staffID, attr,
			model.AttributeLabelUpdateSuccessNoticeContent, model.LevelSuccess); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新属性标签任务success失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateAttributeFail 属性标签更新失败
func (d *dao) UpdateAttributeFail(ctx context.Context, attr *model.Attribute, corpID, staffID uint64) error {
	now := time.Now()
	db := knowClient.DBClient(ctx, attributeTableName, attr.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := attributeModifyUpdating
		attr.IsUpdating = false
		attr.ReleaseStatus = model.AttributeStatusReleaseFail
		attr.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, attr); err != nil {
			log.ErrorContextf(ctx, "更新属性标签fail失败 sql:%s args:%+v err:%+v", querySQL, attr, err)
			return err
		}
		if err := d.sendAttributeLabelUpdateNotice(ctx, tx, corpID, staffID, attr,
			model.AttributeLabelUpdateFailNoticeContent, model.LevelError); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新属性标签任务fail失败 err:%+v", err)
		return err
	}
	return nil
}

// getLabelRelationsFromLabelAble 根据可带标签的数据批量获取对应标签(可带标签的数据有: Doc, DocQA)
// 批量获取的标签必须属于同一个应用, 即必须满足 labelAbles[n].RobotID == appID
func (d *dao) getLabelRelationsFromLabelAble(
	ctx context.Context, appID uint64, labelAbles []model.LabelAble,
) ([][]model.LabelRelationer, error) {
	// 获取需要查询的数据ID
	qaIDs, docIDs, qaDocIDs, err := d.getRelatedIDsFromLabelAble(ctx, appID, labelAbles)
	if err != nil {
		return nil, err
	}
	// 获取所有的 qa / doc 标签
	qaLabelMap, docLabelMap, err := d.getQaAndDocAttributeLabelMap(
		ctx, appID, slicex.Unique(qaIDs), slicex.Unique(docIDs),
	)
	if err != nil {
		return nil, err
	}
	// 获取继承自文档标签的问答的关联文档数据
	qaDocs, err := d.GetDocByIDs(ctx, qaDocIDs, appID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocByIDs fail, qaDocIDs: %+v, err: %v", qaDocIDs, err)
		return nil, err
	}
	// 构造返回结构
	relations, err := d.buildRelationFromLabelAble(ctx, labelAbles, qaLabelMap, docLabelMap, qaDocs)
	if err != nil {
		return nil, err
	}
	return relations, nil
}

func (d *dao) getRelatedIDsFromLabelAble(
	ctx context.Context, appID uint64, labelAbles []model.LabelAble,
) (qaIDs, docIDs, qaDocIDs []uint64, err error) {
	for _, v := range labelAbles {
		switch v.GetType() {
		case model.AttributeLabelTypeDOC:
			doc, ok := v.(*model.Doc)
			if !ok {
				log.ErrorContextf(ctx, "不支持的文档标签类型, type: %d, labeler: %+v", v.GetType(), v)
				return qaIDs, docIDs, qaDocIDs, errs.ErrAttributeLabelNotSupported
			}
			if doc.RobotID != appID {
				log.ErrorContextf(ctx, "批量获取标签失败, 标签不属于同一个应用 appID: %d, doc: %+v", appID, doc)
				return qaIDs, docIDs, qaDocIDs, errs.ErrAttributeBelongsToDifferentApps
			}
			if doc.AttrRange == model.AttrRangeAll {
				// do nothing
			} else {
				docIDs = append(docIDs, doc.ID)
			}
		case model.AttributeLabelTypeQA:
			qa, ok := v.(*model.DocQA)
			if !ok {
				log.ErrorContextf(ctx, "不支持的问答标签类型, type: %d, labeler: %+v", v.GetType(), v)
				return qaIDs, docIDs, qaDocIDs, errs.ErrAttributeLabelNotSupported
			}
			if qa.RobotID != appID {
				log.ErrorContextf(ctx, "批量获取标签失败, 标签不属于同一个应用 appID: %d, doc: %+v", appID, qa)
				return qaIDs, docIDs, qaDocIDs, errs.ErrAttributeBelongsToDifferentApps
			}
			if qa.Source == model.SourceFromDoc {
				qaDocIDs = append(qaDocIDs, qa.OriginDocID)
				docIDs = append(docIDs, qa.OriginDocID)
			} else if qa.AttrRange == model.AttrRangeAll {
				// do nothing
			} else {
				qaIDs = append(qaIDs, qa.ID)
			}
		default:
			log.ErrorContextf(ctx, "不支持的标签类型, type: %d, labeler: %+v", v.GetType(), v)
			return qaIDs, docIDs, qaDocIDs, errs.ErrAttributeLabelNotSupported
		}
	}
	return qaIDs, docIDs, qaDocIDs, nil
}

func (d *dao) buildRelationFromLabelAble(
	ctx context.Context, labelAbles []model.LabelAble,
	qaLabelMap map[uint64][]model.LabelRelationer, docLabelMap map[uint64][]model.LabelRelationer,
	qaDocs map[uint64]*model.Doc,
) (relations [][]model.LabelRelationer, err error) {
	for _, v := range labelAbles {
		switch v.GetType() {
		case model.AttributeLabelTypeDOC:
			doc, ok := v.(*model.Doc)
			if !ok {
				log.ErrorContextf(ctx, "不支持的文档标签类型, type: %d, labeler: %+v", v.GetType(), v)
				return nil, errs.ErrAttributeLabelNotSupported
			}
			if doc.AttrRange == model.AttrRangeAll {
				relations = append(relations, []model.LabelRelationer{})
			} else {
				relations = append(relations, docLabelMap[doc.ID])
			}
		case model.AttributeLabelTypeQA:
			qa, ok := v.(*model.DocQA)
			if !ok {
				log.ErrorContextf(ctx, "不支持的问答标签类型, type: %d, labeler: %+v", v.GetType(), v)
				return nil, errs.ErrAttributeLabelNotSupported
			}
			if qa.Source == model.SourceFromDoc {
				doc, ok := qaDocs[qa.OriginDocID]
				if !ok {
					log.ErrorContextf(
						ctx, "qaDoc not found, qaDocID: %d, err: %v", qa.OriginDocID, errs.ErrDocNotFound,
					)
					return nil, errs.ErrDocNotFound
				}
				if doc.AttrRange == model.AttrRangeAll {
					relations = append(relations, []model.LabelRelationer{})
				} else {
					relations = append(relations, docLabelMap[doc.ID])
				}
			} else if qa.AttrRange == model.AttrRangeAll {
				relations = append(relations, []model.LabelRelationer{})
			} else {
				relations = append(relations, qaLabelMap[qa.ID])
			}
		default:
			log.ErrorContextf(ctx, "不支持的标签类型, type: %d, labeler: %+v", v.GetType(), v)
			return nil, errs.ErrAttributeLabelNotSupported
		}
	}
	return relations, nil
}

func (d *dao) getQaAndDocAttributeLabelMap(
	ctx context.Context, appID uint64, qaIDs, docIDs []uint64,
) (map[uint64][]model.LabelRelationer, map[uint64][]model.LabelRelationer, error) {
	// 获取所有的 qa 标签
	qaLabels, err := d.GetQAAttributeLabel(ctx, appID, slicex.Unique(qaIDs))
	if err != nil {
		return nil, nil, err
	}
	// qa 关联的所有标签 map[qaID][]*model.QAAttributeLabel
	qaLabelMap := make(map[uint64][]model.LabelRelationer)
	for _, v := range qaLabels {
		if v.HasDeleted() {
			continue
		}
		qaLabelMap[v.QAID] = append(qaLabelMap[v.QAID], v)
	}
	// 获取所有的 doc 标签
	docLabels, err := d.GetDocAttributeLabel(ctx, appID, slicex.Unique(docIDs))
	if err != nil {
		return nil, nil, err
	}
	// doc 关联的所有标签 map[docID][]*model.DocAttributeLabel
	docLabelMap := make(map[uint64][]model.LabelRelationer)
	for _, v := range docLabels {
		if v.HasDeleted() {
			continue
		}
		docLabelMap[v.DocID] = append(docLabelMap[v.DocID], v)
	}
	return qaLabelMap, docLabelMap, nil
}

// GetAttributes 获取标签
// 返回数组索引对应入参 labelAbles 索引
func (d *dao) GetAttributes(
	ctx context.Context, appID uint64, labelAbles []model.LabelAble,
) ([]model.Attributes, error) {
	labelRelations, err := d.getLabelRelationsFromLabelAble(ctx, appID, labelAbles)
	if err != nil {
		log.ErrorContextf(ctx, "getLabelRecordsFromLabelAble fail, err: %v", err)
		return nil, err
	}
	// 获取所有关联的 attribute 和 attribute_label
	var attrIDs []uint64
	var attrLabelIDs []uint64
	for _, relations := range labelRelations {
		for _, v := range relations {
			attrIDs = append(attrIDs, uint64(v.GetAttrID()))
			attrLabelIDs = append(attrLabelIDs, uint64(v.GetLabelID()))
		}
	}
	attrIDs = slicex.Unique(attrIDs)
	attrLabelIDs = slicex.Unique(attrLabelIDs)
	// 获取所有关联的属性 map[attrID]*model.Attribute
	attrMap, err := d.GetAttributeByIDs(ctx, appID, attrIDs)
	if err != nil {
		return nil, err
	}
	// 获取所有关联的标签 map[attrLabelID]*model.AttributeLabel
	attrLabelMap, err := d.GetAttributeLabelByIDs(ctx, attrLabelIDs, appID)
	if err != nil {
		return nil, err
	}
	// 按属性值分组合并标签
	var attrGroup []model.Attributes
	for _, relations := range labelRelations {
		g := make(map[model.AttributeID]model.AttributeLabelItem)
		for _, label := range relations {
			v, ok := g[label.GetAttrID()]
			if !ok {
				attr, ok := attrMap[uint64(label.GetAttrID())]
				if !ok {
					log.ErrorContextf(ctx, "attribute not found, id: %d", label.GetAttrID())
					return nil, errs.ErrAttributeLabelNotFound
				}
				v = model.AttributeLabelItem{
					Attr:   attr,
					Labels: []*model.AttributeLabel{},
				}
				g[label.GetAttrID()] = v
			}
			if label.GetLabelID() == 0 {
				continue
			}
			attrLabel, ok := attrLabelMap[uint64(label.GetLabelID())]
			if !ok {
				log.ErrorContextf(ctx, "attribute label not found, id: %d", label.GetAttrID())
				return nil, errs.ErrAttributeLabelNotFound
			}
			v.Labels = append(v.Labels, attrLabel)
			g[label.GetAttrID()] = v
		}
		attrGroup = append(attrGroup, g)
	}
	return attrGroup, nil
}

// GetAttributeKeysDelStatusAndIDs 获取属性标签的删除状态和id
func (d *dao) GetAttributeKeysDelStatusAndIDs(ctx context.Context, robotID uint64, attrKeys []string) (
	map[string]*model.Attribute, error) {
	delStatusAndIDs := make(map[string]*model.Attribute)
	if len(attrKeys) == 0 {
		return delStatusAndIDs, nil
	}
	var args []any
	args = append(args, robotID)
	for _, key := range attrKeys {
		args = append(args, key)
	}
	querySQL := fmt.Sprintf(getAttributeKeysDelStatus, placeholder(len(attrKeys)))
	var attrs []*model.Attribute
	db := knowClient.DBClient(ctx, attributeProdTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &attrs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取属性标签的删除状态 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, attr := range attrs {
		delStatusAndIDs[attr.AttrKey] = attr
	}
	return delStatusAndIDs, nil
}

// createAttributeLabelsRedis 新增属性标签redis
func (d *dao) createAttributeLabelsRedis(ctx context.Context, attrItem *model.AttributeLabelItem) error {
	log.InfoContextf(ctx, "createAttributeLabelsRedis: attrItem: %+v", attrItem)
	redisKey := d.generateAttributeLabelRedisKeyPreview(attrItem.Attr.RobotID, attrItem.Attr.AttrKey)
	redisValue := make([]model.AttributeLabelRedisValue, 0)
	for _, label := range attrItem.Labels {
		redisValue = append(redisValue, model.AttributeLabelRedisValue{
			BusinessID:    label.BusinessID,
			Name:          label.Name,
			SimilarLabels: label.SimilarLabel,
		})
	}
	var redisValueStr string
	redisValueStr, err := jsoniter.MarshalToString(redisValue)
	if err != nil {
		log.ErrorContextf(ctx, "createAttributeLabelsRedis: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	expireSec := config.App().AttributeLabel.RedisExpireSecond
	if _, err = d.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", expireSec); err != nil {
		log.ErrorContextf(ctx, "createAttributeLabelsRedis: set redis value1 failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "createAttributeLabelsRedis ok, key:%s", redisKey)
	return nil
}

// GetAttributeLabelsRedis 获取属性标签redis
func (d *dao) GetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey, envType string) (
	[]model.AttributeLabelRedisValue, error) {
	log.InfoContextf(ctx, "GetAttributeLabelsRedis: env:%s, robotID: %v, attrKey: %v", envType, robotID, attrKey)
	var redisKey string
	if envType == model.AttributeLabelsPreview {
		redisKey = d.generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	} else {
		redisKey = d.generateAttributeLabelRedisKeyProd(robotID, attrKey)
	}
	redisValue, err := redis.String(d.RedisCli().Do(ctx, "GET", redisKey))
	if err == nil {
		if redisValue == "" {
			log.ErrorContextf(ctx, "GetAttributeLabelsRedis: redis value is empty")
			return nil, fmt.Errorf("redis value is empty")
		}
		var attrLabels = make([]model.AttributeLabelRedisValue, 0)
		if err1 := jsoniter.UnmarshalFromString(redisValue, &attrLabels); err1 != nil {
			log.ErrorContextf(ctx, "GetAttributeLabelsRedis: unmarshal redis value failed, %+v", err1)
			return nil, err1
		}
		log.InfoContextf(ctx, "GetAttributeLabelsRedis result: redisKey:%s, value: %+v", redisKey, attrLabels)
		return attrLabels, nil
	}
	if errors.Is(err, redis.ErrNil) { // key不存在
		return nil, nil
	}
	log.ErrorContextf(ctx, "GetAttributeLabelsRedis failed: redisKey:%s, %+v", redisKey, err)
	return nil, err
}

// delAttributeLabelsRedis 删除属性标签redis
func (d *dao) delAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey string) error {
	redisKey := d.generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	var err error
	if _, err = d.RedisCli().Do(ctx, "DEL", redisKey); err != nil {
		log.InfoContextf(ctx, "delAttributeLabelsRedis: del redis key failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "delAttributeLabelsRedis ok, key:%s", redisKey)
	return nil
}

// SetAttributeLabelsRedis 添加属性标签redis
func (d *dao) SetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey string,
	redisValue []model.AttributeLabelRedisValue, envType string) error {
	if len(redisValue) == 0 {
		return nil
	}
	var redisKey string
	if envType == model.AttributeLabelsPreview {
		redisKey = d.generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	} else {
		redisKey = d.generateAttributeLabelRedisKeyProd(robotID, attrKey)
	}

	log.DebugContextf(ctx, "SetAttributeLabelsRedis: redisKey: %v, redisValue: %+v", redisKey, redisValue)
	var redisValueStr string
	redisValueStr, err := jsoniter.MarshalToString(redisValue)
	if err != nil {
		log.ErrorContextf(ctx, "SetAttributeLabelsRedis: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	expireSec := config.App().AttributeLabel.RedisExpireSecond
	if _, err = d.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", expireSec); err != nil {
		log.ErrorContextf(ctx, "SetAttributeLabelsRedis: set redis value failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.DebugContextf(ctx, "SetAttributeLabelsRedis ok, key:%s", redisKey)
	return nil
}

// updateAttributeLabelsRedis 更新属性标签redis
func (d *dao) updateAttributeLabelsRedis(ctx context.Context, req *model.UpdateAttributeLabelReq,
	oldAttr *model.Attribute) error {
	log.InfoContextf(ctx, "updateAttributeLabelsRedis: request|Attr:%+v, deletedIDs:%+v, "+
		"AddLabels:%+v, UpdateLabels:%+v|oldAttr: %+v", req.Attr, req.DeleteLabelIDs, req.AddLabels,
		req.UpdateLabels, oldAttr)
	labelRedisValue, err := d.GetAttributeLabelsRedis(ctx, oldAttr.RobotID, oldAttr.AttrKey,
		model.AttributeLabelsPreview)
	if err != nil {
		// 获取旧数据失败
		log.ErrorContextf(ctx, "updateAttributeLabelsRedis, get old redis failed, err: %v", err)
		return err
	}
	newLabelRedisValue := make([]model.AttributeLabelRedisValue, 0)
	for _, label := range labelRedisValue {
		if slices.Contains(req.DeleteLabelBizIDs, label.BusinessID) { // 删除的
			continue
		}
		var i = 0
		var updateLabel *model.AttributeLabel
		for i, updateLabel = range req.UpdateLabels { // 修改的
			if updateLabel.BusinessID == label.BusinessID {
				newLabelRedisValue = append(newLabelRedisValue, model.AttributeLabelRedisValue{
					BusinessID:    updateLabel.BusinessID,
					Name:          updateLabel.Name,
					SimilarLabels: updateLabel.SimilarLabel,
				})
				break
			}
		}
		if i == len(req.UpdateLabels) {
			newLabelRedisValue = append(newLabelRedisValue, label) // 不变的
		}
	}
	for _, addLabel := range req.AddLabels {
		newLabelRedisValue = append(newLabelRedisValue, model.AttributeLabelRedisValue{
			BusinessID:    addLabel.BusinessID,
			Name:          addLabel.Name,
			SimilarLabels: addLabel.SimilarLabel,
		})
	}
	log.InfoContextf(ctx, "updateAttributeLabelsRedis: oldLabelRedisValue:%+v, newLabelRedisValue: %+v",
		labelRedisValue, newLabelRedisValue)
	return d.SetAttributeLabelsRedis(ctx, oldAttr.RobotID, oldAttr.AttrKey, newLabelRedisValue,
		model.AttributeLabelsPreview)
}

// PiplineDelAttributeLabelRedis 批量删除属性标签redis
func (d *dao) PiplineDelAttributeLabelRedis(ctx context.Context, robotID uint64, attrKeys []string,
	envType string) error {
	log.InfoContextf(ctx, "PiplineDelAttributeLabelRedis begin, robotID:%d, attrKeys:%v", robotID, attrKeys)
	conn, err := d.redis.Pipeline(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "PiplineDelAttributeLabelRedis err:%+v", err)
		return err
	}
	defer func() { _ = conn.Close() }()
	for _, attrKey := range attrKeys {
		var key string
		if envType == model.AttributeLabelsPreview {
			key = d.generateAttributeLabelRedisKeyPreview(robotID, attrKey)
		} else {
			key = d.generateAttributeLabelRedisKeyProd(robotID, attrKey)
		}
		if err = conn.Send("DEL", key); err != nil {
			log.ErrorContextf(ctx, "PiplineDelAttributeLabelRedis: del redis key failed, key:%s, %+v", key, err)
			return err
		}
	}
	if err = conn.Flush(); err != nil {
		log.ErrorContextf(ctx, "PiplineDelAttributeLabelRedis: flush redis pipeline failed, %+v", err)
		return err
	}
	log.InfoContextf(ctx, "PiplineDelAttributeLabelRedis ok, robotID:%d, attrKeys:%v", robotID, attrKeys)
	return nil
}

// PiplineSetAttributeLabelRedis 批量删除属性标签redis
func (d *dao) PiplineSetAttributeLabelRedis(ctx context.Context, robotID uint64,
	attrKey2RedisValue map[string][]model.AttributeLabelRedisValue, envType string) error {
	log.InfoContextf(ctx, "PiplineSetAttributeLabelRedis begin, robotID:%d, attrKey2RedisValue:%v",
		robotID, attrKey2RedisValue)
	conn, err := d.redis.Pipeline(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "PiplineSetAttributeLabelRedis err:%+v", err)
		return err
	}
	defer func() { _ = conn.Close() }()
	for attrKey, redisValue := range attrKey2RedisValue {
		var redisKey string
		if envType == model.AttributeLabelsPreview {
			redisKey = d.generateAttributeLabelRedisKeyPreview(robotID, attrKey)
		} else {
			redisKey = d.generateAttributeLabelRedisKeyProd(robotID, attrKey)
		}
		var redisValueStr string
		redisValueStr, err = jsoniter.MarshalToString(redisValue)
		if err != nil {
			log.ErrorContextf(ctx, "SetAttributeLabelsRedis: marshal redisValue failed, key:%s, %+v",
				redisKey, err)
			return err
		}
		expireSec := config.App().AttributeLabel.RedisExpireSecond
		if err = conn.Send("SET", redisKey, redisValueStr, "EX", expireSec); err != nil {
			log.ErrorContextf(ctx, "PiplineSetAttributeLabelRedis: set redis key failed, key:%s, %+v",
				redisKey, err)
			return err
		}
	}
	if err = conn.Flush(); err != nil {
		log.ErrorContextf(ctx, "PiplineSetAttributeLabelRedis: flush redis pipeline failed, %+v", err)
		return err
	}
	log.InfoContextf(ctx, "PiplineSetAttributeLabelRedis ok, robotID:%d, attrKey2RedisValue:%v",
		robotID, attrKey2RedisValue)
	return nil
}

func (d *dao) generateAttributeLabelRedisKeyPreview(robotID uint64, attrKey string) string {
	return fmt.Sprintf("knowledge_config_attr_label_preview_%d_%s", robotID, attrKey)
}

func (d *dao) generateAttributeLabelRedisKeyProd(robotID uint64, attrKey string) string {
	return fmt.Sprintf("knowledge_config_attr_label_prod_%d_%s", robotID, attrKey)
}
