package dao

import (
	"context"
	"database/sql"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/realtime"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
)

const (
	docFields = `
		id,business_id,robot_id,corp_id,staff_id,file_name,file_name_in_audit,file_type,file_size,bucket,cos_url,cos_hash,message,status,
		is_refer,is_deleted,source,web_url,batch_id,audit_flag,char_size,is_creating_qa,is_creating_index,next_action,
		attr_range,is_created_qa,refer_url_type,create_time,update_time,expire_start,expire_end,opt,category_id,
		original_url,processing_flag,customer_knowledge_id,attribute_flag,is_downloadable,update_period_h,next_update_time,split_rule`
	getDocCount = `
		SELECT
			count(DISTINCT t_doc.id)
		FROM
		    t_doc %s
		WHERE
		    t_doc.corp_id = ? AND t_doc.robot_id = ? AND t_doc.is_deleted = ? %s
	`
	getDocList = `
		SELECT DISTINCT
    		%s
		FROM
		    t_doc %s
		WHERE
		    t_doc.corp_id = ? AND t_doc.robot_id = ? AND t_doc.is_deleted = ? %s
		ORDER BY
		    t_doc.create_time DESC,t_doc.id DESC
		LIMIT ?,?
		`
	deleteDocByID = `
		UPDATE
		    t_doc
		SET
		    status = :status,
		    is_deleted = :is_deleted,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	getDocByIDs = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    id IN (%s)
	`
	getDocByBizIDs = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    business_id IN (%s)
	`
	getDocByIDAndFileName = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    id IN (%s) %s
	`
	getDocByBusinessIDs = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    business_id IN (%s)
	`
	getDocIDByBusinessID = `
		SELECT
			id
		FROM
		    t_doc
		WHERE
		    business_id = ?
	`
	getDocByBusinessIDAndStatus = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    corp_id = ? AND robot_id = ? AND status IN (2,5) LIMIT 1
	`

	referDoc = `
		UPDATE
		    t_doc
		SET
		    is_refer = :is_refer, update_time = :update_time
		WHERE
		    id = :id
	`
	createDoc = `
		INSERT INTO
		    t_doc (%s)
		VALUES
			(null,:business_id,:robot_id,:corp_id,:staff_id,:file_name,:file_name_in_audit,:file_type,:file_size,:bucket,:cos_url,:cos_hash,
		    :message,:status,:is_refer,:is_deleted,:source,:web_url,:batch_id,:audit_flag,:char_size,:is_creating_qa,
			:is_creating_index,:next_action,:attr_range,:is_created_qa,:refer_url_type,:create_time,:update_time,
		    :expire_start,:expire_end,:opt,:category_id,:original_url,:processing_flag,:customer_knowledge_id,
			:attribute_flag,:is_downloadable,:update_period_h,:next_update_time,:split_rule)
	`
	updateDocCharSize = `
		UPDATE
		    t_doc
		SET
		    char_size = :char_size, update_time = :update_time
		WHERE
		    id = :id
	`
	updateDocStatus = `
		UPDATE
		    t_doc
		SET
		    status = :status, update_time = :update_time
		WHERE
		    id = :id
	`
	updateDocStatusAndCharSize = `
		UPDATE
		    t_doc
		SET
		    char_size = :char_size, message = :message, audit_flag = :audit_flag, status = :status,
			update_time = :update_time, staff_id = :staff_id
		WHERE
		    id = :id
	`
	createDocQADone = `
		UPDATE
			t_doc
		SET
		    message = :message,
		    is_deleted = :is_deleted,
		    is_creating_qa = :is_creating_qa,
		    is_created_qa = :is_created_qa,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	getDocByCosHash = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    corp_id = ? AND robot_id = ? AND cos_hash = ? AND is_deleted = ?
	`
	updateCosInfo = `
		UPDATE
			t_doc
		SET
		    file_name = :file_name,
		    cos_url = :cos_url,
            file_type = :file_type,
            cos_hash = :cos_hash,
		    file_size = :file_size,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	getDeletingDoc = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    corp_id = ?
		    AND robot_id = ?
		    AND is_deleted = ?
		    AND status = ?
	`
	getCreatingIndexDoc = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    corp_id = ?
		    AND robot_id = ?
		    AND is_creating_index = ?
	`
	docDeleteSuccess = `
		UPDATE
			t_doc
		SET
		    status = :status,
		    next_action = :next_action,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	updateDocAuditResult = `
		UPDATE
			t_doc
		SET
		    status = :status,
		    message = :message,
		    audit_flag = :audit_flag,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	updateCreatingQAFlag = `
		UPDATE
			t_doc
		SET
		    batch_id = :batch_id,
		    is_creating_qa = :is_creating_qa,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	updateCreatingQATaskFlag = `
		UPDATE
			t_doc
		SET
		    is_creating_qa = :is_creating_qa,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	updateCreatingIndexFlag = `
		UPDATE
			t_doc
		SET
		    batch_id = :batch_id,
			status = :status,
		    is_creating_index = :is_creating_index,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	updateDocNameAndStatus = `
		UPDATE
			t_doc
		SET
			file_name = :file_name,
			file_name_in_audit = :file_name_in_audit,
			status = :status,
			next_action = :next_action,
		    is_creating_index = :is_creating_index,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	updateDocReleasing = `
		UPDATE
			t_doc
		SET
		    status = ?,
		    update_time = ?
		WHERE
		    id IN (%s)
	`
	updateDocReleaseSuccess = `
		UPDATE
			t_doc
		SET
		    status = :status,
		    next_action = :next_action,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	getWaitReleaseDocCount = `
		SELECT
			count(*)
		FROM
		    t_doc
		WHERE
		    corp_id = ? AND robot_id = ? AND status = ? %s
	`
	getWaitReleaseDoc = `
		SELECT
			%s
		FROM
		    t_doc
		WHERE
		    corp_id = ? AND robot_id = ? AND status = ? %s
		LIMIT
			?,?
	`
	getDocCharSize = `
		SELECT
    		IFNULL(SUM(char_size), 0)
		FROM
		    t_doc
		WHERE
		     robot_id = ?
		AND corp_id = ?
		AND is_deleted = ?
		AND status NOT IN (%s)
	`
	updateDoc = `
		UPDATE
			t_doc
		SET
			status = :status,
			is_refer = :is_refer,
			next_action = :next_action,
			attr_range = :attr_range,
			refer_url_type = :refer_url_type,
			web_url = :web_url,
			category_id = :category_id,
			expire_start = :expire_start,
		    expire_end = :expire_end,
			customer_knowledge_id = :customer_knowledge_id,
			attribute_flag = :attribute_flag,
			is_downloadable = :is_downloadable,
			staff_id = :staff_id,
			update_period_h = :update_period_h,
			next_update_time = :next_update_time,
			split_rule = :split_rule
		WHERE
			id = :id
	`
	renameDocSQL = `
		UPDATE
			t_doc
		SET
			file_name_in_audit = :file_name_in_audit,
			status = :status,
			staff_id = :staff_id
		WHERE
			id = :id
	`
	docModifySuccess = `
		UPDATE
			t_doc
		SET
		    status = :status,
		    next_action = :next_action,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	docModifyFail = `
		UPDATE
			t_doc
		SET
		    status = :status,
		    update_time = :update_time
		WHERE
		    id = :id
	`

	getResumeDocCount = `
		SELECT COUNT(*) FROM t_doc
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND status in (?)
	`

	getAppDocExceedCharSize = `
		SELECT
		    IFNULL(SUM(char_size), 0) as exceed_char_size, robot_id
		FROM
		    t_doc
		WHERE
			corp_id = ? AND is_deleted = ? AND robot_id IN (%s) AND status IN (%s)
		Group By
		    robot_id
	`

	getDocListJoinSql = `LEFT JOIN t_doc_attribute_label ON t_doc.id = t_doc_attribute_label.doc_id AND t_doc.robot_id = t_doc_attribute_label.robot_id
  LEFT JOIN t_attribute_label ON t_doc_attribute_label.label_id = t_attribute_label.id
  LEFT JOIN t_attribute ON t_doc_attribute_label.attr_id = t_attribute.id`
)

var special = strings.NewReplacer(`\`, `\\`, `_`, `\_`, `%`, `\%`, `'`, `\'`)

// GetDocList 获取文档列表
func (d *dao) GetDocList(ctx context.Context, req *model.DocListReq) (uint64, []*model.Doc, error) {
	if len(req.Status) > 0 {
		hasDocStatusUpdating := false // 更新中、更新失败两种状态分别合并到学习中、学习失败
		hasDocStatusUpdateFail := false
		for _, stat := range req.Status {
			if stat == model.DocStatusUpdating {
				hasDocStatusUpdating = true
			} else if stat == model.DocStatusUpdateFail {
				hasDocStatusUpdateFail = true
			}
		}
		if hasDocStatusUpdating {
			req.Status = append(req.Status, model.DocStatusCreatingIndex)
		}
		if hasDocStatusUpdateFail {
			req.Status = append(req.Status, model.DocStatusCreateIndexFail)
		}
		req.Status = slicex.Unique(req.Status)
	}
	args := make([]any, 0, 6)
	args = append(args, req.CorpID, req.RobotID, model.DocIsNotDeleted)
	condition := ""
	joinSql := ""
	if req.FileName != "" {
		fileNameArg := fmt.Sprintf("%%%s%%", special.Replace(req.FileName))
		if req.QueryType == model.DocQueryTypeFileName {
			condition = fmt.Sprintf("%s%s", condition, " AND ((t_doc.file_name LIKE ? AND t_doc.file_name_in_audit = '') OR t_doc.file_name_in_audit LIKE ? )")
			args = append(args, fileNameArg, fileNameArg)
		}
		if req.QueryType == model.DocQueryTypeAttribute {
			joinSql = getDocListJoinSql
			condition = fmt.Sprintf("%s%s", condition, " AND (t_attribute_label.name LIKE ? OR t_attribute_label.similar_label LIKE ? OR t_attribute.name LIKE ?)")
			args = append(args, fileNameArg, fileNameArg, fileNameArg)
		}
	}
	if len(req.FileTypes) != 0 {
		condition = fmt.Sprintf("%s AND t_doc.file_type IN (%s)", condition, placeholder(len(req.FileTypes)))
		for _, fileType := range req.FileTypes {
			args = append(args, fileType)
		}
	}
	if req.ValidityStatus != 0 || len(req.Status) != 0 {
		c, a := d.getDocStatusConditionAndArgs(req.Status, req.ValidityStatus)
		condition = fmt.Sprintf("%s%s", condition, c)
		args = append(args, a...)
	}
	if len(req.Opts) != 0 {
		condition = fmt.Sprintf("%s AND t_doc.opt IN (%s)", condition, placeholder(len(req.Opts)))
		for i := range req.Opts {
			args = append(args, req.Opts[i])
		}
	}
	if len(req.CateIDs) != 0 {
		condition = fmt.Sprintf("%s AND category_id IN (%s)", condition, placeholder(len(req.CateIDs)))
		for _, cID := range req.CateIDs {
			args = append(args, cID)
		}
	}
	querySQL := fmt.Sprintf(getDocCount, joinSql, condition)
	var total uint64
	db := knowClient.DBClient(ctx, docTableName, req.RobotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取文档总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	// 查询字段指定表名
	docFieldsArr := strings.Split(docFields, ",")
	for i := range docFieldsArr {
		docFieldsArr[i] = "t_doc." + strings.Trim(docFieldsArr[i], " ")
	}
	querySQL = fmt.Sprintf(getDocList, strings.Join(docFieldsArr, ","), joinSql, condition)
	offset := (req.Page - 1) * req.PageSize
	args = append(args, offset, req.PageSize)
	docs := make([]*model.Doc, 0)
	log.DebugContextf(ctx, "获取文档列表 sql:%s args:%+v", querySQL, args)
	if err := db.QueryToStructs(ctx, &docs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取文档列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	for i := range docs {
		if docs[i].Status == model.DocStatusUpdating {
			docs[i].Status = model.DocStatusCreatingIndex
		} else if docs[i].Status == model.DocStatusUpdateFail {
			docs[i].Status = model.DocStatusCreateIndexFail
		}
	}
	return total, docs, nil
}

// getDocStatusConditionAndArgs 根据过期状态以及发布状态返回相关sql查询条件
func (d *dao) getDocStatusConditionAndArgs(status []uint32, validityStatus uint32) (string, []interface{}) {
	var c string
	var args []any
	// 勾选其他状态，未勾选已过期
	if len(status) != 0 && validityStatus != model.DocExpiredStatus {
		c = fmt.Sprintf(` AND t_doc.status IN (%s) AND (t_doc.expire_end = ? OR t_doc.expire_end >= ?) `,
			placeholder(len(status)))
		for i := range status {
			args = append(args, status[i])
		}
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		return c, args
	}
	// 只勾选已过期
	if len(status) == 0 && validityStatus == model.DocExpiredStatus {
		c = ` AND (t_doc.expire_end > ? && t_doc.expire_end < ?) `
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		return c, args
	}
	// 勾选其他状态+已过期
	if len(status) != 0 && validityStatus == model.DocExpiredStatus {
		c = fmt.Sprintf(` AND (%s OR (%s AND t_doc.status IN (%s))) `,
			` (t_doc.expire_end > ? && t_doc.expire_end < ?) `,
			` (t_doc.expire_end = ? OR t_doc.expire_end >= ?) `, placeholder(len(status)))
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"),
			time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		for i := range status {
			args = append(args, status[i])
		}
	}
	return c, args
}

// GetDocsCharSizeExceededAndExpire 获取超量失效而且已经超过超时状态保留时间的文档列表
func (d *dao) GetDocsCharSizeExceededAndExpire(ctx context.Context, req *model.DocListReq, reserveTime time.Duration) (
	uint64, []*model.Doc, error) {
	args := make([]any, 0)
	args = append(args, req.CorpID, req.RobotID, model.DocIsNotDeleted)
	condition := ""
	joinSql := ""

	// 文档字数服超量相关的状态
	docCharSizeExceededStatus := []uint32{
		model.DocStatusCharExceeded,
		model.DocStatusParseImportFailCharExceeded,
		model.DocStatusAuditFailCharExceeded,
		model.DocStatusUpdateFailCharExceeded,
		model.DocStatusCreateIndexFailCharExceeded,
		model.DocStatusAppealFailedCharExceeded,
	}
	condition += fmt.Sprintf(` AND t_doc.status IN (%s)`, placeholder(len(docCharSizeExceededStatus)))
	for _, status := range docCharSizeExceededStatus {
		args = append(args, status)
	}

	lastUpdateTime := time.Now().Add(-reserveTime).Format("2006-01-02 15:04:05.000")
	condition += fmt.Sprintf(` AND t_doc.update_time <= ? `)
	args = append(args, lastUpdateTime)

	querySQL := fmt.Sprintf(getDocCount, joinSql, condition)
	var total uint64
	db := knowClient.DBClient(ctx, docTableName, req.RobotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取超量失效而且已经超过超时状态保留时间的文档总数失败 sql:%s args:%+v err:%+v",
			querySQL, args, err)
		return 0, nil, err
	}
	// 查询字段指定表名
	docFieldsArr := strings.Split(docFields, ",")
	for i := range docFieldsArr {
		docFieldsArr[i] = "t_doc." + strings.Trim(docFieldsArr[i], " ")
	}
	querySQL = fmt.Sprintf(getDocList, strings.Join(docFieldsArr, ","), joinSql, condition)
	offset := (req.Page - 1) * req.PageSize
	args = append(args, offset, req.PageSize)
	docs := make([]*model.Doc, 0)
	log.DebugContextf(ctx, "获取超量失效而且已经超过超时状态保留时间的文档列表 sql:%s args:%+v", querySQL, args)
	if err := db.QueryToStructs(ctx, &docs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取超量失效而且已经超过超时状态保留时间的文档列表失败 sql:%s args:%+v err:%+v",
			querySQL, args, err)
		return 0, nil, err
	}
	return total, docs, nil
}

// DeleteDocs 删除文档
func (d *dao) DeleteDocs(ctx context.Context, staffID, businessID uint64, docs []*model.Doc) error {
	now := time.Now()
	if len(docs) == 0 {
		return nil
	}
	db := knowClient.DBClient(ctx, docTableName, docs[0].RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, doc := range docs {
			doc.IsDeleted = model.DocIsDeleted
			doc.Status = model.DocStatusDeleting
			doc.UpdateTime = now
			querySQL := deleteDocByID
			if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
				log.ErrorContextf(ctx, "删除文档失败 sql:%s doc:%+v err:%+v", querySQL, doc, err)
				return err
			}
			if err := d.UpdateAppUsedCharSize(ctx, tx, -int64(doc.CharSize), doc.RobotID); err != nil {
				log.ErrorContextf(ctx, "更新机器人已用字符数失败 sql:%s doc:%+v err:%+v", querySQL, doc, err)
				return err
			}
			if err := d.deleteDocAttributeLabel(ctx, tx, doc.RobotID, doc.ID); err != nil {
				log.ErrorContextf(ctx, "删除文档关联的标签标准词失败 sql:%s doc:%+v err:%+v", querySQL, doc, err)
				return err
			}
			if err := newDocDeleteTask(ctx, doc.RobotID, model.DocDeleteParams{
				CorpID:  doc.CorpID,
				StaffID: staffID,
				RobotID: doc.RobotID,
				DocID:   doc.ID,
			}); err != nil {
				return err
			}
			docParse, err := d.GetDocParseByDocIDAndTypeAndStatus(ctx, doc.ID, model.DocParseTaskTypeWordCount,
				model.DocParseIng, doc.RobotID)
			if err != nil {
				log.InfoContextf(ctx, "DeleteDocs 文档解析任务未找到 docID:%+d", doc.ID)
				continue // 如果没有正在进行的解析任务，则不用发送停止解析信号
			}
			requestID := trace.SpanContextFromContext(ctx).TraceID().String()
			err = d.StopDocParseTask(ctx, docParse.TaskID, requestID, businessID)
			if err != nil {
				log.WarnContextf(ctx, "StopDocParseTask err:%+v,docParse:%+v,requestID:%s",
					err, docParse, requestID)
				continue // 如果发送停止解析信号失败，不阻塞流程继续
			}
			docParse.Status = model.DocParseCallBackCancel
			docParse.RequestID = requestID
			err = d.UpdateDocParseTask(ctx, docParse)
			if err != nil {
				log.WarnContextf(ctx, "UpdateDocParseTask err:%+v,docParse:%+v,requestID:%s",
					err, docParse, requestID)
				continue // 如果更新解析任务状态失败，不阻塞流程继续
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "删除文档失败 err:%+v", err)
		return err
	}
	return nil
}

// GetDocByID 通过ID获取文档
func (d *dao) GetDocByID(ctx context.Context, id uint64, robotID uint64) (*model.Doc, error) {
	docs, err := d.GetDocByIDs(ctx, []uint64{id}, robotID)
	if err != nil {
		return nil, err
	}
	doc, ok := docs[id]
	if !ok {
		return nil, nil
	}
	return doc, nil
}

// GetDocByIDs 通过ID获取文档，不区分是否标记为删除
func (d *dao) GetDocByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.Doc, error) {
	docs := make(map[uint64]*model.Doc, 0)
	if len(ids) == 0 {
		return docs, nil
	}
	querySQL := fmt.Sprintf(getDocByIDs, docFields, placeholder(len(ids)))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	list := make([]*model.Doc, 0)
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, doc := range list {
		docs[doc.ID] = doc
	}
	return docs, nil
}

// GetDocByBizIDs 通过业务ID获取文档
func (d *dao) GetDocByBizIDs(ctx context.Context, bizIDs []uint64, robotID uint64) (map[uint64]*model.Doc, error) {
	docs := make(map[uint64]*model.Doc, 0)
	if len(bizIDs) == 0 {
		return docs, nil
	}
	querySQL := fmt.Sprintf(getDocByBizIDs, docFields, placeholder(len(bizIDs)))
	args := make([]any, 0, len(bizIDs))
	for _, id := range bizIDs {
		args = append(args, id)
	}
	list := make([]*model.Doc, 0)
	dbClients := make([]mysql.Client, 0)
	if robotID == knowClient.NotVIP {
		dbClients = knowClient.GetAllDbClients(ctx, docTableName)
	} else {
		db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
		dbClients = append(dbClients, db)
	}
	var err error
	for _, db := range dbClients {
		err = db.QueryToStructs(ctx, &list, querySQL, args...)
		if err != nil {
			log.ErrorContextf(ctx, "通过ID获取文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	for _, doc := range list {
		docs[doc.BusinessID] = doc
	}
	return docs, nil
}

// GetDocByBizID 通过BusinessID获取文档
func (d *dao) GetDocByBizID(ctx context.Context, docBizID uint64, robotID uint64) (*model.Doc, error) {
	docs, err := d.GetDocByBizIDs(ctx, []uint64{docBizID}, robotID)
	if err != nil {
		return nil, err
	}
	doc, ok := docs[docBizID]
	if !ok {
		return nil, errs.ErrDocNotFound
	}
	return doc, nil
}

// GetDocByIDAndFileName 通过ID和文档名称获取文档
func (d *dao) GetDocByIDAndFileName(ctx context.Context, ids []uint64, fileName string) ([]*model.Doc, error) {
	docs := make([]*model.Doc, 0)
	if len(ids) == 0 {
		return docs, nil
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	condition := ""
	if fileName != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND file_name LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(fileName)))
	}
	querySQL := fmt.Sprintf(getDocByIDAndFileName, docFields, placeholder(len(ids)), condition)
	if err := d.db.QueryToStructs(ctx, &docs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID和文档名称获取文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return docs, nil
}

// GetDocIDByBusinessID 通过BusinessID获取文档ID
func (d *dao) GetDocIDByBusinessID(ctx context.Context, businessID uint64, robotID uint64) (uint64, error) {
	querySQL := getDocIDByBusinessID
	docIDs := make([]sql.NullInt64, 0, 1)
	args := []any{businessID}
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.Select(ctx, &docIDs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过BusinessID获取文档ID失败 sql:%s businessID:%+v err:%+v",
			querySQL, businessID, err)
		return 0, err
	}
	if len(docIDs) == 0 {
		return 0, errs.ErrDocNotFound
	}
	if docIDs[0].Valid {
		return uint64(docIDs[0].Int64), nil
	}
	return 0, errs.ErrDocNotFound
}

// IsDocInEditState 判断文档是否正在生成QA或者正在删除
func (d *dao) IsDocInEditState(ctx context.Context, corpID, robotID uint64) (bool, error) {
	querySQL := fmt.Sprintf(getDocByBusinessIDAndStatus, docFields)
	var list []*model.Doc
	args := []any{corpID, robotID}
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过BusinessID获取编辑态文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return false, err
	}
	return len(list) > 0, nil
}

// GetDocByCosHash 通过cos_hash获取文档
func (d *dao) GetDocByCosHash(ctx context.Context, corpID, robotID uint64, cosHash string) (*model.Doc, error) {
	querySQL := fmt.Sprintf(getDocByCosHash, docFields)
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, cosHash, model.DocIsNotDeleted)
	list := make([]*model.Doc, 0)
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过cos_hash获取文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// CreateDoc 创建doc（异步计算字符、审核、生成文档分段）
func (d *dao) CreateDoc(ctx context.Context, staffID uint64, doc *model.Doc,
	attributeLabelReq *model.UpdateDocAttributeLabelReq) error {
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		doc.UpdateTime = now
		doc.CreateTime = now
		sql := fmt.Sprintf(createDoc, docFields)
		res, err := tx.NamedExecContext(ctx, sql, doc)
		if err != nil {
			log.ErrorContextf(ctx, "创建文档失败, sql:%s, args:%+v, err:%+v", sql, doc, err)
			return err
		}
		id, _ := res.LastInsertId()
		doc.ID = uint64(id)
		if err := d.updateDocAttributeLabel(ctx, tx, doc.RobotID, doc.ID, attributeLabelReq); err != nil {
			return err
		}
		// 增加是否批量导入操作，如果是批量导入操作，才会作为excel解析处理
		if doc.IsBatchImport() && doc.IsExcel() {
			if err = d.sendExcelImportNotice(ctx, tx, staffID, doc); err != nil {
				return err
			}
			if err = newExcelToQATask(ctx, doc.RobotID, model.ExcelToQAParams{
				CorpID: doc.CorpID, StaffID: staffID, RobotID: doc.RobotID, DocID: doc.ID, EnvSet: getEnvSet(ctx),
			}); err != nil {
				log.ErrorContextf(ctx, "创建文档生成问答任务失败 err:%+v", err)
				return err
			}
			return nil
		}
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		taskID, err := d.SendDocParseWordCount(ctx, doc, requestID, "")
		if err != nil {
			return err
		}
		docParse := model.DocParse{
			DocID:     doc.ID,
			CorpID:    doc.CorpID,
			RobotID:   doc.RobotID,
			StaffID:   doc.StaffID,
			RequestID: requestID,
			Type:      model.DocParseTaskTypeWordCount,
			OpType:    model.DocParseOpTypeWordCount,
			Status:    model.DocParseIng,
			TaskID:    taskID,
		}
		err = d.CreateDocParse(ctx, tx, docParse)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建文档失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateDocWithLabel 创建doc不提交异步任务
func (d *dao) CreateDocWithLabel(ctx context.Context, doc *model.Doc,
	attributeLabelReq *model.UpdateDocAttributeLabelReq) error {
	if doc == nil {
		log.ErrorContextf(ctx, "CreateDocWithLabel|doc is null")
		return errs.ErrParams
	}
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		doc.UpdateTime = now
		doc.CreateTime = now
		sql := fmt.Sprintf(createDoc, docFields)
		res, err := tx.NamedExecContext(ctx, sql, doc)
		if err != nil {
			log.ErrorContextf(ctx, "创建文档失败, sql:%s, args:%+v, err:%+v", sql, doc, err)
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			log.ErrorContextf(ctx, "CreateDocWithLabel|LastInsertId|err:%+v", err)
			return errs.ErrParams
		}
		doc.ID = uint64(id)
		if err := d.updateDocAttributeLabel(ctx, tx, doc.RobotID, doc.ID, attributeLabelReq); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建文档失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateDocToIndexTask 创建问答生成索引任务
func (d *dao) CreateDocToIndexTask(ctx context.Context, doc *model.Doc, originDocBizID uint64) error {
	if doc == nil {
		return errs.ErrRobotOrDocNotFound
	}
	if err := newDocToIndexTask(ctx, doc.RobotID, model.DocToIndexParams{
		CorpID:                  doc.CorpID,
		StaffID:                 doc.StaffID,
		RobotID:                 doc.RobotID,
		DocID:                   doc.ID,
		InterveneOriginDocBizID: originDocBizID,
		ExpireStart:             doc.ExpireStart,
		ExpireEnd:               doc.ExpireEnd,
	}); err != nil {
		log.ErrorContextf(ctx, "创建问答生成索引任务失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateDocRenameToIndexTask 创建文档重命名后重建向量索引任务
func (d *dao) CreateDocRenameToIndexTask(ctx context.Context, doc *model.Doc) error {
	if err := newDocRenameToIndexTask(ctx, doc.RobotID, model.DocRenameToIndexParams{
		CorpID:      doc.CorpID,
		StaffID:     doc.StaffID,
		RobotID:     doc.RobotID,
		DocID:       doc.ID,
		ExpireStart: doc.ExpireStart,
		ExpireEnd:   doc.ExpireEnd,
	}); err != nil {
		log.ErrorContextf(ctx, "创建文档重命名后重建向量索引任务失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateDocToQATask 创建问答生成索引任务
func (d *dao) CreateDocToQATask(ctx context.Context, doc *model.Doc, qaTask *model.DocQATask, appBizID uint64) (uint64, error) {
	var qaTaskID uint64
	qaTaskType := model.DocQATaskStatusGenerating
	if qaTask != nil {
		qaTaskID = qaTask.ID
		qaTaskType = qaTask.Status
	}
	taskID, err := newDocToQATask(ctx, doc.RobotID, model.DocToQAParams{
		CorpID:     doc.CorpID,
		StaffID:    doc.StaffID,
		RobotID:    doc.RobotID,
		DocID:      doc.ID,
		QaTaskID:   qaTaskID,
		QaTaskType: qaTaskType,
		Uin:        pkg.Uin(ctx),
		Sid:        pkg.SID(ctx),
		AppBizID:   appBizID,
		Language:   i18n.GetUserLang(ctx),
	})
	if err != nil {
		log.ErrorContextf(ctx, "创建文档生成问答任务失败 err:%+v", err)
		return taskID, err
	}
	log.DebugContextf(ctx, "CreateDocToQATask taskID:%v", taskID)
	return taskID, nil
}

// UpdateDocCharSize 更新文档字符信息
func (d *dao) UpdateDocCharSize(ctx context.Context, doc model.Doc) error {
	doc.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, updateDocCharSize, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档字符数失败 sql:%s args:%+v err:%+v", updateDocCharSize, doc, err)
		return err
	}
	return nil
}

// UpdateDocStatus 更新文档状态信息
func (d *dao) UpdateDocStatus(ctx context.Context, doc *model.Doc) error {
	doc.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, updateDocStatus, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档状态失败 sql:%s args:%+v err:%+v", updateDocStatus, doc, err)
		return err
	}
	return nil
}

// UpdateDocIsDelete 更新文档删除状态
func (d *dao) UpdateDocIsDelete(ctx context.Context, doc *model.Doc) error {
	if doc == nil {
		return fmt.Errorf("doc is null")
	}
	doc.UpdateTime = time.Now()
	doc.IsDeleted = model.DocIsDeleted
	doc.Status = model.DocStatusDeleting
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, deleteDocByID, doc); err != nil {
		log.ErrorContextf(ctx, "删除文档失败 sql:%s doc:%+v err:%+v", deleteDocByID, doc, err)
		return err
	}
	return nil
}

// RecoverDocStatusWithInterveneAfterAuditFail 审核失败后恢复文档审核中的状态
func (d *dao) RecoverDocStatusWithInterveneAfterAuditFail(ctx context.Context, doc *model.Doc) error {
	doc.UpdateTime = time.Now()
	doc.IsDeleted = model.DocIsNotDeleted
	doc.Status = model.DocStatusAuditIng
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, deleteDocByID, doc); err != nil {
		log.ErrorContextf(ctx, "删除文档失败 sql:%s doc:%+v err:%+v", deleteDocByID, doc, err)
		return err
	}
	return nil
}

// UpdateDocStatusAndUpdateTime 更新文档状态状态,指定更新时间
func (d *dao) UpdateDocStatusAndUpdateTime(ctx context.Context, doc *model.Doc) error {
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, updateDocStatus, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档状态失败 sql:%s args:%+v err:%+v", updateDocStatus, doc, err)
		return err
	}
	return nil
}

// UpdateDocStatusAndCharSize 更新文档状态和字符大小
func (d *dao) UpdateDocStatusAndCharSize(ctx context.Context, doc *model.Doc) error {
	doc.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, updateDocStatusAndCharSize, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档状态失败 sql:%s args:%+v err:%+v", updateDocStatusAndCharSize, doc, err)
		return err
	}
	return nil
}

// UpdateDoc 更新doc
func (d *dao) UpdateDoc(ctx context.Context, staffID uint64, doc *model.Doc, isNeedPublish bool,
	attributeLabelReq *model.UpdateDocAttributeLabelReq) error {
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		execSQL := updateDoc
		if _, err := tx.NamedExecContext(ctx, execSQL, doc); err != nil {
			log.ErrorContextf(ctx, "更新文档失败 sql:%s args:%+v err:%+v", execSQL, doc, err)
			return err
		}
		if err := d.updateDocAttributeLabel(ctx, tx, doc.RobotID, doc.ID, attributeLabelReq); err != nil {
			return err
		}
		if !isNeedPublish {
			return nil
		}
		if err := d.sendDocModifyNotice(ctx, tx, staffID, doc, model.DocUpdatingNoticeContent,
			model.LevelInfo); err != nil {
			return err
		}
		if err := newDocModifyTask(ctx, doc.RobotID, model.DocModifyParams{
			CorpID:      doc.CorpID,
			StaffID:     staffID,
			RobotID:     doc.RobotID,
			DocID:       doc.ID,
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "更新文档失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocDisableState 更新文档停用启用状态
func (d *dao) UpdateDocDisableState(ctx context.Context, staffID uint64, doc *model.Doc, isDisable bool) error {
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	expireEnd := doc.ExpireEnd
	if isDisable {
		doc.AttributeFlag = doc.AttributeFlag | model.DocAttributeFlagDisable // 停用，第二位文档状态，更新成1
		expireEnd = time.Now()                                                // 停用后过期时间为当前时间
	} else {
		doc.AttributeFlag = doc.AttributeFlag &^ model.DocAttributeFlagDisable // 启用，第二位文档状态，更新成0
	}
	doc.StaffID = staffID
	log.InfoContextf(ctx, "UpdateDocDisableState doc.AttributeFlag:%v", doc.AttributeFlag)
	err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		updateDocFilter := &DocFilter{
			RobotId: doc.RobotID,
		}
		// 考虑后续迁移tdsql,文档更新非事务
		if _, err := GetDocDao().UpdateDoc(ctx, []string{DocTblColAttributeFlag, DocTblColStaffId}, updateDocFilter, doc); err != nil {
			log.ErrorContextf(ctx, "更新文档状态失败 args:%+v err:%+v", doc, err)
			return err
		}
		if err := d.sendDocModifyNotice(ctx, tx, staffID, doc, model.DocUpdatingNoticeContent,
			model.LevelInfo); err != nil {
			return err
		}
		if err := newDocModifyTask(ctx, doc.RobotID, model.DocModifyParams{
			CorpID:      doc.CorpID,
			StaffID:     staffID,
			RobotID:     doc.RobotID,
			DocID:       doc.ID,
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   expireEnd,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "更新文档状态失败 err:%+v", err)
		return err
	}
	return nil
}

// BatchUpdateDoc 批量应用链接，过期时间
func (d *dao) BatchUpdateDoc(ctx context.Context, staffID uint64, docs []*model.Doc,
	isNeedPublishMap map[uint64]int) error {
	if len(docs) == 0 {
		return nil
	}
	db := knowClient.DBClient(ctx, docTableName, docs[0].RobotID, []client.Option{}...)
	err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		execSQL := updateDoc
		for _, doc := range docs {
			if _, err := tx.NamedExecContext(ctx, execSQL, doc); err != nil {
				log.ErrorContextf(ctx, "更新文档失败 sql:%s args:%+v err:%+v", execSQL, doc, err)
				return err
			}

			_, ok := isNeedPublishMap[doc.ID]
			if !ok {
				continue
			}

			if err := d.sendDocModifyNotice(ctx, tx, staffID, doc, model.DocUpdatingNoticeContent,
				model.LevelInfo); err != nil {
				return err
			}
			if err := newDocModifyTask(ctx, doc.RobotID, model.DocModifyParams{
				CorpID:      doc.CorpID,
				StaffID:     staffID,
				RobotID:     doc.RobotID,
				DocID:       doc.ID,
				ExpireStart: doc.ExpireStart,
				ExpireEnd:   doc.ExpireEnd,
			}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "更新文档失败 err:%+v", err)
		return err
	}
	return nil
}

// GenerateQA 开始生成问答
func (d *dao) GenerateQA(ctx context.Context, staffID uint64, docs []*model.Doc, docQaTask *model.DocQATask, appBizID uint64) error {
	now := time.Now()
	if len(docs) == 0 {
		return nil
	}
	db := knowClient.DBClient(ctx, docQaTaskTableName, docs[0].RobotID, []client.Option{}...)
	docDb := knowClient.DBClient(ctx, docTableName, docs[0].RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, doc := range docs {
			// 每次循环创建新的任务对象,避免自增id污染
			qaTask := &model.DocQATask{
				CorpID:  docQaTask.CorpID,
				RobotID: docQaTask.RobotID,
			}
			lastInsertId, err := d.CreateDocQATask(ctx, tx, qaTask, doc)
			if err != nil {
				log.ErrorContextf(ctx, "GenerateQA|CreateDocQATask|qaTask|%v|doc|%v|err:%+v", qaTask, doc, err)
				return err
			}
			if lastInsertId == 0 {
				log.ErrorContextf(ctx, "GenerateQA|CreateDocQATask|fail|qaTask|%v|doc|%v", qaTask, docs)
				return errs.ErrCreateDocToQATaskFail
			}
			qaTask.ID = lastInsertId
			taskID, err := d.CreateDocToQATask(ctx, doc, qaTask, appBizID)
			if err != nil {
				log.ErrorContextf(ctx, "GenerateQA|CreateDocToQATask|err:%+v", err)
				return errs.ErrCreateDocToQATaskFail
			}
			qaTask.TaskID = taskID
			log.InfoContextf(ctx, "GenerateQA|CreateDocToQATask|taskID:%d", taskID)

			if err := d.UpdateDocQATaskID(ctx, tx, qaTask); err != nil {
				log.ErrorContextf(ctx, "GenerateQA|UpdateDocQATaskID|err:%+v", err)
				return err
			}

			// splitStrategy := ""
			// if _, ok := splitStrategys[doc.ID]; ok {
			//	splitStrategy = splitStrategys[doc.ID]
			// }
			// requestID := trace.SpanContextFromContext(ctx).TraceID().String()
			// taskID, err := d.SendDocParseCreateQA(ctx, doc, splitStrategy, requestID, robotBizID)
			// if err != nil {
			//	return err
			// }
			// newDocParse := model.DocParse{
			//	DocID:     doc.ID,
			//	CorpID:    doc.CorpID,
			//	RobotID:   doc.RobotID,
			//	StaffID:   doc.StaffID,
			//	RequestID: requestID,
			//	Type:      model.DocParseTaskTypeSplitQA,
			//	OpType:    model.DocParseOpTypeSplit,
			//	Status:    model.DocParseIng,
			//	TaskID:    taskID,
			// }
			// err = d.CreateDocParse(ctx, tx, newDocParse)
			// if err != nil {
			//	return err
			// }

			doc.IsCreatingQA = true
			doc.AddProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
			doc.UpdateTime = now
			querySQL := updateCreatingQATaskFlag
			if _, err := docDb.NamedExec(ctx, querySQL, doc); err != nil {
				log.ErrorContextf(ctx, "创建文档生成问答任务失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
				return err
			}
			operations := make([]model.Operation, 0)
			noticeOptions := []model.NoticeOption{
				model.WithPageID(model.NoticeQAPageID),
				model.WithLevel(model.LevelInfo),
				model.WithContent(i18n.Translate(ctx, i18nkey.KeyGeneratingQAWithName, doc.GetRealFileName())),
				model.WithForbidCloseFlag(),
			}
			notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID,
				noticeOptions...)
			if err := notice.SetOperation(operations); err != nil {
				log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
				return err
			}
			if err := d.createNotice(ctx, tx, notice); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "开始生成问答失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) sendExcelImportNotice(ctx context.Context, tx *sqlx.Tx, staffID uint64, doc *model.Doc) error {
	if !doc.IsExcel() {
		return nil
	}
	operations := make([]model.Operation, 0)
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeQAPageID),
		model.WithLevel(model.LevelInfo),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyQATemplateImportingWithParam, doc.FileName)),
		model.WithForbidCloseFlag(),
	}
	notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// CreateDocQADone 文档生成QA完成
func (d *dao) CreateDocQADone(ctx context.Context, staffID uint64, doc *model.Doc, qaCount int, success bool) error {
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		repeatStr := ""
		if doc.IsCreatedQA {
			repeatStr = i18n.Translate(ctx, i18nkey.KeyAgain)
		}
		pageID := model.NoticeQAPageID
		level := model.LevelError
		subject := i18n.Translate(ctx, i18nkey.KeyQAGenerateFailureWithParam, repeatStr)
		content := i18n.Translate(ctx, i18nkey.KeyQAGenerateFailureWithNameAndQA, doc.GetRealFileName(), repeatStr)
		operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
		if qaCount == 0 {
			subject = i18n.Translate(ctx, i18nkey.KeyFileContentTooLittleNoQAGenerate)
			content = i18n.Translate(ctx, i18nkey.KeyFileContentTooLittleNoQAGenerateWithName, doc.GetRealFileName())
			level = model.LevelWarning
		}
		if doc.IsBatchImport() && doc.IsExcel() {
			subject = i18n.Translate(ctx, i18nkey.KeyQATemplateImportFailure)
			content = i18n.Translate(ctx, i18nkey.KeyTemplateImportFailureWithName, doc.GetRealFileName())
		}
		if success {
			subject = i18n.Translate(ctx, i18nkey.KeyQAGenerateCompleteWithParam, repeatStr)
			content = i18n.Translate(ctx, i18nkey.KeyQAGenerateCompleteWithNameAndQA, doc.GetRealFileName(), repeatStr)
			if doc.IsBatchImport() && doc.IsExcel() {
				subject = i18n.Translate(ctx, i18nkey.KeyQATemplateImportSuccess)
				content = i18n.Translate(ctx, i18nkey.KeyQATemplateImportSuccessWithName, doc.GetRealFileName())
			} else {
				// 生成QA才需要去校验的按钮，批量导入的不需要
				operations = append(operations, model.Operation{Typ: model.OpTypeVerifyDocQA, Params: model.OpParams{
					CosPath:  doc.CosURL,
					DocBizID: strconv.FormatUint(doc.BusinessID, 10),
				}})
			}
			level = model.LevelSuccess
			doc.IsCreatedQA = true
		}
		querySQL := createDocQADone
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "文档生成QA完成失败 sql:%s doc:%+v err:%+v", querySQL, doc, err)
			return err
		}
		noticeOptions := []model.NoticeOption{
			model.WithGlobalFlag(),
			model.WithPageID(pageID),
			model.WithLevel(level),
			model.WithSubject(subject),
			model.WithContent(content),
		}
		notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID, noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := d.createNotice(ctx, tx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "文档生成问答完成失败 err:%+v", err)
		return err
	}
	return nil
}

// ReferDoc 答案中是否引用
func (d *dao) ReferDoc(ctx context.Context, doc *model.Doc) error {
	doc.UpdateTime = time.Now()
	querySQL := referDoc
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, doc); err != nil {
		log.ErrorContextf(ctx, "答案中是否引用失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
		return err
	}
	return nil
}

// UpdateCosInfo 更新cos信息
func (d *dao) UpdateCosInfo(ctx context.Context, doc *model.Doc) error {
	doc.UpdateTime = time.Now()
	querySQL := updateCosInfo
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, doc); err != nil {
		log.ErrorContextf(ctx, "更新cos信息失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
		return err
	}
	return nil
}

// GetDeletingDoc 获取删除中的文档
func (d *dao) GetDeletingDoc(ctx context.Context, corpID, robotID uint64) (map[uint64]*model.Doc, error) {
	querySQL := fmt.Sprintf(getDeletingDoc, docFields)
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, model.DocIsDeleted, model.DocStatusDeleting)
	list := make([]*model.Doc, 0)
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取删除中的文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	docs := make(map[uint64]*model.Doc, 0)
	for _, doc := range list {
		docs[doc.ID] = doc
	}
	return docs, nil
}

// GetCreatingIndexDoc 获取生成分片中的文档
func (d *dao) GetCreatingIndexDoc(ctx context.Context, corpID, robotID uint64) (map[uint64]*model.Doc, error) {
	querySQL := fmt.Sprintf(getCreatingIndexDoc, docFields)
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, model.DocCreatingIndex)
	list := make([]*model.Doc, 0)
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取生成分片中的文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	docs := make(map[uint64]*model.Doc, 0)
	for _, doc := range list {
		docs[doc.ID] = doc
	}
	return docs, nil
}

// DeleteDocSuccess 删除文档任务成功
func (d *dao) DeleteDocSuccess(ctx context.Context, doc *model.Doc) error {
	now := time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := docDeleteSuccess
		doc.Status = model.DocStatusDeleted
		if !doc.IsNextActionAdd() {
			doc.NextAction = model.DocNextActionDelete
			doc.Status = model.DocStatusWaitRelease
		}
		doc.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "删除文档成功失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "删除文档任务成功失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateCreatingIndexFlag 更新索引生成中标记
func (d *dao) UpdateCreatingIndexFlag(ctx context.Context, doc *model.Doc) error {
	querySQL := updateCreatingIndexFlag
	doc.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, doc); err != nil {
		log.ErrorContextf(ctx, "更新索引生成中标记失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
		return err
	}
	return nil
}

// UpdateDocNameAndStatus 更新文档名称,状态以及索引生成中标记
func (d *dao) UpdateDocNameAndStatus(ctx context.Context, doc *model.Doc) error {
	querySQL := updateDocNameAndStatus
	doc.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档名称,状态以及索引生成中标记失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
		return err
	}
	return nil
}

// UpdateCreatingQAFlag 更新问答生成中标记
func (d *dao) UpdateCreatingQAFlag(ctx context.Context, doc *model.Doc) error {
	querySQL := updateCreatingQAFlag
	doc.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, doc); err != nil {
		log.ErrorContextf(ctx, "更新问答生成中标记失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
		return err
	}
	return nil
}

// UpdateCreatingQATaskFlag 更新问答任务生成中标记
func (d *dao) UpdateCreatingQATaskFlag(ctx context.Context, doc *model.Doc) error {
	querySQL := updateCreatingQATaskFlag
	doc.UpdateTime = time.Now()
	if _, err := d.db.NamedExec(ctx, querySQL, doc); err != nil {
		log.ErrorContextf(ctx, "更新问答任务生成中标记 sql:%s args:%+v err:%+v", querySQL, doc, err)
		return err
	}
	return nil
}

// GetWaitReleaseDocCount 获取待发布的文档数量
func (d *dao) GetWaitReleaseDocCount(ctx context.Context, corpID, robotID uint64, fileName string, startTime,
	endTime time.Time, actions []uint32) (uint64, error) {
	condition := ""
	args := make([]any, 0, 6+len(actions))
	args = append(args, corpID, robotID, model.DocStatusWaitRelease)
	if fileName != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND ((file_name LIKE ? AND file_name_in_audit = '') OR file_name_in_audit LIKE ?)")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(fileName)))
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(fileName)))
	}
	if !startTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time >= ? ")
		args = append(args, startTime)
	}
	if !endTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time <= ? ")
		args = append(args, endTime)
	}
	if len(actions) > 0 {
		condition = fmt.Sprintf("%s AND next_action IN (%s)", condition, placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	querySQL := fmt.Sprintf(getWaitReleaseDocCount, condition)
	var total uint64
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取待发布的文档数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return total, err
	}
	return total, nil
}

// GetWaitReleaseDoc 获取待发布的文档
func (d *dao) GetWaitReleaseDoc(ctx context.Context, corpID, robotID uint64, fileName string, startTime,
	endTime time.Time, actions []uint32, page, pageSize uint32) ([]*model.Doc, error) {
	args := make([]any, 0, 6+len(actions))
	args = append(args, corpID, robotID, model.DocStatusWaitRelease)
	condition := ""
	if fileName != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND ((file_name LIKE ? AND file_name_in_audit = '') OR file_name_in_audit LIKE ?)")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(fileName)))
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(fileName)))
	}
	if !startTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time >= ? ")
		args = append(args, startTime)
	}
	if !endTime.IsZero() {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time <= ? ")
		args = append(args, endTime)
	}
	if len(actions) > 0 {
		condition = fmt.Sprintf("%s AND next_action IN (%s)", condition, placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getWaitReleaseDoc, docFields, condition)
	list := make([]*model.Doc, 0)
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取待发布的文档失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetRobotDocCharSize 获取机器人总文档大小
func (d *dao) GetRobotDocCharSize(ctx context.Context, robotID uint64, corpID uint64) (uint64, error) {
	var sum uint64
	args := []any{robotID, corpID, model.DocIsNotDeleted}
	exceededStatus := []any{
		model.DocStatusCharExceeded,
		model.DocStatusResuming,
		model.DocStatusParseImportFailCharExceeded,
		model.DocStatusAuditFailCharExceeded,
		model.DocStatusUpdateFailCharExceeded,
		model.DocStatusCreateIndexFailCharExceeded,
		model.DocStatusParseImportFailResuming,
		model.DocStatusAuditFailResuming,
		model.DocStatusUpdateFailResuming,
		model.DocStatusCreateIndexFailResuming,
		model.DocStatusExpiredCharExceeded,
		model.DocStatusExpiredResuming,
		model.DocStatusAppealFailedCharExceeded,
		model.DocStatusAppealFailedResuming,
	}
	args = append(args, exceededStatus...)
	querySQL := fmt.Sprintf(getDocCharSize, placeholder(len(exceededStatus)))
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &sum, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "检索机器人总文档字符大小失败 sql:%s args:%+v err:%+v", getDocCharSize, args, err)
		return 0, err
	}
	return sum, nil
}

// GetRobotDocExceedCharSize 获取机器人超量总文档大小
func (d *dao) GetRobotDocExceedCharSize(ctx context.Context, corpID uint64, robotIDs []uint64) (
	map[uint64]uint64, error) {
	robotDocExceedCharSizeMap := make(map[uint64]uint64)
	if len(robotIDs) == 0 {
		return robotDocExceedCharSizeMap, nil
	}
	type AppDocExceedCharSize struct {
		AppID             uint64 `db:"robot_id"`         // 应用ID
		DocExceedCharSize uint64 `db:"exceed_char_size"` // 超量字符
	}
	appDocExceedCharSize := make([]*AppDocExceedCharSize, 0)
	args := []any{corpID, model.DocIsNotDeleted}
	for _, robotID := range robotIDs {
		args = append(args, robotID)
	}
	exceededStatus := []any{
		model.DocStatusCharExceeded,
		model.DocStatusResuming,
		model.DocStatusParseImportFailCharExceeded,
		model.DocStatusAuditFailCharExceeded,
		model.DocStatusUpdateFailCharExceeded,
		model.DocStatusCreateIndexFailCharExceeded,
		model.DocStatusParseImportFailResuming,
		model.DocStatusAuditFailResuming,
		model.DocStatusUpdateFailResuming,
		model.DocStatusCreateIndexFailResuming,
		model.DocStatusExpiredCharExceeded,
		model.DocStatusExpiredResuming,
		model.DocStatusAppealFailedCharExceeded,
		model.DocStatusAppealFailedResuming,
	}
	args = append(args, exceededStatus...)
	querySQL := fmt.Sprintf(getAppDocExceedCharSize, placeholder(len(robotIDs)), placeholder(len(exceededStatus)))
	db := knowClient.DBClient(ctx, docTableName, robotIDs[0], []client.Option{}...)
	if err := db.QueryToStructs(ctx, &appDocExceedCharSize, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetRobotDocExceedCharSize failed|sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}

	for _, app := range appDocExceedCharSize {
		robotDocExceedCharSizeMap[app.AppID] = app.DocExceedCharSize
	}
	return robotDocExceedCharSizeMap, nil
}

func (d *dao) sendDocModifyNotice(ctx context.Context, tx *sqlx.Tx, staffID uint64, doc *model.Doc,
	content, level string) error {
	operations := make([]model.Operation, 0)
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(level),
		model.WithContent(i18n.Translate(ctx, content, doc.GetRealFileName())),
	}
	switch level {
	case model.LevelSuccess:
		noticeOptions = append(noticeOptions, model.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentUpdateSuccess)))
		noticeOptions = append(noticeOptions, model.WithGlobalFlag())
	case model.LevelError:
		noticeOptions = append(noticeOptions, model.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentUpdateFailure)))
		noticeOptions = append(noticeOptions, model.WithGlobalFlag())
	case model.LevelInfo:
		noticeOptions = append(noticeOptions, model.WithForbidCloseFlag())
	}
	notice := model.NewNotice(model.NoticeTypeDocModify, doc.ID, doc.CorpID, doc.RobotID, staffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// UpdateDocToQACreatingQa 更新文档是否生成问答状态
func (d *dao) UpdateDocToQACreatingQa(ctx context.Context, tx *sqlx.Tx, doc *model.Doc) error {
	if _, err := tx.NamedExecContext(ctx, createDocQADone, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档是否生成问答状态 sql:%s args:%+v err:%+v", createDocQADone, doc, err)
		return err
	}
	return nil
}

// ModifyDocSuccess 更新文档任务成功
func (d *dao) ModifyDocSuccess(ctx context.Context, doc *model.Doc, staffID uint64) error {
	now := time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := docModifySuccess
		doc.Status = model.DocStatusWaitRelease
		if !doc.IsNextActionAdd() {
			doc.NextAction = model.DocNextActionUpdate
		}
		doc.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "更新文档success失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
			return err
		}
		if err := d.sendDocModifyNotice(ctx, tx, staffID, doc, model.DocUpdateSuccessNoticeContent,
			model.LevelSuccess); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新文档任务success失败 err:%+v", err)
		return err
	}
	return nil
}

// ModifyDocFail 更新文档任务失败
func (d *dao) ModifyDocFail(ctx context.Context, doc *model.Doc, staffID uint64) error {
	now := time.Now()
	db := knowClient.DBClient(ctx, docTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := docModifyFail
		doc.Status = model.DocStatusUpdateFail
		doc.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "更新文档Fail失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
			return err
		}
		if err := d.sendDocModifyNotice(ctx, tx, staffID, doc, model.DocUpdateFailNoticeContent,
			model.LevelError); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新文档任务Fails失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocAttrRange 更新doc适用范围
func (d *dao) UpdateDocAttrRange(ctx context.Context, staffID uint64, docs []*model.Doc,
	attributeLabelReq *model.UpdateDocAttributeLabelReq) error {
	if len(docs) == 0 {
		return nil
	}
	db := knowClient.DBClient(ctx, docTableName, docs[0].RobotID, []client.Option{}...)
	err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		execSQL := updateDoc
		for _, doc := range docs {
			if _, err := tx.NamedExecContext(ctx, execSQL, doc); err != nil {
				log.ErrorContextf(ctx, "更新文档失败 sql:%s args:%+v err:%+v", execSQL, doc, err)
				return err
			}
			if err := d.updateDocAttributeLabel(ctx, tx, doc.RobotID, doc.ID, attributeLabelReq); err != nil {
				return err
			}

			if err := d.sendDocModifyNotice(ctx, tx, staffID, doc, model.DocUpdatingNoticeContent,
				model.LevelInfo); err != nil {
				return err
			}
			if err := newDocModifyTask(ctx, doc.RobotID, model.DocModifyParams{
				CorpID:      doc.CorpID,
				StaffID:     staffID,
				RobotID:     doc.RobotID,
				DocID:       doc.ID,
				ExpireStart: doc.ExpireStart,
				ExpireEnd:   doc.ExpireEnd,
			}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "更新文档失败 err:%+v", err)
		return err
	}
	return nil
}

// DeleteDocsCharSizeExceeded 删除超量失效超时文档
func (d *dao) DeleteDocsCharSizeExceeded(ctx context.Context, corpID uint64, robotID uint64,
	reserveTime time.Duration) error {
	req := &model.DocListReq{
		CorpID:   corpID,
		RobotID:  robotID,
		Page:     1,
		PageSize: 100,
	}
	_, docs, err := d.GetDocsCharSizeExceededAndExpire(ctx, req, reserveTime)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		return nil
	}
	return d.DeleteDocs(ctx, 0, 0, docs)
}

// GetResumeDocCount 获取恢复中的文档数量
func (d *dao) GetResumeDocCount(ctx context.Context, corpID, robotID uint64) (uint64, error) {
	sql, args, err := sqlx.In(getResumeDocCount, corpID, robotID, model.DocIsNotDeleted, model.DocResumingStatusList)
	if err != nil {
		log.ErrorContextf(ctx, "GetResumeDocCount sqlx.In err:%+v, corpID:%d, robotID:%d", err, corpID, robotID)
		return 0, err
	}
	var count uint64
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err = db.Get(ctx, &count, sql, args...); err != nil {
		log.ErrorContextf(ctx, "GetResumeDocCount fail, sql: %s args: %+v err: %v", sql, args, err)
		return 0, err
	}
	return count, nil
}

// CalcQACharSize 计算doc的charSize(含相似问)
func (d *dao) CalcQACharSize(ctx context.Context, doc *model.QA) uint64 {
	if doc == nil {
		return 0
	}
	var simCharSize = 0
	if len(doc.SimilarQuestions) > 0 {
		for _, q := range doc.SimilarQuestions {
			simCharSize += utf8.RuneCountInString(q)
		}
	}
	return uint64(utf8.RuneCountInString(doc.Question+doc.Answer)) + uint64(simCharSize)
}

// UpdateMsgDocRecord 更新t_msg_doc_record表
func (d *dao) UpdateMsgDocRecord(ctx context.Context, botBizID, docID uint64, docSummary string) error {
	err := d.gormDB.Model(realtime.TMsgDocRecord{}).
		Where("bot_biz_id = ? and doc_id = ?", botBizID, docID).
		Update("summary", docSummary).Error
	if err != nil {
		log.ErrorContextf(ctx, "UpdateMsgDocRecord Failed! err:%+v", err)
		return err
	}
	return nil
}

// GetDocCateStat 按分类统计
func (d *dao) GetDocCateStat(ctx context.Context, corpID, robotID uint64) (map[uint64]uint32,
	error) {
	sql := getDocCateStat
	args := []any{corpID, robotID, model.DocIsNotDeleted}
	var stat []*model.CateStat
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &stat, sql, args...); err != nil {
		log.ErrorContextf(ctx, "统计分类下问题数量失败, sql: %s, args: %+v, err:%+v", sql, args, err)
		return nil, err
	}
	m := make(map[uint64]uint32, len(stat))
	for _, v := range stat {
		m[v.CategoryID] = v.Total
	}
	return m, nil
}

// RenameDoc 文档重命名
func (d *dao) RenameDoc(ctx context.Context, staffID uint64, app *admin.GetAppInfoRsp, doc *model.Doc) error {
	if config.App().AuditSwitch {
		if err := d.createAudit(ctx, model.AuditSendParams{
			CorpID:   app.GetCorpId(),
			RobotID:  app.GetId(),
			StaffID:  staffID,
			Type:     model.AuditBizTypeDocName,
			RelateID: doc.ID,
			EnvSet:   getEnvSet(ctx),
		}); err != nil {
			log.ErrorContextf(ctx, "创建文档重命名送审任务失败 err:%+v", err)
			return err
		}
		doc.Status = model.DocStatusDocNameAuditing
		doc.StaffID = staffID
	} else {
		if err := d.CreateDocRenameToIndexTask(ctx, doc); err != nil {
			log.ErrorContextf(ctx, "新增向量重新入库任务失败 err:%+v", err)
			return err
		}
		doc.Status = model.DocStatusCreatingIndex
		doc.StaffID = staffID
	}
	db := knowClient.DBClient(ctx, docTableName, app.GetId(), []client.Option{}...)
	if _, err := db.NamedExec(ctx, renameDocSQL, doc); err != nil {
		log.ErrorContextf(ctx, "重命名失败, sql: %s, args: %+v, err: %+v", renameDocSQL, doc, err)
		return err
	}
	log.DebugContextf(ctx, "更新文档信息成功 sql: %s, args: %+v", renameDocSQL, doc)
	return nil
}

// CountDocWithTimeAndStatus 通过时间，获取指定状态的文档总数
// 新增文档：✅
// 修改文档：✅
// 删除文档：✅
// 修改后删除：✅
// 新增后删除：❌
func (d *dao) CountDocWithTimeAndStatus(ctx context.Context,
	corpID, robotID uint64,
	status []uint32,
	startTime time.Time,
) (uint64, error) {
	querySQL := `
		SELECT
			count(DISTINCT t_doc.id)
		FROM
		    t_doc
		WHERE
		    t_doc.corp_id = ? 
		AND t_doc.robot_id = ? 
		AND (
                (t_doc.create_time >= ? AND t_doc.is_deleted = 0) OR   -- 新增且未被删除
                (t_doc.update_time >= ? AND t_doc.create_time < ?)     -- 修改或删除
            )
            AND t_doc.status IN (%s)`
	querySQL = fmt.Sprintf(querySQL, placeholder(len(status)))

	timeStr := startTime.Format("2006-01-02 15:04:05.000")
	args := make([]any, 0, 4+len(status))
	args = append(args, corpID, robotID, timeStr, timeStr, timeStr)
	for _, st := range status {
		args = append(args, st)
	}

	var total uint64
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "CountDocWithTimeAndStatus fail, sql:%s, args:%+v, err:%+v",
			querySQL, args, err)
		return 0, err
	}
	return total, nil
}
