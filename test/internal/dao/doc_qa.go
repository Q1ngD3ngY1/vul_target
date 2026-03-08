package dao

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

const (
	qaFields = `
		id,business_id,robot_id,corp_id,staff_id,doc_id,origin_doc_id,segment_id,category_id,source,question,answer,
		custom_param,question_desc,release_status,is_audit_free,is_deleted,message,accept_status,next_action,
		similar_status,char_size,attr_range,create_time,update_time,expire_start,expire_end,attribute_flag
	`
	qaSimilarTipsQueryFields = ` 
		 COALESCE(
		(SELECT question FROM t_qa_similar_question WHERE t_doc_qa.id = t_qa_similar_question.related_qa_id 
		AND t_qa_similar_question.question LIKE ? 
		LIMIT 1),
		(SELECT question FROM t_qa_similar_question WHERE t_doc_qa.id = t_qa_similar_question.related_qa_id 
		ORDER BY t_qa_similar_question.create_time DESC,t_qa_similar_question.id DESC LIMIT 1),
		''
	  ) AS similar_question `
	qaSimilarTipsFields = ` 
		 IFNULL((select question from t_qa_similar_question where  t_qa_similar_question.robot_id = ? 
		 AND t_doc_qa.id = t_qa_similar_question.related_qa_id 
	     AND t_qa_similar_question.is_deleted = 1 
  		 ORDER BY t_qa_similar_question.create_time DESC,t_qa_similar_question.id DESC LIMIT 1 ), 
         '') as similar_question `

	createQA = `
		INSERT INTO
			t_doc_qa (%s)
		VALUES
			(null,:business_id,:robot_id,:corp_id,:staff_id,:doc_id,:origin_doc_id,:segment_id,:category_id,:source,
			:question,:answer,:custom_param,:question_desc,:release_status,:is_audit_free,:is_deleted,:message,:accept_status,
			:next_action,:similar_status,:char_size,:attr_range,:create_time,:update_time,:expire_start,:expire_end,:attribute_flag)
	`
	updateQA = `
		UPDATE
			t_doc_qa
		SET
		    question = :question,
		    answer = :answer,
		    custom_param = :custom_param,
			question_desc = :question_desc,
		    category_id = :category_id,
		    update_time = :update_time,
		    release_status = :release_status,
			is_audit_free = :is_audit_free,
		    similar_status = :similar_status,
		    doc_id = :doc_id,
		    next_action = :next_action,
		    char_size = :char_size,
		    attr_range = :attr_range,
		    expire_start = :expire_start,
		    expire_end = :expire_end,
			staff_id = :staff_id
		WHERE
		    id = :id
	`
	updateQAsExpire = `
		UPDATE
			t_doc_qa
		SET
		    update_time = :update_time,
			is_audit_free = :is_audit_free,
		    release_status = :release_status,
		    next_action = :next_action,
		    expire_end = :expire_end,
			staff_id = :staff_id
		WHERE
		    id = :id
	`
	updateQAsDoc = `
		UPDATE
			t_doc_qa
		SET
		    update_time = :update_time,
			is_audit_free = :is_audit_free,
		    release_status = :release_status,
		    next_action = :next_action,
		    doc_id = :doc_id,
			staff_id = :staff_id
		WHERE
		    id = :id
	`
	updateQAStatus = `
		UPDATE
			t_doc_qa
		SET
		    update_time = :update_time,
		    release_status = :release_status
		WHERE
		    id = :id
	`

	updateQASimilarsDocID = `UPDATE t_doc_qa_similar SET doc_id = ?, update_time = ? WHERE qa_id IN (%s)`

	updateQAAuditStatus = `
		UPDATE
			t_doc_qa
		SET
		    update_time = :update_time,
		    release_status = :release_status,
            is_audit_free = :is_audit_free
		WHERE
		    id = :id
	`
	publishQA = `
		UPDATE
			t_doc_qa
		SET
		    update_time = :update_time,
		    release_status = :release_status,
			is_audit_free = :is_audit_free,
		    message = :message,
		    next_action = :next_action
		WHERE
		    id = :id
	`
	deleteQA = `
		UPDATE
			t_doc_qa
		SET
		    is_deleted = :is_deleted,
		    update_time = :update_time,
		    release_status = :release_status,
			is_audit_free = :is_audit_free,
		    next_action = :next_action
		WHERE
		    id = :id
	`
	deleteDocToQA = `
		UPDATE
			t_doc_qa
		SET
		    update_time = :update_time,
		    doc_id = :doc_id
		WHERE
		    id = :id
	`

	verifyQA = `
		UPDATE
			t_doc_qa
		SET
		    accept_status = :accept_status,
		    category_id = :category_id ,
		    question = :question ,
			answer = :answer ,
			next_action = :next_action,
			similar_status = :similar_status,
			update_time = :update_time,
			char_size = :char_size,
			release_status = :release_status,
			staff_id = :staff_id
		WHERE
		    id = :id
	`
	getQACount = `
		SELECT
			t_doc_qa.accept_status,t_doc_qa.release_status,count(distinct t_doc_qa.id) as total
		FROM
		    t_doc_qa  %s
		WHERE
		     t_doc_qa.corp_id = ? AND t_doc_qa.robot_id = ? %s
		GROUP BY t_doc_qa.accept_status,t_doc_qa.release_status;
	`
	getQACountByDocID = `
		SELECT
			count(*) as total
		FROM
		    t_doc_qa
		WHERE
		     corp_id = ? AND doc_id = ?  AND robot_id = ?;
	`
	getQAUnconfirmedCount = `
		SELECT
			count(*)
		FROM
		    t_doc_qa
		WHERE
		    robot_id = ? AND accept_status = ?	AND is_deleted = ?
	`
	getQAList = `
		SELECT DISTINCT
			%s
		FROM
		    t_doc_qa %s
		WHERE
		     t_doc_qa.corp_id = ? AND t_doc_qa.robot_id = ? %s
		ORDER BY
		    t_doc_qa.update_time DESC,t_doc_qa.id DESC
		LIMIT ?,?
	` // Left JOIN t_qa_similar_question on t_doc_qa.id = t_qa_similar_question.related_qa_id
	getQAsByIDs = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		     corp_id = ? AND robot_id = ? AND accept_status = ? AND is_deleted = ? AND id IN(?)
		ORDER BY
		    id ASC
		LIMIT ?,?
	`
	getQAsByBizIDs = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		     corp_id = ? AND robot_id = ? AND accept_status = ? AND is_deleted = ? AND business_id IN(?)
		ORDER BY
		    id ASC
		LIMIT ?,?
	`
	getQADetail = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND robot_id = ? AND id IN (%s)
	`
	getQADetailByIds = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    id IN (%s)
	`
	getQADetailByBizIDs = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
			corp_id = ? AND robot_id = ? AND business_id IN (%s)
	`
	getQADetailByReleaseStatus = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND robot_id = ? AND release_status = ? AND id IN (%s)
	`
	getQAListToSimilar = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    similar_status = ? AND is_deleted = ? AND update_time >= ? AND update_time <= ?
		LIMIT
			?`
	lockOneQa = `
		UPDATE
			t_doc_qa
		SET
		    similar_status = :similar_status
		WHERE
		    id = :id
	`
	getDocQANum = `
		SELECT
			doc_id,is_deleted,count(*) as total
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND robot_id = ? AND doc_id IN (%s)
		GROUP BY doc_id,is_deleted
	`
	getReleaseQACount = `
		SELECT
			count(*)
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ?
		  	AND robot_id = ?
		    AND accept_status != ?
		    AND release_status = ?
			AND !(next_action = ? AND is_deleted = ?)
		    AND !(next_action = ? AND accept_status = ?) %s
	`
	getReleaseQAList = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ?
			AND robot_id = ?
			AND accept_status != ?
			AND release_status = ?
			AND !(next_action = ? AND is_deleted = ?)
		    AND !(next_action = ? AND accept_status = ?) %s
		LIMIT ?,?
	`
	updateQALastAction = `
		UPDATE
			t_doc_qa
		SET
		    next_action = ?, update_time = ?
		WHERE
		    id IN (%s)
	`
	getQAByID = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    id = ?
	`
	getQAByBizID = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    business_id = ?
	`
	cmdGetQAList = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		     is_deleted = ? %s
		LIMIT
			?,?
	`
	cmdGetQAListCount = `
		SELECT
			COUNT(*)
		FROM
		    t_doc_qa
		WHERE
		     is_deleted = ? %s
	`
	getSegmentQA = `
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND doc_id = ? AND segment_id = ?
		LIMIT ?
	`
	getQAIDsByOriginDocID = `
		SELECT
			id
		FROM
		    t_doc_qa
		WHERE
		    robot_id = ? AND origin_doc_id = ?
	`
	updateQAReleaseStatus = `
		UPDATE
			t_doc_qa
		SET
		    update_time = ?,
		    release_status = ?
		WHERE
		    id = ?
	`
	batchUpdateQAReleaseStatus = `
		UPDATE
			t_doc_qa
		SET
		    update_time = ?,
		    release_status = ?
		WHERE
		    id IN (%s)
    `

	selectNeedModifyQA = `
		SELECT
		    %s
        FROM
			t_doc_qa
		WHERE
		    robot_id = ?
		AND corp_id = ?
        AND is_deleted = ?
		AND category_id = 0
	`
	getQaCharSize = `
		SELECT
			IFNULL(SUM(char_size), 0)
		FROM
		    t_doc_qa
		WHERE
		    robot_id = ?
		AND corp_id = ?
		AND is_deleted = ?
		AND accept_status = ?
		AND release_status NOT IN (%s)
	`
	updateAfterAuditQA = `
		UPDATE
			t_doc_qa
		SET
		    release_status = :release_status, message = :message, update_time = :update_time
		WHERE
		    id = :id AND release_status != 8
	`
	updateAuditQA = `
		UPDATE
			t_doc_qa
		SET
		    release_status = :release_status, message = :message, update_time = :update_time
		WHERE
		    id = :id
	`
	getQAUnconfirmedNum = `
		SELECT
			count(*)
		FROM
		    t_doc_qa
		WHERE
		   corp_id = ? AND  robot_id = ? AND is_deleted = ? AND accept_status = ?
	`
	getQAUnconfirmedNumByTime = `
		SELECT
			count(*)
		FROM
		    t_doc_qa
		WHERE
		   corp_id = ? AND  robot_id = ? AND is_deleted = ? AND accept_status = ? AND create_time >= ?
	`
	getQAChunk = `
		SELECT ` + qaFields + ` FROM t_doc_qa
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND id > ?
		ORDER BY id ASC LIMIT ?
	`
	getQAChunkCount = `
		SELECT COUNT(*) FROM t_doc_qa
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND accept_status = ?
	`
	getSimilarChunkCount = `
		SELECT COUNT(*) FROM t_doc_qa
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ?
	`

	getAppQaExceedCharSize = `
		SELECT
			IFNULL(SUM(char_size), 0) as exceed_char_size, robot_id
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND is_deleted = ? AND accept_status = ? AND robot_id IN (%s) AND release_status IN (%s)
		Group By
		    robot_id
	`

	getDocQAJoinSql = `LEFT JOIN t_qa_attribute_label ON t_doc_qa.id = t_qa_attribute_label.qa_id AND t_doc_qa.robot_id = t_qa_attribute_label.robot_id
  LEFT JOIN t_attribute_label ON t_qa_attribute_label.label_id = t_attribute_label.id
  LEFT JOIN t_attribute ON t_qa_attribute_label.attr_id = t_attribute.id`

	getDocQAUntaggedJoinSql = `LEFT JOIN t_qa_attribute_label as qa_attribute on t_doc_qa.id = qa_attribute.qa_id and  qa_attribute.robot_id = t_doc_qa.robot_id`
)

// CreateQA 创建QA(支持相似问)
func (d *dao) CreateQA(ctx context.Context, qa *model.DocQA, businessSource uint32, businessID uint64,
	attributeLabelReq *model.UpdateQAAttributeLabelReq, simQuestions []string) error {
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		qa.UpdateTime = now
		qa.CreateTime = now
		qa.NextAction = model.NextActionAdd
		qa.ReleaseStatus = model.QAReleaseStatusAuditing
		if !config.AuditSwitch() {
			qa.ReleaseStatus = model.QAReleaseStatusLearning
		}
		querySQL := fmt.Sprintf(createQA, qaFields)
		res, err := tx.NamedExecContext(ctx, querySQL, qa)
		if err != nil {
			log.ErrorContextf(ctx, "创建QA失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
			return err
		}
		id, _ := res.LastInsertId()
		qa.ID = uint64(id)
		if err := d.updateQABusinessSource(ctx, tx, qa, businessSource, businessID); err != nil {
			log.ErrorContextf(ctx, "更新QA业务来源信息失败 err:%+v", err)
			return err
		}
		if err := d.updateQAAttributeLabel(ctx, tx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
			return err
		}
		// 处理相似问
		var syncSimilarQuestionsIDs []uint64
		if len(simQuestions) > 0 {
			sqs := d.NewSimilarQuestions(ctx, qa, simQuestions)
			if err = d.AddSimilarQuestions(ctx, tx, sqs); err != nil {
				log.ErrorContextf(ctx, "添加相似问失败 err:%+v", err)
				return err
			}
			if !config.AuditSwitch() {
				if syncSimilarQuestionsIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs); err != nil {
					log.ErrorContextf(ctx, "创建QA失败，添加相似问同步任务失败 err:%+v", err)
					return err
				}
			}
		}
		if config.AuditSwitch() {
			err = d.CreateQaAudit(ctx, tx, qa)
			if err != nil {
				return errs.ErrCreateAuditFail
			}
		} else {
			syncID, err := d.addQASync(ctx, tx, qa)
			if err != nil {
				log.ErrorContextf(ctx, "创建QA失败， addQASync qa:%+v err:%+v", qa, err)
				return err
			}
			d.vector.Push(ctx, syncID)
			d.vector.BatchPush(ctx, syncSimilarQuestionsIDs)
		}

		return d.UpdateAppUsedCharSize(ctx, tx, int64(qa.CharSize), qa.RobotID)
	})
	if err != nil {
		return err
	}

	return nil
}

// updateQABusinessSource TODO
func (d *dao) updateQABusinessSource(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA, businessSource uint32,
	businessID uint64) error {
	switch businessSource {
	case model.QABusinessSourceUnsatisfiedReply:
		return d.updateUnsatisfiedReplyStatus(ctx, tx, qa.CorpID, qa.RobotID, []uint64{businessID},
			model.UnsatisfiedReplyStatusWait, model.UnsatisfiedReplyStatusPass)
	}
	return nil
}

func (d *dao) updateSegmentStatus(ctx context.Context, tx *sqlx.Tx, seg *model.DocSegmentExtend, now time.Time) error {
	sql := updateSegmentDone
	seg.Status = model.SegmentStatusDone
	seg.UpdateTime = now
	if _, err := tx.NamedExecContext(ctx, sql, seg); err != nil {
		log.ErrorContextf(ctx, "更新 segment done 失败, sql: %s, seg: %+v, err: %+v", sql, seg, err)
		return err
	}
	return nil
}

// fillQAAttributeLabelsFromDoc TODO
func (d *dao) fillQAAttributeLabelsFromDoc(ctx context.Context, doc *model.Doc) (*model.UpdateQAAttributeLabelReq,
	error) {
	attrLabel, err := d.GetDocAttributeLabel(ctx, doc.RobotID, []uint64{doc.ID})
	if err != nil {
		return nil, err
	}
	req := &model.UpdateQAAttributeLabelReq{
		IsNeedChange:    true,
		AttributeLabels: make([]*model.QAAttributeLabel, 0, len(attrLabel)),
	}
	for _, v := range attrLabel {
		req.AttributeLabels = append(req.AttributeLabels, &model.QAAttributeLabel{
			Source:  v.Source,
			AttrID:  v.AttrID,
			LabelID: v.LabelID,
		})
	}
	return req, nil
}

// createQA 创建问答,这里主要是excel生成的问答,保存文档的时候对字符数已经做过更新,故不在此接口中做机器人字符数的更新
func (d *dao) createQA(
	ctx context.Context, tx *sqlx.Tx,
	seg *model.DocSegmentExtend, doc *model.Doc, qas []*model.QA, tree *model.CateNode, isNeedAudit bool,
) ([]uint64, error) {
	sql := fmt.Sprintf(createQA, qaFields)
	var syncIDs []uint64
	var err error
	var attributeLabelReq *model.UpdateQAAttributeLabelReq
	if doc.IsExcel() {
		// 文档未设置attr_range时，使用默认值
		doc.AttrRange = utils.When(doc.AttrRange == model.AttrRangeDefault, model.AttrRangeAll, doc.AttrRange)
		attributeLabelReq, err = d.fillQAAttributeLabelsFromDoc(ctx, doc)
		if err != nil {
			return nil, err
		}
	}
	for _, v := range qas {
		qa := model.NewDocQA(doc, seg, v, uint64(tree.Find(v.Path)), isNeedAudit)
		qa.BusinessID = d.GenerateSeqID()
		videoCharSize, err := d.GetVideoURLsCharSize(ctx, qa.Answer)
		if err != nil {
			log.WarnContextf(ctx, "createQA|GetVideoCharSize err, sql: %s, qa: %+v, err: %+v", sql, qa, err)
			return nil, err
		}
		qa.CharSize = d.CalcQACharSize(ctx, v) + uint64(videoCharSize)
		r, err := tx.NamedExecContext(ctx, sql, qa)
		if err != nil {
			log.ErrorContextf(ctx, "批量创建问答失败, sql: %s, qa: %+v, err: %+v", sql, qa, err)
			return nil, err
		}
		id, _ := r.LastInsertId()
		qa.ID = uint64(id)
		if doc.IsExcel() {
			if err := d.updateQAAttributeLabel(ctx, tx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
				return nil, err
			}
		}
		// 处理相似问
		if len(v.SimilarQuestions) > 0 {
			sqs := d.NewSimilarQuestions(ctx, qa, v.SimilarQuestions)
			if err = d.AddSimilarQuestions(ctx, tx, sqs); err != nil {
				log.ErrorContextf(ctx, "添加相似问失败 err:%+v", err)
				return nil, err
			}
		}
		// if isNeedAudit { // 批量导入问答需要审核，不过审核的代码挪到了excel_to_qa.go文件中
		// 	err = d.CreateQaAudit(ctx, tx, qa)
		// 	if err != nil {
		// 		return nil, errs.ErrCreateAuditFail
		// 	}
		// }
	}
	return syncIDs, nil
}

// BatchCreateQA 批量创建QA
func (d *dao) BatchCreateQA(
	ctx context.Context,
	seg *model.DocSegmentExtend,
	doc *model.Doc, qas []*model.QA, tree *model.CateNode, isNeedAudit bool,
) error {
	// 文档生成问答时不需要审核，在后续采纳问答时走审核;
	// 批量导入问答时需要审核
	now := time.Now()
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := d.updateSegmentStatus(ctx, tx, seg, now); err != nil {
			return err
		}
		if err := d.createCates(ctx, tx, model.QACate, seg.CorpID, seg.RobotID, tree); err != nil {
			return err
		}
		_, err := d.createQA(ctx, tx, seg, doc, qas, tree, isNeedAudit)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// UpdateQA 更新问答对
func (d *dao) UpdateQA(ctx context.Context, qa *model.DocQA, sqm *model.SimilarQuestionModifyInfo, isNeedPublish,
	isNeedAudit bool, diffCharSize int64, attributeLabelReq *model.UpdateQAAttributeLabelReq) error {
	now := time.Now()
	var syncID uint64
	similarSyncIDs := make([]uint64, 0)
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		sql := updateQA
		qa.UpdateTime = now
		if sqm != nil {
			for i := range sqm.UpdateQuestions {
				sqm.UpdateQuestions[i].ReleaseStatus = qa.ReleaseStatus
				sqm.UpdateQuestions[i].IsAuditFree = qa.IsAuditFree
			}
			for i := range sqm.AddQuestions {
				sqm.AddQuestions[i].ReleaseStatus = qa.ReleaseStatus
				sqm.AddQuestions[i].IsAuditFree = qa.IsAuditFree
			}
		}

		if _, err := tx.NamedExecContext(ctx, sql, qa); err != nil {
			log.ErrorContextf(ctx, "更新问答对失败 sql:%s args:%+v err:%+v", sql, qa, err)
			return err
		}
		if err := d.ModifySimilarQuestions(ctx, tx, qa, sqm); err != nil {
			return err
		}
		if attributeLabelReq.IsNeedChange {
			// 只在属性标签发生变化时，才需要更新属性标签
			if err := d.updateQAAttributeLabel(ctx, tx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
				return err
			}
		}
		if isNeedAudit {
			if err := d.CreateQaAudit(ctx, tx, qa); err != nil {
				return errs.ErrCreateAuditFail
			}
		}
		if err := d.UpdateAppUsedCharSize(ctx, tx, diffCharSize, qa.RobotID); err != nil {
			return err
		}
		if !isNeedPublish {
			return nil
		}
		if err := d.deleteQASimilar(ctx, tx, qa); err != nil {
			return err
		}
		if !isNeedAudit { // 如果需要审核，就在审核之后再sync; 如果不需要审核就直接sync
			// 用于同步主问
			id, err := d.addQASync(ctx, tx, qa)
			if err != nil {
				return err
			}
			syncID = id

			// 用于同步相似问
			if sqm != nil {
				sqs := make([]*model.SimilarQuestion, 0)
				sqs = append(sqs, sqm.AddQuestions...)
				sqs = append(sqs, sqm.DeleteQuestions...)
				sqs = append(sqs, sqm.UpdateQuestions...)
				similarSyncIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs)
				if err != nil {
					return err
				}
			}
		} else {
			// 需要审核的场景下，先sync删除的相似问，因为审核回调是异步逻辑，没法知道本次删除了哪些相似问
			if sqm != nil {
				sqs := make([]*model.SimilarQuestion, 0)
				sqs = append(sqs, sqm.DeleteQuestions...)
				var err error
				similarSyncIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !isNeedAudit { // 如果需要审核，就在审核之后再sync; 如果不需要审核就直接sync
		d.vector.Push(ctx, syncID)
	}
	d.vector.BatchPush(ctx, similarSyncIDs)
	return nil
}

// UpdateQADisableState 更新问答对停用启用状态
func (d *dao) UpdateQADisableState(ctx context.Context, qa *model.DocQA, sqm *model.SimilarQuestionModifyInfo,
	isDisable bool) error {
	now := time.Now()
	var syncID uint64
	similarSyncIDs := make([]uint64, 0)
	if isDisable {
		qa.AttributeFlag = qa.AttributeFlag | model.QAAttributeFlagDisable // 停用，第一位QA状态，更新成1
	} else {
		qa.AttributeFlag = qa.AttributeFlag &^ model.QAAttributeFlagDisable // 启用，第一位QA状态，更新成0
	}
	log.InfoContextf(ctx, "UpdateQADisableState qa.AttributeFlag:%v", qa.AttributeFlag)
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		qa.UpdateTime = now
		updateQAFilter := &DocQaFilter{
			RobotId: qa.RobotID,
		}
		// 考虑后续迁移tdsql,问答更新非事务
		if err := GetDocQaDao().UpdateDocQas(ctx, []string{DocQaTblColAttributeFlag, DocQaTblColStaffId,
			DocQaTblColReleaseStatus, DocQaTblColNextAction}, updateQAFilter, qa); err != nil {
			log.ErrorContextf(ctx, "更新问答对失败 args:%+v err:%+v", qa, err)
			return err
		}
		// 用于同步主问
		id, err := d.addQASync(ctx, tx, qa)
		if err != nil {
			return err
		}
		syncID = id
		// 用于同步相似问
		if sqm != nil {
			sqs := make([]*model.SimilarQuestion, 0)
			sqs = append(sqs, sqm.AddQuestions...)
			sqs = append(sqs, sqm.DeleteQuestions...)
			sqs = append(sqs, sqm.UpdateQuestions...)
			similarSyncIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "UpdateQADisableState err:%+v", err)
		return err
	}
	d.vector.Push(ctx, syncID)
	d.vector.BatchPush(ctx, similarSyncIDs)
	return nil
}

// UpdateQAStatusAndUpdateTime 更新问答对状态和更新时间
func (d *dao) UpdateQAStatusAndUpdateTime(ctx context.Context, qa *model.DocQA) error {
	_, err := d.db.NamedExec(ctx, updateQAStatus, qa)
	if err != nil {
		return err
	}
	return nil
}

// UpdateQAAuditStatusAndUpdateTimeTx 事务的方式更新问答对审核状态和更新时间
func (d *dao) UpdateQAAuditStatusAndUpdateTimeTx(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA) error {
	if tx == nil || qa == nil {
		return nil
	}
	_, err := tx.NamedExecContext(ctx, updateQAAuditStatus, qa)
	if err != nil {
		return err
	}
	return nil
}

// UpdateQASimilarsDocID 批量更新t_doc_qa_similar的DocID
func (d *dao) UpdateQASimilarsDocID(ctx context.Context, qaIDs []uint64, docID uint64) error {
	args := make([]any, 0)
	args = append(args, docID, time.Now())
	for _, qaID := range qaIDs {
		args = append(args, qaID)
	}
	sql := fmt.Sprintf(updateQASimilarsDocID, placeholder(len(qaIDs)))
	_, err := d.db.Exec(ctx, sql, args...)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateQASimilarsDocID failed, %v", err)
		return err
	}
	return nil
}

// PublishQA 发布问答对
func (d *dao) PublishQA(ctx context.Context, qa *model.DocQA, releaseQA *model.ReleaseQA) error {
	now := time.Now()
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		qa.UpdateTime = now
		querySQL := publishQA
		if _, err := tx.NamedExecContext(ctx, querySQL, qa); err != nil {
			log.ErrorContextf(ctx, "发布问答对失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
			return err
		}
		releaseQA.UpdateTime = now
		querySQL = publishReleaseQA
		if _, err := tx.NamedExecContext(ctx, querySQL, releaseQA); err != nil {
			log.ErrorContextf(ctx, "发布问答对失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "发布问答对失败 err:%+v", err)
		return err
	}
	return nil
}

// DeleteQA 删除一条问答(支持相似问联动)
func (d *dao) DeleteQA(ctx context.Context, qa *model.DocQA) error {
	var syncID uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	var deleteCharSize int64
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if qa.AcceptStatus == model.AcceptYes && qa.IsDeleted != model.QAIsDeleted {
			deleteCharSize = int64(qa.CharSize)
		}
		if err := d.UpdateAppUsedCharSize(ctx, tx, -deleteCharSize, qa.RobotID); err != nil {
			return err
		}
		qa.IsDeleted = model.QAIsDeleted
		qa.UpdateTime = time.Now()
		if !qa.IsNextActionAdd() {
			qa.NextAction = model.NextActionDelete
			qa.ReleaseStatus = model.QAReleaseStatusInit
			qa.IsAuditFree = model.QAIsAuditNotFree
		}
		querySQL := deleteQA
		if _, err := tx.NamedExecContext(ctx, querySQL, qa); err != nil {
			log.ErrorContextf(ctx, "删除问答对失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
			return err
		}
		if err := d.deleteQAAttributeLabel(ctx, tx, qa.RobotID, qa.ID); err != nil {
			log.ErrorContextf(ctx, "删除问答对关联的标签标准词失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
			return err
		}
		id, err := d.addQASync(ctx, tx, qa)
		if err != nil {
			return err
		}
		syncID = id
		// 相似问处理
		sqs, err := d.GetSimilarQuestionsByQA(ctx, qa)
		if err != nil {
			// 伽利略error日志告警
			log.ErrorContextf(ctx, "DeleteQA qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
			// 柔性放过
		}
		if len(sqs) > 0 {
			if err = d.DeleteSimilarQuestions(ctx, tx, sqs); err != nil {
				log.ErrorContextf(ctx, "删除相似问失败 err:%+v", err)
				return err
			}
			if syncSimilarQuestionsIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs); err != nil {
				log.ErrorContextf(ctx, "添加相似问同步任务失败(delete) err:%+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	d.vector.Push(ctx, syncID)
	d.vector.BatchPush(ctx, syncSimilarQuestionsIDs)

	return nil
}

// DeleteDocToQA 删除文档只取消文档下问答对文档的引用
func (d *dao) DeleteDocToQA(ctx context.Context, qa *model.DocQA) error {
	var syncID uint64
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		qa.UpdateTime = time.Now()
		qa.DocID = 0
		querySQL := deleteDocToQA
		if _, err := tx.NamedExecContext(ctx, querySQL, qa); err != nil {
			log.ErrorContextf(ctx, "删除文档只取消文档下问答对文档的引用 sql:%s args:%+v err:%+v", querySQL, qa, err)
			return err
		}
		id, err := d.addQASync(ctx, tx, qa)
		if err != nil {
			return err
		}
		syncID = id
		return nil
	}); err != nil {
		return err
	}
	d.vector.Push(ctx, syncID)
	return nil
}

// DeleteQAs 删除QA(支持相似问联动)
func (d *dao) DeleteQAs(ctx context.Context, corpID, robotID, staffID uint64, qas []*model.DocQA) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.deleteQAs(ctx, tx, corpID, robotID, staffID, qas)
	}); err != nil {
		log.ErrorContextf(ctx, "删除问答对失败 err:%+v", err)
		return err
	}
	return nil
}

// deleteQAs 删除QA问(支持相似问联动), 无sync操作,新建deleteTask
func (d *dao) deleteQAs(ctx context.Context, tx *sqlx.Tx, corpID, robotID, staffID uint64, qas []*model.DocQA) error {
	length := len(qas)
	pageSize := 100
	pages := int(math.Ceil(float64(length) / float64(pageSize)))
	now := time.Now()
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpQAs := qas[start:end]
		qaIDs := make([]uint64, 0, len(tmpQAs))
		var charSize int64
		for _, qa := range tmpQAs {
			qaIDs = append(qaIDs, qa.ID)
			if qa.AcceptStatus == model.AcceptYes && qa.IsDeleted != model.QAIsDeleted {
				charSize += int64(qa.CharSize)
			}
			qa.IsDeleted = model.QAIsDeleted
			qa.UpdateTime = now
			if !qa.IsNextActionAdd() {
				qa.NextAction = model.NextActionDelete
				qa.ReleaseStatus = model.QAReleaseStatusInit
				qa.IsAuditFree = model.QAIsAuditNotFree
			}
			querySQL := deleteQA
			if _, err := tx.NamedExecContext(ctx, querySQL, qa); err != nil {
				log.ErrorContextf(ctx, "删除问答对失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
				return err
			}
			if err := d.deleteQAAttributeLabel(ctx, tx, qa.RobotID, qa.ID); err != nil {
				log.ErrorContextf(ctx, "删除问答对关联的标签标准词失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
				return err
			}
			if err := d.DeleteSimilarQuestionsByQA(ctx, tx, qa); err != nil {
				log.ErrorContextf(ctx, "删除相似问失败 err:%+v", err)
				return err
			}
		}
		if err := d.UpdateAppUsedCharSize(ctx, tx, -charSize, robotID); err != nil {
			return err
		}
		// 这里没有sync同步操作, task处理内有sync
		if err := newQADeleteTask(ctx, robotID, model.QADeleteParams{
			CorpID:  corpID,
			StaffID: staffID,
			RobotID: robotID,
			QAIDs:   qaIDs,
		}); err != nil {
			log.ErrorContextf(ctx, "创建删除问答对任务失败 err:%+v", err)
			return err
		}
	}
	return nil
}

// DeleteQASimilar 删除相似问答对
func (d *dao) DeleteQASimilar(ctx context.Context, qa *model.DocQA) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.deleteQASimilar(ctx, tx, qa)
	}); err != nil {
		log.ErrorContextf(ctx, "删除相似问答对失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) deleteQASimilar(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA) error {
	now := time.Now()
	querySQL := fmt.Sprintf(SyncDelQaSimilar, "?", "?")
	args := make([]any, 0, 5)
	args = append(args, model.QaSimilarInValid, now, qa.ID, qa.ID, model.QaSimilarStatusInit)
	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "删除相似问答对失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// VerifyQA 校验QA
func (d *dao) VerifyQA(ctx context.Context, qas []*model.DocQA, robotID, charSize uint64) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.verifyQAs(ctx, tx, qas, robotID, charSize)
	}); err != nil {
		log.ErrorContextf(ctx, "校验问答对失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) verifyQAs(ctx context.Context, tx *sqlx.Tx, qas []*model.DocQA, robotID, charSize uint64) error {
	length := len(qas)
	pageSize := 100
	pages := int(math.Ceil(float64(length) / float64(pageSize)))
	now := time.Now()
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpQAs := qas[start:end]
		for _, qa := range tmpQAs {
			qa.UpdateTime = now
			querySQL := verifyQA
			qa.SimilarStatus = model.SimilarStatusInit
			if !qa.IsNextActionAdd() {
				qa.NextAction = model.NextActionUpdate
			}
			if qa.IsAccepted() {
				qa.ReleaseStatus = model.QAReleaseStatusAuditing
				if !config.AuditSwitch() {
					qa.ReleaseStatus = model.QAReleaseStatusLearning
				}
			}
			if _, err := tx.NamedExecContext(ctx, querySQL, qa); err != nil {
				log.ErrorContextf(ctx, "校验QA失败 sql:%s args:%+v err:%+v", querySQL, qa, err)
				return err
			}
			if qa.IsAccepted() && config.AuditSwitch() { // 采纳的问答才需要送审，送审的问答在审核通过后，才会写入向量
				err := d.CreateQaAudit(ctx, tx, qa)
				if err != nil {
					log.ErrorContextf(ctx, "校验QA失败 CreateQaAudit qa:%+v err:%+v", qa, err)
					return err
				}
			} else {
				id, err := d.addQASync(ctx, tx, qa)
				if err != nil {
					log.ErrorContextf(ctx, "校验QA失败 addQASync qa:%+v err:%+v", qa, err)
					return err
				}
				syncID := id
				d.vector.Push(ctx, syncID)
			}
		}
	}
	return d.UpdateAppUsedCharSize(ctx, tx, int64(charSize), robotID)
}

// GetQAList 获取问答对列表
func (d *dao) GetQAList(ctx context.Context, req *model.QAListReq) ([]*model.DocQA, error) {
	condition := ""
	var args []any
	// if req.Query != "" { // 相似问tips子查询
	//	queryArg := fmt.Sprintf("%%%s%%", special.Replace(req.Query))
	//	args = append(args, queryArg)
	// }
	args = append(args, req.RobotID, req.CorpID, req.RobotID)
	if req.IsDeleted != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.is_deleted = ?")
		args = append(args, req.IsDeleted)
	}
	if req.Source != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.source = ?")
		args = append(args, req.Source)
	}
	joinSql := ""
	if req.Query != "" {
		queryArg := fmt.Sprintf("%%%s%%", special.Replace(req.Query))
		if req.QueryType == model.DocQueryTypeFileName && req.Query != model.DocQuerySystemTypeUntagged {
			// condition = fmt.Sprintf("%s%s", condition,
			//	" AND (t_doc_qa.question LIKE ? OR t_qa_similar_question.question like ?)")
			condition = fmt.Sprintf("%s%s", condition,
				" AND (t_doc_qa.question LIKE ? )")
			args = append(args, queryArg)
		}
		if req.QueryType == model.DocQueryTypeAttribute && req.Query != model.DocQuerySystemTypeUntagged {
			joinSql = getDocQAJoinSql
			condition = fmt.Sprintf("%s%s", condition,
				" AND (t_attribute_label.name LIKE ? OR t_attribute_label.similar_label LIKE ? OR t_attribute.name LIKE ?)")
			args = append(args, queryArg, queryArg, queryArg)
		}
		if req.Query == model.DocQuerySystemTypeUntagged {
			joinSql = getDocQAUntaggedJoinSql
			condition = fmt.Sprintf("%s%s", condition, " AND qa_attribute.id IS NULL")
		}
	}
	if req.QueryAnswer != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.answer LIKE ?")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(req.QueryAnswer)))
	}
	if len(req.CateIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.category_id IN (%s)", condition, placeholder(len(req.CateIDs)))
		for _, cID := range req.CateIDs {
			args = append(args, cID)
		}
	}
	if len(req.QABizIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.business_id IN (%s)", condition, placeholder(len(req.QABizIDs)))
		for _, qaBizID := range req.QABizIDs {
			args = append(args, qaBizID)
		}
	}
	if len(req.AcceptStatus) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.accept_status IN (%s)", condition, placeholder(len(req.AcceptStatus)))
		for _, acceptStatus := range req.AcceptStatus {
			args = append(args, acceptStatus)
		}
	}
	if req.ValidityStatus != 0 || len(req.ReleaseStatus) != 0 {
		c, a := d.getQaStatusConditionAndArgs(req.ReleaseStatus, req.ValidityStatus)
		condition = fmt.Sprintf("%s%s", condition, c)
		args = append(args, a...)
	}
	if len(req.DocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id IN (%s)", condition, placeholder(len(req.DocID)))
		for _, id := range req.DocID {
			args = append(args, id)
		}
	}
	if len(req.ExcludeDocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id NOT IN (%s)", condition, placeholder(len(req.ExcludeDocID)))
		for _, eid := range req.ExcludeDocID {
			args = append(args, eid)
		}
	}
	pageSize := uint32(15)
	page := uint32(1)
	if req.PageSize != 0 {
		pageSize = req.PageSize
	}
	if req.Page != 0 {
		page = req.Page
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	// 查询字段指定表名
	qaFieldsArr := strings.Split(qaFields, ",")
	for i := range qaFieldsArr {
		qaFieldsArr[i] = "t_doc_qa." + strings.Trim(qaFieldsArr[i], " ")
	}
	qaSimilarTipsFieldsArr := qaSimilarTipsFields
	// if req.Query != "" {
	// qaSimilarTipsFieldsArr = qaSimilarTipsQueryFields
	// }
	qaFieldsArr = append(qaFieldsArr, qaSimilarTipsFieldsArr)

	querySQL := fmt.Sprintf(getQAList, strings.Join(qaFieldsArr, ","), joinSql, condition)

	log.InfoContextf(ctx, "qaSimilarTipsFieldsArr:%s", querySQL)
	qas := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &qas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return qas, nil
}

// GetQAListCount 获取问答对列表数量
func (d *dao) GetQAListCount(ctx context.Context, req *model.QAListReq) (uint32, uint32, uint32, uint32, error) {
	condition := ""
	var args []any
	args = append(args, req.CorpID, req.RobotID)
	if req.IsDeleted != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.is_deleted = ?")
		args = append(args, req.IsDeleted)
	}
	if req.Source != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.source = ?")
		args = append(args, req.Source)
	}
	joinSql := ""
	if req.Query != "" {
		queryArg := fmt.Sprintf("%%%s%%", special.Replace(req.Query))
		if req.QueryType == model.DocQueryTypeFileName && req.Query != model.DocQuerySystemTypeUntagged {
			// condition = fmt.Sprintf("%s%s", condition,
			//	" AND (t_doc_qa.question LIKE ? OR t_qa_similar_question.question like ?)")
			// args = append(args, queryArg, queryArg)
			condition = fmt.Sprintf("%s%s", condition,
				" AND (t_doc_qa.question LIKE ?)")
			args = append(args, queryArg)
		}
		if req.QueryType == model.DocQueryTypeAttribute && req.Query != model.DocQuerySystemTypeUntagged {
			joinSql = getDocQAJoinSql
			condition = fmt.Sprintf("%s%s", condition,
				" AND (t_attribute_label.name LIKE ? OR t_attribute_label.similar_label LIKE ? OR t_attribute.name LIKE ?)")
			args = append(args, queryArg, queryArg, queryArg)
		}
		if req.Query == model.DocQuerySystemTypeUntagged {
			joinSql = getDocQAUntaggedJoinSql
			condition = fmt.Sprintf("%s%s", condition, " AND qa_attribute.id IS NULL")
		}
	}
	if req.QueryAnswer != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.answer LIKE ?")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(req.QueryAnswer)))
	}
	if len(req.CateIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.category_id IN (%s)", condition, placeholder(len(req.CateIDs)))
		for _, cID := range req.CateIDs {
			args = append(args, cID)
		}
	}
	if len(req.QABizIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.business_id IN (%s)", condition, placeholder(len(req.QABizIDs)))
		for _, qaBizID := range req.QABizIDs {
			args = append(args, qaBizID)
		}
	}
	if len(req.DocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id IN (%s)", condition, placeholder(len(req.DocID)))
		for _, id := range req.DocID {
			args = append(args, id)
		}
	}
	if len(req.ExcludeDocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id NOT IN (%s)", condition, placeholder(len(req.ExcludeDocID)))
		for _, eid := range req.ExcludeDocID {
			args = append(args, eid)
		}
	}
	if !req.UpdateTime.IsZero() && !req.UpdateTimeEqual {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.update_time > ?")
		args = append(args, req.UpdateTime)
	}
	if req.UpdateTimeEqual && !req.UpdateTime.IsZero() && req.QAID != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.update_time = ? AND t_doc_qa.id >= ?")
		args = append(args, req.UpdateTime, req.QAID)
	}
	waitVerify, noAccepted, accepted, total, err := d.getQaTotalContainExpire(ctx, req, joinSql, condition, args)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return waitVerify, noAccepted, accepted, total, nil
}

func (d *dao) getQaTotal(stat []*model.QAStat, req *model.QAListReq) (waitVerify, noAccepted, accepted, total uint32) {
	statMap := make(map[uint32]map[uint32]uint32, len(stat))
	for _, v := range stat {
		if _, ok := statMap[v.AcceptStatus]; !ok {
			statMap[v.AcceptStatus] = make(map[uint32]uint32, 0)
		}
		statMap[v.AcceptStatus][v.ReleaseStatus] = v.Total
		if v.AcceptStatus == model.AcceptInit {
			waitVerify += v.Total
		} else if v.AcceptStatus == model.AcceptNo {
			noAccepted += v.Total
		} else if v.AcceptStatus == model.AcceptYes && v.ReleaseStatus == model.QAReleaseStatusInit {
			accepted += v.Total
		}
		if len(req.AcceptStatus) != 0 {
			continue
		}
		if len(req.ReleaseStatus) == 0 {
			total += v.Total
			continue
		}
		for _, releaseStatus := range req.ReleaseStatus {
			if v.ReleaseStatus == releaseStatus {
				total += v.Total
			}
		}
	}
	for _, acceptStatus := range req.AcceptStatus {
		if _, ok := statMap[acceptStatus]; !ok {
			continue
		}
		if len(req.ReleaseStatus) == 0 {
			for _, t := range statMap[acceptStatus] {
				total += t
			}
		} else {
			for _, releaseStatus := range req.ReleaseStatus {
				total += statMap[acceptStatus][releaseStatus]
			}
		}
	}
	return waitVerify, noAccepted, accepted, total
}

// GetQADetail 获取QA详情
func (d *dao) GetQADetail(ctx context.Context, corpID, robotID uint64, id uint64) (*model.DocQA, error) {
	qas, err := d.GetQADetails(ctx, corpID, robotID, []uint64{id})
	if err != nil {
		return nil, err
	}
	qa, ok := qas[id]
	if !ok {
		return nil, errs.ErrQANotFound
	}
	return qa, nil
}

// GetQADetails 批量获取QA详情
func (d *dao) GetQADetails(ctx context.Context, corpID, robotID uint64, ids []uint64) (map[uint64]*model.DocQA, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, 2+len(ids))
	args = append(args, corpID, robotID)
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getQADetail, qaFields, placeholder(len(ids)))
	list := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取QA详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	qas := make(map[uint64]*model.DocQA, 0)
	for _, item := range list {
		qas[item.ID] = item
	}
	return qas, nil
}

// GetQADetailsByBizIDs 批量获取QA详情
func (d *dao) GetQADetailsByBizIDs(ctx context.Context, corpID, robotID uint64,
	bizIDs []uint64) (map[uint64]*model.DocQA, error) {
	if len(bizIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, 2+len(bizIDs))
	args = append(args, corpID, robotID)
	for _, id := range bizIDs {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getQADetailByBizIDs, qaFields, placeholder(len(bizIDs)))
	list := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "批量获取QA详情 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	qas := make(map[uint64]*model.DocQA, 0)
	for _, item := range list {
		qas[item.BusinessID] = item
	}
	return qas, nil
}

// GetQADetailsByBizID 获取QA详情
func (d *dao) GetQADetailsByBizID(ctx context.Context, corpID, robotID uint64, bizID uint64) (*model.DocQA, error) {
	qas, err := d.GetQADetailsByBizIDs(ctx, corpID, robotID, []uint64{bizID})
	if err != nil {
		return nil, err
	}
	qa, ok := qas[bizID]
	if !ok {
		return nil, errs.ErrQANotFound
	}
	return qa, nil
}

// GetQADetailsByReleaseStatus 批量获取QA详情(机器审核不通过)
func (d *dao) GetQADetailsByReleaseStatus(ctx context.Context, corpID, robotID uint64, ids []uint64,
	releaseStatus uint32) (map[uint64]*model.DocQA, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, 3+len(ids))
	args = append(args, corpID, robotID, releaseStatus)
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getQADetailByReleaseStatus, qaFields, placeholder(len(ids)))
	list := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取QA详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, errs.ErrAppealNotFound
	}
	qas := make(map[uint64]*model.DocQA, 0)
	for _, item := range list {
		qas[item.ID] = item
	}
	return qas, nil
}

// GetDocQANum 统计文档有效问答对
func (d *dao) GetDocQANum(ctx context.Context, corpID, robotID uint64, docIDs []uint64) (map[uint64]map[uint32]uint32,
	error) {
	statMap := make(map[uint64]map[uint32]uint32, 0)
	if len(docIDs) == 0 {
		return statMap, nil
	}
	querySQL := fmt.Sprintf(getDocQANum, placeholder(len(docIDs)))
	args := make([]any, 0, 2+len(docIDs))
	args = append(args, corpID, robotID)
	for _, docID := range docIDs {
		args = append(args, docID)
	}
	stat := make([]*model.DocQANum, 0)
	if err := d.db.QueryToStructs(ctx, &stat, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "统计文档有效问答对失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, v := range stat {
		_, ok := statMap[v.DocID]
		if !ok {
			statMap[v.DocID] = make(map[uint32]uint32)
		}
		statMap[v.DocID][v.IsDeleted] = v.Total
	}
	return statMap, nil
}

// CheckUnconfirmedQa 检查是否有未确认的QA
func (d *dao) CheckUnconfirmedQa(ctx context.Context, robotID uint64) (bool, error) {
	var total uint64
	args := []any{robotID, model.AcceptInit, model.QAIsDeleted}
	if err := d.db.Get(ctx, &total, getQAUnconfirmedCount, args...); err != nil {
		log.ErrorContextf(ctx, "检查是否有未确认的QA失败 sql:%s robotID:%+v err:%+v",
			getQAUnconfirmedCount, robotID, err)
		return false, err
	}
	if total > 0 {
		return true, nil
	}
	return false, nil
}

// PollQaToSimilar 获取要匹配相似度的问答对列表
func (d *dao) PollQaToSimilar(ctx context.Context) ([]*model.DocQA, error) {
	querySQL := fmt.Sprintf(getQAListToSimilar, qaFields)
	endTime := time.Now().Add(-config.App().CronTask.QASimilarTask.WaitAMoment)
	startTime := endTime.Add(-10 * time.Minute)
	args := []any{model.SimilarStatusInit, model.QAIsNotDeleted, startTime, endTime,
		config.App().CronTask.QASimilarTask.PageSize}
	qaList := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &qaList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return qaList, nil
}

// LockOneQa 锁定一条问答对
func (d *dao) LockOneQa(ctx context.Context, task *model.DocQA) error {
	task.SimilarStatus = model.SimilarStatusIng
	querySQL := lockOneQa
	res, err := d.db.NamedExec(ctx, querySQL, task)
	if err != nil {
		log.ErrorContextf(ctx, "锁定一条问答对失败 sql:%s args:%+v err:%+v", querySQL, task, err)
		return err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		log.DebugContextf(ctx, "获取锁定一条问答对影响行数 rowsAffected == 0 ")
		return errs.ErrLockTaskFail
	}
	return nil
}

// UnLockOneQa 解锁一条问答对
func (d *dao) UnLockOneQa(ctx context.Context, task *model.DocQA) error {
	task.SimilarStatus = model.SimilarStatusEnd
	querySQL := lockOneQa
	res, err := d.db.NamedExec(ctx, querySQL, task)
	if err != nil {
		log.ErrorContextf(ctx, "解锁一条问答对失败 sql:%s args:%+v err:%+v", querySQL, task, err)
		return err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return errs.ErrLockTaskFail
	}
	return nil
}

// GetReleaseQACount 获取发布QA总数
func (d *dao) GetReleaseQACount(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, actions []uint32) (uint64, error) {
	var total uint64
	args := make([]any, 0, 11+len(actions))
	args = append(args, corpID, robotID, model.AcceptInit, model.QAReleaseStatusInit, model.NextActionAdd,
		model.QAIsDeleted, model.NextActionAdd, model.AcceptNo)
	condition := ""
	if question != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(question)))
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
	querySQL := fmt.Sprintf(getReleaseQACount, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布QA总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetReleaseQAList 获取发布问答对列表
func (d *dao) GetReleaseQAList(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, actions []uint32, page, pageSize uint32) (
	[]*model.DocQA, error) {
	args := make([]any, 0, 13+len(actions))
	args = append(args, corpID, robotID, model.AcceptInit, model.QAReleaseStatusInit, model.NextActionAdd,
		model.QAIsDeleted, model.NextActionAdd, model.AcceptNo)
	condition := ""
	if question != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(question)))
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
	querySQL := fmt.Sprintf(getReleaseQAList, qaFields, condition)
	qas := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &qas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "获取发布问答对列表 sql:%s args:%+v", querySQL, args)
	return qas, nil
}

// GetQAByID 通过ID获取QA详情
func (d *dao) GetQAByID(ctx context.Context, id uint64) (*model.DocQA, error) {
	args := make([]any, 0, 1)
	args = append(args, id)
	querySQL := fmt.Sprintf(getQAByID, qaFields)
	list := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取QA详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// GetQAByBizID 通过bizID获取QA详情
func (d *dao) GetQAByBizID(ctx context.Context, bizID uint64) (*model.DocQA, error) {
	args := make([]any, 0, 1)
	args = append(args, bizID)
	querySQL := fmt.Sprintf(getQAByBizID, qaFields)
	list := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过bizID获取QA详情 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// GetQABySegment 通过分段获取QA详情
func (d *dao) GetQABySegment(ctx context.Context, segment *model.DocSegmentExtend) ([]*model.DocQA, error) {
	args := make([]any, 0, 4)
	args = append(args, segment.CorpID, segment.DocID, segment.ID, 1)
	querySQL := fmt.Sprintf(getSegmentQA, qaFields)
	list := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过分段获取QA详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetQAIDsByOriginDocID 根据来源文档id获取问答id列表
func (d *dao) GetQAIDsByOriginDocID(ctx context.Context, robotID, originDocID uint64) ([]uint64, error) {
	var args = []any{robotID, originDocID}
	querySQL := getQAIDsByOriginDocID
	ids := make([]uint64, 0)
	if err := d.db.Select(ctx, &ids, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据来源文档id获取问答id列表 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return ids, nil
}

// CmdGetQAList 查询t_doc_qa表  未删除、已采納的qa
func (d *dao) CmdGetQAList(ctx context.Context, corpID uint64, acceptStatus []uint32, page, pageSize uint32) (
	[]*model.DocQA, error) {
	args := make([]any, 0, 4+len(acceptStatus))
	args = append(args, model.QAIsNotDeleted)
	condition := ""
	if corpID != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND corp_id = ?")
		args = append(args, corpID)
	}
	if len(acceptStatus) != 0 {
		condition = fmt.Sprintf("%s AND accept_status IN (%s)", condition, placeholder(len(acceptStatus)))
		for _, status := range acceptStatus {
			args = append(args, status)
		}
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(cmdGetQAList, qaFields, condition)
	qas := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &qas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取问答对列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return qas, nil
}

// CmdGetQAListCount 未删除、未處理或者已采納
func (d *dao) CmdGetQAListCount(ctx context.Context, corpID uint64, acceptStatus []uint32) (uint64, error) {
	args := make([]any, 0, 2+len(acceptStatus))
	args = append(args, model.QAIsNotDeleted)
	condition := ""
	if corpID != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND corp_id = ?")
		args = append(args, corpID)
	}
	if len(acceptStatus) != 0 {
		condition = fmt.Sprintf("%s AND accept_status IN (%s)", condition, placeholder(len(acceptStatus)))
		for _, status := range acceptStatus {
			args = append(args, status)
		}
	}
	querySQL := fmt.Sprintf(cmdGetQAListCount, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取问答对列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetQAsByIDs 根据ID获取问答
func (d *dao) GetQAsByIDs(
	ctx context.Context, corpID, robotID uint64, qaIDs []uint64, offset, limit uint64,
) ([]*model.DocQA, error) {
	if len(qaIDs) == 0 {
		return nil, nil
	}
	var err error
	query := fmt.Sprintf(getQAsByIDs, qaFields)
	args := []any{corpID, robotID, model.AcceptYes, model.QAIsNotDeleted, qaIDs, offset, limit}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		log.ErrorContextf(ctx, "根据QAID获取问答对列表失败 sql:%s args: %+v err: %v", query, args, err)
		return nil, err
	}
	var qas []*model.DocQA
	if err = d.db.QueryToStructs(ctx, &qas, query, args...); err != nil {
		log.ErrorContextf(ctx, "根据QAID获取问答对列表失败 sql:%s args: %+v err: %v", query, args, err)
		return nil, err
	}
	return qas, nil
}

// GetQAsByBizIDs 根据业务ID获取问答
func (d *dao) GetQAsByBizIDs(
	ctx context.Context, corpID, robotID uint64, qaBizIDs []uint64, offset, limit uint64,
) ([]*model.DocQA, error) {
	if len(qaBizIDs) == 0 {
		return nil, nil
	}
	var err error
	query := fmt.Sprintf(getQAsByBizIDs, qaFields)
	args := []any{corpID, robotID, model.AcceptYes, model.QAIsNotDeleted, qaBizIDs, offset, limit}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		log.ErrorContextf(ctx, "根据QAID获取问答对列表失败 sql:%s args: %+v err: %v", query, args, err)
		return nil, err
	}
	var qas []*model.DocQA
	if err = d.db.QueryToStructs(ctx, &qas, query, args...); err != nil {
		log.ErrorContextf(ctx, "根据QAID获取问答对列表失败 sql:%s args: %+v err: %v", query, args, err)
		return nil, err
	}
	return qas, nil
}

// GetRobotQACharSize 获取单个机器人问答字符总数
func (d *dao) GetRobotQACharSize(ctx context.Context, robotID uint64, corpID uint64) (uint64, error) {
	var count uint64
	args := []any{robotID, corpID, model.QAIsNotDeleted, model.AcceptYes}
	exceededStatus := []any{
		model.QAReleaseStatusCharExceeded,
		model.QAReleaseStatusResuming,
		model.QAReleaseStatusAppealFailCharExceeded,
		model.QAReleaseStatusAppealFailResuming,
		model.QAReleaseStatusAuditNotPassCharExceeded,
		model.QAReleaseStatusAuditNotPassResuming,
		model.QAReleaseStatusAuditNotPassCharExceeded,
		model.QAReleaseStatusLearnFailResuming,
	}
	args = append(args, exceededStatus...)
	querySQL := fmt.Sprintf(getQaCharSize, placeholder(len(exceededStatus)))
	if err := d.db.Get(ctx, &count, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取机器人问答字符数失败 sql:%s args:%+v err:%+v", getQaCharSize, args, err)
		return 0, err
	}
	return count, nil
}

// GetRobotQAExceedCharSize 获取机器人超量问答字符总数
func (d *dao) GetRobotQAExceedCharSize(ctx context.Context, corpID uint64, robotIDs []uint64) (
	map[uint64]uint64, error) {
	type AppQAExceedCharSize struct {
		AppID            uint64 `db:"robot_id"`         // 应用ID
		QAExceedCharSize uint64 `db:"exceed_char_size"` // 超量字符
	}
	appQAExceedCharSize := make([]*AppQAExceedCharSize, 0)
	args := []any{corpID, model.QAIsNotDeleted, model.AcceptYes}
	for _, robotID := range robotIDs {
		args = append(args, robotID)
	}
	exceededStatus := []any{
		model.QAReleaseStatusCharExceeded,
		model.QAReleaseStatusResuming,
		model.QAReleaseStatusAppealFailCharExceeded,
		model.QAReleaseStatusAppealFailResuming,
		model.QAReleaseStatusAuditNotPassCharExceeded,
		model.QAReleaseStatusAuditNotPassResuming,
		model.QAReleaseStatusLearnFailCharExceeded,
		model.QAReleaseStatusLearnFailResuming,
	}
	args = append(args, exceededStatus...)
	querySQL := fmt.Sprintf(getAppQaExceedCharSize, placeholder(len(robotIDs)), placeholder(len(exceededStatus)))
	if err := d.db.QueryToStructs(ctx, &appQAExceedCharSize, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetRobotQAExceedCharSize failed|sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}

	robotQAExceedCharSizeMap := make(map[uint64]uint64)
	for _, app := range appQAExceedCharSize {
		robotQAExceedCharSizeMap[app.AppID] = app.QAExceedCharSize
	}
	return robotQAExceedCharSizeMap, nil
}

// UpdateAuditQA 更新QA审核状态
func (d *dao) UpdateAuditQA(ctx context.Context, qa *model.DocQA) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		qa.UpdateTime = time.Now()
		querySQL := updateAuditQA
		if _, err := tx.NamedExecContext(ctx, querySQL, qa); err != nil {
			log.ErrorContextf(ctx, "更新QA审核状态 sql:%s args:%+v err:%+v", querySQL, qa, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新QA审核状态 err:%+v", err)
		return err
	}
	return nil
}

// HasUnconfirmedQa 是否有未确认的QA
func (d *dao) HasUnconfirmedQa(ctx context.Context, corpID, staffID, robotID uint64) (bool, error) {
	key := fmt.Sprintf("qbot:admin:qa:%s:%s", cast.ToString(robotID), cast.ToString(staffID))
	val, err := redis.String(d.redis.Do(ctx, "GET", key))
	if err != nil {
		if err == redis.ErrNil {
			return false, nil
		}
		log.ErrorContextf(ctx, "获取用户访问问答时间错误: %+v, key: %s", err, key)
		return false, err
	}
	log.DebugContextf(ctx, "HasUnconfirmedQa corpID:%d,staffId:%d,robotId:%d,key:%s,val:%s", corpID, robotID,
		staffID, key, val)
	accessTime := time.UnixMilli(cast.ToInt64(val))
	var total uint64
	args := []any{corpID, robotID, model.QAIsNotDeleted, model.AcceptInit, accessTime}
	if err = d.db.Get(ctx, &total, getQAUnconfirmedNumByTime, args...); err != nil {
		log.ErrorContextf(ctx, "检查是否有未确认的QA失败 sql:%s robotID:%+v err:%+v",
			getQAUnconfirmedNumByTime, robotID, err)
		return false, err
	}
	return total > 0, nil
}

// GetUnconfirmedQaNum 未确认的QA数量
func (d *dao) GetUnconfirmedQaNum(ctx context.Context, corpID, robotID uint64) (uint64, error) {
	var total uint64
	args := []any{corpID, robotID, model.QAIsNotDeleted, model.AcceptInit}
	if err := d.db.Get(ctx, &total, getQAUnconfirmedNum, args...); err != nil {
		log.ErrorContextf(ctx, "未确认的QA数量失败 sql:%s cordId:%+v robotID:%+v err:%+v",
			getQAUnconfirmedNum, corpID, robotID, err)
		return 0, err
	}
	return total, nil
}

// UpdateAppealQA 更新申诉单状态
func (d *dao) UpdateAppealQA(ctx context.Context, qaDetails map[uint64]*model.DocQA) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, qa := range qaDetails {
			now := time.Now()
			sql := updateQA
			qa.UpdateTime = now
			if _, err := tx.NamedExecContext(ctx, sql, qa); err != nil {
				log.ErrorContextf(ctx, "申诉更新问答对失败 sql:%s args:%+v err:%+v", sql, qa, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "提交申诉失败 err:%+v", err)
		return err
	}
	return nil
}

// getQaStatusConditionAndArgs 根据过期状态以及发布状态返回相关sql查询条件
func (d *dao) getQaStatusConditionAndArgs(releaseStatus []uint32, validityStatus uint32) (string, []interface{}) {
	var c string
	var args []interface{}
	// 勾选其他状态，未勾选已过期
	if len(releaseStatus) != 0 && validityStatus != model.QaExpiredStatus {
		c = fmt.Sprintf(` AND t_doc_qa.release_status IN (%s) AND (t_doc_qa.expire_end = ? OR t_doc_qa.expire_end >= ?) `,
			placeholder(len(releaseStatus)))
		for i := range releaseStatus {
			args = append(args, releaseStatus[i])
		}
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		return c, args
	}
	// 只勾选已过期
	if len(releaseStatus) == 0 && validityStatus == model.QaExpiredStatus {
		c = ` AND (t_doc_qa.expire_end > ? && t_doc_qa.expire_end < ?) `
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		return c, args
	}
	// 勾选其他状态+已过期
	if len(releaseStatus) != 0 && validityStatus == model.QaExpiredStatus {
		c = fmt.Sprintf(` AND (%s OR (%s AND t_doc_qa.release_status IN (%s))) `,
			` (t_doc_qa.expire_end > ? && t_doc_qa.expire_end < ?) `,
			` (t_doc_qa.expire_end = ? OR t_doc_qa.expire_end >= ?) `, placeholder(len(releaseStatus)))
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"),
			time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		for i := range releaseStatus {
			args = append(args, releaseStatus[i])
		}
	}
	return c, args
}

func (d *dao) getNoExpireTotal(ctx context.Context, req *model.QAListReq, joinSql, condition string,
	args []interface{}) (waitVerify, noAccepted, accepted, total uint32, err error) {
	condition = fmt.Sprintf("%s%s", condition, ` AND (t_doc_qa.expire_end = ? OR t_doc_qa.expire_end >= ?) `)
	args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
		time.Now().Format("2006-01-02 15:04:05.000"))
	querySQL := fmt.Sprintf(getQACount, joinSql, condition)
	stat := make([]*model.QAStat, 0)
	if err = d.db.QueryToStructs(ctx, &stat, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取问答对列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, 0, 0, 0, err
	}
	waitVerify, noAccepted, accepted, total = d.getQaNotExpireTotal(stat, req)
	return waitVerify, noAccepted, accepted, total, nil
}

func (d *dao) getExpireTotal(ctx context.Context, req *model.QAListReq, joinSql, condition string,
	args []interface{}) (waitVerify, noAccepted, accepted, total uint32, err error) {
	condition = fmt.Sprintf("%s%s", condition, ` AND (t_doc_qa.expire_end > ? && t_doc_qa.expire_end < ?) `)
	args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
		time.Now().Format("2006-01-02 15:04:05.000"))
	querySQL := fmt.Sprintf(getQACount, joinSql, condition)
	stat := make([]*model.QAStat, 0)
	if err = d.db.QueryToStructs(ctx, &stat, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取问答对列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, 0, 0, 0, err
	}
	waitVerify, noAccepted, accepted, total = d.getQaExpireTotal(stat, req)
	return waitVerify, noAccepted, accepted, total, nil
}

func (d *dao) getQaNotExpireTotal(stat []*model.QAStat,
	req *model.QAListReq) (waitVerify, noAccepted, accepted, total uint32) {
	statMap := make(map[uint32]map[uint32]uint32, len(stat))
	for _, v := range stat {
		if _, ok := statMap[v.AcceptStatus]; !ok {
			statMap[v.AcceptStatus] = make(map[uint32]uint32, 0)
		}
		statMap[v.AcceptStatus][v.ReleaseStatus] = v.Total
		if v.AcceptStatus == model.AcceptInit {
			waitVerify += v.Total
		} else if v.AcceptStatus == model.AcceptNo {
			noAccepted += v.Total
		} else if v.AcceptStatus == model.AcceptYes && v.ReleaseStatus == model.QAReleaseStatusInit {
			accepted += v.Total
		}
		if len(req.AcceptStatus) != 0 {
			continue
		}
		// 如果是只筛选过期问答对，则不能加总数
		if len(req.ReleaseStatus) == 0 && req.ValidityStatus != model.QaExpiredStatus {
			total += v.Total
			continue
		}
		for _, releaseStatus := range req.ReleaseStatus {
			if v.ReleaseStatus == releaseStatus {
				total += v.Total
			}
		}
	}
	for _, acceptStatus := range req.AcceptStatus {
		if _, ok := statMap[acceptStatus]; !ok {
			continue
		}
		if len(req.ReleaseStatus) == 0 && req.ValidityStatus != model.QaExpiredStatus {
			for _, t := range statMap[acceptStatus] {
				total += t
			}
		} else {
			for _, releaseStatus := range req.ReleaseStatus {
				total += statMap[acceptStatus][releaseStatus]
			}
		}
	}
	return waitVerify, noAccepted, accepted, total
}

func (d *dao) getQaExpireTotal(stat []*model.QAStat,
	req *model.QAListReq) (waitVerify, noAccepted, accepted, total uint32) {
	statMap := make(map[uint32]map[uint32]uint32, len(stat))
	for _, v := range stat {
		if _, ok := statMap[v.AcceptStatus]; !ok {
			statMap[v.AcceptStatus] = make(map[uint32]uint32, 0)
		}
		statMap[v.AcceptStatus][v.ReleaseStatus] = v.Total
		if v.AcceptStatus == model.AcceptInit {
			waitVerify += v.Total
		} else if v.AcceptStatus == model.AcceptNo {
			noAccepted += v.Total
		} else if v.AcceptStatus == model.AcceptYes && v.ReleaseStatus == model.QAReleaseStatusInit {
			accepted += v.Total
		}
		if len(req.AcceptStatus) != 0 {
			continue
		}
		if req.ValidityStatus != model.QaUnExpiredStatus {
			total += v.Total
			continue
		}
	}
	// 如果是已过期的，同时未勾选已过期，那就不需要计算total了，默认就是0
	if req.ValidityStatus == model.QaUnExpiredStatus {
		return waitVerify, noAccepted, accepted, 0
	}
	for _, acceptStatus := range req.AcceptStatus {
		if _, ok := statMap[acceptStatus]; !ok {
			continue
		}
		if req.ValidityStatus != model.QaUnExpiredStatus {
			for _, t := range statMap[acceptStatus] {
				total += t
			}
		}
	}
	return waitVerify, noAccepted, accepted, total
}

// getQaTotalContainExpire 获取问答对总数（包含过期）
func (d *dao) getQaTotalContainExpire(ctx context.Context, req *model.QAListReq, joinSql, condition string,
	args []interface{}) (waitVerify, noAccepted, accepted, total uint32, err error) {
	// 查询个数，没有增加筛选条件，一次查询结果，避免查询两次
	if req.ValidityStatus == 0 && len(req.ReleaseStatus) == 0 {
		querySQL := fmt.Sprintf(getQACount, joinSql, condition)
		stat := make([]*model.QAStat, 0)
		if err = d.db.QueryToStructs(ctx, &stat, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "获取问答对列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return 0, 0, 0, 0, err
		}
		waitVerify, noAccepted, accepted, total = d.getQaTotal(stat, req)
		return waitVerify, noAccepted, accepted, total, nil
	}
	// 计算总数有两部分来源计算
	// 1.未过期，sql语句会先用过期判别，其他流程按照之前老逻辑计算即可
	// 2.已过期，sql语句会先用过期判别，获取总数的逻辑，状态按照非发布状态来计算，但是需要排序如果未勾选过期状态，则计算时，只需要计算过期带校验
	// 原则，total是按照过期 状态筛选，其他状态都不需要过期时间筛选，因为sql语句无法group时间，所以只能分别获取两种条件，然后在进一步计算
	//  先算未过期的数量
	waitVerify, noAccepted, accepted, total, err = d.getNoExpireTotal(ctx, req, joinSql, condition, args)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	// 再算过期的
	eWaitVerify, eNoAccepted, eAccepted, eTotal, err := d.getExpireTotal(ctx, req, joinSql, condition, args)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	waitVerify += eWaitVerify
	noAccepted += eNoAccepted
	accepted += eAccepted
	total += eTotal
	return waitVerify, noAccepted, accepted, total, nil
}

// GetQaCountWithDocID 获取某个文档ID对应的问答个数
func (d *dao) GetQaCountWithDocID(ctx context.Context, req *model.QAListReq) (uint32, error) {
	if req == nil || len(req.DocID) == 0 {
		log.ErrorContextf(ctx, "参数错误，获取文档对应的问答个数失败")
		return 0, fmt.Errorf("req is invalid")
	}
	docID := req.DocID[0]
	stat := make([]*model.QAStat, 0)
	args := []interface{}{req.CorpID, docID, req.RobotID}
	if err := d.db.QueryToStructs(ctx, &stat, getQACountByDocID, args...); err != nil {
		log.ErrorContextf(ctx, "获取文档对应的问答对列表数量失败 sql:%s args:%+v| err:%+v",
			getQACountByDocID, args, err)
		return 0, err
	}
	if len(stat) > 0 {
		log.InfoContextf(ctx, "获取文档对应的问答对列表数量成功，个数为:%d", stat[0].Total)
		return stat[0].Total, nil
	}
	log.ErrorContextf(ctx, "获取文档对应的问答对列表数量失败 sql:%s args:%+v| result is empty",
		getQACountByDocID, args)
	return 0, fmt.Errorf("result is empty")
}

// GetQAChunk 分段获取问答
func (d *dao) GetQAChunk(ctx context.Context, corpID, appID, offset, limit uint64) ([]*model.DocQA, error) {
	query := getQAChunk
	args := []any{corpID, appID, model.QAIsNotDeleted, offset, limit}
	var qas []*model.DocQA
	if err := d.db.Select(ctx, &qas, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetQAChunk fail, query: %s args: %+v err: %v", query, args, err)
		return nil, err
	}
	return qas, nil
}

// GetQAChunkCount 获取问答总数
func (d *dao) GetQAChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	query := getQAChunkCount
	args := []any{corpID, appID, model.QAIsNotDeleted, model.AcceptYes}
	var count int
	if err := d.db.Get(ctx, &count, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetQAChunkCount fail, query: %s args: %+v err: %v", query, args, err)
		return 0, err
	}
	return count, nil
}

// GetSimilarChunkCount 获取相似问总数
func (d *dao) GetSimilarChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	query := getSimilarChunkCount
	args := []any{corpID, appID, model.QAIsNotDeleted}
	var count int
	if err := d.db.Get(ctx, &count, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetQAChunkCount fail, query: %s args: %+v err: %v", query, args, err)
		return 0, err
	}
	return count, nil
}

// UpdateQAAttrRange 更新问答对适用范围(支持相似问联动)
func (d *dao) UpdateQAAttrRange(ctx context.Context, qas []*model.DocQA,
	attributeLabelReq *model.UpdateQAAttributeLabelReq) error {
	if len(qas) == 0 {
		return nil
	}
	now := time.Now()
	var syncIDs []uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		sql := updateQA
		for _, qa := range qas {
			qa.UpdateTime = now
			qa.IsAuditFree = model.QAIsAuditNotFree
			isAuditOrAppealFail := false // 原先处于审核失败或者人工申诉失败，不修改qa状态，也不入库
			if qa.ReleaseStatus == model.QAReleaseStatusAuditNotPass ||
				qa.ReleaseStatus == model.QAReleaseStatusAppealFail {
				isAuditOrAppealFail = true
			}
			if _, err := tx.NamedExecContext(ctx, sql, qa); err != nil {
				log.ErrorContextf(ctx, "更新问答对失败 sql:%s args:%+v err:%+v", sql, qa, err)
				return err
			}
			if err := d.updateQAAttributeLabel(ctx, tx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
				return err
			}
			if !isAuditOrAppealFail {
				id, err := d.addQASync(ctx, tx, qa)
				if err != nil {
					return err
				}
				syncIDs = append(syncIDs, id)
			}

			// 相似问处理
			sqs, err := d.GetSimilarQuestionsByQA(ctx, qa)
			if err != nil {
				// 伽利略error日志告警
				log.ErrorContextf(ctx, "UpdateQAAttrRange qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
				// 柔性放过
			}
			if len(sqs) > 0 {
				if err = d.UpdateSimilarQuestions(ctx, tx, qa, sqs); err != nil {
					log.ErrorContextf(ctx, "更新相似问失败 err:%+v", err)
					return err
				}
				if !isAuditOrAppealFail {
					if syncSimilarQuestionsIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs); err != nil {
						log.ErrorContextf(ctx, "添加相似问同步任务失败(update) err:%+v", err)
						return err
					}
					syncIDs = append(syncIDs, syncSimilarQuestionsIDs...)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		d.vector.Push(ctx, syncID)
	}
	return nil
}

// UpdateQAsExpire 批量更新问答过期时间(支持相似问联动)
func (d *dao) UpdateQAsExpire(ctx context.Context, qas []*model.DocQA) error {
	if len(qas) == 0 {
		return nil
	}
	now := time.Now()
	var syncIDs []uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		sql := updateQAsExpire
		for _, qa := range qas {
			qa.UpdateTime = now
			qa.IsAuditFree = model.QAIsAuditNotFree
			qa.NextAction = model.NextActionUpdate
			isAuditOrAppealFail := false // 原先处于审核失败或者人工申诉失败，不修改qa状态，也不入库
			if qa.ReleaseStatus == model.QAReleaseStatusAuditNotPass ||
				qa.ReleaseStatus == model.QAReleaseStatusAppealFail {
				isAuditOrAppealFail = true
			}
			if !isAuditOrAppealFail {
				qa.ReleaseStatus = model.QAReleaseStatusLearning
			}
			if _, err := tx.NamedExecContext(ctx, sql, qa); err != nil {
				log.ErrorContextf(ctx, "更新问答过期时间失败 sql:%s args:%+v err:%+v", sql, qa, err)
				return err
			}
			if err := d.deleteQASimilar(ctx, tx, qa); err != nil {
				log.ErrorContextf(ctx, "deleteQASimilar|更新问答过期时间失败 sql:%s args:%+v err:%+v", sql, qa, err)
				return err
			}
			if !isAuditOrAppealFail {
				id, err := d.addQASync(ctx, tx, qa)
				if err != nil {
					log.ErrorContextf(ctx, "addQASync|更新问答过期时间失败 sql:%s args:%+v err:%+v", sql, qa, err)
					return err
				}
				syncIDs = append(syncIDs, id)
			}
			// 相似问处理
			sqs, err := d.GetSimilarQuestionsByQA(ctx, qa)
			if err != nil {
				// 伽利略error日志告警
				log.ErrorContextf(ctx, "UpdateQAsExpire qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
				// 柔性放过
			}
			if len(sqs) > 0 {
				if err = d.UpdateSimilarQuestions(ctx, tx, qa, sqs); err != nil {
					log.ErrorContextf(ctx, "更新相似问失败 err:%+v", err)
					return err
				}
				if !isAuditOrAppealFail {
					if syncSimilarQuestionsIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs); err != nil {
						log.ErrorContextf(ctx, "添加相似问同步任务失败(update) err:%+v", err)
						return err
					}
					syncIDs = append(syncIDs, syncSimilarQuestionsIDs...)
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		d.vector.Push(ctx, syncID)
	}
	return nil
}

// UpdateQAsDoc 更新问答关联文档
func (d *dao) UpdateQAsDoc(ctx context.Context, qas []*model.DocQA) error {
	if len(qas) == 0 {
		return nil
	}
	now := time.Now()
	var syncIDs []uint64
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		sql := updateQAsDoc
		for _, qa := range qas {
			qa.UpdateTime = now
			qa.IsAuditFree = model.QAIsAuditNotFree
			qa.NextAction = model.NextActionUpdate
			isAuditOrAppealFail := false // 原先处于审核失败或者人工申诉失败，不修改qa状态，也不入库
			if qa.ReleaseStatus == model.QAReleaseStatusAuditNotPass ||
				qa.ReleaseStatus == model.QAReleaseStatusAppealFail {
				isAuditOrAppealFail = true
			}
			if !isAuditOrAppealFail {
				qa.ReleaseStatus = model.QAReleaseStatusLearning
			}
			if _, err := tx.NamedExecContext(ctx, sql, qa); err != nil {
				log.ErrorContextf(ctx, "更新问答关联文档失败 sql:%s args:%+v err:%+v", sql, qa, err)
				return err
			}
			if err := d.deleteQASimilar(ctx, tx, qa); err != nil {
				log.ErrorContextf(ctx, "deleteQASimilar|更新问答关联文档失败 sql:%s args:%+v err:%+v", sql, qa, err)
				return err
			}
			if !isAuditOrAppealFail {
				id, err := d.addQASync(ctx, tx, qa)
				if err != nil {
					log.ErrorContextf(ctx, "addQASync|更新问答关联文档失败 sql:%s args:%+v err:%+v", sql, qa, err)
					return err
				}
				syncIDs = append(syncIDs, id)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		d.vector.Push(ctx, syncID)
	}
	return nil
}

// GetQAAndRelateDocs 获取QA和QA关联的文档
func (d *dao) GetQAAndRelateDocs(ctx context.Context, ids []uint64, robotID uint64) (
	map[uint64]*model.DocQA, map[uint64]*model.Doc, error) {
	qaMap := make(map[uint64]*model.DocQA)
	qaDocMap := make(map[uint64]*model.Doc)
	if len(ids) == 0 {
		return qaMap, qaDocMap, nil
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	querySQL := fmt.Sprintf(getQADetailByIds, qaFields, placeholder(len(ids)))
	list := make([]*model.DocQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取QA详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, nil, err
	}
	qaRelateDocIDMaps := make(map[uint64]uint64)
	qaRelateDocIds := make([]uint64, 0)
	for _, item := range list {
		qaMap[item.ID] = item
		if item.DocID > 0 {
			qaRelateDocIDMaps[item.ID] = item.DocID
			qaRelateDocIds = append(qaRelateDocIds, item.DocID)
		}
	}
	docs, err := d.GetDocByIDs(ctx, slicex.Unique(qaRelateDocIds), robotID)
	if err != nil {
		return nil, nil, err
	}
	for key, value := range qaRelateDocIDMaps {
		qaDocMap[key] = docs[value]
	}
	return qaMap, qaDocMap, nil
}

// DeleteQAsCharSizeExceeded 删除超量失效过期的问答
func (d *dao) DeleteQAsCharSizeExceeded(ctx context.Context, corpID uint64, robotID uint64,
	reserveTime time.Duration) error {
	req := &model.QAListReq{
		CorpID:  corpID,
		RobotID: robotID,
		ReleaseStatus: []uint32{
			model.QAReleaseStatusCharExceeded,
		},
		Page:     1,
		PageSize: 1000,
	}
	qas, err := d.GetQAList(ctx, req)
	if err != nil {
		return err
	}
	exTimeoutQAs := make([]*model.DocQA, 0, len(qas))
	for _, qa := range qas {
		if time.Now().Before(qa.UpdateTime.Add(reserveTime)) {
			continue
		}
		exTimeoutQAs = append(exTimeoutQAs, qa)
	}
	if len(exTimeoutQAs) == 0 {
		return nil
	}
	return d.DeleteQAs(ctx, corpID, robotID, 0, exTimeoutQAs)
}

// GetVideoURLsCharSize 从html中提取视频链接返回字符数
func (d *dao) GetVideoURLsCharSize(ctx context.Context, htmlStr string) (int, error) {
	if htmlStr == "" {
		return 0, nil
	}
	var fileSizeCount int64
	var fileCharSize int
	videos, err := util.AuditQaVideoURLs(ctx, htmlStr)
	if err != nil {
		return fileCharSize, err
	}
	if len(videos) == 0 {
		return 0, nil
	}
	for _, videoUrl := range videos {
		objectInfo, err := d.GetCosFileInfoByUrl(ctx, videoUrl.CosURL)
		if err != nil {
			return fileCharSize, err
		}
		if objectInfo != nil {
			fileSizeCount += objectInfo.Size
		}
	}
	log.InfoContextf(ctx, "getVideoURLsCharSize|len(files)|%d|fileSizeCount|%d", len(videos), fileSizeCount)
	if len(videos) > 0 && fileSizeCount > 0 {
		fileCharSize = util.ConvertBytesToChars(ctx, fileSizeCount)
		log.InfoContextf(ctx, "getVideoURLsCharSize|ConvertBytesToChars|%d", fileCharSize)
		return fileCharSize, nil
	}
	return 0, nil
}

// GetCosFileInfoByUrl 根据cos_url获取cos文件信息
func (d *dao) GetCosFileInfoByUrl(ctx context.Context, cosUrl string) (*model.ObjectInfo, error) {
	u, err := url.Parse(cosUrl)
	if err != nil {
		return nil, err
	}
	if u.Host != config.App().Storage.VideoDomain {
		log.WarnContextf(ctx, "GetCosFileInfoByUrl|Path:%s != VideoDomain:%s",
			u.Host, config.App().Storage.VideoDomain)
		return nil, errs.ErrVideoURLFail
	}
	// 去掉前面的斜线
	path := strings.TrimPrefix(u.Path, "/")
	log.InfoContextf(ctx, "GetCosFileInfoByUrl|Path:%s", path)
	objectInfo, err := d.StatObject(ctx, path)
	if err != nil || objectInfo == nil {
		log.ErrorContextf(ctx, "GetCosFileInfoByUrl|StatObject:%+v err:%v", objectInfo, err)
		return nil, err
	}
	log.InfoContextf(ctx, "GetCosFileInfoByUrl|StatObject:%+v", objectInfo)
	return objectInfo, nil
}

// CreateQaAudit 创建问答送审任务
func (d *dao) CreateQaAudit(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA) error {
	if err := d.createAudit(ctx, model.AuditSendParams{
		CorpID: qa.CorpID, StaffID: qa.StaffID, RobotID: qa.RobotID, Type: model.AuditBizTypeQa,
		RelateID: qa.ID, EnvSet: getEnvSet(ctx),
	}); err != nil {
		log.ErrorContextf(ctx, "创建问答送审任务失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateQaAuditForExcel2Qa 批量导入问答时，创建问答送审任务
func (d *dao) CreateQaAuditForExcel2Qa(ctx context.Context, doc *model.Doc) error {
	if err := d.createAuditForExcel2Qa(ctx, model.AuditSendParams{
		CorpID: doc.CorpID, StaffID: doc.StaffID, RobotID: doc.RobotID, Type: model.AuditBizTypeQa,
		RelateID: 0, EnvSet: getEnvSet(ctx), ParentRelateID: doc.ID,
	}); err != nil {
		log.ErrorContextf(ctx, "批量导入问答时，创建问答送审任务失败 err:%+v", err)
		return err
	}
	return nil
}
