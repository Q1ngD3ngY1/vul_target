package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"math"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	releaseRecordFields = `
		id,business_id,corp_id,robot_id,staff_id,description,status,create_time,update_time,message,
		total_count,success_count,pause_msg,callback_status
	`
	createRelease = `
		INSERT INTO 
		    t_release (%s) 
		VALUES 
		    (null,:business_id,:corp_id,:robot_id,:staff_id,:description,:status,:create_time,:update_time,
			:message,:total_count,:success_count,:pause_msg,:callback_status)
	`
	getReleaseCount = `
		SELECT 
			count(*)
		FROM 
		    t_release 
		WHERE 
		    corp_id = ? AND robot_id = ?
	`
	getRelease = `
		SELECT 
			%s 
		FROM 
		    t_release 
		WHERE 
		    robot_id = ? AND id = ? 
		LIMIT 1
	`
	getReleaseList = `
		SELECT 
			%s 
		FROM 
		    t_release 
		WHERE 
		    corp_id = ? AND robot_id = ? 
		ORDER BY 
		    update_time DESC 
		LIMIT ?,?
	`
	getLatestRelease = `
		SELECT 
			%s 
		FROM 
		    t_release 
		WHERE 
		    corp_id = ? AND robot_id = ? 
		ORDER BY 
		    id DESC 
		LIMIT 1
	`
	getWaitRelease = `
		SELECT 
			%s 
		FROM 
		    t_release 
		WHERE 
		    status = ? 
		LIMIT ?
	`
	releaseOneRecord = `
		UPDATE 
		    t_release 
		SET 
		    status = :status, 
		    update_time = :update_time, 
		    total_count = :total_count, 
		    success_count = :success_count,
		    callback_status = :callback_status
		WHERE 
		    id = :id AND status = %d
	`
	updateReleaseStatus = `
		UPDATE 
		    t_release 
		SET 
		    status = :status, message = :message, update_time = :update_time, pause_msg = :pause_msg  
		WHERE 
		    id = :id 
	`
	countRelease = `
		UPDATE 
		    t_release 
		SET 
		    update_time = :update_time, total_count = :total_count, success_count = :success_count   
		WHERE 
		    id = :id 
	`
	getReleaseID = `
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
	getReleaseByID = `
		SELECT 
			%s 
		FROM 
		    t_release 
		WHERE 
			id = ?
    `
	getReleaseByBizID = `
		SELECT 
			%s 
		FROM 
		    t_release 
		WHERE 
			business_id = ?
    `
	startRelease = `
		UPDATE 
			t_release 
		SET 
		    callback_status = :callback_status,
		    status = :status,
		    update_time = :update_time 
		WHERE 
		    id = :id
	`
	updateReleaseCallbackStatus = `
		UPDATE 
		    t_release 
		SET 
		    callback_status = ?  
		WHERE 
		    id = ? AND callback_status = ? 
	`
)

// CreateRelease 创建发布记录
func (d *dao) CreateRelease(ctx context.Context, record *model.Release, previewJSON string) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := fmt.Sprintf(createRelease, releaseRecordFields)
		res, err := tx.NamedExecContext(ctx, querySQL, record)
		if err != nil {
			log.ErrorContextf(ctx, "创建发布记录失败 sql:%s args:%+v err:%+v", querySQL, record, err)
			return err
		}
		id, _ := res.LastInsertId()
		record.ID = uint64(id)
		if err = d.createReleaseConfigHistory(ctx, &model.RobotConfigHistory{
			RobotID:     record.RobotID,
			VersionID:   record.ID,
			ReleaseJSON: previewJSON,
			IsRelease:   false,
			CreateTime:  time.Now(),
			UpdateTime:  time.Now(),
		}); err != nil {
			log.ErrorContextf(ctx, "创建应用配置发布记录失败 err:%+v", err)
			return err
		}
		if err = newReleaseCollectTask(ctx, record.RobotID, model.ReleaseCollectParams{
			CorpID:    record.CorpID,
			StaffID:   record.StaffID,
			RobotID:   record.RobotID,
			VersionID: record.ID,
			EnvSet:    getEnvSet(ctx),
		}); err != nil {
			log.ErrorContextf(ctx, "创建发布采集任务失败 err:%+v", err)
			return err
		}
		if err = d.sendNotifyReleasing(ctx, tx, record); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建发布记录失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateReleaseDetail 采集发布记录详情
func (d *dao) CreateReleaseDetail(ctx context.Context, record *model.Release,
	releaseDoc []*model.ReleaseDoc, releaseQA []*model.ReleaseQA, releaseSegments []*model.ReleaseSegment,
	releaseRejectedQuestions []*model.ReleaseRejectedQuestion, releaseConfig []*model.ReleaseConfig) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		record.UpdateTime = now
		querySQL := startRelease
		if _, err := tx.NamedExecContext(ctx, querySQL, record); err != nil {
			log.ErrorContextf(ctx, "开始发布失败 sql:%s args:%+v err:%+v", querySQL, record, err)
			return err
		}
		if err := d.batchCreateReleaseDoc(ctx, tx, releaseDoc); err != nil {
			return err
		}
		if err := d.batchCreateReleaseQA(ctx, tx, releaseQA); err != nil {
			return err
		}
		if err := d.batchCreateReleaseSegment(ctx, tx, releaseSegments); err != nil {
			return err
		}
		if err := d.batchCreateReleaseRejectedQuestion(ctx, tx, record, releaseRejectedQuestions); err != nil {
			return err
		}
		if err := d.batchCreateReleaseConfig(ctx, tx, releaseConfig); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "采集发布记录详情失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateReleaseConfigDetail 采集发布配置记录详情
func (d *dao) CreateReleaseConfigDetail(ctx context.Context, record *model.Release,
	releaseConfig []*model.ReleaseConfig) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		record.UpdateTime = now
		querySQL := startRelease
		if _, err := tx.NamedExecContext(ctx, querySQL, record); err != nil {
			log.ErrorContextf(ctx, "开始发布失败 sql:%s args:%+v err:%+v", querySQL, record, err)
			return err
		}
		if err := d.batchCreateReleaseConfig(ctx, tx, releaseConfig); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "采集发布记录详情失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) batchCreateReleaseDoc(ctx context.Context, tx *sqlx.Tx, releaseDoc []*model.ReleaseDoc) error {
	now := time.Now()
	total := len(releaseDoc)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		tmpReleaseDocs := releaseDoc[start:end]
		querySQL := fmt.Sprintf(createReleaseDoc, releaseDocFields)
		if _, err := tx.NamedExecContext(ctx, querySQL, tmpReleaseDocs); err != nil {
			log.ErrorContextf(ctx, "保存发布文档失败 sql:%s err:%+v", querySQL, err)
			return err
		}
		args := make([]any, 0, 2+len(tmpReleaseDocs))
		args = append(args, model.DocStatusReleasing, now)
		querySQL = fmt.Sprintf(updateDocReleasing, placeholder(len(tmpReleaseDocs)))
		for _, doc := range tmpReleaseDocs {
			args = append(args, doc.DocID)
		}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "更新文档发布状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
	}
	return nil
}

func (d *dao) batchCreateReleaseQA(ctx context.Context, tx *sqlx.Tx, releaseQA []*model.ReleaseQA) error {
	now := time.Now()
	total := len(releaseQA)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		tmpReleaseQAs := releaseQA[start:end]
		querySQL := fmt.Sprintf(createReleaseQA, releaseQAFields)
		if _, err := tx.NamedExecContext(ctx, querySQL, tmpReleaseQAs); err != nil {
			log.ErrorContextf(ctx, "保存发布QA失败 sql:%s err:%+v", querySQL, err)
			return err
		}
		args := make([]any, 0, 2+len(tmpReleaseQAs))
		args = append(args, now, model.QAReleaseStatusIng)
		querySQL = fmt.Sprintf(batchUpdateQAReleaseStatus, placeholder(len(tmpReleaseQAs)))
		for _, qa := range tmpReleaseQAs {
			args = append(args, qa.QAID)
		}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "更新QA发布状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
	}
	return nil
}

func (d *dao) batchCreateReleaseSegment(
	ctx context.Context, tx *sqlx.Tx, releaseSegments []*model.ReleaseSegment) error {
	now := time.Now()
	total := len(releaseSegments)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		tmpReleaseSegs := releaseSegments[start:end]
		querySQL := fmt.Sprintf(createReleaseSegment, releaseSegmentFields)
		if _, err := tx.NamedExecContext(ctx, querySQL, tmpReleaseSegs); err != nil {
			log.ErrorContextf(ctx, "批量创建发布的segment失败 sql:%s seg:%+v err:%+v", querySQL,
				releaseSegments[start:end], err)
			return err
		}
		args := make([]any, 0, 2+len(tmpReleaseSegs))
		args = append(args, now, model.SegmentReleaseStatusIng)
		querySQL = fmt.Sprintf(batchUpdateSegmentReleaseStatus, placeholder(len(tmpReleaseSegs)))
		for _, seg := range tmpReleaseSegs {
			args = append(args, seg.SegmentID)
		}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "更新segment发布状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
	}
	return nil
}

func (d *dao) batchCreateReleaseRejectedQuestion(ctx context.Context, tx *sqlx.Tx, record *model.Release,
	releaseRejectedQuestions []*model.ReleaseRejectedQuestion) error {
	now := time.Now()
	for _, rejectedQuestion := range releaseRejectedQuestions {
		rejectedQuestion.VersionID = record.ID
		rejectedQuestion.CreateTime = now
		rejectedQuestion.UpdateTime = now
	}
	total := len(releaseRejectedQuestions)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		tmpReleaseRejectedQuestions := releaseRejectedQuestions[start:end]
		SQL := fmt.Sprintf(createReleaseRejectedQuestion, releaseRejectedQuestionFields)
		if _, err := tx.NamedExecContext(ctx, SQL, tmpReleaseRejectedQuestions); err != nil {
			log.ErrorContextf(ctx, "批量创建发布的拒答任务失败 SQL:%s tmpReleaseRejectedQuestions:%+v err:%+v",
				SQL, tmpReleaseRejectedQuestions, err)
			return err
		}
		args := make([]any, 0, 2+len(tmpReleaseRejectedQuestions))
		args = append(args, now, model.ReleaseRejectedQuestionReleaseStatusIng)
		SQL = fmt.Sprintf(updateRejectedQuestionReleaseStatus, placeholder(len(tmpReleaseRejectedQuestions)))
		for _, releaseRejectedQuestion := range tmpReleaseRejectedQuestions {
			args = append(args, releaseRejectedQuestion.RejectedQuestionID)
		}
		if _, err := tx.ExecContext(ctx, SQL, args...); err != nil {
			log.ErrorContextf(ctx, "更新拒答问题发布状态失败 SQL:%s args:%+v err:%+v", SQL, args, err)
			return err
		}
	}
	return nil
}

// batchCreateReleaseConfig 批量创建配置发布记录
func (d *dao) batchCreateReleaseConfig(ctx context.Context, tx *sqlx.Tx, releaseConfig []*model.ReleaseConfig) error {
	total := len(releaseConfig)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		tmpReleaseQAs := releaseConfig[start:end]
		querySQL := fmt.Sprintf(createReleaseConfig, releaseConfigField)
		if _, err := tx.NamedExecContext(ctx, querySQL, tmpReleaseQAs); err != nil {
			log.ErrorContextf(ctx, "保存发布配置项失败 sql:%s,args:%+v,err:%+v", querySQL,
				tmpReleaseQAs, err)
			return err
		}
	}
	return nil
}

// CreateReleaseAudit 创建发布送审
func (d *dao) CreateReleaseAudit(ctx context.Context, r *model.Release, envSet string) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := d.createAudit(ctx, model.AuditSendParams{
			CorpID:   r.CorpID,
			RobotID:  r.RobotID,
			StaffID:  r.StaffID,
			Type:     model.AuditBizTypeRelease,
			RelateID: r.ID,
			EnvSet:   envSet,
		}); err != nil {
			log.ErrorContextf(ctx, "创建发布送审任务失败 err:%+v", err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建发布送审失败 err:%+v", err)
		return err
	}
	return nil
}

// NotifyReleaseSuccess 通知发布成功
func (d *dao) NotifyReleaseSuccess(ctx context.Context, release *model.Release) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := newReleaseSuccessTask(ctx, release.RobotID, model.ReleaseSuccessParams{
			CorpID:    release.CorpID,
			StaffID:   release.StaffID,
			RobotID:   release.RobotID,
			VersionID: release.ID,
		}); err != nil {
			return err
		}
		querySQL := updateReleaseStatus
		release.Status = model.ReleaseStatusSuccessCallback
		release.UpdateTime = time.Now()
		release.Message = ""
		release.PauseMsg = ""
		if _, err := tx.NamedExecContext(ctx, querySQL, release); err != nil {
			log.ErrorContextf(ctx, "发布回调处理中失败 sql:%s args:%+v err:%+v", querySQL, release, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "通知发布成功失败 err:%+v", err)
		return err
	}
	return nil
}

// ReleasePause 发布暂停
func (d *dao) ReleasePause(ctx context.Context, release *model.Release) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := updateReleaseStatus
		release.Status = model.ReleaseStatusPause
		release.UpdateTime = time.Now()
		release.Message = i18nkey.KeyReleasePause
		if _, err := tx.NamedExecContext(ctx, querySQL, release); err != nil {
			log.ErrorContextf(ctx, "发布暂停处理中失败 sql:%s args:%+v err:%+v", querySQL, release, err)
			return err
		}
		if err := d.sendNoticeReleasePause(ctx, tx, release); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "发布暂停处理中失败 err:%+v", err)
		return err
	}

	return nil
}

// ReleaseSuccess 发布成功
func (d *dao) ReleaseSuccess(ctx context.Context, appDB *model.AppDB, release *model.Release, qaIDs,
	segmentIDs, rejectedQuestionIDs, forbidReleaseQAIDs []uint64, configAuditPass,
	configAuditFail []*model.ReleaseConfig, releaseDoc []*model.ReleaseDoc, docs []*model.Doc) error {
	now := time.Now()
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := updateReleaseStatus
		release.Status = model.ReleaseStatusSuccess
		release.UpdateTime = now
		release.Message = ""
		release.PauseMsg = ""
		if _, err := tx.NamedExecContext(ctx, querySQL, release); err != nil {
			log.ErrorContextf(ctx, "发布成功-更新release失败 sql:%s args:%+v err:%+v", querySQL, release,
				err)
			return err
		}
		if err := d.releaseQASuccess(ctx, tx, qaIDs); err != nil {
			return err
		}
		if err := d.releaseSegmentSuccess(ctx, tx, segmentIDs); err != nil {
			return err
		}
		if err := d.forbidReleaseQASuccess(ctx, tx, forbidReleaseQAIDs); err != nil {
			return err
		}
		if err := d.releaseDocSuccess(ctx, tx, releaseDoc, docs); err != nil {
			return err
		}
		if err := d.releaseRejectedQuestion(ctx, tx, rejectedQuestionIDs, now); err != nil {
			return err
		}
		if err := d.releaseConfig(ctx, tx, configAuditPass, configAuditFail, now, release, appDB); err != nil {
			return err
		}
		// 统计总数和成功条数
		vectorCount, timeOutNum, err := d.countRelease(ctx, tx, release)
		if err != nil {
			log.ErrorContextf(ctx, "发布成功-统计发布总数和失败数失败：err:%+v", err)
			return err
		}
		if vectorCount != 0 {
			querySQL = updateAppQAVersion
			args := make([]any, 0, 3)
			args = append(args, release.ID, now, release.RobotID)
			if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "发布成功-更新机器人版本失败 sql:%s args:%+v err:%+v", querySQL, args, err)
				return err
			}
		}
		if release.SuccessCount == release.TotalCount && release.SuccessCount > 0 && release.TotalCount > 0 {
			// 发布全部成功
			if err := d.sendNoticeReleaseSuccess(ctx, tx, release, appDB); err != nil {
				return err
			}
		} else {
			// 部分成功，部分失败
			if err := d.sendNoticeReleasePartially(ctx, tx, release, timeOutNum); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "发布成功失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) releaseQASuccess(ctx context.Context, tx *sqlx.Tx, qaIDs []uint64) error {
	total := len(qaIDs)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	now := time.Now()
	for i := 0; i < pages; i++ {
		start := i * pageSize
		end := (i + 1) * pageSize
		if end > total {
			end = total
		}
		tmpIDs := qaIDs[start:end]
		args := make([]any, 0, len(tmpIDs)+2)
		args = append(args, model.NextActionAdd, now)
		for _, id := range tmpIDs {
			args = append(args, id)
		}
		querySQL := fmt.Sprintf(updateQALastAction, placeholder(len(tmpIDs)))
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "发布成功-更新QA失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
	}
	return nil
}

func (d *dao) releaseSegmentSuccess(ctx context.Context, tx *sqlx.Tx, segmentIDs []uint64) error {
	total := len(segmentIDs)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	now := time.Now()
	for i := 0; i < pages; i++ {
		start := i * pageSize
		end := (i + 1) * pageSize
		if end > total {
			end = total
		}
		tmpIDs := segmentIDs[start:end]
		args := make([]any, 0, len(tmpIDs)+2)
		args = append(args, model.SegNextActionAdd, now)
		for _, id := range tmpIDs {
			args = append(args, id)
		}
		querySQL := fmt.Sprintf(updateSegmentLastAction, placeholder(len(tmpIDs)))
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "发布成功-更新Segment失败 sql:%s args:%+v err:%+v", querySQL, args,
				err)
			return err
		}
	}
	return nil
}

func (d *dao) forbidReleaseQASuccess(ctx context.Context, tx *sqlx.Tx, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	querySQL := fmt.Sprintf(batchUpdateQAReleaseStatus, placeholder(len(ids)))
	args := make([]any, 0, 2+len(ids))
	args = append(args, time.Now(), model.QAReleaseStatusInit)
	for _, id := range ids {
		args = append(args, id)
	}
	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "恢复禁止发布的问答发布状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// releaseDocSuccess 文档发布成功，由于vector_doc在ReleaseDetailNotify通知时，只会通知segment成败，所以在ReleaseNotify中处理文档成败
func (d *dao) releaseDocSuccess(
	ctx context.Context, tx *sqlx.Tx, releaseDocs []*model.ReleaseDoc, docs []*model.Doc) error {
	if len(releaseDocs) == 0 || len(docs) == 0 {
		return nil
	}
	now := time.Now()
	querySQL := updateDocReleaseSuccess
	for _, doc := range docs {
		doc.UpdateTime = now
		doc.Status = model.DocStatusReleaseSuccess
		doc.NextAction = model.DocNextActionPublish
		if doc.HasDeleted() {
			doc.NextAction = model.DocNextActionAdd
		}
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "更新文档发布成功失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
			return err
		}
	}
	querySQL = updateReleaseDocSuccess
	for _, doc := range releaseDocs {
		doc.Status = model.DocStatusReleaseSuccess
		doc.UpdateTime = now
		if _, err := tx.NamedExecContext(ctx, querySQL, doc); err != nil {
			log.ErrorContextf(ctx, "更新文档发布成功失败 sql:%s args:%+v err:%+v", querySQL, doc, err)
			return err
		}
	}
	return nil
}

// sendNotifyReleasing 通知发布中
func (d *dao) sendNotifyReleasing(ctx context.Context, tx *sqlx.Tx, record *model.Release) error {
	operations := make([]model.Operation, 0)
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeWaitReleasePageID),
		model.WithLevel(model.LevelInfo),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyKnowledgeBasePublishing)),
		model.WithForbidCloseFlag(),
	}
	notice := model.NewNotice(model.NoticeTypeRelease, record.ID, record.CorpID,
		record.RobotID, record.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// sendNoticeReleaseSuccess 发布成功通知
func (d *dao) sendNoticeReleaseSuccess(ctx context.Context, tx *sqlx.Tx,
	release *model.Release, appDB *model.AppDB) error {
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{VersionID: release.ID}}}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeWaitReleasePageID),
		model.WithLevel(model.LevelSuccess),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyAppPublishSuccess)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyAppPublishSuccessWithName, appDB.Name)),
	}
	notice := model.NewNotice(model.NoticeTypeRelease, release.ID, release.CorpID, release.RobotID, release.StaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// sendNoticeReleasePause 发布暂停通知
func (d *dao) sendNoticeReleasePause(ctx context.Context, tx *sqlx.Tx, release *model.Release) error {
	operations := []model.Operation{
		{Typ: model.OpTypeViewDetail, Params: model.OpParams{VersionID: release.BusinessID}},
		{Typ: model.OpTypeRetryRelease, Params: model.OpParams{VersionID: release.BusinessID}},
	}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeWaitReleasePageID),
		model.WithLevel(model.LevelWarning),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyAppPublishOnlinePause)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyAppPublishOnlinePauseNetworkTimeout)),
	}
	notice := model.NewNotice(model.NoticeTypeRelease, release.ID, release.CorpID, release.RobotID, release.StaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// sendNoticeReleasePartially 发布部分成功
func (d *dao) sendNoticeReleasePartially(ctx context.Context, tx *sqlx.Tx, release *model.Release,
	timeOutNum uint64) error {
	operations := []model.Operation{
		{Typ: model.OpTypeViewDetail, Params: model.OpParams{VersionID: release.BusinessID}},
		{Typ: model.OpTypeAppeal, Params: model.OpParams{VersionID: release.BusinessID,
			AppealType: model.AuditBizTypeRelease}},
	}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeWaitReleasePageID),
		model.WithLevel(model.LevelWarning),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyAppPublishPartialSuccess)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeySuccessFailureCountWithReason+
			utils.When((release.TotalCount-release.SuccessCount) == timeOutNum, i18n.Translate(ctx, i18nkey.KeyReviewTimeout), i18n.Translate(ctx, i18nkey.KeyReviewFailed)),
			release.SuccessCount,
			release.TotalCount-release.SuccessCount)),
	}
	notice := model.NewNotice(model.NoticeTypeRelease, release.ID, release.CorpID, release.RobotID, release.StaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// sendReleaseFailNotice 发布失败通知
func (d *dao) sendReleaseFailNotice(ctx context.Context, tx *sqlx.Tx, record *model.Release,
	appDB *model.AppDB, failStr string) error {
	operations := []model.Operation{
		{Typ: model.OpTypeViewDetail, Params: model.OpParams{VersionID: record.BusinessID}},
		{Typ: model.OpTypeAppeal, Params: model.OpParams{VersionID: record.BusinessID,
			AppealType: model.AuditBizTypeRelease}},
	}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeWaitReleasePageID),
		model.WithLevel(model.LevelError),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyAppPublishFailure)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyAppPublishFailureWithNameAndReason, appDB.Name, failStr)),
	}
	notice := model.NewNotice(model.NoticeTypeRelease, record.ID, record.CorpID, record.RobotID, record.StaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

func (d *dao) noticeReleaseAuditFailConfig(ctx context.Context, tx *sqlx.Tx,
	release *model.Release, content string) error {
	operations := []model.Operation{
		{Typ: model.OpTypeAppeal, Params: model.OpParams{VersionID: release.BusinessID,
			AppealType: model.AuditBizTypeRelease}},
	}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeWaitReleasePageID),
		model.WithLevel(model.LevelWarning),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyConfigurationItemPublishFailure)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyConfigurationItemContentReviewFailureWithParam, content)),
	}
	notice := model.NewNotice(model.NoticeTypeRelease, release.ID, release.CorpID, release.RobotID, release.StaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

func (d *dao) releaseRejectedQuestion(ctx context.Context, tx *sqlx.Tx,
	rejectedQuestionIDs []uint64, now time.Time) error {
	if len(rejectedQuestionIDs) == 0 {
		return nil
	}
	querySQL := fmt.Sprintf(updateRejectedQuestionRelease, placeholder(len(rejectedQuestionIDs)))
	args := make([]any, 0, len(rejectedQuestionIDs)+2)
	args = append(args, model.RejectedQuestionReleaseStatusSuccess, now)
	for _, id := range rejectedQuestionIDs {
		args = append(args, id)
	}
	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新拒答问题发布失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// GetReleaseRecord 获取发布记录
func (d *dao) GetReleaseRecord(ctx context.Context, robotID, id uint64) (*model.Release, error) {
	args := make([]any, 0, 2)
	args = append(args, robotID, id)
	querySQL := fmt.Sprintf(getRelease, releaseRecordFields)
	records := make([]*model.Release, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据向量库版本获取发布记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

// GetReleaseRecords 获取发布记录
func (d *dao) GetReleaseRecords(ctx context.Context, corpID, robotID uint64, page, pageSize uint32) (
	uint64, []*model.Release, error) {
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID)
	querySQL := getReleaseCount
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布记录总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL = fmt.Sprintf(getReleaseList, releaseRecordFields)
	records := make([]*model.Release, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	return total, records, nil
}

// GetLatestRelease 获取最近一次正式发布记录
func (d *dao) GetLatestRelease(ctx context.Context, corpID, robotID uint64) (*model.Release, error) {
	args := make([]any, 0, 2)
	args = append(args, corpID, robotID)
	querySQL := fmt.Sprintf(getLatestRelease, releaseRecordFields)
	records := make([]*model.Release, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取最近一次正式发布记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

// GetWaitRelease 获取待发布记录
func (d *dao) GetWaitRelease(ctx context.Context, pageSize uint32) ([]*model.Release, error) {
	args := make([]any, 0, 2)
	args = append(args, model.ReleaseStatusInit, pageSize)
	querySQL := getWaitRelease
	records := make([]*model.Release, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取待发布记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return records, nil
}

// DoReleaseAfterAudit 执行发布记录
func (d *dao) DoReleaseAfterAudit(
	ctx context.Context, tx *sqlx.Tx, record *model.Release, auditStat map[uint32]*model.AuditResultStat,
	cfgAuditStat map[uint32]*model.AuditResultStat, robot *model.AppDB) error {
	if !record.IsAudit() {
		return nil
	}
	if _, ok := auditStat[model.ReleaseQAAuditStatusInit]; ok { // 存在未送审数据
		return nil
	}
	if _, ok := auditStat[model.ReleaseQAAuditStatusDoing]; ok { // 存在审核中数据
		return nil
	}
	if _, ok := cfgAuditStat[model.ConfigReleaseStatusAuditing]; ok { // 存在审核中数据
		return nil
	}
	qaAllAuditFail, configAllAuditFail, isAllAuditFail := checkAuditResultStat(auditStat, cfgAuditStat)
	releaseSegCount, err := d.GetModifySegmentCount(ctx, record.RobotID, record.ID, 0)
	if err != nil {
		return err
	}
	rejectedQuestionCount, err := d.GetModifyRejectedQuestionCount(ctx, record.CorpID, record.RobotID, record.ID,
		"", nil)
	if err != nil {
		return err
	}
	detail, err := d.GetDataSyncTaskDetail(ctx, record.RobotID, record.BusinessID, record.CorpID, record.StaffID)
	if err != nil {
		return err
	}
	configCount, err := d.GetModifyReleaseConfigCount(ctx, record.ID, nil, "")
	if err != nil {
		return err
	}
	isOnlyReleaseQA := releaseSegCount == 0 && rejectedQuestionCount == 0 && len(detail.Task.SyncItems) == 0
	isReleaseFail := isAllAuditFail && isOnlyReleaseQA
	if isAllAuditFail && record.CallbackStatus != model.ReleaseAllServeCallbackFlag &&
		releaseSegCount == 0 && rejectedQuestionCount == 0 {
		record.CallbackStatus = model.ReleaseVectorSuccessCallbackFlag
	}
	log.DebugContextf(ctx, "更新发布记录状态失败 isReleaseFail:%t", isReleaseFail)
	now := time.Now()
	record.Status = model.ReleaseStatusPending
	if isReleaseFail {
		if err = d.doReleaseFailRecord(ctx, tx, record, configCount); err != nil {
			return err
		}
	}
	if isOnlyReleaseQA && qaAllAuditFail && !configAllAuditFail {
		record.CallbackStatus = model.ReleaseAllServeCallbackFlag
	}
	record.UpdateTime = now
	querySQL := fmt.Sprintf(releaseOneRecord, model.ReleaseStatusAudit)
	res, err := tx.NamedExecContext(ctx, querySQL, record)
	if err != nil {
		log.ErrorContextf(ctx, "更新发布记录状态失败 sql:%s args:%+v err:%+v", querySQL, record, err)
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return nil
	}
	if err = d.ExecRelease(ctx, !qaAllAuditFail, !configAllAuditFail, isOnlyReleaseQA, record); err != nil {
		return err
	}
	if err = d.auditFailNotice(ctx, isReleaseFail, tx, record, robot); err != nil {
		return err
	}
	return nil
}

// doReleaseFailRecord 发布全部审核失败
func (d *dao) doReleaseFailRecord(ctx context.Context, tx *sqlx.Tx, record *model.Release, configCount uint64) error {
	record.Status = model.ReleaseStatusAuditFail
	// 用于统计总条数
	count, err := d.GetModifyQACount(ctx, record.RobotID, record.ID, "", nil, nil)
	if err != nil {
		log.ErrorContextf(ctx, "获取对话总条数失败：args:%+v err:%+v", record, err)
	}
	log.DebugContextf(ctx, "获取对话总条数为：args:%+v count:%+v", record, count)
	record.TotalCount = count + configCount
	record.SuccessCount = 0
	// 变更配置json快照
	cfg, err := d.GetConfigHistoryByVersionID(ctx, record.RobotID, record.ID)
	if err != nil {
		return err
	}
	appDB, err := d.GetAppByID(ctx, record.RobotID)
	if err != nil {
		return err
	}
	if appDB == nil {
		return errs.ErrRobotNotFound
	}
	if configCount > 0 {
		cfg.ReleaseJSON = appDB.ReleaseJSON
	}
	if err = d.updateConfigHistoryByVersionID(ctx, tx, record.RobotID, record.ID, cfg.ReleaseJSON); err != nil {
		return err
	}
	return nil
}

// ExecRelease 通知发布
func (d *dao) ExecRelease(ctx context.Context, isQaAllowExec, isCfgAllowExec, isOnlyReleaseQA bool,
	record *model.Release) error {
	if !isQaAllowExec && !isCfgAllowExec && isOnlyReleaseQA {
		// 全部发送失败不进行通知发布
		return nil
	}
	appDB, err := d.GetAppByID(ctx, record.RobotID)
	if err != nil {
		return err
	}
	switch record.CallbackStatus {
	case model.ReleaseVectorSuccessCallbackFlag:
		if _, err = d.SendDataSyncTask(ctx, appDB.ID, record.BusinessID, record.CorpID, record.StaffID,
			model.TaskConfigEventRelease); err != nil {
			return err
		}
		return nil
	case model.ReleaseTaskFlowSuccessCallbackFlag:
		if !isOnlyReleaseQA || (isOnlyReleaseQA && isQaAllowExec) {
			if _, err = d.Publish(
				ctx,
				record.RobotID,
				record.ID,
				fmt.Sprintf("%d.%d", record.RobotID, record.ID)); err != nil {
				return err
			}
			return nil
		}
	case model.ReleaseAllServeCallbackFlag:
		if err = newReleaseSuccessTask(ctx, record.RobotID, model.ReleaseSuccessParams{
			CorpID:    record.CorpID,
			StaffID:   record.StaffID,
			RobotID:   record.RobotID,
			VersionID: record.ID,
		}); err != nil {
			return err
		}
		return nil
	case model.ReleaseNoServeCallbackFlag:
		if !isOnlyReleaseQA || (isOnlyReleaseQA && isQaAllowExec) {
			if _, err = d.Publish(
				ctx,
				record.RobotID,
				record.ID,
				fmt.Sprintf("%d.%d", record.RobotID, record.ID)); err != nil {
				return err
			}
		}
		if _, err = d.SendDataSyncTask(ctx, appDB.ID, record.BusinessID, record.CorpID, record.StaffID,
			model.TaskConfigEventRelease); err != nil {
			return err
		}
	}
	return nil
}

// auditFailNotice 发布失败通知，QA全部审核不通过且配置全部审核不通过进行通知
func (d *dao) auditFailNotice(ctx context.Context, isReleaseFail bool, tx *sqlx.Tx,
	record *model.Release, appDB *model.AppDB) error {
	if !isReleaseFail {
		return nil
	}
	var notPassItems []string
	failStr := ""
	ids, err := d.GetForbidReleaseQA(ctx, record.ID)
	if err != nil {
		return err
	}
	if err = d.forbidReleaseQASuccess(ctx, tx, ids); err != nil {
		return err
	}
	num, err := d.GetAuditFailReleaseQA(ctx, record.ID, "")
	if err != nil {
		return err
	}
	if num > 0 {
		failStr += i18nkey.KeyKnowledgeBaseContent
	}
	items, err := d.GetConfigItemByVersionID(ctx, record.ID)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.AuditStatus == model.ConfigReleaseStatusAuditNotPass {
			notPassItems = append(notPassItems, item.ConfigItem)
		}
	}
	if len(notPassItems) > 0 {
		if len(failStr) > 0 {
			failStr += "、"
		}
		failStr += strings.Join(notPassItems, "、")
	}
	timeOutNum := uint64(0)
	if num > 0 {
		timeOutNum, err = d.GetAuditFailReleaseQA(ctx, record.ID, model.ReleaseQAMessageAuditTimeOut)
		if err != nil {
			return err
		}
	}
	if timeOutNum != 0 && timeOutNum == num {
		failStr = model.ReleaseQAMessageAuditTimeOut
	} else {
		failStr = failStr + i18nkey.KeyContainsSensitiveInformation
	}
	if err = d.sendReleaseFailNotice(ctx, tx, record, appDB, failStr); err != nil {
		return err
	}
	return nil
}

// GetLatestSuccessRelease 获取最后一次状态为发布成功的记录
func (d *dao) GetLatestSuccessRelease(ctx context.Context, corpID, robotID uint64) (*model.Release, error) {
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, model.ReleaseStatusSuccess)
	querySQL := getReleaseID
	records := make([]*model.Release, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取最后一次状态为发布成功的记录失败 sql:%s args:%+v err:%+v", querySQL, args,
			err)
		return nil, err
	}
	// 如果没有发布成功记录返回空结构体
	if len(records) == 0 {
		return &model.Release{}, nil
	}
	return records[0], nil
}

// GetReleaseByID 通过ID获取发布记录
func (d *dao) GetReleaseByID(ctx context.Context, id uint64) (*model.Release, error) {
	args := make([]any, 0, 1)
	args = append(args, id)
	querySQL := fmt.Sprintf(getReleaseByID, releaseRecordFields)
	records := make([]*model.Release, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取发布记录失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	// 如果没有发布成功记录返回空结构体
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

// GetReleaseByBizID 通过BizID获取发布记录
func (d *dao) GetReleaseByBizID(ctx context.Context, bizID uint64) (*model.Release, error) {
	args := make([]any, 0, 1)
	args = append(args, bizID)
	querySQL := fmt.Sprintf(getReleaseByBizID, releaseRecordFields)
	records := make([]*model.Release, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过BizID获取发布记录 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	// 如果没有发布成功记录返回空结构体
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

// UpdateReleaseCallbackStatus 更新发布通知回调情况
func (d *dao) UpdateReleaseCallbackStatus(ctx context.Context, release *model.Release, oldStatus uint32) error {
	querySQL := updateReleaseCallbackStatus
	args := make([]any, 0, 3)
	args = append(args, release.CallbackStatus, release.ID, oldStatus)
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新发布通知回调情况失败 sql:%s args:%+v err:%+v", querySQL, release, err)
		return err
	}
	return nil
}

// countRelease 统计发布成功和失败数【统计维度：文档，问答，拒答，任务型】
func (d *dao) countRelease(ctx context.Context, tx *sqlx.Tx, release *model.Release) (uint64, uint64, error) {
	robotID := release.RobotID
	versionID := release.ID
	corpID := release.CorpID
	var qaSuccCount, qaFailCount, docCount, rejectQASuccCount, rejectQAFailCount,
		taskFlowSuccCount, taskFlowFailCount, configItemSuccCount, configItemFailCount, timeOutNum uint64
	var err error
	if qaSuccCount, err = d.GetModifyQACount(ctx, robotID, versionID, "", nil,
		[]uint32{model.QAReleaseStatusSuccess}); err != nil {
		log.ErrorContextf(ctx, "统计QA成功数失败:%+v,发布：%+v", err, release)
		return 0, 0, err
	}
	if qaFailCount, err = d.GetModifyQACount(ctx, robotID, versionID, "", nil,
		[]uint32{model.QAReleaseStatusFail}); err != nil {
		log.ErrorContextf(ctx, "统计QA失败数失败:%+v,发布：%+v", err, release)
		return 0, 0, err
	}
	if timeOutNum, err = d.GetAuditFailReleaseQA(ctx, versionID, model.ReleaseQAMessageAuditTimeOut); err != nil {
		return 0, 0, err
	}
	// 文档不会有失败的情况
	if docCount, err = d.GetModifyDocCount(ctx, robotID, versionID, "", nil, nil); err != nil {
		log.ErrorContextf(ctx, "统计文档总数失败:%+v,发布：%+v", err, release)
		return 0, 0, err
	}
	if rejectQASuccCount, err = d.GetModifyRejectedQuestionCount(ctx, corpID, robotID, versionID, "",
		[]uint32{model.RejectedQuestionReleaseStatusSuccess}); err != nil {
		log.ErrorContextf(ctx, "统计拒答成功数失败:%+v,发布：%+v", err, release)
		return 0, 0, err
	}
	if rejectQAFailCount, err = d.GetModifyRejectedQuestionCount(ctx, corpID, robotID, versionID, "",
		[]uint32{model.RejectedQuestionReleaseStatusFail}); err != nil {
		log.ErrorContextf(ctx, "统计拒答失败数失败:%+v,发布：%+v", err, release)
		return 0, 0, err
	}
	if configItemSuccCount, err = d.GetModifyReleaseConfigCount(ctx, versionID,
		[]uint32{model.ConfigReleaseStatusIng}, ""); err != nil {
		log.ErrorContextf(ctx, "统计配置项目成功数失败:%+v,发布：%+v", err, release)
		return 0, 0, err
	}
	if configItemFailCount, err = d.GetModifyReleaseConfigCount(ctx, versionID,
		[]uint32{model.ConfigReleaseStatusFail}, ""); err != nil {
		log.ErrorContextf(ctx, "统计配置项目失败数失败:%+v,发布：%+v", err, release)
		return 0, 0, err
	}
	detail, err := d.GetDataSyncTaskDetail(ctx, robotID, release.BusinessID, release.CorpID, release.StaffID)
	if err != nil {
		return 0, 0, err
	}
	taskFlowSuccCount = detail.Task.SuccessCount
	taskFlowFailCount = detail.Task.FailCount
	log.DebugContextf(ctx, "统计发布数量，QA成功：%d，QA失败：%d，文档总数：%d，"+
		"拒答成功：%d，拒答失败：%d，配置项成功：%d，配置项失败：%d",
		qaSuccCount, qaFailCount, docCount, rejectQASuccCount, rejectQAFailCount,
		configItemSuccCount, configItemFailCount)
	release.UpdateTime = time.Now()
	release.TotalCount = qaSuccCount + qaFailCount + docCount + rejectQASuccCount + rejectQAFailCount +
		taskFlowSuccCount + taskFlowFailCount + configItemSuccCount + configItemFailCount
	release.SuccessCount = qaSuccCount + docCount + rejectQASuccCount + taskFlowSuccCount + configItemSuccCount
	if _, err = tx.NamedExecContext(ctx, countRelease, release); err != nil {
		log.ErrorContextf(ctx, "发布成功-统计release失败 sql:%s args:%+v err:%+v", countRelease, release, err)
		return 0, 0, err
	}
	return docCount + rejectQASuccCount + qaSuccCount + rejectQASuccCount, timeOutNum, nil
}

func checkAuditResultStat(auditStat map[uint32]*model.AuditResultStat,
	cfgAuditStat map[uint32]*model.AuditResultStat) (bool, bool, bool) {
	qaAllAuditFail := true
	configAllAuditFail := true
	isAllAuditFail := true
	if _, ok := auditStat[model.ReleaseQAAuditStatusSuccess]; ok { // 存在审核成功数据
		qaAllAuditFail = false
		isAllAuditFail = false
	}
	if _, ok := cfgAuditStat[model.ReleaseConfigAuditStatusSuccess]; ok { // 存在审核成功数据
		configAllAuditFail = false
		isAllAuditFail = false
	}
	return qaAllAuditFail, configAllAuditFail, isAllAuditFail
}
