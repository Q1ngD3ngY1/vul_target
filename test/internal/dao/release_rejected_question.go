package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	releaseRejectedQuestionFields = `id,corp_id,robot_id,create_staff_id,version_id,rejected_question_id,question,
			release_status,message,is_deleted,action,update_time,create_time`

	createReleaseRejectedQuestion = `
		INSERT INTO
			t_release_rejected_question(%s)
		VALUES
			(null,:corp_id,:robot_id,:create_staff_id,:version_id,:rejected_question_id,:question,:release_status,
			:message,:is_deleted,:action,:update_time,:create_time)
	`
	getReleaseModifyRejectedQuestionByVersion = `
		SELECT
			%s
		FROM
			t_release_rejected_question
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND version_id = ? %s
	`
	getReleaseRejectedQuestionsCount = `
		SELECT
			count(*) total
		FROM
			t_release_rejected_question
		WHERE
			corp_id = ?
			AND robot_id = ? %s
	`
	getReleaseModifyRejectedQuestion = `
		SELECT
			*
		FROM
			t_release_rejected_question
		WHERE
			corp_id = ?
			AND robot_id = ?  %s
		LIMIT 
			?,?
	`
)

// GetModifyRejectedQuestionCount 发布拒答问题预览数量
func (d *dao) GetModifyRejectedQuestionCount(ctx context.Context, corpID, robotID, versionID uint64,
	question string, releaseStatuses []uint32) (uint64, error) {
	args := make([]any, 0, 4+len(releaseStatuses))
	args = append(args, corpID, robotID)
	condition := ""
	if question != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(question)))
	}

	if versionID != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND version_id = ? ")
		args = append(args, versionID)
	}

	if len(releaseStatuses) > 0 {
		condition = fmt.Sprintf("%s AND release_status IN (%s)", condition, placeholder(len(releaseStatuses)))
		for _, releaseStatus := range releaseStatuses {
			args = append(args, releaseStatus)
		}
	}

	var total uint64
	SQL := fmt.Sprintf(getReleaseRejectedQuestionsCount, condition)

	if err := d.db.Get(ctx, &total, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询拒答问题数量失败 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return 0, err
	}

	return total, nil
}

// GetModifyRejectedQuestionList 发布拒答问题预览列表
func (d *dao) GetModifyRejectedQuestionList(ctx context.Context, corpID, robotID, versionID uint64, question string,
	page, pageSize uint32) ([]*model.ReleaseRejectedQuestion, error) {
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID)
	condition := ""
	if question != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(question)))
	}

	if versionID != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND version_id = ? ")
		args = append(args, versionID)
	}

	start := (page - 1) * pageSize
	args = append(args, start, pageSize)

	SQL := fmt.Sprintf(getReleaseModifyRejectedQuestion, condition)
	releaseRejectedQuestion := make([]*model.ReleaseRejectedQuestion, 0)
	if err := d.db.QueryToStructs(ctx, &releaseRejectedQuestion, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布拒答问题预览列表失败 SQL:%s, args:%+v, err:%+v", SQL, args, err)
		return releaseRejectedQuestion, err
	}

	return releaseRejectedQuestion, nil
}

func (d *dao) GetReleaseModifyRejectedQuestion(ctx context.Context, release *model.Release,
	rejectedQuestion []*model.RejectedQuestion) (map[uint64]*model.ReleaseRejectedQuestion, error) {
	args := make([]any, 0, 3+len(rejectedQuestion))
	args = append(args, release.CorpID, release.RobotID, release.ID)
	condition := "AND 1=1"
	if len(rejectedQuestion) > 0 {
		condition = fmt.Sprintf("AND rejected_question_id IN (%s)", placeholder(len(rejectedQuestion)))
		for _, item := range rejectedQuestion {
			args = append(args, item.ID)
		}
	}

	SQL := fmt.Sprintf(getReleaseModifyRejectedQuestionByVersion, releaseRejectedQuestionFields, condition)
	list := make([]*model.ReleaseRejectedQuestion, 0)
	if err := d.db.QueryToStructs(ctx, &list, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动的拒答问题失败 SQL:%s args:%+v err:%+v", SQL, args, err)
		return nil, err
	}
	releaseRejectedQuestion := make(map[uint64]*model.ReleaseRejectedQuestion)
	for _, item := range list {
		releaseRejectedQuestion[item.RejectedQuestionID] = item
	}
	return releaseRejectedQuestion, nil
}

// GetReleaseRejectedQuestionByVersion 按 Version 版本获取拒答问题发布列表
func (d *dao) GetReleaseRejectedQuestionByVersion(ctx context.Context, corpID uint64, robotID uint64,
	versionID uint64) ([]*model.ReleaseRejectedQuestion, error) {
	SQL := fmt.Sprintf(getReleaseModifyRejectedQuestionByVersion, releaseRejectedQuestionFields, "")
	args := make([]any, 0, 3)
	args = append(args, corpID, robotID, versionID)
	releaseRejectedQuestion := make([]*model.ReleaseRejectedQuestion, 0)
	if err := d.db.QueryToStructs(ctx, &releaseRejectedQuestion, SQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本发布拒答问题列表失败 sql:%s args:%+v err:%+v", SQL, args, err)
		return nil, err
	}
	return releaseRejectedQuestion, nil
}
