package release

import (
	"context"

	"gorm.io/gorm/clause"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) getReleaseRejectedQuestionDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseRejectedQuestion.WithContext(ctx).UnderlyingDB()
	}
	return d.mysql.TReleaseRejectedQuestion.WithContext(ctx).Debug().UnderlyingDB()
}

func (d *daoImpl) IsExistReleaseRejectedQuestion(ctx context.Context, filter *releaseEntity.ReleaseRejectedQuestionFilter) (bool, error) {
	/*
		`
				SELECT
					COUNT(1)
				FROM
					t_release_rejected_question
				WHERE
					robot_id = ? AND version_id = ? AND rejected_question_id = ?
			`
	*/
	tbl := d.mysql.TReleaseRejectedQuestion
	db := tbl.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		tbl.RobotID.Eq(filter.RobotID),
		tbl.RejectedQuestionID.Eq(filter.RejectedQuestionID),
		tbl.VersionID.Eq(filter.VersionId),
	}

	total, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "IsExistReleaseRejectedQuestion data req:%+v, error:%v", filter, err)
		return false, err
	}
	return total > 0, nil
}

func (d *daoImpl) CreateReleaseRejectedQuestionRecords(ctx context.Context,
	releaseRejectedQuestions []*releaseEntity.ReleaseRejectedQuestion, tx *gorm.DB) error {
	if len(releaseRejectedQuestions) == 0 {
		logx.I(ctx, "no release rejected_questions to create")
		return nil
	}
	tbl := d.mysql.TReleaseRejectedQuestion
	db := d.getReleaseRejectedQuestionDB(ctx, tx)
	total := len(releaseRejectedQuestions)
	logx.I(ctx, "CreateReleaseRejectedQuestionRecords data %d releaseRejectedQuestions", total)

	toCreateRejectedQAs := BatchConvertReleaseRejectedQuestionPOToDO(
		releaseRejectedQuestions)
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: tbl.RobotID.ColumnName().String()},
			{Name: tbl.VersionID.ColumnName().String()},
			{Name: tbl.RejectedQuestionID.ColumnName().String()},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			tbl.Question.ColumnName().String(),
			tbl.Action.ColumnName().String(),
			tbl.ReleaseStatus.ColumnName().String(),
			tbl.UpdateTime.ColumnName().String(),
			tbl.IsDeleted.ColumnName().String(),
		}), // 冲突时更新question, action, release_status, update_time, is_deleted
	}).CreateInBatches(toCreateRejectedQAs, 100).Error; err != nil {
		logx.E(ctx, "CreateRelaseConfigRecords data %d releaseRejectedQuestions, error:%v",
			len(toCreateRejectedQAs), err)
		return err
	}
	return nil
}

func (d *daoImpl) UpdateReleaseRejectedQuestionRecords(ctx context.Context, updateColumns []string,
	filter *releaseEntity.ReleaseRejectedQuestionFilter, releaseRejectedQuestion *releaseEntity.ReleaseRejectedQuestion, tx *gorm.DB) error {
	db := d.getReleaseRejectedQuestionDB(ctx, tx)
	session := mysqlquery.Use(db).TReleaseRejectedQuestion.WithContext(ctx)

	if filter.RobotID != 0 {
		session = session.Where(d.mysql.TReleaseRejectedQuestion.RobotID.Eq(filter.RobotID))
	}

	if filter.ID != 0 {
		session = session.Where(d.mysql.TReleaseRejectedQuestion.ID.Eq(filter.ID))
	}

	if filter.VersionId != 0 {
		session = session.Where(d.mysql.TReleaseRejectedQuestion.VersionID.Eq(filter.VersionId))
	}

	updateFileds := []field.Expr{}

	for _, v := range updateColumns {
		if f, ok := d.mysql.TReleaseRejectedQuestion.GetFieldByName(v); ok {
			updateFileds = append(updateFileds, f)
		}
	}

	_, err := session.Select(updateFileds...).Updates(releaseRejectedQuestion)
	if err != nil {
		logx.E(ctx, "UpdateReleaseSegmentRecords data error:%v", err)
		return err
	}
	return nil
}

// GetModifyRejectedQuestionCount 发布拒答问题预览数量
func (d *daoImpl) GetModifyRejectedQuestionCount(ctx context.Context,
	corpID, robotID, versionID uint64, question string, releaseStatuses []uint32) (
	uint64, error) {

	/*
		`
			SELECT
				count(*) total
			FROM
				t_release_rejected_question
			WHERE
				corp_id = ?
				AND robot_id = ? %s
		`
	*/
	db := d.mysql.TReleaseRejectedQuestion.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseRejectedQuestion.CorpID.Eq(corpID),
		d.mysql.TReleaseRejectedQuestion.RobotID.Eq(robotID),
	}

	if question != "" {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.Question.Like("%"+special.Replace(question)+"%"))
	}

	if versionID != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.VersionID.Eq(versionID))
	}

	if len(releaseStatuses) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.ReleaseStatus.In(releaseStatuses...))
	}

	total, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "GetModifyRejectedQuestionCount data req:(corpID:%v, robotID:%v, versionID:%v, question:%v, releaseStatuses:%v), error:%v",
			corpID, robotID, versionID, question, releaseStatuses, err)
		return 0, err
	}
	return uint64(total), nil
}

// GetModifyRejectedQuestionList 发布拒答问题预览列表
func (d *daoImpl) GetModifyRejectedQuestionList(ctx context.Context,
	req releaseEntity.ListReleaseRejectedQuestionReq) ([]*releaseEntity.ReleaseRejectedQuestion, error) {
	/*
			 `
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

	*/

	db := d.mysql.TReleaseRejectedQuestion.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseRejectedQuestion.RobotID.Eq(req.RobotID),
	}

	if req.Question != "" {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.Question.Like("%"+special.Replace(req.Question)+"%"))
	}

	if req.VersionID != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.VersionID.Eq(req.VersionID))
	}

	if req.CorpID != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.CorpID.Eq(req.CorpID))
	}

	if req.IsDeletedNot != nil {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.IsDeleted.Neq(*req.IsDeletedNot))
	}

	if req.IsDeleted != nil {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.IsDeleted.Eq(*req.IsDeleted))
	}

	if req.IsAllowRelease != nil {
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.IsAllowRelease.Eq(*req.IsAllowRelease))
	}

	offset, limit := utilx.Page(req.Page, req.PageSize)

	orderFields := []field.Expr{}

	if len(req.OrderBy) > 0 {
		if col, ok := d.mysql.TReleaseRejectedQuestion.GetFieldByName(req.OrderBy); ok {
			orderFields = append(orderFields, col.Asc())
		} else {
			orderFields = append(orderFields, d.mysql.TReleaseRejectedQuestion.RejectedQuestionID.Asc())
		}
	}

	res, err := db.Where(queryCond...).Offset(int(offset)).Limit(int(limit)).Find()
	if err != nil {
		logx.E(ctx, "GetModifyRejectedQuestionList data req:%+v, error:%v", req, err)
		return nil, err
	}

	return BatchConvertReleaseRejectedQuestionDOToPO(res), nil
}

func (d *daoImpl) GetReleaseModifyRejectedQuestion(ctx context.Context, release *releaseEntity.Release,
	rejectedQuestion []*qaEntity.RejectedQuestion) (map[uint64]*releaseEntity.ReleaseRejectedQuestion, error) {

	/*
		`
			SELECT
				%s
			FROM
				t_release_rejected_question
			WHERE
				corp_id = ?
				AND robot_id = ?
				AND version_id = ? %s
		`
	*/

	db := d.mysql.TReleaseRejectedQuestion.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseRejectedQuestion.CorpID.Eq(release.CorpID),
		d.mysql.TReleaseRejectedQuestion.RobotID.Eq(release.RobotID),
		d.mysql.TReleaseRejectedQuestion.VersionID.Eq(release.ID),
	}

	if len(rejectedQuestion) > 0 {
		questionIDs := make([]uint64, 0, len(rejectedQuestion))
		for _, question := range rejectedQuestion {
			questionIDs = append(questionIDs, question.ID)
		}
		queryCond = append(queryCond, d.mysql.TReleaseRejectedQuestion.RejectedQuestionID.In(questionIDs...))
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseModifyRejectedQuestion data req:%+v, error:%v", release, err)
		return nil, err
	}

	releaseRejectedQuestionMap := make(map[uint64]*releaseEntity.ReleaseRejectedQuestion, len(res))
	for _, item := range res {
		releaseRejectedQuestionMap[item.RejectedQuestionID] = ConvertReleaseRejectedQuestionDOToPO(item)
	}

	return releaseRejectedQuestionMap, nil

}

// GetReleaseRejectedQuestionByVersion 按 Version 版本获取拒答问题发布列表
func (d *daoImpl) GetReleaseRejectedQuestionByVersion(ctx context.Context, corpID uint64, robotID uint64,
	versionID uint64) ([]*releaseEntity.ReleaseRejectedQuestion, error) {
	/*
		 `
			SELECT
				%s
			FROM
				t_release_rejected_question
			WHERE
				corp_id = ?
				AND robot_id = ?
				AND version_id = ? %s
		`
	*/

	db := d.mysql.TReleaseRejectedQuestion.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseRejectedQuestion.CorpID.Eq(corpID),
		d.mysql.TReleaseRejectedQuestion.RobotID.Eq(robotID),
		d.mysql.TReleaseRejectedQuestion.VersionID.Eq(versionID),
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseRejectedQuestionByVersion data req:%+v, error:%v", versionID, err)
		return nil, err
	}

	return BatchConvertReleaseRejectedQuestionDOToPO(res), nil
}
