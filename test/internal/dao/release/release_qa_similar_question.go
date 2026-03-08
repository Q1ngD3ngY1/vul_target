package release

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *daoImpl) GetReleaseSimilarQuestionDao(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseQaSimilarQuestion.WithContext(ctx).UnderlyingDB()
	}
	return d.mysql.TReleaseQaSimilarQuestion.WithContext(ctx).Debug().UnderlyingDB()
}

func (d *daoImpl) generateReleaseSimilarQuestionCondition(ctx context.Context, session *gorm.DB, filter *releaseEntity.ReleaseQaSimilarQuestionFilter) *gorm.DB {
	if filter == nil {
		return session
	}

	if filter.ID != 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.ID.Eq(int64(filter.ID)))
	}

	if filter.CorpID != 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.CorpID.Eq(filter.CorpID))
	}

	if filter.VersionID != 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.VersionID.Eq(filter.VersionID))
	}

	if filter.RobotID != 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.RobotID.Eq(filter.RobotID))
	}

	if filter.IsDeleted != nil {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.IsDeleted.Eq(*filter.IsDeleted))
	}

	if filter.RelatedQaID != 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.RelatedQaID.Eq(filter.RelatedQaID))
	}

	if len(filter.RelatedQaIDs) > 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.RelatedQaID.In(filter.RelatedQaIDs...))
	}

	if filter.ReleaseStatusNot != 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.ReleaseStatus.Neq(filter.ReleaseStatusNot))
	}

	if filter.SimilarID > 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.SimilarID.Eq(filter.SimilarID))
	}

	if len(filter.SimilarIDs) > 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.SimilarID.In(filter.SimilarIDs...))
	}

	if filter.AuditStatus != nil {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.AuditStatus.Eq(*filter.AuditStatus))
	}

	if len(filter.AudiStatusList) > 0 {
		session = session.Where(d.mysql.TReleaseQaSimilarQuestion.AuditStatus.In(filter.AudiStatusList...))
	}

	if filter.ExtraCondition != "" {
		if len(filter.ExtraArgs) > 0 {
			session = session.Where(filter.ExtraCondition, filter.ExtraArgs...)
		} else {
			session = session.Where(filter.ExtraCondition)
		}
	}
	return session
}

func (d *daoImpl) CreateReleaseSimilarQuestionRecord(ctx context.Context, sqs []*releaseEntity.ReleaseQaSimilarQuestion, tx *gorm.DB) error {
	/*
					 `
			INSERT INTO
				t_release_qa_similar_question(%s)
			VALUES
				(null,:corp_id,:staff_id,:robot_id,:version_id,:similar_id,:related_qa_id,:source,:question,
				 :release_status,:message,:action,:attr_labels,:audit_status,:audit_result,:is_allow_release,
				 :is_deleted,:expire_time,:create_time,:update_time)
		`
	*/
	db := d.GetReleaseSimilarQuestionDao(ctx, tx)
	tbl := d.mysql.TReleaseQaSimilarQuestion
	tQas := BatchConvertReleaseQASimilarQuestionDOToPO(sqs)
	logx.I(ctx, "CreateReleaseSimilarQuestionRecord %d records", tQas)

	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: tbl.RobotID.ColumnName().String()},
			{Name: tbl.VersionID.ColumnName().String()},
			{Name: tbl.SimilarID.ColumnName().String()},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			tbl.RelatedQaID.ColumnName().String(),
			tbl.Source.ColumnName().String(),
			tbl.Question.ColumnName().String(),
			tbl.Action.ColumnName().String(),
			tbl.AttrLabels.ColumnName().String(),
			tbl.ReleaseStatus.ColumnName().String(),
			tbl.UpdateTime.ColumnName().String(),
			tbl.IsDeleted.ColumnName().String(),
		}), // 冲突时更新 question,answer,similar_status,accept_status,action,attr_labels,release_status,update_time,is_deleted 字段
	}).CreateInBatches(tQas, 100).Error; err != nil {
		logx.E(ctx, "Failed to CreateReleaseSimilarQuestionRecord data, error:%v", err)
		return err
	}
	return nil
}

func (d *daoImpl) UpdateReleaseSimilarQuestionRecords(ctx context.Context, updateColumns []string,
	filter *releaseEntity.ReleaseQaSimilarQuestionFilter, qa *releaseEntity.ReleaseQaSimilarQuestion, tx *gorm.DB) error {
	db := d.GetReleaseSimilarQuestionDao(ctx, tx).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	if filter != nil {
		db = d.generateReleaseSimilarQuestionCondition(ctx, db, filter)
	}

	if len(updateColumns) > 0 {
		db = db.Select(updateColumns)
	}

	tQA := ConvertReleaseQASimilarQuestionDOToPO(qa)

	if err := db.Updates(tQA).Error; err != nil {
		logx.E(ctx, "Failed to UpdateReleaseSimilarQuestionRecords data filter:%+v, qa:%+v, error:%v",
			filter, qa, err)
		return err
	}
	return nil
}

func (d *daoImpl) BatchUpdateReleaseSimilarQuestionRecords(ctx context.Context, updateColumns map[string]any,
	filter *releaseEntity.ReleaseQaSimilarQuestionFilter, tx *gorm.DB) (uint64, error) {

	db := d.GetReleaseSimilarQuestionDao(ctx, tx).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	if filter != nil {
		db = d.generateReleaseSimilarQuestionCondition(ctx, db, filter)
	}

	res := db.Updates(updateColumns)

	if err := res.Error; err != nil {
		logx.E(ctx, "Failed to BatchUpdateReleaseSimilarQuestionRecords data filter:%+v, updateColumns:%+v, error:%v",
			filter, updateColumns, err)
		return 0, err
	}
	return uint64(res.RowsAffected), nil
}

func (d *daoImpl) GetReleaseSimilarQuestionList(ctx context.Context, selectColumns []string,
	filter *releaseEntity.ReleaseQaSimilarQuestionFilter) ([]*releaseEntity.ReleaseQaSimilarQuestion, error) {
	db := d.GetReleaseSimilarQuestionDao(ctx, nil).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	db = db.Select(selectColumns)

	if filter != nil {
		db = d.generateReleaseSimilarQuestionCondition(ctx, db, filter)
		if filter.PageSize > 0 {
			offset, limit := utilx.Page(uint32(filter.PageNo), uint32(filter.PageSize))
			db = db.Offset(int(offset)).Limit(int(limit))
		}
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
				continue
			}
			db = db.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}

	list := []*model.TReleaseQaSimilarQuestion{}

	if err := db.Find(&list).Error; err != nil {
		return nil, err
	}

	res := BatchConvertReleaseQASimilarQuestionPOToDO(list)

	return res, nil

}

func (d *daoImpl) IsExistReleaseQaSimilar(ctx context.Context, filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (bool, error) {
	db := d.GetReleaseSimilarQuestionDao(ctx, nil).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	db = db.Select("COUNT(1) as cnt")

	if filter != nil {
		db = d.generateReleaseSimilarQuestionCondition(ctx, db, filter)
	}

	var count int64

	res := db.Scan(&count)

	if res.Error != nil {
		logx.E(ctx, "Failed to IsExistReleaseQaSimilar. data filter:%+v, error:%v",
			filter, res.Error)
		return false, res.Error
	}

	return count > 0, nil
}

func (d *daoImpl) GetReleaseSimilarQuestionListCount(ctx context.Context, selectColumns []string,
	filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (uint64, error) {
	db := d.GetReleaseSimilarQuestionDao(ctx, nil).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	if filter != nil {
		db = d.generateReleaseSimilarQuestionCondition(ctx, db, filter)
	}

	var count int64

	if err := db.Count(&count).Error; err != nil {
		logx.E(ctx, "Failed to GetReleaseSimilarQuestionListCount data filter:%+v, error:%v",
			filter, selectColumns, err)
		return 0, err
	}

	return uint64(count), nil

}

func (d *daoImpl) GetReleaseSimilarQuestion(ctx context.Context, selectColumns []string,
	filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (*releaseEntity.ReleaseQaSimilarQuestion, error) {
	db := d.GetReleaseSimilarQuestionDao(ctx, nil).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	db = db.Select(selectColumns)

	if filter != nil {
		db = d.generateReleaseSimilarQuestionCondition(ctx, db, filter)
	}

	var res *model.TReleaseQaSimilarQuestion

	if err := db.First(&res).Error; err != nil {
		logx.E(ctx, "Failed to GetReleaseSimilarQuestionList data filter:%+v, selectColumns:%+v, error:%v",
			filter, selectColumns, err)
		return nil, err
	}

	return ConvertReleaseQASimilarQuestionPOToDO(res), nil
}

func (d *daoImpl) GetReleaseSimilarQACountByGroup(ctx context.Context, groupField string, filter *releaseEntity.ReleaseQaSimilarQuestionFilter) (
	[]*releaseEntity.ReleaseQaSimilarQuestionState, error) {
	db := d.GetReleaseSimilarQuestionDao(ctx, nil).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	db = db.Select(groupField, "count(*) as total")

	if filter != nil {
		db = d.generateReleaseSimilarQuestionCondition(ctx, db, filter)
	}

	res := []*releaseEntity.ReleaseQaSimilarQuestionState{}

	if err := db.Group(groupField).Find(&res).Error; err != nil {
		logx.E(ctx, "Failed to GetReleaseSimilarQACountByGroup data filter:%+v, groupField:%+v, error:%v",
			filter, groupField, err)
		return nil, err
	}

	return res, nil
}

// getSimilarIDWithDeleteFlag 分批获取t_release_qa_similar_question本次待发布的similar_id 区分删除/非删除
func (d *daoImpl) GetSimilarIDWithDeleteFlag(ctx context.Context, robotID uint64, versionID uint64, similarID,
	limit uint64, isDeleted bool) ([]uint64, error) {

	db := d.GetReleaseSimilarQuestionDao(ctx, nil).Table(d.mysql.TReleaseQaSimilarQuestion.TableName())

	var similarIDs []uint64
	query := db.WithContext(ctx).Model(&model.TReleaseQaSimilarQuestion{}).
		Select("similar_id").
		Where("robot_id = ? AND version_id = ? AND is_allow_release = ? AND similar_id > ?",
			robotID, versionID, 1, similarID).
		Order("similar_id ASC").
		Limit(int(limit))

	if isDeleted {
		query = query.Where("is_deleted = ?", qaEntity.QAIsDeleted)
	} else {
		query = query.Where("is_deleted != ?", qaEntity.QAIsDeleted)
	}

	err := query.Pluck("similar_id", &similarIDs).Error
	if err != nil {
		logx.E(ctx, "getSimilarIDWithDeleteFlag fail, robotID: %d, versionID: %d, similarID: %d, limit: %d, isDeleted: %v, err: %v",
			robotID, versionID, similarID, limit, isDeleted, err)
		return nil, err
	}
	return similarIDs, nil
}
