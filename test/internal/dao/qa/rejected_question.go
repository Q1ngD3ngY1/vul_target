package qa

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity/qa"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) generateRejectedQuestionCondition(ctx context.Context, q mysqlquery.ITRejectedQuestionDo, req *qaEntity.RejectedQuestionFilter) mysqlquery.ITRejectedQuestionDo {
	if req.ID > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.ID.Eq(req.ID))
	}

	if req.IDs != nil && len(req.IDs) > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.ID.In(req.IDs...))
	}

	if req.IDMore > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.ID.Gt(req.IDMore))
	}

	if req.BusinessID > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.BusinessID.Eq(req.BusinessID))
	}

	if req.BusinessIDs != nil && len(req.BusinessIDs) > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.BusinessID.In(req.BusinessIDs...))
	}

	if req.IsDeleted > 0 {
		// TODO: is_deleted字段没有改成bool类型 @wemysschen
		q = q.Where(d.mysql.TRejectedQuestion.IsDeleted.Eq(req.IsDeleted))
	}

	if req.CorpID > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.CorpID.Eq(req.CorpID))
	}

	if req.RobotID > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.RobotID.Eq(req.RobotID))
	}

	if len(req.Query) > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.Question.Like("%" + req.Query + "%"))
	}

	if len(req.Actions) > 0 {
		actionParams := make([]uint32, len(req.Actions))
		for i, action := range req.Actions {
			actionParams[i] = action
		}
		q = q.Where(d.mysql.TRejectedQuestion.Action.In(actionParams...))
	}
	if !req.UpdateTimeLess.IsZero() {
		q = q.Where(d.mysql.TRejectedQuestion.UpdateTime.Lt(req.UpdateTimeLess))
	}
	if !req.UpdateTimeMore.IsZero() {
		q = q.Where(d.mysql.TRejectedQuestion.UpdateTime.Gt(req.UpdateTimeMore))
	}
	if req.ReleaseStatus > 0 {
		q = q.Where(d.mysql.TRejectedQuestion.ReleaseStatus.Eq(req.ReleaseStatus))
	}
	return q
}

func (d *daoImpl) GetReleaseRejectedQuestionCount(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, status []uint32) (uint64, error) {
	/*
		`
		SELECT
			COUNT(*) total
		FROM
			t_rejected_question
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND release_status = ?
			AND !(action = ? AND is_deleted = ?) %s
	*/
	/*
		filter := &qaEntity.RejectedQuestionFilter{
				CorpPrimaryId:         corpID,
				AppPrimaryId:        robotID,
				Query:          question,
				Actions:        status,
				UpdateTimeLess: endTime,
				UpdateTimeMore: startTime,
				ReleaseStatus:  qaEntity.RejectedQuestionReleaseStatusInit,
				ExtraCondition: "!(action = ? AND is_deleted = ?)",
				ExtraArgs:      []any{qaEntity.RejectedQuestionAdd, qaEntity.RejectedQuestionIsDeleted},
			}
	*/
	tbl := d.mysql.TRejectedQuestion
	session := tbl.WithContext(ctx).Debug()
	session = session.Where(tbl.CorpID.Eq(corpID)).
		Where(tbl.RobotID.Eq(robotID)).
		Where(tbl.ReleaseStatus.Eq(qaEntity.RejectedQuestionReleaseStatusInit)).
		Where(field.Or(tbl.Action.Neq(qaEntity.RejectedQuestionAdd), tbl.IsDeleted.Neq(qaEntity.RejectedQuestionIsDeleted)))

	if len(question) > 0 {
		session = session.Where(tbl.Question.Like(fmt.Sprintf("%%%s%%", util.Special.Replace(question))))
	}
	if len(status) > 0 {
		session = session.Where(tbl.Action.In(status...))
	}

	if !startTime.IsZero() {
		session = session.Where(tbl.UpdateTime.Gt(startTime))
	}
	if !endTime.IsZero() {
		session = session.Where(tbl.UpdateTime.Lt(endTime))
	}
	if count, err := session.Count(); err != nil {
		logx.E(ctx, "GetReleaseRejectedQuestionCount "+
			"corpID:%d, robotID:%d, question:%s, startTime:%v, endTime:%v, status:%v, error:%v",
			corpID, robotID, question, startTime, endTime, status, err)
		return 0, err
	} else {
		return uint64(count), nil
	}
}

func (d *daoImpl) GetReleaseRejectedQuestionList(ctx context.Context, corpID, robotID uint64, page, pageSize uint32,
	query string, startTime, endTime time.Time, status []uint32) ([]*qaEntity.RejectedQuestion, error) {
	/*
			`
			SELECT
				%s
			FROM
				t_rejected_question
			WHERE
				corp_id = ?
				AND robot_id = ?
				AND release_status = ?
				AND !(action = ? AND is_deleted = ?) %s
			ORDER BY
				update_time DESC,id DESC
			LIMIT
				?,?
		`
	*/

	tbl := d.mysql.TRejectedQuestion
	session := tbl.WithContext(ctx).Debug()
	session = session.Where(tbl.CorpID.Eq(corpID)).
		Where(tbl.RobotID.Eq(robotID)).
		Where(tbl.ReleaseStatus.Eq(qaEntity.RejectedQuestionReleaseStatusInit)).
		Where(field.Or(tbl.Action.Neq(qaEntity.RejectedQuestionAdd), tbl.IsDeleted.Neq(qaEntity.RejectedQuestionIsDeleted)))
	// Where(field.Not(field.And(tbl.Action.Eq(qaEntity.RejectedQuestionAdd), tbl.IsDeleted.Eq(qaEntity.RejectedQuestionIsDeleted))))

	if len(query) > 0 {
		session = session.Where(tbl.Question.Like(fmt.Sprintf("%%%s%%", util.Special.Replace(query))))
	}
	if len(status) > 0 {
		session = session.Where(tbl.Action.In(status...))
	}

	if !startTime.IsZero() {
		session = session.Where(tbl.UpdateTime.Gt(startTime))
	}
	if !endTime.IsZero() {
		session = session.Where(tbl.UpdateTime.Lt(endTime))
	}

	if page > 0 || pageSize > 0 {
		offset, limit := utilx.Page(page, pageSize)
		session = session.Limit(limit).Offset(offset)
	}

	session = session.Order(tbl.UpdateTime.Desc(), tbl.ID.Desc())

	if qas, err := session.Find(); err != nil {
		logx.E(ctx, "GetReleaseRejectedQuestionList "+
			"corpID:%d, robotID:%d, page:%d, pageSize:%d, query:%s, startTime:%v, endTime:%v, status:%v, error:%v",
			corpID, robotID, page, pageSize, query, startTime, endTime, status, err)
		return nil, err
	} else {
		return BatchConvertRejectedQuestionsPO2DO(qas), nil
	}
}

func (d *daoImpl) GetRejectedQuestionListCount(ctx context.Context, req *qaEntity.RejectedQuestionFilter) (int64, error) {
	tbl := d.mysql.TRejectedQuestion
	session := tbl.WithContext(ctx)
	session = d.generateRejectedQuestionCondition(ctx, session, req)

	// conds := []gen.Condition{
	// 	d.mysql.TRejectedQuestion.CorpPrimaryId.Eq(req.CorpPrimaryId),
	// 	d.mysql.TRejectedQuestion.AppPrimaryId.Eq(req.AppPrimaryId),
	// 	d.mysql.TRejectedQuestion.IsDeleted.Eq(qa.RejectedQuestionIsNotDeleted),
	// }
	// if len(req.Query) > 0 {
	// 	conds = append(conds, d.mysql.TRejectedQuestion.Question.Like("%"+req.Query+"%"))
	// }
	// if len(req.Actions) > 0 {
	// 	actionParams := make([]uint32, len(req.Actions))
	// 	for i, action := range req.Actions {
	// 		actionParams[i] = uint32(action)
	// 	}
	// 	conds = append(conds, d.mysql.TRejectedQuestion.Action.In(actionParams...))
	// }

	if count, err := session.Count(); err != nil {
		logx.E(ctx, "ListRejectedQuestion count req:%+v, error:%v", req, err)
		return 0, err
	} else {
		return count, nil
	}
}

func (d *daoImpl) getTRejectedQuestioGenFields(selectColumns []string) []field.Expr {
	fields := make([]field.Expr, 0)
	for _, v := range selectColumns {
		if f, ok := d.mysql.TRejectedQuestion.GetFieldByName(v); ok {
			fields = append(fields, f)
		}
	}
	return fields
}

func (d *daoImpl) GetRejectedQuestionList(ctx context.Context, selectColumns []string, req *qaEntity.RejectedQuestionFilter) (
	[]*qaEntity.RejectedQuestion, error) {

	tbl := d.mysql.TRejectedQuestion
	session := tbl.WithContext(ctx).Debug()
	session = d.generateRejectedQuestionCondition(ctx, session, req)
	session = session.Where(tbl.IsDeleted.Eq(qa.RejectedQuestionIsNotDeleted))

	// conds := []gen.Condition{
	// 	d.mysql.TRejectedQuestion.CorpPrimaryId.Eq(req.CorpPrimaryId),
	// 	d.mysql.TRejectedQuestion.AppPrimaryId.Eq(req.AppPrimaryId),
	// 	d.mysql.TRejectedQuestion.IsDeleted.Eq(qa.RejectedQuestionIsNotDeleted),
	// }
	// if len(req.Query) > 0 {
	// 	conds = append(conds, d.mysql.TRejectedQuestion.Question.Like("%"+req.Query+"%"))
	// }
	// if len(req.Actions) > 0 {
	// 	actionParams := make([]uint32, len(req.Actions))
	// 	for i, action := range req.Actions {
	// 		actionParams[i] = uint32(action)
	// 	}
	// 	conds = append(conds, d.mysql.TRejectedQuestion.Action.In(actionParams...))
	// }

	// 算总数
	if req.Page > 0 || req.PageSize > 0 {
		offset, limit := utilx.Page(req.Page, req.PageSize)
		session = session.Limit(limit).Offset(offset)

	} else {
		if req.Limit > 0 {
			session = session.Limit(int(req.Limit))
		}
	}

	if len(selectColumns) > 0 {
		session = session.Select(d.getTRejectedQuestioGenFields(selectColumns)...)
	}

	for i, orderColumn := range req.OrderColumn {
		if req.OrderDirection[i] != util.SqlOrderByAsc && req.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", req.OrderDirection[i])
			continue
		}
		if f, ok := d.mysql.TRejectedQuestion.GetFieldByName(orderColumn); ok {
			if req.OrderDirection[i] == util.SqlOrderByAsc {
				session = session.Order(f.Asc())
			} else {
				session = session.Order(f.Desc())
			}
		}
	}

	// 获取列表
	qas := []*model.TRejectedQuestion{}
	if err := session.Scan(&qas); err != nil {
		logx.E(ctx, "ListRejectedQuestion data req:%+v, error:%v", req, err)
		return nil, err
	}

	return BatchConvertRejectedQuestionsPO2DO(qas), nil
}

func (d *daoImpl) ListRejectedQuestion(ctx context.Context, selectColumns []string, req *qaEntity.RejectedQuestionFilter) (
	[]*qaEntity.RejectedQuestion, int64, error) {
	count, err := d.GetRejectedQuestionListCount(ctx, req)
	if err != nil {
		logx.E(ctx, "ListRejectedQuestion count req:%+v, error:%v", req, err)
		return nil, 0, err
	}

	qas, err := d.GetRejectedQuestionList(ctx, selectColumns, req)
	if err != nil {
		logx.E(ctx, "ListRejectedQuestion data req:%+v, error:%v", req, err)
		return nil, 0, err
	}

	return qas, count, nil
}

func (d *daoImpl) CreateRejectedQuestion(ctx context.Context, rejectedQuestion *qaEntity.RejectedQuestion) error {
	db := d.mysql.TRejectedQuestion.WithContext(ctx).UnderlyingDB().Table(d.mysql.TRejectedQuestion.TableName())
	tRQ := ConvertRejectQuestionDO2PO(rejectedQuestion)

	if err := db.Create(tRQ).Error; err != nil {
		logx.E(ctx, "CreateRejectedQuestion rejectedQuestion:%+v, error:%v", rejectedQuestion, err)
		return err
	}

	rejectedQuestion.ID = tRQ.ID
	return nil
}

func (d *daoImpl) GetRejectedQuestion(ctx context.Context, req *qaEntity.RejectedQuestionFilter) (*qaEntity.RejectedQuestion, error) {
	tbl := d.mysql.TRejectedQuestion
	session := tbl.WithContext(ctx).Debug()
	session = d.generateRejectedQuestionCondition(ctx, session, req)
	tRQ := &model.TRejectedQuestion{}
	tRQ, err := session.First()
	if err != nil {
		logx.E(ctx, "GetRejectedQuestion req:%+v, error:%v", req, err)
		return nil, err
	}

	return ConvertRejectedQuestionsPO2DO(tRQ), nil
}

func (d *daoImpl) GetRejectedQuestionByID(ctx context.Context, id uint64) (*qaEntity.RejectedQuestion, error) {
	tbl := d.mysql.TRejectedQuestion
	db := tbl.WithContext(ctx).Debug()

	q, err := db.Where(tbl.ID.Eq(id)).First()
	if err != nil {
		logx.E(ctx, "GetRejectedQuestionByID id:%d, error:%v", id, err)
		return nil, err
	}

	return ConvertRejectedQuestionsPO2DO(q), nil
}

func (d *daoImpl) BatchUpdateRejectedQuestion(ctx context.Context, filter *qaEntity.RejectedQuestionFilter, updateColumns map[string]any, tx *gorm.DB) error {
	tbl := d.mysql.TRejectedQuestion
	session := tbl.WithContext(ctx)
	if tx != nil {
		session = mysqlquery.Use(tx.Table(model.TableNameTRejectedQuestion)).TRejectedQuestion.WithContext(ctx)
	}
	logx.I(ctx, "BatchUpdateRejectedQuestion -> tabName in session: %s", session.TableName())

	session = d.generateRejectedQuestionCondition(ctx, session, filter)

	if _, err := session.Updates(updateColumns); err != nil {
		logx.E(ctx, "BatchUpdateRejectedQuestions failed,  error:%v", err)
		return err
	}
	return nil
}

func (d *daoImpl) UpdateRejectedQuestion(ctx context.Context, filter *qaEntity.RejectedQuestionFilter,
	updateColumns []string, rqa *qaEntity.RejectedQuestion, tx *gorm.DB) error {
	tbl := d.mysql.TRejectedQuestion
	session := tbl.WithContext(ctx).Debug()
	if tx != nil {
		session = mysqlquery.Use(tx).TRejectedQuestion.WithContext(ctx).Debug()
	}
	logx.I(ctx, "UpdateRejectedQuestion -> tabName in session: %s", session.TableName())

	session = d.generateRejectedQuestionCondition(ctx, session, filter)
	if len(updateColumns) > 0 {
		session = session.Select(d.getTRejectedQuestioGenFields(updateColumns)...)
	}
	trqa := ConvertRejectQuestionDO2PO(rqa)

	if _, err := session.Updates(trqa); err != nil {
		logx.E(ctx, "UpdateRejectedQuestion failed,  error:%v", err)
		return err
	}
	return nil

}

func (d *daoImpl) BatchUpdateRejectedQuestions(ctx context.Context,
	rejectedQuestions []*qa.RejectedQuestion) error {
	db := d.mysql.TRejectedQuestion.WithContext(ctx).Debug().UnderlyingDB()
	if len(rejectedQuestions) == 0 {
		logx.I(ctx, "no rejected question record to update")
		return nil
	}

	rejectQuestionDos := BatchConvertRejectQuestionDO2PO(rejectedQuestions)

	err := db.Transaction(func(tx *gorm.DB) error {
		for _, v := range rejectQuestionDos {
			if err := tx.
				Model(&model.TRejectedQuestion{}).
				Save(v).Error; err != nil {
				logx.E(ctx, "BatchUpdateRejectedQuestions failed, rejectedQuestionId:%d, error:%v",
					v.ID, err)
				return err
			}
		}
		return nil
	})

	if err != nil {
		logx.E(ctx, "BatchUpdateRejectedQuestions failed,  error:%v",
			rejectedQuestions, err)
		return err
	}
	return nil
}
