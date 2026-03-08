package qa

import (
	"context"
	"errors"
	"fmt"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) getConditions(req *qaEntity.SimilarityQuestionReq) []gen.Condition {
	conds := []gen.Condition{}
	tbl := d.mysql.TQaSimilarQuestion

	if req.ID > 0 {
		conds = append(conds, tbl.ID.Eq(req.ID))
	}

	if len(req.IDs) > 0 {
		conds = append(conds, tbl.ID.In(req.IDs...))
	}
	if req.IDLess > 0 {
		conds = append(conds, tbl.ID.Lt(req.IDLess))
	}
	if req.IDMore > 0 {
		conds = append(conds, tbl.ID.Gt(req.IDMore))
	}

	if req.SimilarQuestionID > 0 {
		conds = append(conds, tbl.SimilarID.Eq(req.SimilarQuestionID))
	}
	if len(req.SimilarQuestionIDs) > 0 {
		conds = append(conds, tbl.SimilarID.In(req.SimilarQuestionIDs...))
	}

	if req.CorpId > 0 {
		conds = append(conds, tbl.CorpID.Eq(req.CorpId))
	}
	if req.IsDeleted > 0 {
		conds = append(conds, tbl.IsDeleted.Eq(req.IsDeleted))
	}
	if req.ReleaseStatus > 0 {
		conds = append(conds, tbl.ReleaseStatus.Eq(req.ReleaseStatus))
	}

	if len(req.ReleaseStatusList) > 0 {
		conds = append(conds, tbl.ReleaseStatus.In(req.ReleaseStatusList...))
	}

	if len(req.ReleaseStatusNotIn) > 0 {
		conds = append(conds, tbl.ReleaseStatus.NotIn(req.ReleaseStatusNotIn...))
	}

	if req.StartMore != nil {
		conds = append(conds, tbl.UpdateTime.Gt(*req.StartMore))
	}
	if req.EndLess != nil {
		conds = append(conds, tbl.UpdateTime.Lt(*req.EndLess))
	}

	if req.RelatedQAID > 0 {
		conds = append(conds, tbl.RelatedQaID.Eq(req.RelatedQAID))
	}
	if len(req.RelatedQAIDs) > 0 {
		conds = append(conds, tbl.RelatedQaID.In(req.RelatedQAIDs...))
	}
	if req.RobotId > 0 {
		conds = append(conds, tbl.RobotID.Eq(req.RobotId))
	}
	if len(req.RobotIDs) > 0 {
		conds = append(conds, tbl.RobotID.In(req.RobotIDs...))
	}
	if req.IsDeleted > 0 {
		conds = append(conds, tbl.IsDeleted.Eq(req.IsDeleted))
	}
	if req.ReleaseStatus > 0 {
		conds = append(conds, tbl.ReleaseStatus.Eq(req.ReleaseStatus))
	}
	return conds
}

func (d *daoImpl) getSimilarityQuestionConditions(ctx context.Context, session *gorm.DB, req *qaEntity.SimilarityQuestionReq) *gorm.DB {
	tbl := d.mysql.TQaSimilarQuestion

	if req.ID > 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlEqual, req.ID)
	}
	if len(req.IDs) > 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlIn, req.IDs)
	}

	if req.IDLess > 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlLess, req.IDLess)
	}
	if req.IDMore > 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlMore, req.IDMore)
	}

	if len(req.SimilarQuestionIDs) > 0 {
		session = session.Where(tbl.SimilarID.ColumnName().String()+util.SqlIn, req.SimilarQuestionIDs)
	}
	if req.SimilarQuestionID > 0 {
		session = session.Where(tbl.SimilarID.ColumnName().String()+util.SqlEqual, req.SimilarQuestionID)
	}

	if req.RelatedQAID > 0 {
		session = session.Where(tbl.RelatedQaID.ColumnName().String()+util.SqlEqual, req.RelatedQAID)
	}
	if len(req.RelatedQAIDs) > 0 {
		session = session.Where(tbl.RelatedQaID.ColumnName().String()+util.SqlIn, req.RelatedQAIDs)
	}
	if req.RobotId > 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, req.RobotId)
	}
	if len(req.RobotIDs) > 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlIn, req.RobotIDs)
	}
	if req.CorpId > 0 {
		session = session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, req.CorpId)
	}
	if req.IsDeleted > 0 {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, req.IsDeleted)
	}
	if req.ReleaseStatus > 0 {
		session = session.Where(tbl.ReleaseStatus.ColumnName().String()+util.SqlEqual, req.ReleaseStatus)
	}

	if len(req.ReleaseStatusList) > 0 {
		session = session.Where(tbl.ReleaseStatus.ColumnName().String()+util.SqlSubIn, req.ReleaseStatusList)
	}

	if len(req.ReleaseStatusNotIn) > 0 {
		session = session.Where(tbl.ReleaseStatus.ColumnName().String()+util.SqlSubNotIn, req.ReleaseStatusNotIn)
	}

	if req.StartMore != nil {
		session = session.Where(tbl.UpdateTime.ColumnName().String()+util.SqlMore, req.StartMore)
	}

	if req.EndLess != nil {
		session = session.Where(tbl.UpdateTime.ColumnName().String()+util.SqlLessEqual, req.EndLess)
	}

	return session
}

func (d *daoImpl) GetSimilarQuestionBySimilarID(ctx context.Context, relatedQAId uint64, similarQuestionID uint64) (*qaEntity.SimilarQuestion, error) {
	tbl := d.mysql.TQaSimilarQuestion

	db := d.mysql.TQaSimilarQuestion.WithContext(ctx).Debug()

	q, err := db.Where(
		tbl.SimilarID.Eq(similarQuestionID),
		tbl.RelatedQaID.Eq(relatedQAId)).
		First()
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionBySimilarID id:%d, error:%v", relatedQAId, err)
		return nil, err
	}

	return ConvertSimilarQuestionsPO2DO(q), nil
}

func (d *daoImpl) GetSimilarQuestionByFilter(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) (*qaEntity.SimilarQuestion, error) {
	db := d.mysql.TQaSimilarQuestion.WithContext(ctx).Debug()
	conds := d.getConditions(filter)

	q, err := db.Where(conds...).First()
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionByFilter (filter:%+v) error:%v", filter, err)
		return nil, err
	}

	return ConvertSimilarQuestionsPO2DO(q), nil
}

// GetSimilarQuestionsByQA 根据标准问获取所有相似问
func (d *daoImpl) ListSimilarQuestions(ctx context.Context, selectColumns []string, req *qaEntity.SimilarityQuestionReq) ([]*qaEntity.SimilarQuestion, error) {
	/*
			 `
				SELECT
					%s
		        FROM
		            t_qa_similar_question
		        WHERE
					corp_id = ?
				AND robot_id = ?
				AND is_deleted = ?
				AND related_qa_id = ?
				LIMIT ?
			`
	*/
	tbl := d.Query().TQaSimilarQuestion
	q := tbl.WithContext(ctx)

	if len(selectColumns) > 0 {
		fields := []field.Expr{}
		for _, col := range selectColumns {
			if field, ok := tbl.GetFieldByName(col); ok {
				fields = append(fields, field)
			}
		}
		q = q.Select(fields...)
	}

	cond := d.getConditions(req)
	q = q.Where(cond...)

	if req.IsRelease {
		// !(next_action = ? AND is_deleted = ?) = (next_action != ? OR is_deleted != ?)
		q = q.Where(field.Or(tbl.NextAction.Neq(qaEntity.NextActionAdd), tbl.IsDeleted.Neq(qaEntity.QAIsDeleted)))
	}

	if req.PageSize > 0 {
		offset, limit := utilx.Page(req.Page, req.PageSize)
		q = q.Offset(offset).Limit(limit)
	}

	if sqs, err := q.Find(); err != nil {
		logx.E(ctx, "ListSimilarQuestions failed, err: %+v", err)
		return nil, err
	} else {
		return BatchConvertSimilarQuestionsPO2DO(sqs), nil
	}
}

// GetSimilarQuestionsSimpleByQAIDs 根据qaIDs批量获取相似问id和内容
func (d *daoImpl) BatchListSimilarQuestions(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) ([]*qaEntity.SimilarQuestion, error) {
	qaIDs := filter.RelatedQAIDs
	if len(qaIDs) == 0 {
		return nil, nil
	}

	sqs := make([]*qaEntity.SimilarQuestion, 0)

	for _, qaIDChunks := range slicex.Chunk(qaIDs, util.MaxSqlInCount) {
		/*
					`
					SELECT
					    related_qa_id, similar_id, question
			        FROM
			            t_qa_similar_question
			        WHERE
						corp_id = ? AND robot_id = ? AND is_deleted = ? AND related_qa_id IN (%s)
					LIMIT ?, ?
		*/
		page := 1
		pageSize := 800
		listReq := &qaEntity.SimilarityQuestionReq{
			CorpId:       filter.CorpId,
			RobotId:      filter.RobotId,
			IsDeleted:    qaEntity.QAIsNotDeleted,
			RelatedQAIDs: qaIDChunks,
		}
		// 分页获取相似问
		for {
			listReq.Page = uint32(page)
			listReq.PageSize = uint32(pageSize)
			simQAs, err := d.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, listReq)
			if err != nil {
				logx.E(ctx, "根据QAIDs获取相似问对列表失败 err:%+v", err)
				return nil, err
			}

			if len(simQAs) > 0 {
				sqs = append(sqs, simQAs...)
			}
			if len(simQAs) < pageSize {
				break
			}

			page++
		}
	}

	return sqs, nil
}

func (d *daoImpl) CreateSimilarQuestion(ctx context.Context, tx *gorm.DB, qa *qaEntity.SimilarQuestion) error {
	if qa == nil {
		return errors.New("qa is nil")
	}
	tQa := ConvertSimilarQuestionsDO2PO(qa)
	tbl := d.Query().TQaSimilarQuestion
	db := tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())

	if tx != nil {
		db = mysqlquery.Use(tx).TQaSimilarQuestion.WithContext(ctx).UnderlyingDB()
	}

	err := db.Create(tQa).Error
	if err != nil {
		logx.E(ctx, "CreateSimilarQuestion failed, err: %+v", err)
		return err
	}
	return nil

}

func (d *daoImpl) BatchCreateSimilarQuestions(ctx context.Context, qas []*qaEntity.SimilarQuestion) error {
	if len(qas) == 0 {
		logx.I(ctx, "empty similar questions list")
		return nil
	}

	tbl := d.Query().TQaSimilarQuestion
	db := tbl.WithContext(ctx).UnderlyingDB()

	if err := db.Transaction(func(tx *gorm.DB) error {

		for _, qa := range qas {
			if err := d.CreateSimilarQuestion(ctx, tx, qa); err != nil {
				return err
			}
		}
		return nil

	}); err != nil {
		return err
	}
	return nil
}

func (d *daoImpl) UpdateSimilarQuestion(ctx context.Context, tx *gorm.DB,
	updateColumns []string, req *qaEntity.SimilarityQuestionReq, sq *qaEntity.SimilarQuestion) error {
	tbl := d.Query().TQaSimilarQuestion
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}

	db = db.Table(tbl.TableName())

	db = d.getSimilarityQuestionConditions(ctx, db, req)

	// queryCond := getConditions(req)

	if err := db.Select(updateColumns).Updates(sq).Error; err != nil {
		logx.E(ctx, "BatchUpdateSimilarQuestionReleaseStatusByID failed,  error:%v", err)
		return err
	}

	// info, err := db.Where(queryCond...).Updates(updateColumns)

	logx.I(ctx, "BatchUpdateSimilarQuestionReleaseStatusByID success")
	return nil
}

func (d *daoImpl) BatchUpdateSimilarQuestion(ctx context.Context,
	req *qaEntity.SimilarityQuestionReq, updatedFieleds map[string]any, tx *gorm.DB) error {
	tbl := d.Query().TQaSimilarQuestion
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}

	db = db.Table(tbl.TableName())

	updateColumns := d.getSimilarityQuestionUpdateColumns(updatedFieleds)
	db = d.getSimilarityQuestionConditions(ctx, db, req)

	// queryCond := getConditions(req)

	err := db.Updates(updateColumns).Error

	// info, err := db.Where(queryCond...).Updates(updateColumns)

	if err != nil {
		logx.E(ctx, "BatchUpdateSimilarQuestionReleaseStatusByID failed,  error:%v", err)
		return err
	}

	logx.I(ctx, "BatchUpdateSimilarQuestionReleaseStatusByID success")
	return nil
}

func (d *daoImpl) GetQASimilarQuestionsCount(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) (int, error) {
	/*
		`
			SELECT COUNT(*) FROM t_qa_similar_question
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ?
		`
	*/
	tbl := d.Query().TQaSimilarQuestion
	db := tbl.WithContext(ctx).UnderlyingDB()
	db = db.Table(tbl.TableName())
	db = d.getSimilarityQuestionConditions(ctx, db, filter)

	var total int64

	if err := db.Count(&total).Error; err != nil {
		logx.E(ctx, "GetQASimilarQuestionsCount failed, err: %+v", err)
		return 0, err
	} else {
		return int(total), nil
	}
}

func (d *daoImpl) GetSimilarQuestionsCountByQAIDs(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) ([]*qaEntity.SimilarQuestionCount, error) {
	/*
		 `
		SELECT
			related_qa_id, count(*) as total
		FROM
		    t_qa_similar_question
		WHERE
		     %s
		GROUP BY related_qa_id
	*/
	tbl := d.Query().TQaSimilarQuestion
	db := tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())

	db = d.getSimilarityQuestionConditions(ctx, db, filter)

	var counts []*qaEntity.SimilarQuestionCount

	if err := db.Select(tbl.RelatedQaID.ColumnName().String(), "count(*) as total").
		Group(tbl.RelatedQaID.ColumnName().String()).
		Scan(&counts).Error; err != nil {
		logx.E(ctx, "GetSimilarQuestionsCountByQAIDs failed, err: %+v", err)
		return nil, err
	} else {
		return counts, nil
	}
}

func (d *daoImpl) GetSimilarQuestionsCharSize(ctx context.Context, filter *qaEntity.SimilarityQuestionReq) (uint64, uint64, error) {
	/*
			`
		        SELECT
				    IFNULL(SUM(char_size), 0),
				    IFNULL(SUM(qa_size), 0)
		        FROM
					t_qa_similar_question
		        WHERE
					robot_id = ?
		        AND corp_id = ?
		        AND is_deleted = ?
		        AND release_status NOT IN (%s)
		        AND similar_id IN (%s)
			`
	*/
	tbl := d.Query().TQaSimilarQuestion
	db := tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())

	db = d.getSimilarityQuestionConditions(ctx, db, filter)

	type Result struct {
		CharSizeTotal uint64
		QaSizeTotal   uint64
	}
	var result Result

	if err := db.Select(
		fmt.Sprintf("IFNULL(SUM(%s), 0) as char_size_total", tbl.CharSize.ColumnName().String()),
		fmt.Sprintf("IFNULL(SUM(%s), 0) as qa_size_total", tbl.QaSize.ColumnName().String()),
	).Scan(&result).Error; err != nil {
		logx.E(ctx, "GetSimilarQuestionsCharSize failed, err: %+v", err)
		return 0, 0, err
	} else {
		return result.CharSizeTotal, result.QaSizeTotal, nil
	}

}
