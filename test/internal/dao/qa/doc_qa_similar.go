package qa

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) getDocQaSimilarCondition(ctx context.Context, session *gorm.DB, filter *qaEntity.DocQASimilarFilter) *gorm.DB {
	tbl := d.Query().TDocQaSimilar
	if len(filter.IDs) > 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlIn, filter.IDs)
	}
	if filter.QaID > 0 {
		session = session.Where(tbl.QaID.ColumnName().String()+util.SqlEqual, filter.QaID)
	}
	if len(filter.QaIDs) > 0 {
		session = session.Where(tbl.QaID.ColumnName().String()+util.SqlIn, filter.QaIDs)
	}
	if filter.SimilarID > 0 {
		session = session.Where(tbl.SimilarID.ColumnName().String()+util.SqlEqual, filter.SimilarID)
	}
	if len(filter.SimilarIDs) > 0 {
		session = session.Where(tbl.SimilarID.ColumnName().String()+util.SqlIn, filter.SimilarIDs)
	}
	if filter.BusinessID > 0 {
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlEqual, filter.BusinessID)
	}
	if len(filter.BusinessIDs) > 0 {
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, filter.BusinessIDs)
	}
	if filter.Status != nil {
		session = session.Where(tbl.Status.ColumnName().String()+util.SqlEqual, *filter.Status)
	}
	if filter.IsValid != nil {
		session = session.Where(tbl.IsValid.ColumnName().String()+util.SqlEqual, *filter.IsValid)
	}

	if filter.CorpID > 0 {
		session = session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, filter.CorpID)
	}

	// RobotIDs和RobotID互斥使用，优先使用RobotIDs
	if len(filter.RobotIDs) > 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlIn, filter.RobotIDs)
	} else if filter.RobotID > 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.RobotID)
	}
	return session
}

func (d *daoImpl) CreateDocQASimilar(ctx context.Context, docQaSimilar *qaEntity.DocQASimilar) error {
	tbl := d.Query().TDocQaSimilar
	tDocSim := ConvertDocQaSimilarDO2PO(docQaSimilar)
	if err := tbl.WithContext(ctx).Create(tDocSim); err != nil {
		logx.E(ctx, "Failed to create docQaSimilar. err:%+v", err)
		return err
	}

	return nil
}

func (d *daoImpl) UpdateDocQASimilar(ctx context.Context, updateColumns []string, filter *qaEntity.DocQASimilarFilter, docQaSimilar *qaEntity.DocQASimilar) error {
	tbl := d.Query().TDocQaSimilar
	tableName := tbl.TableName()
	db := tbl.WithContext(ctx).UnderlyingDB()
	db = d.getDocQaSimilarCondition(ctx, db, filter)
	tdocQaSimilar := ConvertDocQaSimilarDO2PO(docQaSimilar)

	if err := db.Table(tableName).Select(updateColumns).Updates(tdocQaSimilar).Error; err != nil {
		logx.E(ctx, "Failed to update docQaSimilar. err:%+v", err)
		return err
	}

	return nil
}

func (d *daoImpl) CheckDedupDocQASimilarCount(ctx context.Context, docQaSimilar *qaEntity.DocQASimilar) (int64, error) {
	checkSql := `
		SELECT
			count(*)
		FROM
			t_doc_qa_similar
		WHERE 
			((qa_id = ? AND similar_id = ?) OR (qa_id = ? AND similar_id = ?))
		AND 
			corp_id = ? AND is_valid = ? AND status = ?`

	queryArgs := []any{docQaSimilar.QaID, docQaSimilar.SimilarID, docQaSimilar.SimilarID, docQaSimilar.QaID, docQaSimilar.CorpID,
		qaEntity.QaSimilarIsValid, qaEntity.QaSimilarStatusInit}

	var count int64
	err := d.Query().TDocQaSimilar.WithContext(ctx).UnderlyingDB().Raw(checkSql, queryArgs...).Scan(&count).Error
	if err != nil {
		logx.E(ctx, "Failed to check dedup docQaSimilar. err:%+v", err)
		return 0, err
	}
	logx.I(ctx, "CheckDedupDocQASimilarCount app:%d count:%d, qaID:%d, similarID:%d", docQaSimilar.RobotID, count, docQaSimilar.QaID, docQaSimilar.SimilarID)
	return count, nil
}

func (d *daoImpl) BatchUpdateDocQASimilar(ctx context.Context, filter *qaEntity.DocQASimilarFilter, updatedFieleds map[string]any, tx *gorm.DB) error {
	tbl := d.Query().TDocQaSimilar
	tableName := tbl.TableName()
	db := tx

	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	db = db.Table(tableName)
	db = d.getDocQaSimilarCondition(ctx, db, filter)
	if err := db.Updates(updatedFieleds).Error; err != nil {
		logx.E(ctx, "Failed to update docQaSimilar. err:%+v", err)
		return err
	}

	return nil
}

func (d *daoImpl) GetDocQaSimilarListCount(ctx context.Context, filter *qaEntity.DocQASimilarFilter) (int64, error) {
	/*
		  `
			SELECT
				count(*)
			FROM
			    t_doc_qa_similar
			WHERE
			   xxx
		`
	*/
	tbl := d.Query().TDocQaSimilar
	db := tbl.WithContext(ctx).UnderlyingDB()
	db = db.Table(tbl.TableName())
	db = d.getDocQaSimilarCondition(ctx, db, filter)

	if filter.PageNo > 0 || filter.PageSize > 0 {
		offset, limit := utilx.Page(filter.PageNo, filter.PageSize)

		db = db.Offset(offset).Limit(limit)
	}

	var count int64
	err := db.Count(&count).Error
	if err != nil {
		logx.E(ctx, "Failed to get docQaSimilar list count. err:%+v", err)
		return 0, err
	}
	return count, nil

}

func (d *daoImpl) GetDocQaSimilarList(ctx context.Context, selectColumns []string, filter *qaEntity.DocQASimilarFilter) ([]*qaEntity.DocQASimilar, error) {
	/*
		  `
			SELECT
				%s
			FROM
			    t_doc_qa_similar
			WHERE
			   xxx
		`
	*/
	tbl := d.Query().TDocQaSimilar
	db := tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())
	if len(selectColumns) > 0 {
		db = db.Select(selectColumns)
	}
	db = d.getDocQaSimilarCondition(ctx, db, filter)

	if filter.PageNo > 0 || filter.PageSize > 0 {
		offset, limit := utilx.Page(filter.PageNo, filter.PageSize)

		db = db.Offset(offset).Limit(limit)
	}

	res := make([]*model.TDocQaSimilar, 0)

	if err := db.Find(&res).Error; err != nil {
		logx.E(ctx, "Failed to get docQaSimilar list. err:%+v", err)
		return nil, err
	}
	return BatchConvertDocQaSimilarPO2DO(res), nil
}

func (d *daoImpl) ListDocQaSimilars(ctx context.Context, selectColumns []string, filter *qaEntity.DocQASimilarFilter) (
	[]*qaEntity.DocQASimilar, int64, error) {
	count, err := d.GetDocQaSimilarListCount(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocQaSimilarList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

func (d *daoImpl) GetDocQaSimilarByFilter(ctx context.Context, filter *qaEntity.DocQASimilarFilter) (*qaEntity.DocQASimilar, error) {
	/*
		  `
			SELECT
				%s
			FROM
			    t_doc_qa_similar
			WHERE
			   xxx
		`
	*/
	tbl := d.Query().TDocQaSimilar
	db := tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())
	db = d.getDocQaSimilarCondition(ctx, db, filter)

	res := &model.TDocQaSimilar{}
	if err := db.First(res).Error; err != nil {
		logx.E(ctx, "Failed to get docQaSimilar. err:%+v", err)
		return nil, err
	}
	return ConvertDocQaSimilarPO2DO(res), nil
}

func (d *daoImpl) DeleteQASimilarByQA(ctx context.Context, qa *qaEntity.DocQA) error {

	/*
		`
			UPDATE
				t_doc_qa_similar
			SET
			    is_valid = ?, update_time = ?
			WHERE
			    (qa_id IN (%s) OR similar_id IN (%s)) AND status = ?
		`
	*/
	tbl := d.Query().TDocQaSimilar
	db := tbl.WithContext(ctx)

	db = db.Where(tbl.Status.Eq(qaEntity.QaSimilarStatusInit)).
		Where(field.Or(tbl.QaID.In(qa.ID), tbl.SimilarID.In(qa.ID)))

	updateColumns := map[string]any{
		qaEntity.DocQaSimTblColIsValid:    qaEntity.QaSimilarInValid,
		qaEntity.DocQaSimTblColUpdateTime: time.Now(),
	}

	if info, err := db.Updates(updateColumns); err != nil {
		logx.E(ctx, "Failed to update docQaSimilar. err:%+v", err)
		return err
	} else {
		logx.I(ctx, "update docQaSimilar info:%+v", info)
	}
	return nil
}

func (d *daoImpl) DeleteQASimilarByBizIDs(ctx context.Context, businessIDs []uint64) error {

	/*
		`
			UPDATE
				t_doc_qa_similar
			SET
			    is_valid = ?, update_time = ?
			WHERE
			    business_id IN (%s) AND status = ?
		`
	*/
	tbl := d.Query().TDocQaSimilar
	db := tbl.WithContext(ctx)

	db = db.Where(tbl.Status.Eq(qaEntity.QaSimilarStatusInit)).
		Where(tbl.BusinessID.In(businessIDs...))

	updateColumns := map[string]any{
		qaEntity.DocQaSimTblColIsValid:    qaEntity.QaSimilarInValid,
		qaEntity.DocQaSimTblColUpdateTime: time.Now(),
	}

	if info, err := db.Updates(updateColumns); err != nil {
		logx.E(ctx, "Failed to update docQaSimilar by businessIDs. err:%+v", err)
		return err
	} else {
		logx.I(ctx, "update docQaSimilar by businessIDs info:%+v", info)
	}
	return nil
}
