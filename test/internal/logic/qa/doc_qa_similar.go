package qa

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// SaveQaSimilar 保存相似问答对
func (l *Logic) SaveQaSimilar(ctx context.Context, qaSimilar *qaEntity.DocQASimilar) error {
	logx.I(ctx, "[SaveQaSimilar], Prepare to check DocQaSimilar Dedup Count.")

	total, err := l.qaDao.CheckDedupDocQASimilarCount(ctx, qaSimilar)
	if err != nil {
		logx.E(ctx, "[SaveQaSimilar], Check DocQaSimilar Dedup Count failed. err:%+v", err)
		return err
	}
	if total > 0 {
		logx.D(ctx, "[SaveQaSimilar], DocQaSimilar Dedup Count is %d", total)
		return nil
	}

	qaSimilar.BusinessID = idgen.GetId()

	err = l.qaDao.CreateDocQASimilar(ctx, qaSimilar)
	if err != nil {
		logx.E(ctx, "[SaveQaSimilar], Create DocQaSimilar failed. err:%+v", err)
		return err
	}
	logx.I(ctx, "[SaveQaSimilar], Create DocQaSimilar Success. %+v", qaSimilar)
	return nil
}

func (l *Logic) DeleteQASimilarByQA(ctx context.Context, qa *qaEntity.DocQA) error {
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

	if err := l.qaDao.DeleteQASimilarByQA(ctx, qa); err != nil {
		logx.E(ctx, "DeleteQASimilarByQA err:%+v", err)
		return err
	}
	return nil
}

// UpdateQASimilarsDocID 批量更新t_doc_qa_similar的DocID
func (l *Logic) UpdateQASimilarsDocID(ctx context.Context, qaIDs []uint64, docID uint64) error {

	/* `UPDATE t_doc_qa_similar SET doc_id = ?, update_time = ? WHERE qa_id IN (%s)` */
	updateColumns := map[string]any{
		qaEntity.DocQaSimTblColDocID:      docID,
		qaEntity.DocQaSimTblColUpdateTime: time.Now(),
	}

	filter := &qaEntity.DocQASimilarFilter{
		QaIDs: qaIDs,
	}

	if err := l.qaDao.BatchUpdateDocQASimilar(ctx, filter, updateColumns, nil); err != nil {
		logx.E(ctx, "UpdateQASimilarsDocID err:%+v", err)
		return err
	}
	return nil
}

// DoQASimilar 处理相似QA
func (l *Logic) DoQASimilar(ctx context.Context, corpID, robotID, staffID uint64, qaSimilarID []uint64,
	delQas []*qaEntity.DocQA) error {
	/*
			`
			UPDATE
				t_doc_qa_similar
			SET
			    is_valid = ?, status = ?, update_time = ?
			WHERE
			    id IN (%s)
		`
	*/
	if err := l.qaDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		if len(qaSimilarID) == 0 {
			return nil
		}

		filter := &qaEntity.DocQASimilarFilter{
			IDs: qaSimilarID,
		}

		updateColumns := map[string]any{
			qaEntity.DocQaSimTblColIsValid:    qaEntity.QaSimilarInValid,
			qaEntity.DocQaSimTblColStatus:     qaEntity.QaSimilarStatusDone,
			qaEntity.DocQaSimTblColUpdateTime: time.Now(),
		}

		if err := l.qaDao.BatchUpdateDocQASimilar(ctx, filter, updateColumns, nil); err != nil {
			logx.E(ctx, "Failed to DoQASimilar when DeleteQASimilarByQA. err:%+v", err)
			return err
		}

		if err := l.deleteQAs(ctx, corpID, robotID, staffID, delQas); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to DoQASimilar. err:%+v", err)
		return err
	}
	return nil
}

// DoQABizSimilar 处理相似QA
func (l *Logic) DoQABizSimilar(ctx context.Context, corpID, robotID, staffID uint64, qaSimilarBizID []uint64,
	delQas []*qaEntity.DocQA) error {
	/*
		`
		UPDATE
			t_doc_qa_similar
		SET
		    is_valid = ?, status = ?, update_time = ?
		WHERE
		    business_id IN (%s)
	*/
	if err := l.qaDao.Query().Transaction(func(query *mysqlquery.Query) error {
		if len(qaSimilarBizID) == 0 {
			return nil
		}
		filter := &qaEntity.DocQASimilarFilter{
			BusinessIDs: qaSimilarBizID,
		}

		updateColumns := map[string]any{
			qaEntity.DocQaSimTblColIsValid:    qaEntity.QaSimilarInValid,
			qaEntity.DocQaSimTblColStatus:     qaEntity.QaSimilarStatusDone,
			qaEntity.DocQaSimTblColUpdateTime: time.Now(),
		}
		if err := l.qaDao.BatchUpdateDocQASimilar(ctx, filter, updateColumns, nil); err != nil {
			logx.E(ctx, "Failed to DoQASimilar when DeleteQASimilarByQA. err:%+v", err)
			return err
		}

		if err := l.deleteQAs(ctx, corpID, robotID, staffID, delQas); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to DoQASimilar. err:%+v", err)
		return err
	}
	return nil
}

// ListQASimilar 获取相似问答对列表
func (l *Logic) ListQASimilar(ctx context.Context, corpID, robotID uint64, pageNumber, pageSize uint32) (
	uint64, []*qaEntity.DocQASimilar, error) {
	docQAFilter := &qaEntity.DocQASimilarFilter{
		CorpID:  corpID,
		RobotID: robotID,
		IsValid: ptrx.Uint32(qaEntity.QaSimilarIsValid),
		Status:  ptrx.Uint64(qaEntity.QaSimilarStatusInit),

		PageNo:   pageNumber,
		PageSize: pageSize,
	}

	qaSimilarList, total, err := l.qaDao.ListDocQaSimilars(ctx, nil, docQAFilter)

	if err != nil {
		logx.E(ctx, "[ListQASimilar], List DocQaSimilar failed. err:%+v", err)
		return 0, nil, err
	}
	return uint64(total), qaSimilarList, nil
}

// GetQASimilarBizID 获取相似问答对 ID
func (l *Logic) GetQASimilarBizID(ctx context.Context, corpID, bizID uint64) (
	*qaEntity.DocQASimilar, error) {
	docQAFilter := &qaEntity.DocQASimilarFilter{
		CorpID:     corpID,
		BusinessID: bizID,
	}

	qaSimilar, err := l.qaDao.GetDocQaSimilarByFilter(ctx, docQAFilter)

	if err != nil {
		logx.E(ctx, "[GetQASimilarBizID], Get DocQaSimilar failed. err:%+v", err)
		return nil, err
	}
	if qaSimilar == nil {
		return nil, errs.ErrQASimilarNotFound
	}
	return qaSimilar, nil
}

// IgnoreAllQASimilar 忽略当前所有相似问答对
func (l *Logic) IgnoreAllQASimilar(ctx context.Context, corpID, robotID uint64) error {
	/*
		`
			UPDATE
				t_doc_qa_similar
			SET
			    is_valid = ?, status = ?, update_time = ?
			WHERE
			    corp_id = ? AND robot_id = ? AND is_valid = ? AND status = ?
		`
	*/

	docQAFilter := &qaEntity.DocQASimilarFilter{
		CorpID:  corpID,
		RobotID: robotID,
		IsValid: ptrx.Uint32(qaEntity.QaSimilarIsValid),
		Status:  ptrx.Uint64(qaEntity.QaSimilarStatusInit),
	}

	updateColumns := map[string]any{
		qaEntity.DocQaSimTblColIsValid:    qaEntity.QaSimilarInValid,
		qaEntity.DocQaSimTblColStatus:     qaEntity.QaSimilarStatusDone,
		qaEntity.DocQaSimTblColUpdateTime: time.Now(),
	}

	if err := l.qaDao.BatchUpdateDocQASimilar(ctx, docQAFilter, updateColumns, nil); err != nil {
		logx.E(ctx, "IgnoreAllQASimilar err:%+v", err)
		return err
	}
	return nil
}
