package qa

import (
	"context"
	"math"
	"time"

	"git.woa.com/adp/common/x/logx"
	"gorm.io/gorm"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	pb "git.woa.com/adp/pb-go/app/app_config"
)

// CreateRejectedQuestion 创建拒答问题
func (l *Logic) CreateRejectedQuestion(ctx context.Context, rejectedQuestion *qaEntity.RejectedQuestion) error {
	var syncID uint64
	err := l.qaDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		/*
				`
				INSERT INTO
					t_rejected_question (%s)
				VALUES
					(null,:business_id,:corp_id,:robot_id,:create_staff_id,:business_source_id,:business_source,:question,
					 :release_status,:is_deleted,:action,:update_time,:create_time)
			`
		*/
		now := time.Now()
		rejectedQuestion.UpdateTime = now
		rejectedQuestion.CreateTime = now
		rejectedQuestion.ReleaseStatus = qaEntity.RejectedQuestionReleaseStatusInit
		rejectedQuestion.IsDeleted = qaEntity.RejectedQuestionIsNotDeleted
		rejectedQuestion.Action = qaEntity.RejectedQuestionAdd

		err := l.qaDao.CreateRejectedQuestion(ctx, rejectedQuestion)
		if err != nil {
			logx.E(ctx, "创建拒答问题失败 err:%+v", err)
			return err
		}

		syncID, err = l.vectorSyncLogic.AddRejectedQuestionSync(ctx, rejectedQuestion)
		if err != nil {
			return err
		}
		if rejectedQuestion.BusinessSource == qaEntity.BusinessSourceUnsatisfiedReply && rejectedQuestion.BusinessSourceID != 0 {
			// err := l.rawSqlDao.UpdateUnsatisfiedReplyStatus(ctx, rejectedQuestion.CorpPrimaryId, rejectedQuestion.AppPrimaryId,
			// 	[]uint64{rejectedQuestion.BusinessSourceID}, entity.UnsatisfiedReplyStatusWait,
			// 	entity.UnsatisfiedReplyStatusReject)
			updateReq := &pb.ModifyUnsatisfiedReplyReq{
				CorpId:     rejectedQuestion.CorpID,
				AppId:      rejectedQuestion.RobotID,
				ReplyBizId: []uint64{rejectedQuestion.BusinessSourceID},
				OldStatus:  entity.UnsatisfiedReplyStatusWait,
				NewStatus:  entity.UnsatisfiedReplyStatusReject,
			}

			_, err := l.rpc.AppAdmin.ModifyUnsatisfiedReply(ctx, updateReq)
			if err != nil {
				logx.E(ctx, "Failed to modify unsatisfied reply err:%+v", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	l.vectorSyncLogic.Push(ctx, syncID)
	return nil
}

func (l *Logic) ListRejectedQuestion(ctx context.Context, req *qaEntity.RejectedQuestionFilter) ([]*qaEntity.RejectedQuestion, int64, error) {
	list, total, err := l.qaDao.ListRejectedQuestion(ctx, []string{}, req)
	if err != nil {
		return nil, 0, err
	}

	return list, total, err
}

// UpdateRejectedQuestion 修改拒答问题
func (l *Logic) UpdateRejectedQuestion(ctx context.Context, rejectedQuestion *qaEntity.RejectedQuestion,
	isNeedPublish bool) error {
	var syncID uint64
	err := l.qaDao.Query().TRejectedQuestion.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		/*
			`
			UPDATE
				t_rejected_question
			SET
				update_time = ?,
				release_status = ?
			WHERE
				id IN (%s)

		*/
		now := time.Now()
		rejectedQuestion.UpdateTime = now
		filter := &qaEntity.RejectedQuestionFilter{
			ID: rejectedQuestion.ID,
		}
		updateColumns := []string{qaEntity.RejectedQuestionTblColQuestion, qaEntity.RejectedQuestionTblColUpdateTime,
			qaEntity.RejectedQuestionTblColReleaseStatus, qaEntity.RejectedQuestionTblColCreateStaffID}
		err := l.qaDao.UpdateRejectedQuestion(ctx, filter, updateColumns, rejectedQuestion, tx)

		if err != nil {
			logx.E(ctx, "Failed to UpdateRejectedQuestion args:%+v err:%+v", rejectedQuestion, err)
			return err
		}

		if !isNeedPublish {
			return nil
		}
		syncID, err = l.vectorSyncLogic.AddRejectedQuestionSync(ctx, rejectedQuestion)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !isNeedPublish {
		return nil
	}
	l.vectorSyncLogic.Push(ctx, syncID)
	return nil
}

func (l *Logic) UpdateRejectQuestionsReleaseStatus(ctx context.Context,
	rejectedQuestions []*qaEntity.RejectedQuestion, releaseStatus uint32) error {
	updateTime := time.Now()

	for _, v := range rejectedQuestions {
		v.ReleaseStatus = releaseStatus
		v.UpdateTime = updateTime
	}
	return l.qaDao.BatchUpdateRejectedQuestions(ctx, rejectedQuestions)
}

// DeleteRejectedQuestion 删除拒答问题
func (l *Logic) DeleteRejectedQuestion(ctx context.Context, corpID, robotID uint64,
	rejectedQuestions []*qaEntity.RejectedQuestion) error {
	return l.qaDao.Query().TRejectedQuestion.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		return l.deleteRejectedQuestion(ctx, tx, corpID, robotID, rejectedQuestions)
	})
}

func (l *Logic) deleteRejectedQuestion(ctx context.Context, tx *gorm.DB, corpID, robotID uint64,
	rejectedQuestions []*qaEntity.RejectedQuestion) error {
	rejectedQAAdd := make([]*qaEntity.RejectedQuestion, 0, len(rejectedQuestions))
	rejectedQANotAdd := make([]*qaEntity.RejectedQuestion, 0, len(rejectedQuestions))
	for _, v := range rejectedQuestions {
		if v.Action == qaEntity.RejectedQuestionAdd {
			rejectedQAAdd = append(rejectedQAAdd, v)
		} else {
			rejectedQANotAdd = append(rejectedQANotAdd, v)
		}
	}
	pageSize := 100
	pagesNotAdd := int(math.Ceil(float64(len(rejectedQANotAdd)) / float64(pageSize)))
	syncIDsNotAdd, err := l.deleteRejectedQuestionNotAdd(ctx, tx, corpID, robotID, pagesNotAdd, pageSize,
		rejectedQANotAdd)
	if err != nil {
		return err
	}
	pagesAdd := int(math.Ceil(float64(len(rejectedQAAdd)) / float64(pageSize)))
	syncIDsAdd, err := l.deleteRejectedQuestionAdd(ctx, tx, corpID, robotID, pagesAdd, pageSize, rejectedQAAdd)
	if err != nil {
		return err
	}
	var syncIDs []uint64
	syncIDs = append(syncIDs, syncIDsNotAdd...)
	syncIDs = append(syncIDs, syncIDsAdd...)
	for _, syncID := range syncIDs {
		l.vectorSyncLogic.Push(ctx, syncID)
	}
	return nil
}

func (l *Logic) deleteRejectedQuestionAdd(ctx context.Context, tx *gorm.DB, corpID uint64, robotID uint64, pagesAdd int,
	pageSize int, rejectedQAAdd []*qaEntity.RejectedQuestion) ([]uint64, error) {
	length := len(rejectedQAAdd)
	now := time.Now()
	var syncIDs []uint64
	/*
		`
			UPDATE
				t_rejected_question
			SET
				is_deleted = ?,
				update_time = ?
			WHERE
				corp_id = ?
				AND robot_id = ? %s
	*/
	updateColumns := map[string]any{
		qaEntity.RejectedQuestionTblColIsDeleted:  qaEntity.RejectedQuestionIsDeleted,
		qaEntity.RejectedQuestionTblColUpdateTime: now,
	}
	filter := &qaEntity.RejectedQuestionFilter{
		CorpID:  corpID,
		RobotID: robotID,
	}
	for i := 0; i < pagesAdd; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpRejectedQuestion := rejectedQAAdd[start:end]
		args := make([]uint64, 0)

		for _, id := range tmpRejectedQuestion {
			args = append(args, id.ID)
		}
		filter.IDs = args

		if err := l.qaDao.BatchUpdateRejectedQuestion(ctx, filter, updateColumns, tx); err != nil {
			logx.E(ctx, "Failed to BatchUpdateRejectedQuestion when deleteRejectedQuestionAdd. err:%+v", err)
			return nil, err
		}

		for _, id := range tmpRejectedQuestion {
			syncID, err := l.vectorSyncLogic.AddRejectedQuestionSync(ctx, &qaEntity.RejectedQuestion{
				ID: id.ID,
			})
			if err != nil {
				return nil, err
			}
			syncIDs = append(syncIDs, syncID)
		}
	}
	return syncIDs, nil
}

func (l *Logic) deleteRejectedQuestionNotAdd(ctx context.Context, tx *gorm.DB, corpID uint64, robotID uint64,
	pagesNotAdd int, pageSize int, rejectedQANotAdd []*qaEntity.RejectedQuestion) ([]uint64, error) {
	length := len(rejectedQANotAdd)
	now := time.Now()
	var syncIDs []uint64
	/*
		`
			UPDATE
				t_rejected_question
			SET
				action = ?,
				is_deleted = ?,
				release_status = ?,
				update_time = ?
			WHERE
				corp_id = ?
				AND robot_id = ? %s
		`
	*/
	updateColumns := map[string]any{
		qaEntity.RejectedQuestionTblColAction:        qaEntity.RejectedQuestionDelete,
		qaEntity.RejectedQuestionTblColReleaseStatus: qaEntity.RejectedQuestionReleaseStatusInit,
		qaEntity.RejectedQuestionTblColIsDeleted:     qaEntity.RejectedQuestionIsDeleted,
		qaEntity.RejectedQuestionTblColUpdateTime:    now,
	}
	filter := &qaEntity.RejectedQuestionFilter{
		CorpID:  corpID,
		RobotID: robotID,
	}
	for i := 0; i < pagesNotAdd; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpRejectedQuestion := rejectedQANotAdd[start:end]
		args := make([]uint64, 0)

		for _, id := range tmpRejectedQuestion {
			args = append(args, id.ID)
		}
		filter.IDs = args
		if err := l.qaDao.BatchUpdateRejectedQuestion(ctx, filter, updateColumns, tx); err != nil {
			logx.E(ctx, "Failed to BatchUpdateRejectedQuestion when deleteRejectedQuestionNotAdd. err:%+v", err)
			return nil, err
		}

		for _, id := range tmpRejectedQuestion {
			syncID, err := l.vectorSyncLogic.AddRejectedQuestionSync(ctx, &qaEntity.RejectedQuestion{
				ID: id.ID,
			})
			if err != nil {
				return nil, err
			}
			syncIDs = append(syncIDs, syncID)
		}
	}
	return syncIDs, nil
}

// GetRejectedQuestionByID 按 ID 查询拒答问题
func (l *Logic) GetRejectedQuestionByID(ctx context.Context, corpId, robotId, id uint64) (*qaEntity.RejectedQuestion, error) {
	/*
					`
			    SELECT
		      		%s
		    	FROM
		      		t_rejected_question
				WHERE
					id = ? AND corp_id = ? AND robot_id =
	*/
	filter := &qaEntity.RejectedQuestionFilter{
		ID:      id,
		CorpID:  corpId,
		RobotID: robotId,
	}
	rejectedQuestion, err := l.qaDao.GetRejectedQuestion(ctx, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetRejectedQuestion. args:%+v, err:%+v", filter, err)
		return nil, err
	}
	return rejectedQuestion, nil
}

// GetRejectedQuestionByBizID 按 bizID 查询拒答问题
func (l *Logic) GetRejectedQuestionByBizID(ctx context.Context, corpId, robotId, bizID uint64) (*qaEntity.RejectedQuestion, error) {
	/*
			`
			    SELECT
		      		%s
		    	FROM
		      		t_rejected_question
				WHERE
					business_id = ? AND corp_id = ? AND robot_id = ?
			`
	*/
	filter := &qaEntity.RejectedQuestionFilter{
		BusinessID: bizID,
		CorpID:     corpId,
		RobotID:    robotId,
	}
	rejectedQuestion, err := l.qaDao.GetRejectedQuestion(ctx, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetRejectedQuestionByBizID. args:%+v, err:%+v", filter, err)
		return nil, err
	}
	return rejectedQuestion, nil
}

// GetRejectedQuestionByIDs 按多个ID查询拒答问题
func (l *Logic) GetRejectedQuestionByIDs(ctx context.Context, corpID uint64, ids []uint64) ([]*qaEntity.RejectedQuestion,
	error) {
	/*
				`
			    SELECT
		      		%s
		    	FROM
		      		t_rejected_question
				WHERE
					corp_id = ?
					AND id IN (%s)
			`
	*/
	filter := &qaEntity.RejectedQuestionFilter{
		IDs:    ids,
		CorpID: corpID,
	}

	selectColumns := qaEntity.RejectedQuestionTblColList

	list, err := l.qaDao.GetRejectedQuestionList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetRejectedQuestionByIDs. args:%+v, err:%+v", filter, err)
		return nil, err
	}
	return list, nil
}

// GetRejectedQuestionByBizIDs 按多个业务ID查询拒答问题
func (l *Logic) GetRejectedQuestionByBizIDs(ctx context.Context, corpID uint64,
	bizIDs []uint64) ([]*qaEntity.RejectedQuestion,
	error) {
	/*
				`
			    SELECT
		      		%s
		    	FROM
		      		t_rejected_question
				WHERE
					corp_id = ?
					AND business_id IN (%s)
			`
	*/
	filter := &qaEntity.RejectedQuestionFilter{
		BusinessIDs: bizIDs,
		CorpID:      corpID,
	}

	selectColumns := qaEntity.RejectedQuestionTblColList

	list, err := l.qaDao.GetRejectedQuestionList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetRejectedQuestionByBizIDs. args:%+v, err:%+v", filter, err)
		return nil, err
	}
	return list, nil
}

// GetRejectChunk 分段获取拒答
func (l *Logic) GetRejectChunk(ctx context.Context, corpID, appID, offset, limit uint64) ([]*qaEntity.RejectedQuestion, error) {
	/*
		`
			SELECT ` + rejectedQuestionField + ` FROM t_rejected_question
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND id > ?
			ORDER BY id ASC LIMIT ?
		`
	*/
	listReq := &qaEntity.RejectedQuestionFilter{
		CorpID:         corpID,
		RobotID:        appID,
		IsDeleted:      qaEntity.RejectedQuestionIsNotDeleted,
		IDMore:         offset,
		Limit:          uint32(limit),
		OrderColumn:    []string{qaEntity.RejectedQuestionTblColId},
		OrderDirection: []string{util.SqlOrderByAsc},
	}
	rejectedQuestions, _, err := l.qaDao.ListRejectedQuestion(ctx, []string{}, listReq)
	if err != nil {
		return nil, err
	}
	return rejectedQuestions, nil
}

// GetRejectChunkCount 获取拒答总数
func (l *Logic) GetRejectChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	/*
		`
			SELECT COUNT(*) FROM t_rejected_question
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ?
		`
	*/
	filter := &qaEntity.RejectedQuestionFilter{
		CorpID:    corpID,
		RobotID:   appID,
		IsDeleted: qaEntity.RejectedQuestionIsNotDeleted,
	}

	count, err := l.qaDao.GetRejectedQuestionListCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetRejectChunkCount. args:%+v, err:%+v", filter, err)
		return 0, err
	}
	return int(count), nil
}

// TODO: not-used
// func (d *dao) PublishRejectedQuestion(ctx context.Context, rejectedQuestion *qaEntity.RejectedQuestion,
// 	modifyRejectedQuestion *releaseEntity.ReleaseRejectedQuestion) error {
// 	now := time.Now()
// 	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
// 		SQL := publishRejectedQuestion
// 		rejectedQuestion.UpdateTime = now
// 		if _, err := tx.NamedExecContext(ctx, SQL, rejectedQuestion); err != nil {
// 			logx.E(ctx, "发布拒答问题失败 SQL:%s rejectedQuestion:%+v err:%+v", SQL, rejectedQuestion,
// 				err)
// 			return err
// 		}
// 		SQL = publishReleaseRejectedQuestion
// 		modifyRejectedQuestion.UpdateTime = now
// 		if _, err := tx.NamedExecContext(ctx, SQL, modifyRejectedQuestion); err != nil {
// 			logx.E(ctx, "发布拒答问题失败 SQL:%s modifyRejectedQuestion:%+v err:%+v", SQL,
// 				modifyRejectedQuestion, err)
// 			return err
// 		}
// 		return nil
// 	})
// }
