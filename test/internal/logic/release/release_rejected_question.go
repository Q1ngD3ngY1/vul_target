package release

import (
	"context"
	"math"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gorm"
)

func (l *Logic) BatchCreateReleaseRejectedQuestions(ctx context.Context,
	releaseRejectedQuestions []*releaseEntity.ReleaseRejectedQuestion) error {
	if err := batchReleaseProcess(ctx, releaseRejectedQuestions, l.batchCreateReleaseRejectedQuestionUnit); err != nil {
		logx.W(ctx, "batchCreateReleaseRejectedQuestion err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) batchCreateReleaseRejectedQuestionUnit(ctx context.Context,
	releaseRejectedQuestions []*releaseEntity.ReleaseRejectedQuestion) error {
	defer timeTrack(ctx, time.Now(), "batchCreateReleaseRejectedQuestionUnit")
	if len(releaseRejectedQuestions) == 0 {
		return nil
	}
	chunkBatchSize := config.App().ReleaseParamConfig.CreateReleaseBatchSize
	if chunkBatchSize == 0 {
		chunkBatchSize = 200
	}

	rjQaChunks := slicex.Chunk(releaseRejectedQuestions, chunkBatchSize)
	for _, chunk := range rjQaChunks {
		batchReleaseRejQAs := chunk
		if err := l.releaseDao.MysqlQuery().TReleaseRejectedQuestion.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
			err := l.releaseDao.CreateReleaseRejectedQuestionRecords(ctx, batchReleaseRejQAs, tx)
			if err != nil {
				logx.E(ctx, "[batchCreateReleaseRejectedQuestionUnit] create release rejected question record error:%v", err)
				return err
			}

			rejectedQuestionIds := make([]uint64, 0, len(batchReleaseRejQAs))
			for _, question := range batchReleaseRejQAs {
				rejectedQuestionIds = append(rejectedQuestionIds, question.RejectedQuestionID)
			}

			logx.I(ctx, "[batchCreateReleaseRejectedQuestionUnit] "+
				"update release status of rejected questions to [ReleaseRejectedQuestionReleaseStatusIng]...")
			filter := &qaEntity.RejectedQuestionFilter{
				IDs: rejectedQuestionIds,
			}
			updateColumns := map[string]any{
				l.releaseDao.MysqlQuery().TRejectedQuestion.ReleaseStatus.ColumnName().String(): releaseEntity.ReleaseRejectedQuestionReleaseStatusIng,
				l.releaseDao.MysqlQuery().TRejectedQuestion.UpdateTime.ColumnName().String():    time.Now(),
			}
			// err = l.qaDao.BatchUpdateRejectedQuestionsReleaseStatusByID(
			//	ctx, rejectedQuestionIds, releaseEntity.ReleaseRejectedQuestionReleaseStatusIng)
			err = l.qaDao.BatchUpdateRejectedQuestion(ctx, filter, updateColumns, tx)
			if err != nil {
				logx.E(ctx,
					"[batchCreateReleaseRejectedQuestionUnit]"+
						"update rejected questions to release status [ReleaseRejectedQuestionReleaseStatusIng] error:%v",
					err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "[batchCreateReleaseRejectedQuestionUnit] Failed. err:%+v", err)
			return err
		}
	}
	return nil

}

func (l *Logic) ReleaseRejectedQuestion(ctx context.Context,
	releaseRejectedQuestions []*releaseEntity.ReleaseRejectedQuestion, tx *gorm.DB) error {
	releaseRejectedQuestionIds := make([]uint64, 0, len(releaseRejectedQuestions))
	if len(releaseRejectedQuestions) == 0 {
		return nil
	}

	for _, question := range releaseRejectedQuestions {
		releaseRejectedQuestionIds = append(releaseRejectedQuestionIds, question.RejectedQuestionID)
	}

	filter := &qaEntity.RejectedQuestionFilter{
		IDs: releaseRejectedQuestionIds,
	}

	updates := map[string]any{
		l.releaseDao.MysqlQuery().TRejectedQuestion.ReleaseStatus.ColumnName().String(): qaEntity.RejectedQuestionReleaseStatusSuccess,
		l.releaseDao.MysqlQuery().TRejectedQuestion.UpdateTime.ColumnName().String():    time.Now(),
	}

	err := l.qaDao.BatchUpdateRejectedQuestion(ctx, filter, updates, tx)

	if err != nil {
		logx.E(ctx,
			"update rejected questions to release status [ReleaseRejectedQuestionReleaseStatusSuccess] error:%v",
			err)
		return err
	}

	return nil
}

func (l *Logic) GetModifyRejectedQuestionList(ctx context.Context, corpID, robotID, versionID uint64, question string,
	page, pageSize uint32) ([]*releaseEntity.ReleaseRejectedQuestion, error) {
	req := releaseEntity.ListReleaseRejectedQuestionReq{
		CorpID:    corpID,
		RobotID:   robotID,
		VersionID: versionID,
		Question:  question,
		Page:      page,
		PageSize:  pageSize,
	}
	return l.releaseDao.GetModifyRejectedQuestionList(ctx, req)
}

func (l *Logic) GetModifyRejectedQuestionCount(ctx context.Context, corpID uint64, robotID uint64, versionID uint64,
	question string, releaseStatuses []uint32) (uint64, error) {
	count, err := l.releaseDao.GetModifyRejectedQuestionCount(ctx, corpID, robotID, versionID, question, releaseStatuses)
	if err != nil {
		logx.E(ctx, "Failed to GetModifyRejectedQuestionCount. err:%+v", err)
		return 0, err
	}
	return count, nil
}

// GetReleaseRejectQuestionID 获取待发布的拒答问题ID 区分非删除和删除的
func (l *Logic) GetReleaseRejectQuestionID(ctx context.Context, robotID, versionID uint64) (
	notDeletedRejectedQAIDs []uint64, deletedRejectedQAIDs []uint64, err error) {
	notDeletedRejectedQAIDs, err = l.getAllRejectQuestionIDWithDeleteFlag(ctx, robotID, versionID, false)
	if err != nil {
		return nil, nil, err
	}
	deletedRejectedQAIDs, err = l.getAllRejectQuestionIDWithDeleteFlag(ctx, robotID, versionID, true)
	if err != nil {
		logx.E(ctx, "GetReleaseRejectQuestionID error: %v", err)
		return nil, nil, err
	}
	return notDeletedRejectedQAIDs, deletedRejectedQAIDs, nil
}

// getAllRejectQuestionIDWithDeleteFlag 获取所有删除或非删除的拒答问题ID
func (l *Logic) getAllRejectQuestionIDWithDeleteFlag(ctx context.Context, robotID uint64, versionID uint64, isDeleted bool) ([]uint64, error) {

	limit := config.App().ReleaseParamConfig.GetIDsChunkSize
	rejectedQuestionID := uint64(0)
	var allIDs []uint64

	isAllowRelease := entity.AllowRelease
	delFlag := uint32(2)

	for {
		listReq := releaseEntity.ListReleaseRejectedQuestionReq{
			RobotID:        robotID,
			VersionID:      versionID,
			PageSize:       limit,
			IsAllowRelease: &isAllowRelease,
			OrderBy:        "qa_id",
		}

		if isDeleted {
			/*
								`SELECT rejected_question_id FROM t_release_rejected_question
				    	WHERE robot_id = ? AND version_id = ? AND  is_deleted = 2 order by rejected_question_id asc limit ?`
			*/
			listReq.MinRejectedQuestionID = rejectedQuestionID
			listReq.IsDeleted = &delFlag
		} else {
			/*
								`SELECT rejected_question_id FROM t_release_rejected_question
				    	WHERE robot_id = ? AND version_id = ? AND  is_deleted != 2 order by rejected_question_id asc limit ? `
			*/
			listReq.MinRejectedQuestionID = rejectedQuestionID
			listReq.IsDeletedNot = &delFlag
		}

		list, err := l.releaseDao.GetModifyRejectedQuestionList(ctx, listReq)
		if err != nil {
			return nil, err
		}

		ids := make([]uint64, 0, len(list))
		for _, item := range list {
			ids = append(ids, item.RejectedQuestionID)
		}
		allIDs = append(allIDs, ids...)

		if len(list) < int(limit) {
			break
		}
		rejectedQuestionID = ids[len(ids)-1]
	}

	return allIDs, nil
}

// GetReleaseRejectedQuestionList 获取待发布拒答问题列表
func (l *Logic) GetReleaseRejectedQuestionList(ctx context.Context, corpID, robotID uint64, page, pageSize uint32,
	query string, startTime, endTime time.Time, status []uint32) ([]*qaEntity.RejectedQuestion, error) {

	rejectedQuestions, err := l.qaDao.GetReleaseRejectedQuestionList(ctx, corpID, robotID, page, pageSize, query, startTime, endTime, status)
	if err != nil {
		logx.E(ctx, "Failed to GetReleaseRejectedQuestionList. err:%+v", err)
		return nil, err
	}

	return rejectedQuestions, nil
}

// GetReleaseRejectedQuestionCount 发布拒答问题预览数量
func (l *Logic) GetReleaseRejectedQuestionCount(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, status []uint32) (uint64, error) {
	count, err := l.qaDao.GetReleaseRejectedQuestionCount(ctx, corpID, robotID, question, startTime, endTime, status)
	if err != nil {
		logx.E(ctx, "Failed to GetRejectedQuestionListCount. err:%+v", err)
		return 0, err
	}
	return count, nil
}

// getReleaseRejectedQuestion 获取待发布拒答问题
func (l *Logic) getReleaseRejectedQuestion(ctx context.Context, corpID,
	robotID uint64) ([]*releaseEntity.ReleaseRejectedQuestion, error) {
	var (
		query     string
		startTime time.Time
		endTime   time.Time
		status    []uint32
	)
	total, err := l.GetReleaseRejectedQuestionCount(ctx, corpID, robotID, query, startTime, endTime, status)
	if err != nil {
		return nil, err
	}
	releaseRejectedQuestion := make([]*releaseEntity.ReleaseRejectedQuestion, 0, total)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for page := 1; page <= pages; page++ {
		list, err := l.GetReleaseRejectedQuestionList(ctx, corpID, robotID, uint32(page), uint32(pageSize),
			query, startTime, endTime, status)
		if err != nil {
			return nil, err
		}
		for _, v := range list {
			releaseRejectedQuestion = append(releaseRejectedQuestion, &releaseEntity.ReleaseRejectedQuestion{
				// ID:                 v.ID,
				CorpID:             v.CorpID,
				RobotID:            v.RobotID,
				CreateStaffID:      v.CreateStaffID,
				VersionID:          0,
				RejectedQuestionID: v.ID,
				Message:            "",
				Question:           v.Question,
				ReleaseStatus:      v.ReleaseStatus,
				IsDeleted:          v.IsDeleted,
				Action:             uint32(v.Action),
				IsAllowRelease:     uint32(1),
			})
		}
	}
	return releaseRejectedQuestion, nil
}

func (l *Logic) releaseRejectedQuestionNotify(ctx context.Context, isSuccess bool, reason string, id uint64,
	release *releaseEntity.Release) error {
	rejectedQuestion, err := l.qaDao.GetRejectedQuestionByID(ctx, id)
	if err != nil {
		return err
	}
	if rejectedQuestion == nil {
		return errs.ErrRejectedQuestionNotFound
	}
	modifyRejectedQuestions, err := l.releaseDao.GetReleaseModifyRejectedQuestion(ctx, release,
		[]*qaEntity.RejectedQuestion{rejectedQuestion})
	if err != nil {
		return err
	}
	modifyRejectedQuestion, ok := modifyRejectedQuestions[rejectedQuestion.ID]
	if err != nil {
		return err
	}
	if !ok {
		logx.D(ctx, "当前版本没有修改这个拒答问题 %v", rejectedQuestion)
		return errs.ErrRejectedQuestionNotModifyFound
	}
	rejectedQuestion.ReleaseStatus = util.When(isSuccess, qaEntity.RejectedQuestionReleaseStatusSuccess,
		qaEntity.RejectedQuestionReleaseStatusFail)
	rejectedQuestion.Action = qaEntity.RejectedQuestionPublish

	modifyRejectedQuestion.Message = reason
	modifyRejectedQuestion.ReleaseStatus = rejectedQuestion.ReleaseStatus
	if err = l.PublishRejectedQuestion(ctx, rejectedQuestion, modifyRejectedQuestion); err != nil {
		return err
	}

	return nil
}

func (l *Logic) PublishRejectedQuestion(ctx context.Context, rejectedQuestion *qaEntity.RejectedQuestion,
	modifyRejectedQuestion *releaseEntity.ReleaseRejectedQuestion) error {
	logx.D(ctx, "PublishRejectedQuestion rejectedQuestion:%+v, releaseRejectedQuestion:%+v",
		rejectedQuestion, modifyRejectedQuestion)
	now := time.Now()
	return l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
		/*
			`
					UPDATE
						t_rejected_question
					SET
						action = :action,
						release_status = :release_status,
						update_time = update_time
					WHERE
						id = :id
				`
		*/

		rejectedQuestion.UpdateTime = now
		reQATbl := tx.TRejectedQuestion
		filter := &qaEntity.RejectedQuestionFilter{
			ID: rejectedQuestion.ID,
		}

		updateColumns := []string{
			reQATbl.Action.ColumnName().String(),
			reQATbl.ReleaseStatus.ColumnName().String(),
			reQATbl.UpdateTime.ColumnName().String(),
		}

		if err := l.qaDao.UpdateRejectedQuestion(ctx, filter, updateColumns, rejectedQuestion,
			reQATbl.WithContext(ctx).UnderlyingDB()); err != nil {
			logx.E(ctx, "Failed to update rejected question err:%+v", err)
			return err
		}

		/*
			`
					UPDATE
						t_release_rejected_question
					SET
						release_status = :release_status,
						message = :message,
						update_time = update_time
					WHERE
						id = :id
		*/

		modifyRejectedQuestion.UpdateTime = now
		reRQATbl := tx.TReleaseRejectedQuestion
		rFilter := &releaseEntity.ReleaseRejectedQuestionFilter{
			ID: modifyRejectedQuestion.ID,
		}

		updateColumns = []string{
			reRQATbl.ReleaseStatus.ColumnName().String(),
			reRQATbl.Message.ColumnName().String(),
			reRQATbl.UpdateTime.ColumnName().String(),
		}

		if err := l.releaseDao.UpdateReleaseRejectedQuestionRecords(ctx, updateColumns, rFilter, modifyRejectedQuestion,
			reRQATbl.WithContext(ctx).UnderlyingDB()); err != nil {
			logx.E(ctx, "Failed to update release rejected question err:%+v", err)
			return err
		}

		return nil
	})
}
