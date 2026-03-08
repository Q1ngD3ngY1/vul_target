package release

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/pkg/errs"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
)

func (l *Logic) BatchCreateReleaseSimilarQA(ctx context.Context, releaseSimilarQAs []*releaseEntity.ReleaseQaSimilarQuestion) error {
	if err := batchReleaseProcess(ctx, releaseSimilarQAs, l.batchCreateReleaseSimilarQAUnit); err != nil {
		logx.W(ctx, "batchCreateReleaseSimilarQA err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) batchCreateReleaseSimilarQAUnit(ctx context.Context, releaseSimilarQAs []*releaseEntity.ReleaseQaSimilarQuestion) error {
	defer timeTrack(ctx, time.Now(), "batchCreateReleaseSimilarQAUnit")
	if len(releaseSimilarQAs) == 0 {
		return nil
	}

	chunkBatchSize := config.App().ReleaseParamConfig.CreateReleaseBatchSize
	if chunkBatchSize == 0 {
		chunkBatchSize = 200
	}

	similarQAChunks := slicex.Chunk(releaseSimilarQAs, chunkBatchSize)

	for _, chunk := range similarQAChunks {
		simChunk := chunk
		if err := l.releaseDao.MysqlQuery().TReleaseQaSimilarQuestion.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {

			if err := l.releaseDao.CreateReleaseSimilarQuestionRecord(ctx, simChunk, tx); err != nil {
				logx.E(ctx, "batchCreateReleaseSimilarQAUnit.CreateReleaseSimilarQuestionRecord error. err:%+v", err)
				return err
			}

			/*
							`
						UPDATE
							t_qa_similar_question
						SET
						    update_time = ?,
						    release_status = ?
						WHERE
						    similar_id IN (%s)
				    `
			*/
			var similarIds []uint64
			for _, item := range simChunk {
				similarIds = append(similarIds, uint64(item.SimilarID))
			}

			filter := &qaEntity.SimilarityQuestionReq{
				SimilarQuestionIDs: similarIds,
			}
			updateColumns := map[string]any{
				l.releaseDao.MysqlQuery().TReleaseQaSimilarQuestion.UpdateTime.ColumnName().String():    time.Now(),
				l.releaseDao.MysqlQuery().TReleaseQaSimilarQuestion.ReleaseStatus.ColumnName().String(): qaEntity.QAReleaseStatusIng,
			}

			if err := l.qaDao.BatchUpdateSimilarQuestion(ctx, filter, updateColumns, tx); err != nil {
				logx.E(ctx, "batchCreateReleaseSimilarQAUnit | Update ReleaseSimilarQA status to %d error. err:%+v",
					qaEntity.QAReleaseStatusIng, err)
				return err
			}

			return nil
		}); err != nil {
			logx.E(ctx, "Failed to batchCreateReleaseSimilarQAUnit. error:%+v", err)
			return err
		}

	}

	return nil
}

func (l *Logic) isExistReleaseQaSimilar(ctx context.Context, releaseSimilarQa *releaseEntity.ReleaseQaSimilarQuestion) (bool, error) {
	/*
		`
			SELECT
				COUNT(1)
			FROM
				t_release_qa_similar_question
			WHERE
				robot_id = ? AND version_id = ? AND similar_id = ?
		`
	*/
	if releaseSimilarQa == nil {
		return false, nil
	}
	filter := &releaseEntity.ReleaseQaSimilarQuestionFilter{
		RobotID:   uint64(releaseSimilarQa.RobotID),
		VersionID: uint64(releaseSimilarQa.VersionID),
		SimilarID: uint64(releaseSimilarQa.SimilarID),
	}
	return l.releaseDao.IsExistReleaseQaSimilar(ctx, filter)
}

// GetReleaseModifySimilarQA 获取版本改动的相似QA
func (l *Logic) GetReleaseModifySimilarQA(ctx context.Context, release *releaseEntity.Release, similarQAs []*qaEntity.SimilarQuestion) (
	map[uint64]*releaseEntity.ReleaseQaSimilarQuestion, error) {

	/*
			`
			SELECT
				%s
			FROM
			    t_release_qa_similar_question
			WHERE
			    corp_id = ? AND robot_id = ? AND version_id = ? %s
		`
	*/

	filter := &releaseEntity.ReleaseQaSimilarQuestionFilter{
		CorpID:         release.CorpID,
		VersionID:      release.ID,
		RobotID:        release.RobotID,
		ExtraCondition: "1=1",
	}

	if len(similarQAs) > 0 {
		filter.SimilarIDs = make([]uint64, 0, len(similarQAs))
		for _, qa := range similarQAs {
			filter.SimilarIDs = append(filter.SimilarIDs, qa.SimilarID)
		}
	}

	list, err := l.releaseDao.GetReleaseSimilarQuestionList(ctx, releaseEntity.ReleaseQaSimilarQuestionColList, filter)

	if err != nil {
		logx.E(ctx, "GetReleaseModifySimilarQA failed|err:%+v", err)
		return nil, err
	}
	modifySimilarQA := make(map[uint64]*releaseEntity.ReleaseQaSimilarQuestion, 0)
	for _, item := range list {
		modifySimilarQA[uint64(item.SimilarID)] = item
	}
	return modifySimilarQA, nil
}

// GetReleaseSimilarQAByID 获取发布的相似QA
func (l *Logic) GetReleaseSimilarQAByID(ctx context.Context, id uint64) (*releaseEntity.ReleaseQaSimilarQuestion, error) {
	/*
		 `
			SELECT
				%s
			FROM
			    t_release_qa_similar_question
			WHERE
				 id = ?
		`
	*/

	filter := &releaseEntity.ReleaseQaSimilarQuestionFilter{
		ID: id,
	}

	return l.releaseDao.GetReleaseSimilarQuestion(ctx, releaseEntity.ReleaseQaSimilarQuestionColList, filter)
}

// GetAuditSimilarQAFailByQaID 根据 QaID 获取机器审核审核不通过的相似QA内容
func (l *Logic) GetAuditSimilarQAFailByQaID(ctx context.Context, corpID, robotID, qaID uint64) (
	[]*releaseEntity.ReleaseQaSimilarQuestion, error) {
	/*
		`
		SELECT
		  id,similar_id,related_qa_id,question,audit_status
		FROM
			t_release_qa_similar_question
		WHERE
			robot_id = ? AND related_qa_id = ?
		ORDER BY
		  id DESC
	*/

	filter := &releaseEntity.ReleaseQaSimilarQuestionFilter{
		CorpID:         corpID,
		RobotID:        robotID,
		RelatedQaID:    qaID,
		OrderColumn:    []string{releaseEntity.ReleaseQaSimilarQuestionTblIDCol},
		OrderDirection: []string{"DESC"},
	}

	selectColumns := []string{
		releaseEntity.ReleaseQaSimilarQuestionTblIDCol,
		releaseEntity.ReleaseQaSimilarQuestionTblSimilarIDCol,
		releaseEntity.ReleaseQaSimilarQuestionTblRelatedQaIDCol,
		releaseEntity.ReleaseQaSimilarQuestionTblQuestionCol,
		releaseEntity.ReleaseQaSimilarQuestionTblAuditStatusCol,
	}

	modifySimilarQas, err := l.releaseDao.GetReleaseSimilarQuestionList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetAuditSimilarQAFailByQaID, err:%+v", err)
		return nil, err
	}
	return modifySimilarQas, nil
}

// GetAuditSimilarQAFailByVersion 获取机器审核审核不通过的相似QA内容
func (l *Logic) GetAuditSimilarQAFailByVersion(ctx context.Context, corpID, robotID, versionID uint64) (
	[]*releaseEntity.ReleaseQaSimilarQuestion, error) {
	/*
		`
			SELECT
			  id,similar_id,related_qa_id,question,audit_status
			FROM
				t_release_qa_similar_question
			WHERE
			    corp_id = ? AND version_id = ? AND audit_status = ?
		`
	*/
	as := releaseEntity.ReleaseQAAuditStatusFail
	filter := &releaseEntity.ReleaseQaSimilarQuestionFilter{
		CorpID:      corpID,
		RobotID:     robotID,
		VersionID:   versionID,
		AuditStatus: &as,
	}

	selectColumns := []string{
		releaseEntity.ReleaseQaSimilarQuestionTblIDCol,
		releaseEntity.ReleaseQaSimilarQuestionTblSimilarIDCol,
		releaseEntity.ReleaseQaSimilarQuestionTblRelatedQaIDCol,
		releaseEntity.ReleaseQaSimilarQuestionTblQuestionCol,
		releaseEntity.ReleaseQaSimilarQuestionTblAuditStatusCol,
	}

	modifySimilarQas, err := l.releaseDao.GetReleaseSimilarQuestionList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "Failed to GetAuditSimilarQAFailByQaID, err:%+v", err)
		return nil, err
	}
	return modifySimilarQas, nil
}

func (l *Logic) transDocQAToReleaseSimilarQA(qa *qaEntity.DocQA, similarQAs []*qaEntity.SimilarQuestion, versionID uint64,
	attrLabelsJSON string) []*releaseEntity.ReleaseQaSimilarQuestion {
	now := time.Now()
	releaseSimilarQAs := make([]*releaseEntity.ReleaseQaSimilarQuestion, 0)
	for _, sqa := range similarQAs {
		auditStatus := releaseEntity.ReleaseQAAuditStatusDoing
		isAllowRelease := entity.ForbidRelease
		auditResult := ""
		if !config.AuditSwitch() || !sqa.IsReleaseNeedAudit() ||
			sqa.IsAuditFree {
			auditStatus = releaseEntity.ReleaseQAAuditStatusSuccess
			isAllowRelease = entity.AllowRelease
			auditResult = "无需审核"
		}
		releaseSimilarQAs = append(releaseSimilarQAs, &releaseEntity.ReleaseQaSimilarQuestion{
			CorpID:         int64(sqa.CorpID),
			StaffID:        int64(sqa.StaffID),
			RobotID:        int64(sqa.RobotID),
			VersionID:      int64(versionID),
			SimilarID:      int64(sqa.SimilarID),
			CreateUserID:   int64(sqa.CreateUserID),
			RelatedQaID:    int64(sqa.RelatedQAID),
			Source:         int(sqa.Source),
			Question:       sqa.Question,
			ReleaseStatus:  int(sqa.ReleaseStatus),
			Message:        sqa.Message,
			Action:         sqa.ReleaseAction(),
			AttrLabels:     attrLabelsJSON,
			AuditStatus:    auditStatus,
			AuditResult:    auditResult,
			IsAllowRelease: isAllowRelease == entity.AllowRelease,
			IsDeleted:      sqa.IsDeleted,
			ExpireTime:     qa.ExpireEnd,
			CreateTime:     now,
			UpdateTime:     now,
		})
	}
	return releaseSimilarQAs
}

// GetReleaseSimilarIDs 获取本次待发布的相似问similarID 区分删除/非删除
func (l *Logic) GetReleaseSimilarIDs(ctx context.Context, robotID uint64, versionID uint64) (
	notDeletedSimilarIDs []uint64, deletedSimilarIDs []uint64,
	err error) {
	notDeletedSimilarIDs, err = l.getAllSimilarIDWithDeleteFlag(ctx, robotID, versionID, false)
	if err != nil {
		return nil, nil, err
	}
	deletedSimilarIDs, err = l.getAllSimilarIDWithDeleteFlag(ctx, robotID, versionID, true)
	if err != nil {
		return nil, nil, err
	}
	return notDeletedSimilarIDs, deletedSimilarIDs, nil
}

// getAllSimilarIDWithDeleteFlag 获取本次待发布的所有相似问similarID 区分删除/非删除
func (l *Logic) getAllSimilarIDWithDeleteFlag(ctx context.Context, robotID uint64, versionID uint64,
	isDeleted bool) ([]uint64,
	error) {
	limit := uint64(config.App().ReleaseParamConfig.GetIDsChunkSize)
	similarID := uint64(0)
	var allIDs []uint64
	for {
		ids, err := l.releaseDao.GetSimilarIDWithDeleteFlag(ctx, robotID, versionID, similarID, limit, isDeleted)
		if err != nil {
			return nil, err
		}
		allIDs = append(allIDs, ids...)
		if len(ids) < int(limit) {
			break
		}
		similarID = ids[len(ids)-1]
	}
	return allIDs, nil
}

func (l *Logic) releaseSimilarQANotify(ctx context.Context, isSuccess bool, reason string, appBizID, id uint64,
	release *releaseEntity.Release) error {
	// 相似问答
	similarQA, err := l.qaDao.GetSimilarQuestionByFilter(ctx, &qaEntity.SimilarityQuestionReq{
		SimilarQuestionID: id,
	})
	if err != nil {
		return err
	}
	if similarQA == nil {
		return errs.ErrQANotFound
	}
	similarQAMap, err := l.GetReleaseModifySimilarQA(ctx, release, []*qaEntity.SimilarQuestion{similarQA})
	if err != nil {
		return err
	}
	modifySimilarQA, ok := similarQAMap[similarQA.SimilarID]
	if !ok {
		logx.D(ctx, "当前版本没有修改这个相似QA %v", similarQA)
		return errs.ErrQANotModifyFound
	}
	similarQA.ReleaseStatus = util.When(isSuccess, qaEntity.QAReleaseStatusSuccess, qaEntity.QAReleaseStatusFail)
	similarQA.IsAuditFree = util.When(isSuccess, qaEntity.QAIsAuditNotFree, similarQA.IsAuditFree)
	similarQA.NextAction = qaEntity.NextActionPublish
	similarQA.Message = reason
	if similarQA.IsDelete() {
		similarQA.NextAction = qaEntity.NextActionAdd
	}
	modifySimilarQA.ReleaseStatus = int(similarQA.ReleaseStatus)
	modifySimilarQA.Message = similarQA.Message

	if similarQA.ReleaseStatus == qaEntity.QAReleaseStatusFail { // 发布失败需要同步更新主问答和发布的主问答
		qa, err := l.qaDao.GetQAByID(ctx, similarQA.RelatedQAID)
		if err != nil {
			return err
		}
		if qa == nil {
			return errs.ErrQANotFound
		}
		modifyQAs, err := l.releaseDao.GetReleaseModifyQA(ctx, release, []*qaEntity.DocQA{qa})
		if err != nil {
			return err
		}
		modifyQA, ok := modifyQAs[qa.ID]
		if !ok {
			logx.D(ctx, "当前版本没有修改这个QA %v", qa)
			return errs.ErrQANotModifyFound
		}
		qa.ReleaseStatus = qaEntity.QAReleaseStatusFail
		qa.IsAuditFree = util.When(isSuccess, qaEntity.QAIsAuditNotFree, qa.IsAuditFree)
		qa.NextAction = qaEntity.NextActionPublish
		qa.Message = reason
		if qa.IsDelete() || !qa.IsAccepted() {
			qa.NextAction = qaEntity.NextActionAdd
		}
		modifyQA.ReleaseStatus = qa.ReleaseStatus
		modifyQA.Message = qa.Message
		if err := l.PublishQA(ctx, appBizID, qa, modifyQA); err != nil {
			return err
		}
	}

	if err = l.PublishSimilarQA(ctx, similarQA, modifySimilarQA); err != nil {
		return err
	}
	return nil
}

// PublishQA 发布问答对
func (l *Logic) PublishSimilarQA(ctx context.Context, simQA *qaEntity.SimilarQuestion, releaseSimQA *releaseEntity.ReleaseQaSimilarQuestion) error {
	now := time.Now()
	if err := l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
		simQA.UpdateTime = now
		/*
				`
				UPDATE
					t_qa_similar_question
				SET
				    update_time = :update_time,
				    release_status = :release_status,
					is_audit_free = :is_audit_free,
				    message = :message,
				    next_action = :next_action
				WHERE
				    similar_id = :similar_id AND release_status != 5
			`

		*/
		filter := &qaEntity.SimilarityQuestionReq{
			SimilarQuestionID:  simQA.SimilarID,
			ReleaseStatusNotIn: []uint32{qaEntity.QAReleaseStatusFail},
		}
		simQATbl := tx.TQaSimilarQuestion
		updateColumns := map[string]any{
			simQATbl.UpdateTime.ColumnName().String():    now,
			simQATbl.ReleaseStatus.ColumnName().String(): simQA.ReleaseStatus,
			simQATbl.IsAuditFree.ColumnName().String():   simQA.IsAuditFree,
			simQATbl.Message.ColumnName().String():       simQA.Message,
			simQATbl.NextAction.ColumnName().String():    simQA.NextAction,
		}
		if err := l.qaDao.BatchUpdateSimilarQuestion(ctx, filter, updateColumns, simQATbl.WithContext(ctx).UnderlyingDB()); err != nil {
			logx.E(ctx, "Failed to Publish SimilarQA args:%+v err:%+v", simQA, err)
			return err
		}

		/*
			`
					UPDATE
						t_release_qa_similar_question
					SET
					    update_time = :update_time,
					    release_status = :release_status,
					    message = :message
					WHERE
					    id = :id AND release_status != 5
				`
		*/

		releaseSimQA.UpdateTime = now
		rQAFilter := &releaseEntity.ReleaseQaSimilarQuestionFilter{
			ID:               uint64(releaseSimQA.ID),
			ReleaseStatusNot: qaEntity.QAReleaseStatusFail,
		}

		rsimQATbl := tx.TReleaseQaSimilarQuestion
		updateColumns = map[string]any{
			rsimQATbl.UpdateTime.ColumnName().String():    now,
			rsimQATbl.ReleaseStatus.ColumnName().String(): releaseSimQA.ReleaseStatus,
			rsimQATbl.Message.ColumnName().String():       releaseSimQA.Message,
		}
		if _, err := l.releaseDao.BatchUpdateReleaseSimilarQuestionRecords(ctx, updateColumns, rQAFilter,
			rsimQATbl.WithContext(ctx).UnderlyingDB()); err != nil {
			logx.E(ctx, "Failed to Publish ReleaseSimilarQA args:%+v err:%+v", releaseSimQA, err)
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to PublishQA err:%+v", err)
		return err
	}
	return nil
}
