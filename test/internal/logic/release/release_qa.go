package release

import (
	"context"
	"math"
	"sync"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	attrEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gorm"
)

func (l *Logic) GetModifyQACount(ctx context.Context, robotID, versionID uint64,
	question string, actions []uint32, releaseStatuses []uint32) (uint64, error) {
	return l.releaseDao.GetModifyQACount(ctx, robotID, versionID, question, actions, releaseStatuses)
}

func (l *Logic) GetModifyQAList(ctx context.Context, robotID, versionID uint64, question string, actions []uint32, page,
	pageSize uint32, orderBy string, releaseStatus []uint32) (
	[]*releaseEntity.ReleaseQA, error) {
	req := &releaseEntity.ListReleaseQAReq{
		RobotID:       robotID,
		VersionID:     versionID,
		Question:      question,
		Actions:       actions,
		ReleaseStatus: releaseStatus,
		Page:          page,
		PageSize:      pageSize,
		OrderBy:       orderBy,
	}
	return l.releaseDao.GetModifyQAList(ctx, req)
}

// GetReleaseModifyQA 获取版本改动的QA
func (l *Logic) GetReleaseModifyQA(ctx context.Context, release *releaseEntity.Release, qas []*qaEntity.DocQA) (
	map[uint64]*releaseEntity.ReleaseQA, error) {
	return l.releaseDao.GetReleaseModifyQA(ctx, release, qas)

}

// GetReleasingQaId 获取发布中的问答id
func (l *Logic) GetReleasingQaId(ctx context.Context, robotID uint64, qaIds []uint64) (map[uint64]struct{}, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	releaseQas := make(map[uint64]struct{}, 0)
	latestRelease, err := l.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return releaseQas, nil
	}
	if latestRelease.IsPublishDone() {
		return releaseQas, nil
	}
	releaseQas, err = l.releaseDao.GetReleaseQaIdMap(ctx, corpID, robotID, latestRelease.ID, qaIds)
	if err != nil {
		return nil, err
	}
	return releaseQas, nil
}

// GetDocQaReleaseCount 获取问答未发布状态总数
func (l *Logic) GetDocQaReleaseCount(ctx context.Context, corpID, robotID uint64) (int64, error) {
	logx.I(ctx, "GetDocQaReleaseCount corpID:%v, robotID:%v", corpID, robotID)
	isDeleted := qaEntity.QAIsNotDeleted
	filter := &qaEntity.DocQaFilter{
		CorpId:  corpID,
		RobotId: robotID,
		ReleaseStatusList: []uint32{qaEntity.QAReleaseStatusInit,
			qaEntity.QAReleaseStatusLearning,
			qaEntity.QAReleaseStatusAuditing,
			qaEntity.QAReleaseStatusAppealIng},
		AcceptStatus: qaEntity.AcceptYes,
		ReleaseCount: true,
		IsDeleted:    &isDeleted,
	}
	count, err := l.qaDao.GetDocQaCountWithTx(ctx, []string{qaEntity.DocQaTblColId}, filter, nil)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (l *Logic) BatchCreateReleaseQAs(ctx context.Context,
	releaseQAs []*releaseEntity.ReleaseQA) error {
	if err := batchReleaseProcess(ctx, releaseQAs, l.batchCreateReleaseQAUnit); err != nil {
		logx.W(ctx, "batchCreateReleaseQA err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) batchCreateReleaseQAUnit(ctx context.Context,
	releaseQAs []*releaseEntity.ReleaseQA) error {
	defer timeTrack(ctx, time.Now(), "batchCreateReleaseQAUnit")
	if len(releaseQAs) == 0 {
		return nil
	}

	chunkBatchSize := config.App().ReleaseParamConfig.CreateReleaseBatchSize
	if chunkBatchSize == 0 {
		chunkBatchSize = 200
	}

	qaChunks := slicex.Chunk(releaseQAs, chunkBatchSize)

	for _, qaChunk := range qaChunks {
		if err := l.releaseDao.MysqlQuery().TReleaseQa.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
			batchReleaseQas := qaChunk
			err := l.releaseDao.CreateReleaseQARecords(ctx, batchReleaseQas, tx)
			if err != nil {
				logx.E(ctx, "create release qa record error:%v", err)
				return err
			}
			// todo: add update t_doc_qa status in t_doc_qa do
			docQaIds := make([]uint64, 0, len(batchReleaseQas))
			for _, releaseQA := range batchReleaseQas {
				docQaIds = append(docQaIds, releaseQA.QAID)
			}

			err = l.qaDao.BatchUpdateDocQAReleaseStatusByID(ctx, docQaIds, qaEntity.QAReleaseStatusIng, tx)
			if err != nil {
				logx.E(ctx, "update doc qa status error:%v", err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "Failed to create release qa records. err:%+v", err)
			return err
		}
	}
	return nil

}

func (l *Logic) GetForbidReleaseQA(ctx context.Context, versionID uint64) (
	[]*releaseEntity.ReleaseQA, error) {
	return l.releaseDao.GetForbidReleaseQA(ctx, versionID)
}

func (l *Logic) ForbidReleaseQA(ctx context.Context, releaseQAs []*releaseEntity.ReleaseQA, tx *gorm.DB) error {
	qaIds := make([]uint64, 0, len(releaseQAs))
	for _, releaseQA := range releaseQAs {
		qaIds = append(qaIds, releaseQA.QAID)
	}
	logx.I(ctx, "[ForbidReleaseQA] update doc qa release status to:%v",
		qaEntity.QAReleaseStatusInit)

	filter := &qaEntity.DocQaFilter{
		QAIds: qaIds,
	}
	updateColumns := map[string]any{
		qaEntity.DocQaTblColUpdateTime:    time.Now(),
		qaEntity.DocQaTblColReleaseStatus: qaEntity.QAReleaseStatusInit,
	}
	_, err := l.qaDao.BatchUpdateDocQA(ctx, filter, updateColumns, tx)
	if err != nil {
		logx.E(ctx, "update doc qa release status error:%v", err)
		return err
	}

	return nil

}

// UpdateQAWaitRelease 更新问答状态到待发布
func (l *Logic) UpdateQAWaitRelease(ctx context.Context, appID, qaID uint64, simBizIDs []uint64) (err error) {
	// 开启事务
	// tx := d.gormDB.Begin()
	tx := l.releaseDao.MysqlQuery().TReleaseQa.WithContext(ctx).UnderlyingDB().Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	// 1.更新问答标准问的状态
	err = l.qaDao.BatchUpdateDocQAReleaseStatusByID(ctx, []uint64{qaID}, qaEntity.QAReleaseStatusInit, tx)
	if err != nil {
		logx.E(ctx, "UpdateQAWaitRelease failed qa:%v, err: %+v", qaID, err)
		return err
	}
	// 2.更新问答相似问的状态
	if len(simBizIDs) > 0 {
		err = l.qaDao.BatchUpdateSimilarQuestion(ctx, &qaEntity.SimilarityQuestionReq{
			RobotId:   appID,
			IsDeleted: qaEntity.QAIsNotDeleted,
		}, map[string]any{
			qaEntity.DocQaSimTblColReleaseStatus: qaEntity.QAReleaseStatusInit,
			qaEntity.DocQaSimTblColUpdateTime:    time.Now()}, tx)
		if err != nil {
			logx.E(ctx, "UpdateQAWaitRelease failed simBizIDs:%v,qaID:%v,err: %+v", simBizIDs, qaID, err)
			return err
		}
	}
	return nil
}

// getReleaseQA 获取发布的QA
func (l *Logic) getReleaseQA(ctx context.Context, corpID, robotID, versionID uint64) (
	[]*releaseEntity.ReleaseQA, []*releaseEntity.ReleaseQaSimilarQuestion, error) {
	var (
		zeroTime      time.Time
		emptyStatus   []uint32
		emptyQuestion string
	)

	docs, err := l.docLogic.GetDeletingDoc(ctx, corpID, robotID)
	if err != nil {
		return nil, nil, err
	}
	total, err := l.qaDao.GetReleaseQACount(ctx, corpID, robotID, emptyQuestion, zeroTime, zeroTime, emptyStatus)
	if err != nil {
		return nil, nil, err
	}
	mapQAID2AttrLabels := new(sync.Map)

	releaseQAs, releaseSimilarQAs := make([]*releaseEntity.ReleaseQA, 0, total), make([]*releaseEntity.ReleaseQaSimilarQuestion, 0)
	releaseQAWithSimilarChan := make(chan *qaEntity.DocQAWithSimilar, 5000)
	existReleaseQA := make(map[uint64]struct{})
	finish := make(chan any)

	go func() {
		for qa := range releaseQAWithSimilarChan {
			if _, ok := docs[qa.DocQA.DocID]; ok && qa.DocQA.IsNextActionAdd() {
				continue
			}
			if _, ok := existReleaseQA[qa.DocQA.ID]; ok {
				continue
			}
			existReleaseQA[qa.DocQA.ID] = struct{}{}

			// 主问答
			attrLabelJSON := parseAttrLabels2Json(mapQAID2AttrLabels, qa.DocQA.ID)
			releaseQAs = append(releaseQAs, l.transDocQAToReleaseQA(qa.DocQA, versionID, attrLabelJSON))

			// 相似问答
			similarAttrLabelJSON := parseSimilarQAAttrLabels2Json(mapQAID2AttrLabels, qa.DocQA.ID)
			releaseSimilarQAs = append(releaseSimilarQAs,
				l.transDocQAToReleaseSimilarQA(qa.DocQA, qa.DocSimilarQA, versionID, similarAttrLabelJSON)...)
		}
		finish <- nil
	}()

	// releaseQA := make([]*releaseEntity.ReleaseQA, 0, total)
	// releaseQAChan := make(chan *qaEntity.DocQA, 5000)
	pageSize := config.App().ReleaseParamConfig.GetReleasingRecordSize
	if pageSize == 0 {
		pageSize = 1000
	}
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		list, err := l.qaDao.GetReleaseQAList(ctx, corpID, robotID, emptyQuestion, zeroTime, zeroTime, emptyStatus,
			page, uint32(pageSize))
		if err != nil {
			return nil, nil, err
		}
		if err := l.getReleaseQAAttrLabels(ctx, robotID, mapQAID2AttrLabels, list); err != nil {
			return nil, nil, err
		}
		releaseQAIDs, releaseQAs := make([]uint64, 0), make([]*qaEntity.DocQA, 0)
		for _, item := range list {
			if !item.IsAllowRelease() {
				continue
			}
			releaseQAIDs = append(releaseQAIDs, item.ID)
			releaseQAs = append(releaseQAs, item)
		}
		similarQAMap, err := l.qaLogic.GetReleaseSimilarQAMap(ctx, corpID, robotID, releaseQAIDs)
		if err != nil {
			return nil, nil, err
		}
		for _, qa := range releaseQAs { // 这里保证相似问答的发布是从需要发布的主问答查询出来的
			releaseQAWithSimilar := &qaEntity.DocQAWithSimilar{DocQA: qa}
			if similarQA, ok := similarQAMap[qa.ID]; ok {
				releaseQAWithSimilar.DocSimilarQA = similarQA
			}
			releaseQAWithSimilarChan <- releaseQAWithSimilar
		}
	}
	close(releaseQAWithSimilarChan)
	<-finish
	return releaseQAs, releaseSimilarQAs, nil
}

func (l *Logic) transDocQAToReleaseQA(qa *qaEntity.DocQA, versionID uint64,
	attrLabelsJSON string) *releaseEntity.ReleaseQA {
	// auditStatus := releaseEntity.ReleaseQAAuditStatusDoing
	// isAllowRelease := entity.ForbidRelease
	// auditResult := ""
	// QA的发布无需审核，所以一个QA可以走到发布流程，默认是审核通过的。
	auditStatus := releaseEntity.ReleaseQAAuditStatusSuccess
	isAllowRelease := entity.AllowRelease
	auditResult := "无需审核"

	now := time.Now()
	return &releaseEntity.ReleaseQA{
		RobotID:        qa.RobotID,
		CorpID:         qa.CorpID,
		StaffID:        qa.StaffID,
		VersionID:      versionID,
		QAID:           qa.ID,
		DocID:          qa.DocID,
		OriginDocID:    qa.OriginDocID,
		SegmentID:      qa.SegmentID,
		CategoryID:     qa.CategoryID,
		Source:         qa.Source,
		Question:       qa.Question,
		Answer:         qa.Answer,
		CustomParam:    qa.CustomParam,
		QuestionDesc:   qa.QuestionDesc,
		ReleaseStatus:  qa.ReleaseStatus,
		IsDeleted:      qa.IsDeleted,
		Message:        qa.Message,
		AcceptStatus:   qa.AcceptStatus,
		SimilarStatus:  qa.SimilarStatus,
		Action:         qa.ReleaseAction(),
		AuditStatus:    auditStatus,
		AuditResult:    auditResult,
		CreateTime:     now,
		UpdateTime:     now,
		IsAllowRelease: isAllowRelease,
		AttrLabels:     attrLabelsJSON,
		ExpireTime:     qa.ExpireEnd,
	}
}

func (l *Logic) getReleaseQAAttrLabels(ctx context.Context, robotID uint64,
	mapQAID2AttrLabels *sync.Map, list []*qaEntity.DocQA) error {
	if len(list) == 0 {
		return nil
	}
	var originDocIDs, qaIDs []uint64
	for _, v := range list {
		if v.Source == docEntity.SourceFromDoc {
			originDocIDs = append(originDocIDs, v.OriginDocID)
			continue
		}
		if v.AttrRange == docEntity.AttrRangeAll {
			continue
		}
		qaIDs = append(qaIDs, v.ID)
	}
	mapDocID2AttrLabels := new(sync.Map)
	if err := l.getReleaseDocAttrLabels(ctx, mapDocID2AttrLabels, robotID, originDocIDs); err != nil {
		return err
	}
	mapQAID2AttrLabelsDetail, err := l.labelLogic.GetQAAttributeLabelDetail(ctx, robotID, qaIDs)
	if err != nil {
		return err
	}
	for _, v := range list {
		if v.Source == docEntity.SourceFromDoc {
			storeAttrLabels(mapQAID2AttrLabels, v.ID, loadAttrLabels(mapDocID2AttrLabels, v.OriginDocID)...)
			continue
		}
		if v.AttrRange == docEntity.AttrRangeAll {
			storeAttrLabels(mapQAID2AttrLabels, v.ID, &releaseEntity.ReleaseAttrLabel{
				Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
				Value: config.App().AttributeLabel.FullLabelValue,
			})
			continue
		}
		storeAttrLabels(mapQAID2AttrLabels, v.ID, fillReleaseAttrLabel(mapQAID2AttrLabelsDetail[v.ID])...)
	}
	return nil

}

// storeAttrLabels TODO
func storeAttrLabels(mapAttrLabels *sync.Map, key any, attrLabels ...*releaseEntity.ReleaseAttrLabel) {
	if mapAttrLabels == nil || len(attrLabels) == 0 {
		return
	}
	mapAttrLabels.Store(key, attrLabels)
}

// fillReleaseAttrLabel TODO
func fillReleaseAttrLabel(attrLabels []*attrEntity.AttrLabel) []*releaseEntity.ReleaseAttrLabel {
	releaseAttrLabels := make([]*releaseEntity.ReleaseAttrLabel, 0)
	for _, v := range attrLabels {
		for _, label := range v.Labels {
			releaseAttrLabels = append(releaseAttrLabels, &releaseEntity.ReleaseAttrLabel{
				Name:  v.AttrKey,
				Value: label.LabelName,
			})
		}
	}
	return releaseAttrLabels
}

func (l *Logic) getReleaseDocAttrLabels(ctx context.Context, mapDocID2AttrLabels *sync.Map,
	robotID uint64, docIDs []uint64) error {
	if len(docIDs) == 0 {
		return nil
	}
	docs, err := l.docLogic.GetDocByIDs(ctx, docIDs, robotID)
	// docs, err := l.docLogic.GetDao().GetDocByIDs(ctx, docIDs, robotID)
	if err != nil {
		return err
	}
	referIDs := make([]uint64, 0)
	for _, v := range docs {
		if v.AttrRange == docEntity.AttrRangeAll {
			storeAttrLabels(mapDocID2AttrLabels, v.ID, &releaseEntity.ReleaseAttrLabel{
				Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
				Value: config.App().AttributeLabel.FullLabelValue,
			})
			continue
		}
		referIDs = append(referIDs, v.ID)
	}
	mapDocID2AttrLabelDetail, err := l.labelLogic.GetDocAttributeLabelDetail(ctx, robotID, referIDs)
	if err != nil {
		return err
	}
	for docID, attrLabels := range mapDocID2AttrLabelDetail {
		storeAttrLabels(mapDocID2AttrLabels, docID, fillReleaseAttrLabel(attrLabels)...)
	}
	return nil
}

// GetReleaseQAIDs 获取本次待发布的qa_id 区分非删除和删除的
func (l *Logic) GetReleaseQAIDs(ctx context.Context, robotID uint64, versionID uint64) (
	notDeletedQAIDs []uint64, deletedQAIDs []uint64, err error) {
	notDeletedQAIDs, err = l.getAllQAIDWithDeleteFlag(ctx, robotID, versionID, false)
	if err != nil {
		return nil, nil, err
	}
	deletedQAIDs, err = l.getAllQAIDWithDeleteFlag(ctx, robotID, versionID, true)
	if err != nil {
		logx.E(ctx, "GetReleaseQAIDs error: %v", err)
		return nil, nil, err
	}
	return notDeletedQAIDs, deletedQAIDs, nil
}

// getAllQAIDWithDeleteFlag 获取所有删除或非删除的qa_id
func (l *Logic) getAllQAIDWithDeleteFlag(ctx context.Context, robotID uint64, versionID uint64, isDeleted bool) ([]uint64, error) {

	limit := config.App().ReleaseParamConfig.GetIDsChunkSize
	qaID := uint64(0)
	var allIDs []uint64

	isAllowRelease := entity.AllowRelease
	delFlag := uint32(2)

	for {
		listReq := &releaseEntity.ListReleaseQAReq{
			RobotID:        robotID,
			VersionID:      versionID,
			IsAllowRelease: &isAllowRelease,
			PageSize:       limit,
			OrderBy:        "qa_id",
		}

		if isDeleted {
			/*
							``SELECT qa_id FROM t_release_qa
					    	WHERE robot_id = ? AND version_id = ? AND is_allow_release = 1 AND is_deleted = 2 AND qa_id > ?
				ORDER BY qa_id ASC LIMIT ?``
			*/
			listReq.MinQAID = qaID
			listReq.IsDeleted = &delFlag
		} else {
			/*
							`SELECT qa_id FROM t_release_qa
					    	WHERE robot_id = ? AND version_id = ? AND is_allow_release = 1 AND is_deleted = 2 AND qa_id > ?
				ORDER BY qa_id ASC LIMIT ?`
			*/
			listReq.MinQAID = qaID
			listReq.IsDeletedNot = &delFlag
		}

		list, err := l.releaseDao.GetModifyQAList(ctx, listReq)
		if err != nil {
			return nil, err
		}

		ids := make([]uint64, 0, len(list))
		for _, item := range list {
			ids = append(ids, item.QAID)
		}
		allIDs = append(allIDs, ids...)

		if len(list) < int(limit) {
			break
		}
		qaID = ids[len(ids)-1]
	}

	return allIDs, nil
}

func (l *Logic) releaseQANotify(ctx context.Context, isSuccess bool, reason string, appBizID, id uint64,
	release *releaseEntity.Release) error {
	// 主问答
	qa, err := l.qaDao.GetQAByID(ctx, id)
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
	qa.ReleaseStatus = util.When(isSuccess, qaEntity.QAReleaseStatusSuccess, qaEntity.QAReleaseStatusFail)
	qa.IsAuditFree = util.When(isSuccess, qaEntity.QAIsAuditNotFree, qa.IsAuditFree)
	qa.NextAction = qaEntity.NextActionPublish
	qa.Message = reason
	if qa.IsDelete() || !qa.IsAccepted() {
		qa.NextAction = qaEntity.NextActionAdd
	}
	modifyQA.ReleaseStatus = qa.ReleaseStatus
	modifyQA.Message = qa.Message
	if err = l.PublishQA(ctx, appBizID, qa, modifyQA); err != nil {
		return err
	}
	return nil
}

// PublishQA 发布问答对
func (l *Logic) PublishQA(ctx context.Context, appBizID uint64, qa *qaEntity.DocQA, releaseQA *releaseEntity.ReleaseQA) error {
	now := time.Now()
	if err := l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
		qa.UpdateTime = now
		/*
				 `
					UPDATE
						t_doc_qa
					SET
					    update_time = :update_time,
					    release_status = :release_status,
						is_audit_free = :is_audit_free,
					    message = :message,
					    next_action = :next_action
					WHERE
			 id = :id AND release_status != 5

		*/
		filter := &qaEntity.DocQaFilter{
			QAId:             qa.ID,
			ReleaseStatusNot: qaEntity.QAReleaseStatusFail,
		}
		updateColumns := map[string]any{
			qaEntity.DocQaTblColUpdateTime:    now,
			qaEntity.DocQaTblColReleaseStatus: qa.ReleaseStatus,
			qaEntity.DocQaTblColIsAuditFree:   qa.IsAuditFree,
			qaEntity.DocQaTblColMessage:       qa.Message,
			qaEntity.DocQaTblColNextAction:    qa.NextAction,
		}
		if _, err := l.qaDao.BatchUpdateDocQA(ctx, filter, updateColumns, tx.TDocQa.WithContext(ctx).UnderlyingDB()); err != nil {
			logx.E(ctx, "Failed to Publish QA args:%+v err:%+v", qa, err)
			return err
		}

		/*
			`
					UPDATE
						t_release_qa
					SET
					    update_time = :update_time,
					    release_status = :release_status,
					    message = :message
					WHERE
					    id = :id AND release_status != 5
				`
		*/

		releaseQA.UpdateTime = now
		rQAFilter := &releaseEntity.ReleaseQAFilter{
			Id:               releaseQA.ID,
			ReleaseStatusNot: qaEntity.QAReleaseStatusFail,
		}
		updateColumns = map[string]any{
			tx.TReleaseQa.UpdateTime.ColumnName().String():    now,
			tx.TReleaseQa.ReleaseStatus.ColumnName().String(): releaseQA.ReleaseStatus,
			tx.TReleaseQa.Message.ColumnName().String():       releaseQA.Message,
		}
		if _, err := l.releaseDao.BatchUpdateReleaseQARecords(ctx, updateColumns, rQAFilter,
			tx.TReleaseQa.WithContext(ctx).UnderlyingDB()); err != nil {
			logx.E(ctx, "Failed to Publish ReleaseQA args:%+v err:%+v", releaseQA, err)
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to PublishQA err:%+v", err)
		return err
	}
	return nil
}
