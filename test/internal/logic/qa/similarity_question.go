package qa

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"

	"git.code.oa.com/trpc-go/trpc-go/codec"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"gorm.io/gorm"

	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// SimilarQAExtract 相似问提取
type SimilarQAExtract struct {
	Question string
	Answer   string
}

// GetSimilarQuestionsByQA 根据标准问获取所有相似问
func (l *Logic) GetSimilarQuestionsByQA(ctx context.Context, qa *qaEntity.DocQA) ([]*qaEntity.SimilarQuestion, error) {
	if qa == nil || qa.ID == 0 {
		logx.E(ctx, "GetSimilarQuestionsByQAID get invalid qa param %v", qa)
		return nil, nil
	}
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

	req := &qaEntity.SimilarityQuestionReq{
		CorpId:      qa.CorpID,
		RobotId:     qa.RobotID,
		RelatedQAID: qa.ID,
		IsDeleted:   qaEntity.QAIsNotDeleted,
		PageSize:    uint32(config.App().DocQA.SimilarQuestionNumLimit),
	}

	return l.qaDao.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, req)
}

// GetReleaseSimilarQAMap 获取发布相似问答对Map
func (l *Logic) GetReleaseSimilarQAMap(ctx context.Context, corpID, robotID uint64, releaseQAIDs []uint64) (
	map[uint64][]*qaEntity.SimilarQuestion, error) {
	/*
		SELECT
			%s
		FROM
		    t_qa_similar_question
		WHERE
		    corp_id = ?
			AND robot_id = ?
			AND release_status = ?
			AND !(next_action = ? AND is_deleted = ?)
			AND related_qa_id IN (%s)

	*/
	if len(releaseQAIDs) == 0 {
		logx.I(ctx, "GetReleaseSimilarQAMap|len(releaseQAIDs):%d|ignore", len(releaseQAIDs))
		return nil, nil
	}

	req := &qaEntity.SimilarityQuestionReq{
		CorpId:        corpID,
		RobotId:       robotID,
		RelatedQAIDs:  releaseQAIDs,
		ReleaseStatus: qaEntity.QAReleaseStatusInit,
		IsRelease:     true,
	}

	list, err := l.qaDao.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, req)

	if err != nil {
		logx.E(ctx, "GetReleaseSimilarQAMap failed|err:%+v", err)
		return nil, err
	}

	modifySimilarQA := make(map[uint64][]*qaEntity.SimilarQuestion)
	for _, item := range list {
		sQAs, ok := modifySimilarQA[item.RelatedQAID]
		if ok {
			sQAs = append(sQAs, item)
		} else {
			sQAs = make([]*qaEntity.SimilarQuestion, 0)
			sQAs = append(sQAs, item)
		}
		modifySimilarQA[item.RelatedQAID] = sQAs
	}
	return modifySimilarQA, nil
}

// GetSimilarQAMap 获取相似问答对Map
func (l *Logic) GetSimilarQAMap(ctx context.Context, corpID, robotID uint64, qaIDs []uint64) (
	map[uint64][]*qaEntity.SimilarQuestion, error) {
	/*
			 `
			SELECT
				%s
			FROM
			    t_qa_similar_question
			WHERE
			    corp_id = ? AND robot_id = ? AND is_deleted = ? AND related_qa_id IN (%s)
		`
	*/
	if len(qaIDs) == 0 {
		logx.I(ctx, "GetSimilarQAMap|len(qaIDs):%d|ignore", len(qaIDs))
		return nil, nil
	}

	req := &qaEntity.SimilarityQuestionReq{
		CorpId:       corpID,
		RobotId:      robotID,
		RelatedQAIDs: qaIDs,
		IsDeleted:    qaEntity.QAIsNotDeleted,
	}

	list, err := l.qaDao.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, req)

	if err != nil {
		logx.E(ctx, "GetSimilarQAMap failed|err:%+v", err)
		return nil, err
	}

	similarQAMap := make(map[uint64][]*qaEntity.SimilarQuestion)
	for _, item := range list {
		sQAs, ok := similarQAMap[item.RelatedQAID]
		if ok {
			sQAs = append(sQAs, item)
		} else {
			sQAs = make([]*qaEntity.SimilarQuestion, 0)
			sQAs = append(sQAs, item)
		}
		similarQAMap[item.RelatedQAID] = sQAs
	}
	return similarQAMap, nil
}

// GetSimilarQADetailsByReleaseStatus 批量获取相似QA详情(机器审核不通过)
func (l *Logic) GetSimilarQADetailsByReleaseStatus(ctx context.Context, corpID, robotID uint64, ids []uint64,
	releaseStatus uint32) (map[uint64]*qaEntity.SimilarQuestion, error) {
	/*
			`
			SELECT
				%s
			FROM
			    t_qa_similar_question
			WHERE
			    corp_id = ? AND robot_id = ? AND release_status = ? AND similar_id IN (%s)
		`
	*/
	if len(ids) == 0 {
		return nil, nil
	}

	req := &qaEntity.SimilarityQuestionReq{
		CorpId:             corpID,
		RobotId:            robotID,
		SimilarQuestionIDs: ids,
		ReleaseStatus:      releaseStatus,
	}

	list, err := l.qaDao.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, req)

	if err != nil {
		logx.E(ctx, "GetSimilarQADetailsByReleaseStatus failed|err:%+v", err)
		return nil, err
	}

	if len(list) == 0 {
		return nil, errs.ErrAppealNotFound
	}
	qas := make(map[uint64]*qaEntity.SimilarQuestion, 0)
	for _, item := range list {
		qas[item.SimilarID] = item
	}
	return qas, nil
}

// AddSimilarQuestions 添加相似问
func (l *Logic) AddSimilarQuestions(ctx context.Context, sqs []*qaEntity.SimilarQuestion) error {
	if len(sqs) == 0 {
		return nil
	}
	/*
				`
			        INSERT INTO
						t_qa_similar_question (%s)
			        VALUES
			            %s
			    `
				 `
			similar_id, robot_id, corp_id, staff_id, create_user_id, related_qa_id, source, question,
			is_deleted, release_status, is_audit_free, next_action, create_time, update_time, char_size
		`
	*/
	if err := l.qaDao.BatchCreateSimilarQuestions(ctx, sqs); err != nil {
		return err
	}
	return nil
}

// DeleteSimilarQuestions 删除相似问
func (l *Logic) DeleteSimilarQuestions(ctx context.Context, sqs []*qaEntity.SimilarQuestion) error {
	/*
			`
		        UPDATE
		            t_qa_similar_question
		        SET
		            is_deleted = ?,
		            update_time = ?,
		            release_status = ?,
		            is_audit_free = ?,
					next_action = ?
		        WHERE
		            corp_id = ? AND robot_id = ? AND similar_id IN (%s)
			`
	*/
	if len(sqs) == 0 {
		return nil
	}

	updateColumns := []string{
		qaEntity.DocQaSimTblColIsDeleted,
		qaEntity.DocQaSimTblColUpdateTime,
		qaEntity.DocQaSimTblColReleaseStatus,
		qaEntity.DocQaSimTblColIsAuditFree,
		qaEntity.DocQaSimTblColNextAction,
	}

	if err := l.qaDao.Query().TQaSimilarQuestion.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		for _, sq := range sqs {
			req := &qaEntity.SimilarityQuestionReq{
				CorpId:            sq.CorpID,
				RobotId:           sq.RobotID,
				SimilarQuestionID: sq.SimilarID,
			}
			if err := l.qaDao.UpdateSimilarQuestion(ctx, tx, updateColumns, req, sq); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// DeleteSimilarQuestionsByQA 根据标准问删除相似问
func (l *Logic) DeleteSimilarQuestionsByQA(ctx context.Context, qa *qaEntity.DocQA) error {
	/*
			`
		        UPDATE
		            t_qa_similar_question
		        SET
		            is_deleted = ?,
		            update_time = ?,
			        release_status = ?,
		            is_audit_free = ?,
					next_action = ?
				WHERE
		             corp_id = ? AND robot_id = ? AND related_qa_id = ?
			`
	*/
	if qa == nil || qa.ID == 0 {
		return nil
	}
	req := &qaEntity.SimilarityQuestionReq{
		CorpId:      qa.CorpID,
		RobotId:     qa.RobotID,
		RelatedQAID: qa.ID,
	}
	updateColumns := map[string]any{
		qaEntity.DocQaSimTblColIsDeleted:     qaEntity.QAIsNotDeleted,
		qaEntity.DocQaSimTblColUpdateTime:    time.Now(),
		qaEntity.DocQaSimTblColReleaseStatus: qaEntity.QAReleaseStatusInit,
		qaEntity.DocQaSimTblColIsAuditFree:   qaEntity.QAIsAuditNotFree,
		qaEntity.DocQaSimTblColNextAction:    qaEntity.NextActionDelete,
	}

	if err := l.qaDao.BatchUpdateSimilarQuestion(ctx, req, updateColumns, nil); err != nil {
		return err
	}
	return nil
}

// UpdateSimilarQuestions 更新相似问, 复用QA的状态(Update时相似问状态严格和qa保持一致)
func (l *Logic) UpdateSimilarQuestions(ctx context.Context, qa *qaEntity.DocQA,
	sqs []*qaEntity.SimilarQuestion) error {
	/*
			`
		        UPDATE
		            t_qa_similar_question
		        SET
		            question = :question,
		            update_time = :update_time,
					release_status = :release_status,
					is_audit_free = :is_audit_free,
					next_action = :next_action,
		            char_size = :char_size
		        WHERE
		            corp_id = :corp_id AND robot_id = :robot_id AND similar_id = :similar_id
			`
	*/
	if qa == nil || len(sqs) == 0 {
		return nil
	}

	updateColumns := []string{
		qaEntity.DocQaSimTblColQuestion,
		qaEntity.DocQaSimTblColUpdateTime,
		qaEntity.DocQaSimTblColReleaseStatus,
		qaEntity.DocQaSimTblColIsAuditFree,
		qaEntity.DocQaSimTblColNextAction,
		qaEntity.DocQaSimTblColCharSize,
	}

	for _, sq := range sqs {
		now := time.Now()
		// 完全和QA状态保持一致(不做额外兜底)
		similarQ := &qaEntity.SimilarQuestion{
			CorpID:        sq.CorpID,
			RobotID:       sq.RobotID,
			SimilarID:     sq.SimilarID,
			Question:      sq.Question,
			ReleaseStatus: qa.ReleaseStatus,
			IsAuditFree:   qa.IsAuditFree,
			NextAction:    qa.NextAction,
			CharSize:      uint64(utf8.RuneCountInString(sq.Question)),
			UpdateTime:    now,
		}
		req := &qaEntity.SimilarityQuestionReq{
			CorpId:            sq.CorpID,
			RobotId:           sq.RobotID,
			SimilarQuestionID: sq.SimilarID,
		}

		if err := l.qaDao.UpdateSimilarQuestion(ctx, nil, updateColumns, req, similarQ); err != nil {
			logx.E(ctx, "Failed to update similar question, sq:%+v, err: %+v", similarQ, err)
			return err
		}
	}

	return nil
}

// UpdateSimilarQuestionsStatus 更新相似问的状态(Update时相似问状态严格和qa保持一致)
func (l *Logic) UpdateSimilarQuestionsReleaseStatus(ctx context.Context,
	sqs []*qaEntity.SimilarQuestion) error {
	/*
				`
		        UPDATE
		            t_qa_similar_question
		        SET
		            update_time = :update_time,
					release_status = :release_status
		        WHERE
		            robot_id = :robot_id AND similar_id = :similar_id
			`
	*/

	if len(sqs) == 0 {
		return nil
	}
	updateColumns := []string{
		qaEntity.DocQaSimTblColReleaseStatus, qaEntity.DocQaSimTblColIsAuditFree,
		qaEntity.DocQaSimTblColUpdateTime,
	}

	if err := l.qaDao.Query().TQaSimilarQuestion.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {

		for _, sq := range sqs {
			fiter := &qaEntity.SimilarityQuestionReq{
				RobotId:           sq.RobotID,
				SimilarQuestionID: sq.SimilarID,
			}
			if err := l.qaDao.UpdateSimilarQuestion(ctx, tx, updateColumns, fiter, sq); err != nil {
				return err
			}
		}
		return nil

	}); err != nil {
		logx.E(ctx, "Update similar questions status failed, err: %+v", err)
		return err
	}
	return nil
}

// ModifySimilarQuestions 整体更新相似问：包括新增、删除、update(复用QA的状态)
func (l *Logic) ModifySimilarQuestions(ctx context.Context, qa *qaEntity.DocQA,
	sqm *qaEntity.SimilarQuestionModifyInfo) error {

	if qa == nil || sqm == nil {
		return nil
	}
	if len(sqm.AddQuestions) > 0 {
		if err := l.AddSimilarQuestions(ctx, sqm.AddQuestions); err != nil {
			return err
		}
	}
	if len(sqm.DeleteQuestions) > 0 {
		if err := l.DeleteSimilarQuestions(ctx, sqm.DeleteQuestions); err != nil {
			return err
		}
	}
	if len(sqm.UpdateQuestions) > 0 {
		if err := l.UpdateSimilarQuestions(ctx, qa, sqm.UpdateQuestions); err != nil {
			return err
		}
	}

	return nil
}

// GetNewCharSize 修改 qa 时，根据已有的 size 和 modify 请求里的相似问的信息，重新计算最新的 size, oldQA, 返回新的QASize
func (l *Logic) GetNewCharSize(ctx context.Context, oldQA *qaEntity.DocQA, modifyReq *pb.ModifyQAReq) (uint64, uint64, error) {
	newVideoSize, newVideoBytes, err := l.GetVideoURLsCharSize(ctx, modifyReq.GetAnswer())
	if err != nil {
		logx.E(ctx, "GetNewCharSize|newVideoSize|failed err:%d", err)
		return 0, 0, err
	}
	oldVideoSize, oldVideoBytes, err := l.GetVideoURLsCharSize(ctx, oldQA.Answer)
	if err != nil {
		logx.E(ctx, "GetNewCharSize|oldVideoSize|failed err:%d", err)
		return 0, 0, err
	}
	logx.I(ctx, "modifyQa|newVideoSize:%d, oldVideoSize:%d",
		newVideoSize, oldVideoSize)
	// 新的主问 size
	newQASize := utf8.RuneCountInString(modifyReq.GetQuestion()+modifyReq.GetAnswer()) + newVideoSize
	newQABytes := len(modifyReq.GetQuestion()+modifyReq.GetAnswer()) + int(newVideoBytes)

	// 旧的主问 size
	oldQASize := utf8.RuneCountInString(oldQA.Question+oldQA.Answer) + oldVideoSize
	oldQABytes := len(oldQA.Question+oldQA.Answer) + int(oldVideoBytes)
	similarModify := modifyReq.GetSimilarQuestionModify()
	if similarModify == nil {
		// 当前修改不涉及相似问，需要考虑主问的大小变化
		newSize := int64(oldQA.CharSize) + int64(newQASize) - int64(oldQASize)
		newBytesSize := int64(oldQA.QaSize) + int64(newQABytes) - int64(oldQABytes)
		if newSize <= 0 {
			logx.E(ctx, "GetNewCharSize failed get invalid size:%d", newSize)
			return 0, 0, nil
		}
		return uint64(newSize), uint64(newBytesSize), nil
	}
	addSimsSize, addSimsBytes := 0, 0
	for _, addQ := range similarModify.GetAddQuestions() {
		addSimsSize += utf8.RuneCountInString(strings.TrimSpace(addQ))
		addSimsBytes += len(strings.TrimSpace(addQ))
	}
	delSimsSize, delSimsBytes := 0, 0
	for _, deleteQ := range similarModify.GetDeleteQuestions() {
		delSimsSize += utf8.RuneCountInString(strings.TrimSpace(deleteQ.GetQuestion()))
		delSimsBytes += len(strings.TrimSpace(deleteQ.GetQuestion()))
	}

	updateBizIds := make([]uint64, 0, len(similarModify.GetUpdateQuestions()))
	updateSimsSizeNew, updateSimsBytesNew := 0, 0
	for _, updateQ := range similarModify.GetUpdateQuestions() {
		updateBizIds = append(updateBizIds, updateQ.GetSimBizId())
		updateSimsSizeNew += utf8.RuneCountInString(strings.TrimSpace(updateQ.GetQuestion()))
		updateSimsBytesNew += len(strings.TrimSpace(updateQ.GetQuestion()))
	}
	updateSimsSizeOld, updateSimsBytesOld, _ := l.GetSimilarQuestionsCharSize(ctx, oldQA, updateBizIds)

	// 新 char size
	newSize := int64(oldQA.CharSize) + int64(newQASize) - int64(oldQASize) + int64(updateSimsSizeNew) -
		int64(updateSimsSizeOld) + int64(addSimsSize) - int64(delSimsSize)
	newBytesSize := int64(oldQA.QaSize) + int64(newQABytes) - int64(oldQABytes) + int64(updateSimsBytesNew) -
		int64(updateSimsBytesOld) + int64(addSimsBytes) - int64(delSimsBytes)
	if newSize <= 0 {
		logx.E(ctx, "GetNewCharSize failed get invalid size:%d", newSize)
		return 0, 0, nil
	}

	return uint64(newSize), uint64(newBytesSize), nil
}

// NewSimilarQuestionsFromDBAndReq 从 db 中和修改请求中生成去重后的相似问
func (l *Logic) NewSimilarQuestionsFromDBAndReq(ctx context.Context, qa *qaEntity.DocQA,
	similarModify *pb.SimilarQuestionModify, isAll bool) (*qaEntity.SimilarQuestionModifyInfo, error) {
	if qa == nil {
		return nil, errors.New("qa is nil")
	}
	var err error
	currentSqs := make([]*qaEntity.SimilarQuestion, 0)
	if isAll {
		currentSqs, err = l.GetSimilarQuestionsByQA(ctx, qa)
	} else {
		currentSqs, err = l.GetSimilarQuestionsByQAWithoutRelease(ctx, qa)
	}
	if err != nil {
		return nil, err
	}
	sqm, err := qaEntity.NewSimilarQuestionsFromModifyReq(ctx, qa, similarModify)
	if err != nil {
		return nil, err
	}
	if sqm == nil { // modify req 里没有相似问
		sqm = &qaEntity.SimilarQuestionModifyInfo{}
	}
	duplicatedIds := make(map[uint64]struct{})
	for _, q := range similarModify.GetDeleteQuestions() {
		duplicatedIds[q.GetSimBizId()] = struct{}{}
	}
	for _, q := range similarModify.GetUpdateQuestions() {
		duplicatedIds[q.GetSimBizId()] = struct{}{}
	}
	for i, sq := range currentSqs {
		if _, ok := duplicatedIds[sq.SimilarID]; ok {
			continue
		}
		// 针对已存储的相似问，如果没有被删除或者更新，这里都放到 update 列表里重新发布
		sqm.UpdateQuestions = append(sqm.UpdateQuestions, currentSqs[i])
	}

	return sqm, nil
}

// GetSimilarQuestionsCharSize 获取相似问的总长度(from db)
func (l *Logic) GetSimilarQuestionsCharSize(ctx context.Context, qa *qaEntity.DocQA, similarQIDS []uint64) (uint64, uint64,
	error) {
	if qa == nil || len(similarQIDS) == 0 {
		return 0, 0, nil
	}

	exceededStatus := []uint32{
		qaEntity.QAReleaseStatusCharExceeded,
		qaEntity.QAReleaseStatusResuming,
		qaEntity.QAReleaseStatusAppealFailCharExceeded,
		qaEntity.QAReleaseStatusAppealFailResuming,
		qaEntity.QAReleaseStatusAuditNotPassCharExceeded,
		qaEntity.QAReleaseStatusAuditNotPassResuming,
		qaEntity.QAReleaseStatusLearnFailCharExceeded,
		qaEntity.QAReleaseStatusLearnFailResuming,
	}

	filter := &qaEntity.SimilarityQuestionReq{
		RobotId:            qa.RobotID,
		CorpId:             qa.CorpID,
		IsDeleted:          qaEntity.QAIsNotDeleted,
		ReleaseStatusNotIn: exceededStatus,
		SimilarQuestionIDs: similarQIDS,
	}

	charSize, qaSize, err := l.qaDao.GetSimilarQuestionsCharSize(ctx, filter)

	if err != nil {

		return 0, 0, err
	}

	return charSize, qaSize, nil
}

// GetSimilarQuestionsByQAWithoutRelease 根据标准问获取非已发布状态的相似问
func (l *Logic) GetSimilarQuestionsByQAWithoutRelease(ctx context.Context, qa *qaEntity.DocQA) ([]*qaEntity.SimilarQuestion,
	error) {
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
		        AND release_status NOT IN (%s)
				LIMIT ?
	*/

	sqs := make([]*qaEntity.SimilarQuestion, 0)
	if qa == nil || qa.ID == 0 {
		logx.E(ctx, "GetSimilarQuestionsByQAWithoutRelease get invalid qa param %v", qa)
		return sqs, nil
	}

	// 忽略掉发布成功 和 待发布状态 的相似问
	notInStatus := []uint32{
		qaEntity.QAReleaseStatusInit,
		qaEntity.QAReleaseStatusSuccess,
	}

	req := &qaEntity.SimilarityQuestionReq{
		CorpId:             qa.CorpID,
		RobotId:            qa.RobotID,
		RelatedQAID:        qa.ID,
		IsDeleted:          qaEntity.QAIsNotDeleted,
		ReleaseStatusNotIn: notInStatus,
		PageSize:           uint32(config.App().DocQA.SimilarQuestionNumLimit),
	}

	list, err := l.qaDao.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, req)
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionsByQAWithoutRelease failed, args: %+v, err: %+v",
			req, err)
		return nil, err
	}

	logx.D(ctx, "GetSimilarQuestionsByQAWithoutRelease sqs len: %d", len(list))
	return list, nil
}

// GetSimilarQuestionsCount 获取相似问的总数
func (l *Logic) GetSimilarQuestionsCount(ctx context.Context, qa *qaEntity.DocQA) (int, error) {
	/*
			`
				SELECT
					count(*)
				FROM
				    t_qa_similar_question
				WHERE
				    robot_id = ?
				AND corp_id = ?
				AND is_deleted = ?
				AND related_qa_id = ?
		        AND release_status NOT IN (%s)
			`
	*/
	if qa == nil {
		logx.W(ctx, "GetSimilarQuestionsCount get invalid qa param %v", qa)
		return 0, nil
	}

	exceededStatus := []uint32{
		qaEntity.QAReleaseStatusCharExceeded,
		qaEntity.QAReleaseStatusResuming,
		qaEntity.QAReleaseStatusAppealFailCharExceeded,
		qaEntity.QAReleaseStatusAppealFailResuming,
		qaEntity.QAReleaseStatusAuditNotPassCharExceeded,
		qaEntity.QAReleaseStatusAuditNotPassResuming,
		qaEntity.QAReleaseStatusLearnFailCharExceeded,
		qaEntity.QAReleaseStatusLearnFailResuming,
	}

	filter := &qaEntity.SimilarityQuestionReq{
		RobotId:            qa.RobotID,
		CorpId:             qa.CorpID,
		IsDeleted:          qaEntity.QAIsNotDeleted,
		RelatedQAID:        qa.ID,
		ReleaseStatusNotIn: exceededStatus,
	}

	count, err := l.qaDao.GetQASimilarQuestionsCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionsCount failed err:%+v", err)
		return 0, err
	}

	return count, nil
}

// GetSimilarQuestionsCountByQAIDs 获取标准问对应的相似问个数
func (l *Logic) GetSimilarQuestionsCountByQAIDs(ctx context.Context, corpID, robotID uint64,
	qaIDs []uint64) (map[uint64]uint32, error) {
	/*
		 `
		SELECT
			related_qa_id, count(*) as total
		FROM
		    t_qa_similar_question
		WHERE
		     corp_id = ? AND robot_id = ? AND is_deleted = ? AND related_qa_id IN (%s)
		GROUP BY related_qa_id
	*/

	sqsMap := make(map[uint64]uint32)
	if len(qaIDs) == 0 {
		return sqsMap, nil
	}

	filter := &qaEntity.SimilarityQuestionReq{
		CorpId:       corpID,
		RobotId:      robotID,
		IsDeleted:    qaEntity.QAIsNotDeleted,
		RelatedQAIDs: qaIDs,
	}

	list, err := l.qaDao.GetSimilarQuestionsCountByQAIDs(ctx, filter)
	if err != nil {
		logx.E(ctx, "Failed to get similar questions count by qa ids, err:%+v", err)
		return nil, err
	}

	for _, sq := range list {
		sqsMap[sq.RelatedQAID] = sq.Total
	}

	return sqsMap, nil
}

// GetSimilarQuestionsByUpdateTime 根据更新时间获取相似问
func (l *Logic) GetSimilarQuestionsByUpdateTime(ctx context.Context, start, end time.Time, limit, offset uint64,
	appidList []uint64) ([]*qaEntity.SimilarQuestion, error) {
	/*
			`
			SELECT %s FROM t_qa_similar_question
			WHERE id > ? AND id <= ? AND update_time > ? AND update_time <= ? AND is_deleted = ?
		`
	*/

	idStart := offset
	idEnd := offset + limit

	if start.After(end) {
		logx.E(ctx, "GetSimilarQuestionsByUpdateTime failed, starttime is after endtime")
		return nil, fmt.Errorf("starttime is after endtime")
	}

	listReq := &qaEntity.SimilarityQuestionReq{
		IDMore: idStart,
		IDLess: idEnd,

		StartMore: &start,
		EndLess:   &end,
		IsDeleted: qaEntity.QAIsDeleted,

		RobotIDs: appidList,
	}

	logx.I(ctx, "GetSimilarQuestionsByUpdateTime args: %+v", listReq)

	simQas, err := l.qaDao.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, listReq)

	if err != nil {
		logx.E(ctx, "GetSimilarQuestionsByUpdateTime failed, err: %+v", err)
		return nil, err
	}
	logx.I(ctx, "GetSimilarQuestionsByUpdateTime simQas len: %d", len(simQas))
	return simQas, nil
}

// GetSimilarQuestionsSimpleByQAIDs 根据qaIDs批量获取相似问id和内容
func (l *Logic) GetSimilarQuestionsSimpleByQAIDs(ctx context.Context, corpID, robotID uint64,
	qaIDs []uint64) (map[uint64][]*qaEntity.SimilarQuestionSimple, error) {
	if len(qaIDs) == 0 {
		return nil, nil
	}

	listReq := &qaEntity.SimilarityQuestionReq{
		CorpId:       corpID,
		RobotId:      robotID,
		IsDeleted:    qaEntity.QAIsNotDeleted,
		RelatedQAIDs: qaIDs,
	}

	simQAs, err := l.qaDao.BatchListSimilarQuestions(ctx, listReq)

	if err != nil {
		logx.E(ctx, "根据QAIDs获取相似问对列表失败 err:%+v", err)
		return nil, err
	}

	sqsMap := make(map[uint64][]*qaEntity.SimilarQuestionSimple)

	for _, sq := range simQAs {
		if _, ok := sqsMap[sq.RelatedQAID]; !ok {
			sqsMap[sq.RelatedQAID] = make([]*qaEntity.SimilarQuestionSimple, 0)
		}
		sqsMap[sq.RelatedQAID] = append(sqsMap[sq.RelatedQAID], &qaEntity.SimilarQuestionSimple{
			SimilarID:   sq.SimilarID,
			Question:    sq.Question,
			RelatedQAID: sq.RelatedQAID,
		})
	}

	return sqsMap, nil
}

// GetQASimilarQuestionsCount 获取qa的相似问总数
func (l *Logic) GetQASimilarQuestionsCount(ctx context.Context, corpID, appID uint64) (int, error) {

	filter := &qaEntity.SimilarityQuestionReq{
		CorpId:  corpID,
		RobotId: appID,
	}

	total, err := l.qaDao.GetQASimilarQuestionsCount(ctx, filter)

	if err != nil {
		logx.E(ctx, "GetQASimilarQuestionsCount fail, err: %+v", err)
		return 0, err
	}
	return total, nil
}

// GenerateSimilarQuestions 生成相似问题
func (l *Logic) GenerateSimilarQuestions(ctx context.Context, app *entity.App, question, answer, modelName string) (
	[]string, error) {
	if app == nil {
		return nil, errs.ErrRobotNotFound
	}
	if question == "" {
		return nil, errs.ErrGenerateSimilarParams
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	dosage, err := l.financeLogic.GetTokenDosage(newCtx, app, modelName)
	if err != nil {
		logx.E(ctx, "task(DocToQA) Init err: %v", err)
		return nil, err
	}
	if dosage == nil {
		logx.E(ctx, "task(DocToQA) Init dosage is nil")
		return nil, errs.ErrSystem
	}
	// 生成相似问使用文档生成问答相同的模型配置
	if !l.financeLogic.CheckModelStatus(newCtx, app.CorpPrimaryId, dosage.ModelName, rpc.DocExtractSimilarQAType) {
		logx.W(ctx, "GenerateSimilarQuestions checkModelStatus failed, modelName:%s", dosage.ModelName)
		return nil, errs.ErrNoTokenBalance
	}
	dosage.StartTime = time.Now()
	prompt, err := l.llmLogic.GetPrompt(newCtx, app, entity.GenerateSimilarQAModel)
	if err != nil {
		logx.E(ctx, "GenerateSimilarQuestions GetPrompt error: %+v", err)
		return nil, err
	}
	similarQuestions, statisticInfo, err := l.LLMGenerateSimilarQuestions(newCtx, app.BizId, dosage.ModelName, prompt, question, answer)
	if err != nil {
		logx.E(ctx, "LLM GenerateSimilarQuestions error: %+v", err)
		return similarQuestions, err
	}
	logx.I(ctx, "LLM GenerateSimilarQuestions|statisticInfo:%+v", statisticInfo)
	if len(similarQuestions) == 0 { // 返回内容为空 不做计费上报
		logx.W(ctx, "LLM|similarQuestions is empty|statisticInfo:%+v",
			statisticInfo)
		return similarQuestions, nil
	}
	// 生成相似问使用文档生成问答相同的模型配置
	err = l.financeLogic.ReportTokenDosage(newCtx, statisticInfo, dosage, app.CorpPrimaryId, rpc.DocExtractSimilarQAType, app)
	if err != nil {
		// 只打印ERROR日志，降级处理
		logx.E(ctx, "LLM GenerateSimilarQuestions reportSimilarQuestionsTokenDosage error: %+v", err)
	}
	return similarQuestions, nil
}

// LLMGenerateSimilarQuestions 生成相似问题
func (l *Logic) LLMGenerateSimilarQuestions(ctx context.Context, appBizID uint64, modelName, prompt, question, answer string) ([]string,
	*rpc.StatisticInfo, error) {
	start := time.Now()
	var similarQuestions []string
	prompt, err := util.Render(ctx, prompt, SimilarQAExtract{Question: question, Answer: answer})
	if err != nil {
		logx.E(ctx, "LLMGenerateSimilarQuestions Render failed, err:%+v", err)
		return similarQuestions, nil, err
	}
	if modelName == "" {
		logx.E(ctx, "LLMGenerateSimilarQuestions failed, modelName is empty")
		return similarQuestions, nil, errs.ErrNotInvalidModel
	}

	req := &rpc.LlmRequest{
		RequestId: codec.Message(ctx).DyeingKey(),
		BizAppId:  appBizID,
		StartTime: time.Now(),
		ModelName: modelName,
		Messages:  []*rpc.Message{{Role: rpc.Role_USER, Content: prompt}},
	}

	rsp, err := l.rpc.AIGateway.SimpleChat(ctx, req)
	if err != nil {
		logx.E(ctx, "LLMGenerateSimilarQuestions error: %+v, prompt: %s", err, prompt)
		return similarQuestions, nil, err
	}
	costTime := time.Since(start).Seconds()
	similarQuestions = formatSimilarQuestions(ctx, rsp.GetReplyContent())
	logx.I(ctx, "LLMGenerateSimilarQuestions|SimpleChat prompt:%+v rsp:%+v costTime:%+v",
		prompt, rsp, costTime)
	return similarQuestions, rsp.GetStatisticInfo(), nil
}

// formatSimilarQuestions 返回结果格式化为相似问集合
func formatSimilarQuestions(ctx context.Context, content string) []string {
	logx.D(ctx, "formatSimilarQuestions question:%s", content)
	// 将 "\\n" 替换为 "\n"
	content = strings.ReplaceAll(content, "\\n", "\\\n")
	// 然后按 "\n" 分割字符串
	lines := strings.Split(content, "\n")
	var similarQuestions []string
	questionSet := make(map[string]bool) // 用于去重的集合
	lastNumber := 0
	for k, line := range lines {
		if line == "" {
			logx.D(ctx, "formatSimilarQuestions line:%d is empty", k)
			continue
		}
		// 如果这一行不是以数字+符号点开头的，丢弃掉
		if !regexp.MustCompile(`^\d+\.`).MatchString(line) {
			logx.D(ctx, "formatSimilarQuestions line:%d lineContent:%s notNumStart", k, line)
			continue
		}
		// 检查数字是否有序
		numberStr := regexp.MustCompile(`^\d+`).FindString(line)
		number, _ := strconv.Atoi(numberStr)
		if number != lastNumber+1 {
			logx.D(ctx, "formatSimilarQuestions line:%d lineContent:%s lastNumber:%d NumIndexFail",
				k, line, lastNumber)
			continue
		}
		lastNumber = number
		// 只截取掉第一个数字和点，保留后面的内容
		line = regexp.MustCompile(`^\d+\.`).ReplaceAllString(line, "")
		line = strings.TrimSpace(line)
		if line == "" {
			logx.D(ctx, "formatSimilarQuestions line:%d lineContent:%s is empty", k, line)
			continue
		}
		// 检查是否已存在相同问题
		if !questionSet[line] {
			questionSet[line] = true
			similarQuestions = append(similarQuestions, line)
		} else {
			logx.D(ctx, "formatSimilarQuestions duplicate question found: %s", line)
		}
	}
	return similarQuestions
}

// AddSimilarQuestionFromUnsatisfiedReply 将不满意回复添加到QA相似问
func (l *Logic) AddSimilarQuestionFromUnsatisfiedReply(ctx context.Context, qaBizID, unsatisfiedReplyBizID uint64, unsatisfiedReplyContent string) error {
	logx.I(ctx, "AddSimilarQuestionFromUnsatisfiedReply qaBizID:%d, unsatisfiedReplyBizID:%d, content:%s", qaBizID, unsatisfiedReplyBizID, unsatisfiedReplyContent)

	// 通过QABizID查询QA数据
	qa, err := l.GetQAByBizID(ctx, qaBizID)
	if err != nil {
		logx.E(ctx, "AddSimilarQuestionFromUnsatisfiedReply GetQAByBizID failed, qaBizID:%d, err:%+v", qaBizID, err)
		return err
	}
	if qa == nil || qa.IsDeleted == qaEntity.QAIsDeleted {
		logx.E(ctx, "AddSimilarQuestionFromUnsatisfiedReply QA not found, qaBizID:%d", qaBizID)
		return errs.ErrQANotFound
	}
	if qa.Question == unsatisfiedReplyContent {
		logx.E(ctx, "AddSimilarQuestionFromUnsatisfiedReply QA question is equal to unsatisfiedReplyContent, qaBizID:%d, qaQuestion:%s, unsatisfiedReplyContent:%s", qaBizID, qa.Question, unsatisfiedReplyContent)
		return errs.ErrWrapf(errs.ErrCodeSimilarQuestionRepeated, i18n.Translate(ctx, i18nkey.KeyDuplicateSimilarQuestionFound), qa.Question)
	}

	// 获取现有的相似问
	existingSimilarQuestions, err := l.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx, "AddSimilarQuestionFromUnsatisfiedReply GetSimilarQuestionsByQA failed, qaID:%d, err:%+v", qa.ID, err)
		return err
	}

	// 检查是否已存在相同的相似问
	for _, sq := range existingSimilarQuestions {
		if sq.Question == unsatisfiedReplyContent {
			logx.I(ctx, "AddSimilarQuestionFromUnsatisfiedReply similar question already exists, qaID:%d, question:%s", qa.ID, unsatisfiedReplyContent)
			return errs.ErrWrapf(errs.ErrCodeSimilarQuestionRepeated, i18n.Translate(ctx, i18nkey.KeyDuplicateSimilarQuestionFound), sq.Question)
		}
	}

	// 创建新的相似问
	newSimilarQuestions := qaEntity.NewSimilarQuestions(ctx, qa, []string{unsatisfiedReplyContent})
	if len(newSimilarQuestions) == 0 {
		logx.E(ctx, "AddSimilarQuestionFromUnsatisfiedReply NewSimilarQuestions failed, qaID:%d", qa.ID)
		return errs.ErrSystem
	}

	qa.StaffID = contextx.Metadata(ctx).StaffID()
	qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditing
	qa.IsAuditFree = qaEntity.QAIsAuditNotFree
	if err = l.UpdateQA(ctx, qa, &qaEntity.SimilarQuestionModifyInfo{
		AddQuestions: newSimilarQuestions,
	}, true, true, 0, 0, &labelEntity.UpdateQAAttributeLabelReq{
		IsNeedChange:    false,
		AttributeLabels: make([]*labelEntity.QAAttributeLabel, 0),
	}); err != nil {
		return errs.ErrSystem
	}

	// 修改不满意回复状态从Wait改为Similar
	err = l.ModifyUnsatisfiedReplyStatus(ctx, qa.CorpID, qa.RobotID, unsatisfiedReplyBizID, entity.UnsatisfiedReplyStatusWait, entity.UnsatisfiedReplyStatusSimilar)
	if err != nil {
		logx.E(ctx, "AddSimilarQuestionFromUnsatisfiedReply ModifyUnsatisfiedReplyStatus failed, qaID:%d, unsatisfiedReplyBizID:%d, err:%+v", qa.ID, unsatisfiedReplyBizID, err)
		return err
	}
	logx.I(ctx,
		"AddSimilarQuestionFromUnsatisfiedReply modify unsatisfied reply status success, qaID:%d, unsatisfiedReplyBizID:%d content:%s",
		qa.ID, unsatisfiedReplyBizID, unsatisfiedReplyContent,
	)

	return nil
}
