package qa

import (
	"context"
	"strings"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"gorm.io/gorm"
)

// CheckQAAndDescAndParam 检查问题答案｜问题描述|自定义参数限制
func (l *Logic) CheckQAAndDescAndParam(ctx context.Context, question, answer, questionDesc, param string) error {
	err := l.CheckQuestionAndAnswer(ctx, question, answer)
	if err != nil {
		return err
	}
	err = l.CheckQuestionDesc(ctx, questionDesc)
	if err != nil {
		return err
	}
	paramCfg := config.App().DocQA.CustomParam
	param = strings.TrimSpace(param)
	if len([]rune(param)) < paramCfg.MinLength {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeyCustomParamTooShort), paramCfg.MinLength)
	}
	if len([]rune(param)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, paramCfg.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooLong, i18n.Translate(ctx, i18nkey.KeyCustomParamTooLong),
			paramCfg.MaxLength)
	}
	return nil
}

func (l *Logic) CheckQuestionAndAnswer(ctx context.Context, question, answer string) error {
	cfg := config.App().DocQA
	question = strings.TrimSpace(question)
	answer = strings.TrimSpace(answer)
	if len([]rune(question)) < cfg.Question.MinLength {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooShort), cfg.Question.MinLength)
	}
	if len([]rune(question)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Question.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooLong, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooLong),
			i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Question.MaxLength))
	}
	if len([]rune(answer)) < cfg.Answer.MinLength {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooShort, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooShort), cfg.Answer.MinLength)
	}
	if len([]rune(answer)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Answer.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooLong, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooLong),
			i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Answer.MaxLength))
	}
	return nil
}

// CheckQuestionDesc 检查问题描述限制
func (l *Logic) CheckQuestionDesc(ctx context.Context, questionDesc string) error {
	questionDescCfg := config.App().DocQA.QuestionDesc
	questionDesc = strings.TrimSpace(questionDesc)
	if len([]rune(questionDesc)) < questionDescCfg.MinLength {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeyProblemDescriptionTooShort), questionDescCfg.MinLength)
	}
	if len([]rune(questionDesc)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, questionDescCfg.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooLong, i18n.Translate(ctx, i18nkey.KeyProblemDescriptionTooLong),
			questionDescCfg.MaxLength)
	}
	return nil
}

// CheckSimilarQuestionNumLimit 检查相似问总的数量限制
func (l *Logic) CheckSimilarQuestionNumLimit(ctx context.Context, newNum, deleteNum, existedNum int) (totalLength int, err error) {
	cfg := config.App().DocQA
	totalNum := existedNum + newNum - deleteNum
	if totalNum > config.App().DocQA.SimilarQuestionNumLimit {
		return 0, errs.ErrWrapf(errs.ErrCodeSimilarQuestionExceedLimit,
			i18n.Translate(ctx, i18nkey.KeySimilarQuestionLimitExceeded),
			cfg.SimilarQuestionNumLimit)
	}
	return totalNum, nil
}

// CheckSimilarQuestionContent 检查相似问内容: 是否存在重复, 以及每一个相似问的字符数(满足限制), 返回相似问总字符数
func (l *Logic) CheckSimilarQuestionContent(ctx context.Context, qa string, sqs []string) (simTotalCharSize int, simTotalBytes int, err error) {
	if len(qa) == 0 || len(sqs) == 0 {
		return 0, 0, nil
	}
	cfg := config.App().DocQA
	simTotalCharSize = 0
	simTotalBytes = 0
	allQuestions := make(map[string]struct{})
	allQuestions[strings.TrimSpace(qa)] = struct{}{}
	for _, q := range sqs {
		s := strings.TrimSpace(q)
		if _, ok := allQuestions[s]; ok {
			return 0, 0, errs.ErrWrapf(errs.ErrCodeSimilarQuestionRepeated, i18n.Translate(ctx, i18nkey.KeyDuplicateSimilarQuestionFound),
				s)
		}
		if len([]rune(s)) < cfg.SimilarQuestion.MinLength {
			return 0, 0, errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooShort),
				cfg.SimilarQuestion.MinLength)
		}
		if len([]rune(s)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.SimilarQuestion.MaxLength) {
			return 0, 0, errs.ErrWrapf(errs.ErrCodeQuestionTooLong, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooLong),
				cfg.SimilarQuestion.MaxLength)
		}
		simTotalCharSize += utf8.RuneCountInString(s)
		simTotalBytes += len(s)
		allQuestions[s] = struct{}{}
	}
	return simTotalCharSize, simTotalBytes, nil
}

// ValidateDocAndRetrieveID 验证文档是否存在，并返回文档ID
func (l *Logic) ValidateDocAndRetrieveID(ctx context.Context, docBizID uint64, robotID uint64) (uint64, error) {
	if docBizID == 0 {
		return 0, nil
	}

	logx.D(ctx, "validateDocAndRetrieveID docBizID:%+v, robotID:%d", docBizID, robotID)

	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()

	dbClients := make([]*gorm.DB, 0)

	if robotID == knowClient.NotVIP {
		dbClients = knowClient.GetAllGormClients(ctx, tableName)
	} else {
		db, err := knowClient.GormClient(ctx, tableName, robotID, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "get GormClient failed, err: %+v", err)
			return 0, err
		}
		dbClients = append(dbClients, db)
	}

	filter := &docEntity.DocFilter{
		BusinessIds: []uint64{docBizID},
	}

	var err error
	list := make([]*docEntity.Doc, 0)
	for _, db := range dbClients {
		list, err = l.docDao.GetDocListWithFilter(ctx, docEntity.DocTblColList, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocByBizIDs failed, err: %+v", err)
			continue
		}
		break
	}

	if err != nil {
		return 0, err
	}
	if len(list) == 0 {
		return 0, errs.ErrDocNotFound
	}
	doc := list[0]
	if doc.HasDeleted() {
		return 0, errs.ErrDocHasDeleted
	}
	if doc.RobotID != robotID { // 文档不属于该应用
		return 0, errs.ErrDocNotFound
	}
	return doc.ID, nil
}
