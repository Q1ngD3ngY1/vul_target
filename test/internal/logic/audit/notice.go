package audit

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

// sendNoticeAndUpdateAuditStatusIfDocAuditFail 审核/申诉未通过，更新审核状态，发送通知
func (l *Logic) sendNoticeAndUpdateAuditStatusIfDocAuditFail(ctx context.Context, doc *docEntity.Doc,
	audit *releaseEntity.Audit, auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList, isAppeal bool, rejectReason string) error {
	var err error
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
		{Type: releaseEntity.OpTypeAppeal, Params: releaseEntity.OpParams{
			AppealType: entity.AppealBizTypeDoc,
			DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
		}},
	}
	content := i18n.Translate(ctx, i18nkey.KeyReviewNotPassedWithName, doc.FileName)
	title := i18n.Translate(ctx, i18nkey.KeyDocumentReviewNotPassed)
	if _, ok := auditsMap[releaseEntity.AuditStatusTimeoutFail]; ok {
		content = i18n.Translate(ctx, i18nkey.KeyReviewTimeoutWithName, doc.FileName)
		operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
			{Type: releaseEntity.OpTypeRetry, Params: releaseEntity.OpParams{DocBizID: strconv.FormatUint(doc.BusinessID, 10)}},
			{Type: releaseEntity.OpTypeAppeal, Params: releaseEntity.OpParams{
				AppealType: entity.AppealBizTypeDoc,
				DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
			}},
		}
	}
	if isAppeal {
		content = i18n.Translate(ctx, i18nkey.KeyManualAppealNotPassedWithNameModifyAndReason, doc.FileName, rejectReason)
		operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
		title = i18n.Translate(ctx, i18nkey.KeyDocumentManualAppealNotPassed)
	}

	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelError),
		releaseEntity.WithSubject(title),
		releaseEntity.WithContent(content),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	if err = l.UpdateAuditStatus(ctx, audit); err != nil {
		logx.E(ctx, "Failed to update audit status err:%+v", err)
		return err
	}
	return nil
}

// sendNoticeAndUpdateAuditStatusIfDocNameAuditFail 审核/申诉未通过，更新审核状态，发送通知
func (l *Logic) sendNoticeAndUpdateAuditStatusIfDocNameAuditFail(ctx context.Context, doc *docEntity.Doc,
	audit *releaseEntity.Audit, auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList, isAppeal bool, rejectReason string) error {
	var err error
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
		{Type: releaseEntity.OpTypeAppeal, Params: releaseEntity.OpParams{
			AppealType: entity.AppealBizTypeDoc,
			DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
		}},
	}
	content := i18n.Translate(ctx, i18nkey.KeyReviewNotPassedWithName, doc.GetRealFileName())
	title := i18n.Translate(ctx, i18nkey.KeyDocumentRenameReviewNotPassed)
	if _, ok := auditsMap[releaseEntity.AuditStatusTimeoutFail]; ok {
		content = i18n.Translate(ctx, i18nkey.KeyReviewTimeoutWithName, doc.FileName)
		operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
			{Type: releaseEntity.OpTypeRetry, Params: releaseEntity.OpParams{DocBizID: strconv.FormatUint(doc.BusinessID, 10)}},
			{Type: releaseEntity.OpTypeAppeal, Params: releaseEntity.OpParams{
				AppealType: entity.AppealBizTypeDoc,
				DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
			}},
		}
	}
	if isAppeal {
		content = i18n.Translate(ctx, i18nkey.KeyManualAppealNotPassedWithNameReModifyAndReason, doc.GetRealFileName(), rejectReason)
		operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
		title = i18n.Translate(ctx, i18nkey.KeyDocumentRenameManualAppealNotPassed)
	}

	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelError),
		releaseEntity.WithSubject(title),
		releaseEntity.WithContent(content),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	if err = l.UpdateAuditStatus(ctx, audit); err != nil {
		logx.E(ctx, "Failed to update audit status err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) sendNotice(
	ctx context.Context,
	isNeedNotice bool,
	audit *releaseEntity.Audit,
	noticeType, pageID uint32,
	subject string,
) error {
	if !isNeedNotice {
		return nil
	}
	operations := make([]releaseEntity.Operation, 0)
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(pageID),
		releaseEntity.WithLevel(releaseEntity.LevelInfo),
		releaseEntity.WithContent(subject),
		releaseEntity.WithForbidCloseFlag(),
	}
	notice := releaseEntity.NewNotice(noticeType, audit.ID, audit.CorpID, audit.RobotID, audit.CreateStaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}

// SendNoticeIfDocAppealPass 人工申诉通过，发送通知
func (l *Logic) SendNoticeIfDocAppealPass(ctx context.Context, doc *docEntity.Doc,
	audit *releaseEntity.Audit) error {
	var err error
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
	content := i18n.Translate(ctx, i18nkey.KeyManualAppealPassedWithName, doc.FileName)
	title := i18n.Translate(ctx, i18nkey.KeyDocumentManualAppealPassed)
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelInfo),
		releaseEntity.WithSubject(title),
		releaseEntity.WithContent(content),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}

// sendNoticeIfDocNameAppealPass 人工申诉通过，发送通知
func (l *Logic) sendNoticeIfDocNameAppealPass(ctx context.Context, doc *docEntity.Doc,
	audit *releaseEntity.Audit) error {
	var err error
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
	content := i18n.Translate(ctx, i18nkey.KeyManualAppealPassedWithName, doc.GetRealFileName())
	title := i18n.Translate(ctx, i18nkey.KeyDocumentRenameManualAppealPassed)
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelInfo),
		releaseEntity.WithSubject(title),
		releaseEntity.WithContent(content),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "CreateNotice err:%+v err:%+v", notice, err)
		return err
	}
	return nil
}

// FailCharSizeNotice 文档字符数总量已超限制失败通知
func (l *Logic) FailCharSizeNotice(ctx context.Context, doc *docEntity.Doc) error {
	logx.D(ctx, "FailCharSizeNotice , doc: %+v", doc)
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelError),
		releaseEntity.WithSubject("文档导入失败。"),
		releaseEntity.WithContent(fmt.Sprintf("文档【%s】导入失败，失败原因：%s", doc.FileName,
			i18n.Translate(ctx, errs.ConvertErr2i18nKeyMsg(common.ConvertErrMsg(ctx, l.rpc, 0, doc.CorpID, errs.ErrOverCharacterSizeLimit))))),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "CreateNotice err:%+v err:%+v", notice, err)
		return err
	}
	return nil
}

// sendAuditNotPassNotice 审核/申诉未通过，发送通知
func sendAuditNotPassNotice(ctx context.Context, r *rpc.RPC, doc *docEntity.Doc,
	audit *releaseEntity.Audit, auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList, isAppeal bool, rejectReason string) error {
	var err error
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
		{Type: releaseEntity.OpTypeAppeal, Params: releaseEntity.OpParams{
			AppealType: entity.AppealBizTypeDoc,
			DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
		}},
	}
	content := i18n.Translate(ctx, i18nkey.KeyReviewNotPassedWithName, doc.FileName)
	title := i18n.Translate(ctx, i18nkey.KeyDocumentReviewNotPassed)
	if _, ok := auditsMap[releaseEntity.AuditStatusTimeoutFail]; ok {
		content = i18n.Translate(ctx, i18nkey.KeyReviewTimeoutWithName, doc.FileName)
		operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
			{Type: releaseEntity.OpTypeRetry, Params: releaseEntity.OpParams{DocBizID: strconv.FormatUint(doc.BusinessID, 10)}},
			{Type: releaseEntity.OpTypeAppeal, Params: releaseEntity.OpParams{
				AppealType: entity.AppealBizTypeDoc,
				DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
			}},
		}
	}
	if isAppeal {
		content = i18n.Translate(ctx, i18nkey.KeyManualAppealNotPassedWithNameModifyAndReason, doc.FileName, rejectReason)
		operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
		title = i18n.Translate(ctx, i18nkey.KeyDocumentManualAppealNotPassed)
	}

	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelError),
		releaseEntity.WithSubject(title),
		releaseEntity.WithContent(content),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = r.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}

// sendNoticeForQaAudit 问答审核/申诉之后，发送通知
func (l *Logic) sendNoticeForQaAudit(ctx context.Context, qa *qaEntity.DocQA, sims []*qaEntity.SimilarQuestion,
	audit *releaseEntity.Audit, pass, isAppeal, isExceeded bool, rejectReason string) error {
	var (
		err            error
		content, title string
		operations     []releaseEntity.Operation
		noticeOptions  []releaseEntity.NoticeOption
	)
	if pass && !isAppeal { // 机器审核通过不需要发通知
		return nil
	}
	if pass && isAppeal && isExceeded { // 申诉通过，但是处于超量状态不需要发通知
		return nil
	}
	if !pass {
		if !isAppeal {
			operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}},
				{Type: releaseEntity.OpTypeAppeal, Params: releaseEntity.OpParams{
					AppealType: entity.AppealBizTypeQa,
					QaBizID:    strconv.FormatUint(qa.BusinessID, 10),
				}},
			}
			failedSlice := make([]string, 0)
			if qa.QaAuditFail {
				failedSlice = append(failedSlice, i18n.Translate(ctx, i18nkey.KeyQA))
			}
			if qa.PicAuditFail {
				failedSlice = append(failedSlice, i18n.Translate(ctx, i18nkey.KeyImage))
			}
			if qa.VideoAuditFail {
				failedSlice = append(failedSlice, i18n.Translate(ctx, i18nkey.KeyVideo))
			}
			for _, v := range sims {
				if v.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass {
					failedSlice = append(failedSlice, i18n.Translate(ctx, i18nkey.KeySimilarQuestion))
					break
				}
			}
			if len(failedSlice) > 0 {
				content = i18n.Translate(ctx, i18nkey.KeyReviewNotPassedInWithName, qa.Question,
					strings.Join(failedSlice, "/"))
			} else { // 加个兜底
				content = i18n.Translate(ctx, i18nkey.KeyReviewNotPassedWithNameModifyQA, qa.Question)
			}
			title = i18n.Translate(ctx, i18nkey.KeyQAReviewNotPassed)
		} else {
			content = i18n.Translate(ctx, i18nkey.KeyManualAppealNotPassedWithNameModifyAndReason, qa.Question, rejectReason)
			operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
			title = i18n.Translate(ctx, i18nkey.KeyQAManualAppealFailure)
		}
		noticeOptions = []releaseEntity.NoticeOption{
			releaseEntity.WithGlobalFlag(),
			releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
			releaseEntity.WithLevel(releaseEntity.LevelError),
			releaseEntity.WithSubject(title),
			releaseEntity.WithContent(content),
		}

	} else if isAppeal {
		content = i18n.Translate(ctx, i18nkey.KeyManualAppealPassedWithName, qa.Question)
		operations = []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
		title = i18n.Translate(ctx, i18nkey.KeyQAManualAppealSuccess)
		noticeOptions = []releaseEntity.NoticeOption{
			releaseEntity.WithGlobalFlag(),
			releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
			releaseEntity.WithLevel(releaseEntity.LevelInfo),
			releaseEntity.WithSubject(title),
			releaseEntity.WithContent(content),
		}
	}

	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeQAAuditOrAppeal, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}

// CreateReleaseAppealNotice 创建问答发布申诉通知
func (l *Logic) CreateReleaseAppealNotice(ctx context.Context, numSuccess, numFail, numTotal uint32,
	audit *releaseEntity.Audit) error {
	subject := i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewSuccess)
	content := i18n.Translate(ctx, i18nkey.KeyPublishOnlineContentManualReviewSuccess)
	level := releaseEntity.LevelSuccess
	if numFail == numTotal || numSuccess == 0 {
		subject = i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewFailure)
		content = i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewFailureSensitiveInfo)
		level = releaseEntity.LevelError
	} else if numFail > 0 {
		subject = i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewPartialSuccess)
		content = i18n.Translate(ctx, i18nkey.KeyPublishOnlineContentManualReviewSuccessFailureCountSensitiveInfo, numSuccess, numFail)
		level = releaseEntity.LevelError
	}
	if err := l.releaseDao.Query().Transaction(func(tx *tdsqlquery.Query) error {
		operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
		noticeOptions := []releaseEntity.NoticeOption{
			releaseEntity.WithGlobalFlag(),
			releaseEntity.WithPageID(releaseEntity.NoticeWaitReleasePageID),
			releaseEntity.WithLevel(level),
			releaseEntity.WithSubject(subject),
			releaseEntity.WithContent(content),
		}
		notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeRelease, audit.ParentID, audit.CorpID, audit.RobotID,
			audit.CreateStaffID, noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "审核机器人昵称失败 err:%+v", err)
		return err
	}

	return nil
}

func (l *Logic) CreateRobotProfileNotice(ctx context.Context, level string, subject string, content string,
	audit *releaseEntity.Audit, opType uint32) error {
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
	if opType == releaseEntity.OpTypeAppeal {
		operations = append(operations, releaseEntity.Operation{
			Type:   opType,
			Params: releaseEntity.OpParams{AppealType: entity.AppealBizTypeRobotProfile},
		})
	}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeRobotInfoPageID),
		releaseEntity.WithLevel(level),
		releaseEntity.WithSubject(subject),
		releaseEntity.WithContent(content),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeRobotBasicInfo, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}
