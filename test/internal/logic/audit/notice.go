package audit

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"strconv"
)

// sendAuditNotPassNotice 审核/申诉未通过，发送通知
func sendAuditNotPassNotice(ctx context.Context, d dao.Dao, doc *model.Doc,
	audit *model.Audit, auditsMap map[uint32][]*model.AuditStatusSourceList, isAppeal bool, rejectReason string) error {
	var err error
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}},
		{Typ: model.OpTypeAppeal, Params: model.OpParams{
			AppealType: model.AppealBizTypeDoc,
			DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
		}},
	}
	content := i18n.Translate(ctx, i18nkey.KeyReviewNotPassedWithName, doc.FileName)
	title := i18n.Translate(ctx, i18nkey.KeyDocumentReviewNotPassed)
	if _, ok := auditsMap[model.AuditStatusTimeoutFail]; ok {
		content = i18n.Translate(ctx, i18nkey.KeyReviewTimeoutWithName, doc.FileName)
		operations = []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}},
			{Typ: model.OpTypeRetry, Params: model.OpParams{DocBizID: strconv.FormatUint(doc.BusinessID, 10)}},
			{Typ: model.OpTypeAppeal, Params: model.OpParams{
				AppealType: model.AppealBizTypeDoc,
				DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
			}},
		}
	}
	if isAppeal {
		content = i18n.Translate(ctx, i18nkey.KeyManualAppealNotPassedWithNameModifyAndReason, doc.FileName, rejectReason)
		operations = []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
		title = i18n.Translate(ctx, i18nkey.KeyDocumentManualAppealNotPassed)
	}

	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelError),
		model.WithSubject(title),
		model.WithContent(content),
	}
	notice := model.NewNotice(model.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = d.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}
