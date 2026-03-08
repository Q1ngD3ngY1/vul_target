package dao

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec"
	"github.com/jmoiron/sqlx"
)

// InfoSecCreateAppeal 批量创建申诉申请
func (d *dao) InfoSecCreateAppeal(ctx context.Context, appealList []*model.Appeal, appealType uint32, robotID, corpID,
	staffID uint64, reason string) error {
	if len(appealList) == 0 {
		return errs.ErrParams
	}
	lists := make([]*infosec.CreateAppealReq_List, 0)
	for _, v := range appealList {
		lists = append(lists, &infosec.CreateAppealReq_List{
			Id:             v.ID,
			CorpId:         v.CorpID,
			CorpFullName:   v.CorpFullName,
			CreateStaffId:  v.CreateStaffID,
			AppealParentId: v.AppealParentID,
			AuditParentId:  v.AuditParentID,
			AuditId:        v.AuditID,
			Params:         v.Params,
			RelateId:       v.RelateID,
			Status:         v.Status,
			Result:         v.Result,
			InKeywordList:  v.InKeywordList,
			Operator:       v.Operator,
			Reason:         v.Reason,
		})
	}
	req := &infosec.CreateAppealReq{
		RobotId:    robotID,
		CorpId:     corpID,
		StaffId:    staffID,
		Type:       appealType,
		AppealList: lists,
		Reason:     reason,
	}

	_, err := d.infosecCli.CreateAppeal(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "提交申诉失败 req:%+v, err:%+v", req, err)
		return err
	}
	return nil
}

func (d *dao) getAppealNoticeByType(appealType uint32) (bool, uint32, uint32, string) {
	var (
		isNeedNotice bool
		noticeType   uint32
		pageID       uint32
		subject      string
	)
	switch appealType {
	case model.AppealBizTypeRobotProfile:
		isNeedNotice = true
		noticeType = model.NoticeTypeRobotBasicInfo
		pageID = model.NoticeRobotInfoPageID
		subject = "角色设置内容人工申诉中。"
	case model.AppealBizTypeBareAnswer:
		isNeedNotice = true
		noticeType = model.NoticeTypeBareAnswer
		pageID = model.NoticeBareAnswerPageID
		subject = "未知问题回复语人工申诉中。"
	case model.AppealBizTypeRelease:
		isNeedNotice = true
		noticeType = model.NoticeTypeRelease
		pageID = model.NoticeWaitReleasePageID
		subject = "发布内容人工申诉中。"
	case model.AppealBizTypeDoc:
		isNeedNotice = true
		noticeType = model.NoticeTypeDocToQA
		pageID = model.NoticeDocPageID
		subject = "文档人工申诉中。"
	case model.AppealBizTypeQa:
		isNeedNotice = true
		noticeType = model.NoticeTypeQAAuditOrAppeal
		pageID = model.NoticeQAPageID
		subject = "问答人工申诉中。"
	default:
		isNeedNotice = false
	}
	return isNeedNotice, noticeType, pageID, subject
}

// SendAppealNotice 发送申诉通知
func (d *dao) SendAppealNotice(
	ctx context.Context,
	appealType uint32,
	audit *model.Audit,
) error {
	isNeedNotice, noticeType, pageID, subject := d.getAppealNoticeByType(appealType)
	log.DebugContextf(ctx, "申诉中通知 isNeedNotice:%t, noticeType:%d, pageID:%d, subject:%s, audit:%+v, "+
		"appealType:%d", isNeedNotice, noticeType, pageID, subject, audit, appealType)
	if !isNeedNotice {
		return nil
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		operations := make([]model.Operation, 0)
		noticeOptions := []model.NoticeOption{
			model.WithPageID(pageID),
			model.WithLevel(model.LevelInfo),
			model.WithContent(subject),
			model.WithForbidCloseFlag(),
		}
		notice := model.NewNotice(noticeType, audit.ID, audit.CorpID, audit.RobotID, audit.CreateStaffID,
			noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := d.CreateNoticex(ctx, tx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "发送申诉中状态失败 err:%+v", err)
		return err
	}
	return nil
}
