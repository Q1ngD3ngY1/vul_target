package audit

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

// getBizAuditList 统计子审核数据
func (l *Logic) getBizAuditList(ctx context.Context, corpID, robotID, parentID uint64) ([]*releaseEntity.AuditStatusList,
	error) {
	/*
			`
			SELECT
				status, type, params
			FROM
			    t_audit
			WHERE
				corp_id = ?
				AND robot_id = ?
			    AND parent_id = ?
		`
	*/

	filter := &releaseEntity.AuditFilter{
		CorpID:   corpID,
		RobotID:  robotID,
		ParentID: ptrx.Uint64(parentID),
	}

	selectCoulums := []string{
		releaseEntity.AuditTblColStatus, releaseEntity.AuditTblColType, releaseEntity.AuditTblColParams,
	}

	audits, err := l.releaseDao.GetAuditList(ctx, selectCoulums, filter)
	if err != nil {
		logx.E(ctx, " failed to getBizAuditList by status err:%+v", err)
		return nil, err
	}

	list := make([]*releaseEntity.AuditStatusList, 0)

	for _, v := range audits {
		list = append(list, &releaseEntity.AuditStatusList{
			Status: v.Status,
			Type:   v.Type,
			Params: v.Params,
		})
	}
	return list, nil
}

func (l *Logic) getBizAudits(ctx context.Context, audit *releaseEntity.Audit) ([]*releaseEntity.AuditStatusSourceList, error) {
	sourceList := make([]*releaseEntity.AuditStatusSourceList, 0)
	auditStatusList, err := l.getBizAuditList(ctx, audit.CorpID, audit.RobotID, audit.ID)
	if err != nil {
		return nil, err
	}
	for _, v := range auditStatusList {
		auditItem := releaseEntity.AuditItem{}
		if err := jsonx.UnmarshalFromString(v.Params, &auditItem); err != nil {
			logx.E(ctx, "任务参数解析失败 v.Params:%s,err:%+v",
				v.Params, err)
			return nil, err
		}
		sourceList = append(sourceList, &releaseEntity.AuditStatusSourceList{
			Status:   v.Status,
			Source:   auditItem.Source,
			Avatar:   auditItem.HeadURL,
			Name:     auditItem.Nick,
			Greeting: auditItem.Greeting,
			Content:  auditItem.Content,
		})
	}
	return sourceList, nil
}

func (l *Logic) bizAuditStatusMap(ctx context.Context, audit *releaseEntity.Audit) (map[uint32][]*releaseEntity.AuditStatusSourceList,
	error) {
	auditStatusSourceMap := make(map[uint32][]*releaseEntity.AuditStatusSourceList)
	lists, err := l.getBizAudits(ctx, audit)
	if err != nil {
		return auditStatusSourceMap, err
	}
	for _, v := range lists {
		if _, ok := auditStatusSourceMap[v.Status]; !ok {
			auditStatusSourceMap[v.Status] = make([]*releaseEntity.AuditStatusSourceList, 0)
		}
		auditStatusSourceMap[v.Status] = append(auditStatusSourceMap[v.Status], v)
	}
	return auditStatusSourceMap, nil
}

func (l *Logic) getFileAuditParams(ctx context.Context, p entity.AuditSendParams) ([]*releaseEntity.AuditItem, error) {
	auditItems := make([]*releaseEntity.AuditItem, 0)
	doc, err := l.docLogic.GetDocByID(ctx, p.RelateID, p.RobotID)
	if err != nil {
		return auditItems, err
	}

	cosUrl, err := l.s3.GetPreSignedURL(ctx, doc.CosURL)
	if err != nil {
		return auditItems, err
	}
	auditItems = append(
		auditItems,
		releaseEntity.NewFileAuditItem(doc.ID, releaseEntity.AuditSourceDoc, cosUrl, p.EnvSet,
			l.s3.GetObjectETag(ctx, cosUrl)),
		// 2.7.0增加文档名称送审
		releaseEntity.NewPlainTextAuditItem(doc.ID, releaseEntity.AuditSourceDocName, doc.FileName, p.EnvSet),
	)
	return auditItems, nil
}

func (l *Logic) getReleaseAuditParams(ctx context.Context, p entity.AuditSendParams) ([]*releaseEntity.AuditItem, error) {
	auditItems := make([]*releaseEntity.AuditItem, 0)
	release, err := l.releaseDao.GetReleaseByID(ctx, p.RelateID)
	if err != nil {
		return auditItems, err
	}
	if release == nil {
		return auditItems, errs.ErrReleaseNotFound
	}
	qas, err := l.releaseDao.GetAuditQAByVersion(ctx, release.ID)
	if err != nil {
		return auditItems, err
	}
	for _, qa := range qas {
		content := fmt.Sprintf("%s\n%s", qa.Question, qa.Answer)
		auditItems = append(
			auditItems,
			releaseEntity.NewPlainTextAuditItem(qa.ID, releaseEntity.AuditSourceReleaseQA, content, p.EnvSet),
		)
		for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
			auditItems = append(
				auditItems,
				releaseEntity.NewPictureAuditItem(qa.ID, releaseEntity.AuditSourceReleaseQA, image, p.EnvSet, l.s3.GetObjectETag(ctx, image)),
			)
		}
	}
	return auditItems, nil
}

func (l *Logic) getRobotProfileAuditParams(ctx context.Context, p entity.AuditSendParams) ([]*releaseEntity.AuditItem, error) {
	auditItems := make([]*releaseEntity.AuditItem, 0)
	appDB, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, p.RelateID)
	if err != nil {
		return auditItems, err
	}
	if appDB == nil {
		return auditItems, errs.ErrRobotNotFound
	}

	if appDB.NameInAudit != "" {
		auditItems = append(
			auditItems,
			releaseEntity.NewUserDataAuditItem(appDB.PrimaryId, releaseEntity.AuditSourceRobotName, appDB.NameInAudit, p.EnvSet),
		)
	}

	if appDB.AvatarInAudit != "" {
		auditItems = append(
			auditItems,
			releaseEntity.NewUserHeadURLAuditItem(appDB.PrimaryId, releaseEntity.AuditSourceRobotAvatar, appDB.AvatarInAudit,
				p.EnvSet, l.s3.GetObjectETag(ctx, appDB.AvatarInAudit)),
		)
	}

	if appDB.GreetingInAudit != "" {
		auditItems = append(
			auditItems,
			releaseEntity.NewUserGreetingAuditItem(appDB.PrimaryId, releaseEntity.AuditSourceRobotGreeting, appDB.GreetingInAudit, p.EnvSet),
		)
	}

	if appDB.RoleDescriptionInAudit != "" {
		auditItems = append(
			auditItems,
			releaseEntity.NewPlainTextAuditItem(appDB.PrimaryId, releaseEntity.AuditSourceRobotRoleDescription,
				appDB.RoleDescriptionInAudit, p.EnvSet),
		)
	}

	return auditItems, nil
}

func (l *Logic) getDocTableSheetAuditParams(ctx context.Context, envSet string, parent *releaseEntity.Audit, originDocBizID uint64) (
	[]*releaseEntity.AuditItem, error) {
	logx.I(ctx, "getDocTableSheetAuditParams|start|parent:%+v", parent)

	auditItems := make([]*releaseEntity.AuditItem, 0)
	corpBizID, appBizID, _, _, err := l.segLogic.SegmentCommonIDsToBizIDs(ctx, parent.CorpID,
		parent.RobotID, 0, 0)
	if err != nil {
		return auditItems, err
	}
	pageNumber := 1
	pageSize := 100
	versionFlag := segEntity.SheetDefaultVersion
	for {
		offset, limit := utilx.Page(pageNumber, pageSize)
		filter := &segEntity.DocSegmentSheetTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  originDocBizID,
			IsDeleted: ptrx.Bool(false),
			Offset:    offset,
			Limit:     limit,
			Version:   &versionFlag,
		}
		sheetList, err := l.segDao.GetSheetList(ctx, segEntity.DocSegmentSheetTemporaryTblColList, filter)
		if err != nil {
			return auditItems, err
		}
		if len(sheetList) == 0 {
			break
		}
		for _, sheet := range sheetList {
			cosUrl, err := l.s3.GetPreSignedURL(ctx, sheet.CosURL)
			if err != nil {
				return auditItems, err
			}
			textItem := releaseEntity.NewFileAuditItem(parent.RelateID, releaseEntity.AuditSourceDoc, cosUrl, envSet,
				l.s3.GetObjectETag(ctx, cosUrl))
			textItem.SegmentBizID = strconv.FormatUint(sheet.BusinessID, 10)
			auditItems = append(auditItems, textItem)
		}
		pageNumber++
	}
	// todo 目前当auditItems为空时转为文件名送审
	if len(auditItems) == 0 {
		doc, err := l.docLogic.GetDocByBizID(ctx, originDocBizID, parent.RobotID)
		if err != nil {
			return auditItems, errs.ErrRobotOrDocNotFound
		}
		textItem := releaseEntity.NewPlainTextAuditItem(parent.RelateID, releaseEntity.AuditSourceDocName, doc.FileName, envSet)
		auditItems = append(auditItems, textItem)
	}
	logx.I(ctx, "getDocTableSheetAuditParams|len(auditItems):%s", len(auditItems))
	return auditItems, nil
}

func (l *Logic) getQaAuditParams(ctx context.Context, envSet string, parent *releaseEntity.Audit, uin string, appBizID uint64) (
	[]*releaseEntity.AuditItem, error) {
	logx.I(ctx, "getQaAuditParams|start|parent:%+v", parent)
	auditItems := make([]*releaseEntity.AuditItem, 0)
	qa, err := l.qaDao.GetQAByID(ctx, parent.RelateID)
	if err != nil {
		return auditItems, err
	}
	if qa == nil {
		return auditItems, errs.ErrQANotFound
	}

	content := fmt.Sprintf("%s\n%s\n%s", qa.Question, qa.Answer, qa.QuestionDesc)
	textItem := releaseEntity.NewPlainTextAuditItem(qa.ID, releaseEntity.AuditSourceQAText, content, envSet)
	textItem.QAFlag = releaseEntity.QAFlagMain
	auditItems = append(auditItems, textItem)
	for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
		if !config.IsInWhiteList(uin, appBizID, config.GetWhitelistConfig().QaURLWhiteList) {
			safe, err := util.IsSafeURL(ctx, image)
			if err != nil {
				logx.W(ctx, "getQaAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
				return auditItems, err
			}
			if !safe {
				logx.W(ctx, "getQaAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
				return auditItems, errs.ErrFileUrlFail
			}
		}
		// imageUrl :=  getRedirectedURL(image)
		imgItem := releaseEntity.NewPictureAuditItem(qa.ID, releaseEntity.AuditSourceQAAnswerPic, image, envSet, l.s3.GetObjectETag(ctx, image))
		imgItem.QAFlag = releaseEntity.QAFlagMain
		auditItems = append(auditItems, imgItem)
	}

	if config.VideoAuditSwitch() {
		videos, err := util.AuditQaVideoURLs(ctx, qa.Answer)
		if err != nil {
			return nil, err
		}
		if len(videos) > 0 {
			for k, video := range videos {
				objectInfo, err := l.qaLogic.GetCosFileInfoByUrl(ctx, video.CosURL)
				if err != nil {
					logx.W(ctx, "getQaAuditParams|GetCosFileInfoByUrl err:%v", err)
					return nil, err
				}
				videos[k].ETag = objectInfo.ETag
				videos[k].Size = objectInfo.Size
				videoAudit, err := l.GetAuditByEtag(ctx, parent.RobotID, parent.CorpID, qa.ID, objectInfo.ETag)
				if err != nil {
					return nil, err
				}
				if len(videoAudit) > 0 {
					logx.I(ctx, "getQaAuditParams|videoAudit|已审核通过的|video:%v qa:%+v", videos[k], qa)
					continue
				}
				auditItems = append(
					auditItems, releaseEntity.NewVideoAuditItem(qa.ID, releaseEntity.AuditSourceQAAnswerVideo, video.CosURL, envSet,
						video.ETag),
				)
			}
		}
	}
	sims, err := l.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		return nil, err
	}
	for _, v := range sims {
		textItem := releaseEntity.NewPlainTextAuditItem(qa.ID, releaseEntity.AuditSourceQAText, v.Question, envSet)
		textItem.QAFlag = releaseEntity.QAFlagSimilar
		auditItems = append(auditItems, textItem)
	}
	return auditItems, nil
}

func (l *Logic) getDocNameAuditParams(ctx context.Context, envSet string, parent *releaseEntity.Audit) (
	[]*releaseEntity.AuditItem, error) {
	auditItems := make([]*releaseEntity.AuditItem, 0)
	doc, err := l.docLogic.GetDocByID(ctx, parent.RelateID, parent.RobotID)
	if err != nil {
		return auditItems, err
	}
	if doc == nil {
		return auditItems, errs.ErrDocNotFound
	}
	textItem := releaseEntity.NewPlainTextAuditItem(doc.ID, releaseEntity.AuditSourceDocName, doc.FileNameInAudit, envSet)
	auditItems = append(auditItems, textItem)
	return auditItems, nil
}

func (l *Logic) newDocSegmentPictureAuditParams(ctx context.Context, envSet string, parent *releaseEntity.Audit,
	seg *segEntity.DocSegmentOrgDataTemporary) ([]*releaseEntity.AuditItem, error) {
	auditItems := make([]*releaseEntity.AuditItem, 0)
	for _, image := range util.ExtractImagesFromMarkdown(seg.OrgData) {
		safe, err := util.IsSafeURL(ctx, image)
		if err != nil {
			logx.W(ctx, "getDocSegmentAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
			return auditItems, err
		}
		if !safe {
			logx.W(ctx, "getDocSegmentAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
			return auditItems, errs.ErrFileUrlFail
		}
		// imageUrl :=  getRedirectedURL(image)
		imgItem := releaseEntity.NewPictureAuditItem(parent.RelateID, releaseEntity.AuditSourceDocSegmentPic, image, envSet, l.s3.GetObjectETag(ctx, image))
		imgItem.SegmentBizID = seg.BusinessID
		auditItems = append(auditItems, imgItem)
	}
	return auditItems, nil
}

func (l *Logic) getDocSegmentAuditParams(ctx context.Context, envSet string, parent *releaseEntity.Audit, originDocBizID uint64) (
	[]*releaseEntity.AuditItem, error) {
	logx.I(ctx, "getDocSegmentAuditParams|start|parent:%+v", parent)
	auditItems := make([]*releaseEntity.AuditItem, 0)
	corpBizID, appBizID, _, _, err := l.segLogic.SegmentCommonIDsToBizIDs(ctx, parent.CorpID,
		parent.RobotID, 0, 0)
	if err != nil {
		return auditItems, err
	}
	// 分页查询查询更新/新增切片
	pageNumber := 1
	pageSize := 100
	for {
		offset, limit := utilx.Page(pageNumber, pageSize)
		filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  originDocBizID,
			IsDeleted: ptrx.Bool(false),
			Offset:    offset,
			Limit:     limit,
		}
		segList, err := l.segDao.GetDocTemporaryOrgDataByDocBizID(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			return auditItems, err
		}
		if len(segList) == 0 {
			break
		}
		for _, seg := range segList {
			textItem := releaseEntity.NewPlainTextAuditItem(parent.RelateID, releaseEntity.AuditSourceDocSegment, seg.OrgData, envSet)
			textItem.SegmentBizID = seg.BusinessID
			auditItems = append(auditItems, textItem)
			// 图片送审
			imgItem, err := l.newDocSegmentPictureAuditParams(ctx, envSet, parent, seg)
			if err != nil {
				return auditItems, err
			}
			auditItems = append(auditItems, imgItem...)
		}
		pageNumber++
	}
	// todo 目前当auditItems为空时转为文件名送审
	if len(auditItems) == 0 {
		doc, err := l.docLogic.GetDocByBizID(ctx, originDocBizID, parent.RobotID)
		if err != nil {
			return auditItems, errs.ErrRobotOrDocNotFound
		}
		textItem := releaseEntity.NewPlainTextAuditItem(parent.RelateID, releaseEntity.AuditSourceDocSegment, doc.FileName, envSet)
		auditItems = append(auditItems, textItem)
	}
	logx.I(ctx, "getDocSegmentAuditParams|total|len(auditItems):%d", len(auditItems))
	return auditItems, nil
}

// getAuditStatus 获取审核状态
func (l *Logic) getAuditStatus(pass, isAppeal bool) uint32 {
	if pass {
		if isAppeal {
			return releaseEntity.AuditStatusAppealSuccess
		}
		return releaseEntity.AuditStatusPass
	} else {
		if isAppeal {
			return releaseEntity.AuditStatusAppealFail
		}
		return releaseEntity.AuditStatusFail
	}
}

func (l *Logic) updateRobotBizAuditPass(auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList, appDB *entity.App) {
	bizListPass := append(auditsMap[releaseEntity.AuditStatusPass], auditsMap[releaseEntity.AuditStatusAppealSuccess]...)
	for _, v := range bizListPass {
		switch v.Source {
		case releaseEntity.AuditSourceRobotName:
			if v.Name == "" {
				continue
			}
			appDB.Name = v.Name
			appDB.NameInAudit = ""
		case releaseEntity.AuditSourceRobotAvatar:
			if v.Avatar == "" {
				continue
			}
			appDB.Avatar = v.Avatar
			appDB.AvatarInAudit = ""
		case releaseEntity.AuditSourceRobotGreeting:
			if v.Greeting == "" {
				continue
			}
			appDB.Greeting = v.Greeting
			appDB.GreetingInAudit = ""
		case releaseEntity.AuditSourceRobotRoleDescription:
			if v.Content == "" {
				continue
			}
			appDB.RoleDescription = v.Content
			appDB.RoleDescriptionInAudit = ""
		}
	}
}

func (l *Logic) getBizAuditListFailStr(bizAuditListFail []*releaseEntity.AuditStatusSourceList,
	audit *releaseEntity.Audit) ([]string, error) {
	var bizAuditListFailStr []string
	for _, v := range bizAuditListFail {
		if auditItemSource := audit.GetSourceDesc(v.Source); auditItemSource != "" {
			bizAuditListFailStr = append(bizAuditListFailStr, auditItemSource)
		}
	}
	return bizAuditListFailStr, nil
}

func (l *Logic) updateRobotBizAuditFail(ctx context.Context, auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList,
	appDB *entity.App) {
	bizListFail := append(append(auditsMap[releaseEntity.AuditStatusFail], auditsMap[releaseEntity.AuditStatusTimeoutFail]...),
		auditsMap[releaseEntity.AuditStatusAppealFail]...)
	for _, v := range bizListFail {
		if releaseEntity.AuditSourceRobotName == v.Source {
			if appDB.Name == appDB.NameInAudit || appDB.Name == "" {
				appDB.Name = fmt.Sprintf("%s%d", config.App().RobotDefault.Name, rand.Intn(100))
			}
			appDB.NameInAudit = ""
		}
		if releaseEntity.AuditSourceRobotAvatar == v.Source {
			avatarURL, err := url.Parse(v.Avatar)
			if err != nil {
				logx.E(ctx, "审核失败 头像地址解析失败 err:%+v, Avatar:%s", err, v.Avatar)
			} else if v.Status == releaseEntity.AuditStatusAppealFail {
				if err = l.s3.DelObject(ctx, avatarURL.Path); err != nil {
					logx.E(ctx, "审核失败，删除头像失败 err:%+v, Avatar:%s", err, v.Avatar)
				}
			}
			if appDB.Avatar == v.Avatar || appDB.Avatar == "" {
				appDB.Avatar = config.App().RobotDefault.Avatar
			}
			appDB.AvatarInAudit = ""
		}
		if releaseEntity.AuditSourceRobotGreeting == v.Source {
			if appDB.Greeting == appDB.GreetingInAudit && appDB.Greeting != "" {
				appDB.Greeting = config.App().RobotDefault.Greeting
			}
			appDB.GreetingInAudit = ""
		}
		if releaseEntity.AuditSourceRobotRoleDescription == v.Source {
			if appDB.RoleDescription == appDB.RoleDescriptionInAudit && appDB.RoleDescription != "" {
				appDB.RoleDescription = config.App().RobotDefault.RoleDescription
			}
			appDB.RoleDescriptionInAudit = ""
		}
	}
}

func (l *Logic) getAuditBareAnswer(ctx context.Context, audit *releaseEntity.Audit, appInfo *entity.App,
	auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList) (
	string, string, string, string, uint32) {
	now := time.Now()
	audit.UpdateTime = now
	subject := i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewPassed)
	content := i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyChangeReviewedPassed)
	appInfo.BareAnswerInAudit = ""
	level := releaseEntity.LevelSuccess
	bareAnswer := appInfo.BareAnswer
	opType := releaseEntity.OpTypeViewDetail
	appInfo.UpdateTime = now
	if _, ok := auditsMap[releaseEntity.AuditStatusTimeoutFail]; ok {
		audit.Status = releaseEntity.AuditStatusTimeoutFail
		bareAnswer = appInfo.BareAnswer
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailure)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailureTimeout)
		level = releaseEntity.LevelError
		opType = releaseEntity.OpTypeAppeal
		appInfo.BareAnswerInAudit = ""
		return subject, content, level, bareAnswer, opType
	}
	if _, ok := auditsMap[releaseEntity.AuditStatusFail]; ok {
		audit.Status = releaseEntity.AuditStatusFail
		bareAnswer = appInfo.BareAnswer
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailure)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailureSensitive)
		level = releaseEntity.LevelError
		opType = releaseEntity.OpTypeAppeal
		appInfo.BareAnswerInAudit = ""
		return subject, content, level, bareAnswer, opType
	}
	if _, ok := auditsMap[releaseEntity.AuditStatusAppealFail]; ok {
		audit.Status = releaseEntity.AuditStatusAppealFail
		bareAnswer = appInfo.BareAnswer
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyManualAppealFailure)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyManualAppealFailureSensitiveInfo)
		level = releaseEntity.LevelError
		appInfo.BareAnswerInAudit = ""
		return subject, content, level, bareAnswer, opType
	}
	if _, ok := auditsMap[releaseEntity.AuditStatusAppealSuccess]; ok {
		audit.Status = releaseEntity.AuditStatusAppealSuccess
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyManualAppealSuccess)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyContentChangeManualAppealSuccess)
		level = releaseEntity.LevelSuccess
		appInfo.BareAnswerInAudit = ""
		for _, v := range auditsMap[releaseEntity.AuditStatusAppealSuccess] {
			if len(v.Content) != 0 {
				bareAnswer = v.Content
				return subject, content, level, bareAnswer, opType
			}
		}
	}
	if _, ok := auditsMap[releaseEntity.AuditStatusPass]; ok {
		audit.Status = releaseEntity.AuditStatusPass
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewPassed)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyChangeReviewedPassed)
		appInfo.BareAnswerInAudit = ""
		level = releaseEntity.LevelSuccess
		for _, v := range auditsMap[releaseEntity.AuditStatusPass] {
			if len(v.Content) != 0 {
				bareAnswer = v.Content
				return subject, content, level, bareAnswer, opType
			}
		}
	}
	return subject, content, level, bareAnswer, opType
}
