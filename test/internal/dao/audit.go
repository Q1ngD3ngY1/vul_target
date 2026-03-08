package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"

	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"

	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"go.opentelemetry.io/otel/trace"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/client"
	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/metadata"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec"
)

const (
	auditFields = `id,business_id,corp_id,robot_id,create_staff_id,parent_id,type,params,relate_id,status,
		retry_times,e_tag,message,update_time,create_time,parent_relate_id`
	createAudit = `
		INSERT INTO
			t_audit (%s)
		VALUES
		    (null,:business_id,:corp_id,:robot_id,:create_staff_id,:parent_id,:type,:params,:relate_id,:status,
			:retry_times,:e_tag,:message,:update_time,:create_time,:parent_relate_id)
	`
	getAuditByID = `
		SELECT
			%s
		FROM
		    t_audit
		WHERE
		    id = ?
	`
	getAuditByBizID = `
		SELECT
			%s
		FROM
		    t_audit
		WHERE
		    business_id = ?
	`
	getAuditByParentID = `
		SELECT
			%s
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_id = ?
		    AND type = ?
	`
	getChildAuditsByParentID = `
		SELECT
			%s
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_id = ?
	`
	getParentAuditsByParentRelateID = `
		SELECT
			%s
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_relate_id = ?
		    AND type = ?
			AND parent_id = 0
			AND id > ?
			ORDER BY id ASC
			LIMIT ?
	`
	testUpdateAuditStatusByParentID = `
		UPDATE
			t_audit
		SET
		    update_time = :update_time,
		    status = :status
		WHERE
		    corp_id = :corp_id
			AND robot_id = :robot_id
			AND parent_id = :id
			AND type = :type
	`
	updateAuditStatusByParentID = `
		UPDATE
			t_audit
		SET
		    update_time = :update_time,
		    status = :status,
			message = :message
		WHERE
		    corp_id = :corp_id
			AND robot_id = :robot_id
			AND parent_id = :id
			AND type = :type
			AND status = 3
		%s
	`
	updateAuditStatus = `
		UPDATE
			t_audit
		SET
		    update_time = :update_time,
		    retry_times = :retry_times,
		    status = :status,
		    message = :message
		WHERE
		    id = :id
	`
	updateAuditSendStatus = `
		UPDATE
			t_audit
		SET
		    update_time = :update_time,
		    retry_times = :retry_times,
		    status = :status,
		    message = :message
		WHERE
		    id = :id and  status = 2 
	`
	getBizAuditStatusStat = `
		SELECT
			status,count(*) total
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
		    AND parent_id = ?
		GROUP BY
		    status
	`
	getBizAuditList = `
		SELECT
			status, type, params
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
		    AND parent_id = ?
	`
	getBizAuditStatusByType = `
		SELECT
			status
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_id = 0 %s
		ORDER BY
			id DESC
		LIMIT
			1
	`
	getBizAuditStatusByTypes = `
		SELECT
		  type,status
		FROM
		  t_audit
		WHERE
		  corp_id = ?
		  AND robot_id = ?
		  AND parent_id = 0
		  %s
	`
	getBizAuditByTypes = `
		SELECT
			MAX(id) as id
		FROM
			t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_id = 0
			%s
			GROUP BY type
	`
	getBizAuditStatusByRelateIDs = `
		SELECT
			max(id) as id,type,relate_id,status
		FROM
			t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_id != 0
			AND type = ?
			%s
			GROUP BY
			    relate_id,status
	`
	getBizAuditParentIDFailList = `
		SELECT
			id, status
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
            AND parent_id = 0
		    AND type = ?

		ORDER BY
			id DESC
		LIMIT
			1
	`
	getBizAuditFailList = `
		SELECT
			id, type, params, parent_id, relate_id
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
            AND parent_id = ?
		    AND type = ?
			AND status IN ( ?, ? )
	`
	getBizAuditFailListByRelateIDs = `
		SELECT
			id, type, params, parent_id, relate_id
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ? %s
		    AND type = ? %s
	`
	sendAuditFail = `
		UPDATE
			t_audit
		SET
		    status = :status,
		    retry_times = :retry_times,
		    message = :message,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	getAuditByEtag = `
		SELECT
			%s
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND relate_id = ?
			AND e_tag = ?  and  status in (?,?)
	`
	getLatestAuditFailListByRelateID = `
		SELECT
			id,status
		FROM
			t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND relate_id = ?
			AND type = ?
			AND parent_id = 0
		ORDER BY
			id desc
		LIMIT 1
`
)

// CreateParentAuditCheck 创建审核回调check任务
func (d *dao) CreateParentAuditCheck(ctx context.Context, parent *model.Audit) error {
	return newAuditCheckTask(ctx, parent.RobotID, model.AuditCheckParams{
		AuditID:        parent.ID,
		ParentRelateID: 0,
	})
}

// CreateParentAuditCheckWithOriginDocBizID 创建审核回调check任务(干预使用)
func (d *dao) CreateParentAuditCheckWithOriginDocBizID(ctx context.Context, parent *model.Audit, originDocBizID uint64) error {
	originDoc, err := d.GetDocByBizID(ctx, originDocBizID, parent.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "CreateParentAuditCheckWithOriginDocBizID|GetDocByBizID|err:%+v", err)
		return errs.ErrDocNotFound
	}
	return newAuditCheckTask(ctx, parent.RobotID, model.AuditCheckParams{
		AuditID:        parent.ID,
		ParentRelateID: 0,
		OriginDocID:    originDoc.ID,
	})
}

// CreateParentAuditCheckForExcel2Qa 批量导入问答场景下，创建审核回调check任务
func (d *dao) CreateParentAuditCheckForExcel2Qa(ctx context.Context, p model.AuditSendParams) error {
	return newAuditCheckTask(ctx, p.RobotID, model.AuditCheckParams{
		AuditID:        0,
		CorpID:         p.CorpID,
		StaffID:        p.StaffID,
		RobotID:        p.RobotID,
		Type:           p.Type,
		ParentRelateID: p.ParentRelateID,
	})
}

// CreateAudit 创建单条送审
func (d *dao) CreateAudit(ctx context.Context, audit *model.Audit) error {
	now := time.Now()
	audit.BusinessID = d.GenerateSeqID()
	audit.UpdateTime = now
	audit.CreateTime = now
	querySQL := fmt.Sprintf(createAudit, auditFields)
	res, err := d.db.NamedExec(ctx, querySQL, audit)
	if err != nil {
		log.ErrorContextf(ctx, "新增审核数据失败 sql:%s args:%+v err:%+v", querySQL, audit, err)
		return err
	}
	id, _ := res.LastInsertId()
	audit.ID = uint64(id)
	return nil
}

// createAudit 创建单条送审
func (d *dao) createAudit(ctx context.Context, p model.AuditSendParams) error {
	if !config.AuditSwitch() {
		return nil
	}
	now := time.Now()
	audit := model.NewParentAudit(p.CorpID, p.RobotID, p.StaffID, p.RelateID, 0, p.Type)
	audit.BusinessID = d.GenerateSeqID()
	audit.UpdateTime = now
	audit.CreateTime = now

	// todo 对于干预任务，在创建前删除历史的审核任务（父任务及子任务）
	querySQL := fmt.Sprintf(createAudit, auditFields)
	db := knowClient.DBClient(ctx, auditTableName, p.RobotID, []client.Option{}...)
	res, err := db.NamedExec(ctx, querySQL, audit)
	if err != nil {
		log.ErrorContextf(ctx, "新增审核数据失败 sql:%s args:%+v err:%+v", querySQL, audit, err)
		return err
	}
	id, _ := res.LastInsertId()
	audit.ID = uint64(id)
	p.ParentAuditBizID = audit.BusinessID
	return newAuditSendTask(ctx, audit.RobotID, p)
}

// createAuditForExcel2Qa 创建批量送审
func (d *dao) createAuditForExcel2Qa(ctx context.Context, p model.AuditSendParams) error {
	log.DebugContextf(ctx, "创建批量送审 params:%+v", p)
	if !config.AuditSwitch() {
		return nil
	}
	qaids, err := d.GetQAIDsByOriginDocID(ctx, p.RobotID, p.ParentRelateID) // 通过来源文档id获取问答id列表
	if err != nil {
		log.ErrorContextf(ctx, "创建批量送审 params:%+v, err:%+v", p, err)
		return err
	}
	if len(qaids) == 0 {
		log.InfoContextf(ctx, "无需送审 params:%+v, len(qaids)=0", p)
		return nil
	}
	now := time.Now()
	var auditList []*model.Audit
	for _, qaid := range qaids {
		audit := model.NewParentAudit(p.CorpID, p.RobotID, p.StaffID, qaid, p.ParentRelateID, p.Type)
		audit.BusinessID = d.GenerateSeqID()
		audit.UpdateTime = now
		audit.CreateTime = now
		auditList = append(auditList, audit)
	}
	err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		length := len(auditList)
		pageSize := 200
		pages := int(math.Ceil(float64(length) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > length {
				end = length
			}
			tmpAudits := auditList[start:end]
			querySQL := fmt.Sprintf(createAudit, auditFields)
			if _, err := tx.NamedExecContext(ctx, querySQL, tmpAudits); err != nil {
				log.ErrorContextf(ctx, "新增审核数据失败 sql:%s args:%+v err:%+v", querySQL, tmpAudits, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.ParentAuditBizID = 0 // 批量导入问答时，params里面的父审核id无意义，使用的是ParentRelateID
	return newAuditSendTask(ctx, p.RobotID, p)
}

// BatchCreateAudit 批量创建审核数据
func (d *dao) BatchCreateAudit(ctx context.Context, parent *model.Audit, appDB *model.AppDB,
	p model.AuditSendParams) ([]*model.Audit, error) {
	if parent == nil {
		log.ErrorContextf(ctx, "批量创建审核数据 父审核不存在")
		return nil, errs.ErrAuditNotFound
	}
	isNeedNotice, noticeType, pageID, subject := d.getAuditNotice(p)
	audits, err := d.getAudits(ctx, parent, appDB, p)
	if err != nil {
		return nil, err
	}
	err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := d.sendNotice(ctx, isNeedNotice, tx, parent, noticeType, pageID, subject); err != nil {
			return err
		}
		length := len(audits)
		pageSize := 100
		pages := int(math.Ceil(float64(length) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > length {
				end = length
			}
			tmpAudits := audits[start:end]
			querySQL := fmt.Sprintf(createAudit, auditFields)
			if _, err := tx.NamedExecContext(ctx, querySQL, tmpAudits); err != nil {
				log.ErrorContextf(ctx, "新增审核数据失败 sql:%s args:%+v err:%+v", querySQL, tmpAudits, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return audits, nil
}

func (d *dao) getAudits(ctx context.Context, parent *model.Audit, appDB *model.AppDB,
	p model.AuditSendParams) ([]*model.Audit, error) {
	auditItems := make([]*model.AuditItem, 0)
	switch p.Type {
	case model.AuditBizTypeBareAnswer:
		auditItems = append(
			auditItems,
			model.NewPlainTextAuditItem(appDB.ID, model.AuditSourceBareAnswer, appDB.BareAnswerInAudit, p.EnvSet),
		)
		for _, image := range util.ExtractImagesFromMarkdown(appDB.BareAnswerInAudit) {
			auditItems = append(
				auditItems,
				model.NewPictureAuditItem(appDB.ID, model.AuditSourceBareAnswer, image, p.EnvSet,
					d.GetObjectETag(ctx, image)),
			)
		}
	case model.AuditBizTypeDoc:
		tmpParams, err := d.getFileAuditParams(ctx, p)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case model.AuditBizTypeRelease:
		tmpParams, err := d.getReleaseAuditParams(ctx, p)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case model.AuditBizTypeRobotProfile:
		tmpParams, err := d.getRobotProfileAuditParams(ctx, p)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case model.AuditBizTypeQa:
		corp, err := d.GetCorpByID(ctx, appDB.CorpID)
		if err != nil {
			return nil, err
		}
		tmpParams, err := d.getQaAuditParams(ctx, p.EnvSet, parent, corp.Uin, appDB.BusinessID)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case model.AuditBizTypeDocName:
		tmpParams, err := d.getDocNameAuditParams(ctx, p.EnvSet, parent)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case model.AuditBizTypeDocSegment:
		tmpParams, err := d.getDocSegmentAuditParams(ctx, p.EnvSet, parent, p.OriginDocBizID)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case model.AuditBizTypeDocTableSheet:
		tmpParams, err := d.getDocTableSheetAuditParams(ctx, p.EnvSet, parent, p.OriginDocBizID)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	default:
		return nil, fmt.Errorf("unknown audit biz type %d", p.Type)
	}
	audits := model.NewAudits(ctx, parent, auditItems)
	now := time.Now()
	for _, audit := range audits {
		audit.BusinessID = d.GenerateSeqID()
		audit.UpdateTime = now
		audit.CreateTime = now
	}
	return audits, nil
}

func (d *dao) getAuditNotice(p model.AuditSendParams) (bool, uint32, uint32, string) {
	var (
		isNeedNotice bool
		noticeType   uint32
		pageID       uint32
		subject      string
	)
	switch p.Type {
	case model.AuditBizTypeRobotProfile:
		isNeedNotice = true
		noticeType = model.NoticeTypeRobotBasicInfo
		pageID = model.NoticeRobotInfoPageID
		subject = "角色设置内容审核中。"
	case model.AuditBizTypeBareAnswer:
		isNeedNotice = true
		noticeType = model.NoticeTypeBareAnswer
		pageID = model.NoticeBareAnswerPageID
		subject = "未知问题回复语审核中。"
	}
	return isNeedNotice, noticeType, pageID, subject
}

func (d *dao) getRobotProfileAuditParams(ctx context.Context, p model.AuditSendParams) ([]*model.AuditItem, error) {
	auditItems := make([]*model.AuditItem, 0)
	appDB, err := d.GetAppByID(ctx, p.RelateID)
	if err != nil {
		return auditItems, err
	}
	if appDB == nil {
		return auditItems, errs.ErrRobotNotFound
	}

	if appDB.NameInAudit != "" {
		auditItems = append(
			auditItems,
			model.NewUserDataAuditItem(appDB.ID, model.AuditSourceRobotName, appDB.NameInAudit, p.EnvSet),
		)
	}

	if appDB.AvatarInAudit != "" {
		auditItems = append(
			auditItems,
			model.NewUserHeadURLAuditItem(appDB.ID, model.AuditSourceRobotAvatar, appDB.AvatarInAudit,
				p.EnvSet, d.GetObjectETag(ctx, appDB.AvatarInAudit)),
		)
	}

	if appDB.GreetingInAudit != "" {
		auditItems = append(
			auditItems,
			model.NewUserGreetingAuditItem(appDB.ID, model.AuditSourceRobotGreeting, appDB.GreetingInAudit, p.EnvSet),
		)
	}

	if appDB.RoleDescriptionInAudit != "" {
		auditItems = append(
			auditItems,
			model.NewPlainTextAuditItem(appDB.ID, model.AuditSourceRobotRoleDescription,
				appDB.RoleDescriptionInAudit, p.EnvSet),
		)
	}

	return auditItems, nil
}

func (d *dao) getFileAuditParams(ctx context.Context, p model.AuditSendParams) ([]*model.AuditItem, error) {
	auditItems := make([]*model.AuditItem, 0)
	doc, err := d.GetDocByID(ctx, p.RelateID, p.RobotID)
	if err != nil {
		return auditItems, err
	}
	cosUrl, err := d.GetPresignedURL(ctx, doc.CosURL)
	if err != nil {
		return auditItems, err
	}
	auditItems = append(
		auditItems,
		model.NewFileAuditItem(doc.ID, model.AuditSourceDoc, cosUrl, p.EnvSet, d.GetObjectETag(ctx, cosUrl)),
		// 2.7.0增加文档名称送审
		model.NewPlainTextAuditItem(doc.ID, model.AuditSourceDocName, doc.FileName, p.EnvSet),
	)
	return auditItems, nil
}

func (d *dao) getReleaseAuditParams(ctx context.Context, p model.AuditSendParams) ([]*model.AuditItem, error) {
	auditItems := make([]*model.AuditItem, 0)
	release, err := d.GetReleaseByID(ctx, p.RelateID)
	if err != nil {
		return auditItems, err
	}
	if release == nil {
		return auditItems, errs.ErrReleaseNotFound
	}
	qas, err := d.GetAuditQAByVersion(ctx, release.ID)
	if err != nil {
		return auditItems, err
	}
	for _, qa := range qas {
		content := fmt.Sprintf("%s\n%s", qa.Question, qa.Answer)
		auditItems = append(
			auditItems,
			model.NewPlainTextAuditItem(qa.ID, model.AuditSourceReleaseQA, content, p.EnvSet),
		)
		for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
			auditItems = append(
				auditItems,
				model.NewPictureAuditItem(qa.ID, model.AuditSourceReleaseQA, image, p.EnvSet,
					d.GetObjectETag(ctx, image)),
			)
		}
	}
	return auditItems, nil
}

func (d *dao) getQaAuditParams(ctx context.Context, envSet string, parent *model.Audit, uin string, appBizID uint64) (
	[]*model.AuditItem, error) {
	auditItems := make([]*model.AuditItem, 0)
	qa, err := d.GetQAByID(ctx, parent.RelateID)
	if err != nil {
		return auditItems, err
	}
	if qa == nil {
		return auditItems, errs.ErrQANotFound
	}

	content := fmt.Sprintf("%s\n%s\n%s", qa.Question, qa.Answer, qa.QuestionDesc)
	textItem := model.NewPlainTextAuditItem(qa.ID, model.AuditSourceQAText, content, envSet)
	textItem.QAFlag = model.QAFlagMain
	auditItems = append(auditItems, textItem)
	for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
		if !utilConfig.IsInWhiteList(uin, appBizID, utilConfig.GetWhitelistConfig().QaURLWhiteList) {
			safe, err := util.IsSafeURL(ctx, image)
			if err != nil {
				log.WarnContextf(ctx, "getQaAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
				return auditItems, err
			}
			if !safe {
				log.WarnContextf(ctx, "getQaAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
				return auditItems, errs.ErrFileUrlFail
			}
		}
		//imageUrl :=  getRedirectedURL(image)
		imgItem := model.NewPictureAuditItem(qa.ID, model.AuditSourceQAAnswerPic, image, envSet,
			d.GetObjectETag(ctx, image))
		imgItem.QAFlag = model.QAFlagMain
		auditItems = append(auditItems, imgItem)
	}
	if config.App().VideoAuditSwitch {
		var videos []*model.DocQAFile
		videos, err = util.AuditQaVideoURLs(ctx, qa.Answer)
		if err != nil {
			return nil, err
		}
		if len(videos) > 0 {
			for k, video := range videos {
				objectInfo, err := d.GetCosFileInfoByUrl(ctx, video.CosURL)
				if err != nil {
					log.WarnContextf(ctx, "getQaAuditParams|GetCosFileInfoByUrl err:%v", err)
					return nil, err
				}
				videos[k].ETag = objectInfo.ETag
				videos[k].Size = objectInfo.Size
				videoAudit, err := d.GetAuditByEtag(ctx, parent.RobotID, parent.CorpID, qa.ID, objectInfo.ETag)
				if err != nil {
					return nil, err
				}
				if len(videoAudit) > 0 {
					log.InfoContextf(ctx, "getQaAuditParams|videoAudit|已审核通过的|video:%v qa:%+v", videos[k], qa)
					continue
				}
				auditItems = append(
					auditItems, model.NewVideoAuditItem(qa.ID, model.AuditSourceQAAnswerVideo, video.CosURL, envSet,
						video.ETag),
				)
			}
		}
	}
	sims, err := d.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		return nil, err
	}
	for _, v := range sims {
		textItem := model.NewPlainTextAuditItem(qa.ID, model.AuditSourceQAText, v.Question, envSet)
		textItem.QAFlag = model.QAFlagSimilar
		auditItems = append(auditItems, textItem)
	}
	return auditItems, nil
}

func (d *dao) getDocNameAuditParams(ctx context.Context, envSet string, parent *model.Audit) (
	[]*model.AuditItem, error) {
	auditItems := make([]*model.AuditItem, 0)
	doc, err := d.GetDocByID(ctx, parent.RelateID, parent.RobotID)
	if err != nil {
		return auditItems, err
	}
	if doc == nil {
		return auditItems, errs.ErrDocNotFound
	}
	textItem := model.NewPlainTextAuditItem(doc.ID, model.AuditSourceDocName, doc.FileNameInAudit, envSet)
	auditItems = append(auditItems, textItem)
	return auditItems, nil
}

func (d *dao) getDocSegmentAuditParams(ctx context.Context, envSet string, parent *model.Audit, originDocBizID uint64) (
	[]*model.AuditItem, error) {
	log.InfoContextf(ctx, "getDocSegmentAuditParams|start|parent:%+v", parent)
	auditItems := make([]*model.AuditItem, 0)
	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, parent.CorpID,
		parent.RobotID, 0, 0)
	if err != nil {
		return auditItems, err
	}
	// 分页查询查询更新/新增切片
	pageNumber := uint32(1)
	pageSize := uint32(100)
	deletedFlag := IsNotDeleted
	for {
		filter := &DocSegmentOrgDataTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  originDocBizID,
			IsDeleted: &deletedFlag,
			Offset:    (pageNumber - 1) * pageSize,
			Limit:     pageSize,
		}
		segList, err := GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByDocBizID(ctx,
			DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			return auditItems, err
		}
		if len(segList) == 0 {
			break
		}
		for _, seg := range segList {
			textItem := model.NewPlainTextAuditItem(parent.RelateID, model.AuditSourceDocSegment, seg.OrgData, envSet)
			textItem.SegmentBizID = seg.BusinessID
			auditItems = append(auditItems, textItem)
			// 图片送审
			imgItem, err := d.newDocSegmentPictureAuditParams(ctx, envSet, parent, seg)
			if err != nil {
				return auditItems, err
			}
			auditItems = append(auditItems, imgItem...)
		}
		pageNumber++
	}
	// todo 目前当auditItems为空时转为文件名送审
	if len(auditItems) == 0 {
		doc, err := d.GetDocByBizID(ctx, originDocBizID, parent.RobotID)
		if err != nil {
			return auditItems, errs.ErrRobotOrDocNotFound
		}
		textItem := model.NewPlainTextAuditItem(parent.RelateID, model.AuditSourceDocSegment, doc.FileName, envSet)
		auditItems = append(auditItems, textItem)
	}
	log.InfoContextf(ctx, "getDocSegmentAuditParams|total|len(auditItems):%d", len(auditItems))
	return auditItems, nil
}

func (d *dao) newDocSegmentPictureAuditParams(ctx context.Context, envSet string, parent *model.Audit,
	seg *model.DocSegmentOrgDataTemporary) ([]*model.AuditItem, error) {
	auditItems := make([]*model.AuditItem, 0)
	for _, image := range util.ExtractImagesFromMarkdown(seg.OrgData) {
		safe, err := util.IsSafeURL(ctx, image)
		if err != nil {
			log.WarnContextf(ctx, "getDocSegmentAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
			return auditItems, err
		}
		if !safe {
			log.WarnContextf(ctx, "getDocSegmentAuditParams|imageUrl:%s|safe:%v|err:%v", image, safe, err)
			return auditItems, errs.ErrFileUrlFail
		}
		//imageUrl :=  getRedirectedURL(image)
		imgItem := model.NewPictureAuditItem(parent.RelateID, model.AuditSourceDocSegmentPic, image, envSet,
			d.GetObjectETag(ctx, image))
		imgItem.SegmentBizID = seg.BusinessID
		auditItems = append(auditItems, imgItem)
	}
	return auditItems, nil
}

func (d *dao) getDocTableSheetAuditParams(ctx context.Context, envSet string, parent *model.Audit, originDocBizID uint64) (
	[]*model.AuditItem, error) {
	log.InfoContextf(ctx, "getDocTableSheetAuditParams|start|parent:%+v", parent)

	auditItems := make([]*model.AuditItem, 0)
	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, parent.CorpID,
		parent.RobotID, 0, 0)
	if err != nil {
		return auditItems, err
	}
	pageNumber := uint32(1)
	pageSize := uint32(100)
	deletedFlag := IsNotDeleted
	versionFlag := model.SheetDefaultVersion
	for {
		filter := &DocSegmentSheetTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  originDocBizID,
			IsDeleted: &deletedFlag,
			Offset:    (pageNumber - 1) * pageSize,
			Limit:     pageSize,
			Version:   &versionFlag,
		}
		sheetList, err := GetDocSegmentSheetTemporaryDao().GetSheetList(ctx, DocSegmentSheetTemporaryTblColList, filter)
		if err != nil {
			return auditItems, err
		}
		if len(sheetList) == 0 {
			break
		}
		for _, sheet := range sheetList {
			cosUrl, err := d.GetPresignedURL(ctx, sheet.CosURL)
			if err != nil {
				return auditItems, err
			}
			textItem := model.NewFileAuditItem(parent.RelateID, model.AuditSourceDoc, cosUrl, envSet, d.GetObjectETag(ctx, cosUrl))
			textItem.SegmentBizID = strconv.FormatUint(sheet.BusinessID, 10)
			auditItems = append(auditItems, textItem)
		}
		pageNumber++
	}
	// todo 目前当auditItems为空时转为文件名送审
	if len(auditItems) == 0 {
		doc, err := d.GetDocByBizID(ctx, originDocBizID, parent.RobotID)
		if err != nil {
			return auditItems, errs.ErrRobotOrDocNotFound
		}
		textItem := model.NewPlainTextAuditItem(parent.RelateID, model.AuditSourceDocName, doc.FileName, envSet)
		auditItems = append(auditItems, textItem)
	}
	log.InfoContextf(ctx, "getDocTableSheetAuditParams|len(auditItems):%s", len(auditItems))
	return auditItems, nil
}

func (d *dao) sendNotice(
	ctx context.Context,
	isNeedNotice bool,
	tx *sqlx.Tx,
	audit *model.Audit,
	noticeType, pageID uint32,
	subject string,
) error {
	if !isNeedNotice {
		return nil
	}
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
}

// GetAuditByID 通过id获取审核数据
func (d *dao) GetAuditByID(ctx context.Context, id uint64) (*model.Audit, error) {
	querySQL := fmt.Sprintf(getAuditByID, auditFields)
	args := make([]any, 0, 1)
	args = append(args, id)
	audits := make([]*model.Audit, 0)
	if err := d.db.QueryToStructs(ctx, &audits, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过id获取审核数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}
	return audits[0], nil
}

// GetAuditByBizID 通过BizID获取审核数据
func (d *dao) GetAuditByBizID(ctx context.Context, bizID uint64) (*model.Audit, error) {
	querySQL := fmt.Sprintf(getAuditByBizID, auditFields)
	args := make([]any, 0, 1)
	args = append(args, bizID)
	audits := make([]*model.Audit, 0)
	if err := d.db.QueryToStructs(ctx, &audits, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过BizID获取审核数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}
	return audits[0], nil
}

// GetAuditByParentID 通过ID获取已存在的审核数据
func (d *dao) GetAuditByParentID(ctx context.Context, parentID uint64, p model.AuditSendParams) ([]*model.Audit,
	error) {
	querySQL := fmt.Sprintf(getAuditByParentID, auditFields)
	args := make([]any, 0, 4)
	args = append(args, p.CorpID, p.RobotID, parentID, p.Type)
	audits := make([]*model.Audit, 0)
	if err := d.db.QueryToStructs(ctx, &audits, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取已存在的审核数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}
	return audits, nil
}

// GetChildAuditsByParentID 通过parentID获取子审核信息
func (d *dao) GetChildAuditsByParentID(ctx context.Context, corpID, robotID, parentID uint64) ([]*model.Audit,
	error) {
	querySQL := fmt.Sprintf(getChildAuditsByParentID, auditFields)
	args := make([]any, 0, 4)
	args = append(args, corpID, robotID, parentID)
	audits := make([]*model.Audit, 0)
	if err := d.db.QueryToStructs(ctx, &audits, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取子审核信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}
	return audits, nil
}

// GetParentAuditsByParentRelateID 通过父关联ID获取已存在的父审核数据
func (d *dao) GetParentAuditsByParentRelateID(ctx context.Context, p model.AuditSendParams, idStart uint64,
	limit int) ([]*model.Audit, error) {
	querySQL := fmt.Sprintf(getParentAuditsByParentRelateID, auditFields)
	args := make([]any, 0, 6)
	args = append(args, p.CorpID, p.RobotID, p.ParentRelateID, p.Type, idStart, limit)
	audits := make([]*model.Audit, 0)
	if err := d.db.QueryToStructs(ctx, &audits, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过父关联ID获取已存在的父审核数据 args:%+v err:%+v sql:%s", args, err, querySQL)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}
	return audits, nil
}

// GetParentAuditIDsByParentRelateID 通过父关联ID获取已存在的父审核ID列表
func (d *dao) GetParentAuditIDsByParentRelateID(ctx context.Context, p model.AuditCheckParams, idStart uint64,
	limit int) ([]uint64, error) {
	querySQL := fmt.Sprintf(getParentAuditsByParentRelateID, "id")
	args := make([]any, 0, 6)
	args = append(args, p.CorpID, p.RobotID, p.ParentRelateID, p.Type, idStart, limit)
	ids := make([]uint64, 0)
	if err := d.db.Select(ctx, &ids, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过父关联ID获取已存在的父审核ID列表 args:%+v err:%+v sql:%s", args, err, querySQL)
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// GetBizAuditStatusStat 按status统计子审核数据
func (d *dao) GetBizAuditStatusStat(ctx context.Context, id, corpID, robotID uint64) (map[uint32]*model.AuditStatusStat,
	error) {
	querySQL := getBizAuditStatusStat
	args := make([]any, 0, 3)
	args = append(args, corpID, robotID, id)
	list := make([]*model.AuditStatusStat, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "按status统计子审核数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	stat := make(map[uint32]*model.AuditStatusStat, 0)
	for _, item := range list {
		stat[item.Status] = item
	}
	return stat, nil
}

// getBizAuditList 统计子审核数据
func (d *dao) getBizAuditList(ctx context.Context, corpID, robotID, parentID uint64) ([]*model.AuditStatusList,
	error) {
	args := make([]any, 0, 3)
	args = append(args, corpID, robotID, parentID)
	querySQL := getBizAuditList
	list := make([]*model.AuditStatusList, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "按status统计子审核数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetBizAuditStatusByType 根据 Type 查询审核状态
func (d *dao) GetBizAuditStatusByType(ctx context.Context, robotID, corpID uint64,
	auditType uint32) (model.AuditStatus, error) {
	args := []any{corpID, robotID}
	condition := ""
	if auditType != 0 {
		condition = " AND type = ?"
		args = append(args, auditType)
	}
	querySQL := fmt.Sprintf(getBizAuditStatusByType, condition)

	auditStatus := model.AuditStatus{}
	if err := d.db.QueryToStruct(ctx, &auditStatus, querySQL, args...); err != nil {
		if mysql.IsNoRowsError(err) {
			return auditStatus, nil
		}
		log.ErrorContextf(ctx, "根据 Type 获取最后一次审核的 ParentID 失败 sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return auditStatus, err
	}
	return auditStatus, nil
}

// GetBizAuditStatusByTypes 根据 Type 查询审核状态
func (d *dao) GetBizAuditStatusByTypes(ctx context.Context, robotID, corpID uint64,
	auditTypes []uint32) (map[uint32]model.AuditStatus, error) {
	if len(auditTypes) == 0 {
		return nil, errs.ErrAuditNotFound
	}
	condition := ""
	condition = fmt.Sprintf(" AND type IN ( %s )", placeholder(len(auditTypes)))
	args := []any{corpID, robotID}
	for _, v := range auditTypes {
		args = append(args, v)
	}
	querySQL := fmt.Sprintf(getBizAuditByTypes, condition)
	auditIDs := make([]uint64, 0)
	if err := d.db.Select(ctx, &auditIDs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据 Type 获取最后一次审核的 ParentID 失败 sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	if len(auditIDs) == 0 {
		return nil, nil
	}
	condition = condition + fmt.Sprintf(" AND id IN ( %s )", placeholder(len(auditIDs)))
	for _, v := range auditIDs {
		args = append(args, v)
	}
	querySQL = fmt.Sprintf(getBizAuditStatusByTypes, condition)
	auditStatus := make([]model.AuditTypeStatus, 0)
	if err := d.db.QueryToStructs(ctx, &auditStatus, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据 Type 获取最后一次审核的 ParentID 失败 sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	audits := make(map[uint32]model.AuditStatus)
	for _, v := range auditStatus {
		audits[v.Type] = model.AuditStatus{Status: v.Status}
	}
	return audits, nil
}

// GetBizAuditFailList 根据 Type 获取最新一次子审核数据
func (d *dao) GetBizAuditFailList(ctx context.Context, corpID, robotID uint64,
	auditType uint32) ([]*model.AuditFailList, error) {
	args := []any{corpID, robotID, auditType}
	ParentIDSQL := getBizAuditParentIDFailList // 获取父审核ID
	auditParent := model.AuditParent{}
	auditFailList := make([]*model.AuditFailList, 0)
	if err := d.db.QueryToStruct(ctx, &auditParent, ParentIDSQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据 Type 获取最后一次审核的 ParentID 失败 sql:%s args:%+v err:%+v", ParentIDSQL,
			args, err)
		return nil, err
	}

	args = []any{corpID, robotID, auditParent.ID, auditType, model.AuditStatusFail, model.AuditStatusTimeoutFail}
	querySQL := getBizAuditFailList // 根据父审核id获取子审核结果
	if err := d.db.QueryToStructs(ctx, &auditFailList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据父审核ID获取子审核结果 失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(auditFailList) == 0 {
		return nil, errs.ErrRobotNotFound
	}
	return auditFailList, nil
}

// GetLatestParentAuditFailByRelateID 根据 relateID 获取最后一次父审核数据
func (d *dao) GetLatestParentAuditFailByRelateID(ctx context.Context, corpID, robotID, releateID uint64,
	auditType uint32) (*model.AuditParent, error) {
	args := []any{corpID, robotID, releateID, auditType}
	auditParent := model.AuditParent{}
	querySQL := getLatestAuditFailListByRelateID
	if err := d.db.QueryToStruct(ctx, &auditParent, querySQL, args...); err != nil {
		log.InfoContextf(ctx, "根据 relateID 获取最后一次审核的 ParentID 失败 sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	return &auditParent, nil
}

// GetLatestAuditFailListByRelateID 根据 relateID 获取最后一次子审核数据
func (d *dao) GetLatestAuditFailListByRelateID(ctx context.Context, corpID, robotID, releateID uint64,
	auditType uint32, isAppeal bool) ([]*model.AuditFailList, error) {
	auditParent, err := d.GetLatestParentAuditFailByRelateID(ctx, corpID, robotID, releateID, auditType)
	if mysql.IsNoRowsError(err) {
		return nil, errs.ErrAppealNotFound
	}
	if err != nil {
		return nil, err
	}
	auditFailList := make([]*model.AuditFailList, 0)
	args := []any{corpID, robotID, auditParent.ID, auditType, model.AuditStatusFail, model.AuditStatusAppealFail}
	if isAppeal { // 审核失败、审核超时，才允许申诉
		args[5] = model.AuditStatusTimeoutFail // 把 AuditStatusAppealFail 改成 AuditStatusTimeoutFail
	}
	querySQL := getBizAuditFailList
	if err = d.db.QueryToStructs(ctx, &auditFailList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据父审核ID获取子审核结果 失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(auditFailList) == 0 {
		return nil, errs.ErrAppealNotFound
	}
	return auditFailList, nil
}

// getBizAuditFailListByRelateIDs 根据 relate_id 查询审核失败列表
func (d *dao) getBizAuditFailListByRelateIDs(ctx context.Context, corpID, robotID uint64, auditType uint32,
	releateIDs []uint64, includedAppealFail bool) ([]*model.AuditFailList, error) {
	args := []any{corpID, robotID}
	if len(releateIDs) == 0 {
		return nil, nil
	}
	condition := fmt.Sprintf(" AND relate_id  in ( %s )", placeholder(len(releateIDs)))
	for _, v := range releateIDs {
		args = append(args, v)
	}
	args = append(args, auditType, model.AuditStatusFail, model.AuditStatusTimeoutFail)
	querySQL := fmt.Sprintf(getBizAuditFailListByRelateIDs, condition, " AND status IN ( ?, ? )")
	if includedAppealFail {
		args = append(args, model.AuditStatusAppealFail)
		querySQL = fmt.Sprintf(getBizAuditFailListByRelateIDs, condition, " AND status IN ( ?, ?, ? )")
	}
	auditFailList := make([]*model.AuditFailList, 0)
	if err := d.db.QueryToStructs(ctx, &auditFailList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据 Type 获取最后一次审核的 ParentID 失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(auditFailList) == 0 {
		return nil, errs.ErrAppealNotFound
	}
	return auditFailList, nil
}

// GetBizAuditFailListByRelateIDs 根据 relate_id 查询审核失败列表，不包括人工申诉失败
func (d *dao) GetBizAuditFailListByRelateIDs(ctx context.Context, corpID, robotID uint64, auditType uint32,
	releateIDs []uint64) ([]*model.AuditFailList, error) {
	return d.getBizAuditFailListByRelateIDs(ctx, corpID, robotID, auditType,
		releateIDs, false)
}

// GetBizAuditFailListByRelateIDsIncludeAppealFail 根据 relate_id 查询审核失败列表，包括人工申诉失败
func (d *dao) GetBizAuditFailListByRelateIDsIncludeAppealFail(ctx context.Context, corpID, robotID uint64,
	auditType uint32, releateIDs []uint64) ([]*model.AuditFailList, error) {
	return d.getBizAuditFailListByRelateIDs(ctx, corpID, robotID, auditType,
		releateIDs, true)
}

// SendAudit 发送审核
func (d *dao) SendAudit(ctx context.Context, audit *model.Audit, appInfosecBizType string) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		robot, err := d.GetAppByID(ctx, audit.RobotID)
		if err != nil || robot == nil {
			return errs.ErrRobotNotFound
		}
		corpReq := &admin.GetCorpReq{
			Id: robot.CorpID,
		}
		corpRsp, err := d.adminApiCli.GetCorp(ctx, corpReq)
		if err != nil || corpRsp == nil {
			return errs.ErrCorpNotFound
		}
		audit.UpdateTime = time.Now()
		audit.RetryTimes = audit.RetryTimes + 1
		params := &model.AuditItem{}
		if err = jsoniter.UnmarshalFromString(audit.Params, params); err != nil {
			log.ErrorContextf(ctx, "审核参数解析失败 audit:%+v err:%+v", audit, err)
			return err
		}
		auditReq := &infosec.CheckReq{
			User: &infosec.CheckReq_User{
				AccountType: params.AccountType,
				Uin:         fmt.Sprintf("%d", robot.BusinessID),
				Nick:        params.Nick,
				HeadUrl:     params.HeadURL,
				Signature:   params.Greeting,
			},
			Id:       fmt.Sprintf("%d", audit.BusinessID),
			PostTime: time.Now().Unix(),
			Source:   params.Source,
			Type:     params.Typ,
			Url:      params.URL,
			Content:  params.Content,
			BizType:  utils.When(len(appInfosecBizType) == 0, corpRsp.GetInfosecBizType(), appInfosecBizType),
		}
		opts := make([]client.Option, 0)
		if params.EnvSet != "" {
			ctx = metadata.SetServerMetaData(ctx, metadata.EnvSet, params.EnvSet)
			opts = append(opts, client.WithCalleeMetadata(metadata.EnvSet, params.EnvSet))
		}
		if _, err = d.infosecCli.Check(ctx, auditReq, opts...); err != nil {
			audit.Message = err.Error()
			log.ErrorContextf(ctx, "请求送审失败 req:%+v err:%+v", auditReq, err)
			return err
		}
		audit.Status = model.AuditStatusSendSuccess
		if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
			log.ErrorContextf(ctx, "发送审核失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "SendAudit|db.Transactionx failed|err:%+v", err)
		if audit.IsMaxSendAuditRetryTimes() {
			audit.Status = model.AuditStatusFail
		}
		if _, err := d.db.NamedExec(ctx, sendAuditFail, audit); err != nil {
			log.ErrorContextf(ctx, "更新发送审核失败 sql:%s args:%+v err:%+v", sendAuditFail, audit, err)
			return err
		}
		return err
	}
	return nil
}

// SendAuditFail 更新发送审核失败
func (d *dao) SendAuditFail(ctx context.Context, audit *model.Audit) error {
	audit.Status = model.AuditStatusSendFail
	audit.UpdateTime = time.Now()
	if _, err := d.db.NamedExec(ctx, updateAuditStatus, audit); err != nil {
		log.ErrorContextf(ctx, "更新发送审核失败失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
		return err
	}
	return nil
}

// CreateReleaseAppealNotice 创建问答发布申诉通知
func (d *dao) CreateReleaseAppealNotice(ctx context.Context, numSuccess, numFail, numTotal uint32,
	audit *model.Audit) error {
	subject := i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewSuccess)
	content := i18n.Translate(ctx, i18nkey.KeyPublishOnlineContentManualReviewSuccess)
	level := model.LevelSuccess
	if numFail == numTotal || numSuccess == 0 {
		subject = i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewFailure)
		content = i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewFailureSensitiveInfo)
		level = model.LevelError
	} else if numFail > 0 {
		subject = i18n.Translate(ctx, i18nkey.KeyPublishContentManualReviewPartialSuccess)
		content = i18n.Translate(ctx, i18nkey.KeyPublishOnlineContentManualReviewSuccessFailureCountSensitiveInfo, numSuccess, numFail)
		level = model.LevelError
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
		noticeOptions := []model.NoticeOption{
			model.WithGlobalFlag(),
			model.WithPageID(model.NoticeWaitReleasePageID),
			model.WithLevel(level),
			model.WithSubject(subject),
			model.WithContent(content),
		}
		notice := model.NewNotice(model.NoticeTypeRelease, audit.ParentID, audit.CorpID, audit.RobotID,
			audit.CreateStaffID, noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := d.createNotice(ctx, tx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "审核机器人昵称失败 err:%+v", err)
		return err
	}

	return nil
}

// AuditRobotProfile 审核机器人资料
func (d *dao) AuditRobotProfile(ctx context.Context, audit *model.Audit) error {
	appDB, err := d.GetAppByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if appDB == nil {
		return errs.ErrRobotNotFound
	}
	now := time.Now()
	if err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		audit.Status = model.AuditStatusPass
		audit.UpdateTime = now
		subject := i18n.Translate(ctx, i18nkey.KeyRoleSettingsReviewSuccess)
		content := i18n.Translate(ctx, i18nkey.KeyRoleSettingsContentReviewPassed)
		opType := model.OpTypeViewDetail
		level := model.LevelSuccess
		appDB.UpdateTime = now
		auditsMap, err := d.bizAuditStatusMap(ctx, audit)
		if err != nil {
			return err
		}
		d.updateRobotBizAuditPass(auditsMap, appDB)
		d.updateRobotBizAuditFail(ctx, auditsMap, appDB)
		if _, ok := auditsMap[model.AuditStatusFail]; ok {
			bizAuditListFailStr, _ := d.getBizAuditListFailStr(auditsMap[model.AuditStatusFail], audit)
			if len(bizAuditListFailStr) > 0 {
				audit.Status = model.AuditStatusFail
				subject, opType, level = i18n.Translate(ctx, i18nkey.KeyRoleSettingsReviewFailure), model.OpTypeAppeal, model.LevelError
				content = i18n.Translate(ctx, i18nkey.KeyRoleSettingsReviewFailureSensitiveInfoWithParam, strings.Join(bizAuditListFailStr, "、"))
			}
		}
		if _, ok := auditsMap[model.AuditStatusTimeoutFail]; ok {
			audit.Status = model.AuditStatusTimeoutFail
			subject, opType, level = i18n.Translate(ctx, i18nkey.KeyRoleSettingsReviewFailure), model.OpTypeAppeal, model.LevelError
			if len(auditsMap[model.AuditStatusFail]) == 0 {
				content = i18n.Translate(ctx, i18nkey.KeyRoleSettingsReviewFailureReason)
			}
			var auditFailTimeoutList []string
			for _, v := range auditsMap[model.AuditStatusTimeoutFail] {
				if v.Source == model.AuditSourceRobotAvatar {
					content += i18n.Translate(ctx, i18nkey.KeyAvatarTooLargeOrMissingPleaseCheck)
				} else if v.Source == model.AuditSourceRobotName || v.Source == model.AuditSourceRobotGreeting {
					auditFailTimeoutList = append(auditFailTimeoutList, audit.GetSourceDesc(v.Source))
				}
			}
			if len(auditFailTimeoutList) > 0 {
				content += i18n.Translate(ctx, i18nkey.KeyReviewTimeoutWithParam, strings.Join(auditFailTimeoutList, "/"))
			}
		}
		if _, ok := auditsMap[model.AuditStatusAppealFail]; ok {
			bizAppealListFailStr, _ := d.getBizAuditListFailStr(auditsMap[model.AuditStatusAppealFail], audit)
			if len(bizAppealListFailStr) > 0 {
				audit.Status = model.AuditStatusAppealFail
				subject, level = i18n.Translate(ctx, i18nkey.KeyRoleSettingsContentManualAppealFailure), model.LevelError
				content = i18n.Translate(ctx, i18nkey.KeyRoleSettingsContentManualAppealFailureSensitiveInfoWithParam, strings.Join(bizAppealListFailStr, "、"))
			}
		} else if _, ok := auditsMap[model.AuditStatusAppealSuccess]; ok {
			subject = i18n.Translate(ctx, i18nkey.KeyRoleSettingsContentManualAppealSuccess)
			content = i18n.Translate(ctx, i18nkey.KeyRoleSettingsContentManualAppealSuccess)
		}
		if _, err = tx.NamedExecContext(ctx, updateAuditName, appDB); err != nil {
			log.ErrorContextf(ctx, "更新机器人昵称失败 robot:%+v err:%+v", appDB, err)
			return err
		}
		if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
			return err
		}
		err = d.createRobotProfileNotice(ctx, tx, level, subject, content, audit, opType)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "审核机器人昵称失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) updateRobotBizAuditPass(auditsMap map[uint32][]*model.AuditStatusSourceList, appDB *model.AppDB) {
	bizListPass := append(auditsMap[model.AuditStatusPass], auditsMap[model.AuditStatusAppealSuccess]...)
	for _, v := range bizListPass {
		switch v.Source {
		case model.AuditSourceRobotName:
			if v.Name == "" {
				continue
			}
			appDB.Name = v.Name
			appDB.NameInAudit = ""
		case model.AuditSourceRobotAvatar:
			if v.Avatar == "" {
				continue
			}
			appDB.Avatar = v.Avatar
			appDB.AvatarInAudit = ""
		case model.AuditSourceRobotGreeting:
			if v.Greeting == "" {
				continue
			}
			appDB.Greeting = v.Greeting
			appDB.GreetingInAudit = ""
		case model.AuditSourceRobotRoleDescription:
			if v.Content == "" {
				continue
			}
			appDB.RoleDescription = v.Content
			appDB.RoleDescriptionInAudit = ""
		}
	}
}

func (d *dao) getBizAuditListFailStr(bizAuditListFail []*model.AuditStatusSourceList,
	audit *model.Audit) ([]string, error) {
	var bizAuditListFailStr []string
	for _, v := range bizAuditListFail {
		if auditItemSource := audit.GetSourceDesc(v.Source); auditItemSource != "" {
			bizAuditListFailStr = append(bizAuditListFailStr, auditItemSource)
		}
	}
	return bizAuditListFailStr, nil
}

func (d *dao) updateRobotBizAuditFail(ctx context.Context, auditsMap map[uint32][]*model.AuditStatusSourceList,
	appDB *model.AppDB) {
	bizListFail := append(append(auditsMap[model.AuditStatusFail], auditsMap[model.AuditStatusTimeoutFail]...),
		auditsMap[model.AuditStatusAppealFail]...)
	for _, v := range bizListFail {
		if model.AuditSourceRobotName == v.Source {
			if appDB.Name == appDB.NameInAudit || appDB.Name == "" {
				appDB.Name = fmt.Sprintf("%s%d", config.App().RobotDefault.Name, rand.Intn(100))
			}
			appDB.NameInAudit = ""
		}
		if model.AuditSourceRobotAvatar == v.Source {
			avatarURL, err := url.Parse(v.Avatar)
			if err != nil {
				log.ErrorContextf(ctx, "审核失败 头像地址解析失败 err:%+v, Avatar:%s", err, v.Avatar)
			} else if v.Status == model.AuditStatusAppealFail {
				if err = d.DelObject(ctx, avatarURL.Path); err != nil {
					log.ErrorContextf(ctx, "审核失败，删除头像失败 err:%+v, Avatar:%s", err, v.Avatar)
				}
			}
			if appDB.Avatar == v.Avatar || appDB.Avatar == "" {
				appDB.Avatar = config.App().RobotDefault.Avatar
			}
			appDB.AvatarInAudit = ""
		}
		if model.AuditSourceRobotGreeting == v.Source {
			if appDB.Greeting == appDB.GreetingInAudit && appDB.Greeting != "" {
				appDB.Greeting = config.App().RobotDefault.Greeting
			}
			appDB.GreetingInAudit = ""
		}
		if model.AuditSourceRobotRoleDescription == v.Source {
			if appDB.RoleDescription == appDB.RoleDescriptionInAudit && appDB.RoleDescription != "" {
				appDB.RoleDescription = config.App().RobotDefault.RoleDescription
			}
			appDB.RoleDescriptionInAudit = ""
		}
	}
}

func (d *dao) bizAuditStatusMap(ctx context.Context, audit *model.Audit) (map[uint32][]*model.AuditStatusSourceList,
	error) {
	auditStatusSourceMap := make(map[uint32][]*model.AuditStatusSourceList)
	lists, err := d.getBizAudits(ctx, audit)
	if err != nil {
		return auditStatusSourceMap, err
	}
	for _, v := range lists {
		if _, ok := auditStatusSourceMap[v.Status]; !ok {
			auditStatusSourceMap[v.Status] = make([]*model.AuditStatusSourceList, 0)
		}
		auditStatusSourceMap[v.Status] = append(auditStatusSourceMap[v.Status], v)
	}
	return auditStatusSourceMap, nil
}

func (d *dao) createRobotProfileNotice(ctx context.Context, tx *sqlx.Tx, level string, subject string, content string,
	audit *model.Audit, opType uint32) error {
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
	if opType == model.OpTypeAppeal {
		operations = append(operations, model.Operation{
			Typ:    opType,
			Params: model.OpParams{AppealType: model.AppealBizTypeRobotProfile},
		})
	}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeRobotInfoPageID),
		model.WithLevel(level),
		model.WithSubject(subject),
		model.WithContent(content),
	}
	notice := model.NewNotice(model.NoticeTypeRobotBasicInfo, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

func (d *dao) getBizAudits(ctx context.Context, audit *model.Audit) ([]*model.AuditStatusSourceList, error) {
	sourceList := make([]*model.AuditStatusSourceList, 0)
	auditStatusList, err := d.getBizAuditList(ctx, audit.CorpID, audit.RobotID, audit.ID)
	if err != nil {
		return nil, err
	}
	for _, v := range auditStatusList {
		auditItem := model.AuditItem{}
		if err := jsoniter.UnmarshalFromString(v.Params, &auditItem); err != nil {
			log.ErrorContextf(ctx, "任务参数解析失败 v.Params:%s,err:%+v",
				v.Params, err)
			return nil, err
		}
		sourceList = append(sourceList, &model.AuditStatusSourceList{
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

// AuditBareAnswer 审核BareAnswer
func (d *dao) AuditBareAnswer(ctx context.Context, audit *model.Audit) error {
	appDB, err := d.GetAppByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if appDB == nil {
		return errs.ErrRobotNotFound
	}
	auditsMap, err := d.bizAuditStatusMap(ctx, audit)
	if err != nil || len(auditsMap) == 0 {
		return errs.ErrAuditNotFound
	}
	if err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		subject, content, level, bareAnswer, opType := d.getAuditBareAnswer(ctx, audit, appDB, auditsMap)
		appDB.BareAnswer = bareAnswer
		if _, err = tx.NamedExecContext(ctx, updateBareAnswer, appDB); err != nil {
			log.ErrorContextf(ctx, "更新未知问题回复语审核结果失败 robot:%+v err:%+v", appDB, err)
			return err
		}
		if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
			return err
		}
		operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
		if opType == model.OpTypeAppeal {
			operations = append(operations, model.Operation{
				Typ:    opType,
				Params: model.OpParams{AppealType: model.AuditBizTypeBareAnswer},
			})
		}
		noticeOptions := []model.NoticeOption{
			model.WithGlobalFlag(),
			model.WithPageID(model.NoticeBareAnswerPageID),
			model.WithLevel(level),
			model.WithSubject(subject),
			model.WithContent(content),
		}
		notice := model.NewNotice(model.NoticeTypeBareAnswer, audit.ID, audit.CorpID, audit.RobotID,
			audit.CreateStaffID, noticeOptions...)
		if err = notice.SetOperation(operations); err != nil {
			log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err = d.createNotice(ctx, tx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "审核机器人未知问题回复失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) getAuditBareAnswer(ctx context.Context, audit *model.Audit, appDB *model.AppDB,
	auditsMap map[uint32][]*model.AuditStatusSourceList) (
	string, string, string, string, uint32) {
	now := time.Now()
	audit.UpdateTime = now
	subject := i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewPassed)
	content := i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyChangeReviewedPassed)
	appDB.BareAnswerInAudit = ""
	level := model.LevelSuccess
	bareAnswer := appDB.BareAnswer
	opType := model.OpTypeViewDetail
	appDB.UpdateTime = now
	if _, ok := auditsMap[model.AuditStatusTimeoutFail]; ok {
		audit.Status = model.AuditStatusTimeoutFail
		bareAnswer = appDB.BareAnswer
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailure)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailureTimeout)
		level = model.LevelError
		opType = model.OpTypeAppeal
		appDB.BareAnswerInAudit = ""
		return subject, content, level, bareAnswer, opType
	}
	if _, ok := auditsMap[model.AuditStatusFail]; ok {
		audit.Status = model.AuditStatusFail
		bareAnswer = appDB.BareAnswer
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailure)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewFailureSensitive)
		level = model.LevelError
		opType = model.OpTypeAppeal
		appDB.BareAnswerInAudit = ""
		return subject, content, level, bareAnswer, opType
	}
	if _, ok := auditsMap[model.AuditStatusAppealFail]; ok {
		audit.Status = model.AuditStatusAppealFail
		bareAnswer = appDB.BareAnswer
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyManualAppealFailure)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyManualAppealFailureSensitiveInfo)
		level = model.LevelError
		appDB.BareAnswerInAudit = ""
		return subject, content, level, bareAnswer, opType
	}
	if _, ok := auditsMap[model.AuditStatusAppealSuccess]; ok {
		audit.Status = model.AuditStatusAppealSuccess
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyManualAppealSuccess)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyContentChangeManualAppealSuccess)
		level = model.LevelSuccess
		appDB.BareAnswerInAudit = ""
		for _, v := range auditsMap[model.AuditStatusAppealSuccess] {
			if len(v.Content) != 0 {
				bareAnswer = v.Content
				return subject, content, level, bareAnswer, opType
			}
		}
	}
	if _, ok := auditsMap[model.AuditStatusPass]; ok {
		audit.Status = model.AuditStatusPass
		subject = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyReviewPassed)
		content = i18n.Translate(ctx, i18nkey.KeyUnknownIssueReplyChangeReviewedPassed)
		appDB.BareAnswerInAudit = ""
		level = model.LevelSuccess
		for _, v := range auditsMap[model.AuditStatusPass] {
			if len(v.Content) != 0 {
				bareAnswer = v.Content
				return subject, content, level, bareAnswer, opType
			}
		}
	}
	return subject, content, level, bareAnswer, opType
}

// AuditRelease 审核发布
func (d *dao) AuditRelease(ctx context.Context, audit *model.Audit, pass bool) error {
	release, err := d.GetReleaseByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if release == nil {
		return errs.ErrReleaseNotFound
	}
	auditStat, err := d.GetReleaseQAAuditStat(ctx, release.ID)
	if err != nil {
		return err
	}
	cfgAuditStat, err := d.getReleaseConfigAuditStat(ctx, release.ID)
	if err != nil {
		return err
	}
	robot, err := d.GetAppByID(ctx, release.RobotID)
	if err != nil {
		return err
	}
	if robot == nil {
		return errs.ErrRobotNotFound
	}
	now := time.Now()
	if err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		audit.Status = model.AuditStatusPass
		audit.UpdateTime = now
		if !pass {
			audit.Status = model.AuditStatusFail
		}
		if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
			return err
		}
		return d.DoReleaseAfterAudit(ctx, tx, release, auditStat, cfgAuditStat, robot)
	}); err != nil {
		log.ErrorContextf(ctx, "审核发布失败 err:%+v", err)
		return err
	}
	return nil
}

// TestUpdateAuditStatusByParentID 根据父审核id更新子审核状态，仅用于测试
func (d *dao) TestUpdateAuditStatusByParentID(ctx context.Context, parentAudit *model.Audit) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, testUpdateAuditStatusByParentID, parentAudit); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s parentAudit:%+v err:%+v",
				testUpdateAuditStatusByParentID, parentAudit, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新审核状态失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateAuditStatusByParentID 根据父审核id更新子审核状态
func (d *dao) UpdateAuditStatusByParentID(ctx context.Context, parentAudit *model.Audit, limit int64) error {
	if parentAudit == nil {
		return nil
	}
	for {
		withLimit := ""
		if limit > 0 {
			withLimit = fmt.Sprintf(" LIMIT %d", limit)
		}
		query := fmt.Sprintf(updateAuditStatusByParentID, withLimit)
		log.InfoContextf(ctx, "根据父审核id更新子审核状态  parentAudit:%+v sql:%s",
			parentAudit, query)
		if result, err := d.db.NamedExec(ctx, query, parentAudit); err != nil {
			log.ErrorContextf(ctx, "根据父审核id更新子审核状态失败  parentAudit:%+v err:%+v sql:%s",
				parentAudit, err, query)
			return err
		} else if limit > 0 {
			affected, err := result.RowsAffected()
			if err != nil {
				log.ErrorContextf(ctx, "获取影响的行数失败  parentAudit:%+v err:%+v sql:%s",
					parentAudit, err, query)
				return err
			}
			if affected < limit {
				break
			}
		}
	}

	return nil
}

// UpdateAuditStatus 更新审核状态
func (d *dao) UpdateAuditStatus(ctx context.Context, audit *model.Audit) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s audits:%+v err:%+v", updateAuditStatus, audit, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新审核状态失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateAuditListStatus 更新审核单状态
func (d *dao) UpdateAuditListStatus(ctx context.Context, auditList []*model.Audit) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, audit := range auditList {
			if _, err := tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
				log.ErrorContextf(ctx, "更新审核状态失败 sql:%s audits:%+v err:%+v", updateAuditStatus, audit, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "提交审核失败 err:%+v", err)
		return err
	}
	return nil
}

// auditReleaseQA 审核发布QA
func (d *dao) auditReleaseQA(ctx context.Context, tx *sqlx.Tx, audit *model.Audit, pass bool) error {
	qa, err := d.GetReleaseQAByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if qa == nil {
		log.DebugContextf(ctx, "try to get releaseQA from audit but not found,id:%+v", audit.RelateID)
		return nil
	}
	// 更新t_release_qa
	now := time.Now()
	args := make([]any, 0, 10)
	querySQL := releaseQAAuditPass
	// 审核通过不通过t_doc_qa发布状态为审核不通过，审核通过继续修改为发布中
	var docQA *model.DocQA
	docQA, err = d.GetQAByID(ctx, qa.QAID)
	if err != nil {
		return err
	}
	if pass {
		args = append(args, qa.ReleaseStatus, model.ReleaseQAAuditStatusSuccess, i18nkey.KeyAuditPass, "-", model.AllowRelease,
			now, qa.ID, model.ReleaseQAAuditStatusDoing, model.ReleaseQAAuditStatusSuccess,
		)
		docQA.ReleaseStatus = model.QAReleaseStatusIng
	} else {
		querySQL = releaseQAAuditNotPass
		args = append(args, model.QAReleaseStatusFail, model.ReleaseQAAuditStatusFail, "审核未通过",
			utils.When(audit.Status == model.AuditStatusTimeoutFail, "审核超时", "包含敏感词"),
			model.ForbidRelease, now, qa.ID,
		)
		docQA.ReleaseStatus = model.QAReleaseStatusAuditNotPass
	}
	// 更新ReleaseQA
	if _, err = tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新发布QA审核结果失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	// 更新DocQA
	docQA.UpdateTime = time.Now()
	if _, err = tx.NamedExecContext(ctx, updateAfterAuditQA, docQA); err != nil {
		log.ErrorContextf(ctx, "更新审核结果到QA文档中失败 sql:%s args:%+v err:%+v", updateAfterAuditQA, args, err)
		return err
	}
	return nil
}

// auditReleaseConfig 审核发布QA
func (d *dao) auditReleaseConfig(ctx context.Context, tx *sqlx.Tx, audit *model.Audit, pass bool) error {
	cfg, err := d.GetReleaseConfigItemByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if cfg == nil {
		log.DebugContextf(ctx, "try to get release config item but not found,id:%+v", audit.RelateID)
		return nil
	}
	now := time.Now()
	args := make([]any, 0, 10)
	querySQL := releaseConfigAuditPass
	if pass {
		args = append(args, cfg.ReleaseStatus, model.ReleaseConfigAuditStatusSuccess, i18nkey.KeyAuditPass, "-",
			now, cfg.ID, model.ConfigReleaseStatusAuditing, model.ReleaseQAAuditStatusSuccess,
		)
	} else {
		querySQL = releaseConfigAuditNotPass
		args = append(args, model.ConfigReleaseStatusFail, model.ConfigReleaseStatusAuditNotPass, "审核未通过",
			utils.When(audit.Status == model.AuditStatusTimeoutFail, "审核超时", "包含敏感词"),
			now, cfg.ID,
		)
	}
	// 更新t_release_config
	if _, err = tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新发布QA审核结果失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// auditRelease 审核发布
func (d *dao) auditRelease(ctx context.Context, tx *sqlx.Tx, audit *model.Audit, pass bool) error {
	appDB, err := d.GetAppByID(ctx, audit.RobotID)
	if err != nil {
		return err
	}
	if appDB == nil {
		return errs.ErrRobotNotFound
	}
	switch appDB.AppType {
	case model.KnowledgeQaAppType:
		err = d.auditReleaseQA(ctx, tx, audit, pass)
		err = d.auditReleaseConfig(ctx, tx, audit, pass)
	case model.ClassifyAppType:
		err = d.auditReleaseConfig(ctx, tx, audit, pass)
	case model.SummaryAppType:
		err = d.auditReleaseConfig(ctx, tx, audit, pass)
	default:
		return errs.ErrGetAppFail
	}
	if err != nil {
		return err
	}
	return nil
}

// AuditDoc 文档审核或者申诉回调处理函数，audit是父审核任务
func (d *dao) AuditDoc(ctx context.Context, audit *model.Audit, pass, isAppeal bool, rejectReason string) error {
	docID := audit.RelateID
	intervene := false
	if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
		intervene = true
	}
	doc, err := d.GetDocByID(ctx, docID, audit.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		audit.UpdateTime = time.Now()
		audit.Status = d.getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = d.UpdateAuditStatus(ctx, audit)
		log.InfoContextf(ctx, "文档已经被删除，不再走审核逻辑，doc:%+v", doc)
		return nil
	}
	if !isAppeal && !doc.NeedAudit() {
		return nil
	}
	auditsMap, err := d.bizAuditStatusMap(ctx, audit)
	if err != nil || len(auditsMap) == 0 {
		return errs.ErrAuditNotFound
	}
	isNeedCharSizeNotice := false
	if err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		doc, audit, err = d.updateDocAuditResult(ctx, tx, doc, audit, auditsMap, pass, isAppeal)
		if err != nil {
			return err
		}
		if !pass {
			return d.sendNoticeAndUpdateAuditStatusIfDocAuditFail(ctx, tx, doc, audit, auditsMap, isAppeal,
				rejectReason)
		}
		// 下面都是审核通过的处理流程
		if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
			return err
		}
		if err = d.IsUsedCharSizeExceeded(ctx, doc.CorpID, doc.RobotID); err == nil { // 没超过上限,进入学习
			if isAppeal {
				_ = d.SendNoticeIfDocAppealPass(ctx, tx, doc, audit) // 人工申诉成功，但是发通知失败，不报错
			}
			if _, err = tx.NamedExecContext(ctx, updateDocAuditResult, doc); err != nil {
				log.ErrorContextf(ctx, "更新文档审核结果失败 sql:%s args:%+v err:%+v", updateDocAuditResult, doc, err)
				return err
			}
			if err = d.DocParseSegment(ctx, tx, doc, intervene); err != nil {
				return err
			}
		} else { // 超过上限不进入学习，返回错误
			isNeedCharSizeNotice = true
			doc.Status = model.DocStatusParseImportFail
			doc.Message = terrs.Msg(d.ConvertErrMsg(ctx, 0, doc.CorpID, errs.ErrDocParseCharSizeExceeded))
			if _, err = tx.NamedExecContext(ctx, updateDocAuditResult, doc); err != nil {
				log.ErrorContextf(ctx, "更新文档审核结果失败 sql:%s args:%+v err:%+v", updateDocAuditResult, doc, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "审核文档失败 err:%+v", err)
		return err
	}
	if isNeedCharSizeNotice {
		var docParses model.DocParse
		if docParses, err = d.GetDocParseByDocID(ctx, doc.ID, doc.RobotID); err != nil {
			log.ErrorContextf(ctx, "查询 文档解析任务失败 args:%+v err:%+v", doc, err)
			return err
		}
		docParses.Status = model.DocParseCallBackCharSizeExceeded
		err = d.UpdateDocParseTask(ctx, docParses) // 更新解析字符状态,重试的时候不会重新解析
		if err != nil {
			return errs.ErrUpdateDocParseTaskStatusFail
		}
		if err = d.FailCharSizeNotice(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

// NoNeedAuditDoc 无需审核文档，直接发起解析片段任务
func (d *dao) NoNeedAuditDoc(ctx context.Context, doc *model.Doc) error {
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.NeedAudit() {
		return nil
	}
	go func(rCtx context.Context) {
		if err := d.db.Transactionx(rCtx, func(tx *sqlx.Tx) error {
			doc.Status = model.DocStatusCreatingIndex
			doc.UpdateTime = time.Now()
			if _, err := tx.NamedExecContext(rCtx, updateDocStatus, doc); err != nil {
				log.ErrorContextf(rCtx, "更新文档状态失败 sql:%s args:%+v err:%+v", updateDocStatus, doc, err)
				return err
			}
			if err := d.DocParseSegment(rCtx, tx, doc, true); err != nil {
				return err
			}
			return nil
		}); err != nil {
			log.ErrorContextf(rCtx, "无需审核文档失败 err:%+v", err)
			return
		}
	}(trpc.CloneContext(ctx))
	return nil
}

// AuditDocName 文档名称审核或者申诉回调处理函数，audit是父审核任务
func (d *dao) AuditDocName(ctx context.Context, audit *model.Audit, pass, isAppeal bool, rejectReason string) error {
	log.InfoContextf(ctx, "AuditDocName %+v, pass: %+v, isAppeal: %+v, rejectReason: %+v",
		audit, pass, isAppeal, rejectReason)
	intervene := false
	if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
		intervene = true
	}
	doc, err := d.GetDocByID(ctx, audit.RelateID, audit.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		audit.UpdateTime = time.Now()
		audit.Status = d.getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = d.UpdateAuditStatus(ctx, audit)
		log.InfoContextf(ctx, "文档已经被删除，不再走审核逻辑，doc:%+v", doc)
		return nil
	}
	auditsMap, err := d.bizAuditStatusMap(ctx, audit)
	if err != nil {
		return err
	}
	// 查询切片表, 看看文档是否有生成过切片,若没有生成过切片,则大概率这次审核是文档导入名称审核失败的送审
	segs, err := d.GetSegmentList(ctx, doc.CorpID, doc.ID, 1, 1, audit.RobotID)
	if err != nil {
		return err
	}
	isNeedCharSizeNotice := false
	if err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		// 重命名的处理逻辑
		doc, audit, err = d.updateDocNameAuditResult(ctx, tx, doc, audit, auditsMap, pass, isAppeal)
		if err != nil {
			return err
		}
		if !pass {
			return d.sendNoticeAndUpdateAuditStatusIfDocNameAuditFail(ctx, tx, doc, audit, auditsMap, isAppeal,
				rejectReason)
		}
		if len(segs) == 0 {
			// 若是导入过程中文档名称审核失败,则需要走切片入库学习流程
			if err = d.IsUsedCharSizeExceeded(ctx, doc.CorpID, doc.RobotID); err == nil { // 没超过上限,进入学习
				if isAppeal {
					_ = d.SendNoticeIfDocAppealPass(ctx, tx, doc, audit) // 人工申诉成功，但是发通知失败，不报错
				}
				if _, err = tx.NamedExecContext(ctx, updateDocAuditResult, doc); err != nil {
					log.ErrorContextf(ctx, "更新文档审核结果失败 sql:%s args:%+v err:%+v", updateDocAuditResult, doc, err)
					return err
				}
				if err = d.DocParseSegment(ctx, tx, doc, intervene); err != nil {
					return err
				}
			} else { // 超过上限不进入学习，返回错误
				isNeedCharSizeNotice = true
				doc.Status = model.DocStatusParseImportFail
				doc.Message = terrs.Msg(d.ConvertErrMsg(ctx, 0, doc.CorpID, errs.ErrDocParseCharSizeExceeded))
				if _, err = tx.NamedExecContext(ctx, updateDocAuditResult, doc); err != nil {
					log.ErrorContextf(ctx, "更新文档审核结果失败 sql:%s args:%+v err:%+v", updateDocAuditResult, doc, err)
					return err
				}
			}
			return nil
		}
		// 审核通过, 执行重命名入库
		if isAppeal {
			_ = d.sendNoticeIfDocNameAppealPass(ctx, tx, doc, audit) // 人工申诉成功，但是发通知失败，不报错
		}
		if err := d.CreateDocRenameToIndexTask(ctx, doc); err != nil {
			log.ErrorContextf(ctx, "审核文档名 新增向量重新入库任务失败 err:%+v", err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "审核文档名 状态修改失败 err:%+v", err)
		return err
	}
	if isNeedCharSizeNotice {
		var docParses model.DocParse
		if docParses, err = d.GetDocParseByDocID(ctx, doc.ID, doc.RobotID); err != nil {
			log.ErrorContextf(ctx, "查询 文档解析任务失败 args:%+v err:%+v", doc, err)
			return err
		}
		docParses.Status = model.DocParseCallBackCharSizeExceeded
		err = d.UpdateDocParseTask(ctx, docParses) // 更新解析字符状态,重试的时候不会重新解析
		if err != nil {
			return errs.ErrUpdateDocParseTaskStatusFail
		}
		if err = d.FailCharSizeNotice(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

func (d *dao) updateDocAuditResult(ctx context.Context, tx *sqlx.Tx, doc *model.Doc, audit *model.Audit,
	auditsMap map[uint32][]*model.AuditStatusSourceList, pass, isAppeal bool) (*model.Doc, *model.Audit, error) {
	now := time.Now()
	audit.Status = d.getAuditStatus(pass, isAppeal)
	audit.UpdateTime, doc.UpdateTime = now, now
	doc.Status = model.DocStatusCreatingIndex
	doc.AuditFlag = model.AuditFlagDone
	if !pass {

		childAudits, err := d.GetChildAuditsByParentID(ctx, audit.CorpID, audit.RobotID, audit.ID)
		if err != nil {
			return nil, nil, err
		}
		contentFailed := false
		nameFailed := false
		segmentFailed := false
		segmentPictureFailed := false
		for _, ca := range childAudits {
			// 解析params
			p := model.AuditItem{}
			if err := jsoniter.UnmarshalFromString(ca.Params, &p); err != nil {
				continue
			}
			if p.Source == model.AuditSourceDocName && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				nameFailed = true
			}
			if p.Source == model.AuditSourceDoc && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				contentFailed = true
			}
			// todo 缺少sheet类型
			if p.Source == model.AuditSourceDocSegment && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				segmentFailed = true
			}
			if p.Source == model.AuditSourceDocSegmentPic && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				segmentPictureFailed = true
			}
		}
		if isAppeal {
			// doc.Message = "审核失败，请修改后重新导入"
			// doc.Status = model.DocStatusAppealFailed
			if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
				if (segmentFailed && segmentPictureFailed) || (contentFailed && segmentPictureFailed) {
					doc.Message = i18nkey.KeyDocumentInterventionTextImageReviewFailed
					doc.Status = model.DocStatusAppealFailed
				} else if segmentPictureFailed {
					doc.Message = i18nkey.KeyDocumentInterventionImageReviewFailed
					doc.Status = model.DocStatusAppealFailed
				} else if segmentFailed || contentFailed {
					doc.Message = i18nkey.KeyDocumentInterventionTextReviewFailed
					doc.Status = model.DocStatusAppealFailed
				}
			} else {
				if contentFailed && nameFailed {
					doc.Message = i18nkey.KeyFileNameAndContentReviewFailed
					doc.Status = model.DocStatusAppealFailed
				} else if nameFailed {
					doc.Message = i18nkey.KeyFileNameReviewFailed
					doc.Status = model.DocStatusDocNameAppealFail
				} else {
					doc.Message = i18nkey.KeyFileContentReviewFailed
					doc.Status = model.DocStatusAppealFailed
				}
			}
		} else if _, ok := auditsMap[model.AuditStatusTimeoutFail]; ok {
			audit.Status = model.AuditStatusTimeoutFail
			doc.Status = model.DocStatusAuditFail
			if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
				// todo 重试/申诉 功能待开发
				doc.Message = i18nkey.KeyDocumentReviewTimeout
			} else {
				doc.Message = i18nkey.KeyFileReviewTimeout
			}
		} else {
			// doc.Message = "审核失败，请修改后重新导入，或点击 人工申诉"
			if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
				if (segmentFailed && segmentPictureFailed) || (contentFailed && segmentPictureFailed) {
					doc.Message = i18nkey.KeyDocumentInterventionTextImageReviewFailed
					doc.Status = model.DocStatusAuditFail
				} else if segmentPictureFailed {
					doc.Message = i18nkey.KeyDocumentInterventionImageReviewFailed
					doc.Status = model.DocStatusAuditFail
				} else if segmentFailed || contentFailed {
					doc.Message = i18nkey.KeyDocumentInterventionTextReviewFailed
					doc.Status = model.DocStatusAuditFail
				}
			} else {
				if contentFailed && nameFailed {
					doc.Message = i18nkey.KeyFileNameAndContentReviewFailedWithOption
					doc.Status = model.DocStatusDocNameAndContentAuditFail
				} else if nameFailed {
					doc.Message = i18nkey.KeyFileNameReviewFailedWithOption
					doc.Status = model.DocStatusImportDocNameAuditFail
				} else {
					doc.Message = i18nkey.KeyFileContentReviewFailedWithOption
					doc.Status = model.DocStatusAuditFail
				}
			}
		}
	}
	if _, err := tx.NamedExecContext(ctx, updateDocAuditResult, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档审核结果失败 sql:%s args:%+v err:%+v", updateDocAuditResult, doc, err)
		return nil, nil, err
	}
	return doc, audit, nil
}

func (d *dao) updateDocNameAuditResult(ctx context.Context, tx *sqlx.Tx, doc *model.Doc, audit *model.Audit,
	auditsMap map[uint32][]*model.AuditStatusSourceList, pass, isAppeal bool) (*model.Doc, *model.Audit, error) {
	now := time.Now()
	audit.Status = d.getAuditStatus(pass, isAppeal)
	audit.UpdateTime, doc.UpdateTime = now, now
	doc.Status = model.DocStatusCreatingIndex
	doc.AuditFlag = model.AuditFlagDone
	if !pass {
		if isAppeal {
			doc.Message = i18nkey.KeyFileNameReviewFailed
			doc.Status = model.DocStatusDocNameAppealFail
		} else if _, ok := auditsMap[model.AuditStatusTimeoutFail]; ok {
			audit.Status = model.AuditStatusTimeoutFail
			doc.Message = i18nkey.KeyDocumentNameReviewTimeout
			doc.Status = model.DocStatusDocNameAuditFail
		} else {
			doc.Message = i18nkey.KeyDocumentNameReviewFailed
			doc.Status = model.DocStatusDocNameAuditFail
		}
	}
	if _, err := tx.NamedExecContext(ctx, updateDocAuditResult, doc); err != nil {
		log.ErrorContextf(ctx, "更新文档审核结果失败 sql:%s args:%+v err:%+v", updateDocAuditResult, doc, err)
		return nil, nil, err
	}
	return doc, audit, nil
}

// sendNoticeAndUpdateAuditStatusIfDocNameAuditFail 审核/申诉未通过，更新审核状态，发送通知
func (d *dao) sendNoticeAndUpdateAuditStatusIfDocNameAuditFail(ctx context.Context, tx *sqlx.Tx, doc *model.Doc,
	audit *model.Audit, auditsMap map[uint32][]*model.AuditStatusSourceList, isAppeal bool, rejectReason string) error {
	var err error
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}},
		{Typ: model.OpTypeAppeal, Params: model.OpParams{
			AppealType: model.AppealBizTypeDoc,
			DocBizID:   strconv.FormatUint(doc.BusinessID, 10),
		}},
	}
	content := i18n.Translate(ctx, i18nkey.KeyReviewNotPassedWithName, doc.GetRealFileName())
	title := i18n.Translate(ctx, i18nkey.KeyDocumentRenameReviewNotPassed)
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
		content = i18n.Translate(ctx, i18nkey.KeyManualAppealNotPassedWithNameReModifyAndReason, doc.GetRealFileName(), rejectReason)
		operations = []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
		title = i18n.Translate(ctx, i18nkey.KeyDocumentRenameManualAppealNotPassed)
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
	if err = d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
		log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
		return err
	}
	return nil
}

// sendNoticeAndUpdateAuditStatusIfDocAuditFail 审核/申诉未通过，更新审核状态，发送通知
func (d *dao) sendNoticeAndUpdateAuditStatusIfDocAuditFail(ctx context.Context, tx *sqlx.Tx, doc *model.Doc,
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
	if err = d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
		log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
		return err
	}
	return nil
}

// SendNoticeIfDocAppealPass 人工申诉通过，发送通知
func (d *dao) SendNoticeIfDocAppealPass(ctx context.Context, tx *sqlx.Tx, doc *model.Doc,
	audit *model.Audit) error {
	var err error
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
	content := i18n.Translate(ctx, i18nkey.KeyManualAppealPassedWithName, doc.FileName)
	title := i18n.Translate(ctx, i18nkey.KeyDocumentManualAppealPassed)
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelInfo),
		model.WithSubject(title),
		model.WithContent(content),
	}
	notice := model.NewNotice(model.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// sendNoticeIfDocNameAppealPass 人工申诉通过，发送通知
func (d *dao) sendNoticeIfDocNameAppealPass(ctx context.Context, tx *sqlx.Tx, doc *model.Doc,
	audit *model.Audit) error {
	var err error
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
	content := i18n.Translate(ctx, i18nkey.KeyManualAppealPassedWithName, doc.GetRealFileName())
	title := i18n.Translate(ctx, i18nkey.KeyDocumentRenameManualAppealPassed)
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelInfo),
		model.WithSubject(title),
		model.WithContent(content),
	}
	notice := model.NewNotice(model.NoticeTypeDocToQA, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// FailCharSizeNotice 文档字符数总量已超限制失败通知
func (d *dao) FailCharSizeNotice(ctx context.Context, doc *model.Doc) error {
	log.DebugContextf(ctx, "FailCharSizeNotice , doc: %+v", doc)
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelError),
		model.WithSubject(i18nkey.KeyDocumentImportFailure),
		model.WithContent(fmt.Sprintf("文档【%s】导入失败，失败原因：%s", doc.FileName,
			terrs.Msg(d.ConvertErrMsg(ctx, 0, doc.CorpID, errs.ErrDocParseCharSizeExceeded)))),
	}
	notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.CreateNotice(ctx, notice); err != nil {
		log.ErrorContextf(ctx, "CreateNotice err:%+v err:%+v", notice, err)
		return err
	}
	return nil
}

func (d *dao) checkDoc(ctx context.Context, audit *model.Audit) (*model.Doc, error) {
	doc, err := d.GetDocByID(ctx, audit.RelateID, audit.RobotID)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, errs.ErrDocNotFound
	}
	if !doc.NeedAudit() {
		return nil, nil
	}
	return doc, err
}

func (d *dao) DocParseSegment(ctx context.Context, tx *sqlx.Tx, doc *model.Doc, intervene bool) error {
	taskID := ""
	docParse, err := d.GetDocParseByDocIDAndType(ctx, doc.ID, model.DocParseTaskTypeWordCount, doc.RobotID)
	if err != nil {
		taskID = ""
	}
	if docParse.TaskID != "" {
		taskID = docParse.TaskID
	}
	if intervene {
		// 获取redis中的新文档cos信息(解析切分干预使用)
		app, err := d.GetAppByID(ctx, doc.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "DocParseSegment|getAppByAppBizID|err:%+v", err)
			return errs.ErrRobotNotFound
		}
		newDoc, redisValue, err := d.GetInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID,
			app.BusinessID, doc.BusinessID, doc.CosHash)
		if err == nil && newDoc != nil && redisValue != nil && (redisValue.InterventionType == model.InterventionTypeSheet ||
			redisValue.InterventionType == model.InterventionTypeOrgData) {
			log.InfoContextf(ctx, "DocParseSegment|GetOldDocCosHashToNewDocRedisValue|docBizID:%d", doc.BusinessID)
			doc.CosURL = newDoc.CosURL
			doc.CosHash = newDoc.CosHash
		}
	}
	requestID := trace.SpanContextFromContext(ctx).TraceID().String()
	if _, err = d.SendDocParseCreateSegment(ctx, taskID, doc, requestID, intervene); err != nil {
		return err
	}
	newDocParse := model.DocParse{
		DocID:        doc.ID,
		CorpID:       doc.CorpID,
		RobotID:      doc.RobotID,
		StaffID:      doc.StaffID,
		RequestID:    requestID,
		Type:         model.DocParseTaskTypeSplitSegment,
		OpType:       model.DocParseOpTypeSplit,
		Status:       model.DocParseIng,
		TaskID:       taskID,
		SourceEnvSet: docParse.SourceEnvSet,
	}
	err = d.CreateDocParse(ctx, tx, newDocParse)
	if err != nil {
		return err
	}
	return nil
}

// getRobotSplitStrategy 获取拆分策略配置
func (d *dao) getRobotSplitStrategy(ctx context.Context, appDB *model.AppDB, fileName string) (string, error) {
	app, err := appDB.ToApp()
	if err != nil {
		log.ErrorContextf(ctx, "获取拆分策略配置失败 err:%+v", err)
		return "", err
	}
	docSplitConf, _, err := app.GetDocSplitConf(model.AppTestScenes)
	if err != nil {
		return "", errs.ErrUnknownSplitConfig
	}
	prefix := strings.TrimSuffix(fileName, filepath.Ext(fileName)) + ": \n"
	splitStrategy, err := docSplitConf.GetSplitStrategy(ctx, prefix, model.DocSplitTypeDoc)
	if err != nil {
		return "", err
	}
	return splitStrategy, nil
}

// GetBizAuditStatusByRelateIDs 根据 RelateIDs 查询审核状态
func (d *dao) GetBizAuditStatusByRelateIDs(ctx context.Context, robotID, corpID uint64,
	relateIDs []uint64) (map[uint64]model.AuditStatus, error) {
	if len(relateIDs) == 0 {
		return nil, errs.ErrAuditNotFound
	}
	condition := ""
	if len(relateIDs) == 1 {
		condition = " AND relate_id = ?"
	} else {
		condition = fmt.Sprintf(" AND relate_id IN ( %s )", placeholder(len(relateIDs)))
	}
	args := []any{corpID, robotID, model.AuditBizTypeDoc}
	for _, v := range relateIDs {
		args = append(args, v)
	}
	querySQL := fmt.Sprintf(getBizAuditStatusByRelateIDs, condition)
	auditRelateIDs := make([]model.AuditRelateID, 0)
	if err := d.db.QueryToStructs(ctx, &auditRelateIDs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "根据 RelateIDs 获取最后一次审核的 AuditStatus 失败 sql:%s args:%+v err:%+v", querySQL,
			args, err)
		return nil, err
	}
	audits := make(map[uint64]model.AuditStatus)
	for _, v := range auditRelateIDs {
		audits[v.RelateID] = model.AuditStatus{Status: v.Status}
	}
	return audits, nil
}

// BatchCreateReleaseAudit 批量创建发布审核数据
func (d *dao) BatchCreateReleaseAudit(ctx context.Context, parent *model.Audit, audits []*model.Audit,
	p model.AuditSendParams) ([]*model.Audit, error) {
	isNeedNotice, noticeType, pageID, subject := d.getAuditNotice(p)
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := d.sendNotice(ctx, isNeedNotice, tx, parent, noticeType, pageID, subject); err != nil {
			return err
		}
		length := len(audits)
		pageSize := 100
		pages := int(math.Ceil(float64(length) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > length {
				end = length
			}
			tmpAudits := audits[start:end]
			querySQL := fmt.Sprintf(createAudit, auditFields)
			if _, err := tx.NamedExecContext(ctx, querySQL, tmpAudits); err != nil {
				log.ErrorContextf(ctx, "新增审核数据失败 sql:%s args:%+v err:%+v", querySQL, tmpAudits, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return audits, nil
}

func (d *dao) getReleaseConfigInfo(ctx context.Context, id uint64, releaseJSON, previewJSON string) (
	model.AppDetailsConfig, model.AppDetailsConfig, *model.ReleaseConfig, error) {
	var release model.AppDetailsConfig
	var preview model.AppDetailsConfig
	cfg, err := d.GetReleaseConfigItemByID(ctx, id)
	if err != nil || cfg == nil {
		return release, preview, cfg, errs.ErrReleaseConfigNotFound
	}
	if err := jsoniter.Unmarshal([]byte(releaseJSON), &release); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
		return release, preview, cfg, nil
	}
	if err := jsoniter.Unmarshal([]byte(previewJSON), &preview); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
		return release, preview, cfg, nil
	}
	return release, preview, cfg, nil
}

// IsUsedCharSizeExceeded 校验字符使用量是否已经超过限
func (d *dao) IsUsedCharSizeExceeded(ctx context.Context, corpID, robotID uint64) error {
	if len(config.App().DebugConfig.CharExceededBotList) > 0 {
		bizID, _ := d.GetBotBizIDByID(ctx, robotID)
		if bizID > 0 && slices.Contains(config.App().DebugConfig.CharExceededBotList, strconv.FormatUint(bizID, 10)) {
			return errs.ErrOverCharacterSizeLimit
		}
	}
	corp, err := d.GetCorpByID(ctx, corpID)
	if err != nil {
		return errs.ErrCorpNotFound
	}
	// 如果没有打开集成商字符数校验开关，就不需要对集成商做校验
	if !utilConfig.GetMainConfig().CheckSystemIntegratorCharacterUsage && d.IsSystemIntegrator(ctx, corp) {
		return nil
	}
	corp, err = d.GetCorpBillingInfo(ctx, corp)
	if err != nil {
		return errs.ErrCorpNotFound
	}
	usedCharSize, err := d.GetCorpUsedCharSizeUsage(ctx, corpID)
	if err != nil {
		return errs.ErrSystem
	}
	if corp.IsUsedCharSizeExceeded(int64(usedCharSize)) {
		return errs.ErrOverCharacterSizeLimit
	}
	return nil
}

// GetAuditByEtag 通过tag获取文件是否已经审核通过
func (d *dao) GetAuditByEtag(ctx context.Context, robotID, corpID, relateID uint64, eTag string) ([]*model.Audit,
	error) {
	querySQL := fmt.Sprintf(getAuditByEtag, auditFields)
	args := make([]any, 0, 6)
	args = append(args, corpID, robotID, relateID, eTag, model.AuditStatusPass, model.AuditStatusTimeoutFail)

	audits := make([]*model.Audit, 0)
	if err := d.db.QueryToStructs(ctx, &audits, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetAuditByEtag sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}
	return audits, nil
}

// AuditQa QA审核或者申诉回调处理函数，audit是父审核任务
func (d *dao) AuditQa(ctx context.Context, audit *model.Audit, pass, isAppeal bool,
	rejectReason string) error {
	qa, err := d.GetQAByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if qa == nil || audit == nil || qa.CorpID != audit.CorpID || qa.RobotID != audit.RobotID {
		return errs.ErrQANotFound
	}
	if qa.IsDelete() {
		audit.UpdateTime = time.Now()
		audit.Status = d.getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = d.UpdateAuditStatus(ctx, audit)
		log.InfoContextf(ctx, "qa已经被删除，不再走审核或者申诉逻辑，qa:%+v", qa)
		return nil
	}
	// 这里只获取了未删除的相似问，已删除的相似问同步向量库是在UpdateQA()函数中处理的
	simQuestions, err := d.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		return err
	}
	var isExceeded bool // 是否超过字符数限制
	var syncID uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	var auditStatusList []*model.AuditStatusList
	err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if pass { // 审核通过
			syncID, syncSimilarQuestionsIDs, isExceeded, err = d.dealQaAuditPass(ctx, tx, qa, simQuestions)
			if err != nil {
				return err
			}
		} else { // 审核不通过
			auditStatusList, err = d.getBizAuditList(ctx, audit.CorpID, audit.RobotID, audit.ID)
			if err != nil {
				return err
			}
			if len(auditStatusList) == 0 {
				return errs.ErrAuditNotFound
			}
			corp, err := d.GetCorpByID(ctx, qa.CorpID)
			if err != nil {
				return err
			}

			appDB, err := d.GetAppByID(ctx, qa.RobotID)
			if err != nil {
				return err
			}

			isExceeded, err = d.dealQaAuditFail(ctx, tx, qa, simQuestions, auditStatusList, isAppeal, corp.Uin, appDB.BusinessID)
			if err != nil {
				log.ErrorContextf(ctx, "审核不通过，更新qa和相似问状态失败 err:%+v", err)
				return err
			}
		}
		// 发送通知失败不报错
		_ = d.sendNoticeForQaAudit(ctx, tx, qa, simQuestions, audit, pass, isAppeal, isExceeded, rejectReason)
		audit.UpdateTime = time.Now()
		audit.Status = d.getAuditStatus(pass, isAppeal)
		if _, err = tx.NamedExecContext(ctx, updateAuditStatus, audit); err != nil {
			log.ErrorContextf(ctx, "更新审核状态失败 sql:%s args:%+v err:%+v", updateAuditStatus, audit, err)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if pass && !isExceeded { // 审核通过，且未超量，则同步
		d.vector.Push(ctx, syncID)
		d.vector.BatchPush(ctx, syncSimilarQuestionsIDs)
	}
	return nil
}

// dealQaAuditPass qa审核通过处理
func (d *dao) dealQaAuditPass(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA,
	sims []*model.SimilarQuestion) (uint64, []uint64, bool, error) {

	now := time.Now()
	isExceeded := false // 是否超量
	qa.ReleaseStatus = model.QAReleaseStatusLearning
	qa.IsAuditFree = model.QAIsAuditFree
	qa.UpdateTime = now
	err := d.auditQaCheckCharExceeded(ctx, qa)
	if err == errs.ErrOverCharacterSizeLimit {
		isExceeded = true
	}
	if isExceeded { // 审核/申诉通过，但是超量，则设置QA状态为超量
		qa.ReleaseStatus = model.QAReleaseStatusCharExceeded
	}
	err = d.UpdateQAAuditStatusAndUpdateTimeTx(ctx, tx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "审核通过，更新qa状态失败 err:%+v", err)
		return 0, nil, isExceeded, err
	}
	var syncID uint64
	if !isExceeded { // 非超量状态才需要同步
		// 添加同步任务
		syncID, err = d.addQASync(ctx, tx, qa)
		if err != nil {
			log.ErrorContextf(ctx, "审核通过，添加qa同步任务失败 err:%+v", err)
			return 0, nil, isExceeded, err
		}
	}

	// 处理相似问
	var syncSimilarQuestionsIDs []uint64
	if len(sims) > 0 {
		for i := range sims {
			sims[i].ReleaseStatus = model.QAReleaseStatusLearning
			if isExceeded {
				sims[i].ReleaseStatus = model.QAReleaseStatusCharExceeded
			}
			sims[i].UpdateTime = now
			sims[i].IsAuditFree = model.QAIsAuditFree
		}
		err = d.UpdateSimilarQuestionsStatus(ctx, tx, sims)
		if err != nil {
			log.ErrorContextf(ctx, "审核通过，更新相似问状态失败 err:%+v", err)
			return 0, nil, isExceeded, err
		}
		if !isExceeded { // 非超量状态才需要同步
			// 添加同步任务
			if syncSimilarQuestionsIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sims); err != nil {
				log.ErrorContextf(ctx, "审核通过，添加相似问同步任务失败 err:%+v", err)
				return 0, nil, isExceeded, err
			}
		}
	}
	return syncID, syncSimilarQuestionsIDs, isExceeded, nil
}

// getAuditStatus 获取审核状态
func (d *dao) getAuditStatus(pass, isAppeal bool) uint32 {
	if pass {
		if isAppeal {
			return model.AuditStatusAppealSuccess
		}
		return model.AuditStatusPass
	} else {
		if isAppeal {
			return model.AuditStatusAppealFail
		}
		return model.AuditStatusFail
	}
}

// auditQaCheckCharExceeded 判断应用使用的字符数是否超量，或者qa对应的问答是否处于超量。文档超量，它对应的qa也当作超量处理
func (d *dao) auditQaCheckCharExceeded(ctx context.Context, qa *model.DocQA) error {
	var doc *model.Doc
	var err error
	if qa.DocID > 0 {
		doc, err = d.GetDocByID(ctx, qa.DocID, qa.RobotID)
		if err != nil {
			log.InfoContextf(ctx, "获取qa关联的doc失败，qa:%+v, err", qa, err)
			doc = nil
		}
	}
	err = d.IsUsedCharSizeExceeded(ctx, qa.CorpID, qa.RobotID)
	if err != nil && err != errs.ErrOverCharacterSizeLimit {
		log.ErrorContextf(ctx, "获取文档使用的字符数失败，qa:%+v, err", qa, err)
		return err
	}
	if err == errs.ErrOverCharacterSizeLimit || (doc != nil && doc.IsCharSizeExceeded()) {
		return errs.ErrOverCharacterSizeLimit
	}
	return nil
}

func (d *dao) dealQaAuditFail(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA,
	sims []*model.SimilarQuestion, auditStatusList []*model.AuditStatusList, isAppeal bool,
	uin string, appBizID uint64) (bool, error) {
	isExceeded := false
	err := d.auditQaCheckCharExceeded(ctx, qa)
	if err == errs.ErrOverCharacterSizeLimit {
		isExceeded = true
	}
	if isExceeded {
		if isAppeal {
			qa.ReleaseStatus = model.QAReleaseStatusAppealFailCharExceeded
		} else {
			qa.ReleaseStatus = model.QAReleaseStatusAuditNotPassCharExceeded
		}
		qa.UpdateTime = time.Now()
		err = d.UpdateQAAuditStatusAndUpdateTimeTx(ctx, tx, qa)
		if err != nil {
			log.ErrorContextf(ctx, "更新qa状态失败 err:%+v", err)
			return isExceeded, err
		}
		if len(sims) > 0 {
			for i := range sims {
				sims[i].ReleaseStatus = qa.ReleaseStatus
				sims[i].UpdateTime = qa.UpdateTime
			}
			err = d.UpdateSimilarQuestionsStatus(ctx, tx, sims)
			if err != nil {
				log.ErrorContextf(ctx, "更新相似问状态失败 err:%+v", err)
				return isExceeded, err
			}
		}
		return isExceeded, nil
	}
	// 下面是未超量的逻辑
	stAndItems, err := getAuditStatusAndItems(ctx, auditStatusList)
	if err != nil {
		return isExceeded, err
	}
	if len(stAndItems) == 0 {
		return isExceeded, fmt.Errorf("未找到失败的子审核数据,related qaID: %d", qa.ID)
	}
	var imageRedictedURLs = make([]string, 0)
	for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
		if !utilConfig.IsInWhiteList(uin, appBizID, utilConfig.GetWhitelistConfig().QaURLWhiteList) {
			safe, err := util.IsSafeURL(ctx, image)
			if err != nil || !safe {
				log.WarnContextf(ctx, "getQaAuditParams|imageUrl|safe:%v|err:%v", safe, err)
				return isExceeded, errs.ErrFileUrlFail
			}
		}
		url := getRedirectedURL(image)
		imageRedictedURLs = append(imageRedictedURLs, url)
	}
	var videos []*model.DocQAFile
	if config.App().VideoAuditSwitch {
		videos, err = util.AuditQaVideoURLs(ctx, qa.Answer)
		if err != nil {
			return isExceeded, err
		}
	}
	qa, sims = setQaAndSimilarAuditStatusIfAuditFail(ctx, qa, sims, stAndItems, imageRedictedURLs, videos, isAppeal)
	err = d.UpdateQAAuditStatusAndUpdateTimeTx(ctx, tx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "更新qa状态失败 err:%+v", err)
		return isExceeded, err
	}
	if len(sims) > 0 {
		err = d.UpdateSimilarQuestionsStatus(ctx, tx, sims)
		if err != nil {
			log.ErrorContextf(ctx, "更新相似问状态失败 err:%+v", err)
			return isExceeded, err
		}
	}
	return isExceeded, nil
}

// sendNoticeForQaAudit 问答审核/申诉之后，发送通知
func (d *dao) sendNoticeForQaAudit(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA, sims []*model.SimilarQuestion,
	audit *model.Audit, pass, isAppeal, isExceeded bool, rejectReason string) error {
	var (
		err            error
		content, title string
		operations     []model.Operation
		noticeOptions  []model.NoticeOption
	)
	if pass && !isAppeal { // 机器审核通过不需要发通知
		return nil
	}
	if pass && isAppeal && isExceeded { // 申诉通过，但是处于超量状态不需要发通知
		return nil
	}
	if !pass {
		if !isAppeal {
			operations = []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}},
				{Typ: model.OpTypeAppeal, Params: model.OpParams{
					AppealType: model.AppealBizTypeQa,
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
				if v.ReleaseStatus == model.QAReleaseStatusAuditNotPass {
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
			operations = []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
			title = i18n.Translate(ctx, i18nkey.KeyQAManualAppealFailure)
		}
		noticeOptions = []model.NoticeOption{
			model.WithGlobalFlag(),
			model.WithPageID(model.NoticeQAPageID),
			model.WithLevel(model.LevelError),
			model.WithSubject(title),
			model.WithContent(content),
		}

	} else if isAppeal {
		content = i18n.Translate(ctx, i18nkey.KeyManualAppealPassedWithName, qa.Question)
		operations = []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
		title = i18n.Translate(ctx, i18nkey.KeyQAManualAppealSuccess)
		noticeOptions = []model.NoticeOption{
			model.WithGlobalFlag(),
			model.WithPageID(model.NoticeQAPageID),
			model.WithLevel(model.LevelInfo),
			model.WithSubject(title),
			model.WithContent(content),
		}
	}

	notice := model.NewNotice(model.NoticeTypeQAAuditOrAppeal, audit.ID, audit.CorpID, audit.RobotID,
		audit.CreateStaffID, noticeOptions...)
	if err = notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err = d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

func getAuditStatusAndItems(ctx context.Context, auditStatusList []*model.AuditStatusList) (
	[]*model.AuditStatusAndItem, error) {
	stAndItems := make([]*model.AuditStatusAndItem, 0)
	for _, v := range auditStatusList {
		auditItem := model.AuditItem{}
		if err := jsoniter.UnmarshalFromString(v.Params, &auditItem); err != nil {
			log.ErrorContextf(ctx, "任务参数解析失败 v.Params:%s,err:%+v", v.Params, err)
			return nil, err
		}
		if v.Status == model.AuditStatusFail || v.Status == model.AuditStatusAppealFail ||
			v.Status == model.AuditStatusTimeoutFail {
			stAndItems = append(stAndItems, &model.AuditStatusAndItem{
				Status:    v.Status,
				Type:      v.Type,
				AuditItem: &auditItem,
			})
		}
	}
	return stAndItems, nil
}

func setQaAndSimilarAuditStatusIfAuditFail(ctx context.Context, qa *model.DocQA, sims []*model.SimilarQuestion,
	stAndItems []*model.AuditStatusAndItem, imageRedictedURLs []string, videos []*model.DocQAFile, isAppeal bool) (
	*model.DocQA, []*model.SimilarQuestion) {
	qa = getQaAuditFailDetails(qa, stAndItems, imageRedictedURLs, videos)
	now := time.Now()
	qa.ReleaseStatus = model.QAReleaseStatusAuditNotPass // 只要问答审核失败(问答文本/图片/视频/相似问)，就置为审核失败
	if isAppeal {
		qa.ReleaseStatus = model.QAReleaseStatusAppealFail
	}
	qa.IsAuditFree = model.QAIsAuditNotFree
	qa.UpdateTime = now

	for i, v := range sims {
		sims[i].ReleaseStatus = model.QAReleaseStatusAuditing
		sims[i].UpdateTime = now
		for _, v1 := range stAndItems {
			if v1.AuditItem.Typ == model.AuditTypePlainText && v1.AuditItem.Content == v.Question {
				// 相似问审核失败
				sims[i].ReleaseStatus = model.QAReleaseStatusAuditNotPass
				if isAppeal {
					sims[i].ReleaseStatus = model.QAReleaseStatusAppealFail
				}
				break
			}
		}
	}
	log.InfoContextf(ctx, "setQaAndSimilarAuditStatusIfAuditFail qa:%+v, sims:%+v", qa, sims)
	return qa, sims
}

func getQaAuditFailDetails(qa *model.DocQA, stAndItems []*model.AuditStatusAndItem,
	imageRedictedURLs []string, videos []*model.DocQAFile) *model.DocQA {
	qaAuditFail := false
	imageAuditFail := false
	audioAuditFail := false
	content := fmt.Sprintf("%s\n%s\n%s", qa.Question, qa.Answer, qa.QuestionDesc)
	for _, v := range stAndItems {
		if !qaAuditFail && v.AuditItem.Typ == model.AuditTypePlainText && v.AuditItem.Content == content {
			// 问答审核失败
			qaAuditFail = true
		} else if !imageAuditFail && v.AuditItem.Typ == model.AuditTypePicture {
			// 图片审核失败
			imageAuditFail = slices.Contains(imageRedictedURLs, v.AuditItem.URL)
		} else if config.App().VideoAuditSwitch && !audioAuditFail && v.AuditItem.Typ == model.AuditTypeVideo {
			// 视频审核失败
			audioAuditFail = slices.ContainsFunc(videos, func(u *model.DocQAFile) bool {
				return u.CosURL == v.AuditItem.URL
			})
		}
	}
	qa.QaAuditFail = qaAuditFail
	qa.PicAuditFail = imageAuditFail
	qa.VideoAuditFail = audioAuditFail
	return qa
}

// getRedirectedURL 获取重定向后的url
func getRedirectedURL(url string) string {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return url
	}
	rsp, err := client.Do(req)
	if err != nil {
		return url
	}
	defer func() { _ = rsp.Body.Close() }()
	if rsp.StatusCode != http.StatusFound && rsp.StatusCode != http.StatusMovedPermanently {
		return url
	}
	return rsp.Header.Get("Location")
}

func getAuditStatusAndItems1(ctx context.Context, auditFailList []*model.AuditFailList) (
	[]*model.AuditStatusAndItem, error) {
	stAndItems := make([]*model.AuditStatusAndItem, 0)
	for _, v := range auditFailList {
		if v.ParentID == 0 {
			continue
		}
		auditItem := model.AuditItem{}
		if err := jsoniter.UnmarshalFromString(v.Params, &auditItem); err != nil {
			log.ErrorContextf(ctx, "任务参数解析失败 v.Params:%s,err:%+v", v.Params, err)
			return nil, err
		}
		stAndItems = append(stAndItems, &model.AuditStatusAndItem{
			Status:    model.AuditStatusFail,
			Type:      v.Type,
			AuditItem: &auditItem,
		})
	}
	return stAndItems, nil
}

// DescribeQaAuditFailStatus 获取问答审核详情
func (d *dao) DescribeQaAuditFailStatus(ctx context.Context, qa *model.DocQA, sims []*model.SimilarQuestion,
	auditFailList []*model.AuditFailList, uin string, appBizID uint64) error {
	stAndItems, err := getAuditStatusAndItems1(ctx, auditFailList)
	if err != nil {
		log.ErrorContextf(ctx, "getAuditStatusAndItems1 err: %+v", err)
		return err
	}
	if len(stAndItems) == 0 {
		log.ErrorContextf(ctx, "未找到失败的子审核数据,related qaID: %d", qa.ID)
		return fmt.Errorf("未找到失败的子审核数据,related qaID: %d", qa.ID)
	}
	var imageRedictedURLs = make([]string, 0)
	for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
		if !utilConfig.IsInWhiteList(uin, appBizID, utilConfig.GetWhitelistConfig().QaURLWhiteList) {
			safe, err := util.IsSafeURL(ctx, image)
			if err != nil || !safe {
				log.WarnContextf(ctx, "getQaAuditParams|imageUrl|safe:%v|err:%v", safe, err)
				return errs.ErrFileUrlFail
			}
		}
		url := getRedirectedURL(image)
		imageRedictedURLs = append(imageRedictedURLs, url)
	}
	var videos []*model.DocQAFile
	if config.App().VideoAuditSwitch {
		videos, err = util.AuditQaVideoURLs(ctx, qa.Answer)
		if err != nil {
			log.ErrorContextf(ctx, "ExtractVideoURLs err: %+v", err)
			return err
		}
	}
	qa = getQaAuditFailDetails(qa, stAndItems, imageRedictedURLs, videos)
	for i, v := range sims {
		sims[i].ReleaseStatus = model.QAReleaseStatusAuditing
		for _, v1 := range stAndItems {
			if v1.AuditItem.Typ == model.AuditTypePlainText && v1.AuditItem.Content == v.Question {
				// 相似问审核失败
				sims[i].ReleaseStatus = model.QAReleaseStatusAuditNotPass
				break
			}
		}
	}
	return nil
}

// ConvertErrMsg 转换错误信息
func (d *dao) ConvertErrMsg(ctx context.Context, sID int, corpID uint64, oldErr error) error {
	if sID == 0 && corpID == 0 {
		return oldErr
	}
	if sID == 0 && corpID != 0 {
		corp, err := d.GetCorpByID(ctx, corpID)
		if err != nil {
			log.ErrorContextf(ctx, "GetCorpByID corpID:%d err:%+v", corpID, err)
			return oldErr
		}
		sID = corp.SID
	}
	systemIntegrator, err := d.GetSystemIntegratorByID(ctx, sID)
	if err != nil {
		log.ErrorContextf(ctx, "GetSystemIntegratorByID sID:%d err:%+v", sID, err)
		return oldErr
	}
	newErr := errs.ConvertErrMsg(systemIntegrator.Name, oldErr)
	return newErr
}
