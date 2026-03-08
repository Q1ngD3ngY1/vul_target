package audit

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
)

// AuditCollect 发布审核数据采集
func (l *Logic) AuditCollect(ctx context.Context, parent *releaseEntity.Audit,
	release *releaseEntity.Release, p entity.AuditSendParams) (
	[]*releaseEntity.Audit, error) {
	auditItems := make([]*releaseEntity.AuditItem, 0)
	qas, err := l.releaseDao.GetAuditQAByVersion(ctx, release.ID)
	if err != nil {
		return nil, err
	}
	cfg, err := l.releaseDao.GetAuditConfigItemByVersion(ctx, release.ID)
	if err != nil {
		return nil, err
	}
	// 采集qa
	for _, qa := range qas {
		content := fmt.Sprintf("%s\n%s", qa.Question, qa.Answer)
		auditItems = append(
			auditItems,
			releaseEntity.NewPlainTextAuditItem(qa.ID, releaseEntity.AuditSourceReleaseQA, content, p.EnvSet),
		)
		for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
			url := getRedirectedURL(image)
			auditItems = append(auditItems, releaseEntity.NewPictureAuditItem(qa.ID, releaseEntity.AuditSourceReleaseQA, url, p.EnvSet, l.s3.GetObjectETag(ctx, url)))
		}
		videos, err := util.ExtractVideoURLs(ctx, qa.Answer)
		if err != nil {
			return nil, err
		}
		if videos == nil || len(videos) == 0 {
			continue
		}
		for k, video := range videos {
			objectInfo, err := l.qaLogic.GetCosFileInfoByUrl(ctx, video.CosURL)
			if err != nil {
				return nil, err
			}
			videos[k].ETag = objectInfo.ETag
			videos[k].Size = objectInfo.Size
			videoAudit, err := l.GetAuditByEtag(ctx, p.RobotID, p.CorpID, qa.ID, objectInfo.ETag)
			if err != nil {
				return nil, err
			}
			if len(videoAudit) > 0 {
				logx.I(ctx, "AuditCollect videoAudit 已经审核 video:%v qa:%v", videos[k], qa)
				continue
			}
			auditItems = append(
				auditItems, releaseEntity.NewVideoAuditItem(qa.ID, releaseEntity.AuditSourceReleaseQA, video.CosURL, p.EnvSet,
					video.ETag),
			)
		}
	}
	// 采集配置
	for _, v := range cfg {
		auditItems = append(auditItems, l.getAuditConfig(ctx, v, p)...)
	}
	audits := releaseEntity.NewAudits(ctx, parent, auditItems)
	now := time.Now()
	for _, audit := range audits {
		audit.BusinessID = idgen.GetId()
		audit.UpdateTime = now
		audit.CreateTime = now
	}
	releaseAudit, err := l.BatchCreateReleaseAudit(ctx, parent, audits, p)
	if err != nil {
		return nil, err
	}
	return releaseAudit, nil
}

func (l *Logic) getAuditConfig(ctx context.Context, cfg *releaseEntity.AuditReleaseConfig,
	p entity.AuditSendParams) []*releaseEntity.AuditItem {
	content := cfg.Value
	var auditItems []*releaseEntity.AuditItem
	switch cfg.ConfigItem {
	case entity.ConfigItemName:
		auditItems = append(auditItems,
			releaseEntity.NewUserDataAuditItem(cfg.ID, releaseEntity.AuditSourceRobotName, content, p.EnvSet))
	case entity.ConfigItemAvatar:
		auditItems = append(auditItems,
			releaseEntity.NewUserHeadURLAuditItem(cfg.ID, releaseEntity.AuditSourceRobotAvatar, content, p.EnvSet, l.s3.GetObjectETag(ctx, content)))
	case entity.ConfigItemGreeting:
		if len(content) == 0 {
			return auditItems
		}
		auditItems = append(auditItems,
			releaseEntity.NewUserGreetingAuditItem(cfg.ID, releaseEntity.AuditSourceRobotGreeting, content, p.EnvSet))
	case entity.ConfigItemBareAnswer:
		auditItems = append(auditItems,
			releaseEntity.NewPlainTextAuditItem(cfg.ID, releaseEntity.AuditSourceBareAnswer, content, p.EnvSet))
		for _, image := range util.ExtractImagesFromMarkdown(content) {
			auditItems = append(auditItems, releaseEntity.NewPictureAuditItem(cfg.ID, releaseEntity.AuditSourceBareAnswer, image, p.EnvSet, l.s3.GetObjectETag(ctx, image)))
		}
	case entity.ConfigItemRoleDescription:
		if len(content) == 0 {
			return auditItems
		}
		auditItems = append(auditItems,
			releaseEntity.NewPlainTextAuditItem(cfg.ID, releaseEntity.AuditSourceRobotRoleDescription, content, p.EnvSet))
	}
	return auditItems
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
