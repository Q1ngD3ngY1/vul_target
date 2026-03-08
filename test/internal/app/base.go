// Package app TODO
package app

import (
	"context"
	"net/http"
	"time"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var handler = make(map[string]Base)
var b *base

// Base is base interface
type Base interface {
	// AnalysisDescribeApp 解析APP数据
	AnalysisDescribeApp(ctx context.Context, appDB *model.AppDB) (*model.App, error)
	// Collect 发布采集
	Collect(ctx context.Context, release *model.Release) error
	// AuditCollect 发布审核数据采集
	AuditCollect(ctx context.Context, parent *model.Audit, release *model.Release, p model.AuditSendParams) (
		[]*model.Audit, error)
	// BeforeAudit 发布审核前处理
	BeforeAudit(ctx context.Context, audit *model.Audit) error
	// AfterAudit 审核后执行发布
	AfterAudit(ctx context.Context, audit *model.Audit, isAuditPass bool) error
	// ExecRelease 执行发布
	ExecRelease(ctx context.Context, release *model.Release) error
	// Success 发布成功
	Success(ctx context.Context, release *model.Release) error
}

type base struct {
	dao dao.Dao
}

// New 创建应用
func New() {
	b = &base{
		dao: dao.New(),
	}
}

// GetApp 获取APP
func GetApp(appType string) Base {
	if len(handler) == 0 {
		return nil
	}
	if v, ok := handler[appType]; ok {
		return v
	}
	return nil
}

func getNeedAuditDiffConfig(needAuditFiled map[string]int, diff []model.AppConfigDiff) ([]model.AppConfigDiff,
	[]model.AppConfigDiff) {
	var needAuditDiff []model.AppConfigDiff
	var noAuditDiff []model.AppConfigDiff
	for _, v := range diff {
		_, ok := needAuditFiled[v.ConfigItem]
		if ok {
			needAuditDiff = append(needAuditDiff, v)
		} else {
			noAuditDiff = append(noAuditDiff, v)
		}
	}
	return needAuditDiff, noAuditDiff
}

func getReleaseConfig(diff []model.AppConfigDiff, needAudit bool,
	release *model.Release) []*model.ReleaseConfig {
	var cfg []*model.ReleaseConfig
	for _, v := range diff {
		auditStatus := model.ConfigReleaseStatusAuditing
		auditResult := ""
		if !config.AuditSwitch() || !needAudit || len(v.NewValue) == 0 {
			auditStatus = model.ReleaseQAAuditStatusSuccess
			auditResult = "无需审核"
		}
		cfg = append(cfg, &model.ReleaseConfig{
			CorpID:        release.CorpID,
			StaffID:       release.StaffID,
			RobotID:       release.RobotID,
			VersionID:     release.ID,
			ConfigItem:    v.ConfigItem,
			OldValue:      v.LastValue,
			Value:         v.NewValue,
			Content:       v.Content,
			Action:        v.Action,
			ReleaseStatus: model.ConfigReleaseStatusIng,
			Message:       "",
			AuditStatus:   auditStatus,
			AuditResult:   auditResult,
			CreateTime:    time.Now(),
			UpdateTime:    time.Now(),
			ExpireTime:    time.Unix(0, 0),
		})
	}
	return cfg
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
