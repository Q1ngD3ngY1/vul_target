package audit

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	jsoniter "github.com/json-iterator/go"
	"strings"
	"time"
)

// ResultCallback 审核结果回调处理
func ResultCallback(ctx context.Context, d dao.Dao, audit *model.Audit, resultCode, resultType uint32) error {
	if audit.IsCallbackDone() {
		return nil
	}
	audit.Status = model.AuditStatusPass
	audit.UpdateTime = time.Now()
	if resultCode != model.AuditResultPass {
		if resultType == model.ResultTypeTimeout {
			audit.Status = model.AuditStatusTimeoutFail
		} else {
			audit.Status = model.AuditStatusFail
		}
	}
	auditParams := &model.AuditItem{}
	if err := jsoniter.UnmarshalFromString(audit.Params, auditParams); err != nil {
		return err
	}
	isInnerCosUrl := false
	if auditParams.URL != "" && len(utilConfig.GetMainConfig().Audit.AuditCallbackCheckCosPathPrefix) != 0 {
		// 判断是否为内部cos地址，只有内部cos地址才需要校验，避免校验外部地址导致cos失败报错
		for _, prefix := range utilConfig.GetMainConfig().Audit.AuditCallbackCheckCosPathPrefix {
			if len(prefix) != 0 && strings.HasPrefix(auditParams.URL, prefix) {
				isInnerCosUrl = true
			}
		}
	}
	if isInnerCosUrl {
		if d.GetObjectETag(ctx, auditParams.URL) != audit.ETag {
			audit.Status = model.AuditStatusFail
			audit.Message = "文件内容被篡改"
		}
	}

	auditFilter := &dao.AuditFilter{
		IDs: []uint64{audit.ID},
	}
	updateColumns := []string{dao.AuditTblColRetryTimes, dao.AuditTblColStatus, dao.AuditTblColMessage}
	_, err := dao.GetAuditDao().UpdateAudit(ctx, nil, updateColumns, auditFilter, audit)
	if err != nil {
		log.ErrorContextf(ctx, "审核结果回调失败 err:%+v", err)
		return err
	}
	return nil
}
