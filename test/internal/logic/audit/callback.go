package audit

import (
	"context"
	"strings"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
)

// ResultCallback 审核结果回调处理
func (l *Logic) ResultCallback(ctx context.Context, audit *releaseEntity.Audit, resultCode, resultType uint32) error {
	if audit.IsCallbackDone() {
		return nil
	}
	audit.Status = releaseEntity.AuditStatusPass
	audit.UpdateTime = time.Now()
	if resultCode != releaseEntity.AuditResultPass {
		if resultType == releaseEntity.ResultTypeTimeout {
			audit.Status = releaseEntity.AuditStatusTimeoutFail
		} else {
			audit.Status = releaseEntity.AuditStatusFail
		}
	}
	auditParams := &releaseEntity.AuditItem{}
	if err := jsonx.UnmarshalFromString(audit.Params, auditParams); err != nil {
		return err
	}
	isInnerCosUrl := false
	if auditParams.URL != "" && len(config.GetMainConfig().Audit.AuditCallbackCheckCosPathPrefix) != 0 {
		// 判断是否为内部cos地址，只有内部cos地址才需要校验，避免校验外部地址导致cos失败报错
		for _, prefix := range config.GetMainConfig().Audit.AuditCallbackCheckCosPathPrefix {
			if len(prefix) != 0 && strings.HasPrefix(auditParams.URL, prefix) {
				isInnerCosUrl = true
			}
		}
	}
	if isInnerCosUrl {
		if l.s3.GetObjectETag(ctx, auditParams.URL) != audit.ETag {
			audit.Status = releaseEntity.AuditStatusFail
			audit.Message = "文件内容被篡改"
		}
	}

	auditFilter := &releaseEntity.AuditFilter{
		IDs: []uint64{audit.ID},
	}
	updateColumns := []string{releaseEntity.AuditTblColRetryTimes, releaseEntity.AuditTblColStatus, releaseEntity.AuditTblColMessage}
	_, err := l.releaseDao.UpdateAudit(ctx, updateColumns, auditFilter, audit, nil)
	if err != nil {
		logx.E(ctx, "Failed to callback for audit result  err:%+v", err)
		return err
	}
	return nil
}
