package api

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"github.com/spf13/cast"

	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	infosec "git.woa.com/adp/pb-go/kb/kb_config"
)

// AuditResultCallback 审核回调
// NOTE(ericjwang): 有调用，qbot.qbot.infosec:/v1/qbot/infosec/callback --> here
func (s *Service) AuditResultCallback(ctx context.Context, req *infosec.CheckResultReq) (
	*infosec.CheckResultRsp, error) {
	rsp := new(infosec.CheckResultRsp)
	bizID := cast.ToUint64(req.GetId())
	audit, err := s.auditLogic.GetAuditByBizID(ctx, bizID)
	if err != nil {
		return rsp, err
	}
	if audit == nil {
		return rsp, errs.ErrAuditNotFound
	}
	if err = s.auditLogic.ResultCallback(ctx, audit, req.GetResultCode(), req.GetResultType()); err != nil {
		logx.E(ctx, "Failed to callback for audit:%+v err:%+v", audit, err)
		return rsp, err
	}

	return rsp, nil
}

func (s *Service) getAppealStatus(isPass bool) uint32 {
	if isPass {
		return releaseEntity.AuditStatusAppealSuccess
	}
	return releaseEntity.AuditStatusAppealFail
}
