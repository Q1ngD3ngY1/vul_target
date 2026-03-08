package service

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

const (
	PermissionInfinityAttributeLabel = "infinity_attribute_label"
)

// CheckKnowledgePermission 检查知识库白名单，校验知识库权限
// TODO(ericjwang): 有调用量。这不是权限，而是白名单。需要确认下使用场景，以及是否还需要
// 【【知识库管理】优化标签管理接口性能，支持百万级标签】https://tapd.woa.com/tapd_fe/70080800/task/detail/1070080800075703235
func (s *Service) CheckKnowledgePermission(ctx context.Context, req *pb.CheckKnowledgePermissionReq) (*pb.CheckKnowledgePermissionRsp, error) {
	rsp := &pb.CheckKnowledgePermissionRsp{}
	if len(req.GetOperations()) == 0 {
		return rsp, nil
	}
	appBaseInfo, err := s.rpc.AppAdmin.GetAppBaseInfo(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil {
		logx.E(ctx, "GetKnowledgeBaseConfig error: %+v", err)
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo == nil {
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo.CorpPrimaryId != contextx.Metadata(ctx).CorpID() {
		logx.E(ctx, "auth bypass, appBaseInfo.CorpPrimaryId:%d, ctx.CorpID:%d", appBaseInfo.CorpPrimaryId, contextx.Metadata(ctx).CorpID())
		return nil, errs.ErrAppNotFound
	}
	uin := contextx.Metadata(ctx).Uin()
	for _, op := range req.GetOperations() {
		if op == "" {
			continue
		}
		flag := false
		switch op {
		case PermissionInfinityAttributeLabel:
			appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
			if err != nil {
				return nil, err
			}
			flag = config.IsInWhiteList(uin, appBizID, config.GetWhitelistConfig().InfinityAttributeLabel)
		default:
			flag = false
		}
		res := &pb.KnowledgePermission{
			Operation: op,
			Allowed:   flag,
		}
		rsp.CheckResult = append(rsp.CheckResult, res)
	}
	return rsp, nil
}
