package api

import (
	"context"
	logicRelease "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/release"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ReleaseDetailNotify 发布后回调-更新QA状态发结果
func (s *Service) ReleaseDetailNotify(ctx context.Context, req *pb.ReleaseDetailNotifyReq) (
	*pb.ReleaseDetailNotifyRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ReleaseDetailNotifyRsp)
	return rsp, nil
}

// ReleaseNotify 发布结果通知
func (s *Service) ReleaseNotify(ctx context.Context, req *pb.ReleaseNotifyReq) (*pb.ReleaseNotifyRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ReleaseNotifyRsp)
	return rsp, nil
}

func (s *Service) KnowledgeReleaseCallback(ctx context.Context, req *pb.KnowledgeReleaseCallbackReq) (
	*pb.KnowledgeReleaseCallbackRsp, error) {
	log.InfoContextf(ctx, "KnowledgeReleaseCallback req:%+v", req)
	rsp := new(pb.KnowledgeReleaseCallbackRsp)
	if req.GetCorpBizId() == "" || req.GetCorpId() == "" || req.GetAppBizId() == "" || req.GetAppId() == "" {
		log.ErrorContextf(ctx, "KnowledgeRelease req is invalid")
		return nil, errs.ErrParameterInvalid
	}
	var err error
	corpBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetCorpBizId())
	if err != nil {
		log.ErrorContextf(ctx, "KnowledgeRelease req corpBizID is invalid")
		return nil, errs.ErrParameterInvalid
	}
	_, err = util.CheckReqParamsIsUint64(ctx, req.GetCorpId())
	if err != nil {
		log.ErrorContextf(ctx, "KnowledgeRelease req corpID is invalid")
		return nil, errs.ErrParameterInvalid
	}
	appBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "KnowledgeRelease req appBizID is invalid")
		return nil, errs.ErrParameterInvalid
	}
	_, err = util.CheckReqParamsIsUint64(ctx, req.GetAppId())
	if err != nil {
		log.ErrorContextf(ctx, "KnowledgeRelease req appID is invalid")
		return nil, errs.ErrParameterInvalid
	}
	releaseItems := req.GetReleaseItems()
	if len(req.GetReleaseItems()) == 0 {
		return rsp, nil
	}
	err = logicRelease.KnowledgeRelease(ctx, corpBizId, appBizID, releaseItems)
	if err != nil {
		log.ErrorContextf(ctx, "KnowledgeRelease err:%+v", err)
		return nil, err
	}

	return &pb.KnowledgeReleaseCallbackRsp{}, nil
}
