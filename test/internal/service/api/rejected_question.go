package api

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ListRejectedQuestion 获取拒答问题列表
func (s *Service) ListRejectedQuestion(ctx context.Context,
	req *pb.ListRejectedQuestionReq) (*pb.ListRejectedQuestionRsp, error) {
	log.InfoContextf(ctx, "ListRejectedQuestion Req:%+v", req)
	rsp := new(pb.ListRejectedQuestionRsp)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	getRejectedQuestionListReq := model.GetRejectedQuestionListReq{
		CorpID:   app.CorpID,
		RobotID:  app.ID,
		Page:     req.GetPageNumber(),
		PageSize: req.GetPageSize(),
		Query:    req.GetQuery(),
		Actions:  req.GetActions(),
	}

	total, list, err := s.listRejectedQuestion(ctx, getRejectedQuestionListReq)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	rsp.Total = total
	rsp.List = list

	return rsp, nil
}

func (s *Service) listRejectedQuestion(ctx context.Context,
	req model.GetRejectedQuestionListReq) (uint64, []*pb.ListRejectedQuestionRsp_RejectedQuestions, error) {
	total, list, err := s.dao.GetRejectedQuestionList(ctx, req)
	if err != nil {
		return 0, nil, err
	}
	latestRelease, err := s.dao.GetLatestRelease(ctx, req.CorpID, req.RobotID)
	if err != nil {
		return 0, nil, err
	}
	rejectedQuestions := make([]*pb.ListRejectedQuestionRsp_RejectedQuestions, 0, len(list))
	for _, v := range list {
		rejectedQuestions = append(rejectedQuestions, &pb.ListRejectedQuestionRsp_RejectedQuestions{
			RejectedBizId: v.BusinessID,
			Question:      v.Question,
			Status:        v.ReleaseStatus,
			StatusDesc:    i18n.Translate(ctx, v.StatusDesc(latestRelease.IsPublishPause())),
			UpdateTime:    v.UpdateTime.Unix(),
			IsAllowDelete: v.IsAllowDeleted(),
			IsAllowEdit:   v.IsAllowEdit(),
		})
	}
	return total, rejectedQuestions, nil
}
