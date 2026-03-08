package api

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

// AddUnsatisfiedReply 添加不满意回复
func (s *Service) AddUnsatisfiedReply(ctx context.Context, req *pb.AddUnsatisfiedReplyReq) (
	*pb.AddUnsatisfiedReplyRsp, error) {
	staffID := pkg.StaffID(ctx)
	log.InfoContextf(ctx, "AddUnsatisfiedReply Req:%+v,staffID:%+v", req, staffID)
	rsp := new(pb.AddUnsatisfiedReplyRsp)
	key := fmt.Sprintf(dao.LockForAddUnsatisfiedReply, req.GetRecordId())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		return nil, errs.ErrUnsatisfiedReplyAdding
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	unsatisfiedReply, err := s.fillUnsatisfiedReply(ctx, req, app)
	if err != nil {
		log.ErrorContextf(ctx, "fill unsatisfied reply req:%+v,err:%v", req, err)
		return rsp, err
	}
	unsatisfiedReplyInfo, err := s.dao.GetUnsatisfiedReplyByRecordID(ctx, app.CorpID, app.ID,
		req.GetRecordId())
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply by record req:%+v,err:%v", req, err)
		return rsp, err
	}
	if unsatisfiedReplyInfo == nil {
		if err := s.dao.AddUnsatisfiedReply(ctx, unsatisfiedReply); err != nil {
			log.ErrorContextf(ctx, "add unsatisfied reply req:%+v,err:%v", req, err)
			return rsp, err
		}
	} else {
		unsatisfiedReply.ID = unsatisfiedReplyInfo.ID
		if req.GetCancelFeedback() { // 取消反馈，则删除不满意记录
			unsatisfiedReply.IsDeleted = model.UnsatisfiedReplyDeleted
		}
		if err := s.dao.UpdateUnsatisfiedReply(ctx, unsatisfiedReply); err != nil {
			log.ErrorContextf(ctx, "update unsatisfied reply reasons req:%+v,err:%v", req, err)
			return rsp, err
		}
	}
	_ = s.dao.AddOperationLog(ctx, model.UnsatisfiedReplyAdd, app.CorpID, app.ID, req, rsp,
		nil, unsatisfiedReply)
	return rsp, nil
}

// fillUnsatisfiedReply TODO
func (s *Service) fillUnsatisfiedReply(ctx context.Context, req *pb.AddUnsatisfiedReplyReq,
	app *model.App) (*model.UnsatisfiedReplyInfo, error) {
	staffID := pkg.StaffID(ctx)
	contextStr, err := fillUnsatisfiedReplyContext(req.GetContext())
	if err != nil {
		return nil, err
	}
	// 用户未选择错误类型，则初始化一个默认错误原因Tag
	reasons := req.GetReasons()
	if len(reasons) == 0 {
		reasons = append(reasons, i18n.Translate(ctx, config.App().UnsatisfiedReplyUnselectReason))
	}
	return &model.UnsatisfiedReplyInfo{
		UnsatisfiedReply: model.UnsatisfiedReply{
			BusinessID: s.dao.GenerateSeqID(),
			CorpID:     app.CorpID,
			RobotID:    app.ID,
			RecordID:   req.GetRecordId(),
			Question:   req.GetQuestion(),
			Answer:     req.GetAnswer(),
			Context:    contextStr,
			IsDeleted:  model.UnsatisfiedReplyIsNotDeleted,
			Status:     model.UnsatisfiedReplyStatusWait,
			UserType:   pkg.LoginUserType(ctx),
			StaffID:    staffID,
		},
		Reasons: reasons,
	}, nil
}

// fillUnsatisfiedReplyContext TODO
func fillUnsatisfiedReplyContext(contexts []*pb.UnsatisfiedReplyContext) (string, error) {
	if len(contexts) == 0 {
		return "", nil
	}
	contextStr, err := jsoniter.MarshalToString(contexts)
	if err != nil {
		return "", err
	}
	return contextStr, nil
}
