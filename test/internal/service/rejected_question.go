package service

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetRejectedQuestionList Deprecated 获取拒答问题列表
func (s *Service) GetRejectedQuestionList(ctx context.Context, req *pb.GetRejectedQuestionListReq) (
	*pb.GetRejectedQuestionListRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetRejectedQuestionListRsp)
	return rsp, nil
}

// ListRejectedQuestion 获取拒答问题列表
func (s *Service) ListRejectedQuestion(ctx context.Context, req *pb.ListRejectedQuestionReq) (
	*pb.ListRejectedQuestionRsp, error) {
	log.InfoContextf(ctx, "ListRejectedQuestion Req:%+v", req)
	rsp := new(pb.ListRejectedQuestionRsp)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil || app.CorpID != corpID {
		return nil, errs.ErrRobotNotFound
	}
	getRejectedQuestionListReq := model.GetRejectedQuestionListReq{
		CorpID:   corpID,
		StaffID:  staffID,
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
	var staffIDs []uint64
	for _, v := range list {
		staffIDs = append(staffIDs, v.CreateStaffID)
	}
	staffNickNameMap, err := s.dao.GetStaffNickNameMapByIDs(ctx, staffIDs)
	if err != nil {
		return 0, nil, err
	}

	rejectedQuestions := make([]*pb.ListRejectedQuestionRsp_RejectedQuestions, 0, len(list))
	for _, v := range list {
		log.InfoContextf(ctx, "ListRejectedQuestion 11111111111 v:%+v", v)
		rejectedQuestions = append(rejectedQuestions, &pb.ListRejectedQuestionRsp_RejectedQuestions{
			RejectedBizId: v.BusinessID,
			Question:      v.Question,
			Status:        v.ReleaseStatus,
			StatusDesc:    i18n.Translate(ctx, v.StatusDesc(latestRelease.IsPublishPause())),
			Operator:      staffNickNameMap[v.CreateStaffID],
			UpdateTime:    v.UpdateTime.Unix(),
			IsAllowDelete: v.IsAllowDeleted(),
			IsAllowEdit:   v.IsAllowEdit(),
		})
	}
	return total, rejectedQuestions, nil
}

func (s *Service) getRejectedQuestionList(ctx context.Context,
	req model.GetRejectedQuestionListReq) (uint64, []*pb.GetRejectedQuestionListRsp_RejectedQuestions, error) {
	total, list, err := s.dao.GetRejectedQuestionList(ctx, req)
	if err != nil {
		return 0, nil, err
	}
	latestRelease, err := s.dao.GetLatestRelease(ctx, req.CorpID, req.RobotID)
	if err != nil {
		return 0, nil, err
	}
	rejectedQuestions := make([]*pb.GetRejectedQuestionListRsp_RejectedQuestions, 0, len(list))
	for _, v := range list {
		rejectedQuestions = append(rejectedQuestions, &pb.GetRejectedQuestionListRsp_RejectedQuestions{
			Id:            v.ID,
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

// CreateRejectedQuestion 创建拒答问题
func (s *Service) CreateRejectedQuestion(ctx context.Context, req *pb.CreateRejectedQuestionReq) (
	*pb.CreateRejectedQuestionRsp, error) {
	log.InfoContextf(ctx, "CreateRejectedQuestion Req:%+v", req)
	rsp := new(pb.CreateRejectedQuestionRsp)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil || app.CorpID != corpID {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var replyID uint64
	if len(req.GetBusinessId()) > 0 {
		businessID, err := util.CheckReqParamsIsUint64(ctx, req.GetBusinessId())
		if err != nil {
			return nil, err
		}
		reply, err := s.dao.GetUnsatisfiedReplyByBizIDs(ctx, corpID, app.ID, []uint64{businessID})
		if err != nil {
			return rsp, errs.ErrSystem
		}
		if len(reply) == 0 {
			return rsp, errs.ErrUnsatisfiedReplyNotFound
		}
		replyID = reply[0].ID
	}
	rejectedQuestion := &model.RejectedQuestion{
		BusinessID:       s.dao.GenerateSeqID(),
		CorpID:           corpID,
		RobotID:          app.ID,
		CreateStaffID:    staffID,
		Question:         req.GetQuestion(),
		BusinessSourceID: replyID,
		BusinessSource:   req.GetBusinessSource(),
	}
	err = s.dao.CreateRejectedQuestion(ctx, rejectedQuestion)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	return rsp, nil
}

// ModifyRejectedQuestion 修改拒答问题
func (s *Service) ModifyRejectedQuestion(ctx context.Context, req *pb.ModifyRejectedQuestionReq) (
	*pb.ModifyRejectedQuestionRsp, error) {
	log.InfoContextf(ctx, "ModifyRejectedQuestion Req:%+v", req)
	var err error
	rsp := new(pb.ModifyRejectedQuestionRsp)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil || app.CorpID != corpID {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var rejectedQuestion *model.RejectedQuestion
	if len(req.Id) > 0 {
		var rejectedQuestionId uint64
		rejectedQuestionId, err = util.CheckReqParamsIsUint64(ctx, req.GetId())
		if err != nil {
			return nil, err
		}
		rejectedQuestion, err = s.dao.GetRejectedQuestionByID(ctx, corpID, app.ID, rejectedQuestionId)
	} else {
		var rejectedQuestionBizId uint64
		rejectedQuestionBizId, err = util.CheckReqParamsIsUint64(ctx, req.GetRejectedBizId())
		if err != nil {
			return nil, err
		}
		rejectedQuestion, err = s.dao.GetRejectedQuestionByBizID(ctx, corpID, app.ID, rejectedQuestionBizId)
	}
	if err != nil {
		return nil, errs.ErrRejectedQuestionNotFound
	}
	modifyRejectedQuestions, err := s.getPendingReleaseRejectedQuestion(ctx, corpID,
		app.ID, []*model.RejectedQuestion{rejectedQuestion})
	if err != nil {
		return nil, errs.ErrSystem
	}
	if _, ok := modifyRejectedQuestions[rejectedQuestion.ID]; ok {
		return nil, errs.ErrRejectedQuestionIsPendingRelease
	}

	isNeedPublish := false
	if rejectedQuestion.Question != req.GetQuestion() {
		isNeedPublish = true
	}
	if isNeedPublish {
		rejectedQuestion.ReleaseStatus = model.RejectedQuestionReleaseStatusInit
	}
	if isNeedPublish && rejectedQuestion.Action != model.RejectedQuestionAdd {
		rejectedQuestion.Action = model.RejectedQuestionUpdate
	}

	updateRejectedQuestion := &model.RejectedQuestion{
		CorpID:        corpID,
		CreateStaffID: staffID,
		RobotID:       app.ID,
		ID:            rejectedQuestion.ID,
		Question:      req.GetQuestion(),
		Action:        rejectedQuestion.Action,
		ReleaseStatus: rejectedQuestion.ReleaseStatus,
	}
	err = s.dao.UpdateRejectedQuestion(ctx, updateRejectedQuestion, isNeedPublish)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

// DeleteRejectedQuestion 删除拒答问题
func (s *Service) DeleteRejectedQuestion(ctx context.Context, req *pb.DeleteRejectedQuestionReq) (
	*pb.DeleteRejectedQuestionRsp, error) {
	log.InfoContextf(ctx, "DeleteRejectedQuestion Req:%+v", req)
	rsp := new(pb.DeleteRejectedQuestionRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil || app.CorpID != corpID {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var rejectedQuestionByIDs []*model.RejectedQuestion

	ids := slicex.Unique(req.GetIds())
	if len(ids) > 0 {
		uint64IDs, err := util.CheckReqSliceUint64(ctx, ids)
		if err != nil {
			return nil, err
		}
		rejectedQuestionByIDs, err = s.dao.GetRejectedQuestionByIDs(ctx, corpID, uint64IDs)
	} else {
		rejectBizIDS, err := util.CheckReqSliceUint64(ctx, req.GetRejectedBizIds())
		if err != nil {
			return nil, err
		}
		rejectedQuestionByIDs, err = s.dao.GetRejectedQuestionByBizIDs(ctx, corpID, rejectBizIDS)
	}
	if err != nil {
		return nil, err
	}
	notDeletedRejectedQA := make([]*model.RejectedQuestion, 0, len(rejectedQuestionByIDs))
	for _, rejectedQuestion := range rejectedQuestionByIDs {
		if rejectedQuestion.IsDelete() {
			continue
		}
		notDeletedRejectedQA = append(notDeletedRejectedQA, rejectedQuestion)
	}
	modifyRejectedQuestions, err := s.getPendingReleaseRejectedQuestion(ctx, corpID, app.ID, notDeletedRejectedQA)
	if err != nil {
		return nil, errs.ErrSystem
	}
	for _, rejectedQuestion := range notDeletedRejectedQA {
		if _, ok := modifyRejectedQuestions[rejectedQuestion.ID]; ok {
			return nil, errs.ErrRejectedQuestionIsPendingRelease
		}
	}
	err = s.dao.DeleteRejectedQuestion(ctx, corpID, app.ID, notDeletedRejectedQA)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

// ExportRejectedQuestion 导出拒答问题
func (s *Service) ExportRejectedQuestion(ctx context.Context, req *pb.ExportRejectedQuestionReq) (
	*pb.ExportRejectedQuestionRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ExportRejectedQuestionRsp)
	return rsp, nil
}

func (s *Service) getPendingReleaseRejectedQuestion(ctx context.Context, corpID, robotID uint64,
	rejectedQuestion []*model.RejectedQuestion) (map[uint64]*model.ReleaseRejectedQuestion, error) {
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return nil, nil
	}
	if latestRelease.IsPublishDone() {
		return nil, nil
	}
	modifyRejectedQuestions, err := s.dao.GetReleaseModifyRejectedQuestion(ctx, latestRelease, rejectedQuestion)
	if err != nil {
		return nil, err
	}
	return modifyRejectedQuestions, nil
}
