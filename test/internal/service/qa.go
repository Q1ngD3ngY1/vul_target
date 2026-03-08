package service

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
)

// ListRejectedQuestion 获取拒答问题列表
func (s *Service) ListRejectedQuestion(ctx context.Context, req *pb.ListRejectedQuestionReq) (*pb.ListRejectedQuestionRsp, error) {
	logx.I(ctx, "ListRejectedQuestion Req:%+v", req)
	if req.GetPageNumber() < 1 || req.GetPageSize() > 200 {
		return nil, errs.ErrPageNumberInvalid
	}
	rsp := new(pb.ListRejectedQuestionRsp)
	appid := convx.Uint64ToString(req.GetBotBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, appid)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	getRejectedQuestionListReq := &qaEntity.RejectedQuestionFilter{
		CorpID:         app.CorpPrimaryId,
		RobotID:        app.PrimaryId,
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		Query:          req.GetQuery(),
		Actions:        req.GetActions(),
		IsDeleted:      qaEntity.RejectedQuestionIsNotDeleted,
		OrderColumn:    []string{qaEntity.RejectedQuestionTblColUpdateTime, qaEntity.RejectedQuestionTblColId},
		OrderDirection: []string{util.SqlOrderByDesc, util.SqlOrderByDesc},
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
	req *qaEntity.RejectedQuestionFilter) (uint64, []*pb.ListRejectedQuestionRsp_RejectedQuestions, error) {
	list, total, err := s.qaLogic.ListRejectedQuestion(ctx, req)
	if err != nil {
		return 0, nil, err
	}
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, req.CorpID, req.RobotID)
	if err != nil {
		return 0, nil, err
	}
	// 获取员工名称
	var staffIDs []uint64
	for _, v := range list {
		staffIDs = append(staffIDs, v.CreateStaffID)
	}
	describeCorpStaffListReq := pm.DescribeCorpStaffListReq{
		Status:          []uint32{entity.CorpStatusValid},
		StaffPrimaryIds: staffIDs,
		Page:            1,
		PageSize:        uint32(len(staffIDs)),
		CorpId:          contextx.Metadata(ctx).CorpBizID(),
	}
	staffs, _, err := s.rpc.PlatformAdmin.DescribeCorpStaffList(ctx, &describeCorpStaffListReq)
	if err != nil { // 失败降级为返回员工ID
		logx.E(ctx, "ListQA get staff name staffIDs:%v, error:%v", staffIDs, err)
	}
	logx.I(ctx, "DescribeCorpStaffList, staffs:%+v", staffs)
	staffIdMap := slicex.MapKV(staffs, func(i *entity.CorpStaff) (uint64, string) { return i.ID, i.NickName })
	rejectedQuestions := make([]*pb.ListRejectedQuestionRsp_RejectedQuestions, 0, len(list))
	for _, v := range list {
		rejectedQuestions = append(rejectedQuestions, &pb.ListRejectedQuestionRsp_RejectedQuestions{
			RejectedBizId: v.BusinessID,
			Question:      v.Question,
			Status:        v.ReleaseStatus,
			StatusDesc:    i18n.Translate(ctx, v.StatusDesc(latestRelease.IsPublishPause())),
			Operator:      staffIdMap[v.CreateStaffID],
			UpdateTime:    v.UpdateTime.Unix(),
			IsAllowDelete: v.IsAllowDeleted(),
			IsAllowEdit:   v.IsAllowEdit(),
		})
	}
	return uint64(total), rejectedQuestions, nil
}

// CreateRejectedQuestion 创建拒答问题
func (s *Service) CreateRejectedQuestion(ctx context.Context, req *pb.CreateRejectedQuestionReq) (
	*pb.CreateRejectedQuestionRsp, error) {
	logx.I(ctx, "CreateRejectedQuestion Req:%+v", req)
	rsp := new(pb.CreateRejectedQuestionRsp)
	staffID := contextx.Metadata(ctx).StaffID()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
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

		replyReq := &appconfig.DescribeUnsatisfiedReplyListReq{
			BotBizId:    convx.Uint64ToString(app.BizId),
			BusinessIds: []uint64{businessID},
		}

		replyRsp, err := s.rpc.AppAdmin.DescribeUnsatisfiedReplyList(ctx, replyReq)
		// reply, err := s.dao.GetUnsatisfiedReplyByBizIDs(ctx, app.CorpPrimaryId, app.ID, []uint64{businessID})
		if err != nil {
			return rsp, errs.ErrSystem
		}
		reply := replyRsp.List
		if len(reply) == 0 {
			return rsp, errs.ErrUnsatisfiedReplyNotFound
		}
		replyID = reply[0].ReplyBizId
	}
	rejectedQuestion := &qaEntity.RejectedQuestion{
		BusinessID:       idgen.GetId(),
		CorpID:           app.CorpPrimaryId,
		RobotID:          app.PrimaryId,
		CreateStaffID:    staffID,
		Question:         req.GetQuestion(),
		BusinessSourceID: replyID,
		BusinessSource:   req.GetBusinessSource(),
	}
	err = s.qaLogic.CreateRejectedQuestion(ctx, rejectedQuestion)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	if rejectedQuestion.BusinessSource == qaEntity.BusinessSourceUnsatisfiedReply {
		auditx.Modify(auditx.BizUnsatisfactoryQuestion).Space(app.SpaceId).App(app.BizId).
			Log(ctx, i18n.Translate(ctx, i18nkey.KeyUnsatisfiedReject, req.GetQuestion()))
	} else {
		auditx.Create(auditx.BizRejectQuestion).Space(app.SpaceId).App(app.BizId).
			Log(ctx, req.GetQuestion())
	}

	return rsp, nil
}

// ModifyRejectedQuestion 修改拒答问题
func (s *Service) ModifyRejectedQuestion(ctx context.Context, req *pb.ModifyRejectedQuestionReq) (
	*pb.ModifyRejectedQuestionRsp, error) {
	logx.I(ctx, "ModifyRejectedQuestion Req:%+v", req)
	rsp := new(pb.ModifyRejectedQuestionRsp)
	staffID := contextx.Metadata(ctx).StaffID()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var rejectedQuestion *qaEntity.RejectedQuestion
	if len(req.Id) > 0 {
		var rejectedQuestionId uint64
		rejectedQuestionId, err = util.CheckReqParamsIsUint64(ctx, req.GetId())
		if err != nil {
			return nil, err
		}
		rejectedQuestion, err = s.qaLogic.GetRejectedQuestionByID(ctx, app.CorpPrimaryId, app.PrimaryId, rejectedQuestionId)
	} else {
		var rejectedQuestionBizId uint64
		rejectedQuestionBizId, err = util.CheckReqParamsIsUint64(ctx, req.GetRejectedBizId())
		if err != nil {
			return nil, err
		}
		rejectedQuestion, err = s.qaLogic.GetRejectedQuestionByBizID(ctx, app.CorpPrimaryId, app.PrimaryId, rejectedQuestionBizId)
	}
	if err != nil {
		return nil, errs.ErrRejectedQuestionNotFound
	}
	modifyRejectedQuestions, err := s.getPendingReleaseRejectedQuestion(ctx, app.CorpPrimaryId, app.PrimaryId, []*qaEntity.RejectedQuestion{rejectedQuestion})
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
		rejectedQuestion.ReleaseStatus = qaEntity.RejectedQuestionReleaseStatusInit
	}
	if isNeedPublish && rejectedQuestion.Action != qaEntity.RejectedQuestionAdd {
		rejectedQuestion.Action = qaEntity.RejectedQuestionUpdate
	}

	updateRejectedQuestion := &qaEntity.RejectedQuestion{
		CorpID:        app.CorpPrimaryId,
		CreateStaffID: staffID,
		RobotID:       app.PrimaryId,
		ID:            rejectedQuestion.ID,
		Question:      req.GetQuestion(),
		Action:        rejectedQuestion.Action,
		ReleaseStatus: rejectedQuestion.ReleaseStatus,
	}
	err = s.qaLogic.UpdateRejectedQuestion(ctx, updateRejectedQuestion, isNeedPublish)
	if err != nil {
		return rsp, err
	}
	auditx.Modify(auditx.BizRejectQuestion).Space(app.SpaceId).App(app.BizId).
		Log(ctx, i18n.Translate(ctx, i18nkey.KeyRejectEdit, rejectedQuestion.Question, req.GetQuestion()))
	return rsp, nil
}

// DeleteRejectedQuestion 删除拒答问题
func (s *Service) DeleteRejectedQuestion(ctx context.Context, req *pb.DeleteRejectedQuestionReq) (*pb.DeleteRejectedQuestionRsp, error) {
	logx.I(ctx, "DeleteRejectedQuestion Req:%+v", req)
	rsp := new(pb.DeleteRejectedQuestionRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var rejectedQuestionByIDs []*qaEntity.RejectedQuestion

	ids := slicex.Unique(req.GetIds())
	if len(ids) > 0 {
		uint64IDs, err := util.CheckReqSliceUint64(ctx, ids)
		if err != nil {
			return nil, err
		}
		rejectedQuestionByIDs, err = s.qaLogic.GetRejectedQuestionByIDs(ctx, app.CorpPrimaryId, uint64IDs)
	} else {
		rejectBizIDS, err := util.CheckReqSliceUint64(ctx, req.GetRejectedBizIds())
		if err != nil {
			return nil, err
		}
		rejectedQuestionByIDs, err = s.qaLogic.GetRejectedQuestionByBizIDs(ctx, app.CorpPrimaryId, rejectBizIDS)
	}
	if err != nil {
		return nil, err
	}
	notDeletedRejectedQA := make([]*qaEntity.RejectedQuestion, 0, len(rejectedQuestionByIDs))
	delBizQuestions := make([]qaEntity.RejectBizQuestion, 0)
	for _, rejectedQuestion := range rejectedQuestionByIDs {
		if rejectedQuestion.IsDelete() {
			continue
		}
		notDeletedRejectedQA = append(notDeletedRejectedQA, rejectedQuestion)
		delBizQuestions = append(delBizQuestions,
			qaEntity.RejectBizQuestion{BizID: rejectedQuestion.BusinessID, Question: rejectedQuestion.Question})
	}
	modifyRejectedQuestions, err := s.getPendingReleaseRejectedQuestion(ctx, app.CorpPrimaryId, app.PrimaryId, notDeletedRejectedQA)
	if err != nil {
		return nil, errs.ErrSystem
	}
	for _, rejectedQuestion := range notDeletedRejectedQA {
		if _, ok := modifyRejectedQuestions[rejectedQuestion.ID]; ok {
			return nil, errs.ErrRejectedQuestionIsPendingRelease
		}
	}
	err = s.qaLogic.DeleteRejectedQuestion(ctx, app.CorpPrimaryId, app.PrimaryId, notDeletedRejectedQA)
	if err != nil {
		return rsp, err
	}

	//  上报操作日志
	for _, v := range delBizQuestions {
		auditx.Delete(auditx.BizRejectQuestion).Space(app.SpaceId).App(app.BizId).Log(ctx, v.Question)
	}
	return rsp, nil
}

func (s *Service) getPendingReleaseRejectedQuestion(ctx context.Context, corpID, robotID uint64,
	rejectedQuestion []*qaEntity.RejectedQuestion) (map[uint64]*releaseEntity.ReleaseRejectedQuestion, error) {
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return nil, nil
	}
	if latestRelease.IsPublishDone() {
		return nil, nil
	}
	modifyRejectedQuestions, err := s.releaseLogic.GetReleaseModifyRejectedQuestion(ctx, latestRelease, rejectedQuestion)
	if err != nil {
		return nil, err
	}
	return modifyRejectedQuestions, nil
}
