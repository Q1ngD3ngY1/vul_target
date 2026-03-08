package service

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

// GetUnsatisfiedReply 获取不满意回复
func (s *Service) GetUnsatisfiedReply(ctx context.Context, req *pb.GetUnsatisfiedReplyReq) (
	*pb.GetUnsatisfiedReplyRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetUnsatisfiedReplyRsp)
	return rsp, nil
}

// ListUnsatisfiedReply 获取不满意回复
func (s *Service) ListUnsatisfiedReply(ctx context.Context, req *pb.ListUnsatisfiedReplyReq) (
	*pb.ListUnsatisfiedReplyRsp, error) {
	log.InfoContextf(ctx, "ListUnsatisfiedReply Req:%+v", req)
	rsp := new(pb.ListUnsatisfiedReplyRsp)

	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	unsatisfiedReplyListReq := fillListUnsatisfiedReplyReq(req, app.CorpID, app.ID)
	total, err := s.dao.GetUnsatisfiedReplyTotal(ctx, unsatisfiedReplyListReq)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply total req:%+v, err:%+v", req, err)
		return rsp, errs.ErrSystem
	}
	rsp.Total = total
	if rsp.GetTotal() == 0 {
		return rsp, nil
	}
	list, err := s.dao.GetUnsatisfiedReplyList(ctx, unsatisfiedReplyListReq)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply list req:%+v, err:%+v", req, err)
		return rsp, errs.ErrSystem
	}

	var staffIDs []uint64
	for _, v := range list {
		staffIDs = append(staffIDs, v.StaffID)
	}
	staffNickNameMap, err := s.dao.GetStaffNickNameMapByIDs(ctx, staffIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	fillListUnsatisfiedReplyResult(rsp, list, staffNickNameMap)
	return rsp, nil
}

// fillListUnsatisfiedReplyReq TODO
func fillListUnsatisfiedReplyReq(req *pb.ListUnsatisfiedReplyReq, corpID,
	robotID uint64) *model.UnsatisfiedReplyListReq {
	return &model.UnsatisfiedReplyListReq{
		CorpID:   corpID,
		RobotID:  robotID,
		Query:    req.GetQuery(),
		Reasons:  req.GetReasons(),
		Page:     req.GetPageNumber(),
		PageSize: req.GetPageSize(),
		Status:   req.GetStatus(),
	}
}

// fillListUnsatisfiedReplyResult TODO
func fillListUnsatisfiedReplyResult(rsp *pb.ListUnsatisfiedReplyRsp, list []*model.UnsatisfiedReplyInfo, nickNameMap map[uint64]string) {

	for _, v := range list {
		rsp.List = append(rsp.List, &pb.ListUnsatisfiedReplyRsp_UnsatisfiedReply{
			ReplyBizId:  v.BusinessID,
			RecordBizId: v.RecordID,
			Question:    v.Question,
			Answer:      v.Answer,
			Reasons:     v.Reasons,
			Operator:    nickNameMap[v.StaffID],
			Status:      v.Status,
			CreateTime:  v.CreateTime.Unix(),
			UpdateTime:  v.UpdateTime.Unix(),
		})
	}
}

// IgnoreUnsatisfiedReply 忽略不满意回复
func (s *Service) IgnoreUnsatisfiedReply(ctx context.Context, req *pb.IgnoreUnsatisfiedReplyReq) (
	*pb.IgnoreUnsatisfiedReplyRsp, error) {
	staffID := pkg.StaffID(ctx)
	log.InfoContextf(ctx, "IgnoreUnsatisfiedReply Req:%+v,staffID:%+v", req, staffID)
	rsp := new(pb.IgnoreUnsatisfiedReplyRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	// API3.0逻辑 上云后删除if判断
	if len(req.GetReplyBizIds()) > 0 {
		var ids []uint64
		replyBizids, err := util.CheckReqSliceUint64(ctx, req.GetReplyBizIds())
		if err != nil {
			return nil, err
		}
		replys, err := s.dao.GetUnsatisfiedReplyByBizIDs(ctx, app.CorpID, app.ID, replyBizids)
		if err != nil {
			log.ErrorContextf(ctx, "get unsatisfied reply by ids req:%+v, err:%+v", req, err)
			return rsp, err
		}
		if len(replys) != len(req.GetReplyBizIds()) {
			log.ErrorContextf(ctx, "get unsatisfied reply by ids data not match req:%+v, err:%+v", req, err)
			return rsp, errs.ErrUnsatisfiedReplyNotFound
		}
		for _, reply := range replys {
			ids = append(ids, reply.ID)
		}
		if err = s.dao.UpdateUnsatisfiedReplyStatus(ctx, app.CorpID, app.ID, ids,
			model.UnsatisfiedReplyStatusWait, model.UnsatisfiedReplyStatusIgnore); err != nil {
			log.ErrorContextf(ctx, "update unsatisfied reply status req:%+v, err:%+v", req, err)
			return rsp, err
		}
		_ = s.dao.AddOperationLog(ctx, model.UnsatisfiedReplyIgnore, app.CorpID, app.ID, req, rsp, nil, nil)
		return rsp, nil
	}
	reqIDs, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return nil, err
	}
	replys, err := s.dao.GetUnsatisfiedReplyByIDs(ctx, app.CorpID, app.ID, reqIDs)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply by ids req:%+v, err:%+v", req, err)
		return rsp, err
	}
	if len(replys) != len(req.GetIds()) {
		log.ErrorContextf(ctx, "get unsatisfied reply by ids data not match req:%+v, err:%+v", req, err)
		return rsp, errs.ErrUnsatisfiedReplyNotFound
	}
	if err = s.dao.UpdateUnsatisfiedReplyStatus(ctx, app.CorpID, app.ID, reqIDs,
		model.UnsatisfiedReplyStatusWait, model.UnsatisfiedReplyStatusIgnore); err != nil {
		log.ErrorContextf(ctx, "update unsatisfied reply status req:%+v, err:%+v", req, err)
		return rsp, err
	}
	_ = s.dao.AddOperationLog(ctx, model.UnsatisfiedReplyIgnore, app.CorpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// GetUnsatisfiedReplyContext 获取不满意回复上下文
func (s *Service) GetUnsatisfiedReplyContext(ctx context.Context, req *pb.GetUnsatisfiedReplyContextReq) (
	*pb.GetUnsatisfiedReplyContextRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetUnsatisfiedReplyContextRsp)
	return rsp, nil
}

// DescribeUnsatisfiedReplyContext 获取不满意回复上下文
func (s *Service) DescribeUnsatisfiedReplyContext(ctx context.Context, req *pb.DescribeUnsatisfiedReplyReq) (
	*pb.DescribeUnsatisfiedReplyRsp, error) {
	rsp := new(pb.DescribeUnsatisfiedReplyRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	replyBizID, err := util.CheckReqParamsIsUint64(ctx, req.ReplyBizId)
	if err != nil {
		return rsp, err
	}
	unsatisfiedReply, err := s.getUnsatisfiedReplyByBizID(ctx, app.CorpID, app.ID, replyBizID)
	if err != nil {
		log.ErrorContextf(ctx, "get unsatisfied reply by id req:%+v, err:%+v", req, err)
		return rsp, err
	}
	if err = s.toFillUnsatisfiedReplyContextRsp(ctx, rsp, app, unsatisfiedReply.Context,
		unsatisfiedReply.UserType); err != nil {
		log.ErrorContextf(ctx, "fill unsatisfied reply context req:%+v, err:%+v", req, err)
		return rsp, err
	}
	return rsp, nil
}

// toFillUnsatisfiedReplyContextRsp 兼容老接口，上云后删除，需修改fillUnsatisfiedReplyContexts入参数和
func (s *Service) toFillUnsatisfiedReplyContextRsp(ctx context.Context, rsp *pb.DescribeUnsatisfiedReplyRsp,
	app *model.App, contextStr string, userType uint32) error {
	if len(contextStr) == 0 {
		return nil
	}
	contextList := make([]*pb.UnsatisfiedReplyContext, 0)
	if err := jsoniter.UnmarshalFromString(contextStr, &contextList); err != nil {
		log.ErrorContextf(ctx, "unsatisfied replay context unmarshal fail,context:%s,err:%+v", contextStr, err)
		return err
	}
	staffIDs := make([]uint64, 0)
	mapStaffID := make(map[uint64]struct{})
	for _, v := range contextList {
		if v.GetIsRobot() {
			continue
		}
		if _, ok := mapStaffID[v.GetFromId()]; ok {
			continue
		}
		// 为企业员工消息或者坐席消息
		mapStaffID[v.GetFromId()] = struct{}{}
		staffIDs = append(staffIDs, v.GetFromId())
	}
	corpStaffs, err := s.getCorpStaffByIDs(ctx, staffIDs, userType)
	if err != nil {
		log.ErrorContextf(ctx, "get corp staff info by ids fail,staffIDs:%+v,err:%+v", staffIDs, err)
		return err
	}
	mapCorpStaff := make(map[uint64]*model.CorpStaff)
	for _, v := range corpStaffs {
		mapCorpStaff[v.ID] = v
	}
	for _, v := range contextList {
		rsp.List = append(rsp.List, fillDescUnsatisfiedReplyRspContext(v, mapCorpStaff, app))
	}
	return nil
}

func (s *Service) getCorpStaffByIDs(ctx context.Context, ids []uint64, userType uint32) ([]*model.CorpStaff, error) {
	if userType == model.LoginUserExpType {
		users, err := s.dao.GetExpUserByIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		var corps []*model.CorpStaff
		for i := range users {
			corps = append(corps, &model.CorpStaff{
				ID:         users[i].ID,
				BusinessID: users[i].BusinessID,
				UserID:     users[i].ID,
				NickName:   users[i].NickName,
				Avatar:     users[i].Avatar,
				Cellphone:  users[i].Cellphone,
				Status:     model.StaffStatusValid,
				JoinTime:   users[i].CreateTime,
				CreateTime: users[i].CreateTime,
				UpdateTime: users[i].UpdateTime,
			})
		}
		return corps, nil
	}
	return s.dao.GetCorpStaffByIDs(ctx, ids)
}

// fillDescUnsatisfiedReplyRspContext 处理上下文数据
func fillDescUnsatisfiedReplyRspContext(contextInfo *pb.UnsatisfiedReplyContext,
	mapCorpStaff map[uint64]*model.CorpStaff, app *model.App) *pb.DescribeUnsatisfiedReplyRsp_Context {
	var nickName, avatar string
	if contextInfo.GetIsRobot() {
		nickName, avatar = app.GetName(model.AppTestScenes), app.GetAvatar(model.AppTestScenes)
	} else {
		nickName, avatar = getUnsatisfiedReplyContextRspContextNameAvatar(mapCorpStaff, contextInfo.GetFromId())
		if avatar == "" {
			avatar = "https://cdn.xiaowei.qq.com/static/default_avatar.png" // 使用默认头像
		}
		if nickName == "" {
			nickName = "访客"
		}
	}
	return &pb.DescribeUnsatisfiedReplyRsp_Context{
		RecordBizId: contextInfo.GetRecordId(),
		IsVisitor:   contextInfo.GetIsVisitor(),
		NickName:    nickName, // 昵称
		Avatar:      avatar,   // 头像
		Content:     contextInfo.GetContent(),
		FileInfos:   contextInfo.GetFileInfos(), // 文档信息
		ReplyMethod: contextInfo.ReplyMethod,
	}
}

// getUnsatisfiedReplyContextRspContextNameAvatar TODO
func getUnsatisfiedReplyContextRspContextNameAvatar(mapCorpStaff map[uint64]*model.CorpStaff,
	staffID uint64) (string, string) {
	corpStaffInfo, ok := mapCorpStaff[staffID]
	if !ok {
		return "", ""
	}
	return corpStaffInfo.NickName, corpStaffInfo.Avatar
}

// getUnsatisfiedReplyByBizID TODO
func (s *Service) getUnsatisfiedReplyByBizID(ctx context.Context, corpID, robotID, id uint64) (
	*model.UnsatisfiedReplyInfo, error) {
	unsatisfiedReplyListReq := &model.UnsatisfiedReplyListReq{
		CorpID:   corpID,
		RobotID:  robotID,
		BizIDs:   []uint64{id},
		Page:     1,
		PageSize: 1}
	list, err := s.dao.GetUnsatisfiedReplyList(ctx, unsatisfiedReplyListReq)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if len(list) == 0 {
		return nil, errs.ErrUnsatisfiedReplyNotFound
	}
	return list[0], nil
}

// ExportUnsatisfiedReply 导出不满意回复
func (s *Service) ExportUnsatisfiedReply(ctx context.Context, req *pb.ExportUnsatisfiedReplyReq) (
	*pb.ExportUnsatisfiedReplyRsp, error) {
	log.InfoContextf(ctx, "ExportUnsatisfiedReply Req:%+v", req)
	rsp := new(pb.ExportUnsatisfiedReplyRsp)
	staffID := pkg.StaffID(ctx)

	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	_, err = util.CheckReqSliceUint64(ctx, req.GetReplyBizIds())
	if err != nil {
		return rsp, err
	}
	paramStr, err := jsoniter.MarshalToString(req)
	if err != nil {
		log.ErrorContextf(ctx, "json marshl to string req:%+v, err:%+v", req, err)
		return rsp, err
	}

	now := time.Now()
	export := model.Export{
		CorpID:        app.CorpID,
		RobotID:       app.ID,
		CreateStaffID: staffID,
		TaskType:      model.ExportUnsatisfiedReplyTaskType,
		Name:          model.ExportUnsatisfiedReplyTaskName,
		Params:        paramStr,
		Status:        model.TaskExportStatusInit,
		UpdateTime:    now,
		CreateTime:    now,
	}

	params := model.ExportParams{
		CorpID:           app.CorpID,
		RobotID:          app.ID,
		CreateStaffID:    staffID,
		TaskType:         model.ExportUnsatisfiedReplyTaskType,
		TaskName:         model.ExportUnsatisfiedReplyTaskName,
		Params:           paramStr,
		NoticeContent:    i18n.Translate(ctx, model.UnsatisfiedReplyNoticeContent),
		NoticePageID:     model.NoticeUnsatisfiedReplyPageID,
		NoticeTypeExport: model.NoticeTypeUnsatisfiedReplyExport,
		NoticeContentIng: i18n.Translate(ctx, model.UnsatisfiedReplyNoticeContentIng),
		Language:         i18n.GetUserLang(ctx),
	}

	if _, err = s.dao.CreateExportTask(ctx, app.CorpID, staffID, app.ID, export, params); err != nil {
		log.ErrorContextf(ctx, "create export task req:%+v, err:%+v", req, err)
		return rsp, err
	}
	return rsp, nil
}
