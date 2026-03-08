package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetAdminTaskList 获取admin任务列表
func (s *Service) GetAdminTaskList(ctx context.Context, req *pb.GetAdminTaskListReq) (*pb.GetAdminTaskListRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetAdminTaskListRsp)
	return rsp, nil
}

// GetAdminTaskHistoryList 获取admin历史任务列表
func (s *Service) GetAdminTaskHistoryList(ctx context.Context, req *pb.GetAdminTaskHistoryListReq) (
	*pb.GetAdminTaskHistoryListRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetAdminTaskHistoryListRsp)
	return rsp, nil
}

// GetVectorDocTaskList 获取获取vector_doc任务列表
func (s *Service) GetVectorDocTaskList(ctx context.Context, req *pb.GetVectorDocTaskListReq) (
	*pb.GetVectorDocTaskListRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)

	rsp := new(pb.GetVectorDocTaskListRsp)
	app, err := s.dao.GetAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		log.ErrorContextf(ctx, "get robot req:%+v, err:%+v", req, err)
		return rsp, err
	}
	if app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	total, err := s.dao.GetTaskTotal(ctx, dao.VectorDocTaskTable, app.ID, req.GetTaskType())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.Total = total
	if rsp.GetTotal() == 0 {
		return rsp, nil
	}
	list, err := s.dao.GetTaskList(ctx, dao.VectorDocTaskTable, app.ID, req.GetTaskType(), req.GetPage(),
		req.GetPageSize())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, v := range list {
		rsp.List = append(rsp.List, &pb.TaskInfo{
			Id:             v.ID,
			RobotId:        v.UserID,
			TaskType:       v.TaskType,
			TaskMutex:      v.TaskMutex,
			Params:         v.Params,
			RetryTimes:     v.RetryTimes,
			MaxRetryTimes:  v.MaxRetryTimes,
			Timeout:        v.Timeout,
			Runner:         v.Runner,
			RunnerInstance: v.RunnerInstance,
			Result:         v.Result,
			TraceId:        v.TraceID,
			StartTime:      v.StartTime.Unix(),
			EndTime:        v.EndTime.Unix(),
			NextStartTime:  v.NextStartTime.Unix(),
			CreateTime:     v.CreateTime.Unix(),
			UpdateTime:     v.UpdateTime.Unix(),
		})
	}
	return rsp, nil
}

// GetVectorDocTaskHistoryList 获取vector_doc任务历史列表
func (s *Service) GetVectorDocTaskHistoryList(ctx context.Context, req *pb.GetVectorDocTaskHistoryListReq) (
	*pb.GetVectorDocTaskHistoryListRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)

	rsp := new(pb.GetVectorDocTaskHistoryListRsp)
	app, err := s.dao.GetAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		log.ErrorContextf(ctx, "get robot req:%+v, err:%+v", req, err)
		return rsp, err
	}
	if app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	total, err := s.dao.GetTaskHistoryTotal(ctx, dao.VectorDocTaskTable, app.ID, req.GetTaskType())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.Total = total
	if rsp.GetTotal() == 0 {
		return rsp, nil
	}
	list, err := s.dao.GetTaskHistoryList(ctx, dao.VectorDocTaskTable, app.ID, req.GetTaskType(),
		req.GetPage(), req.GetPageSize())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, v := range list {
		rsp.List = append(rsp.List, &pb.TaskHistoryInfo{
			Id:             v.ID,
			RobotId:        v.UserID,
			TaskType:       v.TaskType,
			TaskMutex:      v.TaskMutex,
			Params:         v.Params,
			RetryTimes:     v.RetryTimes,
			MaxRetryTimes:  v.MaxRetryTimes,
			Timeout:        v.Timeout,
			Runner:         v.Runner,
			RunnerInstance: v.RunnerInstance,
			Result:         v.Result,
			IsSuccess:      v.IsSuccess,
			TraceId:        v.TraceID,
			StartTime:      v.StartTime.Unix(),
			EndTime:        v.EndTime.Unix(),
			NextStartTime:  v.NextStartTime.Unix(),
			CreateTime:     v.CreateTime.Unix(),
		})
	}
	return rsp, nil
}
