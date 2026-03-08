package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetUnreleasedDbCount 获取外部数据库模块未发布的数量
func (s *Service) GetUnreleasedDbCount(ctx context.Context, req *pb.GetUnreleasedDbCountReq) (
	*pb.GetUnreleasedDbCountRsp, error) {
	dbSources, err := dao.GetDBSourceDao().GetUnreleasedDBSource(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	dbTables, err := dao.GetDBTableDao().GetUnreleasedDBTable(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	rsp := &pb.GetUnreleasedDbCountRsp{Count: int32(len(dbSources) + len(dbTables))}
	log.InfoContextf(ctx, "GetUnreleasedDbCount req: %+v, rsp: %+v", req, rsp)
	return rsp, nil
}

// SendPublishDbTaskEvent 发送发布数据库任务事件, 任务采集、任务发布、任务暂停重试
func (s *Service) SendPublishDbTaskEvent(ctx context.Context, req *pb.SendPublishDbTaskEventReq) (
	*pb.SendPublishDbTaskEventRsp, error) {
	log.InfoContextf(ctx, "SendPublishDbTaskEvent req: %+v", req)
	if req.GetEvent() == model.TaskConfigEventCollect {
		// 收到采集事件，将信息同步到快照表
		err := dao.GetDBSourceDao().CollectUnreleasedDBSource(ctx, req.GetAppBizId(), req.GetReleaseBizId())
		if err != nil {
			return nil, err
		}
		err = dao.GetDBTableDao().CollectUnreleasedDBTable(ctx, req.GetAppBizId(), req.GetReleaseBizId())
		if err != nil {
			return nil, err
		}
	} else if req.GetEvent() == model.TaskConfigEventRelease || req.GetEvent() == model.TaskConfigEventRetry {
		app, err := client.GetAppInfo(ctx, req.GetAppBizId(), model.AppTestScenes)
		if err != nil {
			return nil, err
		}

		_, err = dao.NewReleaseDBTask(ctx, &model.ReleaseDBParams{
			CorpBizID:    app.CorpBizId,
			RobotID:      app.Id,
			AppBizID:     req.GetAppBizId(),
			ReleaseBizID: req.GetReleaseBizId(),
		})
		if err != nil {
			return nil, err
		}
	} else {
		// 暂停事件不处理
		log.InfoContextf(ctx, "receive %v, ignore", req.GetEvent())
	}

	return &pb.SendPublishDbTaskEventRsp{}, nil
}

// GetPublishDbTask 获取发布数据库任务详情，admin用于判断对应的模块当前是否有发布任务处理中，做幂等性处理
func (s *Service) GetPublishDbTask(ctx context.Context, req *pb.GetPublishDbTaskReq) (*pb.GetPublishDbTaskRsp, error) {
	releaseDBSource, err := dao.GetDBSourceDao().GetAllReleaseDBSources(ctx, req.GetAppBizId(), req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	releaseTable, err := dao.GetDBTableDao().GetAllReleaseDBTables(ctx, req.GetAppBizId(), req.GetReleaseBizId(), true)
	if err != nil {
		return nil, err
	}
	count := len(releaseDBSource) + len(releaseTable)
	rsp := &pb.GetPublishDbTaskRsp{
		ReleaseBizId: 0,
		Count:        int32(count),
	}

	if count == 0 {
		log.InfoContextf(ctx, "%v nothing to release", req.GetReleaseBizId())
		return rsp, nil
	}
	rsp.ReleaseBizId = req.GetReleaseBizId()
	return rsp, nil
}
