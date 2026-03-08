package api

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/database"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// GetUnreleasedDbCount 获取外部数据库模块未发布的数量
func (s *Service) GetUnreleasedDbCount(ctx context.Context,
	req *pb.GetUnreleasedDbCountReq) (*pb.GetUnreleasedDbCountRsp, error) {
	dbSources, err := s.dbLogic.GetUnreleasedDBSource(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	dbTables, err := s.dbLogic.GetUnreleasedDBTable(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	rsp := &pb.GetUnreleasedDbCountRsp{Count: int32(len(dbSources) + len(dbTables))}
	logx.I(ctx, "GetUnreleasedDbCount req: %+v, rsp: %+v", req, rsp)
	return rsp, nil
}

// SendPublishDbTaskEvent 发送发布数据库任务事件, 任务采集、任务发布、任务暂停重试
func (s *Service) SendPublishDbTaskEvent(ctx context.Context, req *pb.SendPublishDbTaskEventReq) (
	*pb.SendPublishDbTaskEventRsp, error) {
	logx.I(ctx, "SendPublishDbTaskEvent req: %+v", req)
	if req.GetEvent() == entity.TaskConfigEventCollect {
		// 收到采集事件，将信息同步到快照表
		err := s.dbLogic.CollectUnreleasedDBSource(ctx, req.GetAppBizId(), req.GetReleaseBizId())
		if err != nil {
			return nil, err
		}
		err = s.dbLogic.CollectUnreleasedDBTable(ctx, req.GetAppBizId(), req.GetReleaseBizId())
		if err != nil {
			return nil, err
		}
	} else if req.GetEvent() == entity.TaskConfigEventRelease || req.GetEvent() == entity.TaskConfigEventRetry {
		appDB, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
		if err != nil {
			return nil, err
		}

		_, err = scheduler.NewReleaseDBTask(ctx, &entity.ReleaseDBParams{
			CorpBizID:    appDB.CorpPrimaryId,
			RobotID:      appDB.PrimaryId,
			AppBizID:     req.GetAppBizId(),
			ReleaseBizID: req.GetReleaseBizId(),
		})
		if err != nil {
			return nil, err
		}
	} else {
		// 暂停事件不处理
		logx.I(ctx, "receive %v, ignore", req.GetEvent())
	}

	return &pb.SendPublishDbTaskEventRsp{}, nil
}

// GetPublishDbTask 获取发布数据库任务详情，admin用于判断对应的模块当前是否有发布任务处理中，做幂等性处理
func (s *Service) GetPublishDbTask(ctx context.Context, req *pb.GetPublishDbTaskReq) (*pb.GetPublishDbTaskRsp, error) {
	releaseDBSource, err := s.dbLogic.GetAllReleaseDBSources(ctx, req.GetAppBizId(), req.GetReleaseBizId())
	if err != nil {
		return nil, err
	}
	releaseTable, err := s.dbLogic.GetAllReleaseDBTables(ctx, req.GetAppBizId(), req.GetReleaseBizId(), true)
	if err != nil {
		return nil, err
	}
	count := len(releaseDBSource) + len(releaseTable)
	rsp := &pb.GetPublishDbTaskRsp{
		ReleaseBizId: 0,
		Count:        int32(count),
	}

	if count == 0 {
		logx.I(ctx, "%v nothing to release", req.GetReleaseBizId())
		return rsp, nil
	}
	rsp.ReleaseBizId = req.GetReleaseBizId()
	return rsp, nil
}

// ExecuteSqlForDbSource 针对用户添加的数据库执行SQL
func (s *Service) ExecuteSqlForDbSource(ctx context.Context,
	req *pb.ExecuteSqlForDbSourceReq) (*pb.ExecuteSqlForDbSourceRsp, error) {
	// 2. 获取应用信息
	app, err := s.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "ExecuteSqlForDbSource GetAppInfo fail, err=%+v", err)
		return nil, err
	}
	// 3. 验证SQL表权限
	executeTables, err := s.dbLogic.ValidateSqlTables(ctx, app, req)
	if err != nil {
		return nil, err
	}
	// 4. 执行SQL并获取结果
	columns, data, effCnt, message, err := s.dbLogic.RunSql(ctx, req.GetDbSourceBizId(),
		req.GetSqlToExecute(), req.GetSqlParams())
	if err != nil {
		logx.W(ctx, "ExecuteSqlForDbSource run sql failed", "err", err)
		return nil, err
	}
	// 5. 构建列详细信息
	columnInfos, err := s.dbLogic.BuildColumnInfos(ctx, app, req, executeTables, columns)
	if err != nil {
		return nil, err
	}
	// 6. 构建返回结果
	return &pb.ExecuteSqlForDbSourceRsp{
		PreviewTableRsp: &pb.PreviewTableRsp{
			Columns:     columns,
			Rows:        data,
			Total:       int32(len(data)),
			ColumnsInfo: columnInfos,
		},
		EffectRowCount: int32(effCnt),
		ErrorMessage:   message,
	}, nil
}

// TextToSQLFromKnowledge 根据用户提问生成 SQL
func (s *Service) TextToSQLFromKnowledge(ctx context.Context, req *pb.TextToSQLFromKnowledgeReq) (
	*pb.TextToSQLFromKnowledgeRsp, error) {
	return s.dbLogic.TextToSQLFromKnowledge(ctx, req)
}

// ListDbSourceBizIDsWithTableBizIDs 获取数据库源和数据表业务ID
func (s *Service) ListDbSourceBizIDsWithTableBizIDs(ctx context.Context, req *pb.ListDbSourceBizIDsWithTableBizIDsReq) (
	*pb.ListDbSourceBizIDsWithTableBizIDsRsp, error) {
	if req.GetDbSourceBizId() == 0 {
		// 如果传入了appBizID，则查询该应用下的数据库源
		dbSources, total, err := s.dbLogic.ListDbSourcesWithTables(ctx, req.GetAppBizId(), req.GetPageSize(),
			req.GetPageNumber())
		if err != nil {
			logx.E(ctx, "ListDbSourceBizIDsWithTableBizIDs|batch get fail: %v, appBizID:%v", err,
				req.GetAppBizId())
			return nil, err
		}

		rsp := &pb.ListDbSourceBizIDsWithTableBizIDsRsp{
			DbSources: databasesWithTableToPB(dbSources),
			Total:     int32(total),
		}
		return rsp, nil
	} else {
		// 否则查询单一数据库源
		dbSource, err := s.dbLogic.GetDbSourcesWithTables(ctx, req.GetDbSourceBizId())
		if err != nil {
			logx.E(ctx, "ListDbSourceBizIDsWithTableBizIDs|single get failed: %v, dbSourceBizID:%v", err,
				req.GetDbSourceBizId())
			return nil, err
		}
		rsp := &pb.ListDbSourceBizIDsWithTableBizIDsRsp{
			DbSources: databasesWithTableToPB([]*database.Database{dbSource}),
			Total:     int32(1),
		}
		return rsp, nil
	}
}

func databasesWithTableToPB(dbSources []*database.Database) []*pb.DbSourceBizIDItem {
	var result []*pb.DbSourceBizIDItem
	for _, db := range dbSources {
		view := &pb.DbSourceBizIDItem{
			DbSourceBizId: db.DBSourceBizID,
		}
		for _, dbTable := range db.Tables {
			view.DbSourceTableBizId = append(view.DbSourceTableBizId, dbTable.DBTableBizID)
		}
		result = append(result, view)
	}
	return result
}
