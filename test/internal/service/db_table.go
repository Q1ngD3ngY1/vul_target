package service

import (
	"context"
	"slices"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"google.golang.org/protobuf/proto"
)

// DeleteDbTable 删除单一表数据
func (s *Service) DeleteDbTable(ctx context.Context, req *pb.DeleteDbTableReq) (*pb.DeleteDbTableRsp, error) {
	corpBizID := pkg.CorpBizID(ctx)
	// 获取 robotId
	robotId, err := db_source.GetRobotIdByAppBizId(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	//获取table记录
	table, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbTableBizId())
	if err != nil {
		log.ErrorContextf(ctx, "get table err:%v,req:%+v", err, req)
		return nil, err
	}
	//删除table记录
	err = db_source.DeleteTableAndColumn(ctx, corpBizID, robotId, req.GetAppBizId(), req.DbTableBizId, s.dao)
	if err != nil {
		log.ErrorContextf(ctx, "soft delete db table failed: %v", err)
		return nil, err
	}
	//更新数据库修改人
	err = dao.GetDBSourceDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), table.DBSourceBizID,
		[]string{"staff_id"}, &model.DBSource{StaffID: pkg.StaffID(ctx)})
	if err != nil {
		log.ErrorContextf(ctx, "update db source failed: %v", err)
		return nil, err
	}
	return &pb.DeleteDbTableRsp{}, nil
}

// GetDbTable 查询单一表数据
func (s *Service) GetDbTable(ctx context.Context, req *pb.GetDbTableReq) (*pb.GetDbTableRsp, error) {
	corpBizID := pkg.CorpBizID(ctx)
	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.AppBizId, req.DbTableBizId)
	if err != nil {
		return nil, err
	}
	dbTableView, err := db_source.DBTableModelToView(dbTable, false)
	if err != nil {
		return nil, err
	}
	return &pb.GetDbTableRsp{
		DbTable: dbTableView,
	}, nil
}

// ListDbTable 分页查询表数据
func (s *Service) ListDbTable(ctx context.Context, req *pb.ListDbTableReq) (*pb.ListDbTableRsp, error) {
	corpBizID := pkg.CorpBizID(ctx)
	var (
		dbTables []*model.DBTable
		total    int64
		err      error
	)

	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.AppBizId, req.DbSourceBizId)
	if err != nil {
		return nil, err
	}

	opt := &dao.ListDBTablesOption{
		CorpBizID:     corpBizID,
		AppBizID:      req.AppBizId,
		DBSourceBizID: req.DbSourceBizId,
		TableName:     req.FilterTableName,
		Page:          int(req.PageNumber),
		PageSize:      int(req.PageSize),
	}
	for _, filter := range req.GetFilters() {
		if filter.FilterKey == "IsEnable" {
			if slices.Contains(filter.FilterValue, "true") {
				opt.IsEnable = proto.Bool(true)
			} else {
				opt.IsEnable = proto.Bool(false)
			}
		}
	}
	dbTables, total, err = dao.GetDBTableDao().ListByOption(ctx, opt)
	if err != nil {
		return nil, errs.ErrDataNotExistOrIsDeleted
	}

	// 异步同步数据表信息
	go func() {
		syncCtx := pkg.NewCtxWithTraceID(ctx)
		syncCtx, cancel := context.WithTimeout(syncCtx, 3*time.Minute)
		defer cancel()
		for _, dbTable := range dbTables {
			_, err = db_source.FlashTableAndColumn(syncCtx, dbTable, false, s.dao)
			if err != nil {
				log.ErrorContextf(ctx, "FlashTableAndColumn failed: %v", err)
			}
		}
	}()

	// 获取员工名称
	staffIDs := make([]uint64, 0, len(dbTables))
	for _, v := range dbTables {
		staffIDs = append(staffIDs, v.StaffID)
	}
	staffByID, err := client.ListCorpStaffByIds(ctx, corpBizID, staffIDs)
	if err != nil { //失败降级为返回员工ID
		log.ErrorContextf(ctx, "ListDbTable get staff name err:%v,staffIDs:%v", err, staffIDs)
	}

	appIsShared := make(map[uint64]bool)

	for _, dbTable := range dbTables {
		appIsShared[dbTable.AppBizID] = false
	}
	for k, _ := range appIsShared {
		info, err := client.GetAppInfo(ctx, k, model.AppTestScenes)
		if err != nil {
			log.ErrorContextf(ctx, "ListDbTable get app info err:%v,appBizID:%v", err, k)
			continue
		}
		if info.IsShareKnowledgeBase {
			appIsShared[k] = true
		}
	}
	dbTableViews, err := db_source.DBTableModelToViews(dbTables, staffByID, appIsShared)
	if err != nil {
		log.ErrorContextf(ctx, "ListDbTable failed: %v", err)
		return nil, errs.ErrTypeConvertFail
	}
	return &pb.ListDbTableRsp{
		DbName: dbSource.DBName,
		List:   dbTableViews,
		Alive:  dbSource.Alive,
		Total:  int32(total),
	}, nil
}

// ListReleaseDbTable 发布数据表查看
func (s *Service) ListReleaseDbTable(ctx context.Context,
	req *pb.ListReleaseDbDbTableReq) (*pb.ListReleaseDbDbTableRsp, error) {
	var list []*pb.ReleaseDbTable
	log.DebugContextf(ctx, "ListReleaseDbTable, req: %+v", req)
	corpBizID := pkg.CorpBizID(ctx)
	if req.GetReleaseBizId() == 0 {
		var startTime, endTime time.Time
		if req.GetStartTime() != 0 {
			startTime = time.Unix(req.GetStartTime(), 0)
		}
		if req.GetEndTime() != 0 {
			endTime = time.Unix(req.GetEndTime(), 0)
		}
		tables, err := dao.GetDBTableDao().FindUnReleaseDBTableByConditions(ctx, corpBizID, req.GetAppBizId(), req.GetQuery(),
			startTime, endTime, req.GetActions(), req.GetPageNumber(), req.GetPageSize())
		if err != nil {
			return nil, err
		}

		for _, table := range tables {
			name := table.Name
			dbName := table.TableSchema
			if table.Source == model.TableSourceDoc {
				// 不能把内部的库表名称暴露出去
				name = table.AliasName
				dbName = ""
			}
			list = append(list, &pb.ReleaseDbTable{
				DbTableBizId: table.DBTableBizID,
				TableName:    name,
				DbName:       dbName,
				UpdateTime:   uint64(table.UpdateTime.Unix()),
				Action:       uint32(table.NextAction),
				ActionDesc:   model.ActionDesc(ctx, uint32(table.NextAction)),
			})
		}
	} else {
		releaseTable, err := dao.GetDBTableDao().GetAllReleaseDBTables(ctx, req.GetAppBizId(), req.GetReleaseBizId(), false)
		if err != nil {
			return nil, err
		}

		for _, release := range releaseTable {
			name := release.Name
			dbName := release.TableSchema
			if release.Source == model.TableSourceDoc {
				name = release.AliasName
				dbName = ""
			}
			list = append(list, &pb.ReleaseDbTable{
				DbTableBizId: release.DBTableBizID,
				TableName:    name,
				DbName:       dbName,
				UpdateTime:   uint64(release.TableModifiedTime.Unix()),
				Action:       uint32(release.Action),
				ActionDesc:   model.ActionDesc(ctx, uint32(release.Action)),
			})
		}
	}
	rsp := &pb.ListReleaseDbDbTableRsp{
		Total: int32(len(list)),
		List:  list,
	}

	return rsp, nil
}

func (s *Service) UpdateDbTableEnabled(ctx context.Context, req *pb.UpdateDbTableEnabledReq) (*pb.UpdateDbTableEnabledRsq, error) {
	corpBizID := pkg.CorpBizID(ctx)
	robotId, err := db_source.GetRobotIdByAppBizId(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	var dbTablesBizIDs []uint64
	cols := []string{"learn_status", "staff_id", "is_indexed"}
	if req.GetDbTableBizId() > 0 {
		dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbTableBizId())
		if err != nil {
			return nil, err
		}
		if dbTable.IsIndexed == req.GetIsEnable() {
			return &pb.UpdateDbTableEnabledRsq{}, nil
		}
		if dbTable.LearnStatus == model.LearnStatusLearning {
			return nil, errs.ErrDbTableStatus
		}
		dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), dbTable.DBSourceBizID)
		if err != nil {
			return nil, err
		}
		if !dbSource.IsIndexed {
			return nil, errs.ErrDbSourceNotEnabled
		}
		dbTable.LearnStatus = model.LearnStatusLearning
		dbTable.StaffID = pkg.StaffID(ctx)
		dbTable.IsIndexed = req.GetIsEnable()
		err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbTableBizId(),
			cols, dbTable)
		if err != nil {
			log.ErrorContextf(ctx, "ChangeDbTableEnable failed, %v", err)
			return nil, err
		}
		dbTablesBizIDs = append(dbTablesBizIDs, req.GetDbTableBizId())
	} else if req.GetDbSourceBizId() > 0 {
		dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId())
		if err != nil {
			log.ErrorContextf(ctx, "EnableDbSourceScheduler|Prepare| get db source %v, err: %v", req.GetDbSourceBizId(), err)
			return nil, err
		}
		if dbSource.IsIndexed == req.GetIsEnable() {
			return nil, errs.ErrDbSourceStatusForChangeEnable
		}
		cnt, err := dao.GetDBSourceDao().CountByBizIDAndStatus(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId())
		if err != nil {
			return nil, err
		}
		if cnt > 0 {
			return nil, errs.ErrDbSourceStatusForChangeEnable
		}
		dbTables, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId())
		if err != nil {
			log.ErrorContextf(ctx, "EnableDbSourceScheduler|Prepare| get db table by db source biz id %v, err: %v", req.GetDbSourceBizId(), err)
			return nil, err
		}
		// 如果数据库关闭，则将之前表的状态记录
		if !req.GetIsEnable() {
			cols = append(cols, "prev_is_indexed")
			for _, table := range dbTables {
				table.PrevIsIndexed = table.IsIndexed
			}
		}
		for _, t := range dbTables {
			if t.LearnStatus == model.LearnStatusLearning || t.IsIndexed == req.GetIsEnable() {
				continue
			}
			if (!req.GetIsEnable() && t.IsIndexed) || (req.GetIsEnable() && t.PrevIsIndexed) {
				// 如果数据库关，则需要将所有表的状态改为关闭
				// 如果数据库开启，则将之前用户设置为开的表进行开启
				dbTablesBizIDs = append(dbTablesBizIDs, t.DBTableBizID)
				t.LearnStatus = model.LearnStatusLearning
				t.StaffID = pkg.StaffID(ctx)
				t.IsIndexed = req.GetIsEnable()
			}
		}
		err = dao.GetDBTableDao().BatchUpsertByBizID(ctx, cols, dbTables)
		if err != nil {
			log.ErrorContextf(ctx, "BatchUpsertByBizID failed, %v", err)
			return nil, err
		}
		dbSource.IsIndexed = req.GetIsEnable()
		dbSource.UpdateTime = time.Now()
		dbSource.LastSyncTime = time.Now()
		dbSource.ReleaseStatus = model.ReleaseStatusUnreleased
		if dbSource.NextAction != model.ReleaseActionAdd {
			dbSource.NextAction = model.ReleaseActionUpdate
		}
		dbSource.StaffID = pkg.StaffID(ctx)
		err = dao.GetDBSourceDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId(),
			[]string{"is_indexed", "update_time", "release_status", "staff_id", "last_sync_time"}, dbSource)
		if err != nil {
			log.ErrorContextf(ctx, "EnableDbSourceScheduler|Prepare| update db source %v, err: %v", req.GetDbSourceBizId(), err)
			return nil, err
		}
	}
	if len(dbTablesBizIDs) == 0 {
		return &pb.UpdateDbTableEnabledRsq{}, nil
	}
	for _, tableBizID := range dbTablesBizIDs {
		_, err = dao.NewEnableDbSourceTask(ctx, &model.EnableDBSourceParams{
			Name:          "",
			RobotID:       robotId,
			CorpBizID:     corpBizID,
			AppBizID:      req.GetAppBizId(),
			DbTableBizID:  tableBizID,
			DbSourceBizID: req.GetDbSourceBizId(),
			Enable:        req.GetIsEnable(),
			StaffID:       pkg.StaffID(ctx),
		})
		if err != nil {
			log.ErrorContextf(ctx, "NewEnableDbSourceTask failed, req:%v err:%v", req, err)
			return nil, err
		}
	}
	return &pb.UpdateDbTableEnabledRsq{}, nil
}
