package database

import (
	"context"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	kbDao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	cacheLogic "git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// DescribeDbStatistics 获取数据库相关统计信息
func (l *Logic) DescribeDbStatistics(ctx context.Context, corpBizID, appBizID uint64, kbDaoInst kbDao.Dao, cacheLogicInst *cacheLogic.Logic) (*pb.DescribeDbStatisticsRsp, error) {
	rsp := &pb.DescribeDbStatisticsRsp{}

	// 1. 获取应用关联的共享知识库列表
	shareKGList, err := kbDaoInst.GetAppShareKGList(ctx, appBizID)
	if err != nil {
		logx.E(ctx, "DescribeDbStatistics GetAppShareKGList failed, appBizID: %d, err: %+v", appBizID, err)
		return nil, err
	}

	// 2. 构建知识库BizID列表（包含默认知识库和共享知识库）
	appBizIDs := []uint64{appBizID}
	for _, shareKG := range shareKGList {
		appBizIDs = append(appBizIDs, shareKG.KnowledgeBizID)
	}

	// 3. 将appBizIDs转换为appIDs（用于数据库表查询）
	appIDMap, err := cacheLogicInst.GetAppPrimaryIdsByBizIds(ctx, appBizIDs)
	if err != nil {
		logx.E(ctx, "DescribeDbStatistics GetAppPrimaryIdsByBizIds failed, appBizIDs: %+v, err: %+v", appBizIDs, err)
		return nil, err
	}
	appIDs := make([]uint64, 0, len(appIDMap))
	for _, appID := range appIDMap {
		appIDs = append(appIDs, appID)
	}
	logx.I(ctx, "DescribeDbStatistics appBizIDs: %+v, appIDs: %+v", appBizIDs, appIDs)

	// 边界情况处理：如果appIDs为空，说明应用不存在或已被删除，直接返回空统计
	if len(appIDs) == 0 {
		logx.W(ctx, "DescribeDbStatistics appIDs is empty, appBizIDs: %+v", appBizIDs)
		rsp.KnowledgeEnableScopeStatistics = make([]*pb.KnowledgeEnableScopeStatistics, 0)
		return rsp, nil
	}

	// 4. 统计所有知识库的数据库总数（t_db_source表中未删除的数据库）
	dbSourceFilter := &dbEntity.DatabaseFilter{
		AppBizIDs: appBizIDs,
		IsDeleted: ptrx.Bool(false),
	}
	databaseCount, err := l.dao.CountDatabase(ctx, dbSourceFilter)
	if err != nil {
		logx.E(ctx, "DescribeDbStatistics CountDatabase failed, err: %+v", err)
		return nil, err
	}
	rsp.DatabaseCount = uint64(databaseCount)

	// 5. 统计所有知识库的数据表总数（t_db_table表中未删除的数据表）
	tableFilter := &dbEntity.TableFilter{
		AppBizIDs: appBizIDs,
		IsDeleted: ptrx.Bool(false),
	}
	tableCount, err := l.dao.CountTable(ctx, tableFilter)
	if err != nil {
		logx.E(ctx, "DescribeDbStatistics CountTable failed, err: %+v", err)
		return nil, err
	}
	rsp.TableCount = uint64(tableCount)

	// 6. 按enable_scope统计数据表数量（t_db_table表）
	enableScopeStats := make(map[uint32]uint64)
	for _, enableScope := range []uint32{
		entity.EnableScopeDisable,
		entity.EnableScopeDev,
		entity.EnableScopePublish,
		entity.EnableScopeAll,
	} {
		scopeFilter := &dbEntity.TableFilter{
			AppBizIDs:   appBizIDs,
			IsDeleted:   ptrx.Bool(false),
			EnableScope: ptrx.Uint32(enableScope),
		}
		count, err := l.dao.CountTable(ctx, scopeFilter)
		if err != nil {
			logx.E(ctx, "DescribeDbStatistics CountTable for enable_scope failed, enableScope: %d, err: %+v", enableScope, err)
			return nil, err
		}
		enableScopeStats[enableScope] = uint64(count)
	}

	// 7. 构建KnowledgeEnableScopeStatistics列表
	rsp.KnowledgeEnableScopeStatistics = make([]*pb.KnowledgeEnableScopeStatistics, 0)
	for enableScope, count := range enableScopeStats {
		if count > 0 { // 只返回有数据的统计
			rsp.KnowledgeEnableScopeStatistics = append(rsp.KnowledgeEnableScopeStatistics, &pb.KnowledgeEnableScopeStatistics{
				EnableScope: pb.RetrievalEnableScope(enableScope),
				Count:       count,
			})
		}
	}

	logx.I(ctx, "DescribeDbStatistics result: databaseCount=%d, tableCount=%d, enableScopeStats=%+v",
		rsp.DatabaseCount, rsp.TableCount, enableScopeStats)

	return rsp, nil
}
