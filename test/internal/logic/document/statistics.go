package document

import (
	"context"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	kbDao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	cacheLogic "git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// DescribeDocStatistics 获取文档相关统计信息
func (l *Logic) DescribeDocStatistics(ctx context.Context, corpBizID, appBizID uint64, kbDaoInst kbDao.Dao, cacheLogicInst *cacheLogic.Logic) (*pb.DescribeDocStatisticsRsp, error) {
	rsp := &pb.DescribeDocStatisticsRsp{}

	// 1. 获取应用关联的共享知识库列表
	shareKGList, err := kbDaoInst.GetAppShareKGList(ctx, appBizID)
	if err != nil {
		logx.E(ctx, "DescribeDocStatistics GetAppShareKGList failed, appBizID: %d, err: %+v", appBizID, err)
		return nil, err
	}

	// 2. 构建知识库BizID列表（包含默认知识库和共享知识库）
	appBizIDs := []uint64{appBizID}
	for _, shareKG := range shareKGList {
		appBizIDs = append(appBizIDs, shareKG.KnowledgeBizID)
	}

	// 3. 将appBizIDs转换为appIDs（用于Doc表查询）
	appIDMap, err := cacheLogicInst.GetAppPrimaryIdsByBizIds(ctx, appBizIDs)
	if err != nil {
		logx.E(ctx, "DescribeDocStatistics GetAppPrimaryIdsByBizIds failed, appBizIDs: %+v, err: %+v", appBizIDs, err)
		return nil, err
	}
	appIDs := make([]uint64, 0, len(appIDMap))
	for _, appID := range appIDMap {
		appIDs = append(appIDs, appID)
	}
	logx.I(ctx, "DescribeDocStatistics appBizIDs: %+v, appIDs: %+v", appBizIDs, appIDs)

	// 边界情况处理：如果appIDs为空，说明应用不存在或已被删除，直接返回空统计
	if len(appIDs) == 0 {
		logx.W(ctx, "DescribeDocStatistics appIDs is empty, appBizIDs: %+v", appBizIDs)
		rsp.KnowledgeEnableScopeStatistics = make([]*pb.KnowledgeEnableScopeStatistics, 0)
		return rsp, nil
	}

	// 4. 统计所有知识库的文档总数（未删除的文档）
	filter := &docEntity.DocFilter{
		RobotIDs:       appIDs,
		IsDeleted:      ptrx.Bool(false),
		RouterAppBizID: appBizID,
	}
	docCount, err := l.docDao.GetDocCountByDistinctID(ctx, filter)
	if err != nil {
		logx.E(ctx, "DescribeDocStatistics GetDocCountByDistinctID failed, err: %+v", err)
		return nil, err
	}
	rsp.DocCount = uint64(docCount)

	// 5. 统计待处理的文档比对任务数量（使用appBizIDs数组一次性查询）
	diffTaskFilter := &docEntity.DocDiffTaskFilter{
		CorpBizId:   corpBizID,
		RobotBizIds: appBizIDs, // 使用appBizIDs数组
		IsDeleted:   ptrx.Bool(false),
		Statuses:    []int32{int32(docEntity.DocDiffStatusInit)}, // 待处理状态
	}
	docDiffTaskCount, err := l.docDao.GetDocDiffTaskCount(ctx, []string{docEntity.DocDiffTaskTblColBusinessId}, diffTaskFilter)
	if err != nil {
		logx.E(ctx, "DescribeDocStatistics GetDocDiffTaskCount failed, err: %+v", err)
		return nil, err
	}
	rsp.DocDiffTaskCount = uint64(docDiffTaskCount)

	// 6. 按enable_scope统计文档数量
	enableScopeStats := make(map[uint32]uint64)
	// 统计每个enable_scope的文档数量
	for _, enableScope := range []uint32{
		entity.EnableScopeDisable,
		entity.EnableScopeDev,
		entity.EnableScopePublish,
		entity.EnableScopeAll,
	} {
		scopeFilter := &docEntity.DocFilter{
			RobotIDs:       appIDs,
			IsDeleted:      ptrx.Bool(false),
			EnableScope:    ptrx.Uint32(enableScope),
			RouterAppBizID: appBizID,
		}
		count, err := l.docDao.GetDocCountByDistinctID(ctx, scopeFilter)
		if err != nil {
			logx.E(ctx, "DescribeDocStatistics GetDocCountByDistinctID for enable_scope failed, enableScope: %d, err: %+v", enableScope, err)
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

	logx.I(ctx, "DescribeDocStatistics result: docCount=%d, docDiffTaskCount=%d, enableScopeStats=%+v",
		rsp.DocCount, rsp.DocDiffTaskCount, enableScopeStats)

	return rsp, nil
}
