package qa

import (
	"context"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	kbDao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	cacheLogic "git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// DescribeQaStatistics 获取问答相关统计信息
func (l *Logic) DescribeQaStatistics(ctx context.Context, corpBizID, appBizID uint64, kbDaoInst kbDao.Dao, cacheLogicInst *cacheLogic.Logic) (*pb.DescribeQaStatisticsRsp, error) {
	rsp := &pb.DescribeQaStatisticsRsp{}

	// 1. 获取应用关联的共享知识库列表
	shareKGList, err := kbDaoInst.GetAppShareKGList(ctx, appBizID)
	if err != nil {
		logx.E(ctx, "DescribeQaStatistics GetAppShareKGList failed, appBizID: %d, err: %+v", appBizID, err)
		return nil, err
	}

	// 2. 构建知识库BizID列表（包含默认知识库和共享知识库）
	appBizIDs := []uint64{appBizID}
	for _, shareKG := range shareKGList {
		appBizIDs = append(appBizIDs, shareKG.KnowledgeBizID)
	}

	// 3. 将appBizIDs转换为appIDs（用于DocQA表查询）
	appIDMap, err := cacheLogicInst.GetAppPrimaryIdsByBizIds(ctx, appBizIDs)
	if err != nil {
		logx.E(ctx, "DescribeQaStatistics GetAppPrimaryIdsByBizIds failed, appBizIDs: %+v, err: %+v", appBizIDs, err)
		return nil, err
	}
	appIDs := make([]uint64, 0, len(appIDMap))
	for _, appID := range appIDMap {
		appIDs = append(appIDs, appID)
	}
	logx.I(ctx, "DescribeQaStatistics appBizIDs: %+v, appIDs: %+v", appBizIDs, appIDs)

	// 边界情况处理：如果appIDs为空，说明应用不存在或已被删除，直接返回空统计
	if len(appIDs) == 0 {
		logx.W(ctx, "DescribeQaStatistics appIDs is empty, appBizIDs: %+v", appBizIDs)
		rsp.KnowledgeEnableScopeStatistics = make([]*pb.KnowledgeEnableScopeStatistics, 0)
		return rsp, nil
	}

	// 4. 统计所有知识库的问答总数（未删除的问答）
	filter := &qaEntity.DocQaFilter{
		RobotIDs:     appIDs,
		IsDeleted:    ptrx.Uint32(qaEntity.QAIsNotDeleted),
		AcceptStatus: qaEntity.AcceptYes, // 已校验状态
	}
	qaCount, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, filter, nil)
	if err != nil {
		logx.E(ctx, "DescribeQaStatistics GetDocQaCountWithTx failed, err: %+v", err)
		return nil, err
	}
	rsp.QaCount = uint64(qaCount)

	// 5. 统计待校验的问答数量（accept_status为1未校验且未删除）
	pendingVerificationFilter := &qaEntity.DocQaFilter{
		RobotIDs:     appIDs,
		IsDeleted:    ptrx.Uint32(qaEntity.QAIsNotDeleted),
		AcceptStatus: qaEntity.AcceptInit, // 未校验状态
	}
	pendingVerificationCount, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, pendingVerificationFilter, nil)
	if err != nil {
		logx.E(ctx, "DescribeQaStatistics GetDocQaCountWithTx for pending verification failed, err: %+v", err)
		return nil, err
	}
	rsp.QaPendingVerificationCount = uint64(pendingVerificationCount)

	// 6. 统计待处理的冲突问数量（t_doc_qa_similar表中status为0未处理且is_valid为1有效）
	similarFilter := &qaEntity.DocQASimilarFilter{
		RobotIDs: appIDs, // 使用appIDs（主键ID）
		Status:   ptrx.Uint64(uint64(qaEntity.QaSimilarStatusInit)),
		IsValid:  ptrx.Uint32(uint32(qaEntity.QaSimilarIsValid)),
	}
	conflictCount, err := l.qaDao.GetDocQaSimilarListCount(ctx, similarFilter)
	if err != nil {
		logx.E(ctx, "DescribeQaStatistics GetDocQaSimilarListCount failed, err: %+v", err)
		return nil, err
	}
	rsp.QaPendingProcessConflictCount = uint64(conflictCount)

	// 7. 按enable_scope统计问答数量
	enableScopeStats := make(map[uint32]uint64)
	for _, enableScope := range []uint32{
		entity.EnableScopeDisable,
		entity.EnableScopeDev,
		entity.EnableScopePublish,
		entity.EnableScopeAll,
	} {
		scopeFilter := &qaEntity.DocQaFilter{
			RobotIDs:     appIDs,
			IsDeleted:    ptrx.Uint32(qaEntity.QAIsNotDeleted),
			EnableScope:  ptrx.Uint32(enableScope),
			AcceptStatus: qaEntity.AcceptYes, // 已校验状态
		}
		count, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, scopeFilter, nil)
		if err != nil {
			logx.E(ctx, "DescribeQaStatistics GetDocQaCountWithTx for enable_scope failed, enableScope: %d, err: %+v", enableScope, err)
			return nil, err
		}
		enableScopeStats[enableScope] = uint64(count)
	}

	// 8. 构建KnowledgeEnableScopeStatistics列表
	rsp.KnowledgeEnableScopeStatistics = make([]*pb.KnowledgeEnableScopeStatistics, 0)
	for enableScope, count := range enableScopeStats {
		if count > 0 { // 只返回有数据的统计
			rsp.KnowledgeEnableScopeStatistics = append(rsp.KnowledgeEnableScopeStatistics, &pb.KnowledgeEnableScopeStatistics{
				EnableScope: pb.RetrievalEnableScope(enableScope),
				Count:       count,
			})
		}
	}

	logx.I(ctx, "DescribeQaStatistics result: qaCount=%d, pendingVerificationCount=%d, conflictCount=%d, enableScopeStats=%+v",
		rsp.QaCount, rsp.QaPendingVerificationCount, rsp.QaPendingProcessConflictCount, enableScopeStats)

	return rsp, nil
}
