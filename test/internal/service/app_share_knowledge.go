package service

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicAppSKG "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app_share_knowledge"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/data_statistics"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/common/v3/utils"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// ReferShareKnowledge 引用共享文档
func (s *Service) ReferShareKnowledge(ctx context.Context,
	req *pb.ReferSharedKnowledgeReq) (*pb.ReferSharedKnowledgeRsp, error) {
	start := time.Now()
	log.InfoContextf(ctx, "ReferShareKnowledge Req: %+v", utils.Any2String(req))
	corpID := pkg.CorpID(ctx)
	rsp := new(pb.ReferSharedKnowledgeRsp)
	app, err := client.GetAppInfo(ctx, req.GetAppBizId(), model.RunEnvSandbox)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if app.GetCorpId() != corpID {
		return rsp, errs.ErrPermissionDenied
	}
	// 校验引用的知识库id是否合法
	checkList, err := s.dao.RetrieveBaseSharedKnowledge(ctx, app.GetCorpBizId(), req.GetKnowledgeBizId())
	if err != nil {
		log.ErrorContextf(ctx, "RetrieveBaseSharedKnowledge err:%v", err)
		return rsp, errs.ErrPermissionDenied
	}
	if len(checkList) != len(req.GetKnowledgeBizId()) { // 说明有不合法的id
		log.ErrorContext(ctx, "invalid knowledgeBizId")
		return rsp, errs.ErrPermissionDenied
	}
	// 1. 获取应用引用共享库列表
	shareKGList, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "GetAppShareKGList failed, err: %+v", err)
		return nil, errs.ErrGetAppShareKGListFailed
	}
	var oldList []uint64
	for _, shareKG := range shareKGList {
		oldList = append(oldList, shareKG.KnowledgeBizID)
	}
	newList := req.GetKnowledgeBizId()
	// 2. 对比差异，看哪些是新增，哪些是删除
	addList := slicex.Diff(newList, oldList)
	delList := slicex.Diff(oldList, newList)
	// 3. 新增
	var addShareKGList []model.AppShareKnowledge
	for _, knowledgeBizID := range addList {
		addShareKGList = append(addShareKGList, model.AppShareKnowledge{
			AppBizID:       req.GetAppBizId(),
			KnowledgeBizID: knowledgeBizID,
			CorpBizID:      app.GetCorpBizId(),
			UpdateTime:     time.Now(),
			CreateTime:     time.Now(),
		})
	}
	if len(addShareKGList) > 0 {
		err = dao.GetAppShareKGDao().CreateAppShareKG(ctx, addShareKGList)
		if err != nil {
			log.ErrorContextf(ctx, "CreateAppShareKG failed, err: %+v", err)
			return rsp, errs.ErrSetAppShareKGFailed
		}
	}
	// 4. 删除
	if len(delList) > 0 {
		err = logicAppSKG.DeleteAppShareKG(ctx, app, delList)
		if err != nil {
			log.ErrorContextf(ctx, "DeleteAppShareKG failed, err: %+v", err)
			return rsp, err
		}
	}

	// 上报统计数据
	go func(newCtx context.Context) { //异步上报
		counterInfo := &data_statistics.CounterInfo{
			CorpBizId:       pkg.CorpBizID(newCtx),
			SpaceId:         app.GetSpaceId(),
			AppBizId:        req.GetAppBizId(),
			StatisticObject: statistics.StatObject_STAT_OBJECT_KB,
			StatisticType:   statistics.StatType_STAT_TYPE_EDIT,
			ObjectId:        strconv.FormatUint(req.GetAppBizId(), 10),
			ObjectName:      app.GetBaseConfig().GetName(),
			Count:           1,
		}
		data_statistics.Counter(newCtx, counterInfo)
	}(trpc.CloneContext(ctx))

	log.InfoContextf(ctx, "ReferShareKnowledge Rsp cost: %d", time.Since(start).Milliseconds())
	return rsp, nil
}

// ListReferShareKnowledge 查看引用共享文档列表
func (s *Service) ListReferShareKnowledge(ctx context.Context,
	req *pb.ListReferSharedKnowledgeReq) (*pb.ListReferSharedKnowledgeRsp, error) {
	start := time.Now()
	rsp := new(pb.ListReferSharedKnowledgeRsp)
	log.InfoContextf(ctx, "ListReferShareKnowledge Req: %+v", utils.Any2String(req))
	corpID := pkg.CorpID(ctx)
	app, err := client.GetAppInfo(ctx, req.GetAppBizId(), model.RunEnvSandbox)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if app.GetCorpId() != corpID {
		return rsp, errs.ErrPermissionDenied
	}
	// 1. 获取应用引用共享库列表
	shareKGList, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "GetAppShareKGList failed, err: %+v", err)
		return nil, errs.ErrGetAppShareKGListFailed
	}
	if len(shareKGList) == 0 {
		log.WarnContextf(ctx, "ListReferShareKnowledge GetAppShareKGList is empty")
		return rsp, nil
	}
	// 2. 批量获取共享库详情
	var shareKGBizIDs []uint64
	for _, val := range shareKGList {
		shareKGBizIDs = append(shareKGBizIDs, val.KnowledgeBizID)
	}
	shareKGInfoList, err := s.dao.RetrieveBaseSharedKnowledge(ctx, app.GetCorpBizId(), shareKGBizIDs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rsp, nil
		}
		log.ErrorContextf(ctx, "RetrieveBaseSharedKnowledge failed, err: %+v", err)
		return nil, errs.ErrRetrieveBaseSharedKGFailed
	}
	shareKGInfoMap := make(map[uint64]*model.SharedKnowledgeInfo)
	for _, info := range shareKGInfoList {
		shareKGInfoMap[info.BusinessID] = info
	}
	for _, val := range shareKGList {
		shareKGInfo, ok := shareKGInfoMap[val.KnowledgeBizID]
		if !ok || shareKGInfo == nil {
			log.WarnContextf(ctx, "shareKGInfoMap[%d] not found", val.KnowledgeBizID)
			continue
		}
		rsp.List = append(rsp.List, &pb.KnowledgeBaseInfo{
			KnowledgeBizId:       val.KnowledgeBizID,
			KnowledgeName:        shareKGInfo.Name,
			KnowledgeDescription: shareKGInfo.Description,
		})
	}
	log.InfoContextf(ctx, "ListReferShareKnowledge Rsp: %+v, cost: %d", utils.Any2String(rsp),
		time.Since(start).Milliseconds())
	return rsp, nil
}
