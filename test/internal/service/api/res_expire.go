package api

import (
	"context"
	"database/sql"
	"errors"
	"git.woa.com/adp/common/x/gox/slicex"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

func (s *Service) NotifyKnowledgeCapacityExpired(
	ctx context.Context,
	req *pb.NotifyKnowledgeCapacityExpiredReq,
) (*pb.NotifyKnowledgeCapacityExpiredRsp, error) {
	rsp := &pb.NotifyKnowledgeCapacityExpiredRsp{}

	log.InfoContextf(ctx, "NotifyKnowledgeCapacityExpired: %+v", req)
	corp, err := s.dao.GetCorpBySidAndUin(ctx, req.Uin)
	if err != nil {
		log.ErrorContextf(ctx, "NotifyKnowledgeCapacityExpired GetCorpBySidAndUin err: %+v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "NotifyKnowledgeCapacityExpired: corp: %+v, err: %+v", corp, err)

	params := model.ResExpireParams{
		CorpID:     corp.ID,
		Uin:        req.GetUin(),
		ResourceID: req.GetResourceId(),
		Capacity:   req.GetCapacity(),
		ExpireTime: req.GetExpireTime(),
		IsDebug:    pkg.IsDebug(ctx),
	}
	if err := s.dao.CreateResourceExpireTask(ctx, params); err != nil {
		log.ErrorContextf(ctx, "CreateResourceExpireTask err: %+v", err)
		return rsp, err
	}

	log.InfoContextf(ctx, "NotifyKnowledgeCapacityExpired: %+v", rsp)
	return rsp, nil
}

// GetCorpCharacterUsage 获取企业下应用字符数使用情况
func (s *Service) GetCorpCharacterUsage(ctx context.Context, req *pb.GetCorpCharacterUsageReq) (
	rsp *pb.GetCorpCharacterUsageRsp, err error) {
	log.InfoContextf(ctx, "GetCorpCharacterUsage|req:%+v", req)
	rsp = new(pb.GetCorpCharacterUsageRsp)
	var corp *model.Corp
	if req.GetCorpBizId() != 0 {
		corp, err = s.dao.GetCorpByBusinessID(ctx, req.GetCorpBizId())
		if err != nil {
			return rsp, err
		}
	}
	if corp == nil {
		return rsp, errs.ErrCorpNotFound
	}

	// 企业字符
	rsp.CorpBizId = corp.BusinessID
	corpUsedChatSize, err := s.dao.GetCorpUsedCharSizeUsage(ctx, corp.ID)
	if err != nil {
		return rsp, err
	}
	rsp.CorpUsedCharSize = corpUsedChatSize

	// 应用数量
	total, err := s.dao.GetAppCount(ctx, corp.ID, nil, req.GetAppBizIds(), []string{},
		[]uint32{model.AppIsNotDeleted}, "", "")
	if err != nil {
		return rsp, err
	}
	rsp.Total = uint32(total)
	if rsp.Total == 0 {
		log.InfoContextf(ctx, "GetCorpCharacterUsage|rsp:%+v", rsp)
		return rsp, nil
	}

	// 应用详情
	apps, err := s.dao.GetAppListOrderByUsedCharSize(ctx, corp.ID, req.GetAppBizIds(), []string{},
		[]uint32{model.AppIsNotDeleted}, req.GetPage(), req.GetPageSize())
	if err != nil {
		return rsp, err
	}
	shareAppBizIDs, appIDs := make([]uint64, 0, len(apps)), make([]uint64, 0, len(apps))
	for _, app := range apps {
		appIDs = append(appIDs, app.ID)
		if app.IsShared {
			shareAppBizIDs = append(shareAppBizIDs, app.BusinessID)
		}
	}
	if len(appIDs) == 0 {
		log.InfoContextf(ctx, "GetCorpCharacterUsage|rsp:%+v", rsp)
		return rsp, nil
	}

	allAppDocExceedCharSizeMap := make(map[uint64]uint64)
	allAppQAExceedCharSizeMap := make(map[uint64]uint64)
	chunks := slicex.Chunk(appIDs, 200)
	for _, chunk := range chunks {
		appDocExceedCharSizeMap, err := s.dao.GetRobotDocExceedCharSize(ctx, corp.ID, chunk)
		if err != nil {
			return rsp, err
		}
		appQAExceedCharSizeMap, err := s.dao.GetRobotQAExceedCharSize(ctx, corp.ID, chunk)
		if err != nil {
			return rsp, err
		}
		for appID, docExceedCharSize := range appDocExceedCharSizeMap {
			allAppDocExceedCharSizeMap[appID] = docExceedCharSize
		}
		for appID, qaExceedCharSize := range appQAExceedCharSizeMap {
			allAppQAExceedCharSizeMap[appID] = qaExceedCharSize
		}
	}

	//获取共享知识库名字
	shareknowInfoByID := make(map[uint64]*model.SharedKnowledgeInfo, len(shareAppBizIDs))
	if len(shareAppBizIDs) > 0 {
		shareknowList, err := s.dao.RetrieveBaseSharedKnowledge(ctx, req.GetCorpBizId(), shareAppBizIDs)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			log.ErrorContextf(ctx, "GetCorpCharacterUsage RetrieveBaseSharedKnowledge err:%v,corpBizID:%v,shareAppBizIDs:%v",
				err, pkg.CorpBizID(ctx), shareAppBizIDs)
			return rsp, err
		}
		for _, v := range shareknowList {
			shareknowInfoByID[v.BusinessID] = v
		}
	}
	usageInfo := make([]*pb.GetCorpCharacterUsageRsp_UsageInfo, 0)
	for _, app := range apps {
		tmp := &pb.GetCorpCharacterUsageRsp_UsageInfo{
			AppBizId:       app.BusinessID,
			UsedCharSize:   uint64(app.UsedCharSize),
			ExceedCharSize: allAppDocExceedCharSizeMap[app.ID] + allAppQAExceedCharSizeMap[app.ID],
			KnowledgeBizId: app.BusinessID,
			KnowledgeType:  pb.KnowledgeType_AppDefaultKnowledge,
			KnowledgeName:  app.Name,
		}
		if shareInfo, ok := shareknowInfoByID[app.BusinessID]; ok {
			tmp.KnowledgeType = pb.KnowledgeType_SharedKnowledge
			tmp.KnowledgeName = shareInfo.Name
		}
		usageInfo = append(usageInfo, tmp)
	}
	rsp.UsageInfo = usageInfo
	log.InfoContextf(ctx, "GetCorpCharacterUsage|rsp:%+v", rsp)
	return rsp, nil
}
