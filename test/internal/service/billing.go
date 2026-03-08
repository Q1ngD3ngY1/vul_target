package service

import (
	"context"
	"git.woa.com/adp/common/x/gox/slicex"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetCharacterUsage 获取字符使用量与容量
func (s *Service) GetCharacterUsage(ctx context.Context, req *pb.GetCharacterUsageReq) (*pb.GetCharacterUsageRsp, error) {
	rsp := &pb.GetCharacterUsageRsp{}
	corpID := pkg.CorpID(ctx)
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	corp, err = s.dao.GetCorpBillingInfo(ctx, corp)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpBillingInfo err: %+v", err)
		return rsp, err
	}
	rsp.Total = uint32(corp.MaxCharSize)

	// 应用数量
	total, err := s.dao.GetAppCount(ctx, corp.ID, nil, []uint64{}, []string{},
		[]uint32{model.AppIsNotDeleted}, "", "")
	if err != nil {
		return rsp, err
	}
	if total == 0 {
		return rsp, nil
	}

	// 应用详情
	apps, err := s.dao.GetAppListOrderByUsedCharSize(ctx, corp.ID, []uint64{}, []string{},
		[]uint32{model.AppIsNotDeleted}, 1, uint32(total))
	if err != nil {
		return rsp, err
	}
	if len(apps) == 0 {
		return rsp, nil
	}
	used := int64(0)
	appIDs := make([]uint64, 0, len(apps))
	for _, app := range apps {
		appIDs = append(appIDs, app.ID)
		if app.UsedCharSize > 0 {
			used += app.UsedCharSize
		}
	}
	rsp.Used = uint32(used)
	if knowClient.IsVipCorp(corp.Uin) {
		// TODO: 该接口后续需要优化，支持百万级文档的超大应用
		rsp.Exceed = 0
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
	for _, app := range apps {
		rsp.Exceed += uint32(allAppDocExceedCharSizeMap[app.ID] + allAppQAExceedCharSizeMap[app.ID])
	}
	return rsp, nil
}
