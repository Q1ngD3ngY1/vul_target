package service

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ListDocDiffData 获取对比任务结果列表
func (s *Service) ListDocDiffData(ctx context.Context, req *pb.ListDocDiffDataReq) (*pb.ListDocDiffDataRsp, error) {
	log.InfoContextf(ctx, "ListDocDiff Req:%+v", req)
	rsp := new(pb.ListDocDiffDataRsp)
	corpId := pkg.CorpID(ctx)
	corp, err := s.dao.GetCorpByID(ctx, corpId)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	botBizId, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	diffBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetDocDiffTaskBizId())
	if err != nil {
		return nil, err
	}
	isNotDeleted := dao.IsNotDeleted
	selectColumns := []string{dao.DocDiffDataTblColDiffData}
	filter := &dao.DocDiffDataFilter{
		CorpBizId:  corp.BusinessID,
		RobotBizId: botBizId,
		DiffBizId:  diffBizId,
		IsDeleted:  &isNotDeleted,
		Offset:     common.GetOffsetByPage(req.GetPageNumber(), req.GetPageSize()),
		Limit:      req.GetPageSize(),
	}
	list, count, err := dao.GetDocDiffDataDao().GetDocDiffDataCountAndList(ctx, selectColumns, filter)
	if err != nil {
		log.ErrorContextf(ctx, "ListDocDiff err:%+v", err)
		return rsp, err
	}
	diffDataList := make([]string, len(list))
	for i, diffRes := range list {
		diffDataList[i] = diffRes.DiffData
	}
	rsp.Total = uint64(count)
	rsp.DiffData = diffDataList
	return rsp, nil
}
