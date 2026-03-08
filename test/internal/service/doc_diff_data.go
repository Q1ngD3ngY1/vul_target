package service

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ListDocDiffData 获取对比任务结果列表
func (s *Service) ListDocDiffData(ctx context.Context, req *pb.ListDocDiffDataReq) (*pb.ListDocDiffDataRsp, error) {
	logx.I(ctx, "ListDocDiff Req:%+v", req)
	rsp := new(pb.ListDocDiffDataRsp)
	corpId := contextx.Metadata(ctx).CorpID()
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpId)
	// corp, err := s.dao.GetCorpByID(ctx, corpId)
	if err != nil {
		logx.E(ctx, "GetCorpByID err: %+v", err)
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
	selectColumns := []string{docEntity.DocDiffDataTblColDiffData}
	offset, limit := utilx.Page(req.GetPageNumber(), req.GetPageSize())
	filter := &docEntity.DocDiffDataFilter{
		CorpBizId:  corp.GetCorpId(),
		RobotBizId: botBizId,
		DiffBizId:  diffBizId,
		IsDeleted:  ptrx.Bool(false),
		Offset:     offset,
		Limit:      limit,
	}
	list, count, err := s.docLogic.GetDocDiffDataCountAndList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "ListDocDiff err:%+v", err)
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
