package api

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// UpdateAttrLabelsCacheProd 更新发布端标签缓存
func (s *Service) UpdateAttrLabelsCacheProd(ctx context.Context, req *pb.UpdateAttrLabelsCacheProdReq) (
	*pb.UpdateAttrLabelsCacheProdRsp, error) {
	rsp := new(pb.UpdateAttrLabelsCacheProdRsp)
	err := s.labelLogic.UpdateAttrLabelsCacheProd(ctx, req.GetRobotId(), req.GetAttrKey())
	if err != nil {
		logx.E(ctx, "UpdateAttrLabelsCacheProd err: %v", err)
		return nil, err
	}
	return rsp, nil
}

// GetAttributeInfo 查询标签信息
func (s *Service) GetAttributeInfo(ctx context.Context, req *pb.GetAttributeInfoReq) (
	*pb.GetAttributeInfoRsp, error) {
	logx.I(ctx, "GetAttributeInfo req: %+v", req)
	app, err := s.svc.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if len(req.GetAttrBizIds()) > config.GetMainConfig().BatchInterfaceLimit.GeneralMaxLimit {
		return nil, errs.ErrAttributeLabelAttrLimit
	}
	attrMap, err := s.labelDao.GetAttributeByBizIDs(ctx, app.PrimaryId, req.GetAttrBizIds())
	if err != nil {
		return nil, err
	}
	attrIDs := make([]uint64, 0)
	for _, v := range attrMap {
		attrIDs = append(attrIDs, v.ID)
	}
	mapAttrID2Labels, err := s.labelDao.GetAttributeLabelByAttrIDs(ctx, attrIDs, app.PrimaryId)
	if err != nil {
		return nil, err
	}
	rsp := new(pb.GetAttributeInfoRsp)
	rsp.AttrLabelInfos = make([]*pb.GetAttributeInfoRsp_AttrLabelInfo, 0)
	for _, v := range attrMap {
		attrLabelInfo := &pb.GetAttributeInfoRsp_AttrLabelInfo{
			AttrBizId: v.BusinessID,
			AttrKey:   v.AttrKey,
			AttrName:  v.Name,
		}
		attrLabelInfo.Labels = make([]*pb.GetAttributeInfoRsp_LabelInfo, 0)
		for _, label := range mapAttrID2Labels[v.ID] {
			attrLabelInfo.Labels = append(attrLabelInfo.Labels, &pb.GetAttributeInfoRsp_LabelInfo{
				LabelBizId: label.BusinessID,
				LabelName:  label.Name,
			})
		}
		rsp.AttrLabelInfos = append(rsp.AttrLabelInfos, attrLabelInfo)
	}
	logx.I(ctx, "GetAttributeInfo success rsp: %+v", rsp)
	return rsp, nil
}
