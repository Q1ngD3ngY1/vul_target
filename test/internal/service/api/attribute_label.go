// bot-knowledge-config-server
//
// @(#)attribute_label.go  星期六, 十月 12, 2024
// Copyright(c) 2024, randalchen@Tencent. All rights reserved.

package api

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// UpdateAttrLabelsCacheProd 更新发布端标签缓存
func (s *Service) UpdateAttrLabelsCacheProd(ctx context.Context, req *pb.UpdateAttrLabelsCacheProdReq) (
	*pb.UpdateAttrLabelsCacheProdRsp, error) {
	rsp := new(pb.UpdateAttrLabelsCacheProdRsp)
	log.InfoContextf(ctx, "UpdateAttrLabelsCacheProd req, %+v", req)
	delStatusAndIDs, err := s.dao.GetAttributeKeysDelStatusAndIDs(ctx, req.GetRobotId(), req.GetAttrKey())
	if err != nil {
		log.ErrorContextf(ctx, "UpdateAttrLabelsCacheProd, GetAttributeKeysDelStatusAndIDs err, %v", err)
		return rsp, err
	}
	var updateAttrIDs []uint64 // 包括新增和修改
	var delAttrKeys []string
	for k, v := range delStatusAndIDs {
		if v.IsDeleted == 0 {
			updateAttrIDs = append(updateAttrIDs, v.AttrID)
		} else {
			delAttrKeys = append(delAttrKeys, k)
		}
	}
	log.InfoContextf(ctx, "UpdateAttrLabelsCacheProd delStatusAndIDs, %+v", delStatusAndIDs)
	if len(delAttrKeys) > 0 {
		err = s.dao.PiplineDelAttributeLabelRedis(ctx, req.GetRobotId(), delAttrKeys, model.AttributeLabelsProd)
		if err != nil {
			log.ErrorContextf(ctx, "UpdateAttrLabelsCacheProd, PiplineDelAttributeLabelRedis err, %v", err)
			return rsp, err
		}
	}
	mapAttr2Labels, err := s.dao.GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx, updateAttrIDs, req.RobotId)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateAttrLabelsCacheProd, GetAttributeLabelByAttrIDsProd err, %v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "UpdateAttrLabelsCacheProd mapAttr2Labels, %+v", mapAttr2Labels)
	attrKey2RedisValue := make(map[string][]model.AttributeLabelRedisValue)
	for attrID, Labels := range mapAttr2Labels {
		var redisValue []model.AttributeLabelRedisValue
		for _, l := range Labels {
			redisValue = append(redisValue, model.AttributeLabelRedisValue{
				Name:          l.Name,
				BusinessID:    l.BusinessID,
				SimilarLabels: l.SimilarLabel,
			})
		}
		if len(redisValue) == 0 {
			continue
		}
		for k, v := range delStatusAndIDs {
			if v.AttrID == attrID {
				attrKey2RedisValue[k] = redisValue
				break
			}
		}
	}
	log.InfoContextf(ctx, "UpdateAttrLabelsCacheProd attrKey2RedisValue, %+v", attrKey2RedisValue)
	if len(attrKey2RedisValue) == 0 {
		return rsp, nil
	}
	err = s.dao.PiplineSetAttributeLabelRedis(ctx, req.GetRobotId(), attrKey2RedisValue, model.AttributeLabelsProd)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateAttrLabelsCacheProd, PiplineSetAttributeLabelRedis err, %v", err)
		return rsp, err
	}
	return rsp, nil
}

// GetAttributeInfo 查询标签信息
func (s *Service) GetAttributeInfo(ctx context.Context, req *pb.GetAttributeInfoReq) (
	*pb.GetAttributeInfoRsp, error) {
	log.InfoContextf(ctx, "GetAttributeInfo req: %+v", req)
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if len(req.GetAttrBizIds()) > utilConfig.GetMainConfig().BatchInterfaceLimit.GeneralMaxLimit {
		return nil, errs.ErrAttributeLabelAttrLimit
	}
	attrMap, err := s.dao.GetAttributeByBizIDs(ctx, app.ID, req.GetAttrBizIds())
	if err != nil {
		return nil, err
	}
	attrIDs := make([]uint64, 0)
	for _, v := range attrMap {
		attrIDs = append(attrIDs, v.ID)
	}
	mapAttrID2Labels, err := s.dao.GetAttributeLabelByAttrIDs(ctx, attrIDs, app.ID)
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
	log.InfoContextf(ctx, "GetAttributeInfo success rsp: %+v", rsp)
	return rsp, nil
}
