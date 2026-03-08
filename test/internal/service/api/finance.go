package api

import (
	"context"
	"errors"
	"strconv"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/boolx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
)

// NotifyKnowledgeCapacityExpired 知识库容量到期通知
func (s *Service) NotifyKnowledgeCapacityExpired(ctx context.Context, req *pb.NotifyKnowledgeCapacityExpiredReq) (*pb.NotifyKnowledgeCapacityExpiredRsp, error) {
	rsp := &pb.NotifyKnowledgeCapacityExpiredRsp{}

	logx.I(ctx, "NotifyKnowledgeCapacityExpired: %+v", req)
	corpReq := pm.DescribeCorpReq{Uin: req.GetUin()}
	corp, err := s.rpc.PlatformAdmin.DescribeCorp(ctx, &corpReq)
	if err != nil {
		logx.E(ctx, "NotifyKnowledgeCapacityExpired GetCorpBySidAndUin err: %+v", err)
		return rsp, err
	}
	logx.I(ctx, "NotifyKnowledgeCapacityExpired: corp: %+v, err: %+v", corp, err)
	isPackageScene := false
	// 调用DescribeAccountQuota获取配额信息
	quotaRsp, err := s.rpc.Finance.DescribeAccountQuota(ctx, corp.GetUin(), corp.GetSid())
	if err != nil {
		logx.E(ctx, "CheckKnowledgeBaseQuota DescribeAccountQuota failed, uin:%s, sid:%d, err:%+v", corp.GetUin(), corp.GetSid(), err)
		return rsp, err
	}
	if quotaRsp != nil && quotaRsp.GetIsPackageScene() {
		isPackageScene = true
	}
	params := entity.ResExpireParams{
		CorpID:         corp.GetCorpPrimaryId(),
		Uin:            req.GetUin(),
		ResourceID:     req.GetResourceId(),
		Capacity:       req.GetCapacity(),
		ExpireTime:     req.GetExpireTime(),
		IsDebug:        contextx.Metadata(ctx).IsDebug(),
		IsPackageScene: isPackageScene,
	}
	if err := scheduler.NewResourceExpireTask(ctx, params); err != nil {
		logx.E(ctx, "CreateResourceExpireTask err: %+v", err)
		return rsp, err
	}

	logx.I(ctx, "NotifyKnowledgeCapacityExpired: %+v", rsp)
	return rsp, nil
}

// NotifyKnowledgeCapacityChanged 知识库容量变化通知
// 结合最新的容量，判断是否重置超量信息
func (s *Service) NotifyKnowledgeCapacityChanged(ctx context.Context, req *pb.NotifyKnowledgeCapacityChangedReq) (*pb.NotifyKnowledgeCapacityChangedRsp, error) {
	logx.I(ctx, "NotifyKnowledgeCapacityChanged: %+v", req)
	newCapacity := int64(req.GetKnowledgeCapacity() * 1024 * 1024 * 1024) // GB转字节
	corpReq := pm.DescribeCorpReq{Uin: req.GetUin()}
	corp, err := s.rpc.PlatformAdmin.DescribeCorp(ctx, &corpReq)
	if err != nil {
		logx.E(ctx, "NotifyKnowledgeCapacityChanged DescribeCorp failed, uin:%d, err:%+v", req.GetUin(), err)
		return &pb.NotifyKnowledgeCapacityChangedRsp{}, err
	}
	// 1、查询企业下当前的使用量
	usage, err := s.rpc.AppApi.DescribeCorpKnowledgeCapacity(ctx, corp.GetCorpId(), []uint64{})
	if err != nil {
		logx.E(ctx, "NotifyKnowledgeCapacityChanged DescribeCorpKnowledgeCapacity failed, corpID:%d, err:%+v", corp.GetCorpId(), err)
		return &pb.NotifyKnowledgeCapacityChangedRsp{}, err
	}
	logx.I(ctx, "NotifyKnowledgeCapacityChanged: corpID:%d, currentUsage:%v, newCapacity:%d", corp.GetCorpId(), usage, newCapacity)
	// 2、重置超量信息
	resetOverCapacity := entity.CapacityUsage{
		KnowledgeCapacity: max(0, usage.KnowledgeCapacity-newCapacity),
		StorageCapacity:   max(0, usage.StorageCapacity-newCapacity),
		ComputeCapacity:   max(0, usage.ComputeCapacity-newCapacity),
	}
	err = s.rpc.PlatformApi.ResetCorpKnowledgeOverCapacity(ctx, corp.GetCorpId(), resetOverCapacity)
	if err != nil {
		logx.E(ctx, "NotifyKnowledgeCapacityChanged ResetCorpKnowledgeOverCapacity failed, corpID:%d, err:%+v",
			corp.GetCorpId(), err)
		return &pb.NotifyKnowledgeCapacityChangedRsp{}, err
	}
	logx.I(ctx, "NotifyKnowledgeCapacityChanged: reset over capacity for corpID:%d, capacity:%+v", corp.GetCorpId(), resetOverCapacity)

	return &pb.NotifyKnowledgeCapacityChangedRsp{}, nil
}

// GetCorpCharacterUsage 获取企业下应用字符数使用情况
func (s *Service) GetCorpCharacterUsage(ctx context.Context, req *pb.GetCorpCharacterUsageReq) (rsp *pb.GetCorpCharacterUsageRsp, err error) {
	logx.I(ctx, "GetCorpCharacterUsage|req:%+v", req)
	rsp = new(pb.GetCorpCharacterUsageRsp)
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, req.GetCorpBizId())
	if err != nil {
		logx.E(ctx, "GetCorpCharacterUsage|DescribeCorpByBizId corpBizID:%v, error:%v", req.GetCorpBizId(), err)
		return rsp, err
	}
	if corp == nil {
		return rsp, errs.ErrCorpNotFound
	}

	// 企业字符
	rsp.CorpBizId = req.GetCorpBizId()
	usage, err := s.rpc.AppApi.DescribeCorpKnowledgeCapacity(ctx, corp.GetCorpId(), []uint64{})
	if err != nil {
		logx.E(ctx, "GetCorpCharacterUsage|DescribeCorpKnowledgeCapacity corpPrimaryID:%v, error:%v", corp.GetCorpId(), err)
		return rsp, err
	}
	rsp.CorpUsedCharSize = uint64(usage.CharSize)
	rsp.CorpUsedKnowledgeCapacity = uint64(usage.KnowledgeCapacity)

	// 应用数量
	countAppReq := appconfig.CountAppReq{
		CorpPrimaryId: corp.GetCorpPrimaryId(),
		AppIds:        req.GetAppBizIds(),
		IsDeleted:     ptrx.Uint32(boolx.No),
	}
	total, err := s.rpc.AppAdmin.CountApp(ctx, &countAppReq)
	// total, err := s.dao.GetAppCount(ctx, corp.GetCorpPrimaryId(), nil, req.GetAppBizIds(), []string{}, []uint32{entity.AppIsNotDeleted}, "", "")
	if err != nil {
		return rsp, err
	}
	rsp.Total = uint32(total)
	if rsp.Total == 0 {
		logx.I(ctx, "GetCorpCharacterUsage|rsp:%+v", rsp)
		return rsp, nil
	}

	// 应用详情
	appListReq := appconfig.ListAppBaseInfoReq{
		CorpPrimaryId: corp.GetCorpPrimaryId(),
		PageNumber:    req.GetPage(),
		PageSize:      req.GetPageSize(),
	}
	apps, _, err := s.rpc.AppAdmin.ListAppBaseInfo(ctx, &appListReq)
	// apps, err := s.dao.GetAppListOrderByUsedCharSize(ctx, corp.GetCorpPrimaryId(), req.GetAppBizIds(), req.GetPage(), req.GetPageSize())
	if err != nil {
		return rsp, err
	}
	shareAppBizIDs, appIDs := make([]uint64, 0, len(apps)), make([]uint64, 0, len(apps))
	for _, app := range apps {
		appIDs = append(appIDs, app.PrimaryId)
		if app.IsShared {
			shareAppBizIDs = append(shareAppBizIDs, app.BizId)
		}
	}
	if len(appIDs) == 0 {
		logx.I(ctx, "GetCorpCharacterUsage|rsp:%+v", rsp)
		return rsp, nil
	}
	appDocExceedSizeMap, err := s.docLogic.GetRobotDocExceedUsage(ctx, corp.GetCorpPrimaryId(), appIDs)
	if err != nil {
		return rsp, err
	}
	appQAExceedCharSizeMap, err := s.qaLogic.GetRobotQAExceedUsage(ctx, corp.GetCorpPrimaryId(), appIDs)
	if err != nil {
		return rsp, err
	}
	// 获取共享知识库名字
	shareknowInfoByID := make(map[uint64]*kbEntity.SharedKnowledgeInfo, len(shareAppBizIDs))
	if len(shareAppBizIDs) > 0 {
		shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
			CorpBizID: req.GetCorpBizId(),
			BizIds:    shareAppBizIDs,
		}
		shareknowList, err := s.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
		if err != nil && !errors.Is(err, errx.ErrNotFound) {
			logx.E(ctx, "GetCorpCharacterUsage RetrieveBaseSharedKnowledge err:%v,corpBizID:%v,shareAppBizIDs:%v",
				err, contextx.Metadata(ctx).CorpBizID(), shareAppBizIDs)
			return rsp, err
		}
		for _, v := range shareknowList {
			shareknowInfoByID[v.BusinessID] = v
		}
	}
	usageInfo := make([]*pb.GetCorpCharacterUsageRsp_UsageInfo, 0)
	for _, app := range apps {
		tmp := &pb.GetCorpCharacterUsageRsp_UsageInfo{
			AppBizId:       app.BizId,
			UsedCharSize:   app.UsedCharSize,
			ExceedCharSize: uint64(appDocExceedSizeMap[app.PrimaryId].CharSize + appQAExceedCharSizeMap[app.PrimaryId].CharSize),
			KnowledgeBizId: app.BizId,
			KnowledgeType:  pb.GetCorpCharacterUsageRsp_AppDefaultKnowledge,
			KnowledgeName:  app.Name,
		}
		if shareInfo, ok := shareknowInfoByID[app.BizId]; ok {
			tmp.KnowledgeType = pb.GetCorpCharacterUsageRsp_SharedKnowledge
			tmp.KnowledgeName = shareInfo.Name
		}
		usageInfo = append(usageInfo, tmp)
	}
	rsp.UsageInfo = usageInfo
	logx.I(ctx, "GetCorpCharacterUsage|rsp:%+v", rsp)
	return rsp, nil
}

// GetCharacterUsage 获取字符使用量与容量
func (s *Service) GetCharacterUsage(ctx context.Context, req *pb.InternalGetCharacterUsageReq) (*pb.GetCharacterUsageRsp, error) {
	var err error
	rsp := &pb.GetCharacterUsageRsp{}
	corpID := req.GetCorpPrimaryId()
	knowledgeBaseID := uint64(0)
	if req.GetKnowledgeBaseId() != "" {
		knowledgeBaseID, err = strconv.ParseUint(req.GetKnowledgeBaseId(), 10, 64)
		if err != nil {
			logx.E(ctx, "GetCharacterUsage, ParseUint failed, knowledgeBaseID: %s, err: %+v",
				req.GetKnowledgeBaseId(), err)
			return rsp, err
		}
	}
	return s.kbLogic.GetKnowledgeBaseUsage(ctx, corpID, knowledgeBaseID)
}
