package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx/validx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	dao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	commonLogic "git.woa.com/adp/kb/kb-config/internal/logic/common"
	kbLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	"git.woa.com/adp/pb-go/common"
	kbpb "git.woa.com/adp/pb-go/kb/kb_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
	"github.com/spf13/cast"
)

// DescribeKnowledgeBase 查询知识库信息
func (s *Service) DescribeKnowledgeBase(ctx context.Context,
	req *pb.DescribeKnowledgeBaseReq) (*pb.DescribeKnowledgeBaseRsp, error) {
	logx.I(ctx, "DescribeKnowledgeBase req = %+v", req)
	knowledgeBizIds, err := validx.CheckAndParseUint64Slice(req.GetKnowledgeBizIds())
	if err != nil {
		logx.I(ctx, "DescribeKnowledgeBase check ids req:%+v error:%v", req.GetKnowledgeBizIds(), err)
		return nil, err
	}
	knowledgeBizIds = slicex.Unique(knowledgeBizIds)
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	kbs, err := s.kbDao.GetKnowledgeBases(ctx, corpBizID, knowledgeBizIds)
	if err != nil {
		logx.E(ctx, "DescribeKnowledgeBase kbLogic.DescribeKnowledgeBase req:%+v error:%v",
			req.GetKnowledgeBizIds(), err)
		return nil, err
	}

	// 没查到的知识库赋个默认值，查到的转换下 processing flag
	kbMap := make(map[uint64]*kbe.KnowledgeBase)
	for _, v := range kbs {
		kbMap[v.KnowledgeBizId] = v
	}
	result := make([]*kbpb.KnowledgeBaseInfo, 0, len(knowledgeBizIds))
	for _, id := range knowledgeBizIds {
		kbi := &kbpb.KnowledgeBaseInfo{KnowledgeBizId: id}
		if _, ok := kbMap[id]; ok {
			kbi.ProcessingFlags = getProcessingFlags(kbMap[id])
		}
		result = append(result, kbi)
	}
	return &pb.DescribeKnowledgeBaseRsp{KnowledgeBases: result}, nil
}

// getProcessingFlags 遍历枚举值判断知识库是否包含各种状态标记
func getProcessingFlags(knowledgeBase *kbe.KnowledgeBase) []kbpb.KnowledgeBaseInfo_ProcessingFlag {
	processingFlags := make([]kbpb.KnowledgeBaseInfo_ProcessingFlag, 0)
	for val := range kbpb.KnowledgeBaseInfo_ProcessingFlag_name {
		if val == 0 {
			continue
		}
		if knowledgeBase.HasProcessingFlag(uint64(val)) {
			processingFlags = append(processingFlags, kbpb.KnowledgeBaseInfo_ProcessingFlag(val))
		}
	}
	return processingFlags
}

// getLatestKnowledgeUpdateTime 获取知识库最新更新时间（文档和问答的最大值）
func (s *Service) getLatestKnowledgeUpdateTime(ctx context.Context, corpPrimaryId, robotPrimaryId uint64) (int64, error) {
	docUpdateTime, err := s.docLogic.GetLatestDocUpdateTime(ctx, corpPrimaryId, robotPrimaryId)
	if err != nil {
		logx.E(ctx, "GetLatestDocUpdateTime failed, robotId: %d, err: %+v", robotPrimaryId, err)
		return 0, err
	}
	qaDocUpdateTime, err := s.qaLogic.GetLatestDocQaUpdateTime(ctx, corpPrimaryId, robotPrimaryId)
	if err != nil {
		logx.E(ctx, "GetLatestDocQaUpdateTime failed, robotId: %d, err: %+v", robotPrimaryId, err)
		return 0, err
	}
	// 返回两者中的最大值
	return gox.IfElse(docUpdateTime > qaDocUpdateTime, docUpdateTime, qaDocUpdateTime), nil
}

// ListReferShareKnowledge 查看应用引用的共享知识库列表
func (s *Service) ListReferShareKnowledge(ctx context.Context,
	req *pb.ListReferSharedKnowledgeReq) (*pb.ListReferSharedKnowledgeRsp, error) {
	start := time.Now()
	rsp := new(pb.ListReferSharedKnowledgeRsp)
	logx.I(ctx, "ListReferShareKnowledge Req: %s", req)
	corpID := contextx.Metadata(ctx).CorpID()
	appDB, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if corpID != 0 && appDB.CorpPrimaryId != corpID {
		return rsp, errs.ErrPermissionDenied
	}
	if req.GetReturnDefaultKb() {
		updateTime, err := s.getLatestKnowledgeUpdateTime(ctx, appDB.CorpPrimaryId, appDB.PrimaryId)
		if err != nil {
			logx.E(ctx, "getLatestKnowledgeUpdateTime failed, err: %+v", err)
			return nil, err
		}
		log.InfoContextf(ctx, "getLatestKnowledgeUpdateTime : %v", updateTime)
		// 默认知识库 也返回出去
		defaultKnowledge := &pb.KnowledgeBaseInfo{
			KnowledgeBizId: req.GetAppBizId(),
			UpdateTime:     gox.IfElse(appDB.UpdateTime.Unix() > updateTime, appDB.UpdateTime.Unix(), updateTime),
		}
		rsp.List = append(rsp.List, defaultKnowledge)
	}
	var shareKGList []*kbe.AppShareKnowledge
	if req.GetScope() == pb.ReferSharedKnowledgeScope_SCOPE_TYPE_RELEASE { //  获取发布域下 应用引用共享库列表
		shareKGList, err = s.kbDao.GetAppShareKGListProd(ctx, req.GetAppBizId())
		if err != nil {
			logx.E(ctx, "GetAppShareKGListProd failed, err: %+v", err)
			return nil, errs.ErrGetAppShareProdKGListFailed
		}
	} else {
		//  获取开发域下 应用引用共享库列表
		shareKGList, err = s.kbDao.GetAppShareKGList(ctx, req.GetAppBizId())
		if err != nil {
			logx.E(ctx, "GetAppShareKGList failed, err: %+v", err)
			return nil, errs.ErrGetAppShareKGListFailed
		}
	}
	if len(shareKGList) == 0 {
		logx.W(ctx, "ListReferShareKnowledge GetAppShareKGList is empty")
		return rsp, nil
	}
	// 2. 批量获取共享库详情
	var shareKGBizIDs []uint64
	var shareKGAppBizIDs []uint64
	for _, val := range shareKGList {
		shareKGBizIDs = append(shareKGBizIDs, val.KnowledgeBizID)
		shareKGAppBizIDs = append(shareKGAppBizIDs, val.AppBizID)
	}
	shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
		CorpBizID: appDB.CorpBizId,
		BizIds:    shareKGBizIDs,
	}
	shareKGInfoList, err := s.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil {
		if errors.Is(err, errx.ErrNotFound) {
			logx.W(ctx, "ListReferShareKnowledge RetrieveBaseSharedKnowledge is empty")
			return rsp, nil
		}
		logx.E(ctx, "RetrieveBaseSharedKnowledge failed, err: %+v", err)
		return nil, errs.ErrRetrieveBaseSharedKGFailed
	}
	shareKGInfoMap := make(map[uint64]*kbe.SharedKnowledgeInfo)
	for _, info := range shareKGInfoList {
		shareKGInfoMap[info.BusinessID] = info
	}
	appDBs, _, err := s.rpc.AppAdmin.ListAppBaseInfo(ctx, &appconfig.ListAppBaseInfoReq{
		// IsShared:   ptrx.Bool(true),
		AppBizIds:  shareKGAppBizIDs,
		PageNumber: 1,
		PageSize:   uint32(len(shareKGAppBizIDs)),
	})
	if err != nil {
		logx.E(ctx, "ListAppBaseInfo failed, err: %+v", err)
		return nil, err
	}
	// 创建 appDBMap，以 BizId 为 key，PrimaryId 为 value
	appDBMap := make(map[uint64]uint64)
	for _, app := range appDBs {
		appDBMap[app.BizId] = app.PrimaryId
	}
	for _, val := range shareKGList {
		// 通过 AppBizID 查找对应的 PrimaryId
		PrimaryId, ok := appDBMap[val.AppBizID]
		if !ok {
			logx.W(ctx, "appDBMap[%d] not found", val.AppBizID)
			continue
		}
		KnowledgeUpdateTime, err := s.getLatestKnowledgeUpdateTime(ctx, appDB.CorpPrimaryId, PrimaryId)
		if err != nil {
			logx.E(ctx, "getLatestKnowledgeUpdateTime failed, err: %+v", err)
			return nil, err
		}
		// 还要再对比共享知识库的updateTime
		shareKbInfo, err := s.kbLogic.ListBaseSharedKnowledge(ctx, appDB.CorpBizId, []uint64{val.KnowledgeBizID}, 1, 10, "", contextx.Metadata(ctx).SpaceID())
		if err != nil {
			logx.E(ctx, "ListBaseSharedKnowledge failed, err: %+v", err)
			return nil, err
		}
		if len(shareKbInfo) == 0 {
			continue
		}
		updateTime := gox.IfElse(shareKbInfo[0].UpdateTime.Unix() > KnowledgeUpdateTime, shareKbInfo[0].UpdateTime.Unix(), KnowledgeUpdateTime)
		shareKGInfo, ok := shareKGInfoMap[val.KnowledgeBizID]
		if !ok || shareKGInfo == nil {
			logx.W(ctx, "shareKGInfoMap[%d] not found", val.KnowledgeBizID)
			continue
		}
		rsp.List = append(rsp.List, &pb.KnowledgeBaseInfo{
			UpdateTime:           updateTime,
			KnowledgeBizId:       val.KnowledgeBizID,
			KnowledgeName:        shareKGInfo.Name,
			KnowledgeDescription: shareKGInfo.Description,
		})
	}
	rsp.Total = uint64(len(rsp.List))
	logx.I(ctx, "ListReferShareKnowledge Rsp: %s, cost: %d", rsp, time.Since(start).Milliseconds())
	return rsp, nil
}

// ReferShareKnowledge 引用共享知识库
// 应用可以关联多个共享知识库，此接口会根据提交的应用、知识库id列表，做添加或删除
func (s *Service) ReferShareKnowledge(ctx context.Context,
	req *pb.ReferSharedKnowledgeReq) (*pb.ReferSharedKnowledgeRsp, error) {
	start := time.Now()
	logx.I(ctx, "ReferShareKnowledge Req =====: %s", req)
	corpID := contextx.Metadata(ctx).CorpID()
	rsp := new(pb.ReferSharedKnowledgeRsp)

	// 1. 获取应用信息并校验权限
	appDB, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if appDB.CorpPrimaryId != corpID {
		return rsp, errs.ErrPermissionDenied
	}
	logx.I(ctx, "============appDB.CorpPrimaryId========= %v", appDB.CorpPrimaryId)

	// 2. 调用 logic 层处理业务逻辑
	params := &kbEntity.ReferShareKnowledgeRequest{
		AppBizID:        req.GetAppBizId(),
		AppPrimaryID:    appDB.PrimaryId,
		KnowledgeBizIDs: req.GetKnowledgeBizId(),
		CorpPrimaryID:   appDB.CorpPrimaryId,
		CorpBizID:       appDB.CorpBizId,
		SpaceID:         appDB.SpaceId,
		AppName:         appDB.Name,
	}
	if err := s.kbLogic.ReferShareKnowledge(ctx, params); err != nil {
		return rsp, err
	}

	logx.I(ctx, "ReferShareKnowledge Rsp cost: %d", time.Since(start).Milliseconds())
	return rsp, nil
}

func (s *Service) CreateSharedKnowledge(ctx context.Context, req *pb.CreateSharedKnowledgeReq) (
	*pb.CreateSharedKnowledgeRsp, error) {
	start := time.Now()
	var err error
	rsp := new(pb.CreateSharedKnowledgeRsp)
	logx.I(ctx, "CreateSharedKnowledge, request: %+v", req)
	defer func() {
		logx.I(ctx, "CreateSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	// 获取上下文信息
	corpBizID, staffBizID, staffPrimaryID := contextx.Metadata(ctx).CorpBizID(), contextx.Metadata(ctx).StaffBizID(), contextx.Metadata(ctx).StaffID()
	if corpBizID == 0 || staffBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, err
	}

	// 设置默认 SpaceID
	if req.GetSpaceId() == "" {
		req.SpaceId = kbEntity.DefaultSpaceID
	}
	uin, _ := kbe.GetLoginUinAndSubAccountUin(ctx)
	// 调用 logic 层处理业务逻辑
	params := &kbEntity.CreateSharedKnowledgeRequest{
		Uin:                     uin,
		CorpBizID:               corpBizID,
		StaffBizID:              staffBizID,
		StaffPrimaryID:          staffPrimaryID,
		SpaceID:                 req.GetSpaceId(),
		Name:                    req.GetKnowledgeName(),
		Description:             req.GetKnowledgeDescription(),
		EmbeddingModel:          req.GetEmbeddingModel(),
		SharedKnowledgeAppBizID: 0, // 如果 pb 协议中有此字段，可以传入 req.GetSharedKnowledgeAppBizId()
	}

	result, err := s.kbLogic.CreateSharedKnowledge(ctx, params)
	if err != nil {
		return rsp, err
	}

	rsp.KnowledgeBizId = result.KnowledgeBizID
	return rsp, nil
}

// DeleteSharedKnowledge 删除共享知识库
func (s *Service) DeleteSharedKnowledge(ctx context.Context, req *pb.DeleteSharedKnowledgeReq) (
	*pb.DeleteSharedKnowledgeRsp, error) {
	start := time.Now()
	var err error
	rsp := new(pb.DeleteSharedKnowledgeRsp)
	logx.I(ctx, "DeleteSharedKnowledge, request: %+v", req)
	defer func() {
		logx.I(ctx, "DeleteSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()
	if !kbLogic.VerifyData([]*kbLogic.DataValidation{
		{
			Data:      req.GetKnowledgeBizId(),
			Validator: kbLogic.NewRangeValidator(kbLogic.WithMin(1))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, err
	}
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, err
	}
	deleteKbName := ""
	spaceId := ""
	shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
		CorpBizID: corpBizID,
		BizIds:    []uint64{req.GetKnowledgeBizId()},
	}
	if kbList, err := s.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter); err != nil {
		return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
	} else {
		if len(kbList) == 0 {
			logx.E(ctx, "DeleteSharedKnowledge failed, knowledge biz id: %d, error: %+v",
				req.GetKnowledgeBizId(), err)
			return rsp, errs.ErrSharedKnowledgeRecordNotFound
		}
		deleteKbName = kbList[0].Name
		spaceId = kbList[0].SpaceId
	}

	// 检查关联的应用
	shareKGAppList, err := s.kbDao.GetShareKGAppBizIDList(ctx, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		logx.E(ctx, "DeleteSharedKnowledge failed, error: %+v", err)
		if errors.Is(err, errx.ErrNotFound) {
			return rsp, errs.ErrSharedKnowledgeRecordNotFound
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}

	if len(shareKGAppList) > 0 {
		validAppList, err := kbLogic.ConvertSharedKnowledgeAppInfo(ctx, s.rpc, shareKGAppList)
		if err != nil {
			return rsp, errs.ErrSharedKnowledgeConvertFailed
		}

		if len(validAppList) > 0 {
			err = errs.ErrRelatedAppExist
			logx.E(ctx, "DeleteSharedKnowledge failed, validAppList: %+v, error: %+v",
				validAppList, err)
			return rsp, err
		}
	}
	// 删除共享应用
	uin := contextx.Metadata(ctx).Uin()
	if _, err = s.rpc.AppAdmin.DeleteShareKnowledgeBaseApp(ctx, uin, req.GetKnowledgeBizId()); err != nil {
		logx.W(ctx, "rpc DeleteShareKnowledgeBaseApp failed, err: %+v", err)
		return rsp, errs.ErrDeleteSharedKnowledgeAppFailed
	}
	// NOTICE: 删除模型配置
	err = s.kbDao.DeleteKnowledgeConfigs(ctx, corpBizID, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		logx.W(ctx, "s.kbDao.DeleteKnowledgeConfigs failed, err: %+v", err)
		return rsp, errs.ErrDeleteKnowledgeModelConfigFailed
	}

	// 删除共享库记录
	_, err = s.kbDao.DeleteSharedKnowledge(ctx, corpBizID, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		logx.W(ctx, "s.kbDao.DeleteSharedKnowledge failed, err: %+v", err)
		return rsp, errs.ErrDeleteSharedKnowledgeRecordFailed
	}
	rsp.KnowledgeBizId = req.GetKnowledgeBizId()
	// 上报操作日志
	auditx.Delete(auditx.BizKB).Corp(corpBizID).Space(spaceId).Log(ctx, rsp.KnowledgeBizId, deleteKbName)
	return rsp, nil
}

// ListSharedKnowledge 列举共享知识库
func (s *Service) ListSharedKnowledge(ctx context.Context, req *pb.ListSharedKnowledgeReq) (*pb.ListSharedKnowledgeRsp, error) {
	start := time.Now()
	var err error
	rsp := new(pb.ListSharedKnowledgeRsp)
	logx.I(ctx, "ListSharedKnowledge, request: %+v", req)
	defer func() {
		logx.I(ctx, "ListSharedKnowledge, response: %+v, elapsed: %d, error: %+v", rsp, time.Since(start).Milliseconds(), err)
	}()

	if !kbLogic.VerifyData([]*kbLogic.DataValidation{
		{Data: req.GetPageNumber(), Validator: kbLogic.NewRangeValidator(kbLogic.WithMin(1))},
		{Data: req.GetPageSize(), Validator: kbLogic.NewRangeValidator(
			kbLogic.WithMin(1), kbLogic.WithMax(kbLogic.SharedKnowledgeMaxPageSize))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, err
	}
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}
	spaceID := gox.IfElse(req.GetSpaceId() != "", req.GetSpaceId(), kbEntity.DefaultSpaceID)
	hasAllResourcePerm, otherAllPermissionIDs, shareKnowledgeBizIDList, mapShareKnowledgeBizIDs, err := s.getUserResource(ctx,
		spaceID, common.ResourceType_ResourceTypeKnowledge)
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "ListSharedKnowledge, hasAllResourcePerm: %v, otherAllPermissionIDs: %v, "+
		"shareKnowledgeBizIDList: %v, mapShareKnowledgeBizIDs: %v", hasAllResourcePerm, otherAllPermissionIDs,
		shareKnowledgeBizIDList, mapShareKnowledgeBizIDs)
	if hasAllResourcePerm {
		shareKnowledgeBizIDList = []uint64{}
	} else if len(shareKnowledgeBizIDList) == 0 {
		return rsp, nil
	}
	knowledgeCount, err := s.kbDao.RetrieveSharedKnowledgeCount(ctx, corpBizID, shareKnowledgeBizIDList,
		req.GetKeyword(), spaceID)
	if err != nil {
		return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
	}

	logx.I(ctx, "ListSharedKnowledge, knowledgeCount: %d", knowledgeCount)
	rsp.Total = uint32(knowledgeCount)
	if knowledgeCount == 0 {
		return rsp, nil
	}
	logx.I(ctx, "ListSharedKnowledge, shareKnowledgeBizIDList: %v", shareKnowledgeBizIDList)
	baseKnowledgeList, err := s.kbDao.ListBaseSharedKnowledge(ctx, corpBizID, shareKnowledgeBizIDList,
		req.GetPageNumber(), req.GetPageSize(), req.GetKeyword(), spaceID)
	if err != nil {
		return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
	}
	logx.I(ctx, "ListSharedKnowledge.ListBaseSharedKnowledge, baseKnowledgeList(%d): %+v",
		len(baseKnowledgeList), baseKnowledgeList)
	if len(baseKnowledgeList) == 0 {
		return rsp, nil
	}
	ownerStaffIDs := make([]uint64, 0)
	for _, item := range baseKnowledgeList {
		ownerStaffIDs = append(ownerStaffIDs, item.OwnerStaffID)
	}
	ownerStaffIDs = slicex.Unique(ownerStaffIDs)
	staffs, err := s.rpc.PlatformAdmin.DescribeStaffList(ctx, &pm.DescribeStaffListReq{StaffIds: ownerStaffIDs})
	if err != nil {
		logx.E(ctx, "DescribeCorpStaffList failed, ownerStaffIDs: %+v, error: %+v", ownerStaffIDs, err)
		return nil, errs.ErrStaffNotFound
	}
	for _, item := range baseKnowledgeList {
		staff, ok := staffs[item.OwnerStaffID]
		if ok {
			item.OwnerStaffName = staff.NickName
		}
	}

	// NOTICE: 检索模型配置
	baseKnowledgeList, err = s.kbLogic.RetrieveModelConfig(ctx, corpBizID, baseKnowledgeList)
	if err != nil {
		return rsp, errs.ErrQueryKnowledgeModelConfigFailed
	}
	logx.I(ctx, "ListSharedKnowledge.RetrieveModelConfig, baseKnowledgeList(%d): %+v",
		len(baseKnowledgeList), baseKnowledgeList)

	knowledgeBizIDList := slicex.Map(baseKnowledgeList, func(item *kbEntity.SharedKnowledgeInfo) uint64 {
		return item.BusinessID
	})
	shareKGAppList, err := s.kbDao.GetShareKGAppBizIDList(ctx, knowledgeBizIDList)
	if err != nil {
		logx.E(ctx, "ListSharedKnowledge failed, knowledgeBizIDList: %+v, error: %+v",
			knowledgeBizIDList, err)
		return nil, errs.ErrGetShareKnowledgeAppListFailed
	}
	logx.I(ctx, "ListSharedKnowledge, shareKGAppList(%d): %+v",
		len(shareKGAppList), shareKGAppList)

	// 获取应用基础信息
	var knowledgeAppMap map[uint64][]*pb.AppBaseInfo
	if len(shareKGAppList) > 0 {
		knowledgeAppMap, err = kbLogic.ConvertAppBySharedKnowledge(ctx, s.rpc, shareKGAppList)
		if err != nil {
			return nil, errs.ErrSharedKnowledgeConvertFailed
		}
		logx.I(ctx, "ListSharedKnowledge, knowledgeAppMap(%d): %+v",
			len(knowledgeAppMap), knowledgeAppMap)
	}

	rsp.KnowledgeList = kbLogic.GenerateSharedKnowledgeDetailList(ctx,
		baseKnowledgeList, knowledgeAppMap, otherAllPermissionIDs, mapShareKnowledgeBizIDs)
	return rsp, nil
}

func (s *Service) DescribeSharedKnowledge(ctx context.Context, req *pb.DescribeSharedKnowledgeReq) (
	*pb.DescribeSharedKnowledgeRsp, error) {
	start := time.Now()
	var err error
	rsp := new(pb.DescribeSharedKnowledgeRsp)
	logx.I(ctx, "DescribeSharedKnowledge, request: %+v", req)
	defer func() {
		logx.I(ctx, "DescribeSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if !kbLogic.VerifyData([]*kbLogic.DataValidation{
		{
			Data:      req.GetKnowledgeBizId(),
			Validator: kbLogic.NewRangeValidator(kbLogic.WithMin(1))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, errs.ErrParameterInvalid
	}
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}

	shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
		CorpBizID: corpBizID,
		BizIds:    []uint64{req.GetKnowledgeBizId()},
	}
	knowledgeList, err := s.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil {
		if errors.Is(err, errx.ErrNotFound) {
			return rsp, errs.ErrSharedKnowledgeRecordNotFound
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}

	logx.I(ctx, "DescribeSharedKnowledge.RetrieveBaseSharedKnowledge, knowledgeList: %+v",
		knowledgeList)

	// NOTICE: 检索模型配置
	knowledgeList, err = s.kbLogic.RetrieveModelConfig(ctx, corpBizID, knowledgeList)
	if err != nil {
		return rsp, errs.ErrQueryKnowledgeModelConfigFailed
	}
	logx.I(ctx, "DescribeSharedKnowledge.SupplyModelConfig, knowledgeList: %+v",
		knowledgeList)

	// 查询关联APP
	shareKGAppList, err := s.kbDao.GetShareKGAppBizIDList(ctx, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		logx.E(ctx, "DescribeSharedKnowledge failed, knowledgeBizId: %d, error: %+v",
			req.GetKnowledgeBizId(), err)
		return rsp, errs.ErrGetShareKnowledgeAppListFailed
	}

	var appList []*pb.AppBaseInfo
	if len(shareKGAppList) > 0 {
		appList, err = kbLogic.ConvertSharedKnowledgeAppInfo(ctx, s.rpc, shareKGAppList)
		if err != nil {
			return rsp, errs.ErrSharedKnowledgeConvertFailed
		}
	}

	knowledgeInfo, userInfo := kbLogic.ConvertSharedKnowledgeBaseInfo(ctx, knowledgeList[0])
	rsp.Info = &pb.KnowledgeDetailInfo{
		Knowledge: knowledgeInfo,
		AppList:   appList,
		User:      userInfo,
	}

	return rsp, nil
}

// UpdateSharedKnowledge 更新共享知识库
func (s *Service) UpdateSharedKnowledge(ctx context.Context, req *pb.UpdateSharedKnowledgeReq) (
	*pb.UpdateSharedKnowledgeRsp, error) {
	start := time.Now()
	var err error
	rsp := new(pb.UpdateSharedKnowledgeRsp)
	logx.I(ctx, "UpdateSharedKnowledge, request: %+v", req)
	defer func() {
		logx.I(ctx, "UpdateSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()
	if err = s.validateSharedKnowledgeUpdateRequest(ctx, req); err != nil {
		return rsp, err
	}
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if corpBizID == 0 {
		return rsp, errs.ErrContextInvalid
	}

	shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
		CorpBizID: corpBizID,
		BizIds:    []uint64{req.GetKnowledgeBizId()},
	}
	currentKnowledgeList, err := s.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil {
		if errors.Is(err, errx.ErrNotFound) {
			return rsp, errs.ErrSharedKnowledgeRecordNotFound
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}
	logx.I(ctx, "UpdateSharedKnowledge, currentKnowledgeList: %+v", currentKnowledgeList)

	currentKnowledge := currentKnowledgeList[0]
	spaceId := currentKnowledge.SpaceId
	if currentKnowledge.Name != req.GetInfo().GetKnowledgeName() {
		knowledgeNameList := []string{req.GetInfo().GetKnowledgeName()}
		hitKnowledgeList, err := s.kbDao.RetrieveSharedKnowledgeByName(ctx, corpBizID, knowledgeNameList, spaceId)
		if err != nil {
			return rsp, errs.ErrSharedKnowledgeNameQueryFailed
		}
		logx.I(ctx, "UpdateSharedKnowledge, hitKnowledgeList: %+v", hitKnowledgeList)
		if len(hitKnowledgeList) > 0 {
			return rsp, errs.ErrSharedKnowledgeExist
		}
	}

	staffBizID := contextx.Metadata(ctx).StaffBizID()
	if staffBizID == 0 {
		return rsp, errs.ErrContextInvalid
	}

	uin, subAccountUin := kbEntity.GetLoginUinAndSubAccountUin(ctx)
	staffNameResponse, err := s.rpc.PlatformAdmin.DescribeStaff(ctx, uin, subAccountUin)
	if err != nil {
		return rsp, errs.ErrGetUserNameFailed
	}

	user := &pb.UserBaseInfo{
		UserBizId: staffBizID,
		UserName:  staffNameResponse.GetNickName(),
	}
	// 更新知识库记录
	updateReq := &pb.KnowledgeUpdateInfo{
		KnowledgeName:        req.GetInfo().GetKnowledgeName(),
		KnowledgeDescription: req.GetInfo().KnowledgeDescription,
	}

	affectedRow, err := s.kbDao.UpdateSharedKnowledge(ctx, corpBizID, req.GetKnowledgeBizId(),
		user, updateReq)
	if err != nil {
		return rsp, err
	}
	logx.I(ctx, "UpdateSharedKnowledge, affectedRow: %d", affectedRow)

	rsp.KnowledgeBizId = req.GetKnowledgeBizId()
	// 上报操作日志
	auditx.Modify(auditx.BizKB).Corp(corpBizID).Space(spaceId).Log(ctx, rsp.KnowledgeBizId, req.GetInfo().GetKnowledgeName())
	// 上报统计数据
	go func(newCtx context.Context) { // 异步上报
		defer gox.Recover()
		counterInfo := &commonLogic.CounterInfo{
			CorpBizId:       corpBizID,
			SpaceId:         spaceId,
			StatisticObject: common.StatObject_STAT_OBJECT_KB,
			StatisticType:   common.StatType_STAT_TYPE_EDIT,
			ObjectId:        strconv.FormatUint(rsp.KnowledgeBizId, 10),
			ObjectName:      req.GetInfo().GetKnowledgeName(),
			Count:           1,
		}
		commonLogic.Counter(newCtx, counterInfo, s.rpc)
	}(trpc.CloneContext(ctx))
	return rsp, nil
}

func (s *Service) updateKbEmbeddingModel(ctx context.Context, corpBizID, appBizId, knowledgeBizID uint64,
	modifiedEmbeddingName string) error {

	var currentEmbeddingModel string
	var err error
	if appBizId != 0 {
		currentEmbeddingModel, err = s.kbLogic.GetDefaultKnowledgeBaseConfig(ctx, corpBizID, appBizId, knowledgeBizID,
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL), bot_common.AdpDomain_ADP_DOMAIN_DEV)
		if err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, GetDefaultKnowledgeBaseConfig failed, error: %+v", err)
			return err
		}
	} else {
		currentEmbeddingModel, err = s.kbLogic.GetShareKnowledgeBaseConfig(ctx, corpBizID, knowledgeBizID,
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
		if err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, GetShareKnowledgeBaseConfig failed, error: %+v", err)
			return err
		}

	}

	if currentEmbeddingModel == "" {
		defaultEmbeddingModel, err := s.kbLogic.GetDefaultKnowledgeConfigModel(ctx, entity.ModelCategoryEmbedding)
		if err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, GetDefaultEmbeddingModel failed, error: %+v", err)
			return err
		}
		logx.I(ctx, "updateKbEmbeddingModel, GetDefaultEmbeddingModel, currentEmbeddingName: %+v", currentEmbeddingModel)
		currentEmbeddingModel = defaultEmbeddingModel.ModelName
	}

	currentEmbeddingModel = config.App().EmbeddingConfig.GetMappingModelName(currentEmbeddingModel)
	logx.I(ctx, "updateKbEmbeddingModel, currentEmbeddingModel (after mapped): %+v", currentEmbeddingModel)

	modifiedEmbeddingModel := kbe.KnowledgeModel{
		ModelName: modifiedEmbeddingName,
	}

	if modifiedEmbeddingName == "" {
		defaultEmbeddingModel, err := s.kbLogic.GetDefaultKnowledgeConfigModel(ctx, entity.ModelCategoryEmbedding)
		if err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, GetDefaultEmbeddingModel failed, error: %+v", err)
			return err
		}
		modifiedEmbeddingName = defaultEmbeddingModel.ModelName
		modifiedEmbeddingModel.ModelName = defaultEmbeddingModel.ModelName
		modifiedEmbeddingModel.ModelAliasName = defaultEmbeddingModel.ModelAliasName
		logx.I(ctx, "updateKbEmbeddingModel, GetDefaultEmbeddingModel, modifiedEmbeddingModel: %+v", modifiedEmbeddingModel)
	} else {
		modelRsp, err := s.rpc.Resource.GetModelInfo(ctx, corpBizID, modifiedEmbeddingName)
		if err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, GetModelInfo failed, err: %+v", err)
			return err
		}
		modifiedEmbeddingName = modelRsp.GetModelName()
		modifiedEmbeddingModel.ModelName = modelRsp.GetModelName()
		modifiedEmbeddingModel.ModelAliasName = modelRsp.GetAliasName()
	}

	if currentEmbeddingModel != modifiedEmbeddingName {
		logx.I(ctx, "updateKbEmbeddingModel, "+
			"currentKnowledge.EmbeddingModel: %+v, modifiedEmbeddingName: %+v",
			currentEmbeddingModel, modifiedEmbeddingName)

		kbAppBizId := appBizId
		if appBizId == 0 {
			kbAppBizId = knowledgeBizID
		}

		logx.D(ctx, "updateKbEmbeddingModel, get kbApp kbAppBizId: %+v", kbAppBizId)

		kbApp, err := s.rpc.AppAdmin.DescribeAppById(ctx, kbAppBizId)
		if err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, DescribeAppById failed, error: %+v", err)
			return err
		}
		if err = s.checkKnowledgeModifyCondition(ctx, kbApp); err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, checkKnowledgeModifyCondition failed, error: %+v", err)
			return errs.ErrEmbeddingModelUpdateFailed
		}

		// 共享知识库
		if appBizId == 0 && knowledgeBizID != 0 {
			logx.I(ctx, "updateKbEmbeddingModel | SetSharedKnowledge: %d, embeddingModel: %+v",
				knowledgeBizID, modifiedEmbeddingModel)

			if err := s.setShareKnowledgeEmbeddingModel(ctx, knowledgeBizID, modifiedEmbeddingModel); err != nil {
				logx.E(ctx, "updateKbEmbeddingModel, setShareKnowledgeEmbeddingModel failed, error: %+v", err)
				return errs.ErrSystem
			}

			// 更新知识库记录
			affectedRow, err := s.kbDao.UpdateSharedKnowledge(ctx, corpBizID, knowledgeBizID,
				nil, &pb.KnowledgeUpdateInfo{
					EmbeddingModel: modifiedEmbeddingModel.ModelName,
				})
			if err != nil {
				logx.W(ctx, "updateKbEmbeddingModel, UpdateSharedKnowledge dao failed (knowledgeBizID:%d), err:%+v",
					knowledgeBizID, err)
				return err
			}
			logx.I(ctx, "updateKbEmbeddingModel, affectedRow: %d", affectedRow)

		}

		if err := s.kbLogic.OperateAllVectorIndex(ctx, kbApp.PrimaryId, kbApp.BizId, kbApp.Embedding.Version,
			modifiedEmbeddingModel.ModelName, kbe.OperatorCreate); err != nil {
			logx.E(ctx, "updateKbEmbeddingModel, createAllVectorIndex failed, err: %+v", err)
			return errs.ErrEmbeddingModelUpdateFailed
		}

	}
	return nil
}

func (s *Service) checkKnowledgeModifyCondition(ctx context.Context, kbApp *entity.App) error {
	corpID := kbApp.CorpPrimaryId
	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(3)
	wg.Go(func() error {
		logx.I(wgCtx, "UpdateSharedKnowledge, checkDocCount.")
		docCount, err := s.docLogic.GetDocCount(wgCtx, []string{}, &docEntity.DocFilter{
			CorpId:    corpID,
			RobotId:   kbApp.PrimaryId,
			IsDeleted: ptrx.Bool(false),
		})
		if err != nil {
			return err
		}
		if docCount > 0 {
			logx.W(wgCtx, "UpdateSharedKnowledge, checkDocCount, docCount: %d", docCount)
			return errors.New("UpdateSharedKnowledge failed with documents are not empty")
		}
		return nil
	})
	wg.Go(func() error {
		logx.I(wgCtx, "UpdateSharedKnowledge, checkQaCount.")
		qaCount, err := s.qaLogic.GetDocQaCount(wgCtx, []string{}, &qaEntity.DocQaFilter{
			CorpId:    corpID,
			RobotId:   kbApp.PrimaryId,
			IsDeleted: ptrx.Uint32(qaEntity.QAIsNotDeleted),
		})
		if err != nil {
			return err
		}
		if qaCount > 0 {
			logx.W(wgCtx, "UpdateSharedKnowledge, checkQaCount, qaCount: %d", qaCount)
			return errors.New("UpdateSharedKnowledge failed with qas are not empty")
		}
		return nil
	})
	wg.Go(func() error {
		logx.I(wgCtx, "UpdateSharedKnowledge, checkDbSourceCount.")
		_, count, err := s.dbLogic.ListDbSourcesWithTables(wgCtx, kbApp.BizId, 1, 1)
		if err != nil {
			return err
		}
		if count > 0 {
			logx.W(wgCtx, "UpdateSharedKnowledge, checkDbSourceCount, count: %d", count)
			return errors.New("UpdateSharedKnowledge failed with db sources are not empty")
		}
		return nil
	})
	err := wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

// SetKnowledgeBaseConfig 设置知识库配置
func (s *Service) SetKnowledgeBaseConfig(ctx context.Context,
	req *pb.SetKnowledgeBaseConfigReq) (*pb.SetKnowledgeBaseConfigRsp, error) {
	logx.I(ctx, "SetKnowledgeBaseConfig Req:%+v", req)
	pbConfig := &pb.KnowledgeBaseConfig{
		KnowledgeBizId:       req.GetKnowledgeBizId(),
		ConfigTypes:          req.GetConfigTypes(),
		ThirdAclConfig:       req.GetThirdAclConfig(),
		EmbeddingModel:       req.GetEmbeddingModel(),
		QaExtractModel:       req.GetQaExtractModel(),
		KnowledgeSchemaModel: req.GetKnowledgeSchemaModel(),
	}
	err := s.SetKnowledgeBaseConfigInfo(ctx, []*pb.KnowledgeBaseConfig{pbConfig})
	if err != nil {
		return nil, err
	}
	return &pb.SetKnowledgeBaseConfigRsp{}, nil
}

func (s *Service) setShareKnowledgeEmbeddingModel(ctx context.Context, knowledgeBizId uint64,
	embeddingModel kbe.KnowledgeModel) error {
	pbConfig := &pb.KnowledgeBaseConfig{
		KnowledgeBizId: cast.ToString(knowledgeBizId),
		ConfigTypes:    []pb.KnowledgeBaseConfigType{pb.KnowledgeBaseConfigType_EMBEDDING_MODEL},
		EmbeddingModel: embeddingModel.ModelName,
		EmbeddingModelConfig: &pb.EmbeddingModel{
			ModelName: embeddingModel.ModelName,
			AliasName: embeddingModel.ModelAliasName,
		},
	}
	logx.D(ctx, "SetKnowledgeBaseConfigInfo, pbConfig: %+v", pbConfig)
	err := s.SetKnowledgeBaseConfigInfo(ctx, []*pb.KnowledgeBaseConfig{pbConfig})
	if err != nil {
		return err
	}
	return nil
}

// GetKnowledgeBaseConfig 获取知识库配置
func (s *Service) GetKnowledgeBaseConfig(ctx context.Context,
	req *pb.GetKnowledgeBaseConfigReq) (*pb.GetKnowledgeBaseConfigRsp, error) {
	logx.I(ctx, "GetKnowledgeBaseConfig Req ======== :%+v", req)
	appBaseInfo, err := s.rpc.AppAdmin.GetAppBaseInfo(ctx, cast.ToUint64(req.GetKnowledgeBizId()))
	if err != nil {
		logx.E(ctx, "GetKnowledgeBaseConfig error: %+v", err)
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo == nil {
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo.CorpPrimaryId != contextx.Metadata(ctx).CorpID() {
		logx.E(ctx, "auth bypass, appBaseInfo.CorpPrimaryId:%d, ctx.CorpID:%d", appBaseInfo.CorpPrimaryId, contextx.Metadata(ctx).CorpID())
		return nil, errs.ErrAppNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, appBaseInfo.SpaceId, appBaseInfo.Uin)
	rsp := new(pb.GetKnowledgeBaseConfigRsp)
	pbConfigList, err := s.GetKnowledgeBaseConfigInfo(newCtx, []string{req.GetKnowledgeBizId()}, []string{}, req.GetConfigTypes(), 0, 0)
	if err != nil {
		return nil, err
	}
	if len(pbConfigList) > 0 {
		rsp.KnowledgeBaseConfig = pbConfigList[0]
	}
	logx.I(ctx, "GetKnowledgeBaseConfig Rsp:%+v", rsp)
	return rsp, nil
}

// ===================================非rpc方法，子函数=======================

// GetKnowledgeBaseConfigInfo 获取知识库配置
func (s *Service) GetKnowledgeBaseConfigInfo(ctx context.Context,
	knowledgeBizIds, appBizIds []string, configTypes []pb.KnowledgeBaseConfigType, scenes uint32, releasePrimaryId uint64) ([]*pb.KnowledgeBaseConfig, error) {
	logx.I(ctx, "GetKnowledgeBaseConfigInfo knowledgeBizIds:%+v, appBizIds:%+v, configTypes:%+v, scenes:%d",
		knowledgeBizIds, appBizIds, configTypes, scenes)
	var uiKnowledgeBizIDs []uint64
	for _, knowledgeBizID := range knowledgeBizIds {
		knowledgeBizIdUint64, err := util.CheckReqParamsIsUint64(ctx, knowledgeBizID)
		if err != nil {
			logx.W(ctx, "GetKnowledgeBaseConfigInfo CheckReqParamsIsUint64 knowledgeBizId err:%+v", err)
			return nil, err
		}
		uiKnowledgeBizIDs = append(uiKnowledgeBizIDs, knowledgeBizIdUint64)
	}
	corpBizId := contextx.Metadata(ctx).CorpBizID()
	var pbKnowledgeBaseConfigs []*pb.KnowledgeBaseConfig
	var shareKbPbConfigs []*pb.KnowledgeBaseConfig
	g, gCtx := errgroupx.WithContext(ctx)
	if len(appBizIds) > 0 {
		g.Go(func() error {
			// 请求中的appBizId不为空，则获取appBizId应用下的知识库配置
			uiAppBizIds, err := validx.CheckAndParseUint64Slice(appBizIds)
			if err != nil {
				return err
			}
			appKnowledgeConfigList, err := s.kbLogic.DescribeAppKnowledgeBaseConfigList(gCtx, corpBizId, uiAppBizIds, true, releasePrimaryId)
			if err != nil {
				logx.W(gCtx, "GetKnowledgeBaseConfigInfo DescribeAppKnowledgeBaseConfigList err:%+v", err)
				return err
			}
			if scenes != 0 {
				// 1是评测 2是正式
				pbKnowledgeBaseConfigs, err = s.knowledgeConfigsDbToPb(gCtx, appKnowledgeConfigList, configTypes, scenes)
			} else {
				// 0是同时返回评测和正式
				previewConfigs, err := s.knowledgeConfigsDbToPb(gCtx, appKnowledgeConfigList, configTypes, entity.AppTestScenes)
				if err != nil {
					logx.W(gCtx, "GetKnowledgeBaseConfigInfo entity.AppReleaseScenes KnowledgeConfigsDbToPb err:%+v", err)
					return err
				}
				pbKnowledgeBaseConfigs = append(pbKnowledgeBaseConfigs, previewConfigs...)
				releaseConfigs, err := s.knowledgeConfigsDbToPb(gCtx, appKnowledgeConfigList, configTypes, entity.AppReleaseScenes)
				if err != nil {
					logx.W(gCtx, "GetKnowledgeBaseConfigInfo entity.AppReleaseScenes KnowledgeConfigsDbToPb err:%+v", err)
					return err
				}
				pbKnowledgeBaseConfigs = append(pbKnowledgeBaseConfigs, releaseConfigs...)
			}
			if err != nil {
				logx.W(gCtx, "GetKnowledgeBaseConfigInfo GetShareKBModelNames err:%+v", err)
				return err
			}
			return nil
		})
	}
	if len(uiKnowledgeBizIDs) > 0 {
		g.Go(func() error {
			// 请求中的knowledgeBizIds不为空，则获取共享的知识库配置
			uiConfigTypes := make([]uint32, 0, len(configTypes))
			for _, configType := range configTypes {
				uiConfigTypes = append(uiConfigTypes, uint32(configType))
			}
			knowledgeBaseConfigs, err := s.kbLogic.GetRawShareKnowledgeBaseConfigs(gCtx, corpBizId, uiKnowledgeBizIDs, uiConfigTypes)
			if err != nil {
				return err
			}
			shareKbPbConfigs, err = s.knowledgeConfigsDbToPb(gCtx, knowledgeBaseConfigs, configTypes, entity.AppReleaseScenes)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		logx.E(ctx, "group wait() error:%+v", err)
		return nil, err
	}
	pbKnowledgeBaseConfigs = append(pbKnowledgeBaseConfigs, shareKbPbConfigs...)
	return pbKnowledgeBaseConfigs, nil
}

func (s *Service) knowledgeConfigsDbToPb(ctx context.Context, configs []*kbEntity.KnowledgeConfig, requiredTypes []pb.KnowledgeBaseConfigType, scenes uint32) ([]*pb.KnowledgeBaseConfig,
	error) {
	knowledgeConfigs := make([]*pb.KnowledgeBaseConfig, 0)
	appId2KBConfigList := make(map[uint64][]*kbEntity.KnowledgeConfig)
	isAppKBConfig := false // 本次返回的是否是应用下知识库的配置
	for _, knowledgeConfig := range configs {
		appId2KBConfigList[knowledgeConfig.AppBizID] = append(appId2KBConfigList[knowledgeConfig.AppBizID], knowledgeConfig)
		if knowledgeConfig.AppBizID > 0 {
			isAppKBConfig = true // AppBizID>0，本次返回的是应用下知识库的配置，如果是共享知识库的配置，所有的AppBizID都是0
		}
	}
	for appBizId, configList := range appId2KBConfigList {
		if appBizId == 0 && isAppKBConfig {
			// 应用下知识库的配置，不需要单独返回共享知识库的PB
			continue
		}
		pBKnowledgeConfigMap := make(map[uint64]*pb.KnowledgeBaseConfig)
		var err error
		for _, knowledgeConfig := range configList {
			if knowledgeConfig.KnowledgeBizID == 0 || knowledgeConfig.Type == 0 || (knowledgeConfig.Config == "" && knowledgeConfig.PreviewConfig == "") {
				logx.W(ctx, "KnowledgeConfigsDbToPb knowledgeConfig is empty, knowledgeConfig:%+v", knowledgeConfig)
				continue
			}
			if scenes == entity.AppTestScenes && knowledgeConfig.PreviewConfig == "" {
				// 评测场景下，评测配置为空，直接忽略
				continue
			}
			if scenes == entity.AppReleaseScenes && knowledgeConfig.Config == "" {
				// 发布场景下，发布配置为空，直接忽略
				continue
			}
			if !slices.Contains(requiredTypes, pb.KnowledgeBaseConfigType(knowledgeConfig.Type)) {
				// 如果当前的知识库配置类型不在请求中的类型列表中，可忽略这一配置项；但需要构造一个pb.KnowledgeBaseConfig，用于后续填写默认值
				if _, ok := pBKnowledgeConfigMap[knowledgeConfig.KnowledgeBizID]; !ok {
					pBKnowledgeConfigMap[knowledgeConfig.KnowledgeBizID] = &pb.KnowledgeBaseConfig{
						KnowledgeBizId: strconv.FormatUint(knowledgeConfig.KnowledgeBizID, 10),
						AppBizId:       strconv.FormatUint(knowledgeConfig.AppBizID, 10),
					}
				}
				continue
			}
			pbConfig, ok := pBKnowledgeConfigMap[knowledgeConfig.KnowledgeBizID]
			if !ok {
				pbConfig = &pb.KnowledgeBaseConfig{
					KnowledgeBizId: strconv.FormatUint(knowledgeConfig.KnowledgeBizID, 10),
				}
			}
			pbConfig.ConfigTypes = append(pbConfig.ConfigTypes, pb.KnowledgeBaseConfigType(knowledgeConfig.Type))
			switch pb.KnowledgeBaseConfigType(knowledgeConfig.Type) {
			case pb.KnowledgeBaseConfigType_THIRD_ACL:
				thirdAclConfig := &pb.ThirdAclConfig{}
				err = jsonx.Unmarshal([]byte(knowledgeConfig.Config), thirdAclConfig)
				if err != nil {
					logx.E(ctx, "KnowledgeConfigsDbToPb jsonx.Unmarshal err:%v, knowledgeConfig:%+v", err, knowledgeConfig)
					return nil, err
				}
				pbConfig.ThirdAclConfig = thirdAclConfig
			case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
				configStr := gox.IfElse(scenes == entity.AppTestScenes, knowledgeConfig.PreviewConfig, knowledgeConfig.Config)
				pbConfig.EmbeddingModelConfig, err = s.kbLogic.ConvertStr2EmbeddingModelConfigItem(ctx, configStr, true)
				if err != nil {
					return nil, err
				}
				pbConfig.EmbeddingModel = pbConfig.GetEmbeddingModelConfig().GetModelName()
			case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
				configStr := knowledgeConfig.Config
				if scenes == entity.AppTestScenes && knowledgeConfig.KnowledgeBizID == knowledgeConfig.AppBizID {
					// 只有应用下的默认知识库，才有问答生成模型的预览配置
					configStr = knowledgeConfig.PreviewConfig
				}
				pbConfig.QaExtractModelConfig, err = s.kbLogic.ConvertStr2QAExtractModelConfigItem(ctx, configStr, true)
				if err != nil {
					return nil, err
				}
				pbConfig.QaExtractModel = pbConfig.GetQaExtractModelConfig().GetModelName()
			case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
				configStr := knowledgeConfig.Config
				if scenes == entity.AppTestScenes && knowledgeConfig.KnowledgeBizID == knowledgeConfig.AppBizID {
					// 只有应用下的默认知识库，才有知识库Schema生成模型的预览配置
					configStr = knowledgeConfig.PreviewConfig
				}
				pbConfig.KnowledgeSchemaModelConfig, err = s.kbLogic.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, configStr, true)
				if err != nil {
					return nil, err
				}
				pbConfig.KnowledgeSchemaModel = pbConfig.GetKnowledgeSchemaModelConfig().GetModelName()
			case pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING:
				retrievalConfig := &pb.RetrievalConfig{}
				if scenes == entity.AppTestScenes {
					if len(knowledgeConfig.PreviewConfig) > 0 {
						err = jsonx.Unmarshal([]byte(knowledgeConfig.PreviewConfig), retrievalConfig)
					}
				} else {
					if len(knowledgeConfig.Config) > 0 {
						err = jsonx.Unmarshal([]byte(knowledgeConfig.Config), retrievalConfig)
					}
				}
				if err != nil {
					logx.E(ctx, "KnowledgeConfigsDbToPb jsonx.Unmarshal err:%v, knowledgeConfig:%+v", err, knowledgeConfig)
					return nil, err
				}
				if knowledgeConfig.AppBizID != 0 {
					// 只有应用下的知识库才有AppBizId
					pbConfig.AppBizId = strconv.FormatUint(knowledgeConfig.AppBizID, 10)
				}
				if retrievalConfig.SearchStrategy != nil && retrievalConfig.GetSearchStrategy().GetNatureLanguageToSqlModelConfig().GetModel().GetModelName() == "" {
					// 补齐NL2SQL模型
					defaultNL2SQLModelConfig, err := s.kbLogic.GetDefaultNL2SQLModelConfigItem(ctx)
					if err != nil {
						logx.W(ctx, "Failed to get default nl2sql model, err:%+v", err)
					}
					nl2SqlModelConfig := &pb.NL2SQLModelConfig{
						Model: defaultNL2SQLModelConfig,
					}
					retrievalConfig.SearchStrategy.NatureLanguageToSqlModelConfig = nl2SqlModelConfig
				}
				if retrievalConfig.SearchStrategy != nil {
					s.kbLogic.MapSearchStrategyModels(ctx, retrievalConfig.SearchStrategy)
				}
				pbConfig.RetrievalConfig = retrievalConfig
				// 补齐应用下共享知识库的信息
				if knowledgeConfig.ShareKbName != "" {
					pbConfig.RetrievalConfig.ShareKnowledgeBaseName = knowledgeConfig.ShareKbName
					sharedKbConfigs := appId2KBConfigList[0]
					for _, sharedKbConfig := range sharedKbConfigs {
						if knowledgeConfig.KnowledgeBizID == sharedKbConfig.KnowledgeBizID {
							if sharedKbConfig.Type == uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL) {
								pbConfig.ConfigTypes = append(pbConfig.ConfigTypes, pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL)
								pbConfig.QaExtractModelConfig, _ = s.kbLogic.ConvertStr2QAExtractModelConfigItem(ctx, sharedKbConfig.Config, true)
								pbConfig.QaExtractModel = pbConfig.QaExtractModelConfig.GetModelName()
							} else if sharedKbConfig.Type == uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL) {
								pbConfig.ConfigTypes = append(pbConfig.ConfigTypes, pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL)
								pbConfig.KnowledgeSchemaModelConfig, _ = s.kbLogic.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, sharedKbConfig.Config, true)
								pbConfig.KnowledgeSchemaModel = pbConfig.KnowledgeSchemaModelConfig.GetModelName()
							} else if sharedKbConfig.Type == uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL) {
								pbConfig.ConfigTypes = append(pbConfig.ConfigTypes, pb.KnowledgeBaseConfigType_EMBEDDING_MODEL)
								pbConfig.EmbeddingModelConfig, _ = s.kbLogic.ConvertStr2EmbeddingModelConfigItem(ctx, sharedKbConfig.Config, true)
								pbConfig.EmbeddingModel = pbConfig.EmbeddingModelConfig.GetModelName()
							}
						}
					}
					if !slices.Contains(pbConfig.ConfigTypes, pb.KnowledgeBaseConfigType_EMBEDDING_MODEL) {
						pbConfig.ConfigTypes = append(pbConfig.ConfigTypes, pb.KnowledgeBaseConfigType_EMBEDDING_MODEL)
						pbConfig.EmbeddingModelConfig, _ = s.kbLogic.ConvertStr2EmbeddingModelConfigItem(ctx, "", true)
						pbConfig.EmbeddingModel = pbConfig.EmbeddingModelConfig.GetModelName()
					}
				}
			case pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL:
				logx.D(ctx, "------scenes:%d, preview:%s configStr:%s", scenes, knowledgeConfig.PreviewConfig, knowledgeConfig.Config)
				configStr := gox.IfElse(scenes == entity.AppTestScenes, knowledgeConfig.PreviewConfig, knowledgeConfig.Config)
				pbConfig.FileParseModelConfig, err = s.kbLogic.ConvertStr2FileParseModelConfigItem(ctx, configStr, true)
				if err != nil {
					return nil, err
				}
			}
			pBKnowledgeConfigMap[knowledgeConfig.KnowledgeBizID] = pbConfig
		}
		for _, knowledgeConfig := range pBKnowledgeConfigMap {
			if len(knowledgeConfig.AppBizId) > 0 {
				// 只有应用下的知识库才有场景类型，共享知识库
				knowledgeConfig.Scenes = scenes
			}
			if slices.Contains(requiredTypes, pb.KnowledgeBaseConfigType_EMBEDDING_MODEL) && knowledgeConfig.GetEmbeddingModelConfig().GetModelName() == "" {
				// 需要返回向量化模型，但是没有向量化模型配置，需要使用默认配置
				knowledgeConfig.ConfigTypes = append(knowledgeConfig.ConfigTypes, pb.KnowledgeBaseConfigType_EMBEDDING_MODEL)
				knowledgeConfig.EmbeddingModelConfig, _ = s.kbLogic.ConvertStr2EmbeddingModelConfigItem(ctx, "", true)
			}
			if slices.Contains(requiredTypes, pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL) && knowledgeConfig.GetQaExtractModelConfig().GetModelName() == "" {
				// 需要返回文档生成问答模型，但是没有问答提取模型配置，需要使用默认配置
				knowledgeConfig.ConfigTypes = append(knowledgeConfig.ConfigTypes, pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL)
				knowledgeConfig.QaExtractModelConfig, _ = s.kbLogic.ConvertStr2QAExtractModelConfigItem(ctx, "", true)
			}
			if slices.Contains(requiredTypes, pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL) && knowledgeConfig.GetKnowledgeSchemaModelConfig().GetModelName() == "" {
				// 需要返回知识库schema模型，但是没有知识库schema模型配置，需要使用默认配置
				knowledgeConfig.ConfigTypes = append(knowledgeConfig.ConfigTypes, pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL)
				knowledgeConfig.KnowledgeSchemaModelConfig, _ = s.kbLogic.ConvertStr2KnowledgeSchemaModelConfigItem(ctx, "", true)
			}
			if slices.Contains(requiredTypes, pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL) && knowledgeConfig.GetFileParseModelConfig().GetModelName() == "" {
				// 需要返回文档解析模型，但是没有文档解析模型，需要使用默认配置
				knowledgeConfig.ConfigTypes = append(knowledgeConfig.ConfigTypes, pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL)
				knowledgeConfig.FileParseModelConfig, _ = s.kbLogic.ConvertStr2FileParseModelConfigItem(ctx, "", true)
			}
			knowledgeConfigs = append(knowledgeConfigs, knowledgeConfig)
		}
	}
	return knowledgeConfigs, nil
}

func (s *Service) KnowledgeConfigPB2DO(ctx context.Context, pbConfig *pb.KnowledgeBaseConfig) ([]*kbEntity.KnowledgeConfig, error) {
	if pbConfig == nil {
		logx.W(ctx, "KnowledgeConfigPB2DO pbConfig is nil")
		return nil, nil
	}
	knowledgeBizId, err := util.CheckReqParamsIsUint64(ctx, pbConfig.GetKnowledgeBizId())
	if err != nil {
		logx.E(ctx, "KnowledgeConfigPB2DO CheckReqParamsIsUint64 knowledgeBizId err:%+v", err)
		return nil, err
	}
	knowledgeConfigs := make([]*kbEntity.KnowledgeConfig, 0)
	now := time.Now()
	for _, configType := range pbConfig.ConfigTypes {
		entityConfig := &kbEntity.KnowledgeConfig{
			CorpBizID:      contextx.Metadata(ctx).CorpBizID(),
			KnowledgeBizID: knowledgeBizId,
			Type:           uint32(configType),
			IsDeleted:      false,
			CreateTime:     now,
			UpdateTime:     now,
		}
		configStr := ""
		needUpdateReleasedConfig := false // 是否需要更新已发布的配置
		switch configType {
		case pb.KnowledgeBaseConfigType_THIRD_ACL:
			configStr, err = jsonx.MarshalToString(pbConfig.ThirdAclConfig)
			if err != nil {
				logx.E(ctx, "KnowledgeConfigPB2DO marshal thirdAclConfig fail, err=%+v", err)
				return nil, err
			}
		case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
			if pbConfig.GetEmbeddingModelConfig().GetModelName() != "" {
				// 新配置
				configStr, err = jsonx.MarshalToString(pbConfig.GetEmbeddingModelConfig())
				if err != nil {
					logx.E(ctx, "KnowledgeConfigPB2DO marshal QaExtractModelConfig fail, err=%+v", err)
					return nil, err
				}
			} else {
				// 老配置
				embeddingModel, err := s.kbLogic.ConvertStr2EmbeddingModelConfigItem(ctx, pbConfig.EmbeddingModel, true)
				if err != nil {
					logx.E(ctx, "ConvertStr2EmbeddingModelConfigItem fail, err=%+v", err)
					return nil, err
				}
				configStr, err = jsonx.MarshalToString(embeddingModel)
				if err != nil {
					logx.E(ctx, "KnowledgeConfigPB2DO marshal EmbeddingModel fail, err=%+v", err)
					return nil, err
				}
			}
		case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
			if pbConfig.GetQaExtractModelConfig().GetModelName() != "" {
				// 新配置
				configStr, err = jsonx.MarshalToString(pbConfig.GetQaExtractModelConfig())
				if err != nil {
					logx.E(ctx, "KnowledgeConfigPB2DO marshal QaExtractModelConfig fail, err=%+v", err)
					return nil, err
				}
			} else {
				// 老配置
				qaExtractModel, err := s.kbLogic.ConvertStr2QAExtractModelConfigItem(ctx, pbConfig.QaExtractModel, true)
				if err != nil {
					logx.E(ctx, "ConvertStr2QAExtractModelConfigItem fail, err=%+v", err)
					return nil, err
				}
				configStr, err = jsonx.MarshalToString(qaExtractModel)
				if err != nil {
					logx.E(ctx, "KnowledgeConfigPB2DO marshal QaExtractModelConfig fail, err=%+v", err)
					return nil, err
				}
			}
			needUpdateReleasedConfig = true
		case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
			if pbConfig.GetKnowledgeSchemaModelConfig().GetModelName() != "" {
				// 新配置
				configStr, err = jsonx.MarshalToString(pbConfig.GetKnowledgeSchemaModelConfig())
				if err != nil {
					logx.E(ctx, "KnowledgeConfigPB2DO marshal KnowledgeSchemaModelConfig fail, err=%+v", err)
					return nil, err
				}
			} else {
				// 老配置
				knowledgeSchemaModel, err := s.kbLogic.ConvertStr2QAExtractModelConfigItem(ctx, pbConfig.KnowledgeSchemaModel, true)
				if err != nil {
					logx.E(ctx, "ConvertStr2QAExtractModelConfigItem fail, err=%+v", err)
					return nil, err
				}
				configStr, err = jsonx.MarshalToString(knowledgeSchemaModel)
				if err != nil {
					logx.E(ctx, "KnowledgeConfigPB2DO marshal KnowledgeSchemaModel fail, err=%+v", err)
					return nil, err
				}
			}
			needUpdateReleasedConfig = true
		case pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING:
			configToStore := &pb.RetrievalConfig{
				Retrievals:     pbConfig.RetrievalConfig.Retrievals,
				RetrievalRange: pbConfig.RetrievalConfig.RetrievalRange,
				SearchStrategy: pbConfig.RetrievalConfig.SearchStrategy,
			}
			configStr, err = jsonx.MarshalToString(configToStore)
			if err != nil {
				logx.E(ctx, "KnowledgeConfigPB2DO marshal KnowledgeQaConfig fail, err=%+v", err)
				return nil, err
			}
		case pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL:
			if pbConfig.FileParseModelConfig == nil {
				pbConfig.FileParseModelConfig, err = s.kbLogic.ConvertStr2FileParseModelConfigItem(ctx, "", true)
			}
			configStr, err = jsonx.MarshalToString(pbConfig.FileParseModelConfig)
			if err != nil {
				logx.E(ctx, "KnowledgeConfigPB2DO marshal FileParseModelConfig fail, err=%+v", err)
				return nil, err
			}
			needUpdateReleasedConfig = true
		default:
			logx.E(ctx, "KnowledgeConfigPB2DO configType not support, configType=%+v", configType)
			return nil, fmt.Errorf("configType not support: %v", configType)
		}
		appBizID := cast.ToUint64(pbConfig.GetAppBizId())
		if len(pbConfig.GetAppBizId()) == 0 || appBizID == 0 {
			entityConfig.Config = configStr // 全局共享知识库的配置
		} else {
			// appBizID 有值，则说明是应用下的知识库配置
			// 前端页面调用，knowledgeBizID为空，并且修改评测端配置(previewConfig)
			// app-config的内部调用，knowledgeBizID不为空，appBizID也不为空
			if pbConfig.GetKnowledgeBizId() != "" && pbConfig.GetAppBizId() != pbConfig.GetKnowledgeBizId() {
				// 应用下引用的全局知识库配置，appBizID 和 knowledgeBizID 不一致
				if configType != pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING {
					// 如果不是检索配置--RETRIEVAL_SETTING，则不做存储，因为应用下引用的共享知识库的模型配置需要遵循全局知识库中的模型配置
					logx.D(ctx, "Ignore this item: appBizID(%s)!= knowledgeBizID(%s), configType=%+v",
						pbConfig.GetAppBizId(), pbConfig.GetKnowledgeBizId(), configType)
					continue
				}
			}
			entityConfig.AppBizID = appBizID
			logx.I(ctx, "KnowledgeConfigPB2DO configType=%+v, configStr=%+v, Scenes=%+v (needUpdateReleasedConfig)",
				configType, configStr, pbConfig.GetScenes(), needUpdateReleasedConfig)
			if pbConfig.GetScenes() == entity.AppTestScenes {
				// TODO:这里让前端把KnowledgeBizID=appBizId 过来会安全一些 @wemysschen @yuzhengtao
				if entityConfig.KnowledgeBizID == 0 {
					entityConfig.KnowledgeBizID = appBizID
				}
				entityConfig.PreviewConfig = configStr
				if needUpdateReleasedConfig {
					entityConfig.Config = configStr
				}
			} else {
				entityConfig.Config = configStr
			}
		}
		entityConfig.IsUpdateReleased = needUpdateReleasedConfig
		knowledgeConfigs = append(knowledgeConfigs, entityConfig)
	}
	return knowledgeConfigs, nil
}

// SetKnowledgeBaseConfigInfo 设置知识库配置
func (s *Service) SetKnowledgeBaseConfigInfo(ctx context.Context,
	configs []*pb.KnowledgeBaseConfig) error {
	if contextx.Metadata(ctx).CorpBizID() == 0 {
		return errs.ErrCorpNotFound
	}
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	for _, pbConfig := range configs {
		knowledgeConfigs, err := s.KnowledgeConfigPB2DO(ctx, pbConfig)
		if err != nil {
			logx.E(ctx, "SetKnowledgeBaseConfigInfo KnowledgeConfigPB2DO fail, err=%+v", err)
			return err
		}
		err = s.kbLogic.SetKnowledgeBaseConfig(ctx, corpBizID, knowledgeConfigs)
		if err != nil {
			logx.E(ctx, "SetKnowledgeBaseConfigInfo SetKnowledgeBaseConfig fail, err=%+v", err)
			return err
		}
	}
	return nil
}

func (s *Service) validateSharedKnowledgeUpdateRequest(ctx context.Context, req *pb.UpdateSharedKnowledgeReq) error {
	if !kbLogic.VerifyData([]*kbLogic.DataValidation{
		{
			Data: req.GetKnowledgeBizId(),
			Validator: kbLogic.NewRangeValidator(
				kbLogic.WithMin(1),
			),
		},
		{
			Data: utf8.RuneCountInString(req.GetInfo().GetKnowledgeName()),
			Validator: kbLogic.NewRangeValidator(
				kbLogic.WithMin(1),
				kbLogic.WithMax(float64(i18n.CalculateExpandedLength(ctx,
					i18n.UserInputCharType, kbEntity.ShareKnowledgeNameLength))),
			),
		},
		{
			Data: utf8.RuneCountInString(req.GetInfo().GetKnowledgeDescription()),
			Validator: kbLogic.NewRangeValidator(
				kbLogic.WithMax(float64(i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
					kbEntity.ShareKnowledgeDescriptionLength)),
				)),
		},
	}) {
		return errs.ErrParameterInvalid
	}

	return nil
}

func (s *Service) getUserResource(ctx context.Context, spaceID string, resourceType common.ResourceType) (bool, []string, []uint64, map[uint64][]string,
	error) {
	permissionResource, err := s.rpc.PlatformAdmin.DescribeResourceList(ctx, spaceID, resourceType)
	if err != nil {
		logx.E(ctx, "getUserResource adminApi.GetPermissionResourceList fail, err=%+v", err)
		return false, nil, nil, nil, err
	}
	hasOtherAllPerm := permissionResource.GetHasOtherAllPerm()
	otherAllPermissionIDs := permissionResource.GetOtherAllPermissionIds()
	bizPermissionIDMap := make(map[uint64][]string)
	shareKnowledgeBizIDList := make([]uint64, 0)
	// 处理资源权限位
	for _, v := range permissionResource.GetResourcePermissionIds() {
		shareKnowledgeBizID := cast.ToUint64(v.GetResourceId())
		if !hasOtherAllPerm {
			shareKnowledgeBizIDList = append(shareKnowledgeBizIDList, shareKnowledgeBizID)
		}
		bizPermissionIDMap[shareKnowledgeBizID] = v.GetPermissionIds()
	}
	return hasOtherAllPerm, otherAllPermissionIDs, shareKnowledgeBizIDList, bizPermissionIDMap, nil
}

// DescribeExceededKnowledgeList 获取超量知识列表
// todo cooper 需要确认是否需要返回容量
func (s *Service) DescribeExceededKnowledgeList(ctx context.Context,
	req *pb.DescribeExceededKnowledgeListReq) (*pb.DescribeExceededKnowledgeListRsp, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	return s.kbLogic.DescribeExceededKnowledgeList(ctx, corpID, req.GetSpaceId(),
		req.GetPageNumber(), req.GetPageSize(), true)
}

func (s *Service) describeDocsByAppId(ctx context.Context, appID string, corpID uint64) (*pb.ResumeDocReq, error) {
	// 获取knowledge_base_id对应的robot_id
	baseID, err := util.CheckReqParamsIsUint64(ctx, appID)
	if err != nil {
		return nil, fmt.Errorf("CheckReqParamsIsUint64 failed, err: %w", err)
	}
	app, err := s.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, baseID)
	if err != nil {
		return nil, fmt.Errorf("GetAppByAppBizID failed, err: %w", err)
	}
	if app == nil {
		return nil, errors.New("GetAppByAppBizID failed, app == nil")
	}
	// 知识库文件ID为空，知识库ID不为空，则恢复知识库下面所有的超量失效的文件
	// 获取知识库ID对应的所有doc_biz_id
	docs, err := s.docLogic.GetDao().GetAllDocs(ctx, []string{docEntity.DocTblColBusinessId},
		&docEntity.DocFilter{
			RobotId:        app.PrimaryId,
			CorpId:         corpID,
			Limit:          0,
			Status:         docEntity.DocExceedStatus,
			OrderColumn:    []string{docEntity.DocTblColId},
			OrderDirection: []string{dao.SqlOrderByAsc},
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocs failed, err: %w", err)
	}
	var docIDs []string
	for _, doc := range docs {
		docIDs = append(docIDs, strconv.FormatUint(doc.BusinessID, 10))
	}
	return &pb.ResumeDocReq{
		BotBizId:  appID,
		DocBizIds: docIDs,
	}, nil
}

func (s *Service) describeDocQasByAppId(ctx context.Context, appID string, corpID uint64) (*pb.ResumeQAReq, error) {
	// 获取knowledge_base_id对应的robot_id
	baseID, err := util.CheckReqParamsIsUint64(ctx, appID)
	if err != nil {
		return nil, fmt.Errorf("CheckReqParamsIsUint64 failed, err: %w", err)
	}
	app, err := s.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, baseID)
	if err != nil {
		return nil, fmt.Errorf("GetAppByAppBizID failed, err: %w", err)
	}
	if app == nil {
		return nil, errors.New("GetAppByAppBizID failed, app == nil")
	}
	// 获取问答对应的所有qa_biz_id
	docQas, err := s.qaLogic.GetAllDocQas(ctx, []string{qaEntity.DocQaTblColBusinessId}, &qaEntity.DocQaFilter{
		RobotId:           app.PrimaryId,
		CorpId:            corpID,
		Limit:             0,
		ReleaseStatusList: qaEntity.QaExceedStatus,
		OrderColumn:       []string{qaEntity.DocQaTblColId},
		OrderDirection:    []string{dao.SqlOrderByAsc},
		IsDeleted:         ptrx.Uint32(qaEntity.QAIsNotDeleted),
	})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocQas failed, err: %w", err)
	}
	var qaIDs []string
	for _, docQa := range docQas {
		qaIDs = append(qaIDs, strconv.FormatUint(docQa.BusinessID, 10))
	}
	return &pb.ResumeQAReq{
		BotBizId: appID,
		QaBizIds: qaIDs,
	}, nil
}

func (s *Service) describeDocBySpaceID(ctx context.Context, spaceID string, corpID uint64) ([]*pb.ResumeDocReq, error) {
	// 拉取所有的robot id
	appListReq := appconfig.ListAppBaseInfoReq{SpaceId: spaceID, CorpPrimaryId: corpID}
	apps, _, err := s.rpc.AppAdmin.ListAllAppBaseInfo(ctx, &appListReq)
	if err != nil {
		return nil, fmt.Errorf("ListAllAppBaseInfo failed, err: %w", err)
	}
	botIDToBizID := make(map[uint64]uint64)
	var robotIDs []uint64

	resPermissionMap, err := s.describeAllResPermission(ctx, spaceID)
	if err != nil {
		return nil, fmt.Errorf("describeAllResPermission failed, err: %w", err)
	}
	for _, app := range apps {
		// 过滤掉没有编辑权限的应用
		if !s.hasPermission(app.BizId, resPermissionMap) {
			logx.I(ctx, "app.BusinessID has no edit permission: %d", app.BizId)
			continue
		}
		robotIDs = append(robotIDs, app.PrimaryId)
		botIDToBizID[app.PrimaryId] = app.BizId
	}
	// 拉取当前空间下所有超量的DOC
	docs, err := s.docLogic.GetDao().GetAllDocs(ctx, []string{docEntity.DocTblColBusinessId, docEntity.DocTblColRobotId},
		&docEntity.DocFilter{
			RobotIDs:       robotIDs,
			Limit:          0,
			Status:         docEntity.DocExceedStatus,
			OrderColumn:    []string{docEntity.DocTblColId},
			OrderDirection: []string{dao.SqlOrderByAsc},
			IsDeleted:      ptrx.Bool(false),
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocs failed, err: %w", err)
	}
	appBizDocIDs := make(map[uint64][]string)
	for _, doc := range docs {
		if botBizID, ok := botIDToBizID[doc.RobotID]; ok {
			appBizDocIDs[botBizID] = append(appBizDocIDs[botBizID], strconv.FormatUint(doc.BusinessID, 10))
		}
	}
	var docsInfo []*pb.ResumeDocReq
	for k, v := range appBizDocIDs {
		docsInfo = append(docsInfo, &pb.ResumeDocReq{
			BotBizId:  strconv.FormatUint(k, 10),
			DocBizIds: v,
		})
	}
	return docsInfo, nil
}

func (s *Service) describeDocQasBySpaceID(ctx context.Context, spaceID string, corpID uint64) ([]*pb.ResumeQAReq,
	error) {
	// 拉取所有的robot id
	appListReq := appconfig.ListAppBaseInfoReq{SpaceId: spaceID, CorpPrimaryId: corpID}
	apps, _, err := s.rpc.AppAdmin.ListAllAppBaseInfo(ctx, &appListReq)
	if err != nil {
		return nil, fmt.Errorf("ListAllAppBaseInfo failed, err: %w", err)
	}

	resPermissionMap, err := s.describeAllResPermission(ctx, spaceID)
	if err != nil {
		return nil, fmt.Errorf("describeAllResPermission failed, err: %w", err)
	}

	botIDToBizID := make(map[uint64]uint64)
	var robotIDs []uint64
	for _, app := range apps {
		// 过滤掉没有编辑权限的应用
		if !s.hasPermission(app.BizId, resPermissionMap) {
			logx.I(ctx, "app.BusinessID has no edit permission: %d", app.BizId)
			continue
		}
		robotIDs = append(robotIDs, app.PrimaryId)
		botIDToBizID[app.PrimaryId] = app.BizId
	}
	// 拉取当前空间下所有超量的QA
	docQas, err := s.qaLogic.GetAllDocQas(ctx, []string{qaEntity.DocQaTblColBusinessId, qaEntity.DocQaTblColRobotId},
		&qaEntity.DocQaFilter{
			RobotIDs:          robotIDs,
			Limit:             0,
			ReleaseStatusList: qaEntity.QaExceedStatus,
			OrderColumn:       []string{qaEntity.DocQaTblColId},
			OrderDirection:    []string{dao.SqlOrderByAsc},
			IsDeleted:         ptrx.Uint32(qaEntity.QAIsNotDeleted),
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocQas failed, err: %w", err)
	}
	appBizQaIDs := make(map[uint64][]string)
	for _, qa := range docQas {
		if botBizID, ok := botIDToBizID[qa.RobotID]; ok {
			appBizQaIDs[botBizID] = append(appBizQaIDs[botBizID], strconv.FormatUint(qa.BusinessID, 10))
		}
	}
	var qasInfo []*pb.ResumeQAReq
	for k, v := range appBizQaIDs {
		qasInfo = append(qasInfo, &pb.ResumeQAReq{
			BotBizId: strconv.FormatUint(k, 10),
			QaBizIds: v,
		})
	}
	return qasInfo, nil
}

// ResumeExceedKnowledge 恢复知识库
func (s *Service) ResumeExceedKnowledge(ctx context.Context, req *pb.ResumeExceedKnowledgeReq) (
	*pb.ResumeExceedKnowledgeRsp, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	// 知识库文件ID不为空，则只恢复知识库文件
	var docsInfo []*pb.ResumeDocReq
	var qasInfo []*pb.ResumeQAReq
	for _, knowledgeBizID := range req.GetKnowledgeInfos() {
		if len(knowledgeBizID.GetIds()) == 0 {
			// 知识库文件ID为空，则恢复知识库下的所有文件
			if len(knowledgeBizID.GetKnowledgeBaseId()) == 0 {
				logx.W(ctx, "knowledgeBizID.GetKnowledgeBaseId() is empty, knowledgeBizID:%+v",
					knowledgeBizID)
				continue
			}
			docsI, docQasI, err := s.describeReqKnowledgeIDs(ctx, knowledgeBizID.GetKnowledgeBaseId(), corpID)
			if err != nil {
				logx.E(ctx, "describeReqKnowledgeIDs failed, err: %+v", err)
				return nil, errs.ErrResumeExceedKnowledgeFailed
			}
			if docsI != nil {
				docsInfo = append(docsInfo, docsI)
			}
			if docQasI != nil {
				qasInfo = append(qasInfo, docQasI)
			}
		} else {
			// 知识库文件ID不为空，则只恢复指定知识库文件
			switch knowledgeBizID.Type {
			case pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_DOC:
				docsInfo = append(docsInfo, &pb.ResumeDocReq{
					BotBizId:  knowledgeBizID.GetKnowledgeBaseId(),
					DocBizIds: knowledgeBizID.GetIds(),
				})
			case pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_QA:
				qasInfo = append(qasInfo, &pb.ResumeQAReq{
					BotBizId: knowledgeBizID.GetKnowledgeBaseId(),
					QaBizIds: knowledgeBizID.GetIds(),
				})
			}
		}
	}
	if len(req.GetKnowledgeInfos()) == 0 {
		// 拉取当前空间下所有超量的DOC
		dI, err := s.describeDocBySpaceID(ctx, req.GetSpaceId(), corpID)
		if err != nil {
			logx.E(ctx, "getRobotIDsBySpaceID failed, err: %+v", err)
			return nil, errs.ErrResumeExceedKnowledgeFailed
		}
		docsInfo = append(docsInfo, dI...)
		// 拉取当前空间下所有超量的QA
		qI, err := s.describeDocQasBySpaceID(ctx, req.GetSpaceId(), corpID)
		if err != nil {
			logx.E(ctx, "getRobotIDsBySpaceID failed, err: %+v", err)
			return nil, errs.ErrResumeExceedKnowledgeFailed
		}
		qasInfo = append(qasInfo, qI...)
	}
	// 如果没有可恢复的知识库文件，则直接返回
	if len(docsInfo) == 0 && len(qasInfo) == 0 {
		logx.E(ctx, "docsInfo and qasInfo is empty, req:%+v", req)
		rsp := new(pb.ResumeExceedKnowledgeRsp)
		return rsp, nil
	}

	// 对docBizIds做拆分，防止单个docBizIds过大
	splitDocsInfo := processDocsInfoEnhanced(docsInfo, config.DescribeResumeBatchSize())

	// 分批调用ResumeDoc
	if err := s.processResumeDocReqs(ctx, splitDocsInfo); err != nil {
		logx.E(ctx, "ProcessResumeDocReqs failed, err: %+v", err)
		return nil, errs.ErrResumeExceedKnowledgeFailed
	}
	// 对qaBizIds做拆分，防止单个qaBizIds过大
	splitQasInfo := processQAsInfoWithMaxLength(qasInfo, config.DescribeResumeBatchSize())
	// 分批调用ResumeQA
	if err := s.processResumeDocQaReqs(ctx, splitQasInfo); err != nil {
		logx.E(ctx, "ProcessResumeDocQaReqs failed, err: %+v", err)
		return nil, errs.ErrResumeExceedKnowledgeFailed
	}
	rsp := new(pb.ResumeExceedKnowledgeRsp)
	return rsp, nil
}

// hasPermission 检查是否有权限
func (s *Service) hasPermission(bizID uint64, resPermissionMap map[uint64]bool) bool {
	return s.kbLogic.HasPermission(bizID, resPermissionMap)
}

// 获取所有超量知识库的权限信息
// describeAllResPermission 获取所有资源权限
func (s *Service) describeAllResPermission(ctx context.Context, spaceID string) (map[uint64]bool, error) {
	return s.kbLogic.DescribeAllResPermission(ctx, spaceID)
}

// 获取知识库的权限信息
func (s *Service) describeResPermission(ctx context.Context, spaceID string, resourceType common.ResourceType) (map[uint64]bool, error) {
	resPermissionMap := make(map[uint64]bool)
	// 获取权限信息
	hasAllResourcePerm, otherAllPermissionIDs, shareKnowledgeBizIDList, mapShareKnowledgeBizIDs, err :=
		s.getUserResource(ctx, spaceID, resourceType)
	if err != nil {
		return resPermissionMap, err
	}
	permissions := config.DescribePermissionIDs()
	if hasAllResourcePerm {
		if len(otherAllPermissionIDs) != 0 {
			for _, permissionID := range otherAllPermissionIDs {
				if ok, _ := permissions[permissionID]; ok {
					resPermissionMap[kbLogic.KeyPermissionAll] = true
				}
			}
		}
	}
	for k, v := range mapShareKnowledgeBizIDs {
		for _, i := range v {
			if _, ok := permissions[i]; ok {
				resPermissionMap[k] = true
			}
		}
	}
	logx.D(ctx, "hasAllResourcePerm:%v, otherAllPermissionIDs:%v, "+
		"shareKnowledgeBizIDList:%v, mapShareKnowledgeBizIDs:%v", hasAllResourcePerm,
		otherAllPermissionIDs, shareKnowledgeBizIDList, mapShareKnowledgeBizIDs)

	logx.D(ctx, "resPermissionMap:%v", resPermissionMap)
	return resPermissionMap, nil
}

func (s *Service) describeReqKnowledgeIDs(ctx context.Context, knowledgeBaseID string,
	corpID uint64) (*pb.ResumeDocReq, *pb.ResumeQAReq, error) {
	docInfo, err := s.describeDocsByAppId(ctx, knowledgeBaseID, corpID)
	if err != nil {
		return nil, nil, fmt.Errorf("describeDocByAppId failed, err %w", err)
	}
	qaInfo, err := s.describeDocQasByAppId(ctx, knowledgeBaseID, corpID)
	if err != nil {
		return nil, nil, fmt.Errorf("describeDocQasByAppId failed, err %w", err)
	}
	return docInfo, qaInfo, nil
}

func (s *Service) processResumeDocReqs(ctx context.Context, requests []*pb.ResumeDocReq) error {
	// 创建带缓冲的通道，用于控制并发数量
	semaphore := make(chan struct{}, config.DescribeExceedKnowledgeResumeCon())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	// 处理单个请求的协程函数
	processOne := func(req *pb.ResumeDocReq) {
		defer wg.Done()
		// 获取信号量（如果缓冲区满，则阻塞等待）
		semaphore <- struct{}{}
		defer func() {
			// 释放信号量
			<-semaphore
		}()
		// 执行实际处理
		if _, err := s.ResumeDoc(ctx, req); err != nil {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}
	}
	// 启动协程处理每个请求
	for _, req := range requests {
		wg.Add(1)
		go processOne(req)
	}
	// 等待所有协程完成
	wg.Wait()
	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func (s *Service) processResumeDocQaReqs(ctx context.Context, requests []*pb.ResumeQAReq) error {
	// 创建带缓冲的通道，用于控制并发数量
	semaphore := make(chan struct{}, config.DescribeExceedKnowledgeResumeCon())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	// 处理单个请求的协程函数
	processOne := func(req *pb.ResumeQAReq) {
		defer wg.Done()
		// 获取信号量（如果缓冲区满，则阻塞等待）
		semaphore <- struct{}{}
		defer func() {
			// 释放信号量
			<-semaphore
		}()
		// 执行实际处理
		if _, err := s.ResumeQA(ctx, req); err != nil {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}
	}
	// 启动协程处理每个请求
	for _, req := range requests {
		wg.Add(1)
		go processOne(req)
	}
	// 等待所有协程完成
	wg.Wait()
	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func processDocsInfoEnhanced(docsInfo []*pb.ResumeDocReq, maxLength int) []*pb.ResumeDocReq {
	if maxLength == 0 {
		maxLength = 1000
	}
	var result []*pb.ResumeDocReq

	for _, doc := range docsInfo {
		if len(doc.DocBizIds) <= maxLength {
			result = append(result, doc)
			continue
		}
		// 拆分逻辑
		chunkCount := (len(doc.DocBizIds) + maxLength - 1) / maxLength // 计算需要拆分成几份
		for i := 0; i < chunkCount; i++ {
			start := i * maxLength
			end := start + maxLength
			if end > len(doc.DocBizIds) {
				end = len(doc.DocBizIds)
			}
			newDoc := &pb.ResumeDocReq{
				BotBizId:  doc.BotBizId,
				DocBizIds: make([]string, end-start),
			}
			copy(newDoc.DocBizIds, doc.DocBizIds[start:end])
			result = append(result, newDoc)
		}
	}
	return result
}

// 可配置最大长度的处理函数
func processQAsInfoWithMaxLength(qasInfo []*pb.ResumeQAReq, maxLength int) []*pb.ResumeQAReq {
	if maxLength == 0 {
		maxLength = 1000
	}
	var result []*pb.ResumeQAReq
	for _, qa := range qasInfo {
		if len(qa.QaBizIds) <= maxLength {
			result = append(result, qa)
			continue
		}

		chunkCount := (len(qa.QaBizIds) + maxLength - 1) / maxLength

		for i := 0; i < chunkCount; i++ {
			start := i * maxLength
			end := start + maxLength
			if end > len(qa.QaBizIds) {
				end = len(qa.QaBizIds)
			}

			newQA := &pb.ResumeQAReq{
				BotBizId: qa.BotBizId,
				QaBizIds: make([]string, end-start),
			}
			copy(newQA.QaBizIds, qa.QaBizIds[start:end])

			result = append(result, newQA)
		}
	}
	return result
}

// ModifyKBConfigList 设置知识配置
func (s *Service) ModifyKBConfigList(ctx context.Context, req *pb.ExternalModifyKBConfigListReq) (*pb.ExternalModifyKBConfigListRsp, error) {
	logx.I(ctx, "Admin ModifyKBConfigList, request: %+v", req)
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	modelList := make([]string, 0)
	uin, _ := kbEntity.GetLoginUinAndSubAccountUin(ctx)
	sid := contextx.Metadata(ctx).SID()
	for _, pbConfig := range req.GetKnowledgeBaseConfigs() {
		if (pbConfig.GetScenes() == entity.AppReleaseScenes || pbConfig.GetScenes() == 0) &&
			pbConfig.GetAppBizId() != "" && pbConfig.GetAppBizId() != "0" {
			// 外部的请求不允许修改应用下的发布配置
			logx.E(ctx, "release config is not allowed to change for CAPI request: %+v", pbConfig)
			return nil, errs.ErrParameterInvalid
		}
		if pbConfig.AppBizId == "" && pbConfig.KnowledgeBizId == "" {
			logx.E(ctx, "AppBizId and KnowledgeBizId are both empty for config:%+v.", pbConfig)
			return nil, errs.ErrParameterInvalid
		}
		// 收集所有需要校验的模型
		if pbConfig.GetEmbeddingModelConfig().GetModelName() != "" {
			modelList = append(modelList, pbConfig.GetEmbeddingModelConfig().GetModelName())
		}
		if pbConfig.GetQaExtractModelConfig().GetModelName() != "" {
			modelList = append(modelList, pbConfig.GetQaExtractModelConfig().GetModelName())
		}
		if pbConfig.GetKnowledgeSchemaModelConfig().GetModelName() != "" {
			modelList = append(modelList, pbConfig.GetKnowledgeSchemaModelConfig().GetModelName())
		}
		search := pbConfig.GetRetrievalConfig().GetSearchStrategy()
		if search == nil {
			continue
		}
		if search.GetEmbeddingModel() != "" {
			modelList = append(modelList, search.GetEmbeddingModel())
		}
		if search.GetRerankModel() != "" {
			modelList = append(modelList, search.GetRerankModel())
		}
		if search.GetNatureLanguageToSqlModelConfig().GetModel().GetModelName() != "" {
			modelList = append(modelList, search.GetNatureLanguageToSqlModelConfig().GetModel().GetModelName())
		}
	}
	// 校验套餐模型限制
	if err := s.checkQuotaModel(ctx, uin, sid, modelList); err != nil {
		return nil, err
	}
	innerKBConfigs := make([]*pb.KnowledgeBaseConfig, 0, len(req.GetKnowledgeBaseConfigs()))
	for _, pbConfig := range req.GetKnowledgeBaseConfigs() {
		if pbConfig.AppBizId == "" {
			logx.I(ctx, "AppBizId is empty, check share knowledge embedding config")
			knowledgeBizID := cast.ToUint64(pbConfig.KnowledgeBizId)
			modifiedEmbeddingModel := pbConfig.GetEmbeddingModelConfig().GetModelName()
			if err := s.updateKbEmbeddingModel(ctx, corpBizID, 0, knowledgeBizID, modifiedEmbeddingModel); err != nil {
				logx.E(ctx, "updateKbEmbeddingModel failed, knowledgeBizID: %d, corpBizID: %d, err: %v",
					knowledgeBizID, corpBizID, err)
				return nil, err
			}
		} else {
			logx.I(ctx, "AppBizId is not empty (%s), check default knowledge embedding config", pbConfig.AppBizId)
			appBizID := cast.ToUint64(pbConfig.AppBizId)
			modifiedEmbeddingModel := pbConfig.GetEmbeddingModelConfig().GetModelName()
			knowledgeBizId := appBizID
			if pbConfig.KnowledgeBizId != "" {
				knowledgeBizId = cast.ToUint64(pbConfig.KnowledgeBizId)
			}
			if err := s.updateKbEmbeddingModel(ctx, corpBizID, appBizID, knowledgeBizId, modifiedEmbeddingModel); err != nil {
				logx.E(ctx, "updateKbEmbeddingModel failed, appBizID: %d, corpBizID: %d, knowledgeBizId: %d, err: %v",
					appBizID, corpBizID, knowledgeBizId, err)
				return nil, err
			}

		}
		innerKBConfig := &pb.KnowledgeBaseConfig{
			KnowledgeBizId:             pbConfig.GetKnowledgeBizId(),
			ConfigTypes:                pbConfig.GetConfigTypes(),
			ThirdAclConfig:             pbConfig.GetThirdAclConfig(),
			AppBizId:                   pbConfig.GetAppBizId(),
			Scenes:                     pbConfig.GetScenes(),
			EmbeddingModelConfig:       pbConfig.GetEmbeddingModelConfig(),
			QaExtractModelConfig:       pbConfig.GetQaExtractModelConfig(),
			KnowledgeSchemaModelConfig: pbConfig.GetKnowledgeSchemaModelConfig(),
			RetrievalConfig: &pb.RetrievalConfig{
				RetrievalRange: pbConfig.GetRetrievalConfig().GetRetrievalRange(),
				SearchStrategy: pbConfig.GetRetrievalConfig().GetSearchStrategy(),
			},
			FileParseModelConfig: pbConfig.GetFileParseModelConfig(),
		}
		for _, retrievalInfo := range pbConfig.GetRetrievalConfig().GetRetrievals() {
			retrievalType := kbEntity.GetFiltersType(retrievalInfo.GetRetrievalType())
			if retrievalType == 0 {
				logx.E(ctx, "invalid retrieval type: %s", retrievalInfo.GetRetrievalType())
				return nil, errs.ErrParameterInvalid
			}
			innerKBConfig.RetrievalConfig.Retrievals = append(innerKBConfig.RetrievalConfig.Retrievals, &pb.RetrievalInfo{
				RetrievalType: common.KnowledgeType(retrievalType),
				IndexId:       retrievalInfo.GetIndexId(),
				Confidence:    retrievalInfo.GetConfidence(),
				TopN:          retrievalInfo.GetTopN(),
				IsEnable:      retrievalInfo.GetIsEnable(),
			})
		}
		innerKBConfigs = append(innerKBConfigs, innerKBConfig)
	}
	err := s.SetKnowledgeBaseConfigInfo(ctx, innerKBConfigs)
	if err != nil {
		return nil, err
	}
	return &pb.ExternalModifyKBConfigListRsp{}, nil
}

func (s *Service) checkQuotaModel(ctx context.Context, uin string, sid uint64, modelList []string) error {
	if len(modelList) == 0 {
		return nil
	}
	// 1.获取套餐第三方模型配额
	quota, err := s.rpc.DescribeAccountQuota(ctx, uin, sid)
	thirdModel := true
	if err == nil { // 获取失败,柔性放过
		thirdModel = quota.GetPackageDetail().GetAllowThirdPartyModel()
	}
	// 如果允许第三方模型，直接返回
	if thirdModel {
		return nil
	}
	// 2.获取模型信息,如果是使用第三方模型报错
	modelList = slicex.Unique(modelList)
	modelInfo, err := s.rpc.GetModelInfoByModelName(ctx, modelList)
	if err != nil { // 获取模型信息失败,柔性放过
		log.ErrorContextf(ctx, "checkQuotaModel GetProviderModelCacheOfAdp err:%v", err)
		return nil
	}
	for modelName, v := range modelInfo.GetModelInfo() {
		if v.ProviderType != entity.ProviderTypeSelf {
			log.WarnContextf(ctx, "checkQuotaModel not allow modelName:%v,value:%+v", modelName, v)
			return errs.ErrQuotaModel
		}
	}
	return nil
}

// DescribeKBConfigList 获取知识配置
func (s *Service) DescribeKBConfigList(ctx context.Context, req *pb.ExternalDescribeKBConfigListReq) (*pb.ExternalDescribeKBConfigListRsp, error) {
	logx.I(ctx, "DescribeKBConfigList, request: %+v", req)
	var knowledgeBizIds, appBizIds []string
	var appBizIdToCheck uint64
	if req.GetKnowledgeBizId() != "" {
		knowledgeBizIds = []string{req.GetKnowledgeBizId()}
		appBizIdToCheck = cast.ToUint64(req.GetKnowledgeBizId())
	}
	if req.GetAppBizId() != "" {
		appBizIds = []string{req.GetAppBizId()}
		appBizIdToCheck = cast.ToUint64(req.GetAppBizId())
	}

	appBaseInfo, err := s.rpc.AppAdmin.GetAppBaseInfo(ctx, appBizIdToCheck)
	if err != nil {
		logx.E(ctx, "GetKnowledgeBaseConfig error: %+v", err)
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo == nil {
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo.CorpPrimaryId != contextx.Metadata(ctx).CorpID() {
		logx.E(ctx, "auth bypass, appBaseInfo.CorpPrimaryId:%d, ctx.CorpID:%d", appBaseInfo.CorpPrimaryId, contextx.Metadata(ctx).CorpID())
		return nil, errs.ErrAppNotFound
	}

	newCtx := util.SetMultipleMetaData(ctx, appBaseInfo.SpaceId, appBaseInfo.Uin)
	configList, err := s.GetKnowledgeBaseConfigInfo(newCtx, knowledgeBizIds, appBizIds, req.GetConfigTypes(), req.GetScenes(), 0)
	if err != nil {
		return nil, err
	}
	rsp := &pb.ExternalDescribeKBConfigListRsp{}
	for _, config := range configList {
		externalConfig := &pb.ExternalKnowledgeBaseConfig{
			KnowledgeBizId:             config.GetKnowledgeBizId(),
			ConfigTypes:                config.GetConfigTypes(),
			ThirdAclConfig:             config.GetThirdAclConfig(),
			AppBizId:                   config.GetAppBizId(),
			Scenes:                     config.GetScenes(),
			EmbeddingModelConfig:       config.GetEmbeddingModelConfig(),
			QaExtractModelConfig:       config.GetQaExtractModelConfig(),
			KnowledgeSchemaModelConfig: config.GetKnowledgeSchemaModelConfig(),
			RetrievalConfig: &pb.ExternalRetrievalConfig{
				RetrievalRange:         config.GetRetrievalConfig().GetRetrievalRange(),
				SearchStrategy:         config.GetRetrievalConfig().GetSearchStrategy(),
				ShareKnowledgeBaseName: config.GetRetrievalConfig().GetShareKnowledgeBaseName(),
			},
			FileParseModelConfig: config.GetFileParseModelConfig(),
		}
		for _, retrievalInfo := range config.GetRetrievalConfig().GetRetrievals() {
			retrievalType := ""
			switch uint32(retrievalInfo.GetRetrievalType()) {
			case uint32(common.KnowledgeType_KnowledgeTypeQa):
				retrievalType = kbEntity.QaFilterTypeKey
			case uint32(common.KnowledgeType_KnowledgeTypeDoc):
				retrievalType = kbEntity.DocFilterTypeKey
			case uint32(common.KnowledgeType_KnowledgeTypeDB):
				retrievalType = kbEntity.DBFilterTypeKey
			}
			externalConfig.RetrievalConfig.Retrievals = append(externalConfig.RetrievalConfig.Retrievals, &pb.ExternalRetrievalInfo{
				RetrievalType: retrievalType,
				IndexId:       retrievalInfo.GetIndexId(),
				Confidence:    retrievalInfo.GetConfidence(),
				TopN:          retrievalInfo.GetTopN(),
				IsEnable:      retrievalInfo.GetIsEnable(),
			})
		}
		rsp.KnowledgeBaseConfigs = append(rsp.KnowledgeBaseConfigs, externalConfig)
	}
	logx.D(ctx, "DescribeKBConfigList, response: %+v", rsp)
	return rsp, nil
}
