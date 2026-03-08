package service

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/data_statistics"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/share_knowledge"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"github.com/spf13/cast"
)

// CreateSharedKnowledge 创建共享知识库
func (s *Service) CreateSharedKnowledge(ctx context.Context, req *pb.CreateSharedKnowledgeReq) (
	*pb.CreateSharedKnowledgeRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.CreateSharedKnowledgeRsp)

	log.InfoContextf(ctx, "CreateSharedKnowledge, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "CreateSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if err = s.validateSharedKnowledgeCreateRequest(ctx, req); err != nil {
		return rsp, err
	}

	corpBizID, staffBizID := pkg.CorpBizID(ctx), pkg.StaffBizID(ctx)
	if corpBizID == 0 || staffBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, err
	}
	if req.GetSpaceId() == "" {
		req.SpaceId = model.DefaultSpaceID
	}

	knowledgeList, err := s.dao.RetrieveSharedKnowledgeByName(ctx, corpBizID, []string{
		req.GetKnowledgeName(),
	}, req.GetSpaceId())
	if err != nil {
		return rsp, errs.ErrSharedKnowledgeNameQueryFailed
	}

	log.InfoContextf(ctx, "CreateSharedKnowledge, knowledgeList: %+v", knowledgeList)
	if len(knowledgeList) > 0 {
		return rsp, errs.ErrSharedKnowledgeExist
	}

	uin, subAccountUin := model.GetLoginUinAndSubAccountUin(ctx)
	staffNameResponse, err := client.GetCorpStaffName(ctx, uin, subAccountUin)
	if err != nil {
		return rsp, errs.ErrGetUserNameFailed
	}

	spaceID := utils.When(req.GetSpaceId() != "", req.GetSpaceId(), model.DefaultSpaceID)

	createResponse, err := client.CreateShareKnowledgeBaseApp(ctx, uin,
		share_knowledge.GenerateSharedKnowledgeAppName(req),
		share_knowledge.SharedKnowledgeAppAvatar,
		spaceID)
	if err != nil {
		return rsp, errs.ErrCreateSharedKnowledgeAppFailed
	}

	user := &pb.UserBaseInfo{
		UserBizId: staffBizID,
		UserName:  staffNameResponse.GetStaffName(),
	}

	// 添加知识库记录
	insertID, err := s.dao.CreateSharedKnowledge(ctx, corpBizID, createResponse.GetAppBizId(),
		user, req)
	if err != nil {
		if _, deleteError := client.DeleteShareKnowledgeBaseApp(ctx, uin,
			createResponse.GetAppBizId()); deleteError != nil {
			err = deleteError
			return rsp, errs.ErrDeleteSharedKnowledgeAppFailed
		}

		return rsp, errs.ErrCreateSharedKnowledgeRecordFailed
	}
	log.InfoContextf(ctx, "CreateSharedKnowledge, insertID: %d", insertID)

	rsp.KnowledgeBizId = createResponse.GetAppBizId()

	// 上报统计数据
	go func(newCtx context.Context) { //异步上报
		counterInfo := &data_statistics.CounterInfo{
			CorpBizId:       corpBizID,
			SpaceId:         spaceID,
			StatisticObject: statistics.StatObject_STAT_OBJECT_KB,
			StatisticType:   statistics.StatType_STAT_TYPE_CREATE,
			ObjectId:        strconv.FormatUint(rsp.KnowledgeBizId, 10),
			ObjectName:      req.KnowledgeName,
			Count:           1,
		}
		data_statistics.Counter(newCtx, counterInfo)
	}(trpc.CloneContext(ctx))

	return rsp, nil
}

func (s *Service) validateSharedKnowledgeCreateRequest(ctx context.Context,
	req *pb.CreateSharedKnowledgeReq) error {
	if !share_knowledge.VerifyData([]*share_knowledge.DataValidation{
		{
			Data: utf8.RuneCountInString(req.GetKnowledgeName()),
			Validator: share_knowledge.NewRangeValidator(
				share_knowledge.WithMin(1),
				share_knowledge.WithMax(float64(i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
					model.ShareKnowledgeNameLength))),
			),
		},
		{
			Data: utf8.RuneCountInString(req.GetKnowledgeDescription()),
			Validator: share_knowledge.NewRangeValidator(
				share_knowledge.WithMax(float64(i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
					model.ShareKnowledgeDescriptionLength))),
			),
		},
	}) {
		return errs.ErrParameterInvalid
	}

	return nil
}

// DeleteSharedKnowledge 删除共享知识库
func (s *Service) DeleteSharedKnowledge(ctx context.Context, req *pb.DeleteSharedKnowledgeReq) (
	*pb.DeleteSharedKnowledgeRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.DeleteSharedKnowledgeRsp)

	log.InfoContextf(ctx, "DeleteSharedKnowledge, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "DeleteSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if !share_knowledge.VerifyData([]*share_knowledge.DataValidation{
		{
			Data:      req.GetKnowledgeBizId(),
			Validator: share_knowledge.NewRangeValidator(share_knowledge.WithMin(1))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, err
	}

	corpBizID := pkg.CorpBizID(ctx)
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, err
	}

	if _, err = s.dao.RetrieveBaseSharedKnowledge(ctx, corpBizID, []uint64{
		req.GetKnowledgeBizId(),
	}); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rsp, errs.ErrSharedKnowledgeRecordNotFound
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}

	// 检查关联的应用
	shareKGAppList, err := dao.GetAppShareKGDao().GetShareKGAppBizIDList(ctx, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		log.ErrorContextf(ctx, "DeleteSharedKnowledge failed, error: %+v", err)
		return rsp, errs.ErrGetShareKnowledgeAppListFailed
	}

	if len(shareKGAppList) > 0 {
		validAppList, err := share_knowledge.ConvertSharedKnowledgeAppInfo(ctx, shareKGAppList)
		if err != nil {
			return rsp, errs.ErrSharedKnowledgeConvertFailed
		}

		if len(validAppList) > 0 {
			err = errs.ErrRelatedAppExist
			log.ErrorContextf(ctx, "DeleteSharedKnowledge failed, validAppList: %+v, error: %+v",
				validAppList, err)
			return rsp, err
		}
	}

	// 删除共享应用
	uin, _ := model.GetLoginUinAndSubAccountUin(ctx)
	if _, err = client.DeleteShareKnowledgeBaseApp(ctx, uin, req.GetKnowledgeBizId()); err != nil {
		return rsp, errs.ErrDeleteSharedKnowledgeAppFailed
	}

	// NOTICE: 删除模型配置
	err = share_knowledge.DeleteModelConfig(ctx, corpBizID, req.GetKnowledgeBizId())
	if err != nil {
		return rsp, errs.ErrDeleteKnowledgeModelConfigFailed
	}

	// 删除共享库记录
	_, err = s.dao.DeleteSharedKnowledge(ctx, corpBizID, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		return rsp, errs.ErrDeleteSharedKnowledgeRecordFailed
	}

	rsp.KnowledgeBizId = req.GetKnowledgeBizId()
	return rsp, nil
}

// ListSharedKnowledge 列举共享知识库
func (s *Service) ListSharedKnowledge(ctx context.Context, req *pb.ListSharedKnowledgeReq) (
	*pb.ListSharedKnowledgeRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.ListSharedKnowledgeRsp)

	log.InfoContextf(ctx, "ListSharedKnowledge, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "ListSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if !share_knowledge.VerifyData([]*share_knowledge.DataValidation{
		{Data: req.GetPageNumber(), Validator: share_knowledge.NewRangeValidator(share_knowledge.WithMin(1))},
		{Data: req.GetPageSize(), Validator: share_knowledge.NewRangeValidator(
			share_knowledge.WithMin(1), share_knowledge.WithMax(share_knowledge.SharedKnowledgeMaxPageSize))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, err
	}

	corpBizID := pkg.CorpBizID(ctx)
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}
	spaceID := utils.When(req.GetSpaceId() != "", req.GetSpaceId(), model.DefaultSpaceID)

	hasAllResourcePerm, otherAllPermissionIDs, shareKnowledgeBizIDList, mapShareKnowledgeBizIDs,
		err := s.getUserResource(ctx, spaceID, bot_common.ResourceType_ResourceTypeKnowledge)
	if err != nil {
		return nil, err
	}
	if hasAllResourcePerm {
		shareKnowledgeBizIDList = []uint64{}
	} else if len(shareKnowledgeBizIDList) == 0 {
		return rsp, nil
	}

	knowledgeCount, err := s.dao.RetrieveSharedKnowledgeCount(ctx, corpBizID, shareKnowledgeBizIDList,
		req.GetKeyword(), spaceID)
	if err != nil {
		return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
	}

	log.InfoContextf(ctx, "ListSharedKnowledge, knowledgeCount: %d", knowledgeCount)
	rsp.Total = uint32(knowledgeCount)
	if knowledgeCount == 0 {
		return rsp, nil
	}

	baseKnowledgeList, err := s.dao.ListBaseSharedKnowledge(ctx, corpBizID, shareKnowledgeBizIDList,
		req.GetPageNumber(), req.GetPageSize(), req.GetKeyword(), spaceID)
	if err != nil {
		return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
	}
	log.InfoContextf(ctx, "ListSharedKnowledge.ListBaseSharedKnowledge, baseKnowledgeList(%d): %+v",
		len(baseKnowledgeList), baseKnowledgeList)
	if len(baseKnowledgeList) == 0 {
		return rsp, nil
	}
	ownerStaffIDMap := make(map[uint64]*model.CorpStaff)
	ownerStaffIDs := make([]uint64, 0)
	for _, item := range baseKnowledgeList {
		ownerStaffIDMap[item.OwnerStaffID] = &model.CorpStaff{}
		ownerStaffIDs = append(ownerStaffIDs, item.OwnerStaffID)
	}
	ownerStaffIDs = slicex.Unique(ownerStaffIDs)
	staffs, err := s.dao.GetStaffByIDs(ctx, ownerStaffIDs)
	if err != nil {
		log.ErrorContextf(ctx, "ListSharedKnowledge failed, ownerStaffIDs: %+v, error: %+v",
			ownerStaffIDs, err)
		return nil, errs.ErrStaffNotFound
	}
	for _, staff := range staffs {
		ownerStaffIDMap[staff.ID] = staff
	}
	for _, item := range baseKnowledgeList {
		staff, ok := ownerStaffIDMap[item.OwnerStaffID]
		if ok {
			item.OwnerStaffName = staff.NickName
		}
	}

	// NOTICE: 检索模型配置
	baseKnowledgeList, err = share_knowledge.RetrieveModelConfig(ctx, corpBizID, baseKnowledgeList)
	if err != nil {
		return rsp, errs.ErrQueryKnowledgeModelConfigFailed
	}
	log.InfoContextf(ctx, "ListSharedKnowledge.RetrieveModelConfig, baseKnowledgeList(%d): %+v",
		len(baseKnowledgeList), baseKnowledgeList)

	knowledgeBizIDList := slicex.Map(baseKnowledgeList, func(item *model.SharedKnowledgeInfo) uint64 {
		return item.BusinessID
	})
	shareKGAppList, err := dao.GetAppShareKGDao().GetShareKGAppBizIDList(ctx, knowledgeBizIDList)
	if err != nil {
		log.ErrorContextf(ctx, "ListSharedKnowledge failed, knowledgeBizIDList: %+v, error: %+v",
			knowledgeBizIDList, err)
		return nil, errs.ErrGetShareKnowledgeAppListFailed
	}
	log.InfoContextf(ctx, "ListSharedKnowledge, shareKGAppList(%d): %+v",
		len(shareKGAppList), shareKGAppList)

	// 获取应用基础信息
	var knowledgeAppMap map[uint64][]*pb.AppBaseInfo
	if len(shareKGAppList) > 0 {
		knowledgeAppMap, err = share_knowledge.ConvertAppBySharedKnowledge(ctx, shareKGAppList)
		if err != nil {
			return nil, errs.ErrSharedKnowledgeConvertFailed
		}
		log.InfoContextf(ctx, "ListSharedKnowledge, knowledgeAppMap(%d): %+v",
			len(knowledgeAppMap), knowledgeAppMap)
	}

	rsp.KnowledgeList = share_knowledge.GenerateSharedKnowledgeDetailList(ctx,
		baseKnowledgeList, knowledgeAppMap, otherAllPermissionIDs, mapShareKnowledgeBizIDs)
	return rsp, nil
}

// DescribeSharedKnowledge 查询共享知识库
func (s *Service) DescribeSharedKnowledge(ctx context.Context, req *pb.DescribeSharedKnowledgeReq) (
	*pb.DescribeSharedKnowledgeRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.DescribeSharedKnowledgeRsp)

	log.InfoContextf(ctx, "DescribeSharedKnowledge, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "DescribeSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if !share_knowledge.VerifyData([]*share_knowledge.DataValidation{
		{
			Data:      req.GetKnowledgeBizId(),
			Validator: share_knowledge.NewRangeValidator(share_knowledge.WithMin(1))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, errs.ErrParameterInvalid
	}

	corpBizID := pkg.CorpBizID(ctx)
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}

	knowledgeList, err := s.dao.RetrieveBaseSharedKnowledge(ctx, corpBizID, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rsp, errs.ErrSharedKnowledgeRecordNotFound
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}
	log.InfoContextf(ctx, "DescribeSharedKnowledge.RetrieveBaseSharedKnowledge, knowledgeList: %+v",
		knowledgeList)

	// NOTICE: 检索模型配置
	knowledgeList, err = share_knowledge.RetrieveModelConfig(ctx, corpBizID, knowledgeList)
	if err != nil {
		return rsp, errs.ErrQueryKnowledgeModelConfigFailed
	}
	log.InfoContextf(ctx, "DescribeSharedKnowledge.SupplyModelConfig, knowledgeList: %+v",
		knowledgeList)

	// 查询关联APP
	shareKGAppList, err := dao.GetAppShareKGDao().GetShareKGAppBizIDList(ctx, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		log.ErrorContextf(ctx, "DescribeSharedKnowledge failed, knowledgeBizId: %d, error: %+v",
			req.GetKnowledgeBizId(), err)
		return rsp, errs.ErrGetShareKnowledgeAppListFailed
	}

	var appList []*pb.AppBaseInfo
	if len(shareKGAppList) > 0 {
		appList, err = share_knowledge.ConvertSharedKnowledgeAppInfo(ctx, shareKGAppList)
		if err != nil {
			return rsp, errs.ErrSharedKnowledgeConvertFailed
		}
	}

	knowledgeInfo, userInfo := share_knowledge.ConvertSharedKnowledgeBaseInfo(ctx, knowledgeList[0])
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

	log.InfoContextf(ctx, "UpdateSharedKnowledge, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "UpdateSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if err = s.validateSharedKnowledgeUpdateRequest(ctx, req); err != nil {
		return rsp, err
	}

	corpBizID := pkg.CorpBizID(ctx)
	if corpBizID == 0 {
		return rsp, errs.ErrContextInvalid
	}

	currentKnowledgeList, err := s.dao.RetrieveBaseSharedKnowledge(ctx, corpBizID, []uint64{
		req.GetKnowledgeBizId(),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rsp, errs.ErrSharedKnowledgeRecordNotFound
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}
	log.InfoContextf(ctx, "UpdateSharedKnowledge, currentKnowledgeList: %+v", currentKnowledgeList)

	if currentKnowledgeList[0].Name != req.GetInfo().GetKnowledgeName() {
		hitKnowledgeList, err := s.dao.RetrieveSharedKnowledgeByName(ctx, corpBizID, []string{
			req.GetInfo().GetKnowledgeName(),
		}, currentKnowledgeList[0].SpaceID)
		if err != nil {
			return rsp, errs.ErrSharedKnowledgeNameQueryFailed
		}

		log.InfoContextf(ctx, "UpdateSharedKnowledge, hitKnowledgeList: %+v", hitKnowledgeList)
		if len(hitKnowledgeList) > 0 {
			return rsp, errs.ErrSharedKnowledgeExist
		}
	}
	spaceId := currentKnowledgeList[0].SpaceID

	staffBizID := pkg.StaffBizID(ctx)
	if staffBizID == 0 {
		return rsp, errs.ErrContextInvalid
	}

	uin, subAccountUin := model.GetLoginUinAndSubAccountUin(ctx)
	staffNameResponse, err := client.GetCorpStaffName(ctx, uin, subAccountUin)
	if err != nil {
		return rsp, errs.ErrGetUserNameFailed
	}

	user := &pb.UserBaseInfo{
		UserBizId: staffBizID,
		UserName:  staffNameResponse.GetStaffName(),
	}
	// 更新知识库记录
	affectedRow, err := s.dao.UpdateSharedKnowledge(ctx, corpBizID, req.GetKnowledgeBizId(),
		user, req.GetInfo())
	if err != nil {
		return rsp, err
	}
	log.InfoContextf(ctx, "UpdateSharedKnowledge, affectedRow: %d", affectedRow)

	rsp.KnowledgeBizId = req.GetKnowledgeBizId()
	// 上报统计数据
	go func(newCtx context.Context) { //异步上报
		counterInfo := &data_statistics.CounterInfo{
			CorpBizId:       corpBizID,
			SpaceId:         spaceId,
			StatisticObject: statistics.StatObject_STAT_OBJECT_KB,
			StatisticType:   statistics.StatType_STAT_TYPE_EDIT,
			ObjectId:        strconv.FormatUint(rsp.KnowledgeBizId, 10),
			ObjectName:      req.GetInfo().GetKnowledgeName(),
			Count:           1,
		}
		data_statistics.Counter(newCtx, counterInfo)
	}(trpc.CloneContext(ctx))
	return rsp, nil
}

func (s *Service) getUserResource(ctx context.Context, spaceID string, resourceType bot_common.ResourceType) (
	bool, []string, []uint64, map[uint64][]string, error) {
	permissionResource, err := s.dao.GetAdminApiCli().GetPermissionResourceList(ctx, &admin.GetPermissionResourceListReq{
		SpaceId:      spaceID,
		ResourceType: resourceType,
	})
	if err != nil {
		log.ErrorContextf(ctx, "getUserResource adminApi.GetPermissionResourceList fail, err=%+v", err)
		return false, nil, nil, nil, err
	}
	hasOtherAllPerm := permissionResource.GetHasOtherAll()
	otherAllPermissionIDs := permissionResource.GetOtherAllPermissionIds()
	bizPermissionIDMap := make(map[uint64][]string)
	shareKnowledgeBizIDList := make([]uint64, 0)
	// 处理资源权限位
	for _, v := range permissionResource.GetResourcePermissions() {
		shareKnowledgeBizID := cast.ToUint64(v.GetResourceId())
		if !hasOtherAllPerm {
			shareKnowledgeBizIDList = append(shareKnowledgeBizIDList, shareKnowledgeBizID)
		}
		bizPermissionIDMap[shareKnowledgeBizID] = v.GetPermissionIds()
	}
	return hasOtherAllPerm, otherAllPermissionIDs, shareKnowledgeBizIDList, bizPermissionIDMap, nil

}

func (s *Service) validateSharedKnowledgeUpdateRequest(ctx context.Context,
	req *pb.UpdateSharedKnowledgeReq) error {
	if !share_knowledge.VerifyData([]*share_knowledge.DataValidation{
		{
			Data: req.GetKnowledgeBizId(),
			Validator: share_knowledge.NewRangeValidator(
				share_knowledge.WithMin(1),
			),
		},
		{
			Data: utf8.RuneCountInString(req.GetInfo().GetKnowledgeName()),
			Validator: share_knowledge.NewRangeValidator(
				share_knowledge.WithMin(1), share_knowledge.WithMax(float64(i18n.CalculateExpandedLength(ctx,
					i18n.UserInputCharType, model.ShareKnowledgeNameLength))),
			),
		},
		{
			Data: utf8.RuneCountInString(req.GetInfo().GetKnowledgeDescription()),
			Validator: share_knowledge.NewRangeValidator(
				share_knowledge.WithMax(float64(i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
					model.ShareKnowledgeDescriptionLength)),
				)),
		},
	}) {
		return errs.ErrParameterInvalid
	}

	return nil
}
