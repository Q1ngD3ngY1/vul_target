package kb

import (
	"context"
	"fmt"
	"strconv"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	pc "git.woa.com/adp/pb-go/platform/platform_charger"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/spf13/cast"
)

const (
	KeyPermissionAll = uint64(1)
)

// GetKnowledgeBaseUsage 获取知识库使用量(区分新老用户)
func (l *Logic) GetKnowledgeBaseUsage(ctx context.Context, corpID uint64, knowledgeBaseID uint64) (*pb.GetCharacterUsageRsp, error) {
	rsp := &pb.GetCharacterUsageRsp{}
	// 获取企业信息
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil {
		logx.E(ctx, "GetKnowledgeBaseUsage DescribeCorpByPrimaryId failed, corpID:%d, err:%+v", corpID, err)
		return rsp, err
	}
	// 调用DescribeAccountQuota获取配额信息
	quotaRsp, err := l.rpc.Finance.DescribeAccountQuota(ctx, corp.GetUin(), corp.GetSid())
	if err != nil {
		logx.E(ctx, "GetKnowledgeBaseUsage DescribeAccountQuota failed, uin:%s, sid:%d, err:%+v", corp.GetUin(), corp.GetSid(), err)
		return rsp, err
	}
	if quotaRsp == nil {
		logx.E(ctx, "GetKnowledgeBaseUsage quotaRsp is nil")
		return rsp, errs.ErrSystem
	}
	// 根据IsPurchasePackage判断新老用户
	if quotaRsp.GetIsPackageScene() {
		// 新用户：基于容量判断
		return l.getNewUserCapacityUsage(ctx, corp, quotaRsp, knowledgeBaseID)
	} else {
		// 老用户：沿用字符数判断
		return l.getOldUserCharacterUsage(ctx, corp)
	}
}

// getOldUserCharacterUsage 获取老用户字符使用量
func (l *Logic) getOldUserCharacterUsage(ctx context.Context, corp *pm.DescribeCorpRsp) (*pb.GetCharacterUsageRsp, error) {
	rsp := &pb.GetCharacterUsageRsp{}
	var maxCharSize uint64
	var err error
	// 获取企业的最大字符数
	if config.IsFinanceDisabled() { // 私有化场景，就使用企业信息中的最大字符数
		logx.D(ctx, "getOldUserCharacterUsage private cloud, maxCharSize:%d", corp.GetMaxCharSize())
		maxCharSize = corp.GetMaxCharSize()
	} else {
		maxCharSize, err = l.rpc.Finance.GetCorpMaxCharSize(ctx, corp.GetSid(), corp.GetUin())
		if err != nil {
			logx.E(ctx, "getOldUserCharacterUsage GetCorpMaxCharSize failed, corpID:%d, err:%+v", corp.GetCorpId(), err)
			return rsp, errs.ErrCorpNotFound
		}
	}
	rsp.Total = uint32(maxCharSize)
	// 获取已使用字符数
	usedTotal, err := l.rpc.AppAdmin.CountCorpAppCharSize(ctx, corp.GetCorpPrimaryId())
	if err != nil {
		logx.E(ctx, "getOldUserCharacterUsage CountCorpAppCharSize failed, corpID:%d, err:%+v", corp.GetCorpPrimaryId(), err)
		return rsp, err
	}
	rsp.Used = uint32(usedTotal)
	// 对于vip客户先不判断超量失效的字符数
	if config.IsVipCorp(corp.Uin) {
		rsp.Exceed = 0
		return rsp, nil
	}
	// 获取超量字符数
	appDocExceedSizeMap, err := l.docLogic.GetRobotDocExceedUsage(ctx, corp.GetCorpPrimaryId(), nil)
	if err != nil {
		return rsp, err
	}
	appQAExceedCharSizeMap, err := l.qaLogic.GetRobotQAExceedUsage(ctx, corp.GetCorpPrimaryId(), nil)
	if err != nil {
		return rsp, err
	}
	docExceedCharSize, docQaExceededCharSize := uint64(0), uint64(0)
	for _, usage := range appDocExceedSizeMap {
		docExceedCharSize += uint64(usage.CharSize)
	}
	for _, usage := range appQAExceedCharSizeMap {
		docQaExceededCharSize += uint64(usage.CharSize)
	}

	rsp.Exceed = uint32(docExceedCharSize + docQaExceededCharSize)
	logx.I(ctx, "getOldUserCharacterUsage: corpID:%d, used:%d, total:%d, exceed:%d",
		corp.GetCorpId(), rsp.Used, rsp.Total, rsp.Exceed)

	return rsp, nil
}

// getNewUserCapacityUsage 获取新用户字符使用量-指定知识库
func (l *Logic) getNewUserCapacityUsage(ctx context.Context, corp *pm.DescribeCorpRsp, quotaRsp *pc.DescribeAccountQuotaRsp, knowledgeBaseID uint64) (*pb.GetCharacterUsageRsp, error) {
	rsp := &pb.GetCharacterUsageRsp{}
	var appPrimaryIDs []uint64
	maxCapacity := uint64(quotaRsp.GetPackageDetail().GetKnowledgeCapacity()) * entity.ByteToGB // GB转字节

	// 获取容量使用情况
	corpUsage, err := l.rpc.AppApi.DescribeCorpKnowledgeCapacity(ctx, corp.GetCorpId(), []uint64{})
	if err != nil {
		logx.E(ctx, "getNewUserCapacityUsage DescribeCorpKnowledgeCapacity failed, corpID:%d, err:%+v", corp.GetCorpId(), err)
		return rsp, err
	}
	usedCapacity := uint64(corpUsage.KnowledgeCapacity)
	if knowledgeBaseID != 0 { // 指定知识库
		appUsage, err := l.rpc.AppApi.DescribeCorpKnowledgeCapacity(ctx, corp.GetCorpId(), []uint64{knowledgeBaseID})
		if err != nil {
			logx.E(ctx, "getNewUserCapacityUsage DescribeCorpKnowledgeCapacity failed, corpID:%d, knowledgeBaseID:%d, err:%+v", corp.GetCorpId(), knowledgeBaseID, err)
			return rsp, err
		}
		usedCapacity = uint64(appUsage.KnowledgeCapacity)
		appPrimaryID, err := l.cacheLogic.GetAppPrimaryIdByBizId(ctx, knowledgeBaseID)
		if err != nil {
			logx.E(ctx, "getNewUserCapacityUsage GetAppPrimaryIdByBizId failed, corpID:%d, knowledgeBaseID:%d, err:%+v", corp.GetCorpId(), knowledgeBaseID, err)
			return rsp, err
		}
		appPrimaryIDs = []uint64{appPrimaryID}
	}
	rsp.TotalCapacity = maxCapacity
	rsp.UsedCapacity = usedCapacity

	// 对于vip客户先不判断超量失效的字符数
	if config.IsVipCorp(corp.Uin) {
		rsp.ExceededInvalidCapacity = 0
		return rsp, nil
	}
	// 获取超量失效容量
	appDocExceedCapacityMap, err := l.docLogic.GetRobotDocExceedUsage(ctx, corp.GetCorpPrimaryId(), appPrimaryIDs)
	if err != nil {
		return rsp, err
	}
	appQAExceedCapacityMap, err := l.qaLogic.GetRobotQAExceedUsage(ctx, corp.GetCorpPrimaryId(), appPrimaryIDs)
	if err != nil {
		return rsp, err
	}
	docExceed, docQaExceed := uint64(0), uint64(0)
	for _, usage := range appDocExceedCapacityMap {
		docExceed += uint64(usage.KnowledgeCapacity)
	}
	for _, usage := range appQAExceedCapacityMap {
		docQaExceed += uint64(usage.KnowledgeCapacity)
	}
	rsp.ExceededInvalidCapacity = docExceed + docQaExceed

	// 获取知识库容量状态
	capacityStatus := l.financeLogic.GetCapacityStatus(ctx, corp, corpUsage, int64(maxCapacity))
	rsp.CapacityStatus = uint32(capacityStatus)

	logx.I(ctx, "getNewUserCapacityUsage: corpID:%d, used:%d, total:%d (converted from capacity), exceeded:%d, status:%d",
		corp.GetCorpId(), rsp.UsedCapacity, rsp.TotalCapacity, rsp.ExceededInvalidCapacity, rsp.CapacityStatus)

	return rsp, nil
}

// DescribeExceededKnowledgeList 获取超量知识库列表
// permissionCheck: true-检查权限, false-不检查权限
func (l *Logic) DescribeExceededKnowledgeList(ctx context.Context, corpID uint64, spaceID string, pageNumber, pageSize uint32, permissionCheck bool) (*pb.DescribeExceededKnowledgeListRsp, error) {
	// 拉取所有的应用信息
	appListReq := appconfig.ListAppBaseInfoReq{SpaceId: spaceID, CorpPrimaryId: corpID}
	apps, _, err := l.rpc.AppAdmin.ListAllAppBaseInfo(ctx, &appListReq)
	if err != nil {
		logx.E(ctx, "ListAllAppBaseInfo err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}

	var appPrimaryIds []uint64
	for _, app := range apps {
		appPrimaryIds = append(appPrimaryIds, app.PrimaryId)
	}

	// 从t_doc表拿到corpID对应的所有的超量文件对应的robotID及对应的大小
	appDocExceedSizeMap, err := l.docLogic.GetRobotDocExceedUsage(ctx, corpID, appPrimaryIds)
	if err != nil {
		logx.E(ctx, "GetRobotDocExceedUsage failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	logx.I(ctx, "appDocExceedSizeMap: %+v", appDocExceedSizeMap)

	appQAExceedCharSizeMap, err := l.qaLogic.GetRobotQAExceedUsage(ctx, corpID, appPrimaryIds)
	if err != nil {
		logx.E(ctx, "GetRobotQAExceedUsage failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	logx.I(ctx, "appQAExceedCharSizeMap: %+v", appQAExceedCharSizeMap)

	isResuming, err := l.isResuming(ctx, appPrimaryIds, corpID)
	if err != nil {
		logx.E(ctx, "isResuming failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	logx.I(ctx, "isResuming: %+v", isResuming)

	// 根据参数决定是否检查权限
	var resPermissionMap map[uint64]bool
	if permissionCheck {
		// 需要检查权限
		resPermissionMap, err = l.DescribeAllResPermission(ctx, spaceID)
		if err != nil {
			logx.E(ctx, "DescribeAllResPermission failed, err: %+v", err)
			return nil, errs.ErrDescribeExceededKnowledgeListFailed
		}
		logx.I(ctx, "resPermissionMap: %+v", resPermissionMap)
	}

	var fApps []*pb.ExceededKnowledgeDetail
	for _, app := range apps {
		// 权限检查过滤
		if permissionCheck && !l.HasPermission(app.BizId, resPermissionMap) {
			logx.I(ctx, "app.BusinessID has no edit permission: %d", app.BizId)
			continue
		}
		exceededSize := uint64(appDocExceedSizeMap[app.PrimaryId].CharSize) + uint64(appQAExceedCharSizeMap[app.PrimaryId].CharSize)
		exceededCapacity := uint64(appDocExceedSizeMap[app.PrimaryId].KnowledgeCapacity) + uint64(appQAExceedCharSizeMap[app.PrimaryId].KnowledgeCapacity)
		if exceededSize == 0 {
			logx.I(ctx, "app.BusinessID has no exceeded knowledge: %d", app.BizId)
			continue
		}
		kd := pb.ExceededKnowledgeDetail{
			AppName:                 app.Name,
			ExceedCharSize:          exceededSize,
			ExceededInvalidCapacity: exceededCapacity,
			Id:                      strconv.FormatUint(app.BizId, 10),
		}
		if appDocExceedSizeMap[app.PrimaryId].CharSize != 0 {
			kd.KnowledgeSubType = pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_DOC
		} else {
			kd.KnowledgeSubType = pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_QA
		}
		if app.IsShared {
			kd.KnowledgeType = pb.KnowledgeType_SharedKnowledge
		} else {
			kd.KnowledgeType = pb.KnowledgeType_AppDefaultKnowledge
		}
		if isResuming[app.PrimaryId] {
			kd.State = pb.ExceededKnowledgeTypeState_EXCEEDED_KNOWLEDGE_TYPE_STATE_RESUMING
		} else {
			kd.State = pb.ExceededKnowledgeTypeState_EXCEEDED_KNOWLEDGE_TYPE_STATE_EXCEED
		}
		fApps = append(fApps, &kd)
	}

	rsp := &pb.DescribeExceededKnowledgeListRsp{}
	rsp.Total = uint32(len(fApps))
	if rsp.Total != 0 {
		pApps, err := l.getPaginatedData(int(pageNumber), int(pageSize), fApps)
		if err != nil {
			logx.E(ctx, "getPaginatedData failed, err: %+v", err)
			return rsp, errs.ErrDescribeExceededKnowledgeListFailed
		}
		rsp.List = append(rsp.List, pApps...)
	}
	return rsp, nil
}

// isResuming 检查应用是否正在恢复中
func (l *Logic) isResuming(ctx context.Context, appIDs []uint64, corpID uint64) (map[uint64]bool, error) {
	docs, err := l.docLogic.GetDao().GetAllDocs(ctx, []string{docEntity.DocTblColRobotId},
		&docEntity.DocFilter{
			RobotIDs: appIDs,
			CorpId:   corpID,
			Limit:    1,
			Status:   docEntity.DocExceedResumingStatus,
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocs failed, err: %w", err)
	}
	isResumingMap := make(map[uint64]bool)
	for _, doc := range docs {
		if !isResumingMap[doc.RobotID] {
			isResumingMap[doc.RobotID] = true
		}
	}
	qas, err := l.qaLogic.GetAllDocQas(ctx, []string{qaEntity.DocQaTblColRobotId}, &qaEntity.DocQaFilter{
		RobotIDs:          appIDs,
		CorpId:            corpID,
		Limit:             1,
		ReleaseStatusList: qaEntity.QaExceedResumingStatus,
	})
	if err != nil {
		return nil, fmt.Errorf("GetAllQas failed, err: %w", err)
	}
	for _, qa := range qas {
		if !isResumingMap[qa.RobotID] {
			isResumingMap[qa.RobotID] = true
		}
	}
	return isResumingMap, nil
}

// HasPermission 检查是否有权限
func (l *Logic) HasPermission(bizID uint64, resPermissionMap map[uint64]bool) bool {
	if _, ok := resPermissionMap[bizID]; ok {
		return true
	}
	if _, ok := resPermissionMap[KeyPermissionAll]; ok {
		return true
	}
	return false
}

// DescribeAllResPermission 获取所有资源权限
func (l *Logic) DescribeAllResPermission(ctx context.Context, spaceID string) (map[uint64]bool, error) {
	resPermissionMap := make(map[uint64]bool)
	app, err := l.describeResPermission(ctx, spaceID, common.ResourceType_ResourceTypeApp)
	if err != nil {
		return resPermissionMap, err
	}
	for k, v := range app {
		resPermissionMap[k] = v
	}
	knowledge, err := l.describeResPermission(ctx, spaceID, common.ResourceType_ResourceTypeKnowledge)
	if err != nil {
		return resPermissionMap, err
	}
	for k, v := range knowledge {
		resPermissionMap[k] = v
	}
	return resPermissionMap, nil
}

// describeResPermission 获取资源权限
func (l *Logic) describeResPermission(ctx context.Context, spaceID string, resourceType common.ResourceType) (map[uint64]bool, error) {
	resPermissionMap := make(map[uint64]bool)
	// 获取权限信息
	permissionResource, err := l.rpc.PlatformAdmin.DescribeResourceList(ctx, spaceID, resourceType)
	if err != nil {
		logx.E(ctx, "describeResPermission DescribeResourceList fail, err=%+v", err)
		return resPermissionMap, err
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

	permissions := config.DescribePermissionIDs()
	if hasOtherAllPerm {
		if len(otherAllPermissionIDs) != 0 {
			for _, permissionID := range otherAllPermissionIDs {
				if ok, _ := permissions[permissionID]; ok {
					resPermissionMap[KeyPermissionAll] = true
				}
			}
		}
	}
	for k, v := range bizPermissionIDMap {
		for _, i := range v {
			if _, ok := permissions[i]; ok {
				resPermissionMap[k] = true
			}
		}
	}
	logx.D(ctx, "hasOtherAllPerm:%v, otherAllPermissionIDs:%v, "+
		"shareKnowledgeBizIDList:%v, bizPermissionIDMap:%v", hasOtherAllPerm,
		otherAllPermissionIDs, shareKnowledgeBizIDList, bizPermissionIDMap)

	logx.D(ctx, "resPermissionMap:%v", resPermissionMap)
	return resPermissionMap, nil
}

// getPaginatedData 获取分页数据
func (l *Logic) getPaginatedData(pageNumber, pageSize int, app []*pb.ExceededKnowledgeDetail) ([]*pb.ExceededKnowledgeDetail, error) {
	startIndex := (pageNumber - 1) * pageSize
	if startIndex < 0 || startIndex >= len(app) {
		return nil, fmt.Errorf("startIndex: %d, len(app): %d", startIndex, len(app))
	}
	endIndex := startIndex + pageSize
	if endIndex > len(app) {
		endIndex = len(app)
	}
	if endIndex < startIndex {
		return nil, fmt.Errorf("endIndex < startIndex, endIndex: %d, startIndex: %d", endIndex, startIndex)
	}
	paginatedData := app[startIndex:endIndex]
	return paginatedData, nil
}
