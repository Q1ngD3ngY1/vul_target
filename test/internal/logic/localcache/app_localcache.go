package localcache

import (
	"context"
	"fmt"
	"strconv"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/app/app_config"
	"github.com/spf13/cast"
)

// GetAppBizIdByPrimaryId 根据appID获取appBizID
func (l *Logic) GetAppBizIdByPrimaryId(ctx context.Context, corpPrimaryID uint64, appID uint64) (uint64, error) {
	appID2AppBizIDMap, err := l.GetAppBizIdsByPrimaryIds(ctx, corpPrimaryID, []uint64{appID})
	if err != nil {
		return 0, err
	}
	if appBizID, ok := appID2AppBizIDMap[appID]; ok {
		return appBizID, nil
	}
	return 0, errs.ErrAppNotFound
}

// GetAppBizIdsByPrimaryIds 根据appIDs获取appBizIDs
func (l *Logic) GetAppBizIdsByPrimaryIds(ctx context.Context, corpPrimaryId uint64, primaryIds []uint64) (map[uint64]uint64, error) {
	appID2AppBizIDMap := make(map[uint64]uint64)
	notInCachePrimaryIds := make([]uint64, 0)
	// 先查缓存
	for _, primaryId := range primaryIds {
		key := cast.ToString(primaryId)
		value, exist := appID2AppBizIDCache.Get(key)
		if exist {
			appBizID, ok := value.(uint64)
			if !ok {
				err := fmt.Errorf("appBizID %v format error", value)
				logx.I(ctx, "%+v", err)
				return nil, err
			}
			appID2AppBizIDMap[primaryId] = appBizID
		} else {
			notInCachePrimaryIds = append(notInCachePrimaryIds, primaryId)
		}
	}
	if len(notInCachePrimaryIds) == 0 {
		logx.D(ctx, "GetAppBizIdsByPrimaryIds all hit cache primaryIds:%v", primaryIds)
		return appID2AppBizIDMap, nil
	}
	// 未命中缓存的从数据库中查
	// corpPrimaryId := contextx.Metadata(ctx).CorpID()
	listAppBaseInfoReq := pb.ListAppBaseInfoReq{
		CorpPrimaryId: corpPrimaryId,
		AppPrimaryIds: notInCachePrimaryIds,
		PageNumber:    1,
		PageSize:      uint32(len(notInCachePrimaryIds)),
	}
	listAppBaseInfoRsp, _, err := l.rpc.ListAppBaseInfo(ctx, &listAppBaseInfoReq)
	if err != nil {
		return nil, err
	}
	for _, app := range listAppBaseInfoRsp {
		key := strconv.FormatUint(app.PrimaryId, 10)
		appID2AppBizIDCache.Set(key, app.BizId)
		appID2AppBizIDMap[app.PrimaryId] = app.BizId
	}
	logx.D(ctx, "GetAppBizIdsByPrimaryIds miss cache primaryIds:%v, ret:%+v", notInCachePrimaryIds, appID2AppBizIDMap)
	return appID2AppBizIDMap, nil
}

// GetAppPrimaryIdByBizId 根据appBizID获取appID
func (l *Logic) GetAppPrimaryIdByBizId(ctx context.Context, appBizID uint64) (uint64, error) {
	appBizID2AppIDMap, err := l.GetAppPrimaryIdsByBizIds(ctx, []uint64{appBizID})
	if err != nil {
		return 0, err
	}
	if appID, ok := appBizID2AppIDMap[appBizID]; ok {
		return appID, nil
	}
	return 0, errs.ErrAppNotFound
}

// GetAppPrimaryIdsByBizIds 根据appBizIDs获取appIDs
func (l *Logic) GetAppPrimaryIdsByBizIds(ctx context.Context, appBizIDs []uint64) (map[uint64]uint64, error) {
	appBizID2AppIDMap := make(map[uint64]uint64)
	notInCacheAppBizIDs := make([]uint64, 0)
	// 先查缓存
	for _, appBizID := range appBizIDs {
		key := cast.ToString(appBizID)
		value, exist := appBizID2AppIDCache.Get(key)
		if exist {
			appID, ok := value.(uint64)
			if !ok {
				err := fmt.Errorf("appID %v format error", value)
				logx.I(ctx, "%+v", err)
				return nil, err
			}
			appBizID2AppIDMap[appBizID] = appID
		} else {
			notInCacheAppBizIDs = append(notInCacheAppBizIDs, appBizID)
		}
	}
	if len(notInCacheAppBizIDs) == 0 {
		return appBizID2AppIDMap, nil
	}
	corpPrimaryId := contextx.Metadata(ctx).CorpID()
	idMap, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdOrBizIdList(ctx, corpPrimaryId, notInCacheAppBizIDs)
	// // 未命中缓存的从数据库中查
	// filter := &RobotFilter{
	// 	CorpId:      corpID,
	// 	BusinessIds: notInCacheAppBizIDs,
	// }
	// selectColumns := []string{RobotTblColId, RobotTblColBusinessId}
	// apps, err := GetRobotDao().GetAppList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}

	for primaryId, bizId := range idMap {
		key := strconv.FormatUint(bizId, 10)
		appBizID2AppIDCache.Set(key, primaryId)
		appBizID2AppIDMap[bizId] = primaryId
	}

	return appBizID2AppIDMap, nil
}
