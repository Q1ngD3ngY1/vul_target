package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strconv"

	"git.code.oa.com/trpc-go/trpc-database/localcache"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"github.com/spf13/cast"
)

const (
	expiration = 24 * 3600 // 24小时
	capacity   = 10000
)

var (
	appID2AppBizIDCache localcache.Cache
	appBizID2AppIDCache localcache.Cache
)

func init() {
	appID2AppBizIDCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
	appBizID2AppIDCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
}

// GetAppBizIDByAppID 根据appID获取appBizID
func GetAppBizIDByAppID(ctx context.Context, appID uint64) (uint64, error) {
	appID2AppBizIDMap, err := GetAppBizIDsByAppIDs(ctx, []uint64{appID})
	if err != nil {
		return 0, err
	}
	if appBizID, ok := appID2AppBizIDMap[appID]; ok {
		return appBizID, nil
	}
	return 0, errs.ErrAppNotFound
}

// GetAppBizIDsByAppIDs 根据appIDs获取appBizIDs
func GetAppBizIDsByAppIDs(ctx context.Context, appIDs []uint64) (map[uint64]uint64, error) {
	appID2AppBizIDMap := make(map[uint64]uint64)
	notInCacheAppIDs := make([]uint64, 0)
	// 先查缓存
	for _, appID := range appIDs {
		key := cast.ToString(appID)
		value, exist := appID2AppBizIDCache.Get(key)
		if exist {
			appBizID, ok := value.(uint64)
			if !ok {
				err := fmt.Errorf("appBizID %v format error", value)
				log.InfoContextf(ctx, "%+v", err)
				return nil, err
			}
			appID2AppBizIDMap[appID] = appBizID
		} else {
			notInCacheAppIDs = append(notInCacheAppIDs, appID)
		}
	}
	if len(notInCacheAppIDs) == 0 {
		return appID2AppBizIDMap, nil
	}
	// 未命中缓存的从数据库中查
	corpID := pkg.CorpID(ctx)
	filter := &RobotFilter{
		CorpId: corpID,
		IDs:    notInCacheAppIDs,
	}
	selectColumns := []string{RobotTblColId, RobotTblColBusinessId}
	apps, err := GetRobotDao().GetAppList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, app := range apps {
		key := strconv.FormatUint(app.ID, 10)
		appID2AppBizIDCache.Set(key, app.BusinessID)
		appID2AppBizIDMap[app.ID] = app.BusinessID
	}

	return appID2AppBizIDMap, nil
}

// GetAppIDByAppBizID 根据appBizID获取appID
func GetAppIDByAppBizID(ctx context.Context, appBizID uint64) (uint64, error) {
	appBizID2AppIDMap, err := GetAppIDsByAppBizIDs(ctx, []uint64{appBizID})
	if err != nil {
		return 0, err
	}
	if appID, ok := appBizID2AppIDMap[appBizID]; ok {
		return appID, nil
	}
	return 0, errs.ErrAppNotFound
}

// GetAppIDsByAppBizIDs 根据appBizIDs获取appIDs
func GetAppIDsByAppBizIDs(ctx context.Context, appBizIDs []uint64) (map[uint64]uint64, error) {
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
				log.InfoContextf(ctx, "%+v", err)
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
	// 未命中缓存的从数据库中查
	corpID := pkg.CorpID(ctx)
	filter := &RobotFilter{
		CorpId:      corpID,
		BusinessIds: notInCacheAppBizIDs,
	}
	selectColumns := []string{RobotTblColId, RobotTblColBusinessId}
	apps, err := GetRobotDao().GetAppList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, app := range apps {
		key := strconv.FormatUint(app.BusinessID, 10)
		appBizID2AppIDCache.Set(key, app.ID)
		appBizID2AppIDMap[app.BusinessID] = app.ID
	}

	return appBizID2AppIDMap, nil
}
