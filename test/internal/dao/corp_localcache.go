package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strconv"

	"git.code.oa.com/trpc-go/trpc-database/localcache"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"github.com/spf13/cast"
)

const (
	corpLocalCacheExpiration = 24 * 3600 // 24小时
	corpLocalCacheCapacity   = 10000
)

var (
	corpID2CorpBizIDCache localcache.Cache
	corpBizID2CorpIDCache localcache.Cache
)

func init() {
	corpID2CorpBizIDCache = localcache.New(localcache.WithExpiration(corpLocalCacheExpiration), localcache.WithCapacity(corpLocalCacheCapacity))
	corpBizID2CorpIDCache = localcache.New(localcache.WithExpiration(corpLocalCacheExpiration), localcache.WithCapacity(corpLocalCacheCapacity))
}

// GetCorpBizIDByCorpID 根据CorpID获取CorpBizID
func GetCorpBizIDByCorpID(ctx context.Context, corpID uint64) (uint64, error) {
	corpID2CorpBizIDMap, err := GetCorpBizIDsByCorpIDs(ctx, []uint64{corpID})
	if err != nil {
		return 0, err
	}
	if corpBizID, ok := corpID2CorpBizIDMap[corpID]; ok {
		return corpBizID, nil
	}
	return 0, errs.ErrCorpNotFound
}

// GetCorpBizIDsByCorpIDs 根据CorpIDs获取CorpBizIDs
func GetCorpBizIDsByCorpIDs(ctx context.Context, corpIDs []uint64) (map[uint64]uint64, error) {
	corpID2CorpBizIDMap := make(map[uint64]uint64)
	notInCacheCorpIDs := make([]uint64, 0)
	// 先查缓存
	for _, corpID := range corpIDs {
		key := cast.ToString(corpID)
		value, exist := corpID2CorpBizIDCache.Get(key)
		if exist {
			corpBizID, ok := value.(uint64)
			if !ok {
				err := fmt.Errorf("corpBizID %v format error", value)
				log.InfoContextf(ctx, "%+v", err)
				return nil, err
			}
			corpID2CorpBizIDMap[corpID] = corpBizID
		} else {
			notInCacheCorpIDs = append(notInCacheCorpIDs, corpID)
		}
	}
	if len(notInCacheCorpIDs) == 0 {
		return corpID2CorpBizIDMap, nil
	}
	// 未命中缓存的从数据库中查
	filter := &CorpFilter{
		IDs: notInCacheCorpIDs,
	}
	selectColumns := []string{CorpTblColID, CorpTblColBusinessID}
	corps, err := GetCorpDao().GetCorpList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, corp := range corps {
		key := strconv.FormatUint(corp.ID, 10)
		if ok := corpID2CorpBizIDCache.Set(key, corp.BusinessID); !ok {
			log.WarnContextf(ctx, "corpID2CorpBizIDCache.Set %s failed", key)
		}
		corpID2CorpBizIDMap[corp.ID] = corp.BusinessID
	}

	return corpID2CorpBizIDMap, nil
}

// GetCorpIDByCorpBizID 根据corpBizID获取corpID
func GetCorpIDByCorpBizID(ctx context.Context, corpBizID uint64) (uint64, error) {
	corpBizID2CorpIDMap, err := GetCorpIDsByCorpBizIDs(ctx, []uint64{corpBizID})
	if err != nil {
		return 0, err
	}
	if corpID, ok := corpBizID2CorpIDMap[corpBizID]; ok {
		return corpID, nil
	}
	return 0, errs.ErrCorpNotFound
}

// GetCorpIDsByCorpBizIDs 根据corpBizIDs获取corpIDs
func GetCorpIDsByCorpBizIDs(ctx context.Context, corpBizIDs []uint64) (map[uint64]uint64, error) {
	corpBizID2CorpIDMap := make(map[uint64]uint64)
	notInCacheCorpBizIDs := make([]uint64, 0)
	// 先查缓存
	for _, corpBizID := range corpBizIDs {
		key := cast.ToString(corpBizID)
		value, exist := corpBizID2CorpIDCache.Get(key)
		if exist {
			corpID, ok := value.(uint64)
			if !ok {
				err := fmt.Errorf("corpID %v format error", value)
				log.InfoContextf(ctx, "%+v", err)
				return nil, err
			}
			corpBizID2CorpIDMap[corpBizID] = corpID
		} else {
			notInCacheCorpBizIDs = append(notInCacheCorpBizIDs, corpBizID)
		}
	}
	if len(notInCacheCorpBizIDs) == 0 {
		return corpBizID2CorpIDMap, nil
	}
	// 未命中缓存的从数据库中查
	filter := &CorpFilter{
		BusinessIds: notInCacheCorpBizIDs,
	}
	selectColumns := []string{CorpTblColID, CorpTblColBusinessID}
	corps, err := GetCorpDao().GetCorpList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, corp := range corps {
		key := strconv.FormatUint(corp.BusinessID, 10)
		if ok := corpBizID2CorpIDCache.Set(key, corp.ID); !ok {
			log.WarnContextf(ctx, "corpBizID2CorpIDCache.Set %s failed", key)
		}
		corpBizID2CorpIDMap[corp.BusinessID] = corp.ID
	}

	return corpBizID2CorpIDMap, nil
}
