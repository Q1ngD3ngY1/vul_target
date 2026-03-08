package app

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"github.com/spf13/cast"
)

const (
	DefaultUpgradeCacheExpiredS = 7 * 24 * 3600
)

type UpgradeType string

const (
	SyncAttributeUpgrade        UpgradeType = "sync_attribute"
	SyncVectorLabel             UpgradeType = "update_label"
	SyncOrgDataUpgrade          UpgradeType = "sync_orgdata"
	SyncDbSourceVdbIndexUpgrade UpgradeType = "sync_db_source_vdb_index"
)

// UpgradeCache 记录应用升级的缓存
type UpgradeCache struct {
	// UpgradeType 升级的类型，用于组成redis的key
	UpgradeType  UpgradeType
	ExpiredTimeS int
}

func (u *UpgradeCache) genRedisKey() string {
	return fmt.Sprintf("knowledge:upgrade:%s", u.UpgradeType)
}

// SetAppFinish 标识应用已刷完标签
func (u *UpgradeCache) SetAppFinish(ctx context.Context, robotID uint64) error {
	key := u.genRedisKey()
	client, err := redis.GetGoRedisClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.HSet(ctx, key, strconv.FormatUint(robotID, 10), time.Now().Format("2006-01-02 15:04:05")).Result()
	if err != nil {
		log.ErrorContextf(ctx, "setAppFinish redis.HSet fail, err: %+v, robotID:%d", err, robotID)
		return err
	}
	if u.ExpiredTimeS == 0 {
		u.ExpiredTimeS = DefaultUpgradeCacheExpiredS
	}
	_, err = client.Expire(ctx, key, time.Duration(u.ExpiredTimeS)*time.Second).Result()
	if err != nil {
		log.ErrorContextf(ctx, "set key %v expired fail, err: %v", key, err)
		return err
	}
	return nil
}

// GetNotUpgradedApps 检查应用是否已完成 返回未完成待处理的应用ID
func (u *UpgradeCache) GetNotUpgradedApps(ctx context.Context, robotIDs []uint64) ([]uint64, error) {
	var robotIDStrings []string
	for _, robotID := range robotIDs {
		robotIDStrings = append(robotIDStrings, cast.ToString(robotID))
	}

	key := u.genRedisKey()
	client, err := redis.GetGoRedisClient(ctx)
	if err != nil {
		return nil, err
	}
	pendingIDs := make([]uint64, 0)
	for _, batchIDs := range slicex.Chunk(robotIDStrings, 1000) {
		values, err := client.HMGet(ctx, key, batchIDs...).Result()
		if err != nil {
			log.ErrorContextf(ctx, "GetNotUpgradedApps hmget error, key: %v, err: %v", key, err)
			return nil, err
		}
		for i, v := range values {
			if v == nil {
				pendingIDs = append(pendingIDs, cast.ToUint64(batchIDs[i]))
			} else {
				log.InfoContextf(ctx, "robotID:%s Completed and skip, value:%+v", batchIDs[i], v)
			}
		}
	}
	return pendingIDs, nil
}
