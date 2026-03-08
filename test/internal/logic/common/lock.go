package common

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"time"
)

// LockByBizIds 获取业务id的锁
func LockByBizIds(ctx context.Context, d dao.Dao, keyPrefix string, duration time.Duration, bizIds []uint64) error {
	if len(bizIds) == 0 {
		return nil
	}
	// 必须先去重
	bizIds = slicex.Unique(bizIds)
	for _, bizId := range bizIds {
		key := fmt.Sprintf(keyPrefix, bizId)
		if err := d.Lock(ctx, key, duration); err != nil {
			// 如果某个id加锁失败就直接返回错误，不继续加锁
			log.WarnContextf(ctx, "LockByBizIds failed: %+v", err)
			return err
		}
	}
	return nil
}

// UnlockByBizIds 释放业务id的锁
func UnlockByBizIds(ctx context.Context, d dao.Dao, keyPrefix string, bizIds []uint64) {
	if len(bizIds) == 0 {
		return
	}
	// 必须先去重
	bizIds = slicex.Unique(bizIds)
	for _, bizId := range bizIds {
		key := fmt.Sprintf(keyPrefix, bizId)
		if err := d.UnLock(ctx, key); err != nil {
			// 如果某个id解锁失败，也要继续对后续id解锁
			log.WarnContextf(ctx, "UnlockByBizIds failed: %+v", err)
		}
	}
}
