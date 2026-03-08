package common

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/dao"
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
			logx.W(ctx, "LockByBizIds failed: %+v", err)
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
			logx.W(ctx, "UnlockByBizIds failed: %+v", err)
		}
	}
}
