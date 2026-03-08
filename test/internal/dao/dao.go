// Package dao 与外界系统做数据交换，如访问 http/cache/mq/database 等
package dao

import (
	"context"
	"time"

	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

// Dao is Dao interface
type Dao interface {
	// GetStaffSession 获取员工session
	GetStaffSession(ctx context.Context, token string) (*entity.Session, error)

	// Lock 加锁
	Lock(ctx context.Context, key string, duration time.Duration) error
	// UnLock 解锁
	UnLock(ctx context.Context, key string) error

	// GetBotBizIDByID 获取应用business_id, 带缓存
	GetBotBizIDByID(ctx context.Context, id uint64) (uint64, error)
}

type dao struct {
	rpc      *rpc.RPC
	s3       S3
	adminRdb types.AdminRedis
}

// New creates Dao instance
func New(rpc *rpc.RPC, s3 S3, adminRdb types.AdminRedis) Dao {
	d := &dao{
		rpc:      rpc,
		s3:       s3,
		adminRdb: adminRdb,
	}
	return d
}
