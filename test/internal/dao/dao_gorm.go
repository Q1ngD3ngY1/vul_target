package dao

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"

	"gorm.io/gorm"
)

const (
	sqlEqual     = " = ?"
	sqlNotEqual  = " != ?"
	sqlLess      = " < ?"
	sqlLessEqual = " <= ?"
	sqlMore      = " > ?"
	sqlMoreEqual = " >= ?"
	sqlLike      = " LIKE ?"
	sqlIn        = " IN ?"
	sqlSubIn     = " IN (?)"
	sqlSubNotIn  = " NOT IN (?)"
	sqlOrderAND  = " AND "
	sqlOr        = " OR "

	SqlOrderByAsc  = "ASC"
	SqlOrderByDesc = "DESC"

	IsNotDeleted = 0
	IsDeleted    = 1

	// MaxTextLength utf8mb4_unicode_ci字符集TEXT类型最大长度,65535/4=16383
	MaxTextLength = 16000
	// DefaultMaxPageSize 默认分页大小
	DefaultMaxPageSize = 1000
	// MaxSqlInCount sql中in集合的最大数量，避免导致慢查询
	MaxSqlInCount = 200

	// MinBizID 最小业务ID，用来兼容业务ID和系统自增ID的场景
	MinBizID = 1000000000000000000
)

var globalBaseDao *BaseDao

type BaseDao struct {
	gormDB         *gorm.DB
	gormDBDelete   *gorm.DB
	tdsqlGormDB    *gorm.DB
	text2sqlGormDB *gorm.DB
}

func Init(gormDB *gorm.DB, gormDBDelete *gorm.DB, tdsqlGormDB *gorm.DB, text2sqlGormDB *gorm.DB) {
	globalBaseDao = &BaseDao{gormDB: gormDB, gormDBDelete: gormDBDelete, tdsqlGormDB: tdsqlGormDB, text2sqlGormDB: text2sqlGormDB}
}

func GetTdsqlGormDb(ctx context.Context) *gorm.DB {
	return globalBaseDao.tdsqlGormDB.WithContext(ctx)
}

// CalculateLimit 获取分页大小
func CalculateLimit(wantedCount, alreadyGetCount uint32) uint32 {
	limit := uint32(0)
	if wantedCount > 0 {
		limit = wantedCount - alreadyGetCount
		if limit > DefaultMaxPageSize {
			limit = DefaultMaxPageSize
		}
	} else {
		limit = DefaultMaxPageSize
	}
	return limit
}

// GetGorm 根据appBizID获取gorm
func GetGorm(ctx context.Context, appBizID uint64, tableName string) (*gorm.DB, error) {
	appID, err := GetAppIDByAppBizID(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "get appID failed, err: %+v", err)
		return nil, err
	}

	db, err := knowClient.GormClient(ctx, tableName, appID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}
	return db, nil
}
