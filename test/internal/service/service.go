// Package service 业务逻辑层
package service

import (
	"context"
	"fmt"
	Permis "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/permissions"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/goredis"
	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

// Service is service logic object
type Service struct {
	dao         dao.Dao
	permisLogic Permis.PermisLogic
}

// New creates service instance
func New() *Service {
	d := dao.New()
	logic := Permis.NewPermisLogic(d)
	srv := Service{
		dao:         d,
		permisLogic: logic,
	}
	dao.Init(d.GetGormDB(), d.GetGormDBDelete(), d.GetTdsqlGormDB(), d.GetText2sqlGormDB())
	client.Init(d.GetAdminApiCli(), d.GetDirectIndexCli(), d.GetRetrievalCli(), d.GetTaskFlowCli(),
		d.GetTDocLinkerCli(), d.GetDocParseCli(), d.GetFinanceCli(), d.GetLlmmCli())

	return &srv
}

// GetDB 返回DB实例
func (s *Service) GetDB() mysql.Client {
	return s.dao.GetDB()
}

// Scheduler 调度策略
type Scheduler struct {
	redisClient redis.UniversalClient
}

// NewScheduler 新建调度策略
func NewScheduler() *Scheduler {
	r, err := goredis.New("redis.qbot.admin", nil)
	ctx := trpc.BackgroundContext()
	if err != nil {
		log.ErrorContextf(ctx, "init Scheduler redis client failed.  err:%v", err)
	} else {
		log.InfoContextf(ctx, "init Scheduler goredis client success: %s", r.Ping(context.Background()).String())
	}
	return &Scheduler{
		redisClient: r,
	}
}

const (
	// RedisKey 前缀标识
	RedisKey = "lke:knowledge:scheduler"
)

// Schedule 互斥任务定时器； holeTime 默认1s
func (s *Scheduler) Schedule(serviceName string, newNode string, holdTime time.Duration) (nowNode string, err error) {
	ctx := context.TODO()
	key := RedisKey + ":" + serviceName

	res := s.redisClient.SetNX(ctx, key, newNode, holdTime)
	if res.Val() {
		return newNode, nil
	}
	return s.redisClient.Get(ctx, key).Val(), fmt.Errorf("locak failed")
}

// GetTdSqlDB 返回tdsql实例
func (s *Service) GetTdSqlDB() *gorm.DB {
	return s.dao.GetTdsqlGormDB()
}
