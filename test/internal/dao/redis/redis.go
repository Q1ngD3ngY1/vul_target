package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/goredis"
	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
	redisV8 "github.com/go-redis/redis/v8"
)

var (
	client     redis.Client
	redisCli   redisV8.UniversalClient
	redisMutex = sync.Mutex{}
)

// Init 初始化
func Init() {
	client = redis.NewClientProxy("redis.qbot.admin")
}

func GetGoRedisClient(ctx context.Context) (redisV8.UniversalClient, error) {
	redisMutex.Lock()
	defer redisMutex.Unlock()
	if redisCli == nil {
		cli, err := goredis.New("redis.qbot.admin", nil)
		if err != nil {
			log.ErrorContextf(ctx, "get go redis client error, %v", err)
			return nil, err
		}
		redisCli = cli
	}

	return redisCli, nil
}

// DeleteKeysByPrefix 根据前缀删除
func DeleteKeysByPrefix(rdb redisV8.UniversalClient, prefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 创建迭代器，匹配前缀并设置每次扫描数量（建议 100-1000）
	iter := rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()
		if err := rdb.Del(ctx, key).Err(); err != nil {
			err = fmt.Errorf("DeleteKeysByPrefix key:%s, err:%v", key, err)
			return err
		}
	}

	// 检查迭代过程是否出错
	if err := iter.Err(); err != nil {
		err = fmt.Errorf("DeleteKeysByPrefix err:%v", err)
		return err
	}
	return nil
}
