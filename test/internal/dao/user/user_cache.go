package user

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

const (
	RedisThirdUserID    = "{%d}:NewKnowThirdUserID:%s" // 根据appBizID来设置哈希标签,以thirdUserID为维度 作redis key
	RedisCustUserConfig = "knowCustUserAcl:%d"         // 应用维度的特殊权限配置redis key
	RoleCacheExpire     = 3600 * 24 * 14 * time.Second
)

// ModifyUserConfigRedis 特殊权限配置的角色业务id存到缓存中,供运行时使用
func (l *daoImpl) ModifyUserConfigCache(ctx context.Context,
	appBizID, notSet, notUse uint64) error {
	tmp, err := json.Marshal(entity.CustUserConfigCache{
		NotSet: cast.ToString(notSet),
		NotUse: cast.ToString(notUse),
	})
	if err != nil {
		logx.E(ctx, "ModifyUserConfigCache marshal redis value err:%v,notSet:%v,notUse:%v",
			err, notSet, notUse)
		return err
	}
	// 有效期30天
	aclKey := getCustUserConfigKey(appBizID)
	if status := l.rdb.SetEx(ctx, aclKey, string(tmp), 3600*24*30*time.Second); status.Err() != nil {
		logx.E(ctx, "SetUserConfigRedis set redis err:%v,key:%v,value:%v", err, aclKey, string(tmp))
		return err
	}
	return nil
}

func (l *daoImpl) ModifyThirdUserIDListCache(ctx context.Context,
	appBizID uint64, thirdUserIDs []string, roleBizIDs []uint64) error {
	pipe := l.rdb.Pipeline()
	str := make([]string, 0, len(roleBizIDs))
	for _, v := range roleBizIDs {
		str = append(str, cast.ToString(v))
	}
	value := strings.Join(str, ",")
	for _, v := range thirdUserIDs {
		pipe.SetEx(ctx, getThirdUserIDKey(appBizID, v), value, 3600*24*10*time.Second)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		logx.E(ctx, "ModifyThirdUserIDListCache pipeline exec err:%v,appBizID:%v,thirdUserIDs:%v,roleBizIDs:%v",
			err, appBizID, thirdUserIDs, roleBizIDs)
		return err
	}
	return nil
}

func (l *daoImpl) DeleteThirdUserIDCache(ctx context.Context,
	appBizID uint64, thirdUserIDs []string) error {
	pipe := l.rdb.Pipeline()
	for _, v := range thirdUserIDs {
		pipe.Del(ctx, getThirdUserIDKey(appBizID, v))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		logx.E(ctx, "DelThirdUserIDRedis pipeline exec err:%v,appBizID:%v,thirdUserIDs:%v", err, appBizID, thirdUserIDs)
		return err
	}
	return nil
}

func (l *daoImpl) ModifyThirdUserIDCache(ctx context.Context,
	appBizID uint64, thirdUserID string, roleBizIDs []uint64) error {
	if len(roleBizIDs) == 0 {
		return nil
	}
	str := make([]string, 0, len(roleBizIDs))
	for _, v := range roleBizIDs {
		str = append(str, cast.ToString(v))
	}
	value := strings.Join(str, ",")
	// 有效期10天
	thirdUserIDKey := getThirdUserIDKey(appBizID, thirdUserID)
	logx.D(ctx, "ModifyThirdUserIDCache thirdUserIDKey:%s, roleBizIDs:%+v, value:%s", thirdUserIDKey, roleBizIDs, value)
	if res := l.rdb.SetNX(ctx, thirdUserIDKey, value, 3600*24*10*time.Second); res.Err() != nil {
		logx.E(ctx, "SetThirdUserIDRedis set redis err:%v,key:%v,value:%v",
			res.Err(), thirdUserIDKey, value)
		return res.Err()
	}
	return nil
}

func (l *daoImpl) modifyRoleKnowledgeCache(ctx context.Context,
	corpBizID, appBizID uint64, roleBizID uint64, knowBizID uint64) error {
	key := fmt.Sprintf(entity.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
	logx.D(ctx, "UpdateRoleKnowledgeCache key: %s, field: %d", key, knowBizID)
	if l.rdb.Exists(ctx, key).Val() == 1 {
		err := l.rdb.HSet(ctx, key, cast.ToString(knowBizID), "").Err()
		if err != nil {
			logx.E(ctx, "HSet failed, key: %s, field: %d", key, knowBizID)
			return err
		}
	}
	return nil
}

// getCustUserConfigKey 获取特殊权限配置的redis key
func getCustUserConfigKey(appBizID uint64) string {
	return fmt.Sprintf(RedisCustUserConfig, appBizID)
}

func (l *daoImpl) DescribeUserConfigCache(ctx context.Context,
	appBizID uint64) (notSet, notUse uint64, err error) {
	aclKey := getCustUserConfigKey(appBizID)
	value, err := l.rdb.Get(ctx, aclKey).Result()
	if err != nil {
		if errx.Is(err, redis.Nil) {
			return 0, 0, errx.ErrNotFound
		}
		logx.E(ctx, "DescribeUserConfigCache get redis err:%v,key:%v", err, aclKey)
		return 0, 0, err
	}
	if value == "" {
		return 0, 0, errx.ErrNotFound
	}
	var custUserConfigRedis entity.CustUserConfigCache
	if err = json.Unmarshal([]byte(value), &custUserConfigRedis); err != nil {
		logx.E(ctx, "DescribeUserConfigCache unmarshal redis value err:%v,value:%v", err, value)
		return 0, 0, err
	}
	return cast.ToUint64(custUserConfigRedis.NotSet), cast.ToUint64(custUserConfigRedis.NotUse), nil
}

func (l *daoImpl) DescribeThirdUserIDCache(ctx context.Context,
	appBizID uint64, thirdUserID string) ([]uint64, error) {
	var roleBizIDs []uint64
	thirdUserIDKey := getThirdUserIDKey(appBizID, thirdUserID)
	value, err := l.rdb.Get(ctx, thirdUserIDKey).Result()
	if err == redis.Nil || value == "" {
		logx.D(ctx, "DescribeThirdUserIDCache redis not exist,key:%v", thirdUserIDKey)
		return roleBizIDs, errx.ErrNotFound
	} else if err != nil {
		logx.E(ctx, "DescribeThirdUserIDCache get redis err:%v,key:%v",
			err, thirdUserIDKey)
		return roleBizIDs, err
	}
	str := strings.Split(value, ",")
	for _, v := range str {
		roleBizIDs = append(roleBizIDs, cast.ToUint64(v))
	}
	logx.D(ctx, "DescribeThirdUserIDCache thirdUserIDKey:%s,roleBizIDs:%+v,value:%s", thirdUserIDKey, roleBizIDs, value)
	return roleBizIDs, nil
}

func (l *daoImpl) deleteKnowledgeCache(ctx context.Context,
	corpBizID, appBizID uint64, roleBizID uint64, knowledgeBizIds []uint64) error {
	logx.D(ctx, "DeleteKnowledgeCache corpBizID: %d, appBizID: %d, roleBizID: %d, knowledgeBizIds: %+v",
		corpBizID, appBizID, roleBizID, knowledgeBizIds)
	for _, know := range knowledgeBizIds {
		key := fmt.Sprintf(entity.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
		if err := l.rdb.HDel(ctx, key, cast.ToString(know)).Err(); err != nil {
			logx.E(ctx, "HDel failed, key: %s, field: %d err:%v", key, know, err)
			return err
		}
	}
	return nil
}

func (l *daoImpl) DescribeKnowBizID2FilterCache(ctx context.Context,
	corpBizID, appBizID, roleBizID uint64) (bool, map[string]*retrieval.LabelExpression, error) {
	key := fmt.Sprintf(entity.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
	if l.rdb.Exists(ctx, key).Val() == 0 {
		return false, nil, nil
	}

	value, err := l.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		logx.E(ctx, "DescribeKnowBizID2FilterCache HGetAll err:%v,key:%v", err, key)
		return false, nil, nil
	}

	result := make(map[string]*retrieval.LabelExpression)
	for k, v := range value {
		if v == "" { // 空值，表示该知识库未设置过滤条件
			result[k] = nil
			continue
		}
		knowFilter := &retrieval.LabelExpression{}
		if err := json.Unmarshal([]byte(v), knowFilter); err != nil {
			logx.E(ctx, "json.Unmarshal err:%v", err)
			return false, nil, errs.ErrGetRoleListFail
		}
		result[k] = knowFilter
	}
	return true, result, nil
}

func (l *daoImpl) ModifyKnowBizID2FilterCache(ctx context.Context,
	corpBizID, appBizID, roleBizID uint64, knowBizID2Filter map[uint64]*retrieval.LabelExpression) error {
	key := fmt.Sprintf(entity.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
	for knowBizID, filter := range knowBizID2Filter {
		filterBytes, err := json.Marshal(filter)
		if err != nil {
			logx.E(ctx, "json.Marshal err:%v", err)
			return err
		}
		if err := l.rdb.HSet(ctx, key, cast.ToString(knowBizID), string(filterBytes)).Err(); err != nil {
			logx.E(ctx, "HSet err:%v,key:%v", err, key)
			return err
		}
	}
	l.rdb.Expire(ctx, key, RoleCacheExpire)
	return nil
}

func (l *daoImpl) ModifyRoleChooseAllCache(ctx context.Context,
	corpBizID, appBizID, roleBizID uint64) error {
	key := fmt.Sprintf(entity.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
	l.rdb.Del(ctx, key)
	return l.rdb.HSet(ctx, key, "", "").Err()
}

func (l *daoImpl) DeleteKnowBizID2FilterCache(ctx context.Context,
	corpBizID, appBizID, roleBizID, knowBizID uint64) error {
	key := fmt.Sprintf(entity.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
	if err := l.rdb.HDel(ctx, key, cast.ToString(knowBizID)).Err(); err != nil {
		logx.E(ctx, "HDel err:%v,key:%v", err, key)
		return err
	}
	return nil
}
