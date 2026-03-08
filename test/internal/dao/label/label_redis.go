package label

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"github.com/redis/go-redis/v9"
)

// --------------------------redis--------------------------
func generateAttributeLabelRedisKeyPreview(robotID uint64, attrKey string) string {
	return fmt.Sprintf("knowledge_config_attr_label_preview_%d_%s", robotID, attrKey)
}

func generateAttributeLabelRedisKeyProd(robotID uint64, attrKey string) string {
	return fmt.Sprintf("knowledge_config_attr_label_prod_%d_%s", robotID, attrKey)
}

func (d *daoImpl) GetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey, envType string) ([]entity.AttributeLabelRedisValue, error) {
	var redisKey string
	if envType == entity.AttributeLabelsPreview {
		redisKey = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	} else {
		redisKey = generateAttributeLabelRedisKeyProd(robotID, attrKey)
	}
	redisValue, err := d.rdb.Get(ctx, redisKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("redis key not find : key=%s", redisKey) // Key不存在时返回nil
		}
		return nil, fmt.Errorf("redis query failed : %v", err)
	}
	if redisValue == "" {
		return nil, fmt.Errorf("redis value is empty")
	}
	var attrLabels = make([]entity.AttributeLabelRedisValue, 0)
	if err := jsonx.UnmarshalFromString(redisValue, &attrLabels); err != nil {
		return nil, fmt.Errorf("GetAttributeLabelsRedis: unmarshal redis value failed, %+v", err)
	}
	logx.I(ctx, "GetAttributeLabelsRedis result: redisKey:%s, value: %+v", redisKey, attrLabels)
	return attrLabels, nil
}

// SetAttributeLabelsRedis 添加属性标签到Redis
func (d *daoImpl) SetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey string,
	redisValue []entity.AttributeLabelRedisValue, envType string) error {
	if len(redisValue) == 0 {
		return fmt.Errorf("SetAttributeLabelsRedis redisValue is empty") // 空直接返回，避免无效查询
	}
	// 生成Redis key
	var redisKey string
	if envType == entity.AttributeLabelsPreview {
		redisKey = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	} else {
		redisKey = generateAttributeLabelRedisKeyProd(robotID, attrKey)
	}
	logx.D(ctx, "SetAttributeLabelsRedis: redisKey: %s, valueCount: %d", redisKey, len(redisValue))
	// 序列化数据
	redisValueStr, err := jsonx.MarshalToString(redisValue)
	if err != nil {
		logx.E(ctx, "SetAttributeLabelsRedis: marshal redisValue failed, key: %s, err: %v", redisKey, err)
		return fmt.Errorf("marshal redis value failed: %w", err)
	}
	// 设置过期时间
	expireSec := config.App().AttributeLabel.RedisExpireSecond
	expireTime := time.Duration(expireSec) * time.Second
	// 使用Redis客户端设置值
	err = d.rdb.Set(ctx, redisKey, redisValueStr, expireTime).Err()
	if err != nil {
		logx.E(ctx, "SetAttributeLabelsRedis: set redis value failed, key: %s, err: %v", redisKey, err)
		return fmt.Errorf("set redis value failed: %w", err)
	}
	logx.D(ctx, "SetAttributeLabelsRedis success, key: %s, expire: %v", redisKey, expireTime)
	return nil
}

// PipelineDelAttributeLabelRedis 使用管道批量删除属性标签Redis键
func (d *daoImpl) PipelineDelAttributeLabelRedis(ctx context.Context, robotID uint64, attrKeys []string, envType string) error {
	if len(attrKeys) == 0 {
		return fmt.Errorf("SetAttributeLabelsRedis redisValue is empty") // 空直接返回，避免无效查询
	}
	logx.I(ctx, "PipelineDelAttributeLabelRedis begin, robotID: %d, attrKeys: %v", robotID, attrKeys)
	// 创建管道
	pipe := d.rdb.Pipeline()
	// 为每个属性键生成Redis key并添加到管道
	for _, attrKey := range attrKeys {
		var redisKey string
		if envType == entity.AttributeLabelsPreview {
			redisKey = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
		} else {
			redisKey = generateAttributeLabelRedisKeyProd(robotID, attrKey)
		}
		pipe.Del(ctx, redisKey)
	}
	// 执行管道操作
	_, err := pipe.Exec(ctx)
	if err != nil {
		logx.E(ctx, "PipelineDelAttributeLabelRedis: exec pipeline failed, robotID: %d, err: %v", robotID, err)
		return fmt.Errorf("exec redis pipeline failed: %w", err)
	}
	logx.I(ctx, "PipelineDelAttributeLabelRedis success, robotID: %d, keyCount: %d", robotID, len(attrKeys))
	return nil
}

// CreateAttributeLabelsRedis 新增属性标签redis
func (d *daoImpl) CreateAttributeLabelsRedis(ctx context.Context, attrItem *entity.AttributeLabelItem) error {
	if attrItem == nil || attrItem.Attr == nil || len(attrItem.Labels) == 0 {
		return fmt.Errorf("createAttributeLabelsRedis: invalid attrItem or empty labels")
	}
	logx.I(ctx, "createAttributeLabelsRedis: attrItem: %+v", attrItem)
	// 生成Redis key
	redisKey := generateAttributeLabelRedisKeyPreview(attrItem.Attr.RobotID, attrItem.Attr.AttrKey)
	// 构建Redis值
	redisValue := make([]entity.AttributeLabelRedisValue, 0, len(attrItem.Labels))
	for _, label := range attrItem.Labels {
		redisValue = append(redisValue, entity.AttributeLabelRedisValue{
			BusinessID:    label.BusinessID,
			Name:          label.Name,
			SimilarLabels: label.SimilarLabel,
		})
	}
	// 序列化数据
	redisValueStr, err := jsonx.MarshalToString(redisValue)
	if err != nil {
		logx.E(ctx, "createAttributeLabelsRedis: marshal redisValue failed, key:%s, err:%v", redisKey, err)
		return fmt.Errorf("marshal redis value failed: %w", err)
	}

	// 设置过期时间
	expireSec := config.App().AttributeLabel.RedisExpireSecond
	expireTime := time.Duration(expireSec) * time.Second
	// 使用Redis客户端设置值
	err = d.rdb.Set(ctx, redisKey, redisValueStr, expireTime).Err()
	if err != nil {
		logx.E(ctx, "createAttributeLabelsRedis: set redis value failed, key:%s, err:%v", redisKey, err)
		return fmt.Errorf("set redis value failed: %w", err)
	}

	logx.I(ctx, "createAttributeLabelsRedis ok, key:%s, expire:%v", redisKey, expireTime)
	return nil
}

// PipelineSetAttributeLabelRedis 使用管道批量设置属性标签到Redis
func (d *daoImpl) PipelineSetAttributeLabelRedis(ctx context.Context, robotID uint64,
	attrKey2RedisValue map[string][]entity.AttributeLabelRedisValue, envType string) error {
	if len(attrKey2RedisValue) == 0 {
		return fmt.Errorf("SetAttributeLabelsRedis attrKey2RedisValue is empty") // 空直接返回，避免无效查询
	}
	logx.I(ctx, "PipelineSetAttributeLabelRedis begin, robotID: %d, keyCount: %d",
		robotID, len(attrKey2RedisValue))
	// 创建管道
	pipe := d.rdb.Pipeline()
	expireTime := time.Duration(config.App().AttributeLabel.RedisExpireSecond) * time.Second
	// 处理每个属性键值对
	for attrKey, redisValue := range attrKey2RedisValue {
		if len(redisValue) == 0 {
			continue // 跳过空值
		}
		// 生成Redis key
		var redisKey string
		if envType == entity.AttributeLabelsPreview {
			redisKey = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
		} else {
			redisKey = generateAttributeLabelRedisKeyProd(robotID, attrKey)
		}
		// 序列化数据
		redisValueStr, err := jsonx.MarshalToString(redisValue)
		if err != nil {
			logx.E(ctx, "PipelineSetAttributeLabelRedis: marshal failed, key: %s, err: %v",
				redisKey, err)
			return fmt.Errorf("marshal redis value for key: %s failed err = %v", redisKey, err)
		}
		// 添加到管道
		pipe.Set(ctx, redisKey, redisValueStr, expireTime)
	}
	// 执行管道操作
	_, err := pipe.Exec(ctx)
	if err != nil {
		logx.E(ctx, "PipelineSetAttributeLabelRedis: exec pipeline failed, robotID: %d, err: %v",
			robotID, err)
		return fmt.Errorf("exec redis pipeline failed: %w", err)
	}
	logx.I(ctx, "PipelineSetAttributeLabelRedis success, robotID: %d, keyCount: %d",
		robotID, len(attrKey2RedisValue))
	return nil
}
