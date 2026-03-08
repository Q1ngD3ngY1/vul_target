package redis

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	jsoniter "github.com/json-iterator/go"
	"slices"
)

func generateAttributeLabelRedisKeyPreview(robotID uint64, attrKey string) string {
	return fmt.Sprintf("knowledge_config_attr_label_preview_%d_%s", robotID, attrKey)
}

func generateAttributeLabelRedisKeyProd(robotID uint64, attrKey string) string {
	return fmt.Sprintf("knowledge_config_attr_label_prod_%d_%s", robotID, attrKey)
}

// createAttributeLabelsRedis 新增属性标签redis
func createAttributeLabelsRedis(ctx context.Context, attrItem *model.AttributeLabelItem) error {
	log.InfoContextf(ctx, "createAttributeLabelsRedis: attrItem: %+v", attrItem)
	redisKey := generateAttributeLabelRedisKeyPreview(attrItem.Attr.RobotID, attrItem.Attr.AttrKey)
	redisValue := make([]model.AttributeLabelRedisValue, 0)
	for _, label := range attrItem.Labels {
		if len(label.SimilarLabel) == 0 {
			// 只需要缓存有相似标签的属性标签
			continue
		}
		redisValue = append(redisValue, model.AttributeLabelRedisValue{
			BusinessID:    label.BusinessID,
			Name:          label.Name,
			SimilarLabels: label.SimilarLabel,
		})
	}
	if len(redisValue) == 0 {
		return nil
	}
	var redisValueStr string
	redisValueStr, err := jsoniter.MarshalToString(redisValue)
	if err != nil {
		log.ErrorContextf(ctx, "createAttributeLabelsRedis: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	expireSec := config.App().AttributeLabel.RedisExpireSecond
	if _, err = client.Do(ctx, "SET", redisKey, redisValueStr, "EX", expireSec); err != nil {
		log.ErrorContextf(ctx, "createAttributeLabelsRedis: set redis value1 failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "createAttributeLabelsRedis ok, key:%s", redisKey)
	return nil
}

// GetAttributeLabelsRedis 获取属性标签redis
func GetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey, envType string) (
	[]model.AttributeLabelRedisValue, error) {
	log.InfoContextf(ctx, "GetAttributeLabelsRedis: env:%s, robotID: %v, attrKey: %v", envType, robotID, attrKey)
	var redisKey string
	if envType == model.AttributeLabelsPreview {
		redisKey = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	} else {
		redisKey = generateAttributeLabelRedisKeyProd(robotID, attrKey)
	}
	redisValue, err := redis.String(client.Do(ctx, "GET", redisKey))
	if err == nil {
		if redisValue == "" {
			log.ErrorContextf(ctx, "GetAttributeLabelsRedis: redis value is empty")
			return nil, fmt.Errorf("redis value is empty")
		}
		var attrLabels = make([]model.AttributeLabelRedisValue, 0)
		if err1 := jsoniter.UnmarshalFromString(redisValue, &attrLabels); err1 != nil {
			log.ErrorContextf(ctx, "GetAttributeLabelsRedis: unmarshal redis value failed, %+v", err1)
			return nil, err1
		}
		log.InfoContextf(ctx, "GetAttributeLabelsRedis result: redisKey:%s, value: %+v", redisKey, attrLabels)
		return attrLabels, nil
	}
	if errors.Is(err, redis.ErrNil) { // key不存在
		return nil, nil
	}
	log.ErrorContextf(ctx, "GetAttributeLabelsRedis failed: redisKey:%s, %+v", redisKey, err)
	return nil, err
}

// delAttributeLabelsRedis 删除属性标签redis
func delAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey string) error {
	redisKey := generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	var err error
	if _, err = client.Do(ctx, "DEL", redisKey); err != nil {
		log.InfoContextf(ctx, "delAttributeLabelsRedis: del redis key failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "delAttributeLabelsRedis ok, key:%s", redisKey)
	return nil
}

// SetAttributeLabelsRedis 添加属性标签redis
func SetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey string,
	redisValue []model.AttributeLabelRedisValue, envType string) error {
	if len(redisValue) == 0 {
		return nil
	}
	var redisKey string
	if envType == model.AttributeLabelsPreview {
		redisKey = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
	} else {
		redisKey = generateAttributeLabelRedisKeyProd(robotID, attrKey)
	}

	log.DebugContextf(ctx, "SetAttributeLabelsRedis: redisKey: %v, redisValue: %+v", redisKey, redisValue)
	var redisValueStr string
	redisValueStr, err := jsoniter.MarshalToString(redisValue)
	if err != nil {
		log.ErrorContextf(ctx, "SetAttributeLabelsRedis: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	expireSec := config.App().AttributeLabel.RedisExpireSecond
	if _, err = client.Do(ctx, "SET", redisKey, redisValueStr, "EX", expireSec); err != nil {
		log.ErrorContextf(ctx, "SetAttributeLabelsRedis: set redis value failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.DebugContextf(ctx, "SetAttributeLabelsRedis ok, key:%s", redisKey)
	return nil
}

// updateAttributeLabelsRedis 更新属性标签redis
func updateAttributeLabelsRedis(ctx context.Context, req *model.UpdateAttributeLabelReq,
	oldAttr *model.Attribute) error {
	log.InfoContextf(ctx, "updateAttributeLabelsRedis: request|Attr:%+v, deletedIDs:%+v, "+
		"AddLabels:%+v, UpdateLabels:%+v|oldAttr: %+v", req.Attr, req.DeleteLabelIDs, req.AddLabels,
		req.UpdateLabels, oldAttr)
	labelRedisValue, err := GetAttributeLabelsRedis(ctx, oldAttr.RobotID, oldAttr.AttrKey,
		model.AttributeLabelsPreview)
	if err != nil {
		// 获取旧数据失败
		log.ErrorContextf(ctx, "updateAttributeLabelsRedis, get old redis failed, err: %v", err)
		return err
	}
	newLabelRedisValue := make([]model.AttributeLabelRedisValue, 0)
	for _, label := range labelRedisValue {
		if slices.Contains(req.DeleteLabelBizIDs, label.BusinessID) { // 删除的
			continue
		}
		var i = 0
		var updateLabel *model.AttributeLabel
		for i, updateLabel = range req.UpdateLabels { // 修改的
			if updateLabel.BusinessID == label.BusinessID {
				newLabelRedisValue = append(newLabelRedisValue, model.AttributeLabelRedisValue{
					BusinessID:    updateLabel.BusinessID,
					Name:          updateLabel.Name,
					SimilarLabels: updateLabel.SimilarLabel,
				})
				break
			}
		}
		if i == len(req.UpdateLabels) {
			newLabelRedisValue = append(newLabelRedisValue, label) // 不变的
		}
	}
	for _, addLabel := range req.AddLabels {
		newLabelRedisValue = append(newLabelRedisValue, model.AttributeLabelRedisValue{
			BusinessID:    addLabel.BusinessID,
			Name:          addLabel.Name,
			SimilarLabels: addLabel.SimilarLabel,
		})
	}
	log.InfoContextf(ctx, "updateAttributeLabelsRedis: oldLabelRedisValue:%+v, newLabelRedisValue: %+v",
		labelRedisValue, newLabelRedisValue)
	return SetAttributeLabelsRedis(ctx, oldAttr.RobotID, oldAttr.AttrKey, newLabelRedisValue,
		model.AttributeLabelsPreview)
}

// PipelineDelAttributeLabelRedis 批量删除属性标签redis
func PipelineDelAttributeLabelRedis(ctx context.Context, robotID uint64, attrKeys []string,
	envType string) error {
	log.InfoContextf(ctx, "PiplineDelAttributeLabelRedis begin, robotID:%d, attrKeys:%v", robotID, attrKeys)
	conn, err := client.Pipeline(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "PiplineDelAttributeLabelRedis err:%+v", err)
		return err
	}
	defer func() { _ = conn.Close() }()
	for _, attrKey := range attrKeys {
		var key string
		if envType == model.AttributeLabelsPreview {
			key = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
		} else {
			key = generateAttributeLabelRedisKeyProd(robotID, attrKey)
		}
		if err = conn.Send("DEL", key); err != nil {
			log.ErrorContextf(ctx, "PiplineDelAttributeLabelRedis: del redis key failed, key:%s, %+v", key, err)
			return err
		}
	}
	if err = conn.Flush(); err != nil {
		log.ErrorContextf(ctx, "PiplineDelAttributeLabelRedis: flush redis pipeline failed, %+v", err)
		return err
	}
	log.InfoContextf(ctx, "PiplineDelAttributeLabelRedis ok, robotID:%d, attrKeys:%v", robotID, attrKeys)
	return nil
}

// PipelineSetAttributeLabelRedis 批量设置属性标签redis
func PipelineSetAttributeLabelRedis(ctx context.Context, robotID uint64,
	attrKey2RedisValue map[string][]model.AttributeLabelRedisValue, envType string) error {
	log.InfoContextf(ctx, "PiplineSetAttributeLabelRedis begin, robotID:%d, attrKey2RedisValue:%v",
		robotID, attrKey2RedisValue)
	conn, err := client.Pipeline(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "PiplineSetAttributeLabelRedis err:%+v", err)
		return err
	}
	defer func() { _ = conn.Close() }()
	for attrKey, redisValue := range attrKey2RedisValue {
		var redisKey string
		if envType == model.AttributeLabelsPreview {
			redisKey = generateAttributeLabelRedisKeyPreview(robotID, attrKey)
		} else {
			redisKey = generateAttributeLabelRedisKeyProd(robotID, attrKey)
		}
		var redisValueStr string
		redisValueStr, err = jsoniter.MarshalToString(redisValue)
		if err != nil {
			log.ErrorContextf(ctx, "SetAttributeLabelsRedis: marshal redisValue failed, key:%s, %+v",
				redisKey, err)
			return err
		}
		expireSec := config.App().AttributeLabel.RedisExpireSecond
		if err = conn.Send("SET", redisKey, redisValueStr, "EX", expireSec); err != nil {
			log.ErrorContextf(ctx, "PiplineSetAttributeLabelRedis: set redis key failed, key:%s, %+v",
				redisKey, err)
			return err
		}
	}
	if err = conn.Flush(); err != nil {
		log.ErrorContextf(ctx, "PiplineSetAttributeLabelRedis: flush redis pipeline failed, %+v", err)
		return err
	}
	log.InfoContextf(ctx, "PiplineSetAttributeLabelRedis ok, robotID:%d, attrKey2RedisValue:%v",
		robotID, attrKey2RedisValue)
	return nil
}
