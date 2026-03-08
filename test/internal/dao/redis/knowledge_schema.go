package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/go-redis/redis/v8"
)

func genKnowledgeSchemaKey(appBizId uint64, envType string) string {
	return fmt.Sprintf("knowledge_schema_%d_%s", appBizId, envType)
}

// GetKnowledgeSchema 从缓存中获取知识库schema数据，无需再次转换，可以直接作为结果返回。在首次获取时可能为空
func GetKnowledgeSchema(ctx context.Context, appBizId uint64, envType string) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	redisClient, err := GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema GetGoRedisClient fail, err: %+v", err)
		return nil, err
	}

	key := genKnowledgeSchemaKey(appBizId, envType)
	result, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, err
		}
		log.ErrorContextf(ctx, "GetKnowledgeSchema get redis result fail, err: %+v", err)
		return nil, err
	}

	var schemaItems []*pb.GetKnowledgeSchemaRsp_SchemaItem
	err = json.Unmarshal([]byte(result), &schemaItems)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema Unmarshal schemaItems fail, err: %+v", err)
		return nil, err
	}
	return schemaItems, nil
}

// SetKnowledgeSchema 设置知识库schema缓存数据
func SetKnowledgeSchema(ctx context.Context, appBizId uint64, envType string, schemaItems []*pb.GetKnowledgeSchemaRsp_SchemaItem) error {
	redisClient, err := GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeSchema GetGoRedisClient fail, err: %+v", err)
		return err
	}

	val, err := json.Marshal(schemaItems)
	if err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeSchema Marshal schemaItems fail, err: %+v", err)
		return err
	}
	key := genKnowledgeSchemaKey(appBizId, envType)

	if err = redisClient.Set(ctx, key, val, 0).Err(); err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeSchema set redis value fail, err: %+v", err)
		return err
	}
	return nil
}

// genKnowledgeSchemaDocClusterKeyPrefix 目录id映射文档自增id的缓存key前缀
func genKnowledgeSchemaDocClusterKeyPrefix(appBizId uint64, envType string) string {
	return fmt.Sprintf("knowledge_schema_doc_cluster_%d_%s_", appBizId, envType)
}

// genKnowledgeSchemaDocClusterBizIDKey 目录id映射文档自增id的缓存key
func genKnowledgeSchemaDocClusterBizIDKey(appBizId uint64, envType string, docClusterBizId uint64) string {
	prefix := genKnowledgeSchemaDocClusterKeyPrefix(appBizId, envType)
	return fmt.Sprintf("%s%d", prefix, docClusterBizId)
}

// GetKnowledgeSchemaDocIdByDocClusterId 读取目录id映射文档自增id的缓存，文档自增id对应t_doc表的id字段
func GetKnowledgeSchemaDocIdByDocClusterId(ctx context.Context, appBizId uint64, envType string,
	docClusterBizId uint64) ([]uint64, error) {
	redisClient, err := GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaDocIdByDocClusterId GetGoRedisClient fail, err: %+v", err)
		return nil, err
	}

	key := genKnowledgeSchemaDocClusterBizIDKey(appBizId, envType, docClusterBizId)
	result, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		// 如果redis中没有数据，则返回空，不报错
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		log.ErrorContextf(ctx, "GetKnowledgeSchemaDocIdByDocClusterId get redis result fail, err: %+v", err)
		return nil, err
	}

	var docIds []uint64
	err = json.Unmarshal([]byte(result), &docIds)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaDocIdByDocClusterId Unmarshal docIds fail, err: %+v", err)
		return nil, err
	}
	return docIds, nil
}

// SetKnowledgeSchemaDocIdByDocClusterId 设置目录id映射文档自增id的缓存，文档自增id对应t_doc表的id字段
func SetKnowledgeSchemaDocIdByDocClusterId(ctx context.Context, appBizId uint64, envType string,
	docClusterSchema *model.DocClusterSchema) error {
	if docClusterSchema == nil {
		err := fmt.Errorf("SetKnowledgeSchemaDocIdByDocClusterId docClusterSchema is nil")
		log.ErrorContextf(ctx, "%+v", err)
		return err
	}
	redisClient, err := GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeSchemaDocIdByDocClusterId GetGoRedisClient fail, err: %+v", err)
		return err
	}

	key := genKnowledgeSchemaDocClusterBizIDKey(appBizId, envType, docClusterSchema.BusinessID)

	if err = redisClient.Set(ctx, key, docClusterSchema.DocIDs, 0).Err(); err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeSchemaDocIdByDocClusterId set redis value fail, err: %+v", err)
		return err
	}
	return nil
}

// DeleteKnowledgeSchemaOldVersion 删除所有旧聚类的缓存
func DeleteKnowledgeSchemaOldVersion(ctx context.Context, appBizId uint64, envType string) error {
	prefix := genKnowledgeSchemaDocClusterKeyPrefix(appBizId, envType)
	redisClient, err := GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeSchemaOldVersion GetGoRedisClient fail, err: %+v", err)
		return err
	}
	if err = DeleteKeysByPrefix(redisClient, prefix); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeSchemaOldVersion fail, err: %+v", err)
		return err
	}
	return nil
}

// genKnowledgeSchemaDocClusterId2AppBizIdKey 目录id映射文档AppBizId的缓存key
func genKnowledgeSchemaDocClusterId2AppBizIdKey(docClusterBizId uint64, envType string) string {
	return fmt.Sprintf("knowledge_schema_doc_cluster_2_app_biz_id_%d_%s", docClusterBizId, envType)
}

// GetKnowledgeSchemaAppBizIdByDocClusterId 读取目录id映射问文档对应的AppBizId的缓存
func GetKnowledgeSchemaAppBizIdByDocClusterId(ctx context.Context,
	docClusterBizId uint64, envType string) (uint64, error) {
	redisClient, err := GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaAppBizIdByDocClusterId GetGoRedisClient fail, err: %+v", err)
		return 0, err
	}

	key := genKnowledgeSchemaDocClusterId2AppBizIdKey(docClusterBizId, envType)
	result, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		// 如果redis中没有数据，则返回空，不报错
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		log.ErrorContextf(ctx, "GetKnowledgeSchemaAppBizIdByDocClusterId get redis result fail, err: %+v", err)
		return 0, err
	}

	var appBizId uint64
	err = json.Unmarshal([]byte(result), &appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaAppBizIdByDocClusterId Unmarshal appBizId fail, err: %+v", err)
		return 0, err
	}
	return appBizId, nil
}

// SetKnowledgeSchemaAppBizIdByDocClusterId 设置目录id映射文档AppBizId的缓存
func SetKnowledgeSchemaAppBizIdByDocClusterId(ctx context.Context,
	docClusterBizId, appBizId uint64, envType string) error {
	if docClusterBizId == 0 || appBizId == 0 {
		err := fmt.Errorf("SetKnowledgeSchemaAppBizIdByDocClusterId docClusterBizId or appBizId is 0")
		log.ErrorContextf(ctx, "%+v", err)
		return err
	}
	redisClient, err := GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeSchemaDocIdByDocClusterId GetGoRedisClient fail, err: %+v", err)
		return err
	}

	key := genKnowledgeSchemaDocClusterId2AppBizIdKey(docClusterBizId, envType)

	if err = redisClient.Set(ctx, key, appBizId, 0).Err(); err != nil {
		log.ErrorContextf(ctx, "SetKnowledgeSchemaDocIdByDocClusterId set redis value fail, err: %+v", err)
		return err
	}
	return nil
}
