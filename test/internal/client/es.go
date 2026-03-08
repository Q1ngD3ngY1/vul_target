package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/trpc-go/trpc-database/goes"
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
)

var (
	esMutex    = sync.Mutex{}
	esTypedCli *elasticv8.TypedClient // 公有云es是8.x版本
)

type IndexType string

const (
	// AttributeIndex 存储 attribute 的索引
	AttributeIndex IndexType = "attribute"
	// AttributeLabelIndex 存储 attribute_label 的索引
	AttributeLabelIndex IndexType = "attribute_label"
)

// EsTypedClient  es v8
func EsTypedClient() (*elasticv8.TypedClient, error) {
	esMutex.Lock()
	defer esMutex.Unlock()
	if esTypedCli == nil {
		const minTimeout = 3000 * time.Millisecond
		timeout := time.Duration(config.App().ElasticSearchConfig.Timeout) * time.Millisecond
		if timeout < minTimeout {
			timeout = minTimeout
		}
		// 这里并没有使用trpc的goes客户端，不过使用了其Transport用于发送消息，可以实现有trace链路
		cli, err := elasticv8.NewTypedClient(elasticv8.Config{
			Addresses: []string{config.App().ElasticSearchConfig.URL},
			Username:  config.App().ElasticSearchConfig.User,
			Password:  config.App().ElasticSearchConfig.Password,
			Transport: goes.NewClientProxy(config.App().ElasticSearchConfig.Name, client.WithTimeout(timeout)),
			Logger:    nil,
		})
		if err != nil {
			log.Errorf("EsTypedClient|NewElasticTypedClientV8|err: %v", err)
			return nil, err
		}
		log.Infof("EsTypedClient|NewElasticTypedClientV8|cli: %+v", cli)
		esTypedCli = cli
	}
	return esTypedCli, nil
}

// GetESIndex 获取索引名称
func GetESIndex(ctx context.Context, robotID uint64, indexType IndexType) (string, error) {
	indexName, ok := config.App().ESIndexNameConfig[string(indexType)]
	if !ok {
		log.ErrorContextf(ctx, "index name not found, robot: %v, indexType: %s", robotID, indexType)
		return "", fmt.Errorf("%s es index is empty", indexType)
	}
	groupName := GetVIPGroupName(ctx, Router{RobotID: robotID})
	if groupName == "" {
		return indexName, nil
	}
	vipGroupIndex := fmt.Sprintf("%s_%s", groupName, indexName)
	return vipGroupIndex, nil
}

// Response es的响应
type Response struct {
	Hits struct {
		Hits []struct {
			Source map[string]interface{} `json:"_source"`
			ID     string                 `json:"_id"`
			Score  float64                `json:"_score"`
		} `json:"hits"`
	} `json:"hits"`
}
