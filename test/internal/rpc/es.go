package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
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
	// KnowledgeSegmentProdIndex 存储生产环境片段 knowledge_segment_prod 的索引
	KnowledgeSegmentProdIndex IndexType = "knowledge_segment_prod"
	// DocBigDataProdIndex 存储生产环境文档 big_data_prod 的索引
	DocBigDataProdIndex IndexType = "doc_big_data_prod"
)

// GetESIndex 获取索引名称
func GetESIndex(ctx context.Context, robotID uint64, indexType IndexType) (string, error) {
	indexName, ok := config.App().ESIndexNameConfig[string(indexType)]
	if !ok {
		logx.E(ctx, "index name not found, robot: %v, indexType: %s", robotID, indexType)
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
			Source map[string]any `json:"_source"`
			ID     string         `json:"_id"`
			Score  float64        `json:"_score"`
		} `json:"hits"`
	} `json:"hits"`
}

// Request 执行ES查询请求
func Request(ctx context.Context, esClient *elasticv8.TypedClient, robotID uint64, body map[string]any, indexType IndexType) (*Response, error) {
	t0 := time.Now()
	// 获取索引名称
	indexName, err := GetESIndex(ctx, robotID, indexType)
	if err != nil {
		logx.E(ctx, "Request|GetESIndex|err: %v", err)
		return nil, err
	}

	// 构建查询请求
	logx.I(ctx, "Request|indexName:%s|query:%+v", indexName, body)

	// 使用esapi.SearchRequest进行查询
	req := esapi.SearchRequest{
		Index:      []string{indexName},
		Body:       esutil.NewJSONReader(body),
		Preference: "primary_first",
	}

	res, err := req.Do(ctx, esClient)
	if err != nil {
		logx.E(ctx, "Request|Do|indexName:%s|err: %v", indexName, err)
		return nil, err
	}

	if res == nil || res.Body == nil {
		logx.E(ctx, "Request|res is nil")
		return nil, fmt.Errorf("es response is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()

	logx.D(ctx, "Request|res:%v", res.String())
	if res.IsError() {
		logx.E(ctx, "Request|error, res: %v", res.String())
		return nil, fmt.Errorf("es query error: %s", res.String())
	}

	// 解析响应
	var resp Response
	if err = json.NewDecoder(res.Body).Decode(&resp); err != nil {
		logx.E(ctx, "Request|Decode|err:%v", err)
		return nil, err
	}
	logx.I(ctx, "Request|get hits length:%d, cost: %vms", len(resp.Hits.Hits), time.Now().Sub(t0).Milliseconds())
	return &resp, nil
}
