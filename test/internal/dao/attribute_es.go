package dao

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/common/v3/utils"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/spf13/cast"
)

type AttributeEs struct {
	AttrID     uint64 `json:"attr_id"`
	RobotID    uint64 `json:"robot_id"`
	Attribute  string `json:"attribute"`
	UpdateTime string `json:"update_time"`
}

// BulkAttributeEs 定义批量操作数据结构
type BulkAttributeEs struct {
	Index  string
	DocID  string
	Source AttributeEs
}

// BatchAddAndUpdateAttributes ES批量新增或修改Attribute
func BatchAddAndUpdateAttributes(ctx context.Context, robotID uint64, attributes []*model.Attribute) error {
	log.InfoContextf(ctx, "BatchAddAndUpdateAttributes|count: %d", len(attributes))
	if len(attributes) == 0 {
		return nil
	}

	// 1. 准备索引名称
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeIndex)
	if err != nil {
		log.ErrorContextf(ctx, "GetESIndex error: %v", err)
		return err
	}

	// 2. 转换数据结构
	var bulkRequests []BulkAttributeEs
	for _, attr := range attributes {
		if attr.ID == 0 {
			log.ErrorContextf(ctx, "Invalid attribute ID, attribute: %+v", attr)
			return fmt.Errorf("attribute id is empty")
		}

		updateTime := attr.UpdateTime
		if updateTime.IsZero() {
			updateTime = time.Now()
		}

		doc := AttributeEs{
			AttrID:     attr.ID,
			RobotID:    robotID,
			Attribute:  attr.Name,
			UpdateTime: updateTime.Format("2006-01-02 15:04:05"),
		}

		bulkRequests = append(bulkRequests, BulkAttributeEs{
			Index:  indexName,
			DocID:  cast.ToString(attr.ID),
			Source: doc,
		})
	}

	// 3. 构造Bulk请求体
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	for _, req := range bulkRequests {
		// 操作元数据 (index类型表示创建/更新)
		meta := map[string]interface{}{
			"index": map[string]string{
				"_index": req.Index,
				"_id":    req.DocID,
			},
		}
		if err := encoder.Encode(meta); err != nil {
			log.ErrorContextf(ctx, "Encode metadata error|docID:%s|err:%v", req.DocID, err)
			return err
		}

		// 文档数据
		if err := encoder.Encode(req.Source); err != nil {
			log.ErrorContextf(ctx, "Encode document error|docID:%s|err:%v", req.DocID, err)
			return err
		}
	}
	log.InfoContextf(ctx, "Prepared bulk request with %d operations", len(bulkRequests))

	// 4. 执行Bulk请求
	es, err := knowClient.EsTypedClient()
	if err != nil {
		return err
	}

	bulkReq := esapi.BulkRequest{
		Body:   &buf,
		Pretty: false,
		Human:  false,
	}

	res, err := bulkReq.Do(ctx, es)
	if err != nil {
		log.ErrorContextf(ctx, "Bulk request error|err:%v", err)
		return err
	}
	defer res.Body.Close()

	// 5. 处理响应错误
	if res.IsError() {
		var errRes map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			log.ErrorContextf(ctx, "Error parsing error response|err:%v", err)
			return err
		}
		log.ErrorContextf(ctx, "Bulk operation failed|reason:%v", errRes["error"])
		return fmt.Errorf("bulk operation failed: %v", errRes["error"])
	}

	// 6. 解析响应检查部分失败
	var blkResp map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&blkResp); err != nil {
		log.ErrorContextf(ctx, "Parse bulk response error|err:%v", err)
		return err
	}

	// 7. 检查部分失败的情况
	if blkResp["errors"].(bool) {
		errorItems := make([]string, 0)
		for _, item := range blkResp["items"].([]interface{}) {
			op := item.(map[string]interface{})["index"].(map[string]interface{})
			if status, ok := op["status"].(float64); ok && status >= 400 {
				errorItems = append(errorItems, fmt.Sprintf("ID:%s Status:%v Error:%v",
					op["_id"], status, op["error"]))
			}
		}
		if len(errorItems) > 0 {
			log.ErrorContextf(ctx, "Partial bulk failures|errors:%d/%d|details:%v",
				len(errorItems), len(bulkRequests), strings.Join(errorItems, "; "))
			return fmt.Errorf("partial failures: %d errors", len(errorItems))
		}
	}

	log.InfoContextf(ctx, "Bulk operation success|count:%d", len(bulkRequests))
	return nil
}

// AddAndUpdateAttribute ES新增或修改Attribute
func AddAndUpdateAttribute(ctx context.Context, robotID uint64, attr *model.Attribute) error {
	log.InfoContextf(ctx, "AddAndUpdateAttribute|req: %+v", attr)
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeIndex)
	if err != nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttribute err: %v", err)
		return err
	}
	es, err := knowClient.EsTypedClient()
	if err != nil {
		return err
	}

	updateTime := attr.UpdateTime
	if updateTime.IsZero() {
		updateTime = time.Now()
	}
	data := AttributeEs{
		AttrID:     attr.ID,
		RobotID:    robotID,
		Attribute:  attr.Name,
		UpdateTime: updateTime.Format("2006-01-02 15:04:05"),
	}

	body, err := json.Marshal(data)
	if err != nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttribute marshal err: %v", err)
		return err
	}
	log.InfoContextf(ctx, "AddAndUpdateAttribute req index:%v, es req:%+v", indexName, string(body))
	esReq := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: cast.ToString(attr.ID),
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}

	res, err := esReq.Do(ctx, es)
	if err != nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttribute error, err: %v", err)
		return err
	}
	if res == nil || res.Body == nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttribute knowClient.Response or body is nil")
		return fmt.Errorf("AddAndUpdateAttribute es knowClient.Response or body is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		log.ErrorContextf(ctx, "AddAndUpdateAttribute Error knowClient.Response|err: %v", res.String())
		return fmt.Errorf("AddAndUpdateAttribute es knowClient.Response status indicates failure")
	}
	log.InfoContextf(ctx, "AddAndUpdateES|success")
	return nil
}

// QueryAttributeMatchPhrase 根据query查询出所有的 Attribute ID,  查询分词匹配
func QueryAttributeMatchPhrase(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	body := genAttributeEsQuery(matchPhrase, robotID, query, size)
	attrIDs, err := queryAttribute(ctx, robotID, matchPhrase, body)
	if err != nil {
		return nil, err
	}
	return attrIDs, nil
}

// QueryAttributeWildcard 根据query查询出所有的 Attribute ID,  正则匹配
func QueryAttributeWildcard(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.App().EsSearch.WildcardTimeTimeoutMs)*time.Second)
	defer cancel()
	body := genAttributeEsQuery(wildcard, robotID, query, size)
	attrIDs, err := queryAttribute(ctx, robotID, wildcard, body)
	if err != nil {
		return nil, err
	}
	return attrIDs, nil
}

// QueryAttribute 根据query查询出所有的 Attribute ID
func queryAttribute(ctx context.Context, robotID uint64, strategy queryStrategy, body map[string]interface{}) ([]uint64, error) {
	t0 := time.Now()
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeIndex)
	if err != nil {
		return nil, err
	}
	esClient, err := knowClient.EsTypedClient()
	if err != nil {
		return nil, err
	}
	log.InfoContextf(ctx, "QueryAttribute %v query:%s", strategy, utils.Any2String(body))
	req := esapi.SearchRequest{
		Index:      []string{indexName},
		Body:       esutil.NewJSONReader(body),
		Preference: "primary_first",
	}
	res, err := req.Do(ctx, esClient)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.WarnContextf(ctx, "%v query timeout", strategy)
			return []uint64{}, nil
		}
		log.ErrorContextf(ctx, "QueryAttribute %v|indexName:%s|Do|err: %+v", strategy, indexName, err)
		return nil, err
	}
	if res == nil || res.Body == nil {
		log.ErrorContextf(ctx, "QueryAttributeLabel %v res is nil", strategy)
		return nil, fmt.Errorf("res is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()
	log.InfoContextf(ctx, "QueryAttribute %v|res:%v", strategy, res.String())
	if res.IsError() {
		log.ErrorContextf(ctx, "QueryAttributeLabel error, res: %v", res.String())
		return nil, fmt.Errorf("query attribute res error")
	}
	var resp knowClient.Response
	if err = json.NewDecoder(res.Body).Decode(&resp); err != nil {
		log.ErrorContextf(ctx, "QueryAttribute|NewDecoder|err:%+v", err)
		return nil, err
	}
	var attrIDs []uint64
	for _, hit := range resp.Hits.Hits {
		sourceBytes, err := json.Marshal(hit.Source)
		if err != nil {
			log.ErrorContextf(ctx, "QueryAttribute|Could not marshal _source to bytes: %v", err)
			return nil, err
		}
		var attr AttributeEs
		if err = json.Unmarshal(sourceBytes, &attr); err != nil {
			log.ErrorContextf(ctx, "QueryAttribute|Could not unmarshal _source bytes %s to Source:%v", sourceBytes, err)
			return nil, err
		}
		if attr.AttrID <= 0 {
			log.ErrorContextf(ctx, "QueryAttribute|attr id %v is illegal, skip", attr.AttrID)
			continue
		}
		attrIDs = append(attrIDs, attr.AttrID)
	}
	log.InfoContextf(ctx, "QueryAttribute %v get attr ids: %+v, cost: %vms", strategy, attrIDs, time.Now().Sub(t0).Milliseconds())
	return attrIDs, nil
}

func genAttributeEsQuery(strategy queryStrategy, robotID uint64, query string, size int) map[string]interface{} {
	robotCond := []map[string]interface{}{
		{
			"term": map[string]interface{}{
				"robot_id": robotID,
			},
		},
	}

	var searchCond []map[string]interface{}
	if strategy == matchPhrase {
		// 查询的时候query不分词
		searchCond = []map[string]interface{}{
			{
				"match_phrase": map[string]interface{}{
					"attribute": map[string]interface{}{
						"query":    query,
						"analyzer": "keyword",
					},
				},
			},
		}
	} else if strategy == wildcard {
		wildcardQuery := "*" + query + "*"
		searchCond = []map[string]interface{}{
			{
				"wildcard": map[string]interface{}{
					"attribute.keyword": map[string]interface{}{
						"value":   wildcardQuery,
						"rewrite": "top_terms_100", // 最多选取前100个结果
					},
				},
			},
		}
	}

	body := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must":                 robotCond,
				"should":               searchCond,
				"minimum_should_match": 1,
			},
		},
		"size": size,
		"sort": []map[string]interface{}{
			{"update_time": map[string]string{"order": "desc"}},
		},
	}
	if strategy == wildcard {
		body["timeout"] = getEsQueryTimeout()
	}
	return body
}

// 公共批量删除函数
func batchDeleteByIDs(ctx context.Context, robotID uint64, ids []uint64, indexType knowClient.IndexType) error {
	log.InfoContextf(ctx, "batchDeleteByIDs robotID: %v, type: %v, ids: %+v", robotID, indexType, ids)
	if len(ids) == 0 {
		return nil
	}

	// 1. 获取索引名称
	indexName, err := knowClient.GetESIndex(ctx, robotID, indexType)
	if err != nil {
		return err
	}

	// 2. 准备批量请求体
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	for _, id := range ids {
		deleteMeta := map[string]interface{}{
			"delete": map[string]string{
				"_index": indexName,
				"_id":    strconv.FormatUint(id, 10),
			},
		}

		if err := encoder.Encode(deleteMeta); err != nil {
			log.ErrorContextf(ctx, "Encode error|id:%d|err:%v", id, err)
			return fmt.Errorf("encode error: %v", err)
		}
	}

	// 3. 执行Bulk请求
	es, err := knowClient.EsTypedClient()
	if err != nil {
		return err
	}

	res, err := esapi.BulkRequest{
		Body:   &buf,
		Pretty: false,
	}.Do(ctx, es)

	if err != nil {
		log.ErrorContextf(ctx, "Bulk request error|err:%v", err)
		return fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()

	// 4. 统一错误处理
	if res.IsError() {
		var errRes map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			log.ErrorContextf(ctx, "Error parsing knowClient.Response|err:%v", err)
			return fmt.Errorf("failed with status: %d", res.StatusCode)
		}
		log.ErrorContextf(ctx, "Operation failed|reason:%v", errRes["error"])
		return fmt.Errorf("error: %v", errRes["error"])
	}

	// 5. 解析响应
	var bulkResp struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Delete struct {
				ID     string `json:"_id"`
				Status int    `json:"status"`
				Error  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error"`
			} `json:"delete"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		log.ErrorContextf(ctx, "Parse knowClient.Response error|err:%v", err)
		return fmt.Errorf("parse error: %v", err)
	}

	// 6. 处理部分失败
	if bulkResp.Errors {
		var errors []string
		for _, item := range bulkResp.Items {
			if item.Delete.Status >= 400 {
				errors = append(errors, fmt.Sprintf("ID:%s %s(%s)",
					item.Delete.ID,
					item.Delete.Error.Type,
					item.Delete.Error.Reason))
			}
		}

		log.ErrorContextf(ctx, "Partial failures|errors:%d/%d|details:%v",
			len(errors), len(ids), strings.Join(errors, "; "))

		return fmt.Errorf("partial failures (%d errors)", len(errors))
	}

	log.InfoContextf(ctx, "success|count:%d", len(ids))
	return nil
}

func BatchDeleteAttributes(ctx context.Context, robotID uint64, attrIDs []uint64) error {
	return batchDeleteByIDs(ctx, robotID, attrIDs, knowClient.AttributeIndex)
}
