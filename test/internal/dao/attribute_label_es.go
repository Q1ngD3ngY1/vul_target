package dao

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type AttributeLabelEs struct {
	AttrLabelID  uint64   `json:"attr_label_id"`
	AttrID       uint64   `json:"attr_id"`
	RobotID      uint64   `json:"robot_id"`
	Label        string   `json:"label"`
	SimilarLabel []string `json:"similar_label"`
	UpdateTime   string   `json:"update_time"`
}

// AddAndUpdateAttributeLabel ES新增或修改AttributeLabel
func AddAndUpdateAttributeLabel(ctx context.Context, robotID, attrID uint64, attributeLabel *model.AttributeLabel) error {
	log.InfoContextf(ctx, "AddAndUpdateAttributeLabel|req: %+v", attributeLabel)
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttributeLabel err: %v", err)
		return err
	}
	es, err := knowClient.EsTypedClient()
	if err != nil {
		return err
	}

	var similarLabel []string
	if attributeLabel.SimilarLabel != "" {
		err = json.Unmarshal([]byte(attributeLabel.SimilarLabel), &similarLabel)
		if err != nil {
			log.ErrorContextf(ctx, "AddAndUpdateAttributeLabel unmarshal SimilarLabel error, %v", err)
			return err
		}
	}

	updateTime := attributeLabel.UpdateTime
	if updateTime.IsZero() {
		updateTime = time.Now()
	}
	data := AttributeLabelEs{
		AttrLabelID:  attributeLabel.ID,
		AttrID:       attrID,
		RobotID:      robotID,
		Label:        attributeLabel.Name,
		SimilarLabel: similarLabel,
		UpdateTime:   updateTime.Format("2006-01-02 15:04:05"),
	}
	body, err := json.Marshal(data)
	if err != nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttributeLabel marshal err: %v", err)
		return err
	}
	log.InfoContextf(ctx, "AddAndUpdateAttributeLabel req index:%v, es req:%+v", indexName, string(body))
	esReq := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: cast.ToString(attributeLabel.ID),
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}

	res, err := esReq.Do(ctx, es)
	if err != nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttributeLabel error, err: %v", err)
		return err
	}
	if res == nil || res.Body == nil {
		log.ErrorContextf(ctx, "AddAndUpdateAttributeLabel response or body is nil")
		return fmt.Errorf("AddAndUpdateAttributeLabel es response or body is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		log.ErrorContextf(ctx, "AddAndUpdateAttributeLabel Error response|err: %v", res.String())
		return fmt.Errorf("AddAndUpdateAttributeLabel es response status indicates failure")
	}
	log.InfoContextf(ctx, "AddAndUpdateES|success")
	return nil
}

// BulkAttributeLabelEs Bulk操作结构体
type BulkAttributeLabelEs struct {
	Index  string           `json:"index"`
	DocID  string           `json:"id"`
	Source AttributeLabelEs `json:"source"`
}

// BatchAddAndUpdateAttributeLabels ES批量新增或修改AttributeLabel
// 如果attrID为0，那么使用labels中的attr
func BatchAddAndUpdateAttributeLabels(ctx context.Context, robotID, attrID uint64, labels []*model.AttributeLabel) error {
	log.InfoContextf(ctx, "BatchAddAndUpdateAttributeLabels|count: %d", len(labels))
	if len(labels) == 0 {
		return nil
	}

	// 1. 准备索引名称
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		log.ErrorContextf(ctx, "GetESIndex error: %v", err)
		return err
	}

	// 2. 转换数据结构
	var bulkRequests []BulkAttributeLabelEs
	for _, label := range labels {
		// 数据验证
		if label.ID == 0 {
			log.WarnContextf(ctx, "Invalid label ID, label: %+v", label)
			return fmt.Errorf("label id is empty")
		}

		// 序列化similar_label
		var similarLabels []string
		if label.SimilarLabel != "" {
			if err := json.Unmarshal([]byte(label.SimilarLabel), &similarLabels); err != nil {
				log.ErrorContextf(ctx, "Unmarshal SimilarLabel error|labelID:%d|err:%v", label.ID, err)
				continue
			}
		}

		// 处理更新时间
		updateTime := label.UpdateTime
		if updateTime.IsZero() {
			updateTime = time.Now()
		}

		// 构造文档结构

		doc := AttributeLabelEs{
			AttrLabelID:  label.ID,
			AttrID:       label.AttrID,
			RobotID:      robotID,
			Label:        label.Name,
			SimilarLabel: similarLabels,
			UpdateTime:   updateTime.Format("2006-01-02 15:04:05"),
		}
		if attrID != 0 {
			doc.AttrID = attrID
		}

		bulkRequests = append(bulkRequests, BulkAttributeLabelEs{
			Index:  indexName,
			DocID:  cast.ToString(label.ID),
			Source: doc,
		})
	}

	// 3. 构造Bulk请求体
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	for _, req := range bulkRequests {
		// 操作元数据
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

		// 文档数据（使用index操作实现upsert）
		if err := encoder.Encode(req.Source); err != nil {
			log.ErrorContextf(ctx, "Encode document error|docID:%s|err:%v", req.DocID, err)
			return err
		}
	}
	log.InfoContextf(ctx, "bulk request body: %v", buf.String())

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
		log.ErrorContextf(ctx, "Bulk request error, err:%v", err)
		return err
	}
	defer res.Body.Close()

	// 5. 处理响应
	if res.IsError() {
		var errRes map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			log.ErrorContextf(ctx, "Error parsing error response|err:%v", err)
			return err
		}
		log.ErrorContextf(ctx, "Bulk operation failed|reason:%v", errRes["error"])
		return fmt.Errorf("bulk operation failed: %v", errRes["error"])
	}

	// 6. 解析详细结果
	var blkResp map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&blkResp); err != nil {
		log.ErrorContextf(ctx, "Parse bulk response error|err:%v", err)
		return err
	}

	// 7. 检查部分失败的情况
	if blkResp["errors"].(bool) {
		var errorItems []string
		for _, item := range blkResp["items"].([]interface{}) {
			op := item.(map[string]interface{})["index"].(map[string]interface{})
			if status := op["status"].(float64); status >= 400 {
				errorItems = append(errorItems, fmt.Sprintf("ID:%s Status:%v Error:%v",
					op["_id"], status, op["error"]))
			}
		}
		log.ErrorContextf(ctx, "Partial bulk failures|errors:%d/%d|details:%v",
			len(errorItems), len(bulkRequests), strings.Join(errorItems, "; "))
		return fmt.Errorf("partial failures: %d errors", len(errorItems))
	}

	log.InfoContextf(ctx, "Bulk operation success|count:%d", len(bulkRequests))
	return nil
}

type QueryLabelAggResponse struct {
	Aggregations struct {
		UniqueAttrIds struct {
			Buckets []struct {
				Key uint64 `json:"key"`
			} `json:"buckets"`
		} `json:"unique_attr_ids"`
	} `json:"aggregations"`
}

// GetAttrIDByQueryLabelMatchPhrase 查询 attribute_label 获取 attribute的ID，查询分词匹配
func GetAttrIDByQueryLabelMatchPhrase(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	body := genGetAttrIDByLabelEsQuery(matchPhrase, robotID, query, size)
	attrIDs, err := queryLabelAggAttrID(ctx, robotID, matchPhrase, body)
	if err != nil {
		return nil, err
	}

	return attrIDs, nil
}

// GetAttrIDByQueryLabelWildcard 查询 attribute_label 获取 attribute的ID, 正则匹配
func GetAttrIDByQueryLabelWildcard(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.App().EsSearch.WildcardTimeTimeoutMs)*time.Second)
	defer cancel()
	body := genGetAttrIDByLabelEsQuery(wildcard, robotID, query, size)
	attrIDs, err := queryLabelAggAttrID(ctx, robotID, wildcard, body)
	if err != nil {
		return nil, err
	}

	return attrIDs, nil
}

// queryLabelAggAttrID 查询 attribute_label 获取 attribute的ID
func queryLabelAggAttrID(ctx context.Context, robotID uint64, strategy queryStrategy,
	body map[string]interface{}) ([]uint64, error) {
	t0 := time.Now()
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		return nil, err
	}
	esClient, err := knowClient.EsTypedClient()
	if err != nil {
		return nil, err
	}
	log.InfoContextf(ctx, "QueryAttributeLabel %v query:%s", strategy, utils.Any2String(body))
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
		log.ErrorContextf(ctx, "QueryAttributeLabel %v|indexName:%s|Do|err: %+v", strategy, indexName, err)
		return nil, err
	}
	if res == nil || res.Body == nil {
		log.ErrorContextf(ctx, "QueryAttributeLabel res is nil")
		return nil, fmt.Errorf("res is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()
	log.InfoContextf(ctx, "QueryAttributeLabel %v|res:%v", strategy, res.String())
	if res.IsError() {
		log.ErrorContextf(ctx, "QueryAttributeLabel %v error, res: %v", strategy, res.String())
		return nil, fmt.Errorf(" %v query attribute label res error", strategy)
	}

	var resp QueryLabelAggResponse
	if err = json.NewDecoder(res.Body).Decode(&resp); err != nil {
		log.ErrorContextf(ctx, "QueryAttributeLabel %v|NewDecoder|err:%+v", strategy, err)
		return nil, err
	}
	var attrIDs []uint64
	for _, bucket := range resp.Aggregations.UniqueAttrIds.Buckets {
		if bucket.Key <= 0 {
			log.ErrorContextf(ctx, "QueryAttributeLabel %v |attr id %v is illegal, skip", strategy, bucket.Key)
			continue
		}
		attrIDs = append(attrIDs, bucket.Key)
	}
	log.InfoContextf(ctx, "QueryAttributeLabel %v get attr ids: %+v, cost: %vms",
		strategy, attrIDs, time.Now().Sub(t0).Milliseconds())

	return attrIDs, nil
}

type queryStrategy string

const (
	matchPhrase queryStrategy = "match_phrase"
	wildcard    queryStrategy = "wildcard"
)

func genGetAttrIDByLabelEsQuery(strategy queryStrategy, robotID uint64, query string, size int) map[string]interface{} {
	robotCond := []map[string]interface{}{
		{"term": map[string]interface{}{"robot_id": robotID}},
	}

	var searchCond []map[string]interface{}
	if strategy == matchPhrase {
		// 查询的时候query不分词
		searchCond = []map[string]interface{}{
			{
				"match_phrase": map[string]interface{}{
					"label": map[string]interface{}{
						"query":    query,
						"analyzer": "keyword",
					},
				},
			},
			{
				"match_phrase": map[string]interface{}{
					"similar_label": map[string]interface{}{
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
					"label.keyword": map[string]interface{}{
						"value":   wildcardQuery,
						"rewrite": "top_terms_100", // 最多选取前100个结果
					},
				},
			},
			{
				"wildcard": map[string]interface{}{
					"similar_label.keyword": map[string]interface{}{
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
		"size": 0, // 不返回原始文档
		"aggs": map[string]interface{}{
			"unique_attr_ids": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "attr_id",
					"size":  size, // 控制返回的去重ID数量
					"order": map[string]interface{}{"latest_update": "desc"},
				},
				"aggs": map[string]interface{}{
					"latest_update": map[string]interface{}{
						"max": map[string]interface{}{
							"field": "update_time",
						},
					},
				},
			},
		},
	}

	if strategy == wildcard {
		body["timeout"] = getEsQueryTimeout()
	}
	return body
}

func getEsQueryTimeout() string {
	ms := config.App().EsSearch.WildcardTimeTimeoutMs - 200
	if ms <= 0 {
		ms = 500
	}
	return fmt.Sprintf("%dms", ms)
}

func IntPtr(i int) *int {
	return &i
}

func BoolPtr(b bool) *bool {
	return &b
}

// BatchDeleteAttributeLabelByAttrIDs 根据AttrID字段值批量删除
func BatchDeleteAttributeLabelByAttrIDs(ctx context.Context, robotID uint64, attrIDs []uint64) error {
	log.InfoContextf(ctx, "BatchDeleteByAttrIDs|count: %d", len(attrIDs))
	if len(attrIDs) == 0 {
		return nil
	}

	// 1. 获取索引名称
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		log.ErrorContextf(ctx, "GetESIndex error: %v", err)
		return err
	}

	// 2. 构建Delete By Query请求
	es, err := knowClient.EsTypedClient()
	if err != nil {
		return err
	}

	// 3. 构造terms查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"attr_id": attrIDs,
			},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		log.InfoContextf(ctx, "marshal query error: %v", err)
		return err
	}
	log.InfoContextf(ctx, "DeleteByQuery request body: %s", body)

	// 4. 执行Delete By Query
	res, err := esapi.DeleteByQueryRequest{
		Index:             []string{indexName},
		Body:              bytes.NewReader(body),
		Conflicts:         "proceed",     // 遇到版本冲突继续执行
		Refresh:           BoolPtr(true), // 操作后刷新索引
		ScrollSize:        IntPtr(5000),  // 每次滚动处理数量
		RequestsPerSecond: IntPtr(500),   // 限流
	}.Do(ctx, es)

	if err != nil {
		log.ErrorContextf(ctx, "DeleteByQuery request error: %v", err)
		return err
	}
	defer res.Body.Close()

	// 5. 处理响应
	if res.IsError() {
		var errRes map[string]interface{}
		err = json.NewDecoder(res.Body).Decode(&errRes)
		if err != nil {
			log.ErrorContextf(ctx, "DeleteByQuery Decode failed|reason: %v", err)
			return fmt.Errorf("delete by query error")
		}
		log.ErrorContextf(ctx, "DeleteByQuery failed|reason: %v", errRes["error"])
		return fmt.Errorf("delete by query error: %v", errRes["error"])
	}

	// 6. 解析详细结果
	var result struct {
		Deleted          int64         `json:"deleted"`
		Batches          int           `json:"batches"`
		VersionConflicts int           `json:"version_conflicts"`
		Failures         []interface{} `json:"failures"`
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		log.ErrorContextf(ctx, "Parse response error: %v", err)
		return err
	}

	// 7. 检查部分失败
	if len(result.Failures) > 0 {
		log.ErrorContextf(ctx, "Partial failures|total:%d deleted:%d failures:%d conflicts:%d",
			len(attrIDs), result.Deleted, len(result.Failures), result.VersionConflicts)
		return fmt.Errorf("partial delete failures (%d docs)", len(result.Failures))
	}

	log.InfoContextf(ctx, "DeleteByAttrIDs success|deleted:%d", result.Deleted)
	return nil
}

func BatchDeleteAttributeLabelsByIDs(ctx context.Context, robotID uint64, attrLabelIDs []uint64) error {
	return batchDeleteByIDs(ctx, robotID, attrLabelIDs, knowClient.AttributeLabelIndex)
}

// AttributeLabelES 结构体映射ES文档结构
type AttributeLabelES struct {
	AttrLabelID uint64 `json:"attr_label_id"`
}

// QueryAttributeLabelCursorMatchPhrase 按照游标使用分词匹配查询attribute label es，获取attr_label_id列表
func QueryAttributeLabelCursorMatchPhrase(ctx context.Context, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	return queryAttributeLabel(ctx, matchPhrase, attrID, query, queryScope, lastLabelID, limit, robotID)
}

// QueryAttributeLabelCursorWildcard 按照游标使用通配符查询attribute label es，获取attr_label_id列表
func QueryAttributeLabelCursorWildcard(ctx context.Context, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	return queryAttributeLabel(ctx, wildcard, attrID, query, queryScope, lastLabelID, limit, robotID)
}

// 公共查询函数
func queryAttributeLabel(ctx context.Context, strategy queryStrategy, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	t0 := time.Now()
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		return nil, err
	}
	esClient, err := knowClient.EsTypedClient()
	if err != nil {
		return nil, err
	}

	body := genAttributeLabelEsQuery(strategy, attrID, query, queryScope, lastLabelID, limit, robotID)

	log.InfoContextf(ctx, "queryAttributeLabel %v query: %s", strategy, utils.Any2String(body))

	// 设置超时（仅wildcard）
	if strategy == wildcard {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(config.App().EsSearch.WildcardTimeTimeoutMs)*time.Second)
		defer cancel()
	}

	req := esapi.SearchRequest{
		Index:      []string{indexName},
		Body:       esutil.NewJSONReader(body),
		Preference: "primary_first",
	}

	res, err := req.Do(ctx, esClient)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.InfoContextf(ctx, "%v query timeout", strategy)
			return []uint64{}, nil
		}
		log.ErrorContextf(ctx, "%v esrequest error: %v", strategy, err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.ErrorContextf(ctx, "queryAttributeLabel %v error, res: %v", strategy, res.String())
		return nil, fmt.Errorf("ES error: %s", res.Status())
	}

	var resp struct {
		Hits struct {
			Hits []struct {
				Source AttributeLabelES `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		log.InfoContextf(ctx, "Error decoding response: %v", err)
		return nil, err
	}

	attrLabelIDs := make([]uint64, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		if hit.Source.AttrLabelID > 0 {
			attrLabelIDs = append(attrLabelIDs, hit.Source.AttrLabelID)
		}
	}
	log.InfoContextf(ctx, "queryAttributeLabel get ids %+v, cost:%vms", attrLabelIDs, time.Now().Sub(t0).Milliseconds())
	return attrLabelIDs, nil
}

// 生成ES查询体
func genAttributeLabelEsQuery(strategy queryStrategy, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) map[string]interface{} {

	must := []map[string]interface{}{
		{"term": map[string]interface{}{"attr_id": attrID}},
		{"term": map[string]interface{}{"robot_id": robotID}},
	}

	if lastLabelID > 0 {
		must = append(must, map[string]interface{}{
			"range": map[string]interface{}{
				"attr_label_id": map[string]interface{}{
					"lt": lastLabelID,
				},
			},
		})
	}

	// 确定查询字段
	var fields []string
	switch queryScope {
	case "standard":
		fields = []string{"label"}
	case "similar":
		fields = []string{"similar_label"}
	default:
		fields = []string{"label", "similar_label"}
	}

	should := make([]map[string]interface{}, 0)

	for _, field := range fields {
		var cond map[string]interface{}
		switch strategy {
		case matchPhrase:
			cond = map[string]interface{}{
				"match_phrase": map[string]interface{}{
					field: map[string]interface{}{
						"query":    query,
						"analyzer": "keyword", // 使用keyword分析器，确保不分词
					},
				},
			}
		case wildcard:
			wildcardValue := "*" + query + "*"
			cond = map[string]interface{}{
				"wildcard": map[string]interface{}{
					field + ".keyword": map[string]interface{}{
						"value":   wildcardValue,
						"rewrite": "top_terms_100",
					},
				},
			}
		}
		should = append(should, cond)
	}

	boolQuery := map[string]interface{}{
		"must":                 must,
		"should":               should,
		"minimum_should_match": 1,
	}

	body := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": boolQuery,
		},
		"size": limit,
		"sort": []map[string]interface{}{
			{"attr_label_id": map[string]string{"order": "desc"}},
		},
	}

	if strategy == wildcard {
		body["timeout"] = getEsQueryTimeout()
	}

	return body
}
