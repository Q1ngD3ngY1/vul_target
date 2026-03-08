package label

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
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

type AttributeLabelEs struct {
	AttrLabelID  uint64   `json:"attr_label_id"`
	AttrID       uint64   `json:"attr_id"`
	RobotID      uint64   `json:"robot_id"`
	Label        string   `json:"label"`
	SimilarLabel []string `json:"similar_label"`
	UpdateTime   string   `json:"update_time"`
}

// BulkAttributeLabelEs Bulk操作结构体
type BulkAttributeLabelEs struct {
	Index  string           `json:"index"`
	DocID  string           `json:"id"`
	Source AttributeLabelEs `json:"source"`
}

type queryStrategy string

const (
	matchPhrase queryStrategy = "match_phrase"
	wildcard    queryStrategy = "wildcard"
)

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

func (d *daoImpl) BatchAddAndUpdateAttributes(ctx context.Context, robotID uint64, attributes []*entity.Attribute) error {
	logx.I(ctx, "BatchAddAndUpdateAttributes|count: %d", len(attributes))
	if len(attributes) == 0 {
		return nil
	}
	// 1. 准备索引名称
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeIndex)
	if err != nil {
		logx.E(ctx, "GetESIndex error: %v", err)
		return err
	}
	// 2. 转换数据结构
	var bulkRequests []BulkAttributeEs
	for _, attr := range attributes {
		if attr.ID == 0 {
			logx.E(ctx, "Invalid attribute ID, attribute: %+v", attr)
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
		meta := map[string]any{
			"index": map[string]string{
				"_index": req.Index,
				"_id":    req.DocID,
			},
		}
		if err := encoder.Encode(meta); err != nil {
			logx.E(ctx, "Encode metadata error|docID:%s|err:%v", req.DocID, err)
			return err
		}
		// 文档数据
		if err := encoder.Encode(req.Source); err != nil {
			logx.E(ctx, "Encode document error|docID:%s|err:%v", req.DocID, err)
			return err
		}
	}
	logx.I(ctx, "Prepared bulk request with %d operations", len(bulkRequests))
	// 4. 执行Bulk请求
	bulkReq := esapi.BulkRequest{
		Body:   &buf,
		Pretty: false,
		Human:  false,
	}
	res, err := bulkReq.Do(ctx, d.esClient)
	if err != nil {
		logx.E(ctx, "Bulk request error|err:%v", err)
		return err
	}
	defer res.Body.Close()
	// 5. 处理响应错误
	if res.IsError() {
		var errRes map[string]any
		if err = json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			logx.E(ctx, "Error parsing error response|err:%v", err)
			return err
		}
		logx.E(ctx, "Bulk operation failed|reason:%v", errRes["error"])
		return fmt.Errorf("bulk operation failed: %v", errRes["error"])
	}
	// 6. 解析响应检查部分失败
	var blkResp map[string]any
	if err = json.NewDecoder(res.Body).Decode(&blkResp); err != nil {
		logx.E(ctx, "Parse bulk response error|err:%v", err)
		return err
	}
	// 7. 检查部分失败的情况
	if blkResp["errors"].(bool) {
		errorItems := make([]string, 0)
		for _, item := range blkResp["items"].([]any) {
			op := item.(map[string]any)["index"].(map[string]any)
			if status, ok := op["status"].(float64); ok && status >= 400 {
				errorItems = append(errorItems, fmt.Sprintf("ID:%s Status:%v Error:%v",
					op["_id"], status, op["error"]))
			}
		}
		if len(errorItems) > 0 {
			logx.E(ctx, "Partial bulk failures|errors:%d/%d|details:%v",
				len(errorItems), len(bulkRequests), strings.Join(errorItems, "; "))
			return fmt.Errorf("partial failures: %d errors", len(errorItems))
		}
	}
	logx.I(ctx, "Bulk operation success|count:%d", len(bulkRequests))
	return nil
}

// AddAndUpdateAttribute ES新增或修改Attribute
func (d *daoImpl) AddAndUpdateAttribute(ctx context.Context, robotID uint64, attr *entity.Attribute) error {
	logx.I(ctx, "AddAndUpdateAttribute|req: %+v", attr)
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeIndex)
	if err != nil {
		logx.E(ctx, "AddAndUpdateAttribute err: %v", err)
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
		logx.E(ctx, "AddAndUpdateAttribute marshal err: %v", err)
		return err
	}
	logx.I(ctx, "AddAndUpdateAttribute req index:%v, es req:%+v", indexName, string(body))
	esReq := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: cast.ToString(attr.ID),
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}
	res, err := esReq.Do(ctx, d.esClient)
	if err != nil {
		logx.E(ctx, "AddAndUpdateAttribute error, err: %v", err)
		return err
	}
	if res == nil || res.Body == nil {
		logx.E(ctx, "AddAndUpdateAttribute knowClient.Response or body is nil")
		return fmt.Errorf("AddAndUpdateAttribute es knowClient.Response or body is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.IsError() {
		logx.E(ctx, "AddAndUpdateAttribute Error knowClient.Response|err: %v", res.String())
		return fmt.Errorf("AddAndUpdateAttribute es knowClient.Response status indicates failure")
	}
	logx.I(ctx, "AddAndUpdateES|success")
	return nil
}

func (d *daoImpl) QueryAttributeMatchPhrase(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	body := genAttributeEsQuery(matchPhrase, robotID, query, size)
	attrIDs, err := d.queryAttribute(ctx, robotID, matchPhrase, body)
	if err != nil {
		return nil, err
	}
	return attrIDs, nil
}

func genAttributeEsQuery(strategy queryStrategy, robotID uint64, query string, size int) map[string]any {
	robotCond := []map[string]any{
		{
			"term": map[string]any{
				"robot_id": robotID,
			},
		},
	}
	var searchCond []map[string]any
	if strategy == matchPhrase {
		// 查询的时候query不分词
		searchCond = []map[string]any{
			{
				"match_phrase": map[string]any{
					"attribute": map[string]any{
						"query":    query,
						"analyzer": "keyword",
					},
				},
			},
		}
	} else if strategy == wildcard {
		wildcardQuery := "*" + query + "*"
		searchCond = []map[string]any{
			{
				"wildcard": map[string]any{
					"attribute.keyword": map[string]any{
						"value":   wildcardQuery,
						"rewrite": "top_terms_100", // 最多选取前100个结果
					},
				},
			},
		}
	}

	body := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter":               robotCond,
				"should":               searchCond,
				"minimum_should_match": 1,
			},
		},
		"size": size,
		"sort": []map[string]any{
			{"update_time": map[string]string{"order": "desc"}},
		},
	}
	if strategy == wildcard {
		body["timeout"] = getEsQueryTimeout()
	}
	return body
}

// QueryAttribute 根据query查询出所有的 Attribute ID
func (d *daoImpl) queryAttribute(ctx context.Context, robotID uint64, strategy queryStrategy, body map[string]any) ([]uint64, error) {
	t0 := time.Now()
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeIndex)
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "QueryAttribute %v query:%s", strategy, jsonx.MustMarshalToString(body))
	req := esapi.SearchRequest{
		Index:      []string{indexName},
		Body:       esutil.NewJSONReader(body),
		Preference: "primary_first",
	}
	res, err := req.Do(ctx, d.esClient)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			logx.W(ctx, "%v query timeout", strategy)
			return []uint64{}, nil
		}
		logx.E(ctx, "QueryAttribute %v|indexName:%s|Do|err: %+v", strategy, indexName, err)
		return nil, err
	}
	if res == nil || res.Body == nil {
		logx.E(ctx, "QueryAttributeLabel %v res is nil", strategy)
		return nil, fmt.Errorf("res is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()
	logx.I(ctx, "QueryAttribute %v|res:%v", strategy, res.String())
	if res.IsError() {
		logx.E(ctx, "QueryAttributeLabel error, res: %v", res.String())
		return nil, fmt.Errorf("query attribute res error")
	}
	var resp knowClient.Response
	if err = json.NewDecoder(res.Body).Decode(&resp); err != nil {
		logx.E(ctx, "QueryAttribute|NewDecoder|err:%+v", err)
		return nil, err
	}
	var attrIDs []uint64
	for _, hit := range resp.Hits.Hits {
		sourceBytes, err := json.Marshal(hit.Source)
		if err != nil {
			logx.E(ctx, "QueryAttribute|Could not marshal _source to bytes: %v", err)
			return nil, err
		}
		var attr AttributeEs
		if err = json.Unmarshal(sourceBytes, &attr); err != nil {
			logx.E(ctx, "QueryAttribute|Could not unmarshal _source bytes %s to Source:%v", sourceBytes, err)
			return nil, err
		}
		if attr.AttrID <= 0 {
			logx.E(ctx, "QueryAttribute|attr id %v is illegal, skip", attr.AttrID)
			continue
		}
		attrIDs = append(attrIDs, attr.AttrID)
	}
	logx.I(ctx, "QueryAttribute %v get attr ids: %+v, cost: %vms", strategy, attrIDs, time.Now().Sub(t0).Milliseconds())
	return attrIDs, nil
}

// QueryAttributeWildcard 根据query查询出所有的 Attribute ID,  正则匹配
func (d *daoImpl) QueryAttributeWildcard(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.App().EsSearch.WildcardTimeTimeoutMs)*time.Second)
	defer cancel()
	body := genAttributeEsQuery(wildcard, robotID, query, size)
	attrIDs, err := d.queryAttribute(ctx, robotID, wildcard, body)
	if err != nil {
		return nil, err
	}
	return attrIDs, nil
}

func (d *daoImpl) BatchDeleteAttributes(ctx context.Context, robotID uint64, attrIDs []uint64) error {
	return d.batchDeleteByIDs(ctx, robotID, attrIDs, knowClient.AttributeIndex)
}

// 公共批量删除函数
func (d *daoImpl) batchDeleteByIDs(ctx context.Context, robotID uint64, ids []uint64, indexType knowClient.IndexType) error {
	logx.I(ctx, "batchDeleteByIDs robotID: %v, type: %v, ids: %+v", robotID, indexType, ids)
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
		deleteMeta := map[string]any{
			"delete": map[string]string{
				"_index": indexName,
				"_id":    strconv.FormatUint(id, 10),
			},
		}
		if err := encoder.Encode(deleteMeta); err != nil {
			logx.E(ctx, "Encode error|id:%d|err:%v", id, err)
			return fmt.Errorf("encode error: %v", err)
		}
	}
	// 3. 执行Bulk请求
	res, err := esapi.BulkRequest{
		Body:   &buf,
		Pretty: false,
	}.Do(ctx, d.esClient)
	if err != nil {
		logx.E(ctx, "Bulk request error|err:%v", err)
		return fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()
	// 4. 统一错误处理
	if res.IsError() {
		var errRes map[string]any
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			logx.E(ctx, "Error parsing knowClient.Response|err:%v", err)
			return fmt.Errorf("failed with status: %d", res.StatusCode)
		}
		logx.E(ctx, "Operation failed|reason:%v", errRes["error"])
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
		logx.E(ctx, "Parse knowClient.Response error|err:%v", err)
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

		logx.E(ctx, "Partial failures|errors:%d/%d|details:%v",
			len(errors), len(ids), strings.Join(errors, "; "))

		return fmt.Errorf("partial failures (%d errors)", len(errors))
	}
	logx.I(ctx, "success|count:%d", len(ids))
	return nil
}

// BatchAddAndUpdateAttributeLabels ES批量新增或修改AttributeLabel
// 如果attrID为0，那么使用labels中的attr
func (d *daoImpl) BatchAddAndUpdateAttributeLabels(ctx context.Context, robotID, attrID uint64, labels []*entity.AttributeLabel) error {
	logx.I(ctx, "BatchAddAndUpdateAttributeLabels|count: %d", len(labels))
	if len(labels) == 0 {
		return nil
	}
	// 1. 准备索引名称
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		logx.E(ctx, "GetESIndex error: %v", err)
		return err
	}
	// 2. 转换数据结构
	var bulkRequests []BulkAttributeLabelEs
	for _, label := range labels {
		// 数据验证
		if label.ID == 0 {
			logx.W(ctx, "Invalid label ID, label: %+v", label)
			return fmt.Errorf("label id is empty")
		}
		// 序列化similar_label
		var similarLabels []string
		if label.SimilarLabel != "" {
			if err := json.Unmarshal([]byte(label.SimilarLabel), &similarLabels); err != nil {
				logx.E(ctx, "Unmarshal SimilarLabel error|labelID:%d|err:%v", label.ID, err)
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
		meta := map[string]any{
			"index": map[string]string{
				"_index": req.Index,
				"_id":    req.DocID,
			},
		}
		if err := encoder.Encode(meta); err != nil {
			logx.E(ctx, "Encode metadata error|docID:%s|err:%v", req.DocID, err)
			return err
		}

		// 文档数据（使用index操作实现upsert）
		if err := encoder.Encode(req.Source); err != nil {
			logx.E(ctx, "Encode document error|docID:%s|err:%v", req.DocID, err)
			return err
		}
	}
	logx.I(ctx, "bulk request body: %v", buf.String())

	// 4. 执行Bulk请求
	bulkReq := esapi.BulkRequest{
		Body:   &buf,
		Pretty: false,
		Human:  false,
	}
	res, err := bulkReq.Do(ctx, d.esClient)
	if err != nil {
		logx.E(ctx, "Bulk request error, err:%v", err)
		return err
	}
	defer res.Body.Close()
	// 5. 处理响应
	if res.IsError() {
		var errRes map[string]any
		if err = json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			logx.E(ctx, "Error parsing error response|err:%v", err)
			return err
		}
		logx.E(ctx, "Bulk operation failed|reason:%v", errRes["error"])
		return fmt.Errorf("bulk operation failed: %v", errRes["error"])
	}

	// 6. 解析详细结果
	var blkResp map[string]any
	if err = json.NewDecoder(res.Body).Decode(&blkResp); err != nil {
		logx.E(ctx, "Parse bulk response error|err:%v", err)
		return err
	}

	// 7. 检查部分失败的情况
	if blkResp["errors"].(bool) {
		var errorItems []string
		for _, item := range blkResp["items"].([]any) {
			op := item.(map[string]any)["index"].(map[string]any)
			if status := op["status"].(float64); status >= 400 {
				errorItems = append(errorItems, fmt.Sprintf("ID:%s Status:%v Error:%v",
					op["_id"], status, op["error"]))
			}
		}
		logx.E(ctx, "Partial bulk failures|errors:%d/%d|details:%v",
			len(errorItems), len(bulkRequests), strings.Join(errorItems, "; "))
		return fmt.Errorf("partial failures: %d errors", len(errorItems))
	}

	logx.I(ctx, "Bulk operation success|count:%d", len(bulkRequests))
	return nil
}

// GetAttrIDByQueryLabelMatchPhrase 查询 attribute_label 获取 attribute的ID，查询分词匹配
func (d *daoImpl) GetAttrIDByQueryLabelMatchPhrase(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	body := genGetAttrIDByLabelEsQuery(matchPhrase, robotID, query, size)
	attrIDs, err := d.queryLabelAggAttrID(ctx, robotID, matchPhrase, body)
	if err != nil {
		return nil, err
	}

	return attrIDs, nil
}

func genGetAttrIDByLabelEsQuery(strategy queryStrategy, robotID uint64, query string, size int) map[string]any {
	robotCond := []map[string]any{
		{"term": map[string]any{"robot_id": robotID}},
	}

	var searchCond []map[string]any
	if strategy == matchPhrase {
		// 查询的时候query不分词
		searchCond = []map[string]any{
			{
				"match_phrase": map[string]any{
					"label": map[string]any{
						"query":    query,
						"analyzer": "keyword",
					},
				},
			},
			{
				"match_phrase": map[string]any{
					"similar_label": map[string]any{
						"query":    query,
						"analyzer": "keyword",
					},
				},
			},
		}
	} else if strategy == wildcard {
		wildcardQuery := "*" + query + "*"
		searchCond = []map[string]any{
			{
				"wildcard": map[string]any{
					"label.keyword": map[string]any{
						"value":   wildcardQuery,
						"rewrite": "top_terms_100", // 最多选取前100个结果
					},
				},
			},
			{
				"wildcard": map[string]any{
					"similar_label.keyword": map[string]any{
						"value":   wildcardQuery,
						"rewrite": "top_terms_100", // 最多选取前100个结果
					},
				},
			},
		}
	}

	body := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter":               robotCond,
				"should":               searchCond,
				"minimum_should_match": 1,
			},
		},
		"size": 0, // 不返回原始文档
		"aggs": map[string]any{
			"unique_attr_ids": map[string]any{
				"terms": map[string]any{
					"field": "attr_id",
					"size":  size, // 控制返回的去重ID数量
					"order": map[string]any{"latest_update": "desc"},
				},
				"aggs": map[string]any{
					"latest_update": map[string]any{
						"max": map[string]any{
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

type QueryLabelAggResponse struct {
	Aggregations struct {
		UniqueAttrIds struct {
			Buckets []struct {
				Key uint64 `json:"key"`
			} `json:"buckets"`
		} `json:"unique_attr_ids"`
	} `json:"aggregations"`
}

// queryLabelAggAttrID 查询 attribute_label 获取 attribute的ID
func (d *daoImpl) queryLabelAggAttrID(ctx context.Context, robotID uint64, strategy queryStrategy,
	body map[string]any) ([]uint64, error) {
	t0 := time.Now()
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "QueryAttributeLabel %v query:%s", strategy, jsonx.MustMarshalToString(body))
	req := esapi.SearchRequest{
		Index:      []string{indexName},
		Body:       esutil.NewJSONReader(body),
		Preference: "primary_first",
	}
	res, err := req.Do(ctx, d.esClient)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			logx.W(ctx, "%v query timeout", strategy)
			return []uint64{}, nil
		}
		logx.E(ctx, "QueryAttributeLabel %v|indexName:%s|Do|err: %+v", strategy, indexName, err)
		return nil, err
	}
	if res == nil || res.Body == nil {
		logx.E(ctx, "QueryAttributeLabel res is nil")
		return nil, fmt.Errorf("res is nil")
	}
	defer func() {
		_ = res.Body.Close()
	}()
	logx.I(ctx, "QueryAttributeLabel %v|res:%v", strategy, res.String())
	if res.IsError() {
		logx.E(ctx, "QueryAttributeLabel %v error, res: %v", strategy, res.String())
		return nil, fmt.Errorf(" %v query attribute label res error", strategy)
	}

	var resp QueryLabelAggResponse
	if err = json.NewDecoder(res.Body).Decode(&resp); err != nil {
		logx.E(ctx, "QueryAttributeLabel %v|NewDecoder|err:%+v", strategy, err)
		return nil, err
	}
	var attrIDs []uint64
	for _, bucket := range resp.Aggregations.UniqueAttrIds.Buckets {
		if bucket.Key <= 0 {
			logx.E(ctx, "QueryAttributeLabel %v |attr id %v is illegal, skip", strategy, bucket.Key)
			continue
		}
		attrIDs = append(attrIDs, bucket.Key)
	}
	logx.I(ctx, "QueryAttributeLabel %v get attr ids: %+v, cost: %vms",
		strategy, attrIDs, time.Now().Sub(t0).Milliseconds())

	return attrIDs, nil
}

// GetAttrIDByQueryLabelWildcard 查询 attribute_label 获取 attribute的ID, 正则匹配
func (d *daoImpl) GetAttrIDByQueryLabelWildcard(ctx context.Context, robotID uint64, query string, size int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.App().EsSearch.WildcardTimeTimeoutMs)*time.Second)
	defer cancel()
	body := genGetAttrIDByLabelEsQuery(wildcard, robotID, query, size)
	attrIDs, err := d.queryLabelAggAttrID(ctx, robotID, wildcard, body)
	if err != nil {
		return nil, err
	}

	return attrIDs, nil
}

// BatchDeleteAttributeLabelByAttrIDs 根据AttrID字段值批量删除
func (d *daoImpl) BatchDeleteAttributeLabelByAttrIDs(ctx context.Context, robotID uint64, attrIDs []uint64) error {
	logx.I(ctx, "BatchDeleteByAttrIDs|count: %d", len(attrIDs))
	if len(attrIDs) == 0 {
		return nil
	}
	// 1. 获取索引名称
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		logx.E(ctx, "GetESIndex error: %v", err)
		return err
	}
	// 2. 构建Delete By Query请求
	// 3. 构造terms查询
	query := map[string]any{
		"query": map[string]any{
			"terms": map[string]any{
				"attr_id": attrIDs,
			},
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		logx.I(ctx, "marshal query error: %v", err)
		return err
	}
	logx.I(ctx, "DeleteByQuery request body: %s", body)
	// 4. 执行Delete By Query
	res, err := esapi.DeleteByQueryRequest{
		Index:             []string{indexName},
		Body:              bytes.NewReader(body),
		Conflicts:         "proceed",     // 遇到版本冲突继续执行
		Refresh:           BoolPtr(true), // 操作后刷新索引
		ScrollSize:        IntPtr(5000),  // 每次滚动处理数量
		RequestsPerSecond: IntPtr(500),   // 限流
	}.Do(ctx, d.esClient)

	if err != nil {
		logx.E(ctx, "DeleteByQuery request error: %v", err)
		return err
	}
	defer res.Body.Close()
	// 5. 处理响应
	if res.IsError() {
		var errRes map[string]any
		err = json.NewDecoder(res.Body).Decode(&errRes)
		if err != nil {
			logx.E(ctx, "DeleteByQuery Decode failed|reason: %v", err)
			return fmt.Errorf("delete by query error")
		}
		logx.E(ctx, "DeleteByQuery failed|reason: %v", errRes["error"])
		return fmt.Errorf("delete by query error: %v", errRes["error"])
	}

	// 6. 解析详细结果
	var result struct {
		Deleted          int64 `json:"deleted"`
		Batches          int   `json:"batches"`
		VersionConflicts int   `json:"version_conflicts"`
		Failures         []any `json:"failures"`
	}
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		logx.E(ctx, "Parse response error: %v", err)
		return err
	}
	// 7. 检查部分失败
	if len(result.Failures) > 0 {
		logx.E(ctx, "Partial failures|total:%d deleted:%d failures:%d conflicts:%d",
			len(attrIDs), result.Deleted, len(result.Failures), result.VersionConflicts)
		return fmt.Errorf("partial delete failures (%d docs)", len(result.Failures))
	}
	logx.I(ctx, "DeleteByAttrIDs success|deleted:%d", result.Deleted)
	return nil
}

func (d *daoImpl) BatchDeleteAttributeLabelsByIDs(ctx context.Context, robotID uint64, attrLabelIDs []uint64) error {
	return d.batchDeleteByIDs(ctx, robotID, attrLabelIDs, knowClient.AttributeLabelIndex)
}

// QueryAttributeLabelCursorMatchPhrase 按照游标使用分词匹配查询attribute label es，获取attr_label_id列表
func (d *daoImpl) QueryAttributeLabelCursorMatchPhrase(ctx context.Context, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	return d.queryAttributeLabel(ctx, matchPhrase, attrID, query, queryScope, lastLabelID, limit, robotID)
}

// QueryAttributeLabelCursorWildcard 按照游标使用通配符查询attribute label es，获取attr_label_id列表
func (d *daoImpl) QueryAttributeLabelCursorWildcard(ctx context.Context, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	return d.queryAttributeLabel(ctx, wildcard, attrID, query, queryScope, lastLabelID, limit, robotID)
}

// AttributeLabelES 结构体映射ES文档结构
type AttributeLabelES struct {
	AttrLabelID uint64 `json:"attr_label_id"`
}

// 公共查询函数
func (d *daoImpl) queryAttributeLabel(ctx context.Context, strategy queryStrategy, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	t0 := time.Now()
	indexName, err := knowClient.GetESIndex(ctx, robotID, knowClient.AttributeLabelIndex)
	if err != nil {
		return nil, err
	}
	body := genAttributeLabelEsQuery(strategy, attrID, query, queryScope, lastLabelID, limit, robotID)
	logx.I(ctx, "queryAttributeLabel %v query: %s", strategy, jsonx.MustMarshalToString(body))
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
	res, err := req.Do(ctx, d.esClient)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			logx.I(ctx, "%v query timeout", strategy)
			return []uint64{}, nil
		}
		logx.E(ctx, "%v esrequest error: %v", strategy, err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		logx.E(ctx, "queryAttributeLabel %v error, res: %v", strategy, res.String())
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
		logx.I(ctx, "Error decoding response: %v", err)
		return nil, err
	}

	attrLabelIDs := make([]uint64, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		if hit.Source.AttrLabelID > 0 {
			attrLabelIDs = append(attrLabelIDs, hit.Source.AttrLabelID)
		}
	}
	logx.I(ctx, "queryAttributeLabel get ids %+v, cost:%vms", attrLabelIDs, time.Now().Sub(t0).Milliseconds())
	return attrLabelIDs, nil
}

// 生成ES查询体
func genAttributeLabelEsQuery(strategy queryStrategy, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) map[string]any {
	filter := []map[string]any{
		{"term": map[string]any{"attr_id": attrID}},
		{"term": map[string]any{"robot_id": robotID}},
	}
	if lastLabelID > 0 {
		filter = append(filter, map[string]any{
			"range": map[string]any{
				"attr_label_id": map[string]any{
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

	should := make([]map[string]any, 0)

	for _, field := range fields {
		var cond map[string]any
		switch strategy {
		case matchPhrase:
			cond = map[string]any{
				"match_phrase": map[string]any{
					field: map[string]any{
						"query":    query,
						"analyzer": "keyword", // 使用keyword分析器，确保不分词
					},
				},
			}
		case wildcard:
			wildcardValue := "*" + query + "*"
			cond = map[string]any{
				"wildcard": map[string]any{
					field + ".keyword": map[string]any{
						"value":   wildcardValue,
						"rewrite": "top_terms_100",
					},
				},
			}
		}
		should = append(should, cond)
	}

	boolQuery := map[string]any{
		"filter":               filter,
		"should":               should,
		"minimum_should_match": 1,
	}

	body := map[string]any{
		"query": map[string]any{
			"bool": boolQuery,
		},
		"size": limit,
		"sort": []map[string]any{
			{"attr_label_id": map[string]string{"order": "desc"}},
		},
	}

	if strategy == wildcard {
		body["timeout"] = getEsQueryTimeout()
	}

	return body
}
