package segment

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
)

func (d *daoImpl) QueryDocBigData(ctx context.Context, robotID uint64, docID uint64, source []string, size int) ([]*segEntity.EsDocBigData, error) {
	t0 := time.Now()
	conditions := map[string]any{
		"robot_id": robotID,
		"doc_id":   docID,
	}
	body := genDocBigDataEsQuery(source, conditions, size)

	resp, err := knowClient.Request(ctx, d.esClient, robotID, body, knowClient.DocBigDataProdIndex)
	if err != nil {
		return nil, err
	}
	var bigData []*segEntity.EsDocBigData
	for _, hit := range resp.Hits.Hits {
		sourceBytes, err := jsonx.Marshal(hit.Source)
		if err != nil {
			logx.E(ctx, "QueryDocBigData|Could not marshal _source to bytes: %v", err)
			return nil, err
		}
		data := &segEntity.EsDocBigData{}
		if err = jsonx.Unmarshal(sourceBytes, data); err != nil {
			logx.E(ctx, "QueryDocBigData|Could not unmarshal _source bytes %s to Source:%v", sourceBytes, err)
			return nil, err
		}
		bigData = append(bigData, data)
	}
	logx.I(ctx, "QueryDocBigData get bigData length:%d, cost: %vms", len(bigData), time.Now().Sub(t0).Milliseconds())
	return bigData, nil
}

func genDocBigDataEsQuery(source []string, conditions map[string]any, size int) map[string]any {
	var esConditions []map[string]any
	for k, v := range conditions {
		esConditions = append(esConditions, map[string]any{
			"term": map[string]any{
				k: v,
			},
		},
		)
	}
	body := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": esConditions,
			},
		},
		"size":    size,
		"timeout": "200ms",
	}
	if len(source) != 0 {
		body["_source"] = source
	}
	return body
}
