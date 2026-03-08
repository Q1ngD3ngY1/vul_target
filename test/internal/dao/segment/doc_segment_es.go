package segment

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
)

func (d *daoImpl) QueryQaSegmentInProd(ctx context.Context, robotID uint64, qaID uint64, source []string, size int) ([]*segEntity.EsSegment, error) {
	conditions := map[string]any{
		"id":       qaID,
		"doc_type": entity.DocTypeQA,
	}
	return d.QuerySegment(ctx, robotID, knowClient.KnowledgeSegmentProdIndex, source, conditions, size)
}

func (d *daoImpl) QueryDocSegmentInProd(ctx context.Context, robotID uint64, docID uint64, source []string, size int) ([]*segEntity.EsSegment, error) {
	conditions := map[string]any{
		"doc_id":   docID,
		"doc_type": entity.DocTypeSegment,
	}
	return d.QuerySegment(ctx, robotID, knowClient.KnowledgeSegmentProdIndex, source, conditions, size)
}

func (d *daoImpl) QuerySegment(ctx context.Context, robotID uint64, indexType knowClient.IndexType, source []string,
	conditions map[string]any, size int) ([]*segEntity.EsSegment, error) {
	t0 := time.Now()
	body := genSegmentEsQuery(source, conditions, size)

	resp, err := knowClient.Request(ctx, d.esClient, robotID, body, indexType)
	if err != nil {
		return nil, err
	}
	var segments []*segEntity.EsSegment
	for _, hit := range resp.Hits.Hits {
		sourceBytes, err := jsonx.Marshal(hit.Source)
		if err != nil {
			logx.E(ctx, "querySegment|Could not marshal _source to bytes: %v", err)
			return nil, err
		}
		segment := &segEntity.EsSegment{}
		if err = jsonx.Unmarshal(sourceBytes, segment); err != nil {
			logx.E(ctx, "querySegment|Could not unmarshal _source bytes %s to Source:%v", sourceBytes, err)
			return nil, err
		}
		segments = append(segments, segment)
	}
	logx.I(ctx, "querySegment get segments length:%d, cost: %vms", len(segments), time.Now().Sub(t0).Milliseconds())
	return segments, nil
}

func genSegmentEsQuery(source []string, conditions map[string]any, size int) map[string]any {
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
