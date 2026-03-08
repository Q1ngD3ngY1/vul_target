package label

import (
	"context"
	"sort"

	"git.woa.com/adp/common/x/logx"
	"golang.org/x/sync/errgroup"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/kb/kb-config/internal/config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func (l *Logic) GetAttrIDBySearchLabelsWithEs(ctx context.Context, robotID uint64, req *pb.ListAttributeLabelReq) ([]uint64, int, error) {
	from := (req.GetPageNumber() - 1) * req.GetPageSize()
	if from < 0 {
		from = 0
	}
	var allIDs, idsInAttr1, idsInLabel1, idsInAttr2, idsInLabel2 []uint64
	var err error
	wg, wgCtx := errgroup.WithContext(ctx)
	wg.SetLimit(2)
	wg.Go(func() error {
		// 在 attribute 和 attribute_label 的索引中 查询出所有满足条件的attribute
		idsInAttr1, err = l.dao.QueryAttributeMatchPhrase(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
		if err != nil {
			return err
		}
		idsInLabel1, err = l.dao.GetAttrIDByQueryLabelMatchPhrase(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
		if err != nil {
			return err
		}
		return nil
	})
	wg.Go(func() error {
		// 尝试通过通配符搜索一次
		idsInAttr2, err = l.dao.QueryAttributeWildcard(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
		if err != nil {
			return err
		}
		idsInLabel2, err = l.dao.GetAttrIDByQueryLabelWildcard(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
		if err != nil {
			return err
		}
		return nil
	})
	if err = wg.Wait(); err != nil {
		return allIDs, 0, err
	}

	allIDs = append(allIDs, idsInAttr1...)
	allIDs = append(allIDs, idsInLabel1...)
	allIDs = append(allIDs, idsInAttr2...)
	allIDs = append(allIDs, idsInLabel2...)
	allIDs = slicex.Unique(allIDs)
	// 按照ID降序排列
	sort.Slice(allIDs, func(i, j int) bool {
		return allIDs[i] > allIDs[j]
	})
	total := len(allIDs)
	if total == 0 {
		return allIDs, len(allIDs), nil
	}

	start := int((req.GetPageNumber() - 1) * req.GetPageSize())
	if start < 0 {
		start = 0
	}
	if start > total {
		start = total
	}
	end := start + int(req.GetPageSize())
	if end > total {
		end = total
	}
	ids := allIDs[start:end]

	// 使用es的分页做了多个数据源的合并其实是不准确的，希望尽量全量的查询
	if len(ids) != total {
		logx.W(ctx, "query attribute labels by es, total: %v, return: %v, start: %v, err: %v",
			total, len(ids), start, err)
	}

	return ids, total, nil
}

func (l *Logic) QueryAttributeLabelCursor(ctx context.Context, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	matchIDs, err := l.dao.QueryAttributeLabelCursorMatchPhrase(ctx, attrID, query, queryScope, lastLabelID, limit, robotID)
	if err != nil {
		return nil, err
	}
	wildcardIDs, err := l.dao.QueryAttributeLabelCursorWildcard(ctx, attrID, query, queryScope, lastLabelID, limit, robotID)
	if err != nil {
		return nil, err
	}
	allIDs := append(matchIDs, wildcardIDs...)
	allIDs = slicex.Unique(allIDs)
	// 按照ID降序排列
	sort.Slice(allIDs, func(i, j int) bool {
		return allIDs[i] > allIDs[j]
	})
	end := int(limit)
	if end > len(allIDs) {
		end = len(allIDs)
	}
	ids := allIDs[0:end]
	logx.I(ctx, "QueryAttributeLabelCursor get: %v, chosne: %v, %+v", len(allIDs), end, ids)
	return ids, err
}
