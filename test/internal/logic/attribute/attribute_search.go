package attribute

import (
	"context"
	"sort"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"golang.org/x/sync/errgroup"
)

func GetAttrIDBySearchLabelsWithEs(ctx context.Context, robotID uint64, req *pb.ListAttributeLabelReq) ([]uint64, int, error) {
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
		idsInAttr1, err = dao.QueryAttributeMatchPhrase(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
		if err != nil {
			return err
		}
		idsInLabel1, err = dao.GetAttrIDByQueryLabelMatchPhrase(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
		if err != nil {
			return err
		}
		return nil
	})
	wg.Go(func() error {
		// 尝试通过通配符搜索一次
		idsInAttr2, err = dao.QueryAttributeWildcard(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
		if err != nil {
			return err
		}
		idsInLabel2, err = dao.GetAttrIDByQueryLabelWildcard(wgCtx, robotID, req.GetQuery(), config.App().AttributeLabel.AttrLimit)
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
		log.WarnContextf(ctx, "query attribute labels by es, total: %v, return: %v, start: %v, err: %v",
			total, len(ids), start, err)
	}

	return ids, total, nil
}

func QueryAttributeLabelCursor(ctx context.Context, attrID uint64, query string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]uint64, error) {
	matchIDs, err := dao.QueryAttributeLabelCursorMatchPhrase(ctx, attrID, query, queryScope, lastLabelID, limit, robotID)
	if err != nil {
		return nil, err
	}
	wildcardIDs, err := dao.QueryAttributeLabelCursorWildcard(ctx, attrID, query, queryScope, lastLabelID, limit, robotID)
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
	log.InfoContextf(ctx, "QueryAttributeLabelCursor get: %v, chosne: %v, %+v", len(allIDs), end, ids)
	return ids, err
}
