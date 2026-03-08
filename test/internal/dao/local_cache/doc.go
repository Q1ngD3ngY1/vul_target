package local_cache

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-database/localcache"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"github.com/spf13/cast"
	"strconv"
)

const (
	docIDExpiration = 24 * 3600 // 24小时
	docIDCapacity   = 10000
)

var (
	docID2DocBizIDCache localcache.Cache
	docBizID2DocIDCache localcache.Cache
)

func init() {
	docID2DocBizIDCache = localcache.New(localcache.WithExpiration(docIDExpiration), localcache.WithCapacity(docIDCapacity))
	docBizID2DocIDCache = localcache.New(localcache.WithExpiration(docIDExpiration), localcache.WithCapacity(docIDCapacity))
}

// GetDocBizIDByDocID 根据docID获取docBizID
func GetDocBizIDByDocID(ctx context.Context, routerAppBizID uint64, docID uint64) (uint64, error) {
	docID2DocBizIDMap, err := GetDocBizIDByDocIDs(ctx, routerAppBizID, []uint64{docID})
	if err != nil {
		return 0, err
	}
	if docBizID, ok := docID2DocBizIDMap[docID]; ok {
		return docBizID, nil
	}
	return 0, errs.ErrDocNotFound
}

// GetDocBizIDByDocIDs 根据docIDs获取docBizIDs
func GetDocBizIDByDocIDs(ctx context.Context, routerAppBizID uint64, docIDs []uint64) (map[uint64]uint64, error) {
	docID2DocBizIDMap := make(map[uint64]uint64)
	notInCacheDocIDs := make([]uint64, 0)
	// 先查缓存
	for _, docID := range docIDs {
		key := cast.ToString(docID)
		value, exist := docID2DocBizIDCache.Get(key)
		if exist {
			docBizID, ok := value.(uint64)
			if !ok {
				err := fmt.Errorf("docBizID %v format error", value)
				log.InfoContextf(ctx, "%+v", err)
				return nil, err
			}
			docID2DocBizIDMap[docID] = docBizID
		} else {
			notInCacheDocIDs = append(notInCacheDocIDs, docID)
		}
	}
	if len(notInCacheDocIDs) == 0 {
		return docID2DocBizIDMap, nil
	}
	// 未命中缓存的从数据库中查
	filter := &dao.DocFilter{
		RouterAppBizID: routerAppBizID,
		IDs:            notInCacheDocIDs,
		Limit:          uint32(len(notInCacheDocIDs)),
	}
	selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId}
	docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, doc := range docs {
		key := strconv.FormatUint(doc.ID, 10)
		docID2DocBizIDCache.Set(key, doc.BusinessID)
		docID2DocBizIDMap[doc.ID] = doc.BusinessID
	}

	return docID2DocBizIDMap, nil
}

// GetDocIDByDocBizID 根据docBizID获取docID
func GetDocIDByDocBizID(ctx context.Context, routerAppBizID uint64, docBizID uint64) (uint64, error) {
	docBizID2DocIDMap, err := GetDocIDByDocBizIDs(ctx, routerAppBizID, []uint64{docBizID})
	if err != nil {
		return 0, err
	}
	if docID, ok := docBizID2DocIDMap[docBizID]; ok {
		return docID, nil
	}
	return 0, errs.ErrDocNotFound
}

// GetDocIDByDocBizIDs 根据docBizIDs获取docIDs
func GetDocIDByDocBizIDs(ctx context.Context, routerAppBizID uint64, docBizIDs []uint64) (map[uint64]uint64, error) {
	docBizID2DocIDMap := make(map[uint64]uint64)
	notInCacheDocBizIDs := make([]uint64, 0)
	// 先查缓存
	for _, docBizID := range docBizIDs {
		key := cast.ToString(docBizID)
		value, exist := docBizID2DocIDCache.Get(key)
		if exist {
			docID, ok := value.(uint64)
			if !ok {
				err := fmt.Errorf("docID %v format error", value)
				log.InfoContextf(ctx, "%+v", err)
				return nil, err
			}
			docBizID2DocIDMap[docBizID] = docID
		} else {
			notInCacheDocBizIDs = append(notInCacheDocBizIDs, docBizID)
		}
	}
	if len(notInCacheDocBizIDs) == 0 {
		return docBizID2DocIDMap, nil
	}
	// 未命中缓存的从数据库中查
	filter := &dao.DocFilter{
		RouterAppBizID: routerAppBizID,
		BusinessIds:    notInCacheDocBizIDs,
		Limit:          uint32(len(notInCacheDocBizIDs)),
	}
	selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId}
	docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, doc := range docs {
		key := strconv.FormatUint(doc.BusinessID, 10)
		docBizID2DocIDCache.Set(key, doc.ID)
		docBizID2DocIDMap[doc.BusinessID] = doc.ID
	}

	return docBizID2DocIDMap, nil
}
