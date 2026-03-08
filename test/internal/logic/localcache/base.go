package localcache

import (
	"git.code.oa.com/trpc-go/trpc-database/localcache"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

const (
	expiration = 24 * 3600 // 24小时
	capacity   = 10000
)

var (
	docID2DocBizIDCache localcache.Cache
	docBizID2DocIDCache localcache.Cache

	appID2AppBizIDCache localcache.Cache
	appBizID2AppIDCache localcache.Cache
)

func Init() {
	appID2AppBizIDCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
	appBizID2AppIDCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
	docID2DocBizIDCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
	docBizID2DocIDCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
}

type Logic struct {
	docDao docDao.Dao
	rpc    *rpc.RPC
}

func NewLogic(docDao docDao.Dao, rpc *rpc.RPC) *Logic {
	Init()
	l := &Logic{
		docDao: docDao,
		rpc:    rpc,
	}

	return l
}
