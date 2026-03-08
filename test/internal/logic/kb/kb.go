package kb

import (
	dao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	doc "git.woa.com/adp/kb/kb-config/internal/dao/document"
	"git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	cateLogic "git.woa.com/adp/kb/kb-config/internal/logic/category"
	"git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	cacheLogic "git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/logic/qa"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

type Logic struct {
	kbDao        kb.Dao
	docDao       doc.Dao
	dao          dao.Dao
	rpc          *rpc.RPC
	rdb          types.AdminRedis
	financeLogic *finance.Logic
	userLogic    *user.Logic
	docLogic     *document.Logic
	qaLogic      *qa.Logic
	cacheLogic   *cacheLogic.Logic
	cateLogic    *cateLogic.Logic
}

func NewLogic(dao dao.Dao, kbDao kb.Dao, rdb types.AdminRedis, docDao doc.Dao, rpc *rpc.RPC, financeLogic *finance.Logic, userLogic *user.Logic, docLogic *document.Logic, qaLogic *qa.Logic, cacheLogic *cacheLogic.Logic, cateLogic *cateLogic.Logic) *Logic {
	return &Logic{
		kbDao:        kbDao,
		docDao:       docDao,
		rdb:          rdb,
		rpc:          rpc,
		dao:          dao,
		financeLogic: financeLogic,
		userLogic:    userLogic,
		docLogic:     docLogic,
		qaLogic:      qaLogic,
		cacheLogic:   cacheLogic,
		cateLogic:    cateLogic,
	}
}

func (l *Logic) GetKbDao() kb.Dao {
	return l.kbDao
}
