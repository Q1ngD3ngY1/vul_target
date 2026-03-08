package database

import (
	dao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

type Logic struct {
	rpc        *rpc.RPC
	dao        dao.Dao
	userLogic  *user.Logic
	kbLogic    *kb.Logic
	releaseDao releaseDao.Dao
}

func NewLogic(dao dao.Dao, rpc *rpc.RPC, kbLogic *kb.Logic, userLogic *user.Logic, releaseDao releaseDao.Dao) *Logic {
	return &Logic{
		rpc:        rpc,
		dao:        dao,
		kbLogic:    kbLogic,
		userLogic:  userLogic,
		releaseDao: releaseDao,
	}
}
