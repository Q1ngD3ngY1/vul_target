package segment

import (
	"context"
	qaDao "git.woa.com/adp/kb/kb-config/internal/dao/qa"

	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	segDao "git.woa.com/adp/kb/kb-config/internal/dao/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/logic/vector"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
)

func NewLogic(rpc *rpc.RPC, docDao docDao.Dao, qaDao qaDao.Dao, segDao segDao.Dao, releaseDao releaseDao.Dao,
	dbDao dbdao.Dao, s3 dao.S3, vectorSyncLogic *vector.VectorSyncLogic, cache *localcache.Logic,
) *Logic {
	return &Logic{
		rpc:             rpc,
		docDao:          docDao,
		segDao:          segDao,
		releaseDao:      releaseDao,
		dbDao:           dbDao,
		qaDao:           qaDao,
		s3:              s3,
		cache:           cache,
		vectorSyncLogic: vectorSyncLogic,
	}
}

type Logic struct {
	rpc             *rpc.RPC
	docDao          docDao.Dao
	segDao          segDao.Dao
	qaDao           qaDao.Dao
	releaseDao      releaseDao.Dao
	dbDao           dbdao.Dao
	s3              dao.S3
	cache           *localcache.Logic
	vectorSyncLogic *vector.VectorSyncLogic
}

func (l *Logic) GetVectorSyncLogic() *vector.VectorSyncLogic {
	return l.vectorSyncLogic
}

func (l *Logic) GetDao() segDao.Dao {
	return l.segDao
}

// GetGorm 根据appBizID获取gorm
func (l *Logic) GetGormDB(ctx context.Context, appBizID uint64, tableName string) (*gorm.DB, error) {
	// appID, err := l.cache.GetAppPrimaryIdByBizId(ctx, appBizID)
	// if err != nil {
	//	logx.E(ctx, "get appID failed, err: %+v", err)
	//	return nil, err
	// }

	db, err := knowClient.GormClient(ctx, tableName, 0, appBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}
	return db, nil
}
