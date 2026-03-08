package vector

import (
	"context"
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	vecEntity "git.woa.com/adp/kb/kb-config/internal/entity/vector"
)

type Dao interface {
	CreateVectorSync(ctx context.Context, row *vecEntity.VectorSync) (uint64, error)
	BatchCreateVectorSync(ctx context.Context, row []*vecEntity.VectorSync) error
	BatchUpdateVectorSync(ctx context.Context, syncs []*vecEntity.VectorSync,
		updateFields map[string]any) error
	UpdateAndLockVectorSync(ctx context.Context, syncId uint64, status uint32,
		updateFields map[string]any) (int64, error)
	UpdateVectorSync(ctx context.Context, syncId uint64, updateFields map[string]any) (int64, error)
	DeleteVectorSync(ctx context.Context, syncId uint64) error

	ListVectorSynsByWriteSynId(ctx context.Context, writeSyncIds []uint64) ([]*vecEntity.VectorSync, error)
	FetchNotSuccessSync(ctx context.Context) ([]*vecEntity.VectorSync, error)
	GetVectorSyncBySyncId(ctx context.Context, syncId uint64) (*vecEntity.VectorSync, error)
	GetVectorSyncCount(ctx context.Context, req *vecEntity.ListVectorSyncReq) (uint64, error)

	BatchCreateVectorSyncHistory(ctx context.Context, row []*vecEntity.VectorSync) error
	BatchDeleteVectorSyncHistoryWithCutoffTime(ctx context.Context, updateTimeUpper time.Time, limit int) (int64, error)

	UpdateImageVectorDeleteStatus(ctx context.Context, robotID uint64, imageIDs []uint64, isDeleted int) error

	GetNodeIdsList(ctx context.Context, appID uint64, selectColumns []string,
		filter *entity.RetrievalNodeFilter) (nodeList []*entity.RetrievalNodeInfo, err error)
	GetDocNodeList(ctx context.Context, appID uint64) (nodeList []*entity.RetrievalNodeInfo, err error)
	GetSegmentNodeByDocID(ctx context.Context, appID, docID, startID, count uint64, selectColumns []string) (
		[]*entity.RetrievalNodeInfo, uint64, error)
}

func NewDao(mysqldb types.MySQLDB, tdsqlDB types.TDSQLDB) Dao {
	return &daoImpl{
		mysql: mysqlquery.Use(mysqldb),
		tdsql: tdsqlquery.Use(tdsqlDB),
	}
}

type daoImpl struct {
	mysql *mysqlquery.Query
	tdsql *tdsqlquery.Query
}

func ConvertVectorSyncPOToDO(po *model.TVectorSync) *vecEntity.VectorSync {
	if po == nil {
		return nil
	}
	return &vecEntity.VectorSync{
		ID:          po.ID,
		Type:        po.Type,
		RelateID:    po.RelateID,
		Status:      po.Status,
		Request:     po.Request,
		TryTimes:    po.TryTimes,
		MaxTryTimes: po.MaxTryTimes,
		Result:      po.Result,
		UpdateTime:  po.UpdateTime,
		CreateTime:  po.CreateTime,
		ExtendedId:  po.ExtendedID,
		WriteSyncId: po.WriteSyncID,
	}
}

func BatchConvertVectorSyncPOToDO(pos []*model.TVectorSync) []*vecEntity.VectorSync {
	if pos == nil {
		return nil
	}
	res := make([]*vecEntity.VectorSync, 0, len(pos))
	for _, po := range pos {
		res = append(res, ConvertVectorSyncPOToDO(po))
	}
	return res
}

func ConvertVectorSyncHistoryPOToDO(po *model.TVectorSyncHistory) *vecEntity.VectorSync {
	if po == nil {
		return nil
	}
	return &vecEntity.VectorSync{
		ID:          uint64(po.ID),
		Type:        po.Type,
		RelateID:    po.RelateID,
		Status:      po.Status,
		Request:     po.Request,
		TryTimes:    po.TryTimes,
		MaxTryTimes: po.MaxTryTimes,
		UpdateTime:  po.UpdateTime,
		CreateTime:  po.CreateTime,
		ExtendedId:  po.ExtendedID,
		WriteSyncId: po.WriteSyncID,
	}
}

func BatchConvertVectorSyncHistoryPOToDO(pos []*model.TVectorSyncHistory) []*vecEntity.VectorSync {
	if pos == nil {
		return nil
	}
	res := make([]*vecEntity.VectorSync, 0, len(pos))
	for _, po := range pos {
		res = append(res, ConvertVectorSyncHistoryPOToDO(po))
	}
	return res
}

func ConvertVectorSyncDOToPO(do *vecEntity.VectorSync) *model.TVectorSync {
	if do == nil {
		return nil
	}
	return &model.TVectorSync{
		ID:          do.ID,
		Type:        do.Type,
		RelateID:    do.RelateID,
		Status:      do.Status,
		Request:     do.Request,
		TryTimes:    do.TryTimes,
		MaxTryTimes: do.MaxTryTimes,
		Result:      do.Result,
		UpdateTime:  do.UpdateTime,
		CreateTime:  do.CreateTime,
		ExtendedID:  do.ExtendedId,
		WriteSyncID: do.WriteSyncId,
	}
}

func BatchConvertVectorSyncDOToPO(dos []*vecEntity.VectorSync) []*model.TVectorSync {
	if dos == nil {
		return nil
	}
	res := make([]*model.TVectorSync, 0, len(dos))
	for _, do := range dos {
		res = append(res, ConvertVectorSyncDOToPO(do))
	}
	return res
}

func ConvertVectorSyncHistoryDOToPO(do *vecEntity.VectorSync) *model.TVectorSyncHistory {
	if do == nil {
		return nil
	}
	return &model.TVectorSyncHistory{
		Type:        do.Type,
		RelateID:    do.RelateID,
		Status:      do.Status,
		Request:     do.Request,
		TryTimes:    do.TryTimes,
		MaxTryTimes: do.MaxTryTimes,
		UpdateTime:  do.UpdateTime,
		CreateTime:  do.CreateTime,
		ExtendedID:  do.ExtendedId,
		WriteSyncID: do.WriteSyncId,
	}
}

func BatchConvertVectorSyncHistoryDOToPO(dos []*vecEntity.VectorSync) []*model.TVectorSyncHistory {
	if dos == nil {
		return nil
	}
	res := make([]*model.TVectorSyncHistory, 0, len(dos))
	for _, do := range dos {
		res = append(res, ConvertVectorSyncHistoryDOToPO(do))
	}
	return res
}
