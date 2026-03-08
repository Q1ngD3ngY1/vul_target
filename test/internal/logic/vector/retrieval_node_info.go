package vector

import (
	"context"

	"git.woa.com/adp/kb/kb-config/internal/entity"
)

func (l *VectorSyncLogic) GetNodeIdsList(ctx context.Context, appID uint64, selectColumns []string,
	filter *entity.RetrievalNodeFilter) (nodeList []*entity.RetrievalNodeInfo, err error) {
	return l.vecDao.GetNodeIdsList(ctx, appID, selectColumns, filter)

}
func (l *VectorSyncLogic) GetDocNodeList(ctx context.Context, appID uint64) (nodeList []*entity.RetrievalNodeInfo, err error) {
	return l.vecDao.GetDocNodeList(ctx, appID)
}

func (l *VectorSyncLogic) GetSegmentNodeByDocID(ctx context.Context, appID, docID, startID, count uint64, selectColumns []string) (
	[]*entity.RetrievalNodeInfo, uint64, error) {
	return l.vecDao.GetSegmentNodeByDocID(ctx, appID, docID, startID, count, selectColumns)
}
