package document

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
)

// GetReleasingDocId 获取发布中的文档id
func (l *Logic) GetReleasingDocId(ctx context.Context, robotID uint64, docIds []uint64) (map[uint64]struct{}, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	releaseDocs := make(map[uint64]struct{}, 0)
	latestRelease, err := l.releaseDao.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return releaseDocs, nil
	}
	if latestRelease.IsPublishDone() {
		return releaseDocs, nil
	}
	releaseDocs, err = l.releaseDao.GetReleaseDocIdMap(ctx, corpID, robotID, latestRelease.ID, docIds)
	if err != nil {
		return nil, err
	}
	releaseQaDocs, err := l.releaseDao.GetReleaseQaDocIdMap(ctx, corpID, robotID, latestRelease.ID, docIds)
	if err != nil {
		return nil, err
	}
	for k := range releaseQaDocs {
		releaseDocs[k] = struct{}{}
	}
	return releaseDocs, nil
}
