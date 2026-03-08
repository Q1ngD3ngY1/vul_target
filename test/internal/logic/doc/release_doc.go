package doc

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
)

// GetReleasingDocId 获取发布中的文档id
func GetReleasingDocId(ctx context.Context, robotID uint64, docIds []uint64) (map[uint64]struct{}, error) {
	corpID := pkg.CorpID(ctx)
	releaseDocs := make(map[uint64]struct{}, 0)
	latestRelease, err := dao.GetReleaseDao().GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return releaseDocs, nil
	}
	if latestRelease.IsPublishDone() {
		return releaseDocs, nil
	}
	releaseDocs, err = dao.GetReleaseDocDao().GetReleaseDocIdMap(ctx, corpID, robotID, latestRelease.ID, docIds)
	if err != nil {
		return nil, err
	}
	releaseQaDocs, err := dao.GetReleaseQaDao().GetReleaseQaDocIdMap(ctx, corpID, robotID, latestRelease.ID, docIds)
	if err != nil {
		return nil, err
	}
	for k := range releaseQaDocs {
		releaseDocs[k] = struct{}{}
	}
	return releaseDocs, nil
}
