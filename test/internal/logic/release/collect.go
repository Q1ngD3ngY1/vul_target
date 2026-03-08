package release

import (
	"context"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"time"

	"git.woa.com/adp/common/x/logx"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

func (l *Logic) collectDB(ctx context.Context, appBizID uint64, releaseBizID uint64) error {
	// 收到DB采集事件，将信息同步到快照表
	defer timeTrack(ctx, time.Now(), "collectDB")
	err := l.dbLogic.CollectUnreleasedDBSource(ctx, appBizID, releaseBizID)
	if err != nil {
		return err
	}
	err = l.dbLogic.CollectUnreleasedDBTable(ctx, appBizID, releaseBizID)
	if err != nil {
		return err
	}
	return nil
}

func (l *Logic) collectReleaseQA(ctx context.Context, corpID, appID, appBizID, versionID uint64) error {
	release, err := l.GetReleaseByID(ctx, versionID)
	if err != nil {
		return err
	}
	if release == nil {
		return errs.ErrReleaseNotFound
	}
	if !release.IsInit() {
		return errs.ErrReleaseIsNotInit
	}

	releaseQAs, releaseSimilarQAs, err := l.getReleaseQA(ctx, corpID, appID, versionID)
	if err != nil {
		logx.E(ctx, "getReleaseQA release (versionID:%d, appBizID:%d) err:%+v",
			versionID, appBizID, err)
		return err
	}

	// 这里不需要发布审核了
	for _, qa := range releaseQAs {
		if qa.IsAuditDoing() {
			release.Status = releaseEntity.ReleaseStatusAudit
			break
		}
	}
	if len(releaseQAs) > 0 { // 相似问答，只有主问答有发布，相似问答才会跟随发布
		for _, similarQA := range releaseSimilarQAs {
			if similarQA.IsAuditDoing() {
				release.Status = releaseEntity.ReleaseStatusAudit
				break
			}
		}
	}

	if err := l.BatchCreateReleaseSimilarQA(ctx, releaseSimilarQAs); err != nil {
		logx.E(ctx, "[collectReleaseQA] BatchCreateReleaseSimilarQA error:%v", err)
		return err
	}

	if err := l.BatchCreateReleaseQAs(ctx, releaseQAs); err != nil {
		logx.E(ctx, "[collectReleaseQA] BatchCreateReleaseQAs error:%v", err)
		return err
	}

	return nil
}

func (l *Logic) simpleCollectReleaseDoc(ctx context.Context, corpID, appID, appBizID, versionID uint64) error {
	logx.I(ctx, "simpleCollectReleaseDoc start, corpID: %d, appID: %d, appBizID: %d, versionID: %d", corpID, appID, appBizID, versionID)
	return nil
}

// 文档发布采集，包括Doc 和 DocSegment
func (l *Logic) CollectReleaseDoc(ctx context.Context, corpID, appID, appBizID uint64, versionID uint64) error {
	defer timeTrack(ctx, time.Now(), "CollectReleaseDoc")
	releaseDoc, err := l.getReleaseDocs(ctx, corpID, appID, versionID)
	if err != nil {
		logx.E(ctx, "getReleaseDocs release (versionID:%d, appBizID:%d) err:%+v",
			versionID, appBizID, err)
		return err
	}

	wg, wgCtx := errgroupx.WithContext(ctx)
	concurrency, batchSize := config.App().ReleaseParamConfig.CreateReleaseConcurrency,
		config.App().ReleaseParamConfig.CreateReleaseBatchSize
	if concurrency == 0 {
		concurrency = 2
	}
	if batchSize == 0 {
		batchSize = 100
	}
	wg.SetLimit(concurrency)
	releaseDocChunks := slicex.Chunk(releaseDoc, batchSize)

	logx.I(ctx, "CollectReleaseDoc -> %d releaseDocChunks(%d docs/batches), total:%d",
		len(releaseDocChunks), batchSize, len(releaseDoc))

	for _, releaseDocChunk := range releaseDocChunks {
		releaseDocChunk := releaseDocChunk
		wg.Go(func() error {
			releaseSegments, err := l.getReleaseSegment(ctx, appBizID, versionID, releaseDocChunk)
			if err != nil {
				logx.E(ctx, "getReleaseSegment release (versionID:%d, appBizID:%d) err:%+v",
					versionID, appBizID, err)
				return err
			}
			logx.D(ctx, "CollectReleaseDoc.getReleaseSegment(%d releaseSegments)", len(releaseSegments))

			if err := l.BatchCreateReleaseSegments(wgCtx, releaseSegments); err != nil {
				logx.E(ctx, "[CollectReleaseDoc] BatchCreateReleaseSegments error:%v", err)
				return err
			}

			if err := l.BatchCreateReleaseDocs(wgCtx, releaseDocChunk); err != nil {
				logx.E(ctx, "[CollectReleaseDoc] BatchCreateReleaseDocs error:%v", err)
				return err
			}
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		logx.E(ctx, "collectReleaseDoc wg.Wait error:%v", err)
		return err
	}

	return nil
}

func (l *Logic) collectReleaseRejectedQuestion(ctx context.Context, corpID, appID, versionID uint64) error {
	defer timeTrack(ctx, time.Now(), "collectReleaseRejectedQuestion")
	releaseRejectedQuestions, err := l.getReleaseRejectedQuestion(ctx, corpID, appID)
	if err != nil {
		logx.E(ctx, "getReleaseRejectedQuestion err:%+v", err)
		return err
	}

	for _, v := range releaseRejectedQuestions {
		v.VersionID = versionID
	}

	if err := l.BatchCreateReleaseRejectedQuestions(ctx, releaseRejectedQuestions); err != nil {
		logx.E(ctx, "[collectReleaseRejectedQuestion] BatchCreateReleaseRejectedQuestions error:%v", err)
		return err
	}

	return nil
}

func (l *Logic) collectReleaseLabel(ctx context.Context, appID uint64, versionID uint64) error {
	defer timeTrack(ctx, time.Now(), "collectReleaseLabel")
	releaseLabelDetails, err := l.getReleaseLabel(ctx, appID, versionID)
	if err != nil {
		return err
	}

	if err := l.BatchCreateReleaseLabelDetail(ctx, releaseLabelDetails); err != nil {
		logx.E(ctx, "[collectReleaseLabel] BatchCreateReleaseLabelDetail error:%v", err)
		return err
	}

	return nil
}
