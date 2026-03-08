package release

import (
	"context"
	"fmt"
	"math"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"

	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbRetrieve "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// getWaitReleaseRelatedIDs 从三个release表查询所有待发布的ID(segment_id/qa_id/rejected_question_id)
// 返回map类型：doc_type -> sub_type(区分标准问和相似问，对于文档和拒答填0即可) -> id列表
// 2 -> 0 -> 文档片段ID列表
// 1 -> 1 -> 标准问答列表
// 1 -> 2 -> 相似问答列表
// 3 -> 0 -> 拒答问列表
func (l *Logic) getWaitReleaseRelatedIDs(ctx context.Context, robotId uint64, versionID uint64, docType int, qaType int) (
	map[entity.DocType]map[int][]uint64, map[entity.DocType]map[int][]uint64, error) {
	logx.I(ctx, " getWaitReleaseRelatedIDs start(docType: %d, qaType: %d)", docType, qaType)
	upsertDocTypeRelatedIDs := make(map[entity.DocType]map[int][]uint64)
	deletedDocTypeRelatedIDs := make(map[entity.DocType]map[int][]uint64)
	deletedIDsMap := make(map[int][]uint64)
	upsertIDsMap := make(map[int][]uint64)
	switch docType {
	// 文档类型 (1 QA, 2 文档段)
	case entity.DocTypeQA:
		logx.I(ctx, "[ReleaseVector]  GetReleaseQAIDs start")
		notDeletedRelatedIDs, deletedRelatedIDs, err := l.GetReleaseQAIDs(ctx, robotId, versionID)
		if err != nil {
			err = fmt.Errorf("[ReleaseVector]  fail, GetReleaseQAIDs fail, err: %w", err)
			logx.W(ctx, err.Error())
			return nil, nil, err
		}
		upsertIDsMap[entity.QATypeReleaseStandard] = notDeletedRelatedIDs
		deletedIDsMap[entity.QATypeReleaseStandard] = deletedRelatedIDs
		logx.I(ctx, "[ReleaseVector]  GetReleaseQAIDs success, count: not deleted:%d, deleted:%d",
			len(notDeletedRelatedIDs), len(deletedRelatedIDs))
		notDeletedSimilarIDs, deletedSimilarIDs, err := l.GetReleaseSimilarIDs(ctx, robotId, versionID)
		if err != nil {
			err = fmt.Errorf("[ReleaseVector]  fail, GetReleaseSimilarIDs fail, err: %w", err)
			logx.W(ctx, err.Error())
			return nil, nil, err
		}
		upsertIDsMap[entity.QATypeReleaseSimilar] = notDeletedSimilarIDs
		deletedIDsMap[entity.QATypeReleaseSimilar] = deletedSimilarIDs
		logx.I(ctx, "[ReleaseVector]  GetReleaseSimilarIDs success, count: not deleted:%d, deleted:%d",
			len(notDeletedSimilarIDs), len(deletedSimilarIDs))

		upsertDocTypeRelatedIDs[entity.DocTypeQA] = upsertIDsMap
		deletedDocTypeRelatedIDs[entity.DocTypeQA] = deletedIDsMap
	case entity.DocTypeSegment:
		logx.I(ctx, "[ReleaseVector]  GetReleaseSegmentIDs start")
		notDeletedRelatedIDs, deletedRelatedIDs, err := l.GetReleaseSegmentIDs(ctx, robotId, versionID)
		if err != nil {
			err = fmt.Errorf("[ReleaseVector]  fail, GetReleaseSegmentIDs fail, err: %w", err)
			logx.W(ctx, err.Error())
			return nil, nil, err
		}
		upsertIDsMap[0] = notDeletedRelatedIDs
		deletedIDsMap[0] = deletedRelatedIDs

		upsertDocTypeRelatedIDs[entity.DocTypeSegment] = upsertIDsMap
		deletedDocTypeRelatedIDs[entity.DocTypeSegment] = deletedIDsMap
		logx.I(ctx,
			"[ReleaseVector]  GetReleaseSegmentIDs success, count: not deleted:%d, deleted:%d",
			len(notDeletedRelatedIDs), len(deletedRelatedIDs))
	case entity.DocTypeRejectedQuestion:
		logx.I(ctx, "[ReleaseVector]  GetReleaseRejectQuestionID start")
		notDeletedRelatedIDs, deletedRelatedIDs, err := l.GetReleaseRejectQuestionID(ctx, robotId, versionID)
		if err != nil {
			err = fmt.Errorf("[ReleaseVector]  fail, GetReleaseRejectQuestionID fail, err: %w", err)
			logx.W(ctx, err.Error())
			return nil, nil, err
		}
		upsertIDsMap[0] = notDeletedRelatedIDs
		deletedIDsMap[0] = deletedRelatedIDs
		upsertDocTypeRelatedIDs[entity.DocTypeRejectedQuestion] = upsertIDsMap
		deletedDocTypeRelatedIDs[entity.DocTypeRejectedQuestion] = deletedIDsMap
		logx.I(ctx, "[ReleaseVector] GetReleaseRejectQuestionID success, count: not deleted:%d, deleted:%d",
			len(notDeletedRelatedIDs), len(deletedRelatedIDs))
	case entity.DocTypeImage:
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("[ReleaseVector] getWaitReleaseRelatedIDs fail, unknown docType: %d", docType)
	}
	logx.I(ctx, "[ReleaseVector]  getWaitReleaseRelatedIDs success")
	return upsertDocTypeRelatedIDs, deletedDocTypeRelatedIDs, nil
}

func (l *Logic) ReleaseVector(ctx context.Context, docType, qaType int, params entity.TaskReleaseVectorParams) error {
	logx.I(ctx, "ReleaseVector start, params: %+v, (docType:%d, qaType: %d)", params, docType, qaType)
	defer timeTrack(ctx, time.Now(), "ReleaseVector")
	// step1: 检查是否允许发布
	if ok, err := l.CheckRelease(ctx, params); err != nil {
		return err
	} else if !ok {
		logx.I(ctx, "release vector not allow, params: %+v", params)
		return nil
	}
	logx.I(ctx, "release vector start, params: %+v", params)

	// step2: 获取所有改动待发布的ID
	upsertDocTypeRelatedIDs, deletedDocTypeRelatedIDs, err := l.getWaitReleaseRelatedIDs(
		ctx, params.AppID, uint64(params.VersionID), docType, qaType)
	if err != nil {
		logx.E(ctx, "[ReleaseVector]  getWaitReleaseRelatedIDs fail,err:%+v", err)
		return err
	}

	logx.I(ctx, "[ReleaseVector] upsertDocTypeRelatedIDs: %+v, deletedDocTypeRelatedIDs:%+v",
		upsertDocTypeRelatedIDs, deletedDocTypeRelatedIDs)

	if err := l.releaseVectorData(ctx, deletedDocTypeRelatedIDs, upsertDocTypeRelatedIDs, params); err != nil {
		logx.E(ctx, "[ReleaseVector] releaseVectorData fail,err:%+v", err)
		return err
	}

	for docType, qaTypeMap := range deletedDocTypeRelatedIDs {
		for qaType, ids := range qaTypeMap {
			if len(ids) == 0 {
				continue // 没有ID跳过
			}
			if err := l.batchNotifyVector(ctx, params.AppID, params.AppBizID, uint64(params.VersionID), docType, qaType, ids); err != nil {
				logx.E(ctx, "[ReleaseVector] batchNotifyVector err:%+v", err)
				return err
			}

		}
	}

	for docType, qaTypeMap := range upsertDocTypeRelatedIDs {
		for qaType, ids := range qaTypeMap {
			if len(ids) == 0 {
				continue // 没有ID跳过
			}
			if err := l.batchNotifyVector(ctx, params.AppID, params.AppBizID, uint64(params.VersionID), docType, qaType, ids); err != nil {
				logx.E(ctx, "[ReleaseVector] batchNotifyVector err:%+v", err)
				return err
			}

		}
	}

	return nil
}

func (l *Logic) releaseVectorData(ctx context.Context, deleteData, upsertData map[entity.DocType]map[int][]uint64,
	params entity.TaskReleaseVectorParams) error {
	logx.I(ctx, "[ReleaseVector] releaseVectorData start, params: %+v", params)
	defer timeTrack(ctx, time.Now(), "[ReleaseVector] releaseVectorData")
	// step1: 创建版本增量

	createVersionReq := &kbRetrieve.CreateVersionReq{
		AppId:              params.AppID,
		AppBizId:           params.AppBizID,
		VersionId:          uint64(params.VersionID),
		VersionName:        params.VersionName,
		EmbeddingVersion:   uint64(params.EmbeddingVersionID),
		EmbeddingModelName: params.EmbeddingModelName,
		LastQaVersion:      params.LastQAVersion,
		IsCreateGroup:      params.IsCreateGroup,
	}
	logx.I(ctx, "ReleaseVector.releaseVectorData step1: CreateVersion req:%+v", createVersionReq)
	_, err := l.rpc.Retrieval.CreateVersion(ctx, createVersionReq)
	if err != nil {
		logx.W(ctx, " [ReleaseVector] CreateVersion fail,err:%+v", err)
		return err
	}

	// step2: 发布文本到SQL
	publishText2SQLReq := &kbRetrieve.PublishText2SQLReq{
		AppId:     params.AppID,
		AppBizId:  params.AppBizID,
		VersionId: uint64(params.VersionID),
	}
	logx.I(ctx, "ReleaseVector.releaseVectorData step2: PublishText2SQL req:%+v", publishText2SQLReq)
	_, err = l.rpc.Retrieval.PublishText2SQL(ctx, publishText2SQLReq)
	if err != nil {
		logx.W(ctx, "[ReleaseVector] PublishText2SQL fail,err:%+v", err)
		return err
	}

	// step3: 处理删除的数据
	logx.I(ctx, "ReleaseVector.releaseVectorData step3: dealDeleteData start, deleteData: %+v", deleteData)
	err = l.dealDeleteData(ctx, params, deleteData)
	if err != nil {
		logx.E(ctx, "[ReleaseVector] dealDeleteData err:%+v", err)
		return err
	}

	// step4: 处理新增和修改的数据
	logx.I(ctx, "ReleaseVector.releaseVectorData step4: dealUpsertData start, upsertData: %+v", upsertData)
	err = l.dealUpsertData(ctx, params, upsertData)

	if err != nil {
		logx.E(ctx, " dealUpsertData fail, err: %+v", err)
		return err
	}

	return nil

}

func (l *Logic) batchNotifyVector(ctx context.Context, robotID, appBizID, versionID uint64, docType entity.DocType, subType int,
	relateIDs []uint64) error {
	logx.I(ctx, "batchNotifyVector start, robotID: %d, versionID: %d, docType: %d, subType: %d, relateIDs: %+v",
		robotID, versionID, docType, subType, relateIDs)
	defer timeTrack(ctx, time.Now(), "batchNotifyVector")
	release, err := l.releaseDao.GetReleaseByID(ctx, versionID)
	if err != nil {
		logx.E(ctx, "GetReleaseByID fail, err: %+v", err)
		return err
	}
	if len(relateIDs) == 0 {
		logx.I(ctx, "batchNotifyVector relateIDs empty")
		return nil
	}
	wg, wgCtx := errgroupx.WithContext(ctx)
	notifyBatchSize := config.App().ReleaseParamConfig.NotifyBatchSize
	if notifyBatchSize <= 0 {
		notifyBatchSize = 10
	}

	relatedIDChunks := slicex.Chunk(relateIDs, 200)
	wg.SetLimit(int(math.Max(float64(notifyBatchSize), float64(len(relatedIDChunks)))))
	for _, chunk := range relatedIDChunks {
		batchRelatedIDs := chunk
		wg.Go(func() error {
			return l.DoReleaseAfterVectorized(wgCtx, robotID, appBizID, batchRelatedIDs, docType, subType, release)
		})
	}
	if err := wg.Wait(); err != nil {
		err = fmt.Errorf("task(ReleaseVector) fail, batchNotifyNode wg.Wait fail, err: %w", err)
		logx.W(ctx, err.Error())
		return nil
	}
	return nil
}

// dealDeleteData 处理删除的数据
func (l *Logic) dealDeleteData(ctx context.Context, params entity.TaskReleaseVectorParams,
	docTypeRelateIDs map[entity.DocType]map[int][]uint64) error {
	defer timeTrack(ctx, time.Now(), "dealDeleteData")
	// 构造 ReleasedNodesList
	retries := int(config.App().ReleaseParamConfig.RetryTimes)
	if retries == 0 {
		retries = 2
	}
	batchSize := config.App().ReleaseParamConfig.BatchDeleteNodeSize
	if batchSize == 0 {
		batchSize = 500
	}

	logx.I(ctx, "dealDeleteData.BatchDeleteKnowledgeProd.batchSize: %d, total retries:%d", batchSize, retries)
	for docType, qaTypeMap := range docTypeRelateIDs {
		for qaType, ids := range qaTypeMap {
			if len(ids) == 0 {
				continue // 没有ID跳过
			}
			idChunks := slicex.Chunk(ids, batchSize)
			logx.I(ctx, "dealDeleteData.BatchDeleteKnowledgeProd.releasedDeleteNodeChunk: %d, retries:%d", len(idChunks), retries)
			for _, ids := range idChunks {
				deleteIds := ids
				releasedNodes := &kbRetrieve.ReleasedNodes{
					DocType: uint32(docType),
					QaType:  uint32(qaType),
					Ids:     deleteIds,
				}
				req := &kbRetrieve.BatchDeleteKnowledgeProdReq{
					AppId:              params.AppID,
					AppBizId:           params.AppBizID,
					VersionId:          uint64(params.VersionID),
					EmbeddingModelName: params.EmbeddingModelName,
					ReleasedNodesList:  []*kbRetrieve.ReleasedNodes{releasedNodes},
				}
				for i := 0; i < retries; i++ {
					logx.I(ctx, "dealDeleteData.BatchDeleteKnowledgeProd rpc  req:%+v (retries:%d)", req, i)
					_, err := l.rpc.Retrieval.BatchDeleteKnowledgeProd(ctx, req)
					if err != nil {
						logx.E(ctx, "rpc BatchDeleteKnowledgeProd err:%+v", err)
						continue
					}
					logx.D(ctx, "dealDeleteData.BatchDeleteKnowledgeProd success (idChunks:%+v)", idChunks)
					break
				}

			}
		}
	}
	logx.I(ctx, "dealDeleteData end, versionID: %d, docTypeRelateIDs: %+v",
		params.VersionID, docTypeRelateIDs)

	return nil
}

func (l *Logic) dealUpsertData(ctx context.Context, params entity.TaskReleaseVectorParams,
	docTypeRelateIDs map[entity.DocType]map[int][]uint64) error {
	defer timeTrack(ctx, time.Now(), "dealUpsertData")

	retries := int(config.App().ReleaseParamConfig.RetryTimes)
	if retries == 0 {
		retries = 2
	}
	batchSize := config.App().ReleaseParamConfig.BatchAddNodesSize
	if batchSize == 0 {
		batchSize = 100
	}
	logx.I(ctx, "dealUpsertData.BatchUpsertKnowledgeProd.batchSize: %d, total retries:%d", batchSize, retries)

	for docType, qaTypeMap := range docTypeRelateIDs {
		for qaType, ids := range qaTypeMap {
			if len(ids) == 0 {
				continue // 没有ID跳过
			}
			idChunks := slicex.Chunk(ids, batchSize)
			logx.I(ctx, "dealUpsertData.BatchUpsertKnowledgeProd.releaseIdChunks: %d, retries:%d", len(idChunks), retries)
			for _, idChunk := range idChunks {
				upsertIds := idChunk
				releasedNodes := &kbRetrieve.ReleasedNodes{
					DocType: uint32(docType),
					QaType:  uint32(qaType),
					Ids:     upsertIds,
				}
				req := &kbRetrieve.BatchUpsertKnowledgeProdReq{
					AppId:              params.AppID,
					AppBizId:           params.AppBizID,
					VersionId:          uint64(params.VersionID),
					EmbeddingModelName: params.EmbeddingModelName,
					ReleasedNodesList:  []*kbRetrieve.ReleasedNodes{releasedNodes},
				}
				// 重试一次
				for i := 0; i < retries; i++ {
					logx.I(ctx, "dealUpsertData.BatchUpsertKnowledgeProd rpc  req:%+v (retries:%d)", req, i)
					_, err := l.rpc.Retrieval.BatchUpsertKnowledgeProd(ctx, req)
					if err != nil {
						logx.E(ctx, "rpc BatchUpsertKnowledgeProd err:%+v", err)
						continue
					}
					logx.I(ctx, "dealUpsertData.BatchUpsertKnowledgeProd success")
					break
				}
			}
		}
	}

	logx.I(ctx, "dealUpsertData end, versionID: %d, docTypeRelateIDs: %+v",
		params.VersionID, docTypeRelateIDs)

	return nil
}

func (l *Logic) CheckRelease(ctx context.Context, params entity.TaskReleaseVectorParams) (bool, error) {
	for _, uin := range config.App().ReleaseParamConfig.BlackRobotIDs {
		if params.AppID == uin {
			logx.E(ctx, "robot %v in black list, not allow publish", params.AppID)
			return false, fmt.Errorf("release vector error")
		}
	}

	// 这块逻辑主要是来判断是否需要进行增量发布。但是实际上这块不需要了。
	// publishNow, err := r.publishNow(ctx)
	// if err != nil {
	//	logx.E(ctx, "[ReleaseVector]  publishNow fail,err:%+v,taskID:%d", err, r.task.ID)
	//	return kv, err
	// }
	// if !publishNow {
	//	return kv, errors.New("publishNow is false and no execute")
	// }
	return true, nil
}
