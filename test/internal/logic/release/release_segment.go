package release

import (
	"context"
	"math"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
)

type ReleaseSegmentItem struct {
	Segment        *segEntity.DocSegmentExtend
	ReleaseSegment *releaseEntity.ReleaseSegment
}

func (l *Logic) BatchCreateReleaseSegments(ctx context.Context, releaseSegments []*releaseEntity.ReleaseSegment) error {
	if err := batchReleaseProcess(ctx, releaseSegments, l.batchCreateReleaseSegmentUnit); err != nil {
		logx.W(ctx, "batchCreateReleaseSegment err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) batchCreateReleaseSegmentUnit(ctx context.Context, releaseSegments []*releaseEntity.ReleaseSegment) error {
	logx.I(ctx, "batchCreateReleaseSegmentUnit.start:%d segments", len(releaseSegments))
	defer timeTrack(ctx, time.Now(), "batchCreateReleaseSegmentUnit")
	if len(releaseSegments) == 0 {
		return nil
	}

	chunkBatchSize := config.App().ReleaseParamConfig.CreateReleaseBatchSize
	if chunkBatchSize == 0 {
		chunkBatchSize = 200
	}

	segmentsChunks := slicex.Chunk(releaseSegments, chunkBatchSize)
	for _, batchReleaseSegs := range segmentsChunks {
		if err := l.releaseDao.MysqlQuery().TReleaseSegment.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
			// batchReleaseSegs := []*releaseEntity.ReleaseSegment{v}
			err := l.releaseDao.CreateReleaseSegmentRecords(ctx, batchReleaseSegs, tx)
			if err != nil {
				logx.E(ctx, "create release segment record error:%v", err)
				return err
			}

			docSegmentIds := make([]uint64, 0, len(batchReleaseSegs))
			for _, releaseSeg := range batchReleaseSegs {
				docSegmentIds = append(docSegmentIds, releaseSeg.SegmentID)
			}

			filter := &segEntity.DocSegmentFilter{
				IDs: docSegmentIds,
			}

			updateColumns := map[string]any{
				segEntity.DocSegmentTblColReleaseStatus: segEntity.SegmentReleaseStatusIng,
				segEntity.DocSegmentTblColUpdateTime:    time.Now(),
			}

			// err = l.segDao.BatchUpdateDocSegmentReleaseStatusByID(ctx, docSegmentIds, segEntity.SegmentReleaseStatusIng, tx)
			err = l.segDao.BatchUpdateDocSegmentByFilter(ctx, filter, updateColumns, tx)
			if err != nil {
				logx.E(ctx, "update doc segment release status error:%v", err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "批量创建segment发布失败 err:%+v", err)
			return err
		}
	}
	return nil
}

func (l *Logic) GetReleaseDeleteSegment(ctx context.Context, robotID, versionID uint64) (
	[]*releaseEntity.ReleaseSegment, error) {
	total, err := l.releaseDao.GetModifySegmentCount(ctx, robotID, versionID, qaEntity.NextActionDelete, nil)
	if err != nil {
		return nil, err
	}

	if total == 0 {
		return nil, nil
	}

	segChan := make(chan *releaseEntity.ReleaseSegment, 5000)
	finishChan := make(chan any)
	segments := make([]*releaseEntity.ReleaseSegment, 0, total)

	go func() {
		defer gox.Recover()
		for seg := range segChan {
			segments = append(segments, seg)
		}
		finishChan <- nil
	}()
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))

	listReq := &releaseEntity.ListReleaseSegmentReq{
		RobotID:   robotID,
		VersionID: versionID,
		Actions:   []uint32{qaEntity.NextActionDelete},
		PageSize:  uint32(pageSize),
	}
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		listReq.Page = page

		segments, err := l.releaseDao.GetModifySegmentList(ctx, listReq)
		if err != nil {
			return nil, err
		}
		for _, segment := range segments {
			segChan <- segment
		}
	}
	close(segChan)
	<-finishChan
	return segments, nil
}

func (l *Logic) ReleaseSegmentSuccess(ctx context.Context, releaseSegments []*releaseEntity.ReleaseSegment, tx *gorm.DB) error {
	total := len(releaseSegments)
	batchSize := 500
	batches := int(math.Ceil(float64(total) / float64(batchSize)))

	for batchId := 0; batchId < batches; batchId++ {
		start := batchId * batchSize
		end := (batchId + 1) * batchSize
		if end > total {
			end = total
		}
		batchReleaseSegments := releaseSegments[start:end]
		segIds := make([]uint64, 0, len(batchReleaseSegments))
		for _, releaseSegment := range batchReleaseSegments {
			segIds = append(segIds, releaseSegment.SegmentID)
		}

		logx.I(ctx, "update doc segment action to:%v[batch:%d]", segEntity.SegNextActionAdd, batchId)
		updates := map[string]any{

			segEntity.DocSegmentTblColUpdateTime: time.Now(),
			segEntity.DocSegmentTblColNextAction: segEntity.SegNextActionAdd,
		}
		filter := &segEntity.DocSegmentFilter{
			IDs: segIds,
		}

		err := l.segDao.BatchUpdateDocSegmentByFilter(ctx, filter, updates, tx)
		if err != nil {
			logx.E(ctx, "update doc segment action error:%v", err)
			return err
		}
	}

	return nil
}

// getReleaseSegment 获取发布的Segment
func (l *Logic) getReleaseSegment(
	ctx context.Context, appBizID, versionID uint64, releaseDocs []*releaseEntity.ReleaseDoc) ([]*releaseEntity.ReleaseSegment, error) {
	defer timeTrack(ctx, time.Now(), "getReleaseSegment")
	logx.I(ctx, "getReleaseSegment|start, appBizID:%d, versionID:%d, %d releaseDocs", appBizID, versionID, len(releaseDocs))
	releaseSegments := make([]*releaseEntity.ReleaseSegment, 0, 50000)
	releaseSegmentChan := make(chan *segEntity.DocSegmentExtend, 5000)
	existReleaseSegments := make(map[uint64]struct{})
	mapDocID2AttrLabels := new(sync.Map)
	finish := make(chan any)
	now := time.Now()
	go func() {
		defer gox.Recover()
		for seg := range releaseSegmentChan {
			if _, ok := existReleaseSegments[seg.ID]; ok {
				continue
			}
			existReleaseSegments[seg.ID] = struct{}{}
			releaseSegments = append(releaseSegments, &releaseEntity.ReleaseSegment{
				RobotID:        seg.RobotID,
				CorpID:         seg.CorpID,
				StaffID:        seg.StaffID,
				DocID:          seg.DocID,
				SegmentID:      seg.ID,
				VersionID:      versionID,
				FileType:       seg.FileType,
				Title:          seg.Title,
				PageContent:    seg.PageContent,
				OrgData:        seg.OrgData,
				SegmentType:    seg.SegmentType,
				SplitModel:     seg.SplitModel,
				Status:         seg.Status,
				ReleaseStatus:  seg.ReleaseStatus,
				Message:        seg.Message,
				IsDeleted:      seg.IsDeleted,
				Action:         seg.NextAction,
				BatchID:        int32(seg.BatchID),
				RichTextIndex:  int32(seg.RichTextIndex),
				StartIndex:     int32(seg.StartChunkIndex),
				EndIndex:       int32(seg.EndChunkIndex),
				BigStartIndex:  seg.BigStart,
				BigEndIndex:    seg.BigEnd,
				BigDataID:      seg.BigDataID,
				UpdateTime:     now,
				CreateTime:     now,
				IsAllowRelease: entity.AllowRelease,
				AttrLabels:     parseAttrLabels2Json(mapDocID2AttrLabels, seg.DocID),
				ExpireTime:     seg.ExpireEnd,
			})
		}
		finish <- nil
	}()
	for _, doc := range releaseDocs {
		if err := l.getDocReleaseSegments(ctx, appBizID, doc, releaseSegmentChan, mapDocID2AttrLabels); err != nil {
			return nil, err
		}
	}
	close(releaseSegmentChan)
	<-finish
	return releaseSegments, nil
}

func (l *Logic) getDocReleaseSegments(ctx context.Context, appBizID uint64, doc *releaseEntity.ReleaseDoc,
	releaseSegmentChan chan *segEntity.DocSegmentExtend, mapDocID2AttrLabels *sync.Map) error {
	logx.I(ctx, "getDocReleaseSegments|start, docID:%d, robotID:%d", doc.DocID, doc.RobotID)
	if err := l.getReleaseDocAttrLabels(ctx, mapDocID2AttrLabels, doc.RobotID, []uint64{doc.DocID}); err != nil {
		return err
	}
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()

	db, err := knowClient.GormClient(ctx, docSegmentTableName, doc.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GormClient error: %v", err)
		return err
	}

	orgDB, err := knowClient.GormClient(ctx, l.segDao.TdsqlQuery().TDocSegmentOrgDatum.TableName(), doc.RobotID, appBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GormClient error: %v", err)
		return err
	}

	total, err := l.segDao.GetReleaseSegmentCount(ctx, doc.DocID, doc.RobotID, db)
	if err != nil {
		return err
	}
	pageSize := config.App().ReleaseParamConfig.GetReleasingRecordSize
	if pageSize == 0 {
		pageSize = 1000
	}
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	orgDataBizIDs := make([]uint64, 0)
	segments := make([]*segEntity.DocSegmentExtend, 0)
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		list, err := l.segDao.GetReleaseSegmentList(ctx, doc.DocID, page, uint32(pageSize), doc.RobotID, db)
		if err != nil {
			return err
		}

		for _, v := range list {
			if v.OrgDataBizID == 0 {
				continue
			}
			orgDataBizIDs = append(orgDataBizIDs, v.OrgDataBizID)
		}
		segments = append(segments, list...)
	}
	orgDataBizIDs = slicex.Unique(orgDataBizIDs)
	logx.I(ctx, "getDocReleaseSegments|orgDataBizIDs, docID:%d, len:%d", doc.DocID, len(orgDataBizIDs))
	orgDataChunks := slicex.Chunk(orgDataBizIDs, 1000)

	mapOrgData := make(map[uint64]string)

	for _, orgDataChunk := range orgDataChunks {
		orgDataFiler := &segEntity.DocSegmentOrgDataFilter{
			AppBizID:    appBizID,
			DocBizID:    doc.BusinessID,
			BusinessIDs: orgDataChunk,
		}
		orgDataList, err := l.segDao.GetDocOrgDataList(ctx, []string{}, orgDataFiler, orgDB)
		if err != nil {
			logx.E(ctx, "GetDocOrgDataList error: %v", err)
			return err
		}
		for _, v := range orgDataList {
			mapOrgData[v.BusinessID] = v.OrgData
		}

	}

	for _, item := range segments {
		orgData, ok := mapOrgData[item.OrgDataBizID]
		if ok {
			item.OrgData = orgData
		}
		// TODO 目前暂时只需要管结束时间，等以后扩展生效时间，还需要再复制
		item.ExpireEnd = doc.ExpireTime
		releaseSegmentChan <- item
	}
	return nil
}

// GetReleaseSegmentIDs 获取本次待发布的segmentID 区分非删除和删除的
func (l *Logic) GetReleaseSegmentIDs(ctx context.Context, robotID uint64, versionID uint64) (
	notDeletedSegmentIDs []uint64, deletedSegmentIDs []uint64, err error) {
	notDeletedSegmentIDs, err = l.getAllSegmentIDWithDeleteFlag(ctx, robotID, versionID, false)
	if err != nil {
		return nil, nil, err
	}
	deletedSegmentIDs, err = l.getAllSegmentIDWithDeleteFlag(ctx, robotID, versionID, true)
	if err != nil {
		logx.E(ctx, "GetReleaseSegmentIDs error: %v", err)
		return nil, nil, err
	}
	return notDeletedSegmentIDs, deletedSegmentIDs, nil
}

// GetAllSegmentIDWithDeleteFlag 查询删除或非删除的segment_id
func (l *Logic) getAllSegmentIDWithDeleteFlag(ctx context.Context, robotID uint64, versionID uint64, isDeleted bool) ([]uint64, error) {

	limit := config.App().ReleaseParamConfig.GetIDsChunkSize
	segmentID := uint64(0)
	var allIDs []uint64

	isAllowRelease := entity.AllowRelease
	delFlag := uint32(2)

	for {
		listReq := &releaseEntity.ListReleaseSegmentReq{
			RobotID:        robotID,
			VersionID:      versionID,
			IsAllowRelease: &isAllowRelease,
			PageSize:       limit,
			OrderBy:        []string{"segment_id"},
		}

		if isDeleted {
			/*
					`SELECT segment_id FROM t_release_segment
					    	WHERE robot_id = ? AND version_id = ? AND is_allow_release = 1 AND is_deleted = 2 AND segment_id > ?
				ORDER BY segment_id ASC LIMIT ?`
			*/
			listReq.MinSegmentID = segmentID
			listReq.IsDeleted = ptrx.Uint32(delFlag)
		} else {
			/*
					`SELECT segment_id FROM t_release_segment
					    	WHERE robot_id = ? AND version_id = ? AND is_allow_release = 1 AND is_deleted != 2 AND segment_id >
				?  ORDER BY segment_id ASC LIMIT ?`
			*/
			listReq.MinSegmentID = segmentID
			listReq.IsDeletedNot = ptrx.Uint32(delFlag)
		}

		list, err := l.releaseDao.GetModifySegmentList(ctx, listReq)
		if err != nil {
			return nil, err
		}

		ids := make([]uint64, 0, len(list))
		for _, item := range list {
			ids = append(ids, item.SegmentID)
		}
		allIDs = append(allIDs, ids...)

		if len(list) < int(limit) {
			break
		}
		segmentID = ids[len(ids)-1]
	}

	return allIDs, nil
}
func (l *Logic) ClearRealtimeAppResourceReleaseSegment(ctx context.Context, removeTime int64) error {
	logx.I(ctx, "clearRealtimeAppResourceReleaseSegment,removeTime:%v", removeTime)
	dbClients := knowClient.GetAllGormClients(ctx, "t_release_segment", []client.Option{}...)
	for _, db := range dbClients {
		err := l.releaseDao.ClearRealtimeAppResourceReleaseSegment(ctx, removeTime, db)
		if err != nil {
			return err
		}
	}
	return nil

}

func (l *Logic) GetModifySegmentCount(ctx context.Context, robotID, versionID uint64, action uint32, releaseStatuses []uint32) (uint64, error) {
	return l.releaseDao.GetModifySegmentCount(ctx, robotID, versionID, action, releaseStatuses)
}

func (l *Logic) releaseSegmentNotify(ctx context.Context, isSuccess bool, reason string, robotID uint64, ids []uint64,
	release *releaseEntity.Release) error {
	segFilter := &segEntity.DocSegmentFilter{
		RobotId: robotID,
		IDs:     ids,
	}
	segments, err := l.segDao.GetDocSegmentListWithTx(ctx, segEntity.DocSegmentTblColList, segFilter, nil)
	if err != nil {
		return err
	}

	segmentExtends := make([]*segEntity.DocSegmentExtend, 0, len(segments))
	for _, segment := range segments {
		segmentExtends = append(segmentExtends, &segEntity.DocSegmentExtend{
			DocSegment: *segment,
		})
	}

	modifySegments, err := l.releaseDao.GetReleaseModifySegment(ctx, release, segmentExtends)
	if err != nil {
		return err
	}
	releaseSegmentItems := []*ReleaseSegmentItem{}
	finalReleaseStatus := util.When(isSuccess, segEntity.SegmentReleaseStatusSuccess, segEntity.SegmentReleaseStatusFail)
	for _, segment := range segmentExtends {
		modifySeg, ok := modifySegments[segment.ID]
		if !ok {
			logx.D(ctx, "当前版本没有修改这个文档分片 %v", segment)
			continue
		}
		if !segment.IsSegmentForIndex() && !segment.IsSegmentForQAAndIndex() {
			continue
		}
		segment.ReleaseStatus = finalReleaseStatus
		segment.Message = reason
		segment.NextAction = segEntity.SegNextActionPublish
		segment.UpdateTime = time.Now()
		modifySeg.ReleaseStatus = segment.ReleaseStatus
		modifySeg.Message = segment.Message
		modifySeg.Status = segment.Status
		modifySeg.UpdateTime = segment.UpdateTime

		releaseSegmentItems = append(releaseSegmentItems, &ReleaseSegmentItem{
			Segment:        segment,
			ReleaseSegment: modifySeg,
		})

	}

	gox.GoWithContext(ctx, func(ctx context.Context) {
		if err := l.PublishSegment(ctx, robotID, releaseSegmentItems); err != nil {
			logx.E(ctx, "releaseSegmentNotify PublishSegment err:%+v", err)
		}
	})

	return nil
}

// PublishSegment 发布文档片段
func (l *Logic) PublishSegment(ctx context.Context, robotID uint64, items []*ReleaseSegmentItem) error {
	defer timeTrack(ctx, time.Now(), "PublishSegment")
	logx.I(ctx, "PublishSegment robotID:%d, %d items", robotID, len(items))
	pCtx := trpc.CloneContext(ctx)

	for _, item := range items {
		segment := item.Segment
		releaseSeg := item.ReleaseSegment
		if err := l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
			/*
				`
						UPDATE
							t_doc_segment
						SET
						    update_time = :update_time,
						    release_status = :release_status,
						    message = :message,
						    next_action = :next_action
						WHERE
						    robot_id = :robot_id AND id = :id
					`
			*/

			segTbl := tx.TDocSegment

			updateSegColumns := map[string]any{segTbl.UpdateTime.ColumnName().String(): time.Now(),
				segTbl.ReleaseStatus.ColumnName().String(): segment.ReleaseStatus,
				segTbl.Message.ColumnName().String():       segment.Message,
				segTbl.NextAction.ColumnName().String():    segment.NextAction}

			filter := &segEntity.DocSegmentFilter{
				RobotId: robotID,
				ID:      segment.ID,
			}
			if err := l.segDao.BatchUpdateDocSegmentByFilter(ctx, filter, updateSegColumns,
				segTbl.WithContext(pCtx).UnderlyingDB()); err != nil {
				logx.E(pCtx, "Failed to publish segment err:%+v", err)
				return err
			}

			/*
				 `
					UPDATE
						t_release_segment
					SET
					    update_time = :update_time,
					    release_status = :release_status,
					    message = :message
					WHERE
					    robot_id = :robot_id AND id = :id
			*/

			rsegTbl := tx.TReleaseSegment
			updateSegColumns = map[string]any{rsegTbl.UpdateTime.ColumnName().String(): time.Now(),
				rsegTbl.ReleaseStatus.ColumnName().String(): releaseSeg.ReleaseStatus,
				rsegTbl.Message.ColumnName().String():       releaseSeg.Message}

			rFilter := &releaseEntity.ReleaseSegmentFilter{
				RobotID: robotID,
				ID:      releaseSeg.ID,
			}

			if rows, err := l.releaseDao.BatchUpdateReleaseSegmentRecords(pCtx, updateSegColumns, rFilter,
				rsegTbl.WithContext(pCtx).UnderlyingDB()); err != nil {
				logx.E(pCtx, "Failed to update release segment err:%+v", err)
				return err
			} else {
				logx.I(pCtx, "update release segment rows:%d", rows)
			}

			return nil

		}); err != nil {
			logx.E(pCtx, "Failed to PublishSegment. err:%+v (segment:%+v, releaseSeg:%+v)", err, releaseSeg, segment)
			continue
		}

	}
	return nil
}
