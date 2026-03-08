package release

import (
	"context"
	"math"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"gorm.io/gorm"
)

func (l *Logic) GetReleaseDocByVersion(ctx context.Context, release *releaseEntity.Release) ([]*releaseEntity.ReleaseDoc, error) {
	releaseDocs := make([]*releaseEntity.ReleaseDoc, 0)
	total, err := l.GetModifyDocCount(ctx, release.RobotID, release.ID, "", nil, nil)
	if err != nil {
		return nil, err
	}
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for page := 1; page <= pages; page++ {
		modifyDocs, err := l.GetModifyDocList(ctx, release.RobotID, release.ID, "", nil, uint32(page),
			uint32(pageSize))
		if err != nil {
			return nil, err
		}
		releaseDocs = append(releaseDocs, modifyDocs...)
	}
	return releaseDocs, nil
}

func (l *Logic) GetModifyDocCount(ctx context.Context, robotID, versionID uint64,
	fileName string, actions []uint32, statuses []uint32) (uint64, error) {
	return l.releaseDao.GetModifyDocCount(ctx, robotID, versionID, fileName, actions, statuses)
}

func (l *Logic) GetModifyDocList(ctx context.Context, robotID, versionID uint64,
	fileName string, actions []uint32, pageNo, pageSize uint32) ([]*releaseEntity.ReleaseDoc, error) {
	req := &releaseEntity.ListReleaseDocReq{
		RobotID:   robotID,
		VersionId: versionID,
		FileName:  fileName,
		Actions:   actions,
		Page:      pageNo,
		PageSize:  pageSize,
	}
	return l.releaseDao.GetModifyDocList(ctx, req)
}

func (l *Logic) BatchCreateReleaseDocs(ctx context.Context, releaseDocs []*releaseEntity.ReleaseDoc) error {
	if err := batchReleaseProcess(ctx, releaseDocs, l.batchCreateReleaseDocUnit); err != nil {
		logx.W(ctx, "BatchCreateReleaseDocs err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) batchCreateReleaseDocUnit(ctx context.Context, releaseDoc []*releaseEntity.ReleaseDoc) error {
	defer timeTrack(ctx, time.Now(), "batchCreateReleaseDocUnit")
	for _, v := range releaseDoc {
		filter := &releaseEntity.ReleaseDocFilter{
			DocID:     v.DocID,
			VersionId: v.VersionID,
			RobotID:   v.RobotID,
		}
		isExist, err := l.releaseDao.IsExistReleaseDoc(ctx, filter)
		if err != nil {
			return err
		}
		if isExist {
			continue
		}
		if err := l.releaseDao.MysqlQuery().TReleaseDoc.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
			batchReleaseDocs := []*releaseEntity.ReleaseDoc{v}
			err := l.releaseDao.CreateReleaseDocRecords(ctx, batchReleaseDocs, tx)
			if err != nil {
				logx.E(ctx, "create release doc record error:%v", err)
				return err
			}

			docIds := []uint64{}
			for _, doc := range batchReleaseDocs {
				docIds = append(docIds, doc.DocID)
			}
			docFilter := &docEntity.DocFilter{
				IDs: docIds,
			}
			if err = l.docLogic.GetDao().BatchUpdateDocsByFilter(
				ctx, docFilter, map[string]any{
					docEntity.DocTblColStatus:     docEntity.DocStatusReleasing,
					docEntity.DocTblColUpdateTime: time.Now(),
				}, tx); err != nil {
				logx.E(ctx, "update doc status to [docEntity.DocStatusReleasing] error:%v", err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "Failed to batch create doc err:%+v", err)
			return err
		}
	}
	return nil
}

func (l *Logic) GetModifyReleaseDocs(ctx context.Context, robotID, versionID uint64,
	fileName string, actions, status []uint32) ([]*releaseEntity.ReleaseDoc, error) {
	total, err := l.releaseDao.GetModifyDocCount(ctx, robotID, versionID, fileName, actions, status)
	if err != nil {
		return nil, err
	}
	if total == 0 {
		return nil, nil
	}

	actionParams := []int{}
	for _, action := range actions {
		actionParams = append(actionParams, int(action))
	}
	listReq := &releaseEntity.ListReleaseDocReq{
		RobotID:   robotID,
		VersionId: versionID,
		FileName:  fileName,
		Actions:   nil,
		Page:      1,
		PageSize:  uint32(total),
	}

	return l.releaseDao.GetModifyDocList(ctx, listReq)
}

type ReleaseSuccessDocItem struct {
	releaseDoc *releaseEntity.ReleaseDoc
	doc        *docEntity.Doc
}

func (l *Logic) releaseDocSuccessUnit(
	ctx context.Context, items []ReleaseSuccessDocItem) error {
	if len(items) == 0 {
		return nil
	}
	now := time.Now()
	for _, item := range items {
		if item.releaseDoc.Status == docEntity.DocStatusReleaseSuccess {
			continue
		}
		if err := l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
			docDbTx := tx.TDoc.WithContext(ctx).UnderlyingDB()
			if err := l.releaseDocSuccessOfDoc(ctx, docDbTx, item.doc); err != nil {
				return err
			}
			item.releaseDoc.Status = docEntity.DocStatusReleaseSuccess
			item.releaseDoc.UpdateTime = now
			updateColumns := map[string]any{
				tx.TReleaseDoc.Status.ColumnName().String():     item.releaseDoc.Status,
				tx.TReleaseDoc.UpdateTime.ColumnName().String(): item.releaseDoc.UpdateTime,
			}
			filter := &releaseEntity.ReleaseDocFilter{
				VersionId: item.releaseDoc.VersionID,
				DocID:     item.releaseDoc.DocID,
				RobotID:   item.releaseDoc.RobotID,
			}
			releaseDocTx := tx.TReleaseDoc.WithContext(ctx).UnderlyingDB()
			if err := l.releaseDao.UpdateReleaseDocRecord(ctx, updateColumns, filter, releaseDocTx); err != nil {
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "Failed to releaseDocSuccessUnit err:%+v", err)
			return err
		}
	}
	return nil
}

func (l *Logic) releaseDocSuccessOfDoc(ctx context.Context, tx *gorm.DB, doc *docEntity.Doc) error {
	if doc == nil {
		return nil
	}
	now := time.Now()
	doc.UpdateTime = now
	if len(doc.FileNameInAudit) > 0 {
		doc.FileName = doc.FileNameInAudit
		doc.FileNameInAudit = ""
	}
	doc.Status = docEntity.DocStatusReleaseSuccess
	doc.NextAction = docEntity.DocNextActionPublish
	if doc.HasDeleted() {
		doc.NextAction = docEntity.DocNextActionAdd
	}
	updateColumns := []string{docEntity.DocTblColFileName, docEntity.DocTblColStatus,
		docEntity.DocTblColNextAction, docEntity.DocTblColUpdateTime}
	filter := &docEntity.DocFilter{
		ID: doc.ID,
	}
	if _, err := l.docLogic.GetDao().UpdateDocByTx(ctx, updateColumns, filter, doc, tx); err != nil {
		return err
	}
	return nil
}

// ReleaseDocSuccess 文档发布成功，由于vector_doc在ReleaseDetailNotify通知时，只会通知segment成败，所以在ReleaseNotify中处理文档成败
func (l *Logic) ReleaseDocSuccess(ctx context.Context, releaseDocs []*releaseEntity.ReleaseDoc,
	docs []*docEntity.Doc) error {
	logx.I(ctx, "ReleaseDocSuccess start. %d releaseDocs, %d docs", len(releaseDocs), len(docs))
	defer timeTrack(ctx, time.Now(), "ReleaseDocSuccess")
	if len(releaseDocs) == 0 || len(docs) == 0 {
		return nil
	}
	if len(releaseDocs) == 0 || len(docs) == 0 {
		return nil
	}
	mapDoc := make(map[uint64]*docEntity.Doc)
	for _, doc := range docs {
		mapDoc[doc.ID] = doc
	}
	items := make([]ReleaseSuccessDocItem, 0)
	for _, releaseDoc := range releaseDocs {
		items = append(items, ReleaseSuccessDocItem{
			releaseDoc: releaseDoc,
			doc:        mapDoc[releaseDoc.DocID],
		})
	}
	if err := batchReleaseProcess(ctx, items, l.releaseDocSuccessUnit); err != nil {
		logx.W(ctx, "releaseDocSuccess err :%v", err)
		return err
	}
	return nil
}

// UpdateDocWaitRelease 更新文档状态到待发布
func (l *Logic) UpdateDocWaitRelease(ctx context.Context, appID, docID uint64, segIDs []uint64) error {
	tbl := l.docLogic.GetDao().Query().TDoc
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	// 开启事务
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	docFilter := &docEntity.DocFilter{
		RobotId: appID,
		IDs:     []uint64{docID},
		Limit:   1,
	}

	err = l.docLogic.GetDao().BatchUpdateDocsByFilter(ctx, docFilter, map[string]any{
		"status":      docEntity.DocStatusWaitRelease,
		"next_action": docEntity.DocNextActionUpdate,
		"update_time": time.Now(),
	}, tx)

	if err != nil {
		logx.E(ctx, "UpdateDocWaitRelease failed doc:%v, err: %+v", docID, err)
		return err
	}

	// 2.更新文档切片的状态
	if len(segIDs) > 0 {
		limit := config.GetMainConfig().Permissions.ChunkNumber
		if limit == 0 {
			limit = 3000
		}
		for _, segChunks := range slicex.Chunk(segIDs, limit) {
			segFilter := &segEntity.DocSegmentFilter{
				RobotId: appID,
				IDs:     segChunks,
			}
			err = l.segDao.BatchUpdateDocSegmentByFilter(ctx, segFilter, map[string]any{
				"release_status": segEntity.SegmentReleaseStatusInit,
				"next_action":    qaEntity.NextActionUpdate,
				"update_time":    time.Now(),
			}, tx)
			if err != nil {
				logx.E(ctx, "UpdateDocWaitRelease failed segIDS:%v,docID:%v,err: %+v", segIDs, docID, err)
				return err
			}
		}
	}
	return nil
}

// GetDocReleaseCount 获取文档未发布状态总数
func (l *Logic) GetDocReleaseCount(ctx context.Context, corpID, robotID uint64) (int64, error) {
	tbl := l.docLogic.GetDao().Query().TDoc
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	filter := &docEntity.DocFilter{
		CorpId:  corpID,
		RobotId: robotID,
		Status: []uint32{docEntity.DocStatusWaitRelease, docEntity.DocStatusCreatingIndex, docEntity.DocStatusUpdating,
			docEntity.DocStatusParseIng, docEntity.DocStatusAuditIng, docEntity.DocStatusUnderAppeal},
		Opts:      []uint32{docEntity.DocOptDocImport},
		IsDeleted: ptrx.Bool(false),
	}
	count, err := l.docLogic.GetDao().GetDocCountWithFilter(ctx, []string{docEntity.DocTblColId}, filter, db)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (l *Logic) GetReleaseDoc(ctx context.Context, release *releaseEntity.Release) (map[uint64]struct{}, error) {
	docID := make(map[uint64]struct{}, 0)
	list, err := l.releaseDao.GetDocIDInReleaseDocQAs(ctx, release)
	if err != nil {
		return nil, err
	}
	for _, id := range list {
		docID[id] = struct{}{}
	}

	list, err = l.releaseDao.GetDocIDInReleaseDocSegements(ctx, release)
	if err != nil {
		return nil, err
	}
	for _, id := range list {
		docID[id] = struct{}{}
	}

	return docID, nil
}

func (l *Logic) GetDocReleasedList(ctx context.Context, robotID uint64,
	releaseDoc []*releaseEntity.ReleaseDoc) ([]*docEntity.Doc, error) {
	if len(releaseDoc) == 0 {
		return nil, nil
	}
	docIDs := make([]uint64, 0, len(releaseDoc))
	for _, doc := range releaseDoc {
		docIDs = append(docIDs, doc.DocID)
	}
	docs, err := l.docLogic.GetDao().GetDocByIDs(ctx, docIDs, robotID)
	if err != nil {
		return nil, err
	}
	list := make([]*docEntity.Doc, 0, len(docs))
	for _, doc := range docs {
		list = append(list, doc)
	}
	return list, nil
}

// getReleaseDocs 获取发布的文档
func (l *Logic) getReleaseDocs(ctx context.Context, corpID, robotID, versionID uint64) (
	[]*releaseEntity.ReleaseDoc, error) {
	zeroTime := time.Time{}
	total, err := l.docLogic.GetWaitReleaseDocCount(ctx, corpID, robotID, "", zeroTime, zeroTime, nil)
	if err != nil {
		return nil, err
	}
	docs, err := l.docLogic.GetWaitReleaseDoc(ctx, corpID, robotID, "", zeroTime, zeroTime, nil, 1, uint32(total))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	releaseDocs := make([]*releaseEntity.ReleaseDoc, 0, len(docs))
	for i := range docs {
		doc := docs[i]
		newReleaseDoc := &releaseEntity.ReleaseDoc{
			VersionID:       versionID,
			DocID:           doc.ID,
			BusinessID:      doc.BusinessID,
			RobotID:         doc.RobotID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			FileName:        doc.FileName,
			FileType:        doc.FileType,
			FileSize:        doc.FileSize,
			Bucket:          doc.Bucket,
			CosURL:          doc.CosURL,
			CosHash:         doc.CosHash,
			Message:         doc.Message,
			Status:          doc.Status,
			IsDeleted:       doc.IsDeleted,
			IsRefer:         doc.IsRefer,
			Source:          doc.Source,
			WebURL:          doc.WebURL,
			BatchID:         doc.BatchID,
			AuditFlag:       doc.AuditFlag,
			IsCreatingQA:    doc.IsCreatingQaV1(),
			IsCreatingIndex: doc.IsCreatingIndexV1(),
			Action:          doc.NextAction,
			AttrRange:       doc.AttrRange,
			CreateTime:      now,
			UpdateTime:      now,
			ExpireTime:      doc.ExpireEnd,
		}
		releaseDocs = append(releaseDocs, newReleaseDoc)
	}
	return releaseDocs, nil
}
