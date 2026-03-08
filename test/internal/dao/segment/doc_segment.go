package segment

import (
	"context"
	"errors"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gen"
	"gorm.io/gorm"
)

func (d *daoImpl) CreateDocSegments(ctx context.Context, docSegments []*segEntity.DocSegment, tx *gorm.DB) error {
	logx.I(ctx, "CreateDocSegments|%d docSegments", len(docSegments))
	if len(docSegments) == 0 {
		logx.I(ctx, "no doc segment record to create")
		return nil
	}
	tbl := d.Query().TDocSegment
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	db = db.Table(tbl.TableName())
	tSegs := BatchConvertDocSegementDO2PO(docSegments)
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, v := range tSegs {
			if err := tx.Create(v).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "CreateDocSegments failed, error:%v", err)
		return err
	}
	logx.I(ctx, "CreateDocSegments success")
	for i, v := range docSegments {
		v.ID = uint64(tSegs[i].ID)
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateSegmentCondition(ctx context.Context, session *gorm.DB, filter *segEntity.DocSegmentFilter) *gorm.DB {
	tbl := d.Query().TDocSegment
	if filter.CorpID != 0 {
		session = session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, filter.CorpID)
	}
	if filter.AppID != 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.AppID)
	}
	if filter.DocID != 0 {
		session = session.Where(tbl.DocID.ColumnName().String()+util.SqlEqual, filter.DocID)
	}
	if filter.ID != 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlEqual, filter.ID)
	}
	if len(filter.IDs) != 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlIn, filter.IDs)
	}
	if filter.StaffID > 0 {
		session = session.Where(tbl.StaffID.ColumnName().String()+util.SqlEqual, filter.StaffID)
	}
	if len(filter.BusinessIDs) != 0 {
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, filter.BusinessIDs)
	}
	if filter.Type != 0 {
		session = session.Where(tbl.Type.ColumnName().String()+util.SqlEqual, filter.Type)
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}
	if filter.SegmentType != "" {
		session = session.Where(tbl.SegmentType.ColumnName().String()+util.SqlEqual, filter.SegmentType)
	}

	if filter.BatchID != 0 {
		session = session.Where(tbl.BatchID.ColumnName().String()+util.SqlEqual, filter.BatchID)
	}

	if filter.RobotId != 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.RobotId)
	}

	if filter.StatusNot != 0 {
		session = session.Where(tbl.Status.ColumnName().String()+util.SqlNotEqual, filter.StatusNot)
	}

	if len(filter.ReleaseStatus) != 0 {
		session = session.Where(tbl.ReleaseStatus.ColumnName().String()+util.SqlIn, filter.ReleaseStatus)
	}
	if filter.ExtraCondition != "" {
		if len(filter.ExtraParams) > 0 {
			session = session.Where(filter.ExtraCondition, filter.ExtraParams...)
		} else {
			session = session.Where(filter.ExtraCondition)
		}
	}
	return session
}

func (d *daoImpl) BatchUpdateDocSegmentByFilter(ctx context.Context, filter *segEntity.DocSegmentFilter,
	updateColumns map[string]any, tx *gorm.DB) error {
	if filter == nil {
		logx.I(ctx, "no doc segment record to update")
		return nil
	}

	tbl := d.Query().TDocSegment

	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB().Table(tbl.TableName())
	}

	db = db.Table(tbl.TableName())

	db = d.generateSegmentCondition(ctx, db, filter)

	if err := db.Updates(updateColumns).Error; err != nil {
		logx.E(ctx, "BatchUpdateDocSegmentByFilter failed,  error:%v", err)
		return err
	}

	logx.I(ctx, "BatchUpdateDocSegmentByFilter success")
	return nil
}

func (d *daoImpl) BatchUpdateDocSegmentLastActionByID(ctx context.Context,
	docSegmentIds []uint64, action uint32) error {
	tbl := d.Query().TDocSegment
	db := tbl.WithContext(ctx).Debug()
	if len(docSegmentIds) == 0 {
		logx.I(ctx, "no doc segment record to update")
		return nil
	}

	dDocSegIds := []int64{}
	for _, v := range docSegmentIds {
		dDocSegIds = append(dDocSegIds, int64(v))
	}

	queryCond := []gen.Condition{
		tbl.ID.In(dDocSegIds...),
	}

	info, err := db.Where(queryCond...).
		Updates(map[string]any{
			tbl.NextAction.ColumnName().String(): action,
			tbl.UpdateTime.ColumnName().String(): time.Now(),
		})

	if err != nil {
		logx.E(ctx, "BatchUpdateDocSegmentLastActionByID failed,  error:%v", err)
		return err
	}

	logx.I(ctx, "BatchUpdateDocSegmentLastActionByID success, info:%+v", info)
	return nil
}

func (d *daoImpl) BatchUpdateDocSegmentsWithTx(ctx context.Context, updateColumns []string, docSegments []*segEntity.DocSegment, db *gorm.DB) error {
	if len(docSegments) == 0 {
		logx.I(ctx, "no doc segment record to update")
		return nil
	}
	logx.I(ctx, "BatchUpdateDocSegmentsWithTx|updateColumns:%+v,%d docSegments", updateColumns, len(docSegments))
	session := db
	if session == nil {
		session = d.mysql.TDocSegment.WithContext(ctx).UnderlyingDB()
	}

	docSegDos := BatchConvertDocSegementDO2PO(docSegments)

	if err := session.Transaction(func(tx *gorm.DB) error {
		for _, v := range docSegDos {
			newTx := tx.Table(model.TableNameTDocSegment)
			if len(updateColumns) > 0 {
				newTx = newTx.Select(updateColumns)
			}
			newTx = newTx.Where("id = ?", v.ID)
			if err := newTx.Updates(v).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "BatchUpdateDocSegment failed, error:%v", err)
		return err
	}

	return nil
}

func (d *daoImpl) UpdateDocSegmentWithTx(ctx context.Context, updateColumns []string, filter *segEntity.DocSegmentFilter,
	docSegment *segEntity.DocSegment, db *gorm.DB) error {
	session := db
	if session == nil {
		session = d.mysql.TDocSegment.WithContext(ctx).Debug().UnderlyingDB()

	}
	tDocSeg := ConvertDocSegementDO2PO(docSegment)
	if len(updateColumns) > 0 {
		session = session.Select(updateColumns)
	}
	db = d.generateSegmentCondition(ctx, session, filter)
	if err := session.Updates(tDocSeg).Error; err != nil {
		logx.E(ctx, "UpdateDocSegmentWithTx failed, error:%v", err)
		return err
	}
	return nil

}

func (d *daoImpl) GetDocSegmentByID(ctx context.Context, robotID, docSegmentID uint64) (
	*segEntity.DocSegmentExtend, error) {
	tbl := d.Query().TDocSegment
	db := tbl.WithContext(ctx).Debug()
	if robotID != 0 {
		db = db.Where(tbl.RobotID.Eq(uint64(robotID)))
	}

	if docSegmentID != 0 {
		db = db.Where(tbl.ID.Eq(int64(docSegmentID)))
	}

	do, err := db.Take()
	if err != nil {
		logx.E(ctx, "GetDocSegmentByID failed, error:%v", err)
		return nil, err
	}
	docSegEntity := ConvertDocSegementPO2DO(do)
	seg := &segEntity.DocSegmentExtend{
		DocSegment: *docSegEntity,
	}
	return seg, nil
}

// GetDocSegmentListWithTx 获取文档片段列表
func (d *daoImpl) GetDocSegmentListWithTx(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentFilter, tx *gorm.DB) ([]*segEntity.DocSegment, error) {
	tbl := d.Query().TDocSegment
	tableName := tbl.TableName()
	DocSegmentList := make([]*model.TDocSegment, 0)

	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(tableName)

	if len(selectColumns) > 0 {
		session = session.Select(selectColumns)
	}
	session = d.generateSegmentCondition(ctx, session, filter)
	if filter.Limit > 0 {
		if filter.Limit > util.DefaultMaxPageSize {
			// 限制单次查询最大条数
			filter.Limit = util.DefaultMaxPageSize
			// err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
			// logx.E(ctx, "getDocSegmentList err: %+v", err)
			// return nil, err
		}
		session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	}

	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session = session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Scan(&DocSegmentList)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegementPO2DO(DocSegmentList), nil
}

// GetDocSegmentCountWithTx 获取文档片段总数
func (d *daoImpl) GetDocSegmentCountWithTx(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentFilter, tx *gorm.DB) (int64, error) {
	tbl := d.Query().TDocSegment
	tableName := tbl.TableName()
	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.Table(tableName)
	session = d.generateSegmentCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocSegmentCountAndList 获取文档片段总数和分页列表
func (d *daoImpl) GetDocSegmentCountAndList(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentFilter) ([]*segEntity.DocSegment, int64, error) {
	count, err := d.GetDocSegmentCountWithTx(ctx, selectColumns, filter, nil)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocSegmentListWithTx(ctx, selectColumns, filter, nil)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocSegmentList 获取所有文档片段列表，使用logic层中GetDocSegmentList方法，可同时获取到org_data
func (d *daoImpl) BatchGetDocSegmentList(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentFilter, db *gorm.DB) ([]*segEntity.DocSegment, error) {
	logx.I(ctx, "BatchGetDocSegmentList|selectColumns:%+v,filter:%+v", selectColumns, filter)
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	allDocSegmentList := make([]*segEntity.DocSegment, 0)

	for {
		filter.Offset = offset
		filter.Limit = int(util.CalculateLimit(wantedCount, alreadyGetCount))
		if filter.Limit == 0 {
			break
		}
		DocSegmentList, err := d.GetDocSegmentListWithTx(ctx, selectColumns, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocSegmentList failed, err: %+v", err)
			return nil, err
		}
		allDocSegmentList = append(allDocSegmentList, DocSegmentList...)
		if len(DocSegmentList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.I(ctx, "GetDocSegmentList count:%d cost:%dms",
		len(allDocSegmentList), time.Since(beginTime).Milliseconds())
	return allDocSegmentList, nil
}

func (d *daoImpl) GetDocSegmentByFilter(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentFilter, tx *gorm.DB) (*segEntity.DocSegment, error) {
	tbl := d.Query().TDocSegment
	tableName := tbl.TableName()

	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	if len(selectColumns) > 0 {
		session = session.Select(selectColumns)
	}
	session = d.generateSegmentCondition(ctx, session, filter)

	res := &model.TDocSegment{}

	err := session.Table(tableName).First(res).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		logx.E(ctx, "execute sql failed, err: %+v", err)
		return nil, err
	}
	return ConvertDocSegementPO2DO(res), nil
}

// GetReleaseSegmentCount 获取发布文档分片总数
func (d *daoImpl) GetReleaseSegmentCount(ctx context.Context, docID uint64, robotID uint64, tx *gorm.DB) (uint64, error) {
	/*
		`
			SELECT
				count(*)
			FROM
			    t_doc_segment
			WHERE
				doc_id = ? AND release_status = ?
		`
	*/
	docSegFilter := &segEntity.DocSegmentFilter{
		DocID:         docID,
		ReleaseStatus: []uint32{segEntity.SegmentReleaseStatusInit},
	}

	tbl := d.Query().TDocSegment
	tableName := tbl.TableName()

	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(tableName)

	session = d.generateSegmentCondition(ctx, session, docSegFilter)

	var count int64
	err := session.Count(&count).Error
	if err != nil {
		logx.E(ctx, "Failed to GetReleaseSegmentCount, err: %+v", err)
		return 0, err
	}
	return uint64(count), nil
}

// GetReleaseSegmentList 获取发布文档分片列表
func (d *daoImpl) GetReleaseSegmentList(ctx context.Context, docID uint64, page, pageSize uint32, robotID uint64, tx *gorm.DB) (
	[]*segEntity.DocSegmentExtend, error) {

	docSegFilter := &segEntity.DocSegmentFilter{
		DocID:         docID,
		ReleaseStatus: []uint32{segEntity.SegmentReleaseStatusInit},
	}

	tbl := d.Query().TDocSegment
	tableName := tbl.TableName()

	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.Table(tableName)

	session = d.generateSegmentCondition(ctx, session, docSegFilter)

	offset, limit := utilx.Page(page, pageSize)

	tDocSegs := make([]*model.TDocSegment, 0)

	err := session.Offset(offset).Limit(limit).Find(&tDocSegs).Error
	if err != nil {
		logx.E(ctx, "Failed to GetReleaseSegmentList, err: %+v", err)
		return nil, err
	}
	docSegs := make([]*segEntity.DocSegmentExtend, 0, len(tDocSegs))
	for _, tDocSeg := range tDocSegs {
		s := ConvertDocSegementPO2DO(tDocSeg)
		docSegs = append(docSegs, &segEntity.DocSegmentExtend{
			DocSegment: *s,
		})
	}

	return docSegs, nil
}

// GetSegmentByDocID 批量获取单文档未删除的segment
func (d *daoImpl) GetSegmentByDocID(ctx context.Context, robotID, docID, startID, count uint64, selectColumns []string, tx *gorm.DB) (
	[]*segEntity.DocSegmentExtend, uint64, error) {
	tbl := d.Query().TDocSegment
	tableName := tbl.TableName()
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	segs, limit, lastID := make([]*segEntity.DocSegmentExtend, 0, count), 500, startID
	for {
		var tSegs []*model.TDocSegment
		err := db.WithContext(ctx).Table(tableName).Select(selectColumns).
			Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, segEntity.SegmentIsNotDeleted).
			Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
			Where(tbl.DocID.ColumnName().String()+util.SqlEqual, docID).
			Where(tbl.ID.ColumnName().String()+util.SqlMore, lastID). // 避免深分页
			Limit(limit).Find(&tSegs).Error
		if err != nil {
			logx.E(ctx, "GetSegmentByDocID get err:%v,robotID:%v,docID:%v", err, robotID, docID)
			return nil, 0, err
		}
		if len(tSegs) != 0 {
			for _, tSeg := range tSegs {
				seg := ConvertDocSegementPO2DO(tSeg)
				segs = append(segs, &segEntity.DocSegmentExtend{
					DocSegment: *seg,
				})
			}

			lastID = uint64(tSegs[len(tSegs)-1].ID)

		}
		if len(tSegs) < int(limit) || len(segs) >= int(count) {
			break
		}
	}
	return segs, lastID, nil
}
