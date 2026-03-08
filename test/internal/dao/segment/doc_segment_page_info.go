package segment

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"

	"gorm.io/gorm"
)

func (d *daoImpl) CreateDocSegmentPageInfos(ctx context.Context, docSegPaegInfos []*segEntity.DocSegmentPageInfo, tx *gorm.DB) error {
	/*
		`
			INSERT INTO
			    t_doc_segment_page_info (%s)
			VALUES
			    (null,:page_info_id,:segment_id,:doc_id,:robot_id,:corp_id,:staff_id,:org_page_numbers,:big_page_numbers,
			     :sheet_data,:is_deleted,:create_time,:update_time)
		`
	*/
	tbl := d.Query().TDocSegmentPageInfo
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	tSegs := BatchConvertDocSegmentPageInfoDO2PO(docSegPaegInfos)
	if err := db.Table(tbl.TableName()).CreateInBatches(&tSegs, 100).Error; err != nil {
		logx.E(ctx, "CreateDocSegmentPageInfos failed, error:%v", err)
		return err
	}
	logx.I(ctx, "CreateDocSegmentPageInfos success")
	for i, v := range docSegPaegInfos {
		v.ID = uint64(tSegs[i].ID)
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateSegmentPageCondition(ctx context.Context, session *gorm.DB, filter *entity.DocSegmentPageInfoFilter) *gorm.DB {
	tbl := d.Query().TDocSegmentPageInfo
	if len(filter.IDs) != 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlIn, filter.IDs)
	}
	if filter.CorpID != 0 {
		session = session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, filter.CorpID)
	}
	if filter.AppID != 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.AppID)
	}
	if filter.DocID != 0 {
		session = session.Where(tbl.DocID.ColumnName().String()+util.SqlEqual, filter.DocID)
	}
	if filter.SegmentID > 0 {
		session = session.Where(tbl.SegmentID.ColumnName().String()+util.SqlEqual, filter.SegmentID)
	}
	if len(filter.SegmentIDs) != 0 {
		session = session.Where(tbl.SegmentID.ColumnName().String()+util.SqlIn, filter.SegmentIDs)
	}
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}
	return session
}

func (d *daoImpl) BatchUpdateDocSegmentPageInfos(ctx context.Context, filter *segEntity.DocSegmentPageInfoFilter,
	updateColumns map[string]any, tx *gorm.DB) error {
	tbl := d.Query().TDocSegmentPageInfo
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	session := db.Table(tbl.TableName())
	session = d.generateSegmentPageCondition(ctx, session, filter)
	res := session.Updates(updateColumns)
	if res.Error != nil {
		logx.E(ctx, "BatchUpdateDocSegmentPageInfos failed, error:%v", res.Error)
		return res.Error
	}
	logx.I(ctx, "BatchUpdateDocSegmentPageInfos success")
	return nil
}

// getDocSegmentPageInfoList 获取文档片段页码列表
func (d *daoImpl) GetDocSegmentPageInfoListWithTx(ctx context.Context, selectColumns []string,
	filter *entity.DocSegmentPageInfoFilter, tx *gorm.DB) ([]*entity.DocSegmentPageInfo, error) {

	DocSegmentPageInfoList := make([]*model.TDocSegmentPageInfo, 0)
	tbl := d.Query().TDocSegmentPageInfo
	tableName := tbl.TableName()

	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()

	}

	session = session.Table(tableName)
	if len(selectColumns) != 0 {
		session = session.Select(selectColumns)
	}
	if filter.Offset != 0 || filter.Limit != 0 {
		session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	}
	session = d.generateSegmentPageCondition(ctx, session, filter)
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session = session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&DocSegmentPageInfoList)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentPageInfoPO2DO(DocSegmentPageInfoList), nil
}

// GetDocSegmentPageInfoList 获取所有文档片段页码信息
func (d *daoImpl) BatchGetDocSegmentPageInfoList(ctx context.Context, selectColumns []string,
	filter *entity.DocSegmentPageInfoFilter, db *gorm.DB) ([]*entity.DocSegmentPageInfo, error) {
	tbl := d.Query().TDocSegmentPageInfo
	tableName := tbl.TableName()
	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB().Table(tableName)
	}
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	allDocSegmentPageInfoList := make([]*entity.DocSegmentPageInfo, 0)
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)

		if filter.Limit == 0 {
			// 为0正常返回空结果即可
			break
		}
		if filter.Limit > util.DefaultMaxPageSize {
			// 限制单次查询最大条数
			// err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
			// logx.E(ctx, "getDocSegmentPageInfoList err: %+v", err)
			// return nil, err
			filter.Limit = util.DefaultMaxPageSize
			logx.I(ctx, "getDocSegmentPageInfoList limit: %d", filter.Limit)
			logx.I(ctx, "getDocSegmentPageInfoList wantedCount: %d", wantedCount)
		}
		DocSegmentPageInfoList, err := d.GetDocSegmentPageInfoListWithTx(ctx, selectColumns, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocSegmentPageInfoList failed, err: %+v", err)
			return nil, err
		}
		allDocSegmentPageInfoList = append(allDocSegmentPageInfoList, DocSegmentPageInfoList...)
		if len(DocSegmentPageInfoList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocSegmentPageInfoList count:%d cost:%dms",
		len(allDocSegmentPageInfoList), time.Since(beginTime).Milliseconds())
	return allDocSegmentPageInfoList, nil
}

// GetDocSegmentPageInfosByCursor 游标分页获取文档切片页码信息（用于导出）
func (d *daoImpl) GetDocSegmentPageInfosByCursor(ctx context.Context, robotID, robotBizID, docID uint64, lastID uint64, limit int) ([]*segEntity.DocSegmentPageInfo, error) {
	tbl := d.Query().TDocSegmentPageInfo
	tableName := tbl.TableName()

	db, err := knowClient.GormClient(ctx, tableName, robotID, robotBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocSegmentPageInfosByCursor get GormClient failed, err: %v", err)
		return nil, err
	}

	var tPageInfos []*model.TDocSegmentPageInfo
	err = db.WithContext(ctx).Table(tableName).
		Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
		Where(tbl.DocID.ColumnName().String()+util.SqlEqual, docID).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, segEntity.SegmentIsNotDeleted).
		Where(tbl.ID.ColumnName().String()+util.SqlMore, lastID).
		Order(tbl.ID.ColumnName().String() + " " + util.SqlOrderByAsc).
		Limit(limit).
		Find(&tPageInfos).Error
	if err != nil {
		logx.E(ctx, "GetDocSegmentPageInfosByCursor query failed, err: %v", err)
		return nil, err
	}

	return BatchConvertDocSegmentPageInfoPO2DO(tPageInfos), nil
}
