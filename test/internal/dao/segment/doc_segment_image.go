package segment

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
)

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateSegmentImageCondition(ctx context.Context, session *gorm.DB, filter *entity.DocSegmentImageFilter) *gorm.DB {
	tbl := d.Query().TDocSegmentImage
	if len(filter.IDs) != 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlIn, filter.IDs)
	}
	if filter.AppID != 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.AppID)
	}
	if filter.CorpID != 0 {
		session = session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, filter.CorpID)
	}
	if filter.DocID != 0 {
		session = session.Where(tbl.DocID.ColumnName().String()+util.SqlEqual, filter.DocID)
	}
	if filter.SegmentID != 0 {
		session = session.Where(tbl.SegmentID.ColumnName().String()+util.SqlEqual, filter.SegmentID)
	}
	if len(filter.SegmentIDs) != 0 {
		session = session.Where(tbl.SegmentID.ColumnName().String()+util.SqlIn, filter.SegmentIDs)
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

func (d *daoImpl) CreateDocSegmentImages(ctx context.Context, docSegImages []*segEntity.DocSegmentImage, tx *gorm.DB) error {
	/*
		`
				INSERT INTO
				    t_doc_segment_image (%s)
				VALUES
				    (null,:image_id,:segment_id,:doc_id,:robot_id,:corp_id,:staff_id,:original_url,:external_url,
				     :is_deleted,:create_time,:update_time)
			`
	*/
	tbl := d.Query().TDocSegmentImage
	db := tx
	if db == nil {
		db = tbl.WithContext(ctx).UnderlyingDB()
	}
	db = db.Table(tbl.TableName())
	tSegs := BatchConvertDocSegmentImageDO2PO(docSegImages)
	if err := db.CreateInBatches(&tSegs, 100).Error; err != nil {
		logx.E(ctx, "CreateDocSegmentImages failed, error:%v", err)
		return err
	}
	logx.I(ctx, "CreateDocSegmentImages success")
	for i, v := range docSegImages {
		v.ID = uint64(tSegs[i].ID)
	}
	return nil
}

func (d *daoImpl) BatchUpdateDocSegmentImages(ctx context.Context, filter *segEntity.DocSegmentImageFilter, updateColumns map[string]any, tx *gorm.DB) error {
	tbl := d.Query().TDocSegmentImage
	session := tx

	if tx == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(tbl.TableName())

	session = d.generateSegmentImageCondition(ctx, session, filter)

	if err := session.Updates(updateColumns).Error; err != nil {
		logx.E(ctx, "BatchUpdateDocSegmentImages failed, error:%v", err)
		return err
	}
	logx.I(ctx, "BatchUpdateDocSegmentImages success")
	return nil
}

func (d *daoImpl) GetDocSegmentImageCountWithTx(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentImageFilter, tx *gorm.DB) (int64, error) {
	var count int64
	tbl := d.Query().TDocSegmentImage
	tableName := tbl.TableName()

	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(tableName)

	session = d.generateSegmentImageCondition(ctx, session, filter)

	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "Failed to GetDocSegmentImageCountWithTx , err: %+v", res.Error)
		return 0, res.Error
	}

	logx.I(ctx, "GetDocSegmentImageCountWithTx success")
	return count, nil

}
func (d *daoImpl) GetDocSegmentImageListWithTx(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentImageFilter, tx *gorm.DB) ([]*segEntity.DocSegmentImage, error) {
	tDocSegmentImageList := make([]*model.TDocSegmentImage, 0)
	tbl := d.Query().TDocSegmentImage
	tableName := tbl.TableName()

	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(tableName)

	if len(selectColumns) != 0 {
		session = session.Select(selectColumns)
	} else if len(filter.DistinctColumn) > 0 {
		session = session.Distinct(filter.DistinctColumn)
	}

	if filter.Offset != 0 || filter.Limit != 0 {
		session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	}
	session = d.generateSegmentImageCondition(ctx, session, filter)
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session = session.Order(orderColumn + " " + filter.OrderDirection[i])
	}

	res := session.Scan(&tDocSegmentImageList)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentImagePO2DO(tDocSegmentImageList), nil

}
func (d *daoImpl) GetDocSegmentImageCountAndList(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentImageFilter) ([]*segEntity.DocSegmentImage, int64, error) {

	count, err := d.GetDocSegmentImageCountWithTx(ctx, selectColumns, filter, nil)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocSegmentImageListWithTx(ctx, selectColumns, filter, nil)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocSegmentImagesByCursor 游标分页获取文档切片图片
func (d *daoImpl) GetDocSegmentImagesByCursor(ctx context.Context, robotID, robotBizID, docID uint64, lastID uint64, limit int) ([]*segEntity.DocSegmentImage, error) {
	tbl := d.Query().TDocSegmentImage
	tableName := tbl.TableName()

	db, err := knowClient.GormClient(ctx, tableName, robotID, robotBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocSegmentImagesByCursor get GormClient failed, err: %v", err)
		return nil, err
	}

	var tImages []*model.TDocSegmentImage
	err = db.WithContext(ctx).Table(tableName).
		Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
		Where(tbl.DocID.ColumnName().String()+util.SqlEqual, docID).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, segEntity.SegmentIsNotDeleted). // 1 表示未删除
		Where(tbl.ID.ColumnName().String()+util.SqlMore, lastID).
		Order(tbl.ID.ColumnName().String() + " " + util.SqlOrderByAsc).
		Limit(limit).
		Find(&tImages).Error
	if err != nil {
		logx.E(ctx, "GetDocSegmentImagesByCursor query failed, err: %v", err)
		return nil, err
	}

	return BatchConvertDocSegmentImagePO2DO(tImages), nil
}
