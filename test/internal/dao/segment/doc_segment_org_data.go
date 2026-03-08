package segment

import (
	"context"
	"fmt"
	"strings"

	"git.woa.com/adp/common/x/logx"
	"gorm.io/gorm"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	doc "git.woa.com/adp/kb/kb-config/internal/entity/document"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

const (
	getSegmentByDocIDAndKeywords = `
		SELECT
    		/*+ MAX_EXECUTION_TIME(20000) */ %s
		FROM
		    %s
		WHERE
		    corp_biz_id = ? AND app_biz_id = ? AND doc_biz_id = ? AND is_deleted = ? AND org_data LIKE ?
		ORDER BY
		    create_time ASC
		LIMIT ?,?
		`
)

// CreateDocSegmentOrgData 创建org_data
func (d *daoImpl) CreateDocSegmentOrgData(ctx context.Context, orgData *segEntity.DocSegmentOrgData, db *gorm.DB) error {
	if orgData == nil {
		logx.E(ctx, "CreateDocSegmentOrgData|orgData is null")
		return errs.ErrSystem
	}
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	tOrgData := ConvertDocSegmentOrgDataDO2PO(orgData)
	session := db

	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	res := session.WithContext(ctx).Create(&tOrgData)
	if res.Error != nil {
		logx.E(ctx, "CreateDocSegmentOrgData execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateOrgDataCondition(ctx context.Context, session *gorm.DB, filter *segEntity.DocSegmentOrgDataFilter) *gorm.DB {
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	if filter.CorpBizID != 0 {
		session = session.Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, filter.CorpBizID)
	}
	if filter.AppBizID != 0 {
		session = session.Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, filter.AppBizID)
	}
	if filter.DocBizID != 0 {
		session = session.Where(tbl.DocBizID.ColumnName().String()+util.SqlEqual, filter.DocBizID)
	}
	if len(filter.BusinessIDs) > 0 {
		if len(filter.BusinessIDs) == 1 {
			session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlEqual, filter.BusinessIDs[0])
		} else {
			session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, filter.BusinessIDs)
		}
	}
	if filter.MaxBusinessID != 0 {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlLess, filter.MaxBusinessID)
	}
	if filter.MinBusinessID != 0 {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlMore, filter.MinBusinessID)
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}
	if filter.IsTemporaryDeleted != nil {
		session = session.Where(tbl.IsTemporaryDeleted.ColumnName().String()+util.SqlEqual, *filter.IsTemporaryDeleted)
	}
	if filter.AddMethod != nil {
		session = session.Where(tbl.AddMethod.ColumnName().String()+util.SqlEqual, *filter.AddMethod)
	}
	if filter.SheetName != "" {
		session = session.Where(tbl.SheetName.ColumnName().String()+util.SqlEqual, filter.SheetName)
	}
	return session
}

func (d *daoImpl) GetDocOrgDataList(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) ([]*segEntity.DocSegmentOrgData, error) {
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := tbl.TableName()
	orgDataList := make([]*model.TDocSegmentOrgDatum, 0)

	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.WithContext(ctx).Table(docSegmentOrgDataTableName).Select(selectColumns)
	session = d.generateOrgDataCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	}

	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
				logx.E(ctx, "GetDocOrgDataList|invalid order direction:%s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&orgDataList)
	if res.Error != nil {
		logx.E(ctx, "GetDocOrgDataList|execute sql failed|err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentOrgDataPO2DO(orgDataList), nil
}

// GetDocOrgDataListByKeyWords 通过关键词获取切片
func (d *daoImpl) GetDocOrgDataListByKeyWords(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) ([]*segEntity.DocSegmentOrgData, error) {
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := tbl.TableName()
	orgDataList := make([]*model.TDocSegmentOrgDatum, 0)

	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	querySQL := fmt.Sprintf(getSegmentByDocIDAndKeywords, strings.Join(selectColumns, ","),
		docSegmentOrgDataTableName)
	keywordsArg := fmt.Sprintf("%%%s%%", util.Special.Replace(filter.Keywords))
	res := session.WithContext(ctx).Raw(querySQL, filter.CorpBizID, filter.AppBizID, filter.DocBizID,
		doc.DocIsNotDeleted, keywordsArg, filter.Offset, filter.Limit).Scan(&orgDataList)
	if res.Error != nil {
		logx.E(ctx, "GetDocOrgDataListByKeyWords|execute sql failed|err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentOrgDataPO2DO(orgDataList), nil
}

// GetDocOrgData 获取单个切片数据
func (d *daoImpl) GetDocOrgData(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) (*segEntity.DocSegmentOrgData, error) {
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := tbl.TableName()
	orgData := &model.TDocSegmentOrgDatum{}

	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.Debug().WithContext(ctx).Table(docSegmentOrgDataTableName).Select(selectColumns)
	session = d.generateOrgDataCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session = session.Limit(int(filter.Limit))
	}
	res := session.Take(&orgData)
	if res.Error != nil {
		logx.W(ctx, "GetDocOrgData execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return ConvertDocSegmentOrgDataPO2DO(orgData), nil
}

// GetDocOrgDataCount 获取切片总数
func (d *daoImpl) GetDocOrgDataCount(ctx context.Context,
	filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) (int64, error) {
	count := int64(0)
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := tbl.TableName()

	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.WithContext(ctx).Table(docSegmentOrgDataTableName)
	session = d.generateOrgDataCondition(ctx, session, filter)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "GetDocOrgDataCount execute sql failed, err: %+v", res.Error)
		return count, res.Error
	}
	return count, nil
}

// UpdateDocSegmentOrgData 更新切片数据
func (d *daoImpl) UpdateDocSegmentOrgData(ctx context.Context, updateColumns []string,
	filter *segEntity.DocSegmentOrgDataFilter, orgData *segEntity.DocSegmentOrgData, db *gorm.DB) (int64, error) {
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := tbl.TableName()
	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.WithContext(ctx).Table(docSegmentOrgDataTableName).Select(updateColumns)
	session = d.generateOrgDataCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	tOrgData := ConvertDocSegmentOrgDataDO2PO(orgData)
	res := session.Updates(tOrgData)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocSegmentTemporaryOrgData failed|err: %+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// RealityDeleteDocSegment 物理删除
func (d *daoImpl) RealityDeleteDocSegment(ctx context.Context,
	filter *segEntity.DocSegmentOrgDataFilter, db *gorm.DB) (int64, error) {
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := tbl.TableName()

	session := db

	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.WithContext(ctx).Table(docSegmentOrgDataTableName)
	session = d.generateOrgDataCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Delete(&model.TDocSegmentOrgDatum{})
	if res.Error != nil {
		logx.E(ctx, "RealityDeleteDocSegment|err:%+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// GetDocSegmentOrgDataByCursor 游标分页获取文档切片原始数据（用于导出）
func (d *daoImpl) GetDocSegmentOrgDataByCursor(ctx context.Context, corpBizID, appBizID, docBizID uint64, lastBusinessID uint64, limit int, db *gorm.DB) ([]*segEntity.DocSegmentOrgData, error) {
	tbl := d.TdsqlQuery().TDocSegmentOrgDatum
	tableName := tbl.TableName()
	orgDataList := make([]*model.TDocSegmentOrgDatum, 0)

	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.WithContext(ctx).Table(tableName).
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, corpBizID).
		Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, appBizID).
		Where(tbl.DocBizID.ColumnName().String()+util.SqlEqual, docBizID).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, 0).
		Where(tbl.BusinessID.ColumnName().String()+util.SqlMore, lastBusinessID).
		Order(tbl.BusinessID.ColumnName().String() + " " + util.SqlOrderByAsc).
		Limit(limit)

	res := session.Find(&orgDataList)
	if res.Error != nil {
		logx.E(ctx, "GetDocSegmentOrgDataByCursor execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentOrgDataPO2DO(orgDataList), nil
}
