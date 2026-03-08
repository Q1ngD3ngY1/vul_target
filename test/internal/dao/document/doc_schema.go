package document

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/gox/boolx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
)

// CreateDocSchema 新建文档schema
func (d *daoImpl) CreateDocSchema(ctx context.Context, docSchema *docEntity.DocSchema) error {
	tbl := d.tdsql.TDocSchema
	tableName := tbl.TableName()
	selectColumns := []string{tbl.ID.ColumnName().String(), tbl.CorpBizID.ColumnName().String(),
		tbl.AppBizID.ColumnName().String(), tbl.DocBizID.ColumnName().String(), tbl.FileName.ColumnName().String(),
		tbl.Summary.ColumnName().String(), tbl.Vector.ColumnName().String()}
	tdocSchema := ConvertDocSchemaDO2PO(docSchema)
	res := tbl.WithContext(ctx).UnderlyingDB().Table(tableName).Select(selectColumns).Create(tdocSchema)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateDocSchemaCondition(ctx context.Context, session *gorm.DB, filter *docEntity.DocSchemaFilter) *gorm.DB {
	tbl := d.tdsql.TDocSchema
	if filter.CorpBizId != 0 {
		session = session.Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, filter.CorpBizId)
	}
	if filter.AppBizId != 0 {
		session = session.Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, filter.AppBizId)
	}
	if len(filter.DocBizIds) != 0 {
		session = session.Where(tbl.DocBizID.ColumnName().String()+util.SqlIn, filter.DocBizIds)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	} else {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, boolx.FalseNumber)
	}
	return session
}

// getDocSchemaList 获取文档schema列表
func (d *daoImpl) getDocSchemaList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocSchemaFilter) ([]*docEntity.DocSchema, error) {
	tDocSchemaList := make([]*model.TDocSchema, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return BatchConvertDocSchemaPO2DO(tDocSchemaList), nil
	}
	if filter.Limit > util.DefaultMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		logx.E(ctx, "getDocSchemaList err: %+v", err)
		return BatchConvertDocSchemaPO2DO(tDocSchemaList), err
	}
	tbl := d.tdsql.TDocSchema
	docSchemaTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSchemaTableName).Select(selectColumns)
	session = d.generateDocSchemaCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&tDocSchemaList)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return BatchConvertDocSchemaPO2DO(tDocSchemaList), res.Error
	}
	return BatchConvertDocSchemaPO2DO(tDocSchemaList), nil
}

// GetDocSchemaCount 获取文档schema总数
func (d *daoImpl) GetDocSchemaCount(ctx context.Context, selectColumns []string,
	filter *docEntity.DocSchemaFilter) (int64, error) {
	tbl := d.tdsql.TDocSchema
	docSchemaTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSchemaTableName).Select(selectColumns)
	session = d.generateDocSchemaCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocSchemaCountAndList 获取文档schema总数和列表
func (d *daoImpl) GetDocSchemaCountAndList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocSchemaFilter) ([]*docEntity.DocSchema, int64, error) {

	count, err := d.GetDocSchemaCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocSchemaList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocSchemaList 获取所有文档schema列表
func (d *daoImpl) GetDocSchemaList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocSchemaFilter) ([]*docEntity.DocSchema, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	allDocSchemaList := make([]*docEntity.DocSchema, 0)
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docSchemaList, err := d.getDocSchemaList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetDocSchemaList failed, err: %+v", err)
			return nil, err
		}
		allDocSchemaList = append(allDocSchemaList, docSchemaList...)
		if len(docSchemaList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocSchemaList count:%d cost:%dms",
		len(allDocSchemaList), time.Since(beginTime).Milliseconds())
	return allDocSchemaList, nil
}

// UpdateDocSchema 更新文档schema
func (d *daoImpl) UpdateDocSchema(ctx context.Context, updateColumns []string,
	docSchema *docEntity.DocSchema) error {
	tdocSchema := ConvertDocSchemaDO2PO(docSchema)
	tbl := d.tdsql.TDocSchema
	docSchemaTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSchemaTableName).Select(updateColumns).
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, docSchema.CorpBizID).
		Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, docSchema.AppBizID).
		Where(tbl.DocBizID.ColumnName().String()+util.SqlEqual, docSchema.DocBizID)
	res := session.Updates(tdocSchema)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocSchema failed for docBizId: %+v, err: %+v",
			docSchema.DocBizID, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocSchema 删除文档schema
func (d *daoImpl) DeleteDocSchema(ctx context.Context, corpBizId uint64, appBizId uint64,
	docBizIds []uint64) error {
	tbl := d.tdsql.TDocSchema
	docSchemaTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSchemaTableName).
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, corpBizId).
		Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, appBizId).
		Where(tbl.DocBizID.ColumnName().String()+util.SqlIn, docBizIds)
	res := session.Delete(&model.TDocSchema{})
	if res.Error != nil {
		logx.E(ctx, "DeleteDocSchema failed for appBizId: %+v, err: %+v",
			appBizId, res.Error)
		return res.Error
	}
	return nil
}
