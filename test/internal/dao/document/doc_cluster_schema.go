package document

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
)

// CreateDocClusterSchema 创建文档聚类schema
func (d *daoImpl) CreateDocClusterSchema(ctx context.Context, docClusterSchema *docEntity.DocClusterSchema) error {
	tbl := d.tdsql.TDocClusterSchema
	tDocClusterSchema := ConvertDocClusterSchemaDO2PO(docClusterSchema)
	docClusterSchemaTableName := tbl.TableName()
	// 注意：不包含ID字段，让数据库自动生成自增ID
	selectColumns := []string{tbl.CorpBizID.ColumnName().String(), tbl.AppBizID.ColumnName().String(),
		tbl.BusinessID.ColumnName().String(), tbl.Version.ColumnName().String(), tbl.ClusterName.ColumnName().String(),
		tbl.Summary.ColumnName().String(), tbl.DocIds.ColumnName().String()}
	res := tbl.WithContext(ctx).UnderlyingDB().Table(docClusterSchemaTableName).Select(selectColumns).Create(tDocClusterSchema)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	// 将数据库生成的自增ID设置回传入的参数中
	docClusterSchema.ID = tDocClusterSchema.ID
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateDocClusterSchemaCondition(ctx context.Context, session *gorm.DB, filter *docEntity.DocClusterSchemaFilter) *gorm.DB {
	tbl := d.tdsql.TDocClusterSchema
	if filter.CorpBizId != 0 {
		session = session.Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, filter.CorpBizId)
	}
	if filter.AppBizId != 0 {
		session = session.Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, filter.AppBizId)
	}
	if len(filter.BusinessIds) != 0 {
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, filter.BusinessIds)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}

	return session
}

// getDocClusterSchemaList 获取文档schema列表
func (d *daoImpl) getDocClusterSchemaList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocClusterSchemaFilter) ([]*docEntity.DocClusterSchema, error) {
	tbl := d.tdsql.TDocClusterSchema
	docClusterSchemaList := make([]*model.TDocClusterSchema, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return nil, nil
	}
	if filter.Limit > docEntity.DocClusterSchemaTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		logx.E(ctx, "getDocClusterSchemaList err: %+v", err)
		return nil, err
	}
	docClusterSchemaTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docClusterSchemaTableName).Select(selectColumns)
	session = d.generateDocClusterSchemaCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docClusterSchemaList)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocClusterSchemaPO2DO(docClusterSchemaList), nil
}

// GetDocClusterSchemaCount 获取文档schema总数
func (d *daoImpl) GetDocClusterSchemaCount(ctx context.Context, selectColumns []string,
	filter *docEntity.DocClusterSchemaFilter) (int64, error) {
	tbl := d.tdsql.TDocClusterSchema
	docClusterSchemaTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docClusterSchemaTableName).Select(selectColumns)
	session = d.generateDocClusterSchemaCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocClusterSchemaCountAndList 获取文档schema总数和列表
func (d *daoImpl) GetDocClusterSchemaCountAndList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocClusterSchemaFilter) ([]*docEntity.DocClusterSchema, int64, error) {
	count, err := d.GetDocClusterSchemaCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocClusterSchemaList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocClusterSchemaList 获取所有文档schema列表
func (d *daoImpl) GetDocClusterSchemaList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocClusterSchemaFilter) ([]*docEntity.DocClusterSchema, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	allDocClusterSchemaList := make([]*docEntity.DocClusterSchema, 0)
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docClusterSchemaList, err := d.getDocClusterSchemaList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetDocClusterSchemaList failed, err: %+v", err)
			return nil, err
		}
		allDocClusterSchemaList = append(allDocClusterSchemaList, docClusterSchemaList...)
		if len(docClusterSchemaList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocClusterSchemaList count:%d cost:%dms",
		len(allDocClusterSchemaList), time.Since(beginTime).Milliseconds())
	return allDocClusterSchemaList, nil
}

// UpdateDocClusterSchema 更新文档schema
func (d *daoImpl) UpdateDocClusterSchema(ctx context.Context, updateColumns []string,
	corpBizId uint64, appBizId uint64, clusterBizId uint64, docClusterSchema *docEntity.DocClusterSchema) error {
	tbl := d.tdsql.TDocClusterSchema
	docClusterSchemaTableName := tbl.TableName()
	tDocClusterSchema := ConvertDocClusterSchemaDO2PO(docClusterSchema)
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docClusterSchemaTableName).Select(updateColumns).
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, corpBizId).
		Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, appBizId).
		Where(tbl.BusinessID.ColumnName().String()+util.SqlEqual, clusterBizId)
	res := session.Updates(tDocClusterSchema)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocClusterSchema failed for clusterBizId: %+v, err: %+v",
			clusterBizId, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocClusterSchemaAllOldVersion 硬性删除文档聚类schema的所有旧版
func (d *daoImpl) DeleteDocClusterSchemaAllOldVersion(ctx context.Context, corpBizId,
	appBizId, maxVersion uint64) error {
	tbl := d.tdsql.TDocClusterSchema
	docClusterSchemaTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docClusterSchemaTableName).
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, corpBizId).
		Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, appBizId).
		Where(tbl.Version.ColumnName().String()+util.SqlLess, maxVersion)
	res := session.Delete(&model.TDocClusterSchema{})
	if res.Error != nil {
		logx.E(ctx, "DeleteDocClusterSchemaAllOldVersion failed for appBizId: %+v, err: %+v",
			appBizId, res.Error)
		return res.Error
	}
	return nil
}

// GetDocClusterSchemaDaoMaxVersion 获取文档聚类schema最大版本
func (d *daoImpl) GetDocClusterSchemaDaoMaxVersion(ctx context.Context, appBizId uint64) (uint64, error) {
	// 使用Select和Row获取最大值
	tbl := d.tdsql.TDocClusterSchema
	docClusterSchemaTableName := tbl.TableName()
	maxVersion := uint64(0)
	row := tbl.WithContext(ctx).UnderlyingDB().Table(docClusterSchemaTableName).
		Select("COALESCE(MAX(version), 0) as max_version").
		Where(tbl.AppBizID.ColumnName().String()+util.SqlEqual, appBizId).Row()
	if err := row.Scan(&maxVersion); err != nil {
		return 0, err
	}
	return maxVersion, nil
}
