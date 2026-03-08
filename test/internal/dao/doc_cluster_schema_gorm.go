package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalDocClusterSchemaDao *DocClusterSchemaDao

const (
	docClusterSchemaTableName = "t_doc_cluster_schema"

	DocClusterSchemaTblColId          = "id"
	DocClusterSchemaTblColCorpBizId   = "corp_biz_id"
	DocClusterSchemaTblColAppBizId    = "app_biz_id"
	DocClusterSchemaTblColBusinessId  = "business_id"
	DocClusterSchemaTblColVersion     = "version"
	DocClusterSchemaTblColClusterName = "cluster_name"
	DocClusterSchemaTblColSummary     = "summary"
	DocClusterSchemaTblColDocIds      = "doc_ids"
	DocClusterSchemaTblColIsDeleted   = "is_deleted"
	DocClusterSchemaTblColCreateTime  = "create_time"
	DocClusterSchemaTblColUpdateTime  = "update_time"

	DocClusterSchemaTableMaxPageSize = 1000
)

type DocClusterSchemaDao struct {
	BaseDao
}

// GetDocClusterSchemaDao 获取全局的数据操作对象
func GetDocClusterSchemaDao() *DocClusterSchemaDao {
	if globalDocClusterSchemaDao == nil {
		globalDocClusterSchemaDao = &DocClusterSchemaDao{*globalBaseDao}
	}
	return globalDocClusterSchemaDao
}

type DocClusterSchemaFilter struct {
	CorpBizId      uint64   // 企业 ID
	AppBizId       uint64   // 应用 ID
	BusinessIds    []uint64 // 业务 ID
	IsDeleted      *int
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// CreateDocClusterSchema 创建文档聚类schema
func (d *DocClusterSchemaDao) CreateDocClusterSchema(ctx context.Context, tx *gorm.DB,
	docClusterSchema *model.DocClusterSchema) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	selectColumns := []string{DocClusterSchemaTblColId, DocClusterSchemaTblColCorpBizId, DocClusterSchemaTblColAppBizId,
		DocClusterSchemaTblColBusinessId, DocClusterSchemaTblColVersion, DocClusterSchemaTblColClusterName,
		DocClusterSchemaTblColSummary, DocClusterSchemaTblColDocIds}
	res := d.tdsqlGormDB.WithContext(ctx).Table(docClusterSchemaTableName).Select(selectColumns).Create(docClusterSchema)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func generateDocClusterSchemaCondition(ctx context.Context, session *gorm.DB, filter *DocClusterSchemaFilter) {
	if filter.CorpBizId != 0 {
		session = session.Where(DocClusterSchemaTblColCorpBizId+sqlEqual, filter.CorpBizId)
	}
	if filter.AppBizId != 0 {
		session = session.Where(DocClusterSchemaTblColAppBizId+sqlEqual, filter.AppBizId)
	}
	if len(filter.BusinessIds) != 0 {
		session = session.Where(DocClusterSchemaTblColBusinessId+sqlIn, filter.BusinessIds)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocClusterSchemaTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// getDocClusterSchemaList 获取文档schema列表
func (d *DocClusterSchemaDao) getDocClusterSchemaList(ctx context.Context, selectColumns []string,
	filter *DocClusterSchemaFilter) ([]*model.DocClusterSchema, error) {
	docClusterSchemaList := make([]*model.DocClusterSchema, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docClusterSchemaList, nil
	}
	if filter.Limit > DocClusterSchemaTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocClusterSchemaList err: %+v", err)
		return docClusterSchemaList, err
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docClusterSchemaTableName).Select(selectColumns)
	generateDocClusterSchemaCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docClusterSchemaList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docClusterSchemaList, res.Error
	}
	return docClusterSchemaList, nil
}

// GetDocClusterSchemaCount 获取文档schema总数
func (d *DocClusterSchemaDao) GetDocClusterSchemaCount(ctx context.Context, selectColumns []string,
	filter *DocClusterSchemaFilter) (int64, error) {
	session := d.tdsqlGormDB.WithContext(ctx).Table(docClusterSchemaTableName).Select(selectColumns)
	generateDocClusterSchemaCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocClusterSchemaCountAndList 获取文档schema总数和列表
func (d *DocClusterSchemaDao) GetDocClusterSchemaCountAndList(ctx context.Context, selectColumns []string,
	filter *DocClusterSchemaFilter) ([]*model.DocClusterSchema, int64, error) {
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
func (d *DocClusterSchemaDao) GetDocClusterSchemaList(ctx context.Context, selectColumns []string,
	filter *DocClusterSchemaFilter) ([]*model.DocClusterSchema, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	allDocClusterSchemaList := make([]*model.DocClusterSchema, 0)
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docClusterSchemaList, err := d.getDocClusterSchemaList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocClusterSchemaList failed, err: %+v", err)
			return nil, err
		}
		allDocClusterSchemaList = append(allDocClusterSchemaList, docClusterSchemaList...)
		if uint32(len(docClusterSchemaList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocClusterSchemaList count:%d cost:%dms",
		len(allDocClusterSchemaList), time.Since(beginTime).Milliseconds())
	return allDocClusterSchemaList, nil
}

// UpdateDocClusterSchema 更新文档schema
func (d *DocClusterSchemaDao) UpdateDocClusterSchema(ctx context.Context, tx *gorm.DB, updateColumns []string,
	corpBizId uint64, appBizId uint64, clusterBizId uint64, docClusterSchema *model.DocClusterSchema) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(docClusterSchemaTableName).Select(updateColumns).
		Where(DocClusterSchemaTblColCorpBizId+sqlEqual, corpBizId).
		Where(DocClusterSchemaTblColAppBizId+sqlEqual, appBizId).
		Where(DocClusterSchemaTblColBusinessId+sqlEqual, clusterBizId)
	res := session.Updates(docClusterSchema)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocClusterSchema failed for clusterBizId: %+v, err: %+v",
			clusterBizId, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocClusterSchemaAllOldVersion 硬性删除文档聚类schema的所有旧版
func (d *DocClusterSchemaDao) DeleteDocClusterSchemaAllOldVersion(ctx context.Context, tx *gorm.DB, corpBizId,
	appBizId, maxVersion uint64) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(docClusterSchemaTableName).
		Where(DocClusterSchemaTblColCorpBizId+sqlEqual, corpBizId).
		Where(DocClusterSchemaTblColAppBizId+sqlEqual, appBizId).
		Where(DocClusterSchemaTblColVersion+sqlLess, maxVersion)
	res := session.Delete(&model.DocClusterSchema{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "DeleteDocClusterSchemaAllOldVersion failed for appBizId: %+v, err: %+v",
			appBizId, res.Error)
		return res.Error
	}
	return nil
}

// GetDocClusterSchemaDaoMaxVersion 获取文档聚类schema最大版本
func (d *DocClusterSchemaDao) GetDocClusterSchemaDaoMaxVersion(ctx context.Context, appBizId uint64) (uint64, error) {
	// 使用Select和Row获取最大值
	maxVersion := uint64(0)
	row := d.tdsqlGormDB.WithContext(ctx).Table(docClusterSchemaTableName).
		Select("COALESCE(MAX(version), 0) as max_version").
		Where(KnowledgeSchemaTblColAppBizId+sqlEqual, appBizId).Row()
	if err := row.Scan(&maxVersion); err != nil {
		return 0, err
	}
	return maxVersion, nil
}
