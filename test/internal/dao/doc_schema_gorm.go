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

var globalDocSchemaDao *DocSchemaDao

const (
	docSchemaTableName = "t_doc_schema"

	DocSchemaTblColId         = "id"
	DocSchemaTblColCorpBizId  = "corp_biz_id"
	DocSchemaTblColAppBizId   = "app_biz_id"
	DocSchemaTblColDocBizId   = "doc_biz_id"
	DocSchemaTblColFileName   = "file_name"
	DocSchemaTblColSummary    = "summary"
	DocSchemaTblColVector     = "vector"
	DocSchemaTblColIsDeleted  = "is_deleted"
	DocSchemaTblColCreateTime = "create_time"
	DocSchemaTblColUpdateTime = "update_time"
)

type DocSchemaDao struct {
	BaseDao
}

// GetDocSchemaDao 获取全局的数据操作对象
func GetDocSchemaDao() *DocSchemaDao {
	if globalDocSchemaDao == nil {
		globalDocSchemaDao = &DocSchemaDao{*globalBaseDao}
	}
	return globalDocSchemaDao
}

type DocSchemaFilter struct {
	CorpBizId      uint64 // 企业 ID
	AppBizId       uint64 // 应用 ID
	DocBizIds      []uint64
	IsDeleted      *int
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// CreateDocSchema 新建文档schema
func (d *DocSchemaDao) CreateDocSchema(ctx context.Context, docSchema *model.DocSchema) error {
	selectColumns := []string{DocSchemaTblColId, DocSchemaTblColCorpBizId, DocSchemaTblColAppBizId, DocSchemaTblColDocBizId,
		DocSchemaTblColFileName, DocSchemaTblColSummary, DocSchemaTblColVector}
	res := d.tdsqlGormDB.WithContext(ctx).Table(docSchemaTableName).Select(selectColumns).Create(docSchema)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func generateDocSchemaCondition(ctx context.Context, session *gorm.DB, filter *DocSchemaFilter) {
	if filter.CorpBizId != 0 {
		session = session.Where(DocSchemaTblColCorpBizId+sqlEqual, filter.CorpBizId)
	}
	if filter.AppBizId != 0 {
		session = session.Where(DocSchemaTblColAppBizId+sqlEqual, filter.AppBizId)
	}
	if len(filter.DocBizIds) != 0 {
		session = session.Where(DocSchemaTblColDocBizId+sqlIn, filter.DocBizIds)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocSchemaTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	} else {
		session = session.Where(DocSchemaTblColIsDeleted+sqlEqual, IsNotDeleted)
	}
}

// getDocSchemaList 获取文档schema列表
func (d *DocSchemaDao) getDocSchemaList(ctx context.Context, selectColumns []string,
	filter *DocSchemaFilter) ([]*model.DocSchema, error) {
	docSchemaList := make([]*model.DocSchema, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docSchemaList, nil
	}
	if filter.Limit > DefaultMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocSchemaList err: %+v", err)
		return docSchemaList, err
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSchemaTableName).Select(selectColumns)
	generateDocSchemaCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docSchemaList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docSchemaList, res.Error
	}
	return docSchemaList, nil
}

// GetDocSchemaCount 获取文档schema总数
func (d *DocSchemaDao) GetDocSchemaCount(ctx context.Context, selectColumns []string,
	filter *DocSchemaFilter) (int64, error) {
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSchemaTableName).Select(selectColumns)
	generateDocSchemaCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocSchemaCountAndList 获取文档schema总数和列表
func (d *DocSchemaDao) GetDocSchemaCountAndList(ctx context.Context, selectColumns []string,
	filter *DocSchemaFilter) ([]*model.DocSchema, int64, error) {
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
func (d *DocSchemaDao) GetDocSchemaList(ctx context.Context, selectColumns []string,
	filter *DocSchemaFilter) ([]*model.DocSchema, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	allDocSchemaList := make([]*model.DocSchema, 0)
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docSchemaList, err := d.getDocSchemaList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocSchemaList failed, err: %+v", err)
			return nil, err
		}
		allDocSchemaList = append(allDocSchemaList, docSchemaList...)
		if uint32(len(docSchemaList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocSchemaList count:%d cost:%dms",
		len(allDocSchemaList), time.Since(beginTime).Milliseconds())
	return allDocSchemaList, nil
}

// UpdateDocSchema 更新文档schema
func (d *DocSchemaDao) UpdateDocSchema(ctx context.Context, tx *gorm.DB, updateColumns []string,
	docSchema *model.DocSchema) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(docSchemaTableName).Select(updateColumns).
		Where(DocSchemaTblColCorpBizId+sqlEqual, docSchema.CorpBizID).
		Where(DocSchemaTblColAppBizId+sqlEqual, docSchema.AppBizID).
		Where(DocSchemaTblColDocBizId+sqlEqual, docSchema.DocBizID)
	res := session.Updates(docSchema)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocSchema failed for docBizId: %+v, err: %+v",
			docSchema.DocBizID, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocSchema 删除文档schema
func (d *DocSchemaDao) DeleteDocSchema(ctx context.Context, tx *gorm.DB, corpBizId uint64, appBizId uint64,
	docBizIds []uint64) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(docSchemaTableName).
		Where(DocSchemaTblColCorpBizId+sqlEqual, corpBizId).
		Where(DocSchemaTblColAppBizId+sqlEqual, appBizId).
		Where(DocSchemaTblColDocBizId+sqlIn, docBizIds)
	res := session.Delete(&model.DocSchema{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "DeleteDocSchema failed for appBizId: %+v, err: %+v",
			appBizId, res.Error)
		return res.Error
	}
	return nil
}
