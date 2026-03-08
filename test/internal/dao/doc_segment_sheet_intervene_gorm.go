// bot-knowledge-config-server
//
// @(#)doc_dao.go  星期四, 一月 16, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"gorm.io/gorm"
)

var globalDocSegmentSheetTemporaryDao *DocSegmentSheetTemporaryDao

const (
	docSegmentSheetTemporaryTableName                        = "t_doc_segment_sheet_temporary"
	DocSegmentSheetTemporaryTblColBusinessID                 = "business_id"
	DocSegmentSheetTemporaryTblColAppBizID                   = "app_biz_id"
	DocSegmentSheetTemporaryTblColCorpBizID                  = "corp_biz_id"
	DocSegmentSheetTemporaryTblColStaffBizID                 = "staff_biz_id"
	DocSegmentSheetTemporaryTblColDocBizID                   = "doc_biz_id"
	DocSegmentSheetTemporaryTblColBucket                     = "bucket"
	DocSegmentSheetTemporaryTblColRegion                     = "region"
	DocSegmentSheetTemporaryTblColCosURL                     = "cos_url"
	DocSegmentSheetTemporaryTblColCosHash                    = "cos_hash"
	DocSegmentSheetTemporaryTblColFileName                   = "file_name"
	DocSegmentSheetTemporaryTblColFileType                   = "file_type"
	DocSegmentSheetTemporaryTblColSheetOrder                 = "sheet_order"
	DocSegmentSheetTemporaryTblColSheetName                  = "sheet_name"
	DocSegmentSheetTemporaryTblColSheetTotalNum              = "sheet_total_num"
	DocSegmentSheetTemporaryTblColVersion                    = "version"
	DocSegmentSheetTemporaryTblColIsDeleted                  = "is_deleted"
	DocSegmentSheetTemporaryTblColIsDisabled                 = "is_disabled"
	DocSegmentSheetTemporaryTblColIsDisabledRetrievalEnhance = "is_disabled_retrieval_enhance"
	DocSegmentSheetTemporaryTblColCreateTime                 = "create_time"
	DocSegmentSheetTemporaryTblColUpdateTime                 = "update_time"
	DocSegmentSheetTemporaryTblColAuditStatus                = "audit_status"

	docSegmentSheetTemporaryTableMaxPageSize = 1000
)

var DocSegmentSheetTemporaryTblColList = []string{DocSegmentSheetTemporaryTblColBusinessID,
	DocSegmentSheetTemporaryTblColAppBizID, DocSegmentSheetTemporaryTblColCorpBizID, DocSegmentSheetTemporaryTblColStaffBizID,
	DocSegmentSheetTemporaryTblColBucket, DocSegmentSheetTemporaryTblColRegion, DocSegmentSheetTemporaryTblColCosURL,
	DocSegmentSheetTemporaryTblColCosHash, DocSegmentSheetTemporaryTblColFileName, DocSegmentSheetTemporaryTblColFileType,
	DocSegmentSheetTemporaryTblColSheetOrder, DocSegmentSheetTemporaryTblColSheetName,
	DocSegmentSheetTemporaryTblColSheetTotalNum, DocSegmentSheetTemporaryTblColVersion, DocSegmentSheetTemporaryTblColIsDeleted,
	DocSegmentSheetTemporaryTblColIsDisabled, DocSegmentSheetTemporaryTblColIsDisabledRetrievalEnhance,
	DocSegmentSheetTemporaryTblColCreateTime, DocSegmentSheetTemporaryTblColUpdateTime, DocSegmentSheetTemporaryTblColAuditStatus}

type DocSegmentSheetTemporaryDao struct {
	BaseDao
}

// GetDocSegmentSheetTemporaryDao 获取全局的数据操作对象
func GetDocSegmentSheetTemporaryDao() *DocSegmentSheetTemporaryDao {
	if globalDocSegmentSheetTemporaryDao == nil {
		globalDocSegmentSheetTemporaryDao = &DocSegmentSheetTemporaryDao{*globalBaseDao}
	}
	return globalDocSegmentSheetTemporaryDao
}

type DocSegmentSheetTemporaryFilter struct {
	BusinessIDs    []uint64
	CorpBizID      uint64 // 企业 ID
	AppBizID       uint64
	DocBizID       uint64
	IsDeleted      *int
	SheetNames     []string // 查询sheet名称
	CosHash        string
	Version        *int
	AuditStatus    []uint32
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// CreateDocSegmentSheet 创建sheet数据
func (d *DocSegmentSheetTemporaryDao) CreateDocSegmentSheet(ctx context.Context, tx *gorm.DB, sheet *model.DocSegmentSheetTemporary) error {
	if sheet == nil {
		log.ErrorContextf(ctx, "CreateDocSegmentSheet sheet is null")
		return errs.ErrParams
	}
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	res := tx.WithContext(ctx).Table(docSegmentSheetTemporaryTableName).Create(&sheet)
	if res.Error != nil {
		log.ErrorContextf(ctx, "CreateDocSegmentSheet execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocSegmentSheetTemporaryDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocSegmentSheetTemporaryFilter) {
	if filter.CorpBizID != 0 {
		session = session.Where(DocSegmentSheetTemporaryTblColCorpBizID+sqlEqual, filter.CorpBizID)
	}
	if filter.AppBizID != 0 {
		session = session.Where(DocSegmentSheetTemporaryTblColAppBizID+sqlEqual, filter.AppBizID)
	}
	if filter.DocBizID != 0 {
		session = session.Where(DocSegmentSheetTemporaryTblColDocBizID+sqlEqual, filter.DocBizID)
	}
	if len(filter.BusinessIDs) > 0 {
		if len(filter.BusinessIDs) == 1 {
			session = session.Where(DocSegmentSheetTemporaryTblColBusinessID+sqlEqual, filter.BusinessIDs[0])
		} else {
			session = session.Where(DocSegmentSheetTemporaryTblColBusinessID+sqlIn, filter.BusinessIDs)
		}
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocSegmentSheetTemporaryTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	if len(filter.SheetNames) > 0 {
		// 文件名或者审核中的文件名相同
		if len(filter.SheetNames) == 1 {
			session = session.Where(DocSegmentSheetTemporaryTblColSheetName+sqlEqual, filter.SheetNames[0])
		} else {
			session = session.Where(DocSegmentSheetTemporaryTblColSheetName+sqlIn, filter.SheetNames)
		}
	}
	// 用于确认版本是否为初始版本
	if filter.Version != nil {
		session = session.Where(DocSegmentSheetTemporaryTblColVersion+sqlNotEqual, *filter.Version)
	}
	// 筛选审核状态
	if len(filter.AuditStatus) != 0 {
		session = session.Where(DocSegmentSheetTemporaryTblColAuditStatus+sqlIn, filter.AuditStatus)
	}
}

// GetSheetList 获取文档的sheet列表
func (d *DocSegmentSheetTemporaryDao) GetSheetList(ctx context.Context, selectColumns []string,
	filter *DocSegmentSheetTemporaryFilter) ([]*model.DocSegmentSheetTemporary, error) {
	sheet := make([]*model.DocSegmentSheetTemporary, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetSheetList|filter is null")
		return sheet, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return sheet, nil
	}
	if filter.Limit > docSegmentSheetTemporaryTableMaxPageSize {
		// 限制单次查询最大条数
		err := fmt.Errorf("limit %d exceed max page size %d",
			filter.Limit, docSegmentSheetTemporaryTableMaxPageSize)
		log.ErrorContextf(ctx, "GetSheetList err: %+v", err)
		return sheet, err
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSegmentSheetTemporaryTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
				log.ErrorContextf(ctx, "GetSheetList invalid order direction: %s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&sheet)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetSheetList execute sql failed, err: %+v", res.Error)
		return sheet, res.Error
	}
	return sheet, nil
}

// GetSheet 获取sheet数据
func (d *DocSegmentSheetTemporaryDao) GetSheet(ctx context.Context, selectColumns []string,
	filter *DocSegmentSheetTemporaryFilter) (*model.DocSegmentSheetTemporary, error) {
	sheet := &model.DocSegmentSheetTemporary{}
	if filter == nil {
		log.ErrorContextf(ctx, "GetSheet|filter is null")
		return sheet, errs.ErrSystem
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSegmentSheetTemporaryTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	res := session.Take(&sheet)
	if res.Error != nil {
		log.WarnContextf(ctx, "GetSheet execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return sheet, nil
}

// GetDocSheetCount 获取sheet总数
func (d *DocSegmentSheetTemporaryDao) GetDocSheetCount(ctx context.Context,
	filter *DocSegmentSheetTemporaryFilter) (int64, error) {
	count := int64(0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocSheetCount|filter is null")
		return count, errs.ErrSystem
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSegmentSheetTemporaryTableName)
	d.generateCondition(ctx, session, filter)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocSheetCount execute sql failed, err: %+v", res.Error)
		return count, res.Error
	}
	return count, nil
}

// UpdateDocSegmentSheet 更新sheet数据
func (d *DocSegmentSheetTemporaryDao) UpdateDocSegmentSheet(ctx context.Context, tx *gorm.DB, updateColumns []string,
	filter *DocSegmentSheetTemporaryFilter, sheet *model.DocSegmentSheetTemporary) (int64, error) {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	if filter == nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentSheet|filter is null")
		return 0, errs.ErrSystem
	}
	if sheet == nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentSheet|sheet is null")
		return 0, errs.ErrSystem
	}
	session := tx.WithContext(ctx).Table(docSegmentSheetTemporaryTableName).Select(updateColumns)
	d.generateCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	sheet.Version = sheet.Version + 1
	res := session.Updates(sheet)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentSheet failed err: %+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// DeleteDocSegmentSheet 删除sheet数据
func (d *DocSegmentSheetTemporaryDao) DeleteDocSegmentSheet(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentSheetTemporaryTblColIsDeleted,
		DocSegmentSheetTemporaryTblColUpdateTime,
		DocSegmentSheetTemporaryTblColVersion}
	sheet := &model.DocSegmentSheetTemporary{
		IsDeleted:  IsDeleted,  // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet); err != nil {
		log.ErrorContextf(ctx, "DeleteDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// DisabledDocSegmentSheet 停用sheet数据
func (d *DocSegmentSheetTemporaryDao) DisabledDocSegmentSheet(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentSheetTemporaryTblColIsDisabled,
		DocSegmentSheetTemporaryTblColUpdateTime,
		DocSegmentSheetTemporaryTblColVersion}
	sheet := &model.DocSegmentSheetTemporary{
		IsDisabled: model.SegmentIsDisabled, // 是否启用
		UpdateTime: time.Now(),              // 更新时间
	}
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet); err != nil {
		log.ErrorContextf(ctx, "DisabledDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// EnableDocSegmentSheet 启用sheet数据
func (d *DocSegmentSheetTemporaryDao) EnableDocSegmentSheet(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentSheetTemporaryTblColIsDisabled,
		DocSegmentSheetTemporaryTblColUpdateTime,
		DocSegmentSheetTemporaryTblColVersion}
	sheet := &model.DocSegmentSheetTemporary{
		IsDisabled: model.SegmentIsEnable, // 启用检索增强
		UpdateTime: time.Now(),            // 更新时间
	}
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet); err != nil {
		log.ErrorContextf(ctx, "EnableDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// DisabledRetrievalEnhanceSheet 停用检索增强
func (d *DocSegmentSheetTemporaryDao) DisabledRetrievalEnhanceSheet(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, sheetNames []string) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentSheetTemporaryTblColIsDisabledRetrievalEnhance,
		DocSegmentSheetTemporaryTblColUpdateTime,
		DocSegmentSheetTemporaryTblColVersion}
	sheet := &model.DocSegmentSheetTemporary{
		IsDisabledRetrievalEnhance: model.SheetDisabledRetrievalEnhance, // 是否启用检索增强
		UpdateTime:                 time.Now(),                          // 更新时间
	}
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID:  corpBizID,
		AppBizID:   appBizID,
		DocBizID:   docBizID,
		SheetNames: sheetNames,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet); err != nil {
		log.ErrorContextf(ctx, "DisabledRetrievalEnhanceSheet failed for len(sheetNames): %d, err: %+v", len(sheetNames), err)
		return err
	}
	return nil
}

// EnableRetrievalEnhanceSheet 启用检索增强
func (d *DocSegmentSheetTemporaryDao) EnableRetrievalEnhanceSheet(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, sheetNames []string) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentSheetTemporaryTblColIsDisabledRetrievalEnhance,
		DocSegmentSheetTemporaryTblColUpdateTime,
		DocSegmentSheetTemporaryTblColVersion}
	sheet := &model.DocSegmentSheetTemporary{
		IsDisabledRetrievalEnhance: model.SheetEnableRetrievalEnhance, // 是否启用检索增强
		UpdateTime:                 time.Now(),                        // 更新时间
	}
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID:  corpBizID,
		AppBizID:   appBizID,
		DocBizID:   docBizID,
		SheetNames: sheetNames,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet); err != nil {
		log.ErrorContextf(ctx, "EnableRetrievalEnhanceSheet failed for len(sheetNames): %d, err: %+v", len(sheetNames), err)
		return err
	}
	return nil
}

// UpdateDocSegmentSheetAuditStatus 更新sheet审核状态
func (d *DocSegmentSheetTemporaryDao) UpdateDocSegmentSheetAuditStatus(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64, auditStatus uint32) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentSheetTemporaryTblColAuditStatus,
		DocSegmentSheetTemporaryTblColUpdateTime,
		DocSegmentSheetTemporaryTblColVersion}
	sheet := &model.DocSegmentSheetTemporary{
		AuditStatus: auditStatus, // 是否删除
		UpdateTime:  time.Now(),  // 更新时间
	}
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet); err != nil {
		log.ErrorContextf(ctx, "DeleteDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// BatchUpdateDocSegmentSheet 批量更新sheet
func (d *DocSegmentSheetTemporaryDao) BatchUpdateDocSegmentSheet(ctx context.Context, tx *gorm.DB,
	updateColumns []string, filter *DocSegmentSheetTemporaryFilter, sheet *model.DocSegmentSheetTemporary, batchSize int) error {
	log.DebugContextf(ctx, "BatchUpdateDocSegmentSheet|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = uint32(batchSize)
		rowsAffected, err := d.UpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet)
		if err != nil {
			log.ErrorContextf(ctx, "BatchUpdateDocSegmentSheet failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}

// BatchDeleteDocSegmentSheetByDocBizID 批量删除干预中的OrgData数据(逻辑删除)
func (d *DocSegmentSheetTemporaryDao) BatchDeleteDocSegmentSheetByDocBizID(ctx context.Context, tx *gorm.DB, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	log.DebugContextf(ctx, "BatchDeleteDocSegmentSheetByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentSheetTemporaryTblColIsDeleted,
		DocSegmentSheetTemporaryTblColUpdateTime}
	sheet := &model.DocSegmentSheetTemporary{
		IsDeleted:  IsDeleted,
		UpdateTime: time.Now(),
	}
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	return d.BatchUpdateDocSegmentSheet(ctx, tx, updateColumns, filter, sheet, batchSize)
}

// RealityDeleteDocSegmentSheet 物理删除
func (d *DocSegmentSheetTemporaryDao) RealityDeleteDocSegmentSheet(ctx context.Context, tx *gorm.DB,
	filter *DocSegmentSheetTemporaryFilter) (int64, error) {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	if filter == nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegmentSheet|filter is null")
		return 0, errs.ErrSystem
	}
	session := tx.WithContext(ctx).Table(docSegmentSheetTemporaryTableName)
	d.generateCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Delete(&model.DocSegmentOrgDataTemporary{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegmentSheet|err:%+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// RealityBatchDeleteDocSheet 批量删除Sheet数据(物理删除)
func (d *DocSegmentSheetTemporaryDao) RealityBatchDeleteDocSheet(ctx context.Context, tx *gorm.DB,
	filter *DocSegmentSheetTemporaryFilter, batchSize int) error {
	log.DebugContextf(ctx, "RealityBatchDeleteDocSheet|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = uint32(batchSize)
		rowsAffected, err := d.RealityDeleteDocSegmentSheet(ctx, tx, filter)
		if err != nil {
			log.ErrorContextf(ctx, "RealityBatchDeleteDocSheet failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}
