package segment

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"gorm.io/gorm"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// CreateDocSegmentSheet 创建sheet数据
func (d *daoImpl) CreateDocSegmentSheet(ctx context.Context, sheet *segEntity.DocSegmentSheetTemporary) error {
	if sheet == nil {
		logx.E(ctx, "CreateDocSegmentSheet sheet is null")
		return errs.ErrParams
	}
	tbl := d.tdsql.TDocSegmentSheetTemporary
	docSegmentSheetTemporaryTableName := tbl.TableName()
	tSheet := ConvertDocSegmentSheetTemporaryDataDO2PO(sheet)
	res := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentSheetTemporaryTableName).Create(&tSheet)
	if res.Error != nil {
		logx.E(ctx, "CreateDocSegmentSheet execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateDocSegmentSheetCondition(ctx context.Context, session *gorm.DB, filter *segEntity.DocSegmentSheetTemporaryFilter) *gorm.DB {
	tbl := d.tdsql.TDocSegmentSheetTemporary
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
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}
	if len(filter.SheetNames) > 0 {
		// 文件名或者审核中的文件名相同
		if len(filter.SheetNames) == 1 {
			session = session.Where(tbl.SheetName.ColumnName().String()+util.SqlEqual, filter.SheetNames[0])
		} else {
			session = session.Where(tbl.SheetName.ColumnName().String()+util.SqlIn, filter.SheetNames)
		}
	}
	// 用于确认版本是否为初始版本
	if filter.Version != nil {
		session = session.Where(tbl.Version.ColumnName().String()+util.SqlNotEqual, *filter.Version)
	}
	// 筛选审核状态
	if len(filter.AuditStatus) != 0 {
		session = session.Where(tbl.AuditStatus.ColumnName().String()+util.SqlIn, filter.AuditStatus)
	}
	return session
}

// GetSheetList 获取文档的sheet列表
func (d *daoImpl) GetSheetList(ctx context.Context, selectColumns []string, filter *segEntity.DocSegmentSheetTemporaryFilter) ([]*segEntity.DocSegmentSheetTemporary, error) {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	docSegmentSheetTemporaryTableName := tbl.TableName()
	sheet := make([]*model.TDocSegmentSheetTemporary, 0)
	if filter == nil {
		logx.E(ctx, "GetSheetList|filter is null")
		return nil, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return nil, nil
	}
	if filter.Limit > segEntity.DocSegmentSheetTemporaryTableMaxPageSize {
		// 限制单次查询最大条数
		err := fmt.Errorf("limit %d exceed max page size %d",
			filter.Limit, segEntity.DocSegmentSheetTemporaryTableMaxPageSize)
		logx.E(ctx, "GetSheetList err: %+v", err)
		return nil, err
	}
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentSheetTemporaryTableName).Select(selectColumns)
	session = d.generateDocSegmentSheetCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
				logx.E(ctx, "GetSheetList invalid order direction: %s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&sheet)
	if res.Error != nil {
		logx.E(ctx, "GetSheetList execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentSheetTemporaryDataPO2DO(sheet), nil
}

// GetSheet 获取sheet数据
func (d *daoImpl) GetSheet(ctx context.Context, selectColumns []string, filter *segEntity.DocSegmentSheetTemporaryFilter) (*segEntity.DocSegmentSheetTemporary, error) {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	docSegmentSheetTemporaryTableName := tbl.TableName()
	sheet := &model.TDocSegmentSheetTemporary{}
	if filter == nil {
		logx.E(ctx, "GetSheet|filter is null")
		return nil, errs.ErrSystem
	}
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentSheetTemporaryTableName).Select(selectColumns)
	session = d.generateDocSegmentSheetCondition(ctx, session, filter)
	res := session.Take(&sheet)
	if res.Error != nil {
		logx.W(ctx, "GetSheet execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return ConvertDocSegmentSheetTemporaryDataPO2DO(sheet), nil
}

// GetDocSheetCount 获取sheet总数
func (d *daoImpl) GetDocSheetCount(ctx context.Context, filter *segEntity.DocSegmentSheetTemporaryFilter) (int64, error) {
	count := int64(0)
	if filter == nil {
		logx.E(ctx, "GetDocSheetCount|filter is null")
		return count, errs.ErrSystem
	}
	tbl := d.tdsql.TDocSegmentSheetTemporary
	docSegmentSheetTemporaryTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentSheetTemporaryTableName)
	session = d.generateDocSegmentSheetCondition(ctx, session, filter)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "GetDocSheetCount execute sql failed, err: %+v", res.Error)
		return count, res.Error
	}
	return count, nil
}

// UpdateDocSegmentSheet 更新sheet数据
func (d *daoImpl) UpdateDocSegmentSheet(ctx context.Context, updateColumns []string,
	filter *segEntity.DocSegmentSheetTemporaryFilter, sheet *segEntity.DocSegmentSheetTemporary) (int64, error) {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	docSegmentSheetTemporaryTableName := tbl.TableName()
	if filter == nil {
		logx.E(ctx, "UpdateDocSegmentSheet|filter is null")
		return 0, errs.ErrSystem
	}
	if sheet == nil {
		logx.E(ctx, "UpdateDocSegmentSheet|sheet is null")
		return 0, errs.ErrSystem
	}
	sheet.Version = sheet.Version + 1
	tSheet := ConvertDocSegmentSheetTemporaryDataDO2PO(sheet)
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentSheetTemporaryTableName).Select(updateColumns)
	session = d.generateDocSegmentSheetCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Updates(tSheet)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocSegmentSheet failed err: %+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// DeleteDocSegmentSheet 删除sheet数据
func (d *daoImpl) DeleteDocSegmentSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	updateColumns := []string{tbl.IsDeleted.ColumnName().String(),
		tbl.UpdateTime.ColumnName().String(),
		tbl.Version.ColumnName().String()}
	sheet := &segment.DocSegmentSheetTemporary{
		IsDeleted:  true,       // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet); err != nil {
		logx.E(ctx, "DeleteDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// DisabledDocSegmentSheet 停用sheet数据
func (d *daoImpl) DisabledDocSegmentSheet(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	updateColumns := []string{tbl.IsDisabled.ColumnName().String(),
		tbl.UpdateTime.ColumnName().String(),
		tbl.Version.ColumnName().String()}
	sheet := &segment.DocSegmentSheetTemporary{
		IsDisabled: true,       // 是否启用
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet); err != nil {
		logx.E(ctx, "DisabledDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// EnableDocSegmentSheet 启用sheet数据
func (d *daoImpl) EnableDocSegmentSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	updateColumns := []string{tbl.IsDisabled.ColumnName().String(),
		tbl.UpdateTime.ColumnName().String(),
		tbl.Version.ColumnName().String()}
	sheet := &segment.DocSegmentSheetTemporary{
		IsDisabled: false,      // 启用检索增强
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet); err != nil {
		logx.E(ctx, "EnableDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// DisabledRetrievalEnhanceSheet 停用检索增强
func (d *daoImpl) DisabledRetrievalEnhanceSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, sheetNames []string) error {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	updateColumns := []string{tbl.IsDisabledRetrievalEnhance.ColumnName().String(),
		tbl.UpdateTime.ColumnName().String(),
		tbl.Version.ColumnName().String()}
	sheet := &segment.DocSegmentSheetTemporary{
		IsDisabledRetrievalEnhance: true,       // 是否启用检索增强
		UpdateTime:                 time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:  corpBizID,
		AppBizID:   appBizID,
		DocBizID:   docBizID,
		SheetNames: sheetNames,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet); err != nil {
		logx.E(ctx, "DisabledRetrievalEnhanceSheet failed for len(sheetNames): %d, err: %+v", len(sheetNames), err)
		return err
	}
	return nil
}

// EnableRetrievalEnhanceSheet 启用检索增强
func (d *daoImpl) EnableRetrievalEnhanceSheet(ctx context.Context, corpBizID uint64, appBizID uint64, docBizID uint64, sheetNames []string) error {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	updateColumns := []string{tbl.IsDisabledRetrievalEnhance.ColumnName().String(),
		tbl.UpdateTime.ColumnName().String(),
		tbl.Version.ColumnName().String()}
	sheet := &segEntity.DocSegmentSheetTemporary{
		IsDisabledRetrievalEnhance: false,      // 是否启用检索增强
		UpdateTime:                 time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:  corpBizID,
		AppBizID:   appBizID,
		DocBizID:   docBizID,
		SheetNames: sheetNames,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet); err != nil {
		logx.E(ctx, "EnableRetrievalEnhanceSheet failed for len(sheetNames): %d, err: %+v", len(sheetNames), err)
		return err
	}
	return nil
}

// UpdateDocSegmentSheetAuditStatus 更新sheet审核状态
func (d *daoImpl) UpdateDocSegmentSheetAuditStatus(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64, auditStatus uint32) error {
	tbl := d.tdsql.TDocSegmentSheetTemporary
	updateColumns := []string{tbl.AuditStatus.ColumnName().String(),
		tbl.UpdateTime.ColumnName().String(),
		tbl.Version.ColumnName().String()}
	sheet := &segEntity.DocSegmentSheetTemporary{
		AuditStatus: auditStatus, // 是否删除
		UpdateTime:  time.Now(),  // 更新时间
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	if _, err := d.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet); err != nil {
		logx.E(ctx, "DeleteDocSegmentSheet failed len(businessIDs): %d, err: %+v", len(businessIDs), err)
		return err
	}
	return nil
}

// BatchUpdateDocSegmentSheet 批量更新sheet
func (d *daoImpl) BatchUpdateDocSegmentSheet(ctx context.Context, updateColumns []string, filter *segEntity.DocSegmentSheetTemporaryFilter,
	sheet *segEntity.DocSegmentSheetTemporary, batchSize int) error {
	logx.D(ctx, "BatchUpdateDocSegmentSheet|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = batchSize
		rowsAffected, err := d.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet)
		if err != nil {
			logx.E(ctx, "BatchUpdateDocSegmentSheet failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}

// BatchDeleteDocSegmentSheetByDocBizID 批量删除干预中的OrgData数据(逻辑删除)
func (d *daoImpl) BatchDeleteDocSegmentSheetByDocBizID(ctx context.Context, corpBizID, appBizID, docBizID uint64, batchSize int) error {
	logx.D(ctx, "BatchDeleteDocSegmentSheetByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	tbl := d.tdsql.TDocSegmentSheetTemporary
	updateColumns := []string{tbl.IsDeleted.ColumnName().String(), tbl.UpdateTime.ColumnName().String()}
	sheet := &segment.DocSegmentSheetTemporary{
		IsDeleted:  true,
		UpdateTime: time.Now(),
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	return d.BatchUpdateDocSegmentSheet(ctx, updateColumns, filter, sheet, batchSize)
}

// RealityDeleteDocSegmentSheet 物理删除
func (d *daoImpl) RealityDeleteDocSegmentSheet(ctx context.Context, filter *segEntity.DocSegmentSheetTemporaryFilter) (int64, error) {
	if filter == nil {
		logx.E(ctx, "RealityDeleteDocSegmentSheet|filter is null")
		return 0, errs.ErrSystem
	}
	tbl := d.tdsql.TDocSegmentSheetTemporary
	docSegmentSheetTemporaryTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentSheetTemporaryTableName)
	session = d.generateDocSegmentSheetCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Delete(&model.TDocSegmentOrgDataTemporary{})
	if res.Error != nil {
		logx.E(ctx, "RealityDeleteDocSegmentSheet|err:%+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// RealityBatchDeleteDocSheet 批量删除Sheet数据(物理删除)
func (d *daoImpl) RealityBatchDeleteDocSheet(ctx context.Context,
	filter *segEntity.DocSegmentSheetTemporaryFilter, batchSize int) error {
	logx.D(ctx, "RealityBatchDeleteDocSheet|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = batchSize
		rowsAffected, err := d.RealityDeleteDocSegmentSheet(ctx, filter)
		if err != nil {
			logx.E(ctx, "RealityBatchDeleteDocSheet failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}

func ConvertDocSegmentSheetTemporaryDataPO2DO(p *model.TDocSegmentSheetTemporary) *segEntity.DocSegmentSheetTemporary {
	if p == nil {
		return nil
	}
	return &segEntity.DocSegmentSheetTemporary{
		BusinessID:                 p.BusinessID,
		AppBizID:                   p.AppBizID,
		DocBizID:                   p.DocBizID,
		CorpBizID:                  p.CorpBizID,
		StaffBizID:                 p.StaffBizID,
		Bucket:                     p.Bucket,
		Region:                     p.Region,
		CosURL:                     p.CosURL,
		CosHash:                    p.CosHash,
		FileName:                   p.FileName,
		FileType:                   p.FileType,
		SheetOrder:                 int(p.SheetOrder),
		SheetName:                  p.SheetName,
		SheetTotalNum:              int(p.SheetTotalNum),
		Version:                    int(p.Version),
		IsDeleted:                  p.IsDeleted,
		IsDisabled:                 p.IsDisabled,
		IsDisabledRetrievalEnhance: p.IsDisabledRetrievalEnhance,
		CreateTime:                 p.CreateTime,
		UpdateTime:                 p.UpdateTime,
		AuditStatus:                p.AuditStatus,
	}
}

func BatchConvertDocSegmentSheetTemporaryDataPO2DO(p []*model.TDocSegmentSheetTemporary) []*segEntity.DocSegmentSheetTemporary {
	if p == nil || len(p) == 0 {
		return nil
	}
	var result []*segEntity.DocSegmentSheetTemporary
	for _, v := range p {
		result = append(result, ConvertDocSegmentSheetTemporaryDataPO2DO(v))
	}
	return result
}

func ConvertDocSegmentSheetTemporaryDataDO2PO(d *segEntity.DocSegmentSheetTemporary) *model.TDocSegmentSheetTemporary {
	if d == nil {
		return nil
	}
	return &model.TDocSegmentSheetTemporary{
		BusinessID:                 d.BusinessID,
		DocBizID:                   d.DocBizID,
		AppBizID:                   d.AppBizID,
		CorpBizID:                  d.CorpBizID,
		StaffBizID:                 d.StaffBizID,
		SheetOrder:                 int32(d.SheetOrder),
		SheetName:                  d.SheetName,
		Bucket:                     d.Bucket,
		Region:                     d.Region,
		CosURL:                     d.CosURL,
		CosHash:                    d.CosHash,
		FileName:                   d.FileName,
		FileType:                   d.FileType,
		SheetTotalNum:              int32(d.SheetTotalNum),
		Version:                    int32(d.Version),
		IsDeleted:                  d.IsDeleted,
		IsDisabled:                 d.IsDisabled,
		IsDisabledRetrievalEnhance: d.IsDisabledRetrievalEnhance,
		CreateTime:                 d.CreateTime,
		UpdateTime:                 d.UpdateTime,
		AuditStatus:                d.AuditStatus,
	}
}

func BatchConvertDocSegmentSheetTemporaryDataDO2PO(d []*segEntity.DocSegmentSheetTemporary) []*model.TDocSegmentSheetTemporary {
	if d == nil || len(d) == 0 {
		return nil
	}
	var result []*model.TDocSegmentSheetTemporary
	for _, v := range d {
		result = append(result, ConvertDocSegmentSheetTemporaryDataDO2PO(v))
	}
	return result
}
