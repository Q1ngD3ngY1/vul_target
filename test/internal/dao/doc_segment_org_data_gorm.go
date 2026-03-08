package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"gorm.io/gorm"
)

var globalDocSegmentOrgDataDao *DocSegmentOrgDataDao

const (
	docSegmentOrgDataTableName                = "t_doc_segment_org_data"
	DocSegmentOrgDataTblColBusinessID         = "business_id"
	DocSegmentOrgDataTblColAppBizID           = "app_biz_id"
	DocSegmentOrgDataTblColCorpBizID          = "corp_biz_id"
	DocSegmentOrgDataTblColStaffBizID         = "staff_biz_id"
	DocSegmentOrgDataTblColDocBizID           = "doc_biz_id"
	DocSegmentOrgDataTblColOrgData            = "org_data"
	DocSegmentOrgDataTblColAddMethod          = "add_method"
	DocSegmentOrgDataTblColSegmentType        = "segment_type"
	DocSegmentOrgDataTblColOrgPageNumbers     = "org_page_numbers"
	DocSegmentOrgDataTblColOrgSheetData       = "sheet_data"
	DocSegmentOrgDataTblColIsTemporaryDeleted = "is_temporary_deleted"
	DocSegmentOrgDataTblColIsDeleted          = "is_deleted"
	DocSegmentOrgDataTblColIsDisabled         = "is_disabled"
	DocSegmentOrgDataTblColCreateTime         = "create_time"
	DocSegmentOrgDataTblColUpdateTime         = "update_time"
	DocSegmentOrgDataTblColSheetName          = "sheet_name"
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

var DocSegmentOrgDataTblColList = []string{
	DocSegmentOrgDataTblColBusinessID,
	DocSegmentOrgDataTblColAppBizID,
	DocSegmentOrgDataTblColCorpBizID,
	DocSegmentOrgDataTblColStaffBizID,
	DocSegmentOrgDataTblColDocBizID,
	DocSegmentOrgDataTblColOrgData,
	DocSegmentOrgDataTblColAddMethod,
	DocSegmentOrgDataTblColSegmentType,
	DocSegmentOrgDataTblColOrgPageNumbers,
	DocSegmentOrgDataTblColOrgSheetData,
	DocSegmentOrgDataTblColIsTemporaryDeleted,
	DocSegmentOrgDataTblColIsDisabled,
	DocSegmentOrgDataTblColIsDeleted,
	DocSegmentOrgDataTblColCreateTime,
	DocSegmentOrgDataTblColUpdateTime,
	DocSegmentOrgDataTblColSheetName,
}

type DocSegmentOrgDataDao struct {
	BaseDao
	tableName string
}

// DocSegmentOrgDataDao 获取全局的数据操作对象
func GetDocSegmentOrgDataDao() *DocSegmentOrgDataDao {
	if globalDocSegmentOrgDataDao == nil {
		globalDocSegmentOrgDataDao = &DocSegmentOrgDataDao{*globalBaseDao, docSegmentOrgDataTableName}
	}
	return globalDocSegmentOrgDataDao
}

type DocSegmentOrgDataFilter struct {
	CorpBizID          uint64
	AppBizID           uint64
	DocBizID           uint64
	BusinessIDs        []uint64
	IsDeleted          *int
	IsTemporaryDeleted *int
	AddMethod          *int
	Keywords           string
	Offset             uint32
	Limit              uint32
	OrderColumn        []string
	OrderDirection     []string
	RouterAppBizID     uint64
	SheetName          string
}

// CreateDocSegmentOrgData 创建org_data
func (d *DocSegmentOrgDataDao) CreateDocSegmentOrgData(ctx context.Context, orgData *model.DocSegmentOrgData) error {
	if orgData == nil {
		log.ErrorContextf(ctx, "CreateDocSegmentOrgData|orgData is null")
		return errs.ErrSystem
	}
	db, err := GetGorm(ctx, orgData.AppBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocSegmentOrgData|get gorm failed|err: %+v", err)
		return err
	}
	res := db.WithContext(ctx).Table(docSegmentOrgDataTableName).Create(&orgData)
	if res.Error != nil {
		log.ErrorContextf(ctx, "CreateDocSegmentOrgData execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocSegmentOrgDataDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocSegmentOrgDataFilter) {
	if filter.CorpBizID != 0 {
		session = session.Where(DocSegmentOrgDataTblColCorpBizID+sqlEqual, filter.CorpBizID)
	}
	if filter.AppBizID != 0 {
		session = session.Where(DocSegmentOrgDataTblColAppBizID+sqlEqual, filter.AppBizID)
	}
	if filter.DocBizID != 0 {
		session = session.Where(DocSegmentOrgDataTblColDocBizID+sqlEqual, filter.DocBizID)
	}
	if len(filter.BusinessIDs) > 0 {
		if len(filter.BusinessIDs) == 1 {
			session = session.Where(DocSegmentOrgDataTblColBusinessID+sqlEqual, filter.BusinessIDs[0])
		} else {
			session = session.Where(DocSegmentOrgDataTblColBusinessID+sqlIn, filter.BusinessIDs)
		}
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session.Where(DocSegmentOrgDataTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	if filter.IsTemporaryDeleted != nil {
		session.Where(DocSegmentOrgDataTblColIsTemporaryDeleted+sqlEqual, *filter.IsTemporaryDeleted)
	}
	if filter.AddMethod != nil {
		session.Where(DocSegmentOrgDataTblColAddMethod+sqlEqual, *filter.AddMethod)
	}
	if filter.SheetName != "" {
		session.Where(DocSegmentOrgDataTblColSheetName+sqlEqual, filter.SheetName)
	}
}

func (d *DocSegmentOrgDataDao) GetDocOrgDataList(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataFilter) ([]*model.DocSegmentOrgData, error) {
	orgDataList := make([]*model.DocSegmentOrgData, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocOrgDataList|filter is null")
		return orgDataList, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		log.ErrorContextf(ctx, "GetDocOrgDataList|filter RouterAppBizID is 0")
		return orgDataList, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return orgDataList, nil
	}
	if filter.Limit > DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		log.ErrorContextf(ctx, "GetDocOrgDataList|is limit too large:%d", filter.Limit)
		return orgDataList, errs.ErrGetDocSegmentTooLarge
	}
	var session *gorm.DB
	db, err := GetGorm(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataList|get gorm failed|err: %+v", err)
		return orgDataList, err
	}
	session = db.WithContext(ctx).Table(docSegmentOrgDataTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
				log.ErrorContextf(ctx, "GetDocOrgDataList|invalid order direction:%s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&orgDataList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataList|execute sql failed|err: %+v", res.Error)
		return orgDataList, res.Error
	}
	return orgDataList, nil
}

// GetDocOrgDataListByKeyWords 通过关键词获取切片
func (d *DocSegmentOrgDataDao) GetDocOrgDataListByKeyWords(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataFilter) ([]*model.DocSegmentOrgData, error) {
	orgDataList := make([]*model.DocSegmentOrgData, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocOrgDataListByKeyWords|filter is null")
		return orgDataList, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		log.ErrorContextf(ctx, "GetDocOrgDataListByKeyWords|filter RouterAppBizID is 0")
		return orgDataList, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return orgDataList, nil
	}
	if filter.Limit > DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		log.ErrorContextf(ctx, "GetDocOrgDataListByKeyWords|is limit too large:%d", filter.Limit)
		return orgDataList, errs.ErrGetDocSegmentTooLarge
	}
	db, err := GetGorm(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataListByKeyWords|get gorm failed|err: %+v", err)
		return orgDataList, err
	}
	querySQL := fmt.Sprintf(getSegmentByDocIDAndKeywords, strings.Join(selectColumns, ","),
		docSegmentOrgDataTableName)
	keywordsArg := fmt.Sprintf("%%%s%%", special.Replace(filter.Keywords))
	res := db.WithContext(ctx).Raw(querySQL, filter.CorpBizID, filter.AppBizID, filter.DocBizID,
		model.DocIsNotDeleted, keywordsArg, filter.Offset, filter.Limit).Scan(&orgDataList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataListByKeyWords|execute sql failed|err: %+v", res.Error)
		return orgDataList, res.Error
	}
	return orgDataList, nil
}

// GetDocOrgData 获取单个切片数据
func (d *DocSegmentOrgDataDao) GetDocOrgData(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataFilter) (*model.DocSegmentOrgData, error) {
	orgData := &model.DocSegmentOrgData{}
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocOrgData|filter is null")
		return orgData, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		log.ErrorContextf(ctx, "GetDocOrgData|filter RouterAppBizID is 0")
		return orgData, errs.ErrSystem
	}
	db, err := GetGorm(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgData|get gorm failed|err: %+v", err)
		return orgData, err
	}
	session := db.WithContext(ctx).Table(docSegmentOrgDataTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	res := session.Take(&orgData)
	if res.Error != nil {
		log.WarnContextf(ctx, "GetDocOrgData execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return orgData, nil
}

// GetDocOrgDataByBizID 获取单个切片数据
func (d *DocSegmentOrgDataDao) GetDocOrgDataByBizID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID, businessID uint64) (*model.DocSegmentOrgData, error) {
	deleteFlag := IsNotDeleted
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:          corpBizID,
		AppBizID:           appBizID,
		DocBizID:           docBizID,
		BusinessIDs:        []uint64{businessID},
		IsDeleted:          &deleteFlag,
		IsTemporaryDeleted: &deleteFlag,
		RouterAppBizID:     appBizID,
	}
	return d.GetDocOrgData(ctx, selectColumns, filter)
}

// GetDocOrgDataCount 获取切片总数
func (d *DocSegmentOrgDataDao) GetDocOrgDataCount(ctx context.Context,
	filter *DocSegmentOrgDataFilter) (int64, error) {
	count := int64(0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCount|filter is null")
		return count, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		log.ErrorContextf(ctx, "GetDocOrgDataCount|filter RouterAppBizID is 0")
		return count, errs.ErrSystem
	}
	db, err := GetGorm(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCount|get gorm failed|err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(docSegmentOrgDataTableName)
	d.generateCondition(ctx, session, filter)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCount execute sql failed, err: %+v", res.Error)
		return count, res.Error
	}
	return count, nil
}

// GetLastOrgDataByCurrentOrgDataBizID 获取该切片的上一个切片，依赖business_id顺序
func (d *DocSegmentOrgDataDao) GetLastOrgDataByCurrentOrgDataBizID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID, curBusinessID uint64) (*model.DocSegmentOrgData, error) {
	db, err := GetGorm(ctx, appBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetSegmentByDocID|get gorm failed|err: %+v", err)
		return nil, err
	}
	session := db.WithContext(ctx).Table(docSegmentOrgDataTableName).Select(selectColumns)
	session = session.Where(DocSegmentOrgDataTblColCorpBizID+sqlEqual, corpBizID).Where(
		DocSegmentOrgDataTblColAppBizID+sqlEqual, appBizID).Where(
		DocSegmentOrgDataTblColDocBizID+sqlEqual, docBizID).Where(
		DocSegmentOrgDataTblColIsTemporaryDeleted+sqlEqual, IsNotDeleted).Where(
		DocSegmentOrgDataTblColBusinessID+sqlLess, curBusinessID).Order(
		DocSegmentOrgDataTblColBusinessID + " " + SqlOrderByDesc).Limit(1)
	orgData := &model.DocSegmentOrgData{}
	res := session.Take(&orgData)
	if res.Error != nil {
		log.WarnContextf(ctx, "GetLastOrgDataByCurrentOrgDataBizID execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return orgData, nil
}

// GetAfterOrgDataByCurrentOrgDataBizID 获取该切片的下一个切片，依赖business_id顺序
func (d *DocSegmentOrgDataDao) GetAfterOrgDataByCurrentOrgDataBizID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID, curBusinessID uint64) (*model.DocSegmentOrgData, error) {
	db, err := GetGorm(ctx, appBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetSegmentByDocID|get gorm failed|err: %+v", err)
		return nil, err
	}
	session := db.WithContext(ctx).Table(docSegmentOrgDataTableName).Select(selectColumns)
	session = session.Where(DocSegmentOrgDataTblColCorpBizID+sqlEqual, corpBizID).Where(
		DocSegmentOrgDataTblColAppBizID+sqlEqual, appBizID).Where(
		DocSegmentOrgDataTblColDocBizID+sqlEqual, docBizID).Where(
		DocSegmentOrgDataTblColIsTemporaryDeleted+sqlEqual, IsNotDeleted).Where(
		DocSegmentOrgDataTblColBusinessID+sqlMore, curBusinessID).Order(
		DocSegmentOrgDataTblColBusinessID + " " + SqlOrderByAsc).Limit(1)
	orgData := &model.DocSegmentOrgData{}
	res := session.Take(&orgData)
	if res.Error != nil {
		log.WarnContextf(ctx, "GetAfterOrgDataByCurrentOrgDataBizID execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return orgData, nil
}

// UpdateDocSegmentOrgData 更新切片数据
func (d *DocSegmentOrgDataDao) UpdateDocSegmentOrgData(ctx context.Context, tx *gorm.DB, updateColumns []string,
	filter *DocSegmentOrgDataFilter, orgData *model.DocSegmentOrgData) (int64, error) {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	if filter == nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentOrgData|filter is null")
		return 0, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		log.ErrorContextf(ctx, "UpdateDocSegmentOrgData|filter RouterAppBizID is 0")
		return 0, errs.ErrSystem
	}
	if orgData == nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentOrgData|orgData is null")
		return 0, errs.ErrSystem
	}
	db, err := GetGorm(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetSegmentByDocID|get gorm failed|err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(docSegmentOrgDataTableName).Select(updateColumns)
	d.generateCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Updates(orgData)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentOrgData failed|err: %+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// DeleteDocSegmentOrgData 删除org_data
func (d *DocSegmentOrgDataDao) DeleteDocSegmentOrgData(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{DocSegmentOrgDataTblColIsDeleted, DocSegmentOrgDataTblColUpdateTime}
	orgData := &model.DocSegmentOrgData{
		IsDeleted:  IsDeleted,  // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, orgData)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteDocSegmentOrgData failed for DocBizID: %d, err: %+v", docBizID, err)
		return err
	}
	return nil
}

// TemporaryDeleteDocSegmentOrgData 临时删除org_data
func (d *DocSegmentOrgDataDao) TemporaryDeleteDocSegmentOrgData(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{DocSegmentOrgDataTblColIsTemporaryDeleted, DocSegmentOrgDataTblColUpdateTime}
	orgData := &model.DocSegmentOrgData{
		IsTemporaryDeleted: IsDeleted,  // 是否删除
		UpdateTime:         time.Now(), // 更新时间
	}
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, orgData)
	if err != nil {
		log.ErrorContextf(ctx, "TemporaryDeleteDocSegmentOrgData failed for DocBizID: %d, err: %+v", docBizID, err)
		return err
	}
	return nil
}

// DisabledDocSegmentOrgData 停用切片
func (d *DocSegmentOrgDataDao) DisabledDocSegmentOrgData(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{DocSegmentOrgDataTblColIsDisabled, DocSegmentOrgDataTblColUpdateTime}
	orgData := &model.DocSegmentOrgData{
		IsDisabled: model.SegmentIsDisabled, // 停用切片
		UpdateTime: time.Now(),              // 更新时间
	}
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, orgData)
	if err != nil {
		log.ErrorContextf(ctx, "DisabledDocSegmentOrgData failed for DocBizID: %d, err: %+v", docBizID, err)
		return err
	}
	return nil
}

// EnableDocSegmentOrgData 启用切片
func (d *DocSegmentOrgDataDao) EnableDocSegmentOrgData(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{DocSegmentOrgDataTblColIsDisabled, DocSegmentOrgDataTblColUpdateTime}
	orgData := &model.DocSegmentOrgData{
		IsDisabled: model.SegmentIsEnable, // 启用切片
		UpdateTime: time.Now(),            // 更新时间
	}
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, orgData)
	if err != nil {
		log.ErrorContextf(ctx, "EnableDocSegmentOrgData failed for DocBizID: %d, err: %+v", docBizID, err)
		return err
	}
	return nil
}

// BatchUpdateDocOrgData 批量更新
func (d *DocSegmentOrgDataDao) BatchUpdateDocOrgData(ctx context.Context, tx *gorm.DB,
	updateColumns []string, filter *DocSegmentOrgDataFilter, orgData *model.DocSegmentOrgData, batchSize int) error {
	log.DebugContextf(ctx, "BatchUpdateDocOrgData|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	if filter == nil {
		log.ErrorContextf(ctx, "BatchUpdateDocOrgData|filter is null")
		return errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		log.ErrorContextf(ctx, "BatchUpdateDocOrgData|filter RouterAppBizID is 0")
		return errs.ErrSystem
	}
	if orgData == nil {
		log.ErrorContextf(ctx, "BatchUpdateDocOrgData|orgData is null")
		return errs.ErrSystem
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = uint32(batchSize)
		rowsAffected, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, orgData)
		if err != nil {
			log.ErrorContextf(ctx, "BatchUpdateDocOrgData failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}

// BatchDeleteDocOrgDataByDocBizID 批量删除OrgData数据(逻辑删除)
func (d *DocSegmentOrgDataDao) BatchDeleteDocOrgDataByDocBizID(ctx context.Context, tx *gorm.DB, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	log.DebugContextf(ctx, "BatchDeleteDocSegmentOrgDataDaoByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	updateColumns := []string{DocSegmentOrgDataTblColIsDeleted,
		DocSegmentOrgDataTblColUpdateTime}
	orgData := &model.DocSegmentOrgData{
		IsDeleted:  model.IsDeleted,
		UpdateTime: time.Now(),
	}
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		RouterAppBizID: appBizID,
	}
	return d.BatchUpdateDocOrgData(ctx, tx, updateColumns, filter, orgData, batchSize)
}

// BatchRecoverDocOrgDataByDocBizID 批量恢复OrgData数据
func (d *DocSegmentOrgDataDao) BatchRecoverDocOrgDataByDocBizID(ctx context.Context, tx *gorm.DB, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	log.DebugContextf(ctx, "BatchDeleteDocSegmentOrgDataDaoByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	updateColumns := []string{DocSegmentOrgDataTblColIsDeleted,
		DocSegmentOrgDataTblColUpdateTime}
	orgData := &model.DocSegmentOrgData{
		IsDeleted:  IsNotDeleted,
		UpdateTime: time.Now(),
	}
	deleteFlag := IsDeleted
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      &deleteFlag,
		RouterAppBizID: appBizID,
	}
	return d.BatchUpdateDocOrgData(ctx, tx, updateColumns, filter, orgData, batchSize)
}

// RealityDeleteDocSegment 物理删除
func (d *DocSegmentOrgDataDao) RealityDeleteDocSegment(ctx context.Context, tx *gorm.DB,
	filter *DocSegmentOrgDataFilter) (int64, error) {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	if filter == nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegment|filter is null")
		return 0, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		log.ErrorContextf(ctx, "RealityDeleteDocSegment|filter RouterAppBizID is 0")
		return 0, errs.ErrSystem
	}
	db, err := GetGorm(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegment|get gorm failed|err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(docSegmentOrgDataTableName)
	d.generateCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Delete(&model.DocSegmentOrgData{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegment|err:%+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// RealityDeleteDocSegmentOrgDataByOrgDataBizID 物理删除某个切片
func (d *DocSegmentOrgDataDao) RealityDeleteDocSegmentOrgDataByOrgDataBizID(ctx context.Context, tx *gorm.DB, corpBizID,
	appBizID, docBizID, businessID uint64) error {
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    []uint64{businessID},
		Limit:          uint32(1),
		RouterAppBizID: appBizID,
	}
	_, err := d.RealityDeleteDocSegment(ctx, tx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegmentOrgDataByOrgDataBizID failed for BusinessID: %d, err: %+v", businessID, err)
		return err
	}
	return nil
}

// RealityBatchDeleteDocOrgData 批量删除OrgData数据(物理删除)
func (d *DocSegmentOrgDataDao) RealityBatchDeleteDocOrgData(ctx context.Context, tx *gorm.DB,
	filter *DocSegmentOrgDataFilter, batchSize int) error {
	log.DebugContextf(ctx, "RealityBatchDeleteDocOrgData|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = uint32(batchSize)
		rowsAffected, err := d.RealityDeleteDocSegment(ctx, tx, filter)
		if err != nil {
			log.ErrorContextf(ctx, "RealityBatchDeleteDocOrgData failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}
