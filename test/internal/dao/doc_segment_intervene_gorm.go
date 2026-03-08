package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"gorm.io/gorm"
)

const (
	EditAction                     = 0
	InsertAction                   = 1
	DocSegmentInterveneMaxPageSize = 200
)

var globalDocSegmentOrgDataTemporaryDao *DocSegmentOrgDataTemporaryDao

const (
	docSegmentOrgDataTemporaryTableName                 = "t_doc_segment_org_data_temporary"
	DocSegmentOrgDataTemporaryTblColBusinessID          = "business_id"
	DocSegmentOrgDataTemporaryTblColAppBizID            = "app_biz_id"
	DocSegmentOrgDataTemporaryTblColCorpBizID           = "corp_biz_id"
	DocSegmentOrgDataTemporaryTblColStaffBizID          = "staff_biz_id"
	DocSegmentOrgDataTemporaryTblColDocBizID            = "doc_biz_id"
	DocSegmentOrgDataTemporaryTblColOrgData             = "org_data"
	DocSegmentOrgDataTemporaryTblColAddMethod           = "add_method"
	DocSegmentOrgDataTemporaryTblColAction              = "action"
	DocSegmentOrgDataTemporaryTblColSegmentType         = "segment_type"
	DocSegmentOrgDataTemporaryTblColOrgPageNumbers      = "org_page_numbers"
	DocSegmentOrgDataTemporaryTblColOriginOrgDataID     = "origin_org_data_id"
	DocSegmentOrgDataTemporaryTblColLastOrgDataID       = "last_org_data_id"
	DocSegmentOrgDataTemporaryTblColAfterOrgDataID      = "after_org_data_id"
	DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID = "last_origin_org_data_id"
	DocSegmentOrgDataTemporaryTblColIsDeleted           = "is_deleted"
	DocSegmentOrgDataTemporaryTblColIsDisabled          = "is_disabled"
	DocSegmentOrgDataTemporaryTblColCreateTime          = "create_time"
	DocSegmentOrgDataTemporaryTblColUpdateTime          = "update_time"
	DocSegmentOrgDataTemporaryTblColAuditStatus         = "audit_status"
	DocSegmentOrgDataTemporaryTblColSheetName           = "sheet_name"
)

const (
	getTemporarySegmentByDocIDAndKeywords = `
		SELECT
    		/*+ MAX_EXECUTION_TIME(20000) */ %s
		FROM
		    %s
		WHERE
		    corp_biz_id = ? AND app_biz_id = ? AND doc_biz_id = ? AND is_deleted = ? AND org_data LIKE ? %s
		ORDER BY
		    create_time ASC
		LIMIT ?,?
		`
)

var DocSegmentOrgDataTemporaryTblColList = []string{
	DocSegmentOrgDataTemporaryTblColBusinessID,
	DocSegmentOrgDataTemporaryTblColAppBizID,
	DocSegmentOrgDataTemporaryTblColCorpBizID,
	DocSegmentOrgDataTemporaryTblColStaffBizID,
	DocSegmentOrgDataTemporaryTblColDocBizID,
	DocSegmentOrgDataTemporaryTblColOrgData,
	DocSegmentOrgDataTemporaryTblColAddMethod,
	DocSegmentOrgDataTemporaryTblColAction,
	DocSegmentOrgDataTemporaryTblColSegmentType,
	DocSegmentOrgDataTemporaryTblColOrgPageNumbers,
	DocSegmentOrgDataTemporaryTblColOriginOrgDataID,
	DocSegmentOrgDataTemporaryTblColLastOrgDataID,
	DocSegmentOrgDataTemporaryTblColAfterOrgDataID,
	DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
	DocSegmentOrgDataTemporaryTblColIsDeleted,
	DocSegmentOrgDataTemporaryTblColIsDisabled,
	DocSegmentOrgDataTemporaryTblColCreateTime,
	DocSegmentOrgDataTemporaryTblColUpdateTime,
	DocSegmentOrgDataTemporaryTblColAuditStatus,
	DocSegmentOrgDataTemporaryTblColSheetName,
}

type DocSegmentOrgDataTemporaryDao struct {
	BaseDao
}

// GetDocSegmentOrgDataTemporaryDao 获取全局的数据操作对象
func GetDocSegmentOrgDataTemporaryDao() *DocSegmentOrgDataTemporaryDao {
	if globalDocSegmentOrgDataTemporaryDao == nil {
		globalDocSegmentOrgDataTemporaryDao = &DocSegmentOrgDataTemporaryDao{*globalBaseDao}
	}
	return globalDocSegmentOrgDataTemporaryDao
}

type DocSegmentOrgDataTemporaryFilter struct {
	BusinessIDs          []string
	CorpBizID            uint64
	AppBizID             uint64
	DocBizID             uint64
	IsDeleted            *int
	Action               *int
	OrgData              string
	OriginOrgDataIDs     []string
	LastOriginOrgDataIDs []string
	LastOrgDataID        string

	Keywords       string
	AuditStatus    []uint32
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
	SheetName      string
}

// CreateDocSegmentOrgData 创建切片数据(事务)
func (d *DocSegmentOrgDataTemporaryDao) CreateDocSegmentOrgData(ctx context.Context, tx *gorm.DB, orgData *model.DocSegmentOrgDataTemporary) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	res := tx.WithContext(ctx).Table(docSegmentOrgDataTemporaryTableName).Create(&orgData)
	if res.Error != nil {
		log.ErrorContextf(ctx, "CreateDocSegmentOrgData execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocSegmentOrgDataTemporaryDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocSegmentOrgDataTemporaryFilter) {
	if filter.CorpBizID != 0 {
		session = session.Where(DocSegmentOrgDataTemporaryTblColCorpBizID+sqlEqual, filter.CorpBizID)
	}
	if filter.AppBizID != 0 {
		session = session.Where(DocSegmentOrgDataTemporaryTblColAppBizID+sqlEqual, filter.AppBizID)
	}
	if filter.DocBizID != 0 {
		session = session.Where(DocSegmentOrgDataTemporaryTblColDocBizID+sqlEqual, filter.DocBizID)
	}
	if len(filter.BusinessIDs) > 0 {
		if len(filter.BusinessIDs) == 1 {
			session = session.Where(DocSegmentOrgDataTemporaryTblColBusinessID+sqlEqual, filter.BusinessIDs[0])
		} else {
			session = session.Where(DocSegmentOrgDataTemporaryTblColBusinessID+sqlIn, filter.BusinessIDs)
		}
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocSegmentOrgDataTemporaryTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	if filter.Action != nil {
		session = session.Where(DocSegmentOrgDataTemporaryTblColAction+sqlEqual, *filter.Action)
	}
	if len(filter.OriginOrgDataIDs) != 0 {
		session = session.Where(DocSegmentOrgDataTemporaryTblColOriginOrgDataID+sqlIn, filter.OriginOrgDataIDs)
	}
	if len(filter.LastOriginOrgDataIDs) != 0 {
		session = session.Where(DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID+sqlIn, filter.LastOriginOrgDataIDs)
	}
	if filter.LastOrgDataID != "" {
		session = session.Where(DocSegmentOrgDataTemporaryTblColLastOrgDataID+sqlEqual, filter.LastOrgDataID)
	}
	// 筛选审核状态
	if len(filter.AuditStatus) != 0 {
		session = session.Where(DocSegmentOrgDataTemporaryTblColAuditStatus+sqlIn, filter.AuditStatus)
	}
	if filter.SheetName != "" {
		session = session.Where(DocSegmentOrgDataTemporaryTblColSheetName+sqlEqual, filter.SheetName)
	}
}

// GetDocOrgDataList 获取切片数据
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgDataList(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataTemporaryFilter) ([]*model.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.DocSegmentOrgDataTemporary, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocSegmentOrgDataList|filter is null")
		return orgDataList, errs.ErrSystem
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSegmentOrgDataTemporaryTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
				log.ErrorContextf(ctx, "GetDocSegmentOrgDataList|invalid order direction:%s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&orgDataList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocSegmentOrgDataList execute sql failed, err: %+v", res.Error)
		return orgDataList, res.Error
	}
	return orgDataList, nil
}

// GetDocOrgDataByDocBizID 获取文档更新/新增的切片
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgDataByDocBizID(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataTemporaryFilter) ([]*model.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.DocSegmentOrgDataTemporary, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocOrgDataByDocBizID|filter is null")
		return orgDataList, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return orgDataList, nil
	}
	if filter.Limit > DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		log.ErrorContextf(ctx, "GetDocOrgDataByDocBizID|is limit too large:%d", filter.Limit)
		return orgDataList, errs.ErrGetDocSegmentTooLarge
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSegmentOrgDataTemporaryTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
				log.ErrorContextf(ctx, "GetDocOrgDataByDocBizID|invalid order direction:%s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&orgDataList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataByDocBizID execute sql failed, err: %+v", res.Error)
		return orgDataList, res.Error
	}
	return orgDataList, nil
}

// GetDocOrgDataListByKeyWords 通过关键词获取切片
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgDataListByKeyWords(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataTemporaryFilter) ([]*model.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.DocSegmentOrgDataTemporary, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocOrgDataListByKeyWords|filter is null")
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

	keywordsArg := fmt.Sprintf("%%%s%%", special.Replace(filter.Keywords))
	queryValues := []interface{}{filter.CorpBizID, filter.AppBizID, filter.DocBizID,
		model.DocIsNotDeleted, keywordsArg}
	condition := ""
	if len(filter.AuditStatus) != 0 {
		condition += fmt.Sprintf(` AND audit_status IN (%s)`, placeholder(len(filter.AuditStatus)))
		queryValues = append(queryValues, filter.AuditStatus)
	}
	if filter.SheetName != "" {
		condition += fmt.Sprintf(` AND sheet_name = ?`)
		queryValues = append(queryValues, filter.SheetName)
	}
	querySQL := fmt.Sprintf(getTemporarySegmentByDocIDAndKeywords, strings.Join(selectColumns, ","),
		docSegmentOrgDataTemporaryTableName, condition)
	log.InfoContextf(ctx, "GetDocOrgDataListByKeyWords|querySQL:%s", querySQL)

	queryValues = append(queryValues, filter.Offset, filter.Limit)
	res := d.tdsqlGormDB.WithContext(ctx).Raw(querySQL, queryValues...).Scan(&orgDataList)

	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataListByKeyWords|execute sql failed|err: %+v", res.Error)
		return orgDataList, res.Error
	}
	return orgDataList, nil
}

// GetEditOrgData 获取编辑的切片数据
func (d *DocSegmentOrgDataTemporaryDao) GetEditOrgData(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataTemporaryFilter) ([]*model.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.DocSegmentOrgDataTemporary, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetEditOrgData|filter is null")
		return orgDataList, errs.ErrSystem
	}
	actionFlag := EditAction
	if filter.Action != nil && *filter.Action != actionFlag {
		// 非编辑动作
		err := errors.New(fmt.Sprintf("action is not edit action, action: %v", *filter.Action))
		log.ErrorContextf(ctx, "GetEditOrgData err: %+v", err)
		return orgDataList, err
	}
	if len(filter.OriginOrgDataIDs) == 0 {
		// 为0正常返回空结果即可
		log.WarnContextf(ctx, "GetEditOrgData|len(OriginOrgDataIDs):%d", len(filter.OriginOrgDataIDs))
		return orgDataList, nil
	}
	if len(filter.OriginOrgDataIDs) > DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("origin org data ids too many, limit: %d", DocSegmentInterveneMaxPageSize))
		log.ErrorContextf(ctx, "GetEditOrgData err: %+v", err)
		return orgDataList, err
	}
	return d.GetDocOrgDataList(ctx, selectColumns, filter)
}

// GetInsertOrgData 获取新增的切片数据
func (d *DocSegmentOrgDataTemporaryDao) GetInsertOrgData(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataTemporaryFilter) ([]*model.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.DocSegmentOrgDataTemporary, 0)
	if filter == nil {
		log.ErrorContextf(ctx, "GetInsertOrgData|filter is null")
		return orgDataList, errs.ErrSystem
	}
	actionFlag := InsertAction
	if filter.Action != nil && *filter.Action != actionFlag {
		// 非新增动作
		err := fmt.Errorf("action is not insert action, action: %v", *filter.Action)
		log.ErrorContextf(ctx, "GetInsertOrgData err: %+v", err)
		return orgDataList, err
	}
	if len(filter.LastOriginOrgDataIDs) == 0 {
		// 为0正常返回空结果即可
		log.WarnContextf(ctx, "GetEditOrgData|len(LastOriginOrgDataIDs):%d", len(filter.LastOriginOrgDataIDs))
		return orgDataList, nil
	}
	if len(filter.LastOriginOrgDataIDs) > DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		err := fmt.Errorf("origin org data ids too many, limit: %d", DocSegmentInterveneMaxPageSize)
		log.ErrorContextf(ctx, "GetInsertOrgData err: %+v", err)
		return orgDataList, err
	}
	return d.GetDocOrgDataList(ctx, selectColumns, filter)
}

// GetDocOrgData 获取单个切片数据
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgData(ctx context.Context, selectColumns []string,
	filter *DocSegmentOrgDataTemporaryFilter) (*model.DocSegmentOrgDataTemporary, error) {
	orgData := &model.DocSegmentOrgDataTemporary{}
	if filter == nil {
		log.ErrorContextf(ctx, "GetDocOrgData|filter is null")
		return orgData, errs.ErrSystem
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSegmentOrgDataTemporaryTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	res := session.Take(&orgData)
	if res.Error != nil {
		log.WarnContextf(ctx, "GetDocOrgDataByBizID execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return orgData, nil
}

// GetDocOrgDataByBizID 获取单个切片数据
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgDataByBizID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID uint64, businessID string) (*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetDocOrgDataByBizID|businessID:%s", businessID)
	deletedFlag := IsNotDeleted
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		IsDeleted:   &deletedFlag,
		BusinessIDs: []string{businessID},
	}
	return d.GetDocOrgData(ctx, selectColumns, filter)
}

// GetDocOrgDataCount 获取切片总数
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgDataCount(ctx context.Context,
	filter *DocSegmentOrgDataTemporaryFilter) (int64, error) {
	count := int64(0)
	session := d.tdsqlGormDB.WithContext(ctx).Table(docSegmentOrgDataTemporaryTableName)
	d.generateCondition(ctx, session, filter)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCount execute sql failed, err: %+v", res.Error)
		return count, res.Error
	}
	return count, nil
}

// GetDocOrgDataByLastOrgDataID 获取切片数据
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgDataByLastOrgDataID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID uint64, lastOrgDataID string) (*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetDocOrgDataByLastOrgDataID|lastOrgDataID:%s", lastOrgDataID)
	deletedFlag := IsNotDeleted
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:     corpBizID,
		AppBizID:      appBizID,
		DocBizID:      docBizID,
		IsDeleted:     &deletedFlag,
		LastOrgDataID: lastOrgDataID,
	}
	return d.GetDocOrgData(ctx, selectColumns, filter)
}

// GetDocOrgDataByOriginOrgDataID 获取切片数据
func (d *DocSegmentOrgDataTemporaryDao) GetDocOrgDataByOriginOrgDataID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID uint64, originOrgDataID string) (*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetDocOrgDataByOriginOrgDataID|originOrgDataID:%s", originOrgDataID)
	deletedFlag := IsNotDeleted
	actionFlag := EditAction
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:        corpBizID,
		AppBizID:         appBizID,
		DocBizID:         docBizID,
		IsDeleted:        &deletedFlag,
		Action:           &actionFlag,
		OriginOrgDataIDs: []string{originOrgDataID},
	}
	return d.GetDocOrgData(ctx, selectColumns, filter)
}

// UpdateDocSegmentOrgData 更新切片数据
func (d *DocSegmentOrgDataTemporaryDao) UpdateDocSegmentOrgData(ctx context.Context, tx *gorm.DB, updateColumns []string,
	filter *DocSegmentOrgDataTemporaryFilter, orgData *model.DocSegmentOrgDataTemporary) (int64, error) {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	if filter == nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentOrgData|filter is null")
		return 0, errs.ErrSystem
	}
	if orgData == nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentOrgData|orgData is null")
		return 0, errs.ErrSystem
	}
	session := tx.WithContext(ctx).Table(docSegmentOrgDataTemporaryTableName).Select(updateColumns)
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

// UpdateDocSegmentOrgDataContent 编辑切片内容
func (d *DocSegmentOrgDataTemporaryDao) UpdateDocSegmentOrgDataContent(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string, orgData string) error {
	updateColumns := []string{
		DocSegmentOrgDataTemporaryTblColOrgData,
		DocSegmentOrgDataTemporaryTblColUpdateTime,
	}
	segment := &model.DocSegmentOrgDataTemporary{
		OrgData:    orgData,
		UpdateTime: time.Now(),
	}
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, segment)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateDocSegmentOrgDataContent failed|err: %+v", err)
		return err
	}
	return nil
}

// DeleteDocSegmentOrgData 删除切片数据
func (d *DocSegmentOrgDataTemporaryDao) DeleteDocSegmentOrgData(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentOrgDataTemporaryTblColIsDeleted, DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &model.DocSegmentOrgDataTemporary{
		IsDeleted:  IsDeleted,  // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, segment)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteDocSegmentOrgData failed|err: %+v", err)
	}
	return nil
}

// DisabledDocSegmentOrgData 停用切片
func (d *DocSegmentOrgDataTemporaryDao) DisabledDocSegmentOrgData(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentOrgDataTemporaryTblColIsDisabled, DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &model.DocSegmentOrgDataTemporary{
		IsDisabled: model.SegmentIsDisabled, // 停用切片
		UpdateTime: time.Now(),              // 更新时间
	}
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, segment)
	if err != nil {
		log.ErrorContextf(ctx, "DisabledDocSegmentOrgData failed|err: %+v", err)
	}
	return nil
}

// EnableDocSegmentOrgData 启用切片
func (d *DocSegmentOrgDataTemporaryDao) EnableDocSegmentOrgData(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentOrgDataTemporaryTblColIsDisabled, DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &model.DocSegmentOrgDataTemporary{
		IsDisabled: model.SegmentIsEnable, // 启用切片
		UpdateTime: time.Now(),            // 更新时间
	}
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, segment)
	if err != nil {
		log.ErrorContextf(ctx, "EnableDocSegmentOrgData failed|err: %+v", err)
	}
	return nil
}

// UpdateDocSegmentAuditStatus 更新切片审核状态
func (d *DocSegmentOrgDataTemporaryDao) UpdateDocSegmentAuditStatus(ctx context.Context, tx *gorm.DB, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string, auditStatus uint32) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentOrgDataTemporaryTblColAuditStatus, DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &model.DocSegmentOrgDataTemporary{
		AuditStatus: auditStatus,
		UpdateTime:  time.Now(), // 更新时间
	}
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentOrgData(ctx, tx, updateColumns, filter, segment)
	if err != nil {
		log.ErrorContextf(ctx, "EnableDocSegmentOrgData failed|err: %+v", err)
	}
	return nil
}

// BatchUpdateDocOrgData 批量更新
func (d *DocSegmentOrgDataTemporaryDao) BatchUpdateDocOrgData(ctx context.Context, tx *gorm.DB,
	updateColumns []string, filter *DocSegmentOrgDataTemporaryFilter, orgData *model.DocSegmentOrgDataTemporary,
	batchSize int) error {
	log.DebugContextf(ctx, "BatchUpdateDocOrgData|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
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

// BatchDeleteDocOrgDataByDocBizID 批量删除干预中的OrgData数据(逻辑删除)
func (d *DocSegmentOrgDataTemporaryDao) BatchDeleteDocOrgDataByDocBizID(ctx context.Context, tx *gorm.DB, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	log.DebugContextf(ctx, "BatchDeleteDocOrgDataByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocSegmentOrgDataTemporaryTblColIsDeleted,
		DocSegmentOrgDataTemporaryTblColUpdateTime}
	orgData := &model.DocSegmentOrgDataTemporary{
		IsDeleted:  model.IsDeleted,
		UpdateTime: time.Now(),
	}
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	return d.BatchUpdateDocOrgData(ctx, tx, updateColumns, filter, orgData, batchSize)
}

// BatchRecoverDocOrgDataByDocBizID 批量恢复OrgData数据
func (d *DocSegmentOrgDataTemporaryDao) BatchRecoverDocOrgDataByDocBizID(ctx context.Context, tx *gorm.DB, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	log.DebugContextf(ctx, "BatchDeleteDocSegmentOrgDataDaoByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	updateColumns := []string{DocSegmentOrgDataTemporaryTblColIsDeleted,
		DocSegmentOrgDataTemporaryTblColUpdateTime}
	orgData := &model.DocSegmentOrgDataTemporary{
		IsDeleted:  IsNotDeleted,
		UpdateTime: time.Now(),
	}
	deleteFlag := IsDeleted
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
		IsDeleted: &deleteFlag,
	}
	return d.BatchUpdateDocOrgData(ctx, tx, updateColumns, filter, orgData, batchSize)
}

// RealityDeleteDocSegment 物理删除
func (d *DocSegmentOrgDataTemporaryDao) RealityDeleteDocSegment(ctx context.Context, tx *gorm.DB,
	filter *DocSegmentOrgDataTemporaryFilter) (int64, error) {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	if filter == nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegment|filter is null")
		return 0, errs.ErrSystem
	}
	session := tx.WithContext(ctx).Table(docSegmentOrgDataTemporaryTableName)
	d.generateCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Delete(&model.DocSegmentOrgDataTemporary{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegment|err:%+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// RealityDeleteDocSegmentOrgDataByOrgDataBizID 物理删除某个切片
func (d *DocSegmentOrgDataTemporaryDao) RealityDeleteDocSegmentOrgDataByOrgDataBizID(ctx context.Context, tx *gorm.DB, corpBizID,
	appBizID, docBizID uint64, businessID string) error {
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: []string{businessID},
		Limit:       uint32(1),
	}
	_, err := d.RealityDeleteDocSegment(ctx, tx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "RealityDeleteDocSegmentOrgDataByOrgDataBizID failed for BusinessID: %s, err: %+v", businessID, err)
		return err
	}
	return nil
}

// RealityBatchDeleteDocOrgData 批量删除OrgData数据(物理删除)
func (d *DocSegmentOrgDataTemporaryDao) RealityBatchDeleteDocOrgData(ctx context.Context, tx *gorm.DB,
	filter *DocSegmentOrgDataTemporaryFilter, routerAppBizID uint64, batchSize int) error {
	log.DebugContextf(ctx, "RealityBatchDeleteDocOrgData|routerAppBizID:%d|batchSize:%d",
		routerAppBizID, batchSize)
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
