package segment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gorm"
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

// CreateDocSegmentOrgDataTemporary 创建切片数据(事务)
func (d *daoImpl) CreateDocSegmentOrgDataTemporary(ctx context.Context,
	orgData *segEntity.DocSegmentOrgDataTemporary) error {
	tbl := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tbl.TableName()
	tOrdData := ConvertDocSegmentOrgDataTemporaryDO2PO(orgData)
	res := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentOrgDataTemporaryTableName).Create(&tOrdData)
	if res.Error != nil {
		logx.E(ctx, "CreateDocSegmentOrgData execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateCondition(ctx context.Context, session *gorm.DB, filter *segEntity.DocSegmentOrgDataTemporaryFilter) *gorm.DB {
	tbl := d.tdsql.TDocSegmentOrgDataTemporary
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
	if filter.Action != nil {
		session = session.Where(tbl.Action.ColumnName().String()+util.SqlEqual, *filter.Action)
	}
	if len(filter.OriginOrgDataIDs) != 0 {
		session = session.Where(tbl.OriginOrgDataID.ColumnName().String()+util.SqlIn, filter.OriginOrgDataIDs)
	}
	if len(filter.LastOriginOrgDataIDs) != 0 {
		session = session.Where(tbl.LastOriginOrgDataID.ColumnName().String()+util.SqlIn, filter.LastOriginOrgDataIDs)
	}
	if filter.LastOrgDataID != "" {
		session = session.Where(tbl.LastOrgDataID.ColumnName().String()+util.SqlEqual, filter.LastOrgDataID)
	}
	// 筛选审核状态
	if len(filter.AuditStatus) != 0 {
		session = session.Where(tbl.AuditStatus.ColumnName().String()+util.SqlIn, filter.AuditStatus)
	}
	if filter.SheetName != "" {
		session = session.Where(tbl.SheetName.ColumnName().String()+util.SqlEqual, filter.SheetName)
	}
	return session
}

// GetDocTemporaryOrgDataList 获取切片数据
func (d *daoImpl) GetDocTemporaryOrgDataList(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.TDocSegmentOrgDataTemporary, 0)
	if filter == nil {
		logx.E(ctx, "GetDocSegmentOrgDataList|filter is null")
		return nil, errs.ErrSystem
	}
	tbl := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tbl.TableName()

	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentOrgDataTemporaryTableName)
	if len(selectColumns) > 0 {
		session = session.Select(selectColumns)
	}
	session = d.generateCondition(ctx, session, filter)
	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
				logx.E(ctx, "GetDocSegmentOrgDataList|invalid order direction:%s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&orgDataList)
	if res.Error != nil {
		logx.E(ctx, "GetDocSegmentOrgDataList execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentOrgDataTemporaryPO2DO(orgDataList), nil
}

// GetDocTemporaryOrgDataByDocBizID 获取文档更新/新增的切片
func (d *daoImpl) GetDocTemporaryOrgDataByDocBizID(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.TDocSegmentOrgDataTemporary, 0)
	if filter == nil {
		logx.E(ctx, "GetDocOrgDataByDocBizID|filter is null")
		return nil, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return nil, nil
	}
	if filter.Limit > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		logx.E(ctx, "GetDocOrgDataByDocBizID|is limit too large:%d", filter.Limit)
		return nil, errs.ErrGetDocSegmentTooLarge
	}
	tbl := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentOrgDataTemporaryTableName).Select(selectColumns)
	session = d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	if len(filter.OrderColumn) == len(filter.OrderDirection) {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
				logx.E(ctx, "GetDocOrgDataByDocBizID|invalid order direction:%s", filter.OrderDirection[i])
				continue
			}
			session.Order(orderColumn + " " + filter.OrderDirection[i])
		}
	}
	res := session.Find(&orgDataList)
	if res.Error != nil {
		logx.E(ctx, "GetDocOrgDataByDocBizID execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentOrgDataTemporaryPO2DO(orgDataList), nil
}

// GetDocTemporaryOrgDataListByKeyWords 通过关键词获取切片
func (d *daoImpl) GetDocTemporaryOrgDataListByKeyWords(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	orgDataList := make([]*model.TDocSegmentOrgDataTemporary, 0)
	if filter == nil {
		logx.E(ctx, "GetDocTemporaryOrgDataListByKeyWords|filter is null")
		return nil, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return nil, nil
	}
	if filter.Limit > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		logx.E(ctx, "GetDocOrgDataLisGetDocTemporaryOrgDataListByKeyWordstByKeyWords|is limit too large:%d", filter.Limit)
		return nil, errs.ErrGetDocSegmentTooLarge
	}

	keywordsArg := fmt.Sprintf("%%%s%%", util.Special.Replace(filter.Keywords))
	queryValues := []any{filter.CorpBizID, filter.AppBizID, filter.DocBizID,
		docEntity.DocIsNotDeleted, keywordsArg}
	condition := ""
	if len(filter.AuditStatus) != 0 {
		condition += fmt.Sprintf(` AND audit_status IN (%s)`, util.Placeholder(len(filter.AuditStatus)))
		for _, status := range filter.AuditStatus {
			queryValues = append(queryValues, status)
		}
	}
	if filter.SheetName != "" {
		condition += fmt.Sprintf(` AND sheet_name = ?`)
		queryValues = append(queryValues, filter.SheetName)
	}
	tbl := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tbl.TableName()
	querySQL := fmt.Sprintf(getTemporarySegmentByDocIDAndKeywords, strings.Join(selectColumns, ","),
		docSegmentOrgDataTemporaryTableName, condition)
	logx.I(ctx, "GetDocTemporaryOrgDataListByKeyWords|querySQL:%s", querySQL)

	queryValues = append(queryValues, filter.Offset, filter.Limit)
	res := tbl.WithContext(ctx).UnderlyingDB().Raw(querySQL, queryValues...).Scan(&orgDataList)

	if res.Error != nil {
		logx.E(ctx, "GetDocTemporaryOrgDataListByKeyWords|execute sql failed|err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertDocSegmentOrgDataTemporaryPO2DO(orgDataList), nil
}

// GetEditTemporaryOrgData 获取编辑的切片数据
func (d *daoImpl) GetEditTemporaryOrgData(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	if filter == nil {
		logx.E(ctx, "GetEditTemporaryOrgData|filter is null")
		return nil, errs.ErrSystem
	}
	actionFlag := segEntity.EditAction
	if filter.Action != nil && *filter.Action != actionFlag {
		// 非编辑动作
		err := errors.New(fmt.Sprintf("action is not edit action, action: %v", *filter.Action))
		logx.E(ctx, "GetEditTemporaryOrgData err: %+v", err)
		return nil, err
	}
	if len(filter.OriginOrgDataIDs) == 0 {
		// 为0正常返回空结果即可
		logx.W(ctx, "GetEditTemporaryOrgData|len(OriginOrgDataIDs):%d", len(filter.OriginOrgDataIDs))
		return nil, nil
	}
	if len(filter.OriginOrgDataIDs) > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("origin org data ids too many, limit: %d", segEntity.DocSegmentInterveneMaxPageSize))
		logx.E(ctx, "GetEditOrgData err: %+v", err)
		return nil, err
	}
	return d.GetDocTemporaryOrgDataList(ctx, selectColumns, filter)
}

// GetInsertTemporaryOrgData 获取新增的切片数据
func (d *daoImpl) GetInsertTemporaryOrgData(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	if filter == nil {
		logx.E(ctx, "GetInsertOrgData|filter is null")
		return nil, errs.ErrSystem
	}
	actionFlag := segEntity.InsertAction
	if filter.Action != nil && *filter.Action != actionFlag {
		// 非新增动作
		err := fmt.Errorf("action is not insert action, action: %v", *filter.Action)
		logx.E(ctx, "GetInsertOrgData err: %+v", err)
		return nil, err
	}
	if len(filter.LastOriginOrgDataIDs) == 0 {
		// 为0正常返回空结果即可
		logx.W(ctx, "GetEditOrgData|len(LastOriginOrgDataIDs):%d", len(filter.LastOriginOrgDataIDs))
		return nil, nil
	}
	if len(filter.LastOriginOrgDataIDs) > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		err := fmt.Errorf("origin org data ids too many, limit: %d", segEntity.DocSegmentInterveneMaxPageSize)
		logx.E(ctx, "GetInsertOrgData err: %+v", err)
		return nil, err
	}
	return d.GetDocTemporaryOrgDataList(ctx, selectColumns, filter)
}

// GetDocTemporaryOrgData 获取单个切片数据
func (d *daoImpl) GetDocTemporaryOrgData(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) (*segEntity.DocSegmentOrgDataTemporary, error) {
	orgData := &model.TDocSegmentOrgDataTemporary{}
	if filter == nil {
		logx.E(ctx, "GetDocOrgData|filter is null")
		return nil, errs.ErrSystem
	}

	tbl := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tbl.TableName()

	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentOrgDataTemporaryTableName).Select(selectColumns)
	session = d.generateCondition(ctx, session, filter)
	res := session.Take(orgData)
	if res.Error != nil {
		logx.W(ctx, "GetDocOrgDataByBizID execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return ConvertDocSegmentOrgDataTemporaryPO2DO(orgData), nil
}

// GetDocTemporaryOrgDataByBizID 获取单个切片数据
func (d *daoImpl) GetDocTemporaryOrgDataByBizID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID uint64, businessID string) (*segEntity.DocSegmentOrgDataTemporary, error) {
	logx.I(ctx, "GetDocTemporaryOrgDataByBizID|businessID:%s", businessID)
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		IsDeleted:   ptrx.Bool(false),
		BusinessIDs: []string{businessID},
	}
	return d.GetDocTemporaryOrgData(ctx, selectColumns, filter)
}

// GetDocTemporaryOrgDataCount 获取切片总数
func (d *daoImpl) GetDocTemporaryOrgDataCount(ctx context.Context,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) (int64, error) {
	count := int64(0)

	tbl := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tbl.TableName()

	session := tbl.WithContext(ctx).UnderlyingDB().Table(docSegmentOrgDataTemporaryTableName)
	session = d.generateCondition(ctx, session, filter)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "GetDocTemporaryOrgDataCount execute sql failed, err: %+v", res.Error)
		return count, res.Error
	}
	return count, nil
}

// GetDocTemporaryOrgDataByLastOrgDataID 获取切片数据
func (d *daoImpl) GetDocTemporaryOrgDataByLastOrgDataID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID uint64, lastOrgDataID string) (*segEntity.DocSegmentOrgDataTemporary, error) {
	logx.I(ctx, "GetDocTemporaryOrgDataByLastOrgDataID|lastOrgDataID:%s", lastOrgDataID)
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:     corpBizID,
		AppBizID:      appBizID,
		DocBizID:      docBizID,
		IsDeleted:     ptrx.Bool(false),
		LastOrgDataID: lastOrgDataID,
	}
	return d.GetDocTemporaryOrgData(ctx, selectColumns, filter)
}

// GetDocTemporaryOrgDataByOriginOrgDataID 获取切片数据
func (d *daoImpl) GetDocTemporaryOrgDataByOriginOrgDataID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID uint64, originOrgDataID string) (*segEntity.DocSegmentOrgDataTemporary, error) {
	logx.I(ctx, "GetDocTemporaryOrgDataByOriginOrgDataID|originOrgDataID:%s", originOrgDataID)
	actionFlag := segEntity.EditAction
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:        corpBizID,
		AppBizID:         appBizID,
		DocBizID:         docBizID,
		IsDeleted:        ptrx.Bool(false),
		Action:           &actionFlag,
		OriginOrgDataIDs: []string{originOrgDataID},
	}
	return d.GetDocTemporaryOrgData(ctx, selectColumns, filter)
}

// UpdateDocSegmentTemporaryOrgData 更新切片数据
func (d *daoImpl) UpdateDocSegmentTemporaryOrgData(ctx context.Context, updateColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter, orgData *segEntity.DocSegmentOrgDataTemporary) (int64, error) {
	tx := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tx.TableName()
	tOrgData := ConvertDocSegmentOrgDataTemporaryDO2PO(orgData)

	if filter == nil {
		logx.E(ctx, "UpdateDocSegmentTemporaryOrgData|filter is null")
		return 0, errs.ErrSystem
	}
	if orgData == nil {
		logx.E(ctx, "UpdateDocSegmentTemporaryOrgData|orgData is null")
		return 0, errs.ErrSystem
	}
	session := tx.WithContext(ctx).UnderlyingDB().Table(docSegmentOrgDataTemporaryTableName).Select(updateColumns)
	session = d.generateCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Updates(tOrgData)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocSegmentTemporaryOrgData failed|err: %+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// UpdateDocSegmentTemporaryOrgDataContent 编辑切片内容
func (d *daoImpl) UpdateDocSegmentTemporaryOrgDataContent(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string, orgData string) error {
	updateColumns := []string{
		segEntity.DocSegmentOrgDataTemporaryTblColOrgData,
		segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
	}
	segment := &segEntity.DocSegmentOrgDataTemporary{
		OrgData:    orgData,
		UpdateTime: time.Now(),
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, segment)
	if err != nil {
		logx.E(ctx, "UpdateDocSegmentTemporaryOrgDataContent failed|err: %+v", err)
		return err
	}
	return nil
}

// DeleteDocSegmentTemporaryOrgData 删除切片数据
func (d *daoImpl) DeleteDocSegmentTemporaryOrgData(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string) error {
	updateColumns := []string{segEntity.DocSegmentOrgDataTemporaryTblColIsDeleted, segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &segEntity.DocSegmentOrgDataTemporary{
		IsDeleted:  true,       // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, segment)
	if err != nil {
		logx.E(ctx, "DeleteDocSegmentTemporaryOrgData failed|err: %+v", err)
	}
	return nil
}

// DisabledDocSegmentTemporaryOrgData 停用切片
func (d *daoImpl) DisabledDocSegmentTemporaryOrgData(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string) error {

	updateColumns := []string{segEntity.DocSegmentOrgDataTemporaryTblColIsDisabled, segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &segEntity.DocSegmentOrgDataTemporary{
		IsDisabled: true,       // 停用切片
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, segment)
	if err != nil {
		logx.E(ctx, "DisabledDocSegmentTemporaryOrgData failed|err: %+v", err)
	}
	return nil
}

// EnableDocSegmentTemporaryOrgData 启用切片
func (d *daoImpl) EnableDocSegmentTemporaryOrgData(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string) error {
	updateColumns := []string{segEntity.DocSegmentOrgDataTemporaryTblColIsDisabled, segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &segEntity.DocSegmentOrgDataTemporary{
		IsDisabled: false,      // 启用切片
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, segment)
	if err != nil {
		logx.E(ctx, "EnableDocSegmentTemporaryOrgData failed|err: %+v", err)
	}
	return nil
}

// UpdateDocSegmentAuditStatus 更新切片审核状态
func (d *daoImpl) UpdateDocSegmentAuditStatus(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []string, auditStatus uint32) error {
	updateColumns := []string{segEntity.DocSegmentOrgDataTemporaryTblColAuditStatus, segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime}
	segment := &segEntity.DocSegmentOrgDataTemporary{
		AuditStatus: auditStatus,
		UpdateTime:  time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: businessIDs,
	}
	_, err := d.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, segment)
	if err != nil {
		logx.E(ctx, "UpdateDocSegmentAuditStatus failed|err: %+v", err)
	}
	return nil
}

// BatchUpdateDocTemporaryOrgData 批量更新
func (d *daoImpl) BatchUpdateDocTemporaryOrgData(ctx context.Context,
	updateColumns []string, filter *segEntity.DocSegmentOrgDataTemporaryFilter, orgData *segEntity.DocSegmentOrgDataTemporary,
	batchSize int) error {
	logx.D(ctx, "BatchUpdateDocOrgData|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = batchSize
		rowsAffected, err := d.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, orgData)
		if err != nil {
			logx.E(ctx, "BatchUpdateDocOrgData failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}

// BatchDeleteTemporaryDocOrgDataByDocBizID 批量删除干预中的OrgData数据(逻辑删除)
func (d *daoImpl) BatchDeleteTemporaryDocOrgDataByDocBizID(ctx context.Context, corpBizID, appBizID, docBizID uint64, batchSize int) error {
	logx.D(ctx, "BatchDeleteTemporaryDocOrgDataByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	updateColumns := []string{segEntity.DocSegmentOrgDataTemporaryTblColIsDeleted,
		segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgDataTemporary{
		IsDeleted:  true,
		UpdateTime: time.Now(),
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	return d.BatchUpdateDocTemporaryOrgData(ctx, updateColumns, filter, orgData, batchSize)
}

// BatchRecoverDocTemporaryOrgDataByDocBizID 批量恢复OrgData数据
func (d *daoImpl) BatchRecoverDocTemporaryOrgDataByDocBizID(ctx context.Context, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	logx.D(ctx, "BatchRecoverDocTemporaryOrgDataByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	updateColumns := []string{segEntity.DocSegmentOrgDataTemporaryTblColIsDeleted,
		segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgDataTemporary{
		IsDeleted:  false,
		UpdateTime: time.Now(),
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
		IsDeleted: ptrx.Bool(true),
	}
	return d.BatchUpdateDocTemporaryOrgData(ctx, updateColumns, filter, orgData, batchSize)
}

// RealityDeleteDocSegmentTemporary 物理删除
func (d *daoImpl) RealityDeleteDocSegmentTemporary(ctx context.Context,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) (int64, error) {
	tx := d.tdsql.TDocSegmentOrgDataTemporary
	docSegmentOrgDataTemporaryTableName := tx.TableName()
	if filter == nil {
		logx.E(ctx, "RealityDeleteDocSegmentTemporary|filter is null")
		return 0, errs.ErrSystem
	}
	session := tx.WithContext(ctx).UnderlyingDB().Table(docSegmentOrgDataTemporaryTableName)
	session = d.generateCondition(ctx, session, filter)
	if filter.Limit > 0 {
		session.Limit(int(filter.Limit))
	}
	res := session.Delete(&model.TDocSegmentOrgDataTemporary{})
	if res.Error != nil {
		logx.E(ctx, "RealityDeleteDocSegmentTemporary|err:%+v", res.Error)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// RealityDeleteDocSegmentTemporaryOrgDataByOrgDataBizID 物理删除某个切片
func (d *daoImpl) RealityDeleteDocSegmentTemporaryOrgDataByOrgDataBizID(ctx context.Context, corpBizID,
	appBizID, docBizID uint64, businessID string) error {
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:   corpBizID,
		AppBizID:    appBizID,
		DocBizID:    docBizID,
		BusinessIDs: []string{businessID},
		Limit:       1,
	}
	_, err := d.RealityDeleteDocSegmentTemporary(ctx, filter)
	if err != nil {
		logx.E(ctx, "RealityDeleteDocSegmentOrgDataByOrgDataBizID failed for BusinessID: %s, err: %+v", businessID, err)
		return err
	}
	return nil
}

// RealityBatchDeleteTemporaryDocOrgData 批量删除OrgData数据(物理删除)
func (d *daoImpl) RealityBatchDeleteTemporaryDocOrgData(ctx context.Context,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter, batchSize int) error {
	logx.D(ctx, "RealityBatchDeleteTemporaryDocOrgData|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = batchSize
		rowsAffected, err := d.RealityDeleteDocSegmentTemporary(ctx, filter)
		if err != nil {
			logx.E(ctx, "RealityBatchDeleteTemporaryDocOrgData failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}
