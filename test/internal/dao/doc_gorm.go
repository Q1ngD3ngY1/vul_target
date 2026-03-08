// bot-knowledge-config-server
//
// @(#)doc_dao.go  星期四, 一月 16, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"gorm.io/gorm"
)

var globalDocDao *DocDao

const (
	docTableName = "t_doc"

	DocTblColId                  = "id"
	DocTblColBusinessId          = "business_id"
	DocTblColRobotId             = "robot_id"
	DocTblColCorpId              = "corp_id"
	DocTblColStaffId             = "staff_id"
	DocTblColCreateUserId        = "create_user_id"
	DocTblColFileName            = "file_name"
	DocTblColFileNameInAudit     = "file_name_in_audit"
	DocTblColFileType            = "file_type"
	DocTblColFileSize            = "file_size"
	DocTblColBucket              = "bucket"
	DocTblColCosURL              = "cos_url"
	DocTblColCosHash             = "cos_hash"
	DocTblColMessage             = "message"
	DocTblColStatus              = "status"
	DocTblColIsRefer             = "is_refer"
	DocTblColReferURLType        = "refer_url_type"
	DocTblColIsDeleted           = "is_deleted"
	DocTblColSource              = "source"
	DocTblColWebURL              = "web_url"
	DocTblColBatchId             = "batch_id"
	DocTblColAuditFlag           = "audit_flag"
	DocTblColIsCreatingQa        = "is_creating_qa"
	DocTblColIsCreatedQa         = "is_created_qa"
	DocTblColNextAction          = "next_action"
	DocTblColAttrRange           = "attr_range"
	DocTblColIsCreatingIndex     = "is_creating_index"
	DocTblColCharSize            = "char_size"
	DocTblColCreateTime          = "create_time"
	DocTblColUpdateTime          = "update_time"
	DocTblColExpireStart         = "expire_start"
	DocTblColExpireEnd           = "expire_end"
	DocTblColOpt                 = "opt"
	DocTblColCategoryId          = "category_id"
	DocTblColOriginalURL         = "original_url"
	DocTblColProcessingFlag      = "processing_flag"
	DocTblColCustomerKnowledgeId = "customer_knowledge_id"
	DocTblColAttributeFlag       = "attribute_flag"
	DocTblColIsDownloadable      = "is_downloadable"
	DocTblColUpdatePeriodH       = "update_period_h"
	DocTblColNextUpdateTime      = "next_update_time"
	DocTblColSplitRule           = "split_rule"

	docTableMaxPageSize = 1000

	// docFilterFlagIsCreatedQa 文档筛选是否生成过问答标记位
	docFilterFlagIsCreatedQa = "is_created_qa"
)

// docFilterFlags 支持的文档筛选标识位
var docFilterFlags = map[string]struct{}{
	// 后续支持新类型添加此处
	docFilterFlagIsCreatedQa: {},
}
var DocTblColList = []string{DocTblColId, DocTblColBusinessId, DocTblColRobotId, DocTblColCorpId,
	DocTblColStaffId, DocTblColCreateUserId, DocTblColFileName, DocTblColFileNameInAudit, DocTblColFileType,
	DocTblColFileSize, DocTblColBucket, DocTblColCosURL, DocTblColCosHash, DocTblColMessage,
	DocTblColStatus, DocTblColIsRefer, DocTblColReferURLType, DocTblColIsDeleted, DocTblColSource,
	DocTblColWebURL, DocTblColBatchId, DocTblColAuditFlag, DocTblColIsCreatingQa, DocTblColIsCreatedQa,
	DocTblColNextAction, DocTblColAttrRange, DocTblColIsCreatingIndex, DocTblColCharSize, DocTblColCreateTime,
	DocTblColUpdateTime, DocTblColExpireStart, DocTblColExpireEnd, DocTblColOpt, DocTblColCategoryId,
	DocTblColOriginalURL, DocTblColProcessingFlag, DocTblColCustomerKnowledgeId, DocTblColAttributeFlag,
	DocTblColIsDownloadable, DocTblColUpdatePeriodH, DocTblColNextUpdateTime, DocTblColSplitRule}

type DocDao struct {
	BaseDao
	tableName string
}

// GetDocDao 获取全局的数据操作对象
func GetDocDao() *DocDao {
	if globalDocDao == nil {
		globalDocDao = &DocDao{*globalBaseDao, docTableName}
	}
	return globalDocDao
}

// IsValidDocFilterFlag 是否支持的文档筛选标识位
func IsValidDocFilterFlag(flag string) bool {
	if _, ok := docFilterFlags[flag]; !ok {
		return false
	}
	return true
}

// DocFilter 文档筛选
type DocFilter struct {
	RouterAppBizID                  uint64 // 用来路由数据库实例的应用业务ID，比如isearch独立数据库实例
	IDs                             []uint64
	NotInIDs                        []uint64
	OrIDs                           []uint64
	CorpId                          uint64 // 企业 ID
	RobotId                         uint64
	RobotIDs                        []uint64
	BusinessIds                     []uint64
	IsDeleted                       *int
	NotInBusinessIds                []uint64
	Status                          []uint32
	ValidityStatus                  uint32
	FileNameOrAuditName             string // 查询文件名或者重命名名称
	FileNameSubStrOrAuditNameSubStr string // 查询文件名或者重命名名称的子串
	FileTypes                       []string
	Opts                            []uint32
	CategoryIds                     []uint64
	OriginalURL                     string
	FilterFlag                      map[string]bool // 文档筛选标识位

	CosHash         string
	IsCreatingIndex *uint32
	MinUpdateTime   time.Time
	MaxUpdateTime   time.Time
	NextActions     []uint32
	NextUpdateTime  time.Time
	Source          *uint32

	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

var fileNameReplacer = strings.NewReplacer(`\`, `\\`, `_`, `\_`, `%`, `\%`, `'`, `\'`)

func (d *DocDao) generateStatusCondition(ctx context.Context, session *gorm.DB, status []uint32, validityStatus uint32) {
	zeroTimeStr := time.Unix(0, 0).Format("2006-01-02 15:04:05.000")
	nowTimeStr := time.Now().Format("2006-01-02 15:04:05.000")

	expiredStatusCondition := d.gormDB.Where(DocTblColExpireEnd+sqlMore, zeroTimeStr).
		Where(DocTblColExpireEnd+sqlLess, nowTimeStr)
	validStatusCondition := d.gormDB.Where(DocTblColExpireEnd+sqlEqual, zeroTimeStr).
		Or(DocTblColExpireEnd+sqlMoreEqual, nowTimeStr)
	// 勾选其他状态，未勾选已过期
	if len(status) != 0 && validityStatus != model.DocExpiredStatus {
		session.Where(validStatusCondition).Where(DocTblColStatus+sqlIn, status)
		return
	}

	// 只勾选已过期
	if len(status) == 0 && validityStatus == model.DocExpiredStatus {
		session = session.Where(expiredStatusCondition)
		return
	}
	// 勾选其他状态+已过期
	if len(status) != 0 && validityStatus == model.DocExpiredStatus {
		session = session.Where(
			d.gormDB.Where(expiredStatusCondition).
				Or(d.gormDB.Where(validStatusCondition).Where(DocTblColStatus+sqlIn, status)))
		return
	}

}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocFilter) {
	if filter.CorpId != 0 {
		session.Where(DocTblColCorpId+sqlEqual, filter.CorpId)
	}
	if filter.RobotId != 0 {
		session.Where(DocTblColRobotId+sqlEqual, filter.RobotId)
	}
	if len(filter.RobotIDs) != 0 {
		session.Where(DocTblColRobotId+sqlIn, filter.RobotIDs)
	}

	if len(filter.OrIDs) != 0 {
		session.Or(DocTblColId+sqlIn, filter.OrIDs)
	}

	if filter.Source != nil {
		session.Where(DocTblColSource+sqlEqual, *filter.Source)
	}
	if !filter.NextUpdateTime.IsZero() {
		session.Where(DocTblColNextUpdateTime+sqlEqual, filter.NextUpdateTime)
	}
	// is_deleted不能设置默认查询条件，有些场景需要查询已删除的文档，比如文档比对任务
	if filter.IsDeleted != nil {
		session.Where(DocTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}

	for flag, val := range filter.FilterFlag {
		// 兜底保护, 查询指定标识字段
		switch flag {
		case docFilterFlagIsCreatedQa:
			session.Where(DocTblColIsCreatedQa+sqlEqual, val)
		}
	}

	if len(filter.Status) != 0 || filter.ValidityStatus != 0 {
		// 状态相关的过滤条件
		d.generateStatusCondition(ctx, session, filter.Status, filter.ValidityStatus)
	}
	if filter.FileNameOrAuditName != "" {
		// 文件名或者审核中的文件名相同
		newStr := fileNameReplacer.Replace(filter.FileNameOrAuditName)
		session.Where(d.gormDB.Where(DocTblColFileNameInAudit+sqlEqual, newStr).
			Or(d.gormDB.Where(DocTblColFileNameInAudit+sqlEqual, "").Where(DocTblColFileName+sqlEqual, newStr)))
	}
	if filter.FileNameSubStrOrAuditNameSubStr != "" {
		// 文件名或者审核中的文件名包含该字符串子串
		newStr := fmt.Sprintf("%%%s%%", fileNameReplacer.Replace(filter.FileNameSubStrOrAuditNameSubStr))
		session.Where(d.gormDB.Where(DocTblColFileNameInAudit+sqlLike, newStr).
			Or(d.gormDB.Where(DocTblColFileNameInAudit+sqlEqual, "").Where(DocTblColFileName+sqlLike, newStr)))
	}
	if filter.OriginalURL != "" {
		session.Where(DocTblColOriginalURL+sqlEqual, filter.OriginalURL)
	}
	if len(filter.FileTypes) != 0 {
		session.Where(DocTblColFileType+sqlIn, filter.FileTypes)
	}
	if len(filter.Opts) != 0 {
		session.Where(DocTblColOpt+sqlIn, filter.Opts)
	}
	if len(filter.CategoryIds) != 0 {
		session.Where(DocTblColCategoryId+sqlIn, filter.CategoryIds)
	}
	if len(filter.BusinessIds) != 0 {
		session.Where(DocTblColBusinessId+sqlIn, filter.BusinessIds)
	}
	if len(filter.IDs) != 0 {
		session.Where(DocTblColId+sqlIn, filter.IDs)
	}
	if len(filter.NotInBusinessIds) != 0 {
		// 业务Id为该表唯一索引
		session.Where(DocTblColBusinessId+sqlSubNotIn, filter.NotInBusinessIds)
	}
	if len(filter.NotInIDs) != 0 {
		// 业务Id为该表唯一索引
		session.Where(DocTblColId+sqlSubNotIn, filter.NotInIDs)
	}
	if !filter.MinUpdateTime.IsZero() {
		session.Where(DocTblColUpdateTime+sqlMore, filter.MinUpdateTime)
	}
	if !filter.MaxUpdateTime.IsZero() {
		session.Where(DocTblColUpdateTime+sqlLess, filter.MaxUpdateTime)
	}
	if len(filter.NextActions) != 0 {
		session.Where(DocTblColNextAction+sqlIn, filter.NextActions)
	}
}

func (d *DocDao) getDocList(ctx context.Context, selectColumns []string, filter *DocFilter) ([]*model.Doc, error) {
	docs := make([]*model.Doc, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docs, nil
	}
	if filter.Limit > docTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "GetDocList err: %+v", err)
		return docs, err
	}
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return docs, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docs)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docs, res.Error
	}
	return docs, nil
}

// GetDocList 获取文档列表
func (d *DocDao) GetDocList(ctx context.Context, selectColumns []string,
	filter *DocFilter) ([]*model.Doc, error) {
	log.DebugContextf(ctx, "GetDocList filter:%+v", filter)
	allDocList := make([]*model.Doc, 0)
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docList, err := d.getDocList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocList failed, err: %+v", err)
			return nil, err
		}
		allDocList = append(allDocList, docList...)
		if uint32(len(docList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocList count:%d cost:%dms",
		len(allDocList), time.Since(beginTime).Milliseconds())
	return allDocList, nil
}

// GetDocCount 获取文档总数
func (d *DocDao) GetDocCount(ctx context.Context, selectColumns []string,
	filter *DocFilter) (int64, error) {
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocCountAndList 获取文档总数和分页列表
func (d *DocDao) GetDocCountAndList(ctx context.Context, selectColumns []string, filter *DocFilter) ([]*model.Doc,
	int64, error) {
	count, err := d.GetDocCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetTxDocAutoRefreshList 获取需要自动刷新腾讯文档
func (d *DocDao) GetTxDocAutoRefreshList(ctx context.Context, nextUpdateTime time.Time) ([]*model.Doc, error) {
	source := model.SourceFromTxDoc
	isDeleted := IsNotDeleted
	docFilter := &DocFilter{
		Source:         &source,
		NextUpdateTime: nextUpdateTime,
		IsDeleted:      &isDeleted,
		Opts:           []uint32{model.DocOptDocImport},
	}
	selectColumns := DocTblColList
	docs, err := d.GetDocList(ctx, selectColumns, docFilter)
	if err != nil {
		return nil, err
	}
	return docs, nil
}

// GetDiffDocs 获取需要diff的Doc
func (d *DocDao) GetDiffDocs(ctx context.Context, filter *DocFilter) ([]*model.Doc,
	error) {
	beginTime := time.Now()
	offset := 0
	limit := docTableMaxPageSize
	allDocs := make([]*model.Doc, 0)
	for {
		filter.Offset = uint32(offset)
		filter.Limit = uint32(limit)

		docs, err := d.GetDocList(ctx, []string{DocTblColId, DocTblColBusinessId, DocTblColRobotId, DocTblColStatus,
			DocTblColFileName, DocTblColFileType, DocTblColFileNameInAudit}, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAllDocQas failed, err: %+v", err)
			return nil, err
		}
		allDocs = append(allDocs, docs...)
		if len(docs) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}
	log.DebugContextf(ctx, "GetDocDiffURL count:%d cost:%dms",
		len(allDocs), time.Since(beginTime).Milliseconds())
	return allDocs, nil
}

// UpdateDoc 更新文档
func (d *DocDao) UpdateDoc(ctx context.Context, updateColumns []string, filter *DocFilter,
	doc *model.Doc) (int64, error) {
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(updateColumns)
	d.generateCondition(ctx, session, filter)
	res := session.Updates(doc)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDoc failed doc:%v, err: %+v", doc, res.Error)
		return 0, res.Error
	}
	log.DebugContextf(ctx, "update doc record: %v", res.RowsAffected)
	return res.RowsAffected, nil
}

// GetDocDiff 获取相同的需要发起diff任务的doc
func (d *DocDao) GetDocDiff(ctx context.Context, doc *model.Doc) ([]*model.Doc,
	error) {
	if doc == nil {
		return nil, nil
	}
	// diff的文档需要是 待发布、发布成功的状态
	status := []uint32{model.DocStatusWaitRelease, model.DocStatusReleaseSuccess}
	diffDocs := make([]*model.Doc, 0)
	var err error
	if doc.Source == model.SourceFromWeb {
		if strings.TrimSpace(doc.OriginalURL) == "" {
			return diffDocs, nil
		}
		notDeleted := model.DocIsNotDeleted
		filter := &DocFilter{
			CorpId:           doc.CorpID,
			RobotId:          doc.RobotID,
			IsDeleted:        &notDeleted,
			Status:           status,
			OriginalURL:      doc.OriginalURL,
			FileTypes:        []string{doc.FileType},
			NotInBusinessIds: []uint64{doc.BusinessID}, // 查询的时候过滤掉当前文档,因为已经入库
		}
		diffDocs, err = d.GetDiffDocs(ctx, filter)
		if err != nil {
			log.ErrorContextf(ctx, "获取网页diffUrl失败 GetDocDiffURL err: %+v", err)
			return diffDocs, err
		}
	}

	if doc.Source == model.SourceFromFile {
		if strings.TrimSpace(doc.FileName) == "" {
			return diffDocs, nil
		}
		notDeleted := model.DocIsNotDeleted
		filter := &DocFilter{
			CorpId:              doc.CorpID,
			RobotId:             doc.RobotID,
			IsDeleted:           &notDeleted,
			Status:              status,
			FileNameOrAuditName: doc.FileName,
			FileTypes:           []string{doc.FileType},
			NotInBusinessIds:    []uint64{doc.BusinessID}, // 查询的时候过滤掉当前文档,因为已经入库
		}
		diffDocs, err = d.GetDiffDocs(ctx, filter)
		if err != nil {
			log.ErrorContextf(ctx, "获取导入文件diff失败 GetDocDiffURL err: %+v", err)
			return diffDocs, err
		}
	}

	return diffDocs, nil

}

// GetDocDiffTaskDocs 获取对比任务的文档列表
func (d *DocDao) GetDocDiffTaskDocs(ctx context.Context, filter *DocFilter) (map[uint64]*model.Doc,
	error) {
	beginTime := time.Now()
	offset := 0
	limit := docTableMaxPageSize
	allDocs := make([]*model.Doc, 0)
	for {
		filter.Offset = uint32(offset)
		filter.Limit = uint32(limit)

		docs, err := d.GetDocList(ctx, DocTblColList, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAllDocQas failed, err: %+v", err)
			return nil, err
		}
		allDocs = append(allDocs, docs...)
		if len(docs) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}

	if len(allDocs) == 0 {
		return nil, nil
	}

	docMap := make(map[uint64]*model.Doc)
	for _, doc := range allDocs {
		docMap[doc.BusinessID] = doc
	}

	log.DebugContextf(ctx, "GetDocDiffTaskDocs count:%d cost:%dms",
		len(allDocs), time.Since(beginTime).Milliseconds())
	return docMap, nil
}

// GetDocByID 获取文档详情
func (d *DocDao) GetDocByID(ctx context.Context, corpID, robotID, businessID, id uint64,
	selectColumns []string) (*model.Doc, error) {
	if corpID == 0 || robotID == 0 {
		return nil, errs.ErrParams
	}
	if len(selectColumns) == 0 {
		return nil, errs.ErrParams
	}
	if businessID == 0 && id == 0 {
		return nil, errs.ErrParams
	}
	filter := &DocFilter{
		CorpId:  corpID,
		RobotId: robotID,
	}
	if businessID > 0 {
		filter.BusinessIds = []uint64{businessID}
	} else {
		filter.IDs = []uint64{id}
	}
	log.DebugContextf(ctx, "GetDocByID filter:%+v", filter)
	beginTime := time.Now()
	doc := &model.Doc{}
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	if err := session.First(doc).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.WarnContextf(ctx, "GetDocByID 未找到匹配的文档记录,corpId,robotId,businessId,id分别为:%d,%d,%d,%d",
				corpID, robotID, businessID, id)
			return nil, err
		}
		log.ErrorContextf(ctx, "GetDocByID failed, err: %+v", err)
		return nil, err
	}
	log.DebugContextf(ctx, "GetDocByID  cost:%dms",
		time.Since(beginTime).Milliseconds())
	return doc, nil
}

// GetDocReleaseCount 获取文档未发布状态总数
func (d *DocDao) GetDocReleaseCount(ctx context.Context, corpID, robotID uint64) (int64, error) {
	isDeleted := IsNotDeleted
	filter := &DocFilter{
		CorpId:  corpID,
		RobotId: robotID,
		Status: []uint32{model.DocStatusWaitRelease, model.DocStatusCreatingIndex, model.DocStatusParseIng,
			model.DocStatusAuditIng, model.DocStatusUnderAppeal},
		Opts:      []uint32{model.DocOptDocImport},
		IsDeleted: &isDeleted,
	}
	count, err := d.GetDocCount(ctx, []string{DocTblColId}, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// GetAllDocs 获取所有文档
func (d *DocDao) GetAllDocs(ctx context.Context, selectColumns []string, filter *DocFilter) ([]*model.Doc, error) {
	beginTime := time.Now()
	offset := 0
	limit := docTableMaxPageSize
	allDocs := make([]*model.Doc, 0)
	for {
		filter.Offset = uint32(offset)
		filter.Limit = uint32(limit)

		docs, err := d.getDocList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAllDocs failed, err: %+v", err)
			return nil, err
		}
		allDocs = append(allDocs, docs...)
		if len(docs) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}
	log.DebugContextf(ctx, "GetAllDocs count:%d cost:%dms",
		len(allDocs), time.Since(beginTime).Milliseconds())
	return allDocs, nil
}

// UpdateDocWaitRelease 更新文档状态到待发布
func (d *DocDao) UpdateDocWaitRelease(ctx context.Context, appID, docID uint64, segIDs []uint64) error {
	db, err := knowClient.GormClient(ctx, d.tableName, appID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateDocWaitRelease get GormClient failed, err: %+v", err)
		return err
	}
	//开启事务
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	//1.更新文档的状态
	err = tx.WithContext(ctx).Table(d.tableName).Where(DocTblColRobotId+sqlEqual, appID).
		Where(DocTblColId+sqlEqual, docID).Limit(1).Updates(map[string]any{
		DocTblColStatus:     model.DocStatusWaitRelease,
		DocTblColNextAction: model.DocNextActionUpdate,
		DocTblColUpdateTime: time.Now(),
	}).Error
	if err != nil {
		log.ErrorContextf(ctx, "UpdateDocWaitRelease failed doc:%v, err: %+v", docID, err)
		return err
	}
	//2.更新文档切片的状态
	if len(segIDs) > 0 {
		limit := utilConfig.GetMainConfig().Permissions.ChunkNumber
		if limit == 0 {
			limit = 3000
		}
		for _, segChunks := range slicex.Chunk(segIDs, limit) {
			err = tx.WithContext(ctx).Table(docSegmentTableName).Where(DocSegmentTblColRobotID+sqlEqual, appID).
				Where(DocSegmentTblColID+sqlIn, segChunks).Limit(len(segChunks)).Updates(map[string]any{
				DocSegmentTblColReleaseStatus: model.SegmentReleaseStatusInit,
				DocSegmentTblColNextAction:    model.NextActionUpdate,
				DocSegmentTblColUpdateTime:    time.Now(),
			}).Error
			if err != nil {
				log.ErrorContextf(ctx, "UpdateDocWaitRelease failed segIDS:%v,docID:%v,err: %+v", segIDs, docID, err)
				return err
			}
		}
	}
	return nil
}
