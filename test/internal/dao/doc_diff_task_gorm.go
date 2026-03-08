// bot-knowledge-config-server
//
// @(#)doc_diff_dao.go  星期五, 一月 17, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"

	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalDocDiffTaskDao *DocDiffTaskDao

const (
	docDiffTaskTableName = "t_doc_diff_task"

	DocDiffTaskTblColBusinessId            = "business_id"              // 文档对比ID
	DocDiffTaskTblColCorpBizId             = "corp_biz_id"              // 企业ID
	DocDiffTaskTblColRobotBizId            = "robot_biz_id"             // 应用ID
	DocDiffTaskTblColStaffBizId            = "staff_biz_id"             // 员工ID
	DocDiffTaskTblColNewDocBizId           = "new_doc_biz_id"           // 新文档ID
	DocDiffTaskTblColOldDocBizId           = "old_doc_biz_id"           // 旧文档ID
	DocDiffTaskTblColTaskId                = "task_id"                  // 异步处理任务ID
	DocDiffTaskTblDocQATaskId              = "doc_qa_task_id"           // doc to qa任务ID
	DocDiffTaskTblColNewDocRename          = "new_doc_rename"           // 重命名操作新文件名
	DocDiffTaskTblColOldDocRename          = "old_doc_rename"           // 重命名操作旧文件名
	DocDiffTaskTblColComparisonReason      = "comparison_reason"        // 对比原因
	DocDiffTaskTblColDiffType              = "diff_type"                // 对比类型
	DocDiffTaskTblColDocOperation          = "doc_operation"            // 文档操作类型
	DocDiffTaskTblColDocOperationStatus    = "doc_operation_status"     // 文档操作结果
	DocDiffTaskTblColQaOperation           = "qa_operation"             // 问答操作类型
	DocDiffTaskTblColQaOperationStatus     = "qa_operation_status"      // 问答操作结果
	DocDiffTaskTblColQaOperationResult     = "qa_operation_result"      // 问答操作成功或失败的结果提示
	DocDiffTaskTblColStatus                = "status"                   // 状态
	DocDiffTaskTblColDiffDataProcessStatus = "diff_data_process_status" // 对比数据处理状态
	DocDiffTaskTblColIsDeleted             = "is_deleted"               // 是否删除
	DocDiffTaskTblColCreateTime            = "create_time"              // 创建时间
	DocDiffTaskTblColUpdateTime            = "update_time"              // 更新时间

	DocDiffTaskTableMaxPageSize = 1000
	HandleDocDiffSize           = 50
)

var DocDiffTaskTblColList = []string{
	DocDiffTaskTblColBusinessId,
	DocDiffTaskTblColCorpBizId,
	DocDiffTaskTblColRobotBizId,
	DocDiffTaskTblColStaffBizId,
	DocDiffTaskTblColNewDocBizId,
	DocDiffTaskTblColOldDocBizId,
	DocDiffTaskTblColTaskId,
	DocDiffTaskTblDocQATaskId,
	DocDiffTaskTblColNewDocRename,
	DocDiffTaskTblColOldDocRename,
	DocDiffTaskTblColComparisonReason,
	DocDiffTaskTblColDiffType,
	DocDiffTaskTblColDocOperation,
	DocDiffTaskTblColDocOperationStatus,
	DocDiffTaskTblColQaOperation,
	DocDiffTaskTblColQaOperationStatus,
	DocDiffTaskTblColQaOperationResult,
	DocDiffTaskTblColStatus,
	DocDiffTaskTblColDiffDataProcessStatus,
	DocDiffTaskTblColIsDeleted,
	DocDiffTaskTblColCreateTime,
	DocDiffTaskTblColUpdateTime,
}

type DocDiffTaskDao struct {
	BaseDao
}

// GetDocDiffTaskDao 获取全局的数据操作对象
func GetDocDiffTaskDao() *DocDiffTaskDao {
	if globalDocDiffTaskDao == nil {
		globalDocDiffTaskDao = &DocDiffTaskDao{*globalBaseDao}
	}
	return globalDocDiffTaskDao
}

type DocDiffTaskFilter struct {
	BusinessIds    []uint64 // 文档对比ID
	CorpBizId      uint64   // 企业 ID
	RobotBizId     uint64
	IsDeleted      *int
	Statuses       []int32
	InNewOldDocId  []uint64
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// 生成查询条件，必须按照索引的顺序排列
func generateDocDiffTaskCondition(ctx context.Context, session *gorm.DB, filter *DocDiffTaskFilter) {
	if filter.CorpBizId != 0 {
		session = session.Where(DocDiffTaskTblColCorpBizId+sqlEqual, filter.CorpBizId)
	}
	if filter.RobotBizId != 0 {
		session = session.Where(DocDiffTaskTblColRobotBizId+sqlEqual, filter.RobotBizId)
	}
	if len(filter.BusinessIds) > 0 {
		session = session.Where(DocDiffTaskTblColBusinessId+sqlIn, filter.BusinessIds)
	}

	if len(filter.Statuses) > 0 {
		session = session.Where(DocDiffTaskTblColStatus+sqlIn, filter.Statuses)
	}

	if len(filter.InNewOldDocId) > 0 {
		session = session.Where("("+DocDiffTaskTblColNewDocBizId+sqlIn, filter.InNewOldDocId).Or(
			DocDiffTaskTblColOldDocBizId+sqlIn+")", filter.InNewOldDocId)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocDiffTaskTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// CreateDocDiff 创建对比任务
func (d *DocDiffTaskDao) CreateDocDiff(ctx context.Context, docDiff model.DocDiff) error {
	res := d.tdsqlGormDB.WithContext(ctx).Table(docDiffTaskTableName).Create(&docDiff)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// GetDocDiffTask 获取单个文档对比任务
func (d *DocDiffTaskDao) GetDocDiffTask(ctx context.Context, selectColumns []string, corpBizId,
	robotBizId, diffId uint64) (*model.DocDiff, error) {
	docDiff := &model.DocDiff{}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docDiffTaskTableName).Select(selectColumns)
	session = session.Where(DocDiffTaskTblColCorpBizId+sqlEqual, corpBizId).Where(DocDiffTaskTblColRobotBizId+sqlEqual, robotBizId).
		Where(DocDiffTaskTblColBusinessId+sqlEqual, diffId).Where(DocDiffTaskTblColIsDeleted+sqlEqual, IsNotDeleted)
	res := session.Take(&docDiff)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			log.WarnContextf(ctx, "doc diff task not exist err: %+v", res.Error)
			return docDiff, errs.ErrHandleDocDiffNotFound
		}
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docDiff, res.Error
	}
	return docDiff, nil
}

// getDocDiffTaskList 获取文档对比任务
func (d *DocDiffTaskDao) getDocDiffTaskList(ctx context.Context, selectColumns []string, filter *DocDiffTaskFilter) (
	[]*model.DocDiff, error) {
	docDiffs := make([]*model.DocDiff, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docDiffs, nil
	}
	if filter.Limit > DocDiffTaskTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocDiffTaskList err: %+v", err)
		return docDiffs, err
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docDiffTaskTableName).Select(selectColumns)
	generateDocDiffTaskCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docDiffs)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docDiffs, res.Error
	}
	return docDiffs, nil
}

// GetDocDiffTaskCount 获取文档比对任务总数
func (d *DocDiffTaskDao) GetDocDiffTaskCount(ctx context.Context, selectColumns []string, filter *DocDiffTaskFilter) (int64, error) {
	session := d.tdsqlGormDB.WithContext(ctx).Table(docDiffTaskTableName).Select(selectColumns)
	generateDocDiffTaskCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocDiffTaskCountAndList 获取文档对比任务列表
func (d *DocDiffTaskDao) GetDocDiffTaskCountAndList(ctx context.Context, selectColumns []string, filter *DocDiffTaskFilter) (
	[]*model.DocDiff, int64, error) {
	count, err := d.GetDocDiffTaskCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocDiffTaskList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocDiffTaskList 获取所有文档对比任务
func (d *DocDiffTaskDao) GetDocDiffTaskList(ctx context.Context, selectColumns []string, filter *DocDiffTaskFilter) (
	[]*model.DocDiff, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	allDocDiffTasks := make([]*model.DocDiff, 0)
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docDiffTasks, err := d.getDocDiffTaskList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocDiffTaskList failed, err: %+v", err)
			return nil, err
		}
		allDocDiffTasks = append(allDocDiffTasks, docDiffTasks...)
		if uint32(len(docDiffTasks)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocDiffTaskList count:%d cost:%dms",
		len(allDocDiffTasks), time.Since(beginTime).Milliseconds())
	return allDocDiffTasks, nil
}

// UpdateDocDiffTasks 更新对比任务指定字段
func (d *DocDiffTaskDao) UpdateDocDiffTasks(ctx context.Context, tx *gorm.DB, updateColumns []string,
	corpBizId uint64, robotBizId uint64, businessIds []uint64, docDiff *model.DocDiff) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(docDiffTaskTableName).Select(updateColumns).
		Where(DocDiffTaskTblColCorpBizId+sqlEqual, corpBizId).
		Where(DocDiffTaskTblColRobotBizId+sqlEqual, robotBizId).
		Where(DocDiffTaskTblColBusinessId+sqlIn, businessIds)
	res := session.Updates(docDiff)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocDiffTasks failed for businessIds: %+v, err: %+v", businessIds, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocDiffTasks 删除对比任务
func (d *DocDiffTaskDao) DeleteDocDiffTasks(ctx context.Context, tx *gorm.DB, corpBizId uint64, robotBizId uint64,
	businessIds []uint64) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocDiffTaskTblColIsDeleted, DocDiffTaskTblColUpdateTime}
	docDiff := &model.DocDiff{
		IsDeleted:  IsDeleted,  // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	if err := d.UpdateDocDiffTasks(ctx, tx, updateColumns, corpBizId, robotBizId, businessIds, docDiff); err != nil {
		log.ErrorContextf(ctx, "DeleteDocDiffTasks failed for businessIds: %+v, err: %+v", businessIds, err)
		return err
	}
	return nil
}
