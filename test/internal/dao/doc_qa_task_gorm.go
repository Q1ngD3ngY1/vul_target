// bot-knowledge-config-server
//
// @(#)doc_qa_task_dao.go  星期四, 一月 16, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package dao

import (
	"context"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"gorm.io/gorm"
)

var globalDocQaTaskDao *DocQaTaskDao

const (
	docQaTaskTableName = "t_doc_qa_task"

	DocQaTaskTblColId                = "id"
	DocQaTaskTblColBusinessId        = "business_id"
	DocQaTaskTblColRobotId           = "robot_id"
	DocQaTaskTblColDocId             = "doc_id"
	DocQaTaskTblColCorpId            = "corp_id"
	DocQaTaskTblColTaskId            = "task_id"
	DocQaTaskTblColDocName           = "doc_name"
	DocQaTaskTblColDocType           = "doc_type"
	DocQaTaskTblColQaCount           = "qa_count"
	DocQaTaskTblColSegmentCountDone  = "segment_count_done"
	DocQaTaskTblColSegmentCount      = "segment_count"
	DocQaTaskTblColStopNextSegmentId = "stop_next_segment_id"
	DocQaTaskTblColInputToken        = "input_token"
	DocQaTaskTblColOutputToken       = "output_token"
	DocQaTaskTblColStatus            = "status"
	DocQaTaskTblColMessage           = "message"
	DocQaTaskTblColIsDeleted         = "is_deleted"
	DocQaTaskTblColUpdateTime        = "update_time"
	DocQaTaskTblColCreateTime        = "create_time"

	docQaTaskTableMaxPageSize = 1000
)

var DocQaTaskTblColList = []string{DocQaTaskTblColId, DocQaTaskTblColBusinessId, DocQaTaskTblColRobotId,
	DocQaTaskTblColDocId, DocQaTaskTblColCorpId, DocQaTaskTblColTaskId, DocQaTaskTblColDocName,
	DocQaTaskTblColDocType, DocQaTaskTblColQaCount, DocQaTaskTblColSegmentCountDone,
	DocQaTaskTblColSegmentCount, DocQaTaskTblColStopNextSegmentId, DocQaTaskTblColInputToken,
	DocQaTaskTblColOutputToken, DocQaTaskTblColStatus, DocQaTaskTblColMessage,
	DocQaTaskTblColIsDeleted, DocQaTaskTblColUpdateTime, DocQaTaskTblColCreateTime}

type DocQaTaskDao struct {
	BaseDao
}

// GetDocQaTaskDao 获取全局的数据操作对象
func GetDocQaTaskDao() *DocQaTaskDao {
	if globalDocQaTaskDao == nil {
		globalDocQaTaskDao = &DocQaTaskDao{*globalBaseDao}
	}
	return globalDocQaTaskDao
}

type DocQaTaskFilter struct {
	BusinessId     uint64 // doc qa task business id
	CorpId         uint64 // 企业 ID
	RobotId        uint64
	IsDeleted      *int
	Status         []int
	DocId          []uint64
	BusinessIds    []uint64
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// 生成查询条件，必须按照索引的顺序排列
func generateDocQaTaskCondition(ctx context.Context, session *gorm.DB, filter *DocQaTaskFilter) {
	if filter.CorpId != 0 {
		session = session.Where(DocQaTaskTblColCorpId+sqlEqual, filter.CorpId)
	}
	if filter.RobotId != 0 {
		session = session.Where(DocQaTaskTblColRobotId+sqlEqual, filter.RobotId)
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocQaTaskTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}

	if len(filter.DocId) != 0 {
		session = session.Where(DocQaTaskTblColDocId+sqlIn, filter.DocId)
	}
	if len(filter.Status) != 0 {
		session = session.Where(DocQaTaskTblColStatus+sqlIn, filter.Status)
	}

	if filter.BusinessId != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(DocQaTaskTblColBusinessId+sqlEqual, filter.BusinessId)
	}
	if len(filter.BusinessIds) != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(DocQaTaskTblColBusinessId+sqlIn, filter.BusinessIds)
	}
}

// GetDocQaTasks 获取文档生成问答任务集合
func (d *DocQaTaskDao) GetDocQaTasks(ctx context.Context, selectColumns []string, filter *DocQaTaskFilter) (
	[]*model.DocQATask, error) {
	docQATasks := make([]*model.DocQATask, 0)
	session := d.gormDB.WithContext(ctx).Table(docQaTaskTableName).Select(selectColumns)
	generateDocQaTaskCondition(ctx, session, filter)
	if filter.Limit == 0 || filter.Limit > docQaTaskTableMaxPageSize {
		filter.Limit = docQaTaskTableMaxPageSize
	}
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docQATasks)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docQATasks, res.Error
	}
	return docQATasks, nil
}

// GetDocQATaskGeneratingMaps 查询文档生成问答进行中的任务集合
func (d *DocQaTaskDao) GetDocQATaskGeneratingMaps(ctx context.Context, corpID, robotID uint64, docID []uint64) (
	map[uint64]*model.DocQATask, error) {
	if len(docID) == 0 || corpID == 0 || robotID == 0 {
		return nil, errs.ErrDocQaTaskNotFound
	}
	generatingStatus := []int{model.DocQATaskStatusGenerating, model.DocQATaskStatusPause,
		model.DocQATaskStatusResource, model.DocQATaskStatusFail}

	notDeleted := model.DocIsNotDeleted
	filter := &DocQaTaskFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		IsDeleted: &notDeleted,
		Status:    generatingStatus,
		DocId:     docID,
	}
	docQATaskMap := make(map[uint64]*model.DocQATask)
	docQATasks, err := d.GetDocQaTasks(ctx, DocQaTaskTblColList, filter)
	if err != nil {
		log.ErrorContextf(ctx, "获取文档生成问答进行中的任务集合失败 GetDocQaTasks err: %+v", err)
		return docQATaskMap, err
	}
	if len(docQATasks) == 0 {
		return docQATaskMap, nil
	}
	for _, dqt := range docQATasks {
		docQATaskMap[dqt.DocID] = dqt
	}
	return docQATaskMap, nil
}

// UpdateDocQATasks 更新doc qa tasks
func (d *DocQaTaskDao) UpdateDocQATasks(ctx context.Context, updateColumns []string, filter *DocQaTaskFilter, docQaTask *model.DocQATask) error {
	session := d.gormDB.WithContext(ctx).Table(docQaTaskTableName).Select(updateColumns)
	generateDocQaTaskCondition(ctx, session, filter)
	res := session.Updates(docQaTask)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocQATasks failed, col: %v, param: %+v, qa: %+v, err: %+v",
			updateColumns, filter, docQaTask, res.Error)
		return res.Error
	}
	return nil
}
