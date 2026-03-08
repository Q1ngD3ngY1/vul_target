package qa

import "time"

const (
	// DocQATaskIsDeleted 已删除
	DocQATaskIsDeleted = 1
	// DocQATaskIsNotDeleted 正常
	DocQATaskIsNotDeleted = 0

	DocQATaskStatusGenerating = 1 // DocQATaskStatusGenerating 生成中
	DocQATaskStatusSuccess    = 2 // DocQATaskStatusSuccess 成功
	DocQATaskStatusPause      = 3 // DocQATaskStatusPause 手动暂停
	DocQATaskStatusResource   = 4 // DocQATaskStatusResource 资源耗尽暂停
	DocQATaskStatusFail       = 5 // DocQATaskStatusFail 失败
	DocQATaskStatusCancel     = 6 // DocQATaskStatusCancel 手动取消

	DocQATaskStatusRetry    = 7 // DocQATaskStatusRetry 任务重试
	DocQATaskStatusContinue = 8 // DocQATaskStatusContinue 任务继续

	// DocQaExistsOrgDataPreFix 文档问答任务orgData前缀
	DocQaExistsOrgDataPreFix = "docQa:existsOrgData:"

	// DocQaTaskSourceTypeDocQa DocQATask任务的来源于doc to qa任务
	DocQaTaskSourceTypeOrigin = 0
	// DocQaTaskSourceTypeDocDiff DocQATask任务的来源于 doc diff 任务
	DocQaTaskSourceTypeDocDiff = 1
)

// DocQATask 文档生成问答任务表
type DocQATask struct {
	ID                uint64    `db:"id"  gorm:"column:id"`                                     // id
	BusinessID        uint64    `db:"business_id"  gorm:"column:business_id"`                   // 对外task_id
	RobotID           uint64    `db:"robot_id"  gorm:"column:robot_id"`                         // 机器人ID
	DocID             uint64    `db:"doc_id"  gorm:"column:doc_id"`                             // 文档ID
	CorpID            uint64    `db:"corp_id"  gorm:"column:corp_id"`                           // 企业ID
	TaskID            uint64    `db:"task_id"  gorm:"column:task_id"`                           // 对应定时执行任务表ID
	SourceID          uint64    `db:"source_id"  gorm:"column:source_id"`                       // 来源ID，目前只有DiffTask，本身QaTask时为0
	DocName           string    `db:"doc_name"  gorm:"column:doc_name"`                         // 文档名称
	DocType           string    `db:"doc_type"  gorm:"column:doc_type"`                         // 文档类型
	QACount           uint64    `db:"qa_count"  gorm:"column:qa_count"`                         // 生成的问答数
	SegmentCountDone  uint64    `db:"segment_count_done"  gorm:"column:segment_count_done"`     // 已完成问答的切片总数
	SegmentCount      uint64    `db:"segment_count" gorm:"column:segment_count"`                // 文档切片总数
	StopNextSegmentID uint64    `db:"stop_next_segment_id"  gorm:"column:stop_next_segment_id"` // 暂停后下次开始的segment_id
	InputToken        uint64    `db:"input_token"  gorm:"column:input_token"`                   // 输入token
	OutputToken       uint64    `db:"output_token"  gorm:"column:output_token"`                 // 输出token
	Status            int       `db:"status"  gorm:"column:status"`                             // 状态(1生成中 2成功 3手动暂停 4资源耗尽暂停 5失败 6手动取消)
	Message           string    `db:"message"  gorm:"column:message"`                           // 生成失败原因
	IsDeleted         bool      `db:"is_deleted"  gorm:"column:is_deleted"`                     // 是否删除(0未删除 1已删除）
	UpdateTime        time.Time `db:"update_time"  gorm:"column:update_time"`                   // 更新时间
	CreateTime        time.Time `db:"create_time"  gorm:"column:create_time"`                   // 创建时间
}

// DocQATaskIsCancel 判断文档生成问答任务是否可以取消
func (d *DocQATask) DocQATaskIsCancel() bool {
	if d == nil {
		return false
	}
	if d.Status != DocQATaskStatusPause && d.Status != DocQATaskStatusGenerating && d.Status != DocQATaskStatusResource {
		return false
	}
	return true
}

// DocQATaskIsStop 判断文档生成问答任务是否可以暂停
func (d *DocQATask) DocQATaskIsStop() bool {
	if d == nil {
		return false
	}
	if d.Status != DocQATaskStatusGenerating {
		return false
	}
	return true
}

// DocQATaskIsContinue 判断文档生成问答任务是否可以继续
func (d *DocQATask) DocQATaskIsContinue() bool {
	if d == nil {
		return false
	}
	if d.Status != DocQATaskStatusPause && d.Status != DocQATaskStatusResource {
		return false
	}
	return true
}

const (
	DocQaTaskTableName = "t_doc_qa_task"

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

	DocQaTaskTableMaxPageSize = 1000
)

var DocQaTaskTblColList = []string{DocQaTaskTblColId, DocQaTaskTblColBusinessId, DocQaTaskTblColRobotId,
	DocQaTaskTblColDocId, DocQaTaskTblColCorpId, DocQaTaskTblColTaskId, DocQaTaskTblColDocName,
	DocQaTaskTblColDocType, DocQaTaskTblColQaCount, DocQaTaskTblColSegmentCountDone,
	DocQaTaskTblColSegmentCount, DocQaTaskTblColStopNextSegmentId, DocQaTaskTblColInputToken,
	DocQaTaskTblColOutputToken, DocQaTaskTblColStatus, DocQaTaskTblColMessage,
	DocQaTaskTblColIsDeleted, DocQaTaskTblColUpdateTime, DocQaTaskTblColCreateTime}

type DocQaTaskFilter struct {
	ID             uint64
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

	PageNo   uint32
	PageSize uint32
}
