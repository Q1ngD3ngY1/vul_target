package qa

import (
	"time"

	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

const (
	// RejectedQuestionReleaseStatusInit 待发布
	RejectedQuestionReleaseStatusInit = uint32(1)
	// RejectedQuestionReleaseStatusIng 发布中
	RejectedQuestionReleaseStatusIng = uint32(2)
	// RejectedQuestionReleaseStatusSuccess 已发布
	RejectedQuestionReleaseStatusSuccess = uint32(3)
	// RejectedQuestionReleaseStatusFail 发布失败
	RejectedQuestionReleaseStatusFail = uint32(4)

	// TaskExportStatusInit 未启动
	TaskExportStatusInit = uint32(1)
	// TaskExportStatusExportIng 流程中
	TaskExportStatusExportIng = uint32(2)
	// TaskExportStatusEnd 任务完成
	TaskExportStatusEnd = uint32(3)
	// TaskExportStatusFail 任务失败
	TaskExportStatusFail = uint32(4)
	// TaskExportStatusCallBackSuccess 任务回调成功
	TaskExportStatusCallBackSuccess = uint32(5)

	// RejectedQuestionIsNotDeleted 未删除
	RejectedQuestionIsNotDeleted = uint32(1)
	// RejectedQuestionIsDeleted 已删除
	RejectedQuestionIsDeleted = uint32(2)

	// RejectedQuestionAdd 新建
	RejectedQuestionAdd = uint32(1)
	// RejectedQuestionUpdate 修改
	RejectedQuestionUpdate = uint32(2)
	// RejectedQuestionDelete 删除
	RejectedQuestionDelete = uint32(3)
	// RejectedQuestionPublish 发布
	RejectedQuestionPublish = uint32(4)

	// BusinessSourceUnsatisfiedReply 拒答来源于不满意回复
	BusinessSourceUnsatisfiedReply = uint32(1)
	// BusinessSourceManual 拒答来源于手动添加
	BusinessSourceManual = uint32(2)
	// RejectedQuestionDeleteTaskName 拒答任务名
	RejectedQuestionDeleteTaskName = "RejectedQuestionDeleteTask"

	// ExportRejectedQuestionNoticeContent 导出任务通知内容
	ExportRejectedQuestionNoticeContent = "拒答问题批量导出%s。"

	// ExportRejectedQuestionNoticeContentIng 导出任务通知内容进行中
	ExportRejectedQuestionNoticeContentIng = "拒答问题批量导出中。"
)

var rejectedQuestionMap = map[uint32]string{
	RejectedQuestionReleaseStatusInit:    "待发布",
	RejectedQuestionReleaseStatusIng:     "发布中",
	RejectedQuestionReleaseStatusSuccess: "已发布",
	RejectedQuestionReleaseStatusFail:    "发布失败",
}

const (
	// RejectedQuestionTableName 拒答问题表名
	RejectedQuestionTableName = "t_rejected_question"

	RejectedQuestionTblColId               = "id"
	RejectedQuestionTblColBusinessID       = "business_id"
	RejectedQuestionTblColCorpID           = "corp_id"
	RejectedQuestionTblColRobotID          = "robot_id"
	RejectedQuestionTblColCreateStaffID    = "create_staff_id"
	RejectedQuestionTblColBusinessSourceID = "business_source_id"
	RejectedQuestionTblColBusinessSource   = "business_source"
	RejectedQuestionTblColQuestion         = "question"
	RejectedQuestionTblColReleaseStatus    = "release_status"
	RejectedQuestionTblColIsDeleted        = "is_deleted"
	RejectedQuestionTblColAction           = "action"
	RejectedQuestionTblColUpdateTime       = "update_time"
	RejectedQuestionTblColCreateTime       = "create_time"
)

var RejectedQuestionTblColList = []string{
	RejectedQuestionTblColId,
	RejectedQuestionTblColBusinessID,
	RejectedQuestionTblColCorpID,
	RejectedQuestionTblColRobotID,
	RejectedQuestionTblColCreateStaffID,
	RejectedQuestionTblColBusinessSourceID,
	RejectedQuestionTblColBusinessSource,
	RejectedQuestionTblColQuestion,
	RejectedQuestionTblColReleaseStatus,
	RejectedQuestionTblColIsDeleted,
	RejectedQuestionTblColAction,
	RejectedQuestionTblColUpdateTime,
	RejectedQuestionTblColCreateTime,
}

// RejectedQuestion 拒答问题
type RejectedQuestion struct {
	ID               uint64
	BusinessID       uint64
	CorpID           uint64
	RobotID          uint64
	CreateStaffID    uint64
	BusinessSourceID uint64
	BusinessSource   uint32
	Question         string
	ReleaseStatus    uint32
	IsDeleted        uint32
	Action           uint32
	UpdateTime       time.Time
	CreateTime       time.Time
}

// IsAllowDeleted 是否允许删除拒答问题
func (d *RejectedQuestion) IsAllowDeleted() bool {
	return d.ReleaseStatus != RejectedQuestionReleaseStatusIng
}

// IsAllowEdit 是否允许编辑拒答问题
func (d *RejectedQuestion) IsAllowEdit() bool {
	return d.ReleaseStatus != RejectedQuestionReleaseStatusIng
}

// StatusDesc 拒答问题状态描述
func (d *RejectedQuestion) StatusDesc(isPublishPause bool) string {
	if isPublishPause && d.ReleaseStatus == RejectedQuestionReleaseStatusIng {
		return i18nkey.KeyReleasePause
	}
	return rejectedQuestionMap[d.ReleaseStatus]
}

// IsDelete 判断拒答是否被删除
func (d *RejectedQuestion) IsDelete() bool {
	return d.IsDeleted == RejectedQuestionIsDeleted
}

// ActionDesc 状态描述
func (d *RejectedQuestion) ActionDesc() string {
	return rejectedQuestionMap[uint32(d.Action)]
}

type RejectedQuestionFilter struct {
	ID             uint64
	IDs            []uint64
	IDMore         uint64
	CorpID         uint64
	RobotID        uint64
	StaffID        uint64
	BusinessID     uint64
	BusinessIDs    []uint64
	UpdateTimeLess time.Time
	UpdateTimeMore time.Time
	ReleaseStatus  uint32
	IsDeleted      uint32
	Page           uint32
	PageSize       uint32
	Query          string
	Actions        []uint32
	Limit          uint32
	Offset         uint32
	OrderColumn    []string
	OrderDirection []string
}

type RejectBizQuestion struct {
	BizID    uint64
	Question string
}
