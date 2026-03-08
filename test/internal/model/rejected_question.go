package model

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"
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
	ExportRejectedQuestionNoticeContent = i18nkey.KeyRefusedQuestionsBatchExportStatus

	// ExportRejectedQuestionNoticeContentIng 导出任务通知内容进行中
	ExportRejectedQuestionNoticeContentIng = i18nkey.KeyRefusedQuestionsBatchExporting
)

var rejectedQuestionMap = map[uint32]string{
	RejectedQuestionReleaseStatusInit:    i18nkey.KeyWaitRelease,
	RejectedQuestionReleaseStatusIng:     i18nkey.KeyReleasing,
	RejectedQuestionReleaseStatusSuccess: i18nkey.KeyReleaseSuccess,
	RejectedQuestionReleaseStatusFail:    i18nkey.KeyPublishingFailed,
}

// RejectedQuestion 拒答问题
type RejectedQuestion struct {
	ID               uint64    `db:"id"`                 // 拒答问题表自增ID
	BusinessID       uint64    `db:"business_id"`        // 拒答问题表自增业务ID
	CorpID           uint64    `db:"corp_id"`            // 企业ID
	RobotID          uint64    `db:"robot_id"`           // 企业ID
	CreateStaffID    uint64    `db:"create_staff_id"`    // 新增拒答问题用户ID
	BusinessSourceID uint64    `db:"business_source_id"` // 不满意 reply ID，手动录入时默认为0
	BusinessSource   uint32    `db:"business_source"`    // 来源(1 从不满意问题录入 2 手动录入)
	Question         string    `db:"question"`           // 问题
	ReleaseStatus    uint32    `db:"release_status"`     // 发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)
	IsDeleted        uint32    `db:"is_deleted"`         // 0未删除 1已删除
	Action           uint32    `db:"action"`             // 操作行为：1新增2修改3删除 4发布
	UpdateTime       time.Time `db:"update_time"`        // 更新时间
	CreateTime       time.Time `db:"create_time"`        // 创建时间
}

// GetRejectedQuestionListReq 拉取拒答问题列表请求结构体
type GetRejectedQuestionListReq struct {
	CorpID   uint64
	StaffID  uint64
	RobotID  uint64
	Page     uint32
	PageSize uint32
	Query    string
	Actions  []uint32
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
	return releaseRejectedQuestionMap[d.Action]
}
