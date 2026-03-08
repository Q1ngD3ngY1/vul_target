package model

import (
	"time"

	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	// ExportUnsatisfiedReplyTaskType 导出不满意回复任务类型
	ExportUnsatisfiedReplyTaskType = 3
	// ExportUnsatisfiedReplyTaskName 导出不满意回复任务名称
	ExportUnsatisfiedReplyTaskName = "ExportUnsatisfiedReplyTask"

	// ExportQaTaskType 导出QA任务类型
	ExportQaTaskType = uint32(5)
	// ExportQaTaskName 导出QA任务名
	ExportQaTaskName = "ExportQaTask"

	// ExportQaTaskTypeV1 导出QA任务类型
	ExportQaTaskTypeV1 = uint32(7)
	// ExportQaTaskNameV1 导出QA任务名
	ExportQaTaskNameV1 = "ExportQaTaskV1"

	// ExportRejectedQuestionTaskType 导出任务类型
	ExportRejectedQuestionTaskType = uint32(2)
	// ExportRejectedQuestionTaskName 导出拒答任务名
	ExportRejectedQuestionTaskName = "ExportRejectedQuestionTask"

	// ExportAttributeLabelTaskType 导出属性标签任务类型
	ExportAttributeLabelTaskType = uint32(6)
	// ExportAttributeLabelTaskName 导出属性标签任务名
	ExportAttributeLabelTaskName = "ExportAttributeLabelTask"

	// ExportSynonymsTaskType 导出同义词任务类型
	ExportSynonymsTaskType = uint32(8)
	// ExportSynonymsTaskName 导出同义词任务名
	ExportSynonymsTaskName = "ExportSynonymsTask"
)

// Export 导出记录
type Export struct {
	ID            uint64    `db:"id"`              // 导出任务 ID
	CorpID        uint64    `db:"corp_id"`         // 企业 ID
	RobotID       uint64    `db:"robot_id"`        // 机器人 ID
	CreateStaffID uint64    `db:"create_staff_id"` // 创建任务员工 ID
	TaskType      uint32    `db:"task_type"`       // 任务类型 Type
	Name          string    `db:"name"`            // 任务名
	Params        string    `db:"params"`          // 导出任务参数
	Status        uint32    `db:"status"`          // 导出任务状态
	Result        string    `db:"result"`          // 导出任务结果
	Bucket        string    `db:"bucket"`          // 导出任务上传的 Cos 桶
	CosURL        string    `db:"cos_url"`         // cos url 地址
	UpdateTime    time.Time `db:"update_time"`     // 更新任务时间
	CreateTime    time.Time `db:"create_time"`     // 创建任务时间
}

// GetStatusString 获取任务状态
func (export *Export) GetStatusString() string {
	switch export.Status {
	case AttributeLabelTaskStatusSuccess:
		return pb.TaskStatus_SUCCESS.String()
	case AttributeLabelTaskStatusPending:
		return pb.TaskStatus_PENDING.String()
	case AttributeLabelTaskStatusRunning:
		return pb.TaskStatus_RUNNING.String()
	case AttributeLabelTaskStatusFailed:
		return pb.TaskStatus_FAILED.String()
	default:
		return ""
	}
}
