package release

import (
	"time"
)

type ReleaseFilter struct {
	ID     uint64
	Status uint32
}

// Release 发布记录
type Release struct {
	ID             uint64    `db:"id"`
	BusinessID     uint64    `db:"business_id"`     // 业务ID
	CorpID         uint64    `db:"corp_id"`         // 企业ID
	RobotID        uint64    `db:"robot_id"`        // 机器人ID
	StaffID        uint64    `db:"staff_id"`        // 员工ID
	Description    string    `db:"description"`     // 发布说明
	Status         uint32    `db:"status"`          // 状态 1:发布中,2:已发布,3:发布失败
	Message        string    `db:"message"`         // 失败原因
	TotalCount     uint64    `db:"total_count"`     // 发布总数
	SuccessCount   uint64    `db:"success_count"`   // 发布成功数
	PauseMsg       string    `db:"pause_msg"`       // 发布暂停的消息,vecdoc暂停通知时候参数，用于暂停重试参数
	CallbackStatus uint32    `db:"callback_status"` // 回调成功情况：0:下游服务均未回调,1:vector_doc回调成功,任务型没有回调
	CreateTime     time.Time `db:"create_time"`     // 创建时间
	UpdateTime     time.Time `db:"update_time"`     // 更新时间
}

// StatusDesc 状态描述
func (r *Release) StatusDesc() string {
	if r == nil {
		return ""
	}
	return statusMap[r.Status]
}

// IsPublishDone 是否发布完成
func (r *Release) IsPublishDone() bool {
	if r == nil {
		return false
	}
	return r.IsPublishSuccess() || r.IsPublishFailed()
}

// IsPublishSuccess 是否发布成功
func (r *Release) IsPublishSuccess() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusSuccess
}

// IsPublishFailed 是否发布失败
func (r *Release) IsPublishFailed() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusFail || r.Status == ReleaseStatusAuditFail
}

// IsPublishPending 是否发布中
func (r *Release) IsPublishPending() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusPending ||
		r.Status == ReleaseStatusAudit ||
		r.Status == ReleaseStatusInit
}

// IsPublishPause 是否暂停
func (r *Release) IsPublishPause() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusPause
}

// IsAudit 是否审核中
func (r *Release) IsAudit() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusAudit
}

// IsPending 是否发布中
func (r *Release) IsPending() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusPending
}

// IsAuditSuccess 是否审核成功
func (r *Release) IsAuditSuccess() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusAuditSuccess
}

// IsAuditFail 是否审核失败
func (r *Release) IsAuditFail() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusAuditFail
}

// IsInit 是否初始化状态
func (r *Release) IsInit() bool {
	if r == nil {
		return false
	}
	return r.Status == ReleaseStatusInit
}
