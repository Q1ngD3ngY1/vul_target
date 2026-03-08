package model

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"
)

const (
	// UnsatisfiedReplyStatusWait 不满意回复状态-待处理
	UnsatisfiedReplyStatusWait = iota
	// UnsatisfiedReplyStatusReject 不满意回复状态-已拒答
	UnsatisfiedReplyStatusReject
	// UnsatisfiedReplyStatusIgnore 不满意回复状态-已忽略
	UnsatisfiedReplyStatusIgnore
	// UnsatisfiedReplyStatusPass 不满意回复状态-已添加
	UnsatisfiedReplyStatusPass

	// UnsatisfiedReplyNoticeContent = "不满意回复批量导出成功。"
	UnsatisfiedReplyNoticeContent = i18nkey.KeyUnsatisfiedRepliesBatchExportStatus

	// UnsatisfiedReplyNoticeContentIng = "不满意回复批量导出中。"
	UnsatisfiedReplyNoticeContentIng = i18nkey.KeyUnsatisfiedRepliesBatchExporting
)

const (
	// UnsatisfiedReplyIsNotDeleted 未删除
	UnsatisfiedReplyIsNotDeleted = 0
	// UnsatisfiedReplyDeleted 已删除
	UnsatisfiedReplyDeleted = 1

	// UnsatisfiedReasonIsNotDeleted 未删除
	UnsatisfiedReasonIsNotDeleted = 0
	// UnsatisfiedReasonDeleted 已删除
	UnsatisfiedReasonDeleted = 1

	UnsatisfiedStatusAll       = 0 // 全部【待处理+已处理】
	UnsatisfiedStatusNotHandle = 1 // 待处理
	UnsatisfiedStatusHandled   = 2 // 已处理

)

// UnsatisfiedReply 不满意回复
type UnsatisfiedReply struct {
	ID         uint64    `db:"id"`          // 不满意回复ID
	BusinessID uint64    `db:"business_id"` // 业务ID
	CorpID     uint64    `db:"corp_id"`     // 企业ID
	RobotID    uint64    `db:"robot_id"`    // 机器人ID
	RecordID   string    `db:"record_id"`   // 消息记录ID
	Question   string    `db:"question"`    // 用户问题
	Answer     string    `db:"answer"`      // 机器人回复
	Context    string    `db:"context"`     // 不满意回复上下文信息
	StaffID    uint64    `db:"staff_id"`    // 操作人
	Status     uint32    `db:"status"`      // 不满意回复状态
	UserType   uint32    `db:"user_type"`   // 用户类型，0-普通用户；1-体验用户
	IsDeleted  uint8     `db:"is_deleted"`  // 是否删除
	CreateTime time.Time `db:"create_time"` // 创建时间
	UpdateTime time.Time `db:"update_time"` // 更新时间
}

// UnsatisfiedReplyInfo  不满意回复信息
type UnsatisfiedReplyInfo struct {
	UnsatisfiedReply          // 不满意回复数据
	Reasons          []string // 不满意原因
}

// UnsatisfiedReason 不满意原因
type UnsatisfiedReason struct {
	ID            uint64 `db:"id"`             // ID
	UnsatisfiedID uint64 `db:"unsatisfied_id"` // 不满意回复ID
	Reason        string `db:"reason"`         // 不满意原因
	IsDeleted     uint8  `db:"is_deleted"`     // 是否删除
}

// UnsatisfiedReplyListReq 不满意回复列表请求
type UnsatisfiedReplyListReq struct {
	CorpID   uint64   // 企业ID
	RobotID  uint64   // 机器人ID
	Query    string   // 关键字 检索用户问题或答案
	Reasons  []string // 不满意回复原因
	IDs      []uint64 // 不满意回复IDs 上云后删除
	BizIDs   []uint64 // 业务IDs
	Page     uint32   // 页码
	PageSize uint32   // 每页大小
	Status   uint32   // 操作状态 0-待处理  1-已处理【包括已拒绝，已忽略，已添加】
}
