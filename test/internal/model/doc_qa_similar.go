package model

import "time"

const (
	// QaSimilarStatusInit 未处理
	QaSimilarStatusInit = 0
	// QaSimilarStatusDone 已处理
	QaSimilarStatusDone = 1

	// QaSimilarIsValid 默认有效  当
	QaSimilarIsValid = 1
	// QaSimilarInValid 无效  相似ID不被采纳，根据相似ID获取的其他问答ID被置为无效
	QaSimilarInValid = 0
)

// DocQASimilar 相似问答对表
type DocQASimilar struct {
	ID         uint64    `db:"id"`
	BusinessID uint64    `db:"business_id"` // 业务ID
	RobotID    uint64    `db:"robot_id"`    // 机器人ID
	CorpID     uint64    `db:"corp_id"`     // 企业ID
	StaffID    uint64    `db:"staff_id"`    // 员工ID
	DocID      uint64    `db:"doc_id"`      // 文档ID
	QaID       uint64    `db:"qa_id"`       // 问答对ID
	SimilarID  uint64    `db:"similar_id"`  // 相似ID
	IsValid    uint32    `db:"is_valid"`    // 是否有效1有效0无效
	Status     uint64    `db:"status"`      // 0未处理1已处理
	CreateTime time.Time `db:"create_time"` // 创建时间
	UpdateTime time.Time `db:"update_time"` // 更新时间
}
