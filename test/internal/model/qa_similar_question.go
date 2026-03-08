package model

import (
	"time"
)

// SimilarQuestionSimple 相似问简要信息
type SimilarQuestionSimple struct {
	SimilarID   uint64 `db:"similar_id"`    // 相似问业务ID
	Question    string `db:"question"`      // 相似问的问题内容
	RelatedQAID uint64 `db:"related_qa_id"` // 相关联的主 QA ID
}

// SimilarQuestionCount 相似问count信息
type SimilarQuestionCount struct {
	RelatedQAID uint64 `db:"related_qa_id"` // 相关联的主 QA ID
	Total       uint32 `db:"total"`         // 相似问总数
}

// SimilarQuestion 相似问题
type SimilarQuestion struct {
	ID            uint64    `db:"id"`
	SimilarID     uint64    `db:"similar_id"`     // 相似问业务ID
	RobotID       uint64    `db:"robot_id"`       // 机器人ID
	CorpID        uint64    `db:"corp_id"`        // 企业ID
	StaffID       uint64    `db:"staff_id"`       // 员工ID
	CreateUserID  uint64    `db:"create_user_id"` // 上传用户ID
	RelatedQAID   uint64    `db:"related_qa_id"`  // 相关联的主 QA ID
	Source        uint32    `db:"source"`         // 来源(2 批量导入 3 手动添加)
	Question      string    `db:"question"`       // 相似问的问题内容
	Message       string    `db:"message"`        // 失败原因
	IsDeleted     int       `db:"is_deleted"`     // 1未删除 2已删除
	ReleaseStatus uint32    `db:"release_status"` // 发布状态(1 未发布 2 待发布 3 发布中 4 已发布 5 发布失败 6 不采纳 7 审核中 8 审核失败)
	IsAuditFree   bool      `db:"is_audit_free"`  // 免审 0 不免审（需要机器审核） 1 免审（无需机器审核）
	NextAction    uint32    `db:"next_action"`    // 面向发布操作：1新增 2修改 3删除 4发布
	CreateTime    time.Time `db:"create_time"`    // 创建时间
	UpdateTime    time.Time `db:"update_time"`    // 更新时间
	CharSize      uint64    `db:"char_size"`      // 相似问问题字符长度
}

// SimilarQuestionModifyInfo 相似问修改信息
type SimilarQuestionModifyInfo struct {
	AddQuestions    []*SimilarQuestion // 新增相似问
	DeleteQuestions []*SimilarQuestion // 删除相似问
	UpdateQuestions []*SimilarQuestion // 更新相似问
}

// IsNextActionAdd 是否新增操作
func (d *SimilarQuestion) IsNextActionAdd() bool {
	if d == nil {
		return false
	}
	return d.NextAction == NextActionAdd
}

// IsDelete 是否已删除
func (d *SimilarQuestion) IsDelete() bool {
	if d == nil {
		return false
	}
	return d.IsDeleted == QAIsDeleted
}
