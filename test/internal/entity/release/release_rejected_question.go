package release

import (
	"time"

	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
)

// ReleaseRejectedQuestion 拒答发布信息表
type ReleaseRejectedQuestion struct {
	ID                 uint64    `db:"id"`                   // 拒答问题发布表自增ID
	CorpID             uint64    `db:"corp_id"`              // 企业ID
	RobotID            uint64    `db:"robot_id"`             // 企业ID
	CreateStaffID      uint64    `db:"create_staff_id"`      // 新增拒答问题用户ID
	VersionID          uint64    `db:"version_id"`           // 版本ID
	RejectedQuestionID uint64    `db:"rejected_question_id"` // 拒答问题表 t_rejected_question中的 ID
	Question           string    `db:"question"`             // 问题
	ReleaseStatus      uint32    `db:"release_status"`       // 发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)
	Message            string    `db:"message"`              // 失败原因
	IsDeleted          uint32    `db:"is_deleted"`           // 0未删除 1已删除
	Action             uint32    `db:"action"`               // 操作行为：1新增 2修改 3删除 4发布
	IsAllowRelease     uint32    `db:"is_allow_release"`     // 允许发布： 0不允许发布 1允许发布
	UpdateTime         time.Time `db:"update_time"`          // 更新时间
	CreateTime         time.Time `db:"create_time"`          // 创建时间
}

var ReleaseRejectedQuestionMap = map[uint32]string{
	qaEntity.RejectedQuestionAdd:     "新增",
	qaEntity.RejectedQuestionUpdate:  "修改",
	qaEntity.RejectedQuestionDelete:  "删除",
	qaEntity.RejectedQuestionPublish: "发布",
}

// RejectedQuestionActionDesc 状态描述
func (d *ReleaseRejectedQuestion) RejectedQuestionActionDesc() string {
	return ReleaseRejectedQuestionMap[d.Action]
}

type ListReleaseRejectedQuestionReq struct {
	CorpID                uint64
	RobotID               uint64
	VersionID             uint64
	Question              string
	ReleaseStatuses       []uint32
	MinRejectedQuestionID uint64
	MaxRejectedQuestionID uint64
	IsAllowRelease        *uint32
	IsDeleted             *uint32
	IsDeletedNot          *uint32
	Page                  uint32
	PageSize              uint32
	OrderBy               string
}

type ReleaseRejectedQuestionFilter struct {
	ID                    uint64
	CorpID                uint64
	RobotID               uint64
	RejectedQuestionID    uint64
	VersionId             uint64
	Question              string
	Actions               []int
	IsAllowRelease        *uint32
	IsDeleted             *uint32
	IsDeletedNot          *uint32
	MinRejectedQuestionID uint64
	MaxRejectedQuestionID uint64
}
