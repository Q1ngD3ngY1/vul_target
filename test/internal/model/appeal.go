package model

import "time"

const (
	// AppealIng 申诉中
	AppealIng = 1 // 申诉单 审核中
	// AppealSuccess 申诉单 通过
	AppealSuccess = 2 // 申诉单 通过
	// AppealFail 申诉单 不通过
	AppealFail = 3 // 申诉单 不通过
	// AppealSuccessAddAllow 申诉单 通过并加白
	AppealSuccessAddAllow = 4 // 申诉单 通过并加白
	// AppealFailAddBlock 申诉单 不通过并加黑
	AppealFailAddBlock = 5 // 申诉单 不通过并加黑

	// AppealBizTypeRobotName 机器人昵称 申诉
	AppealBizTypeRobotName = uint32(1)
	// AppealBizTypeBareAnswer 机器人未知问题回复 申诉
	AppealBizTypeBareAnswer = uint32(2)
	// AppealBizTypeDoc 文档审核 申诉
	AppealBizTypeDoc = uint32(3)
	// AppealBizTypeRelease 发布审核 申诉
	AppealBizTypeRelease = uint32(4)
	// AppealBizTypeRobotProfile 机器人角色配置 申诉
	AppealBizTypeRobotProfile = uint32(5)
	// AppealBizTypeRobotAvatar 机器人头像 申诉
	AppealBizTypeRobotAvatar = uint32(6)
	// AppealBizTypeRobotGreeting 机器人欢迎语 申诉
	AppealBizTypeRobotGreeting = uint32(7)
	AppealBizTypeChat          = uint32(8)
	// AppealBizTypeChatInner ChatInner 会话 申诉
	AppealBizTypeChatInner = uint32(9)
	// AppealBizTypeQa 问答申诉
	AppealBizTypeQa = uint32(10)
	// AppealBizTypeDocName 文档名称申诉
	AppealBizTypeDocName = uint32(11)
	// AppealBizTypeDocSegment 文档切片申诉
	AppealBizTypeDocSegment = uint32(12)
	// AppealBizTypeDocTableSheet 文档表格sheet申诉
	AppealBizTypeDocTableSheet = uint32(13)
)

// Appeal 申诉单列表
type Appeal struct {
	ID             uint64    `db:"id"`
	CorpID         uint64    `db:"corp_id"`
	CorpFullName   string    `db:"corp_full_name"`
	RobotID        uint64    `db:"robot_id"`
	CreateStaffID  uint64    `db:"create_staff_id"`
	AuditParentID  uint64    `db:"audit_parent_id"`
	AuditID        uint64    `db:"audit_id"`
	AppealParentID uint64    `db:"appeal_parent_id"`
	Type           uint32    `db:"type"`
	Params         string    `db:"params"`
	RelateID       uint64    `db:"relate_id"`
	Status         uint32    `db:"status"`
	Result         string    `db:"result"`
	InKeywordList  uint32    `db:"in_keyword_list"`
	Reason         string    `db:"reason"`
	Operator       string    `db:"operator"`
	CreateTime     time.Time `db:"create_time"`
	UpdateTime     time.Time `db:"update_time"`
}

// NewParentAppeal 新建父申诉单
func NewParentAppeal(corpID, robotID, staffID, relateID uint64, typ uint32) *Appeal {
	return &Appeal{
		CorpID:         corpID,
		RobotID:        robotID,
		CreateStaffID:  staffID,
		AuditParentID:  0,
		AuditID:        0,
		AppealParentID: 0,
		Type:           typ,
		Params:         "",
		RelateID:       relateID,
		Status:         AuditStatusForbid,
	}
}
