package release

import (
	"fmt"
	"time"
)

// Notice 通知
type Notice struct {
	ID           uint64 `db:"id"`
	Type         uint32 `db:"typ"`       // 业务类型1机器人角色设置2未知问题回复3问答对导出4文档审核5发布
	RelateID     uint64 `db:"relate_id"` // 业务ID
	CorpID       uint64 `db:"corp_id"`   // 企业ID
	RobotID      uint64 `db:"robot_id"`  // 机器人ID
	StaffID      uint64 `db:"staff_id"`  // 员工ID
	IsGlobal     uint32 `db:"is_global"` // 是否全局通知 0否1是
	PageID       uint32 `db:"page_id"`   // 页面ID
	Level        string `db:"level"`     // 通知登记
	Subject      string `db:"subject"`   // 标题
	Content      string `db:"content"`   // 内容
	Operations   []Operation
	IsRead       uint32    `db:"is_read"`        // 是否已读
	IsClosed     uint32    `db:"is_closed"`      // 是否已关闭
	IsAllowClose uint32    `db:"is_allow_close"` // 是否允许关闭
	UpdateTime   time.Time `db:"update_time"`    // 更新时间
	CreateTime   time.Time `db:"create_time"`    // 创建时间
}

// Operation 通知操作
type Operation struct {
	Type   uint32   `json:"type"`
	Params OpParams `json:"params"`
}

// OpParams 通知操作参数
type OpParams struct {
	CosPath       string `json:"cos_path,omitempty"`        // 下载地址
	VersionID     uint64 `json:"version_id,omitempty"`      // 发布ID
	AppealType    uint32 `json:"appeal_type,omitempty"`     // 申诉类型
	DocBizID      string `json:"doc_biz_id,omitempty"`      // 文档ID
	FeedbackBizID uint64 `json:"feedback_biz_id,omitempty"` // 反馈信息ID
	ExtraJSONData string `json:"extra_json_data,omitempty"` // 扩展json string
	QaBizID       string `json:"qa_biz_id,omitempty"`       // Qa biz ID
}

// NoticeOption notice参数
type NoticeOption func(n *Notice)

// NewNotice 初始化一个notice
func NewNotice(noticeType uint32, relateID, corpID, robotID, staffID uint64, options ...NoticeOption) *Notice {
	now := time.Now()
	notice := &Notice{
		Type:         noticeType,
		RelateID:     relateID,
		CorpID:       corpID,
		RobotID:      robotID,
		StaffID:      staffID,
		IsGlobal:     NoticeIsNotGlobal,
		IsRead:       NoticeUnread,
		IsClosed:     NoticeOpen,
		IsAllowClose: NoticeIsAllowClose,
		UpdateTime:   now,
		CreateTime:   now,
	}
	for _, option := range options {
		option(notice)
	}
	return notice
}

// WithGlobalFlag 设置全局通知标记
func WithGlobalFlag() NoticeOption {
	return func(n *Notice) {
		n.IsGlobal = NoticeIsGlobal
	}
}

// WithForbidCloseFlag 设置通知不可关闭
func WithForbidCloseFlag() NoticeOption {
	return func(n *Notice) {
		n.IsAllowClose = NoticeIsForbidClose
	}
}

// WithPageID 设置页面ID
func WithPageID(pageID uint32) NoticeOption {
	return func(n *Notice) {
		n.PageID = pageID
	}
}

// WithLevel 设置通知登记
func WithLevel(level string) NoticeOption {
	return func(n *Notice) {
		n.Level = level
	}
}

// WithSubject 设置通知主题
func WithSubject(subject string) NoticeOption {
	return func(n *Notice) {
		n.Subject = subject
	}
}

// WithContent 设置通知内容
func WithContent(content string) NoticeOption {
	return func(n *Notice) {
		n.Content = content
	}
}

// SetOperation 设置通知对应操作
func (n *Notice) SetOperation(operations []Operation) error {
	if n == nil {
		return fmt.Errorf("notice is nil")
	}
	n.Operations = operations
	return nil
}

// ReadFlag 已读标记
func (n *Notice) ReadFlag() bool {
	if n == nil {
		return false
	}
	return n.IsRead == NoticeRead
}

// AllowCloseFlag 是否允许关闭标记
func (n *Notice) AllowCloseFlag() bool {
	if n == nil {
		return false
	}
	return n.IsAllowClose == NoticeIsAllowClose
}

type ReleaseVectorDocTypeReq struct {
	Ids       []uint64
	DocType   uint32
	QaType    uint32
	VersionID uint64
	AppBizID  uint64
	RobotID   uint64
}
