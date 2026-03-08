package model

import (
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// NoticeTypeRobotBasicInfo 机器人角色设置
	NoticeTypeRobotBasicInfo = uint32(1)
	// NoticeTypeBareAnswer 未知问题回复
	NoticeTypeBareAnswer = uint32(2)
	// NoticeTypeQAExport 问答导出
	NoticeTypeQAExport = uint32(3)
	// NoticeTypeDocToQA 文档生成QA
	NoticeTypeDocToQA = uint32(4)
	// NoticeTypeRelease 发布
	NoticeTypeRelease = uint32(5)
	// NoticeTypeRejectedQuestionExport 拒答问题导出
	NoticeTypeRejectedQuestionExport = uint32(7)
	// NoticeTypeUnsatisfiedReplyExport 不满意问题导出
	NoticeTypeUnsatisfiedReplyExport = uint32(8)
	// NoticeTypeTest 评测任务
	NoticeTypeTest = uint32(6)
	// NoticeTypeAttributeLabelExport 属性标签导出
	NoticeTypeAttributeLabelExport = uint32(9)
	// NoticeTypeDocModify 文档更新
	NoticeTypeDocModify = uint32(10)
	// NoticeTypeAttributeLabelUpdate 属性标签更新
	NoticeTypeAttributeLabelUpdate = uint32(11)
	// NoticeTypeDocCharExceeded 文档字符超量隔离
	NoticeTypeDocCharExceeded = uint32(12)
	// NoticeTypeQACharExceeded 问答字符超量隔离
	NoticeTypeQACharExceeded = uint32(13)
	// NoticeTypeDocResume 文档字符超量隔离恢复
	NoticeTypeDocResume = uint32(14)
	// NoticeTypeQAResume 问答字符超量隔离恢复
	NoticeTypeQAResume = uint32(15)
	// NoticeTypeSynonymsExport 同义词导出
	NoticeTypeSynonymsExport = uint32(16)
	// NoticeTypeSynonymsImport 同义词导入
	NoticeTypeSynonymsImport = uint32(17)
	// NoticeTypeQAAuditOrAppeal 问答审核或申诉
	NoticeTypeQAAuditOrAppeal = uint32(18)
	// NoticeTypeLabelUpdate 2.6迭代标签功能升级通知
	NoticeTypeLabelUpdate = uint32(19)
	// NoticeTypeDocRename 文档重命名通知
	NoticeTypeDocRename = uint32(20)
	// NoticeTypeDocAutoDiffTask 文档自动发起对比任务通知
	NoticeTypeDocAutoDiffTask = uint32(21)
	// NoticeTypeDocDiffTask 文档对比任务完成
	NoticeTypeDocDiffTask = uint32(22)

	// NoticeClosed 通知已关闭
	NoticeClosed = uint32(1)
	// NoticeOpen 通知已开启
	NoticeOpen = uint32(0)

	// NoticeRead 通知已读
	NoticeRead = uint32(1)
	// NoticeUnread 通知未读
	NoticeUnread = uint32(0)

	// NoticeIsGlobal 全局通知
	NoticeIsGlobal = uint32(1)
	// NoticeIsNotGlobal 非全局通知
	NoticeIsNotGlobal = uint32(0)

	// NoticeIsAllowClose 通知是否允许关闭
	NoticeIsAllowClose = uint32(1)
	// NoticeIsForbidClose 通知不允许关闭
	NoticeIsForbidClose = uint32(0)

	// LevelSuccess 成功
	LevelSuccess = "success"
	// LevelWarning 警告
	LevelWarning = "warning"
	// LevelInfo 基础信息
	LevelInfo = "info"
	// LevelError 异常错误
	LevelError = "error"

	// NoticeZeroPageID 无归属页面ID
	NoticeZeroPageID = uint32(0)
	// NoticeRobotInfoPageID 基础设置-角色设置
	NoticeRobotInfoPageID = uint32(1)
	// NoticeBareAnswerPageID 基础设置-行业通用知识库
	NoticeBareAnswerPageID = uint32(2)
	// NoticeDocPageID 知识管理-文档库
	NoticeDocPageID = uint32(3)
	// NoticeQAPageID 知识管理-问答库
	NoticeQAPageID = uint32(4)
	// NoticeWaitReleasePageID 发布上线-待发布
	NoticeWaitReleasePageID = uint32(5)
	// NoticeRejectedQuestionPageID 效果调优-拒答问题
	NoticeRejectedQuestionPageID = uint32(7)
	// NoticeUnsatisfiedReplyPageID 不满意回复
	NoticeUnsatisfiedReplyPageID = uint32(8)
	// NoticeBatchTestPageID 对话测试-批量测试-测试任务
	NoticeBatchTestPageID = uint32(6)
	// NoticeAttributeLabelPageID 属性标签
	NoticeAttributeLabelPageID = uint32(9)
	// NoticeSynonymsPageID 同义词 备注：10 是 workflow
	NoticeSynonymsPageID = uint32(11)

	// OpTypeViewDetail 查看详情
	OpTypeViewDetail = uint32(1)
	// OpTypeExportQADownload 问答导出下载
	OpTypeExportQADownload = uint32(2)
	// OpTypeRetryRelease 知识库发布重试
	OpTypeRetryRelease = uint32(3)
	// OpTypeAppeal 人工申诉
	OpTypeAppeal = uint32(4)
	// OpTypeRetry 文档审核重试
	OpTypeRetry = uint32(5)
	// OpTypeViewDocCharExceeded 查看超量失效的文档
	OpTypeViewDocCharExceeded = uint32(10)
	// OpTypeViewQACharExceeded 查看超量失效的问答
	OpTypeViewQACharExceeded = uint32(11)
	// OpTypeExpandCapacity 跳转购买扩容包
	OpTypeExpandCapacity = uint32(12)
	// OpTypeLabelUpdate 2.6迭代标签功能升级通知
	OpTypeLabelUpdate = uint32(15)
	// OpTypeDocAutoDiffTask 文档自动对比任务通知
	OpTypeDocAutoDiffTask = uint32(18)
	// OpTypeDocDiffRunning 文档对比任务进行中，不需要任务操作，仅用于前端区分类型
	OpTypeDocDiffRunning = uint32(19)
	// OpTypeDocDiffFinish 文档任务完成，可点击跳转详情
	OpTypeDocDiffFinish = uint32(20)
	// OpTypeVerifyDocQA 问答生成完成，可点击校验问答对
	OpTypeVerifyDocQA = uint32(22)
	// OpTypeDocToQaModelCapacity 生成问答模型余量不足跳转购买资源包
	OpTypeDocToQaModelCapacity = uint32(25)
	// OpTypeDocToQaModelPostPaid 生成问答模型余量不足跳转开通后付费
	OpTypeDocToQaModelPostPaid = uint32(26)
	// OpTypeDocToQaModelTopUp 生成问答模型余量不足跳转充值
	OpTypeDocToQaModelTopUp = uint32(27)
	// OpTypeDocToQaModelSwitchModel 生成问答模型余量不足跳转切换模型
	OpTypeDocToQaModelSwitchModel = uint32(28)
)

// Notice 通知
type Notice struct {
	ID           uint64    `db:"id"`
	Type         uint32    `db:"typ"`            // 业务类型1机器人角色设置2未知问题回复3问答对导出4文档审核5发布
	RelateID     uint64    `db:"relate_id"`      // 业务ID
	CorpID       uint64    `db:"corp_id"`        // 企业ID
	RobotID      uint64    `db:"robot_id"`       // 机器人ID
	StaffID      uint64    `db:"staff_id"`       // 员工ID
	IsGlobal     uint32    `db:"is_global"`      // 是否全局通知 0否1是
	PageID       uint32    `db:"page_id"`        // 页面ID
	Level        string    `db:"level"`          // 通知登记
	Subject      string    `db:"subject"`        // 标题
	Content      string    `db:"content"`        // 内容
	Operation    string    `db:"operation"`      // 操作及操作参数
	IsRead       uint32    `db:"is_read"`        // 是否已读
	IsClosed     uint32    `db:"is_closed"`      // 是否已关闭
	IsAllowClose uint32    `db:"is_allow_close"` // 是否允许关闭
	UpdateTime   time.Time `db:"update_time"`    // 更新时间
	CreateTime   time.Time `db:"create_time"`    // 创建时间
}

// Operation 通知操作
type Operation struct {
	Typ    uint32   `json:"type"`
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
func (n *Notice) SetOperation(operations any) error {
	if n == nil {
		return fmt.Errorf("notice is nil")
	}
	ops, err := jsoniter.MarshalToString(operations)
	if err != nil {
		return err
	}
	n.Operation = ops
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

// GetOperation 获取操作
func (n *Notice) GetOperation() ([]Operation, error) {
	if n == nil {
		return nil, fmt.Errorf("notice is nill")
	}
	ops := make([]Operation, 0)
	err := jsoniter.UnmarshalFromString(n.Operation, &ops)
	if err != nil {
		return nil, err
	}
	return ops, nil
}
