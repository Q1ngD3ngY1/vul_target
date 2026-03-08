package model

import "time"

const (
	// ReleaseVectorSuccessCallbackFlag vector服务的回调请求成功
	ReleaseVectorSuccessCallbackFlag = 1 << iota
	// ReleaseTaskFlowSuccessCallbackFlag 任务型服务的回调请求成功
	ReleaseTaskFlowSuccessCallbackFlag
	// ReleaseNoServeCallbackFlag 无服务回调
	ReleaseNoServeCallbackFlag = 0
	// ReleaseAllServeCallbackFlag 所有服务回调请求都成功
	ReleaseAllServeCallbackFlag = 3

	// ReleaseVectorCallback vector服务的回调请求
	ReleaseVectorCallback = uint32(0)
	// ReleaseTaskConfigCallback 任务型服务的回调请求
	ReleaseTaskConfigCallback = uint32(1)
	// SimilarVersionID 相似向量库版本ID
	SimilarVersionID = uint64(1)
	// ReviewVersionID 评测向量库版本ID
	ReviewVersionID = uint64(2)
	// SegmentSimilarVersionID 文档段相似向量库版本ID
	SegmentSimilarVersionID = uint64(3)
	// SegmentReviewVersionID 文档段评测向量库版本ID
	SegmentReviewVersionID = uint64(4)
	// RejectedQuestionSimilarVersionID 拒答问题相似向量库版本ID
	RejectedQuestionSimilarVersionID = uint64(5)
	// RejectedQuestionReviewVersionID 拒答问题评测向量库版本ID
	RejectedQuestionReviewVersionID = uint64(6)
	// SearchEngineVersionID 搜索引擎的版本ID
	SearchEngineVersionID = uint64(7)
	// SearchGlobalVersionID 全局干预知识库
	SearchGlobalVersionID = uint64(8)
	// RealtimeSegmentVersionID 实时文档段向量库版本ID
	RealtimeSegmentVersionID = uint64(9)
	// SegmentImageReviewVersionID 文档段图片评测向量库版本ID
	SegmentImageReviewVersionID = uint64(10)
	// RealtimeSegmentImageVersionID 实时文档段图片向量库版本ID
	RealtimeSegmentImageVersionID = uint64(11)
	// DbSourceVersionID 外部数据库向量库版本ID
	DbSourceVersionID = uint64(12)

	// ReleaseStatusInit 待发布
	ReleaseStatusInit = uint32(1)
	// ReleaseStatusPending 发布中
	ReleaseStatusPending = uint32(2)
	// ReleaseStatusSuccess 发布成功
	ReleaseStatusSuccess = uint32(3)
	// ReleaseStatusFail 发布失败
	ReleaseStatusFail = uint32(4)
	// ReleaseStatusAudit 审核中
	ReleaseStatusAudit = uint32(5)
	// ReleaseStatusAuditSuccess 审核成功
	ReleaseStatusAuditSuccess = uint32(6)
	// ReleaseStatusAuditFail 审核失败
	ReleaseStatusAuditFail = uint32(7)
	// ReleaseStatusSuccessCallback 发布成功回调处理中
	ReleaseStatusSuccessCallback = uint32(8)
	// ReleaseStatusPause 发布暂停
	ReleaseStatusPause = uint32(9)
	// ReleaseStatusAppealIng 申诉审核中
	ReleaseStatusAppealIng = uint32(10)
	// ReleaseStatusAppealSuccess 申诉审核通过
	ReleaseStatusAppealSuccess = uint32(11)
	// ReleaseStatusAppealFail 申诉审核不通过
	ReleaseStatusAppealFail = uint32(12)

	// ReleaseTypeQA QA
	ReleaseTypeQA = uint32(1)
	// ReleaseTypeSegment 文档片段
	ReleaseTypeSegment = uint32(2)
	// ReleaseTypeRejectedQuestion 拒答问题
	ReleaseTypeRejectedQuestion = uint32(3)

	// TaskConfigBusinessNameTextRobot 任务型业务名称文本客服
	TaskConfigBusinessNameTextRobot = "TEXT_ROBOT"
	// TaskConfigEventCollect 任务型采集事件标识
	TaskConfigEventCollect = "COLLECT"
	// TaskConfigEventRelease 任务型发布事件标识
	TaskConfigEventRelease = "RELEASE"
	// TaskConfigEventPause 任务型暂停事件标识
	TaskConfigEventPause = "PAUSE"
	// TaskConfigEventRetry 任务型重试事件标识
	TaskConfigEventRetry = "RETRY"
)

// ReleaseType 发布类型
var ReleaseType = []uint32{ReleaseTypeQA, ReleaseTypeSegment, ReleaseTypeRejectedQuestion}

var statusMap = map[uint32]string{
	ReleaseStatusInit:            "待发布",
	ReleaseStatusPending:         "发布中",
	ReleaseStatusSuccess:         "发布成功",
	ReleaseStatusFail:            "发布失败",
	ReleaseStatusAudit:           "发布中",
	ReleaseStatusAuditSuccess:    "发布中",
	ReleaseStatusAuditFail:       "发布失败",
	ReleaseStatusSuccessCallback: "发布中",
	ReleaseStatusPause:           "发布暂停",
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
	return r.Status == ReleaseStatusPending || r.Status == ReleaseStatusAudit || r.Status == ReleaseStatusInit
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

// GetType 返回类型
func GetType(docType uint32) uint64 {
	typ := ReviewVersionID
	if docType == DocTypeSegment {
		typ = SegmentReviewVersionID
	}
	return typ
}
