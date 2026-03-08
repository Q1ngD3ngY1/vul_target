package model

import "time"

const (
	// DocParseInit 文档待解析
	DocParseInit = 0
	// DocParseIng 文档解析任务进行中
	DocParseIng = 1
	// DocParseCancel 文档解析任务中止
	DocParseCancel = 2
	// DocParseSuccess 文档解析任务成功
	DocParseSuccess = 3
	// DocParseFailed 文档解析任务失败
	DocParseFailed = 4

	// DocParseCallBackUnknown 文档解析结果回调-未知
	DocParseCallBackUnknown = 0
	// DocParseCallBackPending 文档解析结果回调-等待
	DocParseCallBackPending = 1
	// DocParseCallBackRunning 文档解析结果回调-运行中
	DocParseCallBackRunning = 2
	// DocParseCallBackFinish 文档解析结果回调-结束（成功）
	DocParseCallBackFinish = 3
	// DocParseCallBackFailed 文档解析结果回调-失败
	DocParseCallBackFailed = 4
	// DocParseCallBackCancel 文档解析任务-取消
	DocParseCallBackCancel = 5
	// DocParseCallBackCharSizeExceeded 文档字符数超限
	DocParseCallBackCharSizeExceeded = 6

	// DocParseResultCallBackSuccess 文档解析结果回调成功
	DocParseResultCallBackSuccess = 0
	// DocParseResultCallBackFail 文档解析结果回调失败
	DocParseResultCallBackFail = 1
	// DocParseResultCallBackErr 文档解析结果回调异常（这种情况下，回调方可以重试）
	DocParseResultCallBackErr = 2

	// DocParseTaskTypeUnknown 文档解析任务类型-未知
	DocParseTaskTypeUnknown = 0
	// DocParseTaskTypeWordCount 文档解析任务类型-统计字数
	DocParseTaskTypeWordCount = 1
	// DocParseTaskTypeParse 文档解析任务类型-解析文档
	DocParseTaskTypeParse = 2
	// DocParseTaskTypeSplitSegment 文档解析任务类型-拆分文档生成 片段
	DocParseTaskTypeSplitSegment = 3
	// DocParseTaskTypeSplitQA 文档解析任务类型-拆分文档生成 QA对
	DocParseTaskTypeSplitQA = 4

	// DocParseOpTypeUnknown 文档解析任务类型-未知
	DocParseOpTypeUnknown = 0
	// DocParseOpTypeWordCount 文档解析任务类型-统计字数
	DocParseOpTypeWordCount = 1
	// DocParseOpTypeParse 文档解析任务类型-解析文档
	DocParseOpTypeParse = 2
	// DocParseOpTypeSplit 文档解析任务类型-拆分文档生成 片段 或 QA对
	DocParseOpTypeSplit = 3

	// BRecallProgressTrue 解析进度回调 开启
	BRecallProgressTrue = true
	// BRecallProgressFalse 解析进度回调 关闭
	BRecallProgressFalse = false

	// DocParseTaskNorMal 正常优先级
	DocParseTaskNorMal = 0
	// DocParseTaskHigh 高优先级
	DocParseTaskHigh = 1
)

// DocParse 文档解析
type DocParse struct {
	ID           uint64    `db:"id"`             // 文档解析任务ID
	CorpID       uint64    `db:"corp_id"`        // 企业ID
	RobotID      uint64    `db:"robot_id"`       // 机器人ID
	StaffID      uint64    `db:"staff_id"`       // 员工ID
	DocID        uint64    `db:"doc_id"`         // 文档ID
	SourceEnvSet string    `db:"source_env_set"` // 审核来源环境多 SET
	RequestID    string    `db:"request_id"`     // 文档解析任务请求唯一id
	TaskID       string    `db:"task_id"`        // taskID
	Type         uint32    `db:"type"`           // 文档解析服务任务类型-业务服务使用类型
	OpType       uint32    `db:"op_type"`        // 文档解析任务类型-解析服务使用类型
	Result       string    `db:"result"`         // 文档解析任务解析结果
	Status       uint32    `db:"status"`         // 文档解析状态
	CreateTime   time.Time `db:"create_time"`    // 创建时间
	UpdateTime   time.Time `db:"update_time"`    // 更新时间
}
