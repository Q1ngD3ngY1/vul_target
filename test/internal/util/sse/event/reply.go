// bot-knowledge-config-server
//
// @(#)reply.go  星期四, 五月 16, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

package event

// ReplyMethod 回复方式
type ReplyMethod uint8

// 回复方式
const (
	ReplyMethodModel          ReplyMethod = 1  // 大模型直接回复
	ReplyMethodBare           ReplyMethod = 2  // 保守回复, 未知问题回复
	ReplyMethodRejected       ReplyMethod = 3  // 拒答问题回复
	ReplyMethodEvil           ReplyMethod = 4  // 敏感回复
	ReplyMethodPriorityQA     ReplyMethod = 5  // 问答对直接回复, 已采纳问答对优先回复
	ReplyMethodGreeting       ReplyMethod = 6  // 欢迎语回复
	ReplyMethodBusy           ReplyMethod = 7  // 并发超限回复
	ReplyGlobalKnowledge      ReplyMethod = 8  // 全局干预知识
	ReplyMethodTaskFlow       ReplyMethod = 9  // 任务流程过程回复, 当历史记录中 task_flow.type = 0 时, 为大模型回复
	ReplyMethodTaskAnswer     ReplyMethod = 10 // 任务流程答案回复
	ReplyMethodSearch         ReplyMethod = 11 // 搜索引擎回复
	ReplyMethodDecorator      ReplyMethod = 12 // 知识润色后回复
	ReplyMethodImage          ReplyMethod = 13 // 图片理解回复
	ReplyMethodFile           ReplyMethod = 14 // 实时文档回复
	ReplyMethodClarifyConfirm ReplyMethod = 15 // 澄清确认回复
	ReplyMethodWorkflow       ReplyMethod = 16 // 工作流回复
)

// ReplyMethodDescription 回复方式描述
func ReplyMethodDescription(method ReplyMethod) string {
	switch method {
	case ReplyMethodModel:
		return "大模型直接回复"
	case ReplyMethodBare:
		return "保守回复"
	case ReplyMethodRejected:
		return "拒答问题回复"
	case ReplyMethodEvil:
		return "敏感回复"
	case ReplyMethodPriorityQA:
		return "问答对直接回复"
	case ReplyMethodGreeting:
		return "欢迎语回复"
	case ReplyMethodBusy:
		return "并发超限回复"
	case ReplyGlobalKnowledge:
		return "全局干预知识回复"
	case ReplyMethodTaskFlow:
		return "任务流程过程回复"
	case ReplyMethodTaskAnswer:
		return "任务流程答案回复"
	case ReplyMethodSearch:
		return "搜索引擎回复"
	case ReplyMethodDecorator:
		return "知识润色后回复"
	case ReplyMethodImage:
		return "图片理解回复"
	case ReplyMethodFile:
		return "实时文档回复"
	case ReplyMethodClarifyConfirm:
		return "澄清确认回复"
	case ReplyMethodWorkflow:
		return "工作流回复"
	default:
		return "-"
	}
}

// ReplyEvent 回复/确认事件消息体
type ReplyEvent struct {
	RequestID       string           `json:"request_id"`
	SessionID       string           `json:"session_id"`
	Content         string           `json:"content"`
	FromName        string           `json:"from_name"`
	FromAvatar      string           `json:"from_avatar"`
	RecordID        string           `json:"record_id"`
	RelatedRecordID string           `json:"related_record_id"`
	Timestamp       int64            `json:"timestamp"`
	IsFinal         bool             `json:"is_final"`
	IsFromSelf      bool             `json:"is_from_self"`
	CanRating       bool             `json:"can_rating"`
	IsEvil          bool             `json:"is_evil"`
	IsLLMGenerated  bool             `json:"is_llm_generated"`
	Knowledge       []ReplyKnowledge `json:"knowledge"`
	ReplyMethod     ReplyMethod      `json:"reply_method"`
	TraceID         string           `json:"trace_id"`
}

// ReplyKnowledge 回复事件中的知识
type ReplyKnowledge struct {
	ID   string `json:"id"`
	Type uint32 `json:"type"`
}
