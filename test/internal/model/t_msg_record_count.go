package model

import (
	"database/sql"
	"time"
)

// MsgDataCountReq 数据统计请求结构
type MsgDataCountReq struct {
	AppBizIds []uint64
	Type      uint32
	ToType    uint32
	StartTime time.Time
	EndTime   time.Time
}

// LikeDataCount 点赞点踩
type LikeDataCount struct {
	ID           uint64        `db:"id"`            // 主键ID
	BotBizId     uint64        `db:"bot_biz_id"`    // 应用ID
	Type         uint32        `db:"type"`          // 消息类型
	Day          time.Time     `db:"day"`           // 天
	FormatDay    string        `db:"format_day"`    // 天日期转换格式
	Hour         uint32        `db:"hour"`          // 小时
	TotalCount   sql.NullInt32 `db:"total_count"`   // 消息总数
	LikeCount    sql.NullInt32 `db:"like_count"`    // 点赞总数
	DislikeCount sql.NullInt32 `db:"dislike_count"` // 点踩总数
}

// AnswerTypeDataCount 回答类型数据统计
type AnswerTypeDataCount struct {
	ID                      uint64        `db:"id"`                        // 主键ID
	BotBizId                uint64        `db:"bot_biz_id"`                // 应用ID
	Type                    uint32        `db:"type"`                      // 消息类型
	ToType                  uint32        `db:"to_type"`                   // 消息类型
	Day                     time.Time     `db:"day"`                       // 天
	FormatDay               string        `db:"format_day"`                // 天日期转换格式
	Hour                    uint32        `db:"hour"`                      // 小时
	TotalCount              sql.NullInt32 `db:"total_count"`               // 消息总数
	ModelReplyCount         sql.NullInt32 `db:"model_reply_count"`         // 大模型直接回复总数
	KnowledgeCount          sql.NullInt32 `db:"knowledge_count"`           // 知识型回复总数
	TaskFlowCount           sql.NullInt32 `db:"task_flow_count"`           // 任务流回复总数
	SearchEngineCount       sql.NullInt32 `db:"search_engine_count"`       // 搜索引擎回复总数
	ImageUnderstandingCount sql.NullInt32 `db:"image_understanding_count"` // 图片理解回复总数
	RejectCount             sql.NullInt32 `db:"reject_count"`              // 拒答回复总数
	SensitiveCount          sql.NullInt32 `db:"sensitive_count"`           // 敏感回复总数
	ConcurrentLimitCount    sql.NullInt32 `db:"concurrent_limit_count"`    // 并发超限回复总数
	UnknownIssuesCount      sql.NullInt32 `db:"unknown_issues_count"`      // 未知问题回复总数
}
