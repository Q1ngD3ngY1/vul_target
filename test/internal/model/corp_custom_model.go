package model

import (
	"time"
)

// CorpCustomModel 企业模型
type CorpCustomModel struct {
	ID                uint64    `db:"id"`
	CorpID            uint64    `db:"corp_id"`             // 企业ID
	ModelType         string    `db:"model_type"`          // 对话类型
	ModelName         string    `db:"model_name"`          // 模型名称
	AppType           string    `db:"app_type"`            // 应用类型
	Alias             string    `db:"alias"`               // 模型展示名称
	Prompt            string    `db:"prompt"`              // prompt
	Path              string    `db:"path"`                // path
	Target            string    `db:"target"`              // target
	HistoryWordsLimit uint32    `db:"history_words_limit"` // 对话历史内容字符限制
	HistoryLimit      uint32    `db:"history_limit"`       // 对话历史条数限制
	ServiceName       string    `db:"service_name"`        // 下游服务名
	PromptWordsLimit  uint32    `db:"prompt_words_limit"`  // prompt文本大小限制
	Note              string    `db:"note"`                // 添加原因备注
	ExpiredTime       time.Time `db:"expired_time"`        // 过期时间
	CreateTime        time.Time `db:"create_time"`         // 创建时间
	UpdateTime        time.Time `db:"update_time"`         // 更新时间
}
