// bot-knowledge-config-server
//
// @(#)sse_send.go  星期四, 五月 16, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

package event

import (
	"encoding/json"
	"strings"
)

// Label 标签
type Label struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// IsValid 判断标签是否有效
func (l Label) IsValid() bool {
	if strings.TrimSpace(l.Name) == "" || len(l.Values) == 0 {
		return false
	}
	for _, v := range l.Values {
		if strings.TrimSpace(v) == "" {
			return false
		}
	}
	return true
}

// SseSendEvent SSE 发送事件
type SseSendEvent struct {
	ReqID             string          `json:"request_id"`
	Content           string          `json:"content"`
	BotAppKey         string          `json:"bot_app_key"`
	VisitorBizID      string          `json:"visitor_biz_id"`
	SessionID         string          `json:"session_id"`
	VisitorLabels     []Label         `json:"visitor_labels"`
	StreamingThrottle int             `json:"streaming_throttle"`
	Timeout           int64           `json:"timeout"`
	SystemRole        string          `json:"system_role"`
	CustomVariables   json.RawMessage `json:"custom_variables"` // 自定义参数
	IsEvaluateTest    bool            `json:"is_evaluate_test"` // 是否来自应用评测
}

// IsValid 判断请求是否合法
func (e SseSendEvent) IsValid() bool {
	if e.Content == "" || e.BotAppKey == "" || e.VisitorBizID == "" || e.SessionID == "" {
		return false
	}
	return true
}
