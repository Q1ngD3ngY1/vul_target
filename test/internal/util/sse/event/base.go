// bot-knowledge-config-server
//
// @(#)base.go  星期四, 五月 16, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

package event

import "encoding/json"

// Wrapper 事件 Wrapper
type Wrapper struct {
	ReqID     string          `json:"reqID"`
	Type      string          `json:"type,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Error     json.RawMessage `json:"error"`
	MessageID string          `json:"message_id,omitempty"`
}

// Listener 事件监听器
type Listener func(ev Wrapper)
