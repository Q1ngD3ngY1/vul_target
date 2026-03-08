// bot-knowledge-config-server
//
// @(#)error.go  星期四, 五月 16, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

// Package event 事件
package event

// EventError 错误事件
const EventError = "error"

// ErrorEvent 错误事件消息体
type ErrorEvent struct {
	Error     Error  `json:"error"`
	RequestID string `json:"request_id"`
}

// Error 错误
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
