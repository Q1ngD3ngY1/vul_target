// bot-knowledge-config-server
//
// @(#)llm_req.go  星期二, 八月 20, 2024
// Copyright(c) 2024, reinhold@Tencent. All rights reserved.

package util

import (
	"bytes"
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"strings"
	"text/template"

	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	llmm "git.woa.com/dialogue-platform/proto/pb-stub/llm-manager-server"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// TraceID TODO
func TraceID(ctx context.Context) string {
	return trace.SpanContextFromContext(ctx).TraceID().String()
}

// RequestID 获取大模型请求 ID
func RequestID(ctx context.Context, sessionID, requestID string) string {
	return TraceID(ctx) + ":" + sessionID + ":" + requestID
}

// NewLLMRequest 构造摘要请求LLMRequest
func NewLLMRequest(ctx context.Context, request *knowledge.GetDocSummaryReq) *llmm.Request {
	messages := make([]*llmm.Message, 0)
	messages = append(messages, &llmm.Message{Role: llmm.Role_USER, Content: ""})
	return &llmm.Request{
		RequestId:   RequestID(ctx, request.GetSessionId(), uuid.NewString()), // 每次随机生成一个RequestID
		ModelName:   request.GetModelName(),
		AppKey:      fmt.Sprintf("%d", request.BotBizId),
		Messages:    messages,
		PromptType:  llmm.PromptType_TEXT,
		RequestType: llmm.RequestType_ONLINE,
		Biz:         "cs",
	}
}

// Render 模版渲染
func Render(ctx context.Context, tpl string, req any) (string, error) {
	// 去除模版每行中的空白符
	lines := strings.Split(tpl, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	tpl = strings.Join(lines, "\n")

	e, err := template.New("").Parse(tpl)
	if err != nil {
		log.ErrorContextf(ctx, "Compile template失败  tpl:%s err:%+v", tpl, err)
		return "", err
	}
	b := &bytes.Buffer{}
	if err := e.Execute(b, req); err != nil {
		log.ErrorContextf(ctx, "Execute template失败 tpl:%s, req:%+v err:%+v", tpl, req, err)
		return "", err
	}
	return b.String(), nil
}
