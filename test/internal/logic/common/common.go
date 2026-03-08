package common

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"go.opentelemetry.io/otel/trace"
)

// GetOffsetByPage 获取分页的offset和limit, pageNumber从1开始, offset从0开始
func GetOffsetByPage(pageNumber uint32, pageSize uint32) uint32 {
	if pageSize == 0 {
		return 0
	}
	offset := (pageNumber - 1) * pageSize
	return offset
}

// GetRequestID 获取requestID
func GetRequestID(ctx context.Context) string {
	requestID := pkg.RequestID(ctx)
	if requestID != "" {
		return requestID
	}
	return trace.SpanContextFromContext(ctx).TraceID().String()
}
