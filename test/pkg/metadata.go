package pkg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/metadata"
	"github.com/google/uuid"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"
)

const (
	metaUserID     = "UserID"
	metaStaffID    = "StaffID"
	metaStaffBizID = "StaffBizID"
	metaSpaceID    = "SpaceID"
	metaCorpID     = "CorpID"
	metaCorpBizID  = "CorpBizID"
	metaSID        = "SID"
	metaDebug      = "Debug"

	metaToken              = "token"
	metaUin                = "uin"
	metaSubAccountUin      = "sub_account_uin"
	metaLoginUin           = "login_uin"
	metaLoginSubAccountUin = "login_sub_account_uin"
	metaRequestID          = "request_id"
	metaAction             = "action"
	metaAppID              = "app_id"
	metaAppName            = "app_name"

	metaLoginUserType = "login_user_type"

	metaLanguage = "x_tc_language"
)

// WithStaffID 携带员工 ID
func WithStaffID(ctx context.Context, staffID uint64) context.Context {
	return metadata.SetServerMetaData(ctx, metaStaffID, strconv.FormatUint(staffID, 10))
}

// WithStaffBizID 携带员工业务ID
func WithStaffBizID(ctx context.Context, staffID uint64) context.Context {
	return metadata.SetServerMetaData(ctx, metaStaffBizID, strconv.FormatUint(staffID, 10))
}

// WithCorpID 携带企业 ID
func WithCorpID(ctx context.Context, corpID uint64) context.Context {
	return metadata.SetServerMetaData(ctx, metaCorpID, strconv.FormatUint(corpID, 10))
}

// WithCorpBizID 携带企业业务ID
func WithCorpBizID(ctx context.Context, corpID uint64) context.Context {
	return metadata.SetServerMetaData(ctx, metaCorpBizID, strconv.FormatUint(corpID, 10))
}

// WithSID 携带集成商 ID
func WithSID(ctx context.Context, sID uint64) context.Context {
	return metadata.SetServerMetaData(ctx, metaSID, strconv.FormatUint(sID, 10))
}

// WithToken 携带token
func WithToken(ctx context.Context, token string) context.Context {
	return metadata.SetServerMetaData(ctx, metaToken, token)
}

// WithAppID 携带应用 ID
func WithAppID(ctx context.Context, sID uint64) context.Context {
	return metadata.SetServerMetaData(ctx, metaAppID, strconv.FormatUint(sID, 10))
}

// WithAppName 携带应用 ID
func WithAppName(ctx context.Context, appName string) context.Context {
	return metadata.SetServerMetaData(ctx, metaAppName, appName)
}

// WithSpaceID 携带空间ID
func WithSpaceID(ctx context.Context, spaceID string) context.Context {
	return metadata.SetServerMetaData(ctx, metaSpaceID, spaceID)
}

// Token 获取token
func Token(ctx context.Context) string {
	return metadata.Metadata(ctx).Get(metaToken)
}

// LoginUserType 获取登录的用户类型
func LoginUserType(ctx context.Context) uint32 {
	userType := metadata.Metadata(ctx).Get(metaLoginUserType)
	if len(userType) == 0 {
		return 0
	}
	return cast.ToUint32(userType)
}

// SID 获取SID
func SID(ctx context.Context) int {
	staffID := metadata.Metadata(ctx).Get(metaSID)
	if staffID == "" {
		return 0
	}
	return cast.ToInt(staffID)
}

// AppID 获取AppID
func AppID(ctx context.Context) uint64 {
	appID := metadata.Metadata(ctx).Get(metaAppID)
	if appID == "" {
		return 0
	}
	return cast.ToUint64(appID)
}

// AppName 获取AppName
func AppName(ctx context.Context) string {
	appName := metadata.Metadata(ctx).Get(metaAppName)
	return appName
}

// StaffID 获取staffID
func StaffID(ctx context.Context) uint64 {
	staffID := metadata.Metadata(ctx).Get(metaStaffID)
	if staffID == "" {
		return 0
	}
	return cast.ToUint64(staffID)
}

// StaffBizID 获取StaffBizID
func StaffBizID(ctx context.Context) uint64 {
	staffBizID := metadata.Metadata(ctx).Get(metaStaffBizID)
	if staffBizID == "" {
		return 0
	}
	return cast.ToUint64(staffBizID)
}

// CorpID 获取corpID
func CorpID(ctx context.Context) uint64 {
	corpID := metadata.Metadata(ctx).Get(metaCorpID)
	if corpID == "" {
		return 0
	}
	return cast.ToUint64(corpID)
}

// CorpBizID 获取CorpBizID
func CorpBizID(ctx context.Context) uint64 {
	corpBizID := metadata.Metadata(ctx).Get(metaCorpBizID)
	if corpBizID == "" {
		return 0
	}
	return cast.ToUint64(corpBizID)
}

// SpaceID 获取SpaceID
func SpaceID(ctx context.Context) string {
	spaceID := metadata.Metadata(ctx).Get(metaSpaceID)
	return spaceID
}

// Action 当前操作
func Action(ctx context.Context) string {
	return metadata.Metadata(ctx).Get(metaAction)
}

// Uin AKSK 对应的云主账号 Uin
func Uin(ctx context.Context) string {
	return metadata.Metadata(ctx).Get(metaUin)
}

// SubAccountUin AKSK 对应的云子账号 Uin
func SubAccountUin(ctx context.Context) string {
	return metadata.Metadata(ctx).Get(metaSubAccountUin)
}

// LoginUin 登录态对应的云主账号 Uin
func LoginUin(ctx context.Context) string {
	return metadata.Metadata(ctx).Get(metaLoginUin)
}

// LoginSubAccountUin 登录态对应的云子账号 Uin
func LoginSubAccountUin(ctx context.Context) string {
	return metadata.Metadata(ctx).Get(metaLoginSubAccountUin)
}

// RequestID 云请求 ID
func RequestID(ctx context.Context) string {
	return metadata.Metadata(ctx).Get(metaRequestID)
}

// IsDebug 获取调试开关
func IsDebug(ctx context.Context) bool {
	debug := metadata.Metadata(ctx).Get(metaDebug)
	if debug == "" {
		return false
	}
	return cast.ToBool(debug)
}

// WithRequestID 携带云请求 ID
func WithRequestID(ctx context.Context, requestID string) {
	trpc.SetMetaData(ctx, metaRequestID, []byte(requestID))
}

// WithEnvSet 设置env-set
func WithEnvSet(ctx context.Context, envSet string) context.Context {
	return metadata.SetServerMetaData(ctx, metadata.EnvSet, envSet)
}

// WithUin 在context中加入UIN
func WithUin(ctx context.Context, uin string) {
	uinInCtx := metadata.Metadata(ctx).Get(metaUin)
	if uinInCtx != "" {
		return
	}
	trpc.SetMetaData(ctx, metaUin, []byte(uin))
}

// WithSubAccountUin 在context中加入 SubAccountUin
func WithSubAccountUin(ctx context.Context, sub string) {
	uinInCtx := metadata.Metadata(ctx).Get(metaSubAccountUin)
	if uinInCtx != "" {
		return
	}
	trpc.SetMetaData(ctx, metaSubAccountUin, []byte(sub))
}

func WithAction(ctx context.Context, action string) {
	uinInCtx := metadata.Metadata(ctx).Get(metaAction)
	if uinInCtx != "" {
		return
	}
	trpc.SetMetaData(ctx, metaAction, []byte(action))
}

// LanguageDef 返回 language 信息，默认给 def
func LanguageDef(ctx context.Context, def ...string) string {
	defLanguage := "zh-CN"
	if len(def) > 0 {
		defLanguage = def[0]
	}
	l := metadata.Metadata(ctx).Get(metaLanguage)
	if len(l) == 0 {
		return defLanguage
	}
	return l
}

// NewCtxWithTraceID 重新初始化一个ctx，采用原ctx的traceID
func NewCtxWithTraceID(oriCtx context.Context) context.Context {
	traceID := trace.SpanContextFromContext(oriCtx).TraceID().String()
	if traceID == "" {
		traceID = strings.ReplaceAll(uuid.New().String(), "-", "")
	}
	ctx := trpc.BackgroundContext()
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		log.Warnf("trace id from hex err: %v", err)
	}
	sid := trace.SpanID{}
	_, err = rand.Read(sid[:])
	if err != nil {
		log.Warnf("trace id rand failed, err: %v", err)
	}

	spanCtx := trace.SpanContextFromContext(ctx)
	if tid.IsValid() && sid.IsValid() {
		spanCtx = spanCtx.WithTraceID(tid).WithSpanID(sid)
		h := fmt.Sprintf("%.2x-%s-%s-%s", 0, traceID, sid.String(), hex.EncodeToString([]byte{0}))
		trpc.SetMetaData(ctx, "traceparent", []byte(h))
	}
	ctx = trace.ContextWithSpanContext(ctx, spanCtx)
	log.WithContextFields(ctx, "traceID", traceID)
	return ctx
}
