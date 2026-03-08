// Package util 工具方法
package util

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"runtime"
	"strings"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"github.com/spf13/cast"
)

// Md5Hex Md5实现
func Md5Hex(in string) string {
	if in == "" {
		return ""
	}
	hash := md5.New()
	hash.Write([]byte(in))
	return hex.EncodeToString(hash.Sum(nil))
}

// GetFileAuditFlag 获取文件审核标志
func GetFileAuditFlag(fileType string) (uint32, error) {
	if !config.FileAuditSwitch() {
		return docEntity.AuditFlagNoNeed, nil
	}
	auditFlag, ok := docEntity.FileTypeAuditFlag[strings.ToLower(fileType)]
	if !ok {
		return 0, errs.ErrAuditFlagNotFound
	}
	return auditFlag, nil
}

// CheckReqParamsIsUint64 检查入参是否可以转换成uint64
func CheckReqParamsIsUint64(ctx context.Context, param string) (uint64, error) {
	if param == "" {
		return 0, nil
	}
	result, err := cast.ToUint64E(param)
	if err != nil {
		logx.W(ctx, "CastString2Uint64 Failed! FaileInfo:%+v", err)
		return 0, errs.ErrParamsNotExpected
	}
	return result, nil
}

// BatchCheckReqParamsIsUint64 检查入参是否可以转换成uint64
func BatchCheckReqParamsIsUint64(ctx context.Context, params []string) ([]uint64, error) {
	results := make([]uint64, 0, len(params))
	for _, param := range params {
		r, err := CheckReqParamsIsUint64(ctx, param)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, nil
}

// CheckReqBotBizIDUint64 检查入参botBizID是否为uint64
func CheckReqBotBizIDUint64(ctx context.Context, param string) (uint64, error) {
	if param == "" {
		return 0, errs.ErrParams
	}

	return CheckReqParamsIsUint64(ctx, param)

}

// CheckReqStartEndTime 检查传入的开始时间，结束时间
func CheckReqStartEndTime(ctx context.Context, startTime, endTime string) (uint64, uint64, error) {
	var expireStart, expireEnd uint64
	if startTime != "" {
		vv, err := CheckReqParamsIsUint64(ctx, startTime)
		if err != nil {
			return 0, 0, err
		}
		expireStart = vv
	}
	if endTime != "" {
		vv, err := CheckReqParamsIsUint64(ctx, endTime)
		if err != nil {
			return 0, 0, err
		}
		expireEnd = vv
	}
	if expireEnd != 0 {
		// if time.Unix(int64(expireEnd), 0).Before(time.Now().Add(time.Duration(60) * time.Second)) {
		// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800123709548 支持按分钟级别判断
		if time.Unix(int64(expireEnd), 0).Before(time.Now()) {
			// 如果结束时间小于当前时间，则不允许
			return 0, 0, errs.ErrInvalidExpireTime
		}
	}
	return expireStart, expireEnd, nil
}

// CheckReqSliceUint64 检查入参是否合法
func CheckReqSliceUint64(ctx context.Context, params []string) ([]uint64, error) {

	if len(params) == 0 {
		return nil, nil
	}
	paramsUint64 := make([]uint64, 0)
	for _, v := range params {
		vv, err := CheckReqParamsIsUint64(ctx, v)
		if err != nil {
			return paramsUint64, err
		}
		paramsUint64 = append(paramsUint64, vv)
	}
	return paramsUint64, nil
}

// GetCurrentFuncName 获取当前函数名称
func GetCurrentFuncName(skip int) string {
	// skip=0 表示当前函数（GetCurrentFuncName）
	// skip=1 表示调用当前函数的函数（即目标函数）
	pc, _, _, _ := runtime.Caller(skip)
	return runtime.FuncForPC(pc).Name()
}

// MergeJsonString 合并两个Json字符串
func MergeJsonString(jsonStr1 string, jsonStr2 string) (string, error) {
	if jsonStr1 == "" {
		return jsonStr2, nil
	}
	if jsonStr2 == "" {
		return jsonStr1, nil
	}

	var original map[string]any
	err := jsonx.Unmarshal([]byte(jsonStr1), &original)
	if err != nil {
		return "", err
	}
	var fields map[string]any
	err = jsonx.Unmarshal([]byte(jsonStr2), &fields)
	if err != nil {
		return "", err
	}

	// 合并map
	for k, v := range fields {
		original[k] = v
	}
	// 编码回JSON
	mergedJson, err := jsonx.Marshal(original)
	return string(mergedJson), err
}

func GetRequestID(ctx context.Context) string {
	requestID := contextx.Metadata(ctx).RequestID()
	if requestID != "" {
		return requestID
	}
	return contextx.TraceID(ctx)
}

// When returns v that is true when the given predicate is true else returns the second argument.
// Mock ternary operator.
func When[V any](cond bool, yes, no V) V {
	if cond {
		return yes
	}
	return no
}

// Object2String 对象转Json字符串
func Object2String(req any) string {
	b, _ := jsonx.Marshal(req)
	return string(b)
}

// Object2StringEscapeHTML 对象转Json字符串
func Object2StringEscapeHTML(req any) string {
	buff := &bytes.Buffer{}
	enc := jsonx.NewEncoder(buff)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(req)
	return buff.String()
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
		logx.E(ctx, "Compile template失败  tpl:%s err:%+v", tpl, err)
		return "", err
	}
	b := &bytes.Buffer{}
	if err := e.Execute(b, req); err != nil {
		logx.E(ctx, "Execute template失败 tpl:%s, req:%+v err:%+v", tpl, req, err)
		return "", err
	}
	return b.String(), nil
}

// StrictBase64DecodeToValidString 严格校验 Base64 并解码为字符串（仅当解码内容为合法 UTF-8 时）
// 返回 (解码后的字符串或原文, 是否成功解码并验证为文本)
func StrictBase64DecodeToValidString(input string) (string, bool) {
	// 1. 校验 Base64 格式合法性
	decodedBytes, isBase64 := strictBase64Decode(input)
	if !isBase64 {
		return input, false
	}
	// 2. 检查解码后的内容是否为有效 UTF-8
	if !utf8.Valid(decodedBytes) {
		return input, false
	}
	// 3. 安全转换为字符串（无乱码）
	return string(decodedBytes), true
}

// strictBase64Decode 严格校验 Base64 并返回解码后的字节
func strictBase64Decode(input string) ([]byte, bool) {
	// 规则 1: 长度必须是4的倍数
	if len(input)%4 != 0 {
		return nil, false
	}
	// 规则 2: 仅允许 Base64 字符
	for _, c := range input {
		if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=", c) {
			return nil, false
		}
	}
	// 规则 3: Padding 必须连续且在末尾
	padIndex := strings.Index(input, "=")
	if padIndex != -1 {
		padCount := strings.Count(input, "=")
		if padIndex != len(input)-padCount || padCount > 2 {
			return nil, false
		}
	}
	// 规则 4: 实际解码验证
	decoded, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		return nil, false
	}
	return decoded, true
}

func HasSpecificCase(text string, checkFunc func(rune) bool) bool {
	if checkFunc == nil {
		return false
	}

	for _, char := range text {
		if checkFunc(char) {
			return true
		}
	}

	return false
}

// HasLowerCase 判断字符串是否包含小写字母
func HasLowerCase(text string) bool {
	return HasSpecificCase(text, unicode.IsLower)
}

// HasUpperCase 判断字符串是否包含大写字母
func HasUpperCase(text string) bool {
	return HasSpecificCase(text, unicode.IsUpper)
}

// SetMultipleMetaData 封装设置ServerMetaData的重复模式
// 用于同时设置SpaceID和Uin，避免重复代码
func SetMultipleMetaData(ctx context.Context, spaceID, uin string) context.Context {
	newCtx := contextx.SetServerMetaData(ctx, contextx.MDSpaceID, spaceID)
	newCtx = contextx.SetServerMetaData(newCtx, contextx.MDUin, uin)
	return newCtx
}
