// Package util 工具方法
package util

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	jsoniter "github.com/json-iterator/go"
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

// GetAuditFlag TODO
func GetAuditFlag(fileType string) (uint32, error) {
	if !config.AuditSwitch() {
		return model.AuditFlagNoNeed, nil
	}
	auditFlag, ok := model.FileTypeAuditFlag[strings.ToLower(fileType)]
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
		log.WarnContextf(ctx, "CastString2Uint64 Failed! FaileInfo:%+v", err)
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
		//if time.Unix(int64(expireEnd), 0).Before(time.Now().Add(time.Duration(60) * time.Second)) {
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

// Object2String 对象转Json字符串
func Object2String(req any) string {
	b, _ := jsoniter.Marshal(req)
	return string(b)
}

// Object2StringEscapeHTML 对象转Json字符串
func Object2StringEscapeHTML(req any) string {
	buff := &bytes.Buffer{}
	enc := jsoniter.NewEncoder(buff)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(req)
	return buff.String()
}

// GetUint64FromString 字符串转uint64
func GetUint64FromString(str string) uint64 {
	uInt64Data, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		log.ErrorContextf(trpc.BackgroundContext(), "strconv.ParseUint failed, error: %v", err)
	}
	return uInt64Data
}

// GetCurrentFuncName 获取当前函数名称
func GetCurrentFuncName(skip int) string {
	// skip=0 表示当前函数（GetCurrentFuncName）
	// skip=1 表示调用当前函数的函数（即目标函数）
	pc, _, _, _ := runtime.Caller(skip)
	return runtime.FuncForPC(pc).Name()
}

// FloatsToBytes float32转byte
func FloatsToBytes(floats []float32) []byte {
	sizeofFloat32 := int(unsafe.Sizeof(float32(0)))
	if len(floats) == 0 {
		return []byte{}
	}
	ptr := unsafe.Pointer(&floats[0])
	dataLen := len(floats) * sizeofFloat32
	g := make([]byte, dataLen)
	copy(g, (*[1 << 30]byte)(ptr)[:dataLen:dataLen])
	return g
}

// BytesToFloats byte转float32
func BytesToFloats(bytes []byte) []float32 {
	sizeofFloat32 := int(unsafe.Sizeof(float32(0)))
	if len(bytes) < sizeofFloat32 {
		return []float32{}
	}
	ptr := unsafe.Pointer(&bytes[0])
	dataLen := len(bytes) / sizeofFloat32
	g := make([]float32, dataLen)
	copy(g, (*[1 << 30]float32)(ptr)[:dataLen:dataLen])
	return g
}

// MergeJsonString 合并两个Json字符串
func MergeJsonString(jsonStr1 string, jsonStr2 string) (string, error) {
	if jsonStr1 == "" {
		return jsonStr2, nil
	}
	if jsonStr2 == "" {
		return jsonStr1, nil
	}

	var original map[string]interface{}
	err := jsoniter.Unmarshal([]byte(jsonStr1), &original)
	if err != nil {
		return "", err
	}
	var fields map[string]interface{}
	err = jsoniter.Unmarshal([]byte(jsonStr2), &fields)
	if err != nil {
		return "", err
	}

	// 合并map
	for k, v := range fields {
		original[k] = v
	}
	// 编码回JSON
	mergedJson, err := jsoniter.Marshal(original)
	return string(mergedJson), err
}

// IsValidBase64 严格校验 Base64 字符串
func IsValidBase64(s string) bool {
	if len(s)%4 != 0 {
		s += strings.Repeat("=", 4-(len(s)%4))
	}
	for _, r := range s {
		if !(r >= 'A' && r <= 'Z' ||
			r >= 'a' && r <= 'z' ||
			r >= '0' && r <= '9' ||
			r == '+' || r == '/' || r == '=') {
			return false
		}
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return false
	}
	return utf8.Valid(decoded)
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
