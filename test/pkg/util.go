package pkg

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"

	"git.woa.com/baicaoyuan/moss/metadata"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

const (
	saltMask = "KnE9UKnt4WxIZRlxutYC#"
)

// ToUTF8  字符集转码
func ToUTF8(body []byte) []byte {
	r, _ := utf8.DecodeRune(body)
	if r != utf8.RuneError {
		return body
	}
	encodings := append(
		simplifiedchinese.All,
		traditionalchinese.All...,
	)
	for _, enc := range encodings {
		decoded, err := enc.NewDecoder().Bytes(body)
		if err == nil {
			return decoded
		}
	}
	return body
}

// TelephoneMask 手机号掩码
func TelephoneMask(telephone string) string {
	return fmt.Sprintf("%s****%s", telephone[:3], telephone[7:])
}

// GetSaltPassword 密码加盐处理
func GetSaltPassword(password string) string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%s_%s", saltMask, password)))
	return hex.EncodeToString(h.Sum(nil))
}

// StringsIn 在字符串数组中检索字符串
func StringsIn(strArray []string, target string) bool {
	if len(strArray) == 0 {
		return false
	}
	sort.Strings(strArray)
	index := sort.SearchStrings(strArray, target)
	if index < len(strArray) && strArray[index] == target {
		return true
	}
	return false
}

// IsValidURL 校验 URL 是否有效
func IsValidURL(testURL string) bool {
	u, err := url.ParseRequestURI(testURL)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// SplitAndTrimString ...
func SplitAndTrimString(s string, sep string) []string {
	r := make([]string, 0)
	for _, ss := range strings.Split(s, sep) {
		t := strings.TrimSpace(ss)
		if len(t) > 0 {
			r = append(r, t)
		}
	}
	return r
}

// CopyAsPointers ...
func UIntToPtr(src uint32) *uint32 {
	return &src
}

// IntToPtr
func IntToPtr(src int32) *int32 {
	return &src
}

func GetEnvSet(ctx context.Context) string {
	return metadata.Metadata(ctx).EnvSet()
}

func GetIntPtr(num int) *int {
	p := new(int)
	*p = num
	return p
}

func UniqueStrArr(strs []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(strs))

	for _, s := range strs {
		if _, exists := seen[s]; !exists {
			seen[s] = struct{}{}
			result = append(result, s)
		}
	}
	return result
}
