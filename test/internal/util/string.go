package util

import (
	"math"
	"unicode/utf8"
)

// GetPrefixByUTF8Length 按照指定的长度分割字符串，返回前缀
func GetPrefixByUTF8Length(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}
	lastRuneIndex := 0
	for i := 0; i < maxLength; i++ {
		if utf8.RuneStart(s[i]) {
			lastRuneIndex = i
		}
	}
	if lastRuneIndex == 0 {
		// 如果没有找到完整的字符，取到第一个字符的结束位置
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError {
			// 处理无效字符
			lastRuneIndex = 1
		} else {
			// 当maxLength小于第一个字符的字节长度时，取maxLength，会取到无效字符
			lastRuneIndex = int(math.Min(float64(size), float64(maxLength)))
		}
	}
	return s[:lastRuneIndex]
}
