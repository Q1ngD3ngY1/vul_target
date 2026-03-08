package util

import (
	"strings"
	"unicode"

	"golang.org/x/text/width"
)

// ReplaceSpecialCharacters 特殊字符处理函数：大小转小写，全角转半角，去掉标点符号
func ReplaceSpecialCharacters(text string) string {
	text = replaceLineBreak(strings.ToLower(text)) // 先转小写，再去掉回车和换行
	text = width.Narrow.String(text)               // 全角转半角
	text = removeSpecialChars(text)
	return text
}

func replaceLineBreak(text string) string {
	text = strings.ReplaceAll(text, "\\r\\n", "")
	text = strings.ReplaceAll(text, "\\n", "")
	text = strings.ReplaceAll(text, "\r", "")
	text = strings.ReplaceAll(text, "\n", "")
	return text
}

func removeSpecialChars(text string) string {
	return strings.Map(func(r rune) rune {
		if isPunctuation(r) {
			return -1 // 返回 -1 表示删除该字符
		}
		return r
	}, text)
}

// isPunctuation 检查给定字符是否为标点符号
func isPunctuation(char rune) bool {
	// 检查字符是否在 Unicode 标点符号类中
	if unicode.IsPunct(char) {
		return true
	}

	// Check if the character is a non-letter/number ASCII character
	// Characters such as "^", "$", and "`" are not in the Unicode
	// Punctuation class but we treat them as punctuation anyway, for
	// consistency.
	if (char >= ' ' && char <= '/') || // ASCII 32 to 47
		(char >= ':' && char <= '@') || // ASCII 58 to 64
		(char >= '[' && char <= '`') || // ASCII 91 to 96
		(char >= '{' && char <= '~') { // ASCII 123 to 126
		return true
	}

	// Check if the character is a non-breaking space or a tab character
	if char == '\u00A0' || char == '\t' { // 不换行空格或者制表符
		return true
	}

	return false
}
