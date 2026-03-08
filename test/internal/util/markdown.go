package util

import (
	"regexp"
	"strings"
)

var tableLineRegex = regexp.MustCompile(`^\s*\|.*\|\s*$`)
var separatorLineRegex = regexp.MustCompile(`^\s*\|?[\s:-]+\|([\s|:-]*)$`)

// IsTableLine 判断一行是否为表格行
func IsTableLine(line string) bool {
	// 至少有两个 |，且不是全是空格
	return tableLineRegex.MatchString(line) && len(strings.TrimSpace(line)) > 2
}

// IsSeparatorLine 判断是否为分隔线
func IsSeparatorLine(line string) bool {
	// 形如 | --- | --- | 或 |:---|:---:|---:|
	return separatorLineRegex.MatchString(line)
}
