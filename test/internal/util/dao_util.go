package util

import (
	"strings"

	"git.woa.com/adp/common/x/logx"
)

var Special = strings.NewReplacer(`\`, `\\`, `_`, `\_`, `%`, `\%`, `'`, `\'`)

const (
	SqlEqual     = " = ?"
	SqlNotEqual  = " != ?"
	SqlLess      = " < ?"
	SqlLessEqual = " <= ?"
	SqlMore      = " > ?"
	SqlMoreEqual = " >= ?"
	SqlLike      = " LIKE ?"
	SqlIn        = " IN ?"
	SqlSubIn     = " IN (?)"
	SqlSubNotIn  = " NOT IN (?)"

	SqlOrderByAsc  = "ASC"
	SqlOrderByDesc = "DESC"

	// MaxTextLength utf8mb4_unicode_ci字符集TEXT类型最大长度,65535/4=16383
	MaxTextLength = 16000
	// DefaultMaxPageSize 默认分页大小
	DefaultMaxPageSize = 1000
	// MaxSqlInCount sql中in集合的最大数量，避免导致慢查询
	MaxSqlInCount = 200
	// MinBizID 最小业务ID，用来兼容业务ID和系统自增ID的场景
	MinBizID = 1000000000000000000
)

// CalculateLimit 获取分页大小
func CalculateLimit(wantedCount, alreadyGetCount int) int {
	limit := 0
	if wantedCount > 0 {
		limit = wantedCount - alreadyGetCount
		if limit > DefaultMaxPageSize {
			limit = DefaultMaxPageSize
		}
	} else {
		limit = DefaultMaxPageSize
	}
	return limit
}

func Placeholder(c int) string {
	if c <= 0 {
		logx.Errorf("invalid placeholder count: %d", c)
		return ""
	}
	return "?" + strings.Repeat(", ?", c-1)
}
