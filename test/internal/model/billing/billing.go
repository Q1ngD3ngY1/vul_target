// Package billing TODO
// @Author: halelv
// @Date: 2024/6/3 14:46
package billing

import "time"

const (
	// BizType 业务类型
	BizType uint32 = 1

	// CorpType 企业类型
	CorpType uint32 = 1
	// UinType uin类型
	UinType uint32 = 2
)

// TokenDosage 计费Token用量 -- copy from chat
type TokenDosage struct {
	AppID         uint64
	AppType       string
	ModelName     string    // 模型标识
	AliasName     string    //模型别名
	RecordID      string    // 对应 query 的 record id
	StartTime     time.Time // 用量实际发生开始时间 业务侧定义
	EndTime       time.Time // 用量实际发生结束时间 业务侧定义
	InputDosages  []int     // 输入用量详情信息
	OutputDosages []int     // 输出用量详情信息
}
