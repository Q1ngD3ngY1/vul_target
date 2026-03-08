package model

import "time"

const (
	// DefaultIntentPolicyID 通用策略ID
	DefaultIntentPolicyID uint32 = 1
	// IntentPolicyIsValid 策略未删除
	IntentPolicyIsValid = 0
	// IntentPolicyIsInvalid 策略已删除
	IntentPolicyIsInvalid = 1
	// IntentPolicyIsUsed 策略使用中
	IntentPolicyIsUsed = 1
	// IntentPolicyNotUsed 策略未使用
	IntentPolicyNotUsed = 0
)

// IntentPolicy 意图策略
type IntentPolicy struct {
	ID         uint32    `db:"id"`
	Name       string    `db:"name"`        // 策略名称
	IsDeleted  int8      `db:"is_deleted"`  // 是否删除
	IsUsed     int8      `db:"is_used"`     // 是否使用
	Operator   string    `db:"operator"`    // 数据操作人
	UpdateTime time.Time `db:"update_time"` // 更新时间
	CreateTime time.Time `db:"create_time"` // 创建时间
}

// ListIntentPolicyReq 获取意图列表请求
type ListIntentPolicyReq struct {
	Name     string // 意图名称
	Page     uint32 // 页码
	PageSize uint32 // 页面大小
}
