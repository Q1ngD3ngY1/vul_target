package model

import "time"

const (
	// CorpStatusInValid 企业失效
	CorpStatusInValid = 0
	// CorpStatusValid 启用
	CorpStatusValid = 1
	// CorpStatusInAudit 审核中
	CorpStatusInAudit = 2
	// CorpStatusAuditFail 审核未通过
	CorpStatusAuditFail = 3

	// IsTrialNo 非试用期
	IsTrialNo = 0
	// IsTrialYes 是试用期
	IsTrialYes = 1
)

var corpStatusDesc = map[int8]string{
	CorpStatusInValid:   "无效",
	CorpStatusValid:     "有效",
	CorpStatusInAudit:   "审核中",
	CorpStatusAuditFail: "审核未通过",
}

// Corp 企业
type Corp struct {
	ID             uint64    `db:"id"`
	SID            int       `db:"sid"`              // 集成商ID
	BusinessID     uint64    `db:"business_id"`      // 对外业务ID
	Uin            string    `db:"uin"`              // 腾讯云uin
	FullName       string    `db:"full_name"`        // 企业全称
	CreateUserID   uint64    `db:"create_user_id"`   // 创建用户ID
	Cellphone      string    `db:"cellphone"`        // 联系人手机号
	RobotQuota     uint32    `db:"robot_quota"`      // 机器人配额
	ContactName    string    `db:"contact_name"`     // 联系人姓名
	Email          string    `db:"email"`            // 联系人邮箱
	Status         int8      `db:"status"`           // 状态 (1 启用 0 关闭)
	IsTrial        int8      `db:"is_trial"`         // 是否试用 0 否 1 是
	MaxCharSize    uint64    `db:"max_char_size"`    // 机器人最大字符数
	MaxTokenUsage  uint64    `db:"max_token_usage"`  // 机器人最大token数
	TrialStartTime time.Time `db:"trial_start_time"` // 试用开始时间
	TrialEndTime   time.Time `db:"trial_end_time"`   // 试用结束时间
	CreateTime     time.Time `db:"create_time"`      // 创建时间
	UpdateTime     time.Time `db:"update_time"`      // 更新时间
}

// IsInAudit 是否在审核中
func (c *Corp) IsInAudit() bool {
	if c == nil {
		return false
	}
	return c.Status == CorpStatusInAudit
}

// IsValid 是否有效
func (c *Corp) IsValid() bool {
	if c == nil {
		return false
	}
	return c.Status == CorpStatusValid
}

// StatusDesc 企业状态描述
func (c *Corp) StatusDesc() string {
	if c == nil {
		return ""
	}
	return corpStatusDesc[c.Status]
}

// IsCorpTrial 是否是试用
func (c *Corp) IsCorpTrial() bool {
	if c == nil {
		return false
	}
	return c.IsTrial == IsTrialYes
}

// IsCorpTrialExpired 是否试用已过期
func (c *Corp) IsCorpTrialExpired() bool {
	if c == nil {
		return false
	}
	if !c.IsCorpTrial() {
		return false
	}
	return time.Now().After(c.TrialEndTime)
}

// IsCharSizeExceeded 校验字符大小限制
func (c *Corp) IsCharSizeExceeded(usedCharSize, diff int64) bool {
	if c == nil {
		return true
	}
	return diff > 0 && usedCharSize+diff > int64(c.MaxCharSize)
}

// IsUsedCharSizeExceeded 校验字符使用量是否已经超过限制
func (c *Corp) IsUsedCharSizeExceeded(usedCharSize int64) bool {
	if c == nil {
		return true
	}
	return usedCharSize > int64(c.MaxCharSize)
}

// IsUsedTokenUsageExceeded 校验token使用量是否已经超过限制
func (c *Corp) IsUsedTokenUsageExceeded(tokenUsage uint64) bool {
	if c == nil {
		return true
	}
	return tokenUsage > c.MaxTokenUsage
}
