package entity

import (
	"time"
)

const (
	// StaffStatusInvalid 企业员工无效
	StaffStatusInvalid = 0
	// StaffStatusValid 企业员工有效
	StaffStatusValid = 1
)

var corpStaffStatusDesc = map[int8]string{
	StaffStatusInvalid: "无效",
	StaffStatusValid:   "有效",
}

// CorpStaff 企业员工
type CorpStaff struct {
	ID         uint64    `db:"id"`
	BusinessID uint64    `db:"business_id"` // 对外员工ID
	CorpID     uint64    `db:"corp_id"`     // 企业ID
	UserID     uint64    `db:"user_id"`     // 用户ID
	NickName   string    `db:"nick_name"`   // 用户名称
	Avatar     string    `db:"avatar"`      // 用户头像
	Cellphone  string    `db:"cellphone"`   // 手机号
	Status     int8      `db:"status"`      // 0无效1有效
	IsGenQA    bool      `db:"is_gen_qa"`   // 用户是否首次生成QA 1 生成过QA 0 没生成过QA
	JoinTime   time.Time `db:"join_time"`   // 加入时间
	UpdateTime time.Time `db:"update_time"` // 更新时间
	CreateTime time.Time `db:"create_time"` // 创建时间
}

// Session 存储session信息
type Session struct {
	ID            uint64 `json:"id"`
	SID           uint64 `json:"sid"`
	UIN           string `json:"uin"`
	SubAccountUin string `json:"sub_account_uin"`
	BizID         uint64 `json:"biz_id"`
	CorpID        uint64 `json:"corp_id"`
	Cellphone     string `json:"cellphone"`
	Status        int8   `json:"status"`
	ExpireTime    int64  `json:"expire_time"`
}

// IsValid 是否有效员工
func (c *CorpStaff) IsValid() bool {
	if c == nil {
		return false
	}
	return c.Status == StaffStatusValid
}

// StatusDesc 状态描述
func (c *CorpStaff) StatusDesc() string {
	if c == nil {
		return ""
	}
	return corpStaffStatusDesc[c.Status]
}

// IsEmpty 是否为空
func (s *Session) IsEmpty() bool {
	if s == nil {
		return false
	}
	return s.ID == 0
}

// GenQAFlag 是否生成QA标记
func (c *CorpStaff) GenQAFlag() bool {
	if c == nil {
		return false
	}
	return c.IsGenQA
}
