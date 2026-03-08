package model

import (
	"time"
)

const (
	// UserStatusValid 启用
	UserStatusValid = 1
	// UserStatusInValid 关闭
	UserStatusInValid = 0
	// UserStatusWaitAudit 待审核
	UserStatusWaitAudit = 2

	// LoginTypeTelephone 手机号登录
	LoginTypeTelephone = 0
	// LoginTypeAccount 账号密码登录
	LoginTypeAccount = 1

	// LoginUserNormalType 普通用户
	LoginUserNormalType = 0
	// LoginUserExpType 体验用户
	LoginUserExpType = 1
	// UserExpTypePrx 体验用户前缀
	UserExpTypePrx = "exp_user_"
)

// User 用户
type User struct {
	ID            uint64    `db:"id"`
	BusinessID    uint64    `db:"business_id"`     // 体验用户业务ID（只有体验用户有这个ID）
	SID           int       `db:"sid"`             // 集成商ID
	Uin           string    `db:"uin"`             // 腾讯云主账号uin
	SubAccountUin string    `db:"sub_account_uin"` // 腾讯云子账号uin
	NickName      string    `db:"nick_name"`       // 用户名称
	Avatar        string    `db:"avatar"`          // 用户头像
	Cellphone     string    `db:"cellphone"`       // 手机号
	Account       string    `db:"account"`         // 用户账户
	Password      string    `db:"password"`        // 用户密码，md5加密
	Status        int8      `db:"status"`          // 状态 (1 启用 0 关闭)
	CreateTime    time.Time `db:"create_time"`     // 创建时间
	UpdateTime    time.Time `db:"update_time"`     // 更新时间
}

// UserWithRobot 用户机器人信息
type UserWithRobot struct {
	User    *User
	RobotID uint64
}

// IsValid 用户是否有效
func (u *User) IsValid() bool {
	if u == nil {
		return false
	}
	return u.Status == UserStatusValid
}

// IsInValid 用户是否无效
func (u *User) IsInValid() bool {
	if u == nil {
		return false
	}
	return u.Status == UserStatusInValid
}

// IsWaitAudit 用户是否审核中
func (u *User) IsWaitAudit() bool {
	if u == nil {
		return false
	}
	return u.Status == UserStatusWaitAudit
}
