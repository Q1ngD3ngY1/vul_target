package entity

import (
	"time"

	"golang.org/x/exp/slices"
)

const (
	// SystemIntegratorValid 有效集成商状态
	SystemIntegratorValid = 1
	// SystemIntegratorInvalid 无效集成商状态
	SystemIntegratorInvalid = 0

	// CloudSID 腾讯云直客集成商ID
	CloudSID = 1
	// CloudSIUin 腾讯云直客主账号
	CloudSIUin = ""
	// CloudSISubAccountUin 腾讯云直客子账号
	CloudSISubAccountUin = ""
)

// SystemIntegrator 集成商
type SystemIntegrator struct {
	ID               uint64    `db:"id"`
	Name             string    `db:"name"`               // 集成商名称
	Status           uint32    `db:"status"`             // 集成商状态
	Uin              string    `db:"uin"`                // 集成商主账号
	SubAccountUin    string    `db:"sub_account_uin"`    // 集成商子账号
	IsSelfPermission bool      `db:"is_self_permission"` // 是否集成商自己管理权限
	AllowAction      []string  `db:"allow_action"`       // 集成商允许的操作
	DenyAction       []string  `db:"deny_action"`        // 集成商禁止的操作
	CorpAppQuota     uint32    `db:"corp_app_quota"`     // 企业机器人数量
	UpdateTime       time.Time `db:"update_time"`        // 更新时间
	CreateTime       time.Time `db:"create_time"`        // 创建时间
}

// IsValid 是否有效集成商
func (si *SystemIntegrator) IsValid() bool {
	return si.Status == SystemIntegratorValid
}

// IsCloudSI 是否云集成商
func (si *SystemIntegrator) IsCloudSI() bool {
	return si.ID == CloudSID
}

// IsSelfManagePermission 是否自己管理权限
func (si *SystemIntegrator) IsSelfManagePermission() bool {
	return si.IsSelfPermission
}

// IsAllowAction 是否允许操作
func (si *SystemIntegrator) IsAllowAction(action string) bool {
	if len(si.AllowAction) == 0 {
		return false
	}
	if !slices.Contains(si.AllowAction, "*") && !slices.Contains(si.AllowAction, action) {
		return false
	}
	if len(si.DenyAction) == 0 {
		return true
	}
	if slices.Contains(si.DenyAction, action) {
		return false
	}
	return true
}
