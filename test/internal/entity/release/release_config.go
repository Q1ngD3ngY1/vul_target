package release

import (
	"time"
)

// ReleaseConfig 发布配置表
type ReleaseConfig struct {
	ID            uint64    `db:"id"`
	CorpID        uint64    `db:"corp_id"`
	StaffID       uint64    `db:"staff_id"`
	RobotID       uint64    `db:"robot_id"`
	VersionID     uint64    `db:"version_id"`
	ConfigItem    string    `db:"config_item"`
	OldValue      string    `db:"old_value"`
	Value         string    `db:"value"`
	Content       string    `db:"content"`
	Action        uint32    `db:"action"`
	ReleaseStatus uint32    `db:"release_status"`
	Message       string    `db:"message"`
	AuditStatus   uint32    `db:"audit_status"`
	AuditResult   string    `db:"audit_result"`
	UpdateTime    time.Time `db:"update_time"`
	CreateTime    time.Time `db:"create_time"`
	ExpireTime    time.Time `db:"expire_time"`
}

// AuditReleaseConfig 待审核的发布配置
type AuditReleaseConfig struct {
	ID          uint64 `db:"id"`
	ConfigItem  string `db:"config_item"`  // 配置项目
	Value       string `db:"value"`        // 配置内容
	VersionID   uint64 `db:"version_id"`   // 发布版本
	AuditStatus uint32 `db:"audit_status"` // 审核状态
}

type ListReleaseConfigReq struct {
	VersionID uint64
	Page      uint32
	PageSize  uint32
	Query     string
	Status    []uint32
}
