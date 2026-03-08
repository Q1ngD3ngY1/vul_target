package entity

import (
	"time"
)

const (
	// ConfigIsReleased 配置已经发布
	ConfigIsReleased = 1
)

// RobotConfigHistory 机器人配置历史记录
type RobotConfigHistory struct {
	ID          uint64    `db:"id"`
	RobotID     uint64    `db:"robot_id"`     // 机器人(应用)id
	VersionID   uint64    `db:"version_id"`   // 发布版本id
	ReleaseJSON string    `db:"release_json"` // 发布版本对应配置json
	IsRelease   bool      `db:"is_release"`   // 是否发布
	CreateTime  time.Time `db:"create_time"`  // 创建时间
	UpdateTime  time.Time `db:"update_time"`  // 更新时间
}
