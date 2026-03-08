package model

// RobotRole 机器人角色及描述信息
type RobotRole struct {
	ID          uint64 `db:"id"`          // 角色描述id
	BusinessID  uint64 `db:"business_id"` // 业务ID
	Icon        string `db:"icon"`        // 角色icon
	Name        string `db:"name"`        // 角色名
	Description string `db:"description"` // 角色对应描述(prompt 场景使用)
}
