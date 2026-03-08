package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	roleFields = `
		id,business_id,icon,name,description
	`
	getRoleByRobotID = `
		SELECT
			%s
		FROM
			t_robot_role
	`
)

// GetListRole 获取机器人角色配置
func (d *dao) GetListRole(ctx context.Context) ([]*model.RobotRole, error) {
	role := make([]*model.RobotRole, 0)
	SQL := fmt.Sprintf(getRoleByRobotID, roleFields)
	if err := d.db.QueryToStructs(ctx, &role, SQL); err != nil {
		if mysql.IsNoRowsError(err) {
			return nil, nil
		}
	}
	return role, nil
}
