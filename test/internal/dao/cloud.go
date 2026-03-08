package dao

import (
	"context"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/cloud"
	cloudModel "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/cloud"
)

// DescribeNickname 获取昵称
func (d *dao) DescribeNickname(ctx context.Context, uin, subAccountUin string) (*cloudModel.NicknameInfo, error) {
	return cloud.DescribeNickname(ctx, uin, subAccountUin)
}

// BatchCheckWhitelist 批量检查白名单
func (d *dao) BatchCheckWhitelist(ctx context.Context, key, uin string) (bool, error) {
	return cloud.BatchCheckWhitelist(ctx, key, uin)
}
