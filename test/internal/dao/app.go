package dao

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
)

// GetBotBizIDByID 获取应用ID
func (d *dao) GetBotBizIDByID(ctx context.Context, id uint64) (uint64, error) {
	key := fmt.Sprintf("bot_id:bot_biz_id:%d", id)
	// botBizID, err := redis.Uint64(d.RedisCli().Do(ctx, "GET", key))
	botBizID, err := d.adminRdb.Get(ctx, key).Uint64()
	if err == nil {
		return botBizID, nil
	}
	app, err := d.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, id)
	if err != nil {
		return 0, err
	}
	if err := d.adminRdb.Set(ctx, key, app.BizId, time.Hour*24*30).Err(); err != nil {
		logx.W(ctx, "GetBotBizIDByID|Set cache error:%v", err)
	}
	return app.BizId, nil
}
