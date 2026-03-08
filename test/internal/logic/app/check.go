package app

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
)

// CheckApp 检查应用状态
func CheckApp(ctx context.Context, db dao.Dao, robotBizIdStr string) (*model.App, error) {
	// 检查应用状态
	robotBizID, err := util.CheckReqBotBizIDUint64(ctx, robotBizIdStr)
	if err != nil {
		return nil, err
	}
	app, err := GetAppByAppBizID(ctx, db, robotBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	return app, nil
}
