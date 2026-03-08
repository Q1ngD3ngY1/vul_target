package app

import (
	"context"

	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// CheckApp 检查应用状态
func (l *Logic) CheckApp(ctx context.Context, robotBizIdStr string) (*entity.App, error) {
	// 检查应用状态
	robotBizID, err := util.CheckReqBotBizIDUint64(ctx, robotBizIdStr)
	if err != nil {
		return nil, err
	}
	app, err := GetAppByAppBizID(ctx, l.r, robotBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	return app, nil
}
