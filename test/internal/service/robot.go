package service

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
)

func (s *Service) getLoginUinAndSubAccountUin(ctx context.Context) (string, string) {
	uin := pkg.LoginUin(ctx)
	subAccountUin := pkg.LoginSubAccountUin(ctx)
	if pkg.SID(ctx) == model.CloudSID {
		uin = pkg.Uin(ctx)
		subAccountUin = pkg.SubAccountUin(ctx)
	}
	return uin, subAccountUin
}
