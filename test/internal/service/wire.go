package service

import (
	"github.com/google/wire"

	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/logic"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

var ProviderSet = wire.NewSet(
	dao.New,
	logic.ProviderSet,
	rpc.ProviderSet,
)
