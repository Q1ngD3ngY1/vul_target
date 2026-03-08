package api

import (
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"github.com/google/wire"

	"git.woa.com/adp/kb/kb-config/internal/logic"
	"git.woa.com/adp/kb/kb-config/internal/service"
)

var ProviderSet = wire.NewSet(
	dao.New,
	service.New,
	rpc.ProviderSet,
	logic.ProviderSet,
)
