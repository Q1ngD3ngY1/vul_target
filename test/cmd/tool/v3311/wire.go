//go:build wireinject
// +build wireinject

package main

import (
	"github.com/google/wire"

	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/logic"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

//go:generate wire

var ProviderSet = wire.NewSet(
	dao.New,
	logic.ProviderSet,
	rpc.ProviderSet,
)

func newCmdService() *CmdService {
	wire.Build(New, ProviderSet)
	return &CmdService{}
}
