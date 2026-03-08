//go:build wireinject
// +build wireinject

package main

import (
	"github.com/google/wire"

	"git.woa.com/adp/kb/kb-config/internal/async"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/logic"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/service"
	"git.woa.com/adp/kb/kb-config/internal/service/api"
)

//go:generate wire

func newService() *service.Service {
	wire.Build(service.New, service.ProviderSet)
	return &service.Service{}
}

func newAPI() *api.Service {
	wire.Build(api.New, api.ProviderSet)
	return &api.Service{}
}

func initAsync() error {
	wire.Build(async.Init, dao.New, rpc.ProviderSet, logic.ProviderSet)
	return nil
}

func newRPC() *rpc.RPC {
	wire.Build(rpc.ProviderSet)
	return nil
}
