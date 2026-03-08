// KEP.bot-knowledge-config-server
//
// @(#)main.go  March 27, 2024
// Copyright(c) 2024, halelv@Tencent. All rights reserved.

package main

import (
	"math/rand"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.code.oa.com/trpc-go/trpc-go/server"
	_ "git.code.oa.com/trpc-go/trpc-metrics-prometheus"
	_ "git.woa.com/adp/common/x/trpcx/filters/env_propagator"
	_ "git.woa.com/adp/common/x/trpcx/filters/i18n"
	_ "git.woa.com/adp/common/x/trpcx/filters/permission"
	"git.woa.com/baicaoyuan/moss"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/registry"
	"git.woa.com/dialogue-platform/go-comm/clues"
	"git.woa.com/dialogue-platform/go-comm/encode"
	"git.woa.com/dialogue-platform/go-comm/panicprinter"
	"git.woa.com/dialogue-platform/go-comm/runtime0"
	_ "git.woa.com/galileo/trpc-go-galileo"
	"gopkg.in/yaml.v3"

	// 这一行是为了防止企业版和元器trpc命名空间重合，永远放在import的最后一行
	_ "git.woa.com/dialogue-platform/yuanqi/yuanqi-naming-polaris"
)

const (
	// 编译版本号, 流水线会注入相关信息
	buildVersion = "服务编译版本号, 勿动~~"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	clues.Init()
}

func main() {
	defer panicprinter.PrintPanic()
	runtime0.SetServerVersion(buildVersion)
	runtime0.PrintVersion()

	// 服务注册
	srv := moss.NewServer(
		moss.WithRegistry(registry.New()),
		moss.WithTrpcServerOptions(server.WithFilter(pkg.LogFilter)),
	)

	// log
	cfg := trpc.GlobalConfig()
	log.Info("\n-------------------------------------------------------------------------------")
	g0, _ := yaml.Marshal(cfg.Global)
	log.Infof("\nGlobal:\n%v", encode.String(g0))
	log.Info("\n-------------------------------------------------------------------------------")
	s0, _ := yaml.Marshal(cfg.Server)
	log.Infof("\nServer:\n%v", encode.String(s0))
	log.Info("\n-------------------------------------------------------------------------------")
	c0, _ := yaml.Marshal(cfg.Client)
	log.Infof("\nClient:\n%v", encode.String(c0))
	log.Info("\n-------------------------------------------------------------------------------")
	p0, _ := yaml.Marshal(cfg.Plugins)
	log.Infof("\nPlugins:\n%v", encode.String(p0))
	log.Info("\n===============================================================================")

	redis.Init()

	if err := srv.Serve(); err != nil {
		log.Fatal(err)
	}
}
