package main

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-database/timer"
	"git.code.oa.com/trpc-go/trpc-go/admin"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.code.oa.com/trpc-go/trpc-go/server"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/timerx"
	"git.woa.com/adp/common/x/trpcx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	_ "git.woa.com/adp/kb/kb-config/internal/dao/segment"
	"git.woa.com/adp/kb/kb-config/internal/service"
	"git.woa.com/adp/kb/kb-config/internal/service/api"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

var (
	// trpc.adp.kb_config.Admin
	adminServiceName = fmt.Sprintf("trpc.adp.%s.Admin", trpcx.DungeonForServer("kb_config"))

	// trpc.adp.kb_config.Api
	apiServiceName = fmt.Sprintf("trpc.adp.%s.Api", trpcx.DungeonForServer("kb_config"))
)

func main() {
	srv := trpcx.NewServer()

	// 监听应用配置
	_ = config.Watch()

	// 初始化业务 service
	adminService := newService()
	apiService := newAPI()

	pb.RegisterAdminService(srv.Service(adminServiceName), adminService)
	pb.RegisterApiService(srv.Service(apiServiceName), apiService)

	// 注册任务，并开启任务调度
	_ = initAsync()

	rpcInstance = newRPC()

	// 定时任务
	registerTimer(srv.Server, adminService, apiService)

	// 应用的检索配置从DB同步到redis
	// TODO(ericjwang): 确认下，这个应该删除
	admin.HandleFunc("/app/SyncRetrievalConfigFromDB", adminService.SyncRetrievalConfigFromDB)

	// 检查角色业务资源 (供CAM调用)
	// [NOTICE] 部署要求: 仅内网访问, 固定URI, HTTP协议, 非鉴权接口
	// [NOTICE] 变更要求: 变更调整需要提前与CAM同步
	admin.HandleFunc("/cam/CheckResourceByRoleName", adminService.CheckResourceByRoleName)

	if err := srv.Serve(); err != nil {
		panic(err)
	}
}

// registerTimer 注册定时任务
func registerTimer(srv *server.Server, svc *service.Service, api *api.Service) {
	// 自定义 serviceName，本质上这是 redis 中的 key，加个前缀避免冲突。重构过程中之所以这样写，是因为早期就已经是这样的，为了兼容
	serviceNamer := func(originServiceName string) string {
		return fmt.Sprintf("lke:knowledge:scheduler:%s", originServiceName)
	}

	noopHandler := func(ctx context.Context) error {
		logx.I(ctx, "noopHandler called in worker mode, do nothing")
		return nil
	}
	timers := map[string]func(context.Context) error{
		"QASimilarTask":                    svc.QASimilarTaskHandler,              // QASimilarTask
		"DeleteCharSizeExceededTask":       svc.DeleteCharSizeExceededTaskHandler, // DeleteCharSizeExceededTask
		"CleanVectorSyncHistoryTask":       svc.CleanVectorSyncHistory,            // CleanVectorSyncHistoryTask
		"UpdateAttributeLabelsTaskPreview": svc.UpdateAttributeLabelsTaskPreview,  // 刷新评测环境属性&标签缓存
		"UpdateAttributeLabelsTaskProd":    svc.UpdateAttributeLabelsTaskProd,     // 刷新发布环境属性&标签缓存
		"CleanDatabaseCommonData":          svc.CleanDatabaseCommonData,           // 定时清理已删除的数据
		"AutoDocRefresh":                   svc.AutoDocRefresh,                    // 定时刷新文档数据
		"UnfinishedDocParseRefresh":        api.UnfinishedDocParseRefresh,         // 未完成的文档解析刷新任务
	}
	for name, handler := range timers {
		// 注册 scheduler
		timerx.RegisterRedisScheduler(name, svc.AdminRdb, timerx.WithServiceNamer(serviceNamer))

		// 注册 handler
		// 如果开启了 worker 模式，就注册个空的 handler，避免定时任务被执行
		serviceName := fmt.Sprintf("trpc.adp.kb_config.%s", name)
		timerService := srv.Service(serviceName)
		if timerService == nil {
			log.Infof("registerTimer: service %s not found, skip register timer", serviceName)
			continue
		}
		if config.GetMainConfig().EnableWorkerMode {
			timer.RegisterHandlerService(timerService, noopHandler)
		} else {
			timer.RegisterHandlerService(timerService, handler)
		}
	}

	// 如果开启了 worker 模式，就不执行异步任务
	if config.GetMainConfig().EnableWorkerMode {
		return
	}

	go svc.GetVectorLogic().DoSync()
}
