// Package registry 是用来做 service 注册和解注册的
// 把注册单独提出来写，方便类似于私有化部署等需要服务合并的场景
package registry

import (
	"fmt"

	"git.code.oa.com/trpc-go/trpc-database/timer"
	"git.code.oa.com/trpc-go/trpc-go/admin"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.code.oa.com/trpc-go/trpc-go/server"
	"git.woa.com/baicaoyuan/moss"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service/api"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/task"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/vector"
	configCfg "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/go-comm/trpc0"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"

	_ "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/task" // task TODO
)

var (
	// trpc service: trpc.KEP.bot-knowledge-config-server.Admin
	knowledgeAdminService = fmt.Sprintf("trpc.KEP.%s.Admin", trpc0.DungeonForServer("bot-knowledge-config-server"))
	// trpc service: trpc.KEP.bot-knowledge-config-server.Api
	knowledgeApiService = fmt.Sprintf("trpc.KEP.%s.Api", trpc0.DungeonForServer("bot-knowledge-config-server"))
	// trpc service: trpc.KEP.bot-knowledge-config-server.knowledge-config
	knowledgeConfigService = fmt.Sprintf("trpc.KEP.%s.knowledge-config", trpc0.DungeonForServer(
		"bot-knowledge-config-server"))
)

// service 注册器
type registry struct {
	s *service.Service
	i *api.Service
}

// New 新建注册器
func New() moss.Registry {
	return &registry{}
}

// Register 注册服务
func (r *registry) Register(srv *server.Server) error {
	// 配置加载
	configCfg.Init()

	// 监听应用配置
	_ = config.Watch()

	// 初始化配置 file
	dao.InitFileConfig()

	// 初始化业务 service
	r.s = service.New()
	r.i = api.New()

	// 注册任务，并开启任务调度
	task.Init()
	if err := dao.RunTask(); err != nil {
		log.Errorf("run task fail, err: %v", err)
		return err
	}

	// admin 管理后台
	pb.RegisterAdminService(srv.Service(knowledgeAdminService), r.s)
	// api 接口服务
	pb.RegisterApiService(srv.Service(knowledgeApiService), r.i)
	// config 服务
	// pb.RegisterKnowledgeConfigService(srv.Service(knowledgeConfigService), &configService.KnowledgeConfigImp{})

	app.New()

	if !configCfg.GetMainConfig().EnableWorkerMode {
		// 是否开启定时任务
		timerRegister(srv)

		vSync := vector.NewVectorSync(r.s.GetDB(), r.s.GetTdSqlDB())
		vSync.SetGetBotBizIDByIDFunc(dao.New().GetBotBizIDByID)
		go vSync.DoSync()
	}
	// 创建向量库
	admin.HandleFunc("/corp/CreateVectorIndex", r.s.CreateVectorIndex)
	// 重建发布向量库
	admin.HandleFunc("/corp/RebuildVectorIndex", r.s.RebuildVectorIndex)
	// 初始化向量数据
	admin.HandleFunc("/corp/CreateVector", r.s.CreateVector)
	// 脚本 执行后删除
	admin.HandleFunc("/ReleaseDocRebuild", r.s.ReleaseDocRebuild)
	// 创建全局知识库
	admin.HandleFunc("/init/global_knowledge", r.i.InitGlobalKnowledge)
	admin.HandleFunc("/permission", r.s.UserPermission)
	admin.HandleFunc("/verifyPermission", r.s.VerifyUserPermission)
	admin.HandleFunc("/resource", r.s.UserResource)
	admin.HandleFunc("/describeNickname", r.s.DescribeNickname)
	// 同步知识库问答应用数据到app
	admin.HandleFunc("/app/SyncKnowledgeQaAppData", r.s.SyncKnowledgeQaAppData)
	// 初始化同步存量机器人的检索配置
	admin.HandleFunc("/app/SyncRobotRetrievalConfig", r.s.SyncRobotRetrievalConfig)
	// 应用的检索配置从DB同步到redis
	admin.HandleFunc("/app/SyncRetrievalConfigFromDB", r.s.SyncRetrievalConfigFromDB)
	// 刷新知识库问答应用配置数据
	admin.HandleFunc("/app/FlushKnowledgeQaAppConfig", r.s.FlushKnowledgeQaAppConfig)
	admin.HandleFunc("/batchCheckWhitelist", r.s.BatchCheckWhitelist)
	admin.HandleFunc("/modifyAppTokenUsage", r.s.ModifyAppTokenUsage)
	// 应用通知
	admin.HandleFunc("/app/SendAppNotice", r.s.SendAppNotice)
	// 相似问刷数据，db已删除，但是向量未删除，刷向量库 这是一次性的代码，后续应该删掉
	admin.HandleFunc("/app/SyncDeletedSimilarQuestion", r.s.SyncDeletedSimilarQuestion)
	// v3.0.5 刷新共享知识库资源权限
	admin.HandleFunc("/kb/FlushShareKbUserResourcePermission", r.s.FlushShareKbUserResourcePermission)
	// 检查角色业务资源
	admin.HandleFunc("/cam/CheckResourceByRoleName", r.s.CheckResourceByRoleName)
	
	return nil
}

// timerRegister 注册定时任务
func timerRegister(srv *server.Server) {
	// 注册 QASimilarTask 任务
	timer.RegisterScheduler("QASimilarTask", service.NewScheduler())
	timer.RegisterHandlerService(srv.Service("trpc.KEP.bot-knowledge-config-server.QASimilarTask"), service.New().
		QASimilarTaskHandler)

	// 注册 TMsgDataCount 数据统计任务
	timer.RegisterScheduler("TMsgDataCount", service.NewScheduler())
	timer.RegisterHandlerService(srv.Service("trpc.KEP.bot-knowledge-config-server.TMsgDataCount"),
		service.New().TMsgDataCount)

	// 注册DeleteCharSizeExceededTask任务
	timer.RegisterScheduler("DeleteCharSizeExceededTask", service.NewScheduler())
	timer.RegisterHandlerService(
		srv.Service("trpc.KEP.bot-knowledge-config-server.DeleteCharSizeExceededTask"), service.New().DeleteCharSizeExceededTaskHandler,
	)

	// 注册CleanVectorSyncHistoryTask任务
	timer.RegisterScheduler("CleanVectorSyncHistoryTask", service.NewScheduler())
	timer.RegisterHandlerService(
		srv.Service("trpc.KEP.bot-knowledge-config-server.CleanVectorSyncHistoryTask"),
		service.New().CleanVectorSyncHistory,
	)

	// 注册刷新评测环境属性&标签缓存的任务
	timer.RegisterScheduler("UpdateAttributeLabelsTaskPreview", service.NewScheduler())
	timer.RegisterHandlerService(
		srv.Service("trpc.KEP.bot-knowledge-config-server.UpdateAttributeLabelsTaskPreview"), service.New().UpdateAttributeLabelsTaskPreview,
	)

	// 注册刷新发布环境属性&标签缓存的任务
	timer.RegisterScheduler("UpdateAttributeLabelsTaskProd", service.NewScheduler())
	timer.RegisterHandlerService(
		srv.Service("trpc.KEP.bot-knowledge-config-server.UpdateAttributeLabelsTaskProd"), service.New().UpdateAttributeLabelsTaskProd,
	)

	// 注册定时清理已删除的数据任务
	timer.RegisterScheduler("CleanDatabaseCommonData", service.NewScheduler())
	timer.RegisterHandlerService(
		srv.Service("trpc.KEP.bot-knowledge-config-server.CleanDatabaseCommonData"), service.New().CleanDatabaseCommonData,
	)

	// 注册定时刷新文档数据任务
	timer.RegisterScheduler("AutoDocRefresh", service.NewScheduler())
	timer.RegisterHandlerService(
		srv.Service("trpc.KEP.bot-knowledge-config-server.AutoDocRefresh"), service.New().AutoDocRefresh,
	)

}

// Deregister 解注册
func (r *registry) Deregister() {

}
