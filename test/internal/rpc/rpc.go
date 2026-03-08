package rpc

import (
	"git.woa.com/adp/common/llm/openai"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	fileManagerServer "git.woa.com/adp/pb-go/kb/parse_engine/file_manager_server"
	parseRouter "git.woa.com/adp/pb-go/kb/parse_engine/kb_parse_router"
	finance "git.woa.com/adp/pb-go/platform/platform_charger"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	dataStat "git.woa.com/adp/pb-go/platform/platform_metrology"
	resourceGallery "git.woa.com/adp/pb-go/resource_gallery/resource_gallery"
	taskFlow "git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP"
	accessManager "git.woa.com/dialogue-platform/proto/pb-stub/access-manager-server"
	embedding "git.woa.com/dialogue-platform/proto/pb-stub/embedding_server"
	"git.woa.com/dialogue-platform/proto/pb-stub/vector_db_manager"
	webParserServer "git.woa.com/dialogue-platform/proto/pb-stub/web-parser-server"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec"
	shortURL "git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/short_url"
)

var rpcInstance *RPC

type RPC struct {
	app             appconfig.AdminClientProxy
	appApi          appconfig.ApiClientProxy
	platformAdmin   pm.AdminClientProxy
	platformLogin   pm.LoginClientProxy
	platformApi     pm.ApiClientProxy
	directIndex     retrieval.DirectIndexClientProxy
	retrieval       retrieval.RetrievalClientProxy
	taskFlow        taskFlow.TaskConfigClientProxy
	embedding       embedding.EmbeddingObjClientProxy
	tDocLinker      TDocLinkerHTTP
	fileManager     fileManagerServer.ManagerObjClientProxy
	finance         finance.FinanceClientProxy
	infosec         infosec.InfosecClientProxy
	shortURLAdmin   shortURL.AdminClientProxy
	webParserCli    webParserServer.ParserObjClientProxy
	accessManager   accessManager.AccessManagerClientProxy
	resource        resourceGallery.AdminClientProxy
	aiGateway       *openai.Client
	vectorDBManager vector_db_manager.VectorObjClientProxy
	dataStat        dataStat.ApiClientProxy
	parseRouter     parseRouter.ParseRouterClientProxy

	AppAdmin             AppAdminRPC
	AppApi               AppApiRPC
	PlatformAdmin        PlatformAdminRPC
	PlatformLogin        PlatformLoginRPC
	PlatformApi          PlatformAPIRPC
	COS                  CosAccessRPC
	FileManager          FileManagerRPC
	Retrieval            RetrievalRPC
	RetrievalDirectIndex RetrievalDirectIndexRPC
	TDocLinker           TDocLinkerRPC
	ThirdDoc             ThirdDocRpc
	TaskFlow             TaskFlowRPC
	Cloud                CloudRPC
	Finance              FinanceRPC
	InfoSec              InfoSecRPC
	ShortURL             ShortURLRPC
	WebParser            WebParserRPC
	AccessManager        AccessManagerRPC
	Resource             ResourceRPC
	AIGateway            AIGatewayRPC
	VectorDBManager      VectorDBManagerRPC
	DataStat             DataStatRPC
	ParseRouter          ParseRouterRPC
}

func New(
	appCli appconfig.AdminClientProxy,
	appApi appconfig.ApiClientProxy,
	platformAdmin pm.AdminClientProxy,
	platformLogin pm.LoginClientProxy,
	platformApi pm.ApiClientProxy,
	directIndexClient retrieval.DirectIndexClientProxy,
	retrievalClient retrieval.RetrievalClientProxy,
	taskFlowClient taskFlow.TaskConfigClientProxy,
	tDocLinkerClient TDocLinkerHTTP,
	fileManager fileManagerServer.ManagerObjClientProxy,
	embeddingClient embedding.EmbeddingObjClientProxy,
	financeClient finance.FinanceClientProxy,
	infosecClient infosec.InfosecClientProxy,
	shortURLAdminClient shortURL.AdminClientProxy,
	webParserCli webParserServer.ParserObjClientProxy,
	accessManagerClient accessManager.AccessManagerClientProxy,
	resourceClient resourceGallery.AdminClientProxy,
	aiGateway *openai.Client,
	vectorDBManagerClient vector_db_manager.VectorObjClientProxy,
	dataStatClient dataStat.ApiClientProxy,
	parseRouterClient parseRouter.ParseRouterClientProxy,
) *RPC {
	rpcInstance = &RPC{
		app:             appCli,
		appApi:          appApi,
		platformAdmin:   platformAdmin,
		platformLogin:   platformLogin,
		platformApi:     platformApi,
		directIndex:     directIndexClient,
		retrieval:       retrievalClient,
		taskFlow:        taskFlowClient,
		tDocLinker:      tDocLinkerClient,
		embedding:       embeddingClient,
		fileManager:     fileManager,
		finance:         financeClient,
		infosec:         infosecClient,
		shortURLAdmin:   shortURLAdminClient,
		webParserCli:    webParserCli,
		accessManager:   accessManagerClient,
		resource:        resourceClient,
		aiGateway:       aiGateway,
		vectorDBManager: vectorDBManagerClient,
		dataStat:        dataStatClient,
		parseRouter:     parseRouterClient,
	}

	rpcInstance.AppAdmin = rpcInstance
	rpcInstance.AppApi = rpcInstance
	rpcInstance.PlatformAdmin = rpcInstance
	rpcInstance.PlatformLogin = rpcInstance
	rpcInstance.PlatformApi = rpcInstance
	rpcInstance.COS = rpcInstance
	rpcInstance.FileManager = rpcInstance
	rpcInstance.Retrieval = rpcInstance
	rpcInstance.RetrievalDirectIndex = rpcInstance
	rpcInstance.TDocLinker = rpcInstance
	rpcInstance.TaskFlow = rpcInstance
	rpcInstance.Cloud = rpcInstance
	rpcInstance.Finance = rpcInstance
	rpcInstance.InfoSec = rpcInstance
	rpcInstance.ShortURL = rpcInstance
	rpcInstance.WebParser = rpcInstance
	rpcInstance.AccessManager = rpcInstance
	rpcInstance.Resource = rpcInstance
	rpcInstance.AIGateway = rpcInstance
	rpcInstance.VectorDBManager = rpcInstance
	rpcInstance.DataStat = rpcInstance
	rpcInstance.ThirdDoc = rpcInstance
	rpcInstance.ParseRouter = rpcInstance

	return rpcInstance
}
