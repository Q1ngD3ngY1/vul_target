package rpc

import (
	"git.code.oa.com/trpc-go/trpc-go/http"
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
	vectorDbManager "git.woa.com/dialogue-platform/proto/pb-stub/vector_db_manager"
	webParserServer "git.woa.com/dialogue-platform/proto/pb-stub/web-parser-server"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec"
	shortURL "git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/short_url"
	"github.com/google/wire"
)

type TDocLinkerHTTP http.Client

var ProviderSet = wire.NewSet(
	New,
	wire.InterfaceValue(new(appconfig.AdminClientProxy), appconfig.NewAdminClientProxy()),
	wire.InterfaceValue(new(appconfig.ApiClientProxy), appconfig.NewApiClientProxy()),
	wire.InterfaceValue(new(pm.AdminClientProxy), pm.NewAdminClientProxy()),
	wire.InterfaceValue(new(pm.LoginClientProxy), pm.NewLoginClientProxy()),
	wire.InterfaceValue(new(pm.ApiClientProxy), pm.NewApiClientProxy()),
	wire.InterfaceValue(new(retrieval.DirectIndexClientProxy), retrieval.NewDirectIndexClientProxy()),
	wire.InterfaceValue(new(retrieval.RetrievalClientProxy), retrieval.NewRetrievalClientProxy()),
	wire.InterfaceValue(new(taskFlow.TaskConfigClientProxy), taskFlow.NewTaskConfigClientProxy()),
	wire.InterfaceValue(new(embedding.EmbeddingObjClientProxy), embedding.NewEmbeddingObjClientProxy()),
	wire.InterfaceValue(new(TDocLinkerHTTP), http.NewClientProxy("lke-code-node-tdoclinker")),
	wire.InterfaceValue(new(fileManagerServer.ManagerObjClientProxy), fileManagerServer.NewManagerObjClientProxy()),
	wire.InterfaceValue(new(finance.FinanceClientProxy), finance.NewFinanceClientProxy()),
	wire.InterfaceValue(new(infosec.InfosecClientProxy), infosec.NewInfosecClientProxy()),
	wire.InterfaceValue(new(shortURL.AdminClientProxy), shortURL.NewAdminClientProxy()),
	wire.InterfaceValue(new(webParserServer.ParserObjClientProxy), webParserServer.NewParserObjClientProxy()),
	wire.InterfaceValue(new(accessManager.AccessManagerClientProxy), accessManager.NewAccessManagerClientProxy()),
	wire.InterfaceValue(new(resourceGallery.AdminClientProxy), resourceGallery.NewAdminClientProxy()),
	NewOpenAiClient,
	wire.InterfaceValue(new(vectorDbManager.VectorObjClientProxy), vectorDbManager.NewVectorObjClientProxy()),
	wire.InterfaceValue(new(dataStat.ApiClientProxy), dataStat.NewApiClientProxy()),
	wire.InterfaceValue(new(parseRouter.ParseRouterClientProxy), parseRouter.NewParseRouterClientProxy()),
)

func NewOpenAiClient() *openai.Client {
	return openai.NewClient(
		openai.WithRetryTimes(2),
		openai.WithHTTPClientProxy(http.NewClientProxy("trpc.adp.ai-gateway.api")),
	)
}
