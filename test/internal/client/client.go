package client

import (
	"git.code.oa.com/trpc-go/trpc-go/http"
	"git.woa.com/adp/common/llm/openai"
	taskFlow "git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"
	retrieve "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	aiconfManager "git.woa.com/dialogue-platform/proto/pb-stub/aiconf-manager-server"
	embedding "git.woa.com/dialogue-platform/proto/pb-stub/embedding_server"
	fileManagerServer "git.woa.com/dialogue-platform/proto/pb-stub/file_manager_server"
	llmm "git.woa.com/dialogue-platform/proto/pb-stub/llm-manager-server"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/finance/finance"
)

var adminApiCli admin.ApiClientProxy
var directIndexCli retrieve.DirectIndexClientProxy
var retrievalCli retrieve.RetrievalClientProxy
var taskFlowCli taskFlow.TaskConfigClientProxy
var embeddingCli embedding.EmbeddingObjClientProxy
var tDocLinkerCli http.Client
var docParseCli fileManagerServer.ManagerObjClientProxy
var financeClientCli finance.FinanceClientProxy
var llmmCli llmm.ChatClientProxy
var aiGatewayCli *openai.Client
var aiConfMCli aiconfManager.AIConfManagerClientProxy
var statisticsApiCli statistics.ApiClientProxy

func Init(adminApiClient admin.ApiClientProxy,
	directIndexClient retrieve.DirectIndexClientProxy,
	retrievalClient retrieve.RetrievalClientProxy,
	taskFlowClient taskFlow.TaskConfigClientProxy,
	tDocLinkerClient http.Client,
	docParseClient fileManagerServer.ManagerObjClientProxy,
	financeClient finance.FinanceClientProxy,
	llmmClient llmm.ChatClientProxy) {
	adminApiCli = adminApiClient
	directIndexCli = directIndexClient
	retrievalCli = retrievalClient
	taskFlowCli = taskFlowClient
	tDocLinkerCli = tDocLinkerClient
	embeddingCli = embedding.NewEmbeddingObjClientProxy()
	docParseCli = docParseClient
	financeClientCli = financeClient
	llmmCli = llmmClient

	aiGatewayCli = openai.NewClient(
		openai.WithRetryTimes(2),
		openai.WithHTTPClientProxy(http.NewClientProxy("trpc.adp.ai-gateway.api")),
	)
	aiConfMCli = aiconfManager.NewAIConfManagerClientProxy()
	statisticsApiCli = statistics.NewApiClientProxy()
}
