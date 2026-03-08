package search

import (
	"git.woa.com/adp/pb-go/common"
	knowledgeConfig "git.woa.com/adp/pb-go/kb/kb_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

type FuncType uint32

const (
	EnumFuncTypeSearchKG      FuncType = 1
	EnumFuncTypeSearchBatchKG FuncType = 2
)

type EnumSearchScope uint32 // 搜索范围，

const (
	EnumSearchScopeAll        EnumSearchScope = 0 // 全量搜索
	EnumSearchScopeQA         EnumSearchScope = 1 // 搜索QA
	EnumSearchScopeSegment    EnumSearchScope = 2 // 搜索文档
	EnumSearchScopeQAPriority EnumSearchScope = 3 // QA>0.97
)

const (
	EnumScenarioTypeKnowledgeQA           = "KnowledgeQA"           // 对话API
	EnumScenarioTypeKnowledgeQADialogTest = "KnowledgeQADialogTest" // 对话测试
	EnumScenarioTypeEvaluateTest          = "EvaluateTest"          // 应用评测
	EnumScenarioTypeKnowledgeQAUser       = "KnowledgeQAUser"       // 渠道/体验用户端
)

// RetrievalKGConfig 检索知识库配置
type RetrievalKGConfig struct {
	KnowledgeID        uint64 // 知识库自增ID
	KnowledgeBizID     uint64 // 知识库ID
	KnowledgeName      string // 知识库名称
	IsShareKG          bool   // 是否共享知识库【共享知识库只存在评测库，默认知识库有评测和发布】
	Labels             []*knowledgeConfig.VectorLabel
	EmbeddingVersion   uint64                     // embedding版本【评测库使用】
	EmbeddingModelName string                     // embedding模型名称
	Rerank             *retrieval.Rerank          // rerank模型
	QAVersion          uint64                     // 问答库版本，需要用问答版本到overlap层取embedding版本【发布库用】
	WorkflowKGCfg      *pb.WorkflowKnowledgeParam // 工作流独立配置
	FilterKey          string
	Filters            []*retrieval.SearchFilter // 每个知识库自己的filters
	SearchStrategy     *retrieval.SearchStrategy // 检索策略
}

// RetrievalRsp 检索返回的结果
type RetrievalRsp struct {
	ReleaseRsp *retrieval.SearchRsp
	PreviewRsp *retrieval.SearchVectorRsp
}

// BillingInfo 计费上报相关信息
type BillingInfo struct {
	NeedReportEmbeddingAndRerank bool              // 是否需要上报embedding和rerank
	ModelBillingStatus           map[string]bool   // embedding和rerank的模型的计费状态，false:不可用，true:可用
	BillingTags                  map[string]string // 计费标签
	CallSource                   common.CallSource // 调用来源
	FinanceType                  string            // 计费场景
	SubBizType                   string            // 子业务类型
}
