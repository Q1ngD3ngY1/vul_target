package search

import (
	knowledgeConfig "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
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
