package model

// FlushKnowledgeBaseDataReq 刷新知识库数据请求
type FlushKnowledgeBaseDataReq struct {
	StartID uint64 `json:"start_id"` // 起始ID
	EndID   uint64 `json:"end_id"`   // 结束ID
}
