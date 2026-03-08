package kb

// CreateSharedKnowledgeRequest 创建共享知识库的请求参数（Service -> Logic）
type CreateSharedKnowledgeRequest struct {
	Uin                     string
	CorpBizID               uint64
	StaffBizID              uint64
	StaffPrimaryID          uint64
	SpaceID                 string
	Name                    string
	Description             string
	EmbeddingModel          string
	SharedKnowledgeAppBizID uint64 // 可选：共享知识库所在应用ID，如果传入则使用已有应用，不传则创建新应用
}

// ReferShareKnowledgeRequest 引用共享知识库的请求参数（Service -> Logic）
type ReferShareKnowledgeRequest struct {
	AppBizID        uint64
	AppPrimaryID    uint64
	KnowledgeBizIDs []uint64
	CorpPrimaryID   uint64
	CorpBizID       uint64
	SpaceID         string
	AppName         string
}
