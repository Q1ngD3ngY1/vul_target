package finance

import (
	"time"

	"git.woa.com/adp/kb/kb-config/internal/entity"
)

// TokenDosage 计费Token用量 -- copy from chat
type TokenDosage struct {
	AppID           uint64
	AppType         string
	ModelName       string            // 模型标识
	AliasName       string            // 模型别名
	RecordID        string            // 对应 query 的 record id
	StartTime       time.Time         // 用量实际发生开始时间 业务侧定义
	EndTime         time.Time         // 用量实际发生结束时间 业务侧定义
	InputDosages    []int             // 输入用量详情信息
	OutputDosages   []int             // 输出用量详情信息
	PayloadType     string            // payload类型,之前固定是input和output,现在需要区分计费类型
	BillingTags     map[string]string // 计费标签，可以基于标签统计用量
	SkipBilling     bool              // 是否跳过计费，true:跳过计费
	SpaceID         string            // 空间ID
	KnowledgeBaseID uint64            // 知识库ID
	SourceType      string            // 来源类型, enum: knowledge, chat
}

type ModelStatus struct {
	ProviderType string
	IsFree       bool
}

type ModelStatusReq struct {
	OriModelName     string // 原始模型名
	BillingModelName string // 映射成查计费的模型名
	SubBizType       string
}

type QuotaStatus int

const (
	// QuotaStatusAvailable 配额状态可用
	QuotaStatusAvailable QuotaStatus = 0
	// QuotaStatusTolerated 配额超量但可用-需上报超量
	QuotaStatusTolerated QuotaStatus = 1
	// QuotaStatusExceeded 配额超限不可用
	QuotaStatusExceeded QuotaStatus = 2
)

// CheckQuotaReq 检查配额请求
type CheckQuotaReq struct {
	App                  *entity.App
	NewCharSize          uint64 // 新增字符数-老用户
	NewKnowledgeCapacity uint64 // 新增知识容量(字节)-新用户
	NewComputeCapacity   uint64 // 新增计算容量(字节)-新用户
	NewStorageCapacity   uint64 // 新增存储容量(字节)-新用户
}

// CheckQuotaResp 检查配额响应
type CheckQuotaResp struct {
	Status                    QuotaStatus // 配额状态, 0 可用, 1 超量不可用, 2 超量但可用（需要上报超量）
	KnowledgeCapacityExceeded uint64      // 知识容量超量（字节）-上报
	StorageCapacityExceeded   uint64      // 存储容量超量（字节）-计费
	ComputeCapacityExceeded   uint64      // 计算容量超量（字节）-计费
}
