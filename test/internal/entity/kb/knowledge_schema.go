package kb

import (
	"time"
)

type KnowledgeSchemaTask struct {
	Id         uint64    `db:"id" gorm:"column:id"`
	CorpBizId  uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`                                                       // 企业业务ID
	AppBizId   uint64    `db:"app_biz_id" gorm:"column:app_biz_id"`                                                         // 应用业务ID
	BusinessID uint64    `db:"business_id" gorm:"column:business_id"`                                                       // 业务ID
	Status     uint32    `db:"status" gorm:"column:status;default:0"`                                                       // 状态(0待处理 1处理中 2处理成功 3处理失败,4处理中止)
	StatusCode uint32    `db:"status_code" gorm:"column:status_code;default:0"`                                             // 任务状态码，表示任务处于当前状态的具体原因
	Message    string    `db:"message" gorm:"column:message"`                                                               // 失败原因
	IsDeleted  bool      `db:"is_deleted" gorm:"column:is_deleted;default:0"`                                               // 是否删除
	CreateTime time.Time `db:"create_time" gorm:"column:create_time;default:CURRENT_TIMESTAMP"`                             // 创建时间
	UpdateTime time.Time `db:"update_time" gorm:"column:update_time;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
}

// KnowledgeGenerateSchemaParams 文档生成Schema参数
type KnowledgeGenerateSchemaParams struct {
	Name             string `json:"name"`               // 任务名称
	CorpID           uint64 `json:"corp_id"`            // 企业ID
	CorpBizID        uint64 `json:"corp_biz_id"`        // 企业ID
	AppID            uint64 `json:"app_id"`             // 应用ID
	AppBizID         uint64 `json:"app_biz_id"`         // 应用业务ID
	TaskBizID        uint64 `json:"task_biz_id"`        // 任务业务ID
	SummaryModelName string `json:"summary_model_name"` // 摘要模型名称
	NeedCluster      bool   `json:"need_cluster"`       // 是否需要聚类
	StatusCode       uint32 `json:"status_code"`        // 状态码
	Message          string `json:"message"`            // 消息
	Language         string `json:"language"`           // 国际化语言
}

type KnowledgeSchema struct {
	Id         uint64    `db:"id" gorm:"column:id"`
	CorpBizId  uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`                                                       // 企业业务ID
	AppBizId   uint64    `db:"app_biz_id" gorm:"column:app_biz_id"`                                                         // 应用业务ID
	Version    uint64    `db:"version" gorm:"column:version"`                                                               // 版本id，对应任务表的自增id
	ItemType   int8      `db:"item_type" gorm:"column:item_type"`                                                           // 物料类型,1:文档 2:文档聚类
	ItemBizId  uint64    `db:"item_biz_id" gorm:"column:item_biz_id"`                                                       // 物料ID：文档业务ID或文档聚类业务ID
	Name       string    `db:"name" gorm:"column:name"`                                                                     // 文档或者文档聚类名称
	Summary    string    `db:"summary" gorm:"column:summary"`                                                               // 文档或者文档聚类摘要
	IsDeleted  bool      `db:"is_deleted" gorm:"column:is_deleted;default:0"`                                               // 是否删除
	UpdateTime time.Time `db:"update_time" gorm:"column:update_time;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
	CreateTime time.Time `db:"create_time" gorm:"column:create_time;default:CURRENT_TIMESTAMP"`                             // 创建时间
}

type KnowledgeSchemaTaskFilter struct {
	AppBizId       uint64 // 应用业务ID
	Statuses       []int32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string

	IsDeleted *int
}

type KnowledgeSchemaFilter struct {
	AppBizId       uint64 // 应用业务ID
	EnvType        string // 环境类型
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string

	IsDeleted *int
}

// GetKBDocSummaryReq 获取文档摘要请求
type GetKBDocSummaryReq struct {
	RobotID   uint64 `json:"robot_id"`
	BotBizId  uint64 `json:"bot_biz_id"` // 机器人business_id
	RequestId string `json:"request_id"` // 请求ID
	DocID     uint64 `json:"doc_id"`
	FileName  string `json:"file_name"`
	ModelName string `json:"model_name"` // 模型名称
}

// KBDocSummary 文档摘要
type KBDocSummary struct {
	FileName    string
	FileContent string
}

// GetKBDirSummaryReq 获取文件夹摘要请求
type GetKBDirSummaryReq struct {
	BotBizId  uint64       `json:"bot_biz_id"` // 机器人business_id
	RequestId string       `json:"request_id"` // 请求ID
	FileInfos []KBFileInfo `json:"file_infos"` // 文件信息
	ModelName string       `json:"model_name"` // 模型名称
}

// KBFileInfo 文件信息
type KBFileInfo struct {
	FileName    string `json:"file_name"`    // 文件名
	FileSummary string `json:"file_summary"` // 文件摘要
}

// KBDirSummary 文件摘要
type KBDirSummary struct {
	FileInfo string // 文件信息
}

// ModelParam 模型参数
type ModelParam struct {
	Name    string      `json:"name"`
	Default interface{} `json:"default"`
	Min     interface{} `json:"min,omitempty"`
	Max     interface{} `json:"max,omitempty"`
	Type    string      `json:"type"`
}

// KnowledgeSchemaProdExport 导出的知识库schema结构
type KnowledgeSchemaProdExport struct {
	ItemType  int8   `json:"ItemType"`  // 物料类型,1:文档 2:文档聚类
	ItemBizId string `json:"ItemBizId"` // 物料ID：文档业务ID或文档聚类业务ID
	Name      string `json:"Name"`      // 文档或者文档聚类名称
	Summary   string `json:"Summary"`   // 文档或者文档聚类摘要
}

type DocSchemaExport struct {
	DocId    string `json:"DocId"`    // 文档业务ID
	FileName string `json:"FileName"` // 文件名称
	Summary  string `json:"summary"`  // 摘要
	Vector   string `json:"Vector"`   // 特征向量 base64编码 数据库里是blob 会乱码
}

// DocClusterSchemaExport 导出的文档聚类schema结构
type DocClusterSchemaExport struct {
	ClusterId   string `json:"ClusterId"`   // 文档聚类业务ID
	ClusterName string `json:"ClusterName"` // 聚类名称
	Summary     string `json:"Summary"`     // 摘要
	DocIDs      string `json:"DocIDs"`      // 文档ID列表,json格式
}

type ExportKnowledgeSchemaProdRsp struct {
	KbDocBizIDMap   map[string]struct{}
	ClusterBizIDMap map[string]struct{}
}
