package model

import "time"

const (
	KnowledgeSchemaItemTypeDoc        = int8(1) // 文档
	KnowledgeSchemaItemTypeDocCluster = int8(2) // 文档聚类
	KnowledgeSchemaItemTypeDBTable    = int8(3) // 数据库表
)

type KnowledgeSchema struct {
	Id         uint64    `db:"id" gorm:"column:id"`
	CorpBizId  uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`                                                       // 企业业务ID
	AppBizId   uint64    `db:"app_biz_id" gorm:"column:app_biz_id"`                                                         // 应用业务ID
	Version    uint64    `db:"version" gorm:"column:version"`                                                               // 版本id，对应任务表的自增id
	ItemType   int8      `db:"item_type" gorm:"column:item_type"`                                                           // 物料类型,1:文档 2:文档聚类
	ItemBizId  uint64    `db:"item_biz_id" gorm:"column:item_biz_id"`                                                       // 物料ID：文档业务ID或文档聚类业务ID
	Name       string    `db:"name" gorm:"column:name"`                                                                     // 文档或者文档聚类名称
	Summary    string    `db:"summary" gorm:"column:summary"`                                                               // 文档或者文档聚类摘要
	IsDeleted  int8      `db:"is_deleted" gorm:"column:is_deleted;default:0"`                                               // 是否删除
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
