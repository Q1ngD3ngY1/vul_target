package model

import "time"

// DocClusterSchema 文档聚类Schema
type DocClusterSchema struct {
	ID          uint64    `gorm:"column:id"`           // 自增ID
	CorpBizID   uint64    `gorm:"column:corp_biz_id"`  // 企业ID
	AppBizID    uint64    `gorm:"column:app_biz_id"`   // 应用ID
	BusinessID  uint64    `gorm:"column:business_id"`  // 业务ID
	Version     uint64    `gorm:"column:version"`      // 版本
	ClusterName string    `gorm:"column:cluster_name"` // 聚类名称
	Summary     string    `gorm:"column:summary"`      // 摘要
	DocIDs      string    `gorm:"column:doc_ids"`      // 文档ID列表,json格式
	IsDeleted   int       `gorm:"column:is_deleted"`   // 是否删除
	CreateTime  time.Time `gorm:"column:create_time"`  // 创建时间
	UpdateTime  time.Time `gorm:"column:update_time"`  // 更新时间
}
