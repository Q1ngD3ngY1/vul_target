package model

import (
	"time"
)

// Text2sqlMetaMappingPreview text2sql用的field和实际列名的映射表
type Text2sqlMetaMappingPreview struct {
	ID               uint64    `gorm:"column:id;primaryKey;autoIncrement;not null" json:"id"`                       // 主键
	BusinessID       uint64    `gorm:"column:business_id;not null" json:"business_id"`                              // 对外ID，业务ID
	RobotID          uint64    `gorm:"column:robot_id;not null" json:"robot_id"`                                    // 机器人ID，应用ID
	CorpID           uint64    `gorm:"column:corp_id;not null" json:"corp_id"`                                      // 企业ID
	DocID            uint64    `gorm:"column:doc_id;not null" json:"doc_id"`                                        // 文档ID
	TableID          string    `gorm:"column:table_id;type:varchar(32);not null" json:"table_id"`                   // table_id：对于excel表格，就是SheetID
	FileName         string    `gorm:"column:file_name;type:varchar(255);not null;default:''" json:"file_name"`     // 文件名
	Mapping          string    `gorm:"column:mapping;type:mediumtext;not null" json:"mapping"`                      // json格式
	ReleaseStatus    int8      `gorm:"column:release_status;type:tinyint;not null;default:1" json:"release_status"` // 发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)
	IndexName        string    `gorm:"column:index_name;type:varchar(128);not null;default:''" json:"index_name"`   // 评测库索引名称
	DbName           string    `gorm:"column:db_name;type:varchar(64);not null;default:''" json:"db_name"`          // db名称
	MappingTableName string    `gorm:"column:table_name;type:varchar(64);not null;default:''" json:"table_name"`    // 表名
	SubType          string    `gorm:"column:sub_type;type:varchar(64);not null;default:''" json:"sub_type"`        // 列数对应的子类型，用于es索引路由
	IsDeleted        int       `gorm:"column:is_deleted;not null;default:0" json:"is_deleted"`                      // 0：未删除，1：已删除
	CreateTime       time.Time `gorm:"column:create_time;not null;autoCreateTime" json:"create_time"`               // 创建时间
	UpdateTime       time.Time `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`               // 更新时间
}

// TableName 设置表名
func (Text2sqlMetaMappingPreview) TableName() string {
	return "t_text2sql_meta_mapping_preview"
}
