package database

import (
	"time"
)

// TableTopValue 外部数据库数据表top value表
type TableTopValue struct {
	ID                 uint64    `gorm:"primaryKey;autoIncrement;column:id" json:"id"`
	CorpBizID          uint64    `gorm:"not null;column:corp_biz_id" json:"corpBizId"`                            // 企业业务ID
	AppBizID           uint64    `gorm:"not null;column:app_biz_id" json:"appBizId"`                              // 应用业务ID
	DBSourceBizID      uint64    `gorm:"column:db_source_biz_id" json:"dbSourceBizID"`                            // 数据库业务ID
	DbTableBizID       uint64    `gorm:"not null;column:db_table_biz_id" json:"dbTableBizId"`                     // 数据表业务ID
	DbTableColumnBizID uint64    `gorm:"not null;column:db_table_column_biz_id" json:"dbTableColumnBizId"`        // 数据列业务ID
	BusinessID         uint64    `gorm:"not null;column:business_id" json:"businessId"`                           // 新产生的ID，雪花算法 id
	ColumnName         string    `gorm:"not null;size:100;column:column_name" json:"columnName"`                  // 原始的列名称
	ColumnValue        string    `gorm:"not null;type:text;column:column_value" json:"columnValue"`               // 列别名
	ColumnComment      string    `gorm:"not null;size:1500;column:column_comment" json:"columnComment"`           // 列注释
	IsDeleted          bool      `gorm:"not null;column:is_deleted" json:"isDeleted"`                             // 0:未删除,1:已删除
	CreateTime         time.Time `gorm:"not null;default:CURRENT_TIMESTAMP;column:create_time" json:"createTime"` // 创建时间
	UpdateTime         time.Time `gorm:"not null;default:CURRENT_TIMESTAMP;column:update_time" json:"updateTime"` // 更新时间
}
