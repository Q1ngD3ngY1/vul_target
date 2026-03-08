package database

import (
	"time"
)

type Column struct {
	ID                 int64     `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID          uint64    `gorm:"column:corp_biz_id;not null;uniqueIndex:idx_column_id,priority:1;comment:企业业务ID"`
	AppBizID           uint64    `gorm:"column:app_biz_id;not null;uniqueIndex:idx_column_id,priority:2;comment:应用业务ID"`
	DBTableBizID       uint64    `gorm:"column:db_table_biz_id;not null;uniqueIndex:idx_column_id,priority:3;comment:表元数据ID"`
	DBTableColumnBizID uint64    `gorm:"column:db_table_column_biz_id;not null;uniqueIndex:idx_column_id,priority:4;comment:列数据ID"`
	ColumnName         string    `gorm:"column:column_name;type:varchar(100);not null;comment:列名"`
	DataType           string    `gorm:"column:data_type;type:varchar(32);not null;comment:数据类型"`
	ColumnComment      string    `gorm:"column:column_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName          string    `gorm:"column:alias_name;type:varchar(100);default:'';comment:别名"`
	Description        string    `gorm:"column:description;type:varchar(500);default:'';comment:列描述"`
	Unit               string    `gorm:"column:unit;type:varchar(50);default:'';comment:单位"`
	IsIndexed          bool      `gorm:"column:is_indexed;type:tinyint(1);default:0;comment:是否参与索引"`
	IsDeleted          bool      `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	CreateTime         time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime         time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

type ColumnFilter struct {
	CorpBizID      uint64
	AppBizID       uint64
	DBTableBizID   uint64
	DBTableBizIDs  []uint64
	DBColumnBizID  uint64
	DBColumnBizIDs []uint64
	IsDeleted      *bool
	Name           *string
}

type ColumnInfo struct {
	ColumnName string
	DataType   string
	ColComment string
}
