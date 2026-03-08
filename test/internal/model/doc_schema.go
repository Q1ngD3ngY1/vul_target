package model

import "time"

// StructFileTypeMap 结构化文件类型
var StructFileTypeMap = map[string]bool{
	"xlsx": true,
	"xls":  true,
	"csv":  true,
}

var TableDataCellDataType2String = map[TableDataCellDataType]string{
	DataTypeString:   "string",
	DataTypeInteger:  "int",
	DataTypeFloat:    "float",
	DataTypeDate:     "date",
	DataTypeTime:     "time",
	DataTypeDatetime: "datetime",
	DataTypeBoolean:  "boolean",
}

type TableFormatMap struct {
	RawText       string `json:"raw"`
	FormattedText string `json:"format"`
	// [v2.5新增] 未来通过LLM生成的表名
	// 用途：生成SQL的prompt里使用，为了在POC时有手段可以临时给某个客户的case（单个表级别）加到生成SQL的prompt里做干预；
	GeneratedName string `json:"generated_name,omitempty"`
	// [v2.5新增] 未来通过LLM生成的表描述
	// 用途：生成SQL的prompt里使用，为了在POC时有手段可以临时给某个客户的case（单个表级别）加到生成SQL的prompt里做干预；
	GeneratedDesc string `json:"generated_desc,omitempty"`
}

// MappingData 表里的 mapping 字段的内容
type MappingData struct {
	FileName  string               `json:"file_name"`
	TableName TableFormatMap       `json:"table_name"`
	Fields    map[string]FormatMap `json:"fields"` // map的key是根据列名动态生成的 field1/field2...
}

// FormatMap 单个row的映射数据
type FormatMap struct {
	RawText       string                `json:"raw"`
	FormattedText string                `json:"format"`
	DataType      TableDataCellDataType `json:"data_type"` // 字段类型
	// [v2.5新增] 未来通过LLM生成的列描述
	// 用途：生成SQL的prompt里使用，为了在POC时有手段可以临时给某个客户的case（单个表级别）加到生成SQL的prompt里做干预；
	GeneratedDesc string `json:"generated_desc,omitempty"`
}

// DocSchema 文档schema
type DocSchema struct {
	ID            uint64    `gorm:"column:id"`          // 自增ID
	CorpBizID     uint64    `gorm:"column:corp_biz_id"` // 企业业务ID
	AppBizID      uint64    `gorm:"column:app_biz_id"`  // 应用业务ID
	DocBizID      uint64    `gorm:"column:doc_biz_id"`  // 文档业务ID
	DocID         uint64    `gorm:"-"`                  // 文档ID
	FileName      string    `gorm:"column:file_name"`   // 文件名称
	Summary       string    `gorm:"column:summary"`     // 摘要
	Vector        []byte    `gorm:"column:vector"`      // 特征向量
	VectorFloat32 []float32 `gorm:"-"`                  // 特征向量
	IsDeleted     int       `gorm:"column:is_deleted"`  // 是否删除
	CreateTime    time.Time `gorm:"column:create_time"` // 创建时间
	UpdateTime    time.Time `gorm:"column:update_time"` // 更新时间
}
