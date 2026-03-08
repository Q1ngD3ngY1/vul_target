package segment

// Text2sqlVersion 对接文件解析拆分服务的协议版本号，为未来扩充预留
type Text2sqlVersion int

// TableDataType 表格类型 同 解析引擎定义的协议
type TableDataType int32

// TableHeaderType 表头类型
type TableHeaderType int32

const (
	// Text2sqlVersion1 v2.3.0的版本
	Text2sqlVersion1 = Text2sqlVersion(1)

	// TableDataTypeOther TODO
	TableDataTypeOther TableDataType = 0 // 非标准表格返回OTHER
	// TableDataTypeNormal TODO
	TableDataTypeNormal TableDataType = 1 // 标准表格的定义（本次迭代的范围）：列名无重复 && 列名不空 && 无合并单元格
	// TableDataTypeNormalLessThanThreshold TODO
	TableDataTypeNormalLessThanThreshold TableDataType = 2 // 符合标准表格的定义，但长度小于阈值

	// TableHeaderTypeUnknown TODO
	TableHeaderTypeUnknown TableHeaderType = 0
	// TableHeaderTypeColumn TODO
	TableHeaderTypeColumn TableHeaderType = 1 // 列表头
	// TableHeaderTypeRow TODO
	TableHeaderTypeRow TableHeaderType = 2 // 行表头
)

// TableDataCellDataType 数据类型枚举
type TableDataCellDataType int32

// Text2SQLCell Text2SQL的分片时，PageContent里存储的数据格式
type Text2SQLCell struct {
	Value    string                `json:"value"`
	DataType TableDataCellDataType `json:"data_type"` // 字段类型
}

// Text2SQLRow .
type Text2SQLRow struct {
	Cells []*Text2SQLCell `json:"cells"`
}

// Text2SQLSegmentTableMetaHeader .
type Text2SQLSegmentTableMetaHeader struct {
	Type TableHeaderType `json:"type"`
	Rows []*Text2SQLRow  `json:"rows"`
}

// Text2SQLSegmentTableMeta    Text2SQL的分片时，PageContent里存储的数据格式
type Text2SQLSegmentTableMeta struct {
	TableID   string                            `json:"tableID"`
	TableName string                            `json:"tableName"`
	Headers   []*Text2SQLSegmentTableMetaHeader `json:"headers"`
	DataType  TableDataType                     `json:"dataType"`
	Message   string                            `json:"message"`
}

// Text2SQLSegmentMeta  Text2SQL的分片时，SegmentTypeText2SQLMeta: PageContent里存储的数据格式
type Text2SQLSegmentMeta struct {
	Version    Text2sqlVersion             `json:"version"`
	FileName   string                      `json:"fileName"`
	TableMetas []*Text2SQLSegmentTableMeta `json:"tableMetas"`
}

// Text2SQLSegmentContent Text2SQL的分片时，SegmentTypeText2SQLContent: PageContent里存储的数据格式
type Text2SQLSegmentContent struct {
	Version Text2sqlVersion `json:"version"`
	TableID string          `json:"tableID"`
	RowNum  int64           `json:"rowNum"`
	Cells   []*Text2SQLCell `json:"cells"`
	// Row     *Text2SQLRow    `json:"rows"`
}
