package segment

type EsSegment struct {
	RobotID     uint64   `json:"robot_id"`     // 机器人ID
	ID          uint64   `json:"id"`           // ID
	DocType     uint32   `json:"doc_type"`     // 文档类型 (1 QA, 2 文档或者表格, 3 拒答问题, 4 搜索引擎)
	SegmentType string   `json:"segment_type"` // 文档切片类型，segment-文档切片 table-表格，当doc_type=2时需要填写
	DocID       uint64   `json:"doc_id"`       // 文档id，如果是文档则是对应的文档id，如果是qa，则用关联的文档id，即t_doc_qa表里的doc_id字段，没有则为0
	PageContent string   `json:"page_content"` // 文档内容
	Labels      []string `json:"labels"`       // labels 标签
	ExpireTime  int64    `json:"expire_time"`  // 有效期，时间戳，秒。填0时不过期
	UpdateTime  string   `json:"update_time"`  // 数据更新时间，秒
}
