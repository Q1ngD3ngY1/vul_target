package model

import "time"

const (
	// ChunkIsNotDeleted 文档未删除
	ChunkIsNotDeleted = 1
	// ChunkIsDeleted 文档已删除
	ChunkIsDeleted = 2

	// DocChunkTypeText 文本
	DocChunkTypeText DocChunkType = 0
	// DocChunkTypeTable 表格
	DocChunkTypeTable DocChunkType = 1
	// DocChunkTypeImage 图片
	DocChunkTypeImage DocChunkType = 2
)

// DocChunkType 文档chunk类型
type DocChunkType uint32

// DocChunk 文档chunk
type DocChunk struct {
	ID            uint64       `db:"id"`
	PartitionID   uint64       `db:"partition_id"`    // 分区ID doc_id%100
	BatchID       int          `db:"batch_id"`        // 批次ID
	RobotID       uint64       `db:"robot_id"`        // 机器人ID
	DocID         uint64       `db:"doc_id"`          // 文章ID
	RichTextIndex int          `db:"rich_text_index"` // rich text 索引
	Index         int          `db:"c_index"`         // 文章chunk index 同一章节下自增
	CorpID        uint64       `db:"corp_id"`         // 企业ID
	StaffID       uint64       `db:"staff_id"`        // 员工ID
	Content       string       `db:"content"`         // chunk内容
	SplitModel    string       `db:"split_model"`     // 分割模式
	IsDeleted     int          `db:"is_deleted"`      // 1未删除 2已删除
	UpdateTime    time.Time    `db:"update_time"`     // 更新时间
	CreateTime    time.Time    `db:"create_time"`     // 响应时间
	Type          DocChunkType `db:"type"`            // 类型
	ResContent    string       `db:"res_content"`     // 资源内容
	Usage         int          `db:"c_usage"`         // 用途, model.SegmentTypeQA 用于生成QA, model.SegmentTypeIndex 用于生成索引
}
