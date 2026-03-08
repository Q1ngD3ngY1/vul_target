package model

import "time"

// ReleaseSegment 文档段
type ReleaseSegment struct {
	ID              uint64    `db:"id"`
	RobotID         uint64    `db:"robot_id"`         // 机器人ID
	CorpID          uint64    `db:"corp_id"`          // 企业ID
	StaffID         uint64    `db:"staff_id"`         // 员工ID
	DocID           uint64    `db:"doc_id"`           // 文章ID
	SegmentID       uint64    `db:"segment_id"`       // 段落ID
	VersionID       uint64    `db:"version_id"`       // 版本ID
	FileType        string    `db:"file_type"`        // 文件类型(markdown,word,txt)
	Title           string    `db:"title"`            // 标题
	PageContent     string    `db:"page_content"`     // 段落内容
	OrgData         string    `db:"org_data"`         // 段落原文
	SplitModel      string    `db:"split_model"`      // 分割模式line:按行 window:按窗口
	Status          uint32    `db:"status"`           // 状态(1未处理2处理完成)
	ReleaseStatus   uint32    `db:"release_status"`   // 发布状态(2 待发布 3 发布中 4 已发布 5 发布失败)
	Message         string    `db:"message"`          // 失败原因
	IsDeleted       int       `db:"is_deleted"`       // 1未删除 2已删除
	Action          uint32    `db:"action"`           // 面向发布操作：1新增 2修改 3删除 4发布
	BatchID         int       `db:"batch_id"`         // 批次ID
	RichTextIndex   int       `db:"rich_text_index"`  // rich text 索引
	StartChunkIndex int       `db:"start_index"`      // 分片起始索引
	EndChunkIndex   int       `db:"end_index"`        // 分片结束索引
	UpdateTime      time.Time `db:"update_time"`      // 更新时间
	CreateTime      time.Time `db:"create_time"`      // 响应时间
	IsAllowRelease  uint32    `db:"is_allow_release"` // 是否允许发布
	AttrLabels      string    `db:"attr_labels"`      // 属性标签
	ExpireTime      time.Time `db:"expire_time"`      // 有效结束时间
}
