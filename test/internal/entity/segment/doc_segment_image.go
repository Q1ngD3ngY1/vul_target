package segment

import "time"

const (
	DocSegmentImageTblColId         = "id"
	DocSegmentImageTblColImageId    = "image_id"
	DocSegmentImageTblColIsDeleted  = "is_deleted"
	DocSegmentImageTblColCreateTime = "create_time"
	DocSegmentImageTblColUpdateTime = "update_time"
)

// DocSegmentImage 文档切片图片
type DocSegmentImage struct {
	ID           uint64    `db:"id"`           // 自增ID
	ImageID      uint64    `db:"image_id"`     // 图片ID
	SegmentID    uint64    `db:"segment_id"`   // 切片ID
	DocID        uint64    `db:"doc_id"`       // 文档ID
	RobotID      uint64    `db:"robot_id"`     // 机器人ID
	CorpID       uint64    `db:"corp_id"`      // 企业ID
	StaffID      uint64    `db:"staff_id"`     // 员工ID
	OriginalUrl  string    `db:"original_url"` // 原始url
	ExternalUrl  string    `db:"external_url"` // 对外url
	IsDeleted    uint32    `db:"is_deleted"`   // 是否删除(1未删除 2已删除）
	CreateTime   time.Time `db:"create_time"`  // 创建时间
	UpdateTime   time.Time `db:"update_time"`  // 更新时间
	SegmentBizID uint64    `db:"-"`            // 切片BusinessID，查询切片ID使用，不写DB
}

type DocSegmentImageFilter struct {
	ID             uint64
	IDs            []uint64
	AppID          uint64
	SegmentID      uint64
	SegmentIDs     []uint64
	DocID          uint64
	RobotID        uint64
	CorpID         uint64
	Offset         int
	Limit          int
	IsDeleted      *int
	OrderColumn    []string
	OrderDirection []string
	DistinctColumn []string
	ExtraCondition string
	ExtraParams    []any
}
