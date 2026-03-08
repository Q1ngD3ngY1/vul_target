package segment

import "time"

// DocSegmentPageInfo 文档切片页码信息
type DocSegmentPageInfo struct {
	ID             uint64    `db:"id"`               // 自增ID
	PageInfoID     uint64    `db:"page_info_id"`     // 页码ID
	SegmentID      uint64    `db:"segment_id"`       // 切片ID
	DocID          uint64    `db:"doc_id"`           // 文档ID
	RobotID        uint64    `db:"robot_id"`         // 机器人ID
	CorpID         uint64    `db:"corp_id"`          // 企业ID
	StaffID        uint64    `db:"staff_id"`         // 员工ID
	OrgPageNumbers string    `db:"org_page_numbers"` // Org页码信息（json存储）
	BigPageNumbers string    `db:"big_page_numbers"` // Big页码信息（json存储）
	SheetData      string    `db:"sheet_data"`       // Sheet页码信息（json存储）
	IsDeleted      uint32    `db:"is_deleted"`       // 是否删除(1未删除 2已删除）
	CreateTime     time.Time `db:"create_time"`      // 创建时间
	UpdateTime     time.Time `db:"update_time"`      // 更新时间
	SegmentBizID   uint64    `db:"-"`                // 切片BusinessID，查询切片ID使用，不写DB
}

type DocSegmentPageInfoFilter struct {
	RouterAppBizId uint64
	IDs            []uint64
	CorpID         uint64
	AppID          uint64
	DocID          uint64
	SegmentID      uint64
	SegmentIDs     []uint64
	IsDeleted      *int
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
}

const (
	DocSegmentPageInfoTblColID          = "id"
	DocSegmentPageInfoTblPageInfoID     = "page_info_id"
	DocSegmentPageInfoTblSegmentID      = "segment_id"
	DocSegmentPageInfoTblColDocId       = "doc_id"
	DocSegmentPageInfoTblColRobotID     = "robot_id"
	DocSegmentPageInfoTblColCorpID      = "corp_id"
	DocSegmentPageInfoTblColStaffId     = "staff_id"
	DocSegmentPageInfoTblOrgPageNumbers = "org_page_numbers"
	DocSegmentPageInfoTblBigPageNumbers = "big_page_numbers"
	DocSegmentPageInfoTblSheetData      = "sheet_data"
	DocSegmentPageInfoTblColIsDeleted   = "is_deleted"
	DocSegmentPageInfoTblColUpdateTime  = "update_time"
	DocSegmentPageInfoTblColCreateTime  = "create_time"
)

var DocSegmentPageInfoTblCols = []string{
	DocSegmentPageInfoTblColID,
	DocSegmentPageInfoTblPageInfoID,
	DocSegmentPageInfoTblSegmentID,
	DocSegmentPageInfoTblColDocId,
	DocSegmentPageInfoTblColRobotID,
	DocSegmentPageInfoTblColCorpID,
	DocSegmentPageInfoTblColStaffId,
	DocSegmentPageInfoTblOrgPageNumbers,
	DocSegmentPageInfoTblBigPageNumbers,
	DocSegmentPageInfoTblSheetData,
	DocSegmentPageInfoTblColIsDeleted,
	DocSegmentPageInfoTblColUpdateTime,
	DocSegmentPageInfoTblColCreateTime,
}
