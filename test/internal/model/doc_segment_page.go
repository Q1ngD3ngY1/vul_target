package model

import "time"

// DocSegmentPageInfo 文档切片页码信息
type DocSegmentPageInfo struct {
	ID             uint64    `db:"id" gorm:"column:id"`                             // 自增ID
	PageInfoID     uint64    `db:"page_info_id" gorm:"column:page_info_id"`         // 页码ID
	SegmentID      uint64    `db:"segment_id" gorm:"column:segment_id"`             // 切片ID
	DocID          uint64    `db:"doc_id" gorm:"column:doc_id"`                     // 文档ID
	RobotID        uint64    `db:"robot_id" gorm:"column:robot_id"`                 // 机器人ID
	CorpID         uint64    `db:"corp_id" gorm:"column:corp_id"`                   // 企业ID
	StaffID        uint64    `db:"staff_id" gorm:"column:staff_id"`                 // 员工ID
	OrgPageNumbers string    `db:"org_page_numbers" gorm:"column:org_page_numbers"` // Org页码信息（json存储）
	BigPageNumbers string    `db:"big_page_numbers" gorm:"column:big_page_numbers"` // Big页码信息（json存储）
	SheetData      string    `db:"sheet_data" gorm:"column:sheet_data"`             // Sheet页码信息（json存储）
	IsDeleted      int       `db:"is_deleted" gorm:"column:is_deleted"`             // 是否删除(1未删除 2已删除）
	CreateTime     time.Time `db:"create_time" gorm:"column:create_time"`           // 创建时间
	UpdateTime     time.Time `db:"update_time" gorm:"column:update_time"`           // 更新时间
	SegmentBizID   uint64    `db:"-" gorm:"-"`                                      // 切片BusinessID，查询切片ID使用，不写DB
}
