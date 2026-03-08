package model

import "time"

// DocSegmentOrgData 存储ord_data
type DocSegmentOrgData struct {
	BusinessID         uint64    `db:"business_id"   gorm:"column:business_id"`                 // 业务ID
	AppBizID           uint64    `db:"app_biz_id"    gorm:"column:app_biz_id"`                  // 机器人ID
	DocBizID           uint64    `db:"doc_biz_id" gorm:"column:doc_biz_id"`                     // 文档ID
	CorpBizID          uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`                   // 企业ID
	StaffBizID         uint64    `db:"staff_biz_id" gorm:"column:staff_biz_id"`                 // 员工ID
	OrgData            string    `db:"org_data" gorm:"column:org_data"`                         // 原始数据ID
	OrgPageNumbers     string    `db:"org_page_numbers" gorm:"column:org_page_numbers"`         // 原始内容对应的页码。从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空
	SheetData          string    `db:"sheet_data" gorm:"column:sheet_data"`                     // 当输入文件为excel时，返回当前orgdata和bigdata对应的sheet_data，因为表格的orgdata和bigdata相等，所以这里只返回一个
	SegmentType        string    `db:"segment_type" gorm:"column:segment_type"`                 // 段落类型
	AddMethod          uint32    `db:"add_method" gorm:"column:add_method"`                     // (切片干预）添加方式 0:初版解析生成 1:手动添加'
	IsTemporaryDeleted uint32    `db:"is_temporary_deleted" gorm:"column:is_temporary_deleted"` // (切片干预）是否删除 0:未删除 1:已删除'
	IsDeleted          uint32    `db:"is_deleted" gorm:"column:is_deleted"`                     // 是否删除(0未删除 1已删除）
	IsDisabled         uint32    `db:"is_disabled" gorm:"column:is_disabled"`                   // 是否停用(0启用 1停用)
	CreateTime         time.Time `db:"create_time" gorm:"column:create_time"`                   // 创建时间
	UpdateTime         time.Time `db:"update_time" gorm:"column:update_time"`                   // 更新时间
	SheetName          string    `db:"sheet_name" gorm:"column:sheet_name"`                     // 表格sheet名称
}
