package document

import (
	"time"
)

// CorpCOSDoc 企业COS文档
type CorpCOSDoc struct {
	ID              uint64    `db:"id"  gorm:"column:id"`
	BusinessID      uint64    `db:"business_id"  gorm:"column:business_id"`
	CorpID          uint64    `db:"corp_id"  gorm:"column:corp_id"`
	RobotID         uint64    `db:"robot_id"  gorm:"column:robot_id"`
	StaffID         uint64    `db:"staff_id"  gorm:"column:staff_id"`
	CosBucket       string    `db:"cos_bucket"  gorm:"column:cos_bucket"`
	CosPath         string    `db:"cos_path"  gorm:"column:cos_path"`
	CosHash         string    `db:"cos_hash"  gorm:"column:cos_hash"`
	CosTag          string    `db:"cos_tag"  gorm:"column:cos_tag"`
	IsDeleted       bool      `db:"is_deleted" gorm:"column:is_deleted"`
	Status          uint32    `db:"status" gorm:"column:status"`
	FailReason      string    `db:"fail_reason" gorm:"column:fail_reason"`
	SyncTime        time.Time `db:"sync_time" gorm:"column:sync_time"`
	BusinessCosURL  string    `db:"business_cos_url"  gorm:"column:business_cos_url"`
	BusinessCosHash string    `db:"business_cos_hash"  gorm:"column:business_cos_hash"`
	BusinessCosTag  string    `db:"business_cos_tag"  gorm:"column:business_cos_tag"`
	CreateTime      time.Time `db:"create_time"  gorm:"column:create_time"`
	UpdateTime      time.Time `db:"update_time"  gorm:"column:update_time"`
}

const (
	corpCOSDocTableName = "t_corp_cos_doc"

	CorpCOSDocTblColID              = "id"
	CorpCOSDocTblColBusinessID      = "business_id"
	CorpCOSDocTblColCorpID          = "corp_id"
	CorpCOSDocTblColRobotID         = "robot_id"
	CorpCOSDocTblColStaffID         = "staff_id"
	CorpCOSDocTblColCosBucket       = "cos_bucket"
	CorpCOSDocTblColCosPath         = "cos_path"
	CorpCOSDocTblColCosHash         = "cos_hash"
	CorpCOSDocTblColCosTag          = "cos_tag"
	CorpCOSDocTblColIsDeleted       = "is_deleted"
	CorpCOSDocTblColStatus          = "status"
	CorpCOSDocTblColFailReason      = "fail_reason"
	CorpCOSDocTblColSyncTime        = "sync_time"
	CorpCOSDocTblColBusinessCosURL  = "business_cos_url"
	CorpCOSDocTblColBusinessCosHash = "business_cos_hash"
	CorpCOSDocTblColBusinessCosTag  = "business_cos_tag"
	CorpCOSDocTblColCreateTime      = "create_time"
	CorpCOSDocTblColUpdateTime      = "update_time"
)

// CorpCOSDocFilter 企业cos文档过滤器
type CorpCOSDocFilter struct {
	BusinessIDs     []uint64
	CorpID          uint64
	RobotID         uint64
	CosTag          string
	BusinessCosHash string
}
