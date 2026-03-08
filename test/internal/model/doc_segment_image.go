// Package model ...
// @Author: halelv
// @Date: 2024/7/3 19:22
package model

import "time"

// DocSegmentImage 文档切片图片
type DocSegmentImage struct {
	ID           uint64    `db:"id" gorm:"column:id"`                     // 自增ID
	ImageID      uint64    `db:"image_id" gorm:"column:image_id"`         // 图片ID
	SegmentID    uint64    `db:"segment_id" gorm:"column:segment_id"`     // 切片ID
	DocID        uint64    `db:"doc_id" gorm:"column:doc_id"`             // 文档ID
	RobotID      uint64    `db:"robot_id" gorm:"column:robot_id"`         // 机器人ID
	CorpID       uint64    `db:"corp_id" gorm:"column:corp_id"`           // 企业ID
	StaffID      uint64    `db:"staff_id" gorm:"column:staff_id"`         // 员工ID
	OriginalUrl  string    `db:"original_url" gorm:"column:original_url"` // 原始url
	ExternalUrl  string    `db:"external_url" gorm:"column:external_url"` // 对外url
	IsDeleted    int       `db:"is_deleted" gorm:"column:is_deleted"`     // 是否删除(1未删除 2已删除）
	CreateTime   time.Time `db:"create_time" gorm:"column:create_time"`   // 创建时间
	UpdateTime   time.Time `db:"update_time" gorm:"column:update_time"`   // 更新时间
	SegmentBizID uint64    `db:"-" gorm:"-"`                              // 切片BusinessID，查询切片ID使用，不写DB
}
