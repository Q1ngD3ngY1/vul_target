package model

import "time"

// RetrievalNodeInfo 节点信息
type RetrievalNodeInfo struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	RobotID      uint64    `gorm:"column:robot_id"`
	DocType      int8      `gorm:"column:doc_type"`
	RelatedID    uint64    `gorm:"column:related_id"`
	DocID        uint64    `gorm:"column:doc_id"`
	ParentID     uint64    `gorm:"column:parent_id"`
	SegmentType  string    `gorm:"column:segment_type"`
	PageContent  string    `gorm:"column:page_content"`
	OrgData      string    `gorm:"column:org_data"`
	BigDataID    string    `gorm:"column:big_data_id"`
	Question     string    `gorm:"column:question"`
	Answer       string    `gorm:"column:answer"`
	CustomParam  string    `gorm:"column:custom_param"`
	QuestionDesc string    `gorm:"column:question_desc"`
	Labels       string    `gorm:"column:labels"`
	Reserve1     string    `gorm:"column:reserve1"`
	Reserve2     string    `gorm:"column:reserve2"`
	Reserve3     string    `gorm:"column:reserve3"`
	IsDeleted    int8      `gorm:"column:is_deleted"`
	CreateTime   time.Time `gorm:"column:create_time"`
	UpdateTime   time.Time `gorm:"column:update_time"`
	ExpireTime   time.Time `gorm:"column:expire_time"`
}

// 表名配置（GORM 默认使用结构体名的蛇形复数作为表名）
func (RetrievalNodeInfo) TableName() string {
	return "t_retrieval_node_info"
}
