package model

import "time"

type KnowledgeBase struct {
	ID             uint64    `db:"id" gorm:"column:id;primaryKey;autoIncrement"`
	CorpBizID      uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`
	KnowledgeBizId uint64    `db:"knowledge_biz_id" gorm:"column:knowledge_biz_id"` //知识库业务id
	ProcessingFlag uint64    `db:"processing_flag" gorm:"column:processing_flag"`   //知识库处理中状态标记
	IsDeleted      uint32    `db:"is_deleted" gorm:"column:is_deleted"`             //0正常，1已删除
	CreateTime     time.Time `db:"create_time" gorm:"column:create_time"`           //创建时间
	UpdateTime     time.Time `db:"update_time" gorm:"column:update_time"`           //更新时间
}

// TableName 设置表名
func (KnowledgeBase) TableName() string {
	return "t_knowledge_base"
}

// HasProcessingFlag 判断知识库是否包含指定处理中状态标记
func (d *KnowledgeBase) HasProcessingFlag(flag uint64) bool {
	if d == nil {
		return false
	}
	if flag == 0 {
		return true
	}
	if d.ProcessingFlag&flag > 0 {
		return true
	}
	return false
}

// AddProcessingFlag 添加知识库处理中状态标记
func (d *KnowledgeBase) AddProcessingFlag(flags []uint64) {
	if len(flags) == 0 {
		return
	}
	for _, attr := range flags {
		d.ProcessingFlag |= attr
	}
	return
}

// RemoveProcessingFlag 去除知识库处理中状态标记
func (d *KnowledgeBase) RemoveProcessingFlag(flags []uint64) {
	if len(flags) == 0 {
		return
	}
	for _, flag := range flags {
		d.ProcessingFlag = d.ProcessingFlag &^ flag
	}
	return
}
