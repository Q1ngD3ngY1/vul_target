package model

import "time"

// 库表字段限制
const (
	ShareKnowledgeNameLength           = 30
	ShareKnowledgeDescriptionLength    = 2000
	ShareKnowledgeQaExtractModelLength = 128
	ShareKnowledgeEmbeddingModelLength = 128
)

// SharedKnowledgeInfo 共享知识库信息
type SharedKnowledgeInfo struct {
	ID             uint64    `db:"id" gorm:"column:id"`                             // 自增ID
	CorpBizID      uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`           // 企业业务ID
	BusinessID     uint64    `db:"business_id" gorm:"column:business_id"`           // 共享知识库业务ID
	Name           string    `db:"name" gorm:"column:name"`                         // 共享知识库名称
	Description    string    `db:"description" gorm:"column:description"`           // 共享知识库描述
	UserBizID      uint64    `db:"user_biz_id" gorm:"column:user_biz_id"`           // staff表的business_id
	UserName       string    `db:"user_name" gorm:"column:user_name"`               // staff表的name(用于分页搜索)
	EmbeddingModel string    `db:"embedding_model" gorm:"column:embedding_model"`   // Embedding模型
	QaExtractModel string    `db:"qa_extract_model" gorm:"column:qa_extract_model"` // 问答对抽取模型
	IsDeleted      int       `db:"is_deleted" gorm:"column:is_deleted"`             // 0:未删除 1:已删除
	SpaceID        string    `db:"space_id" gorm:"column:space_id"`                 // 空间ID
	OwnerStaffID   uint64    `db:"owner_staff_id" gorm:"column:owner_staff_id"`     // 所有者的员工ID
	CreateTime     time.Time `db:"create_time" gorm:"column:create_time"`           // 创建时间
	UpdateTime     time.Time `db:"update_time" gorm:"column:update_time"`           // 更新时间

	KnowledgeSchemaModel string `db:"-" gorm:"-"` // 知识库Schema模型
	OwnerStaffName       string `db:"-" gorm:"-"` // 所有者的员工名称
}

// TableName 库表名
func (SharedKnowledgeInfo) TableName() string {
	return "t_share_knowledge"
}
