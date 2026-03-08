package model

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"
)

const (
	ColumnID             = "id"               // 自增主键
	ColumnCorpBizID      = "corp_biz_id"      // 企业业务ID
	ColumnAppBizID       = "app_biz_id"       // 应用业务ID
	ColumnBusinessID     = "business_id"      // 角色业务ID
	ColumnName           = "name"             // 角色名称
	ColumnType           = "type"             // 角色类型(1 预置 2 自定义)
	ColumnKnowledgeType  = "knowledge_type"   // 知识库类型
	ColumnDescription    = "description"      // 角色描述
	ColumnSearchType     = "search_type"      // 整体检索范围(1全部知识 2按知识库)
	ColumnUpdateTime     = "update_time"      // 更新时间
	ColumnCreateTime     = "create_time"      // 创建时间
	ColumnLabelOperator  = "lable_operator"   // 标签操作符(1AND 2OR)
	ColumnDocBizID       = "doc_biz_id"       // 文档主键ID
	ColumnQABizID        = "qa_biz_id"        // 问答业务ID
	ColumnAttrBizID      = "attr_biz_id"      // 属性业务ID
	ColumnLabelBizID     = "label_biz_id"     // 属性标签业务ID
	ColumnRoleBizID      = "role_biz_id"      // 角色主键ID
	ColumnKnowledgeBizID = "knowledge_biz_id" // 知识库ID
	ColumnCateBizID      = "cate_biz_id"      // 分类主键ID
	ColumnDeleted        = "is_deleted"       // 是否删除
	ColumnUserBizID      = "user_biz_id"      // 用户业务ID
	ColumnCateType       = "cate_type"        // 分类类型(1文档 2问答)
	ColumnLableCondition = "lable_condition"  // 标签操作符(1AND 2OR)
	ColumnDatabaseBizID  = "database_biz_id"  // 数据库ID
)

const (
	KnowledgeRoleTypePreset = 1 // 预置角色类型
	KnowledgeRoleTypeCustom = 2 // 自定义角色类型

	PresetRoleName     = i18nkey.KeyDefaultRoleAllKnowledge
	NotRoleBizId       = uint64(0) //不检索角色
	NotSearchKnowledge = "不检索知识"

	RoleChooseAll  = 1 //角色选择全部知识
	RoleChooseKnow = 2 //角色选择知识库

	KnowPrivate = 1 //知识库私有
	KnowPublic  = 2 //知识库公有

	KnowSearchAll     = 1 //角色知识库选择全部
	KnowSearchSpecial = 2 //角色知识库选择特定知识
	KnowSearchLabel   = 3 //角色知识库选择标签

	ConditionLogicNo  = 0
	ConditionLogicAnd = 1
	ConditionLogicOr  = 2
)

const RoleKnowledgeRedisKey = "role_knowledge_filter_%d_%d_%d"

// KnowledgeRole 角色信息表
type KnowledgeRole struct {
	ID          uint64    `gorm:"column:id;primaryKey;autoIncrement"`                       // 自增主键
	CorpBizID   uint64    `gorm:"column:corp_biz_id;not null"`                              // 企业业务ID
	AppBizID    uint64    `gorm:"column:app_biz_id;not null"`                               // 应用业务ID
	BusinessID  uint64    `gorm:"column:business_id;not null"`                              // 角色业务ID
	Name        string    `gorm:"column:name;type:varchar(512);not null;default:''"`        // 角色名称
	Type        int8      `gorm:"column:type;not null"`                                     // 角色类型(1 预置 2 自定义)
	Description string    `gorm:"column:description;type:varchar(512);not null;default:''"` // 角色描述
	SearchType  int8      `gorm:"column:search_type;not null"`                              // 整体检索范围(1全部知识 2按知识库)
	IsDeleted   uint64    `gorm:"column:is_deleted;not null;default:0"`                     // 是否删除
	UpdateTime  time.Time `gorm:"column:update_time;;autoUpdateTime"`                       // 更新时间
	CreateTime  time.Time `gorm:"column:create_time;autoCreateTime"`                        // 创建时间
}

// TableName 设置表名
func (KnowledgeRole) TableName() string {
	return "t_knowledge_role"
}

// KnowledgeRoleKnow 角色引用知识库表
type KnowledgeRoleKnow struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色主键ID,为-1代表预置角色
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	KnowledgeType  int8      `gorm:"column:knowledge_type;not null"`       // 知识库类型(1 私有知识库 2 共享知识库)
	SearchType     int8      `gorm:"column:search_type;not null"`          // 检索范围(1全部知识 2按特定知识 3按标签)
	LabelCondition int8      `gorm:"column:lable_condition;not null"`      // 标签操作符(1AND 2OR)
	IsDeleted      uint64    `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleKnow) TableName() string {
	return "t_knowledge_role_know"
}

// KnowledgeRoleDoc 角色文档权限表
type KnowledgeRoleDoc struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色主键ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	DocBizID       uint64    `gorm:"column:doc_biz_id;not null"`           // 文档主键ID
	IsDeleted      uint64    `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleDoc) TableName() string {
	return "t_knowledge_role_doc"
}

// KnowledgeRoleQA 角色问答权限表
type KnowledgeRoleQA struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色业务ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	QABizID        uint64    `gorm:"column:qa_biz_id;not null"`            // 问答业务ID
	IsDeleted      uint64    `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleQA) TableName() string {
	return "t_knowledge_role_qa"
}

// KnowledgeRoleAttributeLabel 角色标签权限表
type KnowledgeRoleAttributeLabel struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色业务ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	AttrBizID      uint64    `gorm:"column:attr_biz_id;not null"`          // 属性业务ID
	LabelBizID     uint64    `gorm:"column:label_biz_id;not null"`         // 属性标签业务ID
	IsDeleted      uint64    `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleAttributeLabel) TableName() string {
	return "t_knowledge_role_attribute_label"
}

const (
	CateTypeDoc = 1 // 文档分类类型
	CateTypeQA  = 2 // 问答分类类型
)

// KnowledgeRoleCate 角色分类权限表
type KnowledgeRoleCate struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色业务ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	CateType       uint64    `gorm:"column:type;not null"`                 // 分类类型(1文档 2问答)
	CateBizID      uint64    `gorm:"column:cate_biz_id;not null"`          // 分类主键ID
	IsDeleted      uint64    `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleCate) TableName() string {
	return "t_knowledge_role_cate"
}

// KnowledgeRoleDatabase 角色数据库权限表
type KnowledgeRoleDatabase struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色业务ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	DatabaseBizID  uint64    `gorm:"column:database_biz_id;not null"`      // 问答业务ID
	IsDeleted      uint64    `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleDatabase) TableName() string {
	return "t_knowledge_role_database"
}
