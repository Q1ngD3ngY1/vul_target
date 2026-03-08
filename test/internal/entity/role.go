package entity

import (
	"time"

	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
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
	NotRoleBizId       = uint64(0) // 不检索角色
	NotSearchKnowledge = "不检索知识"

	RoleChooseAll  = 1 // 角色选择全部知识
	RoleChooseKnow = 2 // 角色选择知识库

	KnowPrivate = 1 // 知识库私有
	KnowPublic  = 2 // 知识库公有

	KnowSearchAll     = 1 // 角色知识库选择全部
	KnowSearchSpecial = 2 // 角色知识库选择特定知识
	KnowSearchLabel   = 3 // 角色知识库选择标签

	ConditionLogicNo  = 0
	ConditionLogicAnd = 1
	ConditionLogicOr  = 2
)

// 搜索类型常量
const (
	SearchTypeDoc      = 1 // 文档搜索类型
	SearchTypeDocCate  = 2 // 文档分类搜索类型
	SearchTypeQA       = 3 // 问答搜索类型
	SearchTypeQACate   = 4 // 问答分类搜索类型
	SearchTypeDatabase = 5 // 数据库搜索类型
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
	IsDeleted   bool      `gorm:"column:is_deleted;not null;default:0"`                     // 是否删除
	UpdateTime  time.Time `gorm:"column:update_time;;autoUpdateTime"`                       // 更新时间
	CreateTime  time.Time `gorm:"column:create_time;autoCreateTime"`                        // 创建时间
}

// TableName 设置表名
func (KnowledgeRole) TableName() string {
	return "t_knowledge_role"
}

// KnowledgeRole 角色信息查询结构
type KnowledgeRoleFilter struct {
	Name        string   // 角色名称
	SearchWord  string   // 搜索名字
	Type        uint32   // 角色类型(1 预置 2 自定义)
	Description string   // 角色描述
	BizIDs      []uint64 // 角色业务ID
	NeedCount   bool     // 是否需要返回总数
	Limit       int
	Offset      int
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
	IsDeleted      bool      `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleKnow) TableName() string {
	return "t_knowledge_role_know"
}

// KnowledgeRoleKnowFilter 角色引用知识库查询结构
type KnowledgeRoleKnowFilter struct {
	RoleBizID       uint64
	KnowledgeBizIDs []uint64
	Limit           int
}

// KnowledgeRoleDoc 角色文档权限表
type KnowledgeRoleDoc struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色主键ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	DocBizID       uint64    `gorm:"column:doc_biz_id;not null"`           // 文档主键ID
	IsDeleted      bool      `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleDoc) TableName() string {
	return "t_knowledge_role_doc"
}

// KnowledgeRoleDocFilter 角色文档权限查询结构
type KnowledgeRoleDocFilter struct {
	RoleBizID       uint64
	KnowledgeBizIDs []uint64
	DocBizIDs       []uint64
	Limit           int
	BatchSize       int
}

// KnowledgeRoleQA 角色问答权限表
type KnowledgeRoleQA struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色业务ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	QABizID        uint64    `gorm:"column:qa_biz_id;not null"`            // 问答业务ID
	IsDeleted      bool      `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleQA) TableName() string {
	return "t_knowledge_role_qa"
}

// KnowledgeRoleQAFilter 角色问答权限查询结构
type KnowledgeRoleQAFilter struct {
	RoleBizID       uint64
	KnowledgeBizIDs []uint64
	QABizIDs        []uint64
	BatchSize       int
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
	IsDeleted      bool      `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleAttributeLabel) TableName() string {
	return "t_knowledge_role_attribute_label"
}

// KnowledgeRoleAttributeLabelFilter 角色标签权限查询结构
type KnowledgeRoleAttributeLabelFilter struct {
	RoleBizID       uint64
	KnowledgeBizIDs []uint64
	AttrBizIDs      []uint64
	LabelBizIDs     []uint64
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
	IsDeleted      bool      `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleCate) TableName() string {
	return "t_knowledge_role_cate"
}

// KnowledgeRoleCateFilter 角色分类权限查询结构
type KnowledgeRoleCateFilter struct {
	RoleBizID       uint64
	KnowledgeBizIDs []uint64
	CatType         uint64
	CateBizIDs      []uint64
	Limit           int
}

// KnowledgeRoleDatabase 角色数据库权限表
type KnowledgeRoleDatabase struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement"`   // 自增主键
	CorpBizID      uint64    `gorm:"column:corp_biz_id;not null"`          // 企业业务ID
	AppBizID       uint64    `gorm:"column:app_biz_id;not null"`           // 应用业务ID
	RoleBizID      uint64    `gorm:"column:role_biz_id;not null"`          // 角色业务ID
	KnowledgeBizID uint64    `gorm:"column:knowledge_biz_id;not null"`     // 知识库ID
	DatabaseBizID  uint64    `gorm:"column:database_biz_id;not null"`      // 问答业务ID
	IsDeleted      bool      `gorm:"column:is_deleted;not null;default:0"` // 是否删除
	UpdateTime     time.Time `gorm:"column:update_time;autoUpdateTime"`    // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;autoCreateTime"`    // 创建时间
}

// TableName 设置表名
func (KnowledgeRoleDatabase) TableName() string {
	return "t_knowledge_role_database"
}

// KnowledgeRoleDatabaseFilter 角色数据库权限查询结构
type KnowledgeRoleDatabaseFilter struct {
	RoleBizID       uint64
	KnowledgeBizIDs []uint64
	DatabaseBizIDs  []uint64
	Limit           int
	BatchSize       int
}

type ChooseLabelLabel struct {
	LabelBizId string
	LabelName  string
}

// 知识库选择标签结构
type ChooseLabel struct {
	AttrBizId string
	AttrName  string
	Labels    []*ChooseLabelLabel
}

// KnowledgeChoose 知识库库选择内容结构
type KnowledgeChoose struct {
	KnowledgeBizId    string         // 知识库业务id
	KnowledgeName     string         // 知识库名字
	Type              uint32         // 知识库类型 1私有 2共享
	SearchType        uint32         // 检索范围(1全部知识 2按特定知识 3按标签)
	DocBizIds         []string       // 选中的文档业务id
	DocCateBizIds     []string       // 选中的文档分类业务id
	QuesAnsBizIds     []string       // 选中的问答业务id
	QuesAnsCateBizIds []string       // 选中的问答分类业务id
	DbBizIds          []string       // 选中的数据库业务id
	Labels            []*ChooseLabel // 关联的标签信息
	Condition         int32          // 操作符 1AND, 2OR
}

type RoleSearchInfo struct {
	Type        uint32 // 类型(1 文档 2 文档分类 3问答 4问答分类 5数据库)
	Name        string
	SearchBizId uint64
	CateBizId   uint64 // 如果是文档或者问答，带上分类业务id
}
