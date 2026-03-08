package kb

// 库表字段限制
const (
	ShareKnowledgeNameLength        = 30
	ShareKnowledgeDescriptionLength = 2000
)

// ===================KnowledgeSchema===========================
// 通用定义
const (
	KnowledgeSchemaItemTypeDoc        = int8(1) // 文档
	KnowledgeSchemaItemTypeDocCluster = int8(2) // 文档聚类
	KnowledgeSchemaItemTypeDBTable    = int8(3) // 数据库表
)

const (
	OperatorCreate = uint32(1)
	OperatorDelete = uint32(2)
)

const (

	// IsDeleted 已删除
	IsDeleted = 1
	// IsNotDeleted 未删除
	IsNotDeleted = 0
	// RunEnvSandbox 沙箱环境
	RunEnvSandbox = 1
	// CloudSID 腾讯云直客集成商ID
	CloudSID = 1
	// DefaultSpaceID 默认空间ID
	DefaultSpaceID = "default_space"
	// EnvTypeSandbox 沙箱环境
	EnvTypeSandbox = "sandbox"
	// EnvTypeProduct 正式环境
	EnvTypeProduct = "product"
	// TaskStatusSuccess 处理成功
	TaskStatusSuccess = uint32(2)
)

const (
	knowledgeSchemaTaskTableName = "t_knowledge_schema_task"

	KnowledgeSchemaTaskTblColStatus     = "status"      // 状态(0待处理 1处理中 2处理成功 3处理失败)
	KnowledgeSchemaTaskTblColStatusCode = "status_code" // 状态(0待处理 1处理中 2处理成功 3处理失败)
	KnowledgeSchemaTaskTblColMessage    = "message"     // 是否删除
	KnowledgeSchemaTaskTblColCreateTime = "create_time" // 创建时间
)

const (
	KnowledgeSchemaTblColItemType    = "item_type"   // 物料类型,1:文档 2:文档聚类
	KnowledgeSchemaTblColItemBizType = "item_biz_id" // 物料ID：文档业务ID或文档聚类业务ID
	KnowledgeSchemaTblColItemName    = "name"        // 文档或者文档聚类名
	KnowledgeSchemaTblColItemSummary = "summary"     // 文档或者文档聚类摘要
)
