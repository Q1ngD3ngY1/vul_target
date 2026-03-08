package release

// DevReleaseRelationType 开发域和发布域关联关系类型
const (
	// DevReleaseRelationTypeDocument 文档类型
	DevReleaseRelationTypeDocument = uint32(2)
	// DevReleaseRelationTypeQA QA类型
	DevReleaseRelationTypeQA = uint32(3)
	// DevReleaseRelationTypeTable 数据表类型
	DevReleaseRelationTypeTable = uint32(4)
	// DevReleaseRelationTypeDatabase 数据库类型
	DevReleaseRelationTypeDatabase = uint32(5)
)
