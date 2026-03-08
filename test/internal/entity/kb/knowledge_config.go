package kb

const (
	//应用配置状态
	NotKnowledge = 1 //知识库无内容
	NotAppUser   = 2 //知识库有内容，未单独配置权限
	HasAppUser   = 3 //知识库有内容，已单独配置权限
	HasThirdAcl  = 4 //知识库有内容，且对接了外部系统时
)

// TableName 设置表名
func (KnowledgeConfig) TableName() string {
	return "t_knowledge_config"
}

// ThirdAclConfig 第三方权限配置
type ThirdAclConfig struct {
	Type                uint32 `json:"type"`
	ThirdToken          string `json:"third_token"`
	CheckPermissionsUrl string `json:"check_permissions_url"`
}

type KnowledgeModel struct {
	ModelName      string `json:"model_name"`
	ModelAliasName string `json:"model_alias_name"`
}
