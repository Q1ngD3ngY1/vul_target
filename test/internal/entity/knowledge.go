package entity

// EnableScopeAttr 启用域属性标签
const EnableScopeAttr = "enable_scope"

const (
	EnableScopeInvalid = 0 // 无效取值
	EnableScopeDisable = 1 // 不生效
	EnableScopeDev     = 2 // 仅调试生效
	EnableScopePublish = 3 // 仅发布生效
	EnableScopeAll     = 4 // 调试/发布都生效
)

// EnableScopeDb2Label 启用状态映射到标签
var EnableScopeDb2Label = map[uint32]string{
	EnableScopeDisable: "disable",
	EnableScopeDev:     "dev-domain",
	EnableScopePublish: "publish-domain",
	EnableScopeAll:     "all",
}
