package entity

// 公用权限位
const (
	// ProductType 云图产品名称
	ProductType = "lke"

	// listAppPermissionID 应用资源权限位
	listAppPermissionID = "lkeApp.listApp"

	// allPermissionID 全部权限
	allPermissionID = "*"
	// allPermissionName 全部
	allPermissionName = "全部"
	// allResourcePermissionID 拥有所有资源权限
	allResourcePermissionID = "*"

	listShareKnowledgePermissionID = "lkeShareKG.listShareKG"
)

// PermissionInfo 权限
type PermissionInfo struct {
	PermissionID   string                `json:"permission_id"`   // 权限id 例, // 机器人范围: BotMManagement.BotFields.Assignment 机器人数量:BotLimit)
	ParentID       string                `json:"parent_id"`       // 父权限id
	ProductType    string                `json:"product_type"`    // 产品英文标识:icr
	PermissionName string                `json:"permission_name"` // 权限中文名称
	PermissionType int32                 `json:"permission_type"` // 权限类型 1:需要校验全局资源 2:校验指定资源 3:不校验资源 4:权限即资源,比如机器人上限
	Actions        []string              `json:"actions"`         // 接口列表
	Resources      []*PermissionResource `json:"resources"`       // 关联的非全局资源
}

// PermissionResource 资源权限
type PermissionResource struct {
	ResourceType        string                                 `json:"resource_type"`                 // 资源类型: AppKey 机器人AppKey, BotLimit 机器人数量上限
	ResourceIDs         []string                               `json:"resource_ids"`                  // 资源ID
	EffectPermissionIDs []string                               `json:"effect_permission_ids"`         // 资源影响的权限ID
	ResourceProperties  map[string]*PermissionResourceProperty `json:"resource_properties,omitempty"` // 资源属性数据，key为资源id
}

// PermissionResourceProperty .
type PermissionResourceProperty struct {
	Properties []*Property `json:"properties"` // 属性列表
}

// Property .
type Property struct {
	Key   string `json:"key"`   // 资源属性标识
	Value string `json:"value"` // 资源属性值
}

// Reference:
// https://git.woa.com/dialogue-platform/permission/aiconf-manager-server/blob/master/model/service_model.go

// Response AIConf 响应
type Response struct {
	Response any `json:"Response"`
}

// Error 错误
type Error struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// CreateBusinessAdministratorReq 创建主账号请求
type CreateBusinessAdministratorReq struct {
	Uin           string              `json:"Uin"`
	SubAccountUin string              `json:"SubAccountUin"`
	Src           string              `json:"Src"`
	AccountUin    string              `json:"AccountUin"`
	Description   string              `json:"Description"`
	Permissions   []ProductPermission `json:"Permissions"`
}

// CreateBusinessAdministratorRsp 创建主账号响应
type CreateBusinessAdministratorRsp struct {
	Error     *Error `json:"Error"`
	RequestID string `json:"RequestId"`
}

// DescribeProductPermissionsReq 产品对应的权限数据请求
type DescribeProductPermissionsReq struct {
	Uin           string   `json:"Uin"`
	SubAccountUin string   `json:"SubAccountUin"`
	OwnerUin      string   `json:"OwnerUin"`
	ProductTypes  []string `json:"ProductTypes"`
}

// DescribeProductPermissionsRsp 产品对应的权限数据响应
type DescribeProductPermissionsRsp struct {
	Error       *Error              `json:"Error"`
	RequestID   string              `json:"RequestId"`
	Permissions []ProductPermission `json:"Permissions"`
}

// DescribeAccountInfoReq 获取账号基本信息请求
type DescribeAccountInfoReq struct {
	Uin           string `json:"Uin"`
	SubAccountUin string `json:"SubAccountUin"`
	AccountUin    string `json:"AccountUin"`
}

// DescribeAccountInfoRsp 获取账号基本信息响应
type DescribeAccountInfoRsp struct {
	Error       *Error `json:"Error"`
	RequestID   string `json:"RequestId"`
	Description string `json:"Description"`
	Name        string `json:"Name"`
	Src         string `json:"Src"`
}

// DescribeUserPermissionsReq 获取主账号权限数据请求
type DescribeUserPermissionsReq struct {
	Uin           string `json:"Uin"`
	SubAccountUin string `json:"SubAccountUin"`
	AccountUin    string `json:"AccountUin"`
}

// DescribeUserPermissionsRsp 获取主账号权限数据响应
type DescribeUserPermissionsRsp struct {
	Error        *Error              `json:"Error"`
	RequestID    string              `json:"RequestId"`
	IsNewAccount bool                `json:"IsNewAccount"`
	Permissions  []ProductPermission `json:"Permissions"`
}

// ModifyBusinessAdministratorReq 编辑主账号请求
type ModifyBusinessAdministratorReq struct {
	Uin           string              `json:"Uin"`
	SubAccountUin string              `json:"SubAccountUin"`
	Src           string              `json:"Src"`
	AccountUin    string              `json:"AccountUin"`
	Description   string              `json:"Description"`
	Permissions   []ProductPermission `json:"Permissions"`
}

// ModifyBusinessAdministratorRsp 编辑主账号响应
type ModifyBusinessAdministratorRsp struct {
	Error     *Error `json:"Error"`
	RequestID string `json:"RequestId"`
}

// ProductPermission 产品权限
type ProductPermission struct {
	ProductName    string       `json:"ProductName"`
	ProductType    string       `json:"ProductType"`
	Permissions    []Permission `json:"Permissions"`
	SubscribeDate  string       `json:"SubscribeDate,omitempty"`
	ExpireDate     string       `json:"ExpireDate,omitempty"`
	EnableValidity int64        `json:"EnableValidity,omitempty"`
}

// Permission 权限
type Permission struct {
	PermissionID    string   `json:"PermissionId"`
	PermissionName  string   `json:"PermissionName"`
	ParentID        string   `json:"ParentId"`
	Level           int64    `json:"Level"`
	ValueType       int64    `json:"ValueType"`
	IsGranted       int64    `json:"IsGranted"`
	ValueInt        int64    `json:"ValueInt"`
	ValueStringList []string `json:"ValueStringList"`
	Category        int64    `json:"Category"`
	Sort            int64    `json:"Sort"`
}

// AllPermissionID 机器人所有权限位
func AllPermissionID() string {
	return allPermissionID
}

// AllPermissionName 机器人所有权限位描述
func AllPermissionName() string {
	return allPermissionName
}

// AllResourcePermissionID 机器人所有资源权限位
func AllResourcePermissionID() string {
	return allResourcePermissionID
}

// HasAllResourcePermission 是否有全部权限
func HasAllResourcePermission(resourceID string) bool {
	return resourceID == allResourcePermissionID
}

// ListAppPermissionID 应用资源权限位
func ListAppPermissionID() string {
	return listAppPermissionID
}

// ListShareKnowledgePermissionID 共享知识库资源权限位
func ListShareKnowledgePermissionID() string {
	return listShareKnowledgePermissionID
}
