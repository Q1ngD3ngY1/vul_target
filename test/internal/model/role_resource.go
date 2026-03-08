package model

// CheckResourceByRoleNameRequest 查询服务角色使用用户资源请求
type CheckResourceByRoleNameRequest struct {
	Action        string `json:"Action"`        // 业务自己填，Action名
	Uin           int64  `json:"Uin"`           // 当前申请删除用户的主账号ownerUin
	SubAccountUin int64  `json:"SubAccountUin"` // 当前申请删除用户的主账号ownerUin
	RoleName      string `json:"RoleName"`      // 角色名
	Language      string `json:"Language"`      // 语言
	Timestamp     int64  `json:"Timestamp"`     // 时间戳
	RequestId     string `json:"RequestId"`     // 请求ID
}

// RoleResourceInfo 角色资源信息
type RoleResourceInfo struct {
	List      []string `json:"List"`      // 资源列表
	RequestId string   `json:"RequestId"` // 请求ID
}

// CheckResourceByRoleNameResponse 查询服务角色使用用户资源响应
type CheckResourceByRoleNameResponse struct {
	Response RoleResourceInfo `json:"Response"`
}
