package model

import "fmt"

// 公用权限位
const (
	// ProductType 云图产品名称
	ProductType = "lke"

	// listAppPermissionID 应用资源权限位
	listAppPermissionID = "lkeApp.listApp"
	// lkeAppModifyAppQuota 修改应用配额
	lkeAppModifyAppQuota = "lkeApp.modifyAppQuota"

	// allPermissionID 全部权限
	allPermissionID = "*"
	// allPermissionName 全部
	allPermissionName = "全部"
	// allResourcePermissionID 拥有所有资源权限
	allResourcePermissionID = "*"

	// PermissionAllow 有权限
	PermissionAllow = 1
	// PermissionDeny 无权限
	PermissionDeny                 = 2
	listShareKnowledgePermissionID = "lkeShareKG.listShareKG"
)

// 知识问答类应用配置权限
const (
	// lkeRepositoryConfigBasic 基础设置权限
	lkeRepositoryConfigBasic = "lkeRepository.config.basic"
	// lkeRepositoryConfigKnowledge 知识配置
	lkeRepositoryConfigKnowledge = "lkeRepository.config.knowledge"
	// lkeRepositoryConfigKnowledgeDoc 文档配置
	lkeRepositoryConfigKnowledgeDoc = "lkeRepository.config.knowledge.doc"
	// lkeRepositoryConfigKnowledgeQa 问答配置
	lkeRepositoryConfigKnowledgeQa = "lkeRepository.config.knowledge.qa"
	// lkeRepositoryConfigKnowledgeQaTopN 问答配置-最大召回数量
	lkeRepositoryConfigKnowledgeQaTopN = "lkeRepository.config.knowledge.qaTopN"
	// lkeRepositoryConfigKnowledgeReplyFlexibility 问答配置-回复灵活度
	lkeRepositoryConfigKnowledgeReplyFlexibility = "lkeRepository.config.knowledge.replyFlexibility"
	// lkeRepositoryConfigKnowledgeTaskFlow 任务流程配置
	lkeRepositoryConfigKnowledgeTaskFlow = "lkeRepository.config.knowledge.taskFlow"
	// lkeRepositoryConfigKnowledgeSearchEngine 搜索增强
	lkeRepositoryConfigKnowledgeSearchEngine = "lkeRepository.config.knowledge.searchEngine"
	// lkeRepositoryConfigModel 模型配置
	lkeRepositoryConfigModel = "lkeRepository.config.model"
	// lkeRepositoryConfigModelVersion 模型版本
	lkeRepositoryConfigModelVersion = "lkeRepository.config.model.version"
	// lkeRepositoryConfigModelHistoryLimit 上下文指代轮次
	lkeRepositoryConfigModelHistoryLimit = "lkeRepository.config.model.historyLimit"
	// lkeRepositoryConfigOutput 输出配置
	lkeRepositoryConfigOutput = "lkeRepository.config.output"
	// lkeRepositoryConfigOutputGeneralKnowledge 通用模型回复
	lkeRepositoryConfigOutputGeneralKnowledge = "lkeRepository.config.output.generalKnowledge"
	// lkeRepositoryConfigOutputMethod 输出方式
	lkeRepositoryConfigOutputMethod = "lkeRepository.config.output.method"
	// lkeRepositoryConfigDocSplit 切分配置
	lkeRepositoryConfigDocSplit = "lkeRepository.config.docsplit"
	// lkeRepositoryConfigSearchVector 相似度配置
	lkeRepositoryConfigSearchVector = "lkeRepository.config.searchvector"
)

// 知识库摘要应用配置权限
const (
	// lkeSummaryConfigBasic 基础设置
	lkeSummaryConfigBasic = "lkeSummary.config.basic"
	// lkeSummaryConfigModelVersion 模型版本
	lkeSummaryConfigModelVersion = "lkeSummary.config.model.version"
	// lkeSummaryConfigOutputRequirement 输出要求
	lkeSummaryConfigOutputRequirement = "lkeSummary.config.output.requirement"
	// lkeSummaryConfigOutputMethod 输出方式
	lkeSummaryConfigOutputMethod = "lkeSummary.config.output.method"
)

// 标签分类应用配置权限
const (
	// lkeClassifyConfigBasic 基础设置
	lkeClassifyConfigBasic = "lkeClassify.config.basic"
	// lkeClassifyConfigModelVersion 模型版本
	lkeClassifyConfigModelVersion = "lkeClassify.config.model.version"
	// lkeClassifyConfigTag 标签配置
	lkeClassifyConfigTag = "lkeClassify.config.tag"
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

// HasAllPermission 是否有全部权限
func HasAllPermission(permissionID string) bool {
	return permissionID == allPermissionID
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

// HasListAppPermission 是否有应用资源权限位
func HasListAppPermission(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == ListAppPermissionID()
}

// LkeAppModifyAppQuota 修改应用配额
func LkeAppModifyAppQuota() string {
	return lkeAppModifyAppQuota
}

// GenAppSecretPermissionID 生成应用密钥
func GenAppSecretPermissionID(appPermissionID string) string {
	return fmt.Sprintf("%s.release.call.secret", appPermissionID)
}

// HasAppSecretPermission 是否有生成应用密钥
func HasAppSecretPermission(permissionID, appPermissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == GenAppSecretPermissionID(appPermissionID)
}

// GenAppConfigBasicPermissionID 生成应用基础权限ID
func GenAppConfigBasicPermissionID(appPermissionID string) string {
	return fmt.Sprintf("%s.config.basic", appPermissionID)
}

// HasAppConfigBasicPermission 是否有应用基础权限ID
func HasAppConfigBasicPermission(permissionID, appPermissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == GenAppConfigBasicPermissionID(appPermissionID)
}

// -------------------------------------知识库问答应用--------------------------------------------------------------------

// LkeRepositoryConfigBasic 基础设置权限
func LkeRepositoryConfigBasic() string {
	return lkeRepositoryConfigBasic
}

// HasLkeRepositoryConfigBasic 是否有基础设置权限
func HasLkeRepositoryConfigBasic(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigBasic()
}

// LkeRepositoryConfigKnowledge 知识配置
func LkeRepositoryConfigKnowledge() string {
	return lkeRepositoryConfigKnowledge
}

// LkeRepositoryConfigKnowledgeDoc 文档配置
func LkeRepositoryConfigKnowledgeDoc() string {
	return lkeRepositoryConfigKnowledgeDoc
}

// HasLkeRepositoryConfigKnowledgeDoc 是否有文档配置权限
func HasLkeRepositoryConfigKnowledgeDoc(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigKnowledgeDoc()
}

// LkeRepositoryConfigKnowledgeQa 问答配置
func LkeRepositoryConfigKnowledgeQa() string {
	return lkeRepositoryConfigKnowledgeQa
}

// HasLkeRepositoryConfigKnowledgeQa 是否有问答配置权限
func HasLkeRepositoryConfigKnowledgeQa(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigKnowledgeQa()
}

// LkeRepositoryConfigKnowledgeQaTopN 问答配置-最大召回数量
func LkeRepositoryConfigKnowledgeQaTopN() string {
	return lkeRepositoryConfigKnowledgeQaTopN
}

// HasLkeRepositoryConfigKnowledgeQaTopN 是否有问答配置-最大召回数量权限
func HasLkeRepositoryConfigKnowledgeQaTopN(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigKnowledgeQaTopN()
}

// LkeRepositoryConfigKnowledgeReplyFlexibility 问答配置-回复灵活度
func LkeRepositoryConfigKnowledgeReplyFlexibility() string {
	return lkeRepositoryConfigKnowledgeReplyFlexibility
}

// HasLkeRepositoryConfigKnowledgeReplyFlexibility 是否有问答配置-回复灵活度权限
func HasLkeRepositoryConfigKnowledgeReplyFlexibility(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigKnowledgeReplyFlexibility()
}

// LkeRepositoryConfigKnowledgeTaskFlow 任务流程配置
func LkeRepositoryConfigKnowledgeTaskFlow() string {
	return lkeRepositoryConfigKnowledgeTaskFlow
}

// HasLkeRepositoryConfigKnowledgeTaskFlow 是否有任务流程配置权限
func HasLkeRepositoryConfigKnowledgeTaskFlow(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigKnowledgeTaskFlow()
}

// LkeRepositoryConfigKnowledgeSearchEngine 搜索增强
func LkeRepositoryConfigKnowledgeSearchEngine() string {
	return lkeRepositoryConfigKnowledgeSearchEngine
}

// HasLkeRepositoryConfigKnowledgeSearchEngine 是否有搜索增强权限
func HasLkeRepositoryConfigKnowledgeSearchEngine(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigKnowledgeSearchEngine()
}

// LkeRepositoryConfigModel 模型配置
func LkeRepositoryConfigModel() string {
	return lkeRepositoryConfigModel
}

// LkeRepositoryConfigModelVersion 模型版本
func LkeRepositoryConfigModelVersion() string {
	return lkeRepositoryConfigModelVersion
}

// HasLkeRepositoryConfigModelVersion 是否有模型版本权限
func HasLkeRepositoryConfigModelVersion(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigModelVersion()
}

// LkeRepositoryConfigModelHistoryLimit 上下文指代轮次
func LkeRepositoryConfigModelHistoryLimit() string {
	return lkeRepositoryConfigModelHistoryLimit
}

// HasLkeRepositoryConfigModelHistoryLimit 是否有上下文指代轮次权限
func HasLkeRepositoryConfigModelHistoryLimit(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigModelHistoryLimit()
}

// LkeRepositoryConfigOutput 输出配置
func LkeRepositoryConfigOutput() string {
	return lkeRepositoryConfigOutput
}

// LkeRepositoryConfigOutputGeneralKnowledge 通用模型回复
func LkeRepositoryConfigOutputGeneralKnowledge() string {
	return lkeRepositoryConfigOutputGeneralKnowledge
}

// HasLkeRepositoryConfigOutputGeneralKnowledge 是否有通用模型回复权限
func HasLkeRepositoryConfigOutputGeneralKnowledge(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigOutputGeneralKnowledge()
}

// LkeRepositoryConfigOutputMethod 输出方式
func LkeRepositoryConfigOutputMethod() string {
	return lkeRepositoryConfigOutputMethod
}

// HasLkeRepositoryConfigOutputMethod 是否有输出方式权限
func HasLkeRepositoryConfigOutputMethod(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeRepositoryConfigOutputMethod()
}

// LkeRepositoryConfigDocSplit 切分配置
func LkeRepositoryConfigDocSplit() string {
	return lkeRepositoryConfigDocSplit
}

// LkeRepositoryConfigSearchVector 相似度配置
func LkeRepositoryConfigSearchVector() string {
	return lkeRepositoryConfigSearchVector
}

// -------------------------------------知识库摘要应用--------------------------------------------------------------------

// LkeSummaryConfigBasic 基础设置
func LkeSummaryConfigBasic() string {
	return lkeSummaryConfigBasic
}

// HasLkeSummaryConfigBasic 是否有基础设置权限
func HasLkeSummaryConfigBasic(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeSummaryConfigBasic()
}

// LkeSummaryConfigModelVersion 模型版本
func LkeSummaryConfigModelVersion() string {
	return lkeSummaryConfigModelVersion
}

// HasLkeSummaryConfigModelVersion 是否有模型版本权限
func HasLkeSummaryConfigModelVersion(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeSummaryConfigModelVersion()
}

// LkeSummaryConfigOutputRequirement 输出要求
func LkeSummaryConfigOutputRequirement() string {
	return lkeSummaryConfigOutputRequirement
}

// HasLkeSummaryConfigOutputRequirement 是否有输出要求权限
func HasLkeSummaryConfigOutputRequirement(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeSummaryConfigOutputRequirement()
}

// LkeSummaryConfigOutputMethod 输出方式
func LkeSummaryConfigOutputMethod() string {
	return lkeSummaryConfigOutputMethod
}

// HasLkeSummaryConfigOutputMethod 是否有输出方式权限
func HasLkeSummaryConfigOutputMethod(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeSummaryConfigOutputMethod()
}

// -------------------------------------标签分类应用----------------------------------------------------------------------

// LkeClassifyConfigBasic 基础设置
func LkeClassifyConfigBasic() string {
	return lkeClassifyConfigBasic
}

// HasLkeClassifyConfigBasic 是否有基础设置权限
func HasLkeClassifyConfigBasic(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeClassifyConfigBasic()
}

// LkeClassifyConfigModelVersion 模型版本
func LkeClassifyConfigModelVersion() string {
	return lkeClassifyConfigModelVersion
}

// HasLkeClassifyConfigModelVersion 是否有模型版本权限
func HasLkeClassifyConfigModelVersion(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeClassifyConfigModelVersion()
}

// LkeClassifyConfigTag 标签配置
func LkeClassifyConfigTag() string {
	return lkeClassifyConfigTag
}

// HasLkeClassifyConfigTag 是否有标签配置权限
func HasLkeClassifyConfigTag(permissionID string) bool {
	return HasAllPermission(permissionID) || permissionID == LkeClassifyConfigTag()
}
