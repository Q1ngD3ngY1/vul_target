package kb_package

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/pb-go/common"
)

// ModuleType 模块类型
type ModuleType string

// 模块类型常量
const (
	// ModuleAppVariable 应用变量模块
	ModuleAppVariable ModuleType = "AppVariables"
	// ModuleKb 知识库模块
	ModuleKb ModuleType = "Kbs"
	// ModuleKbLabel 知识库标签模块
	ModuleKbLabel ModuleType = "KbLabels"
	// ModuleKbLabelValue 知识库标签值模块
	ModuleKbLabelValue ModuleType = "KbLabelValues"
	// ModuleKbDoc 知识库文档模块
	ModuleKbDoc ModuleType = "KbDocs"
	// ModuleKbDocCategory 知识库文档分类模块
	ModuleKbDocCategory ModuleType = "KbDocCategories"
	// ModuleKbSegment 知识库文档段落模块
	ModuleKbSegment ModuleType = "KbSegments"
	// ModuleKbQaCategory 知识库QA分类模块
	ModuleKbQaCategory ModuleType = "KbQaCategories"
	// ModuleKbQa 知识库QA模块
	ModuleKbQa ModuleType = "KbQas"
	// ModuleKbDocClusterSchema schema聚合模块
	ModuleKbDocClusterSchema ModuleType = "KbSchemaClusters"
)

const (
	ExportRootPath = "/app/data/export"
	ImportRootPath = "/app/data/import"
)

// 子模块导出目录常量
const (
	ExportDirQA       = "qa"       // QA导出目录
	ExportDirCategory = "category" // 分类导出目录
	ExportDirLabel    = "label"    // 标签导出目录
	ExportDirDocument = "document" // 文档导出目录
	ExportDirSchema   = "schema"   // schema导出目录
)

// 导入导出场景
const (
	SceneAppPackage    = "AppPackage"    // 应用包导入导出场景
	SceneKBDataPackage = "KBDataPackage" // 知识库数据包导入导出场景
)

// KbMetadata 知识库元数据结构体
type KbMetadata struct {
	KnowledgeBaseId string `json:"KnowledgeBaseId"` // 知识库ID
	Name            string `json:"Name"`            // 知识库名称
	IsShared        bool   `json:"IsShared"`        // 是否为共享知识库
}

// KbMetadataIds 知识库元数据中的IDs信息
type KbMetadataIds struct {
	KbLabel         []string `json:"KbLabels,omitempty"`         // 知识标签ID列表
	KbLabelValue    []string `json:"KbLabelValues,omitempty"`    // 知识标签值ID列表
	KbQaCategory    []string `json:"KbQaCategories,omitempty"`   // 问答分类ID列表
	KbDocCategory   []string `json:"KbDocCategories,omitempty"`  // 文档分类ID列表
	KbDoc           []string `json:"KbDocs,omitempty"`           // 文档ID列表
	KbSegment       []string `json:"KbSegments,omitempty"`       // 文档切片ID列表
	KbSchemaCluster []string `json:"KbSchemaClusters,omitempty"` // 知识图谱聚类ID列表
	KbQa            []string `json:"KbQas,omitempty"`            // 问答ID列表
}

// KbIdsCollector 知识库 IDs 收集器（并发安全）
// 用于在导出过程中收集各模块的依赖 IDs，避免通过文件传递
type KbIdsCollector struct {
	mu  sync.Mutex
	ids *KbMetadataIds
}

// NewKbIdsCollector 创建新的 IDs 收集器
func NewKbIdsCollector() *KbIdsCollector {
	return &KbIdsCollector{
		ids: &KbMetadataIds{
			KbLabel:         make([]string, 0),
			KbLabelValue:    make([]string, 0),
			KbQaCategory:    make([]string, 0),
			KbDocCategory:   make([]string, 0),
			KbDoc:           make([]string, 0),
			KbSegment:       make([]string, 0),
			KbSchemaCluster: make([]string, 0),
			KbQa:            make([]string, 0),
		},
	}
}

// AddKbLabels 添加知识标签 IDs
func (c *KbIdsCollector) AddKbLabels(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbLabel = append(c.ids.KbLabel, ids...)
}

// AddKbLabelValues 添加知识标签值 IDs
func (c *KbIdsCollector) AddKbLabelValues(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbLabelValue = append(c.ids.KbLabelValue, ids...)
}

// AddKbQaCategories 添加问答分类 IDs
func (c *KbIdsCollector) AddKbQaCategories(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbQaCategory = append(c.ids.KbQaCategory, ids...)
}

// AddKbDocCategories 添加文档分类 IDs
func (c *KbIdsCollector) AddKbDocCategories(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbDocCategory = append(c.ids.KbDocCategory, ids...)
}

// AddKbDocs 添加文档 IDs
func (c *KbIdsCollector) AddKbDocs(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbDoc = append(c.ids.KbDoc, ids...)
}

// AddKbSegments 添加文档切片 IDs
func (c *KbIdsCollector) AddKbSegments(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbSegment = append(c.ids.KbSegment, ids...)
}

// AddKbSchemaClusters 添加知识图谱聚类 IDs
func (c *KbIdsCollector) AddKbSchemaClusters(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbSchemaCluster = append(c.ids.KbSchemaCluster, ids...)
}

// AddKbQas 添加问答 IDs
func (c *KbIdsCollector) AddKbQas(ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids.KbQa = append(c.ids.KbQa, ids...)
}

// GetResult 获取收集的所有 IDs
func (c *KbIdsCollector) GetResult() *KbMetadataIds {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ids
}

// KbExportInfo 知识库导出信息
type KbExportInfo struct {
	KnowledgeBaseId string         `json:"KnowledgeBaseId"` // 知识库ID
	Name            string         `json:"Name"`            // 知识库名称
	Path            string         `json:"Path"`            // 知识库包路径
	Hash            string         `json:"Hash"`            // 知识库压缩包的CRC64哈希值（十六进制字符串），用于完整性校验
	Ids             *KbMetadataIds `json:"-"`               // 内部使用，不序列化，用于收集该知识库的IDs信息
}

// KbPackageMetadata 知识库包元数据结构体
type KbPackageMetadata struct {
	KnowledgeBases []KbExportInfo `json:"KnowledgeBases"`         // 知识库列表
	ReferenceIds   *KbMetadataIds `json:"ReferenceIds,omitempty"` // 所有知识库汇总的IDs信息
}

// AppPackageMetadata 应用包元数据（应用包根目录的metadata.json）
type AppPackageMetadata struct {
	PackageId   string               `json:"PackageId"`   // 应用包ID
	PackageType string               `json:"PackageType"` // 包类型，如 "ApplicationPackage"
	Version     string               `json:"Version"`     // 应用包版本，如 "v202512311530"
	Components  AppPackageComponents `json:"Components"`  // 组件信息
	ExtraInfo   AppPackageExtraInfo  `json:"ExtraInfo"`   // 额外信息
}

// AppPackageComponents 应用包组件信息
type AppPackageComponents struct {
	App            *AppComponent            `json:"App,omitempty"`            // 应用配置组件
	KnowledgeBases *KnowledgeBasesComponent `json:"KnowledgeBases,omitempty"` // 知识库组件
}

// AppComponent 应用配置组件
type AppComponent struct {
	Path string `json:"Path"` // 应用配置路径，如 "app"
	Hash string `json:"Hash"` // 应用配置包的哈希值
}

// KnowledgeBasesComponent 知识库组件
type KnowledgeBasesComponent struct {
	Path  string              `json:"Path"`  // 知识库路径，如 "knowledge_bases"
	Items []KnowledgeBaseItem `json:"Items"` // 知识库列表
	Hash  string              `json:"Hash"`  // 知识库包的哈希值
}

// KnowledgeBaseItem 知识库项
type KnowledgeBaseItem struct {
	KnowledgeBaseId string `json:"KnowledgeBaseId"` // 知识库ID
	Path            string `json:"Path"`            // 知识库路径，如 "knowledge_bases/kb_xxx"
}

// AppPackageExtraInfo 应用包额外信息
type AppPackageExtraInfo struct {
	ExportedAt string `json:"ExportedAt"` // 导出时间，ISO 8601格式
	ExportedBy string `json:"ExportedBy"` // 导出者
}

// AppMetadata 应用配置元数据（app/metadata.json）
type AppMetadata struct {
	AppName    string                          `json:"AppName"`    // 应用名称
	AppVersion string                          `json:"AppVersion"` // 应用版本
	Modules    AppMetadataModules              `json:"Modules"`    // 模块信息
	KbInfoMap  map[string]AppKnowledgeBaseItem `json:"-"`          // 知识库信息映射表，key为KnowledgeBaseId（运行时构建，不序列化）
}

// AppMetadataModules 应用元数据模块信息
type AppMetadataModules struct {
	KnowledgeBase *AppModuleKnowledgeBase `json:"KnowledgeBase,omitempty"` // 知识库模块
}

// AppModuleKnowledgeBase 知识库模块
type AppModuleKnowledgeBase struct {
	Items []AppKnowledgeBaseItem `json:"Items"`
}

// AppKnowledgeBaseItem 知识库项
type AppKnowledgeBaseItem struct {
	KnowledgeBaseId string `json:"KnowledgeBaseId"` // 知识库ID
	Name            string `json:"Name"`            // 知识库名称
	IsShared        bool   `json:"IsShared"`        // 是否为共享知识库
}

// ExportConfig 导出配置结构体
type ExportConfig struct {
	CorpPrimaryID uint64 // 企业自增ID
	CorpBizID     uint64 // 企业业务ID
	AppBizID      uint64 // 应用ID
	AppPrimaryID  uint64 // 应用主ID
	KbID          uint64 // 知识库业务ID
	KbPrimaryID   uint64 // 知识库自增ID
	LocalPath     string // 导出的本地路径,例 /app/data/export/task_id/kb_xxx/
}

// ImportConfig 导入配置结构体
type ImportConfig struct {
	CorpPrimaryID   uint64           // 企业自增ID
	CorpBizID       uint64           // 企业业务ID
	StaffPrimaryID  uint64           // 员工自增ID
	AppBizID        uint64           // 应用ID
	AppPrimaryID    uint64           // 应用自增ID
	KbID            uint64           // 知识库业务ID
	KbPrimaryID     uint64           // 知识库自增ID
	LocalPath       string           // 导入包所在的本地路径,例 /app/data/import/task_id/kb_xxx/qa
	IDMappingConfig *IDMappingConfig // ID映射配置
}

// MappedID ID映射结构体
type MappedID struct {
	PrimaryID uint64 // 新的自增ID
	BizID     string // 新的业务ID
}

// IDMappingConfig ID映射配置结构体
type IDMappingConfig struct {
	// Modules 模块映射配置，key为模块名称，value为该模块的ID映射关系
	Modules map[ModuleType]map[string]MappedID `json:"modules"`
}

// GetMappedID 获取指定模块下映射后的ID
// module: 模块类型
// originalID: 原始ID
// 返回值：映射后的ID，如果未找到则返回空字符串
func (c *IDMappingConfig) GetMappedID(module ModuleType, originalID string) MappedID {
	if c.Modules == nil {
		return MappedID{}
	}
	if moduleMapping, exists := c.Modules[module]; exists {
		if mappedID, exists := moduleMapping[originalID]; exists {
			return mappedID
		}
	}
	return MappedID{}
}

// IsMappedIDExist 获取指定模块下映射前的ID是否存在
func (c *IDMappingConfig) IsMappedIDExist(module ModuleType, originalID string) bool {
	if c.Modules == nil {
		return false
	}
	if moduleMapping, ok := c.Modules[module]; ok {
		_, exists := moduleMapping[originalID]
		return exists
	}
	return false
}

// SetMappedID 设置指定模块下的ID映射关系
// module: 模块类型
// originalID: 原始ID
// mappedID: 映射后的ID
func (c *IDMappingConfig) SetMappedID(module ModuleType, originalID string, mappedID MappedID) {
	if c.Modules == nil {
		c.Modules = make(map[ModuleType]map[string]MappedID)
	}
	if _, exists := c.Modules[module]; !exists {
		c.Modules[module] = make(map[string]MappedID)
	}
	c.Modules[module][originalID] = mappedID
}

// GetOrGenerateBizID 获取或生成业务ID 如果 IDMappingConfig 中存在映射，返回映射的 BizID；否则生成新 ID
func (c *IDMappingConfig) GetOrGenerateBizID(module ModuleType, oldBizID uint64) uint64 {
	if c == nil {
		return idgen.GetId()
	}

	oldIDStr := strconv.FormatUint(oldBizID, 10)
	if c.IsMappedIDExist(module, oldIDStr) {
		mapped := c.GetMappedID(module, oldIDStr)
		if mapped.BizID != "" {
			newBizID, err := strconv.ParseUint(mapped.BizID, 10, 64)
			if err == nil && newBizID != 0 {
				return newBizID
			}
		}
	}

	// 不在映射中或解析失败，生成新ID
	return idgen.GetId()
}

// ConvertToPrimaryID 将旧ID转换为新的主键ID
// 返回转换后的主键ID字符串
func (c *IDMappingConfig) ConvertToPrimaryID(ctx context.Context, module ModuleType, oldID string) (uint64, error) {
	if oldID == "" || oldID == "0" {
		return 0, nil
	}

	mappedID := c.GetMappedID(module, oldID)
	if mappedID.PrimaryID != 0 {
		return mappedID.PrimaryID, nil
	}

	logx.W(ctx, "ConvertToPrimaryID mapping not found, module=%s, oldID=%s", module, oldID)
	return 0, fmt.Errorf("primary id mapping not found for module=%s, oldID=%s", module, oldID)
}

// ConvertToBizID 将旧ID转换为新的业务ID
// 返回转换后的业务ID字符串
func (c *IDMappingConfig) ConvertToBizID(ctx context.Context, module ModuleType, oldID string) (string, error) {
	if oldID == "" || oldID == "0" {
		return oldID, nil
	}

	mappedID := c.GetMappedID(module, oldID)
	if mappedID.BizID != "" {
		return mappedID.BizID, nil
	}

	logx.W(ctx, "ConvertToBizID mapping not found, module=%s, oldID=%s", module, oldID)
	return oldID, fmt.Errorf("biz id mapping not found for module=%s, oldID=%s", module, oldID)
}

// ============ 知识库配置导出相关结构体 ============

// KBConfigExport 知识库配置导出根结构
type KBConfigExport struct {
	KnowledgeBases []KBConfigItem `json:"KnowledgeBases"`
}

// KBConfigItem 单个知识库的配置项
type KBConfigItem struct {
	KnowledgeBaseId    string               `json:"KnowledgeBaseId"`
	OldKnowledgeBaseId string               `json:"-"`
	ModelConfig        *ModelConfig         `json:"ModelConfig,omitempty"`
	RetrievalConfig    *RetrievalConfigData `json:"RetrievalConfig,omitempty"`
}

// ModelConfig 模型配置
type ModelConfig struct {
	KnowledgeSchemaModel      *ModelInfo          `json:"KnowledgeSchemaModel,omitempty"`
	QaExtractModel            *ModelInfo          `json:"QaExtractModel,omitempty"`
	EmbeddingModel            *ModelInfo          `json:"EmbeddingModel,omitempty"`
	NaturalLanguageToSqlModel *ModelInfo          `json:"NaturalLanguageToSqlModel,omitempty"`
	FileParseModel            *FileParseModelInfo `json:"FileParseModel,omitempty"`
}

// LocalModelParams 本地模型参数结构体
// 用于替代 common.ModelParams，避免依赖外部 protobuf 定义
type LocalModelParams struct {
	Temperature     *float32 `json:"Temperature,omitempty"`     // 温度参数，控制生成文本的随机性
	TopP            *float32 `json:"TopP,omitempty"`            // Top-P 采样参数
	MaxTokens       *int32   `json:"MaxTokens,omitempty"`       // 最大生成 token 数
	ReplyFormat     string   `json:"ReplyFormat,omitempty"`     // 输出格式
	DeepThinking    string   `json:"DeepThinking,omitempty"`    // 是否开启深度思考
	ReasoningEffort string   `json:"ReasoningEffort,omitempty"` // 深度思考效果
}

// ModelInfo 模型信息
type ModelInfo struct {
	ModelAliasName string            `json:"ModelAliasName,omitempty"`
	HistoryLimit   int               `json:"HistoryLimit,omitempty"`
	ModelName      string            `json:"ModelName,omitempty"`
	ModelParams    *LocalModelParams `json:"ModelParams,omitempty"`
}

// FileParseModelInfo 文件解析模型信息
type FileParseModelInfo struct {
	ModelName                     string `json:"ModelName,omitempty"`
	ModelAliasName                string `json:"ModelAliasName,omitempty"`
	FormulaEnhancement            bool   `json:"FormulaEnhancement,omitempty"`
	LargeLanguageModelEnhancement bool   `json:"LargeLanguageModelEnhancement,omitempty"`
	OutputHtmlTable               bool   `json:"OutputHtmlTable,omitempty"`
}

// RetrievalConfigData 检索配置数据
type RetrievalConfigData struct {
	Filters        []FilterItem      `json:"Filters,omitempty"`
	SearchStrategy *KBSearchStrategy `json:"SearchStrategy,omitempty"`
	RetrievalRange *RetrievalRange   `json:"RetrievalRange,omitempty"`
}

// FilterItem 过滤器项
type FilterItem struct {
	RetrievalType string  `json:"RetrievalType"`
	IndexId       int     `json:"IndexId,omitempty"`
	Confidence    float64 `json:"Confidence,omitempty"`
	TopN          int     `json:"TopN,omitempty"`
	IsEnable      bool    `json:"IsEnable"`
}

// KBSearchStrategy 知识库搜索策略（导出专用）
type KBSearchStrategy struct {
	RerankModelSwitch      string `json:"RerankModelSwitch,omitempty"`
	RerankModel            string `json:"RerankModel,omitempty"`
	StrategyType           string `json:"StrategyType,omitempty"`
	EnableTableEnhancement bool   `json:"EnableTableEnhancement,omitempty"`
}

// RetrievalRange 检索范围
type RetrievalRange struct {
	ApiVarAttrInfos []KBApiVarAttrInfo `json:"ApiVarAttrInfos,omitempty"`
	Condition       string             `json:"Condition,omitempty"`
}

// KBApiVarAttrInfo API变量属性信息（导出专用）
type KBApiVarAttrInfo struct {
	ApiVarId             string `json:"ApiVarId,omitempty"`
	KnowledgeItemLabelId uint64 `json:"KnowledgeItemLabelId,omitempty"`
}

// ConvertToLocalModelParams 将 common.ModelParams 转换为 kb_package.LocalModelParams
// 用于导出知识库配置时，将 protobuf 定义的 ModelParams 转换为本地结构体
// 参数 src: 源 common.ModelParams 对象
// 返回值: 转换后的 kb_package.LocalModelParams 对象
func ConvertToLocalModelParams(src *common.ModelParams) *LocalModelParams {
	if src == nil {
		return nil
	}
	return &LocalModelParams{
		Temperature:     src.Temperature,
		TopP:            src.TopP,
		MaxTokens:       src.MaxTokens,
		ReplyFormat:     src.ReplyFormat,
		DeepThinking:    src.DeepThinking,
		ReasoningEffort: src.ReasoningEffort,
	}
}

// ConvertFromLocalModelParams 将 kb_package.LocalModelParams 转换为 common.ModelParams
// 用于导入知识库配置时，将本地结构体转换为 protobuf 定义的 ModelParams
// 参数 src: 源 kb_package.LocalModelParams 对象
// 返回值: 转换后的 common.ModelParams 对象
func ConvertFromLocalModelParams(src *LocalModelParams) *common.ModelParams {
	if src == nil {
		return nil
	}
	return &common.ModelParams{
		Temperature:     src.Temperature,
		TopP:            src.TopP,
		MaxTokens:       src.MaxTokens,
		ReplyFormat:     src.ReplyFormat,
		DeepThinking:    src.DeepThinking,
		ReasoningEffort: src.ReasoningEffort,
	}
}
