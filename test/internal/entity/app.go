package entity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"github.com/google/go-cmp/cmp"
)

const (
	// AppStatusInit 未上线
	AppStatusInit = 1
	// AppStatusRunning 运行中
	AppStatusRunning = 2
	// AppStatusDeactivate 停用
	AppStatusDeactivate = 3

	// AppTestScenes 测试场景
	AppTestScenes = 1
	// AppReleaseScenes 正式场景
	AppReleaseScenes = 2

	// AppUseDownloadURL 预览使用下载链接地址
	AppUseDownloadURL = uint8(1)
	// IsCreateVectorIndexDone 已创建用户向量库
	IsCreateVectorIndexDone = 1
	// IsCreateVectorIndexToDo = 待创建用户向量库
	IsCreateVectorIndexToDo = 0

	// QAExtractModel 问答提取模型
	QAExtractModel = "qa_extract"
	// GenerateSimilarQAModel 相似问生成模型
	GenerateSimilarQAModel = "generate_similar_qa"
	// KbSchemaDocSummaryModel 知识库schema文档摘要模型
	KbSchemaDocSummaryModel = "kb_schema_doc_summary"
	// KbSchemaDirSummaryModel 知识库schema目录摘要模型
	KbSchemaDirSummaryModel = "kb_schema_dir_summary"
)

const (
	AppStandardPattern       = "standard"        // 标准模式
	AppAgentPattern          = "agent"           // agent模式
	AppSingleWorkflowPattern = "single_workflow" // 单工作流模式

	ModelCategoryGenerate  = "generate"  // 生成模型
	ModelCategoryThought   = "thought"   // 思考模型
	ModelCategoryEmbedding = "embedding" // embedding模型
	ModelCategoryRerank    = "rerank"    // rerank模型
	ModelCategoryFileParse = "fileparse" // 文件解析模型
)

// 知识配置定义
const (
	// QaFilterType 问答检索类型
	QaFilterType = 1
	// DocFilterType 文档检索类型
	DocFilterType = 2
	// RejectedFilterType 拒答检索类型
	RejectedFilterType = 3
	// SearchFilterType 搜索增强检索类型
	SearchFilterType = 4
	// TaskFlowFilterType 任务型检索类型
	TaskFlowFilterType = 5
	// ImageToImageFilterType 图搜图检索类型
	ImageToImageFilterType = 6
	// TextToImageFilterType 文搜图检索类型
	TextToImageFilterType = 7
	// DBFilterType 数据库检索类型
	DBFilterType = 8

	// AppMethodStream .
	AppMethodStream = 1
	// AppDirectReply .
	AppDirectReply = 1
	// AppRequirementSummary .
	AppRequirementSummary = 1

	// ConfigItemName 配置项-应用名称
	ConfigItemName = "应用名称"
	// ConfigItemAvatar 配置项-应用图标
	ConfigItemAvatar = "头像"
	// ConfigItemDescription 配置项-应用描述
	ConfigItemDescription = "应用描述"
	// ConfigItemGreeting 配置项-欢迎语
	ConfigItemGreeting = "欢迎语"
	// ConfigItemRoleDescription 配置项-角色描述
	ConfigItemRoleDescription = "角色描述"
	// ConfigItemMethod 配置项-输出方式
	ConfigItemMethod = "输出方式"
	// ConfigItemUseGeneralKnowledge 配置项-保守回复开关
	ConfigItemUseGeneralKnowledge = "保守回复开关"
	// ConfigItemBareAnswer 配置项-默认问题回复
	ConfigItemBareAnswer = "未知问题回复语"
	// ConfigItemTopN 配置项-最大召回数量
	ConfigItemTopN = "%s最大召回数量"
	// ConfigItemReplyFlexibility 配置项-回复灵活度
	ConfigItemReplyFlexibility = "回复灵活度"
	// ConfigItemIsEnabled 配置项-配置开关
	ConfigItemIsEnabled = "知识来源%s配置"
	// ConfigItemShowSearchEngine 配置项-搜索引擎展示状态
	ConfigItemShowSearchEngine = "搜索引擎状态"
	// ConfigItemHistoryLimit 配置项-上下文指代轮次
	ConfigItemHistoryLimit = "上下文指代轮次"
	// ConfigItemModel 配置项-模型名称
	ConfigItemModel = "模型选择"
	// ConfigItemLabel  配置项-标签配置
	ConfigItemLabel = "标签配置"
	// ConfigItemRequirement  输出要求
	ConfigItemRequirement = "输出要求"
	// ConfigItemRequirementCommand  自定义输出要求
	ConfigItemRequirementCommand = "自定义输出要求"

	// DirectReply 已采纳答案直接回复
	DirectReply = "已采纳答案直接回复"
	// NotDirectReply 已采纳答案润色回复
	NotDirectReply = "已采纳答案润色回复"
	// TextSummary 文本总结
	TextSummary = "文本总结"
	// TextCustom 自定义要求
	TextCustom = "自定义要求"

	// Stream 流式输出
	Stream = "流式输出"
	// NotStream 非流式输出
	NotStream = "非流式输出"

	// Open 开启
	Open = "开启"
	// ShutDown 关闭
	ShutDown = "关闭"
	// ShowOn 显示
	ShowOn = "显示"
	// ShowOff 隐藏
	ShowOff = "隐藏"
	// AvatarChange 头像变更
	AvatarChange = "头像已变更"

	qaFilterType       = "问答"
	docFilterType      = "文档"
	rejectedFilterType = "拒答"
	searchFilterType   = "搜索增强"
	taskFlowFilterType = "任务流程"
)

const (
	// AppModelNormal 知识问答库对话策略
	AppModelNormal = "normal_message"
	// AppModelNormalNonGeneralKnowledge 知识问答库对话策略
	AppModelNormalNonGeneralKnowledge = "normal_message_non_general_knowledge"
	// AppModelQueryRewrite 知识问答库改写策略
	AppModelQueryRewrite = "query_rewrite"

	// RerankModel rerank 模型
	RerankModel = "rerank"
)

const (
	// KnowledgeQaAppType 知识问答应用类型
	KnowledgeQaAppType = "knowledge_qa"

	// RetrievalConfigKey 检索配置redisKey
	RetrievalConfigKey = "retrieval:config:%d"
	// DefaultConfigRobotID 默认配置的机器人ID
	DefaultConfigRobotID = 0
)

var (
	// AppSearchPreviewFilterKey .
	AppSearchPreviewFilterKey = "search_preview"
	// AppSearchReleaseFilterKey .
	AppSearchReleaseFilterKey = "search_release"
	// AppPreviewQuestionFilterKey 评测问答对 (可以直接回复)
	AppPreviewQuestionFilterKey = "search_preview_question"
	// AppReleaseQuestionFilterKey 正式问答对 (可以直接回复)
	AppReleaseQuestionFilterKey = "search_release_question"

	// AppSearchRealtimePreviewFilterKey 实时文档评测库检索FilterKey
	AppSearchRealtimePreviewFilterKey = "search_realtime_preview"
	// AppSearchRealtimeReleaseFilterKey 实时文档正式库检索FilterKey
	AppSearchRealtimeReleaseFilterKey = "search_realtime_release"

	// AppMatchReferFilterKey .
	AppMatchReferFilterKey = "match_refer"
	// AppSearchRejectedQuestionPreview .
	AppSearchRejectedQuestionPreview = "search_rejected_question_preview"
	// AppSearchRejectedQuestionRelease .
	AppSearchRejectedQuestionRelease = "search_rejected_question_release"
	// AppSearchGlobalFilterKey .
	AppSearchGlobalFilterKey = "search_global"
	// AppSearchEngineFilterKey .
	AppSearchEngineFilterKey = "search_engine"
	// AppRerankFilterKey .
	AppRerankFilterKey = "rerank"
)

const (
	// AppSearchConditionAnd 用用检索范围条件 and
	AppSearchConditionAnd = "and"
	// AppSearchConditionOr 用用检索范围条件 or
	AppSearchConditionOr = "or"
	// CustomVariableSplitSep 自定参数标签场景的分隔符
	CustomVariableSplitSep = "|"
)

// PreviewFilterKeys 评测环境的知识配置策略集合
var PreviewFilterKeys = map[string]struct{}{
	AppSearchPreviewFilterKey:        {},
	AppPreviewQuestionFilterKey:      {},
	AppSearchRejectedQuestionPreview: {},
}

// ReleaseFilterKeys 发布环境的知识配置策略集合
var ReleaseFilterKeys = map[string]struct{}{
	AppSearchReleaseFilterKey:        {},
	AppReleaseQuestionFilterKey:      {},
	AppSearchRejectedQuestionRelease: {},
}

// FilterKeyPairs 评测和发布知识配置策略Pairs
var FilterKeyPairs = map[string]string{
	AppSearchPreviewFilterKey:        AppSearchReleaseFilterKey,
	AppSearchReleaseFilterKey:        AppSearchPreviewFilterKey,
	AppPreviewQuestionFilterKey:      AppReleaseQuestionFilterKey,
	AppReleaseQuestionFilterKey:      AppPreviewQuestionFilterKey,
	AppSearchRejectedQuestionPreview: AppSearchRejectedQuestionRelease,
	AppSearchRejectedQuestionRelease: AppSearchRejectedQuestionPreview,
}

type SimpleApp struct {
	id AppID
	// 应用ID 向量接口的appkey使用该字段
	appBizID uint64
	online   bool
	docTypes map[DocType]struct{}
}

func NewSimpleApp(appID AppID, botBizID uint64) *SimpleApp {
	r := &SimpleApp{
		id:       appID,
		appBizID: botBizID,
		// 加上图片类型
		docTypes: map[DocType]struct{}{},
		// docTypes: map[DocType]struct{}{DocTypeQA: {}, DocTypeSegment: {}, DocTypeRejectedQuestion: {},
		//	DocTypeImage: {}},
		online: true,
	}
	return r
}

func (r *SimpleApp) WithDocType(docTypes []DocType) {
	// r.docTypes = make(map[DocType]struct{})
	for _, docType := range docTypes {
		if _, ok := r.docTypes[docType]; ok {
			continue
		}
		r.docTypes[docType] = struct{}{}
	}
}

// ID
func (r *SimpleApp) ID() AppID {
	return r.id
}

// 应用id
func (r *SimpleApp) AppBizID() uint64 {
	return r.appBizID
}

// DocTypes 应用所有文档类型
func (r *SimpleApp) DocTypes() []DocType {
	var docTypes []DocType
	for docType := range r.docTypes {
		docTypes = append(docTypes, docType)
	}
	return docTypes
}

type App struct {
	PrimaryId              uint64
	BizId                  uint64                 // 对外关联的机器人ID
	AppKey                 string                 // 访问端密钥
	CorpPrimaryId          uint64                 // 企业 ID
	CorpBizId              uint64                 // 企业业务ID
	AppType                string                 // 应用类型
	AppStatus              uint32                 // 应用状态
	Name                   string                 // 昵称
	NameInAudit            string                 // 审核中昵称
	Avatar                 string                 // 昵称
	AvatarInAudit          string                 // 审核中头像
	Description            string                 // 描述
	RoleDescription        string                 // 机器人描述(prompt 场景使用)
	RoleDescriptionInAudit string                 // 审核中机器人描述(prompt 场景使用)
	Greeting               string                 // 欢迎语
	GreetingInAudit        string                 // 审核中欢迎语
	Embedding              *config.RobotEmbedding // embedding 配置
	QaVersion              uint64                 // 问答库版本
	UsedCharSize           uint64                 // 机器人已经使用的字符数
	IsDeleted              bool                   // 是否已删除
	BareAnswer             string                 // 未知问题回复语
	BareAnswerInAudit      string                 // 审核中未知问题回复语
	CreateTime             time.Time              // 创建时间
	UpdateTime             time.Time              // 更新时间
	StaffID                uint64                 // 操作人
	InfosecBizType         string                 // 安全审核策略
	IsShared               bool                   // 是否共享知识库应用
	Uin                    string                 // Uin
	QaConfig               *KnowledgeQaConfig     // 问答库配置详情
	SpaceId                string
	ShareKnowledgeBases    map[uint64]*App // 共享知识库
	IsExpCenter            bool            // 是否是体验中心应用
}

// AppBaseInfo 应用基础信息
// 参考：app 的 pb.AppBaseInfo 结构体
type AppBaseInfo struct {
	CorpPrimaryId uint64
	PrimaryId     uint64
	BizId         uint64
	Name          string
	SpaceId       string
	IsExpCenter   bool
	IsShared      bool
	Uin           string
	UsedCharSize  uint64
	QaVersion     uint64
}

// RerankModelConfig .
type RerankModelConfig struct {
	ModelName string // rerank 模型
	TopN      uint32 // 进 rerank 之前取的 topN 数量
	Enable    bool   // 启用 rerank
}

type RetrievalConfig struct {
	ID                 uint64    `json:"-"`
	RobotID            uint64    `json:"-"`
	EnableEsRecall     bool      `json:"enable_es_recall"`     // enable_es_recall
	EnableVectorRecall bool      `json:"enable_vector_recall"` // enable_vector_recall
	EnableRrf          bool      `json:"enable_rrf"`           // enable_rrf
	EnableText2Sql     bool      `json:"enable_text2sql"`      // enable_text2sql
	ReRankThreshold    float32   `json:"rerank_threshold"`     // rerank分数阈值 默认-6.0
	RRFVecWeight       float32   `json:"rrf_vec_weight"`       // rrf向量排序的权重 默认2
	RRFEsWeight        float32   `json:"rrf_es_weight"`        // rrf es排序的权重 默认2
	RRFReRankWeight    float32   `json:"rrf_rerank_weight"`    // rrf rerank排序的权重 默认3
	DocVecRecallNum    uint32    `json:"doc_vec_recall_num"`   // 文档向量召回数量 默认20
	QaVecRecallNum     uint32    `json:"qa_vec_recall_num"`    // 问答向量召回数量 默认20
	EsRecallNum        uint32    `json:"es_recall_num"`        // es召回数量 默认20
	EsReRankMinNum     uint32    `json:"es_rerank_min_num"`    // 召回数量超过时 es做rerank的数量 默认7
	RRFReciprocalConst uint32    `json:"rrf_reciprocal_const"` // rrf倒数常量 默认1
	Operator           string    `json:"-"`                    // 操作人
	CreateTime         time.Time `json:"-"`                    // 创建时间
	UpdateTime         time.Time `json:"-"`                    // 更新时间
	EsTopN             uint32    `json:"es_top_n"`             // 表格召回数量
	Text2sqlModel      string    `json:"text2sql_model"`       // text2sql模型名称
	Text2sqlPrompt     string    `json:"text2sql_prompt"`      // text2sql prompt
}

// IsCheckRetrievalConfig 检查检索配置,向量召回和关键词召回不可同时关闭
func (req *RetrievalConfig) IsCheckRetrievalConfig() bool {
	if !req.EnableVectorRecall && !req.EnableEsRecall {
		return false
	}
	return true
}

// CheckRetrievalConfigDiff 检查检索配置是否相同
func (req *RetrievalConfig) CheckRetrievalConfigDiff(retrievalConfig *RetrievalConfig) bool {
	return cmp.Equal(req, retrievalConfig)
}

// GetRetrievalConfigKey 组合检索配置redisKey
func GetRetrievalConfigKey(robotID uint64) string {
	return fmt.Sprintf(RetrievalConfigKey, robotID)
}

// BaseConfig 基础应用配置
type BaseConfig struct {
	Name        string `json:"name"`
	Avatar      string `json:"avatar"`
	Description string `json:"description"`
}

// AppDetailsConfig 应用详情配置
type AppDetailsConfig struct {
	BaseConfig BaseConfig `json:"base_config"`
	AppConfig  AppConfig  `json:"app_config"`
}

// ToJSON AppDetailsConfig To string
func (c *AppDetailsConfig) ToJSON() string {
	if len(c.BaseConfig.Name) == 0 {
		return ""
	}
	return Marshal2StringUnescapeHTMLNoErr(c)
}

// AppModelDetail 模型详情配置
type AppModelDetail struct {
	HistoryLimit      uint32 `json:"history_limit"`
	HistoryWordsLimit uint32 `json:"history_words_limit"`
	IsEnabled         bool   `json:"is_enabled"`
	ModelName         string `json:"model_name"`
	Prompt            string `json:"prompt"`
	PromptWordsLimit  uint32 `json:"prompt_words_limit"`
	Path              string `json:"path"`
	Target            string `json:"target"`
	Type              uint32 `json:"type"`
	ServiceName       string `yaml:"service_name" json:"service_name"`
}

// SearchFilters 知识管理检索配置
type SearchFilters struct {
	DocType          uint32 `json:"doc_type"`
	TopN             uint32 `json:"top_n,omitempty"`
	ReplyFlexibility uint32 `json:"reply_flexibility"`
	IsEnabled        bool   `json:"is_enabled"`
	UseSearchEngine  bool   `json:"use_search_engine"`
	ShowSearchEngine bool   `json:"show_search_engine"`
}

// SearchRange 检索范围
type SearchRange struct {
	Condition       string            `json:"condition"`
	APIVarMap       map[string]string `json:"api_var_map"`
	LabelAttrMap    map[uint64]string `json:"label_attr_map"`
	ApiVarAttrInfos []ApiVarAttrInfo  `json:"api_var_attr_infos"`
}

type ApiVarAttrInfo struct {
	ApiVarID  string `json:"api_var_id"`
	AttrBizID uint64 `json:"attr_biz_id"`
}

type SearchStrategy struct {
	StrategyType      uint32 // 检索策略类型 0:混合检索，1：语义检索
	TableEnhancement  bool   // excel检索增强，默认关闭
	EmbeddingModel    string // embedding模型，应用知识库可修改，共享知识库不允许修改仅透传；应用知识库修改后需要同步knowledge
	RerankModelSwitch string // reRank模型开关  off-关闭  on-打开
	RerankModel       string // reRank模型
}

// KnowledgeQaConfig 知识问答应用配置
type KnowledgeQaConfig struct {
	Greeting                string
	RoleDescription         string
	Method                  uint32
	UseGeneralKnowledge     bool
	BareAnswer              string
	ReplyFlexibility        uint32
	UseSearchEngine         bool
	ShowSearchEngine        bool
	Model                   config.AppModel
	Filters                 config.RobotFilters
	UseQuestionClarify      bool         // 是否使用问题澄清
	QuestionClarifyKeywords []string     // 问题澄清关键词
	SearchRange             *SearchRange // 检索范围
	SearchStrategy          *SearchStrategy
	Text2sql                bool // 是否开启表格增强
	EnableRerank            bool
	AdvancedConfig          *AdvancedConfig // 高级配置
}

type AdvancedConfig struct {
	RerankModel     string
	RerankRecallNum int32
}

// Equals 获取结构体差异
func (k *KnowledgeQaConfig) Equals(cfg *KnowledgeQaConfig) []AppConfigDiff {
	var diff []AppConfigDiff
	if k.Greeting != cfg.Greeting {
		diff = append(diff, newDiffConfig(ConfigItemGreeting, k.Greeting, cfg.Greeting, ""))
	}
	if k.RoleDescription != cfg.RoleDescription {
		diff = append(diff, newDiffConfig(ConfigItemRoleDescription, k.RoleDescription, cfg.RoleDescription, ""))
	}
	if k.Method != cfg.Method {
		lastMethod := gox.IfElse(k.Method == AppMethodStream, Stream, NotStream)
		method := gox.IfElse(cfg.Method == AppMethodStream, Stream, NotStream)
		diff = append(diff, newDiffConfig(ConfigItemMethod, lastMethod, method, ""))
	}
	if k.UseGeneralKnowledge != cfg.UseGeneralKnowledge {
		lastConfig := gox.IfElse(k.UseGeneralKnowledge, ShutDown, Open)
		newConfig := gox.IfElse(cfg.UseGeneralKnowledge, ShutDown, Open)
		diff = append(diff, newDiffConfig(ConfigItemUseGeneralKnowledge, lastConfig, newConfig, ""))
	}
	if k.BareAnswer != cfg.BareAnswer {
		diff = append(diff, newDiffConfig(ConfigItemBareAnswer, k.BareAnswer, cfg.BareAnswer, ""))
	}
	diff = append(diff, k.equalFilter(cfg)...)
	diff = append(diff, k.equalModel(cfg)...)
	return diff
}

// ClassifyLabels 标签详情
type ClassifyLabels struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Values      []string `json:"values"`
}

// AppConfig 应用配置
type AppConfig struct {
	KnowledgeQaConfig *KnowledgeQaConfig `json:"knowledge_qa,omitempty"`
}

// AppConfigDiff 应用配置差异项
type AppConfigDiff struct {
	ConfigItem string    `json:"config_item"` // 应用配置项名称
	Action     uint32    `json:"action"`      // 变更状态
	NewValue   string    `json:"new_value"`   // 变更后内容
	LastValue  string    `json:"last_value"`  // 变更前内容
	Content    string    `json:"content"`     // 无固定场景的变更内容
	UpdateTime time.Time `json:"update_time"` // 最后更新时间
}

// SyncRetrievalConfigFromDBReq 应用的检索配置从DB同步到redis
type SyncRetrievalConfigFromDBReq struct {
	RobotIDs       []uint64 `json:"robot_ids"`         // 机器人ID列表
	IsAllConfigApp bool     `json:"is_all_config_app"` // 所有已经配置的应用都做同步
}

// GetFiltersTypeName 获取filter 映射关系
func GetFiltersTypeName(i uint32) string {
	switch i {
	case QaFilterType:
		return qaFilterType
	case DocFilterType:
		return docFilterType
	case RejectedFilterType:
		return rejectedFilterType
	case SearchFilterType:
		return searchFilterType
	case TaskFlowFilterType:
		return taskFlowFilterType
	}
	return ""
}

func newDiffConfig(configItem string, lastValue string, value string, content string) AppConfigDiff {
	return AppConfigDiff{
		ConfigItem: configItem,
		Action:     qaEntity.NextActionUpdate,
		NewValue:   value,
		LastValue:  lastValue,
		Content:    content,
		UpdateTime: time.Now(),
	}
}

// Equals 获取结构体差异
func (f *SearchFilters) Equals(cfg *SearchFilters) []AppConfigDiff {
	var diff []AppConfigDiff
	if f.IsEnabled != cfg.IsEnabled {
		lastConfig := gox.IfElse(f.IsEnabled, Open, ShutDown)
		newConfig := gox.IfElse(cfg.IsEnabled, Open, ShutDown)
		diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemIsEnabled, GetFiltersTypeName(cfg.DocType)),
			lastConfig, newConfig, ""))
	}
	if cfg.IsEnabled {
		if f.TopN != cfg.TopN {
			diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemTopN, GetFiltersTypeName(cfg.DocType)),
				strconv.Itoa(int(f.TopN)), strconv.Itoa(int(cfg.TopN)), ""))
		}
		if f.ReplyFlexibility != cfg.ReplyFlexibility {
			lastConfig := gox.IfElse(f.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
			newConfig := gox.IfElse(cfg.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
			diff = append(diff, newDiffConfig(ConfigItemReplyFlexibility, lastConfig, newConfig, ""))
		}
		if f.ShowSearchEngine != cfg.ShowSearchEngine {
			lastConfig := gox.IfElse(f.ShowSearchEngine, ShowOn, ShowOff)
			newConfig := gox.IfElse(cfg.ShowSearchEngine, ShowOn, ShowOff)
			diff = append(diff, newDiffConfig(ConfigItemShowSearchEngine, lastConfig, newConfig, ""))
		}
	}
	return diff
}

// EqualsHistoryLimit 获取上下文轮次差异
func (m *AppModelDetail) EqualsHistoryLimit(cfg *AppModelDetail) []AppConfigDiff {
	var diff []AppConfigDiff
	if m.HistoryLimit != cfg.HistoryLimit {
		diff = append(diff, newDiffConfig(ConfigItemHistoryLimit, strconv.Itoa(int(m.HistoryLimit)),
			strconv.Itoa(int(cfg.HistoryLimit)), ""))
	}
	return diff
}

// EqualsModelName 获取模型名称差异
func (m *AppModelDetail) EqualsModelName(cfg *AppModelDetail, appType string) []AppConfigDiff {
	var diff []AppConfigDiff
	lastValue := ""
	aliasNameMap, err := GetNameToAliasNameMap(appType)
	if err != nil {
		return diff
	}
	if m.ModelName != cfg.ModelName {
		if v, ok := aliasNameMap[m.ModelName]; ok {
			lastValue = v
		}
		value, ok := aliasNameMap[cfg.ModelName]
		if !ok {
			return diff
		}
		diff = append(diff, newDiffConfig(ConfigItemModel, lastValue, value, ""))
	}
	return diff
}

// Equals 获取结构体差异
func (c *AppDetailsConfig) Equals(cfg *AppDetailsConfig) []AppConfigDiff {
	var diff []AppConfigDiff
	if c.BaseConfig.Name != cfg.BaseConfig.Name {
		diff = append(diff, newDiffConfig(ConfigItemName, c.BaseConfig.Name, cfg.BaseConfig.Name, ""))
	}
	if c.BaseConfig.Avatar != cfg.BaseConfig.Avatar {
		diff = append(diff, newDiffConfig(ConfigItemAvatar, c.BaseConfig.Avatar, cfg.BaseConfig.Avatar, AvatarChange))
	}
	return diff
}

// GetNameToAliasNameMap 通过应用名称获取别名map
func GetNameToAliasNameMap(appType string) (map[string]string, error) {
	if _, ok := config.App().RobotDefault.AppDefaultModelConfig[appType]; !ok {
		return nil, errs.ErrNotFoundModel
	}
	nameToAliasNameMap := make(map[string]string)
	for _, v := range config.App().RobotDefault.AppDefaultModelConfig[appType] {
		nameToAliasNameMap[v.Name] = v.AliasName
	}
	return nameToAliasNameMap, nil
}

// Marshal2StringUnescapeHTMLNoErr 将任意类型转换成string,html标签不被转义
func Marshal2StringUnescapeHTMLNoErr(v any) string {
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	err := jsonEncoder.Encode(v)
	if err != nil {
		return ""
	}
	if err != nil || bf.Len() == 0 {
		return ""
	}
	// encode会默认在末尾加上\n，需要去掉
	result := bf.String()
	return result[:len(result)-1]
}

// UnmarshalStr Unmarshal 字符串
func UnmarshalStr(data string, v any) error {
	d := json.NewDecoder(bytes.NewBufferString(data))
	d.UseNumber()
	return d.Decode(v)
}

func transformModelConfig(appModel config.AppModelDetail) AppModelDetail {
	return AppModelDetail{
		HistoryLimit:      appModel.HistoryLimit,
		HistoryWordsLimit: appModel.HistoryWordsLimit,
		IsEnabled:         appModel.IsEnabled,
		ModelName:         appModel.ModelName,
		Prompt:            appModel.Prompt,
		PromptWordsLimit:  appModel.PromptWordsLimit,
		Path:              appModel.Path,
		Target:            appModel.Target,
		Type:              appModel.Type,
		ServiceName:       appModel.ServiceName,
	}
}

func (k *KnowledgeQaConfig) getDiffFromAppFilter(releaseFilter, previewFilter config.RobotFilter,
	cfg *KnowledgeQaConfig) []AppConfigDiff {
	var diff []AppConfigDiff
	releaseMap := releaseFilter.ToFilterMap()
	previewMap := previewFilter.ToFilterMap()
	for key, v := range releaseMap {
		lastConfig := gox.IfElse(v.IsEnabled, Open, ShutDown)
		newConfig := gox.IfElse(previewMap[key].IsEnabled, Open, ShutDown)
		if v.IsEnabled != previewMap[key].IsEnabled {
			diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemIsEnabled, GetFiltersTypeName(v.DocType)),
				lastConfig, newConfig, ""))
		}
		if previewMap[key].IsEnabled {
			if v.TopN != previewMap[key].TopN {
				diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemTopN, GetFiltersTypeName(v.DocType)),
					strconv.Itoa(int(v.TopN)), strconv.Itoa(int(previewMap[key].TopN)), ""))
			}
			if v.DocType == QaFilterType {
				if k.ReplyFlexibility != cfg.ReplyFlexibility {
					lastConfig = gox.IfElse(k.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
					newConfig = gox.IfElse(cfg.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
					diff = append(diff, newDiffConfig(ConfigItemReplyFlexibility, lastConfig,
						newConfig, ""))
				}
			}
			if v.DocType == SearchFilterType {
				if k.ShowSearchEngine != cfg.ShowSearchEngine {
					lastConfig = gox.IfElse(k.ShowSearchEngine, ShowOn, ShowOff)
					newConfig = gox.IfElse(cfg.ShowSearchEngine, ShowOn, ShowOff)
					diff = append(diff, newDiffConfig(ConfigItemShowSearchEngine, lastConfig,
						newConfig, ""))
				}
			}
		}
	}
	return diff
}

func (k *KnowledgeQaConfig) equalFilter(cfg *KnowledgeQaConfig) []AppConfigDiff {
	var diff []AppConfigDiff
	if len(k.Filters) == 0 {
		robotFilter, ok := cfg.Filters[AppSearchPreviewFilterKey]
		if ok {
			for _, v := range robotFilter.Filter {
				if v.IsEnabled {
					diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemIsEnabled, GetFiltersTypeName(v.DocType)),
						ShutDown, Open, ""))
					if v.DocType != TaskFlowFilterType && v.DocType != SearchFilterType {
						diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemTopN, GetFiltersTypeName(v.DocType)),
							strconv.Itoa(int(v.TopN)), strconv.Itoa(int(v.TopN)), ""))
					}
					if v.DocType == QaFilterType {
						if k.ReplyFlexibility != cfg.ReplyFlexibility {
							lastConfig := gox.IfElse(k.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
							newConfig := gox.IfElse(cfg.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
							diff = append(diff, newDiffConfig(ConfigItemReplyFlexibility, lastConfig, newConfig, ""))
						}
					}
					if v.DocType == SearchFilterType {
						if k.ShowSearchEngine != cfg.ShowSearchEngine {
							lastConfig := gox.IfElse(k.ShowSearchEngine, ShowOn, ShowOff)
							newConfig := gox.IfElse(cfg.ShowSearchEngine, ShowOn, ShowOff)
							diff = append(diff, newDiffConfig(ConfigItemShowSearchEngine, lastConfig, newConfig, ""))
						}
					}
				}
			}
		}
	} else {
		releaseFilter, ok := k.Filters[AppSearchReleaseFilterKey]
		if ok {
			previewFilter, ok := cfg.Filters[AppSearchPreviewFilterKey]
			if ok {
				diff = append(diff, k.getDiffFromAppFilter(releaseFilter, previewFilter, cfg)...)
			}
		}
	}
	return diff
}

func (k *KnowledgeQaConfig) equalModel(cfg *KnowledgeQaConfig) []AppConfigDiff {
	var diff []AppConfigDiff
	if len(k.Model) == 0 {
		var releaseDetail AppModelDetail
		for key, v := range cfg.Model {
			previewDetail := transformModelConfig(v)
			if key == AppModelQueryRewrite {
				diff = append(diff, releaseDetail.EqualsHistoryLimit(&previewDetail)...)
			}
			// 是否使用行业知识库模型和通用模型名称会同时变更,此处只判断通用模型变更
			if key == AppModelNormal {
				diff = append(diff, releaseDetail.EqualsModelName(&previewDetail, KnowledgeQaAppType)...)
			}
		}
	} else {
		for key, v := range k.Model {
			modelCfg, ok := cfg.Model[key]
			if !ok {
				continue
			}
			releaseDetail := transformModelConfig(v)
			previewDetail := transformModelConfig(modelCfg)
			if key == AppModelQueryRewrite {
				diff = append(diff, releaseDetail.EqualsHistoryLimit(&previewDetail)...)
			}
			// 是否使用行业知识库模型和通用模型名称会同时变更,此处只判断通用模型变更
			if key == AppModelNormal {
				diff = append(diff, releaseDetail.EqualsModelName(&previewDetail, KnowledgeQaAppType)...)
			}
		}
	}
	return diff
}

// GetDefaultFiltersIndexID 获取默认的知识配置策略索引ID
func GetDefaultFiltersIndexID(defaultFilters config.RobotFilters, key string, docType uint32) uint32 {
	robotFilters, ok := defaultFilters[key]
	if !ok {
		return 0
	}
	for i := range robotFilters.Filter {
		if robotFilters.Filter[i].DocType == docType {
			return robotFilters.Filter[i].IndexID
		}
	}
	return 0
}

// IsWriteable 是否可写
func (a *App) IsWriteable() error {
	if a == nil {
		return errs.ErrRobotNotFound
	}
	if a.Embedding.Version != a.Embedding.UpgradeVersion {
		return errs.ErrEmbeddingUpgrading
	}
	return nil
}

// HasDeleted 是否已删除
func (a *App) HasDeleted() bool {
	if a == nil {
		return true
	}
	return a.IsDeleted
}

// AppCategory 应用类型
type AppCategory struct {
	AppType string // 应用类型
	Name    string // 应用类型名称
	Logo    string // 应用类型logo
}

const (
	MDReleasedApp = "md-kb-released-app"
	MDTestApp     = "md-kb-test-app"
)

// ContextWithApp 携带App
func ContextWithApp(ctx context.Context, scene uint32, app *App) context.Context {
	key := gox.IfElse(scene == AppReleaseScenes, MDReleasedApp, MDTestApp)
	appStr := jsonx.MustMarshal(app)
	return context.WithValue(ctx, key, appStr)
}

func AppFromContext(ctx context.Context, scene uint32) *App {
	key := gox.IfElse(scene == AppReleaseScenes, MDReleasedApp, MDTestApp)
	appStr, ok := ctx.Value(key).([]byte)
	if !ok || len(appStr) == 0 {
		return nil
	}
	var app App
	if err := jsonx.Unmarshal(appStr, &app); err != nil {
		logx.E(ctx, "AppFromContext UnmarshalFromString:%s err:%v", string(appStr), err)
		return nil
	}
	logx.I(ctx, "AppFromContext app:%+v", app)
	return &app
}

// CapacityUsage 容量使用情况
type CapacityUsage struct {
	CharSize          int64 // 使用字符数
	StorageCapacity   int64 // 用于计费-使用存储容量（目前只算cos）即第三方cos不应该计算
	ComputeCapacity   int64 // 用于计费-使用计算容量（db+vdb+es）即后续如果使用第三方es, 就不应该计算
	KnowledgeCapacity int64 // 用于统计报表-知识库使用容量
}
