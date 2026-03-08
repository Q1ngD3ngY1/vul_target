package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/protobuf/testing/protocmp"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	// AppStatusInit 未上线
	AppStatusInit = 1
	// AppStatusRunning 运行中
	AppStatusRunning = 2
	// AppStatusDeactivate 停用
	AppStatusDeactivate = 3

	// AppIsNotDeleted 未删除
	AppIsNotDeleted = 0
	// AppIsDeleted 删除
	AppIsDeleted = 1

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

	// AvatarURLPathPrefix 头像 path 前缀
	AvatarURLPathPrefix = "public"

	// TransferMethodKeywordBit0 关键字转人工开关开启时标志位值
	TransferMethodKeywordBit0 = 1

	// TransferIntentBit1 意图转人工开关开启时标志位值
	TransferIntentBit1 = 2

	// TransferUnsatisfiedBit2 不满意问题转人工开关开启时标志位值
	TransferUnsatisfiedBit2 = 4

	// ReplyFlexibilityLLM 模型润色
	ReplyFlexibilityLLM = 2
	// ReplyFlexibilityQAPriority 已采纳问答对直接回复
	ReplyFlexibilityQAPriority = 1
)

// 机器人配置页面
const (
	// RobotBasicConfig .
	RobotBasicConfig = 1
	// RobotTransferConfig .
	RobotTransferConfig = 2
	// RobotDialogueStrategyConfig .
	RobotDialogueStrategyConfig = 3
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

	// QaFilterTypeKey 问答检索类型
	QaFilterTypeKey = "qa"
	// DocFilterTypeKey 文档检索类型
	DocFilterTypeKey = "doc"
	// RejectedFilterTypeKey 拒答检索类型
	RejectedFilterTypeKey = "rejected"
	// SearchFilterTypeKey 搜索增强检索类型
	SearchFilterTypeKey = "search"
	// TaskFlowFilterTypeKey 任务型检索类型
	TaskFlowFilterTypeKey = "taskflow"

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

	// SummaryNormal 知识摘要策略名
	SummaryNormal = "summary_recognize"
	// ClassifyNormal 标签分类策略名
	ClassifyNormal = "classify_extract"

	// RerankModel rerank 模型
	RerankModel = "rerank"
)

const (
	// KnowledgeQaAppType 知识问答应用类型
	KnowledgeQaAppType = "knowledge_qa"
	// SummaryAppType 知识摘要应用类型
	SummaryAppType = "summary"
	// ClassifyAppType 标签提取应用类型配置
	ClassifyAppType = "classify"

	// 标签配置
	lkeRepositoryConfigLabel = "lkeRepository.config.label"

	// RetrievalConfigKey 检索配置redisKey
	RetrievalConfigKey = "retrieval:config:%d"
	// DefaultConfigRobotID 默认配置的机器人ID
	DefaultConfigRobotID = 0
)

const (
	// SyncToAppLimit 同步数据到app每次查询限制
	SyncToAppLimit = 200
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

// AppStatusDescMap 应用状态说明
var AppStatusDescMap = map[uint32]string{
	AppStatusInit:       "未上线",
	AppStatusRunning:    "运行中",
	AppStatusDeactivate: "停用",
}

// FilterTypeKey .
var FilterTypeKey = map[uint32]string{
	QaFilterType:       QaFilterTypeKey,
	DocFilterType:      DocFilterTypeKey,
	RejectedFilterType: RejectedFilterTypeKey,
	SearchFilterType:   SearchFilterTypeKey,
	TaskFlowFilterType: TaskFlowFilterTypeKey,
}

// App 应用
type App struct {
	AppDB
	PreviewDetails AppDetailsConfig `json:"previewDetails"` // 预览应用配置
	ReleaseDetails AppDetailsConfig `json:"releaseDetails"` // 发布应用配置
}

// AppDB 应用DB字段存储
type AppDB struct {
	ID                       uint64    `db:"id" gorm:"column:id"`
	AppKey                   string    `db:"app_key" gorm:"column:app_key"`                                     // 访问端密钥
	BusinessID               uint64    `db:"business_id" gorm:"column:business_id"`                             // 对外关联的机器人ID
	CorpID                   uint64    `db:"corp_id" gorm:"column:corp_id"`                                     // 企业 ID
	AppType                  string    `db:"app_type" gorm:"column:app_type"`                                   // 应用类型
	AppStatus                uint32    `db:"app_status" gorm:"column:app_status"`                               // 应用状态
	Name                     string    `db:"name" gorm:"column:name"`                                           // 昵称
	NameInAudit              string    `db:"name_in_audit" gorm:"column:name_in_audit"`                         // 审核中昵称
	Avatar                   string    `db:"avatar" gorm:"column:avatar"`                                       // 昵称
	AvatarInAudit            string    `db:"avatar_in_audit" gorm:"column:avatar_in_audit"`                     // 审核中头像
	Description              string    `db:"description" gorm:"column:description"`                             // 描述
	RoleDescription          string    `db:"role_description" gorm:"column:role_description"`                   // 机器人描述(prompt 场景使用)
	RoleDescriptionInAudit   string    `db:"role_description_in_audit" gorm:"column:role_description_in_audit"` // 审核中机器人描述(prompt 场景使用)
	Greeting                 string    `db:"greeting" gorm:"column:greeting"`                                   // 欢迎语
	GreetingInAudit          string    `db:"greeting_in_audit" gorm:"column:greeting_in_audit"`                 // 审核中欢迎语
	AppStatusReason          string    `db:"app_status_reason" gorm:"column:app_status_reason"`                 // 应用状态说明信息
	ModelName                string    `db:"model_name" gorm:"column:model_name"`                               // 模型名称
	PreviewJSON              string    `db:"preview_json" gorm:"column:preview_json"`                           // 预览应用配置
	ReleaseJSON              string    `db:"release_json" gorm:"column:release_json"`                           // 发布应用配置
	UseSearchEngine          bool      `db:"use_search_engine" gorm:"column:use_search_engine"`                 // 是否使用搜索引擎
	ShowSearchEngine         bool      `db:"show_search_engine" gorm:"column:show_search_engine"`
	ReplyFlexibility         uint32    `db:"reply_flexibility" gorm:"column:reply_flexibility"`                   // 机器人回复灵活度
	Model                    string    `db:"model" gorm:"column:model"`                                           // 模型配置
	Filters                  string    `db:"filters" gorm:"column:filters"`                                       // 索引配置
	SplitDoc                 string    `db:"split_doc" gorm:"column:split_doc"`                                   // 切分配置
	Embedding                string    `db:"embedding" gorm:"column:embedding"`                                   // embedding 配置
	SearchVector             string    `db:"search_vector" gorm:"column:search_vector"`                           // 相似配置
	QAVersion                uint64    `db:"qa_version" gorm:"column:qa_version"`                                 // 问答库版本
	TransferKeywords         string    `db:"transfer_keywords" gorm:"column:transfer_keywords"`                   // 转人工关键词
	Enabled                  bool      `db:"enabled" gorm:"column:enabled"`                                       // 是否启用
	UsedCharSize             int64     `db:"used_char_size" gorm:"column:used_char_size"`                         // 机器人已经使用的字符数
	IsCreateVectorIndex      int       `db:"is_create_vector_index" gorm:"column:is_create_vector_index"`         // 是否创建了机器人向量库
	IsDeleted                int       `db:"is_deleted" gorm:"column:is_deleted"`                                 // 是否已删除
	EnableRerank             bool      `db:"enable_rerank" gorm:"column:enable_rerank"`                           // 是否开启重排
	TransferMethod           uint32    `db:"transfer_method" gorm:"column:transfer_method"`                       // 转人工类型
	TransferUnsatisfiedCount uint32    `db:"transfer_unsatisfied_count" gorm:"column:transfer_unsatisfied_count"` // 触发转人工的不满意次数
	IntentPolicyID           uint32    `db:"intent_policy_id" gorm:"column:intent_policy_id"`                     // 意图策略
	BareAnswer               string    `db:"bare_answer" gorm:"column:bare_answer"`                               // 未知问题回复语
	BareAnswerInAudit        string    `db:"bare_answer_in_audit" gorm:"column:bare_answer_in_audit"`             // 审核中未知问题回复语
	UseGeneralKnowledge      bool      `db:"use_general_knowledge" gorm:"column:use_general_knowledge"`           // 是否使用行业通用知识库
	TokenUsage               uint64    `db:"token_usage" gorm:"column:token_usage"`
	ExpireTime               time.Time `db:"expire_time" gorm:"column:expire_time"`           // 过期时间
	CreateTime               time.Time `db:"create_time" gorm:"column:create_time"`           // 创建时间
	UpdateTime               time.Time `db:"update_time" gorm:"column:update_time"`           // 更新时间
	StaffID                  uint64    `db:"staff_id" gorm:"column:staff_id"`                 // 操作人
	InfosecBizType           string    `db:"infosec_biz_type" gorm:"column:infosec_biz_type"` // 安全审核策略
	IsShared                 bool      `db:"is_shared" gorm:"column:is_shared"`               // 是否共享知识库应用
	SpaceID                  string    `db:"space_id" gorm:"column:space_id"`                 // 空间ID
}

// RerankModelConfig .
type RerankModelConfig struct {
	ModelName string // rerank 模型
	TopN      uint32 // 进 rerank 之前取的 topN 数量
	Enable    bool   // 启用 rerank
}

// RetrievalConfigDB 检索配置表
type RetrievalConfigDB struct {
	ID                 int64     `db:"id"`
	RobotID            int64     `db:"robot_id"`             // RobotID，0-表示默认配置
	EnableVectorRecall bool      `db:"enable_vector_recall"` // 向量召回开关 0-关闭 1-开启
	EnableESRecall     bool      `db:"enable_es_recall"`     // es召回开关 0-关闭 1-开启
	EnableRRF          bool      `db:"enable_rrf"`           // rrf排序开关 0-关闭 1-开启
	EnableText2Sql     bool      `db:"enable_text2sql"`      // text2sql开关 0-关闭 1-开启
	ReRankThreshold    float32   `db:"rerank_threshold"`     // rerank分数阈值 默认-6.0
	RRFVecWeight       float32   `db:"rrf_vec_weight"`       // rrf向量排序的权重 默认2
	RRFEsWeight        float32   `db:"rrf_es_weight"`        // rrf es排序的权重 默认2
	RRFReRankWeight    float32   `db:"rrf_rerank_weight"`    // rrf rerank排序的权重 默认3
	DocVecRecallNum    uint32    `db:"doc_vec_recall_num"`   // 文档向量召回数量 默认20
	QaVecRecallNum     uint32    `db:"qa_vec_recall_num"`    // 问答向量召回数量 默认20
	EsRecallNum        uint32    `db:"es_recall_num"`        // es召回数量 默认20
	EsReRankMinNum     uint32    `db:"es_rerank_min_num"`    // 召回数量超过时 es做rerank的数量 默认7
	RRFReciprocalConst int32     `db:"rrf_reciprocal_const"` // rrf倒数常量 默认1
	Operator           string    `db:"operator"`             // 操作人
	CreateTime         time.Time `db:"create_time"`          // 创建时间
	UpdateTime         time.Time `db:"update_time"`          // 更新时间
	EsTopN             uint32    `db:"es_top_n"`             // 表格召回数量
	Text2sqlModel      string    `db:"text2sql_model"`       // text2sql模型名称
	Text2sqlPrompt     string    `db:"text2sql_prompt"`      // text2sql prompt
}

// RetrievalConfig 检索配置
type RetrievalConfig struct {
	EnableEsRecall     bool    `json:"enable_es_recall"`     // enable_es_recall
	EnableVectorRecall bool    `json:"enable_vector_recall"` // enable_vector_recall
	EnableRrf          bool    `json:"enable_rrf"`           // enable_rrf
	EnableText2Sql     bool    `json:"enable_text2sql"`      // enable_text2sql
	ReRankThreshold    float32 `json:"rerank_threshold"`     // rerank分数阈值 默认-6.0
	RRFVecWeight       float32 `json:"rrf_vec_weight"`       // rrf向量排序的权重 默认2
	RRFEsWeight        float32 `json:"rrf_es_weight"`        // rrf es排序的权重 默认2
	RRFReRankWeight    float32 `json:"rrf_rerank_weight"`    // rrf rerank排序的权重 默认3
	DocVecRecallNum    uint32  `json:"doc_vec_recall_num"`   // 文档向量召回数量 默认20
	QaVecRecallNum     uint32  `json:"qa_vec_recall_num"`    // 问答向量召回数量 默认20
	EsRecallNum        uint32  `json:"es_recall_num"`        // es召回数量 默认20
	EsReRankMinNum     uint32  `json:"es_rerank_min_num"`    // 召回数量超过时 es做rerank的数量 默认7
	RRFReciprocalConst int32   `json:"rrf_reciprocal_const"` // rrf倒数常量 默认1
	EsTopN             uint32  `json:"es_top_n"`             // 表格召回数量
	Text2sqlModel      string  `json:"text2sql_model"`       // text2sql模型名称
	Text2sqlPrompt     string  `json:"text2sql_prompt"`      // text2sql prompt
}

// IsCheckRetrievalConfig 检查检索配置,向量召回和关键词召回不可同时关闭
func (req RetrievalConfig) IsCheckRetrievalConfig() bool {
	if !req.EnableVectorRecall && !req.EnableEsRecall {
		return false
	}
	return true
}

// CheckRetrievalConfigDiff 检查检索配置是否相同
func (req RetrievalConfig) CheckRetrievalConfigDiff(retrievalConfig RetrievalConfig) bool {
	return cmp.Equal(req, retrievalConfig)
}

// ConvertToRetrievalConfig db结构转换model结构
func (retrievalConfigDB *RetrievalConfigDB) ConvertToRetrievalConfig() RetrievalConfig {
	return RetrievalConfig{
		EnableEsRecall:     retrievalConfigDB.EnableESRecall,
		EnableVectorRecall: retrievalConfigDB.EnableVectorRecall,
		EnableRrf:          retrievalConfigDB.EnableRRF,
		EnableText2Sql:     retrievalConfigDB.EnableText2Sql,
		ReRankThreshold:    retrievalConfigDB.ReRankThreshold,
		RRFVecWeight:       retrievalConfigDB.RRFVecWeight,
		RRFEsWeight:        retrievalConfigDB.RRFEsWeight,
		RRFReRankWeight:    retrievalConfigDB.RRFReRankWeight,
		DocVecRecallNum:    retrievalConfigDB.DocVecRecallNum,
		QaVecRecallNum:     retrievalConfigDB.QaVecRecallNum,
		EsRecallNum:        retrievalConfigDB.EsRecallNum,
		EsReRankMinNum:     retrievalConfigDB.EsReRankMinNum,
		RRFReciprocalConst: retrievalConfigDB.RRFReciprocalConst,
		EsTopN:             retrievalConfigDB.EsTopN,
		Text2sqlModel:      retrievalConfigDB.Text2sqlModel,
		Text2sqlPrompt:     retrievalConfigDB.Text2sqlPrompt,
	}
}

// GetRetrievalConfigKey 组合检索配置redisKey
func GetRetrievalConfigKey(robotID uint64) string {
	return fmt.Sprintf(RetrievalConfigKey, robotID)
}

// GetAppStatusDesc 获取应用状态描述
func (c *AppDB) GetAppStatusDesc() string {
	if c == nil {
		return ""
	}
	appStatusDesc := AppStatusDescMap[c.AppStatus]
	if len(c.AppStatusReason) != 0 {
		appStatusDesc = fmt.Sprintf("%s-%s", appStatusDesc, c.AppStatusReason)
	}
	return appStatusDesc
}

// CanTransfer 是否开启关键字转人工/是否开启意图转人工/是否开启不满意问题转人工
func (c *AppDB) CanTransfer(transferType uint32) bool {
	return c.TransferMethod&transferType != 0
}

// ToApp AppDB to App
func (c *AppDB) ToApp() (*App, error) {
	preDetail, err := c.toPreviewDetails()
	if err != nil {
		return nil, err
	}
	releaseDetails, err := c.toReleaseDetails()
	if err != nil {
		return nil, err
	}
	return &App{
		AppDB:          *c,
		PreviewDetails: preDetail,
		ReleaseDetails: releaseDetails,
	}, nil
}

// HasDeleted 是否已删除
func (c *AppDB) HasDeleted() bool {
	if c == nil {
		return true
	}
	return c.IsDeleted == AppIsDeleted
}

// GetAppID 应用ID
func (c *AppDB) GetAppID() uint64 {
	if c == nil {
		return 0
	}
	return c.ID
}

// HasCreateVectorIndexDone 是否创建了机器人向量库
func (c *AppDB) HasCreateVectorIndexDone() bool {
	if c == nil {
		return false
	}
	return c.IsCreateVectorIndex == IsCreateVectorIndexDone
}

// GetEmbeddingConf 获取应用 embedding 配置
func (c *AppDB) GetEmbeddingConf() (config.RobotEmbedding, bool, error) {
	if c == nil {
		return config.RobotEmbedding{}, false, fmt.Errorf("robot is nil")
	}
	if c.Embedding == "" {
		return config.App().RobotDefault.Embedding, true, nil
	}
	embedding := config.RobotEmbedding{}
	err := jsoniter.UnmarshalFromString(c.Embedding, &embedding)
	return embedding, false, err
}

// toPreviewDetails json转AppDetailsConfig
func (c *AppDB) toPreviewDetails() (AppDetailsConfig, error) {
	if c == nil {
		return AppDetailsConfig{}, errs.ErrAppNotFound
	}
	var v AppDetailsConfig
	if len(c.PreviewJSON) == 0 {
		return v, nil
	}

	if err := UnmarshalStr(c.PreviewJSON, &v); err != nil {
		return v, err
	}
	return v, nil
}

// toReleaseDetails json转AppDetailsConfig
func (c *AppDB) toReleaseDetails() (AppDetailsConfig, error) {
	if c == nil {
		return AppDetailsConfig{}, errs.ErrAppNotFound
	}
	var v AppDetailsConfig
	if len(c.ReleaseJSON) == 0 {
		return v, nil
	}
	if err := UnmarshalStr(c.ReleaseJSON, &v); err != nil {
		return v, err
	}
	return v, nil
}

// GetTransferMethod 获取 transferMethod
func (c *AppDB) GetTransferMethod(canTransferKeyword, canTransferIntent, canTransferUnsatisfied bool) uint32 {
	if c == nil {
		return 0
	}
	var transferMethod uint32 = 0
	if canTransferKeyword {
		transferMethod += 1
	}

	if canTransferIntent {
		transferMethod += 2
	}

	if canTransferUnsatisfied {
		transferMethod += 4
	}
	return transferMethod
}

// GetAppDetailsConfig 获取app配置详情
func (r *App) GetAppDetailsConfig(scenes uint32) AppDetailsConfig {
	switch scenes {
	case AppTestScenes:
		return r.PreviewDetails
	case AppReleaseScenes:
		return r.ReleaseDetails
	}
	return AppDetailsConfig{}
}

// ToDB App to AppDB
func (r *App) ToDB() *AppDB {
	var roleDescription, greeting string
	var replyFlexibility uint32
	var useGeneralKnowledge bool
	var useSearchEngine bool
	var showSearchEngine bool
	if r.PreviewDetails.AppConfig.KnowledgeQaConfig != nil {
		roleDescription = r.PreviewDetails.AppConfig.KnowledgeQaConfig.RoleDescription
		greeting = r.PreviewDetails.AppConfig.KnowledgeQaConfig.Greeting
		replyFlexibility = r.PreviewDetails.AppConfig.KnowledgeQaConfig.GetReplyFlexibility()
		useGeneralKnowledge = r.PreviewDetails.AppConfig.KnowledgeQaConfig.UseGeneralKnowledge
		useSearchEngine = r.PreviewDetails.AppConfig.KnowledgeQaConfig.UseSearchEngine
		showSearchEngine = r.PreviewDetails.AppConfig.KnowledgeQaConfig.ShowSearchEngine
	}
	return &AppDB{
		ID:                       r.ID,
		AppKey:                   r.AppKey,
		BusinessID:               r.BusinessID,
		CorpID:                   r.CorpID,
		AppType:                  r.AppType,
		AppStatus:                r.AppStatus,
		AppStatusReason:          r.AppStatusReason,
		Name:                     r.PreviewDetails.BaseConfig.Name,
		Avatar:                   r.PreviewDetails.BaseConfig.Avatar,
		Description:              r.PreviewDetails.BaseConfig.Description,
		RoleDescription:          roleDescription,
		Greeting:                 greeting,
		ModelName:                r.ModelName,
		PreviewJSON:              r.PreviewDetails.ToJSON(),
		ReleaseJSON:              r.ReleaseDetails.ToJSON(),
		ReplyFlexibility:         replyFlexibility,
		Model:                    r.Model,
		Filters:                  r.Filters,
		SplitDoc:                 r.SplitDoc,
		Embedding:                r.Embedding,
		SearchVector:             r.SearchVector,
		QAVersion:                r.QAVersion,
		TransferKeywords:         r.TransferKeywords,
		Enabled:                  r.Enabled,
		UsedCharSize:             r.UsedCharSize,
		IsCreateVectorIndex:      r.IsCreateVectorIndex,
		IsDeleted:                r.IsDeleted,
		EnableRerank:             r.EnableRerank,
		TransferMethod:           r.TransferMethod,
		TransferUnsatisfiedCount: r.TransferUnsatisfiedCount,
		IntentPolicyID:           r.IntentPolicyID,
		BareAnswer:               r.PreviewDetails.BaseConfig.Name,
		UseGeneralKnowledge:      useGeneralKnowledge,
		UseSearchEngine:          useSearchEngine,
		ShowSearchEngine:         showSearchEngine,
		ExpireTime:               r.ExpireTime,
		CreateTime:               r.CreateTime,
		UpdateTime:               r.UpdateTime,
		StaffID:                  r.StaffID,
		NameInAudit:              r.NameInAudit,            // 老业务审核
		AvatarInAudit:            r.AvatarInAudit,          // 老业务审核
		GreetingInAudit:          r.GreetingInAudit,        // 老业务审核
		BareAnswerInAudit:        r.BareAnswerInAudit,      // 老业务审核
		RoleDescriptionInAudit:   r.RoleDescriptionInAudit, // 老业务审核
		TokenUsage:               r.TokenUsage,
	}
}

// GetQAExtractModel 获取问答提取模型
func (r *App) GetQAExtractModel(scenes uint32) (config.AppModelDetail, error) {
	if r == nil {
		return config.AppModelDetail{}, fmt.Errorf("robot is nil")
	}
	models, _, err := r.GetModels(scenes)
	if err != nil {
		return config.AppModelDetail{}, err
	}
	model, ok := models[QAExtractModel]
	if !ok {
		return config.AppModelDetail{}, fmt.Errorf("no qa_extract model")
	}
	return model, nil
}

// GetFilter 获取应用的filter配置
func (r *App) GetFilter(scenes uint32, key string) (config.RobotFilter, error) {
	if r == nil {
		return config.RobotFilter{}, fmt.Errorf("robot is nil")
	}
	var filter config.RobotFilter
	filters, _, err := r.GetFilters(scenes)
	if err != nil {
		return filter, errs.ErrAppTypeSupportFilters
	}
	filter, ok := filters[key]
	if !ok {
		return filter, fmt.Errorf("robot %s filter not found scenes %d", key, scenes)
	}
	return filter, nil
}

// CanUseTaskFlow 是否启用任务型
func (r *App) CanUseTaskFlow(scenes uint32) bool {
	if r == nil {
		return false
	}
	if r.AppType != KnowledgeQaAppType {
		return false
	}
	var canUseTaskFlow bool
	filterKey := AppSearchPreviewFilterKey
	if scenes == AppReleaseScenes {
		filterKey = AppSearchReleaseFilterKey
	}
	filter, err := r.GetFilter(scenes, filterKey)
	if err != nil {
		return canUseTaskFlow
	}
	for _, f := range filter.Filter {
		if f.DocType == DocTypeTaskFlow {
			canUseTaskFlow = f.IsEnabled
		}
	}
	return canUseTaskFlow
}

// ToPBBaseConfig app配置转化为PB基础配置
func (r *App) ToPBBaseConfig(isRelease bool) *pb.BaseConfig {
	if r == nil {
		return nil
	}
	basicConfig := r.PreviewDetails.BaseConfig
	if isRelease {
		basicConfig = r.ReleaseDetails.BaseConfig
	}
	return &pb.BaseConfig{
		Name:   basicConfig.Name,
		Avatar: basicConfig.Avatar,
		Desc:   basicConfig.Description,
	}
}

// ToPBKnowledgeQaConfig app配置转化为PB知识库应用配置
func (r *App) ToPBKnowledgeQaConfig(hasAllPerm bool, perms []*PermissionInfo, isRelease bool) *pb.KnowledgeQaConfig {
	if r == nil || r.AppType != KnowledgeQaAppType {
		return nil
	}
	hasPerm := make(map[string]bool)
	for _, v := range perms {
		hasPerm[v.PermissionID] = true
	}
	knowledgeQaConfig := r.PreviewDetails.AppConfig.KnowledgeQaConfig
	if isRelease {
		knowledgeQaConfig = r.ReleaseDetails.AppConfig.KnowledgeQaConfig
	}
	if knowledgeQaConfig == nil {
		return nil
	}

	output := &pb.KnowledgeQaOutput{
		Method: knowledgeQaConfig.Method,
	}
	_, ok := hasPerm[LkeRepositoryConfigOutputGeneralKnowledge()]
	if hasAllPerm || ok {
		output.UseGeneralKnowledge = knowledgeQaConfig.UseGeneralKnowledge
		output.BareAnswer = knowledgeQaConfig.BareAnswer
	}
	var apiVarAttrInfos []*pb.SearchRange_ApiVarAttrInfo
	for _, v := range knowledgeQaConfig.SearchRange.ApiVarAttrInfos {
		apiVarAttrInfos = append(apiVarAttrInfos, &pb.SearchRange_ApiVarAttrInfo{
			ApiVarId:  v.ApiVarID,
			AttrBizId: v.AttrBizID,
		})
	}
	return &pb.KnowledgeQaConfig{
		Greeting:        knowledgeQaConfig.Greeting,
		RoleDescription: knowledgeQaConfig.RoleDescription,
		Model:           r.getKnowledgeQaModel(knowledgeQaConfig, hasAllPerm, hasPerm),
		Search:          r.getKnowledgeQaFilter(knowledgeQaConfig, hasAllPerm, hasPerm),
		Output:          output,
		SearchRange: &pb.SearchRange{
			Condition:       knowledgeQaConfig.SearchRange.Condition,
			ApiVarAttrInfos: apiVarAttrInfos,
		},
	}
}

func (r *App) getKnowledgeQaModel(knowledgeQaConfig *KnowledgeQaConfig, hasAllPerm bool,
	hasPerm map[string]bool) *pb.AppModel {
	appModel := &pb.AppModel{}
	for k, v := range knowledgeQaConfig.Model {
		switch k {
		case AppModelNormal, AppModelNormalNonGeneralKnowledge:
			_, ok := hasPerm[LkeRepositoryConfigModelVersion()]
			if !hasAllPerm && !ok {
				continue
			}
			appModel.Name = v.ModelName
			appModel.AliasName = r.ModelName
		case AppModelQueryRewrite:
			_, ok := hasPerm[LkeRepositoryConfigModelHistoryLimit()]
			if !hasAllPerm && !ok {
				continue
			}
			appModel.ContextLimit = v.HistoryLimit
		}
	}
	return appModel
}

func (r *App) getKnowledgeQaFilter(knowledgeQaConfig *KnowledgeQaConfig,
	hasAllPerm bool, hasPerm map[string]bool) []*pb.KnowledgeQaSearch {
	searchs := make([]*pb.KnowledgeQaSearch, 0)
	for _, v := range knowledgeQaConfig.Filters[AppSearchPreviewFilterKey].Filter {
		typeKey := GetFiltersTypeKey(v.DocType)
		searchInfo := &pb.KnowledgeQaSearch{
			Type:      typeKey,
			IsEnabled: v.IsEnabled,
		}
		if typeKey == QaFilterTypeKey {
			_, ok := hasPerm[LkeRepositoryConfigKnowledgeQa()]
			if !hasAllPerm && !ok {
				continue
			}
			_, ok = hasPerm[LkeRepositoryConfigKnowledgeQaTopN()]
			if hasAllPerm || ok {
				searchInfo.QaTopN = v.TopN
			}
			_, ok = hasPerm[LkeRepositoryConfigKnowledgeReplyFlexibility()]
			if hasAllPerm || ok {
				searchInfo.ReplyFlexibility = knowledgeQaConfig.ReplyFlexibility
			}
		}
		if typeKey == DocFilterTypeKey {
			_, ok := hasPerm[LkeRepositoryConfigKnowledgeDoc()]
			if !hasAllPerm && !ok {
				continue
			}
			searchInfo.DocTopN = v.TopN
		}
		if typeKey == SearchFilterTypeKey {
			_, ok := hasPerm[LkeRepositoryConfigKnowledgeSearchEngine()]
			if !hasAllPerm && !ok {
				continue
			}
			searchInfo.UseSearchEngine = knowledgeQaConfig.UseSearchEngine
			searchInfo.ShowSearchEngine = knowledgeQaConfig.ShowSearchEngine
		}
		if typeKey == TaskFlowFilterTypeKey {
			_, ok := hasPerm[LkeRepositoryConfigKnowledgeTaskFlow()]
			if !hasAllPerm && !ok {
				continue
			}
		}
		searchs = append(searchs, searchInfo)
	}
	return searchs
}

// ToPBSummaryConfig app配置转化为PB知识摘要应用配置
func (r *App) ToPBSummaryConfig(hasAllPerm bool, perms []*PermissionInfo, isRelease bool) *pb.SummaryConfig {
	if r == nil || r.AppType != SummaryAppType {
		return nil
	}
	hasPerm := make(map[string]bool)
	for _, v := range perms {
		hasPerm[v.PermissionID] = true
	}
	summaryConfig := r.PreviewDetails.AppConfig.SummaryConfig
	if isRelease {
		summaryConfig = r.ReleaseDetails.AppConfig.SummaryConfig
	}
	if summaryConfig == nil {
		return nil
	}
	appModel := &pb.AppModel{}
	for k, v := range summaryConfig.Model {
		switch k {
		case SummaryNormal:
			_, ok := hasPerm[LkeSummaryConfigModelVersion()]
			if !hasAllPerm && !ok {
				continue
			}
			appModel.Name = v.ModelName
			appModel.AliasName = r.ModelName
		}
	}
	// 做一个兼容处理
	if summaryConfig.Requirement == 0 {
		summaryConfig.Requirement = 1
	}
	output := &pb.SummaryOutput{}
	_, ok := hasPerm[LkeSummaryConfigOutputRequirement()]
	if hasAllPerm || ok {
		output.Requirement = summaryConfig.Requirement
		output.RequireCommand = summaryConfig.RequireCommand
	}
	_, ok = hasPerm[LkeSummaryConfigOutputMethod()]
	if hasAllPerm || ok {
		output.Method = summaryConfig.Method
	}
	return &pb.SummaryConfig{
		Model:  appModel,
		Output: output,
	}
}

// ToPBClassifyConfig app配置转化为PB标签提取应用配置
func (r *App) ToPBClassifyConfig(hasAllPerm bool, perms []*PermissionInfo, isRelease bool) *pb.ClassifyConfig {
	if r == nil || r.AppType != ClassifyAppType {
		return nil
	}
	hasPerm := make(map[string]bool)
	for _, v := range perms {
		hasPerm[v.PermissionID] = true
	}
	classifyConfig := r.PreviewDetails.AppConfig.ClassifyConfig
	if isRelease {
		classifyConfig = r.ReleaseDetails.AppConfig.ClassifyConfig
	}
	if classifyConfig == nil {
		return nil
	}
	labels := make([]*pb.ClassifyLabel, 0)
	_, ok := hasPerm[LkeClassifyConfigTag()]
	if hasAllPerm || ok {
		for _, v := range classifyConfig.Labels {
			labels = append(labels, &pb.ClassifyLabel{
				Name:        v.Name,
				Description: v.Description,
				Values:      v.Values,
			})
		}
	}
	appModel := &pb.AppModel{}
	for k, v := range classifyConfig.Model {
		switch k {
		case ClassifyNormal:
			_, ok := hasPerm[LkeClassifyConfigModelVersion()]
			if !hasAllPerm && !ok {
				continue
			}
			appModel.Name = v.ModelName
			appModel.AliasName = r.ModelName
		}
	}
	return &pb.ClassifyConfig{
		Model:  appModel,
		Labels: labels,
	}
}

// GetModels 获取应用模型配置
func (r *App) GetModels(scenes uint32) (config.AppModel, bool, error) {
	if r == nil {
		return nil, false, errs.ErrAppNotFound
	}
	appConfig := r.PreviewDetails.AppConfig
	if scenes == AppReleaseScenes {
		appConfig = r.ReleaseDetails.AppConfig
	}
	defaultModel := make(config.AppModel)
	customModel := make(config.AppModel)
	if r.AppType == KnowledgeQaAppType {
		if appConfig.KnowledgeQaConfig == nil || len(appConfig.KnowledgeQaConfig.Model) == 0 {
			return config.App().RobotDefault.AppModelConfig.KnowledgeQaAppModel, true, nil
		}
		defaultModel = config.App().RobotDefault.AppModelConfig.KnowledgeQaAppModel
		customModel = appConfig.KnowledgeQaConfig.Model
	}
	if r.AppType == SummaryAppType {
		if appConfig.SummaryConfig == nil || len(appConfig.SummaryConfig.Model) == 0 {
			return config.App().RobotDefault.AppModelConfig.SummaryAppModel, true, nil
		}
		defaultModel = config.App().RobotDefault.AppModelConfig.SummaryAppModel
		customModel = appConfig.SummaryConfig.Model
	}
	if r.AppType == ClassifyAppType {
		if appConfig.ClassifyConfig == nil || len(appConfig.ClassifyConfig.Model) == 0 {
			return config.App().RobotDefault.AppModelConfig.ClassifyAppModel, true, nil
		}
		defaultModel = config.App().RobotDefault.AppModelConfig.ClassifyAppModel
		customModel = appConfig.ClassifyConfig.Model
	}
	models := make(config.AppModel)
	for k, v := range customModel {
		models[k] = v
	}
	for k, v := range defaultModel {
		if _, ok := models[k]; !ok {
			models[k] = v
		}
	}
	return models, false, nil
}

// GetSearchVector 获取应用搜索向量配置
func (r *App) GetSearchVector(scenes uint32) (*config.SearchVector, bool, error) {
	if r == nil {
		return &config.SearchVector{}, false, fmt.Errorf("robot is nil")
	}
	if r.AppType != KnowledgeQaAppType {
		return nil, false, fmt.Errorf("robot type is not knowledge qa")
	}
	appConfig := r.PreviewDetails.AppConfig
	if scenes == AppReleaseScenes {
		appConfig = r.ReleaseDetails.AppConfig
	}
	if appConfig.KnowledgeQaConfig == nil || appConfig.KnowledgeQaConfig.SearchVector == nil {
		return &config.SearchVector{
			Confidence: config.App().RobotDefault.SearchVector.Confidence,
			TopN:       config.App().RobotDefault.SearchVector.TopN,
		}, true, nil
	}
	return appConfig.KnowledgeQaConfig.SearchVector, false, nil
}

// GetDocSplitConf 获取应用文档分段配置
func (r *App) GetDocSplitConf(scenes uint32) (config.RobotDocSplit, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("robot is nil")
	}
	if r.AppType != KnowledgeQaAppType {
		return nil, false, fmt.Errorf("robot type is not knowledge qa")
	}
	appConfig := r.PreviewDetails.AppConfig
	if scenes == AppReleaseScenes {
		appConfig = r.ReleaseDetails.AppConfig
	}
	if appConfig.KnowledgeQaConfig == nil || len(appConfig.KnowledgeQaConfig.DocSplit) == 0 {
		return config.App().RobotDefault.DocSplit, true, nil
	}
	docSplit := make(config.RobotDocSplit)
	for k, v := range appConfig.KnowledgeQaConfig.DocSplit {
		docSplit[k] = v
	}
	for k, v := range config.App().RobotDefault.DocSplit {
		if _, ok := docSplit[k]; !ok {
			docSplit[k] = v
		}
	}
	return docSplit, false, nil
}

// GetFilters 机器人索引配置
func (r *App) GetFilters(scenes uint32) (config.RobotFilters, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("robot is nil")
	}
	if r.AppType != KnowledgeQaAppType {
		return nil, false, fmt.Errorf("robot type is not knowledge qa")
	}
	appConfig := r.PreviewDetails.AppConfig
	if scenes == AppReleaseScenes {
		appConfig = r.ReleaseDetails.AppConfig
	}
	if appConfig.KnowledgeQaConfig == nil || len(appConfig.KnowledgeQaConfig.Filters) == 0 {
		return config.App().RobotDefault.Filters, true, nil
	}
	filters := make(config.RobotFilters)
	for k, v := range appConfig.KnowledgeQaConfig.Filters {
		filters[k] = v
	}
	for k, v := range config.App().RobotDefault.Filters {
		if _, ok := filters[k]; !ok {
			filters[k] = v
		}
	}
	return filters, false, nil
}

// GetUseSearchEngine 是否试用搜索增强
func (r *App) GetUseSearchEngine(scenes uint32) bool {
	if r == nil {
		return false
	}
	if r.AppType != KnowledgeQaAppType {
		return false
	}
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return r.UseSearchEngine
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.GetUseSearchEngine()
	}
	if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
		return r.UseSearchEngine
	}
	return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.GetUseSearchEngine()
}

// GetRoleDescription 获取角色描述
func (r *App) GetRoleDescription(scenes uint32) string {
	if r == nil {
		return ""
	}
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return r.RoleDescription
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.RoleDescription
	}
	if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
		return r.RoleDescription
	}
	return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.RoleDescription
}

// GetGreeting 获取欢迎语
func (r *App) GetGreeting(scenes uint32) string {
	if r == nil {
		return ""
	}
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return r.Greeting
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.Greeting
	}
	if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
		return r.Greeting
	}
	return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.Greeting
}

// GetShowSearchEngine 是否显示搜索引擎
func (r *App) GetShowSearchEngine(scenes uint32) bool {
	if r == nil {
		return false
	}
	if r.AppType != KnowledgeQaAppType {
		return false
	}
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return r.ShowSearchEngine
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.GetShowSearchEngine()
	}
	if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
		return r.ShowSearchEngine
	}
	return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.GetShowSearchEngine()
}

// GetReplyFlexibility  1：已采纳答案直接回复 2：已采纳答案润色后回复
func (r *App) GetReplyFlexibility(scenes uint32) uint32 {
	if r == nil {
		return 0
	}
	if r.AppType != KnowledgeQaAppType {
		return 0
	}
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return r.ReplyFlexibility
		}
		if !r.isQAFilterOpen(scenes, AppSearchPreviewFilterKey) {
			return 0
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.ReplyFlexibility
	}
	if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
		return r.ReplyFlexibility
	}
	if !r.isQAFilterOpen(scenes, AppSearchReleaseFilterKey) {
		return 0
	}
	return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.ReplyFlexibility
}

func (r *App) isQAFilterOpen(scenes uint32, filterKey string) bool {
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return false
		}
	} else {
		if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
			return false
		}
	}
	filters, err := r.GetFilter(scenes, filterKey)
	if err != nil {
		return false
	}
	for _, f := range filters.Filter {
		if f.DocType == QaFilterType && f.IsEnabled {
			return true
		}
	}
	return false
}

// GetMethod  1：已采纳答案直接回复 2：已采纳答案润色后回复
func (r *App) GetMethod(scenes uint32) uint32 {
	if r == nil {
		return 0
	}
	if r.AppType != KnowledgeQaAppType && r.AppType != SummaryAppType {
		return 0
	}
	if scenes == AppTestScenes {
		if r.AppType == KnowledgeQaAppType {
			return r.PreviewDetails.AppConfig.KnowledgeQaConfig.Method
		}
		return r.PreviewDetails.AppConfig.SummaryConfig.Method
	}
	if r.AppType == KnowledgeQaAppType {
		if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
			return 0
		}
		return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.Method
	}
	if r.ReleaseDetails.AppConfig.SummaryConfig == nil {
		return 0
	}
	return r.ReleaseDetails.AppConfig.SummaryConfig.Method
}

// GetUseGeneralKnowledge  是否通用模型答复
func (r *App) GetUseGeneralKnowledge(scenes uint32) bool {
	if r == nil {
		return false
	}
	if r.AppType != KnowledgeQaAppType {
		return false
	}
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return r.UseGeneralKnowledge
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.UseGeneralKnowledge
	}
	if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
		return r.UseGeneralKnowledge
	}
	return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.UseGeneralKnowledge
}

// GetBareAnswer 获取未知问题答复
func (r *App) GetBareAnswer(scenes uint32) string {
	if r == nil {
		return ""
	}
	if r.AppType != KnowledgeQaAppType {
		return ""
	}
	if scenes == AppTestScenes {
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return r.BareAnswer
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.BareAnswer
	}
	if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
		return r.BareAnswer
	}
	return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.BareAnswer
}

// GetRequirement .
func (r *App) GetRequirement(scenes uint32) uint32 {
	if r == nil {
		return 0
	}
	if r.AppType != SummaryAppType {
		return 0
	}
	if scenes == AppTestScenes {
		return r.PreviewDetails.AppConfig.SummaryConfig.Requirement
	}
	if r.ReleaseDetails.AppConfig.SummaryConfig == nil {
		return 0
	}
	return r.ReleaseDetails.AppConfig.SummaryConfig.Requirement
}

// GetRequireCommand .
func (r *App) GetRequireCommand(scenes uint32) string {
	if r == nil {
		return ""
	}
	if r.AppType != SummaryAppType {
		return ""
	}
	if scenes == AppTestScenes {
		return r.PreviewDetails.AppConfig.SummaryConfig.RequireCommand
	}
	if r.ReleaseDetails.AppConfig.SummaryConfig == nil {
		return ""
	}
	return r.ReleaseDetails.AppConfig.SummaryConfig.RequireCommand
}

// GetRerankModel 获取重排模型
func (r *App) GetRerankModel(scenes uint32) (RerankModelConfig, error) {
	if r == nil {
		return RerankModelConfig{}, fmt.Errorf("robot is nil")
	}
	f, err := r.GetFilter(scenes, AppRerankFilterKey)
	if err != nil {
		return RerankModelConfig{}, err
	}
	models, _, err := r.GetModels(scenes)
	if err != nil {
		return RerankModelConfig{}, err
	}
	model, ok := models[RerankModel]
	if !ok {
		return RerankModelConfig{}, fmt.Errorf("robot %s model not found", RerankModel)
	}

	return RerankModelConfig{
		ModelName: model.ModelName,
		TopN:      f.TopN,
		Enable:    r.EnableRerank,
	}, nil
}

// IsWriteable 是否可写
func (r *App) IsWriteable() error {
	conf, _, err := r.GetEmbeddingConf()
	if err != nil {
		log.Error("robot.GetEmbeddingConf, robot: %v error: %v", r, err)
		return errs.ErrEmbeddingUpgrading
	}
	if conf.Version != conf.UpgradeVersion {
		return errs.ErrEmbeddingUpgrading
	}
	return nil
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

// NotEmpty 检索范围不为空
func (s SearchRange) NotEmpty() bool {
	return s.Condition != "" && len(s.ApiVarAttrInfos) > 0
}

type ApiVarAttrInfo struct {
	ApiVarID  string `json:"api_var_id"`
	AttrBizID uint64 `json:"attr_biz_id"`
}

// KnowledgeQaConfig 知识问答应用配置
type KnowledgeQaConfig struct {
	Greeting                string               `json:"greeting"`
	RoleDescription         string               `json:"role_description"`
	Method                  uint32               `json:"method"`
	UseGeneralKnowledge     bool                 `json:"use_general_knowledge"`
	BareAnswer              string               `json:"bare_answer"`
	ReplyFlexibility        uint32               `json:"reply_flexibility"`
	UseSearchEngine         bool                 `json:"use_search_engine"`
	ShowSearchEngine        bool                 `json:"show_search_engine"`
	Model                   config.AppModel      `json:"model"`
	Filters                 config.RobotFilters  `json:"filters"`
	DocSplit                config.RobotDocSplit `json:"doc_split"`
	SearchVector            *config.SearchVector `json:"search_vector"`
	UseQuestionClarify      bool                 `json:"use_question_clarify"`      // 是否使用问题澄清
	QuestionClarifyKeywords []string             `json:"question_clarify_keywords"` // 问题澄清关键词
	SearchRange             SearchRange          `json:"search_range"`              // 检索范围
	SearchStrategy          uint32               `json:"search_strategy"`           // 检索策略
	Text2sql                bool                 `json:"text2sql"`                  // 是否开启表格增强
}

// GetUseSearchEngine 是否显示搜索增强
func (k *KnowledgeQaConfig) GetUseSearchEngine() bool {
	return k.UseSearchEngine
}

// GetShowSearchEngine 是否显示搜索引擎检索状态
func (k *KnowledgeQaConfig) GetShowSearchEngine() bool {
	return k.ShowSearchEngine
}

// GetReplyFlexibility 1：已采纳答案直接回复 2：已采纳答案润色后回复
func (k *KnowledgeQaConfig) GetReplyFlexibility() uint32 {
	return k.ReplyFlexibility
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
		lastMethod := utils.When(k.Method == AppMethodStream, Stream, NotStream)
		method := utils.When(cfg.Method == AppMethodStream, Stream, NotStream)
		diff = append(diff, newDiffConfig(ConfigItemMethod, lastMethod, method, ""))
	}
	if k.UseGeneralKnowledge != cfg.UseGeneralKnowledge {
		lastConfig := utils.When(k.UseGeneralKnowledge, ShutDown, Open)
		newConfig := utils.When(cfg.UseGeneralKnowledge, ShutDown, Open)
		diff = append(diff, newDiffConfig(ConfigItemUseGeneralKnowledge, lastConfig, newConfig, ""))
	}
	if k.BareAnswer != cfg.BareAnswer {
		diff = append(diff, newDiffConfig(ConfigItemBareAnswer, k.BareAnswer, cfg.BareAnswer, ""))
	}
	diff = append(diff, k.equalFilter(cfg)...)
	diff = append(diff, k.equalModel(cfg)...)
	return diff
}

// SummaryConfig 知识摘要应用配置
type SummaryConfig struct {
	Model          config.AppModel `json:"model"`
	Method         uint32          `json:"method"`
	Requirement    uint32          `json:"requirement"`
	RequireCommand string          `json:"require_command"`
}

// ClassifyLabels 标签详情
type ClassifyLabels struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Values      []string `json:"values"`
}

// ClassifyConfig 知识标签应用配置
type ClassifyConfig struct {
	Model  config.AppModel  `json:"model"`
	Labels []ClassifyLabels `json:"labels"`
}

// AppConfig 应用配置
type AppConfig struct {
	KnowledgeQaConfig *KnowledgeQaConfig `json:"knowledge_qa,omitempty"`
	SummaryConfig     *SummaryConfig     `json:"summary,omitempty"`
	ClassifyConfig    *ClassifyConfig    `json:"classify,omitempty"`
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

// SyncKnowledgeQaAppDataReq 同步知识库问答应用数据请求
type SyncKnowledgeQaAppDataReq struct {
	StartID uint64 `json:"start_id"` // 起始ID
	EndID   uint64 `json:"end_id"`   // 结束ID
}

// SyncRobotRetrievalConfigReq 初始化同步存量机器人的检索配置请求
type SyncRobotRetrievalConfigReq struct {
	RobotIDs []uint64         `json:"robot_ids"` // 机器人ID列表
	Config   *RetrievalConfig `json:"config"`    // 检索配置
}

// SyncRetrievalConfigFromDBReq 应用的检索配置从DB同步到redis
type SyncRetrievalConfigFromDBReq struct {
	RobotIDs       []uint64 `json:"robot_ids"`         // 机器人ID列表
	IsAllConfigApp bool     `json:"is_all_config_app"` // 所有已经配置的应用都做同步
}

// FlushKnowledgeQaAppConfigReq 刷新知识库问答应用配置的请求
type FlushKnowledgeQaAppConfigReq struct {
	StartID     uint64             `json:"start_id"`     // 起始ID
	EndID       uint64             `json:"end_id"`       // 结束ID
	BatchSize   uint32             `json:"batch_size"`   // 批量处理的数量
	FlushConfig *KnowledgeQaConfig `json:"knowledge_qa"` // 要刷新的knowledge_qa配置项
}

// GetFiltersTypeKey 获取filter key-value映射关系
func GetFiltersTypeKey(i uint32) string {
	switch i {
	case QaFilterType:
		return QaFilterTypeKey
	case DocFilterType:
		return DocFilterTypeKey
	case RejectedFilterType:
		return RejectedFilterTypeKey
	case SearchFilterType:
		return SearchFilterTypeKey
	case TaskFlowFilterType:
		return TaskFlowFilterTypeKey
	}
	return ""
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

// GetFiltersType 获取filter key-value映射关系
func GetFiltersType(k string) uint32 {
	switch k {
	case QaFilterTypeKey:
		return QaFilterType
	case DocFilterTypeKey:
		return DocFilterType
	case RejectedFilterTypeKey:
		return RejectedFilterType
	case SearchFilterTypeKey:
		return SearchFilterType
	case TaskFlowFilterTypeKey:
		return TaskFlowFilterType
	}
	return 0
}

func newDiffConfig(configItem string, lastValue string, value string, content string) AppConfigDiff {
	return AppConfigDiff{
		ConfigItem: configItem,
		Action:     NextActionUpdate,
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
		lastConfig := utils.When(f.IsEnabled, Open, ShutDown)
		newConfig := utils.When(cfg.IsEnabled, Open, ShutDown)
		diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemIsEnabled, GetFiltersTypeName(cfg.DocType)),
			lastConfig, newConfig, ""))
	}
	if cfg.IsEnabled {
		if f.TopN != cfg.TopN {
			diff = append(diff, newDiffConfig(fmt.Sprintf(ConfigItemTopN, GetFiltersTypeName(cfg.DocType)),
				strconv.Itoa(int(f.TopN)), strconv.Itoa(int(cfg.TopN)), ""))
		}
		if f.ReplyFlexibility != cfg.ReplyFlexibility {
			lastConfig := utils.When(f.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
			newConfig := utils.When(cfg.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
			diff = append(diff, newDiffConfig(ConfigItemReplyFlexibility, lastConfig, newConfig, ""))
		}
		if f.ShowSearchEngine != cfg.ShowSearchEngine {
			lastConfig := utils.When(f.ShowSearchEngine, ShowOn, ShowOff)
			newConfig := utils.When(cfg.ShowSearchEngine, ShowOn, ShowOff)
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

// Equals 比对标签提供应用配置
func (cc *ClassifyConfig) Equals(cfg *ClassifyConfig) []AppConfigDiff {
	var diffs []AppConfigDiff
	if len(cc.Model) == 0 {
		var releaseDetail AppModelDetail
		for k, v := range cfg.Model {
			if k == ClassifyNormal {
				previewDetail := transformModelConfig(v)
				diffs = append(diffs, releaseDetail.EqualsModelName(&previewDetail, ClassifyAppType)...)
			}
		}
	} else {
		for k, v := range cc.Model {
			c, ok := cfg.Model[k]
			if !ok {
				continue
			}
			if k == ClassifyNormal {
				releaseDetail := transformModelConfig(v)
				previewDetail := transformModelConfig(c)
				diffs = append(diffs, releaseDetail.EqualsModelName(&previewDetail, ClassifyAppType)...)
			}
		}
	}
	mapLabel := make(map[string]ClassifyLabels)
	labelNames := make([]string, 0, len(cfg.Labels))
	for _, v := range cfg.Labels {
		mapLabel[v.Name] = v
		labelNames = append(labelNames, v.Name)
	}
	var changeNameLabels, changeDescLabels, changeValueLabels []string
	isLabelChange := false
	if len(cc.Labels) != len(cfg.Labels) {
		isLabelChange = true
	}
	for _, v := range cc.Labels {
		changeNameLabels = append(changeNameLabels, v.Name)
		label, ok := mapLabel[v.Name]
		if !ok {
			isLabelChange = true
			continue
		}
		if v.Description != label.Description {
			changeDescLabels = append(changeDescLabels, v.Name)
		}
		sort.Strings(v.Values)
		sort.Strings(label.Values)
		if !reflect.DeepEqual(v.Values, label.Values) {
			changeValueLabels = append(changeValueLabels, v.Name)
		}
	}
	if isLabelChange {
		content := ""
		if len(cc.Labels) == 0 {
			content = fmt.Sprintf("标签名称增加:%s", strings.Join(changeNameLabels, ","))
		} else {
			content = fmt.Sprintf("标签名称由:%s变更为:%s", strings.Join(changeNameLabels, ","),
				strings.Join(labelNames, ","))
		}
		diffs = append(diffs, newDiffConfig(ConfigItemLabel, "", "", content))
	}
	if len(changeDescLabels) > 0 {
		diffs = append(diffs, newDiffConfig(ConfigItemLabel, "", "", fmt.Sprintf("标签名称:%s的标签描述被修改",
			strings.Join(changeDescLabels, ","))))
	}
	if len(changeValueLabels) > 0 {
		diffs = append(diffs, newDiffConfig(ConfigItemLabel, "", "", fmt.Sprintf("标签名称:%s的标签取值被修改",
			strings.Join(changeValueLabels, ","))))
	}
	return diffs
}

// GetDefaultModelList 获取默认配置模型列表
func (cc *ClassifyConfig) GetDefaultModelList() map[string]*pb.AppModelInfo {
	if len(config.App().RobotDefault.AppDefaultModelConfig) == 0 {
		return make(map[string]*pb.AppModelInfo)
	}
	if _, ok := config.App().RobotDefault.AppDefaultModelConfig[ClassifyAppType]; !ok {
		return make(map[string]*pb.AppModelInfo)
	}
	return getModelList(config.App().RobotDefault.AppDefaultModelConfig[ClassifyAppType])
}

// Equals 比对标签提供应用配置
func (sc *SummaryConfig) Equals(cfg *SummaryConfig) []AppConfigDiff {
	var diffs []AppConfigDiff
	if len(sc.Model) == 0 {
		var releaseDetail AppModelDetail
		for key, v := range cfg.Model {
			if key == SummaryNormal {
				previewDetail := transformModelConfig(v)
				diffs = append(diffs, releaseDetail.EqualsModelName(&previewDetail, SummaryAppType)...)
			}
		}
	} else {
		for k, v := range sc.Model {
			c, ok := cfg.Model[k]
			if !ok {
				continue
			}
			if k == SummaryNormal {
				releaseDetail := transformModelConfig(v)
				previewDetail := transformModelConfig(c)
				diffs = append(diffs, releaseDetail.EqualsModelName(&previewDetail, SummaryAppType)...)
			}
		}
	}
	if sc.Method != cfg.Method {
		lastMethod := utils.When(sc.Method == AppMethodStream, Stream, NotStream)
		method := utils.When(cfg.Method == AppMethodStream, Stream, NotStream)
		diffs = append(diffs, newDiffConfig(ConfigItemMethod, lastMethod, method, ""))
	}
	if sc.Requirement != cfg.Requirement {
		lastRequirement := utils.When(sc.Requirement == AppRequirementSummary, TextSummary, TextCustom)
		requirement := utils.When(cfg.Requirement == AppRequirementSummary, TextSummary, TextCustom)
		diffs = append(diffs, newDiffConfig(ConfigItemRequirement, lastRequirement, requirement, ""))
	}
	if sc.RequireCommand != cfg.RequireCommand {
		diffs = append(diffs, newDiffConfig(ConfigItemRequirementCommand, sc.RequireCommand,
			cfg.RequireCommand, ""))
	}
	return diffs
}

// GetDefaultModelList 获取默认配置模型列表
func (sc *SummaryConfig) GetDefaultModelList() map[string]*pb.AppModelInfo {
	if len(config.App().RobotDefault.AppDefaultModelConfig) == 0 {
		return make(map[string]*pb.AppModelInfo)
	}
	if _, ok := config.App().RobotDefault.AppDefaultModelConfig[SummaryAppType]; !ok {
		return make(map[string]*pb.AppModelInfo)
	}
	return getModelList(config.App().RobotDefault.AppDefaultModelConfig[SummaryAppType])
}

// LkeRepositoryConfigLabel 标签配置，权限没有标签，暂时加在这里，不影响权限里面配置
func LkeRepositoryConfigLabel() string {
	return lkeRepositoryConfigLabel
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
func Marshal2StringUnescapeHTMLNoErr(v interface{}) string {
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
func UnmarshalStr(data string, v interface{}) error {
	d := json.NewDecoder(bytes.NewBufferString(data))
	d.UseNumber()
	return d.Decode(v)
}

func getModelList(list []config.AppModelInfo) map[string]*pb.AppModelInfo {
	if len(list) == 0 {
		return make(map[string]*pb.AppModelInfo)
	}
	modelList := make(map[string]*pb.AppModelInfo, len(list))
	for i := range list {
		modelList[list[i].Name] = &pb.AppModelInfo{
			ModelName:         list[i].Name,
			Prompt:            list[i].Prompt,
			PromptWordsLimit:  list[i].PromptWordsLimit,
			HistoryLimit:      list[i].HistoryLimit,
			HistoryWordsLimit: list[i].HistoryWordsLimit,
			ServiceName:       list[i].ServiceName,
			IsEnabled:         true,
		}
	}
	return modelList
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
		lastConfig := utils.When(v.IsEnabled, Open, ShutDown)
		newConfig := utils.When(previewMap[key].IsEnabled, Open, ShutDown)
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
					lastConfig = utils.When(k.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
					newConfig = utils.When(cfg.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
					diff = append(diff, newDiffConfig(ConfigItemReplyFlexibility, lastConfig,
						newConfig, ""))
				}
			}
			if v.DocType == SearchFilterType {
				if k.ShowSearchEngine != cfg.ShowSearchEngine {
					lastConfig = utils.When(k.ShowSearchEngine, ShowOn, ShowOff)
					newConfig = utils.When(cfg.ShowSearchEngine, ShowOn, ShowOff)
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
							lastConfig := utils.When(k.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
							newConfig := utils.When(cfg.ReplyFlexibility == AppDirectReply, DirectReply, NotDirectReply)
							diff = append(diff, newDiffConfig(ConfigItemReplyFlexibility, lastConfig, newConfig, ""))
						}
					}
					if v.DocType == SearchFilterType {
						if k.ShowSearchEngine != cfg.ShowSearchEngine {
							lastConfig := utils.When(k.ShowSearchEngine, ShowOn, ShowOff)
							newConfig := utils.When(cfg.ShowSearchEngine, ShowOn, ShowOff)
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

// IsPBAppModelInfoEqual 比较pb.AppModelInfo数据是否相等
func IsPBAppModelInfoEqual(x, y interface{}) bool {
	return cmp.Equal(x, y,
		cmpopts.EquateEmpty(),
		protocmp.Transform())
}

// IsPBAppFiltersEqual 比较pb.AppFilters数据是否相等
func IsPBAppFiltersEqual(x, y interface{}) bool {
	return cmp.Equal(x, y,
		cmpopts.EquateEmpty(),
		protocmp.Transform(),
		cmpopts.SortSlices(func(i, j *pb.AppFiltersInfo) bool { return i.GetDocType() < j.GetDocType() }),
	)
}

// IsPBAppSplitDocEqual 比较pb.AppSplitDoc数据是否相等
func IsPBAppSplitDocEqual(x, y interface{}) bool {
	return cmp.Equal(x, y,
		cmpopts.EquateEmpty(),
		protocmp.Transform(),
		cmpopts.SortSlices(func(i, j string) bool { return i < j }))
}

// IsPBAppSearchVectorEqual 比较pb.AppSearchVector数据是否相等
func IsPBAppSearchVectorEqual(x, y interface{}) bool {
	return cmp.Equal(x, y,
		cmpopts.EquateEmpty(),
		protocmp.Transform())
}

// KnowledgeQaConfigInfo 问答知识库配置信息
type KnowledgeQaConfigInfo struct {
	TransferKeywords      []string
	Embedding             config.RobotEmbedding
	IsEmbeddingDefault    bool
	SearchVector          *config.SearchVector
	IsSearchVectorDefault bool
	DocSplit              config.RobotDocSplit
	IsDocSplitDefault     bool
	Models                config.AppModel
	IsModelDefault        bool
	Filters               config.RobotFilters
	IsFilterDefault       bool
}

// GetOldModels .
func (r *App) GetOldModels() (config.AppModel, bool, error) {
	if len(r.Model) == 0 {
		return config.App().RobotDefault.AppModelConfig.KnowledgeQaAppModel, true, nil
	}
	robotModel := make(config.AppModel)
	if err := jsoniter.UnmarshalFromString(r.Model, &robotModel); err != nil {
		return robotModel, false, err
	}
	return robotModel, false, nil
}

// GetOldFilters .
func (r *App) GetOldFilters() (config.RobotFilters, bool, error) {
	if len(r.Filters) == 0 {
		return config.App().RobotDefault.Filters, true, nil
	}
	robotFilters := make(config.RobotFilters)
	if err := jsoniter.UnmarshalFromString(r.Filters, &robotFilters); err != nil {
		return robotFilters, false, err
	}
	return robotFilters, false, nil
}

// GetOldDocSplitConf .
func (r *App) GetOldDocSplitConf() (config.RobotDocSplit, bool, error) {
	if len(r.SplitDoc) == 0 {
		return config.App().RobotDefault.DocSplit, true, nil
	}
	robotDocSplit := make(config.RobotDocSplit)
	if err := jsoniter.UnmarshalFromString(r.SplitDoc, &robotDocSplit); err != nil {
		return robotDocSplit, false, err
	}
	return robotDocSplit, false, nil
}

// GetOldSearchVector .
func (r *App) GetOldSearchVector() (*config.SearchVector, bool, error) {
	searchVector := config.App().RobotDefault.SearchVector
	if len(r.SearchVector) == 0 {
		return &searchVector, true, nil
	}
	if err := jsoniter.UnmarshalFromString(r.SearchVector, &searchVector); err != nil {
		return nil, false, err
	}
	return &searchVector, false, nil
}

// GetAppModelName 获取应用的模型名称
func (r *App) GetAppModelName(isRelease bool) string {
	knowledgeQaConfig := r.PreviewDetails.AppConfig.KnowledgeQaConfig
	if isRelease {
		knowledgeQaConfig = r.ReleaseDetails.AppConfig.KnowledgeQaConfig
	}
	if knowledgeQaConfig == nil {
		return ""
	}
	for k, v := range knowledgeQaConfig.Model {
		if k == AppModelNormal {
			return v.ModelName
		}
	}
	return ""
}

// GetName 获取应用名称
func (r *App) GetName(scenes uint32) string {
	if r == nil {
		return ""
	}
	switch scenes {
	case AppTestScenes:
		return r.PreviewDetails.BaseConfig.Name
	case AppReleaseScenes:
		return r.ReleaseDetails.BaseConfig.Name
	}
	return ""
}

// GetAvatar 获取应用头像
func (r *App) GetAvatar(scenes uint32) string {
	if r == nil {
		return ""
	}
	switch scenes {
	case AppTestScenes:
		return r.PreviewDetails.BaseConfig.Avatar
	case AppReleaseScenes:
		return r.ReleaseDetails.BaseConfig.Avatar
	}
	return ""
}

// GetSearchRangeCondition 获取应用检索范围条件
func (r *App) GetSearchRangeCondition(scenes uint32) string {
	if r == nil { // 默认为and
		return AppSearchConditionAnd
	}
	switch scenes {
	case AppTestScenes:
		if r.PreviewDetails.AppConfig.KnowledgeQaConfig == nil {
			return AppSearchConditionAnd
		}
		return r.PreviewDetails.AppConfig.KnowledgeQaConfig.SearchRange.Condition
	case AppReleaseScenes:
		if r.ReleaseDetails.AppConfig.KnowledgeQaConfig == nil {
			return AppSearchConditionAnd
		}
		return r.ReleaseDetails.AppConfig.KnowledgeQaConfig.SearchRange.Condition
	}
	return AppSearchConditionAnd
}
