package kb

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

type KnowledgeBase struct {
	ID             uint64
	CorpBizID      uint64
	KnowledgeBizId uint64    // 知识库业务id
	ProcessingFlag uint64    // 知识库处理中状态标记
	IsDeleted      bool      // 0正常，1已删除
	CreateTime     time.Time // 创建时间
	UpdateTime     time.Time // 更新时间
}

// AppShareKnowledge 应用引用共享库表
type AppShareKnowledge struct {
	ID             uint64
	AppBizID       uint64    // 应用业务id
	CorpBizID      uint64    // 企业业务ID
	KnowledgeBizID uint64    // 引用的共享知识库业务id
	UpdateTime     time.Time // 更新时间
	CreateTime     time.Time // 创建时间
}

// SharedKnowledgeInfo 共享知识库信息
type SharedKnowledgeInfo struct {
	ID                   uint64    `db:"id"`               // 自增ID
	CorpBizID            uint64    `db:"corp_biz_id"`      // 企业业务ID
	BusinessID           uint64    `db:"business_id"`      // 共享知识库业务ID
	Name                 string    `db:"name"`             // 共享知识库名称
	Description          string    `db:"description"`      // 共享知识库描述
	UserBizID            uint64    `db:"user_biz_id"`      // 用户ID
	UserName             string    `db:"user_name"`        // 用户名称(用于分页搜索)
	EmbeddingModel       string    `db:"embedding_model"`  // Embedding模型
	QaExtractModel       string    `db:"qa_extract_model"` // 问答对抽取模型
	IsDeleted            bool      `db:"is_deleted"`       // 0:未删除 1:已删除
	CreateTime           time.Time `db:"create_time"`      // 创建时间
	UpdateTime           time.Time `db:"update_time"`      // 更新时间
	SpaceId              string    `db:"space_id"`         // 空间id
	OwnerStaffID         uint64    `db:"owner_staff_id"`   // 所有者的员工ID
	KnowledgeSchemaModel string    `db:"-" gorm:"-"`       // 知识库Schema模型
	OwnerStaffName       string    `db:"-" gorm:"-"`       // 所有者的员工名称
}

// CreateSharedKnowledgeParams 创建共享知识库的参数
type CreateSharedKnowledgeParams struct {
	CorpBizID      uint64 // 企业业务ID
	KnowledgeBizID uint64 // 共享知识库业务ID
	Name           string // 共享知识库名称
	Description    string // 共享知识库描述
	UserBizID      uint64 // 用户ID
	UserName       string // 用户名称
	EmbeddingModel string // Embedding模型
	SpaceID        string // 空间id
	OwnerStaffID   uint64 // 所有者的员工ID
}

type ShareKnowledgeFilter struct {
	CorpBizID   uint64
	BizIds      []uint64
	WithDeleted *bool
}

type KnowledgeConfig struct {
	ID               uint64    `json:"id"`
	CorpBizID        uint64    `json:"corp_biz_id"`
	KnowledgeBizID   uint64    `json:"knowledge_biz_id"`
	Type             uint32    `json:"type"`
	Config           string    `json:"config"`
	IsDeleted        bool      `json:"is_deleted"`
	CreateTime       time.Time `json:"create_time"`
	UpdateTime       time.Time `json:"update_time"`
	AppBizID         uint64    `json:"app_biz_id"`
	PreviewConfig    string    `json:"preview_config"`
	ShareKbName      string    `json:"share_kb_name"`      // 共享知识库的名称
	IsUpdateReleased bool      `json:"is_update_released"` // 是否更新已发布
}

type KnowledgeConfigHistory struct {
	ID             uint64
	CorpBizID      uint64
	KnowledgeBizID uint64
	AppBizID       uint64
	Type           uint32
	VersionID      uint64
	ReleaseJSON    string
	IsRelease      bool
	IsDeleted      bool
	CreateTime     time.Time
	UpdateTime     time.Time
}

type KnowledgeConfigHistoryFilter struct {
	ID             uint64
	CorpBizID      uint64
	KnowledgeBizID uint64
	AppBizID       uint64
	Type           uint32
	VersionID      uint64
	IsRelease      *bool
}

type KnowledgeConfigDiff struct {
	ConfigItem string // 应用配置项名称
	Action     uint32 // 变更状态
	NewValue   string // 变更后内容
	LastValue  string // 变更前内容
	Content    string // 变更内容
}

// HasProcessingFlag 判断知识库是否包含指定处理中状态标记
func (d *KnowledgeBase) HasProcessingFlag(flag uint64) bool {
	if d == nil {
		return false
	}
	if flag == 0 {
		return true
	}
	if d.ProcessingFlag&flag > 0 {
		return true
	}
	return false
}

// AddProcessingFlag 添加知识库处理中状态标记
func (d *KnowledgeBase) AddProcessingFlag(flags []uint64) {
	if len(flags) == 0 {
		return
	}
	for _, attr := range flags {
		d.ProcessingFlag |= attr
	}
	return
}

// RemoveProcessingFlag 去除知识库处理中状态标记
func (d *KnowledgeBase) RemoveProcessingFlag(flags []uint64) {
	if len(flags) == 0 {
		return
	}
	for _, flag := range flags {
		d.ProcessingFlag = d.ProcessingFlag &^ flag
	}
	return
}

// GetLoginUinAndSubAccountUin 获取uin和subAccountUin
func GetLoginUinAndSubAccountUin(ctx context.Context) (string, string) {
	uin := contextx.Metadata(ctx).LoginUin()
	subAccountUin := contextx.Metadata(ctx).LoginSubAccountUin()
	if contextx.Metadata(ctx).SID() == CloudSID {
		uin = contextx.Metadata(ctx).Uin()
		subAccountUin = contextx.Metadata(ctx).SubAccountUin()
	}
	return uin, subAccountUin
}

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
	// ImageToImageFilterTypeKey 图搜图检索类型
	ImageToImageFilterTypeKey = "imageToImage"
	// TextToImageFilterTypeKey 文搜图检索类型
	TextToImageFilterTypeKey = "textToImage"
	// DBFilterTypeKey 数据库检索类型
	DBFilterTypeKey = "database"
	// AppMethodStream .
	AppMethodStream = 1
	// AppDirectReply .
	AppDirectReply = 1
	// AppRequirementSummary .
	AppRequirementSummary = 1
	// AppRequirementTypeCustom 自定义要求摘要类型
	AppRequirementTypeCustom = 2

	// ConfigItemName 配置项-应用名称
	ConfigItemName = i18nkey.KeyAppName
	// ConfigItemAvatar 配置项-应用图标
	ConfigItemAvatar = i18nkey.KeyAvatar
	// ConfigItemDescription 配置项-应用描述
	ConfigItemDescription = i18nkey.KeyAppDescription
	// ConfigItemGreeting 配置项-欢迎语
	ConfigItemGreeting = i18nkey.KeyWelcomeMessage
	// ConfigItemRoleDescription 配置项-角色描述
	ConfigItemRoleDescription = i18nkey.KeyRoleDescription
	// ConfigItemMethod 配置项-输出方式
	ConfigItemMethod = i18nkey.KeyOutputMethod
	// ConfigItemUseGeneralKnowledge 配置项-保守回复开关
	ConfigItemUseGeneralKnowledge = i18nkey.KeyConservativeReplySwitch
	// ConfigItemBareAnswer 配置项-默认问题回复
	ConfigItemBareAnswer = i18nkey.KeyConservativeReplyContent
	// ConfigItemTopN 配置项-最大召回数量
	ConfigItemTopN = i18nkey.KeyMaxRecallNumWithParam
	// ConfigItemConfidence 配置项-检索匹配度
	ConfigItemConfidence = i18nkey.KeyRetrievalMatchingDegreeWithParam
	// ConfigItemReplyFlexibility 配置项-回复灵活度
	ConfigItemReplyFlexibility = i18nkey.KeyReplyFlexibility
	// ConfigItemIsEnabled 配置项-配置开关
	ConfigItemIsEnabled = i18nkey.KeyKnowledgeSourceConfigWithParam
	// ConfigItemShowSearchEngine 配置项-搜索引擎展示状态
	ConfigItemShowSearchEngine = i18nkey.KeySearchEngineStatus
	// ConfigItemHistoryLimit 配置项-上下文改写
	ConfigItemHistoryLimit = i18nkey.KeyContextRewriting
	// ConfigItemHistoryRecordLimit 配置项-上下文记忆轮数
	ConfigItemHistoryRecordLimit = i18nkey.KeyContextMemoryRounds
	// ConfigItemModel 配置项-模型名称
	ConfigItemModel = i18nkey.KeyModelSelectionWithParam
	// ConfigItemLabel  配置项-标签配置
	ConfigItemLabel = i18nkey.KeyTagConfig
	// ConfigItemRequirement  输出要求
	ConfigItemRequirement = i18nkey.KeyOutputRequirement
	// ConfigItemRequirementCommand  自定义输出要求
	ConfigItemRequirementCommand = i18nkey.KeyCustomOutputRequirement
	// ConfigItemUseQuestionClarify 问题澄清
	ConfigItemUseQuestionClarify = i18nkey.KeyQuestionClarification
	// ConfigItemQuestionClarifyKeywords 澄清关键词
	ConfigItemQuestionClarifyKeywords = i18nkey.KeyClarificationKeywords
	// ConfigItemSearchRange 默认知识库检索范围设置
	ConfigItemSearchRange = i18nkey.KeyDefaultKnowledgeBaseRetrievalRangeSetting
	// ConfigItemPattern 应用模式
	ConfigItemPattern = i18nkey.KeyAppMode
	// ConfigItemWorkflowID 指定工作流
	ConfigItemWorkflowID = i18nkey.KeyDesignatedWorkflow
	// ConfigItemTemperature 模型温度
	ConfigItemTemperature = i18nkey.KeyTemperatureWithParam
	// ConfigItemTopP top-p
	ConfigItemTopP = i18nkey.KeyTopPWithParam
	// ConfigItemSeed 随机种子
	ConfigItemSeed = i18nkey.KeyRandomSeedWithParam
	// ConfigItemPresencePenalty 存在惩罚
	ConfigItemPresencePenalty = i18nkey.KeyPresencePenaltyWithParam
	// ConfigItemFrequencyPenalty 频率惩罚
	ConfigItemFrequencyPenalty = i18nkey.KeyFrequencyPenaltyWithParam
	// ConfigItemRepetitionPenalty 重复惩罚
	ConfigItemRepetitionPenalty = i18nkey.KeyRepeatPenaltyWithParam
	// ConfigItemMaxTokens 最大输出长度
	ConfigItemMaxTokens = i18nkey.KeyMaxOutputLengthWithParam
	// ConfigItemStopSequences 停止序列
	ConfigItemStopSequences = i18nkey.KeyStopSequenceWithParam
	// ConfigItemReplyFormat 输出格式
	ConfigItemReplyFormat = i18nkey.KeyOutputFormatWithParam
	// ConfigItemStrategyType 检索策略
	ConfigItemStrategyType = i18nkey.KeyRetrievalStrategy
	// ConfigItemTableEnhancement Excel检索增强
	ConfigItemTableEnhancement = i18nkey.KeyExcelRetrievalEnhancement
	// ConfigItemChatAuditSwitch 配置项-对话内容审核
	ConfigItemChatAuditSwitch = i18nkey.KeyDialogContentReview
	// ConfigItemIntentAchievement 意图达成优先级
	ConfigItemIntentAchievement = i18nkey.KeyIntentAchievementPriority
	// ConfigItemImageTextRetrieval 图文检索
	ConfigItemImageTextRetrieval = i18nkey.KeyImageTextRetrieval
	// ConfigItemAsyncWorkflow 工作流异步调用
	ConfigItemAsyncWorkflow = i18nkey.KeyAsynchronousCall
	// ConfigItemShareKnowledgeSearchRange  共享知识库检索范围设置
	ConfigItemShareKnowledgeSearchRange = i18nkey.KeySharedKnowledgeBaseRetrievalRangeSetting
	// ConfigItemReferenceKnowledge 引用共享知识库
	ConfigItemReferenceKnowledge = i18nkey.KeyReferenceSharedKnowledgeBase
	// ConfigItemKnowledgeSchema 知识库Schema
	ConfigItemKnowledgeSchema = i18nkey.KeyKnowledgeBaseSchema
	// ConfigItemVar 变量设置
	ConfigItemVar = i18nkey.KeyVariableSettings

	// ConfigItemLongMemory 长期记忆
	ConfigItemLongMemory = i18nkey.KeyLongTermMemory

	// ConfigItemLongMemoryDay 长期记忆时效
	ConfigItemLongMemoryDay = i18nkey.KeyLongTermMemoryDuration

	// ConfigItemBackgroundImageOriginal 背景图原始图
	ConfigItemBackgroundImageOriginal = i18nkey.KeyBackgroundImageOriginal
	// ConfigItemBackgroundImageLandscape 背景图WEB效果
	ConfigItemBackgroundImageLandscape = i18nkey.KeyBackgroundImageWebEffect
	// ConfigItemBackgroundImagePortrait 背景图移动端效果
	ConfigItemBackgroundImagePortrait = i18nkey.KeyBackgroundImageMobileEffect
	// ConfigItemBackgroundImageThemeColor 背景图主题色
	ConfigItemBackgroundImageThemeColor = i18nkey.KeyBackgroundImageThemeColor

	// ConfigItemOpeningQuestions 示例问题
	ConfigItemOpeningQuestions = i18nkey.KeyExampleQuestions

	// ConfigItemWebSearchSwitch 配置项-海外联网搜索
	ConfigItemWebSearchSwitch = i18nkey.KeyWebSearch
	// ConfigItemWebSearchAPIKey 配置项-海外联网搜索apikey
	ConfigItemWebSearchAPIKey = i18nkey.KeyWebSearchAPIKey
	// ConfigItemWebSearchTopN 配置项-海外联网搜索TopN
	ConfigItemWebSearchTopN = i18nkey.KeyWebSearchRecallInterfaceFragment
	// ConfigItemWebSearchProvider 配置项-海外联网搜索信源
	ConfigItemWebSearchProvider = i18nkey.KeyWebSearchAPIKeyProvider

	// ConfigItemAgentCollaborationModel Agent协同方式
	ConfigItemAgentCollaborationModel = i18nkey.KeyAgentCollaborationMethod

	ConfigItemSearchStrategyReRankModelSwitch = i18nkey.KeyRetrievalRecallResultReordering

	ConfigItemSearchStrategyReRankModelName    = i18nkey.KeyRetrievalRecallReorderingModel
	ConfigItemSearchStrategyEmbeddingModelName = i18nkey.KeyRetrievalRecallVectorModel

	ConfigItemAdvanceReRankModel     = i18nkey.KeyAdvancedSettingsReorderingModel
	ConfigItemAdvanceReRankRecallNum = i18nkey.KeyAdvancedSettingsReorderingModelRecallCount

	ConfigKnowledgeModelEmbeddingModel = i18nkey.KeyKnowledgeBaseModelVectorModel
	ConfigKnowledgeModelQAExtractModel = i18nkey.KeyKnowledgeBaseModelQAPairGenerationModel
	ConfigKnowledgeModelSchemaModel    = i18nkey.KeyKnowledgeBaseModelSchemaGenerationModel
	ConfigKnowledgeModelFileParseModel = i18nkey.KeyKnowledgeBaseModelFileParseModel

	// DirectReply 已采纳答案直接回复
	DirectReply = i18nkey.KeyDirectReplyWithAdoptedAnswer
	// NotDirectReply 已采纳答案润色回复
	NotDirectReply = i18nkey.KeyPolishedReplyWithAdoptedAnswer
	// TextSummary 文本总结
	TextSummary = i18nkey.KeyTextSummary
	// TextCustom 自定义要求
	TextCustom = i18nkey.KeyCustomRequirement

	// Stream 流式输出
	Stream = i18nkey.KeyStreamingOutput
	// NotStream 非流式输出
	NotStream = i18nkey.KeyNonStreamingOutput

	VoiceInteract = i18nkey.KeyVoiceInteraction
	VoiceCall     = i18nkey.KeyVoiceCall
	VoiceName     = i18nkey.KeyToneColor
	DigitalHuman  = i18nkey.KeyDigitalHuman

	// Open 开启
	Open = i18nkey.KeyEnable
	// ShutDown 关闭
	ShutDown = i18nkey.KeyDisable
	// ShowOn 显示
	ShowOn = i18nkey.KeyShow
	// ShowOff 隐藏
	ShowOff = i18nkey.KeyHide
	// AvatarChange 头像变更
	AvatarChange = i18nkey.KeyAvatarChanged

	qaFilterType           = i18nkey.KeyQA
	docFilterType          = i18nkey.KeyDocument
	rejectedFilterType     = i18nkey.KeyRejection
	searchFilterType       = i18nkey.KeySearchEnhancement
	taskFlowFilterType     = i18nkey.KeyTaskFlow
	imageToImageFilterType = i18nkey.KeyImageSearchByImage
	textToImageFilterType  = i18nkey.KeyImageSearchByText
	dbFilterType           = i18nkey.KeyDatabase

	// TemperatureEnable 温度配置
	TemperatureEnable = i18nkey.KeyTemperatureEnable
	// Temperature 温度
	Temperature = i18nkey.KeyTemperature
	// TopPEnable TopP配置
	TopPEnable = i18nkey.KeyTopPEnable
	// TopP TopP
	TopP = i18nkey.KeyTopP
	// MaxOutputLengthEnable 最大输出长度配置
	MaxTokensEnable = i18nkey.KeyMaxTokensEnable
	// MaxOutputLength 最大输出长度
	MaxTokens = i18nkey.KeyMaxTokens
	// RandomSeedEnable 随机种子配置
	RandomSeedEnable = i18nkey.KeyRandomSeedEnable
	// RandomSeed 随机种子
	RandomSeed = i18nkey.KeyRandomSeed
	// PunishmentFrequencyEnable 存在惩罚配置
	PresencePenaltyEnable = i18nkey.KeyPresencePenaltyEnable
	// PunishmentFrequency 存在惩罚
	PresencePenalty = i18nkey.KeyPresencePenalty
	// PunishmentFrequencyEnable 频率惩罚配置
	FrequencyPenaltyEnable = i18nkey.KeyFrequencyPenaltyEnable
	// PunishmentFrequency 频率惩罚
	FrequencyPenalty = i18nkey.KeyFrequencyPenalty
	// PunishmentFrequencyEnable 重复惩罚配置
	RepetitionPenaltyEnable = i18nkey.KeyRepetitionPenaltyEnable
	// PunishmentFrequency 重复惩罚
	RepetitionPenalty = i18nkey.KeyRepetitionPenalty
	// StopSequenceEnable 停止序列配置
	StopSequenceEnable = i18nkey.KeyStopSequenceEnable
	// StopSequence 停止序列
	StopSequence = i18nkey.KeyStopSequence
	// OutputFormat 输出格式
	ReplyFormat = i18nkey.KeyReplyFormat
	// HistoryLimit 上下文轮数
	HistoryLimit = i18nkey.KeyHistoryLimit
	// ConfigKnowledgeModelNL2SQLModel 知识库模型-NL2SQL改写模型
	ConfigKnowledgeModelNL2SQLModel = i18nkey.KeyKnowledgeBaseModelNL2SQLModel
	// KeyEdit 修改为
	KeyEdit = i18nkey.KeyEdit
	// DocCategory 文档分类
	DocCategory = i18nkey.KeyDocCategory
	// QACategory QA分类
	QACategory = i18nkey.KeyQACategory
)

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
	case ImageToImageFilterType:
		return imageToImageFilterType
	case TextToImageFilterType:
		return textToImageFilterType
	case DBFilterType:
		return dbFilterType
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
	case ImageToImageFilterTypeKey:
		return ImageToImageFilterType
	case TextToImageFilterTypeKey:
		return TextToImageFilterType
	case DBFilterTypeKey:
		return DBFilterType
	}
	return 0
}
