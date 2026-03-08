package entity

import "git.woa.com/adp/kb/kb-config/internal/config"

// enum for Release
const (
	// ReleaseVectorSuccessCallbackFlag vector服务的回调请求成功
	ReleaseVectorSuccessCallbackFlag = 1 << iota
	// ReleaseTaskFlowSuccessCallbackFlag 任务型服务的回调请求成功
	ReleaseTaskFlowSuccessCallbackFlag
	// ReleaseNoServeCallbackFlag 无服务回调
	ReleaseNoServeCallbackFlag = 0
	// ReleaseAllServeCallbackFlag 所有服务回调请求都成功
	ReleaseAllServeCallbackFlag = 3

	// ReleaseDBSuccessCallbackFlag db服务的回调请求成功
	ReleaseDBSuccessCallbackFlag = 8

	// ReleaseVectorCallback vector服务的回调请求
	ReleaseVectorCallback = uint32(0)
	// ReleaseTaskConfigCallback 任务型服务的回调请求
	ReleaseTaskConfigCallback = uint32(1)
	// SimilarVersionID 相似向量库版本ID
	SimilarVersionID = uint64(1)
	// ReviewVersionID 评测向量库版本ID
	ReviewVersionID = uint64(2)
	// SegmentSimilarVersionID 文档段相似向量库版本ID
	SegmentSimilarVersionID = uint64(3)
	// SegmentReviewVersionID 文档段评测向量库版本ID
	SegmentReviewVersionID = uint64(4)
	// RejectedQuestionSimilarVersionID 拒答问题相似向量库版本ID
	RejectedQuestionSimilarVersionID = uint64(5)
	// RejectedQuestionReviewVersionID 拒答问题评测向量库版本ID
	RejectedQuestionReviewVersionID = uint64(6)
	// SearchEngineVersionID 搜索引擎的版本ID
	SearchEngineVersionID = uint64(7)
	// SearchGlobalVersionID 全局干预知识库
	SearchGlobalVersionID = uint64(8)
	// RealtimeSegmentVersionID 实时文档段向量库版本ID
	RealtimeSegmentVersionID = uint64(9)
	// SegmentImageReviewVersionID 文档段图片评测向量库版本ID
	SegmentImageReviewVersionID = uint64(10)
	// RealtimeSegmentImageVersionID 实时文档段图片向量库版本ID
	RealtimeSegmentImageVersionID = uint64(11)
	// DbSourceVersionID 外部数据库向量库版本ID
	DbSourceVersionID = uint64(12)

	// TaskConfigBusinessNameTextRobot 任务型业务名称文本客服
	TaskConfigBusinessNameTextRobot = "TEXT_ROBOT"
	// TaskConfigEventPrepare 任务型准备事件标识
	TaskConfigEventPrepare = "PREPARE"
	// TaskConfigEventCollect 任务型采集事件标识
	TaskConfigEventCollect = "COLLECT"
	// TaskConfigEventRelease 任务型发布事件标识
	TaskConfigEventRelease = "RELEASE"
	// TaskConfigEventPause 任务型暂停事件标识
	TaskConfigEventPause = "PAUSE"
	// TaskConfigEventRetry 任务型重试事件标识
	TaskConfigEventRetry = "RETRY"
	// TaskConfigEventRollback 任务型回滚事件标识
	TaskConfigEventRollback = "ROLLBACK"
)

type EmbeddingVersionID uint64

// RelatedID    关联ID
type RelatedID uint64

// AppID 应用ID
type AppID uint64

// DocType 文档类型
type DocType uint32

// AppVersionID 应用版本
type AppVersionID uint64

// DocID 文档ID
type DocID uint64

// DocSegmentID 文档段ID
type DocSegmentID uint64

// QAID    QAID
type QAID uint64

const (
	// DocTypeQA QA
	DocTypeQA = 1
	// DocTypeSegment 文档段
	DocTypeSegment = 2
	// DocTypeRejectedQuestion 拒答问题
	DocTypeRejectedQuestion = 3
	// DocTypeSearchEngine 搜索引擎检索
	DocTypeSearchEngine = 4
	// DocTypeTaskFlow 任务流
	DocTypeTaskFlow = 5
	// DocTypeImage 文档段图片
	DocTypeImage = 6
	// DocTypeTextSearchImage 文搜图 -- 底层检索服务retrieval需要使用 @harryhlli
	DocTypeTextSearchImage = 7 // 文搜图,检索filter文搜图，但检索的向量库是图片向量库，即doc_type=6
	// DocTypeDB 直连的外部数据库
	DocTypeDB = 8
)

const (
	// QATypeReleaseStandard qa类型-标准问答
	QATypeReleaseStandard = 0
	// QATypeReleaseSimilar qa类型-相似问
	QATypeReleaseSimilar = 1
)

// enum for ReleaseQA
const (

	// ForbidRelease 禁止发布
	ForbidRelease = uint32(0)
	// AllowRelease 允许发布
	AllowRelease = uint32(1)
	// ReleaseQAMessageAuditTimeOut 问答发布超时
	ReleaseQAMessageAuditTimeOut = "审核超时"
)

const (
	EmbeddingVersion11    = uint64(11)
	EmbeddingVersion12    = uint64(12)
	EmbeddingVersion10000 = uint64(10000)
)

// enum for ReleaseConfig
const (

	// ReleaseConfigAuditStatusSuccess 审核成功
	ReleaseConfigAuditStatusSuccess = uint32(3)
	// ConfigReleaseStatusAuditing  审核中
	ConfigReleaseStatusAuditing = uint32(7)
	// ConfigReleaseStatusAuditNotPass  审核不通过
	ConfigReleaseStatusAuditNotPass = uint32(8)
	// ConfigReleaseStatusAppealIng  人工审核中
	ConfigReleaseStatusAppealIng = uint32(9)
	// ConfigReleaseStatusAppealSuccess 人工审核通过
	ConfigReleaseStatusAppealSuccess = uint32(10)
	// ConfigReleaseStatusAppealFail 人工审核不通过
	ConfigReleaseStatusAppealFail = uint32(11)
	// ConfigReleaseStatusAppealSuccessMsg 人工审核通过
	ConfigReleaseStatusAppealSuccessMsg = "人工审核通过"
	// ConfigReleaseStatusAppealFailMsg 人工审核不通过
	ConfigReleaseStatusAppealFailMsg = "人工审核不通过"
)

const (
	// ByteToGB 字节到GB
	ByteToGB = 1024 * 1024 * 1024
)

// GetType 返回类型
func GetType(docType uint32) uint64 {
	typ := ReviewVersionID
	if docType == DocTypeSegment {
		typ = SegmentReviewVersionID
	}
	return typ
}

func GetEmbeddingVersion(embeddingModel string) uint64 {
	if embeddingModel == "" {
		return config.App().EmbeddingConfig.DefaultEmbeddingVersion
	}
	if embeddingVersion, ok := config.App().EmbeddingConfig.EmbeddingModelVersion[embeddingModel]; ok {
		return embeddingVersion
	}
	return config.App().EmbeddingConfig.CustomEmbeddingVersion
}
