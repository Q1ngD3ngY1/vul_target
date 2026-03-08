package release

const (
	// 默认无
	ReleaseTypeNone = uint32(0)
	// 发布配置类型
	ReleaseTypeConfig = uint32(1)
	// 发布文档类型
	ReleaseTypeDocument = uint32(2)
	// 发布QA类型
	ReleaseTypeQA = uint32(3)
	// 发布拒绝问答类型
	ReleaseTypeRejectedQuestion = uint32(4)
	// 发布文档标签类型
	ReleaseTypeLabel = uint32(5)
	// 发布DB类型
	ReleaseDBType = uint32(6)
)

const (
	// ReleaseStatusInit 待发布
	ReleaseStatusInit = uint32(1)
	// ReleaseStatusPending 发布中
	ReleaseStatusPending = uint32(2)
	// ReleaseStatusSuccess 发布成功
	ReleaseStatusSuccess = uint32(3)
	// ReleaseStatusFail 发布失败
	ReleaseStatusFail = uint32(4)
	// ReleaseStatusAudit 审核中
	ReleaseStatusAudit = uint32(5)
	// ReleaseStatusAuditSuccess 审核成功
	ReleaseStatusAuditSuccess = uint32(6)
	// ReleaseStatusAuditFail 审核失败
	ReleaseStatusAuditFail = uint32(7)
	// ReleaseStatusSuccessCallback 发布成功回调处理中
	ReleaseStatusSuccessCallback = uint32(8)
	// ReleaseStatusPause 发布暂停
	ReleaseStatusPause = uint32(9)
	// ReleaseStatusAppealIng 申诉审核中
	ReleaseStatusAppealIng = uint32(10)
	// ReleaseStatusAppealSuccess 申诉审核通过
	ReleaseStatusAppealSuccess = uint32(11)
	// ReleaseStatusAppealFail 申诉审核不通过
	ReleaseStatusAppealFail = uint32(12)
)

// ReleaseAction 发布动作
const (
	// ReleaseActionAdd 新增
	ReleaseActionAdd = uint32(1)
	// ReleaseActionUpdate 更新
	ReleaseActionUpdate = uint32(2)
	// ReleaseActionDelete 删除
	ReleaseActionDelete = uint32(3)
	// ReleaseActionPublish 发布
	ReleaseActionPublish = uint32(4)
)

const (
	// ReleaseQAAuditStatusInit 未审核
	ReleaseQAAuditStatusInit = uint32(1)
	// ReleaseQAAuditStatusDoing 审核中
	ReleaseQAAuditStatusDoing = uint32(2)
	// ReleaseQAAuditStatusSuccess 审核成功
	ReleaseQAAuditStatusSuccess = uint32(3)
	// ReleaseQAAuditStatusFail 审核失败
	ReleaseQAAuditStatusFail = uint32(4)
	// ReleaseQAAppealStatusDoing 申诉人工审核中
	ReleaseQAAppealStatusDoing = uint32(5)
	// ReleaseQAAppealStatusSuccess 申诉人工审核通过
	ReleaseQAAppealStatusSuccess = uint32(6)
	// ReleaseQAAppealStatusFail 申诉人工审核不通过
	ReleaseQAAppealStatusFail = uint32(7)
)

// enum for ReleaseRejectedQuestion
const (
	// ReleaseRejectedQuestionReleaseStatusInit 待发布
	ReleaseRejectedQuestionReleaseStatusInit = uint32(1)
	// ReleaseRejectedQuestionReleaseStatusIng 发布中
	ReleaseRejectedQuestionReleaseStatusIng = uint32(2)
	// ReleaseRejectedQuestionReleaseStatusEnd 已发布
	ReleaseRejectedQuestionReleaseStatusEnd = uint32(3)
	// ReleaseRejectedQuestionReleaseStatusFail 发布失败
	ReleaseRejectedQuestionReleaseStatusFail = uint32(4)
)

// enum for ReleaseConfig
const (
	// ConfigReleaseStatusInit 未发布
	ConfigReleaseStatusInit = uint32(2)
	// ConfigReleaseStatusIng 发布中
	ConfigReleaseStatusIng = uint32(3)
	// ConfigReleaseStatusSuccess 已发布
	ConfigReleaseStatusSuccess = uint32(4)
	// ConfigReleaseStatusFail 发布失败
	ConfigReleaseStatusFail = uint32(5)
)

var statusMap = map[uint32]string{
	ReleaseStatusInit:            "待发布",
	ReleaseStatusPending:         "发布中",
	ReleaseStatusSuccess:         "发布成功",
	ReleaseStatusFail:            "发布失败",
	ReleaseStatusAudit:           "发布中",
	ReleaseStatusAuditSuccess:    "发布中",
	ReleaseStatusAuditFail:       "发布失败",
	ReleaseStatusSuccessCallback: "发布中",
	ReleaseStatusPause:           "发布暂停",
}

const (
	// NoticeTypeRobotBasicInfo 机器人角色设置
	NoticeTypeRobotBasicInfo = uint32(1)
	// NoticeTypeBareAnswer 未知问题回复
	NoticeTypeBareAnswer = uint32(2)
	// NoticeTypeQAExport 问答导出
	NoticeTypeQAExport = uint32(3)
	// NoticeTypeDocToQA 文档生成QA
	NoticeTypeDocToQA = uint32(4)
	// NoticeTypeRelease 发布
	NoticeTypeRelease = uint32(5)
	// NoticeTypeRejectedQuestionExport 拒答问题导出
	NoticeTypeRejectedQuestionExport = uint32(7)
	// NoticeTypeUnsatisfiedReplyExport 不满意问题导出
	NoticeTypeUnsatisfiedReplyExport = uint32(8)
	// NoticeTypeTest 评测任务
	NoticeTypeTest = uint32(6)
	// NoticeTypeAttributeLabelExport 属性标签导出
	NoticeTypeAttributeLabelExport = uint32(9)
	// NoticeTypeDocModify 文档更新
	NoticeTypeDocModify = uint32(10)
	// NoticeTypeAttributeLabelUpdate 属性标签更新
	NoticeTypeAttributeLabelUpdate = uint32(11)
	// NoticeTypeDocCharExceeded 文档字符超量隔离
	NoticeTypeDocCharExceeded = uint32(12)
	// NoticeTypeQACharExceeded 问答字符超量隔离
	NoticeTypeQACharExceeded = uint32(13)
	// NoticeTypeDocResume 文档字符超量隔离恢复
	NoticeTypeDocResume = uint32(14)
	// NoticeTypeQAResume 问答字符超量隔离恢复
	NoticeTypeQAResume = uint32(15)
	// NoticeTypeSynonymsExport 同义词导出
	NoticeTypeSynonymsExport = uint32(16)
	// NoticeTypeSynonymsImport 同义词导入
	NoticeTypeSynonymsImport = uint32(17)
	// NoticeTypeQAAuditOrAppeal 问答审核或申诉
	NoticeTypeQAAuditOrAppeal = uint32(18)
	// NoticeTypeLabelUpdate 2.6迭代标签功能升级通知
	NoticeTypeLabelUpdate = uint32(19)
	// NoticeTypeDocRename 文档重命名通知
	NoticeTypeDocRename = uint32(20)
	// NoticeTypeDocAutoDiffTask 文档自动发起对比任务通知
	NoticeTypeDocAutoDiffTask = uint32(21)
	// NoticeTypeDocDiffTask 文档对比任务完成
	NoticeTypeDocDiffTask = uint32(22)

	// NoticeClosed 通知已关闭
	NoticeClosed = uint32(1)
	// NoticeOpen 通知已开启
	NoticeOpen = uint32(0)

	// NoticeRead 通知已读
	NoticeRead = uint32(1)
	// NoticeUnread 通知未读
	NoticeUnread = uint32(0)

	// NoticeIsGlobal 全局通知
	NoticeIsGlobal = uint32(1)
	// NoticeIsNotGlobal 非全局通知
	NoticeIsNotGlobal = uint32(0)

	// NoticeIsAllowClose 通知是否允许关闭
	NoticeIsAllowClose = uint32(1)
	// NoticeIsForbidClose 通知不允许关闭
	NoticeIsForbidClose = uint32(0)

	// LevelSuccess 成功
	LevelSuccess = "success"
	// LevelWarning 警告
	LevelWarning = "warning"
	// LevelInfo 基础信息
	LevelInfo = "info"
	// LevelError 异常错误
	LevelError = "error"

	// NoticeZeroPageID 无归属页面ID
	NoticeZeroPageID = uint32(0)
	// NoticeRobotInfoPageID 基础设置-角色设置
	NoticeRobotInfoPageID = uint32(1)
	// NoticeBareAnswerPageID 基础设置-行业通用知识库
	NoticeBareAnswerPageID = uint32(2)
	// NoticeDocPageID 知识管理-文档库
	NoticeDocPageID = uint32(3)
	// NoticeQAPageID 知识管理-问答库
	NoticeQAPageID = uint32(4)
	// NoticeWaitReleasePageID 发布上线-待发布
	NoticeWaitReleasePageID = uint32(5)
	// NoticeRejectedQuestionPageID 效果调优-拒答问题
	NoticeRejectedQuestionPageID = uint32(7)
	// NoticeUnsatisfiedReplyPageID 不满意回复
	NoticeUnsatisfiedReplyPageID = uint32(8)
	// NoticeBatchTestPageID 对话测试-批量测试-测试任务
	NoticeBatchTestPageID = uint32(6)
	// NoticeAttributeLabelPageID 属性标签
	NoticeAttributeLabelPageID = uint32(9)
	// NoticeSynonymsPageID 同义词 备注：10 是 workflow
	NoticeSynonymsPageID = uint32(11)

	// OpTypeViewDetail 查看详情
	OpTypeViewDetail = uint32(1)
	// OpTypeExportQADownload 问答导出下载
	OpTypeExportQADownload = uint32(2)
	// OpTypeRetryRelease 知识库发布重试
	OpTypeRetryRelease = uint32(3)
	// OpTypeAppeal 人工申诉
	OpTypeAppeal = uint32(4)
	// OpTypeRetry 文档审核重试
	OpTypeRetry = uint32(5)
	// OpTypeViewDocCharExceeded 查看超量失效的文档
	OpTypeViewDocCharExceeded = uint32(10)
	// OpTypeViewQACharExceeded 查看超量失效的问答
	OpTypeViewQACharExceeded = uint32(11)
	// OpTypeExpandCapacity 跳转购买扩容包
	OpTypeExpandCapacity = uint32(12)
	// OpTypeLabelUpdate 2.6迭代标签功能升级通知
	OpTypeLabelUpdate = uint32(15)
	// OpTypeDocAutoDiffTask 文档自动对比任务通知
	OpTypeDocAutoDiffTask = uint32(18)
	// OpTypeDocDiffRunning 文档对比任务进行中，不需要任务操作，仅用于前端区分类型
	OpTypeDocDiffRunning = uint32(19)
	// OpTypeDocDiffFinish 文档任务完成，可点击跳转详情
	OpTypeDocDiffFinish = uint32(20)
	// OpTypeVerifyDocQA 问答生成完成，可点击校验问答对
	OpTypeVerifyDocQA = uint32(22)
	// OpTypeDocToQaModelCapacity 生成问答模型余量不足跳转购买资源包
	OpTypeDocToQaModelCapacity = uint32(25)
	// OpTypeDocToQaModelPostPaid 生成问答模型余量不足跳转开通后付费
	OpTypeDocToQaModelPostPaid = uint32(26)
	// OpTypeDocToQaModelTopUp 生成问答模型余量不足跳转充值
	OpTypeDocToQaModelTopUp = uint32(27)
	// OpTypeDocToQaModelSwitchModel 生成问答模型余量不足跳转切换模型
	OpTypeDocToQaModelSwitchModel = uint32(28)
)

const (
	// AuditSourceRobotName 来源昵称
	AuditSourceRobotName = "knowledge.robot_name"
	// AuditSourceReleaseQA 来源问答
	AuditSourceReleaseQA = "knowledge.release_qa"
	// AuditSourceDoc 来源文档
	AuditSourceDoc = "knowledge.doc"
	// AuditSourceBareAnswer 来源未知问题
	AuditSourceBareAnswer = "knowledge.bare_answer"
	// AuditSourceRobotAvatar  机器人头像
	AuditSourceRobotAvatar = "knowledge.robot_avatar"
	// AuditSourceRobotGreeting  机器人欢迎语
	AuditSourceRobotGreeting = "knowledge.robot_greeting"
	// AuditSourceRobotRoleDescription 机器人角色描述
	AuditSourceRobotRoleDescription = "knowledge.role_description"
	// AuditSourceQAText qa的文本审核
	AuditSourceQAText = "knowledge.qa.1"
	// AuditSourceQAAnswerPic qa的图片审核
	AuditSourceQAAnswerPic = "knowledge.qa.4"
	// AuditSourceQAAnswerVideo qa的视频审核
	AuditSourceQAAnswerVideo = "knowledge.qa.3"
	// AuditSourceDocName 文档名称的审核策略
	AuditSourceDocName = "knowledge.doc_name.1"
	// AuditSourceDocSegment 文档切片审核策略
	AuditSourceDocSegment = "knowledge.doc_segment.1"
	// AuditSourceDocSegmentPic 文档切片图片审核策略
	AuditSourceDocSegmentPic = "knowledge.doc_segment.4"
	// AuditDbSourceName 数据库名称的审核策略
	AuditDbSourceName = "knowledge.db_name.1"
	// AuditDbSourceDesc 数据库描述审核
	AuditDbSourceDesc = "knowledge.db_desc.1"
	// AuditDbSourceCheckBizType 外部数据库审核类型
	AuditDbSourceCheckBizType = "QD_AI_TEXT"

	// AuditTypePlainText 文本
	AuditTypePlainText = uint32(1)
	// AuditTypeFile 文件
	AuditTypeFile = uint32(2)
	// AuditTypeVideo 视频
	AuditTypeVideo = uint32(3)
	// AuditTypePicture 图片
	AuditTypePicture = uint32(4)
	// AuditTypeUserData 用户资料
	AuditTypeUserData = uint32(5)

	// AccountTypeOther 账号类型
	AccountTypeOther = uint32(7)

	// AuditResultFail 审核不通过
	AuditResultFail = uint32(1)
	// AuditResultPass 审核通过
	AuditResultPass = uint32(2)

	// AuditBizTypeRobotName 机器人昵称审核
	AuditBizTypeRobotName = uint32(1)
	// AuditBizTypeBareAnswer 机器人未知问题回复审核
	AuditBizTypeBareAnswer = uint32(2)
	// AuditBizTypeDoc 文档审核
	AuditBizTypeDoc = uint32(3)
	// AuditBizTypeRelease 发布审核
	AuditBizTypeRelease = uint32(4)
	// AuditBizTypeRobotProfile 机器人角色配置审核
	AuditBizTypeRobotProfile = uint32(5)
	// AuditBizTypeQa 问答审核
	AuditBizTypeQa = uint32(6)
	// AuditBizTypeDocName 文档名称审核
	AuditBizTypeDocName = uint32(7)
	// AuditBizTypeDocSegment 文档切片审核
	AuditBizTypeDocSegment = uint32(8)
	// AuditBizTypeDocTableSheet 文档表格sheet审核
	AuditBizTypeDocTableSheet = uint32(9)

	// AuditStatusForbid 禁止审核
	AuditStatusForbid = uint32(1)
	// AuditStatusDoing 待送审
	AuditStatusDoing = uint32(2)
	// AuditStatusSendSuccess 送审成功
	AuditStatusSendSuccess = uint32(3)
	// AuditStatusSendFail 送审失败
	AuditStatusSendFail = uint32(4)
	// AuditStatusPass 审核通过
	AuditStatusPass = uint32(5)
	// AuditStatusFail 审核不通过
	AuditStatusFail = uint32(6)
	// AuditStatusAppealIng 人工审核（申诉）中
	AuditStatusAppealIng = uint32(7)
	// AuditStatusAppealSuccess 人工审核（申诉）通过
	AuditStatusAppealSuccess = uint32(8)
	// AuditStatusAppealFail 人工审核（申诉）不通过
	AuditStatusAppealFail = uint32(9)
	// AuditStatusTimeoutFail 审核审核超时-兜底不通过
	AuditStatusTimeoutFail = uint32(10)

	// MaxSendAuditRetryTimes 最大送审次数
	MaxSendAuditRetryTimes = uint32(5)

	// ResultTypeFail 审核不通过
	ResultTypeFail = uint32(100)
	// ResultTypeTimeout 审核超时-不通过
	ResultTypeTimeout = uint32(101)

	// MessageAuditCheckReachRetryLimit 审核check任务重试次数达到上限
	MessageAuditCheckReachRetryLimit = "审核check任务重试次数达到上限"
)

const (
	// ReleaseVectorCallback vector服务的回调请求
	ReleaseVectorCallback = uint32(0)
	// ReleaseTaskConfigCallback 任务型服务的回调请求
	ReleaseTaskConfigCallback = uint32(1)
	// ReleaseAgentCallback agent服务回调请求
	ReleaseAgentCallback = uint32(2)
	// ReleaseDBCallback know服务db回调请求
	ReleaseDBCallback = uint32(3)
	// ReleaseQACallback know服务问答回调请求
	ReleaseQACallback = uint32(4)
	// ReleaseDocCallback know服务文档回调请求
	ReleaseDocCallback = uint32(5)
	// ReleaseRejectQACallback know服务拒答回调请求
	ReleaseRejectQACallback = uint32(6)
	// ReleaseLabelCallback know服务标签回调请求
	ReleaseLabelCallback = uint32(7)
	// ReleaseKnowConfigCallback know服务配置回调请求
	ReleaseKnowConfigCallback = uint32(8)
)

var auditSourceDesc = map[string]string{
	AuditSourceRobotName:            "昵称",
	AuditSourceRobotAvatar:          "头像",
	AuditSourceRobotGreeting:        "欢迎语",
	AuditSourceRobotRoleDescription: "角色描述",
}

// QAFlagMain 主问标识
var QAFlagMain = "main"

// QAFlagSimilar 相似问标识
var QAFlagSimilar = "similar"
