package document

import (
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

// DiffTypeOperation 定义生成问答情况的枚举
type DiffTypeOperation uint32

const (
	Op1 DiffTypeOperation = iota + 1 // 新文档有生成问答，旧文档没有
	Op2                              // 旧文档有生成问答，新文档没有
	Op3                              // 新文档和旧文档都有问答
	Op4                              // 新文档和旧文档都没有问答
)

type DocOperation uint32

const (
	// DocOperationDeleteOldDoc 删除旧文档
	DocOperationDeleteOldDoc = DocOperation(1)
	// DocOperationDeleteNewDoc 删除新文档
	DocOperationDeleteNewDoc = DocOperation(2)
	// DocOperationOldReName 旧文档重命名
	DocOperationOldReName = DocOperation(3)
	// DocOperationNewReName 新文档重命名
	DocOperationNewReName = DocOperation(4)
	// DocOperationDefault 不处理
	DocOperationDefault = DocOperation(5)
)

const (
	// DocIsDeleted 已删除
	DocIsDeleted = 1
	// DocIsNotDeleted 正常
	DocIsNotDeleted = 0

	// DocStatusInit 未生成
	DocStatusInit = uint32(1)
	// DocStatusCreating 生成中
	DocStatusCreating = uint32(2)
	// DocStatusCreateSuccess 生成成功
	DocStatusCreateSuccess = uint32(3)
	// DocStatusCreateFailed 生成失败
	DocStatusCreateFailed = uint32(4)
	// DocStatusDeleting 删除中
	DocStatusDeleting = uint32(5)
	// DocStatusDeleted 删除完成
	DocStatusDeleted = uint32(6)
	// DocStatusAuditIng 审核中
	DocStatusAuditIng = uint32(7)
	// DocStatusAuditFail 审核失败
	DocStatusAuditFail = uint32(8)
	// DocStatusAuditPass 审核通过
	DocStatusAuditPass = uint32(9)
	// DocStatusWaitRelease 待发布
	DocStatusWaitRelease = uint32(10)
	// DocStatusReleasing 发布中
	DocStatusReleasing = uint32(11)
	// DocStatusReleaseSuccess 发布成功
	DocStatusReleaseSuccess = uint32(12)
	// DocStatusCreatingIndex 学习中
	DocStatusCreatingIndex = uint32(13)
	// DocStatusCreateIndexFail 学习失败
	DocStatusCreateIndexFail = uint32(14)
	// DocStatusUpdating 更新中，合并到学习中(13)，前端展示为学习中
	DocStatusUpdating = uint32(15)
	// DocStatusUpdateFail 更新失败，合并到学习失败(14)，前端展示为学习失败
	DocStatusUpdateFail = uint32(16)
	// DocStatusParseIng 解析中
	DocStatusParseIng = uint32(17)
	// DocStatusParseFail 解析失败
	DocStatusParseFail = uint32(18)
	// DocStatusParseImportFail 导入失败
	DocStatusParseImportFail = uint32(19)
	// DocStatusExpired 已过期
	DocStatusExpired = uint32(20)
	// DocStatusCharExceeded 超量失效
	DocStatusCharExceeded = uint32(21)
	// DocStatusResuming 超量失效恢复，失效恢复中
	DocStatusResuming = uint32(22)
	// DocStatusParseImportFailCharExceeded 导入失败-超量失效
	DocStatusParseImportFailCharExceeded = uint32(23)
	// DocStatusAuditFailCharExceeded 审核失败-超量失效
	DocStatusAuditFailCharExceeded = uint32(24)
	// DocStatusUpdateFailCharExceeded 更新失败-超量失效
	DocStatusUpdateFailCharExceeded = uint32(25)
	// DocStatusCreateIndexFailCharExceeded 创建索引失败-超量失效
	DocStatusCreateIndexFailCharExceeded = uint32(26)
	// DocStatusParseImportFailResuming 导入失败-超量失效恢复
	DocStatusParseImportFailResuming = uint32(27)
	// DocStatusAuditFailResuming 审核失败-超量失效恢复
	DocStatusAuditFailResuming = uint32(28)
	// DocStatusUpdateFailResuming 更新失败-超量失效恢复
	DocStatusUpdateFailResuming = uint32(29)
	// DocStatusCreateIndexFailResuming 创建索引失败-超量失效恢复
	DocStatusCreateIndexFailResuming = uint32(30)
	// DocStatusExpiredCharExceeded 已过期-超量失效
	DocStatusExpiredCharExceeded = uint32(31)
	// DocStatusExpiredResuming 已过期-超量失效恢复
	DocStatusExpiredResuming = uint32(32)
	// DocStatusUnderAppeal 人工申诉中
	DocStatusUnderAppeal = uint32(33)
	// DocStatusAppealFailed 人工申诉失败
	DocStatusAppealFailed = uint32(34)
	// DocStatusAppealFailedCharExceeded 人工申诉失败-超量失效
	DocStatusAppealFailedCharExceeded = uint32(35)
	// DocStatusAppealFailedResuming 人工申诉失败-超量失效恢复
	DocStatusAppealFailedResuming = uint32(36)

	// DocStatusDocNameAuditing 文档名称审核中
	DocStatusDocNameAuditing = uint32(37)
	// DocStatusDocNameAuditFail 重命名场景-文档名称审核失败, 对于导入场景下文档名审核失败,也会整合到此状态
	DocStatusDocNameAuditFail = uint32(38)
	// DocStatusDocNameAndContentAuditFail 文档导入场景-文档名称与内容都审核失败
	DocStatusDocNameAndContentAuditFail = uint32(39)
	// DocStatusImportDocNameAuditFail 文档导入场景-文档名称审核失败
	DocStatusImportDocNameAuditFail = uint32(40)
	// DocStatusDocNameAppealFail 重命名场景-人工申诉失败
	DocStatusDocNameAppealFail = uint32(41)

	// FileTypeDocx .
	FileTypeDocx = "docx"
	// FileTypeMD .
	FileTypeMD = "md"
	// FileTypeTxt .
	FileTypeTxt = "txt"
	// FileTypeXlsx .
	FileTypeXlsx = "xlsx"
	// FileTypePdf .
	FileTypePdf = "pdf"
	// FileTypePptx .
	FileTypePptx = "pptx"
	// FileTypePpt .
	FileTypePpt = "ppt"
	// FileTypeDoc .
	FileTypeDoc = "doc"
	// FileTypeXls .
	FileTypeXls = "xls"
	// FileTypePng .
	FileTypePng = "png"
	// FileTypeJpg .
	FileTypeJpg = "jpg"
	// FileTypeJpeg .
	FileTypeJpeg = "jpeg"
	// FileTypeCsv .
	FileTypeCsv = "csv"
	// FileTypeHtml .
	FileTypeHtml = "html"
	// FileTypeMhtml .
	FileTypeMhtml = "mhtml"
	// FileTypeWps .
	FileTypeWps = "wps"
	// FileTypePPsx
	FileTypePPsx = "ppsx"
	// FileTypeTiff
	FileTypeTiff = "tiff"
	// FileTypeBmp
	FileTypeBmp = "bmp"
	// FileTypeGif
	FileTypeGif = "gif"
	// FileTypeWebp
	FileTypeWebp = "webp"
	// FileTypeHeif
	FileTypeHeif = "heif"
	// FileTypeHeic
	FileTypeHeic = "heic"
	// FileTypeJp2
	FileTypeJp2 = "jp2"
	// FileTypeEps
	FileTypeEps = "eps"
	// FileTypeIcns
	FileTypeIcns = "icns"
	// FileTypeIm
	FileTypeIm = "im"
	// FileTypePcx
	FileTypePcx = "pcx"
	// FileTypePpm
	FileTypePpm = "ppm"
	// FileTypeXbm
	FileTypeXbm = "xbm"
	// FileTypePpsm
	FileTypePpsm = "ppsm"
	// FileTypeEpub
	FileTypeEpub = "epub"
	// FileTypeTsv
	FileTypeTsv = "tsv"
	// FileTypeJson
	FileTypeJson = "json"
	// FileTypeLog
	FileTypeLog = "log"
	// FileTypeXml
	FileTypeXml = "xml"
	// FileTypeXmind
	FileTypeXmind = "xmind"
	// FileTypeNumbers .
	FileTypeNumbers = "numbers"
	// FileTypePages .
	FileTypePages = "pages"
	// FileTypeKeyNote .
	FileTypeKeyNote = "key"

	// SourceFromFile 源文件导入
	SourceFromFile = uint32(0)
	// SourceFromWeb 网页导入
	SourceFromWeb = uint32(1)
	// SourceFromTxDoc 腾讯文档导入
	SourceFromTxDoc = uint32(2)
	// SourceFromCorpCOSDoc 客户COS文档导入
	SourceFromCorpCOSDoc = uint32(3)
	// SourceFromOnedrive 微软OneDrive导入
	SourceFromOnedrive = uint32(5)

	// AuditFlagWait 待审核
	AuditFlagWait = uint32(1)
	// AuditFlagDone 已审核
	AuditFlagDone = uint32(2)
	// AuditFlagNoRequired 免审
	AuditFlagNoRequired = uint32(3)
	// AuditFlagNoNeed 无需审核
	AuditFlagNoNeed = uint32(4)

	// DocCreatingQA 问答生成中
	DocCreatingQA = uint32(1)
	// DocCreatingIndex 索引生成中
	DocCreatingIndex = uint32(1)

	// DocNextActionAdd 新增
	DocNextActionAdd = uint32(1)
	// DocNextActionUpdate 更新
	DocNextActionUpdate = uint32(2)
	// DocNextActionDelete 删除
	DocNextActionDelete = uint32(3)
	// DocNextActionPublish 发布
	DocNextActionPublish = uint32(4)

	// AttrRangeDefault 属性标签适用范围 默认跟随文档
	AttrRangeDefault = uint32(0)
	// AttrRangeAll 属性标签适用范围 全部
	AttrRangeAll = uint32(1)
	// AttrRangeCondition 属性标签适用范围 按条件
	AttrRangeCondition = uint32(2)

	// DocUpdatingNoticeContent 文档更新中通知内容
	DocUpdatingNoticeContent = i18nkey.KeyDocumentUpdating
	// DocUpdateSuccessNoticeContent 文档更新成功通知内容
	DocUpdateSuccessNoticeContent = "%s文档更新成功"
	// DocUpdateFailNoticeContent 文档更新失败通知内容
	DocUpdateFailNoticeContent = i18nkey.KeyDocumentUpdateFailed

	// ReferURLTypePreview 引用来源链接类型-预览
	ReferURLTypePreview = 0
	// ReferURLTypeUserDefined 引用来源链接类型-用户自定义
	ReferURLTypeUserDefined = 1
	// ReferURLTypeWebDocURL 引用来源链接类型-网页导入文档源url
	ReferURLTypeWebDocURL = 2

	// MatchReferUrlTypeDefault 匹配引用来源链接类型-默认
	MatchReferUrlTypeDefault = 0
	// MatchReferUrlTypeOriginalURL 匹配引用来源链接类型-网页导入源url
	MatchReferUrlTypeOriginalURL = 1

	// DocSplitTypeQa 文档拆分问答
	DocSplitTypeQa = "qa"
	// DocSplitTypeDoc 文档拆分分段
	DocSplitTypeDoc = "doc"

	// DocUnExpiredStatus 未过期
	DocUnExpiredStatus = uint32(2)
	// DocExpiredStatus 已过期
	DocExpiredStatus = uint32(3)

	// ExcelTplTimeLayout excel批量导入，有效期时间格式
	ExcelTplTimeLayout = "2006/01/02-15:04"
	// HalfHourTime 半小时时间，单位s
	HalfHourTime = 1800
	// ExcelNoExpireTime excel批量导入，有效期永久有效
	ExcelNoExpireTime = i18nkey.KeyPermanentlyValid

	// DocOptNormal 普通文档操作类型，主要为了兼容老前端版本
	DocOptNormal = uint32(0)
	// DocOptBatchImport 文档批量导入操作类型
	DocOptBatchImport = uint32(1)
	// DocOptDocImport 文档导入操作类型
	DocOptDocImport = uint32(2)

	// BatchModifyDefault 批量修改过期时间和应用链接
	BatchModifyDefault = 0

	// BatchModifyExpiredTime 批量修改过期时间
	BatchModifyExpiredTime = 1

	// BatchModifyRefer 批量修改引用链接
	BatchModifyRefer = 2

	// BatchModifyUpdatePeriod 批量修改文档下次更新时间
	BatchModifyUpdatePeriod = 3

	// BatchModifyUpdateEnableScope
	BatchModifyUpdateEnableScope = 4

	// DocQueryTypeFileName 查询类型-文件名
	DocQueryTypeFileName = "filename"
	// DocQueryTypeAttribute 查询类型-标签
	DocQueryTypeAttribute = "attribute"
	// DocQuerySystemTypeUntagged 查询没有标签的文档
	DocQuerySystemTypeUntagged = "lke:system:untagged"

	// SysLabelDocID 文档ID默认标签
	SysLabelDocID = "DocID"

	EventProcessSuccess       = "success"
	EventProcessFailed        = "failed"
	EventUsedCharSizeExceeded = "used_char_size_exceeded" // 超量失效
	EventAppealFailed         = "appeal_failed"           // 人工申诉失败
	EventCloseAudit           = "close_audit"

	// 干预类型
	InterventionTypeOP      = uint32(0)
	InterventionTypeOrgData = uint32(1)
	InterventionTypeSheet   = uint32(2)
)

const (
	// DocDiffTaskStatusAll 全部
	DocDiffTaskStatusAll = int32(-1)
	// DocDiffTaskStatusInit 待处理
	DocDiffTaskStatusInit = int32(0)
	// DocDiffTaskStatusProcessing 处理中
	DocDiffTaskStatusProcessing = int32(1)
	// DocDiffTaskStatusSuccess 已完成
	DocDiffTaskStatusSuccess = int32(2)
	// DocDiffTaskStatusExceeded 已失效
	DocDiffTaskStatusExceeded = int32(3)

	// DocDiffTaskComparisonReasonNameDiff 名称相同
	DocDiffTaskComparisonReasonNameDiff = uint32(1)
	// DocDiffTaskComparisonReasonManualDiff 手动添加
	DocDiffTaskComparisonReasonManualDiff = uint32(2)
	// DocDiffTaskComparisonReasonUrlDiff 网址相同
	DocDiffTaskComparisonReasonUrlDiff = uint32(3)

	// DiffDataProcessStatusInit 待处理
	DiffDataProcessStatusInit = uint32(0)
	// DiffDataProcessStatusProcessing 处理中
	DiffDataProcessStatusProcessing = uint32(1)
	// DiffDataProcessStatusSuccess 处理成功
	DiffDataProcessStatusSuccess = uint32(2)
	// DiffDataProcessStatusFailed 处理失败
	DiffDataProcessStatusFailed = uint32(3)
)

const (
	// ExcelInitDocID excel批量导入初始对外值
	ExcelInitDocID = 0

	// SourceFromDoc 文档生成
	SourceFromDoc = uint32(1)
	// SourceFromBatch 批量导入
	SourceFromBatch = uint32(2)
	// SourceFromManual 手动添加
	SourceFromManual = uint32(3)

	// SimilarStatusInit 未处理
	SimilarStatusInit = 0
	// SimilarStatusIng 匹配中
	SimilarStatusIng = 1
	// SimilarStatusEnd 已匹配
	SimilarStatusEnd = 2

	// FrontEndAuditPass 审核成功，给前端的返回状态
	FrontEndAuditPass = uint32(0)
	// FrontEndSimilarQuestionAuditFailed 相似问审核失败，给前端的返回状态
	FrontEndSimilarQuestionAuditFailed = uint32(1)
	// FrontEndQaAuditFailed 问答文本审核失败，给前端的返回状态
	FrontEndQaAuditFailed = uint32(1)
	// FrontEndPicAuditFailed 答案中图片审核失败，给前端的返回状态
	FrontEndPicAuditFailed = uint32(1)
	// FrontEndVideoAuditFailed 答案中视频审核失败，给前端的返回状态
	FrontEndVideoAuditFailed = uint32(1)

	ShowCurrCate = 1 // 文档/问答列表只展示当前分类数据
)

// TableDataCellDataType 数据类型枚举
type TableDataCellDataType int32

const (
	DataTypeString   TableDataCellDataType = 0 // 字符串类型
	DataTypeInteger  TableDataCellDataType = 1 // 整数类型
	DataTypeFloat    TableDataCellDataType = 2 // 浮点数类型
	DataTypeDate     TableDataCellDataType = 3 // 日期类型
	DataTypeTime     TableDataCellDataType = 4 // 时间类型
	DataTypeDatetime TableDataCellDataType = 5 // 日期时间类型
	DataTypeBoolean  TableDataCellDataType = 6 // 布尔类型
)

var DocActionDesc = map[uint32]string{
	DocNextActionAdd:     "新增",
	DocNextActionUpdate:  "修改",
	DocNextActionDelete:  "删除",
	DocNextActionPublish: "发布",
}

var SourceDesc = map[uint32]string{
	SourceFromDoc:    i18nkey.KeyFileGeneration,
	SourceFromBatch:  i18nkey.KeyBatchImport,
	SourceFromManual: i18nkey.KeyManualEntry,
}
