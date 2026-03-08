package label

import (
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

const (
	AttributeTableMaxPageSize      = 1000
	AttributeLabelTableMaxPageSize = 1000
	// AttributeLabelIsNotDeleted 属性标签未删除

	// 查询范围 all(或者传空):标准词和相似词 standard:标准词 similar:相似词
	AttributeLabelQueryScopeAll      = "all"
	AttributeLabelQueryScopeStandard = "standard"
	AttributeLabelQueryScopeSimilar  = "similar"
	// DocAttributeLabelIsNotDeleted 文档属性标签未删除
	DocAttributeLabelIsNotDeleted = 0
	// DocAttributeLabelDeleted 文档属性标签已删除
	DocAttributeLabelDeleted = 1

	// QAAttributeLabelIsNotDeleted QA属性标签未删除
	QAAttributeLabelIsNotDeleted = 0
	// QAAttributeLabelDeleted QA属性标签已删除
	QAAttributeLabelDeleted = 1
	// AttributeLabelsPreview 更新预览环境属性&标签
	AttributeLabelsPreview = "preview"
	// AttributeLabelsProd 更新发布环境属性&标签
	AttributeLabelsProd = "prod"
	// AttributeLabelSourceKg 引用来源-知识标签属性标签
	AttributeLabelSourceKg = 1
	// AttributeLabelDeleted 属性标签已删除
	AttributeLabelDeleted = 1
	// AttributeLabelUpdatingNoticeContent 知识标签更新中通知内容
	AttributeLabelUpdatingNoticeContent = i18nkey.KeyKnowledgeTagUpdating
	// AttributeLabelUpdateSuccessNoticeContent 知识标签更新成功通知内容
	AttributeLabelUpdateSuccessNoticeContent = "%s知识标签更新成功"
	// AttributeLabelUpdateFailNoticeContent 知识标签更新失败通知内容
	AttributeLabelUpdateFailNoticeContent = i18nkey.KeyKnowledgeTagUpdateFailed
)

const (
	// AttributeNextActionAdd 新增
	AttributeNextActionAdd = uint32(1)
	// AttributeNextActionUpdate 更新
	AttributeNextActionUpdate = uint32(2)
	// AttributeNextActionDelete 删除
	AttributeNextActionDelete = uint32(3)
	// AttributeNextActionPublish 发布
	AttributeNextActionPublish = uint32(4)
	// AttributeStatusUnknown 未知
	AttributeStatusUnknown = 0
	// AttributeStatusWaitRelease 等待发布
	AttributeStatusWaitRelease = 1
	// AttributeStatusReleasing 发布中
	AttributeStatusReleasing = 2
	// AttributeStatusReleased 已发布
	AttributeStatusReleased = 3
	// AttributeStatusReleaseFail 发布失败
	AttributeStatusReleaseFail = 4
	// AttributeStatusReleaseUpdating 发布更新中
	AttributeStatusReleaseUpdating = 5
	// AttributeLabelTaskStatusPending 未启动
	AttributeLabelTaskStatusPending = 1
	// AttributeLabelTaskStatusRunning 流程中
	AttributeLabelTaskStatusRunning = 2
	// AttributeLabelTaskStatusSuccess 任务成功
	AttributeLabelTaskStatusSuccess = 3
	// AttributeLabelTaskStatusFailed 任务失败
	AttributeLabelTaskStatusFailed = 4

	// AttributeLabelTypeDOC 文档类型
	AttributeLabelTypeDOC LabelType = 1
	// AttributeLabelTypeQA QA类型
	AttributeLabelTypeQA LabelType = 2
)

// ======================attribute_label=========================
const (
	AttributeLabelTblColId            = "id"
	AttributeLabelTblColRobotId       = "robot_id"
	AttributeLabelTblColBusinessId    = "business_id"
	AttributeLabelTblColAttrId        = "attr_id"
	AttributeLabelTblColName          = "name"
	AttributeLabelTblColSimilarLabel  = "similar_label"
	AttributeLabelTblColReleaseStatus = "release_status"
	AttributeLabelTblColNextAction    = "next_action"
	AttributeLabelTblColIsDeleted     = "is_deleted"
	AttributeLabelTblColCreateTime    = "create_time"
	AttributeLabelTblColUpdateTime    = "update_time"
)

const (
	DocAttributeLabelTblColId         = "id"
	DocAttributeLabelTblColRobotId    = "robot_id"
	DocAttributeLabelTblColDocId      = "doc_id"
	DocAttributeLabelTblColSource     = "source"
	DocAttributeLabelTblColAttrId     = "attr_id"
	DocAttributeLabelTblColLabelId    = "label_id"
	DocAttributeLabelTblColIsDeleted  = "is_deleted"
	DocAttributeLabelTblColCreateTime = "create_time"
	DocAttributeLabelTblColUpdateTime = "update_time"
)

// ===================attribute===========================
const (
	attributeTableName = "t_attribute"

	AttributeTblColId            = "id"
	AttributeTblColBusinessId    = "business_id"
	AttributeTblColRobotId       = "robot_id"
	AttributeTblColAttrKey       = "attr_key"
	AttributeTblColName          = "name"
	AttributeTblColIsUpdating    = "is_updating"
	AttributeTblColReleaseStatus = "release_status"
	AttributeTblColNextAction    = "next_action"
	AttributeTblColIsDeleted     = "is_deleted"
	AttributeTblColDeletedTime   = "deleted_time"
	AttributeTblColCreateTime    = "create_time"
	AttributeTblColUpdateTime    = "update_time"
)

var releaseStatusDesc = map[uint32]string{
	AttributeStatusWaitRelease:     i18nkey.KeyWaitRelease,
	AttributeStatusReleasing:       i18nkey.KeyReleasing,
	AttributeStatusReleased:        i18nkey.KeyReleaseSuccess,
	AttributeStatusReleaseFail:     i18nkey.KeyPublishingFailed,
	AttributeStatusReleaseUpdating: i18nkey.KeyUpdating,
}

const (
	// AttributeLabelNoticeContent = "知识标签批量导出成功。"
	AttributeLabelNoticeContent = i18nkey.KeyKnowledgeTagsBatchExportStatus
	// AttributeLabelNoticeContentIng = "知识标签批量导出中。"
	AttributeLabelNoticeContentIng = i18nkey.KeyKnowledgeTagsBatchExporting
)

const (
	// ExcelTplAttrNameIndex 导入文档索引，属性名称列（必填）
	ExcelTplAttrNameIndex = 0
	// ExcelTplLabelIndex 导入文档索引，标签列（必填）
	ExcelTplLabelIndex = 1
	// ExcelTplSimilarLabelIndex 导入文档索引，相似标签列（可选）
	ExcelTplSimilarLabelIndex = 2
)
