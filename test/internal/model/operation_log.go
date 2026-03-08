package model

import (
	"time"
)

const (
	// DocEventAdd 文档添加
	DocEventAdd = "doc_add"
	// DocEventRename 文档重命名
	DocEventRename = "doc_rename"
	// DocEventDel 文档删除
	DocEventDel = "doc_del"
	// DocEventRefer 文档引用
	DocEventRefer = "doc_refer"
	// DocEventEdit 文档设置
	DocEventEdit = "doc_edit"
	// DocCateEventAdd 分组添加 (Doc)
	DocCateEventAdd = "doc_cate_add"
	// DocCateEventDel 分组删除 (Doc)
	DocCateEventDel = "doc_cate_del"
	// DocCateEventEdit 分组编辑 (Doc)
	DocCateEventEdit = "doc_cate_edit"
	// QaEventAdd 问答手动添加
	QaEventAdd = "qa_add"
	// QaEventDel 问答手动删除
	QaEventDel = "qa_del"
	// QaEventVerify 问答手动验证
	QaEventVerify = "qa_verify"
	// QaEventEdit 问答手动编辑
	QaEventEdit = "qa_edit"
	// QaCateEventAdd 分组添加 (QA)
	QaCateEventAdd = "cate_add"
	// QaCateEventDel 分组删除 (QA)
	QaCateEventDel = "cate_del"
	// QaCateEventEdit 分组编辑 (QA)
	QaCateEventEdit = "cate_edit"
	// SynonymsEventAdd 同义词添加
	SynonymsEventAdd = "synonyms_add"
	// SynonymsEventDel 同义词删除
	SynonymsEventDel = "synonyms_del"
	// SynonymsEventEdit 同义词编辑
	SynonymsEventEdit = "synonyms_edit"
	// SynonymsListUpload 上传同义词
	SynonymsListUpload = "synonyms_list_upload"
	// SynonymsCateEventAdd 分组添加 (Synonyms)
	SynonymsCateEventAdd = "synonyms_cate_add"
	// SynonymsCateEventDel 分组删除 (Synonyms)
	SynonymsCateEventDel = "synonyms_cate_del"
	// SynonymsCateEventEdit 分组编辑 (Synonyms)
	SynonymsCateEventEdit = "synonyms_cate_edit"
	// UnsatisfiedReplyAdd 不满意回复添加
	UnsatisfiedReplyAdd = "unsatisfied_reply_add"
	// UnsatisfiedReplyIgnore 不满意回复忽略
	UnsatisfiedReplyIgnore = "unsatisfied_reply_ignore"
	// SampleEventUpload 样本文件上传
	SampleEventUpload = "sample_upload"
	// SampleEventDelete 样本文件删除
	SampleEventDelete = "sample_delete"
	// TestEventCreate 评测任务创建
	TestEventCreate = "test_create"
	// TestEventOperate 评测任务操作
	TestEventOperate = "test_operate"
	// TestEventDelete 评测任务删除
	TestEventDelete = "test_delete"
	// TestEventStop 评测任务停止
	TestEventStop = "test_stop"
	// TestEventRetry 评测任务重试
	TestEventRetry = "test_retry"
	// TestRecordJudge 评测任务判断
	TestRecordJudge = "test_judge"
	// AttributeLabelAdd 创建属性标签
	AttributeLabelAdd = "attrubute_label_add"
	// AttributeLabelDelete 删除属性标签
	AttributeLabelDelete = "attrubute_label_delete"
	// AttributeLabelUpdate 更新属性标签
	AttributeLabelUpdate = "attrubute_label_update"
	// AttributeLabelUpload 上传属性标签
	AttributeLabelUpload = "attrubute_label_upload"
	// ActivateProduct 激活企业
	ActivateProduct = "activate_product"
	// TrialProduct 试用开通
	TrialProduct = "trial_product"
)

// Snapshot 快照-操作内容详情
type Snapshot struct {
	Req    any `json:"req"`
	Rsp    any `json:"rsp"`
	Before any `json:"before"`
	After  any `json:"after"`
}

// OperationLog 文档
type OperationLog struct {
	ID         uint64    `db:"id"`
	CorpID     uint64    `db:"corp_id"`     // 企业ID
	StaffID    uint64    `db:"staff_id"`    // 员工ID
	RobotID    uint64    `db:"robot_id"`    // 机器人ID
	Content    string    `db:"content"`     // 操作内容详情
	Event      string    `db:"event"`       // 操作类型
	ReleaseID  uint64    `db:"release_id"`  // 当前版本ID
	CreateTime time.Time `db:"create_time"` // 创建时间
	UpdateTime time.Time `db:"update_time"` // 更新时间
}
