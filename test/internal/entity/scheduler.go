package entity

import (
	"time"

	"git.woa.com/dialogue-platform/bot-config/task_scheduler"

	"git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
)

// 常量
const (
	// TaskExport 导出任务
	TaskExport task_scheduler.TaskType = 2

	// DocDeleteTask 文档删除任务
	DocDeleteTask task_scheduler.TaskType = 4

	// QADeleteTask 问答删除任务
	QADeleteTask task_scheduler.TaskType = 5

	// DocToIndexTask 文档分片索引任务
	DocToIndexTask task_scheduler.TaskType = 6

	// DocToQATask 文档分片生成问答任务
	DocToQATask task_scheduler.TaskType = 7

	// SendAuditTask 送审任务
	SendAuditTask task_scheduler.TaskType = 8

	// CheckAuditTask check审核结果任务
	CheckAuditTask task_scheduler.TaskType = 9

	// ReleaseCollectTask 发布采集任务
	ReleaseCollectTask task_scheduler.TaskType = 10

	// ReleaseSuccessTask 发布采集任务
	ReleaseSuccessTask task_scheduler.TaskType = 11

	// ExcelToQATask Excel分片生成问答任务
	ExcelToQATask task_scheduler.TaskType = 12

	// DocModifyTask 文档编辑任务
	DocModifyTask task_scheduler.TaskType = 13

	// AttributeLabelUpdateTask 属性标签更新任务
	AttributeLabelUpdateTask task_scheduler.TaskType = 14

	// EmbeddingUpgradeTask embedding 升级
	// 没有 new task，手动触发
	// EmbeddingUpgradeTask task_scheduler.TaskType = 15

	// ResourceExpireTask 计费资源包过期处理任务
	ResourceExpireTask task_scheduler.TaskType = 16

	// DocResumeTask 超量失效文档恢复任务
	DocResumeTask task_scheduler.TaskType = 17

	// QAResumeTask 超量失效问答恢复任务
	QAResumeTask task_scheduler.TaskType = 18

	// SynonymsDeleteTask 同义词删除任务
	// 没有 new task，手动触发
	SynonymsDeleteTask task_scheduler.TaskType = 19

	// SynonymsImportTask 同义词导入任务
	SynonymsImportTask task_scheduler.TaskType = 20

	// EvaluateTestDeleteTask 删除应用评测数据任务
	// EvaluateTestDeleteTask task_scheduler.TaskType = 21

	// KnowledgeDeleteTask 知识删除任务
	KnowledgeDeleteTask task_scheduler.TaskType = 22

	// DocRenameToIndexTask 文档重命名后分片重新创建索引任务
	DocRenameToIndexTask task_scheduler.TaskType = 23

	// DocDiffDataTask 文档比对任务
	DocDiffDataTask task_scheduler.TaskType = 24

	// DocDiffOperationTask 文档diff操作任务
	DocDiffOperationTask task_scheduler.TaskType = 25

	// SyncAttributeTask 标签同步到es任务
	// 没有 new task，手动触发
	SyncAttributeTask task_scheduler.TaskType = 26

	// BatchUpdateVectorTask 更新文档/问答/数据表标签任务
	BatchUpdateVectorTask task_scheduler.TaskType = 27

	// KnowledgeGenerateSchemaTask 知识库生成schema任务
	KnowledgeGenerateSchemaTask task_scheduler.TaskType = 28

	// DocSegInterveneTask 文档切片干预任务
	DocSegInterveneTask task_scheduler.TaskType = 29

	// ReleaseDBTask db 发布任务
	ReleaseDBTask task_scheduler.TaskType = 30

	// FullUpdateLabelTask 全量刷标签任务
	// 没有 new task，手动触发
	FullUpdateLabelTask task_scheduler.TaskType = 31

	// SyncOrgDataTask org_data 同步任务
	// 没有 new task，手动触发
	SyncOrgDataTask task_scheduler.TaskType = 32

	// EnableDBSourceTask 外部数据库异步任务
	EnableDBSourceTask task_scheduler.TaskType = 33

	// AddDbTableTask 外部数据库添加数据表异步任务
	AddDbTableTask task_scheduler.TaskType = 34

	// SyncDbSourceVdbIndexTask 同步数据库源到VDB索引任务（2.9.5 临时用，已经和 harryjhuang 确认可以删除）
	// SyncDbSourceVdbIndexTask task_scheduler.TaskType = 35

	// TxDocRefreshTask 腾讯文档刷新任务
	TxDocRefreshTask task_scheduler.TaskType = 36

	// DocSplitRuleModifyTask 文档七分规则修改任务
	DocSplitRuleModifyTask task_scheduler.TaskType = 37

	// UpdateEmbeddingModelTask 切换embedding模型异步任务
	UpdateEmbeddingModelTask = 38

	// ReleaseVectorTask 发布向量任务
	ReleaseVectorTask task_scheduler.TaskType = 39

	// ReleaseDocTask 发布文档任务
	ReleaseDocTask task_scheduler.TaskType = 40

	// ReleaseDocQATask 发布文档问答任务
	ReleaseDocQATask task_scheduler.TaskType = 41

	// ReleaseLabelTask 发布标签任务
	ReleaseLabelTask task_scheduler.TaskType = 42

	// ReleaseRejectedQuestionTask 发布被驳回的问答任务
	ReleaseRejectedQuestionTask task_scheduler.TaskType = 43

	// ReleaseKnowledgeConfigTask 发布知识库配置任务
	ReleaseKnowledgeConfigTask task_scheduler.TaskType = 44

	// CorpCOSDocRefreshTask 客户COS文档刷新任务
	CorpCOSDocRefreshTask task_scheduler.TaskType = 45

	// FullUpdateDatabaseLabelTask 全量刷数据库标签任务【人工插入任务】
	FullUpdateDatabaseLabelTask task_scheduler.TaskType = 46

	// MigrateThirdDocTask 同步OneDrive文档任务
	MigrateThirdDocTask task_scheduler.TaskType = 47 // 同步第三方文档任务
	RefreshThirdDocTask task_scheduler.TaskType = 48 // 刷新第三方文档任务

	// ExportKbPackageTask 知识库数据包导出任务
	ExportKbPackageTask task_scheduler.TaskType = 49
	// ImportKbPackageTask 知识库数据包导入任务
	ImportKbPackageTask task_scheduler.TaskType = 50
)

const (
	// TaskMutexNone 无互斥
	TaskMutexNone task_scheduler.TaskMutex = 0
	// TaskMutexAuditCheck 送审
	TaskMutexAuditCheck task_scheduler.TaskMutex = 1
	// TaskMutexEmbeddingUpgrade embedding 升级
	TaskMutexEmbeddingUpgrade task_scheduler.TaskMutex = 2
	// TaskMutexResourceExpire 字段到期超量隔离
	TaskMutexResourceExpire task_scheduler.TaskMutex = 3
	// TaskMutexKnowledgeDelete 知识删除任务互斥锁
	TaskMutexKnowledgeDelete task_scheduler.TaskMutex = 4
	// TestTaskMutex 评测任务一个用户当前只能有一个任务在执行
	TestTaskMutex task_scheduler.TaskMutex = 5
	// TaskMutexKnowledgeGenerateSchema 知识库生成schema任务互斥锁
	TaskMutexKnowledgeGenerateSchema task_scheduler.TaskMutex = 6
	// TaskMutexAddDbTable 添加数据表任务互斥锁
	TaskMutexAddDbTable task_scheduler.TaskMutex = 7
	// TaskMutexUpdateEmbeddingModel 切换embedding模型任务互斥锁
	TaskMutexUpdateEmbeddingModel = 8
	// TaskMutexKbPackageExport 知识库数据包导出任务互斥锁
	TaskMutexKbPackageExport task_scheduler.TaskMutex = 9
	// TaskMutexKbPackageImport 知识库数据包导入任务互斥锁
	TaskMutexKbPackageImport task_scheduler.TaskMutex = 10

	UpdateVectorByRole BatchUpdateVectorType = 1 // 根据appBizID 角色变更触发
	UpdateVectorByCate BatchUpdateVectorType = 2 // 根据appBizID 分类变更触发

	// DataSourceCOS 数据源类型-COS
	DataSourceCOS = 0
	// DataSourceDB 数据源类型-数据库
	DataSourceDB = 1
)

var (
	// TaskTypeNameMap 任务类型名称映射, 用于在配置文件中配置 任务名 时寻找对应的 任务类型
	TaskTypeNameMap = map[task_scheduler.TaskType]string{
		TaskExport:                  "export",
		DocDeleteTask:               "doc_delete",
		QADeleteTask:                "qa_delete",
		DocToIndexTask:              "doc_to_index",
		DocToQATask:                 "doc_to_qa",
		SendAuditTask:               "send_audit",
		CheckAuditTask:              "check_audit",
		ReleaseCollectTask:          "release_collect",
		ReleaseSuccessTask:          "release_success",
		ReleaseVectorTask:           "release_vector",
		ReleaseDocTask:              "release_doc",
		ReleaseDocQATask:            "release_doc_qa",
		ReleaseLabelTask:            "release_label",
		ReleaseRejectedQuestionTask: "release_rejected_question",
		ReleaseKnowledgeConfigTask:  "release_knowledge_config",
		ExcelToQATask:               "excel_to_qa",
		DocModifyTask:               "doc_modify",
		AttributeLabelUpdateTask:    "attribute_label_update",
		ResourceExpireTask:          "resource_expire",
		DocResumeTask:               "doc_resume",
		QAResumeTask:                "qa_resume",
		SynonymsDeleteTask:          "synonyms_delete",
		SynonymsImportTask:          "synonyms_import",
		KnowledgeDeleteTask:         "knowledge_delete",
		DocRenameToIndexTask:        "doc_rename_to_index",
		DocDiffDataTask:             "doc_diff_data",
		DocDiffOperationTask:        "doc_diff_operation_task",
		SyncAttributeTask:           "sync_attribute_task",
		BatchUpdateVectorTask:       "batch_update_vector_task",
		KnowledgeGenerateSchemaTask: "knowledge_generate_schema_task",
		DocSegInterveneTask:         "doc_seg_intervene_task",
		ReleaseDBTask:               "release_db_task",
		FullUpdateLabelTask:         "full_update_label_task",
		SyncOrgDataTask:             "sync_org_data_task",
		EnableDBSourceTask:          "enable_db_source_task",
		AddDbTableTask:              "add_db_table_task",
		TxDocRefreshTask:            "tx_doc_refresh_task",
		DocSplitRuleModifyTask:      "doc_split_rule_modify_task",
		UpdateEmbeddingModelTask:    "update_embedding_model_task",
		CorpCOSDocRefreshTask:       "corp_cos_doc_refresh_task",
		FullUpdateDatabaseLabelTask: "full_update_database_label_task",
		MigrateThirdDocTask:         "migrate_third_doc_task",
		RefreshThirdDocTask:         "refresh_third_doc_task",
		ExportKbPackageTask:         "export_kb_package",
		ImportKbPackageTask:         "import_kb_package",
	}
)

// TaskInvokeAccessor 任务调用访问器
type TaskInvokeAccessor interface {
}

// DocDeleteParams 文档删除任务参数
type DocDeleteParams struct {
	Name     string `json:"name"`     // 任务名称
	CorpID   uint64 `json:"corp_id"`  // 企业ID
	StaffID  uint64 `json:"staff_id"` // 员工ID
	RobotID  uint64 `json:"robot_id"` // 机器人ID
	DocID    uint64 `json:"doc_id"`   // 文档ID
	Language string `json:"language"` // 国际化语言
}

// DocModifyParams 文档编辑任务参数
type DocModifyParams struct {
	Name        string    `json:"name"`         // 任务名称
	CorpID      uint64    `json:"corp_id"`      // 企业ID
	StaffID     uint64    `json:"staff_id"`     // 员工ID
	RobotID     uint64    `json:"robot_id"`     // 机器人ID
	DocID       uint64    `json:"doc_id"`       // 文档ID
	EnableScope uint32    `json:"enable_scope"` // 启用范围
	ExpireStart time.Time `json:"expire_start"` // 有效期的开始时间
	ExpireEnd   time.Time `json:"expire_end"`   // 有效期的结束时间
	NotChangeQA bool      `json:"is_change_qa"` // 不需要更新文档生成的问答
	Language    string    `json:"language"`     // 国际化语言
}

// QADeleteParams 问答删除任务
type QADeleteParams struct {
	Name     string   `json:"name"`     // 任务名称
	CorpID   uint64   `json:"corp_id"`  // 企业ID
	StaffID  uint64   `json:"staff_id"` // 员工ID
	RobotID  uint64   `json:"robot_id"` // 机器人ID
	QAIDs    []uint64 `json:"qa_ids"`   // 问答ID列表
	Language string   `json:"language"` // 国际化语言
}

// DocToIndexParams 文档分片索引任务参数
type DocToIndexParams struct {
	Name                    string    `json:"name"`                        // 任务名称
	CorpID                  uint64    `json:"corp_id"`                     // 企业ID
	StaffID                 uint64    `json:"staff_id"`                    // 员工ID
	RobotID                 uint64    `json:"robot_id"`                    // 机器人ID
	DocID                   uint64    `json:"doc_id"`                      // 文档ID
	InterveneOriginDocBizID uint64    `json:"intervene_origin_doc_biz_id"` // 干预原始文档ID
	ExpireStart             time.Time `json:"expire_start"`                // 有效期的开始时间
	ExpireEnd               time.Time `json:"expire_end"`                  // 有效期的结束时间
	Language                string    `json:"language"`                    // 国际化语言
	IsFromBatchImport       bool      `json:"is_from_batch_import"`        // 是否来自批量导入（批量导入时切片已存在，无需创建）
}

// DocRenameToIndexParams 文档重命名之后重建向量索引
type DocRenameToIndexParams struct {
	Name        string    `json:"name"`         // 任务名称
	CorpID      uint64    `json:"corp_id"`      // 企业ID
	StaffID     uint64    `json:"staff_id"`     // 员工ID
	RobotID     uint64    `json:"robot_id"`     // 机器人ID
	DocID       uint64    `json:"doc_id"`       // 文档ID
	ExpireStart time.Time `json:"expire_start"` // 有效期的开始时间
	ExpireEnd   time.Time `json:"expire_end"`   // 有效期的结束时间
	Language    string    `json:"language"`     // 国际化语言
}

// DocToQAParams 文档分片生成问答任务参数
type DocToQAParams struct {
	Name       string `json:"name"`         // 任务名称
	CorpID     uint64 `json:"corp_id"`      // 企业ID
	CorpBizID  uint64 `json:"corp_biz_id"`  // 企业biz
	StaffID    uint64 `json:"staff_id"`     // 员工ID
	RobotID    uint64 `json:"robot_id"`     // 机器人ID
	DocID      uint64 `json:"doc_id"`       // 文档ID
	QaTaskID   uint64 `json:"qa_task_id"`   // 问答任务表ID
	QaTaskType int    `json:"qa_task_type"` // 问答任务状态
	Uin        string `json:"uin"`          // 云主账号 Uin
	Sid        uint64 `json:"sid"`          // 集成商 ID
	Language   string `json:"language"`     // 国际化语言
}

// ExcelToQAParams Excel分片生成问答任务参数
type ExcelToQAParams struct {
	Name     string `json:"name"`     // 任务名称
	CorpID   uint64 `json:"corp_id"`  // 企业ID
	StaffID  uint64 `json:"staff_id"` // 员工ID
	RobotID  uint64 `json:"robot_id"` // 机器人ID
	DocID    uint64 `json:"doc_id"`   // 文档ID
	EnvSet   string `json:"env_set"`  // 环境参数
	Language string `json:"language"` // 国际化语言
}

// AuditSendParams 送审参数
type AuditSendParams struct {
	Name    string `json:"name"`     // 任务名称
	CorpID  uint64 `json:"corp_id"`  // 企业ID
	StaffID uint64 `json:"staff_id"` // 员工ID
	RobotID uint64 `json:"robot_id"` // 机器人ID
	// 1机器人昵称审核、2机器人未知问题审核、3文档审核、4发布问答审核、5机器人角色配置审核、6问答审核、7文件名审核
	Type             uint32 `json:"type"`
	RelateID         uint64 `json:"audit_id"`            // 送审ID
	EnvSet           string `json:"env_set"`             // 环境参数
	ParentAuditBizID uint64 `json:"parent_audit_biz_id"` // 父审核ID
	ParentRelateID   uint64 `json:"parent_relate_id"`    // 父审核关联ID
	OriginDocBizID   uint64 `json:"origin_doc_biz_id"`   // (切分干预使用)干预原始文档ID
	Language         string `json:"language"`            // 国际化语言
}

func (p *AuditSendParams) GetAuditNotice() (bool, uint32, uint32, string) {
	var (
		isNeedNotice bool
		noticeType   uint32
		pageID       uint32
		subject      string
	)
	switch p.Type {
	case releaseEntity.AuditBizTypeRobotProfile:
		isNeedNotice = true
		noticeType = releaseEntity.NoticeTypeRobotBasicInfo
		pageID = releaseEntity.NoticeRobotInfoPageID
		subject = "角色设置内容审核中。"
	case releaseEntity.AuditBizTypeBareAnswer:
		isNeedNotice = true
		noticeType = releaseEntity.NoticeTypeBareAnswer
		pageID = releaseEntity.NoticeBareAnswerPageID
		subject = "未知问题回复语审核中。"
	}
	return isNeedNotice, noticeType, pageID, subject
}

// AuditCheckParams 审核回调check
type AuditCheckParams struct {
	Name           string `json:"name"`     // 任务名称
	AuditID        uint64 `json:"audit_id"` // 待check的审核ID
	CorpID         uint64 `json:"corp_id"`  // 企业ID
	StaffID        uint64 `json:"staff_id"` // 员工ID
	RobotID        uint64 `json:"robot_id"` // 机器人ID
	Type           uint32 `json:"type"`
	ParentRelateID uint64 `json:"parent_relate_id"` // 父审核关联ID
	OriginDocID    uint64 `json:"origin_doc_id"`    // (切分干预使用)干预原始文档ID
	Language       string `json:"language"`         // 国际化语言
}

// ReleaseCollectParams 发布采集任务参数
type ReleaseCollectParams struct {
	Name      string `json:"name"`       // 任务名称
	CorpID    uint64 `json:"corp_id"`    // 企业ID
	StaffID   uint64 `json:"staff_id"`   // 员工ID
	RobotID   uint64 `json:"robot_id"`   // 机器人ID
	VersionID uint64 `json:"version_id"` // 版本ID
	EnvSet    string `json:"env_set"`    // 环境变量
	Language  string `json:"language"`   // 国际化语言
}

// ReleaseSuccessParams 发布成功任务参数
type ReleaseSuccessParams struct {
	Name      string `json:"name"`       // 任务名称
	CorpID    uint64 `json:"corp_id"`    // 企业ID
	StaffID   uint64 `json:"staff_id"`   // 员工ID
	RobotID   uint64 `json:"robot_id"`   // 机器人ID
	VersionID uint64 `json:"version_id"` // 版本ID
	Language  string `json:"language"`   // 国际化语言
}

type TaskReleaseVectorParams struct {
	ReleaseParams
	CorpID             uint64             `json:"corp_id"`  // 企业ID
	StaffID            uint64             `json:"staff_id"` // 员工ID
	AppID              uint64             `json:"robot_id"` // 机器人ID
	EmbeddingVersionID EmbeddingVersionID `json:"embedding_version_id"`
	EmbeddingModelName string             `json:"embedding_model_name"`
	VersionID          AppVersionID       `json:"version_id"`
	VersionName        string             `json:"version_name"`
	AppBizID           uint64             `json:"bot_biz_id"`
	LastQAVersion      uint64             `json:"last_qa_version"` // 上一个版本号
	IsCreateGroup      bool               `json:"is_create_group"` // 第一次发布的应用需要先创建向量库
}

// ExportParams 通用导出任务参数
type ExportParams struct {
	TaskID           uint64 `json:"task_id"`
	CorpID           uint64 `json:"corp_id"`
	RobotID          uint64 `json:"robot_id"`
	AppBizID         uint64 `json:"app_biz_id"`
	FileName         string `json:"file_name"`
	CreateStaffID    uint64 `json:"user_id"`
	TaskType         uint32 `json:"task_type"`
	TaskName         string `json:"name"`
	Params           string `json:"params"`
	NoticeContent    string `json:"notice_content"`
	NoticePageID     uint32 `json:"notice_page_id"`
	NoticeTypeExport uint32 `json:"notice_type_export"`
	NoticeContentIng string `json:"notice_content_ing"`
	Language         string `json:"language"` // 国际化语言
}

// TestParams 评测任务参数
type TestParams struct {
	CorpID  uint64 `json:"corp_id"`  // 企业 ID
	RobotID uint64 `json:"robot_id"` // 机器人 ID
	TestID  uint64 `json:"test_id"`  // 测试任务 ID
}

// SynonymsImportParams 同义词导入更新参数
type SynonymsImportParams struct {
	Name     string `json:"name"`     // 任务名称
	CorpID   uint64 `json:"corp_id"`  // 企业ID
	StaffID  uint64 `json:"staff_id"` // 员工ID
	RobotID  uint64 `json:"robot_id"` // 机器人ID
	TaskID   uint64 `json:"task_id"`  // 任务ID
	Language string `json:"language"` // 国际化语言
}

// SyncOrgDataParams org_data 同步
type SyncOrgDataParams struct {
	// Name 任务名称
	Name string `json:"name"`
	// AppIDs 要同步的应用, 为空同步所有
	AppIDs []uint64 `json:"app_ids"`
	// ChunkSize 同步时取的记录的分块大小
	ChunkSize uint64 `json:"chunk_size"`
	// RetryTimes 重试次数
	RetryTimes int `json:"retry_times"`
	// RetryInterval 重试间隔
	RetryInterval int `json:"retry_interval"`
	// Batch 运行的并发数
	Batch int `json:"batch"`
	// DelayMs 每个应用执行完休息间隔
	DelayMs int `json:"delay_ms"`
}

// ResExpireParams 资源包过期处理参数
type ResExpireParams struct {
	Name           string  `json:"name"`             // 任务名称
	CorpID         uint64  `json:"corp_id"`          // 企业ID
	Uin            string  `json:"uin"`              // 腾讯云主账号ID
	ResourceID     string  `json:"resource_id"`      // 资源ID
	Capacity       float64 `json:"capacity"`         // 本次到期资源包容量
	ExpireTime     uint64  `json:"expire_time"`      // 本次到期资源包到期时间
	IsDebug        bool    `json:"is_debug"`         // 调试开关, 调试场景下,resource_id作为appid, Capacity作为剩余容量
	Language       string  `json:"language"`         // 国际化语言
	IsPackageScene bool    `json:"is_package_scene"` // 是否是套餐包计费场景
}

// DocExceededTime 文档恢复参数, 需要把更新时间保存下,防止恢复失败的情况下,删除时间被重置
type DocExceededTime struct {
	BizID      uint64
	UpdateTime time.Time
}

// DocResumeParams 文档恢复任务参数
type DocResumeParams struct {
	Name             string            `json:"name"`               // 任务名称
	CorpID           uint64            `json:"corp_id"`            // 企业ID
	StaffID          uint64            `json:"staff_id"`           // 员工ID
	RobotID          uint64            `json:"robot_id"`           // 机器人ID
	VersionID        uint64            `json:"version_id"`         // 版本ID
	DocExceededTimes []DocExceededTime `json:"doc_exceeded_times"` // 本次需要恢复的文档超时时间列表
	Language         string            `json:"language"`           // 国际化语言
}

func (p DocResumeParams) DocBizIDs() []uint64 {
	bizIDs := make([]uint64, 0, len(p.DocExceededTimes))
	for _, d := range p.DocExceededTimes {
		bizIDs = append(bizIDs, d.BizID)
	}
	return bizIDs
}

// QAExceededTime 问答恢复参数, 需要把更新时间保存下,防止恢复失败的情况下,删除时间被重置
type QAExceededTime struct {
	BizID      uint64
	UpdateTime time.Time
}

// QAResumeParams 问答恢复任务参数
type QAResumeParams struct {
	Name            string           `json:"name"`              // 任务名称
	CorpID          uint64           `json:"corp_id"`           // 企业ID
	StaffID         uint64           `json:"staff_id"`          // 员工ID
	RobotID         uint64           `json:"robot_id"`          // 机器人ID
	VersionID       uint64           `json:"version_id"`        // 版本ID
	QAExceededTimes []QAExceededTime `json:"qa_exceeded_times"` // 本次需要恢复的文档超时时间列表
	Language        string           `json:"language"`          // 国际化语言
}

func (p QAResumeParams) QABizIDs() []uint64 {
	bizIDs := make([]uint64, 0, len(p.QAExceededTimes))
	for _, d := range p.QAExceededTimes {
		bizIDs = append(bizIDs, d.BizID)
	}
	return bizIDs
}

// SearchReferences 检索后用于拼接prompt
type SearchReferences struct {
	// 文档ID
	DocID uint64 `json:"doc_id,omitempty"`
	// 1是QA 2是segment
	DocType uint32 `json:"doc_type,omitempty"`
	// QAID/SegmentID
	ID uint64 `json:"id,omitempty"`
	// 问题
	Question string `json:"question,omitempty"`
	// qa答案
	Answer string `json:"answer,omitempty"`
	// 原始文档
	OrgData string `json:"org_data,omitempty"`
	// 置信度
	Confidence float32 `json:"confidence,omitempty"`
}

// TestQuestionInfo questionID
type TestQuestionInfo struct {
	TestID     uint64
	QuestionID string
	RecordID   string
	Question   string
	Answer     string
	References []*SearchReferences
	IsFinal    bool
}

// SynonymsDeleteParams 同义词删除任务
type SynonymsDeleteParams struct {
	Name        string   `json:"name"`         // 任务名称
	CorpID      uint64   `json:"corp_id"`      // 企业ID
	StaffID     uint64   `json:"staff_id"`     // 员工ID
	RobotID     uint64   `json:"robot_id"`     // 机器人ID
	SynonymsIDs []uint64 `json:"synonyms_ids"` // 同义词ID列表
}

// KnowledgeDeleteParams 知识删除任务参数
type KnowledgeDeleteParams struct {
	Name     string `json:"name"`       // 任务名称
	RobotID  uint64 `json:"robot_id"`   // 机器人ID
	AppBizID uint64 `json:"app_biz_id"` // 机器人业务ID
	CorpID   uint64 `json:"corp_id"`    // 企业ID
	TaskID   uint64 `json:"task_id"`    // 本次删除操作的任务ID
}

// DocDiffParams 文档比较任务参数
type DocDiffParams struct {
	Name       string `json:"name"`         // 任务名称
	CorpBizID  uint64 `json:"corp_biz_id"`  // 企业ID
	RobotBizID uint64 `json:"robot_biz_id"` // 机器人ID
	DiffBizID  uint64 `json:"diff_biz_id"`  // diff任务id
	Language   string `json:"language"`     // 国际化语言
}

// DocSegInterveneParams 文档切片干预
type DocSegInterveneParams struct {
	Name           string `json:"name"`        // 任务名称
	TaskID         uint64 `json:"task_id"`     // 任务ID
	CorpID         uint64 `json:"corp_id"`     // 企业ID
	CorpBizID      uint64 `json:"corp_biz_id"` // 企业ID
	StaffID        uint64 `json:"staff_id"`
	StaffBizID     uint64 `json:"staff_biz_id"`
	AppBizID       uint64 `json:"app_biz_id"`        // 机器人ID
	AppID          uint64 `json:"app_id"`            // 机器人ID
	OriginDocBizID uint64 `json:"origin_doc_biz_id"` // 旧文档ID
	FileType       string `json:"file_type"`         // 文件类型
	FileName       string `json:"file_name"`
	SourceEnvSet   string `json:"source_env_set"` // 环境，审核必须使用
	DataSource     uint32 `json:"data_source"`    // 数据来源（仅excel使用，0-cos，1-切分干预表）
	Language       string `json:"language"`       // 国际化语言
}

// DocDiffOperationParams 文档diff的参数
type DocDiffOperationParams struct {
	Name         string                 `json:"name"`
	Uin          string                 `json:"uin"` // 云主账号 Uin
	Sid          uint64                 `json:"sid"` // 集成商 ID
	StaffID      uint64                 `json:"staff_id"`
	CorpID       uint64                 `json:"corp_id"`
	CorpBizID    uint64                 `json:"corp_biz_id"`
	RobotID      uint64                 `json:"robot_id"`
	RobotBizID   uint64                 `json:"robot_biz_id"`
	OldDocBizID  uint64                 `json:"old_doc_biz_id"`
	NewDocBizID  uint64                 `json:"new_doc_biz_id"`
	DocQATaskID  uint64                 `json:"doc_qa_task_id"`
	QaTaskType   int                    `json:"qa_task_type"` // 问答任务状态  是否是continue的任务状态
	DocOperation docEntity.DocOperation `json:"doc_operation"`
	QAOperation  docEntity.QAOperation  `json:"qa_operation"`
	DocDiffID    uint64                 `json:"doc_diff_id"`
	NewName      string                 `json:"new_name"` // 重命名之后的名称
	EnvSet       string                 `json:"env_set"`  // 环境参数
	Language     string                 `json:"language"` // 国际化语言
}

// SyncAttributeParams 标签同步到es的参数
type SyncAttributeParams struct {
	Name              string   `json:"name"`
	RobotIDs          []uint64 `json:"robot_ids"`
	AddLabelChunkSize int      `json:"add_label_chunk_size"`
	DelayMs           int      `json:"delay_ms"`
}

type BatchUpdateVectorType uint32

type BatchUpdateVector struct {
	Name      string                `json:"name"`        // 任务名称
	Type      BatchUpdateVectorType `json:"type"`        // 1是按照from-to刷数据 2是按appBizId执行任务
	CorpBizID uint64                `json:"corp_biz_id"` // 创建任务的主号业务id
	AppBizID  uint64                `json:"app_biz_id"`  // 创建任务的应用业务id
	KnowIDs   map[uint64]KnowData   `json:"know_ids"`    // 以知识库业务id为维度要更新的文档/问答/数据表业务ids
	Language  string                `json:"language"`    // 国际化语言
}

type FullUpdateLabel struct {
	Name   string   `json:"name"`    // 任务名称
	AppIDs []uint64 `json:"app_ids"` // 应用主键ids
	MaxID  uint64   `json:"max_id"`  // 最大应用主键id
}

// FullUpdateDBLabel 更新数据库标签
type FullUpdateDBLabel struct {
	Name           string   `json:"name"`              // 任务名称
	DBSourceBizIDs []uint64 `json:"db_source_biz_ids"` // 数据库业务ids
}

type KnowData struct {
	DocIDs        []uint64 `json:"doc_ids"`
	QaIDs         []uint64 `json:"qa_ids"`
	DbTableBizIDs []uint64 `json:"db_table_biz_ids"`
}

type ReleaseParams struct {
	Name        string `json:"name"`
	ReleaseType uint32
}

// ReleaseDBParams
type ReleaseDBParams struct {
	ReleaseParams
	CorpBizID    uint64
	RobotID      uint64
	AppBizID     uint64
	ReleaseBizID uint64
	Language     string `json:"language"` // 国际化语言
}

type ReleaseKnowledgeConfigParams struct {
	ReleaseParams
	CorpBizID uint64
	RobotID   uint64
	AppBizID  uint64
	ReleaseID uint64
	Language  string `json:"language"` // 国际化语言
}

type ReleaseDocParams struct {
	TaskReleaseVectorParams // segment 需要
	CorpBizID               uint64
	AppBizID                uint64
	RobotID                 uint64
	ReleaseID               uint64
	Language                string `json:"language"` // 国际化语言
}

type ReleaseLabelParams struct {
	ReleaseParams
	CorpBizID uint64
	AppBizID  uint64
	RobotID   uint64
	ReleaseID uint64
	Language  string `json:"language"` // 国际化语言
}

type ReleaseRejectedQuestionParams struct {
	TaskReleaseVectorParams
	CorpBizID uint64
	AppBizID  uint64
	RobotID   uint64
	ReleaseID uint64
	Language  string `json:"language"` // 国际化语言
}

type ReleaseQAParams struct {
	TaskReleaseVectorParams
	CorpBizID uint64
	AppBizID  uint64
	RobotID   uint64
	ReleaseID uint64
	Language  string `json:"language"` // 国际化语言
}

type EnableDBSourceParams struct {
	Name          string `json:"name"`
	RobotID       uint64
	CorpBizID     uint64
	AppBizID      uint64
	DbTableBizID  uint64
	DbSourceBizID uint64
	Enable        bool
	StaffID       uint64
	DBTableBizIDs []uint64
	Language      string `json:"language"` // 国际化语言
}

type LearnDBTableParams struct {
	Name             string `json:"name"`
	RobotID          uint64
	CorpBizID        uint64
	AppBizID         uint64
	DBSource         *database.Database
	DBTableBizID     uint64
	EmbeddingVersion uint64
	EmbeddingName    string
	Language         string `json:"language"` // 国际化语言
}

// SyncDbSourceVdbIndexParams 标签同步到es的参数
type SyncDbSourceVdbIndexParams struct {
	Name              string   `json:"name"`
	RobotIDs          []uint64 `json:"robot_ids"`
	AddLabelChunkSize int      `json:"add_label_chunk_size"`
	DelayMs           int      `json:"delay_ms"`
}

// TxDocRefreshParams 腾讯文档刷新参数
type TxDocRefreshParams struct {
	Name      string                           `json:"name"` // 任务名称
	EnvSet    string                           `json:"env_set"`
	TFileInfo map[uint64]TxDocRefreshTFileInfo `json:"t_file_info"` // 腾讯文档信息
	Language  string                           `json:"language"`    // 国际化语言
}

// TxDocRefreshTFileInfo 腾讯文档信息
type TxDocRefreshTFileInfo struct {
	DocID       uint64 `json:"doc_ID"`       // 文档ID
	CorpID      uint64 `json:"corp_id"`      // 企业ID
	StaffID     uint64 `json:"staff_id"`     // 员工ID
	RobotID     uint64 `json:"robot_id"`     // 机器人ID
	FileID      string `json:"file_id"`      // 腾讯文档ID
	OperationID string `json:"operation_id"` // 操作刷新ID
}

// DocSplitRuleModifyParams 切分规则修改
type DocSplitRuleModifyParams struct {
	Name         string `json:"name"`              // 任务名称
	AppID        uint64 `json:"app_id"`            // 机器人ID
	CorpBizID    uint64 `json:"corp_biz_id"`       // 企业ID
	AppBizID     uint64 `json:"app_biz_id"`        // 机器人ID
	DocBizID     uint64 `json:"origin_doc_biz_id"` // 文档ID
	SourceEnvSet string `json:"source_env_set"`    // 环境，审核必须使用
	Language     string `json:"language"`          // 国际化语言
}

// CorpCOSDocRefreshParams 用户COS文档刷新参数
type CorpCOSDocRefreshParams struct {
	Name     string           `json:"name"` // 任务名称
	EnvSet   string           `json:"env_set"`
	Docs     []*docEntity.Doc `json:"docs"`     // 文档信息
	Language string           `json:"language"` // 国际化语言
}

type MigrateThirdPartyDocParams struct {
	Name         string   `json:"name"`
	AppBizID     uint64   `json:"app_biz_id"`
	StaffID      uint64   `json:"staff_id"`    // 员工ID
	CorpID       uint64   `json:"corp_id"`     // 企业IDq
	CorpBizID    uint64   `json:"corp_biz_id"` // 企业业务ID
	SourceFrom   uint32   `json:"source_from"`
	Uin          string   `json:"uin"`
	SUin         string   `json:"s_uin"`
	OperationIDs []uint64 `json:"operation_ids"`
	Language     string   `json:"language"` // 国际化语言
}

type DocRefreshFileInfo struct {
	DocID       uint64 `json:"doc_ID"`       // 文档ID
	CorpID      uint64 `json:"corp_id"`      // 企业ID
	StaffID     uint64 `json:"staff_id"`     // 员工ID
	RobotID     uint64 `json:"robot_id"`     // 机器人ID
	FileID      string `json:"file_id"`      // 腾讯文档ID
	OperationID string `json:"operation_id"` // 操作刷新ID
}
type DocRefreshParams struct {
	Name       string                         `json:"name"`        // 任务名称
	SourceFrom uint32                         `json:"source_from"` // 任务来源
	EnvSet     string                         `json:"env_set"`
	FileInfo   map[uint64]*DocRefreshFileInfo `json:"file_info"` // 腾讯文档信息
	Language   string                         `json:"language"`  // 国际化语言
}

// ExportKbPackageParams 知识库数据包导出任务参数
type ExportKbPackageParams struct {
	Name           string   `json:"name"`             // 任务名称
	CorpPrimaryID  uint64   `json:"corp_primary_id"`  // 企业自增ID
	CorpBizID      uint64   `json:"corp_biz_id"`      // 企业业务ID
	StaffPrimaryID uint64   `json:"staff_primary_id"` // t_corp_staff表，员工自增ID
	AppPrimaryID   uint64   `json:"app_primary_id"`   // 应用自增ID
	AppBizID       uint64   `json:"app_biz_id"`       // 应用业务ID
	KbIDs          []uint64 `json:"kb_ids"`           // 需要导出的知识库业务ID列表
	ExportCosPath  string   `json:"export_cos_path"`  // 导出路径
	TaskID         uint64   `json:"task_id"`          // 导出任务ID
	SubTaskID      uint64   `json:"sub_task_id"`      // 导出子任务ID
	Language       string   `json:"language"`         // 国际化语言
	Scene          string   `json:"scene"`            // 导出场景,AppPackage:应用包场景，KBDataPackage:知识库数据包场景
}

// ImportKbPackageParams 知识库数据包导入任务参数
type ImportKbPackageParams struct {
	Name                string `json:"name"`                   // 任务名称
	SpaceID             string `json:"space_id"`               // 空间ID
	Uin                 string `json:"uin"`                    // 用户uin
	CorpPrimaryID       uint64 `json:"corp_primary_id"`        // 企业自增ID
	CorpBizID           uint64 `json:"corp_biz_id"`            // 企业业务ID
	StaffPrimaryID      uint64 `json:"staff_primary_id"`       // t_corp_staff表，员工自增ID
	StaffBizID          uint64 `json:"staff_biz_id"`           // t_corp_staff表，员工业务ID
	AppPrimaryID        uint64 `json:"app_primary_id"`         // 应用自增ID
	AppBizID            uint64 `json:"app_biz_id"`             // 应用业务ID
	ImportAppPackageURL string `json:"import_app_package_url"` // 导入应用包地址（后续会支持单独导入知识库包）
	IdMappingCosUrl     string `json:"id_mapping_cos_url"`     // ID映射文件地址
	TaskID              uint64 `json:"task_id"`                // 导入任务ID
	SubTaskID           uint64 `json:"sub_task_id"`            // 导入子任务ID
	Language            string `json:"language"`               // 国际化语言
	Scene               string `json:"scene"`                  // 导入场景，AppPackage:应用包场景，KBDataPackage:知识库数据包场景
}
