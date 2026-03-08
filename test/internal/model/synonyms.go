package model

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"
)

const (
	// SynonymIsNotDeleted 未删除
	SynonymIsNotDeleted = 0
	// SynonymIsDeleted 已删除
	SynonymIsDeleted = 1

	// SynonymsReleaseStatusWaiting 待发布
	SynonymsReleaseStatusWaiting = 1
	// SynonymsReleaseStatusPublishing 发布中
	SynonymsReleaseStatusPublishing = 2
	// SynonymsReleaseStatusPublished 已发布
	SynonymsReleaseStatusPublished = 3
	// SynonymsReleaseStatusPublishFailed 发布失败
	SynonymsReleaseStatusPublishFailed = 4

	// ExportSynonymsNoticeContent 导出同义词通知内容
	ExportSynonymsNoticeContent = i18nkey.KeySynonymBatchExportStatus
	// ExportSynonymsNoticeContentIng 导出同义词通知中通知
	ExportSynonymsNoticeContentIng = i18nkey.KeySynonymBatchExporting

	// SynonymsConflictTypeStandard 当前标准词与已有标准词或者同义词冲突
	SynonymsConflictTypeStandard = 1
	// SynonymsConflictTypeSynonymsAndStandard 当前同义词与与已有标准词冲突
	SynonymsConflictTypeSynonymsAndStandard = 2
	// SynonymsConflictTypeSynonymsAndSynonyms 当前同义词与已有标准词的同义词冲突
	SynonymsConflictTypeSynonymsAndSynonyms = 3

	// SynonymsTaskStatusPending 未启动
	SynonymsTaskStatusPending = 1
	// SynonymsTaskStatusRunning 流程中
	SynonymsTaskStatusRunning = 2
	// SynonymsTaskStatusSuccess 任务成功
	SynonymsTaskStatusSuccess = 3
	// SynonymsTaskStatusFailed 任务失败
	SynonymsTaskStatusFailed = 4
)

// 同义词模板定义
var (
	SynonymsExcelTplHead = []string{
		i18nkey.KeyLevel1Category, i18nkey.KeyLevel2Category, i18nkey.KeyLevel3Category, i18nkey.KeyLevel4Category, i18nkey.KeyLevel5Category,
		i18nkey.KeyLevel6Category, i18nkey.KeyLevel7Category, i18nkey.KeyLevel8Category, i18nkey.KeyLevel9Category, i18nkey.KeyLevel10Category,
		i18nkey.KeyStandardWordRequired, i18nkey.KeySynonymRequired,
	}
	SynonymsExcelTplHeadLen       = len(SynonymsExcelTplHead)
	SynonymsExcelTplCateLen       = 10
	SynonymsExcelTplStandardIndex = SynonymsExcelTplCateLen
	SynonymsExcelTplSynonymsIndex = SynonymsExcelTplStandardIndex + 1
)

// SynonymsListReq 同义词列表请求
type SynonymsListReq struct {
	RobotID         uint64
	CorpID          uint64
	CateIDs         []uint64
	Query           string
	ReleaseStatus   []uint32
	IsDeleted       int
	UpdateTime      time.Time
	UpdateTimeEqual bool
	ID              uint64
	IDs             []uint64
	SynonymsIDs     []uint64
	Page            uint32
	PageSize        uint32
}

// SynonymsListRsp 同义词列表响应
type SynonymsListRsp struct {
	Synonyms []*SynonymsItem
}

// SynonymsItem 同义词项（包含标准词及对应的同义词）
type SynonymsItem struct {
	SynonymsID   uint64
	CateID       uint64
	StandardWord string
	Synonyms     []string
	UpdateTime   time.Time
	Status       uint32
	StatusDesc   string
	CreateTime   time.Time
}

// SynonymsCreateReq 同义词创建请求
type SynonymsCreateReq struct {
	RobotID      uint64
	CorpID       uint64
	CateID       uint64
	StandardWord string
	Synonyms     []string
}

// SynonymsCreateRsp 同义词创建响应
type SynonymsCreateRsp struct {
	ConflictType    uint32
	ConflictContent string
	SynonymsID      uint64
}

// SynonymsModifyReq 同义词修改请求
type SynonymsModifyReq struct {
	RobotID      uint64
	CorpID       uint64
	CateID       uint64
	SynonymID    uint64
	StandardWord string
	Synonyms     []string
}

// SynonymsNERReq 同义词NER请求
type SynonymsNERReq struct {
	RobotID uint64
	CorpID  uint64
	Query   string
	Scenes  uint32 // 1 评测 2 正式
}

// NerInfo NER信息
type NerInfo struct {
	Offset       int32
	NumTokens    int32
	OriginalText string
	RefValue     string
}

// SynonymsNERRsp 同义词NER响应
type SynonymsNERRsp struct {
	ReplacedQuery string
	NERInfos      []*NerInfo
}

// Synonyms 同义词
type Synonyms struct {
	ID            uint64    `db:"id"`
	SynonymsID    uint64    `db:"synonyms_id"`    // 对外ID
	RobotID       uint64    `db:"robot_id"`       // 应用ID
	CorpID        uint64    `db:"corp_id"`        // 企业ID
	CategoryID    uint64    `db:"category_id"`    // 分类ID
	ParentID      uint64    `db:"parent_id"`      // 关联的标准词ID
	Word          string    `db:"word"`           // 标准词或者同义词
	WordMD5       string    `db:"word_md5"`       // 标准词或者同义词的MD5
	ReleaseStatus uint32    `db:"release_status"` // 发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)
	NextAction    uint32    `db:"next_action"`    // 最后操作：1新增 2修改 3删除 4发布
	IsDeleted     uint64    `db:"is_deleted"`     // 是否删除
	CreateTime    time.Time `db:"create_time"`    // 创建时间
	UpdateTime    time.Time `db:"update_time"`    // 更新时间
}

// SynonymsTask 属性标签任务
type SynonymsTask struct {
	ID            uint64 `db:"id"`              // ID
	CorpID        uint64 `db:"corp_id"`         // 企业ID
	RobotID       uint64 `db:"robot_id"`        // 机器人ID
	CreateStaffID uint64 `db:"create_staff_id"` // 员工ID
	// TaskType      uint32    `db:"task_type"`       // 任务类型 (0 导入任务)
	Params      string    `db:"params"`        // 任务参数
	Status      uint32    `db:"status"`        // 任务状态(1 未启动 2 流程中 3 任务完成 4 任务失败)
	Message     string    `db:"message"`       // 状态信息
	FileName    string    `db:"file_name"`     // 文件名称
	CosURL      string    `db:"cos_url"`       // cos文件地址(客户上传)
	ErrorCosURL string    `db:"error_cos_url"` // cos文件地址(错误标注文件)
	UpdateTime  time.Time `db:"update_time"`   // 更新时间
	CreateTime  time.Time `db:"create_time"`   // 创建时间
}

// SynonymsStat 同义词统计
type SynonymsStat struct {
	Total uint64 `db:"total"`
}

// IsDelete 是否删除
func (s *Synonyms) IsDelete() bool {
	if s == nil {
		return false
	}
	return s.IsDeleted == SynonymIsDeleted
}

// IsNextActionAdd 是否新增操作
func (s *Synonyms) IsNextActionAdd() bool {
	if s == nil {
		return false
	}
	return s.NextAction == NextActionAdd
}

// StatusDesc 状态描述
func (s *Synonyms) StatusDesc() string {
	if s == nil {
		return ""
	}
	switch s.ReleaseStatus {
	case SynonymsReleaseStatusWaiting:
		return i18nkey.KeyWaitRelease
	case SynonymsReleaseStatusPublishing:
		return i18nkey.KeyReleasing
	case SynonymsReleaseStatusPublished:
		return i18nkey.KeyReleaseSuccess
	case SynonymsReleaseStatusPublishFailed:
		return i18nkey.KeyPublishingFailed
	default:
		return "未知状态"
	}
}
