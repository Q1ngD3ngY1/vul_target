package document

import (
	"context"
	"strconv"
	"time"

	"github.com/looplab/fsm"
	"golang.org/x/exp/slices"

	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/logx"
	attributeEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	DocTableName = "t_doc"

	DocTableMaxPageSize = 1000

	DocTblColId                  = "id"
	DocTblColBusinessId          = "business_id"
	DocTblColRobotId             = "robot_id"
	DocTblColCorpId              = "corp_id"
	DocTblColStaffId             = "staff_id"
	DocTblColCreateUserId        = "create_user_id"
	DocTblColFileName            = "file_name"
	DocTblColFileNameInAudit     = "file_name_in_audit"
	DocTblColFileType            = "file_type"
	DocTblColFileSize            = "file_size"
	DocTblColBucket              = "bucket"
	DocTblColCosURL              = "cos_url"
	DocTblColCosHash             = "cos_hash"
	DocTblColMessage             = "message"
	DocTblColStatus              = "status"
	DocTblColIsRefer             = "is_refer"
	DocTblColReferURLType        = "refer_url_type"
	DocTblColIsDeleted           = "is_deleted"
	DocTblColSource              = "source"
	DocTblColWebURL              = "web_url"
	DocTblColBatchId             = "batch_id"
	DocTblColAuditFlag           = "audit_flag"
	DocTblColIsCreatingQa        = "is_creating_qa"
	DocTblColIsCreatedQa         = "is_created_qa"
	DocTblColNextAction          = "next_action"
	DocTblColAttrRange           = "attr_range"
	DocTblColIsCreatingIndex     = "is_creating_index"
	DocTblColCharSize            = "char_size"
	DocTblColCreateTime          = "create_time"
	DocTblColUpdateTime          = "update_time"
	DocTblColExpireStart         = "expire_start"
	DocTblColExpireEnd           = "expire_end"
	DocTblColOpt                 = "opt"
	DocTblColCategoryId          = "category_id"
	DocTblColOriginalURL         = "original_url"
	DocTblColProcessingFlag      = "processing_flag"
	DocTblColCustomerKnowledgeId = "customer_knowledge_id"
	DocTblColAttributeFlag       = "attribute_flag"
	DocTblColIsDownloadable      = "is_downloadable"
	DocTblColUpdatePeriodH       = "update_period_h"
	DocTblColNextUpdateTime      = "next_update_time"
	DocTblColSplitRule           = "split_rule"
	DocTblColEnableScope         = "enable_scope"

	// docFilterFlagIsCreatedQa 文档筛选是否生成过问答标记位
	DocFilterFlagIsCreatedQa = "is_created_qa"
)

// docFilterFlags 支持的文档筛选标识位
var docFilterFlags = map[string]struct{}{
	// 后续支持新类型添加此处
	DocFilterFlagIsCreatedQa: {},
}
var DocTblColList = []string{DocTblColId, DocTblColBusinessId, DocTblColRobotId, DocTblColCorpId,
	DocTblColStaffId, DocTblColCreateUserId, DocTblColFileName, DocTblColFileNameInAudit, DocTblColFileType,
	DocTblColFileSize, DocTblColBucket, DocTblColCosURL, DocTblColCosHash, DocTblColMessage,
	DocTblColStatus, DocTblColIsRefer, DocTblColReferURLType, DocTblColIsDeleted, DocTblColSource,
	DocTblColWebURL, DocTblColBatchId, DocTblColAuditFlag, DocTblColIsCreatingQa, DocTblColIsCreatedQa,
	DocTblColNextAction, DocTblColAttrRange, DocTblColIsCreatingIndex, DocTblColCharSize, DocTblColCreateTime,
	DocTblColUpdateTime, DocTblColExpireStart, DocTblColExpireEnd, DocTblColOpt, DocTblColCategoryId,
	DocTblColOriginalURL, DocTblColProcessingFlag, DocTblColCustomerKnowledgeId, DocTblColAttributeFlag,
	DocTblColIsDownloadable, DocTblColUpdatePeriodH, DocTblColNextUpdateTime, DocTblColSplitRule, DocTblColEnableScope}

// 问答对模板定义
var (
	ExcelTplHeadLen              = len(ExcelTplHead)
	ExcelTplCateLen              = 10
	ExcelTpOptionalLen           = 11
	ExcelTplQaEnableScopeIndex   = ExcelTplCustomParamIndex + 1
	ExcelTplCustomParamIndex     = ExcelTplExpireTimeIndex + 1
	ExcelTplExpireTimeIndex      = ExcelTplSimilarQuestionIndex + 1
	ExcelTplSimilarQuestionIndex = ExcelTplQuestionDescIndex + 1
	ExcelTplQuestionDescIndex    = ExcelTplAnswerIndex + 1
	ExcelTplAnswerIndex          = ExcelTplCateLen + 1
	ExcelTplQuestionIndex        = ExcelTplCateLen
	ExcelTplHead                 = []string{
		i18nkey.KeyLevel1Category, i18nkey.KeyLevel2Category, i18nkey.KeyLevel3Category, i18nkey.KeyLevel4Category, i18nkey.KeyLevel5Category,
		i18nkey.KeyLevel6Category, i18nkey.KeyLevel7Category, i18nkey.KeyLevel8Category, i18nkey.KeyLevel9Category, i18nkey.KeyLevel10Category,
		i18nkey.KeyQuestionRequired, i18nkey.KeyAnswerRequired, i18nkey.KeyQuestionDescriptionOptional, i18nkey.KeySimilarQuestionOptional, i18nkey.KeyValidityOptional, i18nkey.KeyCustomParameterOptional,
		i18nkey.KeyEnableScope, "来源", "关联文档", "字符数", "更新时间", "创建时间", "修改人",
	}
	ExportExcelQaTplHead = []string{
		"1级分类", "2级分类", "3级分类", "4级分类", "5级分类",
		"6级分类", "7级分类", "8级分类", "9级分类", "10级分类",
		"问题（必填）", "答案（必填）", "问题描述（非必填）", "相似问（非必填）", "有效期（非必填）", "自定义参数（非必填）",
		"生效范围（非必填）", "来源", "关联文档", "字符数", "更新时间", "创建时间", "修改人",
	}
	ExcelTplQaStatusDisable     = i18nkey.KeyDisabled
	ExcelTplQaStatusEnable      = i18nkey.KeyEnabled
	ExcelTplEnableScopeValidKey = []string{
		i18nkey.KeyEnableScopeAll,
		i18nkey.KeyEnableScopeDev,
		i18nkey.KeyEnableScopePublish,
		i18nkey.KeyEnableScopeDisable,
	}
)

// DocStableStatus 文档稳定状态，即不会再自动流转到其他状态
var DocStableStatus = []uint32{
	DocStatusCreateFailed,
	DocStatusDeleted,
	DocStatusAuditFail,
	DocStatusAuditPass,
	DocStatusWaitRelease,
	DocStatusReleaseSuccess,
	DocStatusCreateIndexFail,
	DocStatusUpdateFail,
	DocStatusParseFail,
	DocStatusParseImportFail,
	DocStatusExpired,
	DocStatusCharExceeded,
	DocStatusParseImportFailCharExceeded,
	DocStatusAuditFailCharExceeded,
	DocStatusUpdateFailCharExceeded,
	DocStatusCreateIndexFailCharExceeded,
	DocStatusExpiredCharExceeded,
	DocStatusAppealFailed,
	DocStatusAppealFailedCharExceeded,
	DocStatusDocNameAuditFail,
	DocStatusDocNameAndContentAuditFail,
	DocStatusImportDocNameAuditFail,
	DocStatusDocNameAppealFail,
}

var statusDesc = map[uint32]string{
	DocStatusInit:            i18nkey.KeyInit,
	DocStatusCreating:        i18nkey.KeyCreating,
	DocStatusCreateSuccess:   i18nkey.KeyCreateSuccess,
	DocStatusCreateFailed:    i18nkey.KeyCreateFailed,
	DocStatusAuditIng:        i18nkey.KeyAuditIng,
	DocStatusAuditFail:       i18nkey.KeyAuditFail,
	DocStatusAuditPass:       i18nkey.KeyAuditPass,
	DocStatusWaitRelease:     i18nkey.KeyWaitRelease,
	DocStatusReleasing:       i18nkey.KeyReleasing,
	DocStatusReleaseSuccess:  i18nkey.KeyReleaseSuccess,
	DocStatusCreatingIndex:   i18nkey.KeyCreatingIndex,
	DocStatusCreateIndexFail: i18nkey.KeyCreateIndexFail,
	// DocStatusUpdating:        i18nkey.KeyUpdating,
	// DocStatusUpdateFail:      i18nkey.KeyUpdateFailed,
	DocStatusParseIng:          i18nkey.KeyParseIng,
	DocStatusParseFail:         i18nkey.KeyParseFail,
	DocStatusParseImportFail:   i18nkey.KeyParseImportFail,
	DocStatusExpired:           i18nkey.KeyExpired,
	DocStatusCharExceeded:      i18nkey.KeyCharExceeded,
	DocStatusResuming:          i18nkey.KeyResuming,
	DocStatusUnderAppeal:       i18nkey.KeyUnderAppeal,
	DocStatusAppealFailed:      i18nkey.KeyAppealFailed,
	DocStatusDocNameAppealFail: i18nkey.KeyAppealFailed,

	DocStatusDocNameAuditing:            i18nkey.KeyAuditIng,
	DocStatusDocNameAuditFail:           i18nkey.KeyAuditFail,
	DocStatusDocNameAndContentAuditFail: i18nkey.KeyAuditFail,
	DocStatusImportDocNameAuditFail:     i18nkey.KeyAuditFail,
}

var docSourceDesc = map[uint32]string{
	SourceFromFile: i18nkey.KeySourceFileImport,
	SourceFromWeb:  i18nkey.KeyWebPageImport,
}

var docNextActionMap = map[uint32]string{
	DocNextActionAdd:     i18nkey.KeyAdd,
	DocNextActionUpdate:  i18nkey.KeyModify,
	DocNextActionDelete:  i18nkey.KeyDeleted,
	DocNextActionPublish: i18nkey.KeyPublish,
}

// FileTypeAuditFlag 文件类型审核标识
var FileTypeAuditFlag = map[string]uint32{
	FileTypeDocx:    AuditFlagWait,
	FileTypeMD:      AuditFlagWait,
	FileTypeTxt:     AuditFlagWait,
	FileTypeXlsx:    AuditFlagNoRequired,
	FileTypePdf:     AuditFlagWait,
	FileTypePptx:    AuditFlagWait,
	FileTypePpt:     AuditFlagWait,
	FileTypeDoc:     AuditFlagWait,
	FileTypeXls:     AuditFlagWait,
	FileTypePng:     AuditFlagWait,
	FileTypeJpeg:    AuditFlagWait,
	FileTypeJpg:     AuditFlagWait,
	FileTypeCsv:     AuditFlagWait,
	FileTypeHtml:    AuditFlagWait,
	FileTypeMhtml:   AuditFlagWait,
	FileTypeWps:     AuditFlagWait,
	FileTypePPsx:    AuditFlagWait,
	FileTypeTiff:    AuditFlagWait,
	FileTypeBmp:     AuditFlagWait,
	FileTypeGif:     AuditFlagWait,
	FileTypeJson:    AuditFlagWait,
	FileTypeLog:     AuditFlagWait,
	FileTypeXml:     AuditFlagWait,
	FileTypeXmind:   AuditFlagWait,
	FileTypeNumbers: AuditFlagNoRequired,
	FileTypePages:   AuditFlagWait,
	FileTypeKeyNote: AuditFlagWait,
}

// DocResumingStatusList 文档恢复中的状态
var DocResumingStatusList = []uint32{
	DocStatusResuming,
	DocStatusParseImportFailResuming,
	DocStatusAuditFailResuming,
	DocStatusUpdateFailResuming,
	DocStatusCreateIndexFailResuming,
	DocStatusExpiredResuming,
	DocStatusAppealFailedResuming,
}

const (
	DocProcessingFlagCreatingQA          = 0x01
	DocProcessingFlagCreatingIndex       = 0x02
	DocProcessingFlagHandlingDocDiffTask = 0x04
	DocProcessingFlagSegmentIntervene    = 0x08
)

// IsProcessingMap 数据库中processing_flag的映射
var IsProcessingMap = map[uint64]pb.DocProcessing{
	DocProcessingFlagHandlingDocDiffTask: pb.DocProcessing_HandleDocDiff,
}

const (
	DocAttributeFlagPublic  = 0x01 // 第一位，文档公开（1公开，0不公开）
	DocAttributeFlagDisable = 0x02 // 知识库概念统一后该字段已废弃，第二位，文档停用（0未停用，1停用）
)

// AttributeFlagMap 数据库中attribute_flag的映射
var AttributeFlagMap = map[uint64]pb.DocAttributeFlag{
	DocAttributeFlagPublic:  pb.DocAttributeFlag_Public,
	DocAttributeFlagDisable: pb.DocAttributeFlag_Disable,
}

var (
	DocExceedStatus = []uint32{
		DocStatusCharExceeded,
		DocStatusResuming,
		DocStatusParseImportFailCharExceeded,
		DocStatusAuditFailCharExceeded,
		DocStatusUpdateFailCharExceeded,
		DocStatusCreateIndexFailCharExceeded,
		DocStatusParseImportFailResuming,
		DocStatusAuditFailResuming,
		DocStatusUpdateFailResuming,
		DocStatusCreateIndexFailResuming,
		DocStatusExpiredCharExceeded,
		DocStatusExpiredResuming,
		DocStatusAppealFailedCharExceeded,
		DocStatusAppealFailedResuming,
	}
	DocExceedResumingStatus = []uint32{
		DocStatusResuming,
		DocStatusParseImportFailResuming,
		DocStatusAuditFailResuming,
		DocStatusUpdateFailResuming,
		DocStatusCreateIndexFailResuming,
		DocStatusExpiredResuming,
		DocStatusAppealFailedResuming,
	}
)

// Doc 文档
type Doc struct {
	ID                  uint64    `db:"id"          gorm:"primaryKey;column:id"`                   // 主键ID
	BusinessID          uint64    `db:"business_id" gorm:"column:business_id"`                     // 业务ID
	RobotID             uint64    `db:"robot_id"    gorm:"column:robot_id"`                        // 机器人ID
	CorpID              uint64    `db:"corp_id"     gorm:"column:corp_id"`                         // 企业ID
	StaffID             uint64    `db:"staff_id"    gorm:"column:staff_id"`                        // 员工ID
	FileName            string    `db:"file_name"   gorm:"column:file_name"`                       // 审核的文件名
	FileNameInAudit     string    `db:"file_name_in_audit"   gorm:"column:file_name_in_audit"`     // 文件名
	FileType            string    `db:"file_type"   gorm:"column:file_type"`                       // 文件类型(markdown,word,txt)
	FileSize            uint64    `db:"file_size"   gorm:"column:file_size"`                       // 文件大小
	Bucket              string    `db:"bucket"      gorm:"column:bucket"`                          // 存储桶
	CosURL              string    `db:"cos_url"     gorm:"column:cos_url"`                         // cos文件地址
	CosHash             string    `db:"cos_hash"    gorm:"column:cos_hash"`                        // x-cos-hash-crc64ecma 用于校验文件一致性
	Message             string    `db:"message"     gorm:"column:message"`                         // 失败原因
	Status              uint32    `db:"status"      gorm:"column:status"`                          // 状态(1 未生成 2 生成中 3生成失败 4 生成成功)
	IsDeleted           bool      `db:"is_deleted"  gorm:"column:is_deleted"`                      // 是否删除(0未删除 1已删除）
	IsRefer             bool      `db:"is_refer"    gorm:"column:is_refer"`                        // 答案是否引用(0不引用 1引用）默认0
	Source              uint32    `db:"source"      gorm:"column:source"`                          // 文档来源( 0  源文件导入  1 网页导入) 默认 0 源文件导入
	WebURL              string    `db:"web_url"     gorm:"column:web_url"`                         // 网页导入url
	BatchID             int       `db:"batch_id"    gorm:"column:batch_id"`                        // 文档版本，用于控制后续生成的chunk和分片
	AuditFlag           uint32    `db:"audit_flag"  gorm:"column:audit_flag"`                      // 1待审核2已审核3无需审核
	CharSize            uint64    `db:"char_size"   gorm:"column:char_size"`                       // 文档字符数
	IsCreatingQA        bool      `db:"is_creating_qa" gorm:"column:is_creating_qa"`               // 是否正在创建QA
	IsCreatedQA         bool      `db:"is_created_qa"  gorm:"column:is_created_qa"`                // 是否正在创建QA
	IsCreatingIndex     bool      `db:"is_creating_index" gorm:"column:is_creating_index"`         // 是否正在创建索引
	NextAction          uint32    `db:"next_action"    gorm:"column:next_action"`                  // 面向发布操作：1新增 2修改 3删除 4发布
	AttrRange           uint32    `db:"attr_range"     gorm:"column:attr_range"`                   // 属性标签适用范围 1 全部 2 按条件设置
	ReferURLType        uint32    `db:"refer_url_type" gorm:"column:refer_url_type"`               // 外部引用链接类型 1 使用本地存储链接（预览） 2 使用本地存储链接（下载） 3 使用自定义链接
	CreateTime          time.Time `db:"create_time"    gorm:"column:create_time"`                  // 创建时间
	UpdateTime          time.Time `db:"update_time"    gorm:"column:update_time"`                  // 更新时间
	ExpireStart         time.Time `db:"expire_start"   gorm:"column:expire_start"`                 // 有效期的开始时间
	ExpireEnd           time.Time `db:"expire_end"     gorm:"column:expire_end"`                   // 有效期的结束时间
	Opt                 uint32    `db:"opt"            gorm:"column:opt"`                          // 文档操作类型
	CategoryID          uint32    `db:"category_id"    gorm:"column:category_id"`                  // 分类ID
	OriginalURL         string    `db:"original_url"    gorm:"column:original_url"`                // 原始网页地址
	ProcessingFlag      uint64    `db:"processing_flag" gorm:"column:processing_flag"`             // 处理中标志位
	CustomerKnowledgeId string    `db:"customer_knowledge_id" gorm:"column:customer_knowledge_id"` // 外部客户的知识ID
	AttributeFlag       uint64    `db:"attribute_flag" gorm:"column:attribute_flag"`               // 文档属性标记，位运算 1：公开 2：文档是否停用
	IsDownloadable      bool      `db:"is_downloadable" gorm:"column:is_downloadable"`             // 0:不可下载,1:可下载
	UpdatePeriodH       uint32    `db:"update_period_h" gorm:"column:update_period_h"`             // 文档更新周期小时数：0不更新，24(1天)，72(3天)，168(7天)
	NextUpdateTime      time.Time `db:"next_update_time" gorm:"column:next_update_time"`           // 文档下次更新执行时间
	SplitRule           string    `db:"split_rule" gorm:"column:split_rule"`                       // 文档下次更新执行时间
	EnableScope         uint32    `db:"enable_scope" gorm:"column:enable_scope"`                   // 启用范围 1 不生效 2 仅开发域 3 仅发布域 4 开发域和发布域

	FSM *fsm.FSM `gorm:"-"` // 有限状态机
}

// DocListReq 拉取doc列表请求结构
type DocListReq struct {
	CorpID         uint64
	RobotID        uint64
	FileName       string
	QueryType      string
	FileTypes      []string
	Page           uint32
	PageSize       uint32
	Status         []uint32
	ValidityStatus uint32 // 当前问答对的有效期状态，1-未生效；2-未过期；3-已过期
	Opts           []uint32
	CateIDs        []uint64
	FilterFlag     map[string]bool
	EnableScope    *uint32
}

// DocParsingInterventionRedisValue 文档解析干预redis值
type DocParsingInterventionRedisValue struct {
	OldDoc           *Doc                         `json:"old_doc,omitempty"`           // 原始文档
	AttrLabels       []*attributeEntity.AttrLabel `json:"attr_labels,omitempty"`       // 属性标签
	InterventionType uint32                       `json:"intervention_type,omitempty"` // 干预类型，区分数据来源op(默认)、orgData、sheet
	OriginDocBizID   uint64                       `json:"origin_doc_biz_id"`           // 干预原始文本ID
}

type NewDocParsingInterventionRedisValue struct { // 新文档解析干预redis值
	DocID    uint64 `json:"doc_id,omitempty"`
	FileName string `json:"file_name,omitempty"` // 文件名
	CosURL   string `json:"cos_url,omitempty"`   // cos文件地址
	FileType string `json:"file_type,omitempty"` // 文件类型
	FileSize uint64 `json:"file_size,omitempty"` // 文件大小
	ETag     string `json:"etag,omitempty"`      // ETag
}

var RefreshSourceList = []uint32{SourceFromOnedrive, SourceFromTxDoc}

// DocFilter 文档筛选
type DocFilter struct {
	RouterAppBizID                  uint64 // 用来路由数据库实例的应用业务ID，比如isearch独立数据库实例
	ID                              uint64
	IDs                             []uint64
	NotInIDs                        []uint64
	OrIDs                           []uint64
	CorpId                          uint64 // 企业 ID
	RobotId                         uint64
	RobotIDs                        []uint64
	BusinessIds                     []uint64
	IsDeleted                       *bool
	NotInBusinessIds                []uint64
	Status                          []uint32
	ValidityStatus                  uint32
	FileNameSubStr                  string
	FileNameOrAuditName             string // 查询文件名或者重命名名称
	FileNameSubStrOrAuditNameSubStr string // 查询文件名或者重命名名称的子串
	FileTypes                       []string
	Opts                            []uint32
	CategoryIds                     []uint32
	OriginalURL                     string
	FilterFlag                      map[string]bool // 文档筛选标识位

	CosHash         string
	IsCreatingIndex *uint32
	MinUpdateTime   time.Time
	MaxUpdateTime   time.Time
	NextActions     []uint32
	NextUpdateTime  time.Time
	Source          []uint32
	EnableScope     *uint32

	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
}

// SplitRule 文档切分规则(excel)
type SplitRule struct {
	SplitConfigNew struct {
		XlsxSplitter struct {
			HeaderInterval []int `json:"header_interval"`
			ContentStart   int   `json:"content_start"`
			SplitRow       int   `json:"split_row"`
		} `json:"xlsx_splitter"`
	} `json:"split_config_new"`
}

type DocParseThirdParseConfigParam struct { // 文档解析第三方模型参数
	FormulaEnhancement *bool  `json:"formula_enhancement,omitempty"`
	LLMEnhancement     *bool  `json:"llm_enhancement,omitempty"`
	EnhancementMode    string `json:"enhancement_mode"`
	OutputHtmlTable    *bool  `json:"output_html_table,omitempty"`
}

// DocSourceDesc 文档来源描述
func (d *Doc) DocSourceDesc() string {
	if d == nil {
		return ""
	}
	return docSourceDesc[d.Source]
}

// HasDeleted 文档是否已删除
func (d *Doc) HasDeleted() bool {
	if d == nil {
		return false
	}
	return d.IsDeleted
}

// IsAllowCreateQA 文档是否允许创建QA
func (d *Doc) IsAllowCreateQA() bool {
	if d == nil {
		return false
	}
	if (d.Status == DocStatusAuditPass ||
		d.Status == DocStatusWaitRelease ||
		d.Status == DocStatusReleasing ||
		d.Status == DocStatusReleaseSuccess ||
		d.Status == DocStatusCreateFailed ||
		d.Status == DocStatusCreateSuccess ||
		d.Status == DocStatusUpdating ||
		d.Status == DocStatusUpdateFail) && !d.IsCreatingQaV1() {
		return true
	}
	return false
}

// IsAllowDelete 文档 正在审核中|人工申诉中|更新中|学习中||生成QA||生成索引 不允许删除
func (d *Doc) IsAllowDelete() bool {
	if d == nil {
		return false
	}
	if d.IsCreatingQaV1() {
		// 高优先级判断，正在生成QA，不允许删除
		return false
	}
	if d.Status == DocStatusWaitRelease || d.Status == DocStatusReleaseSuccess {
		// 待发布和发布成功状态，可以删除，避免由于文档的处理中标记错误导致无法删除
		return true
	}
	if d.Status == DocStatusAuditIng ||
		d.Status == DocStatusUnderAppeal ||
		d.IsLearning() ||
		d.IsCreatingQaV1() ||
		d.IsCreatingIndexV1() {
		return false
	}
	return true
}

// IsAuditing 文档审核中
func (d *Doc) IsAuditing() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusAuditIng ||
		d.Status == DocStatusDocNameAuditing {
		return true
	}
	return false
}

// IsAuditFailed 文档审核失败, 包含文档内容审核失败以及文档名称审核失败
func (d *Doc) IsAuditFailed() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusAuditFail ||
		d.Status == DocStatusDocNameAndContentAuditFail {
		return true
	}
	return false
}

// IsDocNameAuditFailed 文档名称审核失败, 包含文档重命名审核失败以及文档导入名称审核失败
func (d *Doc) IsDocNameAuditFailed() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusDocNameAuditFail ||
		d.Status == DocStatusImportDocNameAuditFail {
		return true
	}
	return false
}

func (d *Doc) GetDocFileName() string {
	if d.FileNameInAudit == "" {
		return d.FileName
	}
	if !d.IsDocNameAuditFailed() {
		return d.FileNameInAudit
	}
	return d.FileName
}

// StatusCorrect 纠正状态
func (d *Doc) StatusCorrect() uint32 {
	// 所有超量失效状态都统一到超量失效
	if d.IsCharSizeExceeded() {
		return DocStatusCharExceeded
	}
	if time.Unix(0, 0).Before(d.ExpireEnd) && time.Now().After(d.ExpireEnd) {
		return DocStatusExpired
	}
	// 所有超量失效恢复状态都统一到超量失效恢复状态
	if d.IsResuming() {
		return DocStatusResuming
	}
	if d.IsLearning() {
		return DocStatusCreatingIndex
	}
	if d.IsLearnFail() {
		return DocStatusCreateIndexFail
	}
	if d.IsAuditing() {
		return DocStatusAuditIng
	}
	if d.IsAuditFailed() {
		return DocStatusAuditFail
	}
	if d.IsDocNameAuditFailed() {
		return DocStatusDocNameAuditFail
	}
	return d.Status
}

// StatusDesc 状态描述
func (d *Doc) StatusDesc(isPublishPause bool) string {
	if d == nil {
		return ""
	}
	if d.IsCharSizeExceeded() {
		return i18nkey.KeyCharExceeded
	}
	if d.IsResuming() {
		return i18nkey.KeyResuming
	}
	if isPublishPause && d.Status == DocStatusReleasing {
		return i18nkey.KeyReleasePause
	}
	if time.Unix(0, 0).Before(d.ExpireEnd) && time.Now().After(d.ExpireEnd) {
		return i18nkey.KeyExpired
	}
	if d.IsLearning() {
		return i18nkey.KeyCreatingIndex
	}
	if d.IsLearnFail() {
		return i18nkey.KeyCreateIndexFail
	}
	return statusDesc[d.Status]
}

// IsAllowRefer 是否允许操作refer
func (d *Doc) IsAllowRefer() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusAuditPass ||
		d.Status == DocStatusWaitRelease ||
		d.Status == DocStatusReleasing ||
		d.Status == DocStatusReleaseSuccess ||
		d.Status == DocStatusCreateFailed ||
		d.Status == DocStatusCreateSuccess ||
		d.Status == DocStatusUpdateFail {
		return true
	}
	return false
}

// IsAllowEdit 是否允许编辑操作
func (d *Doc) IsAllowEdit() bool {
	if d == nil {
		return false
	}
	if (d.Status == DocStatusWaitRelease ||
		d.Status == DocStatusReleaseSuccess ||
		d.Status == DocStatusUpdateFail) && !d.IsCreatingQaV1() {
		return true
	}
	return false
}

// IsReferOpen 是否开启引用链接
func (d *Doc) IsReferOpen() bool {
	if d == nil {
		return false
	}
	return d.IsRefer
}

// UseWebURL 引用来源 是否使用 WebURL内容 作为用户自定义链接
func (d *Doc) UseWebURL() bool {
	if d == nil {
		return false
	}
	if d.ReferURLType != ReferURLTypeUserDefined {
		return false
	}
	if d.WebURL == "" {
		return false
	}
	return true
}

// IsExcel 是否是excel
func (d *Doc) IsExcel() bool {
	if d == nil {
		return false
	}
	return d.FileType == FileTypeXlsx
}

func (d *Doc) IsExcelx() bool {
	if d == nil {
		return false
	}
	return d.FileType == FileTypeXlsx || d.FileType == FileTypeXls || d.FileType == FileTypeCsv || d.FileType == FileTypeNumbers
}

// IsBatchImport 是否批量导入操作
func (d *Doc) IsBatchImport() bool {
	switch d.Opt {
	case DocOptNormal, DocOptBatchImport:
		return true
	}
	return false
}

// NextActionDesc 文档发布描述
func (d *Doc) NextActionDesc() string {
	if d == nil {
		return ""
	}
	return docNextActionMap[d.NextAction]
}

// NeedAudit 文档是否需要审核
func (d *Doc) NeedAudit() bool {
	if d == nil {
		return false
	}
	return d.AuditFlag == AuditFlagWait
}

// IsNextActionAdd 是否新增操作
func (d *Doc) IsNextActionAdd() bool {
	if d == nil {
		return false
	}
	return d.NextAction == DocNextActionAdd
}

// GetType 标签类型
func (d *Doc) GetType() attributeEntity.LabelType {
	return attributeEntity.AttributeLabelTypeDOC
}

// IsExpire 是否过期
func (d *Doc) IsExpire() bool {
	if d == nil {
		return true
	}
	return !d.ExpireEnd.Equal(time.Unix(0, 0)) && d.ExpireEnd.Before(time.Now())
}

// IsCharSizeExceeded 文档是否被标记为超量
func (d *Doc) IsCharSizeExceeded() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusCharExceeded ||
		d.Status == DocStatusParseImportFailCharExceeded ||
		d.Status == DocStatusAuditFailCharExceeded ||
		d.Status == DocStatusUpdateFailCharExceeded ||
		d.Status == DocStatusCreateIndexFailCharExceeded ||
		d.Status == DocStatusExpiredCharExceeded ||
		d.Status == DocStatusAppealFailedCharExceeded {
		return true
	}
	return false
}

// IsResuming 文档是否正在恢复
func (d *Doc) IsResuming() bool {
	if d == nil {
		return false
	}
	for _, resumingStatus := range DocResumingStatusList {
		if d.Status == resumingStatus {
			return true
		}
	}
	return false
}

// IsStableStatus 判断是否稳态
func (d *Doc) IsStableStatus() bool {
	if d == nil {
		return false
	}
	if slices.Contains(DocStableStatus, d.Status) {
		return true
	}
	return false
}

// IsLearning 文档学习中
func (d *Doc) IsLearning() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusCreatingIndex || d.Status == DocStatusUpdating {
		return true
	}
	return false
}

// IsLearnFail 文档学习失败
func (d *Doc) IsLearnFail() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusCreateIndexFail || d.Status == DocStatusUpdateFail {
		return true
	}
	return false
}

// IsDocTypeCreateQA 文档类型是否允许创建QA
func (d *Doc) IsDocTypeCreateQA() bool {
	if d == nil {
		return false
	}
	// 跟ListSelectDoc接口类型保持一致
	if d.FileType == FileTypeDocx ||
		d.FileType == FileTypeMD ||
		d.FileType == FileTypeTxt ||
		d.FileType == FileTypePdf ||
		d.FileType == FileTypePptx ||
		d.FileType == FileTypePpt ||
		d.FileType == FileTypeDoc ||
		d.FileType == FileTypePng ||
		d.FileType == FileTypeJpg ||
		d.FileType == FileTypeJpeg ||
		d.FileType == FileTypeWps ||
		d.FileType == FileTypePPsx ||
		d.FileType == FileTypeTiff ||
		d.FileType == FileTypeBmp ||
		d.FileType == FileTypeGif ||
		d.FileType == FileTypeHtml ||
		d.FileType == FileTypeMhtml ||
		d.FileType == FileTypeJson ||
		d.FileType == FileTypeLog ||
		d.FileType == FileTypeXml ||
		d.FileType == FileTypeXmind ||
		d.FileType == FileTypeKeyNote ||
		d.FileType == FileTypePages {
		return true
	}
	return false
}

// CanRename 可以重命名
func (d *Doc) CanRename() bool {
	if d == nil {
		return false
	}
	if d.Status == DocStatusWaitRelease ||
		d.Status == DocStatusReleaseSuccess ||
		d.Status == DocStatusDocNameAuditFail ||
		d.Status == DocStatusImportDocNameAuditFail ||
		d.Status == DocStatusDocNameAppealFail {
		return true
	}
	return false
}

// GetRealFileName 配置端逻辑使用,用于获取实际的文件名,有改名的情况是改名后的
func (d *Doc) GetRealFileName() string {
	if d.FileNameInAudit != "" {
		return d.FileNameInAudit
	}
	return d.FileName
}

// GetFileNameByStatus 根据当前状态获取文件名
func (d *Doc) GetFileNameByStatus() string {
	if d.Status == DocStatusWaitRelease {
		return d.GetRealFileName()
	}
	return d.FileName
}

// IsCreatingQaV1 兼容旧版本
func (d *Doc) IsCreatingQaV1() bool {
	if d.IsProcessing([]uint64{DocProcessingFlagCreatingQA}) {
		return true
	}
	return d.IsCreatingQA
}

// IsCreatingIndexV1 兼容旧版本
func (d *Doc) IsCreatingIndexV1() bool {
	if d.IsProcessing([]uint64{DocProcessingFlagCreatingIndex}) {
		return true
	}
	return d.IsCreatingIndex
}

// IsProcessing 判断文档是否在处理中，不允许任务修改、删除相关操作
func (d *Doc) IsProcessing(flags []uint64) bool {
	if d == nil {
		return false
	}
	if len(flags) == 0 {
		return d.ProcessingFlag > 0
	}
	for _, flag := range flags {
		if d.ProcessingFlag&flag > 0 {
			return true
		}
	}
	return false
}

// AddProcessingFlag 添加处理中标识位
func (d *Doc) AddProcessingFlag(flags []uint64) {
	if len(flags) == 0 {
		return
	}
	for _, flag := range flags {
		d.ProcessingFlag |= flag
	}
	return
}

// RemoveProcessingFlag 去除处理中标识位
func (d *Doc) RemoveProcessingFlag(flags []uint64) {
	if len(flags) == 0 {
		return
	}
	for _, flag := range flags {
		d.ProcessingFlag = d.ProcessingFlag &^ flag
	}
	return
}

// HasAttributeFlag 判断文档是否包含指定属性
func (d *Doc) HasAttributeFlag(attribute uint64) bool {
	if d == nil {
		return false
	}
	if attribute == 0 {
		return true
	}
	if d.AttributeFlag&attribute > 0 {
		return true
	}
	return false
}

// AddAttributeFlag 添加文档属性标记
func (d *Doc) AddAttributeFlag(attributes []uint64) {
	if len(attributes) == 0 {
		return
	}
	for _, attr := range attributes {
		d.AttributeFlag |= attr
	}
	return
}

// RemoveAttributeFlag 去除文档属性标记
func (d *Doc) RemoveAttributeFlag(attributes []uint64) {
	if len(attributes) == 0 {
		return
	}
	for _, attr := range attributes {
		d.AttributeFlag = d.AttributeFlag &^ attr
	}
	return
}

// IsValidIntervene 文档是否可干预
func (d *Doc) IsValidIntervene(status uint32) bool {
	if d.IsProcessing([]uint64{DocProcessingFlagCreatingQA, DocProcessingFlagCreatingIndex, DocProcessingFlagHandlingDocDiffTask}) {
		return false
	}
	return status == DocStatusAuditFail || status == DocStatusWaitRelease ||
		status == DocStatusReleaseSuccess || status == DocStatusCreateIndexFail ||
		status == DocStatusParseFail || status == DocStatusAppealFailed
}

func (d *Doc) Init() {
	// 每次查出文档时，初始化状态机为当前文档的状态
	currStatus := strconv.Itoa(int(d.Status))

	d.FSM = fsm.NewFSM(
		currStatus,
		fsm.Events{
			// 解析
			{Name: EventProcessSuccess,
				// 解析中 --成功-> 审核中
				Src: []string{convx.Uint32ToString(DocStatusParseIng)}, Dst: convx.Uint32ToString(DocStatusAuditIng)},
			{Name: EventProcessFailed,
				// 解析中 --失败-> 解析失败
				Src: []string{convx.Uint32ToString(DocStatusParseIng)}, Dst: convx.Uint32ToString(DocStatusParseFail)},
			{Name: EventCloseAudit,
				// 解析中 --关闭审核-> 学习中
				Src: []string{convx.Uint32ToString(DocStatusParseIng)}, Dst: convx.Uint32ToString(DocStatusCreatingIndex)},

			// 审核
			{Name: EventProcessSuccess,
				// 审核中 --成功-> 学习中
				Src: []string{convx.Uint32ToString(DocStatusAuditIng)}, Dst: convx.Uint32ToString(DocStatusCreatingIndex)},
			{Name: EventProcessFailed,
				// 审核中 --失败-> 审核失败
				Src: []string{convx.Uint32ToString(DocStatusAuditIng)}, Dst: convx.Uint32ToString(DocStatusAuditFail)},
			{Name: EventAppealFailed,
				// 审核中 --人工申诉失败-> 人工申诉失败
				Src: []string{convx.Uint32ToString(DocStatusAuditIng)}, Dst: convx.Uint32ToString(DocStatusAppealFailed)},

			// 学习
			{Name: EventProcessSuccess,
				// 学习中 --成功-> 待发布
				Src: []string{convx.Uint32ToString(DocStatusCreatingIndex)}, Dst: convx.Uint32ToString(DocStatusWaitRelease)},
			{Name: EventProcessFailed,
				// 学习中 --失败-> 审核失败
				Src: []string{convx.Uint32ToString(DocStatusCreatingIndex)}, Dst: convx.Uint32ToString(DocStatusCreateIndexFail)},

			// 每一步都要做的字符数超限检查
			{Name: EventUsedCharSizeExceeded,
				// 解析中|审核中|学习中 --字符超限-> 导入失败
				Src: []string{convx.Uint32ToString(DocStatusParseIng), convx.Uint32ToString(DocStatusAuditIng),
					convx.Uint32ToString(DocStatusCreatingIndex)}, Dst: convx.Uint32ToString(DocStatusParseImportFail)},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) { d.enterState(ctx, e) },
		},
	)
	return
}

// 状态转移回调
func (d *Doc) enterState(ctx context.Context, e *fsm.Event) {
	logx.D(ctx, "doc:%d status from:%s to:%s", d.ID, e.Src, e.Dst)
	intDst, err := strconv.Atoi(e.Dst)
	if err != nil {
		logx.E(ctx, "doc:%d status from:%s to:%s err:%v", d.ID, e.Src, e.Dst, err)
		return
	}
	d.Status = uint32(intDst)
}

func IsTableTypeDocument(fileType string) bool {
	return fileType == FileTypeXlsx || fileType == FileTypeXls || fileType == FileTypeCsv || fileType == FileTypeNumbers
}

// IsValidDocFilterFlag 是否支持的文档筛选标识位
func IsValidDocFilterFlag(flag string) bool {
	if _, ok := docFilterFlags[flag]; !ok {
		return false
	}
	return true
}
