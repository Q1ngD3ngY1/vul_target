package qa

import (
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/kb/kb-config/internal/config"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	attrEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

const (
	QAAttributeFlagDisable = 0x01 // 第一位，问答停用（0未停用，1停用）
)

const (
	DocQaTableName = "t_doc_qa"

	DocQaTblColId              = "id"
	DocQaTblColBusinessId      = "business_id"
	DocQaTblColRobotId         = "robot_id"
	DocQaTblColCorpId          = "corp_id"
	DocQaTblColStaffId         = "staff_id"
	DocQaTblColDocId           = "doc_id"
	DocQaTblColOriginDocId     = "origin_doc_id"
	DocQaTblColSegmentId       = "segment_id"
	DocQaTblColCategoryId      = "category_id"
	DocQaTblColSource          = "source"
	DocQaTblColQuestion        = "question"
	DocQaTblColAnswer          = "answer"
	DocQaTblColCustomParam     = "custom_param"
	DocQaTblColQuestionDesc    = "question_desc"
	DocQaTblColReleaseStatus   = "release_status"
	DocQaTblColIsAuditFree     = "is_audit_free"
	DocQaTblColIsDeleted       = "is_deleted"
	DocQaTblColMessage         = "message"
	DocQaTblColAcceptStatus    = "accept_status"
	DocQaTblColSimilarStatus   = "similar_status"
	DocQaTblColNextAction      = "next_action"
	DocQaTblColCharSize        = "char_size"
	DocQaTblColAttrRange       = "attr_range"
	DocQaTblColCreateTime      = "create_time"
	DocQaTblColUpdateTime      = "update_time"
	DocQaTblColExpireStart     = "expire_start"
	DocQaTblColExpireEnd       = "expire_end"
	DocQaTblColSimilarQuestion = "similar_question"
	DocQaTblColAttributeFlag   = "attribute_flag"
	DocQaTblEnableScope        = "enable_scope"
	DocQaTblColQaSize          = "qa_size"

	DocQaTableMaxPageSize = 1000
)

var DocQaTblColList = []string{DocQaTblColId, DocQaTblColBusinessId, DocQaTblColRobotId, DocQaTblColCorpId,
	DocQaTblColStaffId, DocQaTblColDocId, DocQaTblColOriginDocId, DocQaTblColSegmentId, DocQaTblColCategoryId,
	DocQaTblColSource, DocQaTblColQuestion, DocQaTblColAnswer, DocQaTblColCustomParam, DocQaTblColQuestionDesc,
	DocQaTblColReleaseStatus, DocQaTblColIsAuditFree, DocQaTblColIsDeleted, DocQaTblColMessage, DocQaTblColAcceptStatus,
	DocQaTblColSimilarStatus, DocQaTblColNextAction, DocQaTblColCharSize, DocQaTblColAttrRange, DocQaTblColCreateTime,
	DocQaTblColUpdateTime, DocQaTblColExpireStart, DocQaTblColExpireEnd, DocQaTblColAttributeFlag, DocQaTblEnableScope, DocQaTblColQaSize}

var (
	QaExceedStatus = []uint32{
		QAReleaseStatusCharExceeded,
		QAReleaseStatusResuming,
		QAReleaseStatusAppealFailCharExceeded,
		QAReleaseStatusAppealFailResuming,
		QAReleaseStatusAuditNotPassCharExceeded,
		QAReleaseStatusAuditNotPassResuming,
		QAReleaseStatusLearnFailCharExceeded,
		QAReleaseStatusLearnFailResuming,
	}
	QaExceedResumingStatus = []uint32{
		QAReleaseStatusResuming,
		QAReleaseStatusAppealFailResuming,
		QAReleaseStatusAuditNotPassResuming,
		QAReleaseStatusLearnFailResuming}
)

type DocQaFilter struct {
	QAId              uint64
	QAIds             []uint64
	BusinessId        uint64 // 业务 ID
	CorpId            uint64 // 企业 ID
	RobotId           uint64
	SegmentIDs        []uint64
	RobotIDs          []uint64
	DocID             uint64
	DocIDs            []uint64
	OriginDocID       uint64
	IsDeleted         *uint32
	BusinessIds       []uint64
	Question          string
	ActionList        []uint32
	MaxUpdateTime     time.Time
	MinUpdateTime     time.Time
	MinEqCreateTime   time.Time
	Offset            int
	Limit             int
	OrderColumn       []string
	OrderDirection    []string
	CategoryIds       []uint64
	SimilarStatus     *uint32
	ReleaseStatusList []uint32
	ReleaseStatusNot  uint32
	AcceptStatus      uint32
	ReleaseCount      bool
	EnableScope       *uint32
	ExtraCondition    string
	ExtraParams       []any

	RawQuery     string
	RawQueryArgs []any
}

// DocQA 文档问答对
type DocQA struct {
	ID                  uint64    `db:"id" gorm:"column:id"`
	BusinessID          uint64    `db:"business_id" gorm:"column:business_id"`       // 业务ID
	RobotID             uint64    `db:"robot_id" gorm:"column:robot_id"`             // 机器人ID
	CorpID              uint64    `db:"corp_id" gorm:"column:corp_id"`               // 企业ID
	StaffID             uint64    `db:"staff_id" gorm:"column:staff_id"`             // 员工ID
	DocID               uint64    `db:"doc_id" gorm:"column:doc_id"`                 // 文档ID
	OriginDocID         uint64    `db:"origin_doc_id" gorm:"column:origin_doc_id"`   // 初始来源文档ID
	SegmentID           uint64    `db:"segment_id" gorm:"column:segment_id"`         // 段落ID
	CategoryID          uint64    `db:"category_id" gorm:"column:category_id"`       // 分类ID
	Source              uint32    `db:"source" gorm:"column:source"`                 // 来源(1 文档生成 2 批量导入 3 手动添加)
	Question            string    `db:"question" gorm:"column:question"`             // 问题
	Answer              string    `db:"answer" gorm:"column:answer"`                 // 答案
	CustomParam         string    `db:"custom_param" gorm:"column:custom_param"`     // 自定义参数
	QuestionDesc        string    `db:"question_desc" gorm:"column:question_desc"`   // 问题描述
	ReleaseStatus       uint32    `db:"release_status" gorm:"column:release_status"` // 发布状态(1 未发布 2 待发布 3 发布中 4 已发布 5 发布失败 6 不采纳 7 审核中 8 审核失败)
	IsAuditFree         bool      `db:"is_audit_free" gorm:"column:is_audit_free"`   // 免审 0 不免审（需要机器审核） 1 免审（无需机器审核）
	IsDeleted           uint32    `db:"is_deleted" gorm:"column:is_deleted"`         // 1未删除 2已删除
	Message             string    `db:"message" gorm:"column:message"`               // 失败原因
	AcceptStatus        uint32    `db:"accept_status" gorm:"column:accept_status"`   // 1未处理2采纳3不采纳
	SimilarStatus       uint32    `db:"similar_status" gorm:"column:similar_status"` // 相似度匹配状态 (0未处理 1匹配中 2已匹配)
	NextAction          uint32    `db:"next_action" gorm:"column:next_action"`       // 面向发布操作：1新增 2修改 3删除 4发布
	CharSize            uint64    `db:"char_size" gorm:"column:char_size"`           // 问答对字符长度
	AttrRange           uint32    `db:"attr_range" gorm:"column:attr_range"`         // 属性标签适用范围 1 全部 2 按条件设置
	CreateTime          time.Time `db:"create_time" gorm:"column:create_time"`       // 创建时间
	UpdateTime          time.Time `db:"update_time" gorm:"column:update_time"`       // 更新时间
	ExpireStart         time.Time `db:"expire_start" gorm:"column:expire_start"`     // 有效期的开始时间
	ExpireEnd           time.Time `db:"expire_end" gorm:"column:expire_end"`         // 有效期的结束时间
	QaAuditFail         bool      // 问答中文本对审核失败，db中无此字段
	PicAuditFail        bool      // 答案的图片对审核失败，db中无此字段
	VideoAuditFail      bool      // 答案的视频对审核失败，db中无此字段
	SimilarQuestionTips string    `db:"similar_question" gorm:"column:similar_question"` // 相似问tips
	AttributeFlag       uint64    `db:"attribute_flag" gorm:"column:attribute_flag"`     // 问答属性标记，位运算 0：问答是否停用
	EnableScope         uint32    `db:"enable_scope" gorm:"column:enable_scope"`         // 启用范围 1 不生效 2 仅开发域 3 仅发布域 4 全部
	QaSize              uint64    `db:"qa_size" gorm:"column:qa_size"`                   // 问答对字节数（含相似问）
}

// NewDocQA 构造文档问答对
func NewDocQA(doc *docEntity.Doc, segment *segEntity.DocSegmentExtend,
	qa *QA, categoryID uint64, isNeedAudit bool) *DocQA {
	now := time.Now()
	if !config.AuditSwitch() {
		isNeedAudit = false
	}
	return &DocQA{
		RobotID:     doc.RobotID,
		CorpID:      doc.CorpID,
		DocID:       gox.IfElse(doc.IsBatchImport() && doc.IsExcel(), docEntity.ExcelInitDocID, doc.ID),
		OriginDocID: doc.ID,
		SegmentID:   segment.ID,
		Source: gox.IfElse(doc.IsBatchImport() && doc.IsExcel(),
			docEntity.SourceFromBatch, docEntity.SourceFromDoc),
		Question:     qa.Question,
		Answer:       qa.Answer,
		CustomParam:  qa.CustomParam,
		QuestionDesc: qa.QuestionDesc,
		ReleaseStatus: func() uint32 {
			if isNeedAudit {
				return QAReleaseStatusAuditing
			}
			return QAReleaseStatusInit
		}(),
		IsAuditFree:   QAIsAuditNotFree,
		IsDeleted:     QAIsNotDeleted,
		AcceptStatus:  gox.IfElse(doc.IsBatchImport() && doc.IsExcel(), AcceptYes, AcceptInit),
		SimilarStatus: docEntity.SimilarStatusInit,
		NextAction:    NextActionAdd,
		CategoryID:    categoryID,
		AttrRange:     gox.IfElse(doc.IsBatchImport() && doc.IsExcel(), doc.AttrRange, docEntity.AttrRangeDefault),
		UpdateTime:    now,
		CreateTime:    now,
		ExpireStart:   qa.ExpireStart,
		ExpireEnd:     qa.ExpireEnd,
		AttributeFlag: qa.AttributeFlag,
		StaffID:       doc.StaffID,
		EnableScope:   gox.IfElse(qa.EnableScope != 0, qa.EnableScope, doc.EnableScope),
	}
}

type AppQAExceedCharSize struct {
	AppPrimaryId     uint64 `gorm:"robot_id"`         // 应用ID
	QAExceedCharSize uint64 `gorm:"exceed_char_size"` // 超量字符
}

// QAListReq 请求QAList
type QAListReq struct {
	RobotID         uint64
	CorpID          uint64
	CateIDs         []uint64
	Query           string
	QueryType       string
	QueryAnswer     string
	DocID           []uint64
	DocBizID        []uint64
	ExcludeDocID    []uint64
	AcceptStatus    []uint32
	ReleaseStatus   []uint32
	IsDeleted       uint32
	UpdateTime      time.Time
	UpdateTimeEqual bool
	QAID            uint64
	QAIDs           []uint64
	QABizIDs        []uint64
	Page            uint32
	PageSize        uint32
	Source          uint32
	EnableScope     *uint32
	ValidityStatus  uint32 // 当前问答对的有效期状态，1-未生效；2-生效中；3-已过期
}

// CateStat 按分类统计
type CateStat struct {
	CategoryID uint64 `db:"category_id"` // 分类ID
	Total      uint32 `db:"total"`
}

// DocQANum 文档问答对
type DocQANum struct {
	DocID     uint64 `gorm:"doc_id"` // 文档ID
	Total     uint32 `gorm:"total"`
	IsDeleted uint32 `gorm:"is_deleted"`
}

// QAStat 按处理状态 和 发布状态 统计
type QAStat struct {
	AcceptStatus  uint32 `gorm:"accept_status"`  // 处理状态
	ReleaseStatus uint32 `gorm:"release_status"` // 发布状态
	Total         uint32 `gorm:"total"`          // 总数
}

// QA 问答
type QA struct {
	BusinessID       uint64    `db:"business_id"`         // 业务ID
	Question         string    `json:"question"`          // 问题
	Answer           string    `json:"answer"`            // 答案
	SimilarQuestions []string  `json:"similar_questions"` // 相似问
	CustomParam      string    `json:"custom_param"`      // 自定义参数
	QuestionDesc     string    `json:"question_desc"`     // 问题意图描述
	Path             []string  `json:"path"`              // 分组路径
	ExpireStart      time.Time `db:"expire_start"`        // 有效期的开始时间
	ExpireEnd        time.Time `db:"expire_end"`          // 有效期的结束时间
	AttributeFlag    uint64    `json:"attribute_flag"`    // 问答属性标记，位运算 0：问答是否停用
	EnableScope      uint32    `json:"enable_scope"`      // 有效范围
}

// ReleaseQADetail 发布QA信息
type ReleaseQADetail struct {
	ID           uint64 `db:"id"`
	DocID        uint64 `db:"doc_id"`        // 文档ID
	IsDeleted    uint32 `db:"is_deleted"`    // 0未删除 1已删除
	AcceptStatus uint32 `db:"accept_status"` // 1未处理2采纳3不采纳
	NextAction   uint32 `db:"next_action"`   // 最后操作：1新增 2修改 3删除 4发布
}

// DocQAFile 文档问答文件
type DocQAFile struct {
	CosURL   string
	FileType int
	Size     int64
	ETag     string
}

// IsAllowRelease 是否允许发布
func (r *ReleaseQADetail) IsAllowRelease() bool {
	if r == nil {
		return false
	}
	// 新增且删除 => 不用发布
	if r.NextAction == NextActionAdd && r.IsDeleted == QAIsDeleted {
		return false
	}
	// 新增且未操作校验 => 不用发布
	if r.NextAction == NextActionAdd && r.AcceptStatus == AcceptInit {
		return false
	}
	// 新增且不采纳 => 不用发布
	if r.NextAction == NextActionAdd && r.AcceptStatus == AcceptNo {
		return false
	}
	return true
}

// ReleaseAction 发布动作
func (r *ReleaseQADetail) ReleaseAction() uint32 {
	if r == nil {
		return 0
	}
	if r.NextAction == NextActionUpdate && r.AcceptStatus == AcceptNo {
		return NextActionDelete
	}
	return r.NextAction
}

// IsAllowRelease 是否允许发布
func (d *DocQA) IsAllowRelease() bool {
	if d == nil {
		return false
	}
	// 新增且删除 => 不用发布
	if d.NextAction == NextActionAdd && d.IsDeleted == QAIsDeleted {
		return false
	}
	// 新增且未操作校验 => 不用发布
	if d.NextAction == NextActionAdd && d.AcceptStatus == AcceptInit {
		return false
	}
	// 新增且不采纳 => 不用发布
	if d.NextAction == NextActionAdd && d.AcceptStatus == AcceptNo {
		return false
	}
	return true
}

// ReleaseAction 发布动作
func (d *DocQA) ReleaseAction() uint32 {
	if d == nil {
		return 0
	}
	if d.NextAction == NextActionUpdate && d.AcceptStatus == AcceptNo {
		return NextActionDelete
	}
	return d.NextAction
}

// IsReleaseNeedAudit 发布是否需要审核
func (d *DocQA) IsReleaseNeedAudit() bool {
	if d == nil {
		return false
	}
	return d.ReleaseAction() != NextActionDelete
}

// GetExpireTime 如果未读取到时间，则给向量库传0
func (d *DocQA) GetExpireTime() int64 {
	expireTime := d.ExpireEnd.Unix()
	if expireTime < 0 {
		return 0
	}
	return expireTime
}

// NextActionDesc 动作描述
func (d *DocQA) NextActionDesc() string {
	if d == nil {
		return ""
	}
	return QANextActionDesc[d.NextAction]
}

// SourceDesc 状态描述
func (d *DocQA) SourceDesc(docs map[uint64]*docEntity.Doc) string {
	if d == nil {
		return ""
	}
	if d.Source != docEntity.SourceFromDoc {
		return d.SourceName()
	}
	doc, ok := docs[d.DocID]
	if !ok {
		return i18nkey.KeyUnknown
	}
	return doc.GetFileNameByStatus()
}

// DocBizID 文档业务ID
func (d *DocQA) DocBizID(docs map[uint64]*docEntity.Doc) uint64 {
	if d == nil {
		return 0
	}
	doc, ok := docs[d.DocID]
	if !ok {
		return 0
	}
	return doc.BusinessID
}

// CateBizID 文档业务ID
func (d *DocQA) CateBizID(cates map[uint64]*cateEntity.CateInfo) uint64 {
	if d == nil {
		return 0
	}
	cate, ok := cates[d.CategoryID]
	if !ok {
		return 0
	}
	return cate.BusinessID
}

// SourceName 状态描述
func (d *DocQA) SourceName() string {
	if d == nil {
		return ""
	}
	return docEntity.SourceDesc[d.Source]
}

// Status 状态值 1未校验2未发布3发布中4已发布
func (d *DocQA) Status() uint32 {
	if d == nil {
		return 0
	}
	if d.IsCharExceeded() {
		return QAReleaseStatusCharExceeded
	}
	if d.IsResuming() {
		return QAReleaseStatusResuming
	}
	if d.AcceptStatus == AcceptInit {
		return AcceptInit
	}
	if d.AcceptStatus == AcceptNo {
		return QAReleaseStatusAcceptNo
	}
	if time.Unix(0, 0).Before(d.ExpireEnd) && time.Now().After(d.ExpireEnd) {
		return QAReleaseStatusExpired
	}
	return d.ReleaseStatus
}

// StatusDesc 状态描述
func (d *DocQA) StatusDesc(isPublishPause bool) string {
	if d == nil {
		return ""
	}
	if d.IsCharExceeded() {
		return i18nkey.KeyCharExceeded
	}
	if d.IsResuming() {
		return i18nkey.KeyResuming
	}
	if d.AcceptStatus == AcceptInit {
		return i18nkey.KeyNotVerified
	}
	if d.AcceptStatus == AcceptNo {
		return i18nkey.KeyNotAdopted
	}
	if time.Unix(0, 0).Before(d.ExpireEnd) && time.Now().After(d.ExpireEnd) {
		return i18nkey.KeyExpired
	}
	// 发布暂停时候，需要把没有发布成功的置为发布暂停文案，但是不支持搜索
	if isPublishPause && d.ReleaseStatus == QAReleaseStatusIng {
		return i18nkey.KeyReleasePause
	}
	if status, ok := qaStatusMap[d.ReleaseStatus]; ok {
		return status
	}
	return ""
}

// IsDelete 是否已删除
func (d *DocQA) IsDelete() bool {
	if d == nil {
		return false
	}
	return d.IsDeleted == QAIsDeleted
}

// IsAccepted 是否采纳
func (d *DocQA) IsAccepted() bool {
	if d == nil {
		return false
	}
	return d.AcceptStatus == AcceptYes
}

// IsNotAccepted 是否未采纳
func (d *DocQA) IsNotAccepted() bool {
	if d == nil {
		return false
	}
	return d.AcceptStatus == AcceptNo
}

// IsAllowEdit 是否允许编辑
func (d *DocQA) IsAllowEdit() bool {
	if d == nil {
		return false
	}
	return d.ReleaseStatus != QAReleaseStatusAppealIng && d.ReleaseStatus != QAReleaseStatusAuditing &&
		d.ReleaseStatus != QAReleaseStatusLearning && !d.IsResuming()
}

// IsAllowAccept 是否允许校验
func (d *DocQA) IsAllowAccept() bool {
	if d == nil {
		return false
	}
	return d.AcceptStatus == AcceptInit || d.AcceptStatus == AcceptNo
}

// IsAllowDelete 是否允许删除
func (d *DocQA) IsAllowDelete() bool {
	if d == nil {
		return false
	}
	return d.IsDeleted == QAIsNotDeleted && d.ReleaseStatus != QAReleaseStatusAppealIng &&
		d.ReleaseStatus != QAReleaseStatusAuditing && d.ReleaseStatus != QAReleaseStatusLearning && !d.IsResuming()
}

// IsNextActionAdd 是否新增操作
func (d *DocQA) IsNextActionAdd() bool {
	if d == nil {
		return false
	}
	return d.NextAction == NextActionAdd
}

// GetType 标签类型
func (d *DocQA) GetType() attrEntity.LabelType {
	return attrEntity.AttributeLabelTypeQA
}

// IsExpire 是否过期
func (d *DocQA) IsExpire() bool {
	if d == nil {
		return true
	}
	return !d.ExpireEnd.Equal(time.Unix(0, 0)) && d.ExpireEnd.Before(time.Now())
}

// GetID 返回主键id
func (d *DocQA) GetID() uint64 {
	if d == nil {
		return 0
	}
	return d.ID
}

// IsCharExceeded 超量隔离状态
func (d *DocQA) IsCharExceeded() bool {
	if d == nil {
		return false
	}
	if d.ReleaseStatus == QAReleaseStatusCharExceeded ||
		d.ReleaseStatus == QAReleaseStatusAppealFailCharExceeded ||
		d.ReleaseStatus == QAReleaseStatusAuditNotPassCharExceeded ||
		d.ReleaseStatus == QAReleaseStatusLearnFailCharExceeded {
		return true
	}
	return false
}

// IsResuming 超量隔离状态恢复
func (d *DocQA) IsResuming() bool {
	if d == nil {
		return false
	}
	if d.ReleaseStatus == QAReleaseStatusResuming ||
		d.ReleaseStatus == QAReleaseStatusAppealFailResuming ||
		d.ReleaseStatus == QAReleaseStatusAuditNotPassResuming ||
		d.ReleaseStatus == QAReleaseStatusLearnFailResuming {
		return true
	}
	return false
}

// IsStableStatus 判断是否稳态
func (d *DocQA) IsStableStatus() bool {
	if d == nil {
		return false
	}
	for _, status := range QAStableStatus {
		if d.ReleaseStatus == status {
			return true
		}
	}
	return false
}

// HasAttributeFlag 判断文档是否包含指定属性
func (d *DocQA) HasAttributeFlag(attribute uint64) bool {
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

// CalcQACharSize 计算doc的charSize(含相似问)
func CalcQACharSize(docQa *QA) uint64 {
	if docQa == nil {
		return 0
	}
	var simCharSize = 0
	if len(docQa.SimilarQuestions) > 0 {
		for _, q := range docQa.SimilarQuestions {
			simCharSize += utf8.RuneCountInString(q)
		}
	}
	return uint64(utf8.RuneCountInString(docQa.Question+docQa.Answer)) + uint64(simCharSize)
}

// CalcQABytes 计算问答的的byteSize(含相似问)
func CalcQABytes(docQa *QA) uint64 {
	if docQa == nil {
		return 0
	}
	var simCharSize = 0
	if len(docQa.SimilarQuestions) > 0 {
		for _, q := range docQa.SimilarQuestions {
			simCharSize += len(q)
		}
	}
	return uint64(len(docQa.Question+docQa.Answer)) + uint64(simCharSize)
}
