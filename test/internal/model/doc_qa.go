package model

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"

	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
)

const (
	// ExcelInitDocID excel批量导入初始对外值
	ExcelInitDocID = 0

	// QAIsNotDeleted 未删除
	QAIsNotDeleted = 1
	// QAIsDeleted 已删除
	QAIsDeleted = 2

	// SourceFromDoc 文档生成
	SourceFromDoc = uint32(1)
	// SourceFromBatch 批量导入
	SourceFromBatch = uint32(2)
	// SourceFromManual 手动添加
	SourceFromManual = uint32(3)

	// QA状态：优先展示校验状态，如果已校验再展示发布状态。t_doc_qa和t_release_qa共用的状态字段

	// QAReleaseStatusInit 未发布
	QAReleaseStatusInit = uint32(2)
	// QAReleaseStatusIng 发布中
	QAReleaseStatusIng = uint32(3)
	// QAReleaseStatusSuccess 已发布
	QAReleaseStatusSuccess = uint32(4)
	// QAReleaseStatusFail 发布失败
	QAReleaseStatusFail = uint32(5)
	// QAReleaseStatusAcceptNo 不采纳
	QAReleaseStatusAcceptNo = uint32(6)
	// QAReleaseStatusAuditing 审核中
	QAReleaseStatusAuditing = uint32(7)
	// QAReleaseStatusAuditNotPass 审核不通过【给页面展示审核失败】
	QAReleaseStatusAuditNotPass = uint32(8)
	// QAReleaseStatusAppealIng 人工审核中
	QAReleaseStatusAppealIng = uint32(9)
	// QAReleaseStatusAppealFail 人工审核不通过
	QAReleaseStatusAppealFail = uint32(11)
	// QAReleaseStatusExpired 已过期
	QAReleaseStatusExpired = uint32(12)
	// QAReleaseStatusCharExceeded 超量失效
	QAReleaseStatusCharExceeded = uint32(13)
	// QAReleaseStatusResuming 超量失效恢复
	QAReleaseStatusResuming = uint32(14)
	// QAReleaseStatusAppealFailCharExceeded 人工审核不通过-超量失效
	QAReleaseStatusAppealFailCharExceeded = uint32(15)
	// QAReleaseStatusAppealFailResuming 人工审核不通过-超量失效恢复
	QAReleaseStatusAppealFailResuming = uint32(16)
	// QAReleaseStatusAuditNotPassCharExceeded 审核失败-超量失效
	QAReleaseStatusAuditNotPassCharExceeded = uint32(17)
	// QAReleaseStatusAuditNotPassResuming 审核失败-超量失效恢复
	QAReleaseStatusAuditNotPassResuming = uint32(18)
	// QAReleaseStatusLearning 学习中
	QAReleaseStatusLearning = uint32(19)

	// QAReleaseStatusLearnFail 学习失败
	QAReleaseStatusLearnFail = uint32(20)
	// QAReleaseStatusLearnFailCharExceeded 学习失败-超量失效
	QAReleaseStatusLearnFailCharExceeded = uint32(21)
	// QAReleaseStatusLearnFailResuming 学习失败-超量失效恢复
	QAReleaseStatusLearnFailResuming = uint32(22)

	// QAIsAuditFree qa 问答免审
	QAIsAuditFree = true
	// QAIsAuditNotFree qa 问答不免审
	QAIsAuditNotFree = false

	// AcceptInit 未校验
	AcceptInit = uint32(1)
	// AcceptYes 采纳
	AcceptYes = uint32(2)
	// AcceptNo 不采纳
	AcceptNo = uint32(3)

	// SimilarStatusInit 未处理
	SimilarStatusInit = 0
	// SimilarStatusIng 匹配中
	SimilarStatusIng = 1
	// SimilarStatusEnd 已匹配
	SimilarStatusEnd = 2

	// ExportQANoticeContent 导出QA通知内容
	ExportQANoticeContent = i18nkey.KeyQaLibraryBatchExportStatus
	// ExportQANoticeContentIng 导出QA通知中通知
	ExportQANoticeContentIng = i18nkey.KeyQaLibraryBatchExporting
	// QABusinessSourceDefault QA数据业务来源，默认问答模块
	QABusinessSourceDefault = uint32(0)
	// QABusinessSourceUnsatisfiedReply QA数据业务来源，来自不满意回复
	QABusinessSourceUnsatisfiedReply = uint32(1)

	// QaUnExpiredStatus 未过期
	QaUnExpiredStatus = uint32(2)
	// QaExpiredStatus 已过期
	QaExpiredStatus = uint32(3)

	// QaVideoFile 问答对视频文件类型
	QaVideoFile = 1

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

	ShowCurrCate = 1 //文档/问答列表只展示当前分类数据
)

// QAStableStatus 问答稳定状态，即不会再自动流转到其他状态
var QAStableStatus = []uint32{
	QAReleaseStatusInit,
	QAReleaseStatusSuccess,
	QAReleaseStatusFail,
	QAReleaseStatusAcceptNo,
	QAReleaseStatusAuditNotPass,
	QAReleaseStatusAppealFail,
	QAReleaseStatusExpired,
	QAReleaseStatusCharExceeded,
	QAReleaseStatusAppealFailCharExceeded,
	QAReleaseStatusAuditNotPassCharExceeded,
	QAReleaseStatusLearnFail,
	QAReleaseStatusLearnFailCharExceeded,
}

// QAUnstableStatus 问答非稳定状态，即进行中的状态，可能会自动流转到其他状态
var QAUnstableStatus = []uint32{
	QAReleaseStatusIng,
	QAReleaseStatusAuditing,
	QAReleaseStatusAppealIng,
	QAReleaseStatusResuming,
	QAReleaseStatusAppealFailResuming,
	QAReleaseStatusAuditNotPassResuming,
	QAReleaseStatusLearning,
	QAReleaseStatusLearnFailResuming,
}

var qaNextActionDesc = map[uint32]string{
	NextActionAdd:     i18nkey.KeyAdd,
	NextActionUpdate:  i18nkey.KeyModify,
	NextActionDelete:  i18nkey.KeyDeleted,
	NextActionPublish: i18nkey.KeyPublish,
}

var sourceDesc = map[uint32]string{
	SourceFromDoc:    i18nkey.KeyFileGeneration,
	SourceFromBatch:  i18nkey.KeyBatchImport,
	SourceFromManual: i18nkey.KeyManualEntry,
}

var qaStatusMap = map[uint32]string{
	QAReleaseStatusInit:         i18nkey.KeyWaitRelease,
	QAReleaseStatusIng:          i18nkey.KeyReleasing,
	QAReleaseStatusSuccess:      i18nkey.KeyReleaseSuccess,
	QAReleaseStatusFail:         i18nkey.KeyPublishingFailed,
	QAReleaseStatusAuditing:     i18nkey.KeyAuditIng,
	QAReleaseStatusAuditNotPass: i18nkey.KeyAuditFail,
	QAReleaseStatusAppealIng:    i18nkey.KeyUnderAppeal,
	QAReleaseStatusAppealFail:   i18nkey.KeyAppealFailed,
	QAReleaseStatusExpired:      i18nkey.KeyExpired,
	QAReleaseStatusCharExceeded: i18nkey.KeyCharExceeded,
	QAReleaseStatusResuming:     i18nkey.KeyResuming,
	QAReleaseStatusLearning:     i18nkey.KeyCreatingIndex,
	QAReleaseStatusLearnFail:    i18nkey.KeyCreateIndexFail,
}

const (
	QAAttributeFlagDisable = 0x01 // 第一位，问答停用（0未停用，1停用）
)

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
	IsDeleted           int       `db:"is_deleted" gorm:"column:is_deleted"`         // 1未删除 2已删除
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
}

// NewDocQA 构造文档问答对
func NewDocQA(doc *Doc, segment *DocSegmentExtend, qa *QA, categoryID uint64, isNeedAudit bool) *DocQA {
	now := time.Now()
	if !config.AuditSwitch() {
		isNeedAudit = false
	}
	return &DocQA{
		RobotID:      doc.RobotID,
		CorpID:       doc.CorpID,
		DocID:        utils.When(doc.IsBatchImport() && doc.IsExcel(), ExcelInitDocID, doc.ID),
		OriginDocID:  doc.ID,
		SegmentID:    segment.ID,
		Source:       utils.When(doc.IsBatchImport() && doc.IsExcel(), SourceFromBatch, SourceFromDoc),
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
		AcceptStatus:  utils.When(doc.IsBatchImport() && doc.IsExcel(), AcceptYes, AcceptInit),
		SimilarStatus: SimilarStatusInit,
		NextAction:    NextActionAdd,
		CategoryID:    categoryID,
		AttrRange:     utils.When(doc.IsBatchImport() && doc.IsExcel(), doc.AttrRange, AttrRangeDefault),
		UpdateTime:    now,
		CreateTime:    now,
		ExpireStart:   qa.ExpireStart,
		ExpireEnd:     qa.ExpireEnd,
		AttributeFlag: qa.AttributeFlag,
		StaffID:       doc.StaffID,
	}
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
	IsDeleted       int
	UpdateTime      time.Time
	UpdateTimeEqual bool
	QAID            uint64
	QAIDs           []uint64
	QABizIDs        []uint64
	Page            uint32
	PageSize        uint32
	Source          uint32
	ValidityStatus  uint32 // 当前问答对的有效期状态，1-未生效；2-生效中；3-已过期
}

// CateStat 按分类统计
type CateStat struct {
	CategoryID uint64 `db:"category_id"` // 分类ID
	Total      uint32 `db:"total"`
}

// DocQANum 文档问答对
type DocQANum struct {
	DocID     uint64 `db:"doc_id"` // 文档ID
	Total     uint32 `db:"total"`
	IsDeleted uint32 `db:"is_deleted"`
}

// QAStat 按处理状态 和 发布状态 统计
type QAStat struct {
	AcceptStatus  uint32 `db:"accept_status"`  // 处理状态
	ReleaseStatus uint32 `db:"release_status"` // 发布状态
	Total         uint32 `db:"total"`          // 总数
}

// QA 问答
type QA struct {
	Question         string    `json:"question"`          // 问题
	Answer           string    `json:"answer"`            // 答案
	SimilarQuestions []string  `json:"similar_questions"` // 相似问
	CustomParam      string    `json:"custom_param"`      // 自定义参数
	QuestionDesc     string    `json:"question_desc"`     // 问题意图描述
	Path             []string  `json:"path"`              // 分组路径
	ExpireStart      time.Time `db:"expire_start"`        // 有效期的开始时间
	ExpireEnd        time.Time `db:"expire_end"`          // 有效期的结束时间
	AttributeFlag    uint64    `json:"attribute_flag"`    // 问答属性标记，位运算 0：问答是否停用
}

// ReleaseQADetail 发布QA信息
type ReleaseQADetail struct {
	ID           uint64 `db:"id"`
	DocID        uint64 `db:"doc_id"`        // 文档ID
	IsDeleted    int    `db:"is_deleted"`    // 0未删除 1已删除
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
	return qaNextActionDesc[d.NextAction]
}

// SourceDesc 状态描述
func (d *DocQA) SourceDesc(docs map[uint64]*Doc) string {
	if d == nil {
		return ""
	}
	if d.Source != SourceFromDoc {
		return d.SourceName()
	}
	doc, ok := docs[d.DocID]
	if !ok {
		return i18nkey.KeyUnknown
	}
	return doc.GetFileNameByStatus()
}

// DocBizID 文档业务ID
func (d *DocQA) DocBizID(docs map[uint64]*Doc) uint64 {
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
func (d *DocQA) CateBizID(cates map[uint64]*CateInfo) uint64 {
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
	return sourceDesc[d.Source]
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
	return qaStatusMap[d.ReleaseStatus]
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

// IsDisable 问答是否停用
func (d *DocQA) IsDisable() bool {
	if d == nil {
		return false
	}
	if d.AttributeFlag&QAAttributeFlagDisable > 0 {
		return true
	}
	return false
}

// GetType 标签类型
func (d *DocQA) GetType() LabelType {
	return AttributeLabelTypeQA
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
