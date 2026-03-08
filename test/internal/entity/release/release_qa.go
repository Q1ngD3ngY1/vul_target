package release

import (
	"time"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
)

const (
	// SysLabelQAFlagName 系统标签(for相似问场景)
	SysLabelQAFlagName = "_sys_str_qa_flag"
	// SysLabelQAFlagValueSimilar 标识相似问
	SysLabelQAFlagValueSimilar = "similar"
	// SysLabelQAIdName 标识相似问的qaId
	SysLabelQAIdName = "_sys_str_qa_id"
	// SysLabelDocIDName 文档ID默认标签
	SysLabelDocIDName = "DocID"
	// SysLabelCategoryKeyName  分类默认标签
	SysLabelCategoryKeyName = "lke_category_key"
	// SysLabelRoleKeyName 权限角色默认标签
	SysLabelRoleKeyName = "lke_role_key"
)

// ReleaseQA 文档问答对
type ReleaseQA struct {
	ID             uint64    `db:"id"`
	RobotID        uint64    `db:"robot_id"`         // 机器人ID
	CorpID         uint64    `db:"corp_id"`          // 企业ID
	StaffID        uint64    `db:"staff_id"`         // 员工ID
	VersionID      uint64    `db:"version_id"`       // 版本ID
	QAID           uint64    `db:"qa_id"`            // QAID
	DocID          uint64    `db:"doc_id"`           // 文档ID
	OriginDocID    uint64    `db:"origin_doc_id"`    // 初始来源文档ID
	SegmentID      uint64    `db:"segment_id"`       // 段落ID
	CategoryID     uint64    `db:"category_id"`      // 分类ID
	Source         uint32    `db:"source"`           // 来源(1 文档生成 2 批量导入 3 手动添加)
	Question       string    `db:"question"`         // 问题
	Answer         string    `db:"answer"`           // 答案
	CustomParam    string    `db:"custom_param"`     // 自定义参数
	QuestionDesc   string    `db:"question_desc"`    // 问题意图描述
	ReleaseStatus  uint32    `db:"release_status"`   // 发布状态(1 未发布 2 发布中 3 已发布 4 发布失败)
	IsDeleted      uint32    `db:"is_deleted"`       // 1未删除 2已删除
	Message        string    `db:"message"`          // 失败原因
	AcceptStatus   uint32    `db:"accept_status"`    // 1未处理2采纳3不采纳
	SimilarStatus  uint32    `db:"similar_status"`   // 相似度匹配状态 (0未处理 1匹配中 2已匹配)
	Action         uint32    `db:"action"`           // 面向发布操作：1新增 2修改 3删除 4发布
	CreateTime     time.Time `db:"create_time"`      // 创建时间
	UpdateTime     time.Time `db:"update_time"`      // 更新时间
	IsAllowRelease uint32    `db:"is_allow_release"` // 是否允许发布
	AuditStatus    uint32    `db:"audit_status"`     // 审核状态(1未审核 2审核中 3审核通过 4审核失败)
	AuditResult    string    `db:"audit_result"`     // 审核结果
	AttrLabels     string    `db:"attr_labels"`      // 属性标签
	ExpireTime     time.Time `db:"expire_time"`      // 有效结束时间
}

type ListReleaseQAReq struct {
	VersionID      uint64
	RobotID        uint64
	Page           uint32
	PageSize       uint32
	OrderBy        string
	Question       string
	IsAllowRelease *uint32
	IsDeleted      *uint32
	IsDeletedNot   *uint32
	MinQAID        uint64
	MaxQAID        uint64
	Actions        []uint32
	ReleaseStatus  []uint32
}

type ReleaseQAFilter struct {
	Id               uint64
	QAID             uint64
	VersionID        uint64
	RobotID          uint64
	Question         string
	IsAllowRelease   *uint32
	IsDeleted        *uint32
	IsDeletedNot     *uint32
	MinQAID          uint64
	MaxQAID          uint64
	Actions          []uint32
	ReleaseStatus    []uint32
	ReleaseStatusNot uint32
}

// AuditReleaseQA 待审核的发布问答
type AuditReleaseQA struct {
	ID          uint64 `db:"id"`
	QaID        uint64 `db:"qa_id"`        // 问答 ID
	Question    string `db:"question"`     // 问题
	Answer      string `db:"answer"`       // 答案
	AuditStatus uint32 `db:"audit_status"` // 审核状态
}

// ReleaseDocID 发布的文档ID
type ReleaseDocID struct {
	DocID uint64 `db:"doc_id"` // 文档ID
}

// AuditResultStat 审核结果统计
type AuditResultStat struct {
	Total       uint32 `db:"total"`
	AuditStatus uint32 `db:"audit_status"` // 审核状态(1未审核 2审核中 3审核通过 4审核失败)
}

// ForbidReleaseQA 禁止发布的QA
type ForbidReleaseQA struct {
	QAID uint64 `db:"qa_id"` // QAID
}

// ActionDesc 状态描述
func (r *ReleaseQA) ActionDesc() string {
	if r == nil {
		return ""
	}
	return qaEntity.QANextActionDesc[r.Action]
}

// SourceDesc 状态描述
func (r *ReleaseQA) SourceDesc(docs map[uint64]*docEntity.Doc) string {
	if r == nil {
		return ""
	}
	if r.Source != docEntity.SourceFromDoc {
		return docEntity.SourceDesc[r.Source]
	}
	doc, ok := docs[r.DocID]
	if !ok {
		return "未知"
	}
	return doc.FileName
}

// IsAuditDoing 是否审核中
func (r *ReleaseQA) IsAuditDoing() bool {
	if r == nil {
		return false
	}
	return r.AuditStatus == ReleaseQAAuditStatusDoing
}

// IsAuditSuccess 是否审核成功
func (r *ReleaseQA) IsAuditSuccess() bool {
	if r == nil {
		return false
	}
	return r.AuditStatus == ReleaseQAAuditStatusSuccess
}

// IsAuditFail 是否审核失败
func (r *ReleaseQA) IsAuditFail() bool {
	if r == nil {
		return false
	}
	return r.AuditStatus == ReleaseQAAuditStatusFail
}
