package release

import (
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
)

const (
	ReleaseQaSimilarQuestionTblIDCol             = "id"
	ReleaseQaSimilarQuestionTblCorpIDCol         = "corp_id"
	ReleaseQaSimilarQuestionTblStaffIDCol        = "staff_id"
	ReleaseQaSimilarQuestionTblRobotIDCol        = "robot_id"
	ReleaseQaSimilarQuestionTblCreateUserIDCol   = "create_user_id"
	ReleaseQaSimilarQuestionTblVersionIDCol      = "version_id"
	ReleaseQaSimilarQuestionTblSimilarIDCol      = "similar_id"
	ReleaseQaSimilarQuestionTblRelatedQaIDCol    = "related_qa_id"
	ReleaseQaSimilarQuestionTblSourceCol         = "source"
	ReleaseQaSimilarQuestionTblQuestionCol       = "question"
	ReleaseQaSimilarQuestionTblReleaseStatusCol  = "release_status"
	ReleaseQaSimilarQuestionTblMessageCol        = "message"
	ReleaseQaSimilarQuestionTblActionCol         = "action"
	ReleaseQaSimilarQuestionTblAttrLabelsCol     = "attr_labels"
	ReleaseQaSimilarQuestionTblAuditStatusCol    = "audit_status"
	ReleaseQaSimilarQuestionTblAuditResultCol    = "audit_result"
	ReleaseQaSimilarQuestionTblIsAllowReleaseCol = "is_allow_release"
	ReleaseQaSimilarQuestionTblExpireTimeCol     = "expire_time"
	ReleaseQaSimilarQuestionTblIsDeletedCol      = "is_deleted"
	ReleaseQaSimilarQuestionTblCreateTimeCol     = "create_time"
	ReleaseQaSimilarQuestionTblUpdateTimeCol     = "update_time"
)

var ReleaseQaSimilarQuestionColList = []string{
	ReleaseQaSimilarQuestionTblIDCol,
	ReleaseQaSimilarQuestionTblCorpIDCol,
	ReleaseQaSimilarQuestionTblStaffIDCol,
	ReleaseQaSimilarQuestionTblRobotIDCol,
	ReleaseQaSimilarQuestionTblCreateUserIDCol,
	ReleaseQaSimilarQuestionTblVersionIDCol,
	ReleaseQaSimilarQuestionTblSimilarIDCol,
	ReleaseQaSimilarQuestionTblRelatedQaIDCol,
	ReleaseQaSimilarQuestionTblSourceCol,
	ReleaseQaSimilarQuestionTblQuestionCol,
	ReleaseQaSimilarQuestionTblReleaseStatusCol,
	ReleaseQaSimilarQuestionTblMessageCol,
	ReleaseQaSimilarQuestionTblActionCol,
	ReleaseQaSimilarQuestionTblAttrLabelsCol,
	ReleaseQaSimilarQuestionTblAuditStatusCol,
	ReleaseQaSimilarQuestionTblAuditResultCol,
	ReleaseQaSimilarQuestionTblIsAllowReleaseCol,
	ReleaseQaSimilarQuestionTblExpireTimeCol,
	ReleaseQaSimilarQuestionTblIsDeletedCol,
	ReleaseQaSimilarQuestionTblCreateTimeCol,
	ReleaseQaSimilarQuestionTblUpdateTimeCol,
}

type ReleaseQaSimilarQuestion struct {
	ID             int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	CorpID         int64     `gorm:"column:corp_id;default:0" json:"corpId"`                                                     // 企业ID
	StaffID        int64     `gorm:"column:staff_id;default:0" json:"staffId"`                                                   // 员工ID
	RobotID        int64     `gorm:"column:robot_id;default:0" json:"robotId"`                                                   // 机器人ID
	CreateUserID   int64     `gorm:"column:create_user_id;default:0" json:"createUserId"`                                        // 上传用户ID
	VersionID      int64     `gorm:"column:version_id;default:0" json:"versionId"`                                               // 版本ID
	SimilarID      int64     `gorm:"column:similar_id;default:0" json:"similarId"`                                               // 相似问ID
	RelatedQaID    int64     `gorm:"column:related_qa_id;default:0" json:"relatedQaId"`                                          // 相关联的QA ID
	Source         int       `gorm:"column:source" json:"source"`                                                                // 来源(1 文档生成(废弃) 2 批量导入 3 手动添加)
	Question       string    `gorm:"column:question" json:"question"`                                                            // 问题
	ReleaseStatus  int       `gorm:"column:release_status" json:"releaseStatus"`                                                 // 发布状态(2 待发布 3 发布中 4 已发布 5 发布失败)
	Message        string    `gorm:"column:message" json:"message"`                                                              // 失败原因
	Action         uint32    `gorm:"column:action;default:0" json:"action"`                                                      // 操作行为：1新增2修改3删除 4发布
	AttrLabels     string    `gorm:"column:attr_labels;type:text" json:"attrLabels"`                                             // 属性标签
	AuditStatus    uint32    `gorm:"column:audit_status;default:0" json:"auditStatus"`                                           // 审核状态 1未审核 2审核中 3审核通过 4审核失败（不通过） 5人工审核中 6人工审核通过 7人工审核不通过
	AuditResult    string    `gorm:"column:audit_result" json:"auditResult"`                                                     // 审核结果信息
	IsAllowRelease bool      `gorm:"column:is_allow_release;default:0" json:"isAllowRelease"`                                    // 0不允许发布1允许发布
	ExpireTime     time.Time `gorm:"column:expire_time;default:'1970-01-01 08:00:00'" json:"expireTime"`                         // 有效期开始时间unix时间戳
	IsDeleted      uint32    `gorm:"column:is_deleted;default:0" json:"isDeleted"`                                               // 1未删除 2已删除
	CreateTime     time.Time `gorm:"column:create_time;default:CURRENT_TIMESTAMP" json:"createTime"`                             // 创建时间
	UpdateTime     time.Time `gorm:"column:update_time;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" json:"updateTime"` // 更新时间
}

// IsAuditDoing 是否审核中
func (r *ReleaseQaSimilarQuestion) IsAuditDoing() bool {
	if r == nil {
		return false
	}
	return r.AuditStatus == ReleaseQAAuditStatusDoing
}

type ReleaseQaSimilarQuestionFilter struct {
	ID               uint64
	CorpID           uint64
	VersionID        uint64
	RobotID          uint64
	RelatedQaID      uint64
	RelatedQaIDs     []uint64
	ReleaseStatusNot uint32
	SimilarID        uint64
	SimilarIDs       []uint64
	Question         string
	AuditStatus      *uint32
	AudiStatusList   []uint32
	IsAllowRelease   *int
	IsDeleted        *uint32

	ExtraCondition string
	ExtraArgs      []interface{}

	PageNo   int
	PageSize int

	OrderColumn    []string
	OrderDirection []string
}

type ReleaseQaSimilarQuestionState struct {
	AuditStatus uint32 `gorm:"column:audit_status;default:0" json:"auditStatus"`
	Total       uint64 `gorm:"column:total;default:0" json:"total"`
}

func (s *ReleaseQaSimilarQuestionState) TableName() string {
	return model.TableNameTReleaseQaSimilarQuestion
}
