package qa

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	DocQaSimTblColID            = "id"
	DocQaSimTblColDocID         = "doc_id"
	DocQaSimTblColSimilarID     = "similar_id"
	DocQaSimTblColRobotID       = "robot_id"
	DocQaSimTblColCorpID        = "corp_id"
	DocQaSimTblColStaffID       = "staff_id"
	DocQaSimTblColCreateUserID  = "create_user_id"
	DocQaSimTblColRelatedQAID   = "related_qa_id"
	DocQaSimTblColSource        = "source"
	DocQaSimTblColMessage       = "message"
	DocQaSimTblColQuestion      = "question"
	DocQaSimTblColIsAuditFree   = "is_audit_free"
	DocQaSimTblColReleaseStatus = "release_status"
	DocQaSimTblColIsDeleted     = "is_deleted"
	DocQaSimTblColCreateTime    = "create_time"
	DocQaSimTblColUpdateTime    = "update_time"
	DocQaSimTblColNextAction    = "next_action"
	DocQaSimTblColCharSize      = "char_size"
	DocQaSimTblColQASize        = "qa_size"

	DocQaSimTblColIsValid = "is_valid"
	DocQaSimTblColStatus  = "status"
)

// SimilarQuestionSimple 相似问简要信息
type SimilarQuestionSimple struct {
	SimilarID   uint64 `db:"similar_id"`    // 相似问业务ID
	Question    string `db:"question"`      // 相似问的问题内容
	RelatedQAID uint64 `db:"related_qa_id"` // 相关联的主 QA ID
}

// SimilarQuestionCount 相似问count信息
type SimilarQuestionCount struct {
	RelatedQAID uint64 `db:"related_qa_id"` // 相关联的主 QA ID
	Total       uint32 `db:"total"`         // 相似问总数
}

// SimilarQuestion 相似问题
type SimilarQuestion struct {
	ID            uint64    `db:"id"`
	SimilarID     uint64    `db:"similar_id"`     // 相似问业务ID
	RobotID       uint64    `db:"robot_id"`       // 机器人ID
	CorpID        uint64    `db:"corp_id"`        // 企业ID
	StaffID       uint64    `db:"staff_id"`       // 员工ID
	CreateUserID  uint64    `db:"create_user_id"` // 上传用户ID
	RelatedQAID   uint64    `db:"related_qa_id"`  // 相关联的主 QA ID
	Source        uint32    `db:"source"`         // 来源(2 批量导入 3 手动添加)
	Question      string    `db:"question"`       // 相似问的问题内容
	Message       string    `db:"message"`        // 失败原因
	IsDeleted     uint32    `db:"is_deleted"`     // 1未删除 2已删除
	ReleaseStatus uint32    `db:"release_status"` // 发布状态(1 未发布 2 待发布 3 发布中 4 已发布 5 发布失败 6 不采纳 7 审核中 8 审核失败)
	IsAuditFree   bool      `db:"is_audit_free"`  // 免审 0 不免审（需要机器审核） 1 免审（无需机器审核）
	NextAction    uint32    `db:"next_action"`    // 面向发布操作：1新增 2修改 3删除 4发布
	CreateTime    time.Time `db:"create_time"`    // 创建时间
	UpdateTime    time.Time `db:"update_time"`    // 更新时间
	CharSize      uint64    `db:"char_size"`      // 相似问问题字符长度
	QaSize        uint64    `db:"qa_size"`        // 相似问问题字节大小
}

var SimilarQuestionTblColList = []string{
	DocQaSimTblColID,
	DocQaSimTblColSimilarID,
	DocQaSimTblColRobotID,
	DocQaSimTblColCorpID,
	DocQaSimTblColStaffID,
	DocQaSimTblColCreateUserID,
	DocQaSimTblColRelatedQAID,
	DocQaSimTblColSource,
	DocQaSimTblColMessage,
	DocQaSimTblColQuestion,
	DocQaSimTblColIsAuditFree,
	DocQaSimTblColReleaseStatus,
	DocQaSimTblColIsDeleted,
	DocQaSimTblColCreateTime,
	DocQaSimTblColUpdateTime,
	DocQaSimTblColNextAction,
	DocQaSimTblColCharSize,
	DocQaSimTblColQASize,
}

// SimilarQuestionModifyInfo 相似问修改信息
type SimilarQuestionModifyInfo struct {
	AddQuestions    []*SimilarQuestion // 新增相似问
	DeleteQuestions []*SimilarQuestion // 删除相似问
	UpdateQuestions []*SimilarQuestion // 更新相似问
}

// IsNextActionAdd 是否新增操作
func (d *SimilarQuestion) IsNextActionAdd() bool {
	if d == nil {
		return false
	}
	return d.NextAction == NextActionAdd
}

// IsDelete 是否已删除
func (d *SimilarQuestion) IsDelete() bool {
	if d == nil {
		return false
	}
	return d.IsDeleted == QAIsDeleted
}

type SimilarityQuestionReq struct {
	ID                 uint64
	IDs                []uint64
	IDLess             uint64
	IDMore             uint64
	StartMore          *time.Time
	EndLess            *time.Time
	RobotId            uint64
	RobotIDs           []uint64
	CorpId             uint64
	RelatedQAID        uint64
	RelatedQAIDs       []uint64
	SimilarQuestionID  uint64
	SimilarQuestionIDs []uint64
	ReleaseStatus      uint32
	ReleaseStatusList  []uint32
	ReleaseStatusNotIn []uint32
	IsDeleted          uint32
	Page               uint32
	PageSize           uint32
	IsRelease          bool
}

// NewSimilarQuestions 相似问将复用QA的大部分属性(除IsDeleted,NextAction,CharSize,Time)
func NewSimilarQuestions(ctx context.Context, qa *DocQA, questions []string) []*SimilarQuestion {
	sqs := make([]*SimilarQuestion, 0)
	if qa == nil || len(questions) == 0 {
		return sqs
	}
	for i := range questions {
		similarID := idgen.GetId()
		now := time.Now()
		question := strings.TrimSpace(questions[i])
		sq := &SimilarQuestion{
			SimilarID:     similarID,
			RobotID:       qa.RobotID,
			CorpID:        qa.CorpID,
			StaffID:       qa.StaffID,
			CreateUserID:  qa.StaffID,
			RelatedQAID:   qa.ID,
			Source:        qa.Source,
			Question:      question,
			ReleaseStatus: qa.ReleaseStatus,
			IsAuditFree:   qa.IsAuditFree,
			IsDeleted:     QAIsNotDeleted,
			NextAction:    NextActionAdd,
			CharSize:      uint64(utf8.RuneCountInString(question)),
			QaSize:        uint64(len(question)),
			CreateTime:    now,
			UpdateTime:    now,
		}
		sqs = append(sqs, sq)
	}

	return sqs
}

// NewSimilarQuestionsFromPB 从pb转换为model
func NewSimilarQuestionsFromPB(ctx context.Context, qa *DocQA, sqsPB []*pb.SimilarQuestion,
	nextAction uint32) []*SimilarQuestion {
	sqs := make([]*SimilarQuestion, 0)
	if qa == nil || len(sqsPB) == 0 {
		return sqs
	}
	releaseStatus := QAReleaseStatusInit
	isAuditFree := QAIsAuditNotFree
	isDeleted := QAIsNotDeleted
	if nextAction == NextActionDelete {
		isDeleted = QAIsDeleted
	}
	for _, q := range sqsPB {
		now := time.Now()
		question := strings.TrimSpace(q.Question)
		sq := &SimilarQuestion{
			SimilarID:     q.SimBizId,
			RobotID:       qa.RobotID,
			CorpID:        qa.CorpID,
			StaffID:       qa.StaffID,
			CreateUserID:  qa.StaffID,
			RelatedQAID:   qa.ID,
			Source:        qa.Source,
			Question:      question,
			ReleaseStatus: releaseStatus,
			IsAuditFree:   isAuditFree,
			IsDeleted:     isDeleted,
			NextAction:    nextAction,
			CharSize:      uint64(utf8.RuneCountInString(question)),
			QaSize:        uint64(len(question)),
			UpdateTime:    now,
		}
		sqs = append(sqs, sq)
	}

	return sqs

}

// NewSimilarQuestionsFromModifyReq 从修改请求中生成相似问
func NewSimilarQuestionsFromModifyReq(ctx context.Context, qa *DocQA,
	similarModify *pb.SimilarQuestionModify) (*SimilarQuestionModifyInfo, error) {
	if qa == nil {
		return nil, errors.New("qa is nil")
	}
	if similarModify == nil { // 当前修改没有涉及相似问
		return nil, nil
	}
	sqm := &SimilarQuestionModifyInfo{
		AddQuestions: NewSimilarQuestions(ctx, qa, similarModify.GetAddQuestions()),
		DeleteQuestions: NewSimilarQuestionsFromPB(ctx, qa, similarModify.GetDeleteQuestions(),
			NextActionDelete),
		UpdateQuestions: NewSimilarQuestionsFromPB(ctx, qa, similarModify.GetUpdateQuestions(), qa.NextAction),
	}

	return sqm, nil
}

// DocQAWithSimilar 文档问答对和相似问答对
type DocQAWithSimilar struct {
	DocQA        *DocQA
	DocSimilarQA []*SimilarQuestion
}

// IsReleaseNeedAudit 发布是否需要审核
func (d *SimilarQuestion) IsReleaseNeedAudit() bool {
	if d == nil {
		return false
	}
	return d.ReleaseAction() != NextActionDelete
}

// ReleaseAction 发布动作
func (d *SimilarQuestion) ReleaseAction() uint32 {
	if d == nil {
		return 0
	}
	return d.NextAction
}
