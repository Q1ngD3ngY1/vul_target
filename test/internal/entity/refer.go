package entity

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/gox"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	similar "git.woa.com/dialogue-platform/llm/sdk/query-similarity"
)

const (
	QATypeStandard = 0 // 标准问
	QATypeSimilar  = 1 // 相似问

	// MarkInit 初始化标签
	MarkInit = 0
	// MarkRight 正确标签
	MarkRight = 1
	// MarkError 错误标签
	MarkError = 2

	// ReferTypeQA QA
	ReferTypeQA = 1
	// ReferTypeSegment 文档段
	ReferTypeSegment = 2
	// ReferTypeDoc 文档
	ReferTypeDoc = 3

	// SessionTypeGreeting 访客端
	SessionTypeGreeting uint32 = 1
	// SessionTypeExperience 评测端
	SessionTypeExperience uint32 = 2

	// DataTypeDoc 文档片段
	DataTypeDoc string = "DOC"
	// DataTypeQA 问答
	DataTypeQA string = "QA"
)

// Refer 来源表
type Refer struct {
	ID          uint64    `db:"id"`
	BusinessID  uint64    `db:"business_id"`  // 对外ID
	RobotID     uint64    `db:"robot_id"`     // 机器人ID
	MsgID       string    `db:"msg_id"`       // 消息ID
	SessionType uint32    `db:"session_type"` // 会话类型（1 访客端 2 评测端）
	RawQuestion string    `db:"raw_question"` // 原始问题
	DocID       uint64    `db:"doc_id"`       // 文档ID
	DocType     uint32    `db:"doc_type"`     // 文档类型 (1 QA, 2 文档段)
	RelateID    uint64    `db:"relate_id"`    // 关联ID
	Confidence  float32   `db:"confidence"`   // 置信度
	RougeScore  string    `db:"rouge_score"`  // Rouge 分数
	Question    string    `db:"question"`     // 问题
	Answer      string    `db:"answer"`       // 答案
	OrgData     string    `db:"org_data"`     // 原始内容
	Mark        uint32    `db:"mark"`         // 标签值
	PageInfos   string    `db:"page_infos"`   // 页码信息（json存储）
	SheetInfos  string    `db:"sheet_infos"`  // sheet信息（json存储)
	UpdateTime  time.Time `db:"update_time"`  // 更新时间
	CreateTime  time.Time `db:"create_time"`  // 创建时间
	IsBigData   bool      `db:"-"`            // 是否big_data true-表示org_data是由big_data填充 当文档类型为 文档段(2) 时有效 不记录到DB，获取页码信息时使用
}

// HighlightRefer 引用高亮标记
func HighlightRefer(ctx context.Context, query string, pageContent string, threshold int) []*pb.Highlight {
	var highlightRes []*pb.Highlight
	if len(pageContent) <= 0 || len(query) <= 0 {
		log.WarnContext(ctx, "pageContent or query is empty")
		return nil
	}
	// 上面做了对应情况的判断 故此处忽略err
	highlights, _ := similar.GetHighlightsWithSlidingWindow(query, pageContent, threshold)
	for _, highlight := range highlights {
		highlightRes = append(highlightRes, &pb.Highlight{
			StartPos: uint64(highlight.StartPos),
			EndPos:   uint64(highlight.EndPos),
			Text:     highlight.Text,
		})
	}
	return highlightRes
}

// NewRefer 构造参考来源
func NewRefer(doc *pb.MatchReferReq_Doc, req *pb.MatchReferReq, robotID, bizID uint64) Refer {
	now := time.Now()
	return Refer{
		BusinessID:  bizID,
		RobotID:     robotID,
		MsgID:       req.GetMsgId(),
		RawQuestion: req.GetQuestion(),
		DocID:       doc.GetDocId(),
		DocType:     doc.GetDocType(),
		RelateID:    doc.GetRelatedId(),
		Question:    doc.GetQuestion(),
		Answer:      gox.IfElse(doc.GetDocType() == DocTypeSegment, req.GetAnswer(), doc.GetAnswer()),
		OrgData:     doc.GetOrgData(),
		Mark:        MarkInit,
		SheetInfos:  doc.GetSheetInfo(), // text2Sql的没有RelateID，sheet信息由这里填充
		SessionType: gox.IfElse(req.GetIsRelease(), SessionTypeGreeting, SessionTypeExperience),
		UpdateTime:  now,
		CreateTime:  now,
		IsBigData:   doc.GetIsBigData(),
	}
}

type ReferFilter struct {
	ID          uint64
	BusinessID  uint64
	BusinessIDs []uint64
	RobotID     uint64
}
