package kb

import (
	"time"
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
