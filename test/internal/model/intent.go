package model

import (
	"time"
)

const (
	// IntentIsValid 意图未删除
	IntentIsValid = 0
	// IntentIsInvalid 意图已删除
	IntentIsInvalid = 1
	// IntentNotUsed 意图未使用
	IntentNotUsed = 0
	// IntentIsUsed 意图使用中
	IntentIsUsed = 1

	// IntentKnowledgeQaCate 知识问答分类
	IntentKnowledgeQaCate = "knowledge_qa"
	// IntentSearchEngineCate 搜索引擎分类
	IntentSearchEngineCate = "search_engine"
	// IntentModelChatCate 模型闲聊分类
	IntentModelChatCate = "model_chat"
	// IntentDocSummaryCate 文档摘要分类
	IntentDocSummaryCate = "doc_summary"
	// IntentSelfAwarenessCate 自我认知分类
	IntentSelfAwarenessCate = "self_awareness"
)

// Intent 意图
type Intent struct {
	ID         int       `db:"id"`
	PolicyID   uint32    `db:"policy_id"`   // 意图策略
	Category   string    `db:"category"`    // 意图分类
	Name       string    `db:"name"`        // 意图名称
	IsDeleted  int8      `db:"is_deleted"`  // 是否删除
	IsUsed     int8      `db:"is_used"`     // 是否使用
	Operator   string    `db:"operator"`    // 数据操作人
	UpdateTime time.Time `db:"update_time"` // 更新时间
	CreateTime time.Time `db:"create_time"` // 创建时间
}

// ListIntentReq 获取意图列表请求
type ListIntentReq struct {
	Name       string // 意图名称
	IsDeleted  int8   // 是否删除
	IsUsed     int8   // 是否使用
	UsedFilter bool   // 是否筛选了使用状态
	Page       uint32 // 页码
	PageSize   uint32 // 页面大小
}

// Condition 意图列表过滤参数
type Condition struct {
	condition string
	args      []interface{}
}

func (c *Condition) addCondition(format string, values ...interface{}) {
	if len(format) > 0 {
		c.condition += " " + format
		c.args = append(c.args, values...)
	}
}

// Where 拼接where
func (c *Condition) Where(format string, values ...interface{}) {
	c.addCondition("WHERE "+format, values...)
}

// And 拼接And
func (c *Condition) And(format string, values ...interface{}) {
	c.addCondition("AND "+format, values...)
}

// Condition return Condition.condition
func (c *Condition) Condition() string {
	return c.condition
}

// Args return Condition.args
func (c *Condition) Args() []interface{} {
	return c.args
}

// WithCondition 携带condition
func (c *Condition) WithCondition(format string, values ...interface{}) {
	if len(c.condition) > 0 {
		c.And(format, values...)
	} else {
		c.Where(format, values...)
	}
}
