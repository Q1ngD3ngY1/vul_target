package model

// 全局知识库干预

import (
	"time"
)

const (
	// GlobalKnowledgeGroupName 检索库名称
	GlobalKnowledgeGroupName = "global"
	// SearchGlobalFilterKey 检索库过滤器key
	SearchGlobalFilterKey = "search_global"
	// GlobalKnowledgeIsNotDeleted 未删除
	GlobalKnowledgeIsNotDeleted = 0
)

// GlobalKnowledgeID 全局知识库ID
type GlobalKnowledgeID uint64

// GlobalKnowledge 全局知识库
type GlobalKnowledge struct {
	ID         GlobalKnowledgeID `db:"id"`          // ID
	Question   string            `db:"question"`    // 问题
	Answer     string            `db:"answer"`      // 答案
	IsSync     bool              `db:"is_sync"`     // 0 未同步, 1 已同步
	IsDeleted  bool              `db:"is_deleted"`  // 0 未删除, 1 已删除
	CreateTime time.Time         `db:"create_time"` // 创建时间
	UpdateTime time.Time         `db:"update_time"` // 更新时间
}
