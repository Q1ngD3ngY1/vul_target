package model

import (
	"time"
)

const (
	// AppShareKGReleaseStatusInit 未发布
	AppShareKGReleaseStatusInit = uint32(1)
	// AppShareKGReleaseStatusIng 发布中
	AppShareKGReleaseStatusIng = uint32(2)
	// AppShareKGReleaseStatusSuccess 已发布
	AppShareKGReleaseStatusSuccess = uint32(3)
	// AppShareKGReleaseStatusFail 发布失败
	AppShareKGReleaseStatusFail = uint32(4)
)

// AppShareKnowledge 应用引用共享库表
type AppShareKnowledge struct {
	ID             uint64 `db:"id" gorm:"primaryKey;column:id"`
	AppBizID       uint64 `db:"app_biz_id" gorm:"column:app_biz_id"`             // 应用业务ID
	CorpBizID      uint64 `db:"corp_biz_id" gorm:"column:corp_biz_id"`           // 企业业务ID
	KnowledgeBizID uint64 `db:"knowledge_biz_id" gorm:"column:knowledge_biz_id"` // 共享库业务ID
	//ReleaseStatus  uint32    `db:"release_status" gorm:"column:release_status"`     // 发布状态(
	// 1 未发布 2 待发布 3 发布中 4 已发布 5 发布失败 6 不采纳 7 审核中 8 审核失败)
	//NextAction uint32    `db:"next_action"    gorm:"column:next_action"` // 面向发布操作：1新增 2修改 3删除 4发布
	//IsDeleted  int       `db:"is_deleted" gorm:"column:is_deleted"`      // 是否已删除
	UpdateTime time.Time `db:"update_time" gorm:"column:update_time"` // 更新时间
	CreateTime time.Time `db:"create_time" gorm:"column:create_time"` // 创建时间
}

// ShareKGAppListRsp 共享知识库引用应用列表响应
type ShareKGAppListRsp struct {
	AppBizID uint64 `json:"app_biz_id"`
	AppName  string `json:"app_name"`
}
