package model

import "time"

type KnowledgeSchemaTask struct {
	Id         uint64    `db:"id" gorm:"column:id"`
	CorpBizId  uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`                                                       // 企业业务ID
	AppBizId   uint64    `db:"app_biz_id" gorm:"column:app_biz_id"`                                                         // 应用业务ID
	BusinessID uint64    `db:"business_id" gorm:"column:business_id"`                                                       // 业务ID
	Status     uint32    `db:"status" gorm:"column:status;default:0"`                                                       // 状态(0待处理 1处理中 2处理成功 3处理失败,4处理中止)
	StatusCode uint32    `db:"status_code" gorm:"column:status_code;default:0"`                                             // 任务状态码，表示任务处于当前状态的具体原因
	Message    string    `db:"message" gorm:"column:message"`                                                               // 失败原因
	IsDeleted  int8      `db:"is_deleted" gorm:"column:is_deleted;default:0"`                                               // 是否删除
	CreateTime time.Time `db:"create_time" gorm:"column:create_time;default:CURRENT_TIMESTAMP"`                             // 创建时间
	UpdateTime time.Time `db:"update_time" gorm:"column:update_time;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
}
