package model

import (
	"context"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"
)

type CateObjectType int

const (
	QACate CateObjectType = iota
	DocCate
	SynonymsCate
)

const (
	// AllCateID 全部分类 ID
	AllCateID = 0

	// AllCateName 全部分类名称
	AllCateName = i18nkey.KeyAllCategories
	// UncategorizedCateName 未分类名称
	UncategorizedCateName = "未分类"

	// CateIsNotDeleted 未删除
	CateIsNotDeleted = 0
	// CateIsDeleted 已删除
	CateIsDeleted = 1
)

// CateInfo 分类信息
type CateInfo struct {
	ID         uint64    `db:"id"`
	BusinessID uint64    `db:"business_id"` // 业务ID
	RobotID    uint64    `db:"robot_id"`
	CorpID     uint64    `db:"corp_id"`     // 企业ID
	Name       string    `db:"name"`        // 名称
	OrderNum   int       `db:"order_num"`   // 排序
	IsDeleted  int       `db:"is_deleted"`  // 0未删除 1已删除
	ParentID   uint64    `db:"parent_id"`   // 父级 ID
	CreateTime time.Time `db:"create_time"` // 创建时间
	UpdateTime time.Time `db:"update_time"` // 更新时间
}

// HasDeleted 是否已删除
func (c *CateInfo) HasDeleted() bool {
	if c == nil {
		return false
	}
	return c.IsDeleted == CateIsDeleted
}

// IsUncategorized 是否为未分类
func (c *CateInfo) IsUncategorized(ctx context.Context) bool {
	return (c.Name == UncategorizedCateName || c.Name == i18n.Translate(ctx, UncategorizedCateName)) && c.ParentID == AllCateID
}
