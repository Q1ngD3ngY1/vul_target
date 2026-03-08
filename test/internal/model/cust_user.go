package model

import "time"

const (
	//特殊权限配置 在用户角色绑定表的类型
	UserRolesNormal   = 0 //正常
	NotSetThirdUserId = 1 //没传第三方用户id兜底配置
	NotUseThirdUserId = 2 //传了第三方用户id匹配为空兜底设置

	//用户表
	UserTblColId          = "id"
	UserTblColCorpBizId   = "corp_biz_id"
	UserTblColAppBizId    = "app_biz_id"
	UserTblColBusinessId  = "business_id"
	UserTblColName        = "name"
	UserTblColThirdUserId = "third_user_id"
	UserTblColIsDeleted   = "is_deleted"  // 是否删除
	UserTblColCreateTime  = "create_time" // 创建时间
	UserTblColUpdateTime  = "update_time" // 更新时间

	//用户角色绑定表
	UserRoleTblColId          = "id"
	UserRoleTblColCorpBizId   = "corp_biz_id"
	UserRoleTblColAppBizId    = "app_biz_id"
	UserRoleTblColUserBizId   = "user_biz_id"
	UserRoleTblColType        = "type"
	UserRoleTblColThirdUserId = "third_user_id"
	UserRoleTblColRoleUserId  = "role_biz_id"
	UserRoleTblColIsDeleted   = "is_deleted"  // 是否删除
	UserRoleTblColCreateTime  = "create_time" // 创建时间
	UserRoleTblColUpdateTime  = "update_time" // 更新时间
)

// CustUser t_knowledge_user表结构
type CustUser struct {
	ID          uint64    `db:"id" gorm:"column:id;primaryKey;autoIncrement"`
	CorpID      uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`
	AppID       uint64    `db:"app_biz_id" gorm:"column:app_biz_id"`
	BusinessID  uint64    `db:"business_id" gorm:"column:business_id"`
	Name        string    `db:"name" gorm:"column:name"`
	ThirdUserID string    `db:"third_user_id" gorm:"column:third_user_id"` //第三方用户id
	IsDeleted   uint32    `db:"is_deleted" gorm:"column:is_deleted"`       //0正常，1已删除
	CreateTime  time.Time `db:"create_time" gorm:"column:create_time"`     //创建时间
	UpdateTime  time.Time `db:"update_time" gorm:"column:update_time"`     //更新时间
}

// TableName 设置表名
func (CustUser) TableName() string {
	return "t_knowledge_user"
}

// UserRole t_knowledge_user_role表结构
type UserRole struct {
	ID          uint64    `db:"id" gorm:"column:id;primaryKey;autoIncrement"`
	CorpID      uint64    `db:"corp_id" gorm:"column:corp_biz_id"`
	AppID       uint64    `db:"app_biz_id" gorm:"column:app_biz_id"`
	UserBizID   uint64    `db:"user_biz_id" gorm:"column:user_biz_id"`     //用户业务ID
	ThirdUserId string    `db:"third_user_id" gorm:"column:third_user_id"` //第三方用户id
	Type        uint32    `db:"type" gorm:"column:type"`                   //类型，0默认，1没传第三方用户id兜底配置，2传了匹配为空兜底设置
	RoleBizID   uint64    `db:"role_biz_id" gorm:"column:role_biz_id"`     //角色业务ID
	IsDeleted   uint32    `db:"is_deleted" gorm:"column:is_deleted"`       //0正常，1已删除
	CreateTime  time.Time `db:"create_time" gorm:"column:create_time"`     //创建时间
	UpdateTime  time.Time `db:"update_time" gorm:"column:update_time"`     //更新时间
}

// TableName 设置表名
func (UserRole) TableName() string {
	return "t_knowledge_user_role"
}

// CustUserConfigRedis 特殊权限配置 redis结构
type CustUserConfigRedis struct {
	NotSet string `json:"not_set"` //未传third_user_id时,配置的角色业务id
	NotUse string `json:"not_use"` //传了third_user_id但角色失效时,配置的角色业务id
}
