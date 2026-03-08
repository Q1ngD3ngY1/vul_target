package model

import "time"

const (
	//应用配置状态
	NotKnowledge = 1 //知识库无内容
	NotAppUser   = 2 //知识库有内容，未单独配置权限
	HasAppUser   = 3 //知识库有内容，已单独配置权限
	HasThirdAcl  = 4 //知识库有内容，且对接了外部系统时

	EmptyEmbeddingModelName = ""
)

type KnowledgeConfig struct {
	ID             uint64    `db:"id" gorm:"column:id;primaryKey;autoIncrement"`
	CorpBizID      uint64    `db:"corp_biz_id" gorm:"column:corp_biz_id"`
	KnowledgeBizId uint64    `db:"knowledge_biz_id" gorm:"column:knowledge_biz_id"` //知识库业务id
	Type           uint32    `db:"type" gorm:"column:type"`                         //类型，0第三方权限接口配置
	Config         string    `db:"config" gorm:"column:config"`
	IsDeleted      uint32    `db:"is_deleted" gorm:"column:is_deleted"`   //0正常，1已删除
	CreateTime     time.Time `db:"create_time" gorm:"column:create_time"` //创建时间
	UpdateTime     time.Time `db:"update_time" gorm:"column:update_time"` //更新时间
}

// TableName 设置表名
func (KnowledgeConfig) TableName() string {
	return "t_knowledge_config"
}

// ThirdAclConfig 第三方权限配置
type ThirdAclConfig struct {
	Type                uint32 `json:"type"`
	ThirdToken          string `json:"third_token"`
	CheckPermissionsUrl string `json:"check_permissions_url"`
}

// EmbeddingModelUpdateInfo embedding模型变更信息
type EmbeddingModelUpdateInfo struct {
	OldModelName    string `json:"old_model_name"`
	OldModelVersion uint64 `json:"old_model_version"`
	NewModelName    string `json:"new_model_name"`
	NewModelVersion uint64 `json:"new_model_version"`
}
