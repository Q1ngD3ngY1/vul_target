package vector

import "time"

const (
	retrievalNodeInfoTable = "t_retrieval_node_info"

	NodeTblColId           = "id"            // ID
	NodeTblColRobotId      = "robot_id"      // 机器人ID
	NodeTblColDocType      = "doc_type"      // 类型: 1 QA, 2 DocSegment
	NodeTblColRelatedId    = "related_id"    // 关联id
	NodeTblColDocId        = "doc_id"        // 文档ID
	NodeTblColParentId     = "parent_id"     // 上一层的ID
	NodeTblColSegmentType  = "segment_type"  // 文档切片类型
	NodeTblColPageContent  = "page_content"  // 内容
	NodeTblColOrgData      = "org_data"      // 原始内容
	NodeTblColBigDataId    = "big_data_id"   // big_data标识id
	NodeTblColQuestion     = "question"      // 问题
	NodeTblColAnswer       = "answer"        // 答案
	NodeTblColCustomParam  = "custom_param"  // 自定义参数
	NodeTblColQuestionDesc = "question_desc" // 问题描述
	NodeTblColLabels       = "labels"        // 标签
	NodeTblColReserve1     = "reserve1"      // 预留字段1
	NodeTblColReserve2     = "reserve2"      // 预留字段2
	NodeTblColReserve3     = "reserve3"      // 预留字段3
	NodeTblColIsDeleted    = "is_deleted"    // 删除标记
	NodeTblColCreateTime   = "create_time"   // 创建时间
	NodeTblColUpdateTime   = "update_time"   // 更新时间
	NodeTblColExpireTime   = "expire_time"   // 过期时间
)

const (
	imageVectorTable = "t_image_vector"
)

// ImageVector 切片图片向量存储
type ImageVector struct {
	ID                 uint64    `gorm:"column:id"` // 自增ID
	ImageID            uint64    `gorm:"column:image_id"`
	RobotID            uint64    `gorm:"column:robot_id"`
	Content            string    `gorm:"column:content"`                                     // 内容,图片链接
	EmbeddingVersionID uint64    `gorm:"column:embedding_version_id"`                        // embedding 版本
	VectorRaw          []byte    `gorm:"column:vector"`                                      // 向量
	IsDeleted          uint32    `gorm:"column:is_deleted"`                                  // 是否删除(0未删除 1已删除）
	UpdateTime         time.Time `gorm:"column:update_time;type:datetime(0);autoUpdateTime"` // 更新时间
	CreateTime         time.Time `gorm:"column:create_time;type:datetime(0);autoCreateTime"` // 响应时间
}

// TableName 切片图片向量存储
func (ImageVector) TableName() string {
	return "t_image_vector"
}
