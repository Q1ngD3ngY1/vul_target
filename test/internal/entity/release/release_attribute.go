package release

import "time"

// 发布标签说明：V2.6版本迭代变更背景
// 原有属性重新定义为标签、原有属性标签重新定义为标签值，原有标签相似值重新定义为标签同义词

// 标签发布状态
const (
	// LabelReleaseStatusInit 待发布
	LabelReleaseStatusInit = uint32(1)
	// LabelReleaseStatusIng 发布中
	LabelReleaseStatusIng = uint32(2)
	// LabelReleaseStatusSuccess 发布成功
	LabelReleaseStatusSuccess = uint32(3)
	// LabelReleaseStatusFail 发布失败
	LabelReleaseStatusFail = uint32(4)
	// LabelReleaseStatusUpdating 更新中
	LabelReleaseStatusUpdating = uint32(5)
)

// 标签操作行为
const (
	// LabelNextActionAdd 新建
	LabelNextActionAdd = uint32(1)
	// LabelNextActionUpdate 修改
	LabelNextActionUpdate = uint32(2)
	// LabelNextActionDelete 删除
	LabelNextActionDelete = uint32(3)
	// LabelNextActionPublish 发布
	LabelNextActionPublish = uint32(4)
)

var labelNextActionMap = map[uint32]string{
	LabelNextActionAdd:     "新增",
	LabelNextActionUpdate:  "更新",
	LabelNextActionDelete:  "删除",
	LabelNextActionPublish: "发布",
}

// ReleaseAttribute 发布标签
type ReleaseAttribute struct {
	ID            uint64    `db:"id"`             // 主键ID
	BusinessID    uint64    `db:"business_id"`    // 标签业务ID
	RobotID       uint64    `db:"robot_id"`       // 应用ID
	VersionID     uint64    `db:"version_id"`     // 发布版本ID
	AttrID        uint64    `db:"attr_id"`        // 标签ID
	AttrKey       string    `db:"attr_key"`       // 标签标识
	Name          string    `db:"name"`           // 标签名称
	Message       string    `db:"message"`        // 失败原因
	ReleaseStatus uint32    `db:"release_status"` // 发布状态
	Action        uint32    `db:"action"`         // 操作行为
	IsDeleted     bool      `db:"is_deleted"`     // 是否删除
	DeletedTime   int64     `db:"deleted_time"`   // 删除的时间戳
	CreateTime    time.Time `db:"create_time"`    // 创建时间
	UpdateTime    time.Time `db:"update_time"`    // 更新时间
}

// ReleaseAttributeLabel 发布标签值
type ReleaseAttributeLabel struct {
	ID            uint64    `db:"id"`             // 主键ID
	BusinessID    uint64    `db:"business_id"`    // 标签业务ID
	VersionID     uint64    `db:"version_id"`     // 发布版本ID
	RobotID       uint64    `db:"robot_id"`       // 应用ID
	AttrID        uint64    `db:"attr_id"`        // 标签ID
	LabelID       uint64    `db:"label_id"`       // 标签值ID
	Name          string    `db:"name"`           // 标签值
	SimilarLabel  string    `db:"similar_label"`  // 标签同义词，json数组格式字符串
	Message       string    `db:"message"`        // 失败原因
	ReleaseStatus uint32    `db:"release_status"` // 发布状态
	Action        uint32    `db:"action"`         // 操作行为
	IsDeleted     bool      `db:"is_deleted"`     // 是否删除
	CreateTime    time.Time `db:"create_time"`    // 创建时间
	UpdateTime    time.Time `db:"update_time"`    // 更新时间
}

// ReleaseLabelDetail 发布标签详情
type ReleaseLabelDetail struct {
	Label       *ReleaseAttribute        // 标签信息
	LabelValues []*ReleaseAttributeLabel // 标签值信息
}

type ReleaseArrtibuteFilter struct {
	RobotID   uint64
	VersionID uint64
	AttrID    uint64
}

type ReleaseArrtibuteLabelFilter struct {
	RobotID   uint64
	VersionID uint64
	AttrID    uint64
}

// ActionDesc 操作行为描述
func (ra *ReleaseAttribute) ActionDesc() string {
	return labelNextActionMap[ra.Action]
}
