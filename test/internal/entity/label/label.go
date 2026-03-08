package label

import (
	"time"

	"git.woa.com/adp/kb/kb-config/internal/config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// LabelAble 可以带标签的结构
type LabelAble interface {
	// GetType 类型 文档 / QA
	GetType() LabelType
}

// LabelType 标签类型
type LabelType int

var labelNextActionMap = map[uint32]string{
	AttributeNextActionAdd:     "新增",
	AttributeNextActionUpdate:  "更新",
	AttributeNextActionDelete:  "删除",
	AttributeNextActionPublish: "发布",
}

// SystemLabelAttrKeys 系统标签
var SystemLabelAttrKeys = []string{
	"general_vector",
	"lke_category_key",
	"DocID",
	"_sys_str_qa_flag",
	"_sys_str_qa_id",
}

// Attribute 属性标签属性
type Attribute struct {
	ID            uint64    // preview环境的属性ID；prod环境该ID仅作为表的主键，无别的含义
	BusinessID    uint64    // 业务ID
	RobotID       uint64    // 机器人ID
	AttrKey       string    // 属性标识
	AttrID        uint64    // prod环境的属性ID；preview环境无此字段
	Name          string    // 属性名称
	IsUpdating    bool      // 是否更新中
	ReleaseStatus uint32    // 发布状态
	NextAction    uint32    // 最后操作
	IsDeleted     bool      // 0：未删除，1：已删除
	DeletedTime   int64     // 删除的时间，纳秒值，用于做唯一键处理
	CreateTime    time.Time // 创建时间
	UpdateTime    time.Time // 更新时间
}

// ActionDesc 操作行为描述
func (a *Attribute) ActionDesc() string {
	return labelNextActionMap[a.NextAction]
}

// AttributeProd 已发布标签
type AttributeProd struct {
	ID          uint64    `db:"id"`           // 主键ID
	AttrID      uint64    `db:"attr_id"`      // 标签ID
	BusinessID  uint64    `db:"business_id"`  // 标签业务ID
	RobotID     uint64    `db:"robot_id"`     // 应用ID
	AttrKey     string    `db:"attr_key"`     // 标签标识
	Name        string    `db:"name"`         // 标签名称
	IsDeleted   bool      `db:"is_deleted"`   // 是否删除
	DeletedTime int64     `db:"deleted_time"` // 删除的时间戳
	CreateTime  time.Time `db:"create_time"`  // 创建时间
	UpdateTime  time.Time `db:"update_time"`  // 更新时间
}

// AttributeLabelProd 已发布标签值
type AttributeLabelProd struct {
	ID           uint64    `db:"id"`            // 主键ID
	RobotID      uint64    `db:"robot_id"`      // 应用ID
	BusinessID   uint64    `db:"business_id"`   // 标签业务ID
	AttrID       uint64    `db:"attr_id"`       // 标签ID
	LabelID      uint64    `db:"label_id"`      // 标签值ID
	Name         string    `db:"name"`          // 标签值
	SimilarLabel string    `db:"similar_label"` // 标签同义词，json数组格式字符串
	IsDeleted    bool      `db:"is_deleted"`    // 是否删除
	CreateTime   time.Time `db:"create_time"`   // 创建时间
	UpdateTime   time.Time `db:"update_time"`   // 更新时间
}

type AttributeFilter struct {
	Ids            []uint64
	BusinessIds    []uint64
	RobotId        uint64
	NameSubStr     string
	IsDeleted      *bool
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
}

// AttributeKeyAndID 属性key和id
type AttributeKeyAndID struct {
	ID      uint64 // preview环境的属性ID；prod环境该ID仅作为表的主键，无别的含义
	AttrID  uint64 // prod环境的属性ID；preview环境无此字段
	AttrKey string // 属性标识
}

// AttributeLabel 属性标签
type AttributeLabel struct {
	ID            uint64    // preview环境的属性ID；prod环境该ID仅作为表的主键，无别的含义
	RobotID       uint64    // 机器人ID
	BusinessID    uint64    // 业务ID
	AttrID        uint64    // 属性ID
	LabelID       uint64    // prod环境的标签ID；preview环境无此字段
	Name          string    // 属性标签名称
	SimilarLabel  string    // 相似标签,JSON数组
	ReleaseStatus uint32    // 发布状态
	NextAction    uint32    // 最后操作
	IsDeleted     bool      // 0：未删除，1：已删除
	CreateTime    time.Time // 创建时间
	UpdateTime    time.Time // 更新时间
}

// GetName 获取标签名称
func (kl *AttributeLabel) GetName() string {
	if kl == nil {
		return ""
	}
	return kl.Name
}

// GetBusinessID 获取标签业务ID
func (kl *AttributeLabel) GetBusinessID() uint64 {
	if kl == nil {
		return 0
	}
	return kl.BusinessID
}

type AttributeLabelFilter struct {
	Ids                      []uint64
	RobotId                  uint64
	AttrId                   uint64
	AttrIds                  []uint64
	BusinessIds              []uint64
	NotEmptySimilarLabel     *bool
	NameOrSimilarLabelSubStr string
	Names                    []string
	IsDeleted                *bool
	Offset                   int
	Limit                    int
	OrderColumn              []string
	OrderDirection           []string
}

type DocAttributeLabelFilter struct {
	RobotId        uint64
	Source         uint64
	AttrIDs        []uint64
	LabelIDs       []uint64
	IsDeleted      *bool
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
}

// UpdateQAAttributeLabelReq 更新QA属性标签请求
type UpdateQAAttributeLabelReq struct {
	IsNeedChange    bool                `db:"is_need_change"`   // 是否需要改变
	AttributeLabels []*QAAttributeLabel `db:"attribute_labels"` // 引用
}

// DocAttributeLabel 文档属性标签
type DocAttributeLabel struct {
	ID         uint64    // ID
	RobotID    uint64    // 机器人ID
	DocID      uint64    // 文档
	Source     uint32    // 来源，1：属性标签
	AttrID     uint64    // 属性ID
	LabelID    uint64    // 标签ID
	IsDeleted  bool      // 0：未删除，1：已删除
	CreateTime time.Time // 创建时间
	UpdateTime time.Time // 更新时间
}

// QAAttributeLabel QA属性标签
type QAAttributeLabel struct {
	ID         uint64    `db:"id"`          // ID
	RobotID    uint64    `db:"robot_id"`    // 机器人ID
	QAID       uint64    `db:"qa_id"`       // QA
	Source     uint32    `db:"source"`      // 来源，1：属性标签
	AttrID     uint64    `db:"attr_id"`     // 属性ID
	LabelID    uint64    `db:"label_id"`    // 标签ID
	IsDeleted  uint32    `db:"is_deleted"`  // 0：未删除，1：已删除
	CreateTime time.Time `db:"create_time"` // 创建时间
	UpdateTime time.Time `db:"update_time"` // 更新时间
}

// AttributeLabelTask 属性标签任务
type AttributeLabelTask struct {
	ID            uint64    `db:"id"`              // ID
	CorpID        uint64    `db:"corp_id"`         // 企业ID
	RobotID       uint64    `db:"robot_id"`        // 机器人ID
	CreateStaffID uint64    `db:"create_staff_id"` // '员工ID
	Params        string    `db:"params"`          // 任务参数
	Status        uint32    `db:"status"`          // 任务状态(1 未启动 2 流程中 3 任务完成 4 任务失败)
	Message       string    `db:"message"`         // 任务信息
	CosURL        string    `db:"cos_url"`         // 任务信息
	UpdateTime    time.Time `db:"update_time"`     // 更新时间
	CreateTime    time.Time `db:"create_time"`     // 创建时间
}

type AttributeLabelRedisValue struct {
	BusinessID    uint64 `json:"business_id"`    // 业务ID
	Name          string `json:"name"`           // 主标签
	SimilarLabels string `json:"similar_labels"` // 相似标签列表,JSON数组
}

// AttrLabel 属性标签
type AttrLabel struct {
	Source     uint32   `json:"source"`      // 属性来源
	AttrID     uint64   `json:"attr_id"`     // 属性ID
	BusinessID uint64   `json:"business_id"` // 属性业务ID
	AttrKey    string   `json:"attr_key"`    // 属性标识
	AttrName   string   `json:"attr_name"`   // 属性名称
	Labels     []*Label `json:"labels"`      // 标签
}

// Label 属性标签
type Label struct {
	LabelID    uint64 `db:"label_id"`    // 标签ID
	BusinessID uint64 `db:"business_id"` // 标签业务ID
	LabelName  string `db:"label_name"`  // 属性标签名称
}

// UpdateAttributeLabelReq 更新知识属性标签请求
type UpdateAttributeLabelReq struct {
	IsNeedPublish     bool                       `db:"is_need_publish"`      // 是否需要同步
	PublishParams     AttributeLabelUpdateParams `db:"publish_params"`       // 推送参数
	Attr              *Attribute                 `db:"attr"`                 // 属性信息
	DeleteLabelIDs    []uint64                   `db:"delete_label_ids"`     // 需要删除的标签信息
	DeleteLabelBizIDs []uint64                   `db:"delete_label_biz_ids"` // 需要删除的标签信息
	AddLabels         []*AttributeLabel          `db:"add_labels"`           // 需要添加的标签信息
	UpdateLabels      []*AttributeLabel          `db:"update_labels"`        // 需要更新的标签信息
}

// AttributeLabelUpdateParams 属性标签更新任务参数
type AttributeLabelUpdateParams struct {
	Name     string   `json:"name"`      // 任务名称
	CorpID   uint64   `json:"corp_id"`   // 企业ID
	StaffID  uint64   `json:"staff_id"`  // 员工ID
	RobotID  uint64   `json:"robot_id"`  // 机器人ID
	AttrID   uint64   `json:"attr_id"`   // 属性标签属性ID
	LabelIDs []uint64 `json:"label_ids"` // 属性标签ID
	TaskID   uint64   `json:"task_id"`   // 任务ID
	Language string   `json:"language"`  // 国际化语言
}

// AttributeLabelItem 创建属性标签
type AttributeLabelItem struct {
	Attr   *Attribute        `db:"attr"`   // 属性信息
	Labels []*AttributeLabel `db:"labels"` // 需要添加的标签信息
}

// UpdateDocAttributeLabelReq 更新文档属性标签请求
type UpdateDocAttributeLabelReq struct {
	IsNeedChange    bool                 `db:"is_need_change"`   // 是否需要改变
	AttributeLabels []*DocAttributeLabel `db:"attribute_labels"` // 引用
}

// LabelRelationer 内容标签关联关系
type LabelRelationer interface {
	GetType() LabelType
	GetID() uint64
	GetRelatedID() uint64
	GetAttrID() AttributeID
	GetLabelID() AttributeLabelID
}

// GetStatusString 获取标签任务状态
func (attributeLabelTask *AttributeLabelTask) GetStatusString() string {
	switch attributeLabelTask.Status {
	case AttributeLabelTaskStatusSuccess:
		return pb.TaskStatus_SUCCESS.String()
	case AttributeLabelTaskStatusPending:
		return pb.TaskStatus_PENDING.String()
	case AttributeLabelTaskStatusRunning:
		return pb.TaskStatus_RUNNING.String()
	case AttributeLabelTaskStatusFailed:
		return pb.TaskStatus_FAILED.String()
	default:
		return ""
	}
}

// AttributeID .
type AttributeID uint64

// AttributeLabelID .
type AttributeLabelID uint64

// DocAttributeLabelID .
type DocAttributeLabelID uint64

// QaAttributeLabelID .
type QaAttributeLabelID uint64

var (
	_ LabelRelationer = (*DocAttributeLabel)(nil)
	_ LabelRelationer = (*QAAttributeLabel)(nil)
)

// IsAttributeLabelReferSource 是否合法的引用来源
func IsAttributeLabelReferSource(source uint32) bool {
	switch source {
	case AttributeLabelSourceKg:
		return true
	default:
		return false
	}
}

// AttrLabelAndSimilarLabels 属性标签和相似标签
type AttrLabelAndSimilarLabels struct {
	BusinessID    uint64   `json:"business_id"`    // 业务ID
	Name          string   `json:"name"`           // 主标签
	SimilarLabels []string `json:"similar_labels"` // 相似标签列表
}

// LabelAble 可以带标签的结构
// type LabelAble interface {
//	// GetType 类型 文档 / QA
//	GetType() attrEntity.LabelType
// }

// Attributes 属性标签
type Attributes map[AttributeID]AttributeLabelItem

// AttrKeyNamePair 属性标识名称匹配对
type AttrKeyNamePair struct {
	Row      int    `json:"row"`       // 行数
	AttrKey  string `json:"attr_key"`  // 属性标识
	AttrName string `json:"attr_name"` // 属性名称
}

// FillVectorLabels 转成成VectorLabel
func FillVectorLabels(attrLabels []*AttrLabel) []*retrieval.VectorLabel {
	list := make([]*retrieval.VectorLabel, 0)
	for _, v := range attrLabels {
		for _, label := range v.Labels {
			list = append(list, &retrieval.VectorLabel{
				Name:  v.AttrKey,
				Value: label.LabelName,
			})
		}
	}
	return list
}

// GetType 类型
func (d *DocAttributeLabel) GetType() LabelType {
	return AttributeLabelTypeDOC
}

// GetID .
func (d *DocAttributeLabel) GetID() uint64 {
	return d.ID
}

// GetRelatedID 获取内容关联的ID, 问答为 QaID, 文档为 DocID
func (d *DocAttributeLabel) GetRelatedID() uint64 {
	return d.DocID
}

// GetAttrID 获取属性ID
func (d *DocAttributeLabel) GetAttrID() AttributeID {
	return AttributeID(d.AttrID)
}

// GetLabelID 获取标签ID
func (d *DocAttributeLabel) GetLabelID() AttributeLabelID {
	return AttributeLabelID(d.LabelID)
}

// HasDeleted 是否已经删除
func (d *DocAttributeLabel) HasDeleted() bool {
	return d.IsDeleted
}

// GetType 类型
func (q *QAAttributeLabel) GetType() LabelType {
	return AttributeLabelTypeQA
}

// GetID .
func (q *QAAttributeLabel) GetID() uint64 {
	return q.ID
}

// GetRelatedID 获取内容关联的ID, 问答为 QaID, 文档为 DocID
func (q *QAAttributeLabel) GetRelatedID() uint64 {
	return q.QAID
}

// GetAttrID 获取属性ID
func (q *QAAttributeLabel) GetAttrID() AttributeID {
	return AttributeID(q.AttrID)
}

// GetLabelID 获取标签ID
func (q *QAAttributeLabel) GetLabelID() AttributeLabelID {
	return AttributeLabelID(q.LabelID)
}

// ToVectorLabels .
func (a Attributes) ToVectorLabels() []*retrieval.VectorLabel {
	var l []*retrieval.VectorLabel
	if len(a) == 0 {
		l = append(l, &retrieval.VectorLabel{
			Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
			Value: config.App().AttributeLabel.FullLabelValue,
		})
		return l
	}
	for _, item := range a {
		if len(item.Labels) == 0 {
			l = append(l, &retrieval.VectorLabel{
				Name:  item.Attr.AttrKey,
				Value: config.App().AttributeLabel.FullLabelValue,
			})
		} else {
			for _, v := range item.Labels {
				l = append(l, &retrieval.VectorLabel{
					Name:  item.Attr.AttrKey,
					Value: v.Name,
				})
			}
		}
	}
	return l
}

// StatusDesc 状态描述
func (a *Attribute) StatusDesc() string {
	return releaseStatusDesc[a.ReleaseStatus]
}

// CustomLabelConfig 自定义标签检索配置
type CustomLabelConfig struct {
	LabelLogicOpr       string             // AND或OR (标签检索条件)
	IsLabelOrGeneral    bool               // 标签检索是否OR的形式检索default标签 默认false
	IsTransAPIParam     bool               // 是否转换API映射参数 默认false【废弃，暂时没有用】
	IsTransSimilarLabel bool               // 是否转换相似标签 默认false【废弃，暂时没有用】
	SearchStrategy      *pb.SearchStrategy // 工作流知识检索策略配置
}

type ModifyAttributeLabelRsp struct {
	TaskId            uint64
	BusinessID        uint64
	DeleteLabelBizIDs []uint64
	AddLabels         []*AttributeLabel
	UpdateLabels      []*AttributeLabel
}
