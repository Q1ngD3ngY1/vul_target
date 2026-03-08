package model

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"

	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

const (
	// AttributeLabelNoticeContent = "知识标签批量导出成功。"
	AttributeLabelNoticeContent = i18nkey.KeyKnowledgeTagsBatchExportStatus
	// AttributeLabelNoticeContentIng = "知识标签批量导出中。"
	AttributeLabelNoticeContentIng = i18nkey.KeyKnowledgeTagsBatchExporting

	// AttributeLabelUpdatingNoticeContent 知识标签更新中通知内容
	AttributeLabelUpdatingNoticeContent = i18nkey.KeyKnowledgeTagUpdating
	// AttributeLabelUpdateSuccessNoticeContent 知识标签更新成功通知内容
	AttributeLabelUpdateSuccessNoticeContent = "%s知识标签更新成功"
	// AttributeLabelUpdateFailNoticeContent 知识标签更新失败通知内容
	AttributeLabelUpdateFailNoticeContent = i18nkey.KeyKnowledgeTagUpdateFailed
)

const (
	// AttributeIsNotDeleted 属性标签属性未删除
	AttributeIsNotDeleted = 0
	// AttributeDeleted 属性标签属性已删除
	AttributeDeleted = 1

	// AttributeLabelIsNotDeleted 属性标签未删除
	AttributeLabelIsNotDeleted = 0
	// AttributeLabelDeleted 属性标签已删除
	AttributeLabelDeleted = 1

	// DocAttributeLabelIsNotDeleted 文档属性标签未删除
	DocAttributeLabelIsNotDeleted = 0
	// DocAttributeLabelDeleted 文档属性标签已删除
	DocAttributeLabelDeleted = 1

	// QAAttributeLabelIsNotDeleted QA属性标签未删除
	QAAttributeLabelIsNotDeleted = 0
	// QAAttributeLabelDeleted QA属性标签已删除
	QAAttributeLabelDeleted = 1

	// AttributeLabelSourceKg 引用来源-知识标签属性标签
	AttributeLabelSourceKg = 1

	// ExcelTplAttrNameIndex 导入文档索引，属性名称列（必填）
	ExcelTplAttrNameIndex = 0
	// ExcelTplLabelIndex 导入文档索引，标签列（必填）
	ExcelTplLabelIndex = 1
	// ExcelTplSimilarLabelIndex 导入文档索引，相似标签列（可选）
	ExcelTplSimilarLabelIndex = 2

	// AttributeLabelTypeUnknown 未知
	AttributeLabelTypeUnknown LabelType = 0
	// AttributeLabelTypeDOC 文档类型
	AttributeLabelTypeDOC LabelType = 1
	// AttributeLabelTypeQA QA类型
	AttributeLabelTypeQA LabelType = 2

	// AttributeLabelTaskStatusPending 未启动
	AttributeLabelTaskStatusPending = 1
	// AttributeLabelTaskStatusRunning 流程中
	AttributeLabelTaskStatusRunning = 2
	// AttributeLabelTaskStatusSuccess 任务成功
	AttributeLabelTaskStatusSuccess = 3
	// AttributeLabelTaskStatusFailed 任务失败
	AttributeLabelTaskStatusFailed = 4

	// AttributeLabelsPreview 更新预览环境属性&标签
	AttributeLabelsPreview = "preview"
	// AttributeLabelsProd 更新发布环境属性&标签
	AttributeLabelsProd = "prod"
)

const (
	// AttributeNextActionAdd 新增
	AttributeNextActionAdd = uint32(1)
	// AttributeNextActionUpdate 更新
	AttributeNextActionUpdate = uint32(2)
	// AttributeNextActionDelete 删除
	AttributeNextActionDelete = uint32(3)
	// AttributeNextActionPublish 发布
	AttributeNextActionPublish = uint32(4)
	// AttributeStatusUnknown 未知
	AttributeStatusUnknown = 0
	// AttributeStatusWaitRelease 等待发布
	AttributeStatusWaitRelease = 1
	// AttributeStatusReleasing 发布中
	AttributeStatusReleasing = 2
	// AttributeStatusReleased 已发布
	AttributeStatusReleased = 3
	// AttributeStatusReleaseFail 发布失败
	AttributeStatusReleaseFail = 4
	// AttributeStatusReleaseUpdating 发布更新中
	AttributeStatusReleaseUpdating = 5
)

var releaseStatusDesc = map[uint32]string{
	AttributeStatusWaitRelease:     i18nkey.KeyWaitRelease,
	AttributeStatusReleasing:       i18nkey.KeyReleasing,
	AttributeStatusReleased:        i18nkey.KeyReleaseSuccess,
	AttributeStatusReleaseFail:     i18nkey.KeyPublishingFailed,
	AttributeStatusReleaseUpdating: i18nkey.KeyUpdating,
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

// LabelType 标签类型
type LabelType int

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

// Attribute 属性标签属性
type Attribute struct {
	ID            uint64    `db:"id"`             // preview环境的属性ID；prod环境该ID仅作为表的主键，无别的含义
	BusinessID    uint64    `db:"business_id"`    // 业务ID
	RobotID       uint64    `db:"robot_id"`       // 机器人ID
	AttrKey       string    `db:"attr_key"`       // 属性标识
	AttrID        uint64    `db:"attr_id"`        // prod环境的属性ID；preview环境无此字段
	Name          string    `db:"name"`           // 属性名称
	IsUpdating    bool      `db:"is_updating"`    // 是否更新中
	ReleaseStatus uint32    `db:"release_status"` // 发布状态
	NextAction    uint32    `db:"next_action"`    // 最后操作
	IsDeleted     uint32    `db:"is_deleted"`     // 0：未删除，1：已删除
	DeletedTime   int64     `db:"deleted_time"`   // 删除的时间，纳秒值，用于做唯一键处理
	CreateTime    time.Time `db:"create_time"`    // 创建时间
	UpdateTime    time.Time `db:"update_time"`    // 更新时间
}

// AttributeKeyAndID 属性key和id
type AttributeKeyAndID struct {
	ID      uint64 `db:"id"`       // preview环境的属性ID；prod环境该ID仅作为表的主键，无别的含义
	AttrID  uint64 `db:"attr_id"`  // prod环境的属性ID；preview环境无此字段
	AttrKey string `db:"attr_key"` // 属性标识
}

// AttributeLabel 属性标签
type AttributeLabel struct {
	ID            uint64    `db:"id"`             // preview环境的属性ID；prod环境该ID仅作为表的主键，无别的含义
	RobotID       uint64    `db:"robot_id"`       // 机器人ID
	BusinessID    uint64    `db:"business_id"`    // 业务ID
	AttrID        uint64    `db:"attr_id"`        // 属性ID
	LabelID       uint64    `db:"label_id"`       // prod环境的标签ID；preview环境无此字段
	Name          string    `db:"name"`           // 属性标签名称
	SimilarLabel  string    `db:"similar_label"`  // 相似标签,JSON数组
	ReleaseStatus uint32    `db:"release_status"` // 发布状态
	NextAction    uint32    `db:"next_action"`    // 最后操作
	IsDeleted     uint32    `db:"is_deleted"`     // 0：未删除，1：已删除
	CreateTime    time.Time `db:"create_time"`    // 创建时间
	UpdateTime    time.Time `db:"update_time"`    // 更新时间
}

type AttributeLabelRedisValue struct {
	BusinessID    uint64 `json:"business_id"`    // 业务ID
	Name          string `json:"name"`           // 主标签
	SimilarLabels string `json:"similar_labels"` // 相似标签列表,JSON数组
}

// AttrLabelAndSimilarLabels 属性标签和相似标签
type AttrLabelAndSimilarLabels struct {
	BusinessID    uint64   `json:"business_id"`    // 业务ID
	Name          string   `json:"name"`           // 主标签
	SimilarLabels []string `json:"similar_labels"` // 相似标签列表
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

// DocAttributeLabel 文档属性标签
type DocAttributeLabel struct {
	ID         uint64    `db:"id"`          // ID
	RobotID    uint64    `db:"robot_id"`    // 机器人ID
	DocID      uint64    `db:"doc_id"`      // 文档
	Source     uint32    `db:"source"`      // 来源，1：属性标签
	AttrID     uint64    `db:"attr_id"`     // 属性ID
	LabelID    uint64    `db:"label_id"`    // 标签ID
	IsDeleted  uint32    `db:"is_deleted"`  // 0：未删除，1：已删除
	CreateTime time.Time `db:"create_time"` // 创建时间
	UpdateTime time.Time `db:"update_time"` // 更新时间
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

// LabelAble 可以带标签的结构
type LabelAble interface {
	// GetType 类型 文档 / QA
	GetType() LabelType
}

// Attributes 属性标签
type Attributes map[AttributeID]AttributeLabelItem

// LabelRelationer 内容标签关联关系
type LabelRelationer interface {
	GetType() LabelType
	GetID() uint64
	GetRelatedID() uint64
	GetAttrID() AttributeID
	GetLabelID() AttributeLabelID
	HasDeleted() bool
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

// UpdateDocAttributeLabelReq 更新文档属性标签请求
type UpdateDocAttributeLabelReq struct {
	IsNeedChange    bool                 `db:"is_need_change"`   // 是否需要改变
	AttributeLabels []*DocAttributeLabel `db:"attribute_labels"` // 引用
}

// UpdateQAAttributeLabelReq 更新QA属性标签请求
type UpdateQAAttributeLabelReq struct {
	IsNeedChange    bool                `db:"is_need_change"`   // 是否需要改变
	AttributeLabels []*QAAttributeLabel `db:"attribute_labels"` // 引用
}

// AttributeLabelItem 创建属性标签
type AttributeLabelItem struct {
	Attr   *Attribute        `db:"attr"`   // 属性信息
	Labels []*AttributeLabel `db:"labels"` // 需要添加的标签信息
}

// Label 属性标签
type Label struct {
	LabelID    uint64 `db:"label_id"`    // 标签ID
	BusinessID uint64 `db:"business_id"` // 标签业务ID
	LabelName  string `db:"label_name"`  // 属性标签名称
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
	return d.IsDeleted == DocAttributeLabelDeleted
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

// HasDeleted 是否已经删除
func (q *QAAttributeLabel) HasDeleted() bool {
	return q.IsDeleted == QAAttributeLabelDeleted
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
	LabelLogicOpr       string                    // AND或OR (标签检索条件)
	IsLabelOrGeneral    bool                      // 标签检索是否OR的形式检索default标签 默认false
	IsTransAPIParam     bool                      // 是否转换API映射参数 默认false【废弃，暂时没有用】
	IsTransSimilarLabel bool                      // 是否转换相似标签 默认false【废弃，暂时没有用】
	SearchStrategy      *knowledge.SearchStrategy // 工作流知识检索策略配置
}
