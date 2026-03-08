package release

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
)

const (
	auditTableName = "t_audit"

	AuditTblColId             = "id"
	AuditTblColBusinessId     = "business_id"
	AuditTblColCorpId         = "corp_id"
	AuditTblColRobotId        = "robot_id"
	AuditTblColCreateStaffId  = "create_staff_id"
	AuditTblColParentId       = "parent_id"
	AuditTblColType           = "type"
	AuditTblColParams         = "params"
	AuditTblColRelateId       = "relate_id"
	AuditTblColStatus         = "status"
	AuditTblColRetryTimes     = "retry_times"
	AuditTblColETag           = "e_tag"
	AuditTblColMessage        = "message"
	AuditTblColCreateTime     = "create_time"
	AuditTblColUpdateTime     = "update_time"
	AuditTblColParentRelateId = "parent_relate_id"

	AuditTableMaxPageSize = 1000
)

var AuditTblColList = []string{AuditTblColId, AuditTblColBusinessId, AuditTblColCorpId, AuditTblColRobotId,
	AuditTblColCreateStaffId, AuditTblColParentId, AuditTblColType, AuditTblColParams, AuditTblColRelateId,
	AuditTblColStatus, AuditTblColRetryTimes, AuditTblColETag, AuditTblColMessage, AuditTblColCreateTime,
	AuditTblColUpdateTime, AuditTblColParentRelateId}

// Audit 审核表
type Audit struct {
	ID             uint64    `db:"id"`
	BusinessID     uint64    `db:"business_id"`      // 业务ID
	CorpID         uint64    `db:"corp_id"`          // 企业ID
	RobotID        uint64    `db:"robot_id"`         // 机器人ID
	CreateStaffID  uint64    `db:"create_staff_id"`  // 员工ID
	ParentID       uint64    `db:"parent_id"`        // 归属父审核
	Type           uint32    `db:"type"`             // 审核业务类型 1机器人昵称审核、2机器人未知问题审核、3文档审核、4发布问答审核、5机器人角色配置审核、6问答审核、7文件名审核
	Params         string    `db:"params"`           // 审核参数
	RelateID       uint64    `db:"relate_id"`        // 关联ID
	Status         uint32    `db:"status"`           // 状态1初始化2待送审3送审成功4送审失败5审核回调成功6审核回调失败
	RetryTimes     uint32    `db:"retry_times"`      // 重试次数
	Message        string    `db:"message"`          // 错误信息
	ETag           string    `db:"e_tag"`            // 返回文件的校验值。ETag 的值可以用于检查 Object 的内容是否发生变化
	UpdateTime     time.Time `db:"update_time"`      // 更新时间
	CreateTime     time.Time `db:"create_time"`      // 创建时间
	ParentRelateID uint64    `db:"parent_relate_id"` // 父关联ID
}

type AuditFilter struct {
	ID              uint64
	IDMore          uint64
	IDLess          uint64
	BusinessID      uint64
	IDs             []uint64
	CorpID          uint64
	RobotID         uint64
	ParentID        *uint64 // 支持传0，所以需要改成指针，区分0值和空值
	RelatedID       int64
	Etag            string
	ParentRelatedID uint64

	Status     uint32
	StatusList []uint32

	IsDeleted    *int
	Type         uint32
	Offset       int
	Limit        int
	OrderByField string
	OrderByType  string
}

// AuditStatusStat 按状态统计
type AuditStatusStat struct {
	Status uint32 `db:"status"`
	Total  uint32 `db:"total"`
}

func (a *AuditStatusStat) TableName() string {
	return auditTableName
}

// AuditStatusList 按状态列表统计
type AuditStatusList struct {
	Status uint32 `db:"status"`
	Type   uint32 `db:"type"`
	Params string `db:"params"`
}

// AuditFailList 审核失败列表
type AuditFailList struct {
	ID       uint64 `db:"id"`
	Type     uint32 `db:"type"`
	ParentID uint64 `db:"parent_id"`
	Params   string `db:"params"`
	RelateID uint64 `db:"relate_id"`
}

// AuditRelateID 审核列表 parent_id
type AuditRelateID struct {
	ID       uint64 `db:"id"`
	Type     uint32 `db:"type"`
	RelateID uint64 `db:"relate_id"`
	Status   uint32 `db:"status"`
}

// AuditParent 机器审核Parent
type AuditParent struct {
	Status uint32 `db:"status"`
	ID     uint64 `db:"id"`
}

// AuditStatus 机器审核状态
type AuditStatus struct {
	Status uint32 `db:"status"`
}

// AuditTypeStatus 机器审核状态
type AuditTypeStatus struct {
	Type   uint32 `db:"type"`
	Status uint32 `db:"status"`
}

// AuditStatusSourceList 按状态列表统计
type AuditStatusSourceList struct {
	Status   uint32 `db:"status"`
	Source   string `db:"source"`
	Avatar   string `db:"url"`        // 头像
	Name     string `json:"nick"`     // 昵称
	Greeting string `json:"greeting"` // 欢迎语
	Content  string `json:"content"`  // 审核文本内容
}

// AuditItem 审核单元
type AuditItem struct {
	Typ uint32 `json:"typ"` // 审核类型 1. 文本 2. 文件 3. 视频 4. 图片
	// 来源：admin.robot_name 昵称 admin.qa 问答对 admin.doc 文档 admin.bare_answer 未知问题回复
	Source       string `json:"source"`
	AccountType  uint32 `json:"account_type"`   // 固定为7
	Nick         string `json:"nick"`           // 昵称
	HeadURL      string `json:"head_url"`       // 头像
	Content      string `json:"content"`        // 审核文本内容
	URL          string `json:"url"`            // 审核链接
	EnvSet       string `json:"env_set"`        // 送审环境
	RelateID     uint64 `json:"relate_id"`      // 关联ID
	Greeting     string `json:"greeting"`       // 欢迎语
	ETag         string `json:"e_tag"`          // 返回文件的校验值。ETag 的值可以用于检查 Object 的内容是否发生变化
	QAFlag       string `json:"qa_flag"`        // 问答对的类型：similar 相似问，main 主问 当Source为问答对时有效
	SegmentBizID string `json:"segment_biz_id"` // 干预关联的切片ID。对于非表格文档，为切片的OrgDataBizID;对于表格文档，为SheetBizID
}

// AuditStatusAndItem 审核状态和审核内容
type AuditStatusAndItem struct {
	Status    uint32     `db:"status"`
	Type      uint32     `db:"type"`
	AuditItem *AuditItem `db:"audit_item"`
}

// NewParentAudit 新建父审核
func NewParentAudit(corpID, robotID, staffID, relateID, parentRelateID uint64, typ uint32) *Audit {
	return &Audit{
		BusinessID:     0,
		CorpID:         corpID,
		RobotID:        robotID,
		CreateStaffID:  staffID,
		ParentID:       0,
		Type:           typ,
		Params:         "",
		RelateID:       relateID,
		Status:         AuditStatusForbid,
		ParentRelateID: parentRelateID,
	}
}

// NewAudits 新建子审核
func NewAudits(ctx context.Context, parent *Audit, items []*AuditItem) []*Audit {
	audits := make([]*Audit, 0, len(items))
	for _, item := range items {
		itemStr, err := jsonx.MarshalToString(item)
		if err != nil {
			logx.E(ctx, "构建子审核任务失败 item:%+v err:%+v", item, err)
			continue
		}
		audits = append(audits, &Audit{
			CorpID:         parent.CorpID,
			RobotID:        parent.RobotID,
			CreateStaffID:  parent.CreateStaffID,
			ParentID:       parent.ID,
			Type:           parent.Type,
			Params:         itemStr,
			RelateID:       item.RelateID,
			Status:         AuditStatusDoing,
			ETag:           item.ETag,
			ParentRelateID: parent.ParentRelateID,
		})
	}
	return audits
}

// NewPlainTextAuditItem 创建文本送审参数
func NewPlainTextAuditItem(relateID uint64, source, content, envSet string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypePlainText,
		Source:      source,
		AccountType: AccountTypeOther,
		Nick:        "",
		HeadURL:     "",
		Content:     content,
		URL:         "",
		Greeting:    "",
		EnvSet:      envSet,
		RelateID:    relateID,
	}
}

// NewFileAuditItem 创建文件送审参数
func NewFileAuditItem(relateID uint64, source, url, envSet, eTag string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypeFile,
		Source:      source,
		AccountType: AccountTypeOther,
		Nick:        "",
		HeadURL:     "",
		Content:     "",
		URL:         url,
		Greeting:    "",
		EnvSet:      envSet,
		RelateID:    relateID,
		ETag:        eTag,
	}
}

// NewPictureAuditItem 创建图片送审参数
func NewPictureAuditItem(relateID uint64, source, url, envSet, eTag string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypePicture,
		Source:      source,
		AccountType: AccountTypeOther,
		Nick:        "",
		HeadURL:     "",
		Content:     "",
		URL:         url,
		Greeting:    "",
		EnvSet:      envSet,
		RelateID:    relateID,
		ETag:        eTag,
	}
}

// NewVideoAuditItem 创建视频送审参数
func NewVideoAuditItem(relateID uint64, source, url, envSet, eTag string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypeVideo,
		Source:      source,
		AccountType: AccountTypeOther,
		Nick:        "",
		HeadURL:     "",
		Content:     "",
		URL:         url,
		Greeting:    "",
		EnvSet:      envSet,
		RelateID:    relateID,
		ETag:        eTag,
	}
}

// NewUserDataAuditItem 创建用户资料送审参数
func NewUserDataAuditItem(relateID uint64, source, nick, envSet string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypeUserData,
		Source:      source,
		AccountType: AccountTypeOther,
		Nick:        nick,
		HeadURL:     "",
		Content:     "",
		URL:         "",
		Greeting:    "",
		EnvSet:      envSet,
		RelateID:    relateID,
	}
}

// NewUserHeadURLAuditItem 上传用户头像送审参数
func NewUserHeadURLAuditItem(relateID uint64, source, url, envSet, eTag string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypePicture,
		Source:      source,
		AccountType: AccountTypeOther,
		Nick:        "",
		HeadURL:     url,
		Content:     "",
		URL:         url,
		Greeting:    "",
		EnvSet:      envSet,
		RelateID:    relateID,
		ETag:        eTag,
	}
}

// NewUserGreetingAuditItem 上传用户欢迎语送审参数（使用 user 中的 desc：简介 字段）
func NewUserGreetingAuditItem(relateID uint64, source, greeting, envSet string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypeUserData,
		Source:      source,
		AccountType: AccountTypeOther,
		Nick:        "",
		HeadURL:     "",
		Content:     "",
		URL:         "",
		Greeting:    greeting,
		EnvSet:      envSet,
		RelateID:    relateID,
	}
}

// IsCallbackDone 是否任务回调完成
func (a *Audit) IsCallbackDone() bool {
	if a == nil {
		return false
	}
	return a.Status == AuditStatusPass || a.Status == AuditStatusFail || a.Status == AuditStatusAppealSuccess ||
		a.Status == AuditStatusAppealFail || a.Status == AuditStatusTimeoutFail
}

// GetSourceDesc 获取审核子项目类型
func (a *Audit) GetSourceDesc(sourceName string) string {
	if sourceName == "" {
		return ""
	}
	if _, ok := auditSourceDesc[sourceName]; !ok {
		return ""
	}
	return auditSourceDesc[sourceName]
}

// IsMaxSendAuditRetryTimes 是否达到最大送审重试次数
func (a *Audit) IsMaxSendAuditRetryTimes() bool {
	return a.RetryTimes > MaxSendAuditRetryTimes
}

func GetAuditStatusAndItems(ctx context.Context, auditStatusList []*AuditStatusList) (
	[]*AuditStatusAndItem, error) {
	stAndItems := make([]*AuditStatusAndItem, 0)
	for _, v := range auditStatusList {
		auditItem := AuditItem{}
		if err := jsonx.UnmarshalFromString(v.Params, &auditItem); err != nil {
			logx.E(ctx, "任务参数解析失败 v.Params:%s,err:%+v", v.Params, err)
			return nil, err
		}
		if v.Status == AuditStatusFail || v.Status == AuditStatusAppealFail ||
			v.Status == AuditStatusTimeoutFail {
			stAndItems = append(stAndItems, &AuditStatusAndItem{
				Status:    v.Status,
				Type:      v.Type,
				AuditItem: &auditItem,
			})
		}
	}
	return stAndItems, nil
}

func GetAuditStatusAndItemsByFailList(ctx context.Context, auditFailList []*AuditFailList) (
	[]*AuditStatusAndItem, error) {
	stAndItems := make([]*AuditStatusAndItem, 0)
	for _, v := range auditFailList {
		if v.ParentID == 0 {
			continue
		}
		auditItem := AuditItem{}
		if err := jsonx.UnmarshalFromString(v.Params, &auditItem); err != nil {
			logx.E(ctx, "任务参数解析失败 v.Params:%s,err:%+v", v.Params, err)
			return nil, err
		}
		stAndItems = append(stAndItems, &AuditStatusAndItem{
			Status:    AuditStatusFail,
			Type:      v.Type,
			AuditItem: &auditItem,
		})
	}
	return stAndItems, nil
}
