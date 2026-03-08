package model

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	jsoniter "github.com/json-iterator/go"
)

const (
	// AuditSourceRobotName 来源昵称
	AuditSourceRobotName = "knowledge.robot_name"
	// AuditSourceReleaseQA 来源问答
	AuditSourceReleaseQA = "knowledge.release_qa"
	// AuditSourceDoc 来源文档
	AuditSourceDoc = "knowledge.doc"
	// AuditSourceBareAnswer 来源未知问题
	AuditSourceBareAnswer = "knowledge.bare_answer"
	// AuditSourceRobotAvatar  机器人头像
	AuditSourceRobotAvatar = "knowledge.robot_avatar"
	// AuditSourceRobotGreeting  机器人欢迎语
	AuditSourceRobotGreeting = "knowledge.robot_greeting"
	// AuditSourceRobotRoleDescription 机器人角色描述
	AuditSourceRobotRoleDescription = "knowledge.role_description"
	// AuditSourceQAText qa的文本审核
	AuditSourceQAText = "knowledge.qa.1"
	// AuditSourceQAAnswerPic qa的图片审核
	AuditSourceQAAnswerPic = "knowledge.qa.4"
	// AuditSourceQAAnswerVideo qa的视频审核
	AuditSourceQAAnswerVideo = "knowledge.qa.3"
	// AuditSourceDocName 文档名称的审核策略
	AuditSourceDocName = "knowledge.doc_name.1"
	// AuditSourceDocSegment 文档切片审核策略
	AuditSourceDocSegment = "knowledge.doc_segment.1"
	// AuditSourceDocSegmentPic 文档切片图片审核策略
	AuditSourceDocSegmentPic = "knowledge.doc_segment.4"
	// AuditDbSourceName 数据库名称的审核策略
	AuditDbSourceName = "knowledge.db_name.1"
	// AuditDbSourceDesc 数据库描述审核
	AuditDbSourceDesc = "knowledge.db_desc.1"
	// AuditDbSourceCheckBizType 外部数据库审核类型
	AuditDbSourceCheckBizType = "QD_AI_TEXT"

	// AuditTypePlainText 文本
	AuditTypePlainText = uint32(1)
	// AuditTypeFile 文件
	AuditTypeFile = uint32(2)
	// AuditTypeVideo 视频
	AuditTypeVideo = uint32(3)
	// AuditTypePicture 图片
	AuditTypePicture = uint32(4)
	// AuditTypeUserData 用户资料
	AuditTypeUserData = uint32(5)

	// AccountTypeOther 账号类型
	AccountTypeOther = uint32(7)

	// AuditResultFail 审核不通过
	AuditResultFail = uint32(1)
	// AuditResultPass 审核通过
	AuditResultPass = uint32(2)

	// AuditBizTypeRobotName 机器人昵称审核
	AuditBizTypeRobotName = uint32(1)
	// AuditBizTypeBareAnswer 机器人未知问题回复审核
	AuditBizTypeBareAnswer = uint32(2)
	// AuditBizTypeDoc 文档审核
	AuditBizTypeDoc = uint32(3)
	// AuditBizTypeRelease 发布审核
	AuditBizTypeRelease = uint32(4)
	// AuditBizTypeRobotProfile 机器人角色配置审核
	AuditBizTypeRobotProfile = uint32(5)
	// AuditBizTypeQa 问答审核
	AuditBizTypeQa = uint32(6)
	// AuditBizTypeDocName 文档名称审核
	AuditBizTypeDocName = uint32(7)
	// AuditBizTypeDocSegment 文档切片审核
	AuditBizTypeDocSegment = uint32(8)
	// AuditBizTypeDocTableSheet 文档表格sheet审核
	AuditBizTypeDocTableSheet = uint32(9)

	// AuditStatusForbid 禁止审核
	AuditStatusForbid = uint32(1)
	// AuditStatusDoing 待送审
	AuditStatusDoing = uint32(2)
	// AuditStatusSendSuccess 送审成功
	AuditStatusSendSuccess = uint32(3)
	// AuditStatusSendFail 送审失败
	AuditStatusSendFail = uint32(4)
	// AuditStatusPass 审核通过
	AuditStatusPass = uint32(5)
	// AuditStatusFail 审核不通过
	AuditStatusFail = uint32(6)
	// AuditStatusAppealIng 人工审核（申诉）中
	AuditStatusAppealIng = uint32(7)
	// AuditStatusAppealSuccess 人工审核（申诉）通过
	AuditStatusAppealSuccess = uint32(8)
	// AuditStatusAppealFail 人工审核（申诉）不通过
	AuditStatusAppealFail = uint32(9)
	// AuditStatusTimeoutFail 审核审核超时-兜底不通过
	AuditStatusTimeoutFail = uint32(10)

	// MaxSendAuditRetryTimes 最大送审次数
	MaxSendAuditRetryTimes = uint32(5)

	// ResultTypeFail 审核不通过
	ResultTypeFail = uint32(100)
	// ResultTypeTimeout 审核超时-不通过
	ResultTypeTimeout = uint32(101)

	// MessageAuditCheckReachRetryLimit 审核check任务重试次数达到上限
	MessageAuditCheckReachRetryLimit = "审核check任务重试次数达到上限"
)

var auditSourceDesc = map[string]string{
	AuditSourceRobotName:            "昵称",
	AuditSourceRobotAvatar:          "头像",
	AuditSourceRobotGreeting:        "欢迎语",
	AuditSourceRobotRoleDescription: "角色描述",
}

// QAFlagMain 主问标识
var QAFlagMain = "main"

// QAFlagSimilar 相似问标识
var QAFlagSimilar = "similar"

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

// AuditStatusStat 按状态统计
type AuditStatusStat struct {
	Status uint32 `db:"status"`
	Total  uint32 `db:"total"`
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
		itemStr, err := jsoniter.MarshalToString(item)
		if err != nil {
			log.ErrorContextf(ctx, "构建子审核任务失败 item:%+v err:%+v", item, err)
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

// NewRoleDescriptionAuditItem 创建角色描述送审参数
func NewRoleDescriptionAuditItem(relateID uint64, source, content, envSet string) *AuditItem {
	return &AuditItem{
		Typ:         AuditTypeUserData,
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
