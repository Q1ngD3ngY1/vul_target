package model

import (
	"context"
	"database/sql"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"time"
	"unicode/utf8"

	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	// IsDeleted 已删除
	IsDeleted = 1
	// IsNotDeleted 未删除
	IsNotDeleted = 0
)

// Sample 样本
type Sample struct {
	ID              uint64         `db:"id"`
	SetID           uint64         `db:"set_id"`           // 集合ID
	Content         string         `db:"content"`          // 样本内容
	RoleDescription sql.NullString `db:"role_description"` // 角色描述
	CustomVariables sql.NullString `db:"custom_variables"` // 自定义参数
	CreateTime      time.Time      `db:"create_time"`      // 创建时间
	UpdateTime      time.Time      `db:"update_time"`      // 更新时间
}

// SampleRecord 样本记录
type SampleRecord struct {
	Content         string // 样本内容
	RoleDescription string // 角色描述
	CustomVariables string // 自定义参数
}

// SampleSet 样本集
type SampleSet struct {
	ID            uint64    `db:"id"`
	BusinessID    uint64    `db:"business_id"`     // 业务ID
	RobotID       uint64    `db:"robot_id"`        // 机器人ID
	CorpID        uint64    `db:"corp_id"`         // 企业ID
	Name          string    `db:"name"`            // 样本集名
	CosBucket     string    `db:"cos_bucket"`      // 存储桶
	CosURL        string    `db:"cos_url"`         // cos文件地址
	CosHash       string    `db:"cos_hash"`        // x-cos-hash-crc64ecma 头部中的 CRC64编码进行校验上传到云端的文件和本地文件的一致性
	Num           uint32    `db:"num"`             // 集合内样本数量
	IsDeleted     uint32    `db:"is_deleted"`      // 是否删除(0未删除 1已删除）
	CreateStaffID uint64    `db:"create_staff_id"` // 上传用户ID
	CreateTime    time.Time `db:"create_time"`     // 创建时间
	UpdateTime    time.Time `db:"update_time"`     // 更新时间
}

// SampleSetDetail 样本集
type SampleSetDetail struct {
	ID            uint64    `db:"id"`
	BusinessID    uint64    `db:"business_id"`     // 业务ID
	RobotID       uint64    `db:"robot_id"`        // 机器人ID
	CorpID        uint64    `db:"corp_id"`         // 企业ID
	Name          string    `db:"name"`            // 样本集名
	CosBucket     string    `db:"cos_bucket"`      // 存储桶
	CosURL        string    `db:"cos_url"`         // cos文件地址
	CosHash       string    `db:"cos_hash"`        // x-cos-hash-crc64ecma 头部中的 CRC64编码进行校验上传到云端的文件和本地文件的一致性
	Num           uint32    `db:"num"`             // 集合内样本数量
	IsDeleted     uint32    `db:"is_deleted"`      // 是否删除(0未删除 1已删除）
	CreateStaffID uint64    `db:"create_staff_id"` // 上传用户ID
	CreateTime    time.Time `db:"create_time"`     // 创建时间
	UpdateTime    time.Time `db:"update_time"`     // 更新时间
}

// ToRspList 转Rsp结果
func (s *SampleSet) ToRspList() *pb.SampleSetDetail {
	return &pb.SampleSetDetail{
		SetBizId:   s.BusinessID,
		SetName:    s.Name,
		Number:     s.Num,
		CreateTime: uint64(s.CreateTime.Unix()),
	}
}

// ToPB 转PB
func (s *SampleSet) ToPB() *pb.SampleSet {
	return &pb.SampleSet{
		SetId:      s.ID,
		SetName:    s.Name,
		Num:        s.Num,
		CreateTime: uint64(s.CreateTime.Unix()),
	}
}

// NewSamples 构建
func NewSamples(ctx context.Context, setID uint64, sampleRecord []SampleRecord) []*Sample {
	if len(sampleRecord) == 0 {
		return nil
	}
	samples := make([]*Sample, 0, len(sampleRecord))
	for _, content := range sampleRecord {
		var sampleItem = &Sample{SetID: setID, Content: "",
			RoleDescription: sql.NullString{
				String: content.RoleDescription,
				Valid:  true,
			},
			CustomVariables: sql.NullString{
				String: content.CustomVariables,
				Valid:  true,
			}}
		// 导入样本集时，每条语料超过12000字符的自动截断，https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118892670
		runeCount := utf8.RuneCountInString(content.Content)
		maxLen := config.App().SampleRule.Question.MaxLength
		if runeCount > maxLen {
			var newContent = string([]rune(content.Content)[:maxLen])
			sampleItem.Content = newContent
		} else {
			sampleItem.Content = content.Content
		}
		log.DebugContextf(ctx, "NewSamples, setID:%d, content length:before(%d), after(%d), max:%d",
			setID, runeCount, utf8.RuneCountInString(sampleItem.Content), maxLen)
		samples = append(samples, sampleItem)
	}
	return samples
}
