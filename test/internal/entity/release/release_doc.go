package release

import (
	"time"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

// ReleaseDoc 文档
type ReleaseDoc struct {
	ID              uint64    `db:"id"`
	VersionID       uint64    `db:"version_id"`        // 版本ID
	DocID           uint64    `db:"doc_id"`            // 文档ID
	BusinessID      uint64    `db:"business_id"`       // 业务ID
	RobotID         uint64    `db:"robot_id"`          // 机器人ID
	CorpID          uint64    `db:"corp_id"`           // 企业ID
	StaffID         uint64    `db:"staff_id"`          // 员工ID
	FileName        string    `db:"file_name"`         // 文件名
	FileType        string    `db:"file_type"`         // 文件类型(markdown,word,txt)
	FileSize        uint64    `db:"file_size"`         // 文件大小
	Bucket          string    `db:"bucket"`            // 存储桶
	CosURL          string    `db:"cos_url"`           // cos文件地址
	CosHash         string    `db:"cos_hash"`          // x-cos-hash-crc64ecma 头部中的 CRC64编码进行校验上传到云端的文件和本地文件的一致性
	Message         string    `db:"message"`           // 失败原因
	Status          uint32    `db:"status"`            // 状态(1 未生成 2 生成中 3生成失败 4 生成成功)
	IsDeleted       bool      `db:"is_deleted"`        // 是否删除(0未删除 1已删除）
	IsRefer         bool      `db:"is_refer"`          // 答案是否引用(0不引用 1引用）默认0
	Source          uint32    `db:"source"`            // 文档来源( 0  源文件导入  1 网页导入) 默认 0 源文件导入
	WebURL          string    `db:"web_url"`           // 网页导入url
	BatchID         int       `db:"batch_id"`          // 文档版本，用于控制后续生成的chunk和分片
	AuditFlag       uint32    `db:"audit_flag"`        // 1待审核2已审核3无需审核
	IsCreatingQA    bool      `db:"is_creating_qa"`    // 是否正在创建QA
	IsCreatingIndex bool      `db:"is_creating_index"` // 是否正在创建索引
	Action          uint32    `db:"action"`            // 面向发布操作：1新增 2修改 3删除 4发布
	AttrRange       uint32    `db:"attr_range"`        // 属性标签适用范围 1 全部 2 按条件设置
	CreateTime      time.Time `db:"create_time"`       // 创建时间
	UpdateTime      time.Time `db:"update_time"`       // 更新时间
	ExpireTime      time.Time `db:"expire_time"`       // 有效结束时间
}

// RebuildVersionDoc 脚本重建使用，执行后删除
type RebuildVersionDoc struct {
	VersionID uint64 `db:"version_id"`
	DocID     uint64 `db:"doc_id"`
	Action    uint32 `db:"action"`
}

// RebuildDoc 脚本重建使用，执行后删除
type RebuildDoc struct {
	DocID  uint64 `db:"doc_id"`
	Action uint32 `db:"action"`
}

// ActionDesc 状态描述
func (r *ReleaseDoc) ActionDesc() string {
	if r == nil {
		return ""
	}

	desc, ok := docEntity.DocActionDesc[r.Action]
	if !ok {
		return ""
	}
	return desc
}

// ReleaseAttrLabel 发布属性标签
type ReleaseAttrLabel struct {
	Name  string `json:"name"`  // 标签名
	Value string `json:"value"` // 标签值
}

type ListReleaseDocReq struct {
	RobotID   uint64
	VersionId uint64
	FileName  string
	Query     string
	Actions   []uint32
	Page      uint32
	PageSize  uint32
}

type ReleaseDocFilter struct {
	RobotID   uint64
	DocID     uint64
	VersionId uint64
	FileName  string
	Query     string
	Actions   []uint32
}
