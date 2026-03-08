package model

import "time"

const (
	// DocTempStatusValid 有效文档模版
	DocTempStatusValid = int8(1)
)

// DocTemplate 文档模版
type DocTemplate struct {
	ID         uint64    `db:"id"`
	IndustryID uint64    `db:"industry_id"` // 行业ID
	FileName   string    `db:"file_name"`   // 文件名
	FileDesc   string    `db:"file_desc"`   // 文件名
	FileType   string    `db:"file_type"`   // 文件类型
	FileSize   uint64    `db:"file_size"`   // 文件大小
	Bucket     string    `db:"bucket"`      // 存储桶
	CosURL     string    `db:"cos_url"`     // cos地址
	CosHash    string    `db:"cos_hash"`    // x-cos-hash-crc64ecma 头部中的 CRC64编码进行校验上传到云端的文件和本地文件的一致性
	Status     int8      `db:"status"`      // 1有效0无效
	UpdateTime time.Time `db:"update_time"` // 更新时间
	CreateTime time.Time `db:"create_time"` // 创建时间
}
