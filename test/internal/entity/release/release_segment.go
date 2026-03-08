package release

import (
	"time"
)

// ReleaseSegment 文档段
type ReleaseSegment struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement:true" json:"id"`
	RobotID        uint64    `gorm:"column:robot_id;primaryKey;comment:机器人ID" json:"robot_id"`                                            // 机器人ID
	DocID          uint64    `gorm:"column:doc_id;not null;comment:文章ID" json:"doc_id"`                                                   // 文章ID
	SegmentID      uint64    `gorm:"column:segment_id;not null;comment:文章片段ID" json:"segment_id"`                                         // 文章片段ID
	CorpID         uint64    `gorm:"column:corp_id;not null;comment:企业ID" json:"corp_id"`                                                 // 企业ID
	StaffID        uint64    `gorm:"column:staff_id;not null;comment:员工ID" json:"staff_id"`                                               // 员工ID
	VersionID      uint64    `gorm:"column:version_id;not null;comment:版本ID" json:"version_id"`                                           // 版本ID
	FileType       string    `gorm:"column:file_type;not null;comment:文件类型" json:"file_type"`                                             // 文件类型
	SegmentType    string    `gorm:"column:segment_type;not null;comment:文档切片类型 segment-文档切片 table-表格" json:"segment_type"`               // 文档切片类型 segment-文档切片 table-表格
	Title          string    `gorm:"column:title;not null;comment:章节标题" json:"title"`                                                     // 章节标题
	CreateUserID   uint64    `gorm:"column:create_user_id;not null;comment:上传用户ID" json:"create_user_id"`                                 // 上传用户ID
	PageContent    string    `gorm:"column:page_content;not null;comment:段落内容" json:"page_content"`                                       // 段落内容
	OrgData        string    `gorm:"column:org_data;not null;comment:原始内容" json:"org_data"`                                               // 原始内容
	BigDataID      string    `gorm:"column:big_data_id;not null;comment:big_data标识id" json:"big_data_id"`                                 // big_data标识id
	BigStartIndex  int32     `gorm:"column:big_start_index;not null;comment:big_data起始偏移位置" json:"big_start_index"`                       // big_data起始偏移位置
	BigEndIndex    int32     `gorm:"column:big_end_index;not null;comment:big_data结束偏移位置" json:"big_end_index"`                           // big_data结束偏移位置
	SplitModel     string    `gorm:"column:split_model;not null;comment:分割模式line:按行 window:按窗口" json:"split_model"`                       // 分割模式line:按行 window:按窗口
	Status         uint32    `gorm:"column:status;not null;comment:1未处理2处理中3处理完成4处理失败" json:"status"`                                     // 1未处理2处理中3处理完成4处理失败
	ReleaseStatus  uint32    `gorm:"column:release_status;not null;comment:发布状态(2 待发布 3 发布中 4 已发布 5 发布失败 6 不需要发布)" json:"release_status"` // 发布状态(2 待发布 3 发布中 4 已发布 5 发布失败 6 不需要发布)
	Message        string    `gorm:"column:message;not null;comment:失败原因" json:"message"`                                                 // 失败原因
	BatchID        int32     `gorm:"column:batch_id;not null;comment:doc批次ID" json:"batch_id"`                                            // doc批次ID
	RichTextIndex  int32     `gorm:"column:rich_text_index;not null;comment:rich text index" json:"rich_text_index"`                      // rich text index
	StartIndex     int32     `gorm:"column:start_index;not null;comment:分片起始索引" json:"start_index"`                                       // 分片起始索引
	EndIndex       int32     `gorm:"column:end_index;not null;comment:分片结束索引" json:"end_index"`                                           // 分片结束索引
	IsDeleted      uint32    `gorm:"column:is_deleted;not null;comment:1未删除 2已删除" json:"is_deleted"`                                      // 1未删除 2已删除
	Action         uint32    `gorm:"column:action;not null;comment:操作行为：1新增2修改3删除 4发布" json:"action"`                                     // 操作行为：1新增2修改3删除 4发布
	AttrLabels     string    `gorm:"column:attr_labels;not null;comment:属性标签" json:"attr_labels"`                                         // 属性标签
	UpdateTime     time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP;comment:更新时间" json:"update_time"`               // 更新时间
	CreateTime     time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP;comment:响应时间" json:"create_time"`               // 响应时间
	IsAllowRelease uint32    `gorm:"column:is_allow_release;not null;comment:0不允许发布1允许发布" json:"is_allow_release"`                        // 0不允许发布1允许发布
	ExpireTime     time.Time `gorm:"column:expire_time;not null;default:1970-01-01 08:00:00;comment:有效期结束时间unix时间戳" json:"expire_time"`   // 有效期结束时间unix时间戳
}

type ListReleaseSegmentReq struct {
	VersionID       uint64
	RobotID         uint64
	MinSegmentID    uint64
	MaxSegmentID    uint64
	IsAllowRelease  *uint32
	IsDeleted       *uint32
	IsDeletedNot    *uint32
	Page            uint32
	PageSize        uint32
	OrderBy         []string
	OrderDirections []string
	Actions         []uint32
}

type ReleaseSegmentFilter struct {
	ID             uint64
	IDs            []int64
	VersionID      uint64
	RobotID        uint64
	DocID          uint64
	SegmentID      uint64
	MinSegmentID   uint64
	MaxSegmentID   uint64
	IsAllowRelease *uint32
	IsDeleted      *uint32
	IsDeletedNot   *uint32
	Actions        []uint32
}
