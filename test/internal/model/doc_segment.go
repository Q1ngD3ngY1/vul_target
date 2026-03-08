package model

import "time"

const (
	// SegmentStatusInit 未处理
	SegmentStatusInit = uint32(1)
	// SegmentStatusDoing 处理中
	SegmentStatusDoing = uint32(2)
	// SegmentStatusDone 处理完成
	SegmentStatusDone = uint32(3)
	// SegmentStatusFail 处理失败
	SegmentStatusFail = uint32(4)
	// SegmentStatusCreatedQa 切片生成问答任务中,标识任务中已完成的切片,任务结束后更新回SegmentStatusDone
	SegmentStatusCreatedQa = uint32(5)

	// SegmentIsNotDeleted 分片未删除
	SegmentIsNotDeleted = 1
	// SegmentIsDeleted 分片删除
	SegmentIsDeleted = 2

	// SegmentImageVectorIsNotDeleted 分片未删除
	SegmentImageVectorIsNotDeleted = 0
	// SegmentImageVectorIsDeleted 分片删除
	SegmentImageVectorIsDeleted = 1

	// SegmentReleaseStatusInit 未发布
	SegmentReleaseStatusInit = uint32(2)
	// SegmentReleaseStatusIng 发布中
	SegmentReleaseStatusIng = uint32(3)
	// SegmentReleaseStatusSuccess 已发布
	SegmentReleaseStatusSuccess = uint32(4)
	// SegmentReleaseStatusFail 发布失败
	SegmentReleaseStatusFail = uint32(5)
	// SegmentReleaseStatusNotRequired 不需要发布
	SegmentReleaseStatusNotRequired = uint32(6)

	// SegmentTypeQA 用于生成QA
	SegmentTypeQA = 1
	// SegmentTypeIndex 用于生成索引
	SegmentTypeIndex = 2
	// SegmentTypeQAAndIndex 用于生成QA和索引
	SegmentTypeQAAndIndex = 3

	// SegmentTypeSegment 文档切片
	SegmentTypeSegment = "segment"
	// SegmentTypeTable 表格切片
	SegmentTypeTable = "table"
	// SegmentTypeText2SQLMeta text2sql 的meta分片
	SegmentTypeText2SQLMeta = "text2sql_meta"
	// SegmentTypeText2SQLContent text2sql 的内容分片
	SegmentTypeText2SQLContent = "text2sql_content"

	// SegNextActionAdd 新增
	SegNextActionAdd = uint32(1)
	// SegNextActionUpdate 更新
	SegNextActionUpdate = uint32(2)
	// SegNextActionDelete 删除
	SegNextActionDelete = uint32(3)
	// SegNextActionPublish 发布
	SegNextActionPublish = uint32(4)
	// SegMaxTimestamp 最大过期时间
	SegMaxTimestamp int64 = 2461420800
)

// DocSegment 文档段
type DocSegment struct {
	ID              uint64    `db:"id" gorm:"column:id"`
	BusinessID      uint64    `db:"business_id" gorm:"column:business_id"`         // 业务ID
	RobotID         uint64    `db:"robot_id" gorm:"column:robot_id"`               // 机器人ID
	CorpID          uint64    `db:"corp_id" gorm:"column:corp_id"`                 // 企业ID
	StaffID         uint64    `db:"staff_id" gorm:"column:staff_id"`               // 员工ID
	DocID           uint64    `db:"doc_id" gorm:"column:doc_id"`                   // 文章ID
	FileType        string    `db:"file_type" gorm:"column:file_type"`             // 文件类型(markdown,word,txt)
	SegmentType     string    `db:"segment_type" gorm:"column:segment_type"`       // 文档切片类型(segment-文档切片 table-表格)
	Title           string    `db:"title" gorm:"column:title"`                     // 标题
	PageContent     string    `db:"page_content" gorm:"column:page_content"`       // 段落内容
	OrgData         string    `db:"org_data" gorm:"column:org_data"`               // 段落原文
	OrgDataBizID    uint64    `db:"org_data_biz_id" gorm:"column:org_data_biz_id"` // 段落原文对应的业务ID
	Outputs         string    `db:"outputs" gorm:"column:outputs"`                 // 算法处理结果
	CostTime        float64   `db:"cost_time" gorm:"column:cost_time"`             // 算法请求成功耗时(s)
	SplitModel      string    `db:"split_model" gorm:"column:split_model"`         // 分割模式line:按行 window:按窗口
	Status          uint32    `db:"status" gorm:"column:status"`                   // 状态(1未处理2处理完成)
	ReleaseStatus   uint32    `db:"release_status" gorm:"column:release_status"`   // 发布状态(2 待发布 3 发布中 4 已发布 5 发布失败)
	Message         string    `db:"message" gorm:"column:message"`                 // 失败原因
	IsDeleted       int       `db:"is_deleted" gorm:"column:is_deleted"`           // 1未删除 2已删除
	Type            int       `db:"type" gorm:"column:type"`                       // 1生成QA 2入检索库
	NextAction      uint32    `db:"next_action" gorm:"column:next_action"`         // 面向发布操作：1新增 2修改 3删除 4发布
	BatchID         int       `db:"batch_id" gorm:"column:batch_id"`               // 批次ID
	RichTextIndex   int       `db:"rich_text_index" gorm:"column:rich_text_index"` // rich text 索引
	StartChunkIndex int       `db:"start_index" gorm:"column:start_index"`         // 分片起始索引
	EndChunkIndex   int       `db:"end_index" gorm:"column:end_index"`             // 分片结束索引
	LinkerKeep      bool      `db:"linker_keep" gorm:"column:linker_keep"`         // 连续文档合并时是否保持不做合并
	UpdateTime      time.Time `db:"update_time" gorm:"column:update_time"`         // 更新时间
	CreateTime      time.Time `db:"create_time" gorm:"column:create_time"`         // 响应时间
	BigDataID       string    `db:"big_data_id" gorm:"column:big_data_id"`         // BigData ID (指向ES）
	BigStart        int32     `db:"big_start_index" gorm:"column:big_start_index"` // BigData 分片起始索引
	BigEnd          int32     `db:"big_end_index" gorm:"column:big_end_index"`     // BigData 分片结束索引
	BigString       string    `db:"-" gorm:"-"`                                    // BigData 的内容
	Images          []string  `db:"-" gorm:"-"`                                    // 切片图片列表
	OrgPageNumbers  string    `db:"-" gorm:"-"`                                    // orgdata对应的页码，从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空  -- json格式的字符串存储
	BigPageNumbers  string    `db:"-" gorm:"-"`                                    // bigdata对应的页码，从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空  -- json格式的字符串存储
	SheetData       string    `db:"-" gorm:"-"`                                    // 当输入文件为excel时，返回当前orgdata和bigdata对应的sheet_data，因为表格的orgdata和bigdata相等，所以这里只返回一个 -- json格式的字符串存储
}

// DocSegmentExtend 文档段扩展字段结构体,主要针对不存入数据库但是需要引用字段的封装
type DocSegmentExtend struct {
	DocSegment
	ExpireStart time.Time // 有效期的开始时间
	ExpireEnd   time.Time // 有效期的结束时间
	MD5         string    // org data MD5
}

type SheetData struct {
	SheetName string `json:"sheet_name" db:"sheet_name"` // 表格名称
}

// IsDelete 是否用于生成索引
func (s *DocSegmentExtend) IsDelete() bool {
	if s == nil {
		return false
	}
	return s.IsDeleted == SegmentIsDeleted
}

// IsSegmentForIndex 是否用于生成索引
func (s *DocSegmentExtend) IsSegmentForIndex() bool {
	if s == nil {
		return false
	}
	return s.Type == SegmentTypeIndex
}

// IsSegmentForQAAndIndex 是否用于生成QA和索引
func (s *DocSegmentExtend) IsSegmentForQAAndIndex() bool {
	if s == nil {
		return false
	}
	return s.Type == SegmentTypeQAAndIndex
}

// IsSegmentForQA 是否用于生成QA
func (s *DocSegmentExtend) IsSegmentForQA() bool {
	if s == nil {
		return false
	}
	return s.Type == SegmentTypeQA
}

// IsText2sqlSegmentType  是否用于text2sql
func (s *DocSegmentExtend) IsText2sqlSegmentType() bool {
	if s == nil {
		return false
	}
	return s.SegmentType == SegmentTypeText2SQLMeta || s.SegmentType == SegmentTypeText2SQLContent
}

// IsNextActionAdd 是否新增操作
func (s *DocSegmentExtend) IsNextActionAdd() bool {
	if s == nil {
		return false
	}
	return s.NextAction == SegNextActionAdd
}

// IsNextActionUpdate 是否更新操作
func (s *DocSegmentExtend) IsNextActionUpdate() bool {
	if s == nil {
		return false
	}
	return s.NextAction == SegNextActionUpdate
}

// IsAllowRelease 是否允许发布
func (s *DocSegmentExtend) IsAllowRelease() bool {
	if s == nil {
		return false
	}
	// 新增且删除 => 不用发布
	if s.NextAction == NextActionAdd && s.IsDeleted == QAIsDeleted {
		return false
	}
	return true
}

// GetExpireTime 如果未读取到时间，则给项目库传0
func (s *DocSegmentExtend) GetExpireTime() int64 {
	expireTime := s.ExpireEnd.Unix()
	if expireTime < 0 {
		return 0
	}
	return expireTime
}
