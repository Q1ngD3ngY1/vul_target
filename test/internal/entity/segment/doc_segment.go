package segment

import (
	"time"
)

const (
	DocSegmentTblColID            = "id"
	DocSegmentTblColBusinessID    = "business_id"
	DocSegmentTblColRobotID       = "robot_id"
	DocSegmentTblColCorpID        = "corp_id"
	DocSegmentTblColStaffId       = "staff_id"
	DocSegmentTblColDocId         = "doc_id"
	DocSegmentTblColFileType      = "file_type"
	DocSegmentTblColTitle         = "title"
	DocSegmentTblColPageContent   = "page_content"
	DocSegmentTblColOrgData       = "org_data"
	DocSegmentTblColOrgDataBizID  = "org_data_biz_id"
	DocSegmentTblColOutputs       = "outputs"
	DocSegmentTblColCostTime      = "cost_time"
	DocSegmentTblColSplitModel    = "split_model"
	DocSegmentTblColStatus        = "status"
	DocSegmentTblColReleaseStatus = "release_status"
	DocSegmentTblColMessage       = "message"
	DocSegmentTblColIsDeleted     = "is_deleted"
	DocSegmentTblColType          = "type"
	DocSegmentTblColNextAction    = "next_action"
	DocSegmentTblColBatchID       = "batch_id"
	DocSegmentTblColRichTextIndex = "rich_text_index"
	DocSegmentTblColStartIndex    = "start_index"
	DocSegmentTblColEndIndex      = "end_index"
	DocSegmentTblColLinkerKeep    = "linker_keep"
	DocSegmentTblColUpdateTime    = "update_time"
	DocSegmentTblColCreateTime    = "create_time"
	DocSegmentTblColBigDataID     = "big_data_id"
	DocSegmentTblColBigStartIndex = "big_start_index"
	DocSegmentTblColBigEndIndex   = "big_end_index"
	DocSegmentTblColSegmentType   = "segment_type"
)

var DocSegmentTblColList = []string{
	DocSegmentTblColID,
	DocSegmentTblColBusinessID,
	DocSegmentTblColRobotID,
	DocSegmentTblColCorpID,
	DocSegmentTblColStaffId,
	DocSegmentTblColDocId,
	DocSegmentTblColFileType,
	DocSegmentTblColTitle,
	DocSegmentTblColPageContent,
	DocSegmentTblColOrgData,
	DocSegmentTblColOrgDataBizID,
	DocSegmentTblColOutputs,
	DocSegmentTblColCostTime,
	DocSegmentTblColSplitModel,
	DocSegmentTblColStatus,
	DocSegmentTblColReleaseStatus,
	DocSegmentTblColMessage,
	DocSegmentTblColIsDeleted,
	DocSegmentTblColType,
	DocSegmentTblColNextAction,
	DocSegmentTblColBatchID,
	DocSegmentTblColRichTextIndex,
	DocSegmentTblColStartIndex,
	DocSegmentTblColEndIndex,
	DocSegmentTblColLinkerKeep,
	DocSegmentTblColUpdateTime,
	DocSegmentTblColCreateTime,
	DocSegmentTblColBigDataID,
	DocSegmentTblColBigStartIndex,
	DocSegmentTblColBigEndIndex,
	DocSegmentTblColSegmentType,
}

type DocSegmentFilter struct {
	ID                uint64
	IDMore            uint64
	IDs               []uint64
	BatchID           int
	BusinessIDs       []uint64
	CorpID            uint64
	AppID             uint64
	RobotId           uint64
	DocID             uint64
	StaffID           uint64
	IsDeleted         *int
	Type              int
	TypesIn           []int
	SegmentType       string
	SegmentTypesNotIn []string
	OrderColumn       []string
	OrderDirection    []string
	RouterAppBizId    uint64
	AuditStatusFilter []uint32
	Offset            int
	Limit             int
	StatusNot         uint32
	ReleaseStatus     []uint32
	ExtraCondition    string
	ExtraParams       []any
	RouterAppBizID    uint64
}

// DocSegment 文档段
type DocSegment struct {
	ID              uint64    `db:"id"`
	BusinessID      uint64    `db:"business_id"`     // 业务ID
	RobotID         uint64    `db:"robot_id"`        // 机器人ID
	CorpID          uint64    `db:"corp_id"`         // 企业ID
	StaffID         uint64    `db:"staff_id"`        // 员工ID
	DocID           uint64    `db:"doc_id"`          // 文章ID
	FileType        string    `db:"file_type"`       // 文件类型(markdown,word,txt)
	SegmentType     string    `db:"segment_type"`    // 文档切片类型(segment-文档切片 table-表格)
	Title           string    `db:"title"`           // 标题
	PageContent     string    `db:"page_content"`    // 段落内容
	OrgData         string    `db:"org_data"`        // 段落原文
	OrgDataBizID    uint64    `db:"org_data_biz_id"` // 段落原文对应的业务ID
	Outputs         string    `db:"outputs"`         // 算法处理结果
	CostTime        float64   `db:"cost_time"`       // 算法请求成功耗时(s)
	SplitModel      string    `db:"split_model"`     // 分割模式line:按行 window:按窗口
	Status          uint32    `db:"status"`          // 状态(1未处理2处理完成)
	ReleaseStatus   uint32    `db:"release_status"`  // 发布状态(2 待发布 3 发布中 4 已发布 5 发布失败)
	Message         string    `db:"message"`         // 失败原因
	IsDeleted       uint32    `db:"is_deleted"`      // 1未删除 2已删除
	Type            int       `db:"type"`            // 1生成QA 2入检索库
	NextAction      uint32    `db:"next_action"`     // 面向发布操作：1新增 2修改 3删除 4发布
	BatchID         int       `db:"batch_id"`        // 批次ID
	RichTextIndex   int       `db:"rich_text_index"` // rich text 索引
	StartChunkIndex int       `db:"start_index"`     // 分片起始索引
	EndChunkIndex   int       `db:"end_index"`       // 分片结束索引
	LinkerKeep      bool      `db:"linker_keep"`     // 连续文档合并时是否保持不做合并
	UpdateTime      time.Time `db:"update_time"`     // 更新时间
	CreateTime      time.Time `db:"create_time"`     // 响应时间
	BigDataID       string    `db:"big_data_id"`     // BigData ID (指向ES）
	BigStart        int32     `db:"big_start_index"` // BigData 分片起始索引
	BigEnd          int32     `db:"big_end_index"`   // BigData 分片结束索引
	BigString       string    `db:"-"`               // BigData 的内容
	Images          []string  `db:"-"`               // 切片图片列表
	OrgPageNumbers  string    `db:"-"`               // orgdata对应的页码，从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空  -- json格式的字符串存储
	BigPageNumbers  string    `db:"-"`               // bigdata对应的页码，从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空  -- json格式的字符串存储
	SheetData       string    `db:"-"`               // 当输入文件为excel时，返回当前orgdata和bigdata对应的sheet_data，因为表格的orgdata和bigdata相等，所以这里只返回一个 -- json格式的字符串存储
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
	if s.NextAction == SegNextActionAdd && s.IsDeleted == SegmentIsDeleted {
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
