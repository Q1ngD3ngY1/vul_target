package segment

import "time"

const (
	InsertAtFirst = "first"
	EditPrefix    = "edit_"
	InsertPrefix  = "insert_"
)

const (
	AddMethodDefault    = uint32(0)
	AddMethodArtificial = uint32(1)
	AddMethodEdit       = uint32(2)
	// SegmentIsEnable 分片启用
	SegmentIsEnable = uint32(0)
	// SegmentIsDisabled 分片未启用
	SegmentIsDisabled = uint32(1)
)

const (
	// SheetEnableRetrievalEnhance 启用检索增强
	SheetEnableRetrievalEnhance = uint32(0)
	// SheetDisabledRetrievalEnhance 未启用检索增强
	SheetDisabledRetrievalEnhance = uint32(1)
	// SheetDefaultVersion 默认版本
	SheetDefaultVersion = 0
)

const (
	EditAction                     = 0
	InsertAction                   = 1
	DocSegmentInterveneMaxPageSize = 200
)

type DocSegmentAuditStatus uint32

const (
	DocSegmentAuditStatusPass DocSegmentAuditStatus = iota
	DocSegmentAuditStatusContentFailed
	DocSegmentAuditStatusPictureFailed
	DocSegmentAuditStatusContentAndPictureFailed
)

type FilterKey int

const (
	DocSegmentFilterKeyAuditStatus FilterKey = iota + 1
)

var (
	DocSegmentFilterKeyMap = map[string]FilterKey{
		"AuditStatus": DocSegmentFilterKeyAuditStatus, // 筛选审核状态
	}
	DocSegmentFilterAuditStatusMap = map[string]DocSegmentAuditStatus{
		"Pass":                    DocSegmentAuditStatusPass,
		"ContentFailed":           DocSegmentAuditStatusContentFailed,
		"PictureFailed":           DocSegmentAuditStatusPictureFailed,
		"ContentAndPictureFailed": DocSegmentAuditStatusContentAndPictureFailed,
	}
)

const (
	DocSegmentSheetTemporaryTblColBusinessID                 = "business_id"
	DocSegmentSheetTemporaryTblColAppBizID                   = "app_biz_id"
	DocSegmentSheetTemporaryTblColCorpBizID                  = "corp_biz_id"
	DocSegmentSheetTemporaryTblColStaffBizID                 = "staff_biz_id"
	DocSegmentSheetTemporaryTblColDocBizID                   = "doc_biz_id"
	DocSegmentSheetTemporaryTblColBucket                     = "bucket"
	DocSegmentSheetTemporaryTblColRegion                     = "region"
	DocSegmentSheetTemporaryTblColCosURL                     = "cos_url"
	DocSegmentSheetTemporaryTblColCosHash                    = "cos_hash"
	DocSegmentSheetTemporaryTblColFileName                   = "file_name"
	DocSegmentSheetTemporaryTblColFileType                   = "file_type"
	DocSegmentSheetTemporaryTblColSheetOrder                 = "sheet_order"
	DocSegmentSheetTemporaryTblColSheetName                  = "sheet_name"
	DocSegmentSheetTemporaryTblColSheetTotalNum              = "sheet_total_num"
	DocSegmentSheetTemporaryTblColVersion                    = "version"
	DocSegmentSheetTemporaryTblColIsDeleted                  = "is_deleted"
	DocSegmentSheetTemporaryTblColIsDisabled                 = "is_disabled"
	DocSegmentSheetTemporaryTblColIsDisabledRetrievalEnhance = "is_disabled_retrieval_enhance"
	DocSegmentSheetTemporaryTblColCreateTime                 = "create_time"
	DocSegmentSheetTemporaryTblColUpdateTime                 = "update_time"
	DocSegmentSheetTemporaryTblColAuditStatus                = "audit_status"

	DocSegmentSheetTemporaryTableMaxPageSize = 1000
)

var DocSegmentSheetTemporaryTblColList = []string{DocSegmentSheetTemporaryTblColBusinessID,
	DocSegmentSheetTemporaryTblColAppBizID, DocSegmentSheetTemporaryTblColCorpBizID, DocSegmentSheetTemporaryTblColStaffBizID,
	DocSegmentSheetTemporaryTblColBucket, DocSegmentSheetTemporaryTblColRegion, DocSegmentSheetTemporaryTblColCosURL,
	DocSegmentSheetTemporaryTblColCosHash, DocSegmentSheetTemporaryTblColFileName, DocSegmentSheetTemporaryTblColFileType,
	DocSegmentSheetTemporaryTblColSheetOrder, DocSegmentSheetTemporaryTblColSheetName,
	DocSegmentSheetTemporaryTblColSheetTotalNum, DocSegmentSheetTemporaryTblColVersion, DocSegmentSheetTemporaryTblColIsDeleted,
	DocSegmentSheetTemporaryTblColIsDisabled, DocSegmentSheetTemporaryTblColIsDisabledRetrievalEnhance,
	DocSegmentSheetTemporaryTblColCreateTime, DocSegmentSheetTemporaryTblColUpdateTime, DocSegmentSheetTemporaryTblColAuditStatus}

type DocSegmentSheetTemporaryFilter struct {
	BusinessIDs    []uint64
	CorpBizID      uint64 // 企业 ID
	AppBizID       uint64
	DocBizID       uint64
	IsDeleted      *bool
	SheetNames     []string // 查询sheet名称
	CosHash        string
	Version        *int
	AuditStatus    []uint32
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
}

const (
	DocSegmentOrgDataTemporaryTblColBusinessID          = "business_id"
	DocSegmentOrgDataTemporaryTblColAppBizID            = "app_biz_id"
	DocSegmentOrgDataTemporaryTblColCorpBizID           = "corp_biz_id"
	DocSegmentOrgDataTemporaryTblColStaffBizID          = "staff_biz_id"
	DocSegmentOrgDataTemporaryTblColDocBizID            = "doc_biz_id"
	DocSegmentOrgDataTemporaryTblColOrgData             = "org_data"
	DocSegmentOrgDataTemporaryTblColAddMethod           = "add_method"
	DocSegmentOrgDataTemporaryTblColAction              = "action"
	DocSegmentOrgDataTemporaryTblColSegmentType         = "segment_type"
	DocSegmentOrgDataTemporaryTblColOrgPageNumbers      = "org_page_numbers"
	DocSegmentOrgDataTemporaryTblColOriginOrgDataID     = "origin_org_data_id"
	DocSegmentOrgDataTemporaryTblColLastOrgDataID       = "last_org_data_id"
	DocSegmentOrgDataTemporaryTblColAfterOrgDataID      = "after_org_data_id"
	DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID = "last_origin_org_data_id"
	DocSegmentOrgDataTemporaryTblColIsDeleted           = "is_deleted"
	DocSegmentOrgDataTemporaryTblColIsDisabled          = "is_disabled"
	DocSegmentOrgDataTemporaryTblColCreateTime          = "create_time"
	DocSegmentOrgDataTemporaryTblColUpdateTime          = "update_time"
	DocSegmentOrgDataTemporaryTblColAuditStatus         = "audit_status"
	DocSegmentOrgDataTemporaryTblColSheetName           = "sheet_name"
)

var DocSegmentOrgDataTemporaryTblColList = []string{
	DocSegmentOrgDataTemporaryTblColBusinessID,
	DocSegmentOrgDataTemporaryTblColAppBizID,
	DocSegmentOrgDataTemporaryTblColCorpBizID,
	DocSegmentOrgDataTemporaryTblColStaffBizID,
	DocSegmentOrgDataTemporaryTblColDocBizID,
	DocSegmentOrgDataTemporaryTblColOrgData,
	DocSegmentOrgDataTemporaryTblColAddMethod,
	DocSegmentOrgDataTemporaryTblColAction,
	DocSegmentOrgDataTemporaryTblColSegmentType,
	DocSegmentOrgDataTemporaryTblColOrgPageNumbers,
	DocSegmentOrgDataTemporaryTblColOriginOrgDataID,
	DocSegmentOrgDataTemporaryTblColLastOrgDataID,
	DocSegmentOrgDataTemporaryTblColAfterOrgDataID,
	DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
	DocSegmentOrgDataTemporaryTblColIsDeleted,
	DocSegmentOrgDataTemporaryTblColIsDisabled,
	DocSegmentOrgDataTemporaryTblColCreateTime,
	DocSegmentOrgDataTemporaryTblColUpdateTime,
	DocSegmentOrgDataTemporaryTblColAuditStatus,
	DocSegmentOrgDataTemporaryTblColSheetName,
}

type DocSegmentOrgDataTemporaryFilter struct {
	BusinessIDs          []string
	CorpBizID            uint64
	AppBizID             uint64
	DocBizID             uint64
	IsDeleted            *bool
	Action               *int
	OrgData              string
	OriginOrgDataIDs     []string
	LastOriginOrgDataIDs []string
	LastOrgDataID        string

	Keywords       string
	AuditStatus    []uint32
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
	SheetName      string
}

// DocSegmentCommon 干预相关公共字段
type DocSegmentCommon struct {
	AppID      uint64 // 应用ID
	AppBizID   uint64 // 应用业务ID
	CorpID     uint64 // 企业ID
	CorpBizID  uint64 // 企业业务ID
	StaffID    uint64 // 员工ID
	StaffBizID uint64 // 员工业务ID
	DocID      uint64 // 文档ID
	DocBizID   uint64 // 文档ID
	SheetName  string // 表格名称
	DataSource uint32 // 数据来源
}

type OldOrgDataInfo struct {
	AddMethod  uint32 // 添加方式 0:初版解析生成 1:手动添加 2:编辑
	IsDisabled uint32 // 0:启用 1:停用
}

// DocSegmentOrgDataTemporary 用于干预临时文档数据
type DocSegmentOrgDataTemporary struct {
	BusinessID          string    `db:"business_id"   gorm:"column:business_id"`                        // 业务ID
	AppBizID            uint64    `db:"app_biz_id"    gorm:"column:app_biz_id"`                         // 应用ID
	DocBizID            uint64    `db:"doc_biz_id"    gorm:"column:doc_biz_id"`                         // 文档ID
	CorpBizID           uint64    `db:"corp_biz_id"   gorm:"column:corp_biz_id"`                        // 企业ID
	StaffBizID          uint64    `db:"staff_biz_id"  gorm:"column:staff_biz_id"`                       // 员工ID
	OrgData             string    `db:"org_data"      gorm:"column:org_data"`                           // 原始数据ID
	AddMethod           uint32    `db:"add_method"    gorm:"column:add_method"`                         // 添加方式
	Action              uint32    `db:"action" gorm:"column:action"`                                    // 操作
	OrgPageNumbers      string    `db:"org_page_numbers"  gorm:"column:org_page_numbers"`               // 原始内容对应的页码。从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空
	SegmentType         string    `db:"segment_type"  gorm:"column:segment_type"`                       // 段落类型
	OriginOrgDataID     string    `db:"origin_org_data_id"  gorm:"column:origin_org_data_id"`           // 原始数据ID
	LastOrgDataID       string    `db:"last_org_data_id"  gorm:"column:last_org_data_id"`               // 原始数据ID
	AfterOrgDataID      string    `db:"after_org_data_id"  gorm:"column:after_org_data_id"`             // 原始数据ID
	LastOriginOrgDataID string    `db:"last_origin_org_data_id"  gorm:"column:last_origin_org_data_id"` // 原始数据ID
	IsDeleted           bool      `db:"is_deleted"  gorm:"column:is_deleted"`                           // 是否删除(0未删除 1已删除）
	IsDisabled          bool      `db:"is_disabled" gorm:"column:is_disabled"`                          // 是否停用(0启用 1停用)
	CreateTime          time.Time `db:"create_time"  gorm:"column:create_time"`                         // 创建时间
	UpdateTime          time.Time `db:"update_time"  gorm:"column:update_time"`                         // 更新时间
	AuditStatus         uint32    `db:"audit_status"  gorm:"column:audit_status"`                       // 审核状态。0:审核通过；1:内容审核失败；2:图片审核失败；3:图片和内容审核失败
	SheetName           string    `db:"sheet_name"  gorm:"column:sheet_name"`                           // 表格sheet名称
}

// DocSegmentSheetTemporary 用于干预临时表格文档数据
type DocSegmentSheetTemporary struct {
	BusinessID                 uint64    `db:"business_id"      gorm:"column:business_id"`                                // 业务ID
	AppBizID                   uint64    `db:"app_biz_id" gorm:"column:app_biz_id"`                                       // 应用ID
	DocBizID                   uint64    `db:"doc_biz_id" gorm:"column:doc_biz_id"`                                       // 文档ID
	CorpBizID                  uint64    `db:"corp_biz_id"   gorm:"column:corp_biz_id"`                                   // 企业ID
	StaffBizID                 uint64    `db:"staff_biz_id"  gorm:"column:staff_biz_id"`                                  // 员工ID
	Bucket                     string    `db:"bucket" gorm:"column:bucket"`                                               // 存储桶
	Region                     string    `db:"region" gorm:"column:region"`                                               // 存储桶所在地域
	CosURL                     string    `db:"cos_url" gorm:"column:cos_url"`                                             // cos地址
	CosHash                    string    `db:"cos_hash" gorm:"column:cos_hash"`                                           // x-cos-hash-crc64ecma 头部中的 CRC64编码进行校验上传到云端的文件和本地文件的一致性
	FileName                   string    `db:"file_name" gorm:"column:file_name"`                                         // 文件名称
	FileType                   string    `db:"file_type" gorm:"column:file_type"`                                         // 文件类型
	SheetOrder                 int       `db:"sheet_order" gorm:"column:sheet_order"`                                     // sheet顺序
	SheetName                  string    `db:"sheet_name" gorm:"column:sheet_name"`                                       // sheet名称
	SheetTotalNum              int       `db:"sheet_total_num" gorm:"column:sheet_total_num"`                             // 文档中的sheet总数
	Version                    int       `db:"version" gorm:"column:version"`                                             // 版本
	IsDeleted                  bool      `db:"is_deleted" gorm:"column:is_deleted"`                                       // 是否删除(0未删除 1已删除）
	IsDisabled                 bool      `db:"is_disabled" gorm:"column:is_disabled"`                                     // 是否停用(0启用 1停用)
	IsDisabledRetrievalEnhance bool      `db:"is_disabled_retrieval_enhance" gorm:"column:is_disabled_retrieval_enhance"` // 是否开启检索增强(0启用 1停用)
	CreateTime                 time.Time `db:"create_time" gorm:"column:create_time"`                                     // 创建时间
	UpdateTime                 time.Time `db:"update_time" gorm:"column:update_time"`                                     // 更新时间
	AuditStatus                uint32    `db:"audit_status"  gorm:"column:audit_status"`                                  // 审核状态。0:审核通过；1:内容审核失败
}
