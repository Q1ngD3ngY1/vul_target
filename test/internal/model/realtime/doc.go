// Package realtime 实时文档
// @Author: halelv
// @Date: 2024/5/15 18:44
package realtime

import (
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"git.woa.com/dialogue-platform/proto/pb-stub/nrt_file_parser_server"
)

const (
	// RealDocStatusInit 初始化
	RealDocStatusInit = "INIT"
	// RealDocStatusParsing 解析中
	RealDocStatusParsing = "PARSING"
	// RealDocStatusLearning 学习中
	RealDocStatusLearning = "LEARNING"
	// RealDocStatusSuccess 成功
	RealDocStatusSuccess = "SUCCESS"
	// RealDocStatusFailed 失败
	RealDocStatusFailed = "FAILED"
	// RealDocStatusCancel 取消
	RealDocStatusCancel = "CANCEL"
)

const (
	// TaskStatusSuccess 文档解析任务成功
	TaskStatusSuccess = 0
	// TaskStatusFailed 文件解析任务失败
	TaskStatusFailed = 1
)

const (
	// RealDocLearningProgress 文档学习中进度标记
	RealDocLearningProgress = 90
	// RealDocSuccessProgress 文档学习完成进度
	RealDocSuccessProgress = 100

	// RealDocLearningDesc 文档学习中描述
	RealDocLearningDesc = "文档学习中"
	// RealDocSuccessDesc 文档学习完成描述
	RealDocSuccessDesc = "文档学习完成"

	// RealDocErrMsgParseFailed 文档解析失败
	RealDocErrMsgParseFailed = "文档解析失败"
	// RealDocErrMsgLearnFailed 文档学习失败
	RealDocErrMsgLearnFailed = "文档学习失败"
	// RealDocErrMsgTaskCancel 文档取消解析
	RealDocErrMsgTaskCancel = "文档取消解析"
)

// TRealtimeDoc 实时文档
type TRealtimeDoc struct {
	ID              uint64    `gorm:"column:id"`                                          // 自增ID
	DocID           uint64    `gorm:"column:doc_id"`                                      // 文档ID
	SessionID       string    `gorm:"column:session_id"`                                  // 会话SessionID
	CosUrlID        uint64    `gorm:"column:cos_url_id"`                                  // cos文件地址ID
	RobotID         uint64    `gorm:"column:robot_id"`                                    // 机器人ID
	CorpID          uint64    `gorm:"column:corp_id"`                                     // 企业ID
	StaffID         uint64    `gorm:"column:staff_id"`                                    // 员工ID
	FileName        string    `gorm:"column:file_name"`                                   // 文件名
	FileType        string    `gorm:"column:file_type"`                                   // 文件类型
	FileSize        uint64    `gorm:"column:file_size"`                                   // 文件大小
	Bucket          string    `gorm:"column:bucket"`                                      // 存储桶
	CosUrl          string    `gorm:"column:cos_url"`                                     // cos文件地址
	CosHash         string    `gorm:"column:cos_hash"`                                    // x-cos-hash-crc64ecma 用于校验文件一致性
	CharSize        int32     `gorm:"column:char_size"`                                   // 文档字符数
	FileFullText    string    `gorm:"column:file_full_text"`                              // 文档全文（不为空表示满足条件可以直接使用，为空不能直接使用）
	Message         string    `gorm:"column:message"`                                     // 失败原因
	Status          string    `gorm:"column:status"`                                      // 状态(INIT: 初始化；PARSING:解析中；LEARNING:学习中；SUCCESS:成功；FAILED:失败；CANCEL:取消)
	RequestID       string    `gorm:"column:request_id"`                                  // 文档解析任务请求唯一id
	TaskID          string    `gorm:"column:task_id"`                                     // 文档解析任务TaskID
	OpType          int32     `gorm:"column:op_type"`                                     // 文档解析任务类型
	Result          string    `gorm:"column:result"`                                      // 文档解析任务结果
	TaskStatus      int32     `gorm:"column:task_status"`                                 // 状态(0:成功，1：失败)
	Progress        int32     `gorm:"column:progress"`                                    // 进度（0~100）
	ProgressMessage string    `gorm:"column:progress_message"`                            // 进度信息
	IsDeleted       uint32    `gorm:"column:is_deleted"`                                  // 是否删除(0未删除 1已删除）
	CreateTime      time.Time `gorm:"column:create_time;type:datetime;null;default:null"` // 创建时间
	UpdateTime      time.Time `gorm:"column:update_time;type:datetime;null;default:null"` // 更新时间
}

// TableName 实时文档表名
func (TRealtimeDoc) TableName() string {
	return "t_realtime_doc"
}

// GetTaskStatus 获取文档解析任务状态
func (doc TRealtimeDoc) GetTaskStatus() knowledge.TaskRsp_StatusType {
	switch doc.Status {
	case RealDocStatusInit, RealDocStatusParsing, RealDocStatusLearning:
		return knowledge.TaskRsp_PARSING
	case RealDocStatusSuccess:
		return knowledge.TaskRsp_SUCCESS
	case RealDocStatusFailed, RealDocStatusCancel:
		return knowledge.TaskRsp_FAILED
	default:
		log.Errorf("GetTaskStatus|illegal status|doc:%+v", doc)
		return knowledge.TaskRsp_UNKNOWN_TYPE
	}
}

// CanParse 文档是否可以提交解析
func (doc TRealtimeDoc) CanParse() bool {
	return doc.Status == RealDocStatusInit
}

// CanCancel 文档是否可以取消解析
func (doc TRealtimeDoc) CanCancel() bool {
	return doc.Status == RealDocStatusParsing
}

// IsFinalStatus 文档是否达到终态
func (doc TRealtimeDoc) IsFinalStatus() bool {
	return doc.Status == RealDocStatusSuccess || doc.Status == RealDocStatusFailed ||
		doc.Status == RealDocStatusCancel
}

// ConvertToParseDocRspChan 组装解析结果回包
func (doc TRealtimeDoc) ConvertToParseDocRspChan(docSummary string, statisticInfo *knowledge.StatisticInfo) *ParseDocRspChan {
	rspChan := &ParseDocRspChan{}
	rspChan.SessionID = doc.SessionID
	rspChan.CosUrlID = doc.CosUrlID

	switch doc.Status {
	case RealDocStatusInit, RealDocStatusParsing, RealDocStatusLearning:
		rspChan.Type = knowledge.StreamSaveDocRsp_PROGRESS
		rspChan.Status = knowledge.TaskRsp_PARSING
		rspChan.Progress = knowledge.Progress{
			Progress: doc.Progress,
			Message:  doc.ProgressMessage,
		}
	case RealDocStatusSuccess:
		rspChan.Type = knowledge.StreamSaveDocRsp_TASK_RSP
		rspChan.Status = knowledge.TaskRsp_SUCCESS
		rspChan.Progress = knowledge.Progress{
			Progress: doc.Progress,
			Message:  doc.ProgressMessage,
		}
		rspChan.DocID = doc.DocID
		rspChan.Summary = docSummary
		rspChan.StatisticInfo = statisticInfo
	case RealDocStatusFailed, RealDocStatusCancel:
		rspChan.Type = knowledge.StreamSaveDocRsp_TASK_RSP
		rspChan.Status = knowledge.TaskRsp_FAILED
		rspChan.ErrMsg = doc.GetErrMsg()
	default:
		log.Errorf("ConvertToParseDocRspChan|illegal status|doc:%+v", doc)
		return nil
	}
	return rspChan
}

// GetErrMsg 获取错误信息
func (doc TRealtimeDoc) GetErrMsg() string {
	switch doc.Status {
	case RealDocStatusInit, RealDocStatusParsing, RealDocStatusLearning, RealDocStatusSuccess:
		return ""
	case RealDocStatusFailed:
		msg, ok := utilConfig.GetMainConfig().RealtimeConfig.TaskStatusErrMsgMap[doc.TaskStatus]
		if ok {
			return msg
		}
		if doc.Progress >= RealDocLearningProgress {
			return RealDocErrMsgLearnFailed
		} else {
			return RealDocErrMsgParseFailed
		}
	case RealDocStatusCancel:
		return RealDocErrMsgTaskCancel
	default:
		log.Errorf("GetErrMsg|illegal status|doc:%+v", doc)
		return ""
	}
}

// GetTaskFileInfo 获取底座解析任务的文件信息
func (doc TRealtimeDoc) GetTaskFileInfo() *nrt_file_parser_server.FileInfo {
	fileInfo := &nrt_file_parser_server.FileInfo{
		FileSource: nrt_file_parser_server.FileSource_DOWNLOAD_URL,
		CosBucket:  doc.Bucket,
		FileUrl:    doc.CosUrl,
		FileName:   doc.FileName,
		FileMd5:    doc.CosHash,
	}
	switch doc.FileType {
	case model.FileTypeDocx:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_DOCX
	case model.FileTypeMD:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_MD
	case model.FileTypeTxt:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_TXT
	case model.FileTypeXlsx:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_XLSX
	case model.FileTypePdf:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_PDF
	case model.FileTypePptx:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_PPTX
	case model.FileTypePpt:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_PPT
	case model.FileTypeDoc:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_DOC
	case model.FileTypeXls:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_XLS
	case model.FileTypePng:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_PNG
	case model.FileTypeJpg:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_JPG
	case model.FileTypeJpeg:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_JPEG
	case model.FileTypeCsv:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_CSV
	default:
		fileInfo.FileType = nrt_file_parser_server.FileInfo_UNKNOWN_FILE_TYPE
	}
	return fileInfo
}

// FillRealtimeDocInfo 填充实时文档信息
func FillRealtimeDocInfo(doc *TRealtimeDoc, newDoc *TRealtimeDoc) {
	doc.ID = newDoc.ID
	doc.DocID = newDoc.DocID
	doc.SessionID = newDoc.SessionID
	doc.CosUrlID = newDoc.CosUrlID
	doc.RobotID = newDoc.RobotID
	doc.CorpID = newDoc.CorpID
	doc.StaffID = newDoc.StaffID
	doc.FileName = newDoc.FileName
	doc.FileType = newDoc.FileType
	doc.FileSize = newDoc.FileSize
	doc.Bucket = newDoc.Bucket
	doc.CosUrl = newDoc.CosUrl
	doc.CosHash = newDoc.CosHash
	doc.CharSize = newDoc.CharSize
	doc.FileFullText = newDoc.FileFullText
	doc.Message = newDoc.Message
	doc.Status = newDoc.Status
	doc.RequestID = newDoc.RequestID
	doc.TaskID = newDoc.TaskID
	doc.OpType = newDoc.OpType
	doc.Result = newDoc.Result
	doc.TaskStatus = newDoc.TaskStatus
	doc.Progress = newDoc.Progress
	doc.ProgressMessage = newDoc.ProgressMessage
	doc.IsDeleted = newDoc.IsDeleted
	doc.CreateTime = newDoc.CreateTime
	doc.UpdateTime = newDoc.UpdateTime
}

// TRealtimeDocSegment 实时文档切片
type TRealtimeDocSegment struct {
	ID              uint64    `gorm:"column:id"`                                          // 自增ID
	SegmentID       uint64    `gorm:"column:segment_id"`                                  // 切片ID
	SessionID       string    `gorm:"column:session_id"`                                  // 会话SessionID
	DocID           uint64    `gorm:"column:doc_id"`                                      // 文档ID
	RobotID         uint64    `gorm:"column:robot_id"`                                    // 机器人ID
	CorpID          uint64    `gorm:"column:corp_id"`                                     // 企业ID
	StaffID         uint64    `gorm:"column:staff_id"`                                    // 员工ID
	FileType        string    `gorm:"column:file_type"`                                   // 文件类型
	SegmentType     string    `gorm:"column:segment_type"`                                // 文档切片类型(segment-文档切片 table-表格)
	Title           string    `gorm:"column:title"`                                       // 标题
	PageContent     string    `gorm:"column:page_content"`                                // 段落内容
	OrgData         string    `gorm:"column:org_data"`                                    // 段落原文
	SplitModel      string    `gorm:"column:split_model"`                                 // 分割模式line:按行 window:按窗口
	IsSyncKnowledge uint32    `gorm:"column:is_sync_knowledge"`                           // 是否同步知识(0未同步 1已同步）
	IsDeleted       uint32    `gorm:"column:is_deleted"`                                  // 是否删除(1-未删除 2-已删除）
	RichTextIndex   int       `gorm:"column:rich_text_index"`                             // rich text 索引
	StartChunkIndex int       `gorm:"column:start_index"`                                 // 分片起始索引
	EndChunkIndex   int       `gorm:"column:end_index"`                                   // 分片结束索引
	LinkerKeep      bool      `gorm:"column:linker_keep"`                                 // 连续文档合并时是否保持不做合并
	BigDataID       string    `gorm:"column:big_data_id"`                                 // BigData ID (指向ES）
	BigStart        int32     `gorm:"column:big_start_index"`                             // BigData 分片起始索引
	BigEnd          int32     `gorm:"column:big_end_index"`                               // BigData 分片结束索引
	BigString       string    `gorm:"-"`                                                  // BigData 的内容
	Images          []string  `gorm:"-"`                                                  // 切片图片列表
	UpdateTime      time.Time `gorm:"column:update_time;type:datetime(0);autoUpdateTime"` // 更新时间
	CreateTime      time.Time `gorm:"column:create_time;type:datetime(0);autoCreateTime"` // 响应时间
}

// TableName 实时文档切片表名
func (TRealtimeDocSegment) TableName() string {
	return "t_realtime_doc_segment"
}

// TRealtimeDocSegmentImage 实时文档切片图片
type TRealtimeDocSegmentImage struct {
	ID          uint64    `gorm:"column:id"`                                          // 自增ID
	ImageID     uint64    `gorm:"column:image_id"`                                    // 图片ID
	SegmentID   uint64    `gorm:"column:segment_id"`                                  // 切片ID
	DocID       uint64    `gorm:"column:doc_id"`                                      // 文档ID
	RobotID     uint64    `gorm:"column:robot_id"`                                    // 机器人ID
	CorpID      uint64    `gorm:"column:corp_id"`                                     // 企业ID
	StaffID     uint64    `gorm:"column:staff_id"`                                    // 员工ID
	OriginalUrl string    `gorm:"column:original_url"`                                // 原始url
	ExternalUrl string    `gorm:"column:external_url"`                                // 对外url
	IsDeleted   uint32    `gorm:"column:is_deleted"`                                  // 是否删除(1未删除 2已删除）
	UpdateTime  time.Time `gorm:"column:update_time;type:datetime(0);autoUpdateTime"` // 更新时间
	CreateTime  time.Time `gorm:"column:create_time;type:datetime(0);autoCreateTime"` // 响应时间
}

// TableName 实时文档切片图片表名
func (TRealtimeDocSegmentImage) TableName() string {
	return "t_realtime_doc_segment_image"
}

// ParseDocReqChan 文档解析请求chan
type ParseDocReqChan struct {
	Type      knowledge.StreamSaveDocReq_ReqType // 请求类型
	Doc       TRealtimeDoc                       // 请求文档
	ModelName string                             // 模型名称
}

// ParseDocRspChan 文档解析响应chan
type ParseDocRspChan struct {
	Type          knowledge.StreamSaveDocRsp_RspType // 响应类型
	SessionID     string                             // 会话SessionID
	CosUrlID      uint64                             // Cos URL 唯一ID
	Status        knowledge.TaskRsp_StatusType       // 解析状态
	Progress      knowledge.Progress                 // 解析进度
	DocID         uint64                             // 文档ID，当Status为TaskRsp_SUCCESS有效
	ErrMsg        string                             // 失败信息
	Summary       string                             // 文档摘要
	StatisticInfo *knowledge.StatisticInfo           // 统计信息
}

// TMsgDocRecord 消息文件映射表
type TMsgDocRecord struct {
	ID         uint64    `gorm:"column:id"`         // 自增ID
	BotBizID   uint64    `gorm:"column:robot_id"`   // 机器人ID
	SessionID  string    `gorm:"column:session_id"` // 会话SessionID
	FileURL    string    `gorm:"column:file_url"`   // 文档url
	DocID      uint64    `gorm:"column:doc_id"`     // 文档id
	Summary    string    `gorm:"column:summary"`    // 摘要
	CosID      uint64    `gorm:"column:cos_id"`
	DocName    string    `gorm:"column:doc_name"`                                    // 文档名
	IsDeleted  uint32    `gorm:"column:is_deleted"`                                  // 是否删除
	UpdateTime time.Time `gorm:"column:update_time;type:datetime(0);autoUpdateTime"` // 更新时间
	CreateTime time.Time `gorm:"column:create_time;type:datetime(0);autoCreateTime"` // 创建时间
}

func (TMsgDocRecord) TableName() string {
	return "t_msg_doc_record"
}
