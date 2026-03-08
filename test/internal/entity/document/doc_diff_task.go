package document

import (
	"context"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	DocDiffTaskTableMaxPageSize = 1000
	HandleDocDiffSize           = 50

	DocDiffTaskTblColBusinessId            = "business_id"              // 文档对比ID
	DocDiffTaskTblColCorpBizId             = "corp_biz_id"              // 企业ID
	DocDiffTaskTblColRobotBizId            = "robot_biz_id"             // 应用ID
	DocDiffTaskTblColStaffBizId            = "staff_biz_id"             // 员工ID
	DocDiffTaskTblColNewDocBizId           = "new_doc_biz_id"           // 新文档ID
	DocDiffTaskTblColOldDocBizId           = "old_doc_biz_id"           // 旧文档ID
	DocDiffTaskTblColTaskId                = "task_id"                  // 异步处理任务ID
	DocDiffTaskTblDocQATaskId              = "doc_qa_task_id"           // doc to qa任务ID
	DocDiffTaskTblColNewDocRename          = "new_doc_rename"           // 重命名操作新文件名
	DocDiffTaskTblColOldDocRename          = "old_doc_rename"           // 重命名操作旧文件名
	DocDiffTaskTblColComparisonReason      = "comparison_reason"        // 对比原因
	DocDiffTaskTblColDiffType              = "diff_type"                // 对比类型
	DocDiffTaskTblColDocOperation          = "doc_operation"            // 文档操作类型
	DocDiffTaskTblColDocOperationStatus    = "doc_operation_status"     // 文档操作结果
	DocDiffTaskTblColQaOperation           = "qa_operation"             // 问答操作类型
	DocDiffTaskTblColQaOperationStatus     = "qa_operation_status"      // 问答操作结果
	DocDiffTaskTblColQaOperationResult     = "qa_operation_result"      // 问答操作成功或失败的结果提示
	DocDiffTaskTblColStatus                = "status"                   // 状态
	DocDiffTaskTblColDiffDataProcessStatus = "diff_data_process_status" // 对比数据处理状态
	DocDiffTaskTblColIsDeleted             = "is_deleted"               // 是否删除
	DocDiffTaskTblColCreateTime            = "create_time"              // 创建时间
	DocDiffTaskTblColUpdateTime            = "update_time"              // 更新时间
)

var DocDiffTaskTblColList = []string{
	DocDiffTaskTblColBusinessId,
	DocDiffTaskTblColCorpBizId,
	DocDiffTaskTblColRobotBizId,
	DocDiffTaskTblColStaffBizId,
	DocDiffTaskTblColNewDocBizId,
	DocDiffTaskTblColOldDocBizId,
	DocDiffTaskTblColTaskId,
	DocDiffTaskTblDocQATaskId,
	DocDiffTaskTblColNewDocRename,
	DocDiffTaskTblColOldDocRename,
	DocDiffTaskTblColComparisonReason,
	DocDiffTaskTblColDiffType,
	DocDiffTaskTblColDocOperation,
	DocDiffTaskTblColDocOperationStatus,
	DocDiffTaskTblColQaOperation,
	DocDiffTaskTblColQaOperationStatus,
	DocDiffTaskTblColQaOperationResult,
	DocDiffTaskTblColStatus,
	DocDiffTaskTblColDiffDataProcessStatus,
	DocDiffTaskTblColIsDeleted,
	DocDiffTaskTblColCreateTime,
	DocDiffTaskTblColUpdateTime,
}

type DocDiffStatus uint32

// DiffTaskOperation 定义操作结构体
type DiffTaskOperation struct {
	LevelDiffTypeOperation DiffTypeOperation
	LevelDocOperation      DocOperation
	LevelQaOperation       QAOperation
}

type DocDiffTaskFilter struct {
	BusinessIds    []uint64 // 文档对比ID
	CorpBizId      uint64   // 企业 ID
	RobotBizId     uint64   // 单个应用ID（与RobotBizIds互斥）
	RobotBizIds    []uint64 // 多个应用ID数组（与RobotBizId互斥）
	IsDeleted      *bool
	Statuses       []int32
	InNewOldDocId  []uint64
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
}

type QAOperation uint32

// 具体操作定义参见  【企微文档】文档比对diff问答按钮展示
// https://doc.weixin.qq.com/sheet/e3_ASsAIwY7AJcQtXMwtjaQTyaEXYrHS?scode=AJEAIQdfAAo4NbIbxZABQAagbGABs&tab=bgy5r8
const (
	QAOperation0  = QAOperation(0)
	QAOperation1  = QAOperation(1)
	QAOperation2  = QAOperation(2)
	QAOperation3  = QAOperation(3)
	QAOperation4  = QAOperation(4)
	QAOperation5  = QAOperation(5)
	QAOperation6  = QAOperation(6)
	QAOperation7  = QAOperation(7)
	QAOperation8  = QAOperation(8)
	QAOperation9  = QAOperation(9)
	QAOperation10 = QAOperation(10)
	QAOperation11 = QAOperation(11)
	QAOperation12 = QAOperation(12)
	QAOperation13 = QAOperation(13)
	QAOperation14 = QAOperation(14)
	QAOperation15 = QAOperation(15)
	QAOperation16 = QAOperation(16)
	QAOperation17 = QAOperation(17)
	QAOperation18 = QAOperation(18)
	QAOperation19 = QAOperation(19)
	QAOperation20 = QAOperation(20)
	QAOperation21 = QAOperation(21)
)

// AllowedQaOperations 定义允许的三级操作范围
var AllowedQaOperations = map[DocOperation]map[DiffTypeOperation][]QAOperation{
	DocOperationDeleteOldDoc: {
		Op1: {QAOperation0},
		Op2: {QAOperation7, QAOperation8, QAOperation9, QAOperation10, QAOperation11, QAOperation12},
		Op3: {QAOperation16, QAOperation17, QAOperation18},
		Op4: {QAOperation0},
	},
	DocOperationDeleteNewDoc: {
		Op1: {QAOperation1, QAOperation2, QAOperation3},
		Op2: {QAOperation0},
		Op3: {QAOperation19, QAOperation20, QAOperation21},
		Op4: {QAOperation0},
	},
	DocOperationOldReName: {
		Op1: {QAOperation4, QAOperation5, QAOperation6},
		Op2: {QAOperation13, QAOperation14, QAOperation15},
		Op3: {QAOperation0},
		Op4: {QAOperation0},
	},
	DocOperationNewReName: {
		Op1: {QAOperation4, QAOperation5, QAOperation6},
		Op2: {QAOperation13, QAOperation14, QAOperation15},
		Op3: {QAOperation0},
		Op4: {QAOperation0},
	},
	DocOperationDefault: {
		Op1: {QAOperation4, QAOperation5, QAOperation6},
		Op2: {QAOperation13, QAOperation14, QAOperation15},
		Op3: {QAOperation0},
		Op4: {QAOperation0},
	},
}

// HandleDocDiffTaskValidate 校验处理对比任务类型
func (op *DiffTaskOperation) HandleDocDiffTaskValidate(ctx context.Context) error {
	// 校验diffTypeOperation
	if op.LevelDiffTypeOperation < Op1 || op.LevelDiffTypeOperation > Op4 {
		return errs.ErrHandleDiffTypeOperationFail
	}
	// 校验docOperation
	if op.LevelDocOperation < DocOperationDeleteOldDoc || op.LevelDocOperation > DocOperationDefault {
		return errs.ErrHandleDocOperationFail
	}
	// 校验问答级联可选的问答操作类型
	allowedOperations, exists := AllowedQaOperations[op.LevelDocOperation]
	if !exists {
		return errs.ErrHandleDocOperationFail
	}
	// 检查级联可选的问答操作是否在允许的范围内
	allowedLevelQaOperations, exists := allowedOperations[op.LevelDiffTypeOperation]
	if !exists {
		return errs.ErrHandleQaOperationFail
	}
	// 检查选择的问答操作是否有效
	isValid := false
	for _, allowedOp := range allowedLevelQaOperations {
		if op.LevelQaOperation == allowedOp {
			isValid = true
			break
		}
	}
	if !isValid {
		logx.W(ctx, "HandleDocDiffTaskValidate 校验选择的问答操作:%v 无效", op.LevelDiffTypeOperation)
		return errs.ErrHandleQaOperationFail
	}
	return nil
}

const (
	DocDiffStatusInit       = DocDiffStatus(0)
	DocDiffStatusProcessing = DocDiffStatus(1)
	// DocDiffStatusFinish 不论执行成功还是失败，结果都是Finish
	DocDiffStatusFinish = DocDiffStatus(2)
	// DocDiffStatusInvalid 已失效，比如在一个页面生成了对比任务，但是在另一个页面删除了文档，无法进行对比任务，暂不实现
	DocDiffStatusInvalid = DocDiffStatus(3)
)

// DocDiffQAAndDocOpStatus 文档和问答的操作status
type DocDiffQAAndDocOpStatus uint32

const (
	DocDiffQAAndDocOpStatusProcessing = DocDiffQAAndDocOpStatus(0)
	DocDiffQAAndDocOpStatusSuccess    = DocDiffQAAndDocOpStatus(1)
	DocDiffQAAndDocOpStatusFailed     = DocDiffQAAndDocOpStatus(2)
)

type DocDiff struct {
	BusinessID            uint64    `db:"business_id"          gorm:"column:business_id"`                                                       // 文档对比ID
	CorpBizID             uint64    `db:"corp_biz_id"          gorm:"column:corp_biz_id"`                                                       // 企业ID
	RobotBizID            uint64    `db:"robot_biz_id"         gorm:"column:robot_biz_id"`                                                      // 应用ID
	StaffBizID            uint64    `db:"staff_biz_id"         gorm:"column:staff_biz_id;default:0"`                                            // 员工ID
	NewDocBizID           uint64    `db:"new_doc_biz_id"       gorm:"column:new_doc_biz_id"`                                                    // 新文档ID
	OldDocBizID           uint64    `db:"old_doc_biz_id"       gorm:"column:old_doc_biz_id"`                                                    // 旧文档ID
	TaskID                uint64    `db:"task_id"              gorm:"column:task_id;default:0"`                                                 // 异步处理任务ID
	DocQATaskID           uint64    `db:"doc_qa_task_id"       gorm:"column:doc_qa_task_id;default:0"`                                          // 文档生成qa任务ID
	NewDocRename          string    `db:"new_doc_rename"       gorm:"column:new_doc_rename;default:''"`                                         // 重命名操作新文件名
	OldDocRename          string    `db:"old_doc_rename"       gorm:"column:old_doc_rename;default:''"`                                         // 重命名操作旧文件名
	ComparisonReason      uint32    `db:"comparison_reason"    gorm:"column:comparison_reason"`                                                 // 对比原因
	DiffType              uint32    `db:"diff_type"            gorm:"column:diff_type"`                                                         // 对比类型
	DocOperation          uint32    `db:"doc_operation"        gorm:"column:doc_operation"`                                                     // 文档操作类型
	DocOperationStatus    uint32    `db:"doc_operation_status" gorm:"column:doc_operation_status"`                                              // 文档操作结果(0处理中，1操作成功，2操作失败)
	QaOperation           uint32    `db:"qa_operation"         gorm:"column:qa_operation"`                                                      // 问答操作类型
	QaOperationStatus     uint32    `db:"qa_operation_status"  gorm:"column:qa_operation_status"`                                               // 问答操作结果(0处理中，1操作成功，2操作失败)
	QaOperationResult     string    `db:"qa_operation_result"  gorm:"column:qa_operation_result;default:''"`                                    // 问答操作成功或失败的结果提示
	Status                uint32    `db:"status"               gorm:"column:status;default:0"`                                                  // 0待处理 1处理中 2已完成 3已失效                                           //                                                // 状态
	DiffDataProcessStatus uint32    `db:"diff_data_process_status" gorm:"column:diff_data_process_status"`                                      // 0待处理 1处理中 2已完成 3已失败
	IsDeleted             bool      `db:"is_deleted"           gorm:"column:is_deleted;default:0"`                                              // 是否删除
	CreateTime            time.Time `db:"create_time"          gorm:"column:create_time;default:CURRENT_TIMESTAMP"`                             // 创建时间
	UpdateTime            time.Time `db:"update_time"          gorm:"column:update_time;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
}

type DocDiffListRsp struct {
	DiffId           string
	NewDoc           DocDiffListRspDocInfo
	OldDoc           DocDiffListRspDocInfo
	ComparisonReason uint32
	DiffType         uint32
	DocOperation     uint32
	QaOperation      uint32
	Status           uint32
	CreatedAt        int64
}

type DocDiffListRspDocInfo struct {
	DocBizId  string
	DocName   string
	CreatedAt int64
	QaCount   uint32
	DocUrl    string
	Status    int32
	FileType  string
	IsDeleted bool
}

// FormatDocDiffDocInfo 组装对比任务文档信息
func FormatDocDiffDocInfo(docID uint64, docs map[uint64]*Doc, qaNums map[uint64]map[uint32]uint32) *pb.DocInfo {
	if docs == nil {
		return &pb.DocInfo{}
	}
	if doc, ok := docs[docID]; ok {
		var qaCount uint32
		if qaNums != nil {
			qaCount = qaNums[doc.ID][DocIsDeleted]
		}
		return &pb.DocInfo{
			DocBizId:    strconv.FormatUint(doc.BusinessID, 10),
			DocName:     doc.GetRealFileName(),
			CreateTime:  doc.CreateTime.Unix(),
			QaCount:     qaCount,
			DocUrl:      doc.CosURL,
			Status:      int32(doc.Status),
			FileType:    doc.FileType,
			IsDeleted:   doc.IsDeleted,
			EnableScope: pb.RetrievalEnableScope(doc.EnableScope),
		}
	}
	return &pb.DocInfo{}
}

type SegmentToQAType string

const (
	SegmentToQATypeNone   = "NotExist"
	SegmentToQATypeOldUni = "OldUni"
	SegmentToQATypeOldAll = "OldAll"
	SegmentToQATypeNewUni = "NewUni"
	SegmentToQATypeNewAll = "NewAll"
)

// GetDocDiffSegmentToQAType 是否存在文档片段转QA
func GetDocDiffSegmentToQAType(op QAOperation) SegmentToQAType {
	if op == QAOperation4 {
		return SegmentToQATypeOldUni
	}

	if op == QAOperation5 {
		return SegmentToQATypeOldAll
	}

	if op == QAOperation7 || op == QAOperation8 || op == QAOperation13 {
		return SegmentToQATypeNewUni
	}

	if op == QAOperation9 || op == QAOperation14 {
		return SegmentToQATypeNewAll
	}

	return SegmentToQATypeNone
}
