package model

import (
	"context"
	"database/sql"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// 记录状态
const (
	JudgeWait  = uint64(0) // 等待
	JudgeRight = uint64(1) // 正确
	JudgeError = uint64(2) // 错误
)

// 任务状态
const (
	TestStatusInit    = uint64(0) // 待评测
	TestStatusRunning = uint64(1) // 评测中
	TestStatusJudging = uint64(2) // 标注中
	TestStatusFinish  = uint64(3) // 标注完成
	TestStatusDeleted = uint64(4) // 已经删除
	TestStatusFail    = uint64(5) // 评测失败
	TestStatusStop    = uint64(6) // 人工停止
)

// 通知文案
const (
	ReasonStop           = i18nkey.KeyManuallyStopped                    // 手动停止
	SubjectSuccess       = i18nkey.KeyBatchTestingCompleted              // 批量测试完成
	SubjectFail          = i18nkey.KeyBatchTestingFailed                 // 批量测试失败
	NoticeContentFail    = i18nkey.KeyBatchTestingFailedWithReason       // 批量测试失败通知
	NoticeContentSuccess = i18nkey.KeyBatchTestingCompletedForAnnotation // 批量测试完成通知
)

// 模型过滤标识
const (
	FilterSearchPreview  = "search_preview"    // 对话测试过滤器类型
	RecordTypeEvaluation = "normal_evaluation" // 评测任务模型类型
)

// RobotTestRecord 测试记录
type RobotTestRecord struct {
	ID              uint64         `db:"id"`
	BusinessID      uint64         `db:"business_id"`
	TestID          uint64         `db:"test_id"`           // 集合ID
	SetID           uint64         `db:"set_id"`            // 集合ID
	Question        string         `db:"question"`          // 样本内容/问题
	Answer          string         `db:"answer"`            // 机器人回复
	AnswerJudge     uint64         `db:"answer_judge"`      // 0 待判断 1 准确 2错误
	Reference       string         `db:"reference"`         // 参考来源明细
	Prompt          string         `db:"prompt"`            // 大模型提示词
	ReplyMethod     int            `db:"reply_method"`      // 回复类型
	RoleDescription sql.NullString `db:"role_description"`  // 角色设定
	TraceID         sql.NullString `db:"trace_id"`          // TraceID
	CreateTime      time.Time      `db:"create_time"`       // 创建时间
	UpdateTime      time.Time      `db:"update_time"`       // 更新时间
	RecordID        string         `db:"record_id"`         // 记录ID
	RelatedRecordID string         `db:"related_record_id"` // 关联记录ID
	CustomVariables sql.NullString `db:"custom_variables"`  // 自定义参数
	MsgRecord       string
}

// RobotTest 测试任务
type RobotTest struct {
	ID            uint64    `db:"id"`
	BusinessID    uint64    `db:"business_id"`     // 业务ID
	RobotID       uint64    `db:"robot_id"`        // 机器人ID
	CorpID        uint64    `db:"corp_id"`         // 企业ID
	TestName      string    `db:"test_name"`       // 任务名
	TestNum       uint64    `db:"test_num"`        // 任务内样本数量
	JudgeNum      uint64    `db:"judge_num"`       // 已经标注的样本数量
	JudgeRightNum uint64    `db:"judge_right_num"` // 标注准确的样本数量
	SetID         uint64    `db:"set_id"`          // 集合ID
	Message       string    `db:"message"`         // 错误信息
	Status        uint64    `db:"status"`          // 0 待评测 1 评测中  2 标注中 3 标注完成 4已经删除 5评测失败 6 人工停止
	TaskID        uint64    `db:"task_id"`         // 任务ID
	CreateStaffID uint64    `db:"create_staff_id"` // 上传用户ID
	CreateTime    time.Time `db:"create_time"`     // 创建时间
	UpdateTime    time.Time `db:"update_time"`     // 更新时间
	TestDoneNum   int       `db:"test_done_num"`   // 已经完成评测数量 -1 历史数据展示 - ,其他为具体数量
}

// ToPB 转PB
func (t *RobotTest) ToPB() *pb.SampleTest {
	return &pb.SampleTest{
		TestId:        t.ID,
		TestName:      t.TestName,
		TestNum:       uint32(t.TestNum),
		JudgeNum:      uint32(t.JudgeNum),
		JudgeRightNum: uint32(t.JudgeRightNum),
		Status:        uint32(t.Status),
		Message:       t.Message,
		CreateTime:    uint64(t.CreateTime.Unix()),
	}
}

// ToRspList 转RspList
func (t *RobotTest) ToRspList() *pb.SampleTestDetail {
	return &pb.SampleTestDetail{
		TestBizId:        t.BusinessID,
		TestName:         t.TestName,
		TestNumber:       uint32(t.TestNum),
		JudgeNumber:      uint32(t.JudgeNum),
		JudgeRightNumber: uint32(t.JudgeRightNum),
		Status:           uint32(t.Status),
		Message:          t.Message,
		CreateTime:       uint64(t.CreateTime.Unix()),
		TestDoneNumber:   int32(t.TestDoneNum),
	}
}

// PromptModel Prompt构建
type PromptModel struct {
	Docs     any
	Question string
}

// JudgeCount 标注统计
type JudgeCount struct {
	Judge uint64 `db:"answer_judge"`
	Count uint64 `db:"judge_count"`
}

// CreateTestNotice 创建通知
func (t *RobotTest) CreateTestNotice(ctx context.Context) *Notice {
	operations := []Operation{{Typ: OpTypeViewDetail, Params: OpParams{}}}
	subject := i18n.Translate(ctx, SubjectSuccess)
	content := i18n.Translate(ctx, NoticeContentSuccess, t.TestName)
	level := LevelSuccess
	if t.Status == TestStatusFail {
		subject = i18n.Translate(ctx, SubjectFail)
		level = LevelError
		content = i18n.Translate(ctx, NoticeContentFail, t.TestName, t.Message)
	}
	opts := []NoticeOption{
		WithGlobalFlag(),
		WithPageID(NoticeBatchTestPageID),
		WithLevel(level),
		WithSubject(subject),
		WithContent(content),
	}
	notice := NewNotice(NoticeTypeTest, t.ID, t.CorpID, t.RobotID, t.CreateStaffID, opts...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
	}
	return notice
}
