// Package model 定义数据模型或方法，不应包含业务逻辑
package model

import (
	"context"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// 通用定义
const (
	// NextActionAdd 新增
	NextActionAdd = uint32(1)
	// NextActionUpdate 更新
	NextActionUpdate = uint32(2)
	// NextActionDelete 删除
	NextActionDelete = uint32(3)
	// NextActionPublish 发布
	NextActionPublish = uint32(4)

	// RunEnvSandbox 沙箱环境
	RunEnvSandbox = 1
	// RunEnvPRODUCT 正式环境
	RunEnvPRODUCT = 2

	// EnvTypeSandbox 沙箱环境
	EnvTypeSandbox = "sandbox"
	// EnvTypeProduct 正式环境
	EnvTypeProduct = "product"

	// TaskStatusInit 待处理
	TaskStatusInit = uint32(0)
	// TaskStatusProcessing 处理中
	TaskStatusProcessing = uint32(1)
	// TaskStatusSuccess 处理成功
	TaskStatusSuccess = uint32(2)
	// TaskStatusFailed 处理失败
	TaskStatusFailed = uint32(3)
	// TaskStatusStop 处理中止
	TaskStatusStop = uint32(4)
)

// 任务状态码，表示任务处于当前状态的具体原因
// 命名规则：TaskStatus + 3位序号
// 如 3001：处理失败的某种具体状态
// 如 4002：处理中止的某种具体状态
const (
	// TaskStatusFailCodeDocCountLimit 文档数量超出限制
	TaskStatusFailCodeDocCountLimit = uint32(3001)

	// TaskStatusStopCodeModelQuoteLimit 模型额度不足
	TaskStatusStopCodeModelQuoteLimit = uint32(4001)
)

var TaskStatusMap = map[uint32]string{
	TaskStatusInit:       "processing", // 任务提交，但是还没有开始执行的状态，这里也当做执行中返回
	TaskStatusProcessing: "processing",
	TaskStatusSuccess:    "success",
	TaskStatusFailed:     "failed",
	TaskStatusStop:       "stop",
}

var Scenes2AttrLabelEnvType = map[uint32]string{
	RunEnvSandbox: AttributeLabelsPreview,
	RunEnvPRODUCT: AttributeLabelsProd,
}

var Scene2EnvType = map[uint32]string{
	uint32(pb.SceneType_TEST): EnvTypeSandbox,
	uint32(pb.SceneType_PROD): EnvTypeProduct,
}

// TaskStatusInt2Str 任务状态转换成字符串
func TaskStatusInt2Str(status uint32) string {
	if _, ok := TaskStatusMap[status]; ok {
		return TaskStatusMap[status]
	}
	// 默认返回初始状态
	return TaskStatusMap[TaskStatusInit]
}

// ReleaseAction 发布动作
type ReleaseAction int

const (
	// ReleaseActionAdd 新增
	ReleaseActionAdd = 1
	// ReleaseActionUpdate 更新
	ReleaseActionUpdate = 2
	// ReleaseActionDelete 删除
	ReleaseActionDelete = 3
	// ReleaseActionPublish 发布
	ReleaseActionPublish = 4
)

// ReleaseStatus 发布状态
type ReleaseStatus int

const (
	// ReleaseStatusUnreleased 待发布
	ReleaseStatusUnreleased = 1
	// ReleaseStatusReleasing 发布中
	ReleaseStatusReleasing = 2
	// ReleaseStatusReleased 已发布
	ReleaseStatusReleased = 3
	// ReleaseStatusFailed 发布失败
	ReleaseStatusFailed = 4
)

type DbTableStatus int

const (
	// LearnStatusUnlearned 未学习
	LearnStatusUnlearned = 1
	// LearnStatusLearning 学习中
	LearnStatusLearning = 2
	// LearnStatusLearned 已学习
	LearnStatusLearned = 3
	// LearnStatusFailed 学习失败
	LearnStatusFailed = 4

	// FaceStatusLearning 界面展示状态，学习中
	FaceStatusLearning = 5
	// FaceStatusLearnFailed 界面展示状态， 学习失败
	FaceStatusLearnFailed = 6
	// FaceStatusLearnSuccess 界面展示状态， 学习成功, 用于共享知识库
	FaceStatusLearnSuccess = 7
)

// GetLoginUinAndSubAccountUin 获取uin和subAccountUin
func GetLoginUinAndSubAccountUin(ctx context.Context) (string, string) {
	uin := pkg.LoginUin(ctx)
	subAccountUin := pkg.LoginSubAccountUin(ctx)
	if pkg.SID(ctx) == CloudSID {
		uin = pkg.Uin(ctx)
		subAccountUin = pkg.SubAccountUin(ctx)
	}
	return uin, subAccountUin
}

const (
	// DefaultSpaceID 默认空间ID
	DefaultSpaceID = "default_space"
)

const (
	UserResourceMaxCount = 10
)
