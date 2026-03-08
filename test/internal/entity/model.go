// Package model 定义数据模型或方法，不应包含业务逻辑
package entity

import (
	"git.woa.com/adp/kb/kb-config/internal/entity/label"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	UserResourceMaxCount = 10
)

const (
	// ProviderTypeSelf 提供商类型-自有提供商
	ProviderTypeSelf = "Self"
	// ProviderTypeCustom 提供商类型-自定义模型提供商
	ProviderTypeCustom = "Custom"
	// ProviderTypeThird 提供商类型-第三方提供商
	ProviderTypeThird = "Third"
)

// 通用定义
const (

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
	RunEnvSandbox: label.AttributeLabelsPreview,
	RunEnvPRODUCT: label.AttributeLabelsProd,
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
