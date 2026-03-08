package entity

import (
	"context"
	"encoding/json"
	"git.woa.com/adp/common/x/logx"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/pb-go/common"
	"google.golang.org/protobuf/proto"
)

// 模型参数
const (
	// ModelParamsNameTemperature 温度
	ModelParamsNameTemperature = "temperature"
	// ModelParamsNameIntentRecognizeTemperature 意图识别温度
	ModelParamsNameIntentRecognizeTemperature = "intent_recognize_temperature"
	// ModelParamsNameTopP top_p
	ModelParamsNameTopP = "top_p"
	// ModelParamsNameIntentRecognizeTopP 意图识别top_p
	ModelParamsNameIntentRecognizeTopP = "intent_recognize_top_p"
	// ModelParamsNameSeed seed
	ModelParamsNameSeed = "seed"
	// ModelParamsNamePresencePenalty 存在惩罚
	ModelParamsNamePresencePenalty = "presence_penalty"
	// ModelParamsNameFrequencyPenalty 频率惩罚
	ModelParamsNameFrequencyPenalty = "frequency_penalty"
	// ModelParamsNameRepetitionPenalty 重复惩罚
	ModelParamsNameRepetitionPenalty = "repetition_penalty"
	// ModelParamsNameMaxTokens max_tokens
	ModelParamsNameMaxTokens = "max_tokens"
)

const (
	DefaultNL2SQLModel = "lke-deepseek-v3-0324"
)

func GetModelParamsFromRules(ctx context.Context, rules []*common.ModelParamRule) *common.ModelParams {
	modelParams := &common.ModelParams{}
	for _, v := range rules {
		switch v.Name {
		case ModelParamsNameTemperature:
			modelParams.Temperature = proto.Float32(v.Default)
		case ModelParamsNameTopP:
			modelParams.TopP = proto.Float32(v.Default)
		case ModelParamsNameSeed:
			modelParams.Seed = proto.Int32(int32(v.Default))
		case ModelParamsNamePresencePenalty:
			modelParams.PresencePenalty = proto.Float32(v.Default)
		case ModelParamsNameFrequencyPenalty:
			modelParams.FrequencyPenalty = proto.Float32(v.Default)
		case ModelParamsNameRepetitionPenalty:
			modelParams.RepetitionPenalty = proto.Float32(v.Default)
		case ModelParamsNameMaxTokens:
			modelParams.MaxTokens = proto.Int32(int32(v.Default))
		default:
			logx.W(ctx, "未处理的模型参数名称: %v", v.Name)
		}
	}
	return modelParams
}

func GetModelParamsFromStr(ctx context.Context, str string) *common.ModelParams {
	if str == "" {
		return nil
	}
	modelParams := &common.ModelParams{}
	var params []kbe.ModelParam
	err := json.Unmarshal([]byte(str), &params)
	if err != nil {
		logx.E(ctx, "GetModelParamsFromStr json.Unmarshal err: %+v", err)
	}
	for _, param := range params {
		switch param.Name {
		case ModelParamsNameTemperature:
			switch value := param.Default.(type) {
			case float64:
				modelParams.Temperature = proto.Float32(float32(value))
			default:
				modelParams.Temperature = proto.Float32(0)
			}
		case ModelParamsNameTopP:
			switch value := param.Default.(type) {
			case float64:
				modelParams.TopP = proto.Float32(float32(value))
			default:
				modelParams.TopP = proto.Float32(0)
			}
		case ModelParamsNameSeed:
			switch value := param.Default.(type) {
			case float64:
				modelParams.Seed = proto.Int32(int32(value))
			default:
				modelParams.Seed = proto.Int32(0)
			}
		case ModelParamsNamePresencePenalty:
			switch value := param.Default.(type) {
			case float64:
				modelParams.PresencePenalty = proto.Float32(float32(value))
			default:
				modelParams.PresencePenalty = proto.Float32(0)
			}
		case ModelParamsNameFrequencyPenalty:
			switch value := param.Default.(type) {
			case float64:
				modelParams.FrequencyPenalty = proto.Float32(float32(value))
			default:
				modelParams.FrequencyPenalty = proto.Float32(0)
			}
		case ModelParamsNameRepetitionPenalty:
			switch value := param.Default.(type) {
			case float64:
				modelParams.PresencePenalty = proto.Float32(float32(value))
			default:
				modelParams.PresencePenalty = proto.Float32(0)
			}
		case ModelParamsNameMaxTokens:
			switch value := param.Default.(type) {
			case float64:
				modelParams.MaxTokens = proto.Int32(int32(value))
			default:
				modelParams.MaxTokens = proto.Int32(0)
			}
		default:
			logx.W(ctx, "未处理的模型参数名称: %v", param.Name)
		}

	}
	return modelParams
}
