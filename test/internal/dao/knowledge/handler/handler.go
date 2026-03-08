package handler

import (
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/knowledge"
)

const (
	CorpRobotIDDeleteHandler      = "CORP_ROBOT_ID"     // 通用删除逻辑
	RobotIDDeleteHandler          = "ROBOT_ID"          // 应用ID删除逻辑
	LabelDeleteHandler            = "LABEL"             // 标签删除逻辑
	EvaluateDeleteHandler         = "EVALUATE"          // 评测删除逻辑
	UnsatisfiedReplyDeleteHandler = "UNSATISFIED_REPLY" // 不满意回复删除逻辑
	CorpRobotBizIDDeleteHandler   = "CORP_ROBOT_BIZ_ID" // 通用BizID删除逻辑
	CorpAppBizIDDeleteHandler     = "CORP_APP_BIZ_ID"   // 通用BizID删除逻辑
)

// GetDeleteHandler 获取handler
func GetDeleteHandler(handlerType string) (knowledge.DeleteHandler, error) {
	switch handlerType {
	case CorpRobotIDDeleteHandler:
		return NewCorpRobotIDHandler(), nil
	case EvaluateDeleteHandler:
		return NewEvaluateHandler(), nil
	case LabelDeleteHandler:
		return NewLabelHandler(), nil
	case RobotIDDeleteHandler:
		return NewRobotIDHandler(), nil
	case UnsatisfiedReplyDeleteHandler:
		return NewUnsatisfiedReplyHandler(), nil
	case CorpRobotBizIDDeleteHandler:
		return NewCorpRobotBizIDHandler(), nil
	case CorpAppBizIDDeleteHandler:
		return NewCorpAppBizIDHandler(), nil
	default:
		return nil, fmt.Errorf("unknown handler type: %s", handlerType)
	}
}
