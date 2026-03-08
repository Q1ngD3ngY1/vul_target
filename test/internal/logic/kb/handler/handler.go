package handler

import (
	"fmt"

	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	internalRPC "git.woa.com/adp/kb/kb-config/internal/rpc"
)

const (
	CorpRobotIDDeleteHandler      = "CORP_ROBOT_ID"     // 通用删除逻辑
	RobotIDDeleteHandler          = "ROBOT_ID"          // 应用ID删除逻辑
	LabelDeleteHandler            = "LABEL"             // 标签删除逻辑
	DocDeleteHandler              = "DOC"               // 文档删除逻辑
	EvaluateDeleteHandler         = "EVALUATE"          // 评测删除逻辑
	UnsatisfiedReplyDeleteHandler = "UNSATISFIED_REPLY" // 不满意回复删除逻辑
	CorpRobotBizIDDeleteHandler   = "CORP_ROBOT_BIZ_ID" // 通用BizID删除逻辑
	CorpAppBizIDDeleteHandler     = "CORP_APP_BIZ_ID"   // 通用BizID删除逻辑

)

// GetDeleteHandler 获取handler
func GetDeleteHandler(handlerType string, r *internalRPC.RPC, kbDao kbdao.Dao, docDao docDao.Dao) (DeleteHandler, error) {
	switch handlerType {
	case CorpRobotIDDeleteHandler:
		return NewCorpRobotIDHandler(kbDao), nil
	case LabelDeleteHandler:
		return NewLabelHandler(kbDao), nil
	case DocDeleteHandler:
		return NewDocHandler(r, docDao), nil
	case RobotIDDeleteHandler:
		return NewRobotIDHandler(kbDao), nil
	case UnsatisfiedReplyDeleteHandler:
		return NewUnsatisfiedReplyHandler(kbDao), nil
	case CorpRobotBizIDDeleteHandler:
		return NewCorpRobotBizIDHandler(kbDao, r), nil
	case CorpAppBizIDDeleteHandler:
		return NewCorpAppBizIDHandler(kbDao, r), nil
	default:
		return nil, fmt.Errorf("unknown handler type: %s", handlerType)
	}
}
