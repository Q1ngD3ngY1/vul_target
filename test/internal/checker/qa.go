package checker

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	VerifyQaReqMaxQaListLen = 1000
)

func VerifyQaChecker(ctx context.Context, req *pb.VerifyQAReq) (uint64, error) {
	botBizId, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return 0, err
	}
	qaList := req.GetList()
	if len(qaList) > VerifyQaReqMaxQaListLen {
		return 0, errs.ErrReqQaListExceedLimit
	}
	return botBizId, nil
}
