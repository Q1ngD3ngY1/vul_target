package client

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"
)

// Counter 上报计数到数据统计服务
func Counter(ctx context.Context, req *statistics.CounterReq) error {
	log.InfoContextf(ctx, "Counter req:%+v", req)
	if req.GetCorpBizId() == 0 {
		log.ErrorContextf(ctx, "Counter corpBizId is 0")
		return errs.ErrSystem
	}
	if req.GetSpaceId() == "" {
		log.ErrorContextf(ctx, "Counter spaceId is empty")
		return errs.ErrSystem
	}
	_, err := statisticsApiCli.Counter(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "Counter Failed, err:%+v", err)
		return err
	}
	return nil
}
