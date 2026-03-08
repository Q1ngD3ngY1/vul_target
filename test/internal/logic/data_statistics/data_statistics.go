package data_statistics

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"
)

type CounterInfo struct {
	CorpBizId       uint64
	SpaceId         string
	AppBizId        uint64
	StatisticObject statistics.StatObject
	StatisticType   statistics.StatType
	ObjectId        string
	ObjectName      string
	Count           uint64
}

// Counter 上报统计数据
func Counter(ctx context.Context, counterInfo *CounterInfo) {
	log.InfoContextf(ctx, "CounterInfo: %+v", counterInfo)
	if counterInfo == nil {
		log.ErrorContextf(ctx, "CounterInfo is nil")
		return
	}
	if counterInfo.CorpBizId == 0 || counterInfo.SpaceId == "" || counterInfo.ObjectName == "" {
		if counterInfo.AppBizId == 0 {
			log.ErrorContextf(ctx, "CounterInfo.CorpBizId and CounterInfo.SpaceId and CounterInfo.AppBizId is empty")
			return
		}
		// 通过app信息获取corpBizId和spaceId
		app, err := client.GetAppInfo(ctx, counterInfo.AppBizId, model.AppTestScenes)
		if err != nil {
			log.ErrorContextf(ctx, "GetAppInfo err: %+v", err)
			return
		}
		counterInfo.CorpBizId = app.GetCorpBizId()
		counterInfo.SpaceId = app.GetSpaceId()
		counterInfo.ObjectName = app.GetBaseConfig().GetName()
	}

	counterItemList := []*statistics.CounterItem{
		{
			Statistic: &statistics.Statistic{
				StatObject: counterInfo.StatisticObject,
				StatType:   counterInfo.StatisticType,
			},
			AppBizId:   counterInfo.AppBizId,
			ObjectId:   counterInfo.ObjectId,
			ObjectName: counterInfo.ObjectName,
			Count:      counterInfo.Count,
		},
	}
	counterReq := &statistics.CounterReq{
		UniqueKey:       utils.NewUUID(),
		CorpBizId:       counterInfo.CorpBizId,
		SpaceId:         counterInfo.SpaceId,
		StatTime:        time.Now().Format(time.RFC3339),
		CounterItemList: counterItemList,
	}
	err := client.Counter(ctx, counterReq)
	if err != nil {
		log.ErrorContextf(ctx, "Counter, counterReq: %+v, error: %+v", counterReq, err)
	}
}
