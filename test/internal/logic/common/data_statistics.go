package common

import (
	"context"
	"time"

	"git.woa.com/adp/pb-go/common"
	dataStat "git.woa.com/adp/pb-go/platform/platform_metrology"
	"github.com/google/uuid"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

type CounterInfo struct {
	CorpBizId       uint64
	SpaceId         string
	AppBizId        uint64
	StatisticObject common.StatObject
	StatisticType   common.StatType
	ObjectId        string
	ObjectName      string
	Count           uint64
}

// Counter 上报统计数据
func Counter(ctx context.Context, counterInfo *CounterInfo, r *rpc.RPC) {
	logx.I(ctx, "CounterInfo: %+v", counterInfo)
	if counterInfo == nil {
		logx.E(ctx, "CounterInfo is nil")
		return
	}
	if counterInfo.CorpBizId == 0 || counterInfo.SpaceId == "" || counterInfo.ObjectName == "" {
		if counterInfo.AppBizId == 0 {
			logx.E(ctx, "CounterInfo.CorpBizId and CounterInfo.SpaceId and CounterInfo.AppBizId is empty")
			return
		}
		// 通过app信息获取corpBizId和spaceId
		app, err := r.DescribeAppById(ctx, counterInfo.AppBizId)
		if err != nil {
			logx.E(ctx, "GetAppInfo err: %+v", err)
			return
		}
		counterInfo.CorpBizId = app.CorpBizId
		counterInfo.SpaceId = app.SpaceId
		counterInfo.ObjectName = app.Name
	}

	counterItemList := []*dataStat.CounterItem{
		{
			Statistic: &dataStat.Statistic{
				StatObject: counterInfo.StatisticObject,
				StatType:   counterInfo.StatisticType,
			},
			AppBizId:   counterInfo.AppBizId,
			ObjectId:   counterInfo.ObjectId,
			ObjectName: counterInfo.ObjectName,
			Count:      counterInfo.Count,
		},
	}
	counterReq := &dataStat.CounterReq{
		UniqueKey:       uuid.NewString(),
		CorpBizId:       counterInfo.CorpBizId,
		SpaceId:         counterInfo.SpaceId,
		StatTime:        time.Now().Format(time.RFC3339),
		CounterItemList: counterItemList,
	}
	err := r.Counter(ctx, counterReq)
	if err != nil {
		logx.E(ctx, "Counter, counterReq: %+v, error: %+v", counterReq, err)
	}
}
