package task

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/common/v3/json"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	SyncDbSourceVdbIndexPrefix = "sync:dbSource:vdb:index:"
)

var SyncDbSourceVdbIndexCache app.UpgradeCache

// SyncDbSourceVdbIndexScheduler 同步向量数据库索引
type SyncDbSourceVdbIndexScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.SyncDbSourceVdbIndexParams
}

func initSyncDbSourceVdbIndexScheduler() {
	task_scheduler.Register(
		model.SyncDbSourceVdbIndexTask,
		func(t task_scheduler.Task, params model.SyncDbSourceVdbIndexParams) task_scheduler.TaskHandler {
			return &SyncDbSourceVdbIndexScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
	SyncDbSourceVdbIndexCache.UpgradeType = app.SyncDbSourceVdbIndexUpgrade
	SyncDbSourceVdbIndexCache.ExpiredTimeS = app.DefaultUpgradeCacheExpiredS
}

// Prepare 数据准备
func (d *SyncDbSourceVdbIndexScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	log.InfoContextf(ctx, "task(SyncDbSourceVdbIndex) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	var allRobotInfos []dao.AppEmbeddingInfo
	var err error

	ids := d.p.RobotIDs
	if len(d.p.RobotIDs) == 0 {
		// 为空则查找所有的未删除、未升级的robot id
		allRobotInfos, err = dao.GetRobotDao().GetAllValidAppEmbeddingInfos(ctx, 0)
		if err != nil {
			return kv, nil
		}
	} else {
		allRobotInfos, err = dao.GetRobotDao().GetAllValidAppEmbeddingInfos(ctx, d.p.RobotIDs[0])
		if err != nil {
			return kv, nil
		}
	}
	allRobotIDs := make([]uint64, 0, len(allRobotInfos))
	for _, info := range allRobotInfos {
		allRobotIDs = append(allRobotIDs, info.AppID)
	}
	ids = allRobotIDs
	pendingIDs, err := SyncDbSourceVdbIndexCache.GetNotUpgradedApps(ctx, ids)
	if err != nil {
		return kv, err
	}
	for _, info := range allRobotInfos {
		encodeJSON, err := json.EncodeJSON(info)
		if err != nil {
			log.ErrorContextf(ctx, "encode json error: %v", err)
			continue
		}
		kv[SyncDbSourceVdbIndexPrefix+cast.ToString(info.AppID)] = encodeJSON
	}
	log.InfoContextf(ctx, "task(SyncDbSourceVdbIndex) prepare finish, robot id count %v", len(pendingIDs))
	return kv, nil
}

// Init 初始化
func (d *SyncDbSourceVdbIndexScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	return nil
}

// Process 任务处理
func (d *SyncDbSourceVdbIndexScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		var info dao.AppEmbeddingInfo
		err := json.DecodeJSON(v, &info)
		if err != nil {
			return err
		}
		log.DebugContextf(ctx, "SyncDbSourceVdbIndexScheduler|sync attribute for robot %v, ", info)

		req := &retrieval.CreateIndexReq{
			RobotId:          info.AppID,
			IndexId:          model.DbSourceVersionID,
			EmbeddingVersion: info.EmbeddingVersion,
			DocType:          model.DocTypeSegment,
			BotBizId:         info.AppBizID,
		}

		_, err = retrieval.NewDirectIndexClientProxy().CreateIndex(ctx, req)
		if err != nil {
			log.ErrorContextf(ctx, "SyncDbSourceVdbIndexScheduler|req :%v input vdb err: %v", req, err)
			return err
		}

		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "finish %v error", k)
			return err
		}

		err = SyncDbSourceVdbIndexCache.SetAppFinish(ctx, info.AppID)
		if err != nil {
			log.ErrorContextf(ctx, "SetAppFinish: error: %v, info:%v", err, info)
		}
		log.InfoContextf(ctx, "robot %v upgrade success, cost: %vms",
			info.AppID, time.Now().Sub(t0).Milliseconds())
		// 每个robot之间增加延时，防止对数据库、redis和es压力过大
		if d.p.DelayMs == 0 {
			d.p.DelayMs = defaultSleepMsEachRobot
		}
		time.Sleep(time.Duration(d.p.DelayMs) * time.Millisecond)
	}
	return nil
}

// Stop 任务停止
func (d *SyncDbSourceVdbIndexScheduler) Stop(ctx context.Context) error {
	log.InfoContextf(ctx, "task(SyncDbSourceVdbIndex) stopped")
	return nil
}

// Done 任务完成回调
func (d *SyncDbSourceVdbIndexScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(SyncDbSourceVdbIndex) finish, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}

// Fail 任务失败
func (d *SyncDbSourceVdbIndexScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "task(SyncDbSourceVdbIndex) fail, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}
