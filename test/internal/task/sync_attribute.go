package task

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	syncAttributePrefix      = "sync:attribute:"
	defaultSleepMsEachRobot  = 5
	getAttrChunkSize         = 200
	defaultAddLabelChunkSize = 5
)

var syncAttrUpgradeCache app.UpgradeCache

// SyncAttributeScheduler 文档处理任务
type SyncAttributeScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.SyncAttributeParams
}

func initSyncAttributeScheduler() {
	task_scheduler.Register(
		model.SyncAttributeTask,
		func(t task_scheduler.Task, params model.SyncAttributeParams) task_scheduler.TaskHandler {
			return &SyncAttributeScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
	syncAttrUpgradeCache.UpgradeType = app.SyncAttributeUpgrade
	syncAttrUpgradeCache.ExpiredTimeS = app.DefaultUpgradeCacheExpiredS
}

// Prepare 数据准备
func (d *SyncAttributeScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	log.InfoContextf(ctx, "task(SyncAttribute) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	ids := d.p.RobotIDs
	if len(d.p.RobotIDs) == 0 {
		// 为空则查找所有的未删除、未升级的robot id
		allRobotIDs, err := dao.GetRobotDao().GetAllValidAppIDs(ctx, 0)
		if err != nil {
			return kv, nil
		}
		ids = allRobotIDs
	}
	pendingIDs, err := syncAttrUpgradeCache.GetNotUpgradedApps(ctx, ids)
	if err != nil {
		return kv, err
	}
	for _, id := range pendingIDs {
		kv[syncAttributePrefix+cast.ToString(id)] = cast.ToString(id)
	}
	log.InfoContextf(ctx, "task(SyncAttribute) prepare finish, robot id count %v", len(pendingIDs))
	return kv, nil
}

// Init 初始化
func (d *SyncAttributeScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	return nil
}

// Process 任务处理
func (d *SyncAttributeScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		robotID := cast.ToUint64(v)
		log.InfoContextf(ctx, "sync attribute for robot %v, ", robotID)
		selectColumns := []string{dao.AttributeTblColId, dao.AttributeTblColName, dao.AttributeLabelTblColUpdateTime}

		// 1. 查询robot的attribute，属性数量最大100，不做分批
		isDeleted := dao.IsNotDeleted
		attributeFilter := &dao.AttributeFilter{
			RobotId:   robotID,
			IsDeleted: &isDeleted,
			Limit:     uint32(config.App().AttributeLabel.AttrLimit),
		}
		attributes, err := dao.GetAttributeDao().GetAttributeList(ctx, selectColumns, attributeFilter)
		if err != nil {
			return err
		}
		for _, attr := range attributes {
			// 2. 将attribute写入es的attribute索引
			err = dao.AddAndUpdateAttribute(ctx, robotID, attr)
			if err != nil {
				return err
			}

			// 3. 查询 attribute_label并写入es attribute_label 索引
			err = d.syncAttributeLabelToEs(ctx, robotID, attr.ID)
		}

		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "finish %v error", k)
			return err
		}
		_ = syncAttrUpgradeCache.SetAppFinish(ctx, robotID)
		log.InfoContextf(ctx, "robot %v upgrade success, attribute: %v, cost: %vms",
			robotID, len(attributes), time.Now().Sub(t0).Milliseconds())
		// 每个robot之间增加延时，防止对数据库、redis和es压力过大
		if d.p.DelayMs == 0 {
			d.p.DelayMs = defaultSleepMsEachRobot
		}
		time.Sleep(time.Duration(d.p.DelayMs) * time.Millisecond)
	}
	return nil
}

func (d *SyncAttributeScheduler) syncAttributeLabelToEs(ctx context.Context, robotID, attrID uint64) error {
	limit := getAttrChunkSize
	startID := uint64(0)
	selectColumns := []string{dao.AttributeLabelTblColId, dao.AttributeLabelTblColName,
		dao.AttributeLabelTblColSimilarLabel, dao.AttributeLabelTblColUpdateTime}
	for {
		attributeLabelList, err := dao.GetAttributeLabelDao().GetAttributeLabelChunkByAttrID(ctx, selectColumns, robotID, attrID, startID, limit)
		if err != nil {
			return nil
		}
		chunkSize := d.p.AddLabelChunkSize
		if chunkSize == 0 {
			chunkSize = defaultAddLabelChunkSize
		}
		for _, attributeLabelChunk := range slicex.Chunk(attributeLabelList, chunkSize) {
			err = dao.BatchAddAndUpdateAttributeLabels(ctx, robotID, attrID, attributeLabelChunk)
			if err != nil {
				return err
			}
		}
		if len(attributeLabelList) < limit {
			break
		}
		startID = attributeLabelList[len(attributeLabelList)-1].ID
	}
	return nil
}

// Stop 任务停止
func (d *SyncAttributeScheduler) Stop(ctx context.Context) error {
	log.InfoContextf(ctx, "task(SyncAttribute) stopped")
	return nil
}

// Done 任务完成回调
func (d *SyncAttributeScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(SyncAttribute) finish, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}

// Fail 任务失败
func (d *SyncAttributeScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "task(SyncAttribute) fail, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}
