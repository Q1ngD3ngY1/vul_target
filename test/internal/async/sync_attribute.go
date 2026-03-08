package async

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	labelDao "git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/logic/app"
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

// SyncAttributeTaskHandler 文档处理任务
type SyncAttributeTaskHandler struct {
	*taskCommon

	task     task_scheduler.Task
	p        entity.SyncAttributeParams
	labelDao labelDao.Dao
}

func registerSyncAttributeTaskHandler(tc *taskCommon, labelDao labelDao.Dao) {
	task_scheduler.Register(
		entity.SyncAttributeTask,
		func(t task_scheduler.Task, params entity.SyncAttributeParams) task_scheduler.TaskHandler {
			return &SyncAttributeTaskHandler{
				taskCommon: tc,
				labelDao:   labelDao,
				task:       t,
				p:          params,
			}
		},
	)
	syncAttrUpgradeCache.UpgradeType = app.SyncAttributeUpgrade
	syncAttrUpgradeCache.ExpiredTimeS = app.DefaultUpgradeCacheExpiredS
	syncAttrUpgradeCache.Rdb = tc.adminRdb
}

// Prepare 数据准备
func (d *SyncAttributeTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	logx.I(ctx, "task(SyncAttribute) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	ids := d.p.RobotIDs
	if len(d.p.RobotIDs) == 0 {
		// 为空则查找所有的未删除、未升级的robot id
		apps, _, err := d.rpc.AppAdmin.ListAllAppBaseInfo(ctx, nil)
		if err != nil {
			logx.W(ctx, "ListAllAppBaseInfo failed, err: %v", err)
			return nil, err
		}
		ids = slicex.Pluck(apps, func(v *entity.AppBaseInfo) uint64 { return v.PrimaryId })
		// allRobotIDs, err := dao.GetRobotDao().GetAllValidAppIDs(ctx, 0)
		// if err != nil {
		// 	return kv, nil
		// }
		// ids = allRobotIDs
	}
	pendingIDs, err := syncAttrUpgradeCache.GetNotUpgradedApps(ctx, ids)
	if err != nil {
		return kv, err
	}
	for _, id := range pendingIDs {
		kv[syncAttributePrefix+cast.ToString(id)] = cast.ToString(id)
	}
	logx.I(ctx, "task(SyncAttribute) prepare finish, robot id count %v", len(pendingIDs))
	return kv, nil
}

// Init 初始化
func (d *SyncAttributeTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	return nil
}

// Process 任务处理
func (d *SyncAttributeTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		robotID := cast.ToUint64(v)
		logx.I(ctx, "sync attribute for robot %v, ", robotID)
		selectColumns := []string{labelEntity.AttributeTblColId, labelEntity.AttributeTblColName, labelEntity.AttributeLabelTblColUpdateTime}

		// 1. 查询robot的attribute，属性数量最大100，不做分批
		attributeFilter := &labelEntity.AttributeFilter{
			RobotId:   robotID,
			IsDeleted: ptrx.Bool(false),
			Limit:     config.App().AttributeLabel.AttrLimit,
		}
		attributes, err := d.labelLogic.GetAttributeList(ctx, selectColumns, attributeFilter)
		if err != nil {
			return err
		}
		for _, attr := range attributes {
			// 2. 将attribute写入es的attribute索引
			err = d.labelDao.AddAndUpdateAttribute(ctx, robotID, attr)
			if err != nil {
				return err
			}

			// 3. 查询 attribute_label并写入es attribute_label 索引
			err = d.syncAttributeLabelToEs(ctx, robotID, attr.ID)
		}

		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "finish %v error", k)
			return err
		}
		_ = syncAttrUpgradeCache.SetAppFinish(ctx, robotID)
		logx.I(ctx, "robot %v upgrade success, attribute: %v, cost: %vms",
			robotID, len(attributes), time.Now().Sub(t0).Milliseconds())
		// 每个robot之间增加延时，防止对数据库、redis和es压力过大
		if d.p.DelayMs == 0 {
			d.p.DelayMs = defaultSleepMsEachRobot
		}
		time.Sleep(time.Duration(d.p.DelayMs) * time.Millisecond)
	}
	return nil
}

func (d *SyncAttributeTaskHandler) syncAttributeLabelToEs(ctx context.Context, robotID, attrID uint64) error {
	limit := getAttrChunkSize
	startID := uint64(0)
	selectColumns := []string{labelEntity.AttributeLabelTblColId, labelEntity.AttributeLabelTblColName,
		labelEntity.AttributeLabelTblColSimilarLabel, labelEntity.AttributeLabelTblColUpdateTime}
	for {
		attributeLabelList, err := d.labelDao.GetAttributeLabelChunkByAttrID(ctx, selectColumns, robotID, attrID, startID, limit)
		if err != nil {
			return nil
		}
		chunkSize := d.p.AddLabelChunkSize
		if chunkSize == 0 {
			chunkSize = defaultAddLabelChunkSize
		}
		for _, attributeLabelChunk := range slicex.Chunk(attributeLabelList, chunkSize) {
			err = d.labelDao.BatchAddAndUpdateAttributeLabels(ctx, robotID, attrID, attributeLabelChunk)
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
func (d *SyncAttributeTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "task(SyncAttribute) stopped")
	return nil
}

// Done 任务完成回调
func (d *SyncAttributeTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(SyncAttribute) finish, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}

// Fail 任务失败
func (d *SyncAttributeTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "task(SyncAttribute) fail, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}
