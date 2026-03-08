package task

import (
	"context"
	"encoding/json"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// EnableDbSourceScheduler db 开启关闭任务
type EnableDbSourceScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.EnableDBSourceParams
}

func initDbSourceScheduler() {
	task_scheduler.Register(
		model.EnableDBSourceTask,
		func(t task_scheduler.Task, params model.EnableDBSourceParams) task_scheduler.TaskHandler {
			return &EnableDbSourceScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *EnableDbSourceScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.InfoContextf(ctx, "task(EnableDbSourceScheduler) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	// 如果 DbTableBizID 不为空，收集单个 db_table 的索引状态
	if d.p.DbTableBizID > 0 {
		ids := []uint64{d.p.DbTableBizID}
		err := updateTask(ctx, kv, 0, ids)
		if err != nil {
			log.ErrorContextf(ctx, "EnableDbSourceScheduler|Prepare| update task %v, err: %v", d.p, err)
			return nil, err
		}
		return kv, nil
	}

	// 如果 DbSourceBizID 不为空，收集 db_source 的索引状态
	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DbSourceBizID)
	if err != nil {
		log.ErrorContextf(ctx, "EnableDbSourceScheduler|Prepare| get db source %v, err: %v", d.p, err)
		return nil, err
	}
	if dbSource.IsIndexed == d.p.Enable {
		log.WarnContextf(ctx, "EnableDbSourceScheduler|Prepare| db source %v is already %v", d.p, d.p.Enable)
		return kv, nil
	}
	dbSource.IsIndexed = d.p.Enable
	dbSource.UpdateTime = time.Now()
	dbSource.LastSyncTime = time.Now()
	dbSource.ReleaseStatus = model.ReleaseStatusUnreleased
	if dbSource.NextAction != model.ReleaseActionAdd {
		dbSource.NextAction = model.ReleaseActionUpdate
	}
	dbSource.StaffID = d.p.StaffID
	err = dao.GetDBSourceDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DbSourceBizID,
		[]string{"is_indexed", "update_time", "release_status", "staff_id", "last_sync_time"}, dbSource)
	if err != nil {
		log.ErrorContextf(ctx, "EnableDbSourceScheduler|Prepare| update db source %v, err: %v", d.p, err)
		return nil, err
	}

	for index, tables := range slicex.Chunk(d.p.DBTableBizIDs, tableKVChunkSize) {
		err := updateTask(ctx, kv, index, tables)
		if err != nil {
			log.ErrorContextf(ctx, "EnableDbSourceScheduler|Prepare| update task %v, err: %v", d.p, err)
			return nil, err
		}
	}
	log.InfoContextf(ctx, "EnableDbSourceScheduler, table id count %v", len(d.p.DBTableBizIDs))
	return kv, nil
}

func updateTask(ctx context.Context, kv task_scheduler.TaskKV, index int, ids []uint64) error {
	buf, err := json.Marshal(ids)
	if err != nil {
		log.ErrorContextf(ctx, "marshal table biz id %v error, %v", ids, err)
		return err
	}
	kv[cast.ToString(index)] = string(buf)
	log.DebugContextf(ctx, "index: %v, ids: %v", index, string(buf))
	return nil
}

// Init 初始化
func (d *EnableDbSourceScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *EnableDbSourceScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	ctx = pkg.WithStaffID(ctx, d.p.StaffID)
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		var tablesIDs []uint64
		err := json.Unmarshal([]byte(v), &tablesIDs)
		if err != nil {
			log.ErrorContextf(ctx, "unmarshal tables ids %v error, %v", tablesIDs, err)
			return err
		}
		for _, tableID := range tablesIDs {
			if d.p.DbSourceBizID > 0 {
				err = db_source.ChangeDbTableEnable(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, tableID, d.p.Enable, d.dao)
			} else {
				err = db_source.ChangeDbTableEnable(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, tableID, d.p.Enable, d.dao)
			}
			if err != nil {
				log.ErrorContextf(ctx, "ChangeDbTableEnable failed, table %v, err: %v", tableID, err)
				return err
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "finish %v error", k)
			return err
		}
		log.InfoContextf(ctx, "EnableDbSourceScheduler|change enable table successfully, index: %v, count: %v, cost: %vms",
			k, len(tablesIDs), time.Now().Sub(t0).Milliseconds())
	}
	return nil
}

// Stop 任务停止
func (d *EnableDbSourceScheduler) Stop(ctx context.Context) error {
	log.InfoContextf(ctx, "task(ReleaseDB) stopped")
	return nil
}

// Done 任务完成回调
func (d *EnableDbSourceScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(EnableDbSourceScheduler) finish, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}

// Fail 任务失败
func (d *EnableDbSourceScheduler) Fail(ctx context.Context) error {
	log.WarnContextf(ctx, "task(ReleaseDB) ChangeDbTableEnable failed,task: %v, params: %+v, table %v", d.task.ID, d.p, d.p.DbTableBizID)
	err := dao.GetDBTableDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DbTableBizID,
		[]string{"learn_status", "staff_id"}, &model.DBTable{LearnStatus: model.LearnStatusFailed, StaffID: d.p.StaffID})
	if err != nil {
		log.ErrorContextf(ctx, "ChangeDbTableEnable|update LearnStatusFailed failed,table %v, err: %v", d.p.DbTableBizID, err)
		return err
	}
	return nil
}
