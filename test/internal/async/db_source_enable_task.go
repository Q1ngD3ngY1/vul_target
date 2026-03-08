package async

import (
	"context"
	"encoding/json"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	entity0 "git.woa.com/adp/kb/kb-config/internal/entity"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// EnableDbSourceTaskHandler db 开启关闭任务
type EnableDbSourceTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity0.EnableDBSourceParams
}

func registerDbSourceTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity0.EnableDBSourceTask,
		func(t task_scheduler.Task, params entity0.EnableDBSourceParams) task_scheduler.TaskHandler {
			return &EnableDbSourceTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *EnableDbSourceTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.I(ctx, "task(EnableDbSourceTaskHandler) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	// 如果 DbTableBizID 不为空，收集单个 db_table 的索引状态
	if d.p.DbTableBizID > 0 {
		ids := []uint64{d.p.DbTableBizID}
		err := updateTask(ctx, kv, 0, ids)
		if err != nil {
			logx.E(ctx, "EnableDbSourceTaskHandler|Prepare| update task %v, err: %v", d.p, err)
			return nil, err
		}
		return kv, nil
	}

	// 如果 DbSourceBizID 不为空，收集 db_source 的索引状态
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     d.p.CorpBizID,
		AppBizID:      d.p.AppBizID,
		DBSourceBizID: d.p.DbSourceBizID,
	}
	dbSource, err := d.dbLogic.DescribeDatabase(ctx, &dbFilter)
	// dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DbSourceBizID)
	if err != nil {
		logx.E(ctx, "EnableDbSourceTaskHandler|Prepare| get db source %v, err: %v", d.p, err)
		return nil, err
	}
	if dbSource.IsIndexed == d.p.Enable {
		logx.W(ctx, "EnableDbSourceTaskHandler|Prepare| db source %v is already %v", d.p, d.p.Enable)
		return kv, nil
	}
	now := time.Now()
	err = d.dbLogic.ModifyDatabaseSimple(ctx, &dbFilter, map[string]any{
		"is_indexed":     convx.BoolToInt[uint32](d.p.Enable),
		"release_status": releaseEntity.ReleaseStatusInit,
		"staff_id":       d.p.StaffID,
		"update_time":    now,
		"last_sync_time": now,
	})
	// err = dao.GetDBSourceDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DbSourceBizID, []string{"is_indexed", "update_time", "release_status", "staff_id", "last_sync_time"}, dbSource)
	if err != nil {
		logx.E(ctx, "EnableDbSourceTaskHandler|Prepare| update db source %v, err: %v", d.p, err)
		return nil, err
	}

	for index, tables := range slicex.Chunk(d.p.DBTableBizIDs, tableKVChunkSize) {
		err := updateTask(ctx, kv, index, tables)
		if err != nil {
			logx.E(ctx, "EnableDbSourceTaskHandler|Prepare| update task %v, err: %v", d.p, err)
			return nil, err
		}
	}
	logx.I(ctx, "EnableDbSourceTaskHandler, table id count %v", len(d.p.DBTableBizIDs))
	return kv, nil
}

func updateTask(ctx context.Context, kv task_scheduler.TaskKV, index int, ids []uint64) error {
	buf, err := json.Marshal(ids)
	if err != nil {
		logx.E(ctx, "marshal table biz id %v error, %v", ids, err)
		return err
	}
	kv[cast.ToString(index)] = string(buf)
	logx.D(ctx, "index: %v, ids: %v", index, string(buf))
	return nil
}

// Init 初始化
func (d *EnableDbSourceTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *EnableDbSourceTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	contextx.Metadata(ctx).WithStaffID(d.p.StaffID)
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		var tablesIDs []uint64
		err := json.Unmarshal([]byte(v), &tablesIDs)
		if err != nil {
			logx.E(ctx, "unmarshal tables ids %v error, %v", tablesIDs, err)
			return err
		}
		for _, tableID := range tablesIDs {
			if d.p.DbSourceBizID > 0 {
				err = d.dbLogic.ChangeDbTableEnable(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, tableID, d.p.Enable)
			} else {
				err = d.dbLogic.ChangeDbTableEnable(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, tableID, d.p.Enable)
			}
			if err != nil {
				logx.E(ctx, "ChangeDbTableEnable failed, table %v, err: %v", tableID, err)
				return err
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "finish %v error", k)
			return err
		}
		logx.I(ctx, "EnableDbSourceTaskHandler|change enable table successfully, index: %v, count: %v, cost: %vms",
			k, len(tablesIDs), time.Now().Sub(t0).Milliseconds())
	}
	return nil
}

// Stop 任务停止
func (d *EnableDbSourceTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "task(ReleaseDB) stopped")
	return nil
}

// Done 任务完成回调
func (d *EnableDbSourceTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(EnableDbSourceTaskHandler) finish, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}

// Fail 任务失败
func (d *EnableDbSourceTaskHandler) Fail(ctx context.Context) error {
	logx.W(ctx, "task(ReleaseDB) ChangeDbTableEnable failed,task: %v, params: %+v, table %v", d.task.ID, d.p, d.p.DbTableBizID)
	tableFilter := entity.TableFilter{
		CorpBizID:    d.p.CorpBizID,
		AppBizID:     d.p.AppBizID,
		DBTableBizID: d.p.DbTableBizID,
	}
	err := d.dbLogic.ModifyTable(ctx, &tableFilter, map[string]any{
		"learn_status": entity.LearnStatusFailed,
		"staff_id":     d.p.StaffID,
	})
	// err := dao.GetDBTableDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DbTableBizID, []string{"learn_status", "staff_id"}, &database.Table{LearnStatus: database.LearnStatusFailed, StaffID: d.p.StaffID})
	if err != nil {
		logx.E(ctx, "ChangeDbTableEnable|update LearnStatusFailed failed,table %v, err: %v", d.p.DbTableBizID, err)
		return err
	}
	return nil
}
