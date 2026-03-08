package async

import (
	"context"
	"encoding/json"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	entity0 "git.woa.com/adp/kb/kb-config/internal/entity"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// AddDbTableSchedulerTaskHandler 添加数据表任务
type AddDbTableSchedulerTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity0.LearnDBTableParams
}

func registerAddDbTableTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity0.AddDbTableTask,
		func(t task_scheduler.Task, params entity0.LearnDBTableParams) task_scheduler.TaskHandler {
			return &AddDbTableSchedulerTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *AddDbTableSchedulerTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.I(ctx, "task(AddDbTableSchedulerTaskHandler) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	// 表状态已在创建任务前批量更新为学习中，此处不再重复更新
	kv[cast.ToString(d.p.DBTableBizID)] = cast.ToString(d.p.DBTableBizID)
	return kv, nil
}

// Init 初始化
func (d *AddDbTableSchedulerTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *AddDbTableSchedulerTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		var tablesID uint64
		err := json.Unmarshal([]byte(v), &tablesID)
		if err != nil {
			logx.E(ctx, "AddDbTableSchedulerTaskHandler|unmarshal tables ids %v error, %v", tablesID, err)
			return err
		}
		corpBizID := d.p.CorpBizID
		// 先清空数据，再添加
		err = d.dbLogic.DeleteDbTableVdb(ctx, d.p.RobotID, corpBizID, d.p.AppBizID, d.p.DBTableBizID,
			d.p.EmbeddingVersion, d.p.EmbeddingName, retrieval.EnvType_Test)
		if err != nil {
			logx.E(ctx, "DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v", d.p.DBTableBizID, err)
			return err
		}

		err = d.dbLogic.AddDbTableData2ES1(ctx, d.p.DBSource, d.p.RobotID, d.p.DBTableBizID, retrieval.EnvType_Test)
		if err != nil {
			logx.E(ctx, "AddDbTableSchedulerTaskHandler|add table data to es1 failed: table: %v, err: %v", d.p.DBTableBizID, err)
			return err
		}

		err = d.dbLogic.GetTopNValueV2(ctx, d.p.DBSource, d.p.RobotID, d.p.DBTableBizID, d.p.EmbeddingVersion, d.p.EmbeddingName)
		if err != nil {
			logx.E(ctx, "AddDbTableSchedulerTaskHandler|get top n value for mysql failed: table: %v, err: %v", d.p.DBTableBizID, err)
			return err
		}

		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "AddDbTableSchedulerTaskHandler|finish task key %v error: %v", k, err)
			return err
		}
		logx.I(ctx, "AddDbTableSchedulerTaskHandler|process success, tablesID: %v, cost: %vms",
			tablesID, time.Now().Sub(t0).Milliseconds())
	}
	return nil
}

// Stop 任务停止
func (d *AddDbTableSchedulerTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "task(AddDbTableSchedulerTaskHandler) stopped")
	return nil
}

// Done 任务完成回调
func (d *AddDbTableSchedulerTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(AddDbTableSchedulerTaskHandler) finish, task: %v, params: %+v", d.task.ID, d.p)
	tableFilter := entity.TableFilter{
		CorpBizID:    d.p.CorpBizID,
		AppBizID:     d.p.AppBizID,
		DBTableBizID: d.p.DBTableBizID,
	}
	err := d.dbLogic.ModifyTable(ctx, &tableFilter, map[string]any{"learn_status": entity.LearnStatusLearned})
	// err := dao.GetDBTableDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DBTableBizID, []string{mysqlStatusName}, &entity.Table{LearnStatus: entity.LearnStatusLearned})
	if err != nil {
		logx.E(ctx, "AddDbTableSchedulerTaskHandler|update table learn Done failed: table: %v, err: %v", d.p.DBTableBizID, err)
		return err
	}
	return nil
}

// Fail 任务失败
func (d *AddDbTableSchedulerTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "task(AddDbTableSchedulerTaskHandler) fail, task: %v, params: %+v", d.task.ID, d.p)
	tableFilter := entity.TableFilter{
		CorpBizID:    d.p.CorpBizID,
		AppBizID:     d.p.AppBizID,
		DBTableBizID: d.p.DBTableBizID,
	}
	err := d.dbLogic.ModifyTable(ctx, &tableFilter, map[string]any{"learn_status": entity.LearnStatusFailed})
	// err := dao.GetDBTableDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DBTableBizID, []string{mysqlStatusName}, &entity.Table{LearnStatus: entity.LearnStatusFailed})
	if err != nil {
		logx.E(ctx, "AddDbTableSchedulerTaskHandler|update table learn status failed: table: %v, err: %v", d.p.DBTableBizID, err)
		return err
	}
	return nil
}
