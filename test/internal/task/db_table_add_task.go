package task

import (
	"context"
	"encoding/json"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"
)

const (
	mysqlStatusName = "learn_status"
)

// AddDbTableScheduler 添加数据表任务
type AddDbTableScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.LearnDBTableParams
}

func initAddDbTableScheduler() {
	task_scheduler.Register(
		model.AddDbTableTask,
		func(t task_scheduler.Task, params model.LearnDBTableParams) task_scheduler.TaskHandler {
			return &AddDbTableScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *AddDbTableScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.InfoContextf(ctx, "task(AddDbTableScheduler) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	err := dao.GetDBTableDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DBTableBizID, []string{mysqlStatusName}, &model.DBTable{LearnStatus: model.LearnStatusLearning})
	if err != nil {
		log.ErrorContextf(ctx, "AddDbTableScheduler|update table learn status failed: table: %v, err: %v", d.p.DBTableBizID, err)
		return nil, err
	}

	kv[cast.ToString(d.p.DBTableBizID)] = cast.ToString(d.p.DBTableBizID)
	return kv, nil
}

// Init 初始化
func (d *AddDbTableScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *AddDbTableScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		var tablesID uint64
		err := json.Unmarshal([]byte(v), &tablesID)
		if err != nil {
			log.ErrorContextf(ctx, "AddDbTableScheduler|unmarshal tables ids %v error, %v", tablesID, err)
			return err
		}
		corpBizID := pkg.CorpBizID(ctx)
		err = db_source.DeleteDbTableVdb(ctx, d.p.RobotID, corpBizID, d.p.AppBizID, d.p.DBTableBizID, d.p.EmbeddingVersion, retrieval.EnvType_Test)
		if err != nil {
			log.ErrorContextf(ctx, "DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v", d.p.DBTableBizID, err)
			return err
		}

		err = db_source.AddDbTableData2ES1(ctx, d.p.DBSource, d.p.RobotID, d.p.DBTableBizID, bot_retrieval_server.EnvType_Test)
		if err != nil {
			log.ErrorContextf(ctx, "AddDbTableScheduler|add table data to es1 failed: table: %v, err: %v", d.p.DBTableBizID, err)
			return err
		}

		err = dao.GetDBSourceDao().GetTopNValueV2(ctx, d.p.DBSource, d.p.RobotID, d.p.DBTableBizID, d.p.EmbeddingVersion, d.dao)
		if err != nil {
			log.ErrorContextf(ctx, "AddDbTableScheduler|get top n value for mysql failed: table: %v, err: %v", d.p.DBTableBizID, err)
			return err
		}

		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "AddDbTableScheduler|finish task key %v error: %v", k, err)
			return err
		}
		log.InfoContextf(ctx, "AddDbTableScheduler|process success, tablesID: %v, cost: %vms",
			tablesID, time.Now().Sub(t0).Milliseconds())
	}
	return nil
}

// Stop 任务停止
func (d *AddDbTableScheduler) Stop(ctx context.Context) error {
	log.InfoContextf(ctx, "task(AddDbTableScheduler) stopped")
	return nil
}

// Done 任务完成回调
func (d *AddDbTableScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(AddDbTableScheduler) finish, task: %v, params: %+v", d.task.ID, d.p)
	err := dao.GetDBTableDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DBTableBizID, []string{mysqlStatusName}, &model.DBTable{LearnStatus: model.LearnStatusLearned})
	if err != nil {
		log.ErrorContextf(ctx, "AddDbTableScheduler|update table learn Done failed: table: %v, err: %v", d.p.DBTableBizID, err)
		return err
	}
	return nil
}

// Fail 任务失败
func (d *AddDbTableScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "task(AddDbTableScheduler) fail, task: %v, params: %+v", d.task.ID, d.p)
	err := dao.GetDBTableDao().UpdateByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, d.p.DBTableBizID, []string{mysqlStatusName}, &model.DBTable{LearnStatus: model.LearnStatusFailed})
	if err != nil {
		log.ErrorContextf(ctx, "AddDbTableScheduler|update table learn status failed: table: %v, err: %v", d.p.DBTableBizID, err)
		return err
	}
	return nil
}
