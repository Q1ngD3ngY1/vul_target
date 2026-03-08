package task

import (
	"context"
	"encoding/json"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"
)

const (
	tableKVChunkSize = 100
)

// ReleaseDBScheduler db 发布任务
type ReleaseDBScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.ReleaseDBParams
}

func initReleaseDBScheduler() {
	task_scheduler.Register(
		model.ReleaseDBTask,
		func(t task_scheduler.Task, params model.ReleaseDBParams) task_scheduler.TaskHandler {
			return &ReleaseDBScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *ReleaseDBScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.InfoContextf(ctx, "task(ReleaseDB) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)

	// 在prepare将所有的数据库信息先发布
	err := dao.GetDBSourceDao().ReleaseDBSource(ctx, d.p.AppBizID, d.p.ReleaseBizID)
	if err != nil {
		return nil, err
	}
	log.InfoContextf(ctx, "db source release successfully")

	// 收集所有release_db_table待发布表的BIZ ID信息
	tableBizIDs, err := dao.GetDBTableDao().GetAllReleaseDBTables(ctx, d.p.AppBizID, d.p.ReleaseBizID, true)
	if err != nil {
		return nil, err
	}
	if len(tableBizIDs) == 0 {
		return kv, nil
	}

	for index, bizIDs := range slicex.Chunk(tableBizIDs, tableKVChunkSize) {
		var ids []uint64
		for _, t := range bizIDs {
			ids = append(ids, t.DBTableBizID)
		}
		buf, err := json.Marshal(ids)
		if err != nil {
			log.ErrorContextf(ctx, "marsh table biz id error, %v", err)
			return nil, err
		}
		kv[cast.ToString(index)] = string(buf)
		log.DebugContextf(ctx, "index: %v, ids: %v", index, string(buf))
	}

	log.InfoContextf(ctx, "task prepare finish, table id count %v", len(tableBizIDs))
	return kv, nil
}

// Init 初始化
func (d *ReleaseDBScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *ReleaseDBScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		var tablesIDs []uint64
		err := json.Unmarshal([]byte(v), &tablesIDs)
		if err != nil {
			log.ErrorContextf(ctx, "unmarshal tables ids %v error, %v", tablesIDs, err)
			return err
		}
		dbMap := make(map[uint64]*model.DBSource)
		for _, tableID := range tablesIDs {
			err = d.releaseOneTable(ctx, tableID, dbMap)
			if err != nil {
				log.ErrorContextf(ctx, "release table failed, table %v, err: %v", tableID, err)
				return err
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "finish %v error", k)
			return err
		}
		log.InfoContextf(ctx, "release table successfully, index: %v, count: %v, cost: %vms",
			k, len(tablesIDs), time.Now().Sub(t0).Milliseconds())
	}
	return nil
}

func (d *ReleaseDBScheduler) releaseOneTable(ctx context.Context, tableID uint64, dbMap map[uint64]*model.DBSource) error {
	// 1. 从release_db_table表全量覆盖到prod表
	releaseTable, err := dao.GetDBTableDao().GetReleaseDBTable(ctx, d.p.AppBizID, d.p.ReleaseBizID, tableID)
	if err != nil {
		return err
	}
	err = dao.GetDBTableDao().ReleaseDBTableToProd(ctx, releaseTable)
	if err != nil {
		return err
	}
	// 目前t_db_table_column表没有检索服务中并未直接使用，而是通过es解析，暂不需要发布

	// 2. 发布es数据
	if releaseTable.Source == model.TableSourceDoc {
		// 文档解析干预产生的数据表，在文档发布过程中写es的数据，这里不做处理
		log.DebugContextf(ctx, "doc table, no need to publish es")
		return nil
	}
	embeddingVersion, err := db_source.GetAppEmbeddingVersionById(ctx, d.p.RobotID, d.dao)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppEmbeddingVersionById failed, get app embedding version failed: %v", err)
		return err
	}

	if releaseTable.Action == model.ReleaseActionAdd || releaseTable.Action == model.ReleaseActionUpdate {
		err = db_source.DeleteDBText2SQL(ctx, d.p.RobotID, []uint64{tableID}, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
		err = db_source.DeleteDbTableVdb(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, releaseTable.DBTableBizID,
			embeddingVersion, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
		dbSource, ok := dbMap[releaseTable.DBSourceBizID]
		if !ok {
			// 用户发布的过程中将数据库删除的情况，不继续进行发布流程
			dbSource, err = dao.GetDBSourceDao().GetByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, releaseTable.DBSourceBizID)
			if err != nil || dbSource == nil {
				log.ErrorContextf(ctx, "releaseOneTable|get db source by biz id failed, table:%+v err:%v", releaseTable, err)
				return nil
			}
			dbMap[releaseTable.DBSourceBizID] = dbSource
		}
		if releaseTable.IsIndexed {
			existed, err := dao.GetDBSourceDao().CheckDbTableIsExisted(ctx, dbSource, releaseTable.Name)
			// 如果数据库表不存在，或者数据库无法连接，则不进行后续操作
			if err != nil || !existed {
				log.WarnContextf(ctx, "CheckDbTableIsExisted|check db table is existed failed, table: %+v, err: %v", releaseTable, err)
				return nil
			}
			err = db_source.AddDbTableData2ES1(ctx, dbSource, d.p.RobotID, tableID, retrieval.EnvType_Prod)
			if err != nil {
				log.ErrorContextf(ctx, "add db table data to es1 failed, table: %v, err: %v", tableID, err)
				return err
			}
			err = db_source.UpsertDbTable2Vdb(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID,
				releaseTable.DBTableBizID, embeddingVersion)
			if err != nil {
				log.ErrorContextf(ctx, "upsert db table to vdb failed, table: %v, err: %v", tableID, err)
				return err
			}
		}
	} else if releaseTable.Action == model.ReleaseActionDelete {
		err = db_source.DeleteDBText2SQL(ctx, d.p.RobotID, []uint64{tableID}, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
		err = db_source.DeleteDbTableVdb(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, releaseTable.DBTableBizID,
			embeddingVersion, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
	} else {
		log.ErrorContextf(ctx, "illegal release action, table: %v, action: %v", tableID, releaseTable.Action)
		return fmt.Errorf("illegal release action")
	}

	return nil
}

// Stop 任务停止
func (d *ReleaseDBScheduler) Stop(ctx context.Context) error {
	log.InfoContextf(ctx, "task(ReleaseDB) stopped")
	return nil
}

// Done 任务完成回调
func (d *ReleaseDBScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(ReleaseDB) finish, task: %v, params: %+v", d.task.ID, d.p)
	const dbCallBack = 3
	req := &admin.ReleaseNotifyReq{
		RobotId:        d.p.RobotID,
		VersionId:      d.p.ReleaseBizID,
		IsSuccess:      true,
		CallbackSource: dbCallBack,
		RobotBizId:     d.p.AppBizID,
	}
	_, err := admin.NewApiClientProxy().ReleaseNotify(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "ReleaseNotify req: %+v, error %v", err)
		return err
	}

	return nil
}

// Fail 任务失败
func (d *ReleaseDBScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "task(ReleaseDB) fail, task: %v, params: %+v", d.task.ID, d.p)
	return nil
}
