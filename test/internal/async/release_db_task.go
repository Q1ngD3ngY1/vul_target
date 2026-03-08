package async

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	app "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_retrieval"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	tableKVChunkSize = 100
)

// ReleaseDBTaskHandler db 发布任务
type ReleaseDBTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.ReleaseDBParams
}

func registerReleaseDBTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.ReleaseDBTask,
		func(t task_scheduler.Task, params entity.ReleaseDBParams) task_scheduler.TaskHandler {
			return &ReleaseDBTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *ReleaseDBTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.I(ctx, "task(ReleaseDB) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)

	// 在prepare将所有的数据库信息先发布
	err := d.dbLogic.ReleaseDBSource(ctx, d.p.AppBizID, d.p.ReleaseBizID)
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "db source release successfully")

	// 收集所有release_db_table待发布表的BIZ ID信息
	tableBizIDs, err := d.dbLogic.GetAllReleaseDBTables(ctx, d.p.AppBizID, d.p.ReleaseBizID, true)
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
			logx.E(ctx, "marsh table biz id error, %v", err)
			return nil, err
		}
		kv[cast.ToString(index)] = string(buf)
		logx.D(ctx, "index: %v, ids: %v", index, string(buf))
	}

	logx.I(ctx, "task prepare finish, table id count %v", len(tableBizIDs))
	return kv, nil
}

// Init 初始化
func (d *ReleaseDBTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *ReleaseDBTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		t0 := time.Now()
		var tablesIDs []uint64
		err := json.Unmarshal([]byte(v), &tablesIDs)
		if err != nil {
			logx.E(ctx, "unmarshal tables ids %v error, %v", tablesIDs, err)
			return err
		}
		dbMap := make(map[uint64]*dbEntity.Database)
		for _, tableID := range tablesIDs {
			err = d.releaseOneTable(ctx, tableID, dbMap)
			if err != nil {
				logx.E(ctx, "release table failed, table %v, err: %v", tableID, err)
				return err
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "finish %v error", k)
			return err
		}
		logx.I(ctx, "release table successfully, index: %v, count: %v, cost: %vms",
			k, len(tablesIDs), time.Now().Sub(t0).Milliseconds())
	}
	return nil
}

func (d *ReleaseDBTaskHandler) releaseOneTable(ctx context.Context, tableID uint64, dbMap map[uint64]*dbEntity.Database) error {
	// 1. 从release_db_table表全量覆盖到prod表
	releaseTable, err := d.dbLogic.GetReleaseDBTable(ctx, d.p.AppBizID, d.p.ReleaseBizID, tableID)
	if err != nil {
		return err
	}
	err = d.dbLogic.ReleaseDBTableToProd(ctx, releaseTable)
	if err != nil {
		return err
	}
	// 目前t_db_table_column表没有检索服务中并未直接使用，而是通过es解析，暂不需要发布

	// 2. 发布es数据
	if releaseTable.Source == dbEntity.TableSourceDoc {
		// 文档解析干预产生的数据表，在文档发布过程中写es的数据，这里不做处理
		logx.D(ctx, "doc table, no need to publish es")
		return nil
	}
	embeddingVersion, embeddingModelName, err := d.dbLogic.GetAppEmbeddingInfoById(ctx, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingInfoById failed, get app embedding version failed: %v", err)
		return err
	}

	if releaseTable.Action == releaseEntity.ReleaseActionAdd || releaseTable.Action == releaseEntity.ReleaseActionUpdate {
		err = d.rpc.RetrievalDirectIndex.DeleteDBText2SQL(ctx, d.p.RobotID, []uint64{tableID}, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
		err = d.dbLogic.DeleteDbTableVdb(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, releaseTable.DBTableBizID,
			embeddingVersion, embeddingModelName, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
		dbSource, ok := dbMap[releaseTable.DBSourceBizID]
		if !ok {
			// 用户发布的过程中将数据库删除的情况，不继续进行发布流程
			dbFilter := dbEntity.DatabaseFilter{
				CorpBizID:     d.p.CorpBizID,
				AppBizID:      d.p.AppBizID,
				DBSourceBizID: releaseTable.DBSourceBizID,
			}
			dbSource, err = d.dbLogic.DescribeDatabase(ctx, &dbFilter)
			// dbSource, err = dao.GetDBSourceDao().GetByBizID(ctx, d.p.CorpBizID, d.p.AppBizID, releaseTable.DBSourceBizID)
			if err != nil || dbSource == nil {
				logx.E(ctx, "releaseOneTable|get db source by biz id failed, table:%+v err:%v", releaseTable, err)
				return nil
			}
			dbMap[releaseTable.DBSourceBizID] = dbSource
		}
		if releaseTable.IsIndexed {
			existed, err := d.dbLogic.CheckDbTableIsExisted(ctx, dbSource, releaseTable.Name)
			// 如果数据库表不存在，或者数据库无法连接，则不进行后续操作
			if err != nil || !existed {
				logx.W(ctx, "CheckDbTableIsExisted|check db table is existed failed, table: %+v, err: %v", releaseTable, err)
				return nil
			}
			err = d.dbLogic.AddDbTableData2ES1(ctx, dbSource, d.p.RobotID, tableID, retrieval.EnvType_Prod)
			if err != nil {
				logx.E(ctx, "add db table data to es1 failed, table: %v, err: %v", tableID, err)
				return err
			}
			err = d.dbLogic.UpsertDbTable2Vdb(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, releaseTable.DBTableBizID, embeddingVersion, embeddingModelName)
			if err != nil {
				logx.E(ctx, "upsert db table to vdb failed, table: %v, err: %v", tableID, err)
				return err
			}
		}
	} else if releaseTable.Action == releaseEntity.ReleaseActionDelete {
		err = d.rpc.RetrievalDirectIndex.DeleteDBText2SQL(ctx, d.p.RobotID, []uint64{tableID}, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
		err = d.dbLogic.DeleteDbTableVdb(ctx, d.p.RobotID, d.p.CorpBizID, d.p.AppBizID, releaseTable.DBTableBizID,
			embeddingVersion, embeddingModelName, retrieval.EnvType_Prod)
		if err != nil {
			return err
		}
	} else {
		logx.E(ctx, "illegal release action, table: %v, action: %v", tableID, releaseTable.Action)
		return fmt.Errorf("illegal release action")
	}

	return nil
}

// Stop 任务停止
func (d *ReleaseDBTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "task(ReleaseDB) stopped")
	return nil
}

// Done 任务完成回调
func (d *ReleaseDBTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(ReleaseDB) finish, task: %v, params: %+v", d.task.ID, d.p)
	if err := d.releaseLogic.DoSuccessNotifyRelease(ctx, d.p.RobotID, d.p.AppBizID, d.p.ReleaseBizID, releaseEntity.ReleaseDBCallback); err != nil {
		return err
	}

	return nil
}

// Fail 任务失败
func (d *ReleaseDBTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "task(ReleaseDBTaskHandler) fail, task ID: %v, param: %+v", d.task.ID, d.p)
	retryTimes := task_scheduler.DefaultTaskConfig.MaxRetry
	c, ok := config.App().Tasks[entity.TaskTypeNameMap[entity.ReleaseDBTask]]
	if ok {
		retryTimes = c.MaxRetry
	}
	r := &pb.ContinueTerminatedTaskReq{
		TaskId:      uint64(d.task.ID),
		RetryTimes:  uint64(retryTimes),
		WaitToStart: 0,
	}
	transparent, err := protojson.Marshal(r)
	if err != nil {
		err = fmt.Errorf("task (ReleaseDBTaskHandler) fail, protojson.Marshal fail, err: %w", err)
		logx.W(ctx, err.Error())
		return err
	}

	logx.I(ctx, "task(ReleaseDBTaskHandler) fail, SEND FAIL CALLBACK. transparent: %v",
		string(transparent))

	_, err = d.rpc.AppAdmin.ReleaseNotify(ctx, &app.ReleaseNotifyReq{
		RobotId:        d.p.RobotID,
		VersionId:      d.p.ReleaseBizID,
		IsSuccess:      false,
		Message:        "",
		Transparent:    string(transparent),
		CallbackSource: releaseEntity.ReleaseDBCallback,
	})
	if err != nil {
		err = fmt.Errorf("task (ReleaseDBTaskHandler) fail, Fail, err: %w", err)
		logx.W(ctx, err.Error())
		return err
	}
	return nil
}
