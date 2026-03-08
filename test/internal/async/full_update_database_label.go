package async

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/database"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

// FullUpdateDatabaseLabelTaskHandler 全量刷数据标签
type FullUpdateDatabaseLabelTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.FullUpdateDBLabel
}

func registerFullUpdateDatabaseLabelTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.FullUpdateDatabaseLabelTask,
		func(t task_scheduler.Task, params entity.FullUpdateDBLabel) task_scheduler.TaskHandler {
			return &FullUpdateDatabaseLabelTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare .
func (d *FullUpdateDatabaseLabelTaskHandler) Prepare(ctx context.Context) (kv task_scheduler.TaskKV, err error) {
	logx.I(ctx, "FullUpdateDBLabel Prepare, req:%+v", d.p)
	// 按应用维度刷数据
	kv = make(task_scheduler.TaskKV)
	if len(d.p.DBSourceBizIDs) == 0 {
		var (
			page     = uint32(1)
			pageSize = 500
			allDBs   []*database.Database
		)

		for {
			filter := database.DatabaseFilter{
				PageSize:   uint32(pageSize),
				PageNumber: ptrx.Uint32(page),
			}
			dbs, _, err := d.dbLogic.DescribeDatabaseList(ctx, &filter)
			if err != nil {
				logx.E(ctx, "FullUpdateDBLabel DescribeDatabaseList err:%v,req:%+v", err, filter)
				return nil, err
			}
			allDBs = append(allDBs, dbs...)
			if len(dbs) < pageSize {
				break
			}
			page++
		}
		for _, db := range allDBs {
			kv[cast.ToString(db.DBSourceBizID)] = cast.ToString(db.AppBizID)
		}
		return kv, nil

	}
	filter := &database.DatabaseFilter{
		DBSourceBizIDs: d.p.DBSourceBizIDs,
	}
	dbs, _, err := d.dbLogic.DescribeDatabaseList(ctx, filter)
	if err != nil {
		logx.W(ctx, "FullUpdateDBLabel DescribeDatabaseList err:%v,req:%+v", err, d.p)
		return nil, err
	}
	if len(dbs) == 0 {
		logx.I(ctx, "FullUpdateDBLabel get databases empty,req:%+v", d.p)
		return kv, nil
	}
	for _, db := range dbs {
		kv[cast.ToString(db.DBSourceBizID)] = cast.ToString(db.AppBizID)
	}
	return kv, nil
}

// Init .
func (d *FullUpdateDatabaseLabelTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	logx.I(ctx, "FullUpdateDBLabel Init,req:%+v", d.p)
	return nil
}

// Process .
func (d *FullUpdateDatabaseLabelTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		if !config.GetMainConfig().Permissions.UpdateDBLabelSwitch {
			logx.I(ctx, "FullUpdateDBLabel Process,k:%v,UpdateDBLabelSwitch is false", k)
			return errx.New(1000, "停止任务")
		}
		startTime := time.Now()
		logx.I(ctx, "FullUpdateDBLabel Process,k:%v", k)
		dbSourceBizID := cast.ToUint64(k)
		appBizID := cast.ToUint64(v)
		// 获取数据库详情
		dbInfo, err := d.dbLogic.DescribeDatabase(ctx, &database.DatabaseFilter{AppBizID: appBizID, DBSourceBizID: dbSourceBizID})
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel DescribeDatabase err:%v,dbSourceBizID:%v", err, dbSourceBizID)
			continue
		}
		if dbInfo.IsDeleted { // 已经删除,直接跳过
			logx.I(ctx, "FullUpdateDBLabel dbInfo.IsDeleted,dbSourceBizID:%v", dbSourceBizID)
			continue
		}
		// 获取应用信息
		appInfo, err := d.rpc.AppAdmin.DescribeAppById(ctx, appBizID)
		if (err != nil && errs.Is(err, errs.ErrRobotNotFound)) || (appInfo != nil && appInfo.HasDeleted()) {
			if err := progress.Finish(ctx, k); err != nil {
				logx.E(ctx, "FullUpdateDBLabel Finish key:%s,err:%+v", k, err)
				return err
			}
			continue
		}
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(2)
		g.Go(func() error { // 开发域DB
			err1 := d.processDBLabelTest(gCtx, dbInfo, appInfo)
			if err1 != nil {
				logx.E(gCtx, "FullUpdateDBLabel processDBLabelTest err:%v,dbSourceBizID:%v", err1, dbSourceBizID)
				return err1
			}
			logx.I(gCtx, "FullUpdateDBLabel processDBLabelTest key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		g.Go(func() error { // 发布域DB
			err1 := d.processDBLabelProd(gCtx, dbInfo, appInfo)
			if err1 != nil {
				logx.E(gCtx, "FullUpdateDBLabel processDBLabelProd err:%v,dbSourceBizID:%v", err1, dbSourceBizID)
				return err1
			}
			logx.I(gCtx, "FullUpdateDBLabel processDBLabel key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		if err := g.Wait(); err != nil { // 柔性放过
			logx.E(ctx, "FullUpdateDBLabel Process err:%v,dbSourceBizID:%v", err, dbSourceBizID)
		}
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "FullUpdateDBLabel Finish key:%s,err:%+v", k, err)
			return err
		}
		logx.I(ctx, "FullUpdateDBLabel Finish key:%s,cost:%v", k, time.Since(startTime))
	}
	return nil
}

// processDBLabelTest 开发域DB标签更新
func (d *FullUpdateDatabaseLabelTaskHandler) processDBLabelTest(ctx context.Context, db *database.Database, app *entity.App) error {
	// 获取数据库下的所有表
	dbTable, err := d.describeAllTableList(ctx, db, app)
	if err != nil {
		return err
	}
	embeddingVersion := app.Embedding.Version
	embeddingName := ""
	embeddingName, err =
		d.kbLogic.GetKnowledgeEmbeddingModel(ctx, app.CorpBizId, app.BizId, app.BizId, app.IsShared)

	if err != nil {
		if err != nil {
			logx.W(ctx, "task(FullUpdateDBLabel) GetShareKnowledgeBaseConfig err:%+v", err)
		}
	}
	if embeddingName != "" {
		embeddingVersion = entity.GetEmbeddingVersion(embeddingName)
	}
	logx.I(ctx, "FullUpdateDBLabel  "+
		" embeddingModelName:%s, using embeddingVersion:%d", embeddingName, embeddingVersion)
	// 遍历所有表，更新es和向量标签
	for _, table := range dbTable {
		if table.IsDeleted {
			continue
		}
		if table.LearnStatus == database.LearnStatusFailed || table.LearnStatus == database.LearnStatusUnlearned {
			logx.I(ctx, "FullUpdateDBLabel|processDBLabel|status:%v no need process, skip dbTableBizID:%v", table.LearnStatus, table.DBTableBizID)
			continue
		}
		// 先清空数据，再添加
		err = d.dbLogic.DeleteDbTableVdb(ctx, app.PrimaryId, app.CorpBizId, app.BizId, table.DBTableBizID, embeddingVersion, embeddingName, retrieval.EnvType_Test)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel|processDBLabelTest|DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v", table.DBTableBizID, err)
			return err
		}

		err = d.dbLogic.AddDbTableData2ES1(ctx, db, app.PrimaryId, table.DBTableBizID, retrieval.EnvType_Test)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel|processDBLabelTest|add table data to es1 failed: table: %v, err: %v", table.DBTableBizID, err)
			return err
		}

		err = d.dbLogic.GetTopNValueV2(ctx, db, app.PrimaryId, table.DBTableBizID, embeddingVersion, embeddingName)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel|processDBLabelTest|get top n value for mysql failed: table: %v, err: %v", table.DBTableBizID, err)
			return err
		}
	}
	return nil
}

// processDBLabelProd 发布域DB标签更新
func (d *FullUpdateDatabaseLabelTaskHandler) processDBLabelProd(ctx context.Context, db *database.Database, app *entity.App) error {
	// 先查这个数据库是否发布过
	_, err := d.dbLogic.DescribeDatabaseProd(ctx, &database.DatabaseFilter{AppBizID: app.BizId, DBSourceBizID: db.DBSourceBizID})
	if err != nil {
		if errs.Is(err, errs.ErrDataNotExistOrIsDeleted) || errs.Is(err, errx.ErrNotFound) {
			logx.I(ctx, "FullUpdateDBLabel|processDBLabelProd|database prod not exist, skip dbSourceBizID:%v", db.DBSourceBizID)
			return nil
		}
		logx.E(ctx, "FullUpdateDBLabel|processDBLabelProd|DescribeDatabaseProd failed, skip dbSourceBizID:%v err:%v", db.DBSourceBizID, err)
		return err
	}
	// 获取数据库下的所有表
	dbTable, err := d.describeAllTableProdList(ctx, db, app)
	if err != nil {
		return err
	}

	embeddingVersion := app.Embedding.Version
	embeddingName := ""
	if app.IsShared {
		logx.W(ctx, "FullUpdateDBLabel app.IsShared()|appID:%d", app.PrimaryId)
		embeddingName, err = d.kbLogic.GetShareKnowledgeBaseConfig(ctx, app.CorpBizId, app.BizId,
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
		if err != nil {
			logx.W(ctx, "FullUpdateDBLabel GetKnowledgeBaseConfig err:%+v", err)
			//return err
		}
		logx.I(ctx, "FullUpdateDBLabel shared kg "+
			" embeddingModelName:%s, using embeddingVersion:%d", embeddingName, embeddingVersion)

	}
	// 遍历所有表，更新es和向量标签
	for _, table := range dbTable {
		if table.IsDeleted {
			continue
		}
		// 先清空数据，再添加
		err = d.dbLogic.DeleteDbTableVdb(ctx, app.PrimaryId, app.CorpBizId, app.BizId, table.DBTableBizID,
			embeddingVersion, embeddingName, retrieval.EnvType_Prod)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel|processDBLabelProd|DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v", table.DBTableBizID, err)
			return err
		}

		err = d.dbLogic.AddDbTableData2ES1(ctx, db, app.PrimaryId, table.DBTableBizID, retrieval.EnvType_Prod)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel|processDBLabelProd|add table data to es1 failed: table: %v, err: %v", table.DBTableBizID, err)
			return err
		}

		err = d.dbLogic.GetTopNValueV2(ctx, db, app.PrimaryId, table.DBTableBizID, embeddingVersion, embeddingName)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel|processDBLabelProd|get top n value for mysql failed: table: %v, err: %v", table.DBTableBizID, err)
			return err
		}
	}
	return nil
}

// Done .
func (d *FullUpdateDatabaseLabelTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task FullUpdateDBLabel finish")
	return nil
}

// Fail .
func (d *FullUpdateDatabaseLabelTaskHandler) Fail(ctx context.Context) error {
	logx.E(ctx, "task FullUpdateDBLabel fail")
	return nil
}

// Stop .
func (d *FullUpdateDatabaseLabelTaskHandler) Stop(ctx context.Context) error {
	return nil
}

func (d *FullUpdateDatabaseLabelTaskHandler) describeAllTableList(ctx context.Context, db *database.Database, app *entity.App) ([]*database.Table, error) {
	var (
		page      = uint32(1)
		pageSize  = 200
		allTables []*database.Table
	)
	for {
		filter := database.TableFilter{
			PageSize:      uint32(pageSize),
			PageNumber:    ptrx.Uint32(page),
			DBSourceBizID: db.DBSourceBizID,
			AppBizID:      app.BizId,
		}
		tables, _, err := d.dbLogic.DescribeTableList(ctx, &filter)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel DescribeTableList err:%v,req:%+v", err, filter)
			return nil, err
		}
		allTables = append(allTables, tables...)
		if len(tables) < pageSize {
			break
		}
		page++
	}
	return allTables, nil
}

func (d *FullUpdateDatabaseLabelTaskHandler) describeAllTableProdList(ctx context.Context, db *database.Database, app *entity.App) ([]*database.TableProd, error) {
	var (
		page      = uint32(1)
		pageSize  = 200
		allTables []*database.TableProd
	)
	for {
		filter := database.TableFilter{
			PageSize:      uint32(pageSize),
			PageNumber:    ptrx.Uint32(page),
			DBSourceBizID: db.DBSourceBizID,
			AppBizID:      app.BizId,
		}
		tables, _, err := d.dbLogic.DescribeTableProdList(ctx, &filter)
		if err != nil {
			logx.E(ctx, "FullUpdateDBLabel DescribeTableList err:%v,req:%+v", err, filter)
			return nil, err
		}
		allTables = append(allTables, tables...)
		if len(tables) < pageSize {
			break
		}
		page++
	}
	return allTables, nil
}
