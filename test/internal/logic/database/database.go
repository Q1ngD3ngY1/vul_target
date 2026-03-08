package database

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	sqlParser "git.woa.com/adp/common/workflow/sql-parser"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity0 "git.woa.com/adp/kb/kb-config/internal/entity"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieve "git.woa.com/adp/pb-go/kb/kb_retrieval"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
)

// DescribeAvailableDBTypes 查询可用的数据库类型
func (l *Logic) DescribeAvailableDBTypes(ctx context.Context, corpBizId, appBizId uint64) ([]*pb.DbType, error) {
	dbTypeIds, err := l.dao.DescribeAvailableDBTypes(ctx)
	if err != nil {
		logx.E(ctx, "DescribeAvailableDBTypes failed, error: %+v", err)
		return nil, err
	}

	dbTypeIdMap := slicex.MapKV(dbTypeIds, func(item string) (string, bool) {
		return item, true
	})

	var dbTypes []*pb.DbType
	for _, dbTypeInfo := range config.App().DbSource.DatabaseTypes {
		if _, ok := dbTypeIdMap[dbTypeInfo.TypeId]; ok {
			dbTypes = append(dbTypes, &pb.DbType{
				TypeId:   dbTypeInfo.TypeId,
				TypeName: dbTypeInfo.TypeName,
			})
		} else {
			logx.W(ctx, "DescribeAvailableDBTypes, no available executor, dbTypeInfo: %+v", dbTypeInfo)
		}
	}

	return dbTypes, nil
}

// BuildDbSource 构建数据库
func (l *Logic) BuildDbSource(ctx context.Context, corpBizId, appBizId uint64, aliasName, description string,
	connDbSource entity.DatabaseConn) (*entity.Database, error) {

	encryptedPassword, err := util.Encrypt(connDbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		logx.E(ctx, "encrypt password failed: %v", err)
		return nil, err
	}
	connDbSource.Host = strings.TrimSpace(connDbSource.Host)
	connDbSource.Username = strings.TrimSpace(connDbSource.Username)
	dbSource := &entity.Database{
		CorpBizID:     corpBizId,
		AppBizID:      appBizId,
		DBSourceBizID: idgen.GetId(),
		DBName:        connDbSource.DbName,
		AliasName:     aliasName,
		Description:   description,
		DBType:        connDbSource.DbType,
		Host:          connDbSource.Host,
		Port:          connDbSource.Port,
		Username:      connDbSource.Username,
		Password:      encryptedPassword,
		Alive:         true,
		LastSyncTime:  time.Now(),
		ReleaseStatus: releaseEntity.ReleaseStatusInit,
		NextAction:    releaseEntity.ReleaseActionAdd,
		IsDeleted:     false,
		IsIndexed:     true,
		CreateTime:    time.Now(),
		UpdateTime:    time.Now(),
		StaffID:       contextx.Metadata(ctx).StaffID(),
		SchemaName:    connDbSource.SchemaName,
	}
	return dbSource, nil
}

// AddDbSource 添加数据库
func (l *Logic) AddDbSource(ctx context.Context, db *entity.Database, tableNames []string) (*entity.Database, error) {
	dbFilter := entity.DatabaseFilter{
		CorpBizID: db.CorpBizID,
		AppBizID:  db.AppBizID,
	}
	cnt, err := l.dao.CountDatabase(ctx, &dbFilter)
	if err != nil {
		logx.E(ctx, "get db source num failed: %v", err)
		return nil, err
	}
	if cnt > int64(config.App().DbSource.MaxDbSourceNum) {
		return nil, errs.ErrDbSourceLimitExceeded
	}

	// 1. 新建数据库, 但是不保存到数据库
	encryptedPassword, err := util.Encrypt(db.Password, config.App().DbSource.Salt)
	if err != nil {
		logx.E(ctx, "encrypt password failed: %v", err)
		return nil, err
	}
	db.Host = strings.TrimSpace(db.Host)
	db.Username = strings.TrimSpace(db.Username)
	db.Password = encryptedPassword
	db.DBSourceBizID = idgen.GetId()
	db.Alive = true
	db.LastSyncTime = time.Now()
	db.ReleaseStatus = releaseEntity.ReleaseStatusInit
	db.NextAction = releaseEntity.ReleaseActionAdd
	db.IsDeleted = false
	db.IsIndexed = true
	db.CreateTime = time.Now()
	db.UpdateTime = time.Now()
	db.StaffID = contextx.Metadata(ctx).StaffID()

	err = l.rpc.InfoSec.CheckDbSourceField(ctx, db.AppBizID, db.DBSourceBizID, releaseEntity.AuditDbSourceName, db.AliasName)
	if err != nil {
		return nil, err
	}
	err = l.rpc.InfoSec.CheckDbSourceField(ctx, db.AppBizID, db.DBSourceBizID, releaseEntity.AuditDbSourceDesc, db.Description)
	if err != nil {
		return nil, err
	}

	// 2. 批量创建数据表和列
	dbTables, err := l.BatchCreateDbTableAndColumn(ctx, db, tableNames)
	if err != nil {
		logx.E(ctx, "create db table failed: %v", err)
		return nil, err
	}
	// 3. 获取 robotId
	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, db.AppBizID)
	if err != nil {
		return nil, err
	}
	robotId := appDB.PrimaryId

	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(5)
	// 4. 保存到 es
	for _, value := range dbTables {
		table := value
		wg.Go(func() (err error) {
			err = l.CreateDbTableLearnTask(ctx, db, robotId, table.DBTableBizID)
			if err != nil {
				logx.E(wgCtx, "CreateDbTableLearnTask failed, table:%v err:%v", table.DBTableBizID, err)
				return err
			}
			return nil
		})
	}
	if err = wg.Wait(); err != nil {
		logx.E(ctx, "AddDbSource|AddDbTableData2ES1 dbSource %v; dbTable %v; failed: %v", db, dbTables, err)
		return nil, err
	}

	// 5. 保存数据库
	err = l.dao.CreateDatabase(ctx, db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// DeleteDatabase 删除数据库和数据库表以及列
// DeleteDbSourceAndTableCol
func (l *Logic) DeleteDatabase(ctx context.Context, appBizId, dbSourceBizId uint64) error {
	corpBizId := contextx.Metadata(ctx).CorpBizID()

	// 1. 删除数据库表和列
	err := l.deleteTablesAndColByDbSourceBizID(ctx, corpBizId, appBizId, dbSourceBizId)
	if err != nil {
		logx.E(ctx, "delete db table failed: %v", err)
		return err
	}

	// 2. 删除数据库
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     corpBizId,
		AppBizID:      appBizId,
		DBSourceBizID: dbSourceBizId,
	}
	if err = l.dao.DeleteDatabase(ctx, &dbFilter); err != nil {
		logx.E(ctx, "soft delete db source failed: %v", err)
		return err
	}
	return nil

}

func (l *Logic) SyncDatabasesAlive(ctx context.Context, dbs []*entity.Database) error {
	g, gCtx := errgroupx.WithContext(ctx)
	g.SetLimit(10)
	for _, db := range dbs {
		g.Go(func() error {
			return l.SyncDatabaseAlive(gCtx, db)
		})
	}
	return g.Wait()
}

// SyncDatabaseAlive 更新数据库状态， 如果较长时间没有同步 且 没有失效，触发同步流程
func (l *Logic) SyncDatabaseAlive(ctx context.Context, db *entity.Database) error {
	// 1. 判断是否需要同步, 如果较长时间没有同步 且 没有失效，触发同步流程
	if time.Since(db.LastSyncTime).Minutes() < 30 {
		return nil
	}
	logx.I(ctx, "too long 30 minutes not sync, start sync db source: %v", db)

	// 2. 获取数据库连接
	decrypt, err := util.Decrypt(db.Password, config.App().DbSource.Salt)
	if err != nil {
		logx.E(ctx, "decrypt password failed: %v", err)
		return err
	}
	uin, err := l.dao.GetDBUin(ctx, db.CorpBizID)
	if err != nil {
		return err
	}
	connDbSource := entity.DatabaseConn{
		DbType:     db.DBType,
		Host:       db.Host,
		DbName:     db.DBName,
		Username:   db.Username,
		Password:   decrypt,
		Port:       db.Port,
		SchemaName: db.SchemaName,
		CreateTime: &db.CreateTime,
		Uin:        uin,
	}
	connection, err := l.dao.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.W(ctx, "UpdateDbSourceState|get db connection failed: %v", err)
	}
	defer func() {
		if connection != nil {
			CloseErr := connection.Close()
			if CloseErr != nil {
				logx.E(ctx, "close db connection failed: %v", err)
			}
		}
	}()

	// 3. 判断数据库连接是否正常
	if err != nil {
		// 3.1 如果连接失败，将数据库状态置为失效
		logx.E(ctx, "get db connection failed: %v", err)
		db.Alive = false
	} else {
		db.Alive = true
	}

	// 4. 如果连接正常，将数据库状态置为正常
	db.LastSyncTime = time.Now()
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     contextx.Metadata(ctx).CorpBizID(),
		AppBizID:      db.AppBizID,
		DBSourceBizID: db.DBSourceBizID,
	}
	err = l.dao.ModifyDatabaseSimple(ctx, &dbFilter, map[string]any{
		"alive":          db.Alive,
		"last_sync_time": db.LastSyncTime,
		"update_time":    db.UpdateTime, // 心跳探测不更新update_time，避免误以为有更新，需要重新生成schema
	})
	// err = dao.GetDBSourceDao().UpdateByBizID(ctx, contextx.Metadata(ctx).CorpBizID(), db.AppBizID, db.DBSourceBizID, []string{"alive", "last_sync_time"}, db)
	if err != nil {
		logx.E(ctx, "update db source failed: %v", err)
		return err
	}

	return nil
}

// GetAppEmbeddingInfoById 获取机器人对应的 embeddingVersion
func (l *Logic) GetAppEmbeddingInfoById(ctx context.Context, appBizID uint64) (uint64, string, error) {
	app, err := l.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appBizID, entity0.AppTestScenes)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingInfoById|查询机器人数据失败 err:%+v", err)
		return 0, "", err
	}
	logx.D(ctx, "GetAppEmbeddingInfoById| appBizID:%v appDB:%+v", appBizID, app)
	if app.IsDeleted {
		logx.W(ctx, "GetAppEmbeddingInfoById app.HasDeleted()|appID:%d", appBizID)
		return 0, "", fmt.Errorf("app %d has been deleted", appBizID)
	}
	embeddingName := ""
	embeddingVersion := app.Embedding.Version
	logx.I(ctx, "GetAppEmbeddingInfoById app.IsShared()|appID:%d", appBizID)
	embeddingName, err = l.kbLogic.GetKnowledgeEmbeddingModel(ctx, app.CorpBizId, app.BizId, app.BizId, app.IsShared)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingInfoById | GetKnowledgeEmbeddingModel  err:%+v", err)
		return 0, "", err
	}
	if embeddingName != "" {
		embeddingVersion = entity0.GetEmbeddingVersion(embeddingName)
	}

	logx.I(ctx, "GetAppEmbeddingInfoById result: embeddingVersion:%d, embeddingName:%s (app.IsShared:%t)",
		embeddingVersion, embeddingName, app.IsShared)
	return embeddingVersion, embeddingName, nil
}

// ListDbSourcesWithTables 获取列表，数据库源以及其对应的表
func (l *Logic) ListDbSourcesWithTables(ctx context.Context, appBizID uint64,
	pageSize, pageNumber uint32) ([]*entity.Database, int64, error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	logx.D(ctx, "ListDbSourcesWithTables|corpBizID: %d, appBizID: %d, pageSize: %d, pageNumber: %d",
		corpBizID, appBizID, pageSize, pageNumber)
	dbFilter := entity.DatabaseFilter{
		CorpBizID:  corpBizID,
		AppBizID:   appBizID,
		PageSize:   pageSize,
		PageNumber: ptrx.Uint32(pageNumber),
	}
	dbSources, total, err := l.dao.DescribeDatabaseList(ctx, &dbFilter)
	// dbSources, total, err := l.dao.ListOnlyByAppBizID(ctx, appBizID, int(pageNumber), int(pageSize))
	if err != nil {
		return nil, 0, err
	}
	for _, dbSource := range dbSources {
		tableFilter := entity.TableFilter{
			CorpBizID:     dbSource.CorpBizID,
			AppBizID:      dbSource.AppBizID,
			DBSourceBizID: dbSource.DBSourceBizID,
		}
		dbTableList, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
		// dbTableList, err := l.dao.ListAllByDBSourceBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbSource.DBSourceBizID)
		logx.I(ctx, "ListDbSourcesWithTables|get db tables, dbSource: %+v, dbTableList: %+v", dbSource,
			dbTableList)
		if err != nil {
			logx.E(ctx, "BatchGetDbSourcesWithTables|get db tables failed: %v", err)
			return nil, 0, err
		}
		dbSource.Tables = dbTableList
	}
	return dbSources, total, nil
}

// GetDbSourcesWithTables 获取单一数据源和数据表
func (l *Logic) GetDbSourcesWithTables(ctx context.Context, dbSourceBizID uint64) (*entity.Database, error) {
	dbFilter := entity.DatabaseFilter{
		DBSourceBizIDs: []uint64{dbSourceBizID},
	}
	dbSources, _, err := l.dao.DescribeDatabaseList(ctx, &dbFilter)
	// dbSources, err := dao.GetDBSourceDao().BatchGetByBizIDs(ctx, []uint64{dbSourceBizID})
	if err != nil {
		return nil, err
	}
	if len(dbSources) == 0 {
		return nil, errs.ErrDbSourceNoPermission
	}
	dbSource := dbSources[0]
	tableFilter := entity.TableFilter{
		CorpBizID:     dbSource.CorpBizID,
		AppBizID:      dbSource.AppBizID,
		DBSourceBizID: dbSource.DBSourceBizID,
	}
	dbTableList, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
	// dbTableList, err := l.dao.ListAllByDBSourceBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbSource.DBSourceBizID)
	if err != nil {
		logx.E(ctx, "BatchGetDbSourcesWithTables|get db tables failed: %v", err)
		return nil, err
	}
	dbSource.Tables = dbTableList
	return dbSource, nil
}

// IsQueryOperation 判断 SQL 语句是执行操作还是查询操作，支持 MySQL 和 SQL Server
func IsQueryOperation(sqlStr string) bool {
	// 转换为小写以便统一处理
	sqlLower := strings.ToLower(strings.TrimSpace(sqlStr))
	// 查询操作的关键字
	queryKeywords := []string{"select", "show", "describe", "explain"}
	for _, keyword := range queryKeywords {
		if strings.HasPrefix(sqlLower, keyword) {
			return true
		}
	}
	return false
}

type sqlCache struct {
	SqlStr    string   `redis:"sqlStr"`
	SqlParams []string `redis:"sqlParams"`
}

func (l *Logic) RunSql(ctx context.Context, dbSourceBizID uint64, sqlStr string, sqlParams []string) (
	columns []string, data []*pb.RowData, effectCnt int64, errMessage string, err error) {
	logx.D(ctx, "ExecuteSqlForDbSource11| dbSourceBizID: %d, sqlStr: %s, sqlParams: %v",
		dbSourceBizID, sqlStr, sqlParams)

	// NOTE: 下面这段 redis 神操作，不知道是什么原因。据说是为了绕过公司安全检查，执行的 sql 不能来源于参数，所以转存了下
	// 这种只为了躲过安全检查的做法不可取，真正需要做的是 sql 和 params 的校验。不重构到 dao 层了，这里留个 todo，优化下校验吧
	sc := &sqlCache{SqlStr: sqlStr, SqlParams: sqlParams}
	temp, err := jsonx.Marshal(sc)
	if err != nil {
		logx.E(ctx, "ExecuteSqlForDbSource|marshal sql cache failed: %v, sqlCache: %+v", err, sc)
		return nil, nil, 0, "", err
	}
	rdb := l.dao.RedisClient()
	key := fmt.Sprintf("sql:%v", idgen.GetId())
	err = rdb.Set(ctx, key, string(temp), 1*time.Minute).Err()
	if err != nil {
		logx.E(ctx, "ExecuteSqlForDbSource|add sql cache to redis failed: %v, sqlCache: %+v", err, sc)
		return nil, nil, 0, "", err
	}
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		logx.W(ctx, "ExecuteSqlForDbSource|get sql cache from redis failed (redis: nil), will retry after 20ms, sqlCache: %+v", sc)
		// 等待 20ms 后重试
		time.Sleep(20 * time.Millisecond)
		val, err = rdb.Get(ctx, key).Result()
		if err != nil {
			logx.E(ctx, "ExecuteSqlForDbSource|get sql cache from redis failed (second attempt): %v, sqlCache: %+v", err, sc)
			return nil, nil, 0, "", err
		}
	}
	runSql := &sqlCache{}
	err = jsonx.Unmarshal([]byte(val), runSql)
	if err != nil {
		logx.E(ctx, "ExecuteSqlForDbSource|unmarshal sql cache failed: %v, sqlCache: %+v", err, sc)
		return nil, nil, 0, "", err
	}

	dbFilter := entity.DatabaseFilter{
		DBSourceBizID: dbSourceBizID,
	}
	dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
	// dbSource, err := dao.GetDBSourceDao().GetOnlyByBizID(ctx, dbSourceBizID)
	if err != nil {
		logx.E(ctx, "QuerySqlForDbSource|get db source by biz id failed: %v", err)
		return nil, nil, 0, "", err
	}
	if dbSource == nil {
		logx.E(ctx, "QuerySqlForDbSource|db source is nil")
		return nil, nil, 0, "", fmt.Errorf("db source is nil")
	}
	decrypt, err := util.Decrypt(dbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		logx.E(ctx, "QuerySqlForDbSource|decrypt db source password failed: %v", err)
		return nil, nil, 0, "", err
	}
	uin, err := l.dao.GetDBUin(ctx, dbSource.CorpBizID)
	if err != nil {
		return nil, nil, 0, "", err
	}
	connDbSource := entity.DatabaseConn{
		DbType:     dbSource.DBType,
		Host:       dbSource.Host,
		Port:       dbSource.Port,
		Username:   dbSource.Username,
		Password:   decrypt,
		DbName:     dbSource.DBName,
		SchemaName: dbSource.SchemaName,
		CreateTime: &dbSource.CreateTime,
		Uin:        uin,
	}

	if IsQueryOperation(sqlStr) {
		columns, data, errMessage, err = l.dao.QuerySqlForDbSource(ctx, connDbSource, sqlStr, sqlParams)
		if err != nil {
			logx.W(ctx, "RunSql|query sql for db source failed: %v", err)
			return nil, nil, 0, errMessage, err
		}
	} else {
		effectCnt, errMessage, err = l.dao.RunSqlForDbSource(ctx, connDbSource, sqlStr, sqlParams)
		if err != nil {
			logx.E(ctx, "RunSql|exec sql for db source failed: %v", err)
			return nil, nil, 0, errMessage, err
		}
	}
	return
}

// TextToSQLFromKnowledge TODO
func (l *Logic) TextToSQLFromKnowledge(ctx context.Context, req *pb.TextToSQLFromKnowledgeReq) (
	*pb.TextToSQLFromKnowledgeRsp, error) {
	if req == nil || req.GetAppBizId() == 0 || req.GetDbSourceBizId() == 0 || len(req.GetDbSourceTableBizId()) == 0 ||
		utf8.RuneCountInString(req.GetQuery()) == 0 {
		return nil, errs.ErrParameterInvalid
	}
	if len(req.GetDbSourceTableBizId()) > config.App().DbSource.MaxText2SqlTableNum {
		return nil, errs.ErrText2sqlTableNums
	}
	logx.D(ctx, "TextToSQLFromKnowledge req:%v GenerateSqlTimeout:%v", req, config.App().
		DbSource.GenerateSqlTimeout)
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.GenerateSqlTimeout)
	defer cancel()
	appInfo, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "ListDbSource get app info err:%v,appBizId:%v", err, req.GetAppBizId())
		return nil, err
	}
	if appInfo == nil {
		return nil, errs.ErrRobotNotFound
	}
	if appInfo.IsShared {
		req.EnvType = pb.EnvType_Test
	}
	logx.D(ctx, "TextToSQLFromKnowledge req:%v, GetIsShareKnowledgeBase: %v", req,
		appInfo.IsShared)
	generateSql, err := l.textToSqlForDbSource(ctx, req, appInfo)
	if err != nil {
		return nil, err
	}
	rsp := &pb.TextToSQLFromKnowledgeRsp{
		AppBizId:      req.GetAppBizId(),
		DbSourceBizId: req.GetDbSourceBizId(),
		Sql:           generateSql,
	}
	return rsp, nil
}

// textToSqlForDbSource 外部数据库根据用户语句生成对应SQL
func (l *Logic) textToSqlForDbSource(ctx context.Context, req *pb.TextToSQLFromKnowledgeReq, appInfo *entity0.App) (
	string, error) {
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     appInfo.CorpBizId,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
	}
	dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
	if err != nil || dbSource == nil {
		logx.E(ctx, "TextToSqlForDbSource|get db source by biz id failed: %v, dbSourceBizID: %d", err,
			req.GetDbSourceBizId())
		return "", errs.ErrDataNotExistOrIsDeleted
	}

	var validTableIDs []uint64
	if req.GetEnvType() == pb.EnvType_Test {
		tableFilter := entity.TableFilter{
			CorpBizID:     dbSource.CorpBizID,
			AppBizID:      dbSource.AppBizID,
			DBTableBizIDs: req.GetDbSourceTableBizId(),
		}
		dbTables, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
		if err != nil {
			logx.E(ctx, "TextToSqlForDbSource|get db tables by biz ids failed: %v, dbTableBizIDs: %v", err,
				req.GetDbSourceTableBizId())
			return "", err
		}
		for _, dbTable := range dbTables {
			if dbTable.LearnStatus == entity.LearnStatusLearning ||
				dbTable.LearnStatus == entity.LearnStatusFailed {
				return "", errs.ErrWrapf(errs.ErrDbTableStatusForText2Sql, i18n.Translate(ctx,
					i18nkey.KeyDataTableLearningOrFailed), dbTable.Name)
			}
			validTableIDs = append(validTableIDs, dbTable.DBTableBizID)
		}
	} else {
		if dbSource.ReleaseStatus == releaseEntity.ReleaseStatusPending {
			return "", errs.ErrWrapf(errs.ErrDbSourceStatusForText2Sql, i18n.Translate(ctx,
				i18nkey.KeyDatabasePublishingPleaseWait), dbSource.AliasName)
		}
		tableFilter := entity.TableFilter{
			CorpBizID:     dbSource.CorpBizID,
			AppBizID:      dbSource.AppBizID,
			DBTableBizIDs: req.GetDbSourceTableBizId(),
		}
		dbTableProd, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
		if err != nil {
			logx.E(ctx, "TextToSqlForDbSource|get db tables by biz ids failed: %v, dbTableBizIDs: %v", err,
				req.GetDbSourceTableBizId())
			return "", err
		}
		bizIDSets := make(map[uint64]struct{})
		for _, dbTable := range dbTableProd {
			validTableIDs = append(validTableIDs, dbTable.DBTableBizID)
			bizIDSets[dbTable.DBTableBizID] = struct{}{}
		}
		notExist := make([]uint64, 0)
		for _, dbTableBizID := range req.GetDbSourceTableBizId() {
			if _, ok := bizIDSets[dbTableBizID]; !ok {
				notExist = append(notExist, dbTableBizID)
			}
		}
		if len(notExist) > 0 {
			logx.W(ctx, "TextToSqlForDbSource|db table biz ids not exist: %v", notExist)
			tableFilter := entity.TableFilter{
				CorpBizID:     dbSource.CorpBizID,
				AppBizID:      dbSource.AppBizID,
				DBTableBizIDs: notExist,
			}
			dbTablesNotExist, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
			if err != nil {
				logx.E(ctx, "TextToSqlForDbSource|get db tables by biz ids failed: %v, dbTableBizIDs: %v", err, notExist)
				return "", err
			}
			names := make([]string, 0, len(dbTablesNotExist))
			for _, dbTable := range dbTablesNotExist {
				if dbTable != nil {
					names = append(names, dbTable.Name)
				}
			}
			return "", errs.ErrWrapf(errs.ErrDbTableNotExistForProd, i18n.Translate(ctx, i18nkey.KeyDataTableNotPublishedRetry),
				names)
		}
	}

	newCtx := util.SetMultipleMetaData(ctx, appInfo.SpaceId, appInfo.Uin)

	text2sqlConfig, err := l.kbLogic.GetKnowledgeText2sqlModelConfig(ctx, appInfo.CorpBizId, appInfo.BizId,
		appInfo.BizId, appInfo.IsShared)
	if err != nil {
		logx.W(ctx, "TextToSqlForDbSource|get knowledge text2sql model config failed: %v", err)
		//return "", err
	}

	genSQLReq := &retrieve.GenerateSQLReq{
		RobotId:            appInfo.PrimaryId,
		AppBizId:           req.GetAppBizId(),
		DbSourceBizId:      req.GetDbSourceBizId(),
		DbSourceTableBizId: validTableIDs,
		Query:              req.GetQuery(),
		EnvType:            retrieve.EnvType(req.GetEnvType()),
		SqlTemplate:        req.GetSqlTemplate(),
		KnowledgeTemplate:  req.GetKnowledgeTemplate(),
	}
	if text2sqlConfig != nil {
		genSQLReq.Text2SqlModelConfig = text2sqlConfig
	}
	rsp, err := l.rpc.RetrievalDirectIndex.GenerateSQL(newCtx, genSQLReq)
	if err != nil {
		logx.E(newCtx, "TextToSqlForDbSource|generate sql failed: %v, req: %+v", err, genSQLReq)
		if terrs.Code(err) == 102 {
			return "", errs.ErrText2sqlTimeOut
		}
		return "", err
	}
	return rsp.GenerateSql, nil
}

func (l *Logic) ShowDatabases(ctx context.Context, db entity.DatabaseConn) ([]string, error) {
	return l.dao.ShowDatabases(ctx, db)
}

func (l *Logic) GetDBTableList(ctx context.Context, connDbSource entity.DatabaseConn,
	page, pageSize int) (tables []string, total int, err error) {
	return l.dao.GetDBTableList(ctx, connDbSource, page, pageSize)
}

func (l *Logic) ModifyDatabaseSimple(ctx context.Context, filter *entity.DatabaseFilter,
	updateMap map[string]any) error {
	return l.dao.ModifyDatabaseSimple(ctx, filter, updateMap)
}

// ModifyDatabase 更新数据库
// TODO(ericjwang): 这里只处理了新增的表，那删除的表是在哪里处理的？
func (l *Logic) ModifyDatabase(ctx context.Context, req *pb.UpdateDbSourceReq) (*entity.Database, error) {
	logx.I(ctx, "UpdateDatabase: %v", req)
	if len(req.TableNames) == 0 {
		return nil, errs.ErrWrapf(errs.ErrDbTableNumIsInvalid, i18n.Translate(ctx, i18nkey.KeySingleAddTableRange),
			config.App().DbSource.MaxTableNumOnce)
	}

	if utf8.RuneCountInString(req.GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		entity.MaxDbSourceAliasNameLength) || utf8.RuneCountInString(
		req.GetDescription()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		entity.MaxDbSourceDescriptionLength) {
		return nil, errs.ErrDbSourceInputExtraLong
	}

	copBizId := contextx.Metadata(ctx).CorpBizID()

	// 1. 获取原始数据库
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     copBizId,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
	}
	dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
	if err != nil {
		logx.E(ctx, "get db source failed: %v", err)
		return nil, errs.ErrUpdateDbSourceGetFail
	}

	if dbSource.DBName != req.GetDbName() {
		return nil, errs.ErrWrapf(errs.ErrDbNameIsInvalid, i18n.Translate(ctx, i18nkey.KeyDatabaseNameNotAllowedModify))
	}

	// 1.1 判断数据库名称是否被禁用
	err = l.rpc.InfoSec.CheckDbSourceField(ctx, req.GetAppBizId(), dbSource.DBSourceBizID, releaseEntity.AuditDbSourceName,
		req.GetAliasName())
	if err != nil {
		return nil, err
	}
	// 1.2 判断数据库描述是否存在敏感词
	err = l.rpc.InfoSec.CheckDbSourceField(ctx, req.GetAppBizId(), dbSource.DBSourceBizID, releaseEntity.AuditDbSourceDesc,
		req.GetDescription())
	if err != nil {
		return nil, err
	}

	// 2. 判断数据库是否已经存在
	tableFilter := entity.TableFilter{
		CorpBizID:     copBizId,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
	}
	dbTableList, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
	// dbTableList, err := l.dao.ListAllByDBSourceBizID(ctx, copBizId, req.GetAppBizId(), req.GetDbSourceBizId())
	if err != nil {
		logx.E(ctx, "get db table failed: %v", err)
		return nil, errs.ErrUpdateDbSourceGetFail
	}

	isEnableScopedChange := false

	if req.GetEnableScope() != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN && dbSource.EnableScope != uint32(req.GetEnableScope()) {
		isEnableScopedChange = true
		dbSource.EnableScope = uint32(req.GetEnableScope())
	}

	learnTableBizIds := make([]uint64, 0)

	localDbTables := make(map[string]*entity.Table)
	for _, dbTable := range dbTableList {
		localDbTables[dbTable.Name] = dbTable
		if isEnableScopedChange {
			dbTable.EnableScope = dbSource.EnableScope
			learnTableBizIds = append(learnTableBizIds, dbTable.DBTableBizID)
		}
	}

	newTable := make([]string, 0)
	for _, tableName := range req.GetTableNames() {
		if _, ok := localDbTables[tableName]; !ok {
			newTable = append(newTable, tableName)
		}
	}

	if len(newTable)+len(dbTableList) > config.App().DbSource.MaxTableNum {
		return nil, errs.ErrWrapf(errs.ErrDbTableNumIsInvalid, i18n.Translate(ctx, i18nkey.KeyDatabaseMaxAddAmount),
			config.App().DbSource.MaxTableNum)
	}

	// 2. 批量创建数据表和列
	newCreateTables, err := l.BatchCreateDbTableAndColumn(ctx, dbSource, newTable)
	if err != nil {
		logx.E(ctx, "batch create db table and column failed: %v", err)
		return nil, err
	}

	for _, dbTable := range newCreateTables {
		learnTableBizIds = append(learnTableBizIds, dbTable.DBTableBizID)
	}

	// 3. 获取 robotId
	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	robotId := appDB.PrimaryId

	password, err := entity.DecodePassword(req.GetPassword())
	if err != nil {
		logx.W(ctx, "decode password failed: req: %v err:%v", req, err)
		return nil, err
	}

	// 4. 更新数据库
	encryptedPassword, err := util.Encrypt(password, config.App().DbSource.Salt)
	if err != nil {
		logx.E(ctx, "encrypt password failed: %v", err)
		return nil, err
	}
	dbSource.Password = encryptedPassword
	dbSource.Description = req.GetDescription()
	dbSource.AliasName = req.GetAliasName()
	dbSource.Username = req.GetUsername()
	dbSource.StaffID = contextx.Metadata(ctx).StaffID()
	dbSource.LastSyncTime = time.Now()

	dbSource.ReleaseStatus = releaseEntity.ReleaseStatusInit
	if dbSource.NextAction != releaseEntity.ReleaseActionAdd {
		dbSource.NextAction = releaseEntity.ReleaseActionUpdate
	}

	err = l.dao.ModifyDatabase(ctx, copBizId, req.GetAppBizId(), req.GetDbSourceBizId(),
		[]string{
			"alias_name",
			"description",
			"username",
			"password",
			"enable_scope",
			"staff_id",
			"release_status",
			"last_sync_time"},
		dbSource)
	if err != nil {
		logx.E(ctx, "update db source failed: %v", err)
		return nil, errs.ErrUpdateDbSourceCreateFail
	}

	if isEnableScopedChange {
		logx.D(ctx, "update dbTable enable_scope and learn_status field of dbsource: %d", req.GetDbSourceBizId())
		// 对所有现有表同时更新 enable_scope 和 learn_status
		if err = l.dao.ModifyTable(ctx, &tableFilter, map[string]any{
			"enable_scope": dbSource.EnableScope,
			"learn_status": entity.LearnStatusLearning,
		}); err != nil {
			logx.E(ctx, "update table enable scope and learn status of dbsource:%d failed: %v", req.GetDbSourceBizId(), err)
			return nil, errs.ErrUpdateDbSourceGetFail
		}
	}

	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(5)
	// 5. 保存到 es
	for _, value := range learnTableBizIds {
		dbTableId := value
		wg.Go(func() (err error) {
			err = l.CreateDbTableLearnTask(ctx, dbSource, robotId, dbTableId)
			if err != nil {
				logx.E(wgCtx, "CreateDbTableLearnTask failed, table:%v err:%v", dbTableId, err)
				return errs.ErrWriteIntoEsFail
			}
			return nil
		})
	}
	if err = wg.Wait(); err != nil {
		logx.E(ctx, "UpdateDbSource|AddDbTableData2ES1 dbSource %v; dbTable %v; failed: %v", dbSource,
			newCreateTables, err)
		return nil, err
	}

	return dbSource, nil
}

func (l *Logic) DescribeDatabaseList(ctx context.Context, filter *entity.DatabaseFilter) ([]*entity.Database, int64, error) {
	dbs, count, err := l.dao.DescribeDatabaseList(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	if filter.WithSyncAlive {
		if err = l.SyncDatabasesAlive(ctx, dbs); err != nil {
			logx.E(ctx, "sync db sources alive failed: %v", err)
			return nil, 0, err
		}
	}

	if filter.WithTable {
		// 如果数据表存在学习中，则将数据库的状态修改为学习中
		dbSourcesBizIDs := slicex.Pluck(dbs, func(i *entity.Database) uint64 { return i.DBSourceBizID })
		tableFilter := entity.TableFilter{
			CorpBizID:      filter.CorpBizID,
			DBSourceBizIDs: dbSourcesBizIDs,
			DBTableBizIDs:  filter.DBTableBizIDs,
			NameLike:       filter.TableNameLike,
			PageNumber:     filter.TablePageNumber,
			PageSize:       filter.TablePageSize,
		}
		tables, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
		if err != nil {
			return nil, 0, err
		}
		learningDatabaseMap := make(map[uint64]struct{})
		tablesMap := make(map[uint64][]*entity.Table)
		for _, table := range tables {
			tablesMap[table.DBSourceBizID] = append(tablesMap[table.DBSourceBizID], table)
			if table.LearnStatus == entity.LearnStatusLearning {
				learningDatabaseMap[table.DBSourceBizID] = struct{}{}
			}
		}
		for _, db := range dbs {
			if _, ok := learningDatabaseMap[db.DBSourceBizID]; ok {
				db.ReleaseStatus = entity.FaceStatusLearning
			}
			if tableList, ok := tablesMap[db.DBSourceBizID]; ok {
				db.Tables = tableList
			}
		}
	}

	if filter.WithStaffName {
		// 获取员工名称
		staffIDs := slicex.Pluck(dbs, func(i *entity.Database) uint64 { return i.StaffID })
		staffs, err := l.rpc.PlatformAdmin.DescribeStaffList(ctx, &pm.DescribeStaffListReq{
			StaffIds: staffIDs,
		})
		if err != nil { // 失败降级为返回员工ID
			logx.E(ctx, "DescribeDatabaseList get staff name staffIDs:%v, error:%v", staffIDs, err)
		}
		for _, db := range dbs {
			if staff, ok := staffs[db.StaffID]; ok {
				db.StaffName = staff.GetNickName()
			} else {
				db.StaffName = fmt.Sprintf("%d", db.StaffID)
			}
		}
	}

	return dbs, count, nil
}

func (l *Logic) DescribeDatabase(ctx context.Context, filter *entity.DatabaseFilter) (*entity.Database, error) {
	db, err := l.dao.DescribeDatabase(ctx, filter)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errs.ErrDataNotExistOrIsDeleted
	}
	return db, nil
}

func (l *Logic) QueryDBSchemas(ctx context.Context, connDbSource entity.DatabaseConn) (schemas []string, err error) {
	return l.dao.QueryDBSchemas(ctx, connDbSource)
}

// DescribeDatabaseProd 查询发布域数据库
func (l *Logic) DescribeDatabaseProd(ctx context.Context, filter *entity.DatabaseFilter) (*entity.DatabaseProd, error) {
	db, err := l.dao.DescribeDatabaseProd(ctx, filter)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errs.ErrDataNotExistOrIsDeleted
	}
	return db, nil
}

// ListReleaseDb 发布数据库查看
func (l *Logic) ListReleaseDb(ctx context.Context, req *pb.ListReleaseDbReq) (*pb.ListReleaseDbRsp, error) {
	var list []*pb.ReleaseDb
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if req.GetReleaseBizId() == 0 {
		var startTime, endTime time.Time
		if req.GetStartTime() != 0 {
			startTime = time.Unix(req.GetStartTime(), 0)
		}
		if req.GetEndTime() != 0 {
			endTime = time.Unix(req.GetEndTime(), 0)
		}

		dbSources, err := l.dao.FindUnreleaseDatabaseByConds(ctx, corpBizID, req.GetAppBizId(), req.GetQuery(),
			startTime, endTime, req.GetActions(), req.GetPageNumber(), req.GetPageSize())
		if err != nil {
			return nil, err
		}

		for _, release := range dbSources {
			list = append(list, &pb.ReleaseDb{
				DbSourceBizId: release.DBSourceBizID,
				DbName:        release.DBName,
				UpdateTime:    uint64(release.UpdateTime.Unix()),
				Action:        release.NextAction,
				ActionDesc:    i18n.Translate(ctx, docEntity.DocActionDesc[release.NextAction]),
			})
		}
	} else {
		releaseDBSource, err := l.dao.DescribeReleaseDatabaseList(ctx, req.GetAppBizId(), req.GetReleaseBizId())
		if err != nil {
			return nil, err
		}

		for _, release := range releaseDBSource {
			list = append(list, &pb.ReleaseDb{
				DbSourceBizId: release.DBSourceBizID,
				DbName:        release.DBName,
				UpdateTime:    uint64(release.UpdateTime.Unix()),
				Action:        release.Action,
				ActionDesc:    i18n.Translate(ctx, docEntity.DocActionDesc[release.Action]),
			})
		}
	}

	rsp := &pb.ListReleaseDbRsp{Total: int32(len(list)), List: list}
	return rsp, nil
}

func (l *Logic) GetUnreleasedDBSource(ctx context.Context, appBizID uint64) ([]*entity.Database, error) {
	return l.dao.DescribeUnreleasedDatabase(ctx, appBizID)
}

func (l *Logic) CollectUnreleasedDBSource(ctx context.Context, appBizID, releaseBizID uint64) error {
	return l.dao.CollectUnreleasedDatabase(ctx, appBizID, releaseBizID)
}

func (l *Logic) GetAllReleaseDBSources(ctx context.Context, appBizID, releaseBizID uint64) ([]*entity.DatabaseRelease,
	error) {
	return l.dao.DescribeReleaseDatabaseList(ctx, appBizID, releaseBizID)
}

func (l *Logic) CheckDbTableIsExisted(ctx context.Context, dbSource *entity.Database, table string) (bool, error) {
	return l.dao.CheckDbTableIsExisted(ctx, dbSource, table)
}

func (l *Logic) ReleaseDBSource(ctx context.Context, appBizID, releaseBizID uint64) error {
	return l.dao.ReleaseDBSource(ctx, appBizID, releaseBizID)
}

// IsTableValidInEnv 判断表在指定环境下是否有效
// envType: 环境类型（Test或Prod）
// table: 数据库表信息
// 返回: true表示表在当前环境有效，false表示无效
func (l *Logic) IsTableValidInEnv(envType pb.EnvType, table *entity.Table) bool {
	// 如果启用范围是全部，则直接返回true
	if table.EnableScope == entity0.EnableScopeAll {
		return true
	}

	// 如果启用范围无效，则返回false
	if table.EnableScope == entity0.EnableScopeInvalid {
		return false
	}

	// 测试环境且启用范围为开发环境
	if envType == pb.EnvType_Test && table.EnableScope == entity0.EnableScopeDev {
		return true
	}

	// 生产环境且启用范围为发布环境
	if envType == pb.EnvType_Prod && table.EnableScope == entity0.EnableScopePublish {
		return true
	}

	return false
}

// ValidateSqlTables 验证SQL表权限
func (l *Logic) ValidateSqlTables(ctx context.Context, app *entity0.App,
	req *pb.ExecuteSqlForDbSourceReq) (map[uint64]*entity.Table, error) {
	executeTableInfos := make(map[uint64]*entity.Table)
	// 知识库概念统一刷数据需要兼容工作流和agent绑定文档的场景
	// 如果是检索发布域，需要先从临时表中查询是否该文档从发布域拷贝数据到了开发域
	// 先查询docBizId拷贝后的新docBizId（从发布域拷贝到开发域的映射关系）
	dbSourceTableBizIds := req.GetDbSourceTableBizId()
	if req.GetEnvType() == pb.EnvType_Prod {
		devReleaseRelationMap, err := l.releaseDao.GetDevReleaseRelationInfoList(ctx, app.CorpPrimaryId, app.PrimaryId,
			releaseEntity.DevReleaseRelationTypeTable, req.GetDbSourceTableBizId())
		if err != nil {
			logx.W(ctx, "ValidateSqlTables GetDevReleaseRelationInfoList err:%v", err)
		} else if len(devReleaseRelationMap) > 0 {
			// 如果查询到了映射关系，将原dbSourceTableBizIds中可以替换的替换掉，不能替换的继续保留
			newDbSourceTableBizIds := make([]uint64, 0, len(dbSourceTableBizIds))
			for _, dbSourceTableBizId := range dbSourceTableBizIds {
				if newDbSourceTableBizId, ok := devReleaseRelationMap[dbSourceTableBizId]; ok {
					newDbSourceTableBizIds = append(newDbSourceTableBizIds, newDbSourceTableBizId)
				} else {
					newDbSourceTableBizIds = append(newDbSourceTableBizIds, dbSourceTableBizId)
				}
			}
			dbSourceTableBizIds = newDbSourceTableBizIds
			logx.I(ctx, "ValidateSqlTables found dev-release relation, use new dbSourceTableBizIds:%v", dbSourceTableBizIds)
		}
	}
	// 获取用户选择的表信息
	rawDbTables, err := l.dao.BatchGetTableByBizIDs(ctx, app.CorpBizId, req.GetAppBizId(), dbSourceTableBizIds)
	if err != nil {
		logx.E(ctx, "ExecuteSqlForDbSource GetDBTables fail", "err", err)
		return executeTableInfos, err
	}
	logx.I(ctx, "ExecuteSqlForDbSource user Tables:%s", jsonx.MustMarshalToString(rawDbTables))
	// 验证SQL表的生效范围
	envType := req.GetEnvType()
	dbTables := slicex.Filter(rawDbTables, func(t *entity.Table) bool {
		return l.IsTableValidInEnv(envType, t)
	})
	logx.I(ctx, "ExecuteSqlForDbSource filter enable_scoped Tables:%s", jsonx.MustMarshalToString(dbTables))

	// 解析SQL中的表
	sqlTables, err := getTablesFromSql(strings.ToLower(req.GetDbType()), req.GetSqlToExecute())
	if err != nil {
		logx.E(ctx, "ExecuteSqlForDbSource get execute table fail", "err", err)
		return executeTableInfos, err
	}
	logx.I(ctx, "ExecuteSqlForDbSource execute Tables:%s", jsonx.MustMarshalToString(sqlTables))
	// 构建用户表名集合
	userTables := make(map[string]*entity.Table, len(dbTables))
	for _, table := range dbTables {
		userTables[strings.ToLower(table.Name)] = table
	}
	// 验证SQL表是否在允许范围内
	for _, table := range sqlTables {
		info, exists := userTables[strings.ToLower(table)]
		if !exists {
			return executeTableInfos, fmt.Errorf(
				"SQL statement references table '%s' not included in the allowed data table range", table)
		}
		executeTableInfos[info.DBTableBizID] = info
	}
	return executeTableInfos, nil
}

// BuildColumnInfos 构建列详细信息
func (l *Logic) BuildColumnInfos(ctx context.Context, app *entity0.App, req *pb.ExecuteSqlForDbSourceReq,
	executeTables map[uint64]*entity.Table, columns []string) ([]*pb.DbTableColumnView, error) {
	// 1. 获取所有表的列信息
	tableColumns, err := l.getTableColumns(ctx, app, req, executeTables)
	if err != nil {
		logx.E(ctx, "Failed to get table columns", "err", err)
		return nil, err
	}
	log.InfoContext(ctx, "ExecuteSqlForDbSource tableColumns:", jsonx.MustMarshalToString(tableColumns))
	// 2. 构建列名到列信息的映射
	columnMap := buildColumnMap(executeTables, tableColumns)
	log.InfoContext(ctx, "ExecuteSqlForDbSource columnMap:", jsonx.MustMarshalToString(columnMap))
	// 3. 构建列详细信息
	columnInfos := make([]*pb.DbTableColumnView, 0, len(columns))
	for _, column := range columns {
		columnInfo := buildColumnInfo(ctx, column, columnMap)
		columnInfos = append(columnInfos, columnInfo)
	}
	return columnInfos, nil
}

// buildColumnMap 构建列名到列信息的映射
func buildColumnMap(executeTables map[uint64]*entity.Table,
	tableColumns map[uint64][]*entity.Column,
) map[string][]*pb.DbTableColumnView {
	columnMap := make(map[string][]*pb.DbTableColumnView)
	for tableBizID, columns := range tableColumns {
		_, exists := executeTables[tableBizID]
		if !exists {
			continue
		}
		for _, col := range columns {
			meta := &pb.DbTableColumnView{
				ColumnName:  col.ColumnName,
				DataType:    col.DataType,
				Description: col.ColumnComment,
				Unit:        col.Unit,
			}
			columnMap[col.ColumnName] = append(columnMap[col.ColumnName], meta)
		}
	}
	return columnMap
}

// buildColumnInfo 构建单个列信息
func buildColumnInfo(ctx context.Context, column string,
	columnMap map[string][]*pb.DbTableColumnView) *pb.DbTableColumnView {
	columnInfo := &pb.DbTableColumnView{
		ColumnName: column,
	}
	// 查找对应的列元数据
	cols, exists := columnMap[column]
	if !exists {
		logx.W(ctx, "Column '%s' not found in table", column)
		return columnInfo
	}
	// 处理同名列（联表查询场景）
	if len(cols) > 1 { // 联表场景：默认取第一个,这里预留分支，后续联表场景可能需要更复杂的处理逻辑
		col := cols[0]
		logx.W(ctx, "Column '%s' has more than one column", column)
		columnInfo = col
	} else { // 单表场景
		columnInfo = cols[0]
	}
	return columnInfo
}

// getTableColumns 获取所有表的列信息
func (l *Logic) getTableColumns(ctx context.Context, app *entity0.App, req *pb.ExecuteSqlForDbSourceReq,
	executeTables map[uint64]*entity.Table) (map[uint64][]*entity.Column, error) {
	// 获取执行表的列详情
	executeTableBizIds := make([]uint64, 0, len(executeTables))
	for tableBizID := range executeTables {
		executeTableBizIds = append(executeTableBizIds, tableBizID)
	}
	return l.dao.BatchGetByTableBizID(ctx, app.CorpBizId, app.BizId, executeTableBizIds)
}

// getTablesFromSql 解析sql中涉及到的表名
func getTablesFromSql(dbName string, sql string) ([]string, error) {
	var dbType sqlParser.DatabaseType
	switch dbName {
	case "mysql":
		dbType = sqlParser.MySQL
	case "sqlserver":
		dbType = sqlParser.SQLServer
	case "oracle":
		dbType = sqlParser.Oracle
	case "postgres", "postgresql":
		dbType = sqlParser.Postgres
	default:
		return nil, fmt.Errorf("unsupported database type")
	}
	tables, err := sqlParser.ParseSingleTables(dbType, sql)
	if err != nil {
		return nil, err
	}
	return tables, nil
}
