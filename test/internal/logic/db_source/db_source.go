// Package db_source TODO
package db_source

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	errgroup "git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieve "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

// BuildDbSource 构建数据库
func BuildDbSource(ctx context.Context, corpBizId, appBizId uint64, aliasName, description string,
	connDbSource model.ConnDbSource, getDao dao.Dao) (*model.DBSource, error) {

	encryptedPassword, err := util.Encrypt(connDbSource.Password)
	if err != nil {
		log.ErrorContextf(ctx, "encrypt password failed: %v", err)
		return nil, err
	}
	connDbSource.Host = strings.TrimSpace(connDbSource.Host)
	connDbSource.Username = strings.TrimSpace(connDbSource.Username)
	dbSource := &model.DBSource{
		CorpBizID:     corpBizId,
		AppBizID:      appBizId,
		DBSourceBizID: getDao.GenerateSeqID(),
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
		ReleaseStatus: model.ReleaseStatusUnreleased,
		NextAction:    model.ReleaseActionAdd,
		IsDeleted:     false,
		IsIndexed:     true,
		CreateTime:    time.Now(),
		UpdateTime:    time.Now(),
		StaffID:       pkg.StaffID(ctx),
	}
	return dbSource, nil
}

// AddDbSource 添加数据库
func AddDbSource(ctx context.Context, appBizId uint64, aliasName, description string,
	connDbSource model.ConnDbSource, tableNames []string, getDao dao.Dao) (*model.DBSource, error) {

	corpBizId := pkg.CorpBizID(ctx)

	cnt, err := dao.GetDBSourceDao().GetDbSourceNumByAppBizID(ctx, corpBizId, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "get db source num failed: %v", err)
		return nil, err
	}
	if cnt > int64(config.App().DbSource.MaxDbSourceNum) {
		return nil, errs.ErrDbSourceLimitExceeded
	}

	// 1. 新建数据库, 但是不保存到数据库
	dbSource, err := BuildDbSource(ctx, corpBizId, appBizId, aliasName, description, connDbSource, getDao)
	if err != nil {
		log.ErrorContextf(ctx, "create db source failed: %v", err)
		return nil, err
	}

	err = dao.GetDBSourceDao().CheckDbSourceField(ctx, appBizId, dbSource.DBSourceBizID, model.AuditDbSourceName,
		dbSource.AliasName, getDao)
	if err != nil {
		return nil, err
	}

	err = dao.GetDBSourceDao().CheckDbSourceField(ctx, appBizId, dbSource.DBSourceBizID, model.AuditDbSourceDesc,
		dbSource.Description, getDao)
	if err != nil {
		return nil, err
	}

	// 2. 批量创建数据表和列
	dbTables, err := BatchCreateDbTableAndColumn(ctx, dbSource, tableNames, getDao)
	if err != nil {
		log.ErrorContextf(ctx, "create db table failed: %v", err)
		return nil, err
	}
	// 3. 获取 robotId
	robotId, err := GetRobotIdByAppBizId(ctx, appBizId)
	if err != nil {
		return nil, err
	}

	wg, wgCtx := errgroup.WithContext(ctx)
	wg.SetLimit(5)
	// 4. 保存到 es
	for _, value := range dbTables {
		table := value
		wg.Go(func() (err error) {
			err = CreateDbTableLearnTask(ctx, robotId, corpBizId, appBizId, table.DBTableBizID, dbSource, getDao)
			if err != nil {
				log.ErrorContextf(wgCtx, "CreateDbTableLearnTask failed, table:%v err:%v", table.DBTableBizID, err)
				return err
			}
			return nil
		})
	}
	if err = wg.Wait(); err != nil {
		log.ErrorContextf(ctx, "AddDbSource|AddDbTableData2ES1 dbSource %v; dbTable %v; failed: %v", dbSource, dbTables, err)
		return nil, err
	}

	// 5. 保存数据库
	err = dao.GetDBSourceDao().Create(ctx, dbSource)
	if err != nil {
		return nil, err
	}

	return dbSource, nil
}

// DeleteDbSourceAndTableCol 删除数据库和数据库表以及列
func DeleteDbSourceAndTableCol(ctx context.Context, appBizId, dbSourceBizId uint64, getDao dao.Dao) error {

	corpBizId := pkg.CorpBizID(ctx)

	// 1. 删除数据库表和列
	err := DeleteTablesAndColByDbSourceBizID(ctx, corpBizId, appBizId, dbSourceBizId, getDao)
	if err != nil {
		log.ErrorContextf(ctx, "delete db table failed: %v", err)
		return err
	}

	// 2. 删除数据库
	err = dao.GetDBSourceDao().SoftDeleteByBizID(ctx, corpBizId, appBizId, dbSourceBizId)
	if err != nil {
		log.ErrorContextf(ctx, "soft delete db source failed: %v", err)
		return err
	}
	return nil

}

// UpdateDbSourceState 更新数据库状态， 如果较长时间没有同步 且 没有失效，触发同步流程
func UpdateDbSourceState(ctx context.Context, dbSource *model.DBSource) error {
	// 1. 判断是否需要同步, 如果较长时间没有同步 且 没有失效，触发同步流程
	if time.Since(dbSource.LastSyncTime).Minutes() < 30 {
		return nil
	}
	log.InfoContextf(ctx, "too long 30 minutes not sync, start sync db source: %v", dbSource)

	// 2. 获取数据库连接
	decrypt, err := util.Decrypt(dbSource.Password)
	if err != nil {
		log.ErrorContextf(ctx, "decrypt password failed: %v", err)
		return err
	}
	connDbSource := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: decrypt,
		Port:     dbSource.Port,
	}
	connection, err := dao.GetDBSourceDao().GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "UpdateDbSourceState|get db connection failed: %v", err)
	}
	defer func() {
		if connection != nil {
			CloseErr := connection.Close()
			if CloseErr != nil {
				log.ErrorContextf(ctx, "close db connection failed: %v", err)
			}
		}
	}()

	// 3. 判断数据库连接是否正常
	if err != nil {
		// 3.1 如果连接失败，将数据库状态置为失效
		log.ErrorContextf(ctx, "get db connection failed: %v", err)
		dbSource.Alive = false
	} else {
		dbSource.Alive = true
	}

	// 4. 如果连接正常，将数据库状态置为正常
	dbSource.LastSyncTime = time.Now()
	err = dao.GetDBSourceDao().UpdateByBizID(ctx, pkg.CorpBizID(ctx), dbSource.AppBizID, dbSource.DBSourceBizID, []string{
		"alive", "last_sync_time"}, dbSource)
	if err != nil {
		log.ErrorContextf(ctx, "update db source failed: %v", err)
		return err
	}

	return nil
}

// GetAppEmbeddingVersionById 获取机器人对应的 embeddingVersion
func GetAppEmbeddingVersionById(ctx context.Context, robotId uint64, getDao dao.Dao) (uint64, error) {
	appDB, err := getDao.GetAppByID(ctx, robotId)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppEmbeddingVersionById|查询机器人数据失败 err:%+v", err)
		return 0, err
	}
	log.DebugContextf(ctx, "GetAppEmbeddingVersionById| robotId:%v appDB:%+v", robotId, appDB)
	if appDB.HasDeleted() {
		log.WarnContextf(ctx, "GetAppEmbeddingVersionById appDB.HasDeleted()|appID:%d", appDB.GetAppID())
		return 0, fmt.Errorf("app %d has been deleted", appDB.GetAppID())
	}
	embeddingConf, _, err := appDB.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx,
			"GetAppEmbeddingVersionById 向量同步,查询机器人数据失败 robots[0].GetEmbeddingConf() err:%+v", err)
		return 0, err
	}
	embeddingVersion := embeddingConf.Version
	return embeddingVersion, nil
}

// BatchGetDbSourcesWithTables 批量获取数据库源和表
func BatchGetDbSourcesWithTables(ctx context.Context, dbSourceBizIDs []uint64) ([]*pb.DbSourceWithTables, error) {
	dbSourcesList, err := dao.GetDBSourceDao().BatchGetByBizIDs(ctx, dbSourceBizIDs)
	if err != nil {
		log.ErrorContextf(ctx, "BatchGetDbSourcesWithTables|get db sources failed: %v", err)
		return nil, err
	}
	corpBizID := pkg.CorpBizID(ctx)
	var dbSourceWithTablesList []*pb.DbSourceWithTables
	for _, dbSource := range dbSourcesList {
		if dbSource.CorpBizID != corpBizID {
			continue
		}
		dbTableList, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, corpBizID, dbSource.AppBizID,
			dbSource.DBSourceBizID)
		if err != nil {
			log.ErrorContextf(ctx, "BatchGetDbSourcesWithTables|get db tables failed: %v", err)
			return nil, err
		}
		view := DbSourceWithTablesToView(dbSource, dbTableList)
		dbSourceWithTablesList = append(dbSourceWithTablesList, view)
	}
	return dbSourceWithTablesList, nil
}

// ListDbSourcesWithTables 获取列表，数据库源以及其对应的表
func ListDbSourcesWithTables(ctx context.Context, appBizID uint64, pageSize,
	pageNumber uint32) ([]*pb.DbSourceBizIDItem, int64, error) {
	corpBizID := pkg.CorpBizID(ctx)
	log.DebugContextf(ctx, "ListDbSourcesWithTables|corpBizID: %d, appBizID: %d, pageSize: %d, pageNumber: %d", corpBizID,
		appBizID, pageSize, pageNumber)
	dbSources, total, err := dao.GetDBSourceDao().ListOnlyByAppBizID(ctx, appBizID, int(pageNumber), int(pageSize))
	if err != nil {
		return nil, 0, err
	}
	var dbSourceItems []*pb.DbSourceBizIDItem
	for _, dbSource := range dbSources {
		dbTableList, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID,
			dbSource.DBSourceBizID)
		log.InfoContextf(ctx, "ListDbSourcesWithTables|get db tables, dbSource: %+v, dbTableList: %+v", dbSource, dbTableList)
		if err != nil {
			log.ErrorContextf(ctx, "BatchGetDbSourcesWithTables|get db tables failed: %v", err)
			return nil, 0, err
		}
		view := DbSourceWithTableToView(dbSource, dbTableList)
		if view != nil {
			dbSourceItems = append(dbSourceItems, view)
		}
	}
	return dbSourceItems, total, nil
}

// GetDbSourcesWithTables 获取单一数据源和数据表
func GetDbSourcesWithTables(ctx context.Context, dbSourceBizID uint64) ([]*pb.DbSourceBizIDItem, error) {
	dbSources, err := dao.GetDBSourceDao().BatchGetByBizIDs(ctx, []uint64{dbSourceBizID})
	if err != nil {
		return nil, err
	}
	if len(dbSources) == 0 {
		return nil, errs.ErrDbSourceNoPermission
	}
	dbSource := dbSources[0]
	var dbSourceItems []*pb.DbSourceBizIDItem
	dbTableList, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID,
		dbSource.DBSourceBizID)
	if err != nil {
		log.ErrorContextf(ctx, "BatchGetDbSourcesWithTables|get db tables failed: %v", err)
		return nil, err
	}
	view := DbSourceWithTableToView(dbSource, dbTableList)
	if view != nil {
		dbSourceItems = append(dbSourceItems, view)
	}
	return dbSourceItems, nil
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

// SqlCache TODO
type SqlCache struct {
	SqlStr    string   `redis:"sqlStr"`
	SqlParams []string `redis:"sqlParams"`
}

// RunSql TODO
func RunSql(ctx context.Context, appBizID, dbSourceBizID uint64, sqlStr string, sqlParams []string, getDao dao.Dao) (
	columns []string, data []*pb.RowData, effectCnt int64, errMessage string, err error) {
	log.DebugContextf(ctx, "ExecuteSqlForDbSource11|appBizID: %d, dbSourceBizID: %d, sqlStr: %s, sqlParams: %v",
		appBizID, dbSourceBizID, sqlStr, sqlParams)
	client, err := redis.GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "ExecuteSqlForDbSource|get redis client failed: %v", err)
		return nil, nil, 0, "", err
	}
	key := fmt.Sprintf("sql:%v", getDao.GenerateSeqID())
	sqlCache := &SqlCache{
		SqlStr:    sqlStr,
		SqlParams: sqlParams,
	}
	temp, err := json.Marshal(sqlCache)
	if err != nil {
		log.ErrorContextf(ctx, "ExecuteSqlForDbSource|marshal sql cache failed: %v, sqlCache: %+v", err, sqlCache)
		return nil, nil, 0, "", err
	}

	err = client.Set(ctx, key, string(temp), 1*time.Minute).Err()
	if err != nil {
		log.ErrorContextf(ctx, "ExecuteSqlForDbSource|add sql cache to redis failed: %v, sqlCache: %+v", err, sqlCache)
		return nil, nil, 0, "", err
	}
	val, err := client.Get(ctx, key).Result()
	if err != nil {
		log.ErrorContextf(ctx, "ExecuteSqlForDbSource|get sql cache from redis failed: %v, sqlCache: %+v", err, sqlCache)
		return nil, nil, 0, "", err
	}
	runSql := &SqlCache{}
	err = json.Unmarshal([]byte(val), runSql)
	if err != nil {
		log.ErrorContextf(ctx, "ExecuteSqlForDbSource|unmarshal sql cache failed: %v, sqlCache: %+v", err, sqlCache)
		return nil, nil, 0, "", err
	}

	dbSource, err := dao.GetDBSourceDao().GetOnlyByBizID(ctx, dbSourceBizID)
	if err != nil {
		log.ErrorContextf(ctx, "QuerySqlForDbSource|get db source by biz id failed: %v", err)
		return nil, nil, 0, "", err
	}
	if dbSource == nil {
		log.ErrorContextf(ctx, "QuerySqlForDbSource|db source is nil")
		return nil, nil, 0, "", fmt.Errorf("db source is nil")
	}
	decrypt, err := util.Decrypt(dbSource.Password)
	if err != nil {
		log.ErrorContextf(ctx, "QuerySqlForDbSource|decrypt db source password failed: %v", err)
		return nil, nil, 0, "", err
	}
	connDbSource := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		Port:     dbSource.Port,
		Username: dbSource.Username,
		Password: decrypt,
		DbName:   dbSource.DBName,
	}

	if IsQueryOperation(sqlStr) {
		columns, data, errMessage, err = dao.GetDBSourceDao().QuerySqlForDbSource(ctx, connDbSource, runSql.SqlStr,
			runSql.SqlParams)
		if err != nil {
			log.ErrorContextf(ctx, "RunSql|query sql for db source failed: %v", err)
			return nil, nil, 0, errMessage, err
		}
	} else {
		effectCnt, errMessage, err = dao.GetDBSourceDao().RunSqlForDbSource(ctx, connDbSource, runSql.SqlStr,
			runSql.SqlParams)
		if err != nil {
			log.ErrorContextf(ctx, "RunSql|exec sql for db source failed: %v", err)
			return nil, nil, 0, errMessage, err
		}
	}
	return
}

// TextToSQLFromKnowledge TODO
func TextToSQLFromKnowledge(ctx context.Context, req *pb.TextToSQLFromKnowledgeReq, getDao dao.Dao) (
	*pb.TextToSQLFromKnowledgeRsp, error) {
	if req == nil || req.GetAppBizId() == 0 || req.GetDbSourceBizId() == 0 || len(req.GetDbSourceTableBizId()) == 0 ||
		utf8.RuneCountInString(req.GetQuery()) == 0 {
		return nil, errs.ErrParameterInvalid
	}
	if len(req.GetDbSourceTableBizId()) > config.App().DbSource.MaxText2SqlTableNum {
		return nil, errs.ErrText2sqlTableNums
	}
	log.DebugContextf(ctx, "TextToSQLFromKnowledge req:%v GenerateSqlTimeout:%v", req, config.App().
		DbSource.GenerateSqlTimeout)
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.GenerateSqlTimeout)
	defer cancel()

	appInfo, err := client.GetAppInfo(ctx, req.GetAppBizId(), model.AppTestScenes)
	if err != nil {
		log.ErrorContextf(ctx, "ListDbSource get app info err:%v,appBizId:%v", err, req.GetAppBizId())
		return nil, err
	}
	if appInfo.GetIsShareKnowledgeBase() {
		req.EnvType = pb.EnvType_Test
	}
	log.DebugContextf(ctx, "TextToSQLFromKnowledge req:%v, GetIsShareKnowledgeBase: %v", req,
		appInfo.GetIsShareKnowledgeBase())
	generateSql, err := textToSqlForDbSource(ctx, req, appInfo, getDao)
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
func textToSqlForDbSource(ctx context.Context, req *pb.TextToSQLFromKnowledgeReq, appInfo *admin.GetAppInfoRsp,
	getDao dao.Dao) (
	string, error) {
	ctx = pkg.WithSpaceID(ctx, appInfo.GetSpaceId())
	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, appInfo.CorpBizId, req.GetAppBizId(), req.GetDbSourceBizId())
	if err != nil || dbSource == nil {
		log.ErrorContextf(ctx, "TextToSqlForDbSource|get db source by biz id failed: %v, dbSourceBizID: %d", err,
			req.GetDbSourceBizId())
		return "", errs.ErrDataNotExistOrIsDeleted
	}

	var validTableIDs []uint64
	if req.GetEnvType() == pb.EnvType_Test {
		dbTables, err := dao.GetDBTableDao().BatchGetByBizIDs(ctx, dbSource.CorpBizID, dbSource.AppBizID,
			req.GetDbSourceTableBizId())
		if err != nil {
			log.ErrorContextf(ctx, "TextToSqlForDbSource|get db tables by biz ids failed: %v, dbTableBizIDs: %v", err,
				req.GetDbSourceTableBizId())
			return "", err
		}
		for _, dbTable := range dbTables {
			if dbTable.LearnStatus == model.LearnStatusLearning ||
				dbTable.LearnStatus == model.LearnStatusFailed {
				return "", errs.ErrWrapf(errs.ErrDbTableStatusForText2Sql, i18n.Translate(ctx,
					i18nkey.KeyDataTableLearningOrFailed), dbTable.Name)
			}
			validTableIDs = append(validTableIDs, dbTable.DBTableBizID)
		}
	} else {
		if dbSource.ReleaseStatus == model.ReleaseStatusReleasing {
			return "", errs.ErrWrapf(errs.ErrDbSourceStatusForText2Sql, i18n.Translate(ctx,
				i18nkey.KeyDatabasePublishingPleaseWait), dbSource.AliasName)
		}
		dbTableProd, err := dao.GetDBTableDao().BatchGetByBizIDsForProd(ctx, dbSource.CorpBizID, dbSource.AppBizID,
			req.GetDbSourceTableBizId())
		if err != nil {
			log.ErrorContextf(ctx, "TextToSqlForDbSource|get db tables by biz ids failed: %v, dbTableBizIDs: %v", err,
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
			log.WarnContextf(ctx, "TextToSqlForDbSource|db table biz ids not exist: %v", notExist)
			dbTablesNotExist, err := dao.GetDBTableDao().BatchGetByBizIDs(ctx, dbSource.CorpBizID, dbSource.AppBizID, notExist)
			if err != nil {
				log.ErrorContextf(ctx, "TextToSqlForDbSource|get db tables by biz ids failed: %v, dbTableBizIDs: %v", err, notExist)
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

	genSQLReq := &retrieve.GenerateSQLReq{
		RobotId:            appInfo.GetId(),
		AppBizId:           req.GetAppBizId(),
		DbSourceBizId:      req.GetDbSourceBizId(),
		DbSourceTableBizId: validTableIDs,
		Query:              req.GetQuery(),
		EnvType:            retrieve.EnvType(req.GetEnvType()),
		SqlTemplate:        req.GetSqlTemplate(),
		KnowledgeTemplate:  req.GetKnowledgeTemplate(),
	}
	rsp, err := getDao.GetDirectIndexCli().GenerateSQL(ctx, genSQLReq)
	if err != nil {
		log.ErrorContextf(ctx, "TextToSqlForDbSource|generate sql failed: %v, req: %+v", err, genSQLReq)
		if terrs.Code(err) == 102 {
			return "", errs.ErrText2sqlTimeOut
		}
		return "", err
	}
	return rsp.GenerateSql, nil
}
