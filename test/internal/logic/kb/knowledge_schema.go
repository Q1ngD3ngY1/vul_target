package kb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/distributedlockx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
)

func (l *Logic) GetKnowledgeSchemaTask(ctx context.Context, appBizId uint64) (*pb.GetKnowledgeSchemaTaskRsp, error) {
	rsp := new(pb.GetKnowledgeSchemaTaskRsp)
	isNotDeleted := kbEntity.IsNotDeleted
	knowledgeSchemaTask, err := l.kbDao.GetKnowledgeSchemaTask(ctx,
		[]string{kbEntity.KnowledgeSchemaTaskTblColStatus, kbEntity.KnowledgeSchemaTaskTblColStatusCode,
			kbEntity.KnowledgeSchemaTaskTblColCreateTime},
		&kbEntity.KnowledgeSchemaTaskFilter{
			AppBizId:       appBizId,
			Limit:          1,
			OrderColumn:    []string{kbEntity.KnowledgeSchemaTaskTblColCreateTime},
			OrderDirection: []string{util.SqlOrderByDesc},
			IsDeleted:      &isNotDeleted,
		})
	if err != nil {
		if errors.Is(err, errs.ErrKnowledgeSchemaTaskNotFound) {
			logx.D(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaTask not found")
			return rsp, nil
		}
		logx.E(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaTask err: %+v", err)
		return rsp, err
	}
	rsp.Status = entity.TaskStatusInt2Str(knowledgeSchemaTask.Status)
	rsp.StatusCode = knowledgeSchemaTask.StatusCode
	rsp.StatusMessage = knowledgeSchemaTask.Message

	if knowledgeSchemaTask.Status == kbEntity.TaskStatusSuccess {
		rsp.LatestSuccessTime = knowledgeSchemaTask.CreateTime.Unix()
	} else {
		knowledgeSchemaSuccessTask, err := l.kbDao.GetKnowledgeSchemaTask(ctx,
			[]string{kbEntity.KnowledgeSchemaTaskTblColCreateTime},
			&kbEntity.KnowledgeSchemaTaskFilter{
				AppBizId:       appBizId,
				Statuses:       []int32{int32(kbEntity.TaskStatusSuccess)},
				Limit:          1,
				OrderColumn:    []string{kbEntity.KnowledgeSchemaTaskTblColCreateTime},
				OrderDirection: []string{util.SqlOrderByDesc},
				IsDeleted:      &isNotDeleted,
			})
		if err != nil {
			if errors.Is(err, errs.ErrKnowledgeSchemaTaskNotFound) {
				logx.D(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaSuccessTask not found")
				return rsp, nil
			} else {
				logx.E(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaSuccessTask err: %+v", err)
				return rsp, err
			}
		}
		logx.I(ctx, "GetKnowledgeSchemaTask knowledgeSchemaSuccessTask: %+v", knowledgeSchemaSuccessTask)
		if !knowledgeSchemaSuccessTask.CreateTime.IsZero() {
			rsp.LatestSuccessTime = knowledgeSchemaSuccessTask.CreateTime.Unix()
		}
	}
	return rsp, nil
}

// GenerateKnowledgeSchemaTask 创建生成知识库schema任务
func (l *Logic) GenerateKnowledgeSchemaTask(ctx context.Context,
	corpID, corpBizID, appBizId, businessID, robotID uint64, isSharedApp bool) error {
	logx.D(ctx, "GenerateKnowledgeSchemaTask corpID=%v, corpBizID=%v, appBizId=%v, businessID=%v, robotID=%v",
		corpID, corpBizID, appBizId, businessID, robotID)
	summaryModelName, err := l.getKnowledgeSchemaModelName(ctx, corpBizID, appBizId, isSharedApp)
	if err != nil {
		logx.E(ctx, "GenerateKnowledgeSchema GetAppNormalModelName err: %+v", err)
		return err
	}

	if !l.financeLogic.CheckModelStatus(ctx, corpID, summaryModelName, rpc.KnowledgeSchemaFinanceBizType) {
		logx.E(ctx, "GenerateKnowledgeSchema CheckModelStatusBySubBizType false, appBizId=%+v", appBizId)
		return errs.ErrOverModelTokenLimit
	}

	err = l.kbDao.CreateKnowledgeSchemaTask(ctx, &kbEntity.KnowledgeSchemaTask{
		CorpBizId:  corpBizID,
		AppBizId:   appBizId,
		BusinessID: businessID,
		Status:     entity.TaskStatusInit,
	})
	if err != nil {
		logx.E(ctx, "GenerateKnowledgeSchema CreateKnowledgeSchemaTask err: %+v", err)
		return err
	}

	taskId, err := scheduler.NewKnowledgeGenerateSchemaTask(ctx, robotID, &kbEntity.KnowledgeGenerateSchemaParams{
		Name:             entity.TaskTypeNameMap[entity.KnowledgeGenerateSchemaTask],
		CorpID:           corpID,
		CorpBizID:        corpBizID,
		AppID:            robotID,
		AppBizID:         appBizId,
		TaskBizID:        businessID,
		SummaryModelName: summaryModelName,
		NeedCluster:      false, // 默认传false
	})
	logx.I(ctx, "GenerateKnowledgeSchema NewKnowledgeGenerateSchemaTask success, taskId=%+v", taskId)
	return nil
}

func (l *Logic) getKnowledgeSchemaModelName(ctx context.Context, corpBizId, appBizID uint64, isSharedApp bool) (string, error) {
	var (
		modelName  string
		err        error
		configType = uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL)
	)
	if isSharedApp {
		modelName, err = l.GetShareKnowledgeBaseConfig(ctx, corpBizId, appBizID, configType)
	} else {
		modelName, err = l.GetDefaultKnowledgeBaseConfig(ctx, corpBizId, appBizID, appBizID, configType, bot_common.AdpDomain_ADP_DOMAIN_DEV)
	}
	if err != nil {
		logx.E(ctx, "getKnowledgeSchemaModelName GetShareKnowledgeBaseConfig fail, err: %+v", err)
		return "", err
	}
	return modelName, nil
}

func (l *Logic) GetKnowledgeSchema(ctx context.Context, corpBizId, appBizId uint64,
	envType string) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	knowledgeSchemaPb, err := l.kbDao.GetKnowledgeSchema(ctx, appBizId, envType)
	if err != nil {
		if err == errx.ErrNotFound {
			knowledgeSchemaPb, err = l.loadKnowledgeSchema(ctx, appBizId, envType)
			if err != nil {
				logx.E(ctx, "GetKnowledgeSchema loadKnowledgeSchema err: %+v", err)
				return nil, err
			}
		} else {
			logx.E(ctx, "GetKnowledgeSchema redis.GetKnowledgeSchema err: %+v", err)
			return nil, err
		}
	}
	logx.D(ctx, "GetKnowledgeSchema knowledgeSchemaPb: %+v", knowledgeSchemaPb)
	dbTableSchema, err := l.GenerateDBTableKnowledgeSchema(ctx, corpBizId, appBizId, envType)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema GenerateDBTableKnowledgeSchema fail, err: %+v", err)
		return nil, err
	}
	shareKGList, err := l.kbDao.GetAppShareKGList(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "getAppShareKnowledgeBizIDList GetAppShareKGList fail, err: %+v", err)
		return nil, err
	}
	for _, shareKG := range shareKGList {
		// 共享知识库的schema
		shareDBTableSchema, err := l.GenerateDBTableKnowledgeSchema(ctx, corpBizId, shareKG.KnowledgeBizID, envType)
		if err != nil {
			logx.E(ctx, "GetKnowledgeSchema GenerateDBTableKnowledgeSchema fail, err: %+v", err)
			return nil, err
		}
		dbTableSchema = append(dbTableSchema, shareDBTableSchema...)
	}
	logx.D(ctx, "GetKnowledgeSchema dbTableSchema: %+v", dbTableSchema)
	knowledgeSchemaPb = append(knowledgeSchemaPb, TransformKnowledgeSchema2Pb(dbTableSchema)...)
	return knowledgeSchemaPb, nil
}

// loadKnowledgeSchema 尝试从数据库加载知识库schema，涉及到加锁、缓存设置、数据库查询等操作，已考虑并发问题
func (l *Logic) loadKnowledgeSchema(ctx context.Context, appBizId uint64,
	envType string) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	expireTTL := time.Duration(config.GetMainConfig().KnowledgeSchema.CacheLockExpireTTL) * time.Second
	lockKey := fmt.Sprintf(dao.LockKnowledgeSchemaCache, appBizId, envType)
	lock := distributedlockx.NewRedisLock(l.rdb, lockKey, distributedlockx.WithTTL(expireTTL))
	success, err := lock.Lock(ctx)
	if err != nil {
		logx.E(ctx, "Lock err:%v", err)
		return nil, errs.ErrCommonFail
	}
	if success { // 加锁成功
		defer func() {
			if err = lock.Unlock(ctx); err != nil {
				logx.E(ctx, "Unlock err:%v", err)
			}
		}()
	} else {
		// 之前的逻辑就是加锁失败 等5s 继续获取lock
		logx.W(ctx, "ModifyRole lock failed, appBizID:%d", appBizId)
		time.Sleep(expireTTL)
		knowledgeSchemaPb, err := l.kbDao.GetKnowledgeSchema(ctx, appBizId, envType)
		if err != nil {
			logx.E(ctx, "GetKnowledgeSchema redis.GetKnowledgeSchema fail when lock fail err: %+v", err)
			return nil, nil // 这里返回nil，不报错，下次再调用时会重新获取
		}
		return knowledgeSchemaPb, nil
	}
	// 加锁成功，先尝试从缓存获取
	if knowledgeSchemaPb, err := l.kbDao.GetKnowledgeSchema(ctx, appBizId, envType); err == nil {
		return knowledgeSchemaPb, nil
	}
	// 缓存没有，从数据库获取
	knowledgeSchema, err := l.GetKnowledgeSchemaFromDB(ctx, appBizId, envType)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema getKnowledgeSchemaFromDB fail, err: %+v", err)
		return nil, err
	}
	knowledgeSchemaPb := TransformKnowledgeSchema2Pb(knowledgeSchema)
	// 获取成功，设置缓存
	if err := l.kbDao.SetKnowledgeSchema(ctx, appBizId, envType, knowledgeSchemaPb); err != nil {
		logx.E(ctx, "GetKnowledgeSchema redis.SetKnowledgeSchema fail, err: %+v", err)
		return nil, err
	}
	return knowledgeSchemaPb, nil
}

// GetKnowledgeSchemaFromDB 从数据库获取知识库schema
func (l *Logic) GetKnowledgeSchemaFromDB(ctx context.Context, appBizId uint64,
	envType string) ([]*kbEntity.KnowledgeSchema, error) {
	isNotDeleted := kbEntity.IsNotDeleted
	knowledgeSchema, err := l.kbDao.FindKnowledgeSchema(ctx,
		[]string{kbEntity.KnowledgeSchemaTblColItemType, kbEntity.KnowledgeSchemaTblColItemBizType,
			kbEntity.KnowledgeSchemaTblColItemName, kbEntity.KnowledgeSchemaTblColItemSummary},
		&kbEntity.KnowledgeSchemaFilter{
			AppBizId:  appBizId,
			EnvType:   envType,
			IsDeleted: &isNotDeleted,
		})
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema dao.GetKnowledgeSchema err: %+v", err)
		return nil, err
	}
	return knowledgeSchema, nil
}

// TransformKnowledgeSchema2Pb 将知识库结构体转换为pb结构体
func TransformKnowledgeSchema2Pb(knowledgeSchema []*kbEntity.KnowledgeSchema) []*pb.GetKnowledgeSchemaRsp_SchemaItem {
	rsp := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0, len(knowledgeSchema))
	for _, item := range knowledgeSchema {
		schemaItem := &pb.GetKnowledgeSchemaRsp_SchemaItem{
			Name:    item.Name,
			Summary: item.Summary,
		}
		if item.ItemType == kbEntity.KnowledgeSchemaItemTypeDoc {
			schemaItem.BusinessId = fmt.Sprintf("doc_%d", item.ItemBizId)
		} else if item.ItemType == kbEntity.KnowledgeSchemaItemTypeDBTable {
			schemaItem.BusinessId = fmt.Sprintf("db_table_%d", item.ItemBizId)
		} else {
			schemaItem.BusinessId = fmt.Sprintf("doc_cluster_%d", item.ItemBizId)
		}
		rsp = append(rsp, schemaItem)
	}
	return rsp
}

// GenerateDBTableKnowledgeSchema 构建应用下所有数据库的所有表的建表SQL
func (l *Logic) GenerateDBTableKnowledgeSchema(ctx context.Context, corpBizId, appBizId uint64, envType string) ([]*kbEntity.KnowledgeSchema,
	error) {
	releaseStatus := []uint32{releaseEntity.ReleaseStatusSuccess}
	if envType == kbEntity.EnvTypeSandbox {
		releaseStatus = append(releaseStatus, releaseEntity.ReleaseStatusInit)
	}
	dbFilter := dbEntity.DatabaseFilter{
		CorpBizID:     corpBizId,
		AppBizID:      appBizId,
		ReleaseStatus: releaseStatus,
	}
	dbSources, _, err := l.dao.DescribeDatabaseList(ctx, &dbFilter)
	// dbSourceBizIds, err := dao.GetDBSourceDao().GetDbSourceBizIdByAppBizID(ctx, corpBizID, appBizID)
	if err != nil {
		logx.E(ctx, "BuildAppCreateTableSQL GetDbSourceBizIdByAppBizID fail, err:%v", err)
		return nil, err
	}
	var dbSourceBizIds []uint64
	for _, dbSource := range dbSources {
		dbSourceBizIds = append(dbSourceBizIds, dbSource.DBSourceBizID)
	}

	var result []*kbEntity.KnowledgeSchema
	for _, dbSourceBizId := range dbSourceBizIds {
		tableFilter := dbEntity.TableFilter{
			CorpBizID:     corpBizId,
			AppBizID:      appBizId,
			DBSourceBizID: dbSourceBizId,
		}
		dbTables, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
		// dbTables, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, corpBizID, appBizID, dbSourceBizId)
		if err != nil {
			logx.E(ctx, "BuildAppCreateTableSQL ListAllByDBSourceBizID fail, dbSourceBizId:%v, err:%v",
				dbSourceBizId, err)
			return nil, err
		}
		if len(dbTables) == 0 {
			logx.I(ctx, "BuildAppCreateTableSQL dbTables is nil, dbSourceBizId:%v", dbSourceBizId)
			continue
		}

		for _, dbTable := range dbTables {
			columnFilter := dbEntity.ColumnFilter{
				CorpBizID:    corpBizId,
				AppBizID:     appBizId,
				DBTableBizID: dbTable.DBTableBizID,
			}
			dbColumns, _, err := l.dao.DescribeColumnList(ctx, &columnFilter)
			// dbColumns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
			if err != nil {
				logx.E(ctx, "BuildAppCreateTableSQL GetByTableBizID fail, dbTableBizIDs:%v, err:%v",
					dbTable.DBTableBizID, err)
				return nil, err
			}
			if len(dbColumns) == 0 {
				logx.I(ctx, "BuildAppCreateTableSQL dbColumns is nil, dbTableBizID:%v", dbTable.DBTableBizID)
				continue
			}
			topValuesMap, err := l.getTopValuesMap(ctx, corpBizId, appBizId, dbTable.DBTableBizID)
			if err != nil {
				logx.E(ctx, "BuildAppCreateTableSQL getTopValueMap fail, err:%v", err)
				return nil, err
			}

			example := dbTable.Name + " (\n"
			for _, col := range dbColumns {
				dbColumnsTopValues := topValuesMap[col.DBTableColumnBizID]
				if len(dbColumnsTopValues) == 0 {
					example += fmt.Sprintf("  %s %s COMMENT '%s',\n", col.ColumnName, col.DataType, col.ColumnComment)
				} else {
					example += fmt.Sprintf("  %s %s COMMENT '%s', -- example: [%s]\n", col.ColumnName, col.DataType,
						col.ColumnComment, strings.Join(dbColumnsTopValues, ","))
				}

			}
			example += fmt.Sprintf(") COMMENT='%s", dbTable.TableComment)
			example += "'\n"

			result = append(result, &kbEntity.KnowledgeSchema{
				CorpBizId: corpBizId,
				AppBizId:  appBizId,
				ItemType:  kbEntity.KnowledgeSchemaItemTypeDBTable,
				ItemBizId: dbTable.DBTableBizID,
				Name:      dbTable.Name,
				Summary:   example,
			})
		}
	}

	return result, nil
}

func (l *Logic) getTopValuesMap(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) (map[uint64][]string,
	error) {
	dbTableTopValues, err := l.dao.GetTopValuesByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
	if err != nil {
		logx.E(ctx, "getTopValuesMap GetTopValuesByDbTableBizID fail, err:%v", err)
		return nil, err
	}

	resultMap := make(map[uint64]map[string]bool, len(dbTableTopValues))
	for _, dbTableTopValue := range dbTableTopValues {
		dbTableColumnBizID := dbTableTopValue.DbTableColumnBizID
		if resultMap[dbTableColumnBizID] == nil {
			resultMap[dbTableColumnBizID] = map[string]bool{}
		}
		resultMap[dbTableColumnBizID][dbTableTopValue.ColumnValue] = true
	}

	result := make(map[uint64][]string, len(resultMap))
	for columnBizID, columnValues := range resultMap {
		if result[columnBizID] == nil {
			result[columnBizID] = make([]string, 0, len(columnValues))
		}
		for value := range columnValues {
			result[columnBizID] = append(result[columnBizID], value)
		}
	}
	return result, nil
}

func (l *Logic) GetKnowledgeSchemaLogic(ctx context.Context, req *pb.GetKnowledgeSchemaReq) (*pb.GetKnowledgeSchemaRsp, error) {
	logx.I(ctx, "GetKnowledgeSchema Req:%+v", req)
	rsp := new(pb.GetKnowledgeSchemaRsp)
	if req.GetAppBizId() == 0 {
		return rsp, errs.ErrParams
	}
	if req.GetEnvType() == "" {
		return rsp, errs.ErrParams
	}
	if req.GetEnvType() != kbEntity.EnvTypeSandbox && req.GetEnvType() != kbEntity.EnvTypeProduct {
		return rsp, errs.ErrParams
	}
	app, err := l.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema logicApp.GetAppByAppBizID fail, err=%+v", err)
		return rsp, errs.ErrAppNotFound
	}
	corpBizId := contextx.Metadata(ctx).CorpBizID()
	if corpBizId == 0 {
		corpBizId = app.CorpBizId
	}
	knowledgeSchemaList, err := l.GetKnowledgeSchema(ctx, corpBizId, req.GetAppBizId(), req.GetEnvType())
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema knowledge_schema.GetKnowledgeSchema fail, err=%+v", err)
		return rsp, err
	}
	// 需要将结构化文件的schema信息展开
	commonFileSchemas := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0)
	structFileSchemas := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0)
	for _, schema := range knowledgeSchemaList {
		fileSuffix := path.Ext(schema.Name)
		if len(fileSuffix) < 2 {
			commonFileSchemas = append(commonFileSchemas, schema)
			continue
		}
		_, ok := docEntity.StructFileTypeMap[fileSuffix[1:]]
		if ok && strings.HasPrefix(schema.Summary, "[{") && strings.HasSuffix(schema.Summary, "}]") {
			// 结构化文件的schema信息展开
			oneStructFileSchemas, err := getStructFileSchema(ctx, schema)
			if err != nil {
				logx.E(ctx, "GetKnowledgeSchema getStructFileSchema fail, err=%+v", err)
				return rsp, err
			}
			if len(oneStructFileSchemas) != 0 {
				structFileSchemas = append(structFileSchemas, oneStructFileSchemas...)
			}
		} else {
			commonFileSchemas = append(commonFileSchemas, schema)
		}
	}

	rsp.Schemas = append(commonFileSchemas, structFileSchemas...)
	schemaNeedUpdate, err := l.GetKnowledgeSchemaNeedUpdate(ctx, corpBizId, app.PrimaryId, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema knowledge_schema.GetKnowledgeSchemaNeedUpdate fail, err=%+v", err)
		return rsp, err
	}
	rsp.SchemaNeedUpdate = schemaNeedUpdate
	return rsp, nil
}

func (l *Logic) GetKnowledgeSchemaNeedUpdate(ctx context.Context, corpBizId, appId, appBizId uint64) (bool, error) {
	schemaTask, err := l.GetKnowledgeSchemaTask(ctx, appBizId)
	if err != nil {
		return false, fmt.Errorf("GetKnowledgeSchemaTask err: %+v", err)
	}
	corpId := contextx.Metadata(ctx).CorpID()
	db, err := knowClient.GormClient(ctx, model.TableNameTDoc, appId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return false, err
	}
	count, err := l.docDao.CountDocWithTimeAndStatus(ctx, corpId, appId,
		[]uint32{docEntity.DocStatusWaitRelease, docEntity.DocStatusReleaseSuccess},
		time.Unix(schemaTask.GetLatestSuccessTime(), 0), db) // 即使 LatestSuccessTime 为 0，也要查是否有最新的文档

	if err != nil {
		return false, fmt.Errorf("CountDocWithTimeAndStatus err: %+v", err)
	}
	if count > 0 {
		return true, nil
	}

	// 判断应用引用的数据库
	count, err = l.dao.CountDBSourceWithTimeAndStatus(ctx, corpBizId, appBizId,
		time.Unix(schemaTask.GetLatestSuccessTime(), 0)) // 即使 LatestSuccessTime 为 0，也要查是否有最新的数据库
	if err != nil {
		return false, fmt.Errorf("GetKnowledgeSchemaNeedUpdate CountDBSourceWithTimeAndStatus fail, err: %+v", err)
	}
	if count > 0 {
		return true, nil
	}

	// 判断应用引用的共享知识库的文档
	shareKGList, err := l.kbDao.GetAppShareKGList(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchemaNeedUpdate GetAppShareKGList fail, err: %+v", err)
		return false, err
	}
	if len(shareKGList) == 0 {
		log.DebugContextf(ctx, "GetKnowledgeSchemaNeedUpdate AppShareKG is nil")
		return false, nil
	}
	for _, shareKGInfo := range shareKGList {
		if shareKGInfo.UpdateTime.Unix() >= schemaTask.GetLatestSuccessTime() {
			return true, nil
		}
		shareKG, err := l.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, shareKGInfo.KnowledgeBizID, entity.AppTestScenes)
		if err != nil {
			logx.E(ctx, "GetKnowledgeSchemaNeedUpdate DescribeAppInfoUsingScenesById fail, err=%+v", err)
			return false, errs.ErrAppNotFound
		}
		db, err := knowClient.GormClient(ctx, model.TableNameTDoc, shareKG.PrimaryId, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "get GormClient failed, err: %+v", err)
			return false, err
		}
		count, err := l.docDao.CountDocWithTimeAndStatus(ctx, corpId, shareKG.PrimaryId,
			[]uint32{docEntity.DocStatusWaitRelease, docEntity.DocStatusReleaseSuccess},
			time.Unix(schemaTask.GetLatestSuccessTime(), 0), db) // 即使 LatestSuccessTime 为 0，也要查是否有最新的文档

		if err != nil {
			return false, fmt.Errorf("GetKnowledgeSchemaNeedUpdate shareKG CountDocWithTimeAndStatus fail, err: %+v", err)
		}
		if count > 0 {
			return true, nil
		}
	}

	return false, nil
}

// getStructFileSchema 将结构化文件的schema信息展开
func getStructFileSchema(ctx context.Context,
	schema *pb.GetKnowledgeSchemaRsp_SchemaItem) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	structFileSchemas := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0)
	text2sqlMeta := make([]docEntity.Text2sqlMetaMappingPreview, 0)
	err := jsonx.Unmarshal([]byte(schema.Summary), &text2sqlMeta)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema json.Unmarshal err: %v", err)
		return nil, err
	}
	for _, text2sqlMetaItem := range text2sqlMeta {
		summary := ""
		text2sqlSummary := "{"
		tableInfo := &docEntity.MappingData{}
		err = jsonx.Unmarshal([]byte(text2sqlMetaItem.Mapping), &tableInfo)
		if err != nil {
			logx.W(ctx, "task(KnowledgeGenerateSchema) Process json.Unmarshal err: %v", err)
			continue
		}
		for _, filed := range tableInfo.Fields {
			text2sqlSummary += fmt.Sprintf("'%s': '%s, %s', ", filed.FormattedText, filed.RawText,
				docEntity.TableDataCellDataType2String[filed.DataType])
		}
		if text2sqlSummary != "{" {
			text2sqlSummary = text2sqlSummary[:len(text2sqlSummary)-2] + "}"
			summary += text2sqlSummary + ";"
		}
		schemaItem := &pb.GetKnowledgeSchemaRsp_SchemaItem{
			BusinessId: schema.BusinessId,
			Name:       schema.Name + "." + tableInfo.TableName.RawText,
			Summary:    summary,
		}
		structFileSchemas = append(structFileSchemas, schemaItem)
	}
	return structFileSchemas, nil
}
