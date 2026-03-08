package knowledge_schema

import (
	"context"
	"errors"
	"fmt"
	logicCorp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/corp"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	redis_v8 "github.com/go-redis/redis/v8"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GenerateKnowledgeSchema 创建生成知识库schema任务
func GenerateKnowledgeSchema(ctx context.Context, db dao.Dao, corpID, corpBizID, appBizId, businessID, robotID uint64) error {
	summaryModelName, err := getKnowledgeSchemaModelName(ctx, corpBizID, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GenerateKnowledgeSchema GetAppNormalModelName err: %+v", err)
		return err
	}

	if !logicCorp.CheckModelStatus(ctx, db, corpID, summaryModelName, client.KnowledgeSchemaFinanceBizType) {
		log.ErrorContextf(ctx, "GenerateKnowledgeSchema CheckModelStatusBySubBizType false, appBizId=%+v", appBizId)
		return errs.ErrOverModelTokenLimit
	}

	err = dao.GetKnowledgeSchemaTaskDao().CreateKnowledgeSchemaTask(ctx, &model.KnowledgeSchemaTask{
		CorpBizId:  corpBizID,
		AppBizId:   appBizId,
		BusinessID: businessID,
		Status:     model.TaskStatusInit,
	})
	if err != nil {
		log.ErrorContextf(ctx, "GenerateKnowledgeSchema CreateKnowledgeSchemaTask err: %+v", err)
		return err
	}

	taskId, err := dao.NewKnowledgeGenerateSchemaTask(ctx, robotID, &model.KnowledgeGenerateSchemaParams{
		Name:             model.TaskTypeNameMap[model.KnowledgeGenerateSchemaTask],
		CorpID:           corpID,
		CorpBizID:        corpBizID,
		AppID:            robotID,
		AppBizID:         appBizId,
		TaskBizID:        businessID,
		SummaryModelName: summaryModelName,
		NeedCluster:      false, // 默认传false
	})
	log.InfoContextf(ctx, "GenerateKnowledgeSchema NewKnowledgeGenerateSchemaTask success, taskId=%+v", taskId)
	return nil
}

func getKnowledgeSchemaModelName(ctx context.Context, corpBizId, appBizID uint64) (string, error) {
	knowledgeBaseConfig, err := knowledge_config.GetKnowledgeBaseConfig(ctx,
		corpBizId,
		appBizID,
		uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
	)
	if err != nil {
		log.ErrorContextf(ctx, "getKnowledgeSchemaModelName GetKnowledgeBaseConfig fail, err: %+v", err)
		return "", err
	}
	return knowledgeBaseConfig, nil
}

func GetKnowledgeSchemaTask(ctx context.Context, appBizId uint64) (*pb.GetKnowledgeSchemaTaskRsp, error) {
	rsp := new(pb.GetKnowledgeSchemaTaskRsp)
	isNotDeleted := dao.IsNotDeleted
	knowledgeSchemaTask, err := dao.GetKnowledgeSchemaTaskDao().GetKnowledgeSchemaTask(ctx,
		[]string{dao.KnowledgeSchemaTaskTblColStatus, dao.KnowledgeSchemaTaskTblColStatusCode, dao.KnowledgeSchemaTaskTblColCreateTime},
		&dao.KnowledgeSchemaTaskFilter{
			AppBizId:       appBizId,
			Limit:          1,
			OrderColumn:    []string{dao.KnowledgeSchemaTaskTblColCreateTime},
			OrderDirection: []string{dao.SqlOrderByDesc},
			IsDeleted:      &isNotDeleted,
		})
	if err != nil {
		if errors.Is(err, errs.ErrKnowledgeSchemaTaskNotFound) {
			log.DebugContextf(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaTask not found")
			return rsp, nil
		}
		log.ErrorContextf(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaTask err: %+v", err)
		return rsp, err
	}
	rsp.Status = model.TaskStatusInt2Str(knowledgeSchemaTask.Status)
	rsp.StatusCode = knowledgeSchemaTask.StatusCode
	rsp.StatusMessage = knowledgeSchemaTask.Message

	if knowledgeSchemaTask.Status == model.TaskStatusSuccess {
		rsp.LatestSuccessTime = knowledgeSchemaTask.CreateTime.Unix()
	} else {
		knowledgeSchemaSuccessTask, err := dao.GetKnowledgeSchemaTaskDao().GetKnowledgeSchemaTask(ctx,
			[]string{dao.KnowledgeSchemaTaskTblColCreateTime},
			&dao.KnowledgeSchemaTaskFilter{
				AppBizId:       appBizId,
				Statuses:       []int32{int32(model.TaskStatusSuccess)},
				Limit:          1,
				OrderColumn:    []string{dao.KnowledgeSchemaTaskTblColCreateTime},
				OrderDirection: []string{dao.SqlOrderByDesc},
				IsDeleted:      &isNotDeleted,
			})
		if err != nil {
			if errors.Is(err, errs.ErrKnowledgeSchemaTaskNotFound) {
				log.DebugContextf(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaSuccessTask not found")
			} else {
				log.ErrorContextf(ctx, "GetKnowledgeSchemaTask dao.GetKnowledgeSchemaSuccessTask err: %+v", err)
				return rsp, err
			}
		}
		if !knowledgeSchemaSuccessTask.CreateTime.IsZero() {
			rsp.LatestSuccessTime = knowledgeSchemaSuccessTask.CreateTime.Unix()
		}
	}

	return rsp, nil
}

func GetKnowledgeSchema(ctx context.Context, d dao.Dao, corpBizID, appBizId uint64, envType string) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	knowledgeSchemaPb, err := redis.GetKnowledgeSchema(ctx, appBizId, envType)
	if err != nil {
		if errors.Is(err, redis_v8.Nil) {
			knowledgeSchemaPb, err = loadKnowledgeSchema(ctx, d, appBizId, envType)
			if err != nil {
				log.ErrorContextf(ctx, "GetKnowledgeSchema loadKnowledgeSchema err: %+v", err)
				return nil, err
			}
		} else {
			log.ErrorContextf(ctx, "GetKnowledgeSchema redis.GetKnowledgeSchema err: %+v", err)
			return nil, err
		}
	}
	dbTableSchema, err := GenerateDBTableKnowledgeSchema(ctx, corpBizID, appBizId, envType)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema GenerateDBTableKnowledgeSchema fail, err: %+v", err)
		return nil, err
	}
	knowledgeSchemaPb = append(knowledgeSchemaPb, TransformKnowledgeSchema2Pb(dbTableSchema)...)
	return knowledgeSchemaPb, nil
}

// loadKnowledgeSchema 尝试从数据库加载知识库schema，涉及到加锁、缓存设置、数据库查询等操作，已考虑并发问题
func loadKnowledgeSchema(ctx context.Context, d dao.Dao, appBizId uint64, envType string) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	expireTTL := time.Duration(utilConfig.GetMainConfig().KnowledgeSchema.CacheLockExpireTTL) * time.Second
	lockKey := fmt.Sprintf(dao.LockKnowledgeSchemaCache, appBizId, envType)
	err := d.Lock(ctx, lockKey, expireTTL)
	// 加锁失败，等待5秒后尝试从缓存获取
	if err != nil {
		time.Sleep(expireTTL)
		knowledgeSchemaPb, err := redis.GetKnowledgeSchema(ctx, appBizId, envType)
		if err != nil {
			log.ErrorContextf(ctx, "GetKnowledgeSchema redis.GetKnowledgeSchema fail when lock fail err: %+v", err)
			return nil, nil // 这里返回nil，不报错，下次再调用时会重新获取
		}
		return knowledgeSchemaPb, nil
	}
	defer d.UnLock(ctx, lockKey)
	// 加锁成功，先尝试从缓存获取
	if knowledgeSchemaPb, err := redis.GetKnowledgeSchema(ctx, appBizId, envType); err == nil {
		return knowledgeSchemaPb, nil
	}
	// 缓存没有，从数据库获取
	knowledgeSchema, err := GetKnowledgeSchemaFromDB(ctx, appBizId, envType)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema getKnowledgeSchemaFromDB fail, err: %+v", err)
		return nil, err
	}
	knowledgeSchemaPb := TransformKnowledgeSchema2Pb(knowledgeSchema)
	// 获取成功，设置缓存
	if err := redis.SetKnowledgeSchema(ctx, appBizId, envType, knowledgeSchemaPb); err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema redis.SetKnowledgeSchema fail, err: %+v", err)
		return nil, err
	}
	return knowledgeSchemaPb, nil
}

// GetKnowledgeSchemaFromDB 从数据库获取知识库schema
func GetKnowledgeSchemaFromDB(ctx context.Context, appBizId uint64, envType string) ([]*model.KnowledgeSchema, error) {
	isNotDeleted := dao.IsNotDeleted
	knowledgeSchema, err := dao.GetKnowledgeSchemaDao().FindKnowledgeSchema(ctx,
		[]string{dao.KnowledgeSchemaTblColItemType, dao.KnowledgeSchemaTblColItemBizType, dao.KnowledgeSchemaTblColItemName, dao.KnowledgeSchemaTblColItemSummary},
		&dao.KnowledgeSchemaFilter{
			AppBizId:  appBizId,
			EnvType:   envType,
			IsDeleted: &isNotDeleted,
		})
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema dao.GetKnowledgeSchema err: %+v", err)
		return nil, err
	}
	return knowledgeSchema, nil
}

func GetKnowledgeSchemaNeedUpdate(ctx context.Context, d dao.Dao, corpBizID, appId, appBizId uint64) (bool, error) {
	// 判断应用知识库本身的文档
	schemaTask, err := GetKnowledgeSchemaTask(ctx, appBizId)
	if err != nil {
		return false, fmt.Errorf("GetKnowledgeSchemaTask err: %+v", err)
	}
	corpID := pkg.CorpID(ctx)
	count, err := d.CountDocWithTimeAndStatus(ctx, corpID, appId,
		[]uint32{model.DocStatusWaitRelease, model.DocStatusReleaseSuccess},
		time.Unix(schemaTask.GetLatestSuccessTime(), 0)) // 即使 LatestSuccessTime 为 0，也要查是否有最新的文档
	if err != nil {
		return false, fmt.Errorf("GetKnowledgeSchemaNeedUpdate CountDocWithTimeAndStatus fail, err: %+v", err)
	}
	if count > 0 {
		return true, nil
	}

	// 判断应用引用的数据库
	count, err = dao.GetDBSourceDao().CountDBSourceWithTimeAndStatus(ctx, corpBizID, appBizId,
		time.Unix(schemaTask.GetLatestSuccessTime(), 0)) // 即使 LatestSuccessTime 为 0，也要查是否有最新的数据库
	if err != nil {
		return false, fmt.Errorf("GetKnowledgeSchemaNeedUpdate CountDBSourceWithTimeAndStatus fail, err: %+v", err)
	}
	if count > 0 {
		return true, nil
	}

	// 判断应用引用的共享知识库的文档
	shareKGList, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaNeedUpdate GetAppShareKGList fail, err: %+v", err)
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
		shareKG, err := client.GetAppInfo(ctx, shareKGInfo.KnowledgeBizID, model.AppTestScenes)
		if err != nil {
			log.ErrorContextf(ctx, "GetKnowledgeSchemaNeedUpdate GetAppInfo fail, err=%+v", err)
			return false, errs.ErrAppNotFound
		}
		count, err := d.CountDocWithTimeAndStatus(ctx, corpID, shareKG.Id,
			[]uint32{model.DocStatusWaitRelease, model.DocStatusReleaseSuccess},
			time.Unix(schemaTask.GetLatestSuccessTime(), 0)) // 即使 LatestSuccessTime 为 0，也要查是否有最新的文档
		if err != nil {
			return false, fmt.Errorf("GetKnowledgeSchemaNeedUpdate shareKG CountDocWithTimeAndStatus fail, err: %+v", err)
		}
		if count > 0 {
			return true, nil
		}
	}

	return false, nil
}

// TransformKnowledgeSchema2Pb 将知识库结构体转换为pb结构体
func TransformKnowledgeSchema2Pb(knowledgeSchema []*model.KnowledgeSchema) []*pb.GetKnowledgeSchemaRsp_SchemaItem {
	rsp := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0, len(knowledgeSchema))
	for _, item := range knowledgeSchema {
		schemaItem := &pb.GetKnowledgeSchemaRsp_SchemaItem{
			Name:    item.Name,
			Summary: item.Summary,
		}
		if item.ItemType == model.KnowledgeSchemaItemTypeDoc {
			schemaItem.BusinessId = fmt.Sprintf("doc_%d", item.ItemBizId)
		} else if item.ItemType == model.KnowledgeSchemaItemTypeDBTable {
			schemaItem.BusinessId = fmt.Sprintf("db_table_%d", item.ItemBizId)
		} else {
			schemaItem.BusinessId = fmt.Sprintf("doc_cluster_%d", item.ItemBizId)
		}
		rsp = append(rsp, schemaItem)
	}
	return rsp
}
