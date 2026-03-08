package release

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_schema"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// KnowledgeRelease 知识库发布时的回调处理
func KnowledgeRelease(ctx context.Context, corpBizId, appBizId uint64,
	releaseItems []*pb.KnowledgeReleaseCallbackReq_ReleaseItem) error {
	var err error
	if len(releaseItems) == 0 {
		return nil
	}
	for _, releaseItem := range releaseItems {
		switch releaseItem.GetReleaseItemType() {
		case pb.ReleaseItemType_ReleaseItemTypeKnowledgeSchema:
			err = releaseKnowledgeSchema(ctx, corpBizId, appBizId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 知识库schema发布时的回调
func releaseKnowledgeSchema(ctx context.Context, corpBizId, appBizId uint64) error {
	// 从数据库获取
	knowledgeSchema, err := knowledge_schema.GetKnowledgeSchemaFromDB(ctx, appBizId, model.EnvTypeProduct)
	if err != nil {
		log.ErrorContextf(ctx, "releaseKnowledgeSchema getKnowledgeSchemaFromDB fail, err: %+v", err)
		return err
	}
	if len(knowledgeSchema) == 0 {
		return nil
	}
	// 获取成功，设置缓存
	knowledgeSchemaPb := knowledge_schema.TransformKnowledgeSchema2Pb(knowledgeSchema)
	if err = redis.SetKnowledgeSchema(ctx, appBizId, model.EnvTypeProduct, knowledgeSchemaPb); err != nil {
		log.ErrorContextf(ctx, "releaseKnowledgeSchema redis.SetKnowledgeSchema fail, err: %+v", err)
		return err
	}
	// 生成成功，如果是聚类后的schema，需要设置文件聚类目录缓存
	docClusterSchemaBizIds := make([]uint64, 0, len(knowledgeSchema))
	for _, schemaItem := range knowledgeSchema {
		if schemaItem.ItemType != model.KnowledgeSchemaItemTypeDocCluster || schemaItem.ItemBizId == 0 {
			continue
		}
		// 查询该聚类下的文档id
		docClusterSchemaBizIds = append(docClusterSchemaBizIds, schemaItem.ItemBizId)
	}
	if len(docClusterSchemaBizIds) == 0 {
		return nil
	}
	filter := &dao.DocClusterSchemaFilter{
		CorpBizId:   corpBizId,
		AppBizId:    appBizId,
		BusinessIds: docClusterSchemaBizIds,
	}
	selectColumns := []string{dao.DocClusterSchemaTblColBusinessId, dao.DocClusterSchemaTblColDocIds,
		dao.DocClusterSchemaTblColVersion}
	docClusterSchemaList, err := dao.GetDocClusterSchemaDao().GetDocClusterSchemaList(ctx, selectColumns, filter)
	if err != nil {
		log.ErrorContextf(ctx, "releaseKnowledgeSchema GetDocClusterSchemaList fail, err: %+v", err)
		return err
	}
	if len(docClusterSchemaList) == 0 {
		err = fmt.Errorf("releaseKnowledgeSchema GetDocClusterSchemaList docClusterSchemaList is empty")
		log.ErrorContextf(ctx, "%+v", err)
		return err
	}

	// 需要先删除所有旧聚类的缓存，再写入新聚类的缓存
	err = redis.DeleteKnowledgeSchemaOldVersion(ctx, appBizId, model.EnvTypeProduct)
	if err != nil {
		log.ErrorContextf(ctx, "releaseKnowledgeSchema redis.DeleteKnowledgeSchemaOldVersion fail, err: %+v", err)
		return err
	}

	for _, docClusterSchema := range docClusterSchemaList {
		if err := redis.SetKnowledgeSchemaDocIdByDocClusterId(ctx, appBizId, model.EnvTypeProduct, docClusterSchema); err != nil {
			log.ErrorContextf(ctx, "releaseKnowledgeSchema redis.SetKnowledgeSchemaDocIdByDocClusterId fail, err: %+v", err)
			return err
		}
		if err := redis.SetKnowledgeSchemaAppBizIdByDocClusterId(ctx, docClusterSchema.BusinessID, appBizId, model.EnvTypeProduct); err != nil {
			log.ErrorContextf(ctx, "releaseKnowledgeSchema redis.SetKnowledgeSchemaAppBizIdByDocClusterId fail, err: %+v", err)
			return err
		}
	}

	// 【硬性】删除所有旧版本
	productVersion := docClusterSchemaList[0].Version
	err = dao.GetDocClusterSchemaDao().DeleteDocClusterSchemaAllOldVersion(ctx, nil, corpBizId, appBizId, productVersion)
	if err != nil {
		log.ErrorContextf(ctx, "releaseKnowledgeSchema DeleteDocClusterSchemaAllOldVersion err:%+v", err)
		return err
	}

	return nil
}
