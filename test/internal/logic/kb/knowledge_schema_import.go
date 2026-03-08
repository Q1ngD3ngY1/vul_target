package kb

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"github.com/spf13/cast"
)

const (
	schemaImportBatchSize    = 200
	docSchemaFileName        = "doc_schema.jsonl"
	docClusterSchemaFileName = "doc_cluster_schema.jsonl"
)

func (l *Logic) ImportKnowledgeSchemas(ctx context.Context, config *kb_package.ImportConfig) error {
	defer gox.Recover()
	// 从config.LocalPath指定的本地路径读取分类数据
	logx.I(ctx, "ImportKnowledgeSchemas start, kbPrimaryID: %d, kbID: %d, localPath: %s",
		config.KbPrimaryID, config.KbID, config.LocalPath)

	// 打印 ModuleKbDoc 的 ID 映射信息
	if config.IDMappingConfig != nil && config.IDMappingConfig.Modules != nil {
		if kbDocMapping, exists := config.IDMappingConfig.Modules[kb_package.ModuleKbDoc]; exists {
			logx.InfoContextf(ctx, "ImportKnowledgeSchemas IDMappingConfig ModuleKbDoc mapping: %+v", kbDocMapping)
		} else {
			logx.InfoContextf(ctx, "ImportKnowledgeSchemas IDMappingConfig ModuleKbDoc mapping not found")
		}
	} else {
		logx.InfoContextf(ctx, "ImportKnowledgeSchemas IDMappingConfig is nil")
	}
	// 确保导入目录存在
	if _, err := os.Stat(config.LocalPath); os.IsNotExist(err) {
		logx.I(ctx, "ImportKnowledgeSchemas  schema  directory not exist, skip import, path: %s", config.LocalPath)
		return nil
	}
	// 有依赖关系 得先解析doc_cluster_schema.jsonl 生成cluster_id  后续才能用
	docClusterSchemaFilePath := filepath.Join(config.LocalPath, docClusterSchemaFileName)
	err := l.importDocClusterSchema(ctx, config, docClusterSchemaFilePath)
	if err != nil {
		logx.E(ctx, "ImportKnowledgeSchemas importDocClusterSchema failed, err: %v", err)
		return err
	}

	//再导入t_doc_schema 表
	docSchemaFilePath := filepath.Join(config.LocalPath, docSchemaFileName)
	err = l.importDocSchema(ctx, config, docSchemaFilePath)
	if err != nil {
		logx.E(ctx, "ImportKnowledgeSchemas importDocSchema failed, err: %v", err)
		return err
	}
	//最后导入t_knowledge_schema
	knowledgeSchemaFilePath := filepath.Join(config.LocalPath, knowledgeSchemaFileName)
	err = l.importKnowledgeSchema(ctx, config, knowledgeSchemaFilePath)
	if err != nil {
		logx.E(ctx, "ImportKnowledgeSchemas importKnowledgeSchema failed, err: %v", err)
		return err
	}
	// 写任务表
	err = l.writeSchemaTask(ctx, config.CorpBizID, config.KbID)
	if err != nil {
		logx.E(ctx, "ImportKnowledgeSchemas writeSchemaTask failed, err: %v", err)
		return err
	}
	return nil
}

func (l *Logic) writeSchemaTask(ctx context.Context, corpBizId, appBizId uint64) error {
	err := l.kbDao.CreateKnowledgeSchemaTask(ctx, &kbEntity.KnowledgeSchemaTask{
		CorpBizId:  corpBizId,
		AppBizId:   appBizId,
		BusinessID: idgen.GetId(),
		Status:     entity.TaskStatusSuccess,
	})
	if err != nil {
		logx.E(ctx, "GenerateKnowledgeSchema CreateKnowledgeSchemaTask err: %+v", err)
		return err
	}
	return nil
}

func (l *Logic) importDocSchema(ctx context.Context, config *kb_package.ImportConfig, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importDocSchema file not exist, filePath: %s", filePath)
			return nil
		}
		logx.E(ctx, "importDocSchema open file failed, err: %v", err)
		return err
	}
	defer file.Close()
	reader := jsonlx.NewReader[kbEntity.DocSchemaExport](file)
	// 批量插入，避免一次打挂数据库
	var schemaBatch []*docEntity.DocSchema
	err = reader.Each(func(schema kbEntity.DocSchemaExport) error {
		itemBizId, convertErr := config.IDMappingConfig.ConvertToBizID(ctx, kb_package.ModuleKbDoc, schema.DocId)
		if convertErr != nil {
			logx.WarnContextf(ctx, "ConvertToBizID failed for DocId=%s, error=%v, using original ID", schema.DocId, convertErr)
			itemBizId = schema.DocId
		}
		docBizID, err := strconv.ParseUint(itemBizId, 10, 64)
		if err != nil {
			logx.ErrorContextf(ctx, "Failed to parse DocBizID: %s, error=%v", itemBizId, err)
			return fmt.Errorf("invalid DocId: %s", itemBizId)
		}
		// 处理Vector字段，确保不为nil
		var vectorBytes []byte
		if schema.Vector != "" {
			// 如果Vector不为空，进行base64编码
			vectorBytes = []byte(base64.StdEncoding.EncodeToString([]byte(schema.Vector)))
		} else {
			// 如果Vector为空，使用空字符串的base64编码（空字符串的base64编码还是空字符串）
			vectorBytes = []byte(base64.StdEncoding.EncodeToString([]byte("")))
		}
		docSchema := &docEntity.DocSchema{
			CorpBizID:  config.CorpBizID,
			AppBizID:   config.KbID,
			DocBizID:   docBizID,
			FileName:   schema.FileName,
			Summary:    schema.Summary,
			Vector:     vectorBytes,
			IsDeleted:  false,
			CreateTime: time.Time{},
			UpdateTime: time.Time{},
		}
		schemaBatch = append(schemaBatch, docSchema)
		// 达到批量大小时执行批量插入
		if len(schemaBatch) >= schemaImportBatchSize {
			if batchErr := l.batchCreateDocSchemas(ctx, schemaBatch); batchErr != nil {
				logx.ErrorContextf(ctx, "batchCreateDocSchemas failed: %v, batch size: %d", batchErr, len(schemaBatch))
				return batchErr
			}
			// 清空批次
			schemaBatch = nil
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "importDocSchema read failed, err: %v", err)
		return err
	}
	// 处理剩余的批次
	if len(schemaBatch) > 0 {
		if err := l.batchCreateDocSchemas(ctx, schemaBatch); err != nil {
			logx.E(ctx, "batchCreateDocSchemas failed for final batch: %v", err)
			return err
		}
	}
	logx.I(ctx, "importDocSchema completed successfully")
	return nil
}

func (l *Logic) importKnowledgeSchema(ctx context.Context, config *kb_package.ImportConfig, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importKnowledgeSchema file not exist, filePath: %s", filePath)
			return nil
		}
		logx.E(ctx, "importKnowledgeSchema open file failed, err: %v", err)
		return err
	}
	defer file.Close()
	reader := jsonlx.NewReader[kbEntity.KnowledgeSchemaProdExport](file)
	// 批量插入，避免一次打挂数据库
	var schemaBatch []*kbEntity.KnowledgeSchema
	err = reader.Each(func(schema kbEntity.KnowledgeSchemaProdExport) error {
		// 根据ItemType决定使用哪个模块类型进行ID转换
		var itemBizId string
		var err error
		if schema.ItemType == 1 {
			// ItemType=1: 文档类型，使用ModuleKbDoc
			var primaryId uint64
			primaryId, err = config.IDMappingConfig.ConvertToPrimaryID(ctx, kb_package.ModuleKbDoc, schema.ItemBizId)
			itemBizId = fmt.Sprintf("%d", primaryId)
			logx.I(ctx, "importKnowledgeSchema ItemType=1, ItemBizId=%s, primaryId=%d", schema.ItemBizId, primaryId)
		} else if schema.ItemType == 2 {
			// ItemType=2: 文档聚类类型，使用ModuleKbDocClusterSchema
			itemBizId, err = config.IDMappingConfig.ConvertToBizID(ctx, kb_package.ModuleKbDocClusterSchema, schema.ItemBizId)
		} else {
			logx.E(ctx, "importKnowledgeSchema unknown ItemType: %d, ItemBizId: %s", schema.ItemType, schema.ItemBizId)
			return fmt.Errorf("unknown ItemType: %d", schema.ItemType)
		}
		if err != nil {
			logx.E(ctx, "importKnowledgeSchema ConvertToBizID failed for ItemType=%d, ItemBizId=%s, err: %v", schema.ItemType, schema.ItemBizId, err)
			return err
		}
		knowledgeSchema := &kbEntity.KnowledgeSchema{
			CorpBizId:  config.CorpBizID,
			AppBizId:   config.KbID,
			Version:    1, // 默认版本
			ItemType:   schema.ItemType,
			ItemBizId:  cast.ToUint64(itemBizId),
			Name:       schema.Name,
			Summary:    schema.Summary,
			IsDeleted:  false,
			CreateTime: time.Time{},
			UpdateTime: time.Time{},
		}
		schemaBatch = append(schemaBatch, knowledgeSchema)
		// 达到批量大小时执行批量插入
		if len(schemaBatch) >= schemaImportBatchSize {
			if err := l.batchCreateKnowledgeSchemas(ctx, schemaBatch); err != nil {
				return err
			}
			// 清空批次
			schemaBatch = nil
		}
		return nil
	})
	// 处理剩余的批次
	if err == nil && len(schemaBatch) > 0 {
		err = l.batchCreateKnowledgeSchemas(ctx, schemaBatch)
	}
	if err != nil {
		logx.E(ctx, "importKnowledgeSchema read failed, err: %v", err)
		return err
	}
	return nil
}

// importDocClusterSchema 解析doc_cluster_schema.jsonl
func (l *Logic) importDocClusterSchema(ctx context.Context, config *kb_package.ImportConfig, filePath string) error {
	DocClusterMapping := make(map[string]*kb_package.MappedID) // oldBizID -> MappedID
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importDocClusterSchema file not exist, filePath: %s", filePath)
			return nil
		}
		logx.E(ctx, "importDocClusterSchema open file failed, err: %v", err)
		return err
	}
	defer file.Close()

	reader := jsonlx.NewReader[kbEntity.DocClusterSchemaExport](file)
	// 批量插入，避免一次打挂数据库
	var schemaBatch []*docEntity.DocClusterSchema
	var clusterIDs []string // 保存每个schema的原始ClusterId
	err = reader.Each(func(schema kbEntity.DocClusterSchemaExport) error {
		//newBizIdStr, _ := config.IDMappingConfig.ConvertToBizID(ctx, entity.ModuleKbDocClusterSchema, schema.ClusterId)
		businessID := idgen.GetId()
		docClusterSchema := &docEntity.DocClusterSchema{
			CorpBizID:   config.CorpBizID,
			AppBizID:    config.KbID,
			BusinessID:  businessID,
			Version:     1,
			ClusterName: schema.ClusterName,
			Summary:     schema.Summary,
			DocIDs:      schema.DocIDs,
			IsDeleted:   false,
			CreateTime:  time.Time{},
			UpdateTime:  time.Time{},
		}
		schemaBatch = append(schemaBatch, docClusterSchema)
		clusterIDs = append(clusterIDs, schema.ClusterId)
		// 达到批量大小时执行批量插入
		if len(schemaBatch) >= schemaImportBatchSize {
			//得把老的biz id传进去，为了更新id mapping用
			if err := l.batchCreateDocClusterSchemas(ctx, schemaBatch, clusterIDs, DocClusterMapping); err != nil {
				return err
			}
			// 清空批次
			schemaBatch = nil
			clusterIDs = nil
		}
		return nil
	})
	// 处理剩余的批次
	if err == nil && len(schemaBatch) > 0 {
		err = l.batchCreateDocClusterSchemas(ctx, schemaBatch, clusterIDs, DocClusterMapping)
	}
	if err != nil {
		logx.E(ctx, "importDocClusterSchema read failed, err: %v", err)
		return err
	}
	// 更新IDMappingConfig中的DocCluster ID映射
	for oldBizID, mapping := range DocClusterMapping {
		// 只有在 IDMappingConfig 中已存在的才更新
		if config.IDMappingConfig.IsMappedIDExist(kb_package.ModuleKbDocClusterSchema, oldBizID) {
			logx.I(ctx, "update doc cluster id mapping, oldBizID: %s, mapping: %v", oldBizID, mapping)
			config.IDMappingConfig.SetMappedID(kb_package.ModuleKbDocClusterSchema, oldBizID, *mapping)
		}
	}
	return nil
}

// batchCreateDocClusterSchemas 批量创建文档聚类schema
func (l *Logic) batchCreateDocClusterSchemas(ctx context.Context, schemas []*docEntity.DocClusterSchema, clusterIDs []string, idMap map[string]*kb_package.MappedID) error {
	if len(schemas) == 0 {
		return nil
	}
	if len(schemas) != len(clusterIDs) {
		logx.E(ctx, "batchCreateDocClusterSchemas schemas and clusterIDs length mismatch: %d != %d", len(schemas), len(clusterIDs))
		return fmt.Errorf("schemas and clusterIDs length mismatch: %d != %d", len(schemas), len(clusterIDs))
	}
	for i, schema := range schemas {
		clusterIdStr := clusterIDs[i]
		err := l.docDao.CreateDocClusterSchema(ctx, schema)
		if err != nil {
			logx.E(ctx, "batchCreateDocClusterSchemas create doc cluster schema failed, err: %v", err)
			return err
		}
		logx.InfoContextf(ctx, "batchCreateDocClusterSchemas create doc cluster schema success, schema: %+v", schema)
		// 保存ID映射: 原始ClusterId -> 新的BusinessID
		// 这里使用 schema.BusinessID（新生成的业务ID）作为PrimaryID
		idMap[clusterIdStr] = &kb_package.MappedID{
			PrimaryID: schema.ID,
			BizID:     fmt.Sprintf("%d", schema.BusinessID),
		}
	}
	return nil
}

// batchCreateDocSchemas 批量创建文档schema
func (l *Logic) batchCreateDocSchemas(ctx context.Context, schemas []*docEntity.DocSchema) error {
	if len(schemas) == 0 {
		return nil
	}
	for i, schema := range schemas {
		err := l.docDao.CreateDocSchema(ctx, schema)
		if err != nil {
			logx.E(ctx, "batchCreateDocSchemas failed at index %d: DocBizID=%d, FileName=%s, error: %v",
				i, schema.DocBizID, schema.FileName, err)
			return fmt.Errorf("failed to create doc schema at index %d: %w", i, err)
		}
	}

	logx.I(ctx, "batchCreateDocSchemas successfully created %d doc schemas", len(schemas))
	return nil
}

// batchCreateKnowledgeSchemas 批量创建知识库schema
func (l *Logic) batchCreateKnowledgeSchemas(ctx context.Context, schemas []*kbEntity.KnowledgeSchema) error {
	if len(schemas) == 0 {
		return nil
	}
	for _, schema := range schemas {
		err := l.kbDao.CreateKnowledgeSchema(ctx, schema)
		if err != nil {
			logx.E(ctx, "batchCreateKnowledgeSchemas create knowledge schema failed, err: %v", err)
			return err
		}
	}
	logx.I(ctx, "batchCreateKnowledgeSchemas successfully created %d knowledge schemas", len(schemas))
	return nil
}
