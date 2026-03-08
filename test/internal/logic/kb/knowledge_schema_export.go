package kb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/logx"
	application_config "git.woa.com/adp/kb/kb-config/internal/config"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	"git.woa.com/adp/kb/kb-config/internal/util"
)

type ItemType int8

var (
	ItemDoc            ItemType = 1
	ItemTypeDocCluster ItemType = 2
)

const (
	knowledgeSchemaFileName = "knowledge_schema.jsonl"
	schemaExportBatchSize   = 200
)

// ExportKnowledgeSchemas 导出知识库schema数据到本地文件
// idsCollector: 用于收集导出过程中的依赖 IDs
func (l *Logic) ExportKnowledgeSchemas(ctx context.Context, config *kb_package.ExportConfig, idsCollector *kb_package.KbIdsCollector) error {
	logx.I(ctx, "ExportKnowledgeSchemas start, appBizID: %d, appPrimaryID: %d, localPath: %s",
		config.AppBizID, config.AppPrimaryID, config.LocalPath)

	// 确保导出目录存在
	if err := os.MkdirAll(config.LocalPath, 0755); err != nil {
		logx.E(ctx, "ExportKnowledgeSchemas mkdir failed, err: %v", err)
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// 1. 导出 t_knowledge_schema_prod 表 -> knowledge_schema_prod.jsonl
	knowledgeSchemaProdPath := filepath.Join(config.LocalPath, knowledgeSchemaFileName)
	// 本身t_knowledge_schema_prod 就引用了doc -id  但是 t_doc_cluster_schema表里的doc_ids字段有几百个doc id 需要导出 这两个需要合并doc ids
	rsp, err := l.exportKnowledgeSchemaProd(ctx, config, knowledgeSchemaProdPath)
	if err != nil {
		return fmt.Errorf("export knowledge_schema_prod failed: %w", err)
	}

	// 2. 导出 t_doc_schema 表 -> doc_schema.jsonl
	docSchemaPath := filepath.Join(config.LocalPath, docSchemaFileName)
	if err := l.exportDocSchema(ctx, config.KbID, docSchemaPath); err != nil {
		return fmt.Errorf("export doc_schema failed: %w", err)
	}

	// 3. 导出 t_doc_cluster_schema 表 -> doc_cluster_schema.jsonl
	docClusterSchemaPath := filepath.Join(config.LocalPath, docClusterSchemaFileName)
	err = l.exportDocClusterSchema(ctx, config, docClusterSchemaPath, rsp.KbDocBizIDMap)
	if err != nil {
		return fmt.Errorf("export doc_cluster_schema failed: %w", err)
	}
	// KbDocBizIDMap
	logx.InfoContextf(ctx, "rsp.KbDocBizIDMap: %+v", rsp.KbDocBizIDMap)
	// 4. 将收集的 IDs 添加到收集器中
	l.collectSchemaExportIDs(idsCollector, rsp.KbDocBizIDMap, rsp.ClusterBizIDMap)

	logx.I(ctx, "ExportKnowledgeSchemas success, appBizID: %d, appPrimaryID: %d", config.AppBizID, config.AppPrimaryID)
	return nil
}

// collectSchemaExportIDs 将 Schema 导出过程中收集的 IDs 添加到收集器中
func (l *Logic) collectSchemaExportIDs(idsCollector *kb_package.KbIdsCollector, kbDocBizIDMap, clusterBizIDMap map[string]struct{}) {
	// 添加 KbDoc IDs
	docIDItems := make([]string, 0, len(kbDocBizIDMap))
	for id := range kbDocBizIDMap {
		docIDItems = append(docIDItems, id)
	}
	idsCollector.AddKbDocs(docIDItems)

	// 添加 KbSchemaCluster IDs
	clusterIDItems := make([]string, 0, len(clusterBizIDMap))
	for id := range clusterBizIDMap {
		clusterIDItems = append(clusterIDItems, id)
	}
	idsCollector.AddKbSchemaClusters(clusterIDItems)
}

// exportKnowledgeSchemaProd 导出知识库schema已发布数据表
func (l *Logic) exportKnowledgeSchemaProd(ctx context.Context, config *kb_package.ExportConfig, filePath string) (*kbEntity.ExportKnowledgeSchemaProdRsp, error) {
	logx.I(ctx, "exportKnowledgeSchemaProd start, appBizID: %d, filePath: %s", config.AppBizID, filePath)
	rsp := &kbEntity.ExportKnowledgeSchemaProdRsp{
		KbDocBizIDMap:   make(map[string]struct{}),
		ClusterBizIDMap: make(map[string]struct{}),
	}
	kbDocBizIDMap := make(map[string]struct{})
	clusterBizIDMap := make(map[string]struct{})
	// 创建输出文件
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportKnowledgeSchemaProd create file failed, err: %v", err)
		return nil, err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[kbEntity.KnowledgeSchemaProdExport](file)

	// 查询所有数据（使用FindKnowledgeSchema，指定EnvType为product）
	isNotDeleted := kbEntity.IsNotDeleted
	knowledgeSchemas, err := l.kbDao.FindKnowledgeSchema(ctx,
		[]string{"version", "item_type", "item_biz_id", "name", "summary"},
		&kbEntity.KnowledgeSchemaFilter{
			AppBizId:  config.KbID,
			EnvType:   application_config.App().KbSchemaExportEnv,
			IsDeleted: &isNotDeleted,
		})
	if err != nil {
		logx.E(ctx, "exportKnowledgeSchemaProd query failed, err: %v", err)
		return nil, err
	}

	totalCount := 0
	for _, schema := range knowledgeSchemas {
		var ItemBizId string
		// 根据物料类型处理不同的逻辑
		switch schema.ItemType {
		case int8(ItemDoc):
			// 1是doc 主键id 还得换 biz id
			docMap, err := l.docDao.GetDocByIDs(ctx, []uint64{schema.ItemBizId}, config.KbPrimaryID)
			if err != nil {
				logx.E(ctx, "exportDocClusterSchema GetDocByIDs failed, err: %v", err)
				return nil, err
			}
			if doc, exists := docMap[schema.ItemBizId]; exists && doc != nil {
				kbDocBizIDMap[fmt.Sprintf("%d", doc.BusinessID)] = struct{}{}
				ItemBizId = fmt.Sprintf("%d", doc.BusinessID)
			} else {
				logx.W(ctx, "exportKnowledgeSchemaProd doc not found, itemBizId :%d", schema.ItemBizId)
				continue
			}
		case int8(ItemTypeDocCluster):
			// 2直接是cluster biz id
			ItemBizId = fmt.Sprintf("%d", schema.ItemBizId)
			clusterBizIDMap[fmt.Sprintf("%d", schema.ItemBizId)] = struct{}{}
		}

		// 构建导出结构
		exportData := kbEntity.KnowledgeSchemaProdExport{
			ItemType:  schema.ItemType,
			ItemBizId: ItemBizId,
			Name:      schema.Name,
			Summary:   schema.Summary,
		}
		logx.InfoContextf(ctx, "exportKnowledgeSchemaProd exportData : %+v", exportData)
		// 写入文件
		if err := writer.Write(exportData); err != nil {
			logx.E(ctx, "exportKnowledgeSchemaProd write failed, schema: %+v, err: %v", schema, err)
			return nil, err
		}
		totalCount++
	}
	logx.I(ctx, "exportKnowledgeSchemaProd done, total: %d, file: %s", totalCount, filePath)
	rsp.KbDocBizIDMap = kbDocBizIDMap
	rsp.ClusterBizIDMap = clusterBizIDMap
	return rsp, nil
}

// exportDocSchema 导出文档schema表
func (l *Logic) exportDocSchema(ctx context.Context, appBizID uint64, filePath string) error {
	logx.I(ctx, "exportDocSchema start, appBizID: %d, filePath: %s", appBizID, filePath)

	// 创建输出文件
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocSchema create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[kbEntity.DocSchemaExport](file)

	var lastID uint64 = 0
	totalCount := 0

	for {
		// 使用游标分页查询
		isNotDeleted := 0
		docSchemas, err := l.docDao.GetDocSchemaList(ctx,
			[]string{"id", "doc_biz_id", "file_name", "summary"},
			&docEntity.DocSchemaFilter{
				AppBizId:       appBizID,
				IsDeleted:      &isNotDeleted,
				Offset:         0,
				Limit:          schemaExportBatchSize,
				OrderColumn:    []string{"id"},
				OrderDirection: []string{util.SqlOrderByAsc},
			})
		if err != nil {
			logx.E(ctx, "exportDocSchema query failed, err: %v", err)
			return err
		}

		if len(docSchemas) == 0 {
			break
		}

		// 过滤出 id > lastID 的数据
		var filteredSchemas []*docEntity.DocSchema
		for _, schema := range docSchemas {
			if schema.ID > lastID {
				filteredSchemas = append(filteredSchemas, schema)
			}
		}

		if len(filteredSchemas) == 0 {
			break
		}

		for _, schema := range filteredSchemas {
			// 构建导出结构
			exportData := kbEntity.DocSchemaExport{
				DocId:    fmt.Sprintf("%d", schema.DocBizID),
				FileName: schema.FileName,
				Summary:  schema.Summary,
				Vector:   base64.StdEncoding.EncodeToString(schema.Vector),
			}

			// 写入文件
			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportDocSchema write failed, schema: %+v, err: %v", schema, err)
				return err
			}

			lastID = schema.ID
			totalCount++
		}

		logx.I(ctx, "exportDocSchema progress, lastID: %d, batch: %d, total: %d", lastID, len(filteredSchemas), totalCount)

		// 如果本批次数据少于批次大小，说明已经查询完毕
		if len(docSchemas) < schemaExportBatchSize {
			break
		}
	}

	logx.I(ctx, "exportDocSchema done, total: %d, file: %s", totalCount, filePath)
	return nil
}

// exportDocClusterSchema 导出文档聚类schema表
func (l *Logic) exportDocClusterSchema(ctx context.Context, config *kb_package.ExportConfig, filePath string, kbDocBizIDMap map[string]struct{}) error {
	logx.I(ctx, "exportDocClusterSchema start, appBizID: %d, filePath: %s", config.AppBizID, filePath)
	// 创建输出文件
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocClusterSchema create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[kbEntity.DocClusterSchemaExport](file)

	var lastID uint64 = 0
	totalCount := 0

	for {
		// 使用游标分页查询
		isNotDeleted := 0
		docClusterSchemas, err := l.docDao.GetDocClusterSchemaList(ctx,
			[]string{"id", "business_id", "version", "cluster_name", "summary", "doc_ids"},
			&docEntity.DocClusterSchemaFilter{
				AppBizId:       config.KbID,
				IsDeleted:      &isNotDeleted,
				Offset:         0,
				Limit:          schemaExportBatchSize,
				OrderColumn:    []string{"id"},
				OrderDirection: []string{util.SqlOrderByAsc},
			})
		if err != nil {
			logx.E(ctx, "exportDocClusterSchema query failed, err: %v", err)
			return err
		}

		if len(docClusterSchemas) == 0 {
			break
		}

		// 过滤出 id > lastID 的数据
		var filteredSchemas []*docEntity.DocClusterSchema
		for _, schema := range docClusterSchemas {
			if schema.ID > lastID {
				filteredSchemas = append(filteredSchemas, schema)
			}
		}

		if len(filteredSchemas) == 0 {
			// 更新lastID为本批次最后一条记录的ID
			if len(docClusterSchemas) > 0 {
				lastID = docClusterSchemas[len(docClusterSchemas)-1].ID
			}
			// 如果本批次数据少于批次大小，说明已经查询完毕
			if len(docClusterSchemas) < schemaExportBatchSize {
				break
			}
			continue
		}

		for _, schema := range filteredSchemas {
			//这里要把docIDs转换为doc bizID
			// 1. 解析字符串格式的DocIDs为[]uint64
			docIDs, err := parseDocIDsString(schema.DocIDs)
			if err != nil {
				logx.E(ctx, "exportDocClusterSchema parse docIDs failed, docIDs: %s, err: %v", schema.DocIDs, err)
				return err
			}
			// 2. 使用解析后的docIDs批量查询
			if len(docIDs) > 0 {
				docMap, err := l.docDao.GetDocByIDs(ctx, docIDs, config.KbPrimaryID)
				if err != nil {
					logx.E(ctx, "exportDocClusterSchema GetDocByIDs failed, err: %v", err)
					return err
				}
				// 3. 提取业务ID并序列化为JSON数组
				var businessIDs []uint64
				for _, doc := range docMap {
					if doc != nil {
						businessIDs = append(businessIDs, doc.BusinessID)
						kbDocBizIDMap[fmt.Sprintf("%d", doc.BusinessID)] = struct{}{}
					}
				}
				jsonBytes, err := json.Marshal(businessIDs)
				if err != nil {
					logx.E(ctx, "exportDocClusterSchema marshal businessIDs failed, err: %v", err)
					return err
				}
				schema.DocIDs = string(jsonBytes)
			}
			// 构建导出结构
			exportData := kbEntity.DocClusterSchemaExport{
				ClusterId:   fmt.Sprintf("%d", schema.BusinessID),
				ClusterName: schema.ClusterName,
				Summary:     schema.Summary,
				DocIDs:      schema.DocIDs,
			}

			// 写入文件
			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportDocClusterSchema write failed, schema: %+v, err: %v", schema, err)
				return err
			}
			lastID = schema.ID
			totalCount++
		}
		logx.I(ctx, "exportDocClusterSchema progress, lastID: %d, batch: %d, total: %d", lastID, len(filteredSchemas), totalCount)
		// 如果本批次数据少于批次大小，说明已经查询完毕
		if len(docClusterSchemas) < schemaExportBatchSize {
			break
		}
	}
	logx.I(ctx, "exportDocClusterSchema done, total: %d, file: %s", totalCount, filePath)
	return nil
}

// parseDocIDsString 解析字符串格式的DocIDs，如'[195148,193112,193898]'
func parseDocIDsString(docIDsStr string) ([]uint64, error) {
	// 直接使用json.Unmarshal解析JSON数组
	var result []uint64
	err := json.Unmarshal([]byte(docIDsStr), &result)
	if err != nil {
		return nil, fmt.Errorf("parse docIDs JSON failed: %w", err)
	}
	return result, nil
}
