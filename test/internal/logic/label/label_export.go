package label

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
)

const (
	knowledgeItemLabelFileName      = "knowledge_item_label.jsonl"
	knowledgeItemLabelValueFileName = "knowledge_item_label_value.jsonl"
	labelExportBatchSize            = 200
)

// knowledgeItemLabelExport 导出的知识项标签结构
type knowledgeItemLabelExport struct {
	KnowledgeItemLabelId string
	LabelKey             string // AttrKey -> LabelKey
	Name                 string
}

// knowledgeItemLabelValueExport 导出的知识项标签值结构
type knowledgeItemLabelValueExport struct {
	KnowledgeItemLabelValueId string
	KnowledgeItemLabelId      string // 关联的attribute ID
	Name                      string
	SimilarLabel              string
}

// ExportLabels 导出标签数据到本地文件
func (l *Logic) ExportLabels(ctx context.Context, config *kb_package.ExportConfig) error {
	logx.I(ctx, "ExportLabels start, kbPrimaryID: %d, kbID: %d, localPath: %s", config.KbPrimaryID, config.KbID, config.LocalPath)

	// 确保导出目录存在 (在LocalPath下创建label子目录)
	if err := os.MkdirAll(config.LocalPath, 0755); err != nil {
		logx.E(ctx, "ExportLabels mkdir failed, err: %v", err)
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// 1. 导出 t_attribute 表 -> knowledge_item_label.jsonl
	// 同时返回 attr_id -> business_id 的映射，用于后续导出标签值
	labelPath := filepath.Join(config.LocalPath, knowledgeItemLabelFileName)
	labelIDToBizID, err := l.exportKnowledgeItemLabels(ctx, config.KbPrimaryID, labelPath)
	if err != nil {
		return fmt.Errorf("export knowledge item labels failed: %w", err)
	}

	// 2. 导出 t_attribute_label 表 -> knowledge_item_label_value.jsonl
	// 使用 labelIDToBizID 中的 attr_id 列表作为过滤条件，避免按 robot_id 查询导致的慢查询
	valuePath := filepath.Join(config.LocalPath, knowledgeItemLabelValueFileName)
	if err := l.exportKnowledgeItemLabelValues(ctx, config.KbPrimaryID, valuePath, labelIDToBizID); err != nil {
		return fmt.Errorf("export knowledge item label values failed: %w", err)
	}

	logx.I(ctx, "ExportLabels success, kbPrimaryID: %d, kbID: %d", config.KbPrimaryID, config.KbID)
	return nil
}

// exportKnowledgeItemLabels 导出知识项标签表 (原 t_attribute)
// 返回 attr_id -> business_id 的映射，用于后续导出标签值时转换 attr_id
func (l *Logic) exportKnowledgeItemLabels(ctx context.Context, kbPrimaryID uint64, filePath string) (map[uint64]uint64, error) {
	logx.I(ctx, "exportKnowledgeItemLabels start, kbPrimaryID: %d, filePath: %s", kbPrimaryID, filePath)

	// 构建 attr_id -> business_id 的映射
	labelIDToBizID := make(map[uint64]uint64)

	// 创建输出文件
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportKnowledgeItemLabels create file failed, err: %v", err)
		return nil, err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[knowledgeItemLabelExport](file)

	var lastID uint64 = 0
	totalCount := 0

	for {
		// 使用 DAO 层的游标分页查询
		attrs, err := l.dao.GetAttributeChunkByRobotID(ctx, kbPrimaryID, lastID, labelExportBatchSize)
		if err != nil {
			logx.E(ctx, "exportKnowledgeItemLabels query failed, err: %v", err)
			return nil, err
		}
		if len(attrs) == 0 {
			break
		}

		for _, attr := range attrs {
			// 构建映射: attr_id -> business_id
			labelIDToBizID[attr.ID] = attr.BusinessID

			// 构建导出结构
			exportData := knowledgeItemLabelExport{
				KnowledgeItemLabelId: fmt.Sprintf("%d", attr.BusinessID),
				LabelKey:             attr.AttrKey,
				Name:                 attr.Name,
			}

			// 写入文件
			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportKnowledgeItemLabels write failed, attr: %+v, err: %v", attr, err)
				return nil, err
			}

			lastID = attr.ID
			totalCount++
		}

		logx.I(ctx, "exportKnowledgeItemLabels progress, lastID: %d, batch: %d, total: %d", lastID, len(attrs), totalCount)
	}

	// 如果没有数据，删除空文件
	if totalCount == 0 {
		_ = file.Close()
		_ = os.Remove(filePath)
		logx.I(ctx, "exportKnowledgeItemLabels no data, removed empty file: %s", filePath)
		return labelIDToBizID, nil
	}

	logx.I(ctx, "exportKnowledgeItemLabels done, total: %d, file: %s", totalCount, filePath)
	return labelIDToBizID, nil
}

// exportKnowledgeItemLabelValues 导出知识项标签值表 (原 t_attribute_label)
// 使用 labelIDToBizID 中的 attr_id 列表作为过滤条件，利用 idx_atrr_label 索引避免慢查询
func (l *Logic) exportKnowledgeItemLabelValues(ctx context.Context, kbPrimaryID uint64, filePath string, labelIDToBizID map[uint64]uint64) error {
	logx.I(ctx, "exportKnowledgeItemLabelValues start, kbPrimaryID: %d, filePath: %s, attrCount: %d", kbPrimaryID, filePath, len(labelIDToBizID))

	// 如果没有属性，不需要导出标签值
	if len(labelIDToBizID) == 0 {
		logx.I(ctx, "exportKnowledgeItemLabelValues no attrs, skip")
		return nil
	}

	// 创建输出文件
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportKnowledgeItemLabelValues create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[knowledgeItemLabelValueExport](file)

	totalCount := 0

	// 遍历每个 attr_id 对应的attribute label
	for attrID, labelBizID := range labelIDToBizID {
		var lastID uint64 = 0

		for {
			// 使用 DAO 层的游标分页查询
			labels, err := l.dao.GetAttributeLabelChunkByAttrID(ctx, nil, kbPrimaryID, attrID, lastID, labelExportBatchSize)
			if err != nil {
				logx.E(ctx, "exportKnowledgeItemLabelValues query failed, attrID: %d, err: %v", attrID, err)
				return err
			}

			if len(labels) == 0 {
				break
			}

			for _, label := range labels {
				// 构建导出结构
				exportData := knowledgeItemLabelValueExport{
					KnowledgeItemLabelValueId: fmt.Sprintf("%d", label.BusinessID),
					KnowledgeItemLabelId:      fmt.Sprintf("%d", labelBizID),
					Name:                      label.Name,
					SimilarLabel:              label.SimilarLabel,
				}

				// 写入文件
				if err := writer.Write(exportData); err != nil {
					logx.E(ctx, "exportKnowledgeItemLabelValues write failed, label: %+v, err: %v", label, err)
					return err
				}

				lastID = label.ID
				totalCount++
			}

			logx.D(ctx, "exportKnowledgeItemLabelValues progress, attrID: %d, lastID: %d, batch: %d", attrID, lastID, len(labels))
		}
	}

	// 如果没有数据，删除空文件
	if totalCount == 0 {
		_ = file.Close()
		_ = os.Remove(filePath)
		logx.I(ctx, "exportKnowledgeItemLabelValues no data, removed empty file: %s", filePath)
		return nil
	}

	logx.I(ctx, "exportKnowledgeItemLabelValues done, total: %d, file: %s", totalCount, filePath)
	return nil
}
