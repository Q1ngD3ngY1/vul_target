package label

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
)

// importLabelBatchSize 导入标签的批次大小
const importLabelBatchSize = 5

// ImportLabels 从本地文件导入标签数据到新的应用
func (l *Logic) ImportLabels(ctx context.Context, config *kb_package.ImportConfig) error {
	logx.I(ctx, "ImportLabels start, kbPrimaryID: %d, kbID: %d, localPath: %s",
		config.KbPrimaryID, config.KbID, config.LocalPath)

	// 1. 读取导出文件，构建 AttributeLabelItem 列表
	labelPath := filepath.Join(config.LocalPath, knowledgeItemLabelFileName)
	valuePath := filepath.Join(config.LocalPath, knowledgeItemLabelValueFileName)

	attrItems, err := l.readLabelExportFiles(ctx, config.KbPrimaryID, labelPath, valuePath)
	if err != nil {
		return fmt.Errorf("read label export files failed: %w", err)
	}
	if len(attrItems) == 0 {
		logx.I(ctx, "ImportLabels no labels to import")
		return nil
	}

	// 2. 校验每个属性标签
	for _, item := range attrItems {
		req := l.buildCheckReqFromItem(ctx, config.KbID, item)
		if err := l.CheckCreateAttributeLabel(ctx, req); err != nil {
			logx.E(ctx, "ImportLabels check failed, attrKey: %s, attrName: %s, err: %v",
				item.Attr.AttrKey, item.Attr.Name, err)
			return fmt.Errorf("check attribute label failed, attrName=%s: %w", item.Attr.Name, err)
		}
	}

	// 3. 分批创建属性标签
	result := &BatchCreateAttributeResult{
		AttrOldBizIDMapping:  make(map[uint64]*IDMapping),
		LabelOldBizIDMapping: make(map[uint64]*IDMapping),
	}

	for i, batch := range slicex.Chunk(attrItems, importLabelBatchSize) {
		batchResult, err := l.BatchCreateAttribute(ctx, batch, config.IDMappingConfig)
		if err != nil {
			logx.E(ctx, "ImportLabels BatchCreateAttribute failed, batch: %d, err: %v", i+1, err)
			return fmt.Errorf("batch create attribute failed at batch %d: %w", i+1, err)
		}
		logx.I(ctx, "ImportLabels batch result %v", jsonx.MustMarshalToString(batchResult))
		// 合并结果
		for k, v := range batchResult.AttrOldBizIDMapping {
			result.AttrOldBizIDMapping[k] = v
		}
		for k, v := range batchResult.LabelOldBizIDMapping {
			result.LabelOldBizIDMapping[k] = v
		}
	}

	// 4. 更新 IDMappingConfig 中的标签ID映射
	l.updateLabelIDMapping(config, result)

	logx.I(ctx, "ImportLabels done, attrCount: %d", len(attrItems))
	return nil
}

// readLabelExportFiles 读取导出的标签文件，返回 AttributeLabelItem 列表
func (l *Logic) readLabelExportFiles(ctx context.Context, kbPrimaryID uint64, labelPath, valuePath string) (
	[]*labelEntity.AttributeLabelItem, error) {

	// 1. 读取属性文件，构建 oldBizID -> Attribute 的映射
	attrMap, err := l.readAttributeFile(ctx, kbPrimaryID, labelPath)
	if err != nil {
		return []*labelEntity.AttributeLabelItem{}, err
	}
	if len(attrMap) == 0 {
		return []*labelEntity.AttributeLabelItem{}, nil
	}

	// 2. 读取标签值文件，填充到对应的属性中
	if err := l.readLabelValueFile(ctx, kbPrimaryID, valuePath, attrMap); err != nil {
		return []*labelEntity.AttributeLabelItem{}, err
	}

	// 3. 转换为列表
	items := make([]*labelEntity.AttributeLabelItem, 0, len(attrMap))
	for _, item := range attrMap {
		items = append(items, item)
	}

	return items, nil
}

// readAttributeFile 读取属性文件
// 返回 oldBizID -> AttributeLabelItem 的映射，其中 Attr.BusinessID 保存了旧的 business_id
func (l *Logic) readAttributeFile(ctx context.Context, kbPrimaryID uint64, filePath string) (
	map[string]*labelEntity.AttributeLabelItem, error) {

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "readAttributeFile file not exist, skip import, filePath: %s", filePath)
			return make(map[string]*labelEntity.AttributeLabelItem), nil
		}
		logx.E(ctx, "readAttributeFile open file failed, err: %v", err)
		return make(map[string]*labelEntity.AttributeLabelItem), err
	}
	defer file.Close()

	reader := jsonlx.NewReader[knowledgeItemLabelExport](file)
	attrMap := make(map[string]*labelEntity.AttributeLabelItem)

	err = reader.Each(func(label knowledgeItemLabelExport) error {
		// 将 string 类型的 KnowledgeItemLabelId 转换为 uint64 保存到 BusinessID
		businessID, parseErr := strconv.ParseUint(label.KnowledgeItemLabelId, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "readAttributeFile parse KnowledgeItemLabelId failed, value: %s, err: %v", label.KnowledgeItemLabelId, parseErr)
			return parseErr
		}
		item := &labelEntity.AttributeLabelItem{
			Attr: &labelEntity.Attribute{
				RobotID:    kbPrimaryID,
				BusinessID: businessID, // 保存旧的 business_id，用于后续映射
				AttrKey:    label.LabelKey,
				Name:       label.Name,
			},
			Labels: make([]*labelEntity.AttributeLabel, 0),
		}
		attrMap[label.KnowledgeItemLabelId] = item
		return nil
	})
	if err != nil {
		logx.E(ctx, "readAttributeFile read failed, err: %v", err)
		return make(map[string]*labelEntity.AttributeLabelItem), err
	}

	logx.I(ctx, "readAttributeFile done, count: %d", len(attrMap))
	return attrMap, nil
}

// readLabelValueFile 读取标签值文件，填充到对应的属性中
// 其中 label.BusinessID 保存了旧的 business_id，用于后续映射
func (l *Logic) readLabelValueFile(ctx context.Context, kbPrimaryID uint64, filePath string,
	attrMap map[string]*labelEntity.AttributeLabelItem) error {

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "readLabelValueFile file not exist, filePath: %s", filePath)
			return nil
		}
		logx.E(ctx, "readLabelValueFile open file failed, err: %v", err)
		return err
	}
	defer file.Close()

	reader := jsonlx.NewReader[knowledgeItemLabelValueExport](file)
	totalCount := 0

	err = reader.Each(func(lv knowledgeItemLabelValueExport) error {
		item, ok := attrMap[lv.KnowledgeItemLabelId]
		if !ok {
			logx.W(ctx, "readLabelValueFile attr not found, labelValueId: %s, attrBizId: %s",
				lv.KnowledgeItemLabelValueId, lv.KnowledgeItemLabelId)
			return nil
		}

		// 将 string 类型的 KnowledgeItemLabelValueId 转换为 uint64 保存到 BusinessID
		businessID, parseErr := strconv.ParseUint(lv.KnowledgeItemLabelValueId, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "readLabelValueFile parse KnowledgeItemLabelValueId failed, value: %s, err: %v", lv.KnowledgeItemLabelValueId, parseErr)
			return parseErr
		}

		// 解析相似标签
		var similarLabels []string
		if lv.SimilarLabel != "" {
			if err := jsonx.Unmarshal([]byte(lv.SimilarLabel), &similarLabels); err != nil {
				logx.W(ctx, "readLabelValueFile parse similar label failed, err: %v", err)
				similarLabels = nil
			}
		}

		label := &labelEntity.AttributeLabel{
			RobotID:      kbPrimaryID,
			BusinessID:   businessID, // 保存旧的 business_id，用于后续映射
			Name:         lv.Name,
			SimilarLabel: lv.SimilarLabel,
		}
		item.Labels = append(item.Labels, label)
		totalCount++
		return nil
	})
	if err != nil {
		logx.E(ctx, "readLabelValueFile read failed, err: %v", err)
		return err
	}

	logx.I(ctx, "readLabelValueFile done, count: %d", totalCount)
	return nil
}

// buildCheckReqFromItem 从 AttributeLabelItem 构建校验请求
func (l *Logic) buildCheckReqFromItem(ctx context.Context, kbID uint64, item *labelEntity.AttributeLabelItem) *CreateAttributeLabelReq {
	labels := make([]*CreateAttributeLabelItem, 0, len(item.Labels))
	for _, label := range item.Labels {
		// 解析 SimilarLabel JSON 字符串为数组
		var similarLabels []string
		if label.SimilarLabel != "" {
			_ = jsonx.Unmarshal([]byte(label.SimilarLabel), &similarLabels)
		}
		labels = append(labels, &CreateAttributeLabelItem{
			LabelName:     label.Name,
			SimilarLabels: similarLabels,
		})
	}

	return &CreateAttributeLabelReq{
		RobotID:  item.Attr.RobotID,
		BizID:    kbID,
		Uin:      contextx.Metadata(ctx).Uin(),
		AttrKey:  item.Attr.AttrKey,
		AttrName: item.Attr.Name,
		Labels:   labels,
	}
}

// updateLabelIDMapping 更新 IDMappingConfig 中的标签ID映射
// 只有在 IDMappingConfig 中已存在的旧 BusinessID 才会被更新
func (l *Logic) updateLabelIDMapping(config *kb_package.ImportConfig, result *BatchCreateAttributeResult) {
	// 更新属性（Label Key）ID 映射
	for oldBizID, mapping := range result.AttrOldBizIDMapping {
		oldIDStr := strconv.FormatUint(oldBizID, 10)
		// 只有在 IDMappingConfig 中已存在的才更新
		if config.IDMappingConfig.IsMappedIDExist(kb_package.ModuleKbLabel, oldIDStr) {
			config.IDMappingConfig.SetMappedID(kb_package.ModuleKbLabel, oldIDStr, kb_package.MappedID{
				PrimaryID: mapping.NewPrimaryID,
				BizID:     strconv.FormatUint(mapping.NewBizID, 10),
			})
		}
	}

	// 更新标签值（Label Value）ID 映射
	for oldBizID, mapping := range result.LabelOldBizIDMapping {
		oldIDStr := strconv.FormatUint(oldBizID, 10)
		// 只有在 IDMappingConfig 中已存在的才更新
		if config.IDMappingConfig.IsMappedIDExist(kb_package.ModuleKbLabelValue, oldIDStr) {
			config.IDMappingConfig.SetMappedID(kb_package.ModuleKbLabelValue, oldIDStr, kb_package.MappedID{
				PrimaryID: mapping.NewPrimaryID,
				BizID:     strconv.FormatUint(mapping.NewBizID, 10),
			})
		}
	}
}
