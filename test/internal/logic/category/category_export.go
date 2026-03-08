package category

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/logx"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
)

const (
	categoryExportBatchSize = 200
	docCategoryFileName     = "doc_category.jsonl"
	qaCategoryFileName      = "qa_category.jsonl"
)

// ExportCategory 导出分类数据
// idsCollector: 用于收集导出过程中的依赖 IDs
func (l *Logic) ExportCategory(ctx context.Context, config *kb_package.ExportConfig, idsCollector *kb_package.KbIdsCollector) error {
	logx.I(ctx, "ExportCategory start, corpPrimaryID: %d, kbPrimaryID: %d, kbID: %d, localPath: %s",
		config.CorpPrimaryID, config.KbPrimaryID, config.KbID, config.LocalPath)

	// 确保导出目录存在 (在LocalPath下创建qa子目录)
	if err := os.MkdirAll(config.LocalPath, 0755); err != nil {
		logx.E(ctx, "ExportCategory mkdir failed, err: %v", err)
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// 创建doc_category.jsonl文件
	docCategoryFilePath := filepath.Join(config.LocalPath, docCategoryFileName)
	docCategoryFile, err := os.Create(docCategoryFilePath)
	if err != nil {
		logx.E(ctx, "ExportCategory  create docCategoryFilePath file failed, err: %v", err)
		return fmt.Errorf("create docCategoryFilePath file failed: %w", err)
	}
	defer docCategoryFile.Close()
	docCategoryWriter := jsonlx.NewWriter[cateEntity.CateExport](docCategoryFile)

	// 创建qa_category.jsonl文件
	qaCategoryFilePath := filepath.Join(config.LocalPath, qaCategoryFileName)
	qaCategoryFile, err := os.Create(qaCategoryFilePath)
	if err != nil {
		logx.E(ctx, "ExportCategory  create qaCategoryFilePath file failed, err: %v", err)
		return fmt.Errorf("create qaCategoryFilePath file failed: %w", err)
	}
	defer qaCategoryFile.Close()
	qaCategoryWriter := jsonlx.NewWriter[cateEntity.CateExport](qaCategoryFile)

	// 导出文档分类数
	docCategoryCount, docCategoryIDs, err := l.exportDocCategories(ctx, config, docCategoryWriter)
	if err != nil {
		logx.E(ctx, "ExportCategory export doc categories failed, err: %v", err)
		return fmt.Errorf("export doc categories failed: %w", err)
	}

	// 导出问答分类数据
	qaCategoryCount, qaCategoryIDs, err := l.exportQACategories(ctx, config, qaCategoryWriter)
	if err != nil {
		logx.E(ctx, "ExportCategory export qa categories failed, err: %v", err)
		return fmt.Errorf("export qa categories failed: %w", err)
	}

	// 将收集的 IDs 添加到收集器中
	l.collectCategoryExportIDs(idsCollector, docCategoryIDs, qaCategoryIDs)

	logx.I(ctx, "ExportCategory done, docCategories: %d, qaCategoryCount: %d, docCategoryIDs: %d, qaCategoryIDs: %d",
		docCategoryCount, qaCategoryCount, len(docCategoryIDs), len(qaCategoryIDs))

	return nil
}

// collectCategoryExportIDs 将 Category 导出过程中收集的 IDs 添加到收集器中
func (l *Logic) collectCategoryExportIDs(idsCollector *kb_package.KbIdsCollector, docCategoryIDs, qaCategoryIDs []uint64) {
	// 添加 KbDocCategory IDs
	docCategoryIDItems := make([]string, 0, len(docCategoryIDs))
	for _, id := range docCategoryIDs {
		docCategoryIDItems = append(docCategoryIDItems, fmt.Sprintf("%d", id))
	}
	idsCollector.AddKbDocCategories(docCategoryIDItems)

	// 添加 KbQaCategory IDs
	qaCategoryIDItems := make([]string, 0, len(qaCategoryIDs))
	for _, id := range qaCategoryIDs {
		qaCategoryIDItems = append(qaCategoryIDItems, fmt.Sprintf("%d", id))
	}
	idsCollector.AddKbQaCategories(qaCategoryIDItems)
}

// exportDocCategories 导出文档分类数据
func (l *Logic) exportDocCategories(ctx context.Context, config *kb_package.ExportConfig, writer *jsonlx.Writer[cateEntity.CateExport]) (int, []uint64, error) {
	logx.I(ctx, "exportDocCategories start")

	var lastID uint64 = 0
	totalCount := 0
	businessIDs := make([]uint64, 0, categoryExportBatchSize)

	logx.I(ctx, "exportDocCategories step1: collecting doc category data with cursor pagination")
	for {
		// 使用游标方式获取文档分类，利用索引排序
		categories, err := l.dao.GetDocCategoryByCursor(ctx, config.CorpPrimaryID, config.KbPrimaryID, lastID, categoryExportBatchSize, nil)
		if err != nil {
			logx.E(ctx, "exportDocCategories GetDocCategoryByCursor failed, err: %v", err)
			return 0, nil, err
		}

		if len(categories) == 0 {
			break
		}

		// 写入数据并收集BusinessID
		for _, category := range categories {
			var ParentID uint64
			if category.ParentID != 0 {
				//要把parent_id转成业务id，导入的时候再转成t_doc_category的自增主键
				cate, err := l.dao.DescribeCateByID(ctx, cateEntity.DocCate, category.ParentID, category.CorpID, category.RobotID)
				if err != nil {
					logx.E(ctx, "exportDocCategories DescribeCateByID failed, err: %v", err)
					return 0, nil, err
				}
				ParentID = cate.BusinessID

			} else {
				ParentID = category.ParentID
			}
			category.ParentID = ParentID
			if err := l.writeCategoryData(ctx, category, writer); err != nil {
				logx.E(ctx, "exportDocCategories write category data failed, categoryBizID: %d, err: %v", category.BusinessID, err)
				return 0, nil, fmt.Errorf("write category data failed, categoryBizID: %d, err: %w", category.BusinessID, err)
			}
			businessIDs = append(businessIDs, category.BusinessID)
			totalCount++
		}

		// 更新游标为最后一条记录的 BusinessID
		lastCategory := categories[len(categories)-1]
		lastID = lastCategory.BusinessID

		logx.D(ctx, "exportDocCategories progress, lastID: %d, batch: %d, total: %d, collectedIDs: %d",
			lastID, len(categories), totalCount, len(businessIDs))

		// 如果返回的记录数少于请求的数量，说明已经没有更多数据了
		if len(categories) < categoryExportBatchSize {
			break
		}
	}

	logx.I(ctx, "exportDocCategories done, total: %d, collectedIDs: %d", totalCount, len(businessIDs))
	return totalCount, businessIDs, nil
}

// exportQACategories 导出问答分类数据
func (l *Logic) exportQACategories(ctx context.Context, config *kb_package.ExportConfig, writer *jsonlx.Writer[cateEntity.CateExport]) (int, []uint64, error) {
	logx.I(ctx, "exportQACategories start")

	var lastID uint64 = 0
	totalCount := 0
	businessIDs := make([]uint64, 0, categoryExportBatchSize)

	logx.I(ctx, "exportQACategories step1: collecting qa category data with cursor pagination")

	for {
		// 使用游标方式获取问答分类，利用索引排序
		categories, err := l.dao.GetQCategoryByCursor(ctx, config.CorpPrimaryID, config.KbPrimaryID, lastID, categoryExportBatchSize, nil)
		if err != nil {
			logx.E(ctx, "exportQACategories GetQCategoryByCursor failed, err: %v", err)
			return 0, nil, err
		}

		if len(categories) == 0 {
			break
		}

		// 写入数据并收集BusinessID
		for _, category := range categories {
			//要把parent_id转成业务id，导入的时候再转成t_qa_category的自增主键
			qaBizId, err := l.dao.DescribeQACateBusinessIDByID(ctx, category.ParentID, category.CorpID, category.RobotID)
			if err != nil {
				logx.E(ctx, "exportQACategories DescribeQACateBusinessIDByID failed, err: %v", err)
				return 0, nil, err
			}
			category.ParentID = qaBizId
			if err := l.writeCategoryData(ctx, category, writer); err != nil {
				logx.E(ctx, "exportQACategories write category data failed, categoryBizID: %d, err: %v", category.BusinessID, err)
				return 0, nil, fmt.Errorf("write category data failed, categoryBizID: %d, err: %w", category.BusinessID, err)
			}
			businessIDs = append(businessIDs, category.BusinessID)
			totalCount++
		}

		// 更新游标为最后一条记录的 BusinessID
		lastCategory := categories[len(categories)-1]
		lastID = lastCategory.BusinessID

		logx.D(ctx, "exportQACategories progress, lastID: %d, batch: %d, total: %d, collectedIDs: %d",
			lastID, len(categories), totalCount, len(businessIDs))

		// 如果返回的记录数少于请求的数量，说明已经没有更多数据了
		if len(categories) < categoryExportBatchSize {
			break
		}
	}

	logx.I(ctx, "exportQACategories done, total: %d, collectedIDs: %d", totalCount, len(businessIDs))
	return totalCount, businessIDs, nil
}

// writeCategoryData 写入单条分类数据
func (l *Logic) writeCategoryData(ctx context.Context, category *cateEntity.CateInfo, writer *jsonlx.Writer[cateEntity.CateExport]) error {

	exportData := cateEntity.CateExport{
		CategoryId: fmt.Sprintf("%d", category.BusinessID),
		Name:       category.Name,
		OrderNum:   int(category.OrderNum),
		ParentId:   fmt.Sprintf("%d", category.ParentID),
	}

	return writer.Write(exportData)
}
