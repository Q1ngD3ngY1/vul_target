package category

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"github.com/spf13/cast"
)

const (
	categoryImportBatchSize = 200
)

// ImportCategory 导入分类数据
func (l *Logic) ImportCategory(ctx context.Context, config *kb_package.ImportConfig) error {
	// 从config.LocalPath指定的本地路径读取分类数据
	// LocalPath 已经是 category 子目录，直接使用
	logx.I(ctx, "ImportCategory start, kbPrimaryID: %d, kbID: %d, localPath: %s",
		config.KbPrimaryID, config.KbID, config.LocalPath)
	// 确保导入目录存在
	if _, err := os.Stat(config.LocalPath); os.IsNotExist(err) {
		logx.I(ctx, "ImportCategory category directory not exist, skip import, path: %s", config.LocalPath)
		return nil
	}
	//得先把未分类的脏数据删掉
	if err := l.dao.DeleteCateById(ctx, cateEntity.DocCate, config.KbPrimaryID); err != nil {
		logx.E(ctx, "ImportCategory delete uncategorized DocCate failed, err: %v", err)
		return err
	}
	if err := l.dao.DeleteCateById(ctx, cateEntity.QACate, config.KbPrimaryID); err != nil {
		logx.E(ctx, "ImportCategory delete uncategorized QACate failed, err: %v", err)
		return err
	}

	// 解析doc_category.jsonl
	docCategoryFilePath := filepath.Join(config.LocalPath, docCategoryFileName)
	err := l.readDocCategoryFile(ctx, config, docCategoryFilePath, cateEntity.DocCate)
	if err != nil {
		logx.E(ctx, "ImportCategory docCategoryFilePath readDocCategoryFile failed, err: %v", err)
		return err
	}
	// 解析qa_category.jsonl
	qaDocCategoryFilePath := filepath.Join(config.LocalPath, qaCategoryFileName)
	err = l.readDocCategoryFile(ctx, config, qaDocCategoryFilePath, cateEntity.QACate)
	if err != nil {
		logx.E(ctx, "ImportCategory qaDocCategoryFilePath readDocCategoryFile failed, err: %v", err)
		return err
	}
	return nil
}

// readDocCategoryFile 解析doc_category.jsonl
func (l *Logic) readDocCategoryFile(ctx context.Context, config *kb_package.ImportConfig, filePath string, cateType cateEntity.CateObjectType) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "readDocCategoryFile file not exist, filePath: %s", filePath)
			return nil
		}
		logx.E(ctx, "readDocCategoryFile open file failed, err: %v", err)
		return err
	}
	defer file.Close()

	reader := jsonlx.NewReader[cateEntity.CateExport](file)
	// 批量插入，避免一次打挂数据库
	var cateBatch []*cateEntity.CateInfo
	// 记录BusinessID到旧CategoryId的映射，用于后续更新IDMappingConfig
	oldIDMap := make(map[uint64]string)
	err = reader.Each(func(cate cateEntity.CateExport) error {
		cateInfo := &cateEntity.CateInfo{
			BusinessID: idgen.GetId(),
			RobotID:    config.KbPrimaryID,
			CorpID:     config.CorpPrimaryID,
			Name:       cate.Name,
			OrderNum:   int32(cate.OrderNum),
			IsDeleted:  false,
			ParentID:   cast.ToUint64(cate.ParentId),
			CreateTime: time.Time{},
			UpdateTime: time.Time{},
		}
		moduleType := gox.IfElse(cateType == cateEntity.DocCate, kb_package.ModuleKbDocCategory, kb_package.ModuleKbQaCategory)
		if config.IDMappingConfig.IsMappedIDExist(moduleType, cate.CategoryId) {
			if newBizID, err := config.IDMappingConfig.ConvertToBizID(ctx, moduleType, cate.CategoryId); err == nil {
				cateInfo.BusinessID = cast.ToUint64(newBizID)
			}
		}
		logx.I(ctx, "ori cateInfo: %v", jsonx.MustMarshalToString(cate))
		logx.I(ctx, "new cateInfo: %v", jsonx.MustMarshalToString(cateInfo))
		cateBatch = append(cateBatch, cateInfo)
		// 记录BusinessID到旧CategoryId的映射
		oldIDMap[cateInfo.BusinessID] = cate.CategoryId
		// 达到批量大小时执行批量插入
		if len(cateBatch) >= categoryImportBatchSize {
			if err := l.batchCreateCates(ctx, config, cateBatch, cateType, oldIDMap); err != nil {
				return err
			}
			// 清空批次
			cateBatch = nil
			oldIDMap = make(map[uint64]string)
		}
		return nil
	})
	// 处理剩余的批次
	if err == nil && len(cateBatch) > 0 {
		err = l.batchCreateCates(ctx, config, cateBatch, cateType, oldIDMap)
	}
	if err != nil {
		logx.E(ctx, "readDocCategoryFile read failed, err: %v", err)
		return err
	}
	return nil
}

/*
	遍历
	D
	 C
	  B
	   A
	- D: 父分类 C 不存在 → 跳过
	- C: 父分类 B 不存在 → 跳过
	- B: 父分类 A 不存在 → 跳过
	- A: 根分类 →  创建成功
*/
// batchCreateCates 批量创建分类
func (l *Logic) batchCreateCates(ctx context.Context, config *kb_package.ImportConfig, cates []*cateEntity.CateInfo,
	t cateEntity.CateObjectType, oldIDMap map[uint64]string) error {
	logx.I(ctx, "batchCreateCates cates: %v", jsonx.MustMarshalToString(cates))
	if len(cates) == 0 {
		return nil
	}
	moduleType := gox.IfElse(t == cateEntity.DocCate, kb_package.ModuleKbDocCategory, kb_package.ModuleKbQaCategory)

	// 使用 map 记录已创建的分类（通过 BusinessID）
	createdCates := make(map[uint64]bool)
	// 循环创建分类，每次循环创建一层，直到所有分类都创建完成
	// 最多循环 len(cates) 次，避免死循环
	maxIterations := len(cates)
	for iteration := 0; iteration < maxIterations; iteration++ {
		hasCreated := false
		for _, cate := range cates {
			// 如果已经创建过，跳过
			if createdCates[cate.BusinessID] {
				continue
			}

			// 判断是否为根分类：ParentID 为 0
			isRootCate := cate.ParentID == 0

			var newParentID uint64
			if !isRootCate {
				// 如果不是根分类，尝试将旧的 ParentID 转换为新的 PrimaryID
				convertedID, err := config.IDMappingConfig.ConvertToPrimaryID(ctx, moduleType, cast.ToString(cate.ParentID))
				if err != nil {
					// 父分类还没有创建，跳过本次，等待下一轮
					continue
				}
				newParentID = convertedID
			}

			// 更新 ParentID（根分类保持为 0，子分类使用转换后的新 ID）
			cate.ParentID = newParentID

			// 检查分类是否已存在（通过 BusinessID）
			existingCate, err := l.dao.DescribeCateByBusinessID(ctx, t, cate.BusinessID, config.CorpPrimaryID, config.KbPrimaryID)
			if err == nil && existingCate != nil {
				// 分类已存在，更新ID映射后跳过
				if oldID, exists := oldIDMap[cate.BusinessID]; exists {
					config.IDMappingConfig.SetMappedID(moduleType, oldID, kb_package.MappedID{
						PrimaryID: existingCate.ID,
						BizID:     cast.ToString(cate.BusinessID),
					})
				}
				createdCates[cate.BusinessID] = true
				hasCreated = true
				continue
			}

			// 创建分类
			primaryID, err := l.dao.CreateCate(ctx, t, cate)
			if err != nil {
				logx.E(ctx, "batchCreateCates create cate failed, isRoot: %v, err: %v", isRootCate, err)
				return err
			}

			// 更新ID映射：将旧的CategoryId映射到新的自增ID
			if oldID, exists := oldIDMap[cate.BusinessID]; exists {
				config.IDMappingConfig.SetMappedID(moduleType, oldID, kb_package.MappedID{
					PrimaryID: primaryID,
					BizID:     cast.ToString(cate.BusinessID),
				})
			}

			createdCates[cate.BusinessID] = true
			hasCreated = true
		}

		// 如果本轮没有创建任何分类，说明所有分类都已创建完成
		if !hasCreated {
			break
		}
	}

	// 检查是否所有分类都已创建
	if len(createdCates) != len(cates) {
		logx.E(ctx, "batchCreateCates not all cates created, created: %d, total: %d", len(createdCates), len(cates))
		return fmt.Errorf("部分分类创建失败，可能存在循环依赖或父分类不存在")
	}

	return nil
}
