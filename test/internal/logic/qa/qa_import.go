package qa

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	appConfig "git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"github.com/spf13/cast"
)

// ImportQAs 从本地文件导入QA数据到新的知识库
func (l *Logic) ImportQAs(ctx context.Context, config *kb_package.ImportConfig) error {
	// 记录开始时间，用于统计耗时
	startTime := time.Now()
	// LocalPath 已经是 qa 子目录，直接使用
	logx.I(ctx, "ImportQAs start, kbPrimaryID: %d, kbID: %d, localPath: %s",
		config.KbPrimaryID, config.KbID, config.LocalPath)

	// 确保导入目录存在
	if _, err := os.Stat(config.LocalPath); os.IsNotExist(err) {
		logx.I(ctx, "ImportQAs qa directory not exist, skip import, path: %s", config.LocalPath)
		return nil
	}

	// 1. 读取QA主数据文件
	qaFilePath := filepath.Join(config.LocalPath, qaDataFileName)
	qas, err := l.readQADataFile(ctx, qaFilePath)
	if err != nil {
		return fmt.Errorf("read qa data file failed: %w", err)
	}
	if len(qas) == 0 {
		logx.I(ctx, "ImportQAs no qas to import")
		return nil
	}

	// 2. 读取相似问题文件，构建 QA业务ID -> 相似问题列表 的映射
	similarQAFilePath := filepath.Join(config.LocalPath, qaSimilarQuestionFileName)
	similarQuestionsMap, err := l.readSimilarQuestionFile(ctx, similarQAFilePath)
	if err != nil {
		return fmt.Errorf("read similar question file failed: %w", err)
	}

	// 3. 读取属性标签文件，构建 QA业务ID -> 属性标签列表 的映射
	attrLabelFilePath := filepath.Join(config.LocalPath, qaAttributeLabelFileName)
	attributeLabelsMap, err := l.readAttributeLabelFile(ctx, attrLabelFilePath)
	if err != nil {
		return fmt.Errorf("read attribute label file failed: %w", err)
	}

	// 4. 获取App信息（用于获取CorpID等）
	app, err := l.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, config.KbPrimaryID)
	if err != nil {
		logx.E(ctx, "ImportQAs get app failed, err: %v", err)
		return fmt.Errorf("get app failed: %w", err)
	}
	if app == nil {
		logx.E(ctx, "ImportQAs app is nil, kbPrimaryID: %d", config.KbPrimaryID)
		return fmt.Errorf("app not found")
	}

	// 5. 使用并发goroutine导入QA，并收集ID映射
	qaIDMapping := &sync.Map{} // oldBizID -> MappedID，使用sync.Map保证并发安全
	totalCount := 0
	var countMutex sync.Mutex

	// 获取并发数配置，默认为20
	concurrency := appConfig.DescribeImportQAConcurrency()

	// 使用errgroupx进行并发控制
	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(concurrency)

	logx.I(ctx, "ImportQAs start concurrent import with concurrency: %d, total qas: %d", concurrency, len(qas))

	for _, qa := range qas {
		// 保存原始业务ID（用于后续查找相似问题和属性标签）
		oldQaBizIDStr := qa.QaId

		// 转换QA中的主键ID为业务ID
		if err := l.convertQAIDs(ctx, config, qa); err != nil {
			logx.E(ctx, "ImportQAs convert qa ids failed, qaBizID: %s, err: %v", oldQaBizIDStr, err)
			return fmt.Errorf("convert qa ids failed, qaBizID: %s: %w", oldQaBizIDStr, err)
		}

		// 获取该QA的相似问题
		simQuestions := similarQuestionsMap[oldQaBizIDStr]

		// 获取该QA的属性标签，并进行ID映射转换
		attrLabels := attributeLabelsMap[oldQaBizIDStr]
		if err := l.convertAttributeLabelIDs(ctx, config, attrLabels); err != nil {
			logx.E(ctx, "ImportQAs convert attribute label ids failed, qaBizID: %s, err: %v", oldQaBizIDStr, err)
			return fmt.Errorf("convert attribute label ids failed, qaBizID: %s: %w", oldQaBizIDStr, err)
		}

		// 捕获循环变量，避免闭包问题
		qaCopy := qa
		simQuestionsCopy := simQuestions
		attrLabelsCopy := attrLabels
		oldQaBizIDStrCopy := oldQaBizIDStr

		// 并发导入单个QA
		wg.Go(func() error {
			// 导入单个QA
			oldBizIDStr, newPrimaryID, newBizID, err := l.importSingleQA(wgCtx, config, app, qaCopy, simQuestionsCopy, attrLabelsCopy)
			if err != nil {
				logx.E(wgCtx, "ImportQAs import single qa failed, qaBizID: %s, err: %v", oldQaBizIDStrCopy, err)
				return fmt.Errorf("import single qa failed, qaBizID: %s: %w", oldQaBizIDStrCopy, err)
			}

			// 保存ID映射（使用sync.Map保证并发安全）
			qaIDMapping.Store(cast.ToUint64(oldBizIDStr), &kb_package.MappedID{
				PrimaryID: newPrimaryID,
				BizID:     fmt.Sprintf("%d", newBizID),
			})

			// 更新计数（使用互斥锁保证并发安全）
			countMutex.Lock()
			totalCount++
			currentCount := totalCount
			countMutex.Unlock()

			if currentCount%100 == 0 {
				logx.I(wgCtx, "ImportQAs progress: %d/%d", currentCount, len(qas))
			}

			return nil
		})
	}

	// 等待所有goroutine完成
	if err := wg.Wait(); err != nil {
		logx.E(ctx, "ImportQAs concurrent import failed, err: %v", err)
		return fmt.Errorf("concurrent import failed: %w", err)
	}

	// 6. 更新IDMappingConfig中的QA ID映射
	qaIDMapping.Range(func(key, value interface{}) bool {
		oldBizID := key.(uint64)
		mapping := value.(*kb_package.MappedID)
		// 只有在 IDMappingConfig 中已存在的才更新
		if !config.IDMappingConfig.IsMappedIDExist(kb_package.ModuleKbQa, fmt.Sprintf("%d", oldBizID)) {
			config.IDMappingConfig.SetMappedID(kb_package.ModuleKbQa, fmt.Sprintf("%d", oldBizID), *mapping)
		}
		return true
	})

	// 统计映射数量
	mappedCount := 0
	qaIDMapping.Range(func(key, value interface{}) bool {
		mappedCount++
		return true
	})

	// 计算总耗时
	duration := time.Since(startTime)
	logx.I(ctx, "ImportQAs done, total: %d, mapped: %d, duration: %v", totalCount, mappedCount, duration)
	return nil
}

// readQADataFile 读取QA主数据文件
func (l *Logic) readQADataFile(ctx context.Context, filePath string) ([]*qaDataExport, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "readQADataFile file not exist, skip import, filePath: %s", filePath)
			return []*qaDataExport{}, nil
		}
		logx.E(ctx, "readQADataFile open file failed, err: %v", err)
		return nil, err
	}
	defer file.Close()

	reader := jsonlx.NewReader[qaDataExport](file)
	var qas []*qaDataExport

	err = reader.Each(func(qa qaDataExport) error {
		qaCopy := qa
		qas = append(qas, &qaCopy)
		return nil
	})
	if err != nil {
		logx.E(ctx, "readQADataFile read failed, err: %v", err)
		return nil, err
	}

	logx.I(ctx, "readQADataFile done, count: %d", len(qas))
	return qas, nil
}

// readSimilarQuestionFile 读取相似问题文件，返回 QA业务ID -> 相似问题列表 的映射
func (l *Logic) readSimilarQuestionFile(ctx context.Context, filePath string) (map[string][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "readSimilarQuestionFile file not exist, filePath: %s", filePath)
			return make(map[string][]string), nil
		}
		logx.E(ctx, "readSimilarQuestionFile open file failed, err: %v", err)
		return nil, err
	}
	defer file.Close()

	reader := jsonlx.NewReader[qaSimilarQuestionExport](file)
	similarQuestionsMap := make(map[string][]string)

	err = reader.Each(func(sq qaSimilarQuestionExport) error {
		similarQuestionsMap[sq.RelatedQaId] = append(similarQuestionsMap[sq.RelatedQaId], sq.Question)
		return nil
	})
	if err != nil {
		logx.E(ctx, "readSimilarQuestionFile read failed, err: %v", err)
		return nil, err
	}

	logx.I(ctx, "readSimilarQuestionFile done, qa count: %d", len(similarQuestionsMap))
	return similarQuestionsMap, nil
}

// readAttributeLabelFile 读取属性标签文件，返回 QA业务ID -> 属性标签列表 的映射
func (l *Logic) readAttributeLabelFile(ctx context.Context, filePath string) (map[string][]*qaAttributeLabelExport, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "readAttributeLabelFile file not exist, filePath: %s", filePath)
			return make(map[string][]*qaAttributeLabelExport), nil
		}
		logx.E(ctx, "readAttributeLabelFile open file failed, err: %v", err)
		return nil, err
	}
	defer file.Close()

	reader := jsonlx.NewReader[qaAttributeLabelExport](file)
	attributeLabelsMap := make(map[string][]*qaAttributeLabelExport)

	err = reader.Each(func(al qaAttributeLabelExport) error {
		alCopy := al
		attributeLabelsMap[al.QaId] = append(attributeLabelsMap[al.QaId], &alCopy)
		return nil
	})
	if err != nil {
		logx.E(ctx, "readAttributeLabelFile read failed, err: %v", err)
		return nil, err
	}

	logx.I(ctx, "readAttributeLabelFile done, qa count: %d", len(attributeLabelsMap))
	return attributeLabelsMap, nil
}

// convertQAIDs 转换QA中的主键ID为业务ID（避免在importSingleQA中多次RPC查询）
func (l *Logic) convertQAIDs(ctx context.Context, config *kb_package.ImportConfig, qa *qaDataExport) error {
	// 记录转换前的ID
	logx.I(ctx, "convertQAIDs start, qaBizID: %s, before convert - OriginDocId: %s, DocId: %s, SegmentId: %s, CategoryId: %s",
		qa.QaId, qa.OriginDocId, qa.DocId, qa.SegmentId, qa.CategoryId)

	// 转换 OriginDocId（原始文档ID）- 使用主键ID
	originDocPrimaryID, _ := config.IDMappingConfig.ConvertToPrimaryID(ctx, kb_package.ModuleKbDoc, qa.OriginDocId)
	qa.OriginDocId = fmt.Sprintf("%d", originDocPrimaryID)

	// 转换 DocId（文档ID）- 使用业务ID
	newDocBizIdStr, _ := config.IDMappingConfig.ConvertToBizID(ctx, kb_package.ModuleKbDoc, qa.DocId)
	qa.DocId = newDocBizIdStr

	// 转换 SegmentId（段落ID）- 使用主键ID
	segmentPrimaryID, _ := config.IDMappingConfig.ConvertToPrimaryID(ctx, kb_package.ModuleKbSegment, qa.SegmentId)
	qa.SegmentId = fmt.Sprintf("%d", segmentPrimaryID)

	// 转换 CategoryId（分类ID）- 使用业务ID
	newCategoryBizID, _ := config.IDMappingConfig.ConvertToBizID(ctx, kb_package.ModuleKbQaCategory, qa.CategoryId)
	qa.CategoryId = newCategoryBizID

	// 记录转换后的ID
	logx.I(ctx, "convertQAIDs end, qaBizID: %s, after convert - OriginDocId: %s, DocId: %s, SegmentId: %s, CategoryId: %s",
		qa.QaId, qa.OriginDocId, qa.DocId, qa.SegmentId, qa.CategoryId)

	return nil
}

// convertAttributeLabelIDs 转换属性标签中的业务ID为新的主键ID
func (l *Logic) convertAttributeLabelIDs(ctx context.Context, config *kb_package.ImportConfig, attrLabels []*qaAttributeLabelExport) error {
	for _, al := range attrLabels {
		// 转换 AttrId（属性ID）
		if al.AttrId != "" && al.AttrId != "0" {
			attrMappedID := config.IDMappingConfig.GetMappedID(kb_package.ModuleKbLabel, al.AttrId)
			if attrMappedID.PrimaryID == 0 {
				logx.W(ctx, "convertAttributeLabelIDs attr not found in mapping, attrBizID: %s", al.AttrId)
				// 如果找不到映射，保持原值（后续处理时会跳过）
				continue
			}
			al.AttrId = fmt.Sprintf("%d", attrMappedID.PrimaryID)
		}

		// 转换 LabelId（标签值ID）
		if al.LabelId != "" && al.LabelId != "0" {
			labelMappedID := config.IDMappingConfig.GetMappedID(kb_package.ModuleKbLabelValue, al.LabelId)
			if labelMappedID.PrimaryID == 0 {
				logx.W(ctx, "convertAttributeLabelIDs label not found in mapping, labelBizID: %s", al.LabelId)
				// 如果找不到映射，保持原值（后续处理时会跳过）
				continue
			}
			al.LabelId = fmt.Sprintf("%d", labelMappedID.PrimaryID)
		}
	}

	return nil
}

// importSingleQA 导入单个QA，返回旧业务ID和新的主键ID、业务ID
func (l *Logic) importSingleQA(ctx context.Context, config *kb_package.ImportConfig, app *entity.App,
	qa *qaDataExport, simQuestions []string, attrLabels []*qaAttributeLabelExport) (string, uint64, uint64, error) {

	logx.D(ctx, "importSingleQA start, qaBizID: %s", qa.QaId)

	// 保存原始的业务ID（用于后续映射）
	oldQaBizIDStr := qa.QaId

	// 解析时间字段（使用本地时区，避免时区偏移问题）
	expireStart, err := time.ParseInLocation(timeFormat, qa.ExpireStart, time.Local)
	if err != nil {
		logx.W(ctx, "importSingleQA parse expire start failed, use zero time, err: %v", err)
		expireStart = time.Time{}
	}

	expireEnd, err := time.ParseInLocation(timeFormat, qa.ExpireEnd, time.Local)
	if err != nil {
		logx.W(ctx, "importSingleQA parse expire end failed, use zero time, err: %v", err)
		expireEnd = time.Time{}
	}

	// 构建DocQA实体 参考internal/service/doc_qa.go中CreateQA的qaEntity.DocQA中所需要的字段，这里只需要填充需要的字段即可
	docQA := &qaEntity.DocQA{
		BusinessID:   idgen.GetId(),
		StaffID:      config.StaffPrimaryID,
		OriginDocID:  cast.ToUint64(qa.OriginDocId), // OriginDocID使用主键ID（已在convertQAIDs中转换），与CreateQA流程保持一致
		SegmentID:    cast.ToUint64(qa.SegmentId),   // SegmentID使用主键ID（已在convertQAIDs中转换），与CreateQA流程保持一致
		Source:       qa.Source,
		Question:     qa.Question,
		Answer:       qa.Answer,
		CustomParam:  qa.CustomParam,
		QuestionDesc: qa.QuestionDesc,
		AcceptStatus: qa.AcceptStatus,
		AttrRange:    qa.AttrRange,
		ExpireStart:  expireStart,
		ExpireEnd:    expireEnd,
		EnableScope:  gox.IfElse(config.AppBizID == config.KbID, uint32(entity.EnableScopeDev), uint32(entity.EnableScopeAll)), // 共享知识库需要都生效
	}

	// 构建属性标签请求
	var attributeLabelReq *labelEntity.UpdateQAAttributeLabelReq
	if len(attrLabels) > 0 {
		attributeLabelReq = &labelEntity.UpdateQAAttributeLabelReq{
			IsNeedChange:    true,
			AttributeLabels: make([]*labelEntity.QAAttributeLabel, 0, len(attrLabels)),
		}

		for _, al := range attrLabels {
			// 属性标签的ID已经在convertAttributeLabelIDs中转换过了
			// 这里只需要检查ID是否有效（如果转换失败，ID会保持原值，这里跳过）
			attrID := cast.ToUint64(al.AttrId)
			labelID := cast.ToUint64(al.LabelId)
			if attrID == 0 {
				logx.W(ctx, "importSingleQA skip invalid attribute label, attrID: %s, labelID: %s", al.AttrId, al.LabelId)
				continue
			}

			attributeLabelReq.AttributeLabels = append(attributeLabelReq.AttributeLabels, &labelEntity.QAAttributeLabel{
				Source:  al.Source,
				AttrID:  attrID,
				LabelID: labelID,
			})
		}
	}

	// CreateQA需要的是业务ID字符串
	// CategoryId、DocId 已经在convertQAIDs中转换为业务ID，直接使用
	cateBizIdStr := qa.CategoryId
	docBizIdStr := qa.DocId
	logx.D(ctx, "importSingleQA using categoryBizID: %s, docBizID: %s", cateBizIdStr, docBizIdStr)

	// 调用CreateQA函数（businessSource和businessID填0）
	if err := l.CreateQA(ctx, app, docQA, cateBizIdStr, docBizIdStr,
		0, 0, false, attributeLabelReq, simQuestions); err != nil {
		logx.E(ctx, "importSingleQA create qa failed, err: %v", err)
		return "", 0, 0, fmt.Errorf("create qa failed: %w", err)
	}

	logx.D(ctx, "importSingleQA done, oldQaBizID: %s, newQAID: %d, newQABizID: %d", oldQaBizIDStr, docQA.ID, docQA.BusinessID)
	return oldQaBizIDStr, docQA.ID, docQA.BusinessID, nil
}
