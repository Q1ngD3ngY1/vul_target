package qa

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
)

const (
	qaDataFileName            = "qa.jsonl"
	qaSimilarQuestionFileName = "similar_qa.jsonl"
	qaAttributeLabelFileName  = "qa_attribute_label.jsonl"
	qaExportBatchSize         = 200
	similarQuestionBatchSize  = 200
	attributeLabelBatchSize   = 200
	docBatchSize              = 200
	segmentBatchSize          = 200
	categoryBatchSize         = 200
	timeFormat                = "2006-01-02 15:04:05"
)

// qaDataExport 导出的QA数据结构 (t_doc_qa) - 使用大驼峰命名
type qaDataExport struct {
	QaId          string `json:"QaId"`
	OriginDocId   string `json:"OriginDocId"`
	DocId         string `json:"DocId"`
	SegmentId     string `json:"SegmentId"`
	CategoryId    string `json:"CategoryId"`
	Source        uint32 `json:"Source"`
	Question      string `json:"Question"`
	Answer        string `json:"Answer"`
	CustomParam   string `json:"CustomParam"`
	QuestionDesc  string `json:"QuestionDesc"`
	IsAuditFree   uint32 `json:"IsAuditFree"` // 0或1
	Message       string `json:"Message"`
	SimilarStatus uint32 `json:"SimilarStatus"`
	AcceptStatus  uint32 `json:"AcceptStatus"`
	AttrRange     uint32 `json:"AttrRange"`
	ExpireStart   string `json:"ExpireStart"` // 格式: "1970-01-01 08:00:00"
	ExpireEnd     string `json:"ExpireEnd"`   // 格式: "1970-01-01 08:00:00"
	AttributeFlag uint64 `json:"AttributeFlag"`
	EnableScope   uint32 `json:"EnableScope"`
}

// qaSimilarQuestionExport 导出的相似问题结构 (t_qa_similar_question) - 使用大驼峰命名
type qaSimilarQuestionExport struct {
	SimilarId   string `json:"SimilarId"`
	RelatedQaId string `json:"RelatedQaId"`
	Source      uint32 `json:"Source"`
	Question    string `json:"Question"`
	Message     string `json:"Message"`
	IsAuditFree uint32 `json:"IsAuditFree"` // 0或1
}

// qaAttributeLabelExport 导出的QA属性标签结构 (t_qa_attribute_label) - 使用大驼峰命名
type qaAttributeLabelExport struct {
	QaId    string `json:"QaId"`
	Source  uint32 `json:"Source"`
	AttrId  string `json:"KnowledgeItemLabelId"`
	LabelId string `json:"KnowledgeItemLabelValueId"`
}

// ExportQAs 导出指定知识库的所有QA数据
// 使用游标分页遍历，利用索引 idx_corp_robot (corp_id, robot_id, is_deleted)
// 所有QA数据导出到一个qa.jsonl文件中
// idsCollector: 用于收集导出过程中的依赖 IDs
func (l *Logic) ExportQAs(ctx context.Context, config *kb_package.ExportConfig, idsCollector *kb_package.KbIdsCollector) error {
	logx.I(ctx, "ExportQAs start, corpPrimaryID: %d, kbPrimaryID: %d, kbID: %d, localPath: %s",
		config.CorpPrimaryID, config.KbPrimaryID, config.KbID, config.LocalPath)

	// 确保导出目录存在
	if err := os.MkdirAll(config.LocalPath, 0755); err != nil {
		logx.E(ctx, "ExportQAs mkdir failed, err: %v", err)
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// 创建qa.jsonl文件
	qaFilePath := filepath.Join(config.LocalPath, qaDataFileName)
	qaFile, err := os.Create(qaFilePath)
	if err != nil {
		logx.E(ctx, "ExportQAs create qa file failed, err: %v", err)
		return fmt.Errorf("create qa file failed: %w", err)
	}
	defer qaFile.Close()
	qaWriter := jsonlx.NewWriter[qaDataExport](qaFile)

	// 创建similar_qa.jsonl文件
	similarQAFilePath := filepath.Join(config.LocalPath, qaSimilarQuestionFileName)
	similarQAFile, err := os.Create(similarQAFilePath)
	if err != nil {
		logx.E(ctx, "ExportQAs create similar qa file failed, err: %v", err)
		return fmt.Errorf("create similar qa file failed: %w", err)
	}
	defer similarQAFile.Close()
	similarQAWriter := jsonlx.NewWriter[qaSimilarQuestionExport](similarQAFile)

	// 创建qa_attribute_label.jsonl文件
	attrLabelFilePath := filepath.Join(config.LocalPath, qaAttributeLabelFileName)
	attrLabelFile, err := os.Create(attrLabelFilePath)
	if err != nil {
		logx.E(ctx, "ExportQAs create attribute label file failed, err: %v", err)
		return fmt.Errorf("create attribute label file failed: %w", err)
	}
	defer attrLabelFile.Close()
	attrLabelWriter := jsonlx.NewWriter[qaAttributeLabelExport](attrLabelFile)

	// 1. 导出QA主数据
	totalQACount, qaPrimaryIDToBizIDMap, docBizIDs, segmentBizIDs, categoryBizIDs, err := l.exportQAData(ctx, config, qaWriter)
	if err != nil {
		return fmt.Errorf("export qa data failed: %w", err)
	}

	// 2. 导出相似问题（独立查询，不依赖QA）
	totalSimilarCount, err := l.exportSimilarQuestions(ctx, config, qaPrimaryIDToBizIDMap, similarQAWriter)
	if err != nil {
		return fmt.Errorf("export similar questions failed: %w", err)
	}

	// 3. 导出属性标签（独立查询，不依赖QA）
	totalAttrLabelCount, attrBizIDs, labelBizIDs, err := l.exportAttributeLabels(ctx, config, qaPrimaryIDToBizIDMap, attrLabelWriter)
	if err != nil {
		return fmt.Errorf("export attribute labels failed: %w", err)
	}

	// 4. 将收集的 IDs 添加到收集器中
	l.collectQAExportIDs(idsCollector, docBizIDs, segmentBizIDs, categoryBizIDs, attrBizIDs, labelBizIDs)

	logx.I(ctx, "ExportQAs done, totalQA: %d, totalSimilar: %d, totalAttrLabel: %d",
		totalQACount, totalSimilarCount, totalAttrLabelCount)
	return nil
}

// collectQAExportIDs 将 QA 导出过程中收集的 IDs 添加到收集器中
func (l *Logic) collectQAExportIDs(idsCollector *kb_package.KbIdsCollector,
	docBizIDs, segmentBizIDs, categoryBizIDs, attrBizIDs, labelBizIDs map[uint64]struct{}) {

	// 添加 KbDoc IDs
	docIDItems := make([]string, 0, len(docBizIDs))
	for bizID := range docBizIDs {
		docIDItems = append(docIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbDocs(docIDItems)

	// 添加 KbSegment IDs
	segmentIDItems := make([]string, 0, len(segmentBizIDs))
	for bizID := range segmentBizIDs {
		segmentIDItems = append(segmentIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbSegments(segmentIDItems)

	// 添加 KbQaCategory IDs
	categoryIDItems := make([]string, 0, len(categoryBizIDs))
	for bizID := range categoryBizIDs {
		categoryIDItems = append(categoryIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbQaCategories(categoryIDItems)

	// 添加 KbLabel IDs (Attribute)
	attrIDItems := make([]string, 0, len(attrBizIDs))
	for bizID := range attrBizIDs {
		attrIDItems = append(attrIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbLabels(attrIDItems)

	// 添加 KbLabelValue IDs (AttributeLabel)
	labelIDItems := make([]string, 0, len(labelBizIDs))
	for bizID := range labelBizIDs {
		labelIDItems = append(labelIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbLabelValues(labelIDItems)
}

// writeQAData 写入单条QA数据
func (l *Logic) writeQAData(ctx context.Context, qa *qaEntity.DocQA,
	docIDToBizIDMap, segmentIDToBizIDMap, categoryIDToBizIDMap map[uint64]uint64,
	writer *jsonlx.Writer[qaDataExport]) error {
	// 布尔值转整数
	var isAuditFree uint32 = 0
	if qa.IsAuditFree {
		isAuditFree = 1
	}

	// 获取业务ID（允许为0）
	originDocBizID := uint64(0)
	if qa.OriginDocID != 0 {
		if bizID, ok := docIDToBizIDMap[qa.OriginDocID]; ok {
			originDocBizID = bizID
		} else {
			logx.W(ctx, "writeQAData origin doc not found, originDocID: %d", qa.OriginDocID)
		}
	}

	docBizID := uint64(0)
	if qa.DocID != 0 {
		if bizID, ok := docIDToBizIDMap[qa.DocID]; ok {
			docBizID = bizID
		} else {
			logx.W(ctx, "writeQAData doc not found, docID: %d", qa.DocID)
		}
	}

	segmentBizID := uint64(0)
	if qa.SegmentID != 0 {
		if bizID, ok := segmentIDToBizIDMap[qa.SegmentID]; ok {
			segmentBizID = bizID
		} else {
			logx.W(ctx, "writeQAData segment not found, segmentID: %d", qa.SegmentID)
		}
	}

	categoryBizID := uint64(0)
	if qa.CategoryID != 0 {
		if bizID, ok := categoryIDToBizIDMap[qa.CategoryID]; ok {
			categoryBizID = bizID
		} else {
			logx.W(ctx, "writeQAData category not found, categoryID: %d", qa.CategoryID)
		}
	}

	exportData := qaDataExport{
		QaId:          fmt.Sprintf("%d", qa.BusinessID),
		OriginDocId:   fmt.Sprintf("%d", originDocBizID),
		DocId:         fmt.Sprintf("%d", docBizID),
		SegmentId:     fmt.Sprintf("%d", segmentBizID),
		CategoryId:    fmt.Sprintf("%d", categoryBizID),
		Source:        qa.Source,
		Question:      qa.Question,
		Answer:        qa.Answer,
		CustomParam:   qa.CustomParam,
		QuestionDesc:  qa.QuestionDesc,
		IsAuditFree:   isAuditFree,
		Message:       qa.Message,
		SimilarStatus: qa.SimilarStatus,
		AcceptStatus:  qa.AcceptStatus,
		AttrRange:     qa.AttrRange,
		ExpireStart:   formatTime(qa.ExpireStart),
		ExpireEnd:     formatTime(qa.ExpireEnd),
		AttributeFlag: qa.AttributeFlag,
		EnableScope:   qa.EnableScope,
	}

	return writer.Write(exportData)
}

// isQAExportable 判断QA是否允许导出
// 规则：
// 1. ReleaseStatus 必须是：待发布(Init)、发布中(Ing)、已发布(Success)、发布失败(Fail)
func (l *Logic) isQAExportable(qa *qaEntity.DocQA) bool {
	// 检查 ReleaseStatus 是否在允许导出的状态列表中
	allowedStatuses := []uint32{
		qaEntity.QAReleaseStatusInit,
		qaEntity.QAReleaseStatusIng,
		qaEntity.QAReleaseStatusSuccess,
		qaEntity.QAReleaseStatusFail,
	}
	for _, status := range allowedStatuses {
		if qa.ReleaseStatus == status {
			return true
		}
	}
	return false
}

// exportQAData 导出所有QA主数据，返回总数、主键ID到业务ID的映射、以及所有关联的业务ID集合
func (l *Logic) exportQAData(ctx context.Context, config *kb_package.ExportConfig,
	writer *jsonlx.Writer[qaDataExport]) (int, map[uint64]uint64, map[uint64]struct{}, map[uint64]struct{}, map[uint64]struct{}, error) {

	logx.I(ctx, "exportQAData start")

	// 第一步：收集所有QA数据和关联ID
	var allQAs []*qaEntity.DocQA
	qaPrimaryIDToBizIDMap := make(map[uint64]uint64)
	docIDSet := make(map[uint64]struct{})
	segmentIDSet := make(map[uint64]struct{})
	categoryIDSet := make(map[uint64]struct{})
	var lastID uint64 = 0

	logx.I(ctx, "exportQAData step1: collecting QA data")
	for {
		// 使用游标方式获取QA，利用索引 idx_corp_robot
		qas, err := l.qaDao.GetQAsByCursor(ctx, config.CorpPrimaryID, config.KbPrimaryID, lastID, qaExportBatchSize)
		if err != nil {
			logx.E(ctx, "exportQAData GetQAsByCursor failed, err: %v", err)
			return 0, nil, nil, nil, nil, err
		}

		if len(qas) == 0 {
			break
		}
		// 收集数据和ID
		for _, qa := range qas {
			// 判断QA是否允许导出
			if !l.isQAExportable(qa) {
				logx.I(ctx, "exportQAData skip QA, releaseStatus: %d, acceptStatus: %d, qa_id: %d",
					qa.ReleaseStatus, qa.AcceptStatus, qa.ID)
				continue
			}
			allQAs = append(allQAs, qa)
			qaPrimaryIDToBizIDMap[qa.ID] = qa.BusinessID

			// 收集非零的关联ID
			if qa.OriginDocID != 0 {
				docIDSet[qa.OriginDocID] = struct{}{}
			}
			if qa.DocID != 0 {
				docIDSet[qa.DocID] = struct{}{}
			}
			if qa.SegmentID != 0 {
				segmentIDSet[qa.SegmentID] = struct{}{}
			}
			if qa.CategoryID != 0 {
				categoryIDSet[qa.CategoryID] = struct{}{}
			}
		}

		// 更新游标为最后一条记录的 id
		lastQA := qas[len(qas)-1]
		lastID = lastQA.ID

		logx.D(ctx, "exportQAData collecting progress, lastID: %d, batch: %d",
			lastID, len(qas))

		// 如果返回的记录数少于请求的数量，说明已经没有更多数据了
		if len(qas) < qaExportBatchSize {
			break
		}
	}

	logx.I(ctx, "exportQAData collected %d QAs, %d docs, %d segments, %d categories",
		len(allQAs), len(docIDSet), len(segmentIDSet), len(categoryIDSet))

	// 第二步：批量查询关联实体的业务ID
	docIDToBizIDMap, err := l.batchGetDocBusinessIDs(ctx, config.KbPrimaryID, docIDSet)
	if err != nil {
		return 0, nil, nil, nil, nil, fmt.Errorf("batch get doc business ids failed: %w", err)
	}
	logx.I(ctx, "exportQAData got %d doc business ids", len(docIDToBizIDMap))

	segmentIDToBizIDMap, err := l.batchGetSegmentBusinessIDs(ctx, config.KbID, segmentIDSet)
	if err != nil {
		return 0, nil, nil, nil, nil, fmt.Errorf("batch get segment business ids failed: %w", err)
	}
	logx.I(ctx, "exportQAData got %d segment business ids", len(segmentIDToBizIDMap))

	categoryIDToBizIDMap, err := l.batchGetCategoryBusinessIDs(ctx, config.CorpPrimaryID, config.KbPrimaryID, categoryIDSet)
	if err != nil {
		return 0, nil, nil, nil, nil, fmt.Errorf("batch get category business ids failed: %w", err)
	}
	logx.I(ctx, "exportQAData got %d category business ids", len(categoryIDToBizIDMap))

	// 收集所有业务ID（用于metadata）
	docBizIDSet := make(map[uint64]struct{})
	for _, bizID := range docIDToBizIDMap {
		if bizID != 0 {
			docBizIDSet[bizID] = struct{}{}
		}
	}

	segmentBizIDSet := make(map[uint64]struct{})
	for _, bizID := range segmentIDToBizIDMap {
		if bizID != 0 {
			segmentBizIDSet[bizID] = struct{}{}
		}
	}

	categoryBizIDSet := make(map[uint64]struct{})
	for _, bizID := range categoryIDToBizIDMap {
		if bizID != 0 {
			categoryBizIDSet[bizID] = struct{}{}
		}
	}

	// 第三步：导出数据
	totalCount := 0
	for _, qa := range allQAs {
		if err := l.writeQAData(ctx, qa, docIDToBizIDMap, segmentIDToBizIDMap, categoryIDToBizIDMap, writer); err != nil {
			logx.E(ctx, "exportQAData write qa data failed, qaBizID: %d, err: %v", qa.BusinessID, err)
			return 0, nil, nil, nil, nil, fmt.Errorf("write qa data failed, qaBizID: %d, err: %w", qa.BusinessID, err)
		}
		totalCount++
	}

	logx.I(ctx, "exportQAData done, total: %d", totalCount)
	return totalCount, qaPrimaryIDToBizIDMap, docBizIDSet, segmentBizIDSet, categoryBizIDSet, nil
}

// exportSimilarQuestions 导出相似问
func (l *Logic) exportSimilarQuestions(ctx context.Context, config *kb_package.ExportConfig,
	qaPrimaryIDToBizIDMap map[uint64]uint64, writer *jsonlx.Writer[qaSimilarQuestionExport]) (int, error) {

	logx.I(ctx, "exportSimilarQuestions start")

	totalCount := 0
	page := uint32(1)
	pageSize := uint32(similarQuestionBatchSize)

	for {
		filter := &qaEntity.SimilarityQuestionReq{
			CorpId:    config.CorpPrimaryID,
			RobotId:   config.KbPrimaryID,
			IsDeleted: qaEntity.QAIsNotDeleted,
			Page:      page,
			PageSize:  pageSize,
		}

		similarQuestions, err := l.qaDao.ListSimilarQuestions(ctx, qaEntity.SimilarQuestionTblColList, filter)
		if err != nil {
			logx.E(ctx, "exportSimilarQuestions query failed, err: %v", err)
			return 0, err
		}

		if len(similarQuestions) == 0 {
			break
		}

		// 写入相似问题数据
		for _, sq := range similarQuestions {
			// 从映射中获取QA业务ID
			qaBizID, ok := qaPrimaryIDToBizIDMap[sq.RelatedQAID]
			if !ok {
				// 如果找不到对应的QA（可能QA已被删除），跳过
				logx.W(ctx, "exportSimilarQuestions qa not found, relatedQAID: %d, similarID: %d",
					sq.RelatedQAID, sq.SimilarID)
				continue
			}

			// 布尔值转整数
			var isAuditFree uint32 = 0
			if sq.IsAuditFree {
				isAuditFree = 1
			}

			exportData := qaSimilarQuestionExport{
				SimilarId:   fmt.Sprintf("%d", sq.SimilarID),
				RelatedQaId: fmt.Sprintf("%d", qaBizID), // 使用业务ID
				Source:      sq.Source,
				Question:    sq.Question,
				Message:     sq.Message,
				IsAuditFree: isAuditFree,
			}

			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportSimilarQuestions write failed, err: %v", err)
				return 0, err
			}

			totalCount++
		}

		logx.I(ctx, "exportSimilarQuestions progress, page: %d, batch: %d, total: %d",
			page, len(similarQuestions), totalCount)

		// 如果返回的记录数少于请求的数量，说明已经没有更多数据了
		if len(similarQuestions) < int(pageSize) {
			break
		}

		page++
	}

	logx.I(ctx, "exportSimilarQuestions done, total: %d", totalCount)
	return totalCount, nil
}

// exportAttributeLabels 独立导出所有属性标签（不依赖QA查询），返回总数和所有关联的业务ID集合
func (l *Logic) exportAttributeLabels(ctx context.Context, config *kb_package.ExportConfig,
	qaPrimaryIDToBizIDMap map[uint64]uint64, writer *jsonlx.Writer[qaAttributeLabelExport]) (int, map[uint64]struct{}, map[uint64]struct{}, error) {

	logx.I(ctx, "exportAttributeLabels start")

	// 1. 先查询所有QA属性标签，收集用到的AttrID和LabelID
	var allAttributeLabels []*labelEntity.QAAttributeLabel
	attrIDSet := make(map[uint64]struct{})
	labelIDSet := make(map[uint64]struct{})
	var lastID uint64 = 0

	logx.I(ctx, "exportAttributeLabels step1: collecting attribute labels")
	for {
		// 直接根据robotID使用游标分页查询所有QA属性标签
		attributeLabels, err := l.labelDao.GetQAAttributeLabelForExport(ctx, config.KbPrimaryID, lastID, qaExportBatchSize)
		if err != nil {
			logx.E(ctx, "exportAttributeLabels query failed, err: %v", err)
			return 0, nil, nil, err
		}

		if len(attributeLabels) == 0 {
			break
		}

		// 收集数据和ID
		for _, al := range attributeLabels {
			// 只收集有效的QA（在映射中存在的）
			if _, ok := qaPrimaryIDToBizIDMap[al.QAID]; ok {
				allAttributeLabels = append(allAttributeLabels, al)
				attrIDSet[al.AttrID] = struct{}{}
				labelIDSet[al.LabelID] = struct{}{}
			}
		}

		// 更新游标为最后一条记录的ID
		lastLabel := attributeLabels[len(attributeLabels)-1]
		lastID = lastLabel.ID

		logx.D(ctx, "exportAttributeLabels collecting progress, lastID: %d, batch: %d",
			lastID, len(attributeLabels))

		// 如果返回的记录数少于请求的数量，说明已经没有更多数据了
		if len(attributeLabels) < qaExportBatchSize {
			break
		}
	}

	logx.I(ctx, "exportAttributeLabels collected %d labels, %d unique attrs, %d unique labels",
		len(allAttributeLabels), len(attrIDSet), len(labelIDSet))

	// 2. 批量查询用到的 Attribute 业务ID
	attrIDToBizIDMap, err := l.batchGetAttributeBusinessIDs(ctx, config.KbPrimaryID, attrIDSet)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("batch get attribute business ids failed: %w", err)
	}
	logx.I(ctx, "exportAttributeLabels got %d attribute business ids", len(attrIDToBizIDMap))

	// 3. 批量查询用到的 AttributeLabel 业务ID
	labelIDToBizIDMap, err := l.batchGetAttributeLabelBusinessIDs(ctx, config.KbPrimaryID, labelIDSet)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("batch get attribute label business ids failed: %w", err)
	}
	logx.I(ctx, "exportAttributeLabels got %d label business ids", len(labelIDToBizIDMap))

	// 收集所有业务ID（用于metadata）
	attrBizIDSet := make(map[uint64]struct{})
	for _, bizID := range attrIDToBizIDMap {
		if bizID != 0 {
			attrBizIDSet[bizID] = struct{}{}
		}
	}

	labelBizIDSet := make(map[uint64]struct{})
	for _, bizID := range labelIDToBizIDMap {
		if bizID != 0 {
			labelBizIDSet[bizID] = struct{}{}
		}
	}

	// 4. 导出数据
	totalCount := 0
	for _, al := range allAttributeLabels {
		// 从映射中获取QA业务ID
		qaBizID, ok := qaPrimaryIDToBizIDMap[al.QAID]
		if !ok {
			logx.W(ctx, "exportAttributeLabels qa not found, qaID: %d", al.QAID)
			continue
		}

		// 从映射中获取 Attribute 业务ID
		attrBizID, ok := attrIDToBizIDMap[al.AttrID]
		if !ok {
			logx.W(ctx, "exportAttributeLabels attribute not found, attrID: %d", al.AttrID)
			continue
		}

		// 从映射中获取 AttributeLabel 业务ID
		labelBizID, ok := labelIDToBizIDMap[al.LabelID]
		if !ok {
			logx.W(ctx, "exportAttributeLabels label not found, labelID: %d", al.LabelID)
			continue
		}

		exportData := qaAttributeLabelExport{
			QaId:    fmt.Sprintf("%d", qaBizID), // 使用QA业务ID
			Source:  al.Source,
			AttrId:  fmt.Sprintf("%d", attrBizID),  // 使用Attribute业务ID
			LabelId: fmt.Sprintf("%d", labelBizID), // 使用AttributeLabel业务ID
		}

		if err := writer.Write(exportData); err != nil {
			logx.E(ctx, "exportAttributeLabels write failed, err: %v", err)
			return 0, nil, nil, err
		}

		totalCount++
	}

	logx.I(ctx, "exportAttributeLabels done, total: %d", totalCount)
	return totalCount, attrBizIDSet, labelBizIDSet, nil
}

// batchGetAttributeBusinessIDs 批量查询Attribute的业务ID
func (l *Logic) batchGetAttributeBusinessIDs(ctx context.Context, robotID uint64, attrIDSet map[uint64]struct{}) (map[uint64]uint64, error) {
	if len(attrIDSet) == 0 {
		return make(map[uint64]uint64), nil
	}

	// 将set转换为slice
	attrIDs := make([]uint64, 0, len(attrIDSet))
	for id := range attrIDSet {
		attrIDs = append(attrIDs, id)
	}

	logx.I(ctx, "batchGetAttributeBusinessIDs querying %d attributes", len(attrIDs))

	// 使用GetAttributeByIDs批量查询（该方法内部已实现分批）
	attrMap, err := l.labelDao.GetAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		logx.E(ctx, "batchGetAttributeBusinessIDs query failed, err: %v", err)
		return nil, fmt.Errorf("query attributes failed: %w", err)
	}

	// 构建主键ID到业务ID的映射
	attrIDToBizIDMap := make(map[uint64]uint64, len(attrMap))
	for id, attr := range attrMap {
		attrIDToBizIDMap[id] = attr.BusinessID
	}

	logx.I(ctx, "batchGetAttributeBusinessIDs done, total: %d", len(attrIDToBizIDMap))
	return attrIDToBizIDMap, nil
}

// batchGetAttributeLabelBusinessIDs 批量查询AttributeLabel的业务ID
func (l *Logic) batchGetAttributeLabelBusinessIDs(ctx context.Context, robotID uint64, labelIDSet map[uint64]struct{}) (map[uint64]uint64, error) {
	if len(labelIDSet) == 0 {
		return make(map[uint64]uint64), nil
	}

	// 将set转换为slice
	labelIDs := make([]uint64, 0, len(labelIDSet))
	for id := range labelIDSet {
		labelIDs = append(labelIDs, id)
	}

	logx.I(ctx, "batchGetAttributeLabelBusinessIDs querying %d labels", len(labelIDs))

	labelIDToBizIDMap := make(map[uint64]uint64)

	// 分批查询（避免IN查询参数过多）
	for i := 0; i < len(labelIDs); i += attributeLabelBatchSize {
		end := i + attributeLabelBatchSize
		if end > len(labelIDs) {
			end = len(labelIDs)
		}
		batch := labelIDs[i:end]

		// 使用GetAttributeLabelByIDs批量查询（返回map[主键ID]*AttributeLabel）
		labelMap, err := l.labelDao.GetAttributeLabelByIDs(ctx, batch, robotID)
		if err != nil {
			logx.E(ctx, "batchGetAttributeLabelBusinessIDs query failed, batch: %d-%d, err: %v", i, end, err)
			return nil, fmt.Errorf("query attribute labels failed: %w", err)
		}

		// 构建主键ID到业务ID的映射
		for id, label := range labelMap {
			labelIDToBizIDMap[id] = label.BusinessID
		}

		logx.D(ctx, "batchGetAttributeLabelBusinessIDs batch %d-%d done, got %d labels", i, end, len(labelMap))
	}

	logx.I(ctx, "batchGetAttributeLabelBusinessIDs done, total: %d", len(labelIDToBizIDMap))
	labelIDToBizIDMap[0] = 0 // 补充默认的全部标签
	return labelIDToBizIDMap, nil
}

// batchGetDocBusinessIDs 批量查询Doc的业务ID
func (l *Logic) batchGetDocBusinessIDs(ctx context.Context, robotID uint64, docIDSet map[uint64]struct{}) (map[uint64]uint64, error) {
	if len(docIDSet) == 0 {
		return make(map[uint64]uint64), nil
	}

	// 将set转换为slice
	docIDs := make([]uint64, 0, len(docIDSet))
	for id := range docIDSet {
		docIDs = append(docIDs, id)
	}

	logx.I(ctx, "batchGetDocBusinessIDs querying %d docs", len(docIDs))

	docIDToBizIDMap := make(map[uint64]uint64)

	// 分批查询（避免IN查询参数过多）
	for i := 0; i < len(docIDs); i += docBatchSize {
		end := i + docBatchSize
		if end > len(docIDs) {
			end = len(docIDs)
		}
		batch := docIDs[i:end]

		// 使用GetDocByIDs批量查询
		docMap, err := l.docDao.GetDocByIDs(ctx, batch, robotID)
		if err != nil {
			logx.E(ctx, "batchGetDocBusinessIDs query failed, batch: %d-%d, err: %v", i, end, err)
			return nil, fmt.Errorf("query docs failed: %w", err)
		}

		// 构建主键ID到业务ID的映射
		for id, doc := range docMap {
			docIDToBizIDMap[id] = doc.BusinessID
		}

		logx.D(ctx, "batchGetDocBusinessIDs batch %d-%d done, got %d docs", i, end, len(docMap))
	}

	logx.I(ctx, "batchGetDocBusinessIDs done, total: %d", len(docIDToBizIDMap))
	return docIDToBizIDMap, nil
}

// batchGetSegmentBusinessIDs 批量查询Segment的业务ID
func (l *Logic) batchGetSegmentBusinessIDs(ctx context.Context, appBizId uint64, segmentIDSet map[uint64]struct{}) (map[uint64]uint64, error) {
	if len(segmentIDSet) == 0 {
		return make(map[uint64]uint64), nil
	}

	// 将set转换为slice
	segmentIDs := make([]uint64, 0, len(segmentIDSet))
	for id := range segmentIDSet {
		segmentIDs = append(segmentIDs, id)
	}

	logx.I(ctx, "batchGetSegmentBusinessIDs querying %d segments", len(segmentIDs))

	segmentIDToBizIDMap := make(map[uint64]uint64)
	db, err := knowClient.GormClient(ctx, model.TableNameTDocSegment, 0, appBizId, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}
	// 分批查询（避免IN查询参数过多）
	for i := 0; i < len(segmentIDs); i += segmentBatchSize {
		end := i + segmentBatchSize
		if end > len(segmentIDs) {
			end = len(segmentIDs)
		}
		batch := segmentIDs[i:end]

		deleteFlag := segEntity.SegmentIsNotDeleted
		filter := &segEntity.DocSegmentFilter{
			IDs:       batch,
			IsDeleted: &deleteFlag,
		}
		selectColumns := []string{segEntity.DocSegmentTblColID, segEntity.DocSegmentTblColBusinessID}
		segments, err := l.segDao.GetDocSegmentListWithTx(ctx, selectColumns, filter, db)
		if err != nil {
			logx.E(ctx, "batchGetSegmentBusinessIDs query failed, batch: %d-%d, err: %v", i, end, err)
			return nil, fmt.Errorf("query segments failed: %w", err)
		}

		// 构建主键ID到业务ID的映射
		for _, seg := range segments {
			segmentIDToBizIDMap[seg.ID] = seg.BusinessID
		}

		logx.D(ctx, "batchGetSegmentBusinessIDs batch %d-%d done, got %d segments", i, end, len(segments))
	}

	logx.I(ctx, "batchGetSegmentBusinessIDs done, total: %d", len(segmentIDToBizIDMap))
	return segmentIDToBizIDMap, nil
}

// batchGetCategoryBusinessIDs 批量查询Category的业务ID
func (l *Logic) batchGetCategoryBusinessIDs(ctx context.Context, corpID, robotID uint64, categoryIDSet map[uint64]struct{}) (map[uint64]uint64, error) {
	if len(categoryIDSet) == 0 {
		return make(map[uint64]uint64), nil
	}

	// 将set转换为slice
	categoryIDs := make([]uint64, 0, len(categoryIDSet))
	for id := range categoryIDSet {
		categoryIDs = append(categoryIDs, id)
	}

	logx.I(ctx, "batchGetCategoryBusinessIDs querying %d categories", len(categoryIDs))

	categoryIDToBizIDMap := make(map[uint64]uint64)

	// 分批查询（避免IN查询参数过多）
	for i := 0; i < len(categoryIDs); i += categoryBatchSize {
		end := i + categoryBatchSize
		if end > len(categoryIDs) {
			end = len(categoryIDs)
		}
		batch := categoryIDs[i:end]

		// 使用DescribeCateListByBusinessIDs批量查询
		// 注意：这个方法接收的是业务ID，但我们需要通过主键ID查询
		// 所以需要使用DescribeCateByIDs方法
		cateMap, err := l.cateDao.DescribeCateByIDs(ctx, cateEntity.QACate, batch)
		if err != nil {
			logx.E(ctx, "batchGetCategoryBusinessIDs query failed, batch: %d-%d, err: %v", i, end, err)
			return nil, fmt.Errorf("query categories failed: %w", err)
		}

		// 构建主键ID到业务ID的映射
		for id, cate := range cateMap {
			categoryIDToBizIDMap[id] = cate.BusinessID
		}

		logx.D(ctx, "batchGetCategoryBusinessIDs batch %d-%d done, got %d categories", i, end, len(cateMap))
	}

	logx.I(ctx, "batchGetCategoryBusinessIDs done, total: %d", len(categoryIDToBizIDMap))
	return categoryIDToBizIDMap, nil
}

// formatTime 格式化时间为字符串 "2006-01-02 15:04:05"
func formatTime(t time.Time) string {
	return t.Format(timeFormat)
}
