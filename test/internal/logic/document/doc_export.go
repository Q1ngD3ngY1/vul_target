package document

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/config"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	pb "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

const (
	metadataFileName          = "document.json"
	segmentFileName           = "segment.jsonl"
	segmentImageFileName      = "segment_image.jsonl"
	segmentOrgDataFileName    = "segment_org_data.jsonl"
	segmentPageInfoFileName   = "segment_page_info.jsonl"
	bigDataFileName           = "big_data.jsonl"
	docAttributeLabelFileName = "doc_label.jsonl"
	assetDir                  = "asset"
	assetImageDir             = "image"
	assetDocumentDir          = "document"
	docExportBatchSize        = 200
	bigDataBatchSize          = 100 // 批量获取 BigData 的批次大小

	// DocExportDirPrefix 导出的单个文档子目录前缀
	DocExportDirPrefix = "document_"
)

// ExportDocuments 导出指定知识库的所有文档数据
// 使用游标分页遍历，利用索引 idx_corp_robot_opt (corp_id, robot_id, is_deleted, opt)
// idsCollector: 用于收集导出过程中的依赖 IDs (KbLabel, KbLabelValue, KbDocCategory)
func (l *Logic) ExportDocuments(ctx context.Context, exportConfig *kb_package.ExportConfig, idsCollector *kb_package.KbIdsCollector) error {
	logx.I(ctx, "ExportDocuments start, corpPrimaryID: %d, kbPrimaryID: %d, kbID: %d, localPath: %s",
		exportConfig.CorpPrimaryID, exportConfig.KbPrimaryID, exportConfig.KbID, exportConfig.LocalPath)

	// 确保导出目录存在 (在LocalPath下创建doc子目录)
	if err := os.MkdirAll(exportConfig.LocalPath, 0755); err != nil {
		logx.E(ctx, "ExportDocuments mkdir doc dir failed, err: %v", err)
		return fmt.Errorf("mkdir doc dir failed: %w", err)
	}

	// 用于收集文档依赖的 IDs（需要并发安全）
	var mu sync.Mutex
	var docCategoryBizIDs []uint64
	var labelBizIDs []uint64
	var labelValueBizIDs []uint64

	// 获取并发数配置
	concurrency := config.DescribeExportDocConcurrency()
	logx.I(ctx, "ExportDocuments using concurrency: %d", concurrency)

	// 使用游标分页遍历所有未删除的文档
	// 游标: lastID，排序: ORDER BY id ASC
	var lastID uint64 = 0
	totalCount := 0

	for {
		// 使用游标方式获取文档，利用索引 idx_corp_robot_opt
		docs, err := l.docDao.GetDocsByCursor(ctx, exportConfig.CorpPrimaryID, exportConfig.KbPrimaryID, lastID, docExportBatchSize)
		if err != nil {
			logx.E(ctx, "ExportDocuments GetDocsByCursor failed, err: %v", err)
			return err
		}

		if len(docs) == 0 {
			break
		}

		// 使用并发处理每批文档
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, concurrency) // 控制并发数
		errChan := make(chan error, len(docs))        // 收集错误

		// 遍历每个文档，并发调用单文档导出
		for _, doc := range docs {
			// 跳过导入问答等使用的批量导入的 Excel 文档（不支持导出）
			if doc.IsBatchImport() && doc.IsExcel() {
				logx.I(ctx, "ExportDocuments skip batch import excel doc, docBizID: %d, fileName: %s", doc.BusinessID, doc.FileName)
				continue
			}
			// 文档状态只有在待发布，发布中，和已发布支持导出
			if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleasing && doc.Status != docEntity.DocStatusReleaseSuccess {
				logx.I(ctx, "ExportDocuments skip doc, docBizID: %d, fileName: %s, status: %d", doc.BusinessID, doc.FileName, doc.Status)
				continue
			}
			wg.Add(1)
			go func(d *docEntity.Doc) {
				defer wg.Done()

				// 获取信号量
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				// 导出单个文档
				docCateBizID, docLabelBizIDs, docLabelValueBizIDs, err := l.ExportDocument(ctx, exportConfig, d)
				if err != nil {
					logx.E(ctx, "ExportDocuments export doc failed, docBizID: %d, fileName: %s, err: %v", d.BusinessID, d.FileName, err)
					errChan <- fmt.Errorf("export doc failed, docBizID: %d, fileName: %s, err: %w", d.BusinessID, d.FileName, err)
					return
				}

				// 合并收集的 IDs（需要加锁保护）
				mu.Lock()
				if docCateBizID != 0 {
					docCategoryBizIDs = append(docCategoryBizIDs, docCateBizID)
				}
				labelBizIDs = append(labelBizIDs, docLabelBizIDs...)
				labelValueBizIDs = append(labelValueBizIDs, docLabelValueBizIDs...)
				mu.Unlock()
			}(doc)
		}

		// 等待当前批次所有文档处理完成
		wg.Wait()
		close(errChan)

		// 检查是否有错误
		if len(errChan) > 0 {
			err := <-errChan
			return err
		}

		totalCount += len(docs)

		// 更新游标为最后一条记录的 id
		lastDoc := docs[len(docs)-1]
		lastID = lastDoc.ID

		logx.I(ctx, "ExportDocuments progress, lastID: %d, batch: %d, total: %d",
			lastID, len(docs), totalCount)

		// 如果返回的记录数少于请求的数量，说明已经没有更多数据了
		if len(docs) < docExportBatchSize {
			break
		}
	}

	// 去重并将收集的 IDs 添加到收集器中
	docCategoryBizIDs = slicex.Unique(docCategoryBizIDs)
	labelBizIDs = slicex.Unique(labelBizIDs)
	labelValueBizIDs = slicex.Unique(labelValueBizIDs)
	l.collectDocExportIDs(idsCollector, docCategoryBizIDs, labelBizIDs, labelValueBizIDs)

	logx.I(ctx, "ExportDocuments done, total: %d, docCategories: %d, labels: %d, labelValues: %d",
		totalCount, len(docCategoryBizIDs), len(labelBizIDs), len(labelValueBizIDs))
	return nil
}

// collectDocExportIDs 将文档导出过程中收集的 IDs 添加到收集器中
func (l *Logic) collectDocExportIDs(idsCollector *kb_package.KbIdsCollector,
	docCategoryBizIDs, labelBizIDs, labelValueBizIDs []uint64) {

	// 添加 KbDocCategory IDs
	docCategoryIDItems := make([]string, 0, len(docCategoryBizIDs))
	for _, bizID := range docCategoryBizIDs {
		docCategoryIDItems = append(docCategoryIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbDocCategories(docCategoryIDItems)

	// 添加 KbLabel IDs
	labelIDItems := make([]string, 0, len(labelBizIDs))
	for _, bizID := range labelBizIDs {
		labelIDItems = append(labelIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbLabels(labelIDItems)

	// 添加 KbLabelValue IDs
	labelValueIDItems := make([]string, 0, len(labelValueBizIDs))
	for _, bizID := range labelValueBizIDs {
		labelValueIDItems = append(labelValueIDItems, fmt.Sprintf("%d", bizID))
	}
	idsCollector.AddKbLabelValues(labelValueIDItems)
}

// docMetadataExport 导出的文档元数据结构 (t_doc)
type docMetadataExport struct {
	DocId                   string
	FileName                string
	FileType                string
	FileSize                int64
	IsRefer                 bool
	ReferUrlType            uint32
	Source                  uint32
	WebUrl                  string
	KnowledgeItemLabelRange uint32 // AttrRange -> KnowledgeItemLabelRange
	CharSize                int64
	ExpireStart             time.Time
	ExpireEnd               time.Time
	CategoryId              string // 分类的 BusinessID
	OriginalUrl             string
	CustomerKnowledgeId     string
	KnowledgeItemLabelFlag  int64
	IsDownloadable          bool
	UpdatePeriodH           uint32
	NextUpdateTime          time.Time
	SplitRule               string
	EnableScope             uint32
	Opt                     uint32 // 文档操作类型，用于判断是否批量导入
	AssetPath               string // 原始文件在 asset 中的相对路径，如 asset/document/10144502298158725050
}

// docSegmentExport 导出的文档切片结构 (t_doc_segment)
type docSegmentExport struct {
	SegmentId       string
	FileType        string
	SegmentType     string
	Title           string
	PageContent     string
	OrgDataId       string
	BigDataId       string
	BigStartIndex   int32
	BigEndIndex     int32
	Outputs         string
	SplitModel      string
	Type            int32
	BatchId         int32
	RichTextIndex   int32
	StartChunkIndex int32
	EndChunkIndex   int32
	LinkerKeep      bool
}

// docSegmentImageExport 导出的文档切片图片结构 (t_doc_segment_image)
type docSegmentImageExport struct {
	ImageId   string
	SegmentId string // 切片ID
	AssetPath string // 图片在 asset 中的相对路径，如 asset/image/33bc22816bf69cab79c8646cfb64267a-image.png
	UrlParam  string // URL 参数，如 ?size=min|135.7*90.7|0.08
}

// docSegmentOrgDataExport 导出的文档切片原始数据结构 (t_doc_segment_org_data)
type docSegmentOrgDataExport struct {
	OrgDataId          string
	OrgData            string // 原始数据
	OrgPageNumbers     string // 原始内容对应的页码
	SheetData          string // sheet_data
	SegmentType        string // 段落类型
	AddMethod          uint32 // 添加方式
	IsTemporaryDeleted bool   // 是否临时删除
	IsDisabled         bool   // 是否停用
	SheetName          string // 表格sheet名称
}

// bigDataExport 导出的 big_data 结构
type bigDataExport struct {
	BigDataId string // BigData的ID
	BigStart  int32  // BigData 分片起始索引
	BigEnd    int32  // BigData 分片结束索引
	BigString string // BigData的内容
}

// docSegmentPageInfoExport 导出的文档切片页码信息结构 (t_doc_segment_page_info)
type docSegmentPageInfoExport struct {
	PageInfoId     string
	SegmentId      string // 切片 BusinessID
	OrgPageNumbers string // 页码信息（json存储）
	BigPageNumbers string // 页码信息（json存储）
	SheetData      string // sheet信息（json）
}

// docAttributeLabelExport 导出的文档关联标签结构 (t_doc_attribute_label)
// 一个文档可能关联多个标签key，或者关联一个标签key的多个value值
type docAttributeLabelExport struct {
	DocId                     string // 文档 BusinessID
	Source                    uint32 // 来源，1：知识标签
	KnowledgeItemLabelID      string // 知识标签 BusinessID (t_attribute 表的 business_id)
	KnowledgeItemLabelValueID string // 知识标签值 BusinessID (t_attribute_label 表的 business_id)
}

// ExportDocument 导出单个文档到本地文件夹
// config: 导出配置
// doc: 文档实体
// 返回值:
//   - categoryBizID: 文档分类的 BusinessID（0 表示无分类）
//   - labelBizIDs: 知识标签的 BusinessID 列表
//   - labelValueBizIDs: 知识标签值的 BusinessID 列表
//   - error: 错误信息
func (l *Logic) ExportDocument(ctx context.Context, config *kb_package.ExportConfig, doc *docEntity.Doc) (
	categoryBizID uint64, labelBizIDs, labelValueBizIDs []uint64, err error) {

	if doc == nil {
		return 0, nil, nil, fmt.Errorf("doc is nil")
	}
	start := time.Now()
	docBizID := doc.BusinessID
	docPrimaryID := doc.ID
	logx.I(ctx, "ExportDocument start, kbPrimaryID: %d, kbID: %d, docPrimaryID: %d, docBizID: %d, fileName: %s",
		config.KbPrimaryID, config.KbID, docPrimaryID, docBizID, doc.FileName)

	// 1. 创建导出目录（先删除旧目录）
	exportDir := filepath.Join(config.LocalPath, fmt.Sprintf("%s%d", DocExportDirPrefix, docBizID))
	if err := os.RemoveAll(exportDir); err != nil {
		logx.W(ctx, "ExportDocument remove old dir %v failed, err: %v", exportDir, err)
	}
	if err := os.MkdirAll(exportDir, 0755); err != nil {
		logx.E(ctx, "ExportDocument mkdir failed, err: %v", err)
		return 0, nil, nil, fmt.Errorf("mkdir failed: %w", err)
	}

	// 2. 创建 asset 目录结构（image 和 document 子目录）
	assetDirPath := filepath.Join(exportDir, assetDir)
	assetImageDirPath := filepath.Join(assetDirPath, assetImageDir)
	assetDocumentDirPath := filepath.Join(assetDirPath, assetDocumentDir)
	if err := os.MkdirAll(assetImageDirPath, 0755); err != nil {
		logx.E(ctx, "ExportDocument mkdir asset/image dir failed, err: %v", err)
		return 0, nil, nil, fmt.Errorf("mkdir asset/image dir failed: %w", err)
	}
	if err := os.MkdirAll(assetDocumentDirPath, 0755); err != nil {
		logx.E(ctx, "ExportDocument mkdir asset/document dir failed, err: %v", err)
		return 0, nil, nil, fmt.Errorf("mkdir asset/document dir failed: %w", err)
	}

	// 3. 下载原始文件到 asset/document 目录，使用原始文件名
	docAssetPath, err := l.downloadFileFromCOS(ctx, doc.CosURL, assetDocumentDirPath, assetDocumentDir, doc.FileName)
	if err != nil {
		logx.E(ctx, "ExportDocument download original file failed, err: %v", err)
		return 0, nil, nil, err
	}

	// 4. 查询分类的 BusinessID
	if doc.CategoryID != 0 {
		cateInfo, err := l.cateDao.DescribeCateByID(ctx, cateEntity.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
		if err != nil {
			logx.W(ctx, "ExportDocument get category failed, categoryID: %d, err: %v", doc.CategoryID, err)
			return 0, nil, nil, err
		}
		categoryBizID = cateInfo.BusinessID
	}

	// 5. 导出 metadata.json（包含原始文件的 asset 路径）
	if err := l.exportDocMetadata(ctx, doc, exportDir, docAssetPath, categoryBizID); err != nil {
		return 0, nil, nil, fmt.Errorf("export metadata failed: %w", err)
	}

	// 6. 导出 segment.jsonl 并收集 big_data_id
	segmentIDToBizID, bigDataIDSet, err := l.exportDocSegments(ctx, config.KbPrimaryID, config.KbID, docPrimaryID, exportDir)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("export segments failed: %w", err)
	}

	// 7. 导出 segment_image.jsonl 并下载图片到 asset/image 目录
	if err := l.exportDocSegmentImages(ctx, config.KbPrimaryID, config.KbID, docPrimaryID, exportDir, assetImageDirPath, segmentIDToBizID); err != nil {
		return 0, nil, nil, fmt.Errorf("export segment images failed: %w", err)
	}

	// 8. 导出 segment_org_data.jsonl
	if err := l.exportDocSegmentOrgData(ctx, config.CorpBizID, config.KbID, docBizID, exportDir); err != nil {
		return 0, nil, nil, fmt.Errorf("export segment org data failed: %w", err)
	}

	// 9. 导出 segment_page_info.jsonl
	if err := l.exportDocSegmentPageInfo(ctx, config.KbPrimaryID, config.KbID, docPrimaryID, exportDir, segmentIDToBizID); err != nil {
		return 0, nil, nil, fmt.Errorf("export segment page info failed: %w", err)
	}

	// 10. 导出 big_data.jsonl（从 kb-retrieval 获取实际数据）
	if err := l.exportBigData(ctx, config.KbPrimaryID, bigDataIDSet, exportDir); err != nil {
		return 0, nil, nil, fmt.Errorf("export big data failed: %w", err)
	}

	// 11. 导出 doc_attribute_label.jsonl（文档关联的标签），并返回 label 和 labelValue 的 BusinessID
	labelBizIDs, labelValueBizIDs, err = l.exportDocAttributeLabels(ctx, config.KbPrimaryID, docPrimaryID, docBizID, exportDir)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("export doc attribute labels failed: %w", err)
	}

	logx.I(ctx, "ExportDocument success, cost:%d ms, kbPrimaryID: %d, docPrimaryID: %d, exportDir: %s", time.Since(start).Milliseconds(), config.KbPrimaryID, docPrimaryID, exportDir)
	return categoryBizID, labelBizIDs, labelValueBizIDs, nil
}

// exportDocMetadata 导出文档元数据到 metadata.json
func (l *Logic) exportDocMetadata(ctx context.Context, doc *docEntity.Doc, exportDir, docAssetPath string, categoryBizID uint64) error {
	logx.I(ctx, "exportDocMetadata start, docId: %d", doc.BusinessID)

	metadata := docMetadataExport{
		DocId:                   fmt.Sprintf("%d", doc.BusinessID),
		FileName:                doc.FileName,
		FileType:                doc.FileType,
		FileSize:                int64(doc.FileSize),
		IsRefer:                 doc.IsRefer,
		ReferUrlType:            doc.ReferURLType,
		Source:                  doc.Source,
		WebUrl:                  doc.WebURL,
		KnowledgeItemLabelRange: doc.AttrRange,
		CharSize:                int64(doc.CharSize),
		ExpireStart:             doc.ExpireStart,
		ExpireEnd:               doc.ExpireEnd,
		CategoryId:              fmt.Sprintf("%d", categoryBizID),
		OriginalUrl:             doc.OriginalURL,
		CustomerKnowledgeId:     doc.CustomerKnowledgeId,
		KnowledgeItemLabelFlag:  int64(doc.AttributeFlag),
		IsDownloadable:          doc.IsDownloadable,
		UpdatePeriodH:           doc.UpdatePeriodH,
		NextUpdateTime:          doc.NextUpdateTime,
		SplitRule:               doc.SplitRule,
		EnableScope:             doc.EnableScope,
		Opt:                     doc.Opt,
		AssetPath:               docAssetPath,
	}

	filePath := filepath.Join(exportDir, metadataFileName)
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocMetadata create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		logx.E(ctx, "exportDocMetadata encode failed, err: %v", err)
		return err
	}

	logx.I(ctx, "exportDocMetadata done, file: %s", filePath)
	return nil
}

// downloadFileFromCOS 从 COS 下载文件
// cosURL: COS 文件的 URL
// dirPath: 本地保存目录
// assetSubDir: asset 子目录名（如 "image" 或 "document"）
// fileName: 指定的文件名
// 返回文件在 asset 中的相对路径，如 asset/document/产品说明书.pdf
func (l *Logic) downloadFileFromCOS(ctx context.Context, cosURL, dirPath, assetSubDir, fileName string) (string, error) {
	if cosURL == "" {
		logx.I(ctx, "downloadFileFromCOS skip, cosURL is empty")
		return "", nil
	}

	// 使用 s3x 下载文件
	// 从 cosURL 中提取 key（路径部分）
	u, err := url.Parse(cosURL)
	if err != nil {
		return "", fmt.Errorf("parse url failed: %w", err)
	}
	key := u.Path
	if key == "" {
		return "", fmt.Errorf("key is empty")
	}

	// 使用指定文件名，使用 filepath.Base 提取纯文件名，防止路径遍历攻击
	fileName = filepath.Base(fileName)

	// 构建本地文件路径
	filePath := filepath.Join(dirPath, fileName)

	// 使用 GetObjectToFile 直接下载到文件
	if err := l.s3.GetObjectToFile(ctx, key, filePath); err != nil {
		return "", fmt.Errorf("GetObjectToFile failed: %w", err)
	}

	// 返回相对路径
	assetPath := fmt.Sprintf("%s/%s/%s", assetDir, assetSubDir, fileName)
	return assetPath, nil
}

// extractImageExtFromURL 从图片 URL 中提取图片格式
// URL 格式示例:
// https://xxx.cos.xxx/image_files/2026-01-04/33bc22816bf69cab79c8646cfb64267a-image.png?size=min
// https://xxx.cos.xxx/image_files/2025-12-01/ef06d4bafef5869c1df884d8aca8a9d2-image.svg?size=def
func extractImageExtFromURL(imageURL string) string {
	if imageURL == "" {
		return ""
	}

	// 解析 URL，去掉查询参数
	u, err := url.Parse(imageURL)
	if err != nil {
		return ""
	}

	// 获取路径部分的扩展名
	ext := filepath.Ext(u.Path)
	if ext != "" && ext[0] == '.' {
		ext = ext[1:] // 去掉前面的点
	}

	return ext
}

// extractOriginalFileNameFromURL 从原始图片 URL 中提取原始文件名
// URL 格式示例: https://xxx.cos.xxx/image_files/2026-01-04/33bc22816bf69cab79c8646cfb64267a-image.png?size=min
// 返回: 33bc22816bf69cab79c8646cfb64267a-image.png
func extractOriginalFileNameFromURL(imageURL string) string {
	if imageURL == "" {
		return ""
	}

	// 解析 URL，去掉查询参数
	u, err := url.Parse(imageURL)
	if err != nil {
		return ""
	}

	// 获取文件名部分
	return filepath.Base(u.Path)
}

// exportDocSegments 导出文档切片到 segment.jsonl（复用 GetSegmentByDocID）并收集 big_data_id
// 返回 segmentIDToBizID: segment自增ID -> segment业务ID 的映射
// 返回 bigDataIDSet: 收集到的 big_data_id 集合
func (l *Logic) exportDocSegments(ctx context.Context, kbPrimaryID, kbID, docPrimaryID uint64, exportDir string) (map[uint64]uint64, map[string]struct{}, error) {
	logx.I(ctx, "exportDocSegments start, kbPrimaryID: %d, docPrimaryID: %d", kbPrimaryID, docPrimaryID)

	segmentIDToBizID := make(map[uint64]uint64)
	bigDataIDSet := make(map[string]struct{})

	filePath := filepath.Join(exportDir, segmentFileName)
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocSegments create file failed, err: %v", err)
		return nil, nil, err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[docSegmentExport](file)

	// 获取分库分表的 db
	db, err := knowClient.GormClient(ctx, model.TableNameTDocSegment, kbPrimaryID, kbID, client.WithCalleeMethod("ExportSegment"))
	if err != nil {
		logx.E(ctx, "exportDocSegments get GormClient failed, err: %v", err)
		return nil, nil, err
	}

	var lastID uint64 = 0
	totalCount := 0

	for {
		// 复用 GetSegmentByDocID，每次获取 docExportBatchSize 条记录
		segments, newLastID, err := l.segDao.GetSegmentByDocID(ctx, kbPrimaryID, docPrimaryID, lastID, uint64(docExportBatchSize), nil, db)
		if err != nil {
			logx.E(ctx, "exportDocSegments query failed, err: %v", err)
			return nil, nil, err
		}

		if len(segments) == 0 {
			break
		}

		for _, seg := range segments {
			// 构建映射: segment_id -> business_id
			segmentIDToBizID[seg.ID] = seg.BusinessID

			// 收集 big_data_id
			if seg.BigDataID != "" {
				bigDataIDSet[seg.BigDataID] = struct{}{}
			}

			exportData := l.convertSegmentToExport(&seg.DocSegment)

			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportDocSegments write failed, err: %v", err)
				return nil, nil, err
			}

			totalCount++
		}

		logx.I(ctx, "exportDocSegments progress, lastID: %d, batch: %d, total: %d", newLastID, len(segments), totalCount)

		// 更新 lastID
		lastID = newLastID

		// 如果返回的记录数少于请求的数量，说明已经没有更多数据了
		if len(segments) < docExportBatchSize {
			break
		}
	}

	logx.I(ctx, "exportDocSegments done, total: %d, file: %s", totalCount, filePath)
	return segmentIDToBizID, bigDataIDSet, nil
}

// convertSegmentToExport 将 segment entity 转换为导出结构
func (l *Logic) convertSegmentToExport(seg *segEntity.DocSegment) docSegmentExport {
	return docSegmentExport{
		SegmentId:       fmt.Sprintf("%d", seg.BusinessID),
		FileType:        seg.FileType,
		SegmentType:     seg.SegmentType,
		Title:           seg.Title,
		PageContent:     seg.PageContent,
		OrgDataId:       fmt.Sprintf("%d", seg.OrgDataBizID),
		BigDataId:       seg.BigDataID,
		BigStartIndex:   seg.BigStart,
		BigEndIndex:     seg.BigEnd,
		Outputs:         seg.Outputs,
		SplitModel:      seg.SplitModel,
		Type:            int32(seg.Type),
		BatchId:         int32(seg.BatchID),
		RichTextIndex:   int32(seg.RichTextIndex),
		StartChunkIndex: int32(seg.StartChunkIndex),
		EndChunkIndex:   int32(seg.EndChunkIndex),
		LinkerKeep:      seg.LinkerKeep,
	}
}

// exportDocSegmentImages 导出文档切片图片到 segment_image.jsonl 并下载图片到 asset/image 目录
func (l *Logic) exportDocSegmentImages(ctx context.Context, kbPrimaryID, kbID, docPrimaryID uint64, exportDir, assetImageDirPath string, segmentIDToBizID map[uint64]uint64) error {
	logx.I(ctx, "exportDocSegmentImages start, kbPrimaryID: %d, docPrimaryID: %d", kbPrimaryID, docPrimaryID)

	filePath := filepath.Join(exportDir, segmentImageFileName)
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocSegmentImages create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[docSegmentImageExport](file)

	var lastID uint64 = 0
	totalCount := 0

	for {
		images, err := l.segDao.GetDocSegmentImagesByCursor(ctx, kbPrimaryID, kbID, docPrimaryID, lastID, docExportBatchSize)
		if err != nil {
			logx.E(ctx, "exportDocSegmentImages query failed, err: %v", err)
			return err
		}
		if len(images) == 0 {
			break
		}

		for _, img := range images {
			lastID = img.ID

			// 获取 segment_biz_id
			segmentBizID, ok := segmentIDToBizID[img.SegmentID]
			if !ok {
				// 如果图片存在，但是切片不存在，跳过
				logx.E(ctx, "exportDocSegmentImages segment not found, skip, segment_id: %d, image_id: %d", img.SegmentID, img.ImageID)
				continue
			}
			if img.OriginalUrl == "" {
				logx.E(ctx, "exportDocSegmentImages image url empty, skip, imageID: %d", img.ImageID)
				continue
			}

			// 从原始 URL 中提取原始文件名
			originalFileName := extractOriginalFileNameFromURL(img.OriginalUrl)
			if originalFileName == "" {
				logx.E(ctx, "exportDocSegmentImages extract original file name failed, skip, imageID: %d, url: %s", img.ImageID, img.OriginalUrl)
				continue
			}

			// 下载图片到 asset/image 目录，使用原始文件名
			imageAssetPath, err := l.downloadFileFromCOS(ctx, img.OriginalUrl, assetImageDirPath, assetImageDir, originalFileName)
			if err != nil {
				logx.E(ctx, "exportDocSegmentImages download image failed, skip, imageID: %d, err: %v", img.ImageID, err)
				continue
			}

			// 从原始 URL 中提取 URL 参数
			urlParam := extractSizeParamsFromURL(img.OriginalUrl)

			exportData := docSegmentImageExport{
				ImageId:   fmt.Sprintf("%d", img.ImageID),
				SegmentId: fmt.Sprintf("%d", segmentBizID),
				AssetPath: imageAssetPath,
				UrlParam:  urlParam, // 保存 URL 参数
			}

			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportDocSegmentImages write failed, err: %v", err)
				return err
			}

			totalCount++
		}

		logx.I(ctx, "exportDocSegmentImages progress, lastID: %d, batch: %d, total: %d", lastID, len(images), totalCount)
	}

	// 如果没有图片数据，删除空文件
	if totalCount == 0 {
		_ = file.Close()
		_ = os.Remove(filePath)
		logx.I(ctx, "exportDocSegmentImages no images, removed empty file: %s", filePath)
		return nil
	}

	logx.I(ctx, "exportDocSegmentImages done, total: %d, file: %s", totalCount, filePath)
	return nil
}

// exportDocSegmentOrgData 导出文档切片原始数据到 segment_org_data.jsonl
func (l *Logic) exportDocSegmentOrgData(ctx context.Context, corpBizID, kbID, docBizID uint64, exportDir string) error {
	logx.I(ctx, "exportDocSegmentOrgData start, corpBizID: %d, kbID: %d, docBizID: %d", corpBizID, kbID, docBizID)

	filePath := filepath.Join(exportDir, segmentOrgDataFileName)
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocSegmentOrgData create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[docSegmentOrgDataExport](file)

	db, err := knowClient.GormClient(ctx, model.TableNameTDocSegmentOrgDatum, 0, kbID, client.WithCalleeMethod("ExportSegmentOrgData"))
	if err != nil {
		logx.E(ctx, "exportDocSegmentOrgData get GormClient failed, err: %v", err)
		return err
	}

	var lastBusinessID uint64 = 0
	totalCount := 0

	for {
		orgDataList, err := l.segDao.GetDocSegmentOrgDataByCursor(ctx, corpBizID, kbID, docBizID, lastBusinessID, docExportBatchSize, db)
		if err != nil {
			logx.E(ctx, "exportDocSegmentOrgData query failed, err: %v", err)
			return err
		}
		if len(orgDataList) == 0 {
			break
		}

		for _, orgData := range orgDataList {
			exportData := docSegmentOrgDataExport{
				OrgDataId:          fmt.Sprintf("%d", orgData.BusinessID),
				OrgData:            orgData.OrgData,
				OrgPageNumbers:     orgData.OrgPageNumbers,
				SheetData:          orgData.SheetData,
				SegmentType:        orgData.SegmentType,
				AddMethod:          orgData.AddMethod,
				IsTemporaryDeleted: orgData.IsTemporaryDeleted,
				IsDisabled:         orgData.IsDisabled,
				SheetName:          orgData.SheetName,
			}

			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportDocSegmentOrgData write failed, err: %v", err)
				return err
			}

			lastBusinessID = orgData.BusinessID
			totalCount++
		}

		logx.I(ctx, "exportDocSegmentOrgData progress, lastBusinessID: %d, batch: %d, total: %d", lastBusinessID, len(orgDataList), totalCount)
		if len(orgDataList) < docExportBatchSize {
			break
		}
	}

	logx.I(ctx, "exportDocSegmentOrgData done, total: %d, file: %s", totalCount, filePath)
	return nil
}

// exportDocSegmentPageInfo 导出文档切片页码信息到 segment_page_info.jsonl
func (l *Logic) exportDocSegmentPageInfo(ctx context.Context, kbPrimaryID, kbID, docPrimaryID uint64, exportDir string, segmentIDToBizID map[uint64]uint64) error {
	logx.I(ctx, "exportDocSegmentPageInfo start, kbPrimaryID: %d, docPrimaryID: %d", kbPrimaryID, docPrimaryID)

	filePath := filepath.Join(exportDir, segmentPageInfoFileName)
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocSegmentPageInfo create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[docSegmentPageInfoExport](file)

	var lastID uint64 = 0
	totalCount := 0

	for {
		pageInfos, err := l.segDao.GetDocSegmentPageInfosByCursor(ctx, kbPrimaryID, kbID, docPrimaryID, lastID, docExportBatchSize)
		if err != nil {
			logx.E(ctx, "exportDocSegmentPageInfo query failed, err: %v", err)
			return err
		}
		if len(pageInfos) == 0 {
			break
		}

		for _, pageInfo := range pageInfos {
			// 获取 segment_biz_id
			segmentBizID, ok := segmentIDToBizID[pageInfo.SegmentID]
			if !ok {
				logx.E(ctx, "exportDocSegmentPageInfo segment not found, skip, segment_id: %d, page_info_id: %d", pageInfo.SegmentID, pageInfo.PageInfoID)
				continue
			}

			exportData := docSegmentPageInfoExport{
				PageInfoId:     fmt.Sprintf("%d", pageInfo.PageInfoID),
				SegmentId:      fmt.Sprintf("%d", segmentBizID),
				OrgPageNumbers: pageInfo.OrgPageNumbers,
				BigPageNumbers: pageInfo.BigPageNumbers,
				SheetData:      pageInfo.SheetData,
			}

			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportDocSegmentPageInfo write failed, err: %v", err)
				return err
			}

			lastID = pageInfo.ID
			totalCount++
		}

		logx.I(ctx, "exportDocSegmentPageInfo progress, lastID: %d, batch: %d, total: %d", lastID, len(pageInfos), totalCount)
	}

	logx.I(ctx, "exportDocSegmentPageInfo done, total: %d, file: %s", totalCount, filePath)
	return nil
}

// exportBigData 导出 big_data 到 big_data.jsonl（从 kb-retrieval 获取实际数据）
func (l *Logic) exportBigData(ctx context.Context, kbPrimaryID uint64, bigDataIDSet map[string]struct{}, exportDir string) error {
	logx.I(ctx, "exportBigData start, kbPrimaryID: %d, bigDataIDCount: %d", kbPrimaryID, len(bigDataIDSet))

	if len(bigDataIDSet) == 0 {
		logx.W(ctx, "exportBigData skip, no big data to export")
		return nil
	}

	filePath := filepath.Join(exportDir, bigDataFileName)
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportBigData create file failed, err: %v", err)
		return err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[bigDataExport](file)

	// 将 bigDataIDSet 转换为切片
	bigDataIDs := make([]string, 0, len(bigDataIDSet))
	for id := range bigDataIDSet {
		bigDataIDs = append(bigDataIDs, id)
	}

	// 分批获取 BigData 内容
	totalCount := 0
	for _, batchIDs := range slicex.Chunk(bigDataIDs, bigDataBatchSize) {
		// 调用 kb-retrieval 接口获取 BigData 内容
		req := &pb.BatchGetBigDataESByRobotBigDataIDReq{
			RobotId:    kbPrimaryID,
			BigDataIds: batchIDs,
			Type:       pb.KnowledgeType_KNOWLEDGE, // 离线知识库
		}

		rsp, err := l.rpc.RetrievalDirectIndex.BatchGetBigDataESByRobotBigDataID(ctx, req)
		if err != nil {
			logx.E(ctx, "exportBigData BatchGetBigDataESByRobotBigDataID failed, kbPrimaryID: %d, batchIDs: %v, err: %v", kbPrimaryID, batchIDs, err)
			return fmt.Errorf("BatchGetBigDataESByRobotBigDataID failed: %w", err)
		}

		// 构建 bigDataID -> BigData 的映射
		bigDataMap := make(map[string]*pb.BigData)
		for _, data := range rsp.GetData() {
			bigDataMap[data.GetBigDataId()] = data
		}

		// 写入文件
		for _, bigDataID := range batchIDs {
			data, ok := bigDataMap[bigDataID]
			if !ok {
				logx.E(ctx, "exportBigData bigDataID not found in response, skip, bigDataID: %s", bigDataID)
				// 如果找不到，忽略
				continue
			}

			exportData := bigDataExport{
				BigDataId: data.GetBigDataId(),
				BigStart:  data.GetBigStart(),
				BigEnd:    data.GetBigEnd(),
				BigString: data.GetBigString(),
			}

			if err := writer.Write(exportData); err != nil {
				logx.E(ctx, "exportBigData write failed, err: %v", err)
				return err
			}

			totalCount++
		}

		logx.I(ctx, "exportBigData progress, batchSize: %d, total: %d", len(batchIDs), totalCount)
	}

	logx.I(ctx, "exportBigData done, total: %d, file: %s", totalCount, filePath)
	return nil
}

// exportDocAttributeLabels 导出文档关联的标签到 doc_attribute_label.jsonl
// 一个文档可能关联多个标签key（attr），或者关联一个标签key的多个value值（label）
// 返回值:
//   - labelBizIDs: 知识标签的 BusinessID 列表
//   - labelValueBizIDs: 知识标签值的 BusinessID 列表
//   - error: 错误信息
func (l *Logic) exportDocAttributeLabels(ctx context.Context, kbPrimaryID, docPrimaryID, docBizID uint64, exportDir string) (
	labelBizIDs, labelValueBizIDs []uint64, err error) {

	logx.I(ctx, "exportDocAttributeLabels start, kbPrimaryID: %d, docPrimaryID: %d, docBizID: %d", kbPrimaryID, docPrimaryID, docBizID)

	// 1. 获取文档关联的标签列表（使用文档自增ID查询）
	docLabels, err := l.labelDao.GetDocAttributeLabel(ctx, kbPrimaryID, []uint64{docPrimaryID})
	if err != nil {
		logx.E(ctx, "exportDocAttributeLabels GetDocAttributeLabel failed, err: %v", err)
		return nil, nil, err
	}
	if len(docLabels) == 0 {
		logx.I(ctx, "exportDocAttributeLabels skip, no labels for doc, docPrimaryID: %d", docPrimaryID)
		return nil, nil, nil
	}

	// 2. 收集所有需要查询的 attrID 和 labelID（自增ID），并去重
	attrIDs := make([]uint64, 0, len(docLabels))
	labelIDs := make([]uint64, 0, len(docLabels))
	for _, label := range docLabels {
		attrIDs = append(attrIDs, label.AttrID)
		labelIDs = append(labelIDs, label.LabelID)
	}
	attrIDs = slicex.Unique(attrIDs)
	labelIDs = slicex.Unique(labelIDs)

	// 3. 批量获取属性信息（自增ID -> BusinessID 映射）
	attrMap, err := l.labelDao.GetAttributeByIDs(ctx, kbPrimaryID, attrIDs)
	if err != nil {
		logx.E(ctx, "exportDocAttributeLabels GetAttributeByIDs failed, err: %v", err)
		return nil, nil, err
	}

	// 4. 批量获取标签信息（自增ID -> BusinessID 映射）
	labelMap, err := l.labelDao.GetAttributeLabelByIDs(ctx, labelIDs, kbPrimaryID)
	if err != nil {
		logx.E(ctx, "exportDocAttributeLabels GetAttributeLabelByIDs failed, err: %v", err)
		return nil, nil, err
	}

	// 5. 创建导出文件
	filePath := filepath.Join(exportDir, docAttributeLabelFileName)
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "exportDocAttributeLabels create file failed, err: %v", err)
		return nil, nil, err
	}
	defer file.Close()

	writer := jsonlx.NewWriter[docAttributeLabelExport](file)

	// 6. 写入导出数据，并收集 label 和 labelValue 的 BusinessID
	totalCount := 0
	for _, docLabel := range docLabels {
		// 获取属性的 BusinessID
		attr, ok := attrMap[docLabel.AttrID]
		if !ok {
			logx.W(ctx, "exportDocAttributeLabels attr not found, skip, attrID: %d", docLabel.AttrID)
			continue
		}

		labelBizId := uint64(0)
		if docLabel.LabelID != 0 {
			// 非“全部”标签，获取标签的 BusinessID
			label, ok := labelMap[docLabel.LabelID]
			if !ok {
				logx.W(ctx, "exportDocAttributeLabels label not found, skip, labelID: %d", docLabel.LabelID)
				continue
			}
			labelBizId = label.BusinessID
		}

		// 收集 KbLabel (Attribute) BusinessID
		labelBizIDs = append(labelBizIDs, attr.BusinessID)
		// 收集 KbLabelValue (AttributeLabel) BusinessID
		if labelBizId != 0 {
			labelValueBizIDs = append(labelValueBizIDs, labelBizId)
		}

		exportData := docAttributeLabelExport{
			DocId:                     fmt.Sprintf("%d", docBizID),        // 使用文档的 BusinessID
			Source:                    docLabel.Source,                    // 来源
			KnowledgeItemLabelID:      fmt.Sprintf("%d", attr.BusinessID), // 知识标签的 BusinessID
			KnowledgeItemLabelValueID: fmt.Sprintf("%d", labelBizId),      // 知识标签值的 BusinessID
		}

		if err := writer.Write(exportData); err != nil {
			logx.E(ctx, "exportDocAttributeLabels write failed, err: %v", err)
			return nil, nil, err
		}

		totalCount++
	}

	logx.I(ctx, "exportDocAttributeLabels done, total: %d, file: %s", totalCount, filePath)
	return labelBizIDs, labelValueBizIDs, nil
}
