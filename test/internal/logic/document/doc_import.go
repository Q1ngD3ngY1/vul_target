package document

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/encodingx/jsonlx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	configx "git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	retrieval_pb "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

const (
	docImportBatchSize = 200 // 导入切片的批次大小
)

// IDMapping ID映射结构
type IDMapping struct {
	PrimaryID uint64 // 自增ID
	BizID     uint64 // 业务ID
}

// docImportMapping 文档导入过程中的临时ID映射
type docImportMapping struct {
	OldDocBizID     string // 旧文档业务ID
	NewDocPrimaryID uint64 // 新文档自增ID
	NewDocBizID     uint64 // 新文档业务ID

	OldOrgDataBizID2New map[uint64]uint64     // 旧OrgDataBizID → 新OrgDataBizID
	OldSegmentID2New    map[uint64]*IDMapping // 旧SegmentBizID → 新Segment映射
	OldBigDataID2New    map[string]string     // 旧BigDataID → 新BigDataID
}

// ImportDocuments 从本地文件导入所有文档数据到新的应用（支持并发）
func (l *Logic) ImportDocuments(ctx context.Context, config *kb_package.ImportConfig) error {
	logx.I(ctx, "ImportDocuments start, kbPrimaryID: %d, kbID: %d, localPath: %s",
		config.KbPrimaryID, config.KbID, config.LocalPath)

	// 遍历导入目录，查找所有 document_xxx 子目录
	entries, err := os.ReadDir(config.LocalPath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "ImportDocuments dir not exist, skip, path: %s", config.LocalPath)
			return nil
		}
		logx.E(ctx, "ImportDocuments read dir failed, err: %v", err)
		return fmt.Errorf("read import dir failed: %w", err)
	}

	// 收集所有需要导入的文档目录
	var docDirs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// 检查是否是文档目录（以 document_ 开头）
		if len(entry.Name()) <= len(DocExportDirPrefix) {
			continue
		}
		if entry.Name()[:len(DocExportDirPrefix)] != DocExportDirPrefix {
			continue
		}
		docDir := filepath.Join(config.LocalPath, entry.Name())
		docDirs = append(docDirs, docDir)
	}

	if len(docDirs) == 0 {
		logx.I(ctx, "ImportDocuments no document dirs found")
		return nil
	}

	totalDocs := len(docDirs)

	// 从配置文件读取并发数
	concurrency := configx.DescribeImportDocConcurrency()
	logx.I(ctx, "ImportDocuments start importing, concurrency: %d, total docs: %d", concurrency, totalDocs)

	// 创建任务通道和结果通道
	docDirChan := make(chan string, totalDocs)
	errChan := make(chan error, totalDocs)
	doneChan := make(chan struct{})

	// 进度计数器（需要加锁保护）
	var progressMutex sync.Mutex
	completedCount := 0
	startTime := time.Now()

	// 将所有文档目录放入通道
	for _, docDir := range docDirs {
		docDirChan <- docDir
	}
	close(docDirChan)

	// 启动并发 worker
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for docDir := range docDirChan {
				// 提取文档名称用于日志
				docName := filepath.Base(docDir)

				if err := l.ImportDocument(ctx, config, docDir); err != nil {
					logx.E(ctx, "ImportDocuments worker %d import doc failed, docDir: %s, err: %v", workerID, docDir, err)
					errChan <- fmt.Errorf("import doc failed, docDir: %s, err: %w", docDir, err)
					return // 遇到错误立即退出
				}

				// 更新进度并打印
				progressMutex.Lock()
				completedCount++
				currentCompleted := completedCount
				progressMutex.Unlock()

				// 计算进度百分比
				percentage := float64(currentCompleted) / float64(totalDocs) * 100
				elapsed := time.Since(startTime)

				logx.I(ctx, "ImportDocuments progress: [%d/%d] %.1f%% | worker %d completed: %s | elapsed: %s",
					currentCompleted, totalDocs, percentage, workerID, docName,
					elapsed.Round(time.Second))
			}
		}(i)
	}

	// 等待所有 worker 完成
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	// 等待完成或错误
	select {
	case err := <-errChan:
		logx.E(ctx, "ImportDocuments failed, completed: %d/%d, elapsed: %s, err: %v",
			completedCount, totalDocs, time.Since(startTime).Round(time.Second), err)
		return err
	case <-doneChan:
		totalElapsed := time.Since(startTime)
		avgTimePerDoc := totalElapsed / time.Duration(totalDocs)
		logx.I(ctx, "ImportDocuments completed successfully! total: %d, elapsed: %s, avg time per doc: %s",
			totalDocs, totalElapsed.Round(time.Second), avgTimePerDoc.Round(time.Millisecond))
		return nil
	}
}

// ImportDocument 导入单个文档
func (l *Logic) ImportDocument(ctx context.Context, config *kb_package.ImportConfig, docDir string) error {
	logx.I(ctx, "ImportDocument start, docDir: %s", docDir)

	// 1. 读取并校验 metadata.json
	metadata, err := l.readDocMetadata(ctx, docDir)
	if err != nil {
		return fmt.Errorf("read metadata failed: %w", err)
	}

	// 3. 获取/生成文档ID
	mapping := &docImportMapping{
		OldDocBizID:         metadata.DocId,
		OldOrgDataBizID2New: make(map[uint64]uint64),
		OldSegmentID2New:    make(map[uint64]*IDMapping),
		OldBigDataID2New:    make(map[string]string),
	}

	// 获取或生成文档ID
	// 将 string 类型的 DocId 转换为 uint64
	oldDocBizID, parseErr := strconv.ParseUint(metadata.DocId, 10, 64)
	if parseErr != nil {
		logx.E(ctx, "ImportDocument parse DocId failed, value: %s, err: %v", metadata.DocId, parseErr)
		return fmt.Errorf("parse DocId failed: %w", parseErr)
	}
	mapping.NewDocBizID = config.IDMappingConfig.GetOrGenerateBizID(kb_package.ModuleKbDoc, oldDocBizID)

	// 4. 上传原始文件到 COS (CRC64 校验)
	cosURL, cosHash, err := l.uploadFileToCOS(ctx, config, docDir, metadata.AssetPath, "doc", metadata.FileType)
	if err != nil {
		return fmt.Errorf("upload doc file to COS failed: %w", err)
	}

	// 5. 创建 t_doc 记录 (状态=13 学习中)
	doc, err := l.createImportDoc(ctx, config, metadata, mapping.NewDocBizID, cosURL, cosHash)
	if err != nil {
		return fmt.Errorf("create doc failed: %w", err)
	}
	mapping.NewDocPrimaryID = doc.ID

	// 6. 导入 segment_org_data.jsonl → t_doc_segment_org_data
	if err := l.importSegmentOrgData(ctx, config, docDir, doc, mapping); err != nil {
		return fmt.Errorf("import segment org data failed: %w", err)
	}

	// 7. 导入 big_data.jsonl → ES (生成新 BigDataID，维护映射)
	if err := l.importBigData(ctx, config, docDir, doc, mapping); err != nil {
		return fmt.Errorf("import big data failed: %w", err)
	}

	// 8. 导入 segment.jsonl → t_doc_segment (使用新的 OrgDataID 和 BigDataID)
	if err := l.importSegments(ctx, config, docDir, doc, mapping); err != nil {
		return fmt.Errorf("import segments failed: %w", err)
	}

	// 9. 导入 segment_image.jsonl → t_doc_segment_image (上传图片到 COS)
	if err := l.importSegmentImages(ctx, config, docDir, doc, mapping); err != nil {
		return fmt.Errorf("import segment images failed: %w", err)
	}

	// 10. 导入 segment_page_info.jsonl → t_doc_segment_page_info
	if err := l.importSegmentPageInfo(ctx, config, docDir, mapping); err != nil {
		return fmt.Errorf("import segment page info failed: %w", err)
	}

	// 11. 导入 doc_label.jsonl → t_doc_attribute_label (从 IDMappingConfig 查找标签ID)
	if err := l.importDocAttributeLabels(ctx, config, docDir, doc, mapping); err != nil {
		return fmt.Errorf("import doc attribute labels failed: %w", err)
	}

	// 12. 更新 IDMappingConfig 中的文档ID映射
	l.updateIDMappingConfig(config, mapping)

	// 13. 触发异步索引任务 (DocImportToIndexTask)
	if err := l.triggerDocImportToIndexTask(ctx, config, doc); err != nil {
		return fmt.Errorf("trigger doc %v import to index task failed: %w", doc.BusinessID, err)
	}

	logx.I(ctx, "ImportDocument success, oldDocBizID: %s, newDocBizID: %d, newDocPrimaryID: %d",
		mapping.OldDocBizID, mapping.NewDocBizID, mapping.NewDocPrimaryID)
	return nil
}

// readDocMetadata 读取文档元数据
func (l *Logic) readDocMetadata(ctx context.Context, docDir string) (*docMetadataExport, error) {
	metadataPath := filepath.Join(docDir, metadataFileName)
	file, err := os.Open(metadataPath)
	if err != nil {
		logx.E(ctx, "readDocMetadata open file failed, path: %s, err: %v", metadataPath, err)
		return nil, err
	}
	defer file.Close()

	var metadata docMetadataExport
	if err := jsonx.NewDecoder(file).Decode(&metadata); err != nil {
		logx.E(ctx, "readDocMetadata decode failed, err: %v", err)
		return nil, err
	}

	return &metadata, nil
}

// uploadFileToCOS 上传文件到 COS（通用方法，支持文档和图片）
// fileType: "doc" 或 "image"
// fileExt: 文件扩展名，用于生成 COS 文件名
// 返回: cosPath, cosHash, error
func (l *Logic) uploadFileToCOS(ctx context.Context, config *kb_package.ImportConfig, docDir, assetPath, fileType, fileExt string) (string, string, error) {
	if assetPath == "" {
		logx.I(ctx, "uploadFileToCOS skip, assetPath is empty")
		return "", "", nil
	}

	localFilePath := filepath.Join(docDir, assetPath)

	// 检查文件是否存在
	if _, err := os.Stat(localFilePath); os.IsNotExist(err) {
		logx.W(ctx, "uploadFileToCOS file not exist, skip, path: %s", localFilePath)
		return "", "", nil
	}

	// 生成 COS 文件名
	fileName := GenerateCOSFileName(fileExt)

	// 根据文件类型获取 COS 路径
	var cosPath string
	if fileType == "image" {
		// 图片路径: /public/{corp_biz_id}/{kb_biz_id}/image/{fileName}
		cosPath = l.s3.GetCorpAppImagePath(ctx, config.CorpBizID, config.KbID, fileName)
	} else {
		// 文档路径: /corp/{corp_biz_id}/{kb_biz_id}/doc/{fileName}
		cosPath = l.s3.GetCorpRobotCOSPath(ctx, config.CorpBizID, config.KbID, fileName)
	}

	// 上传到 COS
	if err := l.s3.PutFile(ctx, localFilePath, cosPath); err != nil {
		logx.E(ctx, "uploadFileToCOS PutFile failed, cosPath: %s, err: %v", cosPath, err)
		return "", "", fmt.Errorf("upload to COS failed: %w", err)
	}

	// 获取上传后的 CosHash
	var cosHash string
	objInfo, err := l.s3.StatObject(ctx, cosPath)
	if err != nil {
		logx.E(ctx, "uploadFileToCOS StatObject failed, cosPath: %s, err: %v", cosPath, err)
		return "", "", fmt.Errorf("stat object failed: %w", err)
	}
	if objInfo != nil {
		cosHash = objInfo.Hash
	}

	logx.I(ctx, "uploadFileToCOS success, localPath: %s, cosPath: %s, cosHash: %s", localFilePath, cosPath, cosHash)
	return cosPath, cosHash, nil
}

// extractSizeParamsFromURL 从 URL 中提取 size 参数
// 输入: https://xxx.cos.xxx/image_files/2025-12-26/be84f9b3315d05d800d26fd2c68d1991-image.png?size=min|135.7*90.7|0.08
// 输出: ?size=min|135.7*90.7|0.08
func extractSizeParamsFromURL(imageURL string) string {
	if imageURL == "" {
		return ""
	}

	// 解析 URL
	u, err := url.Parse(imageURL)
	if err != nil {
		return ""
	}

	// 获取查询参数
	if u.RawQuery != "" {
		return "?" + u.RawQuery
	}

	return ""
}

// createImportDoc 创建导入的文档记录
func (l *Logic) createImportDoc(ctx context.Context, config *kb_package.ImportConfig, metadata *docMetadataExport, newDocBizID uint64, cosURL, cosHash string) (*docEntity.Doc, error) {
	// 将 string 类型的 CategoryId 转换为 uint64
	oldCategoryBizID := uint64(0)
	if metadata.CategoryId != "" && metadata.CategoryId != "0" {
		var parseErr error
		oldCategoryBizID, parseErr = strconv.ParseUint(metadata.CategoryId, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "createImportDoc parse CategoryId failed, value: %s, err: %v", metadata.CategoryId, parseErr)
			return nil, fmt.Errorf("parse CategoryId failed: %w", parseErr)
		}
	}

	// 获取新的分类ID
	newCategoryPrimaryID, err := config.IDMappingConfig.ConvertToPrimaryID(ctx, kb_package.ModuleKbDocCategory, strconv.FormatUint(oldCategoryBizID, 10))
	if err != nil {
		return nil, fmt.Errorf("get new category ID failed: %w", err)
	}

	now := time.Now()
	doc := &docEntity.Doc{
		BusinessID:          newDocBizID,
		RobotID:             config.KbPrimaryID,
		CorpID:              config.CorpPrimaryID,
		StaffID:             config.StaffPrimaryID,
		FileName:            metadata.FileName,
		FileType:            metadata.FileType,
		FileSize:            uint64(metadata.FileSize),
		CosURL:              cosURL,
		Bucket:              l.s3.GetBucket(ctx),
		CosHash:             cosHash,
		Status:              docEntity.DocStatusCreatingIndex, // 学习中状态
		IsRefer:             metadata.IsRefer,
		ReferURLType:        metadata.ReferUrlType,
		Source:              metadata.Source,
		WebURL:              metadata.WebUrl,
		AttrRange:           metadata.KnowledgeItemLabelRange,
		AuditFlag:           docEntity.AuditFlagDone,
		CharSize:            uint64(metadata.CharSize),
		ExpireStart:         metadata.ExpireStart,
		ExpireEnd:           metadata.ExpireEnd,
		Opt:                 metadata.Opt,
		CategoryID:          uint32(newCategoryPrimaryID),
		OriginalURL:         metadata.OriginalUrl,
		CustomerKnowledgeId: metadata.CustomerKnowledgeId,
		AttributeFlag:       uint64(metadata.KnowledgeItemLabelFlag),
		IsDownloadable:      metadata.IsDownloadable,
		UpdatePeriodH:       metadata.UpdatePeriodH,
		NextUpdateTime:      metadata.NextUpdateTime,
		SplitRule:           metadata.SplitRule,
		EnableScope:         gox.IfElse(config.AppBizID == config.KbID, uint32(entity.EnableScopeDev), uint32(entity.EnableScopeAll)), // 共享知识库需要都生效
		CreateTime:          now,
		UpdateTime:          now,
		BatchID:             1,
		IsCreatingIndex:     true,
	}
	if doc.Source == docEntity.SourceFromWeb || doc.Source == docEntity.SourceFromTxDoc { // 不支持网页解析导入，需要手动改成文档
		doc.Source = docEntity.SourceFromFile
	}
	// 初始化 GormClient
	db, err := knowClient.GormClient(ctx, model.TableNameTDoc, config.KbPrimaryID, config.KbID, client.WithCalleeMethod("ImportDoc"))
	if err != nil {
		return nil, fmt.Errorf("get GormClient failed: %w", err)
	}

	// 创建文档记录
	if err := l.docDao.CreateDoc(ctx, doc, db); err != nil {
		logx.E(ctx, "createImportDoc CreateDoc failed, err: %v", err)
		return nil, err
	}
	// 更新应用使用容量
	err = l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
		CharSize:          int64(doc.CharSize),
		StorageCapacity:   gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, 0, int64(doc.FileSize)),
		ComputeCapacity:   int64(doc.FileSize),
		KnowledgeCapacity: int64(doc.FileSize),
	}, doc.RobotID, doc.CorpID)
	if err != nil {
		return nil, fmt.Errorf("UpdateAppCapacityUsage failed: %w", err)
	}

	logx.I(ctx, "createImportDoc success, docID: %d, docBizID: %d", doc.ID, doc.BusinessID)
	return doc, nil
}

// importSegmentOrgData 导入切片原始数据
func (l *Logic) importSegmentOrgData(ctx context.Context, config *kb_package.ImportConfig, docDir string, doc *docEntity.Doc, mapping *docImportMapping) error {
	filePath := filepath.Join(docDir, segmentOrgDataFileName)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importSegmentOrgData file not exist, skip, path: %s", filePath)
			return nil
		}
		return err
	}
	defer file.Close()

	// 初始化 GormClient
	db, err := knowClient.GormClient(ctx, model.TableNameTDocSegmentOrgDatum, 0, config.KbID, client.WithCalleeMethod("ImportSegmentOrgData"))
	if err != nil {
		return fmt.Errorf("get GormClient failed: %w", err)
	}

	reader := jsonlx.NewReader[docSegmentOrgDataExport](file)
	totalCount := 0

	err = reader.Each(func(orgData docSegmentOrgDataExport) error {
		// 将 string 类型的 OrgDataId 转换为 uint64
		oldOrgDataBizID, parseErr := strconv.ParseUint(orgData.OrgDataId, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "importSegmentOrgData parse OrgDataId failed, value: %s, err: %v", orgData.OrgDataId, parseErr)
			return fmt.Errorf("parse OrgDataId failed: %w", parseErr)
		}

		// 生成新的 OrgData 业务ID
		newOrgDataBizID := idgen.GetId()
		mapping.OldOrgDataBizID2New[oldOrgDataBizID] = newOrgDataBizID

		now := time.Now()
		newOrgData := &segEntity.DocSegmentOrgData{
			BusinessID:         newOrgDataBizID,
			CorpBizID:          config.CorpBizID,
			AppBizID:           config.KbID,
			DocBizID:           mapping.NewDocBizID,
			OrgData:            orgData.OrgData,
			OrgPageNumbers:     orgData.OrgPageNumbers,
			SheetData:          orgData.SheetData,
			SegmentType:        orgData.SegmentType,
			AddMethod:          orgData.AddMethod,
			IsTemporaryDeleted: orgData.IsTemporaryDeleted,
			IsDisabled:         orgData.IsDisabled,
			SheetName:          orgData.SheetName,
			CreateTime:         now,
			UpdateTime:         now,
		}
		// 单条创建 OrgData
		if err := l.segDao.CreateDocSegmentOrgData(ctx, newOrgData, db); err != nil {
			return fmt.Errorf("create org data failed: %w", err)
		}
		totalCount++
		return nil
	})
	if err != nil {
		return err
	}

	logx.I(ctx, "importSegmentOrgData done, total: %d", totalCount)
	return nil
}

// importBigData 导入 BigData 到 ES
func (l *Logic) importBigData(ctx context.Context, config *kb_package.ImportConfig, docDir string, doc *docEntity.Doc, mapping *docImportMapping) error {
	filePath := filepath.Join(docDir, bigDataFileName)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importBigData file not exist, skip, path: %s", filePath)
			return nil
		}
		return err
	}
	defer file.Close()

	reader := jsonlx.NewReader[bigDataExport](file)
	var bigDataList []*retrieval_pb.BigData
	totalCount := 0

	err = reader.Each(func(data bigDataExport) error {
		// 生成新的 BigDataID
		newBigDataID := strconv.FormatUint(idgen.GetId(), 10)
		mapping.OldBigDataID2New[data.BigDataId] = newBigDataID

		bigData := &retrieval_pb.BigData{
			RobotId:   config.KbPrimaryID,
			DocId:     doc.ID,
			BigDataId: newBigDataID,
			BigStart:  data.BigStart,
			BigEnd:    data.BigEnd,
			BigString: data.BigString,
		}
		bigDataList = append(bigDataList, bigData)
		totalCount++

		// 分批写入 ES
		if len(bigDataList) >= docImportBatchSize {
			req := &retrieval_pb.AddBigDataElasticReq{
				Data: bigDataList,
				Type: retrieval_pb.KnowledgeType_KNOWLEDGE,
			}
			if err := l.rpc.RetrievalDirectIndex.AddBigDataElastic(ctx, req); err != nil {
				return fmt.Errorf("add big data to ES failed: %w", err)
			}
			bigDataList = bigDataList[:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 写入剩余数据
	if len(bigDataList) > 0 {
		req := &retrieval_pb.AddBigDataElasticReq{
			Data: bigDataList,
			Type: retrieval_pb.KnowledgeType_KNOWLEDGE,
		}
		if err := l.rpc.RetrievalDirectIndex.AddBigDataElastic(ctx, req); err != nil {
			return fmt.Errorf("add big data to ES failed: %w", err)
		}
	}

	logx.I(ctx, "importBigData done, total: %d", totalCount)
	return nil
}

// importSegments 导入切片数据
func (l *Logic) importSegments(ctx context.Context, config *kb_package.ImportConfig, docDir string, doc *docEntity.Doc, mapping *docImportMapping) error {
	filePath := filepath.Join(docDir, segmentFileName)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importSegments file not exist, skip, path: %s", filePath)
			return nil
		}
		return err
	}
	defer file.Close()

	// 初始化 GormClient
	db, err := knowClient.GormClient(ctx, model.TableNameTDocSegment, config.KbPrimaryID, config.KbID, client.WithCalleeMethod("ImportSegments"))
	if err != nil {
		return fmt.Errorf("get GormClient failed: %w", err)
	}

	reader := jsonlx.NewReader[docSegmentExport](file)
	var segmentList []*segEntity.DocSegment
	totalCount := 0
	// 临时映射：新BizID → *IDMapping，用于批量写入后快速更新 PrimaryID
	newBizID2Mapping := make(map[uint64]*IDMapping)

	err = reader.Each(func(seg docSegmentExport) error {
		// 将 string 类型的 SegmentId 转换为 uint64
		oldSegmentBizID, parseErr := strconv.ParseUint(seg.SegmentId, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "importSegments parse SegmentId failed, value: %s, err: %v", seg.SegmentId, parseErr)
			return fmt.Errorf("parse SegmentId failed: %w", parseErr)
		}

		// 获取或生成新的 Segment 业务ID
		newSegmentBizID := config.IDMappingConfig.GetOrGenerateBizID(kb_package.ModuleKbSegment, oldSegmentBizID)
		// 先记录 BizID 映射，PrimaryID 在写入数据库后更新
		idMapping := &IDMapping{BizID: newSegmentBizID}
		mapping.OldSegmentID2New[oldSegmentBizID] = idMapping
		newBizID2Mapping[newSegmentBizID] = idMapping

		// 将 string 类型的 OrgDataId 转换为 uint64
		oldOrgDataBizID := uint64(0)
		if seg.OrgDataId != "" && seg.OrgDataId != "0" {
			oldOrgDataBizID, parseErr = strconv.ParseUint(seg.OrgDataId, 10, 64)
			if parseErr != nil {
				logx.E(ctx, "importSegments parse OrgDataId failed, value: %s, err: %v", seg.OrgDataId, parseErr)
				return fmt.Errorf("parse OrgDataId failed: %w", parseErr)
			}
		}

		// 获取新的 OrgDataBizID
		newOrgDataBizID := uint64(0)
		if oldOrgDataBizID != 0 {
			if newID, ok := mapping.OldOrgDataBizID2New[oldOrgDataBizID]; ok {
				newOrgDataBizID = newID
			} else {
				logx.W(ctx, "importSegments orgDataId mapping not found, oldOrgDataId: %d", oldOrgDataBizID)
			}
		}

		// 获取新的 BigDataID
		newBigDataID := ""
		if seg.BigDataId != "" {
			if newID, ok := mapping.OldBigDataID2New[seg.BigDataId]; ok {
				newBigDataID = newID
			} else {
				logx.W(ctx, "importSegments bigDataId mapping not found, oldBigDataId: %s", seg.BigDataId)
			}
		}

		now := time.Now()
		newSegment := &segEntity.DocSegment{
			BusinessID:      newSegmentBizID,
			RobotID:         config.KbPrimaryID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			DocID:           doc.ID,
			FileType:        seg.FileType,
			SegmentType:     seg.SegmentType,
			Title:           seg.Title,
			PageContent:     seg.PageContent,
			OrgDataBizID:    newOrgDataBizID,
			BigDataID:       newBigDataID,
			BigStart:        seg.BigStartIndex,
			BigEnd:          seg.BigEndIndex,
			Outputs:         seg.Outputs,
			SplitModel:      seg.SplitModel,
			Type:            int(seg.Type),
			BatchID:         int(seg.BatchId),
			RichTextIndex:   int(seg.RichTextIndex),
			StartChunkIndex: int(seg.StartChunkIndex),
			EndChunkIndex:   int(seg.EndChunkIndex),
			LinkerKeep:      seg.LinkerKeep,
			Status:          segEntity.SegmentStatusDone,
			IsDeleted:       segEntity.SegmentIsNotDeleted,
			CreateTime:      now,
			UpdateTime:      now,
		}
		segmentList = append(segmentList, newSegment)
		totalCount++

		// 分批写入
		if len(segmentList) >= docImportBatchSize {
			if err := l.segDao.CreateDocSegments(ctx, segmentList, db); err != nil {
				return fmt.Errorf("batch create segment failed: %w", err)
			}
			// 更新 PrimaryID 映射：通过 newBizID2Mapping 快速查找
			for _, s := range segmentList {
				if m, ok := newBizID2Mapping[s.BusinessID]; ok {
					m.PrimaryID = s.ID
				}
			}
			segmentList = segmentList[:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 写入剩余数据
	if len(segmentList) > 0 {
		if err := l.segDao.CreateDocSegments(ctx, segmentList, db); err != nil {
			return fmt.Errorf("batch create segment failed: %w", err)
		}
		// 更新 PrimaryID 映射：通过 newBizID2Mapping 快速查找
		for _, s := range segmentList {
			if m, ok := newBizID2Mapping[s.BusinessID]; ok {
				m.PrimaryID = s.ID
			}
		}
	}

	logx.I(ctx, "importSegments done, total: %d", totalCount)
	return nil
}

// importSegmentImages 导入切片图片
func (l *Logic) importSegmentImages(ctx context.Context, config *kb_package.ImportConfig, docDir string, doc *docEntity.Doc, mapping *docImportMapping) error {
	filePath := filepath.Join(docDir, segmentImageFileName)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importSegmentImages file not exist, skip, path: %s", filePath)
			return nil
		}
		return err
	}
	defer file.Close()

	// 初始化 GormClient
	db, err := knowClient.GormClient(ctx, model.TableNameTDocSegmentImage, config.KbPrimaryID, config.KbID, client.WithCalleeMethod("ImportSegmentImages"))
	if err != nil {
		return fmt.Errorf("get GormClient failed: %w", err)
	}

	reader := jsonlx.NewReader[docSegmentImageExport](file)
	var imageList []*segEntity.DocSegmentImage
	totalCount := 0

	err = reader.Each(func(img docSegmentImageExport) error {
		// 将 string 类型的 SegmentId 转换为 uint64
		oldSegmentBizID, parseErr := strconv.ParseUint(img.SegmentId, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "importSegmentImages parse SegmentId failed, value: %s, err: %v", img.SegmentId, parseErr)
			return fmt.Errorf("parse SegmentId failed: %w", parseErr)
		}

		// 获取新的 Segment 映射
		segmentMapping, ok := mapping.OldSegmentID2New[oldSegmentBizID]
		if !ok {
			logx.W(ctx, "importSegmentImages segmentId mapping not found, skip, oldSegmentId: %d", oldSegmentBizID)
			return nil
		}

		// 从导出的 AssetPath 中提取原始文件名
		originalFileName := filepath.Base(img.AssetPath)
		if originalFileName == "" {
			logx.E(ctx, "importSegmentImages extract file name failed, skip, assetPath: %s", img.AssetPath)
			return nil
		}

		// 构建新的图片路径：image_files/{当天日期}/{原始文件名}
		today := time.Now().Format("2006-01-02")
		newImagePath := fmt.Sprintf("image_files/%s/%s", today, originalFileName)

		var cosURL string
		var externalURL string
		// 上传图片
		localFilePath := filepath.Join(docDir, img.AssetPath)

		// 检查本地文件是否存在
		if _, err := os.Stat(localFilePath); os.IsNotExist(err) {
			logx.E(ctx, "importSegmentImages local file not exist, skip, path: %s", localFilePath)
			return nil
		}

		// 读取文件内容
		content, err := os.ReadFile(localFilePath)
		if err != nil {
			logx.E(ctx, "importSegmentImages read file failed, path: %s, err: %v", localFilePath, err)
			return err
		}

		// 上传到 COS
		if err := l.s3.PutObject(ctx, content, newImagePath); err != nil {
			logx.E(ctx, "importSegmentImages upload to COS failed, path: %s, err: %v", newImagePath, err)
			return err
		}
		// 构建 URL
		cosURL = fmt.Sprintf("%s/%s", l.s3.GetBucketURL(ctx), newImagePath)
		logx.I(ctx, "importSegmentImages uploaded new image, path: %s", newImagePath)
		// 从导出数据中获取 URL 参数
		if img.UrlParam != "" {
			cosURL += img.UrlParam
		}

		// 生成短链接，保持 size 后缀
		URL, err := url.Parse(cosURL)
		if err != nil {
			logx.E(ctx, "importSegmentImages parse cos url failed, url: %s, err: %v", cosURL, err)
			return err
		}

		storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, l.s3.GetBucket(ctx))
		shortURL, err := l.shortURLCode(ctx, storageTypeKey, URL.Path)
		if err != nil {
			logx.E(ctx, "importSegmentImages generate short url failed, err: %v", err)
			return err
		}

		// 为短链接添加 URL 参数
		if img.UrlParam != "" {
			externalURL = shortURL + img.UrlParam
		} else {
			externalURL = shortURL
		}

		// 生成新的 ImageID
		newImageBizID := idgen.GetId()
		now := time.Now()
		newImage := &segEntity.DocSegmentImage{
			ImageID:     newImageBizID,
			SegmentID:   segmentMapping.PrimaryID,
			DocID:       doc.ID,
			RobotID:     config.KbPrimaryID,
			CorpID:      config.CorpPrimaryID,
			StaffID:     config.StaffPrimaryID,
			OriginalUrl: cosURL,
			ExternalUrl: externalURL,
			CreateTime:  now,
			UpdateTime:  now,
		}
		imageList = append(imageList, newImage)
		totalCount++

		// 分批写入
		if len(imageList) >= docImportBatchSize {
			if err := l.segDao.CreateDocSegmentImages(ctx, imageList, db); err != nil {
				return fmt.Errorf("batch create segment image failed: %w", err)
			}
			imageList = imageList[:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 写入剩余数据
	if len(imageList) > 0 {
		if err := l.segDao.CreateDocSegmentImages(ctx, imageList, db); err != nil {
			return fmt.Errorf("batch create segment image failed: %w", err)
		}
	}

	logx.I(ctx, "importSegmentImages done, total: %d", totalCount)
	return nil
}

// importSegmentPageInfo 导入切片页码信息
func (l *Logic) importSegmentPageInfo(ctx context.Context, config *kb_package.ImportConfig, docDir string, mapping *docImportMapping) error {
	filePath := filepath.Join(docDir, segmentPageInfoFileName)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importSegmentPageInfo file not exist, skip, path: %s", filePath)
			return nil
		}
		return err
	}
	defer file.Close()

	// 初始化 GormClient
	db, err := knowClient.GormClient(ctx, model.TableNameTDocSegmentPageInfo, config.KbPrimaryID, config.KbID, client.WithCalleeMethod("ImportSegmentPageInfo"))
	if err != nil {
		return fmt.Errorf("get GormClient failed: %w", err)
	}

	reader := jsonlx.NewReader[docSegmentPageInfoExport](file)
	var pageInfoList []*segEntity.DocSegmentPageInfo
	totalCount := 0

	err = reader.Each(func(pageInfo docSegmentPageInfoExport) error {
		// 将 string 类型的 SegmentId 转换为 uint64
		oldSegmentBizID, parseErr := strconv.ParseUint(pageInfo.SegmentId, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "importSegmentPageInfo parse SegmentId failed, value: %s, err: %v", pageInfo.SegmentId, parseErr)
			return fmt.Errorf("parse SegmentId failed: %w", parseErr)
		}

		// 获取新的 Segment 映射
		segmentMapping, ok := mapping.OldSegmentID2New[oldSegmentBizID]
		if !ok {
			logx.W(ctx, "importSegmentPageInfo segmentId mapping not found, skip, oldSegmentId: %d", oldSegmentBizID)
			return nil
		}

		// 生成新的 PageInfoID
		newPageInfoBizID := idgen.GetId()
		now := time.Now()
		newPageInfo := &segEntity.DocSegmentPageInfo{
			PageInfoID:     newPageInfoBizID,
			SegmentID:      segmentMapping.PrimaryID,
			OrgPageNumbers: pageInfo.OrgPageNumbers,
			BigPageNumbers: pageInfo.BigPageNumbers,
			SheetData:      pageInfo.SheetData,
			CreateTime:     now,
			UpdateTime:     now,
		}
		pageInfoList = append(pageInfoList, newPageInfo)
		totalCount++

		// 分批写入
		if len(pageInfoList) >= docImportBatchSize {
			if err := l.segDao.CreateDocSegmentPageInfos(ctx, pageInfoList, db); err != nil {
				return fmt.Errorf("batch create segment page info failed: %w", err)
			}
			pageInfoList = pageInfoList[:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 写入剩余数据
	if len(pageInfoList) > 0 {
		if err := l.segDao.CreateDocSegmentPageInfos(ctx, pageInfoList, db); err != nil {
			return fmt.Errorf("batch create segment page info failed: %w", err)
		}
	}

	logx.I(ctx, "importSegmentPageInfo done, total: %d", totalCount)
	return nil
}

// importDocAttributeLabels 导入文档关联的标签
func (l *Logic) importDocAttributeLabels(ctx context.Context, config *kb_package.ImportConfig, docDir string, doc *docEntity.Doc, mapping *docImportMapping) error {
	filePath := filepath.Join(docDir, docAttributeLabelFileName)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logx.I(ctx, "importDocAttributeLabels file not exist, skip, path: %s", filePath)
			return nil
		}
		return err
	}
	defer file.Close()

	// 初始化 GormClient
	db, err := knowClient.GormClient(ctx, model.TableNameTDocAttributeLabel, config.KbPrimaryID, config.KbID, client.WithCalleeMethod("ImportDocAttributeLabels"))
	if err != nil {
		return fmt.Errorf("get GormClient failed: %w", err)
	}

	reader := jsonlx.NewReader[docAttributeLabelExport](file)
	var labelList []*labelEntity.DocAttributeLabel
	totalCount := 0

	err = reader.Each(func(label docAttributeLabelExport) error {
		// 将 string 类型的 KnowledgeItemLabelID 转换为 uint64
		oldAttrBizID, parseErr := strconv.ParseUint(label.KnowledgeItemLabelID, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "importDocAttributeLabels parse KnowledgeItemLabelID failed, value: %s, err: %v", label.KnowledgeItemLabelID, parseErr)
			return fmt.Errorf("parse KnowledgeItemLabelID failed: %w", parseErr)
		}

		// 将 string 类型的 KnowledgeItemLabelValueID 转换为 uint64
		oldLabelBizID, parseErr := strconv.ParseUint(label.KnowledgeItemLabelValueID, 10, 64)
		if parseErr != nil {
			logx.E(ctx, "importDocAttributeLabels parse KnowledgeItemLabelValueID failed, value: %s, err: %v", label.KnowledgeItemLabelValueID, parseErr)
			return fmt.Errorf("parse KnowledgeItemLabelValueID failed: %w", parseErr)
		}

		// 从 IDMappingConfig 获取新的 KnowledgeItemLabelID (AttrID)
		newAttrID, err := config.IDMappingConfig.ConvertToPrimaryID(ctx, kb_package.ModuleKbLabel, strconv.FormatUint(oldAttrBizID, 10))
		if err != nil {
			return fmt.Errorf("get new attr ID failed, oldAttrID: %d, err: %w", oldAttrBizID, err)
		}

		newLabelID := uint64(0)
		if oldLabelBizID != 0 {
			// 非“全部”标签，从 IDMappingConfig 获取新的 KnowledgeItemLabelValueID (LabelID)
			newLabelID, err = config.IDMappingConfig.ConvertToPrimaryID(ctx, kb_package.ModuleKbLabelValue, strconv.FormatUint(oldLabelBizID, 10))
			if err != nil {
				return fmt.Errorf("get new label ID failed, oldLabelID: %d, err: %w", oldLabelBizID, err)
			}
		}
		now := time.Now()
		newLabel := &labelEntity.DocAttributeLabel{
			RobotID:    config.KbPrimaryID,
			DocID:      doc.ID,
			AttrID:     newAttrID,
			LabelID:    newLabelID,
			Source:     label.Source,
			CreateTime: now,
			UpdateTime: now,
		}
		labelList = append(labelList, newLabel)
		totalCount++

		// 分批写入
		if len(labelList) >= docImportBatchSize {
			if err := l.labelDao.CreateDocAttributeLabel(ctx, labelList, db); err != nil {
				return fmt.Errorf("batch create doc attribute label failed: %w", err)
			}
			labelList = labelList[:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 写入剩余数据
	if len(labelList) > 0 {
		if err := l.labelDao.CreateDocAttributeLabel(ctx, labelList, db); err != nil {
			return fmt.Errorf("batch create doc attribute label failed: %w", err)
		}
	}

	logx.I(ctx, "importDocAttributeLabels done, total: %d", totalCount)
	return nil
}

// updateIDMappingConfig 更新 IDMappingConfig 中的文档ID映射和切片ID映射
// 只有在 IDMappingConfig 中已存在的旧 BusinessID 才会被更新
func (l *Logic) updateIDMappingConfig(config *kb_package.ImportConfig, mapping *docImportMapping) {
	// 更新文档ID映射
	oldIDStr := mapping.OldDocBizID
	// 只有在 IDMappingConfig 中已存在的才更新
	if config.IDMappingConfig.IsMappedIDExist(kb_package.ModuleKbDoc, oldIDStr) {
		config.IDMappingConfig.SetMappedID(kb_package.ModuleKbDoc, oldIDStr, kb_package.MappedID{
			PrimaryID: mapping.NewDocPrimaryID,
			BizID:     strconv.FormatUint(mapping.NewDocBizID, 10),
		})
	}

	// 更新切片ID映射
	for oldSegmentBizID, idMapping := range mapping.OldSegmentID2New {
		oldSegmentIDStr := strconv.FormatUint(oldSegmentBizID, 10)
		// 只有在 IDMappingConfig 中已存在的才更新
		if config.IDMappingConfig.IsMappedIDExist(kb_package.ModuleKbSegment, oldSegmentIDStr) {
			config.IDMappingConfig.SetMappedID(kb_package.ModuleKbSegment, oldSegmentIDStr, kb_package.MappedID{
				PrimaryID: idMapping.PrimaryID,
				BizID:     strconv.FormatUint(idMapping.BizID, 10),
			})
		}
	}
}

// triggerDocImportToIndexTask 触发文档导入索引任务（使用 DocToIndexTask，设置 IsFromBatchImport=true）
func (l *Logic) triggerDocImportToIndexTask(ctx context.Context, config *kb_package.ImportConfig, doc *docEntity.Doc) error {
	params := entity.DocToIndexParams{
		CorpID:            doc.CorpID,
		StaffID:           doc.StaffID,
		RobotID:           config.KbPrimaryID,
		DocID:             doc.ID,
		ExpireStart:       doc.ExpireStart,
		ExpireEnd:         doc.ExpireEnd,
		IsFromBatchImport: true, // 批量导入标记，切片已存在，无需创建
	}

	if err := scheduler.NewDocToIndexTask(ctx, config.KbPrimaryID, params); err != nil {
		logx.E(ctx, "triggerDocImportToIndexTask failed, err: %v", err)
		return err
	}

	logx.I(ctx, "triggerDocImportToIndexTask success, docID: %d", doc.ID)
	return nil
}
