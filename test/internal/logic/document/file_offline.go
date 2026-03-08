package document

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var parseDataTypes = []pb.FileParserSubDataType{
	pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_DOC,
	pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_ES_TABLE,
	pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_TEXT2SQL_TABLE,
}

// ParseOfflineDocTaskResult 解析实时文档解析结果
// // 1. 结果文件下载并解析
// // 2. 解析结果同步数据库
func (l *Logic) ParseOfflineDocTaskResult(ctx context.Context, doc *docEntity.Doc,
	docParse *docEntity.DocParse, segmentType uint32,
	intervene bool) (err error) {
	logx.I(ctx, "ParseOfflineDocTaskResult|doc:%+v, docParse:%+v, segmentType:%d",
		doc, docParse, segmentType)
	// 获取解析结果COS URL
	if docParse.Result == "" {
		logx.E(ctx, "ParseOfflineDocTaskResult|docParse.Result is empty, err:%+v", err)
		return errs.ErrDocParseCosURLNotFound
	}
	result := &pb.FileParserCallbackReq{}
	err = jsonx.UnmarshalFromString(docParse.Result, result)
	if err != nil {
		logx.E(ctx, "ParseOfflineDocTaskResult|jsonx.UnmarshalFromString failed, err:%+v", err)
		return err
	}
	printFileParserPretty(ctx, result)
	logx.I(ctx, "ParseOfflineDocTaskResult|taskResult:%+v", result)
	resultDataMap := result.GetResults()

	// 全局数据（需要保证并发安全）
	// 短链接
	shortURLSyncMap := &sync.Map{}
	// OrgData
	// 创建哈希表存储唯一字符串和OrgData的ID
	orgDataSyncMap := &sync.Map{}
	// BigData
	// 创建哈希表存储唯一字符串和BigData的ID
	bigDataSyncMap := &sync.Map{}
	// text2sql/tableID
	// 为了解决一个table(如：Excel中的sheet）被切分到多个cos文件中时，保证TableID是相同的
	// key: <string>, ${DocID}_${TableName} value: <uint64>, TableID
	text2sqlTableIDMap := &sync.Map{}
	// 切片图片表存储唯一图片URL和图片ID
	imageDataSyncMap := &sync.Map{}

	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(len(parseDataTypes))

	// 处理split数据
	for _, parseDataType := range parseDataTypes {
		wg.Go(func() error {
			logx.I(wgCtx, "ParseOfflineDocTaskResult|handler %s", parseDataType.String())
			return l.handlerOfflineSplitData(wgCtx, doc, segmentType,
				shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap,
				resultDataMap[int32(parseDataType)], intervene)
		})
	}

	if err := wg.Wait(); err != nil {
		logx.E(ctx, "ParseOfflineDocTaskResult|failed|err:%+v", err)
		return err
	}

	logx.I(ctx, "ParseOfflineDocTaskResult|success")

	return nil
}

// GetOfflineDocParseResult 获取解析结果
func (l *Logic) GetOfflineDocParseResult(ctx context.Context, docParse *docEntity.DocParse) (result string, err error) {
	cbReq := &pb.FileParserCallbackReq{}
	err = jsonx.UnmarshalFromString(docParse.Result, cbReq)
	if err != nil {
		logx.E(ctx, "GetOfflineDocParseResult|jsonx.UnmarshalFromString failed, err:%+v", err)
		return "", err
	}
	cosURL := cbReq.GetDebugInfo().GetParseResultCosUrl()
	if len(cosURL) == 0 {
		return "", nil
	}
	parse := new(pb.ParseResult)
	parseResultObj, err := l.s3.GetObject(ctx, cosURL)
	if err != nil {
		logx.E(ctx, "GetOfflineDocParseResult:%+v,err:%+v", cosURL, err)
		return "", err
	}
	err = proto.Unmarshal(parseResultObj, parse)
	if err != nil {
		logx.E(ctx, "GetOfflineDocParseResult.proto Unmarshal data err:%+v", err)
		return "", err
	}
	logx.I(ctx, "KBAgentGetOneDocSummary parseResult.Result:%+v", parse.Result)
	return parse.Result, nil
}

// handlerOfflineSplitData 处理拆分数据
func (l *Logic) handlerOfflineSplitData(ctx context.Context, doc *docEntity.Doc, segmentType uint32,
	shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap *sync.Map,
	data *pb.FileParserCallbackReq_DataResult, intervene bool) (err error) {
	logx.I(ctx, "handlerOfflineSplitData|doc:%+v, segmentType:%d, data:%+v (intervene:%t)",
		doc, segmentType, data, intervene)
	if data == nil {
		logx.I(ctx, "handlerOfflineSplitData|data is nil, ignore")
		return nil
	}
	if data.TotalFileNumber == 0 || data.TotalFileNumber != int32(len(data.Result)) {
		err = fmt.Errorf("data:%+v illegal", data)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logx.I(ctx, "handlerOfflineSplitData|data:(TotalFileNumber:%d, ResultLen:%d)",
		data.TotalFileNumber, len(data.GetResult()))

	// 解析结果channel
	parseResultChan := make(chan parseResult, data.TotalFileNumber)

	// 并发下载解析文件
	wg := &sync.WaitGroup{}
	wg.Add(int(data.TotalFileNumber))

	for _, res := range data.Result {
		go func(ctx context.Context, res *pb.FileParserCallbackReq_Result, shortURLSyncMap *sync.Map) {
			defer gox.Recover()
			logx.I(ctx, "handlerOfflineSplitData|res:%+v goroutine init", res)
			select {
			case <-ctx.Done():
				wg.Done()
				logx.I(ctx, "handlerOfflineSplitData|res:%+v goroutine ctx.Done()", res)
				return
			default:
				// 获取worker资源
				<-fileCfg.offlineWorkerChan
				defer func() {
					wg.Done()
					// 释放worker资源
					fileCfg.offlineWorkerChan <- struct{}{}
				}()
				logx.I(ctx, "handlerOfflineSplitData|res:%+v goroutine run", res)
				// 解析文件
				cosBucket := l.s3.GetBucket(ctx)
				logx.I(ctx, "handlerOfflineSplitData|res:%+v goroutine: download result pb file from bucket:%s",
					res, cosBucket)

				resultPB, err := l.downloadParseResult(ctx, cosBucket, res.Result)
				if err != nil {
					logx.E(ctx, "handlerOfflineSplitData|downloadParseResult failed, err:%+v", err)
					parseResultChan <- parseResult{
						index: res.CurrentFileIndex,
						err:   err,
					}
					return
				}

				logx.D(ctx, "handlerOfflineSplitData|res:%+v goroutine: download result pb file from bucket:%s done!, "+
					"resultPB:(%d images, %d origin strings)",
					res, cosBucket, len(resultPB.Images), len(resultPB.OriginStr))

				docPageContents, tablePageContents, text2SqlResults, err :=
					l.getSplitDataFromParsedResult(ctx, shortURLSyncMap, cosBucket, resultPB)
				if err != nil {
					logx.E(ctx, "handlerOfflineSplitData|getSplitDataFromCosURL failed, "+
						"err:%+v", err)
				}
				parseResultChan <- parseResult{
					index:             res.CurrentFileIndex,
					err:               err,
					docPageContents:   docPageContents,
					tablePageContents: tablePageContents,
					text2SqlResults:   text2SqlResults,
				}
				logx.I(ctx, "handlerOfflineSplitData|res:%+v goroutine done", res)
			}
		}(ctx, res, shortURLSyncMap)
	}

	// 等待所有解析 goroutine 完成并关闭解析结果channel
	go func(ctx context.Context) {
		defer gox.Recover()
		wg.Wait()
		close(parseResultChan)
		logx.I(ctx, "handlerOfflineSplitData|all res data complete")
	}(ctx)

	// 顺序处理解析结果
	err = l.dealOfflineSplitResultByOrder(ctx, doc,
		shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap, segmentType,
		parseResultChan, intervene)
	if err != nil {
		logx.E(ctx, "handlerOfflineSplitData|dealOfflineSplitResultByOrder failed, err:%+v", err)
		return err
	}
	return nil
}

// dealOfflineSplitResultByOrder 顺序处理拆分结果数据
func (l *Logic) dealOfflineSplitResultByOrder(ctx context.Context, doc *docEntity.Doc,
	shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap *sync.Map,
	segmentType uint32, parseResultChan chan parseResult, intervene bool) (err error) {
	logx.I(ctx, "dealOfflineSplitResultByOrder|run|doc:%+v, segmentType:%d", doc, segmentType)
	nextIndex := int32(0)
	resultMap := make(map[int32]parseResult)
	// 获取干预前的原始切片内容
	oldOrgDataInfos := make([]*segEntity.OldOrgDataInfo, 0)
	currentOrgDataOrder := 0
	if intervene && segmentType == segEntity.SegmentTypeIndex && !docEntity.IsTableTypeDocument(doc.FileType) {
		oldOrgDataInfos, err = l.getPreInterventionOrgData(ctx, doc)
		if err != nil {
			logx.E(ctx, "dealOfflineSplitResultByOrder|getPreInterventionOrgData, err:%+v",
				err)
		}
		logx.I(ctx, "dealOfflineSplitResultByOrder|len(oldOrgDataInfos):%d", len(oldOrgDataInfos))
	}
	// todo 确认是否只有一个文件
	// 如果只有一个文件则对比orgdata的数量
	for resultChan := range parseResultChan {
		if resultChan.err != nil {
			return resultChan.err
		}

		resultMap[resultChan.index] = resultChan

		// 顺序处理
		for {
			if result, ok := resultMap[nextIndex]; ok {
				logx.I(ctx, "dealOfflineSplitResultByOrder|dealIndex:%d", nextIndex)
				var releaseStatus uint32
				var typ int
				if segmentType == segEntity.SegmentTypeIndex {
					releaseStatus, typ = segEntity.SegmentReleaseStatusInit, segEntity.SegmentTypeIndex
				}
				if segmentType == segEntity.SegmentTypeQA {
					releaseStatus, typ = segEntity.SegmentReleaseStatusNotRequired, segEntity.SegmentTypeQA
				}
				var segments []*segEntity.DocSegmentExtend
				segments, err = l.newOfflineDocSegmentFromPageContent(ctx, doc, result.docPageContents,
					result.tablePageContents, result.text2SqlResults, releaseStatus, text2sqlTableIDMap, typ)
				if err != nil {
					return err
				}
				// 写DB 存储BigData和OrgData
				if err = l.segLogic.CreateSegmentWithBigData(ctx,
					shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, imageDataSyncMap, segments, doc.RobotID,
					&currentOrgDataOrder, oldOrgDataInfos); err != nil {
					return err
				}
				// 释放内存
				delete(resultMap, nextIndex)
				nextIndex++
			} else {
				break
			}
		}
	}
	// 处理完成后Map应该不会有值
	if len(resultMap) > 0 {
		err = fmt.Errorf("resultMap has unprocessed data, err:%+v", err)
		logx.E(ctx, "dealOfflineSplitResultByOrder|failed, err:%+v|len(resultMap):%d, nextIndex:%d",
			err, len(resultMap), nextIndex)
		return err
	}

	logx.I(ctx, "dealOfflineSplitResultByOrder|done")
	return nil
}

func replacePrefix(org string, title string, renamePrefix string) string {
	if renamePrefix == "" {
		return org
	}
	return strings.Replace(org, title, renamePrefix, 1)
}

// newOfflineDocSegmentFromPageContent 转换为DB存储segment
func (l *Logic) newOfflineDocSegmentFromPageContent(ctx context.Context, doc *docEntity.Doc,
	docPageContents, tablePageContents []*pb.PageContent, text2SQLTables *pb.Tables,
	releaseStatus uint32, text2sqlTableIDMap *sync.Map, typ int) ([]*segEntity.DocSegmentExtend, error) {
	title := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	segments := make([]*segEntity.DocSegmentExtend, 0, len(docPageContents))
	// 文档
	renamePrefix := ""
	if doc.FileNameInAudit != "" {
		// 文档导入重命名的场景,需要将文档切片的前缀替换成新的前缀
		renamePrefix = util.FileNameNoSuffix(doc.FileNameInAudit) + ": \n"
	}
	if text2SQLTables != nil && text2SQLTables.MetaData != nil {
		text2SQLTables.MetaData.FileName = replacePrefix(text2SQLTables.MetaData.FileName, title, renamePrefix)
	}
	for _, pageContent := range docPageContents {
		pgContent := replacePrefix(pageContentToString(pageContent), title, renamePrefix)
		orgData := replacePrefix(pageContent.GetPageContentOrgString(), title, renamePrefix)
		if err := l.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &segEntity.DocSegmentExtend{
			DocSegment: segEntity.DocSegment{
				RobotID:         doc.RobotID,
				CorpID:          doc.CorpID,
				StaffID:         doc.StaffID,
				DocID:           doc.ID,
				Outputs:         "",
				FileType:        doc.FileType,
				SegmentType:     segEntity.SegmentTypeSegment,
				Title:           replacePrefix(title, title, renamePrefix),
				PageContent:     pgContent,
				OrgData:         orgData,
				SplitModel:      "",
				Status:          segEntity.SegmentStatusInit,
				ReleaseStatus:   releaseStatus,
				IsDeleted:       segEntity.SegmentIsNotDeleted,
				Type:            typ,
				NextAction:      segEntity.SegNextActionAdd,
				RichTextIndex:   int(pageContent.GetRichContentId()),
				UpdateTime:      time.Now(),
				StartChunkIndex: int(pageContent.GetOrgStart()),
				EndChunkIndex:   int(pageContent.GetOrgEnd()),
				LinkerKeep:      pageContent.GetLinkerKeep(),
				CreateTime:      time.Now(),
				BatchID:         doc.BatchID,
				BigStart:        pageContent.GetBigStart(),
				BigEnd:          pageContent.GetBigEnd(),
				BigString:       replacePrefix(pageContent.GetPageContentBigString(), title, renamePrefix),
				Images:          pageContent.GetImages(),
				OrgPageNumbers:  l.convertOrgPageNumbers2Str(ctx, pageContent.GetOrgPageNumbers()),
				BigPageNumbers:  l.convertBigPageNumbers2Str(ctx, pageContent.GetBigPageNumbers()),
				SheetData:       l.convertSheetData2Str(ctx, pageContent.GetSheetData()),
			},
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		})
	}
	// 表格
	for _, pageContent := range tablePageContents {
		pgContent := pageContentToString(pageContent)
		orgData := pageContent.GetPageContentOrgString()
		if err := l.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &segEntity.DocSegmentExtend{
			DocSegment: segEntity.DocSegment{
				RobotID:         doc.RobotID,
				CorpID:          doc.CorpID,
				StaffID:         doc.StaffID,
				DocID:           doc.ID,
				Outputs:         "",
				FileType:        doc.FileType,
				SegmentType:     segEntity.SegmentTypeTable,
				Title:           replacePrefix(title, title, renamePrefix),
				PageContent:     pgContent,
				OrgData:         orgData,
				SplitModel:      "",
				Status:          segEntity.SegmentStatusInit,
				ReleaseStatus:   releaseStatus,
				IsDeleted:       segEntity.SegmentIsNotDeleted,
				Type:            typ,
				NextAction:      segEntity.SegNextActionAdd,
				RichTextIndex:   int(pageContent.GetRichContentId()),
				UpdateTime:      time.Now(),
				StartChunkIndex: int(pageContent.GetOrgStart()),
				EndChunkIndex:   int(pageContent.GetOrgEnd()),
				LinkerKeep:      pageContent.GetLinkerKeep(),
				CreateTime:      time.Now(),
				BatchID:         doc.BatchID,
				BigStart:        pageContent.GetBigStart(),
				BigEnd:          pageContent.GetBigEnd(),
				BigString:       pageContent.GetPageContentBigString(),
				Images:          pageContent.GetImages(),
				OrgPageNumbers:  l.convertOrgPageNumbers2Str(ctx, pageContent.GetOrgPageNumbers()),
				BigPageNumbers:  l.convertBigPageNumbers2Str(ctx, pageContent.GetBigPageNumbers()),
				SheetData:       l.convertSheetData2Str(ctx, pageContent.GetSheetData()),
			},
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		})
	}
	s0 := l.appendText2sqlSegments(ctx, doc, replacePrefix(title, title, renamePrefix), text2SQLTables, releaseStatus, text2sqlTableIDMap, typ)
	if len(s0) > 0 {
		segments = append(segments, s0...)
	}
	return segments, nil
}

// convertOrgPageNumbers2Str 转换切片页码信息
func (l *Logic) convertOrgPageNumbers2Str(ctx context.Context, orgPageNumbers []int32) string {
	if len(orgPageNumbers) == 0 {
		return ""
	}
	orgPageStr, err := jsonx.MarshalToString(orgPageNumbers)
	if err != nil {
		logx.E(ctx, "convertOrgPageNumbers2Str|jsonx.MarshalToString|"+
			"failed|orgPageNumbers:%+v|err:%+v", orgPageNumbers, err)
	}
	return orgPageStr
}

// convertBigPageNumbers2Str 转换切片页码信息
func (l *Logic) convertBigPageNumbers2Str(ctx context.Context, bigPageNumbers []int32) string {
	if len(bigPageNumbers) == 0 {
		return ""
	}
	bigPageStr, err := jsonx.MarshalToString(bigPageNumbers)
	if err != nil {
		logx.E(ctx, "convertBigPageNumbers2Str|jsonx.MarshalToString|"+
			"failed|bigPageNumbers:%+v|err:%+v", bigPageNumbers, err)
	}
	return bigPageStr
}

// convertSheetData2Str 转换切片页码信息
func (l *Logic) convertSheetData2Str(ctx context.Context, sheetData []*pb.PageContent_SheetData) string {
	if len(sheetData) == 0 {
		return ""
	}
	for _, data := range sheetData {
		if data == nil {
			logx.E(ctx, "convertSheetData2Str|sheetData slice is nil|"+
				"sheetData:%+v", sheetData)
			return ""
		}
	}
	sheetPageStr, err := jsonx.MarshalToString(sheetData)
	if err != nil {
		logx.E(ctx, "convertSheetData2Str|jsonx.MarshalToString|"+
			"failed|sheetData:%+v|err:%+v", sheetData, err)
	}
	return sheetPageStr
}

// appendText2sqlSegments 追加text2sql的分片
// purpose: 用于QA还是文档索引的Index
func (l *Logic) appendText2sqlSegments(ctx context.Context, doc *docEntity.Doc, title string, tableFiles *pb.Tables,
	releaseStatus uint32, text2sqlTableIDMap *sync.Map, purpose int) []*segEntity.DocSegmentExtend {

	segments := make([]*segEntity.DocSegmentExtend, 0)
	if tableFiles == nil {
		logx.W(ctx, "appendText2sqlSegments|DocID:%d|tableFiles is empty", doc.ID)
		return nil
	}

	options := protojson.MarshalOptions{
		UseEnumNumbers:  false, // 使用枚举的名称(字符串)
		EmitUnpopulated: false, // 是否包含未设置的值
		// Indent:          "  ",
		EmitDefaultValues: true,
	}
	// 将 protobuf 消息序列化为 JSON
	tableStr, _ := options.Marshal(tableFiles)
	logx.I(ctx, "appendText2sqlSegments|DocID:%d|tableStr:%s", doc.ID, string(tableStr))

	meta, contents, err := l.buildText2sqlSegments(ctx, doc, text2sqlTableIDMap, tableFiles)
	if err != nil {
		logx.E(ctx, "appendText2sqlSegments|buildText2sqlSegments|DocID:%d|err:%+v|", doc.ID, err)
		return nil
	}
	m, err := jsonx.Marshal(meta)
	if err != nil {
		logx.E(ctx, "appendText2sqlSegments|DocID:%d|err:%+v|meta:%+v", doc.ID, err, meta)
		return nil
	}
	if len([]rune(string(m))) > config.App().Text2sqlPageContentMaxLength {
		logx.W(ctx, "appendText2sqlSegments|DocID:%d|meta.length:%d|Text2sqlPageContentMaxLength:%d",
			doc.ID, len([]rune(string(m))), config.App().Text2sqlPageContentMaxLength)
		return nil
	}
	metaSegment := newText2sqlSegments(doc, segEntity.SegmentTypeText2SQLMeta, title, string(m), releaseStatus, purpose)
	segments = append(segments, metaSegment)

	for i, content := range contents {
		c, err := jsonx.Marshal(content)
		if err != nil {
			logx.E(ctx, "appendText2sqlSegments|[%d]|DocID:%d|err:%+v|content:%+v", i, doc.ID, err, content)
			return nil
		}
		if len([]rune(string(c))) > config.App().Text2sqlPageContentMaxLength {
			logx.W(ctx, "appendText2sqlSegments|DocID:%d|[%d]|content.length:%d|MaxLength:%d",
				doc.ID, i, len([]rune(string(c))), config.App().Text2sqlPageContentMaxLength)
			return nil
		}
		// title 这里可以加上tableName，便于排查问题
		c0 := newText2sqlSegments(doc, segEntity.SegmentTypeText2SQLContent, title, string(c), releaseStatus, purpose)
		segments = append(segments, c0)
	}
	logx.I(ctx, "appendText2sqlSegments|DocID:%d|contents.len:%d|segments.len:%d", doc.ID,
		len(contents), len(segments))

	return segments
}

func (l *Logic) buildText2sqlSegments(ctx context.Context, doc *docEntity.Doc, text2sqlTableIDMap *sync.Map,
	tableFile *pb.Tables) (*segEntity.Text2SQLSegmentMeta, []*segEntity.Text2SQLSegmentContent, error) {

	meta := new(segEntity.Text2SQLSegmentMeta)
	meta.Version = segEntity.Text2sqlVersion1

	contents := make([]*segEntity.Text2SQLSegmentContent, 0)
	// 对应到Excel文件是文件名；
	fileName := tableFile.GetMetaData().GetFileName()
	if len(fileName) == 0 {
		logx.E(ctx, "buildText2sqlSegments|fileName is empty|doc:%+v", doc)
		return nil, nil, fmt.Errorf("fileName is empty")
	}

	logx.I(ctx, "buildText2sqlSegments|fileName:%s|Tables.len:%d", fileName, len(tableFile.GetTables()))

	meta.FileName = fileName
	meta.TableMetas = make([]*segEntity.Text2SQLSegmentTableMeta, 0, len(tableFile.GetTables()))
	// 对应到Excel文件是一个个 Sheet
	for ti, table := range tableFile.GetTables() {

		headers := table.GetMetaData().GetHeaders()
		tableName := table.GetMetaData().GetTableName()
		logx.I(ctx, "buildText2sqlSegments|fileName:%s|[%d]|tableName:%s|headers.len:%d", fileName,
			ti, tableName, len(headers))

		key := fmt.Sprintf("%d_%s", doc.ID, table.GetMetaData().GetTableName())
		var tableID uint64
		if tid, ok := text2sqlTableIDMap.Load(key); ok {
			tableID = tid.(uint64)
		} else {
			tableID = idgen.GetId()
			text2sqlTableIDMap.Store(key, tableID)
		}
		tableMeta := &segEntity.Text2SQLSegmentTableMeta{
			TableID:   strconv.FormatUint(tableID, 10),
			TableName: table.GetMetaData().GetTableName(),
			DataType:  segEntity.TableDataType(table.GetMetaData().GetDataType()),
			Headers:   make([]*segEntity.Text2SQLSegmentTableMetaHeader, 0, len(headers)),
			Message:   table.GetMetaData().GetMessage(),
		}
		for hi, header := range headers {
			logx.I(ctx, "buildText2sqlSegments|fileName:%s|[%d]|tableName:%s|headerType:%s", fileName, hi,
				tableName, header.GetType().String())
			metaHeader := &segEntity.Text2SQLSegmentTableMetaHeader{
				Type: segEntity.TableHeaderType(header.GetType()),
			}
			metaHeader.Rows = make([]*segEntity.Text2SQLRow, 0, len(header.GetRows()))
			for _, row := range header.GetRows() {
				r0 := &segEntity.Text2SQLRow{}
				r0.Cells = make([]*segEntity.Text2SQLCell, 0, len(row.GetCells()))
				for _, cell := range row.GetCells() {
					r0.Cells = append(r0.Cells, &segEntity.Text2SQLCell{
						Value:    cell.GetValue(),
						DataType: segEntity.TableDataCellDataType(cell.GetCellDataType()),
					})
				}
				metaHeader.Rows = append(metaHeader.Rows, r0)
			}
			tableMeta.Headers = append(tableMeta.Headers, metaHeader)
		}
		meta.TableMetas = append(meta.TableMetas, tableMeta)

		for i, row := range table.GetRows() {
			content := new(segEntity.Text2SQLSegmentContent)
			content.Version = segEntity.Text2sqlVersion1
			content.TableID = strconv.FormatUint(tableID, 10)
			content.RowNum = int64(i)
			content.Cells = make([]*segEntity.Text2SQLCell, 0, len(row.GetCells()))
			for _, cell := range row.GetCells() {
				content.Cells = append(content.Cells, &segEntity.Text2SQLCell{
					Value: cell.GetValue(),
					//  知识引擎v2.4.0版本，content cells里面的DataType字段无意义，20240701
					// DataType: model.TableDataCellDataType(cell.GetCellDataType()),
				})
			}
			contents = append(contents, content)
		}
	}

	return meta, contents, nil
}

func newText2sqlSegments(doc *docEntity.Doc, segmentType, title string, pageContent string, releaseStatus uint32,
	purpose int) *segEntity.DocSegmentExtend {

	return &segEntity.DocSegmentExtend{
		DocSegment: segEntity.DocSegment{
			RobotID:         doc.RobotID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			DocID:           doc.ID,
			Outputs:         "",
			FileType:        doc.FileType,
			SegmentType:     segmentType,
			Title:           title,
			PageContent:     pageContent,
			OrgData:         "",
			SplitModel:      "",
			Status:          segEntity.SegmentStatusInit,
			ReleaseStatus:   releaseStatus,
			IsDeleted:       segEntity.SegmentIsNotDeleted,
			Type:            purpose,
			NextAction:      segEntity.SegNextActionAdd,
			RichTextIndex:   0,
			UpdateTime:      time.Now(),
			StartChunkIndex: 0,
			EndChunkIndex:   0,
			LinkerKeep:      false,
			CreateTime:      time.Now(),
			BatchID:         doc.BatchID,
			BigStart:        0,
			BigEnd:          0,
			BigString:       "",
		},
		ExpireStart: doc.ExpireStart,
		ExpireEnd:   doc.ExpireEnd,
	}
}

// printFileParserPretty 打印解析结果
func printFileParserPretty(ctx context.Context, req *pb.FileParserCallbackReq) {
	for k, result := range req.GetResults() {
		dataType := pb.FileParserSubDataType_name[k]
		logx.I(ctx, "printFileParserPretty|%s|total:%d", dataType, result.GetTotalFileNumber())
		for i, r := range result.GetResult() {
			logx.I(ctx, "printFileParserPretty|%s|[%d]|r:%+v", dataType, i, r)
		}
	}
}

func (l *Logic) checkPageContentAndOrgDataLen(ctx context.Context, docID uint64, pgContent, orgData string) error {
	if utf8.RuneCountInString(pgContent) > config.App().PageContentMaxLength {
		logx.E(ctx, "checkPageContentAndOrgDataLen|page_content is too long, length:%d|docID:%d, "+
			"pageContent:%v", utf8.RuneCountInString(pgContent), docID, pgContent)
		return fmt.Errorf("page_content is too long")
	}
	if utf8.RuneCountInString(orgData) > config.App().OrgDataMaxLength {
		logx.E(ctx, "checkPageContentAndOrgDataLen|org_data is too long, length:%d|docID:%d, "+
			"orgData:%v", utf8.RuneCountInString(orgData), docID, orgData)
		return fmt.Errorf("org_data is too long")
	}
	return nil
}

// getPreInterventionOrgData 获取干预的切片
func (l *Logic) getPreInterventionOrgData(ctx context.Context, doc *docEntity.Doc) ([]*segEntity.OldOrgDataInfo, error) {
	logx.I(ctx, "getPreInterventionOrgData|start, FileType:%s", doc.FileType)
	oldOrgDataInfos := make([]*segEntity.OldOrgDataInfo, 0)
	corpBizID, appBizID, _, _, err := l.segLogic.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		logx.E(ctx, "getPreInterventionOrgData|failed, err:%+v", err)
		return oldOrgDataInfos, err
	}
	docCommon := &segEntity.DocSegmentCommon{
		AppID:     doc.RobotID,
		AppBizID:  appBizID,
		CorpID:    doc.CorpID,
		CorpBizID: corpBizID,
		DocBizID:  doc.BusinessID,
	}
	pageNumber := uint32(0)
	pageSize := uint32(100)
	skipTimes := 0
	deleteDataMaxLimit := 10
	count := 0
	// 统计文档的切片数
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          corpBizID,
		AppBizID:           appBizID,
		DocBizID:           doc.BusinessID,
		IsDeleted:          ptrx.Bool(false),
		IsTemporaryDeleted: ptrx.Bool(true),
		RouterAppBizID:     appBizID,
	}
	total, err := l.segLogic.GetDocOrgDatumCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|err:%v", err)
		return oldOrgDataInfos, err
	}
	logx.I(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|total:%d", total)
	for {
		pageNumber++
		list, err := l.GetDocSegmentList(ctx, docCommon, pageNumber, pageSize)
		if err != nil {
			logx.E(ctx, "getPreInterventionOrgData|GetDocSegmentList|err:%v", err)
			return oldOrgDataInfos, err
		}
		// 特殊处理，在干预中数据库中数据被修改报错
		if len(list) == 0 {
			skipTimes++
			if skipTimes > deleteDataMaxLimit {
				break
			}
			err = fmt.Errorf("切片数据获取错误或连续删除切片数量超过%d", deleteDataMaxLimit*int(pageSize))
			logx.E(ctx, "task(DocSegIntervene) GetDocSegmentList|err:%v", err)
			return oldOrgDataInfos, err
		} else {
			skipTimes = 0
		}
		// 拼接md，上传cos
		for _, seg := range list {
			addMethod := segEntity.AddMethodDefault
			if seg.IsAdd {
				addMethod = segEntity.AddMethodArtificial
			} else if !seg.IsOrigin && !seg.IsAdd {
				addMethod = segEntity.AddMethodEdit
			}
			isDisabled := segEntity.SegmentIsEnable
			if seg.IsDisabled {
				isDisabled = segEntity.SegmentIsDisabled
			}
			oldOrgDataInfos = append(oldOrgDataInfos, &segEntity.OldOrgDataInfo{
				AddMethod:  addMethod,
				IsDisabled: isDisabled,
			})
		}
		count += len(list)
		if int(pageNumber)*int(pageSize) >= int(total) {
			break
		}
	}
	logx.I(ctx, "getPreInterventionOrgData len(oldOrgDataInfos):%d", len(oldOrgDataInfos))
	return oldOrgDataInfos, nil
}

// GetDocSegmentList logic 复制，待迁移
func (l *Logic) GetDocSegmentList(ctx context.Context, docCommon *segEntity.DocSegmentCommon,
	pageNum, pageSize uint32) ([]*knowledge.ListDocSegmentRsp_DocSegmentItem, error) {
	// 1.获取原始切片
	offset, limit := utilx.Page(pageNum, pageSize)
	originList, orgDateBizIDs, err := l.GetDocSegmentOrgData(ctx, docCommon, offset, limit)
	if err != nil {
		logx.E(ctx, "GetDocSegmentOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	// 2.编辑切片内容替换
	editOriginList, err := l.segLogic.GetEditOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		logx.E(ctx, "GetEditOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	for _, edit := range editOriginList {
		for _, item := range originList {
			if item.SegBizId == edit.OriginOrgDataID {
				item.OrgData = edit.OrgData
				item.SegBizId = edit.BusinessID
				item.IsOrigin = false
				item.AuditStatus = uint64(edit.AuditStatus)
			}
		}
	}
	// 3.新增切片添加
	// 兼容原始切片都删除，只留新增切片的场景
	if pageNum == 1 {
		orgDateBizIDs = append(orgDateBizIDs, segEntity.InsertAtFirst)
	}
	insertOriginList, err := l.GetInsertOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		logx.E(ctx, "GetInsertOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}

	originList, err = InsertIntoOrgDataList(ctx, insertOriginList, originList)
	if err != nil {
		logx.E(ctx, "InsertIntoOrgDataList failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	logx.I(ctx, "GetDocSegmentList|len(originList):%d", len(originList))
	return originList, nil
}

// GetDocSegmentOrgData logic 复制，待迁移
func (l *Logic) GetDocSegmentOrgData(ctx context.Context, docCommon *segEntity.DocSegmentCommon,
	offset, limit int) ([]*knowledge.ListDocSegmentRsp_DocSegmentItem, []string, error) {
	logx.I(ctx, "GetDocSegmentOrgData|start|offset:%d|limit:%d", offset, limit)
	orgDataList := make([]*knowledge.ListDocSegmentRsp_DocSegmentItem, 0)
	orgDateBizIDs := make([]string, 0)
	// 由于前面已将上一版本的OrgData逻辑删除，这里需要查找已经删除的OrgData
	list := make([]*segEntity.DocSegmentOrgData, 0)
	var err error
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		IsDeleted:          ptrx.Bool(true),
		IsTemporaryDeleted: ptrx.Bool(false),
		OrderColumn:        []string{segEntity.DocSegmentOrgDataTblColBusinessID},
		OrderDirection:     []string{util.SqlOrderByAsc},
		Offset:             offset,
		Limit:              limit,
		RouterAppBizID:     docCommon.AppBizID,
	}
	list, err = l.segLogic.GetDocOrgDataList(ctx,
		segEntity.DocSegmentOrgDataTblColList, filter)
	if err != nil {
		return orgDataList, orgDateBizIDs, err
	}
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err = jsonx.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				logx.W(ctx, "GetDocSegmentOrgData|PageInfos|UnmarshalFromString|err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint64(page))
			}
		}
		orgDateBizIDs = append(orgDateBizIDs, strconv.FormatUint(orgDate.BusinessID, 10))
		docSegmentItem := &knowledge.ListDocSegmentRsp_DocSegmentItem{
			SegBizId:    strconv.FormatUint(orgDate.BusinessID, 10),
			OrgData:     orgDate.OrgData,
			PageInfos:   pageInfos,
			IsOrigin:    orgDate.AddMethod == segEntity.AddMethodDefault,
			IsAdd:       orgDate.AddMethod == segEntity.AddMethodArtificial,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled,
		}
		orgDataList = append(orgDataList, docSegmentItem)
	}
	logx.I(ctx, "GetDocSegmentOrgData|len(OrgData):%d", len(orgDataList))
	return orgDataList, orgDateBizIDs, nil
}

// GetInsertOrgData 获取插入的切片 logic 复制，待迁移
func (l *Logic) GetInsertOrgData(ctx context.Context, orgDateBizIDs []string,
	docCommon *segEntity.DocSegmentCommon) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	logx.I(ctx, "GetInsertOrgData|start")
	actionFlag := segEntity.InsertAction
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:            docCommon.CorpBizID,
		AppBizID:             docCommon.AppBizID,
		DocBizID:             docCommon.DocBizID,
		IsDeleted:            ptrx.Bool(false),
		Action:               &actionFlag,
		LastOriginOrgDataIDs: orgDateBizIDs,
		OrderColumn:          []string{segEntity.DocSegmentOrgDataTemporaryTblColBusinessID},
		OrderDirection:       []string{util.SqlOrderByAsc},
	}

	originList, err := l.segDao.GetInsertTemporaryOrgData(ctx,
		segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		logx.E(ctx, "GetInsertOrgData|err:%v", err)
		return nil, err
	}
	logx.I(ctx, "GetInsertOrgData|len(OrgData):%d", len(originList))
	return originList, nil
}

// InsertIntoOrgDataList 插入切片 logic 复制，待迁移
func InsertIntoOrgDataList(ctx context.Context, insertOriginList []*segEntity.DocSegmentOrgDataTemporary,
	originList []*knowledge.ListDocSegmentRsp_DocSegmentItem) ([]*knowledge.ListDocSegmentRsp_DocSegmentItem, error) {
	logx.I(ctx, "InsertIntoOrgDataList|start")
	// 切片内容插入
	// 构建非新增数据映射（用于快速查找）
	originMap := make(map[string]struct{})
	for _, originSeg := range originList {
		originMap[originSeg.SegBizId] = struct{}{}
	}
	originMap[segEntity.InsertAtFirst] = struct{}{}

	// 构建新增数据映射（key: last_org_data_id, value: 切片）
	insertMap := make(map[string]*segEntity.DocSegmentOrgDataTemporary)
	for _, insertSeg := range insertOriginList {
		insertMap[insertSeg.LastOrgDataID] = insertSeg
	}

	// 收集指向非新增数据中的起点节点
	startSegs := make([]*segEntity.DocSegmentOrgDataTemporary, 0)
	for _, insertSeg := range insertOriginList {
		if _, exists := originMap[insertSeg.LastOrgDataID]; exists {
			startSegs = append(startSegs, insertSeg)
		}
	}

	// 按非新增数据分组存储插入数据
	segChains := make(map[string][]*segEntity.DocSegmentOrgDataTemporary)
	for _, startSeg := range startSegs {
		originSegID := startSeg.LastOrgDataID
		chain := []*segEntity.DocSegmentOrgDataTemporary{startSeg}

		// 沿着链向后收集所有节点
		current := startSeg
		for {
			nextSeg, exists := insertMap[current.BusinessID]
			if !exists {
				break
			}
			chain = append(chain, nextSeg)
			current = nextSeg
		}
		segChains[originSegID] = append(segChains[originSegID], chain...)
	}

	// 构建最终节点列表
	finalSegs := make([]*knowledge.ListDocSegmentRsp_DocSegmentItem, 0)
	// 先增加LastOrgDataID为first的链
	if segChain, exists := segChains[segEntity.InsertAtFirst]; exists {
		for _, orgData := range segChain {
			docSegmentItem := &knowledge.ListDocSegmentRsp_DocSegmentItem{
				SegBizId:    orgData.BusinessID,
				OrgData:     orgData.OrgData,
				PageInfos:   []uint64{},
				IsOrigin:    false,
				IsAdd:       true,
				SegmentType: "",
				IsDisabled:  orgData.IsDisabled,
				AuditStatus: uint64(orgData.AuditStatus),
			}
			finalSegs = append(finalSegs, docSegmentItem)
		}
	}
	// 增加非新增数据关联的链
	for _, originSeg := range originList {
		// 添加非新增数据
		finalSegs = append(finalSegs, originSeg)

		// 添加该节点对应的插入链
		if segChain, exists := segChains[originSeg.SegBizId]; exists {
			for _, orgData := range segChain {
				docSegmentItem := &knowledge.ListDocSegmentRsp_DocSegmentItem{
					SegBizId:    orgData.BusinessID,
					OrgData:     orgData.OrgData,
					PageInfos:   []uint64{},
					IsOrigin:    false,
					IsAdd:       true,
					SegmentType: "",
					IsDisabled:  orgData.IsDisabled,
					AuditStatus: uint64(orgData.AuditStatus),
				}
				finalSegs = append(finalSegs, docSegmentItem)
			}
		}
	}
	logx.I(ctx, "InsertIntoOrgDataList|len(finalSegs):%d", len(finalSegs))
	return finalSegs, nil
}
