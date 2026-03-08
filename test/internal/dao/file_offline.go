// Package dao TODO
// @Author: halelv
// @Date: 2024/5/24 21:25
package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	knowledge "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/common/v3/errors"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ParseOfflineDocTaskResult 解析实时文档解析结果
// // 1. 结果文件下载并解析
// // 2. 解析结果同步数据库
func (d *dao) ParseOfflineDocTaskResult(ctx context.Context, doc *model.Doc, docParse model.DocParse, segmentType uint32,
	intervene bool) (err error) {
	log.InfoContextf(ctx, "ParseOfflineDocTaskResult|doc:%+v, docParse:%+v, segmentType:%d",
		doc, docParse, segmentType)
	// 获取解析结果COS URL
	if docParse.Result == "" {
		return errs.ErrDocParseCosURLNotFound
	}
	result := &pb.FileParserCallbackReq{}
	err = jsoniter.UnmarshalFromString(docParse.Result, result)
	if err != nil {
		log.ErrorContextf(ctx, "ParseOfflineDocTaskResult|jsoniter.UnmarshalFromString failed, err:%+v", err)
		return err
	}
	printFileParserPretty(ctx, result)
	log.InfoContextf(ctx, "ParseOfflineDocTaskResult|taskResult:%+v", result)
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

	wg := sync.WaitGroup{}
	wg.Add(3)

	errChan := make(chan error, 3)

	// 处理split数据
	go func() {
		defer errors.PanicHandler()
		// 解析拆分的文档片段
		log.InfoContextf(ctx, "ParseOfflineDocTaskResult|handler TYPE_DOC")
		if err := d.handlerOfflineSplitData(ctx, doc, segmentType,
			shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap,
			resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_DOC)], intervene); err != nil {
			errChan <- err
		}
		wg.Done()
	}()
	go func() {
		defer errors.PanicHandler()
		// 解析拆分的表格片段
		log.InfoContextf(ctx, "ParseOfflineDocTaskResult|handler TYPE_ES_TABLE")
		if err := d.handlerOfflineSplitData(ctx, doc, segmentType,
			shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap,
			resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_ES_TABLE)], intervene); err != nil {
			errChan <- err
		}
		wg.Done()
	}()
	go func() {
		defer errors.PanicHandler()
		// 解析拆分的Text2Sql片段
		log.InfoContextf(ctx, "ParseOfflineDocTaskResult|handler TYPE_TEXT2SQL_TABLE")
		if err := d.handlerOfflineSplitData(ctx, doc, segmentType,
			shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap,
			resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_TEXT2SQL_TABLE)], intervene); err != nil {
			errChan <- err
		}
		wg.Done()
	}()

	wg.Wait()

	for {
		select {
		case err = <-errChan:
			log.ErrorContextf(ctx, "ParseOfflineDocTaskResult|failed|err:%+v", err)
			return err
		default:
			log.InfoContextf(ctx, "ParseOfflineDocTaskResult|success|doc:%+v", doc)
			return nil
		}
	}
}

// GetOfflineDocParseResult 获取解析结果
func (d *dao) GetOfflineDocParseResult(ctx context.Context, docParse model.DocParse) (result string, err error) {
	cbReq := &pb.FileParserCallbackReq{}
	err = jsoniter.UnmarshalFromString(docParse.Result, cbReq)
	if err != nil {
		log.ErrorContextf(ctx, "GetOfflineDocParseResult|jsoniter.UnmarshalFromString failed, err:%+v", err)
		return "", err
	}
	cosURL := cbReq.GetDebugInfo().GetParseResultCosUrl()
	if len(cosURL) == 0 {
		return "", nil
	}
	parse := new(pb.ParseResult)
	parseResultObj, err := d.GetObject(ctx, cosURL)
	if err != nil {
		log.ErrorContextf(ctx, "GetOfflineDocParseResult:%+v,err:%+v", cosURL, err)
		return "", err
	}
	err = proto.Unmarshal(parseResultObj, parse)
	if err != nil {
		log.ErrorContextf(ctx, "GetOfflineDocParseResult.proto Unmarshal data err:%+v", err)
		return "", err
	}
	log.InfoContextf(ctx, "KBAgentGetOneDocSummary parseResult.Result:%+v", parse.Result)
	return parse.Result, nil
}

// handlerOfflineSplitData 处理拆分数据
func (d *dao) handlerOfflineSplitData(ctx context.Context, doc *model.Doc, segmentType uint32,
	shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap *sync.Map,
	data *pb.FileParserCallbackReq_DataResult, intervene bool) (err error) {
	log.InfoContextf(ctx, "handlerOfflineSplitData|doc:%+v, segmentType:%d, data:%+v", doc, segmentType, data)
	if data == nil {
		log.InfoContextf(ctx, "handlerOfflineSplitData|data is nil, ignore")
		return nil
	}
	if data.TotalFileNumber == 0 || data.TotalFileNumber != int32(len(data.Result)) {
		err = fmt.Errorf("data:%+v illegal", data)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 解析结果channel
	parseResultChan := make(chan parseResult, data.TotalFileNumber)

	// 并发下载解析文件
	wg := &sync.WaitGroup{}
	wg.Add(int(data.TotalFileNumber))

	for _, res := range data.Result {
		go func(ctx context.Context, res *pb.FileParserCallbackReq_Result, shortURLSyncMap *sync.Map) {
			defer errors.PanicHandler()
			log.InfoContextf(ctx, "handlerOfflineSplitData|res:%+v goroutine init", res)
			select {
			case <-ctx.Done():
				wg.Done()
				log.InfoContextf(ctx, "handlerOfflineSplitData|res:%+v goroutine ctx.Done()", res)
				return
			default:
				// 获取worker资源
				<-fileCfg.offlineWorkerChan
				defer func() {
					wg.Done()
					// 释放worker资源
					fileCfg.offlineWorkerChan <- struct{}{}
				}()
				log.InfoContextf(ctx, "handlerOfflineSplitData|res:%+v goroutine run", res)
				// 解析文件
				docPageContents, tablePageContents, text2SqlResults, err :=
					d.getSplitDataFromCosURL(ctx, shortURLSyncMap, d.storageCli.GetBucket(ctx), res.Result)
				if err != nil {
					log.ErrorContextf(ctx, "handlerOfflineSplitData|getSplitDataFromCosURL failed, "+
						"err:%+v", err)
				}
				parseResultChan <- parseResult{
					index:             res.CurrentFileIndex,
					err:               err,
					docPageContents:   docPageContents,
					tablePageContents: tablePageContents,
					text2SqlResults:   text2SqlResults,
				}
				log.InfoContextf(ctx, "handlerOfflineSplitData|res:%+v goroutine done", res)
			}
		}(ctx, res, shortURLSyncMap)
	}

	// 等待所有解析 goroutine 完成并关闭解析结果channel
	go func(ctx context.Context) {
		defer errors.PanicHandler()
		wg.Wait()
		close(parseResultChan)
		log.InfoContextf(ctx, "handlerOfflineSplitData|all res data complete")
	}(ctx)

	// 顺序处理解析结果
	err = d.dealOfflineSplitResultByOrder(ctx, doc,
		shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap, segmentType,
		parseResultChan, intervene)
	if err != nil {
		log.ErrorContextf(ctx, "handlerOfflineSplitData|dealOfflineSplitResultByOrder failed, err:%+v", err)
		return err
	}
	return nil
}

// dealOfflineSplitResultByOrder 顺序处理拆分结果数据
func (d *dao) dealOfflineSplitResultByOrder(ctx context.Context, doc *model.Doc,
	shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, text2sqlTableIDMap, imageDataSyncMap *sync.Map,
	segmentType uint32, parseResultChan chan parseResult, intervene bool) (err error) {
	log.InfoContextf(ctx, "dealOfflineSplitResultByOrder|run|doc:%+v, segmentType:%d", doc, segmentType)
	nextIndex := int32(0)
	resultMap := make(map[int32]parseResult)
	// 获取干预前的原始切片内容
	oldOrgDataInfos := make([]*model.OldOrgDataInfo, 0)
	currentOrgDataOrder := 0
	if intervene && segmentType == model.SegmentTypeIndex && !model.IsTableTypeDocument(doc.FileType) {
		oldOrgDataInfos, err = d.getPreInterventionOrgData(ctx, doc)
		if err != nil {
			log.ErrorContextf(ctx, "dealOfflineSplitResultByOrder|getPreInterventionOrgData, err:%+v",
				err)
		}
		log.InfoContextf(ctx, "dealOfflineSplitResultByOrder|len(oldOrgDataInfos):%d", len(oldOrgDataInfos))
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
				log.InfoContextf(ctx, "dealOfflineSplitResultByOrder|dealIndex:%d", nextIndex)
				var releaseStatus uint32
				var typ int
				if segmentType == model.SegmentTypeIndex {
					releaseStatus, typ = model.SegmentReleaseStatusInit, model.SegmentTypeIndex
				}
				if segmentType == model.SegmentTypeQA {
					releaseStatus, typ = model.SegmentReleaseStatusNotRequired, model.SegmentTypeQA
				}
				var segments []*model.DocSegmentExtend
				segments, err = d.newOfflineDocSegmentFromPageContent(ctx, doc, result.docPageContents,
					result.tablePageContents, result.text2SqlResults, releaseStatus, text2sqlTableIDMap, typ)
				if err != nil {
					return err
				}
				// 写DB 存储BigData和OrgData
				if err = d.createSegmentWithBigData(ctx,
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
		log.ErrorContextf(ctx, "dealOfflineSplitResultByOrder|failed, err:%+v|len(resultMap):%d, nextIndex:%d",
			err, len(resultMap), nextIndex)
		return err
	}

	log.InfoContextf(ctx, "dealOfflineSplitResultByOrder|done")
	return nil
}

func replacePrefix(org string, title string, renamePrefix string) string {
	if renamePrefix == "" {
		return org
	}
	return strings.Replace(org, title, renamePrefix, 1)
}

// newOfflineDocSegmentFromPageContent 转换为DB存储segment
func (d *dao) newOfflineDocSegmentFromPageContent(ctx context.Context, doc *model.Doc,
	docPageContents, tablePageContents []*pb.PageContent, text2SQLTables *pb.Tables,
	releaseStatus uint32, text2sqlTableIDMap *sync.Map, typ int) ([]*model.DocSegmentExtend, error) {
	title := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	segments := make([]*model.DocSegmentExtend, 0, len(docPageContents))
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
		pgContent := replacePrefix(util.String(pageContent), title, renamePrefix)
		orgData := replacePrefix(pageContent.GetPageContentOrgString(), title, renamePrefix)
		if err := d.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &model.DocSegmentExtend{
			DocSegment: model.DocSegment{
				RobotID:         doc.RobotID,
				CorpID:          doc.CorpID,
				StaffID:         doc.StaffID,
				DocID:           doc.ID,
				Outputs:         "",
				FileType:        doc.FileType,
				SegmentType:     model.SegmentTypeSegment,
				Title:           replacePrefix(title, title, renamePrefix),
				PageContent:     pgContent,
				OrgData:         orgData,
				SplitModel:      "",
				Status:          model.SegmentStatusInit,
				ReleaseStatus:   releaseStatus,
				IsDeleted:       model.SegmentIsNotDeleted,
				Type:            typ,
				NextAction:      model.SegNextActionAdd,
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
				OrgPageNumbers:  d.convertOrgPageNumbers2Str(ctx, pageContent.GetOrgPageNumbers()),
				BigPageNumbers:  d.convertBigPageNumbers2Str(ctx, pageContent.GetBigPageNumbers()),
				SheetData:       d.convertSheetData2Str(ctx, pageContent.GetSheetData()),
			},
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		})
	}
	// 表格
	for _, pageContent := range tablePageContents {
		pgContent := util.String(pageContent)
		orgData := pageContent.GetPageContentOrgString()
		if err := d.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &model.DocSegmentExtend{
			DocSegment: model.DocSegment{
				RobotID:         doc.RobotID,
				CorpID:          doc.CorpID,
				StaffID:         doc.StaffID,
				DocID:           doc.ID,
				Outputs:         "",
				FileType:        doc.FileType,
				SegmentType:     model.SegmentTypeTable,
				Title:           replacePrefix(title, title, renamePrefix),
				PageContent:     pgContent,
				OrgData:         orgData,
				SplitModel:      "",
				Status:          model.SegmentStatusInit,
				ReleaseStatus:   releaseStatus,
				IsDeleted:       model.SegmentIsNotDeleted,
				Type:            typ,
				NextAction:      model.SegNextActionAdd,
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
				OrgPageNumbers:  d.convertOrgPageNumbers2Str(ctx, pageContent.GetOrgPageNumbers()),
				BigPageNumbers:  d.convertBigPageNumbers2Str(ctx, pageContent.GetBigPageNumbers()),
				SheetData:       d.convertSheetData2Str(ctx, pageContent.GetSheetData()),
			},
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		})
	}
	s0 := d.appendText2sqlSegments(ctx, doc, replacePrefix(title, title, renamePrefix), text2SQLTables, releaseStatus, text2sqlTableIDMap, typ)
	if len(s0) > 0 {
		segments = append(segments, s0...)
	}
	return segments, nil
}

// convertOrgPageNumbers2Str 转换切片页码信息
func (d *dao) convertOrgPageNumbers2Str(ctx context.Context, orgPageNumbers []int32) string {
	if len(orgPageNumbers) == 0 {
		return ""
	}
	orgPageStr, err := jsoniter.MarshalToString(orgPageNumbers)
	if err != nil {
		log.ErrorContextf(ctx, "convertOrgPageNumbers2Str|jsoniter.MarshalToString|"+
			"failed|orgPageNumbers:%+v|err:%+v", orgPageNumbers, err)
	}
	return orgPageStr
}

// convertBigPageNumbers2Str 转换切片页码信息
func (d *dao) convertBigPageNumbers2Str(ctx context.Context, bigPageNumbers []int32) string {
	if len(bigPageNumbers) == 0 {
		return ""
	}
	bigPageStr, err := jsoniter.MarshalToString(bigPageNumbers)
	if err != nil {
		log.ErrorContextf(ctx, "convertBigPageNumbers2Str|jsoniter.MarshalToString|"+
			"failed|bigPageNumbers:%+v|err:%+v", bigPageNumbers, err)
	}
	return bigPageStr
}

// convertSheetData2Str 转换切片页码信息
func (d *dao) convertSheetData2Str(ctx context.Context, sheetData []*pb.PageContent_SheetData) string {
	if len(sheetData) == 0 {
		return ""
	}
	for _, data := range sheetData {
		if data == nil {
			log.ErrorContextf(ctx, "convertSheetData2Str|sheetData slice is nil|"+
				"sheetData:%+v", sheetData)
			return ""
		}
	}
	sheetPageStr, err := jsoniter.MarshalToString(sheetData)
	if err != nil {
		log.ErrorContextf(ctx, "convertSheetData2Str|jsoniter.MarshalToString|"+
			"failed|sheetData:%+v|err:%+v", sheetData, err)
	}
	return sheetPageStr
}

// appendText2sqlSegments 追加text2sql的分片
// purpose: 用于QA还是文档索引的Index
func (d *dao) appendText2sqlSegments(ctx context.Context, doc *model.Doc, title string, tableFiles *pb.Tables,
	releaseStatus uint32, text2sqlTableIDMap *sync.Map, purpose int) []*model.DocSegmentExtend {

	segments := make([]*model.DocSegmentExtend, 0)
	if tableFiles == nil {
		log.WarnContextf(ctx, "appendText2sqlSegments|DocID:%d|tableFiles is empty", doc.ID)
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
	log.InfoContextf(ctx, "appendText2sqlSegments|DocID:%d|tableStr:%s", doc.ID, string(tableStr))

	meta, contents, err := d.buildText2sqlSegments(ctx, doc, text2sqlTableIDMap, tableFiles)
	if err != nil {
		log.ErrorContextf(ctx, "appendText2sqlSegments|buildText2sqlSegments|DocID:%d|err:%+v|", doc.ID, err)
		return nil
	}
	m, err := jsoniter.Marshal(meta)
	if err != nil {
		log.ErrorContextf(ctx, "appendText2sqlSegments|DocID:%d|err:%+v|meta:%+v", doc.ID, err, meta)
		return nil
	}
	if len([]rune(string(m))) > config.App().Text2sqlPageContentMaxLength {
		log.WarnContextf(ctx, "appendText2sqlSegments|DocID:%d|meta.length:%d|Text2sqlPageContentMaxLength:%d",
			doc.ID, len([]rune(string(m))), config.App().Text2sqlPageContentMaxLength)
		return nil
	}
	metaSegment := newText2sqlSegments(doc, model.SegmentTypeText2SQLMeta, title, string(m), releaseStatus, purpose)
	segments = append(segments, metaSegment)

	for i, content := range contents {
		c, err := jsoniter.Marshal(content)
		if err != nil {
			log.ErrorContextf(ctx, "appendText2sqlSegments|[%d]|DocID:%d|err:%+v|content:%+v", i, doc.ID, err, content)
			return nil
		}
		if len([]rune(string(c))) > config.App().Text2sqlPageContentMaxLength {
			log.WarnContextf(ctx, "appendText2sqlSegments|DocID:%d|[%d]|content.length:%d|MaxLength:%d",
				doc.ID, i, len([]rune(string(c))), config.App().Text2sqlPageContentMaxLength)
			return nil
		}
		// title 这里可以加上tableName，便于排查问题
		c0 := newText2sqlSegments(doc, model.SegmentTypeText2SQLContent, title, string(c), releaseStatus, purpose)
		segments = append(segments, c0)
	}
	log.InfoContextf(ctx, "appendText2sqlSegments|DocID:%d|contents.len:%d|segments.len:%d", doc.ID,
		len(contents), len(segments))

	return segments
}

func (d *dao) buildText2sqlSegments(ctx context.Context, doc *model.Doc, text2sqlTableIDMap *sync.Map,
	tableFile *pb.Tables) (*model.Text2SQLSegmentMeta, []*model.Text2SQLSegmentContent, error) {

	meta := new(model.Text2SQLSegmentMeta)
	meta.Version = model.Text2sqlVersion1

	contents := make([]*model.Text2SQLSegmentContent, 0)
	// 对应到Excel文件是文件名；
	fileName := tableFile.GetMetaData().GetFileName()
	if len(fileName) == 0 {
		log.ErrorContextf(ctx, "buildText2sqlSegments|fileName is empty|doc:%+v", doc)
		return nil, nil, fmt.Errorf("fileName is empty")
	}

	log.InfoContextf(ctx, "buildText2sqlSegments|fileName:%s|Tables.len:%d", fileName, len(tableFile.GetTables()))

	// 合并相同表名的数据
	tableDataMap := make(map[string]*pb.TableData)
	for i := range tableFile.GetTables() {
		if tableData, ok := tableDataMap[tableFile.GetTables()[i].GetMetaData().GetTableName()]; ok {
			tableData.Rows = append(tableData.Rows, tableFile.GetTables()[i].Rows...)
		} else {
			tableDataMap[tableFile.GetTables()[i].GetMetaData().GetTableName()] = &pb.TableData{
				Rows:     tableFile.GetTables()[i].GetRows(),
				MetaData: tableFile.GetTables()[i].GetMetaData(),
			}
		}
	}
	// 记录表名是否已处理
	tableNames := make(map[string]bool)

	meta.FileName = fileName
	meta.TableMetas = make([]*model.Text2SQLSegmentTableMeta, 0, len(tableFile.GetTables()))
	// 对应到Excel文件是一个个 Sheet
	for ti, table := range tableFile.GetTables() {
		headers := table.GetMetaData().GetHeaders()
		tableName := table.GetMetaData().GetTableName()
		if tableNames[tableName] {
			continue
		}
		tableNames[tableName] = true
		log.InfoContextf(ctx, "buildText2sqlSegments|fileName:%s|[%d]|tableName:%s|headers.len:%d", fileName,
			ti, tableName, len(headers))
		table = tableDataMap[tableName]
		if table == nil {
			log.ErrorContextf(ctx, "buildText2sqlSegments|fileName:%s|[%d]|tableName:%s|table is nil", fileName,
				ti, tableName)
			continue
		}

		key := fmt.Sprintf("%d_%s", doc.ID, table.GetMetaData().GetTableName())
		var tableID uint64
		if tid, ok := text2sqlTableIDMap.Load(key); ok {
			tableID = tid.(uint64)
		} else {
			tableID = d.GenerateSeqID()
			text2sqlTableIDMap.Store(key, tableID)
		}
		tableMeta := &model.Text2SQLSegmentTableMeta{
			TableID:   strconv.FormatUint(tableID, 10),
			TableName: table.GetMetaData().GetTableName(),
			DataType:  model.TableDataType(table.GetMetaData().GetDataType()),
			Headers:   make([]*model.Text2SQLSegmentTableMetaHeader, 0, len(headers)),
			Message:   table.GetMetaData().GetMessage(),
		}
		for hi, header := range headers {
			log.InfoContextf(ctx, "buildText2sqlSegments|fileName:%s|[%d]|tableName:%s|headerType:%s", fileName, hi,
				tableName, header.GetType().String())
			metaHeader := &model.Text2SQLSegmentTableMetaHeader{
				Type: model.TableHeaderType(header.GetType()),
			}
			metaHeader.Rows = make([]*model.Text2SQLRow, 0, len(header.GetRows()))
			for _, row := range header.GetRows() {
				r0 := &model.Text2SQLRow{}
				r0.Cells = make([]*model.Text2SQLCell, 0, len(row.GetCells()))
				for _, cell := range row.GetCells() {
					r0.Cells = append(r0.Cells, &model.Text2SQLCell{
						Value:    cell.GetValue(),
						DataType: model.TableDataCellDataType(cell.GetCellDataType()),
					})
				}
				metaHeader.Rows = append(metaHeader.Rows, r0)
			}
			tableMeta.Headers = append(tableMeta.Headers, metaHeader)
		}
		meta.TableMetas = append(meta.TableMetas, tableMeta)

		for i, row := range table.GetRows() {
			content := new(model.Text2SQLSegmentContent)
			content.Version = model.Text2sqlVersion1
			content.TableID = strconv.FormatUint(tableID, 10)
			content.RowNum = int64(i)
			content.Cells = make([]*model.Text2SQLCell, 0, len(row.GetCells()))
			for _, cell := range row.GetCells() {
				content.Cells = append(content.Cells, &model.Text2SQLCell{
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

func newText2sqlSegments(doc *model.Doc, segmentType, title string, pageContent string, releaseStatus uint32,
	purpose int) *model.DocSegmentExtend {

	return &model.DocSegmentExtend{
		DocSegment: model.DocSegment{
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
			Status:          model.SegmentStatusInit,
			ReleaseStatus:   releaseStatus,
			IsDeleted:       model.SegmentIsNotDeleted,
			Type:            purpose,
			NextAction:      model.SegNextActionAdd,
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
		log.InfoContextf(ctx, "printFileParserPretty|%s|total:%d", dataType, result.GetTotalFileNumber())
		for i, r := range result.GetResult() {
			log.InfoContextf(ctx, "printFileParserPretty|%s|[%d]|r:%+v", dataType, i, r)
		}
	}
}

func (d *dao) checkPageContentAndOrgDataLen(ctx context.Context, docID uint64, pgContent, orgData string) error {
	if utf8.RuneCountInString(pgContent) > config.App().PageContentMaxLength {
		log.ErrorContextf(ctx, "checkPageContentAndOrgDataLen|page_content is too long, length:%d|docID:%d, "+
			"pageContent:%v", utf8.RuneCountInString(pgContent), docID, pgContent)
		return fmt.Errorf("page_content is too long")
	}
	if utf8.RuneCountInString(orgData) > config.App().OrgDataMaxLength {
		log.ErrorContextf(ctx, "checkPageContentAndOrgDataLen|org_data is too long, length:%d|docID:%d, "+
			"orgData:%v", utf8.RuneCountInString(orgData), docID, orgData)
		return fmt.Errorf("org_data is too long")
	}
	return nil
}

// getPreInterventionOrgData 获取干预的切片
func (d *dao) getPreInterventionOrgData(ctx context.Context, doc *model.Doc) ([]*model.OldOrgDataInfo, error) {
	log.InfoContextf(ctx, "getPreInterventionOrgData|start, FileType:%s", doc.FileType)
	oldOrgDataInfos := make([]*model.OldOrgDataInfo, 0)
	// todo在这取值时之前的
	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		log.ErrorContextf(ctx, "getPreInterventionOrgData|failed, err:%+v", err)
		return oldOrgDataInfos, err
	}
	docCommon := &model.DocSegmentCommon{
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
	notDeleteFlag := IsNotDeleted
	deleteFlag := IsDeleted
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:          corpBizID,
		AppBizID:           appBizID,
		DocBizID:           doc.BusinessID,
		IsDeleted:          &notDeleteFlag,
		IsTemporaryDeleted: &deleteFlag,
		RouterAppBizID:     appBizID,
	}
	total, err := GetDocSegmentOrgDataDao().GetDocOrgDataCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|err:%v", err)
		return oldOrgDataInfos, err
	}
	log.InfoContextf(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|total:%d", total)
	for {
		pageNumber++
		list, err := d.GetDocSegmentList(ctx, docCommon, pageNumber, pageSize)
		if err != nil {
			log.ErrorContextf(ctx, "getPreInterventionOrgData|GetDocSegmentList|err:%v", err)
			return oldOrgDataInfos, err
		}
		// 特殊处理，在干预中数据库中数据被修改报错
		if len(list) == 0 {
			skipTimes++
			if skipTimes > deleteDataMaxLimit {
				break
			}
			err = fmt.Errorf("切片数据获取错误或连续删除切片数量超过%d", deleteDataMaxLimit*int(pageSize))
			log.ErrorContextf(ctx, "task(DocSegIntervene) GetDocSegmentList|err:%v", err)
			return oldOrgDataInfos, err
		} else {
			skipTimes = 0
		}
		// 拼接md，上传cos
		for _, seg := range list {
			addMethod := model.AddMethodDefault
			if seg.IsAdd {
				addMethod = model.AddMethodArtificial
			} else if !seg.IsOrigin && !seg.IsAdd {
				addMethod = model.AddMethodEdit
			}
			isDisabled := model.SegmentIsEnable
			if seg.IsDisabled {
				isDisabled = model.SegmentIsDisabled
			}
			oldOrgDataInfos = append(oldOrgDataInfos, &model.OldOrgDataInfo{
				AddMethod:  addMethod,
				IsDisabled: isDisabled,
			})
		}
		count += len(list)
		if int(pageNumber)*int(pageSize) >= int(total) {
			break
		}
	}
	log.InfoContextf(ctx, "getPreInterventionOrgData len(oldOrgDataInfos):%d", len(oldOrgDataInfos))
	return oldOrgDataInfos, nil
}

// GetDocSegmentList logic 复制，待迁移
func (d *dao) GetDocSegmentList(ctx context.Context, docCommon *model.DocSegmentCommon,
	pageNum, pageSize uint32) ([]*knowledge.ListDocSegmentRsp_DocSegmentItem, error) {
	// 1.获取原始切片
	offset := GetOffsetByPage(pageNum, pageSize)
	limit := pageSize
	originList, orgDateBizIDs, err := GetDocSegmentOrgData(ctx, docCommon, offset, limit)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocSegmentOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	// 2.编辑切片内容替换
	editOriginList, err := GetEditOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		log.ErrorContextf(ctx, "GetEditOrgData failed, err:%+v", err)
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
		orgDateBizIDs = append(orgDateBizIDs, model.InsertAtFirst)
	}
	insertOriginList, err := GetInsertOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		log.ErrorContextf(ctx, "GetInsertOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}

	originList, err = InsertIntoOrgDataList(ctx, insertOriginList, originList)
	if err != nil {
		log.ErrorContextf(ctx, "InsertIntoOrgDataList failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	log.InfoContextf(ctx, "GetDocSegmentList|len(originList):%d", len(originList))
	return originList, nil
}

// GetDocSegmentOrgData logic 复制，待迁移
func GetDocSegmentOrgData(ctx context.Context, docCommon *model.DocSegmentCommon,
	offset, limit uint32) ([]*knowledge.ListDocSegmentRsp_DocSegmentItem, []string, error) {
	log.InfoContextf(ctx, "GetDocSegmentOrgData|start|offset:%d|limit:%d", offset, limit)
	orgDataList := make([]*knowledge.ListDocSegmentRsp_DocSegmentItem, 0)
	orgDateBizIDs := make([]string, 0)
	// 由于前面已将上一版本的OrgData逻辑删除，这里需要查找已经删除的OrgData
	notDeleteFlag := IsNotDeleted
	deleteFlag := IsDeleted
	list := make([]*model.DocSegmentOrgData, 0)
	var err error
	filter := &DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		IsDeleted:          &deleteFlag,
		IsTemporaryDeleted: &notDeleteFlag,
		OrderColumn:        []string{DocSegmentOrgDataTblColBusinessID},
		OrderDirection:     []string{SqlOrderByAsc},
		Offset:             offset,
		Limit:              limit,
		RouterAppBizID:     docCommon.AppBizID,
	}
	list, err = GetDocSegmentOrgDataDao().GetDocOrgDataList(ctx,
		DocSegmentOrgDataTblColList, filter)
	if err != nil {
		return orgDataList, orgDateBizIDs, err
	}
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err = jsoniter.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				log.WarnContextf(ctx, "GetDocSegmentOrgData|PageInfos|UnmarshalFromString|err:%+v", err)
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
			IsOrigin:    orgDate.AddMethod == model.AddMethodDefault,
			IsAdd:       orgDate.AddMethod == model.AddMethodArtificial,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled == model.SegmentIsDisabled,
		}
		orgDataList = append(orgDataList, docSegmentItem)
	}
	log.InfoContextf(ctx, "GetDocSegmentOrgData|len(OrgData):%d", len(orgDataList))
	return orgDataList, orgDateBizIDs, nil
}

// GetEditOrgData 获取编辑的切片 logic 复制，待迁移
func GetEditOrgData(ctx context.Context, orgDateBizIDs []string,
	docCommon *model.DocSegmentCommon) ([]*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetEditOrgData|start")
	actionFlag := EditAction
	deletedFlag := IsNotDeleted
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:        docCommon.CorpBizID,
		AppBizID:         docCommon.AppBizID,
		DocBizID:         docCommon.DocBizID,
		IsDeleted:        &deletedFlag,
		Action:           &actionFlag,
		OriginOrgDataIDs: orgDateBizIDs,
		OrderColumn:      []string{DocSegmentOrgDataTemporaryTblColBusinessID},
		OrderDirection:   []string{SqlOrderByAsc},
	}
	originList, err := GetDocSegmentOrgDataTemporaryDao().GetEditOrgData(ctx,
		DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}
	log.InfoContextf(ctx, "GetEditOrgData|len(OrgData):%d", len(originList))
	return originList, nil
}

// GetInsertOrgData 获取插入的切片 logic 复制，待迁移
func GetInsertOrgData(ctx context.Context, orgDateBizIDs []string,
	docCommon *model.DocSegmentCommon) ([]*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetInsertOrgData|start")
	actionFlag := InsertAction
	deletedFlag := IsNotDeleted
	filter := &DocSegmentOrgDataTemporaryFilter{
		CorpBizID:            docCommon.CorpBizID,
		AppBizID:             docCommon.AppBizID,
		DocBizID:             docCommon.DocBizID,
		IsDeleted:            &deletedFlag,
		Action:               &actionFlag,
		LastOriginOrgDataIDs: orgDateBizIDs,
		OrderColumn:          []string{DocSegmentOrgDataTemporaryTblColBusinessID},
		OrderDirection:       []string{SqlOrderByAsc},
	}
	originList, err := GetDocSegmentOrgDataTemporaryDao().GetInsertOrgData(ctx,
		DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetInsertOrgData|err:%v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "GetInsertOrgData|len(OrgData):%d", len(originList))
	return originList, nil
}

// InsertIntoOrgDataList 插入切片 logic 复制，待迁移
func InsertIntoOrgDataList(ctx context.Context, insertOriginList []*model.DocSegmentOrgDataTemporary,
	originList []*knowledge.ListDocSegmentRsp_DocSegmentItem) ([]*knowledge.ListDocSegmentRsp_DocSegmentItem, error) {
	log.InfoContextf(ctx, "InsertIntoOrgDataList|start")
	// 切片内容插入
	// 构建非新增数据映射（用于快速查找）
	originMap := make(map[string]struct{})
	for _, originSeg := range originList {
		originMap[originSeg.SegBizId] = struct{}{}
	}
	originMap[model.InsertAtFirst] = struct{}{}

	// 构建新增数据映射（key: last_org_data_id, value: 切片）
	insertMap := make(map[string]*model.DocSegmentOrgDataTemporary)
	for _, insertSeg := range insertOriginList {
		insertMap[insertSeg.LastOrgDataID] = insertSeg
	}

	// 收集指向非新增数据中的起点节点
	startSegs := make([]*model.DocSegmentOrgDataTemporary, 0)
	for _, insertSeg := range insertOriginList {
		if _, exists := originMap[insertSeg.LastOrgDataID]; exists {
			startSegs = append(startSegs, insertSeg)
		}
	}

	// 按非新增数据分组存储插入数据
	segChains := make(map[string][]*model.DocSegmentOrgDataTemporary)
	for _, startSeg := range startSegs {
		originSegID := startSeg.LastOrgDataID
		chain := []*model.DocSegmentOrgDataTemporary{startSeg}

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
	if segChain, exists := segChains[model.InsertAtFirst]; exists {
		for _, orgData := range segChain {
			docSegmentItem := &knowledge.ListDocSegmentRsp_DocSegmentItem{
				SegBizId:    orgData.BusinessID,
				OrgData:     orgData.OrgData,
				PageInfos:   []uint64{},
				IsOrigin:    false,
				IsAdd:       true,
				SegmentType: "",
				IsDisabled:  orgData.IsDisabled == model.SegmentIsDisabled,
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
					IsDisabled:  orgData.IsDisabled == model.SegmentIsDisabled,
					AuditStatus: uint64(orgData.AuditStatus),
				}
				finalSegs = append(finalSegs, docSegmentItem)
			}
		}
	}
	log.InfoContextf(ctx, "InsertIntoOrgDataList|len(finalSegs):%d", len(finalSegs))
	return finalSegs, nil
}

// GetOffsetByPage common 复制，待迁移
func GetOffsetByPage(pageNumber uint32, pageSize uint32) uint32 {
	if pageSize == 0 {
		return 0
	}
	offset := (pageNumber - 1) * pageSize
	return offset
}
