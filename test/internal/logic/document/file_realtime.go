package document

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity/realtime"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/adp/pb-go/kb/parse_engine/file_parse_common"
)

func pageContentToString(c *pb.PageContent) string {
	r := c.Prefix
	for _, v := range c.Head {
		r = append(r, v.Runes...)
	}
	for _, v := range c.Body {
		r = append(r, v.Runes...)
	}
	for _, v := range c.Tail {
		r = append(r, v.Runes...)
	}
	return string(r)
}

// parseRealtimeDocTaskResult 解析实时文档解析结果
// 1. 结果文件下载并解析
// 2. 解析结果同步数据库
func (l *Logic) parseRealtimeDocTaskResult(ctx context.Context, doc *realtime.TRealtimeDoc) (err error) {
	logx.I(ctx, "parseRealtimeDocTaskResult|doc:%+v", doc)
	// 获取解析结果COS URL
	if doc.Result == "" {
		return errs.ErrDocParseCosURLNotFound
	}
	result := &file_parse_common.ParseRsp{}
	err = jsonx.UnmarshalFromString(doc.Result, result)
	if err != nil {
		logx.E(ctx, "parseRealtimeDocTaskResult|jsonx.UnmarshalFromString failed, err:%+v", err)
		return err
	}
	printTaskResPretty(ctx, result)
	logx.I(ctx, "parseRealtimeDocTaskResult|taskResult:%+v", result)
	resultDataMap := result.GetResults()

	// 全局数据（需要保证并发安全）
	// 短链接
	shortURLSyncMap := &sync.Map{}
	// 创建哈希表存储唯一字符串和BigData的ID
	bigDataSyncMap := &sync.Map{}
	// 切片图片表存储唯一图片URL和图片ID
	imageDataSyncMap := &sync.Map{}

	wg := sync.WaitGroup{}
	wg.Add(3)

	errChan := make(chan error, 3)

	// 处理split数据
	go func() {
		defer gox.Recover()
		// 解析拆分的文档片段
		logx.I(ctx, "parseRealtimeDocTaskResult|handler TYPE_DOC")
		if docTypeData, ok := resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_DOC)]; ok {
			if err := l.handlerRealtimeSplitData(ctx, doc, shortURLSyncMap, bigDataSyncMap, imageDataSyncMap, docTypeData); err != nil {
				errChan <- err
			}
		} else {
			logx.I(ctx, "parseRealtimeDocTaskResult|handler TYPE_DOC|data is nil, ignore")
		}
		wg.Done()
	}()
	go func() {
		defer gox.Recover()
		// 解析拆分的表格片段
		logx.I(ctx, "parseRealtimeDocTaskResult|handler TYPE_ES_TABLE")
		if esTableData, ok := resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_ES_TABLE)]; ok {
			if err := l.handlerRealtimeSplitData(ctx, doc, shortURLSyncMap, bigDataSyncMap, imageDataSyncMap, esTableData); err != nil {
				errChan <- err
			}
		} else {
			logx.I(ctx, "parseRealtimeDocTaskResult|handler TYPE_ES_TABLE|data is nil, ignore")
		}
		wg.Done()
	}()

	// 处理parse数据
	go func() {
		defer gox.Recover()
		// 解析拆分的文档全文（只有满足长度要求才会返回）
		logx.I(ctx, "parseRealtimeDocTaskResult|handler TYPE_FULL_TEXT")
		if fullTextData, ok := resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_FULL_TEXT)]; ok {
			if err := l.handlerRealtimeParseData(ctx, doc, shortURLSyncMap, fullTextData); err != nil {
				errChan <- err
			}
		} else {
			logx.I(ctx, "parseRealtimeDocTaskResult|handler TYPE_FULL_TEXT|data is nil, ignore")
		}
		wg.Done()
	}()

	wg.Wait()

	for {
		select {
		case err = <-errChan:
			logx.E(ctx, "parseRealtimeDocTaskResult|failed|err:%+v", err)
			return err
		default:
			logx.I(ctx, "parseRealtimeDocTaskResult|success|doc:%+v", doc)
			return nil
		}
	}
}

// handlerRealtimeSplitData 处理拆分数据
func (l *Logic) handlerRealtimeSplitData(ctx context.Context, doc *realtime.TRealtimeDoc,
	shortURLSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map,
	data *file_parse_common.DataResult) (err error) {
	logx.I(ctx, "handlerRealtimeSplitData|doc:%+v, data:%+v", doc, data)
	if data == nil {
		logx.I(ctx, "handlerRealtimeSplitData|data is nil, ignore")
		return nil
	}
	if data.TotalFileNumber == 0 || data.TotalFileNumber != int32(len(data.Results)) {
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

	for _, res := range data.Results {
		go func(ctx context.Context, res *file_parse_common.Result, shortURLSyncMap *sync.Map) {
			defer gox.Recover()
			logx.I(ctx, "handlerRealtimeSplitData|res:%+v goroutine init", res)
			select {
			case <-ctx.Done():
				wg.Done()
				logx.I(ctx, "handlerRealtimeSplitData|res:%+v goroutine ctx.Done()", res)
				return
			default:
				// 获取worker资源
				<-fileCfg.realtimeWorkerChan
				defer func() {
					wg.Done()
					// 释放worker资源
					fileCfg.realtimeWorkerChan <- struct{}{}
				}()
				logx.I(ctx, "handlerRealtimeSplitData|res:%+v goroutine run", res)
				// 解析文件
				docPageContents, tablePageContents, _, err :=
					l.getSplitDataFromCosURL(ctx, shortURLSyncMap, doc.Bucket, res.Result)
				if err != nil {
					logx.E(ctx, "handlerRealtimeSplitData|getSplitDataFromCosURL failed, "+
						"err:%+v", err)
				}
				parseResultChan <- parseResult{
					index:             res.CurrentFileIndex,
					err:               err,
					docPageContents:   docPageContents,
					tablePageContents: tablePageContents,
				}
				logx.I(ctx, "handlerRealtimeSplitData|res:%+v goroutine done", res)
			}
		}(ctx, res, shortURLSyncMap)
	}

	// 等待所有解析 goroutine 完成并关闭解析结果channel
	go func(ctx context.Context) {
		defer gox.Recover()
		wg.Wait()
		close(parseResultChan)
		logx.I(ctx, "handlerRealtimeSplitData|all res data complete")
	}(ctx)

	// 顺序处理解析结果
	err = l.dealRealtimeSplitResultByOrder(ctx, doc,
		shortURLSyncMap, bigDataSyncMap, imageDataSyncMap, parseResultChan)
	if err != nil {
		logx.E(ctx, "handlerRealtimeSplitData|dealRealtimeSplitResultByOrder failed, err:%+v", err)
		return err
	}
	return nil
}

// dealRealtimeSplitResultByOrder 顺序处理拆分结果数据
func (l *Logic) dealRealtimeSplitResultByOrder(ctx context.Context, doc *realtime.TRealtimeDoc,
	shortURLSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map, parseResultChan chan parseResult) (err error) {
	logx.I(ctx, "dealRealtimeSplitResultByOrder|run|doc:%+v", doc)

	tx := l.segDao.Query()

	nextIndex := int32(0)
	resultMap := make(map[int32]parseResult)

	for resultChan := range parseResultChan {
		if resultChan.err != nil {
			return resultChan.err
		}

		resultMap[resultChan.index] = resultChan

		// 顺序处理
		for {
			if result, ok := resultMap[nextIndex]; ok {
				logx.I(ctx, "dealRealtimeSplitResultByOrder|dealIndex:%d", nextIndex)
				var segments []*realtime.TRealtimeDocSegment
				segments, err = l.newRealtimeDocSegmentFromPageContent(ctx, doc,
					result.docPageContents, result.tablePageContents)
				if err != nil {
					return err
				}
				// 写DB
				if err = l.txBatchCreateRealtimeSegment(ctx, tx,
					shortURLSyncMap, bigDataSyncMap, imageDataSyncMap, segments); err != nil {
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
		logx.E(ctx, "dealRealtimeSplitResultByOrder|failed, err:%+v|len(resultMap):%d, nextIndex:%d",
			err, len(resultMap), nextIndex)
		return err
	}

	logx.I(ctx, "dealRealtimeSplitResultByOrder|done")
	return nil
}

// handlerRealtimeParseData 处理解析数据
func (l *Logic) handlerRealtimeParseData(ctx context.Context, doc *realtime.TRealtimeDoc,
	shortURLSyncMap *sync.Map, data *file_parse_common.DataResult) (err error) {
	logx.I(ctx, "handlerRealtimeParseData|doc:%+v, data:%+v", doc, data)
	if data == nil {
		logx.I(ctx, "handlerRealtimeParseData|data is nil, ignore")
		return nil
	}
	// 文档全文解析出来应该只有一个文件
	if data.TotalFileNumber != 1 || data.TotalFileNumber != int32(len(data.Results)) {
		err = fmt.Errorf("data:%+v illegal", data)
		return err
	}
	res := data.Results[0]

	logx.I(ctx, "handlerRealtimeParseData|res:%+v goroutine init", res)

	// 获取worker资源
	<-fileCfg.realtimeWorkerChan
	defer func() {
		// 释放worker资源
		fileCfg.realtimeWorkerChan <- struct{}{}
	}()
	logx.I(ctx, "handlerRealtimeParseData|res:%+v goroutine run", res)

	docFullText, err := l.getParseDataFromCosURL(ctx, shortURLSyncMap, doc.Bucket, res.Result)
	if err != nil {
		logx.E(ctx, "handlerRealtimeParseData|getParseDataFromCosURL failed, err:%+v", err)
		return err
	}
	doc.FileFullText = docFullText

	// 更新DB
	err = l.updateRealtimeDoc(ctx, doc)
	if err != nil {
		logx.E(ctx, "handlerRealtimeParseData|updateRealtimeDoc failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "handlerRealtimeParseData|res:%+v goroutine done", res)
	return nil
}

// newRealtimeDocSegmentFromPageContent 转换为DB存储segment
func (l *Logic) newRealtimeDocSegmentFromPageContent(ctx context.Context, doc *realtime.TRealtimeDoc,
	docPageContents, tablePageContents []*pb.PageContent) ([]*realtime.TRealtimeDocSegment, error) {
	title := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	segments := make([]*realtime.TRealtimeDocSegment, 0, len(docPageContents))
	// 文档
	for _, pageContent := range docPageContents {
		pgContent := pageContentToString(pageContent)
		orgData := pageContent.GetPageContentOrgString()
		if err := l.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &realtime.TRealtimeDocSegment{
			SegmentID:       idgen.GetId(),
			SessionID:       doc.SessionID,
			DocID:           doc.DocID,
			RobotID:         doc.RobotID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			FileType:        doc.FileType,
			SegmentType:     segment.SegmentTypeSegment,
			Title:           title,
			PageContent:     pgContent,
			OrgData:         orgData,
			SplitModel:      "",
			IsSyncKnowledge: unSyncKnowledge,
			IsDeleted:       segment.SegmentIsNotDeleted,
			RichTextIndex:   int(pageContent.GetRichContentId()),
			StartChunkIndex: int(pageContent.GetOrgStart()),
			EndChunkIndex:   int(pageContent.GetOrgEnd()),
			LinkerKeep:      pageContent.GetLinkerKeep(),
			BigStart:        pageContent.GetBigStart(),
			BigEnd:          pageContent.GetBigEnd(),
			BigString:       pageContent.GetPageContentBigString(),
			Images:          pageContent.GetImages(),
			CreateTime:      time.Now(),
			UpdateTime:      time.Now(),
		})
	}
	// 表格
	for _, pageContent := range tablePageContents {
		pgContent := pageContentToString(pageContent)
		orgData := pageContent.GetPageContentOrgString()
		if err := l.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &realtime.TRealtimeDocSegment{
			SegmentID:       idgen.GetId(),
			SessionID:       doc.SessionID,
			DocID:           doc.DocID,
			RobotID:         doc.RobotID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			FileType:        doc.FileType,
			SegmentType:     segment.SegmentTypeTable,
			Title:           title,
			PageContent:     pgContent,
			OrgData:         orgData,
			SplitModel:      "",
			IsSyncKnowledge: unSyncKnowledge,
			IsDeleted:       segment.SegmentIsNotDeleted,
			RichTextIndex:   int(pageContent.GetRichContentId()),
			StartChunkIndex: int(pageContent.GetOrgStart()),
			EndChunkIndex:   int(pageContent.GetOrgEnd()),
			LinkerKeep:      pageContent.GetLinkerKeep(),
			BigStart:        pageContent.GetBigStart(),
			BigEnd:          pageContent.GetBigEnd(),
			BigString:       pageContent.GetPageContentBigString(),
			Images:          pageContent.GetImages(),
			CreateTime:      time.Now(),
			UpdateTime:      time.Now(),
		})
	}
	return segments, nil
}

// printTaskResPretty 打印解析结果
func printTaskResPretty(ctx context.Context, result *file_parse_common.ParseRsp) {
	for k, v := range result.GetResults() {
		dataType := pb.FileParserSubDataType_name[k]
		logx.I(ctx, "printTaskResPretty|%s|total:%d", dataType, v.GetTotalFileNumber())
		for i, r := range v.GetResults() {
			logx.I(ctx, "printTaskResPretty|%s|[%d]|r:%+v", dataType, i, r)
		}
	}
}
