// Package dao TODO
// @Author: halelv
// @Date: 2024/5/24 21:25
package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/common/v3/errors"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/realtime"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util/db"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"git.woa.com/dialogue-platform/proto/pb-stub/nrt_file_parser_server"
	jsoniter "github.com/json-iterator/go"
)

// parseRealtimeDocTaskResult 解析实时文档解析结果
// 1. 结果文件下载并解析
// 2. 解析结果同步数据库
func (d *dao) parseRealtimeDocTaskResult(ctx context.Context, doc *realtime.TRealtimeDoc) (err error) {
	log.InfoContextf(ctx, "parseRealtimeDocTaskResult|doc:%+v", doc)
	// 获取解析结果COS URL
	if doc.Result == "" {
		return errs.ErrDocParseCosURLNotFound
	}
	result := &nrt_file_parser_server.TaskRes{}
	err = jsoniter.UnmarshalFromString(doc.Result, result)
	if err != nil {
		log.ErrorContextf(ctx, "parseRealtimeDocTaskResult|jsoniter.UnmarshalFromString failed, err:%+v", err)
		return err
	}
	printTaskResPretty(ctx, result)
	log.InfoContextf(ctx, "parseRealtimeDocTaskResult|taskResult:%+v", result)
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
		defer errors.PanicHandler()
		// 解析拆分的文档片段
		log.InfoContextf(ctx, "parseRealtimeDocTaskResult|handler TYPE_DOC")
		if err := d.handlerRealtimeSplitData(ctx, doc, shortURLSyncMap, bigDataSyncMap, imageDataSyncMap,
			resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_DOC)]); err != nil {
			errChan <- err
		}
		wg.Done()
	}()
	go func() {
		defer errors.PanicHandler()
		// 解析拆分的表格片段
		log.InfoContextf(ctx, "parseRealtimeDocTaskResult|handler TYPE_ES_TABLE")
		if err := d.handlerRealtimeSplitData(ctx, doc, shortURLSyncMap, bigDataSyncMap, imageDataSyncMap,
			resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_ES_TABLE)]); err != nil {
			errChan <- err
		}
		wg.Done()
	}()

	// 处理parse数据
	go func() {
		defer errors.PanicHandler()
		// 解析拆分的文档全文（只有满足长度要求才会返回）
		log.InfoContextf(ctx, "parseRealtimeDocTaskResult|handler TYPE_FULL_TEXT")
		if err := d.handlerRealtimeParseData(ctx, doc, shortURLSyncMap,
			resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_FULL_TEXT)]); err != nil {
			errChan <- err
		}
		wg.Done()
	}()

	wg.Wait()

	for {
		select {
		case err = <-errChan:
			log.ErrorContextf(ctx, "parseRealtimeDocTaskResult|failed|err:%+v", err)
			return err
		default:
			log.InfoContextf(ctx, "parseRealtimeDocTaskResult|success|doc:%+v", doc)
			return nil
		}
	}
}

// handlerRealtimeSplitData 处理拆分数据
func (d *dao) handlerRealtimeSplitData(ctx context.Context, doc *realtime.TRealtimeDoc,
	shortURLSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map,
	data *nrt_file_parser_server.TaskRes_DataResult) (err error) {
	log.InfoContextf(ctx, "handlerRealtimeSplitData|doc:%+v, data:%+v", doc, data)
	if data == nil {
		log.InfoContextf(ctx, "handlerRealtimeSplitData|data is nil, ignore")
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
		go func(ctx context.Context, res *nrt_file_parser_server.TaskRes_Result, shortURLSyncMap *sync.Map) {
			defer errors.PanicHandler()
			log.InfoContextf(ctx, "handlerRealtimeSplitData|res:%+v goroutine init", res)
			select {
			case <-ctx.Done():
				wg.Done()
				log.InfoContextf(ctx, "handlerRealtimeSplitData|res:%+v goroutine ctx.Done()", res)
				return
			default:
				// 获取worker资源
				<-fileCfg.realtimeWorkerChan
				defer func() {
					wg.Done()
					// 释放worker资源
					fileCfg.realtimeWorkerChan <- struct{}{}
				}()
				log.InfoContextf(ctx, "handlerRealtimeSplitData|res:%+v goroutine run", res)
				// 解析文件
				docPageContents, tablePageContents, _, err :=
					d.getSplitDataFromCosURL(ctx, shortURLSyncMap, doc.Bucket, res.Result)
				if err != nil {
					log.ErrorContextf(ctx, "handlerRealtimeSplitData|getSplitDataFromCosURL failed, "+
						"err:%+v", err)
				}
				parseResultChan <- parseResult{
					index:             res.CurrentFileIndex,
					err:               err,
					docPageContents:   docPageContents,
					tablePageContents: tablePageContents,
				}
				log.InfoContextf(ctx, "handlerRealtimeSplitData|res:%+v goroutine done", res)
			}
		}(ctx, res, shortURLSyncMap)
	}

	// 等待所有解析 goroutine 完成并关闭解析结果channel
	go func(ctx context.Context) {
		defer errors.PanicHandler()
		wg.Wait()
		close(parseResultChan)
		log.InfoContextf(ctx, "handlerRealtimeSplitData|all res data complete")
	}(ctx)

	// 顺序处理解析结果
	err = d.dealRealtimeSplitResultByOrder(ctx, doc,
		shortURLSyncMap, bigDataSyncMap, imageDataSyncMap, parseResultChan)
	if err != nil {
		log.ErrorContextf(ctx, "handlerRealtimeSplitData|dealRealtimeSplitResultByOrder failed, err:%+v", err)
		return err
	}
	return nil
}

// dealRealtimeSplitResultByOrder 顺序处理拆分结果数据
func (d *dao) dealRealtimeSplitResultByOrder(ctx context.Context, doc *realtime.TRealtimeDoc,
	shortURLSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map, parseResultChan chan parseResult) (err error) {
	log.InfoContextf(ctx, "dealRealtimeSplitResultByOrder|run|doc:%+v", doc)

	tx := db.BeginDBTx(ctx, d.gormDB).Debug()
	defer func() {
		// 事务的提交或者回滚
		txErr := db.CommitOrRollbackTx(ctx, tx, err)
		if txErr != nil {
			err = txErr
		}
	}()

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
				log.InfoContextf(ctx, "dealRealtimeSplitResultByOrder|dealIndex:%d", nextIndex)
				var segments []*realtime.TRealtimeDocSegment
				segments, err = d.newRealtimeDocSegmentFromPageContent(ctx, doc,
					result.docPageContents, result.tablePageContents)
				if err != nil {
					return err
				}
				// 写DB
				if err = d.txBatchCreateRealtimeSegment(ctx, tx,
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
		log.ErrorContextf(ctx, "dealRealtimeSplitResultByOrder|failed, err:%+v|len(resultMap):%d, nextIndex:%d",
			err, len(resultMap), nextIndex)
		return err
	}

	log.InfoContextf(ctx, "dealRealtimeSplitResultByOrder|done")
	return nil
}

// handlerRealtimeParseData 处理解析数据
func (d *dao) handlerRealtimeParseData(ctx context.Context, doc *realtime.TRealtimeDoc,
	shortURLSyncMap *sync.Map, data *nrt_file_parser_server.TaskRes_DataResult) (err error) {
	log.InfoContextf(ctx, "handlerRealtimeParseData|doc:%+v, data:%+v", doc, data)
	if data == nil {
		log.InfoContextf(ctx, "handlerRealtimeParseData|data is nil, ignore")
		return nil
	}
	// 文档全文解析出来应该只有一个文件
	if data.TotalFileNumber != 1 || data.TotalFileNumber != int32(len(data.Result)) {
		err = fmt.Errorf("data:%+v illegal", data)
		return err
	}
	res := data.Result[0]

	log.InfoContextf(ctx, "handlerRealtimeParseData|res:%+v goroutine init", res)

	// 获取worker资源
	<-fileCfg.realtimeWorkerChan
	defer func() {
		// 释放worker资源
		fileCfg.realtimeWorkerChan <- struct{}{}
	}()
	log.InfoContextf(ctx, "handlerRealtimeParseData|res:%+v goroutine run", res)

	docFullText, err := d.getParseDataFromCosURL(ctx, shortURLSyncMap, doc.Bucket, res.Result)
	if err != nil {
		log.ErrorContextf(ctx, "handlerRealtimeParseData|getParseDataFromCosURL failed, err:%+v", err)
		return err
	}
	doc.FileFullText = docFullText

	// 更新DB
	err = d.updateRealtimeDoc(ctx, doc)
	if err != nil {
		log.ErrorContextf(ctx, "handlerRealtimeParseData|updateRealtimeDoc failed, err:%+v", err)
		return err
	}
	log.InfoContextf(ctx, "handlerRealtimeParseData|res:%+v goroutine done", res)
	return nil
}

// newRealtimeDocSegmentFromPageContent 转换为DB存储segment
func (d *dao) newRealtimeDocSegmentFromPageContent(ctx context.Context, doc *realtime.TRealtimeDoc,
	docPageContents, tablePageContents []*pb.PageContent) ([]*realtime.TRealtimeDocSegment, error) {
	title := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	segments := make([]*realtime.TRealtimeDocSegment, 0, len(docPageContents))
	// 文档
	for _, pageContent := range docPageContents {
		pgContent := util.String(pageContent)
		orgData := pageContent.GetPageContentOrgString()
		if err := d.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &realtime.TRealtimeDocSegment{
			SegmentID:       d.GenerateSeqID(),
			SessionID:       doc.SessionID,
			DocID:           doc.DocID,
			RobotID:         doc.RobotID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			FileType:        doc.FileType,
			SegmentType:     model.SegmentTypeSegment,
			Title:           title,
			PageContent:     pgContent,
			OrgData:         orgData,
			SplitModel:      "",
			IsSyncKnowledge: unSyncKnowledge,
			IsDeleted:       model.SegmentIsNotDeleted,
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
		pgContent := util.String(pageContent)
		orgData := pageContent.GetPageContentOrgString()
		if err := d.checkPageContentAndOrgDataLen(ctx, doc.ID, pgContent, orgData); err != nil {
			return nil, err
		}
		segments = append(segments, &realtime.TRealtimeDocSegment{
			SegmentID:       d.GenerateSeqID(),
			SessionID:       doc.SessionID,
			DocID:           doc.DocID,
			RobotID:         doc.RobotID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			FileType:        doc.FileType,
			SegmentType:     model.SegmentTypeTable,
			Title:           title,
			PageContent:     pgContent,
			OrgData:         orgData,
			SplitModel:      "",
			IsSyncKnowledge: unSyncKnowledge,
			IsDeleted:       model.SegmentIsNotDeleted,
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
func printTaskResPretty(ctx context.Context, result *nrt_file_parser_server.TaskRes) {
	for k, v := range result.GetResults() {
		dataType := pb.FileParserSubDataType_name[k]
		log.InfoContextf(ctx, "printTaskResPretty|%s|total:%d", dataType, v.GetTotalFileNumber())
		for i, r := range v.GetResult() {
			log.InfoContextf(ctx, "printTaskResPretty|%s|[%d]|r:%+v", dataType, i, r)
		}
	}
}
