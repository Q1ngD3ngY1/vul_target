// Package dao 实时文档相关
// @Author: halelv
// @Date: 2024/5/15 18:03
package dao

import (
	"context"
	"crypto/sha256"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"io"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/linker"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/realtime"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/errors"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"git.woa.com/dialogue-platform/proto/pb-stub/nrt_file_parser_server"
	"github.com/avast/retry-go"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

const (
	defaultInsertDBBatchSize          = 500 // 默认DB分批插入数量
	defaultSyncAddVectorBatchSize     = 20  // 默认插入Vector分批同步数量
	defaultSyncDeletedVectorBatchSize = 100 // 默认删除Vector分批同步数量
	defaultSyncVectorMaxRetry         = 3   // 默认Vector重试次数

	defaultFullTextMaxSize = 32000 // 默认文档全文提取的最大字数 32k

	defaultRealtimeFileManagerVersion = 2 // 默认实时文档解析服务版本号 2

	defaultRealtimeTickerSecond = 5 // 默认实时文档解析进度回包间隔（单位：秒）

	unSyncKnowledge = 0 // 还未同步知识
	syncedKnowledge = 1 // 已经同步知识

	realtimeKnowledgeType = 2 // 实时文档知识类型

	// RealtimeSessionIDLabel TODO
	RealtimeSessionIDLabel = "SessionID" // 实时文档SessionID标签
	// RealtimeDocIDLabel TODO
	RealtimeDocIDLabel = "DocID" // 实时文档DocID标签
)

const (
	// urlPattern 实时文档路径正则匹配
	urlPattern = `/corp/\d+/%d/doc/[\p{L}\p{P}]+`
)

// CheckRealtimeStorageInfo 校验实时文档存储信息
func (d *dao) CheckRealtimeStorageInfo(ctx context.Context, bucket, url, eTag string, app *model.App) error {
	log.InfoContextf(ctx, "CheckRealtimeStorageInfo|bucket:%s, url:%s, eTag:%s, app.BusinessIds:%d",
		bucket, url, eTag, app.BusinessID)
	// bucket校验
	realtimeBucket, err := d.GetBucketWithTypeKey(ctx, model.RealtimeStorageTypeKey)
	if err != nil {
		return err
	}
	if realtimeBucket != bucket {
		return errs.ErrInvalidURL
	}
	// url校验
	if !isRealtimeURLLegal(url, app.BusinessID) {
		return errs.ErrInvalidURL
	}
	// 文件校验
	if len(eTag) == 0 {
		err = fmt.Errorf("url:%s, eTag:%s is empty", url, eTag)
		return err
	}
	// 这里objectInfo.ETag的结果会带有转义字符 类似 "\"5784a190d6af4214020f54edc87429ab\""
	// 需要对转义字符特殊处理
	objectInfo, err := d.StatObjectWithTypeKey(ctx, model.RealtimeStorageTypeKey, url)
	if err != nil {
		return err
	}
	e1, err := strconv.Unquote(eTag)
	if err == nil {
		eTag = e1
	}
	e2, err := strconv.Unquote(objectInfo.ETag)
	if err == nil {
		objectInfo.ETag = e2
	}
	if eTag != objectInfo.ETag {
		err = fmt.Errorf("url:%s, objectInfo.ETag:%s, eTag:%s illegal", url, objectInfo.ETag, eTag)
		return err
	}
	return nil
}

func isRealtimeURLLegal(url string, appBizID uint64) bool {
	// 正则匹配
	regex := fmt.Sprintf(urlPattern, appBizID)
	match, _ := regexp.MatchString(regex, url)
	return match
}

// GetRealtimeDocByID 根据DocID查询实时文档
func (d *dao) GetRealtimeDocByID(ctx context.Context, docID uint64) (*realtime.TRealtimeDoc, error) {
	log.InfoContextf(ctx, "GetRealtimeDocByID|docID:%d", docID)
	var docs []*realtime.TRealtimeDoc
	err := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDoc{}).
		Where("doc_id = ? and is_deleted = ?", docID, model.DocIsNotDeleted).
		Find(&docs).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetRealtimeDocByID|gormDB.Find failed, err:%+v", err)
		return nil, err
	}
	if len(docs) != 1 {
		return nil, errs.ErrDocNotFound
	}
	return docs[0], nil
}

// ParseRealtimeDoc 提交实时文档解析
func (d *dao) ParseRealtimeDoc(ctx context.Context, reqCh <-chan *realtime.ParseDocReqChan,
	rspCh chan<- *realtime.ParseDocRspChan) error {
	log.InfoContextf(ctx, "ParseRealtimeDoc|called")

	// 准实时解析引擎cli
	opts := []client.Option{WithTrpcSelector()}
	cli, err := d.nrtFileManagerCli.StreamParse(ctx, opts...)
	if err != nil {
		log.ErrorContextf(ctx, "ParseRealtimeDoc|d.nrtFileManagerCli.StreamParse err:%+v", err)
		return err
	}

	// 是否接收中
	isReceiving := false
	// 停止信号
	stopSignal := make(chan struct{}, 1)
	defer func() {
		// 如果没有启动接收，在这里关闭rspCh
		if !isReceiving {
			close(rspCh)
			log.InfoContextf(ctx, "ParseRealtimeDoc|rspCh closed")
		}
		if cli != nil {
			_ = cli.CloseSend()
		}
	}()

	// 实时文档 全局使用
	doc := &realtime.TRealtimeDoc{}

	for {
		select {
		case <-ctx.Done():
			log.InfoContextf(ctx, "ParseRealtimeDoc|<-ctx.Done()")
			return nil
		case req, ok := <-reqCh:
			if !ok {
				log.InfoContextf(ctx, "ParseRealtimeDoc|reqCh closed")
				return nil
			}
			log.InfoContextf(ctx, "ParseRealtimeDoc|req:%+v", req)
			// 填充实时文档信息
			if err := d.fillRealtimeDocByUrlID(ctx, doc, req.Doc.SessionID, req.Doc.CosUrlID); err != nil {
				log.ErrorContextf(ctx, "ParseRealtimeDoc|fillRealtimeDocByUrlID failed, err:%+v", err)
				return err
			}
			// 组装实时解析请求
			nrtReq, err := d.getNrtReqAndFillDoc(ctx, req.Type, doc)
			if err != nil {
				log.ErrorContextf(ctx, "ParseRealtimeDoc|getNrtReqAndFillDoc failed, err:%+v", err)
				return err
			}
			if err = d.updateRealtimeDoc(ctx, doc); err != nil {
				log.ErrorContextf(ctx, "ParseRealtimeDoc|updateRealtimeDoc failed, err:%+v", err)
				return err
			}
			// 请求实时解析引擎
			if err = cli.Send(nrtReq); err != nil {
				log.ErrorContextf(ctx, "ParseRealtimeDoc|cli.Send failed, err:%+v", err)
				doc.Status = realtime.RealDocStatusFailed
				doc.Message = fmt.Sprintf("cli.Send failed, err:%+v", err)
				_ = d.updateRealtimeDoc(ctx, doc)
				return err
			}
			log.InfoContextf(ctx, "ParseRealtimeDoc|cli.Send success")
			if nrtReq.GetRequestType() == nrt_file_parser_server.NRTReq_TASK_CANCEL {
				stopSignal <- struct{}{}
			}
			if !isReceiving {
				// 保证接收启动
				wg := sync.WaitGroup{}
				wg.Add(1)

				go func() {
					defer errors.PanicHandler()
					d.handlerNrtReceive(ctx, req.ModelName, &wg, doc, cli, rspCh, stopSignal)
				}()

				wg.Wait()
				isReceiving = true
			}
		}
	}
}

// fillRealtimeDocByUrlID 根据UrlID查询实时文档
func (d *dao) fillRealtimeDocByUrlID(ctx context.Context, doc *realtime.TRealtimeDoc,
	sessionID string, cosUrlID uint64) (err error) {
	log.InfoContextf(ctx, "fillRealtimeDocByUrlID|doc:%+v, sessionID:%s, cosUrlID:%d",
		doc, sessionID, cosUrlID)
	if doc == nil {
		err = fmt.Errorf("dos is nill")
		log.ErrorContextf(ctx, "fillRealtimeDocByUrlID|failed, err:%+v", err)
		return err
	}
	var docs []*realtime.TRealtimeDoc
	err = d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDoc{}).
		Where("session_id = ? and cos_url_id = ? and is_deleted = ?",
			sessionID, cosUrlID, model.DocIsNotDeleted).
		Find(&docs).Error
	if err != nil {
		log.ErrorContextf(ctx, "fillRealtimeDocByUrlID|gormDB.Find failed, err:%+v", err)
		return err
	}
	if len(docs) != 1 {
		err = fmt.Errorf("has duplicate docs or docs is empty")
		log.ErrorContextf(ctx, "fillRealtimeDocByUrlID|len(docs):%d|failed, err:%+v", len(docs), err)
		return err
	}
	// 填充实时文档信息
	// 指针传递地址不能直接赋值这里要使用参数替换的方式
	realtime.FillRealtimeDocInfo(doc, docs[0])
	log.InfoContextf(ctx, "fillRealtimeDocByUrlID|suucess|doc:%+v", doc)
	return nil
}

// getNrtReqAndFillDoc 组装实时文档解析请求
func (d *dao) getNrtReqAndFillDoc(ctx context.Context, taskType knowledge.StreamSaveDocReq_ReqType,
	doc *realtime.TRealtimeDoc) (nrtReq *nrt_file_parser_server.NRTReq, err error) {
	nrtReq = &nrt_file_parser_server.NRTReq{}
	// 任务类型校验
	switch taskType {
	case knowledge.StreamSaveDocReq_TASK_PARSE:
		if !doc.CanParse() {
			err = fmt.Errorf("doc:%+v cannot parse", doc)
			log.ErrorContextf(ctx, "getNrtReqAndFillDoc|err:%+v", err)
			return nil, err
		}
		nrtReq.RequestType = nrt_file_parser_server.NRTReq_TASK_REQ
		doc.Status = realtime.RealDocStatusParsing
	case knowledge.StreamSaveDocReq_TASK_CANCEL:
		if !doc.CanCancel() {
			err = fmt.Errorf("doc:%+v cannot cancel", doc)
			log.ErrorContextf(ctx, "getNrtReqAndFillDoc|err:%+v", err)
			return nil, err
		}
		nrtReq.RequestType = nrt_file_parser_server.NRTReq_TASK_CANCEL
		doc.Status = realtime.RealDocStatusCancel
	default:
		err = fmt.Errorf("illegal taskType:%v", taskType)
		log.ErrorContextf(ctx, "getNrtReqAndFillDoc|err:%+v", err)
		return nil, err
	}
	// 组装请求
	robot, err := d.GetAppByID(ctx, doc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "getNrtReqAndFillDoc|GetAppByID failed, err:%+v", err)
		return nil, err
	}
	if robot == nil {
		return nil, errs.ErrRobotNotFound
	}
	// 拆分策略
	splitStrategy, err := d.getRealtimeSplitStrategy(ctx, robot, doc)
	if err != nil {
		return nil, err
	}
	// 实时文档解析版本号
	fileManagerVersion := utilConfig.GetMainConfig().FileParseConfig.RealtimeFileManagerVersion
	if fileManagerVersion <= 0 {
		fileManagerVersion = defaultRealtimeFileManagerVersion
	}
	nrtReq.TaskReq = &nrt_file_parser_server.TaskReq{
		RequestId: trace.SpanContextFromContext(ctx).TraceID().String(),
		AppInfo: &nrt_file_parser_server.AppInfo{
			AppKey: fmt.Sprintf("%d", robot.BusinessID),
			Biz:    fileManagerBiz,
		},
		CurrentOpType: nrt_file_parser_server.TaskReq_SPLIT,
		FileInfo:      doc.GetTaskFileInfo(),
		ParseSetting: &nrt_file_parser_server.ParseSetting{
			ParseMode: nrt_file_parser_server.ParseSetting_ParseModel(
				utilConfig.GetMainConfig().FileParseConfig.RealtimeParseSetting.ParseMode),
			IsOpenSubimg:  utilConfig.GetMainConfig().FileParseConfig.RealtimeParseSetting.IsOpenSubimg,
			IsOpenFormula: utilConfig.GetMainConfig().FileParseConfig.RealtimeParseSetting.IsOpenFormula,
		},
		SplitStrategy: []*nrt_file_parser_server.SplitStrategy{splitStrategy},
		MetaData: &nrt_file_parser_server.TaskReq_MetaData{
			Version: int32(fileManagerVersion),
		},
	}
	doc.RequestID = nrtReq.GetTaskReq().GetRequestId()
	doc.OpType = int32(nrtReq.GetRequestType())
	log.InfoContextf(ctx, "getNrtReqAndFillDoc|nrtReq:%+v, doc:%+v", nrtReq, doc)
	return nrtReq, err
}

// getRealtimeSplitStrategy 获取实时文档解析配置
func (d *dao) getRealtimeSplitStrategy(ctx context.Context, appDB *model.AppDB, doc *realtime.TRealtimeDoc) (
	*nrt_file_parser_server.SplitStrategy, error) {
	// 拆分策略
	splitStrategyStr, err := d.getRobotSplitStrategy(ctx, appDB, doc.FileName)
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimeSplitStrategy|getRobotSplitStrategy failed, err:%+v", err)
		return nil, err
	}
	var splitStrategy nrt_file_parser_server.SplitStrategy
	err = jsoniter.UnmarshalFromString(splitStrategyStr, &splitStrategy)
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimeSplitStrategy|jsoniter.UnmarshalFromString failed, err:%+v", err)
		return nil, err
	}
	// 文档全文提取最大字数配置
	fullTextMaxSize := utilConfig.GetMainConfig().RealtimeConfig.FullTextMaxSize
	if fullTextMaxSize <= 0 {
		fullTextMaxSize = defaultFullTextMaxSize
	}
	splitStrategy.ModelSplitConfig = &nrt_file_parser_server.SplitStrategy_ModelSplitConfig{
		ReturnFullTextMaxLength: int32(fullTextMaxSize),
	}
	// 文档解析器配置
	splitStrategy.ParserConfig = &nrt_file_parser_server.SplitStrategy_ParserConfig{
		SingleParagraph: utilConfig.GetMainConfig().FileParseConfig.RealtimeParseSetting.ParserConfig.SingleParagraph,
		SplitSubTable:   utilConfig.GetMainConfig().FileParseConfig.RealtimeParseSetting.ParserConfig.SplitSubTable,
	}
	return &splitStrategy, nil
}

// updateRealtimeDoc 根据UrlID更新实时文档
func (d *dao) updateRealtimeDoc(ctx context.Context, doc *realtime.TRealtimeDoc) error {
	log.InfoContextf(ctx, "updateRealtimeDoc|doc:%+v", doc)
	err := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDoc{}).
		Where("doc_id = ? and is_deleted = ?", doc.DocID, model.DocIsNotDeleted).
		Updates(map[string]interface{}{
			"char_size":        doc.CharSize,
			"file_full_text":   doc.FileFullText,
			"message":          doc.Message,
			"status":           doc.Status,
			"request_id":       doc.RequestID,
			"task_id":          doc.TaskID,
			"op_type":          doc.OpType,
			"result":           doc.Result,
			"task_status":      doc.TaskStatus,
			"progress":         doc.Progress,
			"progress_message": doc.ProgressMessage,
			"update_time":      time.Now(),
		}).Error
	if err != nil {
		log.ErrorContextf(ctx, "updateRealtimeDoc|gormDB.Updates failed, err:%+v", err)
		return err
	}
	return err
}

// handlerNrtReceive 处理实时解析引擎回包
func (d *dao) handlerNrtReceive(ctx context.Context, modelName string, wg *sync.WaitGroup,
	doc *realtime.TRealtimeDoc, cli nrt_file_parser_server.ManagerObj_StreamParseClient,
	rspCh chan<- *realtime.ParseDocRspChan, stopSignal chan struct{}) {
	log.InfoContextf(ctx, "handlerNrtReceive|called")
	wg.Done()

	// 如果已经启动接收，在这里关闭rspCh
	defer func() {
		close(rspCh)
		log.InfoContextf(ctx, "handlerNrtReceive|rspCh closed")
	}()

	for {
		select {
		case <-ctx.Done():
			log.InfoContextf(ctx, "handlerNrtReceive|<-ctx.Done()")
			cancelReq, err := d.getNrtReqAndFillDoc(ctx, knowledge.StreamSaveDocReq_TASK_CANCEL, doc)
			if err != nil {
				return
			}
			_ = d.updateRealtimeDoc(ctx, doc)
			_ = cli.Send(cancelReq)
			return
		case <-stopSignal:
			log.InfoContextf(ctx, "handlerNrtReceive|stopSignal|return")
			doc.Status = realtime.RealDocStatusCancel
			_ = d.updateRealtimeDoc(ctx, doc)
			return
		default:
			nrtRsp, err := cli.Recv()
			if err == nil && nrtRsp != nil {
				// 转换解析结果
				d.parseNrtRsp(ctx, doc, nrtRsp)
				log.InfoContextf(ctx, "handlerNrtReceive|parseNrtRsp|doc:%+v", doc)
			}
			if err == io.EOF {
				log.InfoContextf(ctx, "handlerNrtReceive|cli.Recv EOF return")
				if doc.IsFinalStatus() {
					return
				}
			}
			if err != nil {
				log.ErrorContextf(ctx, "handlerNrtReceive|cli.Recv faield, err:%+v", err)
				if !doc.IsFinalStatus() {
					doc.Status = realtime.RealDocStatusFailed
					doc.Message = fmt.Sprintf("handlerNrtReceive|cli.Recv failed, err:%+v", err)
				}
			}
			err = d.updateRealtimeDoc(ctx, doc)
			if err != nil {
				log.ErrorContextf(ctx, "handlerNrtReceive|updateRealtimeDoc failed, err:%+v", err)
				doc.Status = realtime.RealDocStatusFailed
				doc.Message += fmt.Sprintf("updateRealtimeDoc failed, err:%+v", err)
			}
			// 结果解析
			docSummary, statisticInfo, err := d.handlerDocTaskResult(ctx, modelName, doc, rspCh)
			if err != nil {
				// 这里做一次更新
				_ = d.updateRealtimeDoc(ctx, doc)
			}
			log.InfoContextf(ctx, "outer docSummary :%+v", docSummary)
			// 回包
			handlerRspCh(ctx, docSummary, statisticInfo, doc, rspCh)
			// 是否结束
			if err != nil || doc.IsFinalStatus() {
				log.InfoContextf(ctx, "handlerNrtReceive|cli.Recv return|err:%+v, doc.Status:%s",
					err, doc.Status)
				return
			}
		}
	}
}

// parseNrtRsp 转换解析结果
func (d *dao) parseNrtRsp(ctx context.Context, doc *realtime.TRealtimeDoc, nrtRsp *nrt_file_parser_server.NRTRsp) {
	log.InfoContextf(ctx, "parseNrtRsp|nrtRsp:%+v", nrtRsp)
	log.InfoContextf(ctx, "parseNrtRsp|doc:%+v", doc)
	if doc.IsFinalStatus() {
		log.InfoContextf(ctx, "parseNrtRsp|doc IsFinalStatus ignore|doc.Status:%s", doc.Status)
		return
	}
	doc.RequestID = nrtRsp.GetTaskRsp().GetRequestId()
	doc.TaskID = nrtRsp.GetTaskRsp().GetTaskId()
	doc.TaskStatus = nrtRsp.GetTaskRsp().GetStatusCode()
	doc.Message = nrtRsp.GetTaskRsp().GetMessage()
	if doc.TaskStatus == realtime.TaskStatusSuccess {
		switch nrtRsp.ResponseType {
		case nrt_file_parser_server.NRTRsp_PROGRESS:
			doc.Status = realtime.RealDocStatusParsing
			doc.Progress = nrtRsp.GetTaskRsp().GetProgress().GetProgress()
			doc.ProgressMessage = nrtRsp.GetTaskRsp().GetProgress().GetMessage()
			return
		case nrt_file_parser_server.NRTRsp_TASK_RSP:
			doc.Progress = nrtRsp.GetTaskRsp().GetProgress().GetProgress()
			doc.ProgressMessage = nrtRsp.GetTaskRsp().GetProgress().GetMessage()
			doc.CharSize = nrtRsp.GetTaskRsp().GetWordCount()
			if doc.CharSize <= 0 {
				log.ErrorContextf(ctx, "parseNrtRsp|nrtRsp.GetTaskRsp().GetWordCount():%d illegal",
					nrtRsp.GetTaskRsp().GetWordCount())
				doc.Status = realtime.RealDocStatusFailed
				doc.Message += fmt.Sprintf("parseNrtRsp doc.CharSize <= 0")
				return
			}
			// 解析结果存储
			result, err := jsoniter.MarshalToString(nrtRsp.GetTaskRsp())
			if err != nil {
				log.ErrorContextf(ctx, "parseNrtRsp|jsoniter.MarshalToString failed, err:%+v", err)
				doc.Status = realtime.RealDocStatusFailed
				doc.Message += fmt.Sprintf("parseNrtRsp jsoniter.MarshalToString failed, err:%+v", err)
				return
			}
			doc.Status = realtime.RealDocStatusLearning
			doc.Result = result
			return
		default:
			log.ErrorContextf(ctx, "parseNrtRsp|illegal rsp type:%v",
				nrtRsp.ResponseType)
			doc.Status = realtime.RealDocStatusFailed
			doc.Message += fmt.Sprintf("parseNrtRsp illegal rsp type:%v", nrtRsp.ResponseType)
			return
		}
	} else {
		doc.Status = realtime.RealDocStatusFailed
		doc.Message += fmt.Sprintf("parseNrtRsp failed taskStatus:%v", doc.TaskStatus)
		return
	}
}

// handlerDocTaskResult 处理解析结果
func (d *dao) handlerDocTaskResult(ctx context.Context, modelName string, doc *realtime.TRealtimeDoc, rspCh chan<- *realtime.ParseDocRspChan) (docSummary string, statisticInfo *knowledge.StatisticInfo, err error) {
	if doc.Status == realtime.RealDocStatusLearning {
		// 更新进度 Learning
		doc.Progress = realtime.RealDocLearningProgress
		doc.ProgressMessage = realtime.RealDocLearningDesc
		err = d.updateRealtimeDoc(ctx, doc)
		if err != nil {
			log.ErrorContextf(ctx, "handlerDocTaskResult|updateRealtimeDoc failed, err:%+v", err)
			doc.Status = realtime.RealDocStatusFailed
			doc.Message += fmt.Sprintf("updateRealtimeDoc failed, err:%+v", err)
		} else {
			// 开始定时回包
			wg := sync.WaitGroup{}
			wg.Add(1)

			// 停止信号
			stopSignal := make(chan struct{}, 1)

			go func() {
				defer errors.PanicHandler()
				defer wg.Done()

				realtimeTickerSecond := utilConfig.GetMainConfig().RealtimeConfig.RealtimeTickerSecond
				if realtimeTickerSecond <= 0 {
					realtimeTickerSecond = defaultRealtimeTickerSecond
				}
				log.InfoContextf(ctx, "handlerDocTaskResult|handlerRspCh|begin|realtimeTickerSecond:%d",
					realtimeTickerSecond)

				ticker := time.NewTicker(time.Duration(realtimeTickerSecond) * time.Second)
				defer ticker.Stop()

				for {
					select {
					case <-stopSignal:
						log.InfoContextf(ctx, "handlerDocTaskResult|handlerRspCh|stop")
						return
					case <-ticker.C:
						log.InfoContextf(ctx, "handlerDocTaskResult|handlerRspCh|ticker")
						// 回包
						if !doc.IsFinalStatus() {
							// 在文档未到终态时发送进度信息
							handlerRspCh(ctx, "", nil, doc, rspCh)
						}
					}
				}
			}()

			// 解析结果学习
			docSummary, statisticInfo, err = d.parseDocTaskResult(ctx, modelName, doc)
			if err != nil {
				log.ErrorContextf(ctx, "handlerDocTaskResult|parseDocTaskResult failed, err:%+v", err)
				doc.Status = realtime.RealDocStatusFailed
				doc.Message += fmt.Sprintf("parseDocTaskResult failed, err:%+v", err)
			}

			// 终止定时回包
			stopSignal <- struct{}{}

			wg.Wait()
		}
		return docSummary, statisticInfo, err
	}
	return docSummary, statisticInfo, nil
}

// handlerRspCh 组装回包
func handlerRspCh(ctx context.Context, docSummary string, statisticInfo *knowledge.StatisticInfo, doc *realtime.TRealtimeDoc, rspCh chan<- *realtime.ParseDocRspChan) {
	rsp := doc.ConvertToParseDocRspChan(docSummary, statisticInfo)
	log.InfoContextf(ctx, "handlerRspCh|rsp:%+v", rsp)
	rspCh <- rsp
}

// parseDocTaskResult 解析任务结果
// 1. 解析文件
// 2. 同步DB
// 3. 同步Vector
// 4. 更新文档状态
func (d *dao) parseDocTaskResult(ctx context.Context, modelName string, doc *realtime.TRealtimeDoc) (summary string, statisticInfo *knowledge.StatisticInfo, err error) {
	log.InfoContextf(ctx, "parseDocTaskResult|doc:%+v", doc)
	if doc.Status != realtime.RealDocStatusLearning {
		err = fmt.Errorf("doc status:%s is not learning", doc.Status)
		return "", nil, err
	}
	// 0. 解析前清理一次旧数据
	err = d.deletedRealtimeSegmentByDocIDs(ctx, doc.RobotID, []uint64{doc.DocID})
	if err != nil {
		log.ErrorContextf(ctx, "parseDocTaskResult|deletedRealtimeSegmentByDocIDs failed, err:%+v", err)
		return "", nil, err
	}
	// 1. 解析文件
	// 2. 同步DB
	err = d.parseRealtimeDocTaskResult(ctx, doc)
	if err != nil {
		log.ErrorContextf(ctx, "parseDocTaskResult|parseRealtimeDocTaskResult failed, err:%+v", err)
		return "", nil, err
	}

	// 摘要与同步vector并行处理
	wg := sync.WaitGroup{}
	wg.Add(2)
	var errChan = make(chan error, 2) // 可以存储两个错误

	needSummaryAppIDMap := utilConfig.GetMainConfig().RealtimeConfig.NeedSummaryAppID
	// 摘要处理
	// 20250207, 最新逻辑， 默认文档不做摘要【摘要很慢】，在走摘要appid白名单内的才走摘要逻辑
	go func() {
		defer errors.PanicHandler()
		defer wg.Done()

		appInfo, err := d.GetAppByID(ctx, doc.RobotID)
		if err != nil {
			errChan <- err
			return
		}
		if appInfo == nil {
			return
		}
		ctx = pkg.WithSpaceID(ctx, appInfo.SpaceID)
		// 特定appid,才需要走摘要逻辑
		if _, ok := needSummaryAppIDMap[appInfo.BusinessID]; !ok {
			log.InfoContextf(ctx, "parseDocTaskResult|NeedSummaryAppID,appID:%+v", appInfo.BusinessID)
			return
		}
		summary, statisticInfo, err = d.handleDocSummary(ctx, modelName, doc)
		if err != nil {
			log.ErrorContextf(ctx, "parseDocTaskResult|handleDocSummary failed,err:%+v", err)
			errChan <- err
			return
		}
	}()

	go func() {
		defer errors.PanicHandler()
		// 3. 同步Vector
		defer wg.Done()
		if err = d.batchSyncRealtimeKnowledge(ctx, doc.RobotID, doc.DocID); err != nil {
			log.ErrorContextf(ctx, "parseDocTaskResult|batchSyncRealtimeKnowledge failed, err:%+v", err)
			errChan <- err // 发送错误到 channel
			return
		}
	}()
	wg.Wait()
	close(errChan) // 关闭 channel，表示不会再发送更多错误

	// 检查是否有错误发生
	for err := range errChan {
		if err != nil {
			return "", nil, err // 如果有错误，返回第一个接收到的错误
		}
	}

	// 继续后续步骤
	// 4. 更新文档状态
	unSyncSegments, err := d.getUnSyncKnowledgeSegment(ctx, doc.DocID, defaultInsertDBBatchSize)
	if err != nil {
		log.ErrorContextf(ctx, "parseDocTaskResult|getUnSyncKnowledgeSegment failed, err:%+v", err)
		return "", nil, err
	}
	if len(unSyncSegments) > 0 {
		err = fmt.Errorf("parseDocTaskResult|still has unSyncKnowledgeSegment, docID:%d", doc.DocID)
		return "", nil, err
	}
	if doc.Status == realtime.RealDocStatusLearning {
		doc.Status = realtime.RealDocStatusSuccess
		// 更新进度 Success
		doc.Progress = realtime.RealDocSuccessProgress
		doc.ProgressMessage = realtime.RealDocSuccessDesc
	}
	err = d.updateRealtimeDoc(ctx, doc)
	if err != nil {
		log.ErrorContextf(ctx, "handlerNrtReceive|updateRealtimeDoc failed, err:%+v", err)
		doc.Message += fmt.Sprintf("updateRealtimeDoc failed, err:%+v", err)
		return "", nil, err
	}
	return summary, statisticInfo, nil
}

// txBatchCreateRealtimeSegment 事物批量写入RealtimeDocSegment到DB
func (d *dao) txBatchCreateRealtimeSegment(ctx context.Context, tx *gorm.DB,
	shortURLSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map, segments []*realtime.TRealtimeDocSegment) error {
	batchSize := utilConfig.GetMainConfig().RealtimeConfig.InsertDBBatchSize
	if batchSize <= 0 {
		batchSize = defaultInsertDBBatchSize
	}
	log.InfoContextf(ctx, "txBatchCreateRealtimeSegment|len(segments):%d, batchSize:%d",
		len(segments), batchSize)
	if len(segments) == 0 {
		return nil
	}
	total := len(segments)
	batches := int(math.Ceil(float64(total) / float64(batchSize)))
	for i := 0; i < batches; i++ {
		start := batchSize * i
		end := batchSize * (i + 1)
		if end > total {
			end = total
		}
		tmpSegments := segments[start:end]

		var bigData []*retrieval.BigData

		var images []*realtime.TRealtimeDocSegmentImage

		for _, tmpSegment := range tmpSegments {
			// bigData数据
			tmpSegmentBigData, err := d.getRealtimeDocSegmentBigData(ctx, bigDataSyncMap, tmpSegment)
			if err != nil {
				return err
			}
			if tmpSegmentBigData != nil {
				bigData = append(bigData, tmpSegmentBigData)
			}

			// image数据
			tmpSegmentImages, err := d.getRealtimeDocSegmentImages(ctx, shortURLSyncMap, imageDataSyncMap, tmpSegment)
			if err != nil {
				return err
			}
			if len(tmpSegmentImages) > 0 {
				images = append(images, tmpSegmentImages...)
			}
		}

		// 写DB
		log.InfoContextf(ctx, "txBatchCreateRealtimeSegment|len(tmpSegments):%d", len(tmpSegments))
		if len(tmpSegments) > 0 {
			err := tx.Model(&realtime.TRealtimeDocSegment{}).Create(tmpSegments).Error
			if err != nil {
				log.InfoContextf(ctx, "txBatchCreateRealtimeSegment|seg:%+v|Create failed, err:%+v",
					tmpSegments, err)
				return err
			}
		}
		log.InfoContextf(ctx, "txBatchCreateRealtimeSegment|len(images):%d", len(images))
		if len(images) > 0 {
			err := tx.Model(&realtime.TRealtimeDocSegmentImage{}).Create(images).Error
			if err != nil {
				log.InfoContextf(ctx, "txBatchCreateRealtimeSegment|seg:%+v|Create failed, err:%+v",
					tmpSegments, err)
				return err
			}
		}

		// 写ES
		log.InfoContextf(ctx, "txBatchCreateRealtimeSegment|len(bigData):%d", len(bigData))
		if len(bigData) > 0 {
			if err := d.AddBigDataElastic(ctx, bigData, retrieval.KnowledgeType_REALTIME); err != nil {
				log.ErrorContextf(ctx, "CreateSegment|AddBigDataElastic|seg:%+v|err:%+v", tmpSegments, err)
				return err
			}
		}
	}
	log.InfoContextf(ctx, "txBatchCreateRealtimeSegment|success")
	return nil
}

// getRealtimeDocSegmentBigData 获取实时文档切片BigData
func (d *dao) getRealtimeDocSegmentBigData(ctx context.Context, bigDataSyncMap *sync.Map,
	segment *realtime.TRealtimeDocSegment) (*retrieval.BigData, error) {
	if len(segment.BigString) == 0 {
		log.WarnContextf(ctx, "getRealtimeDocSegmentBigData|segment:%+v|BigString is empty", segment)
		return nil, nil
	}
	hash := sha256.New()
	_, _ = io.WriteString(hash, segment.BigString)
	hashValue := hash.Sum(nil)
	// 2024-04-14:
	// 	为了确保 模型解出来的结果有不同的拆分策略（规则拆分、模型拆分），所以加上rich_text_index
	//	uniqueKey := strconv.Itoa(tmpSegment.RichTextIndex) + string(hashValue)
	// 2024-04-15: mobisysfeng, tangyuanlin, harryhlli 沟通后决定，只用 BigString 即可；
	uniqueKey := string(hashValue)
	// 长文本通过hash，只把不重复的存入ES
	if id, ok := bigDataSyncMap.Load(uniqueKey); ok {
		segment.BigDataID = id.(string)
		return nil, nil
	} else {
		segment.BigDataID = strconv.FormatUint(d.GenerateSeqID(), 10) // 生成ES的ID
		bigDataSyncMap.Store(uniqueKey, segment.BigDataID)
		return &retrieval.BigData{
			RobotId:   segment.RobotID,
			DocId:     segment.DocID,
			BigDataId: segment.BigDataID,
			BigStart:  segment.BigStart,
			BigEnd:    segment.BigEnd,
			BigString: segment.BigString,
		}, nil
	}
}

// getRealtimeDocSegmentImages 获取实时文档切片Images
func (d *dao) getRealtimeDocSegmentImages(ctx context.Context, shortURLSyncMap, imageDataSyncMap *sync.Map,
	segment *realtime.TRealtimeDocSegment) ([]*realtime.TRealtimeDocSegmentImage, error) {
	if len(segment.Images) == 0 {
		log.InfoContextf(ctx, "getRealtimeDocSegmentImages|segment:%+v|Images is empty", segment)
		return nil, nil
	}
	segmentImages := make([]*realtime.TRealtimeDocSegmentImage, 0)
	for _, originalUrl := range segment.Images {
		if originalUrl == "" {
			log.WarnContextf(ctx, "getRealtimeDocSegmentImages|segment:%+v|originalUrl is empty", segment)
			continue
		}
		//imageID := uint64(0)
		//if id, ok := imageDataSyncMap.Load(originalUrl); ok {
		//	imageID = id.(uint64)
		//} else {
		//	imageID = d.GenerateSeqID()
		//	imageDataSyncMap.Store(originalUrl, imageID)
		//}
		// 2.4.0 @harryhlli @jouislu 结论：相同图片也用不同图片ID
		imageID := d.GenerateSeqID()
		externalUrl := ""
		URL, err := url.Parse(originalUrl)
		if err != nil || URL.Path == "" {
			log.ErrorContextf(ctx, "getRealtimeDocSegmentImages|segment:%+v|originalUrl:%s parse res:%+v err:%+v",
				segment, originalUrl, URL, err)
			return nil, fmt.Errorf("originalUrl is illegal")
		}
		oldURL := URL.Scheme + "://" + URL.Host + URL.Path
		if value, ok := shortURLSyncMap.Load(oldURL); ok {
			newURL := value.(string)
			externalUrl = strings.ReplaceAll(originalUrl, oldURL, newURL)
		} else {
			log.ErrorContextf(ctx, "getRealtimeDocSegmentImages|segment:%+v|externalUrl is empty", segment)
			return nil, fmt.Errorf("externalUrl is empty")
		}
		segmentImages = append(segmentImages, &realtime.TRealtimeDocSegmentImage{
			ImageID:     imageID,
			SegmentID:   segment.SegmentID,
			DocID:       segment.DocID,
			RobotID:     segment.RobotID,
			CorpID:      segment.CorpID,
			StaffID:     segment.StaffID,
			OriginalUrl: originalUrl,
			ExternalUrl: externalUrl,
			IsDeleted:   segment.IsDeleted,
			CreateTime:  time.Now(),
			UpdateTime:  time.Now(),
		})
	}
	return segmentImages, nil
}

// batchSyncRealtimeKnowledge 批量同步RealtimeDocSegment到Vector
func (d *dao) batchSyncRealtimeKnowledge(ctx context.Context, robotID, docID uint64) error {
	batchSize := utilConfig.GetMainConfig().RealtimeConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	retryCount := utilConfig.GetMainConfig().RealtimeConfig.SyncVectorMaxRetry
	if retryCount <= 0 {
		retryCount = defaultSyncVectorMaxRetry
	}
	log.InfoContextf(ctx, "batchSyncRealtimeKnowledge|docID:%d, retryCount:%d", docID, retryCount)
	// 带失败重试
	err := retry.Do(
		func() error {
			appDB, err := d.GetAppByID(ctx, robotID)
			if err != nil {
				return err
			}
			embeddingConf, _, err := appDB.GetEmbeddingConf()
			if err != nil {
				return err
			}
			log.InfoContextf(ctx, "batchSyncRealtimeKnowledge|embeddingConf:%+v", embeddingConf)

			segments, err := d.getUnSyncKnowledgeSegment(ctx, docID, batchSize)
			if err != nil {
				return err
			}
			for len(segments) > 0 {
				log.InfoContextf(ctx, "batchSyncRealtimeKnowledge|len(segments):%d", len(segments))
				// 批量写向量到Vector
				err = d.directAddRealtimeSegmentKnowledge(ctx, segments, embeddingConf.Version)
				if err != nil {
					return err
				}
				// 更新Vector同步状态
				err = d.updateSegmentSyncKnowledge(ctx, segments, syncedKnowledge)
				if err != nil {
					return err
				}
				segments, err = d.getUnSyncKnowledgeSegment(ctx, docID, batchSize)
				if err != nil {
					return err
				}
			}
			return nil
		},
		retry.Attempts(uint(retryCount)),
		retry.OnRetry(func(n uint, err error) {
			log.ErrorContextf(ctx, "batchSyncRealtimeKnowledge|retry:%d, err:%v", n, err)
		}),
	)
	if err != nil {
		log.ErrorContextf(ctx, "batchSyncRealtimeKnowledge|retry.Do failed, err:%+v", err)
		return err
	}
	log.InfoContextf(ctx, "batchSyncRealtimeKnowledge|success")
	return nil
}

// handleDocSummary 获取文档摘要
func (d *dao) handleDocSummary(ctx context.Context, modelName string, doc *realtime.TRealtimeDoc) (summary string, statisticInfo *knowledge.StatisticInfo, err error) {
	request := &knowledge.GetDocSummaryReq{
		BotBizId:    doc.RobotID,
		ModelName:   modelName,
		PromptLimit: uint32(d.GetModelPromptLimit(ctx, doc.CorpID, modelName)),
		Query:       "",
	}
	summaryInfo, err := d.GetOneDocSummary(ctx, request, doc.DocID, doc.FileName)
	if err != nil {
		log.ErrorContextf(ctx, "SimpleChat error: %v", err)
		return "", nil, err
	}
	summary = summaryInfo.GetDocSummary()
	if len(summaryInfo.GetStatisticInfos()) > 0 {
		statisticInfo = summaryInfo.StatisticInfos[0]
	}

	log.InfoContextf(ctx, "docSummary:%s,statisticInfo:%+v", summary, statisticInfo)
	return summary, statisticInfo, nil

}

// getUnSyncKnowledgeSegment 获取未同步Vector的segment
func (d *dao) getUnSyncKnowledgeSegment(ctx context.Context, docID uint64, limit int) (
	[]*realtime.TRealtimeDocSegment, error) {
	log.InfoContextf(ctx, "getUnSyncKnowledgeSegment|docID:%d, limit:%d", docID, limit)
	var docSegments []*realtime.TRealtimeDocSegment
	err := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDocSegment{}).
		Where("doc_id = ? and is_sync_knowledge = ? and is_deleted = ?",
			docID, unSyncKnowledge, model.SegmentIsNotDeleted).
		Limit(limit).Find(&docSegments).Error
	if err != nil {
		log.ErrorContextf(ctx, "getUnSyncKnowledgeSegment|gormDB.Find failed, err:%+v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "getUnSyncKnowledgeSegment|success|len(docSegments):%d", len(docSegments))
	return docSegments, nil
}

// directAddRealtimeSegmentKnowledge 新增实时文档分片知识
func (d *dao) directAddRealtimeSegmentKnowledge(ctx context.Context, segments []*realtime.TRealtimeDocSegment,
	embeddingVersion uint64) error {
	log.InfoContextf(ctx, "directAddRealtimeSegmentKnowledge|len(segments):%d, embeddingVersion:%d",
		len(segments), embeddingVersion)
	if len(segments) == 0 {
		return nil
	}
	robotID, knowledgeData, err := d.getRealtimeKnowledgeData(ctx, segments)
	if err != nil {
		log.ErrorContextf(ctx, "directAddRealtimeSegmentKnowledge|getRealtimeKnowledgeData failed, err:%+v", err)
		return err
	}
	botBizID, err := d.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "directAddRealtimeSegmentKnowledge|GetBotBizIDByID:%+v err:%+v", robotID, err)
		return err
	}
	req := &retrieval.AddRealTimeKnowledgeReq{
		RobotId:          robotID,
		IndexId:          model.RealtimeSegmentVersionID,
		DocType:          realtimeKnowledgeType,
		EmbeddingVersion: embeddingVersion,
		Knowledge:        knowledgeData,
		BotBizId:         botBizID,
	}
	log.InfoContextf(ctx, "directAddRealtimeSegmentKnowledge|req:%+v", req)
	rsp, err := d.directIndexCli.AddRealTimeKnowledge(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "directAddRealtimeSegmentKnowledge|directIndexCli.AddRealTimeKnowledge failed, "+
			"err:%+v", err)
		return err
	}
	log.InfoContextf(ctx, "directAddRealtimeSegmentKnowledge|rsp:%+v", rsp)
	return nil
}

// directDeleteRealtimeSegmentKnowledge 删除实时文档分片知识
func (d *dao) directDeleteRealtimeSegmentKnowledge(ctx context.Context, segments []*realtime.TRealtimeDocSegment,
	embeddingVersion uint64) error {
	log.InfoContextf(ctx, "directDeleteRealtimeSegmentKnowledge|len(segments):%d, embeddingVersion:%d",
		len(segments), embeddingVersion)
	if len(segments) == 0 {
		return nil
	}
	robotID, knowledgeData, err := d.getRealtimeKnowledgeData(ctx, segments)
	if err != nil {
		log.ErrorContextf(ctx, "directDeleteRealtimeSegmentKnowledge|getRealtimeKnowledgeData failed, "+
			"err:%+v", err)
		return err
	}
	botBizID, err := d.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "directDeleteRealtimeSegmentKnowledge|GetBotBizIDByID:%+v err:%+v", robotID, err)
		return err
	}
	dataIDTypes := make([]*retrieval.KnowledgeIDType, 0)
	for _, data := range knowledgeData {
		dataIDTypes = append(dataIDTypes, &retrieval.KnowledgeIDType{
			Id:          data.Id,
			SegmentType: data.SegmentType,
		})
	}
	// 做个批次处理
	batchSize := utilConfig.GetMainConfig().RealtimeConfig.SyncVectorDeletedBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncDeletedVectorBatchSize
	}
	// 计算需要的批次数量
	numBatches := len(dataIDTypes) / batchSize
	if len(dataIDTypes)%batchSize != 0 {
		numBatches++
	}
	for i := 0; i < numBatches; i++ {
		// 计算当前批次的起始和结束索引
		startIdx := i * batchSize
		endIdx := startIdx + batchSize
		if endIdx > len(dataIDTypes) {
			endIdx = len(dataIDTypes)
		}

		// 根据批次创建数据ID的子切片
		dataBatch := dataIDTypes[startIdx:endIdx]
		req := &retrieval.DeleteRealTimeKnowledgeReq{
			RobotId:          robotID,
			IndexId:          model.RealtimeSegmentVersionID,
			EmbeddingVersion: embeddingVersion,
			DocType:          realtimeKnowledgeType,
			Data:             dataBatch,
			BotBizId:         botBizID,
		}
		log.InfoContextf(ctx, "directDeleteRealtimeSegmentKnowledge|req:%+v", req)
		rsp, err := d.directIndexCli.DeleteRealTimeKnowledge(ctx, req)
		if err != nil {
			log.ErrorContextf(ctx, "directDeleteRealtimeSegmentKnowledge|directIndexCli.DeleteRealTimeKnowledge "+
				"failed, err:%+v", err)
			return err
		}
		log.InfoContextf(ctx, "directDeleteRealtimeSegmentKnowledge|rsp:%+v", rsp)
	}

	log.InfoContextf(ctx, "directDeleteRealtimeSegmentKnowledge Success!")

	return nil
}

// getRealtimeKnowledgeData 获取添加Vector的RealtimeKnowledgeData
func (d *dao) getRealtimeKnowledgeData(ctx context.Context, segments []*realtime.TRealtimeDocSegment) (
	robotID uint64, data []*retrieval.KnowledgeData, err error) {
	log.InfoContextf(ctx, "getRealtimeKnowledgeData|len(segments):%d", len(segments))
	data = make([]*retrieval.KnowledgeData, 0)
	if len(segments) == 0 {
		return 0, data, nil
	}
	robotID, docID := uint64(0), uint64(0)
	for _, segment := range segments {
		if robotID == 0 || docID == 0 {
			robotID = segment.RobotID
			docID = segment.DocID
		} else {
			if robotID != segment.RobotID || docID != segment.DocID {
				err = fmt.Errorf("getRealtimeKnowledgeData|robotID or docID not equal")
				log.InfoContextf(ctx, "getRealtimeKnowledgeData|robotID:%d, docID:%d, segment:%+v, err:%+v",
					robotID, docID, segment, err)
				return 0, nil, err
			}
		}

		// 实时文档检索标签
		labels := make([]*retrieval.VectorLabel, 0)
		labels = append(labels, &retrieval.VectorLabel{
			Name:  RealtimeSessionIDLabel,
			Value: segment.SessionID,
		})
		labels = append(labels, &retrieval.VectorLabel{
			Name:  RealtimeDocIDLabel,
			Value: strconv.FormatUint(segment.DocID, 10),
		})
		data = append(data, &retrieval.KnowledgeData{
			Id:          segment.SegmentID,
			SegmentType: segment.SegmentType,
			DocId:       segment.DocID,
			PageContent: segment.PageContent,
			Labels:      labels,
		})
	}
	log.InfoContextf(ctx, "getRealtimeKnowledgeData|robotID:%d, docID:%d, len(data):%d",
		robotID, docID, len(data))
	return robotID, data, nil
}

// updateSegmentSyncKnowledge Status 更新segment同步Vector状态
func (d *dao) updateSegmentSyncKnowledge(ctx context.Context, segments []*realtime.TRealtimeDocSegment,
	syncStatus int) error {
	log.InfoContextf(ctx, "updateSegmentSyncKnowledge|len(segments):%d, syncStatus:%d",
		len(segments), syncStatus)
	if len(segments) == 0 {
		return nil
	}
	segmentIDs := make([]uint64, 0)
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, segment.SegmentID)
	}
	err := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDocSegment{}).
		Where("segment_id IN ? and is_deleted = ?", segmentIDs, model.SegmentIsNotDeleted).
		Updates(map[string]interface{}{
			"is_sync_knowledge": syncStatus,
		}).Error
	if err != nil {
		log.ErrorContextf(ctx, "updateSegmentSyncKnowledge|gormDB.Updates failed, err:%+v", err)
		return err
	}
	return err
}

// deletedRealtimeSegmentByDocIDs 删除文档片段
func (d *dao) deletedRealtimeSegmentByDocIDs(ctx context.Context, robotID uint64, docIDs []uint64) error {
	log.InfoContextf(ctx, "deletedRealtimeSegmentByDocIDs|docIDs:%+v", docIDs)
	if len(docIDs) == 0 {
		return nil
	}
	err := d.gormDB.Transaction(func(tx *gorm.DB) error {
		// 删除DB
		err := tx.WithContext(ctx).Model(&realtime.TRealtimeDocSegment{}).
			Where("robot_id = ? and doc_id IN ? and is_deleted = ?",
				robotID, docIDs, model.SegmentIsNotDeleted).
			Updates(map[string]interface{}{
				"is_deleted": model.SegmentIsDeleted,
			}).Error
		if err != nil {
			log.ErrorContextf(ctx, "deletedRealtimeSegmentByDocIDs|gormDB.Updates failed, err:%+v", err)
			return err
		}
		err = tx.WithContext(ctx).Model(&realtime.TRealtimeDocSegmentImage{}).
			Where("robot_id = ? and doc_id IN ? and is_deleted = ?",
				robotID, docIDs, model.SegmentIsNotDeleted).
			Updates(map[string]interface{}{
				"is_deleted": model.SegmentIsDeleted,
			}).Error
		if err != nil {
			log.ErrorContextf(ctx, "deletedRealtimeSegmentByDocIDs|gormDB.Updates failed, err:%+v", err)
			return err
		}
		// 删除ES
		for _, docID := range docIDs {
			err = d.DeleteBigDataElastic(ctx, robotID, docID, retrieval.KnowledgeType_REALTIME, true)
			if err != nil {
				log.ErrorContextf(ctx, "deletedRealtimeSegmentByDocIDs|DeleteBigDataElastic failed, "+
					"robotID:%d, docID:%d, err:%+v", robotID, docID, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "deletedRealtimeSegmentByDocIDs|gormDB.Transaction failed, err:%+v", err)
		return err
	}
	return nil
}

// SearchRealtimeKnowledge 实时文档检索
func (d *dao) SearchRealtimeKnowledge(ctx context.Context, req *retrieval.RetrievalRealTimeReq) (
	*retrieval.RetrievalRealTimeRsp, error) {
	log.InfoContextf(ctx, "SearchRealtimeKnowledge|req:%+v", req)
	botBizID, err := d.GetBotBizIDByID(ctx, req.RobotId)
	if err != nil {
		log.ErrorContextf(ctx, "SearchRealtimeKnowledge|GetBotBizIDByID:%+v err:%+v", req.RobotId, err)
		return nil, err
	}
	req.BotBizId = botBizID
	rsp, err := d.directIndexCli.RetrievalRealTime(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "SearchRealtimeKnowledge|directIndexCli.RetrievalRealTime failed, err:%+v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "SearchRealtimeKnowledge|rsp:%+v", rsp)
	return rsp, nil
}

// GetLinkContentsFromRealtimeSearchVectorResponse 从检索请求构造 linkContents
func (d *dao) GetLinkContentsFromRealtimeSearchVectorResponse(ctx context.Context,
	docs []*retrieval.RetrievalRealTimeRsp_Doc,
	segmentFn func(doc *retrieval.RetrievalRealTimeRsp_Doc, segment *realtime.TRealtimeDocSegment) any,
	searchEngineFn func(doc *retrieval.RetrievalRealTimeRsp_Doc) any) ([]linker.Content, error) {
	log.InfoContextf(ctx, "GetLinkContentsFromRealtimeSearchVectorResponse|len(docs):%d", len(docs))
	linkContents := make([]linker.Content, 0, len(docs))
	var err error
	for _, doc := range docs {
		var linkContent linker.Content
		switch doc.GetDocType() {
		case model.DocTypeSearchEngine:
			linkContent, err = d.getSearchEngineLinkContent(
				ctx,
				func(doc *retrieval.RetrievalRealTimeRsp_Doc) func() any {
					return func() any {
						return searchEngineFn(doc)
					}
				}(doc),
			)
			if err != nil {
				return nil, err
			}
		case model.DocTypeSegment:
			linkContent, err = d.getRealtimePreviewSegmentLinkContent(
				ctx, doc,
				func(doc *retrieval.RetrievalRealTimeRsp_Doc) func(segment *realtime.TRealtimeDocSegment) any {
					return func(segment *realtime.TRealtimeDocSegment) any {
						return segmentFn(doc, segment)
					}
				}(doc),
			)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errs.ErrUnknownIndexID
		}
		linkContents = append(linkContents, linkContent)
	}
	log.InfoContextf(ctx, "GetLinkContentsFromRealtimeSearchVectorResponse|len(linkContents):%d",
		len(linkContents))
	return linkContents, nil
}

// getRealtimePreviewSegmentLinkContent 获取实时文档合并内容
func (d *dao) getRealtimePreviewSegmentLinkContent(ctx context.Context,
	doc *retrieval.RetrievalRealTimeRsp_Doc, fn func(segment *realtime.TRealtimeDocSegment) any) (
	linker.Content, error) {
	seg, err := d.getRealtimeDocSegmentByID(ctx, doc.GetId())
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimePreviewSegmentLinkContent|getRealtimeDocSegmentByID failed, "+
			"err:%v ", err)
		return linker.Content{}, err
	}
	if seg == nil {
		log.ErrorContextf(ctx, "getRealtimePreviewSegmentLinkContent|getRealtimeDocSegmentByID failed, "+
			"segment is not exist")
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	if seg.DocID != doc.GetDocId() {
		log.ErrorContextf(ctx, "getRealtimePreviewSegmentLinkContent|getRealtimeDocSegmentByID failed, "+
			"seg and doc ID not equal|seg.DocID: %d, doc.GetDocId():%d", seg.DocID, doc.GetDocId())
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	startIndex := seg.StartChunkIndex
	endIndex := seg.EndChunkIndex
	// 如果是bigData则对应取bigData的start和end
	if doc.GetIsBigData() {
		startIndex = int(seg.BigStart)
		endIndex = int(seg.BigEnd)
		log.InfoContextf(ctx, "getRealtimePreviewSegmentLinkContent|isBigData startIndex:%d,endIndex:%d,"+
			"segmentID:%d,docID:%d", startIndex, endIndex, doc.GetId(), seg.DocID)
	}
	return linker.Content{
		Key:    fmt.Sprintf("%d-%d-%d", model.DocTypeSegment, seg.DocID, seg.RichTextIndex),
		Extra:  fn(seg),
		Value:  doc.GetOrgData(),
		Start:  startIndex,
		End:    endIndex,
		Prefix: seg.Title,
		Keep:   seg.LinkerKeep,
	}, nil
}

// getRealtimeDocSegmentByID 根据ID查询实时文档切片
func (d *dao) getRealtimeDocSegmentByID(ctx context.Context, segmentID uint64) (*realtime.TRealtimeDocSegment, error) {
	log.InfoContextf(ctx, "getRealtimeDocSegmentByID|segmentID:%d", segmentID)
	var docSegments []*realtime.TRealtimeDocSegment
	err := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDocSegment{}).
		Where("segment_id = ? and is_deleted = ?", segmentID, model.SegmentIsNotDeleted).
		Find(&docSegments).Error
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimeDocSegmentByID|gormDB.Find failed, err:%+v", err)
		return nil, err
	}
	if len(docSegments) != 1 {
		err = fmt.Errorf("has duplicate docSegments or docSegments is empty")
		return nil, err
	}
	return docSegments[0], nil
}

// getRealtimeDocBySessionDocID 根据sessionID或者文档docID查询实时文档
func (d *dao) getRealtimeDocBySessionDocID(ctx context.Context, sessionID string,
	docIds []uint64) ([]*realtime.TRealtimeDoc, error) {
	var docInfos []*realtime.TRealtimeDoc
	db := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDoc{})
	if sessionID != "" {
		db = db.Where("session_id = ?", sessionID)
	}
	if len(docIds) > 0 {
		db = db.Where("doc_id in ?", docIds)
	}
	err := db.Where("is_deleted = ?", 0).Find(&docInfos).Error
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimeDocSegmentBySessionDocID Failed,sessionID:%s,docIDs:%+v,err:%+v", sessionID,
			docIds, err)
		return nil, err
	}
	return docInfos, nil
}

// getRealtimeDocSegmentBySessionDocID 根据sessionID或者文档docID查询实时文档切片
func (d *dao) getRealtimeDocSegmentBySessionDocID(ctx context.Context, sessionID string,
	docIds []uint64) ([]*realtime.TRealtimeDocSegment, error) {
	var docSegments []*realtime.TRealtimeDocSegment
	db := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDocSegment{})
	if sessionID != "" {
		db = db.Where("session_id = ?", sessionID)
	}
	if len(docIds) > 0 {
		db = db.Where("doc_id in ?", docIds)
	}
	err := db.Where("is_deleted = ?", model.SegmentIsNotDeleted).Find(&docSegments).Error
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimeDocSegmentBySessionDocID Failed,sessionID:%s,docIDs:%+v,err:%+v", sessionID,
			docIds, err)
		return nil, err
	}
	return docSegments, nil
}

func (d *dao) CreateRealtimeDoc(ctx context.Context, realtimeDoc *realtime.TRealtimeDoc) (*realtime.TRealtimeDoc,
	error) {
	err := d.gormDB.WithContext(ctx).Create(&realtimeDoc).Error

	if err != nil {
		log.ErrorContextf(ctx, "CreateRealtimeDoc Failed, realtimeDoc:%+v,err:%+v", realtimeDoc, err)
		return nil, err
	}
	return realtimeDoc, nil
}

// deletedRealtimeDocByDocID 删除文档
func (d *dao) deletedRealtimeDocByDocID(ctx context.Context, sessionID string, docIds []uint64, stageDay uint64) error {
	log.InfoContextf(ctx, "DeletedRealtimeDocByDocID| sessionID:%s,docIds: %+v", sessionID, docIds)
	db := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDoc{})
	if sessionID != "" {
		db = db.Where("session_id = ?", sessionID)
	}
	if len(docIds) > 0 {
		db = db.Where("doc_id in ?", docIds)
	}
	// 保存时间
	if stageDay > 0 {
		expireTime := -1 * 24 * time.Duration(stageDay) * time.Hour
		db = db.Where("update_time < ?", time.Now().Add(expireTime))
	}
	err := db.Update("is_deleted", 1).Error
	if err != nil {
		log.ErrorContextf(ctx, "DeletedRealtimeDocByDocID Failed! err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) DeletedRealtimeDocInfo(ctx context.Context, botBizID uint64, sessionID string, docIds []uint64) error {
	var err error

	app, err := d.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		log.ErrorContextf(ctx, "DeletedRealtimeDocInfo|GetAppByID failed, err:%+v", err)
		return err
	}
	if app == nil {
		log.ErrorContextf(ctx, "DeletedRealtimeDocInfo ,RobotNotFound!")
		return errs.ErrRobotNotFound
	}

	if len(docIds) == 0 {
		docInfos, err := d.getRealtimeDocBySessionDocID(ctx, sessionID, nil)
		if err != nil {
			return err
		}
		for _, doc := range docInfos {
			docIds = append(docIds, doc.DocID)
		}
	}

	// 文档删除操作
	// 1.逻辑删除相关t_realtime_doc, 这个地方的删除特殊处理【保留半年】
	realtimeCfg := utilConfig.GetMainConfig().RealtimeConfig
	if realtimeCfg.NeedDeleteRealtimeDoc {
		err = d.deletedRealtimeDocByDocID(ctx, sessionID, docIds, uint64(realtimeCfg.RealTimeDocStageTime))
		if err != nil {
			log.ErrorContextf(ctx, "DeletedRealtimeDocByDocID Failed, err:%+v", err)
			return err
		}
	}

	log.InfoContextf(ctx, "DeletedRealtimeDocInfo,appID:%+v,docIds:%+v", app.ID, docIds)

	// 获取文档片段
	segments, err := d.getRealtimeDocSegmentBySessionDocID(ctx, sessionID, docIds)
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimeDocSegmentBySessionDocID Failed,err:%+v", err)
		return err
	}

	// 2.删除文档片段[包括t_realtime_doc_segment 和 es]
	err = d.deletedRealtimeSegmentByDocIDs(ctx, app.ID, docIds)
	if err != nil {
		log.ErrorContextf(ctx, "deletedRealtimeSegmentByDocIDs Failed, err:%+v", err)
		return err
	}

	// segments按照docID + robotID不同分组之后在处理
	docSegmentIDMap := make(map[string][]*realtime.TRealtimeDocSegment, 0)
	for _, segment := range segments {
		tmpKey := cast.ToString(segment.RobotID) + cast.ToString(segment.DocID)
		vals, ok := docSegmentIDMap[tmpKey]
		if ok {
			vals = append(vals, segment)
			docSegmentIDMap[tmpKey] = vals
		} else {
			docSegmentIDMap[tmpKey] = []*realtime.TRealtimeDocSegment{segment}
		}
	}

	// 应用删除之后不删除Vector,避免向量group不存在，报警
	if app.IsDeleted == model.IsDeleted {
		log.InfoContextf(ctx, "App is deleted! appInfo:%+v", app)
		return nil
	}

	// 3.删除Vector
	embeddingConf, _, err := app.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "searchRealtimePreview|GetRobotEmbeddingConf failed, err: %v", err)
		return err
	}
	for _, segments := range docSegmentIDMap {
		err = d.directDeleteRealtimeSegmentKnowledge(ctx, segments, embeddingConf.Version)
		if err != nil {
			log.ErrorContextf(ctx, "directDeleteRealtimeSegmentKnowledge Failed,err:%+v", err)
			return err
		}
	}

	log.InfoContextf(ctx, "DeletedRealtimeDocInfo Success! ")

	return nil
}

// GetRealTimeSegmentChunk 分片查询实时文档切片 用于embedding升级时拉取
func (d *dao) GetRealTimeSegmentChunk(ctx context.Context, corpID, robotID, offset,
	limit uint64) ([]*realtime.TRealtimeDocSegment, error) {
	var docSegments []*realtime.TRealtimeDocSegment
	db := d.gormDB.WithContext(ctx).Model(&realtime.TRealtimeDocSegment{})
	db = db.Where("corp_id = ? AND robot_id = ? AND is_deleted = ? AND id > ?",
		corpID, robotID, model.SegmentIsNotDeleted, offset).
		Order("id ASC").Limit(int(limit))

	err := db.Find(&docSegments).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetRealTimeSegmentChunk Failed,corpID:%d,robotID:%d,offset:%d,limit:%d,err:%+v",
			corpID, robotID, offset, limit, err)
		return nil, err
	}
	return docSegments, nil
}
