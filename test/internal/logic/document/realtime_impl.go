package document

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/realtime"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/internal/util/linker"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	app "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/adp/pb-go/kb/parse_engine/file_parse_common"
	kbParseRouter "git.woa.com/adp/pb-go/kb/parse_engine/kb_parse_router"
	"git.woa.com/dialogue-platform/proto/pb-stub/nrt_file_parser_server"
	"github.com/avast/retry-go"
	"github.com/spf13/cast"
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

func (l *Logic) getRealtimeDocDB(ctx context.Context) *gorm.DB {
	return l.segDao.Query().TRealtimeDoc.WithContext(ctx).UnderlyingDB()
}

func (l *Logic) getRealtimeDocSegmentDB(ctx context.Context) *gorm.DB {
	return l.segDao.Query().TRealtimeDocSegment.WithContext(ctx).UnderlyingDB()
}

// CheckRealtimeStorageInfo 校验实时文档存储信息
func (l *Logic) CheckRealtimeStorageInfo(ctx context.Context, bucket, url, eTag string, app *entity.App) error {
	logx.I(ctx, "CheckRealtimeStorageInfo|bucket:%s, url:%s, eTag:%s, app.BusinessIds:%d",
		bucket, url, eTag, app.BizId)
	// bucket校验
	realtimeBucket, err := l.s3.GetBucketWithTypeKey(ctx, entity.RealtimeStorageTypeKey)
	if err != nil {
		return err
	}
	if realtimeBucket != bucket {
		return errs.ErrInvalidURL
	}
	// url校验
	if !isRealtimeURLLegal(url, app.BizId) {
		return errs.ErrInvalidURL
	}
	// 文件校验
	if len(eTag) == 0 {
		err = fmt.Errorf("url:%s, eTag:%s is empty", url, eTag)
		return err
	}
	// 这里objectInfo.ETag的结果会带有转义字符 类似 "\"5784a190d6af4214020f54edc87429ab\""
	// 需要对转义字符特殊处理
	objectInfo, err := l.s3.StatObjectWithTypeKey(ctx, entity.RealtimeStorageTypeKey, url)
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
func (l *Logic) GetRealtimeDocByID(ctx context.Context, docID uint64) (*realtime.TRealtimeDoc, error) {
	logx.I(ctx, "GetRealtimeDocByID|docID:%d", docID)
	var docs []*realtime.TRealtimeDoc
	err := l.getRealtimeDocDB(ctx).Model(&realtime.TRealtimeDoc{}).
		Where("doc_id = ? and is_deleted = ?", docID, docEntity.DocIsNotDeleted).
		Find(&docs).Error
	if err != nil {
		logx.E(ctx, "GetRealtimeDocByID|gormDB.Find failed, err:%+v", err)
		return nil, err
	}
	if len(docs) != 1 {
		return nil, errs.ErrDocNotFound
	}
	return docs[0], nil
}

// ParseRealtimeDoc 提交实时文档解析
func (l *Logic) ParseRealtimeDoc(ctx context.Context, reqCh <-chan *realtime.ParseDocReqChan,
	rspCh chan<- *realtime.ParseDocRspChan) error {
	logx.I(ctx, "ParseRealtimeDoc|called")

	// 准实时解析引擎cli
	cli, err := l.rpc.FileManager.StreamParse(ctx)
	if err != nil {
		logx.E(ctx, "ParseRealtimeDoc|d.nrtFileManagerCli.StreamParse err:%+v", err)
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
			logx.I(ctx, "ParseRealtimeDoc|rspCh closed")
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
			logx.I(ctx, "ParseRealtimeDoc|<-ctx.Done()")
			return nil
		case req, ok := <-reqCh:
			if !ok {
				logx.I(ctx, "ParseRealtimeDoc|reqCh closed")
				return nil
			}
			logx.I(ctx, "ParseRealtimeDoc|req:%+v", req)
			// 填充实时文档信息
			if err := l.fillRealtimeDocByUrlID(ctx, doc, req.Doc.SessionID, req.Doc.CosUrlID); err != nil {
				logx.E(ctx, "ParseRealtimeDoc|fillRealtimeDocByUrlID failed, err:%+v", err)
				return err
			}
			// 组装实时解析请求
			nrtReq, err := l.getNrtReqAndFillDoc(ctx, req.Type, doc, req.ModelName)
			if err != nil {
				logx.E(ctx, "ParseRealtimeDoc|getNrtReqAndFillDoc failed, err:%+v", err)
				return err
			}
			if err = l.updateRealtimeDoc(ctx, doc); err != nil {
				logx.E(ctx, "ParseRealtimeDoc|updateRealtimeDoc failed, err:%+v", err)
				return err
			}
			logx.D(ctx, "-----uin=%s, login_uin=%s", contextx.Metadata(ctx).Uin(), contextx.Metadata(ctx).LoginUin())
			// 请求实时解析引擎
			if err = cli.Send(nrtReq); err != nil {
				logx.E(ctx, "ParseRealtimeDoc|cli.Send failed, err:%+v", err)
				doc.Status = realtime.RealDocStatusFailed
				doc.Message = fmt.Sprintf("cli.Send failed, err:%+v", err)
				_ = l.updateRealtimeDoc(ctx, doc)
				return err
			}
			logx.I(ctx, "ParseRealtimeDoc|cli.Send success")
			if nrtReq.GetRequestType() == file_parse_common.ReqType_REQ_TYPE_TASK_CANCEL {
				stopSignal <- struct{}{}
			}
			if !isReceiving {
				// 保证接收启动
				wg := sync.WaitGroup{}
				wg.Add(1)

				go func() {
					defer gox.Recover()
					l.handlerNrtReceive(ctx, req.ModelName, &wg, doc, cli, rspCh, stopSignal)
				}()

				wg.Wait()
				isReceiving = true
			}
		}
	}
}

// fillRealtimeDocByUrlID 根据UrlID查询实时文档
func (l *Logic) fillRealtimeDocByUrlID(ctx context.Context, doc *realtime.TRealtimeDoc,
	sessionID string, cosUrlID uint64) (err error) {
	logx.I(ctx, "fillRealtimeDocByUrlID|doc:%+v, sessionID:%s, cosUrlID:%d",
		doc, sessionID, cosUrlID)
	if doc == nil {
		err = fmt.Errorf("dos is nill")
		logx.E(ctx, "fillRealtimeDocByUrlID|failed, err:%+v", err)
		return err
	}
	var docs []*realtime.TRealtimeDoc
	err = l.getRealtimeDocDB(ctx).Model(&realtime.TRealtimeDoc{}).
		Where("session_id = ? and cos_url_id = ? and is_deleted = ?",
			sessionID, cosUrlID, docEntity.DocIsNotDeleted).
		Find(&docs).Error
	if err != nil {
		logx.E(ctx, "fillRealtimeDocByUrlID|gormDB.Find failed, err:%+v", err)
		return err
	}
	if len(docs) != 1 {
		err = fmt.Errorf("has duplicate docs or docs is empty")
		logx.E(ctx, "fillRealtimeDocByUrlID|len(docs):%d|failed, err:%+v", len(docs), err)
		return err
	}
	// 填充实时文档信息
	// 指针传递地址不能直接赋值这里要使用参数替换的方式
	realtime.FillRealtimeDocInfo(doc, docs[0])
	logx.I(ctx, "fillRealtimeDocByUrlID|suucess|doc:%+v", doc)
	return nil
}

func (l *Logic) getRealtimeFileParseModelConfig(ctx context.Context, appPrimaryId uint64, modelName string) (*file_parse_common.ThirdModelConfig, error) {
	describeAppRsp, err := l.rpc.DescribeApp(ctx, &app.DescribeAppReq{
		AppPrimaryId: appPrimaryId,
	})
	logx.D(ctx, "getRealtimeFileParseModelConfig|describeAppRsp:%+v, err:%+v", describeAppRsp, err)
	if err != nil {
		logx.E(ctx, "getRealtimeFileParseModelConfig|DescribeAppReq failed, err:%+v", err)
		return nil, err
	}
	if describeAppRsp.GetAppConfig().GetFileParseModelConfig().GetModelName() != modelName {
		logx.I(ctx, "getRealtimeFileParseModelConfig|preview modelName (%s) is not match to chat model name(%s)",
			describeAppRsp.GetAppConfig().GetFileParseModelConfig().GetModelName(), modelName)
		// 如果chat传过来的modelName，与评测端拿到的modelName不一致，则重新调用DescribeAppReq，获取发布端配置
		describeAppRsp, err = l.rpc.DescribeApp(ctx, &app.DescribeAppReq{
			AppPrimaryId: appPrimaryId,
			IsRelease:    true,
		})
		if err != nil {
			logx.E(ctx, "getRealtimeFileParseModelConfig|DescribeAppReq failed, err:%+v", err)
			return nil, err
		}
		if describeAppRsp.GetAppConfig().GetFileParseModelConfig().GetModelName() != modelName {
			logx.W(ctx, "getRealtimeFileParseModelConfig|release modelName (%s) is not match to chat model name(%s)",
				describeAppRsp.GetAppConfig().GetFileParseModelConfig().GetModelName(), modelName)
		}
	}
	if describeAppRsp.GetAppConfig().GetFileParseModelConfig() != nil {
		fileParseModel := describeAppRsp.GetAppConfig().GetFileParseModelConfig()
		thirdModelConfig := &file_parse_common.ThirdModelConfig{
			ModelName: fileParseModel.GetAliasName(),
			ModelId:   fileParseModel.GetModelName(),
		}
		for _, supportedFileType := range fileParseModel.SupportedFiles {
			thirdModelConfig.SupportedFiles = append(thirdModelConfig.SupportedFiles, &file_parse_common.SupportedFileType{
				FileExt:      supportedFileType.FileExt,
				MaxSizeBytes: supportedFileType.MaxSizeBytes,
				Description:  supportedFileType.Description,
			})
		}
		docParseThirdParseConfigParam := &docEntity.DocParseThirdParseConfigParam{
			FormulaEnhancement: fileParseModel.FormulaEnhancement,
			LLMEnhancement:     fileParseModel.LargeLanguageModelEnhancement,
			EnhancementMode:    fileParseModel.EnhancementMode,
			OutputHtmlTable:    fileParseModel.OutputHtmlTable,
		}
		thirdModelConfig.Param, err = jsonx.MarshalToString(docParseThirdParseConfigParam)
		if err != nil {
			logx.E(ctx, "getThirdModelConfig Marshal err:%v", err)
			return nil, err
		}
		return thirdModelConfig, nil
	} else {
		logx.E(ctx, "getRealtimeFileParseModelConfig|describeAppRsp.GetAppConfig().GetFileParseModelConfig() is nil")
		return nil, fmt.Errorf("FileParseModelConfig is nil")
	}
}

// getNrtReqAndFillDoc 组装实时文档解析请求
func (l *Logic) getNrtReqAndFillDoc(ctx context.Context, taskType pb.StreamSaveDocReq_ReqType,
	doc *realtime.TRealtimeDoc, modelName string) (nrtReq *file_parse_common.StreamParseReq, err error) {
	nrtReq = &file_parse_common.StreamParseReq{}
	// 任务类型校验
	switch taskType {
	case pb.StreamSaveDocReq_TASK_PARSE:
		if !doc.CanParse() {
			err = fmt.Errorf("doc:%+v cannot parse", doc)
			logx.E(ctx, "getNrtReqAndFillDoc|err:%+v", err)
			return nil, err
		}
		nrtReq.RequestType = file_parse_common.ReqType_REQ_TYPE_TASK_REQ
		doc.Status = realtime.RealDocStatusParsing
	case pb.StreamSaveDocReq_TASK_CANCEL:
		if !doc.CanCancel() {
			err = fmt.Errorf("doc:%+v cannot cancel", doc)
			logx.E(ctx, "getNrtReqAndFillDoc|err:%+v", err)
			return nil, err
		}
		nrtReq.RequestType = file_parse_common.ReqType_REQ_TYPE_TASK_CANCEL
		doc.Status = realtime.RealDocStatusCancel
	default:
		err = fmt.Errorf("illegal taskType:%v", taskType)
		logx.E(ctx, "getNrtReqAndFillDoc|err:%+v", err)
		return nil, err
	}
	// 组装请求
	robot, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil {
		logx.E(ctx, "getNrtReqAndFillDoc|DescribeAppByPrimaryIdWithoutNotFoundError failed, err:%+v", err)
		return nil, err
	}
	if robot == nil {
		return nil, errs.ErrRobotNotFound
	}
	// 拆分策略
	splitStrategy, err := l.getRealtimeSplitStrategy(ctx, doc)
	if err != nil {
		return nil, err
	}
	splitStrategyStr, err := jsonx.MarshalToString(splitStrategy)
	if err != nil {
		logx.E(ctx, "getNrtReqAndFillDoc|jsonx.MarshalToString failed, err:%+v", err)
		return nil, err
	}
	// 实时文档解析版本号
	fileManagerVersion := config.GetMainConfig().FileParseConfig.RealtimeFileManagerVersion
	if fileManagerVersion <= 0 {
		fileManagerVersion = defaultRealtimeFileManagerVersion
	}
	thirdModelConfig, err := l.getRealtimeFileParseModelConfig(ctx, robot.PrimaryId, modelName)
	if err != nil {
		// 这里拿不到模型配置，柔性放过
		logx.E(ctx, "getNrtReqAndFillDoc|getRealtimeFileParseModelConfig failed, err:%+v", err)
		err = nil
	}
	if thirdModelConfig != nil {
		if !l.financeLogic.CheckModelStatus(ctx, robot.CorpPrimaryId, thirdModelConfig.ModelId, rpc.RealTimeDocParseFinanceBizType) {
			logx.E(ctx, "getNrtReqAndFillDoc|CheckModelStatus failed, modelId:%s", thirdModelConfig.ModelId)
			return nil, fmt.Errorf("model:%s is not enabled", thirdModelConfig.ModelId)
		}
	}
	nrtReq.TaskReq = &file_parse_common.CreateTaskReq{
		RequestId: contextx.TraceID(ctx),
		AppInfo: &file_parse_common.AppInfo{
			AppKey: fmt.Sprintf("%d", robot.BizId),
			Biz:    fileManagerBiz,
		},
		OpInfo: &file_parse_common.OpInfo{
			CurrentOpType: file_parse_common.OpType_OP_TYPE_SPLIT,
			FinalOpType:   file_parse_common.OpType_OP_TYPE_SPLIT,
		},
		FileInfo: doc.GetTaskFileInfo(),
		ParseSetting: &file_parse_common.ParseSetting{
			ParseStrategy: file_parse_common.ParseStrategy(
				config.GetMainConfig().FileParseConfig.RealtimeParseSetting.ParseMode),
			// 3.3.2.1需求，实时文档解析切换了新的协议，下面两个参数在新协议中不存在，后续是否需要待产品统一规划
			//IsOpenSubimg:  config.GetMainConfig().FileParseConfig.RealtimeParseSetting.IsOpenSubimg,
			//IsOpenFormula: config.GetMainConfig().FileParseConfig.RealtimeParseSetting.IsOpenFormula,
			ThirdModelConfig: thirdModelConfig,
		},
		SplitStrategy: splitStrategyStr,
	}
	doc.RequestID = nrtReq.GetTaskReq().GetRequestId()
	doc.OpType = int32(nrtReq.GetRequestType())
	logx.I(ctx, "getNrtReqAndFillDoc|nrtReq:%+v, doc:%+v", nrtReq, doc)
	return nrtReq, err
}

// getRealtimeSplitStrategy 获取实时文档解析配置
func (l *Logic) getRealtimeSplitStrategy(ctx context.Context, doc *realtime.TRealtimeDoc) (
	*nrt_file_parser_server.SplitStrategy, error) {
	// 拆分策略
	splitStrategyStr, err := l.getRobotSplitStrategy(ctx, doc.FileName)
	if err != nil {
		logx.E(ctx, "getRealtimeSplitStrategy|getRobotSplitStrategy failed, err:%+v", err)
		return nil, err
	}
	var splitStrategy nrt_file_parser_server.SplitStrategy
	err = jsonx.UnmarshalFromString(splitStrategyStr, &splitStrategy)
	if err != nil {
		logx.E(ctx, "getRealtimeSplitStrategy|jsonx.UnmarshalFromString failed, err:%+v", err)
		return nil, err
	}
	// 文档全文提取最大字数配置
	fullTextMaxSize := config.GetMainConfig().RealtimeConfig.FullTextMaxSize
	if fullTextMaxSize <= 0 {
		fullTextMaxSize = defaultFullTextMaxSize
	}
	splitStrategy.ModelSplitConfig = &nrt_file_parser_server.SplitStrategy_ModelSplitConfig{
		ReturnFullTextMaxLength: int32(fullTextMaxSize),
	}
	// 文档解析器配置
	splitStrategy.ParserConfig = &nrt_file_parser_server.SplitStrategy_ParserConfig{
		SingleParagraph: config.GetMainConfig().FileParseConfig.RealtimeParseSetting.ParserConfig.SingleParagraph,
		SplitSubTable:   config.GetMainConfig().FileParseConfig.RealtimeParseSetting.ParserConfig.SplitSubTable,
	}
	return &splitStrategy, nil
}

// updateRealtimeDoc 根据UrlID更新实时文档
func (l *Logic) updateRealtimeDoc(ctx context.Context, doc *realtime.TRealtimeDoc) error {
	logx.I(ctx, "updateRealtimeDoc|doc:%+v", doc)
	err := l.getRealtimeDocDB(ctx).Model(&realtime.TRealtimeDoc{}).
		Where("doc_id = ? and is_deleted = ?", doc.DocID, docEntity.DocIsNotDeleted).
		Updates(map[string]any{
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
		logx.E(ctx, "updateRealtimeDoc|gormDB.Updates failed, err:%+v", err)
		return err
	}
	return err
}

// handlerNrtReceive 处理实时解析引擎回包
func (l *Logic) handlerNrtReceive(ctx context.Context, modelName string, wg *sync.WaitGroup,
	doc *realtime.TRealtimeDoc, cli kbParseRouter.ParseRouter_StreamParseClient,
	rspCh chan<- *realtime.ParseDocRspChan, stopSignal chan struct{}) {
	logx.I(ctx, "handlerNrtReceive|called")
	wg.Done()

	// 如果已经启动接收，在这里关闭rspCh
	defer func() {
		close(rspCh)
		logx.I(ctx, "handlerNrtReceive|rspCh closed")
	}()

	for {
		select {
		case <-ctx.Done():
			logx.I(ctx, "handlerNrtReceive|<-ctx.Done()")
			cancelReq, err := l.getNrtReqAndFillDoc(ctx, pb.StreamSaveDocReq_TASK_CANCEL, doc, modelName)
			if err != nil {
				return
			}
			_ = l.updateRealtimeDoc(ctx, doc)
			_ = cli.Send(cancelReq)
			return
		case <-stopSignal:
			logx.I(ctx, "handlerNrtReceive|stopSignal|return")
			doc.Status = realtime.RealDocStatusCancel
			_ = l.updateRealtimeDoc(ctx, doc)
			return
		default:
			nrtRsp, err := cli.Recv()
			if err == nil && nrtRsp != nil {
				// 转换解析结果
				l.parseNrtRsp(ctx, doc, nrtRsp)
				logx.I(ctx, "handlerNrtReceive|parseNrtRsp|doc:%+v", doc)
			}
			if err == io.EOF {
				logx.I(ctx, "handlerNrtReceive|cli.Recv EOF return")
				if doc.IsFinalStatus() {
					return
				}
			}
			if err != nil {
				logx.E(ctx, "handlerNrtReceive|cli.Recv faield, err:%+v", err)
				if !doc.IsFinalStatus() {
					doc.Status = realtime.RealDocStatusFailed
					doc.Message = fmt.Sprintf("handlerNrtReceive|cli.Recv failed, err:%+v", err)
				}
			}
			err = l.updateRealtimeDoc(ctx, doc)
			if err != nil {
				logx.E(ctx, "handlerNrtReceive|updateRealtimeDoc failed, err:%+v", err)
				doc.Status = realtime.RealDocStatusFailed
				doc.Message += fmt.Sprintf("updateRealtimeDoc failed, err:%+v", err)
			}
			// 结果解析
			docSummary, statisticInfo, err := l.handlerDocTaskResult(ctx, modelName, doc, rspCh)
			if err != nil {
				// 这里做一次更新
				_ = l.updateRealtimeDoc(ctx, doc)
			}
			logx.I(ctx, "outer docSummary :%+v", docSummary)
			// 回包
			handlerRspCh(ctx, docSummary, statisticInfo, doc, rspCh)
			// 是否结束
			if err != nil || doc.IsFinalStatus() {
				logx.I(ctx, "handlerNrtReceive|cli.Recv return|err:%+v, doc.Status:%s",
					err, doc.Status)
				return
			}
		}
	}
}

// parseNrtRsp 转换解析结果
func (l *Logic) parseNrtRsp(ctx context.Context, doc *realtime.TRealtimeDoc, nrtRsp *file_parse_common.StreamParseRsp) {
	logx.I(ctx, "parseNrtRsp|nrtRsp:%+v", nrtRsp)
	logx.I(ctx, "parseNrtRsp|doc:%+v", doc)
	if doc.IsFinalStatus() {
		logx.I(ctx, "parseNrtRsp|doc IsFinalStatus ignore|doc.Status:%s", doc.Status)
		return
	}
	doc.RequestID = nrtRsp.GetTaskRsp().GetRequestId()
	doc.TaskID = nrtRsp.GetTaskRsp().GetTaskId()
	doc.TaskStatus = int32(nrtRsp.GetTaskRsp().GetErrorCode())
	doc.Message = nrtRsp.GetTaskRsp().GetMessage()
	if doc.TaskStatus == realtime.TaskStatusSuccess {
		switch nrtRsp.ResponseType {
		case file_parse_common.RspType_RSP_TYPE_PROGRESS:
			doc.Status = realtime.RealDocStatusParsing
			doc.Progress = nrtRsp.GetTaskRsp().GetProgress().GetProgress()
			doc.ProgressMessage = nrtRsp.GetTaskRsp().GetProgress().GetMessage()
			return
		case file_parse_common.RspType_RSP_TYPE_TASK_RSP:
			doc.Progress = nrtRsp.GetTaskRsp().GetProgress().GetProgress()
			doc.ProgressMessage = nrtRsp.GetTaskRsp().GetProgress().GetMessage()
			doc.CharSize = nrtRsp.GetTaskRsp().GetTextLength() //todo yuzhengtao 是不是就是GetWordCount？
			if doc.CharSize <= 0 {
				logx.E(ctx, "parseNrtRsp|nrtRsp.GetTaskRsp().GetWordCount():%d illegal",
					nrtRsp.GetTaskRsp().GetTextLength())
				doc.Status = realtime.RealDocStatusFailed
				doc.Message += fmt.Sprintf("parseNrtRsp doc.CharSize <= 0")
				return
			}
			// 解析结果存储
			result, err := jsonx.MarshalToString(nrtRsp.GetTaskRsp())
			if err != nil {
				logx.E(ctx, "parseNrtRsp|jsonx.MarshalToString failed, err:%+v", err)
				doc.Status = realtime.RealDocStatusFailed
				doc.Message += fmt.Sprintf("parseNrtRsp jsonx.MarshalToString failed, err:%+v", err)
				return
			}
			doc.Status = realtime.RealDocStatusLearning
			doc.Result = result
			doc.PageCount = uint32(nrtRsp.GetTaskRsp().GetPageNum())
			return
		default:
			logx.E(ctx, "parseNrtRsp|illegal rsp type:%v",
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
func (l *Logic) handlerDocTaskResult(ctx context.Context, modelName string, doc *realtime.TRealtimeDoc, rspCh chan<- *realtime.ParseDocRspChan) (docSummary string, statisticInfo *pb.StatisticInfo, err error) {
	if doc.Status == realtime.RealDocStatusLearning {
		// 更新进度 Learning
		doc.Progress = realtime.RealDocLearningProgress
		doc.ProgressMessage = realtime.RealDocLearningDesc
		err = l.updateRealtimeDoc(ctx, doc)
		if err != nil {
			logx.E(ctx, "handlerDocTaskResult|updateRealtimeDoc failed, err:%+v", err)
			doc.Status = realtime.RealDocStatusFailed
			doc.Message += fmt.Sprintf("updateRealtimeDoc failed, err:%+v", err)
		} else {
			// 开始定时回包
			wg := sync.WaitGroup{}
			wg.Add(1)

			// 停止信号
			stopSignal := make(chan struct{}, 1)

			go func() {
				defer gox.Recover()
				defer wg.Done()

				realtimeTickerSecond := config.GetMainConfig().RealtimeConfig.RealtimeTickerSecond
				if realtimeTickerSecond <= 0 {
					realtimeTickerSecond = defaultRealtimeTickerSecond
				}
				logx.I(ctx, "handlerDocTaskResult|handlerRspCh|begin|realtimeTickerSecond:%d",
					realtimeTickerSecond)

				ticker := time.NewTicker(time.Duration(realtimeTickerSecond) * time.Second)
				defer ticker.Stop()

				for {
					select {
					case <-stopSignal:
						logx.I(ctx, "handlerDocTaskResult|handlerRspCh|stop")
						return
					case <-ticker.C:
						logx.I(ctx, "handlerDocTaskResult|handlerRspCh|ticker")
						// 回包
						if !doc.IsFinalStatus() {
							// 在文档未到终态时发送进度信息
							handlerRspCh(ctx, "", nil, doc, rspCh)
						}
					}
				}
			}()

			// 解析结果学习
			docSummary, statisticInfo, err = l.parseDocTaskResult(ctx, modelName, doc)
			if err != nil {
				logx.E(ctx, "handlerDocTaskResult|parseDocTaskResult failed, err:%+v", err)
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
func handlerRspCh(ctx context.Context, docSummary string, statisticInfo *pb.StatisticInfo, doc *realtime.TRealtimeDoc, rspCh chan<- *realtime.ParseDocRspChan) {
	rsp := doc.ConvertToParseDocRspChan(docSummary, statisticInfo)
	logx.I(ctx, "handlerRspCh|rsp:%+v", rsp)
	rspCh <- rsp
}

// parseDocTaskResult 解析任务结果
// 1. 解析文件
// 2. 同步DB
// 3. 同步Vector
// 4. 更新文档状态
func (l *Logic) parseDocTaskResult(ctx context.Context, modelName string, doc *realtime.TRealtimeDoc) (summary string, statisticInfo *pb.StatisticInfo, err error) {
	logx.I(ctx, "parseDocTaskResult|doc:%+v", doc)
	if doc.Status != realtime.RealDocStatusLearning {
		err = fmt.Errorf("doc status:%s is not learning", doc.Status)
		return "", nil, err
	}
	// 0. 解析前清理一次旧数据
	err = l.deletedRealtimeSegmentByDocIDs(ctx, doc.RobotID, []uint64{doc.DocID})
	if err != nil {
		logx.E(ctx, "parseDocTaskResult|deletedRealtimeSegmentByDocIDs failed, err:%+v", err)
		return "", nil, err
	}
	// 1. 解析文件
	// 2. 同步DB
	err = l.parseRealtimeDocTaskResult(ctx, doc)
	if err != nil {
		logx.E(ctx, "parseDocTaskResult|parseRealtimeDocTaskResult failed, err:%+v", err)
		return "", nil, err
	}

	// 摘要与同步vector并行处理
	wg := sync.WaitGroup{}
	wg.Add(2)
	var errChan = make(chan error, 2) // 可以存储两个错误

	needSummaryAppIDMap := config.GetMainConfig().RealtimeConfig.NeedSummaryAppID

	// 摘要处理
	// 20250207, 最新逻辑， 默认文档不做摘要【摘要很慢】，在走摘要appid白名单内的才走摘要逻辑
	go func() {
		defer gox.Recover()
		defer wg.Done()

		appInfo, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
		if err != nil {
			errChan <- err
			return
		}
		if appInfo == nil {
			return
		}
		newCtx := util.SetMultipleMetaData(ctx, appInfo.SpaceId, appInfo.Uin)
		// 特定appid,才需要走摘要逻辑
		if _, ok := needSummaryAppIDMap[appInfo.BizId]; !ok {
			logx.I(ctx, "parseDocTaskResult|NeedSummaryAppID,appID:%+v", appInfo.BizId)
			return
		}
		summary, statisticInfo, err = l.handleDocSummary(newCtx, modelName, doc)
		if err != nil {
			logx.E(ctx, "parseDocTaskResult|handleDocSummary failed,err:%+v", err)
			errChan <- err
			return
		}
	}()

	go func() {
		defer gox.Recover()
		// 3. 同步Vector
		defer wg.Done()
		if err = l.batchSyncRealtimeKnowledge(ctx, doc.RobotID, doc.DocID); err != nil {
			logx.E(ctx, "parseDocTaskResult|batchSyncRealtimeKnowledge failed, err:%+v", err)
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
	unSyncSegments, err := l.getUnSyncKnowledgeSegment(ctx, doc.DocID, defaultInsertDBBatchSize)
	if err != nil {
		logx.E(ctx, "parseDocTaskResult|getUnSyncKnowledgeSegment failed, err:%+v", err)
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
	err = l.updateRealtimeDoc(ctx, doc)
	if err != nil {
		logx.E(ctx, "handlerNrtReceive|updateRealtimeDoc failed, err:%+v", err)
		doc.Message += fmt.Sprintf("updateRealtimeDoc failed, err:%+v", err)
		return "", nil, err
	}
	return summary, statisticInfo, nil
}

// txBatchCreateRealtimeSegment 事物批量写入RealtimeDocSegment到DB
func (l *Logic) txBatchCreateRealtimeSegment(ctx context.Context, tx *mysqlquery.Query,
	shortURLSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map, segments []*realtime.TRealtimeDocSegment) error {
	if err := tx.Transaction(func(tx *mysqlquery.Query) error {
		batchSize := config.GetMainConfig().RealtimeConfig.InsertDBBatchSize
		if batchSize <= 0 {
			batchSize = defaultInsertDBBatchSize
		}
		logx.I(ctx, "txBatchCreateRealtimeSegment|len(segments):%d, batchSize:%d",
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
				tmpSegmentBigData, err := l.getRealtimeDocSegmentBigData(ctx, bigDataSyncMap, tmpSegment)
				if err != nil {
					return err
				}
				if tmpSegmentBigData != nil {
					bigData = append(bigData, tmpSegmentBigData)
				}

				// image数据
				tmpSegmentImages, err := l.getRealtimeDocSegmentImages(ctx, shortURLSyncMap, imageDataSyncMap, tmpSegment)
				if err != nil {
					return err
				}
				if len(tmpSegmentImages) > 0 {
					images = append(images, tmpSegmentImages...)
				}
			}

			// 写DB
			logx.I(ctx, "txBatchCreateRealtimeSegment|len(tmpSegments):%d", len(tmpSegments))
			if len(tmpSegments) > 0 {
				err := tx.TRealtimeDocSegment.WithContext(ctx).
					CreateInBatches(realtime.BatchConvertRealtimeSegmentDO2POs(tmpSegments), 100)
				if err != nil {
					logx.I(ctx, "txBatchCreateRealtimeSegment|seg:%+v|Create failed, err:%+v",
						tmpSegments, err)
					return err
				}
			}
			logx.I(ctx, "txBatchCreateRealtimeSegment|len(images):%d", len(images))
			if len(images) > 0 {
				err := tx.TRealtimeDocSegmentImage.WithContext(ctx).
					CreateInBatches(realtime.BatchConvertRealtimeDocSegmentImageDO2POs(images), 100)
				// err := tx.Model(&realtime.TRealtimeDocSegmentImage{}).Create(images).Error
				if err != nil {
					logx.I(ctx, "txBatchCreateRealtimeSegment|seg:%+v|Create failed, err:%+v",
						tmpSegments, err)
					return err
				}
			}

			// 写ES
			logx.I(ctx, "txBatchCreateRealtimeSegment|len(bigData):%d", len(bigData))
			if len(bigData) > 0 {
				req := retrieval.AddBigDataElasticReq{Data: bigData, Type: retrieval.KnowledgeType_REALTIME}
				if err := l.rpc.RetrievalDirectIndex.AddBigDataElastic(ctx, &req); err != nil {
					logx.E(ctx, "CreateSegment|AddBigDataElastic|seg:%+v|err:%+v", tmpSegments, err)
					return err
				}
			}
		}
		return nil

	}); err != nil {
		logx.E(ctx, "txBatchCreateRealtimeSegment|err:%+v", err)
		return err
	}

	logx.I(ctx, "txBatchCreateRealtimeSegment|success")
	return nil
}

// getRealtimeDocSegmentBigData 获取实时文档切片BigData
func (l *Logic) getRealtimeDocSegmentBigData(ctx context.Context, bigDataSyncMap *sync.Map,
	segment *realtime.TRealtimeDocSegment) (*retrieval.BigData, error) {
	if len(segment.BigString) == 0 {
		logx.W(ctx, "getRealtimeDocSegmentBigData|segment:%+v|BigString is empty", segment)
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
		segment.BigDataID = strconv.FormatUint(idgen.GetId(), 10) // 生成ES的ID
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
func (l *Logic) getRealtimeDocSegmentImages(ctx context.Context, shortURLSyncMap, imageDataSyncMap *sync.Map,
	segment *realtime.TRealtimeDocSegment) ([]*realtime.TRealtimeDocSegmentImage, error) {
	if len(segment.Images) == 0 {
		logx.I(ctx, "getRealtimeDocSegmentImages|segment:%+v|Images is empty", segment)
		return nil, nil
	}
	segmentImages := make([]*realtime.TRealtimeDocSegmentImage, 0)
	for _, originalUrl := range segment.Images {
		if originalUrl == "" {
			logx.W(ctx, "getRealtimeDocSegmentImages|segment:%+v|originalUrl is empty", segment)
			continue
		}
		// imageID := uint64(0)
		// if id, ok := imageDataSyncMap.Load(originalUrl); ok {
		//	imageID = id.(uint64)
		// } else {
		//	imageID = idgen.GetId()
		//	imageDataSyncMap.Store(originalUrl, imageID)
		// }
		// 2.4.0 @harryhlli @jouislu 结论：相同图片也用不同图片ID
		imageID := idgen.GetId()
		externalUrl := ""
		URL, err := url.Parse(originalUrl)
		if err != nil || URL.Path == "" {
			logx.E(ctx, "getRealtimeDocSegmentImages|segment:%+v|originalUrl:%s parse res:%+v err:%+v",
				segment, originalUrl, URL, err)
			return nil, fmt.Errorf("originalUrl is illegal")
		}
		oldURL := URL.Scheme + "://" + URL.Host + URL.Path
		if value, ok := shortURLSyncMap.Load(oldURL); ok {
			newURL := value.(string)
			externalUrl = strings.ReplaceAll(originalUrl, oldURL, newURL)
		} else {
			logx.E(ctx, "getRealtimeDocSegmentImages|segment:%+v|externalUrl is empty", segment)
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
func (l *Logic) batchSyncRealtimeKnowledge(ctx context.Context, robotID, docID uint64) error {
	batchSize := config.GetMainConfig().RealtimeConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	retryCount := config.GetMainConfig().RealtimeConfig.SyncVectorMaxRetry
	if retryCount <= 0 {
		retryCount = defaultSyncVectorMaxRetry
	}
	logx.I(ctx, "batchSyncRealtimeKnowledge|docID:%d, retryCount:%d", docID, retryCount)
	// 带失败重试
	err := retry.Do(
		func() error {
			appDB, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, robotID)
			if err != nil {
				return err
			}

			newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)

			segments, err := l.getUnSyncKnowledgeSegment(newCtx, docID, batchSize)
			if err != nil {
				return err
			}
			for len(segments) > 0 {
				logx.I(ctx, "batchSyncRealtimeKnowledge|len(segments):%d", len(segments))
				// 批量写向量到Vector
				err = l.directAddRealtimeSegmentKnowledge(newCtx, segments, appDB.Embedding.Version)
				if err != nil {
					return err
				}
				// 更新Vector同步状态
				err = l.updateSegmentSyncKnowledge(newCtx, segments, syncedKnowledge)
				if err != nil {
					return err
				}
				segments, err = l.getUnSyncKnowledgeSegment(ctx, docID, batchSize)
				if err != nil {
					return err
				}
			}
			return nil
		},
		retry.Attempts(uint(retryCount)),
		retry.OnRetry(func(n uint, err error) {
			logx.E(ctx, "batchSyncRealtimeKnowledge|retry:%d, err:%v", n, err)
		}),
	)
	if err != nil {
		logx.E(ctx, "batchSyncRealtimeKnowledge|retry.Do failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "batchSyncRealtimeKnowledge|success")
	return nil
}

// handleDocSummary 获取文档摘要
func (l *Logic) handleDocSummary(ctx context.Context, modelName string, doc *realtime.TRealtimeDoc) (summary string, statisticInfo *pb.StatisticInfo, err error) {
	request := &pb.GetDocSummaryReq{
		BotBizId:    doc.RobotID,
		ModelName:   modelName,
		PromptLimit: uint32(l.llmLogic.GetModelPromptLimit(ctx, modelName)),
		Query:       "",
	}
	summaryInfo, err := l.GetOneDocSummary(ctx, request, doc.DocID, doc.FileName)
	if err != nil {
		logx.E(ctx, "SimpleChat error: %v", err)
		return "", nil, err
	}
	summary = summaryInfo.GetDocSummary()
	if len(summaryInfo.GetStatisticInfos()) > 0 {
		statisticInfo = summaryInfo.StatisticInfos[0]
	}

	logx.I(ctx, "docSummary:%s,statisticInfo:%+v", summary, statisticInfo)
	return summary, statisticInfo, nil

}

// getUnSyncKnowledgeSegment 获取未同步Vector的segment
func (l *Logic) getUnSyncKnowledgeSegment(ctx context.Context, docID uint64, limit int) (
	[]*realtime.TRealtimeDocSegment, error) {
	logx.I(ctx, "getUnSyncKnowledgeSegment|docID:%d, limit:%d", docID, limit)
	var docSegments []*realtime.TRealtimeDocSegment
	err := l.getRealtimeDocSegmentDB(ctx).Model(&realtime.TRealtimeDocSegment{}).
		Where("doc_id = ? and is_sync_knowledge = ? and is_deleted = ?",
			docID, unSyncKnowledge, segment.SegmentIsNotDeleted).
		Limit(limit).Find(&docSegments).Error
	if err != nil {
		logx.E(ctx, "getUnSyncKnowledgeSegment|gormDB.Find failed, err:%+v", err)
		return nil, err
	}
	logx.I(ctx, "getUnSyncKnowledgeSegment|success|len(docSegments):%d", len(docSegments))
	return docSegments, nil
}

// directAddRealtimeSegmentKnowledge 新增实时文档分片知识
func (l *Logic) directAddRealtimeSegmentKnowledge(ctx context.Context, segments []*realtime.TRealtimeDocSegment,
	embeddingVersion uint64) error {
	logx.I(ctx, "directAddRealtimeSegmentKnowledge|len(segments):%d, embeddingVersion:%d",
		len(segments), embeddingVersion)
	if len(segments) == 0 {
		return nil
	}
	robotID, knowledgeData, err := l.getRealtimeKnowledgeData(ctx, segments)
	if err != nil {
		logx.E(ctx, "directAddRealtimeSegmentKnowledge|getRealtimeKnowledgeData failed, err:%+v", err)
		return err
	}
	botBizID, err := l.rawSqlDao.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		logx.E(ctx, "directAddRealtimeSegmentKnowledge|GetBotBizIDByID:%+v err:%+v", robotID, err)
		return err
	}
	req := &retrieval.AddRealTimeKnowledgeReq{
		RobotId:          robotID,
		IndexId:          entity.RealtimeSegmentVersionID,
		DocType:          realtimeKnowledgeType,
		EmbeddingVersion: embeddingVersion,
		Knowledge:        knowledgeData,
		BotBizId:         botBizID,
	}
	logx.I(ctx, "directAddRealtimeSegmentKnowledge|req:%+v", req)
	rsp, err := l.rpc.RetrievalDirectIndex.AddRealTimeKnowledge(ctx, req)
	if err != nil {
		logx.E(ctx, "directAddRealtimeSegmentKnowledge|directIndexCli.AddRealTimeKnowledge failed, "+
			"err:%+v", err)
		return err
	}
	logx.I(ctx, "directAddRealtimeSegmentKnowledge|rsp:%+v", rsp)
	return nil
}

// directDeleteRealtimeSegmentKnowledge 删除实时文档分片知识
func (l *Logic) directDeleteRealtimeSegmentKnowledge(ctx context.Context, segments []*realtime.TRealtimeDocSegment,
	embeddingVersion uint64) error {
	logx.I(ctx, "directDeleteRealtimeSegmentKnowledge|len(segments):%d, embeddingVersion:%d",
		len(segments), embeddingVersion)
	if len(segments) == 0 {
		return nil
	}
	robotID, knowledgeData, err := l.getRealtimeKnowledgeData(ctx, segments)
	if err != nil {
		logx.E(ctx, "directDeleteRealtimeSegmentKnowledge|getRealtimeKnowledgeData failed, "+
			"err:%+v", err)
		return err
	}
	botBizID, err := l.rawSqlDao.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		logx.E(ctx, "directDeleteRealtimeSegmentKnowledge|GetBotBizIDByID:%+v err:%+v", robotID, err)
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
	batchSize := config.GetMainConfig().RealtimeConfig.SyncVectorDeletedBatchSize
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
			IndexId:          entity.RealtimeSegmentVersionID,
			EmbeddingVersion: embeddingVersion,
			DocType:          realtimeKnowledgeType,
			Data:             dataBatch,
			BotBizId:         botBizID,
		}
		logx.I(ctx, "directDeleteRealtimeSegmentKnowledge|req:%+v", req)
		rsp, err := l.rpc.RetrievalDirectIndex.DeleteRealTimeKnowledge(ctx, req)
		if err != nil {
			logx.E(ctx, "directDeleteRealtimeSegmentKnowledge|directIndexCli.DeleteRealTimeKnowledge "+
				"failed, err:%+v", err)
			return err
		}
		logx.I(ctx, "directDeleteRealtimeSegmentKnowledge|rsp:%+v", rsp)
	}

	logx.I(ctx, "directDeleteRealtimeSegmentKnowledge Success!")

	return nil
}

// getRealtimeKnowledgeData 获取添加Vector的RealtimeKnowledgeData
func (l *Logic) getRealtimeKnowledgeData(ctx context.Context, segments []*realtime.TRealtimeDocSegment) (
	robotID uint64, data []*retrieval.KnowledgeData, err error) {
	logx.I(ctx, "getRealtimeKnowledgeData|len(segments):%d", len(segments))
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
				logx.I(ctx, "getRealtimeKnowledgeData|robotID:%d, docID:%d, segment:%+v, err:%+v",
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
	logx.I(ctx, "getRealtimeKnowledgeData|robotID:%d, docID:%d, len(data):%d",
		robotID, docID, len(data))
	return robotID, data, nil
}

// updateSegmentSyncKnowledge Status 更新segment同步Vector状态
func (l *Logic) updateSegmentSyncKnowledge(ctx context.Context, segments []*realtime.TRealtimeDocSegment,
	syncStatus int) error {
	logx.I(ctx, "updateSegmentSyncKnowledge|len(segments):%d, syncStatus:%d",
		len(segments), syncStatus)
	if len(segments) == 0 {
		return nil
	}
	segmentIDs := make([]uint64, 0)
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, segment.SegmentID)
	}
	err := l.getRealtimeDocSegmentDB(ctx).Model(&realtime.TRealtimeDocSegment{}).
		Where("segment_id IN ? and is_deleted = ?", segmentIDs, segment.SegmentIsNotDeleted).
		Updates(map[string]any{
			"is_sync_knowledge": syncStatus,
		}).Error
	if err != nil {
		logx.E(ctx, "updateSegmentSyncKnowledge|gormDB.Updates failed, err:%+v", err)
		return err
	}
	return err
}

// deletedRealtimeSegmentByDocIDs 删除文档片段
func (l *Logic) deletedRealtimeSegmentByDocIDs(ctx context.Context, robotID uint64, docIDs []uint64) error {
	logx.I(ctx, "deletedRealtimeSegmentByDocIDs|docIDs:%+v", docIDs)
	if len(docIDs) == 0 {
		return nil
	}
	err := l.getRealtimeDocSegmentDB(ctx).Transaction(func(tx *gorm.DB) error {
		// 删除DB
		err := tx.WithContext(ctx).Model(&realtime.TRealtimeDocSegment{}).
			Where("robot_id = ? and doc_id IN ? and is_deleted = ?",
				robotID, docIDs, segment.SegmentIsNotDeleted).
			Updates(map[string]any{
				"is_deleted": segment.SegmentIsDeleted,
			}).Error
		if err != nil {
			logx.E(ctx, "deletedRealtimeSegmentByDocIDs|gormDB.Updates failed, err:%+v", err)
			return err
		}
		err = tx.WithContext(ctx).Model(&realtime.TRealtimeDocSegmentImage{}).
			Where("robot_id = ? and doc_id IN ? and is_deleted = ?",
				robotID, docIDs, segment.SegmentIsNotDeleted).
			Updates(map[string]any{
				"is_deleted": segment.SegmentIsDeleted,
			}).Error
		if err != nil {
			logx.E(ctx, "deletedRealtimeSegmentByDocIDs|gormDB.Updates failed, err:%+v", err)
			return err
		}
		// 删除ES
		for _, docID := range docIDs {
			req := retrieval.DeleteBigDataElasticReq{
				RobotId:    robotID,
				DocId:      docID,
				Type:       retrieval.KnowledgeType_REALTIME,
				HardDelete: true,
			}
			err = l.rpc.RetrievalDirectIndex.DeleteBigDataElastic(ctx, &req)
			if err != nil {
				logx.E(ctx, "deletedRealtimeSegmentByDocIDs|DeleteBigDataElastic failed, "+
					"robotID:%d, docID:%d, err:%+v", robotID, docID, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "deletedRealtimeSegmentByDocIDs|gormDB.Transaction failed, err:%+v", err)
		return err
	}
	return nil
}

// SearchRealtimeKnowledge 实时文档检索
func (l *Logic) SearchRealtimeKnowledge(ctx context.Context, req *retrieval.RetrievalRealTimeReq) (
	*retrieval.RetrievalRealTimeRsp, error) {
	logx.I(ctx, "SearchRealtimeKnowledge|req:%+v", req)
	botBizID, err := l.rawSqlDao.GetBotBizIDByID(ctx, req.RobotId)
	if err != nil {
		logx.E(ctx, "SearchRealtimeKnowledge|GetBotBizIDByID:%+v err:%+v", req.RobotId, err)
		return nil, err
	}
	req.BotBizId = botBizID
	rsp, err := l.rpc.RetrievalDirectIndex.RetrievalRealTime(ctx, req)
	if err != nil {
		logx.E(ctx, "SearchRealtimeKnowledge|directIndexCli.RetrievalRealTime failed, err:%+v", err)
		return nil, err
	}
	logx.I(ctx, "SearchRealtimeKnowledge|rsp:%+v", rsp)
	return rsp, nil
}

// GetLinkContentsFromRealtimeSearchVectorResponse 从检索请求构造 linkContents
func (l *Logic) GetLinkContentsFromRealtimeSearchVectorResponse(ctx context.Context,
	docs []*retrieval.RetrievalRealTimeRsp_Doc,
	segmentFn func(doc *retrieval.RetrievalRealTimeRsp_Doc, segment *realtime.TRealtimeDocSegment) any,
	searchEngineFn func(doc *retrieval.RetrievalRealTimeRsp_Doc) any) ([]linker.Content, error) {
	logx.I(ctx, "GetLinkContentsFromRealtimeSearchVectorResponse|len(docs):%d", len(docs))
	linkContents := make([]linker.Content, 0, len(docs))
	var err error
	for _, doc := range docs {
		var linkContent linker.Content
		switch doc.GetDocType() {
		case entity.DocTypeSearchEngine:
			linkContent, err = l.GetSearchEngineLinkContent(
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
		case entity.DocTypeSegment:
			linkContent, err = l.getRealtimePreviewSegmentLinkContent(
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
	logx.I(ctx, "GetLinkContentsFromRealtimeSearchVectorResponse|len(linkContents):%d",
		len(linkContents))
	return linkContents, nil
}

// getRealtimePreviewSegmentLinkContent 获取实时文档合并内容
func (l *Logic) getRealtimePreviewSegmentLinkContent(ctx context.Context,
	doc *retrieval.RetrievalRealTimeRsp_Doc, fn func(segment *realtime.TRealtimeDocSegment) any) (
	linker.Content, error) {
	seg, err := l.getRealtimeDocSegmentByID(ctx, doc.GetId())
	if err != nil {
		logx.E(ctx, "getRealtimePreviewSegmentLinkContent|getRealtimeDocSegmentByID failed, "+
			"err:%v ", err)
		return linker.Content{}, err
	}
	if seg == nil {
		logx.E(ctx, "getRealtimePreviewSegmentLinkContent|getRealtimeDocSegmentByID failed, "+
			"segment is not exist")
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	if seg.DocID != doc.GetDocId() {
		logx.E(ctx, "getRealtimePreviewSegmentLinkContent|getRealtimeDocSegmentByID failed, "+
			"seg and doc ID not equal|seg.DocID: %d, doc.GetDocId():%d", seg.DocID, doc.GetDocId())
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	startIndex := seg.StartChunkIndex
	endIndex := seg.EndChunkIndex
	// 如果是bigData则对应取bigData的start和end
	if doc.GetIsBigData() {
		startIndex = int(seg.BigStart)
		endIndex = int(seg.BigEnd)
		logx.I(ctx, "getRealtimePreviewSegmentLinkContent|isBigData startIndex:%d,endIndex:%d,"+
			"segmentID:%d,docID:%d", startIndex, endIndex, doc.GetId(), seg.DocID)
	}
	return linker.Content{
		Key:    fmt.Sprintf("%d-%d-%d", entity.DocTypeSegment, seg.DocID, seg.RichTextIndex),
		Extra:  fn(seg),
		Value:  doc.GetOrgData(),
		Start:  startIndex,
		End:    endIndex,
		Prefix: seg.Title,
		Keep:   seg.LinkerKeep,
	}, nil
}

// getRealtimeDocSegmentByID 根据ID查询实时文档切片
func (l *Logic) getRealtimeDocSegmentByID(ctx context.Context, segmentID uint64) (*realtime.TRealtimeDocSegment, error) {
	logx.I(ctx, "getRealtimeDocSegmentByID|segmentID:%d", segmentID)
	var docSegments []*realtime.TRealtimeDocSegment
	err := l.getRealtimeDocSegmentDB(ctx).Model(&realtime.TRealtimeDocSegment{}).
		Where("segment_id = ? and is_deleted = ?", segmentID, segment.SegmentIsNotDeleted).
		Find(&docSegments).Error
	if err != nil {
		logx.E(ctx, "getRealtimeDocSegmentByID|gormDB.Find failed, err:%+v", err)
		return nil, err
	}
	if len(docSegments) != 1 {
		err = fmt.Errorf("has duplicate docSegments or docSegments is empty")
		return nil, err
	}
	return docSegments[0], nil
}

// getRealtimeDocBySessionDocID 根据sessionID或者文档docID查询实时文档
func (l *Logic) getRealtimeDocBySessionDocID(ctx context.Context, sessionID string,
	docIds []uint64) ([]*realtime.TRealtimeDoc, error) {
	var docInfos []*realtime.TRealtimeDoc
	db := l.getRealtimeDocDB(ctx).Model(&realtime.TRealtimeDoc{})
	if sessionID != "" {
		db = db.Where("session_id = ?", sessionID)
	}
	if len(docIds) > 0 {
		db = db.Where("doc_id in ?", docIds)
	}
	err := db.Where("is_deleted = ?", 0).Find(&docInfos).Error
	if err != nil {
		logx.E(ctx, "getRealtimeDocSegmentBySessionDocID Failed,sessionID:%s,docIDs:%+v,err:%+v", sessionID,
			docIds, err)
		return nil, err
	}
	return docInfos, nil
}

// getRealtimeDocSegmentBySessionDocID 根据sessionID或者文档docID查询实时文档切片
func (l *Logic) getRealtimeDocSegmentBySessionDocID(ctx context.Context, sessionID string,
	docIds []uint64) ([]*realtime.TRealtimeDocSegment, error) {
	var docSegments []*realtime.TRealtimeDocSegment
	db := l.getRealtimeDocSegmentDB(ctx).Model(&realtime.TRealtimeDocSegment{})
	if sessionID != "" {
		db = db.Where("session_id = ?", sessionID)
	}
	if len(docIds) > 0 {
		db = db.Where("doc_id in ?", docIds)
	}
	err := db.Where("is_deleted = ?", segment.SegmentIsNotDeleted).Find(&docSegments).Error
	if err != nil {
		logx.E(ctx, "getRealtimeDocSegmentBySessionDocID Failed,sessionID:%s,docIDs:%+v,err:%+v", sessionID,
			docIds, err)
		return nil, err
	}
	return docSegments, nil
}

func (l *Logic) CreateRealtimeDoc(ctx context.Context, realtimeDoc *realtime.TRealtimeDoc) (*realtime.TRealtimeDoc, error) {
	err := l.getRealtimeDocDB(ctx).Model(&realtime.TRealtimeDoc{}).Create(&realtimeDoc).Error
	if err != nil {
		logx.E(ctx, "CreateRealtimeDoc Failed, realtimeDoc:%+v,err:%+v", realtimeDoc, err)
		return nil, err
	}
	return realtimeDoc, nil
}

// deletedRealtimeDocByDocID 删除文档
func (l *Logic) deletedRealtimeDocByDocID(ctx context.Context, sessionID string, docIds []uint64, stageDay uint64) error {
	logx.I(ctx, "DeletedRealtimeDocByDocID| sessionID:%s,docIds: %+v", sessionID, docIds)
	db := l.getRealtimeDocDB(ctx).Model(&realtime.TRealtimeDoc{})
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
		logx.E(ctx, "DeletedRealtimeDocByDocID Failed! err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) DeletedRealtimeDocInfo(ctx context.Context, botBizID uint64, sessionID string, docIds []uint64) error {
	var err error
	app, err := l.rpc.AppAdmin.DescribeAppById(ctx, botBizID)
	// app, err := d.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		logx.E(ctx, "DeletedRealtimeDocInfo|DescribeAppByPrimaryIdWithoutNotFoundError failed, err:%+v", err)
		return err
	}
	if app == nil {
		logx.E(ctx, "DeletedRealtimeDocInfo ,RobotNotFound!")
		return errs.ErrRobotNotFound
	}

	if len(docIds) == 0 {
		docInfos, err := l.getRealtimeDocBySessionDocID(ctx, sessionID, nil)
		if err != nil {
			return err
		}
		for _, doc := range docInfos {
			docIds = append(docIds, doc.DocID)
		}
	}

	// 文档删除操作
	// 1.逻辑删除相关t_realtime_doc, 这个地方的删除特殊处理【保留半年】
	realtimeCfg := config.GetMainConfig().RealtimeConfig
	if realtimeCfg.NeedDeleteRealtimeDoc {
		err = l.deletedRealtimeDocByDocID(ctx, sessionID, docIds, uint64(realtimeCfg.RealTimeDocStageTime))
		if err != nil {
			logx.E(ctx, "DeletedRealtimeDocByDocID Failed, err:%+v", err)
			return err
		}
	}

	logx.I(ctx, "DeletedRealtimeDocInfo,appID:%+v,docIds:%+v", app.PrimaryId, docIds)

	// 获取文档片段
	segments, err := l.getRealtimeDocSegmentBySessionDocID(ctx, sessionID, docIds)
	if err != nil {
		logx.E(ctx, "getRealtimeDocSegmentBySessionDocID Failed,err:%+v", err)
		return err
	}

	// 2.删除文档片段[包括t_realtime_doc_segment 和 es]
	err = l.deletedRealtimeSegmentByDocIDs(ctx, app.PrimaryId, docIds)
	if err != nil {
		logx.E(ctx, "deletedRealtimeSegmentByDocIDs Failed, err:%+v", err)
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
	if app.IsDeleted {
		logx.I(ctx, "App is deleted! appInfo:%+v", app)
		return nil
	}

	// 3.删除Vector
	for _, segments := range docSegmentIDMap {
		err = l.directDeleteRealtimeSegmentKnowledge(ctx, segments, app.Embedding.Version)
		if err != nil {
			logx.E(ctx, "directDeleteRealtimeSegmentKnowledge Failed,err:%+v", err)
			return err
		}
	}

	logx.I(ctx, "DeletedRealtimeDocInfo Success! ")

	return nil
}

// GetRealTimeSegmentChunk 分片查询实时文档切片 用于embedding升级时拉取
func (l *Logic) GetRealTimeSegmentChunk(ctx context.Context, corpID, robotID, offset,
	limit uint64) ([]*realtime.TRealtimeDocSegment, error) {
	var docSegments []*realtime.TRealtimeDocSegment
	db := l.getRealtimeDocSegmentDB(ctx).Model(&realtime.TRealtimeDocSegment{})
	db = db.Where("corp_id = ? AND robot_id = ? AND is_deleted = ? AND id > ?",
		corpID, robotID, segment.SegmentIsNotDeleted, offset).
		Order("id ASC").Limit(int(limit))

	err := db.Find(&docSegments).Error
	if err != nil {
		logx.E(ctx, "GetRealTimeSegmentChunk Failed,corpID:%d,robotID:%d,offset:%d,limit:%d,err:%+v",
			corpID, robotID, offset, limit, err)
		return nil, err
	}
	return docSegments, nil
}
