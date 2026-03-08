package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/adp/kb/kb-config/internal/entity/realtime"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/internal/util/linker"
	"git.woa.com/adp/kb/kb-config/internal/util/markdown"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// StreamSaveDoc 实时解析文档
func (s *Service) StreamSaveDoc(server pb.Api_StreamSaveDocServer) error {
	ctx := server.Context()
	logx.I(ctx, "StreamSaveDoc|server:%+v", server)

	wg := sync.WaitGroup{}
	wg.Add(2)

	serverErrCh := make(chan error, 2)
	serverReqCh := make(chan *pb.StreamSaveDocReq)

	// 处理流式请求
	go func() {
		defer gox.Recover()
		defer wg.Done()
		defer close(serverReqCh)
		for {
			select {
			case <-ctx.Done():
				logx.I(ctx, "StreamSaveDoc|<-ctx.Done()")
				return
			default:
				// 这里不能阻塞
				req, err := server.Recv()
				if err == nil && req != nil {
					logx.I(ctx, "StreamSaveDoc|cli.Recv|req:%+v", req)
					serverReqCh <- req
				}
				if err == io.EOF {
					logx.I(ctx, "StreamSaveDoc|cli.Recv EOF return")
					return
				}
				if err != nil {
					logx.W(ctx, "StreamSaveDoc|cli.Recv faield, err:%+v", err)
					serverErrCh <- err
					return
				}
			}
		}
	}()

	// 处理实时解析
	go func() {
		defer gox.Recover()
		defer wg.Done()
		err := s.realtimeParse(ctx, server, serverReqCh)
		if err != nil {
			logx.W(ctx, "StreamSaveDoc|realtimeParse failed, err:%+v", err)
			serverErrCh <- err
		}
		return
	}()

	wg.Wait()
	for {
		select {
		case <-ctx.Done():
			logx.I(ctx, "StreamSaveDoc|<-ctx.Done()")
			return nil
		case err := <-serverErrCh:
			if err != nil {
				logx.W(ctx, "StreamSaveDoc|failed, err:%+v", err)
				return err
			}
		default:
			logx.I(ctx, "StreamSaveDoc|success")
			return nil
		}
	}
}

// realtimeParse 实时解析
func (s *Service) realtimeParse(ctx context.Context,
	server pb.Api_StreamSaveDocServer, serverReqCh chan *pb.StreamSaveDocReq) error {
	logx.I(ctx, "realtimeParse|called")
	// 实时解析chan
	parseReqChan := make(chan *realtime.ParseDocReqChan)
	parseRspChan := make(chan *realtime.ParseDocRspChan)

	wg := sync.WaitGroup{}
	wg.Add(2)
	errCh := make(chan error, 2)

	// 异步处理解析请求
	go func() {
		defer gox.Recover()
		defer wg.Done()
		if err := s.handlerParseReq(ctx, server, serverReqCh, parseReqChan); err != nil {
			logx.W(ctx, "realtimeParse|handlerParseReq faield, err:%+v", err)
			errCh <- err
		}
		return
	}()

	// 异步处理解析结果
	go func() {
		defer gox.Recover()
		defer wg.Done()
		if err := s.handlerParseRsp(ctx, server, parseReqChan, parseRspChan); err != nil {
			logx.W(ctx, "realtimeParse|handlerParseRsp faield, err:%+v", err)
			errCh <- err
		}
		return
	}()

	wg.Wait()
	for {
		select {
		case <-ctx.Done():
			logx.I(ctx, "realtimeParse|<-ctx.Done()")
			return nil
		case err := <-errCh:
			logx.W(ctx, "realtimeParse|failed, err:%+v", err)
			return err
		default:
			logx.I(ctx, "realtimeParse|suucess")
			return nil
		}
	}
}

// handlerParseReq 处理解析请求
func (s *Service) handlerParseReq(ctx context.Context, server pb.Api_StreamSaveDocServer,
	serverReqCh chan *pb.StreamSaveDocReq, parseReqChan chan *realtime.ParseDocReqChan) error {
	logx.I(ctx, "handlerParseReq|called")
	defer close(parseReqChan)
	for {
		select {
		case <-ctx.Done():
			logx.I(ctx, "handlerParseReq|<-ctx.Done()")
			return nil
		default:
			req, ok := <-serverReqCh
			if !ok {
				logx.I(ctx, "handlerParseReq|serverReqCh closed")
				return nil
			}
			parseReq, err := s.getParseDocReqFromServerReq(ctx, req)
			if err != nil {
				// 这里异常需要回包
				logx.W(ctx, "handlerParseReq|getParseDocReqFromServerReq faield, err:%+v", err)
				var e *errx.Error
				if !errors.As(err, &e) {
					err = errs.ErrRealtimeDocParseFailed
				}
				streamRsp := &pb.StreamSaveDocRsp{
					RspType: pb.StreamSaveDocRsp_TASK_RSP,
					TaskRsp: &pb.TaskRsp{
						SessionId: req.GetTaskReq().GetSessionId(),
						CosUrlId:  req.GetTaskReq().GetCosUrlId(),
						Status:    pb.TaskRsp_FAILED,
						ErrMsg:    errx.Msg(err),
					},
					IsFinal: true,
				}
				logx.I(ctx, "handlerParseReq|streamRsp:%+v", streamRsp)
				err = server.Send(streamRsp)
				if err != nil {
					logx.W(ctx, "handlerParseReq|server.Send() failed, err:%+v", err)
					return err
				}
				return nil
			}
			parseReqChan <- parseReq
		}
	}
}

// handlerParseRsp 处理解析结果
func (s *Service) handlerParseRsp(ctx context.Context, server pb.Api_StreamSaveDocServer,
	parseReqChan chan *realtime.ParseDocReqChan, parseRspChan chan *realtime.ParseDocRspChan) error {
	logx.I(ctx, "handlerParseRsp|called")
	wg := sync.WaitGroup{}
	wg.Add(2)
	errCh := make(chan error, 2)
	// 请求实时解析
	go func() {
		defer gox.Recover()
		defer wg.Done()
		err := s.docLogic.ParseRealtimeDoc(ctx, parseReqChan, parseRspChan)
		if err != nil {
			logx.W(ctx, "handlerParseRsp|ParseRealtimeDoc failed, err:%+v", err)
			errCh <- err
		}
		return
	}()
	// 处理解析结果
	go func() {
		defer gox.Recover()
		defer wg.Done()
		timeoutDuration := time.Duration(config.GetMainConfig().RealtimeConfig.ParseTimeout) * time.Hour
		timeout := time.NewTicker(timeoutDuration)
		defer timeout.Stop()
		for {
			select {
			case <-ctx.Done():
				logx.I(ctx, "handlerParseRsp|<-ctx.Done()")
				return
			case <-timeout.C:
				err := fmt.Errorf("timeout|timeoutConfig:%+v h",
					config.GetMainConfig().RealtimeConfig.ParseTimeout)
				logx.I(ctx, "handlerParseRsp|failed, err:%+v", err)
				errCh <- err
				return
			case rsp, ok := <-parseRspChan:
				if !ok {
					return
				}
				logx.I(ctx, "handlerParseRsp|parseRsp:%+v", rsp)
				streamRsp := &pb.StreamSaveDocRsp{
					RspType: rsp.Type,
					TaskRsp: &pb.TaskRsp{
						SessionId: rsp.SessionID,
						CosUrlId:  rsp.CosUrlID,
						Progress: &pb.Progress{
							Progress: rsp.Progress.GetProgress(),
							Message:  rsp.Progress.GetMessage(),
						},
						Status:        rsp.Status,
						DocId:         rsp.DocID,
						ErrMsg:        rsp.ErrMsg,
						Summary:       rsp.Summary,
						StatisticInfo: rsp.StatisticInfo,
						PageCount:     rsp.PageCount,
					},
					IsFinal: func() bool {
						return rsp.Type == pb.StreamSaveDocRsp_TASK_RSP
					}(),
				}
				logx.I(ctx, "handlerParseRsp|streamRsp:%+v", streamRsp)
				err := server.Send(streamRsp)
				if err != nil {
					logx.W(ctx, "handlerParseRsp|server.Send() failed, err:%+v", err)
					errCh <- err
					return
				}
			}
		}
	}()

	wg.Wait()
	for {
		select {
		case err := <-errCh:
			logx.W(ctx, "handlerParseRsp|failed, err:%+v", err)
			return err
		default:
			logx.I(ctx, "handlerParseRsp|success")
			return nil
		}
	}
}

// getParseDocReqFromServerReq 获取文档解析请求
func (s *Service) getParseDocReqFromServerReq(ctx context.Context, req *pb.StreamSaveDocReq) (*realtime.ParseDocReqChan, error) {
	logx.I(ctx, "getParseDocReqFromServerReq|req:%+v", req)

	// embedding库是否可写入校验
	appid := convx.Uint64ToString(req.GetTaskReq().GetBotBizId())
	app, err := s.svc.DescribeAppAndCheckCorp(ctx, appid)
	if err != nil {
		logx.W(ctx, "getParseDocReqFromServerReq|getAppByBizID failed, err:%+v", err)
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}

	parseReqChan := &realtime.ParseDocReqChan{}
	switch req.GetReqType() {
	case pb.StreamSaveDocReq_TASK_PARSE:

		reqFileType := strings.ToLower(strings.TrimSpace(req.GetTaskReq().GetFileType()))
		maxSize, ok := config.GetMainConfig().RealtimeConfig.FileTypeSize[reqFileType]
		// 文件类型校验
		if !ok {
			logx.W(ctx, "getParseDocReqFromServerReq|CheckRealtimeDocFileType not support fileType, reqFileType:%+v", reqFileType)
			return nil, errs.ErrUnSupportFileType
		}

		// 文件大小限制
		if req.GetTaskReq().GetSize() > maxSize {
			return nil, errs.ErrFileSizeTooBig
		}
		// 文件后缀校验
		if err = s.checkCanSaveDoc(ctx, app.StaffID, req.GetTaskReq().GetFileName(),
			req.GetTaskReq().GetFileType()); err != nil {
			logx.W(ctx, "getParseDocReqFromServerReq|checkCanSaveDoc failed, err:%+v", err)
			return nil, err
		}
		// 字符总数校验
		if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
			logx.W(ctx, "getParseDocReqFromServerReq|CheckKnowledgeBaseQuota failed, err:%+v", err)
			return nil, common.ConvertErrMsg(ctx, s.rpc, 0, app.CorpPrimaryId, errs.ErrOverCharacterSizeLimit)
		}
		// cos信息校验
		if err = s.docLogic.CheckRealtimeStorageInfo(ctx, req.GetTaskReq().GetCosBucket(), req.GetTaskReq().GetCosUrl(),
			req.GetTaskReq().GetETag(), app); err != nil {
			logx.W(ctx, "getParseDocReqFromServerReq|CheckRealtimeStorageInfo failed, err:%+v", err)
			return nil, err
		}
		docID := idgen.GetId()
		realtimeDoc := buildRealtimeDoc(req, req.GetTaskReq().GetSessionId(), docID, app.CorpPrimaryId, app.StaffID, app.PrimaryId)
		realtimeDoc, err = s.docLogic.CreateRealtimeDoc(ctx, realtimeDoc)
		if err != nil {
			return nil, errs.ErrSystem
		}
		parseReqChan = &realtime.ParseDocReqChan{
			Type:      pb.StreamSaveDocReq_TASK_PARSE,
			Doc:       *realtimeDoc,
			ModelName: req.GetTaskReq().GetModelName(),
		}
	case pb.StreamSaveDocReq_TASK_CANCEL:
		realtimeDoc := buildRealtimeDoc(req, req.GetTaskReq().GetSessionId(), 0, app.CorpPrimaryId, app.StaffID, app.PrimaryId)
		parseReqChan = &realtime.ParseDocReqChan{
			Type: pb.StreamSaveDocReq_TASK_CANCEL,
			Doc:  *realtimeDoc,
		}
	default:
		err = fmt.Errorf("illegal reqType:%v", req.GetReqType())
		logx.W(ctx, "getParseDocReqFromServerReq|failed, err:%+v", err)
		return nil, err
	}
	logx.I(ctx, "getParseDocReqFromServerReq|parseReqChan:%+v", parseReqChan)
	return parseReqChan, nil
}

// GetDocFullText 获取文档全文
func (s *Service) GetDocFullText(ctx context.Context, req *pb.GetDocFullTextReq) (
	rsp *pb.GetDocFullTextRsp, err error) {
	logx.I(ctx, "GetDocFullText|req:%+v", req)
	if req.GetDocId() == 0 {
		return nil, errs.ErrDocIDFail
	}
	rsp = &pb.GetDocFullTextRsp{}
	// 目前只支持实时文档
	switch req.StorageType {
	case pb.StorageType_STORAGE_REALTIME:
		doc, err := s.docLogic.GetRealtimeDocByID(ctx, req.DocId)
		if err != nil {
			logx.W(ctx, "GetDocFullText|GetRealtimeDocByID failed, err:%+v", err)
			return nil, err
		}
		logx.I(ctx, "GetDocFullText|doc:%+v", doc)
		// 2.4.0 放开sessionID一致性校验：针对文档上传中清空对话，上传完再发送的场景，sessionID是对不上的
		// if doc.SessionID != req.SessionId {
		//	err = fmt.Errorf("doc.SessionID:%s is not equals req.SessionID:%s", doc.SessionID, req.SessionId)
		//	logx.W(ctx, "GetDocFullText|sessionID illegal, err:%+v", err)
		//	return nil, err
		// }
		rsp.DocId = doc.DocID
		if doc.CharSize > 0 {
			if int64(doc.CharSize) <= req.MaxSize {
				rsp.IsCharSizeExceed = false
				rsp.DocFullText, rsp.TextPlaceholders =
					s.extractFullTextPlaceholder(ctx, doc.FileFullText, req.GetUsePlaceholder())
			} else {
				rsp.IsCharSizeExceed = true
				docLength := len([]rune(doc.FileFullText))
				cutLength := gox.IfElse(docLength > int(req.MaxSize), int(req.MaxSize), docLength)
				fullText := string([]rune(doc.FileFullText)[:cutLength])
				rsp.DocFullText, rsp.TextPlaceholders =
					s.extractFullTextPlaceholder(ctx, fullText, req.GetUsePlaceholder())
			}
		} else {
			err = fmt.Errorf("doc.CharSize:%d illegal", doc.CharSize)
			logx.W(ctx, "GetDocFullText|doc.CharSize illegal, err:%+v", err)
			return nil, err
		}
		return rsp, nil
	default:
		err = fmt.Errorf("StorageType:%+v is illegal", req.StorageType)
		return nil, err
	}
}

// extractFullTextPlaceholder 抽取文档全文的站位符号
func (s *Service) extractFullTextPlaceholder(ctx context.Context, fullText string, usePlaceholder bool) (
	string, []*pb.Placeholder) {
	logx.I(ctx, "extractFullTextPlaceholder|fullText:%s, usePlaceholder:%v", fullText, usePlaceholder)
	placeholders := make(map[string]string, 0)
	textPlaceholders := make([]*pb.Placeholder, 0)
	c, p := markdown.New(
		markdown.WithLinkPlaceholder(config.App().DocPlaceholder.Link),
		markdown.WithImgPlaceholder(config.App().DocPlaceholder.Img),
	).ExtractLinkWithPlaceholder([]byte(fullText))
	for _, v := range p {
		placeholders[v.Key] = v.Value
	}
	newFullText := string(c)
	realtimeBucket, err := s.s3.GetBucketWithTypeKey(ctx, entity.RealtimeStorageTypeKey)
	if err != nil {
		logx.W(ctx, "extractFullTextPlaceholder|GetBucketWithTypeKey failed, err:%+v", err)
		return fullText, nil
	}
	for ph, oriURL := range placeholders {
		// oriURL 去掉最前面的( 和最后面的)
		result := strings.TrimPrefix(oriURL, "(")
		result = strings.TrimSuffix(result, ")")
		shortURL, err := s.docLogic.ConvertImage2ShortURL(ctx, realtimeBucket, result)
		if err != nil {
			logx.W(ctx, "extractFullTextPlaceholder|ConvertImage2ShortURL failed, err:%+v", err)
			shortURL = oriURL
		}
		shortURL = "(" + shortURL + ")"
		placeholders[ph] = shortURL
		newFullText = strings.ReplaceAll(newFullText, ph, shortURL) //先把原始链接替换成短链
		textPlaceholders = append(textPlaceholders, &pb.Placeholder{
			Key:   ph,
			Value: shortURL,
		})
	}
	if !usePlaceholder {
		return newFullText, nil
	}
	// 如果需要换成占位符，再处理一次
	for ph, shortURL := range placeholders {
		newFullText = strings.ReplaceAll(newFullText, shortURL, ph)
	}
	logx.I(ctx, "extractFullTextPlaceholder|fullText:%s, placeholders:%+v", newFullText, placeholders)
	return newFullText, textPlaceholders
}

// SearchRealtime 实时文档检索
func (s *Service) SearchRealtime(ctx context.Context, req *pb.SearchRealtimeReq) (rsp *pb.SearchRealtimeRsp, err error) {
	logx.I(ctx, "SearchRealtime|req:%+v", req)
	filterKey, err := s.getSearchRealtimeFilterKey(ctx, req.GetFilterKey())
	if err != nil {
		return nil, err
	}
	switch filterKey {
	case entity.AppSearchPreviewFilterKey:
		app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppTestScenes)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		if app.QaConfig == nil {
			return nil, errs.ErrAppTypeSupportFilters
		}
		newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
		return s.searchRealtimePreview(newCtx, app, filterKey, req)
	case entity.AppSearchReleaseFilterKey:
		app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetBotBizId(), entity.AppReleaseScenes)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		if app.QaConfig == nil {
			return nil, errs.ErrAppTypeSupportFilters
		}
		newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
		return s.searchRealtimeRelease(newCtx, app, filterKey, req)
	default:
		return nil, err
	}
}

// getSearchRealtimeFilterKey 获取实时文档检索的Filters
// 目前完全复用离线的配置，后续有需要再拆开
func (s *Service) getSearchRealtimeFilterKey(ctx context.Context, filterKey string) (string, error) {
	logx.I(ctx, "getSearchRealtimeFilterKey|filterKey:%s", filterKey)
	if filterKey == "" || filterKey == entity.AppSearchRealtimePreviewFilterKey {
		// 复用离线评测库
		return entity.AppSearchPreviewFilterKey, nil
	}
	if filterKey == entity.AppSearchRealtimeReleaseFilterKey {
		// 复用离线正式库
		return entity.AppSearchReleaseFilterKey, nil
	}
	// 非法的filterKey
	err := fmt.Errorf("illegal filterKey:%s", filterKey)
	logx.W(ctx, "getSearchRealtimeFilterKey|err:%+v", err)
	return "", errs.ErrAppTypeSupportFilters
}

// realtimeSearchIndexMap 实时文档检索的Index映射 @harryhlli
var realtimeSearchIndexMap = map[uint32]uint64{
	entity.DocTypeSegment:         entity.RealtimeSegmentVersionID,      // type:2 -> index:9
	entity.DocTypeImage:           entity.RealtimeSegmentImageVersionID, // type:6 -> index:11
	entity.DocTypeTextSearchImage: entity.RealtimeSegmentImageVersionID, // type:7 -> index:11
}

// searchRealtimePreview 实时文档检索--评测库
func (s *Service) searchRealtimePreview(ctx context.Context, app *entity.App, filterKey string,
	req *pb.SearchRealtimeReq) (rsp *pb.SearchRealtimeRsp, err error) {
	logx.I(ctx, "searchRealtimePreview|app:%+v, filterKey:%s", app, filterKey)
	// filters
	filter, ok := app.QaConfig.Filters[filterKey]
	if !ok {
		logx.W(ctx, "searchRealtimePreview|app.GetFilter failed,filter not found")
		return nil, fmt.Errorf("filter not found")
	}
	filters := make([]*retrieval.RetrievalRealTimeReq_Filter, 0, len(filter.Filter))
	for _, f := range filter.Filter {
		if f.DocType == entity.DocTypeSearchEngine {
			continue
		}
		// 实时文档只检索文档段、文档段图片、文搜图 @harryhlli
		if _, ok := realtimeSearchIndexMap[f.DocType]; !ok {
			continue
		}
		if filterKey == entity.AppSearchPreviewFilterKey && !f.IsEnabled {
			continue
		}
		filters = append(filters, &retrieval.RetrievalRealTimeReq_Filter{
			IndexId:    realtimeSearchIndexMap[f.DocType], // 实时文档的IndexId需要根据映射关系转换
			Confidence: f.Confidence,
			TopN:       f.TopN,
			DocType:    f.DocType,
		})
	}
	// rerank
	rerank, err := getRerankModel(app)
	if err != nil {
		logx.W(ctx, "searchRealtimePreview|GetRerankModel failed, err: %v", err)
		return nil, err
	}
	// 调用Vector检索
	searchRealtimeRsp, err := s.docLogic.SearchRealtimeKnowledge(ctx, &retrieval.RetrievalRealTimeReq{
		RobotId:          app.PrimaryId,
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             filter.TopN,
		EmbeddingVersion: app.Embedding.Version,
		Rerank: &retrieval.RetrievalRealTimeReq_Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		// 传FilterKey区分知识检索和已采纳问题直接回复
		FilterKey: filterKey,
		Labels:    convertSearchRealtimeLabel(req.GetLabels()),
		ImageUrls: req.GetImageUrls(),
		BotBizId:  app.BizId,
		LabelExpression: fillLabelWithoutGeneralVectorExpression(
			convertPbLabel(req.GetLabels()), entity.AppSearchConditionAnd),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.QaConfig.SearchStrategy),
		ModelName:      req.GetModelName(),
	})
	if err != nil {
		logx.W(ctx, "searchRealtimePreview|dao.SearchRealtimeKnowledge failed, err: %v", err)
		return nil, err
	}
	docs, err := s.getRealtimeRetrievalRspDoc(ctx, searchRealtimeRsp)
	if err != nil {
		logx.W(ctx, "searchRealtimePreview|getRealtimeRetrievalRspDoc failed, err: %v", err)
		return nil, err
	}
	// 检索后处理
	rsp = &pb.SearchRealtimeRsp{Docs: docs}
	return searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp), nil
}

// searchRealtimeRelease 实时文档检索--正式库
func (s *Service) searchRealtimeRelease(ctx context.Context, app *entity.App, filterKey string,
	req *pb.SearchRealtimeReq) (rsp *pb.SearchRealtimeRsp, err error) {
	logx.I(ctx, "searchRealtimeRelease|app:%+v, filterKey:%s", app, filterKey)
	// filters
	filter, ok := app.QaConfig.Filters[filterKey]
	if !ok {
		logx.W(ctx, "searchRealtimeRelease|app.GetFilter failed, not found")
		return nil, fmt.Errorf("filter not found")
	}
	filters := make([]*retrieval.RetrievalRealTimeReq_Filter, 0, len(filter.Filter))
	for _, f := range filter.Filter {
		if f.DocType == entity.DocTypeSearchEngine {
			continue
		}
		// 实时文档只检索文档段、文档段图片、文搜图 @harryhlli
		if _, ok := realtimeSearchIndexMap[f.DocType]; !ok {
			continue
		}
		if filterKey == entity.AppSearchPreviewFilterKey && !f.IsEnabled {
			continue
		}
		filters = append(filters, &retrieval.RetrievalRealTimeReq_Filter{
			IndexId:    realtimeSearchIndexMap[f.DocType], // 实时文档的IndexId需要根据映射关系转换
			Confidence: f.Confidence,
			TopN:       f.TopN,
			DocType:    f.DocType,
		})
	}
	// rerank
	rerank, err := getRerankModel(app)
	if err != nil {
		logx.W(ctx, "searchRealtimeRelease|GetRerankModel failed, err: %v", err)
		return nil, err
	}
	// 调用Vector检索
	searchRealtimeRsp, err := s.docLogic.SearchRealtimeKnowledge(ctx, &retrieval.RetrievalRealTimeReq{
		RobotId:          app.PrimaryId,
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             filter.TopN,
		EmbeddingVersion: app.Embedding.Version,
		Rerank: &retrieval.RetrievalRealTimeReq_Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		// 传FilterKey区分知识检索和已采纳问题直接回复
		FilterKey: filterKey,
		Labels:    convertSearchRealtimeLabel(req.GetLabels()),
		ImageUrls: req.GetImageUrls(),
		BotBizId:  app.BizId,
		LabelExpression: fillLabelWithoutGeneralVectorExpression(
			convertPbLabel(req.GetLabels()), entity.AppSearchConditionAnd),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.QaConfig.SearchStrategy),
		ModelName:      req.GetModelName(),
	})
	if err != nil {
		logx.W(ctx, "searchRealtimeRelease|dao.SearchRealtimeKnowledge failed, err: %v", err)
		return nil, err
	}
	docs, err := s.getRealtimeRetrievalRspDoc(ctx, searchRealtimeRsp)
	if err != nil {
		logx.W(ctx, "searchRealtimeRelease|getRealtimeRetrievalRspDoc failed, err: %v", err)
		return nil, err
	}
	// 检索后处理
	rsp = &pb.SearchRealtimeRsp{Docs: docs}
	return searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp), nil
}

// getRealtimeRetrievalRspDoc 获取实时文档评测库检索结果
func (s *Service) getRealtimeRetrievalRspDoc(ctx context.Context, rsp *retrieval.RetrievalRealTimeRsp) (
	[]*pb.SearchRealtimeRsp_Doc, error) {
	linkContents, err := s.docLogic.GetLinkContentsFromRealtimeSearchVectorResponse(
		ctx, rsp.GetDocs(),
		func(doc *retrieval.RetrievalRealTimeRsp_Doc, segment *realtime.TRealtimeDocSegment) any {
			return &pb.SearchRealtimeRsp_Doc{
				DocId:        segment.DocID,
				DocType:      doc.GetDocType(),
				RelatedId:    doc.GetId(),
				OrgData:      doc.GetOrgData(),
				Confidence:   doc.GetConfidence(),
				RelatedBizId: segment.SegmentID,
				IsBigData:    doc.GetIsBigData(),
				Extra:        convertRealtimeRetrievalExtra(doc.GetExtra()),
				ImageUrls:    doc.GetImageUrls(),
				ResultType:   convertRealtimeRetrievalResultType(doc.GetResultType()),
			}
		},
		func(doc *retrieval.RetrievalRealTimeRsp_Doc) any {
			return &pb.SearchRealtimeRsp_Doc{
				DocType:    doc.GetDocType(),
				Question:   doc.GetQuestion(),
				Answer:     doc.GetAnswer(),
				Confidence: doc.GetConfidence(),
				Extra:      convertRealtimeRetrievalExtra(doc.GetExtra()),
				ImageUrls:  doc.GetImageUrls(),
				ResultType: convertRealtimeRetrievalResultType(doc.GetResultType()),
			}
		},
	)
	if err != nil {
		return nil, err
	}
	docs := linker.Link(ctx, linkContents,
		func(t *pb.SearchRealtimeRsp_Doc, v linker.Content) *pb.SearchRealtimeRsp_Doc {
			t.OrgData = v.Value
			return t
		})
	return docs, nil
}

// convertRealtimeRetrievalExtra 转换实时文档检索额外信息
func convertRealtimeRetrievalExtra(extra *retrieval.RetrievalExtra) *pb.RetrievalExtra {
	if extra == nil {
		return nil
	}
	return &pb.RetrievalExtra{
		EmbRank:     extra.EmbRank,
		EsScore:     extra.EsScore,
		EsRank:      extra.EsRank,
		RerankScore: extra.RerankScore,
		RerankRank:  extra.RerankRank,
		RrfScore:    extra.RrfScore,
		RrfRank:     extra.RrfRank,
	}
}

// convertRealtimeRetrievalResultType 转换结果类型
func convertRealtimeRetrievalResultType(resultType retrieval.RetrievalResultType) pb.RetrievalResultType {
	return pb.RetrievalResultType(resultType.Number())
}

// convertSearchRealtimeLabel 转换实时检索标签的结构体
func convertSearchRealtimeLabel(labels []*pb.VectorLabel) []*retrieval.SearchVectorLabel {
	searchLabels := make([]*retrieval.SearchVectorLabel, 0, len(labels))
	for _, label := range labels {
		searchLabels = append(searchLabels, &retrieval.SearchVectorLabel{
			Name:   label.Name,
			Values: label.Values,
		})
	}
	return searchLabels
}

// convertSearchRealtimeLabel 转换实时检索标签的结构体
func convertPbLabel(labels []*pb.VectorLabel) []*pb.VectorLabel {
	pbLabels := make([]*pb.VectorLabel, 0, len(labels))
	for _, label := range labels {
		pbLabels = append(pbLabels, &pb.VectorLabel{
			Name:   label.Name,
			Values: label.Values,
		})
	}
	return pbLabels
}

// DeleteRealtimeDoc 删除文档
func (s *Service) DeleteRealtimeDoc(ctx context.Context, req *pb.DeleteRealtimeDocReq) (
	*pb.DeleteRealtimeDocRsp, error) {
	logx.I(ctx, "DeleteRealtimeDoc req:%+v", req)
	if len(req.GetSessionId()) == 0 && len(req.GetDocIds()) == 0 {
		return nil, errs.ErrParams
	}

	if req.GetBotBizId() == 0 {
		return nil, errs.ErrParams
	}

	newCtx := trpc.CloneContext(ctx)

	go func() {
		defer gox.Recover()
		s.docLogic.DeletedRealtimeDocInfo(newCtx, req.GetBotBizId(), req.GetSessionId(), req.GetDocIds())
	}()
	return new(pb.DeleteRealtimeDocRsp), nil
}

func buildRealtimeDoc(req *pb.StreamSaveDocReq, sessionID string, docID, corpID, staffID,
	robotID uint64) *realtime.TRealtimeDoc {
	return &realtime.TRealtimeDoc{
		StaffID:   staffID,
		CorpID:    corpID,
		DocID:     docID,
		SessionID: sessionID,
		CosUrlID:  req.GetTaskReq().GetCosUrlId(),
		RobotID:   robotID,
		FileName:  req.GetTaskReq().GetFileName(),
		FileType:  req.GetTaskReq().GetFileType(),
		FileSize:  req.GetTaskReq().GetSize(),
		Bucket:    req.GetTaskReq().GetCosBucket(),
		CosUrl:    req.GetTaskReq().GetCosUrl(),
		CosHash:   req.GetTaskReq().GetCosHash(),
		OpType:    int32(req.GetReqType()),
		Status:    realtime.RealDocStatusInit,
	}
}

// checkCanSaveDoc 判断用户是否能上传文档
func (s *Service) checkCanSaveDoc(ctx context.Context, staffID uint64, fileName, fileType string) error {
	staff, err := s.rpc.PlatformAdmin.GetStaffByID(ctx, staffID)
	if err != nil || staff == nil {
		return errs.ErrStaffNotFound
	}
	if len(strings.TrimSuffix(fileName, "."+fileType)) == 0 {
		return errs.ErrInvalidFileName
	}
	if !util.CheckFileType(ctx, fileName, fileType) {
		return errs.ErrFileExtNotMatch
	}

	return nil
}
