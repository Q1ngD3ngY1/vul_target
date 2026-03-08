// Package api TODO
// @Author: halelv
// @Date: 2024/5/16 21:04
package api

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"io"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/markdown"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/linker"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/realtime"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/errors"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// StreamSaveDoc 实时解析文档
func (s *Service) StreamSaveDoc(server pb.Api_StreamSaveDocServer) error {
	ctx := server.Context()
	log.InfoContextf(ctx, "StreamSaveDoc|server:%+v", server)

	wg := sync.WaitGroup{}
	wg.Add(2)

	serverErrCh := make(chan error, 2)
	serverReqCh := make(chan *knowledge.StreamSaveDocReq)

	// 处理流式请求
	go func() {
		defer errors.PanicHandler()
		defer wg.Done()
		defer close(serverReqCh)
		for {
			select {
			case <-ctx.Done():
				log.InfoContextf(ctx, "StreamSaveDoc|<-ctx.Done()")
				return
			default:
				// 这里不能阻塞
				req, err := server.Recv()
				if err == nil && req != nil {
					log.InfoContextf(ctx, "StreamSaveDoc|cli.Recv|req:%+v", req)
					serverReqCh <- req
				}
				if err == io.EOF {
					log.InfoContextf(ctx, "StreamSaveDoc|cli.Recv EOF return")
					return
				}
				if err != nil {
					log.WarnContextf(ctx, "StreamSaveDoc|cli.Recv faield, err:%+v", err)
					serverErrCh <- err
					return
				}
			}
		}
	}()

	// 处理实时解析
	go func() {
		defer errors.PanicHandler()
		defer wg.Done()
		err := s.realtimeParse(ctx, server, serverReqCh)
		if err != nil {
			log.WarnContextf(ctx, "StreamSaveDoc|realtimeParse failed, err:%+v", err)
			serverErrCh <- err
		}
		return
	}()

	wg.Wait()
	for {
		select {
		case <-ctx.Done():
			log.InfoContextf(ctx, "StreamSaveDoc|<-ctx.Done()")
			return nil
		case err := <-serverErrCh:
			if err != nil {
				log.WarnContextf(ctx, "StreamSaveDoc|failed, err:%+v", err)
				return err
			}
		default:
			log.InfoContextf(ctx, "StreamSaveDoc|success")
			return nil
		}
	}
}

// realtimeParse 实时解析
func (s *Service) realtimeParse(ctx context.Context,
	server pb.Api_StreamSaveDocServer, serverReqCh chan *knowledge.StreamSaveDocReq) error {
	log.InfoContextf(ctx, "realtimeParse|called")
	// 实时解析chan
	parseReqChan := make(chan *realtime.ParseDocReqChan)
	parseRspChan := make(chan *realtime.ParseDocRspChan)

	wg := sync.WaitGroup{}
	wg.Add(2)
	errCh := make(chan error, 2)

	// 异步处理解析请求
	go func() {
		defer errors.PanicHandler()
		defer wg.Done()
		if err := s.handlerParseReq(ctx, server, serverReqCh, parseReqChan); err != nil {
			log.WarnContextf(ctx, "realtimeParse|handlerParseReq faield, err:%+v", err)
			errCh <- err
		}
		return
	}()

	// 异步处理解析结果
	go func() {
		defer errors.PanicHandler()
		defer wg.Done()
		if err := s.handlerParseRsp(ctx, server, parseReqChan, parseRspChan); err != nil {
			log.WarnContextf(ctx, "realtimeParse|handlerParseRsp faield, err:%+v", err)
			errCh <- err
		}
		return
	}()

	wg.Wait()
	for {
		select {
		case <-ctx.Done():
			log.InfoContextf(ctx, "realtimeParse|<-ctx.Done()")
			return nil
		case err := <-errCh:
			log.WarnContextf(ctx, "realtimeParse|failed, err:%+v", err)
			return err
		default:
			log.InfoContextf(ctx, "realtimeParse|suucess")
			return nil
		}
	}
}

// handlerParseReq 处理解析请求
func (s *Service) handlerParseReq(ctx context.Context, server pb.Api_StreamSaveDocServer,
	serverReqCh chan *knowledge.StreamSaveDocReq, parseReqChan chan *realtime.ParseDocReqChan) error {
	log.InfoContextf(ctx, "handlerParseReq|called")
	defer close(parseReqChan)
	for {
		select {
		case <-ctx.Done():
			log.InfoContextf(ctx, "handlerParseReq|<-ctx.Done()")
			return nil
		default:
			req, ok := <-serverReqCh
			if !ok {
				log.InfoContextf(ctx, "handlerParseReq|serverReqCh closed")
				return nil
			}
			parseReq, err := s.getParseDocReqFromServerReq(ctx, req)
			if err != nil {
				// 这里异常需要回包
				log.WarnContextf(ctx, "handlerParseReq|getParseDocReqFromServerReq faield, err:%+v", err)
				var e *terrs.Error
				if !errors.As(err, &e) {
					err = errs.ErrRealtimeDocParseFailed
				}
				streamRsp := &knowledge.StreamSaveDocRsp{
					RspType: knowledge.StreamSaveDocRsp_TASK_RSP,
					TaskRsp: &knowledge.TaskRsp{
						SessionId: req.GetTaskReq().GetSessionId(),
						CosUrlId:  req.GetTaskReq().GetCosUrlId(),
						Status:    knowledge.TaskRsp_FAILED,
						ErrMsg:    terrs.Msg(err),
					},
					IsFinal: true,
				}
				log.InfoContextf(ctx, "handlerParseReq|streamRsp:%+v", streamRsp)
				err = server.Send(streamRsp)
				if err != nil {
					log.WarnContextf(ctx, "handlerParseReq|server.Send() failed, err:%+v", err)
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
	log.InfoContextf(ctx, "handlerParseRsp|called")
	wg := sync.WaitGroup{}
	wg.Add(2)
	errCh := make(chan error, 2)
	// 请求实时解析
	go func() {
		defer errors.PanicHandler()
		defer wg.Done()
		err := s.dao.ParseRealtimeDoc(ctx, parseReqChan, parseRspChan)
		if err != nil {
			log.WarnContextf(ctx, "handlerParseRsp|ParseRealtimeDoc failed, err:%+v", err)
			errCh <- err
		}
		return
	}()
	// 处理解析结果
	go func() {
		defer errors.PanicHandler()
		defer wg.Done()
		timeoutDuration := time.Duration(utilConfig.GetMainConfig().RealtimeConfig.ParseTimeout) * time.Hour
		timeout := time.NewTicker(timeoutDuration)
		defer timeout.Stop()
		for {
			select {
			case <-ctx.Done():
				log.InfoContextf(ctx, "handlerParseRsp|<-ctx.Done()")
				return
			case <-timeout.C:
				err := fmt.Errorf("timeout|timeoutConfig:%+v h",
					utilConfig.GetMainConfig().RealtimeConfig.ParseTimeout)
				log.InfoContextf(ctx, "handlerParseRsp|failed, err:%+v", err)
				errCh <- err
				return
			case rsp, ok := <-parseRspChan:
				if !ok {
					return
				}
				log.InfoContextf(ctx, "handlerParseRsp|parseRsp:%+v", rsp)
				streamRsp := &knowledge.StreamSaveDocRsp{
					RspType: rsp.Type,
					TaskRsp: &knowledge.TaskRsp{
						SessionId: rsp.SessionID,
						CosUrlId:  rsp.CosUrlID,
						Progress: &knowledge.Progress{
							Progress: rsp.Progress.GetProgress(),
							Message:  rsp.Progress.GetMessage(),
						},
						Status:        rsp.Status,
						DocId:         rsp.DocID,
						ErrMsg:        rsp.ErrMsg,
						Summary:       rsp.Summary,
						StatisticInfo: rsp.StatisticInfo,
					},
					IsFinal: func() bool {
						return rsp.Type == knowledge.StreamSaveDocRsp_TASK_RSP
					}(),
				}
				log.InfoContextf(ctx, "handlerParseRsp|streamRsp:%+v", streamRsp)
				err := server.Send(streamRsp)
				if err != nil {
					log.WarnContextf(ctx, "handlerParseRsp|server.Send() failed, err:%+v", err)
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
			log.WarnContextf(ctx, "handlerParseRsp|failed, err:%+v", err)
			return err
		default:
			log.InfoContextf(ctx, "handlerParseRsp|success")
			return nil
		}
	}
}

// getParseDocReqFromServerReq 获取文档解析请求
func (s *Service) getParseDocReqFromServerReq(ctx context.Context, req *knowledge.StreamSaveDocReq) (
	*realtime.ParseDocReqChan, error) {
	log.InfoContextf(ctx, "getParseDocReqFromServerReq|req:%+v", req)

	// embedding库是否可写入校验
	app, err := s.getAppByAppBizID(ctx, req.GetTaskReq().GetBotBizId())
	if err != nil {
		log.WarnContextf(ctx, "getParseDocReqFromServerReq|getAppByBizID failed, err:%+v", err)
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}

	parseReqChan := &realtime.ParseDocReqChan{}
	switch req.GetReqType() {
	case knowledge.StreamSaveDocReq_TASK_PARSE:

		reqFileType := strings.ToLower(strings.TrimSpace(req.GetTaskReq().GetFileType()))
		maxSize, ok := utilConfig.GetMainConfig().RealtimeConfig.FileTypeSize[reqFileType]
		// 文件类型校验
		if !ok {
			log.WarnContextf(ctx, "getParseDocReqFromServerReq|CheckRealtimeDocFileType not support fileType, reqFileType:%+v", reqFileType)
			return nil, errs.ErrUnSupportFileType
		}

		// 文件大小限制
		if req.GetTaskReq().GetSize() > maxSize {
			return nil, errs.ErrFileSizeTooBig
		}
		// 文件后缀校验
		if err = s.checkCanSaveDoc(ctx, app.StaffID, req.GetTaskReq().GetFileName(),
			req.GetTaskReq().GetFileType()); err != nil {
			log.WarnContextf(ctx, "getParseDocReqFromServerReq|checkCanSaveDoc failed, err:%+v", err)
			return nil, err
		}
		// 字符总数校验
		if err = service.CheckIsUsedCharSizeExceeded(ctx, s.dao, app.BusinessID, app.CorpID); err != nil {
			log.WarnContextf(ctx, "getParseDocReqFromServerReq|isUsedCharSizeExceeded failed, err:%+v", err)
			return nil, s.dao.ConvertErrMsg(ctx, 0, app.CorpID, errs.ErrOverCharacterSizeLimit)
		}
		// cos信息校验
		if err = s.dao.CheckRealtimeStorageInfo(ctx, req.GetTaskReq().GetCosBucket(), req.GetTaskReq().GetCosUrl(),
			req.GetTaskReq().GetETag(), app); err != nil {
			log.WarnContextf(ctx, "getParseDocReqFromServerReq|CheckRealtimeStorageInfo failed, err:%+v", err)
			return nil, err
		}
		docID := s.dao.GenerateSeqID()
		realtimeDoc := buildRealtimeDoc(req, req.GetTaskReq().GetSessionId(), docID, app.CorpID, app.StaffID, app.ID)
		realtimeDoc, err = s.dao.CreateRealtimeDoc(ctx, realtimeDoc)
		if err != nil {
			return nil, errs.ErrSystem
		}
		parseReqChan = &realtime.ParseDocReqChan{
			Type:      knowledge.StreamSaveDocReq_TASK_PARSE,
			Doc:       *realtimeDoc,
			ModelName: req.GetTaskReq().GetModelName(),
		}
	case knowledge.StreamSaveDocReq_TASK_CANCEL:
		realtimeDoc := buildRealtimeDoc(req, req.GetTaskReq().GetSessionId(), 0, app.CorpID, app.StaffID, app.ID)
		parseReqChan = &realtime.ParseDocReqChan{
			Type: knowledge.StreamSaveDocReq_TASK_CANCEL,
			Doc:  *realtimeDoc,
		}
	default:
		err = fmt.Errorf("illegal reqType:%v", req.GetReqType())
		log.WarnContextf(ctx, "getParseDocReqFromServerReq|failed, err:%+v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "getParseDocReqFromServerReq|parseReqChan:%+v", parseReqChan)
	return parseReqChan, nil
}

// GetDocFullText 获取文档全文
func (s *Service) GetDocFullText(ctx context.Context, req *knowledge.GetDocFullTextReq) (
	rsp *knowledge.GetDocFullTextRsp, err error) {
	log.InfoContextf(ctx, "GetDocFullText|req:%+v", req)
	if req.GetDocId() == 0 {
		return nil, errs.ErrDocIDFail
	}
	rsp = &knowledge.GetDocFullTextRsp{}
	// 目前只支持实时文档
	switch req.StorageType {
	case knowledge.StorageType_STORAGE_REALTIME:
		doc, err := s.dao.GetRealtimeDocByID(ctx, req.DocId)
		if err != nil {
			log.WarnContextf(ctx, "GetDocFullText|GetRealtimeDocByID failed, err:%+v", err)
			return nil, err
		}
		log.InfoContextf(ctx, "GetDocFullText|doc:%+v", doc)
		// 2.4.0 放开sessionID一致性校验：针对文档上传中清空对话，上传完再发送的场景，sessionID是对不上的
		// if doc.SessionID != req.SessionId {
		//	err = fmt.Errorf("doc.SessionID:%s is not equals req.SessionID:%s", doc.SessionID, req.SessionId)
		//	log.WarnContextf(ctx, "GetDocFullText|sessionID illegal, err:%+v", err)
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
				cutLength := utils.When(docLength > int(req.MaxSize), int(req.MaxSize), docLength)
				fullText := string([]rune(doc.FileFullText)[:cutLength])
				rsp.DocFullText, rsp.TextPlaceholders =
					s.extractFullTextPlaceholder(ctx, fullText, req.GetUsePlaceholder())
			}
		} else {
			err = fmt.Errorf("doc.CharSize:%d illegal", doc.CharSize)
			log.WarnContextf(ctx, "GetDocFullText|doc.CharSize illegal, err:%+v", err)
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
	string, []*knowledge.Placeholder) {
	log.InfoContextf(ctx, "extractFullTextPlaceholder|fullText:%s, usePlaceholder:%v", fullText, usePlaceholder)
	placeholders := make([]*knowledge.Placeholder, 0)
	if !usePlaceholder {
		return fullText, placeholders
	}
	c, p := markdown.New(
		markdown.WithLinkPlaceholder(config.App().DocPlaceholder.Link),
		markdown.WithImgPlaceholder(config.App().DocPlaceholder.Img),
	).ExtractLinkWithPlaceholder([]byte(fullText))
	for _, v := range p {
		placeholders = append(placeholders, &knowledge.Placeholder{
			Key:   v.Key,
			Value: v.Value,
		})
	}
	log.InfoContextf(ctx, "extractFullTextPlaceholder|fullText:%s, placeholders:%+v", string(c), placeholders)
	return string(c), placeholders
}

// SearchRealtime 实时文档检索
func (s *Service) SearchRealtime(ctx context.Context, req *knowledge.SearchRealtimeReq) (
	rsp *knowledge.SearchRealtimeRsp, err error) {
	log.InfoContextf(ctx, "SearchRealtime|req:%+v", req)
	filterKey, err := s.getSearchRealtimeFilterKey(ctx, req.GetFilterKey())
	if err != nil {
		return nil, err
	}
	switch filterKey {
	case model.AppSearchPreviewFilterKey:
		app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppTestScenes)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		if app.GetKnowledgeQa() == nil {
			return nil, errs.ErrAppTypeSupportFilters
		}
		ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
		return s.searchRealtimePreview(ctx, app, filterKey, req)
	case model.AppSearchReleaseFilterKey:
		app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppReleaseScenes)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		if app.GetKnowledgeQa() == nil {
			return nil, errs.ErrAppTypeSupportFilters
		}
		ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
		return s.searchRealtimeRelease(ctx, app, filterKey, req)
	default:
		return nil, err
	}
}

// getSearchRealtimeFilterKey 获取实时文档检索的Filters
// 目前完全复用离线的配置，后续有需要再拆开
func (s *Service) getSearchRealtimeFilterKey(ctx context.Context, filterKey string) (string, error) {
	log.InfoContextf(ctx, "getSearchRealtimeFilterKey|filterKey:%s", filterKey)
	if filterKey == "" || filterKey == model.AppSearchRealtimePreviewFilterKey {
		// 复用离线评测库
		return model.AppSearchPreviewFilterKey, nil
	}
	if filterKey == model.AppSearchRealtimeReleaseFilterKey {
		// 复用离线正式库
		return model.AppSearchReleaseFilterKey, nil
	}
	// 非法的filterKey
	err := fmt.Errorf("illegal filterKey:%s", filterKey)
	log.WarnContextf(ctx, "getSearchRealtimeFilterKey|err:%+v", err)
	return "", errs.ErrAppTypeSupportFilters
}

// realtimeSearchIndexMap 实时文档检索的Index映射 @harryhlli
var realtimeSearchIndexMap = map[uint32]uint64{
	model.DocTypeSegment:         model.RealtimeSegmentVersionID,      // type:2 -> index:9
	model.DocTypeImage:           model.RealtimeSegmentImageVersionID, // type:6 -> index:11
	model.DocTypeTextSearchImage: model.RealtimeSegmentImageVersionID, // type:7 -> index:11
}

// searchRealtimePreview 实时文档检索--评测库
func (s *Service) searchRealtimePreview(ctx context.Context, app *admin.GetAppInfoRsp, filterKey string,
	req *knowledge.SearchRealtimeReq) (rsp *knowledge.SearchRealtimeRsp, err error) {
	log.InfoContextf(ctx, "searchRealtimePreview|app:%+v, filterKey:%s", app, filterKey)
	// filters
	filter, ok := app.GetKnowledgeQa().GetFilters()[filterKey]
	if !ok {
		log.WarnContextf(ctx, "searchRealtimePreview|app.GetFilter failed,filter not found")
		return nil, fmt.Errorf("filter not found")
	}
	filters := make([]*retrieval.RetrievalRealTimeReq_Filter, 0, len(filter.GetFilter()))
	for _, f := range filter.GetFilter() {
		if f.GetDocType() == model.DocTypeSearchEngine {
			continue
		}
		// 实时文档只检索文档段、文档段图片、文搜图 @harryhlli
		if _, ok := realtimeSearchIndexMap[f.GetDocType()]; !ok {
			continue
		}
		if filterKey == model.AppSearchPreviewFilterKey && !f.GetIsEnable() {
			continue
		}
		filters = append(filters, &retrieval.RetrievalRealTimeReq_Filter{
			IndexId:    realtimeSearchIndexMap[f.GetDocType()], // 实时文档的IndexId需要根据映射关系转换
			Confidence: f.GetConfidence(),
			TopN:       f.GetTopN(),
			DocType:    f.GetDocType(),
		})
	}
	// rerank
	rerank, err := getRerankModel(app)
	if err != nil {
		log.WarnContextf(ctx, "searchRealtimePreview|GetRerankModel failed, err: %v", err)
		return nil, err
	}
	// 调用Vector检索
	searchRealtimeRsp, err := s.dao.SearchRealtimeKnowledge(ctx, &retrieval.RetrievalRealTimeReq{
		RobotId:          app.GetId(),
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             filter.GetTopN(),
		EmbeddingVersion: app.GetKnowledgeQa().GetEmbedding().GetVersion(),
		Rerank: &retrieval.RetrievalRealTimeReq_Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		// 传FilterKey区分知识检索和已采纳问题直接回复
		FilterKey: filterKey,
		Labels:    convertSearchRealtimeLabel(req.GetLabels()),
		ImageUrls: req.GetImageUrls(),
		BotBizId:  app.GetAppBizId(),
		LabelExpression: fillLabelWithoutGeneralVectorExpression(
			convertPbLabel(req.GetLabels()), model.AppSearchConditionAnd),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy()),
		ModelName:      req.GetModelName(),
	})
	if err != nil {
		log.WarnContextf(ctx, "searchRealtimePreview|dao.SearchRealtimeKnowledge failed, err: %v", err)
		return nil, err
	}
	docs, err := s.getRealtimeRetrievalRspDoc(ctx, searchRealtimeRsp)
	if err != nil {
		log.WarnContextf(ctx, "searchRealtimePreview|getRealtimeRetrievalRspDoc failed, err: %v", err)
		return nil, err
	}
	// 检索后处理
	rsp = &knowledge.SearchRealtimeRsp{Docs: docs}
	return searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp), nil
}

// searchRealtimeRelease 实时文档检索--正式库
func (s *Service) searchRealtimeRelease(ctx context.Context, app *admin.GetAppInfoRsp, filterKey string,
	req *knowledge.SearchRealtimeReq) (rsp *knowledge.SearchRealtimeRsp, err error) {
	log.InfoContextf(ctx, "searchRealtimeRelease|app:%+v, filterKey:%s", app, filterKey)
	// filters
	filter, ok := app.GetKnowledgeQa().GetFilters()[filterKey]
	if !ok {
		log.WarnContextf(ctx, "searchRealtimeRelease|app.GetFilter failed, not found")
		return nil, fmt.Errorf("filter not found")
	}
	filters := make([]*retrieval.RetrievalRealTimeReq_Filter, 0, len(filter.GetFilter()))
	for _, f := range filter.GetFilter() {
		if f.GetDocType() == model.DocTypeSearchEngine {
			continue
		}
		// 实时文档只检索文档段、文档段图片、文搜图 @harryhlli
		if _, ok := realtimeSearchIndexMap[f.GetDocType()]; !ok {
			continue
		}
		if filterKey == model.AppSearchPreviewFilterKey && !f.GetIsEnable() {
			continue
		}
		filters = append(filters, &retrieval.RetrievalRealTimeReq_Filter{
			IndexId:    realtimeSearchIndexMap[f.DocType], // 实时文档的IndexId需要根据映射关系转换
			Confidence: f.GetConfidence(),
			TopN:       f.GetTopN(),
			DocType:    f.GetDocType(),
		})
	}
	// rerank
	rerank, err := getRerankModel(app)
	if err != nil {
		log.WarnContextf(ctx, "searchRealtimeRelease|GetRerankModel failed, err: %v", err)
		return nil, err
	}
	// 调用Vector检索
	searchRealtimeRsp, err := s.dao.SearchRealtimeKnowledge(ctx, &retrieval.RetrievalRealTimeReq{
		RobotId:          app.GetId(),
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             filter.GetTopN(),
		EmbeddingVersion: app.GetKnowledgeQa().GetEmbedding().GetVersion(),
		Rerank: &retrieval.RetrievalRealTimeReq_Rerank{
			Model:  rerank.ModelName,
			TopN:   rerank.TopN,
			Enable: rerank.Enable,
		},
		// 传FilterKey区分知识检索和已采纳问题直接回复
		FilterKey: filterKey,
		Labels:    convertSearchRealtimeLabel(req.GetLabels()),
		ImageUrls: req.GetImageUrls(),
		BotBizId:  app.GetAppBizId(),
		LabelExpression: fillLabelWithoutGeneralVectorExpression(
			convertPbLabel(req.GetLabels()), model.AppSearchConditionAnd),
		SubQuestions:   req.GetSubQuestions(),
		SearchStrategy: getSearchStrategy(app.GetKnowledgeQa().GetSearchStrategy()),
		ModelName:      req.GetModelName(),
	})
	if err != nil {
		log.WarnContextf(ctx, "searchRealtimeRelease|dao.SearchRealtimeKnowledge failed, err: %v", err)
		return nil, err
	}
	docs, err := s.getRealtimeRetrievalRspDoc(ctx, searchRealtimeRsp)
	if err != nil {
		log.WarnContextf(ctx, "searchRealtimeRelease|getRealtimeRetrievalRspDoc failed, err: %v", err)
		return nil, err
	}
	// 检索后处理
	rsp = &knowledge.SearchRealtimeRsp{Docs: docs}
	return searchRspPostProcess(ctx, req.GetUsePlaceholder(), rsp), nil
}

// getRealtimeRetrievalRspDoc 获取实时文档评测库检索结果
func (s *Service) getRealtimeRetrievalRspDoc(ctx context.Context, rsp *retrieval.RetrievalRealTimeRsp) (
	[]*knowledge.SearchRealtimeRsp_Doc, error) {
	linkContents, err := s.dao.GetLinkContentsFromRealtimeSearchVectorResponse(
		ctx, rsp.GetDocs(),
		func(doc *retrieval.RetrievalRealTimeRsp_Doc, segment *realtime.TRealtimeDocSegment) any {
			return &knowledge.SearchRealtimeRsp_Doc{
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
			return &knowledge.SearchRealtimeRsp_Doc{
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
	docs := dao.Link(ctx, linkContents,
		func(t *knowledge.SearchRealtimeRsp_Doc, v linker.Content) *knowledge.SearchRealtimeRsp_Doc {
			t.OrgData = v.Value
			return t
		})
	return docs, nil
}

// convertRealtimeRetrievalExtra 转换实时文档检索额外信息
func convertRealtimeRetrievalExtra(extra *retrieval.RetrievalExtra) *knowledge.RetrievalExtra {
	if extra == nil {
		return nil
	}
	return &knowledge.RetrievalExtra{
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
func convertRealtimeRetrievalResultType(resultType retrieval.RetrievalResultType) knowledge.RetrievalResultType {
	return knowledge.RetrievalResultType(resultType.Number())
}

// convertSearchRealtimeLabel 转换实时检索标签的结构体
func convertSearchRealtimeLabel(labels []*knowledge.VectorLabel) []*retrieval.SearchVectorLabel {
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
func convertPbLabel(labels []*knowledge.VectorLabel) []*pb.VectorLabel {
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
func (s *Service) DeleteRealtimeDoc(ctx context.Context, req *knowledge.DeleteRealtimeDocReq) (
	*knowledge.DeleteRealtimeDocRsp, error) {
	log.InfoContextf(ctx, "DeleteRealtimeDoc req:%+v", req)
	if len(req.GetSessionId()) == 0 && len(req.GetDocIds()) == 0 {
		return nil, errs.ErrParams
	}

	if req.GetBotBizId() == 0 {
		return nil, errs.ErrParams
	}

	newCtx := trpc.CloneContext(ctx)

	go func() {
		defer errors.PanicHandler()
		s.dao.DeletedRealtimeDocInfo(newCtx, req.GetBotBizId(), req.GetSessionId(), req.GetDocIds())
	}()
	return new(knowledge.DeleteRealtimeDocRsp), nil
}

func buildRealtimeDoc(req *knowledge.StreamSaveDocReq, sessionID string, docID, corpID, staffID,
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
	staff, err := s.dao.GetStaffByID(ctx, staffID)
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
