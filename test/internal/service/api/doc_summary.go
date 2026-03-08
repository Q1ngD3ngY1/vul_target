package api

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/errors"
	commutils "git.woa.com/dialogue-platform/go-comm/utils"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// GetDocSummary 提供给Chat的接口。返回文档摘要
func (s *Service) GetDocSummary(req *knowledge.GetDocSummaryReq, server pb.Api_GetDocSummaryServer) error {
	ctx := server.Context()
	app, err := s.dao.GetAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		log.ErrorContextf(ctx, "GetDocSummary|get app error:%+v", err)
		return err
	}

	if app == nil {
		return errs.ErrRobotNotFound
	}

	ctx = pkg.WithSpaceID(ctx, app.SpaceID)

	chReply, errChan, err := s.GetDocSummaryImpl(trpc.CloneContext(ctx), req)
	if err != nil {
		return err
	}
	finalRsp := &knowledge.GetDocSummaryRsp{
		DocSummary:       "",
		ReasoningContent: "",
		IsFinal:          false,
		StatisticInfos:   make([]*knowledge.StatisticInfo, 0),
	}
	for {
		select {
		case <-ctx.Done():
			finalRsp.IsFinal = true
			err = server.Send(finalRsp)
			if err != nil {
				log.ErrorContextf(ctx, "server.Send failed, error: %v", err)
				return err
			}
			return nil
		case summaryRsp, ok := <-chReply:
			if !ok {
				return nil
			}

			finalRsp.ReasoningContent = summaryRsp.GetReasoningContent()
			finalRsp.DocSummary = summaryRsp.GetDocSummary()
			finalRsp.StatisticInfos = summaryRsp.GetStatisticInfos()

			if summaryRsp != nil && summaryRsp.IsFinal { // 特殊回包
				finalRsp.IsFinal = true
				server.Send(finalRsp)
				log.InfoContextf(ctx, "send last success: %s", commutils.ToJsonString(finalRsp))
				return nil
			}

			err := server.Send(finalRsp)
			if err != nil {
				log.WarnContextf(ctx, "server.Send failed, error: %v", err)
				return err
			}
			log.InfoContextf(ctx, "server.Send success: %s", commutils.ToJsonString(finalRsp))
		case <-time.After(10 * time.Second):
			if len(finalRsp.DocSummary) == 0 {
				continue
			}
			err = server.Send(finalRsp) // 这里其实不是流式返回，结果一次性返回了。
			if err != nil {
				log.ErrorContextf(ctx, "server.Send failed, error: %v", err)
				return err
			}
			log.DebugContextf(ctx, "server.Send success: %s", commutils.ToJsonString(finalRsp))
		case err := <-errChan:
			return err
		}
	}
}

// GetDocSummaryImpl 获取文档摘要
// 1. Query可能为空，补充上固定的Prompt：请帮我总结文件的核心内容
func (s *Service) GetDocSummaryImpl(ctx context.Context, request *knowledge.GetDocSummaryReq) (
	chReply chan *knowledge.GetDocSummaryRsp, errChan chan error, err error) {
	log.InfoContextf(ctx, "GetDocSummaryImpl, req: %s", commutils.ToJsonString(request))

	var finalSummaryRsp knowledge.GetDocSummaryRsp
	var finalString string
	//var finalReasoningContent string

	// 特殊处理下，如果是lke-deepseek-r1模型的话，只处理第一个文件
	lkeDeepseekR1Name := strings.TrimSpace(utilConfig.GetMainConfig().RealtimeConfig.LkeDeepSeekR1Name)
	if request.GetModelName() == lkeDeepseekR1Name && len(request.GetFileInfos()) > 1 {
		request.FileInfos = request.FileInfos[:1]
	}
	chReply = make(chan *knowledge.GetDocSummaryRsp, 10)
	errChan = make(chan error, 1)
	go func() {
		defer errors.PanicHandler()
		defer close(chReply)
		for _, fileInfo := range request.FileInfos {
			log.InfoContextf(ctx, "processing file: %s", fileInfo.FileName)
			//summary, err := s.GetOneDocSummary(trpc.CloneContext(ctx), request, fileInfo.DocId, fileInfo.FileName)
			summary, err := s.dao.StreamGetOneDocSummary(trpc.CloneContext(ctx), request, finalString, fileInfo.DocId, fileInfo.FileName, chReply)
			if err != nil {
				log.ErrorContextf(ctx, "GetOneDocSummary error: %v", err)
				errChan <- err
				return
			}
			// 单个文档结束
			log.InfoContextf(ctx, "oneDocSummaryDone, fileName:%s,summary:%+v", fileInfo.FileName, summary)
			finalString = summary.GetDocSummary() + "\n\n"
			//finalReasoningContent = summary.GetReasoningContent()
			finalSummaryRsp.DocSummary = finalString
			finalSummaryRsp.ReasoningContent = summary.GetReasoningContent()
			finalSummaryRsp.StatisticInfos = append(finalSummaryRsp.StatisticInfos, summary.StatisticInfos...)
		}
		log.InfoContextf(ctx, "GetDocSummaryImpl AllFile DocSummary Done!")
		finalSummaryRsp.IsFinal = true
		chReply <- &finalSummaryRsp
	}()

	return chReply, errChan, nil
}
