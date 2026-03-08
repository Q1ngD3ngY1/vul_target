package api

import (
	"context"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// GetDocSummary 提供给Chat的接口。返回文档摘要
func (s *Service) GetDocSummary(req *pb.GetDocSummaryReq, server pb.Api_GetDocSummaryServer) error {
	ctx := server.Context()
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetBotBizId())
	if err != nil {
		logx.E(ctx, "GetDocSummary|get app error:%+v", err)
		return err
	}

	if app == nil {
		return errs.ErrRobotNotFound
	}

	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	chReply, errChan, err := s.GetDocSummaryImpl(newCtx, req)
	if err != nil {
		return err
	}
	finalRsp := &pb.GetDocSummaryRsp{
		DocSummary:       "",
		ReasoningContent: "",
		IsFinal:          false,
		StatisticInfos:   make([]*pb.StatisticInfo, 0),
	}
	for {
		select {
		case <-ctx.Done():
			finalRsp.IsFinal = true
			err = server.Send(finalRsp)
			if err != nil {
				logx.E(ctx, "server.Send failed, error: %v", err)
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
				logx.I(ctx, "send last success: %s", finalRsp)
				return nil
			}

			err := server.Send(finalRsp)
			if err != nil {
				logx.W(ctx, "server.Send failed, error: %v", err)
				return err
			}
			logx.I(ctx, "server.Send success: %s", finalRsp)
		case <-time.After(10 * time.Second):
			if len(finalRsp.DocSummary) == 0 {
				continue
			}
			err = server.Send(finalRsp) // 这里其实不是流式返回，结果一次性返回了。
			if err != nil {
				logx.E(ctx, "server.Send failed, error: %v", err)
				return err
			}
			logx.D(ctx, "server.Send success: %s", finalRsp)
		case err := <-errChan:
			return err
		}
	}
}

// GetDocSummaryImpl 获取文档摘要
// 1. Query可能为空，补充上固定的Prompt：请帮我总结文件的核心内容
func (s *Service) GetDocSummaryImpl(ctx context.Context, request *pb.GetDocSummaryReq) (
	chReply chan *pb.GetDocSummaryRsp, errChan chan error, err error) {
	logx.I(ctx, "GetDocSummaryImpl, req: %s", request)

	var finalSummaryRsp pb.GetDocSummaryRsp
	var finalString string
	// var finalReasoningContent string

	// 特殊处理下，如果是lke-deepseek-r1模型的话，只处理第一个文件
	lkeDeepseekR1Name := strings.TrimSpace(config.GetMainConfig().RealtimeConfig.LkeDeepSeekR1Name)
	if request.GetModelName() == lkeDeepseekR1Name && len(request.GetFileInfos()) > 1 {
		request.FileInfos = request.FileInfos[:1]
	}
	chReply = make(chan *pb.GetDocSummaryRsp, 10)
	errChan = make(chan error, 1)
	go func() {
		defer gox.Recover()
		defer close(chReply)
		for _, fileInfo := range request.FileInfos {
			logx.I(ctx, "processing file: %s", fileInfo.FileName)
			// summary, err := s.GetOneDocSummary(trpc.CloneContext(ctx), request, fileInfo.DocId, fileInfo.FileName)
			summary, err := s.docLogic.StreamGetOneDocSummary(trpc.CloneContext(ctx), request, finalString, fileInfo.DocId, fileInfo.FileName, chReply)
			if err != nil {
				logx.E(ctx, "GetOneDocSummary error: %v", err)
				errChan <- err
				return
			}
			// 单个文档结束
			logx.I(ctx, "oneDocSummaryDone, fileName:%s,summary:%+v", fileInfo.FileName, summary)
			finalString = summary.GetDocSummary() + "\n\n"
			// finalReasoningContent = summary.GetReasoningContent()
			finalSummaryRsp.DocSummary = finalString
			finalSummaryRsp.ReasoningContent = summary.GetReasoningContent()
			finalSummaryRsp.StatisticInfos = append(finalSummaryRsp.StatisticInfos, summary.StatisticInfos...)
		}
		logx.I(ctx, "GetDocSummaryImpl AllFile DocSummary Done!")
		finalSummaryRsp.IsFinal = true
		chReply <- &finalSummaryRsp
	}()

	return chReply, errChan, nil
}
