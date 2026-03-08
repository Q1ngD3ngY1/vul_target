// bot-knowledge-config-server
//
// @(#)doc_summary.go  星期三, 八月 28, 2024
// Copyright(c) 2024, reinhold@Tencent. All rights reserved.

package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/common/v3/errors"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	commutils "git.woa.com/dialogue-platform/go-comm/utils"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"
)

// SummaryResult 摘要结果
type SummaryResult struct {
	Result        string
	Index         int
	StatisticInfo *client.StatisticInfo
}

func (d *dao) worker(ctx context.Context, request *knowledge.GetDocSummaryReq,
	tasks <-chan SummaryResult, results chan<- SummaryResult, wg *sync.WaitGroup) {
	for task := range tasks {
		req := NewLLMRequest(ctx, request, "")
		err := d.GetFinalSummary(trpc.CloneContext(ctx), req, request.Query, task, results)
		wg.Done()

		if err != nil {
			log.ErrorContextf(ctx, "worker err: %+v", err)
			continue
		}
		log.InfoContextf(ctx, "worker done: %s", commutils.ToJsonString(task))
	}
}

// StreamGetOneDocSummary 流式获取摘要
func (d *dao) StreamGetOneDocSummary(ctx context.Context, request *knowledge.GetDocSummaryReq, finalDocSummary string,
	docID uint64, fileName string, summaryCh chan *knowledge.GetDocSummaryRsp) (summary *knowledge.GetDocSummaryRsp, err error) {

	summary = &knowledge.GetDocSummaryRsp{
		DocSummary:     "",
		IsFinal:        false,
		StatisticInfos: make([]*knowledge.StatisticInfo, 0),
	}
	summaryRate, fullTextToSummary, err := d.GetFileFullTextToSummary(ctx, request, docID)
	if err != nil {
		return summary, err
	}
	log.DebugContextf(ctx, "fullTextToSummary: %s, length: %d",
		commutils.ToJsonString(fullTextToSummary), len(fullTextToSummary))

	rspChan := make(chan *client.LlmStreamResponse, 10)
	g, _ := errgroupx.WithContext(ctx)

	summaryCtx := &SummaryContext{
		Query:      request.GetQuery(),
		DocContent: fullTextToSummary[0],
	}
	prompt := ""
	if request.GetQuery() == "" {
		prompt, err = util.Render(ctx, TplDefaultSummary, summaryCtx)
	} else {
		prompt, err = util.Render(ctx, TplUserSummary, summaryCtx)
	}

	req := NewLLMRequest(ctx, request, prompt)

	log.InfoContextf(ctx, "docSummary Chat LLMReq:%s", commutils.ToJsonString(req))
	g.Go(func() error {
		err = client.Chat(ctx, req, rspChan)
		if err != nil {
			log.ErrorContextf(ctx, "StreamGetFileSummary chat stream err: %+v", err)

			return err
		}
		return nil
	})

	for rsp := range rspChan {
		summary.ReasoningContent = rsp.GetReasoningContent()

		// 先输出思考，后输出摘要，下面代码保证先有摘要内容后输出摘要内容
		if len(rsp.GetReplyContent()) > 0 {
			summary.DocSummary = finalDocSummary + getDocSummaryFileNameExt(summaryRate, fileName) + "中核心观点包含：\n" +
				rsp.GetReplyContent()
		}
		if rsp.IsFinished() {
			rsp.GetStatisticInfo()
			summary.StatisticInfos = append(summary.StatisticInfos, convertStatisticInfo(rsp.GetStatisticInfo()))
		}
		summaryCh <- summary
	}
	return summary, nil

}

// TraceID TODO
func TraceID(ctx context.Context) string {
	return trace.SpanContextFromContext(ctx).TraceID().String()
}

// RequestID 获取大模型请求 ID
func RequestID(ctx context.Context, sessionID, requestID string) string {
	return TraceID(ctx) + ":" + sessionID + ":" + requestID
}

// NewLLMRequest 构造摘要请求LLMRequest
func NewLLMRequest(ctx context.Context, request *knowledge.GetDocSummaryReq, prompt string) *client.LlmRequest {
	messages := make([]*client.Message, 0)
	messages = append(messages, &client.Message{Role: client.Role_USER, Content: prompt})
	return &client.LlmRequest{
		RequestId: RequestID(ctx, request.GetSessionId(), uuid.NewString()), // 每次随机生成一个RequestID
		ModelName: request.GetModelName(),
		Messages:  messages,
		BizAppId:  request.GetBotBizId(),
		StartTime: time.Time{},
	}
}

func convertStatisticInfo(st *client.StatisticInfo) *knowledge.StatisticInfo {
	return &knowledge.StatisticInfo{
		FirstTokenCost: st.FirstTokenCost,
		TotalCost:      st.TotalCost,
		InputTokens:    st.InputTokens,
		OutputTokens:   st.OutputTokens,
		TotalTokens:    st.TotalTokens,
	}
}

// GetOneDocSummary 获取一个文档的摘要
func (d *dao) GetOneDocSummary(ctx context.Context, request *knowledge.GetDocSummaryReq,
	docID uint64, fileName string) (summary *knowledge.GetDocSummaryRsp, err error) {
	summary = &knowledge.GetDocSummaryRsp{
		DocSummary:     "",
		IsFinal:        false,
		StatisticInfos: make([]*knowledge.StatisticInfo, 0),
	}
	summaryRate, fullTextToSummary, err := d.GetFileFullTextToSummary(ctx, request, docID)
	if err != nil {
		return summary, err
	}
	log.DebugContextf(ctx, "fullTextToSummary: %s, length: %d",
		commutils.ToJsonString(fullTextToSummary), len(fullTextToSummary))

	// 一个文件最高5个并发，5个文件最高25并发
	tasks := make([]SummaryResult, 0)
	for idx, content := range fullTextToSummary {
		tasks = append(tasks, SummaryResult{Result: content, Index: idx})
	}
	results := make(chan SummaryResult, len(tasks))
	wg := &sync.WaitGroup{}
	jobs := make(chan SummaryResult, len(tasks))
	for w := 0; w < 1; w++ {
		go func() {
			defer errors.PanicHandler()
			d.worker(ctx, request, jobs, results, wg)
		}()
	}
	for _, task := range tasks {
		jobs <- task
		log.DebugContextf(ctx, "jobs: %s", commutils.ToJsonString(task))
		wg.Add(1)
	}
	close(jobs)
	wg.Wait()
	finalResults := make([]SummaryResult, len(tasks))
	for range tasks {
		result := <-results
		finalResults[result.Index] = result
	}
	log.InfoContextf(ctx, "finalResults of filename[%s]: %s", fileName, commutils.ToJsonString(finalResults))
	// 一个结果直接返回
	if len(finalResults) == 1 {
		if finalResults[0].Result == "error" || finalResults[0].StatisticInfo == nil {
			return summary, errors.New("GetFinalSummary error")
		}
		log.DebugContextf(ctx, "finalResults: %s", finalResults[0])
		summary.DocSummary = getDocSummaryFileNameExt(summaryRate, fileName) + "中核心观点包含：\n" +
			finalResults[0].Result
		summary.StatisticInfos = append(summary.StatisticInfos, convertStatisticInfo(finalResults[0].StatisticInfo))
		return summary, nil
	}
	// 拼接finalResults, 再做一次摘要
	middleSummary := ""
	for _, result := range finalResults {
		if result.Result == "error" || strings.HasPrefix(result.Result, "抱歉，您提供的信息") {
			continue
		}
		middleSummary += result.Result + "\n"
	}
	log.DebugContextf(ctx, "summary: %s", middleSummary)
	if len([]rune(middleSummary)) > int(request.GetPromptLimit()) { // 后续优化
		middleSummary = middleSummary[:request.GetPromptLimit()-200] // 安全边际
	}
	req := NewLLMRequest(ctx, request, middleSummary)
	rsp, err := client.SimpleChat(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "SimpleChat error: %v", err)
		return summary, errors.New("GetFinalSummary error")
	}
	log.InfoContextf(ctx, "SimpleChat rsp: %s", commutils.ToJsonString(rsp))
	summary.DocSummary = getDocSummaryFileNameExt(summaryRate, fileName) + "中核心观点包含：\n" +
		rsp.GetReplyContent()
	summary.StatisticInfos = append(summary.StatisticInfos, convertStatisticInfo(rsp.GetStatisticInfo()))
	return summary, nil
}

// GetContentToSummary 获取内容摘要
func (d *dao) GetContentToSummary(ctx context.Context,
	request *knowledge.GetDocSummaryReq, docID uint64) (contentToSummary []string, err error) {
	contentToSummary = make([]string, 0) // 当前要做摘要的内容,要做几次摘要
	// 获取文档片段总数
	count, err := d.GetCountByDocID(ctx, docID)
	if err != nil {
		return contentToSummary, err
	}
	if count == 0 {
		return contentToSummary, errors.New("docID not found")
	}
	// 每次10条，获取OrgData
	orgDataList := make([]string, 0) // 去重后的文档片段
	limit, offset := 20, 0
	for {
		// 获取文档片段，每次20条 orgdata每条4k左右
		contents, err := d.GetOrgDataListByDocID(ctx, docID, uint64(offset), uint64(limit))
		if err != nil {
			return contentToSummary, err
		}
		for _, content := range contents {
			if len(orgDataList) == 0 {
				orgDataList = append(orgDataList, content)
				continue
			}
			if content != orgDataList[len(orgDataList)-1] { // 去重
				orgDataList = append(orgDataList, content)
			}
		}
		offset += limit
		if offset >= count {
			break
		}
	}
	tmp := ""
	for _, orgData := range orgDataList { // 拼接orgData
		if tmp != "" && len([]rune(tmp+orgData)) > int(request.GetPromptLimit()) {
			contentToSummary = append(contentToSummary, tmp)
			tmp = ""
			break
		}
		tmp += orgData
	}
	// 最后一个
	if tmp != "" {
		contentToSummary = append(contentToSummary, tmp)
	}
	return contentToSummary, nil
}

// GetFinalSummary 获取最终的摘要
func (d *dao) GetFinalSummary(ctx context.Context, req *client.LlmRequest, query string,
	summaryResult SummaryResult, results chan<- SummaryResult) (err error) {

	summaryCtx := &SummaryContext{
		Query:      query,
		DocContent: summaryResult.Result,
	}
	prompt := ""
	if query == "" {
		prompt, err = util.Render(ctx, TplDefaultSummary, summaryCtx)
	} else {
		prompt, err = util.Render(ctx, TplUserSummary, summaryCtx)
	}

	req.Messages[0].Content = prompt
	log.InfoContextf(ctx, "SimpleChat req: %s", commutils.ToJsonString(req))
	rsp, err := client.SimpleChat(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "SimpleChat error: %v", err)
		summaryResult.Result = "error"
		results <- summaryResult
		return err
	}
	log.InfoContextf(ctx, "SimpleChat rsp: %s", commutils.ToJsonString(rsp))
	summaryResult.Result = rsp.GetReplyContent()
	summaryResult.StatisticInfo = rsp.GetStatisticInfo()
	results <- summaryResult
	return nil
}

// GetFileFullTextToSummary 获取全文内容摘要
func (d *dao) GetFileFullTextToSummary(ctx context.Context, request *knowledge.GetDocSummaryReq,
	docID uint64) (float64, []string, error) {
	realTimeDoc, err := d.GetRealtimeDocByID(ctx, docID)
	if err != nil {
		return 0, nil, err
	}
	var summaryText string
	fullTextLen := uint32(utf8.RuneCountInString(realTimeDoc.FileFullText))
	charSize := uint32(realTimeDoc.CharSize)
	if fullTextLen <= request.GetPromptLimit() {
		summaryText = realTimeDoc.FileFullText
	} else {
		summaryText = string([]rune(realTimeDoc.FileFullText)[:request.GetPromptLimit()])
	}
	summaryRate := float64(100)
	summartTextLen := uint32(utf8.RuneCountInString(summaryText))
	if request.GetPromptLimit() < charSize {
		fCharSize := decimal.NewFromInt(int64(charSize))
		fSummartTextLen := decimal.NewFromInt(int64(summartTextLen))
		summaryRate, _ = fSummartTextLen.Div(fCharSize).Mul(decimal.NewFromInt(100)).Float64()
	}
	return summaryRate, []string{summaryText}, nil
}

func getDocSummaryFileNameExt(summaryRate float64, fileName string) string {
	if summaryRate == 100 {
		return fileName
	}
	return fmt.Sprintf("%s【因阅读长度限制，仅阅读%.1f%%的字数】", fileName, summaryRate)
}
