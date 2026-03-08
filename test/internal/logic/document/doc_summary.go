package document

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/util"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

// SummaryResult 摘要结果
type SummaryResult struct {
	Result        string
	Index         int
	StatisticInfo *rpc.StatisticInfo
}

func (l *Logic) worker(ctx context.Context, request *pb.GetDocSummaryReq,
	tasks <-chan SummaryResult, results chan<- SummaryResult, wg *sync.WaitGroup) {
	for task := range tasks {
		req := NewLLMRequest(ctx, request, "")
		err := l.GetFinalSummary(trpc.CloneContext(ctx), req, request.Query, task, results)
		wg.Done()

		if err != nil {
			logx.E(ctx, "worker err: %+v", err)
			continue
		}
		logx.I(ctx, "worker done: %s", jsonx.MustMarshalToString(task))
	}
}

// StreamGetOneDocSummary 流式获取摘要
func (l *Logic) StreamGetOneDocSummary(ctx context.Context, request *pb.GetDocSummaryReq, finalDocSummary string,
	docID uint64, fileName string, summaryCh chan *pb.GetDocSummaryRsp) (summary *pb.GetDocSummaryRsp, err error) {

	summary = &pb.GetDocSummaryRsp{
		DocSummary:     "",
		IsFinal:        false,
		StatisticInfos: make([]*pb.StatisticInfo, 0),
	}
	summaryRate, fullTextToSummary, err := l.GetFileFullTextToSummary(ctx, request, docID)
	if err != nil {
		return summary, err
	}
	logx.D(ctx, "fullTextToSummary: %s, length: %d", jsonx.MustMarshalToString(fullTextToSummary), len(fullTextToSummary))

	rspChan := make(chan *rpc.LlmStreamResponse, 10)
	g, _ := errgroupx.WithContext(ctx)

	summaryCtx := &entity.SummaryContext{
		Query:      request.GetQuery(),
		DocContent: fullTextToSummary[0],
	}
	prompt := ""
	if request.GetQuery() == "" {
		prompt, err = util.Render(ctx, entity.TplDefaultSummary, summaryCtx)
	} else {
		prompt, err = util.Render(ctx, entity.TplUserSummary, summaryCtx)
	}

	req := NewLLMRequest(ctx, request, prompt)

	logx.I(ctx, "docSummary Chat LLMReq:%s", jsonx.MustMarshalToString(req))
	g.Go(func() error {
		err = l.rpc.AIGateway.Chat(ctx, req, rspChan)
		if err != nil {
			logx.E(ctx, "StreamGetFileSummary chat stream err: %+v", err)

			return err
		}
		return nil
	})

	for rsp := range rspChan {
		summary.ReasoningContent = rsp.GetReasoningContent()

		// 先输出思考，后输出摘要，下面代码保证先有摘要内容后输出摘要内容
		if len(rsp.GetReplyContent()) > 0 {
			summary.DocSummary = finalDocSummary + getDocSummaryFileNameExt(summaryRate, fileName) + "中核心观点包含：\n" + rsp.GetReplyContent()
		}
		if rsp.IsFinished() {
			rsp.GetStatisticInfo()
			summary.StatisticInfos = append(summary.StatisticInfos, convertStatisticInfo(rsp.GetStatisticInfo()))
		}
		summaryCh <- summary
	}
	return summary, nil

}

// RequestID 获取大模型请求 ID
func RequestID(ctx context.Context, sessionID, requestID string) string {
	return contextx.TraceID(ctx) + ":" + sessionID + ":" + requestID
}

// NewLLMRequest 构造摘要请求LLMRequest
func NewLLMRequest(ctx context.Context, request *pb.GetDocSummaryReq, prompt string) *rpc.LlmRequest {
	messages := make([]*rpc.Message, 0)
	messages = append(messages, &rpc.Message{Role: rpc.Role_USER, Content: prompt})
	return &rpc.LlmRequest{
		RequestId: RequestID(ctx, request.GetSessionId(), uuid.NewString()), // 每次随机生成一个RequestID
		ModelName: request.GetModelName(),
		Messages:  messages,
		BizAppId:  request.GetBotBizId(),
		StartTime: time.Time{},
	}
}

func convertStatisticInfo(st *rpc.StatisticInfo) *pb.StatisticInfo {
	return &pb.StatisticInfo{
		FirstTokenCost: st.FirstTokenCost,
		TotalCost:      st.TotalCost,
		InputTokens:    st.InputTokens,
		OutputTokens:   st.OutputTokens,
		TotalTokens:    st.TotalTokens,
	}
}

// GetOneDocSummary 获取一个文档的摘要
func (l *Logic) GetOneDocSummary(ctx context.Context, request *pb.GetDocSummaryReq,
	docID uint64, fileName string) (summary *pb.GetDocSummaryRsp, err error) {
	summary = &pb.GetDocSummaryRsp{
		DocSummary:     "",
		IsFinal:        false,
		StatisticInfos: make([]*pb.StatisticInfo, 0),
	}
	summaryRate, fullTextToSummary, err := l.GetFileFullTextToSummary(ctx, request, docID)
	if err != nil {
		return summary, err
	}
	logx.D(ctx, "fullTextToSummary: %s, length: %d",
		jsonx.MustMarshalToString(fullTextToSummary), len(fullTextToSummary))

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
			defer gox.Recover()
			l.worker(ctx, request, jobs, results, wg)
		}()
	}
	for _, task := range tasks {
		jobs <- task
		logx.D(ctx, "jobs: %s", jsonx.MustMarshalToString(task))
		wg.Add(1)
	}
	close(jobs)
	wg.Wait()
	finalResults := make([]SummaryResult, len(tasks))
	for range tasks {
		result := <-results
		finalResults[result.Index] = result
	}
	logx.I(ctx, "finalResults of filename[%s]: %s", fileName, jsonx.MustMarshalToString(finalResults))
	// 一个结果直接返回
	if len(finalResults) == 1 {
		if finalResults[0].Result == "error" || finalResults[0].StatisticInfo == nil {
			return summary, errors.New("GetFinalSummary error")
		}
		logx.D(ctx, "finalResults: %s", finalResults[0])
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
	logx.D(ctx, "summary: %s", middleSummary)
	if len([]rune(middleSummary)) > int(request.GetPromptLimit()) { // 后续优化
		middleSummary = middleSummary[:request.GetPromptLimit()-200] // 安全边际
	}
	req := NewLLMRequest(ctx, request, middleSummary)
	req.Messages[0].Content = middleSummary
	rsp, err := l.rpc.AIGateway.SimpleChat(ctx, req)
	if err != nil {
		logx.E(ctx, "SimpleChat error: %v", err)
		return summary, errors.New("GetFinalSummary error")
	}
	logx.I(ctx, "SimpleChat rsp: %s", jsonx.MustMarshalToString(rsp))
	summary.DocSummary = getDocSummaryFileNameExt(summaryRate, fileName) + "中核心观点包含：\n" + rsp.GetReplyContent()
	summary.StatisticInfos = append(summary.StatisticInfos, convertStatisticInfo(rsp.GetStatisticInfo()))
	return summary, nil
}

// GetContentToSummary 获取内容摘要
func (l *Logic) GetContentToSummary(ctx context.Context,
	request *pb.GetDocSummaryReq, docID uint64) (contentToSummary []string, err error) {
	contentToSummary = make([]string, 0) // 当前要做摘要的内容,要做几次摘要
	// 获取文档片段总数
	count, err := l.docDao.GetOrgDataCountByDocID(ctx, docID)
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
		contents, err := l.docDao.GetOrgDataListByDocID(ctx, docID, uint64(offset), uint64(limit))
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
		if offset >= int(count) {
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
func (l *Logic) GetFinalSummary(ctx context.Context, req *rpc.LlmRequest, query string,
	summaryResult SummaryResult, results chan<- SummaryResult) (err error) {

	summaryCtx := &entity.SummaryContext{
		Query:      query,
		DocContent: summaryResult.Result,
	}
	prompt := ""
	if query == "" {
		prompt, err = util.Render(ctx, entity.TplDefaultSummary, summaryCtx)
	} else {
		prompt, err = util.Render(ctx, entity.TplUserSummary, summaryCtx)
	}

	req.Messages[0].Content = prompt
	logx.I(ctx, "SimpleChat req: %s", jsonx.MustMarshalToString(req))
	rsp, err := l.rpc.AIGateway.SimpleChat(ctx, req)
	if err != nil {
		logx.E(ctx, "SimpleChat error: %v", err)
		summaryResult.Result = "error"
		results <- summaryResult
		return err
	}
	logx.I(ctx, "SimpleChat rsp: %s", jsonx.MustMarshalToString(rsp))
	summaryResult.Result = rsp.GetReplyContent()
	summaryResult.StatisticInfo = rsp.GetStatisticInfo()
	results <- summaryResult
	return nil
}

// GetFileFullTextToSummary 获取全文内容摘要
func (l *Logic) GetFileFullTextToSummary(ctx context.Context, request *pb.GetDocSummaryReq,
	docID uint64) (float64, []string, error) {
	realTimeDoc, err := l.GetRealtimeDocByID(ctx, docID)
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
