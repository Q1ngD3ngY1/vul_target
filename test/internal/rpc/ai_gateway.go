package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/llm/openai/chat"
	"git.woa.com/adp/common/llm/openai/common"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
)

type AIGatewayRPC interface {
	SimpleChat(ctx context.Context, req *LlmRequest) (res *LlmResponse, err error)
	Chat(ctx context.Context, req *LlmRequest, resChan chan *LlmStreamResponse) (err error)
}

// 消息的角色
type Role string

const (
	Role_USER      Role = chat.ChatMessageRoleUser      // 用户说的内容
	Role_ASSISTANT Role = chat.ChatMessageRoleAssistant // 大模型回复的额内容
	Role_SYSTEM    Role = chat.ChatMessageRoleSystem    // 一般作为系统Prompt用
	Role_TOOL      Role = chat.ChatMessageRoleTool      // 一般作为系统Prompt用
	Role_NONE      Role = "none"                        // 无角色（与LLM交互时将不自动补齐角色名）
)

// Message 一次对话消息
type Message struct {
	Role             Role     `json:"role,omitempty"`              // 角色
	Content          string   `json:"content,omitempty"`           // 内容
	Images           []string `json:"images,omitempty"`            // 图片 url 多模态大模型使用，可以同时传递多张图片url地址
	ReasoningContent string   `json:"reasoning_content,omitempty"` // 推理内容，思维链的输出
}

// InferParams 模型推理扩展参数
type InferParams struct {
	*bot_common.ModelParams
	TopK *int32 `json:"top_k,omitempty"`

	Citation                 bool     `json:"citation,omitempty"`                    // 引文
	EnableEnhancement        bool     `json:"enable_enhancement,omitempty"`          // 开启指令遵循搜索
	EnableInstructionSearch  bool     `json:"enable_instruction_search,omitempty"`   // 开启指令遵循搜索
	ExcludeSearchEngineTypes []string `json:"exclude_search_engine_types,omitempty"` // 排除搜索引擎类型
	FixedSearchEngineTypes   []string `json:"fixed_search_engine_types,omitempty"`   // 固定搜索引擎类型
}

// LlmRequest 请求封装
type LlmRequest struct {
	RequestId string    `json:"request_id,omitempty"` // 请求ID
	BizAppId  uint64    `json:"biz_app_id,omitempty"` // 租户id（标识一个客户），建议助手使用appkey，客服使用租户id
	StartTime time.Time `json:"start_time,omitempty"` // 请求时间

	ModelName   string       `json:"model_name,omitempty"` // 模型名称，见可选模型列表
	Messages    []*Message   `json:"messages,omitempty"`   // 消息列表（含历史对话及prompt）
	InferParams *InferParams `json:"infer_params,omitempty"`

	chatRequest *chat.ChatCompletionRequest
}

func toChatMessage(msgs []*Message) []*chat.ChatCompletionMessage {
	chatMessages := make([]*chat.ChatCompletionMessage, 0)
	for _, msg := range msgs {
		msgChat := &chat.ChatCompletionMessage{
			Role: string(msg.Role),
			Content: &chat.ChatCompletionMessageContent{
				ListValue: []*chat.ChatCompletionMessageContentPart{},
			},
			ReasoningContent: msg.ReasoningContent,
		}
		// 判断长度存在才加入，否则下层接收空文本可能会被报错
		if len(msg.Content) > 0 {
			msgChat.Content.ListValue = append(msgChat.Content.ListValue, &chat.ChatCompletionMessageContentPart{
				Type: chat.ChatCompletionMessageContentPartTypeText,
				Text: msg.Content,
			})
		}
		for _, img := range msg.Images {
			msgChat.Content.ListValue = append(msgChat.Content.ListValue, &chat.ChatCompletionMessageContentPart{
				Type: chat.ChatCompletionMessageContentPartTypeImageURL,
				ImageURL: &chat.ChatMessageImageURL{
					URL: img,
				},
			})
		}
		chatMessages = append(chatMessages, msgChat)
	}
	return chatMessages
}

func (l *LlmRequest) setInferParams() error {
	chatRequest := l.chatRequest
	inferP := l.InferParams
	if l.InferParams == nil || l.chatRequest == nil {
		return nil
	}
	if modelParam := inferP.ModelParams; modelParam != nil {
		chatRequest.Temperature = modelParam.Temperature
		chatRequest.PresencePenalty = modelParam.PresencePenalty
		chatRequest.FrequencyPenalty = modelParam.FrequencyPenalty
		chatRequest.RepetitionPenalty = modelParam.RepetitionPenalty
		chatRequest.TopP = modelParam.TopP
		if modelParam.Seed != nil {
			seed := int(*modelParam.Seed)
			chatRequest.Seed = &seed
		}
		if modelParam.MaxTokens != nil {
			maxTokens := int(*modelParam.MaxTokens)
			chatRequest.MaxTokens = &maxTokens
		}
		chatRequest.Stop = modelParam.StopSequences
		if modelParam.ReplyFormat != "" {
			chatRequest.ResponseFormat = &chat.ResponseFormat{
				Type: chat.ResponseFormatType(modelParam.ReplyFormat),
			}
		}
	}
	// 搜索相关参数
	if inferP.TopK != nil {
		k := int(*inferP.TopK)
		chatRequest.TopK = &k
	}
	chatRequest.WebSearchOptions = &chat.WebSearchOptions{
		Citation:                 &inferP.Citation,
		EnableEnhancement:        &inferP.EnableEnhancement,
		EnableInstructionSearch:  &inferP.EnableInstructionSearch,
		ExcludeSearchEngineTypes: inferP.ExcludeSearchEngineTypes,
		FixedSearchEngineTypes:   inferP.FixedSearchEngineTypes,
	}
	return nil
}

func (l *LlmRequest) ToChatRequest() error {
	// 金融大模型使用 cs-normal
	if l.ModelName == "" || l.ModelName == "finance-13b" {
		l.ModelName = "cs-normal"
	}
	chatUser := strconv.FormatUint(l.BizAppId, 10)
	chatRequest := &chat.ChatCompletionRequest{
		Model:    l.ModelName,
		User:     &chatUser,
		Messages: toChatMessage(l.Messages),
	}
	l.chatRequest = chatRequest
	return l.setInferParams()
}

// LlmResponse 响应封装
type LlmResponse struct {
	stats         *StatisticInfo
	searchResults []*SearchResult
	chatResponse  *chat.ChatCompletionResponse
}

// SearchResult 搜索结果
type SearchResult struct {
	Index int64  `json:"index,omitempty"` // 索引
	Title string `json:"title,omitempty"` // 标题
	Url   string `json:"url,omitempty"`   // url
}

// 统计信息
type StatisticInfo struct {
	FirstTokenCost          uint32                   `json:"first_token_cost,omitempty"`          // 首token耗时
	TotalCost               uint32                   `json:"total_cost,omitempty"`                // 推理总耗时
	InputTokens             uint32                   `json:"input_tokens,omitempty"`              // 输入token数量
	OutputTokens            uint32                   `json:"output_tokens,omitempty"`             // 输出token数量
	TotalTokens             uint32                   `json:"total_tokens,omitempty"`              // 输入+输出总token
	PromptTokensDetails     *PromptTokensDetails     `json:"prompt_tokens_details,omitempty"`     // 输入token详情
	CompletionTokensDetails *CompletionTokensDetails `json:"completion_tokens_details,omitempty"` // 输出token详情
}

// CompletionTokensDetails
type CompletionTokensDetails struct {
	ReasoningTokens uint32 `json:"reasoning_tokens,omitempty"` // 思维链token数
}

// PromptTokensDetails 输入token详情
type PromptTokensDetails struct {
	CachedTokens uint32 `json:"cached_tokens,omitempty"` // 命中缓存token数
}

// GetData ...
func (l *LlmResponse) GetData() *chat.ChatCompletionResponse {
	return l.chatResponse
}

// GetStatisticInfo ..
func (l *LlmResponse) GetStatisticInfo() *StatisticInfo {
	if l.stats != nil {
		return l.stats
	}
	if d := l.GetData(); d != nil {
		return loadUsageToStats(d.Usage)
	}
	return &StatisticInfo{}
}

func loadUsageToStats(u common.Usage) *StatisticInfo {
	return &StatisticInfo{
		InputTokens:  uint32(u.PromptTokens),
		OutputTokens: uint32(u.CompletionTokens),
		TotalTokens:  uint32(u.TotalTokens),
	}
}

// GetReplyContent 获取回复内容
func (l *LlmResponse) GetReplyContent() string {
	return l.chatResponse.GetContent()
}

// SimpleChat 非流式调用 LLM
func (r *RPC) SimpleChat(ctx context.Context, req *LlmRequest) (res *LlmResponse, err error) {
	t0 := time.Now()
	logx.D(ctx, "SimpleChat call req: %v || chat: %v",
		util.Object2String(req), util.Object2StringEscapeHTML(req.chatRequest))
	if err = req.ToChatRequest(); err != nil {
		return
	}
	stream := false
	req.chatRequest.Stream = &stream
	rsp, err := r.aiGateway.CreateChatCompletion(ctx, req.chatRequest)
	if err != nil {
		logx.E(ctx, "SimpleChat Invoke CreateChatCompletion error: %+v, requestId: %+v", err, req.RequestId)
		return nil, err
	}
	tc := time.Since(t0)
	logx.D(ctx, "SimpleChat, rsp: %v, ERR: %v, %s",
		util.Object2String(rsp), err, tc)
	stats := loadUsageToStats(rsp.Usage)
	stats.TotalCost = uint32(tc.Milliseconds())
	stats.FirstTokenCost = uint32(tc.Milliseconds())
	return &LlmResponse{chatResponse: &rsp, stats: stats}, nil
}

type LlmStreamResponse struct {
	RequestId string

	reply     string
	reasoning string
	status    *CompletionStatus
	stats     *StatisticInfo

	searchResults []*SearchResult

	chatResponse *chat.ChatCompletionStreamResponse
}

// SetReplyContent 把包更新替换内容
func (l *LlmStreamResponse) SetReplyContent(reply string) {
	l.reply = reply
}

// SetReplyContent 把包更新替换内容
func (l *LlmStreamResponse) SetReasoningContent(reasoning string) {
	l.reasoning = reasoning
}

// SetContentSearchResult 把包更新替换搜索结果
func (l *LlmStreamResponse) SetContentSearchResult(searchResults []*SearchResult) {
	l.searchResults = searchResults
}

// SetContent 把包更新替换内容
func (l *LlmStreamResponse) SetFinished() {
	isFin := l.IsFinished()
	if isFin {
		log.Info("set finished ignored")
		return
	}
	if l.status == nil {
		l.status = &CompletionStatus{}
	}
	if !l.status.IsFinished {
		l.status.IsFinished = true
		l.status.Message = "SetFinished"
	}
}

// GetSearchResults 获取搜索结果
func (l *LlmStreamResponse) GetSearchResults() []*SearchResult {
	if l.searchResults != nil {
		return l.searchResults
	}
	// get from chatResponse
	l.searchResults = convertToSearchResult(l.chatResponse.GetAnnotations())
	return l.searchResults
}

func convertToSearchResult(annotations []*chat.ChatCompletionMessageAnnotation) []*SearchResult {
	if len(annotations) == 0 {
		return nil
	}
	searchResults := make([]*SearchResult, 0, len(annotations))
	for _, item := range annotations {
		searchResults = append(searchResults, &SearchResult{
			Index: item.URLCitation.Index,
			Title: item.URLCitation.Title,
			Url:   item.URLCitation.URL,
		})
	}
	return searchResults
}

// GetData ...
func (l *LlmStreamResponse) GetData() *chat.ChatCompletionStreamResponse {
	return l.chatResponse
}

func (l *LlmStreamResponse) GetUsage() common.Usage {
	if l.chatResponse == nil {
		return common.Usage{}
	}
	return l.chatResponse.Usage
}

func (l *LlmStreamResponse) GetStatisticInfo() *StatisticInfo {
	if l.stats != nil {
		return l.stats
	}
	if d := l.GetData(); d != nil {
		return loadUsageToStats(d.Usage)
	}
	return &StatisticInfo{}
}

// IsFinished 是否结束包
func (l *LlmStreamResponse) IsFinished() bool {
	if l.status != nil {
		return l.status.IsFinished
	}
	status, err := getCompletionStatus(l.chatResponse)
	if err != nil {
		return false
	}
	l.status = &status
	return status.IsFinished
}

func (l *LlmStreamResponse) GetReplyContent() string {
	if l.reply != "" {
		return l.reply
	}
	if l.chatResponse == nil {
		return ""
	}
	return l.chatResponse.GetContent()
}

func (l *LlmStreamResponse) GetReasoningContent() string {
	if l.reasoning != "" {
		return l.reasoning
	}
	if l.chatResponse == nil {
		return ""
	}
	res := l.chatResponse.GetReasoningContent()
	return res
}

// CompletionStatus 表示完成状态的详细信息
type CompletionStatus struct {
	IsFinished bool              // 是否已完成
	Reason     chat.FinishReason // 完成原因
	IsComplete bool              // 是否完整输出（非中断）
	Message    string            // 状态描述信息
}

// getCompletionStatus 检查流式响应的完成状态
func getCompletionStatus(response *chat.ChatCompletionStreamResponse) (CompletionStatus, error) {
	if response == nil {
		return CompletionStatus{}, fmt.Errorf("nil response")
	}
	if len(response.Choices) == 0 {
		return CompletionStatus{}, fmt.Errorf("empty choices in response")
	}
	finishReason := response.Choices[0].FinishReason
	// 如果没有完成原因，表示未完成
	if finishReason == "" {
		return CompletionStatus{
			IsFinished: false,
			Reason:     "",
			IsComplete: false,
			Message:    "Response is not finished yet",
		}, nil
	}
	// 根据完成原因分类处理
	status := CompletionStatus{
		IsFinished: true,
		Reason:     finishReason,
	}
	switch finishReason {
	case chat.FinishReasonStop:
		status.IsComplete = true
		status.Message = "API returned complete model output"
	case chat.FinishReasonLength:
		status.IsComplete = false
		status.Message = "Incomplete output due to max_tokens limit"
	case chat.FinishReasonContentFilter:
		status.IsComplete = false
		status.Message = "Content omitted due to content filter"
	case chat.FinishReasonFunctionCall:
		status.IsComplete = true
		status.Message = "Model decided to call a function"
	default:
		status.IsComplete = false
		status.Message = fmt.Sprintf("Unknown finish reason: %s", finishReason)
	}
	return status, nil
}

var logOutput = []int{1, 1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000}

// Chat 流式调用 LLM
func (r *RPC) Chat(ctx context.Context, req *LlmRequest, resChan chan *LlmStreamResponse) (err error) {
	defer close(resChan)
	if err = req.ToChatRequest(); err != nil {
		return
	}
	stream := true
	req.chatRequest.Stream = &stream
	req.chatRequest.StreamOptions = &chat.StreamOptions{
		IncludeUsage:      true,
		ChunkIncludeUsage: true,
	}
	logx.D(ctx, "llmRepoImplV2|streamChat call req: %v || chat: %v",
		util.Object2String(req), util.Object2StringEscapeHTML(req.chatRequest))
	chatStream, err := r.aiGateway.CreateChatCompletionStream(ctx, req.chatRequest)
	if err != nil {
		logx.E(ctx, "llmRepoImplV2|New chat stream client error: %+v", err)
		return err
	}
	defer chatStream.Close()
	idx, logSpeed, start := 0, 0, time.Now()
	var firstCost time.Duration
	for {
		select {
		case <-ctx.Done():
			logx.I(ctx, "llmRepoImplV2|streamChat done with context done, requestId: %s", req.RequestId)
			return
		default:
			// RecvAccumulated 全量接收
			rsp, recvErr := chatStream.RecvAccumulated()
			if recvErr != nil {
				if errors.Is(recvErr, context.Canceled) || strings.Contains(recvErr.Error(), "context canceled") {
					logx.W(ctx, "llmRepoImplV2|streamChat canceled: %+v, requestId: %s", recvErr, req.RequestId)
				}
				if errors.Is(recvErr, io.EOF) {
					logx.W(ctx, "llmRepoImplV2|streamChat done with EOF, maybe timeout, requestId: %s", req.RequestId)
					return
				}
			}
			if idx == 0 {
				firstCost = time.Since(start)
			}
			llmRsp := &LlmStreamResponse{
				RequestId:    trace.SpanContextFromContext(ctx).TraceID().String(),
				chatResponse: &rsp,
			}
			if idx%(10*logOutput[logSpeed]) == 0 || llmRsp.IsFinished() {
				logx.D(ctx, "llmRepoImplV2|streamChat[%d] rsp:%s,LLM time cost:%s, total time cost:%v ERR: %v", idx,
					util.Object2StringEscapeHTML(rsp), time.Since(start), time.Since(req.StartTime).Milliseconds(), err)
				if logSpeed < len(logOutput)-1 {
					logSpeed++
				}
			}

			// reply := llmRsp.GetReplyContent()
			// lidx := strconv.Itoa(idx / 10)
			// isFirst, isFinal := utils.When(idx == 0, "1", "0"), utils.When(llmRsp.IsFinished(), "1", "0")
			// metrics.ReportLLMLength(float64(len(reply)), lidx, isFirst, isFinal)
			// metrics.ReportLLMLatency(time.Since(start).Seconds(), lidx, isFirst, isFinal)
			ms := time.Since(start).Milliseconds()
			// metrics.ReportTime("llmm_chat_ms", idx, ms)
			idx++
			resChan <- llmRsp
			if llmRsp.IsFinished() {
				stats := loadUsageToStats(rsp.Usage)
				stats.TotalCost = uint32(ms)
				stats.FirstTokenCost = uint32(firstCost.Milliseconds())
				logx.D(ctx, "llmRepoImplV2|streamChat done with Finished, requestId: %s", req.RequestId)
				return
			}
		}
	}
}
