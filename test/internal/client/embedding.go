package client

// NOCA:tosa/linelength(go:generate)
//go:generate mockgen -source embedding.go -destination embedding_mock.go -package embedding

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strconv"
	"time"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/utils/rpcutils"

	"git.code.oa.com/trpc-go/trpc-filter/slime/retry"
	"git.code.oa.com/trpc-go/trpc-go/client"
	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/proto/pb-stub/vector_db_manager"
)

const (
	// CodeOK 无异常
	CodeOK = int32(0)
	// ErrImageURLError 图片url格式不对/图片不存在
	ErrImageURLError = int32(712404)
	// ErrImageFormatError 图片内容格式错误
	ErrImageFormatError = int32(712405)
	// ErrImageInferenceError 图片Embedding推理错误
	ErrImageInferenceError = int32(712500)
	// ErrImageDownloadError 图片下载超时
	ErrImageDownloadError = int32(712406)
)

const (
	// ModelQA 问答
	ModelQA = "qa"
	// ModelSE 搜索引擎
	ModelSE = "search_engine"
	// ModelDocSegment 文档段
	ModelDocSegment = "doc"
	// ModelRejectQA 拒答问题
	ModelRejectQA = "reject_qa"
	// ModelImage 图片embedding
	ModelImage = "image"
	// ModelTextSearchImage 文搜图embedding
	ModelTextSearchImage = "text_search_image"
)

// EmbType 请求embedding类型 区分是query的还是问答/文档切片的内容(取不同的前缀配置)
type EmbType string

const (
	// EmbTypeQuery query的embedding请求
	EmbTypeQuery EmbType = "query"
	// EmbTypeContent content(问答/切片等内容)的embedding请求
	EmbTypeContent EmbType = "content"
)

// Embedding 查询
// 如果是图片本身的问题，则不返回错误，即向量结果和error都是nil
func Embedding(ctx context.Context, appBizId uint64, text string) ([]float32, error) {
	conf := utilConfig.GetMainConfig().Embedding
	embType := EmbTypeContent
	online := false
	model := conf.ModelName
	text = fillInstruction(ctx, text, conf, embType)
	runes := []rune(text)
	if len(runes) > int(conf.MaxLen) {
		runes = runes[:int(conf.MaxLen)]
	}
	text = string(runes)
	req := &vector_db_manager.EmbeddingReq{
		RequestId: rpcutils.GetDyeingKey(ctx),
		Prompts:   []string{text},
		ModelName: conf.ModelName,
		AppInfo: &vector_db_manager.AppInfo{
			Biz:    "cs",
			AppKey: strconv.FormatUint(appBizId, 10),
		},
		Offline:     !online,
		Instruction: getInstruction(model, conf, embType),
	}
	opts := []client.Option{
		client.WithCalleeMethod("GetEmbedding"),
		util.WithTRPCSelector(),
		withRetry(conf.MaxRetry, conf.RetryWaitMs),
	}
	startTime := time.Now()
	log.DebugContextf(ctx, "KnowledgeGenerateSchema Embedding req:%+v", req)
	rsp, err := vector_db_manager.NewVectorObjClientProxy().Embedding(ctx, req, opts...)
	if err != nil {
		log.ErrorContextf(ctx, "KnowledgeGenerateSchema Embedding request fail, req: %+v, err: %v", req, err)
		return nil, err
	}
	log.DebugContextf(ctx, "KnowledgeGenerateSchema Embedding rsp cost:%dms", time.Since(startTime).Milliseconds())
	if rsp.GetCode() != CodeOK {
		if rsp.GetCode() == ErrImageURLError || rsp.GetCode() == ErrImageFormatError ||
			rsp.GetCode() == ErrImageInferenceError || rsp.GetCode() == ErrImageDownloadError {
			// 图片本身的问题则跳过不返回错误，向量的结果也是nil
			// 先打印error日志告警确认图片有问题，后面再调整日志级别
			log.WarnContextf(ctx, "KnowledgeGenerateSchema Embedding image invalid and ignore, %s(%d), req: %+v",
				rsp.GetErrMsg(), rsp.GetCode(), req)
			return nil, nil
		}
		err = errs.ErrGetEmbedding
		log.ErrorContextf(
			ctx, "KnowledgeGenerateSchema Embedding request fail, %s(%d), req: %+v, err: %v",
			rsp.GetErrMsg(), rsp.GetCode(), req, err,
		)
		return nil, err
	}
	if len(rsp.GetEmbeddings()) != 1 {
		err = errs.ErrGetEmbeddingEmpty
		log.ErrorContextf(ctx, "KnowledgeGenerateSchema Embedding request fail, len(GetEmbeddings) != 1, req: %+v, err: %v",
			req, err)
		return nil, err
	}
	if len(rsp.GetEmbeddings()[0].GetEmbedding()) == 0 {
		err = errs.ErrGetEmbeddingEmpty
		log.ErrorContextf(ctx, "KnowledgeGenerateSchema Embedding request fail, len(GetEmbedding) == 0, req: %+v, err: %v", req, err)
		return nil, err
	}
	return rsp.GetEmbeddings()[0].GetEmbedding(), nil
}

// fillInstruction 填充instruction（前缀）
func fillInstruction(ctx context.Context, text string, conf utilConfig.EmbeddingClientConfig, embType EmbType) string {
	if conf.DInstruction != "" || conf.QInstruction != "" {
		// llm embedding不需要加前缀
		return text
	}
	var prefix string
	switch embType {
	case EmbTypeQuery:
		prefix = conf.Prefix
	case EmbTypeContent:
		prefix = conf.ContentPrefix
	default:
		log.ErrorContextf(ctx, "embType invalid:%s", embType)
		return text
	}
	if len(prefix) > 0 {
		return fmt.Sprintf("%s%s", prefix, text)
	}
	return text
}

// getInstruction 获取模型的instruction，目前只有llm embedding模型配置使用
func getInstruction(model string, conf utilConfig.EmbeddingClientConfig, embType EmbType) string {
	if model == ModelDocSegment && embType == EmbTypeContent {
		return conf.DInstruction
	}
	return conf.QInstruction
}

// withRetry 重试
func withRetry(retryTimes, retryWaitMs int) client.Option {
	if retryWaitMs <= 0 {
		retryWaitMs = 10
	}

	r, err := retry.New(retryTimes, []int{
		terrs.RetServerOverload,
		terrs.RetServerFullLinkTimeout,
		terrs.RetClientTimeout,
		terrs.RetClientFullLinkTimeout,
		terrs.RetClientNetErr,
	}, retry.WithLinearBackoff(time.Millisecond*time.Duration(retryWaitMs)))
	if err != nil {
		log.Warnf("set retry option fail, err: %v", err)
		return func(o *client.Options) {}
	}
	return client.WithFilter(r.Invoke)
}
