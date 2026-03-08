package doc_qa

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicApp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	logicCorp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/corp"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/google/uuid"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// GenerateSimilarQuestions 生成相似问题
func GenerateSimilarQuestions(ctx context.Context, db dao.Dao, app *admin.GetAppInfoRsp, question, answer string) (
	[]string, error) {
	if app == nil {
		return nil, errs.ErrRobotNotFound
	}
	if question == "" {
		return nil, errs.ErrGenerateSimilarParams
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	dosage, err := logicCorp.GetTokenDosage(ctx, app.GetAppBizId(), "",
		uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL))
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Init err: %v", err)
		return nil, err
	}
	if dosage == nil {
		log.ErrorContextf(ctx, "task(DocToQA) Init dosage is nil")
		return nil, errs.ErrSystem
	}
	// 生成相似问使用文档生成问答相同的模型配置
	if !logicCorp.CheckModelStatus(ctx, db, app.GetCorpId(), dosage.ModelName, client.DocExtractSimilarQAType) {
		log.WarnContextf(ctx, "GenerateSimilarQuestions checkModelStatus failed, modelName:%s", dosage.ModelName)
		return nil, errs.ErrNoTokenBalance
	}
	dosage.StartTime = time.Now()
	prompt, err := logicApp.GetPrompt(ctx, db, app, model.GenerateSimilarQAModel)
	if err != nil {
		log.ErrorContextf(ctx, "GenerateSimilarQuestions GetPrompt error: %+v", err)
		return nil, err
	}
	similarQuestions, statisticInfo, err := LLMGenerateSimilarQuestions(ctx, app.GetAppBizId(), dosage.ModelName, prompt, question, answer)
	if err != nil {
		log.ErrorContextf(ctx, "LLM GenerateSimilarQuestions error: %+v", err)
		return similarQuestions, err
	}
	log.InfoContextf(ctx, "LLM GenerateSimilarQuestions|statisticInfo:%+v", statisticInfo)
	if len(similarQuestions) == 0 { // 返回内容为空 不做计费上报
		log.WarnContextf(ctx, "LLM|similarQuestions is empty|statisticInfo:%+v",
			statisticInfo)
		return similarQuestions, nil
	}
	// 生成相似问使用文档生成问答相同的模型配置
	err = logicCorp.ReportTokenDosage(ctx, statisticInfo, dosage, db, app.GetCorpId(), client.DocExtractSimilarQAType, app.GetAppBizId())
	if err != nil {
		// 只打印ERROR日志，降级处理
		log.ErrorContextf(ctx, "LLM GenerateSimilarQuestions reportSimilarQuestionsTokenDosage error: %+v", err)
	}
	return similarQuestions, nil
}

// LLMGenerateSimilarQuestions 生成相似问题
func LLMGenerateSimilarQuestions(ctx context.Context, appBizID uint64, modelName, prompt, question, answer string) ([]string,
	*client.StatisticInfo, error) {
	start := time.Now()
	var similarQuestions []string
	prompt, err := util.Render(ctx, prompt, dao.SimilarQAExtract{Question: question, Answer: answer})
	if err != nil {
		log.ErrorContextf(ctx, "LLMGenerateSimilarQuestions Render failed, err:%+v", err)
		return similarQuestions, nil, err
	}
	if modelName == "" {
		log.ErrorContextf(ctx, "LLMGenerateSimilarQuestions failed, modelName is empty")
		return similarQuestions, nil, errs.ErrNotInvalidModel
	}

	req := &client.LlmRequest{
		RequestId: util.RequestID(ctx, "LLMGenerateSimilarQuestions.SessionID", uuid.NewString()),
		BizAppId:  appBizID,
		StartTime: time.Now(),
		ModelName: modelName,
		Messages:  []*client.Message{{Role: client.Role_USER, Content: prompt}},
	}

	rsp, err := client.SimpleChat(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "LLMGenerateSimilarQuestions error: %+v, prompt: %s", err, prompt)
		return similarQuestions, nil, err
	}
	costTime := time.Since(start).Seconds()
	similarQuestions = formatSimilarQuestions(ctx, rsp.GetReplyContent())
	log.InfoContextf(ctx, "LLMGenerateSimilarQuestions|SimpleChat prompt:%+v rsp:%+v costTime:%+v",
		prompt, rsp, costTime)
	return similarQuestions, rsp.GetStatisticInfo(), nil
}

// formatSimilarQuestions 返回结果格式化为相似问集合
func formatSimilarQuestions(ctx context.Context, content string) []string {
	log.DebugContextf(ctx, "formatSimilarQuestions question:%s", content)
	// 将 "\\n" 替换为 "\n"
	content = strings.ReplaceAll(content, "\\n", "\\\n")
	// 然后按 "\n" 分割字符串
	lines := strings.Split(content, "\n")
	var similarQuestions []string
	questionSet := make(map[string]bool) // 用于去重的集合
	lastNumber := 0
	for k, line := range lines {
		if line == "" {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d is empty", k)
			continue
		}
		// 如果这一行不是以数字+符号点开头的，丢弃掉
		if !regexp.MustCompile(`^\d+\.`).MatchString(line) {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d lineContent:%s notNumStart", k, line)
			continue
		}
		// 检查数字是否有序
		numberStr := regexp.MustCompile(`^\d+`).FindString(line)
		number, _ := strconv.Atoi(numberStr)
		if number != lastNumber+1 {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d lineContent:%s lastNumber:%d NumIndexFail",
				k, line, lastNumber)
			continue
		}
		lastNumber = number
		// 只截取掉第一个数字和点，保留后面的内容
		line = regexp.MustCompile(`^\d+\.`).ReplaceAllString(line, "")
		line = strings.TrimSpace(line)
		if line == "" {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d lineContent:%s is empty", k, line)
			continue
		}
		// 检查是否已存在相同问题
		if !questionSet[line] {
			questionSet[line] = true
			similarQuestions = append(similarQuestions, line)
		} else {
			log.DebugContextf(ctx, "formatSimilarQuestions duplicate question found: %s", line)
		}
	}
	return similarQuestions
}
