package kb

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/codec"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	llmLogic "git.woa.com/adp/kb/kb-config/internal/logic/llm"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"

	"github.com/google/uuid"

	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

// KBAgentGetDirSummary 获取一个目录的摘要
func KBAgentGetDirSummary(ctx context.Context, app *entity.App, request *kbEntity.GetKBDirSummaryReq,
	llmLogic *llmLogic.Logic, r *rpc.RPC) (string, string, *rpc.StatisticInfo, error) {
	start := time.Now()
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	if len(request.RequestId) == 0 {
		request.RequestId = uuid.NewString()
	}

	logx.I(ctx, "KBAgentGetDirSummary: req:%+v", request)

	// 拼接prompt中文件的信息
	var promptFileInfos string
	schemaPromptDirSummaryFileList := i18n.Translate(ctx, i18nkey.KeyKBSchemaPromptDirSummaryFileList)
	for idx, val := range request.FileInfos {
		promptFileInfos += fmt.Sprintf(schemaPromptDirSummaryFileList, idx+1, val.FileName, val.FileSummary)
	}

	// 防御性, 加上其他提示词等信息，这个地方取值7500
	inputLimit := config.GetMainConfig().KnowledgeSchema.DocSummaryInputLimit
	if len([]rune(promptFileInfos)) > inputLimit {
		promptFileInfos = string([]rune(promptFileInfos)[:inputLimit])
	}

	// 2、调用接口实现摘要
	prompt, err := llmLogic.GetPrompt(ctx, app, entity.KbSchemaDirSummaryModel)
	if err != nil {
		logx.E(ctx, "KBAgentGetDirSummary,getPromptFailed,err:%+v", err)
		return "", "", nil, err
	}
	prompt, err = util.Render(ctx, prompt, kbEntity.KBDirSummary{FileInfo: promptFileInfos})
	if err != nil {
		logx.E(ctx, "KBAgentGetDirSummary Rendor failed err:%+v", err)
		return "", "", nil, err
	}

	req := &rpc.LlmRequest{
		RequestId: codec.Message(ctx).DyeingKey(),
		BizAppId:  request.BotBizId,
		StartTime: time.Now(),
		ModelName: request.ModelName,
		Messages:  []*rpc.Message{{Role: rpc.Role_USER, Content: prompt}},
	}

	rsp, err := r.SimpleChat(newCtx, req)
	if err != nil {
		logx.E(ctx, "KBAgentGetDirSummary SimpleChat failed err:%+v,prompt:%+v", err, prompt)
		return "", "", nil, err
	}
	cost := time.Since(start).Seconds()

	dirName, dirSummary := getDirNameSummary(newCtx, rsp.GetReplyContent())
	logx.I(ctx, "KBAgentGetDirSummary cost:%+v秒, fileInfos:%+v，SimpleChat rsp:%+v,dirName:%+v,dirSummary:%+v",
		cost, request.FileInfos, jsonx.MustMarshalToString(rsp), dirName, dirSummary)
	return dirName, dirSummary, rsp.GetStatisticInfo(), nil
}

func getDirNameSummary(ctx context.Context, input string) (string, string) {
	// 编译正则表达式
	abstract := i18n.Translate(ctx, i18nkey.KeyKBSchemaPromptDirSummaryRegexAbstract)
	folder := i18n.Translate(ctx, i18nkey.KeyKBSchemaPromptDirSummaryRegexFolder)
	abstractRegex := regexp.MustCompile(fmt.Sprintf(`(?s)<%s>\s*(.*?)(?:\s*</%s>|\s*<%s>|\z)`,
		abstract, abstract, folder))
	folderRegex := regexp.MustCompile(fmt.Sprintf(`(?s)<%s>\s*(.*?)(?:\s*</%s>|\z)`, folder, folder))
	logx.D(ctx, "getDirNameSummary input:%s, abstractRegex:%s, folderRegex:%s",
		input, abstractRegex, folderRegex)
	// 提取内容
	var dirName, dirSummary string
	if absMatch := abstractRegex.FindStringSubmatch(input); len(absMatch) > 1 {
		dirSummary = strings.TrimSpace(absMatch[1])
	}
	if folderMatch := folderRegex.FindStringSubmatch(input); len(folderMatch) > 1 {
		dirName = strings.TrimSpace(folderMatch[1])
	}
	logx.D(ctx, "getDirNameSummary dirName:%s, dirSummary:%s", dirName, dirSummary)

	// 特殊处理：如果文件夹名在摘要之后且未被标签捕获
	if dirName == "" && dirSummary != "" {
		if parts := strings.SplitN(dirSummary, fmt.Sprintf("<%s>", folder), 2); len(parts) > 1 {
			dirSummary = strings.TrimSpace(parts[0])
			dirName = strings.TrimSpace(parts[1])
			logx.D(ctx, "getDirNameSummary dirName:%s, dirSummary:%s", dirName, dirSummary)
		} else {
			dirName = "1"
		}
	}
	if dirName == "" && dirSummary == "" {
		dirName = "1"
		dirSummary = input
	}

	return dirName, dirSummary
}
