// bot-knowledge-config-server
//
// @(#)doc_summary.go  星期六, 五月 24, 2025
// Copyright(c) 2025, reinhold@Tencent. All rights reserved.

package knowledge_schema

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicApp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"regexp"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	commutils "git.woa.com/dialogue-platform/go-comm/utils"
	"github.com/google/uuid"
)

// KBAgentGetDirSummary 获取一个目录的摘要
func KBAgentGetDirSummary(ctx context.Context, dao dao.Dao, app *admin.GetAppInfoRsp, request *model.GetKBDirSummaryReq) (string, string,
	*client.StatisticInfo, error) {
	start := time.Now()
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	if len(request.RequestId) == 0 {
		request.RequestId = uuid.NewString()
	}

	log.InfoContextf(ctx, "KBAgentGetDirSummary: req:%+v", request)

	// 拼接prompt中文件的信息
	var promptFileInfos string
	schemaPromptDirSummaryFileList := i18n.Translate(ctx, i18nkey.KeyKBSchemaPromptDirSummaryFileList)
	for idx, val := range request.FileInfos {
		promptFileInfos += fmt.Sprintf(schemaPromptDirSummaryFileList, idx+1, val.FileName, val.FileSummary)
	}

	// 防御性, 加上其他提示词等信息，这个地方取值7500
	inputLimit := utilConfig.GetMainConfig().KnowledgeSchema.DocSummaryInputLimit
	if len([]rune(promptFileInfos)) > inputLimit {
		promptFileInfos = string([]rune(promptFileInfos)[:inputLimit])
	}

	// 2、调用接口实现摘要
	prompt, err := logicApp.GetPrompt(ctx, dao, app, model.KbSchemaDirSummaryModel)
	if err != nil {
		log.ErrorContextf(ctx, "KBAgentGetDirSummary,getPromptFailed,err:%+v", err)
		return "", "", nil, err
	}
	prompt, err = util.Render(ctx, prompt, model.KBDirSummary{FileInfo: promptFileInfos})
	if err != nil {
		log.ErrorContextf(ctx, "KBAgentGetDirSummary Rendor failed err:%+v", err)
		return "", "", nil, err
	}

	req := &client.LlmRequest{
		RequestId: util.RequestID(ctx, "KBAgentGetDirSummary.SessionID", uuid.NewString()),
		BizAppId:  request.BotBizId,
		StartTime: time.Now(),
		ModelName: request.ModelName,
		Messages:  []*client.Message{{Role: client.Role_USER, Content: prompt}},
	}

	rsp, err := client.SimpleChat(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "KBAgentGetDirSummary SimpleChat failed err:%+v,prompt:%+v", err, prompt)
		return "", "", nil, err
	}
	cost := time.Since(start).Seconds()

	dirName, dirSummary := getDirNameSummary(ctx, rsp.GetReplyContent())
	log.InfoContextf(ctx, "KBAgentGetDirSummary cost:%+v秒, fileInfos:%+v，SimpleChat rsp:%+v,dirName:%+v,dirSummary:%+v",
		cost, request.FileInfos, commutils.ToJsonString(rsp), dirName, dirSummary)
	return dirName, dirSummary, rsp.GetStatisticInfo(), nil
}

func getDirNameSummary(ctx context.Context, input string) (string, string) {
	// 编译正则表达式
	abstract := i18n.Translate(ctx, i18nkey.KeyKBSchemaPromptDirSummaryRegexAbstract)
	folder := i18n.Translate(ctx, i18nkey.KeyKBSchemaPromptDirSummaryRegexFolder)
	abstractRegex := regexp.MustCompile(fmt.Sprintf(`(?s)<%s>\s*(.*?)(?:\s*</%s>|\s*<%s>|\z)`,
		abstract, abstract, folder))
	folderRegex := regexp.MustCompile(fmt.Sprintf(`(?s)<%s>\s*(.*?)(?:\s*</%s>|\z)`, folder, folder))
	log.DebugContextf(ctx, "getDirNameSummary input:%s, abstractRegex:%s, folderRegex:%s",
		input, abstractRegex, folderRegex)
	// 提取内容
	var dirName, dirSummary string
	if absMatch := abstractRegex.FindStringSubmatch(input); len(absMatch) > 1 {
		dirSummary = strings.TrimSpace(absMatch[1])
	}
	if folderMatch := folderRegex.FindStringSubmatch(input); len(folderMatch) > 1 {
		dirName = strings.TrimSpace(folderMatch[1])
	}
	log.DebugContextf(ctx, "getDirNameSummary dirName:%s, dirSummary:%s", dirName, dirSummary)

	// 特殊处理：如果文件夹名在摘要之后且未被标签捕获
	if dirName == "" && dirSummary != "" {
		if parts := strings.SplitN(dirSummary, fmt.Sprintf("<%s>", folder), 2); len(parts) > 1 {
			dirSummary = strings.TrimSpace(parts[0])
			dirName = strings.TrimSpace(parts[1])
			log.DebugContextf(ctx, "getDirNameSummary dirName:%s, dirSummary:%s", dirName, dirSummary)
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
