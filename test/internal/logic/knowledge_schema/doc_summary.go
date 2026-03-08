// bot-knowledge-config-server
//
// @(#)doc_summary.go  星期六, 五月 24, 2025
// Copyright(c) 2025, reinhold@Tencent. All rights reserved.

package knowledge_schema

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	logicApp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"path/filepath"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	commutils "git.woa.com/dialogue-platform/go-comm/utils"
	"github.com/google/uuid"
)

func isImageByExtension(filename string) bool {
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filename), "."))
	imageExtensions := []string{"jpg", "jpeg", "png", "gif", "bmp", "webp"}
	for _, e := range imageExtensions {
		if ext == e {
			return true
		}
	}
	return false
}

// KBAgentGetOneDocSummary 获取一个文档的摘要
func KBAgentGetOneDocSummary(ctx context.Context, d dao.Dao, app *admin.GetAppInfoRsp, request *model.GetKBDocSummaryReq) (
	string, *client.StatisticInfo, error) {
	start := time.Now()
	if len(request.RequestId) == 0 {
		request.RequestId = uuid.NewString()
	}
	// 与算法沟通，图片暂不处理
	if isImageByExtension(request.FileName) {
		return "", nil, nil
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())

	log.InfoContextf(ctx, "KBAgentGetOneDocSummary: req:%+v", request)
	// 1、根据doc_id找到对应的解析id, 并下载前8K的解析文档字符
	docParse, err := d.GetDocParseByDocIDAndTypeAndStatus(ctx, request.DocID, model.DocParseTaskTypeSplitSegment,
		model.DocParseSuccess, request.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "KBAgentGetOneDocSummary,getDocParseFailed,err:%+v", err)
		return "", nil, err
	}
	parseMDContent, err := d.GetOfflineDocParseResult(ctx, docParse)
	if err != nil {
		return "", nil, err
	}

	// 解析结果为空
	if len(parseMDContent) == 0 {
		return "", nil, nil
	}

	log.InfoContextf(ctx, "KBAgentGetOneDocSummary: parseMDContent:%+v", parseMDContent)
	//
	// 防御性, 加上其他提示词等信息，这个地方取值7500
	inputLimit := utilConfig.GetMainConfig().KnowledgeSchema.DocSummaryInputLimit
	if len([]rune(parseMDContent)) > inputLimit {
		parseMDContent = string([]rune(parseMDContent)[:inputLimit])
	}

	// 2、调用接口实现摘要
	prompt, err := logicApp.GetPrompt(ctx, d, app, model.KbSchemaDocSummaryModel)
	if err != nil {
		log.ErrorContextf(ctx, "KBAgentGetOneDocSummary,getPromptFailed,err:%+v", err)
		return "", nil, err
	}
	prompt, err = util.Render(ctx, prompt, model.KBDocSummary{FileName: request.FileName, FileContent: parseMDContent})
	if err != nil {
		log.ErrorContextf(ctx, "KBAgentGetOneDocSummary Rendor failed err:%+v", err)
		return "", nil, err
	}

	req := &client.LlmRequest{
		RequestId: util.RequestID(ctx, "KBAgentGetOneDocSummary.SessionID", uuid.NewString()),
		BizAppId:  request.BotBizId,
		StartTime: time.Now(),
		ModelName: request.ModelName,
		Messages:  []*client.Message{{Role: client.Role_USER, Content: prompt}},
	}
	rsp, err := client.SimpleChat(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "KBAgentGetOneDocSummary SimpleChat failed err:%+v,prompt:%+v", err, prompt)
		return "", nil, err
	}
	cost := time.Since(start).Seconds()
	log.InfoContextf(ctx, "KBAgentGetOneDocSummary cost:%+v秒,docId:%+v,docName:%+v，SimpleChat rsp:%+v",
		cost, request.DocID, request.FileName, commutils.ToJsonString(rsp))
	return rsp.GetReplyContent(), rsp.GetStatisticInfo(), nil
}
