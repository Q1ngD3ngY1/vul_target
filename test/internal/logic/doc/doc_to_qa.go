package doc

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"github.com/google/uuid"
	"regexp"
	"strings"
	"time"
)

// LLMSegmentQA 获取段落的问答对
func LLMSegmentQA(ctx context.Context, doc *model.Doc, segment *model.DocSegmentExtend, app *admin.GetAppInfoRsp,
	modelName string, prompt string) ([]*model.QA,
	*client.StatisticInfo, error) {
	start := time.Now()
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())
	prompt, err := util.Render(ctx, prompt, model.QAExtract{Content: segment.OrgData})
	if err != nil {
		return nil, nil, err
	}

	req := &client.LlmRequest{
		RequestId: util.RequestID(ctx, "LLMSegmentQA.SessionID", uuid.NewString()),
		BizAppId:  app.GetAppBizId(),
		StartTime: time.Now(),
		ModelName: modelName,
		Messages:  []*client.Message{{Role: client.Role_USER, Content: prompt}},
	}
	rsp, err := client.SimpleChat(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "LLM Segment QA error: %+v, doc: %+v", err, doc)
		return nil, nil, err
	}
	log.InfoContextf(ctx, "LLMSegmentQA|SimpleChat result:%+v", rsp)
	// 文档已过期
	if time.Unix(0, 0).Before(segment.ExpireEnd) && time.Now().After(segment.ExpireEnd) {
		log.ErrorContextf(ctx, "LLM Segment QA error: %+v, doc: %+v", errs.ErrDocToQaExpiredFail, doc)
		return nil, nil, errs.ErrDocToQaExpiredFail
	}
	segment.Outputs = rsp.GetReplyContent()
	segment.CostTime = time.Since(start).Seconds()
	if segment.Outputs == "" {
		log.DebugContextf(ctx, "getLLMRsp output is empty docID:%d fileName:%s segmentID:%d question:%s",
			doc.ID, doc.FileName, segment.ID, segment.OrgData)
		return nil, nil, errs.ErrSystem
	}
	filter := &dao.DocSegmentFilter{
		RouterAppBizID: app.GetAppBizId(),
		IDs:            []uint64{segment.ID},
	}
	updateColumns := []string{dao.DocSegmentTblColOutputs, dao.DocSegmentTblColCostTime}
	segmentInDb := &model.DocSegment{
		Outputs:  segment.Outputs,
		CostTime: segment.CostTime,
	}
	_, err = dao.GetDocSegmentDao().UpdateDocSegment(ctx, updateColumns, filter, segmentInDb)
	if err != nil {
		// 降级处理，只打印ERROR日志
		log.ErrorContextf(ctx, "LLMSegmentQA|UpdateDocSegment err:%+v", err)
	}
	qas := formatAnswerToQA(ctx, segment.Outputs, segment.ExpireEnd)
	return qas, rsp.GetStatisticInfo(), nil
}

// formatAnswerToQA 把返回格式化为QA
func formatAnswerToQA(ctx context.Context, answer string, expireEnd time.Time) []*model.QA {
	log.DebugContextf(ctx, "formatAnswerToQA answer:%s", answer)
	qaModels := make([]*model.QA, 0)
	answerReg := regexp.MustCompile(config.App().DocQA.QuestionRegexp)
	qaPairs := answerReg.Split(answer, -1)
	for _, qa := range qaPairs {
		if qa == "" {
			continue
		}
		anPairs := strings.SplitN(qa, config.App().DocQA.AnswerRegexp, 2)
		if len(anPairs) != 2 {
			continue
		}
		qaModels = append(qaModels, &model.QA{
			Question:    strings.TrimSpace(strings.TrimRight(anPairs[0], "\n")),
			Answer:      strings.TrimSpace(strings.TrimRight(anPairs[1], "\n")),
			ExpireStart: time.Now(),
			ExpireEnd:   expireEnd,
		})
	}
	return qaModels
}
