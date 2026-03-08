package qa

import (
	"context"
	"regexp"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/codec"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// LLMSegmentQA 获取段落的问答对
func (l *Logic) LLMSegmentQA(ctx context.Context, doc *docEntity.Doc, segment *segEntity.DocSegmentExtend, app *entity.App,
	modelName string, prompt string) ([]*qaEntity.QA,
	*rpc.StatisticInfo, error) {
	logx.D(ctx, "LLMSegmentQA|docID:%d fileName:%s segmentID:%d question:%s",
		doc.ID, doc.FileName, segment.ID, segment.OrgData)
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	start := time.Now()
	prompt, err := util.Render(newCtx, prompt, entity.QAExtract{Content: segment.OrgData})
	if err != nil {
		return nil, nil, err
	}

	req := &rpc.LlmRequest{
		RequestId: codec.Message(ctx).DyeingKey(),
		BizAppId:  app.BizId,
		StartTime: time.Now(),
		ModelName: modelName,
		Messages:  []*rpc.Message{{Role: rpc.Role_USER, Content: prompt}},
	}
	rsp, err := l.rpc.AIGateway.SimpleChat(newCtx, req)
	if err != nil {
		logx.E(ctx, "LLM Segment QA error: %+v, doc: %+v", err, doc)
		return nil, nil, err
	}
	logx.I(ctx, "LLMSegmentQA|SimpleChat result:%+v", rsp)
	// 文档已过期
	if time.Unix(0, 0).Before(segment.ExpireEnd) && time.Now().After(segment.ExpireEnd) {
		logx.E(ctx, "LLM Segment QA error: %+v, doc: %+v", errs.ErrDocToQaExpiredFail, doc)
		return nil, nil, errs.ErrDocToQaExpiredFail
	}
	segment.Outputs = rsp.GetReplyContent()
	segment.CostTime = time.Since(start).Seconds()
	if segment.Outputs == "" {
		logx.D(ctx, "getLLMRsp output is empty docID:%d fileName:%s segmentID:%d question:%s",
			doc.ID, doc.FileName, segment.ID, segment.OrgData)
		return nil, nil, errs.ErrSystem
	}
	filter := &segEntity.DocSegmentFilter{
		RouterAppBizID: app.BizId,
		IDs:            []uint64{segment.ID},
	}
	updateColumns := []string{segEntity.DocSegmentTblColOutputs, segEntity.DocSegmentTblColCostTime}
	segmentInDb := &segEntity.DocSegment{
		Outputs:  segment.Outputs,
		CostTime: segment.CostTime,
	}
	db, err := knowClient.GormClient(ctx, l.segDao.Query().TDocSegment.TableName(), 0, app.BizId)
	if err != nil {
		logx.E(ctx, "LLMSegmentQA|GetGormClient err:%+v", err)
		return nil, nil, err
	}
	err = l.segDao.UpdateDocSegmentWithTx(ctx, updateColumns, filter, segmentInDb, db)
	if err != nil {
		// 降级处理，只打印ERROR日志
		logx.E(ctx, "LLMSegmentQA|UpdateDocSegment err:%+v", err)
	}
	qas := formatAnswerToQA(ctx, segment.Outputs, segment.ExpireEnd)
	for _, qa := range qas {
		qa.EnableScope = doc.EnableScope
	}
	return qas, rsp.GetStatisticInfo(), nil
}

// formatAnswerToQA 把返回格式化为QA
func formatAnswerToQA(ctx context.Context, answer string, expireEnd time.Time) []*qaEntity.QA {
	logx.D(ctx, "formatAnswerToQA answer:%s", answer)
	qaModels := make([]*qaEntity.QA, 0)
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
		qaModels = append(qaModels, &qaEntity.QA{
			Question:    strings.TrimSpace(strings.TrimRight(anPairs[0], "\n")),
			Answer:      strings.TrimSpace(strings.TrimRight(anPairs[1], "\n")),
			ExpireStart: time.Now(),
			ExpireEnd:   expireEnd,
		})
	}
	return qaModels
}
