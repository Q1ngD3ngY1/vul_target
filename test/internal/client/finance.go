package client

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/billing"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/finance/finance"
)

const (
	// KnowledgeQAFinanceBizType 知识问答计费类型
	KnowledgeQAFinanceBizType = "KnowledgeQA"
	// KnowledgeCapacityFinanceBizType 知识容量计费类型
	KnowledgeCapacityFinanceBizType = "KnowledgeCapacity"
	// DocExtractQABizType 文档生成问答计费类型
	DocExtractQABizType = "DocExtractQA"
	// DocExtractSimilarQAType 生成相似问计费类型
	DocExtractSimilarQAType = "DocExtractSimilarQA"
	// KnowledgeSchemaFinanceBizType 知识库schema生成计费类型
	KnowledgeSchemaFinanceBizType = "SchemaExtract"

	// TokenDosageAppTypeSharedKnowledge 计费上报中共享知识库的AppType
	TokenDosageAppTypeSharedKnowledge = "shared_knowledge"
)

// DescribeAccountStatus 根据业务类型获取token账户状态
func DescribeAccountStatus(ctx context.Context, uin string, sid int, modelName,
	subBizType string) (uint32, error) {
	log.DebugContextf(ctx, "DescribeAccountStatus|uin:%s, modelName:%s, sid:%d", uin, modelName, sid)
	req := &finance.DescribeAccountStatusReq{
		Biz:       &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: subBizType},
		Account:   &finance.Account{Sid: finance.SID(sid), Uin: uin},
		ModelName: modelName,
	}
	rsp, err := financeClientCli.DescribeAccountStatus(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "DescribeAccountStatus failed, err:%+v", err)
		return 0, err
	}
	return rsp.GetStatus(), nil
}

// ReportTokenDosage 上报文档生成问答Token用量
func ReportTokenDosage(ctx context.Context, corp *model.Corp, dosage *billing.TokenDosage, subBizType string) error {
	if corp == nil {
		log.ErrorContextf(ctx, "ReportTokenDosage|corp is nil")
		return nil
	}
	if dosage == nil {
		log.ErrorContextf(ctx, "ReportTokenDosage|dosage is nil")
		return nil
	}
	// 上报逻辑：业务不需要区分体验资源和购买资源，统一上报计费；上报需要区分输入和输出的token
	log.DebugContextf(ctx, "ReportTokenDosage|corp:%+v, dosage:%+v", corp, dosage)
	if config.IsReportDosageDisabled() {
		log.InfoContextf(ctx, "ReportTokenDosage|IsFinanceDisabled:%+v|ignore",
			config.IsReportDosageDisabled())
		return nil
	}
	req := &finance.ReportDosageReq{
		Biz:       &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: subBizType},
		Account:   &finance.Account{Sid: finance.SID(corp.SID), Uin: corp.Uin},
		ModelName: dosage.ModelName,
		DosageId:  dosage.RecordID,
		Payload:   fmt.Sprintf(`{"AppID":"%v","AppType":"%v"}`, dosage.AppID, dosage.AppType),
	}
	if !dosage.StartTime.IsZero() {
		req.StartTime = uint64(dosage.StartTime.Unix())
	}
	if !dosage.EndTime.IsZero() {
		req.EndTime = uint64(dosage.EndTime.Unix())
	}
	// 输入token
	inputDosage := float64(0)
	for i := range dosage.InputDosages {
		inputDosage += float64(dosage.InputDosages[i])
	}
	if inputDosage > 0 {
		req.List = append(req.List, &finance.ReportDosageReq_Detail{
			Dosage:  inputDosage,
			Payload: `{"type":"input"}`,
		})
	}
	// 输出token
	outputDosage := float64(0)
	for i := range dosage.OutputDosages {
		outputDosage += float64(dosage.OutputDosages[i])
	}
	if outputDosage > 0 {
		req.List = append(req.List, &finance.ReportDosageReq_Detail{
			Dosage:  outputDosage,
			Payload: `{"type":"output"}`,
		})
	}
	log.DebugContextf(ctx, "ReportTokenDosage|ReportDosage req:%+v", req)
	rsp, err := financeClientCli.ReportDosage(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "ReportTokenDosage|ReportDosage failed, err:%+v", err)
		return err
	}
	log.DebugContextf(ctx, "ReportTokenDosage|ReportDosage rsp:%+v", rsp)
	return nil
}
