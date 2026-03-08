// bot-knowledge-config-server
//
// @(#)finance.go  星期二, 五月 28, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

package dao

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
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

// GetCorpBillingInfo 获取企业的计费信息
func (d *dao) GetCorpBillingInfo(ctx context.Context, corp *model.Corp) (*model.Corp, error) {
	log.InfoContextf(ctx, "GetCorpBillingInfo|called|corp:%+v", corp)
	if config.IsFinanceDisabled() {
		log.InfoContextf(ctx, "GetCorpBillingInfo|IsFinanceDisabled:%+v|ignore", config.IsFinanceDisabled())
		return corp, nil
	}
	// 查询计费余额
	req := &finance.DescribeAccountInfoReq{
		Biz:     &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: KnowledgeCapacityFinanceBizType},
		Account: &finance.Account{Sid: finance.SID(corp.SID), Uin: corp.Uin},
		// 这里不需要指定模型名称
	}
	log.InfoContextf(ctx, "GetCorpBillingInfo|DescribeAccountInfo req:%+v", req)
	rsp, err := d.qBotFinanceClient.DescribeAccountInfo(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpBillingInfo|DescribeAccountInfo failed, err:%+v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "GetCorpBillingInfo|DescribeAccountInfo rsp:%+v", rsp)
	if rsp != nil {
		// 企业最大字符数限制 = 企业计费资源包字符数
		corp.MaxCharSize = uint64(rsp.GetTotal())
	}
	log.InfoContextf(ctx, "GetCorpBillingInfo|success|corp:%+v", corp)
	return corp, nil
}

// CreateResourceExpireTask 创建计费资源包到期后离线任务
func (d *dao) CreateResourceExpireTask(ctx context.Context, params model.ResExpireParams) error {
	return newResourceExpireTask(ctx, params)
}

// CreateDocResumeTask 创建文档恢复任务
func (d *dao) CreateDocResumeTask(ctx context.Context, corpID, robotID, stuffID uint64, docExceededTimes []model.DocExceededTime) error {
	return newDocResumeTask(ctx, corpID, robotID, stuffID, docExceededTimes)
}

// CreateQAResumeTask 创建问答恢复任务
func (d *dao) CreateQAResumeTask(ctx context.Context, corpID, robotID, stuffID uint64, qaExceededTimes []model.QAExceededTime) error {
	return newQAResumeTask(ctx, corpID, robotID, stuffID, qaExceededTimes)
}
