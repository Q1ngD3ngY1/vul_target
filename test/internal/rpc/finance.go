package rpc

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/finance"
	finance "git.woa.com/adp/pb-go/platform/platform_charger"
	pb "git.woa.com/adp/pb-go/platform/platform_manager"
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
	// RealTimeDocParse 实时文档解析计费类型
	RealTimeDocParseFinanceBizType = "RealTimeDocParse"
	// DocParseCommonModel 文档解析通用模型计费类型
	DocParseCommonModel = "Youtu/youtu-parsing-sync"
	// TokenDosageAppTypeSharedKnowledge 计费上报中共享知识库的AppType
	TokenDosageAppTypeSharedKnowledge = "shared_knowledge"
	// KbCapacityOverStorage 知识库容量超储
	KbCapacityOverStorage = "KbCapacityOverStorage"

	// 对话API场景
	DialogApiKbQaCall     = "DialogApiKbQaCall"     // 对话API-知识库问答调用
	DialogApiWorkflowNode = "DialogApiWorkflowNode" // 对话API-工作流节点
	DialogApiPluginCall   = "DialogApiPluginCall"   // 对话API-插件调用

	// 对话测试场景
	DialogTestKbQaCall     = "DialogTestKbQaCall"     // 对话测试-知识库问答调用
	DialogTestWorkflowNode = "DialogTestWorkflowNode" // 对话测试-工作流节点
	DialogTestPluginCall   = "DialogTestPluginCall"   // 对话测试-插件调用

	// 应用评测场景
	DialogEvalKbQaCall  = "DialogEvalKbQaCall"  // 应用评测-知识库问答调用
	AppEvalWorkflowNode = "AppEvalWorkflowNode" // 应用评测-工作流节点
	AppEvalPluginCall   = "AppEvalPluginCall"   // 应用评测-插件调用

	// 渠道/体验用户端场景
	ChannelExpKbQaCall     = "ChannelExpKbQaCall"     // 渠道/体验用户端-知识库问答调用
	ChannelExpWorkflowNode = "ChannelExpWorkflowNode" // 渠道/体验用户端-工作流
	ChannelExpPluginCall   = "ChannelExpPluginCall"   // 渠道/体验用户端-插件调用
)

type FinanceRPC interface {
	DescribeAccountStatus(ctx context.Context, uin string, sid uint64, modelName, subBizType string) (uint32, error)
	// GetModelToken 获取token余量
	GetModelToken(ctx context.Context, uin string, sid uint64, modelName string) (float64, bool, bool)
	// GetModelStatus 获取token账户状态
	GetModelStatus(ctx context.Context, uin string, sid uint64, modelName string) (uint32, error)
	// GetModelStatusBySubBizType 根据业务类型获取token账户状态
	GetModelStatusBySubBizType(ctx context.Context, uin string, sid uint64, modelName, subBizType string) (uint32, error)
	// GetCorpMaxCharSize 获取企业的最大字符数
	GetCorpMaxCharSize(ctx context.Context, sid uint64, uin string) (uint64, error)
	// ReportTokenDosage 上报token用量
	ReportTokenDosage(ctx context.Context, corp *pb.DescribeCorpRsp, dosage *entity.TokenDosage, subBizType string) error
	// DescribeAccountQuota 获取账户配额
	DescribeAccountQuota(ctx context.Context, uin string, sid uint64) (*finance.DescribeAccountQuotaRsp, error)
}

// DescribeAccountStatus 根据业务类型获取token账户状态
func (r *RPC) DescribeAccountStatus(ctx context.Context, uin string, sid uint64, modelName, subBizType string) (uint32, error) {
	logx.D(ctx, "DescribeAccountStatus|uin:%s, modelName:%s, sid:%d", uin, modelName, sid)
	req := &finance.DescribeAccountStatusReq{
		Biz:       &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: subBizType},
		Account:   &finance.Account{Sid: finance.SID(sid), Uin: uin},
		ModelName: modelName,
	}
	rsp, err := r.finance.DescribeAccountStatus(ctx, req)
	if err != nil {
		logx.E(ctx, "DescribeAccountStatus failed, err:%+v", err)
		return 0, err
	}
	return rsp.GetStatus(), nil
}

// GetModelToken 获取token余量
func (r *RPC) GetModelToken(ctx context.Context, uin string, sid uint64, modelName string) (float64, bool, bool) {
	req := &finance.DescribeAccountInfoReq{
		Biz:       &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: KnowledgeQAFinanceBizType},
		Account:   &finance.Account{Sid: finance.SID(sid), Uin: uin},
		ModelName: modelName,
	}
	logx.I(ctx, "GetModelToken req uin: %s|modelName:%s|req:%+v", uin, modelName, req)
	rsp, err := r.finance.DescribeAccountInfo(ctx, req)
	logx.I(ctx, "GetModelToken rsp %v, ERR: %v", rsp, err)
	if err != nil {
		logx.E(ctx, "GetModelToken error: %+v", err)
		return 0, false, false
	}
	return rsp.GetBalance(), rsp.GetIsExclusive(), rsp.GetIsPostPayValid()
}

// GetModelStatus 获取token账户状态
func (r *RPC) GetModelStatus(ctx context.Context, uin string, sid uint64, modelName string) (uint32, error) {
	logx.I(ctx, "GetModelStatus|uin:%s, modelName:%s, sid:%d", uin, modelName, sid)
	req := &finance.DescribeAccountStatusReq{
		Biz:       &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: KnowledgeQAFinanceBizType},
		Account:   &finance.Account{Sid: finance.SID(sid), Uin: uin},
		ModelName: modelName,
	}
	rsp, err := r.finance.DescribeAccountStatus(ctx, req)
	if err != nil {
		logx.E(ctx, "GetModelStatus|DescribeAccountStatus failed, err:%+v", err)
		return 0, err
	}
	return rsp.GetStatus(), nil
}

// GetModelStatusBySubBizType 根据业务类型获取token账户状态
func (r *RPC) GetModelStatusBySubBizType(ctx context.Context, uin string, sid uint64, modelName,
	subBizType string) (uint32, error) {
	logx.I(ctx, "GetModelStatus|uin:%s, modelName:%s, sid:%d", uin, modelName, sid)
	req := &finance.DescribeAccountStatusReq{
		Biz:       &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: subBizType},
		Account:   &finance.Account{Sid: finance.SID(sid), Uin: uin},
		ModelName: modelName,
	}
	rsp, err := r.finance.DescribeAccountStatus(ctx, req)
	if err != nil {
		logx.E(ctx, "GetModelStatus|DescribeAccountStatus failed, err:%+v", err)
		return 0, err
	}
	return rsp.GetStatus(), nil
}

// GetCorpMaxCharSize 获取企业的计费信息
func (r *RPC) GetCorpMaxCharSize(ctx context.Context, sid uint64, uin string) (uint64, error) {
	logx.I(ctx, "GetCorpMaxCharSize|called|sid:%d,uin:%s", sid, uin)
	// 查询计费余额
	req := &finance.DescribeAccountInfoReq{
		Biz:     &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: KnowledgeCapacityFinanceBizType},
		Account: &finance.Account{Sid: finance.SID(sid), Uin: uin},
		// 这里不需要指定模型名称
	}
	logx.I(ctx, "GetCorpMaxCharSize|DescribeAccountInfo req:%+v", req)
	rsp, err := r.finance.DescribeAccountInfo(ctx, req)
	if err != nil {
		logx.E(ctx, "GetCorpMaxCharSize|DescribeAccountInfo failed, err:%+v", err)
		return 0, err
	}
	logx.I(ctx, "GetCorpMaxCharSize|DescribeAccountInfo rsp:%+v", rsp)
	var maxCharSize uint64
	if rsp != nil {
		// 企业最大字符数限制 = 企业计费资源包字符数
		maxCharSize = uint64(rsp.GetTotal())
	}
	logx.I(ctx, "GetCorpMaxCharSize|success|rsp:%s", rsp.String())
	return maxCharSize, nil
}

// ReportTokenDosage 上报文档生成问答Token用量
func (r *RPC) ReportTokenDosage(ctx context.Context, corp *pb.DescribeCorpRsp, dosage *entity.TokenDosage, subBizType string) error {
	if corp == nil {
		logx.E(ctx, "ReportTokenDosage|corp is nil")
		return nil
	}
	if dosage == nil {
		logx.E(ctx, "ReportTokenDosage|dosage is nil")
		return nil
	}
	// 上报逻辑：业务不需要区分体验资源和购买资源，统一上报计费；上报需要区分输入和输出的token
	logx.D(ctx, "ReportTokenDosage|corp:%+v, dosage:%+v", corp, dosage)
	if config.IsReportDosageDisabled() {
		logx.I(ctx, "ReportTokenDosage|IsFinanceDisabled:%+v|ignore",
			config.IsReportDosageDisabled())
		return nil
	}
	req := &finance.ReportDosageReq{
		Biz:       &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: subBizType},
		Account:   &finance.Account{Sid: finance.SID(corp.GetSid()), Uin: corp.Uin},
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
	inputType := gox.IfElse(dosage.PayloadType != "", dosage.PayloadType, "input")
	if inputDosage > 0 {
		req.List = append(req.List, &finance.ReportDosageReq_Detail{
			Dosage:  inputDosage,
			Payload: `{"type":"` + inputType + `"}`,
		})
	}
	// 输出token
	outputDosage := float64(0)
	for i := range dosage.OutputDosages {
		outputDosage += float64(dosage.OutputDosages[i])
	}
	outputType := gox.IfElse(dosage.PayloadType != "", dosage.PayloadType, "output")
	if outputDosage > 0 {
		req.List = append(req.List, &finance.ReportDosageReq_Detail{
			Dosage:  outputDosage,
			Payload: `{"type":"` + outputType + `"}`,
		})
	}
	logx.D(ctx, "ReportTokenDosage|ReportDosage req:%+v", req)
	rsp, err := r.finance.ReportDosage(ctx, req)
	if err != nil {
		logx.E(ctx, "ReportTokenDosage|ReportDosage failed, err:%+v", err)
		return err
	}
	logx.D(ctx, "ReportTokenDosage|ReportDosage rsp:%+v", rsp)
	return nil
}

// DescribeAccountQuota 获取账户配额信息
func (r *RPC) DescribeAccountQuota(ctx context.Context, uin string, sid uint64) (*finance.DescribeAccountQuotaRsp, error) {
	logx.I(ctx, "DescribeAccountQuota|uin:%s, sid:%d", uin, sid)
	req := &finance.DescribeAccountQuotaReq{
		Biz:     &finance.Biz{BizType: finance.BizType_BIZ_TYPE_LKE, SubBizType: "PackageQuota"},
		Account: &finance.Account{Sid: finance.SID(sid), Uin: uin},
	}
	rsp, err := r.finance.DescribeAccountQuota(ctx, req)
	if err != nil {
		logx.E(ctx, "DescribeAccountQuota failed, err:%+v", err)
		return nil, err
	}
	logx.I(ctx, "DescribeAccountQuota success, rsp:%+v", rsp)
	return rsp, nil
}
