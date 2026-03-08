package rpc

import (
	"context"
	"errors"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	adpCommon "git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/platform/platform_manager"
	dataStat "git.woa.com/adp/pb-go/platform/platform_metrology"
	"github.com/spf13/cast"
)

type DataStatRPC interface {
	Counter(ctx context.Context, req *dataStat.CounterReq) error
	ReportBusinessUsage(ctx context.Context, req *dataStat.ReportBusinessUsageReq) (*dataStat.ReportBusinessUsageRsp, error)
	ReportDosage(ctx context.Context, corp *pb.DescribeCorpRsp, dosage *entity.TokenDosage, subBizType string) error
}

// Counter 上报计数到数据统计服务
func (r *RPC) Counter(ctx context.Context, req *dataStat.CounterReq) error {
	logx.I(ctx, "Counter req:%+v", req)
	if req.GetCorpBizId() == 0 {
		logx.E(ctx, "Counter corpBizId is 0")
		return errs.ErrSystem
	}
	if req.GetSpaceId() == "" {
		logx.E(ctx, "Counter spaceId is empty")
		return errs.ErrSystem
	}
	_, err := r.dataStat.Counter(ctx, req)
	if err != nil {
		logx.E(ctx, "Counter Failed, err:%+v", err)
		return err
	}
	return nil
}

// ReportBusinessUsage 上报业务用量到数据统计服务
func (r *RPC) ReportBusinessUsage(ctx context.Context, req *dataStat.ReportBusinessUsageReq) (*dataStat.ReportBusinessUsageRsp, error) {
	logx.I(ctx, "ReportBusinessUsage req:%+v", req)
	// 参数验证逻辑根据实际字段结构进行调整
	rsp, err := r.dataStat.ReportBusinessUsage(ctx, req)
	if err != nil {
		logx.E(ctx, "ReportBusinessUsage Failed, err:%+v", err)
		return nil, err
	}
	logx.I(ctx, "ReportBusinessUsage success, rsp:%+v", rsp)
	return rsp, nil
}

// ReportDosage 上报用量到数据统计服务
func (r *RPC) ReportDosage(ctx context.Context, corp *pb.DescribeCorpRsp, dosage *entity.TokenDosage, subBizType string) error {
	logx.I(ctx, "ReportDosage req:%+v", dosage)
	if corp == nil {
		return errors.New("corp is nil")
	}
	if dosage == nil {
		return errors.New("dosage is nil")
	}
	// 参数验证逻辑根据实际字段结构进行调整
	req := &dataStat.ReportDosageReq{
		Biz: &adpCommon.Biz{
			BizType:    adpCommon.BizType_BIZ_TYPE_LKE,
			SubBizType: subBizType,
		},
		Account: &adpCommon.Account{
			Uin: corp.GetUin(),
			Sid: adpCommon.SID(corp.GetSid()),
		},
		ModelName:   dosage.ModelName,
		DosageId:    dosage.RecordID,
		StartTime:   dosage.StartTime.Unix(),
		EndTime:     dosage.EndTime.Unix(),
		SkipBilling: dosage.SkipBilling,
		Payload:     &dataStat.DosagePayload{},
	}
	req.Payload.Payload = fmt.Sprintf(`{"AppID":"%v","AppType":"%v"}`, dosage.AppID, dosage.AppType) // 兼容老数据
	req.Payload.SpaceId = dosage.SpaceID
	req.Payload.AppBizId = cast.ToString(dosage.AppID)
	req.Payload.DosageType = dataStat.DosageType_DOSAGE_TYPE_MODEL
	req.Payload.DosageUnit = dataStat.DosageUnit_DOSAGE_UNIT_TOKEN
	req.Payload.KnowledgeBizId = cast.ToString(dosage.KnowledgeBaseID)
	req.Payload.ModelDosageSourceType = dataStat.ModelDosageSourceType_MODEL_DOSAGE_SOURCE_TYPE_KNOWLEDGE
	if dosage.SourceType == "chat" { // 对话请求，归属于应用开发
		req.Payload.ModelDosageSourceType = dataStat.ModelDosageSourceType_MODEL_DOSAGE_SOURCE_TYPE_APP
	}
	// 输入token
	inputDosage := float64(0)
	for i := range dosage.InputDosages {
		inputDosage += float64(dosage.InputDosages[i])
	}
	if inputDosage > 0 {
		detail := &dataStat.UsageDetail{Dosage: inputDosage}
		if dosage.SourceType != "chat" {
			detail.Payload = &dataStat.UsageDetailPayload{Type: "input"}
		}
		req.Details = append(req.Details, detail)
	}
	// 输出token
	outputDosage := float64(0)
	for i := range dosage.OutputDosages {
		outputDosage += float64(dosage.OutputDosages[i])
	}
	if outputDosage > 0 {
		detail := &dataStat.UsageDetail{Dosage: outputDosage}
		if dosage.SourceType != "chat" {
			detail.Payload = &dataStat.UsageDetailPayload{Type: "output"}
		}
		req.Details = append(req.Details, detail)
	}
	rsp, err := r.dataStat.ReportDosage(ctx, req)
	if err != nil {
		logx.E(ctx, "ReportDosage Failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "ReportDosage success, rsp:%+v", rsp)
	return nil
}
