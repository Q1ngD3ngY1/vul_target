package rpc

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec"
)

type InfoSecRPC interface {
	CheckAuditSendItem(ctx context.Context, appBizId, auditBizId uint64, appInfosecBizType, corpInfosecBizType string,
		params *releaseEntity.AuditItem) error
	CheckDbSourceField(ctx context.Context, appBizId, dbSourceBizId uint64, source, content string) error
}

// CheckAuditSend 校验发布审核参数
func (r *RPC) CheckAuditSendItem(ctx context.Context, appBizId, auditBizId uint64, appInfosecBizType, corpInfosecBizType string,
	params *releaseEntity.AuditItem) error {
	t0 := time.Now()
	auditReq := &infosec.CheckReq{
		User: &infosec.CheckReq_User{
			AccountType: params.AccountType,
			Uin:         fmt.Sprintf("%d", appBizId),
			Nick:        params.Nick,
			HeadUrl:     params.HeadURL,
			Signature:   params.Greeting,
		},
		Id:       fmt.Sprintf("%d", auditBizId),
		PostTime: time.Now().Unix(),
		Source:   params.Source,
		Type:     params.Typ,
		Url:      params.URL,
		Content:  params.Content,
		BizType:  gox.IfElse(len(appInfosecBizType) == 0, corpInfosecBizType, appInfosecBizType),
	}
	opts := make([]client.Option, 0)
	if params.EnvSet != "" {
		contextx.Metadata(ctx).WithEnvSet(params.EnvSet)
		opts = append(opts, client.WithCalleeMetadata(contextx.MDEnvSet, params.EnvSet))
	}

	req, err := r.infosec.Check(ctx, auditReq)
	if err != nil {
		logx.E(ctx, "请求送审失败 req:%+v err:%+v", auditReq, err)
		return err
	}
	if req.GetResultCode() == releaseEntity.AuditResultFail {
		log.InfoContext(ctx, "CheckAuditSend|appBizId:%d  auditBizId:%d params:%+v", appBizId, auditBizId, params)
		return errs.ErrInvalidFields
	}
	t1 := time.Now()
	logx.I(ctx, "CheckAuditSend|cost:%v", t1.Sub(t0).Milliseconds())

	return nil
}

// CheckDbSourceField  审核用户自定义的数据库描述
func (r *RPC) CheckDbSourceField(ctx context.Context, appBizId, dbSourceBizId uint64, source, content string) error {
	if len(content) == 0 {
		return nil
	}
	t0 := time.Now()
	auditReq := &infosec.CheckReq{
		User: &infosec.CheckReq_User{
			AccountType: releaseEntity.AccountTypeOther,
			Uin:         fmt.Sprintf("%d", appBizId),
		},
		Id:       fmt.Sprintf("%d", dbSourceBizId),
		PostTime: time.Now().Unix(),
		Source:   source,
		Type:     releaseEntity.AuditTypePlainText,
		Content:  content,
		BizType:  releaseEntity.AuditDbSourceCheckBizType,
	}

	req, err := r.infosec.Check(ctx, auditReq)
	if err != nil {
		logx.E(ctx, "请求送审失败 req:%+v err:%+v", auditReq, err)
		return err
	}
	if req.GetResultCode() == releaseEntity.AuditResultFail {
		log.InfoContext(ctx, "CheckDbSourceName|appBizId:%d dbSourceBizId:%d content:%s", appBizId, dbSourceBizId, content)
		return errs.ErrInvalidFields
	}
	t1 := time.Now()
	logx.I(ctx, "CheckDbSourceField|cost:%v", t1.Sub(t0).Milliseconds())
	return nil
}
