package service

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func (s *Service) GenerateKnowledgeSchema(ctx context.Context, req *pb.GenerateKnowledgeSchemaReq) (*pb.GenerateKnowledgeSchemaRsp, error) {
	logx.I(ctx, "GenerateKnowledgeSchema Req:%+v", req)
	rsp := new(pb.GenerateKnowledgeSchemaRsp)
	if req.GetAppBizId() == 0 {
		return rsp, errs.ErrParams
	}
	appBizID := convx.Uint64ToString(req.GetAppBizId())
	app, err := s.DescribeAppAndCheckCorp(ctx, appBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	corpBizID := contextx.Metadata(ctx).CorpBizID()
	err = s.kbLogic.GenerateKnowledgeSchemaTask(ctx, app.CorpPrimaryId, corpBizID, req.GetAppBizId(), idgen.GetId(), app.PrimaryId, app.IsShared)
	if err != nil {
		logx.E(ctx, "GenerateKnowledgeSchema knowledge_schema.GenerateKnowledgeSchema fail, err: %+v", err)
		return rsp, err
	}
	return rsp, nil
}

func (s *Service) GetKnowledgeSchemaTask(ctx context.Context, req *pb.GetKnowledgeSchemaTaskReq) (*pb.GetKnowledgeSchemaTaskRsp, error) {
	logx.I(ctx, "GetKnowledgeSchemaTask Req:%+v", req)
	rsp := new(pb.GetKnowledgeSchemaTaskRsp)
	if req.GetAppBizId() == 0 {
		return rsp, errs.ErrParams
	}
	appBaseInfo, err := s.rpc.AppAdmin.GetAppBaseInfo(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "GetKnowledgeBaseConfig error: %+v", err)
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo == nil {
		return nil, errs.ErrAppNotFound
	}
	if appBaseInfo.CorpPrimaryId != contextx.Metadata(ctx).CorpID() {
		logx.E(ctx, "auth bypass, appBaseInfo.CorpPrimaryId:%d, ctx.CorpID:%d", appBaseInfo.CorpPrimaryId, contextx.Metadata(ctx).CorpID())
		return nil, errs.ErrAppNotFound
	}
	resp, err := s.kbLogic.GetKnowledgeSchemaTask(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchemaTask knowledge_schema.GetKnowledgeSchemaTask fail, err: %+v", err)
		return rsp, err
	}

	return resp, nil
}

func (s *Service) GetKnowledgeSchema(ctx context.Context, req *pb.GetKnowledgeSchemaReq) (*pb.GetKnowledgeSchemaRsp, error) {
	return s.kbLogic.GetKnowledgeSchemaLogic(ctx, req)
}
