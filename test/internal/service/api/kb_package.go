package api

import (
	"context"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ConvertKBId 转换业务ID
func (s *Service) ConvertKBId(ctx context.Context, req *pb.ConvertKBIdReq) (*pb.ConvertKBIdRsp, error) {
	logx.I(ctx, "ConvertKBId req:%v", jsonx.MustMarshalToString(req))
	rsp, err := s.kbPKGLogic.ConvertBizIdLogic(ctx, req)
	if err != nil {
		logx.ErrorContextf(ctx, "ConvertKBId err:%v", err)
		return nil, err
	}
	logx.I(ctx, "ConvertKBId rsp:%v", jsonx.MustMarshalToString(rsp))
	return rsp, nil
}

// ExportKnowledgeBases 导出知识库
func (s *Service) ExportKnowledgeBases(ctx context.Context, req *pb.ExportKnowledgeBasesReq) (*pb.ExportKnowledgeBasesRsp, error) {
	// 参数校验
	if req.GetAppBizId() == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "AppBizId is required")
	}
	if req.GetCorpBizId() == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "CorpBizId is required")
	}
	if req.GetTaskId() == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "TaskId is required")
	}
	if req.GetSubTaskId() == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "SubTaskId is required")
	}
	if len(req.GetKnowledgeBaseIds()) == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "KnowledgeBaseIds is required")
	}
	if req.GetSpaceId() == "" {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "SpaceId is required")
	}
	if req.GetCosFilePath() == "" {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "CosFilePath is required")
	}

	// 调用logic层实现导出逻辑
	err := s.kbPKGLogic.ExportKnowledgeBases(ctx, req)
	if err != nil {
		return nil, err
	}
	return &pb.ExportKnowledgeBasesRsp{}, nil
}

// ImportKnowledgeBases 导入知识库
func (s *Service) ImportKnowledgeBases(ctx context.Context, req *pb.ImportKnowledgeBasesReq) (*pb.ImportKnowledgeBasesRsp, error) {
	// 参数校验
	if req.GetAppBizId() == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "AppBizId is required")
	}
	if req.GetTaskId() == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "TaskId is required")
	}
	if req.GetSubTaskId() == 0 {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "SubTaskId is required")
	}
	if req.GetAppPackageUrl() == "" {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "AppPackageUrl is required")
	}
	if req.GetIdMappingCosUrl() == "" {
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid, "IdMappingCosUrl is required")
	}

	// 调用logic层实现导入逻辑
	err := s.kbPKGLogic.ImportKnowledgeBases(ctx, req)
	if err != nil {
		return nil, err
	}
	return &pb.ImportKnowledgeBasesRsp{}, nil
}
