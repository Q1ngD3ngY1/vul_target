package service

import (
	"context"

	"git.woa.com/adp/common/x/gox/slicex"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// CreateSynonyms 创建同义词
func (s *Service) CreateSynonyms(ctx context.Context, req *pb.CreateSynonymsReq) (*pb.CreateSynonymsRsp, error) {
	newReq := &appConfig.CreateSynonymsReq{
		BotBizId:     req.GetBotBizId(),
		CateBizId:    req.GetCateBizId(),
		StandardWord: req.GetStandardWord(),
		Synonyms:     req.GetSynonyms(),
	}
	newRsp, err := s.rpc.AppAdmin.CreateSynonyms(ctx, newReq)
	if newRsp != nil {
		return &pb.CreateSynonymsRsp{
			SynonymBizId:    newRsp.GetSynonymBizId(),
			ConflictType:    newRsp.GetConflictType(),
			ConflictContent: newRsp.GetConflictContent(),
		}, err
	}
	return nil, err
}

func appConfigSynonymPB2ServerPB(synonym *appConfig.Synonym) *pb.Synonym {
	return &pb.Synonym{
		SynonymBizId: synonym.GetSynonymBizId(),
		StandardWord: synonym.GetStandardWord(),
		Synonyms:     synonym.GetSynonyms(),
		Status:       synonym.GetStatus(),
		StatusDesc:   synonym.GetStatusDesc(),
		CreateTime:   synonym.GetCreateTime(),
		UpdateTime:   synonym.GetUpdateTime(),
	}
}

func appConfigSynonymsListPB2ServerPB(synonyms []*appConfig.Synonym) []*pb.Synonym {
	return slicex.Map(synonyms, func(synonym *appConfig.Synonym) *pb.Synonym {
		return appConfigSynonymPB2ServerPB(synonym)
	})
}

// ListSynonyms 同义词列表
func (s *Service) ListSynonyms(ctx context.Context, req *pb.ListSynonymsReq) (*pb.ListSynonymsRsp, error) {
	newReq := &appConfig.DescribeSynonymsListReq{
		Query:         req.GetQuery(),
		CateBizId:     req.GetCateBizId(),
		ReleaseStatus: req.GetReleaseStatus(),
		PageNumber:    req.GetPageNumber(),
		PageSize:      req.GetPageSize(),
		BotBizId:      req.GetBotBizId(),
		SynonymBizId:  req.GetSynonymBizId(),
		SynonymBizIds: req.GetSynonymBizIds(),
	}
	newRsp, err := s.rpc.AppAdmin.DescribeSynonymsList(ctx, newReq)
	if newRsp != nil {
		return &pb.ListSynonymsRsp{
			Total:      newRsp.GetTotal(),
			PageNumber: newRsp.GetPageNumber(),
			List:       appConfigSynonymsListPB2ServerPB(newRsp.GetList()),
		}, err
	}
	return nil, err
}

// ModifySynonyms 更新同义词
func (s *Service) ModifySynonyms(ctx context.Context, req *pb.ModifySynonymsReq) (*pb.ModifySynonymsRsp, error) {
	newReq := &appConfig.ModifySynonymsReq{
		SynonymBizId: req.GetSynonymBizId(),
		StandardWord: req.GetStandardWord(),
		Synonyms:     req.GetSynonyms(),
		CateBizId:    req.GetCateBizId(),
		BotBizId:     req.GetBotBizId(),
	}
	newRsp, err := s.rpc.AppAdmin.ModifySynonyms(ctx, newReq)
	if newRsp != nil {
		return &pb.ModifySynonymsRsp{
			ConflictType:    newRsp.GetConflictType(),
			ConflictContent: newRsp.GetConflictContent(),
		}, err
	}
	return nil, err
}

// DeleteSynonyms 删除同义词
func (s *Service) DeleteSynonyms(ctx context.Context, req *pb.DeleteSynonymsReq) (*pb.DeleteSynonymsRsp, error) {
	newReq := &appConfig.DeleteSynonymsReq{
		SynonymBizIds: req.GetSynonymBizIds(),
		BotBizId:      req.GetBotBizId(),
	}
	_, err := s.rpc.AppAdmin.DeleteSynonyms(ctx, newReq)
	if err != nil {
		return nil, err
	}
	return &pb.DeleteSynonymsRsp{}, nil
}

// UploadSynonymsList 上传同义词列表
func (s *Service) UploadSynonymsList(ctx context.Context, req *pb.UploadSynonymsListReq) (*pb.UploadSynonymsListRsp,
	error) {
	newReq := &appConfig.UploadSynonymsListReq{
		BotBizId: req.GetBotBizId(),
		FileName: req.GetFileName(),
		CosUrl:   req.GetCosUrl(),
		CosHash:  req.GetCosHash(),
		Size:     req.GetSize(),
	}
	newRsp, err := s.rpc.AppAdmin.UploadSynonymsList(ctx, newReq)
	if newRsp != nil {
		return &pb.UploadSynonymsListRsp{
			ErrorMsg:      newRsp.GetErrorMsg(),
			ErrorLink:     newRsp.GetErrorLink(),
			ErrorLinkText: newRsp.GetErrorLinkText(),
		}, err
	}
	return nil, err
}

func listSynonymsReqPB2AppPB(req *pb.ListSynonymsReq) *appConfig.DescribeSynonymsListReq {
	return &appConfig.DescribeSynonymsListReq{
		Query:         req.GetQuery(),
		CateBizId:     req.GetCateBizId(),
		ReleaseStatus: req.GetReleaseStatus(),
		PageNumber:    req.GetPageNumber(),
		PageSize:      req.GetPageSize(),
		BotBizId:      req.GetBotBizId(),
		SynonymBizId:  req.GetSynonymBizId(),
		SynonymBizIds: req.GetSynonymBizIds(),
	}
}

// ExportSynonymsList 导出同义词列表
func (s *Service) ExportSynonymsList(ctx context.Context, req *pb.ExportSynonymsListReq) (*pb.ExportSynonymsListRsp,
	error) {
	newReq := &appConfig.ExportSynonymsListReq{
		Filters:        listSynonymsReqPB2AppPB(req.GetFilters()),
		BotBizId:       req.GetBotBizId(),
		SynonymsBizIds: req.GetSynonymsBizIds(),
	}
	_, err := s.rpc.AppAdmin.ExportSynonymsList(ctx, newReq)
	return &pb.ExportSynonymsListRsp{}, err
}

// GroupSynonyms 分类批量操作
func (s *Service) GroupSynonyms(ctx context.Context, req *pb.GroupObjectReq) (*pb.GroupObjectRsp, error) {
	newReq := &appConfig.GroupObjectReq{
		Ids:       req.GetIds(),
		CateId:    req.GetCateId(),
		BotBizId:  req.GetBotBizId(),
		BizIds:    req.GetBizIds(),
		CateBizId: req.GetCateBizId(),
	}
	_, err := s.rpc.AppAdmin.GroupSynonyms(ctx, newReq)
	return &pb.GroupObjectRsp{}, err
}
