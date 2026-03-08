package service

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func (s *Service) GetMigrateThirdPartyProcess(ctx context.Context, req *pb.GetMigrateThirdPartyProcessReq) (*pb.GetMigrateThirdPartyProcessRsp, error) {
	log.DebugContextf(ctx, "get migrate third doc is coming, req body is %+v", req)
	return s.thirdDocLogic.GetThirdDocLogic(req.GetSourceFrom()).GetImportProgress(ctx, req)
}

func (s *Service) MigrateThirdPartyDoc(ctx context.Context, req *pb.MigrateThirdPartyDocReq) (*pb.MigrateThirdPartyDocRsp, error) {
	log.DebugContextf(ctx, "Migtrate third party doc req is coming, req body is %+v", req)
	return s.thirdDocLogic.GetThirdDocLogic(req.GetSourceFrom()).ImportDoc(ctx, req)
}

func (s *Service) ListThirdPartyDoc(ctx context.Context, req *pb.ListThirdPartyDocReq) (*pb.ListThirdPartyDocRsp, error) {
	log.DebugContextf(ctx, "ListThirdPartyDoc req is coming, req body: %v", req)
	return s.thirdDocLogic.GetThirdDocLogic(req.GetSourceFrom()).ListDoc(ctx, req)
}
