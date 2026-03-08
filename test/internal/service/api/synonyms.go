package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// SynonymsNER 同义词 NER 接口
func (s *Service) SynonymsNER(ctx context.Context, req *pb.SynonymsNERReq) (*pb.SynonymsNERRsp, error) {
	log.InfoContextf(ctx, "SynonymsNER Req: %+v", req)
	corpID := pkg.CorpID(ctx)
	robotID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, robotID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	nerReq := s.getNERReq(corpID, app.ID, req)
	rsp, err := s.dao.GetSynonymsNER(ctx, nerReq)
	if err != nil {
		return nil, err
	}
	log.InfoContextf(ctx, "SynonymsNER original query: %s, Rsp: %+v", req.Query, rsp)

	return &pb.SynonymsNERRsp{
		ReplacedQuery: rsp.ReplacedQuery,
		NerInfo:       s.getNerInfo(rsp.NERInfos),
	}, nil
}

func (s *Service) getNERReq(corpID uint64, robotID uint64,
	req *pb.SynonymsNERReq) *model.SynonymsNERReq {
	return &model.SynonymsNERReq{
		CorpID:  corpID,
		RobotID: robotID,
		Query:   req.GetQuery(),
		Scenes:  req.GetScenes(),
	}
}

func (s *Service) getNerInfo(nerInfos []*model.NerInfo) []*pb.SynonymsNERRsp_NERInfo {
	nerInfo := make([]*pb.SynonymsNERRsp_NERInfo, 0, len(nerInfos))
	for _, info := range nerInfos {
		nerInfo = append(nerInfo, &pb.SynonymsNERRsp_NERInfo{
			NumTokens:    uint32(info.NumTokens),
			OriginalText: info.OriginalText,
			RefValue:     info.RefValue,
		})
	}
	return nerInfo
}
