package api

import (
	"context"

	"git.woa.com/adp/common/x/gox/slicex"

	appConfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func NERInfoAppConfigPB2ServerPB(nerInfo *appConfig.SynonymsNERRsp_NERInfo) *pb.SynonymsNERRsp_NERInfo {
	return &pb.SynonymsNERRsp_NERInfo{
		NumTokens:    nerInfo.GetNumTokens(),
		OriginalText: nerInfo.GetOriginalText(),
		RefValue:     nerInfo.GetRefValue(),
	}
}

func NERInfoAppConfigsPB2ServerPB(nerInfos []*appConfig.SynonymsNERRsp_NERInfo) []*pb.SynonymsNERRsp_NERInfo {
	return slicex.Map(nerInfos, func(nerInfo *appConfig.SynonymsNERRsp_NERInfo) *pb.SynonymsNERRsp_NERInfo {
		return NERInfoAppConfigPB2ServerPB(nerInfo)
	})
}

// SynonymsNER 同义词 NER(命名实体识别) 接口
func (s *Service) SynonymsNER(ctx context.Context, req *pb.SynonymsNERReq) (*pb.SynonymsNERRsp, error) {
	newReq := appConfig.SynonymsNERReq{
		BotBizId: req.GetBotBizId(),
		Query:    req.GetQuery(),
		Scenes:   req.GetScenes(),
	}
	newRsp, err := s.rpc.AppAdmin.SynonymsNER(ctx, &newReq)
	if newRsp != nil {
		return &pb.SynonymsNERRsp{
			ReplacedQuery: newRsp.GetReplacedQuery(),
			NerInfo:       NERInfoAppConfigsPB2ServerPB(newRsp.GetNerInfo()),
		}, err
	}
	return nil, err
}
