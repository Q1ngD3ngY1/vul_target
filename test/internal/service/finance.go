package service

import (
	"context"
	"strconv"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// GetCharacterUsage 获取字符使用量与容量
func (s *Service) GetCharacterUsage(ctx context.Context, req *pb.GetCharacterUsageReq) (*pb.GetCharacterUsageRsp, error) {
	var err error
	rsp := &pb.GetCharacterUsageRsp{}
	corpID := contextx.Metadata(ctx).CorpID()
	knowledgeBaseID := uint64(0)
	if req.GetKnowledgeBaseId() != "" {
		knowledgeBaseID, err = strconv.ParseUint(req.GetKnowledgeBaseId(), 10, 64)
		if err != nil {
			logx.E(ctx, "GetCharacterUsage knowledgeBaseID ParseUint err: %+v", err)
			return rsp, err
		}
	}
	return s.kbLogic.GetKnowledgeBaseUsage(ctx, corpID, knowledgeBaseID)
}
