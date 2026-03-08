package api

import (
	"context"

	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ListRejectedQuestion 获取拒答问题列表
func (s *Service) ListRejectedQuestion(ctx context.Context, req *pb.ListRejectedQuestionReq) (*pb.ListRejectedQuestionRsp, error) {
	return s.svc.ListRejectedQuestion(ctx, req)
}
