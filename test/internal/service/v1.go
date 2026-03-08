package service

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetCredential 获取临时密钥
func (s *Service) GetCredential(ctx context.Context, _ *pb.GetCredentialReq) (*pb.GetCredentialRsp, error) {
	log.ErrorContext(ctx, "准备删除的接口收到了请求 deprecated interface req:")
	rsp := new(pb.GetCredentialRsp)
	return rsp, nil
}

// ListDocV1 文档列表
func (s *Service) ListDocV1(ctx context.Context, req *pb.ListDocV1Req) (*pb.ListDocV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ListDocV1Rsp)
	return rsp, nil
}

// SaveDocV1 保存文档
func (s *Service) SaveDocV1(ctx context.Context, req *pb.SaveDocV1Req) (*pb.SaveDocV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.SaveDocV1Rsp)
	return rsp, nil
}

// ModifyDocV1 修改文档
func (s *Service) ModifyDocV1(ctx context.Context, req *pb.ModifyDocV1Req) (*pb.ModifyDocV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ModifyDocV1Rsp)
	return rsp, nil
}

// StartCreateQA 开始/重新生成QA
func (s *Service) StartCreateQA(ctx context.Context, req *pb.StartCreateQAReq) (*pb.StartCreateQARsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.StartCreateQARsp)
	return rsp, nil
}

// GetQAList 问答对列表
func (s *Service) GetQAList(ctx context.Context, req *pb.GetQAListReq) (*pb.GetQAListRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetQAListRsp)
	return rsp, nil
}

// CreateQAV1 创建QA
func (s *Service) CreateQAV1(ctx context.Context, req *pb.CreateQAV1Req) (*pb.CreateQAV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CreateQAV1Rsp)
	return rsp, nil
}

// UpdateQA 更新QA
func (s *Service) UpdateQA(ctx context.Context, req *pb.UpdateQAReq) (*pb.UpdateQARsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.UpdateQARsp)
	return rsp, nil
}

// DeleteQAV1 删除QA
func (s *Service) DeleteQAV1(ctx context.Context, req *pb.DeleteQAV1Req) (*pb.DeleteQAV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.DeleteQAV1Rsp)
	return rsp, nil
}

// VerifyQAV1 验证QA
func (s *Service) VerifyQAV1(ctx context.Context, req *pb.VerifyQAV1Req) (*pb.VerifyQAV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.VerifyQAV1Rsp)
	return rsp, nil
}

// ListQACateV1 获取问答分类列表
func (s *Service) ListQACateV1(ctx context.Context, req *pb.ListQACateV1Req) (*pb.ListQACateV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.ListQACateV1Rsp{}
	return rsp, nil
}

// CreateQACateV1 创建问答分类
func (s *Service) CreateQACateV1(ctx context.Context, req *pb.CreateQACateV1Req) (*pb.CreateQACateV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.CreateQACateV1Rsp{}
	return rsp, nil
}

// GetQaSimilar 获取相似问答对
func (s *Service) GetQaSimilar(ctx context.Context, req *pb.GetQaSimilarReq) (*pb.GetQaSimilarRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.GetQaSimilarRsp{}
	return rsp, nil
}

// GetQaSimilarDetail 获取相似问答对
func (s *Service) GetQaSimilarDetail(ctx context.Context, req *pb.GetQaSimilarDetailReq) (
	*pb.GetQaSimilarDetailRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.GetQaSimilarDetailRsp{}
	return rsp, nil
}

// CreateAttributeLabelV1 创建属性标签
func (s *Service) CreateAttributeLabelV1(ctx context.Context, req *pb.CreateAttributeLabelV1Req) (
	*pb.CreateAttributeLabelV1Rsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.CreateAttributeLabelV1Rsp{}
	return rsp, nil
}

// UpdateQACate 更新问答分类
func (s *Service) UpdateQACate(ctx context.Context, req *pb.UpdateQACateReq) (*pb.UpdateQACateRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.UpdateQACateRsp{}
	return rsp, nil
}

// UpdateRejectedQuestion 修改拒答问题
func (s *Service) UpdateRejectedQuestion(ctx context.Context, req *pb.UpdateRejectedQuestionReq) (
	*pb.UpdateRejectedQuestionRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.UpdateRejectedQuestionRsp{}
	return rsp, nil
}

// ExportQAListV1 导出QA
func (s *Service) ExportQAListV1(ctx context.Context, req *pb.ExportQAListReqV1) (*pb.ExportQAListRspV1, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.ExportQAListRspV1{}
	return rsp, nil
}
