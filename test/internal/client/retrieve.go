package client

import (
	"context"
	"errors"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/go-comm/clues"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

// SearchVector 向量搜索
func SearchVector(ctx context.Context, req *pb.SearchVectorReq) (*pb.SearchVectorRsp, error) {
	if req.BotBizId == 0 {
		err := errors.New("向量搜索 BotBizId为空")
		log.ErrorContextf(ctx, "err:%+v req:%+v", err, req)
		return nil, err
	}
	beginTimestamp := time.Now()
	rsp, err := directIndexCli.SearchVector(ctx, req)
	clues.AddTrack4RPC(ctx, "directIndexCli.SearchVector", req, rsp, err, beginTimestamp)
	log.DebugContextf(ctx, "向量搜索 req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil {
		log.ErrorContextf(ctx, "向量搜索失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// SearchMultiKnowledgePreview 向量搜索-支持同时检索多个知识库
func SearchMultiKnowledgePreview(ctx context.Context, req *pb.SearchMultiKnowledgeReq) (*pb.SearchVectorRsp, error) {
	if req.BotBizId == 0 {
		err := errors.New("向量搜索 BotBizId为空")
		log.ErrorContextf(ctx, "err:%+v req:%+v", err, req)
		return nil, err
	}
	beginTimestamp := time.Now()
	rsp, err := directIndexCli.SearchMultiKnowledgePreview(ctx, req)
	clues.AddTrack4RPC(ctx, "directIndexCli.SearchMultiKnowledge", req, rsp, err, beginTimestamp)
	log.DebugContextf(ctx, "向量搜索 req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil {
		log.ErrorContextf(ctx, "向量搜索失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// SearchMultiKnowledgeRelease 向量搜索-支持同时检索多个知识库
func SearchMultiKnowledgeRelease(ctx context.Context, req *pb.SearchMultiKnowledgeReq) (*pb.SearchRsp, error) {
	if req.BotBizId == 0 {
		err := errors.New("向量搜索 BotBizId为空")
		log.ErrorContextf(ctx, "err:%+v req:%+v", err, req)
		return nil, err
	}
	beginTimestamp := time.Now()
	rsp, err := retrievalCli.SearchMultiKnowledgeRelease(ctx, req)
	clues.AddTrack4RPC(ctx, "directIndexCli.SearchMultiKnowledge", req, rsp, err, beginTimestamp)
	log.DebugContextf(ctx, "向量搜索 req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil {
		log.ErrorContextf(ctx, "向量搜索失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// UpdateVectorLabel 更新标签
func UpdateVectorLabel(ctx context.Context, req *pb.UpdateLabelReq) (*pb.UpdateLabelRsp, error) {
	rsp, err := directIndexCli.UpdateLabel(ctx, req)
	log.InfoContextf(ctx, "UpdateVectorLabel req:%+v", req)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateVectorLabel err:%v,req:%+v", err, req)
		return nil, err
	}
	return rsp, nil
}
