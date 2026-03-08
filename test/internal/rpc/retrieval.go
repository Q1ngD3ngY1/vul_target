package rpc

import (
	"context"
	"errors"
	"fmt"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/kb/kb-config/internal/entity"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/metrics"
	"git.woa.com/adp/common/x/logx"
	pb "git.woa.com/adp/pb-go/kb/kb_retrieval"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

type RetrievalRPC interface {
	Publish(ctx context.Context, req *pb.PublishReq, opts ...client.Option) (rsp *pb.PublishRsp, err error)
	SearchMultiKnowledgeRelease(context.Context, *pb.SearchMultiKnowledgeReq) (*pb.SearchRsp, error)
	UpgradeEmbedding(context.Context, *pb.UpgradeEmbeddingReq) (*pb.UpgradeEmbeddingRsp, error)
	BatchDeleteAllKnowledgeProd(context.Context, *pb.BatchDeleteAllKnowledgeProdReq) (*pb.BatchDeleteAllKnowledgeProdRsp, error)
	CreateVersion(ctx context.Context, req *retrieval.CreateVersionReq) (*retrieval.CreateVersionRsp, error)
	PublishText2SQL(ctx context.Context, req *retrieval.PublishText2SQLReq) (*retrieval.PublishText2SQLRsp, error)
	BatchDeleteKnowledgeProd(ctx context.Context, req *retrieval.BatchDeleteKnowledgeProdReq) (*retrieval.BatchDeleteKnowledgeProdRsp, error)
	BatchUpsertKnowledgeProd(ctx context.Context, req *retrieval.BatchUpsertKnowledgeProdReq) (*retrieval.BatchUpsertKnowledgeProdRsp, error)
}

type RetrievalDirectIndexRPC interface {
	SearchMultiKnowledgePreview(context.Context, *pb.SearchMultiKnowledgeReq) (*pb.SearchVectorRsp, error)
	UpdateVectorLabel(context.Context, *pb.UpdateLabelReq) (*pb.UpdateLabelRsp, error)
	DeleteIndex(context.Context, *pb.DeleteIndexReq) (*pb.DeleteIndexRsp, error)
	CreateIndex(context.Context, *pb.CreateIndexReq) (*pb.CreateIndexRsp, error)
	RecoverBigDataElastic(context.Context, *pb.RecoverBigDataElasticReq) (*pb.RecoverBigDataElasticRsp, error)
	BatchDeleteKnowledge(context.Context, *pb.BatchDeleteKnowledgeReq) error
	BatchGetBigDataESByRobotBigDataID(context.Context, *pb.BatchGetBigDataESByRobotBigDataIDReq) (*pb.BatchGetBigDataESByRobotBigDataIDResp, error)
	AddRealTimeKnowledge(context.Context, *pb.AddRealTimeKnowledgeReq) (*pb.AddRealTimeKnowledgeRsp, error)
	DeleteRealTimeKnowledge(context.Context, *pb.DeleteRealTimeKnowledgeReq) (*pb.DeleteRealTimeKnowledgeRsp, error)
	RetrievalRealTime(context.Context, *pb.RetrievalRealTimeReq) (*pb.RetrievalRealTimeRsp, error)
	AddBigDataElastic(context.Context, *pb.AddBigDataElasticReq) error
	DeleteBigDataElastic(context.Context, *pb.DeleteBigDataElasticReq) error
	BatchAddKnowledge(context.Context, *pb.BatchAddKnowledgeReq) (*pb.BatchAddKnowledgeRsp, error)
	AddText2SQL(context.Context, *pb.AddText2SQLReq) (*pb.AddText2SQLRsp, error)
	DeleteText2SQL(context.Context, *pb.DeleteText2SQLReq) error
	GenerateSQL(context.Context, *pb.GenerateSQLReq) (*pb.GenerateSQLRsp, error)
	DeleteDBText2SQL(ctx context.Context, robotId uint64, dbTableBizIDs []uint64, envType pb.EnvType) error
	BatchDeleteKnowledgeProd(ctx context.Context, req *retrieval.BatchDeleteKnowledgeProdReq) (*retrieval.BatchDeleteKnowledgeProdRsp, error)
}

func (r *RPC) Publish(ctx context.Context, req *pb.PublishReq, opts ...client.Option) (
	rsp *pb.PublishRsp, err error) {
	return r.retrieval.Publish(ctx, req, opts...)
}

// SearchMultiKnowledgePreview 向量搜索-支持同时检索多个知识库
func (r *RPC) SearchMultiKnowledgePreview(ctx context.Context, req *pb.SearchMultiKnowledgeReq) (*pb.SearchVectorRsp, error) {
	if req.BotBizId == 0 {
		err := errors.New("向量搜索 BotBizId为空")
		logx.E(ctx, "err:%+v req:%+v", err, req)
		return nil, err
	}
	rsp, err := r.directIndex.SearchMultiKnowledgePreview(ctx, req)
	logx.D(ctx, "向量搜索 req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil {
		logx.E(ctx, "向量搜索失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// SearchMultiKnowledgeRelease 向量搜索-支持同时检索多个知识库
func (r *RPC) SearchMultiKnowledgeRelease(ctx context.Context, req *pb.SearchMultiKnowledgeReq) (*pb.SearchRsp, error) {
	if req.BotBizId == 0 {
		err := errors.New("向量搜索 BotBizId为空")
		logx.E(ctx, "err:%+v req:%+v", err, req)
		return nil, err
	}
	rsp, err := r.retrieval.SearchMultiKnowledgeRelease(ctx, req)
	logx.D(ctx, "向量搜索 req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil {
		logx.E(ctx, "向量搜索失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// CreateVersion 创建版本
func (r *RPC) CreateVersion(ctx context.Context, req *retrieval.CreateVersionReq) (*retrieval.CreateVersionRsp, error) {
	logx.I(ctx, "CreateVersion req:%+v", req)
	rsp, err := r.retrieval.CreateVersion(ctx, req)
	if err != nil {
		logx.E(ctx, "CreateVersion req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// PublishText2SQL 发布text2sql版本
func (r *RPC) PublishText2SQL(ctx context.Context, req *retrieval.PublishText2SQLReq) (*retrieval.PublishText2SQLRsp, error) {
	logx.I(ctx, "PublishText2SQL req:%+v", req)
	rsp, err := r.retrieval.PublishText2SQL(ctx, req)
	if err != nil {
		logx.E(ctx, "PublishText2SQL req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// BatchDeleteKnowledgeProd  批量删除知识
func (r *RPC) BatchDeleteKnowledgeProd(ctx context.Context, req *retrieval.BatchDeleteKnowledgeProdReq) (*retrieval.BatchDeleteKnowledgeProdRsp, error) {
	logx.I(ctx, "BatchDeleteKnowledgeProd req:%s", jsonx.MustMarshalToString(req))
	rsp, err := r.retrieval.BatchDeleteKnowledgeProd(ctx, req)
	if err != nil {
		logx.E(ctx, "BatchDeleteKnowledgeProd req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// BatchUpsertKnowledgeProd  批量创建/更新知识
func (r *RPC) BatchUpsertKnowledgeProd(ctx context.Context, req *retrieval.BatchUpsertKnowledgeProdReq) (*retrieval.BatchUpsertKnowledgeProdRsp, error) {
	logx.I(ctx, "BatchUpsertKnowledgeProd req:%s", jsonx.MustMarshalToString(req))
	rsp, err := r.retrieval.BatchUpsertKnowledgeProd(ctx, req)
	if err != nil {
		logx.E(ctx, "BatchUpsertKnowledgeProd req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// UpdateVectorLabel 更新标签
func (r *RPC) UpdateVectorLabel(ctx context.Context, req *pb.UpdateLabelReq) (*pb.UpdateLabelRsp, error) {
	rsp, err := r.directIndex.UpdateLabel(ctx, req)
	logx.I(ctx, "UpdateVectorLabel req:%+v", req)
	if err != nil {
		logx.E(ctx, "UpdateVectorLabel err:%v,req:%+v", err, req)
		return nil, err
	}
	return rsp, nil
}

// DeleteIndex 删除检索库
func (r *RPC) DeleteIndex(ctx context.Context, req *pb.DeleteIndexReq) (*pb.DeleteIndexRsp, error) {
	return r.directIndex.DeleteIndex(ctx, req)
}

// CreateIndex 新增向量库
func (r *RPC) CreateIndex(ctx context.Context, req *pb.CreateIndexReq) (*pb.CreateIndexRsp, error) {
	return r.directIndex.CreateIndex(ctx, req)
}

// RecoverBigDataElastic 从ES恢复离线知识库的BigData
func (r *RPC) RecoverBigDataElastic(ctx context.Context, req *pb.RecoverBigDataElasticReq) (*pb.RecoverBigDataElasticRsp, error) {
	return r.directIndex.RecoverBigDataElastic(ctx, req)
}

// BatchDeleteKnowledge  批量删除知识 -- 替换DeleteKnowledge接口
func (r *RPC) BatchDeleteKnowledge(ctx context.Context, req *pb.BatchDeleteKnowledgeReq) error {
	if req.EmbeddingModelName != "" {
		req.EmbeddingVersion = entity.GetEmbeddingVersion(req.EmbeddingModelName)
	}
	_, err := r.directIndex.BatchDeleteKnowledge(ctx, req)
	if err != nil {
		return fmt.Errorf("BatchDeleteKnowledge err:%v,req:%+v", err, req)
	}
	return nil
}

func (r *RPC) BatchGetBigDataESByRobotBigDataID(ctx context.Context, req *pb.BatchGetBigDataESByRobotBigDataIDReq) (*pb.BatchGetBigDataESByRobotBigDataIDResp, error) {
	logx.I(ctx, "BatchGetBigDataESByRobotBigDataID|robotID:%s, bigDataIDs:%v", req.GetRobotId(), len(req.GetBigDataIds()))
	rsp, err := r.directIndex.BatchGetBigDataESByRobotBigDataID(ctx, req)
	if err != nil {
		metrics.IncrCounter("BatchGetBigDataESByRobotBigDataID.err", 1)
		return nil, err
	}
	metrics.IncrCounter("BatchGetBigDataESByRobotBigDataID.success", 1)
	logx.I(ctx, "BatchGetBigDataESByRobotBigDataID|bigData.len=:%d|ok", len(rsp.Data))
	return rsp, nil
}

func (r *RPC) AddRealTimeKnowledge(ctx context.Context, req *pb.AddRealTimeKnowledgeReq) (*pb.AddRealTimeKnowledgeRsp, error) {
	return r.directIndex.AddRealTimeKnowledge(ctx, req)
}

func (r *RPC) DeleteRealTimeKnowledge(ctx context.Context, req *pb.DeleteRealTimeKnowledgeReq) (*pb.DeleteRealTimeKnowledgeRsp, error) {
	return r.directIndex.DeleteRealTimeKnowledge(ctx, req)
}

func (r *RPC) RetrievalRealTime(ctx context.Context, req *pb.RetrievalRealTimeReq) (*pb.RetrievalRealTimeRsp, error) {
	return r.directIndex.RetrievalRealTime(ctx, req)
}

func (r *RPC) AddBigDataElastic(ctx context.Context, req *pb.AddBigDataElasticReq) error {
	logx.I(ctx, "AddBigDataElastic|bigData.len:%d", len(req.Data))
	if len(req.Data) == 0 {
		return nil
	}
	_, err := r.directIndex.AddBigDataElastic(ctx, req)
	if err != nil {
		metrics.IncrCounter("AddBigDataElastic.err", 1)
		return err
	}
	metrics.IncrCounter("AddBigDataElastic.success", 1)
	logx.I(ctx, "AddBigDataElastic|bigData.len=:%d|ok", len(req.Data))
	return nil
}

func (r *RPC) DeleteBigDataElastic(ctx context.Context, req *pb.DeleteBigDataElasticReq) error {
	_, err := r.directIndex.DeleteBigDataElastic(ctx, req)
	return err
}

func (r *RPC) BatchAddKnowledge(ctx context.Context, req *pb.BatchAddKnowledgeReq) (*pb.BatchAddKnowledgeRsp, error) {
	if req.EmbeddingModelName != "" {
		req.EmbeddingVersion = entity.GetEmbeddingVersion(req.EmbeddingModelName)
	}
	return r.directIndex.BatchAddKnowledge(ctx, req)
}

func (r *RPC) AddText2SQL(ctx context.Context, req *pb.AddText2SQLReq) (*pb.AddText2SQLRsp, error) {
	return r.directIndex.AddText2SQL(ctx, req)
}

func (r *RPC) DeleteText2SQL(ctx context.Context, req *pb.DeleteText2SQLReq) error {
	_, err := r.directIndex.DeleteText2SQL(ctx, req)
	return err
}

func (r *RPC) GenerateSQL(ctx context.Context, req *pb.GenerateSQLReq) (*pb.GenerateSQLRsp, error) {
	return r.directIndex.GenerateSQL(ctx, req)
}

func (r *RPC) UpgradeEmbedding(ctx context.Context, req *pb.UpgradeEmbeddingReq) (*pb.UpgradeEmbeddingRsp, error) {
	return r.retrieval.UpgradeEmbedding(ctx, req)
}

func (r *RPC) BatchDeleteAllKnowledgeProd(ctx context.Context, req *pb.BatchDeleteAllKnowledgeProdReq) (*pb.BatchDeleteAllKnowledgeProdRsp, error) {
	return r.retrieval.BatchDeleteAllKnowledgeProd(ctx, req)
}

func (r *RPC) DeleteDBText2SQL(ctx context.Context, robotId uint64, dbTableBizIDs []uint64, envType pb.EnvType) error {
	_, err := r.directIndex.DeleteDBText2SQL(ctx, &pb.DeleteDBText2SQLReq{
		RobotId:      robotId,
		DbTableBizId: dbTableBizIDs,
		EnvType:      envType,
	})
	if err != nil {
		logx.E(ctx, "DeleteDBText2SQL table: %+v, env %v", dbTableBizIDs, envType.String())
		return err
	}
	return nil
}
