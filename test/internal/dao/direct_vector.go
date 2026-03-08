package dao

import (
	"context"

	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

// DirectSearchVector 向量搜索
func (d *dao) DirectSearchVector(
	ctx context.Context, req *retrieval.DirectSearchVectorReq,
) (*retrieval.DirectSearchVectorRsp, error) {
	return d.directIndexCli.DirectSearchVector(ctx, req)
}

// DirectAddVector 新增向量
func (d *dao) DirectAddVector(
	ctx context.Context, req *retrieval.DirectAddVectorReq,
) (*retrieval.DirectAddVectorRsp, error) {
	return d.directIndexCli.DirectAddVector(ctx, req)
}

// DirectUpdateVector 更新向量
func (d *dao) DirectUpdateVector(
	ctx context.Context, req *retrieval.DirectUpdateVectorReq,
) (*retrieval.DirectUpdateVectorRsp, error) {
	return d.directIndexCli.DirectUpdateVector(ctx, req)
}

// DirectDeleteVector 删除向量
func (d *dao) DirectDeleteVector(
	ctx context.Context, req *retrieval.DirectDeleteVectorReq,
) (*retrieval.DirectDeleteVectorRsp, error) {
	return d.directIndexCli.DirectDeleteVector(ctx, req)
}

// DirectCreateIndex 创建索引库
func (d *dao) DirectCreateIndex(
	ctx context.Context, req *retrieval.DirectCreateIndexReq,
) (*retrieval.DirectCreateIndexRsp, error) {
	return d.directIndexCli.DirectCreateIndex(ctx, req)
}

// DirectDeleteIndex 删除索引库
func (d *dao) DirectDeleteIndex(
	ctx context.Context, req *retrieval.DirectDeleteIndexReq,
) (*retrieval.DirectDeleteIndexRsp, error) {
	return d.directIndexCli.DirectDeleteIndex(ctx, req)
}

// AddVector 新增向量
func (d *dao) AddVector(
	ctx context.Context, req *retrieval.AddVectorReq,
) (*retrieval.AddVectorRsp, error) {
	return d.directIndexCli.AddVector(ctx, req)
}

// DeleteIndex 删除检索库
func (d *dao) DeleteIndex(
	ctx context.Context, req *retrieval.DeleteIndexReq,
) (*retrieval.DeleteIndexRsp, error) {
	return d.directIndexCli.DeleteIndex(ctx, req)
}

// CreateIndex 新增向量库
func (d *dao) CreateIndex(
	ctx context.Context, req *retrieval.CreateIndexReq,
) (*retrieval.CreateIndexRsp, error) {
	return d.directIndexCli.CreateIndex(ctx, req)
}

// RecoverBigDataElastic 从ES恢复离线知识库的BigData
func (d *dao) RecoverBigDataElastic(
	ctx context.Context, req *retrieval.RecoverBigDataElasticReq,
) (*retrieval.RecoverBigDataElasticRsp, error) {
	return d.directIndexCli.RecoverBigDataElastic(ctx, req)
}

// BatchDeleteKnowledge  批量删除知识 -- 替换DeleteKnowledge接口
func (d *dao) BatchDeleteKnowledge(
	ctx context.Context, req *retrieval.BatchDeleteKnowledgeReq,
) (*retrieval.BatchDeleteKnowledgeRsp, error) {
	return d.directIndexCli.BatchDeleteKnowledge(ctx, req)
}
