package rpc

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	webParserServer "git.woa.com/dialogue-platform/proto/pb-stub/web-parser-server"
)

type WebParserRPC interface {
	FetchURLContent(ctx context.Context, fetchReq *webParserServer.FetchURLContentReq) (*webParserServer.FetchURLContentRsp, error)
	GetDocUpdateFrequency(ctx context.Context, req *webParserServer.GetDocUpdateFrequencyReq) (*webParserServer.GetDocUpdateFrequencyRsp, error)
	GetWebTaskMetaData(ctx context.Context, req *webParserServer.GetWebTaskMetaDataReq) (*webParserServer.GetWebTaskMetaDataRsp, error)
}

func (r *RPC) FetchURLContent(ctx context.Context, fetchReq *webParserServer.FetchURLContentReq) (*webParserServer.FetchURLContentRsp, error) {
	rsp, err := r.webParserCli.FetchURLContent(ctx, fetchReq)
	if err != nil {
		logx.E(ctx, "FetchURLContent req:%+v err:%+v", fetchReq, err)
		return nil, err
	}
	return rsp, nil
}

func (r *RPC) GetDocUpdateFrequency(ctx context.Context, req *webParserServer.GetDocUpdateFrequencyReq) (*webParserServer.GetDocUpdateFrequencyRsp, error) {
	rsp, err := r.webParserCli.GetDocUpdateFrequency(ctx, req)
	if err != nil {
		logx.E(ctx, "GetDocUpdateFrequency req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

func (r *RPC) GetWebTaskMetaData(ctx context.Context, req *webParserServer.GetWebTaskMetaDataReq) (
	*webParserServer.GetWebTaskMetaDataRsp, error) {
	rsp, err := r.webParserCli.GetWebTaskMetaData(ctx, req)
	if err != nil {
		logx.E(ctx, "GetWebTaskMetaData req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}
