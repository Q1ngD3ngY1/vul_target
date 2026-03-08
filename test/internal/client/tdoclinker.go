// bot-knowledge-config-server
//
// @(#)tdoclinker.go  星期二, 七月 15, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package client

import (
	"context"
	"errors"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.code.oa.com/trpc-go/trpc-go/naming/selector"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
)

// CheckUserAuthRequest 检查用户是否授权腾讯文档请求结构体
type CheckUserAuthRequest struct {
	LoginUin           string `json:"LoginUin"`
	LoginSubAccountUin string `json:"LoginSubAccountUin"`
}

// CheckUserAuthResponse 检查用户是否授权腾讯文档响应结构体
type CheckUserAuthResponse struct {
	Response struct {
		Code int `json:"Code"`
		Data struct {
			Avatar string `json:"Avatar"`
			Nick   string `json:"Nick"`
		} `json:"Data"`
		Msg       string `json:"Msg"`
		RequestId string `json:"RequestId"`
	} `json:"Response"`
}

// ImportTFileRequest 导入腾讯文档文件请求结构体
type ImportTFileRequest struct {
	LoginUin           string `json:"LoginUin"`
	LoginSubAccountUin string `json:"LoginSubAccountUin"`
	FileID             string `json:"FileId"`
}

// ImportTFileResponse 导入腾讯文档文件响应结构体
type ImportTFileResponse struct {
	Response struct {
		Code int `json:"Code"`
		Data struct {
			Msg         string `json:"Msg"`
			OperationID string `json:"OperationId"`
			Ret         int    `json:"Ret"`
		} `json:"Data"`
		Msg       string `json:"Msg"`
		RequestId string `json:"RequestId"`
	} `json:"Response"`
}

// ImportTFileProgressRequest 查询导入文件操作进度请求结构体
type ImportTFileProgressRequest struct {
	LoginUin           string           `json:"LoginUin"`
	LoginSubAccountUin string           `json:"LoginSubAccountUin"`
	FileID             string           `json:"FileId"`
	CosObj             ImportTDocCosObj `json:"CosObj"`
	OperationID        string           `json:"OperationId"`
}

type ImportTDocCosObj struct {
	Bucket        string `json:"Bucket"`
	Region        string `json:"Region"`
	CosPath       string `json:"CosPath"`
	TempSecretId  string `json:"TempSecretId"`
	TempSecretKey string `json:"TempSecretKey"`
	SecurityToken string `json:"SecurityToken"`
}

// ImportTFileProgressResponse 查询导入文件操作进度响应结构体
type ImportTFileProgressResponse struct {
	Response struct {
		Code int `json:"Code"`
		Data struct {
			Url      string `json:"Url"`
			Progress uint64 `json:"Progress"`
			ETag     string `json:"ETag"`
			CosHash  string `json:"CosHash"`
			Size     uint64 `json:"Size"`
		} `json:"Data"`
		Msg       string `json:"Msg"`
		RequestId string `json:"RequestId"`
	} `json:"Response"`
}

// WithTrpcSelector 还原为 trpc 默认 Selector
func WithTrpcSelector() client.Option {
	return func(o *client.Options) {
		o.Selector = &selector.TrpcSelector{}
	}
}

// CheckUserAuth 检查用户是否授权腾讯文档
func CheckUserAuth(ctx context.Context, loginUin, subAccountUin string) (*CheckUserAuthResponse, error) {
	req := CheckUserAuthRequest{
		LoginUin:           loginUin,
		LoginSubAccountUin: subAccountUin,
	}
	opts := []client.Option{WithTrpcSelector()}
	var result CheckUserAuthResponse
	if err := tDocLinkerCli.Post(ctx, "/tdapi/checkUserAuth", req, &result, opts...); err != nil {
		log.ErrorContextf(ctx, "checkUserAuth error: %+v, req: %+v", err, req)
		return nil, err
	}
	return &result, nil
}

// ImportTFile 导入腾讯文档文件
func ImportTFile(ctx context.Context, loginUin, subAccountUin, fileID string) (string, error) {
	req := ImportTFileRequest{
		LoginUin:           loginUin,
		LoginSubAccountUin: subAccountUin,
		FileID:             fileID,
	}
	opts := []client.Option{WithTrpcSelector()}
	var result ImportTFileResponse
	if err := tDocLinkerCli.Post(ctx, "/tdapi/importTdocFile", req, &result, opts...); err != nil {
		log.ErrorContextf(ctx, "ImportTFile error: %+v, req: %+v", err, req)
		return "", errs.ErrRefreshTxDocFail
	}
	if result.Response.Data.Ret == 9998 {
		log.InfoContextf(ctx, "ImportTFile 下载次数用尽 result: %+v", result)
		return "", errs.ErrTxDownloadDocFailed
	}
	if result.Response.Code != 200 {
		log.ErrorContextf(ctx, "ImportTFile err: %+v", result)
		return "", errors.New(result.Response.Msg)
	}
	log.DebugContextf(ctx, "ImportTFile response: %+v", result)
	return result.Response.Data.OperationID, nil
}

// ImportTFileProgress 查询导入文件操作进度
func ImportTFileProgress(ctx context.Context, loginUin, subAccountUin, fileID, operationID string,
	cosObj ImportTDocCosObj) (
	ImportTFileProgressResponse, error) {
	var result ImportTFileProgressResponse
	req := ImportTFileProgressRequest{
		LoginUin:           loginUin,
		LoginSubAccountUin: subAccountUin,
		FileID:             fileID,
		OperationID:        operationID,
		CosObj:             cosObj,
	}
	opts := []client.Option{WithTrpcSelector()}
	if err := tDocLinkerCli.Post(ctx, "/tdapi/importTdocFileProgress", req, &result, opts...); err != nil {
		log.ErrorContextf(ctx, "ImportTFileProgress error: %+v, req: %+v", err, req)
		return result, err
	}
	if result.Response.Code != 200 {
		log.ErrorContextf(ctx, "ImportTFileProgress err: %+v", result)
		return result, nil
	}
	log.DebugContextf(ctx, "ImportTFileProgress response: %+v", result)
	return result, nil
}
