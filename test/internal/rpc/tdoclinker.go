package rpc

import (
	"context"
	"errors"

	"git.woa.com/adp/common/x/logx"
)

type TDocLinkerRPC interface {
	CheckUserAuth(ctx context.Context, loginUin, subAccountUin string) (*CheckUserAuthResponse, error)
	ImportTFile(ctx context.Context, loginUin, subAccountUin, fileID string) (string, error)
	ImportTFileProgress(ctx context.Context, loginUin, subAccountUin, fileID, operationID string, cosObj ImportTDocCosObj) (ImportTFileProgressResponse, error)
}

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

// CheckUserAuth 检查用户是否授权腾讯文档
func (r *RPC) CheckUserAuth(ctx context.Context, loginUin, subAccountUin string) (*CheckUserAuthResponse, error) {
	req := CheckUserAuthRequest{
		LoginUin:           loginUin,
		LoginSubAccountUin: subAccountUin,
	}
	var result CheckUserAuthResponse
	if err := r.tDocLinker.Post(ctx, "/tdapi/checkUserAuth", req, &result); err != nil {
		logx.E(ctx, "checkUserAuth error: %+v, req: %+v", err, req)
		return nil, err
	}
	return &result, nil
}

// ImportTFile 导入腾讯文档文件
func (r *RPC) ImportTFile(ctx context.Context, loginUin, subAccountUin, fileID string) (string, error) {
	req := ImportTFileRequest{
		LoginUin:           loginUin,
		LoginSubAccountUin: subAccountUin,
		FileID:             fileID,
	}
	var result ImportTFileResponse
	if err := r.tDocLinker.Post(ctx, "/tdapi/importTdocFile", req, &result); err != nil {
		logx.E(ctx, "ImportTFile error: %+v, req: %+v", err, req)
		return "", err
	}
	if result.Response.Code != 200 {
		logx.E(ctx, "ImportTFile err: %+v", result)
		return "", errors.New(result.Response.Msg)
	}
	logx.D(ctx, "ImportTFile response: %+v", result)
	return result.Response.Data.OperationID, nil
}

// ImportTFileProgress 查询导入文件操作进度
func (r *RPC) ImportTFileProgress(ctx context.Context, loginUin, subAccountUin, fileID, operationID string,
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
	if err := r.tDocLinker.Post(ctx, "/tdapi/importTdocFileProgress", req, &result); err != nil {
		logx.E(ctx, "ImportTFileProgress error: %+v, req: %+v", err, req)
		return result, err
	}
	if result.Response.Code != 200 {
		logx.E(ctx, "ImportTFileProgress err: %+v", result)
		return result, nil
	}
	logx.D(ctx, "ImportTFileProgress response: %+v", result)
	return result, nil
}
