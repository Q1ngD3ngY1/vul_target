package rpc

import (
	"context"
	"fmt"
	"net/url"

	"git.woa.com/adp/common/x/logx"
	shortURL "git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/short_url"
)

type ShortURLRPC interface {
	AddShortURL(ctx context.Context, name string, url string) (string, error)
	ShortURLToCosPath(ctx context.Context, code string) (string, error)
}

// AddShortURL 生成短链接
func (r *RPC) AddShortURL(ctx context.Context, name string, url string) (string, error) {
	req := &shortURL.AddReq{
		Urls: []*shortURL.URL{
			{
				Name: name,
				Url:  url,
			},
		},
	}
	logx.I(ctx, "AddShortURL|req:%+v", req)
	rsp, err := r.shortURLAdmin.Add(ctx, req)
	if err != nil {
		logx.E(ctx, "创建短链失败, name: %s, url: %s, err: %v", name, url, err)
		return "", err
	}
	logx.I(ctx, "AddShortURL|rsp:%+v", rsp)
	return rsp.GetCodes()[0], nil
}

// ShortURLToCosPath 获取短链接对应的cos path
func (r *RPC) ShortURLToCosPath(ctx context.Context, code string) (string, error) {
	req := &shortURL.GetReq{
		Code: code,
	}
	logx.I(ctx, "ShortURLToCosPath|req:%+v", req)
	rsp, err := r.shortURLAdmin.Get(ctx, req)
	if err != nil {
		logx.E(ctx, "获取短链接对应的cos path失败|code:%s|err:%+v", code, err)
		return "", err
	}
	logx.I(ctx, "ShortURLToCosPath|rsp:%+v", rsp)
	if rsp == nil {
		logx.E(ctx, "ShortURLToCosPath rsp is nil|code:%s", code)
		return "", fmt.Errorf("rsp is nil")
	}
	cosPath, err := url.Parse(rsp.Url)
	if err != nil {
		logx.E(ctx, "ShortURLToCosPath Parse|code:%s|err:%+v", code, err)
		return "", err
	}
	return cosPath.Path, nil
}
