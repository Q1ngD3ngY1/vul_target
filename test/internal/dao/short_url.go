package dao

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	shortURL "git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/short_url"
	"net/url"
)

// AddShortURL 生成短链接
func (d *dao) AddShortURL(ctx context.Context, name string, url string) (string, error) {
	req := &shortURL.AddReq{
		Urls: []*shortURL.URL{
			{
				Name: name,
				Url:  url,
			},
		},
	}
	log.InfoContextf(ctx, "AddShortURL|req:%+v", req)
	rsp, err := d.shortURLCli.Add(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "创建短链失败, name: %s, url: %s, err: %v", name, url, err)
		return "", err
	}
	log.InfoContextf(ctx, "AddShortURL|rsp:%+v", rsp)
	return rsp.GetCodes()[0], nil
}

// ShortURLToCosPath 获取短链接对应的cos path
func (d *dao) ShortURLToCosPath(ctx context.Context, code string) (string, error) {
	req := &shortURL.GetReq{
		Code: code,
	}
	log.InfoContextf(ctx, "ShortURLToCosPath|req:%+v", req)
	rsp, err := d.shortURLCli.Get(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "获取短链接对应的cos path失败|code:%s|err:%+v", code, err)
		return "", err
	}
	log.InfoContextf(ctx, "ShortURLToCosPath|rsp:%+v", rsp)
	if rsp == nil {
		log.ErrorContextf(ctx, "ShortURLToCosPath rsp is nil|code:%s", code)
		return "", fmt.Errorf("rsp is nil")
	}
	cosPath, err := url.Parse(rsp.Url)
	if err != nil {
		log.ErrorContextf(ctx, "ShortURLToCosPath Parse|code:%s|err:%+v", code, err)
		return "", err
	}
	return cosPath.Path, nil
}
