package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	secapi "git.woa.com/sec-api/go/scurl"

	thirdDocEntity "git.woa.com/adp/kb/kb-config/internal/entity/third_doc"
	"git.woa.com/adp/kb/kb-config/internal/util"
)

type ThirdDocRpc interface {
	ListOnedriveDoc(ctx context.Context, opts ...thirdDocEntity.ListDocOption) (*thirdDocEntity.CommonDocListResult, error)
}

func convertCommonDocListResultPO2DO(po *thirdDocEntity.TThirdOnedriveDocList) *thirdDocEntity.CommonDocListResult {
	const (
		nameSplitTag = "."
		systemUser   = "System User"
		vaultFolder  = "Personal Vault"
	)
	do := &thirdDocEntity.CommonDocListResult{
		Docs: make([]*thirdDocEntity.CommonDocInfo, 0, len(po.Values)),
	}
	for _, item := range po.Values {
		isFolder := false
		if item.Folder != nil {
			isFolder = true
		}
		var mimeType string
		// 过滤个人文档库
		if item.Name == vaultFolder && item.CreatedBy.User.DisplayName == systemUser {
			continue
		}

		if !isFolder {
			mimeType = util.GetFileExt(item.Name)
		}
		do.Docs = append(do.Docs, &thirdDocEntity.CommonDocInfo{
			ID:               item.ID,
			Name:             item.Name,
			Size:             item.Size,
			MimeType:         mimeType,
			IsFolder:         isFolder,
			CreatedBy:        item.CreatedBy.User.DisplayName,
			DownloadURL:      item.DownloadURL,
			LastModifiedTime: item.LastModifiedTime,
		})
	}
	return do
}

func (d *RPC) ListOnedriveDoc(ctx context.Context, opts ...thirdDocEntity.ListDocOption) (*thirdDocEntity.CommonDocListResult, error) {

	const (
		onedriveFetchURL        = "https://graph.microsoft.com/v1.0/me/drive/items/%s/children"
		onedriveSearchURL       = "https://graph.microsoft.com/v1.0/me/drive/items/%s/search(q='%s')"
		onedriveDescribeItemURL = "https://graph.microsoft.com/v1.0/me/drive/items/%s"
	)

	if len(opts) == 0 {
		return nil, fmt.Errorf("missing list options")
	}
	opt := &thirdDocEntity.ListDocOptions{}
	for _, optItem := range opts {
		optItem(opt)
	}
	folderID := opt.FolderID
	// 使用默认文件夹（如果未指定）
	if len(folderID) == 0 {
		folderID = thirdDocEntity.DefaultFolder
	}
	fetchURL := fmt.Sprintf(onedriveFetchURL, folderID)
	switch {
	case len(opt.ItemID) > 0:
		fetchURL = fmt.Sprintf(onedriveDescribeItemURL, opt.ItemID)
	case len(opt.NextLink) > 0:
		// TODO: 出于安全考虑，这里需要从 link 中提取 token/folder/top 部分, 然后拼成新的 url
		fetchURL = opt.NextLink
	case len(opt.Keyword) > 0:
		fetchURL = fmt.Sprintf(onedriveSearchURL, folderID, opt.Keyword)
	}

	if len(opt.Extra) > 0 {
		fetchURL = fmt.Sprintf("%s?", fetchURL)
	}
	idx := 0
	for ext, v := range opt.Extra {
		if idx == 0 {
			fetchURL = fmt.Sprintf("%s%s=", fetchURL, ext)
		} else {
			fetchURL = fmt.Sprintf("%s&%s=", fetchURL, ext)
		}
		for vIdx, vItem := range v {
			if vIdx == 0 {
				fetchURL = fmt.Sprintf("%s%s", fetchURL, vItem)
			} else {
				fetchURL = fmt.Sprintf("%s,%s", fetchURL, vItem)
			}
		}
		idx++
	}

	// 构建请求
	safeClient := secapi.NewSafeClient()
	httpReq, err := http.NewRequest("GET", fetchURL, nil)
	logx.DebugContextf(ctx, "fetch url is %s", fetchURL)
	if err != nil {
		logx.E(ctx, "http.NewRequest fail: url(%s), err(%v)", fetchURL, err)
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+opt.AccessToken)
	httpReq.Header.Set("Accept", "application/json")

	// 发送请求
	httpRsp, err := safeClient.Do(httpReq)
	if err != nil {
		logx.E(ctx, "safeClient.Do fail: url(%s), err(%v)", fetchURL, err)
		return nil, errx.ErrInternalServerError
	}
	defer httpRsp.Body.Close()

	if httpRsp.StatusCode != http.StatusOK {
		logx.ErrorContextf(ctx, "fetch onedrive content failed: url:%s statusCode:%d", fetchURL, httpRsp.StatusCode)
		return nil, errx.ErrInternalServerError
	}

	// 解析 OneDrive 特有的响应结构
	onedriveResult := &thirdDocEntity.TThirdOnedriveDocList{}
	itemResult := &thirdDocEntity.TThirdOnedriveDocInfo{}
	if len(opt.ItemID) > 0 {
		if err := json.NewDecoder(httpRsp.Body).Decode(&itemResult); err != nil {
			logx.ErrorContextf(ctx, "Decode response body failed: %v", err)
			return nil, errx.ErrInternalServerError
		}
		logx.DebugContextf(ctx, "fetch onedrive content: %+v", itemResult)
		onedriveResult.Values = []*thirdDocEntity.TThirdOnedriveDocInfo{itemResult}
		return convertCommonDocListResultPO2DO(onedriveResult), nil
	}

	if err := json.NewDecoder(httpRsp.Body).Decode(&onedriveResult); err != nil {
		logx.ErrorContextf(ctx, "Decode response body failed: %v", err)
		return nil, errx.ErrInternalServerError
	}
	logx.DebugContextf(ctx, "fetch onedrive content: %+v", onedriveResult)
	// 转换为通用结构
	return convertCommonDocListResultPO2DO(onedriveResult), nil
}
