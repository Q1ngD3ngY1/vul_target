package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	jsoniter "github.com/json-iterator/go"
	"io"
	"net/http"
	"time"
)

type CheckKnowledgePermissionReq struct {
	RequestId            string   `json:"RequestId"`
	CustomerKnowledgeIds []string `json:"CustomerKnowledgeIds"`
	CustomerUserId       string   `json:"CustomerUserId"`
}

type CheckKnowledgePermissionRsp struct {
	RequestId            string   `json:"RequestId"`
	CustomerKnowledgeIds []string `json:"CustomerKnowledgeIds"`
}

// CheckKnowledgePermission 调用第三方接口检查知识权限
func CheckKnowledgePermission(ctx context.Context, thirdPermissionConfig utilConfig.ThirdPermissionConfig,
	req *CheckKnowledgePermissionReq) (*CheckKnowledgePermissionRsp, error) {
	log.DebugContextf(ctx, "CheckThirdPermission CheckKnowledgePermission thirdPermissionConfig: %+v, req: %+v",
		thirdPermissionConfig, req)
	if thirdPermissionConfig.Retry == 0 {
		// 默认重试2次
		thirdPermissionConfig.Retry = 2
	}
	if thirdPermissionConfig.Timeout == 0 {
		// 默认超时1000ms
		thirdPermissionConfig.Timeout = 1000
	}
	// 将请求体序列化为JSON
	jsonBody, err := jsoniter.Marshal(req)
	if err != nil {
		log.ErrorContextf(ctx, "CheckThirdPermission CheckKnowledgePermission marshal request err: %+v", err)
		return nil, err
	}

	// 创建带超时的context
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(thirdPermissionConfig.Timeout)*time.Millisecond)
	defer cancel()

	// 重试逻辑
	var lastError error
	for i := 0; i < thirdPermissionConfig.Retry; i++ {
		// 创建新的请求
		httpReq, err := http.NewRequestWithContext(ctx, "POST", thirdPermissionConfig.Url, bytes.NewBuffer(jsonBody))
		if err != nil {
			log.ErrorContextf(ctx, "CheckThirdPermission CheckKnowledgePermission request err: %+v", err)
			time.Sleep(time.Second * time.Duration(i+1)) // 指数退避
			lastError = err
			continue
		}

		// 设置请求头
		httpReq.Header.Set("Content-Type", "application/json")
		for key, value := range thirdPermissionConfig.Header {
			httpReq.Header.Set(key, value)
		}

		// 发送请求
		client := &http.Client{}
		httpRsp, err := client.Do(httpReq)
		if err != nil {
			log.ErrorContextf(ctx, "CheckThirdPermission CheckKnowledgePermission request err: %+v", err)
			time.Sleep(time.Second * time.Duration(i+1)) // 指数退避
			lastError = err
			continue
		}
		// 立即处理并关闭响应体
		rsp, err := func() (*CheckKnowledgePermissionRsp, error) {
			defer httpRsp.Body.Close()
			// 处理响应体代码...
			// 读取响应体
			respBody, err := io.ReadAll(httpRsp.Body)
			if err != nil {
				log.ErrorContextf(ctx, "CheckThirdPermission CheckKnowledgePermission read response body err: %+v", err)
				return nil, err
			}

			// 检查HTTP状态码
			if httpRsp.StatusCode < 200 || httpRsp.StatusCode >= 300 {
				errMsg := fmt.Sprintf("CheckThirdPermission CheckKnowledgePermission http status code: %d", httpRsp.StatusCode)
				log.ErrorContextf(ctx, "%s", errMsg)
				return nil, errors.New(errMsg)
			}

			// 解析响应体到结构体
			rsp := &CheckKnowledgePermissionRsp{}
			if err := jsoniter.Unmarshal(respBody, rsp); err != nil {
				log.ErrorContextf(ctx, "CheckThirdPermission CheckKnowledgePermission unmarshal response body err: %+v", err)
				return nil, err
			}

			return rsp, nil
		}()
		if err != nil {
			lastError = err
			continue
		}
		log.DebugContextf(ctx, "CheckThirdPermission CheckKnowledgePermission rsp: %+v", rsp)
		return rsp, nil
	}

	log.DebugContextf(ctx, "CheckThirdPermission CheckKnowledgePermission rsp err: %+v", lastError)
	return nil, lastError
}
