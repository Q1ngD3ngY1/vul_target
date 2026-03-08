// Package client sse客户端
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util/sse/event"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util/sse/polaris0"
)

const defaultBufSize = 1000

type SSEClient struct {
	client  *http.Client
	options config.SSEConnOptions
	onReply event.Listener
	onError event.Listener
}

func NewSSEClient(options config.SSEConnOptions, onReply, onError event.Listener) *SSEClient {
	client := &http.Client{
		Timeout: time.Duration(options.ClientTimeOut) * time.Second,
	}
	return &SSEClient{
		options: options,
		client:  client,
		onReply: onReply,
		onError: onError,
	}
}

func (s *SSEClient) StartStream(ctx context.Context, reqEvent *event.SseSendEvent) error {
	targetURL, err := s.parseURL(ctx)
	if err != nil {
		return err
	}

	body, err := json.Marshal(reqEvent)
	if err != nil {
		log.ErrorContextf(ctx, "marshal event %+v error, %v", reqEvent, err)
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", targetURL, bytes.NewBuffer(body))
	if err != nil {
		log.ErrorContextf(ctx, "create sse req %s error：%v", string(body), err)
		return err
	}
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Connection", "keep-alive")
	log.DebugContextf(ctx, "sse request, url: %v, body: %s", targetURL, string(body))

	resp, err := s.client.Do(req)
	if err != nil {
		log.ErrorContextf(ctx, "request sse req %s error：%v", string(body), err)
		return err
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.WarnContextf(ctx, "close body error, %v", err)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		log.ErrorContextf(ctx, "sse request error, req: %s, rsp: %+v", string(body), resp)
		return fmt.Errorf("sse request error, %v", resp.StatusCode)
	}

	// 流式读取数据
	return s.receiveStream(ctx, resp.Body)
}

func (s *SSEClient) receiveStream(ctx context.Context, body io.ReadCloser) error {
	scanner := bufio.NewScanner(body)
	bufSize := defaultBufSize * 1024
	if config.App().SampleTest.SSEBufferSizeK > 0 {
		bufSize = config.App().SampleTest.SSEBufferSizeK * 1024
	}
	buf := make([]byte, bufSize)
	scanner.Buffer(buf, bufSize)
	for scanner.Scan() {
		// 不缓存，逐行解析，不需要解析完整的帧，增加健壮性
		line := scanner.Text()
		if strings.HasPrefix(line, "event:") {
			eventCode := strings.TrimPrefix(line, "event:")
			if eventCode == event.EventError {
				// 收到错误时间，下一行应该会解析到错误事件的data:，再回调OnError
				log.WarnContextf(ctx, "receive error event")
			}
		} else if strings.HasPrefix(line, "data:") {
			data := strings.TrimPrefix(line, "data:")
			result := event.Wrapper{}
			err := json.Unmarshal([]byte(data), &result)
			if err != nil {
				log.ErrorContextf(ctx, "unmarshal response error, %v, rsp: %s", err, data)
				continue
			}
			// type有 reply  token_stat 和  error
			if result.Type == "token_stat" {
				continue
			}
			if result.Type == "error" {
				log.WarnContextf(ctx, "receive event error: %s", data)
				s.onError(result)
				continue
			}
			s.onReply(result)
		} else if line == "" {
			continue
		} else {
			log.WarnContextf(ctx, "unexpcet data, %s", line)
		}
	}
	if err := scanner.Err(); err != nil {
		log.ErrorContextf(ctx, "receiveStream scanner error: %v", err)
		return err
	}
	return nil
}

func (s *SSEClient) parseURL(ctx context.Context) (string, error) {
	if s.options.ModID > 0 && s.options.CmdID > 0 {
		// 优先使用北极星，直接访问后端实例的IP地址
		ip, port, err := polaris0.GetL5IpAndPort(ctx, s.options.ModID, s.options.CmdID, s.options.NameSpace,
			s.options.ENV, s.options.HashKey)
		if err != nil {
			log.ErrorContextf(ctx, "sse client parse url failed, options: %+v, %v", s.options, err)
			return "", err
		}
		parsed, err := url.Parse(s.options.ConnURL)
		if err != nil {
			log.ErrorContextf(ctx, "parse sse url %v failed, %v", s.options.ConnURL, err)
			return "", err
		}
		newURL := url.URL{
			Scheme:      "http",
			Opaque:      parsed.Opaque,
			User:        parsed.User,
			Host:        fmt.Sprintf("%s:%d", ip, port),
			Path:        parsed.Path,
			RawPath:     parsed.RawPath,
			ForceQuery:  parsed.ForceQuery,
			RawQuery:    parsed.RawQuery,
			Fragment:    parsed.Fragment,
			RawFragment: parsed.RawFragment,
		}
		return newURL.String(), nil
	}
	return s.options.ConnURL, nil
}
