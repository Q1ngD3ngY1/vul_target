package client

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	fileManagerServer "git.woa.com/dialogue-platform/proto/pb-stub/file_manager_server"
	"strconv"
	"time"
)

const (
	// fileManagerBiz 知识问答文档解析Biz
	fileManagerBiz = "knowledge"
)

// StopDocParseTask 终止文档解析任务
func StopDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error {
	req := &fileManagerServer.CancelTaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: strconv.FormatUint(robotBizID, 10),
			Biz:    fileManagerBiz,
		},
		TaskId: taskID,
	}
	opts := []client.Option{WithTrpcSelector()}
	rsp, err := docParseCli.CancelTask(ctx, req, opts...)
	log.DebugContextf(ctx, "StopDocParseTask req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil || rsp == nil || rsp.StatusCode != 0 {
		log.ErrorContextf(ctx, "StopDocParseTask req:%+v rsp:%+v err:%+v", req, rsp, err)
		return errs.ErrStopDocParseFail
	}
	return nil
}

// AddTask 添加文档解析任务
func AddTask(ctx context.Context, req *fileManagerServer.TaskReq) (*fileManagerServer.TaskRes, error) {
	// 可能存在失败，需要重试
	timeoutSeconds := []int{1, 2, 4}
	opts := []client.Option{WithTrpcSelector()}
	for _, timeout := range timeoutSeconds {
		// 多次重试
		rsp, err := func() (*fileManagerServer.TaskRes, error) {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			rsp, err := docParseCli.AddTask(newCtx, req, opts...)
			log.DebugContextf(ctx, "AddTask req:%+v rsp:%+v err:%+v", req, rsp, err)
			if err != nil || rsp == nil || rsp.StatusCode != 0 {
				log.WarnContextf(ctx, "AddTask req:%+v rsp:%+v err:%+v", req, rsp, err)
				return nil, errs.ErrCreateDocParseSplitSegmentTaskFail
			}
			return rsp, nil
		}()
		if err == nil && rsp != nil && rsp.StatusCode == 0 {
			return rsp, nil
		}
	}
	return nil, errs.ErrCreateDocParseSplitSegmentTaskFail
}
