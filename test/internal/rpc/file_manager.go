package rpc

import (
	"context"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	fileManagerServer "git.woa.com/adp/pb-go/kb/parse_engine/file_manager_server"
	kbParseRouter "git.woa.com/adp/pb-go/kb/parse_engine/kb_parse_router"
)

type FileManagerRPC interface {
	AddTask(context.Context, *fileManagerServer.TaskReq) (*fileManagerServer.TaskRes, error)
	CancelTask(context.Context, *fileManagerServer.CancelTaskReq) (*fileManagerServer.CancelTaskRes, error)
	RetryTask(context.Context, *fileManagerServer.RetryTaskReq) (*fileManagerServer.RetryTaskRes, error)
	StopDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error

	StreamParse(ctx context.Context) (kbParseRouter.ParseRouter_StreamParseClient, error)
}

const (
	// fileManagerBiz 知识问答文档解析Biz
	fileManagerBiz = "knowledge"

	// fileManagerStatusCodeFileSizeOverLimit 文档解析返回的文件大小超限错误码
	fileManagerStatusCodeFileSizeOverLimit = 80520
	// fileManagerStatusCodeFileTypeNotSupported 文档解析返回的文件类型不支持错误码
	fileManagerStatusCodeFileTypeNotSupported = 80521
)

// StopDocParseTask 终止文档解析任务
func (r *RPC) StopDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error {
	req := &fileManagerServer.CancelTaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: strconv.FormatUint(robotBizID, 10),
			Biz:    fileManagerBiz,
		},
		TaskId: taskID,
	}
	rsp, err := r.fileManager.CancelTask(ctx, req)
	logx.D(ctx, "StopDocParseTask req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil || rsp == nil || rsp.StatusCode != 0 {
		logx.E(ctx, "StopDocParseTask req:%+v rsp:%+v err:%+v", req, rsp, err)
		return errs.ErrStopDocParseFail
	}
	return nil
}

// AddTask 添加文档解析任务
func (r *RPC) AddTask(ctx context.Context, req *fileManagerServer.TaskReq) (*fileManagerServer.TaskRes, error) {
	// 可能存在失败，需要重试
	timeoutSeconds := []int{1, 2, 4}
	for _, timeout := range timeoutSeconds {
		// 多次重试
		rsp, err := func() (*fileManagerServer.TaskRes, error) {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			rsp, err := r.fileManager.AddTask(newCtx, req)
			logx.D(ctx, "AddTask req:%+v rsp:%+v err:%+v", req, rsp, err)
			if err != nil {
				// 根据上游服务返回的错误码，返回对应的错误
				errCode := errx.Code(err)
				switch errCode {
				case fileManagerStatusCodeFileSizeOverLimit:
					return nil, errs.ErrFileSizeTooBig
				case fileManagerStatusCodeFileTypeNotSupported:
					return nil, errs.ErrUnSupportFileType
				}
				logx.W(ctx, "AddTask req:%+v rsp:%+v err:%+v", req, rsp, err)
				return nil, errs.ErrCreateDocParseSplitSegmentTaskFail
			}
			if rsp == nil || rsp.StatusCode != 0 {
				logx.W(ctx, "AddTask req:%+v rsp:%+v err:%+v", req, rsp, err)
				return nil, errs.ErrCreateDocParseSplitSegmentTaskFail
			}
			return rsp, nil
		}()
		// 如果是文件大小超限或文件类型不支持错误，直接返回，不重试
		if errs.Is(err, errs.ErrFileSizeTooBig) || errs.Is(err, errs.ErrUnSupportFileType) {
			return nil, err
		}
		if err == nil && rsp != nil && rsp.StatusCode == 0 {
			return rsp, nil
		}
	}
	return nil, errs.ErrCreateDocParseSplitSegmentTaskFail
}

func (r *RPC) CancelTask(ctx context.Context, req *fileManagerServer.CancelTaskReq) (*fileManagerServer.CancelTaskRes, error) {
	return r.fileManager.CancelTask(ctx, req)
}

func (r *RPC) RetryTask(ctx context.Context, req *fileManagerServer.RetryTaskReq) (*fileManagerServer.RetryTaskRes, error) {
	return r.fileManager.RetryTask(ctx, req)
}

func (r *RPC) StreamParse(ctx context.Context) (kbParseRouter.ParseRouter_StreamParseClient, error) {
	cli, err := r.parseRouter.StreamParse(ctx)
	if err != nil {
		logx.E(ctx, "StreamParse err:%+v", err)
		return nil, err
	}
	return cli, nil
}
