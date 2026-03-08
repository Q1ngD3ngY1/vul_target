package api

import (
	"context"

	"git.woa.com/adp/common/x/gox/slicex"
	appConfig "git.woa.com/adp/pb-go/app/app_config"

	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func contextSeverPB2AppConfigPB(unsatisfiedReplyContext *pb.UnsatisfiedReplyContext) *appConfig.UnsatisfiedReplyContext {
	res := &appConfig.UnsatisfiedReplyContext{
		RecordId:    unsatisfiedReplyContext.GetRecordId(),
		IsVisitor:   unsatisfiedReplyContext.GetIsVisitor(),
		IsRobot:     unsatisfiedReplyContext.GetIsRobot(),
		FromId:      unsatisfiedReplyContext.GetFromId(),
		Content:     unsatisfiedReplyContext.GetContent(),
		ReplyMethod: unsatisfiedReplyContext.GetReplyMethod(),
	}
	for _, fileInfo := range unsatisfiedReplyContext.GetFileInfos() {
		res.FileInfos = append(res.FileInfos, &appConfig.UnsatisfiedFileInfo{
			FileName: fileInfo.GetFileName(),
			FileSize: fileInfo.GetFileSize(),
			FileUrl:  fileInfo.GetFileUrl(),
			FileType: fileInfo.GetFileType(),
			DocId:    fileInfo.GetDocId(),
		})
	}
	return res
}

func contextsSeverPB2AppConfigPB(unsatisfiedReplyContexts []*pb.UnsatisfiedReplyContext) []*appConfig.UnsatisfiedReplyContext {
	return slicex.Map(unsatisfiedReplyContexts, func(v *pb.UnsatisfiedReplyContext) *appConfig.UnsatisfiedReplyContext {
		return contextSeverPB2AppConfigPB(v)
	})
}

// AddUnsatisfiedReply 添加不满意回复
func (s *Service) AddUnsatisfiedReply(ctx context.Context, req *pb.AddUnsatisfiedReplyReq) (*pb.AddUnsatisfiedReplyRsp, error) {
	newReq := &appConfig.AddUnsatisfiedReplyReq{
		BotBizId:        req.GetBotBizId(),
		RecordId:        req.GetRecordId(),
		Question:        req.GetQuestion(),
		Answer:          req.GetAnswer(),
		Context:         contextsSeverPB2AppConfigPB(req.GetContext()),
		Reasons:         req.GetReasons(),
		CancelFeedback:  req.GetCancelFeedback(),
		FeedbackContent: req.GetFeedbackContent(),
	}
	_, err := s.rpc.AppAdmin.AddUnsatisfiedReply(ctx, newReq)
	return &pb.AddUnsatisfiedReplyRsp{}, err
}
