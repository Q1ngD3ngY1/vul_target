package service

import (
	"context"

	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func appConfigUnsatisfiedReplyPB2ServerPB(unsatisfiedReply *appConfig.UnsatisfiedReply) *pb.ListUnsatisfiedReplyRsp_UnsatisfiedReply {
	return &pb.ListUnsatisfiedReplyRsp_UnsatisfiedReply{
		ReplyBizId:      unsatisfiedReply.GetReplyBizId(),
		RecordBizId:     unsatisfiedReply.GetRecordBizId(),
		Question:        unsatisfiedReply.GetQuestion(),
		Answer:          unsatisfiedReply.GetAnswer(),
		Reasons:         unsatisfiedReply.GetReasons(),
		Status:          unsatisfiedReply.GetStatus(),
		Operator:        unsatisfiedReply.GetOperator(),
		CreateTime:      unsatisfiedReply.GetCreateTime(),
		UpdateTime:      unsatisfiedReply.GetUpdateTime(),
		FeedbackContent: unsatisfiedReply.GetFeedbackContent(),
	}
}

func appConfigUnsatisfiedRepliesPB2ServerPB(unsatisfiedReplies []*appConfig.UnsatisfiedReply) []*pb.ListUnsatisfiedReplyRsp_UnsatisfiedReply {
	return slicex.Map(unsatisfiedReplies, func(unsatisfiedReply *appConfig.UnsatisfiedReply) *pb.ListUnsatisfiedReplyRsp_UnsatisfiedReply {
		return appConfigUnsatisfiedReplyPB2ServerPB(unsatisfiedReply)
	})
}

// ListUnsatisfiedReply 获取不满意回复
func (s *Service) ListUnsatisfiedReply(ctx context.Context, req *pb.ListUnsatisfiedReplyReq) (*pb.ListUnsatisfiedReplyRsp, error) {
	newReq := &appConfig.DescribeUnsatisfiedReplyListReq{
		BotBizId:         req.GetBotBizId(),
		Query:            req.GetQuery(),
		Reasons:          req.GetReasons(),
		PageNumber:       req.GetPageNumber(),
		PageSize:         req.GetPageSize(),
		Status:           req.GetStatus(),
		HandlingStatuses: req.GetHandlingStatuses(),
	}
	newRsp, err := s.rpc.AppAdmin.DescribeUnsatisfiedReplyList(ctx, newReq)
	if newRsp != nil {
		return &pb.ListUnsatisfiedReplyRsp{
			Total: newRsp.GetTotal(),
			List:  appConfigUnsatisfiedRepliesPB2ServerPB(newRsp.GetList()),
		}, err
	}
	return nil, err
}

// IgnoreUnsatisfiedReply 忽略不满意回复
func (s *Service) IgnoreUnsatisfiedReply(ctx context.Context, req *pb.IgnoreUnsatisfiedReplyReq) (*pb.IgnoreUnsatisfiedReplyRsp, error) {
	newReq := &appConfig.IgnoreUnsatisfiedReplyReq{
		BotBizId:    req.GetBotBizId(),
		Ids:         req.GetIds(),
		ReplyBizIds: req.GetReplyBizIds(),
	}
	_, err := s.rpc.AppAdmin.IgnoreUnsatisfiedReply(ctx, newReq)

	return &pb.IgnoreUnsatisfiedReplyRsp{}, err
}

func appContextPB2ServerPB(appContext *appConfig.Context) *pb.DescribeUnsatisfiedReplyRsp_Context {
	res := &pb.DescribeUnsatisfiedReplyRsp_Context{
		RecordBizId: appContext.GetRecordId(),
		IsVisitor:   appContext.GetIsVisitor(),
		NickName:    appContext.GetNickName(),
		Avatar:      appContext.GetAvatar(),
		Content:     appContext.GetContent(),
		ReplyMethod: appContext.GetReplyMethod(),
	}
	for _, v := range appContext.FileInfos {
		res.FileInfos = append(res.FileInfos, &pb.FileInfo{
			FileName: v.GetFileName(),
			FileType: v.GetFileType(),
			FileUrl:  v.GetFileUrl(),
			FileSize: v.GetFileSize(),
			DocId:    v.GetDocId(),
		})
	}
	return res
}

func appContextsPB2ServerPB(appContexts []*appConfig.Context) []*pb.DescribeUnsatisfiedReplyRsp_Context {
	return slicex.Map(appContexts, func(appContext *appConfig.Context) *pb.DescribeUnsatisfiedReplyRsp_Context {
		return appContextPB2ServerPB(appContext)
	})
}

// DescribeUnsatisfiedReplyContext 获取不满意回复上下文
func (s *Service) DescribeUnsatisfiedReplyContext(ctx context.Context, req *pb.DescribeUnsatisfiedReplyReq) (*pb.DescribeUnsatisfiedReplyRsp, error) {
	newReq := &appConfig.DescribeUnsatisfiedReplyContextReq{
		BotBizId: convx.MustStringToUint64(req.GetBotBizId()),
		Id:       convx.MustStringToUint64(req.GetReplyBizId()),
	}
	newRsp, err := s.rpc.AppAdmin.DescribeUnsatisfiedReplyContext(ctx, newReq)
	if newRsp != nil {
		return &pb.DescribeUnsatisfiedReplyRsp{
			List: appContextsPB2ServerPB(newRsp.GetList()),
		}, err
	}
	return nil, err
}

func exportUnsatisfiedReplyReqFiltersPB2AppConfigPB(filter *pb.ExportUnsatisfiedReplyReq_Filters) *appConfig.Filters {
	return &appConfig.Filters{
		Query:            filter.GetQuery(),
		Reasons:          filter.GetReasons(),
		HandlingStatuses: filter.GetHandlingStatuses(),
	}
}

// ExportUnsatisfiedReply 导出不满意回复
func (s *Service) ExportUnsatisfiedReply(ctx context.Context, req *pb.ExportUnsatisfiedReplyReq) (*pb.ExportUnsatisfiedReplyRsp, error) {
	newReq := &appConfig.ExportUnsatisfiedReplyReq{
		BotBizId:    req.GetBotBizId(),
		Filters:     exportUnsatisfiedReplyReqFiltersPB2AppConfigPB(req.GetFilters()),
		Ids:         req.GetIds(),
		ReplyBizIds: req.GetReplyBizIds(),
	}
	_, err := s.rpc.AppAdmin.ExportUnsatisfiedReply(ctx, newReq)
	return &pb.ExportUnsatisfiedReplyRsp{}, err
}

// CreateQaSimilarFromUnsatisfiedReply 将不满意回复添加到QA相似问
func (s *Service) CreateQaSimilarFromUnsatisfiedReply(ctx context.Context, req *pb.CreateQaSimilarFromUnsatisfiedReplyReq) (*pb.CreateQaSimilarFromUnsatisfiedReplyRsp, error) {
	logx.I(ctx, "CreateQaSimilarFromUnsatisfiedReply Req:%+v", req)
	rsp := new(pb.CreateQaSimilarFromUnsatisfiedReplyRsp)

	qaBizId, err := convx.StringToUint64(req.GetQaBizId())
	if err != nil {
		logx.E(ctx, "CreateQaSimilarFromUnsatisfiedReply qaBizId is invalid, err:%+v", err)
		return rsp, err
	}
	unsatisfiedReplyBizId, err := convx.StringToUint64(req.GetUnsatisfiedReplyBizId())
	if err != nil {
		logx.E(ctx, "CreateQaSimilarFromUnsatisfiedReply unsatisfiedReplyBizId is invalid, err:%+v", err)
		return rsp, err
	}

	// 参数校验
	if qaBizId == 0 {
		return rsp, errs.ErrParams
	}
	if unsatisfiedReplyBizId == 0 {
		return rsp, errs.ErrParams
	}
	if req.GetUnsatisfiedReplyContent() == "" {
		return rsp, errs.ErrParams
	}

	// 调用logic层函数处理相似问添加
	err = s.qaLogic.AddSimilarQuestionFromUnsatisfiedReply(ctx, qaBizId, unsatisfiedReplyBizId, req.GetUnsatisfiedReplyContent())
	if err != nil {
		logx.E(ctx, "CreateQaSimilarFromUnsatisfiedReply AddSimilarQuestionFromUnsatisfiedReply failed, qaBizId:%d, unsatisfiedReplyBizId:%d, err:%+v", qaBizId, unsatisfiedReplyBizId, err)
		return rsp, err
	}

	logx.I(ctx, "CreateQaSimilarFromUnsatisfiedReply success, qaBizId:%d, similarQuestion:%s", qaBizId, req.GetUnsatisfiedReplyContent())
	return rsp, nil
}
