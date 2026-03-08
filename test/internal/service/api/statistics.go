package api

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// DescribeDocStatistics 获取文档相关统计信息
func (s *Service) DescribeDocStatistics(ctx context.Context, req *pb.DescribeDocStatisticsReq) (*pb.DescribeDocStatisticsRsp, error) {
	logx.I(ctx, "DescribeDocStatistics Req: %+v", req)

	// 调用logic层的方法
	rsp, err := s.docLogic.DescribeDocStatistics(ctx, req.GetCorpBizId(), req.GetAppBizId(), s.kbDao, s.cacheLogic)
	if err != nil {
		logx.E(ctx, "DescribeDocStatistics failed, err: %+v", err)
		return nil, err
	}

	return rsp, nil
}

// DescribeQaStatistics 获取问答相关统计信息
func (s *Service) DescribeQaStatistics(ctx context.Context, req *pb.DescribeQaStatisticsReq) (*pb.DescribeQaStatisticsRsp, error) {
	logx.I(ctx, "DescribeQaStatistics Req: %+v", req)

	// 调用logic层的方法
	rsp, err := s.qaLogic.DescribeQaStatistics(ctx, req.GetCorpBizId(), req.GetAppBizId(), s.kbDao, s.cacheLogic)
	if err != nil {
		logx.E(ctx, "DescribeQaStatistics failed, err: %+v", err)
		return nil, err
	}

	return rsp, nil
}

// DescribeDbStatistics 获取数据库相关统计信息
func (s *Service) DescribeDbStatistics(ctx context.Context, req *pb.DescribeDbStatisticsReq) (*pb.DescribeDbStatisticsRsp, error) {
	logx.I(ctx, "DescribeDbStatistics Req: %+v", req)

	// 调用logic层的方法
	rsp, err := s.dbLogic.DescribeDbStatistics(ctx, req.GetCorpBizId(), req.GetAppBizId(), s.kbDao, s.cacheLogic)
	if err != nil {
		logx.E(ctx, "DescribeDbStatistics failed, err: %+v", err)
		return nil, err
	}

	return rsp, nil
}
