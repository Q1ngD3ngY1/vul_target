package service

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/logx"
	jsoniter "github.com/json-iterator/go"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/tencentyun/cos-go-sdk-v5"
)

// ListBucketWithCORS 列举带CORS配置的存储桶
//
//	@alias=/ListBucketWithCORS
func (s *Service) ListBucketWithCORS(ctx context.Context, req *pb.ListBucketWithCORSReq) (
	*pb.ListBucketWithCORSRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.ListBucketWithCORSRsp)

	logx.I(ctx, "ListBucketWithCORS, request: %+v", req)
	defer func() {
		logx.I(ctx, "ListBucketWithCORS, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}

	_, err = s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, req.GetAppBizId(), entity.RunEnvSandbox)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	uin, _ := kbEntity.GetLoginUinAndSubAccountUin(ctx)

	logx.I(ctx, "ListBucketWithCORS, uin: %+v", uin)
	credentialResponse, status, err := s.rpc.Cloud.AssumeServiceRole(ctx, uin,
		config.App().COSDocumentConfig.ServiceRole, 0, nil)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	if status != pb.RoleStatusType_RoleStatusAvailable {
		err = errs.ErrServiceRoleUnavailable
		return rsp, err
	}
	responseText, _ := jsoniter.MarshalToString(credentialResponse)
	logx.D(ctx, "ListBucketWithCORS, credentialResponse: %s, status: %+v",
		responseText, status)

	// NOTICE: 调用cos接口获取存储桶列表
	bucketResponse, err := s.rpc.COS.ListBucketByCredential(ctx, credentialResponse.Credentials)
	if err != nil {
		return rsp, errs.ErrListBucketWithCORSFailed
	}

	if len(bucketResponse.Buckets) == 0 {
		logx.I(ctx, "ListBucketWithCORS, buckets empty, bucketResponse: %+v", bucketResponse)
		return rsp, nil
	}

	// NOTICE: 添加存储桶CORS
	corsList := make([]string, len(bucketResponse.Buckets))

	wg := errgroupx.New()
	concurrency := config.App().COSDocumentConfig.BucketCORSConcurrency
	if concurrency <= 0 {
		concurrency = 5
	}
	wg.SetLimit(concurrency)
	for i, bucket := range bucketResponse.Buckets {
		currentIndex := i
		currentBucket := bucket
		wg.Go(func() error {
			corsRule, add, err := s.rpc.COS.AddBucketCORSRule(ctx, credentialResponse.Credentials,
				currentBucket.Name, currentBucket.Region, config.App().COSDocumentConfig.CORSOrigin)
			if err != nil {
				// NOTICE: 如果添加CORS失败，则跳过该存储桶
				logx.W(ctx, "ListBucketWithCORS, AddBucketCORSRule failed, bucket[%d]: %+v, error: %+v",
					currentIndex, currentBucket, err)
				return nil
			}

			corsText, _ := jsonx.MarshalToString(corsRule)
			logx.I(ctx, "ListBucketWithCORS, bucketName[%d]: %s, add: %t, corsText: %+v",
				currentIndex, currentBucket.Name, add, corsText)
			corsList[currentIndex] = corsText
			return nil
		})
	}

	if err = wg.Wait(); err != nil {
		logx.W(ctx, "ListBucketWithCORS, wg.Wait failed, error: %+v", err)
		return rsp, errs.ErrListBucketWithCORSFailed
	}

	// NOTICE: 返回存储桶列表
	rsp.BucketList = s.generateBucketWithCORSList(ctx, bucketResponse.Buckets, corsList)
	return rsp, nil
}

func (s *Service) generateBucketWithCORSList(ctx context.Context,
	bucketList []cos.Bucket, corsList []string) []*pb.BucketWithCORS {
	bucketWithCORSList := make([]*pb.BucketWithCORS, 0)
	if len(bucketList) == 0 || len(corsList) == 0 {
		return bucketWithCORSList
	}

	for index, bucket := range bucketList {
		bucketWithCORS := &pb.BucketWithCORS{
			BucketName:   bucket.Name,
			BucketRegion: bucket.Region,
			BucketType:   bucket.BucketType,
		}

		// NOTICE: 填充 BucketCors
		if index < len(corsList) {
			if len(corsList[index]) == 0 {
				continue
			}

			bucketWithCORS.BucketCors = corsList[index]
		}

		// NOTICE: 填充 CreateTime
		createTime, err := time.Parse(time.RFC3339, bucket.CreationDate)
		if err == nil {
			bucketWithCORS.CreateTime = createTime.Unix()
		} else {
			logx.W(ctx, "generateBucketWithCORSList, time.Parse failed, "+
				"creationDate: %s, error: %+v", bucket.CreationDate, err)
		}

		bucketWithCORSList = append(bucketWithCORSList, bucketWithCORS)
	}

	return bucketWithCORSList
}
