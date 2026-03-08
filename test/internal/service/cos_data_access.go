// Package service 业务逻辑层-客户COS文档
package service

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"
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

	log.InfoContextf(ctx, "ListBucketWithCORS, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "ListBucketWithCORS, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	corpBizID := pkg.CorpBizID(ctx)
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}

	_, err = client.GetAppInfo(ctx, req.GetAppBizId(), model.RunEnvSandbox)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	uin, _ := model.GetLoginUinAndSubAccountUin(ctx)
	credentialResponse, status, err := s.dao.AssumeServiceRole(ctx, uin,
		config.App().COSDocumentConfig.ServiceRole, 0, nil)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	if status != pb.RoleStatusType_RoleStatusAvailable {
		err = errs.ErrServiceRoleUnavailable
		return rsp, err
	}
	log.DebugContextf(ctx, "ListBucketWithCORS, credentialResponse: %+v", credentialResponse)

	// NOTICE: 调用cos接口获取存储桶列表
	bucketResponse, err := s.dao.ListBucketByCredential(ctx, credentialResponse.Credentials)
	if err != nil {
		return rsp, errs.ErrListBucketWithCORSFailed
	}

	if len(bucketResponse.Buckets) == 0 {
		log.InfoContextf(ctx, "ListBucketWithCORS, buckets empty, bucketResponse: %+v", bucketResponse)
		return rsp, nil
	}

	// NOTICE: 添加存储桶CORS
	corsList := make([]string, 0)

	for _, bucket := range bucketResponse.Buckets {
		corsRule, _, err := s.dao.AddBucketCORSRule(ctx, credentialResponse.Credentials,
			bucket.Name, bucket.Region, config.App().COSDocumentConfig.CORSOrigin)
		if err != nil {
			return rsp, errs.ErrListBucketWithCORSFailed
		}

		corsText, _ := jsoniter.MarshalToString(corsRule)
		log.InfoContextf(ctx, "ListBucketWithCORS, corsText: %+v", corsText)
		corsList = append(corsList, corsText)
	}

	// NOTICE: 返回存储桶列表
	rsp.BucketList = s.generateBucketWithCORSList(ctx, bucketResponse.Buckets, corsList)
	return rsp, nil
}

func (s *Service) generateBucketWithCORSList(ctx context.Context,
	bucketList []cos.Bucket, corsList []string) []*pb.BucketWithCORS {
	bucketWithCORSList := make([]*pb.BucketWithCORS, 0)
	for index, bucket := range bucketList {
		bucketWithCORS := &pb.BucketWithCORS{
			BucketName:   bucket.Name,
			BucketRegion: bucket.Region,
			BucketType:   bucket.BucketType,
		}

		// NOTICE: 填充 CreateTime
		createTime, err := time.Parse(time.RFC3339, bucket.CreationDate)
		if err == nil {
			bucketWithCORS.CreateTime = createTime.Unix()
		} else {
			log.WarnContextf(ctx, "generateBucketWithCORSList, time.Parse failed, "+
				"creationDate: %s, error: %+v", bucket.CreationDate, err)
		}

		// NOTICE: 填充 BucketCors
		if corsList != nil && index < len(corsList) {
			//bucketWithCORS.BucketConfig = corsList[index]
			bucketWithCORS.BucketCors = corsList[index]
		}

		bucketWithCORSList = append(bucketWithCORSList, bucketWithCORS)
	}
	
	return bucketWithCORSList
}
