package service

import (
	"context"
	"encoding/json"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	sts "github.com/tencentyun/qcloud-cos-sts-sdk/go"
	"net/url"
	"path/filepath"
	"regexp"
	"time"
)

// DescribeServiceRole 查询服务角色
//
//	@alias=/DescribeServiceRole
func (s *Service) DescribeServiceRole(ctx context.Context, req *knowledge.DescribeServiceRoleReq) (
	*knowledge.DescribeServiceRoleRsp, error) {
	start := time.Now()

	var err error
	rsp := new(knowledge.DescribeServiceRoleRsp)

	log.InfoContextf(ctx, "DescribeServiceRole, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "DescribeServiceRole, response: %+v, elapsed: %d, error: %+v",
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

	if req.GetRoleName() == "" {
		// NOTICE: 从配置中获取 ADP_QCSLinkedRolelnCOS
		req.RoleName = config.App().COSDocumentConfig.ServiceRole
	}

	uin, _ := model.GetLoginUinAndSubAccountUin(ctx)
	_, status, err := s.dao.AssumeServiceRole(ctx, uin, req.GetRoleName(), 0, nil)
	if err != nil {
		return rsp, errs.ErrDescribeServiceRoleFailed
	}

	rsp.RoleId = util.Md5Hex(req.GetRoleName())
	rsp.RoleName = req.GetRoleName()
	rsp.RoleStatus = status
	
	return rsp, nil
}

// DescribeUserResourceCredential 获取资源临时凭证
//
//	@alias=/DescribeUserResourceCredential
func (s *Service) DescribeUserResourceCredential(ctx context.Context, req *knowledge.DescribeUserResourceCredentialReq) (
	*knowledge.DescribeUserResourceCredentialRsp, error) {
	start := time.Now()

	var err error
	rsp := new(knowledge.DescribeUserResourceCredentialRsp)

	log.InfoContextf(ctx, "DescribeUserResourceCredential, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "DescribeUserResourceCredential, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	// NOTICE: 验证资源描述
	if err = s.validateUserResourceCredentialDescribeRequest(ctx, req); err != nil {
		return rsp, err
	}

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

	// NOTICE: 生成资源路径
	resourcePolicy, err := s.generatePolicy(ctx, req)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}

	// NOTICE: 构造访问策略
	policyBytes, err := json.Marshal(resourcePolicy)
	if err != nil {
		log.ErrorContextf(ctx, "DescribeUserResourceCredential, Marshal failed, error: %+v", err)
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	policy := url.QueryEscape(string(policyBytes))
	log.InfoContextf(ctx, "DescribeUserResourceCredential, policy: %+v", policy)

	credentialResponse, status, err := s.dao.AssumeServiceRole(ctx, uin,
		config.App().COSDocumentConfig.ServiceRole,
		uint64(config.App().COSDocumentConfig.CredentialDuration.Seconds()), &policy)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	if status != knowledge.RoleStatusType_RoleStatusAvailable {
		err = errs.ErrServiceRoleUnavailable
		return rsp, err
	}

	log.DebugContextf(ctx, "DescribeUserResourceCredential, credentialResponse: %+v", credentialResponse)

	rsp.Credential = &knowledge.ResourceCredential{
		SessionToken:  *credentialResponse.Credentials.Token,
		TempSecretId:  *credentialResponse.Credentials.TmpSecretId,
		TempSecretKey: *credentialResponse.Credentials.TmpSecretKey,
	}

	rsp.ExpireTime = *credentialResponse.ExpiredTime
	rsp.StartTime = time.Now().Unix()

	return rsp, nil
}

func (s *Service) extractUINFromBucketName(ctx context.Context, bucketName string) (string, error) {
	// 存储桶名格式: bucketName-1250000000
	pattern := `.*-(\d{5,12})$`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(bucketName)
	if len(matches) < 2 {
		log.ErrorContextf(ctx, "extractUINFromBucketName failed, bucketName: %s", bucketName)
		return "", errs.ErrParameterInvalid
	}

	return matches[1], nil
}

func (s *Service) generatePolicy(ctx context.Context, req *knowledge.DescribeUserResourceCredentialReq) (
	*sts.CredentialPolicy, error) {
	resourceList := make([]string, 0)
	for _, resourceDescriptor := range req.GetResourceList() {
		uid, err := s.extractUINFromBucketName(ctx, resourceDescriptor.ResourceBucket)
		if err != nil {
			log.ErrorContextf(ctx, "generatePolicy, extractUINFromBucketName failed, error: %+v", err)
			return nil, err
		}

		// qcs::cos:{region}:uid/{appid}:{bucket}/{path}
		resource := "qcs::cos:" + resourceDescriptor.ResourceRegion +
			":uid/" + uid + ":" +
			resourceDescriptor.ResourceBucket + resourceDescriptor.ResourcePath

		if len(filepath.Ext(resourceDescriptor.ResourcePath)) == 0 {
			// qcs::cos:{region}:uid/{appid}:{bucket}/{path}/*
			resource += "/*"
		}
		log.InfoContextf(ctx, "generatePolicy, resourceDescriptor: %+v, resource: %+v",
			resourceDescriptor, resource)
		resourceList = append(resourceList, resource)
	}

	return &sts.CredentialPolicy{
		Version: "2.0",
		Statement: []sts.CredentialPolicyStatement{{
			Action: []string{
				"cos:GetObject",
				"cos:GetBucket",
			},
			Effect:   "allow",
			Resource: resourceList,
		}}}, nil
}

func (s *Service) validateUserResourceCredentialDescribeRequest(ctx context.Context,
	req *knowledge.DescribeUserResourceCredentialReq) error {
	if len(req.GetResourceList()) == 0 ||
		len(req.GetResourceList()) > model.UserResourceMaxCount {
		log.ErrorContextf(ctx, "validateUserResourceCredentialDescribeRequest, resourceList invalid, "+
			"length: %d", len(req.ResourceList))
		return errs.ErrParameterInvalid
	}

	if req.GetResourceType() != knowledge.ResourceType_ResourceCOS {
		log.ErrorContextf(ctx, "validateUserResourceCredentialDescribeRequest, resourceType invalid, "+
			"type: %d", req.GetResourceType())
		return errs.ErrParameterInvalid
	}

	return nil
}
