package service

import (
	"context"
	"encoding/json"
	"net/url"
	"path/filepath"
	"regexp"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/util"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
	sts "github.com/tencentyun/qcloud-cos-sts-sdk/go"
)

// DescribeServiceRole 查询服务角色
//
//	@alias=/DescribeServiceRole
func (s *Service) DescribeServiceRole(ctx context.Context, req *knowledge.DescribeServiceRoleReq) (
	*knowledge.DescribeServiceRoleRsp, error) {
	start := time.Now()

	var err error
	rsp := new(knowledge.DescribeServiceRoleRsp)

	logx.I(ctx, "DescribeServiceRole, request: %+v", req)
	defer func() {
		logx.I(ctx, "DescribeServiceRole, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}
	appBizId := cast.ToUint64(req.GetAppBizId())

	_, err = s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appBizId, entity.RunEnvSandbox)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	if req.GetRoleName() == "" {
		// NOTICE: 从配置中获取 ADP_QCSLinkedRolelnCOS
		req.RoleName = config.App().COSDocumentConfig.ServiceRole
	}

	uin, _ := kbEntity.GetLoginUinAndSubAccountUin(ctx)
	_, status, err := s.rpc.Cloud.AssumeServiceRole(ctx, uin, req.GetRoleName(), 0, nil)
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

	logx.I(ctx, "DescribeUserResourceCredential, request: %+v", req)
	defer func() {
		logx.I(ctx, "DescribeUserResourceCredential, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	// NOTICE: 验证资源描述
	if err = s.validateUserResourceCredentialDescribeRequest(ctx, req); err != nil {
		return rsp, err
	}

	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if corpBizID == 0 {
		err = errs.ErrContextInvalid
		return rsp, errs.ErrContextInvalid
	}
	appBizId := cast.ToUint64(req.GetAppBizId())

	_, err = s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appBizId, entity.RunEnvSandbox)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	uin, _ := kbEntity.GetLoginUinAndSubAccountUin(ctx)

	// NOTICE: 生成资源路径
	resourcePolicy, err := s.generatePolicy(ctx, req)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}

	// NOTICE: 构造访问策略
	policyBytes, err := json.Marshal(resourcePolicy)
	if err != nil {
		logx.E(ctx, "DescribeUserResourceCredential, Marshal failed, error: %+v", err)
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	policy := url.QueryEscape(string(policyBytes))
	logx.I(ctx, "DescribeUserResourceCredential, policy: %+v", policy)

	credentialResponse, status, err := s.rpc.Cloud.AssumeServiceRole(ctx, uin,
		config.App().COSDocumentConfig.ServiceRole,
		uint64(config.App().COSDocumentConfig.CredentialDuration.Seconds()), &policy)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	if status != knowledge.RoleStatusType_RoleStatusAvailable {
		err = errs.ErrServiceRoleUnavailable
		return rsp, err
	}

	logx.D(ctx, "DescribeUserResourceCredential, credentialResponse: %+v", credentialResponse)

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
		logx.E(ctx, "extractUINFromBucketName failed, bucketName: %s", bucketName)
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
			logx.E(ctx, "generatePolicy, extractUINFromBucketName failed, error: %+v", err)
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
		logx.I(ctx, "generatePolicy, resourceDescriptor: %+v, resource: %+v",
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
		len(req.GetResourceList()) > entity.UserResourceMaxCount {
		logx.E(ctx, "validateUserResourceCredentialDescribeRequest, resourceList invalid, "+
			"length: %d", len(req.ResourceList))
		return errs.ErrParameterInvalid
	}

	if req.GetResourceType() != knowledge.ResourceType_ResourceCOS {
		logx.E(ctx, "validateUserResourceCredentialDescribeRequest, resourceType invalid, "+
			"type: %d", req.GetResourceType())
		return errs.ErrParameterInvalid
	}

	return nil
}
