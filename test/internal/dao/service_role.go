package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cloudsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"
)

const (
	// ErrRoleIsNotExistV3 角色不存在V3
	ErrRoleIsNotExistV3 = "InternalError.GetRoleError"
)

// AssumeServiceRole 扮演服务角色
func (d *dao) AssumeServiceRole(ctx context.Context, roleUIN, roleName string,
	duration uint64, policy *string) (
	*cloudsts.AssumeRoleResponseParams, pb.RoleStatusType, error) {
	roleARN := d.generateARN(roleUIN, roleName)

	response, exist, err := d.assumeRole(ctx, roleARN, duration, policy)
	if err != nil {
		log.ErrorContextf(ctx, "AssumeServiceRole failed, error: %+v", err)
		return nil, pb.RoleStatusType_RoleStatusAbnormal, err
	}

	if !exist {
		return nil, pb.RoleStatusType_RoleStatusNotExist, nil
	}

	return response, pb.RoleStatusType_RoleStatusAvailable, nil
}

// generateARN 构建授权角色信息
func (d *dao) generateARN(roleUIN, roleName string) string {
	return fmt.Sprintf("qcs::cam::uin/%s:role/tencentcloudServiceRoleName/%s", roleUIN, roleName)
}

func (d *dao) assumeRole(ctx context.Context, roleArn string, duration uint64, policy *string) (
	*cloudsts.AssumeRoleResponseParams, bool, error) {
	log.InfoContextf(ctx, "assumeRole, roleArn: %s, duration: %d, policy: %+v",
		roleArn, duration, policy)

	cfg := config.App().COSDocumentConfig
	log.DebugContextf(ctx, "assumeRole, cfg: %+v", cfg)

	// 实例化一个认证对象，入参需要传入腾讯云账户 SecretId 和 SecretKey，此处还需注意密钥对的保密
	// 代码泄露可能会导致 SecretId 和 SecretKey 泄露，并威胁账号下所有资源的安全性
	// 以下代码示例仅供参考，建议采用更安全的方式来使用密钥
	// 请参见：https://cloud.tencent.com/document/product/1278/85305
	// 密钥可前往官网控制台 https://console.cloud.tencent.com/cam/capi 进行获取
	credential := common.NewCredential(
		cfg.SecretID,
		cfg.SecretKey,
	)

	// 使用临时密钥示例
	// credential := common.NewTokenCredential("SecretId", "SecretKey", "Token")
	// 实例化一个client选项，可选的，没有特殊需求可以跳过
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = cfg.STSEndpoint

	// 实例化要请求产品的client对象,clientProfile是可选的
	client, err := cloudsts.NewClient(credential, cfg.STSRegion, cpf)
	if err != nil {
		log.ErrorContextf(ctx, "STS-SDK NewClient failed, error: %+v", err)
		return nil, false, err
	}

	// 实例化一个请求对象,每个接口都会对应一个request对象
	request := cloudsts.NewAssumeRoleRequest()
	request.RoleArn = common.StringPtr(roleArn)
	request.RoleSessionName = common.StringPtr(fmt.Sprintf("role-%d", time.Now().UnixMilli()))

	if policy != nil {
		request.Policy = policy
	}

	if duration != 0 {
		request.DurationSeconds = common.Uint64Ptr(duration)
	}

	requestText, _ := jsoniter.MarshalToString(request)
	log.InfoContextf(ctx, "assumeRole, request: %+v", requestText)

	// 返回的resp是一个AssumeRoleResponse的实例，与请求对象对应
	response, err := client.AssumeRole(request)
	if err != nil {
		if sdkErr, ok := err.(*errors.TencentCloudSDKError); ok {
			if sdkErr.GetCode() == ErrRoleIsNotExistV3 {
				log.InfoContextf(ctx, "STS-SDK AssumeRole, roleArn: %s, error: %+v", roleArn, sdkErr)
				return nil, false, nil
			}
		}

		log.ErrorContextf(ctx, "STS-SDK AssumeRole failed, roleArn: %s, error: %+v", roleArn, err)
		return nil, false, err
	}

	if response == nil {
		log.ErrorContextf(ctx, "assumeRole, AssumeRoleResponse is nil")
		return nil, false, errs.ErrAssumeServiceRoleFailed
	}

	responseText, _ := jsoniter.MarshalToString(response)
	log.InfoContextf(ctx, "assumeRole, response: %+v", responseText)

	if response.Response == nil {
		log.ErrorContextf(ctx, "assumeRole, AssumeRoleResponse.Response is nil")
		return nil, false, errs.ErrAssumeServiceRoleFailed
	}

	return response.Response, true, nil
}
