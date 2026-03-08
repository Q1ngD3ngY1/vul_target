package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	tcommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	tprofile "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cloudsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity/cloud"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common"
	tchttp "git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common/http"
	"git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common/profile"
)

const (
	// ErrRoleIsNotExistV3 角色不存在V3
	ErrRoleIsNotExistV3 = "InternalError.GetRoleError"
)

// TODO: 这里import需要优化一下，github.com/tencentcloud/tencentcloud-sdk-go 和  git.woa.com/tencentcloud-internal/tencentcloud-sdk-go 要统一用一个 @wemysschen

type CloudRPC interface {
	DescribeNickname(ctx context.Context, uin, subAccountUin string) (*cloud.NicknameInfo, error)
	BatchCheckWhitelist(ctx context.Context, key, uin string) (bool, error)
	AssumeServiceRole(ctx context.Context, roleUIN, roleName string, duration uint64, policy *string) (
		*cloudsts.AssumeRoleResponseParams, pb.RoleStatusType, error)
}

// DescribeNickname 获取昵称
func (r *RPC) DescribeNickname(ctx context.Context, uin, subAccountUin string) (*cloud.NicknameInfo, error) {
	cfg := config.App().CloudAPIs
	credential := common.NewCredential(cfg.SecretID, cfg.SecretKey)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = cfg.AccountHost
	cpf.HttpProfile.ReqMethod = "POST"
	// 创建common client
	client := common.NewCommonClient(credential, cfg.Region, cpf)

	// 创建common request，依次传入产品名、产品版本、接口名称
	request := tchttp.NewCommonRequest("account", cfg.Version, "DescribeNickname")

	// 自定义请求参数:
	body := map[string]any{}

	// set custom headers
	request.SetHeader(map[string]string{
		"X-TC-TraceId":    uuid.NewString(),
		"X-Auth-OwnerUin": uin,
		"X-Auth-SubUin":   subAccountUin,
	})

	// 设置action所需的请求数据
	err := request.SetActionParameters(body)
	if err != nil {
		logx.E(ctx, "DescribeNickname request:%+v err:%+v", request, err)
		return nil, err
	}

	// 创建common response
	response := tchttp.NewCommonResponse()

	// 发送请求
	err = client.Send(request, response)
	if err != nil {
		logx.E(ctx, "DescribeNickname request:%+v err:%+v", request, err)
		return nil, err
	}
	logx.D(ctx, "DescribeNickname request:%+v rsp:%s", request, string(response.GetBody()))

	rsp := cloud.DescribeNicknameRsp{}
	if err = jsonx.Unmarshal(response.GetBody(), &rsp); err != nil {
		logx.E(ctx, "DescribeNickname response:%+v err:%+v", response, err)
		return nil, err
	}
	return &rsp.Response, nil
}

// BatchCheckWhitelist 批量检查白名单
func (r *RPC) BatchCheckWhitelist(ctx context.Context, key, uin string) (bool, error) {
	cfg := config.App().CloudAPIs
	credential := common.NewCredential(cfg.SecretID, cfg.SecretKey)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = cfg.AccountHost
	cpf.HttpProfile.ReqMethod = "POST"
	// 创建common client
	client := common.NewCommonClient(credential, cfg.Region, cpf)

	// 创建common request，依次传入产品名、产品版本、接口名称
	// DescribeNickname
	// BatchCheckWhitelist
	request := tchttp.NewCommonRequest("account", cfg.Version, "BatchCheckWhitelist")

	body := map[string]any{
		"WhitelistKeyList": []string{key},
	}

	// set custom headers
	request.SetHeader(map[string]string{
		"X-TC-TraceId":    uuid.NewString(),
		"X-Auth-OwnerUin": uin,
	})

	// 设置action所需的请求数据
	err := request.SetActionParameters(body)
	if err != nil {
		logx.E(ctx, "SetActionParameters err:%+v", err)
		return false, err
	}

	// 创建common response
	response := tchttp.NewCommonResponse()

	// 发送请求
	err = client.Send(request, response)
	if err != nil {
		logx.E(ctx, "BatchCheckWhitelist request:%+v err:%+v", request, err)
		return false, err
	}
	logx.D(ctx, "BatchCheckWhitelist uin:%s rsp:%s", uin, string(response.GetBody()))

	// 获取响应结果
	rsp := cloud.BatchCheckWhitelistRsp{}
	if err = jsonx.Unmarshal(response.GetBody(), &rsp); err != nil {
		logx.E(ctx, "BatchCheckWhitelist response:%+v err:%+v", response, err)
		return false, err
	}
	if len(rsp.Response.MatchedWhitelist) == 0 {
		return false, nil
	}
	return true, nil
}

// AssumeServiceRole 扮演服务角色
func (r *RPC) AssumeServiceRole(ctx context.Context, roleUIN, roleName string,
	duration uint64, policy *string) (
	*cloudsts.AssumeRoleResponseParams, pb.RoleStatusType, error) {
	roleARN := r.generateARN(roleUIN, roleName)

	response, exist, err := r.assumeRole(ctx, roleARN, duration, policy)
	if err != nil {
		logx.E(ctx, "AssumeServiceRole failed, error: %+v", err)
		return nil, pb.RoleStatusType_RoleStatusAbnormal, err
	}

	if !exist {
		return nil, pb.RoleStatusType_RoleStatusNotExist, nil
	}

	return response, pb.RoleStatusType_RoleStatusAvailable, nil
}

// generateARN 构建授权角色信息
func (r *RPC) generateARN(roleUIN, roleName string) string {
	return fmt.Sprintf("qcs::cam::uin/%s:role/tencentcloudServiceRoleName/%s", roleUIN, roleName)
}

func (r *RPC) assumeRole(ctx context.Context, roleArn string, duration uint64, policy *string) (
	*cloudsts.AssumeRoleResponseParams, bool, error) {
	logx.I(ctx, "assumeRole, roleArn: %s, duration: %d, policy: %+v",
		roleArn, duration, policy)

	cfg := config.App().COSDocumentConfig
	logx.D(ctx, "assumeRole, cfg: %+v", cfg)

	// 实例化一个认证对象，入参需要传入腾讯云账户 SecretId 和 SecretKey，此处还需注意密钥对的保密
	// 代码泄露可能会导致 SecretId 和 SecretKey 泄露，并威胁账号下所有资源的安全性
	// 以下代码示例仅供参考，建议采用更安全的方式来使用密钥
	// 请参见：https://cloud.tencent.com/document/product/1278/85305
	// 密钥可前往官网控制台 https://console.cloud.tencent.com/cam/capi 进行获取
	credential := tcommon.NewCredential(
		cfg.SecretID,
		cfg.SecretKey,
	)

	// 使用临时密钥示例
	// credential := common.NewTokenCredential("SecretId", "SecretKey", "Token")
	// 实例化一个client选项，可选的，没有特殊需求可以跳过
	cpf := tprofile.NewClientProfile()
	cpf.HttpProfile.Endpoint = cfg.STSEndpoint

	// 实例化要请求产品的client对象,clientProfile是可选的
	client, err := cloudsts.NewClient(credential, cfg.STSRegion, cpf)
	if err != nil {
		logx.E(ctx, "STS-SDK NewClient failed, error: %+v", err)
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

	requestText, _ := jsonx.MarshalToString(request)
	logx.I(ctx, "assumeRole, request: %+v", requestText)

	// 返回的resp是一个AssumeRoleResponse的实例，与请求对象对应
	response, err := client.AssumeRole(request)
	if err != nil {
		if sdkErr, ok := err.(*errors.TencentCloudSDKError); ok {
			if sdkErr.GetCode() == ErrRoleIsNotExistV3 {
				logx.I(ctx, "STS-SDK AssumeRole, roleArn: %s, error: %+v", roleArn, sdkErr)
				return nil, false, nil
			}
		}

		logx.E(ctx, "STS-SDK AssumeRole failed, roleArn: %s, error: %+v", roleArn, err)
		return nil, false, err
	}

	if response == nil {
		logx.E(ctx, "assumeRole, AssumeRoleResponse is nil")
		return nil, false, errs.ErrAssumeServiceRoleFailed
	}

	responseText, _ := jsonx.MarshalToString(response)
	logx.I(ctx, "assumeRole, response: %+v", responseText)

	if response.Response == nil {
		logx.E(ctx, "assumeRole, AssumeRoleResponse.Response is nil")
		return nil, false, errs.ErrAssumeServiceRoleFailed
	}

	return response.Response, true, nil
}
