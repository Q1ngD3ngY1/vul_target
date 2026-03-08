// Package cloud TODO
package cloud

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/cloud"
	"git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common"
	tchttp "git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common/http"
	"git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common/profile"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

// DescribeNickname 获取昵称
func DescribeNickname(ctx context.Context, uin, subAccountUin string) (*cloud.NicknameInfo, error) {
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
		log.ErrorContextf(ctx, "DescribeNickname request:%+v err:%+v", request, err)
		return nil, err
	}

	// 创建common response
	response := tchttp.NewCommonResponse()

	// 发送请求
	err = client.Send(request, response)
	if err != nil {
		log.ErrorContextf(ctx, "DescribeNickname request:%+v err:%+v", request, err)
		return nil, err
	}
	log.DebugContextf(ctx, "DescribeNickname request:%+v rsp:%s", request, string(response.GetBody()))

	rsp := cloud.DescribeNicknameRsp{}
	if err = jsoniter.Unmarshal(response.GetBody(), &rsp); err != nil {
		log.ErrorContextf(ctx, "DescribeNickname response:%+v err:%+v", response, err)
		return nil, err
	}
	return &rsp.Response, nil
}

// BatchCheckWhitelist 批量检查白名单
func BatchCheckWhitelist(ctx context.Context, key, uin string) (bool, error) {
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
		log.ErrorContextf(ctx, "SetActionParameters err:%+v", err)
		return false, err
	}

	// 创建common response
	response := tchttp.NewCommonResponse()

	// 发送请求
	err = client.Send(request, response)
	if err != nil {
		log.ErrorContextf(ctx, "BatchCheckWhitelist request:%+v err:%+v", request, err)
		return false, err
	}
	log.DebugContextf(ctx, "BatchCheckWhitelist uin:%s rsp:%s", uin, string(response.GetBody()))

	// 获取响应结果
	rsp := cloud.BatchCheckWhitelistRsp{}
	if err = jsoniter.Unmarshal(response.GetBody(), &rsp); err != nil {
		log.ErrorContextf(ctx, "BatchCheckWhitelist response:%+v err:%+v", response, err)
		return false, err
	}
	if len(rsp.Response.MatchedWhitelist) == 0 {
		return false, nil
	}
	return true, nil
}
