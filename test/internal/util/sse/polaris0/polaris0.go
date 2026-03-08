// bot-knowledge-config-server
//
// @(#)polaris0.go  星期四, 五月 16, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

// Package polaris0 解析北极星
package polaris0

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/polaris/polaris-go/v2/api"
	"git.woa.com/polaris/polaris-go/v2/pkg/config"
	"git.woa.com/polaris/polaris-go/v2/pkg/model"
)

var (
	flowID   uint64
	consumer api.ConsumerAPI
	initAPI  sync.Once
)

func initOnce() {
	initAPI.Do(func() {
		var err error
		cfg := api.NewConfiguration()
		// 配置使用元数据路由以及就近路由
		chains := []string{config.DefaultServiceRouterDstMeta, config.DefaultServiceRouterNearbyBased}
		cfg.GetConsumer().GetServiceRouter().SetChain(chains)
		consumer, err = api.NewConsumerAPIByConfig(cfg)
		if err != nil {
			log.Errorf("NewConsumerAPIByConfig p:%+v,err:%v", consumer, err)
			panic(err)
		}
	})
}

// GetL5IpAndPort 通过L5获取ip，point
func GetL5IpAndPort(ctx context.Context, modID, cmd int, ns, env, hashKey string) (string, uint32, error) {
	instance, err := getPolarisInfo(ctx, fmt.Sprintf("%d:%d", modID, cmd), ns, env, "", hashKey)
	if err != nil {
		return "", 0, err
	}
	return instance.GetHost(), instance.GetPort(), nil
}

func getPolarisInfo(ctx context.Context, serviceName, namespace, envName, setName,
	hashKey string) (model.Instance, error) {
	getInstancesReq := &api.GetOneInstanceRequest{
		GetOneInstanceRequest: model.GetOneInstanceRequest{
			FlowID:    atomic.AddUint64(&flowID, 1),
			Namespace: namespace,
			Service:   serviceName,
		},
	}
	if len(hashKey) > 0 {
		getInstancesReq.HashKey = str2Bytes(hashKey)
		getInstancesReq.LbPolicy = api.LBPolicyRingHash
	}
	getInstancesReq.Metadata = make(map[string]string)
	// 增加元数据透传
	if len(envName) > 0 {
		getInstancesReq.Metadata["env"] = envName
	}
	if len(setName) > 0 {
		getInstancesReq.Metadata["internal-enable-set"] = "Y"
		getInstancesReq.Metadata["internal-set-name"] = setName
	}
	return getOneInstance(ctx, getInstancesReq)
}

func getOneInstance(ctx context.Context, req *api.GetOneInstanceRequest) (model.Instance, error) {
	initOnce()
	resp, err := consumer.GetOneInstance(req)
	if err != nil {
		return nil, err
	}
	if len(resp.Instances) == 0 {
		return nil, fmt.Errorf("%+v|polaris not found any endpoints", ctx)
	}
	return resp.Instances[0], nil
}

// str2Bytes string 转换为 byte 数组
func str2Bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}
