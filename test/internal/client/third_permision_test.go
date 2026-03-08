package client

import (
	"context"
	"fmt"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"testing"
	"time"
)

func TestCheckKnowledgePermission(t *testing.T) {
	ctx := context.Background()
	url := "http://so-test.woa.com/openapi/v1/lke/auth"
	header := map[string]string{
		"ISearch-From":  "lke",
		"ISearch-Token": "NmI1NTBmNGQtNTUzOS00N2ViLWEzMjYtZDRhNjkwMGY1ZjY3",
	}
	requestId := time.Now().Format("20060102150405")
	req := &CheckKnowledgePermissionReq{
		RequestId: requestId,
		CustomerKnowledgeIds: []string{
			"km::post::551046",
			"yunzhi::course::f5de6c0c767811ef9ee81aa2468d7463_dd773068767811ef8db10aaf13da51fd",
			"yunzhi::doc::d7e061a89b9111eb9ad532974ea376d4",
			"eknow::eknow::hr-eknow-585-3-174",
			"tapd::tapd_wiki::1210135051000024405",
			"Q-Learning::网络课::1_1833",
		},
		CustomerUserId: "libli",
	}
	thirdPermissionConfig := utilConfig.ThirdPermissionConfig{
		Enable:  true,
		Url:     url,
		Header:  header,
		Timeout: 500,
		Retry:   2,
	}
	rsp, err := CheckKnowledgePermission(ctx, thirdPermissionConfig, req)
	fmt.Printf("rsp:%+v err:%+v", rsp, err)
}
