package rpc

import (
	"context"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/glycerine/goconvey/convey"

	"git.woa.com/adp/common/x/configx"
	"git.woa.com/adp/kb/kb-config/internal/config"
)

func TestDescribeNickname(t *testing.T) {
	Convey("test", t, func() {
		Convey("test", func() {
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{
					CloudAPIs: config.CloudAPIs{
						SecretID:    "",
						SecretKey:   "",
						AccountHost: "account.tencentcloudapi.com",
						Region:      "",
						Version:     "2018-12-25",
					},
				}
			})
			defer patches.Reset()
			rsp, err := rpcInstance.DescribeNickname(context.Background(), "100034812935", "100034812935")
			So(err, ShouldBeNil)
			t.Logf("%+v", rsp)
		})
	})
}

func TestBatchCheckWhitelist(t *testing.T) {
	Convey("test", t, func() {
		Convey("test", func() {
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{
					CloudAPIs: config.CloudAPIs{
						SecretID:    "",
						SecretKey:   "",
						AccountHost: "account.tencentcloudapi.com",
						Region:      "",
						Version:     "2018-12-25",
					},
				}
			})
			defer patches.Reset()
			rsp, err := rpcInstance.BatchCheckWhitelist(context.Background(), "", "")
			So(err, ShouldBeNil)
			t.Logf("%+v", rsp)
		})
	})
}
