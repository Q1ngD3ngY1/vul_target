package cloud

import (
	"context"
	"testing"

	"git.woa.com/baicaoyuan/moss/configx"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"github.com/agiledragon/gomonkey/v2"
	. "github.com/glycerine/goconvey/convey"
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
			rsp, err := DescribeNickname(context.Background(), "100034812935", "100034812935")
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
			rsp, err := BatchCheckWhitelist(context.Background(), "", "")
			So(err, ShouldBeNil)
			t.Logf("%+v", rsp)
		})
	})
}
