package model

import (
	"fmt"
	"testing"

	"git.woa.com/baicaoyuan/moss/configx"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"github.com/agiledragon/gomonkey/v2"
	. "github.com/glycerine/goconvey/convey"
)

func TestApp_GetModelsConfig(t *testing.T) {
	Convey("LLMSegmentQA", t, func() {
		Convey("test llmRsp return err", func() {
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				cfg := config.Application{}
				knowledgeQaAppModel := make(config.AppModel)
				knowledgeQaAppModel["aaa"] = config.AppModelDetail{}
				cfg.RobotDefault.AppModelConfig = config.AppModelConfig{
					KnowledgeQaAppModel: knowledgeQaAppModel,
					SummaryAppModel:     config.AppModel{},
					ClassifyAppModel:    config.AppModel{},
				}
				return cfg
			})
			defer patches.Reset()
			app := &App{
				AppDB: AppDB{
					AppType: KnowledgeQaAppType,
				},
				PreviewDetails: AppDetailsConfig{
					BaseConfig: BaseConfig{},
				},
			}
			m, _, _ := app.GetModels(AppTestScenes)
			fmt.Println(m)

			n, _, _ := app.GetModels(AppTestScenes)
			fmt.Println(n)
		})
	})

}
