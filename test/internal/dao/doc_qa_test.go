package dao

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"reflect"
	"testing"

	"git.woa.com/ivy/protobuf/trpc-go/qbot/finance/finance"
	"github.com/golang/mock/gomock"

	"git.woa.com/baicaoyuan/moss/configx"

	"github.com/bwmarrin/snowflake"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/billing"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	llmm "git.woa.com/dialogue-platform/proto/pb-stub/llm-manager-server"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

// TestGenerateSimilarQuestions 测试 GenerateSimilarQuestions 函数
func TestGenerateSimilarQuestions(t *testing.T) {
	tests := []struct {
		name          string
		app           *model.App
		question      string
		answer        string
		mockModelName string
		mockReturn    []string
		mockError     error
		expectedError error
		expectedList  []string
	}{
		{
			name:          "test question ErrGenerateSimilarParams",
			app:           &model.App{},
			question:      "",
			answer:        "",
			mockModelName: "testModelName",
			expectedError: errs.ErrGenerateSimilarParams,
		},
		{
			name:          "test ModelName ErrNotInvalidModel",
			app:           &model.App{},
			question:      "test",
			answer:        "",
			mockModelName: "",
			expectedError: errs.ErrNotInvalidModel,
		},
		{
			name:          "test mockReturn ok",
			app:           &model.App{},
			question:      "test",
			answer:        "",
			mockModelName: "",
			expectedError: nil,
			mockReturn:    []string{"test1", "test2"},
			expectedList:  []string{"test1", "test2"},
		},
		{
			name:          "test return empty",
			app:           &model.App{},
			question:      "test",
			answer:        "",
			mockModelName: "",
			expectedError: nil,
			mockReturn:    []string{},
			expectedList:  []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := new(dao)
			if len(tt.mockReturn) > 0 {
				tt.app = &model.App{}
				knowledgeQaConfig := &model.KnowledgeQaConfig{}
				appModel := config.AppModel{}
				appModels := make(map[string]*config.AppModelInfo)
				appModels["normal_message"] = &config.AppModelInfo{Name: "mockModelName"}
				if info, exists := appModels["normal_message"]; exists {
					appModel["normal_message"] = config.AppModelDetail{
						ModelName: info.Name,
					}
				}
				knowledgeQaConfig.Model = appModel
				tt.app.PreviewDetails.AppConfig.KnowledgeQaConfig = knowledgeQaConfig

				patches := gomonkey.NewPatches()
				patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
					func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
						return &llmm.Response{Message: &llmm.Message{Content: ""}, Finished: true}, nil
					},
				)
				defer patches.Reset()
				patchesLLM := gomonkey.NewPatches()
				patchesLLM.ApplyPrivateMethod(reflect.TypeOf(d), "LLMGenerateSimilarQuestions",
					func(ctx context.Context, appKey string,
						modelName string, question string, answer string) ([]string, *llmm.StatisticInfo,
						error) {
						return tt.mockReturn, nil, tt.mockError
					})
				defer patchesLLM.Reset()

				patchesReport := gomonkey.NewPatches()
				patchesReport.ApplyPrivateMethod(reflect.TypeOf(d), "reportSimilarQuestionsTokenDosage",
					func(ctx context.Context, corpID uint64, tokenStatisticInfo *llmm.StatisticInfo,
						dosage *billing.TokenDosage) error {
						return tt.mockError
					})
				defer patchesReport.Reset()
			} else {
				tt.app = &model.App{}
				knowledgeQaConfig := &model.KnowledgeQaConfig{}
				if tt.name != "test ModelName ErrNotInvalidModel" {
					appModel := config.AppModel{}
					appModels := make(map[string]*config.AppModelInfo)
					appModels["normal_message"] = &config.AppModelInfo{Name: "mockModelName"}
					if info, exists := appModels["normal_message"]; exists {
						appModel["normal_message"] = config.AppModelDetail{
							ModelName: info.Name,
						}
					}
					knowledgeQaConfig.Model = appModel
				}

				tt.app.PreviewDetails.AppConfig.KnowledgeQaConfig = knowledgeQaConfig
				patchesLLM := gomonkey.NewPatches()
				patchesLLM.ApplyPrivateMethod(reflect.TypeOf(d), "LLMGenerateSimilarQuestions",
					func(ctx context.Context, appKey string,
						modelName string, question string, answer string) ([]string, *llmm.StatisticInfo,
						error) {
						return tt.mockReturn, nil, tt.mockError
					})
				defer patchesLLM.Reset()
			}
			similarList, err := d.GenerateSimilarQuestions(context.Background(), tt.app, tt.question, tt.answer)
			assert.Equal(t, tt.expectedError, err)
			assert.Equal(t, tt.expectedList, similarList)
		})
	}
}

// TestReportSimilarQuestionsTokenDosage 测试 reportSimilarQuestionsTokenDosage 函数
func TestReportSimilarQuestionsTokenDosage(t *testing.T) {
	tests := []struct {
		name               string
		corpID             uint64
		tokenStatisticInfo *llmm.StatisticInfo
		mockError          error
		dosage             *billing.TokenDosage
	}{
		{
			name:               "test ReportSimilarQuestions StatisticInfo is empty",
			tokenStatisticInfo: &llmm.StatisticInfo{},
			mockError:          nil,
			dosage:             &billing.TokenDosage{},
		},
		{
			name:               "test ReportSimilarQuestions StatisticInfo not empty err",
			tokenStatisticInfo: &llmm.StatisticInfo{InputTokens: 1, OutputTokens: 2},
			mockError:          errs.ErrSystem,
			dosage:             &billing.TokenDosage{},
		},
		{
			name:               "test ReportSimilarQuestions ok",
			tokenStatisticInfo: &llmm.StatisticInfo{InputTokens: 1, OutputTokens: 2},
			mockError:          nil,
			dosage:             &billing.TokenDosage{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := new(dao)
			patchesGenerateSeqID := gomonkey.NewPatches()
			patchesGenerateSeqID.ApplyPrivateMethod(reflect.TypeOf(d.uniIDNode), "Generate",
				func() snowflake.ID {
					return snowflake.ID(1)
				})
			patchesGenerateSeqID.ApplyPrivateMethod(reflect.TypeOf(d), "GenerateSeqID",
				func() uint64 {
					return uint64(1)
				})
			defer patchesGenerateSeqID.Reset()

			patchesGetCorpByID := gomonkey.NewPatches()
			patchesGetCorpByID.ApplyPrivateMethod(reflect.TypeOf(d), "GetCorpByID",
				func(ctx context.Context, id uint64) (*model.Corp, error) {
					return nil, tt.mockError
				})
			defer patchesGetCorpByID.Reset()

			patchesTokenDosage := gomonkey.NewPatches()
			patchesTokenDosage.ApplyPrivateMethod(reflect.TypeOf(d), "ReportSimilarQuestionsTokenDosage",
				func(ctx context.Context, corp *model.Corp, dosage *billing.TokenDosage) error {
					return tt.mockError
				})
			defer patchesTokenDosage.Reset()

			err := d.reportSimilarQuestionsTokenDosage(context.Background(), tt.corpID, tt.tokenStatisticInfo, tt.dosage)
			assert.Equal(t, tt.mockError, err)
		})
	}
}

// TestFinanceReportSimilarQuestionsTokenDosage 测试 ReportSimilarQuestionsTokenDosage 函数
func TestFinanceReportSimilarQuestionsTokenDosage(t *testing.T) {
	tests := []struct {
		name               string
		corp               *model.Corp
		tokenStatisticInfo *billing.TokenDosage
		mockError          error
	}{
		{
			name:               "test IsFinanceDisabled is true",
			corp:               &model.Corp{},
			tokenStatisticInfo: &billing.TokenDosage{},
			mockError:          nil,
		},
		{
			name: "test ReportSimilarQuestionsTokenDosage ok",
			corp: &model.Corp{},
			tokenStatisticInfo: &billing.TokenDosage{AppID: 1, AppType: DocExtractSimilarQAType,
				InputDosages: []int{1}, OutputDosages: []int{2}},
			mockError: nil,
		},
		{
			name:               "test ReportSimilarQuestionsTokenDosage err",
			corp:               &model.Corp{},
			tokenStatisticInfo: &billing.TokenDosage{},
			mockError:          errs.ErrSystem,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := new(dao)
			application := config.Application{}
			if tt.name == "test IsFinanceDisabled is true" {
				application = config.Application{Finance: config.Finance{Disabled: true}}
			} else {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockClient := finance.NewMockFinanceClientProxy(ctrl)
				mockClient.EXPECT().ReportDosage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, tt.mockError)
				d.qBotFinanceClient = mockClient
			}
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return application
			})
			defer patches.Reset()

			err := d.ReportSimilarQuestionsTokenDosage(context.Background(), tt.corp, tt.tokenStatisticInfo)
			assert.Equal(t, tt.mockError, err)
		})
	}
}
