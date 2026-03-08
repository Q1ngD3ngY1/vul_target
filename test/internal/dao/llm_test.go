package dao

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"git.woa.com/baicaoyuan/moss/configx"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	llmm "git.woa.com/dialogue-platform/proto/pb-stub/llm-manager-server"
	"github.com/agiledragon/gomonkey/v2"
	. "github.com/glycerine/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

// NOCA:golint/fnsize(此处不适合拆分方法)
func Test_LLMSegmentQA(t *testing.T) {
	Convey("LLMSegmentQA", t, func() {
		Convey("test llmRsp return err", func() {
			d := new(dao)
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{}
			})
			getLLMRspErr := fmt.Errorf("mock getLLMRsp error")
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
				func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
					return &llmm.Response{}, getLLMRspErr
				},
			)
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "updateSegmentOutputs", func(ctx context.Context,
				segment *model.DocSegmentExtend) error {
				return nil
			})
			defer patches.Reset()
			doc := &model.Doc{
				ID: 0, FileName: "mock.file",
			}
			segment := &model.DocSegmentExtend{
				DocSegment: model.DocSegment{
					ID: 0, PageContent: "",
				},
			}
			_, _, err := d.LLMSegmentQA(context.Background(), doc, segment, &model.App{})
			So(err, ShouldEqual, getLLMRspErr)
		})
		Convey("test llmRsp return empty", func() {
			d := new(dao)
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{}
			})
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
				func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
					return &llmm.Response{Message: &llmm.Message{Content: ""}, Finished: true}, nil
				},
			)
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "updateSegmentOutputs", func(ctx context.Context,
				segment *model.DocSegmentExtend) error {
				return nil
			})
			defer patches.Reset()
			doc := &model.Doc{
				ID: 0, FileName: "mock.file",
			}
			segment := &model.DocSegmentExtend{
				DocSegment: model.DocSegment{
					ID: 0, PageContent: "",
				},
			}
			rsp, _, err := d.LLMSegmentQA(context.Background(), doc, segment, &model.App{})
			So(err, ShouldBeNil)
			So(rsp, ShouldBeNil)
		})
		Convey("test llmRsp return", func() {
			d := new(dao)
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{}
			})
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
				func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
					return &llmm.Response{
						Message:  &llmm.Message{Content: "1.问：腾讯会议提供哪些版本？\n答：腾讯会议提供免费版、商务版、企业版。\n2.问：腾讯会议的价格是多少？\n答：可以登录腾讯会议购买页进行购买，选择商业版或企事业单位进行购买，价格不同。\n3.问：腾讯会议和普通会议的区别是什么？\n答：腾讯会议和普通会议最大的区别在于功能和质量，腾讯会议提供高清音视频会议、不限会议时长、云服务等，能够满足企业级需求。\n4.问：腾讯的会议功能是否会免费升级？\n答：2023年4月4日，腾讯会议免费版的免费版和会员服务将进行升级，详情可以查看腾讯会议的调整说明。\n5.问：腾讯次会议的价格升级是否会影响购买？\n答：不会影响购买，可以继续购买腾讯会议商务版和企业版。"},
						Finished: true,
					}, nil
				},
			)
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "updateSegmentOutputs", func(ctx context.Context,
				segment *model.DocSegmentExtend) error {
				return nil
			})
			defer patches.Reset()
			doc := &model.Doc{
				ID: 0, FileName: "mock.file",
			}
			segment := &model.DocSegmentExtend{
				DocSegment: model.DocSegment{
					ID: 0, PageContent: "",
				},
			}
			rsp, _, err := d.LLMSegmentQA(context.Background(), doc, segment, &model.App{})
			So(err, ShouldBeNil)
			So(rsp, ShouldResemble, []*model.QA{
				{
					Question: "腾讯会议提供哪些版本？",
					Answer:   "腾讯会议提供免费版、商务版、企业版。",
				},
				{
					Question: "腾讯会议的价格是多少？",
					Answer:   "可以登录腾讯会议购买页进行购买，选择商业版或企事业单位进行购买，价格不同。",
				},
				{
					Question: "腾讯会议和普通会议的区别是什么？",
					Answer:   "腾讯会议和普通会议最大的区别在于功能和质量，腾讯会议提供高清音视频会议、不限会议时长、云服务等，能够满足企业级需求。",
				},
				{
					Question: "腾讯的会议功能是否会免费升级？",
					Answer:   "2023年4月4日，腾讯会议免费版的免费版和会员服务将进行升级，详情可以查看腾讯会议的调整说明。",
				},
				{
					Question: "腾讯次会议的价格升级是否会影响购买？",
					Answer:   "不会影响购买，可以继续购买腾讯会议商务版和企业版。",
				},
			})
		})
		Convey("test llmRsp return break rule", func() {
			d := new(dao)
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{}
			})
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
				func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
					return &llmm.Response{
						Message:  &llmm.Message{Content: "胡编乱造的outputs"},
						Finished: true,
					}, nil
				},
			)
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "updateSegmentOutputs", func(ctx context.Context,
				segment *model.DocSegmentExtend) error {
				return nil
			})
			defer patches.Reset()
			doc := &model.Doc{
				ID: 0, FileName: "mock.file",
			}
			segment := &model.DocSegmentExtend{
				DocSegment: model.DocSegment{
					ID: 0, PageContent: "",
				},
			}
			rsp, _, err := d.LLMSegmentQA(context.Background(), doc, segment, &model.App{})
			So(err, ShouldBeNil)
			So(rsp, ShouldResemble, []*model.QA{})
		})
	})
}

// NOCA:golint/fnsize(此处不适合拆分方法)
func Test_formatAnswerToQA(t *testing.T) {
	type args struct {
		ctx    context.Context
		answer string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]*model.QA
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				ctx:    context.Background(),
				answer: "1.问：你是谁？\n答：我是Stan。\n2.问：你在哪里？\n答：我在这里，在电脑上。\n3.问：你有什么特点或优点？\n答：我没有特点或优点，我只是一台电脑程序。\n4.问：你能做什么？\n答：我能够进行一些基础的计算和处理数据。\n5.问：你对人类有什么影响？\n答：我是一台电脑程序，对人类没有影响。\n11.问：你对自然有什么影响？\n答：我是一台电脑程序，对自然没有影响。\n123.问：你对公司有什么影响？\n答：我是一台电脑程序，对公司没有影响。",
			},
			want: map[string]*model.QA{
				"你是谁？": {
					Question: "你是谁？",
					Answer:   "我是Stan。",
				},
				"你在哪里？": {
					Question: "你在哪里？",
					Answer:   "我在这里，在电脑上。",
				},
				"你有什么特点或优点？": {
					Question: "你有什么特点或优点？",
					Answer:   "我没有特点或优点，我只是一台电脑程序。",
				},
				"你能做什么？": {
					Question: "你能做什么？",
					Answer:   "我能够进行一些基础的计算和处理数据。",
				},
				"你对人类有什么影响？": {
					Question: "你对人类有什么影响？",
					Answer:   "我是一台电脑程序，对人类没有影响。",
				},
				"你对自然有什么影响？": {
					Question: "你对自然有什么影响？",
					Answer:   "我是一台电脑程序，对自然没有影响。",
				},
				"你对公司有什么影响？": {
					Question: "你对公司有什么影响？",
					Answer:   "我是一台电脑程序，对公司没有影响。",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatAnswerToQA(tt.args.ctx, tt.args.answer, time.Now())
			for _, v := range got {
				fmt.Printf("format Q:%s, A:%s\n", v.Question, v.Answer)
				vv, ok := tt.want[v.Question]
				if !ok {
					t.Errorf("formatAnswerToQA() got.Question:%s is not exist", v.Question)
					return
				}
				if v.Answer != vv.Answer {
					t.Errorf("formatAnswerToQA() got.Question:%s answer:%s is not expect.expect:%s", v.Question,
						v.Answer, vv.Answer)
					return
				}
			}
		})
	}
}

func Test_Render(t *testing.T) {
	type args struct {
		ctx context.Context
		tpl string
		req any
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test",
			args: args{
				ctx: context.Background(),
				tpl: "根据下文生成多个问答对：\n{{.Content}}",
				req: &model.QAExtract{Content: "你是谁？"},
			},
			want: "根据下文生成多个问答对：\n你是谁？",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Render(tt.args.ctx, tt.args.tpl, tt.args.req)
			t.Logf("got:%s err:%+v", got, err)
			if got != tt.want {
				t.Errorf("Render() = %s, want %s", got, tt.want)
			}
		})
	}
}

func Test_DefaultSummary(t *testing.T) {
	prompt, _ := Render(context.Background(), TplDefaultSummary, SummaryContext{
		DocContent: "文档内容",
	})
	fmt.Printf("prompt: %s", prompt)
	fmt.Printf("ok")
	// t.Log(prompt, err)
}

func Test_UserSummary(t *testing.T) {
	prompt, _ := Render(context.Background(), TplUserSummary, SummaryContext{
		DocContent: "文档内容",
		Query:      "大哥，帮我总结一下啊",
	})
	fmt.Printf("prompt: %s", prompt)
	fmt.Printf("ok")
	// t.Log(prompt, err)
}

// Test_TplSimilarQuestion 拼接生成相似问prompt
func Test_TplSimilarQuestion(t *testing.T) {
	prompt, _ := Render(context.Background(), TplDefaultSimilarQuestion, SimilarQAExtract{
		Question: "你是谁",
		Answer:   "我是小助手",
	})
	fmt.Printf("prompt: %s", prompt)
	fmt.Printf("ok")
}

// TestFormatSimilarQuestions 解析相似问结果集
func TestFormatSimilarQuestions(t *testing.T) {
	tests := []struct {
		content  string
		expected []string
	}{
		{"1. 问题1\n2. 问题2", []string{"问题1", "问题2"}},
		{"1. 问题1\n\n2. 问题2", []string{"问题1", "问题2"}},
		{"1. 问题1\n2. 问题2\n3.", []string{"问题1", "问题2"}},
		{"前缀错误的的文本", nil},
		{"1. 问题1\n2. 问题2\n\\n3. 问题3", []string{"问题1", "问题2", "问题3"}},
		{"1. 问题1\n\n2. 问题2\n\\n3. 问题3", []string{"问题1", "问题2", "问题3"}},
		{"1.2.3 问题1\n2. 问题2\n\\n3. 问题3", []string{"2.3 问题1", "问题2", "问题3"}},
		{"1.2.3有空的内容\n\n2. 有不是序号开头的2\n\\n3. 问题3\n5.序号不是有序的",
			[]string{"2.3有空的内容", "有不是序号开头的2", "问题3"}},
		{"1.2.3只有一条没有换行符的", []string{"2.3只有一条没有换行符的"}},
		{"1. 你打算买哪种车型？\n2. 你打算购买哪款汽车？",
			[]string{"你打算买哪种车型？", "你打算购买哪款汽车？"}},
		{"1. 问题1\n2. 问题1",
			[]string{"问题1"}},
	}

	for _, tt := range tests {
		t.Run(tt.content, func(t *testing.T) {
			result := formatSimilarQuestions(context.Background(), tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFormatLLMResultRegexp 解析相似问结果集
func TestFormatLLMResultRegexp(t *testing.T) {
	tests := []struct {
		content        string
		expected       string
		expectedRegexp []string
	}{
		{"问：齐天大圣是什么？", "齐天大圣是什么？", []string{
			"^问：",
			"^问:",
		}},
		{"问:齐天大圣是什么？", "齐天大圣是什么？", []string{
			"^问：",
			"^问:",
		}},
		{"齐天大圣是什么？", "齐天大圣是什么？", []string{
			"^问：",
			"^问:",
		}},
		{"问：齐天大圣是什么？", "问：齐天大圣是什么？", []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.content, func(t *testing.T) {
			application := config.Application{}
			application = config.Application{DocQA: config.DocQA{SimilarQuestionLLMRegexp: tt.expectedRegexp}}
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return application
			})
			defer patches.Reset()

			result := formatLLMResultRegexp(context.Background(), tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestLLMGenerateSimilarQuestions 相似问模型返回值测试
func TestLLMGenerateSimilarQuestions(t *testing.T) {
	Convey("LLMGenerateSimilarQuestions", t, func() {
		Convey("test LLMGenerateSimilarQuestions return err", func() {
			d := new(dao)
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{}
			})
			getLLMRspErr := fmt.Errorf("mock LLMGenerateSimilarQuestions return error")
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
				func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
					return &llmm.Response{}, getLLMRspErr
				},
			)
			defer patches.Reset()
			_, _, err := d.LLMGenerateSimilarQuestions(context.Background(),
				"", "test", "", "")
			So(err, ShouldEqual, getLLMRspErr)
		})
		Convey("test LLMGenerateSimilarQuestions return empty", func() {
			d := new(dao)
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{}
			})
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
				func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
					return &llmm.Response{Message: &llmm.Message{Content: ""}, Finished: true}, nil
				},
			)
			defer patches.Reset()
			rsp, _, err := d.LLMGenerateSimilarQuestions(context.Background(), "",
				"test", "", "")
			So(err, ShouldBeNil)
			So(rsp, ShouldBeNil)
		})
		Convey("test llmRsp return", func() {
			d := new(dao)
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
				return config.Application{}
			})
			content := "1.你有什么功能\\n2.你的能力是什么\\n3.你的用途是什么\\n" +
				"4.你可以做什么\\n5.你有哪些功能\\n6.你的用途是什么\\n7.你的能力范围\\n8.你的功能是什么"
			patches.ApplyPrivateMethod(reflect.TypeOf(d), "SimpleChat",
				func(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
					return &llmm.Response{
						Message:  &llmm.Message{Content: content},
						Finished: true,
					}, nil
				},
			)
			defer patches.Reset()
			rsp, _, err := d.LLMGenerateSimilarQuestions(context.Background(),
				"", "test", "", "")
			So(err, ShouldBeNil)
			So(rsp, ShouldResemble, []string{"你有什么功能", "你的能力是什么", "你的用途是什么",
				"你可以做什么", "你有哪些功能", "你的用途是什么", "你的能力范围", "你的功能是什么"})
		})
	})
}
