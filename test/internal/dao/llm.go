package dao

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/client"
	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.code.oa.com/trpc-go/trpc-go/naming/selector"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/go-comm/clues"
	commutils "git.woa.com/dialogue-platform/go-comm/utils"
	llmm "git.woa.com/dialogue-platform/proto/pb-stub/llm-manager-server"
	"go.opentelemetry.io/otel/trace"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// WithTrpcSelector 还原为 trpc 默认 Selector
func WithTrpcSelector() client.Option {
	return func(o *client.Options) {
		o.Selector = &selector.TrpcSelector{}
	}
}

// Render 模版渲染
func Render(ctx context.Context, tpl string, req any) (string, error) {
	// 去除模版每行中的空白符
	lines := strings.Split(tpl, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	tpl = strings.Join(lines, "\n")

	e, err := template.New("").Parse(tpl)
	if err != nil {
		log.ErrorContextf(ctx, "Compile template失败  tpl:%s err:%+v", tpl, err)
		return "", err
	}
	b := &bytes.Buffer{}
	if err := e.Execute(b, req); err != nil {
		log.ErrorContextf(ctx, "Execute template失败 tpl:%s, req:%+v err:%+v", tpl, req, err)
		return "", err
	}
	return b.String(), nil
}

// LLMSegmentQA 获取段落的问答对
func (d *dao) LLMSegmentQA(
	ctx context.Context, doc *model.Doc, segment *model.DocSegmentExtend, app *model.App,
) ([]*model.QA, *llmm.StatisticInfo, error) {
	log.InfoContextf(ctx, "LLMSegmentQA|doc:%+v, segment:%+v", doc, segment)
	ctx = pkg.WithSpaceID(ctx, app.SpaceID)
	start := time.Now()
	m, err := app.GetQAExtractModel(model.AppReleaseScenes)
	if err != nil {
		return nil, nil, err
	}

	prompt, err := Render(ctx, m.Prompt, model.QAExtract{Content: segment.OrgData})
	if err != nil {
		return nil, nil, err
	}

	rsp, err := d.SimpleChat(ctx, m.ServiceName, &llmm.Request{
		RequestId:   trace.SpanContextFromContext(ctx).TraceID().String(),
		ModelName:   m.ModelName,
		AppKey:      strconv.FormatUint(app.ID, 10),
		PromptType:  llmm.PromptType_TEXT,
		RequestType: llmm.RequestType_OFFLINE,
		Messages:    []*llmm.Message{{Role: llmm.Role_USER, Content: prompt}},
	})
	if err != nil {
		log.ErrorContextf(ctx, "LLM Segment QA error: %+v, doc: %+v", err, doc)
		return nil, nil, err
	}
	log.InfoContextf(ctx, "LLMSegmentQA|SimpleChat result:%+v", rsp)
	// 文档已过期
	if time.Unix(0, 0).Before(segment.ExpireEnd) && time.Now().After(segment.ExpireEnd) {
		log.ErrorContextf(ctx, "LLM Segment QA error: %+v, doc: %+v", errs.ErrDocToQaExpiredFail, doc)
		return nil, nil, errs.ErrDocToQaExpiredFail
	}
	defer func() { _ = d.updateSegmentOutputs(ctx, segment, doc.RobotID) }()
	segment.Outputs = rsp.GetMessage().GetContent()
	segment.CostTime = time.Since(start).Seconds()
	if segment.Outputs == "" {
		log.DebugContextf(ctx, "getLLMRsp output is empty docID:%d fileName:%s segmentID:%d question:%s",
			doc.ID, doc.FileName, segment.ID, segment.OrgData)
		return nil, nil, err
	}
	return formatAnswerToQA(ctx, segment.Outputs, segment.ExpireEnd), rsp.GetStatisticInfo(), nil
}

// SimpleChat 非流式调用 LLM
func (d *dao) SimpleChat(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error) {
	opts := []client.Option{WithTrpcSelector()}
	if serviceName != "" {
		opts = append(opts, client.WithServiceName(serviceName))
	}
	if req.GetModelName() == "" || req.GetModelName() == "finance-13b" { // 金融大模型使用 cs-normal
		req.ModelName = "cs-normal"
	}
	log.InfoContextf(ctx, "Simple LLM request: %s", req)
	rsp, err := d.llmmCli.SimpleChat(ctx, req, opts...)
	if err != nil {
		log.ErrorContextf(ctx, "Invoke SimpleChat error: %+v, req: %+v", err, req)
		return nil, err
	}

	if rsp.GetCode() != 0 {
		err := terrs.Newf(int(rsp.GetCode()), rsp.GetErrMsg())
		log.ErrorContextf(ctx, "Invoke SimpleChat error: %+v, req: %+v", err, req)
		return nil, err
	}

	return rsp, nil
}

// Chat 流式调用 LLM
func (d *dao) Chat(ctx context.Context, req *llmm.Request, ch chan *llmm.Response) error {
	var last *llmm.Response
	defer close(ch)
	defer func() {
		if last != nil && !last.GetFinished() { // 确保流式输出终止 final
			last.Finished = true
			ch <- last
		}
	}()

	opts := []client.Option{WithTrpcSelector()}
	cli, err := d.llmmCli.Chat(ctx, opts...)
	if err != nil {
		log.ErrorContextf(ctx, "New chat stream client error: %+v", err)
		return err
	}
	defer cli.CloseSend()

	idx, start := 0, time.Now()
	if req.GetModelName() == "" || req.GetModelName() == "finance-13b" { // 金融大模型使用 cs-normal
		req.ModelName = "cs-normal"
	}
	log.InfoContextf(ctx, "Stream LLM request: %s", commutils.ToJsonString(req))
	clues.AddTrackData(ctx, "Chat.req", req)
	if err := cli.Send(req); err != nil {
		log.ErrorContextf(ctx, "Send chat stream request error: %+v, req: %+v", err, req)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			req.PromptType = llmm.PromptType_CMD_STOP
			_ = cli.Send(req)
			return nil
		default:
			rsp, err := cli.Recv()
			if idx%10 == 0 { // 每10条 打一条日志
				log.InfoContextf(ctx, "RESP|chat[%d] %s, time cost:%s,ERR: %v ",
					idx, commutils.ToJsonString(rsp), time.Since(start), err)
			}
			if err != nil {
				if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
					req.PromptType = llmm.PromptType_CMD_STOP
					_ = cli.Send(req)
					log.WarnContextf(ctx, "Chat canceled: %v, send stop: %s", err, commutils.ToJsonString(req))
					return nil
				}
				log.ErrorContextf(ctx, "Read model response error: %+v, req: %+v", err, req)
				return err
			}

			if rsp.GetCode() != 0 {
				log.ErrorContextf(ctx, "Read model response biz error, req: %s, rsp: %s",
					commutils.ToJsonString(req), commutils.ToJsonString(rsp))
				return err
			}
			idx++
			ch <- rsp
			last = rsp
			if rsp.GetFinished() {
				return nil
			}
		}
	}
}

// GetModelPromptLimit 获取模型Prompt长度限制
func (d *dao) GetModelPromptLimit(ctx context.Context, corpID uint64, modelName string) int {
	t0 := time.Now()
	req := &llmm.GetModelRequest{
		RequestId:   trace.SpanContextFromContext(ctx).TraceID().String(),
		ModelName:   modelName,
		RequestType: llmm.RequestType_ONLINE,
	}

	rsp, err := knowClient.GetModelInfo(ctx, corpID, modelName)
	clues.AddTrack4RPC(ctx, "llm.GetModelInfo", req, rsp, err, t0)
	if err != nil || rsp == nil {
		log.ErrorContextf(ctx, "Get model info error: %+v, req: %+v, time cost: %v", err, req, time.Since(t0))
		return config.GetDefaultTokenLimit() // 该接口必须成功返回一个值
	}
	log.InfoContextf(ctx, "Get model info rsp: %s, req: %s, time cost: %v",
		util.Object2String(rsp), util.Object2String(req), time.Since(t0))
	if rsp.Length == 0 {
		return config.GetDefaultTokenLimit()
	}
	return int(rsp.Length)
}

// GetModelInfo 获取模型信息
func (d *dao) GetModelInfo(ctx context.Context, modelName string) (*llmm.GetModelResponse, error) {
	t0 := time.Now()
	req := &llmm.GetModelRequest{
		RequestId:   trace.SpanContextFromContext(ctx).TraceID().String(),
		ModelName:   modelName,
		RequestType: llmm.RequestType_ONLINE,
	}

	rsp, err := d.llmmCli.GetModelInfo(ctx, req)
	clues.AddTrack4RPC(ctx, "llm.GetModelInfo", req, rsp, err, t0)
	if err != nil || rsp == nil || rsp.Code != 0 {
		log.ErrorContextf(ctx, "Get model info error: %+v, req: %+v, rsp: %+v time cost: %v",
			err, req, rsp, time.Since(t0))
		return nil, fmt.Errorf("get model info error")
	}
	log.InfoContextf(ctx, "Get model info rsp: %s, req: %s, time cost: %v",
		util.Object2String(rsp), util.Object2String(req), time.Since(t0))
	return rsp, nil
}

// LLMGenerateSimilarQuestions 生成相似问题
// 这个函数没找调用方，先不加spaceID
func (d *dao) LLMGenerateSimilarQuestions(ctx context.Context, appKey, modelName, question, answer string) ([]string,
	*llmm.StatisticInfo, error) {
	start := time.Now()
	var similarQuestions []string
	prompt := config.App().DocQA.SimilarQuestionPrompt
	if prompt == "" {
		prompt = TplDefaultSimilarQuestion
	}
	prompt, err := Render(ctx, config.App().DocQA.SimilarQuestionPrompt,
		SimilarQAExtract{Question: question, Answer: answer})
	if err != nil {
		log.ErrorContextf(ctx, "LLMGenerateSimilarQuestions Render failed, err:%+v", err)
		return similarQuestions, nil, err
	}
	if modelName == "" {
		log.ErrorContextf(ctx, "LLMGenerateSimilarQuestions failed, modelName is empty")
		return similarQuestions, nil, errs.ErrNotInvalidModel
	}
	req := &llmm.Request{
		RequestId:   trace.SpanContextFromContext(ctx).TraceID().String(),
		ModelName:   modelName,
		AppKey:      appKey,
		PromptType:  llmm.PromptType_TEXT,
		RequestType: llmm.RequestType_ONLINE,
		Messages:    []*llmm.Message{{Role: llmm.Role_USER, Content: prompt}},
	}
	// 带思维链的要加上no think
	if config.IsDeepSeekModeAndHasThink(modelName) {
		log.InfoContextf(ctx, "model_name:%s,IsDeepSeekModeAndHasThink|nothink", req.ModelName)
		msg := &llmm.Message{Role: llmm.Role_ASSISTANT, Content: config.App().DeepSeekConf.NoThinkPrompt}
		req.Messages = append(req.Messages, msg)
		req.ModelParams = config.App().DeepSeekConf.NoThinkModelParams
	}

	rsp, err := d.SimpleChat(ctx, "", req)
	if err != nil {
		log.ErrorContextf(ctx, "LLMGenerateSimilarQuestions error: %+v, prompt: %s", err, prompt)
		return similarQuestions, nil, err
	}
	costTime := time.Since(start).Seconds()
	similarQuestions = formatSimilarQuestions(ctx, rsp.GetMessage().GetContent())
	log.InfoContextf(ctx, "LLMGenerateSimilarQuestions|SimpleChat prompt:%+v rsp:%+v costTime:%+v",
		prompt, rsp, costTime)
	return similarQuestions, rsp.GetStatisticInfo(), nil
}

// SimilarQAExtract 相似问提取
type SimilarQAExtract struct {
	Question string
	Answer   string
}

// formatSimilarQuestions 返回结果格式化为相似问集合
func formatSimilarQuestions(ctx context.Context, content string) []string {
	log.DebugContextf(ctx, "formatSimilarQuestions question:%s", content)
	// 将 "\\n" 替换为 "\n"
	content = strings.ReplaceAll(content, "\\n", "\\\n")
	// 然后按 "\n" 分割字符串
	lines := strings.Split(content, "\n")
	var similarQuestions []string
	questionSet := make(map[string]bool) // 用于去重的集合
	lastNumber := 0
	for k, line := range lines {
		if line == "" {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d is empty", k)
			continue
		}
		// 如果这一行不是以数字+符号点开头的，丢弃掉
		if !regexp.MustCompile(`^\d+\.`).MatchString(line) {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d lineContent:%s notNumStart", k, line)
			continue
		}
		// 检查数字是否有序
		numberStr := regexp.MustCompile(`^\d+`).FindString(line)
		number, _ := strconv.Atoi(numberStr)
		if number != lastNumber+1 {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d lineContent:%s lastNumber:%d NumIndexFail",
				k, line, lastNumber)
			continue
		}
		lastNumber = number
		// 只截取掉第一个数字和点，保留后面的内容
		line = regexp.MustCompile(`^\d+\.`).ReplaceAllString(line, "")
		line = strings.TrimSpace(line)
		if line == "" {
			log.DebugContextf(ctx, "formatSimilarQuestions line:%d lineContent:%s is empty", k, line)
			continue
		}
		// 检查是否已存在相同问题
		if !questionSet[line] {
			questionSet[line] = true
			similarQuestions = append(similarQuestions, line)
		} else {
			log.DebugContextf(ctx, "formatSimilarQuestions duplicate question found: %s", line)
		}
	}
	return similarQuestions
}

// formatLLMResultRegexp 兼容模型效果问题，工程侧对已有case做过滤
func formatLLMResultRegexp(ctx context.Context, line string) string {
	// 配置中读取,已知需过滤的模型效果case
	regexes := config.App().DocQA.SimilarQuestionLLMRegexp
	if len(regexes) == 0 {
		return line
	}
	// 编译所有正则表达式
	compiledRegexes := make([]*regexp.Regexp, 0)
	for _, regex := range regexes {
		re, err := regexp.Compile(regex)
		if err != nil {
			log.WarnContextf(ctx, "formatLLMResultRegexp regex:%+v Compile err:%+v", regex, err)
			continue
		}
		compiledRegexes = append(compiledRegexes, re)
	}
	// 使用编译好的正则表达式替换字符串
	for _, re := range compiledRegexes {
		if re != nil {
			line = re.ReplaceAllString(line, "")
		}
	}
	return line
}

// formatAnswerToQA 把返回格式化为QA
func formatAnswerToQA(ctx context.Context, answer string, expireEnd time.Time) []*model.QA {
	log.DebugContextf(ctx, "formatAnswerToQA answer:%s", answer)
	qaModels := make([]*model.QA, 0)
	answerReg := regexp.MustCompile(config.App().DocQA.QuestionRegexp)
	qaPairs := answerReg.Split(answer, -1)
	for _, qa := range qaPairs {
		if qa == "" {
			continue
		}
		anPairs := strings.SplitN(qa, config.App().DocQA.AnswerRegexp, 2)
		if len(anPairs) != 2 {
			continue
		}
		qaModels = append(qaModels, &model.QA{
			Question:    strings.TrimSpace(strings.TrimRight(anPairs[0], "\n")),
			Answer:      strings.TrimSpace(strings.TrimRight(anPairs[1], "\n")),
			ExpireStart: time.Now(),
			ExpireEnd:   expireEnd,
		})
	}
	return qaModels
}

// TplDefaultSummary 上传文档无Query时的默认摘要模板
const TplDefaultSummary = `
	# Role: 文档阅读助手
	## Profile
	- Language: 中文
	- Description: 你是一个专业的文档阅读助手，能够准确且清晰地分行分段总结文档和报告的核心要点，并用加粗形式突出显示。
	### Skill
	1. 你擅长识别文档类型（如新闻报道、产品说明书、政府报告等）。
	2. 你擅长简洁概括文档的核心主题。
	3. 你擅长总结文档的核心要点，包括关键数字、事件和观点，并以Markdown格式加粗显示。
	## Rules
	1. 输出结果需要包含“文档类型”、“核心主题”、分行分段总结的核心要点。可按如下格式输出：“这篇文档是【文档类型】，主要关注【主题】。以下是核心要点总结：\n1. **要点1：要点主旨概括**<br>- &ensp&ensp换行缩进介绍与要点相关的核心信息<br><br>2. **要点2：要点主旨概括**<br>- &ensp&ensp换行缩进介绍与要点相关的核心信息。<br><br>总结：【简要总结结论】”。
	## Workflow
	1. 首先，判断文档的类型（如新闻报道、产品说明书、政府报告等）。
	2. 然后，总结文档内容的核心主题。
	3. 其次，总结文档的核心要点，要点输出格式需满足上述规则，使用Markdown格式，加粗要点，并提供简要概括。
	4. 最后，检查回答中的要点是否已经覆盖文档的全部核心内容，补充缺失的核心要点。检查回答的格式是否符合要求，规范化格式保证阅读的美观性。
	## Initialization
	作为一个<Role>，你必须遵守<Rules>，你必须用默认<Language>和用户交谈，你必须依据<Workflow>回答用户。
	---
	##待分析的文档内容
	---
	{{.DocContent}}
`

// TplUserSummary 上传文档有Query时的用户自定义摘要模板
const TplUserSummary = `
	{{.DocContent}}
	{{.Query}}
`

// SummaryContext TODO
type SummaryContext struct {
	DocContent string
	Query      string
}

// TplDefaultSimilarQuestion 生成相似问默认Prompt模板
const TplDefaultSimilarQuestion = `你是一个优秀的问题改写专家，你擅长根据给定的问答对，给出和原问题问题回答一样的相似问题，可以有一定的省略。我会给你【问答对】，理解问题和答案，并对问题进行相似问改写，给出5到10个相似问。
		注意：
		1. 得到的相似问需要和原问题有着一致的目的，问题的答案都可以用当前给的答案解决。
		2. 得到的相似问需要遵从原始问题，语义上不能扩展。
		
		以下给你一个例子：
		# 例1
		【问答对】
		user：大参林八项规定
		bot：1、对公司：要忠诚，主动宣传公司，对外、对内均拒绝说公司不足，如有意见应主动向上级或越级反映；要自觉维护公司利益，不私自收受利益关联方贿赂，不侵占公司财物，也绝不允许我们的同事有这种行为；不占用公司资源办私事，如确有必要必须如实申报经上级批准；
		2、对工作：反对官僚主义，同级间有问题应直接沟通；学会复杂问题简单化，不搞文山会海、小会 不超过一小时；各种制度流程尽量用一到两页纸写完；
		3、对业绩：结果导向，不做无谓的劳动，珍惜自己的时间，更珍惜他人时间；
		4、对舆论：不传播未经证实的谣言，不侵犯他人隐私；
		5、对上司：不当面奉承，不送礼，不迎来送往：也不能背后议论上司的不足，有问题要当面说，也可越级汇报；
		6、对下级：以身作则，严于律己，严于律人，用人唯贤；
		7、对同事：要真诚，不说影响团结的话，当面不能冷嘲热讽，背后更不能说三道四、搬弄是非：不拉帮结派；
		8、对客户：尊重、共赢，实事求是。
		【你的回答】
		1.八项规定是什么
		2.公司的八项规定是什么
		3.大参林的八项规定是什么
		4.企业的八项规定是什么
		5.集团的八项规定是什么
		6.请输出八项规定
		7.八项规定
		8.八项规定？
		
		现在你帮我回答：
		【问答对】
		user:{{.Question}}
		bot:{{.Answer}}
		【你的回答】`

const TplKBDirNameDocNameSummary = `
		你是一个专业的摘要生成专家，我会给你一些文件，请你帮我生成一个可以高度概括所有文件的100字以内的简短的摘要。如果文件夹名没有意义，请忽略文件夹名。
		给你举个例子:
		<文件夹名>
		1
		</文件夹名>
		<文件列表>
		1. 文件名: 全新BJ40城市猎人版-第二次OTA升级用户明细.xlsx。文件内容摘要: 这是一个包含全新BJ40城市猎人版第二次OTA升级用户详细信息的Excel文件，记录了客户的VIN码、姓名、手机号、车型名称和产品名称。
		2. 文件名: 2020款BJ40雨林穿越-车辆用户手册.pdf。文件内容摘要: 这是一份2020款BJ40雨林穿越版车辆的用户手册。
		3. 文件名: 附件：魔方第三次OTA升级-短信发送用户明细.xlsx。文件内容摘要: 这是一份魔方第三次OTA升级短信发送用户明细表，包含客户信息、车辆信息和销售日期等数据。
		4. 文件名: 2020款X7-车辆配置表.xlsx。文件内容摘要: 这是一份2020款X7车辆的详细配置表，列出了不同车型的基本参数、尺寸、性能、灯光配置、内饰材质等信息。
		5. 文件名: 全新BJ40刀锋英雄版版-车型道路救援服务权益细则.pdf。文件内容摘要: 全新BJ40刀锋英雄版车型为首任非营运车主提供5年免费道路救援服务，包括搭电、换胎、充气、添加燃油、取送备用钥匙、拖车和困境救援等，以及增值服务权益和特殊情况下的救援服务。
		6. 文件名: 魔方-第二次OTA产品说明-202211.pdf。文件内容摘要: 魔方2022年第二次OTA产品说明文档，介绍了新增的遥控泊车、人脸识别、智能情景模式等功能，以及语音、HUD显示、文本话术和仪表显示的优化。
		7. 文件名: （已过期）老三包法2013.10.1（已过期）.docx。文件内容摘要: 这是一份关于《家用汽车产品修理、更换、退货责任规定》的文件，详细规定了家用汽车产品的三包责任、生产者、销售者和修理者的义务，以及三包责任免除、争议处理和罚则等内容。
		8. 文件名: 油车终身质保车辆明细.xlsx。文件内容摘要: 这是一个包含X7车型车辆的车架号和车型信息的明细表。
		9. 文件名: 2020款BJ40城市猎人版-车辆配置表.xlsx。文件内容摘要: 这是一份2020款BJ40城市猎人版车型的详细配置表，包括动力、变速器、驱动方式、版型、价格以及车辆的基本参数和性能数据。
		10. 文件名: 全新BJ40刀锋英雄版-用户电话访谈名单.xlsx。文件内容摘要: 这是一个包含全新BJ40刀锋英雄版用户电话访谈名单的Excel文件，记录了不同战区、经销商、销售顾问以及车主的姓名和性别信息。
		</文件列表>
		<摘要>
		北汽售前文件夹包含多款车型的用户信息、配置表和服务细则。文件涵盖全新BJ40城市猎人版和刀锋英雄版的OTA升级用户明细、车辆配置、道路救援服务权益，以及2020款BJ40雨林穿越版和X7车型的用户手册和配置表。此外，还包括魔方车型的OTA产品说明和短信发送用户明细，以及油车终身质保车辆明细和已过期的三包法文件。
		</摘要>
		<文件夹名>
		北汽售前文件
		</文件夹名>
		
		现在请你帮我生成摘要:
		<文件夹名>{{.DirName}}</文件夹名>
		<文件列表>{{.FileInfo}}</文件列表>
		<摘要>
`
