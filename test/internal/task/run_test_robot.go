package task

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"

	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go"
	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/linker"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	sse "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util/sse/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util/sse/event"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

const (
	// ErrNoBalanceCode sse计费余量错误code
	ErrNoBalanceCode = 460032
	// TestStatusDone 任务完成状态
	TestStatusDone = "TestDone"
)

// RunScheduler 执行评测任务
type RunScheduler struct {
	task           task_scheduler.Task
	params         model.TestParams
	dao            dao.Dao
	sseClient      *sse.SSEClient
	message        string
	filterTopN     uint32
	filters        []*retrieval.SearchVectorReq_Filter
	robot          *model.AppDB
	app            *admin.GetAppInfoRsp
	errRecordIDs   []string
	QuestionInfos  sync.Map
	BillingError   string
	OldTestDoneNum int
}

func initRunScheduler() {
	task_scheduler.Register(
		model.TaskTest,
		func(t task_scheduler.Task, params model.TestParams) task_scheduler.TaskHandler {
			return &RunScheduler{
				task:   t,
				params: params,
				dao:    dao.New(),
			}
		},
	)
}

// Prepare 数据准备
func (r *RunScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	log.InfoContextf(ctx, "task(run test %d) Prepare start", r.params.TestID)
	// 查询待评测内容
	records, err := r.dao.GetRecordByTestIDs(ctx, r.params.TestID)
	if err != nil {
		r.message = i18n.Translate(ctx, i18nkey.KeyGetEvaluationCaseFailure)
		return nil, err
	}
	if len(records) == 0 {
		// 空记录继续执行
		log.InfoContextf(ctx, "Prepare taskID:%d 空或已完成的待评测记录", r.params.TestID)
		return nil, nil
	}
	kv := make(task_scheduler.TaskKV, len(records))
	for _, record := range records {
		kv[strconv.FormatInt(int64(record.ID), 10)] = record.Question
	}
	return kv, nil
}

// Init 初始化
func (r *RunScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	log.InfoContextf(ctx, "task(run test %d) Init start, run %d records", r.params.TestID, len(kv))
	testTask, err := r.dao.GetTestByTestID(ctx, r.params.TestID)
	if err != nil {
		log.ErrorContextf(ctx, "[评测任务] Init, testID:%d, err: %v", r.params.TestID, err)
		return err
	}
	if testTask.Status != model.TestStatusInit && testTask.Status != model.TestStatusRunning {
		log.WarnContextf(ctx, "[评测任务] Init 状态不可评测, test:%v", testTask)
		return nil
	}
	r.OldTestDoneNum = testTask.TestDoneNum
	appDB, err := r.dao.GetAppByID(ctx, r.params.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "[评测任务] robot 查询异常, test:%d, err: %v", r.params.TestID, err)
		return err
	}
	if appDB == nil {
		log.ErrorContextf(ctx, "[评测任务] robot 未查询到")
		return errs.ErrRobotNotFound
	}
	r.robot = appDB
	r.app, err = client.GetAppInfo(ctx, appDB.BusinessID, model.AppTestScenes)
	if err != nil {
		return err
	}
	// 解析模型配置
	if err := r.getModelConf(ctx); err != nil {
		r.message = i18n.Translate(ctx, i18nkey.KeyModelConfigurationException)
		return err
	}

	r.sseClient = sse.NewSSEClient(config.GetChatSSEConnOptions(), r.onReply, r.onError)
	r.QuestionInfos = sync.Map{}
	return nil
}

// Process 任务处理
func (r *RunScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.InfoContextf(ctx, "task(run test %d) Process, task: %+v kv_len: %d, params: %+v", r.params.TestID, r.task,
		len(progress.TaskKV(ctx)), r.params)
	taskKV := progress.TaskKV(ctx)
	for id, v := range taskKV {
		log.DebugContextf(ctx, "task(run test) Start k:%s, v:%s", id, v)
		key := id
		appDB, err := r.dao.GetAppByID(ctx, r.params.RobotID)
		if err != nil {
			return err
		}
		ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(run test) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(run test) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		if !r.checkRunProcess(ctx) {
			log.DebugContextf(ctx, "task(DocToQA) checkRunProcess progress.Stop id,v,len(taskKV)|%s,%s,%d",
				id, v, len(taskKV))
			progress.Stop(ctx)
			return nil
		}
		if r.BillingError != "" {
			progress.Stop(ctx)
			err := r.Fail(ctx)
			if err != nil {
				log.ErrorContextf(ctx, "Process r.Fail err: %+v", err)
				return err
			}
			return nil
		}
		record, err := r.dao.GetTestRecordByID(ctx, r.params.TestID, cast.ToUint64(id))
		if err != nil {
			r.message = i18n.Translate(ctx, i18nkey.KeyRecordGetEvaluationRecordException, id)
			log.ErrorContextf(ctx, "GetTestRecordByBizID failed, reqID:%s, err:%s", r.task.TraceID, err)
			return err
		}
		log.DebugContextf(ctx, "doRecordTest record:%+v", record)
		roleDescription := ""
		if record.RoleDescription.Valid {
			roleDescription = record.RoleDescription.String
		}
		customVariables := ""
		if record.CustomVariables.Valid {
			customVariables = record.CustomVariables.String
		}
		if err := r.doRecordTest(ctx, id, v, roleDescription, customVariables, appDB.BusinessID); err != nil {
			log.ErrorContextf(ctx, "doRecordTest failed, reqID:%s, err:%s", r.task.TraceID, err)
			r.errRecordIDs = append(r.errRecordIDs, id)
			// chat返回或者see请求失败，不做处理当作没有回复让任务继续
		}
		if r.OldTestDoneNum != -1 {
			if err := r.dao.UpdateTestDoneNum(ctx, r.params.CorpID, r.params.RobotID, r.params.TestID); err != nil {
				r.errRecordIDs = append(r.errRecordIDs, id)
				log.ErrorContextf(ctx, "UpdateTestDoneNum failed, testID:%d, err:%s", r.params.TestID, err)
				return err
			}
		} else {
			// 历史任务重启，没有进度记录，继续显示-
			log.DebugContextf(ctx, "test: %d, OldTestDoneNum: %d run success", r.params.TestID, r.OldTestDoneNum)
		}
		if err := progress.Finish(ctx, id); err != nil {
			r.message = i18n.Translate(ctx, i18nkey.KeyRecordUpdateCompletionStatusException, id)
			log.ErrorContextf(ctx, "[评测任务] 执行测试记录：更新任务key状态异常, ID:%s, err:%+v", id, err)
			return err
		}
		log.InfoContextf(ctx, "test: %d, record: %s run success", r.params.TestID, v)
	}
	log.InfoContextf(ctx, "task(run test %d) Process done, errorIDs:%s", r.params.TestID,
		strings.Join(r.errRecordIDs, ","))
	if !r.checkResult(ctx, taskKV) {
		log.WarnContextf(ctx, "Process checkResult fail taskID:%d err:%v errRecordIDs:%v",
			r.params.TestID, errs.ErrRunningTestRecords, r.errRecordIDs)
		r.message = i18n.Translate(ctx, terrs.Msg(errs.ErrRunningTestRecords))
		return nil
	}
	return nil
}

// Fail 任务失败
func (r *RunScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "task(run test %d) fail", r.params.TestID)
	if len(r.message) == 0 {
		r.message = i18n.Translate(ctx, i18nkey.KeyPartialFailure)
	}
	if r.BillingError != "" { // 带上计费错误信息
		log.InfoContextf(ctx, "task(run test %d) Fail BillingError:%s", r.params.TestID, r.BillingError)
		r.message = r.BillingError
	}
	r.dealTaskResult(ctx, model.TestStatusFail)
	return nil
}

// Stop 任务停止
func (r *RunScheduler) Stop(ctx context.Context) error {
	return nil
}

// Done 任务完成回调
func (r *RunScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(run test %d) done", r.params.TestID)
	if r.BillingError != "" { // 如有计费错误标记失败
		log.InfoContextf(ctx, "task(run test %d) done BillingError:%s", r.params.TestID, r.BillingError)
		r.message = r.BillingError
		r.dealTaskResult(ctx, model.TestStatusFail)
		return nil
	}
	if r.message != "" { // 如有标记失败
		log.InfoContextf(ctx, "task(run test %d) done message:%s", r.params.TestID, r.message)
		r.dealTaskResult(ctx, model.TestStatusFail)
		return nil
	}
	r.dealTaskResult(ctx, model.TestStatusJudging)
	return nil
}

func (r *RunScheduler) getRerankModel() (model.RerankModelConfig, error) {
	f, ok := r.app.GetKnowledgeQa().GetFilters()[model.AppRerankFilterKey]
	if !ok {
		return model.RerankModelConfig{}, fmt.Errorf("filters not found")
	}
	modelInfo, ok := r.app.GetKnowledgeQa().GetModel()[model.RerankModel]
	if !ok {
		return model.RerankModelConfig{}, fmt.Errorf("robot %s model not found", model.RerankModel)
	}
	return model.RerankModelConfig{
		ModelName: modelInfo.GetModelName(),
		TopN:      f.GetTopN(),
		Enable:    r.app.GetKnowledgeQa().GetEnableRerank(),
	}, nil
}

func (r *RunScheduler) getAppModelName() string {
	modelInfo, ok := r.app.GetKnowledgeQa().GetModel()[model.AppModelNormal]
	if !ok {
		return ""
	}
	return modelInfo.GetModelName()
}

// doRecordTest 单条测试
func (r *RunScheduler) doRecordTest(ctx context.Context, id, question, roleDescription, customVariables string, appBizId uint64) error {
	rerank, err := r.getRerankModel()
	if err != nil {
		log.ErrorContextf(ctx, "get rerank model err:%v", err)
		return err
	}
	newQuestion, err := r.truncateQuestion(ctx, r.getAppModelName(), question)
	if err != nil {
		r.message = i18n.Translate(ctx, i18nkey.KeyGetModelCharLimitFailure)
		log.ErrorContextf(ctx, "[评测任务] 获取模型字符限制失败, question: %s", question)
		return err
	}
	docs, err := r.searchPreview(ctx, &retrieval.SearchVectorReq{
		RobotId: r.params.RobotID, BotBizId: appBizId, Filters: r.filters, TopN: r.filterTopN, Question: newQuestion,
		EmbeddingVersion: r.app.GetKnowledgeQa().GetEmbedding().GetVersion(),
		Rerank:           &retrieval.SearchVectorReq_Rerank{Model: rerank.ModelName, TopN: rerank.TopN, Enable: rerank.Enable},
		FilterKey:        model.AppSearchPreviewFilterKey, SearchStrategy: r.getSearchStrategy(),
	})
	if err != nil {
		r.message = i18n.Translate(ctx, i18nkey.KeyRecordKnowledgeRetrievalException, id)
		log.ErrorContextf(ctx, "[评测任务] 执行测试记录：%s 请求宙斯异常, ID:%s, err:%+v", newQuestion, id, err)
		return err
	}
	sid := fmt.Sprintf("%d", r.dao.GenerateSeqID())
	r.QuestionInfos.Store(sid+"_"+util.Md5Hex(strings.TrimSpace(strings.TrimRight(newQuestion, "\n"))),
		model.TestQuestionInfo{
			QuestionID: id,
			TestID:     r.params.TestID,
			Question:   newQuestion,
			References: docs,
		})

	sseSendEvent := &event.SseSendEvent{
		ReqID:           fmt.Sprintf("%s-%s", id, sid),
		Content:         newQuestion,
		BotAppKey:       r.robot.AppKey,
		VisitorBizID:    strconv.FormatUint(r.robot.CorpID, 10),
		SessionID:       sid,                          // r.task.TraceID,
		Timeout:         config.GetSSEClientTimeOut(), // 跟底座超时时间保持一致
		SystemRole:      roleDescription,
		IsEvaluateTest:  true,
		CustomVariables: nil,
	}
	log.InfoContextf(ctx, "doRecordTest|CustomVariables %s",
		customVariables)
	if customVariables != "" {
		sseSendEvent.CustomVariables = json.RawMessage(customVariables)
	}
	log.InfoContextf(ctx, "doRecordTest|question length original:%d, new:%d|sseClient.Send|sseSendEvent:%+v",
		len(question), len(newQuestion), sseSendEvent)
	return r.sseClient.StartStream(ctx, sseSendEvent)
}

func (r *RunScheduler) getSearchStrategy() *retrieval.SearchStrategy {
	var searchStrategy *retrieval.SearchStrategy
	if r.app.GetKnowledgeQa() != nil {
		searchStrategy = &retrieval.SearchStrategy{
			StrategyType: retrieval.SearchStrategyTypeEnum(
				r.app.GetKnowledgeQa().GetSearchStrategy().GetStrategyType()),
			TableEnhancement: r.app.GetKnowledgeQa().GetSearchStrategy().GetTableEnhancement(),
		}
	}
	return searchStrategy
}
func (r *RunScheduler) searchPreview(ctx context.Context,
	req *retrieval.SearchVectorReq) ([]*model.SearchReferences, error) {
	rsp, err := client.SearchVector(ctx, req)
	if err != nil {
		return nil, err
	}
	linkContents, err := r.dao.GetLinkContentsFromSearchVectorResponse(
		ctx, req.RobotId, rsp.GetDocs(),
		func(doc *retrieval.SearchVectorRsp_Doc, qa *model.DocQA) any {
			return &model.SearchReferences{
				DocID:      qa.DocID,
				DocType:    doc.GetDocType(),
				ID:         doc.GetId(),
				Question:   qa.Question,
				Answer:     qa.Answer,
				Confidence: doc.GetConfidence(),
			}
		},
		func(doc *retrieval.SearchVectorRsp_Doc, segment *model.DocSegmentExtend) any {
			return &model.SearchReferences{
				DocID:      segment.DocID,
				DocType:    doc.GetDocType(),
				ID:         doc.GetId(),
				OrgData:    segment.OrgData,
				Confidence: doc.GetConfidence(),
			}
		},
		func(doc *retrieval.SearchVectorRsp_Doc) any {
			return &model.SearchReferences{
				DocID:      0,
				DocType:    doc.GetDocType(),
				ID:         0,
				Question:   doc.GetQuestion(),
				Answer:     doc.GetAnswer(),
				Confidence: doc.GetConfidence(),
			}
		},
	)
	if err != nil {
		return nil, err
	}
	return dao.Link(ctx, linkContents, func(r *model.SearchReferences, v linker.Content) *model.SearchReferences {
		r.OrgData = v.Value
		return r
	}), nil
}

// getModelConf 解析模型配置
func (r *RunScheduler) getModelConf(ctx context.Context) error {
	filter, ok := r.app.GetKnowledgeQa().GetFilters()[model.FilterSearchPreview]
	if !ok {
		log.ErrorContextf(ctx, "[评测任务] robot 配置filter异常, test:%d", r.params.TestID)
	}
	modelInfo, ok := r.app.GetKnowledgeQa().GetModel()[model.RecordTypeEvaluation]
	if !ok {
		log.ErrorContextf(ctx, "[评测任务] robot model 异常, test:%d", r.params.TestID)
	}
	log.InfoContextf(ctx, "[评测任务] robot model: %v", modelInfo)
	filters := make([]*retrieval.SearchVectorReq_Filter, 0, len(filter.GetFilter()))
	for _, f := range filter.GetFilter() {
		if f.GetDocType() == model.DocTypeSearchEngine { // 批量评测不走搜索引擎
			continue
		}
		if f.GetDocType() == model.DocTypeTaskFlow {
			continue
		}
		if !f.GetIsEnable() {
			continue
		}
		filters = append(filters, &retrieval.SearchVectorReq_Filter{
			DocType:    f.GetDocType(),
			Confidence: f.GetConfidence(),
			TopN:       f.GetTopN(),
			IndexId:    uint64(f.GetIndexId()),
		})
	}
	r.filters = filters
	r.filterTopN = filter.GetTopN()
	return nil
}

// 新建评测任务时，根据不同模型切换字符数上限，并进行自动截断
// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118892670
func (r *RunScheduler) truncateQuestion(ctx context.Context, modelName, question string) (string, error) {
	limitNum := config.App().SampleTest.DefaultLimit
	modelLengthLimit := config.App().SampleTest.ModelLengthLimit
	resp, err := r.dao.GetModelInfo(ctx, modelName)
	if err != nil {
		return "", err
	}
	for i := range modelLengthLimit {
		if strings.ToUpper(resp.GetModelLength()) == strings.ToUpper(modelLengthLimit[i].Length) {
			limitNum = modelLengthLimit[i].Limit
			break
		}
	}

	if utf8.RuneCountInString(question) <= limitNum {
		return question, nil
	}
	return string([]rune(question)[:limitNum]), nil
}

func (r *RunScheduler) dealTaskResult(ctx context.Context, status uint64) {
	test, err := r.dao.GetTestByTestID(ctx, r.params.TestID)
	if err != nil {
		log.ErrorContextf(ctx, "[评测任务] 查询之前任务状态失败，test:%d err:%+v", r.params.TestID, err)
		return
	}
	if test == nil {
		log.ErrorContextf(ctx, "[评测任务] test empty")
		return
	}
	if test.Status != model.TestStatusRunning {
		log.WarnContextf(ctx, "[评测任务] 非评测中状态，退出任务，任务状态:%d", test.Status)
		return
	}
	test.Status = status
	if status == model.TestStatusFail {
		test.Message = r.message
	}
	if err = r.dao.UpdateTestStatus(ctx, test); err != nil {
		log.ErrorContextf(ctx, "[评测任务] 更新任务状态失败，test:%d err:%+v", r.params.TestID, err)
		return
	}
	if err = r.dao.CreateNotice(ctx, test.CreateTestNotice(ctx)); err != nil {
		log.ErrorContextf(ctx, "[评测任务] 下发通知失败，test:%d err:%+v", r.params.TestID, err)
	}
}

func (r *RunScheduler) onReply(ev event.Wrapper) {
	reply := event.ReplyEvent{}
	if err := json.Unmarshal(ev.Payload, &reply); err != nil {
		log.ErrorContextf(trpc.BackgroundContext(),
			"doRecordTest onReply Unmarshal failed, taskID:%d, err:%+v", r.task.ID, err)
		return
	}
	log.DebugContextf(trpc.BackgroundContext(), "评测结果 onReply: %+v", reply)
	var ok bool
	var info model.TestQuestionInfo
	if reply.IsFromSelf {
		key := reply.SessionID + "_" + util.Md5Hex(strings.TrimSpace(strings.TrimRight(reply.Content, "\n")))
		value, ok := r.QuestionInfos.Load(key)
		if !ok {
			r.message = i18n.Translate(trpc.BackgroundContext(), i18nkey.KeyLargeModelRequestFailedWithDetail, reply.SessionID, reply.Content)
			log.ErrorContextf(trpc.BackgroundContext(), "%s|[评测任务] 执行测试记录：请求模型异常, question:%s, "+
				"RecordID:%s, err:问题信息未找到", reply.SessionID, reply.Content, reply.RecordID)
			return
		}
		info, ok = value.(model.TestQuestionInfo)
		if !ok {
			r.message = i18n.Translate(trpc.BackgroundContext(), i18nkey.KeyLargeModelRequestFailedWithDetail, reply.SessionID, reply.Content)
			log.ErrorContextf(trpc.BackgroundContext(), "%s|[评测任务] 执行测试记录：请求模型异常, question:%s, "+
				"RecordID:%s, err:问题信息未找到", reply.SessionID, reply.Content, reply.RecordID)
			return
		}
		if reply.RecordID == "" {
			log.ErrorContextf(trpc.BackgroundContext(), "评测结果 onReply RecordID is null reply:%v", reply)
			return
		}
		err := r.updateTestRecordFromSelf(info, reply.SessionID, reply.RecordID, reply.ReplyMethod)
		if err != nil {
			log.ErrorContextf(trpc.BackgroundContext(), "onReply updateTestRecord err:%+v info:%v reply:%v",
				err, info, reply)
			return
		}
		if reply.IsEvil { // 第一条回复发现是敏感词,不会有后续回答直接删除
			log.InfoContextf(trpc.BackgroundContext(),
				"doRecordTest onReply 敏感词回复 taskID:%d, reply:%+v", r.task.ID, reply)
			r.QuestionInfos.Delete(key)
			return
		}

		r.QuestionInfos.Delete(key)
		info.RecordID = reply.RecordID
		r.QuestionInfos.Store(info.RecordID, info)
		return
	}
	if !reply.IsFinal {
		return
	}
	if reply.IsEvil { // 第一条不是敏感词，但是结束的回复是敏感词,正常走回复答案逻辑
		log.InfoContextf(trpc.BackgroundContext(),
			"doRecordTest|onReply|IsFinal|敏感词回复|taskID:%d, reply:%+v", r.task.ID, reply)
	}
	value, ok := r.QuestionInfos.Load(reply.RelatedRecordID)
	if !ok {
		r.message = i18n.Translate(trpc.BackgroundContext(), i18nkey.KeyLargeModelRequestFailed, reply.SessionID)
		log.ErrorContextf(trpc.BackgroundContext(), "%s|[评测任务] 执行测试记录：请求模型异常, answer:%s, "+
			"RelatedRecordID:%s, err:问题信息未找到", reply.SessionID, reply.Content, reply.RelatedRecordID)
		return
	}
	info, ok = value.(model.TestQuestionInfo)
	if !ok {
		r.message = i18n.Translate(trpc.BackgroundContext(), i18nkey.KeyLargeModelRequestFailed, reply.SessionID)
		log.ErrorContextf(trpc.BackgroundContext(), "%s|[评测任务] 执行测试记录：请求模型异常, answer:%s, "+
			"RelatedRecordID:%s, err:问题信息未找到", reply.SessionID, reply.Content, reply.RelatedRecordID)
		return
	}
	err := r.onComplete(reply, info)
	if err != nil {
		r.message = i18n.Translate(trpc.BackgroundContext(), i18nkey.KeyLargeModelRequestFailed, info.RecordID)
		log.ErrorContextf(trpc.BackgroundContext(), "%s|[评测任务] 执行测试记录：%s", reply.SessionID, err.Error())
	}
}

func (r *RunScheduler) onComplete(reply event.ReplyEvent, info model.TestQuestionInfo) error {
	ctx := trpc.BackgroundContext()
	if info.RecordID != reply.RelatedRecordID {
		log.ErrorContextf(ctx, "记录：%s 请求模型异常, ID:%s, err:记录ID未对应上, %s!=%s, 请求大模型异常", info.Question,
			info.RecordID, info.RecordID, reply.RelatedRecordID)
		return nil
	}
	info.Answer = reply.Content
	id, err := strconv.ParseUint(info.QuestionID, 10, 64)
	if err != nil {
		return fmt.Errorf("%s 请求模型异常, "+"ID:%s, err:%+v", info.Question, info.RecordID, err)
	}
	record := &model.RobotTestRecord{
		ID:          id,
		TestID:      info.TestID,
		Question:    info.Question,
		Answer:      reply.Content,
		Reference:   getReferences(info.References),
		ReplyMethod: int(reply.ReplyMethod),
		TraceID: sql.NullString{
			String: reply.SessionID,
			Valid:  true,
		},
		RecordID:        reply.RecordID,
		RelatedRecordID: reply.RelatedRecordID,
	}
	log.InfoContextf(ctx, "%s|[评测任务] 执行测试记录-ID: %d, 问题: %s, 答案:%s",
		reply.SessionID, id, info.Question, info.Answer)
	if err = r.dao.UpdateTestRecord(ctx, record); err != nil {
		return fmt.Errorf("%s 更新DB异常, ID:%d, err:%+v", record.Question, record.ID, err)
	}
	info.IsFinal = true
	r.QuestionInfos.Store(reply.RelatedRecordID, info)

	value, ok := r.QuestionInfos.Load(TestStatusDone)
	if !ok || !value.(bool) {
		r.QuestionInfos.Store(TestStatusDone, true)
	}
	return nil
}

// updateTestRecord 更新测试记录
func (r *RunScheduler) updateTestRecordFromSelf(info model.TestQuestionInfo, traceID, relatedRecordID string,
	replyMethod event.ReplyMethod) error {
	id, err := strconv.ParseUint(info.QuestionID, 10, 64)
	if err != nil {
		return fmt.Errorf("%s updateTestRecordFromSelf ParseUint, ID:%s, err:%+v",
			info.Question, info.RecordID, err)
	}
	record := &model.RobotTestRecord{
		ID:     id,
		TestID: info.TestID,
		TraceID: sql.NullString{
			String: traceID,
			Valid:  true,
		},
		RelatedRecordID: relatedRecordID,
		ReplyMethod:     int(replyMethod),
	}
	ctx := trpc.BackgroundContext()
	if err = r.dao.UpdateTestRecordFromSelf(ctx, record); err != nil {
		return fmt.Errorf("UpdateTestRecordFromSelf fail,info:%v traceID:%s relatedRecordID:%s , err:%+v ", info,
			traceID, relatedRecordID, err)
	}
	return nil
}

func (r *RunScheduler) onError(ev event.Wrapper) {
	ctx := trpc.BackgroundContext()
	eventJson, err := jsoniter.MarshalToString(ev)
	if err != nil {
		log.InfoContextf(ctx, "onError event.Wrapper Marshal err: %+v", err)
		return
	}
	log.InfoContextf(ctx, "onError taskID:%s,event.Wrapper:%v", r.task.TraceID, eventJson)
	errEvent := event.ErrorEvent{
		RequestID: ev.MessageID,
	}
	if ev.Error != nil {
		if err := json.Unmarshal(ev.Error, &errEvent.Error); err != nil {
			log.ErrorContextf(ctx, "doRecordTest onError error Unmarshal, taskID:%s, failed:%s", r.task.TraceID,
				err)
			return
		}
	}
	if ev.Payload != nil {
		payload := event.ErrorEvent{
			RequestID: ev.MessageID,
		}
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			log.ErrorContextf(ctx, "onError Unmarshal Payload,taskID:%s,failed:%s", r.task.TraceID, err.Error())
			return
		}
		if payload.Error.Code == ErrNoBalanceCode {
			r.BillingError = payload.Error.Message
			log.InfoContextf(ctx, "doRecordTest onError ErrNoBalanceCode, taskID:%s, payload:%+v", r.task.TraceID,
				payload)
			return
		}
	}
	log.InfoContextf(ctx, "doRecordTest onError, taskID:%s, errEvent:%+v", r.task.TraceID, errEvent)
}

func (r *RunScheduler) checkResult(ctx context.Context, kv task_scheduler.TaskKV) bool {
	if r.BillingError != "" { // 如有计费错误标记失败
		log.WarnContextf(ctx, "doRecordTest checkResult taskID:%s BillingError:%s",
			r.task.TraceID, r.BillingError)
		return true
	}
	value, ok := r.QuestionInfos.Load(TestStatusDone)
	if ok && value.(bool) {
		log.InfoContextf(ctx, "checkResult, 任务:%d 已至少有一条完成", r.params.TestID)
		r.message = ""
		return true
	}

	r.QuestionInfos.Range(func(key, value any) bool {
		info, ok := value.(model.TestQuestionInfo)
		if !ok || info.IsFinal {
			return true
		}
		if _, ok = kv[info.QuestionID]; ok {
			log.WarnContextf(ctx, "doRecordTest checkResult Unmarshal, sid:%s, failed:%+v",
				r.task.TraceID, info)
			r.errRecordIDs = append(r.errRecordIDs, info.QuestionID)
		}
		return true
	})
	return len(r.errRecordIDs) == 0
}

func getReferences(docs []*model.SearchReferences) string {
	if len(docs) == 0 {
		return ""
	}
	str, _ := jsoniter.MarshalToString(docs)
	return str
}

// checkRunProcess 检查是否可以执行任务
func (r *RunScheduler) checkRunProcess(ctx context.Context) bool {
	testTask, err := r.dao.GetTestByTestID(ctx, r.params.TestID)
	if err != nil {
		log.ErrorContextf(ctx, "[评测任务] checkRunProcess, testID:%d, err: %v", r.params.TestID, err)
		return false
	}
	if testTask.Status != model.TestStatusInit && testTask.Status != model.TestStatusRunning {
		log.WarnContextf(ctx, "[评测任务] checkRunProcess 状态不可评测, test:%v", testTask)
		return false
	}
	return true
}
