package task

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	logicApp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	logicCorp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/corp"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strings"
	"time"

	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"gorm.io/gorm"

	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/billing"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

const (
	docQaDosagePrefix = "docQa:dosage:"
)

// DocToQAScheduler 文档生成问答任务
type DocToQAScheduler struct {
	dao         dao.Dao
	task        task_scheduler.Task
	p           model.DocToQAParams
	tokenDosage billing.TokenDosage
}

func initDocToQAScheduler() {
	task_scheduler.Register(
		model.DocToQATask,
		func(t task_scheduler.Task, params model.DocToQAParams) task_scheduler.TaskHandler {
			return &DocToQAScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocToQAScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocToQA) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return kv, nil
	}
	if doc.IsExcel() {
		return kv, errs.ErrDocIsExcel
	}
	log.DebugContextf(ctx, "task(DocToQA) Prepare doc.IsCreatingQA %v", doc.IsCreatingQaV1())
	if !doc.IsCreatingQaV1() { // 如果在执行前已经被点暂时或取消，跳过该任务
		log.InfoContextf(ctx, "task(DocToQA) Prepare doc.IsCreatingQA is false doc:%+v", doc)
		return kv, nil
	}
	var qas []*model.DocQA
	if d.p.QaTaskType != model.DocQATaskStatusContinue {
		// 还原已完成切片状态
		if err = d.dao.UpdateQaSegmentToDocStatus(ctx, doc.ID, doc.BatchID, doc.RobotID); err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) Prepare, UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
				err, d.p.QaTaskID)
			return kv, err
		}

		key := fmt.Sprintf("%s%d", model.DocQaExistsOrgDataPreFix, d.p.DocID)
		// 重置orgData去重缓存
		if _, err = d.dao.RedisCli().Do(ctx, "DEL", key); err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) Prepare, Redis del failed, err:%+v", err)
			return kv, errs.ErrGetQaExistsFail
		}
	}

	for _, qa := range qas {
		kv[fmt.Sprintf("%s%d", qaDeletePrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}

	var idsAll []uint64
	log.InfoContextf(ctx, "task(DocToQA) GetSegmentIDByDocIDAndBatchID doc:%v", doc)
	idsAll, err = d.dao.GetSegmentIDByDocIDAndBatchID(ctx, doc.ID, doc.BatchID, d.p.RobotID)
	if err != nil {
		return kv, err
	}

	if d.p.QaTaskType == model.DocQATaskStatusContinue {
		log.InfoContextf(ctx, "task(DocToQA) retry task: %+v, DocToQAParams: %+v", d.task, d.p)
		docQATask, err := d.dao.GetDocQATaskByID(ctx, d.p.QaTaskID, d.p.CorpID, d.p.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) retry 获取生成问答任务详情失败 err:%+v", err)
			return kv, err
		}
		if docQATask == nil || docQATask.ID <= 0 {
			log.InfoContextf(ctx, "task(DocToQA) retry 获取生成问答任务不存在 corpID|%d|robotID%d|id|%d ",
				d.p.CorpID, d.p.RobotID, d.p.QaTaskID)
			return kv, errs.ErrDocQaTaskNotFound
		}
		log.InfoContextf(ctx, "task(DocToQA) retry QaTask:%+v|oferset:%d|limit:%d", docQATask,
			docQATask.StopNextSegmentID, uint64(len(idsAll)))
		ids, err := d.dao.GetQASegmentIDByDocIDAndBatchID(ctx, doc.ID, docQATask.StopNextSegmentID, uint64(len(idsAll)),
			doc.BatchID, d.p.RobotID)
		if err != nil {
			return kv, err
		}
		idsAll = ids
	}
	if len(idsAll) == 0 {
		log.DebugContextf(ctx, "task(DocToQA) DocToQaSegment is nil len(ids): %d", len(idsAll))
		return kv, errs.ErrQaTaskSegmentFail
	}
	for _, id := range idsAll {
		log.DebugContextf(ctx, "task(DocToQA) DocToQaSegment seg.ID: %d", id)
		kv[fmt.Sprintf("%s%d", segGenQAPrefix, id)] = fmt.Sprintf("%d", id)
	}

	return kv, nil
}

// Init 初始化
func (d *DocToQAScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	// token用量统计初始化
	log.DebugContextf(ctx, "task(DocToQA) prepareTokenDosage, task: %+v, params: %+v", d.task, d.p)
	dosage, err := logicCorp.GetTokenDosage(ctx, d.p.AppBizID, "", uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL))
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Init err: %v", err)
		return err
	}
	if dosage == nil {
		log.ErrorContextf(ctx, "task(DocToQA) Init dosage is nil")
		return errs.ErrSystem
	}
	d.tokenDosage = *dosage
	return nil
}

// Process 任务处理
func (d *DocToQAScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocToQA) Process, task: %+v, params: %+v,kv_len: %d,progress_len:%d",
		d.task, d.p, len(progress.TaskKV(ctx)), progress.Total())
	uniqueQaMap := make(map[string]bool)
	if d.p.CorpID == 0 || d.p.RobotID == 0 || d.p.QaTaskID == 0 {
		log.ErrorContextf(ctx, "task(DocToQA) Process 服务异常,任务init数据丢失 DocToQAParams:%v", d.p)
		return errs.ErrSystem
	}
	corpReq := &admin.GetCorpReq{
		Id: d.p.CorpID,
	}
	corpRsp, err := d.dao.GetAdminApiCli().GetCorp(ctx, corpReq)
	if err != nil || corpRsp == nil {
		return errs.ErrCorpNotFound
	}
	d.tokenDosage.StartTime = time.Now()
	taskKV := progress.TaskKV(ctx)
	for k, v := range taskKV {
		log.DebugContextf(ctx, "task(DocToQA) Start k:%s, v:%s", k, v)
		key := k
		app, err := client.GetAppInfo(ctx, d.p.AppBizID, model.AppTestScenes)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) GetAppInfo err:%+v", err)
			return err
		}
		if app.GetIsDelete() {
			log.DebugContextf(ctx, "task(DocToQA) appDB.HasDeleted()|appID:%d", app.GetId())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocToQA) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		if !d.checkRunProcess(ctx) {
			log.DebugContextf(ctx, "task(DocToQA) checkRunProcess progress.Stop k,v,len(taskKV)|%s,%s,%d",
				k, v, len(taskKV))
			progress.Stop(ctx)
			return nil
		}
		if !logicCorp.CheckModelStatus(ctx, d.dao, d.p.CorpID, d.tokenDosage.ModelName, client.DocExtractQABizType) {
			log.DebugContextf(ctx, "task(DocToQA) checkModelStatus 余量不足 TaskKV k:%s|v:%s", k, v)
			err := d.dao.StopQaTask(ctx, d.p.CorpID, d.p.RobotID, d.p.QaTaskID, true, d.tokenDosage.AliasName)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocToQA) 余量不足 StopQaTask暂停任务失败|err:%+v", err)
				return err
			}
			progress.Stop(ctx)
			return nil
		}
		var uniqueQas []*model.QA
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, segGenQAPrefix) {
			segment, err := d.dao.GetSegmentByID(ctx, id, app.GetId())
			if err != nil {
				return err
			}
			if segment == nil {
				return errs.ErrSegmentNotFound
			}
			log.DebugContextf(ctx, "task(DocToQA) Process GetSegmentByID|%v",
				segment)
			// 2.2.2迭代：表格类型的先不进行QA提取
			if segment.SegmentType == model.SegmentTypeTable {
				if err := progress.Finish(ctx, key); err != nil {
					log.ErrorContextf(ctx, "task(DocToQA) Finish kv:%s err:%+v", key, err)
					return err
				}
				continue
			}
			if segment.OrgData == "" { // 如果旧表格没有orgData，则从t_doc_segment_org_data新表中获取orgData
				doc, err := d.dao.GetDocByID(ctx, segment.DocID, segment.RobotID)
				if err != nil {
					return err
				}
				if doc == nil {
					return errs.ErrDocNotFound
				}
				orgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataByBizID(ctx,
					[]string{dao.DocSegmentOrgDataTblColOrgData}, corpRsp.GetCorpBizId(), app.GetAppBizId(),
					doc.BusinessID, segment.OrgDataBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if orgData != nil && orgData.OrgData != "" {
					segment.OrgData = orgData.OrgData
				}
				log.DebugContextf(ctx, "task(DocToQA) Process GetDocOrgDataByBizID|segment.OrgData:%s",
					segment.OrgData)
			}
			if segment.OrgData == "" {
				log.InfoContextf(ctx, "task(DocToQA) Process GetDocOrgDataByBizID|%v|orgData is empty", segment)
				if err = progress.Finish(ctx, key); err != nil {
					log.ErrorContextf(ctx, "task(DocToQA) Finish kv:%s err:%+v", key, err)
					return err
				}
				continue
			}

			// 判断是否有重复的orgData,重复的orgData不进行问答生成
			exists, err := checkSegmentOrgDataExists(ctx, segment, d.dao, d.p.DocID)
			if err != nil {
				if errors.Is(err, errs.ErrOperateDoing) {
					log.ErrorContextf(ctx, "task(DocToQA) checkSegmentOrgDataExists err:%v", err)
					continue
				}
				return err
			}
			if exists {
				log.DebugContextf(ctx, "task(DocToQA) checkSegmentOrgDataExists 重复ordData:%v segment:%+v",
					exists, segment)
				// 更新生成问答任务已完成切片数量和问答数量
				if err = d.dao.UpdateDocQATaskSegmentDoneAndQaCount(ctx, 0, 1,
					d.p.CorpID, d.p.RobotID, d.p.QaTaskID); err != nil {
					log.ErrorContextf(ctx, "task(DocToQA) Done UpdateDocQATaskSegmentCountDone failed,"+
						" err:%+v|QaTaskID|%d", err, d.p.QaTaskID)
					return err
				}
				segment.Status = model.SegmentStatusCreatedQa
				// 更新切片状态
				if err = d.dao.UpdateQaSegmentStatus(ctx, segment, d.p.RobotID); err != nil {
					log.ErrorContextf(ctx, "task(DocToQA) Done UpdateDocQATaskSegmentCountDone failed,"+
						" err:%+v|QaTaskID|%d", err, d.p.QaTaskID)
					return err
				}
				if err := progress.Finish(ctx, key); err != nil {
					log.ErrorContextf(ctx, "task(DocToQA) Finish kv:%s err:%+v", key, err)
					return err
				}
				continue
			}

			doc, err := d.dao.GetDocByID(ctx, segment.DocID, app.GetId())
			if err != nil {
				return err
			}
			if doc == nil {
				log.ErrorContextf(ctx, "task(DocToQA) Process GetDocByID doc is nil DocID:%d",
					segment.DocID)
				return errs.ErrDocNotFound
			}
			if doc.HasDeleted() {
				log.InfoContextf(ctx, "task(DocToQA) Process doc is Deleted|doc:%+v|Finish|key|%s", doc, key)
				if err = progress.Finish(ctx, key); err != nil {
					log.ErrorContextf(ctx, "task(DocToQA) Finish kv:%s err:%+v", key, err)
					return err
				}
				return nil
			}

			// 需要先集成文档有效期，防止后面使用未空
			segment.ExpireStart = doc.ExpireStart
			segment.ExpireEnd = doc.ExpireEnd
			tree, qas, tokenStatisticInfo, err := getQAAndCateNode(ctx, doc, segment, app, d.dao, d.tokenDosage.ModelName)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocToQA) getQAAndCateNode err|%v|doc:%+v|segment:%+v",
					err, doc, segment)
				return err
			}
			qas = slicex.Filter(qas, func(qa *model.QA) bool {
				return checkQuestionAndAnswer(ctx, qa.Question, qa.Answer, qa.SimilarQuestions) == nil
			})
			for _, qa := range qas {
				uniqueKey := func(qa *model.QA) string {
					return qa.Question + qa.Answer
				}(qa)
				if !uniqueQaMap[uniqueKey] {
					uniqueQaMap[uniqueKey] = true
					uniqueQas = append(uniqueQas, qa)
				}
			}
			// 文档生成问答时不需要审核，在后续采纳问答时走审核
			if err = d.dao.BatchCreateQA(ctx, segment, doc, uniqueQas, tree, false); err != nil {
				log.ErrorContextf(ctx, "task(DocToQA) Process BatchCreateQA failed,"+
					" err:%+v|QaTaskID|%d|docID|%d", err, d.p.QaTaskID, doc.ID)
				return err
			}
			// 通过文档生成的问答默认未采纳，这里在采纳问答的时候会更新机器人字符使用量

			// 调用大模型，消耗token，上报token
			err = reportTokenDosage(ctx, tokenStatisticInfo, d.dao, &d.tokenDosage, d.p.CorpID, d.p.RobotID, app.GetAppBizId(), d.p.QaTaskID)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocToQA) Process reportTokenDosage failed,"+
					" err:%+v|tokenStatisticInfo|%v", err, tokenStatisticInfo)
				return err
			}

			segment.Status = model.SegmentStatusCreatedQa
			// 更新切片状态
			if err = d.dao.UpdateQaSegmentStatus(ctx, segment, d.p.RobotID); err != nil {
				log.ErrorContextf(ctx, "task(DocToQA) Done UpdateDocQATaskSegmentCountDone failed,"+
					" err:%+v|QaTaskID|%d", err, d.p.QaTaskID)
				return err
			}

			// 更新生成问答任务已完成切片数量和问答数量
			if err = d.dao.UpdateDocQATaskSegmentDoneAndQaCount(ctx, uint64(len(uniqueQas)), 1,
				d.p.CorpID, d.p.RobotID, d.p.QaTaskID); err != nil {
				log.ErrorContextf(ctx, "task(DocToQA) Done UpdateDocQATaskSegmentCountDone failed,"+
					" err:%+v|QaTaskID|%d", err, d.p.QaTaskID)
				return err
			}
		}
		if strings.HasPrefix(key, qaDeletePrefix) {
			qa, err := d.dao.GetQAByID(ctx, id)
			if err != nil {
				return err
			}
			if err = d.dao.DeleteQA(ctx, qa); err != nil {
				return err
			}
			if err = d.dao.DeleteQASimilar(ctx, qa); err != nil {
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocToQA) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *DocToQAScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocToQA) Fail")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Fail GetDocByID failed, err:%+v|DocID|%d", err, d.p.DocID)
		return err
	}
	if doc == nil {
		log.ErrorContextf(ctx, "task(DocToQA) Fail GetDocByID doc is nil|DocID|%d", d.p.DocID)
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		log.ErrorContextf(ctx, "task(DocToQA) Fail GetDocByID doc HasDeleted|DocID|%d", d.p.DocID)
		return nil
	}
	doc.Message = i18nkey.KeyGenerateQAFailed
	doc.IsCreatingQA = false
	doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
	doc.UpdateTime = time.Now()
	if err = d.dao.CreateDocQADone(ctx, d.p.StaffID, doc, -1, false); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Fail CreateDocQADone failed, err:%+v|doc|%v",
			err, doc)
		return err
	}
	// 更新生成问答任务状态
	if err = d.dao.UpdateDocQATaskStatus(ctx, model.DocQATaskStatusFail, d.p.QaTaskID); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Fail UpdateDocQATaskStatus failed, err:%+v|QaTaskID|%d",
			err, d.p.QaTaskID)
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocToQAScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocToQAScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocToQA) Done")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done GetDocByID failed, err:%+v|DocID|%d", err, d.p.DocID)
		return err
	}
	if doc == nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done GetDocByID doc is nil|DocID|%d", d.p.DocID)
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		log.ErrorContextf(ctx, "task(DocToQA) Done GetDocByID doc HasDeleted|DocID|%d", d.p.DocID)
		return nil
	}
	qaListReq := &model.QAListReq{
		CorpID:  d.p.CorpID,
		DocID:   []uint64{d.p.DocID},
		RobotID: d.p.RobotID,
	}
	count, err := d.dao.GetQaCountWithDocID(ctx, qaListReq)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done GetQaCountWithDocID failed, err:%+v|qaListReq|%v",
			err, qaListReq)
		return err
	}
	doc.Message = i18nkey.KeyGenerateQASuccess
	doc.IsCreatingQA = false
	doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
	doc.UpdateTime = time.Now()

	success := true
	if count == 0 {
		success = false
	}
	// 更新DB
	if err = d.dao.CreateDocQADone(ctx, d.p.StaffID, doc, int(count), success); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done CreateDocQADone failed, err:%+v|DocID|%d",
			err, d.p.DocID)
		return err
	}
	// 更新生成问答任务状态
	if err = d.dao.UpdateDocQATaskStatus(ctx, model.DocQATaskStatusSuccess, d.p.QaTaskID); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done UpdateDocQATaskStatus failed, err:%+v|QaTaskID|%d",
			err, d.p.QaTaskID)
		return err
	}

	// 还原已完成切片状态
	if err = d.dao.UpdateQaSegmentToDocStatus(ctx, doc.ID, doc.BatchID, doc.RobotID); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done UpdateQaSegmentToDocStatus failed, err:%+v|QaTaskID|%d",
			err, d.p.QaTaskID)
		return err
	}

	key := fmt.Sprintf("%s%d", model.DocQaExistsOrgDataPreFix, d.p.DocID)
	// 重置orgData去重缓存
	if _, err = d.dao.RedisCli().Do(ctx, "DEL", key); err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Done, Redis del failed, err:%+v", err)
		return err
	}
	return nil
}

func (d *DocToQAScheduler) createSegment(ctx context.Context, doc *model.Doc, docParse model.DocParse) error {
	return d.dao.ParseOfflineDocTaskResult(ctx, doc, docParse, model.SegmentTypeQA, false)
}

func getQAAndCateNode(ctx context.Context, doc *model.Doc, segment *model.DocSegmentExtend,
	app *admin.GetAppInfoRsp, dao dao.Dao, modelName string) (*model.CateNode, []*model.QA, *client.StatisticInfo, error) {
	cates, err := dao.GetCateList(ctx, model.QACate, doc.CorpID, app.GetId())
	if err != nil {
		return nil, nil, nil, err
	}
	tree := model.BuildCateTree(cates)

	prompt, err := logicApp.GetPrompt(ctx, dao, app, model.QAExtractModel)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) getQAAndCateNode, GetPrompt failed, err:%+v", err)
		return nil, nil, nil, err
	}
	qas, tokenStatisticInfo, err := logicDoc.LLMSegmentQA(ctx, doc, segment, app, modelName, prompt)
	if err != nil {
		return nil, nil, nil, err
	}
	qas = slicex.UniqueFunc(qas, func(qa *model.QA) string { return qa.Question + qa.Answer })
	for _, qa := range qas {
		if len(qa.Path) == 0 {
			qa.Path = []string{model.UncategorizedCateName}
		}
	}
	return tree, qas, tokenStatisticInfo, nil
}

// reportTokenDosage 上报token用量
func reportTokenDosage(ctx context.Context, tokenStatisticInfo *client.StatisticInfo, dao dao.Dao,
	dosage *billing.TokenDosage, corpID, robotID, appBizId, QATaskID uint64) error {
	log.DebugContextf(ctx, "task(DocToQA) reportTokenDosage, tokenStatisticInfo:%+v", tokenStatisticInfo)

	err := logicCorp.ReportTokenDosage(ctx, tokenStatisticInfo, dosage, dao, corpID, client.DocExtractQABizType, appBizId)
	if err != nil {
		// 只打印ERROR日志，降级处理
		log.ErrorContextf(ctx, "task(DocToQA) reportTokenDosage, ReportTokenDosage failed, err:%+v", err)
		return nil
	}

	// 每次更新任务使用token
	if tokenStatisticInfo == nil {
		// 只打印ERROR日志，降级处理
		log.ErrorContextf(ctx, "task(DocToQA) reportTokenDosage, tokenStatisticInfo is nil")
		return nil
	}
	if err := dao.UpdateDocQATaskToken(ctx, uint64(tokenStatisticInfo.InputTokens),
		uint64(tokenStatisticInfo.OutputTokens), corpID, robotID, QATaskID); err != nil {
		// 只打印ERROR日志，降级处理
		log.ErrorContextf(ctx, "task(DocToQA) UpdateDocQATaskToken,failed, err:%+v", err)
		return nil
	}
	return nil
}

// // reportTokenFinance 上报token用量到计费
// func (d *DocToQAScheduler) reportTokenFinance(ctx context.Context) error {
//	log.DebugContextf(ctx, "task(DocToQA) reportTokenFinance")
//	dosage := &billing.TokenDosage{}
//
//	key := fmt.Sprintf("%s%d", docQaDosagePrefix, d.p.DocID)
//	val, err := redis.String(d.dao.RedisCli().Do(ctx, "GET", key))
//	if err != nil {
//		log.ErrorContextf(ctx, "task(DocToQA) reportTokenFinance, Redis get failed, err:%+v", err)
//		return err
//	}
//	log.DebugContextf(ctx, "task(DocToQA) reportTokenFinance, Redis get success, key:%s, val:%s", key, val)
//	if err = jsoniter.UnmarshalFromString(val, dosage); err != nil {
//		log.ErrorContextf(ctx, "task(DocToQA) reportTokenFinance, UnmarshalFromString failed, err:%+v", err)
//		return err
//	}
//
//	dosage.EndTime = time.Now()
//	log.DebugContextf(ctx, "task(DocToQA) reportTokenFinance, dosage%+v", dosage)
//
//	if len(dosage.InputDosages) > 0 || len(dosage.OutputDosages) > 0 {
//		corp, err := d.dao.GetCorpByID(ctx, d.p.CorpID)
//		if err != nil {
//			log.ErrorContextf(ctx, "task(DocToQA) reportTokenFinance, GetCorpByID failed, err:%+v", err)
//			return err
//		}
//		if err = d.dao.ReportDocExtractQATokenDosage(ctx, corp, dosage); err != nil {
//			log.ErrorContextf(ctx, "task(DocToQA) reportTokenFinance, ReportDocExtractQATokenDosage "+
//				"failed, err:%+v", err)
//			return err
//		}
//	}
//
//	// 删除redis
//	if _, err = d.dao.RedisCli().Do(ctx, "DEL", key); err != nil {
//		log.ErrorContextf(ctx, "task(DocToQA) reportTokenFinance, Redis del failed, err:%+v", err)
//		return err
//	}
//	return nil
// }

// checkQuestionAndAnswer 支持相似问
func checkQuestionAndAnswer(ctx context.Context, question, answer string, simQuestion []string) error {
	cfg := config.App().DocQA
	question = strings.TrimSpace(question)
	answer = strings.TrimSpace(answer)
	if len([]rune(question)) < cfg.Question.MinLength {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooShort), cfg.Question.MinLength)
	}
	if len([]rune(question)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Question.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeQuestionTooLong, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooLong), i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Question.MaxLength))
	}
	if len([]rune(answer)) < cfg.Answer.MinLength {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooShort, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooShort), cfg.Answer.MinLength)
	}
	if len([]rune(answer)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Answer.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeAnswerTooLong, i18n.Translate(ctx, i18nkey.KeyQACharLengthTooLong), i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.Answer.MaxLength))
	}
	if len(simQuestion) == 0 {
		return nil
	}
	// 检查相似问
	if len(simQuestion) > config.App().DocQA.SimilarQuestionNumLimit {
		return errs.ErrWrapf(errs.ErrCodeSimilarQuestionExceedLimit,
			i18n.Translate(ctx, i18nkey.KeySimilarQuestionLimitExceeded),
			cfg.SimilarQuestionNumLimit)
	}
	for _, q := range simQuestion {
		s := strings.TrimSpace(q)
		if len([]rune(s)) < cfg.SimilarQuestion.MinLength {
			return errs.ErrWrapf(errs.ErrCodeQuestionTooShort, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooShort),
				cfg.SimilarQuestion.MinLength)
		}
		if len([]rune(s)) > cfg.SimilarQuestion.MaxLength {
			return errs.ErrWrapf(errs.ErrCodeQuestionTooLong, i18n.Translate(ctx, i18nkey.KeySimilarQuestionTooLong),
				cfg.SimilarQuestion.MaxLength)
		}
	}

	return nil
}

// hashString orgData生成hash
func hashString(s string) string {
	hashes := md5.New()
	hashes.Write([]byte(s))
	hashBytes := hashes.Sum(nil)
	return hex.EncodeToString(hashBytes)
}

// checkSegmentOrgDataExists 判断切片orgData在整个文档切片中是否有相同的
func checkSegmentOrgDataExists(ctx context.Context, segment *model.DocSegmentExtend, daoAPI dao.Dao, docID uint64) (
	bool, error) {
	lockKey := fmt.Sprintf("qbot:knowledge:lock:segment_exists:%d", docID)
	if err := daoAPI.Lock(ctx, lockKey, 10*time.Second); err != nil {
		log.ErrorContextf(ctx, "checkSegmentOrgDataExists is operating, err:%v", err)
		return false, errs.ErrOperateDoing
	}
	defer func() { _ = daoAPI.UnLock(ctx, lockKey) }()

	if segment == nil || segment.ID <= 0 {
		return false, errs.ErrSegmentNotFound
	}
	hashOrgData := hashString(segment.OrgData)
	log.DebugContextf(ctx, "task(DocToQA) checkSegmentOrgDataExists docID:%d|segmentID:%d|hashOrgData|%s|"+
		"segment.OrgData|%s", segment.DocID, segment.ID, hashOrgData, segment.OrgData)

	// redis 判断orgData是否存在
	// redis 不存在写入redis hash中,key使用docID
	key := fmt.Sprintf("%s%d", model.DocQaExistsOrgDataPreFix, docID)
	exists, err := redis.Bool(daoAPI.RedisCli().Do(ctx, "HEXISTS", key, hashOrgData))
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) checkSegmentOrgDataExists, Redis HEXISTS failed, err:%+v", err)
		return false, err
	}
	log.DebugContextf(ctx, "task(DocToQA) checkSegmentOrgDataExists, exists:%+v hashOrgData:%s", exists,
		hashOrgData)
	if !exists {
		log.DebugContextf(ctx, "task(DocToQA) checkSegmentOrgDataExists, 不存在保存hashOrgData:%s",
			hashOrgData)
		// 没有重复的将问题添加到 Redis 哈希中
		_, err = redis.Int64(daoAPI.RedisCli().Do(ctx, "HSET", key, hashOrgData, "processed"))
		if err != nil {
			log.ErrorContextf(ctx, "task(DocToQA) checkSegmentOrgDataExists, Redis HSET failed, err:%+v", err)
			return false, err
		}
	}
	return exists, err
}

// checkRunProcess 检查是否可以执行任务
func (d *DocToQAScheduler) checkRunProcess(ctx context.Context) bool {
	qaTask, err := d.dao.GetDocQATaskByID(ctx, d.p.QaTaskID, d.p.CorpID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToQA) Process, checkRunProcess GetDocQATaskByID failed, "+
			"err:%+v|DocToQAParams|%v",
			err, d.p)
		return false
	}
	if qaTask == nil || qaTask.Status != model.DocQATaskStatusGenerating {
		log.InfoContextf(ctx, "task(DocToQA) Process, checkRunProcess status stop|qaTask|%v|DocToQAParams|%v",
			qaTask, d.p)
		return false
	}
	return true
}
