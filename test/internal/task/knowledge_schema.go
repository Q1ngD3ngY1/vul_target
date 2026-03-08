// Package task 送审任务
package task

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	logicCorp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/corp"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	jsoniter "github.com/json-iterator/go"
	"gorm.io/gorm"
	"math"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/algorithm/kmeans"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/algorithm/kmeans/cluster"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_schema"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/billing"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/dialogue-platform/common/v3/utils/rpcutils"
)

const (
	defaultDocProcessBatchSize = 20    // 默认插入Vector分批同步数量
	defaultMaxProcessDocCount  = 30000 // 默认插入Vector分批同步数量
	defaultDocClusterThreshold = 100   // 默认文档聚类阈值
)

var (
	statusCodeMessage = map[uint32]string{
		model.TaskStatusStopCodeModelQuoteLimit: "知识库Schema生成模型无法使用",
		model.TaskStatusFailCodeDocCountLimit:   "知识容量已达知识库检索Agent的使用上限，知识库Schema无法继续更新，可尝试删除不必要文档。",
	}
)

// KnowledgeGenerateSchemaScheduler 生成schema任务
type KnowledgeGenerateSchemaScheduler struct {
	dao    dao.Dao
	task   task_scheduler.Task
	params *model.KnowledgeGenerateSchemaParams

	isSharedKnowledge            bool     // 是否为共享知识库
	referShareKnowledgeBizIDList []uint64 // 引用的共享知识库BizID列表

	// 计费相关
	tokenDosage *billing.TokenDosage
}

func initKnowledgeGenerateSchemaScheduler() {
	task_scheduler.Register(
		model.KnowledgeGenerateSchemaTask,
		func(t task_scheduler.Task, params model.KnowledgeGenerateSchemaParams) task_scheduler.TaskHandler {
			return &KnowledgeGenerateSchemaScheduler{
				dao:    dao.New(),
				task:   t,
				params: &params,
			}
		},
	)
}

// Prepare 数据准备
func (d *KnowledgeGenerateSchemaScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Prepare, task: %+v, params: %+v", d.task, d.params)
	// 准备批处理文档数据
	kv := make(task_scheduler.TaskKV)
	// 更新任务状态为处理中
	task := &model.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     model.TaskStatusProcessing,
	}
	err := dao.GetKnowledgeSchemaTaskDao().UpdateKnowledgeSchemaTask(ctx, nil,
		[]string{dao.KnowledgeSchemaTaskTblColStatus}, task)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Prepare UpdateKnowledgeSchemaTask err:%+v", err)
		return kv, err
	}

	app, err := client.GetAppInfo(ctx, d.params.AppBizID, model.AppTestScenes)
	if err != nil {
		return nil, err
	}
	if app == nil || app.GetIsDelete() {
		return nil, errs.ErrRobotNotFound
	}

	appShareKnowledgeBizIDList, err := d.getAppShareKnowledgeBizIDList(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) getAppShareKnowledgeBizIDList fail err:%+v", err)
		return kv, err
	}
	d.referShareKnowledgeBizIDList = appShareKnowledgeBizIDList

	docBizIDs, err := d.getDocBizIDs(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) getDocBizIDs fail err:%+v", err)
		return kv, err
	}

	batchSize := utilConfig.GetMainConfig().KnowledgeSchema.DocProcessBatchSize
	if batchSize <= 0 {
		batchSize = defaultDocProcessBatchSize
	}
	for index, docChunks := range slicex.Chunk(docBizIDs, batchSize) {
		docChunksStr, err := jsoniter.MarshalToString(docChunks)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Prepare jsoniter.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Prepare index:%d, docBizIds: %+v", index, docChunksStr)
		kv[strconv.Itoa(index)] = docChunksStr
	}
	return kv, nil
}

// getAppShareKnowledgeBizIDList 获取应用引用的共享知识库BizID列表
func (d *KnowledgeGenerateSchemaScheduler) getAppShareKnowledgeBizIDList(ctx context.Context) ([]uint64, error) {
	shareKGList, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, d.params.AppBizID)
	if err != nil {
		log.ErrorContextf(ctx, "getAppShareKnowledgeBizIDList GetAppShareKGList fail, err: %+v", err)
		return nil, err
	}
	if len(shareKGList) == 0 {
		log.InfoContextf(ctx, "getAppShareKnowledgeBizIDList AppShareKG is nil, appBizID=%+v", d.params.AppBizID)
		return nil, nil
	}
	appShareKnowledgeBizIDList := make([]uint64, 0, len(shareKGList))
	for _, shareKGInfo := range shareKGList {
		appShareKnowledgeBizIDList = append(appShareKnowledgeBizIDList, shareKGInfo.KnowledgeBizID)
	}
	return appShareKnowledgeBizIDList, nil
}

func (d *KnowledgeGenerateSchemaScheduler) getDocBizIDs(ctx context.Context) ([]uint64, error) {
	// 先获取当前应用的
	appDB, err := d.dao.GetAppByAppBizID(ctx, d.params.AppBizID)
	if err != nil {
		return nil, err
	}
	if appDB == nil {
		return nil, errs.ErrRobotNotFound
	}
	// 只需要生成待发布、已发布文档的schema
	isDeleted := dao.IsNotDeleted
	filter := &dao.DocFilter{
		CorpId:    appDB.CorpID,
		RobotId:   appDB.ID,
		Status:    []uint32{model.DocStatusWaitRelease, model.DocStatusReleaseSuccess},
		IsDeleted: &isDeleted,
	}
	selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId, dao.DocTblColCharSize}
	docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, filter)
	if err != nil {
		log.ErrorContextf(ctx, "getDocBizIDs GetDocList fail,appBizID=%+v, err=%+v", d.params.AppBizID, err)
		return nil, err
	}

	docBizIds := make([]uint64, 0)
	for _, doc := range docs {
		if doc.CharSize == 0 {
			continue
		}
		docBizIds = append(docBizIds, doc.BusinessID)
	}

	if len(d.referShareKnowledgeBizIDList) == 0 {
		return docBizIds, nil
	}

	// 再获取app引用的共享知识库的文档
	for _, shareKnowledgeBizID := range d.referShareKnowledgeBizIDList {
		appDB, err = d.dao.GetAppByAppBizID(ctx, shareKnowledgeBizID)
		if err != nil {
			log.ErrorContextf(ctx, "getDocBizIDs GetAppByAppBizID fail, "+
				"appBizID=%+v, err=%+v", shareKnowledgeBizID, err)
			return nil, err
		}
		if appDB == nil {
			log.DebugContextf(ctx, "getDocBizIDs appDB is nil, appBizID=%+v", shareKnowledgeBizID)
			continue
		}
		filter.CorpId = appDB.CorpID
		filter.RobotId = appDB.ID
		shareKnowledgeDocs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "getDocBizIDs GetDocList fail, appBizID=%+v, err=%+v", shareKnowledgeBizID, err)
			return nil, err
		}
		for _, doc := range shareKnowledgeDocs {
			if doc.CharSize == 0 {
				continue
			}
			docBizIds = append(docBizIds, doc.BusinessID)
		}
	}

	return docBizIds, nil
}

// Init 初始化
func (d *KnowledgeGenerateSchemaScheduler) Init(ctx context.Context, taskKvMap task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Init, task: %+v, params: %+v", d.task, d.params)
	docCount := 0
	for _, taskValue := range taskKvMap {
		docBizIds := make([]uint64, 0)
		err := jsoniter.UnmarshalFromString(taskValue, &docBizIds)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Init jsoniter.UnmarshalFromString err:%+v", err)
			return err
		}
		docCount += len(docBizIds)
	}
	maxProcessDocCount := getMaxProcessDocCountThreshold()
	if docCount > maxProcessDocCount {
		// 文档数量超过限制
		err := fmt.Errorf("task(KnowledgeGenerateSchema) Init docCount over limit docCount:%d, MaxProcessDocCount:%d",
			docCount, maxProcessDocCount)
		log.WarnContextf(ctx, "%v", err)
		d.params.StatusCode = model.TaskStatusFailCodeDocCountLimit
		d.params.Message = fmt.Sprintf("需要生成schema的文档数量超过%d限制", maxProcessDocCount)
		return err
	}
	d.params.NeedCluster = false
	docClusterThreshold := getDocClusterThreshold()
	if docCount > docClusterThreshold {
		// 文档数量超过限制，需要聚类
		d.params.NeedCluster = true
	}
	// token用量统计初始化
	dosage, err := logicCorp.GetTokenDosage(ctx, d.params.AppBizID, d.params.SummaryModelName,
		0)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Init err: %v", err)
		return err
	}
	if dosage == nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Init dosage is nil")
		return errs.ErrSystem
	}
	d.tokenDosage = dosage

	return nil
}

// Process 任务处理
func (d *KnowledgeGenerateSchemaScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process, task: %+v, params: %+v", d.task, d.params)
	var err error
	app, err := client.GetAppInfo(ctx, d.params.AppBizID, model.AppTestScenes)
	if err != nil {
		return err
	}
	if app == nil || app.GetIsDelete() {
		log.WarnContextf(ctx, "task(KnowledgeGenerateSchema) Process app is nil or is delete, appBizID=%+v", d.params.AppBizID)
		progress.Stop(ctx)
		return nil
	}

	taskKvMap := progress.TaskKV(ctx)
	allEmbedding := cluster.Observations{}
	docSchemas := make(map[uint64]*model.DocSchema)
	for taskKey, taskValue := range taskKvMap {
		// 如果token额度不足，则停止处理，并更新任务状态为中止
		// XXX：放在这里其实不太好，因为token的上报只存在于for循环外的generateKnowledgeSchema方法中，并且任务不会并发执行，所以这里每次查询的结果应该是一样的
		if !logicCorp.CheckModelStatus(ctx, d.dao, d.params.CorpID, d.tokenDosage.ModelName, client.KnowledgeSchemaFinanceBizType) {
			log.InfoContextf(ctx, "task(KnowledgeGenerateSchema) Process token dosage not enough, stop process,"+
				" AppBizID=%+v", d.params.AppBizID)
			d.params.StatusCode = model.TaskStatusStopCodeModelQuoteLimit
			d.params.Message = getStatusCodeMessage(model.TaskStatusStopCodeModelQuoteLimit)
			progress.Stop(ctx)
			return nil
		}
		docBizIds := make([]uint64, 0)
		err = jsoniter.UnmarshalFromString(taskValue, &docBizIds)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process jsoniter.UnmarshalFromString err:%+v", err)
			return err
		}
		isDeleted := dao.IsNotDeleted
		docFilter := &dao.DocFilter{
			CorpId:      d.params.CorpID,
			BusinessIds: docBizIds,
			IsDeleted:   &isDeleted,
		}
		selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId, dao.DocTblColFileName, dao.DocTblColFileType}
		docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, docFilter)
		if err != nil {
			return err
		}
		docBizId2Id := make(map[uint64]uint64)
		for _, doc := range docs {
			docBizId2Id[doc.BusinessID] = doc.ID
		}
		// 批量查询已经生成过schema和embedding的文档
		docSchemaFilter := &dao.DocSchemaFilter{
			CorpBizId: d.params.CorpBizID,
			AppBizId:  d.params.AppBizID,
			DocBizIds: docBizIds,
		}
		selectColumns = []string{dao.DocSchemaTblColDocBizId, dao.DocSchemaTblColFileName, dao.DocSchemaTblColSummary,
			dao.DocSchemaTblColVector}
		existDocSchemas, err := dao.GetDocSchemaDao().GetDocSchemaList(ctx, selectColumns, docSchemaFilter)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process GetDocSchemaList err:%+v", err)
			return err
		}
		log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process existDocSchemas length:%d",
			len(existDocSchemas))
		docSchemaNeedDelete := make([]uint64, 0)
		for _, docSchema := range existDocSchemas {
			docId, ok := docBizId2Id[docSchema.DocBizID]
			if !ok {
				docSchemaNeedDelete = append(docSchemaNeedDelete, docSchema.DocBizID)
				continue
			}
			docSchemas[docId] = docSchema
		}

		if len(docSchemaNeedDelete) > 0 {
			err = dao.GetDocSchemaDao().DeleteDocSchema(ctx, nil, d.params.CorpBizID, d.params.AppBizID, docSchemaNeedDelete)
			if err != nil {
				// 清理无效的数据，不影响主流程
				log.WarnContextf(ctx, "task(KnowledgeGenerateSchema) Process DeleteDocSchema err:%+v", err)
			}
		}

		for _, doc := range docs {
			summary := ""
			isNewDocSchema := false
			if _, ok := docSchemas[doc.ID]; !ok {
				isStructFile := false
				if model.StructFileTypeMap[doc.FileType] && !d.params.NeedCluster {
					// 结构化文档在非聚类场景，需要特殊处理，支持text2sql
					summary, isStructFile, err = getStructFileSummary(ctx, d.params, doc)
					if err != nil {
						log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process getStructFileSummary docBizId:%d err: %v",
							doc.BusinessID, err)
						return err
					}
				}
				if !isStructFile {
					// 非结构化文档，或者聚类场景
					summary, err = d.getCommonDocSummary(ctx, app, doc)
					if err != nil {
						if errors.Is(err, errs.ErrOverModelTokenLimit) {
							log.InfoContextf(ctx, "task(KnowledgeGenerateSchema) Process token dosage not enough, stop process,"+
								" AppBizID=%+v", d.params.AppBizID)
							d.params.StatusCode = model.TaskStatusStopCodeModelQuoteLimit
							d.params.Message = getStatusCodeMessage(model.TaskStatusStopCodeModelQuoteLimit)
							progress.Stop(ctx)
							return nil
						}
						log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary docBizId:%d err: %v",
							doc.BusinessID, err)
						return err
					}
				}
				if summary == "" {
					// 摘要为空就不用再进行后续的embedding和写入数据库了
					log.WarnContextf(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary docBizId:%d summary is empty",
						doc.BusinessID)
					continue
				}
				newDocSchema := &model.DocSchema{
					CorpBizID: d.params.CorpBizID,
					AppBizID:  d.params.AppBizID,
					DocBizID:  doc.BusinessID,
					DocID:     doc.ID,
					FileName:  doc.FileName,
					Summary:   summary,
					Vector:    make([]byte, 0), // 必须有默认值，不然插入数据库会失败
					IsDeleted: dao.IsNotDeleted,
				}
				docSchemas[doc.ID] = newDocSchema
				isNewDocSchema = true
			}

			docSchema := docSchemas[doc.ID]
			docSchema.CorpBizID = d.params.CorpBizID
			docSchema.AppBizID = d.params.AppBizID
			docSchema.DocBizID = doc.BusinessID
			docSchema.DocID = doc.ID
			docSchema.FileName = doc.FileName // 兼容文件名修改的场景
			// 如果是已经拼接过摘要(以[{开头 且 }]结尾)的结构化文档，在聚类的时候也需要重新拿文档内容生成摘要，同时要更新到数据库
			updateColumns := []string{dao.DocSchemaTblColVector}
			if model.StructFileTypeMap[doc.FileType] && d.params.NeedCluster &&
				strings.HasPrefix(docSchema.Summary, "[{") && strings.HasSuffix(docSchema.Summary, "}]") {
				summary, err = d.getCommonDocSummary(ctx, app, doc)
				if err != nil {
					if errors.Is(err, errs.ErrOverModelTokenLimit) {
						log.InfoContextf(ctx, "task(KnowledgeGenerateSchema) Process token dosage not enough, stop process,"+
							" AppBizID=%+v", d.params.AppBizID)
						d.params.StatusCode = model.TaskStatusStopCodeModelQuoteLimit
						d.params.Message = getStatusCodeMessage(model.TaskStatusStopCodeModelQuoteLimit)
						progress.Stop(ctx)
						return nil
					}
					log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary docBizId:%d err: %v",
						doc.BusinessID, err)
					return err
				}
				docSchema.Summary = summary
				docSchema.Vector = []byte{} // 摘要变了，需要重新embedding，所以需要清空向量
				updateColumns = append(updateColumns, dao.DocSchemaTblColSummary)
			}

			if d.params.NeedCluster {
				log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process d.params.NeedCluster:%+v",
					d.params.NeedCluster)
				float32Vector := make([]float32, 0)
				if len(docSchema.Vector) == 0 {
					// 该文档在数据库中无向量结果
					// 获取embedding结果
					vector, err := client.Embedding(ctx, d.params.AppBizID, docSchema.Summary)
					if err != nil {
						log.WarnContextf(ctx, "task(KnowledgeGenerateSchema) Process embedding failed, docID:%d err: %v", docSchema.DocID, err)
						continue
						// return err 这里不返回，因为embedding失败的文档可以继续处理
					}
					if len(vector) == 0 {
						log.WarnContextf(ctx, "task(KnowledgeGenerateSchema) Process embedding vector is empty, docID:%d", docSchema.DocID)
						continue
						// return err 这里不返回，因为embedding失败的文档可以继续处理
					}
					docSchema.Vector = util.FloatsToBytes(vector)
					float32Vector = vector
					if !isNewDocSchema {
						// 非新增文档schema，需要更新数据库
						err = dao.GetDocSchemaDao().UpdateDocSchema(ctx, nil, updateColumns, docSchema)
						if err != nil {
							log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process UpdateDocSchema err:%+v", err)
							return err
						}
					}
				} else {
					// 该文档在数据库里已有向量结果
					float32Vector = util.BytesToFloats(docSchema.Vector)
				}
				float64Vector := make([]float64, len(float32Vector))
				for i, v := range float32Vector {
					float64Vector[i] = float64(v)
				}
				allEmbedding = append(allEmbedding, cluster.Coordinates{
					ID:     strconv.FormatUint(doc.ID, 10),
					Vector: float64Vector,
				})
			}

			// 先把新文档摘要和向量写入数据库，避免后续重复生成
			if isNewDocSchema {
				err = dao.GetDocSchemaDao().CreateDocSchema(ctx, docSchema)
				if err != nil {
					log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process CreateDocSchema err:%+v", err)
					return err
				}
			}
		}
		log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process taskKey:%s, docs length:%d", taskKey, len(docs))
	}

	err = d.generateKnowledgeSchema(ctx, app, allEmbedding, docSchemas)
	if err != nil {
		if errors.Is(err, errs.ErrOverModelTokenLimit) {
			log.InfoContextf(ctx, "task(KnowledgeGenerateSchema) Process token dosage not enough, stop process,"+
				" AppBizID=%+v", d.params.AppBizID)
			d.params.StatusCode = model.TaskStatusStopCodeModelQuoteLimit
			d.params.Message = getStatusCodeMessage(model.TaskStatusStopCodeModelQuoteLimit)
			progress.Stop(ctx)
			return nil
		}
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process generateKnowledgeSchema err:%+v", err)
		return err
	}

	err = d.generateSharedKnowledgeSchemaTask(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process generateSharedKnowledgeSchemaTask err:%+v", err)
		return err
	}

	for taskKey := range taskKvMap {
		if err := progress.Finish(ctx, taskKey); err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Finish kv:%s err:%+v", taskKey, err)
			return err
		}
	}

	return nil
}

// Fail 任务失败
func (d *KnowledgeGenerateSchemaScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Fail, appBizId id: %v", d.params.AppBizID)
	// 更新任务状态
	task := &model.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     model.TaskStatusFailed,
		StatusCode: d.params.StatusCode,
		Message:    d.params.Message,
	}
	err := dao.GetKnowledgeSchemaTaskDao().UpdateKnowledgeSchemaTask(ctx, nil,
		[]string{
			dao.KnowledgeSchemaTaskTblColStatus,
			dao.KnowledgeSchemaTaskTblColStatusCode,
			dao.KnowledgeSchemaTaskTblColMessage,
		}, task)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Fail UpdateKnowledgeSchemaTask err:%+v", err)
		return err
	}

	return nil
}

// Stop 任务停止
func (d *KnowledgeGenerateSchemaScheduler) Stop(ctx context.Context) error {
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Stop, appBizId id: %v", d.params.AppBizID)
	// 更新任务状态
	task := &model.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     model.TaskStatusStop,
		StatusCode: d.params.StatusCode,
		Message:    d.params.Message,
	}
	err := dao.GetKnowledgeSchemaTaskDao().UpdateKnowledgeSchemaTask(ctx, nil,
		[]string{
			dao.KnowledgeSchemaTaskTblColStatus,
			dao.KnowledgeSchemaTaskTblColStatusCode,
			dao.KnowledgeSchemaTaskTblColMessage,
		},
		task)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Stop UpdateKnowledgeSchemaTask err:%+v", err)
		return err
	}

	return nil
}

// Done 任务完成回调
func (d *KnowledgeGenerateSchemaScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Done, appBizId id: %v", d.params.AppBizID)
	// 更新任务状态
	task := &model.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     model.TaskStatusSuccess,
	}
	err := dao.GetKnowledgeSchemaTaskDao().UpdateKnowledgeSchemaTask(ctx, nil,
		[]string{dao.KnowledgeSchemaTaskTblColStatus}, task)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Done UpdateKnowledgeSchemaTask err:%+v", err)
		return err
	}

	return nil
}

// DocCluster 文档聚类
// (d *KnowledgeGenerateSchemaScheduler)
func (d *KnowledgeGenerateSchemaScheduler) docCluster(
	ctx context.Context,
	app *admin.GetAppInfoRsp,
	allEmbedding cluster.Observations,
	allDocSchema map[uint64]*model.DocSchema,
	clusterCount int) ([]*model.DocClusterSchema, error) {
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process allEmbedding length:%d allDocSchema length:%d clusterCount:%d",
		len(allEmbedding), len(allDocSchema), clusterCount)
	if clusterCount <= 1 {
		return nil, errors.New("clusterCount must be greater than 1")
	}
	// 文档按向量聚类
	km := kmeans.New()
	clusters, err := km.Partition(allEmbedding, clusterCount)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process error partitioning with more clusters than data points, got nil")
		return nil, err
	}

	// 获取当前maxVersion
	maxVersion, err := dao.GetDocClusterSchemaDao().GetDocClusterSchemaDaoMaxVersion(ctx, d.params.AppBizID)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process error, GetDocClusterSchemaDaoMaxVersion err:%+v", err)
		return nil, err
	}

	// 向量聚类结果转换成文档聚类结果
	docClusterSchemaList := make([]*model.DocClusterSchema, 0)
	for _, clusterItem := range clusters {
		clusterSchema := &model.DocClusterSchema{
			CorpBizID:  d.params.CorpBizID,
			AppBizID:   d.params.AppBizID,
			BusinessID: d.dao.GenerateSeqID(),
			IsDeleted:  dao.IsNotDeleted,
		}
		docClusterSchemaList = append(docClusterSchemaList, clusterSchema)
		kbFileInfos := make([]model.KBFileInfo, 0)
		docIds := make([]uint64, 0)
		for _, observation := range clusterItem.Observations {
			docID, err := strconv.ParseUint(observation.Coordinates().ID, 10, 64)
			if err != nil {
				log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process error, strconv.ParseUint err:%+v", err)
				return nil, err
			}
			docSchema, ok := allDocSchema[docID]
			if !ok {
				log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process error, docID:%d not found", docID)
				return nil, err
			}
			kbFileInfos = append(kbFileInfos, model.KBFileInfo{
				FileName:    docSchema.FileName,
				FileSummary: docSchema.Summary,
			})
			docIds = append(docIds, docID)
		}
		docIdsJson, err := jsoniter.Marshal(docIds)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process error, jsoniter.Marshal err:%+v", err)
			return nil, err
		}
		clusterSchema.DocIDs = string(docIdsJson)

		clusterName, clusterSummary, err := d.getDirSummary(ctx, app, kbFileInfos)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process error, getDirSummary err:%+v", err)
			return nil, err
		}

		clusterSchema.ClusterName = clusterName
		clusterSchema.Summary = clusterSummary
		clusterSchema.Version = maxVersion + 1
	}

	// 事务写入cluster schema表，避免写入一半失败
	if err := dao.GetTdsqlGormDb(ctx).Transaction(func(tx *gorm.DB) error {
		for _, clusterSchema := range docClusterSchemaList {
			err = dao.GetDocClusterSchemaDao().CreateDocClusterSchema(ctx, tx, clusterSchema)
			if err != nil {
				log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process CreateDocClusterSchema err:%+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process err: %+v", err)
		return nil, err
	}

	return docClusterSchemaList, nil
}

func getDocClusterThreshold() int {
	return utils.When(utilConfig.GetMainConfig().KnowledgeSchema.DocClusterThreshold > 0,
		utilConfig.GetMainConfig().KnowledgeSchema.DocClusterThreshold, defaultDocClusterThreshold)
}

func getMaxProcessDocCountThreshold() int {
	return utils.When(utilConfig.GetMainConfig().KnowledgeSchema.MaxProcessDocCount > 0,
		utilConfig.GetMainConfig().KnowledgeSchema.MaxProcessDocCount, defaultMaxProcessDocCount)
}

func getStatusCodeMessage(statusCode uint32) string {
	return utils.When(utilConfig.GetMainConfig().KnowledgeSchema.TaskStatusCodeMessage[statusCode] != "",
		utilConfig.GetMainConfig().KnowledgeSchema.TaskStatusCodeMessage[statusCode], statusCodeMessage[statusCode])
}

func getStructFileSummary(ctx context.Context, params *model.KnowledgeGenerateSchemaParams, doc *model.Doc) (string, bool, error) {
	// 非聚类场景，结构化文档需要特殊处理，支持text2sql
	// 先走结构化逻辑
	var err error
	summary := ""
	isStructFile := false
	text2sqlMeta := make([]model.Text2sqlMetaMappingPreview, 0)
	isStructFile, text2sqlMeta, err = dao.GetDocMetaDataDao().GetDocMetaDataForSchema(
		ctx, doc.ID, params.AppID, model.RunEnvSandbox)
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process StructFile:%s isStructFile:%+v",
		doc.FileName, isStructFile)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process GetDocMetaDataForSchema err:%+v", err)
		return "", false, err
	}

	if isStructFile {
		bytes, err := jsoniter.Marshal(text2sqlMeta)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process jsoniter.Marshal err:%+v", err)
			return "", false, err
		}
		summary = string(bytes)
	}
	return summary, isStructFile, nil
}

func (d *KnowledgeGenerateSchemaScheduler) getCommonDocSummary(ctx context.Context, app *admin.GetAppInfoRsp, doc *model.Doc) (string, error) {
	if !logicCorp.CheckModelStatus(ctx, d.dao, d.params.CorpID, d.params.SummaryModelName, client.KnowledgeSchemaFinanceBizType) {
		return "", errs.ErrOverModelTokenLimit
	}
	d.tokenDosage.StartTime = time.Now()
	getKBDocSummaryReq := &model.GetKBDocSummaryReq{
		RobotID:   d.params.AppID,
		BotBizId:  d.params.AppBizID,
		RequestId: rpcutils.GetDyeingKey(ctx),
		DocID:     doc.ID,
		FileName:  doc.FileName,
		ModelName: d.params.SummaryModelName,
	}
	summary, tokenStatisticInfo, err := knowledge_schema.KBAgentGetOneDocSummary(ctx, d.dao, app, getKBDocSummaryReq)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary err:%+v", err)
		return "", err
	}
	err = logicCorp.ReportTokenDosage(ctx, tokenStatisticInfo, d.tokenDosage, d.dao, d.params.CorpID, client.KnowledgeSchemaFinanceBizType, app.GetAppBizId())
	if err != nil {
		// 只打印ERROR日志，降级处理
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process logicCorp.ReportTokenDosage err:%+v", err)
	}
	return summary, nil
}

func (d *KnowledgeGenerateSchemaScheduler) getDirSummary(ctx context.Context, app *admin.GetAppInfoRsp, kbFileInfos []model.KBFileInfo) (string, string, error) {
	if !logicCorp.CheckModelStatus(ctx, d.dao, d.params.CorpID, d.params.SummaryModelName, client.KnowledgeSchemaFinanceBizType) {
		return "", "", errs.ErrOverModelTokenLimit
	}
	d.tokenDosage.StartTime = time.Now()
	req := &model.GetKBDirSummaryReq{
		BotBizId:  d.params.AppBizID,
		RequestId: rpcutils.GetDyeingKey(ctx),
		FileInfos: kbFileInfos,
		ModelName: d.params.SummaryModelName,
	}
	clusterName, clusterSummary, tokenStatisticInfo, err := knowledge_schema.KBAgentGetDirSummary(ctx, d.dao, app, req)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process error, KBAgentGetDirSummary err:%+v", err)
		return "", "", err
	}
	err = logicCorp.ReportTokenDosage(ctx, tokenStatisticInfo, d.tokenDosage, d.dao, d.params.CorpID, client.KnowledgeSchemaFinanceBizType, app.GetAppBizId())
	if err != nil {
		// 只打印ERROR日志，降级处理
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process logicCorp.ReportTokenDosage err:%+v", err)
	}
	return clusterName, clusterSummary, nil
}

func (d *KnowledgeGenerateSchemaScheduler) generateKnowledgeSchema(
	ctx context.Context,
	app *admin.GetAppInfoRsp,
	allEmbedding cluster.Observations,
	docSchemas map[uint64]*model.DocSchema) error {
	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process generateKnowledgeSchema params:%+v "+
		"allEmbedding length:%d docSchemas length:%d", d.params, len(allEmbedding), len(docSchemas))
	knowledgeSchemaList := make([]*model.KnowledgeSchema, 0)
	maxVersion, err := dao.GetKnowledgeSchemaDao().GetKnowledgeSchemaMaxVersion(ctx, d.params.AppBizID)
	if err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process GetKnowledgeSchemaMaxVersion err:%+v", err)
		return err
	}
	docClusterSchemaList := make([]*model.DocClusterSchema, 0)

	docClusterThreshold := getDocClusterThreshold()
	clusterCount := int(math.Ceil(float64(len(allEmbedding)) / float64(docClusterThreshold)))
	if clusterCount <= 1 {
		// 拿到的embedding结果效果单个集合大小阈值，不用聚类了
		d.params.NeedCluster = false
	}

	if d.params.NeedCluster {
		log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process d.params.NeedCluster:%+v",
			d.params.NeedCluster)
		docClusterSchemaList, err = d.docCluster(ctx, app, allEmbedding, docSchemas, clusterCount)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process DocCluster err:%+v", err)
			return err
		}
		for _, docClusterSchema := range docClusterSchemaList {
			knowledgeSchema := &model.KnowledgeSchema{
				CorpBizId: d.params.CorpBizID,
				AppBizId:  d.params.AppBizID,
				Version:   maxVersion + 1,
				ItemType:  model.KnowledgeSchemaItemTypeDocCluster,
				ItemBizId: docClusterSchema.BusinessID,
				Name:      docClusterSchema.ClusterName,
				Summary:   docClusterSchema.Summary,
				IsDeleted: dao.IsNotDeleted,
			}
			knowledgeSchemaList = append(knowledgeSchemaList, knowledgeSchema)
		}
	} else {
		for _, docSchema := range docSchemas {
			knowledgeSchema := &model.KnowledgeSchema{
				CorpBizId: d.params.CorpBizID,
				AppBizId:  d.params.AppBizID,
				Version:   maxVersion + 1,
				ItemType:  model.KnowledgeSchemaItemTypeDoc,
				ItemBizId: docSchema.DocID,
				Name:      docSchema.FileName,
				Summary:   docSchema.Summary,
				IsDeleted: dao.IsNotDeleted,
			}
			knowledgeSchemaList = append(knowledgeSchemaList, knowledgeSchema)
		}
	}

	log.DebugContextf(ctx, "task(KnowledgeGenerateSchema) Process knowledgeSchemaList length:%d",
		len(knowledgeSchemaList))
	// 事务写入知识库schema表，避免写入一半的时候被发布任务快照
	if err := dao.GetTdsqlGormDb(ctx).Transaction(func(tx *gorm.DB) error {
		// 先【硬性】删除所有旧版本，再写入新版本
		err = dao.GetKnowledgeSchemaDao().DeleteKnowledgeSchema(ctx, tx, d.params.CorpBizID, d.params.AppBizID)
		if err != nil {
			log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process DeleteKnowledgeSchema err:%+v", err)
			return err
		}
		for _, knowledgeSchema := range knowledgeSchemaList {
			err = dao.GetKnowledgeSchemaDao().CreateKnowledgeSchema(ctx, tx, knowledgeSchema)
			if err != nil {
				log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process CreateKnowledgeSchema err:%+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process err: %+v", err)
		return err
	}

	if d.params.NeedCluster {
		// 生成成功，设置文件聚类目录缓存
		for _, docClusterSchema := range docClusterSchemaList {
			// 这里的envType要根据是否是共享知识库判断，因为共享知识库没有发布的流程，操作即生效，所以这里的envType是product
			if err := redis.SetKnowledgeSchemaDocIdByDocClusterId(ctx,
				d.params.AppBizID,
				utils.When(d.isSharedKnowledge, model.EnvTypeProduct, model.EnvTypeSandbox),
				docClusterSchema); err != nil {
				log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process redis.SetKnowledgeSchemaDocIdByDocClusterId fail, err: %+v", err)
				return err
			}
			if err := redis.SetKnowledgeSchemaAppBizIdByDocClusterId(ctx, docClusterSchema.BusinessID, d.params.AppBizID, model.EnvTypeSandbox); err != nil {
				log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process redis.SetKnowledgeSchemaAppBizIdByDocClusterId fail, err: %+v", err)
				return err
			}
		}
	}
	schemaItemsPbList := knowledge_schema.TransformKnowledgeSchema2Pb(knowledgeSchemaList)
	// 生成成功，设置schema缓存
	if err := redis.SetKnowledgeSchema(ctx, d.params.AppBizID, model.EnvTypeSandbox, schemaItemsPbList); err != nil {
		log.ErrorContextf(ctx, "task(KnowledgeGenerateSchema) Process redis.SetKnowledgeSchema fail, err: %+v", err)
		return err
	}

	return nil
}

// generateSharedKnowledgeSchemaTask 查询应用下所有引用的共享知识库，生成共享知识库的schema任务
func (d *KnowledgeGenerateSchemaScheduler) generateSharedKnowledgeSchemaTask(ctx context.Context) error {
	for _, appBizID := range d.referShareKnowledgeBizIDList {
		appDB, err := d.dao.GetAppByAppBizID(ctx, appBizID)
		if err != nil {
			log.ErrorContextf(ctx, "generateSharedKnowledgeSchemaTask GetAppByAppBizID fail, "+
				"appBizID=%+v, err=%+v", appBizID, err)
			return err
		}
		if appDB == nil {
			log.DebugContextf(ctx, "generateSharedKnowledgeSchemaTask appDB is nil, appBizID=%+v", appBizID)
			continue
		}
		err = knowledge_schema.GenerateKnowledgeSchema(ctx, d.dao, d.params.CorpID, d.params.CorpBizID, appBizID, d.dao.GenerateSeqID(), appDB.ID)
		if err != nil {
			log.ErrorContextf(ctx, "generateSharedKnowledgeSchemaTask GenerateKnowledgeSchema fail, "+
				"appBizID=%+v, err=%+v", appBizID, err)
		}
		log.DebugContextf(ctx, "generateSharedKnowledgeSchemaTask GenerateKnowledgeSchema success, appBizID=%+v", appBizID)
	}
	return nil
}
