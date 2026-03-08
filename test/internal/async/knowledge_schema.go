// Package task 送审任务
package async

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/logic/document"
	llmLogic "git.woa.com/adp/kb/kb-config/internal/logic/llm"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"github.com/google/uuid"

	"git.code.oa.com/trpc-go/trpc-go/codec"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	kbLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util/algorithm/kmeans"
	"git.woa.com/adp/kb/kb-config/internal/util/algorithm/kmeans/cluster"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	llmm "git.woa.com/dialogue-platform/proto/pb-stub/llm-manager-server"
)

const (
	defaultDocProcessBatchSize = 20    // 默认插入Vector分批同步数量
	defaultMaxProcessDocCount  = 30000 // 默认插入Vector分批同步数量
	defaultDocClusterThreshold = 100   // 默认文档聚类阈值
)

var (
	statusCodeMessage = map[uint32]string{
		entity.TaskStatusStopCodeModelQuoteLimit: "知识库Schema生成模型无法使用",
		entity.TaskStatusFailCodeDocCountLimit:   "知识容量已达知识库检索Agent的使用上限，知识库Schema无法继续更新，可尝试删除不必要文档。",
	}
)

// KnowledgeGenerateSchemaTaskHandler 生成schema任务
type KnowledgeGenerateSchemaTaskHandler struct {
	*taskCommon

	task   task_scheduler.Task
	params *kbEntity.KnowledgeGenerateSchemaParams
	kbDao  kbdao.Dao
	tdsql  *tdsqlquery.Query

	isSharedKnowledge            bool     // 是否为共享知识库
	referShareKnowledgeBizIDList []uint64 // 引用的共享知识库BizID列表

	// 计费相关
	tokenStatisticInfos []*llmm.StatisticInfo
	tokenDosage         *finance.TokenDosage
}

func registerKnowledgeGenerateSchemaTaskHandler(tc *taskCommon, tdsql types.TDSQLDB, kbDao kbdao.Dao) {
	task_scheduler.Register(
		entity.KnowledgeGenerateSchemaTask,
		func(t task_scheduler.Task, params kbEntity.KnowledgeGenerateSchemaParams) task_scheduler.TaskHandler {
			return &KnowledgeGenerateSchemaTaskHandler{
				taskCommon: tc,
				kbDao:      kbDao,
				task:       t,
				params:     &params,
				tdsql:      tdsqlquery.Use(tdsql),
			}
		},
	)
}

// Prepare 数据准备
func (d *KnowledgeGenerateSchemaTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	logx.D(ctx, "task(KnowledgeGenerateSchema) Prepare, task: %+v, params: %+v", d.task, d.params)
	// 准备批处理文档数据
	kv := make(task_scheduler.TaskKV)
	// 更新任务状态为处理中
	task := &kbEntity.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     entity.TaskStatusProcessing,
	}
	err := d.kbDao.UpdateKnowledgeSchemaTask(ctx, []string{kbEntity.KnowledgeSchemaTaskTblColStatus}, task)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Prepare UpdateKnowledgeSchemaTask err:%+v", err)
		return kv, err
	}

	appDB, err := d.taskCommon.rpc.AppAdmin.DescribeAppById(ctx, d.params.AppBizID)
	// appDB, err := d.dao.GetAppByAppBizID(ctx, d.params.AppBizID)
	if err != nil {
		return nil, err
	}
	if appDB == nil || appDB.HasDeleted() {
		return nil, errs.ErrRobotNotFound
	}

	appShareKnowledgeBizIDList, err := d.getAppShareKnowledgeBizIDList(ctx)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) getAppShareKnowledgeBizIDList fail err:%+v", err)
		return kv, err
	}
	d.referShareKnowledgeBizIDList = appShareKnowledgeBizIDList

	docBizIDs, err := d.getDocBizIDs(ctx)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) getDocBizIDs fail err:%+v", err)
		return kv, err
	}

	batchSize := config.GetMainConfig().KnowledgeSchema.DocProcessBatchSize
	if batchSize <= 0 {
		batchSize = defaultDocProcessBatchSize
	}
	for index, docChunks := range slicex.Chunk(docBizIDs, batchSize) {
		docChunksStr, err := jsonx.MarshalToString(docChunks)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Prepare jsonx.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		logx.D(ctx, "task(KnowledgeGenerateSchema) Prepare index:%d, docBizIds: %+v", index, docChunksStr)
		kv[strconv.Itoa(index)] = docChunksStr
	}
	return kv, nil
}

// getAppShareKnowledgeBizIDList 获取应用引用的共享知识库BizID列表
func (d *KnowledgeGenerateSchemaTaskHandler) getAppShareKnowledgeBizIDList(ctx context.Context) ([]uint64, error) {
	shareKGList, err := d.kbDao.GetAppShareKGList(ctx, d.params.AppBizID)
	if err != nil {
		logx.E(ctx, "getAppShareKnowledgeBizIDList GetAppShareKGList fail, err: %+v", err)
		return nil, err
	}
	if len(shareKGList) == 0 {
		logx.I(ctx, "getAppShareKnowledgeBizIDList AppShareKG is nil, appBizID=%+v", d.params.AppBizID)
		return nil, nil
	}
	appShareKnowledgeBizIDList := make([]uint64, 0, len(shareKGList))
	for _, shareKGInfo := range shareKGList {
		appShareKnowledgeBizIDList = append(appShareKnowledgeBizIDList, shareKGInfo.KnowledgeBizID)
	}
	return appShareKnowledgeBizIDList, nil
}

func (d *KnowledgeGenerateSchemaTaskHandler) getDocBizIDs(ctx context.Context) ([]uint64, error) {
	// 先获取当前应用的
	appDB, err := d.taskCommon.rpc.AppAdmin.DescribeAppById(ctx, d.params.AppBizID)
	// appDB, err := d.dao.GetAppByAppBizID(ctx, d.params.AppBizID)
	if err != nil {
		return nil, err
	}
	if appDB == nil {
		return nil, errs.ErrRobotNotFound
	}
	// 只需要生成待发布、已发布文档的schema
	filter := &docEntity.DocFilter{
		CorpId:    appDB.CorpPrimaryId,
		RobotId:   appDB.PrimaryId,
		Status:    []uint32{docEntity.DocStatusWaitRelease, docEntity.DocStatusReleaseSuccess},
		IsDeleted: ptrx.Bool(false),
	}
	selectColumns := []string{docEntity.DocTblColId, docEntity.DocTblColBusinessId, docEntity.DocTblColCharSize}
	docs, err := d.docLogic.GetDocList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "getDocBizIDs GetDocList fail,appBizID=%+v, err=%+v", d.params.AppBizID, err)
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
		appDB, err := d.taskCommon.rpc.AppAdmin.DescribeAppById(ctx, shareKnowledgeBizID)
		// appDB, err = d.dao.GetAppByAppBizID(ctx, shareKnowledgeBizID)
		if err != nil {
			logx.E(ctx, "getDocBizIDs GetAppByAppBizID fail, "+
				"appBizID=%+v, err=%+v", shareKnowledgeBizID, err)
			return nil, err
		}
		if appDB == nil {
			logx.D(ctx, "getDocBizIDs appDB is nil, appBizID=%+v", shareKnowledgeBizID)
			continue
		}
		filter.CorpId = appDB.CorpPrimaryId
		filter.RobotId = appDB.PrimaryId
		shareKnowledgeDocs, err := d.docLogic.GetDocList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "getDocBizIDs GetDocList fail, appBizID=%+v, err=%+v", shareKnowledgeBizID, err)
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

func (d *KnowledgeGenerateSchemaTaskHandler) generateTokenDosage(appInfo *entity.App) *finance.TokenDosage {
	dosage := &finance.TokenDosage{
		AppID:           d.params.AppBizID,
		AppType:         appInfo.AppType,
		ModelName:       d.params.SummaryModelName,
		StartTime:       time.Now(),
		InputDosages:    []int{},
		OutputDosages:   []int{},
		KnowledgeBaseID: appInfo.BizId,
		SpaceID:         appInfo.SpaceId,
	}
	if appInfo.IsShared {
		dosage.AppType = rpc.TokenDosageAppTypeSharedKnowledge
		d.isSharedKnowledge = true
	}
	return dosage
}

// Init 初始化
func (d *KnowledgeGenerateSchemaTaskHandler) Init(ctx context.Context, taskKvMap task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	logx.D(ctx, "task(KnowledgeGenerateSchema) Init, task: %+v, params: %+v", d.task, d.params)
	docCount := 0
	for _, taskValue := range taskKvMap {
		docBizIds := make([]uint64, 0)
		err := jsonx.UnmarshalFromString(taskValue, &docBizIds)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Init jsonx.UnmarshalFromString err:%+v", err)
			return err
		}
		docCount += len(docBizIds)
	}
	maxProcessDocCount := getMaxProcessDocCountThreshold()
	if docCount > maxProcessDocCount {
		// 文档数量超过限制
		err := fmt.Errorf("task(KnowledgeGenerateSchema) Init docCount over limit docCount:%d, MaxProcessDocCount:%d",
			docCount, maxProcessDocCount)
		logx.W(ctx, "%v", err)
		d.params.StatusCode = entity.TaskStatusFailCodeDocCountLimit
		d.params.Message = fmt.Sprintf("需要生成schema的文档数量超过%d限制", maxProcessDocCount)
		return err
	}
	d.params.NeedCluster = false
	docClusterThreshold := getDocClusterThreshold()
	logx.D(ctx, "task(KnowledgeGenerateSchema) Init docCount:%d docClusterThreshold:%d",
		docCount, docClusterThreshold)
	if docCount > docClusterThreshold {
		// 文档数量超过限制，需要聚类
		d.params.NeedCluster = true
	}
	return nil
}

// Process 任务处理
func (d *KnowledgeGenerateSchemaTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(KnowledgeGenerateSchema) Process, task: %+v, params: %+v", d.task, d.params)
	app, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.params.AppBizID)
	if err != nil {
		return err
	}
	if app.HasDeleted() {
		logx.W(ctx, "task(KnowledgeGenerateSchema) Process app is nil or is delete, appBizID=%+v", d.params.AppBizID)
		progress.Stop(ctx)
		return nil
	}
	// 初始化计费
	d.tokenDosage = d.generateTokenDosage(app)

	taskKvMap := progress.TaskKV(ctx)
	allEmbedding := cluster.Observations{}
	docSchemas := make(map[uint64]*docEntity.DocSchema)
	for taskKey, taskValue := range taskKvMap {
		// 如果token额度不足，则停止处理，并更新任务状态为中止
		// XXX：放在这里其实不太好，因为token的上报只存在于for循环外的generateKnowledgeSchema方法中，并且任务不会并发执行，所以这里每次查询的结果应该是一样的
		if !d.financeLogic.CheckModelStatus(ctx, d.params.CorpID, d.params.SummaryModelName, rpc.KnowledgeSchemaFinanceBizType) {
			logx.I(ctx, "task(KnowledgeGenerateSchema) Process token dosage not enough, stop process,"+
				" AppBizID=%+v", d.params.AppBizID)
			d.params.StatusCode = entity.TaskStatusStopCodeModelQuoteLimit
			d.params.Message = getStatusCodeMessage(entity.TaskStatusStopCodeModelQuoteLimit)
			progress.Stop(ctx)
			return nil
		}
		docBizIds := make([]uint64, 0)
		err = jsonx.UnmarshalFromString(taskValue, &docBizIds)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Process jsonx.UnmarshalFromString err:%+v", err)
			return err
		}
		docFilter := &docEntity.DocFilter{
			CorpId:      d.params.CorpID,
			BusinessIds: docBizIds,
			IsDeleted:   ptrx.Bool(false),
		}
		selectColumns := []string{docEntity.DocTblColId, docEntity.DocTblColBusinessId, docEntity.DocTblColFileName, docEntity.DocTblColFileType}
		docs, err := d.docLogic.GetDocList(ctx, selectColumns, docFilter)
		if err != nil {
			return err
		}
		docBizId2Id := make(map[uint64]uint64)
		for _, doc := range docs {
			docBizId2Id[doc.BusinessID] = doc.ID
		}
		// 批量查询已经生成过schema和embedding的文档
		docSchemaFilter := &docEntity.DocSchemaFilter{
			CorpBizId: d.params.CorpBizID,
			AppBizId:  d.params.AppBizID,
			DocBizIds: docBizIds,
		}
		selectColumns = []string{docEntity.DocSchemaTblColDocBizId, docEntity.DocSchemaTblColFileName,
			docEntity.DocSchemaTblColSummary, docEntity.DocSchemaTblColVector}
		existDocSchemas, _, err := d.docLogic.GetDocSchemaCountAndList(ctx, selectColumns, docSchemaFilter)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Process GetDocSchemaList err:%+v", err)
			return err
		}
		logx.D(ctx, "task(KnowledgeGenerateSchema) Process existDocSchemas length:%d",
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
			err = d.docLogic.DeleteDocSchema(ctx, d.params.CorpBizID, d.params.AppBizID, docSchemaNeedDelete)
			if err != nil {
				// 清理无效的数据，不影响主流程
				logx.W(ctx, "task(KnowledgeGenerateSchema) Process DeleteDocSchema err:%+v", err)
			}
		}

		for _, doc := range docs {
			summary := ""
			isNewDocSchema := false
			if _, ok := docSchemas[doc.ID]; !ok {
				isStructFile := false
				if docEntity.StructFileTypeMap[doc.FileType] && !d.params.NeedCluster {
					// 结构化文档在非聚类场景，需要特殊处理，支持text2sql
					summary, isStructFile, err = d.getStructFileSummary(ctx, d.params, doc)
					if err != nil {
						logx.E(ctx, "task(KnowledgeGenerateSchema) Process getStructFileSummary docBizId:%d err: %v",
							doc.BusinessID, err)
						return err
					}
				}
				if !isStructFile {
					// 非结构化文档，或者聚类场景
					summary, err = d.getCommonDocSummary(ctx, app, doc)
					if err != nil {
						logx.E(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary docBizId:%d err: %v",
							doc.BusinessID, err)
						return err
					}
				}
				if summary == "" {
					// 摘要为空就不用再进行后续的embedding和写入数据库了
					logx.W(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary docBizId:%d summary is empty",
						doc.BusinessID)
					continue
				}
				newDocSchema := &docEntity.DocSchema{
					CorpBizID: d.params.CorpBizID,
					AppBizID:  d.params.AppBizID,
					DocBizID:  doc.BusinessID,
					DocID:     doc.ID,
					FileName:  doc.FileName,
					Summary:   summary,
					Vector:    make([]byte, 0), // 必须有默认值，不然插入数据库会失败
					IsDeleted: false,
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
			updateColumns := []string{docEntity.DocSchemaTblColVector}
			if docEntity.StructFileTypeMap[doc.FileType] && d.params.NeedCluster &&
				strings.HasPrefix(docSchema.Summary, "[{") && strings.HasSuffix(docSchema.Summary, "}]") {
				summary, err = d.getCommonDocSummary(ctx, app, doc)
				if err != nil {
					logx.E(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary docBizId:%d err: %v",
						doc.BusinessID, err)
					return err
				}
				docSchema.Summary = summary
				docSchema.Vector = []byte{} // 摘要变了，需要重新embedding，所以需要清空向量
				updateColumns = append(updateColumns, docEntity.DocSchemaTblColSummary)
			}

			if d.params.NeedCluster {
				logx.D(ctx, "task(KnowledgeGenerateSchema) Process d.params.NeedCluster:%+v",
					d.params.NeedCluster)
				float32Vector := make([]float32, 0)
				if len(docSchema.Vector) == 0 {
					// 该文档在数据库中无向量结果
					// 获取embedding结果
					vector, err := d.rpc.VectorDBManager.Embedding(ctx, d.params.AppBizID, docSchema.Summary)
					if err != nil {
						logx.W(ctx, "task(KnowledgeGenerateSchema) Process embedding failed, docID:%d err: %v", docSchema.DocID, err)
						continue
						// return err 这里不返回，因为embedding失败的文档可以继续处理
					}
					if len(vector) == 0 {
						logx.W(ctx, "task(KnowledgeGenerateSchema) Process embedding vector is empty, docID:%d", docSchema.DocID)
						continue
						// return err 这里不返回，因为embedding失败的文档可以继续处理
					}
					docSchema.Vector = convx.SliceFloat32ToByte(vector)
					float32Vector = vector
					if !isNewDocSchema {
						// 非新增文档schema，需要更新数据库
						err = d.docLogic.UpdateDocSchema(ctx, updateColumns, docSchema)
						if err != nil {
							logx.E(ctx, "task(KnowledgeGenerateSchema) Process UpdateDocSchema err:%+v", err)
							return err
						}
					}
				} else {
					// 该文档在数据库里已有向量结果
					float32Vector = convx.SliceByteToFloat32(docSchema.Vector)
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
				err = d.docLogic.CreateDocSchema(ctx, docSchema)
				if err != nil {
					logx.E(ctx, "task(KnowledgeGenerateSchema) Process CreateDocSchema err:%+v", err)
					return err
				}
			}
		}
		logx.D(ctx, "task(KnowledgeGenerateSchema) Process taskKey:%s, docs length:%d", taskKey, len(docs))
	}

	err = d.generateKnowledgeSchema(ctx, app, allEmbedding, docSchemas)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process generateKnowledgeSchema err:%+v", err)
		return err
	}

	err = d.generateSharedKnowledgeSchemaTask(ctx)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process generateSharedKnowledgeSchemaTask err:%+v", err)
		return err
	}

	for taskKey := range taskKvMap {
		if err := progress.Finish(ctx, taskKey); err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Finish kv:%s err:%+v", taskKey, err)
			return err
		}
	}

	return nil
}

// Fail 任务失败
func (d *KnowledgeGenerateSchemaTaskHandler) Fail(ctx context.Context) error {
	logx.D(ctx, "task(KnowledgeGenerateSchema) Fail, appBizId id: %v", d.params.AppBizID)
	// 更新任务状态
	task := &kbEntity.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     entity.TaskStatusFailed,
		StatusCode: d.params.StatusCode,
		Message:    d.params.Message,
	}
	err := d.kbDao.UpdateKnowledgeSchemaTask(ctx, []string{
		kbEntity.KnowledgeSchemaTaskTblColStatus,
		kbEntity.KnowledgeSchemaTaskTblColStatusCode,
		kbEntity.KnowledgeSchemaTaskTblColMessage,
	}, task)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Fail UpdateKnowledgeSchemaTask err:%+v", err)
		return err
	}

	return nil
}

// Stop 任务停止
func (d *KnowledgeGenerateSchemaTaskHandler) Stop(ctx context.Context) error {
	logx.D(ctx, "task(KnowledgeGenerateSchema) Stop, appBizId id: %v", d.params.AppBizID)
	// 更新任务状态
	task := &kbEntity.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     entity.TaskStatusStop,
		StatusCode: d.params.StatusCode,
		Message:    d.params.Message,
	}
	err := d.kbDao.UpdateKnowledgeSchemaTask(ctx, []string{
		kbEntity.KnowledgeSchemaTaskTblColStatus,
		kbEntity.KnowledgeSchemaTaskTblColStatusCode,
		kbEntity.KnowledgeSchemaTaskTblColMessage,
	},
		task)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Stop UpdateKnowledgeSchemaTask err:%+v", err)
		return err
	}

	return nil
}

// Done 任务完成回调
func (d *KnowledgeGenerateSchemaTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(KnowledgeGenerateSchema) Done, appBizId id: %v", d.params.AppBizID)
	// 更新任务状态
	task := &kbEntity.KnowledgeSchemaTask{
		CorpBizId:  d.params.CorpBizID,
		AppBizId:   d.params.AppBizID,
		BusinessID: d.params.TaskBizID,
		Status:     entity.TaskStatusSuccess,
	}
	err := d.kbDao.UpdateKnowledgeSchemaTask(ctx,
		[]string{kbEntity.KnowledgeSchemaTaskTblColStatus}, task)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Done UpdateKnowledgeSchemaTask err:%+v", err)
		return err
	}

	return nil
}

// DocCluster 文档聚类
func (d *KnowledgeGenerateSchemaTaskHandler) docCluster(
	ctx context.Context,
	app *entity.App,
	allEmbedding cluster.Observations,
	allDocSchema map[uint64]*docEntity.DocSchema,
	clusterCount int) ([]*docEntity.DocClusterSchema, error) {
	logx.D(ctx, "task(KnowledgeGenerateSchema) Process allEmbedding length:%d allDocSchema length:%d clusterCount:%d",
		len(allEmbedding), len(allDocSchema), clusterCount)
	if clusterCount <= 1 {
		return nil, errors.New("clusterCount must be greater than 1")
	}
	// 文档按向量聚类
	km := kmeans.New()
	clusters, err := km.Partition(allEmbedding, clusterCount)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process error partitioning with more clusters than data points, got nil")
		return nil, err
	}

	// 获取当前maxVersion
	maxVersion, err := d.docLogic.GetDocClusterSchemaDaoMaxVersion(ctx, d.params.AppBizID)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process error, GetDocClusterSchemaDaoMaxVersion err:%+v", err)
		return nil, err
	}

	// 向量聚类结果转换成文档聚类结果
	docClusterSchemaList := make([]*docEntity.DocClusterSchema, 0)
	for _, clusterItem := range clusters {
		clusterSchema := &docEntity.DocClusterSchema{
			CorpBizID:  d.params.CorpBizID,
			AppBizID:   d.params.AppBizID,
			BusinessID: idgen.GetId(),
			IsDeleted:  false,
		}
		docClusterSchemaList = append(docClusterSchemaList, clusterSchema)
		kbFileInfos := make([]kbEntity.KBFileInfo, 0)
		docIds := make([]uint64, 0)
		for _, observation := range clusterItem.Observations {
			docID, err := strconv.ParseUint(observation.Coordinates().ID, 10, 64)
			if err != nil {
				logx.E(ctx, "task(KnowledgeGenerateSchema) Process error, strconv.ParseUint err:%+v", err)
				return nil, err
			}
			docSchema, ok := allDocSchema[docID]
			if !ok {
				logx.E(ctx, "task(KnowledgeGenerateSchema) Process error, docID:%d not found", docID)
				return nil, err
			}
			kbFileInfos = append(kbFileInfos, kbEntity.KBFileInfo{
				FileName:    docSchema.FileName,
				FileSummary: docSchema.Summary,
			})
			docIds = append(docIds, docID)
		}
		docIdsJson, err := jsonx.Marshal(docIds)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Process error, jsonx.Marshal err:%+v", err)
			return nil, err
		}
		clusterSchema.DocIDs = string(docIdsJson)

		clusterName, clusterSummary, err := d.getDirSummary(ctx, app, kbFileInfos)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Process error, getDirSummary err:%+v", err)
			return nil, err
		}

		clusterSchema.ClusterName = clusterName
		clusterSchema.Summary = clusterSummary
		clusterSchema.Version = maxVersion + 1
	}

	// 事务写入cluster schema表，避免写入一半失败
	err = d.tdsql.Transaction(func(tx *tdsqlquery.Query) error {
		for _, clusterSchema := range docClusterSchemaList {
			err = d.docLogic.CreateDocClusterSchema(ctx, clusterSchema)
			if err != nil {
				logx.E(ctx, "task(KnowledgeGenerateSchema) Process CreateDocClusterSchema err:%+v", err)
				return err
			}
		}
		return nil
	}, nil)
	if err != nil {
		logx.E(ctx, "UpdateAttributeSuccess failed err: %+v", err)
		return nil, err
	}
	return docClusterSchemaList, nil
}

func getDocClusterThreshold() int {
	return gox.IfElse(config.GetMainConfig().KnowledgeSchema.DocClusterThreshold > 0,
		config.GetMainConfig().KnowledgeSchema.DocClusterThreshold, defaultDocClusterThreshold)
}

func getMaxProcessDocCountThreshold() int {
	return gox.IfElse(config.GetMainConfig().KnowledgeSchema.MaxProcessDocCount > 0,
		config.GetMainConfig().KnowledgeSchema.MaxProcessDocCount, defaultMaxProcessDocCount)
}

func getStatusCodeMessage(statusCode uint32) string {
	return gox.IfElse(config.GetMainConfig().KnowledgeSchema.TaskStatusCodeMessage[statusCode] != "",
		config.GetMainConfig().KnowledgeSchema.TaskStatusCodeMessage[statusCode], statusCodeMessage[statusCode])
}

func (d *KnowledgeGenerateSchemaTaskHandler) getStructFileSummary(ctx context.Context, params *kbEntity.KnowledgeGenerateSchemaParams, doc *docEntity.Doc) (
	string, bool, error) {
	// 非聚类场景，结构化文档需要特殊处理，支持text2sql
	// 先走结构化逻辑
	var err error
	summary := ""
	isStructFile := false
	text2sqlMeta := make([]docEntity.Text2sqlMetaMappingPreview, 0)
	isStructFile, text2sqlMeta, err = d.docLogic.GetDocMetaDataForSchema(
		ctx, doc.ID, params.AppID, entity.RunEnvSandbox)
	logx.D(ctx, "task(KnowledgeGenerateSchema) Process StructFile:%s isStructFile:%+v",
		doc.FileName, isStructFile)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process GetDocMetaDataForSchema err:%+v", err)
		return "", false, err
	}

	if isStructFile {
		bytes, err := jsonx.Marshal(text2sqlMeta)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Process jsonx.Marshal err:%+v", err)
			return "", false, err
		}
		summary = string(bytes)
	}
	return summary, isStructFile, nil
}

func (d *KnowledgeGenerateSchemaTaskHandler) getCommonDocSummary(ctx context.Context, app *entity.App, doc *docEntity.Doc) (string, error) {
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	if !d.financeLogic.CheckModelStatus(ctx, d.params.CorpID, d.params.SummaryModelName, rpc.KnowledgeSchemaFinanceBizType) {
		return "", errs.ErrOverModelTokenLimit
	}
	d.tokenDosage.StartTime = time.Now()
	getKBDocSummaryReq := &kbEntity.GetKBDocSummaryReq{
		RobotID:   d.params.AppID,
		BotBizId:  d.params.AppBizID,
		RequestId: codec.Message(ctx).DyeingKey(),
		DocID:     doc.ID,
		FileName:  doc.FileName,
		ModelName: d.params.SummaryModelName,
	}
	summary, tokenStatisticInfo, err := d.KBAgentGetOneDocSummary(newCtx, d.docLogic, getKBDocSummaryReq, app, d.llmLogic, d.rpc)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process KBAgentGetOneDocSummary err:%+v", err)
		return "", err
	}
	err = d.financeLogic.ReportTokenDosage(newCtx, tokenStatisticInfo, d.tokenDosage, d.params.CorpID, rpc.KnowledgeSchemaFinanceBizType, app)
	if err != nil {
		// 只打印ERROR日志，降级处理
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process logicCorp.ReportTokenDosage err:%+v", err)
	}
	return summary, nil
}

func (d *KnowledgeGenerateSchemaTaskHandler) getDirSummary(ctx context.Context, app *entity.App, kbFileInfos []kbEntity.KBFileInfo) (string, string, error) {
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	if !d.financeLogic.CheckModelStatus(ctx, d.params.CorpID, d.params.SummaryModelName, rpc.KnowledgeSchemaFinanceBizType) {
		return "", "", errs.ErrOverModelTokenLimit
	}
	d.tokenDosage.StartTime = time.Now()
	req := &kbEntity.GetKBDirSummaryReq{
		BotBizId:  d.params.AppBizID,
		RequestId: codec.Message(ctx).DyeingKey(),
		FileInfos: kbFileInfos,
		ModelName: d.params.SummaryModelName,
	}
	clusterName, clusterSummary, tokenStatisticInfo, err := kbLogic.KBAgentGetDirSummary(newCtx, app, req, d.llmLogic, d.rpc)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process error, KBAgentGetDirSummary err:%+v", err)
		return "", "", err
	}
	err = d.financeLogic.ReportTokenDosage(newCtx, tokenStatisticInfo, d.tokenDosage, d.params.CorpID, rpc.KnowledgeSchemaFinanceBizType, app)
	if err != nil {
		// 只打印ERROR日志，降级处理
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process logicCorp.ReportTokenDosage err:%+v", err)
	}
	return clusterName, clusterSummary, nil
}

func (d *KnowledgeGenerateSchemaTaskHandler) generateKnowledgeSchema(
	ctx context.Context,
	app *entity.App,
	allEmbedding cluster.Observations,
	docSchemas map[uint64]*docEntity.DocSchema) error {
	logx.D(ctx, "task(KnowledgeGenerateSchema) Process generateKnowledgeSchema params:%+v "+
		"allEmbedding length:%d docSchemas length:%d", d.params, len(allEmbedding), len(docSchemas))
	knowledgeSchemaList := make([]*kbEntity.KnowledgeSchema, 0)
	maxVersion, err := d.kbDao.GetKnowledgeSchemaMaxVersion(ctx, d.params.AppBizID)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process GetKnowledgeSchemaMaxVersion err:%+v", err)
		return err
	}
	docClusterSchemaList := make([]*docEntity.DocClusterSchema, 0)

	docClusterThreshold := getDocClusterThreshold()
	clusterCount := int(math.Ceil(float64(len(allEmbedding)) / float64(docClusterThreshold)))
	if clusterCount <= 1 {
		// 拿到的embedding结果效果单个集合大小阈值，不用聚类了
		d.params.NeedCluster = false
	}

	if d.params.NeedCluster {
		logx.D(ctx, "task(KnowledgeGenerateSchema) Process d.params.NeedCluster:%+v",
			d.params.NeedCluster)
		docClusterSchemaList, err = d.docCluster(ctx, app, allEmbedding, docSchemas, clusterCount)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Process DocCluster err:%+v", err)
			return err
		}
		for _, docClusterSchema := range docClusterSchemaList {
			knowledgeSchema := &kbEntity.KnowledgeSchema{
				CorpBizId: d.params.CorpBizID,
				AppBizId:  d.params.AppBizID,
				Version:   maxVersion + 1,
				ItemType:  kbEntity.KnowledgeSchemaItemTypeDocCluster,
				ItemBizId: docClusterSchema.BusinessID,
				Name:      docClusterSchema.ClusterName,
				Summary:   docClusterSchema.Summary,
				IsDeleted: false,
			}
			knowledgeSchemaList = append(knowledgeSchemaList, knowledgeSchema)
		}
	} else {
		for _, docSchema := range docSchemas {
			knowledgeSchema := &kbEntity.KnowledgeSchema{
				CorpBizId: d.params.CorpBizID,
				AppBizId:  d.params.AppBizID,
				Version:   maxVersion + 1,
				ItemType:  kbEntity.KnowledgeSchemaItemTypeDoc,
				ItemBizId: docSchema.DocID,
				Name:      docSchema.FileName,
				Summary:   docSchema.Summary,
				IsDeleted: false,
			}
			knowledgeSchemaList = append(knowledgeSchemaList, knowledgeSchema)
		}
	}

	logx.D(ctx, "task(KnowledgeGenerateSchema) Process knowledgeSchemaList length:%d", len(knowledgeSchemaList))
	// 事务写入知识库schema表，避免写入一半的时候被发布任务快照
	err = d.tdsql.Transaction(func(tx *tdsqlquery.Query) error {
		// 先【硬性】删除所有旧版本，再写入新版本
		err = d.kbDao.DeleteKnowledgeSchema(ctx, d.params.CorpBizID, d.params.AppBizID)
		if err != nil {
			logx.E(ctx, "task(KnowledgeGenerateSchema) Process DeleteKnowledgeSchema err:%+v", err)
			return err
		}
		for _, knowledgeSchema := range knowledgeSchemaList {
			err = d.kbDao.CreateKnowledgeSchema(ctx, knowledgeSchema)
			if err != nil {
				logx.E(ctx, "task(KnowledgeGenerateSchema) Process CreateKnowledgeSchema err:%+v", err)
				return err
			}
		}
		return nil
	}, nil)
	if err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process err: %+v", err)
		return err
	}
	if d.params.NeedCluster {
		// 生成成功，设置文件聚类目录缓存
		for _, docClusterSchema := range docClusterSchemaList {
			// 这里的envType要根据是否是共享知识库判断，因为共享知识库没有发布的流程，操作即生效，所以这里的envType是product
			if err := d.kbDao.SetKnowledgeSchemaDocIdByDocClusterId(ctx,
				d.params.AppBizID,
				gox.IfElse(d.isSharedKnowledge, entity.EnvTypeProduct, entity.EnvTypeSandbox),
				docClusterSchema); err != nil {
				logx.E(ctx, "task(KnowledgeGenerateSchema) Process redis.SetKnowledgeSchemaDocIdByDocClusterId fail, err: %+v", err)
				return err
			}
			if err := d.kbDao.SetKnowledgeSchemaAppBizIdByDocClusterId(ctx, docClusterSchema.BusinessID, d.params.AppBizID, entity.EnvTypeSandbox); err != nil {
				logx.E(ctx, "task(KnowledgeGenerateSchema) Process redis.SetKnowledgeSchemaAppBizIdByDocClusterId fail, err: %+v", err)
				return err
			}
		}
	}
	schemaItemsPbList := kbLogic.TransformKnowledgeSchema2Pb(knowledgeSchemaList)
	// 生成成功，设置schema缓存
	if err := d.kbDao.SetKnowledgeSchema(ctx, d.params.AppBizID, entity.EnvTypeSandbox, schemaItemsPbList); err != nil {
		logx.E(ctx, "task(KnowledgeGenerateSchema) Process redis.SetKnowledgeSchema fail, err: %+v", err)
		return err
	}

	return nil
}

// generateSharedKnowledgeSchemaTask 查询应用下所有引用的共享知识库，生成共享知识库的schema任务
func (d *KnowledgeGenerateSchemaTaskHandler) generateSharedKnowledgeSchemaTask(ctx context.Context) error {
	for _, appBizID := range d.referShareKnowledgeBizIDList {
		appDB, err := d.taskCommon.rpc.AppAdmin.DescribeAppById(ctx, appBizID)
		// appDB, err := d.dao.GetAppByAppBizID(ctx, appBizID)
		if err != nil {
			logx.E(ctx, "generateSharedKnowledgeSchemaTask GetAppByAppBizID fail, "+
				"appBizID=%+v, err=%+v", appBizID, err)
			return err
		}
		if appDB == nil {
			logx.D(ctx, "generateSharedKnowledgeSchemaTask appDB is nil, appBizID=%+v", appBizID)
			continue
		}
		err = d.kbLogic.GenerateKnowledgeSchemaTask(ctx, d.params.CorpID, d.params.CorpBizID, appBizID, idgen.GetId(), appDB.PrimaryId, appDB.IsShared)
		if err != nil {
			logx.E(ctx, "generateSharedKnowledgeSchemaTask GenerateKnowledgeSchema fail, "+
				"appBizID=%+v, err=%+v", appBizID, err)
		}
		logx.D(ctx, "generateSharedKnowledgeSchemaTask GenerateKnowledgeSchema success, appBizID=%+v", appBizID)
	}
	return nil
}

func isImageByExtension(filename string) bool {
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filename), "."))
	imageExtensions := []string{"jpg", "jpeg", "png", "gif", "bmp", "webp"}
	for _, e := range imageExtensions {
		if ext == e {
			return true
		}
	}
	return false
}

// KBAgentGetOneDocSummary 获取一个文档的摘要
func (d *KnowledgeGenerateSchemaTaskHandler) KBAgentGetOneDocSummary(ctx context.Context, docLogic *document.Logic, request *kbEntity.GetKBDocSummaryReq,
	app *entity.App, llmLogic *llmLogic.Logic, r *rpc.RPC) (
	string, *rpc.StatisticInfo, error) {
	start := time.Now()
	if len(request.RequestId) == 0 {
		request.RequestId = uuid.NewString()
	}
	// 与算法沟通，图片暂不处理
	if isImageByExtension(request.FileName) {
		return "", nil, nil
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)

	logx.I(ctx, "KBAgentGetOneDocSummary: req:%+v", request)
	// 1、根据doc_id找到对应的解析id, 并下载前8K的解析文档字符
	docParse, err := docLogic.GetDocParseByDocIDAndTypeAndStatus(ctx, request.DocID, docEntity.DocParseTaskTypeSplitSegment,
		docEntity.DocParseSuccess, request.RobotID)
	if err != nil {
		logx.E(ctx, "KBAgentGetOneDocSummary,getDocParseFailed,err:%+v", err)
		return "", nil, err
	}
	parseMDContent, err := docLogic.GetOfflineDocParseResult(ctx, docParse)
	if err != nil {
		return "", nil, err
	}

	// 解析结果为空
	if len(parseMDContent) == 0 {
		return "", nil, nil
	}

	logx.I(ctx, "KBAgentGetOneDocSummary: parseMDContent:%+v", parseMDContent)
	//
	// 防御性, 加上其他提示词等信息，这个地方取值7500
	inputLimit := config.GetMainConfig().KnowledgeSchema.DocSummaryInputLimit
	if len([]rune(parseMDContent)) > inputLimit {
		parseMDContent = string([]rune(parseMDContent)[:inputLimit])
	}

	// 2、调用接口实现摘要
	prompt, err := llmLogic.GetPrompt(newCtx, app, entity.KbSchemaDocSummaryModel)
	if err != nil {
		logx.E(ctx, "KBAgentGetOneDocSummary,getPromptFailed,err:%+v", err)
		return "", nil, err
	}
	prompt, err = util.Render(ctx, prompt, kbEntity.KBDocSummary{FileName: request.FileName, FileContent: parseMDContent})
	if err != nil {
		logx.E(ctx, "KBAgentGetOneDocSummary Rendor failed err:%+v", err)
		return "", nil, err
	}

	req := &rpc.LlmRequest{
		RequestId: codec.Message(ctx).DyeingKey(),
		BizAppId:  request.BotBizId,
		StartTime: time.Now(),
		ModelName: request.ModelName,
		Messages:  []*rpc.Message{{Role: rpc.Role_USER, Content: prompt}},
	}
	rsp, err := r.SimpleChat(newCtx, req)
	if err != nil {
		logx.E(newCtx, "KBAgentGetOneDocSummary SimpleChat failed err:%+v,prompt:%+v", err, prompt)
		return "", nil, err
	}
	cost := time.Since(start).Seconds()
	logx.I(ctx, "KBAgentGetOneDocSummary cost:%+v秒,docId:%+v,docName:%+v，SimpleChat rsp:%+v",
		cost, request.DocID, request.FileName, jsonx.MustMarshalToString(rsp))
	return rsp.GetReplyContent(), rsp.GetStatisticInfo(), nil
}
