package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sync"
	"time"

	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao/vector"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/service"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	pb "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"

	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
)

var (
	cmdQA = &cobra.Command{
		Use:     "qa",
		Short:   "Operations on QA resources",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    func(cmd *cobra.Command, args []string) error { return cmd.Usage() },
	}
	flagQABizIDs []string
)

var (
	cmdQAList = &cobra.Command{
		Use:     "list",
		Short:   "List QA resources with the given filters",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdQAList,
	}
	flagQAListFields []string
)

var (
	cmdQAEnableScope = &cobra.Command{
		Use:     "enable-scope",
		Short:   "Set enable_scope for QA resources",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdQAEnableScope,
	}
	flagQAEnableScopeType        string
	flagQAEnableScopeUin         string
	flagQAEnableScopeAppBizIDs   []string
	flagQAEnableScopeSpaceID     string
	flagQAEnableScopeAll         bool
	flagQAEnableScopeWorkerCount int
)

var (
	cmdQACharUsage = &cobra.Command{
		Use:     "char-usage",
		Short:   "Get character usage and calculate QA copy size",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdQACharUsage,
	}
	flagQACharUsageUin       string
	flagQACharUsageAppBizIDs []string
	flagQACharUsageAll       bool
)

var (
	cmdQARevert = &cobra.Command{
		Use:     "revert",
		Short:   "Revert QA enable_scope to 0 and delete copied QAs from publish domain",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdQARevert,
	}
	flagQARevertUin       string
	flagQARevertAppBizIDs []string
	flagQARevertSpaceID   string
	flagQARevertAll       bool
)

func init() {
	flags := cmdQA.PersistentFlags()
	flags.StringSliceVar(&flagQABizIDs, "biz_ids", []string{}, "biz IDs of QA resources")

	flags = cmdQAList.PersistentFlags()
	flags.StringSliceVar(&flagQAListFields, "fields", []string{}, "db field names to list QA separated with comma")
	flags = cmdQAEnableScope.PersistentFlags()
	flags.StringVarP(&flagQAEnableScopeType, "type", "t", "field", "enable scope type: field or label")
	flags.StringVar(&flagQAEnableScopeUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagQAEnableScopeAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (optional, cannot be used with --space_id or --all)")
	flags.StringVar(&flagQAEnableScopeSpaceID, "space_id", "", "space ID to process all apps under it (optional, cannot be used with --app_biz_ids or --all)")
	flags.BoolVar(&flagQAEnableScopeAll, "all", false, "process all apps under the uin (optional, cannot be used with --app_biz_ids or --space_id)")
	flags.IntVar(&flagQAEnableScopeWorkerCount, "worker_count", 5, "number of concurrent workers for QA copy (default: 5, min: 1, max: 20)")

	flags = cmdQACharUsage.PersistentFlags()
	flags.StringVar(&flagQACharUsageUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagQACharUsageAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (required, unless --all is specified)")
	flags.BoolVar(&flagQACharUsageAll, "all", false, "process all apps under the uin")

	flags = cmdQARevert.PersistentFlags()
	flags.StringVar(&flagQARevertUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagQARevertAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (optional, cannot be used with --space_id or --all)")
	flags.StringVar(&flagQARevertSpaceID, "space_id", "", "space ID to process all apps under it (optional, cannot be used with --app_biz_ids or --all)")
	flags.BoolVar(&flagQARevertAll, "all", false, "process all apps under the uin (optional, cannot be used with --app_biz_ids or --space_id)")

	cmdQA.AddCommand(cmdQAList)
	cmdQA.AddCommand(cmdQAEnableScope)
	cmdQA.AddCommand(cmdQACharUsage)
	cmdQA.AddCommand(cmdQARevert)
}

func RunCmdQAList(cmd *cobra.Command, args []string) error {
	filter := &qaEntity.DocQaFilter{
		BusinessIds: slicex.Map(flagQABizIDs, func(s string) uint64 { return cast.ToUint64(s) }),
		Limit:       PageSize,
	}
	if len(CorpIDs) > 0 {
		filter.CorpId = cast.ToUint64(CorpIDs[0])
	}
	qaList, err := GetCmdService().QaLogic.GetDao().GetDocQaList(cmd.Context(), nil, filter)
	if err != nil {
		return err
	}

	tw := table.NewWriter()
	tw.SetOutputMirror(os.Stdout)
	var header table.Row
	if !slices.Contains(flagQAListFields, "id") {
		flagQAListFields = append([]string{"id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "business_id") {
		flagQAListFields = append([]string{"business_id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "corp_id") {
		flagQAListFields = append([]string{"corp_id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "robot_id") {
		flagQAListFields = append([]string{"robot_id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "question") {
		flagQAListFields = append([]string{"question"}, flagQAListFields...)
	}

	for _, field := range flagQAListFields {
		header = append(header, field)
	}
	tw.AppendHeader(header)
	for _, q := range qaList {
		var row table.Row
		for _, field := range flagQAListFields {
			qv := reflect.ValueOf(q).Elem()
			qt := qv.Type()

			var fieldIdx int
			for i := 0; i < qt.NumField(); i++ {
				if field == qt.Field(i).Tag.Get("db") {
					fieldIdx = i
					break
				}
			}
			row = append(row, qv.Field(fieldIdx).Interface())
		}
		tw.AppendRow(row)
	}
	tw.Render()
	return nil
}

func RunCmdQAEnableScope(cmd *cobra.Command, args []string) error {
	return RunEnableScopeCommand(cmd, ProcessAppQa, EnableScopeParams{
		Uin:       flagQAEnableScopeUin,
		AppBizIDs: flagQAEnableScopeAppBizIDs,
		SpaceID:   flagQAEnableScopeSpaceID,
		All:       flagQAEnableScopeAll,
		TypeName:  "Qa",
	}, &AppWorkerConfig{
		QaWorkerCount: flagQAEnableScopeWorkerCount,
	})
}

// GetQaProcessingData 获取QA处理所需的数据
// 返回值：qasEnableScopeMap, qasNeedCopyFromPublishDomain, qaSegmentsNeedCopyFromPublishDomain, error
func GetQaProcessingData(ctx context.Context, app *entity.App) (map[uint32][]*qaEntity.DocQA, []*qaEntity.DocQA, []*segEntity.EsSegment, []uint32, error) {
	// 获取该应用所有enable_scope为0的QA对，不为0的说明已经刷过了
	logx.I(ctx, "GetQaProcessingData start, app_id: %d, app_name: %s", app.PrimaryId, app.Name)
	qaList, err := GetCmdService().QaLogic.GetDao().GetDocQaList(ctx, nil, &qaEntity.DocQaFilter{
		RobotId:     app.PrimaryId,
		EnableScope: ptrx.Uint32(entity.EnableScopeInvalid),
	})
	if err != nil {
		logx.E(ctx, "GetDocQaList err: %+v, app_id: %d", err, app.PrimaryId)
		return nil, nil, nil, nil, err
	}
	logx.I(ctx, "GetDocQaList success, app_id: %d, qa_count: %d", app.PrimaryId, len(qaList))

	// key: enable_scope value: qa_id_list
	qasEnableScopeMap := make(map[uint32][]*qaEntity.DocQA)
	// 需要从发布域复制数据到开发域的qa_id_list
	qasNeedCopy := make([]*qaEntity.DocQA, 0)
	qaSegmentsNeedCopy := make([]*segEntity.EsSegment, 0)
	qaEnableScopesNeedCopy := make([]uint32, 0)
	for idx, qa := range qaList {
		logx.I(ctx, "Processing QA [%d/%d], qa_id: %d, question: %s", idx+1, len(qaList), qa.ID, qa.Question)
		// 从ES查询发布域的QA片段
		qaSegmentList, err := GetCmdService().SegLogic.GetDao().QueryQaSegmentInProd(ctx, app.PrimaryId, qa.ID, []string{}, 1)
		if err != nil {
			logx.E(ctx, "QueryQaSegmentInProd err: %+v, qa_id: %d", err, qa.ID)
			return nil, nil, nil, nil, err
		}
		if len(qaSegmentList) > 1 {
			logx.E(ctx, "QueryQaSegmentInProd qaSegmentList length is gt 1, qa_id: %d, length: %d", qa.ID, len(qaSegmentList))
			return nil, nil, nil, nil, errs.ErrWrapf(errs.ErrParams, "qa_id: %d, qaSegmentList length: %d", qa.ID, len(qaSegmentList))
		}
		qaSegment := (*segEntity.EsSegment)(nil)
		if len(qaSegmentList) == 1 {
			qaSegment = qaSegmentList[0]
		}
		devEnableScope, publishEnableScope := getQaEnableScope(ctx, app.IsShared, qa, qaSegment)
		logx.I(ctx, "QA [%d] devEnableScope: %d, publishEnableScope: %d", qa.ID, devEnableScope, publishEnableScope)

		appendQasEnableScopeMap(qasEnableScopeMap, devEnableScope, qa)

		if publishEnableScope == entity.EnableScopePublish {
			// 发布域中有数据的问答需要从发布域复制数据到开发域
			logx.I(ctx, "QA [%d] need copy from publish domain, char_size: %d, enable_scope: %d",
				qa.ID, qa.CharSize, publishEnableScope)
			if qaSegment == nil {
				logx.E(ctx, "QA [%d] qaSegment is nil", qa.ID)
				return nil, nil, nil, nil, errs.ErrWrapf(errs.ErrParams, "qa_id: %d, qaSegment is nil", qa.ID)
			}
			qasNeedCopy = append(qasNeedCopy, qa)
			qaSegmentsNeedCopy = append(qaSegmentsNeedCopy, qaSegment)
			qaEnableScopesNeedCopy = append(qaEnableScopesNeedCopy, publishEnableScope)
		}
	}

	logx.I(ctx, "GetQaProcessingData finished, app_id: %d, qasEnableScopeMap size: %d, qasNeedCopy count: %d",
		app.PrimaryId, len(qasEnableScopeMap), len(qasNeedCopy))
	return qasEnableScopeMap, qasNeedCopy, qaSegmentsNeedCopy, qaEnableScopesNeedCopy, nil
}

func ProcessAppQa(ctx context.Context, app *entity.App, config *AppWorkerConfig) error {
	logx.I(ctx, "processing app: %s", jsonx.MustMarshal(app))
	if config == nil {
		logx.E(ctx, "ProcessAppQa config is nil")
		return errs.ErrWrapf(errs.ErrParams, "ProcessAppQa config is nil")
	}

	// 获取QA处理所需的数据
	qasEnableScopeMap, qasNeedCopy, qaSegmentsNeedCopy, qaEnableScopeNeedCopy, err := GetQaProcessingData(ctx, app)
	if err != nil {
		logx.E(ctx, "GetQaProcessingData err:%+v", err)
		return err
	}
	// 批量更新ES的enable_scope标签
	err = updateQaEnableScopeInES(ctx, app, qasEnableScopeMap)
	if err != nil {
		logx.E(ctx, "updateQaEnableScopeInES err:%+v", err)
		return err
	}
	// 批量更新数据库enable_scope字段
	err = updateQaEnableScopeInDB(ctx, qasEnableScopeMap)
	if err != nil {
		logx.E(ctx, "updateQaEnableScopeInDB err:%+v", err)
		return err
	}
	// 从发布域复制数据库到开发域（并发处理）
	if len(qasNeedCopy) > 0 {
		err = copyQasFromPublishDomain2DevDomainConcurrently(ctx, app, qasNeedCopy, qaSegmentsNeedCopy, qaEnableScopeNeedCopy, config.QaWorkerCount)
		if err != nil {
			logx.E(ctx, "copyQasFromPublishDomain2DevDomainConcurrently err:%+v", err)
			return err
		}
	}
	return nil
}

func getQaEnableScope(ctx context.Context, isSharedApp bool, qa *qaEntity.DocQA, esSegment *segEntity.EsSegment) (uint32, uint32) {
	if isSharedApp || qa.ReleaseStatus == qaEntity.QAReleaseStatusSuccess || esSegment == nil {
		// 如果是共享知识库，或者默认知识库的已发布问答，或者从来没有发布过，则通过attribute_flag字段来判断是否开启
		if qa.HasAttributeFlag(qaEntity.QAAttributeFlagDisable) {
			// 停用状态
			return entity.EnableScopeDisable, entity.EnableScopeInvalid
		}
		if !isSharedApp && esSegment == nil {
			// 如果是非共享知识库，而且从来没有发布过
			return entity.EnableScopeDev, entity.EnableScopeInvalid
		}
		return entity.EnableScopeAll, entity.EnableScopeInvalid
	}
	// 如果是默认知识库，超量失效状态，且曾经发布过
	if slices.Contains(qaEntity.QaExceedStatus, qa.ReleaseStatus) {
		return entity.EnableScopeDev, entity.EnableScopeInvalid
	}
	// 如果是默认知识库，非发布状态、非超量失效状态，且曾经发布过
	devEnableScope := uint32(entity.EnableScopeDev)
	if qa.HasAttributeFlag(qaEntity.QAAttributeFlagDisable) {
		devEnableScope = entity.EnableScopeDisable
	}

	publishEnableScope := uint32(entity.EnableScopePublish)
	// EsSegment.ExpireTime是int64类型的时间戳（秒）
	if esSegment.ExpireTime != 0 && esSegment.ExpireTime < time.Now().Unix() && esSegment.ExpireTime != qa.ExpireEnd.Unix() {
		// 需要通过查询问答在发布域的片段，通过对比片段的过期时间和数据表中的过期时间来判断发布域的启用状态
		publishEnableScope = entity.EnableScopeDisable
	}
	return devEnableScope, publishEnableScope
}

// qasEnableScopeMap 追加qasEnableScopeMap
func appendQasEnableScopeMap(qasEnableScopeMap map[uint32][]*qaEntity.DocQA, enableScope uint32, qa *qaEntity.DocQA) {
	if _, ok := qasEnableScopeMap[enableScope]; !ok {
		qasEnableScopeMap[enableScope] = make([]*qaEntity.DocQA, 0)
	}
	qasEnableScopeMap[enableScope] = append(qasEnableScopeMap[enableScope], qa)
}

// updateQaEnableScopeInDB 更新数据库enable_scope字段
func updateQaEnableScopeInDB(ctx context.Context, qasEnableScopeMap map[uint32][]*qaEntity.DocQA) error {
	for enableScope, qas := range qasEnableScopeMap {
		qaIds := make([]uint64, 0)
		for _, qa := range qas {
			qaIds = append(qaIds, qa.GetID())
		}
		qaChunks := slicex.Chunk(qaIds, 200)
		for _, qaChunkIDs := range qaChunks {
			filter := &qaEntity.DocQaFilter{
				QAIds: qaChunkIDs,
			}
			rowsAffected, err := GetCmdService().QaLogic.GetDao().BatchUpdateDocQA(ctx, filter, map[string]any{"enable_scope": enableScope}, nil)
			if err != nil {
				logx.E(ctx, "BatchUpdateDocQA qas:%+v err:%+v", qas, err)
				return err
			}
			logx.I(ctx, "BatchUpdateDocQA qas:%+v rowsAffected:%d", qas, rowsAffected)
		}
	}
	return nil
}

// updateQaEnableScopeInES 更新ES enable_scope字段
func updateQaEnableScopeInES(ctx context.Context, app *entity.App, qasEnableScopeMap map[uint32][]*qaEntity.DocQA) error {
	embeddingVersion := app.Embedding.Version
	embeddingModel, err := GetCmdService().QaLogic.GetVectorSyncLogic().ExtractEmbeddingModelOfKB(ctx, app.CorpBizId, app)
	if err != nil {
		logx.E(ctx, "updateQaEnableScopeInES ExtractEmbeddingModelOfKB err:%v,app:%+v", err, app)
		return err
	} else {
		if embeddingModel != "" {
			embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
		}
	}
	logx.I(ctx, "updateQaEnableScopeInES embeddingVersion:%s, embeddingModel:%s",
		embeddingVersion, embeddingModel)
	for enableScope, qas := range qasEnableScopeMap {
		label, ok := entity.EnableScopeDb2Label[enableScope]
		if !ok {
			logx.E(ctx, "updateQaEnableScopeInES enableScope:%d not found in EnableScopeDb2Label", enableScope)
			return errs.ErrWrapf(errs.ErrSystem, "enableScope:%d not exist in EnableScopeDb2Label", enableScope)
		}
		vectorLabels := []*retrieval.VectorLabel{
			{
				Name:  entity.EnableScopeAttr,
				Value: label,
			},
		}
		for _, qa := range qas {
			if qa.IsDeleted == qaEntity.QAIsDeleted {
				logx.I(ctx, "updateQaEnableScopeInES qaID:%d qaBizID:%d has deleted", qa.ID, qa.BusinessID)
				continue
			}
			// 获取qa的相似问
			similarQuestions, err := GetCmdService().QaLogic.GetSimilarQuestionsByQA(ctx, qa)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processQa GetSimilarQuestionsByQA err:%v,qaID:%v", err, qa.GetID())
				return err
			}
			// 5.写评测端es和向量
			// 标准问需要双写两个向量库
			req := &retrieval.UpdateLabelReq{
				RobotId:            qa.RobotID,
				AppBizId:           app.BizId,
				EnvType:            retrieval.EnvType_Test,
				IndexId:            entity.ReviewVersionID,
				Ids:                []uint64{qa.ID},
				DocType:            entity.DocTypeQA,
				QaType:             entity.QATypeStandard,
				EmbeddingModelName: embeddingModel,
				EmbeddingVersion:   embeddingVersion,
				Labels:             vectorLabels,
			}
			_, err = GetCmdService().RpcImpl.RetrievalDirectIndex.UpdateVectorLabel(ctx, req)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
				return err
			}
			req = &retrieval.UpdateLabelReq{
				RobotId:            qa.RobotID,
				AppBizId:           app.BizId,
				EnvType:            retrieval.EnvType_Test,
				IndexId:            entity.SimilarVersionID,
				Ids:                []uint64{qa.ID},
				DocType:            entity.DocTypeQA,
				QaType:             entity.QATypeStandard,
				EmbeddingModelName: embeddingModel,
				EmbeddingVersion:   embeddingVersion,
				Labels:             vectorLabels,
			}
			_, err = GetCmdService().RpcImpl.RetrievalDirectIndex.UpdateVectorLabel(ctx, req)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
				return err
			}
			sleepSwitch, sleepMillisecond := config.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
				config.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
			simBizIDs := make([]uint64, 0, len(similarQuestions))
			if len(similarQuestions) > 0 {
				for _, sims := range slicex.Chunk(similarQuestions, 100) {
					tmp := make([]uint64, 0, 200)
					for _, v := range sims {
						tmp = append(tmp, v.SimilarID)
						simBizIDs = append(simBizIDs, v.SimilarID)
					}
					req := &retrieval.UpdateLabelReq{
						RobotId:            qa.RobotID,
						AppBizId:           app.BizId,
						EnvType:            retrieval.EnvType_Test,
						IndexId:            entity.ReviewVersionID,
						Ids:                tmp, // 相似问业务id
						DocType:            entity.DocTypeQA,
						QaType:             entity.QATypeSimilar,
						EmbeddingModelName: embeddingModel,
						EmbeddingVersion:   embeddingVersion,
						Labels:             vectorLabels,
					}
					_, err = GetCmdService().RpcImpl.RetrievalDirectIndex.UpdateVectorLabel(ctx, req)
					if err != nil {
						logx.E(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
						return err
					}
					if sleepSwitch {
						time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
					}
				}
			}
		}
	}
	return nil
}

// QaCopyTask QA复制任务
type QaCopyTask struct {
	Qa                 *qaEntity.DocQA
	QaSegment          *segEntity.EsSegment
	PublishEnableScope uint32
	Index              int
	Total              int
}

// copyQasFromPublishDomain2DevDomainConcurrently 并发复制QA从发布域到开发域
func copyQasFromPublishDomain2DevDomainConcurrently(ctx context.Context, app *entity.App,
	qasNeedCopy []*qaEntity.DocQA, qaSegmentsNeedCopy []*segEntity.EsSegment,
	qaEnableScopesNeedCopy []uint32, qaWorkerCount int) error {

	totalQas := len(qasNeedCopy)
	logx.I(ctx, "copyQasFromPublishDomain2DevDomainConcurrently start, app_id: %d, total qas: %d, qa_worker_count: %d",
		app.PrimaryId, totalQas, qaWorkerCount)

	// 设置worker数量，使用传入的qaWorkerCount参数
	workerCount := gox.IfElse(qaWorkerCount > 0, qaWorkerCount, defaultQaWorkerCount)
	if totalQas < workerCount {
		workerCount = totalQas // QA数量少于worker数量时，使用QA数量
	}
	logx.I(ctx, "using %d workers to copy %d QAs concurrently", workerCount, totalQas)

	// 创建任务通道和错误通道
	taskChan := make(chan *QaCopyTask, totalQas)
	errChan := make(chan error, totalQas)
	doneChan := make(chan struct{})

	// 启动worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logx.I(ctx, "QA worker %d: started and waiting for tasks", workerID)

			for task := range taskChan {
				logx.I(ctx, "QA worker %d: processing QA [%d/%d], qa_id: %d",
					workerID, task.Index, task.Total, task.Qa.ID)

				err := copyQaFromPublishDomain2DevDomain(ctx, app, task.Qa, task.QaSegment, task.PublishEnableScope)
				if err != nil {
					logx.E(ctx, "QA worker %d: failed to copy QA [%d/%d], qa_id: %d, err: %+v",
						workerID, task.Index, task.Total, task.Qa.ID, err)
					errChan <- fmt.Errorf("copy QA %d failed: %w", task.Qa.ID, err)
					return
				}

				logx.I(ctx, "QA worker %d: successfully copied QA [%d/%d], qa_id: %d",
					workerID, task.Index, task.Total, task.Qa.ID)
			}

			logx.I(ctx, "QA worker %d: no more tasks, exiting", workerID)
		}(i)
	}

	// 等待所有worker完成的goroutine
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	// 发送任务到通道
	for i, qa := range qasNeedCopy {
		taskChan <- &QaCopyTask{
			Qa:                 qa,
			QaSegment:          qaSegmentsNeedCopy[i],
			PublishEnableScope: qaEnableScopesNeedCopy[i],
			Index:              i + 1,
			Total:              totalQas,
		}
	}
	close(taskChan)

	// 等待完成或错误
	select {
	case <-doneChan:
		logx.I(ctx, "copyQasFromPublishDomain2DevDomainConcurrently completed successfully, app_id: %d, total: %d",
			app.PrimaryId, totalQas)
		return nil
	case err := <-errChan:
		logx.E(ctx, "copyQasFromPublishDomain2DevDomainConcurrently failed, app_id: %d, err: %+v",
			app.PrimaryId, err)
		return err
	}
}

// copyQaFromPublishDomain2DevDomain 将发布域的问答复制到开发域
func copyQaFromPublishDomain2DevDomain(ctx context.Context, app *entity.App, oldDevDomainQa *qaEntity.DocQA,
	esSegment *segEntity.EsSegment, publishEnableScope uint32) error {
	// 将检索标签字符串转换成ReleaseAttrLabel数组
	releaseLabels := GetCmdService().LabelLogic.ParseLabelStrings(ctx, esSegment.Labels)
	// 将ReleaseAttrLabel数组转换成属性id和标签id
	attrLabels, err := GetCmdService().LabelLogic.TransRetrievalLabel2AttrIDAndLabelID(ctx, app.PrimaryId, releaseLabels)
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain TransRetrievalLabel2AttrIDAndLabelID err:%v", err)
		return err
	}
	// 从nodeInfo中查询发布域的问题答案
	oldDevDomainQaNodeInfo, err := GetCmdService().VectorLogic.GetDao().GetNodeIdsList(ctx, app.PrimaryId,
		[]string{vector.NodeTblColId, vector.NodeTblColRelatedId, vector.NodeTblColQuestion, vector.NodeTblColAnswer},
		&entity.RetrievalNodeFilter{
			APPID:     app.PrimaryId,
			DocType:   entity.DocTypeQA,
			RelatedID: oldDevDomainQa.ID,
			Limit:     1,
		})
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain GetNodeIdsList err:%v,oldDevDomainQa.ID:%v", err, oldDevDomainQa.ID)
		return err
	}
	answerInPublishDomain := oldDevDomainQa.Answer
	if len(oldDevDomainQaNodeInfo) > 0 {
		answerInPublishDomain = oldDevDomainQaNodeInfo[0].Answer
	}

	// 查询相似问
	simQasInNodeInfo, err := GetCmdService().VectorLogic.GetDao().GetNodeIdsList(ctx, app.PrimaryId,
		[]string{vector.NodeTblColId, vector.NodeTblColRelatedId, vector.NodeTblColQuestion},
		&entity.RetrievalNodeFilter{
			APPID:    app.PrimaryId,
			DocType:  entity.DocTypeQA,
			ParentID: oldDevDomainQa.ID,
		})
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain GetNodeIdsList err:%v,oldDevDomainQa.ID:%v", err, oldDevDomainQa.ID)
		return err
	}

	// 构造标准问
	// 从ES片段中获取问题内容（PageContent字段存储的是问题）
	// 答案、问题描述等信息从旧的开发域QA中复用
	newDevDomainQa := &qaEntity.DocQA{
		BusinessID:    idgen.GetId(),
		RobotID:       app.PrimaryId,
		CorpID:        app.CorpPrimaryId,
		StaffID:       oldDevDomainQa.StaffID,
		DocID:         oldDevDomainQa.DocID,
		OriginDocID:   oldDevDomainQa.OriginDocID,
		SegmentID:     oldDevDomainQa.SegmentID,
		Source:        oldDevDomainQa.Source,
		Question:      esSegment.PageContent, // ES片段的PageContent存储的是问题
		Answer:        answerInPublishDomain,
		CustomParam:   oldDevDomainQa.CustomParam,
		QuestionDesc:  oldDevDomainQa.QuestionDesc, // 问题描述从旧QA复用
		ReleaseStatus: qaEntity.QAReleaseStatusInit,
		SimilarStatus: docEntity.SimilarStatusEnd, // 后续会手动插入一条冲突问，所以这里直接设置
		IsAuditFree:   oldDevDomainQa.IsAuditFree,
		IsDeleted:     qaEntity.QAIsNotDeleted,
		AcceptStatus:  oldDevDomainQa.AcceptStatus,
		CategoryID:    oldDevDomainQa.CategoryID,
		NextAction:    qaEntity.NextActionAdd,
		CharSize:      oldDevDomainQa.CharSize, // 这里字符数直接复用开发域的问答字符数，跟实际的可能有出入，但是不影响
		AttrRange:     oldDevDomainQa.AttrRange,
		ExpireStart:   oldDevDomainQa.ExpireStart,
		ExpireEnd:     time.Unix(esSegment.ExpireTime, 0), // ES片段的ExpireTime是时间戳（秒）
		AttributeFlag: oldDevDomainQa.AttributeFlag,
		EnableScope:   publishEnableScope,
	}
	// 保存标准问到数据库
	err = GetCmdService().QaLogic.GetDao().CreateDocQa(ctx, newDevDomainQa)
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain CreateDocQa err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
			err, oldDevDomainQa.ID, newDevDomainQa.ID)
		return err
	}
	// 插入冲突问记录
	qaSimilarBusinessID := uint64(0)
	if oldDevDomainQa.IsDeleted == qaEntity.QAIsNotDeleted {
		qaSimilar := &qaEntity.DocQASimilar{
			RobotID:    app.PrimaryId,
			CorpID:     app.CorpPrimaryId,
			StaffID:    oldDevDomainQa.StaffID,
			DocID:      oldDevDomainQa.DocID,
			QaID:       oldDevDomainQa.ID,
			SimilarID:  newDevDomainQa.ID,
			IsValid:    true,
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
			Status:     qaEntity.QaSimilarStatusInit,
		}
		err = GetCmdService().QaLogic.SaveQaSimilar(ctx, qaSimilar)
		if err != nil {
			logx.E(ctx, "copyQaFromPublishDomain2DevDomain SaveQaSimilar err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
				err, oldDevDomainQa.ID, newDevDomainQa.ID)
			return err
		}
		qaSimilarBusinessID = qaSimilar.BusinessID
		logx.I(ctx, "copyQaFromPublishDomain2DevDomain SaveQaSimilar success, qaSimilar.BusinessID:%v", qaSimilar.BusinessID)
	}

	// 写入新旧问答的关联关系到t_dev_release_relation_info表
	q := GetCmdService().QaLogic.GetDao().Query()
	err = q.TDevReleaseRelationInfo.WithContext(ctx).Create(&model.TDevReleaseRelationInfo{
		CorpID:             app.CorpPrimaryId,
		RobotID:            app.PrimaryId,
		Type:               releaseEntity.DevReleaseRelationTypeQA,
		DevBusinessID:      oldDevDomainQa.BusinessID,
		ReleaseBusinessID:  newDevDomainQa.BusinessID,
		DiffTaskBusinessID: qaSimilarBusinessID, // 冲突问的business_id
	})
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain create TDevReleaseRelationInfo err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
			err, oldDevDomainQa.ID, newDevDomainQa.ID)
		return err
	}
	logx.I(ctx, "copyQaFromPublishDomain2DevDomain create TDevReleaseRelationInfo success, oldDevDomainQa.BusinessID:%v, newDevDomainQa.BusinessID:%v, qaSimilar.BusinessID:%v",
		oldDevDomainQa.BusinessID, newDevDomainQa.BusinessID, qaSimilarBusinessID)
	// 保存标签到数据库
	qaLabels := make([]*labelEntity.QAAttributeLabel, 0)
	for _, attrLabel := range attrLabels {
		qaLabels = append(qaLabels, &labelEntity.QAAttributeLabel{
			RobotID: app.PrimaryId,
			QAID:    newDevDomainQa.ID,
			Source:  labelEntity.AttributeLabelSourceKg,
			AttrID:  attrLabel.AttrID,
			LabelID: attrLabel.ID,
		})
	}
	err = GetCmdService().LabelDao.CreateQAAttributeLabel(ctx, qaLabels)
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain CreateQAAttributeLabel err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
			err, oldDevDomainQa.ID, newDevDomainQa.ID)
		return err
	}
	// 保存相似问到数据库
	simQas := make([]*qaEntity.SimilarQuestion, 0)
	if len(simQasInNodeInfo) > 0 {
		simQaStrList := make([]string, 0)
		for _, similarQa := range simQasInNodeInfo {
			simQaStrList = append(simQaStrList, similarQa.Question)
		}
		simQas = qaEntity.NewSimilarQuestions(ctx, newDevDomainQa, simQaStrList)
		if err = GetCmdService().QaLogic.AddSimilarQuestions(ctx, simQas); err != nil {
			logx.E(ctx, "copyQaFromPublishDomain2DevDomain AddSimilarQuestions err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
				err, oldDevDomainQa.ID, newDevDomainQa.ID)
			return err
		}
	}
	// 新增enable_scope属性标签
	// 将检索标签字符串转换成ReleaseAttrLabel数组，再转换为VectorLabel
	releaseLabelsForVector := GetCmdService().LabelLogic.ParseLabelStrings(ctx, esSegment.Labels)
	vectorLabels := make([]*retrieval.VectorLabel, 0, len(releaseLabelsForVector))
	for _, label := range releaseLabelsForVector {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  label.Name,
			Value: label.Value,
		})
	}
	vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
		Name:  entity.EnableScopeAttr,
		Value: entity.EnableScopeDb2Label[newDevDomainQa.EnableScope],
	})
	// 获取向量库版本和模型
	embeddingVersion := app.Embedding.Version
	embeddingModel := ""
	embeddingModel, err = GetCmdService().QaLogic.GetVectorSyncLogic().ExtractEmbeddingModelOfKB(ctx, app.CorpBizId, app)
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain ExtractEmbeddingModelOfKB err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v")
		return err
	}
	if embeddingModel != "" {
		embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
	}
	logx.I(ctx, "copyQaFromPublishDomain2DevDomain isShared,embeddingVersion:%d,embeddingModel:%s", embeddingVersion, embeddingModel)
	// 标准问题同步到向量库
	if err = addQASimilar(ctx, app, newDevDomainQa, embeddingVersion, embeddingModel, vectorLabels); err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain addQASimilar err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
			err, oldDevDomainQa.ID, newDevDomainQa.ID)
		return err
	}
	// 先单独添加标准问到Knowledge向量库
	if err = addQAKnowledge(ctx, app, []*qaEntity.DocQA{newDevDomainQa}, embeddingVersion, embeddingModel, vectorLabels, 60); err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain addQAKnowledge err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
			err, oldDevDomainQa.ID, newDevDomainQa.ID)
		return err
	}
	// 相似问需要增加特殊的标签
	vectorLabels = append(vectorLabels, &retrieval.VectorLabel{Name: releaseEntity.SysLabelQAFlagName, Value: releaseEntity.SysLabelQAFlagValueSimilar})
	vectorLabels = append(vectorLabels, &retrieval.VectorLabel{Name: releaseEntity.SysLabelQAIdName, Value: fmt.Sprintf("%d", newDevDomainQa.ID)})
	// 批量添加标准问和相似问到Knowledge向量库
	simQasToAdd := make([]*qaEntity.DocQA, 0, 1+len(simQas))
	// 再添加相似问
	for _, simQa := range simQas {
		simQasToAdd = append(simQasToAdd, &qaEntity.DocQA{
			ID:        simQa.SimilarID,
			RobotID:   newDevDomainQa.RobotID,
			DocID:     newDevDomainQa.DocID,
			Question:  simQa.Question,
			ExpireEnd: newDevDomainQa.ExpireEnd,
		})
	}
	// 批量添加到Knowledge向量库，每批60个
	if err = addQAKnowledge(ctx, app, simQasToAdd, embeddingVersion, embeddingModel, vectorLabels, 60); err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain addQAKnowledge err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
			err, oldDevDomainQa.ID, newDevDomainQa.ID)
		return err
	}
	// 更新应用已使用字符数
	err = GetCmdService().RpcImpl.AppAdmin.UpdateAppUsedCharSize(ctx, int64(newDevDomainQa.CharSize), newDevDomainQa.RobotID)
	if err != nil {
		logx.E(ctx, "copyQaFromPublishDomain2DevDomain UpdateAppUsedCharSize err:%v,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
			err, oldDevDomainQa.ID, newDevDomainQa.ID)
		return err
	}
	logx.I(ctx, "copyQaFromPublishDomain2DevDomain success,oldDevDomainQa.ID:%v,newDevDomainQa.ID:%v",
		oldDevDomainQa.ID, newDevDomainQa.ID)
	return nil
}

// addQASimilar 添加标准问到冲突问向量库
func addQASimilar(ctx context.Context, app *entity.App, qa *qaEntity.DocQA, embeddingVersion uint64, embeddingModel string,
	vectorLabels []*retrieval.VectorLabel) error {
	ctx = contextx.SetServerMetaData(ctx, contextx.MDSpaceID, app.SpaceId)
	logx.I(ctx, "addQASimilar qa:%+v embeddingVersion:%d embeddingModel:%s", qa, embeddingVersion, embeddingModel)
	req := &retrieval.BatchAddKnowledgeReq{
		RobotId:            qa.RobotID,
		IndexId:            entity.SimilarVersionID,
		DocType:            entity.DocTypeQA,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		BotBizId:           app.BizId,
		IsVector:           true,
		Knowledge: []*retrieval.KnowledgeData{{
			Id:          qa.ID,
			PageContent: qa.Question,
			Labels:      vectorLabels,
			ExpireTime:  qa.GetExpireTime(),
		}},
	}
	if _, err := GetCmdService().RpcImpl.RetrievalDirectIndex.BatchAddKnowledge(ctx, req); err != nil {
		logx.E(ctx, "addQASimilar BatchAddKnowledge err:%v,qa.ID:%v", err, qa.ID)
		return err
	}
	logx.I(ctx, "addQASimilar BatchAddKnowledge success,qa.ID:%v", qa.ID)
	return nil
}

// addQAKnowledge 批量添加标准问或者相似问到向量库
// qas: 待添加的QA数组
// batchSize: 每批处理的数量，默认60
func addQAKnowledge(ctx context.Context, app *entity.App, qas []*qaEntity.DocQA, embeddingVersion uint64, embeddingModel string,
	vectorLabels []*retrieval.VectorLabel, batchSize int) error {
	if len(qas) == 0 {
		return nil
	}

	if batchSize <= 0 {
		batchSize = 60
	}

	ctx = contextx.SetServerMetaData(ctx, contextx.MDSpaceID, app.SpaceId)
	logx.I(ctx, "addQAKnowledge start, total QAs:%d, batchSize:%d, embeddingVersion:%d, embeddingModel:%s",
		len(qas), batchSize, embeddingVersion, embeddingModel)

	// 按批次处理
	qaChunks := slicex.Chunk(qas, batchSize)
	for i, qaChunk := range qaChunks {
		// 构造 KnowledgeData 数组
		knowledgeList := make([]*retrieval.KnowledgeData, 0, len(qaChunk))
		for _, qa := range qaChunk {
			knowledgeList = append(knowledgeList, &retrieval.KnowledgeData{
				Id:          qa.ID,
				DocId:       qa.DocID,
				PageContent: qa.Question,
				Labels:      vectorLabels,
				ExpireTime:  qa.GetExpireTime(),
			})
		}

		// 构造请求
		req := &retrieval.BatchAddKnowledgeReq{
			RobotId:            qaChunk[0].RobotID,
			IndexId:            entity.ReviewVersionID,
			DocType:            entity.DocTypeQA,
			EmbeddingVersion:   embeddingVersion,
			EmbeddingModelName: embeddingModel,
			BotBizId:           app.BizId,
			IsVector:           false,
			Type:               retrieval.KnowledgeType_KNOWLEDGE,
			Knowledge:          knowledgeList,
		}

		// 调用RPC
		rsp, err := GetCmdService().RpcImpl.RetrievalDirectIndex.BatchAddKnowledge(ctx, req)
		if err != nil {
			logx.E(ctx, "addQAKnowledge BatchAddKnowledge err:%v, batch:%d/%d", err, i+1, len(qaChunks))
			return err
		}
		logx.I(ctx, "addQAKnowledge BatchAddKnowledge success, batch:%d/%d, count:%d, rsp:%+v",
			i+1, len(qaChunks), len(qaChunk), rsp)
	}

	logx.I(ctx, "addQAKnowledge finished, total QAs:%d", len(qas))
	return nil
}

func RunCmdQARevert(cmd *cobra.Command, args []string) error {
	return RunEnableScopeCommand(cmd, RevertAppQa, EnableScopeParams{
		Uin:                  flagQARevertUin,
		AppBizIDs:            flagQARevertAppBizIDs,
		SpaceID:              flagQARevertSpaceID,
		All:                  flagQARevertAll,
		TypeName:             "Qa",
		SkipEmbeddingUpgrade: true, // Revert操作跳过embedding升级
	}, &AppWorkerConfig{})
}

// RevertAppQa 回滚应用的QA数据
func RevertAppQa(ctx context.Context, app *entity.App, config *AppWorkerConfig) error {
	logx.I(ctx, "reverting app: %s", jsonx.MustMarshal(app))

	// 1. 将该应用下所有QA的enable_scope字段置0
	filter := &qaEntity.DocQaFilter{
		RobotId: app.PrimaryId,
	}
	rowsAffected, err := GetCmdService().QaLogic.GetDao().BatchUpdateDocQA(ctx, filter, map[string]any{"enable_scope": entity.EnableScopeInvalid}, nil)
	if err != nil {
		logx.E(ctx, "BatchUpdateDocQA err:%+v, app_id:%d", err, app.PrimaryId)
		return err
	}
	logx.I(ctx, "BatchUpdateDocQA success, app_id:%d, rowsAffected:%d", app.PrimaryId, rowsAffected)

	// 2. 查询t_dev_release_relation_info表，获取从发布域复制到开发域的问答关联信息
	q := GetCmdService().QaLogic.GetDao().Query()
	relationInfoList, err := q.TDevReleaseRelationInfo.WithContext(ctx).
		Where(q.TDevReleaseRelationInfo.CorpID.Eq(app.CorpPrimaryId)).
		Where(q.TDevReleaseRelationInfo.RobotID.Eq(app.PrimaryId)).
		Where(q.TDevReleaseRelationInfo.Type.Eq(3)).Find() // 3表示QA类型
	if err != nil {
		logx.E(ctx, "query TDevReleaseRelationInfo err:%+v, app_id:%d", err, app.PrimaryId)
		return err
	}
	logx.I(ctx, "query TDevReleaseRelationInfo success, app_id:%d, count:%d", app.PrimaryId, len(relationInfoList))

	if len(relationInfoList) == 0 {
		logx.I(ctx, "no copied QAs found for app_id:%d", app.PrimaryId)
		return nil
	}

	// 3. 根据ReleaseBusinessID查询需要删除的问答
	releaseBusinessIDs := make([]uint64, 0, len(relationInfoList))
	for _, relationInfo := range relationInfoList {
		releaseBusinessIDs = append(releaseBusinessIDs, relationInfo.ReleaseBusinessID)
	}

	// 分批查询问答
	qaChunks := slicex.Chunk(releaseBusinessIDs, 200)
	for _, bizIDChunk := range qaChunks {
		qas, err := GetCmdService().QaLogic.GetDao().GetDocQaList(ctx, nil, &qaEntity.DocQaFilter{
			BusinessIds: bizIDChunk,
		})
		if err != nil {
			logx.E(ctx, "GetDocQaList err:%+v, app_id:%d", err, app.PrimaryId)
			return err
		}
		// 4. 调用DeleteQAs接口删除问答
		err = GetCmdService().QaLogic.DeleteQAs(ctx, app.CorpPrimaryId, app.PrimaryId, 0, qas)
		if err != nil {
			logx.E(ctx, "DeleteQAs err:%+v, app_id:%d", err, app.PrimaryId)
			return err
		}
		logx.I(ctx, "DeleteQAs success, app_id:%d, deleted count:%d", app.PrimaryId, len(qas))
	}

	// 5. 删除冲突问记录（通过DiffTaskBusinessID）
	diffTaskBusinessIDs := make([]uint64, 0, len(relationInfoList))
	for _, relationInfo := range relationInfoList {
		diffTaskBusinessIDs = append(diffTaskBusinessIDs, relationInfo.DiffTaskBusinessID)
	}
	if len(diffTaskBusinessIDs) > 0 {
		err = GetCmdService().QaLogic.GetDao().DeleteQASimilarByBizIDs(ctx, diffTaskBusinessIDs)
		if err != nil {
			logx.E(ctx, "DeleteQASimilarByBizIDs err:%+v, app_id:%d", err, app.PrimaryId)
			return err
		}
		logx.I(ctx, "DeleteQASimilarByBizIDs success, app_id:%d, deleted count:%d", app.PrimaryId, len(diffTaskBusinessIDs))
	}

	// 6. 删除t_dev_release_relation_info表中的关联记录
	relationIDs := make([]uint64, 0, len(relationInfoList))
	for _, relationInfo := range relationInfoList {
		relationIDs = append(relationIDs, relationInfo.ID)
	}
	_, err = q.TDevReleaseRelationInfo.WithContext(ctx).
		Where(q.TDevReleaseRelationInfo.ID.In(relationIDs...)).Delete()
	if err != nil {
		logx.E(ctx, "delete TDevReleaseRelationInfo err:%+v, app_id:%d", err, app.PrimaryId)
		return err
	}
	logx.I(ctx, "delete TDevReleaseRelationInfo success, app_id:%d, deleted count:%d", app.PrimaryId, len(relationIDs))

	logx.I(ctx, "RevertAppQa success, app_id:%d", app.PrimaryId)
	return nil
}

// calculateQACopyStats 计算需要拷贝的QA统计信息
// 输入：apps []*entity.App
// 输出：totalQACount int, totalCopyCharSize uint64, error
func calculateQACopyStats(ctx context.Context, apps []*entity.App, corpBizId uint64) (int, uint64, error) {
	if len(apps) == 0 {
		return 0, 0, nil
	}

	// 使用并发处理
	type qaResult struct {
		appID        uint64
		appName      string
		qaCount      int
		copyCharSize uint64
		err          error
	}

	resultChan := make(chan qaResult, len(apps))
	var wg sync.WaitGroup

	// 并发处理每个 app
	for _, app := range apps {
		wg.Add(1)
		go func(a *entity.App) {
			defer wg.Done()

			a.CorpBizId = corpBizId
			_, qasNeedCopyFromPublishDomain, _, _, err := GetQaProcessingData(ctx, a)
			if err != nil {
				logx.E(ctx, "GetQaProcessingData app: %s err: %+v", jsonx.MustMarshal(a), err)
				resultChan <- qaResult{appID: a.PrimaryId, appName: a.Name, err: err}
				return
			}

			// 计算该 app 的 CharSize
			var appCopyCharSize uint64
			for _, qa := range qasNeedCopyFromPublishDomain {
				appCopyCharSize += qa.CharSize
			}

			resultChan <- qaResult{
				appID:        a.PrimaryId,
				appName:      a.Name,
				qaCount:      len(qasNeedCopyFromPublishDomain),
				copyCharSize: appCopyCharSize,
			}

			logx.I(ctx, "App[%d] %s: need copy %d QAs, char size: %d",
				a.PrimaryId, a.Name, len(qasNeedCopyFromPublishDomain), appCopyCharSize)
		}(app)
	}

	// 等待所有 goroutine 完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	var totalCopyCharSize uint64
	var totalQACount int
	for result := range resultChan {
		if result.err != nil {
			return 0, 0, result.err
		}
		totalQACount += result.qaCount
		totalCopyCharSize += result.copyCharSize
	}

	return totalQACount, totalCopyCharSize, nil
}

func RunCmdQACharUsage(cmd *cobra.Command, args []string) error {
	ctx := NewContext(cmd.Context())

	// 检查uin参数
	if flagQACharUsageUin == "" {
		logx.E(ctx, "uin is required")
		return errs.ErrWrapf(errs.ErrParams, "uin is required")
	}

	// 检查app_biz_ids和all参数
	if !flagQACharUsageAll && len(flagQACharUsageAppBizIDs) == 0 {
		logx.E(ctx, "either --app_biz_ids or --all must be specified")
		return errs.ErrWrapf(errs.ErrParams, "either --app_biz_ids or --all must be specified")
	}

	if flagQACharUsageAll && len(flagQACharUsageAppBizIDs) > 0 {
		logx.E(ctx, "--app_biz_ids and --all cannot be used together")
		return errs.ErrWrapf(errs.ErrParams, "--app_biz_ids and --all cannot be used together")
	}

	// 通过uin获取企业信息
	corp, err := GetCmdService().RpcImpl.PlatformAdmin.DescribeCorp(ctx, &pb.DescribeCorpReq{
		Uin: flagQACharUsageUin,
	})
	if err != nil {
		logx.E(ctx, "DescribeCorp by uin err: %+v", err)
		return err
	}
	corpID := corp.GetCorpPrimaryId()

	// 调用service.GetCharacterUsageInternal获取字符使用情况
	total, used, exceed, err := service.GetCharacterUsageInternal(ctx, corpID,
		GetCmdService().RpcImpl, GetCmdService().DocLogic, GetCmdService().QaLogic)
	if err != nil {
		logx.E(ctx, "GetCharacterUsageInternal err: %+v", err)
		return err
	}

	// 获取应用列表
	req := &appConfig.GetAppListReq{
		CorpPrimaryId: corpID,
		DisablePrompt: true,
	}

	// 根据all参数决定是否获取所有应用
	var apps []*entity.App
	var totalApps uint64
	if flagQACharUsageAll {
		// 处理该uin下所有应用
		apps, totalApps, err = GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	} else {
		// 只处理指定的app_biz_ids
		appBizIDs := slicex.Map(flagQACharUsageAppBizIDs, func(s string) uint64 { return cast.ToUint64(s) })
		req.BotBizIds = appBizIDs
		apps, totalApps, err = GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	}

	logx.I(ctx, "got apps of corp (uin: %s): %d", flagQACharUsageUin, totalApps)

	// 计算需要拷贝的QA统计信息
	totalQACount, totalCopyCharSize, err := calculateQACopyStats(ctx, apps, corp.GetCorpId())
	if err != nil {
		logx.E(ctx, "calculateQACopyStats err: %+v", err)
		return err
	}

	// 输出汇总信息
	logx.I(ctx, "\n========== Character Usage Summary ==========")
	logx.I(ctx, "Total Character Capacity: %d", total)
	logx.I(ctx, "Used Character Size: %d", used)
	logx.I(ctx, "Exceed Character Size: %d", exceed)
	logx.I(ctx, "Available Character Size: %d", int64(total)-int64(used))
	logx.I(ctx, "\n========== QA Copy Summary ==========")
	logx.I(ctx, "Total QAs Need Copy: %d", totalQACount)
	logx.I(ctx, "Total Copy Character Size: %d", totalCopyCharSize)
	logx.I(ctx, "After Copy Used Character Size: %d", uint64(used)+totalCopyCharSize)
	logx.I(ctx, "After Copy Available Character Size: %d", int64(total)-int64(used)-int64(totalCopyCharSize))
	logx.I(ctx, "=============================================")

	return nil
}
