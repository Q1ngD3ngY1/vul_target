package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/service"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	pb "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
)

var (
	cmdDoc = &cobra.Command{
		Use:     "doc",
		Short:   "Operations on document resources",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    func(cmd *cobra.Command, args []string) error { return cmd.Usage() },
	}
	flagDocBizIDs []string
)

// 全局staffID到staffBizID的缓存
var staffIDToStaffBizIDCache sync.Map

var (
	cmdDocList = &cobra.Command{
		Use:     "list",
		Short:   "List document resources with the given filters",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdDocList,
	}
	flagDocListFields []string
)

var (
	cmdDocEnableScope = &cobra.Command{
		Use:     "enable-scope",
		Short:   "Set enable_scope for document resources",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdDocEnableScope,
	}
	flagDocEnableScopeUin                    string
	flagDocEnableScopeAppBizIDs              []string
	flagDocEnableScopeSpaceID                string
	flagDocEnableScopeAll                    bool
	flagDocEnableScopeWorkerCount            int
	flagDocEnableScopeUpdateLabelWorkerCount int
)

var (
	cmdDocCharUsage = &cobra.Command{
		Use:     "char-usage",
		Short:   "Get character usage and calculate Doc copy size",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdDocCharUsage,
	}
	flagDocCharUsageUin       string
	flagDocCharUsageAppBizIDs []string
	flagDocCharUsageAll       bool
)

var (
	cmdDocRevert = &cobra.Command{
		Use:     "revert",
		Short:   "Revert Doc enable_scope to 0 and delete copied Docs from publish domain",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdDocRevert,
	}
	flagDocRevertUin       string
	flagDocRevertAppBizIDs []string
	flagDocRevertSpaceID   string
	flagDocRevertAll       bool
)

func init() {
	flags := cmdDoc.PersistentFlags()
	flags.StringSliceVar(&flagDocBizIDs, "biz_ids", []string{}, "biz IDs of document resources")

	flags = cmdDocList.PersistentFlags()
	flags.StringSliceVar(&flagDocListFields, "fields", []string{}, "db field names to list documents separated with comma")

	flags = cmdDocEnableScope.PersistentFlags()
	flags.StringVar(&flagDocEnableScopeUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagDocEnableScopeAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (optional, cannot be used with --space_id or --all)")
	flags.StringVar(&flagDocEnableScopeSpaceID, "space_id", "", "space ID to process all apps under it (optional, cannot be used with --app_biz_ids or --all)")
	flags.BoolVar(&flagDocEnableScopeAll, "all", false, "process all apps under the uin (optional, cannot be used with --app_biz_ids or --space_id)")
	flags.IntVar(&flagDocEnableScopeWorkerCount, "worker_count", 5, "number of workers for concurrent document copying (default: 5, range: 1-20)")
	flags.IntVar(&flagDocEnableScopeUpdateLabelWorkerCount, "update_label_worker_count", 5, "number of workers for concurrent document label updating (default: 5, range: 1-20)")

	flags = cmdDocCharUsage.PersistentFlags()
	flags.StringVar(&flagDocCharUsageUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagDocCharUsageAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (required, unless --all is specified)")
	flags.BoolVar(&flagDocCharUsageAll, "all", false, "process all apps under the uin")

	flags = cmdDocRevert.PersistentFlags()
	flags.StringVar(&flagDocRevertUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagDocRevertAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (optional, cannot be used with --space_id or --all)")
	flags.StringVar(&flagDocRevertSpaceID, "space_id", "", "space ID to process all apps under it (optional, cannot be used with --app_biz_ids or --all)")
	flags.BoolVar(&flagDocRevertAll, "all", false, "process all apps under the uin (optional, cannot be used with --app_biz_ids or --space_id)")

	cmdDoc.AddCommand(cmdDocList)
	cmdDoc.AddCommand(cmdDocEnableScope)
	cmdDoc.AddCommand(cmdDocCharUsage)
	cmdDoc.AddCommand(cmdDocRevert)
}

// getStaffBizIDByStaffID 通过staffID获取staffBizID，使用缓存优化性能
func getStaffBizIDByStaffID(ctx context.Context, staffID uint64) (uint64, error) {
	// 先从缓存中查找
	if bizID, ok := staffIDToStaffBizIDCache.Load(staffID); ok {
		return bizID.(uint64), nil
	}

	// 缓存中不存在，通过RPC获取
	staff, err := GetCmdService().RpcImpl.GetStaffByID(ctx, staffID)
	if err != nil {
		logx.E(ctx, "getStaffBizIDByStaffID GetStaffByID err:%v, staffID:%d", err, staffID)
		return 0, err
	}

	// 保存到缓存
	staffIDToStaffBizIDCache.Store(staffID, staff.BusinessID)
	logx.I(ctx, "getStaffBizIDByStaffID cached staffID:%d -> staffBizID:%s", staffID, staff.BusinessID)

	return staff.BusinessID, nil
}

func RunCmdDocList(cmd *cobra.Command, args []string) error {
	filter := &docEntity.DocFilter{
		BusinessIds: slicex.Map(flagDocBizIDs, func(s string) uint64 { return cast.ToUint64(s) }),
		Limit:       PageSize,
	}
	if len(CorpIDs) > 0 {
		filter.CorpId = cast.ToUint64(CorpIDs[0])
	}
	docList, err := GetCmdService().DocLogic.GetDao().GetDocList(cmd.Context(), nil, filter)
	if err != nil {
		return err
	}

	tw := table.NewWriter()
	tw.SetOutputMirror(os.Stdout)
	var header table.Row
	if !slices.Contains(flagDocListFields, "id") {
		flagDocListFields = append([]string{"id"}, flagDocListFields...)
	}
	if !slices.Contains(flagDocListFields, "business_id") {
		flagDocListFields = append([]string{"business_id"}, flagDocListFields...)
	}
	if !slices.Contains(flagDocListFields, "corp_id") {
		flagDocListFields = append([]string{"corp_id"}, flagDocListFields...)
	}
	if !slices.Contains(flagDocListFields, "robot_id") {
		flagDocListFields = append([]string{"robot_id"}, flagDocListFields...)
	}
	if !slices.Contains(flagDocListFields, "file_name") {
		flagDocListFields = append([]string{"file_name"}, flagDocListFields...)
	}

	for _, field := range flagDocListFields {
		header = append(header, field)
	}
	tw.AppendHeader(header)
	for _, d := range docList {
		var row table.Row
		for _, field := range flagDocListFields {
			dv := reflect.ValueOf(d).Elem()
			dt := dv.Type()

			var fieldIdx int
			for i := 0; i < dt.NumField(); i++ {
				if field == dt.Field(i).Tag.Get("db") {
					fieldIdx = i
					break
				}
			}
			row = append(row, dv.Field(fieldIdx).Interface())
		}
		tw.AppendRow(row)
	}
	tw.Render()
	return nil
}

func RunCmdDocEnableScope(cmd *cobra.Command, args []string) error {
	return RunEnableScopeCommand(cmd, ProcessAppDoc, EnableScopeParams{
		Uin:       flagDocEnableScopeUin,
		AppBizIDs: flagDocEnableScopeAppBizIDs,
		SpaceID:   flagDocEnableScopeSpaceID,
		All:       flagDocEnableScopeAll,
		TypeName:  "Doc",
	}, &AppWorkerConfig{
		DocCopyWorkerCount:        flagDocEnableScopeWorkerCount,
		DocUpdateLabelWorkerCount: flagDocEnableScopeUpdateLabelWorkerCount,
	})
}

func RunCmdDocCharUsage(cmd *cobra.Command, args []string) error {
	ctx := NewContext(cmd.Context())

	// 检查uin参数
	if flagDocCharUsageUin == "" {
		logx.E(ctx, "uin is required")
		return errs.ErrWrapf(errs.ErrParams, "uin is required")
	}

	// 检查app_biz_ids和all参数
	if !flagDocCharUsageAll && len(flagDocCharUsageAppBizIDs) == 0 {
		logx.E(ctx, "either --app_biz_ids or --all must be specified")
		return errs.ErrWrapf(errs.ErrParams, "either --app_biz_ids or --all must be specified")
	}

	if flagDocCharUsageAll && len(flagDocCharUsageAppBizIDs) > 0 {
		logx.E(ctx, "--app_biz_ids and --all cannot be used together")
		return errs.ErrWrapf(errs.ErrParams, "--app_biz_ids and --all cannot be used together")
	}

	// 通过uin获取企业信息
	corp, err := GetCmdService().RpcImpl.PlatformAdmin.DescribeCorp(ctx, &pb.DescribeCorpReq{
		Uin: flagDocCharUsageUin,
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
	if flagDocCharUsageAll {
		// 处理该uin下所有应用
		apps, totalApps, err = GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	} else {
		// 只处理指定的app_biz_ids
		appBizIDs := slicex.Map(flagDocCharUsageAppBizIDs, func(s string) uint64 { return cast.ToUint64(s) })
		req.BotBizIds = appBizIDs
		apps, totalApps, err = GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	}

	logx.I(ctx, "got apps of corp (uin: %s): %d", flagDocCharUsageUin, totalApps)

	// 计算需要拷贝的文档统计信息
	totalDocCount, totalCopyCharSize, err := calculateDocCopyStats(ctx, apps, corp.GetCorpId())
	if err != nil {
		logx.E(ctx, "calculateDocCopyStats err: %+v", err)
		return err
	}

	// 输出汇总信息
	logx.I(ctx, "\n========== Character Usage Summary ==========")
	logx.I(ctx, "Total Character Capacity: %d", total)
	logx.I(ctx, "Used Character Size: %d", used)
	logx.I(ctx, "Exceed Character Size: %d", exceed)
	logx.I(ctx, "Available Character Size: %d", int64(total)-int64(used))
	logx.I(ctx, "\n========== Doc Copy Summary ==========")
	logx.I(ctx, "Total Docs Need Copy: %d", totalDocCount)
	logx.I(ctx, "Total Copy Character Size: %d", totalCopyCharSize)
	logx.I(ctx, "After Copy Used Character Size: %d", uint64(used)+totalCopyCharSize)
	logx.I(ctx, "After Copy Available Character Size: %d", int64(total)-int64(used)-int64(totalCopyCharSize))
	logx.I(ctx, "=============================================")

	return nil
}

// calculateDocCopyStats 计算需要拷贝的文档统计信息
// 输入：apps []*entity.App
// 输出：totalDocCount int, totalCopyCharSize uint64, error
func calculateDocCopyStats(ctx context.Context, apps []*entity.App, corpBizId uint64) (int, uint64, error) {
	if len(apps) == 0 {
		return 0, 0, nil
	}

	// 使用并发处理
	type docResult struct {
		appID        uint64
		appName      string
		docCount     int
		copyCharSize uint64
		err          error
	}

	resultChan := make(chan docResult, len(apps))
	var wg sync.WaitGroup

	// 并发处理每个 app
	for _, app := range apps {
		wg.Add(1)
		go func(a *entity.App) {
			defer wg.Done()

			a.CorpBizId = corpBizId
			_, docsNeedCopyFromPublishDomain, _, _, err := GetDocProcessingData(ctx, a)
			if err != nil {
				logx.E(ctx, "GetDocProcessingData app: %s err: %+v", jsonx.MustMarshal(a), err)
				resultChan <- docResult{appID: a.PrimaryId, appName: a.Name, err: err}
				return
			}

			// 计算该 app 的 CharSize
			var appCopyCharSize uint64
			for _, doc := range docsNeedCopyFromPublishDomain {
				appCopyCharSize += uint64(doc.CharSize)
			}

			resultChan <- docResult{
				appID:        a.PrimaryId,
				appName:      a.Name,
				docCount:     len(docsNeedCopyFromPublishDomain),
				copyCharSize: appCopyCharSize,
			}

			logx.I(ctx, "App[%d] %s: need copy %d Docs, char size: %d",
				a.PrimaryId, a.Name, len(docsNeedCopyFromPublishDomain), appCopyCharSize)
		}(app)
	}

	// 等待所有 goroutine 完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	var totalCopyCharSize uint64
	var totalDocCount int
	for result := range resultChan {
		if result.err != nil {
			return 0, 0, result.err
		}
		totalDocCount += result.docCount
		totalCopyCharSize += result.copyCharSize
	}

	return totalDocCount, totalCopyCharSize, nil
}

// GetDocProcessingData 获取文档处理所需的数据
// 返回值：docsEnableScopeMap, docsNeedCopyFromPublishDomain, docNodesNeedCopyFromPublishDomain, error
func GetDocProcessingData(ctx context.Context, app *entity.App) (map[uint32][]*docEntity.Doc, []*docEntity.Doc, []*segEntity.EsSegment, []uint32, error) {
	logx.I(ctx, "GetDocProcessingData start, app_id: %d, app_name: %s", app.PrimaryId, app.Name)
	// 获取该应用所有enable_scope为0的文档，不为0的说明已经刷过了
	docList, err := GetCmdService().DocLogic.GetDao().GetDocList(ctx, nil, &docEntity.DocFilter{
		RobotId:     app.PrimaryId,
		EnableScope: ptrx.Uint32(entity.EnableScopeInvalid),
	})
	if err != nil {
		logx.E(ctx, "GetDocList err: %+v, app_id: %d", err, app.PrimaryId)
		return nil, nil, nil, nil, err
	}
	logx.I(ctx, "GetDocList success, app_id: %d, doc_count: %d", app.PrimaryId, len(docList))

	// key: enable_scope value: doc_id_list
	docsEnableScopeMap := make(map[uint32][]*docEntity.Doc)
	// 需要从发布域复制数据到开发域的doc_id_list
	docsNeedCopy := make([]*docEntity.Doc, 0)
	docSegmentsNeedCopy := make([]*segEntity.EsSegment, 0)
	docEnableScopesNeedCopy := make([]uint32, 0)
	for idx, doc := range docList {
		logx.I(ctx, "Processing Doc [%d/%d], doc_id: %d, file_name: %s", idx+1, len(docList), doc.ID, doc.FileName)
		// 查询任意一个片段的过期时间即可
		esSegments, err := GetCmdService().SegLogic.GetDao().QueryDocSegmentInProd(ctx, app.PrimaryId, doc.ID, []string{}, 1)
		if err != nil {
			logx.E(ctx, "QueryDocSegmentInProd err: %+v, doc_id: %d", err, doc.ID)
			return nil, nil, nil, nil, err
		}
		logx.I(ctx, "QueryDocSegmentInProd success, doc_id: %d, segment_count: %d", doc.ID, len(esSegments))
		var esSegment *segEntity.EsSegment
		if len(esSegments) >= 1 {
			esSegment = esSegments[0]
		}
		devEnableScope, publishEnableScope := getDocEnableScope(ctx, app.IsShared, doc, esSegment)
		logx.I(ctx, "Doc [%d] devEnableScope: %d, publishEnableScope: %d", doc.ID, devEnableScope, publishEnableScope)

		appendDocsEnableScopeMap(docsEnableScopeMap, devEnableScope, doc)

		if publishEnableScope == entity.EnableScopePublish {
			// 发布域有有效数据，需要从发布域复制数据到开发域
			logx.I(ctx, "Doc [%d] need copy from publish domain, char_size: %d, enable_scope: %d",
				doc.ID, doc.CharSize, publishEnableScope)
			docsNeedCopy = append(docsNeedCopy, doc)
			docSegmentsNeedCopy = append(docSegmentsNeedCopy, esSegment)
			docEnableScopesNeedCopy = append(docEnableScopesNeedCopy, publishEnableScope)
		}
	}

	logx.I(ctx, "GetDocProcessingData finished, app_id: %d, docsEnableScopeMap size: %d, docsNeedCopy count: %d",
		app.PrimaryId, len(docsEnableScopeMap), len(docsNeedCopy))
	return docsEnableScopeMap, docsNeedCopy, docSegmentsNeedCopy, docEnableScopesNeedCopy, nil
}

func ProcessAppDoc(ctx context.Context, app *entity.App, config *AppWorkerConfig) error {
	logx.I(ctx, "processing app: %s", jsonx.MustMarshal(app))
	if config == nil {
		logx.E(ctx, "ProcessAppDoc config is nil")
		return fmt.Errorf("ProcessAppDoc config is nil")
	}
	// 获取文档处理所需的数据
	docsEnableScopeMap, docsNeedCopy, docSegmentsNeedCopy, docEnableScopesNeedCopy, err := GetDocProcessingData(ctx, app)
	if err != nil {
		logx.E(ctx, "GetDocProcessingData err:%+v", err)
		return err
	}
	// 批量更新ES的enable_scope标签
	err = updateDocEnableScopeInES(ctx, app, docsEnableScopeMap, config.DocUpdateLabelWorkerCount)
	if err != nil {
		logx.E(ctx, "updateDocEnableScopeInES err:%+v", err)
		return err
	}
	// 批量更新数据库enable_scope字段，只有在上一步更新ES成功后，才更新数据库，避免数据库和标签不一致
	err = updateDocEnableScopeInDB(ctx, docsEnableScopeMap)
	if err != nil {
		logx.E(ctx, "updateDocEnableScopeInDB err:%+v", err)
		return err
	}
	// 从发布域复制数据库到开发域（并发处理）
	if len(docsNeedCopy) > 0 {
		err = copyDocsFromPublishDomain2DevDomainConcurrently(ctx, app, docsNeedCopy, docSegmentsNeedCopy, docEnableScopesNeedCopy, config.DocCopyWorkerCount)
		if err != nil {
			logx.E(ctx, "copyDocsFromPublishDomain2DevDomainConcurrently err:%+v", err)
			return err
		}
	}

	return nil
}

func getDocEnableScope(ctx context.Context, isSharedApp bool, doc *docEntity.Doc, esSegment *segEntity.EsSegment) (uint32, uint32) {
	if isSharedApp || doc.Status == docEntity.DocStatusReleaseSuccess || esSegment == nil {
		// 如果是共享知识库，或者默认知识库的已发布文档，或者从来没有发布过，则通过attribute_flag字段来判断是否开启
		if doc.HasAttributeFlag(docEntity.DocAttributeFlagDisable) {
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
	if slices.Contains(docEntity.DocExceedStatus, doc.Status) {
		return entity.EnableScopeDev, entity.EnableScopeInvalid
	}
	// 如果是默认知识库，非发布状态、非超量失效状态，且曾经发布过
	devEnableScope := uint32(entity.EnableScopeDev)
	if doc.HasAttributeFlag(docEntity.DocAttributeFlagDisable) {
		devEnableScope = entity.EnableScopeDisable
	}

	publishEnableScope := uint32(entity.EnableScopePublish)
	if esSegment.ExpireTime != 0 && esSegment.ExpireTime < time.Now().Unix() && esSegment.ExpireTime != doc.ExpireEnd.Unix() {
		// 需要通过查询文档在发布域的片段，通过对比片段的过期时间和数据表中的过期时间来判断发布域的启用状态
		publishEnableScope = entity.EnableScopeDisable
	}
	return devEnableScope, publishEnableScope
}

// appendDocsEnableScopeMap 追加docsEnableScopeMap
func appendDocsEnableScopeMap(docsEnableScopeMap map[uint32][]*docEntity.Doc, enableScope uint32, doc *docEntity.Doc) {
	if _, ok := docsEnableScopeMap[enableScope]; !ok {
		docsEnableScopeMap[enableScope] = make([]*docEntity.Doc, 0)
	}
	docsEnableScopeMap[enableScope] = append(docsEnableScopeMap[enableScope], doc)
}

// updateDocEnableScopeInDB 更新数据库enable_scope字段
func updateDocEnableScopeInDB(ctx context.Context, docsEnableScopeMap map[uint32][]*docEntity.Doc) error {
	for enableScope, docs := range docsEnableScopeMap {
		docIds := make([]uint64, 0)
		for _, doc := range docs {
			docIds = append(docIds, doc.ID)
		}
		docChunks := slicex.Chunk(docIds, 200)
		for _, docChunkIDs := range docChunks {
			filter := &docEntity.DocFilter{
				IDs: docChunkIDs,
			}
			err := GetCmdService().DocLogic.GetDao().BatchUpdateDocsByFilter(ctx, filter, map[string]any{"enable_scope": enableScope}, nil)
			if err != nil {
				logx.E(ctx, "BatchUpdateDocsByFilter docs:%+v err:%+v", docs, err)
				return err
			}
			logx.I(ctx, "BatchUpdateDocsByFilter docs:%+v", docs)
		}
	}
	return nil
}

// updateDocEnableScopeInES 更新ES enable_scope字段
func updateDocEnableScopeInES(ctx context.Context, app *entity.App, docsEnableScopeMap map[uint32][]*docEntity.Doc, docUpdateLabelWorkerCount int) error {
	embeddingVersion := app.Embedding.Version
	embeddingModel, err := GetCmdService().VectorLogic.ExtractEmbeddingModelOfKB(ctx, app.CorpBizId, app)
	if err != nil {
		logx.E(ctx, "updateDocEnableScopeInES ExtractEmbeddingModelOfKB err:%v,app:%+v", err, app)
	} else {
		if embeddingModel != "" {
			embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
		}
	}
	logx.I(ctx, "updateDocEnableScopeInES embeddingVersion:%s, embeddingModel:%s",
		embeddingVersion, embeddingModel)

	// 设置worker数量
	workerCount := gox.IfElse(docUpdateLabelWorkerCount > 0, docUpdateLabelWorkerCount, defaultDocUpdateLabelWorkerCount)

	for enableScope, docs := range docsEnableScopeMap {
		label, ok := entity.EnableScopeDb2Label[enableScope]
		if !ok {
			logx.E(ctx, "updateDocEnableScopeInES enableScope:%d not found in EnableScopeDb2Label", enableScope)
			continue
		}
		vectorLabels := []*retrieval.VectorLabel{
			{
				Name:  entity.EnableScopeAttr,
				Value: label,
			},
		}

		// 过滤掉已删除的文档
		validDocs := make([]*docEntity.Doc, 0, len(docs))
		for _, doc := range docs {
			if doc.HasDeleted() {
				logx.I(ctx, "updateDocEnableScopeInES docID:%d docBizID:%d has deleted", doc.ID, doc.BusinessID)
				continue
			}
			validDocs = append(validDocs, doc)
		}

		if len(validDocs) == 0 {
			continue
		}

		totalDocs := len(validDocs)
		logx.I(ctx, "updateDocEnableScopeInES start, enableScope:%d, total docs:%d, worker_count:%d",
			enableScope, totalDocs, workerCount)

		// 如果文档数量少于worker数量，使用文档数量
		if totalDocs < workerCount {
			workerCount = totalDocs
		}

		// 创建任务通道和错误通道
		type docTask struct {
			doc   *docEntity.Doc
			index int
		}
		taskChan := make(chan *docTask, totalDocs)
		errChan := make(chan error, totalDocs)
		var wg sync.WaitGroup

		// 启动 workers
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				logx.I(ctx, "doc update label worker %d: started", workerID)

				for task := range taskChan {
					doc := task.doc
					startTime := time.Now()
					logx.I(ctx, "doc update label worker %d: [%d/%d] processing doc %d (file: %s)",
						workerID, task.index+1, totalDocs, doc.ID, doc.FileName)

					// 获取文档的所有片段
					startID, limit := uint64(0), uint64(5000)
					hasError := false
					for {
						segments, lastID, err := GetCmdService().SegLogic.GetSegmentByDocID(ctx, doc.RobotID, doc.ID, startID, limit,
							[]string{segEntity.DocSegmentTblColID, segEntity.DocSegmentTblColType, segEntity.DocSegmentTblColSegmentType})
						if err != nil {
							logx.E(ctx, "doc update label worker %d: GetSegmentByDocID err:%v,docID:%v", workerID, err, doc.ID)
							// 使用非阻塞方式发送错误
							select {
							case errChan <- err:
							default:
								logx.E(ctx, "doc update label worker %d: error channel full, dropping error: %v", workerID, err)
							}
							hasError = true
							break
						}
						if len(segments) == 0 {
							break
						}
						startID = lastID

						// 更新片段的enable_scope标签
						segmentIDs := make([]uint64, 0)
						for _, segment := range segments {
							if segment.IsSegmentForQA() || segment.SegmentType == segEntity.SegmentTypeText2SQLMeta {
								continue
							}
							segmentIDs = append(segmentIDs, segment.ID)
						}

						if len(segmentIDs) > 0 {
							for _, segIDChunk := range slicex.Chunk(segmentIDs, 100) {
								req := &retrieval.UpdateLabelReq{
									RobotId:            doc.RobotID,
									AppBizId:           app.BizId,
									EnvType:            retrieval.EnvType_Test,
									IndexId:            entity.SegmentReviewVersionID,
									Ids:                segIDChunk,
									DocType:            entity.DocTypeSegment,
									SegmentType:        segEntity.SegmentTypeSegment,
									EmbeddingModelName: embeddingModel,
									EmbeddingVersion:   embeddingVersion,
									Labels:             vectorLabels,
								}
								_, err = GetCmdService().RpcImpl.RetrievalDirectIndex.UpdateVectorLabel(ctx, req)
								if err != nil {
									logx.E(ctx, "doc update label worker %d: UpdateVectorLabel err:%v,req:%+v", workerID, err, req)
									// 使用非阻塞方式发送错误
									select {
									case errChan <- err:
									default:
										logx.E(ctx, "doc update label worker %d: error channel full, dropping error: %v", workerID, err)
									}
									hasError = true
									break
								}
							}
							// 如果更新标签时出错，退出外层循环
							if hasError {
								break
							}
						}

						sleepSwitch, sleepMillisecond := config.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
							config.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
						if sleepSwitch {
							time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
						}
					}

					if !hasError {
						duration := time.Since(startTime)
						logx.I(ctx, "doc update label worker %d: [%d/%d] successfully processed doc %d in %v",
							workerID, task.index+1, totalDocs, doc.ID, duration)
					} else {
						logx.E(ctx, "doc update label worker %d: [%d/%d] failed to process doc %d",
							workerID, task.index+1, totalDocs, doc.ID)
					}
				}

				logx.I(ctx, "doc update label worker %d: finished", workerID)
			}(i)
		}

		// 发送任务到通道
		for i, doc := range validDocs {
			taskChan <- &docTask{
				doc:   doc,
				index: i,
			}
		}
		close(taskChan)

		// 等待所有 worker 完成
		wg.Wait()
		close(errChan)

		// 检查是否有错误
		var errs []error
		for err := range errChan {
			errs = append(errs, err)
		}

		if len(errs) > 0 {
			logx.E(ctx, "updateDocEnableScopeInES completed with %d errors out of %d docs",
				len(errs), totalDocs)
			// 返回第一个错误
			return fmt.Errorf("failed to update %d docs, first error: %w", len(errs), errs[0])
		}

		logx.I(ctx, "updateDocEnableScopeInES completed successfully, enableScope:%d, total docs:%d",
			enableScope, totalDocs)
	}
	return nil
}

// DocCopyTask 文档复制任务
type DocCopyTask struct {
	Doc                *docEntity.Doc
	EsSegment          *segEntity.EsSegment
	PublishEnableScope uint32
	Index              int
}

// copyDocsFromPublishDomain2DevDomainConcurrently 并发复制文档从发布域到开发域
func copyDocsFromPublishDomain2DevDomainConcurrently(ctx context.Context, app *entity.App,
	docsNeedCopy []*docEntity.Doc, docSegmentsNeedCopy []*segEntity.EsSegment,
	docEnableScopesNeedCopy []uint32, docCopyWorkerCount int) error {

	totalDocs := len(docsNeedCopy)
	logx.I(ctx, "copyDocsFromPublishDomain2DevDomainConcurrently start, app_id: %d, total docs: %d, doc_worker_count: %d",
		app.PrimaryId, totalDocs, docCopyWorkerCount)

	// 设置worker数量，使用传入的docCopyWorkerCount参数
	workerCount := gox.IfElse(docCopyWorkerCount > 0, docCopyWorkerCount, defaultDocCopyWorkerCount)
	if totalDocs < workerCount {
		workerCount = totalDocs // 文档数量少于worker数量时，使用文档数量
	}
	logx.I(ctx, "starting %d workers to copy %d docs", workerCount, totalDocs)

	// 创建任务通道
	taskChan := make(chan *DocCopyTask, totalDocs)
	// 创建错误通道
	errChan := make(chan error, totalDocs)
	// 使用 WaitGroup 等待所有 worker 完成
	var wg sync.WaitGroup

	// 启动 workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logx.I(ctx, "doc copy worker %d: started", workerID)

			for task := range taskChan {
				startTime := time.Now()
				logx.I(ctx, "doc copy worker %d: [%d/%d] processing doc %d (file: %s, char_size: %d)",
					workerID, task.Index+1, totalDocs, task.Doc.ID, task.Doc.FileName, task.Doc.CharSize)

				err := copyDocFromPublishDomain2DevDomain(ctx, app, task.Doc, task.EsSegment, task.PublishEnableScope)
				if err != nil {
					logx.E(ctx, "doc copy worker %d: [%d/%d] failed to copy doc %d: %+v",
						workerID, task.Index+1, totalDocs, task.Doc.ID, err)
					errChan <- fmt.Errorf("doc %d (file: %s): %w", task.Doc.ID, task.Doc.FileName, err)
					continue
				}

				duration := time.Since(startTime)
				logx.I(ctx, "doc copy worker %d: [%d/%d] successfully copied doc %d in %v",
					workerID, task.Index+1, totalDocs, task.Doc.ID, duration)
			}

			logx.I(ctx, "doc copy worker %d: finished", workerID)
		}(i)
	}

	// 发送任务到通道
	for i, doc := range docsNeedCopy {
		taskChan <- &DocCopyTask{
			Doc:                doc,
			EsSegment:          docSegmentsNeedCopy[i],
			PublishEnableScope: docEnableScopesNeedCopy[i],
			Index:              i,
		}
	}
	close(taskChan)

	// 等待所有 worker 完成
	wg.Wait()
	close(errChan)

	// 检查是否有错误
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		logx.E(ctx, "copyDocsFromPublishDomain2DevDomainConcurrently completed with %d errors out of %d docs",
			len(errs), totalDocs)
		// 返回第一个错误
		return fmt.Errorf("failed to copy %d docs, first error: %w", len(errs), errs[0])
	}

	logx.I(ctx, "copyDocsFromPublishDomain2DevDomainConcurrently completed successfully, app_id: %d, total docs: %d",
		app.PrimaryId, totalDocs)
	return nil
}

// copyDocFromPublishDomain2DevDomain 将发布域的文档复制到开发域
func copyDocFromPublishDomain2DevDomain(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	esSegment *segEntity.EsSegment, publishEnableScope uint32) error {

	// 1. 创建新文档
	newDevDomainDoc, err := createNewDevDomainDoc(ctx, app, oldDevDomainDoc, esSegment, publishEnableScope)
	if err != nil {
		return err
	}

	// 2. 写入新旧文档的关联关系到t_dev_release_relation_info表
	err = createDocDevReleaseRelation(ctx, app, oldDevDomainDoc, newDevDomainDoc)
	if err != nil {
		return err
	}

	// 2.1. 复制文档解析任务记录
	err = copyDocParseRecords(ctx, app, oldDevDomainDoc, newDevDomainDoc)
	if err != nil {
		return err
	}

	// 3. 复制文档标签
	err = copyDocLabels(ctx, app, oldDevDomainDoc, newDevDomainDoc, esSegment)
	if err != nil {
		return err
	}

	// 4. 查询发布域的所有片段
	segmentsInProdNodeInfo, err := queryPublishDomainSegments(ctx, app, oldDevDomainDoc)
	if err != nil {
		return err
	}

	// 5. 复制big_data数据
	oldBigDataID2NewBigDataID, err := copyDocBigData(ctx, oldDevDomainDoc, newDevDomainDoc)
	if err != nil {
		return err
	}

	// 6. 复制org_data数据
	oldOrgDataString2NewOrgDataBizID, err := copyDocOrgData(ctx, app, oldDevDomainDoc, newDevDomainDoc, segmentsInProdNodeInfo)
	if err != nil {
		return err
	}

	// 7. 复制segment数据
	newSegments, oldSegmentID2NewSegmentID, err := copyDocSegments(ctx, oldDevDomainDoc, newDevDomainDoc,
		segmentsInProdNodeInfo, oldBigDataID2NewBigDataID, oldOrgDataString2NewOrgDataBizID)
	if err != nil {
		return err
	}

	// 8. 复制segment_image数据
	err = copyDocSegmentImages(ctx, oldDevDomainDoc, newDevDomainDoc, oldSegmentID2NewSegmentID)
	if err != nil {
		return err
	}

	// 9. 复制segment_page_info数据
	err = copyDocSegmentPageInfos(ctx, oldDevDomainDoc, newDevDomainDoc, oldSegmentID2NewSegmentID)
	if err != nil {
		return err
	}

	// 10. 写入向量库
	err = addSegmentsToVectorDB(ctx, app, oldDevDomainDoc, newDevDomainDoc, esSegment, newSegments)
	if err != nil {
		return err
	}

	// 11. 创建文档对比任务
	if !oldDevDomainDoc.HasDeleted() {
		// 旧文档未删除，创建文档比对详情异步任务
		docDiff, err := createDocDiffTask(ctx, app, oldDevDomainDoc, newDevDomainDoc)
		if err != nil {
			return err
		}

		// 12. 更新t_dev_release_relation_info表中的DiffTaskBusinessID字段
		err = updateDocDevReleaseRelationDiffTaskID(ctx, app, oldDevDomainDoc, newDevDomainDoc, docDiff)
		if err != nil {
			return err
		}

		// 13. 创建文档比对详情异步任务
		err = createDocDiffDataAsyncTask(ctx, app, docDiff)
		if err != nil {
			return err
		}
	}

	// 14. 更新应用已使用字符数
	err = updateAppUsedCharSize(ctx, oldDevDomainDoc, newDevDomainDoc)
	if err != nil {
		return err
	}

	logx.I(ctx, "copyDocFromPublishDomain2DevDomain success,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
		oldDevDomainDoc.ID, newDevDomainDoc.ID)
	return nil
}

// appendOneToFileName 将文件路径中文件名末尾拼接字符串"1"，与旧文件的cos区分
// 输入: /corp/1747547736762744832/1994308374538044032/doc/GvVAbgLaoenlNuzlVbHO-1994345964910830400.txt
// 输出: /corp/1747547736762744832/1994308374538044032/doc/GvVAbgLaoenlNuzlVbHO-19943459649108304001.txt
func appendOneToFileName(filePath string) (string, error) {
	// 分离目录和文件名
	dir := filepath.Dir(filePath)
	baseName := filepath.Base(filePath)

	// 分离文件名和扩展名
	ext := filepath.Ext(baseName)
	nameWithoutExt := strings.TrimSuffix(baseName, ext)

	// 在文件名末尾拼接字符串"1"
	newNameWithoutExt := nameWithoutExt + "1"
	newBaseName := newNameWithoutExt + ext

	// 组合完整路径
	newFilePath := filepath.Join(dir, newBaseName)

	return newFilePath, nil
}

// createNewDevDomainDoc 创建新的开发域文档
func createNewDevDomainDoc(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	esSegment *segEntity.EsSegment, publishEnableScope uint32) (*docEntity.Doc, error) {
	var err error
	newDevDomainDoc := &docEntity.Doc{
		BusinessID:          idgen.GetId(),
		RobotID:             app.PrimaryId,
		CorpID:              app.CorpPrimaryId,
		StaffID:             oldDevDomainDoc.StaffID,
		FileName:            oldDevDomainDoc.FileName,
		FileNameInAudit:     oldDevDomainDoc.FileNameInAudit,
		FileType:            oldDevDomainDoc.FileType,
		FileSize:            oldDevDomainDoc.FileSize,
		Bucket:              oldDevDomainDoc.Bucket,
		CosHash:             oldDevDomainDoc.CosHash,
		Status:              docEntity.DocStatusWaitRelease,
		IsDeleted:           false,
		IsRefer:             oldDevDomainDoc.IsRefer,
		Source:              oldDevDomainDoc.Source,
		WebURL:              oldDevDomainDoc.WebURL,
		BatchID:             oldDevDomainDoc.BatchID,
		AuditFlag:           oldDevDomainDoc.AuditFlag,
		IsCreatedQA:         oldDevDomainDoc.IsCreatedQA,
		CharSize:            oldDevDomainDoc.CharSize,
		NextAction:          docEntity.DocNextActionAdd,
		AttrRange:           docEntity.AttrRangeAll,
		ReferURLType:        oldDevDomainDoc.ReferURLType,
		ExpireStart:         oldDevDomainDoc.ExpireStart,
		ExpireEnd:           time.Unix(esSegment.ExpireTime, 0),
		Opt:                 oldDevDomainDoc.Opt,
		CategoryID:          oldDevDomainDoc.CategoryID,
		OriginalURL:         oldDevDomainDoc.OriginalURL,
		CustomerKnowledgeId: oldDevDomainDoc.CustomerKnowledgeId,
		AttributeFlag:       oldDevDomainDoc.AttributeFlag,
		IsDownloadable:      oldDevDomainDoc.IsDownloadable,
		UpdatePeriodH:       oldDevDomainDoc.UpdatePeriodH,
		NextUpdateTime:      oldDevDomainDoc.NextUpdateTime,
		SplitRule:           oldDevDomainDoc.SplitRule,
		EnableScope:         publishEnableScope,
	}

	newDevDomainDoc.CosURL, err = appendOneToFileName(oldDevDomainDoc.CosURL)
	if err != nil {
		logx.E(ctx, "appendOneToFileName err:%v,oldDevDomainDoc.CosURL:%v", err, oldDevDomainDoc.CosURL)
		return nil, err
	}

	_, err = GetCmdService().S3.CopyObject(ctx, oldDevDomainDoc.CosURL, newDevDomainDoc.CosURL, nil)
	if err != nil {
		logx.E(ctx, "CopyObject err:%v,oldDevDomainDoc.CosURL:%v,newDevDomainDoc.CosURL:%v", err, oldDevDomainDoc.CosURL, newDevDomainDoc.CosURL)
		return nil, err
	}

	err = GetCmdService().DocLogic.GetDao().CreateDoc(ctx, newDevDomainDoc, nil)
	if err != nil {
		logx.E(ctx, "writeNewDevDomainDocToDB CreateDoc err:%v,newDevDomainDoc.ID:%v",
			err, newDevDomainDoc.ID)
		return nil, err
	}
	return newDevDomainDoc, nil
}

// copyDocLabels 复制文档标签
func copyDocLabels(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc, esSegment *segEntity.EsSegment) error {
	// 将检索标签字符串转换成ReleaseAttrLabel数组
	releaseLabels := GetCmdService().LabelLogic.ParseLabelStrings(ctx, esSegment.Labels)
	// 将ReleaseAttrLabel数组转换成属性id和标签id
	attrLabels, err := GetCmdService().LabelLogic.TransRetrievalLabel2AttrIDAndLabelID(ctx, app.PrimaryId, releaseLabels)
	if err != nil {
		logx.E(ctx, "copyDocLabels TransRetrievalLabel2AttrIDAndLabelID err:%v", err)
		return err
	}

	// 保存标签关联关系到数据库
	docLabels := make([]*labelEntity.DocAttributeLabel, 0)
	for _, attrLabel := range attrLabels {
		docLabels = append(docLabels, &labelEntity.DocAttributeLabel{
			DocID:   newDevDomainDoc.ID,
			RobotID: newDevDomainDoc.RobotID,
			Source:  labelEntity.AttributeLabelSourceKg,
			AttrID:  attrLabel.AttrID,
			LabelID: attrLabel.ID,
		})
	}

	if len(docLabels) == 0 {
		return nil
	}
	updateColumns := []string{docEntity.DocTblColAttrRange}
	newDevDomainDoc.AttrRange = docEntity.AttrRangeCondition
	filter := &docEntity.DocFilter{ID: newDevDomainDoc.ID}
	rowsAffected, err := GetCmdService().DocLogic.GetDao().UpdateDoc(ctx, updateColumns, filter, newDevDomainDoc)
	if err != nil || rowsAffected == 0 {
		logx.E(ctx, "copyDocLabels UpdateDoc err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
			err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
		return err
	}

	err = GetCmdService().LabelDao.CreateDocAttributeLabel(ctx, docLabels, nil)
	if err != nil {
		logx.E(ctx, "copyDocLabels CreateDocAttributeLabel err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
			err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
		return err
	}

	return nil
}

// queryPublishDomainSegments 查询发布域的所有片段
func queryPublishDomainSegments(ctx context.Context, app *entity.App,
	oldDevDomainDoc *docEntity.Doc) ([]*entity.RetrievalNodeInfo, error) {
	segmentsInProdNodeInfo, err := GetCmdService().VectorLogic.GetDao().GetNodeIdsList(ctx, app.PrimaryId, []string{},
		&entity.RetrievalNodeFilter{
			APPID:   app.PrimaryId,
			DocType: entity.DocTypeSegment,
			DocID:   oldDevDomainDoc.ID,
		})
	if err != nil {
		logx.E(ctx, "queryPublishDomainSegments GetNodeIdsList err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
		return nil, err
	}

	return segmentsInProdNodeInfo, nil
}

// copyDocBigData 复制文档的big_data数据
func copyDocBigData(ctx context.Context, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc) (map[string]string, error) {
	bigDataList, err := GetCmdService().SegLogic.GetDao().QueryDocBigData(ctx, oldDevDomainDoc.RobotID, oldDevDomainDoc.ID, []string{}, 10000)
	if err != nil {
		logx.E(ctx, "copyDocBigData QueryDocBigData err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
		return nil, err
	}

	oldBigDataID2NewBigDataID := make(map[string]string)
	newBigDataList := make([]*retrieval.BigData, 0)
	for _, bigData := range bigDataList {
		// 通过bigData构造newBigData，并写入ES，保存旧big_data_biz_id到新big_data_biz_id的映射
		newBigDataID := cast.ToString(idgen.GetId())
		oldBigDataID2NewBigDataID[bigData.BigDataID] = newBigDataID
		newBigDataList = append(newBigDataList, &retrieval.BigData{
			RobotId:   newDevDomainDoc.RobotID,
			DocId:     newDevDomainDoc.ID,
			BigDataId: newBigDataID,
			BigStart:  bigData.BigStart,
			BigEnd:    bigData.BigEnd,
			BigString: bigData.BigString,
		})
	}

	// 批量写入big_data到ES
	if len(newBigDataList) > 0 {
		for _, bigDataChunks := range slicex.Chunk(newBigDataList, 200) {
			req := retrieval.AddBigDataElasticReq{Data: bigDataChunks, Type: retrieval.KnowledgeType_KNOWLEDGE}
			if err := GetCmdService().RpcImpl.RetrievalDirectIndex.AddBigDataElastic(ctx, &req); err != nil {
				logx.E(ctx, "copyDocBigData AddBigDataElastic err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
				return nil, err
			}
		}
	}

	return oldBigDataID2NewBigDataID, nil
}

// copyDocOrgData 复制文档的org_data数据
func copyDocOrgData(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc, segmentsInProdNodeInfo []*entity.RetrievalNodeInfo) (map[string]uint64, error) {
	oldOrgDataString2NewOrgDataBizID := make(map[string]uint64)

	for _, segmentInProdNodeInfo := range segmentsInProdNodeInfo {
		// 通过segmentInProdNodeInfo构造newOrgData，并保存到数据库，保存旧org_data_biz_id到新org_data_biz_id的映射
		if segmentInProdNodeInfo.OrgData == "" {
			continue
		}
		newOrgDataBizID := idgen.GetId()
		oldOrgDataString2NewOrgDataBizID[segmentInProdNodeInfo.OrgData] = newOrgDataBizID

		// 解析SheetData
		var sheetDatas []segEntity.SheetData
		sheetName := ""
		if segmentInProdNodeInfo.Reserve1 != "" {
			err := jsonx.Unmarshal([]byte(segmentInProdNodeInfo.Reserve1), &sheetDatas)
			if err != nil {
				logx.E(ctx, "copyDocOrgData Unmarshal SheetData err:%v", err)
			}
			if len(sheetDatas) > 0 {
				sheetName = sheetDatas[0].SheetName
			}
		}

		// 根据staff_id获取staff_biz_id
		staffBizID, err := getStaffBizIDByStaffID(ctx, oldDevDomainDoc.StaffID)
		if err != nil {
			logx.E(ctx, "copyDocOrgData getStaffBizIDByStaffID err:%v", err)
			return nil, err
		}

		orgData := &segEntity.DocSegmentOrgData{
			BusinessID:         newOrgDataBizID,
			AppBizID:           app.BizId,
			DocBizID:           newDevDomainDoc.BusinessID,
			CorpBizID:          app.CorpBizId,
			StaffBizID:         staffBizID,
			OrgData:            segmentInProdNodeInfo.OrgData,
			OrgPageNumbers:     segmentInProdNodeInfo.Reserve2,
			SheetData:          segmentInProdNodeInfo.Reserve1,
			SegmentType:        segmentInProdNodeInfo.SegmentType,
			AddMethod:          segEntity.AddMethodDefault,
			IsTemporaryDeleted: false,
			IsDeleted:          false,
			IsDisabled:         false,
			CreateTime:         segmentInProdNodeInfo.CreateTime,
			UpdateTime:         segmentInProdNodeInfo.UpdateTime,
			SheetName:          sheetName,
		}
		err = GetCmdService().SegLogic.CreateDocSegmentOrgData(ctx, orgData)
		if err != nil {
			logx.E(ctx, "copyDocOrgData CreateDocSegmentOrgData err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
			return nil, err
		}
	}

	return oldOrgDataString2NewOrgDataBizID, nil
}

// copyDocSegments 复制文档的segment数据
func copyDocSegments(ctx context.Context, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc, segmentsInProdNodeInfo []*entity.RetrievalNodeInfo,
	oldBigDataID2NewBigDataID map[string]string, oldOrgDataString2NewOrgDataBizID map[string]uint64) ([]*segEntity.DocSegmentExtend, map[uint64]uint64, error) {
	newSegments := make([]*segEntity.DocSegmentExtend, 0)
	newSegmentsBizID2oldSegmentID := make(map[uint64]uint64)
	for _, segmentInProdNodeInfo := range segmentsInProdNodeInfo {
		// 通过segmentInProdNodeInfo构造newSegment，并保存到数据库，保存旧org_segment_id到新org_segment_id的映射
		newSegmentBizID := idgen.GetId()
		newSegmentsBizID2oldSegmentID[newSegmentBizID] = segmentInProdNodeInfo.RelatedID
		// 获取新的big_data_id
		newBigDataID := ""
		if segmentInProdNodeInfo.BigDataID != "" {
			newID, ok := oldBigDataID2NewBigDataID[segmentInProdNodeInfo.BigDataID]
			if !ok {
				logx.E(ctx, "copyDocSegments oldBigDataID2NewBigDataID not found,oldDevDomainDoc.ID:%v", oldDevDomainDoc.ID)
				return nil, nil, errs.ErrWrapf(errs.ErrParams, "oldBigDataID2NewBigDataID not found")
			}
			newBigDataID = newID

		}

		// 获取新的org_data_biz_id
		newOrgDataBizID := uint64(0)
		if segmentInProdNodeInfo.OrgData != "" {
			newID, ok := oldOrgDataString2NewOrgDataBizID[segmentInProdNodeInfo.OrgData]
			if !ok {
				logx.E(ctx, "copyDocSegments oldOrgDataString2NewOrgDataBizID not found,oldDevDomainDoc.ID:%v", oldDevDomainDoc.ID)
				return nil, nil, errs.ErrWrapf(errs.ErrParams, "oldOrgDataString2NewOrgDataBizID not found")
			}
			newOrgDataBizID = newID
		}

		// 解析images、org_page_numbers、big_page_numbers、sheet_data
		var images []string
		if segmentInProdNodeInfo.Reserve3 != "" {
			err := jsonx.Unmarshal([]byte(segmentInProdNodeInfo.Reserve3), &images)
			if err != nil {
				logx.E(ctx, "copyDocSegments Unmarshal images err:%v", err)
			}
		}

		newSegment := &segEntity.DocSegmentExtend{
			DocSegment: segEntity.DocSegment{
				BusinessID:      newSegmentBizID,
				RobotID:         newDevDomainDoc.RobotID,
				CorpID:          newDevDomainDoc.CorpID,
				StaffID:         newDevDomainDoc.StaffID,
				DocID:           newDevDomainDoc.ID,
				FileType:        newDevDomainDoc.FileType,
				SegmentType:     segmentInProdNodeInfo.SegmentType,
				Title:           "",
				PageContent:     segmentInProdNodeInfo.PageContent,
				OrgData:         "",
				OrgDataBizID:    newOrgDataBizID,
				Outputs:         "",
				CostTime:        0,
				SplitModel:      "",
				Status:          segEntity.SegmentStatusDone,
				ReleaseStatus:   segEntity.SegmentReleaseStatusNotRequired,
				Message:         "",
				IsDeleted:       segEntity.SegmentIsNotDeleted,
				Type:            segEntity.SegmentTypeIndex,
				NextAction:      segEntity.SegNextActionAdd,
				RichTextIndex:   0,
				UpdateTime:      newDevDomainDoc.UpdateTime,
				StartChunkIndex: 0,
				EndChunkIndex:   0,
				LinkerKeep:      false,
				CreateTime:      newDevDomainDoc.CreateTime,
				BatchID:         newDevDomainDoc.BatchID,
				BigStart:        0,
				BigEnd:          0,
				BigString:       "",
				BigDataID:       newBigDataID,
				Images:          images,
				OrgPageNumbers:  segmentInProdNodeInfo.Reserve2,
				BigPageNumbers:  "",
				SheetData:       segmentInProdNodeInfo.Reserve1,
			},
			ExpireStart: newDevDomainDoc.ExpireStart,
			ExpireEnd:   newDevDomainDoc.ExpireEnd,
		}
		newSegments = append(newSegments, newSegment)
	}

	// 批量保存segment到数据库
	if len(newSegments) > 0 {
		docSegTableName := GetCmdService().SegLogic.GetDao().Query().TDocSegment.TableName()
		db, err := knowClient.GormClient(ctx, docSegTableName, newDevDomainDoc.RobotID, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "copyDocSegments GormClient err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
			return nil, nil, err
		}

		for _, segmentChunk := range slicex.Chunk(newSegments, 200) {
			segmentsToCreate := make([]*segEntity.DocSegment, 0)
			for _, seg := range segmentChunk {
				segmentsToCreate = append(segmentsToCreate, &seg.DocSegment)
			}
			err = GetCmdService().SegLogic.GetDao().CreateDocSegments(ctx, segmentsToCreate, db)
			if err != nil {
				logx.E(ctx, "copyDocSegments CreateDocSegments err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
				return nil, nil, err
			}
		}
	}
	oldSegmentID2NewSegmentID := make(map[uint64]uint64)
	for _, seg := range newSegments {
		oldSegmentID, ok := newSegmentsBizID2oldSegmentID[seg.BusinessID]
		if !ok {
			logx.E(ctx, "copyDocSegments newSegmentsBizID2oldSegmentID not found,oldDevDomainDoc.ID:%v", oldDevDomainDoc.ID)
			return nil, nil, errs.ErrWrapf(errs.ErrParams, "newSegmentsBizID2oldSegmentID not found")
		}
		oldSegmentID2NewSegmentID[oldSegmentID] = seg.ID
	}

	return newSegments, oldSegmentID2NewSegmentID, nil
}

// copyDocSegmentImages 复制文档的segment_image数据
func copyDocSegmentImages(ctx context.Context, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc, oldSegmentID2NewSegmentID map[uint64]uint64) error {
	oldDocSegmentImages, err := GetCmdService().SegLogic.GetDao().GetDocSegmentImageListWithTx(ctx, nil,
		&segEntity.DocSegmentImageFilter{
			DocID: oldDevDomainDoc.ID,
			AppID: oldDevDomainDoc.RobotID,
		}, nil)
	if err != nil {
		logx.E(ctx, "copyDocSegmentImages GetDocSegmentImageListWithTx err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
		return err
	}
	if len(oldDocSegmentImages) > 0 {
		newDocSegmentImages := make([]*segEntity.DocSegmentImage, 0)
		for _, oldImage := range oldDocSegmentImages {
			newSegmentID, ok := oldSegmentID2NewSegmentID[oldImage.SegmentID]
			if !ok {
				logx.E(ctx, "copyDocSegmentImages oldSegmentID2NewSegmentID not found,oldImage.SegmentID:%v", oldImage.SegmentID)
				continue
			}
			newDocSegmentImages = append(newDocSegmentImages, &segEntity.DocSegmentImage{
				ImageID:     idgen.GetId(),
				SegmentID:   newSegmentID,
				DocID:       newDevDomainDoc.ID,
				RobotID:     newDevDomainDoc.RobotID,
				CorpID:      newDevDomainDoc.CorpID,
				StaffID:     newDevDomainDoc.StaffID,
				OriginalUrl: oldImage.OriginalUrl,
				ExternalUrl: oldImage.ExternalUrl,
				IsDeleted:   segEntity.SegmentIsNotDeleted,
				CreateTime:  oldImage.CreateTime,
				UpdateTime:  oldImage.UpdateTime,
			})
		}
		if len(newDocSegmentImages) > 0 {
			docSegTableName := GetCmdService().SegLogic.GetDao().Query().TDocSegment.TableName()
			db, err := knowClient.GormClient(ctx, docSegTableName, newDevDomainDoc.RobotID, 0, []client.Option{}...)
			if err != nil {
				logx.E(ctx, "copyDocSegmentImages GormClient for images err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
				return err
			}
			for _, imageChunk := range slicex.Chunk(newDocSegmentImages, 200) {
				err = GetCmdService().SegLogic.GetDao().CreateDocSegmentImages(ctx, imageChunk, db)
				if err != nil {
					logx.E(ctx, "copyDocSegmentImages CreateDocSegmentImages err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
					return err
				}
			}
		}
	}

	return nil
}

// copyDocSegmentPageInfos 复制文档的segment_page_info数据
func copyDocSegmentPageInfos(ctx context.Context, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc, oldSegmentID2NewSegmentID map[uint64]uint64) error {
	oldDocSegmentPageInfos, err := GetCmdService().SegLogic.GetDao().GetDocSegmentPageInfoListWithTx(ctx, nil,
		&segEntity.DocSegmentPageInfoFilter{
			DocID: oldDevDomainDoc.ID,
			AppID: oldDevDomainDoc.RobotID,
		}, nil)
	if err != nil {
		logx.E(ctx, "copyDocSegmentPageInfos GetDocSegmentPageInfoListWithTx err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
		return err
	}
	if len(oldDocSegmentPageInfos) > 0 {
		newDocSegmentPageInfos := make([]*segEntity.DocSegmentPageInfo, 0)
		for _, oldPageInfo := range oldDocSegmentPageInfos {
			newSegmentID, ok := oldSegmentID2NewSegmentID[oldPageInfo.SegmentID]
			if !ok {
				logx.E(ctx, "copyDocSegmentPageInfos oldSegmentID2NewSegmentID not found,oldPageInfo.SegmentID:%v", oldPageInfo.SegmentID)
				continue
			}
			newDocSegmentPageInfos = append(newDocSegmentPageInfos, &segEntity.DocSegmentPageInfo{
				PageInfoID:     idgen.GetId(),
				SegmentID:      newSegmentID,
				DocID:          newDevDomainDoc.ID,
				RobotID:        newDevDomainDoc.RobotID,
				CorpID:         newDevDomainDoc.CorpID,
				StaffID:        newDevDomainDoc.StaffID,
				OrgPageNumbers: oldPageInfo.OrgPageNumbers,
				BigPageNumbers: oldPageInfo.BigPageNumbers,
				SheetData:      oldPageInfo.SheetData,
				IsDeleted:      segEntity.SegmentIsNotDeleted,
				CreateTime:     oldPageInfo.CreateTime,
				UpdateTime:     oldPageInfo.UpdateTime,
			})
		}
		if len(newDocSegmentPageInfos) > 0 {
			docSegTableName := GetCmdService().SegLogic.GetDao().Query().TDocSegment.TableName()
			db, err := knowClient.GormClient(ctx, docSegTableName, newDevDomainDoc.RobotID, 0, []client.Option{}...)
			if err != nil {
				logx.E(ctx, "copyDocSegmentPageInfos GormClient for pageInfos err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
				return err
			}
			for _, pageInfoChunk := range slicex.Chunk(newDocSegmentPageInfos, 200) {
				err = GetCmdService().SegLogic.GetDao().CreateDocSegmentPageInfos(ctx, pageInfoChunk, db)
				if err != nil {
					logx.E(ctx, "copyDocSegmentPageInfos CreateDocSegmentPageInfos err:%v,oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
					return err
				}
			}
		}
	}

	return nil
}

// addSegmentsToVectorDB 将片段写入向量库
func addSegmentsToVectorDB(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc, esSegment *segEntity.EsSegment, newSegments []*segEntity.DocSegmentExtend) error {
	// 新增enable_scope属性标签
	// 将检索标签字符串转换成ReleaseAttrLabel数组，再转换为VectorLabel
	releaseLabels := GetCmdService().LabelLogic.ParseLabelStrings(ctx, esSegment.Labels)
	vectorLabels := make([]*retrieval.VectorLabel, 0, len(releaseLabels)+1)
	for _, label := range releaseLabels {
		vectorLabel := &retrieval.VectorLabel{
			Name:  label.Name,
			Value: label.Value,
		}
		if label.Name == docEntity.SysLabelDocID {
			// 需要将DocID标签换成新文档的ID
			vectorLabel.Value = strconv.FormatUint(newDevDomainDoc.ID, 10)
		}
		vectorLabels = append(vectorLabels, vectorLabel)
	}
	vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
		Name:  entity.EnableScopeAttr,
		Value: entity.EnableScopeDb2Label[newDevDomainDoc.EnableScope],
	})
	logx.I(ctx, "addSegmentsToVectorDB vectorLabels:%+v", vectorLabels)
	// 获取向量库版本和模型
	embeddingVersion := app.Embedding.Version
	embeddingModel := ""
	embeddingModel, err := GetCmdService().VectorLogic.ExtractEmbeddingModelOfKB(ctx, app.CorpBizId, app)
	if err != nil {
		logx.E(ctx, "addSegmentsToVectorDB ExtractEmbeddingModelOfKB err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
			err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
		return err
	}
	if embeddingModel != "" {
		embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
	}
	logx.I(ctx, "addSegmentsToVectorDB embeddingVersion:%d,embeddingModel:%s", embeddingVersion, embeddingModel)

	// 过滤出需要写入向量库的segments
	validSegments := make([]*segEntity.DocSegmentExtend, 0)
	for _, newSegment := range newSegments {
		if newSegment.IsSegmentForQA() || newSegment.SegmentType == segEntity.SegmentTypeText2SQLMeta {
			continue
		}
		validSegments = append(validSegments, newSegment)
	}

	// 按100个分批调用addDocSegmentKnowledge
	for i, segmentChunk := range slicex.Chunk(validSegments, 60) {
		logx.I(ctx, "addSegmentsToVectorDB batch [%d], segment count:%d", i+1, len(segmentChunk))
		err := addDocSegmentKnowledge(ctx, app, segmentChunk, embeddingVersion, embeddingModel, vectorLabels)
		if err != nil {
			logx.E(ctx, "addSegmentsToVectorDB addDocSegmentKnowledge err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
				err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
			return err
		}

		sleepSwitch, sleepMillisecond := config.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
			config.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
		if sleepSwitch {
			time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
		}
	}

	return nil
}

// addDocSegmentKnowledge 添加文档片段到向量库（支持批量）
func addDocSegmentKnowledge(ctx context.Context, app *entity.App, segments []*segEntity.DocSegmentExtend, embeddingVersion uint64, embeddingModel string,
	vectorLabels []*retrieval.VectorLabel) error {
	if len(segments) == 0 {
		return nil
	}

	ctx = contextx.SetServerMetaData(ctx, contextx.MDSpaceID, app.SpaceId)

	// 构造批量知识数据
	knowledgeList := make([]*retrieval.KnowledgeData, 0, len(segments))
	for _, segment := range segments {
		knowledgeList = append(knowledgeList, &retrieval.KnowledgeData{
			Id:          segment.ID,
			DocId:       segment.DocID,
			PageContent: segment.PageContent,
			Labels:      vectorLabels,
			ExpireTime:  segment.GetExpireTime(),
		})
	}

	req := &retrieval.BatchAddKnowledgeReq{
		RobotId:            segments[0].RobotID,
		IndexId:            entity.SegmentReviewVersionID,
		DocType:            entity.DocTypeSegment,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		Knowledge:          knowledgeList,
		BotBizId:           app.BizId,
		IsVector:           false,
		Type:               retrieval.KnowledgeType_KNOWLEDGE,
	}
	rsp, err := GetCmdService().RpcImpl.RetrievalDirectIndex.BatchAddKnowledge(ctx, req)
	if err != nil {
		logx.E(ctx, "addDocSegmentKnowledge BatchAddKnowledge err:%v, segment count:%d", err, len(segments))
		return err
	}
	logx.I(ctx, "addDocSegmentKnowledge BatchAddKnowledge success, segment count:%d, rsp:%+v", len(segments), rsp)
	return nil
}

// createDocDevReleaseRelation 创建文档开发域和发布域的关联关系
func createDocDevReleaseRelation(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc) error {
	q := GetCmdService().DocLogic.GetDao().Query()
	err := q.TDevReleaseRelationInfo.WithContext(ctx).Create(&model.TDevReleaseRelationInfo{
		CorpID:             app.CorpPrimaryId,
		RobotID:            app.PrimaryId,
		Type:               releaseEntity.DevReleaseRelationTypeDocument,
		DevBusinessID:      oldDevDomainDoc.BusinessID,
		ReleaseBusinessID:  newDevDomainDoc.BusinessID,
		DiffTaskBusinessID: 0, // 文档对比任务会在后续步骤创建
	})
	if err != nil {
		logx.E(ctx, "createDocDevReleaseRelation create TDevReleaseRelationInfo err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
			err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
		return err
	}
	logx.I(ctx, "createDocDevReleaseRelation create TDevReleaseRelationInfo success, oldDevDomainDoc.BusinessID:%v, newDevDomainDoc.BusinessID:%v",
		oldDevDomainDoc.BusinessID, newDevDomainDoc.BusinessID)
	return nil
}

// copyDocParseRecords 复制文档的解析任务记录
func copyDocParseRecords(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc) error {
	// 查询旧文档的所有解析任务记录
	filter := &docEntity.DocParseFilter{
		RouterAppBizID: app.BizId,
		DocID:          oldDevDomainDoc.ID,
	}

	oldDocParseRecords, err := GetCmdService().DocLogic.GetDocParseList(ctx, []string{}, filter)
	if err != nil {
		logx.E(ctx, "copyDocParseRecords GetDocParseList err:%v, oldDevDomainDoc.ID:%v", err, oldDevDomainDoc.ID)
		return err
	}

	if len(oldDocParseRecords) == 0 {
		logx.I(ctx, "copyDocParseRecords no parse records found for oldDevDomainDoc.ID:%v", oldDevDomainDoc.ID)
		return nil
	}

	// 创建新的解析任务记录
	for _, oldDocParse := range oldDocParseRecords {
		newDocParse := &docEntity.DocParse{
			CorpID:       oldDocParse.CorpID,
			RobotID:      oldDocParse.RobotID,
			StaffID:      oldDocParse.StaffID,
			DocID:        newDevDomainDoc.ID, // 替换为新文档ID
			SourceEnvSet: oldDocParse.SourceEnvSet,
			RequestID:    oldDocParse.RequestID,
			TaskID:       oldDocParse.TaskID,
			Type:         oldDocParse.Type,
			OpType:       oldDocParse.OpType,
			Result:       oldDocParse.Result,
			Status:       oldDocParse.Status,
			CreateTime:   time.Now(),
			UpdateTime:   time.Now(),
		}

		err = GetCmdService().DocLogic.CreateDocParseTask(ctx, newDocParse)
		if err != nil {
			logx.E(ctx, "copyDocParseRecords CreateDocParseTask err:%v, oldDevDomainDoc.ID:%v, newDevDomainDoc.ID:%v",
				err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
			return err
		}
	}

	logx.I(ctx, "copyDocParseRecords success, oldDevDomainDoc.ID:%v, newDevDomainDoc.ID:%v, copied count:%d",
		oldDevDomainDoc.ID, newDevDomainDoc.ID, len(oldDocParseRecords))
	return nil
}

// createDocDiffTask 创建文档对比任务
func createDocDiffTask(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc) (*docEntity.DocDiff, error) {
	// 根据staff_id获取staff_biz_id
	staffBizID, err := getStaffBizIDByStaffID(ctx, oldDevDomainDoc.StaffID)
	if err != nil {
		logx.E(ctx, "createDocDiffTask getStaffBizIDByStaffID err:%v", err)
		return nil, err
	}

	docDiff := &docEntity.DocDiff{
		BusinessID:            idgen.GetId(),
		CorpBizID:             app.CorpBizId,
		RobotBizID:            app.BizId,
		StaffBizID:            staffBizID,
		NewDocBizID:           newDevDomainDoc.BusinessID,
		OldDocBizID:           oldDevDomainDoc.BusinessID,
		ComparisonReason:      docEntity.DocDiffTaskComparisonReasonManualDiff,
		DiffDataProcessStatus: docEntity.DiffDataProcessStatusInit,
		IsDeleted:             false,
	}
	err = GetCmdService().DocLogic.GetDao().CreateDocDiff(ctx, docDiff)
	if err != nil {
		logx.E(ctx, "createDocDiffTask CreateDocDiff err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
			err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
		return nil, err
	}
	logx.I(ctx, "createDocDiffTask CreateDocDiff success, docDiff.BusinessID:%v", docDiff.BusinessID)
	return docDiff, nil
}

// updateDocDevReleaseRelationDiffTaskID 更新t_dev_release_relation_info表中的DiffTaskBusinessID字段
func updateDocDevReleaseRelationDiffTaskID(ctx context.Context, app *entity.App, oldDevDomainDoc *docEntity.Doc,
	newDevDomainDoc *docEntity.Doc, docDiff *docEntity.DocDiff) error {
	q := GetCmdService().DocLogic.GetDao().Query()
	_, err := q.TDevReleaseRelationInfo.WithContext(ctx).
		Where(q.TDevReleaseRelationInfo.CorpID.Eq(app.CorpPrimaryId)).
		Where(q.TDevReleaseRelationInfo.RobotID.Eq(app.PrimaryId)).
		Where(q.TDevReleaseRelationInfo.Type.Eq(2)).
		Where(q.TDevReleaseRelationInfo.DevBusinessID.Eq(oldDevDomainDoc.BusinessID)).
		Where(q.TDevReleaseRelationInfo.ReleaseBusinessID.Eq(newDevDomainDoc.BusinessID)).
		Update(q.TDevReleaseRelationInfo.DiffTaskBusinessID, docDiff.BusinessID)
	if err != nil {
		logx.E(ctx, "updateDocDevReleaseRelationDiffTaskID update TDevReleaseRelationInfo err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v,docDiff.BusinessID:%v",
			err, oldDevDomainDoc.ID, newDevDomainDoc.ID, docDiff.BusinessID)
		return err
	}
	logx.I(ctx, "updateDocDevReleaseRelationDiffTaskID update TDevReleaseRelationInfo success, oldDevDomainDoc.BusinessID:%v, newDevDomainDoc.BusinessID:%v, docDiff.BusinessID:%v",
		oldDevDomainDoc.BusinessID, newDevDomainDoc.BusinessID, docDiff.BusinessID)
	return nil
}

// createDocDiffDataAsyncTask 创建文档比对详情异步任务
func createDocDiffDataAsyncTask(ctx context.Context, app *entity.App, docDiff *docEntity.DocDiff) error {
	docDiffParams := entity.DocDiffParams{
		CorpBizID:  app.CorpBizId,
		RobotBizID: app.BizId,
		DiffBizID:  docDiff.BusinessID,
	}
	err := scheduler.NewDocDiffDataTask(ctx, app.PrimaryId, docDiffParams)
	if err != nil {
		logx.E(ctx, "createDocDiffDataAsyncTask NewDocDiffDataTask err:%v,docDiff.BusinessID:%v",
			err, docDiff.BusinessID)
		return err
	}
	logx.I(ctx, "createDocDiffDataAsyncTask NewDocDiffDataTask success, docDiff.BusinessID:%v", docDiff.BusinessID)
	return nil
}

// updateAppUsedCharSize 更新应用已使用字符数
func updateAppUsedCharSize(ctx context.Context, oldDevDomainDoc *docEntity.Doc, newDevDomainDoc *docEntity.Doc) error {
	err := GetCmdService().RpcImpl.AppAdmin.UpdateAppUsedCharSize(ctx, int64(newDevDomainDoc.CharSize), newDevDomainDoc.RobotID)
	if err != nil {
		logx.E(ctx, "updateAppUsedCharSize UpdateAppUsedCharSize err:%v,oldDevDomainDoc.ID:%v,newDevDomainDoc.ID:%v",
			err, oldDevDomainDoc.ID, newDevDomainDoc.ID)
		return err
	}
	logx.I(ctx, "updateAppUsedCharSize UpdateAppUsedCharSize success, charSize:%v, robotID:%v",
		newDevDomainDoc.CharSize, newDevDomainDoc.RobotID)
	return nil
}

func RunCmdDocRevert(cmd *cobra.Command, args []string) error {
	return RunEnableScopeCommand(cmd, RevertAppDoc, EnableScopeParams{
		Uin:                  flagDocRevertUin,
		AppBizIDs:            flagDocRevertAppBizIDs,
		SpaceID:              flagDocRevertSpaceID,
		All:                  flagDocRevertAll,
		TypeName:             "Doc",
		SkipEmbeddingUpgrade: true, // Revert操作跳过embedding升级
	}, &AppWorkerConfig{})
}

// RevertAppDoc 回滚应用的文档数据
func RevertAppDoc(ctx context.Context, app *entity.App, config *AppWorkerConfig) error {
	logx.I(ctx, "reverting app: %s", jsonx.MustMarshal(app))

	// 1. 将该应用下所有文档的enable_scope字段置0
	filter := &docEntity.DocFilter{
		RobotId: app.PrimaryId,
	}
	err := GetCmdService().DocLogic.GetDao().BatchUpdateDocsByFilter(ctx, filter, map[string]any{"enable_scope": entity.EnableScopeInvalid}, nil)
	if err != nil {
		logx.E(ctx, "BatchUpdateDocsByFilter err:%+v, app_id:%d", err, app.PrimaryId)
		return err
	}
	logx.I(ctx, "BatchUpdateDocsByFilter success, app_id:%d", app.PrimaryId)

	// 2. 查询t_dev_release_relation_info表，获取从发布域复制到开发域的文档关联信息
	q := GetCmdService().DocLogic.GetDao().Query()
	relationInfoList, err := q.TDevReleaseRelationInfo.WithContext(ctx).
		Where(q.TDevReleaseRelationInfo.CorpID.Eq(app.CorpPrimaryId)).
		Where(q.TDevReleaseRelationInfo.RobotID.Eq(app.PrimaryId)).
		Where(q.TDevReleaseRelationInfo.Type.Eq(2)).Find() // 2表示文档类型
	if err != nil {
		logx.E(ctx, "query TDevReleaseRelationInfo err:%+v, app_id:%d", err, app.PrimaryId)
		return err
	}
	logx.I(ctx, "query TDevReleaseRelationInfo success, app_id:%d, count:%d", app.PrimaryId, len(relationInfoList))

	if len(relationInfoList) == 0 {
		logx.I(ctx, "no copied Docs found for app_id:%d", app.PrimaryId)
		return nil
	}

	// 3. 删除文档对比任务和对比详情数据（通过DiffTaskBusinessID）
	diffTaskBusinessIDs := make([]uint64, 0, len(relationInfoList))
	for _, relationInfo := range relationInfoList {
		if relationInfo.DiffTaskBusinessID > 0 {
			diffTaskBusinessIDs = append(diffTaskBusinessIDs, relationInfo.DiffTaskBusinessID)
		}
	}
	if len(diffTaskBusinessIDs) > 0 {
		err = GetCmdService().DocLogic.GetDao().DeleteDocDiffTasks(ctx, app.CorpBizId, app.BizId, diffTaskBusinessIDs)
		if err != nil {
			logx.E(ctx, "DeleteDocDiffTasks err:%+v, app_id:%d", err, app.PrimaryId)
			return err
		}
		logx.I(ctx, "DeleteDocDiffTasks success, app_id:%d, deleted count:%d", app.PrimaryId, len(diffTaskBusinessIDs))

		err = GetCmdService().DocLogic.GetDao().DeleteDocDiffData(ctx, app.CorpBizId, app.BizId, diffTaskBusinessIDs)
		if err != nil {
			logx.E(ctx, "DeleteDocDiffData err:%+v, app_id:%d", err, app.PrimaryId)
			return err
		}
		logx.I(ctx, "DeleteDocDiffData success, app_id:%d, deleted count:%d", app.PrimaryId, len(diffTaskBusinessIDs))
	}

	// 4. 根据ReleaseBusinessID查询需要删除的文档
	newDocBizIDs := make([]uint64, 0, len(relationInfoList))
	for _, relationInfo := range relationInfoList {
		newDocBizIDs = append(newDocBizIDs, relationInfo.ReleaseBusinessID)
	}

	// 分批查询文档
	docChunks := slicex.Chunk(newDocBizIDs, 200)
	for _, bizIDChunk := range docChunks {
		docs, err := GetCmdService().DocLogic.GetDao().GetDocList(ctx, nil, &docEntity.DocFilter{
			BusinessIds: bizIDChunk,
		})
		if err != nil {
			logx.E(ctx, "GetDocList err:%+v, app_id:%d", err, app.PrimaryId)
			return err
		}
		// 5. 调用DeleteDocs接口删除文档
		err = GetCmdService().DocLogic.DeleteDocs(ctx, app.CorpPrimaryId, app.PrimaryId, 0, docs)
		if err != nil {
			logx.E(ctx, "DeleteDocs err:%+v, app_id:%d", err, app.PrimaryId)
			return err
		}
		logx.I(ctx, "DeleteDocs success, app_id:%d, deleted count:%d", app.PrimaryId, len(docs))
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

	logx.I(ctx, "RevertAppDoc success, app_id:%d", app.PrimaryId)
	return nil
}
