package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/service"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/app/app_config"
	platformPb "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	// DefaultAppProcessedRedisKeyPrefix Redis key前缀默认值
	DefaultAppProcessedRedisKeyPrefix = "kb_config:v3311_app_enable_scope:processed:"

	// ProcessFunc 名称常量
	ProcessFuncNameDoc                = "ProcessAppDoc"
	ProcessFuncNameQa                 = "ProcessAppQa"
	ProcessFuncNameDb                 = "ProcessAppDb"
	ProcessFuncNameRevertDoc          = "RevertAppDoc"
	ProcessFuncNameRevertQa           = "RevertAppQa"
	ProcessFuncNameRevertDb           = "RevertAppDb"
	ProcessFuncNameClearProcessRecord = "ClearAppProcessedRecord"
)

var (
	cmdApp = &cobra.Command{
		Use:     "app",
		Short:   "App commands",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    func(cmd *cobra.Command, args []string) error { return cmd.Usage() },
	}
)

// 全局变量：日志文件目录
var logFileDir string

var (
	cmdAppList = &cobra.Command{
		Use:     "list",
		Short:   "List app with the given filters",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdAppList,
	}
)

var (
	cmdAppEnableScope = &cobra.Command{
		Use:     "enable-scope",
		Short:   "Process apps with enable_scope from config file",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdAppEnableScope,
	}
	flagAppEnableScopeConfigFile string
)

var (
	cmdAppCharUsage = &cobra.Command{
		Use:     "char-usage",
		Short:   "Get character usage and calculate doc/qa copy size",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdAppCharUsage,
	}
	flagAppCharUsageConfigFile string
)

var (
	cmdAppRevert = &cobra.Command{
		Use:     "revert",
		Short:   "Revert apps with enable_scope from config file",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdAppRevert,
	}
	flagAppRevertConfigFile string
)

// AppEnableScopeConfig 配置文件结构
type AppEnableScopeConfig struct {
	AllUin                    bool                    `yaml:"all_uin"` // 是否处理所有企业，为true时从t_corp表分批读取
	UinList                   []AppEnableScopeUinList `yaml:"uin_list"`
	BlackUinList              []string                `yaml:"black_uin_list"`                // 黑名单uin列表，这些uin会被过滤掉
	BlackAppIDList            []uint64                `yaml:"black_app_id_list"`             // 黑名单appID列表，这些app会被过滤掉
	UinWorkerCount            int                     `yaml:"uin_worker_count"`              // Uin级别的Worker并发数量，默认为4
	AppWorkerCount            int                     `yaml:"app_worker_count"`              // App级别的Worker并发数量，默认为4
	DocCopyWorkerCount        int                     `yaml:"doc_copy_worker_count"`         // Doc级别数据拷贝的Worker并发数量，默认为4
	QaWorkerCount             int                     `yaml:"qa_worker_count"`               // QA级别的Worker并发数量，默认为4
	DocUpdateLabelWorkerCount int                     `yaml:"doc_update_label_worker_count"` // Doc更新标签的Worker并发数量，默认为5
	RedisKeyPrefix            string                  `yaml:"redis_key_prefix"`              // Redis key前缀，默认为DefaultAppProcessedRedisKeyPrefix
	DisableProcessFunc        map[string]bool         `yaml:"disable_process_func"`          // 禁用的处理函数，key为函数名称，value为true表示禁用
}

// AppEnableScopeUinList 单个参数配置
type AppEnableScopeUinList struct {
	Uin       string   `yaml:"uin"`
	AppBizIDs []uint64 `yaml:"app_biz_ids"`
	SpaceID   string   `yaml:"space_id"`
	All       bool     `yaml:"all"`
}

func init() {
	cmdApp.AddCommand(cmdAppList)
	cmdApp.AddCommand(cmdAppEnableScope)
	cmdApp.AddCommand(cmdAppCharUsage)
	cmdApp.AddCommand(cmdAppRevert)

	flags := cmdAppEnableScope.PersistentFlags()
	flags.StringVar(&flagAppEnableScopeConfigFile, "config", "", "path to the YAML config file (required)")

	flags = cmdAppCharUsage.PersistentFlags()
	flags.StringVar(&flagAppCharUsageConfigFile, "config", "", "path to the YAML config file (required)")

	flags = cmdAppRevert.PersistentFlags()
	flags.StringVar(&flagAppRevertConfigFile, "config", "", "path to the YAML config file (required)")
}

func RunCmdAppList(cmd *cobra.Command, args []string) error {
	req := &pb.ListAppBaseInfoReq{}
	if len(CorpIDs) != 0 {
		req.CorpPrimaryId = cast.ToUint64(CorpIDs[0])
	}
	if len(SpaceIDs) != 0 {
		req.SpaceId = SpaceIDs[0]
	}
	if PageSize > 0 {
		req.PageSize = uint32(PageSize)
	}
	appList, total, err := GetCmdService().RpcImpl.ListAppBaseInfo(cmd.Context(), req)
	if err != nil {
		return err
	}

	tw := table.NewWriter()
	tw.SetOutputMirror(os.Stdout)
	tw.AppendHeader(table.Row{"corp_id", "space_id", "id", "biz_id", "name", "uin", "is_shared", "is_exp_center", "qa_version", "used_char_size"})
	for _, app := range appList {
		tw.AppendRow(table.Row{app.CorpPrimaryId, app.SpaceId, app.PrimaryId, app.BizId, app.Name, app.Uin, app.IsShared, app.IsExpCenter, app.QaVersion, app.UsedCharSize})
	}
	tw.AppendFooter(table.Row{total})
	tw.Render()
	return nil
}

func RunCmdAppEnableScope(cmd *cobra.Command, args []string) error {
	ctx := NewContext(cmd.Context())
	return runAppProcess(ctx, AppProcessConfig{
		ConfigFile:           flagAppEnableScopeConfigFile,
		NeedEmbeddingUpgrade: true,
		ProcessFuncs: []AppProcessFuncWithName{
			{Name: ProcessFuncNameDoc, Func: ProcessAppDoc},
			{Name: ProcessFuncNameQa, Func: ProcessAppQa},
			{Name: ProcessFuncNameDb, Func: ProcessAppDb},
		},
		IsRevert: false,
	})
}

func RunCmdAppCharUsage(cmd *cobra.Command, args []string) error {
	ctx := NewContext(cmd.Context())

	// 复用配置加载逻辑
	appConfig, err := loadAppProcessConfig(ctx, flagAppCharUsageConfigFile)
	if err != nil {
		return err
	}

	// 设置默认 worker 数量
	appConfig.UinWorkerCount = gox.IfElse(appConfig.UinWorkerCount > 0, appConfig.UinWorkerCount, defaultUinWorkerCount)
	appConfig.AppWorkerCount = gox.IfElse(appConfig.AppWorkerCount > 0, appConfig.AppWorkerCount, defaultAppWorkerCount)

	// 复用 uin 列表构建逻辑
	uinList, err := buildProcessUinList(ctx, appConfig)
	if err != nil {
		return err
	}

	logx.I(ctx, "loaded config with %d uins, uin_worker_count: %d, app_worker_count: %d", len(uinList), appConfig.UinWorkerCount, appConfig.AppWorkerCount)

	// 初始化结果文件并写入表头
	outputFile := "char-usage.result.txt"
	file, err := os.Create(outputFile)
	if err != nil {
		logx.E(ctx, "failed to create output file: %+v", err)
		return errs.ErrWrapf(errs.ErrParams, "failed to create output file: %+v", err)
	}
	defer file.Close()

	// 写入表头
	header := "UIN\t总字符容量\t已使用字符数\t超额字符数\t可用字符数\t需拷贝文档数\t文档拷贝字符数\t需拷贝QA数\tQA拷贝字符数\t需拷贝总数(文档+QA)\t总拷贝字符数(文档+QA)\t拷贝后已使用字符数\t拷贝后可用字符数\n"
	if _, err := file.WriteString(header); err != nil {
		logx.E(ctx, "failed to write header to file: %+v", err)
		return errs.ErrWrapf(errs.ErrParams, "failed to write header to file: %+v", err)
	}

	logx.I(ctx, "initialized result file: %s", outputFile)

	// 并发处理所有 uin，逐个写入结果
	err = processCharUsageUinsWithWorkerPool(ctx, uinList, appConfig, file)
	if err != nil {
		logx.E(ctx, "processCharUsageUinsWithWorkerPool failed: %+v", err)
		return err
	}

	logx.I(ctx, "all uins processed successfully, results written to file: %s", outputFile)
	return nil
}

// CharUsageResult 字符使用统计结果
type CharUsageResult struct {
	Uin                        string
	TotalCapacity              uint64
	UsedCharSize               uint64
	ExceedCharSize             uint64
	AvailableCharSize          int64
	TotalDocsNeedCopy          int
	TotalDocCopyCharSize       uint64
	TotalQAsNeedCopy           int
	TotalQACopyCharSize        uint64
	TotalItemsNeedCopy         int
	TotalCopyCharSize          uint64
	AfterCopyUsedCharSize      uint64
	AfterCopyAvailableCharSize int64
}

// CharUsageTask 字符使用统计任务
type CharUsageTask struct {
	UinConfig AppEnableScopeUinList
	UinIndex  int
	TotalUins int
}

// CharUsageErrorResult 字符使用统计错误结果
type CharUsageErrorResult struct {
	Uin string
	Err error
}

// CharUsageAppTask App字符使用统计任务
type CharUsageAppTask struct {
	App            *entity.App
	CorpBizID      uint64
	UinIndex       int
	TotalUins      int
	AppIndex       int
	TotalApps      int
	AppWorkerCount int
}

// processCharUsageUinsWithWorkerPool 使用 Worker Pool 并发处理 Uin 的字符使用统计
func processCharUsageUinsWithWorkerPool(ctx context.Context, uinList []AppEnableScopeUinList, appConfig *AppEnableScopeConfig, resultFile *os.File) error {
	totalUins := len(uinList)
	uinWorkerCount := appConfig.UinWorkerCount
	appWorkerCount := appConfig.AppWorkerCount

	logx.I(ctx, "starting char usage worker pool to process %d uins concurrently with %d uin workers, %d app workers", totalUins, uinWorkerCount, appWorkerCount)

	// 调整 worker 数量
	if totalUins < uinWorkerCount {
		uinWorkerCount = totalUins
	}

	taskChan := make(chan *CharUsageTask, totalUins)
	resultChan := make(chan CharUsageResult, totalUins)
	errResultChan := make(chan CharUsageErrorResult, totalUins)
	doneChan := make(chan struct{})

	// 创建互斥锁用于写入结果文件
	var fileMutex sync.Mutex

	// 使用 WaitGroup 等待所有 worker 完成
	var wg sync.WaitGroup

	// 启动 workers
	for i := 0; i < uinWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logx.I(ctx, "char usage worker %d: started and waiting for tasks", workerID)

			for task := range taskChan {
				logx.I(ctx, "char usage worker %d: processing uin [%d/%d], uin=%d",
					workerID, task.UinIndex, task.TotalUins, task.UinConfig.Uin)

				result, err := processCharUsageUin(ctx, task.UinConfig, task.UinIndex, task.TotalUins, appWorkerCount)
				if err != nil {
					logx.E(ctx, "char usage worker %d: failed to process uin [%d/%d], uin=%d, err: %+v",
						workerID, task.UinIndex, task.TotalUins, task.UinConfig.Uin, err)
					// 将错误信息发送到错误结果通道
					errResultChan <- CharUsageErrorResult{
						Uin: task.UinConfig.Uin,
						Err: err,
					}
					continue
				}

				// 将结果发送到结果通道
				resultChan <- result

				logx.I(ctx, "char usage worker %d: successfully processed uin [%d/%d], uin=%d",
					workerID, task.UinIndex, task.TotalUins, task.UinConfig.Uin)
			}

			logx.I(ctx, "char usage worker %d: no more tasks, exiting", workerID)
		}(i)
	}

	// 等待所有worker完成的goroutine
	go func() {
		wg.Wait()
		close(doneChan)
		close(errResultChan)
		close(resultChan)
	}()

	// 发送任务到通道
	for i, uinConfig := range uinList {
		taskChan <- &CharUsageTask{
			UinConfig: uinConfig,
			UinIndex:  i + 1,
			TotalUins: totalUins,
		}
	}
	close(taskChan)

	// 启动结果写入goroutine
	go func() {
		for result := range resultChan {
			// 将结果写入文件
			line := fmt.Sprintf("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
				result.Uin,
				result.TotalCapacity,
				result.UsedCharSize,
				result.ExceedCharSize,
				result.AvailableCharSize,
				result.TotalDocsNeedCopy,
				result.TotalDocCopyCharSize,
				result.TotalQAsNeedCopy,
				result.TotalQACopyCharSize,
				result.TotalItemsNeedCopy,
				result.TotalCopyCharSize,
				result.AfterCopyUsedCharSize,
				result.AfterCopyAvailableCharSize,
			)

			fileMutex.Lock()
			if _, writeErr := resultFile.WriteString(line); writeErr != nil {
				logx.E(ctx, "failed to write result to file for uin %d: %+v", result.Uin, writeErr)
			}
			fileMutex.Unlock()

			logx.I(ctx, "written result to file for uin %d", result.Uin)
		}
	}()

	// 启动错误结果写入goroutine
	go func() {
		for errResult := range errResultChan {
			// 将错误信息写入文件（所有字段填充为错误标识）
			line := fmt.Sprintf("%d\tERROR\tERROR\tERROR\tERROR\tERROR\tERROR\tERROR\tERROR\tERROR\tERROR\tERROR\tERROR\t%v\n",
				errResult.Uin,
				errResult.Err,
			)

			fileMutex.Lock()
			if _, writeErr := resultFile.WriteString(line); writeErr != nil {
				logx.E(ctx, "failed to write error result to file for uin %d: %+v", errResult.Uin, writeErr)
			}
			fileMutex.Unlock()

			logx.I(ctx, "written error result to file for uin %d", errResult.Uin)
		}
	}()

	// 等待完成
	<-doneChan

	logx.I(ctx, "processCharUsageUinsWithWorkerPool completed successfully")
	return nil
}

// charUsageAppResult App字符使用统计结果
type charUsageAppResult struct {
	appID           uint64
	appName         string
	docCount        int
	docCopyCharSize uint64
	qaCount         int
	qaCopyCharSize  uint64
	err             error
}

// processCharUsageAppTask 处理单个 App 的字符使用统计任务
func processCharUsageAppTask(ctx context.Context, task *CharUsageAppTask, workerID int,
	logMutex *sync.Mutex, countMutex *sync.Mutex, processedCount *int) charUsageAppResult {

	startTime := time.Now()
	totalUins := task.TotalUins
	totalApps := task.TotalApps
	uinIndex := task.UinIndex

	// 获取当前进度
	countMutex.Lock()
	currentCount := *processedCount + 1
	countMutex.Unlock()

	// 计算百分比
	uinPercent := float64(uinIndex) / float64(totalUins) * 100
	appPercent := float64(currentCount) / float64(totalApps) * 100

	logx.I(ctx, "char usage app worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] processing app %d (biz_id: %d)",
		workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent,
		task.App.PrimaryId, task.App.BizId)

	// 记录开始处理到日志文件
	logMutex.Lock()
	if writeErr := writeCharUsageProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "START", 0, nil); writeErr != nil {
		logx.E(ctx, "char usage app worker %d: failed to write process log: %+v", workerID, writeErr)
	}
	logMutex.Unlock()

	// 处理 Doc 和 QA
	task.App.CorpBizId = task.CorpBizID
	var docCount, qaCount int
	var docCopyCharSize, qaCopyCharSize uint64
	var processErr error

	// 串行处理 Doc 和 QA，避免下游组件压力过大
	// 先处理 Doc
	_, docsNeedCopy, _, _, err := GetDocProcessingData(ctx, task.App)
	if err != nil {
		logx.E(ctx, "char usage app worker %d: GetDocProcessingData app %d err: %+v",
			workerID, task.App.PrimaryId, err)
		processErr = err
	} else {
		docCount = len(docsNeedCopy)
		for _, doc := range docsNeedCopy {
			docCopyCharSize += doc.CharSize
		}
		logx.I(ctx, "char usage app worker %d: App[%d] %s: need copy %d docs, char size: %d",
			workerID, task.App.PrimaryId, task.App.Name, docCount, docCopyCharSize)
	}

	// 再处理 QA（只有 Doc 处理成功才继续）
	if processErr == nil {
		_, qasNeedCopy, _, _, err := GetQaProcessingData(ctx, task.App)
		if err != nil {
			logx.E(ctx, "char usage app worker %d: GetQaProcessingData app %d err: %+v",
				workerID, task.App.PrimaryId, err)
			processErr = err
		} else {
			qaCount = len(qasNeedCopy)
			for _, qa := range qasNeedCopy {
				qaCopyCharSize += qa.CharSize
			}
			logx.I(ctx, "char usage app worker %d: App[%d] %s: need copy %d QAs, char size: %d",
				workerID, task.App.PrimaryId, task.App.Name, qaCount, qaCopyCharSize)
		}
	}

	duration := time.Since(startTime)

	// 更新已处理计数
	countMutex.Lock()
	*processedCount++
	countMutex.Unlock()

	// 构建结果
	result := charUsageAppResult{
		appID:   task.App.PrimaryId,
		appName: task.App.Name,
		err:     processErr,
	}

	if processErr != nil {
		logx.E(ctx, "char usage app worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] failed to process app %d (biz_id: %d): %+v",
			workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent,
			task.App.PrimaryId, task.App.BizId, processErr)

		// 记录错误到日志文件
		logMutex.Lock()
		if writeErr := writeCharUsageProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "ERROR", duration, processErr); writeErr != nil {
			logx.E(ctx, "char usage app worker %d: failed to write error log: %+v", workerID, writeErr)
		}
		logMutex.Unlock()
	} else {
		logx.I(ctx, "char usage app worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] successfully processed app %d (biz_id: %d) in %v",
			workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent,
			task.App.PrimaryId, task.App.BizId, duration)

		// 记录成功处理到日志文件
		logMutex.Lock()
		if writeErr := writeCharUsageProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "SUCCESS", duration, nil); writeErr != nil {
			logx.E(ctx, "char usage app worker %d: failed to write process log: %+v", workerID, writeErr)
		}
		logMutex.Unlock()

		// 设置成功结果
		result.docCount = docCount
		result.docCopyCharSize = docCopyCharSize
		result.qaCount = qaCount
		result.qaCopyCharSize = qaCopyCharSize
	}

	return result
}

// processCharUsageAppsWithWorkerPool 使用 Worker Pool 并发处理 App 的字符使用统计
func processCharUsageAppsWithWorkerPool(ctx context.Context, apps []*entity.App, corpBizID uint64,
	uinIndex, totalUins, appWorkerCount int) (int, uint64, int, uint64, error) {

	totalApps := len(apps)
	if totalApps == 0 {
		return 0, 0, 0, 0, nil
	}

	logx.I(ctx, "uin %d/%d: starting app worker pool with %d workers to process %d apps",
		uinIndex, totalUins, appWorkerCount, totalApps)

	// 调整 worker 数量
	if totalApps < appWorkerCount {
		appWorkerCount = totalApps
	}

	// 创建任务通道和结果通道
	taskChan := make(chan *CharUsageAppTask, totalApps)
	resultChan := make(chan charUsageAppResult, totalApps)
	doneChan := make(chan struct{})

	// 使用 WaitGroup 等待所有 worker 完成
	var wg sync.WaitGroup
	var logMutex sync.Mutex
	var processedCount int
	var countMutex sync.Mutex

	// 启动 workers
	for i := 0; i < appWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logx.I(ctx, "char usage app worker %d: started and waiting for tasks", workerID)

			for task := range taskChan {
				// 调用抽象出的处理函数
				result := processCharUsageAppTask(ctx, task, workerID, &logMutex, &countMutex, &processedCount)
				resultChan <- result
			}

			logx.I(ctx, "char usage app worker %d: no more tasks, exiting", workerID)
		}(i)
	}

	// 等待所有worker完成的goroutine
	go func() {
		wg.Wait()
		close(doneChan)
		close(resultChan)
	}()

	// 发送任务到通道
	for i, app := range apps {
		taskChan <- &CharUsageAppTask{
			App:            app,
			CorpBizID:      corpBizID,
			UinIndex:       uinIndex,
			TotalUins:      totalUins,
			AppIndex:       i + 1,
			TotalApps:      totalApps,
			AppWorkerCount: appWorkerCount,
		}
	}
	close(taskChan)

	// 等待完成并收集结果
	<-doneChan

	// 收集所有结果
	var totalDocCount, totalQACount int
	var totalDocCopyCharSize, totalQACopyCharSize uint64
	var errorCount int

	for result := range resultChan {
		if result.err != nil {
			errorCount++
			logx.E(ctx, "uin %d/%d: app %d (%s) processing failed: %+v",
				uinIndex, totalUins, result.appID, result.appName, result.err)
			// 不返回错误，继续处理
			continue
		}
		totalDocCount += result.docCount
		totalDocCopyCharSize += result.docCopyCharSize
		totalQACount += result.qaCount
		totalQACopyCharSize += result.qaCopyCharSize
	}

	if errorCount > 0 {
		logx.W(ctx, "uin %d/%d: processCharUsageAppsWithWorkerPool completed with %d errors out of %d apps",
			uinIndex, totalUins, errorCount, totalApps)
	} else {
		logx.I(ctx, "uin %d/%d: processCharUsageAppsWithWorkerPool completed successfully, processed %d apps",
			uinIndex, totalUins, totalApps)
	}

	return totalDocCount, totalDocCopyCharSize, totalQACount, totalQACopyCharSize, nil
}

// processCharUsageUin 处理单个uin的字符使用统计
func processCharUsageUin(ctx context.Context, uinConfig AppEnableScopeUinList, uinIndex, totalUins, appWorkerCount int) (CharUsageResult, error) {
	logx.I(ctx, "processing uin %d/%d: uin=%d, all=%v, app_biz_ids=%v",
		uinIndex, totalUins, uinConfig.Uin, uinConfig.All, uinConfig.AppBizIDs)

	// 参数验证
	if err := validateCharUsageUin(uinConfig, uinIndex); err != nil {
		return CharUsageResult{}, err
	}

	// 获取企业信息
	corp, err := GetCmdService().RpcImpl.PlatformAdmin.DescribeCorp(ctx, &platformPb.DescribeCorpReq{
		Uin: uinConfig.Uin,
	})
	if err != nil {
		logx.E(ctx, "uin %s: DescribeCorp by uin err: %+v", uinConfig.Uin, err)
		return CharUsageResult{}, err
	}
	corpID := corp.GetCorpPrimaryId()
	corpBizID := corp.GetCorpId()

	// 获取字符使用情况
	total, used, exceed, err := service.GetCharacterUsageInternal(ctx, corpID,
		GetCmdService().RpcImpl, GetCmdService().DocLogic, GetCmdService().QaLogic)
	if err != nil {
		logx.E(ctx, "uin %d: GetCharacterUsageInternal err: %+v", uinConfig.Uin, err)
		return CharUsageResult{}, err
	}

	// 如果total为0，直接返回已知信息，不再查询应用列表和处理
	var totalDocCount, totalDocCopyCharSize, totalQACount, totalQACopyCharSize uint64
	if total == 0 {
		logx.W(ctx, "uin %d: total character capacity is 0, skip app processing", uinConfig.Uin)
	} else {
		// 获取应用列表
		apps, totalApps, err := getAppListByUin(ctx, uinIndex, uinConfig, corpID)
		if err != nil {
			return CharUsageResult{}, err
		}
		logx.I(ctx, "uin %d: got %d apps of corp (uin: %d)", uinIndex, totalApps, uinConfig.Uin)

		// 使用 Worker Pool 并发处理应用，计算拷贝统计信息
		var docCount, qaCount int
		docCount, totalDocCopyCharSize, qaCount, totalQACopyCharSize, err = processCharUsageAppsWithWorkerPool(
			ctx, apps, corpBizID, uinIndex, totalUins, appWorkerCount)
		if err != nil {
			logx.E(ctx, "uin %d: processCharUsageAppsWithWorkerPool err: %+v", uinConfig.Uin, err)
			// 即使有错误也不返回，继续处理
		}
		totalDocCount = uint64(docCount)
		totalQACount = uint64(qaCount)
	}

	// 构建结果
	result := buildCharUsageResult(uinConfig.Uin, total, used, exceed,
		totalDocCount, totalDocCopyCharSize, totalQACount, totalQACopyCharSize)

	// 输出汇总信息
	logCharUsageSummary(ctx, uinIndex, totalUins, uinConfig.Uin, total, used, exceed,
		int(totalDocCount), totalDocCopyCharSize, int(totalQACount), totalQACopyCharSize,
		result.TotalItemsNeedCopy, result.TotalCopyCharSize, result.AfterCopyUsedCharSize,
		result.AfterCopyAvailableCharSize)

	logx.I(ctx, "uin %d: completed successfully", uinConfig.Uin)
	return result, nil
}

// validateCharUsageUin 验证参数
func validateCharUsageUin(uinConfig AppEnableScopeUinList, uinIndex int) error {
	if uinConfig.Uin == "" {
		return errs.ErrWrapf(errs.ErrParams, "uin %s: uin is required", uinConfig.Uin)
	}

	// 检查参数组合逻辑
	if uinConfig.All {
		// 如果All为true，SpaceID和AppBizIDs都必须为空
		if uinConfig.SpaceID != "" || len(uinConfig.AppBizIDs) > 0 {
			return errs.ErrWrapf(errs.ErrParams, "uin %s: when all is true, space_id and app_biz_ids must be empty", uinConfig.Uin)
		}
	} else {
		// 如果All为false，SpaceID和AppBizIDs只能填一个
		if uinConfig.SpaceID != "" && len(uinConfig.AppBizIDs) > 0 {
			return errs.ErrWrapf(errs.ErrParams, "uin %s: space_id and app_biz_ids cannot be used together", uinConfig.Uin)
		}
		if uinConfig.SpaceID == "" && len(uinConfig.AppBizIDs) == 0 {
			return errs.ErrWrapf(errs.ErrParams, "uin %s: either space_id, app_biz_ids or all must be specified", uinConfig.Uin)
		}
	}

	return nil
}

// buildCharUsageResult 构建字符使用统计结果
func buildCharUsageResult(uin string, total, used, exceed uint32,
	totalDocCount uint64, totalDocCopyCharSize uint64,
	totalQACount uint64, totalQACopyCharSize uint64) CharUsageResult {

	totalCopyCharSize := totalDocCopyCharSize + totalQACopyCharSize
	totalCopyCount := int(totalDocCount + totalQACount)

	return CharUsageResult{
		Uin:                        uin,
		TotalCapacity:              uint64(total),
		UsedCharSize:               uint64(used),
		ExceedCharSize:             uint64(exceed),
		AvailableCharSize:          int64(total) - int64(used),
		TotalDocsNeedCopy:          int(totalDocCount),
		TotalDocCopyCharSize:       totalDocCopyCharSize,
		TotalQAsNeedCopy:           int(totalQACount),
		TotalQACopyCharSize:        totalQACopyCharSize,
		TotalItemsNeedCopy:         totalCopyCount,
		TotalCopyCharSize:          totalCopyCharSize,
		AfterCopyUsedCharSize:      uint64(used) + totalCopyCharSize,
		AfterCopyAvailableCharSize: int64(total) - int64(used) - int64(totalCopyCharSize),
	}
}

// logCharUsageSummary 输出字符使用汇总信息
func logCharUsageSummary(ctx context.Context, uinIndex, totalUins int, uin string,
	total, used, exceed uint32,
	totalDocCount int, totalDocCopyCharSize uint64,
	totalQACount int, totalQACopyCharSize uint64,
	totalCopyCount int, totalCopyCharSize, afterCopyUsedCharSize uint64,
	afterCopyAvailableCharSize int64) {

	logx.I(ctx, "\n========== Uin %d/%d: Character Usage Summary (uin: %s) ==========", uinIndex, totalUins, uin)
	logx.I(ctx, "Total Character Capacity: %d", total)
	logx.I(ctx, "Used Character Size: %d", used)
	logx.I(ctx, "Exceed Character Size: %d", exceed)
	logx.I(ctx, "Available Character Size: %d", int64(total)-int64(used))
	logx.I(ctx, "\n========== Document Copy Summary ==========")
	logx.I(ctx, "Total Docs Need Copy: %d", totalDocCount)
	logx.I(ctx, "Total Doc Copy Character Size: %d", totalDocCopyCharSize)
	logx.I(ctx, "\n========== QA Copy Summary ==========")
	logx.I(ctx, "Total QAs Need Copy: %d", totalQACount)
	logx.I(ctx, "Total QA Copy Character Size: %d", totalQACopyCharSize)
	logx.I(ctx, "\n========== Total Copy Summary ==========")
	logx.I(ctx, "Total Items Need Copy (Doc + QA): %d", totalCopyCount)
	logx.I(ctx, "Total Copy Character Size (Doc + QA): %d", totalCopyCharSize)
	logx.I(ctx, "After Copy Used Character Size: %d", afterCopyUsedCharSize)
	logx.I(ctx, "After Copy Available Character Size: %d", afterCopyAvailableCharSize)
	logx.I(ctx, "=============================================\n")
}

func RunCmdAppRevert(cmd *cobra.Command, args []string) error {
	ctx := NewContext(cmd.Context())
	return runAppProcess(ctx, AppProcessConfig{
		ConfigFile:           flagAppRevertConfigFile,
		NeedEmbeddingUpgrade: false,
		ProcessFuncs: []AppProcessFuncWithName{
			{Name: ProcessFuncNameRevertDoc, Func: RevertAppDoc},
			{Name: ProcessFuncNameRevertQa, Func: RevertAppQa},
			{Name: ProcessFuncNameRevertDb, Func: RevertAppDb},
			{Name: ProcessFuncNameClearProcessRecord, Func: ClearAppProcessedRecord}, // 清理Redis中的处理记录
		},
		IsRevert: true,
	})
}

// AppProcessFunc 应用处理函数类型
type AppProcessFunc func(ctx context.Context, app *entity.App, appWorkerConfig *AppWorkerConfig) error

// AppProcessFuncWithName 带名称的应用处理函数
type AppProcessFuncWithName struct {
	Name string         // 函数名称，用于配置中的开关控制
	Func AppProcessFunc // 实际的处理函数
}

// AppProcessConfig 应用处理配置
type AppProcessConfig struct {
	ConfigFile           string
	NeedEmbeddingUpgrade bool                     // 是否需要embedding升级
	ProcessFuncs         []AppProcessFuncWithName // 处理函数列表，按顺序执行
	IsRevert             bool                     // 是否是revert命令
}

// runAppProcess 通用的应用处理函数
func runAppProcess(ctx context.Context, config AppProcessConfig) error {
	// 加载和解析配置文件
	appConfig, err := loadAppProcessConfig(ctx, config.ConfigFile)
	if err != nil {
		return err
	}

	// 设置默认 worker 数量
	appConfig.UinWorkerCount = gox.IfElse(appConfig.UinWorkerCount > 0, appConfig.UinWorkerCount, defaultUinWorkerCount)
	appConfig.AppWorkerCount = gox.IfElse(appConfig.AppWorkerCount > 0, appConfig.AppWorkerCount, defaultAppWorkerCount)
	appConfig.DocCopyWorkerCount = gox.IfElse(appConfig.DocCopyWorkerCount > 0, appConfig.DocCopyWorkerCount, defaultDocCopyWorkerCount)
	appConfig.QaWorkerCount = gox.IfElse(appConfig.QaWorkerCount > 0, appConfig.QaWorkerCount, defaultQaWorkerCount)

	// 构建处理uin列表（支持 all_uin 模式）
	uinList, err := buildProcessUinList(ctx, appConfig)
	if err != nil {
		return err
	}

	logx.I(ctx, "loaded config with %d uins, uin_worker_count: %d, app_worker_count: %d, doc_worker_count: %d, qa_worker_count: %d", len(uinList), appConfig.UinWorkerCount, appConfig.AppWorkerCount, appConfig.DocCopyWorkerCount, appConfig.QaWorkerCount)

	// 创建日志目录
	logFileDir = time.Now().Format("20060102_150405")
	if err := os.MkdirAll(logFileDir, 0755); err != nil {
		logx.E(ctx, "failed to create log directory: %+v", err)
		return errs.ErrWrapf(errs.ErrParams, "failed to create log directory: %+v", err)
	}
	logx.I(ctx, "created log directory: %s", logFileDir)

	// 并行处理所有 uin
	if err := processUinsWithWorkerPool(ctx, uinList, config, appConfig); err != nil {
		logx.E(ctx, "processUinsWithWorkerPool failed: %+v", err)
		return err
	}

	logx.I(ctx, "all uins processed successfully")
	return nil
}

// AppTask 应用处理任务
type AppTask struct {
	App                       *entity.App
	CorpBizID                 uint64
	Config                    AppProcessConfig
	UinIndex                  int
	EmbeddingVersion          uint64
	StartTime                 time.Time       // 任务开始时间
	DocCopyWorkerCount        int             // Doc级别的Worker并发数量
	QaWorkerCount             int             // QA级别的Worker并发数量
	DocUpdateLabelWorkerCount int             // Doc更新标签的Worker并发数量
	RedisKeyPrefix            string          // Redis key前缀
	DisableProcessFunc        map[string]bool // 禁用的处理函数
}

// UinTask Uin处理任务
type UinTask struct {
	UinConfig AppEnableScopeUinList
	UinIndex  int
	TotalUins int
}

// processUinsWithWorkerPool 使用 Worker Pool 并发处理 Uin
func processUinsWithWorkerPool(ctx context.Context, uinList []AppEnableScopeUinList, config AppProcessConfig, appConfig *AppEnableScopeConfig) error {
	totalUins := len(uinList)
	logx.I(ctx, "starting uin worker pool to process %d uins concurrently", totalUins)

	// 从配置中获取 Uin 级别的并发数量
	uinWorkerCount := appConfig.UinWorkerCount
	if totalUins < uinWorkerCount {
		uinWorkerCount = totalUins
	}

	taskChan := make(chan *UinTask, totalUins)
	errChan := make(chan error, totalUins)
	doneChan := make(chan struct{})

	// 使用 WaitGroup 等待所有 worker 完成
	var wg sync.WaitGroup

	// 启动 uin workers
	for i := 0; i < uinWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logx.I(ctx, "uin worker %d: started and waiting for tasks", workerID)

			for task := range taskChan {
				logx.I(ctx, "uin worker %d: processing uin [%d/%d], uin=%d",
					workerID, task.UinIndex, task.TotalUins, task.UinConfig.Uin)

				if err := processUinTask(ctx, task, config, appConfig); err != nil {
					logx.E(ctx, "uin worker %d: failed to process uin [%d/%d], uin=%d, err: %+v",
						workerID, task.UinIndex, task.TotalUins, task.UinConfig.Uin, err)
					// 将错误发送到错误通道，但不立即返回，让其他 worker 继续处理
					select {
					case errChan <- err:
					default:
					}
					continue
				}

				logx.I(ctx, "uin worker %d: successfully processed uin [%d/%d], uin=%d",
					workerID, task.UinIndex, task.TotalUins, task.UinConfig.Uin)
			}

			logx.I(ctx, "uin worker %d: no more tasks, exiting", workerID)
		}(i)
	}

	// 等待所有worker完成的goroutine
	go func() {
		wg.Wait()
		close(doneChan)
		close(errChan)
	}()

	// 发送任务到通道
	for i, uinConfig := range uinList {
		taskChan <- &UinTask{
			UinConfig: uinConfig,
			UinIndex:  i + 1,
			TotalUins: totalUins,
		}
	}
	close(taskChan)

	// 等待完成并收集错误
	<-doneChan

	// 检查是否有错误
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		logx.E(ctx, "processUinsWithWorkerPool completed with %d errors", len(errors))
		// 返回第一个错误
		return errors[0]
	}

	logx.I(ctx, "processUinsWithWorkerPool completed successfully")
	return nil
}

// processUinTask 处理单个 Uin 任务
func processUinTask(ctx context.Context, task *UinTask, config AppProcessConfig, appConfig *AppEnableScopeConfig) error {
	uinConfig := task.UinConfig
	uinIndex := task.UinIndex
	totalUins := task.TotalUins

	logx.I(ctx, "processing uin %d/%d: uin=%d, all=%v, app_biz_ids=%v, space_id=%v",
		uinIndex, totalUins, uinConfig.Uin, uinConfig.All, uinConfig.AppBizIDs, uinConfig.SpaceID)

	// 参数验证
	if err := validateAppUin(ctx, uinIndex, uinConfig); err != nil {
		return err
	}

	// 通过uin获取企业信息
	corp, err := GetCmdService().RpcImpl.PlatformAdmin.DescribeCorp(ctx, &platformPb.DescribeCorpReq{
		Uin: uinConfig.Uin,
	})
	if err != nil {
		logx.E(ctx, "uin %s: DescribeCorp by uin err: %+v", uinConfig.Uin, err)
		return err
	}
	corpID := corp.GetCorpPrimaryId()

	// 获取字符使用情况
	total, _, _, err := service.GetCharacterUsageInternal(ctx, corpID,
		GetCmdService().RpcImpl, GetCmdService().DocLogic, GetCmdService().QaLogic)
	if err != nil {
		logx.E(ctx, "uin %d: GetCharacterUsageInternal err: %+v", uinConfig.Uin, err)
		return err
	}
	if total == 0 {
		logx.W(ctx, "uin %d: GetCharacterUsageInternal total is 0", uinConfig.Uin)
		return nil
	}

	// 获取应用列表
	apps, totalApps, err := getAppListByUin(ctx, uinIndex, uinConfig, corpID)
	if err != nil {
		return err
	}

	logx.I(ctx, "uin %d: got %d apps of corp (uin: %d) before filtering", uinIndex, totalApps, uinConfig.Uin)

	// 过滤黑名单appID
	if len(appConfig.BlackAppIDList) > 0 {
		// 构建黑名单map，提高查找效率
		blackAppIDMap := make(map[uint64]bool)
		for _, appID := range appConfig.BlackAppIDList {
			blackAppIDMap[appID] = true
		}

		// 过滤掉黑名单中的app
		var filteredApps []*entity.App
		for _, app := range apps {
			if !blackAppIDMap[app.PrimaryId] {
				filteredApps = append(filteredApps, app)
			} else {
				logx.I(ctx, "uin %d: filtered out blacklisted app: %d (biz_id: %d)", uinConfig.Uin, app.PrimaryId, app.BizId)
			}
		}

		logx.I(ctx, "uin %d: filtered %d blacklisted apps, remaining %d apps", uinConfig.Uin, len(apps)-len(filteredApps), len(filteredApps))
		apps = filteredApps
	}

	logx.I(ctx, "uin %d: final app list count: %d", uinConfig.Uin, len(apps))

	// 使用 Worker Pool 并发处理应用
	if err := processAppsWithWorkerPool(ctx, apps, corp.GetCorpId(), config, task, appConfig); err != nil {
		logx.E(ctx, "uin %d: processAppsWithWorkerPool failed: %+v", uinConfig.Uin, err)
		return err
	}

	logx.I(ctx, "uin %d: completed successfully", uinConfig.Uin)
	return nil
}

// processAppsWithWorkerPool 使用 Worker Pool 并发处理应用
func processAppsWithWorkerPool(ctx context.Context, apps []*entity.App, corpBizID uint64, config AppProcessConfig, uinTask *UinTask, appConfig *AppEnableScopeConfig) error {
	uinIndex := uinTask.UinIndex
	totalUins := uinTask.TotalUins
	uin := uinTask.UinConfig.Uin
	totalApps := len(apps)
	appWorkerCount := appConfig.AppWorkerCount
	docCopyWorkerCount := appConfig.DocCopyWorkerCount
	qaWorkerCount := appConfig.QaWorkerCount
	docUpdateLabelWorkerCount := appConfig.DocUpdateLabelWorkerCount

	logx.I(ctx, "uin %d/%d (uin: %d): starting worker pool with %d app workers to process %d apps", uinIndex, totalUins, uin, appWorkerCount, totalApps)

	// 创建任务通道
	taskChan := make(chan *AppTask, len(apps))

	// 使用 WaitGroup 等待所有 worker 完成
	var wg sync.WaitGroup

	// 创建互斥锁用于写入日志文件
	var logMutex sync.Mutex

	// 创建计数器用于跟踪已处理的app数量
	var processedCount int
	var countMutex sync.Mutex

	// 启动 workers
	for i := 0; i < appWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskChan {
				// 记录开始时间
				startTime := time.Now()
				task.StartTime = startTime

				// 获取当前进度
				countMutex.Lock()
				currentCount := processedCount + 1
				countMutex.Unlock()

				// 计算百分比
				uinPercent := float64(uinIndex) / float64(totalUins) * 100
				appPercent := float64(currentCount) / float64(totalApps) * 100

				logx.I(ctx, "worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] processing app %d (biz_id: %d)",
					workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent, task.App.PrimaryId, task.App.BizId)

				// 记录开始处理到日志文件
				logMutex.Lock()
				if writeErr := writeProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "START", 0, nil); writeErr != nil {
					logx.E(ctx, "worker %d: failed to write process log: %+v", workerID, writeErr)
				}
				logMutex.Unlock()

				if err := processAppTask(ctx, task); err != nil {
					// 计算耗时
					duration := time.Since(startTime)

					logx.E(ctx, "worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] failed to process app %d (biz_id: %d): %+v",
						workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent, task.App.PrimaryId, task.App.BizId, err)

					// 记录错误到日志文件
					logMutex.Lock()
					if writeErr := writeProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "ERROR", duration, err); writeErr != nil {
						logx.E(ctx, "worker %d: failed to write error log: %+v", workerID, writeErr)
					}
					logMutex.Unlock()

					// 更新已处理计数
					countMutex.Lock()
					processedCount++
					countMutex.Unlock()

					// 不返回错误，继续执行
					continue
				}

				// 计算耗时
				duration := time.Since(startTime)

				// 更新已处理计数
				countMutex.Lock()
				processedCount++
				countMutex.Unlock()

				logx.I(ctx, "worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] successfully processed app %d (biz_id: %d) in %v",
					workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent, task.App.PrimaryId, task.App.BizId, duration)

				// 记录成功处理到日志文件
				logMutex.Lock()
				if writeErr := writeProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "SUCCESS", duration, nil); writeErr != nil {
					logx.E(ctx, "worker %d: failed to write process log: %+v", workerID, writeErr)
				}
				logMutex.Unlock()
			}
		}(i)
	}

	// 发送任务到通道
	for _, app := range apps {
		app.CorpBizId = corpBizID
		taskChan <- &AppTask{
			App:                       app,
			CorpBizID:                 corpBizID,
			Config:                    config,
			UinIndex:                  uinIndex,
			DocCopyWorkerCount:        docCopyWorkerCount,
			QaWorkerCount:             qaWorkerCount,
			DocUpdateLabelWorkerCount: docUpdateLabelWorkerCount,
			RedisKeyPrefix:            appConfig.RedisKeyPrefix,
			DisableProcessFunc:        appConfig.DisableProcessFunc,
		}
	}
	close(taskChan)

	// 等待所有 worker 完成
	wg.Wait()

	return nil
}

// processAppTask 处理单个应用任务
func processAppTask(ctx context.Context, task *AppTask) error {
	app := task.App
	config := task.Config
	uinIndex := task.UinIndex

	appWorkerConfig := &AppWorkerConfig{
		DocCopyWorkerCount:        task.DocCopyWorkerCount,
		QaWorkerCount:             task.QaWorkerCount,
		DocUpdateLabelWorkerCount: task.DocUpdateLabelWorkerCount,
		RedisKeyPrefix:            task.RedisKeyPrefix,
	}

	logx.I(ctx, "uin %d: processing app: %s", uinIndex, jsonx.MustMarshal(app))

	// 1. 检查Redis中是否已有该app处理成功的记录（仅在非revert场景下检查）
	if !config.IsRevert {
		processed, err := checkAppProcessed(ctx, app.BizId, task.RedisKeyPrefix)
		if err != nil {
			logx.E(ctx, "uin %d: checkAppProcessed failed for app %d (biz_id: %d): %+v",
				uinIndex, app.PrimaryId, app.BizId, err)
			// Redis错误不影响主流程，继续处理
		} else if processed {
			logx.I(ctx, "uin %d: app %d (biz_id: %d) already processed, skip",
				uinIndex, app.PrimaryId, app.BizId)
			return nil
		}
	}

	// 如果需要embedding升级，则调用开始升级
	var embeddingVersion uint64
	if config.NeedEmbeddingUpgrade {
		embeddingVersion = app.Embedding.Version
		startReq := &pb.StartEmbeddingUpgradeAppReq{
			AppBizId:             app.BizId,
			FromEmbeddingVersion: embeddingVersion,
			ToEmbeddingVersion:   embeddingVersion,
		}
		_, err := GetCmdService().RpcImpl.AppAdmin.StartEmbeddingUpgradeApp(ctx, startReq)
		if err != nil {
			logx.E(ctx, "uin %d: StartEmbeddingUpgradeApp failed for app %d (biz_id: %d): %+v",
				uinIndex, app.PrimaryId, app.BizId, err)
			return fmt.Errorf("uin %d: StartEmbeddingUpgradeApp failed for app %d: %w", uinIndex, app.PrimaryId, err)
		}
		logx.I(ctx, "uin %d: StartEmbeddingUpgradeApp succeeded for app %d (biz_id: %d), embedding_version: %d",
			uinIndex, app.PrimaryId, app.BizId, embeddingVersion)
	}

	// 依次调用处理函数
	var processErr error
	for funcIdx, processFuncWithName := range config.ProcessFuncs {
		if processFuncWithName.Func == nil {
			continue
		}

		// 检查该函数是否被禁用
		if task.DisableProcessFunc != nil && task.DisableProcessFunc[processFuncWithName.Name] {
			logx.I(ctx, "uin %d: ProcessFunc[%d] %s is disabled, skip for app %d (biz_id: %d)",
				uinIndex, funcIdx, processFuncWithName.Name, app.PrimaryId, app.BizId)
			continue
		}

		if err := processFuncWithName.Func(ctx, app, appWorkerConfig); err != nil {
			logx.E(ctx, "uin %d: ProcessFunc[%d] %s failed for app %d (biz_id: %d): %+v",
				uinIndex, funcIdx, processFuncWithName.Name, app.PrimaryId, app.BizId, err)
			processErr = err
			break
		}
		logx.I(ctx, "uin %d: ProcessFunc[%d] %s succeeded for app %d (biz_id: %d)",
			uinIndex, funcIdx, processFuncWithName.Name, app.PrimaryId, app.BizId)
	}

	// 如果需要embedding升级，则调用结束升级（无论成功失败都要调用）
	if config.NeedEmbeddingUpgrade {
		finishReq := &pb.FinishEmbeddingUpgradeAppReq{
			AppBizId:             app.BizId,
			FromEmbeddingVersion: embeddingVersion,
			ToEmbeddingVersion:   embeddingVersion,
		}
		_, err := GetCmdService().RpcImpl.AppAdmin.FinishEmbeddingUpgradeApp(ctx, finishReq)
		if err != nil {
			logx.E(ctx, "uin %d: FinishEmbeddingUpgradeApp failed for app %d (biz_id: %d): %+v",
				uinIndex, app.PrimaryId, app.BizId, err)
			// 现网有脏数据会导致调用结果失败报错，这里只打印错误信息，不返回错误
			// return fmt.Errorf("uin %d: FinishEmbeddingUpgradeApp failed for app %d: %w", uinIndex, app.PrimaryId, err)
		} else {
			logx.I(ctx, "uin %d: FinishEmbeddingUpgradeApp succeeded for app %d (biz_id: %d)",
				uinIndex, app.PrimaryId, app.BizId)
		}
	}

	// 如果处理失败，返回错误
	if processErr != nil {
		return processErr
	}

	// 2. 处理成功后，标记该app已处理（仅在非revert场景下标记）
	if !config.IsRevert {
		if err := markAppProcessed(ctx, app.BizId, task.RedisKeyPrefix); err != nil {
			logx.E(ctx, "uin %d: markAppProcessed failed for app %d (biz_id: %d): %+v",
				uinIndex, app.PrimaryId, app.BizId, err)
			// Redis错误不影响主流程，只记录日志
		} else {
			logx.I(ctx, "uin %d: markAppProcessed succeeded for app %d (biz_id: %d)",
				uinIndex, app.PrimaryId, app.BizId)
		}
	}

	return nil
}

// loadAppProcessConfig 加载和解析配置文件
func loadAppProcessConfig(ctx context.Context, configFile string) (*AppEnableScopeConfig, error) {
	if configFile == "" {
		logx.E(ctx, "config file is required")
		return nil, errs.ErrWrapf(errs.ErrParams, "config file is required")
	}

	configData, err := os.ReadFile(configFile)
	if err != nil {
		logx.E(ctx, "failed to read config file: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrParams, "failed to read config file: %+v", err)
	}

	var appConfig AppEnableScopeConfig
	if err := yaml.Unmarshal(configData, &appConfig); err != nil {
		logx.E(ctx, "failed to parse config file: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrParams, "failed to parse config file: %+v", err)
	}

	// 设置默认值
	if appConfig.RedisKeyPrefix == "" {
		appConfig.RedisKeyPrefix = DefaultAppProcessedRedisKeyPrefix
	}

	return &appConfig, nil
}

// loadAllCorps 分批查询所有企业
func loadAllCorps(ctx context.Context) ([]AppEnableScopeUinList, error) {
	logx.I(ctx, "all_uin is true, querying all corps")

	var uinList []AppEnableScopeUinList
	pageSize := uint32(500)
	page := uint32(1)

	for {
		corpListReq := &platformPb.DescribeCorpListReq{
			Page:     page,
			PageSize: pageSize,
		}
		corpListRsp, err := GetCmdService().RpcImpl.PlatformAdmin.DescribeCorpList(ctx, corpListReq)
		if err != nil {
			logx.E(ctx, "DescribeCorpList err: %+v, page: %d", err, page)
			return nil, err
		}

		if corpListRsp == nil || len(corpListRsp.GetList()) == 0 {
			break
		}

		logx.I(ctx, "DescribeCorpList success, page: %d, corp_count: %d, total: %d",
			page, len(corpListRsp.GetList()), corpListRsp.GetTotal())

		// 将每个企业转换为uin配置
		for _, corp := range corpListRsp.GetList() {
			uinList = append(uinList, AppEnableScopeUinList{
				Uin: corp.GetUin(),
				All: true, // 处理该企业下所有应用
			})
		}

		// 如果查询结果少于pageSize，说明已经查完了
		if len(corpListRsp.GetList()) < int(pageSize) {
			break
		}

		page++
	}

	logx.I(ctx, "loaded %d corps from all_uin", len(uinList))
	return uinList, nil
}

// buildProcessUinList 构建处理uin列表
func buildProcessUinList(ctx context.Context, appConfig *AppEnableScopeConfig) ([]AppEnableScopeUinList, error) {
	var uinList []AppEnableScopeUinList

	if appConfig.AllUin {
		// 如果 all_uin 为 true，uin_list 必须为空
		if len(appConfig.UinList) > 0 {
			logx.E(ctx, "when all_uin is true, uin_list must be empty")
			return nil, errs.ErrWrapf(errs.ErrParams, "when all_uin is true, uin_list must be empty")
		}
		logx.I(ctx, "all_uin mode enabled, will process all corps from t_corp table")
		allCorps, err := loadAllCorps(ctx)
		if err != nil {
			return nil, err
		}
		uinList = allCorps
	} else {
		// 使用配置文件中的uin
		if len(appConfig.UinList) == 0 {
			logx.E(ctx, "no uins found in config file")
			return nil, errs.ErrWrapf(errs.ErrParams, "no uins found in config file")
		}
		uinList = appConfig.UinList
	}

	logx.I(ctx, "loaded config with %d uins before filtering", len(uinList))

	// 过滤黑名单uin
	if len(appConfig.BlackUinList) > 0 {
		// 构建黑名单map，提高查找效率
		blackUinMap := make(map[string]bool)
		for _, uin := range appConfig.BlackUinList {
			blackUinMap[uin] = true
		}

		// 过滤掉黑名单中的uin
		var filteredUinList []AppEnableScopeUinList
		for _, uinConfig := range uinList {
			if !blackUinMap[uinConfig.Uin] {
				filteredUinList = append(filteredUinList, uinConfig)
			} else {
				logx.I(ctx, "filtered out blacklisted uin: %s", uinConfig.Uin)
			}
		}

		logx.I(ctx, "filtered %d blacklisted uins, remaining %d uins", len(uinList)-len(filteredUinList), len(filteredUinList))
		uinList = filteredUinList
	}

	logx.I(ctx, "final uin list count: %d", len(uinList))
	return uinList, nil
}

// validateAppUin 验证参数
func validateAppUin(ctx context.Context, uinIndex int, uinConfig AppEnableScopeUinList) error {
	if uinConfig.Uin == "" {
		logx.E(ctx, "uin %d: uin is required", uinIndex)
		return errs.ErrWrapf(errs.ErrParams, "uin %d: uin is required", uinIndex)
	}

	// 检查参数组合逻辑
	if uinConfig.All {
		// 如果All为true，SpaceID和AppBizIDs都必须为空
		if uinConfig.SpaceID != "" || len(uinConfig.AppBizIDs) > 0 {
			logx.E(ctx, "uin %d: when all is true, space_id and app_biz_ids must be empty", uinIndex)
			return errs.ErrWrapf(errs.ErrParams, "uin %d: when all is true, space_id and app_biz_ids must be empty", uinIndex)
		}
	} else {
		// 如果All为false，SpaceID和AppBizIDs只能填一个
		if uinConfig.SpaceID != "" && len(uinConfig.AppBizIDs) > 0 {
			logx.E(ctx, "uin %d: space_id and app_biz_ids cannot be used together", uinIndex)
			return errs.ErrWrapf(errs.ErrParams, "uin %d: space_id and app_biz_ids cannot be used together", uinIndex)
		}
		if uinConfig.SpaceID == "" && len(uinConfig.AppBizIDs) == 0 {
			logx.E(ctx, "uin %d: either space_id, app_biz_ids or all must be specified", uinIndex)
			return errs.ErrWrapf(errs.ErrParams, "uin %d: either space_id, app_biz_ids or all must be specified", uinIndex)
		}
	}

	return nil
}

// getAppListByUin 根据uin配置获取应用列表（支持分页查询所有数据）
func getAppListByUin(ctx context.Context, uinIndex int, uinConfig AppEnableScopeUinList, corpID uint64) ([]*entity.App, uint64, error) {
	req := &pb.GetAppListReq{
		CorpPrimaryId: corpID,
		DisablePrompt: true,
	}

	// 根据uin配置设置查询条件
	if uinConfig.SpaceID != "" {
		req.SpaceId = uinConfig.SpaceID
	} else if len(uinConfig.AppBizIDs) > 0 {
		req.BotBizIds = uinConfig.AppBizIDs
	}

	// 分页查询所有应用
	var allApps []*entity.App
	var totalApps uint64
	pageSize := uint32(200) // 每页200条
	page := uint32(1)

	for {
		req.Page = page
		req.PageSize = pageSize

		apps, total, err := GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "uin %d: DescribeAppList err: %+v, page: %d", uinIndex, err, page)
			return nil, 0, err
		}

		// 记录总数（第一次查询时）
		if page == 1 {
			totalApps = total
			logx.I(ctx, "uin %d: total apps count: %d", uinIndex, totalApps)
		}

		// 追加到结果集
		allApps = append(allApps, apps...)
		logx.I(ctx, "uin %d: fetched page %d, got %d apps, accumulated %d/%d apps",
			uinIndex, page, len(apps), len(allApps), totalApps)

		// 如果查询结果少于pageSize，说明已经查完了
		if len(apps) < int(pageSize) {
			break
		}

		// 如果已经获取了所有数据，退出循环
		if uint64(len(allApps)) >= totalApps {
			break
		}

		page++
	}

	logx.I(ctx, "uin %d: got %d apps of corp (uin: %d)", uinIndex, len(allApps), uinConfig.Uin)
	return allApps, totalApps, nil
}

// generateAppProcessedRedisKey 生成app处理成功的Redis key
func generateAppProcessedRedisKey(appBizID uint64, redisKeyPrefix string) string {
	if redisKeyPrefix == "" {
		redisKeyPrefix = DefaultAppProcessedRedisKeyPrefix
	}
	return fmt.Sprintf("%s%d", redisKeyPrefix, appBizID)
}

// checkAppProcessed 检查app是否已经处理成功
func checkAppProcessed(ctx context.Context, appBizID uint64, redisKeyPrefix string) (bool, error) {
	key := generateAppProcessedRedisKey(appBizID, redisKeyPrefix)
	err := GetCmdService().AdminRdb.Get(ctx, key).Err()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// key不存在，说明未处理过
			return false, nil
		}
		// 其他错误
		return false, fmt.Errorf("redis get failed: %w", err)
	}
	// key存在，说明已处理过
	return true, nil
}

// markAppProcessed 标记app处理成功
func markAppProcessed(ctx context.Context, appBizID uint64, redisKeyPrefix string) error {
	key := generateAppProcessedRedisKey(appBizID, redisKeyPrefix)
	// 设置有效期为1个月（30天）
	expireTime := 30 * 24 * time.Hour
	value := time.Now().Format("2006-01-02 15:04:05")
	err := GetCmdService().AdminRdb.Set(ctx, key, value, expireTime).Err()
	if err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}
	return nil
}

// clearAppProcessed 清理app处理成功的记录
func clearAppProcessed(ctx context.Context, appBizID uint64, redisKeyPrefix string) error {
	key := generateAppProcessedRedisKey(appBizID, redisKeyPrefix)
	err := GetCmdService().AdminRdb.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("redis del failed: %w", err)
	}
	return nil
}

// ClearAppProcessedRecord 清理app处理成功的记录（用于revert流程）
func ClearAppProcessedRecord(ctx context.Context, app *entity.App, appWorkerConfig *AppWorkerConfig) error {
	logx.I(ctx, "clearing processed record for app %d (biz_id: %d)", app.PrimaryId, app.BizId)

	if err := clearAppProcessed(ctx, app.BizId, appWorkerConfig.RedisKeyPrefix); err != nil {
		logx.E(ctx, "clearAppProcessed failed for app %d (biz_id: %d): %+v",
			app.PrimaryId, app.BizId, err)
		// Redis错误不影响主流程，只记录日志
		return nil
	}

	logx.I(ctx, "successfully cleared processed record for app %d (biz_id: %d)",
		app.PrimaryId, app.BizId)
	return nil
}

// writeProcessLog 将处理信息写入日志文件
// status: START, SUCCESS, ERROR
func writeProcessLog(ctx context.Context, app *entity.App, uinIndex, totalUins, currentCount, totalApps int, status string, duration time.Duration, err error) error {
	if logFileDir == "" {
		return fmt.Errorf("log file directory not initialized")
	}

	logFilePath := fmt.Sprintf("%s/app_processing.log", logFileDir)
	file, openErr := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to open log file: %w", openErr)
	}
	defer file.Close()

	// 计算百分比
	uinPercent := float64(uinIndex) / float64(totalUins) * 100
	appPercent := float64(currentCount) / float64(totalApps) * 100

	// 格式: 时间\tstatus\tuin\tuin进度\tuin百分比\tappID\tappBizID\tapp进度\tapp百分比\t耗时\t错误信息(如有)
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	uinProgress := fmt.Sprintf("%d/%d", uinIndex, totalUins)
	appProgress := fmt.Sprintf("%d/%d", currentCount, totalApps)

	// 格式化耗时
	var durationStr string
	if duration > 0 {
		durationStr = duration.String()
	} else {
		durationStr = "-"
	}

	var logLine string
	if err != nil {
		logLine = fmt.Sprintf("%s\t%s\t%s\t%s\t%.2f%%\t%d\t%d\t%s\t%.2f%%\t%s\t%v\n",
			timestamp, status, app.Uin, uinProgress, uinPercent, app.PrimaryId, app.BizId, appProgress, appPercent, durationStr, err)
	} else {
		logLine = fmt.Sprintf("%s\t%s\t%s\t%s\t%.2f%%\t%d\t%d\t%s\t%.2f%%\t%s\t\n",
			timestamp, status, app.Uin, uinProgress, uinPercent, app.PrimaryId, app.BizId, appProgress, appPercent, durationStr)
	}

	if _, writeErr := file.WriteString(logLine); writeErr != nil {
		return fmt.Errorf("failed to write to log file: %w", writeErr)
	}

	return nil
}

// writeCharUsageProcessLog 将字符使用统计处理信息写入日志文件
// status: START, SUCCESS, ERROR
func writeCharUsageProcessLog(ctx context.Context, app *entity.App, uinIndex, totalUins, currentCount, totalApps int, status string, duration time.Duration, err error) error {
	// 使用固定的日志文件名
	logFilePath := "char-usage-processing.log"
	file, openErr := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to open log file: %w", openErr)
	}
	defer file.Close()

	// 计算百分比
	uinPercent := float64(uinIndex) / float64(totalUins) * 100
	appPercent := float64(currentCount) / float64(totalApps) * 100

	// 格式: 时间\tstatus\tuin\tuin进度\tuin百分比\tappID\tappBizID\tapp进度\tapp百分比\t耗时\t错误信息(如有)
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	uinProgress := fmt.Sprintf("%d/%d", uinIndex, totalUins)
	appProgress := fmt.Sprintf("%d/%d", currentCount, totalApps)

	// 格式化耗时
	var durationStr string
	if duration > 0 {
		durationStr = duration.String()
	} else {
		durationStr = "-"
	}

	var logLine string
	if err != nil {
		logLine = fmt.Sprintf("%s\t%s\t%s\t%s\t%.2f%%\t%d\t%d\t%s\t%.2f%%\t%s\t%v\n",
			timestamp, status, app.Uin, uinProgress, uinPercent, app.PrimaryId, app.BizId, appProgress, appPercent, durationStr, err)
	} else {
		logLine = fmt.Sprintf("%s\t%s\t%s\t%s\t%.2f%%\t%d\t%d\t%s\t%.2f%%\t%s\t\n",
			timestamp, status, app.Uin, uinProgress, uinPercent, app.PrimaryId, app.BizId, appProgress, appPercent, durationStr)
	}

	if _, writeErr := file.WriteString(logLine); writeErr != nil {
		return fmt.Errorf("failed to write to log file: %w", writeErr)
	}

	return nil
}
