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
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
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
	DefaultAppProcessedRedisKeyPrefix = "kb_config:v3321_app_update_used_capacity:processed:"
)

// 全局变量：当前运行的page size配置
var currentPageSize int

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
	cmdAppUpdateUsedCapacity = &cobra.Command{
		Use:     "update-used-capacity",
		Short:   "Update used capacity for apps from config file",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdAppUpdateUsedCapacity,
	}
	flagAppUpdateUsedCapacityConfigFile string
)

// AppUpdateUsedCapacityConfig 配置文件结构
type AppUpdateUsedCapacityConfig struct {
	AllUin          bool                           `yaml:"all_uin"`           // 是否处理所有uin
	UinList         []AppUpdateUsedCapacityUinList `yaml:"uin_list"`          // uin列表
	UinWorkerCount  int                            `yaml:"uin_worker_count"`  // Uin级别的Worker并发数量，默认为4
	AppWorkerCount  int                            `yaml:"app_worker_count"`  // App级别的Worker并发数量，默认为4
	RedisKeyPrefix  string                         `yaml:"redis_key_prefix"`  // Redis key前缀，默认为DefaultAppProcessedRedisKeyPrefix
	DefaultPageSize int                            `yaml:"default_page_size"` // 默认分页大小，默认为200
}

// AppUpdateUsedCapacityUinList 单个参数配置
type AppUpdateUsedCapacityUinList struct {
	Uin       uint64   `yaml:"uin"`
	AppBizIDs []uint64 `yaml:"app_biz_ids"` // 指定的应用ID列表
	SpaceID   string   `yaml:"space_id"`    // 指定的空间ID
	All       bool     `yaml:"all"`         // 是否处理该uin下所有应用
}

func init() {
	cmdApp.AddCommand(cmdAppList)
	cmdApp.AddCommand(cmdAppUpdateUsedCapacity)

	flags := cmdAppUpdateUsedCapacity.PersistentFlags()
	flags.StringVar(&flagAppUpdateUsedCapacityConfigFile, "config", "", "path to the YAML config file (required)")
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

func RunCmdAppUpdateUsedCapacity(cmd *cobra.Command, args []string) error {
	return runAppProcess(cmd.Context(), AppProcessConfig{
		ConfigFile:           flagAppUpdateUsedCapacityConfigFile,
		NeedEmbeddingUpgrade: false,
		ProcessFuncs: []AppProcessFunc{
			ProcessAppQa,
			ProcessApp,
		},
	})
}

// AppProcessFunc 应用处理函数类型
type AppProcessFunc func(ctx context.Context, app *entity.AppBaseInfo) error

// AppProcessConfig 应用处理配置
type AppProcessConfig struct {
	ConfigFile           string
	NeedEmbeddingUpgrade bool             // 是否需要embedding升级
	ProcessFuncs         []AppProcessFunc // 处理函数列表，按顺序执行
}

// runAppProcess 通用的应用处理函数
func runAppProcess(ctx context.Context, config AppProcessConfig) error {
	// 1. 加载配置
	appConfig, err := loadAppProcessConfig(ctx, config.ConfigFile)
	if err != nil {
		return err
	}

	// 设置默认 worker 数量
	appConfig.UinWorkerCount = gox.IfElse(appConfig.UinWorkerCount > 0, appConfig.UinWorkerCount, defaultUinWorkerCount)
	appConfig.AppWorkerCount = gox.IfElse(appConfig.AppWorkerCount > 0, appConfig.AppWorkerCount, defaultAppWorkerCount)
	// 设置默认 page size
	if appConfig.DefaultPageSize == 0 {
		appConfig.DefaultPageSize = DefaultPageSize
	}
	// 将配置的page size设置到全局变量
	currentPageSize = appConfig.DefaultPageSize

	// 2. 构建处理参数列表
	params, err := buildProcessParams(ctx, appConfig)
	if err != nil {
		return err
	}

	// 设置默认 redis key prefix
	if appConfig.RedisKeyPrefix == "" {
		appConfig.RedisKeyPrefix = DefaultAppProcessedRedisKeyPrefix
	}

	logx.I(ctx, "loaded config with %d uins, uin_worker_count: %d, app_worker_count: %d, redis_key_prefix: %s, default_page_size: %d",
		len(params), appConfig.UinWorkerCount, appConfig.AppWorkerCount, appConfig.RedisKeyPrefix, currentPageSize)

	// 创建日志目录
	logFileDir = time.Now().Format("20060102_150405")
	if err := os.MkdirAll(logFileDir, 0755); err != nil {
		logx.E(ctx, "failed to create log directory: %+v", err)
		return errs.ErrWrapf(errs.ErrParams, "failed to create log directory: %+v", err)
	}
	logx.I(ctx, "created log directory: %s", logFileDir)

	// 3. 使用 Worker Pool 并发处理所有 uin
	if err := processUinsWithWorkerPool(ctx, params, config, appConfig); err != nil {
		logx.E(ctx, "processUinsWithWorkerPool failed: %+v", err)
		return err
	}

	logx.I(ctx, "all params processed successfully")
	return nil
}

// loadAppProcessConfig 加载和解析配置文件
func loadAppProcessConfig(ctx context.Context, configFile string) (*AppUpdateUsedCapacityConfig, error) {
	if configFile == "" {
		logx.E(ctx, "config file is required")
		return nil, errs.ErrWrapf(errs.ErrParams, "config file is required")
	}

	configData, err := os.ReadFile(configFile)
	if err != nil {
		logx.E(ctx, "failed to read config file: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrParams, "failed to read config file: %+v", err)
	}

	var appConfig AppUpdateUsedCapacityConfig
	if err := yaml.Unmarshal(configData, &appConfig); err != nil {
		logx.E(ctx, "failed to parse config file: %+v", err)
		return nil, errs.ErrWrapf(errs.ErrParams, "failed to parse config file: %+v", err)
	}

	return &appConfig, nil
}

// loadAllCorps 分批查询所有企业
func loadAllCorps(ctx context.Context, appConfig *AppUpdateUsedCapacityConfig) ([]AppUpdateUsedCapacityUinList, error) {
	logx.I(ctx, "all_uin is true, querying all corps")

	var params []AppUpdateUsedCapacityUinList
	pageSize := uint32(getCurrentPageSize())
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

		// 将每个企业转换为param
		for _, corp := range corpListRsp.GetList() {
			params = append(params, AppUpdateUsedCapacityUinList{
				Uin: cast.ToUint64(corp.GetUin()),
				All: true, // 处理该企业下所有应用
			})
		}

		// 如果查询结果少于pageSize，说明已经查完了
		if len(corpListRsp.GetList()) < int(pageSize) {
			break
		}

		page++
	}

	logx.I(ctx, "loaded %d corps from all_uin", len(params))
	return params, nil
}

// buildProcessParams 构建处理参数列表
func buildProcessParams(ctx context.Context, appConfig *AppUpdateUsedCapacityConfig) ([]AppUpdateUsedCapacityUinList, error) {
	if appConfig.AllUin {
		return loadAllCorps(ctx, appConfig)
	}

	// 使用配置文件中的uin
	if len(appConfig.UinList) == 0 {
		logx.E(ctx, "no uins found in config file")
		return nil, errs.ErrWrapf(errs.ErrParams, "no uins found in config file")
	}

	logx.I(ctx, "loaded config with %d uins", len(appConfig.UinList))
	return appConfig.UinList, nil
}

// validateAppParam 验证参数
func validateAppParam(ctx context.Context, paramIdx int, param AppUpdateUsedCapacityUinList) error {
	if param.Uin == 0 {
		logx.E(ctx, "param %d: uin is required", paramIdx)
		return errs.ErrWrapf(errs.ErrParams, "param %d: uin is required", paramIdx)
	}

	// 检查参数组合逻辑
	if param.All {
		// 如果All为true，SpaceID和AppBizIDs都必须为空
		if param.SpaceID != "" || len(param.AppBizIDs) > 0 {
			logx.E(ctx, "param %d: when all is true, space_id and app_biz_ids must be empty", paramIdx)
			return errs.ErrWrapf(errs.ErrParams, "param %d: when all is true, space_id and app_biz_ids must be empty", paramIdx)
		}
	} else {
		// 如果All为false，SpaceID和AppBizIDs只能填一个
		if param.SpaceID != "" && len(param.AppBizIDs) > 0 {
			logx.E(ctx, "param %d: space_id and app_biz_ids cannot be used together", paramIdx)
			return errs.ErrWrapf(errs.ErrParams, "param %d: space_id and app_biz_ids cannot be used together", paramIdx)
		}
		if param.SpaceID == "" && len(param.AppBizIDs) == 0 {
			logx.E(ctx, "param %d: either space_id, app_biz_ids or all must be specified", paramIdx)
			return errs.ErrWrapf(errs.ErrParams, "param %d: either space_id, app_biz_ids or all must be specified", paramIdx)
		}
	}

	return nil
}

// getAppListByParam 根据参数获取应用列表
func getAppListByParam(ctx context.Context, paramIdx int, param AppUpdateUsedCapacityUinList, corpID uint64) ([]*entity.AppBaseInfo, uint64, error) {
	req := &pb.ListAppBaseInfoReq{
		CorpPrimaryId: corpID,
	}

	var apps []*entity.AppBaseInfo
	var totalApps uint64
	var err error

	if param.All {
		// 处理该uin下所有应用
		apps, totalApps, err = GetCmdService().RpcImpl.ListAllAppBaseInfo(ctx, req)
		if err != nil {
			logx.E(ctx, "param %d: ListAllAppBaseInfo err: %+v", paramIdx, err)
			return nil, 0, err
		}
	} else if param.SpaceID != "" {
		// 处理指定space_id下的所有应用
		req.SpaceId = param.SpaceID
		apps, totalApps, err = GetCmdService().RpcImpl.ListAllAppBaseInfo(ctx, req)
		if err != nil {
			logx.E(ctx, "param %d: ListAllAppBaseInfo by space_id err: %+v", paramIdx, err)
			return nil, 0, err
		}
	} else {
		// 只处理指定的app_biz_ids
		req.AppBizIds = param.AppBizIDs
		apps, totalApps, err = GetCmdService().RpcImpl.ListAllAppBaseInfo(ctx, req)
		if err != nil {
			logx.E(ctx, "param %d: ListAllAppBaseInfo err: %+v", paramIdx, err)
			return nil, 0, err
		}
	}

	logx.I(ctx, "param %d: got %d apps of corp (uin: %d)", paramIdx, totalApps, param.Uin)
	return apps, totalApps, nil
}

// UinTask Uin处理任务
type UinTask struct {
	Param     AppUpdateUsedCapacityUinList
	UinIndex  int
	TotalUins int
}

// AppTask App处理任务
type AppTask struct {
	App           *entity.AppBaseInfo
	CorpPrimaryID uint64
	Config        AppProcessConfig
	UinIndex      int
	TotalUins     int
	AppIndex      int
	TotalApps     int
}

// processUinsWithWorkerPool 使用 Worker Pool 并发处理 Uin
func processUinsWithWorkerPool(ctx context.Context, params []AppUpdateUsedCapacityUinList,
	config AppProcessConfig, appConfig *AppUpdateUsedCapacityConfig) error {
	totalUins := len(params)
	uinWorkerCount := appConfig.UinWorkerCount

	logx.I(ctx, "starting uin worker pool to process %d uins concurrently with %d workers",
		totalUins, uinWorkerCount)

	// 调整 worker 数量
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
					workerID, task.UinIndex, task.TotalUins, task.Param.Uin)

				if err := processUinTask(ctx, task, config, appConfig); err != nil {
					logx.E(ctx, "uin worker %d: failed to process uin [%d/%d], uin=%d, err: %+v",
						workerID, task.UinIndex, task.TotalUins, task.Param.Uin, err)
					// 将错误发送到错误通道，但不立即返回，让其他 worker 继续处理
					select {
					case errChan <- err:
					default:
					}
					continue
				}

				logx.I(ctx, "uin worker %d: successfully processed uin [%d/%d], uin=%d",
					workerID, task.UinIndex, task.TotalUins, task.Param.Uin)
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
	for i, param := range params {
		taskChan <- &UinTask{
			Param:     param,
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
func processUinTask(ctx context.Context, task *UinTask, config AppProcessConfig,
	appConfig *AppUpdateUsedCapacityConfig) error {
	param := task.Param
	paramIdx := task.UinIndex
	totalParams := task.TotalUins

	logx.I(ctx, "processing param %d/%d: uin=%d, all=%v, app_biz_ids=%v, space_id=%v",
		paramIdx, totalParams, param.Uin, param.All, param.AppBizIDs, param.SpaceID)

	// 1. 参数验证
	if err := validateAppParam(ctx, paramIdx, param); err != nil {
		return err
	}

	// 2. 通过uin获取企业信息
	corp, err := GetCmdService().RpcImpl.PlatformAdmin.DescribeCorp(ctx, &platformPb.DescribeCorpReq{
		Uin: cast.ToString(param.Uin),
	})
	if err != nil {
		logx.E(ctx, "param %d: DescribeCorp by uin err: %+v", paramIdx, err)
		return err
	}
	corpID := corp.GetCorpPrimaryId()

	// 3. 获取应用列表
	apps, _, err := getAppListByParam(ctx, paramIdx, param, corpID)
	if err != nil {
		return err
	}

	// 4. 使用 Worker Pool 并发处理应用列表
	return processAppsWithWorkerPool(ctx, paramIdx, totalParams, apps, corp.GetCorpPrimaryId(), config, appConfig)
}

// AppWorkerConfig App Worker配置
type AppWorkerConfig struct {
	RedisKeyPrefix string // Redis key前缀
}

// processAppsWithWorkerPool 使用 Worker Pool 并发处理应用
func processAppsWithWorkerPool(ctx context.Context, uinIndex, totalUins int,
	apps []*entity.AppBaseInfo, corpPrimaryID uint64, config AppProcessConfig,
	appConfig *AppUpdateUsedCapacityConfig) error {
	totalApps := len(apps)
	appWorkerCount := appConfig.AppWorkerCount

	logx.I(ctx, "uin %d/%d: starting app worker pool with %d workers to process %d apps",
		uinIndex, totalUins, appWorkerCount, totalApps)

	// 调整 worker 数量
	if totalApps < appWorkerCount {
		appWorkerCount = totalApps
	}

	// 创建任务通道
	taskChan := make(chan *AppTask, len(apps))

	// 使用 WaitGroup 等待所有 worker 完成
	var wg sync.WaitGroup

	// 创建互斥锁用于写入日志文件
	var logMutex sync.Mutex

	// 创建计数器用于跟踪已处理的app数量
	var processedCount int
	var countMutex sync.Mutex

	// 创建 worker 配置
	workerConfig := &AppWorkerConfig{
		RedisKeyPrefix: appConfig.RedisKeyPrefix,
	}

	// 启动 workers
	for i := 0; i < appWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskChan {
				// 记录开始时间
				startTime := time.Now()

				// 获取当前进度
				countMutex.Lock()
				currentCount := processedCount + 1
				countMutex.Unlock()

				// 计算百分比
				uinPercent := float64(uinIndex) / float64(totalUins) * 100
				appPercent := float64(currentCount) / float64(totalApps) * 100

				logx.I(ctx, "app worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] processing app %d (biz_id: %d)",
					workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent,
					task.App.PrimaryId, task.App.BizId)

				// 记录开始处理到日志文件
				logMutex.Lock()
				if writeErr := writeProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "START", 0, nil); writeErr != nil {
					logx.E(ctx, "app worker %d: failed to write process log: %+v", workerID, writeErr)
				}
				logMutex.Unlock()

				if err := processSingleApp(ctx, uinIndex, task.App, task.Config, workerConfig); err != nil {
					duration := time.Since(startTime)
					logx.E(ctx, "app worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] failed to process app %d (biz_id: %d) in %v: %+v",
						workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent,
						task.App.PrimaryId, task.App.BizId, duration, err)

					// 记录错误到日志文件
					logMutex.Lock()
					if writeErr := writeProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "ERROR", duration, err); writeErr != nil {
						logx.E(ctx, "app worker %d: failed to write error log: %+v", workerID, writeErr)
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

				logx.I(ctx, "app worker %d: [uin %d/%d %.2f%%] [app %d/%d %.2f%%] successfully processed app %d (biz_id: %d) in %v",
					workerID, uinIndex, totalUins, uinPercent, currentCount, totalApps, appPercent,
					task.App.PrimaryId, task.App.BizId, duration)

				// 记录成功处理到日志文件
				logMutex.Lock()
				if writeErr := writeProcessLog(ctx, task.App, uinIndex, totalUins, currentCount, totalApps, "SUCCESS", duration, nil); writeErr != nil {
					logx.E(ctx, "app worker %d: failed to write process log: %+v", workerID, writeErr)
				}
				logMutex.Unlock()
			}
		}(i)
	}

	// 发送任务到通道
	for i, app := range apps {
		app.CorpPrimaryId = corpPrimaryID
		taskChan <- &AppTask{
			App:           app,
			CorpPrimaryID: corpPrimaryID,
			Config:        config,
			UinIndex:      uinIndex,
			TotalUins:     totalUins,
			AppIndex:      i + 1,
			TotalApps:     totalApps,
		}
	}
	close(taskChan)

	// 等待所有 worker 完成
	wg.Wait()

	return nil
}

// processSingleApp 处理单个应用
func processSingleApp(ctx context.Context, paramIdx int, app *entity.AppBaseInfo, config AppProcessConfig, workerConfig *AppWorkerConfig) error {
	logx.I(ctx, "param %d: processing app: %s", paramIdx, jsonx.MustMarshal(app))

	// 1. 检查Redis中是否已有该app处理成功的记录
	processed, err := checkAppProcessed(ctx, app.BizId, workerConfig.RedisKeyPrefix)
	if err != nil {
		logx.E(ctx, "param %d: checkAppProcessed failed for app %d (biz_id: %d): %+v",
			paramIdx, app.PrimaryId, app.BizId, err)
		// Redis错误不影响主流程，继续处理
	} else if processed {
		logx.I(ctx, "param %d: app %d (biz_id: %d) already processed, skip",
			paramIdx, app.PrimaryId, app.BizId)
		return nil
	}

	// 3. 依次调用处理函数
	processErr := executeProcessFuncs(ctx, paramIdx, app, config.ProcessFuncs)

	// 5. 如果处理失败，返回错误
	if processErr != nil {
		return processErr
	}

	// 6. 处理成功，记录到Redis
	if err := markAppProcessed(ctx, app.BizId, workerConfig.RedisKeyPrefix); err != nil {
		logx.E(ctx, "param %d: markAppProcessed failed for app %d (biz_id: %d): %+v",
			paramIdx, app.PrimaryId, app.BizId, err)
		// Redis错误不影响主流程
	} else {
		logx.I(ctx, "param %d: markAppProcessed succeeded for app %d (biz_id: %d)",
			paramIdx, app.PrimaryId, app.BizId)
	}

	return nil
}

// executeProcessFuncs 执行处理函数列表
func executeProcessFuncs(ctx context.Context, paramIdx int, app *entity.AppBaseInfo, processFuncs []AppProcessFunc) error {
	for funcIdx, processFunc := range processFuncs {
		if processFunc == nil {
			continue
		}
		if err := processFunc(ctx, app); err != nil {
			logx.E(ctx, "param %d: ProcessFunc[%d] failed for app %d (biz_id: %d): %+v",
				paramIdx, funcIdx, app.PrimaryId, app.BizId, err)
			return err
		}
		logx.I(ctx, "param %d: ProcessFunc[%d] succeeded for app %d (biz_id: %d)",
			paramIdx, funcIdx, app.PrimaryId, app.BizId)
	}
	return nil
}

// getCurrentPageSize 获取当前配置的pageSize
func getCurrentPageSize() int {
	if currentPageSize > 0 {
		return currentPageSize
	}
	return DefaultPageSize
}

// ProcessApp 处理应用的容量使用情况
// 更新t_robot表的used_knowledge_capacity、used_storage_capacity和used_compute_capacity三个字段
func ProcessApp(ctx context.Context, app *entity.AppBaseInfo) error {
	logx.I(ctx, "processing app capacity: %s", jsonx.MustMarshal(app))

	// 1. 计算文档的file_size总和（未删除状态）
	var totalDocFileSize uint64
	var totalDocFileSizeForStorage uint64

	docPageSize := getCurrentPageSize()
	docOffset := 0

	// 分批获取文档列表
	docList, err := GetCmdService().DocLogic.GetDao().GetAllDocs(ctx,
		[]string{"id", "file_size", "source", "is_deleted"},
		&docEntity.DocFilter{
			RouterAppBizID: app.BizId,
			RobotId:        app.PrimaryId,
			IsDeleted:      ptrx.Bool(false),
			Offset:         docOffset,
			Limit:          docPageSize,
		})
	if err != nil {
		logx.E(ctx, "GetAllDocs err: %+v, app_id: %d, offset: %d", err, app.PrimaryId, docOffset)
		return err
	}

	logx.I(ctx, "GetAllDocs success, app_id: %d, offset: %d, doc_count: %d",
		app.PrimaryId, docOffset, len(docList))

	// 累加文档的file_size
	for _, doc := range docList {
		totalDocFileSize += doc.FileSize
		// 只有source不为SourceFromCorpCOSDoc的文件才计入storage_capacity
		if doc.Source != docEntity.SourceFromCorpCOSDoc {
			totalDocFileSizeForStorage += doc.FileSize
		}
	}
	logx.I(ctx, "app_id: %d, total_doc_file_size: %d, total_doc_file_size_for_storage: %d",
		app.PrimaryId, totalDocFileSize, totalDocFileSizeForStorage)

	// 2. 计算问答的qa_size总和（未删除状态）
	var totalQaSize uint64

	qaPageSize := getCurrentPageSize()
	qaOffset := 0

	// 分批获取QA列表
	qaList, err := GetCmdService().QaLogic.GetDao().GetAllDocQas(ctx,
		[]string{"id", "qa_size"},
		&qaEntity.DocQaFilter{
			RobotId:   app.PrimaryId,
			IsDeleted: ptrx.Uint32(qaEntity.QAIsNotDeleted),
			Offset:    qaOffset,
			Limit:     qaPageSize,
		})
	if err != nil {
		logx.E(ctx, "GetAllDocQas err: %+v, app_id: %d", err, app.PrimaryId)
		return err
	}

	logx.I(ctx, "GetAllDocQas success, app_id: %d, qa_count: %d",
		app.PrimaryId, len(qaList))

	// 累加问答的qa_size
	for _, qa := range qaList {
		totalQaSize += qa.QaSize
	}

	logx.I(ctx, "app_id: %d, total_qa_size: %d", app.PrimaryId, totalQaSize)

	// 3. 计算三个容量字段的值
	usedKnowledgeCapacity := totalDocFileSize + totalQaSize
	usedStorageCapacity := totalDocFileSizeForStorage
	usedComputeCapacity := usedKnowledgeCapacity

	logx.I(ctx, "app_id: %d, used_knowledge_capacity: %d, used_storage_capacity: %d, used_compute_capacity: %d",
		app.PrimaryId, usedKnowledgeCapacity, usedStorageCapacity, usedComputeCapacity)

	// 4. 更新t_robot表的三个字段
	err = GetCmdService().RpcImpl.AppAdmin.SetAppUsage(ctx, entity.CapacityUsage{
		KnowledgeCapacity: int64(usedKnowledgeCapacity),
		StorageCapacity:   int64(usedStorageCapacity),
		ComputeCapacity:   int64(usedComputeCapacity),
	}, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "SetAppUsage err: %+v, app_id: %d", err, app.PrimaryId)
		return err
	}

	logx.I(ctx, "SetAppUsage success, app_id: %d", app.PrimaryId)
	return nil
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

// writeProcessLog 将处理信息写入日志文件
// status: START, SUCCESS, ERROR
func writeProcessLog(ctx context.Context, app *entity.AppBaseInfo, uinIndex, totalUins, currentCount, totalApps int, status string, duration time.Duration, err error) error {
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
