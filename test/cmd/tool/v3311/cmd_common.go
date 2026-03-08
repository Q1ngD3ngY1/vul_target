package main

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	// 智言环境变量key
	zhiYanEnvKey = "CLE_ENV_NAME"
)

const (
	defaultUinWorkerCount            = 4
	defaultAppWorkerCount            = 4
	defaultDocCopyWorkerCount        = 4
	defaultQaWorkerCount             = 4
	defaultDocUpdateLabelWorkerCount = 5
)

// AppWorkerConfig 定义处理应用时的Worker配置参数
type AppWorkerConfig struct {
	DocCopyWorkerCount        int    // Doc级别的Worker并发数量
	QaWorkerCount             int    // QA级别的Worker并发数量
	DocUpdateLabelWorkerCount int    // Doc更新标签的Worker并发数量
	RedisKeyPrefix            string // Redis key前缀
}

// ProcessAppFunc 定义处理应用的函数类型
type ProcessAppFunc func(ctx context.Context, app *entity.App, config *AppWorkerConfig) error

// EnableScopeParams 定义enable_scope命令的参数
type EnableScopeParams struct {
	Uin                  string
	AppBizIDs            []string
	SpaceID              string
	All                  bool
	TypeName             string // 用于日志输出，如 "Doc", "Db", "QA"
	SkipEmbeddingUpgrade bool   // 是否跳过锁定应用操作，应用锁定后不能对知识库做任何知识变更操作
}

// RunEnableScopeCommand 通用的enable_scope处理逻辑
// params: 命令参数
// processFunc: 具体的处理函数（ProcessAppDoc、ProcessAppDb、ProcessAppQa）
func RunEnableScopeCommand(cmd *cobra.Command, processFunc ProcessAppFunc, params EnableScopeParams, appWorkerConfig *AppWorkerConfig) error {
	ctx := NewContext(cmd.Context())

	// 检查uin参数
	if params.Uin == "" {
		logx.E(ctx, "uin is required")
		return errs.ErrWrapf(errs.ErrParams, "uin is required")
	}

	// 检查参数组合逻辑
	if params.All {
		// 如果All为true，SpaceID和AppBizIDs都必须为空
		if params.SpaceID != "" || len(params.AppBizIDs) > 0 {
			logx.E(ctx, "when --all is specified, --space_id and --app_biz_ids must be empty")
			return errs.ErrWrapf(errs.ErrParams, "when --all is specified, --space_id and --app_biz_ids must be empty")
		}
	} else {
		// 如果All为false，SpaceID和AppBizIDs只能填一个
		if params.SpaceID != "" && len(params.AppBizIDs) > 0 {
			logx.E(ctx, "--space_id and --app_biz_ids cannot be used together")
			return errs.ErrWrapf(errs.ErrParams, "--space_id and --app_biz_ids cannot be used together")
		}
		if params.SpaceID == "" && len(params.AppBizIDs) == 0 {
			logx.E(ctx, "either --space_id, --app_biz_ids or --all must be specified")
			return errs.ErrWrapf(errs.ErrParams, "either --space_id, --app_biz_ids or --all must be specified")
		}
	}

	// 通过uin获取企业信息
	corp, err := GetCmdService().RpcImpl.PlatformAdmin.DescribeCorp(ctx, &pb.DescribeCorpReq{
		Uin: params.Uin,
	})
	if err != nil {
		logx.E(ctx, "DescribeCorp by uin err: %+v", err)
		return err
	}
	corpID := corp.GetCorpPrimaryId()

	// 获取应用列表
	req := &appConfig.GetAppListReq{
		CorpPrimaryId: corpID,
		DisablePrompt: true,
	}

	// 根据参数决定获取哪些应用
	var apps []*entity.App
	var totalApps uint64
	if params.All {
		// 处理该uin下所有应用
		apps, totalApps, err = GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	} else if params.SpaceID != "" {
		// 处理指定space_id下的所有应用
		req.SpaceId = params.SpaceID
		apps, totalApps, err = GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList by space_id err: %+v", err)
			return err
		}
	} else {
		// 只处理指定的app_biz_ids
		appBizIDs := slicex.Map(params.AppBizIDs, func(s string) uint64 { return cast.ToUint64(s) })
		req.BotBizIds = appBizIDs
		apps, totalApps, err = GetCmdService().RpcImpl.DescribeAppList(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	}

	logx.I(ctx, "got apps of corp (uin: %s): %d", params.Uin, totalApps)

	// 处理每个应用
	for i, app := range apps {
		app.CorpBizId = corp.GetCorpId()
		logx.I(ctx, "processing %s app [%d/%d]: app_id=%d, app_biz_id=%d, app_name=%s",
			params.TypeName, i+1, totalApps, app.PrimaryId, app.BizId, app.Name)

		// 如果不跳过embedding升级，则执行升级逻辑
		var embeddingVersion uint64
		if !params.SkipEmbeddingUpgrade {
			// 应用升级embedding开始
			embeddingVersion = app.Embedding.Version
			startReq := &appConfig.StartEmbeddingUpgradeAppReq{
				AppBizId:             app.BizId,
				FromEmbeddingVersion: embeddingVersion,
				ToEmbeddingVersion:   embeddingVersion,
			}
			_, err := GetCmdService().RpcImpl.AppAdmin.StartEmbeddingUpgradeApp(ctx, startReq)
			if err != nil {
				logx.E(ctx, "StartEmbeddingUpgradeApp failed for app %d (biz_id: %d): %+v",
					app.PrimaryId, app.BizId, err)
				return fmt.Errorf("StartEmbeddingUpgradeApp failed for app %d: %w", app.PrimaryId, err)
			}
			logx.I(ctx, "StartEmbeddingUpgradeApp succeeded for app %d (biz_id: %d), embedding_version: %d",
				app.PrimaryId, app.BizId, embeddingVersion)
		}

		// 调用具体的处理函数
		var processErr error
		if err := processFunc(ctx, app, appWorkerConfig); err != nil {
			logx.E(ctx, "Process%sApp failed for app %d (biz_id: %d): %+v",
				params.TypeName, app.PrimaryId, app.BizId, err)
			processErr = err
		} else {
			logx.I(ctx, "Process%sApp succeeded for app %d (biz_id: %d)",
				params.TypeName, app.PrimaryId, app.BizId)
		}

		// 如果不跳过embedding升级，则执行结束逻辑
		if !params.SkipEmbeddingUpgrade {
			// 应用升级embedding结束（无论成功失败都要调用）
			finishReq := &appConfig.FinishEmbeddingUpgradeAppReq{
				AppBizId:             app.BizId,
				FromEmbeddingVersion: embeddingVersion,
				ToEmbeddingVersion:   embeddingVersion,
			}
			_, err := GetCmdService().RpcImpl.AppAdmin.FinishEmbeddingUpgradeApp(ctx, finishReq)
			if err != nil {
				logx.E(ctx, "FinishEmbeddingUpgradeApp failed for app %d (biz_id: %d): %+v",
					app.PrimaryId, app.BizId, err)
				return fmt.Errorf("FinishEmbeddingUpgradeApp failed for app %d: %w", app.PrimaryId, err)
			}
			logx.I(ctx, "FinishEmbeddingUpgradeApp succeeded for app %d (biz_id: %d)",
				app.PrimaryId, app.BizId)
		}

		// 如果处理失败，返回错误
		if processErr != nil {
			return processErr
		}
	}
	return nil
}

// NewContext 创建一个带有CleRouteEnv的上下文
func NewContext(ctx context.Context) context.Context {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)

	newCtx, _ := contextx.StartTrace(ctx, "enable_scope")
	/* 当前无法解决泳道插件的问题，这里携带了env也无法生效，先注释掉
	env := os.Getenv(zhiYanEnvKey)
	logx.I(ctx, "NewContext got env: %s", env)
	if env != "" {
		contextx.Metadata(newCtx).WithCleRouteEnv(env)
	}
	*/
	logx.I(newCtx, "NewContext got newCtx: %+v", newCtx)
	return newCtx
}
