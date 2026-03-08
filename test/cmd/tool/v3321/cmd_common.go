package main

import (
	"context"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
)

const (
	defaultUinWorkerCount = 4
	defaultAppWorkerCount = 4
	DefaultPageSize       = 200
)

// ProcessAppFunc 定义处理应用的函数类型
type ProcessAppFunc func(ctx context.Context, app *entity.AppBaseInfo) error

// UpdateUsedCapacityParams 定义update_used_capacity命令的参数
type UpdateUsedCapacityParams struct {
	Uin                  string
	AppBizIDs            []string
	SpaceID              string
	All                  bool
	TypeName             string // 用于日志输出，如 "Doc", "Db", "QA"
	SkipEmbeddingUpgrade bool   // 是否跳过锁定应用操作，应用锁定后不能对知识库做任何知识变更操作
}

// RunUpdateUsedCapacityCommand 通用的update_used_capacity处理逻辑
// params: 命令参数
// processFunc: 具体的处理函数（ProcessAppDoc、ProcessAppDb、ProcessAppQa）
func RunUpdateUsedCapacityCommand(cmd *cobra.Command, params UpdateUsedCapacityParams, processFunc ProcessAppFunc) error {
	ctx := cmd.Context()
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
	req := &appConfig.ListAppBaseInfoReq{
		CorpPrimaryId: corpID,
	}

	// 根据参数决定获取哪些应用
	var apps []*entity.AppBaseInfo
	var totalApps uint64
	if params.All {
		// 处理该uin下所有应用
		apps, totalApps, err = GetCmdService().RpcImpl.ListAppBaseInfo(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	} else if params.SpaceID != "" {
		// 处理指定space_id下的所有应用
		req.SpaceId = params.SpaceID
		apps, totalApps, err = GetCmdService().RpcImpl.ListAppBaseInfo(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList by space_id err: %+v", err)
			return err
		}
	} else {
		// 只处理指定的app_biz_ids
		appBizIDs := slicex.Map(params.AppBizIDs, func(s string) uint64 { return cast.ToUint64(s) })
		req.AppBizIds = appBizIDs
		apps, totalApps, err = GetCmdService().RpcImpl.ListAppBaseInfo(ctx, req)
		if err != nil {
			logx.E(ctx, "DescribeAppList err: %+v", err)
			return err
		}
	}

	logx.I(ctx, "got apps of corp (uin: %s): %d", params.Uin, totalApps)

	// 处理每个应用
	for i, app := range apps {
		app.CorpPrimaryId = corp.GetCorpPrimaryId()
		logx.I(ctx, "processing %s app [%d/%d]: app_id=%d, app_biz_id=%d, app_name=%s",
			params.TypeName, i+1, totalApps, app.PrimaryId, app.BizId, app.Name)

		// 调用具体的处理函数
		var processErr error
		if err := processFunc(ctx, app); err != nil {
			logx.E(ctx, "Process%sApp failed for app %d (biz_id: %d): %+v",
				params.TypeName, app.PrimaryId, app.BizId, err)
			processErr = err
		} else {
			logx.I(ctx, "Process%sApp succeeded for app %d (biz_id: %d)",
				params.TypeName, app.PrimaryId, app.BizId)
		}

		// 如果处理失败，返回错误
		if processErr != nil {
			return processErr
		}
	}
	return nil
}
