package kb_package

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ImportKnowledgeBases 导入知识库数据
func (l *Logic) ImportKnowledgeBases(ctx context.Context, req *pb.ImportKnowledgeBasesReq) error {
	logx.I(ctx, "import kb data, appBizId: %d", req.GetAppBizId())
	// 1. 获取应用详情
	app, err := l.rpc.GetAppBaseInfo(ctx, req.GetAppBizId())
	if err != nil {
		return err
	}
	// 2. 创建知识库数据包导入的异步任务
	importTaskID, err := l.createImportTask(ctx, req, app)
	if err != nil {
		return err
	}

	logx.I(ctx, "kb data import task created successfully, task ID: %d", importTaskID)
	return nil
}

// createImportTask 创建导入异步任务
func (l *Logic) createImportTask(ctx context.Context, req *pb.ImportKnowledgeBasesReq, app *entity.AppBaseInfo) (uint64, error) {
	importParams := entity.ImportKbPackageParams{
		Name:                "kb_package_import",
		SpaceID:             req.SpaceId,
		Uin:                 app.Uin,
		CorpPrimaryID:       app.CorpPrimaryId,
		CorpBizID:           req.GetCorpBizId(),
		StaffPrimaryID:      contextx.Metadata(ctx).StaffID(),
		AppPrimaryID:        app.PrimaryId,
		AppBizID:            app.BizId,
		ImportAppPackageURL: req.GetAppPackageUrl(),
		IdMappingCosUrl:     req.GetIdMappingCosUrl(),
		TaskID:              req.GetTaskId(),
		SubTaskID:           req.GetSubTaskId(),
		Scene:               kb_package.SceneAppPackage,
	}
	// 创建知识库导入任务
	taskID, err := scheduler.NewKbPackageImportTask(ctx, req.GetCorpBizId(), importParams)
	if err != nil {
		logx.E(ctx, "Failed to create kb package import task, appBizId: %d, error: %v", req.GetAppBizId(), err)
		return 0, err
	}
	logx.I(ctx, "kb package import task created successfully, task ID: %d, appBizId: %d", taskID, req.GetAppBizId())
	return taskID, nil
}
