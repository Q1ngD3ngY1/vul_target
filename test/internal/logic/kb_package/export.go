package kb_package

import (
	"context"
	"errors"
	"strconv"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbPackageEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ExportKnowledgeBases 导出知识库数据
func (l *Logic) ExportKnowledgeBases(ctx context.Context, req *pb.ExportKnowledgeBasesReq) error {
	logx.I(ctx, "export kb data, appBizId: %d", req.GetAppBizId())

	// 1. 获取应用详情和企业详情
	app, err := l.rpc.GetAppBaseInfo(ctx, req.GetAppBizId())
	if err != nil {
		return err
	}
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, req.GetCorpBizId())
	if err != nil {
		return err
	}
	if app.CorpPrimaryId != corp.GetCorpPrimaryId() {
		logx.E(ctx, "app and corp not match, appCorpPrimaryId: %d, corpPrimaryId: %d", app.CorpPrimaryId, corp.GetCorpPrimaryId())
		return errors.New("app and corp not match")
	}

	// 处理要导出的知识库列表
	kbIDs := make([]uint64, 0)
	for _, kbIDStr := range req.GetKnowledgeBaseIds() {
		kbID, err := strconv.ParseUint(kbIDStr, 10, 64)
		if err != nil {
			logx.E(ctx, "parse kb id failed, kbID: %s, err: %+v", kbIDStr, err)
			return err
		}
		kbIDs = append(kbIDs, kbID)
	}
	// 检查导出应用的容量大小。如果超过100MB，则不允许导出
	if err := l.checkExportCapacity(ctx, corp.GetCorpId(), app.BizId, kbIDs); err != nil {
		return err
	}
	// 创建知识库数据包导出的异步任务
	exportTaskID, err := l.createExportTask(ctx, req, app, kbIDs)
	if err != nil {
		return err
	}

	logx.I(ctx, "kb data export task created successfully, task ID: %d", exportTaskID)
	return nil
}

// createExportTask 创建导出异步任务
func (l *Logic) createExportTask(ctx context.Context, req *pb.ExportKnowledgeBasesReq, app *entity.AppBaseInfo, kbIDs []uint64) (uint64, error) {
	// 创建知识库导出任务参数
	exportParams := entity.ExportKbPackageParams{
		Name:           "kb_package_export",
		AppPrimaryID:   app.PrimaryId,
		AppBizID:       app.BizId,
		CorpBizID:      req.GetCorpBizId(),
		CorpPrimaryID:  app.CorpPrimaryId,
		StaffPrimaryID: contextx.Metadata(ctx).StaffID(),
		KbIDs:          kbIDs,
		ExportCosPath:  req.GetCosFilePath(),            // 导出路径，可以根据需要设置
		TaskID:         req.GetTaskId(),                 // 任务ID
		SubTaskID:      req.GetSubTaskId(),              // 子任务ID
		Scene:          kbPackageEntity.SceneAppPackage, // 场景
	}

	// 创建知识库导出任务
	taskID, err := scheduler.NewKbPackageExportTask(ctx, req.GetCorpBizId(), exportParams)
	if err != nil {
		logx.E(ctx, "Failed to create kb package export task, appBizId: %d, error: %v", req.GetAppBizId(), err)
		return 0, err
	}
	logx.I(ctx, "kb package export task created successfully, task ID: %d, appBizId: %d", taskID, req.GetAppBizId())
	return taskID, nil
}

// checkExportCapacity 检查导出容量是否超过限制
func (l *Logic) checkExportCapacity(ctx context.Context, corpBizID, appBizID uint64, kbIDs []uint64) error {
	logx.I(ctx, "checkExportCapacity start, corpBizID: %d, kbIDs: %v", corpBizID, kbIDs)

	// 调用RPC获取知识库容量使用情况
	capacityUsage, err := l.rpc.AppApi.DescribeCorpKnowledgeCapacity(ctx, corpBizID, kbIDs)
	if err != nil {
		logx.E(ctx, "checkExportCapacity DescribeCorpKnowledgeCapacity failed, corpBizID: %d, kbIDs: %v, err: %+v", corpBizID, kbIDs, err)
		return err
	}

	// 获取知识库容量（单位：字节）
	knowledgeCapacity := capacityUsage.KnowledgeCapacity
	logx.I(ctx, "checkExportCapacity knowledge capacity: %d bytes (%.2f MB)", knowledgeCapacity, float64(knowledgeCapacity)/(1024*1024))

	// 从配置中获取导出容量限制（单位：MB），支持按应用配置
	maxExportCapacityMB := config.DescribePackageSizeLimitMB(appBizID)
	logx.I(ctx, "checkExportCapacity appBizID: %d, maxExportCapacityMB: %d MB", appBizID, maxExportCapacityMB)

	// 将MB转换为字节
	maxExportCapacityBytes := int64(maxExportCapacityMB) * 1024 * 1024

	if knowledgeCapacity > maxExportCapacityBytes {
		logx.W(ctx, "checkExportCapacity capacity exceeds limit, corpBizID: %d, kbIDs: %v, appBizID: %d, capacity: %d bytes (%.2f MB), limit: %d MB (%d bytes)",
			corpBizID, kbIDs, appBizID, knowledgeCapacity, float64(knowledgeCapacity)/(1024*1024), maxExportCapacityMB, maxExportCapacityBytes)
		return errs.ErrExportKbPackageOverMaxLimit
	}

	logx.I(ctx, "checkExportCapacity passed, appBizID: %d, size: %.2f MB, limit: %d MB, corpBizID: %d, kbIDs: %v",
		appBizID, float64(knowledgeCapacity)/(1024*1024), maxExportCapacityMB, corpBizID, kbIDs)
	return nil
}
